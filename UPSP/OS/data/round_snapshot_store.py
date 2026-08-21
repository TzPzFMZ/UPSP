"""Round audit JSONL store.

Runtime writes one append-only round_{round_num}.jsonl event stream per round.
The rolling setup/reaction/cleanup step files remain the latest-step audit view;
this store keeps the historical round timeline and prunes only round_*.jsonl.
"""
import json
import os
import re
import copy
import threading
from datetime import datetime, timezone

from paths import AUDIT_DIR, STM_CONTEXT_DIR
from data.prompt_cache_telemetry import extract_prompt_cache_telemetry
from data.round_audit_codec import RoundAuditDecoder, RoundAuditEncoder
from data.round_retention import enforce_round_retention

SCHEMA_VERSION = "round_audit.v2"


def reaction_popup_snapshot_status(event):
    """Return the audit-completeness status for one reaction input snapshot.

    A ``step_input_snapshot`` is evidence of the exact model-visible request
    only when its persisted ten-layer snapshot includes exactly one textual
    ``99_popup`` layer.  The empty string is valid: an empty POPUP is still a
    recorded model-visible fact.  This helper deliberately validates stored
    audit data rather than reconstructing a request from messages.
    """
    if not isinstance(event, dict):
        return "event_invalid"
    if event.get("event_type") != "step_input_snapshot":
        return "event_type_invalid"
    if event.get("phase") != "reaction":
        return "phase_invalid"
    payload = event.get("payload")
    if not isinstance(payload, dict):
        return "payload_missing"
    layers_snapshot = payload.get("layers_snapshot")
    if not isinstance(layers_snapshot, dict):
        return "layers_snapshot_missing"
    if layers_snapshot.get("error"):
        return "layers_snapshot_error"
    layers = layers_snapshot.get("layers")
    if not isinstance(layers, list):
        return "layers_missing"
    popup_layers = [
        layer for layer in layers
        if isinstance(layer, dict) and layer.get("layer_key") == "99_popup"
    ]
    if not popup_layers:
        return "popup_layer_missing"
    if len(popup_layers) != 1:
        return "popup_layer_ambiguous"
    if not isinstance(popup_layers[0].get("content"), str):
        return "popup_content_missing"
    return "complete"
HIGH_RISK_NATIVE_TOOL_IDS = {
    "file_edit",
    "shell_command",
    "subagent_dispatch",
}
HIGH_RISK_NATIVE_REDACT_KEYS = {
    "allowed_paths",
    "command",
    "cwd",
    "diff",
    "forbidden",
    "input_materials",
    "patch",
    "path",
    "task_goal",
    "url",
    "validation_commands",
    "write_scope",
}


class RoundSnapshotStore:
    def __init__(
        self,
        context_root=None,
        retention_count=8,
        retention_max_mib=256,
        retention_enforcer=None,
        static_audit_dir=None,
        static_projection_enabled=True,
    ):
        self.context_root = context_root or STM_CONTEXT_DIR
        self.retention_count = int(retention_count)
        self.retention_max_mib = int(retention_max_mib)
        self.retention_enforcer = retention_enforcer
        self.static_audit_dir = static_audit_dir or AUDIT_DIR
        self.static_projection_enabled = bool(static_projection_enabled)
        self._append_lock = threading.RLock()
        self._event_indexes = {}
        self._encoders = {}
        self._latest_request_refs = {}
        self.last_retention_receipt = None

    def _round_dir(self):
        return os.path.join(self.context_root, "round")

    def _round_path(self, round_num):
        return os.path.join(self._round_dir(), f"round_{int(round_num)}.jsonl")

    def _now(self):
        return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

    def _last_event_index(self, round_num):
        round_num = int(round_num)
        if round_num in self._event_indexes:
            return self._event_indexes[round_num]
        last = 0
        path = self._round_path(round_num)
        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as handle:
                for line in handle:
                    line = line.strip()
                    if not line:
                        continue
                    event = json.loads(line)
                    last = max(last, int(event.get("event_index") or 0))
        self._event_indexes[round_num] = last
        return last

    def _read_events_quiet(self, round_num):
        path = self._round_path(round_num)
        if not os.path.isfile(path):
            return []
        events = []
        decoder = RoundAuditDecoder()
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        events.append(decoder.feed(json.loads(line)))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            return []
        return events

    def append_event(self, round_num, event_type, payload=None, phase=None, iteration=None):
        round_num = int(round_num)
        round_dir = self._round_dir()
        os.makedirs(round_dir, exist_ok=True)
        path = self._round_path(round_num)
        with self._append_lock:
            event_index = self._last_event_index(round_num) + 1
            event = {
                "schema_version": SCHEMA_VERSION,
                "round": round_num,
                "event_index": event_index,
                "event_id": f"R{round_num:06d}-{event_index:06d}",
                "event_type": str(event_type),
                "recorded_at": self._now(),
                "payload": payload or {},
            }
            if phase is not None:
                event["phase"] = str(phase)
            if iteration is not None:
                event["iteration"] = int(iteration)
            if phase is not None and iteration is not None:
                event["frame_id"] = (
                    f"R{round_num:06d}:{str(phase)}:{int(iteration)}")
            with open(path, "a", encoding="utf-8", newline="\n") as f:
                f.write(json.dumps(
                    event,
                    ensure_ascii=False,
                    sort_keys=True,
                    separators=(",", ":"),
                    default=str,
                ) + "\n")
            self._event_indexes[round_num] = event_index
        return event

    def start_round(self, round_num, round_type=None, input_snapshot=None):
        if (
            isinstance(self.last_retention_receipt, dict)
            and self.last_retention_receipt.get("status") == "error"
        ):
            self.last_retention_receipt = (
                self.retention_enforcer()
                if callable(self.retention_enforcer)
                else self.prune()
            )
        round_dir = self._round_dir()
        os.makedirs(round_dir, exist_ok=True)
        path = self._round_path(round_num)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8", newline="\n") as f:
            pass
        os.replace(tmp, path)
        self._event_indexes[int(round_num)] = 0
        self._encoders[int(round_num)] = RoundAuditEncoder()
        self._latest_request_refs = {
            key: value
            for key, value in self._latest_request_refs.items()
            if key[0] != int(round_num)
        }
        payload = {}
        if round_type:
            payload["round_type"] = round_type
        if input_snapshot is not None:
            payload["input_snapshot"] = input_snapshot
        self.append_event(round_num, "round_started", payload)
        return path

    def record_step_input(self, round_num, phase, iteration=1, manifest=None,
                          provider_request_envelope=None, layers_snapshot=None,
                          error=None):
        payload = {}
        if isinstance(error, dict):
            payload.update(copy.deepcopy(error))
        elif error:
            payload["error"] = str(error)
        if manifest is not None:
            payload["manifest"] = manifest
        envelope = self._sanitize_provider_request_envelope(
            provider_request_envelope,
            include_source_map=True,
        ) if provider_request_envelope is not None else {}
        snapshot = self._sanitize_layers_snapshot(layers_snapshot)
        round_num = int(round_num)
        with self._append_lock:
            event_index = self._last_event_index(round_num) + 1
            event_id = f"R{round_num:06d}-{event_index:06d}"
            encoder = self._encoders.setdefault(
                round_num, RoundAuditEncoder()
            ).clone()
            if envelope and snapshot:
                payload["audit_snapshot"] = encoder.encode(
                    envelope,
                    snapshot,
                    phase=phase,
                    event_id=event_id,
                )
            else:
                if envelope:
                    payload["provider_request_envelope"] = envelope
                if snapshot:
                    payload["layers_snapshot"] = snapshot
            event = self.append_event(
                round_num,
                "step_input_snapshot",
                payload,
                phase=phase,
                iteration=iteration,
            )
            self._encoders[round_num] = encoder
        if envelope:
            self._latest_request_refs[(round_num, str(phase), int(iteration))] = {
                "request_snapshot_event_id": event["event_id"],
                "wire_body_sha256": envelope.get("wire_body_sha256"),
                "wire_body_bytes": envelope.get("wire_body_bytes"),
            }
        materialized = copy.deepcopy(event)
        materialized["payload"]["provider_request_envelope"] = envelope
        materialized["payload"]["layers_snapshot"] = snapshot
        return materialized

    def record_step_input_from_files(self, round_num, phase, iteration=1):
        provider_request_envelope = None
        error = None
        context_step = self._context_step_for_phase(phase)
        step_json = os.path.join(self.context_root, context_step, "step.json")
        if os.path.isfile(step_json):
            try:
                with open(step_json, "r", encoding="utf-8") as f:
                    step_data = json.load(f)
                if isinstance(step_data, list):
                    error = {
                        "error": "legacy_step_json_rejected",
                        "reason": "active_step_json_must_be_provider_request_v1",
                        "legacy_step_json_format": "messages_list",
                        "historical": False,
                    }
                elif (
                        isinstance(step_data, dict)
                        and step_data.get("schema") == "provider_request.v1"):
                    provider_request_envelope = step_data
            except (OSError, json.JSONDecodeError):
                provider_request_envelope = None
        manifest = None
        manifest_path = os.path.join(self.context_root, context_step, "manifest.json")
        if os.path.isfile(manifest_path):
            try:
                with open(manifest_path, "r", encoding="utf-8") as f:
                    manifest = json.load(f)
            except (OSError, json.JSONDecodeError):
                manifest = None
        layers_snapshot = self._read_layers_snapshot(context_step)
        return self.record_step_input(
            round_num,
            phase,
            iteration=iteration,
            manifest=manifest,
            provider_request_envelope=provider_request_envelope,
            layers_snapshot=layers_snapshot,
            error=error,
        )

    @staticmethod
    def _context_step_for_phase(phase):
        normalized = str(phase or "").strip()
        if normalized == "final_reply":
            return "reaction"
        return normalized

    def _read_layers_snapshot(self, context_step):
        layers_dir = os.path.join(self.context_root, context_step, "layers")
        if not os.path.isdir(layers_dir):
            return None
        try:
            from data.audit_store import AuditStore
            store = AuditStore(
                setup_dir=os.path.join(self.context_root, "setup"),
                reaction_dir=os.path.join(self.context_root, "reaction"),
                cleanup_dir=os.path.join(self.context_root, "cleanup"),
            )
            layers = store.read_context_layers(context_step)
        except Exception as exc:
            return {
                "schema": "context_layers_snapshot.v1",
                "source": f"context/{context_step}/layers",
                "error": str(exc),
                "layers": [],
                "layer_order": [],
            }
        return {
            "schema": "context_layers_snapshot.v1",
            "source": f"context/{context_step}/layers",
            "layer_order": [layer.get("layer_key") for layer in layers],
            "layers": layers,
        }

    def record_llm_output(self, round_num, phase, iteration, result):
        result = result or {}
        payload = {
            "response": result.get("response", ""),
        }
        for key in (
            "model",
            "endpoint",
            "tokens_input",
            "tokens_output",
            "latency_ms",
            "raw_usage",
            "provider_response_meta",
            "request_contract_audit",
            "error",
        ):
            if key in result:
                value = result.get(key)
                if key == "request_contract_audit":
                    value = self._sanitize_request_contract_audit(value)
                payload[key] = value
        payload.update(self._request_reference(
            round_num, phase, iteration, result
        ))
        telemetry = result.get("prompt_cache_telemetry")
        if (
                not isinstance(telemetry, dict)
                or telemetry.get("schema_version") != "prompt_cache_telemetry.v2"):
            extracted = self._extract_prompt_cache_telemetry(
                result.get("raw_usage"),
                result.get("request_contract_audit"),
            )
            if extracted:
                telemetry = extracted
        if telemetry:
            payload["prompt_cache_telemetry"] = telemetry
        if "tool_call_envelopes" in result:
            payload["tool_call_envelopes"] = self._sanitize_tool_call_envelopes(
                result.get("tool_call_envelopes"))
        return self.append_event(
            round_num,
            "llm_output_raw",
            payload,
            phase=phase,
            iteration=iteration,
        )

    @staticmethod
    def _extract_prompt_cache_telemetry(raw_usage, request_contract_audit=None):
        return extract_prompt_cache_telemetry(
            raw_usage,
            request_contract_audit=request_contract_audit,
        )

    def record_llm_call_started(self, round_num, phase, iteration, contract):
        contract = contract or {}
        payload = {}
        if "request_contract_audit" in contract:
            payload["request_contract_audit"] = self._sanitize_request_contract_audit(
                contract.get("request_contract_audit"))
        payload.update(self._request_reference(
            round_num, phase, iteration, contract
        ))
        for key in (
                "call_channel", "phase", "tier",
                "logical_call_id", "route_slot"):
            if key in contract:
                payload[key] = contract.get(key)
        return self.append_event(
            round_num,
            "llm_call_started",
            payload,
            phase=phase,
            iteration=iteration,
        )

    def record_llm_call_failed(self, round_num, phase, iteration, contract, error):
        contract = contract or {}
        payload = {"error": str(error)}
        if "request_contract_audit" in contract:
            payload["request_contract_audit"] = self._sanitize_request_contract_audit(
                contract.get("request_contract_audit"))
        payload.update(self._request_reference(
            round_num, phase, iteration, contract
        ))
        for key in (
                "call_channel", "phase", "tier",
                "logical_call_id", "route_slot"):
            if key in contract:
                payload[key] = contract.get(key)
        return self.append_event(
            round_num,
            "llm_call_failed",
            payload,
            phase=phase,
            iteration=iteration,
        )

    def _request_reference(self, round_num, phase, iteration, source=None):
        ref = copy.deepcopy(self._latest_request_refs.get(
            (int(round_num), str(phase), int(iteration)),
            {},
        ))
        source = source if isinstance(source, dict) else {}
        envelope = source.get("provider_request_envelope")
        if isinstance(envelope, dict):
            ref.setdefault("wire_body_sha256", envelope.get("wire_body_sha256"))
            ref.setdefault("wire_body_bytes", envelope.get("wire_body_bytes"))
        return {key: value for key, value in ref.items() if value not in (None, "")}

    def record_llm_stream_event(self, round_num, phase, iteration, event_type, payload):
        event_type = str(event_type or "").strip()
        if event_type not in {
            "llm_stream_first_chunk",
            "llm_stream_delta",
            "llm_stream_done",
            "llm_stream_error",
            "llm_http_attempt",
        }:
            event_type = "llm_stream_delta"
        return self.append_event(
            round_num,
            event_type,
            payload if isinstance(payload, dict) else {},
            phase=phase,
            iteration=iteration,
        )

    @staticmethod
    def _sanitize_request_contract_audit(audit):
        if not isinstance(audit, dict):
            return {}
        allowed = (
            "step",
            "provider",
            "model",
            "tool_names",
            "terminal_tool",
            "tool_mode",
            "tools_transmitted",
            "standard_tools_enabled",
            "native_tool_mode",
            "prompt_cache_lane",
            "prompt_cache_key",
            "prompt_cache_key_applied",
            "prompt_cache_retention",
            "prompt_cache_profile",
            "prompt_cache_mode",
            "prompt_cache_plan",
            "prompt_cache_breakpoint_strategy",
            "prompt_cache_breakpoint_targets",
            "prompt_cache_prefix_fingerprint",
            "prompt_cache_lately_epoch",
        )
        return {key: copy.deepcopy(audit.get(key)) for key in allowed if key in audit}

    @staticmethod
    def _sanitize_provider_request_envelope(
            envelope, *, include_source_map=False):
        if not isinstance(envelope, dict):
            return {}
        cleaned, _redacted = RoundSnapshotStore._redact_value(
            copy.deepcopy(envelope))
        if include_source_map and isinstance(cleaned, dict):
            wire_contract = all(key in envelope for key in (
                "request_body_sha256",
                "wire_body_encoding",
                "wire_body_sha256",
                "wire_body_bytes",
                "request_body_source_map",
            ))
            if wire_contract:
                cleaned["request_body"] = copy.deepcopy(envelope.get("request_body"))
            else:
                cleaned["request_body"], _ = (
                    RoundSnapshotStore._redact_provider_request_body(
                        copy.deepcopy(envelope.get("request_body"))))
        context_window = envelope.get("context_window_tokens")
        if (
                isinstance(cleaned, dict)
                and isinstance(context_window, int)
                and not isinstance(context_window, bool)
                and context_window > 0):
            cleaned["context_window_tokens"] = context_window
        if isinstance(cleaned, dict) and not include_source_map:
            cleaned.pop("request_body_source_map", None)
        return cleaned if isinstance(cleaned, dict) else {}

    @staticmethod
    def _sanitize_layers_snapshot(snapshot):
        if not isinstance(snapshot, dict):
            return {}
        cleaned, _redacted = RoundSnapshotStore._redact_value(
            copy.deepcopy(snapshot))
        return cleaned if isinstance(cleaned, dict) else {}

    @staticmethod
    def _sanitize_tool_call_envelopes(envelopes):
        return [
            RoundSnapshotStore._sanitize_tool_call_envelope(envelope)
            for envelope in envelopes or []
            if isinstance(envelope, dict)
        ]

    @staticmethod
    def _sanitize_tool_call_envelope(envelope):
        data = copy.deepcopy(envelope)
        high_risk_tool = str(data.get("tool_id") or "") in HIGH_RISK_NATIVE_TOOL_IDS
        redaction_hit = False
        if "arguments" in data:
            if high_risk_tool:
                data["arguments"], redacted = (
                    RoundSnapshotStore._redact_high_risk_native_value(
                        data.get("arguments")))
            else:
                data["arguments"], redacted = RoundSnapshotStore._redact_value(
                    data.get("arguments"))
            redaction_hit = redaction_hit or redacted
        arguments_json = data.get("arguments_json")
        if isinstance(arguments_json, str) and (
            redaction_hit or RoundSnapshotStore._contains_sensitive_marker(arguments_json)
        ):
            data["arguments_json"] = "[redacted]"
        elif isinstance(arguments_json, str):
            data["arguments_json"] = RoundSnapshotStore._redact_secret_like_text(
                arguments_json)
        return data

    @staticmethod
    def _redact_high_risk_native_value(value, key_name=""):
        if RoundSnapshotStore._is_high_risk_native_redact_key(key_name):
            return "[redacted]", True
        if RoundSnapshotStore._is_sensitive_key(key_name):
            return "[redacted]", True
        if isinstance(value, dict):
            redacted = False
            result = {}
            for key, item in value.items():
                result[key], item_redacted = (
                    RoundSnapshotStore._redact_high_risk_native_value(
                        item, str(key)))
                redacted = redacted or item_redacted
            return result, redacted
        if isinstance(value, list):
            redacted = False
            result = []
            for item in value:
                cleaned, item_redacted = (
                    RoundSnapshotStore._redact_high_risk_native_value(
                        item, key_name))
                result.append(cleaned)
                redacted = redacted or item_redacted
            return result, redacted
        if isinstance(value, str):
            cleaned = RoundSnapshotStore._redact_secret_like_text(value)
            return cleaned, cleaned != value
        return value, False

    @staticmethod
    def _is_high_risk_native_redact_key(key):
        return str(key or "").lower() in HIGH_RISK_NATIVE_REDACT_KEYS

    @staticmethod
    def _redact_provider_request_body(value, key_name=""):
        key = str(key_name or "").lower()
        if (
                key in {
                    "api_key", "apikey", "authorization", "credential",
                    "credentials", "password", "secret", "access_token",
                    "refresh_token", "bearer_token",
                }
                or key.endswith((
                    "_api_key", "_authorization", "_credential",
                    "_password", "_secret",
                ))):
            return "[redacted]", True
        if isinstance(value, dict):
            redacted = False
            result = {}
            for child_key, item in value.items():
                result[child_key], item_redacted = (
                    RoundSnapshotStore._redact_provider_request_body(
                        item, child_key))
                redacted = redacted or item_redacted
            return result, redacted
        if isinstance(value, list):
            redacted = False
            result = []
            for item in value:
                cleaned, item_redacted = (
                    RoundSnapshotStore._redact_provider_request_body(
                        item, key_name))
                result.append(cleaned)
                redacted = redacted or item_redacted
            return result, redacted
        if isinstance(value, str):
            cleaned = RoundSnapshotStore._redact_secret_like_text(value)
            return cleaned, cleaned != value
        return value, False

    @staticmethod
    def _redact_value(value, key_name=""):
        if RoundSnapshotStore._is_sensitive_key(key_name):
            return "[redacted]", True
        if isinstance(value, dict):
            redacted = False
            result = {}
            for key, item in value.items():
                result[key], item_redacted = RoundSnapshotStore._redact_value(
                    item, str(key))
                redacted = redacted or item_redacted
            return result, redacted
        if isinstance(value, list):
            redacted = False
            result = []
            for item in value:
                cleaned, item_redacted = RoundSnapshotStore._redact_value(item, key_name)
                result.append(cleaned)
                redacted = redacted or item_redacted
            return result, redacted
        if isinstance(value, str):
            cleaned = RoundSnapshotStore._redact_secret_like_text(value)
            return cleaned, cleaned != value
        return value, False

    @staticmethod
    def _is_sensitive_key(key):
        text = str(key or "").lower()
        sensitive_parts = (
            "api_key",
            "apikey",
            "authorization",
            "credential",
            "password",
            "secret",
            "token",
        )
        return any(part in text for part in sensitive_parts)

    @staticmethod
    def _contains_sensitive_marker(text):
        lowered = str(text or "").lower()
        return RoundSnapshotStore._is_sensitive_key(lowered)

    @staticmethod
    def _redact_secret_like_text(text):
        return re.sub(r"sk-[A-Za-z0-9_-]{8,}", "sk-[redacted]", str(text))

    def record_llm_error(self, round_num, phase, iteration, error):
        return self.append_event(
            round_num,
            "llm_error",
            {"error": str(error)},
            phase=phase,
            iteration=iteration,
        )

    def record_parsed_result(self, round_num, phase, iteration, parsed):
        return self.append_event(
            round_num,
            "llm_output_parsed",
            parsed or {},
            phase=phase,
            iteration=iteration,
        )

    def record_step_settlement(self, round_num, phase, iteration, settlement):
        return self.append_event(
            round_num,
            "step_settlement",
            settlement or {},
            phase=phase,
            iteration=iteration,
        )

    def _has_step_snapshot(self, round_num, phase):
        for event in self._read_events_quiet(round_num):
            if (
                event.get("event_type") == "step_input_snapshot"
                and event.get("phase") == phase
            ):
                return True
        return False

    def write_snapshot(self, round_num, runtime=None, final_response_source=None,
                       final_response=None, status="closed", executed_phases=None,
                       close_round=True):
        """Close a round audit stream.

        The legacy method name is kept for Runtime call sites, but it now writes
        round_{round_num}.jsonl events instead of one combined JSON object.
        """
        steps = tuple(executed_phases or ("setup", "reaction", "cleanup"))
        for step in steps:
            step_path = os.path.join(self.context_root, step, "step.json")
            if not os.path.isfile(step_path) or self._has_step_snapshot(round_num, step):
                continue
            self.record_step_input_from_files(round_num, step)
        if runtime:
            self.append_event(round_num, "runtime_audit", dict(runtime))
        if close_round:
            return self.close_round(
                round_num,
                final_response_source=final_response_source,
                final_response=final_response,
                status=status,
            )
        return self._round_path(round_num)

    def close_round(self, round_num, final_response_source=None,
                    final_response=None, status="closed"):
        close_payload = {"status": status}
        if final_response_source:
            close_payload["final_response_source"] = final_response_source
        if final_response is not None:
            close_payload["final_response"] = final_response
        self.append_event(round_num, "round_closed", close_payload)
        try:
            self.last_retention_receipt = (
                self.retention_enforcer()
                if callable(self.retention_enforcer)
                else self.prune()
            )
        except Exception as exc:
            self.last_retention_receipt = {
                "status": "error",
                "error": str(exc),
            }
        try:
            self._write_static_projection()
        except Exception:
            pass
        return self._round_path(round_num)

    def _write_static_projection(self):
        if not self.static_projection_enabled:
            return None
        from data.round_audit_static import write_static_projection

        return write_static_projection(self._round_dir(), self.static_audit_dir)

    def read_events(self, round_num):
        path = self._round_path(round_num)
        if not os.path.isfile(path):
            raise FileNotFoundError(path)
        events = []
        decoder = RoundAuditDecoder()
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                events.append(decoder.feed(json.loads(line)))
        return events

    def list_rounds(self):
        round_dir = self._round_dir()
        if not os.path.isdir(round_dir):
            return []
        rounds = []
        for name in os.listdir(round_dir):
            match = re.match(r"^round_(\d+)\.jsonl$", name)
            if not match:
                continue
            path = os.path.join(round_dir, name)
            try:
                stat = os.stat(path)
            except OSError:
                continue
            rounds.append({
                "round": int(match.group(1)),
                "file": name,
                "size": stat.st_size,
                "mtime": datetime.fromtimestamp(
                    stat.st_mtime, timezone.utc
                ).astimezone().isoformat(timespec="seconds"),
            })
        rounds.sort(key=lambda item: item["round"])
        return rounds

    def prune(self):
        return enforce_round_retention(
            self._round_dir(),
            retention_count=self.retention_count,
            max_mib=self.retention_max_mib,
        )
