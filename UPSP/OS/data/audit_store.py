"""
审计痕迹存储 — 写入 STM/context/{step}/
DDS §19.3 全透明可审计

data 层独占文件 I/O。assembly 层通过本模块读写审计数据。
"""
import json
import hashlib
import os
from datetime import datetime

from paths import (
    STM_CTX_SETUP_DIR, STM_CTX_REACTION_DIR, STM_CTX_CLEANUP_DIR,
)
from constants import local_now
from data import atomic_write
from schemas.context import validate_audit_manifest
from errors import WriteError


CONTEXT_LAYER_ORDER = (
    ("10_permanent", "permanent", 10),
    ("20_periodic", "periodic", 20),
    ("30_lately", "lately", 30),
    ("40_high_freq", "high_freq", 40),
    ("50_now", "now", 50),
    ("60_statusbar", "statusbar", 60),
    ("99_popup", "popup", 99),
)

CALL_HEADER_LAYERS = (
    ("00_call_header", 0),
    ("01_tool_header", 1),
    ("02_generation_config", 2),
)

ALL_CONTEXT_LAYER_ORDER = (
    ("00_call_header", "call_header", 0),
    ("01_tool_header", "tool_header", 1),
    ("02_generation_config", "generation_config", 2),
    *CONTEXT_LAYER_ORDER,
)


class AuditStore:
    """审计痕迹存储（文件 I/O 唯一入口）"""

    def __init__(
        self,
        setup_dir=None,
        reaction_dir=None,
        cleanup_dir=None,
    ):
        self.step_dirs = {
            "setup":       setup_dir or STM_CTX_SETUP_DIR,
            "reaction":    reaction_dir or STM_CTX_REACTION_DIR,
            "cleanup":     cleanup_dir or STM_CTX_CLEANUP_DIR,
        }

    def write_audit(self, step, layers):
        """写入一次装配的审计痕迹"""
        ctx_dir = self.step_dirs.get(step)
        if not ctx_dir:
            return
        os.makedirs(ctx_dir, exist_ok=True)
        now = local_now().isoformat()

        # step.md 和 layers/*.md 都是 layers/*.json / step.json 的审计渲染，
        # 不作为机器源反向解析。
        file_map = {
            "step.md": layers.get("full_system", ""),
        }
        for filename, content in file_map.items():
            path = os.path.join(ctx_dir, filename)
            self._write_text_if_changed(path, content)

        for filename in ("permanent.md", "periodic.md", "high_freq.md"):
            stale_path = os.path.join(ctx_dir, filename)
            if os.path.exists(stale_path):
                os.remove(stale_path)

        layers_dir = os.path.join(ctx_dir, "layers")
        os.makedirs(layers_dir, exist_ok=True)
        layer_file_map = {
            "10_permanent.md":     layers.get("permanent", ""),
            "20_periodic.md":      layers.get("periodic", ""),
            "30_lately.md":        layers.get("lately_markdown", ""),
            "40_high_freq.md":     layers.get("high_freq", ""),
            "50_now.md":           layers.get("now_markdown", layers.get("now", "")),
            "60_statusbar.md":     layers.get("statusbar", ""),
            "99_popup.md":         layers.get("popup", ""),
        }
        stale_layer_files = (
            "00_permanent.md",
            "00_permanent.json",
            "10_periodic.md",
            "10_periodic.json",
            "20_remote_cache.md",
            "30_high_freq.md",
            "30_high_freq.json",
            "40_lately.md",
            "40_lately.json",
            "40_near_cache.md",
            "50_current_input.md",
            "50_interaction_input.md",
            "55_material_input.md",
            "60_internal_handoff.md",
            "90_statusbar.md",
            "90_statusbar.json",
        )
        for filename in stale_layer_files:
            stale_path = os.path.join(layers_dir, filename)
            if os.path.exists(stale_path):
                os.remove(stale_path)
        for filename, content in layer_file_map.items():
            path = os.path.join(layers_dir, filename)
            self._write_text_if_changed(path, self._content_for_markdown(content))
        layer_manifest = {}
        for layer_key, layer_id, order in CONTEXT_LAYER_ORDER:
            content = layers.get(layer_id, "")
            path = os.path.join(layers_dir, f"{layer_key}.json")
            payload = self._layer_payload(
                layer_key,
                layer_id,
                order,
                content,
                source="context_assembler",
                content_markdown=layers.get(f"{layer_id}_markdown"),
                block_index=layers.get(f"{layer_id}_block_index"),
            )
            if layer_id == "statusbar" and isinstance(
                    layers.get("statusbar_projection"), dict):
                payload["projection"] = layers["statusbar_projection"]
            self._validate_context_layer_payload(payload, layer_key, path)
            status = self._write_layer_json_if_changed(path, payload)
            layer_manifest[layer_id] = self._layer_manifest_entry(
                payload,
                dirty=status["dirty"],
                reused=status["reused"],
            )

        manifest = {
            "step": step,
            "assembled_at": now,
            "layer_order": [item[0] for item in CONTEXT_LAYER_ORDER],
            "layers": layer_manifest,
            "total_chars": len(layers.get("full_system", "")),
        }
        manifest_path = os.path.join(ctx_dir, "manifest.json")
        ok, errors = validate_audit_manifest(manifest)
        if not ok:
            raise WriteError(manifest_path, message=f"manifest 校验失败: {errors}")
        self._atomic_write_json(manifest_path, manifest)

    def write_call_layers(self, step, *, call, provider, endpoint, tool_header,
                          generation_config):
        ctx_dir = self.step_dirs.get(step)
        if not ctx_dir:
            return None
        os.makedirs(ctx_dir, exist_ok=True)
        layers_dir = os.path.join(ctx_dir, "layers")
        os.makedirs(layers_dir, exist_ok=True)
        layer_payloads = {
            "00_call_header": {
                "call": call if isinstance(call, dict) else {},
                "provider": provider if isinstance(provider, dict) else {},
                "endpoint": endpoint if isinstance(endpoint, dict) else {},
            },
            "01_tool_header": (
                tool_header if isinstance(tool_header, dict) else {}
            ),
            "02_generation_config": (
                generation_config if isinstance(generation_config, dict) else {}
            ),
        }
        call_layer_statuses = {}
        for layer_key, order in CALL_HEADER_LAYERS:
            path = os.path.join(layers_dir, f"{layer_key}.json")
            payload = self._layer_payload(
                layer_key,
                layer_key[3:],
                order,
                layer_payloads[layer_key],
                source="api_executor",
            )
            status = self._write_layer_json_if_changed(path, payload)
            call_layer_statuses[layer_key] = self._layer_manifest_entry(
                payload,
                dirty=status["dirty"],
                reused=status["reused"],
            )
        return call_layer_statuses

    def write_compiled_provider_request(self, step, envelope,
                                        *, call_layer_statuses=None):
        ctx_dir = self.step_dirs.get(step)
        if not ctx_dir:
            return None
        if not isinstance(envelope, dict):
            raise WriteError(os.path.join(ctx_dir, "step.json"),
                             message="provider request envelope must be dict")
        os.makedirs(ctx_dir, exist_ok=True)
        envelope = dict(envelope)
        envelope["layers_manifest"] = self._read_layers_manifest(
            ctx_dir,
            layer_statuses=call_layer_statuses or {},
        )
        self._atomic_write_json(os.path.join(ctx_dir, "step.json"), envelope)
        return envelope

    def read_context_layers(self, step):
        ctx_dir = self.step_dirs.get(step)
        if not ctx_dir:
            raise ValueError("provider_request_step_unknown")
        layers_dir = os.path.join(ctx_dir, "layers")
        payloads = []
        for layer_key, _layer_id, _order in ALL_CONTEXT_LAYER_ORDER:
            path = os.path.join(layers_dir, f"{layer_key}.json")
            if not os.path.isfile(path):
                self._raise_context_truth_corrupted(f"layer_missing:{path}")
            try:
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except (OSError, json.JSONDecodeError):
                self._raise_context_truth_corrupted(f"layer_unreadable:{path}")
            self._validate_context_layer_payload(payload, layer_key, path)
            payloads.append(payload)
        return payloads


    def read_provider_request_body(self, step):
        """读取唯一发送体；旧 messages list step.json 在活路径直接拒绝。"""
        envelope = self.read_provider_request(step)
        request_body = envelope.get("request_body")
        if not isinstance(request_body, dict):
            raise ValueError("provider_request_body_missing")
        return request_body

    def read_provider_request(self, step):
        ctx_dir = self.step_dirs.get(step)
        if not ctx_dir:
            raise ValueError("provider_request_step_unknown")
        path = os.path.join(ctx_dir, "step.json")
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except FileNotFoundError as exc:
            raise ValueError("provider_request_missing") from exc
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError("provider_request_unreadable") from exc
        if isinstance(data, list):
            raise ValueError("legacy_step_json_rejected")
        if not isinstance(data, dict) or data.get("schema") != "provider_request.v1":
            raise ValueError("provider_request_envelope_invalid")
        return data


    @classmethod
    def _layer_payload(
            cls,
            layer_key,
            layer_id,
            order,
            content,
            *,
            source="",
            content_markdown=None,
            block_index=None):
        normalized = cls._content_for_hash(content)
        payload = {
            "schema": "context_layer.v1",
            "layer_key": layer_key,
            "layer_id": layer_id,
            "order": int(order),
            "source": str(source or ""),
            "chars": cls._content_chars(content),
            "sha256": cls._sha256_text(normalized),
            "content": content,
        }
        if content_markdown is not None:
            payload["content_markdown"] = str(content_markdown or "")
            payload["model_visible_chars"] = len(payload["content_markdown"])
        if block_index is not None:
            payload["block_index"] = block_index
        return payload

    @staticmethod
    def _content_chars(content):
        if isinstance(content, str):
            return len(content)
        try:
            return len(json.dumps(content, ensure_ascii=False, sort_keys=True))
        except (TypeError, ValueError):
            return len(str(content or ""))

    @staticmethod
    def _content_for_hash(content):
        if isinstance(content, str):
            return content
        try:
            return json.dumps(
                content,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            )
        except (TypeError, ValueError):
            return str(content or "")

    @staticmethod
    def _content_for_markdown(content):
        if isinstance(content, str):
            return content
        try:
            return json.dumps(content, ensure_ascii=False, sort_keys=True, indent=2)
        except (TypeError, ValueError):
            return str(content or "")

    @staticmethod
    def _sha256_text(text):
        return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()

    @staticmethod
    def _generation_config_from_body(request_body):
        if not isinstance(request_body, dict):
            return {}
        reserved = {"messages", "input", "instructions", "system", "tools"}
        return {
            key: value
            for key, value in request_body.items()
            if key not in reserved
        }

    @classmethod
    def _layer_manifest_entry(cls, payload, *, dirty, reused):
        entry = {
            "layer_key": payload.get("layer_key"),
            "layer_id": payload.get("layer_id"),
            "order": payload.get("order"),
            "chars": payload.get("chars"),
            "sha256": payload.get("sha256"),
            "source": payload.get("source"),
            "dirty": bool(dirty),
            "reused": bool(reused),
        }
        if "model_visible_chars" in payload:
            entry["model_visible_chars"] = payload["model_visible_chars"]
        return entry

    def _write_layer_json_if_changed(self, path, payload):
        try:
            with open(path, "r", encoding="utf-8") as f:
                existing = json.load(f)
        except FileNotFoundError:
            existing = None
        except (OSError, json.JSONDecodeError):
            existing = None

        if isinstance(existing, dict) and existing == payload:
            return {"dirty": False, "reused": True}

        self._atomic_write_json(path, payload)
        return {"dirty": True, "reused": False}

    def _read_context_layer_statuses(self, ctx_dir):
        manifest_path = os.path.join(ctx_dir, "manifest.json")
        if not os.path.isfile(manifest_path):
            return {}
        try:
            with open(manifest_path, "r", encoding="utf-8") as f:
                manifest = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            self._raise_context_truth_corrupted(
                f"manifest_unreadable:{manifest_path}"
            )
        if not isinstance(manifest, dict):
            self._raise_context_truth_corrupted(
                f"manifest_schema_invalid:{manifest_path}"
            )
        layers = manifest.get("layers") if isinstance(manifest, dict) else {}
        if not isinstance(layers, dict):
            self._raise_context_truth_corrupted(
                f"manifest_layers_invalid:{manifest_path}"
            )
        statuses = {}
        for layer_key, layer_id, _order in CONTEXT_LAYER_ORDER:
            entry = layers.get(layer_id)
            if not isinstance(entry, dict):
                self._raise_context_truth_corrupted(
                    f"manifest_layer_missing:{layer_id}"
                )
            if entry.get("layer_key") != layer_key:
                self._raise_context_truth_corrupted(
                    f"manifest_layer_key_mismatch:{layer_id}"
                )
            if "dirty" not in entry or "reused" not in entry:
                self._raise_context_truth_corrupted(
                    f"manifest_layer_status_missing:{layer_id}"
                )
            statuses[str(layer_id)] = entry
        return statuses

    def _read_layers_manifest(self, ctx_dir, *, layer_statuses=None):
        layers_dir = os.path.join(ctx_dir, "layers")
        order = [item[0] for item in CALL_HEADER_LAYERS]
        order.extend(item[0] for item in CONTEXT_LAYER_ORDER)
        layers = []
        status_by_key = dict(layer_statuses or {})
        context_status_by_id = self._read_context_layer_statuses(ctx_dir)
        context_status_by_key = {
            str(entry.get("layer_key")): entry
            for entry in context_status_by_id.values()
            if isinstance(entry, dict)
        }
        for layer_key in order:
            path = os.path.join(layers_dir, f"{layer_key}.json")
            if not os.path.isfile(path):
                if layer_key in status_by_key or layer_key in context_status_by_key:
                    self._raise_context_truth_corrupted(
                        f"layer_missing:{path}"
                    )
                continue
            try:
                with open(path, "r", encoding="utf-8") as f:
                    payload = json.load(f)
            except (OSError, json.JSONDecodeError) as exc:
                self._raise_context_truth_corrupted(
                    f"layer_unreadable:{path}"
                )
            if (
                    layer_key not in status_by_key
                    and layer_key in {item[0] for item in CONTEXT_LAYER_ORDER}
                    and not context_status_by_key):
                self._raise_context_truth_corrupted(
                    f"manifest_missing_for_context_layer:{path}"
                )
            self._validate_context_layer_payload(payload, layer_key, path)
            entry = {
                "layer_key": payload.get("layer_key") or layer_key,
                "layer_id": payload.get("layer_id"),
                "order": payload.get("order"),
                "chars": payload.get("chars"),
                "sha256": payload.get("sha256"),
                "source": payload.get("source"),
            }
            if "model_visible_chars" in payload:
                entry["model_visible_chars"] = payload["model_visible_chars"]
            status = status_by_key.get(layer_key)
            if status is None:
                status = context_status_by_key.get(layer_key)
            if layer_key in status_by_key or layer_key in context_status_by_key:
                if (
                        not isinstance(status, dict)
                        or "dirty" not in status
                        or "reused" not in status):
                    self._raise_context_truth_corrupted(
                        f"layer_status_missing:{layer_key}"
                    )
            if isinstance(status, dict):
                if "dirty" in status:
                    entry["dirty"] = bool(status.get("dirty"))
                if "reused" in status:
                    entry["reused"] = bool(status.get("reused"))
            layers.append(entry)
        return {
            "layer_order": order,
            "layers": layers,
        }

    @staticmethod
    def _raise_context_truth_corrupted(detail):
        raise ValueError(f"context_truth_corrupted:{detail}")

    def _validate_context_layer_payload(self, payload, layer_key, path):
        if not isinstance(payload, dict):
            self._raise_context_truth_corrupted(f"layer_schema_invalid:{path}")
        if payload.get("schema") != "context_layer.v1":
            self._raise_context_truth_corrupted(f"layer_schema_invalid:{path}")
        if payload.get("layer_key") != layer_key:
            self._raise_context_truth_corrupted(f"layer_key_mismatch:{path}")
        for field in ("layer_id", "order", "source", "chars", "sha256", "content"):
            if field not in payload:
                self._raise_context_truth_corrupted(
                    f"layer_field_missing:{field}:{path}"
                )
        content = payload.get("content")
        expected_chars = self._content_chars(content)
        if payload.get("chars") != expected_chars:
            self._raise_context_truth_corrupted(f"layer_chars_mismatch:{path}")
        if "model_visible_chars" in payload:
            content_markdown = payload.get("content_markdown")
            if (
                    not isinstance(content_markdown, str)
                    or payload.get("model_visible_chars") != len(content_markdown)):
                self._raise_context_truth_corrupted(
                    f"layer_visible_chars_mismatch:{path}")
        expected_sha = self._sha256_text(self._content_for_hash(content))
        if payload.get("sha256") != expected_sha:
            self._raise_context_truth_corrupted(f"layer_sha_mismatch:{path}")
        block_index = payload.get("block_index")
        if block_index is None:
            return
        if not isinstance(content, str) or not isinstance(block_index, list):
            self._raise_context_truth_corrupted(f"layer_block_index_invalid:{path}")
        seen = set()
        previous_end = 0
        for block in block_index:
            if not isinstance(block, dict):
                self._raise_context_truth_corrupted(f"layer_block_invalid:{path}")
            block_id = block.get("block_id")
            start = block.get("char_start")
            end = block.get("char_end")
            if (
                    not isinstance(block_id, str)
                    or not block_id
                    or block_id in seen
                    or not isinstance(start, int)
                    or isinstance(start, bool)
                    or not isinstance(end, int)
                    or isinstance(end, bool)
                    or start < previous_end
                    or start < 0
                    or end <= start
                    or end > len(content)):
                self._raise_context_truth_corrupted(f"layer_block_invalid:{path}")
            if not isinstance(block.get("title"), str):
                self._raise_context_truth_corrupted(f"layer_block_title_invalid:{path}")
            for field in ("kind", "source_block_id"):
                if field in block and not isinstance(block.get(field), str):
                    self._raise_context_truth_corrupted(
                        f"layer_block_field_invalid:{field}:{path}"
                    )
            seen.add(block_id)
            previous_end = end

    @staticmethod
    def _write_text_if_changed(path, content):
        try:
            with open(path, "r", encoding="utf-8") as f:
                if f.read() == content:
                    return False
        except FileNotFoundError:
            pass
        except OSError:
            pass
        AuditStore._atomic_write_text(path, content)
        return True

    @staticmethod
    def _atomic_write_text(path, content):
        atomic_write.atomic_write_text(
            path,
            content,
            replace_attempts=8,
        )

    @staticmethod
    def _atomic_write_json(path, data):
        atomic_write.atomic_write_json(
            path,
            data,
            replace_attempts=8,
        )
