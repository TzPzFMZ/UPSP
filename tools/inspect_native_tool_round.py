#!/usr/bin/env python3
"""Inspect a round JSONL file for provider-native tool-call evidence."""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from paths import STM_CTX_ROUND_DIR  # noqa: E402
from data.round_audit_codec import read_round_audit_file  # noqa: E402


LEGACY_TEXT_REQUEST_MARKERS = (
    "tool_request",
    "protocol_tool_request",
    "general_tool_request",
)
LEGACY_ACTION_MARKERS = (
    "TOOL_ACTIONS_START",
    "TOOL_ACTIONS_END",
)
DEFAULT_DOGFOOD_LABEL = "读书轮"
CURRENT_USER_MESSAGE_HISTORY_KEYS = (
    "compact_reason",
    "raw_log_key",
    "raw_log_keys",
    "source_block_id",
    "source_block_ids",
    "round",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_round_dir() -> Path:
    return Path(STM_CTX_ROUND_DIR)


def _unique(values: list[str]) -> list[str]:
    seen = set()
    result = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return read_round_audit_file(path)


def _round_num_from_path(path: Path) -> int | None:
    match = re.search(r"round_(\d+)\.jsonl$", path.name)
    if not match:
        return None
    return int(match.group(1))


def find_latest_round(round_dir: str | Path | None = None) -> Path:
    directory = Path(round_dir) if round_dir else default_round_dir()
    candidates = []
    for path in directory.glob("round_*.jsonl"):
        round_num = _round_num_from_path(path)
        if round_num is None:
            continue
        candidates.append((round_num, path.stat().st_mtime, path))
    if not candidates:
        raise FileNotFoundError(f"no round_*.jsonl files under {directory}")
    candidates.sort(key=lambda item: (item[0], item[1]))
    return candidates[-1][2]


def _as_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _line_starts_with_legacy_request_marker(line: str, marker: str) -> bool:
    escaped = re.escape(marker)
    return bool(
        re.match(rf"^\|\s*{escaped}\s*(?:\||$)", line, re.IGNORECASE)
        or re.match(rf"^{escaped}\s*(?:\||:|：)", line, re.IGNORECASE)
    )


def _contains_legacy_text_request(text: str) -> bool:
    for line in str(text or "").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        upper = stripped.upper()
        if any(marker in upper for marker in LEGACY_ACTION_MARKERS):
            return True
        if any(
            _line_starts_with_legacy_request_marker(stripped, marker)
            for marker in LEGACY_TEXT_REQUEST_MARKERS
        ):
            return True
    return False


def _collect_native_outputs(value: Any) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    if isinstance(value, dict):
        outputs = value.get("native_tool_outputs")
        if isinstance(outputs, list):
            for output in outputs:
                if isinstance(output, dict):
                    call_id = str(output.get("call_id") or "")
                    if call_id:
                        collected.append({
                            "call_id": call_id,
                            "tool_id": str(output.get("tool_id") or ""),
                            "status": str(output.get("status") or ""),
                            "reason": str(output.get("reason") or ""),
                        })
        for item in value.values():
            collected.extend(_collect_native_outputs(item))
    elif isinstance(value, list):
        for item in value:
            collected.extend(_collect_native_outputs(item))
    return collected


def _collect_tool_results(value: Any) -> list[dict[str, str]]:
    collected: list[dict[str, str]] = []
    if isinstance(value, dict):
        for key, outputs in value.items():
            if key not in {
                "general_tool_results",
                "native_tool_outputs",
                "protocol_tool_receipts",
                "_protocol_tool_receipts",
            } and not str(key).endswith("_receipts"):
                continue
            if not isinstance(outputs, list):
                continue
            for output in outputs:
                if not isinstance(output, dict):
                    continue
                call_id = str(output.get("call_id") or "")
                if not call_id:
                    continue
                collected.append({
                    "call_id": call_id,
                    "tool_id": str(output.get("tool_id") or ""),
                    "status": str(output.get("status") or ""),
                    "reason": str(output.get("reason") or ""),
                })
        for item in value.values():
            collected.extend(_collect_tool_results(item))
    elif isinstance(value, list):
        for item in value:
            collected.extend(_collect_tool_results(item))
    return collected


def _collect_guide_loaded_call_ids(value: Any) -> list[str]:
    collected: list[str] = []
    if isinstance(value, dict):
        for key in ("protocol_tool_receipts", "_protocol_tool_receipts"):
            receipts = value.get(key)
            if not isinstance(receipts, list):
                continue
            for receipt in receipts:
                if not isinstance(receipt, dict):
                    continue
                if str(receipt.get("status") or "") != "guide_loaded":
                    continue
                call_id = str(receipt.get("call_id") or "")
                if call_id:
                    collected.append(call_id)
        for item in value.values():
            collected.extend(_collect_guide_loaded_call_ids(item))
    elif isinstance(value, list):
        for item in value:
            collected.extend(_collect_guide_loaded_call_ids(item))
    return collected


def _dedupe_native_outputs(
    outputs: list[dict[str, str]]
) -> list[dict[str, str]]:
    by_call_id: dict[str, dict[str, str]] = {}
    order: list[str] = []
    for output in outputs:
        call_id = output.get("call_id") or ""
        if not call_id:
            continue
        if call_id not in by_call_id:
            order.append(call_id)
        by_call_id[call_id] = output
    return [by_call_id[call_id] for call_id in order]


def _dedupe_dicts(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = set()
    result: list[dict[str, Any]] = []
    for item in items:
        key = json.dumps(item, ensure_ascii=False, sort_keys=True)
        if key in seen:
            continue
        seen.add(key)
        result.append(item)
    return result


def _settlement_digest(value: Any) -> dict[str, Any] | None:
    if not isinstance(value, dict):
        return None
    status = str(value.get("status") or "").strip()
    refs = [
        str(item).strip()
        for item in (value.get("refs") or [])
        if str(item).strip()
    ] if isinstance(value.get("refs"), list) else []
    reason = str(value.get("reason") or "").strip()
    if not status and not refs and not reason:
        return None
    return {
        "status": status,
        "refs": refs,
        "reason": reason,
    }


def _count_values(values: list[str]) -> dict[str, int]:
    counts: dict[str, int] = {}
    for value in values:
        if not value:
            continue
        counts[value] = counts.get(value, 0) + 1
    return counts


def _load_tool_definitions() -> dict[str, dict[str, Any]]:
    os_root = repo_root() / "UPSP" / "OS"
    if str(os_root) not in sys.path:
        sys.path.insert(0, str(os_root))
    try:
        from logic.protocol_tools import TOOL_DEFINITIONS  # type: ignore
    except Exception:
        return {}
    return TOOL_DEFINITIONS if isinstance(TOOL_DEFINITIONS, dict) else {}


def _tool_metadata(tool_id: str) -> dict[str, Any]:
    definitions = _load_tool_definitions()
    value = definitions.get(str(tool_id or ""))
    return value if isinstance(value, dict) else {}


def _is_step_terminal_tool(tool_id: str) -> bool:
    meta = _tool_metadata(tool_id)
    return bool(
        meta.get("native_only")
        and meta.get("step_terminal")
    )


def _is_step_runtime_tool(tool_id: str) -> bool:
    meta = _tool_metadata(tool_id)
    return bool(
        meta.get("tool_family") == "substrate_tool"
        and meta.get("native_only")
        and meta.get("step_runtime")
    )


def _current_user_message_texts(messages: Any) -> list[str]:
    if not isinstance(messages, list):
        return []
    texts: list[str] = []
    for message in messages:
        if not isinstance(message, dict):
            continue
        if str(message.get("role") or "") != "user":
            continue
        if any(key in message for key in CURRENT_USER_MESSAGE_HISTORY_KEYS):
            continue
        content = str(message.get("content") or "")
        if content:
            texts.append(content)
    return texts


def _round_payload_label_texts(payload: dict[str, Any]) -> list[str]:
    texts: list[str] = []
    for source in (payload, payload.get("input_snapshot")):
        if not isinstance(source, dict):
            continue
        for key in (
                "user_input",
                "user_message",
                "message",
                "message_text",
                "input",
                "input_text",
                "prompt",
                "dogfood_label",
                "label"):
            value = source.get(key)
            if isinstance(value, str) and value:
                texts.append(value)
    return texts


def _runtime_audit_status(payload: dict[str, Any]) -> str:
    status = str(payload.get("status") or "")
    if status:
        return status
    audit = payload.get("tool_transaction_audit")
    if isinstance(audit, dict):
        return str(audit.get("status") or "")
    runtime = payload.get("runtime")
    if isinstance(runtime, dict):
        status = str(runtime.get("status") or "")
        if status:
            return status
        audit = runtime.get("tool_transaction_audit")
        if isinstance(audit, dict):
            return str(audit.get("status") or "")
    return ""


def _collect_corrected_invalid_call_ids(payload: dict[str, Any]) -> list[str]:
    call_ids: list[str] = []

    def collect_from_list(items):
        if not isinstance(items, list):
            return
        for item in items:
            if not isinstance(item, dict):
                continue
            call_id = str(item.get("call_id") or "").strip()
            if not call_id:
                continue
            if (
                item.get("correctable_terminal_attempt")
                or str(item.get("correction_reason") or "").strip()
            ):
                call_ids.append(call_id)

    collect_from_list(payload.get("corrected_invalid_tool_requests"))
    collect_from_list(payload.get("corrected_invalid_requests"))
    audit = payload.get("tool_transaction_audit")
    if isinstance(audit, dict):
        collect_from_list(audit.get("corrected_invalid_requests"))
    runtime = payload.get("runtime")
    if isinstance(runtime, dict):
        collect_from_list(runtime.get("corrected_invalid_tool_requests"))
        audit = runtime.get("tool_transaction_audit")
        if isinstance(audit, dict):
            collect_from_list(audit.get("corrected_invalid_requests"))
    return call_ids


def inspect_round_file(
    round_file: str | Path,
    *,
    required_tools: list[str] | None = None,
    required_provider: str | None = None,
    required_tool_result_status: str | None = None,
    require_reading_dogfood: bool = False,
    dogfood_label: str = DEFAULT_DOGFOOD_LABEL,
    require_read_only_tools_only: bool = False,
    require_round_closed: bool = False,
    require_final_response: bool = False,
    require_runtime_audit_ok: bool = False,
    require_settlement_quality: bool = False,
    forbid_legacy_execution: bool = True,
) -> dict[str, Any]:
    path = Path(round_file)
    events = _read_jsonl(path)
    required_tools = list(required_tools or [])

    providers: list[str] = []
    tool_ids: list[str] = []
    call_ids: list[str] = []
    native_outputs: list[dict[str, str]] = []
    tool_results: list[dict[str, str]] = []
    envelope_tool_by_call_id: dict[str, str] = {}
    parse_statuses: dict[str, int] = {}
    legacy_sources: list[str] = []
    reaction_envelope_count = 0
    raw_tool_call_count = 0
    raw_count_seen = False
    runtime_statuses: list[str] = []
    round_closed_status = ""
    final_response = ""
    final_response_source = ""
    verified_continue_handoff = False
    dogfood_label_texts: list[str] = []
    memory_settlements: list[dict[str, Any]] = []
    read_settlements: list[dict[str, Any]] = []
    settlement_ledgers: list[dict[str, Any]] = []
    settlement_issues: list[str] = []
    guide_loaded_call_ids: list[str] = []
    corrected_invalid_call_ids: list[str] = []

    for event in events:
        event_type = str(event.get("event_type") or "")
        phase = str(event.get("phase") or "")
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        if event_type == "round_started":
            dogfood_label_texts.extend(_round_payload_label_texts(payload))
        elif event_type == "step_input_snapshot":
            dogfood_label_texts.extend(
                _current_user_message_texts(payload.get("messages"))
            )
        if event_type == "llm_output_raw" and phase == "reaction":
            iteration = str(event.get("iteration") or "")
            meta = payload.get("provider_response_meta")
            if isinstance(meta, dict):
                provider = str(meta.get("provider") or "")
                if provider:
                    providers.append(provider)
                raw_count = _as_int(meta.get("raw_tool_call_count"))
                if raw_count is not None:
                    raw_count_seen = True
                    raw_tool_call_count += raw_count
            if forbid_legacy_execution and _contains_legacy_text_request(
                str(payload.get("response") or "")
            ):
                legacy_sources.append(f"reaction:{iteration or '?'}")
            envelopes = payload.get("tool_call_envelopes")
            if isinstance(envelopes, list):
                reaction_envelope_count += len(envelopes)
                for envelope in envelopes:
                    if not isinstance(envelope, dict):
                        continue
                    provider = str(envelope.get("provider") or "")
                    if provider:
                        providers.append(provider)
                    tool_id = str(envelope.get("tool_id") or "")
                    if tool_id:
                        tool_ids.append(tool_id)
                    call_id = str(envelope.get("call_id") or "")
                    if call_id:
                        call_ids.append(call_id)
                        envelope_tool_by_call_id[call_id] = tool_id
                    parse_status = str(envelope.get("parse_status") or "missing")
                    parse_statuses[parse_status] = parse_statuses.get(parse_status, 0) + 1
        elif event_type == "llm_output_parsed" and phase == "reaction":
            iteration = str(event.get("iteration") or "")
            for item in payload.get("invalid_tool_requests", []) or []:
                if not isinstance(item, dict):
                    continue
                if str(item.get("reason") or "") == "legacy_tool_request_retired":
                    legacy_sources.append(f"parsed:{iteration or '?'}")
            if str(payload.get("memory_no_write_reason") or "").strip():
                settlement_issues.append("memory_no_write_reason_retired")
            memory_settlement = _settlement_digest(
                payload.get("memory_settlement")
            )
            if memory_settlement:
                memory_settlements.append(memory_settlement)
            read_settlement = _settlement_digest(payload.get("read_settlement"))
            if read_settlement:
                read_settlements.append(read_settlement)
            for error in payload.get("reaction_finalize_errors") or []:
                if error:
                    settlement_issues.append(str(error))
        ledgers = payload.get("settlement_ledgers")
        if isinstance(ledgers, list):
            settlement_ledgers.extend(
                item for item in ledgers if isinstance(item, dict)
            )
        if event_type == "runtime_audit":
            status = _runtime_audit_status(payload)
            if status:
                runtime_statuses.append(status)
        elif event_type == "round_closed":
            round_closed_status = str(payload.get("status") or "")
            final_response = str(payload.get("final_response") or "").strip()
            final_response_source = str(payload.get("final_response_source") or "").strip()
            if final_response_source == "reaction.continue_handoff":
                verified_continue_handoff = True
        elif event_type == "heartbeat_rearm":
            status = str(payload.get("status") or "").strip()
            flags = {str(flag) for flag in payload.get("set_flags") or []}
            if "continue_requested" in flags:
                if status == "continue_requested_rearmed":
                    relay_intent = payload.get("relay_intent")
                    verified_continue_handoff = verified_continue_handoff or bool(
                        isinstance(relay_intent, dict)
                        and str(relay_intent.get("status") or "").strip() == "open"
                    )
                elif status == "continue_requested_rearmed_from_open_relay_intents":
                    verified_continue_handoff = verified_continue_handoff or bool(
                        payload.get("open_relay_intent_ids")
                    )

        native_outputs.extend(_collect_native_outputs(payload))
        tool_results.extend(_collect_tool_results(payload))
        guide_loaded_call_ids.extend(_collect_guide_loaded_call_ids(payload))
        corrected_invalid_call_ids.extend(_collect_corrected_invalid_call_ids(payload))

    providers = _unique(providers)
    tool_ids = _unique(tool_ids)
    call_ids = _unique(call_ids)
    native_outputs = _dedupe_native_outputs(native_outputs)
    tool_results = _dedupe_native_outputs(tool_results)
    native_output_call_ids = _unique([
        output.get("call_id") or "" for output in native_outputs
    ])
    tool_result_by_call_id = {
        result.get("call_id") or "": result for result in tool_results
    }
    tool_result_call_ids = _unique([
        result.get("call_id") or "" for result in tool_results
    ])
    guide_loaded_call_ids = _unique(guide_loaded_call_ids)
    corrected_invalid_call_ids = _unique(corrected_invalid_call_ids)
    tool_result_statuses = _count_values([
        result.get("status") or "" for result in tool_results
    ])
    tool_result_reasons = _count_values([
        result.get("reason") or "" for result in tool_results
    ])
    step_terminal_call_ids = [
        call_id for call_id in call_ids
        if _is_step_terminal_tool(envelope_tool_by_call_id.get(call_id, ""))
    ]
    step_runtime_call_ids = [
        call_id for call_id in call_ids
        if _is_step_runtime_tool(envelope_tool_by_call_id.get(call_id, ""))
    ]
    ordinary_call_ids = [
        call_id for call_id in call_ids
        if (
            call_id not in step_terminal_call_ids
            and call_id not in step_runtime_call_ids
        )
    ]
    guide_receipt_matched_call_ids = [
        call_id for call_id in ordinary_call_ids
        if envelope_tool_by_call_id.get(call_id) == "protocol_tool_guide_request"
        and call_id in guide_loaded_call_ids
    ]
    corrected_invalid_matched_call_ids = [
        call_id for call_id in ordinary_call_ids
        if call_id in corrected_invalid_call_ids
    ]
    matched_call_ids = [
        call_id for call_id in ordinary_call_ids
        if (
            call_id in tool_result_call_ids
            or call_id in guide_receipt_matched_call_ids
            or call_id in corrected_invalid_matched_call_ids
        )
    ]
    unmatched_call_ids = [
        call_id for call_id in ordinary_call_ids
        if (
            call_id not in tool_result_call_ids
            and call_id not in guide_receipt_matched_call_ids
            and call_id not in corrected_invalid_matched_call_ids
        )
    ]
    raw_mismatch = bool(
        raw_count_seen and raw_tool_call_count != reaction_envelope_count
    )
    tool_classes: dict[str, str] = {}
    tool_families: dict[str, str] = {}
    read_only_tool_ids: list[str] = []
    non_read_only_tool_ids: list[str] = []
    step_terminal_tool_ids: list[str] = []
    for tool_id in tool_ids:
        meta = _tool_metadata(tool_id)
        tool_class = str(meta.get("tool_class") or "")
        tool_family = str(meta.get("tool_family") or "")
        if tool_class:
            tool_classes[tool_id] = tool_class
        if tool_family:
            tool_families[tool_id] = tool_family
        if _is_step_terminal_tool(tool_id):
            step_terminal_tool_ids.append(tool_id)
            continue
        if tool_class == "read_tool":
            read_only_tool_ids.append(tool_id)
        else:
            non_read_only_tool_ids.append(tool_id)
    read_only_tool_ids = _unique(read_only_tool_ids)
    non_read_only_tool_ids = _unique(non_read_only_tool_ids)
    step_terminal_tool_ids = _unique(step_terminal_tool_ids)
    dogfood_label = str(dogfood_label or DEFAULT_DOGFOOD_LABEL)
    reading_dogfood = any(
        dogfood_label in text for text in dogfood_label_texts
    )

    issues: list[str] = []
    for tool_id in required_tools:
        if tool_id not in tool_ids:
            issues.append(f"missing_required_tool:{tool_id}")
    if required_provider and required_provider not in providers:
        issues.append(f"missing_required_provider:{required_provider}")
    if require_round_closed and round_closed_status != "closed":
        issues.append(f"round_not_closed:{round_closed_status or 'missing'}")
    if require_final_response and not final_response and not verified_continue_handoff:
        issues.append("final_response_empty")
    runtime_audit_status = runtime_statuses[-1] if runtime_statuses else ""
    if require_runtime_audit_ok and runtime_audit_status != "ok":
        issues.append(f"runtime_audit_not_ok:{runtime_audit_status or 'missing'}")
    if raw_mismatch:
        issues.append("raw_tool_call_count_mismatch")
    for call_id in unmatched_call_ids:
        issues.append(f"unmatched_tool_result:{call_id}")
    if require_reading_dogfood and not reading_dogfood:
        issues.append(f"reading_dogfood_label_missing:{dogfood_label}")
    if require_read_only_tools_only:
        for tool_id in non_read_only_tool_ids:
            issues.append(f"non_read_only_tool_seen:{tool_id}")
    if required_tool_result_status:
        scoped_tools = set(required_tools)
        for call_id in call_ids:
            tool_id = envelope_tool_by_call_id.get(call_id, "")
            if _is_step_terminal_tool(tool_id) or _is_step_runtime_tool(tool_id):
                continue
            if scoped_tools and tool_id not in scoped_tools:
                continue
            output = tool_result_by_call_id.get(call_id)
            status = str(output.get("status") or "missing") if output else "missing"
            if status != required_tool_result_status:
                issues.append(
                    "tool_result_status_not_"
                    f"{required_tool_result_status}:{tool_id or 'unknown'}:"
                    f"{call_id}:{status}"
                )
    if forbid_legacy_execution and legacy_sources:
        issues.append("legacy_text_request_seen")
    settlement_issues = _unique(settlement_issues)
    if require_settlement_quality:
        issues.extend(settlement_issues)

    summary = {
        "round_file": str(path),
        "round_num": _round_num_from_path(path),
        "providers": providers,
        "tool_ids": tool_ids,
        "call_ids": call_ids,
        "native_output_call_ids": native_output_call_ids,
        "tool_result_call_ids": tool_result_call_ids,
        "guide_loaded_call_ids": guide_loaded_call_ids,
        "guide_receipt_matched_call_ids": guide_receipt_matched_call_ids,
        "corrected_invalid_call_ids": corrected_invalid_matched_call_ids,
        "matched_call_ids": matched_call_ids,
        "unmatched_envelope_call_ids": unmatched_call_ids,
        "step_terminal_tool_ids": step_terminal_tool_ids,
        "step_terminal_call_ids": step_terminal_call_ids,
        "tool_result_statuses": tool_result_statuses,
        "tool_result_reasons": tool_result_reasons,
        "dogfood_label": dogfood_label,
        "dogfood_label_texts": _unique(dogfood_label_texts),
        "reading_dogfood": reading_dogfood,
        "read_only_tool_ids": read_only_tool_ids,
        "non_read_only_tool_ids": non_read_only_tool_ids,
        "tool_classes": tool_classes,
        "tool_families": tool_families,
        "parse_statuses": parse_statuses,
        "raw_tool_call_count": raw_tool_call_count,
        "raw_tool_call_count_mismatch": raw_mismatch,
        "runtime_audit_status": runtime_audit_status,
        "round_closed_status": round_closed_status,
        "final_response": final_response,
        "final_response_source": final_response_source,
        "verified_continue_handoff": verified_continue_handoff,
        "memory_settlements": _dedupe_dicts(memory_settlements),
        "read_settlements": _dedupe_dicts(read_settlements),
        "settlement_ledgers": _dedupe_dicts(settlement_ledgers),
        "settlement_issues": settlement_issues,
        "legacy_text_requests_seen": bool(legacy_sources),
        "legacy_text_request_sources": _unique(legacy_sources),
        "ok": not issues,
        "issues": issues,
    }
    return summary


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Inspect a UPSP round JSONL for provider-native tool-call evidence."
    )
    parser.add_argument("--round-file", help="Path to a round_N.jsonl file.")
    parser.add_argument(
        "--latest",
        action="store_true",
        help="Inspect the latest round_N.jsonl in --round-dir.",
    )
    parser.add_argument(
        "--round-dir",
        default=str(default_round_dir()),
        help="Directory containing round_N.jsonl files.",
    )
    parser.add_argument(
        "--require-tool",
        action="append",
        default=[],
        help="Require a provider-native envelope for this tool_id. Repeatable.",
    )
    parser.add_argument("--require-provider", help="Require this provider.")
    parser.add_argument(
        "--require-tool-result-status",
        help="Require matched native tool results to report this status.",
    )
    parser.add_argument(
        "--require-reading-dogfood",
        action="store_true",
        help="Require the round to carry the reading dogfood label.",
    )
    parser.add_argument(
        "--require-dogfood-label",
        default=DEFAULT_DOGFOOD_LABEL,
        help="Reading dogfood label to require when --require-reading-dogfood is set.",
    )
    parser.add_argument(
        "--require-read-only-tools-only",
        action="store_true",
        help="Reject provider-native tool calls whose registry tool_class is not read_tool.",
    )
    parser.add_argument("--require-round-closed", action="store_true")
    parser.add_argument("--require-final-response", action="store_true")
    parser.add_argument("--require-runtime-audit-ok", action="store_true")
    parser.add_argument(
        "--require-settlement-quality",
        action="store_true",
        help="Reject retired or invalid reaction_finalize settlement fields.",
    )
    parser.add_argument(
        "--forbid-legacy-execution",
        action="store_true",
        default=True,
        help="Reject current reaction legacy text request execution. Enabled by default.",
    )
    parser.add_argument(
        "--allow-legacy-execution",
        action="store_false",
        dest="forbid_legacy_execution",
        help="Do not reject current reaction legacy text request markers.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    round_file = Path(args.round_file) if args.round_file else None
    if args.latest or round_file is None:
        round_file = find_latest_round(args.round_dir)
    summary = inspect_round_file(
        round_file,
        required_tools=args.require_tool,
        required_provider=args.require_provider,
        required_tool_result_status=args.require_tool_result_status,
        require_reading_dogfood=args.require_reading_dogfood,
        dogfood_label=args.require_dogfood_label,
        require_read_only_tools_only=args.require_read_only_tools_only,
        require_round_closed=args.require_round_closed,
        require_final_response=args.require_final_response,
        require_runtime_audit_ok=args.require_runtime_audit_ok,
        require_settlement_quality=args.require_settlement_quality,
        forbid_legacy_execution=args.forbid_legacy_execution,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
