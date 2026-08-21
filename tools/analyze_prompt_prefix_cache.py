"""Estimate prompt prefix stability from UPSP round audit snapshots."""
from __future__ import annotations

import argparse
from collections import defaultdict
import json
import re
import statistics
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from data.prompt_cache_telemetry import (  # noqa: E402
    extract_prompt_cache_telemetry as _extract_prompt_cache_telemetry_v2,
)
from data.round_audit_codec import read_round_audit_file  # noqa: E402
from paths import STM_CTX_ROUND_DIR  # noqa: E402

DEFAULT_ROUND_DIR = Path(STM_CTX_ROUND_DIR)
CALLS_JSONL_NAME = "prefix_cache_calls.jsonl"
SUMMARY_JSON_NAME = "prefix_cache_summary.json"
SUMMARY_MD_NAME = "prefix_cache_summary.md"


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    return read_round_audit_file(path)


def _round_num_from_path(path: Path) -> int:
    match = re.search(r"round_(\d+)\.jsonl$", path.name)
    return int(match.group(1)) if match else -1


def _event_int(event: dict[str, Any], key: str, fallback: int = 0) -> int:
    try:
        return int(event.get(key))
    except (TypeError, ValueError):
        return fallback


def _safe_messages(payload: dict[str, Any]) -> list[Any]:
    messages = payload.get("messages")
    return list(messages) if isinstance(messages, list) else []


def _provider_request_envelope(payload: dict[str, Any]) -> dict[str, Any]:
    envelope = payload.get("provider_request_envelope")
    return envelope if isinstance(envelope, dict) else {}


def _request_body_from_envelope(payload: dict[str, Any]) -> dict[str, Any]:
    envelope = _provider_request_envelope(payload)
    request_body = envelope.get("request_body")
    return request_body if isinstance(request_body, dict) else {}


def _request_body_message_count(request_body: dict[str, Any]) -> int:
    messages = request_body.get("messages")
    if isinstance(messages, list):
        return len(messages)
    input_value = request_body.get("input")
    if isinstance(input_value, list):
        return len(input_value)
    if isinstance(input_value, str) and input_value:
        return 1
    return 0


def canonical_payload(payload: dict[str, Any]) -> str:
    """Return a stable character representation of provider-visible input."""
    request_body = _request_body_from_envelope(payload)
    if request_body:
        return json.dumps(
            request_body,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    canonical = {
        "system": payload.get("system") if payload.get("system") is not None else "",
        "messages": _safe_messages(payload),
    }
    return json.dumps(
        canonical,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def common_prefix_len(left: str, right: str) -> int:
    limit = min(len(left), len(right))
    for index in range(limit):
        if left[index] != right[index]:
            return index
    return limit


def _ratio(numerator: int, denominator: int) -> float:
    if denominator <= 0:
        return 0.0
    return numerator / denominator


def _usage_int(*values: Any) -> int:
    for value in values:
        try:
            if value in (None, ""):
                continue
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def extract_prompt_cache_telemetry(
    raw_usage: Any,
    request_contract_audit: Any = None,
) -> dict[str, Any]:
    return _extract_prompt_cache_telemetry_v2(
        raw_usage,
        request_contract_audit=request_contract_audit,
    )


def _provider_from_payload(
    payload: dict[str, Any],
    envelope: dict[str, Any],
    audit: dict[str, Any],
) -> str:
    if audit.get("provider"):
        return str(audit.get("provider") or "")
    meta = payload.get("provider_response_meta")
    if isinstance(meta, dict) and meta.get("provider"):
        return str(meta.get("provider") or "")
    provider_meta = envelope.get("provider")
    if isinstance(provider_meta, dict):
        return str(provider_meta.get("provider") or "")
    return ""


def _model_from_payload(
    payload: dict[str, Any],
    envelope: dict[str, Any],
    audit: dict[str, Any],
    request_body: dict[str, Any],
) -> str:
    if audit.get("model"):
        return str(audit.get("model") or "")
    if payload.get("model"):
        return str(payload.get("model") or "")
    provider_meta = envelope.get("provider")
    if isinstance(provider_meta, dict) and provider_meta.get("model"):
        return str(provider_meta.get("model") or "")
    if request_body.get("model"):
        return str(request_body.get("model") or "")
    return ""


def _api_shape(request_body: dict[str, Any]) -> str:
    if "messages" in request_body:
        return "chat"
    if "input" in request_body:
        return "responses"
    return ""


def _tool_names_from_request_body(request_body: dict[str, Any]) -> list[str]:
    tools = request_body.get("tools")
    if not isinstance(tools, list):
        return []
    names: list[str] = []
    for tool in tools:
        if not isinstance(tool, dict):
            continue
        function = tool.get("function")
        if isinstance(function, dict) and function.get("name"):
            names.append(str(function.get("name")))
            continue
        if tool.get("name"):
            names.append(str(tool.get("name")))
    return sorted(name for name in names if name)


def _toolset_fingerprint(tool_names: list[str]) -> str:
    if not tool_names:
        return ""
    return ",".join(sorted(str(name) for name in tool_names if str(name)))


def _layer_map(payload: dict[str, Any]) -> dict[str, str]:
    snapshot = payload.get("layers_snapshot")
    if not isinstance(snapshot, dict):
        return {}
    layers = snapshot.get("layers")
    if not isinstance(layers, list):
        return {}
    result: dict[str, str] = {}
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        key = str(layer.get("layer_key") or "").strip()
        if not key:
            continue
        digest = str(layer.get("sha256") or "").strip()
        if not digest and layer.get("content") is not None:
            digest = str(layer.get("content"))
        result[key] = digest
    return result


def _changed_layers(
    previous_layers: dict[str, str],
    current_layers: dict[str, str],
) -> tuple[list[str], list[str]]:
    ordered = [
        "00_call_header",
        "01_tool_header",
        "02_generation_config",
        "10_permanent",
        "20_periodic",
        "30_lately",
        "40_high_freq",
        "50_now",
        "60_statusbar",
        "99_popup",
    ]
    keys = [key for key in ordered if key in previous_layers or key in current_layers]
    extras = sorted(
        set(previous_layers) | set(current_layers) | set(keys)
    )
    for key in extras:
        if key not in keys:
            keys.append(key)
    changed = [
        key for key in keys
        if previous_layers.get(key) != current_layers.get(key)
    ]
    return keys, changed


def _round_files_from_summary(summary_path: Path, round_dir: Path) -> list[Path]:
    data = _read_json(summary_path)
    results = data.get("results") if isinstance(data, dict) else []
    files: list[Path] = []
    seen: set[Path] = set()
    for item in results if isinstance(results, list) else []:
        if not isinstance(item, dict):
            continue
        round_name = str(item.get("round_name") or "").strip()
        if not round_name:
            continue
        path = Path(round_name)
        if not path.is_absolute():
            path = round_dir / round_name
        path = path.resolve()
        if path in seen:
            continue
        seen.add(path)
        files.append(path)
    return files


def _list_round_files(round_dir: Path) -> list[Path]:
    if not round_dir.is_dir():
        return []
    return sorted(
        (path for path in round_dir.glob("round_*.jsonl") if path.is_file()),
        key=lambda path: (_round_num_from_path(path), path.name),
    )


def select_round_files(
    *,
    round_files: list[Path] | None = None,
    round_dir: Path | None = None,
    recent: int | None = None,
    from_round: int | None = None,
    to_round: int | None = None,
    summary_path: Path | None = None,
) -> list[Path]:
    base_round_dir = Path(round_dir or DEFAULT_ROUND_DIR)
    selected: list[Path]
    if summary_path is not None:
        selected = _round_files_from_summary(Path(summary_path), base_round_dir)
    elif round_files:
        selected = [Path(path) for path in round_files]
    else:
        selected = _list_round_files(base_round_dir)

    if from_round is not None:
        selected = [path for path in selected if _round_num_from_path(path) >= int(from_round)]
    if to_round is not None:
        selected = [path for path in selected if _round_num_from_path(path) <= int(to_round)]

    selected = sorted(
        selected,
        key=lambda path: (_round_num_from_path(Path(path)), Path(path).name),
    )
    if recent is not None and int(recent) > 0:
        selected = selected[-int(recent):]
    return selected


def _extract_snapshot_calls(round_file: Path) -> list[dict[str, Any]]:
    round_file = Path(round_file)
    path_round_num = _round_num_from_path(round_file)
    calls: list[dict[str, Any]] = []
    envelope_calls: dict[tuple[int, str, int, str], dict[str, Any]] = {}
    events = _read_jsonl(round_file)
    has_provider_envelopes = any(
        _request_body_from_envelope(
            event.get("payload")
            if isinstance(event.get("payload"), dict)
            else {}
        )
        for event in events
        if str(event.get("event_type") or "") in {"llm_call_started", "llm_output_raw"}
    )
    for event in events:
        event_type = str(event.get("event_type") or "")
        if event_type not in {
                "step_input_snapshot", "llm_call_started", "llm_output_raw"}:
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        round_num = _event_int(event, "round", path_round_num)
        event_index = _event_int(event, "event_index", len(calls) + 1)
        phase = str(event.get("phase") or "")
        iteration = _event_int(event, "iteration", 1)
        request_body = _request_body_from_envelope(payload)
        envelope = _provider_request_envelope(payload)
        audit = payload.get("request_contract_audit")
        audit = audit if isinstance(audit, dict) else {}
        call = envelope.get("call") if isinstance(envelope.get("call"), dict) else {}
        call_channel = str(call.get("channel") or "").strip()
        call_attempt = call.get("attempt")
        attempt_key = ""
        if call_attempt not in (None, ""):
            attempt_key = str(call_attempt)
        if not request_body and has_provider_envelopes:
            continue
        if not request_body and event_type != "step_input_snapshot":
            continue
        canonical = canonical_payload(payload)
        message_count = (
            _request_body_message_count(request_body)
            if request_body
            else len(_safe_messages(payload))
        )
        tool_names = audit.get("tool_names")
        if not isinstance(tool_names, list):
            tool_names = _tool_names_from_request_body(request_body)
        else:
            tool_names = sorted(str(name) for name in tool_names if str(name))
        telemetry = payload.get("prompt_cache_telemetry")
        if not isinstance(telemetry, dict):
            telemetry = extract_prompt_cache_telemetry(
                payload.get("raw_usage"),
                audit,
            )
        layer_map = _layer_map(payload)
        call_record = {
            "_canonical": canonical,
            "_sort_key": (round_num, event_index),
            "_layers": layer_map,
            "round_name": round_file.name,
            "round_num": round_num,
            "event_index": event_index,
            "phase": phase,
            "iteration": iteration,
            "source_event_type": event_type,
            "call_channel": call_channel,
            "request_body_sha256": envelope.get("request_body_sha256") or "",
            "provider": _provider_from_payload(payload, envelope, audit),
            "model": _model_from_payload(payload, envelope, audit, request_body),
            "api_shape": _api_shape(request_body),
            "prompt_cache_lane": str(
                audit.get("prompt_cache_lane")
                or telemetry.get("prompt_cache_lane")
                or ""
            ),
            "prompt_cache_key": str(audit.get("prompt_cache_key") or ""),
            "prompt_cache_key_applied": bool(audit.get("prompt_cache_key_applied")),
            "prompt_cache_mode": str(telemetry.get("prompt_cache_mode") or ""),
            "breakpoint_strategy": str(
                telemetry.get("breakpoint_strategy") or ""
            ),
            "breakpoint_targets": list(
                telemetry.get("breakpoint_targets") or []
            ),
            "prefix_fingerprint": str(
                telemetry.get("prefix_fingerprint") or ""
            ),
            "lately_epoch": str(telemetry.get("lately_epoch") or ""),
            "tool_names": tool_names,
            "toolset_fingerprint": _toolset_fingerprint(tool_names),
            "prompt_cache_telemetry": telemetry,
            "message_count": message_count,
            "total_chars": len(canonical),
            "historical": bool(not request_body and event_type == "step_input_snapshot"),
        }
        if request_body:
            envelope_key = (
                round_num,
                phase,
                iteration,
                call_channel,
                attempt_key,
            )
            existing = envelope_calls.get(envelope_key)
            if existing is not None:
                issues = list(existing.get("_issues") or [])
                previous_sha = str(existing.get("request_body_sha256") or "")
                current_sha = str(call_record.get("request_body_sha256") or "")
                if previous_sha and current_sha and previous_sha != current_sha:
                    issue = (
                        "provider_request_envelope_mismatch:"
                        f"{round_file.name}:{phase}:{iteration}:{call_channel}"
                    )
                    if issue not in issues:
                        issues.append(issue)
                if (
                        event_type == "llm_output_raw"
                        and existing.get("source_event_type") != "llm_output_raw"):
                    if not call_record.get("_layers"):
                        call_record["_layers"] = existing.get("_layers") or {}
                    call_record["_issues"] = issues
                    envelope_calls[envelope_key] = call_record
                else:
                    if not existing.get("_layers") and call_record.get("_layers"):
                        existing["_layers"] = call_record.get("_layers") or {}
                    existing["_issues"] = issues
                continue
            envelope_calls[envelope_key] = call_record
            continue
        calls.append(call_record)
    if envelope_calls:
        calls.extend(envelope_calls.values())
    return calls


def _percentile(values: list[float], percent: float) -> float:
    if not values:
        return 0.0
    if len(values) == 1:
        return values[0]
    ordered = sorted(values)
    index = (len(ordered) - 1) * percent
    lower = int(index)
    upper = min(lower + 1, len(ordered) - 1)
    if lower == upper:
        return ordered[lower]
    weight = index - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _ratio_stats(calls: list[dict[str, Any]]) -> dict[str, Any]:
    compared = [call for call in calls if call.get("previous_round_name")]
    compared_chars = sum(int(call.get("total_chars") or 0) for call in compared)
    prefix_chars = sum(int(call.get("common_prefix_chars") or 0) for call in compared)
    ratios = [float(call.get("prefix_ratio") or 0.0) for call in compared]
    return {
        "compared_call_count": len(compared),
        "weighted_prefix_ratio": _ratio(prefix_chars, compared_chars),
        "mean_prefix_ratio": statistics.fmean(ratios) if ratios else 0.0,
        "median_prefix_ratio": statistics.median(ratios) if ratios else 0.0,
        "min_prefix_ratio": min(ratios) if ratios else 0.0,
        "max_prefix_ratio": max(ratios) if ratios else 0.0,
        "p10_prefix_ratio": _percentile(ratios, 0.10),
        "p90_prefix_ratio": _percentile(ratios, 0.90),
    }


def _phase_summary(calls: list[dict[str, Any]]) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for call in calls:
        grouped[str(call.get("phase") or "")].append(call)
    summary: dict[str, Any] = {}
    for phase, phase_calls in sorted(grouped.items()):
        stats = _ratio_stats(phase_calls)
        summary[phase or "unknown"] = {
            "call_count": len(phase_calls),
            "total_chars": sum(int(call.get("total_chars") or 0) for call in phase_calls),
            **stats,
        }
    return summary


def _simple_cache_group_summary(calls: list[dict[str, Any]]) -> dict[str, Any]:
    prompt_tokens = 0
    cached_tokens = 0
    cache_write_tokens = 0
    unclassified_prompt_tokens = 0
    zero_cache = 0
    nonzero_cache = 0
    write_status_counts: dict[str, int] = defaultdict(int)
    for call in calls:
        telemetry = call.get("prompt_cache_telemetry")
        if not isinstance(telemetry, dict):
            write_status_counts["not_reported"] += 1
            continue
        call_prompt = _usage_int(telemetry.get("prompt_tokens"))
        call_cached = _usage_int(
            telemetry.get("cache_read_tokens"),
            telemetry.get("cached_tokens"),
        )
        call_write = _usage_int(telemetry.get("cache_write_tokens"))
        prompt_tokens += call_prompt
        cached_tokens += call_cached
        cache_write_tokens += call_write
        if "unclassified_prompt_tokens" in telemetry:
            unclassified_prompt_tokens += _usage_int(
                telemetry.get("unclassified_prompt_tokens")
            )
        else:
            unclassified_prompt_tokens += max(
                call_prompt - call_cached - call_write,
                0,
            )
        write_status_counts[
            str(telemetry.get("cache_write_status") or "not_reported")
        ] += 1
        if call_prompt > 0 and call_cached > 0:
            nonzero_cache += 1
        elif call_prompt > 0:
            zero_cache += 1
    return {
        "call_count": len(calls),
        "prompt_tokens": prompt_tokens,
        "cached_tokens": cached_tokens,
        "cache_read_tokens": cached_tokens,
        "cache_write_tokens": cache_write_tokens,
        "unclassified_prompt_tokens": unclassified_prompt_tokens,
        "cache_hit_ratio": round(_ratio(cached_tokens, prompt_tokens), 6),
        "cache_write_ratio": round(_ratio(cache_write_tokens, prompt_tokens), 6),
        "zero_cache_call_count": zero_cache,
        "nonzero_cache_call_count": nonzero_cache,
        "cache_write_status_counts": dict(sorted(write_status_counts.items())),
        "cache_accounting_complete": bool(calls)
        and write_status_counts.get("not_reported", 0) == 0,
    }


def _group_summary(calls: list[dict[str, Any]], key: str) -> dict[str, Any]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for call in calls:
        value = str(call.get(key) or "")
        if not value:
            continue
        grouped[value].append(call)
    return {
        value: _simple_cache_group_summary(items)
        for value, items in sorted(grouped.items())
    }


def _layer_churn_summary(calls: list[dict[str, Any]]) -> dict[str, Any]:
    by_layer: dict[str, dict[str, int]] = {}
    compared_count = 0
    for call in calls:
        layers = call.get("same_lane_compared_layers")
        if not isinstance(layers, list) or not layers:
            continue
        compared_count += 1
        changed = set(call.get("same_lane_changed_layers") or [])
        for layer in layers:
            layer_key = str(layer)
            stats = by_layer.setdefault(
                layer_key,
                {"compared_count": 0, "changed_count": 0},
            )
            stats["compared_count"] += 1
            if layer_key in changed:
                stats["changed_count"] += 1
    return {
        "same_lane_compared_call_count": compared_count,
        "by_layer": by_layer,
    }


def _prompt_cache_key_hygiene_issues(calls: list[dict[str, Any]]) -> list[str]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for call in calls:
        key = str(call.get("prompt_cache_key") or "")
        if key:
            grouped[key].append(call)
    issues: list[str] = []
    for key, key_calls in sorted(grouped.items()):
        if len({str(call.get("provider") or "") for call in key_calls}) > 1:
            issues.append(f"prompt_cache_key_crosses_provider:{key}")
        if len({str(call.get("model") or "") for call in key_calls}) > 1:
            issues.append(f"prompt_cache_key_crosses_model:{key}")
        if len({str(call.get("api_shape") or "") for call in key_calls}) > 1:
            issues.append(f"prompt_cache_key_crosses_api_shape:{key}")
        if len({str(call.get("toolset_fingerprint") or "") for call in key_calls}) > 1:
            issues.append(f"prompt_cache_key_crosses_toolset:{key}")
    return issues


def _build_summary(calls: list[dict[str, Any]], round_files: list[Path], issues: list[str]) -> dict[str, Any]:
    stats = _ratio_stats(calls)
    total_chars = sum(int(call.get("total_chars") or 0) for call in calls)
    return {
        "schema_version": "prompt_prefix_cache_summary.v1",
        "round_files": [Path(path).name for path in round_files],
        "call_count": len(calls),
        "total_chars": total_chars,
        **stats,
        "by_phase": _phase_summary(calls),
        "by_call_channel": _group_summary(calls, "call_channel"),
        "by_prompt_cache_key": _group_summary(calls, "prompt_cache_key"),
        "by_lately_epoch": _group_summary(calls, "lately_epoch"),
        "by_request_body_sha256": _group_summary(calls, "request_body_sha256"),
        "provider_cache": _simple_cache_group_summary(calls),
        "layer_churn": _layer_churn_summary(calls),
        "issues": list(issues),
    }


def analyze_round_files(round_files: list[Path]) -> dict[str, Any]:
    existing = [Path(path) for path in round_files if Path(path).is_file()]
    missing = [Path(path) for path in round_files if not Path(path).is_file()]
    calls: list[dict[str, Any]] = []
    for round_file in existing:
        calls.extend(_extract_snapshot_calls(round_file))
    calls.sort(key=lambda call: call["_sort_key"])

    issues: list[str] = []
    previous: dict[str, Any] | None = None
    previous_by_phase: dict[str, dict[str, Any]] = {}
    previous_by_lane: dict[str, dict[str, Any]] = {}
    for call in calls:
        for issue in call.pop("_issues", []) or []:
            if issue not in issues:
                issues.append(issue)
        canonical = str(call.pop("_canonical"))
        layers = call.pop("_layers", {}) or {}
        call.pop("_sort_key", None)
        if previous is None:
            call.update(
                {
                    "previous_round_name": "",
                    "previous_phase": "",
                    "previous_iteration": 0,
                    "common_prefix_chars": 0,
                    "prefix_ratio": 0.0,
                    "changed_suffix_chars": int(call["total_chars"]),
                }
            )
        else:
            common = common_prefix_len(str(previous["_canonical"]), canonical)
            call.update(
                {
                    "previous_round_name": previous["round_name"],
                    "previous_phase": previous["phase"],
                    "previous_iteration": previous["iteration"],
                    "common_prefix_chars": common,
                    "prefix_ratio": _ratio(common, int(call["total_chars"])),
                    "changed_suffix_chars": int(call["total_chars"]) - common,
                }
            )

        phase = str(call.get("phase") or "")
        same_phase_previous = previous_by_phase.get(phase)
        if same_phase_previous is None:
            call.update(
                {
                    "same_phase_previous_round_name": "",
                    "same_phase_previous_iteration": 0,
                    "same_phase_common_prefix_chars": 0,
                    "same_phase_prefix_ratio": 0.0,
                }
            )
        else:
            same_phase_common = common_prefix_len(
                str(same_phase_previous["_canonical"]),
                canonical,
            )
            call.update(
                {
                    "same_phase_previous_round_name": same_phase_previous["round_name"],
                    "same_phase_previous_iteration": same_phase_previous["iteration"],
                    "same_phase_common_prefix_chars": same_phase_common,
                    "same_phase_prefix_ratio": _ratio(
                        same_phase_common,
                        int(call["total_chars"]),
                    ),
                }
            )

        lane = str(call.get("call_channel") or call.get("prompt_cache_lane") or phase)
        same_lane_previous = previous_by_lane.get(lane)
        if same_lane_previous is None:
            call.update(
                {
                    "same_lane_previous_round_name": "",
                    "same_lane_previous_iteration": 0,
                    "same_lane_common_prefix_chars": 0,
                    "same_lane_prefix_ratio": 0.0,
                    "same_lane_compared_layers": [],
                    "same_lane_changed_layers": [],
                    "same_lane_first_changed_layer": "",
                }
            )
        else:
            same_lane_common = common_prefix_len(
                str(same_lane_previous["_canonical"]),
                canonical,
            )
            compared_layers, changed_layers = _changed_layers(
                same_lane_previous.get("_layers") or {},
                layers if isinstance(layers, dict) else {},
            )
            call.update(
                {
                    "same_lane_previous_round_name": same_lane_previous["round_name"],
                    "same_lane_previous_iteration": same_lane_previous["iteration"],
                    "same_lane_common_prefix_chars": same_lane_common,
                    "same_lane_prefix_ratio": _ratio(
                        same_lane_common,
                        int(call["total_chars"]),
                    ),
                    "same_lane_compared_layers": compared_layers,
                    "same_lane_changed_layers": changed_layers,
                    "same_lane_first_changed_layer": (
                        changed_layers[0] if changed_layers else ""
                    ),
                }
            )

        stored = dict(call)
        stored["_canonical"] = canonical
        stored["_layers"] = layers if isinstance(layers, dict) else {}
        previous = stored
        previous_by_phase[phase] = stored
        previous_by_lane[lane] = stored

    if missing:
        issues.extend(f"round_file_missing:{path}" for path in missing)
    if not calls:
        issues.append("no_step_input_snapshots")
    for issue in _prompt_cache_key_hygiene_issues(calls):
        if issue not in issues:
            issues.append(issue)
    return {
        "summary": _build_summary(calls, existing, issues),
        "calls": calls,
        "issues": issues,
    }


def _provider_calls_from_summary(summary_path: Path) -> list[dict[str, Any]]:
    data = _read_json(Path(summary_path))
    results = data.get("results") if isinstance(data, dict) else []
    calls: list[dict[str, Any]] = []
    for result_index, result in enumerate(results if isinstance(results, list) else []):
        if not isinstance(result, dict):
            continue
        round_name = str(result.get("round_name") or "")
        round_num = _round_num_from_path(Path(round_name)) if round_name else -1
        provider_calls = result.get("provider_calls")
        if not isinstance(provider_calls, list):
            continue
        for call_index, call in enumerate(provider_calls, start=1):
            if not isinstance(call, dict):
                continue
            usage = call.get("usage")
            telemetry = call.get("prompt_cache_telemetry")
            if not isinstance(telemetry, dict):
                telemetry = extract_prompt_cache_telemetry(
                    usage,
                    call.get("request_contract_audit"),
                )
            calls.append({
                "round_name": round_name,
                "round_num": round_num,
                "result_index": result_index,
                "call_index": call_index,
                "phase": str(call.get("phase") or ""),
                "iteration": _usage_int(call.get("iteration")),
                "provider": str(call.get("provider") or ""),
                "model": str(call.get("model") or ""),
                "call_channel": str(call.get("call_channel") or ""),
                "prompt_cache_key": str(call.get("prompt_cache_key") or ""),
                "lately_epoch": str(telemetry.get("lately_epoch") or ""),
                "request_body_sha256": str(call.get("request_body_sha256") or ""),
                "prompt_cache_telemetry": telemetry,
            })
    return calls


def analyze_provider_cache_summary(summary_path: Path) -> dict[str, Any]:
    calls = _provider_calls_from_summary(Path(summary_path))
    summary = _simple_cache_group_summary(calls)
    summary.update({
        "schema_version": "provider_prompt_cache_summary.v1",
        "summary_path": str(Path(summary_path)),
        "by_phase": _group_summary(calls, "phase"),
        "by_call_channel": _group_summary(calls, "call_channel"),
        "by_prompt_cache_key": _group_summary(calls, "prompt_cache_key"),
        "by_lately_epoch": _group_summary(calls, "lately_epoch"),
        "by_request_body_sha256": _group_summary(calls, "request_body_sha256"),
    })
    return {
        "summary": summary,
        "calls": calls,
        "issues": [],
    }


def _public_call(call: dict[str, Any]) -> dict[str, Any]:
    return {key: value for key, value in call.items() if not key.startswith("_")}


def render_markdown(report: dict[str, Any]) -> str:
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    lines = [
        "# Prompt Prefix Cache Estimate",
        "",
        "This report combines character-level prefix stability with provider token-cache telemetry when raw usage is available.",
        "",
        f"- Calls: {summary.get('call_count', 0)}",
        f"- Compared calls: {summary.get('compared_call_count', 0)}",
        f"- Total chars: {summary.get('total_chars', 0)}",
        f"- Weighted prefix ratio: {float(summary.get('weighted_prefix_ratio') or 0.0):.4f}",
        f"- Median prefix ratio: {float(summary.get('median_prefix_ratio') or 0.0):.4f}",
        f"- Min / Max prefix ratio: {float(summary.get('min_prefix_ratio') or 0.0):.4f} / {float(summary.get('max_prefix_ratio') or 0.0):.4f}",
        "",
        "## By Phase",
        "",
        "| phase | calls | compared | chars | weighted prefix ratio |",
        "| --- | ---: | ---: | ---: | ---: |",
    ]
    provider_cache = summary.get("provider_cache")
    if isinstance(provider_cache, dict):
        insert_at = lines.index("## By Phase")
        lines[insert_at:insert_at] = [
            "## Provider Cache Telemetry",
            "",
            f"- Prompt tokens: {provider_cache.get('prompt_tokens', 0)}",
            f"- Cached tokens: {provider_cache.get('cached_tokens', 0)}",
            f"- Cache write tokens: {provider_cache.get('cache_write_tokens', 0)}",
            "- Cache write status: "
            f"{json.dumps(provider_cache.get('cache_write_status_counts') or {}, ensure_ascii=False, sort_keys=True)}",
            "- Cache accounting complete: "
            f"{str(bool(provider_cache.get('cache_accounting_complete'))).lower()}",
            "- Token cache hit ratio: "
            f"{float(provider_cache.get('cache_hit_ratio') or 0.0):.4f}",
            "- Zero / nonzero cache calls: "
            f"{provider_cache.get('zero_cache_call_count', 0)} / "
            f"{provider_cache.get('nonzero_cache_call_count', 0)}",
            "",
        ]
    by_phase = summary.get("by_phase") if isinstance(summary.get("by_phase"), dict) else {}
    for phase, data in sorted(by_phase.items()):
        if not isinstance(data, dict):
            continue
        lines.append(
            "| {phase} | {calls} | {compared} | {chars} | {ratio:.4f} |".format(
                phase=phase,
                calls=data.get("call_count", 0),
                compared=data.get("compared_call_count", 0),
                chars=data.get("total_chars", 0),
                ratio=float(data.get("weighted_prefix_ratio") or 0.0),
            )
        )
    layer_churn = summary.get("layer_churn")
    if isinstance(layer_churn, dict):
        by_layer = (
            layer_churn.get("by_layer")
            if isinstance(layer_churn.get("by_layer"), dict)
            else {}
        )
        if by_layer:
            lines.extend([
                "",
                "## Layer Churn",
                "",
                "| layer | compared | changed |",
                "| --- | ---: | ---: |",
            ])
            for layer, data in by_layer.items():
                if not isinstance(data, dict):
                    continue
                lines.append(
                    "| {layer} | {compared} | {changed} |".format(
                        layer=layer,
                        compared=data.get("compared_count", 0),
                        changed=data.get("changed_count", 0),
                    )
                )
    issues = summary.get("issues") if isinstance(summary.get("issues"), list) else []
    if issues:
        lines.extend(["", "## Issues", ""])
        lines.extend(f"- {issue}" for issue in issues)
    lines.append("")
    return "\n".join(lines)


def write_artifacts(report: dict[str, Any], output_dir: Path) -> dict[str, Path]:
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    calls_path = output_dir / CALLS_JSONL_NAME
    summary_path = output_dir / SUMMARY_JSON_NAME
    md_path = output_dir / SUMMARY_MD_NAME
    with calls_path.open("w", encoding="utf-8", newline="\n") as f:
        for call in report.get("calls") or []:
            f.write(json.dumps(_public_call(call), ensure_ascii=False, sort_keys=True) + "\n")
    summary_payload = {
        "summary": report.get("summary") or {},
        "issues": report.get("issues") or [],
    }
    _write_json(summary_path, summary_payload)
    md_path.write_text(render_markdown(report), encoding="utf-8", newline="\n")
    return {
        "calls_jsonl": calls_path,
        "summary_json": summary_path,
        "summary_md": md_path,
    }


def _artifact_relpath(path: Path, base: Path) -> str:
    try:
        rel = path.resolve().relative_to(base.resolve())
    except ValueError:
        rel = Path(re.sub(r"^[A-Za-z]:", "", str(path)))
    return rel.as_posix()


def update_dogfood_summary(
    summary_path: Path,
    report: dict[str, Any],
    *,
    artifacts_dir: Path | None = None,
) -> dict[str, Any]:
    summary_path = Path(summary_path)
    data = _read_json(summary_path)
    if not isinstance(data, dict):
        raise ValueError(f"summary is not a JSON object: {summary_path}")
    summary = report.get("summary") if isinstance(report.get("summary"), dict) else {}
    payload = {
        "schema_version": summary.get("schema_version", "prompt_prefix_cache_summary.v1"),
        "call_count": int(summary.get("call_count") or 0),
        "compared_call_count": int(summary.get("compared_call_count") or 0),
        "total_chars": int(summary.get("total_chars") or 0),
        "weighted_prefix_ratio": float(summary.get("weighted_prefix_ratio") or 0.0),
        "median_prefix_ratio": float(summary.get("median_prefix_ratio") or 0.0),
        "min_prefix_ratio": float(summary.get("min_prefix_ratio") or 0.0),
        "max_prefix_ratio": float(summary.get("max_prefix_ratio") or 0.0),
        "by_phase": summary.get("by_phase") or {},
        "by_call_channel": summary.get("by_call_channel") or {},
        "by_prompt_cache_key": summary.get("by_prompt_cache_key") or {},
        "by_request_body_sha256": summary.get("by_request_body_sha256") or {},
        "provider_cache": summary.get("provider_cache") or {},
        "layer_churn": summary.get("layer_churn") or {},
        "issues": summary.get("issues") or [],
    }
    if artifacts_dir is not None:
        artifacts_base = Path(artifacts_dir)
        base = summary_path.parent
        payload["artifacts"] = {
            "calls_jsonl": _artifact_relpath(artifacts_base / CALLS_JSONL_NAME, base),
            "summary_json": _artifact_relpath(artifacts_base / SUMMARY_JSON_NAME, base),
            "summary_md": _artifact_relpath(artifacts_base / SUMMARY_MD_NAME, base),
        }
    data["prompt_prefix_cache"] = payload
    _write_json(summary_path, data)
    return data


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze character-level prompt prefix stability from UPSP round audits."
    )
    parser.add_argument("--round-file", action="append", default=[])
    parser.add_argument("--round-dir", default=str(DEFAULT_ROUND_DIR))
    parser.add_argument("--recent", type=int, default=None)
    parser.add_argument("--from-round", type=int, default=None)
    parser.add_argument("--to-round", type=int, default=None)
    parser.add_argument("--summary-path", default="")
    parser.add_argument("--output-dir", default="")
    parser.add_argument(
        "--update-summary",
        action="store_true",
        help="Write prompt_prefix_cache headline fields into --summary-path.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    summary_path = Path(args.summary_path) if args.summary_path else None
    output_dir = Path(args.output_dir) if args.output_dir else None
    round_files = select_round_files(
        round_files=[Path(path) for path in args.round_file],
        round_dir=Path(args.round_dir),
        recent=args.recent,
        from_round=args.from_round,
        to_round=args.to_round,
        summary_path=summary_path,
    )
    report = analyze_round_files(round_files)
    artifacts: dict[str, Path] | None = None
    if output_dir is not None:
        artifacts = write_artifacts(report, output_dir)
    if args.update_summary:
        if summary_path is None:
            raise SystemExit("--update-summary requires --summary-path")
        update_dogfood_summary(
            summary_path,
            report,
            artifacts_dir=output_dir if artifacts is not None else None,
        )
    print(json.dumps(report["summary"], ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if not report["issues"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
