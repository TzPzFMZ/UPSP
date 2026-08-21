"""Replay UPSP round snapshots through deterministic explicit-cache plans."""
from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from engines.prompt_cache_planner import apply_explicit_breakpoints  # noqa: E402
from data.round_audit_codec import read_round_audit_file  # noqa: E402


CALLS_NAME = "prompt_cache_replay_calls.jsonl"
SUMMARY_JSON_NAME = "prompt_cache_replay_summary.json"
SUMMARY_MD_NAME = "prompt_cache_replay_summary.md"


def _canonical_sha(value: Any) -> str:
    raw = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _read_events(path: Path) -> Iterable[dict[str, Any]]:
    yield from read_round_audit_file(path)


def _layer_contents(payload: dict[str, Any]) -> dict[str, Any]:
    snapshot = payload.get("layers_snapshot")
    layers = snapshot.get("layers") if isinstance(snapshot, dict) else None
    if not isinstance(layers, list):
        raise ValueError("prompt_cache_replay_layers_missing")
    return {
        str(layer.get("layer_key") or ""): layer.get("content")
        for layer in layers
        if isinstance(layer, dict) and layer.get("layer_key")
    }


def _layer_digests(payload: dict[str, Any]) -> dict[str, str]:
    snapshot = payload.get("layers_snapshot")
    layers = snapshot.get("layers") if isinstance(snapshot, dict) else None
    return {
        str(layer.get("layer_key") or ""): str(layer.get("sha256") or "")
        for layer in layers or []
        if isinstance(layer, dict) and layer.get("layer_key")
    }


def _message_layers(
    contents: dict[str, Any],
    messages: list[Any],
) -> list[str]:
    labels: list[str] = []
    permanent = contents.get("10_permanent")
    if isinstance(permanent, list):
        has_system = False
        for item in permanent:
            if not isinstance(item, dict):
                continue
            role = str(item.get("role") or "user").strip()
            if role in {"system", "developer"}:
                has_system = has_system or bool(str(item.get("content") or "").strip())
            else:
                labels.append("10_permanent")
        if has_system:
            labels.insert(0, "10_permanent")
    elif str(permanent or "").strip():
        labels.append("10_permanent")

    for layer in (
        "20_periodic",
        "30_lately",
        "40_high_freq",
        "50_now",
        "60_statusbar",
        "99_popup",
    ):
        content = contents.get(layer)
        if isinstance(content, list):
            labels.extend(layer for item in content if isinstance(item, dict))
        elif str(content or "").strip():
            labels.append(layer)
    if len(labels) != len(messages):
        raise ValueError(
            "prompt_cache_replay_message_alignment_invalid:"
            f"labels={len(labels)} messages={len(messages)}"
        )
    return labels


def _reaction_inputs(path: Path) -> list[dict[str, Any]]:
    calls: list[dict[str, Any]] = []
    for event in _read_events(path):
        if event.get("event_type") != "step_input_snapshot":
            continue
        if str(event.get("phase") or "").lower() != "reaction":
            continue
        payload = event.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        envelope = payload.get("provider_request_envelope")
        envelope = envelope if isinstance(envelope, dict) else {}
        request_body = envelope.get("request_body")
        if not isinstance(request_body, dict):
            raise ValueError("prompt_cache_replay_request_body_missing")
        messages = request_body.get("messages")
        if not isinstance(messages, list):
            raise ValueError("prompt_cache_replay_chat_messages_missing")
        contents = _layer_contents(payload)
        calls.append({
            "event_index": event.get("event_index"),
            "iteration": event.get("iteration"),
            "payload": request_body,
            "message_layers": _message_layers(contents, messages),
            "layer_contents": contents,
            "layer_digests": _layer_digests(payload),
        })
    return calls


def _last_target(plan: dict[str, Any]) -> dict[str, Any]:
    details = plan.get("target_details")
    if not isinstance(details, list) or not details:
        raise ValueError("prompt_cache_replay_plan_target_missing")
    return details[-1]


def _epoch_runs(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    runs: list[dict[str, Any]] = []
    for row in rows:
        tiered = row["tiered"]
        index = tiered["context_epoch_index"]
        if not runs or runs[-1]["context_epoch_index"] != index:
            runs.append({
                "context_epoch_index": index,
                "b1_present": tiered["b1_present"],
                "lately_epoch": tiered["lately_epoch"],
                "prefix_fingerprint": tiered["prefix_fingerprint"],
                "start_call": row["call_number"],
                "end_call": row["call_number"],
                "consecutive_calls": 1,
            })
            continue
        runs[-1]["end_call"] = row["call_number"]
        runs[-1]["consecutive_calls"] += 1
    return runs


def replay_round(path: Path, *, promoted_min_chars: int = 4096) -> dict[str, Any]:
    source_calls = _reaction_inputs(path)
    rows: list[dict[str, Any]] = []
    seen_fixed: set[str] = set()
    seen_b1: set[str] = set()
    previous_b0 = ""
    previous_b1 = ""
    previous_lately_digest: str | None = None
    previous_request = ""
    previous_context_epoch = ""
    context_epoch_index = 0
    context_epoch_ordinal = 0

    for call_number, source in enumerate(source_calls, start=1):
        payload = source["payload"]
        layers = source["message_layers"]
        contents = source["layer_contents"]
        _fixed_payload, fixed = apply_explicit_breakpoints(
            payload,
            profile="automatic_tiered",
            message_layers=layers,
            layer_contents=contents,
            promoted_min_chars=10 ** 12,
        )
        _tiered_payload, tiered = apply_explicit_breakpoints(
            payload,
            profile="automatic_tiered",
            message_layers=layers,
            layer_contents=contents,
            promoted_min_chars=promoted_min_chars,
        )
        fixed_target = _last_target(fixed)
        tiered_target = _last_target(tiered)
        b0 = fixed_target["prefix_fingerprint"]
        has_b1 = len(tiered["targets"]) > 1
        b1 = tiered_target["prefix_fingerprint"] if has_b1 else ""
        context_epoch = b1 or "no_b1"
        if context_epoch != previous_context_epoch:
            context_epoch_index += 1
            context_epoch_ordinal = 1
        else:
            context_epoch_ordinal += 1
        fixed_action = "read" if b0 in seen_fixed else "write"
        b1_action = "none"
        if has_b1:
            b1_action = "read" if b1 in seen_b1 else "write"
        fallback = bool(has_b1 and previous_b1 and b1 != previous_b1 and b0 == previous_b0)
        request_fingerprint = _canonical_sha(payload)
        implicit_write = not previous_request or request_fingerprint != previous_request
        lately_digest = source["layer_digests"].get("30_lately", "")
        lately_changed = (
            previous_lately_digest is not None
            and lately_digest != previous_lately_digest
        )
        row = {
            "call_number": call_number,
            "event_index": source["event_index"],
            "iteration": source["iteration"],
            "fixed": {
                "target": fixed["targets"][-1],
                "prefix_chars": fixed_target["prefix_chars"],
                "prefix_fingerprint": b0,
                "estimated_action": fixed_action,
            },
            "tiered": {
                "targets": tiered["targets"],
                "prefix_chars": tiered_target["prefix_chars"],
                "prefix_fingerprint": tiered_target["prefix_fingerprint"],
                "b1_present": has_b1,
                "b1_estimated_action": b1_action,
                "b0_fallback_on_epoch_change": fallback,
                "lately_epoch": tiered["lately_epoch"],
                "context_epoch_index": context_epoch_index,
                "consecutive_reuse_ordinal": context_epoch_ordinal,
            },
            "promoted_prefix_delta_chars": (
                tiered_target["prefix_chars"] - fixed_target["prefix_chars"]
            ),
            "lately_changed_from_previous": lately_changed,
            "implicit_latest_breakpoint_potential_write": implicit_write,
        }
        rows.append(row)
        seen_fixed.add(b0)
        if b1:
            seen_b1.add(b1)
        previous_b0 = b0
        previous_b1 = b1
        previous_lately_digest = lately_digest
        previous_request = request_fingerprint
        previous_context_epoch = context_epoch

    b1_actions = Counter(row["tiered"]["b1_estimated_action"] for row in rows)
    fixed_actions = Counter(row["fixed"]["estimated_action"] for row in rows)
    summary = {
        "schema": "prompt_cache_counterfactual_replay.v1",
        "source_round_file": str(path.resolve()),
        "reaction_calls": len(rows),
        "promoted_min_chars": promoted_min_chars,
        "fixed_only": {
            "unique_prefixes": len(seen_fixed),
            "estimated_writes": fixed_actions["write"],
            "estimated_reads": fixed_actions["read"],
        },
        "tiered_b1": {
            "unique_epochs": len(seen_b1),
            "estimated_writes": b1_actions["write"],
            "estimated_reads": b1_actions["read"],
            "calls_without_b1": b1_actions["none"],
            "b0_fallbacks": sum(
                bool(row["tiered"]["b0_fallback_on_epoch_change"])
                for row in rows
            ),
        },
        "tiered_context_epoch_runs": _epoch_runs(rows),
        "lately_changes": sum(row["lately_changed_from_previous"] for row in rows),
        "implicit_latest_breakpoint_potential_writes": sum(
            row["implicit_latest_breakpoint_potential_write"] for row in rows
        ),
        "calls": rows,
    }
    return summary


def _write_outputs(output_dir: Path, result: dict[str, Any]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    calls_path = output_dir / CALLS_NAME
    calls_path.write_text(
        "".join(
            json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n"
            for row in result["calls"]
        ),
        encoding="utf-8",
        newline="\n",
    )
    summary = {key: value for key, value in result.items() if key != "calls"}
    (output_dir / SUMMARY_JSON_NAME).write_text(
        json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )
    fixed = summary["fixed_only"]
    tiered = summary["tiered_b1"]
    epoch_runs = summary["tiered_context_epoch_runs"]
    markdown = (
        "# Prompt Cache Counterfactual Replay\n\n"
        f"- Reaction calls: {summary['reaction_calls']}\n"
        f"- Fixed-only prefixes: {fixed['unique_prefixes']} "
        f"({fixed['estimated_writes']} writes / {fixed['estimated_reads']} reads)\n"
        f"- Tiered B1 epochs: {tiered['unique_epochs']} "
        f"({tiered['estimated_writes']} writes / {tiered['estimated_reads']} reads)\n"
        f"- Deterministic context epoch runs: {len(epoch_runs)}\n"
        f"- Calls without B1: {tiered['calls_without_b1']}\n"
        f"- B0 fallbacks on B1 rollover: {tiered['b0_fallbacks']}\n"
        f"- Lately changes: {summary['lately_changes']}\n"
        "- Implicit latest-message potential writes: "
        f"{summary['implicit_latest_breakpoint_potential_writes']}\n"
    )
    (output_dir / SUMMARY_MD_NAME).write_text(
        markdown,
        encoding="utf-8",
        newline="\n",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--round-file", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--promoted-min-chars", type=int, default=4096)
    args = parser.parse_args(argv)
    result = replay_round(
        args.round_file,
        promoted_min_chars=max(1, args.promoted_min_chars),
    )
    _write_outputs(args.output_dir, result)
    print(json.dumps({key: value for key, value in result.items() if key != "calls"},
                     ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
