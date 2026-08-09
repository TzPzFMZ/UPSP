"""Read-only longest-prefix comparison for audited provider request bodies."""
from __future__ import annotations

import json
import re
from pathlib import Path

from data.provider_request_wire import (
    request_string_char_offset,
    verified_provider_request_wire,
)


DIFF_SCHEMA = "seed_gui_request_prefix_diff.v1"
_ROUND_FILE_RE = re.compile(r"^round_(\d+)\.jsonl$")


def _unavailable(reason):
    return {
        "schema_version": DIFF_SCHEMA,
        "state": "unavailable",
        "reason": str(reason or "unavailable"),
    }


def _round_paths(round_dir, maximum_round):
    root = Path(round_dir).resolve()
    paths = []
    for path in root.glob("round_*.jsonl"):
        match = _ROUND_FILE_RE.fullmatch(path.name)
        if (
                match
                and int(match.group(1)) <= maximum_round
                and path.resolve().parent == root):
            paths.append((int(match.group(1)), path))
    return sorted(paths)


def _snapshots_from_path(round_num, path):
    snapshots = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return snapshots
    for line in lines:
        try:
            event = json.loads(line)
        except (TypeError, json.JSONDecodeError):
            continue
        if not isinstance(event, dict) or event.get("event_type") != "step_input_snapshot":
            continue
        payload = event.get("payload") if isinstance(event.get("payload"), dict) else {}
        envelope = payload.get("provider_request_envelope")
        if not isinstance(envelope, dict):
            continue
        snapshots.append({
            "round": round_num,
            "event_index": int(event.get("event_index") or 0),
            "frame_id": str(event.get("frame_id") or ""),
            "envelope": envelope,
        })
    return snapshots


def _api_shape(envelope):
    body = envelope.get("request_body")
    if not isinstance(body, dict):
        return ""
    shape = []
    for key in ("messages", "input", "instructions", "system", "tools"):
        if key not in body:
            continue
        value = body[key]
        shape.append((key, "list" if isinstance(value, list) else type(value).__name__))
    return json.dumps(shape, separators=(",", ":"))


def _compatibility_key(record):
    envelope = record["envelope"]
    provider = envelope.get("provider") if isinstance(envelope.get("provider"), dict) else {}
    audit = (
        envelope.get("request_contract_audit")
        if isinstance(envelope.get("request_contract_audit"), dict)
        else {}
    )
    prompt_cache_key = str(audit.get("prompt_cache_key") or "").strip()
    lane = str(audit.get("prompt_cache_lane") or "").strip()
    protocol = str(provider.get("provider") or "").strip()
    model = str(provider.get("model") or "").strip()
    connection_id = str(provider.get("connection_id") or "").strip()
    shape = _api_shape(envelope)
    if not all((prompt_cache_key, connection_id, lane, protocol, model, shape)):
        return None
    return (
        prompt_cache_key,
        connection_id,
        protocol,
        model,
        lane,
        shape,
    )


def _verified(record):
    envelope = record["envelope"]
    wire = verified_provider_request_wire(envelope)
    return wire, envelope["request_body_source_map"]["entries"]


def _common_prefix(left, right):
    limit = min(len(left), len(right))
    index = 0
    while index < limit and left[index] == right[index]:
        index += 1
    return index


def _entry_at_or_after(entries, offset):
    direct = [
        entry for entry in entries
        if entry["byte_start"] <= offset < entry["byte_end"]
    ]
    direct_blocks = [entry for entry in direct if entry.get("block_id")]
    if direct_blocks:
        return direct_blocks[0]
    containing = [
        entry for entry in entries
        if entry.get("owner_byte_start", entry["byte_start"]) <= offset
        < entry.get("owner_byte_end", entry["byte_end"])
    ]
    if containing:
        following_blocks = [
            entry for entry in containing
            if entry.get("block_id") and entry["byte_start"] >= offset
        ]
        if following_blocks:
            return min(following_blocks, key=lambda entry: entry["byte_start"])
        preceding_blocks = [entry for entry in containing if entry.get("block_id")]
        if preceding_blocks:
            return max(preceding_blocks, key=lambda entry: entry["byte_end"])
        if direct:
            return direct[0]
        return min(containing, key=lambda entry: entry["byte_start"])
    for entry in entries:
        if entry["byte_end"] > offset:
            return entry
    return None


def _entry_before(entries, offset):
    previous = None
    for entry in entries:
        if entry["byte_start"] >= offset:
            break
        previous = entry
    return previous


def _block_entry_at_or_after(entries, offset):
    return next((
        entry for entry in entries
        if entry["byte_end"] > offset and entry.get("block_id")
    ), None)


def _owner_sequence(entries, offset):
    result = []
    for entry in entries:
        if entry["byte_end"] <= offset:
            continue
        signature = (entry["pane_id"], entry["block_id"])
        if not result or result[-1] != signature:
            result.append(signature)
    return result


def _change_kind(current_entries, previous_entries, offset, current_wire, previous_wire):
    if offset >= len(current_wire):
        return "delete"
    if offset >= len(previous_wire):
        return "insert"
    current = _owner_sequence(current_entries, offset)
    previous = _owner_sequence(previous_entries, offset)
    shared = 0
    while (
            shared < len(current)
            and shared < len(previous)
            and current[shared] == previous[shared]):
        shared += 1
    current = current[shared:]
    previous = previous[shared:]
    common = [
        (current.index(owner), previous.index(owner))
        for owner in current
        if owner in previous
    ]
    if common:
        current_index, previous_index = min(common, key=lambda item: sum(item))
        if current_index == 0 and previous_index > 0:
            return "delete"
        if previous_index == 0 and current_index > 0:
            return "insert"
    return "replace"


def _source_offsets(body, entry, wire_offset):
    source_start = int(entry.get("source_char_start") or 0)
    request_start = entry.get("request_char_start")
    if not isinstance(request_start, int):
        return source_start, 0
    request_offset = request_string_char_offset(
        body,
        entry.get("request_path", ""),
        wire_offset,
    )
    if request_offset is None:
        return source_start, 0
    width = max(0, int(entry.get("source_char_end") or source_start) - source_start)
    block_offset = min(width, max(0, request_offset - request_start))
    return source_start + block_offset, block_offset


def _target(current, previous, current_wire, previous_wire, offset):
    current_map = current["envelope"]["request_body_source_map"]
    previous_map = previous["envelope"]["request_body_source_map"]
    current_entries = current_map["entries"]
    previous_entries = previous_map["entries"]
    change_kind = _change_kind(
        current_entries,
        previous_entries,
        offset,
        current_wire,
        previous_wire,
    )
    current_owner = _entry_at_or_after(current_entries, offset)
    previous_owner = _entry_at_or_after(previous_entries, offset)
    if change_kind == "delete" and previous_owner and not previous_owner["block_id"]:
        previous_owner = _block_entry_at_or_after(previous_entries, offset)
    if change_kind != "delete" and current_owner and not current_owner["block_id"]:
        current_owner = _block_entry_at_or_after(current_entries, offset)
    owner = previous_owner if change_kind == "delete" else current_owner
    if owner is None:
        owner = _entry_before(current_entries, offset) or _entry_before(previous_entries, offset)
    if owner is None:
        return {
            "pane_id": "02_generation_config",
            "block_id": "",
            "placement": "request_end",
            "change_kind": change_kind,
            "source_offset": 0,
            "block_offset": 0,
            "request_path": "",
        }

    if change_kind == "delete":
        if current_owner is None:
            placement = "layer_end"
        elif current_owner["pane_id"] != owner["pane_id"]:
            placement = "layer_end"
        elif current_owner["byte_start"] >= offset:
            placement = "block_boundary"
            owner = current_owner
        else:
            placement = "layer_end"
        source_offset = int(owner.get("source_char_start") or 0)
        block_offset = 0
    else:
        source_offset, block_offset = _source_offsets(
            current["envelope"]["request_body"],
            owner,
            offset,
        )
        before = _entry_before(current_entries, offset)
        if owner["byte_start"] < offset < owner["byte_end"]:
            placement = "block_inside" if owner["block_id"] else "layer_inside"
        elif (
                owner.get("owner_byte_start", owner["byte_start"]) <= offset
                < owner.get("owner_byte_end", owner["byte_end"])
                and owner["byte_end"] <= offset):
            placement = "block_inside" if owner["block_id"] else "layer_inside"
        elif before and before["pane_id"] == owner["pane_id"]:
            placement = "block_boundary"
        elif before:
            placement = "layer_boundary"
        else:
            placement = "layer_start"
    target = {
        "pane_id": owner["pane_id"],
        "block_id": owner["block_id"],
        "placement": placement,
        "change_kind": change_kind,
        "source_offset": source_offset,
        "block_offset": block_offset,
        "request_path": owner.get("request_path", ""),
    }
    if owner.get("source_mapping") == "derived":
        target["source_mapping"] = "derived"
    return target


def _frame_summary(record):
    return {
        "round": record["round"],
        "frame_id": record["frame_id"],
        "wire_body_sha256": record["envelope"]["wire_body_sha256"],
    }


def build_request_prefix_diff(round_dir, round_num, frame_id):
    try:
        round_num = int(round_num)
    except (TypeError, ValueError):
        return _unavailable("invalid_round")
    frame_id = str(frame_id or "").strip()
    paths = _round_paths(round_dir, round_num)
    current_path = next((path for number, path in paths if number == round_num), None)
    if current_path is None:
        return _unavailable("frame_not_found")
    snapshots = _snapshots_from_path(round_num, current_path)
    current_index = next((
        index for index in range(len(snapshots) - 1, -1, -1)
        for item in [snapshots[index]]
        if item["round"] == round_num and item["frame_id"] == frame_id
    ), None)
    if current_index is None:
        return _unavailable("frame_not_found")
    current = snapshots[current_index]
    compatibility = _compatibility_key(current)
    if compatibility is None:
        return _unavailable("current_frame_incompatible")
    try:
        current_wire, _current_entries = _verified(current)
    except (KeyError, TypeError, ValueError):
        return _unavailable("current_wire_contract_invalid")

    previous = None
    previous_wire = None
    candidate_groups = iter([snapshots[:current_index]])
    previous_paths = (
        (number, path) for number, path in reversed(paths) if number < round_num
    )
    while True:
        try:
            group = next(candidate_groups)
        except StopIteration:
            try:
                number, path = next(previous_paths)
            except StopIteration:
                break
            group = _snapshots_from_path(number, path)
        seen_frames = set()
        for candidate in reversed(group):
            candidate_frame = (candidate["round"], candidate["frame_id"])
            if candidate_frame in seen_frames:
                continue
            seen_frames.add(candidate_frame)
            if (
                    candidate["round"] == current["round"]
                    and candidate["frame_id"] == current["frame_id"]):
                continue
            if _compatibility_key(candidate) != compatibility:
                continue
            try:
                candidate_wire, _candidate_entries = _verified(candidate)
            except (KeyError, TypeError, ValueError):
                continue
            previous = candidate
            previous_wire = candidate_wire
            break
        if previous is not None:
            break
    if previous is None or previous_wire is None:
        return _unavailable("compatible_previous_frame_not_found")

    prefix = _common_prefix(current_wire, previous_wire)
    base = {
        "schema_version": DIFF_SCHEMA,
        "current": _frame_summary(current),
        "previous": _frame_summary(previous),
        "common_prefix_bytes": prefix,
        "current_wire_bytes": len(current_wire),
        "previous_wire_bytes": len(previous_wire),
        "prefix_ratio": round(prefix / len(current_wire), 6) if current_wire else 1.0,
        "changed_suffix_bytes": max(0, len(current_wire) - prefix),
    }
    if current_wire == previous_wire:
        base["state"] = "identical"
        return base
    base["state"] = "ready"
    base["target"] = _target(
        current,
        previous,
        current_wire,
        previous_wire,
        prefix,
    )
    return base
