"""Canonical provider request bytes and their audit-only source map."""
from __future__ import annotations

import hashlib
import json
from bisect import bisect_right


WIRE_BODY_ENCODING = "canonical_json_utf8.v1"
SOURCE_MAP_SCHEMA = "request_body_source_map.v1"
CONTEXT_LAYER_KEYS = (
    "10_permanent",
    "20_periodic",
    "30_lately",
    "40_high_freq",
    "50_now",
    "60_statusbar",
    "99_popup",
)
SOURCE_PANES = {
    "00_call_header",
    "01_tool_header",
    "02_generation_config",
    *CONTEXT_LAYER_KEYS,
}
_CONTEXT_ROOTS = {"messages", "input", "instructions", "system"}


def _pointer(path, key):
    escaped = str(key).replace("~", "~0").replace("/", "~1")
    return f"{path}/{escaped}" if path else f"/{escaped}"


def _json_scalar(value):
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _encode(value, path, output, nodes):
    start = len(output)
    node = {"path": path, "byte_start": start}
    if isinstance(value, dict):
        if any(not isinstance(key, str) for key in value):
            raise TypeError("provider_request_keys_must_be_strings")
        output.extend(b"{")
        for index, key in enumerate(sorted(value)):
            if index:
                output.extend(b",")
            member_start = len(output)
            output.extend(_json_scalar(key))
            output.extend(b":")
            child = _encode(value[key], _pointer(path, key), output, nodes)
            child["member_start"] = member_start
        output.extend(b"}")
    elif isinstance(value, (list, tuple)):
        output.extend(b"[")
        for index, item in enumerate(value):
            if index:
                output.extend(b",")
            _encode(item, _pointer(path, index), output, nodes)
        output.extend(b"]")
    elif isinstance(value, str):
        output.extend(_json_scalar(value))
        node["value"] = value
    elif value is None or isinstance(value, (bool, int, float)):
        output.extend(_json_scalar(value))
    else:
        raise TypeError(f"provider_request_value_not_json:{type(value).__name__}")
    node["byte_end"] = len(output)
    nodes[path] = node
    return node


def _encoded_request_body(request_body):
    if not isinstance(request_body, dict):
        raise ValueError("provider_request_body_must_be_object")
    output = bytearray()
    nodes = {}
    _encode(request_body, "", output, nodes)
    return bytes(output), nodes


def serialize_provider_request_body(request_body):
    """Return the only bytes allowed to drive request SHA and HTTP POST."""
    return _encoded_request_body(request_body)[0]


def provider_request_body_sha256(request_body):
    return hashlib.sha256(serialize_provider_request_body(request_body)).hexdigest()


def _root_key(path):
    return path.split("/", 2)[1] if path.startswith("/") else ""


def _entry_block_id(entry, pane_id, index):
    ref = entry.get("ref") if isinstance(entry.get("ref"), dict) else {}
    return str(
        entry.get("source_block_id")
        or entry.get("active_corpus_id")
        or entry.get("id")
        or ref.get("source_block_id")
        or f"{pane_id}:B{index:02d}"
    )


def _layer_candidates(layer):
    pane_id = str(layer.get("layer_key") or "")
    content = layer.get("content")
    if pane_id not in CONTEXT_LAYER_KEYS:
        return []
    if isinstance(content, str):
        match_text = content.strip()
        if not match_text:
            return []
        source_start = len(content) - len(content.lstrip())
        source_end = source_start + len(match_text)
        indexes = []
        for item in layer.get("block_index") or []:
            if not isinstance(item, dict):
                continue
            start = item.get("char_start")
            end = item.get("char_end")
            if (
                    isinstance(start, int)
                    and not isinstance(start, bool)
                    and isinstance(end, int)
                    and not isinstance(end, bool)
                    and source_start <= start < end <= source_end):
                indexes.append({
                    "block_id": str(item.get("block_id") or ""),
                    "title": str(item.get("title") or ""),
                    "kind": str(item.get("kind") or ""),
                    "source_start": start,
                    "source_end": end,
                })
        return [{
            "pane_id": pane_id,
            "match_text": match_text,
            "source_start": source_start,
            "source_end": source_end,
            "blocks": indexes,
        }]
    if not isinstance(content, list):
        return []
    candidates = []
    for index, item in enumerate(content, 1):
        if not isinstance(item, dict):
            continue
        value = item.get("content")
        if isinstance(value, (dict, list)):
            value = json.dumps(value, ensure_ascii=False, sort_keys=True)
        text = str(value or "")
        if not text:
            continue
        candidates.append({
            "pane_id": pane_id,
            "match_text": text,
            "source_start": 0,
            "source_end": len(text),
            "native_replay": (
                item.get("native_replay")
                if isinstance(item.get("native_replay"), dict)
                else None
            ),
            "blocks": [{
                "block_id": _entry_block_id(item, pane_id, index),
                "title": "",
                "kind": str(item.get("kind") or ""),
                "source_start": 0,
                "source_end": len(text),
            }],
        })
    return candidates


def _native_replay_messages(native_replay):
    expected = []
    assistant = native_replay.get("assistant_message")
    if isinstance(assistant, dict):
        message = {
            "role": "assistant",
            "content": assistant.get("content") or "",
        }
        reasoning = str(assistant.get("reasoning_content") or "").strip()
        tools = assistant.get("tool_calls")
        if reasoning:
            message["reasoning_content"] = reasoning
        if isinstance(tools, list) and tools:
            message["tool_calls"] = [item for item in tools if isinstance(item, dict)]
        if len(message) > 2:
            expected.append(message)
    for item in native_replay.get("tool_results") or []:
        if not isinstance(item, dict):
            continue
        tool_call_id = str(item.get("tool_call_id") or "").strip()
        if not tool_call_id:
            continue
        content = item.get("content")
        if isinstance(content, (dict, list)):
            content = json.dumps(content, ensure_ascii=False, sort_keys=True)
        expected.append({
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": str(content or ""),
        })
    return expected


def _context_owner_node(node, nodes):
    parts = str(node.get("path") or "").split("/")[1:]
    if not parts:
        return node
    nested = parts[0] in {"messages", "input", "system"} and len(parts) > 1
    owner_parts = parts[:2] if nested else parts[:1]
    owner_path = "/" + "/".join(owner_parts)
    return nodes.get(owner_path) or node


def _owner_range(node, nodes):
    owner = _context_owner_node(node, nodes)
    return {
        "owner_byte_start": owner.get("member_start", owner["byte_start"]),
        "owner_byte_end": owner["byte_end"],
    }


def _native_replay_entries(
        candidate, request_body, nodes, used_message_indexes, cursor):
    replay = candidate.get("native_replay")
    messages = request_body.get("messages")
    if not isinstance(replay, dict) or not isinstance(messages, list):
        return []
    expected = _native_replay_messages(replay)
    if not expected:
        return []
    matched = []
    matched_indexes = []
    search_start = 0
    block = (candidate.get("blocks") or [{}])[0]
    for wanted in expected:
        found = next((
            index for index in range(search_start, len(messages))
            if index not in used_message_indexes
            and messages[index] == wanted
            and (nodes.get(_pointer("/messages", index)) or {}).get("byte_start", -1) >= cursor
        ), None)
        if found is None:
            return []
        node = nodes.get(_pointer("/messages", found))
        if not node:
            return []
        matched.append({
            "byte_start": node["byte_start"],
            "byte_end": node["byte_end"],
            "pane_id": candidate["pane_id"],
            "block_id": block.get("block_id") or "",
            "request_path": node["path"],
            "source_char_start": candidate["source_start"],
            "source_char_end": candidate["source_end"],
            "source_mapping": "derived",
            **_owner_range(node, nodes),
        })
        matched_indexes.append(found)
        search_start = found + 1
    used_message_indexes.update(matched_indexes)
    return matched


def _is_context_text_path(path):
    root = _root_key(path)
    leaf = path.rsplit("/", 1)[-1]
    if path in {"/input", "/instructions", "/system"}:
        return True
    if root in {"messages", "input"}:
        return leaf in {"content", "text"}
    return root == "system" and leaf == "text"


def _context_string_nodes(nodes):
    return [
        node for node in nodes.values()
        if isinstance(node.get("value"), str)
        and _is_context_text_path(node.get("path", ""))
    ]


def _char_byte_offsets(node):
    cached = node.get("char_byte_offsets")
    if isinstance(cached, list):
        return cached
    offset = int(node["byte_start"]) + 1
    offsets = [offset]
    short_escape = {'"', "\\", "\b", "\f", "\n", "\r", "\t"}
    for character in node.get("value", ""):
        codepoint = ord(character)
        if character in short_escape:
            offset += 2
        elif codepoint < 0x20:
            offset += 6
        else:
            offset += len(character.encode("utf-8"))
        offsets.append(offset)
    node["char_byte_offsets"] = offsets
    return offsets


def _occurrences(node, text, occupied):
    value = node["value"]
    start = 0
    while True:
        index = value.find(text, start)
        if index < 0:
            return
        end = index + len(text)
        if not any(index < right and end > left for left, right in occupied):
            yield index, end
        start = index + 1


def _match_candidate(candidate, nodes, occupied, cursor):
    pane_id = candidate["pane_id"]
    choices = []
    for node in nodes:
        ranges = occupied.setdefault(node["path"], [])
        root = _root_key(node["path"])
        if pane_id == "10_permanent":
            preferred = 0 if root in {"instructions", "system"} else 1
        else:
            preferred = 0 if root in {"messages", "input"} else 2
        for start, end in _occurrences(node, candidate["match_text"], ranges):
            absolute = _char_byte_offsets(node)[start]
            choices.append((preferred, absolute < cursor, absolute, node, start, end))
    if not choices:
        return None
    _preferred, _before_cursor, absolute, node, start, end = min(choices)
    occupied[node["path"]].append((start, end))
    occupied[node["path"]].sort()
    return node, start, end, absolute


def _partition_candidate(candidate):
    start = candidate["source_start"]
    end = candidate["source_end"]
    blocks = sorted(candidate.get("blocks") or [], key=lambda item: item["source_start"])
    if not blocks:
        return [{
            "block_id": "",
            "title": "",
            "kind": "",
            "source_start": start,
            "source_end": end,
        }]
    segments = []
    cursor = start
    for block in blocks:
        if block["source_start"] > cursor:
            segments.append({
                "block_id": "",
                "title": "",
                "kind": "",
                "source_start": cursor,
                "source_end": block["source_start"],
            })
        segments.append(block)
        cursor = block["source_end"]
    if cursor < end:
        segments.append({
            "block_id": "",
            "title": "",
            "kind": "",
            "source_start": cursor,
            "source_end": end,
        })
    return segments


def _context_entries(request_body, layer_payloads, nodes):
    string_nodes = _context_string_nodes(nodes)
    occupied = {}
    entries = []
    used_replay_message_indexes = set()
    cursor = 0
    layers = {
        str(layer.get("layer_key") or ""): layer
        for layer in layer_payloads or []
        if isinstance(layer, dict)
    }
    for pane_id in CONTEXT_LAYER_KEYS:
        for candidate in _layer_candidates(layers.get(pane_id, {})):
            match = _match_candidate(candidate, string_nodes, occupied, cursor)
            if match is None:
                derived = _native_replay_entries(
                    candidate,
                    request_body,
                    nodes,
                    used_replay_message_indexes,
                    cursor,
                )
                if derived:
                    entries.extend(derived)
                    cursor = derived[-1]["byte_end"]
                    continue
                raise ValueError(
                    f"request_body_source_map_layer_missing:{pane_id}")
            node, request_start, _request_end, absolute = match
            if pane_id != "10_permanent":
                cursor = absolute
            for segment in _partition_candidate(candidate):
                relative_start = segment["source_start"] - candidate["source_start"]
                relative_end = segment["source_end"] - candidate["source_start"]
                char_start = request_start + relative_start
                char_end = request_start + relative_end
                if char_end <= char_start:
                    continue
                entry = {
                    "byte_start": _char_byte_offsets(node)[char_start],
                    "byte_end": _char_byte_offsets(node)[char_end],
                    "pane_id": pane_id,
                    "block_id": segment["block_id"],
                    "request_path": node["path"],
                    "request_char_start": char_start,
                    "request_char_end": char_end,
                    "source_char_start": segment["source_start"],
                    "source_char_end": segment["source_end"],
                    **_owner_range(node, nodes),
                }
                if segment.get("title"):
                    entry["title"] = segment["title"]
                if segment.get("kind"):
                    entry["kind"] = segment["kind"]
                entries.append(entry)
    return entries


def _header_entries(request_body, nodes):
    entries = []
    for key in sorted(request_body):
        if key in _CONTEXT_ROOTS:
            continue
        pane_id = (
            "00_call_header" if key == "model"
            else "01_tool_header" if key == "tools"
            else "02_generation_config"
        )
        node = nodes.get(_pointer("", key))
        if not node:
            continue
        entries.append({
            "byte_start": node.get("member_start", node["byte_start"]),
            "byte_end": node["byte_end"],
            "pane_id": pane_id,
            "block_id": "",
            "request_path": node["path"],
            "source_char_start": 0,
            "source_char_end": 0,
        })
    return entries


def build_request_body_source_map(request_body, layer_payloads):
    wire, nodes = _encoded_request_body(request_body)
    entries = _header_entries(request_body, nodes)
    entries.extend(_context_entries(request_body, layer_payloads, nodes))
    entries.sort(key=lambda item: (item["byte_start"], item["byte_end"]))
    source_map = {
        "schema_version": SOURCE_MAP_SCHEMA,
        "wire_body_encoding": WIRE_BODY_ENCODING,
        "wire_body_sha256": hashlib.sha256(wire).hexdigest(),
        "wire_body_bytes": len(wire),
        "entries": entries,
    }
    _validate_source_map(wire, nodes, source_map)
    return source_map


def _validate_source_map(wire, nodes, source_map):
    if not isinstance(source_map, dict):
        raise ValueError("request_body_source_map_missing")
    if (
            source_map.get("schema_version") != SOURCE_MAP_SCHEMA
            or source_map.get("wire_body_encoding") != WIRE_BODY_ENCODING
            or source_map.get("wire_body_sha256") != hashlib.sha256(wire).hexdigest()
            or source_map.get("wire_body_bytes") != len(wire)):
        raise ValueError("request_body_source_map_wire_mismatch")
    entries = source_map.get("entries")
    if not isinstance(entries, list) or not entries:
        raise ValueError("request_body_source_map_entries_invalid")
    previous_end = 0
    for entry in entries:
        if not isinstance(entry, dict):
            raise ValueError("request_body_source_map_entry_invalid")
        start = entry.get("byte_start")
        end = entry.get("byte_end")
        path = entry.get("request_path")
        if (
                not isinstance(start, int)
                or isinstance(start, bool)
                or not isinstance(end, int)
                or isinstance(end, bool)
                or start < previous_end
                or end <= start
                or end > len(wire)
                or entry.get("pane_id") not in SOURCE_PANES
                or not isinstance(entry.get("block_id"), str)
                or not isinstance(path, str)
                or path not in nodes):
            raise ValueError("request_body_source_map_entry_invalid")
        node = nodes[path]
        node_start = int(node.get("member_start", node["byte_start"]))
        if start < node_start or end > node["byte_end"]:
            raise ValueError("request_body_source_map_entry_invalid")
        for field in (
                "source_char_start", "source_char_end",
                "request_char_start", "request_char_end",
                "owner_byte_start", "owner_byte_end"):
            if field in entry and (
                    not isinstance(entry[field], int)
                    or isinstance(entry[field], bool)
                    or entry[field] < 0):
                raise ValueError("request_body_source_map_entry_invalid")
        if entry.get("source_char_end", 0) < entry.get("source_char_start", 0):
            raise ValueError("request_body_source_map_entry_invalid")
        owner_start = entry.get("owner_byte_start")
        owner_end = entry.get("owner_byte_end")
        if (owner_start is None) != (owner_end is None) or (
                owner_start is not None
                and (owner_start > start or owner_end < end or owner_end > len(wire))):
            raise ValueError("request_body_source_map_entry_invalid")
        if owner_start is not None:
            expected_owner = _owner_range(node, nodes)
            if (
                    owner_start != expected_owner["owner_byte_start"]
                    or owner_end != expected_owner["owner_byte_end"]):
                raise ValueError("request_body_source_map_entry_invalid")
        if entry["pane_id"] in CONTEXT_LAYER_KEYS:
            if entry.get("source_mapping") == "derived":
                if (
                        wire[node["byte_start"]:node["byte_start"] + 1]
                        not in {b"{", b"["}
                        or entry.get("request_char_start") is not None
                        or entry.get("request_char_end") is not None):
                    raise ValueError("request_body_source_map_entry_invalid")
                previous_end = end
                continue
            request_start = entry.get("request_char_start")
            request_end = entry.get("request_char_end")
            source_start = entry.get("source_char_start")
            source_end = entry.get("source_char_end")
            if (
                    not isinstance(node.get("value"), str)
                    or not isinstance(request_start, int)
                    or isinstance(request_start, bool)
                    or not isinstance(request_end, int)
                    or isinstance(request_end, bool)
                    or not isinstance(source_start, int)
                    or isinstance(source_start, bool)
                    or not isinstance(source_end, int)
                    or isinstance(source_end, bool)
                    or request_start < 0
                    or request_end <= request_start
                    or request_end > len(node["value"])
                    or end - start <= 0
                    or source_end - source_start != request_end - request_start):
                raise ValueError("request_body_source_map_entry_invalid")
            offsets = _char_byte_offsets(node)
            if start != offsets[request_start] or end != offsets[request_end]:
                raise ValueError("request_body_source_map_entry_invalid")
        previous_end = end
    return True


def validate_request_body_source_map(request_body, source_map):
    wire, nodes = _encoded_request_body(request_body)
    return _validate_source_map(wire, nodes, source_map)


def verified_provider_request_wire(envelope, request_body=None):
    if not isinstance(envelope, dict):
        raise ValueError("provider_request_envelope_invalid")
    body = request_body if request_body is not None else envelope.get("request_body")
    wire, nodes = _encoded_request_body(body)
    digest = hashlib.sha256(wire).hexdigest()
    if (
            envelope.get("wire_body_encoding") != WIRE_BODY_ENCODING
            or envelope.get("wire_body_sha256") != digest
            or envelope.get("wire_body_bytes") != len(wire)
            or envelope.get("request_body_sha256") != digest):
        raise ValueError("provider_request_wire_mismatch")
    _validate_source_map(wire, nodes, envelope.get("request_body_source_map"))
    return wire


def request_string_char_offset(request_body, request_path, wire_offset):
    _wire, nodes = _encoded_request_body(request_body)
    node = nodes.get(request_path)
    if not isinstance(node, dict) or not isinstance(node.get("value"), str):
        return None
    offsets = _char_byte_offsets(node)
    return max(0, min(len(offsets) - 1, bisect_right(offsets, wire_offset) - 1))
