import hashlib
import json
import copy

import pytest

from data.request_prefix_diff import build_request_prefix_diff
from data.provider_request_wire import (
    WIRE_BODY_ENCODING,
    build_request_body_source_map,
    serialize_provider_request_body,
    validate_request_body_source_map,
    verified_provider_request_wire,
)


def _layers(*, high_freq="HIGH", high_blocks=None, now="NOW"):
    values = {
        "10_permanent": {
            "content": "CORE\n\nRULE",
            "block_index": [
                {"block_id": "permanent:core", "title": "Core", "char_start": 0, "char_end": 4},
                {"block_id": "rule:one", "title": "Rule", "char_start": 6, "char_end": 10},
            ],
        },
        "20_periodic": {"content": ""},
        "30_lately": {"content": []},
        "40_high_freq": {
            "content": high_freq,
            "block_index": high_blocks or [
                {"block_id": "high:base", "title": "Base", "char_start": 0, "char_end": len(high_freq)},
            ],
        },
        "50_now": {"content": [{
            "role": "user",
            "kind": "interaction",
            "source_block_id": "R000001-user-0000",
            "content": now,
        }]},
        "60_statusbar": {"content": "STATUS"},
        "99_popup": {"content": "POPUP"},
    }
    return [
        {"layer_key": key, **values[key]}
        for key in values
    ]


def _body(*, high_freq="HIGH", now="NOW"):
    return {
        "messages": [
            {"role": "system", "content": "CORE\n\nRULE"},
            {"role": "system", "content": high_freq},
            {"role": "user", "content": now},
            {"role": "system", "content": "STATUS"},
            {"role": "system", "content": "POPUP"},
        ],
        "model": "模型-alpha",
        "stream": True,
        "temperature": 0.7,
        "tools": [{"type": "function", "function": {"name": "say", "description": "引号 \" 与换行\n"}}],
    }


def _body_with_periodic(periodic=""):
    messages = [{"role": "system", "content": "CORE\n\nRULE"}]
    if periodic:
        messages.append({"role": "system", "content": periodic})
    messages.extend([
        {"role": "system", "content": "HIGH"},
        {"role": "user", "content": "NOW"},
        {"role": "system", "content": "STATUS"},
        {"role": "system", "content": "POPUP"},
    ])
    return {"messages": messages, "model": "妯″瀷-alpha", "stream": True}


def _envelope(body, layers, *, lane="reaction.loop", connection="conn-a"):
    wire = serialize_provider_request_body(body)
    digest = hashlib.sha256(wire).hexdigest()
    return {
        "schema": "provider_request.v1",
        "provider": {
            "provider": "openai_chat",
            "model": "模型-alpha",
            "connection_id": connection,
        },
        "request_contract_audit": {
            "prompt_cache_key": "upsp:test:reaction",
            "prompt_cache_lane": lane,
        },
        "request_body": body,
        "request_body_sha256": digest,
        "wire_body_encoding": WIRE_BODY_ENCODING,
        "wire_body_sha256": digest,
        "wire_body_bytes": len(wire),
        "request_body_source_map": build_request_body_source_map(body, layers),
    }


def _event(round_num, frame_id, event_index, envelope):
    return {
        "event_type": "step_input_snapshot",
        "event_index": event_index,
        "round": round_num,
        "frame_id": frame_id,
        "payload": {"provider_request_envelope": envelope},
    }


def _write_round(root, round_num, events):
    (root / f"round_{round_num}.jsonl").write_text(
        "\n".join(json.dumps(event, ensure_ascii=False) for event in events) + "\n",
        encoding="utf-8",
    )


def test_spec725_canonical_wire_and_source_map_share_exact_bytes():
    body = _body(now="用户：反斜杠\\、换行\n与 emoji 🧭")
    layers = _layers(now="用户：反斜杠\\、换行\n与 emoji 🧭")
    wire = serialize_provider_request_body(body)
    assert wire == json.dumps(
        body,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    source_map = build_request_body_source_map(body, layers)
    assert validate_request_body_source_map(body, source_map) is True
    assert source_map["wire_body_bytes"] == len(wire)
    assert source_map["wire_body_sha256"] == hashlib.sha256(wire).hexdigest()
    assert [entry["pane_id"] for entry in source_map["entries"] if entry["block_id"]] == [
        "10_permanent",
        "10_permanent",
        "40_high_freq",
        "50_now",
    ]
    assert all(
        left["byte_end"] <= right["byte_start"]
        for left, right in zip(source_map["entries"], source_map["entries"][1:])
    )
    for entry in source_map["entries"]:
        if entry["pane_id"] not in {
                "10_permanent", "20_periodic", "30_lately", "40_high_freq",
                "50_now", "60_statusbar", "99_popup"}:
            continue
        value = body
        for segment in entry["request_path"].split("/")[1:]:
            value = value[int(segment)] if isinstance(value, list) else value[segment]
        expected = json.dumps(
            value[entry["request_char_start"]:entry["request_char_end"]],
            ensure_ascii=False,
        ).encode("utf-8")[1:-1]
        assert wire[entry["byte_start"]:entry["byte_end"]] == expected
    assert "request_body_source_map" not in body


def test_spec725_wire_verification_fails_closed_after_payload_tamper():
    body = _body()
    envelope = _envelope(body, _layers())
    assert verified_provider_request_wire(envelope) == serialize_provider_request_body(body)
    tampered = {**body, "temperature": 0.8}
    with pytest.raises(ValueError, match="provider_request_wire_mismatch"):
        verified_provider_request_wire(envelope, tampered)
    with pytest.raises(ValueError, match="Out of range float values"):
        serialize_provider_request_body({"temperature": float("nan")})


def test_spec725_source_map_rejects_range_that_does_not_point_to_its_json_node():
    body = _body()
    source_map = build_request_body_source_map(body, _layers())
    broken = copy.deepcopy(source_map)
    context_entry = next(
        entry for entry in broken["entries"] if entry["pane_id"] == "40_high_freq"
    )
    context_entry["byte_start"] += 1
    with pytest.raises(ValueError, match="request_body_source_map_entry_invalid"):
        validate_request_body_source_map(body, broken)


def test_spec725_source_map_fails_closed_when_active_layer_is_not_in_wire_body():
    layers = _layers(high_freq="MISSING_FROM_REQUEST")
    with pytest.raises(
            ValueError,
            match="request_body_source_map_layer_missing:40_high_freq"):
        build_request_body_source_map(_body(high_freq="DIFFERENT"), layers)


def test_spec725_source_map_attributes_native_replay_to_its_source_block():
    replay = {
        "assistant_message": {
            "role": "assistant",
            "content": "",
            "reasoning_content": "reasoning",
            "tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {"name": "file_read", "arguments": "{}"},
            }],
        },
        "tool_results": [{
            "role": "tool",
            "tool_call_id": "call-1",
            "content": "ok",
        }],
    }
    layers = _layers(high_freq="", high_blocks=[], now="")
    for layer in layers:
        if layer["layer_key"] not in {"50_now"}:
            layer["content"] = [] if layer["layer_key"] == "30_lately" else ""
            layer.pop("block_index", None)
    layers[-3]["content"] = [{
        "role": "assistant",
        "source_block_id": "R000001-assistant-0000",
        "content": "context\nreasoning",
        "native_replay": replay,
    }]
    body = {
        "model": "fixture",
        "messages": [replay["assistant_message"], replay["tool_results"][0]],
    }
    source_map = build_request_body_source_map(body, layers)
    derived = [item for item in source_map["entries"] if item.get("source_mapping") == "derived"]
    assert [item["request_path"] for item in derived] == ["/messages/0", "/messages/1"]
    assert {item["block_id"] for item in derived} == {"R000001-assistant-0000"}


def test_spec725_source_map_keeps_repeated_native_replays_disjoint():
    replay = {
        "assistant_message": {
            "role": "assistant",
            "content": "",
            "reasoning_content": "same reasoning",
            "tool_calls": [{
                "id": "call-1",
                "type": "function",
                "function": {"name": "file_read", "arguments": "{}"},
            }],
        },
        "tool_results": [{
            "role": "tool",
            "tool_call_id": "call-1",
            "content": "ok",
        }],
    }
    layers = _layers(high_freq="", high_blocks=[], now="")
    for layer in layers:
        layer["content"] = [] if layer["layer_key"] in {"30_lately", "50_now"} else ""
        layer.pop("block_index", None)
    next(layer for layer in layers if layer["layer_key"] == "30_lately")["content"] = [
        {
            "role": "assistant",
            "source_block_id": block_id,
            "content": "context\nsame reasoning",
            "native_replay": replay,
        }
        for block_id in ("replay:a", "replay:b")
    ]
    body = {
        "model": "fixture",
        "messages": [
            replay["assistant_message"], replay["tool_results"][0],
            replay["assistant_message"], replay["tool_results"][0],
        ],
    }

    source_map = build_request_body_source_map(body, layers)
    derived = [item for item in source_map["entries"] if item.get("source_mapping") == "derived"]
    assert [item["request_path"] for item in derived] == [
        "/messages/0", "/messages/1", "/messages/2", "/messages/3",
    ]
    assert [item["block_id"] for item in derived] == [
        "replay:a", "replay:a", "replay:b", "replay:b",
    ]


@pytest.mark.parametrize(
    ("body", "expected_path"),
    [
        (
            {"model": "fixture", "messages": [{"role": "user", "content": "user"}]},
            "/messages/0/content",
        ),
        (
            {
                "model": "fixture",
                "input": [{
                    "role": "user",
                    "content": [{"type": "input_text", "text": "user"}],
                }],
            },
            "/input/0/content/0/text",
        ),
    ],
)
def test_spec725_source_map_never_uses_role_or_content_type_as_layer_text(
        body, expected_path):
    layers = _layers(
        high_freq="",
        high_blocks=[],
        now="user",
    )
    for layer in layers:
        if layer["layer_key"] not in {"50_now"}:
            layer["content"] = [] if layer["layer_key"] in {"30_lately"} else ""
            layer.pop("block_index", None)
    source_map = build_request_body_source_map(body, layers)
    entry = next(item for item in source_map["entries"] if item["pane_id"] == "50_now")
    assert entry["request_path"] == expected_path


def test_spec725_diff_finds_deleted_block_and_cross_round_compatible_frame(tmp_path):
    previous_high = "BASE\n\nREMOVED"
    previous_layers = _layers(high_freq=previous_high, high_blocks=[
        {"block_id": "high:base", "title": "Base", "char_start": 0, "char_end": 4},
        {"block_id": "high:removed", "title": "Removed", "char_start": 6, "char_end": 13},
    ])
    current_layers = _layers(high_freq="BASE", high_blocks=[
        {"block_id": "high:base", "title": "Base", "char_start": 0, "char_end": 4},
    ])
    _write_round(tmp_path, 8, [
        _event(8, "R000008:reaction:1", 1, _envelope(
            _body(high_freq=previous_high), previous_layers)),
    ])
    _write_round(tmp_path, 9, [
        _event(9, "R000009:setup:1", 1, _envelope(
            _body(high_freq="SETUP"), _layers(high_freq="SETUP"), lane="setup")),
        _event(9, "R000009:reaction:1", 2, _envelope(
            _body(high_freq="BASE"), current_layers)),
    ])

    result = build_request_prefix_diff(tmp_path, 9, "R000009:reaction:1")
    assert result["state"] == "ready"
    assert result["previous"]["frame_id"] == "R000008:reaction:1"
    assert result["target"] == {
        "pane_id": "40_high_freq",
        "block_id": "high:removed",
        "placement": "layer_end",
        "change_kind": "delete",
        "source_offset": 6,
        "block_offset": 0,
        "request_path": "/messages/1/content",
    }
    assert 0 < result["prefix_ratio"] < 1
    assert "request_body" not in json.dumps(result)


@pytest.mark.parametrize(
    ("previous_high", "current_high", "current_blocks", "placement", "kind", "block", "offset"),
    [
        (
            "ABCDEF", "ABCXEF",
            [{"block_id": "high:base", "title": "Base", "char_start": 0, "char_end": 6}],
            "block_inside", "replace", "high:base", 3,
        ),
        (
            "BASE", "BASE\n\nNEW",
            [
                {"block_id": "high:base", "title": "Base", "char_start": 0, "char_end": 4},
                {"block_id": "high:new", "title": "New", "char_start": 6, "char_end": 9},
            ],
            "block_boundary", "insert", "high:new", 0,
        ),
    ],
)
def test_spec725_diff_targets_block_replacement_and_insertion(
        tmp_path, previous_high, current_high, current_blocks,
        placement, kind, block, offset):
    previous_layers = _layers(high_freq=previous_high, high_blocks=[{
        "block_id": "high:base",
        "title": "Base",
        "char_start": 0,
        "char_end": len(previous_high),
    }])
    current_layers = _layers(
        high_freq=current_high,
        high_blocks=current_blocks,
    )
    _write_round(tmp_path, 11, [
        _event(11, "R000011:reaction:1", 1, _envelope(
            _body(high_freq=previous_high), previous_layers)),
        _event(11, "R000011:reaction:2", 2, _envelope(
            _body(high_freq=current_high), current_layers)),
    ])

    result = build_request_prefix_diff(tmp_path, 11, "R000011:reaction:2")
    assert result["state"] == "ready"
    assert result["target"]["placement"] == placement
    assert result["target"]["change_kind"] == kind
    assert result["target"]["block_id"] == block
    assert result["target"]["block_offset"] == offset


def test_spec725_diff_places_middle_deletion_before_current_successor(tmp_path):
    previous_high = "A\n\nREMOVE\n\nB"
    current_high = "A\n\nB"
    previous_layers = _layers(high_freq=previous_high, high_blocks=[
        {"block_id": "high:a", "title": "A", "char_start": 0, "char_end": 1},
        {"block_id": "high:removed", "title": "Removed", "char_start": 3, "char_end": 9},
        {"block_id": "high:b", "title": "B", "char_start": 11, "char_end": 12},
    ])
    current_layers = _layers(high_freq=current_high, high_blocks=[
        {"block_id": "high:a", "title": "A", "char_start": 0, "char_end": 1},
        {"block_id": "high:b", "title": "B", "char_start": 3, "char_end": 4},
    ])
    _write_round(tmp_path, 15, [
        _event(15, "R000015:reaction:1", 1, _envelope(
            _body(high_freq=previous_high), previous_layers)),
        _event(15, "R000015:reaction:2", 2, _envelope(
            _body(high_freq=current_high), current_layers)),
    ])

    result = build_request_prefix_diff(tmp_path, 15, "R000015:reaction:2")
    assert result["target"] == {
        "pane_id": "40_high_freq",
        "block_id": "high:b",
        "placement": "block_boundary",
        "change_kind": "delete",
        "source_offset": 3,
        "block_offset": 0,
        "request_path": "/messages/1/content",
    }


def test_spec725_diff_attributes_message_structure_to_its_context_block(tmp_path):
    layers = _layers()
    next(layer for layer in layers if layer["layer_key"] == "30_lately")["content"] = [{
        "role": "user",
        "source_block_id": "lately:1",
        "content": "SAME",
    }]
    previous = _body()
    previous["messages"].insert(1, {"role": "assistant", "content": "SAME"})
    current = copy.deepcopy(previous)
    current["messages"][1]["role"] = "user"
    _write_round(tmp_path, 16, [
        _event(16, "R000016:reaction:1", 1, _envelope(previous, layers)),
        _event(16, "R000016:reaction:2", 2, _envelope(current, layers)),
    ])

    result = build_request_prefix_diff(tmp_path, 16, "R000016:reaction:2")
    assert result["target"]["pane_id"] == "30_lately"
    assert result["target"]["block_id"] == "lately:1"
    assert result["target"]["placement"] == "block_inside"


def test_spec725_diff_uses_last_snapshot_and_never_compares_same_frame(tmp_path):
    older = _envelope(
        _body(now="before"), _layers(now="before"), connection="conn-b")
    previous_first_route = _envelope(
        _body(now="stale-route"), _layers(now="stale-route"), connection="conn-b")
    previous_final_route = _envelope(
        _body(now="other-route"), _layers(now="other-route"), connection="conn-c")
    first_route = _envelope(
        _body(now="first-route"), _layers(now="first-route"), connection="conn-a")
    final_route = _envelope(
        _body(now="final-route"), _layers(now="final-route"), connection="conn-b")
    _write_round(tmp_path, 17, [
        _event(17, "R000017:reaction:0", 1, older),
        _event(17, "R000017:reaction:1", 2, previous_first_route),
        _event(17, "R000017:reaction:1", 3, previous_final_route),
        _event(17, "R000017:reaction:2", 4, first_route),
        _event(17, "R000017:reaction:2", 5, final_route),
    ])

    result = build_request_prefix_diff(tmp_path, 17, "R000017:reaction:2")
    assert result["current"]["wire_body_sha256"] == final_route["wire_body_sha256"]
    assert result["previous"]["frame_id"] == "R000017:reaction:0"


def test_spec725_diff_uses_nearest_compatible_cleanup_and_rejects_broken_current_map(
        tmp_path):
    cleanup_one = _envelope(_body(now="cleanup-one"), _layers(now="cleanup-one"), lane="cleanup")
    reaction = _envelope(_body(now="reaction"), _layers(now="reaction"), lane="reaction.loop")
    cleanup_two = _envelope(_body(now="cleanup-two"), _layers(now="cleanup-two"), lane="cleanup")
    _write_round(tmp_path, 12, [
        _event(12, "R000012:cleanup:1", 1, cleanup_one),
        _event(12, "R000012:reaction:2", 2, reaction),
        _event(12, "R000012:cleanup:2", 3, cleanup_two),
    ])

    result = build_request_prefix_diff(tmp_path, 12, "R000012:cleanup:2")
    assert result["state"] == "ready"
    assert result["previous"]["frame_id"] == "R000012:cleanup:1"

    cleanup_two["request_body_source_map"]["entries"][0]["byte_end"] += 1
    _write_round(tmp_path, 13, [
        _event(13, "R000013:cleanup:1", 1, cleanup_one),
        _event(13, "R000013:cleanup:2", 2, cleanup_two),
    ])
    broken = build_request_prefix_diff(tmp_path, 13, "R000013:cleanup:2")
    assert broken == {
        "schema_version": "seed_gui_request_prefix_diff.v1",
        "state": "unavailable",
        "reason": "current_wire_contract_invalid",
    }


def test_spec725_diff_places_a_new_layer_at_the_layer_boundary(tmp_path):
    previous_layers = _layers()
    current_layers = _layers()
    periodic = next(
        layer for layer in current_layers if layer["layer_key"] == "20_periodic"
    )
    periodic.update({
        "content": "PERIODIC",
        "block_index": [{
            "block_id": "periodic:new",
            "title": "Periodic",
            "char_start": 0,
            "char_end": 8,
        }],
    })
    _write_round(tmp_path, 14, [
        _event(14, "R000014:reaction:1", 1, _envelope(
            _body_with_periodic(), previous_layers)),
        _event(14, "R000014:reaction:2", 2, _envelope(
            _body_with_periodic("PERIODIC"), current_layers)),
    ])

    result = build_request_prefix_diff(tmp_path, 14, "R000014:reaction:2")
    assert result["state"] == "ready"
    assert result["target"]["pane_id"] == "20_periodic"
    assert result["target"]["block_id"] == "periodic:new"
    assert result["target"]["placement"] == "layer_boundary"
    assert result["target"]["change_kind"] == "insert"


def test_spec725_diff_classifies_lately_tail_promotion_as_insert(tmp_path):
    def entry(block_id, content):
        return {
            "role": "system",
            "kind": "tool_fact",
            "source_block_id": block_id,
            "content": content,
        }

    existing = entry("R000639-system-0329", "existing")
    promoted = [
        entry(f"R000639-system-{index:04d}", f"promoted {index}")
        for index in range(330, 394)
    ]
    previous_layers = _layers()
    current_layers = _layers()
    next(layer for layer in previous_layers if layer["layer_key"] == "30_lately")[
        "content"] = [existing]
    next(layer for layer in current_layers if layer["layer_key"] == "30_lately")[
        "content"] = [existing, *promoted]

    def body(lately):
        payload = _body()
        payload["messages"][1:1] = [
            {"role": item["role"], "content": item["content"]}
            for item in lately
        ]
        return payload

    _write_round(tmp_path, 18, [
        _event(18, "R000018:reaction:4", 1, _envelope(
            body([existing]), previous_layers)),
        _event(18, "R000018:reaction:5", 2, _envelope(
            body([existing, *promoted]), current_layers)),
    ])

    result = build_request_prefix_diff(tmp_path, 18, "R000018:reaction:5")
    assert result["state"] == "ready"
    assert result["target"]["pane_id"] == "30_lately"
    assert result["target"]["block_id"] == "R000639-system-0330"
    assert result["target"]["change_kind"] == "insert"


def test_spec725_diff_reports_identical_and_skips_incompatible_connection(tmp_path):
    body = _body()
    layers = _layers()
    first = _envelope(body, layers, connection="conn-a")
    same = _envelope(body, layers, connection="conn-a")
    other = _envelope(body, layers, connection="conn-b")
    _write_round(tmp_path, 10, [
        _event(10, "R000010:reaction:1", 1, first),
        _event(10, "R000010:reaction:2", 2, same),
        _event(10, "R000010:reaction:3", 3, other),
    ])
    identical = build_request_prefix_diff(tmp_path, 10, "R000010:reaction:2")
    assert identical["state"] == "identical"
    assert identical["common_prefix_bytes"] == identical["current_wire_bytes"]
    unavailable = build_request_prefix_diff(tmp_path, 10, "R000010:reaction:3")
    assert unavailable == {
        "schema_version": "seed_gui_request_prefix_diff.v1",
        "state": "unavailable",
        "reason": "compatible_previous_frame_not_found",
    }
