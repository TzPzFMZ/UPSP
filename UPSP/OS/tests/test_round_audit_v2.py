from __future__ import annotations

import json
import hashlib
from pathlib import Path

import pytest

from data.provider_request_wire import (
    build_request_body_source_map,
    provider_request_body_sha256,
    serialize_provider_request_body,
)
from data.round_audit_codec import RoundAuditDecoder, RoundAuditEncoder
from data.round_snapshot_store import RoundSnapshotStore


def _envelope(body, *, connection="connection-a", lane="reaction"):
    wire = serialize_provider_request_body(body)
    digest = provider_request_body_sha256(body)
    return {
        "schema": "provider_request.v1",
        "created_at": "2026-08-09T12:00:00+08:00",
        "call": {"step": "reaction", "channel": "reaction.loop"},
        "provider": {
            "provider": "openai_chat",
            "model": "model-a",
            "profile_id": "profile-a",
            "connection_id": connection,
        },
        "request_contract_audit": {
            "prompt_cache_key": "stable-cache-key",
            "prompt_cache_lane": lane,
        },
        "request_body": body,
        "request_body_sha256": digest,
        "wire_body_encoding": "canonical_json_utf8.v1",
        "wire_body_sha256": digest,
        "wire_body_bytes": len(wire),
        "request_body_source_map": build_request_body_source_map(body, []),
    }


def _layers(changing="one"):
    layers = []
    for key, content in (
        ("10_permanent", "fixed"),
        ("20_periodic", ""),
        ("30_lately", "lately"),
        ("40_high_freq", changing),
        ("50_now", "now"),
        ("60_statusbar", "status"),
        ("99_popup", ""),
    ):
        layers.append({
            "schema": "context_layer.v1",
            "layer_key": key,
            "content": content,
            "chars": len(content),
            "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest(),
        })
    return {
        "schema": "context_layers_snapshot.v1",
        "source": "context/reaction/layers",
        "layer_order": [item["layer_key"] for item in layers],
        "layers": layers,
    }


def _raw_events(path):
    return [
        json.loads(line)
        for line in Path(path).read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def test_v2_delta_and_layer_refs_reconstruct_exact_wire(tmp_path):
    store = RoundSnapshotStore(tmp_path / "context")
    store.start_round(731, "interactive")
    body1 = {
        "model": "model-a",
        "messages": [{"role": "user", "content": "甲" * 5000}],
    }
    body2 = {
        "model": "model-a",
        "messages": [{"role": "user", "content": "甲" * 5000 + "乙"}],
    }
    store.record_step_input(
        731, "reaction", 1,
        provider_request_envelope=_envelope(body1),
        layers_snapshot=_layers("one"),
    )
    store.record_step_input(
        731, "reaction", 2,
        provider_request_envelope=_envelope(body2),
        layers_snapshot=_layers("two"),
    )

    path = tmp_path / "context" / "round" / "round_731.jsonl"
    raw = [
        event for event in _raw_events(path)
        if event["event_type"] == "step_input_snapshot"
    ]
    first = raw[0]["payload"]["audit_snapshot"]
    second = raw[1]["payload"]["audit_snapshot"]
    assert first["provider_request"]["mode"] == "full"
    assert second["provider_request"]["mode"] == "prefix_delta"
    assert len(first["layers_snapshot"]["new_blobs"]) == 7
    assert len(second["layers_snapshot"]["new_blobs"]) == 1
    assert "messages" not in raw[0]["payload"]
    assert "step_md" not in raw[0]["payload"]

    decoded = store.read_events(731)
    snapshots = [
        event for event in decoded
        if event["event_type"] == "step_input_snapshot"
    ]
    assert snapshots[0]["payload"]["provider_request_envelope"]["request_body"] == body1
    assert snapshots[1]["payload"]["provider_request_envelope"]["request_body"] == body2
    assert snapshots[1]["payload"]["layers_snapshot"] == _layers("two")


def test_v2_incompatible_route_and_cleanup_are_full_anchors(tmp_path):
    store = RoundSnapshotStore(tmp_path / "context")
    store.start_round(1, "interactive")
    body = {"model": "model-a", "messages": [{"role": "user", "content": "x" * 5000}]}
    store.record_step_input(1, "reaction", 1, provider_request_envelope=_envelope(body), layers_snapshot=_layers())
    store.record_step_input(1, "reaction", 2, provider_request_envelope=_envelope(body, connection="connection-b"), layers_snapshot=_layers())
    store.record_step_input(1, "cleanup", 1, provider_request_envelope=_envelope(body), layers_snapshot=_layers())

    raw = [
        event["payload"]["audit_snapshot"]["provider_request"]["mode"]
        for event in _raw_events(tmp_path / "context" / "round" / "round_1.jsonl")
        if event["event_type"] == "step_input_snapshot"
    ]
    assert raw == ["full", "full", "full"]


def test_v2_missing_compatibility_key_never_creates_delta(tmp_path):
    store = RoundSnapshotStore(tmp_path / "context")
    store.start_round(1, "interactive")
    body = {"model": "model-a", "messages": [{"role": "user", "content": "x" * 5000}]}
    envelope = _envelope(body)
    envelope["request_contract_audit"].pop("prompt_cache_key")
    store.record_step_input(1, "reaction", 1, provider_request_envelope=envelope, layers_snapshot=_layers())
    store.record_step_input(1, "reaction", 2, provider_request_envelope=envelope, layers_snapshot=_layers())

    modes = [
        event["payload"]["audit_snapshot"]["provider_request"]["mode"]
        for event in _raw_events(tmp_path / "context" / "round" / "round_1.jsonl")
        if event["event_type"] == "step_input_snapshot"
    ]
    assert modes == ["full", "full"]


def test_v2_rejects_malformed_wire_contract():
    encoder = RoundAuditEncoder()
    envelope = _envelope({"model": "model-a", "messages": []})
    envelope["wire_body_sha256"] = "0" * 64

    with pytest.raises(ValueError):
        encoder.encode(
            envelope,
            _layers(),
            phase="reaction",
            event_id="e1",
        )


def test_v2_rejects_layer_order_drift():
    layers = _layers()
    layers["layer_order"] = list(reversed(layers["layer_order"]))

    with pytest.raises(ValueError, match="layer_order_mismatch"):
        RoundAuditEncoder().encode(
            _envelope({"model": "model-a", "messages": []}),
            layers,
            phase="reaction",
            event_id="e1",
        )


def test_v2_delta_can_split_utf8_codepoint_and_still_reconstruct(tmp_path):
    store = RoundSnapshotStore(tmp_path / "context")
    store.start_round(1, "interactive")
    body1 = {"model": "model-a", "messages": [{"role": "user", "content": "x" * 5000 + "你"}]}
    body2 = {"model": "model-a", "messages": [{"role": "user", "content": "x" * 5000 + "佢"}]}
    store.record_step_input(1, "reaction", 1, provider_request_envelope=_envelope(body1), layers_snapshot=_layers())
    store.record_step_input(1, "reaction", 2, provider_request_envelope=_envelope(body2), layers_snapshot=_layers())

    raw = [
        event for event in _raw_events(tmp_path / "context" / "round" / "round_1.jsonl")
        if event["event_type"] == "step_input_snapshot"
    ]
    delta = raw[1]["payload"]["audit_snapshot"]["provider_request"]
    assert delta["mode"] == "prefix_delta"
    decoded = store.read_events(1)
    snapshots = [event for event in decoded if event["event_type"] == "step_input_snapshot"]
    assert snapshots[1]["payload"]["provider_request_envelope"]["request_body"] == body2


def test_v2_corrupt_delta_fails_closed(tmp_path):
    store = RoundSnapshotStore(tmp_path / "context")
    store.start_round(1, "interactive")
    body = {"model": "model-a", "messages": [{"role": "user", "content": "x" * 5000}]}
    store.record_step_input(1, "reaction", 1, provider_request_envelope=_envelope(body), layers_snapshot=_layers())
    changed = {
        "model": "model-a",
        "messages": [{"role": "user", "content": "x" * 5000 + "y"}],
    }
    store.record_step_input(1, "reaction", 2, provider_request_envelope=_envelope(changed), layers_snapshot=_layers())
    events = _raw_events(tmp_path / "context" / "round" / "round_1.jsonl")
    snapshot = [event for event in events if event["event_type"] == "step_input_snapshot"][1]
    snapshot["payload"]["audit_snapshot"]["provider_request"]["base_wire_sha256"] = "0" * 64

    decoder = RoundAuditDecoder()
    decoder.feed([event for event in events if event["event_type"] == "step_input_snapshot"][0])
    with pytest.raises(ValueError, match="base_sha_mismatch"):
        decoder.feed(snapshot)


def test_v2_append_failure_does_not_commit_encoder_state(tmp_path, monkeypatch):
    store = RoundSnapshotStore(tmp_path / "context")
    store.start_round(1, "interactive")
    body1 = {
        "model": "model-a",
        "messages": [{
            "role": "user",
            "content": "literal sk-12345678 " + "x" * 5000,
        }],
    }
    body2 = {
        "model": "model-a",
        "messages": [{
            "role": "user",
            "content": "literal sk-12345678 " + "x" * 5000 + "y",
        }],
    }
    store.record_step_input(
        1, "reaction", 1,
        provider_request_envelope=_envelope(body1),
        layers_snapshot=_layers(),
    )
    append = store.append_event

    def fail_append(*_args, **_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(store, "append_event", fail_append)
    with pytest.raises(OSError, match="disk full"):
        store.record_step_input(
            1, "reaction", 2,
            provider_request_envelope=_envelope(body2),
            layers_snapshot=_layers("failed"),
        )
    monkeypatch.setattr(store, "append_event", append)

    store.record_step_input(
        1, "reaction", 2,
        provider_request_envelope=_envelope(body2),
        layers_snapshot=_layers("saved"),
    )

    snapshots = [
        event for event in store.read_events(1)
        if event["event_type"] == "step_input_snapshot"
    ]
    assert snapshots[-1]["payload"]["provider_request_envelope"]["request_body"] == body2
    assert snapshots[-1]["payload"]["layers_snapshot"] == _layers("saved")


def test_call_events_reference_snapshot_without_copying_envelope(tmp_path):
    store = RoundSnapshotStore(tmp_path / "context")
    store.start_round(1, "interactive")
    body = {"model": "model-a", "messages": [{"role": "user", "content": "hello"}]}
    envelope = _envelope(body)
    snapshot = store.record_step_input(
        1, "reaction", 1,
        provider_request_envelope=envelope,
        layers_snapshot=_layers(),
    )
    contract = {
        "logical_call_id": "R000001:reaction:1",
        "route_slot": 1,
        "provider_request_envelope": envelope,
    }
    store.record_llm_call_started(1, "reaction", 1, contract)
    store.record_llm_output(1, "reaction", 1, {
        "response": "ok",
        "provider_request_envelope": envelope,
    })
    raw = _raw_events(tmp_path / "context" / "round" / "round_1.jsonl")
    for event in raw[-2:]:
        assert "provider_request_envelope" not in event["payload"]
        assert event["payload"]["request_snapshot_event_id"] == snapshot["event_id"]
        assert event["payload"]["wire_body_sha256"] == envelope["wire_body_sha256"]
