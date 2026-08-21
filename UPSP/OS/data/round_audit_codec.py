"""Compact round-audit step snapshots without changing provider wire truth."""
from __future__ import annotations

import base64
import copy
import hashlib
import json
from pathlib import Path

from data.provider_request_wire import (
    WIRE_BODY_ENCODING,
    serialize_provider_request_body,
    verified_provider_request_wire,
)


STEP_SNAPSHOT_SCHEMA = "round_step_snapshot.v2"
LAYER_REFS_SCHEMA = "context_layers_refs.v1"


def _canonical_bytes(value):
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _blob_sha256(value):
    return hashlib.sha256(_canonical_bytes(value)).hexdigest()


def common_prefix_bytes(left, right):
    limit = min(len(left), len(right))
    offset = 0
    while offset < limit and left[offset] == right[offset]:
        offset += 1
    return offset


def _api_shape(body):
    if not isinstance(body, dict):
        return ""
    shape = []
    for key in ("messages", "input", "instructions", "system", "tools"):
        if key in body:
            value = body[key]
            shape.append((
                key,
                "list" if isinstance(value, list) else type(value).__name__,
            ))
    return json.dumps(shape, separators=(",", ":"))


def request_compatibility_key(envelope):
    provider = envelope.get("provider")
    provider = provider if isinstance(provider, dict) else {}
    audit = envelope.get("request_contract_audit")
    audit = audit if isinstance(audit, dict) else {}
    values = (
        str(audit.get("prompt_cache_key") or "").strip(),
        str(provider.get("connection_id") or "").strip(),
        str(provider.get("provider") or "").strip(),
        str(provider.get("model") or "").strip(),
        str(audit.get("prompt_cache_lane") or "").strip(),
        _api_shape(envelope.get("request_body")),
    )
    return values if all(values) else None


class RoundAuditEncoder:
    """Stateful encoder scoped to one append-only Round file at a time."""

    def __init__(self):
        self._layer_blobs = {}
        self._last_reaction = None

    def clone(self):
        candidate = RoundAuditEncoder()
        candidate._layer_blobs = dict(self._layer_blobs)
        candidate._last_reaction = self._last_reaction
        return candidate

    def encode(self, envelope, layers_snapshot, *, phase, event_id):
        return {
            "schema_version": STEP_SNAPSHOT_SCHEMA,
            "provider_request": self._encode_request(
                envelope,
                phase=str(phase or ""),
                event_id=str(event_id or ""),
            ),
            "layers_snapshot": self._encode_layers(layers_snapshot),
        }

    def _encode_request(self, envelope, *, phase, event_id):
        wire = verified_provider_request_wire(envelope)

        meta = {
            key: copy.deepcopy(value)
            for key, value in envelope.items()
            if key not in {
                "request_body",
                "request_body_source_map",
                "request_body_sha256",
                "wire_body_encoding",
                "wire_body_sha256",
                "wire_body_bytes",
            }
        }
        record = {
            "mode": "full",
            "meta": meta,
            "request_body": copy.deepcopy(envelope["request_body"]),
            "request_body_source_map": copy.deepcopy(
                envelope.get("request_body_source_map")
            ),
            "wire_body_encoding": WIRE_BODY_ENCODING,
            "wire_body_sha256": envelope["wire_body_sha256"],
            "wire_body_bytes": len(wire),
        }

        compatible = request_compatibility_key(envelope)
        previous = self._last_reaction
        if (
            phase == "reaction"
            and previous
            and compatible is not None
            and compatible == previous["key"]
        ):
            prefix = common_prefix_bytes(previous["wire"], wire)
            suffix = base64.b64encode(wire[prefix:]).decode("ascii")
            delta = {
                "mode": "prefix_delta",
                "meta": meta,
                "request_body_source_map": copy.deepcopy(
                    envelope.get("request_body_source_map")
                ),
                "base_event_id": previous["event_id"],
                "base_wire_sha256": previous["sha256"],
                "common_prefix_bytes": prefix,
                "suffix_b64": suffix,
                "wire_body_encoding": WIRE_BODY_ENCODING,
                "wire_body_sha256": envelope["wire_body_sha256"],
                "wire_body_bytes": len(wire),
            }
            if len(_canonical_bytes(delta)) < len(_canonical_bytes(record)):
                record = delta

        if phase == "reaction":
            self._last_reaction = {
                "event_id": event_id,
                "key": compatible,
                "wire": wire,
                "sha256": envelope["wire_body_sha256"],
            }
        return record

    def _encode_layers(self, snapshot):
        if (
            not isinstance(snapshot, dict)
            or snapshot.get("schema") != "context_layers_snapshot.v1"
            or not isinstance(snapshot.get("layers"), list)
        ):
            raise ValueError("round_layers_snapshot_invalid")
        layers = snapshot["layers"]
        if any(not isinstance(layer, dict) for layer in layers):
            raise ValueError("round_layer_blob_invalid")
        keys = [str(layer.get("layer_key") or "") for layer in layers]
        if not all(keys) or len(set(keys)) != len(keys) or snapshot.get("layer_order") != keys:
            raise ValueError("round_layer_order_mismatch")
        refs = []
        new_blobs = []
        for layer in layers:
            digest = _blob_sha256(layer)
            refs.append({
                "layer_key": str(layer.get("layer_key") or ""),
                "blob_sha256": digest,
            })
            if digest not in self._layer_blobs:
                blob = copy.deepcopy(layer)
                self._layer_blobs[digest] = blob
                new_blobs.append({"blob_sha256": digest, "layer": blob})
        return {
            "mode": "refs",
            "schema": LAYER_REFS_SCHEMA,
            "source": snapshot.get("source"),
            "layer_order": keys,
            "refs": refs,
            "new_blobs": new_blobs,
        }


class RoundAuditDecoder:
    """Materialize v2 events into the existing in-memory reader contract."""

    def __init__(self):
        self._layer_blobs = {}
        self._wires = {}

    def feed(self, event):
        if not isinstance(event, dict):
            raise ValueError("round_audit_event_invalid")
        if event.get("event_type") != "step_input_snapshot":
            return copy.deepcopy(event)
        payload = event.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        compact = payload.get("audit_snapshot")
        if not isinstance(compact, dict):
            return copy.deepcopy(event)
        if compact.get("schema_version") != STEP_SNAPSHOT_SCHEMA:
            raise ValueError("round_step_snapshot_schema_invalid")
        materialized = copy.deepcopy(event)
        target = materialized.setdefault("payload", {})
        wires = dict(self._wires)
        blobs = dict(self._layer_blobs)
        try:
            target["provider_request_envelope"] = self._decode_request(
                compact.get("provider_request"),
                event_id=str(event.get("event_id") or ""),
            )
            target["layers_snapshot"] = self._decode_layers(
                compact.get("layers_snapshot")
            )
        except Exception:
            self._wires = wires
            self._layer_blobs = blobs
            raise
        return materialized

    def _decode_request(self, record, *, event_id):
        if not isinstance(record, dict):
            raise ValueError("round_request_snapshot_invalid")
        mode = record.get("mode")
        if mode == "full":
            body = copy.deepcopy(record.get("request_body"))
            wire = serialize_provider_request_body(body)
        elif mode == "prefix_delta":
            base_id = str(record.get("base_event_id") or "")
            base = self._wires.get(base_id)
            if base is None:
                raise ValueError("round_request_delta_base_missing")
            if hashlib.sha256(base).hexdigest() != record.get("base_wire_sha256"):
                raise ValueError("round_request_delta_base_sha_mismatch")
            prefix = record.get("common_prefix_bytes")
            if isinstance(prefix, bool) or not isinstance(prefix, int):
                raise ValueError("round_request_delta_prefix_invalid")
            if prefix < 0 or prefix > len(base):
                raise ValueError("round_request_delta_prefix_invalid")
            try:
                suffix = base64.b64decode(
                    str(record.get("suffix_b64") or ""), validate=True
                )
            except (ValueError, TypeError) as exc:
                raise ValueError("round_request_delta_suffix_invalid") from exc
            wire = base[:prefix] + suffix
            try:
                body = json.loads(wire.decode("utf-8"))
            except (UnicodeError, json.JSONDecodeError) as exc:
                raise ValueError("round_request_delta_body_invalid") from exc
        else:
            raise ValueError("round_request_snapshot_mode_invalid")

        expected_size = record.get("wire_body_bytes")
        expected_sha = str(record.get("wire_body_sha256") or "")
        if (
            len(wire) != expected_size
            or hashlib.sha256(wire).hexdigest() != expected_sha
        ):
            raise ValueError("round_request_wire_mismatch")
        meta = record.get("meta")
        envelope = copy.deepcopy(meta if isinstance(meta, dict) else {})
        envelope.update({
            "request_body": body,
            "request_body_source_map": copy.deepcopy(
                record.get("request_body_source_map")
            ),
            "request_body_sha256": expected_sha,
            "wire_body_encoding": record.get("wire_body_encoding"),
            "wire_body_sha256": expected_sha,
            "wire_body_bytes": expected_size,
        })
        verified_provider_request_wire(envelope)
        self._wires[event_id] = wire
        return envelope

    def _decode_layers(self, record):
        if not isinstance(record, dict):
            raise ValueError("round_layers_snapshot_invalid")
        mode = record.get("mode")
        if mode != "refs" or record.get("schema") != LAYER_REFS_SCHEMA:
            raise ValueError("round_layers_snapshot_mode_invalid")
        for item in record.get("new_blobs") or []:
            if not isinstance(item, dict) or not isinstance(item.get("layer"), dict):
                raise ValueError("round_layer_blob_invalid")
            digest = str(item.get("blob_sha256") or "")
            if _blob_sha256(item["layer"]) != digest:
                raise ValueError("round_layer_blob_sha_mismatch")
            existing = self._layer_blobs.get(digest)
            if existing is not None and existing != item["layer"]:
                raise ValueError("round_layer_blob_collision")
            self._layer_blobs[digest] = copy.deepcopy(item["layer"])
        layers = []
        for ref in record.get("refs") or []:
            if not isinstance(ref, dict):
                raise ValueError("round_layer_ref_invalid")
            digest = str(ref.get("blob_sha256") or "")
            layer = self._layer_blobs.get(digest)
            if layer is None:
                raise ValueError("round_layer_blob_missing")
            if str(layer.get("layer_key") or "") != str(ref.get("layer_key") or ""):
                raise ValueError("round_layer_ref_key_mismatch")
            layers.append(copy.deepcopy(layer))
        order = copy.deepcopy(record.get("layer_order") or [])
        if order and order != [layer.get("layer_key") for layer in layers]:
            raise ValueError("round_layer_order_mismatch")
        return {
            "schema": "context_layers_snapshot.v1",
            "source": record.get("source"),
            "layer_order": order,
            "layers": layers,
        }


def read_round_audit_file(path):
    """Read v1/v2 JSONL through the one fail-closed materializer."""
    decoder = RoundAuditDecoder()
    events = []
    with Path(path).open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, 1):
            text = line.strip()
            if not text:
                continue
            try:
                event = json.loads(text)
                events.append(decoder.feed(event))
            except (json.JSONDecodeError, TypeError, ValueError) as exc:
                raise ValueError(
                    f"round_audit_invalid_line:{line_number}:{exc}"
                ) from exc
    return events
