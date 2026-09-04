"""分身本地跨轮正文常驻引用账本。"""

from __future__ import annotations

from copy import deepcopy
import json
import os
import threading

from data.atomic_write import atomic_write_json, atomic_write_text
from errors import ReadError, WriteError
from paths import RESIDENT_LIST_JSON


SCHEMA_VERSION = "resident_list.v1"
ITEM_TYPES = frozenset({"memory", "container", "relation"})
_LOCK = threading.RLock()


class ResidentListError(ValueError):
    pass


def default_resident_list():
    return {
        "schema_version": SCHEMA_VERSION,
        "revision": 0,
        "items": [],
    }


def _clean(value):
    return str(value or "").strip().strip("`")


def _normalize_item(raw):
    if not isinstance(raw, dict):
        raise ResidentListError("resident_item_invalid")
    allowed = {"item_type", "item_id", "target_file"}
    unknown = set(raw).difference(allowed)
    if unknown:
        raise ResidentListError(
            "resident_item_unknown_fields:" + ",".join(sorted(unknown)))
    item_type = _clean(raw.get("item_type"))
    item_id = _clean(raw.get("item_id"))
    if item_type not in ITEM_TYPES:
        raise ResidentListError("resident_item_type_invalid")
    if not item_id:
        raise ResidentListError("resident_item_id_required")
    target_file = _clean(raw.get("target_file"))
    if item_type == "container" and not target_file:
        raise ResidentListError("resident_container_target_required")
    if item_type == "container" and (
        target_file != os.path.basename(target_file)
        or "/" in target_file
        or "\\" in target_file
    ):
        raise ResidentListError("resident_container_target_invalid")
    if item_type != "container" and target_file:
        raise ResidentListError("resident_target_not_allowed")
    normalized = {"item_type": item_type, "item_id": item_id}
    if target_file:
        normalized["target_file"] = target_file
    return normalized


def item_key(item):
    normalized = _normalize_item(item)
    return (
        normalized["item_type"],
        normalized["item_id"],
        normalized.get("target_file", ""),
    )


def normalize_resident_list(raw, *, allow_legacy_empty=True):
    if raw == {"items": []} and allow_legacy_empty:
        return default_resident_list(), True
    if not isinstance(raw, dict):
        raise ResidentListError("resident_list_invalid")
    current_fields = {"schema_version", "revision", "items"}
    legacy_fields = current_fields | {"next_sequence"}
    if frozenset(raw) not in {frozenset(current_fields), frozenset(legacy_fields)}:
        raise ResidentListError("resident_list_shape_unknown")
    if raw.get("schema_version") != SCHEMA_VERSION:
        raise ResidentListError("resident_list_schema_unknown")
    revision = raw.get("revision")
    if (
        isinstance(revision, bool)
        or not isinstance(revision, int)
        or revision < 0
    ):
        raise ResidentListError("resident_revision_invalid")
    if not isinstance(raw.get("items"), list):
        raise ResidentListError("resident_items_invalid")
    legacy = "next_sequence" in raw
    legacy_items = raw["items"]
    if legacy:
        sequences = []
        for item in legacy_items:
            if not isinstance(item, dict):
                raise ResidentListError("resident_item_invalid")
            sequence = item.get("sequence")
            if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
                raise ResidentListError("resident_sequence_invalid")
            sequences.append(sequence)
        if len(sequences) != len(set(sequences)):
            raise ResidentListError("resident_sequence_duplicate")
        if sequences != sorted(sequences):
            raise ResidentListError("resident_sequence_order_invalid")
        next_sequence = raw.get("next_sequence")
        if (
            isinstance(next_sequence, bool)
            or not isinstance(next_sequence, int)
            or next_sequence < 1
            or (sequences and next_sequence <= max(sequences))
        ):
            raise ResidentListError("resident_next_sequence_conflict")
        legacy_items = [
            {key: value for key, value in item.items() if key != "sequence"}
            for item in legacy_items
        ]
    items = [_normalize_item(item) for item in legacy_items]
    keys = [item_key(item) for item in items]
    if len(keys) != len(set(keys)):
        raise ResidentListError("resident_item_duplicate")
    return {
        "schema_version": SCHEMA_VERSION,
        "revision": revision,
        "items": items,
    }, legacy


class ResidentListStore:
    def __init__(self, path=None):
        self.path = str(path or RESIDENT_LIST_JSON)

    def _read_raw(self):
        if not os.path.isfile(self.path):
            return default_resident_list()
        try:
            with open(self.path, "r", encoding="utf-8-sig") as handle:
                return json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ReadError(self.path, cause=exc) from exc

    def load(self):
        with _LOCK:
            normalized, _legacy = normalize_resident_list(self._read_raw())
            return deepcopy(normalized)

    def reconcile(self):
        """升级已知空旧形状；未知非空形状保持原文件并拒绝 READY。"""
        preview = self.preview_reconcile()
        with _LOCK:
            if preview["status"] == "applied":
                self._write_verified(preview["document"])
            return {
                "schema_version": "resident_list_reconcile_receipt.v1",
                "status": preview["status"],
                "revision": preview["document"]["revision"],
                "items": len(preview["document"]["items"]),
            }

    def preview_reconcile(self):
        """Return the normalized known shape without mutating the source file."""
        with _LOCK:
            missing = not os.path.isfile(self.path)
            normalized, legacy = normalize_resident_list(self._read_raw())
            return {
                "status": "applied" if legacy or missing else "noop",
                "document": deepcopy(normalized),
            }

    def preview_add(self, item):
        with _LOCK:
            document = self.load()
            normalized = _normalize_item(item)
            key = item_key(normalized)
            if any(item_key(existing) == key for existing in document["items"]):
                return document, False
            document["items"].append(normalized)
            document["revision"] += 1
            return document, True

    def add(self, item, *, candidate=None, expected_revision=None):
        with _LOCK:
            before = self.snapshot_bytes()
            current = self.load()
            normalized_item = _normalize_item(item)
            if candidate is None:
                document, changed = self.preview_add(normalized_item)
            else:
                document, legacy = normalize_resident_list(candidate)
                if legacy:
                    raise ResidentListError("resident_candidate_legacy")
                if expected_revision != current["revision"]:
                    raise ResidentListError("resident_revision_conflict")
                key = item_key(normalized_item)
                already_present = any(
                    item_key(existing) == key for existing in current["items"]
                )
                expected_items = list(current["items"])
                if not already_present:
                    expected_items.append(normalized_item)
                expected_candidate = {
                    "schema_version": SCHEMA_VERSION,
                    "revision": current["revision"] + (0 if already_present else 1),
                    "items": expected_items,
                }
                if document != expected_candidate:
                    raise ResidentListError("resident_candidate_mismatch")
                changed = not already_present
            if changed:
                self._write_with_rollback(document, before)
            return {
                "status": "applied" if changed else "noop",
                "revision": document["revision"],
                "item": deepcopy(normalized_item),
            }

    def remove_matching(self, *, item_type, item_id, target_file=""):
        """Remove every matching reference; omitted container target means all files."""
        clean_type = _clean(item_type)
        clean_id = _clean(item_id)
        clean_target = _clean(target_file)
        if clean_type not in ITEM_TYPES:
            raise ResidentListError("resident_item_type_invalid")
        if not clean_id:
            raise ResidentListError("resident_item_id_required")
        if clean_type != "container" and clean_target:
            raise ResidentListError("resident_target_not_allowed")
        with _LOCK:
            before = self.snapshot_bytes()
            document = self.load()
            removed_items = []
            remaining = []
            for item in document["items"]:
                matches = (
                    item.get("item_type") == clean_type
                    and item.get("item_id") == clean_id
                    and (
                        not clean_target
                        or item.get("target_file", "") == clean_target
                    )
                )
                if matches:
                    removed_items.append(deepcopy(item))
                else:
                    remaining.append(item)
            if removed_items:
                document["items"] = remaining
                document["revision"] += 1
                self._write_with_rollback(document, before)
            return {
                "status": "applied" if removed_items else "not_found",
                "revision": document["revision"],
                "removed": bool(removed_items),
                "removed_items": removed_items,
            }

    def contains(self, *, item_type, item_id, target_file=""):
        clean_type = _clean(item_type)
        clean_id = _clean(item_id)
        clean_target = _clean(target_file)
        return any(
            item.get("item_type") == clean_type
            and item.get("item_id") == clean_id
            and (
                clean_type != "container"
                or not clean_target
                or item.get("target_file", "") == clean_target
            )
            for item in self.load()["items"]
        )

    def replace(self, document):
        with _LOCK:
            before = self.snapshot_bytes()
            normalized, _legacy = normalize_resident_list(document)
            self._write_with_rollback(normalized, before)
            return deepcopy(normalized)

    def snapshot_bytes(self):
        with _LOCK:
            if not os.path.isfile(self.path):
                return None
            try:
                with open(self.path, "rb") as handle:
                    return handle.read()
            except OSError as exc:
                raise ReadError(self.path, cause=exc) from exc

    def restore_bytes(self, payload):
        with _LOCK:
            if payload is None:
                if os.path.exists(self.path):
                    os.remove(self.path)
                return
            try:
                text = payload.decode("utf-8")
                raw = json.loads(text.lstrip("\ufeff"))
                normalize_resident_list(raw)
                atomic_write_text(self.path, text, newline="")
                if self.snapshot_bytes() != payload:
                    raise WriteError(
                        self.path,
                        message="resident_list_restore_readback_mismatch",
                    )
            except (UnicodeError, json.JSONDecodeError, OSError) as exc:
                raise WriteError(self.path, cause=exc) from exc

    def _write_verified(self, document):
        normalized, _legacy = normalize_resident_list(document)
        try:
            atomic_write_json(self.path, normalized)
        except Exception as exc:
            if isinstance(exc, WriteError):
                raise
            raise WriteError(self.path, cause=exc) from exc
        actual, _legacy = normalize_resident_list(self._read_raw())
        if actual != normalized:
            raise WriteError(self.path, message="resident_list_readback_mismatch")

    def _write_with_rollback(self, document, before):
        try:
            self._write_verified(document)
        except Exception as exc:
            try:
                self.restore_bytes(before)
            except Exception as rollback_exc:
                raise WriteError(
                    self.path,
                    message=(
                        "resident_list_rollback_failed:"
                        f"{type(rollback_exc).__name__}"
                    ),
                ) from exc
            raise
