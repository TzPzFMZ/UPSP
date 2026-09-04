"""Instance-local manual periodic-memory mount ledger."""
import json
import os
import re

from data.atomic_write import atomic_write_json
from constants import local_now
from errors import ReadError
from paths import STM_CONTEXT_DIR


DEFAULT_PERIODIC_MOUNTS_PATH = os.path.join(STM_CONTEXT_DIR, "periodic_mounts.json")
PERIODIC_MOUNTS_SCHEMA = "periodic_mounts.v3"


class PeriodicMountStore:
    def __init__(self, periodic_mounts_path=None, now_fn=None):
        self.path = periodic_mounts_path or DEFAULT_PERIODIC_MOUNTS_PATH
        self.now_fn = now_fn or (lambda: local_now().isoformat())

    def load(self):
        if not os.path.isfile(self.path):
            return self._default()
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ReadError(self.path, cause=exc) from exc
        if not isinstance(data, dict):
            raise ReadError(self.path)
        if data.get("schema_version") == PERIODIC_MOUNTS_SCHEMA:
            self._validate_v3(data)
            return data
        if data.get("schema_version") == "periodic_mounts.v2":
            self._validate_v2(data)
            migrated = dict(data)
            migrated["schema_version"] = PERIODIC_MOUNTS_SCHEMA
            migrated["pending_memory_items"] = []
            return self.save_document(migrated)

        # Empty legacy files have no ownership ambiguity and are upgraded in
        # place. Non-empty legacy shapes remain readable but must never be
        # overwritten by the manual mutation path.
        legacy_items = data.get("periodic_memory_items")
        if legacy_items == [] or not data:
            return self._default()
        return data

    def save_document(self, data):
        document = dict(data or {})
        document["schema_version"] = PERIODIC_MOUNTS_SCHEMA
        document["updated_at"] = self.now_fn()
        self._validate_v3(document)
        self._atomic_json(document)
        return document

    @staticmethod
    def _default():
        return {
            "schema_version": PERIODIC_MOUNTS_SCHEMA,
            "updated_at": "",
            "instance_id": "",
            "periodic_memory_items": [],
            "pending_memory_items": [],
        }

    def _validate_v2(self, data):
        items = data.get("periodic_memory_items")
        instance_id = data.get("instance_id", "")
        updated_at = data.get("updated_at", "")
        if (
            data.get("schema_version") not in {
                "periodic_mounts.v2",
                PERIODIC_MOUNTS_SCHEMA,
            }
            or not isinstance(items, list)
            or not isinstance(instance_id, str)
            or not isinstance(updated_at, str)
            or (items and (not instance_id.strip() or not updated_at.strip()))
        ):
            raise ReadError(self.path)
        seen = set()
        for item in items:
            if not isinstance(item, dict):
                raise ReadError(self.path)
            mem_id = item.get("id")
            if (
                not isinstance(mem_id, str)
                or not re.fullmatch(r"MEM-[0-9A-F]{8}", mem_id)
                or mem_id in seen
                or item.get("source") != "user_manual"
                or not isinstance(item.get("mounted_at"), str)
                or not item["mounted_at"].strip()
            ):
                raise ReadError(self.path)
            seen.add(mem_id)

    def _validate_v3(self, data):
        if data.get("schema_version") != PERIODIC_MOUNTS_SCHEMA:
            raise ReadError(self.path)
        legacy = dict(data)
        legacy.pop("pending_memory_items", None)
        self._validate_v2(legacy)
        pending = data.get("pending_memory_items")
        if not isinstance(pending, list):
            raise ReadError(self.path)
        seen = {item["id"] for item in data.get("periodic_memory_items", [])}
        for item in pending:
            if not isinstance(item, dict):
                raise ReadError(self.path)
            mem_id = item.get("id")
            if (
                not isinstance(mem_id, str)
                or not re.fullmatch(r"MEM-[0-9A-F]{8}", mem_id)
                or mem_id in seen
                or item.get("source") != "user_manual"
                or item.get("status") not in {"awaiting_completion", "mount_blocked"}
                or not isinstance(item.get("requested_at"), str)
                or not item["requested_at"].strip()
                or not isinstance(item.get("reason", ""), str)
            ):
                raise ReadError(self.path)
            seen.add(mem_id)

    def _atomic_json(self, data):
        atomic_write_json(self.path, data)
