"""PID-shared ownership ledger for periodic-memory-created Pinned entries."""

import json
import os
import re

from constants import local_now
from data.atomic_write import atomic_write_json
from errors import ReadError
from paths import PERIODIC_PIN_OWNERS_JSON


PERIODIC_PIN_OWNERS_SCHEMA = "periodic_pin_owners.v2"


class PeriodicPinOwnerStore:
    def __init__(self, path=None, now_fn=None):
        self.path = path or PERIODIC_PIN_OWNERS_JSON
        self.now_fn = now_fn or (lambda: local_now().isoformat())

    def load(self):
        if not os.path.isfile(self.path):
            return self._default()
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ReadError(self.path, cause=exc) from exc
        if data.get("schema_version") == "periodic_pin_owners.v1":
            entries = data.get("entries")
            updated_at = data.get("updated_at")
            if (
                not isinstance(entries, dict)
                or not isinstance(updated_at, str)
                or (entries and not updated_at.strip())
            ):
                raise ReadError(self.path)
            migrated = dict(data)
            migrated["entries"] = {
                mem_id: {
                    **dict(entry),
                    "alignment_verified": entry.get("pin_source") == "preexisting",
                } if isinstance(entry, dict) else entry
                for mem_id, entry in entries.items()
            }
            migrated["schema_version"] = PERIODIC_PIN_OWNERS_SCHEMA
            return self.save_document(migrated)
        self._validate(data)
        return data

    def save_document(self, data):
        document = dict(data or {})
        document["schema_version"] = PERIODIC_PIN_OWNERS_SCHEMA
        document["updated_at"] = self.now_fn()
        self._validate(document)
        atomic_write_json(self.path, document)
        return document

    @staticmethod
    def _default():
        return {
            "schema_version": PERIODIC_PIN_OWNERS_SCHEMA,
            "updated_at": "",
            "entries": {},
        }

    def _validate(self, data):
        updated_at = data.get("updated_at", "") if isinstance(data, dict) else ""
        entries = data.get("entries") if isinstance(data, dict) else None
        if (
            not isinstance(data, dict)
            or data.get("schema_version") != PERIODIC_PIN_OWNERS_SCHEMA
            or not isinstance(updated_at, str)
            or not isinstance(entries, dict)
            or (entries and not updated_at.strip())
        ):
            raise ReadError(self.path)
        for mem_id, entry in entries.items():
            if not isinstance(entry, dict):
                raise ReadError(self.path)
            owners = entry.get("owners")
            created_at = entry.get("created_at")
            if (
                not re.fullmatch(r"MEM-[0-9A-F]{8}", str(mem_id))
                or entry.get("pin_source") not in {"periodic", "preexisting"}
                or not isinstance(owners, list)
                or not owners
                or any(
                    not isinstance(owner, str) or not owner.strip()
                    for owner in owners
                )
                or len(owners) != len(set(owners))
                or not isinstance(created_at, str)
                or not created_at.strip()
                or not isinstance(entry.get("alignment_verified"), bool)
            ):
                raise ReadError(self.path)
