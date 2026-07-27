"""
Periodic layer mount projection store.

This store owns the IO for STM/context/periodic_mounts.json. It does not decide
what should enter the periodic layer, and it does not trim content budgets; consumers such
as ContextAssembler keep those policy decisions.
"""
import json
import os
from datetime import datetime

from data.atomic_write import atomic_write_json
from constants import local_now
from paths import STM_CONTEXT_DIR


DEFAULT_PERIODIC_MOUNTS_PATH = os.path.join(STM_CONTEXT_DIR, "periodic_mounts.json")


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
        except (OSError, json.JSONDecodeError):
            return self._default()
        if not isinstance(data, dict):
            return self._default()
        return data

    def save(self, periodic_memory_items=None):
        data = {
            "updated_at": self.now_fn(),
            "periodic_memory_items": list(periodic_memory_items or []),
        }
        self._atomic_json(data)
        return data

    def save_ids(self, memory_ids=None):
        return self.save(
            periodic_memory_items=[
                {"id": str(item), "rendered_text": str(item)}
                for item in (memory_ids or [])
            ],
        )

    @staticmethod
    def _default():
        return {"periodic_memory_items": []}

    def _atomic_json(self, data):
        atomic_write_json(self.path, data)
