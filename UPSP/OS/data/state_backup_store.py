"""
State backup JSONL store — STM/buffer/state_backups.jsonl.

The file is a small rolling audit trail for cleanup-success state snapshots.
"""
import json
import os
from datetime import datetime

from constants import local_now
from data.atomic_write import atomic_write_jsonl
from errors import ReadError
from paths import STATE_BACKUPS_JSONL


class StateBackupStore:
    """Append-only state snapshot store with numeric round FIFO retention."""

    def __init__(self, path=STATE_BACKUPS_JSONL, retention_count=8):
        self.path = path
        self.retention_count = retention_count

    def read_backups(self):
        if not os.path.isfile(self.path):
            return []
        rows = []
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        row = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(row, dict):
                        rows.append(row)
        except OSError as e:
            raise ReadError(self.path, cause=e)
        return rows

    def append_backup(self, round_num, state, reason="cleanup"):
        rows = self.read_backups()
        rows.append({
            "round": self._round_num(round_num),
            "timestamp": local_now().isoformat(),
            "reason": str(reason or "cleanup"),
            "state": state,
        })
        rows = self._retain(rows)
        atomic_write_jsonl(self.path, rows)
        return rows[-1]

    def _retain(self, rows):
        if self.retention_count <= 0:
            return []
        sorted_rows = sorted(rows, key=lambda row: self._round_num(row.get("round")))
        return sorted_rows[-self.retention_count:]

    @staticmethod
    def _round_num(value):
        try:
            return int(value)
        except (TypeError, ValueError):
            return 0
