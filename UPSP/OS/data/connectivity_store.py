"""Read/write API connectivity health records."""

import json
import os
from datetime import datetime

from data.atomic_write import atomic_write_json
from data.config_store import ConfigStore
from paths import CONNECTIVITY_JSON
from errors import ReadError
from constants import local_now


class ConnectivityStore:
    """Manage connectivity.json for executor writes and heartbeat reads."""

    OK_STATUSES = {"ok", "success", "healthy"}
    DEGRADED_STATUSES = {"error", "timeout"}
    API_ENDPOINT_TIERS = {"primary", "fallback", "emergency"}  # 旧格式兼容

    def __init__(self, path=None, active_endpoint_ids=None,
                 recovery_endpoint_ids=None, config_store=None):
        self.path = path or CONNECTIVITY_JSON
        self._active_endpoint_ids = active_endpoint_ids
        self._recovery_endpoint_ids = recovery_endpoint_ids
        self._config_store = config_store or ConfigStore(use_api_environment=False)

    def active_endpoint_ids(self):
        if callable(self._active_endpoint_ids):
            values = self._active_endpoint_ids()
            return {str(value) for value in (values or []) if str(value)}
        return set(self.API_ENDPOINT_TIERS)

    def recovery_endpoint_ids(self):
        if callable(self._recovery_endpoint_ids):
            values = self._recovery_endpoint_ids()
            return {str(value) for value in (values or []) if str(value)}
        return set()

    def load(self):
        if not os.path.isfile(self.path):
            return {"_comment": "API connectivity log", "recent_latencies": []}
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(self.path, cause=e)

    def save(self, data):
        atomic_write_json(self.path, data)

    def _normalize_status(self, status):
        text = str(status or "").strip().lower()
        if text in self.OK_STATUSES:
            return "ok"
        if text in self.DEGRADED_STATUSES:
            return text
        return text or "unknown"

    def log_latency(self, endpoint, status, message="", *, source=""):
        data = self.load()
        entry = {
            "endpoint": endpoint or "unknown",
            "status": self._normalize_status(status),
            "message": str(message or "")[:200],
            "timestamp": local_now().isoformat(),
        }
        if source:
            entry["source"] = str(source)[:80]
        data.setdefault("recent_latencies", []).append(entry)
        limit = self._config_store.load("system")["connectivity"]["max_latency_records"]
        data["recent_latencies"] = data["recent_latencies"][-limit:]
        self.save(data)

    def latest_status_by_endpoint(self, data=None):
        data = data if data is not None else self.load()
        latest = {}
        for entry in data.get("recent_latencies", []):
            if not isinstance(entry, dict):
                continue
            endpoint = entry.get("endpoint") or "unknown"
            latest[endpoint] = self._normalize_status(entry.get("status"))
        return latest

    def has_degraded(self):
        data = self.load()
        recovery = self.recovery_endpoint_ids()
        if recovery:
            latest = self.latest_status_by_endpoint(data)
            slots = data.get("endpoints", {})
            return all(
                (
                    isinstance(slots.get(endpoint), dict)
                    and slots[endpoint].get("circuit_breaker") == "open"
                )
                or latest.get(endpoint) in self.DEGRADED_STATUSES
                for endpoint in recovery
            )
        active = self.active_endpoint_ids()
        for endpoint, slot in data.get("endpoints", {}).items():
            if endpoint not in active:
                continue
            if isinstance(slot, dict) and slot.get("circuit_breaker") == "open":
                return True
        for endpoint, status in self.latest_status_by_endpoint(data).items():
            if endpoint not in active:
                continue
            if status in self.DEGRADED_STATUSES:
                return True
        return False

    def has_recovered(self):
        data = self.load()
        recovery = self.recovery_endpoint_ids()
        if recovery:
            statuses = self.recovery_statuses(data)
            return any(status == "ok" for status in statuses) and not self.has_degraded()
        statuses = self.active_statuses(data)
        return (
            bool(statuses)
            and all(status == "ok" for status in statuses)
            and not self.has_degraded()
        )

    def active_statuses(self, data=None):
        active = self.active_endpoint_ids()
        latest = self.latest_status_by_endpoint(data)
        return [status for endpoint, status in latest.items() if endpoint in active]

    def recovery_statuses(self, data=None):
        recovery = self.recovery_endpoint_ids()
        latest = self.latest_status_by_endpoint(data)
        return [
            latest[endpoint]
            for endpoint in recovery
            if endpoint in latest
        ]
