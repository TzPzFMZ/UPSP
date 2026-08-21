"""Deterministic storage operations for LTM daily degradation."""
import json
import os
import re

import paths
from data.atomic_write import atomic_write_json, atomic_write_text
from data.memory_store import MemoryStore


DEFAULT_DECAY_PERIOD_DAYS = 30


class LTMDegradationFailure(RuntimeError):
    def __init__(self, phase, from_tier, to_tier, mem_id, cause):
        self.phase = phase
        self.from_tier = from_tier
        self.to_tier = to_tier
        self.mem_id = mem_id
        self.cause = cause
        super().__init__(
            f"{phase}:{from_tier}->{to_tier}:{mem_id or 'tier'}:"
            f"{type(cause).__name__}:{cause}"
        )


class LTMDegradationManager:
    """Physical LTM countdown and tier-move manager."""

    def __init__(self, memory_store=None):
        self.memory_store = memory_store or MemoryStore()

    def decrement_daily_countdowns(self):
        """Decrement positive countdowns in Full/Summary/Abstract by one day."""
        updates = []
        for tier in ("Full", "Summary", "Abstract"):
            meta_path = self._tier_paths(tier)["meta"]
            try:
                original = None
                if os.path.isfile(meta_path):
                    with open(meta_path, "r", encoding="utf-8") as handle:
                        original = handle.read()
                meta = self._load_json(meta_path)
            except Exception as exc:
                raise LTMDegradationFailure(
                    "countdown_read", tier, tier, "", exc) from exc
            tier_changed = False
            for info in meta.values():
                if not isinstance(info, dict) or info.get("type") == "P":
                    continue
                if not str(info.get("stored_at") or "").strip():
                    continue
                period = self._positive_int(
                    info.get("decay_period_days"), DEFAULT_DECAY_PERIOD_DAYS)
                if info.get("decay_period_days") != period:
                    info["decay_period_days"] = period
                    tier_changed = True
                countdown = self._int_or_default(
                    info.get("decay_countdown_days"), period)
                next_countdown = countdown - 1 if countdown > 0 else countdown
                if info.get("decay_countdown_days") != next_countdown:
                    info["decay_countdown_days"] = next_countdown
                    tier_changed = True
            if tier_changed:
                updates.append((tier, meta_path, original, meta))
        attempted = []
        try:
            for tier, meta_path, original, meta in updates:
                attempted.append((tier, meta_path, original))
                self._save_json(meta_path, meta)
        except Exception as exc:
            try:
                for _tier, path, original in attempted:
                    if original is None:
                        if os.path.isfile(path):
                            os.remove(path)
                    else:
                        atomic_write_text(path, original)
            except Exception as restore_exc:
                raise LTMDegradationFailure(
                    "countdown_restore", tier, tier, "", restore_exc
                ) from exc
            raise LTMDegradationFailure(
                "countdown_write", tier, tier, "", exc) from exc
        return bool(updates)

    def apply_due_abstract_backups(self, round_num):
        """Move due Abstract entries to Backup without LLM compression."""
        changed = False
        try:
            entries = self._due_entries_with_content("Abstract")
        except Exception as exc:
            raise LTMDegradationFailure(
                "backup_scan", "Abstract", "Backup", "", exc) from exc
        for mem_id, info, content in entries:
            try:
                self._move_entry(
                    mem_id, "Abstract", "Backup", content, info, round_num)
                changed = True
            except Exception as exc:
                raise LTMDegradationFailure(
                    "backup_store", "Abstract", "Backup", mem_id, exc
                ) from exc
        return changed

    def _due_entries_with_content(self, tier):
        meta_path = self._tier_paths(tier)["meta"]
        body_path = self._tier_paths(tier)["body"]
        meta = self._load_json(meta_path)
        if not meta or not os.path.isfile(body_path):
            return []
        with open(body_path, "r", encoding="utf-8") as handle:
            body_text = handle.read()
        results = []
        for mem_id, info in meta.items():
            if not isinstance(info, dict) or info.get("type") == "P":
                continue
            if not str(info.get("stored_at") or "").strip():
                continue
            countdown = self._int_or_default(
                info.get("decay_countdown_days"),
                self._positive_int(
                    info.get("decay_period_days"), DEFAULT_DECAY_PERIOD_DAYS),
            )
            if countdown > 0:
                continue
            content = self._extract_body_block(body_text, mem_id)
            if content:
                results.append((mem_id, info, content))
        return results

    def due_entries(self, tier):
        """Return verified due entries for the rhythm compression ledger."""
        if tier not in {"Full", "Summary", "Abstract"}:
            raise ValueError("ltm_degradation_tier_invalid")
        return self._due_entries_with_content(tier)

    def _move_entry(self, mem_id, source_tier, dest_tier, content, source_info,
                    round_num):
        entry = dict(source_info)
        entry["type"] = "S" if dest_tier == "Summary" else "A"
        if dest_tier == "Backup":
            entry["decay_period_days"] = 0
            entry["decay_countdown_days"] = 0
        else:
            period = self._positive_int(
                source_info.get("decay_period_days"), DEFAULT_DECAY_PERIOD_DAYS)
            entry["decay_period_days"] = period
            entry["decay_countdown_days"] = period
        self.memory_store.store_ltm_entry(
            dest_tier, mem_id, content, entry, source_tier=source_tier)

    @staticmethod
    def _extract_body_block(body_text, mem_id):
        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        pattern = re.compile(
            rf"(?ms)^##\s+MEM-{re.escape(clean_id)}\b.*?"
            rf"(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
        )
        match = pattern.search(body_text or "")
        return match.group(0).strip() if match else ""

    @staticmethod
    def _tier_paths(tier):
        configs = {
            "Full": {"dir": "LTM_FULL_DIR", "body": "LTM_FULL_FULL_MD",
                     "meta": "LTM_FULL_META_JSON", "index": "LTM_FULL_INDEX_MD"},
            "Summary": {"dir": "LTM_SUMMARY_DIR", "body": "LTM_SUMMARY_SUMMARY_MD",
                        "meta": "LTM_SUMMARY_META_JSON", "index": "LTM_SUMMARY_INDEX_MD"},
            "Abstract": {"dir": "LTM_ABSTRACT_DIR", "body": "LTM_ABSTRACT_ABSTRACT_MD",
                         "meta": "LTM_ABSTRACT_META_JSON", "index": "LTM_ABSTRACT_INDEX_MD"},
            "Backup": {"dir": "LTM_BACKUP_DIR", "body": "LTM_BACKUP_BACKUP_MD",
                       "meta": "LTM_BACKUP_META_JSON", "index": "LTM_BACKUP_INDEX_MD"},
        }
        return {name: getattr(paths, attr) for name, attr in configs[tier].items()}

    @staticmethod
    def _load_json(path):
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ValueError(f"ltm_meta_invalid:{path}") from exc
        if not isinstance(data, dict):
            raise ValueError(f"ltm_meta_invalid:{path}")
        return data

    @staticmethod
    def _save_json(path, data):
        atomic_write_json(path, data)

    @staticmethod
    def _int_or_default(value, default=0):
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _positive_int(value, default=1):
        try:
            number = int(value)
        except (TypeError, ValueError):
            number = int(default)
        return number if number > 0 else int(default)
