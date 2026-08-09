"""LTM daily degradation contract.

This module owns slow-metabolism decisions for LTM memory layers:
daily countdown decrement, due-candidate selection, LLM compression
context rendering, and physical tier moves. CleanupPipeline only calls
these operations at the calendar-day rhythm boundary.
"""
import json
import os
import re

import paths
from engines.cleanup_helpers import (
    append_ltm_index,
    move_ltm_keyword_tier,
    remove_ltm_body_block,
    remove_ltm_index_row,
    strip_memory_heading,
)
from logic.mem_id import make_meta_template


DEFAULT_DECAY_PERIOD_DAYS = 30


class LTMDegradationManager:
    """Physical LTM degradation manager."""

    def prepare_daily_degradation(self, round_num):
        """Run daily countdown settlement and direct Abstract -> Backup moves."""
        changed = self.decrement_daily_countdowns()
        moved = self.apply_due_abstract_backups(round_num)
        return changed or moved

    def decrement_daily_countdowns(self):
        """Decrement positive countdowns in Full/Summary/Abstract by one day."""
        changed = False
        for tier in ("Full", "Summary", "Abstract"):
            meta_path = self._tier_paths(tier)["meta"]
            meta = self._load_json(meta_path)
            tier_changed = False
            for _mem_id, info in list(meta.items()):
                if not isinstance(info, dict) or info.get("type") == "P":
                    continue
                period = self._positive_int(
                    info.get("decay_period_days"),
                    DEFAULT_DECAY_PERIOD_DAYS,
                )
                if info.get("decay_period_days") != period:
                    info["decay_period_days"] = period
                    tier_changed = True
                countdown = self._int_or_default(
                    info.get("decay_countdown_days"),
                    period,
                )
                next_countdown = countdown - 1 if countdown > 0 else countdown
                if info.get("decay_countdown_days") != next_countdown:
                    info["decay_countdown_days"] = next_countdown
                    tier_changed = True
            if tier_changed:
                self._save_json(meta_path, meta)
                changed = True
        return changed

    def build_compression_context(self):
        """Render due Full/Summary entries for cleanup LLM compression."""
        tasks = []
        for tier in ("Full", "Summary"):
            for mem_id, info, content in self._due_entries_with_content(tier):
                to_tier = "Summary" if tier == "Full" else "Abstract"
                target_len = 512 if to_tier == "Summary" else 128
                tasks.append({
                    "mem_id": mem_id,
                    "from_tier": tier,
                    "to_tier": to_tier,
                    "target_len": target_len,
                    "content": content,
                    "title": info.get("title", mem_id),
                })

        if not tasks:
            return ""

        parts = ["## LTM 降格压缩（decay_countdown 到期条目）\n"]
        for task in tasks:
            parts.append(
                f"### {task['mem_id']} ({task['from_tier']}->{task['to_tier']}，"
                f"约{task['target_len']}字)\n"
                f"原标题：{task['title']}\n原文：\n{task['content']}\n"
                f"请压缩后用 `<!-- LTM_DEGRADE:{task['mem_id']} -->` 和 "
                "`<!-- /LTM_DEGRADE -->` 包裹输出。"
            )
        return "\n\n".join(parts)

    def apply_compression_results(self, degradation_results, round_num):
        """Apply LLM compression results for due Full/Summary entries."""
        changed = False
        for mem_id, compressed_text in degradation_results:
            text = str(compressed_text or "").strip()
            if not text:
                continue
            source_tier, source_info = self._find_source(mem_id, tiers=("Full", "Summary"))
            if not source_tier:
                continue
            countdown = self._int_or_default(
                source_info.get("decay_countdown_days"),
                self._positive_int(source_info.get("decay_period_days"), DEFAULT_DECAY_PERIOD_DAYS),
            )
            if countdown > 0:
                continue
            to_tier = "Summary" if source_tier == "Full" else "Abstract"
            self._move_entry(
                mem_id,
                source_tier,
                to_tier,
                text,
                source_info,
                round_num,
            )
            changed = True
        return changed

    def apply_due_abstract_backups(self, round_num):
        """Move due Abstract entries to Backup without LLM compression."""
        changed = False
        for mem_id, info, content in self._due_entries_with_content("Abstract"):
            self._move_entry(mem_id, "Abstract", "Backup", content, info, round_num)
            changed = True
        return changed

    def _due_entries_with_content(self, tier):
        meta_path = self._tier_paths(tier)["meta"]
        body_path = self._tier_paths(tier)["body"]
        meta = self._load_json(meta_path)
        if not meta or not os.path.isfile(body_path):
            return []
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                body_text = f.read()
        except OSError:
            return []

        results = []
        for mem_id, info in meta.items():
            if not isinstance(info, dict) or info.get("type") == "P":
                continue
            countdown = self._int_or_default(
                info.get("decay_countdown_days"),
                self._positive_int(info.get("decay_period_days"), DEFAULT_DECAY_PERIOD_DAYS),
            )
            if countdown > 0:
                continue
            content = self._extract_body_block(body_text, mem_id)
            if content:
                results.append((mem_id, info, content))
        return results

    def _move_entry(self, mem_id, source_tier, dest_tier, content, source_info, round_num):
        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        source_weight = self._int_or_default(source_info.get("weight"), 1)
        dest_type = "S" if dest_tier == "Summary" else "A"
        dest_paths = self._tier_paths(dest_tier)
        source_paths = self._tier_paths(source_tier)

        os.makedirs(dest_paths["dir"], exist_ok=True)
        body = strip_memory_heading(content)
        with open(dest_paths["body"], "a", encoding="utf-8") as f:
            f.write(f"\n## MEM-{clean_id}  [{dest_type}]  权重{source_weight}\n{body}\n")
        append_ltm_index(
            dest_paths["index"],
            mem_id,
            dest_type,
            source_weight,
            source_info.get("title", mem_id),
            source_info.get("subject", ""),
            round_num,
        )
        self._write_dest_meta(mem_id, dest_tier, dest_type, source_weight, source_info, round_num)

        self._remove_source(mem_id, source_paths)
        move_ltm_keyword_tier(mem_id, source_tier, dest_tier)

    def _write_dest_meta(self, mem_id, dest_tier, dest_type, weight, source_info, round_num):
        dest_meta = self._tier_paths(dest_tier)["meta"]
        data = self._load_json(dest_meta)
        entry = make_meta_template(
            mem_id,
            title=source_info.get("title", mem_id),
            weight=weight,
            subject=source_info.get("subject", ""),
            model=source_info.get("model", ""),
        )
        entry["type"] = dest_type
        for key in (
            "tags", "access", "linked_containers", "current_overview",
            "current_overview_updated_at",
            "dream", "media",
        ):
            if key in source_info:
                entry[key] = source_info[key]
        if source_info.get("created_at"):
            entry["created_at"] = source_info["created_at"]
        if source_info.get("last_recalled_at"):
            entry["last_recalled_at"] = source_info["last_recalled_at"]
        entry["created_round"] = source_info.get("created_round", round_num)
        entry["last_recalled_round"] = round_num

        if dest_tier == "Backup":
            entry["decay_period_days"] = 0
            entry["decay_countdown_days"] = 0
        else:
            period = self._positive_int(
                source_info.get("decay_period_days"),
                DEFAULT_DECAY_PERIOD_DAYS,
            )
            entry["decay_period_days"] = period
            entry["decay_countdown_days"] = period

        data[mem_id] = entry
        self._save_json(dest_meta, data)

    def _remove_source(self, mem_id, source_paths):
        meta = self._load_json(source_paths["meta"])
        if mem_id in meta:
            del meta[mem_id]
            self._save_json(source_paths["meta"], meta)
        remove_ltm_body_block(source_paths["body"], mem_id)
        remove_ltm_index_row(source_paths["index"], mem_id)

    def _find_source(self, mem_id, tiers):
        for tier in tiers:
            meta = self._load_json(self._tier_paths(tier)["meta"])
            if mem_id in meta and isinstance(meta[mem_id], dict):
                return tier, meta[mem_id]
        return None, {}

    def _extract_body_block(self, body_text, mem_id):
        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        pattern = re.compile(
            rf"(?ms)^##\s+MEM-{re.escape(clean_id)}\b.*?(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
        )
        match = pattern.search(body_text or "")
        return match.group(0).strip() if match else ""

    def _tier_paths(self, tier):
        configs = {
            "Full": {
                "dir": "LTM_FULL_DIR",
                "body": "LTM_FULL_FULL_MD",
                "meta": "LTM_FULL_META_JSON",
                "index": "LTM_FULL_INDEX_MD",
            },
            "Summary": {
                "dir": "LTM_SUMMARY_DIR",
                "body": "LTM_SUMMARY_SUMMARY_MD",
                "meta": "LTM_SUMMARY_META_JSON",
                "index": "LTM_SUMMARY_INDEX_MD",
            },
            "Abstract": {
                "dir": "LTM_ABSTRACT_DIR",
                "body": "LTM_ABSTRACT_ABSTRACT_MD",
                "meta": "LTM_ABSTRACT_META_JSON",
                "index": "LTM_ABSTRACT_INDEX_MD",
            },
            "Backup": {
                "dir": "LTM_BACKUP_DIR",
                "body": "LTM_BACKUP_BACKUP_MD",
                "meta": "LTM_BACKUP_META_JSON",
                "index": "LTM_BACKUP_INDEX_MD",
            },
        }
        cfg = configs[tier]
        return {name: getattr(paths, attr) for name, attr in cfg.items()}

    def _load_json(self, path):
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def _save_json(self, path, data):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        os.replace(tmp, path)

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
