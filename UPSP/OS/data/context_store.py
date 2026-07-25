"""
上下文缓存读写 — now_cache/lately_cache + raw_log.jsonl
DDS §19 上下文工程

now_cache.jsonl: 当前缓存语料块主源
lately_cache.jsonl: 最近缓存语料块主源
raw_log.jsonl: 原始语料备份主源
"""
import json
import os
import hashlib
import re
from datetime import datetime

from constants import TZ_SHANGHAI
from data.atomic_write import atomic_write_text
from data.config_store import ConfigStore
from errors import WriteError
from paths import (
    RAW_LOG,
    RAW_LOG_JSONL,
    STM_CONTEXT_CACHE_DIR,
    STM_CONTEXT_NOW_CACHE_JSONL,
    STM_CONTEXT_LATELY_CACHE_JSONL,
)
from schemas.context import context_safe_read_tool_result

_DEFAULT_RAW_LOG = RAW_LOG
_DEFAULT_RAW_LOG_JSONL = RAW_LOG_JSONL
_DEFAULT_CONTEXT_CACHE_DIR = STM_CONTEXT_CACHE_DIR
_DEFAULT_NOW_CACHE_JSONL = STM_CONTEXT_NOW_CACHE_JSONL
_DEFAULT_LATELY_CACHE_JSONL = STM_CONTEXT_LATELY_CACHE_JSONL


class ContextStore:
    """now/lately 语料缓存与 raw_log 备份管理。"""

    CACHE_COMPACTION_DEBT_SCHEMA = "cache_compaction_debt.v1"

    TEMPLATE_PLACEHOLDER_TOKENS = {
        "assistant_reply": {"assistant_reply"},
    }
    MODEL_CONTEXT_KINDS = {
        "interaction",
        "assistant_reply",
        "dialogue_progress",
        "material",
        "reasoning_context",
        "tool_fact",
        "setup_fact",
        "relay_handoff",
        "minimum_commitment",
        "fault_note",
        "cache_summary",
    }
    RETIRED_CONTEXT_KINDS = {
        "tool_result",
        "tool_summary",
        "protocol_tool_receipt",
        "native_tool_result",
        "training_evidence",
        "final_reply_handoff",
    }
    TRANSIENT_DISPLAY_KINDS = {
        "popup",
        "popup_event",
        "step_guide",
        "tool_guide",
        "protocol_tool_guide",
        "general_tool_guide",
        "native_tool_feedback",
        "runtime_warning",
        "correction_warning",
        "internal_correction",
        "training_material_evidence",
        "cleanup_training_evidence",
        "cleanup_material_evidence",
        "training_evidence",
        "final_reply_handoff",
    }
    TRANSIENT_DISPLAY_KIND_PREFIXES = (
        "popup_",
        "guide_",
        "warning_",
        "runtime_warning_",
        "internal_correction_",
    )

    def __init__(self, state_store=None, config_store=None,
                 cache_dir=None, now_cache_jsonl=None, lately_cache_jsonl=None,
                 raw_log_jsonl=None, raw_log_md=None):
        self.state_store = state_store
        self.config_store = config_store or ConfigStore()
        self._cache_dir_override = cache_dir
        self._now_cache_jsonl_override = now_cache_jsonl
        self._lately_cache_jsonl_override = lately_cache_jsonl
        self._raw_log_jsonl_override = raw_log_jsonl
        self._raw_log_md_override = raw_log_md
        self._last_cache_stats = self._empty_cache_stats()

    # ==========================================================
    # now/lately cache
    # ==========================================================

    def save_round_to_cache(self, round_num, user_input, response,
                            interaction_object="unknown",
                            identity_status="unknown",
                            interaction_source="unresolved",
                            interaction_object_id=None):
        """保存一轮交互/回复语料到 now/lately 主源。"""
        now = datetime.now(TZ_SHANGHAI).isoformat()
        common = {
            "round": round_num,
            "timestamp": now,
            "interaction_object": interaction_object,
            "identity_status": identity_status,
            "interaction_source": interaction_source,
            "step": "round",
            "iter": 0,
        }
        if interaction_object_id:
            common["interaction_object_id"] = str(interaction_object_id)
        existing_lately = self._load_lately_entries()
        existing_now = self.get_now_entries()
        existing_context = existing_now + existing_lately
        round_entries = []
        if user_input and not self._has_same_round_corpus_entry(
                existing_context, round_num, "user", "interaction", user_input):
            round_entries.append(self._normalize_entry({
                "role": "user",
                "kind": "interaction",
                "content": user_input,
                **common,
            }))
        if response and not self._has_same_round_corpus_entry(
                existing_context, round_num, "assistant", "assistant_reply", response):
            round_entries.append(self._normalize_entry({
                "role": "assistant",
                "kind": "assistant_reply",
                "content": response,
                **common,
            }))
        if not round_entries:
            self._last_cache_stats = self._empty_cache_stats()
            return

        now_entries = self._merge_now_entries(
            existing_now, round_entries)
        self._sync_caches(existing_lately, now_entries)

    def append_to_cache(self, round_num, role, content,
                        interaction_object="unknown",
                        identity_status="unknown",
                        interaction_source="unresolved",
                        interaction_object_id=None,
                        kind=None, step="round", iter=0,
                        tool_result=None, protocol_receipt=None,
                        protocol_receipts=None,
                        message_channel=None,
                        message_envelope=None,
                        native_replay=None):
        """追加 A/B 持久语料到 now/lately 主源。

        Spec625 后普通语料只能按 kind 进入 A（raw 镜像）或 B（无
        raw 镜像）履带；单次调用材料必须走 ``append_call_transient``。
        """
        if not content:
            return
        kind_value = kind or ("assistant_reply" if role == "assistant" else "interaction")
        if not self._is_model_context_kind(kind_value):
            return
        if not self._persistent_lane_for_kind(kind_value):
            raise ValueError(f"普通语料必须声明持久轨道: {kind_value}")
        now = datetime.now(TZ_SHANGHAI).isoformat()
        entry_data = {
            "round": round_num,
            "role": role,
            "kind": kind_value,
            "content": content,
            "timestamp": now,
            "interaction_object": interaction_object,
            "identity_status": identity_status,
            "interaction_source": interaction_source,
            "step": step,
            "iter": iter,
        }
        if interaction_object_id:
            entry_data["interaction_object_id"] = str(interaction_object_id)
        entry = self._normalize_entry(entry_data)
        if isinstance(tool_result, dict):
            entry["tool_result"] = dict(tool_result)
        if isinstance(protocol_receipt, dict):
            entry["protocol_receipt"] = dict(protocol_receipt)
        if isinstance(protocol_receipts, list):
            entry["protocol_receipts"] = [
                dict(item) for item in protocol_receipts if isinstance(item, dict)
            ]
        if message_channel:
            entry["message_channel"] = str(message_channel).strip()
        if isinstance(message_envelope, dict):
            entry["message_envelope"] = dict(message_envelope)
        if isinstance(native_replay, dict):
            entry["native_replay"] = dict(native_replay)
        entries = self._load_lately_entries()
        now_entries = self._merge_now_entries(
            self.get_now_entries(), [entry])
        self._sync_caches(entries, now_entries)

    def append_reasoning_context(self, round_num, reasoning_content,
                                 native_replay=None, step="reaction", iter=0,
                                 **cache_meta):
        """写入只供下一次 reaction 调用消费的推理续接临时语料。"""
        text = str(reasoning_content or "").strip()
        if not text:
            return
        if not text.startswith("【本轮推理上下文】"):
            text = "【本轮推理上下文】\n\n" + text
        return self.append_call_transient(
            round_num,
            "assistant",
            text,
            kind="reasoning_context",
            step=step,
            iter=iter,
            native_replay=native_replay,
            transient_scope="reasoning_replay",
            transient_target_step="reaction",
            transient_target_iteration=max(int(iter or 0) + 2, 1),
            **cache_meta,
        )

    def append_cleanup_round_material(self, round_num, content, **cache_meta):
        """写入只供本轮善后调用读取的 C 轨临时材料块。"""
        return self.append_call_transient(
            round_num,
            "system",
            content,
            kind="material",
            step="cleanup",
            transient_scope="cleanup_round",
            transient_target_step="cleanup",
            **cache_meta,
        )

    def append_call_transient(self, round_num, role, content,
                              interaction_object="unknown",
                              identity_status="unknown",
                              interaction_source="unresolved",
                              interaction_object_id=None,
                              kind="material", step="reaction", iter=0,
                              transient_scope=None,
                              transient_target_step=None,
                              transient_target_iteration=None,
                              native_replay=None):
        """追加 C 轨单次调用语料；它不参加 now/lately/raw 履带。"""
        if not content:
            return
        scope = str(transient_scope or "").strip()
        target_step = str(transient_target_step or "").strip()
        if not scope or not target_step:
            raise ValueError("单次调用语料必须声明 scope 与目标步骤")
        kind_value = str(kind or "material").strip()
        if not self._is_model_context_kind(kind_value):
            return
        entry_data = {
            "round": round_num,
            "role": role,
            "kind": kind_value,
            "content": content,
            "timestamp": datetime.now(TZ_SHANGHAI).isoformat(),
            "interaction_object": interaction_object,
            "identity_status": identity_status,
            "interaction_source": interaction_source,
            "step": step,
            "iter": iter,
            "call_transient": True,
            "transient_scope": scope,
            "transient_target_step": target_step,
        }
        if interaction_object_id:
            entry_data["interaction_object_id"] = str(interaction_object_id)
        entry = self._normalize_entry(entry_data)
        if transient_target_iteration is not None:
            entry["transient_target_iteration"] = int(transient_target_iteration)
        if isinstance(native_replay, dict):
            entry["native_replay"] = dict(native_replay)
        all_now = self._all_now_entries()
        self._write_now_cache(self._merge_now_entries(all_now, [entry]))
        self._last_cache_stats = self._empty_cache_stats()

    def clear_transient_entries(self, round_num=None, transient_scope=None,
                                transient_target_step=None,
                                transient_target_iteration=None):
        """清除 C 轨单次调用语料；绝不改 lately/raw。"""
        now_entries = self._all_now_entries()

        def matches(entry):
            if not isinstance(entry, dict):
                return False
            if round_num is not None and self._entry_round(entry) != int(round_num):
                return False
            if not self._is_call_transient(entry):
                return False
            if transient_scope and str(entry.get("transient_scope") or "") != str(transient_scope):
                return False
            if (transient_target_step
                    and str(entry.get("transient_target_step") or "") != str(transient_target_step)):
                return False
            if transient_target_iteration is not None:
                try:
                    return int(entry.get("transient_target_iteration")) == int(transient_target_iteration)
                except (TypeError, ValueError):
                    return False
            return True

        kept_now = [entry for entry in now_entries if not matches(entry)]
        self._write_now_cache(kept_now)
        self._last_cache_stats = self._empty_cache_stats()
        return {
            "status": "applied",
            "round": round_num,
            "transient_scope": transient_scope or "",
            "transient_target_step": transient_target_step or "",
            "transient_target_iteration": transient_target_iteration,
            "now_removed": len(now_entries) - len(kept_now),
            "lately_removed": 0,
        }

    def clear_stale_call_transients(self, current_round):
        """新轮开始前丢弃未消费的旧轮 C 轨，防止跨轮污染。"""
        try:
            active_round = int(current_round)
        except (TypeError, ValueError):
            return {"status": "rejected", "reason": "invalid_round"}
        now_entries = self._all_now_entries()
        kept = [
            entry for entry in now_entries
            if not self._is_call_transient(entry)
            or self._entry_round(entry) == active_round
        ]
        self._write_now_cache(kept)
        self._last_cache_stats = self._empty_cache_stats()
        return {
            "status": "applied",
            "current_round": active_round,
            "now_removed": len(now_entries) - len(kept),
        }

    def get_lately_entries(self, step="setup"):
        """读取当前字符窗口内的完整 lately 语料块。"""
        return [
            entry for entry in self._load_lately_entries()
            if self._is_model_context_kind(entry.get("kind"))
        ]

    def get_now_entries(self):
        """读取当前缓存主源。"""
        return [entry for entry in self._all_now_entries()
                if not self._is_call_transient(entry)]

    def _all_now_entries(self):
        """读取 now 主源，含尚未消费的 C 轨临时语料。"""
        return [entry for entry in (
            self._corpus_block_to_entry(block)
            for block in self._read_jsonl(self._now_cache_jsonl())
        ) if entry and self._is_model_context_kind(entry.get("kind"))]

    def get_call_transient_entries(self, round_num, step,
                                   reaction_iteration=None):
        """返回恰好应由本次调用消费的 C 轨语料。"""
        try:
            current_round = int(round_num)
        except (TypeError, ValueError):
            return []
        target_step = str(step or "").strip()
        entries = []
        for entry in self._all_now_entries():
            if not self._is_call_transient(entry):
                continue
            if self._entry_round(entry) != current_round:
                continue
            if str(entry.get("transient_target_step") or "") != target_step:
                continue
            target_iteration = entry.get("transient_target_iteration")
            if target_iteration is not None:
                try:
                    if int(target_iteration) != int(reaction_iteration):
                        continue
                except (TypeError, ValueError):
                    continue
            entries.append(entry)
        return entries

    def get_current_input_text(self):
        """返回当前或最近一条交互输入文本。"""
        for entry in reversed(self.get_now_entries() + self._load_lately_entries()):
            if entry.get("role") == "user" and entry.get("content"):
                return entry["content"]
        return None

    def get_last_cache_stats(self):
        return dict(self._last_cache_stats)

    def get_now_budget_chars(self):
        """Return the configured now soft watermark for Runtime admission."""
        return int(self._now_cache_params()["budget_chars"])

    def get_round_material_chars(self, round_num, iteration=None):
        """统计当前轮跨 now/lately 可见的 B 轨资料字符数。"""
        try:
            current_round = int(round_num)
        except (TypeError, ValueError):
            return 0
        total = 0
        seen = set()
        for entry in self.get_now_entries() + self._load_lately_entries():
            if entry.get("kind") != "material" or self._is_call_transient(entry):
                continue
            if self._entry_round(entry) != current_round:
                continue
            marker = self._entry_key(entry)
            if marker in seen:
                continue
            seen.add(marker)
            total += self._entry_chars(entry)
        return total

    def cache_compaction_debt_path(self):
        return os.path.join(self._cache_dir(), "cache_compaction_debt.json")

    def load_cache_compaction_debt(self):
        path = self.cache_compaction_debt_path()
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                debt = json.load(f)
        except (OSError, json.JSONDecodeError):
            return {}
        if not isinstance(debt, dict):
            return {}
        if debt.get("schema_version") != self.CACHE_COMPACTION_DEBT_SCHEMA:
            return {}
        if str(debt.get("status") or "").strip() != "open":
            return {}
        return dict(debt)

    def save_cache_compaction_debt(self, receipt, round_num):
        receipt = receipt if isinstance(receipt, dict) else {}
        if receipt.get("status") != "due":
            return {}
        plan = self._sanitize_cache_compaction_debt_value(
            receipt.get("plan") or receipt.get("compaction_plan") or {}
        )
        stats = self._sanitize_cache_compaction_debt_value(
            receipt.get("cache_stats") or {}
        )
        candidate_ids = self._cache_compaction_candidate_ids(
            receipt.get("candidate_ids"),
            plan,
        )
        debt = {
            "schema_version": self.CACHE_COMPACTION_DEBT_SCHEMA,
            "status": "open",
            "created_round": self._sanitize_int(round_num, 0),
            "created_at": datetime.now(TZ_SHANGHAI).isoformat(),
            "cache_stats": stats if isinstance(stats, dict) else {},
            "compaction_plan": plan if isinstance(plan, dict) else {},
            "candidate_ids": candidate_ids,
            "completed_shards": [],
        }
        self._write_json_atomic(self.cache_compaction_debt_path(), debt)
        return dict(debt)

    def update_cache_compaction_debt(self, **fields):
        debt = self.load_cache_compaction_debt()
        if not debt:
            return {}
        sanitized = self._sanitize_cache_compaction_debt_value(fields)
        if isinstance(sanitized, dict):
            debt.update(sanitized)
        if "completed_shards" in fields:
            debt["completed_shards"] = self._unique_text_list(fields.get("completed_shards"))
        debt["updated_at"] = datetime.now(TZ_SHANGHAI).isoformat()
        self._write_json_atomic(self.cache_compaction_debt_path(), debt)
        return dict(debt)

    def clear_cache_compaction_debt(self):
        path = self.cache_compaction_debt_path()
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError:
            pass

    def _cache_compaction_candidate_ids(self, values, plan):
        candidate_ids = self._unique_text_list(values)
        if candidate_ids:
            return candidate_ids
        for shard in (plan or {}).get("shards") or []:
            if not isinstance(shard, dict):
                continue
            candidate_ids.extend(self._unique_text_list(shard.get("source_block_ids")))
        candidate_ids = self._unique_text_list(candidate_ids)
        if candidate_ids:
            return candidate_ids
        try:
            candidates = self.build_lately_compression_candidates(max_blocks=None)
        except TypeError:
            candidates = self.build_lately_compression_candidates()
        except Exception:
            candidates = []
        return self._unique_text_list(
            item.get("id")
            for item in candidates or []
            if isinstance(item, dict)
        )

    @classmethod
    def _sanitize_cache_compaction_debt_value(cls, value):
        if isinstance(value, dict):
            cleaned = {}
            for key, item in value.items():
                token = str(key or "").strip().lower()
                if cls._is_cache_compaction_debt_text_key(token):
                    continue
                cleaned[key] = cls._sanitize_cache_compaction_debt_value(item)
            return cleaned
        if isinstance(value, list):
            return [cls._sanitize_cache_compaction_debt_value(item) for item in value]
        if isinstance(value, (str, int, float, bool)) or value is None:
            return value
        return str(value)

    @staticmethod
    def _is_cache_compaction_debt_text_key(key):
        return (
            key in {
                "text",
                "content",
                "summary",
                "body",
                "raw",
                "raw_text",
                "replacement_text",
            }
            or key.endswith("_text")
            or key.endswith("_content")
        )

    @staticmethod
    def _unique_text_list(values):
        unique = []
        seen = set()
        for value in values or []:
            token = str(value or "").strip()
            if not token or token in seen:
                continue
            unique.append(token)
            seen.add(token)
        return unique

    def get_lately_compact_ratio(self):
        """读取 lately 删后幸存段压缩比例；默认 0.618。"""
        return self.get_lately_compaction_params()["compact_ratio"]

    def get_lately_compaction_params(self):
        """读取 lately 压缩节律参数。"""
        return self.config_store.get_lately_compaction_params()

    def _sync_caches(self, entries, now_entries):
        active_now, promoted, now_stats = self._apply_now_watermark(now_entries)
        stats = dict(now_stats)

        merged_lately = self._merge_lately_entries(entries, promoted)
        admitted_blocks = self._entries_to_lately_blocks(merged_lately)
        active_lately, lately_stats = self._apply_lately_watermark(merged_lately)
        for key in (
                "lately_deleted_blocks",
                "lately_deleted_chars",
                "lately_trimmed",
                "cache_compaction_required",
                "lately_surviving_chars",
                "lately_compact_target_chars",
                "lately_compact_ratio",
                "lately_compact_shard_chars",
                "lately_compact_shard_ratio",
        ):
            stats[key] = lately_stats[key]

        call_transients = [
            entry for entry in self._all_now_entries()
            if self._is_call_transient(entry)
        ]
        self._write_now_cache(self._merge_now_entries(active_now, call_transients))
        self._write_lately_cache(active_lately)
        self._mirror_lately_blocks_to_raw_log(admitted_blocks)
        self._last_cache_stats = stats

    @staticmethod
    def _has_same_round_corpus_entry(entries, round_num, role, kind, content):
        text = str(content or "")
        for entry in entries or []:
            if not isinstance(entry, dict):
                continue
            try:
                same_round = int(entry.get("round")) == int(round_num)
            except (TypeError, ValueError):
                same_round = False
            entry_text = str(entry.get("content") or "")
            same_content = (
                entry_text.strip() == text.strip()
                if kind == "interaction"
                else entry_text == text
            )
            if (
                    same_round
                    and entry.get("role") == role
                    and entry.get("kind") == kind
                    and same_content):
                return True
        return False

    @staticmethod
    def _merge_now_entries(existing_entries, new_entries):
        merged = []
        seen = set()

        for entry in list(existing_entries or []) + list(new_entries or []):
            marker = ContextStore._entry_key(entry)
            if marker in seen:
                continue
            merged.append(ContextStore._normalize_entry(entry))
            seen.add(marker)
        return merged

    @staticmethod
    def _entry_key(entry):
        return (
            entry.get("round") if isinstance(entry, dict) else None,
            entry.get("role") if isinstance(entry, dict) else None,
            entry.get("kind") if isinstance(entry, dict) else None,
            entry.get("content") if isinstance(entry, dict) else None,
            entry.get("step") if isinstance(entry, dict) else None,
            entry.get("iter", 0) if isinstance(entry, dict) else 0,
            entry.get("transient_scope") if isinstance(entry, dict) else None,
            entry.get("transient_target_step") if isinstance(entry, dict) else None,
            entry.get("transient_target_iteration") if isinstance(entry, dict) else None,
        )

    @staticmethod
    def _is_call_transient(entry):
        if not isinstance(entry, dict):
            return False
        if entry.get("call_transient") is True:
            return True
        if str(entry.get("transient_scope") or "").strip() == "setup_to_reaction":
            return False
        # 兼容尚未被下一次同步改写的旧 C 轨条目。
        return bool(
            entry.get("transient_scope")
            or entry.get("expires_after_step")
            or str(entry.get("round_retention") or "").strip() == "drop"
        )

    @staticmethod
    def _normalize_entry(entry):
        if not isinstance(entry, dict):
            return entry
        normalized = dict(entry)
        normalized.setdefault("kind", "assistant_reply" if normalized.get("role") == "assistant" else "interaction")
        normalized.setdefault("interaction_object", "unknown")
        normalized.setdefault("identity_status", "unknown")
        normalized.setdefault("interaction_source", "unresolved")
        normalized.setdefault("step", "round")
        normalized.setdefault("iter", 0)
        legacy_setup_fact = (
            str(normalized.get("transient_scope") or "").strip()
            == "setup_to_reaction"
        )
        if legacy_setup_fact:
            normalized.pop("transient_scope", None)
            normalized.pop("round_retention", None)
        if str(normalized.get("round_retention") or "").strip() == "round_pinned":
            normalized.pop("round_retention", None)
        if ContextStore._is_call_transient(normalized):
            normalized["call_transient"] = True
            if not normalized.get("transient_target_step"):
                normalized["transient_target_step"] = (
                    normalized.get("expires_after_step")
                    or normalized.get("step")
                    or "reaction"
                )
        elif (
                normalized.get("kind") == "reasoning_context"
                and isinstance(normalized.get("native_replay"), dict)
        ):
            # 旧 now-only 推理续接：只允许下一次 reaction 消费一次。
            normalized["call_transient"] = True
            normalized["transient_scope"] = "reasoning_replay"
            normalized["transient_target_step"] = "reaction"
            normalized["transient_target_iteration"] = max(
                int(normalized.get("iter", 0) or 0) + 2,
                1,
            )
        normalized.pop("expires_after_step", None)
        normalized.pop("round_retention", None)
        normalized.pop("now", None)
        normalized.pop("lately", None)
        return normalized

    def _write_now_cache(self, entries):
        blocks = []
        for i, entry in enumerate(entries or []):
            block = self._entry_to_corpus_block(entry, index=i, cache_policy="now")
            if block:
                blocks.append(block)
        self._write_jsonl_atomic(self._now_cache_jsonl(), blocks)

    def _write_lately_cache(self, entries):
        blocks = self._entries_to_lately_blocks(entries)
        self._write_jsonl_atomic(self._lately_cache_jsonl(), blocks)
        return blocks

    def _load_lately_entries(self):
        return [entry for entry in (
            self._corpus_block_to_entry(block)
            for block in self._read_jsonl(self._lately_cache_jsonl())
        ) if (
            entry
            and self._is_model_context_kind(entry.get("kind"))
            and not self._is_call_transient(entry)
        )]

    def _entries_to_lately_blocks(self, entries):
        blocks = []
        allowed = set(self._lately_allowed_kinds())
        for i, entry in enumerate(entries or []):
            if entry.get("kind") not in allowed:
                continue
            if not self._entry_lately_enabled(entry):
                continue
            block = self._entry_to_corpus_block(entry, index=i, cache_policy="lately")
            if block:
                blocks.append(block)
        return blocks

    @staticmethod
    def _empty_cache_stats():
        return {
            "now_moved_blocks": 0,
            "now_moved_chars": 0,
            "now_dropped_blocks": 0,
            "now_dropped_chars": 0,
            "now_soft_overflow": False,
            "now_soft_overflow_chars": 0,
            "lately_deleted_blocks": 0,
            "lately_deleted_chars": 0,
            "lately_trimmed": False,
            "cache_compaction_required": False,
            "lately_surviving_chars": 0,
            "lately_compact_target_chars": 0,
            "lately_compact_ratio": 0.618,
            "lately_compact_shard_chars": 8192,
            "lately_compact_shard_ratio": 0.314,
        }

    def _now_cache_params(self):
        return self.config_store.get_now_cache_params()

    def _lately_cache_params(self):
        return self.config_store.get_lately_cache_params()

    @staticmethod
    def _sanitize_int(value, default):
        try:
            return int(value)
        except (TypeError, ValueError):
            return int(default)

    @staticmethod
    def _sanitize_ratio(value, default):
        try:
            ratio = float(value)
        except (TypeError, ValueError):
            ratio = float(default)
        return min(1.0, max(0.0, ratio))

    def _apply_now_watermark(self, entries):
        active = [
            self._normalize_entry(entry)
            for entry in entries or []
            if (
                isinstance(entry, dict)
                and self._is_model_context_kind(entry.get("kind"))
                and not self._is_call_transient(entry)
            )
        ]
        params = self._now_cache_params()
        total = self._entries_chars(active)
        stats = self._empty_cache_stats()
        if total <= params["budget_chars"] or not active:
            return active, [], stats

        target_chars = max(0, params["budget_chars"] - params["trim_chars"])
        promoted = []
        index = 0
        while index < len(active) and total > target_chars:
            entry = active.pop(index)
            chars = self._entry_chars(entry)
            total -= chars
            if self._entry_lately_enabled(entry):
                promoted.append(entry)
                stats["now_moved_blocks"] += 1
                stats["now_moved_chars"] += chars
            else:
                stats["now_dropped_blocks"] += 1
                stats["now_dropped_chars"] += chars
        if total > params["budget_chars"]:
            stats["now_soft_overflow"] = True
            stats["now_soft_overflow_chars"] = total - params["budget_chars"]
        return active, promoted, stats

    def _apply_lately_watermark(self, entries):
        active = [
            self._normalize_entry(entry)
            for entry in entries or []
            if (
                isinstance(entry, dict)
                and self._is_model_context_kind(entry.get("kind"))
            )
        ]
        params = self._lately_cache_params()
        total = self._entries_chars(active)
        stats = self._empty_cache_stats()
        if total <= params["budget_chars"] or not active:
            return active, stats

        target_chars = max(0, params["budget_chars"] - params["trim_chars"])
        while active and total > target_chars:
            entry = active.pop(0)
            chars = self._entry_chars(entry)
            total -= chars
            stats["lately_deleted_blocks"] += 1
            stats["lately_deleted_chars"] += chars
            stats["lately_trimmed"] = True
        if stats["lately_trimmed"]:
            compaction = self.get_lately_compaction_params()
            stats.update({
                "cache_compaction_required": True,
                "lately_surviving_chars": total,
                "lately_compact_target_chars": int(total * compaction["compact_ratio"]),
                "lately_compact_ratio": compaction["compact_ratio"],
                "lately_compact_shard_chars": compaction["compact_shard_chars"],
                "lately_compact_shard_ratio": compaction["compact_shard_ratio"],
            })
        return active, stats

    def _merge_lately_entries(self, existing_entries, promoted_entries):
        merged = []
        seen = set()
        for entry in list(existing_entries or []) + list(promoted_entries or []):
            if not isinstance(entry, dict):
                continue
            if not self._is_model_context_kind(entry.get("kind")):
                continue
            marker = self._entry_key(entry)
            if marker in seen:
                continue
            merged.append(self._normalize_entry(entry))
            seen.add(marker)
        return merged

    def _entry_lately_enabled(self, entry):
        if not isinstance(entry, dict):
            return False
        if self._is_call_transient(entry):
            return False
        kind = entry.get("kind")
        if kind not in set(self._lately_allowed_kinds()):
            return False
        policy = self._policy_for_entry(entry, kind, "now")
        return bool(self._sanitize_policy(policy, kind=kind).get("lately"))

    @staticmethod
    def _entry_chars(entry):
        if not isinstance(entry, dict):
            return 0
        return len(str(entry.get("content") or ""))

    @classmethod
    def _entries_chars(cls, entries):
        return sum(cls._entry_chars(entry) for entry in entries or [])

    def _lately_allowed_kinds(self):
        lanes = self._persistent_lanes()
        return list(dict.fromkeys(
            lanes["now_lately_raw"]
            + lanes["now_lately_no_raw"]
            + ["cache_summary"]
        ))

    def _persistent_lanes(self):
        configured = self.config_store.get_context_persistent_lanes()
        lanes = {
            lane: list(dict.fromkeys(configured[lane]))
            for lane in ("now_lately_raw", "now_lately_no_raw")
        }
        overlap = set(lanes["now_lately_raw"]) & set(lanes["now_lately_no_raw"])
        if overlap:
            raise ValueError("持久语料轨道重复 kind: " + ", ".join(sorted(overlap)))
        return lanes

    def _persistent_lane_for_kind(self, kind):
        token = str(kind or "").strip()
        lanes = self._persistent_lanes()
        for lane, kinds in lanes.items():
            if token in set(kinds):
                return lane
        return ""

    def _raw_log_excluded_kinds(self):
        return set(self._persistent_lanes()["now_lately_no_raw"])

    def _compaction_excluded_kinds(self):
        return set(self._persistent_lanes()["now_lately_no_raw"])

    def _policy_by_kind(self):
        lanes = self._persistent_lanes()
        policies = {
            kind: {"now": True, "lately": True}
            for kind in lanes["now_lately_raw"] + lanes["now_lately_no_raw"]
        }
        policies["cache_summary"] = {"now": False, "lately": True}
        return policies

    @staticmethod
    def _entry_round(entry):
        try:
            return int(entry.get("round"))
        except (TypeError, ValueError, AttributeError):
            return None

    def _entry_to_corpus_block(self, entry, index=0, cache_policy="lately"):
        if not isinstance(entry, dict):
            return None
        normalized = self._normalize_entry(entry)
        content = str(normalized.get("content", ""))
        if not content:
            return None
        role = normalized.get("role") or "system"
        kind = normalized.get("kind") or ("assistant_reply" if role == "assistant" else "interaction")
        if not self._is_model_context_kind(kind):
            return None
        if self._is_template_placeholder(kind, content):
            return None
        round_num = self._entry_round(normalized) or 0
        timestamp = normalized.get("timestamp") or datetime.now(TZ_SHANGHAI).isoformat()
        policy = self._policy_for_entry(normalized, kind, cache_policy)
        interaction_ref = {
            "object": normalized.get("interaction_object", "unknown"),
            "identity_status": normalized.get("identity_status", "unknown"),
            "interaction_source": normalized.get("interaction_source", "unresolved"),
        }
        if normalized.get("interaction_object_id"):
            interaction_ref["object_id"] = normalized["interaction_object_id"]
        ref = {"interaction": interaction_ref}
        if normalized.get("raw_log_key"):
            ref["raw_log_key"] = normalized.get("raw_log_key")
        if normalized.get("source_block_id"):
            ref["source_block_id"] = normalized.get("source_block_id")
        for key in (
            "source_block_ids",
            "raw_log_keys",
            "oldest_source_round",
            "oldest_cached_at",
            "source_block_count",
            "compacted_at",
            "compact_reason",
            "transient_scope",
            "transient_target_step",
            "transient_target_iteration",
        ):
            if normalized.get(key):
                ref[key] = normalized.get(key)
        if isinstance(normalized.get("tool_result"), dict):
            ref["tool_result"] = context_safe_read_tool_result(
                normalized.get("tool_result")
            )
        if isinstance(normalized.get("protocol_receipt"), dict):
            ref["protocol_receipt"] = dict(normalized.get("protocol_receipt"))
        if isinstance(normalized.get("protocol_receipts"), list):
            ref["protocol_receipts"] = [
                dict(item)
                for item in normalized.get("protocol_receipts")
                if isinstance(item, dict)
            ]
        if isinstance(normalized.get("native_replay"), dict):
            ref["native_replay"] = dict(normalized.get("native_replay"))
        return {
            "id": f"R{round_num:06d}-{role}-{index:04d}",
            "role": role,
            "kind": kind,
            "text": content,
            "loc": {
                "round": round_num,
                "step": normalized.get("step", "round"),
                "iter": int(normalized.get("iter", 0) or 0),
                "time": timestamp,
            },
            "policy": policy,
            "ref": ref,
        }

    def _policy_for_kind(self, kind, cache_policy):
        policies = self._policy_by_kind()
        policy = dict(policies.get(kind, {"now": True, "lately": False}))
        if cache_policy == "now":
            policy["now"] = True
        if cache_policy == "lately":
            policy["lately"] = True
        return self._sanitize_policy(policy, kind=kind)

    def _policy_for_entry(self, entry, kind, cache_policy):
        if self._is_call_transient(entry):
            return {"now": True, "lately": False}
        policy = self._policy_for_kind(kind, cache_policy)
        return self._sanitize_policy(policy, kind=kind)

    @staticmethod
    def _sanitize_policy(policy, kind=None):
        source = dict(policy or {})
        extra = set(source) - {"now", "lately"}
        if extra:
            raise ValueError(
                "corpus_block.policy 不允许字段: " + ", ".join(sorted(extra)))
        default_lately = kind in {
            "interaction",
            "assistant_reply",
            "dialogue_progress",
            "tool_fact",
            "setup_fact",
            "relay_handoff",
            "minimum_commitment",
            "fault_note",
            "cache_summary",
            "material",
        }
        return {
            "now": bool(source.get("now", True)),
            "lately": bool(source.get("lately", default_lately)),
        }

    @classmethod
    def _sanitize_legacy_policy(cls, policy, kind=None):
        source = dict(policy or {})
        for key in ("ttl", "backup"):
            source.pop(key, None)
        return cls._sanitize_policy(source, kind=kind)

    @classmethod
    def _corpus_block_to_entry(cls, block):
        if not isinstance(block, dict):
            return None
        loc = block.get("loc") if isinstance(block.get("loc"), dict) else {}
        ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
        interaction = ref.get("interaction") if isinstance(ref.get("interaction"), dict) else {}
        policy = block.get("policy") if isinstance(block.get("policy"), dict) else {}
        text = block.get("text", "")
        if not text:
            return None
        kind = block.get("kind", "handoff")
        if not cls._is_model_context_kind(kind):
            return None
        if cls._is_template_placeholder(kind, text):
            return None
        entry = {
            "round": loc.get("round"),
            "role": block.get("role", "system"),
            "kind": kind,
            "content": text,
            "timestamp": loc.get("time", ""),
            "step": loc.get("step", "round"),
            "iter": loc.get("iter", 0),
            "interaction_object": interaction.get("object", "unknown"),
            "interaction_object_id": interaction.get("object_id"),
            "identity_status": interaction.get("identity_status", "unknown"),
            "interaction_source": interaction.get("interaction_source", "unresolved"),
            "raw_log_key": ref.get("raw_log_key"),
            "source_block_id": block.get("id"),
            "source_block_ids": ref.get("source_block_ids"),
            "raw_log_keys": ref.get("raw_log_keys"),
            "oldest_source_round": ref.get("oldest_source_round"),
            "oldest_cached_at": ref.get("oldest_cached_at"),
            "source_block_count": ref.get("source_block_count"),
            "compacted_at": ref.get("compacted_at"),
            "compact_reason": ref.get("compact_reason"),
            "transient_scope": ref.get("transient_scope"),
            "transient_target_step": ref.get("transient_target_step"),
            "transient_target_iteration": ref.get("transient_target_iteration"),
            "tool_result": context_safe_read_tool_result(ref.get("tool_result")),
            "protocol_receipt": ref.get("protocol_receipt"),
            "protocol_receipts": ref.get("protocol_receipts"),
            "native_replay": ref.get("native_replay"),
        }
        if "now" in policy:
            entry["now"] = policy.get("now")
        if "lately" in policy:
            entry["lately"] = policy.get("lately")
        return entry

    @classmethod
    def _is_template_placeholder(cls, kind, content):
        token = str(content or "").strip().strip("`").strip().lower()
        return token in cls.TEMPLATE_PLACEHOLDER_TOKENS.get(kind, set())

    @classmethod
    def _is_model_context_kind(cls, kind):
        return str(kind or "").strip() in cls.MODEL_CONTEXT_KINDS

    @staticmethod
    def _write_jsonl_atomic(path, items):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                for item in items or []:
                    f.write(json.dumps(item, ensure_ascii=False, separators=(",", ":")) + "\n")
            os.replace(tmp, path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(path, cause=e)

    @staticmethod
    def _write_json_atomic(path, data):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f, ensure_ascii=False, separators=(",", ":"))
            os.replace(tmp, path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(path, cause=e)

    @staticmethod
    def _read_jsonl(path):
        if not path or not os.path.isfile(path):
            return []
        items = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if not line:
                        continue
                    try:
                        items.append(json.loads(line))
                    except json.JSONDecodeError:
                        continue
        except OSError:
            return []
        return items

    def _cache_dir(self):
        if self._cache_dir_override:
            return self._cache_dir_override
        return STM_CONTEXT_CACHE_DIR

    def _now_cache_jsonl(self):
        if self._now_cache_jsonl_override:
            return self._now_cache_jsonl_override
        if STM_CONTEXT_NOW_CACHE_JSONL == _DEFAULT_NOW_CACHE_JSONL:
            return os.path.join(self._cache_dir(), "now_cache.jsonl")
        return STM_CONTEXT_NOW_CACHE_JSONL

    def _lately_cache_jsonl(self):
        if self._lately_cache_jsonl_override:
            return self._lately_cache_jsonl_override
        if STM_CONTEXT_LATELY_CACHE_JSONL == _DEFAULT_LATELY_CACHE_JSONL:
            return os.path.join(self._cache_dir(), "lately_cache.jsonl")
        return STM_CONTEXT_LATELY_CACHE_JSONL

    def _raw_log_jsonl(self):
        if self._raw_log_jsonl_override:
            return self._raw_log_jsonl_override
        if RAW_LOG_JSONL == _DEFAULT_RAW_LOG_JSONL:
            if self._raw_log_md_override:
                return os.path.join(os.path.dirname(self._raw_log_md_override), "raw_log.jsonl")
            if RAW_LOG != _DEFAULT_RAW_LOG:
                return os.path.join(os.path.dirname(RAW_LOG), "raw_log.jsonl")
            return RAW_LOG_JSONL
        return RAW_LOG_JSONL

    def _raw_log_md(self):
        if self._raw_log_md_override:
            return self._raw_log_md_override
        return RAW_LOG

    def _raw_log_key(self, block):
        ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
        if ref.get("raw_log_key"):
            return str(ref["raw_log_key"])
        loc = block.get("loc") if isinstance(block.get("loc"), dict) else {}
        payload = {
            "round": loc.get("round"),
            "step": loc.get("step"),
            "iter": loc.get("iter"),
            "role": block.get("role"),
            "kind": block.get("kind"),
            "text": block.get("text", ""),
        }
        raw = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _normalize_raw_log_block(self, block):
        if not isinstance(block, dict):
            return None
        clone = {
            "id": block.get("id", ""),
            "role": block.get("role", "system"),
            "kind": block.get("kind", "handoff"),
            "text": block.get("text", ""),
            "loc": dict(block.get("loc") if isinstance(block.get("loc"), dict) else {}),
            "policy": self._sanitize_legacy_policy(block.get("policy"), kind=block.get("kind")),
            "ref": dict(block.get("ref") if isinstance(block.get("ref"), dict) else {}),
        }
        if not clone["text"]:
            return None
        clone["ref"]["raw_log_key"] = self._raw_log_key(clone)
        return clone

    @staticmethod
    def _is_compacted_summary(block):
        if not isinstance(block, dict):
            return False
        ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
        return block.get("kind") == "cache_summary" or ref.get("compact_reason") == "post_lately_trim"

    def _normalize_lately_block(self, block):
        if not isinstance(block, dict):
            return None
        clone = {
            "id": block.get("id", ""),
            "role": block.get("role", "system"),
            "kind": block.get("kind", "handoff"),
            "text": block.get("text", ""),
            "loc": dict(block.get("loc") if isinstance(block.get("loc"), dict) else {}),
            "policy": self._sanitize_legacy_policy(block.get("policy"), kind=block.get("kind")),
            "ref": dict(block.get("ref") if isinstance(block.get("ref"), dict) else {}),
        }
        if not clone["text"]:
            return None
        if (
            not self._is_compacted_summary(clone)
            and clone.get("kind") not in self._raw_log_excluded_kinds()
        ):
            clone["ref"]["raw_log_key"] = self._raw_log_key(clone)
        elif clone.get("kind") in self._raw_log_excluded_kinds():
            clone["ref"].pop("raw_log_key", None)
        return clone

    def _mirror_lately_blocks_to_raw_log(self, lately_blocks):
        """raw_log 只镜像 lately 接纳的语料块；以 stable raw_log_key 去重。"""
        normalized_existing = [
            block for block in (
                self._normalize_raw_log_block(item)
                for item in self._read_jsonl(self._raw_log_jsonl())
            )
            if block
        ]
        by_key = {
            self._raw_log_key(block): block
            for block in normalized_existing
        }
        changed = False
        for block in lately_blocks or []:
            if self._is_compacted_summary(block):
                continue
            if block.get("kind") in self._raw_log_excluded_kinds():
                continue
            raw_block = self._normalize_raw_log_block(block)
            if not raw_block:
                continue
            key = self._raw_log_key(raw_block)
            if key in by_key:
                continue
            by_key[key] = raw_block
            normalized_existing.append(raw_block)
            changed = True
        if not changed and os.path.isfile(self._raw_log_jsonl()):
            return
        self._write_jsonl_atomic(self._raw_log_jsonl(), normalized_existing)
        try:
            atomic_write_text(self._raw_log_md(), self._render_raw_log_md(normalized_existing))
        except WriteError:
            pass

    def build_lately_compression_candidates(self, current_round=None, max_blocks=None):
        """返回删后幸存段的完整 lately 语料块，供善后语义融合压缩。"""
        candidates = []
        for block in self._read_jsonl(self._lately_cache_jsonl()):
            if not isinstance(block, dict):
                continue
            kind = block.get("kind")
            if kind in self._compaction_excluded_kinds():
                continue
            loc = block.get("loc") if isinstance(block.get("loc"), dict) else {}
            round_num = loc.get("round")
            text = str(block.get("text", "") or "")
            if not text:
                continue
            ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
            raw_log_keys = ref.get("raw_log_keys")
            if not isinstance(raw_log_keys, list):
                raw_log_keys = [self._raw_log_key(block)] if not self._is_compacted_summary(block) else []
            candidates.append({
                "id": block.get("id", ""),
                "raw_log_key": self._raw_log_key(block),
                "raw_log_keys": raw_log_keys,
                "source_block_ids": ref.get("source_block_ids") if isinstance(ref.get("source_block_ids"), list) else [block.get("id", "")],
                "round": round_num,
                "step": loc.get("step", "round"),
                "kind": kind,
                "chars": len(text),
                "text": text,
            })
            if max_blocks is not None and len(candidates) >= int(max_blocks):
                break
        return candidates

    def rewrite_lately_blocks(self, decisions, current_round=None):
        """按 cache_compact 语义重写 lately；raw_log 原文镜像不随压缩改写。"""
        blocks = [
            block for block in (
                self._normalize_lately_block(item)
                for item in self._read_jsonl(self._lately_cache_jsonl())
            )
            if block
        ]
        before_chars = sum(len(str(block.get("text", "") or "")) for block in blocks)
        if not decisions:
            return {
                "kept": len(blocks),
                "replaced": 0,
                "dropped": 0,
                "skipped": 0,
                "summaries": 0,
                "source_blocks": 0,
                "before_chars": before_chars,
                "after_chars": before_chars,
            }

        id_to_block = {str(block.get("id", "")): block for block in blocks}
        compactable_blocks = [
            block for block in blocks
            if block.get("kind") not in self._compaction_excluded_kinds()
        ]
        candidate_id_by_number = {
            str(index): str(block.get("id", ""))
            for index, block in enumerate(compactable_blocks, start=1)
            if str(block.get("id", ""))
        }
        raw_to_id = {
            self._raw_log_key(block): str(block.get("id", ""))
            for block in compactable_blocks
            if not self._is_compacted_summary(block)
        }
        index_by_id = {str(block.get("id", "")): index for index, block in enumerate(blocks)}
        grouped = {}
        skip_ids = set()
        claimed_ids = set()
        stats = {
            "kept": 0,
            "replaced": 0,
            "dropped": 0,
            "skipped": 0,
            "summaries": 0,
            "source_blocks": 0,
            "before_chars": before_chars,
            "after_chars": 0,
        }
        for decision in decisions:
            if not isinstance(decision, dict):
                stats["skipped"] += 1
                continue
            source_ids = self._decision_source_block_ids(decision)
            for number in self._decision_candidate_numbers(decision):
                block_id = candidate_id_by_number.get(number)
                if block_id:
                    source_ids.append(block_id)
            raw_key = str(decision.get("raw_log_key") or "").strip()
            if raw_key and raw_key in raw_to_id:
                source_ids.append(raw_to_id[raw_key])
            source_ids = [
                source_id for source_id in dict.fromkeys(source_ids)
                if source_id in id_to_block
                and id_to_block[source_id].get("kind")
                not in self._compaction_excluded_kinds()
            ]
            source_ids.sort(key=lambda item: index_by_id[item])
            if not source_ids:
                stats["skipped"] += 1
                continue
            action = str(decision.get("action", "keep")).strip().lower()
            if action in {"", "keep", "保留"}:
                stats["kept"] += len(source_ids)
                continue
            if any(source_id in claimed_ids for source_id in source_ids):
                stats["skipped"] += len(source_ids)
                continue
            first_id = source_ids[0]
            spec = dict(decision)
            spec["source_block_ids"] = source_ids
            grouped[first_id] = spec
            claimed_ids.update(source_ids)
            skip_ids.update(source_ids[1:])

        rewritten = []
        now = datetime.now(TZ_SHANGHAI).isoformat()
        summary_index = 0
        for block in blocks:
            block_id = str(block.get("id", ""))
            if block_id in skip_ids:
                continue
            decision = grouped.get(block_id)
            if not decision:
                rewritten.append(block)
                continue
            action = str(decision.get("action", "keep")).strip().lower()
            source_ids = decision.get("source_block_ids") or [block_id]
            source_blocks = [id_to_block[source_id] for source_id in source_ids if source_id in id_to_block]
            if action in {"drop", "delete", "删除", "丢弃"}:
                stats["dropped"] += len(source_blocks)
                stats["source_blocks"] += len(source_blocks)
                continue
            if action in {"replace", "compress", "压缩", "替换"}:
                replacement = str(
                    decision.get("replacement_text")
                    or decision.get("replacement")
                    or decision.get("text")
                    or ""
                ).strip()
                if not replacement:
                    stats["skipped"] += 1
                    rewritten.extend(source_blocks)
                    continue
                new_block = self._build_cache_summary_block(
                    source_blocks,
                    replacement,
                    now,
                    current_round=current_round,
                    index=summary_index,
                )
                summary_index += 1
                stats["replaced"] += len(source_blocks)
                stats["summaries"] += 1
                stats["source_blocks"] += len(source_blocks)
                rewritten.append(new_block)
                continue
            stats["skipped"] += 1
            rewritten.extend(source_blocks)

        stats["after_chars"] = sum(len(str(block.get("text", "") or "")) for block in rewritten)
        self._write_jsonl_atomic(self._lately_cache_jsonl(), rewritten)
        return stats

    @staticmethod
    def _decision_source_block_ids(decision):
        values = []
        raw_values = decision.get("source_block_ids")
        if isinstance(raw_values, list):
            values.extend(raw_values)
        elif raw_values:
            values.extend(re.split(r"[,，\s]+", str(raw_values)))
        block_id = decision.get("block_id") or decision.get("id")
        if block_id:
            values.append(block_id)
        return [str(value).strip() for value in values if str(value).strip()]

    @staticmethod
    def _decision_candidate_numbers(decision):
        values = []
        for key in ("candidate_numbers", "candidate_number", "候选编号", "候选"):
            raw_values = decision.get(key)
            if isinstance(raw_values, list):
                values.extend(raw_values)
            elif raw_values:
                values.extend(re.split(r"[,，、\s]+", str(raw_values)))
        numbers = []
        for value in values:
            match = re.search(r"\d+", str(value or ""))
            if match:
                numbers.append(str(int(match.group(0))))
        return list(dict.fromkeys(numbers))

    def _build_cache_summary_block(self, source_blocks, text, compacted_at,
                                   current_round=None, index=0):
        source_ids = [str(block.get("id", "")) for block in source_blocks if block.get("id")]
        raw_log_keys = []
        source_round_values = []
        source_locs = []
        for block in source_blocks:
            ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
            existing_keys = ref.get("raw_log_keys")
            if isinstance(existing_keys, list):
                raw_log_keys.extend(str(key) for key in existing_keys if str(key))
            elif ref.get("raw_log_key"):
                raw_log_keys.append(str(ref["raw_log_key"]))
            elif not self._is_compacted_summary(block):
                raw_log_keys.append(self._raw_log_key(block))
            loc = block.get("loc") if isinstance(block.get("loc"), dict) else {}
            if loc.get("round") is not None:
                source_round_values.append(loc.get("round"))
            if loc:
                source_locs.append(loc)
        try:
            round_num = int(current_round) if current_round is not None else int(source_round_values[-1])
        except (TypeError, ValueError, IndexError):
            round_num = 0
        oldest_loc = source_locs[0] if source_locs else {}
        return {
            "id": f"CMP-R{round_num:06d}-{index:04d}",
            "role": "system",
            "kind": "cache_summary",
            "text": text,
            "loc": {
                "round": round_num,
                "step": "cleanup",
                "iter": 0,
                "time": compacted_at,
            },
            "policy": {"now": False, "lately": True},
            "ref": {
                "source_block_ids": source_ids,
                "raw_log_keys": list(dict.fromkeys(raw_log_keys)),
                "oldest_source_round": oldest_loc.get("round"),
                "oldest_cached_at": oldest_loc.get("time", ""),
                "source_block_count": len(source_blocks or []),
                "compacted_at": compacted_at,
                "compact_reason": "post_lately_trim",
            },
        }

    @staticmethod
    def _render_raw_log_md(blocks):
        if not blocks:
            return "<!-- 原始语料备份 -->\n"
        parts = ["<!-- 原始语料备份 -->"]
        current_round = None
        for block in blocks:
            loc = block.get("loc") if isinstance(block.get("loc"), dict) else {}
            round_num = loc.get("round", "?")
            if round_num != current_round:
                current_round = round_num
                parts.append("")
                parts.append(f"## R{round_num} [{loc.get('time', '')}]")
            role = block.get("role", "system")
            kind = block.get("kind", "handoff")
            text = block.get("text", "")
            ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
            interaction = ref.get("interaction") if isinstance(ref.get("interaction"), dict) else {}
            meta = (
                f"交互对象={interaction.get('object', 'unknown')}; "
                f"identity_status={interaction.get('identity_status', 'unknown')}; "
                f"interaction_source={interaction.get('interaction_source', 'unresolved')}"
            )
            parts.append(f"**{role} / {kind}** ({meta}): {text}")
        return "\n\n".join(parts).rstrip() + "\n"

    # ==========================================================
    # raw_log.md / raw_log.jsonl
    # ==========================================================

    def read_raw_log(self):
        """读取 raw_log 审计渲染全文。"""
        path = self._raw_log_md()
        if not os.path.isfile(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except OSError:
            return ""

    def archive_raw_log(self):
        """
        归档 raw_log 到 COR 语料库容器（节律轮固定职责）。
        返回归档文件路径，或 None。
        """
        jsonl_content = ""
        if os.path.isfile(self._raw_log_jsonl()):
            try:
                with open(self._raw_log_jsonl(), "r", encoding="utf-8") as f:
                    jsonl_content = f.read()
            except OSError:
                jsonl_content = ""
        content = self.read_raw_log()
        if (
            not jsonl_content.strip()
            and (not content.strip() or content.strip() == "<!-- 原始语料备份 -->")
        ):
            return None

        from paths import CONTAINER_CORPUS_DIR
        archive_dir = os.path.join(CONTAINER_CORPUS_DIR, "raw_logs")
        os.makedirs(archive_dir, exist_ok=True)
        stamp = datetime.now(TZ_SHANGHAI).strftime("%Y%m%d_%H%M%S")
        archive_path = None

        if jsonl_content.strip():
            archive_path = os.path.join(archive_dir, f"raw_log_{stamp}.jsonl")
            tmp = archive_path + ".tmp"
            try:
                with open(tmp, "w", encoding="utf-8") as f:
                    f.write(jsonl_content)
                os.replace(tmp, archive_path)
            except OSError as e:
                if os.path.isfile(tmp):
                    os.remove(tmp)
                raise WriteError(archive_path, cause=e)

        if content.strip() and content.strip() != "<!-- 原始语料备份 -->":
            md_archive_path = os.path.join(archive_dir, f"raw_log_{stamp}.md")
            tmp_md = md_archive_path + ".tmp"
            try:
                with open(tmp_md, "w", encoding="utf-8") as f:
                    f.write(content)
                os.replace(tmp_md, md_archive_path)
            except OSError:
                if os.path.isfile(tmp_md):
                    os.remove(tmp_md)
            if archive_path is None:
                archive_path = md_archive_path

        self.clear_raw_log()
        return archive_path

    def clear_raw_log(self):
        """清空 raw_log（归档后调用）。"""
        jsonl_path = self._raw_log_jsonl()
        if os.path.isfile(jsonl_path):
            atomic_write_text(jsonl_path, "")
        tmp = RAW_LOG + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("<!-- 原始语料备份 -->\n")
            os.replace(tmp, RAW_LOG)
        except OSError:
            if os.path.isfile(tmp):
                os.remove(tmp)
