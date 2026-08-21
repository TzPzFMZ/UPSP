"""上下文缓存读写：now/lately 热缓存、raw_log 与 Corpus 节归档。"""
import json
import os
import hashlib
import re
from datetime import datetime

from constants import local_now
from data.atomic_write import atomic_write_text
from data.config_store import ConfigStore
from errors import ReadError, WriteError
from paths import (
    CONTAINER_CORPUS_DIR,
    RAW_LOG,
    RAW_LOG_JSONL,
    STM_CONTEXT_CACHE_DIR,
    STM_CONTEXT_NOW_CACHE_JSONL,
    STM_CONTEXT_LATELY_CACHE_JSONL,
)
from schemas.context import context_safe_read_tool_result

_DEFAULT_CONTEXT_CACHE_DIR = STM_CONTEXT_CACHE_DIR
_DEFAULT_NOW_CACHE_JSONL = STM_CONTEXT_NOW_CACHE_JSONL
_DEFAULT_LATELY_CACHE_JSONL = STM_CONTEXT_LATELY_CACHE_JSONL
_DEFAULT_RAW_LOG = RAW_LOG
_DEFAULT_RAW_LOG_JSONL = RAW_LOG_JSONL

class ContextStore:
    """now/lately 语料缓存、raw_log 与 Corpus 节归档管理。"""

    LEGACY_CACHE_COMPACTION_DEBT_V2_SCHEMA = "cache_compaction_debt.v2"
    PROGRESSIVE_CACHE_COMPACTION_DEBT_SCHEMA = "cache_compaction_debt.v3"
    CACHE_COMPACTION_DEBT_SCHEMAS = {
        "cache_compaction_debt.v1",
        LEGACY_CACHE_COMPACTION_DEBT_V2_SCHEMA,
        PROGRESSIVE_CACHE_COMPACTION_DEBT_SCHEMA,
    }
    ACTIVE_CORPUS_META_SCHEMA = "active_corpus_meta.v1"
    NOW_CACHE_LIFECYCLE_SCHEMA = "now_cache_lifecycle.v1"
    CURRENT_CACHE_TRANSITION_SCHEMA = "current_cache_transition.v1"

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
        "interaction_summary",
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
                 corpus_rhythms_dir=None, raw_log_jsonl=None, raw_log_md=None):
        self.state_store = state_store
        self.config_store = config_store or ConfigStore()
        self._cache_dir_override = cache_dir
        self._now_cache_jsonl_override = now_cache_jsonl
        self._lately_cache_jsonl_override = lately_cache_jsonl
        self._raw_log_jsonl_override = raw_log_jsonl
        self._raw_log_md_override = raw_log_md
        self._corpus_rhythms_dir_override = corpus_rhythms_dir
        self._last_cache_stats = self._empty_cache_stats()
        self._active_corpus_migrated = False

    # ==========================================================
    # now/lately cache
    # ==========================================================

    def save_round_to_cache(self, round_num, user_input, response,
                            interaction_object="unknown",
                            identity_status="unknown",
                            interaction_source="unresolved",
                            interaction_object_id=None):
        """把 Round 收束交互／回复补入下一帧 now 包。"""
        now = local_now().isoformat()
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
        now = local_now().isoformat()
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
            transient_target_iteration=max(int(iter or 0) + 1, 1),
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
            "timestamp": local_now().isoformat(),
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
        """读取当前 lately 履带中的完整语料块。"""
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
        self._migrate_active_corpus_metadata()
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

    def get_round_material_chars(self, round_num, iteration=None):
        """统计当前轮指定生产迭代的 B 轨资料字符数。"""
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
            if iteration is not None:
                try:
                    if int(entry.get("iter", 0) or 0) != int(iteration):
                        continue
                except (TypeError, ValueError):
                    continue
            marker = self._entry_key(entry)
            if marker in seen:
                continue
            seen.add(marker)
            total += self._entry_chars(entry)
        return total

    def now_cache_lifecycle_path(self):
        return os.path.join(self._cache_dir(), "now_cache_lifecycle.json")

    def reconcile_now_cache_lifecycle_on_startup(self):
        """Migrate the watermark era once, then only repair proven duplicate tails."""
        path = self.now_cache_lifecycle_path()
        if not os.path.isfile(path):
            return self.transition_current_cache(
                boundary="startup_legacy_migration",
                consumer_frame_id="runtime:startup",
                expire_call_transients=True,
                write_lifecycle_marker=True,
            )
        self._load_now_cache_lifecycle()
        return self.transition_current_cache(
            boundary="startup_duplicate_recovery",
            consumer_frame_id="runtime:startup",
            duplicate_tail_only=True,
        )

    def transition_current_cache(
            self,
            *,
            boundary,
            consumer_frame_id="",
            expire_call_transients=False,
            transient_round=None,
            transient_target_step=None,
            transient_target_iteration=None,
            duplicate_tail_only=False,
            write_lifecycle_marker=False):
        """Atomically consume the current Frame package into lately/raw."""
        boundary = str(boundary or "").strip()
        consumer_frame_id = str(consumer_frame_id or "").strip()
        if not boundary:
            raise ValueError("current cache transition boundary is required")
        target_step = str(transient_target_step or "").strip()
        try:
            target_round = (
                int(transient_round) if transient_round is not None else None)
            target_iteration = (
                int(transient_target_iteration)
                if transient_target_iteration is not None else None)
        except (TypeError, ValueError) as exc:
            raise ValueError("current cache transient selector is invalid") from exc
        marker_path = self.now_cache_lifecycle_path()
        paths = [
            self.active_corpus_meta_path(),
            self.cache_compaction_debt_path(),
            self._raw_log_jsonl(),
            self._raw_log_md(),
            self._lately_cache_jsonl(),
            self._now_cache_jsonl(),
            marker_path,
        ]
        snapshots = self._snapshot_text_files(paths)
        try:
            # Fail closed before the legacy metadata upgrader can rewrite a bad file.
            if not self._active_corpus_migrated:
                self._current_now_blocks()
                self._current_lately_blocks()
            self._migrate_active_corpus_metadata()
            debt = self.load_cache_compaction_debt()
            if debt.get("schema_version") == self.LEGACY_CACHE_COMPACTION_DEBT_V2_SCHEMA:
                self.recover_cache_compaction_debt()

            now_blocks = self._current_now_blocks()
            lately_blocks = self._current_lately_blocks()
            prefix_verified = self._verify_compaction_frozen_prefix(lately_blocks)
            now_before_sha = self._lately_blocks_sha256(now_blocks)
            lately_before_sha = self._lately_blocks_sha256(lately_blocks)

            lately_by_active_id = {}
            lately_by_identity = {}
            for block in lately_blocks:
                entry = self._corpus_block_to_entry(block)
                if entry is None:
                    raise ReadError(
                        self._lately_cache_jsonl(),
                        message="lately_cache_block_invalid",
                    )
                active_id = self._normalize_active_corpus_id(
                    entry.get("active_corpus_id"))
                identity = self._transition_entry_identity(entry)
                if active_id:
                    prior = lately_by_active_id.get(active_id)
                    if prior is not None:
                        raise ReadError(
                            self._lately_cache_jsonl(),
                            message=f"lately_active_corpus_duplicate:{active_id}",
                        )
                    lately_by_active_id[active_id] = entry
                prior_id = lately_by_identity.get(identity)
                if prior_id and prior_id != active_id:
                    raise ReadError(
                        self._lately_cache_jsonl(),
                        message="lately_active_corpus_id_conflict",
                    )
                if active_id:
                    lately_by_identity[identity] = active_id

            kept_now_blocks = []
            promoted_entries = []
            moved_ids = []
            moved_chars = 0
            lane_a = 0
            lane_b = 0
            expired_c = 0
            recovered_duplicates = 0
            lanes = self._persistent_lanes()
            lane_by_kind = {
                kind: lane
                for lane, kinds in lanes.items()
                for kind in kinds
            }
            for block in now_blocks:
                entry = self._corpus_block_to_entry(block)
                if entry is None:
                    raise ReadError(
                        self._now_cache_jsonl(),
                        message="now_cache_block_invalid",
                    )
                lane = lane_by_kind.get(str(entry.get("kind") or ""), "")
                is_transient = self._is_call_transient(entry) or not lane
                if is_transient:
                    expire_transient = bool(expire_call_transients)
                    if expire_transient and target_round is not None:
                        expire_transient = self._entry_round(entry) == target_round
                    if expire_transient and target_step:
                        expire_transient = (
                            str(entry.get("transient_target_step") or "")
                            == target_step
                        )
                    entry_target_iteration = entry.get(
                        "transient_target_iteration")
                    if (
                            expire_transient
                            and target_step
                            and entry_target_iteration is not None):
                        try:
                            expire_transient = (
                                target_iteration is not None
                                and int(entry_target_iteration) == target_iteration
                            )
                        except (TypeError, ValueError):
                            expire_transient = False
                    if expire_transient:
                        expired_c += 1
                    else:
                        kept_now_blocks.append(block)
                    continue

                active_id = self._normalize_active_corpus_id(
                    entry.get("active_corpus_id"))
                identity = self._transition_entry_identity(entry)
                duplicate = lately_by_active_id.get(active_id) if active_id else None
                existing_id = lately_by_identity.get(identity)
                if duplicate is not None:
                    if self._transition_entry_identity(duplicate) != identity:
                        raise ReadError(
                            self._now_cache_jsonl(),
                            message=f"now_lately_active_corpus_conflict:{active_id}",
                        )
                    recovered_duplicates += 1
                elif existing_id and existing_id != active_id:
                    raise ReadError(
                        self._now_cache_jsonl(),
                        message="now_lately_active_corpus_id_conflict",
                    )
                elif duplicate_tail_only:
                    kept_now_blocks.append(block)
                    continue
                else:
                    promoted_entries.append(entry)
                    if active_id:
                        lately_by_active_id[active_id] = entry
                        lately_by_identity[identity] = active_id
                    moved_ids.append(active_id or str(block.get("id") or ""))
                    moved_chars += self._entry_chars(entry)
                    if lane == "now_lately_raw":
                        lane_a += 1
                    else:
                        lane_b += 1

            promoted_blocks = []
            raw_excluded_kinds = set(lanes["now_lately_no_raw"])
            for index, entry in enumerate(promoted_entries, start=len(lately_blocks)):
                promoted = self._entry_to_corpus_block(
                    entry, index=index, cache_policy="lately")
                promoted = self._normalize_lately_block(
                    promoted,
                    raw_log_excluded_kinds=raw_excluded_kinds,
                )
                if promoted is None:
                    raise WriteError(
                        self._lately_cache_jsonl(),
                        message="current_cache_promotion_invalid",
                    )
                promoted_blocks.append(promoted)
            next_lately = list(lately_blocks) + promoted_blocks

            raw_incoming = [
                block for block, entry in zip(promoted_blocks, promoted_entries)
                if lane_by_kind.get(str(entry.get("kind") or ""))
                == "now_lately_raw"
            ]
            raw_changed = bool(raw_incoming)
            if raw_changed:
                from data.chronicle_store import dedupe_corpus_records

                existing_raw = self._current_raw_log_blocks()
                incoming_raw = self._repair_incoming_legacy_raw_key_collisions(
                    existing_raw, raw_incoming)
                next_raw = dedupe_corpus_records(existing_raw + incoming_raw)
                self._write_jsonl_atomic(self._raw_log_jsonl(), next_raw)
                atomic_write_text(
                    self._raw_log_md(), self._render_raw_log_md(next_raw))

            data_changed = bool(
                promoted_blocks or recovered_duplicates or expired_c)
            if data_changed:
                self._write_jsonl_atomic(self._lately_cache_jsonl(), next_lately)
                self._write_jsonl_atomic(self._now_cache_jsonl(), kept_now_blocks)
            if write_lifecycle_marker:
                self._write_json_atomic(marker_path, {
                    "schema_version": self.NOW_CACHE_LIFECYCLE_SCHEMA,
                })

            verified_lately = self._current_lately_blocks()
            verified_now = self._current_now_blocks()
            if (
                    self._lately_blocks_sha256(verified_lately)
                    != self._lately_blocks_sha256(next_lately)
                    or self._lately_blocks_sha256(verified_now)
                    != self._lately_blocks_sha256(kept_now_blocks)):
                raise WriteError(
                    self._cache_dir(),
                    message="current_cache_transition_verification_failed",
                )
            if raw_changed:
                verified_raw = self._current_raw_log_blocks()
                if (
                        self._lately_blocks_sha256(verified_raw)
                        != self._lately_blocks_sha256(next_raw)
                        or self.read_raw_log() != self._render_raw_log_md(next_raw)):
                    raise WriteError(
                        self._raw_log_jsonl(),
                        message="current_cache_raw_verification_failed",
                    )
            if write_lifecycle_marker:
                self._load_now_cache_lifecycle()
        except Exception:
            self._active_corpus_migrated = False
            self._restore_text_files(snapshots)
            raise

        self._last_cache_stats = self._empty_cache_stats()
        changed = data_changed or write_lifecycle_marker
        status = (
            "recovered" if recovered_duplicates and boundary.startswith("startup_")
            else ("applied" if changed else "noop")
        )
        return {
            "schema_version": self.CURRENT_CACHE_TRANSITION_SCHEMA,
            "status": status,
            "boundary": boundary,
            "consumer_frame_id": consumer_frame_id,
            "now_before_sha256": now_before_sha,
            "now_after_sha256": self._lately_blocks_sha256(kept_now_blocks),
            "lately_before_sha256": lately_before_sha,
            "lately_after_sha256": self._lately_blocks_sha256(next_lately),
            "moved_block_ids": [item for item in moved_ids if item],
            "moved_blocks": len(moved_ids),
            "moved_chars": moved_chars,
            "lane_a_blocks": lane_a,
            "lane_b_blocks": lane_b,
            "expired_c_blocks": expired_c,
            "recovered_duplicate_blocks": recovered_duplicates,
            "compaction_frozen_prefix_verified": prefix_verified,
        }

    def _load_now_cache_lifecycle(self):
        path = self.now_cache_lifecycle_path()
        try:
            with open(path, "r", encoding="utf-8") as handle:
                marker = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ReadError(path, cause=exc) from exc
        if marker != {"schema_version": self.NOW_CACHE_LIFECYCLE_SCHEMA}:
            raise ReadError(path, message="now_cache_lifecycle_invalid")
        return marker

    @staticmethod
    def _transition_entry_identity(entry):
        value = ContextStore._normalize_entry(entry)
        for key in ("active_corpus_id", "source_block_id", "raw_log_key", "now", "lately"):
            value.pop(key, None)
        return json.dumps(
            value, ensure_ascii=False, sort_keys=True, separators=(",", ":"),
        )

    def cache_compaction_debt_path(self):
        return os.path.join(self._cache_dir(), "cache_compaction_debt.json")

    def load_cache_compaction_debt(self):
        path = self.cache_compaction_debt_path()
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                debt = json.load(f)
        except (OSError, json.JSONDecodeError) as exc:
            raise ReadError(path, cause=exc) from exc
        if not isinstance(debt, dict):
            raise ReadError(path, message="cache_compaction_debt_not_object")
        if debt.get("schema_version") not in self.CACHE_COMPACTION_DEBT_SCHEMAS:
            raise ReadError(path, message="unsupported_cache_compaction_debt")
        if str(debt.get("status") or "").strip() != "open":
            return {}
        return dict(debt)

    def load_cache_compaction_record(self):
        """Read open or terminal v3 state for suppression and recovery."""
        path = self.cache_compaction_debt_path()
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                debt = json.load(handle)
        except (OSError, json.JSONDecodeError) as exc:
            raise ReadError(path, cause=exc) from exc
        if not isinstance(debt, dict):
            raise ReadError(path, message="cache_compaction_debt_not_object")
        if debt.get("schema_version") not in self.CACHE_COMPACTION_DEBT_SCHEMAS:
            raise ReadError(path, message="unsupported_cache_compaction_debt")
        return debt

    def has_cache_compaction_debt(self):
        return bool(self.load_cache_compaction_debt())

    def clear_cache_compaction_debt(self):
        path = self.cache_compaction_debt_path()
        try:
            if os.path.isfile(path):
                os.remove(path)
        except OSError as exc:
            raise WriteError(path, message="cache_compaction_debt_clear_failed", cause=exc) from exc

    def prepare_lately_pressure_compaction(self, round_num, observation):
        """Freeze a v3 interaction plan without rewriting lately."""
        from data.progressive_cache_compaction import (
            SCHEMA_VERSION,
            plan_debt,
            source_fingerprint,
        )

        observation = dict(observation or {})
        if str(observation.get("kind") or "") not in {
                "token_ratio", "context_too_long"}:
            return {"status": "skipped", "reason": "no_cache_pressure"}
        blocks = self._current_lately_blocks()
        if not blocks:
            return {"status": "skipped", "reason": "lately_empty"}
        params = self._lately_cache_params()
        logical_window = self._sanitize_int(
            observation.get("round_context_window_tokens")
            or observation.get("context_window"), 0)
        if logical_window <= 0:
            return {
                "status": "error",
                "reason": "cache_compaction_logical_window_unknown",
            }

        record = self.load_cache_compaction_record()
        legacy_note = None
        if record:
            if record.get("schema_version") == SCHEMA_VERSION:
                if str(record.get("status") or "") == "open":
                    recovered = self.recover_cache_compaction_debt()
                    return {
                        "status": "skipped",
                        "reason": "open_cache_compaction_debt",
                        "debt": recovered,
                    }
                fingerprint = source_fingerprint(blocks, params, logical_window)
                if fingerprint == str(record.get("source_fingerprint") or ""):
                    return {
                        "status": "skipped",
                        "reason": "cache_compaction_terminal_fingerprint_unchanged",
                        "terminal_phase": record.get("phase"),
                    }
            else:
                legacy = dict(record)
                try:
                    legacy = self.recover_cache_compaction_debt()
                except ReadError:
                    return {"status": "error", "reason": "legacy_cache_compaction_lately_drift"}
                if legacy and str(legacy.get("phase") or "") not in {"applied", ""}:
                    legacy_note = {
                        "schema_version": legacy.get("schema_version"),
                        "phase": legacy.get("phase"),
                        "discarded_staged_summaries": len(
                            legacy.get("staged_summaries") or []),
                    }
                else:
                    legacy_note = {
                        "schema_version": record.get("schema_version"),
                        "phase": record.get("phase"),
                    }
                self.clear_cache_compaction_debt()
        now = local_now().isoformat()
        try:
            debt = plan_debt(
                blocks,
                params,
                logical_window=logical_window,
                round_num=round_num,
                observation=observation,
                now_iso=now,
            )
        except ValueError as exc:
            return {"status": "error", "reason": str(exc)}
        if legacy_note:
            debt["legacy_migration"] = legacy_note
        if not debt.get("shards"):
            debt["status"] = "closed"
            debt["phase"] = "completed_with_protected_floor"
            debt["completed_at"] = now
        self._write_json_atomic(self.cache_compaction_debt_path(), debt)
        if debt["status"] == "closed":
            return {
                "status": "completed_with_protected_floor",
                "reason": "no_compressible_lately_content",
                "debt": debt,
            }
        return {
            "status": "prepared",
            "reason": "cache_pressure_progressive_compaction_due",
            "debt": debt,
        }

    def recover_cache_compaction_debt(self):
        """Recover active v3 writes; verify legacy v1/v2 without executing them."""
        debt = self.load_cache_compaction_debt()
        if (
                debt
                and debt.get("schema_version")
                == self.PROGRESSIVE_CACHE_COMPACTION_DEBT_SCHEMA):
            blocks = self._current_lately_blocks()
            frozen = debt.get("frozen_prefix") if isinstance(
                debt.get("frozen_prefix"), dict) else {}
            count = int(frozen.get("block_count") or 0)
            source_sha = str(frozen.get("sha256") or "")
            phase = str(debt.get("phase") or "staging")
            if phase == "applying":
                applied = debt.get("applied_prefix") if isinstance(
                    debt.get("applied_prefix"), dict) else {}
                applied_count = int(applied.get("block_count") or 0)
                applied_sha = str(applied.get("sha256") or "")
                if self._lately_prefix_matches(
                        blocks, applied_count, applied_sha):
                    from data.progressive_cache_compaction import source_fingerprint

                    meta_after = debt.get("active_corpus_meta_after")
                    if isinstance(meta_after, dict):
                        self._write_json_atomic(
                            self.active_corpus_meta_path(), meta_after)
                        if self._load_active_corpus_meta() != meta_after:
                            raise WriteError(
                                self.active_corpus_meta_path(),
                                message="cache_compaction_recovery_meta_verification_failed",
                            )
                    debt["source_fingerprint"] = source_fingerprint(
                        blocks,
                        debt.get("policy") or {},
                        debt.get("logical_window_tokens"),
                    )
                    debt["status"] = "closed"
                    debt["phase"] = str(
                        debt.get("terminal_phase") or "applied")
                    debt["updated_at"] = local_now().isoformat()
                    debt["completed_at"] = debt["updated_at"]
                    self._write_json_atomic(
                        self.cache_compaction_debt_path(), debt)
                    terminal = self.load_cache_compaction_record()
                    if (
                            terminal.get("status") != "closed"
                            or terminal.get("phase") != debt["phase"]
                            or terminal.get("source_fingerprint")
                            != debt["source_fingerprint"]):
                        raise WriteError(
                            self.cache_compaction_debt_path(),
                            message="cache_compaction_recovery_terminal_verification_failed",
                        )
                    return {}
                if self._lately_prefix_matches(blocks, count, source_sha):
                    meta_before = debt.get("active_corpus_meta_before")
                    if isinstance(meta_before, dict):
                        self._write_json_atomic(
                            self.active_corpus_meta_path(), meta_before)
                    debt["phase"] = "staging"
                    debt.pop("applied_prefix", None)
                    debt.pop("terminal_phase", None)
                    debt.pop("active_corpus_meta_before", None)
                    debt.pop("active_corpus_meta_after", None)
                    debt["updated_at"] = local_now().isoformat()
                    self._write_json_atomic(
                        self.cache_compaction_debt_path(), debt)
                    return debt
                raise ReadError(
                    self.cache_compaction_debt_path(),
                    message="cache_compaction_lately_drift",
                )
            if not self._lately_prefix_matches(blocks, count, source_sha):
                raise ReadError(
                    self.cache_compaction_debt_path(),
                    message="cache_compaction_lately_drift",
                )
            self._verify_progressive_compaction_source_map(debt, blocks, count)
            return debt
        if not debt:
            return debt
        if debt.get("schema_version") == "cache_compaction_debt.v1":
            # v1 did not persist a verifiable prefix. Keep it read-only until
            # the next measured pressure replaces it with a fresh v3 plan.
            return debt
        blocks = self._current_lately_blocks()
        fifo = debt.get("fifo") if isinstance(debt.get("fifo"), dict) else {}
        after_sha = str(fifo.get("after_sha256") or "")
        after_count = int(fifo.get("after_block_count") or 0)
        if not after_sha or not self._lately_prefix_matches(
                blocks, after_count, after_sha):
            raise ReadError(
                self.cache_compaction_debt_path(),
                message="legacy_cache_compaction_lately_drift",
            )
        return debt

    def stage_progressive_cache_compaction(
            self, results, *, current_round=None,
            current_reaction_iteration=None):
        """Validate one v3 answer sheet and atomically apply completed groups."""
        from data.progressive_cache_compaction import current_batch, pending_shards

        debt = self.recover_cache_compaction_debt()
        if not debt or debt.get("schema_version") != (
                self.PROGRESSIVE_CACHE_COMPACTION_DEBT_SCHEMA):
            return {"status": "rejected", "reason": "cache_compaction_v3_debt_missing"}
        batch = current_batch(debt)
        expected = {str(item.get("shard_id") or ""): item for item in batch}
        if not isinstance(results, list) or not results:
            return {"status": "rejected", "reason": "cache_compaction_results_required"}
        with open(self.cache_compaction_debt_path(), "r", encoding="utf-8") as handle:
            debt_text_before_batch = handle.read()
        submitted_ids = [
            str(item.get("shard_id") or "").strip()
            for item in results if isinstance(item, dict)
        ]
        if len(submitted_ids) != len(results) or len(submitted_ids) != len(set(submitted_ids)):
            return {"status": "rejected", "reason": "cache_compaction_result_ids_invalid"}
        if any(item not in expected for item in submitted_ids):
            return {"status": "rejected", "reason": "cache_compaction_unknown_shard"}
        if submitted_ids != list(expected)[:len(submitted_ids)]:
            return {"status": "rejected", "reason": "cache_compaction_out_of_order"}

        accepted = []
        rejected = []
        for raw in results:
            shard_id = str(raw.get("shard_id") or "").strip()
            shard = expected[shard_id]
            action = str(raw.get("action") or "").strip().lower()
            semantic = str(raw.get("semantic_content") or "").strip()
            reason = str(raw.get("reason") or "").strip()
            if action not in {"replace", "keep"}:
                rejected.append({"shard_id": shard_id, "reason": "action_invalid"})
                continue
            if action == "keep" and (not reason or semantic):
                rejected.append({"shard_id": shard_id, "reason": "keep_reason_required"})
                continue
            if action == "replace" and len(semantic) > int(shard.get("target_chars") or 0):
                rejected.append({
                    "shard_id": shard_id,
                    "reason": "semantic_content_too_long",
                    "actual_chars": len(semantic),
                    "limit_chars": int(shard.get("target_chars") or 0),
                })
                continue
            accepted.append({
                "shard_id": shard_id,
                "action": action,
                "semantic_content": semantic,
                "reason": reason,
                "source_sha256": str(shard.get("projection_sha256") or ""),
            })
        if accepted:
            debt["results"] = list(debt.get("results") or []) + accepted
            debt["revision"] = int(debt.get("revision") or 0) + 1
            debt["updated_at"] = local_now().isoformat()
            self._write_json_atomic(self.cache_compaction_debt_path(), debt)
        remaining = pending_shards(debt)
        applied = False
        terminal_phase = ""
        if self._progressive_compaction_target_met(debt) or not remaining:
            try:
                terminal_phase = self._apply_progressive_cache_compaction_debt(
                    debt,
                    current_round=current_round,
                    current_reaction_iteration=current_reaction_iteration,
                )
            except Exception:
                atomic_write_text(
                    self.cache_compaction_debt_path(), debt_text_before_batch)
                raise
            applied = True
            remaining = []
        return {
            "status": "applied" if accepted else "rejected",
            "reason": (
                terminal_phase or "cache_compaction_batch_staged"
                if accepted else "cache_compaction_batch_rejected"
            ),
            "completed_ids": [item["shard_id"] for item in accepted],
            "remaining_ids": [str(item.get("shard_id") or "") for item in remaining],
            "rejected_results": rejected,
            "rewrite_applied": applied,
            "compaction_id": debt.get("compaction_id"),
        }

    def _apply_progressive_cache_compaction_debt(
            self, debt, *, current_round=None,
            current_reaction_iteration=None):
        from data.progressive_cache_compaction import (
            blocks_sha256,
            source_fingerprint,
        )

        raw_blocks = self._read_jsonl_strict(
            self._lately_cache_jsonl(), label="lately_cache")
        blocks = self._current_lately_blocks()
        frozen = debt.get("frozen_prefix") or {}
        count = int(frozen.get("block_count") or 0)
        if not self._lately_prefix_matches(blocks, count, str(frozen.get("sha256") or "")):
            raise ReadError(
                self.cache_compaction_debt_path(),
                message="cache_compaction_lately_drift",
            )
        prefix = blocks[:count]
        normalized_tail = blocks[count:]
        raw_tail = raw_blocks[count:]
        by_id, groups = self._verify_progressive_compaction_source_map(
            debt, blocks, count)
        result_by_id = {
            str(item.get("shard_id") or ""): item
            for item in debt.get("results") or [] if isinstance(item, dict)
        }
        rewritten_groups = []
        now = local_now().isoformat()
        for group_index, group in enumerate(groups):
            source_blocks = [by_id[item] for item in group["source_block_ids"]]
            shard_ids = list(group.get("shard_ids") or [])
            group_results = [result_by_id.get(item) for item in shard_ids]
            completed = bool(shard_ids) and all(item is not None for item in group_results)
            if not completed or any(item.get("action") == "keep" for item in group_results):
                rewritten_groups.append({
                    "group_id": group.get("group_id"),
                    "blocks": source_blocks,
                    "deletable_summary": (
                        not group.get("protected")
                        and bool(source_blocks)
                        and all(str(item.get("kind") or "") == "interaction_summary"
                                for item in source_blocks)
                    ),
                })
                continue
            summaries = [
                str(item.get("semantic_content") or "").strip()
                for item in group_results if str(item.get("semantic_content") or "").strip()
            ]
            user_id = str(group.get("user_block_id") or "")
            if group.get("protected") and user_id:
                group_blocks = []
                user = by_id.get(user_id)
                if user is not None:
                    group_blocks.append(user)
                if summaries:
                    summary = self._build_progressive_cache_summary_block(
                        source_blocks,
                        "\n\n".join(summaries),
                        now,
                        group=group,
                        current_round=current_round,
                        current_reaction_iteration=current_reaction_iteration,
                        index=group_index,
                        compaction_id=debt.get("compaction_id"),
                    )
                    group_blocks.append(summary)
                rewritten_groups.append({
                    "group_id": group.get("group_id"),
                    "blocks": group_blocks,
                    "deletable_summary": False,
                })
            elif summaries:
                summary = self._build_progressive_cache_summary_block(
                    source_blocks,
                    "\n\n".join(summaries),
                    now,
                    group=group,
                    current_round=current_round,
                    current_reaction_iteration=current_reaction_iteration,
                    index=group_index,
                    compaction_id=debt.get("compaction_id"),
                )
                rewritten_groups.append({
                    "group_id": group.get("group_id"),
                    "blocks": [summary],
                    "deletable_summary": bool(user_id),
                })
            else:
                rewritten_groups.append({
                    "group_id": group.get("group_id"),
                    "blocks": [],
                    "deletable_summary": False,
                })
        rewritten = [
            block for item in rewritten_groups for block in item["blocks"]
        ]
        target = int(debt.get("target_chars") or 0)
        current_chars = self._lately_blocks_chars(rewritten)
        if current_chars > target:
            for item in rewritten_groups:
                if current_chars <= target:
                    break
                if not item["deletable_summary"]:
                    continue
                current_chars -= self._lately_blocks_chars(item["blocks"])
                item["blocks"] = []
                debt.setdefault("deleted_group_ids", []).append(item["group_id"])
            rewritten = [
                block for item in rewritten_groups for block in item["blocks"]
            ]
        terminal_phase = (
            "applied" if self._lately_blocks_chars(rewritten) <= target
            else "completed_with_protected_floor"
        )
        original_meta = self._load_active_corpus_meta()
        debt_path = self.cache_compaction_debt_path()
        lately_path = self._lately_cache_jsonl()
        meta_path = self.active_corpus_meta_path()
        with open(debt_path, "r", encoding="utf-8") as handle:
            original_debt_text = handle.read()
        with open(lately_path, "r", encoding="utf-8") as handle:
            original_lately_text = handle.read()
        original_meta_exists = os.path.isfile(meta_path)
        original_meta_text = ""
        if original_meta_exists:
            with open(meta_path, "r", encoding="utf-8") as handle:
                original_meta_text = handle.read()
        try:
            rewritten = self._assign_active_corpus_metadata_to_blocks(
                rewritten, persist_meta=False)
            assigned_ids = [
                self._normalize_active_corpus_id((item.get("ref") or {}).get(
                    "active_corpus_id")) for item in rewritten]
            assigned_numbers = [int(item[2:]) for item in assigned_ids if item]
            assigned_rounds = [self._sanitize_int(
                (item.get("ref") or {}).get("interaction_round_index"), 0)
                for item in rewritten]
            updated_meta = {
                "schema_version": self.ACTIVE_CORPUS_META_SCHEMA,
                "next_short_id": max(
                    int(original_meta["next_short_id"]),
                    max(assigned_numbers, default=0) + 1),
                "interaction_round_count": max(
                    int(original_meta["interaction_round_count"]),
                    max(assigned_rounds, default=0)),
            }
            next_blocks = rewritten + raw_tail
            expected_blocks = rewritten + normalized_tail
            debt["phase"] = "applying"
            debt["terminal_phase"] = terminal_phase
            debt["applied_prefix"] = {
                "block_count": len(rewritten),
                "chars": self._lately_blocks_chars(rewritten),
                "sha256": blocks_sha256(rewritten),
            }
            debt["active_corpus_meta_before"] = original_meta
            debt["active_corpus_meta_after"] = updated_meta
            debt["updated_at"] = now
            self._write_json_atomic(self.cache_compaction_debt_path(), debt)
            applying_record = self.load_cache_compaction_record()
            if (
                    applying_record.get("status") != "open"
                    or applying_record.get("phase") != "applying"
                    or applying_record.get("applied_prefix")
                    != debt["applied_prefix"]):
                raise WriteError(
                    self.cache_compaction_debt_path(),
                    message="cache_compaction_debt_verification_failed",
                )
            self._write_json_atomic(self.active_corpus_meta_path(), updated_meta)
            if self._load_active_corpus_meta() != updated_meta:
                raise WriteError(
                    self.active_corpus_meta_path(),
                    message="cache_compaction_meta_verification_failed",
                )
            self._write_jsonl_atomic(self._lately_cache_jsonl(), next_blocks)
            persisted_blocks = self._current_lately_blocks()
            if (
                    self._lately_blocks_sha256(persisted_blocks)
                    != self._lately_blocks_sha256(expected_blocks)
                    or not self._lately_prefix_matches(
                        persisted_blocks,
                        len(rewritten),
                        debt["applied_prefix"]["sha256"],
                    )):
                raise WriteError(
                    self._lately_cache_jsonl(),
                    message="cache_compaction_apply_verification_failed",
                )
            debt["status"] = "closed"
            debt["phase"] = terminal_phase
            debt["source_fingerprint"] = source_fingerprint(
                self._current_lately_blocks(),
                debt.get("policy") or {},
                debt.get("logical_window_tokens"),
            )
            debt["completed_at"] = local_now().isoformat()
            debt["updated_at"] = debt["completed_at"]
            self._write_json_atomic(self.cache_compaction_debt_path(), debt)
            terminal_record = self.load_cache_compaction_record()
            if (
                    terminal_record.get("status") != "closed"
                    or terminal_record.get("phase") != terminal_phase
                    or terminal_record.get("source_fingerprint")
                    != debt["source_fingerprint"]):
                raise WriteError(
                    self.cache_compaction_debt_path(),
                    message="cache_compaction_terminal_verification_failed",
                )
        except Exception:
            # Best-effort rollback uses the same atomic writers. If the crash
            # happens after lately commit, the applying record above lets READY
            # finish deterministically instead of guessing.
            try:
                atomic_write_text(lately_path, original_lately_text)
                if original_meta_exists:
                    atomic_write_text(meta_path, original_meta_text)
                elif os.path.isfile(meta_path):
                    os.remove(meta_path)
                atomic_write_text(debt_path, original_debt_text)
            except Exception:
                pass
            raise
        return terminal_phase

    def _progressive_compaction_target_met(self, debt):
        """Estimate the frozen prefix after fully completed oldest groups."""
        result_by_id = {
            str(item.get("shard_id") or ""): item
            for item in debt.get("results") or [] if isinstance(item, dict)
        }
        projected = int((debt.get("frozen_prefix") or {}).get("chars") or 0)
        for group in debt.get("groups") or []:
            shard_ids = list(group.get("shard_ids") or [])
            results = [result_by_id.get(item) for item in shard_ids]
            if not shard_ids or any(item is None for item in results):
                break
            if any(item.get("action") == "keep" for item in results):
                continue
            summary_chars = sum(
                len(str(item.get("semantic_content") or "").strip())
                for item in results
            )
            protected_user_chars = 0
            if group.get("protected") and group.get("user_block_id"):
                protected_user_chars = int(group.get("user_chars") or 0)
            projected -= int(group.get("source_chars") or 0)
            projected += protected_user_chars + summary_chars
        return projected <= int(debt.get("target_chars") or 0)

    def _current_lately_blocks(self):
        path = self._lately_cache_jsonl()
        blocks = []
        raw_log_excluded_kinds = self._raw_log_excluded_kinds()
        for item in self._read_jsonl_strict(path, label="lately_cache"):
            block = self._normalize_lately_block(
                item,
                raw_log_excluded_kinds=raw_log_excluded_kinds,
            )
            if block is None:
                raise ReadError(path, message="lately_cache_block_invalid")
            blocks.append(block)
        return blocks

    def _current_now_blocks(self):
        path = self._now_cache_jsonl()
        blocks = []
        for item in self._read_jsonl_strict(path, label="now_cache"):
            if self._corpus_block_to_entry(item) is None:
                raise ReadError(path, message="now_cache_block_invalid")
            blocks.append(dict(item))
        return blocks

    def _current_raw_log_blocks(self):
        path = self._raw_log_jsonl()
        blocks = []
        for item in self._read_jsonl_strict(path, label="raw_log"):
            block = self._normalize_corpus_block(item)
            if block is None:
                raise ReadError(path, message="raw_log_block_invalid")
            blocks.append(block)
        return blocks

    @classmethod
    def _lately_prefix_matches(cls, blocks, count, expected_sha):
        return (
            isinstance(count, int)
            and not isinstance(count, bool)
            and count >= 0
            and len(blocks) >= count
            and cls._lately_blocks_sha256(blocks[:count]) == str(expected_sha or "")
        )

    def _verify_progressive_compaction_source_map(self, debt, blocks, count):
        prefix = list(blocks[:count])
        prefix_ids = [str(item.get("id") or "") for item in prefix]
        by_id = {str(item.get("id") or ""): item for item in prefix}
        groups = debt.get("groups")
        flattened_source_ids = []
        if isinstance(groups, list):
            for group in groups:
                source_ids = (
                    group.get("source_block_ids")
                    if isinstance(group, dict) else None
                )
                if (
                        not isinstance(source_ids, list)
                        or any(
                            not isinstance(item, str) or not item
                            for item in source_ids
                        )):
                    break
                flattened_source_ids.extend(source_ids)
            else:
                if (
                        prefix_ids
                        and groups
                        and all(prefix_ids)
                        and len(by_id) == len(prefix_ids)
                        and flattened_source_ids == prefix_ids):
                    return by_id, groups
        raise ReadError(
            self.cache_compaction_debt_path(),
            message="cache_compaction_source_map_invalid",
        )

    def _verify_compaction_frozen_prefix(self, blocks):
        """Guard the v3 frozen prefix; legacy debt is read-only verified."""
        debt = self.load_cache_compaction_debt()
        if not debt:
            return True
        if debt.get("schema_version") == self.PROGRESSIVE_CACHE_COMPACTION_DEBT_SCHEMA:
            frozen = debt.get("frozen_prefix") if isinstance(
                debt.get("frozen_prefix"), dict) else {}
            if not self._lately_prefix_matches(
                    blocks,
                    int(frozen.get("block_count") or 0),
                    str(frozen.get("sha256") or "")):
                raise ReadError(
                    self.cache_compaction_debt_path(),
                    message="cache_compaction_lately_prefix_drift",
                )
            return True
        self.recover_cache_compaction_debt()
        return True

    @staticmethod
    def _text_sha256(value):
        return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()

    @classmethod
    def _lately_blocks_sha256(cls, blocks):
        payload = json.dumps(
            list(blocks or []), ensure_ascii=False, sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _lately_blocks_chars(blocks):
        return sum(len(str(block.get("text") or "")) for block in blocks or [])

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

    def _sync_caches(self, entries, now_entries):
        _, now_entries = self._assign_active_corpus_metadata(
            entries, now_entries)
        call_transients = [
            entry for entry in self._all_now_entries()
            if self._is_call_transient(entry)
        ]
        self._write_now_cache(
            self._merge_now_entries(now_entries, call_transients))
        self._last_cache_stats = self._empty_cache_stats()

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
            # 旧记录的 iter 是 0 基生产序号；只允许下一次 1 基 reaction 消费一次。
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
        self._migrate_active_corpus_metadata()
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
            "lately_soft_overflow": False,
            "lately_soft_overflow_chars": 0,
        }

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

    def _policy_by_kind(self):
        lanes = self._persistent_lanes()
        policies = {
            kind: {"now": True, "lately": True}
            for kind in lanes["now_lately_raw"] + lanes["now_lately_no_raw"]
        }
        policies["cache_summary"] = {"now": False, "lately": True}
        policies["interaction_summary"] = {"now": False, "lately": True}
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
        timestamp = normalized.get("timestamp") or local_now().isoformat()
        policy = self._policy_for_entry(normalized, kind, cache_policy)
        interaction_ref = {
            "object": normalized.get("interaction_object", "unknown"),
            "identity_status": normalized.get("identity_status", "unknown"),
            "interaction_source": normalized.get("interaction_source", "unresolved"),
        }
        if normalized.get("interaction_object_id"):
            interaction_ref["object_id"] = normalized["interaction_object_id"]
        ref = {"interaction": interaction_ref}
        active_corpus_id = self._normalize_active_corpus_id(
            normalized.get("active_corpus_id"))
        if active_corpus_id:
            ref["active_corpus_id"] = active_corpus_id
        if self._sanitize_int(normalized.get("interaction_round_index"), 0) > 0:
            ref["interaction_round_index"] = self._sanitize_int(
                normalized.get("interaction_round_index"), 0)
        if normalized.get("raw_log_key"):
            ref["raw_log_key"] = normalized.get("raw_log_key")
        if normalized.get("source_block_id"):
            ref["source_block_id"] = normalized.get("source_block_id")
        for key in (
            "source_block_ids",
            "source_group_id",
            "source_sha256",
            "source_round_start",
            "source_round_end",
            "raw_log_keys",
            "oldest_source_round",
            "oldest_cached_at",
            "source_block_count",
            "compacted_at",
            "compact_reason",
            "compaction_id",
            "compaction_shard_id",
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
            "interaction_summary",
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
            "source_group_id": ref.get("source_group_id"),
            "source_sha256": ref.get("source_sha256"),
            "source_round_start": ref.get("source_round_start"),
            "source_round_end": ref.get("source_round_end"),
            "raw_log_keys": ref.get("raw_log_keys"),
            "oldest_source_round": ref.get("oldest_source_round"),
            "oldest_cached_at": ref.get("oldest_cached_at"),
            "source_block_count": ref.get("source_block_count"),
            "compacted_at": ref.get("compacted_at"),
            "compact_reason": ref.get("compact_reason"),
            "compaction_id": ref.get("compaction_id"),
            "compaction_shard_id": ref.get("compaction_shard_id"),
            "transient_scope": ref.get("transient_scope"),
            "transient_target_step": ref.get("transient_target_step"),
            "transient_target_iteration": ref.get("transient_target_iteration"),
            "tool_result": context_safe_read_tool_result(ref.get("tool_result")),
            "protocol_receipt": ref.get("protocol_receipt"),
            "protocol_receipts": ref.get("protocol_receipts"),
            "native_replay": ref.get("native_replay"),
            "active_corpus_id": ref.get("active_corpus_id"),
            "interaction_round_index": ref.get("interaction_round_index"),
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

    @staticmethod
    def _read_jsonl_strict(path, *, label="lately_cache"):
        if not path or not os.path.isfile(path):
            return []
        items = []
        try:
            with open(path, "r", encoding="utf-8") as f:
                for line_number, line in enumerate(f, 1):
                    if not line.strip():
                        continue
                    try:
                        item = json.loads(line)
                    except (json.JSONDecodeError, UnicodeError) as exc:
                        raise ReadError(
                            path,
                            message=f"{label}_json_invalid:{line_number}",
                            cause=exc,
                        ) from exc
                    if not isinstance(item, dict):
                        raise ReadError(
                            path,
                            message=f"{label}_entry_not_object:{line_number}",
                        )
                    items.append(item)
        except ReadError:
            raise
        except OSError as exc:
            raise ReadError(path, cause=exc) from exc
        return items

    @staticmethod
    def _snapshot_text_files(paths):
        snapshots = {}
        for path in dict.fromkeys(paths):
            if os.path.isfile(path):
                try:
                    with open(
                            path, "r", encoding="utf-8", newline="") as handle:
                        snapshots[path] = handle.read()
                except (OSError, UnicodeError) as exc:
                    raise ReadError(path, cause=exc) from exc
            else:
                snapshots[path] = None
        return snapshots

    @staticmethod
    def _restore_text_files(snapshots):
        failures = []
        for path, payload in snapshots.items():
            try:
                if payload is None:
                    if os.path.isfile(path):
                        os.remove(path)
                else:
                    atomic_write_text(path, payload, newline="")
            except Exception as exc:
                failures.append(f"{path}:{type(exc).__name__}:{exc}")
        if failures:
            raise WriteError(
                next(iter(snapshots), "context_cache"),
                message="current_cache_transition_rollback_failed:" + "|".join(failures),
            )

    def _cache_dir(self):
        if self._cache_dir_override:
            return self._cache_dir_override
        return STM_CONTEXT_CACHE_DIR

    def active_corpus_meta_path(self):
        return os.path.join(self._cache_dir(), "active_corpus_meta.json")

    def _load_active_corpus_meta(self):
        path = self.active_corpus_meta_path()
        if not os.path.isfile(path):
            return {
                "schema_version": self.ACTIVE_CORPUS_META_SCHEMA,
                "next_short_id": 1,
                "interaction_round_count": 0,
            }
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not isinstance(data, dict):
                raise ValueError("active_corpus_meta_not_object")
            if data.get("schema_version") != self.ACTIVE_CORPUS_META_SCHEMA:
                raise ValueError("active_corpus_meta_schema_invalid")
            next_short_id = int(data.get("next_short_id"))
            interaction_round_count = int(data.get("interaction_round_count"))
            if next_short_id < 1 or interaction_round_count < 0:
                raise ValueError("active_corpus_meta_counter_invalid")
        except (OSError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise ReadError(path, cause=exc)
        return {
            "schema_version": self.ACTIVE_CORPUS_META_SCHEMA,
            "next_short_id": next_short_id,
            "interaction_round_count": interaction_round_count,
        }

    @staticmethod
    def _normalize_active_corpus_id(value):
        text = str(value or "").strip().upper()
        return text if re.fullmatch(r"C-[0-9]{5}", text) else ""

    @classmethod
    def _entry_supports_active_corpus_id(cls, entry):
        if not isinstance(entry, dict) or not str(entry.get("content") or "").strip():
            return False
        if cls._is_call_transient(entry):
            return False
        kind = str(entry.get("kind") or "").strip()
        return kind not in {"reasoning_context", "runtime_call_request"}

    @staticmethod
    def _interaction_batch_key(entry, fallback_index):
        round_num = ContextStore._entry_round(entry)
        if round_num is not None:
            return ("round", round_num)
        return ("entry", fallback_index, ContextStore._entry_key(entry))

    def _assign_active_corpus_metadata(
            self, lately_entries, now_entries, *, persist_meta=True):
        meta = self._load_active_corpus_meta()
        groups = [
            [self._normalize_entry(entry) for entry in entries or []]
            for entries in (lately_entries, now_entries)
        ]
        all_entries = groups[0] + groups[1]
        existing_ids = [
            self._normalize_active_corpus_id(entry.get("active_corpus_id"))
            for entry in all_entries
        ]
        existing_numbers = [int(value[2:]) for value in existing_ids if value]
        next_short_id = max(
            meta["next_short_id"],
            max(existing_numbers, default=0) + 1,
        )
        interaction_round_count = max(
            meta["interaction_round_count"],
            max((
                self._sanitize_int(entry.get("interaction_round_index"), 0)
                for entry in all_entries
            ), default=0),
        )
        batch_indexes = {}
        for index, entry in enumerate(all_entries):
            if entry.get("role") == "user" and entry.get("kind") == "interaction":
                interaction_index = self._sanitize_int(
                    entry.get("interaction_round_index"), 0)
                if interaction_index > 0:
                    batch_indexes[self._interaction_batch_key(entry, index)] = interaction_index

        changed = False
        for index, entry in enumerate(all_entries):
            if self._entry_supports_active_corpus_id(entry):
                active_id = self._normalize_active_corpus_id(
                    entry.get("active_corpus_id"))
                if not active_id:
                    active_id = f"C-{next_short_id:05d}"
                    next_short_id += 1
                    entry["active_corpus_id"] = active_id
                    changed = True
            if entry.get("role") != "user" or entry.get("kind") != "interaction":
                continue
            interaction_index = self._sanitize_int(
                entry.get("interaction_round_index"), 0)
            if interaction_index <= 0:
                batch_key = self._interaction_batch_key(entry, index)
                interaction_index = batch_indexes.get(batch_key, 0)
                if interaction_index <= 0:
                    interaction_round_count += 1
                    interaction_index = interaction_round_count
                    batch_indexes[batch_key] = interaction_index
                entry["interaction_round_index"] = interaction_index
                changed = True

        updated_meta = {
            "schema_version": self.ACTIVE_CORPUS_META_SCHEMA,
            "next_short_id": next_short_id,
            "interaction_round_count": interaction_round_count,
        }
        if persist_meta and (
                changed or updated_meta != meta or not os.path.isfile(
                    self.active_corpus_meta_path())):
            self._write_json_atomic(self.active_corpus_meta_path(), updated_meta)
        split = len(groups[0])
        return all_entries[:split], all_entries[split:]

    def _migrate_active_corpus_metadata(self):
        if self._active_corpus_migrated:
            return
        if not (
                os.path.isfile(self._now_cache_jsonl())
                or os.path.isfile(self._lately_cache_jsonl())):
            self._active_corpus_migrated = True
            return
        lately_blocks = self._read_jsonl(self._lately_cache_jsonl())
        now_blocks = self._read_jsonl(self._now_cache_jsonl())
        needs_cache_write = False
        for block in lately_blocks + now_blocks:
            entry = self._corpus_block_to_entry(block)
            if not entry:
                continue
            ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
            if (
                    self._entry_supports_active_corpus_id(entry)
                    and not self._normalize_active_corpus_id(
                        ref.get("active_corpus_id"))):
                needs_cache_write = True
            if (
                    entry.get("role") == "user"
                    and entry.get("kind") == "interaction"
                    and self._sanitize_int(
                        ref.get("interaction_round_index"), 0) <= 0):
                needs_cache_write = True
        lately = [
            entry for entry in (
                self._corpus_block_to_entry(block)
                for block in lately_blocks
            ) if entry
        ]
        now = [
            entry for entry in (
                self._corpus_block_to_entry(block)
                for block in now_blocks
            ) if entry
        ]
        lately, now = self._assign_active_corpus_metadata(
            lately, now, persist_meta=False)
        if needs_cache_write:
            self._write_lately_cache(lately)
            self._write_now_cache(now)
        self._assign_active_corpus_metadata(lately, now)
        self._active_corpus_migrated = True

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
                return os.path.join(
                    os.path.dirname(self._raw_log_md_override),
                    "raw_log.jsonl",
                )
            if RAW_LOG != _DEFAULT_RAW_LOG:
                return os.path.join(os.path.dirname(RAW_LOG), "raw_log.jsonl")
        return RAW_LOG_JSONL

    def _raw_log_md(self):
        if self._raw_log_md_override:
            return self._raw_log_md_override
        return RAW_LOG

    def _corpus_rhythms_dir(self):
        return (
            self._corpus_rhythms_dir_override
            or os.path.join(CONTAINER_CORPUS_DIR, "public", "rhythms")
        )

    def _raw_log_key(self, block):
        ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
        if ref.get("raw_log_key"):
            return str(ref["raw_log_key"])
        return self._legacy_raw_log_key(block)

    @staticmethod
    def _legacy_raw_log_key(block):
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

    @staticmethod
    def _disambiguated_raw_log_key(legacy_key, source_block_id):
        payload = {
            "legacy_raw_log_key": str(legacy_key),
            "source_block_id": str(source_block_id),
        }
        raw = json.dumps(
            payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def _repair_incoming_legacy_raw_key_collisions(
            self, existing, promoted_blocks):
        """Disambiguate only proven legacy semantic-key collisions."""
        from data.chronicle_store import dedupe_corpus_records

        dedupe_corpus_records(existing)
        by_key = {
            str((record.get("ref") or {}).get("raw_log_key") or ""): record
            for record in existing
        }
        incoming = []
        for promoted in promoted_blocks:
            normalized = self._normalize_corpus_block(promoted)
            if normalized is None:
                continue
            ref = normalized.get("ref") or {}
            key = str(ref.get("raw_log_key") or "")
            prior = by_key.get(key)
            if prior is not None:
                try:
                    dedupe_corpus_records([prior, normalized])
                except ValueError:
                    prior_ref = prior.get("ref") or {}
                    prior_source = str(prior_ref.get("source_block_id") or "")
                    source = str(ref.get("source_block_id") or "")
                    if (
                            not prior_source
                            or not source
                            or prior_source == source
                            or key != self._legacy_raw_log_key(prior)
                            or key != self._legacy_raw_log_key(normalized)):
                        raise
                    key = self._disambiguated_raw_log_key(key, source)
                    promoted.setdefault("ref", {})["raw_log_key"] = key
                    normalized["ref"]["raw_log_key"] = key
                    rekeyed_prior = by_key.get(key)
                    if rekeyed_prior is not None:
                        dedupe_corpus_records([rekeyed_prior, normalized])
            by_key[key] = normalized
            incoming.append(normalized)
        return incoming

    def _normalize_corpus_block(self, block):
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
        clone["ref"].pop("active_corpus_id", None)
        clone["ref"].pop("interaction_round_index", None)
        clone["ref"]["raw_log_key"] = self._raw_log_key(clone)
        return clone

    @staticmethod
    def _is_compacted_summary(block):
        if not isinstance(block, dict):
            return False
        ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
        return (
            block.get("kind") in {"cache_summary", "interaction_summary"}
            or ref.get("compact_reason") in {
                "post_lately_trim",
                "progressive_lately_pressure",
            }
        )

    def _normalize_lately_block(
            self, block, *, raw_log_excluded_kinds=None):
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
        excluded_kinds = (
            set(raw_log_excluded_kinds)
            if raw_log_excluded_kinds is not None
            else self._raw_log_excluded_kinds()
        )
        if (
            not self._is_compacted_summary(clone)
            and clone.get("kind") not in excluded_kinds
        ):
            clone["ref"]["raw_log_key"] = self._raw_log_key(clone)
        elif clone.get("kind") in excluded_kinds:
            clone["ref"].pop("raw_log_key", None)
        return clone

    def _mirror_lately_blocks_to_raw_log(self, blocks):
        """raw_log 只镜像刚被 lately 接纳的语料块。"""
        from data.chronicle_store import dedupe_corpus_records

        existing = [
            block for block in (
                self._normalize_corpus_block(item)
                for item in self._read_jsonl(self._raw_log_jsonl())
            )
            if block
        ]
        incoming = [
            block for block in (
                self._normalize_corpus_block(item)
                for item in blocks or []
                if (
                    not self._is_compacted_summary(item)
                    and item.get("kind") not in self._raw_log_excluded_kinds()
                )
            )
            if block
        ]
        merged = dedupe_corpus_records(existing + incoming)
        if merged == existing and os.path.isfile(self._raw_log_jsonl()):
            return
        self._write_jsonl_atomic(self._raw_log_jsonl(), merged)
        atomic_write_text(self._raw_log_md(), self._render_raw_log_md(merged))

    def read_raw_log(self):
        path = self._raw_log_md()
        if not os.path.isfile(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return handle.read()
        except OSError:
            return ""

    def archive_raw_log(self):
        """主轴节律轮把当前 raw_log 原子归档为一份节级 Corpus。"""
        from data.chronicle_store import dedupe_corpus_records

        records = dedupe_corpus_records([
            block for block in (
                self._normalize_corpus_block(item)
                for item in self._read_jsonl(self._raw_log_jsonl())
            )
            if block
        ])
        if not records:
            return None

        rounds = []
        for record in records:
            loc = record.get("loc") if isinstance(record.get("loc"), dict) else {}
            try:
                rounds.append(int(loc.get("round")))
            except (TypeError, ValueError):
                raise ValueError("raw_log record missing loc.round")
        first_round, last_round = min(rounds), max(rounds)
        stem = (
            f"rhythm_{local_now().strftime('%Y-%m-%d')}_"
            f"R{first_round:06d}-R{last_round:06d}"
        )
        jsonl_path = os.path.join(self._corpus_rhythms_dir(), stem + ".jsonl")
        existing = [
            block for block in (
                self._normalize_corpus_block(item)
                for item in self._read_jsonl(jsonl_path)
            )
            if block
        ]
        merged = dedupe_corpus_records(existing + records)
        self._write_jsonl_atomic(jsonl_path, merged)
        atomic_write_text(
            os.path.splitext(jsonl_path)[0] + ".md",
            self._render_corpus_md(merged),
        )
        self.clear_raw_log()
        return jsonl_path

    def clear_raw_log(self):
        atomic_write_text(self._raw_log_jsonl(), "")
        atomic_write_text(self._raw_log_md(), "<!-- 原始语料备份 -->\n")

    def _assign_active_corpus_metadata_to_blocks(self, blocks, *, persist_meta=True):
        entries = [self._corpus_block_to_entry(block) for block in blocks]
        assigned, _ = self._assign_active_corpus_metadata(
            [entry for entry in entries if entry], [], persist_meta=persist_meta)
        assigned_iter = iter(assigned)
        result = []
        for block, entry in zip(blocks, entries):
            copied = dict(block)
            if entry:
                assigned_entry = next(assigned_iter)
                ref = dict(copied.get("ref") or {})
                active_id = self._normalize_active_corpus_id(
                    assigned_entry.get("active_corpus_id"))
                if active_id:
                    ref["active_corpus_id"] = active_id
                interaction_index = self._sanitize_int(
                    assigned_entry.get("interaction_round_index"), 0)
                if interaction_index > 0:
                    ref["interaction_round_index"] = interaction_index
                copied["ref"] = ref
            result.append(copied)
        return result

    def _build_progressive_cache_summary_block(
            self, source_blocks, text, compacted_at, *, group,
            current_round=None, current_reaction_iteration=None,
            index=0, compaction_id=None):
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
            nested_start = ref.get("source_round_start")
            nested_end = ref.get("source_round_end")
            for value in (
                    nested_start,
                    nested_end,
                    loc.get("round") if nested_start is None and nested_end is None else None):
                try:
                    source_round_values.append(int(value))
                except (TypeError, ValueError):
                    pass
            if loc:
                source_locs.append(loc)
        try:
            round_num = int(current_round)
        except (TypeError, ValueError):
            round_num = 0
        try:
            reaction_iteration = int(current_reaction_iteration)
        except (TypeError, ValueError):
            reaction_iteration = 0
        oldest_loc = source_locs[0] if source_locs else {}
        compaction_id = str(compaction_id or "").strip()
        group_id = str(group.get("group_id") or "").strip()
        interaction_index = self._sanitize_int(
            group.get("interaction_round_index"), 0)
        kind = (
            "interaction_summary"
            if (
                self._sanitize_int(group.get("interaction_round_index"), 0) > 0
                or any(
                    str(item.get("kind") or "") == "interaction_summary"
                    for item in source_blocks
                )
            )
            else "cache_summary"
        )
        summary_id = (
            f"{compaction_id}:{group_id}"
            if compaction_id and group_id
            else f"CMP-R{round_num:06d}-{index:04d}"
        )
        source_round_start = min(source_round_values, default=0)
        source_round_end = max(source_round_values, default=0)
        ref = {
            "source_group_id": group_id,
            "source_block_ids": source_ids,
            "source_sha256": self._text_sha256(json.dumps(
                [str(block.get("text") or "") for block in source_blocks],
                ensure_ascii=False,
                separators=(",", ":"),
            )),
            "raw_log_keys": list(dict.fromkeys(raw_log_keys)),
            "oldest_source_round": source_round_start or None,
            "oldest_cached_at": oldest_loc.get("time", ""),
            "source_block_count": len(source_blocks or []),
            "compacted_at": compacted_at,
            "compact_reason": "progressive_lately_pressure",
            **({"source_round_start": source_round_start} if source_round_start else {}),
            **({"source_round_end": source_round_end} if source_round_end else {}),
            **({"compaction_id": compaction_id} if compaction_id else {}),
            **({"interaction_round_index": interaction_index} if interaction_index else {}),
        }
        return {
            "id": summary_id,
            "role": "system",
            "kind": kind,
            "text": text,
            "loc": {
                "round": round_num,
                "step": "reaction",
                "iter": reaction_iteration,
                "time": compacted_at,
            },
            "policy": {"now": False, "lately": True},
            "ref": ref,
        }

    @staticmethod
    def _render_raw_log_md(blocks):
        return ContextStore._render_corpus_md(blocks, "原始语料备份")

    @staticmethod
    def _render_corpus_md(blocks, heading="Corpus 节归档"):
        if not blocks:
            return f"<!-- {heading} -->\n"
        parts = [f"<!-- {heading}；机器真源为同名 JSONL -->"]
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
