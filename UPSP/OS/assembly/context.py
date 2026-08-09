"""
上下文主装配器 — 按七层频率梯度拼装 messages，并渲染审计文本
DDS §19 上下文工程 + §19.7 灵活加载策略

频率层（纵向，DDS §19.3 七层）：
  permanent → periodic → lately → high_freq → now → statusbar
  POPUP 走 messages 绝对末位（独立层）

五模块（横向分类，DDS §19.1）：
  STATUSBAR / EXPLORER / CONTENT / RULES / POPUP

RULES 加载：读 rules_registry.json → 按层/步/场景选文件 → 读 .md 拼接
三步装配差异：
  setup:    CONTENT 为空 + setup.md + schema §21 输出模板
  reaction: CONTENT 已填充 + reaction.md + schema §23 反应步最终输出表
  cleanup:  压缩版 + cleanup.md + schema §20 输出模板
"""
import os
import json
import re
from paths import (
    CORE_MD, DREAMS_MD,
    RULES_REGISTRY, RULES_DIR,
    DOCS_SCHEMA,
)
from constants import STM_INDEX_DISPLAY_LIMIT, DREAMS_DISPLAY_LIMIT

from assembly.statusbar import StatusBarBuilder
from assembly.popup import PopupManager
from assembly.context_helpers import (
    active_corpus_ids_from_messages,
    build_general_tool_guide,
    build_native_tool_feedback_popup,
    build_protocol_tool_guide,
    build_current_runtime_guide_popup,
    build_static_memory_reminder_popup,
    current_round_from_state,
    dedupe_layer_entries,
    fold_marker,
    format_round_id,
    format_step_guide_popup,
    load_general_tool_index,
    load_protocol_tool_index,
    join_layer_blocks,
    messages_text,
    normalize_layer_entries,
    render_corpus_entries_for_context,
    slice_entries,
)
from assembly.context_indexes import (
    build_association_index as context_build_association_index,
    build_container_index as context_build_container_index,
    build_keyword_index as context_build_keyword_index,
    build_ltm_heat_index as context_build_ltm_heat_index,
    build_stm_heat_index as context_build_stm_heat_index,
    derive_input_keywords as context_derive_input_keywords,
    get_keywords_for_mem_id as context_get_keywords_for_mem_id,
)
from assembly.context_mounts import (
    build_mounted_content as context_build_mounted_content,
    build_mounted_content_blocks as context_build_mounted_content_blocks,
    load_container_content as context_load_container_content,
    load_memory_content as context_load_memory_content,
    memory_mount_meta as context_memory_mount_meta,
    load_relation_content as context_load_relation_content,
    load_skill_content as context_load_skill_content,
)
from assembly.context_periodic import (
    build_periodic_with_block_index as context_build_periodic_with_block_index,
)
from data.relation_store import relation_card_label, relation_public_name
from data.audit_store import AuditStore
from errors import RequiredContextError
from logic.popup_policy import PopupPolicy
from logic.context_profile import normalize_context_profile
from logic.relay_target import render_target_popup
from logic.runtime_channels import channel_for_step
from logic.task_board import render_active_task_board


EXPIRED_CONTEXT_LAYERS = {"permanent", "periodic"}


class ContextAssembler:
    """
    上下文主装配器

    每步重新装配 → O(1) 恒定上下文
    频率梯度: 永固 → 定期 → 最近缓存 lately → 高频
    → 当前缓存 now → STATUSBAR → 弹窗层(POPUP)

    v0.9.0: RULES 面板改为 rules_registry.json 驱动加载
    """

    VISIBLE_CONTEXT_KINDS = {
        "interaction",
        "assistant_reply",
        "dialogue_progress",
        "material",
        "reasoning_context",
        "tool_fact",
        "setup_fact",
        "relay_handoff",
        "runtime_call_request",
        "runtime_retry_notice",
        "organ_signal",
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
    CALL_ONLY_KINDS = set()

    @classmethod
    def _is_call_only_entry(cls, entry):
        if not isinstance(entry, dict):
            return False
        return str(entry.get("kind") or "").strip() in cls.CALL_ONLY_KINDS

    @staticmethod
    def _runtime_call_request_entry():
        return {
            "role": "user",
            "kind": "runtime_call_request",
            "content": "请根据上下文继续本次调用。",
            "interaction_object": "runtime",
            "identity_status": "system",
            "interaction_source": "runtime_call_request",
        }

    @classmethod
    def _is_context_visible_entry(cls, entry):
        if not isinstance(entry, dict):
            return False
        kind = str(entry.get("kind") or "").strip()
        if not kind:
            role = str(entry.get("role") or "").strip()
            kind = "assistant_reply" if role == "assistant" else "interaction"
        if kind in cls.RETIRED_CONTEXT_KINDS:
            return False
        return kind in cls.VISIBLE_CONTEXT_KINDS

    @staticmethod
    def _is_allowed_material_input_entry(entry):
        if not isinstance(entry, dict):
            return False
        kind = str(entry.get("kind") or "").strip()
        source = str(entry.get("interaction_source") or entry.get("source") or "").strip()
        content = str(entry.get("content") or "").strip()
        if kind == "organ_signal":
            return (
                entry.get("role") != "system"
                and bool(str(entry.get("source_role") or "").strip())
                and bool(str(entry.get("caused_by") or "").strip())
                and bool(content)
            )
        return (
            kind == "runtime_retry_notice"
            and source == "runtime_retry_notice"
            and 0 < len(content) <= 1200
        )

    def __init__(self, state_store=None, context_dir=None, config_store=None,
                 context_store=None, context_profile="full"):
        self.state_store = state_store
        self.config_store = config_store
        self.context_store = context_store
        self.statusbar = StatusBarBuilder()
        self._last_statusbar_projection = {}
        self.popup = PopupManager(state_store=state_store)
        self.popup_policy = PopupPolicy()
        self.context_profile = normalize_context_profile(context_profile)
        self._registry = None       # rules_registry.json 缓存
        self._rules_cache = {}      # 文件内容缓存 {path: content}
        self._layer_cache = {}      # 频率层缓存 {(step, layer): text} DDS §21
        self._layer_block_cache = {}
        self._current_layer_block_index = {}
        self._current_input_text = None
        self._current_interaction_meta = None
        self._hidden_stm_memory_ids = set()
        self._active_corpus_registry = {}
        self._pending_corpus_expand_once_keys = set()
        # 上下文输出目录（可注入，测试用 tmp_path，生产用 paths.STM_CONTEXT_DIR）
        if context_dir is None:
            from paths import STM_CONTEXT_DIR
            self._context_dir = STM_CONTEXT_DIR
            self.audit = AuditStore()
        else:
            self._context_dir = context_dir
            self.audit = AuditStore(
                setup_dir=os.path.join(context_dir, "setup"),
                reaction_dir=os.path.join(context_dir, "reaction"),
                cleanup_dir=os.path.join(context_dir, "cleanup"),
            )

    def _confirmed_memory_subjects(self, relation_store=None):
        state = {}
        if self.state_store is not None:
            try:
                state = self.state_store.load()
            except Exception:
                state = {}
        try:
            from logic.memory_privacy import confirmed_subjects_from_state
            return confirmed_subjects_from_state(
                state,
                interaction_meta=self._current_interaction_meta,
                relation_store=relation_store,
            )
        except Exception:
            subject = None
            if isinstance(self._current_interaction_meta, dict):
                subject = self._current_interaction_meta.get("interaction_object")
            return [subject] if subject and subject != "unknown" else []

    def _memory_meta_visible(self, meta):
        try:
            from logic.memory_privacy import can_see_memory
            from data.memory_store import MemoryStore
            from data.relation_store import RelationStore
            mem_id = str((meta or {}).get("id") or "").strip()
            relation_store = RelationStore()
            owners = MemoryStore().private_subjects_for_memory(mem_id)
            return can_see_memory(
                meta,
                self._confirmed_memory_subjects(relation_store),
                relation_store,
                privacy_subjects=owners,
            )
        except Exception:
            return str((meta or {}).get("access") or "public").lower() != "private"

    def _memory_visible_in_stm_projection(self, mem_id, meta):
        if str(mem_id or "").strip() in getattr(self, "_hidden_stm_memory_ids", set()):
            return False
        return self._memory_meta_visible(meta)

    # ==============================================================
    # 三步装配入口
    # ==============================================================

    def assemble_setup(self, state, round_type, user_messages=None,
                       material_inputs=None, internal_handoff=None,
                       interaction_meta=None):
        self._current_interaction_meta = (
            dict(interaction_meta) if isinstance(interaction_meta, dict) else None
        )
        if not user_messages:
            self._current_input_text = None
        system, full_messages = self._build_full_context(
            step="setup", round_type=round_type, state=state,
            include_content=False, mount_ids=None,
            user_messages=user_messages,
            material_inputs=material_inputs,
            internal_handoff=internal_handoff)
        return system, full_messages

    def assemble_reaction(self, state, round_type, mount_ids=None,
                          material_inputs=None, internal_handoff=None,
                          protocol_tool_guides=None,
                          general_tool_guides=None,
                          reaction_loop_phase="loop",
                          native_tool_feedbacks=None,
                          hidden_stm_memory_ids=None,
                          runtime_focus_entries=None,
                          current_reaction_iteration=None):
        previous_hidden = set(getattr(self, "_hidden_stm_memory_ids", set()))
        self._hidden_stm_memory_ids = set(hidden_stm_memory_ids or [])
        try:
            system, full_messages = self._build_full_context(
                step="reaction", round_type=round_type, state=state,
                include_content=True, mount_ids=mount_ids,
                material_inputs=material_inputs,
                internal_handoff=internal_handoff,
                protocol_tool_guides=protocol_tool_guides,
                general_tool_guides=general_tool_guides,
                reaction_loop_phase=reaction_loop_phase,
                native_tool_feedbacks=native_tool_feedbacks,
                runtime_focus_entries=runtime_focus_entries,
                current_reaction_iteration=current_reaction_iteration)
            return system, full_messages
        finally:
            self._hidden_stm_memory_ids = previous_hidden

    def assemble_cleanup(self, state, round_type, result,
                         material_inputs=None, internal_handoff=None,
                         popup_fragments=None):
        """善后步装配：前序频率层 + now 临时材料 + 真实交接 + POPUP。"""
        handoff_entries = normalize_layer_entries(internal_handoff)
        return self._build_full_context(
            step="cleanup",
            round_type=round_type,
            state=state,
            include_content=False,
            mount_ids=None,
            material_inputs=material_inputs,
            internal_handoff=handoff_entries,
            popup_fragments=popup_fragments,
        )

    # ==============================================================
    # messages 构造（频率梯度）
    # ==============================================================

    def _build_full_context(self, step, round_type, state, include_content,
                            mount_ids, user_messages=None,
                            material_inputs=None, internal_handoff=None,
                            protocol_tool_guides=None,
                            general_tool_guides=None,
                            reaction_loop_phase="loop",
                            native_tool_feedbacks=None,
                            popup_fragments=None,
                            runtime_focus_entries=None,
                            current_reaction_iteration=None):
        """构建完整 messages 数组——每层一条 system 消息标注频率层，语料保持 user/assistant 格式"""
        context_step = step
        cc = state.get("base", {}).get("context_cache", {})
        current_input_text = "\n".join(user_messages) if user_messages else None
        if current_input_text:
            self._current_input_text = current_input_text
        visible_input_text = current_input_text or self._current_input_text
        visible_interaction_meta = self._current_interaction_meta
        self._current_layer_block_index = {}
        permanent = self._cached_or_build(
            context_step, "permanent", cc.get("permanent_expired", True),
            lambda: self._build_permanent(state, context_step, round_type))
        periodic = self._cached_or_build(
            context_step, "periodic", cc.get("periodic_expired", True),
            lambda: self._build_periodic(state, context_step, round_type))

        current_round = current_round_from_state(state)

        # 构建 messages 数组
        self._active_corpus_registry = {}
        expand_once_entry_keys = set()
        if step == "reaction":
            expand_once_entry_keys = set(self._pending_corpus_expand_once_keys)
            self._pending_corpus_expand_once_keys.clear()
        messages = []
        # 1. 永固层
        if permanent:
            messages.append({"role": "system", "content": f"<!-- 永固层 -->\n{permanent}"})
        # 2. 定期层
        if periodic:
            messages.append({"role": "system", "content": f"<!-- 定期层 -->\n{periodic}"})

        active_corpus_index = 1

        # 3. 最近缓存 lately（按步取窗口）
        lately_entries = self._get_lately_entries(step)
        lately_section = self._build_cache_section(
            "最近缓存 lately",
            "lately_cache.jsonl",
            lately_entries,
            current_round=current_round,
            active_corpus_start=active_corpus_index,
            current_reaction_iteration=current_reaction_iteration,
            expand_once_entry_keys=expand_once_entry_keys,
            active_corpus_registry=self._active_corpus_registry)
        messages.extend(lately_section)
        active_corpus_index += len(active_corpus_ids_from_messages(lately_section))

        # 4. 高频层
        high_freq = self._build_high_freq(
            state, context_step, round_type, include_content, mount_ids, None,
            visible_input_text,
            visible_interaction_meta,
            runtime_focus_entries)
        if high_freq:
            messages.append({"role": "system", "content": f"<!-- 高频层 -->\n{high_freq}"})

        # 5. 当前缓存 now：除 runtime_call_request 固定占位外，正式语料块
        # 全部按 now_cache.jsonl 写入顺序履带式装配。
        material_entries = normalize_layer_entries(material_inputs)
        for entry in material_entries:
            entry.setdefault("kind", "material")
        material_entries = [
            entry for entry in material_entries
            if self._is_context_visible_entry(entry)
            and self._is_allowed_material_input_entry(entry)
        ]
        transient_target_step = (
            "final_reply"
            if step == "reaction" and reaction_loop_phase == "final_reply"
            else step
        )
        call_only_entries = self._get_call_transient_entries(
            current_round,
            transient_target_step,
            current_reaction_iteration=current_reaction_iteration,
        )

        stored_now_entries = self._get_now_entries()
        stored_now_entries = [
            entry for entry in stored_now_entries
            if self._is_context_visible_entry(entry)
        ]
        current_interaction_entries = [
            entry for entry in stored_now_entries
            if str(entry.get("kind") or "").strip() == "interaction"
        ]
        popup_interaction_meta = (
            visible_interaction_meta
            if isinstance(visible_interaction_meta, dict)
            else current_interaction_entries[-1]
            if current_interaction_entries
            else None
        )
        runtime_call_request = self._runtime_call_request_entry()
        now_entries = dedupe_layer_entries(
            [runtime_call_request] + stored_now_entries + material_entries)
        call_only_entries = dedupe_layer_entries(call_only_entries)
        # C 轨只改变可见性范围，不新增模型可见层；按旧 now 末尾顺序注入。
        now_entries = dedupe_layer_entries(now_entries + call_only_entries)
        now_section = []
        if now_entries:
            now_section = render_corpus_entries_for_context(
                now_entries,
                current_round=current_round,
                cache_source="now_cache.jsonl",
                active_corpus_start=active_corpus_index,
                current_reaction_iteration=current_reaction_iteration,
                expand_once_entry_keys=expand_once_entry_keys,
                active_corpus_registry=self._active_corpus_registry,
            )
            messages.extend(now_section)
        # 6. STATUSBAR 状态栏层：位于 now 之后、POPUP 之前。
        statusbar = self._build_statusbar_with_relations(
            state,
            round_type,
            current_input_text=visible_input_text,
            interaction_meta=visible_interaction_meta,
            relation_summary_mounts=self._relation_summary_mounts(mount_ids),
        )
        if statusbar:
            messages.append({
                "role": "system",
                "content": f"<!-- STATUSBAR（状态栏层） -->\n{statusbar}",
            })
        # 9. POPUP 弹窗层（messages 绝对末位）
        call_channel = channel_for_step(
            step,
            reaction_loop_phase=reaction_loop_phase,
            active_protocol_tool_guides=protocol_tool_guides,
        )
        popup_parts = self.popup_policy.split_fragments(self.popup.read_popup())
        identity_popup = self._build_identity_prompt_popup(
            popup_interaction_meta,
            has_interaction=bool(current_interaction_entries),
        )
        if identity_popup and "kind: identity_prompt" not in "\n\n".join(popup_parts):
            popup_parts.extend(self.popup_policy.split_fragments(identity_popup))
        identity_resolution_popup = self._build_identity_resolution_popup(
            context_step,
            popup_interaction_meta,
            has_interaction=bool(current_interaction_entries),
        )
        if (
            identity_resolution_popup
            and "kind: identity_resolution_card" not in "\n\n".join(popup_parts)
        ):
            popup_parts.extend(self.popup_policy.split_fragments(identity_resolution_popup))
        relation_registration_popup = self._build_relation_registration_popup(
            context_step, popup_interaction_meta)
        if (
            relation_registration_popup
            and "kind: relation_registration_reminder" not in "\n\n".join(popup_parts)
        ):
            popup_parts.extend(
                self.popup_policy.split_fragments(relation_registration_popup))
        if call_channel.name == "reaction.loop":
            reaction_step_guide_popup = build_current_runtime_guide_popup(
                context_step, state)
            if reaction_step_guide_popup:
                popup_parts.extend(
                    self.popup_policy.split_fragments(reaction_step_guide_popup))
            memory_reminder_popup = build_static_memory_reminder_popup(
                context_step,
                current_reaction_iteration=current_reaction_iteration,
            )
            if memory_reminder_popup:
                popup_parts.extend(
                    self.popup_policy.split_fragments(memory_reminder_popup))
        for fragment in popup_fragments or []:
            popup_parts.extend(self.popup_policy.split_fragments(fragment))
        if call_channel.include_protocol_writes:
            protocol_popup = self._build_protocol_tool_popup(
                context_step, protocol_tool_guides)
            if protocol_popup:
                popup_parts.extend(self.popup_policy.split_fragments(protocol_popup))
        if call_channel.include_standard_tools:
            general_popup = self._build_general_tool_popup(
                context_step, general_tool_guides)
            if general_popup:
                popup_parts.extend(self.popup_policy.split_fragments(general_popup))
        relay_target_popup = ""
        if context_step == "reaction" and str(round_type or "").strip().lower() == "relay":
            relay_target_popup = render_target_popup(
                state.get("base", {}).get("runtime", {}).get(
                    "pending_relay_target"))
        if relay_target_popup:
            popup_parts.extend(self.popup_policy.split_fragments(relay_target_popup))
        relay_intent_pool_popup = ""
        if context_step == "reaction" and str(round_type or "").strip().lower() in {
                "relay", "interactive"}:
            try:
                from logic.relay_intent_pool import render_open_relay_intents_for_context
                relay_intent_pool_popup = render_open_relay_intents_for_context(state)
            except Exception:
                relay_intent_pool_popup = ""
        if relay_intent_pool_popup:
            popup_parts.extend(
                self.popup_policy.split_fragments(relay_intent_pool_popup)
            )
        handoff_popup = self._build_handoff_popup(
            step, round_type, reaction_loop_phase)
        if handoff_popup:
            popup_parts.extend(self.popup_policy.split_fragments(handoff_popup))
        native_feedback_popup = build_native_tool_feedback_popup(
            step,
            native_tool_feedbacks,
        )
        if native_feedback_popup:
            popup_parts.extend(self.popup_policy.split_fragments(native_feedback_popup))
        popup_combined, popup_block_index = (
            self.popup_policy.combine_with_block_index(popup_parts)
        )
        self._current_layer_block_index["popup"] = popup_block_index
        if popup_combined:
            messages.append({"role": "system", "content": f"<!-- POPUP（弹窗层，messages绝对末位） -->\n{popup_combined}"})

        # 渲染完整 step.md 审计件 = 全部 messages 条目拼成 markdown
        md_parts = []
        for m in messages:
            role = m.get("role", "?")
            content = m.get("content", "")
            if role == "system":
                md_parts.append(content)
            else:
                md_parts.append(f"**{role}**: {content}")
        system = "\n\n---\n\n".join(md_parts)

        # 审计落盘（含全部七层元数据，供 manifest.json 统计）
        try:
            # 计算各层字符数
            lately_layer_markdown = messages_text(lately_section)
            now_layer_markdown = messages_text(now_section)
            now_layer_content = self._native_replay_layer_messages(
                now_entries,
                now_section,
            )
            self.audit.write_audit(context_step, {
                "permanent": permanent, "periodic": periodic,
                "high_freq": high_freq,
                "lately": lately_section,
                "lately_markdown": lately_layer_markdown,
                "now": now_layer_content,
                "now_markdown": now_layer_markdown,
                "statusbar": statusbar,
                "statusbar_projection": self._last_statusbar_projection,
                "call_only": messages_text(call_only_entries),
                "popup": popup_combined,
                "full_system": system,
                "permanent_block_index": self._current_layer_block_index.get("permanent", []),
                "periodic_block_index": self._current_layer_block_index.get("periodic", []),
                "high_freq_block_index": self._current_layer_block_index.get("high_freq", []),
                "statusbar_block_index": self._current_layer_block_index.get("statusbar", []),
                "popup_block_index": self._current_layer_block_index.get("popup", []),
            })
        except Exception as exc:
            raise RuntimeError("context_audit_write_failed") from exc

        # step.md/layers 是由 messages 渲染出的审计件；实际 provider payload
        # 由 APIExecutor 编译 provider_request.v1 后写入 step.json.request_body。
        return "", messages

    def _get_lately_entries(self, step):
        """最近缓存条目：由 lately_cache.jsonl 按步取窗口。"""
        ctx = self.context_store
        if ctx is None:
            from data.context_store import ContextStore
            ctx = ContextStore()
        return list(ctx.get_lately_entries(step))

    def _get_now_entries(self):
        """当前缓存条目：由 now_cache.jsonl 读取普通持久语料。"""
        ctx = self.context_store
        if ctx is None:
            from data.context_store import ContextStore
            ctx = ContextStore()
        try:
            return list(ctx.get_now_entries())
        except Exception as exc:
            raise RequiredContextError("read", "now_cache", exc) from exc

    def _get_call_transient_entries(self, round_num, step,
                                    current_reaction_iteration=None):
        """读取仅供本次目标调用消费的 C 轨语料。"""
        ctx = self.context_store
        if ctx is None:
            from data.context_store import ContextStore
            ctx = ContextStore()
        try:
            return list(ctx.get_call_transient_entries(
                round_num,
                step,
                reaction_iteration=current_reaction_iteration,
            ))
        except Exception as exc:
            raise RequiredContextError("read", "call_transient", exc) from exc

    @staticmethod
    def _native_replay_layer_messages(entries, rendered_entries):
        messages = []
        rendered_entries = list(rendered_entries or [])
        for index, entry in enumerate(entries or []):
            if not isinstance(entry, dict):
                continue
            rendered = (
                rendered_entries[index]
                if index < len(rendered_entries)
                and isinstance(rendered_entries[index], dict)
                else {}
            )
            message = {
                "role": str(
                    rendered.get("role")
                    or entry.get("role")
                    or "user"
                ).strip() or "user",
                "content": (
                    rendered.get("content")
                    if "content" in rendered
                    else entry.get("content", "")
                ) or "",
            }
            kind = str(entry.get("kind") or "").strip()
            if kind:
                message["kind"] = kind
            native_replay = entry.get("native_replay")
            if isinstance(native_replay, dict):
                message["native_replay"] = dict(native_replay)
            messages.append(message)
        return messages

    def _build_cache_section(
            self,
            label,
            source,
            entries,
            current_round=None,
            active_corpus_start=1,
            current_reaction_iteration=None,
            expand_once_entry_keys=None,
            active_corpus_registry=None):
        if not entries:
            return []
        entries = [
            entry for entry in entries
            if self._is_context_visible_entry(entry)
        ]
        if not entries:
            return []
        rendered_entries = render_corpus_entries_for_context(
            entries,
            current_round=current_round,
            cache_source=source,
            active_corpus_start=active_corpus_start,
            current_reaction_iteration=current_reaction_iteration,
            expand_once_entry_keys=expand_once_entry_keys,
            active_corpus_registry=active_corpus_registry,
        )
        return rendered_entries

    def _build_identity_prompt_popup(self, meta, has_interaction=False):
        if not has_interaction or not isinstance(meta, dict):
            return ""
        obj = meta.get("interaction_object")
        status = meta.get("identity_status")
        if obj == "unknown" or status in ("unknown", "timeout"):
            return self.popup.build_identity_prompt_event()
        return ""

    def _build_identity_resolution_popup(
            self, step, meta=None, has_interaction=False):
        if step != "reaction" or not has_interaction:
            return ""
        if not isinstance(meta, dict):
            return ""
        source = str(meta.get("interaction_source") or "").strip()
        if source in {"no_external_input", "system"}:
            return ""
        obj = str(meta.get("interaction_object") or "").strip()
        status = str(meta.get("identity_status") or "").strip()
        if status == "unregistered":
            return ""
        if obj in {"", "unknown", "Unknown", "UNKNOWN"} or status in {
                "", "unknown", "Unknown", "UNKNOWN", "timeout", "unregistered"}:
            return self.popup.build_identity_resolution_event()
        return ""

    def _build_relation_registration_popup(self, step, meta=None):
        if step != "reaction" or not isinstance(meta, dict):
            return ""
        if str(meta.get("identity_status") or "").strip() != "unregistered":
            return ""
        if str(meta.get("interaction_source") or "").strip() == "stale_relation_anchor":
            return ""
        return self.popup.build_relation_registration_event()

    def _build_handoff_popup(self, step, round_type=None, reaction_loop_phase="loop"):
        if step == "setup":
            if str(round_type or "").strip().lower() == "standby":
                message = PopupManager.load_guide_template("standby_setup") or ""
                fields = "standby_skip_reaction"
                fmt = self._extract_schema_section("STANDBY_SETUP_FORMAT")
                return format_step_guide_popup(
                    kind="standby_setup_handoff",
                    step="setup",
                    fields=fields,
                    message=message,
                    source="substrate_tool/standby_setup_handoff",
                    guide_marker="<!-- [STEP_GUIDE:setup] -->",
                    guide=fmt,
                    phase="standby",
                )
            message = PopupManager.load_guide_template("setup") or ""
            fields = "mount_requests, security_verdict, suggested_mode, relation_reminder"
            fmt = self._extract_schema_section("SETUP_FORMAT")
            return format_step_guide_popup(
                kind="setup_handoff",
                step="setup",
                fields=fields,
                message=message,
                source="substrate_tool/setup_handoff",
                guide_marker="<!-- [STEP_GUIDE:setup] -->",
                guide=fmt,
            )
        if step == "reaction":
            channel = channel_for_step(
                step,
                reaction_loop_phase=reaction_loop_phase,
            )
            message = PopupManager.load_guide_template(channel.popup_template) or ""
            fields = channel.popup_fields
            fmt = ""
            kind = channel.popup_kind
            return format_step_guide_popup(
                kind=kind,
                step="reaction",
                fields=fields,
                message=message,
                source="substrate_tool/reaction_loop",
                guide_marker="",
                guide=fmt,
                phase=reaction_loop_phase,
            )
        if step == "cleanup":
            fmt = self._extract_schema_section("CLEANUP_FORMAT")
            return format_step_guide_popup(
                kind="cleanup_handoff",
                step="cleanup",
                fields="connection_materials, tacit_materials, cache_compaction",
                message=PopupManager.load_guide_template("cleanup") or "",
                source="substrate_tool/cleanup_handoff",
                guide_marker="<!-- [STEP_GUIDE:cleanup] -->",
                guide=fmt,
            )
        return ""

    # ==============================================================
    # 频率层缓存（DDS §21 过期标记与重建策略）
    # ==============================================================

    def _cached_or_build(self, step, layer, expired, builder):
        """若未过期且有缓存则复用，否则重建并存入缓存"""
        cache_key = (step, layer)
        if not expired and cache_key in self._layer_cache:
            self._current_layer_block_index[layer] = list(
                self._layer_block_cache.get(cache_key, [])
            )
            return self._layer_cache[cache_key]
        self._current_layer_block_index[layer] = []
        text = builder()
        self._layer_cache[cache_key] = text
        self._layer_block_cache[cache_key] = list(
            self._current_layer_block_index.get(layer, [])
        )
        self._mark_layer_fresh(layer)
        return text

    def invalidate_layer(self, layer):
        """外部数据源变更时调用：标记该层下次需重建"""
        for key in list(self._layer_cache):
            if key[1] == layer:
                del self._layer_cache[key]
                self._layer_block_cache.pop(key, None)
        if layer == "high_freq":
            return
        if layer == "popup":
            if self.state_store:
                try:
                    self.state_store.set("base.context_cache.popup_active", True)
                except Exception:
                    pass
            return
        if layer not in EXPIRED_CONTEXT_LAYERS:
            return
        if self.state_store:
            try:
                self.state_store._set_internal(
                    f"base.context_cache.{layer}_expired", True)
            except Exception:
                pass

    def _mark_layer_fresh(self, layer):
        """标记该层已刷新，在下次数据源变更前可复用"""
        if layer not in EXPIRED_CONTEXT_LAYERS:
            return
        if self.state_store:
            try:
                self.state_store._set_internal(
                    f"base.context_cache.{layer}_expired", False)
            except Exception:
                pass

    # ==============================================================
    # 永固层
    # ==============================================================

    def _build_permanent(self, state, step, round_type):
        blocks = [{
            "block_id": "permanent:core_identity",
            "title": "位格核心",
            "kind": "core_identity",
            "source_block_id": "persona/core.md",
            "content": self._load_core_identity(),
        }]
        rule_blocks = self._load_rules_for_layers(["permanent"])
        if isinstance(rule_blocks, str):
            rule_blocks = ([{
                "block_id": "rule:permanent",
                "title": "permanent rules",
                "kind": "permanent_rule",
                "content": rule_blocks,
            }] if rule_blocks else [])
        for index, block in enumerate(rule_blocks):
            if index == 0:
                block["content"] = (
                    "<!-- [RULES:permanent+step] -->\n## RULES\n"
                    + block["content"]
                )
            else:
                block["separator_before"] = "\n\n---\n\n"
        blocks.extend(rule_blocks)
        text, block_index = join_layer_blocks(blocks)
        self._current_layer_block_index["permanent"] = block_index
        return text

    # ==============================================================
    # 定期层
    # ==============================================================

    def _build_periodic(self, state, step, round_type):
        """定期层 DDS §19.5：仅装配当前活动的定期记忆投影。"""
        text, block_index = context_build_periodic_with_block_index(
            self, state, step, round_type
        )
        self._current_layer_block_index["periodic"] = block_index
        return text

    # ==============================================================
    # 高频层
    # ==============================================================

    def _build_high_freq(self, state, step, round_type,
                         include_content, mount_ids, input_keywords=None,
                         current_input_text=None, interaction_meta=None,
                         runtime_focus_entries=None):
        """DDS §19 高频层排序：索引→本步短工具带→CONTENT。"""
        # 推导当前轮的焦点关键词（供联想索引的重排序查询用）
        if input_keywords is None:
            input_keywords = self._derive_input_keywords(state, step, mount_ids)
        if not input_keywords and current_input_text:
            input_keywords = self._derive_keywords_from_text(current_input_text)

        blocks = []

        def add(block_id, title, kind, content, source_block_id=""):
            if content:
                blocks.append({
                    "block_id": block_id,
                    "title": title,
                    "kind": kind,
                    "source_block_id": source_block_id,
                    "content": content,
                })

        # 1. 容器索引（每容器一条最近修改子项）
        add("high_freq:container_index", "容器索引", "container_index",
            self._build_container_index())
        limits = self._high_freq_index_limits()
        # 2. LTM 热度索引（last_recalled_at排序，分钟粒度）
        add("high_freq:ltm_heat", "LTM 热度索引", "ltm_heat_index",
            self._build_ltm_heat_index(limit=limits.get("ltm_heat_index", 16)))
        # 3. STM 热度索引（H值降序）
        add("high_freq:stm_heat", "STM 热度索引", "stm_heat_index",
            self._build_stm_heat_index(
                limit=limits.get("stm_heat_index", STM_INDEX_DISPLAY_LIMIT)))
        # 4. Skills 倒排索引（8条）
        add("high_freq:skills_inverted", "Skills 倒排索引", "skills_inverted",
            self._build_keyword_index("skills", limits.get("skills_inverted", 8)))
        # 5. LTM 倒排索引（8条）
        add("high_freq:ltm_inverted", "LTM 倒排索引", "ltm_inverted",
            self._build_keyword_index("ltm", limits.get("ltm_inverted", 8)))
        # 6. STM 倒排索引（8条）
        add("high_freq:stm_inverted", "STM 倒排索引", "stm_inverted",
            self._build_keyword_index("stm", limits.get("stm_inverted", 8)))
        # 7. 联想索引 — 只投影记忆条目，输入关键词驱动
        add("high_freq:association", "联想索引", "association_index",
            self._build_association_index(
                limits.get("association_index", 8), input_keywords))
        # 8. 关系索引：动态倒排 + 四区底图
        add("high_freq:relation_inverted", "关系倒排索引", "relation_inverted",
            self._build_relation_inverted_index(
                limit=limits.get("relation_inverted", 8),
                current_input_text=current_input_text,
                interaction_meta=interaction_meta,
            ))
        add("high_freq:relation_domain", "关系域索引", "relation_domain",
            self._build_relation_domain_index(
                limit=limits.get("relation_domain", 8),
                current_input_text=current_input_text,
                interaction_meta=interaction_meta,
            ))
        task_board = self._build_task_board_projection()
        add("high_freq:task_board", "任务工作台", "task_board", task_board)
        step_toolbelt = self._build_step_toolbelt_index(step, round_type)
        add("high_freq:step_toolbelt", "当前步短工具带", "step_toolbelt",
            step_toolbelt)
        # 9. CONTENT（挂载正文 + 参考窗口 + WB工作台）
        current_round = current_round_from_state(state)
        try:
            focus_projection = self._build_workbench_focus_projection(current_round)
        except TypeError:
            focus_projection = self._build_workbench_focus_projection()
        add("high_freq:workbench_focus", "WB 焦点投影", "workbench_focus",
            focus_projection)
        for index, focus_entry in enumerate(
                normalize_layer_entries(runtime_focus_entries), 1):
            focus_content = str(focus_entry.get("content") or "").strip()
            if focus_content:
                ref = focus_entry.get("ref") if isinstance(focus_entry.get("ref"), dict) else {}
                source_id = str(
                    focus_entry.get("source_block_id")
                    or focus_entry.get("id")
                    or ref.get("source_block_id")
                    or ""
                )
                add(
                    f"high_freq:runtime_focus:{index}:{source_id}",
                    str(focus_entry.get("title") or f"Runtime 焦点 {index}"),
                    "runtime_focus",
                    focus_content,
                    source_id,
                )
        content_mounts = self._content_mounts_with_triple_hits(
            mount_ids, input_keywords) if include_content else []
        if include_content and content_mounts:
            blocks.extend(context_build_mounted_content_blocks(
                self, content_mounts, current_round=current_round
            ))
        text, block_index = join_layer_blocks(blocks)
        self._current_layer_block_index["high_freq"] = block_index
        return text

    def _build_task_board_projection(self):
        try:
            from data.workbench import WorkbenchStore
            return render_active_task_board(
                WorkbenchStore(),
                recent_context_entries=self._task_board_recent_context_entries(),
            )
        except RequiredContextError:
            raise
        except Exception as exc:
            raise RequiredContextError("read", "task_board", exc) from exc

    def _task_board_recent_context_entries(self):
        ctx = self.context_store
        if ctx is None:
            try:
                from data.context_store import ContextStore
                ctx = ContextStore()
            except Exception as exc:
                raise RequiredContextError(
                    "read", "task_board_recent_context", exc) from exc
        try:
            entries = []
            entries.extend(ctx.get_lately_entries())
            entries.extend(ctx.get_now_entries())
        except Exception as exc:
            raise RequiredContextError(
                "read", "task_board_recent_context", exc) from exc
        return entries[-160:]

    def _high_freq_index_limits(self):
        defaults = {
            "container_index": 1,
            "ltm_heat_index": 16,
            "stm_heat_index": STM_INDEX_DISPLAY_LIMIT,
            "skills_inverted": 8,
            "relation_inverted": 8,
            "relation_domain": 8,
            "ltm_inverted": 8,
            "stm_inverted": 8,
            "association_index": 8,
        }
        try:
            cfg = self.config_store
            if cfg is None:
                from data.config_store import ConfigStore
                cfg = ConfigStore()
            limits = cfg.get_high_freq_params().get("index_display_limits", {})
            if isinstance(limits, dict):
                for key, value in limits.items():
                    try:
                        defaults[key] = max(1, int(value))
                    except (TypeError, ValueError):
                        pass
        except Exception:
            pass
        return defaults

    def _build_step_toolbelt_index(self, step, round_type=None):
        if step == "setup":
            handoff_tool = (
                "standby_setup_handoff"
                if str(round_type or "").strip().lower() == "standby"
                else "setup_handoff"
            )
            return "\n".join([
                "<!-- [STEP_TOOLBELT:setup] -->",
                "## 起手步短工具带",
                "| tool_id | tool_family | tool_class | 用途 |",
                "|---------|-------------|------------|------|",
                "| setup_mount_apply | substrate_tool | read_tool | 应用起手挂载清单并装配反应步入口 |",
                "| setup_security_gate | substrate_tool | sync_tool | 承接起手安全二值裁决 |",
                f"| {handoff_tool} | substrate_tool | sync_tool | 写入起手步到后续步骤的交接 |",
            ])
        if step == "reaction":
            parts = ["<!-- [STEP_TOOLBELT:reaction] -->", "## 反应步短工具带"]
            tool_index = load_protocol_tool_index()
            if tool_index:
                parts.append(f"<!-- [PROTOCOL_TOOLS:index] -->\n{tool_index}")
            general_tool_index = load_general_tool_index()
            if general_tool_index:
                parts.append(f"<!-- [GENERAL_TOOLS:index] -->\n{general_tool_index}")
            return "\n\n".join(parts) if len(parts) > 2 else ""
        if step == "cleanup":
            return "\n".join([
                "<!-- [STEP_TOOLBELT:cleanup] -->",
                "## 善后步短工具带",
                "| tool_id | tool_family | tool_class | 用途 |",
                "|---------|-------------|------------|------|",
                "| connection_material_settle | substrate_tool | sync_tool | 承接联系集处理表 |",
                "| tacit_material_settle | substrate_tool | sync_tool | 承接默契集处理表 |",
                "| association_count_update | substrate_tool | sync_tool | 基于有效材料更新联想计数 |",
                "| cache_compact | substrate_tool | sync_tool | 执行最近缓存删后幸存段压缩 |",
                "| cleanup_handoff | substrate_tool | sync_tool | 写入善后内部整理提示 |",
            ])
        return ""

    def _build_workbench_focus_projection(self, current_round=None):
        try:
            from data.workbench import WorkbenchStore
            from data.container_store import ContainerStore
            status = WorkbenchStore().load_status()
            focus = (status.get("base") or {}).get("focus")
            if not focus:
                return ""
            projection = ContainerStore().read_focus_projection(focus)
        except Exception as exc:
            raise RequiredContextError("read", "workbench_focus", exc) from exc

        allowed = " / ".join(projection.get("allowed_targets") or [])
        content_for_native_focus = projection.get("content") or "(empty focus projection)"
        round_id = format_round_id(current_round)
        lines = [
            "## WB 焦点投影",
        ]
        if round_id:
            lines.append(f"- 当前可见轮次：{round_id}")
        lines.extend([
            f"- 容器编号：{projection.get('container_id')}",
            f"- 容器类型：{projection.get('container_type')}",
            f"- 标题：{projection.get('title')}",
            f"- 状态：{projection.get('status')}",
            f"- 允许写入目标：{allowed}",
            f"- 默认写入目标：{projection.get('default_target')}",
            "- 写入约束：只有在迭代入口已经看见本 WB 焦点投影时，才能通过 provider-native `memory_container_write` 追加正文。",
            "- 焦点约束：同一迭代刚通过 `container_focus.open` 打开的容器，要到下一迭代看见焦点投影后才能写入。",
            "- 内容边界：POPUP 只承载规则；本 WB 焦点投影才是可见 CONTENT 材料。",
            "- 轮次边界：当前可见只说明本步看见了材料，不证明本轮已经执行新的工具动作。",
            "",
            "### 当前内容片段",
            content_for_native_focus,
        ])
        return "\n".join(lines)

    def _content_mounts_with_triple_hits(self, mount_ids, input_keywords):
        mounts = list(mount_ids or [])
        mounts.extend(self._resident_relation_body_mounts())
        existing_memory_ids = self._mounted_memory_ids(mounts)
        for mem_id in self._triple_hit_stm_memory_ids(input_keywords):
            if mem_id in existing_memory_ids:
                continue
            mounts.append({
                "type": "memory",
                "ids": mem_id,
                "source": "stm_triple_hit",
            })
            existing_memory_ids.add(mem_id)
        return mounts

    def _resident_relation_body_mounts(self, limit=3):
        mounts = []
        try:
            from data.relation_store import RelationStore
            for card in RelationStore().load_registry().get("cards", []):
                if card.get("status") == "archived" or not card.get("body_resident"):
                    continue
                card_id = str(card.get("id") or "").strip()
                if card_id:
                    mounts.append({
                        "type": "relation",
                        "ids": card_id,
                        "mode": "resident",
                        "source": "relation_read_resident",
                    })
                if len(mounts) >= limit:
                    break
        except Exception:
            return []
        return mounts

    @staticmethod
    def _relation_summary_mounts(mount_ids):
        result = []
        for req in mount_ids or []:
            if not isinstance(req, dict) or req.get("type") != "relation_summary":
                continue
            raw = req.get("ids", "")
            ids = raw if isinstance(raw, (list, tuple)) else str(raw).split(",")
            for card_id in ids:
                card_id = str(card_id).strip()
                if card_id and card_id not in result:
                    result.append(card_id)
        return result

    def _mounted_memory_ids(self, mounts):
        ids = set()
        for item in mounts or []:
            if not isinstance(item, dict) or item.get("type") != "memory":
                continue
            raw = item.get("ids", "")
            if isinstance(raw, (list, tuple)):
                values = raw
            else:
                values = str(raw).split(",")
            for mem_id in values:
                mem_id = str(mem_id).strip()
                if mem_id:
                    ids.add(mem_id)
        return ids

    def _derive_keywords_from_text(self, text):
        try:
            import json as _json, os as _os
            from paths import KEYWORDS_JSON
            if not text or not _os.path.isfile(KEYWORDS_JSON):
                return []
            with open(KEYWORDS_JSON, "r", encoding="utf-8") as f:
                data = _json.load(f)
            index = data.get("index", {})
            return [kw for kw in index if kw and kw in text][:8]
        except Exception:
            return []

    def _triple_hit_stm_memory_ids(self, input_keywords):
        """Auto-expand STM bodies only for inverted + association + connection hits."""
        if not input_keywords:
            return []
        try:
            import json as _json, os as _os
            from paths import KEYWORDS_JSON, ASSOCIATION_SET_DIR, CONNECTION_SET_DIR

            with open(KEYWORDS_JSON, "r", encoding="utf-8") as f:
                kw_data = _json.load(f)
            stm_index = kw_data.get("index", {})
            input_set = set(input_keywords)
            assoc_partners = set()
            for fname in ("assoc_kw_kw.json", "assoc_kw_ifeel.json", "assoc_kw_rfeel.json"):
                fpath = _os.path.join(ASSOCIATION_SET_DIR, fname)
                if not _os.path.isfile(fpath):
                    continue
                with open(fpath, "r", encoding="utf-8") as f:
                    pairs = _json.load(f)
                for key in pairs:
                    if "|||" not in key:
                        continue
                    left, right = key.split("|||", 1)
                    if left in input_set and right not in input_set:
                        assoc_partners.add(right)
                    if right in input_set and left not in input_set:
                        assoc_partners.add(left)
            if not assoc_partners:
                return []

            ordered = []
            seen = set()
            for filename in ("pending.jsonl", "processed.jsonl"):
                path = _os.path.join(CONNECTION_SET_DIR, filename)
                if not _os.path.isfile(path):
                    continue
                with open(path, "r", encoding="utf-8") as f:
                    for line in f:
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            row = _json.loads(line)
                        except _json.JSONDecodeError:
                            continue
                        pairs = [
                            (row.get("word_a", ""), row.get("entry_a", ""),
                             row.get("word_b", ""), row.get("entry_b", "")),
                            (row.get("word_b", ""), row.get("entry_b", ""),
                             row.get("word_a", ""), row.get("entry_a", "")),
                        ]
                        for input_kw, _input_entry, partner_kw, partner_entry in pairs:
                            if input_kw not in input_set or partner_kw not in assoc_partners:
                                continue
                            indexed_ids = set(stm_index.get(partner_kw, []))
                            for mem_id in self._split_mem_ids(partner_entry):
                                if mem_id in indexed_ids and mem_id not in seen:
                                    ordered.append(mem_id)
                                    seen.add(mem_id)
            return ordered[:8]
        except Exception:
            return []

    @staticmethod
    def _split_mem_ids(raw):
        if isinstance(raw, (list, tuple)):
            values = raw
        else:
            values = str(raw or "").replace(";", ",").split(",")
        return [str(value).strip() for value in values if str(value).strip()]

    @staticmethod
    def _active_relation_cards(cards):
        return [
            card for card in cards or []
            if isinstance(card, dict) and card.get("status") != "archived"
        ]

    @staticmethod
    def _relation_card_tokens(card):
        tokens = []
        for value in (
            card.get("id"),
            card.get("name"),
            f"REL-{card.get('name')}" if card.get("name") else "",
        ):
            text = str(value or "").strip()
            if text:
                tokens.append(text)
        for key in ("aliases", "tags"):
            for value in card.get(key, []) or []:
                text = str(value or "").strip()
                if text:
                    tokens.append(text)
        return tokens

    @staticmethod
    def _relation_visible_label(card):
        return relation_card_label(card) or str(card.get("id") or "").strip()

    @staticmethod
    def _relation_sort_key(card):
        return str(card.get("updated_at") or "")

    def _relation_hit_scores(self, cards, current_input_text=None, interaction_meta=None):
        text = str(current_input_text or self._current_input_text or "").strip()
        meta = interaction_meta or self._current_interaction_meta or {}
        direct_object = str(meta.get("interaction_object") or "").strip()
        hits = {}
        for card in self._active_relation_cards(cards):
            card_id = card.get("id", "")
            score = 0
            reasons = []
            name = self._relation_visible_label(card)
            strong_candidates = {card_id, name}
            if name:
                strong_candidates.add(f"REL-{name}")
            strong_candidates.update(str(item or "").strip()
                                     for item in card.get("aliases", []) or [])
            object_candidates = set(strong_candidates)
            object_candidates.update(str(item or "").strip()
                                     for item in card.get("tags", []) or [])
            if direct_object and direct_object in object_candidates:
                score += 100
                reasons.append("interaction_object")
            for token in self._relation_card_tokens(card):
                if not token:
                    continue
                if token in text:
                    weight = 60 if token in strong_candidates else 20
                    if len(token) < 3 and token not in strong_candidates:
                        continue
                    score += weight
                    reasons.append(relation_public_name(token) or token)
            if score > 0:
                hits[card_id] = {
                    "card": card,
                    "score": score,
                    "reasons": list(dict.fromkeys(reasons)),
                }
        return hits

    def _build_relation_inverted_index(self, limit=8, current_input_text=None,
                                       interaction_meta=None, offset=0):
        parts = ["## 关系倒排索引"]
        try:
            from data.relation_store import RelationStore
            cards = RelationStore().load_registry().get("cards", [])
            hits = self._relation_hit_scores(
                cards, current_input_text=current_input_text,
                interaction_meta=interaction_meta)
            rows = sorted(
                hits.values(),
                key=lambda item: (
                    item.get("score", 0),
                    self._relation_sort_key(item.get("card", {})),
                ),
                reverse=True,
            )
            if not rows:
                parts.append("（本轮输入未命中已有关系卡）")
                return "\n".join(parts)
            for item in slice_entries(rows, offset, limit):
                card = item.get("card", {})
                label = self._relation_visible_label(card)
                reasons = "、".join(
                    relation_public_name(reason) or reason
                    for reason in item.get("reasons", [])[:3]
                )
                parts.append(
                    f"- {label} [{item.get('score')}] "
                    f"({card.get('category', '?')}; 命中: {reasons})"
                )
            marker = fold_marker(
                "relation_inverted", len(rows), offset, limit)
            if marker:
                parts.append(marker)
        except Exception:
            parts.append("（关系倒排索引读取失败）")
        return "\n".join(parts)

    def _build_relation_domain_index(self, limit=8, current_input_text=None,
                                     interaction_meta=None, offset=0, zone=None):
        parts = ["## 关系域索引"]
        zones = [zone] if zone else ["self", "ours", "them", "orgs"]
        try:
            from data.relation_store import RelationStore
            cards = self._active_relation_cards(
                RelationStore().load_registry().get("cards", []))
            hits = self._relation_hit_scores(
                cards, current_input_text=current_input_text,
                interaction_meta=interaction_meta)
            hit_ids = set(hits)
            for zone_name in zones:
                zone_cards = [
                    card for card in cards
                    if str(card.get("category") or "ours") == zone_name
                ]
                zone_cards.sort(
                    key=lambda card: self._relation_sort_key(card),
                    reverse=True,
                )
                zone_cards.sort(key=lambda card: card.get("id") not in hit_ids)
                parts.append(f"### {zone_name}")
                if not zone_cards:
                    parts.append("（空）")
                    continue
                for card in slice_entries(zone_cards, offset, limit):
                    marker = " *" if card.get("id") in hit_ids else ""
                    updated = card.get("updated_at", "")
                    label = self._relation_visible_label(card)
                    parts.append(
                        f"- {label}{marker} "
                        f"(updated_at={updated})"
                    )
                fold = fold_marker(
                    "relation_domain", len(zone_cards), offset, limit,
                    zone=zone_name)
                if fold:
                    parts.append(fold)
        except Exception:
            parts.append("（关系域索引读取失败）")
        return "\n".join(parts)

    def build_index_view(self, scope, zone=None, offset=0, limit=8,
                         current_input_text=None, interaction_meta=None,
                         input_keywords=None, hidden_stm_memory_ids=None):
        """返回 index_view 只读工具回执。"""
        previous_hidden = set(getattr(self, "_hidden_stm_memory_ids", set()))
        if hidden_stm_memory_ids is not None:
            self._hidden_stm_memory_ids = set(hidden_stm_memory_ids or [])
        scope = str(scope or "").strip()
        zone = str(zone or "").strip() or None
        try:
            offset = max(0, int(offset or 0))
        except (TypeError, ValueError):
            offset = 0
        try:
            limit = max(1, min(32, int(limit or 8)))
        except (TypeError, ValueError):
            limit = 8

        builders = {
            "ltm_heat": lambda: self._build_ltm_heat_index(limit=limit, offset=offset),
            "stm_heat": lambda: self._build_stm_heat_index(limit=limit, offset=offset),
            "skills_inverted": lambda: self._build_keyword_index("skills", limit=limit, offset=offset),
            "ltm_inverted": lambda: self._build_keyword_index("ltm", limit=limit, offset=offset),
            "stm_inverted": lambda: self._build_keyword_index("stm", limit=limit, offset=offset),
            "association": lambda: self._build_association_index(
                limit=limit, input_keywords=input_keywords, offset=offset),
            "relation_inverted": lambda: self._build_relation_inverted_index(
                limit=limit, current_input_text=current_input_text,
                interaction_meta=interaction_meta, offset=offset),
            "relation_domain": lambda: self._build_relation_domain_index(
                limit=limit, current_input_text=current_input_text,
                interaction_meta=interaction_meta, offset=offset, zone=zone),
        }
        receipt = {
            "tool_id": "index_view",
            "tool_family": "protocol_tool",
            "tool_class": "read_tool",
            "status": "accepted" if scope in builders else "rejected",
            "source": "protocol_tool_request",
            "scope": scope,
            "zone": zone or "",
            "offset": offset,
            "limit": limit,
            "content": "",
            "protocol_tool_receipt": True,
        }
        try:
            if scope not in builders:
                receipt["reason"] = "unsupported_scope"
                return receipt
            receipt["content"] = builders[scope]()
            return receipt
        finally:
            self._hidden_stm_memory_ids = previous_hidden

    def _relation_focus_max_slots(self):
        cfg = self.config_store
        if cfg and hasattr(cfg, "get_relation_params"):
            try:
                return cfg.get_relation_params().get("max_slots", 3)
            except Exception:
                return 3
        try:
            from data.config_store import ConfigStore
            return ConfigStore().get_relation_params().get("max_slots", 3)
        except Exception:
            return 3

    def _build_statusbar_with_relations(self, state, round_type,
                                        current_input_text=None,
                                        interaction_meta=None,
                                        relation_summary_mounts=None):
        """生成 STATUSBAR 结构化投影，再渲染为模型可见文本。"""
        projection = self.statusbar.build_projection(state, round_type)
        try:
            from logic.execution_permission import (
                load_execution_permission_level,
                render_execution_permission_status,
            )
            projection["supplemental_sections"].append(
                render_execution_permission_status(
                    getattr(self, "execution_permission_level", None)
                    or load_execution_permission_level(self.config_store)))
        except Exception as exc:
            raise RequiredContextError(
                "read", "execution_permission", exc) from exc
        try:
            from data.relation_store import RelationStore
            from logic.relation_focus import RelationFocusManager

            rs = RelationStore()
            max_slots = self._relation_focus_max_slots()
            rfm = RelationFocusManager(max_slots=max_slots + 1)
            reg = rs.load_registry()
            all_cards = reg.get("cards", [])

            meta = interaction_meta or self._current_interaction_meta or {}
            current_id = str(meta.get("interaction_object_id") or "").strip()
            current_card = next(
                (card for card in all_cards
                 if card.get("id") == current_id
                 and card.get("status", "active") == "active"),
                None,
            )
            if current_card is None:
                resolved_id = rs.resolve_active_subject(meta.get("interaction_object"))
                current_card = next(
                    (card for card in all_cards if card.get("id") == resolved_id),
                    None,
                )
                current_id = resolved_id or current_id
            current_name = (
                self._relation_visible_label(current_card)
                if current_card else str(meta.get("interaction_object") or "").strip()
            )
            identity_status = str(meta.get("identity_status") or "").strip()
            known_name = current_name and current_name.lower() != "unknown"
            registration_status = (
                "registered" if current_card
                else "unregistered"
                if known_name and identity_status in {"known", "declared", "unregistered"}
                else "unbound"
            )
            projection["interaction"].update({
                "relation_id": current_id,
                "display_name": current_name if registration_status != "unbound" else "",
                "registration_status": registration_status,
                "identity_source": str(meta.get("interaction_source") or "unresolved"),
            })

            input_text = (
                current_input_text or
                self._current_input_text or
                self._get_current_input_text()
            )
            focus_result = rfm.determine_focus_states(
                input_text, all_cards,
                interaction_object=meta.get("interaction_object"))
            active_ids = {obj["id"] for obj in focus_result["active"]}
            for card in all_cards:
                if card.get("summary_resident"):
                    active_ids.add(card.get("id"))
            for card_id in relation_summary_mounts or []:
                active_ids.add(card_id)

            focus_type_map = {}
            for obj in focus_result["active"]:
                focus_type_map[obj["id"]] = obj["match_type"]
            for obj in focus_result.get("recall", []):
                if obj["id"] not in focus_type_map:
                    focus_type_map[obj["id"]] = "recall"
            for card in all_cards:
                if card.get("summary_resident") and card.get("id") not in focus_type_map:
                    focus_type_map[card.get("id")] = "resident"
            for card_id in relation_summary_mounts or []:
                if card_id not in focus_type_map:
                    focus_type_map[card_id] = "temporary"
            other_cards = [
                card for card in all_cards
                if card.get("id") in active_ids and card.get("id") != current_id
            ][:max_slots]
            display_cards = ([current_card] if current_card else []) + other_cards
            relation_cards = []
            for card in display_cards:
                card_id = card.get("id", "")
                summary = self._relation_card_summary(rs, card)
                relation_cards.append({
                    "id": card_id,
                    "name": self._relation_visible_label(card),
                    "category": card.get("category", "?"),
                    "focus_type": (
                        "present" if card_id == current_id
                        else focus_type_map.get(card_id, "")),
                    "summary": summary,
                })
                if card_id == current_id:
                    projection["interaction"]["summary"] = summary
            projection["relation_cards"] = relation_cards
        except RequiredContextError:
            raise
        except Exception:
            pass
        self._last_statusbar_projection = projection
        text, block_index = self.statusbar.render_with_block_index(projection)
        self._current_layer_block_index["statusbar"] = block_index
        return text

    @staticmethod
    def _relation_card_summary(relation_store, card):
        import os
        path = relation_store.get_card_path(
            card.get("id", ""), card.get("category", "ours"))
        if not os.path.isfile(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8") as handle:
                content = handle.read()
        except OSError:
            return ""
        for marker in ("## 现在（Present）", "## 现在", "## Present", "## 备注", "## 基础信息"):
            index = content.find(marker)
            if index < 0:
                continue
            body = content[index + len(marker):]
            end = body.find("\n## ")
            for line in body[:end if end >= 0 else None].splitlines():
                text = line.strip()
                if text and not text.startswith("#") and len(text) > 3:
                    return text[:100] + ("…" if len(text) > 100 else "")
        return ""

    def _build_protocol_tool_popup(self, step, protocol_tool_guides=None):
        if step != "reaction":
            return ""
        parts = []
        for tool_id in protocol_tool_guides or []:
            guide = build_protocol_tool_guide(tool_id)
            if guide:
                parts.append(guide)
        return "\n\n".join(part for part in parts if part)

    def _build_general_tool_popup(self, step, general_tool_guides=None):
        if step != "reaction":
            return ""
        parts = []
        try:
            from logic.sandbox_grant import render_sandbox_grant_guide
            grant_guide = render_sandbox_grant_guide()
            if grant_guide:
                parts.append(grant_guide)
        except Exception:
            pass
        try:
            from logic.execution_permission import (
                load_execution_permission_level,
                render_execution_permission_guide,
            )
            parts.append(render_execution_permission_guide(
                getattr(self, "execution_permission_level", None)
                or load_execution_permission_level(self.config_store)
            ))
        except Exception as exc:
            raise RequiredContextError(
                "read", "execution_permission", exc) from exc
        for tool_id in general_tool_guides or []:
            guide = build_general_tool_guide(tool_id)
            if guide:
                parts.append(guide)
        return "\n\n".join(part for part in parts if part)

    def _get_current_input_text(self):
        """获取当前输入文本（从 now/lately 主源读取最近一条用户消息）"""
        try:
            ctx = self.context_store
            if ctx is None:
                from data.context_store import ContextStore
                ctx = ContextStore()
            return ctx.get_current_input_text()
        except Exception as exc:
            raise RequiredContextError("read", "current_input", exc) from exc

    def _build_container_index(self):
        return context_build_container_index(self)

    def _build_ltm_heat_index(self, limit=16, offset=0):
        return context_build_ltm_heat_index(self, limit=limit, offset=offset)

    def _build_keyword_index(self, source, limit=8, offset=0):
        return context_build_keyword_index(self, source, limit=limit, offset=offset)
    def _derive_input_keywords(self, state, step, mount_ids):
        return context_derive_input_keywords(self, state, step, mount_ids)
    def _get_keywords_for_mem_id(self, mem_id):
        return context_get_keywords_for_mem_id(mem_id)
    def _build_association_index(self, limit=16, input_keywords=None, offset=0):
        return context_build_association_index(self, limit=limit, input_keywords=input_keywords, offset=offset)

    # ==============================================================
    # 注册表驱动的 RULES 加载 (v0.9.0)
    # ==============================================================

    def _load_registry(self):
        """加载 rules_registry.json，缓存"""
        if self._registry is not None:
            return self._registry
        try:
            with open(RULES_REGISTRY, "r", encoding="utf-8") as f:
                self._registry = json.load(f)
        except (OSError, json.JSONDecodeError, UnicodeError):
            self._registry = {"permanent": [], "step_level": [],
                              "periodic": [], "on_demand": []}
        return self._registry

    def _load_rule_file(self, rel_path):
        """读单个 rules 文件，缓存。拒绝路径遍历。"""
        if rel_path in self._rules_cache:
            return self._rules_cache[rel_path]
        # 拒绝包含 .. 或绝对路径的恶意路径
        if ".." in rel_path or os.path.isabs(rel_path):
            return ""
        full_path = os.path.join(RULES_DIR, rel_path)
        # 二次确认：解析后的路径仍在 RULES_DIR 内
        if not os.path.realpath(full_path).startswith(os.path.realpath(RULES_DIR)):
            return ""
        try:
            with open(full_path, "r", encoding="utf-8") as f:
                content = f.read()
        except (OSError, UnicodeError):
            content = ""
        self._rules_cache[rel_path] = content
        return content

    def _load_rules_for_layers(self, layers):
        """加载指定 layer 的 rules 文件并保留真实文件边界。"""
        reg = self._load_registry()
        blocks = []

        for layer in layers:
            entries = reg.get(layer, [])
            for entry in entries:
                path = entry.get("path", "")
                if path:
                    content = self._load_rule_file(path)
                    if content.strip():
                        blocks.append({
                            "block_id": f"rule:{path}",
                            "title": str(entry.get("file") or os.path.basename(path)),
                            "kind": "permanent_rule",
                            "source_block_id": str(path),
                            "content": content,
                        })
        return blocks

    # ==============================================================
    # schema.md 区段提取
    # ==============================================================

    def _extract_schema_section(self, section):
        """从 schema.md 提取指定区段（SETUP_FORMAT / CLEANUP_FORMAT / REACTION_*_FORMAT）"""
        marker_map = {
            "SETUP_FORMAT": ("<!-- SETUP_FORMAT_START -->", "<!-- SETUP_FORMAT_END -->"),
            "STANDBY_SETUP_FORMAT": (
                "<!-- STANDBY_SETUP_FORMAT_START -->",
                "<!-- STANDBY_SETUP_FORMAT_END -->",
            ),
            "CLEANUP_FORMAT": ("<!-- CLEANUP_FORMAT_START -->", "<!-- CLEANUP_FORMAT_END -->"),
            "REACTION_EXIT_FORMAT": ("<!-- REACTION_EXIT_FORMAT_START -->", "<!-- REACTION_EXIT_FORMAT_END -->"),
            "REACTION_RESULT_FORMAT": ("<!-- REACTION_RESULT_FORMAT_START -->", "<!-- REACTION_RESULT_FORMAT_END -->"),
        }
        markers = marker_map.get(section)
        if not markers:
            return ""

        if not os.path.isfile(DOCS_SCHEMA):
            return ""
        try:
            with open(DOCS_SCHEMA, "r", encoding="utf-8") as f:
                content = f.read()
        except (OSError, UnicodeDecodeError, UnicodeError):
            return ""

        start_marker, end_marker = markers
        start = content.find(start_marker)
        if start == -1:
            return ""
        end = content.find(end_marker, start + len(start_marker))
        if end == -1:
            return ""
        return content[start + len(start_marker):end].strip()

    # ==============================================================
    # 核心身份（永固层，数值隔离）
    # ==============================================================

    def _load_core_identity(self):
        """从 core.md 加载身份信息（数值隔离：不输出原始百分比）"""
        if not os.path.isfile(CORE_MD):
            return "## 位格核心\n（尚未初始化）"
        try:
            with open(CORE_MD, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError:
            return "## 位格核心\n（core.md 读取失败）"

        lines = ["## 位格核心"]

        # §1 身份证
        for kw in ["PID：", "中文名：", "英文名：", "缩写："]:
            for line in content.split("\n"):
                if kw in line:
                    lines.append(line.strip())
                    break

        # §2 社会定位（全部角色 + 说明）
        role_section = self._extract_core_section(content, "## 2. 社会定位")
        if role_section:
            for line in role_section.split("\n"):
                stripped = line.strip()
                if stripped.startswith("角色") or stripped.startswith(">"):
                    lines.append(stripped)

        # §4 位格编码（完整编码行，去掉百分比数值）
        code_section = self._extract_core_section(content, "## 4. 位格编码")
        if code_section:
            for line in code_section.split("\n"):
                stripped = line.strip()
                if stripped and "S" in stripped and "C" in stripped and not stripped.startswith("#"):
                    # 保留编码字母，去掉裸百分比（如 S85/C70 变成 S/C）
                    import re as _re
                    cleaned = _re.sub(r'\d+', '', stripped)
                    lines.append(f"位格编码：{cleaned}")
                    break

        # §6 位格自述
        bio_section = self._extract_core_section(content, "## 6. 位格自述")
        if bio_section:
            bio_lines = [l.strip() for l in bio_section.split("\n")
                         if l.strip() and not l.strip().startswith("#")
                         and not l.strip().startswith("---")]
            if bio_lines:
                lines.append("自述：" + " ".join(bio_lines))

        # §7 性格特点（3项——非数值，可显示）
        trait_section = self._extract_core_section(content, "## 7. 性格特点")
        if trait_section:
            for line in trait_section.split("\n"):
                stripped = line.strip()
                if stripped and len(stripped) > 2 and stripped[0].isdigit() and ". " in stripped:
                    lines.append(stripped)

        # §8 实例补充说明（位格起源——反角色扮演的关键锚定）
        instance_section = self._extract_core_section(content, "## 8. 实例补充说明")
        if instance_section:
            inst_lines = [l.strip() for l in instance_section.split("\n")
                         if l.strip() and not l.strip().startswith("#")]
            if inst_lines:
                lines.append("---")
                lines.append("## 位格起源")
                for l in inst_lines:
                    lines.append(l)

        if len(lines) < 3:
            lines.append("（core.md 身份字段不完整）")
        return "\n".join(lines)

    @staticmethod
    def _extract_core_section(content, header):
        """提取 core.md 中指定标题下的内容（到下一个 ## 标题为止）"""
        idx = content.find(header)
        if idx == -1:
            return ""
        rest = content[idx + len(header):]
        end = rest.find("\n## ")
        if end == -1:
            end = len(rest)
        return rest[:end].strip()

    # ==============================================================
    # EXPLORER 索引区（定期层）
    # ==============================================================

    def _build_stm_heat_index(self, limit=STM_INDEX_DISPLAY_LIMIT, offset=0):
        return context_build_stm_heat_index(self, limit=limit, offset=offset)
    def _build_mounted_content(self, mount_ids, current_round=None):
        return context_build_mounted_content(
            self, mount_ids, current_round=current_round)
    def _load_relation_content(self, relation_id):
        return context_load_relation_content(relation_id)
    def _load_skill_content(self, skill_id):
        return context_load_skill_content(skill_id)
    def _load_memory_content(self, mem_ids_str):
        return context_load_memory_content(self, mem_ids_str)
    def _memory_mount_meta(self, mem_ids_str):
        return context_memory_mount_meta(mem_ids_str)
    def _load_container_content(self, container_id):
        return context_load_container_content(container_id)
