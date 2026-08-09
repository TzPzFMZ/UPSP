"""Reaction step loop runner."""
from datetime import datetime

from constants import local_now
from engines.round_context import RoundContext
from engines.reaction_loop_main import ReactionLoopState, ReactionSession
from engines.reaction_guide_feedback import render_active_guide_feedback
from engines.reaction_terminal_state import (
    apply_reaction_handoff_relay_receipt,
    build_runtime_auto_continue_closeout,
    project_reaction_terminal_response,
    rhythm_guide_acceptance_feedback,
    rhythm_guide_closeout_acceptance,
    runtime_auto_blocked_final_response,
    task_acceptance_block_signature,
    task_acceptance_feedback,
    task_closeout_acceptance,
    validate_natural_final_reply_candidate,
)
from engines.reaction_runtime_guards import (
    clear_provider_interruption_recovery_state,
    provider_interruption_kind,
    receipt_counts_as_provider_recovery_progress,
    provider_recovery_next_count,
    recover_provider_interruption_if_possible,
)
from engines.reaction_protocol_tool_execution import (
    reasoning_context_native_replay,
    visible_relation_body_ids_from_mounts,
)
from engines.product_committer import build_protocol_processor_state
from engines.runtime_services import EngineComponent
from logic.closeout_copy import closeout_final_reply_reminder
from logic.evolution_set import build_evolution_context, summarize_pending
from logic.interaction_meta import cache_interaction_meta
from logic.protocol_tools import normalize_tool_id
from logic.reaction_time_policy import reaction_time_milestone_seconds
from logic.sandbox_grant import load_sandbox_grant
from logic.reaction_resident_guide import reaction_loop_resident_feedback
from logic.task_board import render_task_execution_action_guide
from logic.rhythm_guidance import current_guide
from logic.rhythm_guide_materializer import (
    materialize_current_rhythm_guide,
    reconcile_recovered_emergency_flags,
)
from schemas.context import context_safe_read_tool_result
from errors import RequiredContextError
from engines.reaction_helpers import (
    format_protocol_tool_fact,
    format_protocol_tool_material_entry,
    protocol_receipt_should_enter_tool_fact,
)


GUIDE_CALENDAR_LAYER_TO_FLAG = {
    "daily": "calendar_day_due",
    "weekly": "calendar_week_due",
    "monthly": "calendar_month_due",
    "quarterly": "calendar_quarter_due",
    "yearly": "calendar_year_due",
}
GUIDE_CALENDAR_FLAG_TO_LAYER = {
    flag: layer for layer, flag in GUIDE_CALENDAR_LAYER_TO_FLAG.items()
}
GUIDE_CALENDAR_SOURCE_LAYER = {
    "daily": "rhythms",
    "weekly": "daily",
    "monthly": "weekly",
    "quarterly": "monthly",
    "yearly": "quarterly",
}
GUIDE_ALERT_FLAGS = {
    "api_degraded",
    "token_usage_warning",
    "context_pressure",
}

class ReactionLoopRunner(EngineComponent):
    # 循环 helper 只做判断、投影和反馈文本；主循环状态机仍留在本类。
    def _sync_chronicle_focus_for_current_guide(
            self,
            *,
            round_type,
            current_state,
            round_num,
            completed_flags):
        """让 chronicle_write 的隐藏写入焦点跟随当前 Runtime GUIDE。"""
        if str(round_type or "").strip().lower() != "rhythm":
            self.chronicle_focus = None
            return {}
        try:
            guide_id = str(
                self.workbench.get("base.active_guides.rhythm") or ""
            ).strip()
            guide = self.workbench.load_guide(guide_id) if guide_id else {}
        except Exception:
            guide = {}
        kind = str(guide.get("kind") or "").strip()
        if kind == "main_axis_rhythm_guide":
            return getattr(self, "chronicle_focus", None) or {}
        if kind != "calendar_rhythm_guide":
            self.chronicle_focus = None
            return {}
        target = {}
        completed = {str(item or "").strip() for item in completed_flags or []}
        for item in guide.get("items") or []:
            flag = str(item.get("item_id") or "").strip()
            status = str(item.get("status") or "open").strip().lower()
            if flag and flag not in completed and status not in {"completed", "done"}:
                target = item
                break
        flag = str(target.get("item_id") or "").strip()
        layer = GUIDE_CALENDAR_FLAG_TO_LAYER.get(flag, "")
        if not layer:
            self.chronicle_focus = None
            return {}
        store = getattr(self, "chronicle_store", None)
        if store is None:
            try:
                from data.chronicle_store import ChronicleStore
                store = ChronicleStore()
                self.chronicle_store = store
            except Exception:
                self.chronicle_focus = None
                return {}
        closed_at = local_now().isoformat()
        title = str(target.get("title") or flag or layer).strip()
        source_layer = GUIDE_CALENDAR_SOURCE_LAYER.get(layer, "")
        try:
            refresher = getattr(store, "refresh_active_calendar", None)
            if callable(refresher):
                path = refresher(
                    layer=layer,
                    title=title,
                    round_num=round_num,
                    closed_at=closed_at,
                    source_layer=source_layer,
                )
            else:
                path = ""
        except Exception:
            self.chronicle_focus = None
            return {}
        focus = {
            "layer": layer,
            "path": path,
            "round_num": int(round_num or 0),
            "round_type": "rhythm",
            "calendar_flag": flag,
            "title": title,
            "source_layer": source_layer,
            "source_refs": [f"calendar:{flag}", f"round:{int(round_num or 0)}"],
            "range_end_round": int(round_num or 0),
            "range_end_time": closed_at,
        }
        self.chronicle_focus = focus
        return focus

    def _materialize_next_runtime_rhythm_guide_if_needed(
            self,
            current_state,
            round_type,
            round_num,
            completed_flags):
        base = (
            (current_state or {}).get("base", {})
            if isinstance(current_state, dict)
            else {}
        )
        flags = base.get("heartbeat_flags", {}) if isinstance(base, dict) else {}
        if not isinstance(flags, dict):
            return None
        try:
            flags, _cleared = reconcile_recovered_emergency_flags(
                flags,
                state_store=self.sm,
                connectivity_store=self.connectivity_store,
            )
            if isinstance(base, dict):
                base["heartbeat_flags"] = flags
            return materialize_current_rhythm_guide(
                self.workbench,
                flags,
                round_num=round_num,
                completed_flags=completed_flags,
                context_store=self.ctx_store,
                state_store=self.sm,
                connectivity_store=self.connectivity_store,
            )
        except Exception:
            return None

    def _chronicle_focus_content_projection(self):
        focus = getattr(self, "chronicle_focus", None)
        if not isinstance(focus, dict) or not focus:
            return {}
        path = str(focus.get("path") or "").strip()
        if not path:
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read().strip()
        except OSError:
            content = ""
        if not content:
            return {}
        return {
            "role": "user",
            "content": "\n".join([
                "## 编年史写入焦点（Runtime 预填）",
                f"- layer: {focus.get('layer')}",
                f"- path: {path}",
                f"- title: {focus.get('title') or ''}",
                "- 工具约束：chronicle_write 只填写正文，Runtime 使用本焦点决定写入层与路径。",
                "",
                "### 当前正文写入框",
                content,
            ]).rstrip(),
        }

    def _append_to_context_cache(self, *args, **kwargs):
        stores = []
        primary = getattr(self, "ctx_store", None)
        if primary is not None:
            stores.append(primary)
        assembler_store = getattr(getattr(self, "assembler", None), "context_store", None)
        if assembler_store is not None and not any(
                assembler_store is store for store in stores):
            stores.append(assembler_store)
        wrote = bool(stores)
        for store in stores:
            try:
                store.append_to_cache(*args, **kwargs)
            except Exception:
                wrote = False
        return wrote

    def _append_reasoning_context(self, *args, **kwargs):
        """将接口推理续接写为下一次 reaction 专用的 C 轨语料。"""
        stores = []
        primary = getattr(self, "ctx_store", None)
        if primary is not None:
            stores.append(primary)
        assembler_store = getattr(getattr(self, "assembler", None), "context_store", None)
        if assembler_store is not None and not any(
                assembler_store is store for store in stores):
            stores.append(assembler_store)
        wrote = False
        for store in stores:
            try:
                store.append_reasoning_context(*args, **kwargs)
                wrote = True
            except Exception:
                pass
        return wrote

    def _clear_consumed_reasoning_replay(self, round_num, iteration):
        """仅在目标 reaction 成功返回后清除已消费的 C 轨推理续接。"""
        stores = []
        primary = getattr(self, "ctx_store", None)
        if primary is not None:
            stores.append(primary)
        assembler_store = getattr(getattr(self, "assembler", None), "context_store", None)
        if assembler_store is not None and not any(
                assembler_store is store for store in stores):
            stores.append(assembler_store)
        reports = []
        for store in stores:
            try:
                reports.append(store.clear_transient_entries(
                    round_num=round_num,
                    transient_scope="reasoning_replay",
                    transient_target_step="reaction",
                    transient_target_iteration=iteration,
                ))
            except Exception:
                pass
        return reports

    @staticmethod
    def _provider_interruption_kind(exc):
        return provider_interruption_kind(exc)

    @staticmethod
    def _receipt_counts_as_provider_recovery_progress(receipt):
        return receipt_counts_as_provider_recovery_progress(receipt)

    def _provider_recovery_next_count(self, kind):
        return provider_recovery_next_count(self.sm, self.workbench, kind)

    def _clear_provider_interruption_recovery_state(self):
        clear_provider_interruption_recovery_state(self.sm)

    def _recover_provider_interruption_if_possible(
            self,
            exc,
            *,
            round_num,
            iteration,
            general_tool_results=None,
            protocol_receipts=None,
            guide_submit_receipts=None,
            memory_write_receipts=None,
            interaction_meta=None):
        return recover_provider_interruption_if_possible(
            exc,
            state_manager=self.sm,
            workbench=self.workbench,
            append_to_context_cache=self._append_to_context_cache,
            round_num=round_num,
            iteration=iteration,
            general_tool_results=general_tool_results,
            protocol_receipts=protocol_receipts,
            guide_submit_receipts=guide_submit_receipts,
            memory_write_receipts=memory_write_receipts,
            interaction_meta=interaction_meta,
        )

    @staticmethod
    def _receipt_counts_as_relay_execution(receipt):
        if not isinstance(receipt, dict):
            return False
        tool_id = normalize_tool_id(receipt.get("tool_id", ""))
        if tool_id in {"reaction_finalize", "reaction_loop", "final_reply"}:
            return False
        status = str(receipt.get("status") or "").strip().lower()
        if not status:
            return False
        if any(token in status for token in (
                "rejected", "invalid", "failed", "error", "blocked",
                "missing", "skipped", "dropped")):
            return False
        return status in {
            "ok",
            "accepted",
            "applied",
            "updated",
            "created",
            "completed",
            "written",
            "settled",
            "kept",
            "added",
        }

    @classmethod
    def _relay_iteration_has_execution_progress(
            cls, parsed_reaction, general_tool_results, protocol_receipts):
        parsed_reaction = parsed_reaction or {}
        if str(parsed_reaction.get("assistant_progress") or "").strip():
            return True
        if any(cls._receipt_counts_as_relay_execution(item)
               for item in general_tool_results or []):
            return True
        if any(cls._receipt_counts_as_relay_execution(item)
               for item in protocol_receipts or []):
            return True
        return False

    @staticmethod
    def _emergency_tool_attempt_count(iter_protocol_submissions, iter_general_tool_requests):
        """统计紧急处理指南下本迭代实际消耗的非终端工具动作。"""
        terminal_tools = {
            "reaction_finalize",
            "final_reply",
        }
        protocol_count = len([
            tool_id for tool_id in (iter_protocol_submissions or [])
            if normalize_tool_id(tool_id)
            and normalize_tool_id(tool_id) not in terminal_tools
        ])
        general_count = len([
            request for request in (iter_general_tool_requests or [])
            if isinstance(request, dict)
            and normalize_tool_id(request.get("tool_id"))
        ])
        return protocol_count + general_count

    @staticmethod
    def _alert_settled_in_iteration(alert_type, receipts):
        alert_type = str(alert_type or "").strip()
        for receipt in receipts or []:
            if not isinstance(receipt, dict):
                continue
            if str(receipt.get("alert_type") or "").strip() != alert_type:
                continue
            if str(receipt.get("status") or "").strip() == "applied":
                return True
        return False

    @staticmethod
    def _current_runtime_guide_pending_flags(state, completed_flags=None):
        flags = (
            (state or {}).get("base", {}).get("heartbeat_flags", {})
            if isinstance(state, dict)
            else {}
        )
        completed = {
            str(flag or "").strip()
            for flag in (completed_flags or [])
            if str(flag or "").strip()
        }
        guide = current_guide(flags, completed_flags=completed)
        pending = []
        for item in guide.get("items") or []:
            flag = str(item.get("flag") or "").strip()
            if flag and flag not in completed:
                pending.append(flag)
        return guide, pending

    @classmethod
    def _guide_completed_flags_from_receipts(
            cls,
            receipts,
            *,
            state=None,
            round_type="",
            completed_flags=None):
        completed = set()
        known_completed = {
            str(flag or "").strip()
            for flag in (completed_flags or [])
            if str(flag or "").strip()
        }
        for receipt in receipts or []:
            if not isinstance(receipt, dict):
                continue
            tool_id = str(receipt.get("tool_id") or "").strip()
            status = str(receipt.get("status") or "").strip().lower()
            if tool_id == "guide_submit":
                for flag in receipt.get("completed_flags") or []:
                    flag = str(flag or "").strip()
                    if flag:
                        completed.add(flag)
                completed.update(cls._guide_completed_flags_from_receipts(
                    receipt.get("backend_receipts") or [],
                    state=state,
                    round_type=round_type,
                    completed_flags=known_completed | completed,
                ))
                continue
            if tool_id == "chronicle_write":
                layer = str(receipt.get("layer") or "").strip()
                receipt_round_type = str(receipt.get("round_type") or "").strip()
                if status == "applied":
                    if layer == "rhythms" and receipt_round_type == "rhythm":
                        completed.add("rhythm_due")
                    calendar_flag = GUIDE_CALENDAR_LAYER_TO_FLAG.get(layer)
                    if calendar_flag:
                        completed.add(calendar_flag)
            elif tool_id == "alert_mode_settle":
                if status != "applied":
                    continue
                alert_type = str(receipt.get("alert_type") or "").strip()
                alert_status = str(
                    receipt.get("alert_status")
                    or receipt.get("alert_status_requested")
                    or receipt.get("status_requested")
                    or receipt.get("alert_mode_status")
                    or receipt.get("settle_status")
                    or receipt.get("mode")
                    or ""
                ).strip()
                cleared_flags = (
                    receipt.get("cleared_flags")
                    or receipt.get("clear_flags")
                    or []
                )
                if alert_type in GUIDE_ALERT_FLAGS and (
                        alert_status in {"recovered", "deferred", "needs_human"}
                        or cleared_flags):
                    completed.add(alert_type)
                for flag in cleared_flags:
                    flag = str(flag or "").strip()
                    if flag in GUIDE_ALERT_FLAGS:
                        completed.add(flag)
            elif tool_id == "fault_record":
                if status != "applied":
                    continue
                guide, pending = cls._current_runtime_guide_pending_flags(
                    state,
                    completed_flags=known_completed | completed,
                )
                if guide.get("kind") == "emergency_handling_guide" and pending:
                    completed.add(pending[0])
        return completed

    def _active_guide_protocol_tools(self):
        try:
            if hasattr(self.workbench, "current_active_guide_id"):
                guide_id = str(self.workbench.current_active_guide_id() or "").strip()
            else:
                guide_id = str(self.workbench.get("base.active_guide") or "").strip()
            if not guide_id:
                return []
            self.workbench.load_active_guide()
            return ["guide_submit"]
        except Exception:
            return []

    def _active_general_tool_guides(self):
        guide_ids = []
        try:
            if hasattr(self.workbench, "current_active_guide_id"):
                guide_id = str(self.workbench.current_active_guide_id() or "").strip()
            else:
                guide_id = str(self.workbench.get("base.active_guide") or "").strip()
            if guide_id:
                guide_ids.append(guide_id)
            if hasattr(self.workbench, "active_guide_slots"):
                slots = self.workbench.active_guide_slots()
                for value in (slots or {}).values():
                    value = str(value or "").strip()
                    if value:
                        guide_ids.append(value)
        except Exception:
            return []
        return list(dict.fromkeys(guide_ids))

    def _reaction_resident_guide_feedback(self, *, suppress_task_entry=False):
        if suppress_task_entry:
            return ""
        try:
            slots = (
                self.workbench.active_guide_slots()
                if hasattr(self.workbench, "active_guide_slots")
                else {}
            )
            rhythm_guide = str((slots or {}).get("rhythm") or "").strip()
            active_guide = (
                str(self.workbench.current_active_guide_id() or "").strip()
                if hasattr(self.workbench, "current_active_guide_id")
                else str(self.workbench.get("base.active_guide") or "").strip()
            )
            if rhythm_guide and active_guide == rhythm_guide:
                return ""
            work_guide = str((slots or {}).get("work") or "").strip()
            if work_guide:
                return ""
        except Exception:
            pass
        return reaction_loop_resident_feedback()

    @staticmethod
    def _suppress_workbench_guides(round_type, state):
        if str(round_type or "").strip().lower() != "rhythm":
            return False
        base = (state or {}).get("base", {}) if isinstance(state, dict) else {}
        flags = base.get("heartbeat_flags", {}) if isinstance(base, dict) else {}
        if not flags.get("user_message_waiting"):
            return False
        runtime = base.get("runtime", {}) if isinstance(base, dict) else {}
        completed = (
            runtime.get("guide_completed_flags", [])
            if isinstance(runtime, dict)
            else []
        )
        guide = current_guide(flags, completed_flags=completed)
        return str(guide.get("kind") or "").strip() in {
            "main_axis_rhythm_guide",
            "calendar_rhythm_guide",
            "emergency_handling_guide",
            "context_pressure_rhythm_guide",
            "cache_compaction_rhythm_guide",
        }

    def _should_suppress_active_guide_feedback(self, round_type, state):
        if not self._suppress_workbench_guides(round_type, state):
            return False
        try:
            slots = (
                self.workbench.active_guide_slots()
                if hasattr(self.workbench, "active_guide_slots")
                else {}
            )
            active = (
                self.workbench.current_active_guide_id()
                if hasattr(self.workbench, "current_active_guide_id")
                else self.workbench.get("base.active_guide")
            )
        except Exception:
            return True
        active = str(active or "").strip()
        rhythm = str((slots or {}).get("rhythm") or "").strip()
        if active and rhythm and active == rhythm:
            return False
        return True

    def _active_guide_feedback(self):
        try:
            guide = self.workbench.load_active_guide()
        except Exception:
            return ""
        return render_active_guide_feedback(
            guide,
            workbench=self.workbench,
            task_execution_renderer=render_task_execution_action_guide,
        )

    def _task_closeout_acceptance(self, closeout_form):
        return task_closeout_acceptance(
            self.sm,
            self.workbench,
            closeout_form,
        )

    @staticmethod
    def _task_acceptance_feedback(result):
        return task_acceptance_feedback(result)

    @staticmethod
    def _task_acceptance_block_signature(result):
        return task_acceptance_block_signature(result)

    def _rhythm_guide_closeout_acceptance(
            self,
            closeout_form,
            state,
            round_type,
            completed_flags):
        return rhythm_guide_closeout_acceptance(
            closeout_form,
            state,
            round_type,
            completed_flags=completed_flags,
            current_runtime_guide_pending_flags=(
                self._current_runtime_guide_pending_flags),
        )

    @staticmethod
    def _rhythm_guide_acceptance_feedback(result):
        return rhythm_guide_acceptance_feedback(result)

    def _validate_natural_final_reply_candidate(
            self,
            *,
            reaction_obligations,
            write_pending_tracker,
            current_state,
            round_type,
            runtime_guide_completed_flags,
            prior_general_tool_results=None,
            reminded_unfinished_read_signatures=None):
        obligation_state = (
            reaction_obligations.audit_state()
            if reaction_obligations is not None
            else {}
        )
        return validate_natural_final_reply_candidate(
            closeout_form_validator=(
                reaction_obligations.validate_closeout_form),
            write_pending_tracker=write_pending_tracker,
            current_state=current_state,
            round_type=round_type,
            runtime_guide_completed_flags=runtime_guide_completed_flags,
            current_runtime_guide_pending_flags=(
                self._current_runtime_guide_pending_flags),
            task_closeout_acceptance=self._task_closeout_acceptance,
            prior_general_tool_results=prior_general_tool_results,
            unfinished_file_reads=(
                obligation_state.get("unfinished_file_reads", [])
                if isinstance(obligation_state, dict)
                else []
            ),
            reminded_unfinished_read_signatures=(
                reminded_unfinished_read_signatures
            ),
        )

    @staticmethod
    def _emergency_attempt_notice(alert_type, attempts):
        return (
            "紧急处理循环提醒："
            f"{alert_type} 已经消耗 {attempts} 次工具动作仍未结算。"
            "如果当前无法恢复，直接用 alert_mode_settle(status=\"deferred\") 搁置，"
            "不要继续困在同一个无解处理里。"
        )

    @staticmethod
    def _relay_execution_missing_feedback(correction_count):
        return (
            "relay_execution_missing: 本轮是中继执行轮，不是确认上轮或向用户复述计划。"
            "你请求继续中继，但本轮还没有新的可审计推进证据。"
            "请先执行交接里的第一动作；若现实上不能执行，就直接自然语言说明当前阻断，"
            "不要再次请求继续中继。"
            "如果当前确实只是等待用户新指令，下一迭代直接自然语言回复用户；"
            "只有确需跨轮继续时，才调用 reaction_finalize(handoff_text)。"
            f"本提示是第 {correction_count}/2 次纠偏。"
        )

    @staticmethod
    def _relay_target_unfulfilled_receipt(source, target, correction_count):
        return {
            "tool_id": "reaction_finalize",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": "relay_target_unfulfilled",
            "source": source,
            "target": target or {},
            "reasons": ["pending_relay_target_not_satisfied"],
            "correction_count": correction_count,
        }

    def run(self, *args, **kwargs):
        if args and isinstance(args[0], RoundContext):
            context = args[0]
            setup_result = args[1]
            return self._run_loop(
                context.state,
                context.round_type,
                setup_result.intent.get("mount_requests", []),
                interaction_meta=setup_result.interaction_meta,
                trigger_id=(
                    context.trigger.trigger_id if context.trigger else ""),
                caused_by=(
                    setup_result.frame_ref.frame_id
                    if setup_result.frame_ref else ""),
                topology_version=context.topology_version,
            )
        return self._run_loop(*args, **kwargs)

    def _run_loop(
            self, state, round_type, mount_ids, interaction_meta=None,
            trigger_id="", caused_by="", topology_version=""):
        session = self.start_session(
            state,
            round_type,
            mount_ids,
            interaction_meta=interaction_meta,
            trigger_id=trigger_id,
            caused_by=caused_by,
            topology_version=topology_version,
        )
        while not session.completed:
            self.run_frame(session)
        return session.result

    def start_session(
            self, state, round_type, mount_ids, interaction_meta=None,
            trigger_id="", caused_by="", topology_version=""):
        return ReactionSession(ReactionLoopState(
            runner=self,
            state=state,
            round_type=round_type,
            mount_ids=mount_ids,
            interaction_meta=interaction_meta,
            trigger_id=trigger_id,
            caused_by=caused_by,
            topology_version=topology_version,
        ))

    @staticmethod
    def run_frame(session):
        return session.run_frame()

    @staticmethod
    def _reaction_time_feedback(elapsed_seconds=0, time_limit_seconds=600):
        try:
            elapsed = max(0.0, float(elapsed_seconds or 0))
        except (TypeError, ValueError):
            return ""
        reminder_at, warning_at, _auto_relay_at = reaction_time_milestone_seconds(
            time_limit_seconds
        )
        minutes = int(elapsed // 60)
        if elapsed >= warning_at:
            return (
                f"【时间警告】本轮已运行约{minutes}分钟，进入收束阶段。"
                "请停止无效扩张，只保留完成当前事务、验证、证据补齐、必要 memory_write；"
                "若形成稳定工作方法、关系变化或判断口径，可短写 memory_write。"
                "完成就直接自然语言回复用户；确需跨轮继续才调用 reaction_finalize(handoff_text)。"
                f"{closeout_final_reply_reminder()}"
            )
        if elapsed >= reminder_at:
            return (
                f"【时间提醒】本轮已运行约{minutes}分钟。"
                "保持在场：请停止无效扩张，优先推进并完成当前事务；"
                "若已经形成会影响后续判断、协作、关系或工作方法的稳定沉淀，可短调用 memory_write；"
                "如果已完成就直接自然语言回复用户；需要跨轮继续就 "
                "调用 reaction_finalize(handoff_text)，可与最后一批工具同次提交。"
                f"{closeout_final_reply_reminder()}"
                "必要工具仍可继续使用，但行动应服务于完成回复或跨轮中继。"
            )
        return ""

    def _build_runtime_auto_continue_closeout(
            self, *, elapsed_seconds=0, time_limit_seconds=600):
        return build_runtime_auto_continue_closeout(
            self.sm,
            elapsed_seconds=elapsed_seconds,
            time_limit_seconds=time_limit_seconds,
        )

    @staticmethod
    def _runtime_auto_blocked_final_response(settlement_ledgers=None):
        return runtime_auto_blocked_final_response(settlement_ledgers)

    @staticmethod
    def _project_reaction_terminal_response(
            settlement_ledgers=None,
            assistant_text=""):
        return project_reaction_terminal_response(
            settlement_ledgers,
            assistant_text,
        )

    def _existing_stm_memory_ids(self):
        try:
            return set(self.memory_store.list_entries())
        except Exception:
            return set()

    def _boost_mounted_memory_once(
            self, mem_id, round_num, boosted_memory_ids, memory_layer="STM"):
        mem_id = str(mem_id or "").strip()
        if not mem_id or mem_id in boosted_memory_ids:
            return
        try:
            if str(memory_layer or "STM").strip() == "STM":
                self.heat.recall_boost(mem_id, round_num=round_num)
            else:
                self.memory_store.mark_recalled(mem_id, round_num=round_num)
            boosted_memory_ids.add(mem_id)
        except Exception:
            pass

    def _build_evolution_reaction_context(self, state):
        flags = state.get("base", {}).get("heartbeat_flags", {}) if isinstance(state, dict) else {}
        flagged = flags.get("evolution_pending")
        thresholds = self._load_evolution_thresholds()
        if not flagged and not self.evolution_store.should_trigger(thresholds):
            return "", None
        pending = self.evolution_store.load_pending()
        tacit_records = pending.get("tacit", [])
        connection_records = pending.get("connection", [])
        if not tacit_records and not connection_records:
            return "", None
        stats = summarize_pending(tacit_records, connection_records)
        return build_evolution_context(stats, tacit_records, connection_records), stats

    def _load_evolution_thresholds(self):
        try:
            if self.cfg:
                return self.cfg.get_autonomous_trigger_params()
        except Exception:
            pass
        return {
            "tacit_pending_threshold": 512,
            "connection_pending_threshold": 512,
        }

    def _apply_reaction_identity_resolution(self, resolution, interaction_meta):
        current = dict(interaction_meta or {})
        interaction_object = str(resolution.get("interaction_object") or "").strip()
        identity_status = str(resolution.get("identity_status") or "known").strip()
        interaction_source = str(
            resolution.get("interaction_source") or "context_continuity").strip()
        if not interaction_object or interaction_object == "unknown":
            return current
        updated = dict(current)
        updated.update({
            "interaction_object": interaction_object,
            "identity_status": identity_status,
            "interaction_source": interaction_source,
        })
        try:
            self.sm.confirm_identity()
        except Exception:
            pass
        return updated

    def _apply_closeout_relay_receipt(self, relay_receipt, trace=None):
        return apply_reaction_handoff_relay_receipt(
            self.sm,
            relay_receipt,
            trace=trace,
        )

    def _identity_blocked_native_requests(self, envelopes):
        invalids = []
        for envelope in envelopes or []:
            if not isinstance(envelope, dict):
                continue
            tool_id = normalize_tool_id(envelope.get("tool_id", ""))
            if tool_id == "reaction_finalize":
                continue
            item = {}
            for key in (
                    "tool_id",
                    "tool_family",
                    "tool_class",
                    "risk",
                    "source",
                    "call_id",
                    "provider",
                    "response_id",
                    "provider_item_id",
                    "index"):
                value = envelope.get(key)
                if value not in (None, ""):
                    item[key] = value
            if not item.get("call_id"):
                continue
            item["reason"] = "identity_unresolved"
            item["source"] = "reaction_identity_gate"
            invalids.append(item)
        return invalids

    @staticmethod
    def _visible_relation_body_ids_from_mounts(mount_ids):
        return visible_relation_body_ids_from_mounts(mount_ids)

    def _build_protocol_processor_state(self, interaction_meta):
        return build_protocol_processor_state(self.services, interaction_meta)

    def _write_reaction_tool_summaries(self, summaries, round_num, iteration, interaction_meta):
        """Spec276: model-written tool summaries no longer enter cache."""
        return None

    def _write_reaction_progress(
            self, message_envelope, round_num, iteration, interaction_meta):
        """Persist loop assistant_text as dialogue progress, separate from tool facts."""
        if (message_envelope or {}).get("terminal_text_candidate"):
            return None
        if str((message_envelope or {}).get("phase") or "").strip() != "loop":
            return None
        text = str((message_envelope or {}).get("text") or "").strip()
        if not text:
            return None
        cache_meta = cache_interaction_meta(interaction_meta)
        try:
            self._append_to_context_cache(
                round_num,
                "assistant",
                text,
                kind="dialogue_progress",
                step="reaction",
                iter=iteration,
                message_channel=str(
                    (message_envelope or {}).get("channel") or "assistant_text"),
                message_envelope=message_envelope,
                **cache_meta,
            )
            return {"status": "applied", "chars": len(text)}
        except Exception:
            return None

    def _write_reasoning_context_if_needed(
            self,
            envelopes,
            general_results,
            protocol_receipts,
            round_num,
            iteration,
            interaction_meta):
        reasoning_texts = [
            str(envelope.get("reasoning_content") or "").strip()
            for envelope in envelopes or []
            if isinstance(envelope, dict)
            and str(envelope.get("reasoning_content") or "").strip()
        ]
        if not reasoning_texts:
            return None
        native_replay = reasoning_context_native_replay(
            envelopes,
            general_results,
            protocol_receipts,
        )
        if not native_replay:
            return None
        cache_meta = cache_interaction_meta(interaction_meta)
        try:
            wrote = self._append_reasoning_context(
                round_num,
                "【本轮推理上下文】\n\n" + reasoning_texts[0],
                step="reaction",
                iter=iteration,
                native_replay=native_replay,
                **cache_meta,
            )
            if wrote:
                return {"status": "applied", "chars": len(reasoning_texts[0])}
            return {
                "status": "failed",
                "reason": "context_store_write_failed",
                "chars": len(reasoning_texts[0]),
            }
        except Exception:
            return {
                "status": "failed",
                "reason": "context_store_write_failed",
                "chars": len(reasoning_texts[0]),
            }

    def _write_general_tool_results(self, results, round_num, iteration, interaction_meta):
        """Write model-visible tool facts and read materials as separate blocks."""
        cache_meta = cache_interaction_meta(interaction_meta)
        task_root = ""
        try:
            sandbox_grant = load_sandbox_grant()
            task_root = str((sandbox_grant or {}).get("task_root") or "").strip()
        except Exception:
            task_root = ""
        for result in results or []:
            self._record_source_read_evidence(result, round_num, iteration, task_root)
            try:
                wrote = self._append_to_context_cache(
                    round_num,
                    "system",
                    self.general_tool_dispatcher.format_fact(result),
                    kind="tool_fact",
                    step="reaction",
                    iter=iteration,
                    tool_result=context_safe_read_tool_result(result),
                    **cache_meta,
                )
                if not wrote:
                    raise RuntimeError("context_store_write_failed")
            except Exception as exc:
                raise RequiredContextError(
                    "projection", "general_tool_fact", exc) from exc
            try:
                material = self.general_tool_dispatcher.format_material_entry(result)
                if not material:
                    continue
                wrote = self._append_to_context_cache(
                    round_num,
                    material.get("role") or "system",
                    material.get("content") or "",
                    kind="material",
                    step="reaction",
                    iter=iteration,
                    tool_result=context_safe_read_tool_result(result),
                    **cache_meta,
                )
                if not wrote:
                    raise RuntimeError("context_store_write_failed")
            except Exception as exc:
                raise RequiredContextError(
                    "projection", "general_tool_material", exc) from exc

    def _record_source_read_evidence(self, result, round_num, iteration, task_root=""):
        if not isinstance(result, dict):
            return
        tool_id = str(result.get("tool_id") or "").strip()
        if tool_id not in {"file_read", "web_fetch"}:
            return
        payload = {
            "tool_id": tool_id,
            "status": result.get("status"),
            "round": round_num,
            "iteration": iteration,
            "task_root": task_root,
        }
        if tool_id == "file_read":
            payload["path"] = result.get("path") or result.get("file_path")
        else:
            payload["url"] = result.get("url") or result.get("source_url")
        try:
            self.workbench.append_source_read_evidence(payload)
        except Exception as exc:
            raise RequiredContextError(
                "projection", "source_read_evidence", exc) from exc

    def _write_protocol_tool_receipts(self, receipts):
        """把协议工具结构化回执投影成短事实条和必要资料块。"""
        if not receipts:
            return
        lines = []
        for receipt in receipts:
            if not protocol_receipt_should_enter_tool_fact(receipt):
                continue
            fact = format_protocol_tool_fact(
                receipt,
                fact_context={"workbench_store": self.workbench},
            )
            if fact:
                lines.append(fact)
        if lines:
            try:
                wrote = self._append_to_context_cache(
                    self.sm.get_total_round(),
                    "system",
                    "\n".join(lines),
                    kind="tool_fact",
                    step="reaction",
                    protocol_receipts=receipts,
                )
                if not wrote:
                    raise RuntimeError("context_store_write_failed")
            except Exception as exc:
                raise RequiredContextError(
                    "projection", "protocol_tool_fact", exc) from exc
        for receipt in receipts:
            material = format_protocol_tool_material_entry(receipt)
            if not material:
                continue
            try:
                wrote = self._append_to_context_cache(
                    self.sm.get_total_round(),
                    material.get("role") or "system",
                    material.get("content") or "",
                    kind="material",
                    step="reaction",
                    protocol_receipt=receipt,
                )
                if not wrote:
                    raise RuntimeError("context_store_write_failed")
            except Exception as exc:
                raise RequiredContextError(
                    "projection", "protocol_tool_material", exc) from exc
