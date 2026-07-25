"""Cleanup step pipeline and slow metabolism boundary."""
import os
import re
from datetime import datetime, timedelta, timezone

from constants import STANDBY_COUNTDOWN_INITIAL, TZ_SHANGHAI
from errors import ProviderCallCancelled
from engines.cleanup_helpers import (
    append_ltm_index,
    extract_memory_field,
    ltm_has_entry,
    round_text,
    strip_memory_heading,
)
from engines.ltm_degradation import LTMDegradationManager
from engines.round_context import FrameRef, RoundContext
from engines.runtime_services import EngineComponent
from logic.interaction_meta import cache_interaction_meta
from logic.cleanup_processor import process_cleanup
from logic.native_tool_calls import project_step_finalize, terminal_finalize_from_envelopes
from logic.task_acceptance import DONE_ITEM_STATUSES, PASSED_ACCEPTANCE_STATUSES
from logic.evolution_set import (
    extract_evolution_blocks,
)
from logic.mem_id import make_meta_template
from logic.state_settlement import StateSettlementError, settle_state
from logic.relay_target import normalize_pending_target
from logic.relay_intent_pool import (
    create_relay_intent,
    open_relay_intents,
    settle_open_relay_intents,
)
from logic.write_pending_settlement import rhythm_chronicle_write_applied


CALENDAR_FLAG_TO_LAYER = {
    "calendar_day_due": "daily",
    "calendar_week_due": "weekly",
    "calendar_month_due": "monthly",
    "calendar_quarter_due": "quarterly",
    "calendar_year_due": "yearly",
}

ALERT_FLAG_NAMES = {
    "api_degraded",
    "process_down",
    "token_usage_warning",
    "context_pressure",
}


class CleanupPipeline(EngineComponent):
    @staticmethod
    def _cleanup_round_receipt_lines(result):
        result = result if isinstance(result, dict) else {}
        lines = []
        memory_write_receipts = result.get("_memory_write_receipts", [])
        if memory_write_receipts:
            lines.append("memory_write 同步回执：")
            for receipt in memory_write_receipts:
                lines.append(
                    "- {status} {mem_id} {title} keywords={keywords} reason={reason}".format(
                        status=receipt.get("status", ""),
                        mem_id=receipt.get("mem_id") or "无",
                        title=receipt.get("title") or "",
                        keywords=",".join(receipt.get("keywords") or []),
                        reason=receipt.get("reason") or "",
                    )
                )
        relation_receipts = result.get("_relation_card_receipts", [])
        if relation_receipts:
            lines.append("relation_card 同步回执：")
            for receipt in relation_receipts:
                lines.append(
                    "- {status} {card_id} reason={reason}".format(
                        status=receipt.get("status", ""),
                        card_id=receipt.get("card_id") or "无",
                        reason=receipt.get("reason") or "",
                    )
                )
        return lines

    @staticmethod
    def _redact_runtime_exception_text(text):
        text = str(text or "")
        text = re.sub(r"sk-[A-Za-z0-9_-]{8,}", "[REDACTED_SECRET]", text)
        text = re.sub(
            r"(?i)((?:api[_-]?key|authorization|bearer)\s*[:=]\s*)\S+",
            r"\1[REDACTED_SECRET]",
            text,
        )
        if len(text) > 2000:
            return text[:2000] + "...[truncated]"
        return text

    @classmethod
    def _runtime_exception_audit(cls, result):
        if not isinstance(result, dict):
            return None
        error_text = str(result.get("error") or "").strip()
        failed_phase = str(result.get("_failed_phase") or "").strip()
        aborted = bool(result.get("aborted"))
        step_exception_marker = " step exception:"
        if not (aborted or failed_phase or step_exception_marker in error_text):
            return None
        if not failed_phase and step_exception_marker in error_text:
            failed_phase = error_text.split(step_exception_marker, 1)[0].strip()
        failed_phase = failed_phase or "unknown"
        audit = {
            "status": "error",
            "aborted": aborted,
            "failed_phase": failed_phase,
        }
        if error_text:
            audit["error"] = cls._redact_runtime_exception_text(error_text)
        return audit

    def _build_cleanup_round_material(self, user_input_text, result):
        result = result if isinstance(result, dict) else {}
        parts = []
        user_input_text = str(user_input_text or "").strip()
        if user_input_text:
            parts.append(f"本轮用户输入：\n{user_input_text}")
        response = str(result.get("response") or "").strip()
        if response:
            parts.append(f"本轮反应步最终回复：\n{response}")
        receipt_lines = self._cleanup_round_receipt_lines(result)
        if receipt_lines:
            parts.append("\n".join(receipt_lines))
        return "\n\n---\n\n".join(parts).strip()

    def _write_cleanup_round_material(self, round_num, user_input_text,
                                      result, cache_meta):
        content = self._build_cleanup_round_material(user_input_text, result)
        if not content:
            return None
        material_meta = dict(cache_meta or {})
        material_meta["interaction_source"] = "cleanup_round_material"
        try:
            self.ctx_store.append_cleanup_round_material(
                round_num,
                content,
                **material_meta,
            )
            if isinstance(result, dict):
                result["_cleanup_round_material_written"] = True
            return {"status": "applied", "chars": len(content)}
        except Exception as exc:
            if isinstance(result, dict):
                result["_cleanup_round_material_error"] = str(exc)
            return {"status": "error", "reason": str(exc)}

    def _write_cleanup_task_material(self, round_num, title, content,
                                     interaction_source, result=None):
        content = str(content or "").strip()
        if not content:
            return None
        title = str(title or "善后内部任务").strip() or "善后内部任务"
        material = f"善后内部任务：{title}\n\n{content}"
        meta = {
            "interaction_object": "runtime",
            "identity_status": "system",
            "interaction_source": str(interaction_source or "cleanup_task"),
        }
        try:
            self.ctx_store.append_cleanup_round_material(
                round_num,
                material,
                **meta,
            )
            if isinstance(result, dict):
                result.setdefault("_cleanup_task_materials_written", []).append({
                    "source": meta["interaction_source"],
                    "title": title,
                    "chars": len(material),
                })
            return {"status": "applied", "chars": len(material)}
        except Exception as exc:
            if isinstance(result, dict):
                result.setdefault("_cleanup_task_material_errors", []).append({
                    "source": meta["interaction_source"],
                    "title": title,
                    "reason": str(exc),
                })
            return {"status": "error", "reason": str(exc)}

    def _clear_cleanup_round_material(self, round_num, result=None):
        try:
            report = self.ctx_store.clear_transient_entries(
                round_num=round_num,
                transient_scope="cleanup_round",
                transient_target_step="cleanup",
            )
            if isinstance(result, dict):
                result["_cleanup_round_material_clear"] = report
            return report
        except Exception as exc:
            if isinstance(result, dict):
                result["_cleanup_round_material_clear_error"] = str(exc)
            return {"status": "error", "reason": str(exc)}

    def _prepare_lately_compression_pending(self, result, round_num):
        try:
            cache_stats = self.ctx_store.get_last_cache_stats()
            if not cache_stats.get("lately_trimmed"):
                return None
            compact_ratio = self.ctx_store.get_lately_compact_ratio()
            compact_ratio = min(1.0, max(0.0, float(compact_ratio)))
            if compact_ratio >= 1.0:
                return None
            candidates = list(
                self.ctx_store.build_lately_compression_candidates(
                    max_blocks=None,
                ) or []
            )
            source_ids = [
                str(item.get("id") or "").strip()
                for item in candidates
                if str(item.get("id") or "").strip()
            ]
            if not source_ids:
                return None
            total_chars = sum(int(item.get("chars", 0) or 0) for item in candidates)
            pending = {
                "lately_trimmed": True,
                "compact_ratio": compact_ratio,
                "current_round": round_num,
                "source_block_ids": source_ids,
                "candidate_blocks": len(source_ids),
                "before_chars": total_chars,
                "target_chars": int(total_chars * compact_ratio),
            }
            if isinstance(result, dict):
                result["_lately_compression_pending"] = pending
            return pending
        except Exception as exc:
            if isinstance(result, dict):
                result["_lately_compression_pending_error"] = str(exc)
            return None

    @staticmethod
    def _lately_compression_popup(pending):
        if not pending:
            return ""
        ratio = pending.get("compact_ratio", 0.618)
        count = pending.get("candidate_blocks", 0)
        return (
            "【最近缓存压缩提醒】\n"
            f"最近缓存发生水位删除，删后幸存段仍在“最近缓存 lately”层，共 {count} 个语料块。"
            f"建议压缩比例为 {ratio}。本轮善后只置位 cache_compaction_due；"
            "下一轮由维护节律 guide 处理压缩，不要把压缩内容写成交接。"
        )

    def run(self, *args, **kwargs):
        if args and isinstance(args[0], RoundContext):
            context = args[0]
            result = args[1]
            return self._run_pipeline(
                context.round_type,
                context.state,
                result,
                context.round_num,
                user_input_text=context.user_input_text,
                close_round=False,
                trigger_id=(
                    context.trigger.trigger_id if context.trigger else ""),
                caused_by=getattr(context.setup_frame, "frame_id", ""),
                topology_version=context.topology_version,
                external_interaction=bool(
                    context.trigger and context.trigger.messages),
            )
        return self._run_pipeline(*args, **kwargs)

    def _run_pipeline(
            self, round_type, state, result, round_num, user_input_text="",
            close_round=True, trigger_id="", caused_by="",
            topology_version="", external_interaction=None):
        """善后步——不可跳过，不回滚。各子操作独立try，互不阻断"""
        cleanup_result = {"response": ""}
        resp_text = ""
        cleanup_iteration = 1
        degraded_reasons = []
        fatal_reasons = []
        user_stop = bool(
            isinstance(result, dict) and result.get("_user_stop_requested")
        )
        if user_stop:
            degraded_reasons.append("user_stopped")

        def set_stage(stage):
            callback = getattr(self, "stage_callback", None)
            if callable(callback):
                callback(stage)

        def failure_reason(scope, exc):
            return f"{scope}:{type(exc).__name__}:{self._redact_runtime_exception_text(exc)}"

        cleanup_caused_by = str(
            (result.get("_frame_settlements") or [{}])[-1].get("frame_id")
            or caused_by
        ) if isinstance(result, dict) else str(caused_by or "")

        def cleanup_frame_ref(sequence):
            return FrameRef.for_axis(
                round_num, "cleanup", sequence,
                trigger_id=trigger_id,
                caused_by=(
                    cleanup_caused_by if sequence == 1
                    else f"R{int(round_num):06d}:cleanup:{sequence - 1}"
                ),
                topology_version=topology_version, role_id="cleanup")

        frame_ref = cleanup_frame_ref(cleanup_iteration)

        # ① 热度衰减（独立——失败不阻断后续）
        if not user_stop:
            try:
                self.heat.tick_decay(round_num=round_num)
            except Exception as exc:
                fatal_reasons.append(failure_reason("heat_decay", exc))

        # ② 遗忘分流
        forgetting_text = ""
        if not user_stop:
            try:
                forgetting_text = self._build_forgetting_context()
            except Exception as exc:
                fatal_reasons.append(failure_reason("forgetting_context", exc))
        cleanup_task_materials = []
        if forgetting_text:
            cleanup_task_materials.append((
                "STM 遗忘压缩",
                "cleanup_forgetting_task",
                forgetting_text,
            ))
        # 日历日节律：LTM降格压缩任务作为善后步临时材料挂载。
        if round_type == "rhythm" and not user_stop:
            try:
                flags = state.get("base", {}).get("heartbeat_flags", {})
                if flags.get("calendar_day_due"):
                    self._prepare_ltm_degradation_for_day(round_num)
                    ltm_tasks = self._build_ltm_degradation_context()
                    if ltm_tasks:
                        cleanup_task_materials.append((
                            "LTM 降格处理",
                            "cleanup_ltm_degradation_task",
                            ltm_tasks,
                        ))
            except Exception as exc:
                fatal_reasons.append(failure_reason("ltm_degradation", exc))
        result["_user_input"] = user_input_text
        interaction_meta = result.get("_interaction_meta", {}) if isinstance(result, dict) else {}
        cache_meta = cache_interaction_meta(interaction_meta)
        if not user_stop:
            self._write_cleanup_round_material(
                round_num,
                user_input_text,
                result,
                cache_meta,
            )
            for title, source, content in cleanup_task_materials:
                self._write_cleanup_task_material(
                    round_num,
                    title,
                    content,
                    source,
                    result,
                )
        lately_pending = (
            self._prepare_lately_compression_pending(result, round_num)
            if not user_stop else None
        )
        cleanup_popup_fragments = []
        lately_popup = self._lately_compression_popup(lately_pending)
        if lately_popup:
            cleanup_popup_fragments.append(lately_popup)
        cleanup_result = {}
        executed_phases = ("setup", "reaction", "cleanup")
        if isinstance(result, dict):
            failed_phase = str(result.get("_failed_phase") or "").strip()
            error_text = str(result.get("error") or "")
            setup_failed_before_reaction = (
                failed_phase == "setup"
                or error_text.startswith("setup step exception:")
            )
            if (
                    result.get("_setup_reject_reason")
                    or result.get("standby_skipped_reaction")
                    or setup_failed_before_reaction):
                executed_phases = ("setup", "cleanup")
        try:
            audit_store = self._get_round_audit_store()
            for prior_phase in ("setup", "reaction"):
                if prior_phase not in executed_phases:
                    continue
                if not audit_store._has_step_snapshot(round_num, prior_phase):
                    audit_store.record_step_input_from_files(round_num, prior_phase)
        except Exception as exc:
            fatal_reasons.append(failure_reason("phase_input_audit", exc))

        # ③ 调API + 解析（用户停止时明确跳过，直接进入本地结算）
        try:
            if user_stop:
                set_stage("cleanup_local")
                cleanup_result = {}
                raise ProviderCallCancelled("cleanup_provider_skipped_after_user_stop")
            set_stage("cleanup_model")
            cleanup_assemble_kwargs = {}
            if cleanup_popup_fragments:
                cleanup_assemble_kwargs["popup_fragments"] = cleanup_popup_fragments
            organ_runtime = getattr(self, "organ_runtime", None)
            if organ_runtime is not None:
                organ_materials = organ_runtime.begin_frame_materials(frame_ref)
                if organ_materials:
                    cleanup_assemble_kwargs["material_inputs"] = organ_materials
            system, messages = self.assembler.assemble_cleanup(
                state, round_type, result,
                **cleanup_assemble_kwargs)

            cleanup_result = self._call_llm_with_round_audit(
                "cleanup",
                system,
                messages,
                round_num,
                iteration=1,
            )
            cleanup_iteration = 1
            retry_needed, parsed, terminal_invalids = (
                self._cleanup_finalize_retry_parse(cleanup_result, state, result)
            )
            while retry_needed and cleanup_iteration < 3:
                self._round_audit_parsed(round_num, "cleanup", cleanup_iteration, parsed)
                retry_settlement = self._cleanup_finalize_retry_settlement(
                    parsed,
                    terminal_invalids,
                )
                retry_settlement["frame_ref"] = cleanup_frame_ref(
                    cleanup_iteration).as_dict()
                self._round_audit_settlement(
                    round_num,
                    "cleanup",
                    cleanup_iteration,
                    retry_settlement,
                )
                messages = self._cleanup_finalize_retry_messages(
                    messages,
                    terminal_invalids,
                )
                try:
                    next_iteration = cleanup_iteration + 1
                    cleanup_result = self._call_llm_with_round_audit(
                        "cleanup",
                        system,
                        messages,
                        round_num,
                        iteration=next_iteration,
                    )
                except Exception as retry_exc:
                    interrupted = (
                        self._cleanup_finalize_retry_interrupted_settlement(
                            parsed,
                            terminal_invalids,
                            retry_exc,
                        ))
                    interrupted["frame_ref"] = cleanup_frame_ref(
                        cleanup_iteration + 1).as_dict()
                    self._round_audit_settlement(
                        round_num,
                        "cleanup",
                        cleanup_iteration + 1,
                        interrupted,
                    )
                    raise
                cleanup_iteration = next_iteration
                retry_needed, parsed, terminal_invalids = (
                    self._cleanup_finalize_retry_parse(cleanup_result, state, result)
                )
            if retry_needed:
                cleanup_result = {
                    "response": "",
                    "tool_call_envelopes": [],
                    "_terminal_finalize_missing": True,
                    "_terminal_finalize_issue": "cleanup_finalize_missing_after_retry",
                    "_terminal_invalids": list(terminal_invalids or []),
                }
            self._isolate_cleanup_natural_response(cleanup_result)
            self._update_token_usage(cleanup_result)

            self._process_cleanup_output(
                cleanup_result,
                round_num,
                state,
                result,
                iteration=cleanup_iteration,
            )
        except ProviderCallCancelled:
            user_stop = True
            if isinstance(result, dict):
                result["_user_stop_requested"] = True
            if "user_stopped" not in degraded_reasons:
                degraded_reasons.append("user_stopped")
            cleanup_result = {}
        except Exception as e:
            fatal_reasons.append(failure_reason("cleanup_api", e))
            self._run_l3_emergency_save(
                round_type, state, result, round_num, user_input_text, e)
        finally:
            set_stage("cleanup_local")
            self._clear_cleanup_round_material(round_num, result)

        # ④ 主体状态结算：所有 Round 都执行；任何失败都使本轮保持 unsettled。
        try:
            state_settle_receipt = settle_state(
                self.sm,
                self.relation_store,
                self._get_round_audit_store(),
                round_num,
                round_type,
                memory_write_receipts=(
                    result.get("_memory_write_receipts", [])
                    if isinstance(result, dict) else []
                ),
                user_input_text=user_input_text,
                external_interaction=external_interaction,
            )
            if isinstance(result, dict):
                result["_state_settle_receipt"] = state_settle_receipt
        except StateSettlementError as exc:
            if isinstance(result, dict):
                result["_state_settle_receipt"] = exc.receipt
            fatal_reasons.append(f"state_settle:{exc}")
        except Exception as exc:
            fatal_reasons.append(f"state_settle:{type(exc).__name__}:{exc}")

        # ⑤ 遗忘压缩落盘（独立——API失败也有兜底处理）
        if not user_stop:
            try:
                self._process_forgetting_result(cleanup_result, round_num)
            except Exception as exc:
                fatal_reasons.append(failure_reason("forgetting_persist", exc))

        # ⑥ 升格检查（独立）
        if not user_stop:
            try:
                self._process_memory_lifecycle(round_num)
            except Exception as exc:
                fatal_reasons.append(failure_reason("memory_lifecycle", exc))

        # ⑥.4 进化集整理（自主轮阈值触发，独立）
        if not user_stop:
            try:
                self._process_evolution_set(round_type, state, result, round_num)
            except Exception as exc:
                fatal_reasons.append(failure_reason("evolution_set", exc))

        # ⑥.5 疲劳/休眠/做梦（独立——失败不阻断善后）
        if not user_stop:
            try:
                self._process_rest_cycle(round_type, state, cleanup_result, round_num)
            except Exception as exc:
                fatal_reasons.append(failure_reason("rest_cycle", exc))

        # ⑦ 保存语料缓冲（独立）
        resp_text = result.get("response", "")
        if not result.get("_l3_emergency_buffer_saved"):
            try:
                self.ctx_store.save_round_to_cache(
                    round_num,
                    user_input=user_input_text,
                    response=resp_text,
                    **cache_meta,
                )
            except Exception as exc:
                fatal_reasons.append(failure_reason("round_cache_save", exc))

        # ⑧ round 审计事件流（写入 round_{N}.jsonl）
        try:
            runtime_snapshot = {}
            if isinstance(result, dict) and result.get("_tool_transaction_audit"):
                runtime_snapshot["tool_transaction_audit"] = result.get(
                    "_tool_transaction_audit")
            runtime_exception = self._runtime_exception_audit(result)
            if runtime_exception:
                failed_phase = runtime_exception.get("failed_phase") or "unknown"
                runtime_snapshot["status"] = "issues"
                runtime_snapshot["issues"] = [f"runtime_exception:{failed_phase}"]
                runtime_snapshot["runtime_exception"] = runtime_exception
            self._get_round_audit_store().write_snapshot(
                round_num,
                runtime=runtime_snapshot or None,
                final_response_source=(
                    "runtime.user_stop"
                    if user_stop and not resp_text
                    else "reaction.final_reply_text"
                ),
                final_response=resp_text,
                executed_phases=executed_phases,
                close_round=False,
            )
        except Exception as exc:
            fatal_reasons.append(f"audit_closeout:{exc}")

        # ⑨ 节律轮收尾（独立）
        if round_type == "rhythm" and not user_stop:
            # raw_log 归档
            try:
                archived = self.ctx_store.archive_raw_log()
                if archived:
                    result["_raw_log_archived"] = archived
            except Exception as exc:
                degraded_reasons.append(failure_reason("raw_log_archive", exc))
            # 日历层处理：编年史写入 + 语料合并 + 保留清理
            try:
                self._process_calendar_cleanup(cleanup_result, round_num, state)
            except Exception as exc:
                fatal_reasons.append(failure_reason("calendar_cleanup", exc))

        # ⑨ 回调通知（独立）
        if self.on_round_complete:
            try:
                is_interactive = (round_type == "interactive")
                self.on_round_complete(round_num, resp_text, is_interactive)
            except Exception as exc:
                degraded_reasons.append(failure_reason("round_complete_callback", exc))

        # 终态：只有结算义务仍可闭合时才消费 flags。
        if not fatal_reasons:
            try:
                if user_stop:
                    self._finalize_user_stop_flags(
                        state, round_type, round_num, result=result)
                else:
                    self._finalize_flags(
                        state, round_type, round_num, result=result)
            except Exception as exc:
                fatal_reasons.append(f"flag_finalization:{exc}")
        try:
            self.state_backup_store.append_backup(
                round_num,
                self.sm.load(),
                reason="cleanup",
            )
        except Exception as exc:
            fatal_reasons.append(f"state_backup:{exc}")
        if close_round and not fatal_reasons:
            try:
                self._get_round_audit_store().close_round(
                    round_num,
                    final_response_source=(
                        "runtime.user_stop"
                        if user_stop and not resp_text
                        else "reaction.final_reply_text"
                    ),
                    final_response=resp_text,
                )
            except Exception as exc:
                fatal_reasons.append(f"round_close:{exc}")
        if close_round:
            self.hb.resume()
        status = (
            "unsettled" if fatal_reasons
            else "degraded" if degraded_reasons
            else "settled"
        )
        frame_ref = cleanup_frame_ref(cleanup_iteration)
        return {
            "status": status,
            "frame_ref": frame_ref.as_dict(),
            "degraded_reasons": degraded_reasons,
            "fatal_reasons": fatal_reasons,
        }

    def _finalize_user_stop_flags(
            self, state, round_type, round_num, result=None):
        """Consume only obligations proven complete before a user stop."""
        heartbeat_flags = state.get("base", {}).get("heartbeat_flags", {})
        flags_to_clear = []
        if heartbeat_flags.get("user_message_waiting"):
            flags_to_clear.append("user_message_waiting")
        if round_type == "relay" and heartbeat_flags.get("continue_requested"):
            flags_to_clear.append("continue_requested")
            receipt = settle_open_relay_intents(
                self.sm,
                status="deferred",
                round_num=round_num,
                note="user_stopped",
                source="runtime.user_stop",
            )
            if isinstance(result, dict):
                result.setdefault(
                    "_relay_intent_settle_receipts", []).append(receipt)
        completed_calendar = self._calendar_flags_cleared_by_result(result or {})
        flags_to_clear.extend(
            flag for flag in completed_calendar
            if heartbeat_flags.get(flag)
        )
        if (
                heartbeat_flags.get("rhythm_due")
                and rhythm_chronicle_write_applied(result or {})):
            flags_to_clear.append("rhythm_due")
        flags_to_clear.extend(
            flag for flag in self._alert_flags_cleared_by_result(result or {})
            if heartbeat_flags.get(flag)
        )
        if (
                heartbeat_flags.get("cache_compaction_due")
                and self._cache_compaction_cleared_by_result(result or {})):
            flags_to_clear.append("cache_compaction_due")
        updates = {
            "base.meta.last_round_closed_at": datetime.now(
                TZ_SHANGHAI).isoformat(),
        }
        if "rhythm_due" in flags_to_clear:
            updates["base.meta.last_rhythm_round"] = round_num
        if completed_calendar:
            updates["base.meta.last_calendar_check_at"] = datetime.now(
                timezone(timedelta(hours=8))).isoformat()
        self.sm.update_many(updates)
        if flags_to_clear:
            self.sm.clear_flags(list(dict.fromkeys(flags_to_clear)))

    def _run_l3_emergency_save(self, round_type, state, result, round_num,
                               user_input_text, error):
        """L3 心跳急救：善后步 API 失败时只做纯文件 IO 保全。"""
        error_text = str(error)
        last_error = f"善后步API异常 R{round_num}: {error_text}"
        try:
            self.sm.set("base.meta.last_error", last_error)
        except Exception:
            pass

        try:
            self.connectivity_store.log_latency(
                "cleanup", "error",
                f"L3 cleanup API failure: {error_text}")
        except Exception:
            pass

        try:
            self.alert_store.append_alert(
                round_num=round_num,
                step="cleanup",
                event_type="l3_cleanup_api_failure",
                detail=error_text,
                action="script_emergency_save",
            )
        except Exception:
            pass

        resp_text = result.get("response", "") if isinstance(result, dict) else ""
        cache_meta = cache_interaction_meta(
            result.get("_interaction_meta", {}) if isinstance(result, dict) else {})
        if isinstance(result, dict):
            result["_cleanup_api_error"] = error_text

        try:
            self.ctx_store.save_round_to_cache(
                round_num,
                user_input=user_input_text,
                response=resp_text,
                **cache_meta,
            )
            if isinstance(result, dict):
                result["_l3_emergency_buffer_saved"] = True
        except Exception:
            pass

        try:
            detail = resp_text or "（无反应步残留）"
            self.ctx_store.append_to_cache(
                round_num,
                "system",
                (
                    f"[L3心跳急救] R{round_num}: 善后步API异常: "
                    f"{error_text}；反应步残留: {detail}"
                ),
                kind="fault_note",
                step="cleanup",
                **cache_meta,
            )
        except Exception:
            pass

    def _process_evolution_set(self, round_type, state, result, round_num):
        """解析自主轮进化集块，写入 Materials/Evolution 并迁移 pending。"""
        if round_type != "autonomous" or not isinstance(result, dict):
            return []
        if not result.get("_evolution_requested"):
            return []
        blocks = extract_evolution_blocks(result.get("response", ""))
        if not blocks:
            return []
        stats = result.get("_evolution_stats") or {}
        outputs = []
        for block in blocks:
            outputs.append(self.evolution_store.process_pending(block, round_num, stats))
        return outputs

    def _process_rest_cycle(self, round_type, state, result, round_num):
        """Spec598: live Runtime 退役疲劳倒计时；善后只清零残留压力。"""
        self.sm.update_many({
            "base.fatigue.value": 0,
            "base.fatigue.awake_since": None,
            "base.sleep_state.level": "awake",
            "base.sleep_state.entered_at": None,
            "base.heartbeat_flags.fatigue_expired": False,
        })


    @staticmethod
    def _iter_receipts_with_backend(receipts):
        stack = list(receipts or [])
        while stack:
            receipt = stack.pop(0)
            yield receipt
            if isinstance(receipt, dict):
                stack[0:0] = list(receipt.get("backend_receipts") or [])

    @staticmethod
    def _alert_flags_cleared_by_result(result):
        flags = set()
        if not isinstance(result, dict):
            return flags
        receipts = []
        for key in ("_alert_mode_settle_receipts", "_protocol_tool_receipts"):
            receipts.extend(result.get(key) or [])
        for receipt in CleanupPipeline._iter_receipts_with_backend(receipts):
            if not isinstance(receipt, dict):
                continue
            if receipt.get("tool_id") != "alert_mode_settle":
                continue
            if str(receipt.get("status") or "").lower() not in {"applied", "ok"}:
                continue
            cleared_flags = receipt.get("cleared_flags") or receipt.get("clear_flags") or []
            for flag in cleared_flags:
                if flag in ALERT_FLAG_NAMES:
                    flags.add(flag)
            alert_type = str(receipt.get("alert_type") or "").strip()
            alert_status = str(
                receipt.get("alert_status") or receipt.get("alert_status_requested")
                or receipt.get("status_requested") or receipt.get("alert_mode_status")
                or receipt.get("settle_status") or receipt.get("mode") or ""
            ).strip()
            if alert_type in ALERT_FLAG_NAMES and (
                    alert_status in {"recovered", "deferred", "needs_human"}
                    or cleared_flags):
                flags.add(alert_type)
        return flags

    @staticmethod
    def _calendar_flags_cleared_by_result(result):
        flags = set()
        if not isinstance(result, dict):
            return flags
        receipts = []
        for key in ("_chronicle_write_receipts", "_protocol_tool_receipts"):
            receipts.extend(result.get(key) or [])
        for receipt in CleanupPipeline._iter_receipts_with_backend(receipts):
            if not isinstance(receipt, dict):
                continue
            if receipt.get("tool_id") != "chronicle_write":
                continue
            if str(receipt.get("status") or "").lower() != "applied":
                continue
            layer = str(receipt.get("layer") or "").strip()
            for flag, expected_layer in CALENDAR_FLAG_TO_LAYER.items():
                if layer == expected_layer:
                    flags.add(flag)
        return flags

    @staticmethod
    def _cache_compaction_cleared_by_result(result):
        if not isinstance(result, dict):
            return False
        receipts = []
        for key in ("_protocol_tool_receipts",):
            receipts.extend(result.get(key) or [])
        for receipt in CleanupPipeline._iter_receipts_with_backend(receipts):
            if not isinstance(receipt, dict):
                continue
            completed = {
                str(flag or "").strip()
                for flag in receipt.get("completed_flags") or []
                if str(flag or "").strip()
            }
            if "cache_compaction_due" in completed:
                return True
            compaction = receipt.get("cache_compaction")
            if isinstance(compaction, dict) and compaction.get("all_done"):
                return True
        return False

    @staticmethod
    def _interaction_consumed_by_result(result):
        if not isinstance(result, dict):
            return False
        return bool(
            str(result.get("_user_input_text") or "").strip()
            and result.get("_reaction_finalize_validated")
        )

    def _finalize_flags(self, state, round_type, round_num, result=None):
        """善后步终态：清零本轮已消费的 flags"""
        # 通用清理：stm_degrade_pending 每轮都清（心跳下次按实际状态重新置位）
        flags_to_clear = ["stm_degrade_pending"]
        heartbeat_flags = state.get("base", {}).get("heartbeat_flags", {})
        rhythm_chronicle_applied = (
            round_type != "rhythm"
            or rhythm_chronicle_write_applied(result or {})
        )
        # 根据轮类型确定要清理的 flag
        cal_flags = ["calendar_day_due", "calendar_week_due", "calendar_month_due",
                     "calendar_quarter_due", "calendar_year_due"]
        if round_type == "interactive":
            flags_to_clear.append("user_message_waiting")
            hf = heartbeat_flags
            if hf.get("identity_timeout"):
                flags_to_clear.append("identity_timeout")
        elif round_type == "rhythm":
            if heartbeat_flags.get("rhythm_due") and rhythm_chronicle_applied:
                flags_to_clear.append("rhythm_due")
            flags_to_clear.extend(
                flag for flag in self._alert_flags_cleared_by_result(result or {})
                if heartbeat_flags.get(flag)
            )
            flags_to_clear.extend(
                flag for flag in self._calendar_flags_cleared_by_result(result or {})
                if heartbeat_flags.get(flag)
            )
            if (
                    heartbeat_flags.get("cache_compaction_due")
                    and self._cache_compaction_cleared_by_result(result or {})):
                flags_to_clear.append("cache_compaction_due")
            if (
                    heartbeat_flags.get("user_message_waiting")
                    and self._interaction_consumed_by_result(result or {})):
                flags_to_clear.append("user_message_waiting")
        elif round_type == "standby":
            flags_to_clear.extend(["standby_due", "shelve_timer_expired"])
        elif round_type == "relay":
            if "continue_requested" not in flags_to_clear:
                flags_to_clear.append("continue_requested")
        elif round_type == "autonomous":
            flags_to_clear.extend(["feeling_settle_due", "fatigue_expired", "evolution_pending"])

        # checkpoint + 日历更新时间戳
        update_meta = {}
        if heartbeat_flags.get("rhythm_due") and rhythm_chronicle_applied:
            update_meta["base.meta.last_rhythm_round"] = round_num
        if any(cf in flags_to_clear for cf in cal_flags):
            update_meta["base.meta.last_calendar_check_at"] = datetime.now(
                timezone(timedelta(hours=8))).isoformat()
        update_meta["base.meta.last_round_closed_at"] = datetime.now(
            TZ_SHANGHAI).isoformat()
        if round_type == "standby":
            update_meta["base.meta.last_standby_round"] = round_num
            # standby_countdown 递减
            countdown = state.get("base", {}).get("runtime", {}).get("standby_countdown", STANDBY_COUNTDOWN_INITIAL)
            self.sm.set("base.runtime.standby_countdown", max(0, countdown - 1))
        else:
            # 非待命轮：重置 standby_countdown
            self.sm.set("base.runtime.standby_countdown", STANDBY_COUNTDOWN_INITIAL)
            if heartbeat_flags.get("standby_due") and "standby_due" not in flags_to_clear:
                flags_to_clear.append("standby_due")

        if update_meta:
            try:
                self.sm.update_many(update_meta)
            except Exception:
                pass

        if flags_to_clear:
            try:
                self.sm.clear_flags(flags_to_clear)
            except Exception:
                pass
        self._rearm_continue_requested_from_closeout_form(
            result,
            round_type=round_type,
            consumed_flags=flags_to_clear,
            round_num=round_num,
        )
        self._rearm_continue_requested_from_open_relay_intents(
            result,
            round_type=round_type,
            round_num=round_num,
        )

        # TD-009: 节律轮后定期层数据源可能已更新，标记过期
        if round_type == "rhythm":
            try:
                self.assembler.invalidate_layer("periodic")
            except Exception:
                pass

    def _rearm_continue_requested_from_closeout_form(
            self, result, *, round_type, consumed_flags, round_num):
        """清掉本轮入口 flag 后，按反应步 closeout_form 重臂下一轮。"""
        if not isinstance(result, dict):
            return
        receipts = result.get("_closeout_relay_receipts") or []
        if not any(self._is_continue_requested_relay_receipt(item) for item in receipts):
            if round_type != "relay":
                return
            self._set_pending_relay_target(
                {},
                result,
                round_num,
                status="pending_relay_target_cleared",
                source="no_closeout_form_relay",
            )
            return
        pending_target = normalize_pending_target(result.get("_pending_relay_target"))
        self._set_pending_relay_target(
            pending_target,
            result,
            round_num,
            status=(
                "pending_relay_target_set"
                if pending_target
                else "pending_relay_target_cleared"
            ),
            source="closeout_form",
        )
        relay_intent = None
        try:
            source_receipt = next(
                item for item in receipts
                if self._is_continue_requested_relay_receipt(item)
            )
            relay_intent = create_relay_intent(
                self.sm,
                source_round=round_num,
                handoff_text=(
                    pending_target.get("handoff_text")
                    or source_receipt.get("handoff_text")
                    or source_receipt.get("summary")
                    or ""
                ),
                reaction_finalize_id=source_receipt.get("call_id", ""),
                user_input_ref=source_receipt.get("user_input_ref", ""),
            )
        except Exception as exc:
            relay_intent = {"status": "relay_intent_create_error", "reason": str(exc)}
        receipt = {
            "tool_id": "cleanup_pipeline",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": "continue_requested_rearmed",
            "source": "closeout_form",
            "round_type": round_type,
            "consumed_flags": [
                flag for flag in consumed_flags
                if flag == "continue_requested"
            ],
            "set_flags": ["continue_requested"],
            "relay_intent": relay_intent or {},
        }
        try:
            self.sm.set_flag("continue_requested", True)
        except Exception as exc:
            receipt["status"] = "continue_requested_rearm_error"
            receipt["reason"] = str(exc)
            receipt["set_flags"] = []
        result.setdefault("_heartbeat_rearm_receipts", []).append(receipt)
        try:
            self._get_round_audit_store().append_event(
                round_num,
                "heartbeat_rearm",
                receipt,
                phase="cleanup",
            )
        except Exception:
            pass

    def _rearm_continue_requested_from_open_relay_intents(
            self, result, *, round_type, round_num):
        if round_type != "relay" or not isinstance(result, dict):
            return
        terminal_decision = self._relay_terminal_closeout_decision(result)
        if terminal_decision:
            self._settle_open_relay_intents_for_terminal_closeout(
                result,
                round_num=round_num,
                closeout_decision=terminal_decision,
            )
            return
        try:
            if self.sm.get_flags().get("continue_requested"):
                return
            open_intents = open_relay_intents(self.sm.load())
        except Exception:
            open_intents = []
        if not open_intents:
            return
        receipt = {
            "tool_id": "cleanup_pipeline",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": "continue_requested_rearmed_from_open_relay_intents",
            "source": "relay_intent_pool",
            "round_type": round_type,
            "set_flags": ["continue_requested"],
            "open_relay_intent_ids": [
                item.get("relay_intent_id") for item in open_intents
            ],
        }
        try:
            self.sm.set_flag("continue_requested", True)
        except Exception as exc:
            receipt["status"] = "continue_requested_rearm_error"
            receipt["reason"] = str(exc)
            receipt["set_flags"] = []
        result.setdefault("_heartbeat_rearm_receipts", []).append(receipt)
        try:
            self._get_round_audit_store().append_event(
                round_num,
                "heartbeat_rearm",
                receipt,
                phase="cleanup",
            )
        except Exception:
            pass

    @staticmethod
    def _relay_terminal_closeout_decision(result):
        if not isinstance(result, dict):
            return ""
        ledgers = result.get("_settlement_ledgers") or []
        for item in reversed(ledgers):
            if not isinstance(item, dict):
                continue
            if item.get("tool_id") and item.get("tool_id") != "reaction_finalize":
                continue
            decision = str(item.get("closeout_decision") or "").strip().lower()
            if decision in {"finish", "blocked"}:
                return decision
            if decision == "continue":
                return ""
        return ""

    def _settle_open_relay_intents_for_terminal_closeout(
            self, result, *, round_num, closeout_decision):
        final_status = "blocked" if closeout_decision == "blocked" else "completed"
        try:
            receipt = settle_open_relay_intents(
                self.sm,
                status=final_status,
                round_num=round_num,
                note=f"relay closeout_decision={closeout_decision}",
                source="relay_terminal_closeout",
            )
        except Exception as exc:
            receipt = {
                "tool_id": "cleanup_pipeline",
                "tool_family": "substrate_tool",
                "tool_class": "sync_tool",
                "status": "relay_terminal_closeout_intent_settle_error",
                "reason": str(exc),
                "closeout_decision": closeout_decision,
                "round_num": int(round_num or 0),
            }
        if receipt.get("status") == "applied":
            receipt = {
                **receipt,
                "tool_id": "cleanup_pipeline",
                "tool_family": "substrate_tool",
                "tool_class": "sync_tool",
                "status": "relay_terminal_closeout_intents_settled",
                "closeout_decision": closeout_decision,
                "set_flags": [],
            }
        result.setdefault("_relay_intent_terminal_receipts", []).append(receipt)
        try:
            self._get_round_audit_store().append_event(
                round_num,
                "relay_intent_terminal_closeout",
                receipt,
                phase="cleanup",
            )
        except Exception:
            pass

    def _set_pending_relay_target(self, target, result, round_num, *, status, source):
        receipt = {
            "tool_id": "cleanup_pipeline",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": status,
            "source": source,
            "target": target or {},
        }
        try:
            self.sm.set("base.runtime.pending_relay_target", target or {})
        except Exception as exc:
            receipt["status"] = "pending_relay_target_state_error"
            receipt["reason"] = str(exc)
        result.setdefault("_relay_target_state_receipts", []).append(receipt)
        try:
            self._get_round_audit_store().append_event(
                round_num,
                "relay_target_state",
                receipt,
                phase="cleanup",
            )
        except Exception:
            pass

    @staticmethod
    def _is_continue_requested_relay_receipt(receipt):
        if not isinstance(receipt, dict):
            return False
        return (
            receipt.get("status") == "continue_requested_set"
            and receipt.get("source") == "closeout_form"
            and "continue_requested" in (receipt.get("set_flags") or [])
        )

    def _process_calendar_cleanup(self, cleanup_result, round_num, state):
        """日历节律善后步：语料合并 + 保留清理"""
        import re as _re
        from data.chronicle_store import ChronicleStore, CorpusStore

        response = cleanup_result.get("response", "")
        flags = state.get("base", {}).get("heartbeat_flags", {})
        cal_flags = [flag for flag in CALENDAR_FLAG_TO_LAYER if flags.get(flag)]

        ch_store = ChronicleStore()
        co_store = CorpusStore()

        # LTM降格压缩落盘（从LLM响应提取 <!-- LTM_DEGRADE:MEM-xxx --> 块）
        if response and "calendar_day_due" in cal_flags:
            ltm_pattern = r'<!--\s*LTM_DEGRADE:(MEM-[0-9A-F]{8})\s*-->\s*\n?(.*?)\n?\s*<!--\s*/LTM_DEGRADE\s*-->'
            try:
                self._apply_ltm_degradation(_re.findall(ltm_pattern, response, _re.DOTALL), round_num)
            except Exception:
                pass

        # 语料合并（逐层）
        try:
            merge_chain = [
                ("rounds", "daily"), ("daily", "weekly"),
                ("weekly", "monthly"), ("monthly", "quarterly"),
                ("quarterly", "yearly"),
            ]
            active_names = {
                "calendar_day_due": "daily", "calendar_week_due": "weekly",
                "calendar_month_due": "monthly", "calendar_quarter_due": "quarterly",
                "calendar_year_due": "yearly",
            }
            for src, tgt in merge_chain:
                # 只在目标层激活时合并（日报→日合并；周报→周合并）
                tgt_flag = [k for k, v in active_names.items() if v == tgt]
                if tgt_flag and tgt_flag[0] in cal_flags:
                    co_store.merge_layer(src, tgt)
        except Exception:
            pass

        # 编年史 + 语料保留清理
        try:
            ch_store.cleanup_expired()
        except Exception:
            pass
        try:
            co_store.cleanup_expired()
        except Exception:
            pass
        # Attic阁楼迁移（年节律时执行）
        if "calendar_year_due" in cal_flags:
            try:
                co_store.move_to_attic()
            except Exception:
                pass

        # Trash清理（日节律时执行，衰减期1年）
        if "calendar_day_due" in cal_flags:
            try:
                self._cleanup_trash()
            except Exception:
                pass

    def _cleanup_trash(self):
        """清理 trash/ 目录中超过1年的文件"""
        import os as _os
        from datetime import datetime
        from paths import TRASH_DIR
        if not _os.path.isdir(TRASH_DIR):
            return
        cutoff = datetime.now(TZ_SHANGHAI) - timedelta(days=365)
        for root, dirs, files in _os.walk(TRASH_DIR, topdown=False):
            for fname in files:
                fpath = _os.path.join(root, fname)
                try:
                    mtime = datetime.fromtimestamp(_os.path.getmtime(fpath), TZ_SHANGHAI)
                    if mtime < cutoff:
                        _os.remove(fpath)
                except OSError:
                    pass
            # 删除空目录
            for dname in dirs:
                dpath = _os.path.join(root, dname)
                try:
                    if not _os.listdir(dpath):
                        _os.rmdir(dpath)
                except OSError:
                    pass

    def _build_ltm_degradation_context(self):
        """Build LTM degradation compression context for due Full/Summary entries."""
        return LTMDegradationManager().build_compression_context()

    def _apply_ltm_degradation(self, degradation_results, round_num):
        """Apply cleanup LLM LTM degradation results for Full/Summary only."""
        return LTMDegradationManager().apply_compression_results(
            degradation_results,
            round_num,
        )

    def _prepare_ltm_degradation_for_day(self, round_num):
        """Settle daily LTM countdowns and direct Abstract -> Backup moves."""
        return LTMDegradationManager().prepare_daily_degradation(round_num)

    def _process_memory_lifecycle(self, round_num):
        """STM→LTM 升格：AH_high ≥ 5 的条目复制到 LTM Full"""
        import os as _os
        import json as _json
        from paths import LTM_FULL_DIR, LTM_FULL_META_JSON

        try:
            candidates = self.heat.check_upgrade()
        except Exception:
            candidates = []
        if not candidates:
            return

        ms = self.memory_store
        for mem_id in candidates:
            try:
                entry = ms.read_entry(mem_id)
                meta = ms.load_meta()
                mem_meta = meta.get(mem_id, {})
                clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id

                _os.makedirs(LTM_FULL_DIR, exist_ok=True)
                full_md = _os.path.join(LTM_FULL_DIR, "full.md")
                entry_body = strip_memory_heading(entry)
                with open(full_md, "a", encoding="utf-8") as f:
                    f.write(f"\n## MEM-{clean_id}\n{entry_body}\n")

                fm = {}
                if _os.path.isfile(LTM_FULL_META_JSON):
                    with open(LTM_FULL_META_JSON, "r", encoding="utf-8") as f:
                        fm = _json.load(f)
                fm[mem_id] = make_meta_template(
                    mem_id, title=mem_meta.get("title", mem_id),
                    weight=5, subject=mem_meta.get("subject", ""),
                    model=mem_meta.get("model", ""))
                if mem_meta.get("created_at"):
                    fm[mem_id]["created_at"] = mem_meta["created_at"]
                if mem_meta.get("last_recalled_at"):
                    fm[mem_id]["last_recalled_at"] = mem_meta["last_recalled_at"]
                fm[mem_id]["created_round"] = round_num
                fm[mem_id]["last_recalled_round"] = round_num
                tmp = LTM_FULL_META_JSON + ".tmp"
                with open(tmp, "w", encoding="utf-8") as f:
                    _json.dump(fm, f, ensure_ascii=False, indent=2)
                _os.replace(tmp, LTM_FULL_META_JSON)
                self.heat.mark_stored(mem_id)
            except Exception:
                pass

    def _build_forgetting_context(self):
        """STM 遗忘分流：找出需要 LLM 语义压缩的 F/S 级条目，
        组装为追加到善后步 messages 的压缩指令文本。
        不额外调 API——复用善后步同一次调用。"""
        to_delete, to_abstract, need_compress = self._forgetting_candidates()

        if not need_compress:
            return ""

        ms = self.memory_store
        parts = ["## STM 遗忘压缩（本轮 AH_low 到线的 F/S 级条目，请语义压缩后输出）\n"]
        for mem_id in need_compress:
            try:
                entry = ms.read_entry(mem_id)
            except Exception:
                entry = f"*(条目 {mem_id} 正文读取失败)*"
            parts.append(
                f"### {mem_id}\n原文：\n{entry}\n\n"
                f"请将以上内容压缩为梗概（约128-256字），"
                f"用 `<!-- FORGET:{mem_id} -->` 和 `<!-- /FORGET -->` 包裹输出。"
            )
        return "\n".join(parts)

    def _process_forgetting_result(self, cleanup_result, round_num):
        """遗忘处理落盘：
        - F 级   → 取 LLM 压缩结果 → 写 LTM Summary → 删 STM 副本
        - S 级   → 取 LLM 压缩结果 → 写 LTM Abstract → 删 STM 副本
        - A 级   → 直接搬 LTM Abstract → 删 STM 副本
        - 已归档  → 直接删 STM 副本
        """
        import re as _re
        to_delete, to_abstract, need_compress = self._forgetting_candidates()

        response = cleanup_result.get("response", "")
        ms = self.memory_store

        # ① F/S 级：解析 LLM 压缩输出 → F 写 Summary，S 写 Abstract
        compress_results = {}
        if response and need_compress:
            pattern = r'<!--\s*FORGET:(MEM-[0-9A-F]{8})\s*-->\s*\n?(.*?)\n?\s*<!--\s*/FORGET\s*-->'
            for mem_id, compressed_text in _re.findall(pattern, response, _re.DOTALL):
                compress_results[mem_id] = compressed_text.strip()

        for mem_id in need_compress:
            if ltm_has_entry(mem_id):
                try:
                    self._remove_stm_copy(mem_id, ms)
                except Exception:
                    pass
                continue
            compressed = compress_results.get(mem_id, "")
            if not compressed:
                # LLM 没产出压缩版 → 截断兜底，下轮善后步重试
                try:
                    compressed = ms.read_entry(mem_id)
                except Exception:
                    compressed = f"*(记忆 {mem_id} 压缩失败)*"
            try:
                self._archive_compressed_stm(mem_id, compressed, round_num, ms)
                self._remove_stm_copy(mem_id, ms)
            except Exception:
                pass

        # ② A 级：直接搬 LTM Abstract（不需语义压缩）
        for mem_id in to_abstract:
            if ltm_has_entry(mem_id):
                try:
                    self._remove_stm_copy(mem_id, ms)
                except Exception:
                    pass
                continue
            try:
                entry = ms.read_entry(mem_id)
            except Exception:
                entry = f"*(记忆 {mem_id} 自动归档)*"
            try:
                self._archive_to_abstract(mem_id, entry, round_num, ms)
                self._remove_stm_copy(mem_id, ms)
            except Exception:
                pass

        # ③ 已归档（stored=True）：直接删 STM 副本
        for mem_id in to_delete:
            try:
                self._remove_stm_copy(mem_id, ms)
            except Exception:
                pass

    def _forgetting_candidates(self):
        """Exclude dormant private entries from every cleanup forgetting path."""
        from data.stm_heat_calculator import STMHeatCalculator
        from logic.memory_privacy import MEMORY_PRIVACY_ENABLED

        entries = self.heat.load_heat().get("entries", {})
        memory_store = self.memory_store
        public_entries = {}
        for mem_id, heat_entry in entries.items():
            try:
                access = str(
                    memory_store.get_meta(mem_id).get("access") or "public"
                ).strip().lower()
            except Exception:
                continue
            if MEMORY_PRIVACY_ENABLED or access != "private":
                public_entries[mem_id] = heat_entry
        return STMHeatCalculator().process_forgetting(public_entries)

    def _remove_stm_copy(self, mem_id, ms=None):
        """删除 STM 层同编号副本：正文、meta、index、keywords、heat 同步收口。"""
        ms = ms or self.memory_store
        for action in (
            lambda: ms.remove_entry(mem_id),
            lambda: ms.delete_meta(mem_id),
            lambda: ms.remove_index(mem_id),
            lambda: self.memory_index.remove_stm_entry(mem_id),
            lambda: self.heat.remove_entry(mem_id),
        ):
            try:
                action()
            except Exception:
                pass

    def _format_ltm_memory_body(self, text, mem_meta, round_num, tier):
        body = strip_memory_heading(text)
        subject = extract_memory_field(body, "交互对象") or mem_meta.get("subject", "—") or "—"
        title = extract_memory_field(body, "标题") or mem_meta.get("title", "记忆归档")
        gist = (
            extract_memory_field(body, "梗概")
            or title
        )
        content = (
            extract_memory_field(body, "摘要")
            or extract_memory_field(body, "内容")
            or body.strip()
            or gist
        )
        tags = extract_memory_field(body, "标签")
        if not tags and mem_meta.get("tags"):
            tags = ", ".join(str(t) for t in mem_meta.get("tags", []))
        feelings = extract_memory_field(body, "感受词") or "无"
        linked = extract_memory_field(body, "关联容器")
        created_at = extract_memory_field(body, "入库时间") or mem_meta.get("created_at", "")
        created_round = mem_meta.get("created_round", round_num)
        gist = gist[:128] + ("…" if len(gist) > 128 else "")

        lines = [
            f"**交互对象**：{subject}",
            f"**入库**：{round_text(created_round)}",
            f"**最后调用**：{round_text(round_num)}",
            f"**标题**：{title}",
            f"**梗概**（≤128字）：{gist}",
        ]
        if tier == "Summary":
            summary = content[:512] + ("…" if len(content) > 512 else "")
            lines.append(f"**摘要**（≤512字）：{summary}")
        if created_at:
            lines.append(f"入库时间：{created_at}")
        lines.extend([
            f"标签：{tags}",
            f"感受词：{feelings}",
            f"关联容器：{linked}",
            f"注释：{extract_memory_field(body, '注释') or 'null'}",
        ])
        return "\n".join(lines), title, gist, subject, tags

    def _archive_to_abstract(self, mem_id, text, round_num, ms):
        """写一条记忆到 LTM Abstract 层"""
        import os as _os
        import json as _json
        from paths import (
            LTM_ABSTRACT_DIR, LTM_ABSTRACT_META_JSON, LTM_ABSTRACT_ABSTRACT_MD,
            LTM_ABSTRACT_INDEX_MD,
        )

        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        _os.makedirs(LTM_ABSTRACT_DIR, exist_ok=True)

        am = {}
        if _os.path.isfile(LTM_ABSTRACT_META_JSON):
            with open(LTM_ABSTRACT_META_JSON, "r", encoding="utf-8") as f:
                am = _json.load(f)

        meta = ms.load_meta()
        mem_meta = meta.get(mem_id, {})
        source_weight = int(mem_meta.get("weight", 1) or 1)
        body, title, gist, subject, tags_text = self._format_ltm_memory_body(
            text, mem_meta, round_num, "Abstract")
        with open(LTM_ABSTRACT_ABSTRACT_MD, "a", encoding="utf-8") as f:
            f.write(f"\n## MEM-{clean_id}  [A]  权重{source_weight}\n{body}\n")
        append_ltm_index(
            LTM_ABSTRACT_INDEX_MD, mem_id, "A", source_weight,
            title, subject, round_num)

        am[mem_id] = make_meta_template(
            mem_id, title=title,
            weight=source_weight, subject=subject,
            model=mem_meta.get("model", ""))
        am[mem_id]["type"] = "A"
        if tags_text:
            am[mem_id]["tags"] = [t.strip() for t in tags_text.split(",") if t.strip()]
        if mem_meta.get("created_at"):
            am[mem_id]["created_at"] = mem_meta["created_at"]
        if mem_meta.get("last_recalled_at"):
            am[mem_id]["last_recalled_at"] = mem_meta["last_recalled_at"]
        am[mem_id]["created_round"] = round_num
        am[mem_id]["last_recalled_round"] = round_num
        tmp = LTM_ABSTRACT_META_JSON + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            _json.dump(am, f, ensure_ascii=False, indent=2)
        _os.replace(tmp, LTM_ABSTRACT_META_JSON)

    def _archive_compressed_stm(self, mem_id, text, round_num, ms):
        """按 STM 原始权重把遗忘压缩结果写入对应 LTM 层。"""
        try:
            mem_meta = ms.load_meta().get(mem_id, {})
        except Exception:
            mem_meta = {}
        weight = int(mem_meta.get("weight", 2) or 2)
        if weight >= 5:
            self._archive_to_summary(mem_id, text, round_num, ms)
        else:
            self._archive_to_abstract(mem_id, text, round_num, ms)

    def _archive_to_summary(self, mem_id, text, round_num, ms):
        """写一条记忆到 LTM Summary 层"""
        import os as _os
        import json as _json
        from paths import (
            LTM_SUMMARY_DIR, LTM_SUMMARY_META_JSON, LTM_SUMMARY_SUMMARY_MD,
            LTM_SUMMARY_INDEX_MD,
        )

        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        _os.makedirs(LTM_SUMMARY_DIR, exist_ok=True)

        sm = {}
        if _os.path.isfile(LTM_SUMMARY_META_JSON):
            with open(LTM_SUMMARY_META_JSON, "r", encoding="utf-8") as f:
                sm = _json.load(f)

        meta = ms.load_meta()
        mem_meta = meta.get(mem_id, {})
        source_weight = int(mem_meta.get("weight", 5) or 5)
        body, title, gist, subject, tags_text = self._format_ltm_memory_body(
            text, mem_meta, round_num, "Summary")
        with open(LTM_SUMMARY_SUMMARY_MD, "a", encoding="utf-8") as f:
            f.write(f"\n## MEM-{clean_id}  [S]  权重{source_weight}\n{body}\n")
        append_ltm_index(
            LTM_SUMMARY_INDEX_MD, mem_id, "S", source_weight,
            title, subject, round_num)

        sm[mem_id] = make_meta_template(
            mem_id, title=title,
            weight=source_weight, subject=subject,
            model=mem_meta.get("model", ""))
        sm[mem_id]["type"] = "S"
        if tags_text:
            sm[mem_id]["tags"] = [t.strip() for t in tags_text.split(",") if t.strip()]
        if mem_meta.get("created_at"):
            sm[mem_id]["created_at"] = mem_meta["created_at"]
        if mem_meta.get("last_recalled_at"):
            sm[mem_id]["last_recalled_at"] = mem_meta["last_recalled_at"]
        sm[mem_id]["created_round"] = mem_meta.get("created_round", round_num)
        sm[mem_id]["last_recalled_round"] = round_num
        tmp = LTM_SUMMARY_META_JSON + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            _json.dump(sm, f, ensure_ascii=False, indent=2)
        _os.replace(tmp, LTM_SUMMARY_META_JSON)

    @staticmethod
    def _cleanup_finalize_retry_parse(cleanup_result, state=None, result=None):
        if not isinstance(cleanup_result, dict):
            return False, {}, []
        if "tool_call_envelopes" not in cleanup_result:
            parsed = project_step_finalize("cleanup", {})
            parsed["_terminal_finalize_missing"] = True
            return True, parsed, []
        parsed, _ordinary_envelopes, terminal_invalids = terminal_finalize_from_envelopes(
            cleanup_result.get("tool_call_envelopes", []),
            "cleanup",
        )
        if parsed is None:
            parsed = project_step_finalize("cleanup", {})
            parsed["_terminal_finalize_missing"] = True
        if terminal_invalids:
            parsed["_terminal_invalids"] = terminal_invalids
        truth_violation = CleanupPipeline._cleanup_internalization_truth_violation(
            parsed,
            state,
            result,
        )
        if truth_violation:
            terminal_invalids = list(terminal_invalids or []) + [{
                "reason": "cleanup_internalization_truth_violation",
                "details": truth_violation,
            }]
            parsed["_terminal_invalids"] = terminal_invalids
            parsed["_cleanup_internalization_truth_violation"] = truth_violation
        retry_needed = bool(
            parsed.get("_terminal_finalize_missing")
            or terminal_invalids
        )
        return retry_needed, parsed, list(terminal_invalids or [])

    @staticmethod
    def _cleanup_internalization_truth_violation(parsed, state=None, result=None):
        if not CleanupPipeline._cleanup_has_unclosed_task_blocker(state, result):
            return None
        for field_path, text in CleanupPipeline._iter_cleanup_internal_text(parsed):
            if CleanupPipeline._is_false_completion_claim(text):
                return {
                    "field": field_path,
                    "reason": "unclosed_task_completion_claim",
                    "excerpt": str(text or "")[:160],
                }
        return None

    @staticmethod
    def _cleanup_has_unclosed_task_blocker(state=None, result=None):
        task_guard_blocker_seen = False
        if isinstance(result, dict):
            for ledger in result.get("_settlement_ledgers") or []:
                if not isinstance(ledger, dict):
                    continue
                if str(ledger.get("closeout_decision") or "").strip() == "blocked":
                    return True
                reason = str(ledger.get("blocked_reason") or "").strip()
                if reason:
                    return True
            for receipt in result.get("_reaction_loop_guard_receipts") or []:
                if not isinstance(receipt, dict):
                    continue
                status = str(receipt.get("status") or "").strip()
                reason = str(receipt.get("reason") or "").strip()
                if reason in {
                    "task_acceptance_blocked",
                    "task_bootstrap_required",
                }:
                    task_guard_blocker_seen = True
                    continue
                if "task" in status and "blocked" in status:
                    task_guard_blocker_seen = True
                    continue
                if "blocked" in status:
                    return True
        if isinstance(state, dict):
            base = state.get("base") or {}
            active_guides = base.get("active_guides") or {}
            if active_guides.get("work"):
                return True
            runtime = base.get("runtime") or {}
            debt = runtime.get("work_intent_debt") or {}
            if isinstance(debt, dict) and str(debt.get("status") or "").strip() == "open":
                return True
        try:
            from data.workbench import WorkbenchStore

            workbench = WorkbenchStore()
            active_task = str(workbench.get("base.active_task") or "").strip()
            if active_task:
                return True
            slots = workbench.active_guide_slots()
            if isinstance(slots, dict) and slots.get("work"):
                return True
            if task_guard_blocker_seen:
                # 历史 task guard receipt 只能说明本轮曾被拦过；如果
                # Workbench 已无 active task/work guide，且任务已进入 output，
                # 它不再代表 cleanup 时仍有未闭合任务。
                if CleanupPipeline._cleanup_has_completed_output_task(workbench):
                    return False
                return False
        except Exception:
            pass
        return False

    @staticmethod
    def _cleanup_has_completed_output_task(workbench):
        output_dir = getattr(workbench, "zone_dirs", {}).get("output")
        if not output_dir or not os.path.isdir(output_dir):
            return False
        try:
            task_ids = sorted(os.listdir(output_dir))
        except OSError:
            return False
        for task_id in task_ids:
            try:
                manifest = workbench.load_manifest(task_id, zone="output")
                if str(manifest.get("status") or "") != "output":
                    continue
                guide = workbench.load_task_guide(task_id)
            except Exception:
                continue
            if CleanupPipeline._cleanup_task_guide_completed(guide):
                return True
        return False

    @staticmethod
    def _cleanup_task_guide_completed(guide):
        guide = guide if isinstance(guide, dict) else {}
        required_seen = False
        for item in guide.get("items") or []:
            if not isinstance(item, dict):
                continue
            if not (item.get("required") is True or item.get("mandatory") is True):
                continue
            required_seen = True
            status = str(item.get("status") or "").strip().lower()
            if status not in DONE_ITEM_STATUSES:
                return False
            if not CleanupPipeline._cleanup_record_has_evidence(item):
                return False
        for item in guide.get("acceptance") or []:
            if not isinstance(item, dict):
                continue
            if item.get("required") is False:
                continue
            required_seen = True
            status = str(item.get("status") or "").strip().lower()
            if status not in PASSED_ACCEPTANCE_STATUSES:
                return False
            if not CleanupPipeline._cleanup_record_has_evidence(item):
                return False
        return required_seen

    @staticmethod
    def _cleanup_record_has_evidence(record):
        refs = record.get("evidence_refs") if isinstance(record, dict) else None
        if isinstance(refs, list):
            return any(str(item or "").strip() for item in refs)
        return bool(str(refs or "").strip())

    @staticmethod
    def _iter_cleanup_internal_text(parsed):
        if not isinstance(parsed, dict):
            return
        for index, item in enumerate(parsed.get("connection_bridges") or []):
            if isinstance(item, dict):
                yield f"connection_bridges[{index}].note", item.get("note")
        for index, item in enumerate(parsed.get("tacit_associations") or []):
            if isinstance(item, dict):
                yield f"tacit_associations[{index}].note", item.get("note")
                yield (
                    f"tacit_associations[{index}].drop_reason",
                    item.get("drop_reason"),
                )
        compression = parsed.get("lately_compression") or {}
        if isinstance(compression, dict):
            yield "lately_compression.reason", compression.get("reason")
            yield (
                "lately_compression.replacement_text",
                compression.get("replacement_text"),
            )

    @staticmethod
    def _is_false_completion_claim(text):
        normalized = "".join(str(text or "").lower().split())
        if not normalized:
            return False
        partial_markers = (
            "部分完成",
            "未全部完成",
            "没有全部完成",
            "并非全部完成",
            "不是全部完成",
            "尚未全部完成",
            "blocked",
            "阻塞",
            "未完成",
        )
        if any(marker in normalized for marker in partial_markers):
            return False
        completion_markers = (
            "全部完成",
            "全都完成",
            "所有任务完成",
            "全部任务完成",
            "全量完成",
            "全通过",
            "全部通过",
            "全部验收通过",
            "全验收通过",
            "已交付完成",
            "已经交付完成",
            "alltaskscompleted",
            "allcompleted",
            "fullycompleted",
        )
        return any(marker in normalized for marker in completion_markers)

    @staticmethod
    def _cleanup_finalize_retry_settlement(parsed, terminal_invalids):
        settlement = {
            "parsed": parsed,
            "retry_requested": "cleanup_finalize_missing_or_invalid",
        }
        if terminal_invalids:
            settlement["cleanup_terminal_violation"] = True
            for invalid in terminal_invalids:
                if not isinstance(invalid, dict):
                    continue
                reason = str(invalid.get("reason") or "").strip()
                if reason:
                    settlement["violation_reason"] = reason
                    break
        return settlement

    @staticmethod
    def _cleanup_finalize_retry_interrupted_settlement(
            parsed, terminal_invalids, exc):
        settlement = CleanupPipeline._cleanup_finalize_retry_settlement(
            parsed,
            terminal_invalids,
        )
        settlement["cleanup_retry_interrupted"] = True
        settlement["error"] = str(exc)
        return settlement

    @staticmethod
    def _cleanup_finalize_retry_messages(messages, terminal_invalids=None):
        truth_violation = any(
            isinstance(item, dict)
            and item.get("reason") == "cleanup_internalization_truth_violation"
            for item in (terminal_invalids or [])
        )
        if truth_violation:
            content = (
                "上一轮 cleanup_finalize 的内化字段把未闭合任务写成了完成性结论。"
                "未闭合任务、blocked 项或 pending task guide 存在时，不能写“全部完成、全通过、已交付完成”。"
                "请立刻只调用 cleanup_finalize，改写为真实的部分完成或 blocked 表达；"
                "例如“完成 11 项，任务 02 blocked，现场已归档”。"
            )
        else:
            content = (
                "上一轮 cleanup 输出缺少 provider-native cleanup_finalize。"
                "上一轮裸文本只进 audit，不作为事实、训练材料、缓存决策或善后落账；"
                "请立刻只调用 cleanup_finalize 工具完成善后步终端确认；"
                "不要用普通文本替代。"
            )
        retry = {
            "role": "user",
            "content": content,
        }
        return list(messages or []) + [retry]

    @staticmethod
    def _isolate_cleanup_natural_response(cleanup_result):
        if not isinstance(cleanup_result, dict):
            return
        if cleanup_result.get("response"):
            cleanup_result["_ordinary_response_ignored"] = True
            cleanup_result["response"] = ""

    def _process_cleanup_output(
            self, cleanup_result, round_num, state, result, iteration=1):
        """处理善后 LLM 两线清单 → 分派到 logic/ data/ 管线"""

        # 解析善后步输出
        if "tool_call_envelopes" in cleanup_result:
            parsed, _ordinary_envelopes, terminal_invalids = terminal_finalize_from_envelopes(
                cleanup_result.get("tool_call_envelopes", []),
                "cleanup",
            )
            if parsed is None:
                parsed = project_step_finalize("cleanup", {})
                parsed["_terminal_finalize_missing"] = True
            if terminal_invalids:
                parsed["_terminal_invalids"] = terminal_invalids
            cleanup_result["_step_terminal_invalids"] = terminal_invalids
        else:
            parsed = project_step_finalize("cleanup", {})
            parsed["_terminal_finalize_missing"] = True
        self._round_audit_parsed(round_num, "cleanup", iteration, parsed)

        # 组装 data 模块引用
        data_modules = {
            "state_store": self.sm,
            "memory_store": self.memory_store,
            "memory_index": self.memory_index,
            "memory_heat": self.heat,
            "container_store": self.container_store,
            "relation_store": self.relation_store,
            "context_store": self.ctx_store,
            "alert_store": self.alert_store,
        }

        # 执行善后步落盘管线
        report = process_cleanup(parsed, state, round_num, result, data_modules)

        pending = result.get("_lately_compression_pending") if isinstance(result, dict) else None
        if isinstance(pending, dict) and pending.get("lately_trimmed"):
            report["_lately_compression"] = {
                "status": "skipped",
                "reason": "moved_to_cache_compaction_rhythm",
                "flag": "cache_compaction_due",
            }

        # 执行训练材料落盘（默契集/联系集 — logic只产出数据，engine层执行I/O）
        import os as _os2
        from paths import TACIT_SET_DIR, CONNECTION_SET_DIR, ASSOCIATION_SET_DIR
        from data.training_material_store import write_tacit_set, write_connection_set, write_association_counts
        if report.get("_tacit_associations"):
            try:
                count = write_tacit_set(
                    _os2.path.join(TACIT_SET_DIR, "pending.jsonl"),
                    _os2.path.join(TACIT_SET_DIR, "processed.jsonl"),
                    round_num, report["_tacit_associations"])
                if count:
                    report.setdefault("warnings", []).append(f"默契集: {count} 条")
            except Exception as e:
                report.setdefault("errors", []).append(f"默契集写入失败: {e}")
        if report.get("_connection_bridges"):
            try:
                count = write_connection_set(
                    _os2.path.join(CONNECTION_SET_DIR, "pending.jsonl"),
                    _os2.path.join(CONNECTION_SET_DIR, "processed.jsonl"),
                    round_num, report["_connection_bridges"])
                if count:
                    report.setdefault("warnings", []).append(f"联系集: {count} 条")
            except Exception as e:
                report.setdefault("errors", []).append(f"联系集写入失败: {e}")
        # 联想集五表落盘（同条目内暴力计数）
        if report.get("_association_counts"):
            try:
                write_association_counts(ASSOCIATION_SET_DIR, report["_association_counts"])
                report.setdefault("warnings", []).append("联想集: 已更新五表")
            except Exception as e:
                report.setdefault("errors", []).append(f"联想集写入失败: {e}")

        # 处理悬空容器 POPUP（#24：持久化给下一轮 + 写入 popup store）
        if report.get("popup"):
            result["_popup"] = report["popup"]
            try:
                self.assembler.popup.write_popup(report["popup"])
            except Exception:
                pass

        # 记录错误
        for err in report.get("errors", []):
            try:
                self.sm.set("base.meta.last_error", f"R{round_num}: {err}")
            except Exception:
                pass
        self._round_audit_settlement(round_num, "cleanup", 1, report)
        return report
