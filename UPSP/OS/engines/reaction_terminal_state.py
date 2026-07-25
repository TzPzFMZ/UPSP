"""Runtime-derived terminal-state helpers for the reaction loop."""

from engines.reaction_task_acceptance import (
    check_task_closeout_acceptance,
    task_acceptance_block_signature as _task_acceptance_block_signature,
    task_acceptance_feedback as _task_acceptance_feedback,
)
from logic.write_pending_settlement import render_write_pending_blocker
from logic.work_intent_debt import clear_work_intent_debt
from logic.reaction_time_policy import reaction_time_milestone_seconds


RHYTHM_GUIDE_KINDS = {
    "main_axis_rhythm_guide",
    "calendar_rhythm_guide",
    "emergency_handling_guide",
    "context_pressure_rhythm_guide",
    "cache_compaction_rhythm_guide",
}
TASK_BOOTSTRAP_ACCESS_FAILURE_TOOLS = {
    "file_read",
    "web_fetch",
}
TASK_BOOTSTRAP_ACCESS_FAILURE_REASONS = {
    "access_denied",
    "capability_denied",
    "outside_allowlist",
    "permission_denied",
    "private_network_denied",
    "sandbox_tool_not_allowed",
    "unauthorized",
    "forbidden",
    "http_401",
    "http_403",
}


def _text(value):
    return str(value or "").strip()


def _line_text(value):
    text = _text(value)
    return text if text else "未知"


def _unfinished_read_signature(reads):
    parts = []
    for item in reads or []:
        if not isinstance(item, dict):
            continue
        path = _text(item.get("path")) or "unknown"
        next_line = _text(
            item.get("next_line_start")
            if item.get("next_line_start") not in (None, "")
            else item.get("next_start_line")
        )
        if not next_line:
            continue
        pair = f"{path}:{next_line}"
        if pair not in parts:
            parts.append(pair)
    return "|".join(sorted(parts))


def _format_read_total(item):
    total = _text((item or {}).get("total_lines"))
    return f"共 {total} 行" if total else "总行数未知"


def _format_unfinished_read_line(index, item):
    path = _text(item.get("path")) or "unknown"
    start = _line_text(item.get("start_line"))
    end = _line_text(item.get("end_line"))
    next_line = _line_text(item.get("next_line_start"))
    return (
        f"{index}. {path}：已读第 {start}-{end} 行 / "
        f"{_format_read_total(item)}；has_more=true，下一段从第 {next_line} 行开始"
    )


def build_unfinished_file_read_final_reply_reminder(
        unfinished_file_reads,
        reminded_signatures=None):
    """Return one-shot final-reply reminder for unfinished file reads."""
    reads = [
        dict(item)
        for item in (unfinished_file_reads or [])
        if isinstance(item, dict)
        and _text(item.get("path"))
        and _text(item.get("next_line_start"))
    ]
    if not reads:
        return None
    signature = _unfinished_read_signature(reads)
    if not signature:
        return None
    if signature in set(reminded_signatures or []):
        return None
    shown = reads[-3:]
    if len(shown) == 1:
        item = shown[0]
        path = _text(item.get("path")) or "unknown"
        start = _line_text(item.get("start_line"))
        end = _line_text(item.get("end_line"))
        next_line = _line_text(item.get("next_line_start"))
        feedback = "\n".join([
            "最终回复事实提醒：",
            "",
            "你刚才生成了一次无工具调用的自然最终回复候选，但本轮还有未闭合的读取事实。"
            "该候选尚未作为最终回复发给用户，也不会回灌进后续上下文。",
            "",
            "读取事实：",
            f"- {path}",
            f"- 已读：第 {start}-{end} 行 / {_format_read_total(item)}",
            f"- has_more=true，下一段从第 {next_line} 行开始",
            "",
            "不要把未完成读取说成已经读完。",
            "如果用户原始任务要求完整阅读、完整处理或完整核对，请优先继续 file_read；"
            "不要把阶段性已读当作任务完成。",
            "现在选一种方式：",
            f"- 继续读取：调用 file_read(path=\"{path}\", line_start={next_line})",
            f"- 跨轮继续：调用 reaction_finalize(handoff_text=\"从 {path} 第 {next_line} 行继续\")",
            "- 只回答已读部分：可以自然回复，但按已读范围说",
        ])
    else:
        lines = [
            "最终回复事实提醒：",
            "",
            "你刚才生成了一次无工具调用的自然最终回复候选，但本轮还有未闭合的读取事实。"
            "该候选尚未作为最终回复发给用户，也不会回灌进后续上下文。",
            "不要把未完成读取说成已经读完。",
            "",
            "未闭合读取：",
        ]
        lines.extend(
            _format_unfinished_read_line(index, item)
            for index, item in enumerate(shown, start=1)
        )
        lines.extend([
            "",
            "如果用户原始任务要求完整阅读、完整处理或完整核对，请优先继续 file_read；"
            "不要把阶段性已读当作任务完成。",
            "现在选一种方式：继续 file_read，跨轮 reaction_finalize(handoff_text)，"
            "或只按已读范围自然回复。",
        ])
        feedback = "\n".join(lines)
    return {
        "signature": signature,
        "feedback": feedback,
        "read_refs": [
            _text(item.get("read_ref"))
            for item in reads
            if _text(item.get("read_ref"))
        ],
        "shown_count": len(shown),
        "total_count": len(reads),
    }


def task_closeout_acceptance(state_manager, workbench, closeout_form):
    return check_task_closeout_acceptance(
        state_manager,
        workbench,
        closeout_form,
    )


def task_acceptance_feedback(result):
    return _task_acceptance_feedback(result)


def task_acceptance_block_signature(result):
    return _task_acceptance_block_signature(result)


def rhythm_guide_closeout_acceptance(
        closeout_form,
        state,
        round_type,
        completed_flags,
        *,
        current_runtime_guide_pending_flags):
    if str(round_type or "").strip().lower() != "rhythm":
        return {"allowed": True}
    decision = str(
        (closeout_form or {}).get("closeout_decision") or ""
    ).strip().lower()
    if decision not in {"finish", "continue"}:
        return {"allowed": True}
    guide, pending = current_runtime_guide_pending_flags(
        state,
        completed_flags=completed_flags,
    )
    kind = str((guide or {}).get("kind") or "").strip()
    if kind not in RHYTHM_GUIDE_KINDS:
        return {"allowed": True}
    if not pending:
        return {"allowed": True}
    titles = []
    for item in (guide or {}).get("items") or []:
        if not isinstance(item, dict):
            continue
        flag = str(item.get("flag") or "").strip()
        if flag in pending:
            title = str(item.get("title") or flag).strip()
            titles.append(title)
    return {
        "allowed": False,
        "reason": "rhythm_guide_blocked",
        "guide_kind": kind,
        "blockers": pending,
        "titles": titles,
    }


def rhythm_guide_acceptance_feedback(result):
    blockers = ", ".join(str(item) for item in result.get("blockers") or [])
    titles = ", ".join(str(item) for item in result.get("titles") or [])
    if titles:
        blockers = f"{blockers} ({titles})" if blockers else titles
    if not blockers:
        blockers = str(result.get("reason") or "rhythm_guide_blocked")
    return (
        "rhythm_guide_blocked: 当前最高优先节律/紧急清单未闭合，"
        "不能自然结束或跨轮中继，也不能切到低优先任务。"
        f"未完成=[{blockers}]。"
        "请继续处理当前 GUIDE；可以穿插 memory_write、关系或容器等主体基础沉淀，"
        "但清单结算前不能结束本轮。"
    )


def validate_natural_final_reply_candidate(
        *,
        closeout_form_validator,
        write_pending_tracker,
        current_state,
        round_type,
        runtime_guide_completed_flags,
        current_runtime_guide_pending_flags,
        task_closeout_acceptance,
        prior_general_tool_results=None,
        unfinished_file_reads=None,
        reminded_unfinished_read_signatures=None):
    closeout_form = {
        "closeout_decision": "finish",
        "handoff_text": "",
    }
    validation = closeout_form_validator(closeout_form)
    if validation.get("blocked"):
        return {
            "allowed": False,
            "status": "closeout_form_blocked",
            "source": "natural_final_reply_candidate",
            "feedback": (
                "最终回复候选已拦截：本轮还有必须处理的反应义务；"
                "先处理工具回执、读取游标或不可跳过 pending，再自然回复用户。"
            ),
            "reasons": list(validation.get("reasons") or []),
        }

    write_pending_blocker = write_pending_tracker.finalize_blocker()
    deferred_subject_resolution = list(
        write_pending_blocker.get("deferred_subject_resolution") or []
    )
    if write_pending_blocker.get("blocked"):
        return {
            "allowed": False,
            "status": "write_pending_blocked",
            "source": "natural_final_reply_candidate",
            "reason": write_pending_blocker.get("reason"),
            "pendings": write_pending_blocker.get("pendings", []),
            "feedback": render_write_pending_blocker(write_pending_blocker),
        }

    rhythm_acceptance = rhythm_guide_closeout_acceptance(
        closeout_form,
        current_state,
        round_type,
        runtime_guide_completed_flags,
        current_runtime_guide_pending_flags=current_runtime_guide_pending_flags,
    )
    if not rhythm_acceptance.get("allowed", True):
        return {
            "allowed": False,
            "status": "rhythm_guide_blocked",
            "source": "natural_final_reply_candidate",
            "reason": rhythm_acceptance.get("reason"),
            "guide_kind": rhythm_acceptance.get("guide_kind"),
            "blockers": rhythm_acceptance.get("blockers", []),
            "feedback": rhythm_guide_acceptance_feedback(rhythm_acceptance),
        }

    task_acceptance = task_closeout_acceptance(closeout_form)
    if not task_acceptance.get("allowed", True):
        missing_access = _task_bootstrap_missing_access_final_reply_result(
            task_acceptance,
            prior_general_tool_results,
        )
        if missing_access:
            settlement_ledger = dict(validation.get("settlement_ledger") or {})
            settlement_ledger["closeout_decision"] = "finish"
            settlement_ledger.setdefault(
                "source",
                "natural_final_reply_candidate",
            )
            settlement_ledger["task_bootstrap_missing_access_final_reply"] = True
            settlement_ledger["missing_access_tool_id"] = missing_access.get("tool_id")
            settlement_ledger["missing_access_reason"] = missing_access.get("reason")
            for key in ("path", "url", "call_id"):
                value = missing_access.get(key)
                if value not in (None, ""):
                    settlement_ledger[f"missing_access_{key}"] = value
            return {
                "allowed": True,
                "closeout_form": closeout_form,
                "settlement_ledger": settlement_ledger,
            }
        if task_acceptance.get("terminal_blocked") is True:
            blocked_closeout_form = {
                "closeout_decision": "blocked",
                "handoff_text": "",
            }
            settlement_ledger = dict(validation.get("settlement_ledger") or {})
            settlement_ledger.update({
                "closeout_decision": "blocked",
                "source": "natural_final_reply_candidate",
                "runtime_derived_blocked": True,
                "blocked_reason": (
                    task_acceptance.get("reason")
                    or "task_acceptance_blocked"
                ),
                "blockers": list(task_acceptance.get("blockers") or []),
            })
            if deferred_subject_resolution:
                settlement_ledger["write_status"] = (
                    "subject_resolution_waiting_for_user"
                )
                settlement_ledger["write_applied"] = False
                settlement_ledger["deferred_write_reasons"] = sorted({
                    _text(item.get("reason"))
                    for item in deferred_subject_resolution
                    if _text(item.get("reason"))
                })
            return {
                "allowed": True,
                "closeout_form": blocked_closeout_form,
                "settlement_ledger": settlement_ledger,
            }
        return {
            "allowed": False,
            "status": "task_acceptance_blocked",
            "source": "natural_final_reply_candidate",
            "reason": task_acceptance.get("reason"),
            "blockers": task_acceptance.get("blockers", []),
            "feedback": task_acceptance_feedback(task_acceptance),
        }

    settlement_ledger = dict(validation.get("settlement_ledger") or {})
    settlement_ledger["closeout_decision"] = "finish"
    settlement_ledger.setdefault("source", "natural_final_reply_candidate")
    if deferred_subject_resolution:
        settlement_ledger["write_status"] = "subject_resolution_waiting_for_user"
        settlement_ledger["write_applied"] = False
        settlement_ledger["deferred_write_reasons"] = sorted({
            _text(item.get("reason"))
            for item in deferred_subject_resolution
            if _text(item.get("reason"))
        })
    if str(settlement_ledger.get("read_status") or "").strip() == "partial_user_wait":
        reminder = build_unfinished_file_read_final_reply_reminder(
            unfinished_file_reads,
            reminded_unfinished_read_signatures,
        )
        if reminder:
            return {
                "allowed": False,
                "status": "unfinished_file_read_final_reply_reminder",
                "source": "natural_final_reply_candidate",
                "reason": "unfinished_file_read",
                "feedback": reminder.get("feedback"),
                "signature": reminder.get("signature"),
                "read_refs": reminder.get("read_refs", []),
                "shown_count": reminder.get("shown_count"),
                "total_count": reminder.get("total_count"),
                "settlement_ledger": settlement_ledger,
            }
    return {
        "allowed": True,
        "closeout_form": closeout_form,
        "settlement_ledger": settlement_ledger,
    }


def _task_bootstrap_missing_access_final_reply_result(
        task_acceptance,
        prior_general_tool_results):
    reason = str((task_acceptance or {}).get("reason") or "").strip()
    if reason not in {"task_bootstrap_pending", "task_bootstrap_required"}:
        return None
    for result in reversed(list(prior_general_tool_results or [])):
        if not isinstance(result, dict):
            continue
        tool_id = str(result.get("tool_id") or "").strip()
        if tool_id not in TASK_BOOTSTRAP_ACCESS_FAILURE_TOOLS:
            continue
        status = str(result.get("status") or "").strip().lower()
        if status in {"ok", "success", "accepted", "applied"}:
            continue
        failure_reason = _access_failure_reason(result)
        if not failure_reason:
            continue
        selected = dict(result)
        selected["reason"] = failure_reason
        return selected
    return None


def _access_failure_reason(result):
    reason = str((result or {}).get("reason") or "").strip().lower()
    if reason in TASK_BOOTSTRAP_ACCESS_FAILURE_REASONS:
        return reason
    for key in ("http_status", "status_code", "response_status"):
        value = str((result or {}).get(key) or "").strip()
        if value == "401":
            return "http_401"
        if value == "403":
            return "http_403"
    text = " ".join(
        str((result or {}).get(key) or "").strip().lower()
        for key in ("error", "message", "detail")
    )
    if "unauthorized" in text or "401" in text:
        return "unauthorized"
    if "forbidden" in text or "403" in text:
        return "forbidden"
    if "permission denied" in text or "access denied" in text:
        return "permission_denied"
    return ""


def apply_task_bootstrap_missing_access_terminal_settlement(
        workbench_store,
        state_manager,
        settlement_ledger):
    if not (
        isinstance(settlement_ledger, dict)
        and settlement_ledger.get("task_bootstrap_missing_access_final_reply") is True
    ):
        return None
    guide_id = "task_bootstrap"
    guide = {}
    try:
        guide = workbench_store.load_guide(guide_id)
    except Exception:
        guide = {}
    if isinstance(guide, dict) and guide:
        updated = dict(guide)
        updated["status"] = "dismissed"
        updated["dismiss_reason"] = "missing_access_final_reply"
        try:
            workbench_store.save_guide(updated, active=False)
        except Exception:
            pass
    try:
        workbench_store.append_guide_ledger(guide_id, {
            "event": "task_bootstrap_dismissed",
            "status": "dismissed",
            "reason": "missing_access_final_reply",
            "source": "natural_final_reply_candidate",
            "missing_access_tool_id": settlement_ledger.get("missing_access_tool_id"),
            "missing_access_reason": settlement_ledger.get("missing_access_reason"),
        })
    except Exception:
        pass
    try:
        if hasattr(workbench_store, "clear_active_guide"):
            workbench_store.clear_active_guide(guide_id)
        else:
            workbench_store.set("base.active_guide", None)
    except Exception:
        pass
    try:
        clear_work_intent_debt(state_manager)
    except Exception:
        pass
    return {
        "tool_id": "final_reply",
        "tool_family": "message_channel",
        "tool_class": "runtime_guard",
        "status": "task_bootstrap_missing_access_finalized",
        "source": "natural_final_reply_candidate",
        "guide_id": guide_id,
        "cleared_work_intent_debt": True,
        "missing_access_tool_id": settlement_ledger.get("missing_access_tool_id"),
        "missing_access_reason": settlement_ledger.get("missing_access_reason"),
    }


def apply_reaction_handoff_relay_receipt(state_manager, relay_receipt, trace=None):
    """Set the continue flag for a valid reaction_finalize handoff."""
    if not isinstance(relay_receipt, dict) or not relay_receipt:
        return None
    handoff_text = str(relay_receipt.get("handoff_text") or "").strip()
    reason = str(relay_receipt.get("reason") or handoff_text).strip()
    try:
        state_manager.set_flag("continue_requested", True)
        status = "continue_requested_set"
    except Exception as exc:
        status = "continue_requested_error"
        reason = str(exc) or reason
    receipt = {
        "tool_id": "reaction_finalize",
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "status": status,
        "source": "closeout_form",
        "set_flags": ["continue_requested"] if status == "continue_requested_set" else [],
        "reason": reason,
    }
    if handoff_text:
        receipt["handoff_text"] = handoff_text
    for key, value in (trace or {}).items():
        if value not in (None, ""):
            receipt[key] = value
    return receipt


def build_runtime_auto_continue_closeout(
        state_manager, *, elapsed_seconds=0, time_limit_seconds=600):
    elapsed = int(max(0, elapsed_seconds or 0))
    limit = int(max(1, time_limit_seconds or 600))
    _reminder_at, _warning_at, auto_relay_at = reaction_time_milestone_seconds(limit)
    if auto_relay_at % 60 == 0:
        auto_relay_label = f"{auto_relay_at // 60}分钟"
    else:
        auto_relay_label = f"{auto_relay_at}秒"
    handoff_text = (
        f"本轮已达到{auto_relay_label}事务预算，Runtime 自动按 continue 中继。"
        "下一轮请从已有工具事实、产物、任务看板、未完成项和阻断事实继续。"
    )
    reason = "round_time_budget_exhausted"
    relay_receipt = apply_reaction_handoff_relay_receipt(
        state_manager,
        {
            "handoff_text": handoff_text,
            "reason": reason,
        },
        trace={
            "source": "runtime_auto_continue",
            "elapsed_seconds": elapsed,
            "time_limit_seconds": limit,
        },
    )
    settlement_ledger = {
        "closeout_decision": "continue",
        "handoff_text": handoff_text,
        "reason": reason,
        "source": "runtime_auto_continue",
        "elapsed_seconds": elapsed,
        "time_limit_seconds": limit,
    }
    guard_receipt = {
        "tool_id": "reaction_finalize",
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "status": "runtime_auto_continue",
        "source": "runtime_auto_continue",
        "reason": reason,
        "elapsed_seconds": elapsed,
        "time_limit_seconds": limit,
        "set_flags": ["continue_requested"],
    }
    return relay_receipt, settlement_ledger, guard_receipt


def runtime_auto_blocked_final_response(settlement_ledgers=None):
    ledgers = [
        item for item in (settlement_ledgers or [])
        if isinstance(item, dict)
    ]
    latest = ledgers[-1] if ledgers else {}
    if not (
        latest.get("auto_blocked") is True
        and str(latest.get("closeout_decision") or "").strip() == "blocked"
    ):
        return ""
    reason = str(
        latest.get("blocked_reason") or "runtime_auto_blocked"
    ).strip()
    blockers = [
        str(item or "").strip()
        for item in latest.get("blockers") or []
        if str(item or "").strip()
    ]
    shown = ", ".join(blockers[:24])
    if len(blockers) > 24:
        shown += f", ... (+{len(blockers) - 24})"
    if not shown:
        shown = reason
    if reason == "provider_model_format_empty_output":
        return (
            "Runtime auto-blocked 本轮，不能把本轮说成成功完成。\n\n"
            "原因：provider_model_format_empty_output。\n"
            f"未结算项：{shown}。\n\n"
            "provider/model 连续没有返回工具调用，也没有自然语言文本；"
            "本轮按蓝屏类 blocked 保留现场，等待后续修复或继续处理。"
        )
    if reason == "reaction_closeout_protocol_violation":
        return (
            "Runtime auto-blocked 本轮，不能把本轮说成成功完成。\n\n"
            "原因：reaction_closeout_protocol_violation。\n"
            f"未结算项：{shown}。\n\n"
            "Runtime 检测到旧收束协议连续无效；"
            "本轮按 blocked 收束并保留现场，等待后续修复或继续处理。"
        )
    return (
        "Runtime auto-blocked 本轮，不能把本轮说成成功完成。\n\n"
        f"原因：{reason}。\n"
        f"未结算项：{shown}。\n\n"
        "任务清单仍有 required item/acceptance 没有通过 "
        "guide_submit 落账为 done/passed，也缺少对应 evidence_refs。"
        "本轮生成的文件只能作为事故现场证据或后续继续工作的材料，"
        "不计为成功样本。"
    )


def project_reaction_terminal_response(
        settlement_ledgers=None,
        assistant_text=""):
    ledgers = [
        item for item in (settlement_ledgers or [])
        if isinstance(item, dict)
    ]
    latest = ledgers[-1] if ledgers else {}
    decision = str(latest.get("closeout_decision") or "").strip()
    if decision == "continue":
        return "", False, ""
    runtime_auto_blocked = runtime_auto_blocked_final_response(ledgers)
    if runtime_auto_blocked:
        return (
            runtime_auto_blocked,
            True,
            "reaction.runtime_auto_blocked_final_reply",
        )
    text = str(assistant_text or "").strip()
    if text and decision in {"finish", "blocked"}:
        return text, True, "reaction.natural_final_reply"
    if decision == "blocked":
        reason = str(
            latest.get("blocked_reason")
            or latest.get("reason")
            or "blocked"
        ).strip()
        return (
            f"本轮按 blocked 收束。\n\n原因：{reason}。",
            True,
            "reaction.runtime_closeout_fallback",
        )
    if decision == "finish":
        return (
            "本轮已完成。",
            True,
            "reaction.runtime_closeout_fallback",
        )
    return "", False, ""
