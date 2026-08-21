"""Runtime guard helpers for reaction loop retries and recovery."""

from dataclasses import dataclass, field
from datetime import datetime

from constants import local_now
from engines.general_tool_dispatcher import SUCCESS_STATUSES
from engines.reaction_helpers import safe_feedback_value
from logic.interaction_meta import cache_interaction_meta


PROVIDER_RECOVERABLE_INTERRUPTION_KINDS = {
    "provider_stream_idle_timeout",
    "provider_stream_interrupted",
    "provider_stream_incomplete_tool_call",
    "provider_native_tool_empty_output",
}
PROVIDER_INTERRUPTION_RECOVERY_LIMIT = 3
DUPLICATE_PROTOCOL_READ_REASONS = {
    "duplicate_protocol_read_satisfied",
    "duplicate_protocol_read_failure_repeated",
}


@dataclass
class ProtocolReadDuplicateGuard:
    signature: str = ""
    rejection_count: int = 0
    receipts: list = field(default_factory=list)

    def observe(self, receipts, effective_progress):
        if effective_progress:
            self.signature = ""
            self.rejection_count = 0
            self.receipts = []
            return {}
        grouped = {}
        for receipt in receipts or []:
            if not isinstance(receipt, dict):
                continue
            reason = str(receipt.get("reason") or "").strip()
            signature = str(receipt.get("protocol_read_signature") or "").strip()
            if reason in DUPLICATE_PROTOCOL_READ_REASONS and signature:
                grouped.setdefault(signature, []).append(receipt)
        if not grouped:
            self.signature = ""
            self.rejection_count = 0
            self.receipts = []
            return {}
        signature = self.signature if self.signature in grouped else sorted(grouped)[0]
        if signature == self.signature:
            self.rejection_count += 1
        else:
            self.signature = signature
            self.rejection_count = 1
            self.receipts = []
        self.receipts = (self.receipts + [grouped[signature][0]])[-3:]
        if self.rejection_count < 3:
            return {}
        blocked_reason = "blocked/protocol_read_correction_exhausted"
        blockers = [
            str(receipt.get("reason") or "protocol_read_rejected")
            for receipt in self.receipts
        ]
        return {
            "blocked_reason": blocked_reason,
            "settlement_ledger": {
                "closeout_decision": "blocked",
                "handoff_text": "",
                "auto_blocked": True,
                "blocked_reason": blocked_reason,
                "blockers": blockers,
                "source": "reaction_protocol_read_correction",
            },
            "guard_receipt": {
                "tool_id": "protocol_read",
                "tool_family": "protocol_tool",
                "tool_class": "runtime_guard",
                "status": "protocol_read_correction_exhausted_auto_blocked",
                "source": "reaction_protocol_read_correction",
                "reason": blocked_reason,
                "duplicate_signature": self.signature,
                "rejection_count": self.rejection_count,
                "rejected_receipt_count": len(self.receipts),
                "blockers": blockers,
            },
        }


def has_reaction_empty_output(parsed_reaction):
    for item in (parsed_reaction or {}).get("invalid_tool_requests", []) or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("reason") or "").strip() == "reaction_empty_output":
            return True
    return False


def reaction_empty_output_feedback(correction):
    title = "空输出警告" if str(correction or "") == "warning" else "空输出提醒"
    tail = (
        "下一次仍然没有工具调用也没有自然语言时，Runtime 会按 "
        "provider_model_format_empty_output 蓝屏类 blocked 结束本轮。"
        if title.endswith("警告")
        else "下一次请给出一个有效动作。"
    )
    return "\n".join([
        f"{title}：上一迭代没有工具调用，也没有自然语言文本。",
        "继续执行就调用当前合法工具；完成就直接自然语言回复用户；"
        "需要跨轮继续才调用 reaction_finalize(handoff_text)。",
        tail,
    ])


def is_general_tool_duplicate_feedback(feedback):
    text = str(feedback or "")
    return (
        "reason: duplicate_tool_result_satisfied" in text
        or "reason: duplicate_tool_failure_repeated" in text
        or "reason: web_backend_exhausted_duplicate" in text
    )


def remove_general_tool_duplicate_feedbacks(feedbacks):
    return [
        item for item in list(feedbacks or [])
        if not is_general_tool_duplicate_feedback(item)
    ]


def duplicate_signature(item):
    return str(
        (item or {}).get("duplicate_guard_key")
        or (item or {}).get("tool_signature")
        or ""
    ).strip()


def group_duplicate_general_tool_results(results):
    grouped = {}
    for item in results or []:
        if not isinstance(item, dict):
            continue
        signature = duplicate_signature(item)
        if not signature:
            continue
        grouped.setdefault(signature, []).append(item)
    return grouped


def duplicate_tool_summary(item):
    item = item or {}
    payload = item.get("duplicate_guard_payload") or item.get("tool_signature_payload")
    if not isinstance(payload, dict):
        payload = {}
    tool_id = (
        safe_feedback_value(payload.get("tool_id"))
        or safe_feedback_value(item.get("tool_id"))
        or "general_tool"
    )
    arguments = payload.get("arguments")
    if not isinstance(arguments, dict):
        arguments = {}
    parts = [
        f"{key}={safe_feedback_value(value, limit=80)}"
        for key, value in arguments.items()
        if safe_feedback_value(value, limit=80)
    ]
    if parts:
        return f"{tool_id}({', '.join(parts)})"
    signature = safe_feedback_value(item.get("tool_signature"), limit=16)
    return f"{tool_id}(tool_signature={signature})"


def general_tool_result_success(item):
    status = str((item or {}).get("status") or "").strip()
    return status in SUCCESS_STATUSES


def general_tool_guard_failure_trackable(item):
    item = item or {}
    status = str(item.get("status") or "").strip()
    reason = str(item.get("reason") or "").strip()
    if not reason or status in SUCCESS_STATUSES:
        return False
    if str(item.get("tool_id") or "").strip() != "file_edit":
        return False
    return bool(duplicate_signature(item))


def with_guard_duplicate_reference(item, prior_results):
    item = dict(item or {})
    if item.get("duplicate_of_call_id"):
        return item
    signature = duplicate_signature(item)
    call_id = str(item.get("call_id") or "").strip()
    if not signature:
        return item
    for prior in reversed(prior_results or []):
        if not isinstance(prior, dict):
            continue
        prior_call_id = str(prior.get("call_id") or "").strip()
        if prior_call_id and prior_call_id == call_id:
            continue
        if duplicate_signature(prior) != signature:
            continue
        item["duplicate_of_call_id"] = prior_call_id
        previous_status = str(prior.get("status") or "").strip()
        previous_reason = str(prior.get("reason") or "").strip()
        if previous_status:
            item["previous_status"] = previous_status
        if previous_reason:
            item["previous_reason"] = previous_reason
        break
    return item


def has_effective_protocol_progress(receipts):
    success_statuses = {"ok", "success", "accepted", "applied"}
    for item in receipts or []:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "").strip()
        if (
                status in success_statuses
                or (
                    status == "degraded"
                    and item.get("tool_id") == "relation_card_write"
                    and item.get("card_id"))):
            return True
    return False


def format_general_tool_duplicate_guard_feedback(
    *,
    items,
    tier,
    streak_count,
    effective_new_progress,
    multi_signature_count,
    has_main_chain,
    closeout_next,
):
    items = [item for item in items or [] if isinstance(item, dict)]
    first = items[0] if items else {}
    reason = str(first.get("reason") or "").strip()
    title = "工具重复警告" if tier == "warning" else "工具重复提醒"
    next_action = "stop_or_change_arguments" if closeout_next else "consume_existing_result_or_change_arguments"
    lines = [
        "- kind: native_tool_result",
        f"  tier: {tier}",
        "  decision_required: false",
        f"  title: {title}",
        f"  tool_id: {safe_feedback_value(first.get('tool_id'))}",
        f"  call_id: {safe_feedback_value(first.get('call_id'))}",
        f"  reason: {safe_feedback_value(reason)}",
        f"  duplicate_of_call_id: {safe_feedback_value(first.get('duplicate_of_call_id'))}",
        f"  previous_status: {safe_feedback_value(first.get('previous_status'))}",
    ]
    previous_reason = safe_feedback_value(first.get("previous_reason"))
    if previous_reason:
        lines.append(f"  previous_reason: {previous_reason}")
    lines.extend([
        f"  duplicate_signature: {duplicate_tool_summary(first)}",
        f"  next_action: {next_action}",
        "  message: |",
    ])
    if len(items) > 1:
        lines.append(f"    本迭代同一参数重复提交 {len(items)} 次。")
    duplicate_of = safe_feedback_value(first.get("duplicate_of_call_id"))
    previous_status = safe_feedback_value(first.get("previous_status"))
    if duplicate_of:
        lines.append(f"    重复对象：{duplicate_of}。")
    if previous_status:
        lines.append(f"    上次状态：{previous_status}。")
    if previous_reason:
        lines.append(f"    上次原因：{previous_reason}。")
    if multi_signature_count > 1:
        if has_main_chain:
            lines.append(
                f"    本迭代共有 {multi_signature_count} 个不同工具签名重复；这里只按主连续签名处理。"
            )
        else:
            lines.append(
                f"    本迭代共有 {multi_signature_count} 个不同工具签名重复；这是批量复读提醒，不推进连续重复阶梯。"
            )
    if (
            reason == "duplicate_tool_failure_repeated"
            or reason == "web_backend_exhausted_duplicate"
            or (
                str(first.get("status") or "").strip() not in SUCCESS_STATUSES
                and reason != "duplicate_tool_result_satisfied"
            )):
        lines.append("    这个参数组合上次失败或被拒绝，原样重试不会产生新证据。")
        lines.append("    请修正参数、换工具，或停止这条路径。")
    else:
        lines.append("    这个参数组合已有成功结果，原样重试不会产生新证据。")
        lines.append("    请直接使用已有工具事实，或改参数推进下一步。")
    if effective_new_progress:
        lines.append("    本迭代同时有有效新进展；重复项仅作冗余提醒，不推进连续重复阶梯。")
    elif closeout_next:
        lines.append(
            "    这条重复链已达到纠偏阈值；请停止原样复读，改参数推进，或直接自然回复说明当前结果。确需跨轮继续时调用 reaction_finalize(handoff_text)。"
        )
    elif streak_count >= 2:
        lines.append(
            "    这是同一工具签名第 2 次连续重复；下一迭代仍重复同一工具签名，将升级为停止或收束提醒。"
        )
    else:
        lines.append("    这是同一工具签名第 1 次重复提醒；请先按上面的路线纠偏。")
    return "\n".join(lines)


def provider_interruption_kind(exc):
    text = str(exc or "")
    for kind in PROVIDER_RECOVERABLE_INTERRUPTION_KINDS:
        if kind in text:
            return kind
    return ""


def receipt_counts_as_provider_recovery_progress(receipt):
    if not isinstance(receipt, dict):
        return False
    status = str(receipt.get("status") or "").strip()
    if (
            status == "degraded"
            and receipt.get("tool_id") == "relation_card_write"
            and receipt.get("card_id")):
        return True
    if not status or status not in SUCCESS_STATUSES:
        return False
    return True


def provider_recovery_has_committed_progress(
        *,
        general_tool_results=None,
        protocol_receipts=None,
        guide_submit_receipts=None,
        memory_write_receipts=None):
    if any(general_tool_result_success(item) for item in general_tool_results or []):
        return True
    for collection in (
            protocol_receipts or [],
            guide_submit_receipts or [],
            memory_write_receipts or []):
        if isinstance(collection, dict):
            collection = [collection]
        if any(receipt_counts_as_provider_recovery_progress(item)
               for item in collection or []):
            return True
    return False


def provider_recovery_next_count(state_manager, workbench, kind):
    task_id = ""
    try:
        task_id = str(workbench.get("base.active_task") or "").strip()
    except Exception:
        task_id = ""
    try:
        runtime = (state_manager.load().get("base", {}).get("runtime", {}))
        current = runtime.get("provider_interruption_recovery") or {}
    except Exception:
        current = {}
    previous_kind = str((current or {}).get("kind") or "").strip()
    previous_task = str((current or {}).get("task_id") or "").strip()
    previous_count = int((current or {}).get("count") or 0)
    if previous_kind == kind and previous_task == task_id:
        count = previous_count + 1
    else:
        count = 1
    payload = {
        "kind": kind,
        "task_id": task_id,
        "count": count,
        "limit": PROVIDER_INTERRUPTION_RECOVERY_LIMIT,
        "updated_at": local_now().isoformat(),
    }
    try:
        state_manager._set_internal(
            "base.runtime.provider_interruption_recovery",
            payload,
        )
    except Exception:
        pass
    return count, task_id


def clear_provider_interruption_recovery_state(state_manager):
    try:
        current = (
            state_manager.load()
            .get("base", {})
            .get("runtime", {})
            .get("provider_interruption_recovery")
        )
    except Exception:
        current = None
    if not current:
        return
    try:
        state_manager._set_internal("base.runtime.provider_interruption_recovery", {})
    except Exception:
        pass


def recover_provider_interruption_if_possible(
        exc,
        *,
        state_manager,
        workbench,
        append_to_context_cache,
        round_num,
        iteration,
        general_tool_results=None,
        protocol_receipts=None,
        guide_submit_receipts=None,
        memory_write_receipts=None,
        interaction_meta=None):
    kind = provider_interruption_kind(exc)
    if not kind:
        return None
    has_progress = provider_recovery_has_committed_progress(
        general_tool_results=general_tool_results,
        protocol_receipts=protocol_receipts,
        guide_submit_receipts=guide_submit_receipts,
        memory_write_receipts=memory_write_receipts,
    )
    if not has_progress:
        return None
    count, task_id = provider_recovery_next_count(state_manager, workbench, kind)
    if count >= PROVIDER_INTERRUPTION_RECOVERY_LIMIT:
        return {
            "tool_id": "provider_interruption",
            "tool_family": "runtime_guard",
            "tool_class": "provider_recovery",
            "status": "provider_model_format_instability",
            "source": "reaction_loop",
            "reason": kind,
            "provider_error_kind": kind,
            "provider_error_classification": "provider_model_format_instability",
            "provider_interruption_count": count,
            "provider_interruption_limit": PROVIDER_INTERRUPTION_RECOVERY_LIMIT,
            "task_id": task_id,
            "terminal": True,
        }
    receipt = {
        "tool_id": "provider_interruption",
        "tool_family": "runtime_guard",
        "tool_class": "provider_recovery",
        "status": "provider_interruption_recovered",
        "source": "reaction_loop",
        "reason": kind,
        "provider_error_kind": kind,
        "provider_error_recoverable": True,
        "provider_interruption_count": count,
        "provider_interruption_limit": PROVIDER_INTERRUPTION_RECOVERY_LIMIT,
        "task_id": task_id,
        "set_flags": ["continue_requested"],
    }
    try:
        state_manager.set_flag("continue_requested", True)
    except Exception as flag_error:
        receipt["status"] = "provider_interruption_recovery_flag_error"
        receipt["flag_error"] = str(flag_error)
        receipt["set_flags"] = []
    fact = "\n".join([
        "【工具事实｜provider 中断可恢复】",
        f"provider/model 流式或工具格式中断：{kind}。",
        "本轮此前已有成功工具事实、文件写入或任务账本进展；这些已落账事实保留。",
        "Runtime 已置位 continue_requested，下一轮从现有产物、工具事实和任务看板继续。",
        f"连续恢复次数：{count}/{PROVIDER_INTERRUPTION_RECOVERY_LIMIT}。",
    ])
    try:
        append_to_context_cache(
            round_num,
            "system",
            fact,
            kind="tool_fact",
            step="reaction",
            iter=iteration,
            tool_result=receipt,
            **cache_interaction_meta(interaction_meta or {}),
        )
    except Exception:
        pass
    return receipt
