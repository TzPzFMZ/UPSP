"""Task acceptance helpers for Runtime terminal-state handling."""

import json

from logic.closeout_copy import closeout_final_reply_reminder
from logic.task_progress_copy import task_acceptance_checkpoint_text


def check_task_closeout_acceptance(state_manager, workbench_store, closeout_form):
    """Validate whether a reaction closeout can finish the active task."""
    try:
        from logic.task_acceptance import validate_task_closeout
        from logic.work_intent_debt import current_work_intent_debt
        result = validate_task_closeout(workbench_store, closeout_form)
        if isinstance(result, dict) and result.get("allowed") is False:
            task_id = str(result.get("task_id") or "").strip()
            if task_id and not result.get("guide_id"):
                result["guide_id"] = f"task:{task_id}"
            return result
        decision = str((closeout_form or {}).get("closeout_decision") or "").strip().lower()
        if decision == "finish" and current_work_intent_debt(state_manager):
            return {
                "allowed": False,
                "reason": "task_bootstrap_required",
                "blockers": ["work_intent_debt"],
            }
        return result
    except Exception as exc:
        return {
            "allowed": False,
            "reason": "task_acceptance_check_failed",
            "error": str(exc),
            "blockers": ["task_acceptance_check_failed"],
        }


def task_acceptance_feedback(result):
    blockers = ", ".join(str(item) for item in result.get("blockers") or [])
    if not blockers:
        blockers = str(result.get("reason") or "task_acceptance_blocked")
    reason = str((result or {}).get("reason") or "").strip()
    if reason in {"task_bootstrap_required", "task_bootstrap_pending"}:
        return (
            "WARNING｜任务清单入口未闭合。"
            f"阻断项=[{blockers}]。"
            "当前存在 task_bootstrap；不要更新不存在的当前任务清单。"
            "若这是可在本轮直接回答的请求，立即调用 guide_submit："
            "guide_id=task_bootstrap；item_id=build_initial_task_guide；option_id=not_a_task；"
            "并填写 reason 说明无需清单化；"
            "若用户确实派发了需清单化的任务，再读取必要材料并以同一 guide/item 调用 "
            "option_id=submit_initial_guide。"
            f"{closeout_final_reply_reminder(task_delivery=True)}"
            "task_bootstrap 撤下或建账完成后再自然回复用户；只有需要跨轮继续时，才调用 reaction_finalize(handoff_text)。"
        )
    guide_id = str((result or {}).get("guide_id") or "").strip()
    if not guide_id:
        task_id = str((result or {}).get("task_id") or "").strip()
        guide_id = f"task:{task_id}" if task_id else "<当前任务指南>"
    return (
        "WARNING｜任务账本未闭合。"
        "任务验收 checkpoint：任务清单未闭合，不能按完成收束。"
        f"阻断项=[{blockers}]。"
        "此刻才需要登记账本；执行期不要反复填账。"
        "使用工具：guide_submit；入口：guide_submit/update_task_status；"
        f"坐标：guide_id={guide_id}；item_id=task_progress；option_id=update_task_status。"
        f"{task_acceptance_checkpoint_text()}"
        f"{closeout_final_reply_reminder(task_delivery=True)}"
        "确实无法继续的项标 blocked 并写 reason；账本闭合后直接自然回复用户。"
        "blocked 还必须引用 Runtime 已登记的 call:<call_id> 或成功证据；失败调用不能冒充 EV-*。"
        "只有需要跨轮继续时，才调用 reaction_finalize(handoff_text)。"
        "全部必需项证据通过后，Runtime 会自动撤下任务清单。"
    )


def task_acceptance_block_signature(result):
    blockers = sorted({
        str(item or "").strip()
        for item in (result or {}).get("blockers") or []
        if str(item or "").strip()
    })
    if not blockers:
        blockers = [str(
            (result or {}).get("reason") or "task_acceptance_blocked"
        ).strip()]
    parts = [
        str((result or {}).get("reason") or "task_acceptance_blocked").strip(),
        str((result or {}).get("guide_id") or (result or {}).get("task_id") or "").strip(),
        *blockers,
    ]
    ledger_state = (result or {}).get("ledger_state")
    if isinstance(ledger_state, dict):
        parts.append(json.dumps(
            ledger_state,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ))
    return "|".join(parts)
