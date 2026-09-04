"""One-frame model material for Runtime-classified interrupted actions."""

import json

from logic.task_guide import append_task_pending_input, refresh_task_execution_active_guide


def attach_pending_task(workbench, receipt, round_num):
    active_task = str(workbench.get("base.active_task") or "").strip()
    if not active_task or not receipt:
        return False
    outcomes = ", ".join(
        f"{item['action_id']}={item['outcome']}"
        for item in receipt.get("items") or []
    )
    append_task_pending_input(
        workbench, active_task,
        source_refs=receipt.get("source_refs") or [],
        summary=("上次运行在宿主行动期间中断。Runtime 已完成机械分类："
                 f"{outcomes}。请核对现有任务并继续、修订或阻塞。"),
        input_kind="interrupted_action_recovery",
        round_num=round_num,
        task_guidance_route="current_work",
    )
    refresh_task_execution_active_guide(workbench, active_task)
    return True


def render_materials(receipt):
    items = list((receipt or {}).get("items") or [])
    if not items:
        return []
    lines = [
        "意外中断动作核对",
        "以下状态由 Runtime 根据持久动作账本和当前文件 SHA 确定。",
        "已落盘或已有成功结果的同签名动作不会重复执行；"
        "冲突或结果不确定的同签名动作保持阻断。",
        "本材料只披露恢复事实，不代表任务已经完成。",
        "actions:",
    ]
    for item in items:
        lines.append(json.dumps({
            "action_id": item["action_id"],
            "tool_id": item["tool_id"],
            "target": item.get("target") or "",
            "outcome": item.get("outcome") or "",
            "result_status": item.get("result_status") or "",
            "result_reason": item.get("result_reason") or "",
            "evidence_ref": item.get("evidence_ref") or "",
        }, ensure_ascii=False, sort_keys=True))
    return [{
        "role": "system",
        "kind": "material",
        "source": "action_recovery",
        "source_block_id": f"action_recovery:{items[0]['action_id']}",
        "content": "\n".join(lines),
    }]
