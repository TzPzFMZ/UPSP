"""Shared visible copy for task progress updates."""

import json


TASK_ITEM_ID_PLACEHOLDER = "<逐字复制当前任务项ID>"
TASK_ACCEPTANCE_ID_PLACEHOLDER = "<逐字复制当前验收项ID>"


def _update_example(field, record_id, status):
    payload = {
        str(record_id): {
            "status": status,
            "evidence_refs": ["EV-..."],
        }
    }
    return f"fields.{field}=" + json.dumps(
        payload, ensure_ascii=False, separators=(",", ":")
    )

TASK_ITEM_UPDATE_EXAMPLE = (
    _update_example("items", TASK_ITEM_ID_PLACEHOLDER, "done")
)
TASK_ACCEPTANCE_UPDATE_EXAMPLE = (
    _update_example(
        "acceptance", TASK_ACCEPTANCE_ID_PLACEHOLDER, "passed"
    )
)
TASK_ORIGINAL_GOAL_NON_SHRINK_REMINDER = (
    "原始目标：不要把用户原始目标改写成更小的阶段性目标；"
    "部分完成不能登记为全部 done/passed，未完成项留在清单里，"
    "或用 reaction_finalize(handoff_text) 说明下一轮从哪里继续。"
)


def task_board_instruction_lines(
        task_id, *, item_ids=None, acceptance_ids=None):
    item_id = next(
        (
            str(value or "").strip()
            for value in item_ids or []
            if str(value or "").strip()
        ),
        TASK_ITEM_ID_PLACEHOLDER,
    )
    acceptance_id = next(
        (
            str(value or "").strip()
            for value in acceptance_ids or []
            if str(value or "").strip()
        ),
        TASK_ACCEPTANCE_ID_PLACEHOLDER,
    )
    return [
        "固定使用说明：只读看板，不是提交入口，也不是普通执行入口；先做真实工作，checkpoint 或证据齐了再登记。",
        f"计划结构修订入口：guide_submit(guide_id=task:{task_id}, item_id=task_progress, option_id=revise_task_plan)；只在新证据真正改变来源、拆分、验收或风险时使用，提交需要替换的完整目标片段和外层 reason。",
        f"批量登记入口：guide_submit(guide_id=task:{task_id}, item_id=task_progress, option_id=update_task_status)。",
        "revise_task_plan 只改工作计划结构，不能提交 status/evidence_refs；完成与验收仍只走 update_task_status。",
        "状态词：任务项只用 done / blocked；验收项只用 passed / blocked。",
        TASK_ORIGINAL_GOAL_NON_SHRINK_REMINDER,
        "done / passed 必须带 evidence_refs；已产出未登记时不要重复写文件。",
        "blocked 必须同时写 reason 和 evidence_refs；失败调用使用 Runtime 给出的 call:<call_id>，不要猜造 EV-*。",
        "登记格式：不要只写 reason；reason 不会改变账本状态。",
        "任务项 ID 与验收项 ID 都是不透明标识；必须逐字复制当前看板中的真实 ID，禁止改大小写、连字符、下划线或补零。",
        f"任务项格式：{_update_example('items', item_id, 'done')}。",
        f"验收项格式：{_update_example('acceptance', acceptance_id, 'passed')}。",
    ]


def task_acceptance_checkpoint_text():
    lines = [
        "已完成任务项写 fields.items，验收结果写 fields.acceptance。",
        "不要只写 reason；reason 不会改变账本状态。",
        f"任务项格式：{TASK_ITEM_UPDATE_EXAMPLE}。",
        f"验收项格式：{TASK_ACCEPTANCE_UPDATE_EXAMPLE}。",
        "以上尖括号是占位说明；提交时必须用阻断项/当前看板中的真实 ID 逐字替换，禁止把 - 改成 _ 或反向改写。",
        "任务项状态用 done/blocked；验收项状态用 passed/blocked。",
        "字段示例：任务项 status:'done'；验收项 status:'passed' 或 status:'blocked'。",
        "done/passed 必须带 evidence_refs，可引用 EV-*、输出文件路径、file:<路径>、任务根相对路径或成功 shell_command 原始命令。",
        "blocked 必须同时带 reason 和 evidence_refs；失败调用只能引用 Runtime 已登记的 call:<call_id>。",
    ]
    return "".join(lines)
