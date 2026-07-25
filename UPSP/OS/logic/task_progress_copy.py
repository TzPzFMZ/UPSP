"""Shared visible copy for task progress updates."""

TASK_ITEM_UPDATE_EXAMPLE = (
    'fields.items={"task_01":{"status":"done","evidence_refs":["EV-..."]}}'
)
TASK_ACCEPTANCE_UPDATE_EXAMPLE = (
    'fields.acceptance={"acc_01":{"status":"passed","evidence_refs":["EV-..."]}}'
)
TASK_ORIGINAL_GOAL_NON_SHRINK_REMINDER = (
    "原始目标：不要把用户原始目标改写成更小的阶段性目标；"
    "部分完成不能登记为全部 done/passed，未完成项留在清单里，"
    "或用 reaction_finalize(handoff_text) 说明下一轮从哪里继续。"
)


def task_board_instruction_lines(task_id):
    return [
        "固定使用说明：只读看板，不是提交入口，也不是普通执行入口；先做真实工作，checkpoint 或证据齐了再登记。",
        f"批量登记入口：guide_submit(guide_id=task:{task_id}, item_id=task_progress, option_id=update_task_status)。",
        "状态词：任务项只用 done / blocked；验收项只用 passed / blocked。",
        TASK_ORIGINAL_GOAL_NON_SHRINK_REMINDER,
        "done / passed 必须带 evidence_refs；已产出未登记时不要重复写文件。",
        "登记格式：不要只写 reason；reason 不会改变账本状态。",
        f"任务项格式：{TASK_ITEM_UPDATE_EXAMPLE}。",
        f"验收项格式：{TASK_ACCEPTANCE_UPDATE_EXAMPLE}。",
    ]


def task_acceptance_checkpoint_text():
    lines = [
        "已完成任务项写 fields.items，验收结果写 fields.acceptance。",
        "不要只写 reason；reason 不会改变账本状态。",
        f"任务项格式：{TASK_ITEM_UPDATE_EXAMPLE}。",
        f"验收项格式：{TASK_ACCEPTANCE_UPDATE_EXAMPLE}。",
        "任务项状态用 done/blocked；验收项状态用 passed/blocked。",
        "字段示例：任务项 status:'done'；验收项 status:'passed' 或 status:'blocked'。",
        "done/passed 必须带 evidence_refs，可引用 EV-*、输出文件路径、file:<路径>、任务根相对路径或成功 shell_command 原始命令。",
    ]
    return "".join(lines)
