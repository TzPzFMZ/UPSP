"""Resident reaction-loop guide options."""


REACTION_LOOP_GUIDE_ID = "reaction_loop_guide"
REACTION_TASK_GUIDANCE_ITEM_ID = "task_guidance_entry"
REACTION_TASK_GUIDANCE_OPTION_ID = "request_task_guidance"


def reaction_loop_resident_guide():
    return {
        "guide_id": REACTION_LOOP_GUIDE_ID,
        "kind": "reaction_step_guide",
        "resident": True,
        "items": [
            {
                "item_id": REACTION_TASK_GUIDANCE_ITEM_ID,
                "mandatory": False,
                "options": [
                    {
                        "option_id": REACTION_TASK_GUIDANCE_OPTION_ID,
                        "required_fields": [],
                        "allowed_fields": [],
                    },
                ],
            },
        ],
    }


def reaction_loop_resident_feedback():
    lines = [
        "任务清单入口",
        "需要任务清单时，用这个短入口请 Runtime 挂出建账卡。",
        "普通闲聊、单个状态查询、简单读命令或纯节律维护可以忽略。",
        "使用工具：guide_submit",
        "调用坐标：guide_id=reaction_loop_guide",
        "该坐标 executable=true；上下文和历史账本中的其他 guide_id 均为 historical / executable=false。",
        "- item_id=task_guidance_entry",
        "  - option_id=request_task_guidance：请求 Runtime 挂出任务清单创建指南。",
    ]
    return (
        "- kind: resident_task_guidance_guide\n"
        "  tier: guide\n"
        "  decision_required: false\n"
        "  title: 任务清单入口\n"
        "  source: runtime/reaction_resident_guide\n"
        "  message: |\n"
        + "\n".join(f"    {line}" if line else "" for line in lines)
    )
