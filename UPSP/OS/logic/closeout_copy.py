"""Shared visible copy for reaction closeout final replies."""

CLOSEOUT_FINAL_REPLY_REMINDER = (
    "注意：完成时直接自然语言回复用户；这段话就是用户最终看到的回复。"
    "不要只写“收束本轮”，先把该回应的话自然说完。"
)

TASK_DELIVERY_CLOSEOUT_REMINDER = (
    "如果本轮是交付任务，最终回复里顺手交代完成情况、剩余问题和主要产物位置。"
)


def closeout_final_reply_reminder(*, task_delivery=False):
    text = CLOSEOUT_FINAL_REPLY_REMINDER
    if task_delivery:
        text += TASK_DELIVERY_CLOSEOUT_REMINDER
    return text
