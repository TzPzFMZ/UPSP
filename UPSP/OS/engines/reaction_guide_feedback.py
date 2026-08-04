"""Model-visible guide feedback renderers for the reaction loop."""


RHYTHM_GUIDE_KINDS = {
    "calendar_rhythm_guide",
    "main_axis_rhythm_guide",
    "emergency_handling_guide",
    "context_pressure_rhythm_guide",
    "cache_compaction_rhythm_guide",
}
RETIRED_TASK_BOOTSTRAP_OPTION_IDS = {
    "need_more" "_sources",
    "blocked_by_" "missing_access",
}


def _guide_kind_label(kind):
    return {
        "task_bootstrap": "任务清单创建指南",
        "task_execution": "任务执行清单",
        "calendar_rhythm_guide": "日历节律指南",
        "main_axis_rhythm_guide": "主轴节律指南",
        "emergency_handling_guide": "紧急处理指南",
        "context_pressure_rhythm_guide": "上下文压力指南",
        "cache_compaction_rhythm_guide": "最近缓存压缩指南",
    }.get(str(kind or "").strip(), "指南清单")


def _guide_visible_title(kind):
    kind = str(kind or "").strip()
    if kind in RHYTHM_GUIDE_KINDS:
        return "节律指南"
    return _guide_kind_label(kind)


def _guide_fragment_kind(kind):
    kind = str(kind or "").strip()
    if kind == "task_bootstrap":
        return "active_task_bootstrap_guide"
    if kind == "task_execution":
        return "active_task_execution_guide"
    if kind in RHYTHM_GUIDE_KINDS:
        return "active_rhythm_guide"
    return "active_guide"


def _typed_guide_fragment(kind, message):
    message = str(message or "").strip()
    if not message:
        return ""
    title = _guide_visible_title(kind)
    return (
        f"- kind: {_guide_fragment_kind(kind)}\n"
        "  tier: guide\n"
        "  decision_required: false\n"
        f"  title: {title}\n"
        "  source: runtime/active_guide\n"
        "  message: |\n"
        + "\n".join(f"    {line}" if line else "" for line in message.splitlines())
    )


def _guide_processing_label(kind, guide):
    kind = str(kind or "").strip()
    if kind in RHYTHM_GUIDE_KINDS:
        for item in guide.get("items") or []:
            if not isinstance(item, dict):
                continue
            for option in item.get("options") or []:
                if not isinstance(option, dict):
                    continue
                if str(option.get("option_id") or "").strip() == "write_chronicle":
                    return "节律编年史写入"
    return _guide_kind_label(kind)


def _guide_option_action(option_id):
    return {
        "write_chronicle": "写入本次节律编年史。",
        "settle_calendar_item": "结算这个日历节律项。",
        "settle_alert": "确认紧急事项已经处理或恢复。",
        "record_fault": "记录仍未恢复的故障事实。",
        "settle_context_pressure": "结算上下文压力提醒。",
        "submit_cache_compaction_shard": "提交一个最近缓存压缩分片结果。",
        "submit_initial_guide": "提交初始任务清单。",
        "not_a_task": "确认这不是需要清单化的任务。",
        "update_task_status": "更新任务项或验收项状态。",
        "integrate_pending_input": "先整合待处理用户输入。",
    }.get(str(option_id or "").strip(), "执行这个选项。")


def _guide_field_label(field):
    return {
        "content": "content（正文）",
        "reason": "reason（原因）",
        "settlement": "settlement（结算说明）",
        "status": "status（状态）",
        "summary": "summary（中文摘要）",
        "fault_type": "fault_type（故障类型）",
        "severity": "severity（严重程度）",
        "step": "step（发生步骤）",
        "source": "source（来源）",
        "detail": "detail（详情）",
        "task_title": "task_title（任务标题）",
        "task_goal": "task_goal（任务目标）",
        "source_requirements": "source_requirements（来源需求账）",
        "items": "items（任务项）",
        "acceptance": "acceptance（验收项）",
        "pending_inputs": "pending_inputs（待整合输入）",
        "shard_id": "shard_id（分片 ID）",
        "source_block_ids": "source_block_ids（来源语料块 ID）",
        "input_chars": "input_chars（压缩前字符数）",
        "output_chars": "output_chars（压缩后字符数）",
    }.get(str(field or "").strip(), str(field or "").strip())


def _normalized_fields(fields):
    return [
        str(field or "").strip()
        for field in (fields or [])
        if str(field or "").strip()
    ]


def _guide_option_lines(option, *, rhythm=False):
    option_id = str((option or {}).get("option_id") or "").strip()
    if not option_id:
        return []
    if option_id in RETIRED_TASK_BOOTSTRAP_OPTION_IDS:
        return []
    prefix = "" if rhythm else "  - "
    lines = [f"{prefix}option_id={option_id}：{_guide_option_action(option_id)}"]

    required_fields = _normalized_fields((option or {}).get("required_fields"))
    allowed_fields = _normalized_fields((option or {}).get("allowed_fields"))
    if option_id == "write_chronicle":
        required_fields = ["content"]
        allowed_fields = ["content"]

    required = [_guide_field_label(field) for field in required_fields]
    allowed = [_guide_field_label(field) for field in allowed_fields]
    optional = [field for field in allowed if field not in required]
    indent = "" if rhythm else "    "
    if required:
        lines.append(f"{indent}需要填写：" + "、".join(required) + "。")
    else:
        lines.append(f"{indent}无需填写额外字段。")
    if optional:
        lines.append(f"{indent}可补充：" + "、".join(optional) + "。")
    return lines


def _cache_compaction_plan_lines(guide):
    plan = guide.get("compaction_plan") if isinstance(
        guide.get("compaction_plan"), dict) else {}
    shards = [
        shard for shard in plan.get("shards") or []
        if isinstance(shard, dict)
    ]
    completed = {
        str(item or "").strip()
        for item in guide.get("completed_shards") or []
        if str(item or "").strip()
    }
    remaining = [
        shard for shard in shards
        if str(shard.get("shard_id") or "").strip()
        and str(shard.get("shard_id") or "").strip() not in completed
    ]
    if not (plan or completed or remaining):
        return []
    lines = ["", "缓存压缩计划："]
    metrics = []
    metric_labels = {
        "before_chars": "压缩前字符数",
        "target_chars": "目标字符数",
        "compact_ratio": "目标比例",
    }
    for key, label in metric_labels.items():
        value = plan.get(key)
        if value is not None:
            metrics.append(f"{label} {value}")
    if metrics:
        lines.append("- " + "；".join(metrics) + "。")
    if completed:
        lines.append("- 已完成分片：" + "、".join(sorted(completed)) + "。")
    if remaining:
        lines.append("- 剩余分片：")
        for shard in remaining[:12]:
            shard_id = str(shard.get("shard_id") or "").strip()
            source_ids = [
                str(item)
                for item in shard.get("source_block_ids") or []
                if str(item)
            ]
            details = []
            if source_ids:
                details.append("来源块 " + "、".join(source_ids))
            input_chars = shard.get("input_chars")
            target_chars = shard.get("target_chars")
            if input_chars is not None:
                details.append(f"输入 {input_chars} 字")
            if target_chars is not None:
                details.append(f"目标 {target_chars} 字")
            suffix = "（" + "；".join(details) + "）" if details else ""
            lines.append(f"  - {shard_id}{suffix}。")
        if len(remaining) > 12:
            lines.append(f"  - 还有 {len(remaining) - 12} 个分片未显示。")
    return lines


def _active_slots_from_workbench(workbench):
    if workbench is None or not hasattr(workbench, "active_guide_slots"):
        return {}
    try:
        slots = workbench.active_guide_slots()
    except Exception:
        return {}
    return slots if isinstance(slots, dict) else {}


def _active_slot_for_guide(guide_id, active_slots):
    guide_id = str(guide_id or "").strip()
    if not guide_id:
        return ""
    for slot_name in ("rhythm", "work"):
        if str((active_slots or {}).get(slot_name) or "").strip() == guide_id:
            return slot_name
    return ""


def _waiting_task_lines(guide_id, kind, active_slots):
    if str(kind or "").strip() not in RHYTHM_GUIDE_KINDS:
        return []
    if _active_slot_for_guide(guide_id, active_slots) != "rhythm":
        return []
    waiting_work = str((active_slots or {}).get("work") or "").strip()
    if not waiting_work:
        return []
    return [
        "等待中的任务指南：先完成当前节律指南，完成后任务指南会重新显示。",
        "",
    ]


def _task_bootstrap_instruction_lines():
    return [
        "",
        "建账专用卡：",
        "- 先读材料：路径/URL/文件名只是入口；未见 file_read / web_fetch 等回执前，不写材料内部任务项。",
        "- 一次提交完整初始账本：source_refs=已读材料目录，source_requirements=任务需求账，items=执行项，acceptance=验收项。",
        "- 最小结构：source_requirements=[{requirement_id, source_ref, summary}]；items=[{item_id, title, requirement_refs:[...]}]；acceptance=[{acceptance_id, description, item_refs:[...]}]。",
        "- source_ref 必须来自已读来源；可用完整路径、唯一文件名或任务根相对路径，不要引用未读材料。",
        "- 不要把用户原始目标改写成更小的阶段性目标；只完成部分内容时不能报全完成，未完成项留在清单或用 reaction_finalize(handoff_text) 交接。",
        "- summary/title/description 用中文自然语言；Runtime 只校验和正规化，不替你理解材料。",
        "- 读取材料和 submit_initial_guide 不同次提交；读取结果下一次反应才真正可见。",
        "- 清单很长就压缩描述，但 submit_initial_guide 仍必须带齐需求、执行项和验收项。",
        "- 工具调用走 native 通道；正文只写简短进展，不承载 DSML/JSON/完整参数。",
    ]


def render_active_guide_feedback(
        guide,
        *,
        workbench=None,
        active_slots=None,
        task_execution_renderer=None):
    if not isinstance(guide, dict):
        return ""
    guide_id = str(guide.get("guide_id") or "").strip()
    if not guide_id:
        return ""
    kind = str(guide.get("kind") or "").strip()
    if kind == "task_execution":
        if task_execution_renderer is None:
            return ""
        return _typed_guide_fragment(kind, task_execution_renderer(guide, workbench))

    slots = active_slots if isinstance(active_slots, dict) else _active_slots_from_workbench(workbench)
    rhythm = kind in RHYTHM_GUIDE_KINDS
    lines = []
    lines.extend(_waiting_task_lines(guide_id, kind, slots))
    lines.extend([
        f"当前指南：{_guide_visible_title(kind)}",
        f"处理对象：{_guide_processing_label(kind, guide)}。",
        "使用工具：guide_submit",
        f"调用坐标：guide_id={guide_id}",
        "该坐标 executable=true；上下文和历史账本中的其他 guide_id 均为 historical / executable=false。",
        "只选择下方列出的 item_id 与 option_id；不要编造不存在的清单项、动作或字段。",
        "不要把可见标题当作 guide_id；真正的调用坐标以上面的 ID 为准。",
    ])

    if kind == "cache_compaction_rhythm_guide":
        lines.extend(_cache_compaction_plan_lines(guide))
    if kind == "task_bootstrap":
        lines.extend(_task_bootstrap_instruction_lines())

    for item in guide.get("items") or []:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("item_id") or "").strip()
        if not item_id:
            continue
        item_prefix = "" if rhythm else "- "
        lines.append(f"{item_prefix}item_id={item_id}")
        for option in item.get("options") or []:
            if not isinstance(option, dict):
                continue
            lines.extend(_guide_option_lines(option, rhythm=rhythm))
    return _typed_guide_fragment(kind, "\n".join(lines))
