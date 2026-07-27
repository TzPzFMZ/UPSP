"""Runtime-owned rhythm guide selection helpers."""

from engines.heartbeat import round_decision_from_heartbeat_flags


EMERGENCY_FLAGS = (
    ("api_degraded", "API 异常处理", "当前存在 API 异常。请根据可见连通性事实判断：已恢复则结算；未恢复但当前能跑则搁置；不能处理则记录故障。"),
)

CONTEXT_PRESSURE_FLAGS = (
    ("token_usage_warning", "上下文压力处理", "当前存在上下文窗口或挂载压力。请取消不必要挂载，或确认后续维护动作；处理后结算。"),
    ("context_pressure", "上下文压力处理", "当前存在 Runtime 置位的上下文压力。请根据可见状态处理或搁置，并形成可审计结算。"),
)

CACHE_COMPACTION_ITEMS = (
    ("cache_compaction_due", "最近缓存压缩", "最近缓存 lately 发生水位删除，需要按可见缓存压缩清单处理幸存段压缩。"),
)

DEFAULT_DEFER_SECONDS = 3600

CALENDAR_ITEMS = (
    ("calendar_day_due", "日志", "按当前 active guide 的 item_id=calendar_day_due，选择 option_id=write_chronicle，并在 fields.content 填写日志正文。"),
    ("calendar_week_due", "周志", "按当前 active guide 的 item_id=calendar_week_due，选择 option_id=write_chronicle，并在 fields.content 填写周志正文。"),
    ("calendar_month_due", "月志", "按当前 active guide 的 item_id=calendar_month_due，选择 option_id=write_chronicle，并在 fields.content 填写月志正文。"),
    ("calendar_quarter_due", "季志", "按当前 active guide 的 item_id=calendar_quarter_due，选择 option_id=write_chronicle，并在 fields.content 填写季志正文。"),
    ("calendar_year_due", "年志", "按当前 active guide 的 item_id=calendar_year_due，选择 option_id=write_chronicle，并在 fields.content 填写年志正文。"),
)

HIGH_PRIORITY_BEFORE_INTERACTION = {
    "emergency",
    "context_pressure",
    "cache_compaction",
    "main_axis_rhythm",
    "calendar_day",
    "calendar_week",
    "calendar_month",
    "calendar_quarter",
    "calendar_year",
}


def active_emergency_items(flags):
    flags = flags or {}
    items = []
    seen_titles = set()
    for flag, title, text in EMERGENCY_FLAGS:
        if not flags.get(flag) or title in seen_titles:
            continue
        items.append({"flag": flag, "title": title, "text": text})
        seen_titles.add(title)
    return items


def active_context_pressure_items(flags):
    flags = flags or {}
    items = []
    seen_titles = set()
    for flag, title, text in CONTEXT_PRESSURE_FLAGS:
        if not flags.get(flag) or (flag, title) in seen_titles:
            continue
        items.append({"flag": flag, "title": title, "text": text})
        seen_titles.add((flag, title))
    return items


def _completed_set(completed_flags=None):
    if not completed_flags:
        return set()
    return {str(flag or "").strip() for flag in completed_flags if str(flag or "").strip()}


def _checkbox(flag, completed_flags=None):
    return "[x]" if str(flag or "").strip() in _completed_set(completed_flags) else "[ ]"


def _guide_item_flags(item):
    return [
        str(flag or "").strip()
        for flag in (item or {}).get("flags", [])
        if str(flag or "").strip()
    ]


def _guide_item_completed(item, completed_flags):
    flags = _guide_item_flags(item)
    return bool(flags) and all(flag in completed_flags for flag in flags)


def _current_guide_item(decision, completed_flags):
    for item in decision.get("guide_queue") or []:
        flags = _guide_item_flags(item)
        if item.get("kind") == "emergency":
            if any(flag not in completed_flags for flag in flags):
                return item
            continue
        if not _guide_item_completed(item, completed_flags):
            return item
    return {}


def _has_higher_priority_before_interaction(decision, completed_flags=None):
    completed_flags = _completed_set(completed_flags)
    for item in decision.get("guide_queue") or []:
        if item.get("kind") == "interaction":
            return False
        if (
                item.get("kind") in HIGH_PRIORITY_BEFORE_INTERACTION
                and not _guide_item_completed(item, completed_flags)):
            return True
    return False


def _main_axis_guide(flags, completed_flags=None):
    marker = _checkbox("rhythm_due", completed_flags)
    return {
        "kind": "main_axis_rhythm_guide",
        "text": "\n".join([
            "GUIDE｜主轴节律指南",
            "",
            "本轮需要处理主轴节律事项。请根据当前可见材料完成下面项目。",
            "",
            f"{marker} 检查当前编年史焦点是否已经由 Runtime 挂出。",
            "如果没有可用的编年史焦点，guide_submit 回执会返回当前无需写入。",
            "",
            f"{marker} 按当前 active guide 选择 option_id=write_chronicle，提交本次主轴节志正文。",
            "正文只写模型需要概括和判断的部分；轮次、时间、状态数值由 Runtime 预填。",
            "",
            f"{marker} 本轮必要事项完成后，直接自然语言回复用户；需要跨轮继续才调用 reaction_finalize(handoff_text)。",
        ]).rstrip(),
        "items": [{"flag": "rhythm_due", "title": "主轴节律"}],
    }


def _context_pressure_guide(flags, completed_flags=None):
    items = [
        item for item in active_context_pressure_items(flags)
        if item["flag"] not in _completed_set(completed_flags)
    ]
    lines = [
        "GUIDE｜上下文压力维护指南",
        "",
        "本轮存在上下文压力事项。请处理下面仍未完成的项目。",
        "",
    ]
    for item in items:
        lines.append(f"{_checkbox(item['flag'], completed_flags)} {item['title']}")
        lines.append(item["text"])
        lines.append("")
    lines.append("[ ] 当前上下文压力事项结算后，继续本轮后续 guide；不要直接绕过更低优先工作。")
    return {
        "kind": "context_pressure_rhythm_guide",
        "text": "\n".join(lines).rstrip(),
        "items": items,
    }


def _cache_compaction_guide(flags, completed_flags=None):
    marker = _checkbox("cache_compaction_due", completed_flags)
    return {
        "kind": "cache_compaction_rhythm_guide",
        "text": "\n".join([
            "GUIDE｜最近缓存压缩维护指南",
            "",
            "最近缓存 lately 已发生水位删除，Runtime 会把压缩分片清单物化为 active guide。",
            "",
            f"{marker} 按当前 active guide 的 option_id=submit_cache_compaction_shard 逐片提交压缩摘要。",
            "每片压缩后 Runtime 会实测 lately 当前字符数；达到全局目标后当前压缩节律自动完成。",
            "",
            f"{marker} 压缩节律完成后，继续本轮后续 guide 或自然收束。",
        ]).rstrip(),
        "items": [{"flag": "cache_compaction_due", "title": "最近缓存压缩"}],
    }


def _calendar_guide(flags, completed_flags=None, target_flag=None):
    flags = flags or {}
    lines = [
        "GUIDE｜日历节律指南",
        "",
        "本轮需要处理当前到期的日历节律事项。请按可见材料完成下面项目。",
        "",
    ]
    if flags.get("user_message_waiting"):
        lines.extend([
            "本轮 heartbeat_flags.user_message_waiting=true：起手已看到真实用户输入并完成交互意图判断。",
            "先结算当前节律清单；节律结算后，同一轮 next reaction.loop 必须显现交互/任务指南并继续处理（same-round），不要把它默认登记为 relay 债务。",
            "",
        ])
    items = []
    for flag, title, text in CALENDAR_ITEMS:
        if target_flag and flag != target_flag:
            continue
        if not flags.get(flag):
            continue
        lines.append(f"{_checkbox(flag, completed_flags)} {title}")
        lines.append(text)
        lines.append("")
        items.append({"flag": flag, "title": title})
    if flags.get("user_message_waiting"):
        lines.append("[ ] 当前节律项结算后，继续本轮后续交互/任务指南；不要直接结束本轮。")
    else:
        lines.append("[ ] 本轮必要事项完成后，直接自然语言回复用户；需要跨轮继续才调用 reaction_finalize(handoff_text)。")
    return {
        "kind": "calendar_rhythm_guide",
        "text": "\n".join(lines).rstrip(),
        "items": items,
    }


def _interaction_guide(flags, completed_flags=None):
    lines = [
        "GUIDE｜用户交互指南",
        "",
        "本轮 heartbeat_flags.user_message_waiting=true，需要处理用户刚刚输入的内容。",
        "",
    ]
    if any(flags.get(flag) for flag, _title, _text in CALENDAR_ITEMS):
        lines.extend([
            "本轮可与节律事项合轮；若节律已完成，现在继续处理真实用户输入。",
            "",
        ])
    lines.extend([
        f"{_checkbox('user_message_waiting', completed_flags)} 先确认用户真正要你处理的事项。",
        f"{_checkbox('user_message_waiting', completed_flags)} 需要工具时调用合适工具；不需要工具时直接组织回答。",
        f"{_checkbox('user_message_waiting', completed_flags)} 完成后直接自然语言回复用户；需要跨轮继续才调用 reaction_finalize(handoff_text)。",
    ])
    return {
        "kind": "interactive_guide",
        "text": "\n".join(lines).rstrip(),
        "items": [{"flag": "user_message_waiting", "title": "用户交互"}],
    }


def _relay_guide(flags, completed_flags=None):
    marker = _checkbox("continue_requested", completed_flags)
    return {
        "kind": "relay_guide",
        "text": "\n".join([
            "GUIDE｜中继规划指南",
            "",
            "本轮需要处理尚未完成的中继意图。",
            "",
            f"{marker} 读取当前可见中继意图，判断能否合题、继续、反问或搁置。",
            f"{marker} 需要更新中继意图状态时调用 relay_intent_settle。",
            f"{marker} 完成后直接自然语言回复用户；需要跨轮继续才调用 reaction_finalize(handoff_text)。",
        ]).rstrip(),
        "items": [{"flag": "continue_requested", "title": "中继意图"}],
    }


def current_guide(flags, completed_flags=None):
    """Return the single current guide Runtime should show."""
    flags = flags or {}
    decision = round_decision_from_heartbeat_flags(flags)
    completed_flags = _completed_set(completed_flags)
    head = _current_guide_item(decision, completed_flags)
    if head.get("kind") == "emergency":
        items = [
            item for item in active_emergency_items(flags)
            if item["flag"] not in completed_flags
        ]
        lines = [
            "GUIDE｜紧急处理指南",
            "",
            "本轮存在紧急处理事项。请处理下面仍未完成的项目。",
            "",
        ]
        for item in items:
            lines.append(f"{_checkbox(item['flag'], completed_flags)} {item['title']}")
            lines.append(item["text"])
            lines.append("")
        return {
            "kind": "emergency_handling_guide",
            "text": "\n".join(lines).rstrip(),
            "items": items,
        }
    if head.get("kind") == "context_pressure":
        return _context_pressure_guide(flags, completed_flags)
    if head.get("kind") == "cache_compaction":
        return _cache_compaction_guide(flags, completed_flags)
    if head.get("kind") == "main_axis_rhythm":
        return _main_axis_guide(flags, completed_flags)
    if str(head.get("kind") or "").startswith("calendar_"):
        target_flag = (head.get("flags") or [""])[0]
        return _calendar_guide(flags, completed_flags, target_flag=target_flag)
    if head.get("kind") == "interaction":
        if _has_higher_priority_before_interaction(decision, completed_flags):
            return {"kind": "reaction_step_guide", "text": ""}
        return _interaction_guide(flags, completed_flags)
    if head.get("kind") == "relay":
        return _relay_guide(flags, completed_flags)
    items = [
        item for item in active_emergency_items(flags)
        if item["flag"] not in completed_flags
    ]
    if items:
        lines = [
            "GUIDE｜紧急处理指南",
            "",
            "本轮存在紧急处理事项。请处理下面仍未完成的项目。",
            "",
        ]
        for item in items:
            lines.append(f"{_checkbox(item['flag'], completed_flags)} {item['title']}")
            lines.append(item["text"])
            lines.append("")
        return {
            "kind": "emergency_handling_guide",
            "text": "\n".join(lines).rstrip(),
            "items": items,
        }
    context_items = [
        item for item in active_context_pressure_items(flags)
        if item["flag"] not in completed_flags
    ]
    if context_items:
        return _context_pressure_guide(flags, completed_flags)
    return {"kind": "reaction_step_guide", "text": ""}


def emergency_attempt_decision(alert_type, attempts):
    """Bound emergency handling loops without asking the model to close out."""
    try:
        attempts = int(attempts or 0)
    except (TypeError, ValueError):
        attempts = 0
    if attempts >= 10:
        return {
            "alert_type": str(alert_type or "").strip(),
            "action": "auto_defer",
            "defer_seconds": DEFAULT_DEFER_SECONDS,
        }
    if attempts >= 8:
        return {
            "alert_type": str(alert_type or "").strip(),
            "action": "nudge_defer",
            "defer_seconds": DEFAULT_DEFER_SECONDS,
        }
    return {
        "alert_type": str(alert_type or "").strip(),
        "action": "continue",
        "defer_seconds": DEFAULT_DEFER_SECONDS,
    }


def render_current_guide_popup(flags, completed_flags=None):
    guide = current_guide(flags, completed_flags=completed_flags)
    if not guide.get("text"):
        return ""
    return (
        f"- kind: {guide['kind']}\n"
        "  tier: guide\n"
        "  decision_required: false\n"
        "  source: runtime.rhythm_guidance\n"
        "  message: |\n"
        + "\n".join(
            f"    {line}" if line else ""
            for line in guide["text"].splitlines()
        )
    )
