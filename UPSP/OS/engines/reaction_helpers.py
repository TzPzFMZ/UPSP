"""ReactionLoopRunner 的循环投影 helper。

这里放只处理 reaction 结果投影、feedback 文本、handoff 路由和幂等合并的函数。
主循环的迭代顺序、native tool 执行顺序和 finalize 消费规则仍留在 ReactionLoopRunner。
"""

import json
import re
from pathlib import PurePosixPath, PureWindowsPath

from data.relation_store import relation_public_name
from logic.evidence_refs import evidence_handle_for_result, result_supports_evidence
from logic.write_pending_settlement import format_pending_cancel_tool_fact

from logic.handoff_prefixes import prefix_reaction_loop_handoff
from engines.reaction_protocol_tool_execution import model_visible_error_hint


def reaction_identity_requires_resolution(interaction_meta):
    """判断本轮身份是否仍需反应步处理。"""
    if not isinstance(interaction_meta, dict):
        return False
    source = str(interaction_meta.get("interaction_source") or "").strip()
    if source in {"no_external_input", "system"}:
        return False
    obj = str(interaction_meta.get("interaction_object") or "").strip()
    status = str(interaction_meta.get("identity_status") or "").strip()
    unknown_objects = {"", "unknown", "Unknown", "UNKNOWN", "none", "None", "-"}
    unknown_statuses = {"", "unknown", "Unknown", "UNKNOWN", "timeout"}
    return obj in unknown_objects or status in unknown_statuses


def reaction_identity_has_blocked_activity(parsed_reaction):
    """身份未确认时，判断是否夹带了应被拦截的物质动作。"""
    parsed_reaction = parsed_reaction or {}
    blocked_keys = (
        "protocol_tool_requests",
        "protocol_tool_submissions",
        "general_tool_requests",
        "relation_card_declarations",
        "memory_write_declarations",
        "memory_annotation_declarations",
        "memory_recall_completion_requests",
        "memory_link_update_declarations",
        "memory_container_create_declarations",
        "memory_container_write_declarations",
        "memory_privacy_declarations",
        "memory_privacy_declassify_declarations",
        "chronicle_write_declarations",
        "alert_mode_settle_declarations",
        "fault_record_declarations",
        "container_focus_declarations",
        "pending_cancel_requests",
    )
    return any(parsed_reaction.get(key) for key in blocked_keys)


def record_pending_memory_ids(pending_memory_ids, receipts):
    """把本迭代新建记忆映射回 pending 占位符。"""
    applied_ids = [
        receipt.get("mem_id")
        for receipt in receipts or []
        if receipt.get("status") == "applied" and receipt.get("mem_id")
    ]
    if not applied_ids:
        return
    existing_numbers = []
    for key in pending_memory_ids:
        if not str(key).startswith("PENDING-"):
            continue
        try:
            existing_numbers.append(int(str(key).split("-", 1)[1]))
        except (TypeError, ValueError, IndexError):
            continue
    next_index = max(existing_numbers or [0]) + 1
    for offset, mem_id in enumerate(applied_ids):
        pending_memory_ids[f"PENDING-{next_index + offset}"] = mem_id
        pending_memory_ids["PENDING"] = mem_id
        pending_memory_ids["PENDING-1"] = mem_id


def attach_native_trace_to_receipts(receipts, declarations):
    """把 provider-native trace 附加到 processor receipt 上，供审计串联。"""
    trace_keys = (
        "call_id",
        "provider",
        "response_id",
        "provider_item_id",
        "index",
        "resolves_pending_id",
    )
    traces = []
    for declaration in declarations or []:
        if not isinstance(declaration, dict):
            traces.append({})
            continue
        trace = {
            key: declaration.get(key)
            for key in trace_keys
            if declaration.get(key) not in (None, "")
        }
        traces.append(trace)
    if not traces:
        return receipts
    for index, receipt in enumerate(receipts or []):
        if not isinstance(receipt, dict):
            continue
        trace = traces[index] if index < len(traces) else traces[-1]
        for key, value in trace.items():
            receipt.setdefault(key, value)
    return receipts


def _message_receipt_call_ids(message):
    values = []
    if not isinstance(message, dict):
        return values
    raw = message.get("_tool_fact_receipt_call_ids") or []
    if isinstance(raw, str):
        raw = [raw]
    for item in raw:
        text = str(item or "").strip()
        if text:
            values.append(text)
    return values


def remove_settled_write_pending_context(
        accumulated_messages,
        pending_native_tool_feedbacks,
        settled_pendings):
    """Drop stale pending reminder facts after a write retry resolves them."""
    pending_ids = set()
    call_ids = set()
    for pending in settled_pendings or []:
        if not isinstance(pending, dict):
            continue
        pending_id = str(pending.get("pending_id") or "").strip()
        call_id = str(pending.get("call_id") or "").strip()
        if pending_id:
            pending_ids.add(pending_id)
        if call_id:
            call_ids.add(call_id)
        raw_call_ids = pending.get("call_ids") or []
        if isinstance(raw_call_ids, str):
            raw_call_ids = [raw_call_ids]
        for item in raw_call_ids:
            text = str(item or "").strip()
            if text:
                call_ids.add(text)
    if not pending_ids and not call_ids:
        return

    kept_messages = []
    for message in accumulated_messages or []:
        content = str((message or {}).get("content") or "")
        message_call_ids = set(_message_receipt_call_ids(message))
        if pending_ids and any(pending_id in content for pending_id in pending_ids):
            continue
        if call_ids and message_call_ids.intersection(call_ids):
            continue
        kept_messages.append(message)
    accumulated_messages[:] = kept_messages

    kept_feedbacks = []
    for feedback in pending_native_tool_feedbacks or []:
        text = str(feedback or "")
        if pending_ids and any(pending_id in text for pending_id in pending_ids):
            continue
        kept_feedbacks.append(feedback)
    pending_native_tool_feedbacks[:] = kept_feedbacks


def reaction_loop_has_other_activity(parsed_reaction):
    """结束判定/收束终步的护栏：reaction_loop 之外不得夹带工具动作。"""
    parsed_reaction = parsed_reaction or {}
    activity_keys = (
        "protocol_tool_requests",
        "protocol_tool_submissions",
        "invalid_protocol_tool_submissions",
        "general_tool_requests",
        "invalid_tool_requests",
        "tool_summaries",
        "relation_card_declarations",
        "memory_write_declarations",
        "memory_annotation_declarations",
        "memory_recall_completion_requests",
        "memory_link_update_declarations",
        "memory_container_create_declarations",
        "memory_container_write_declarations",
        "memory_privacy_declarations",
        "memory_privacy_declassify_declarations",
        "chronicle_write_declarations",
        "alert_mode_settle_declarations",
        "fault_record_declarations",
        "container_focus_declarations",
        "pending_cancel_requests",
    )
    return any(parsed_reaction.get(key) for key in activity_keys)


def reaction_loop_has_protocol_submission_activity(parsed_reaction):
    """已加载 guide 的协议提交不应被结束判定夹带，也不能静默丢单。"""
    parsed_reaction = parsed_reaction or {}
    submission_keys = (
        "protocol_tool_submissions",
        "relation_card_declarations",
        "memory_write_declarations",
        "memory_annotation_declarations",
        "memory_recall_completion_requests",
        "memory_link_update_declarations",
        "memory_container_create_declarations",
        "memory_container_write_declarations",
        "memory_privacy_declarations",
        "memory_privacy_declassify_declarations",
        "chronicle_write_declarations",
        "alert_mode_settle_declarations",
        "fault_record_declarations",
        "container_focus_declarations",
        "pending_cancel_requests",
    )
    return any(parsed_reaction.get(key) for key in submission_keys)


def format_protocol_tool_material_entry(receipt):
    """Project protocol read content as material when no CONTENT mount exists."""
    if not isinstance(receipt, dict):
        return None
    tool_id = str(receipt.get("tool_id") or "").strip()
    status = str(receipt.get("status") or "").strip()
    if status not in {"ok", "accepted", "applied", "success"}:
        return None
    if tool_id != "index_view":
        return None
    content = str(receipt.get("content") or "").strip()
    if not content:
        return None
    title = "index_view scope={} offset={} limit={}".format(
        receipt.get("scope") or "",
        receipt.get("offset") or "",
        receipt.get("limit") or "",
    ).strip()
    return {
        "role": "system",
        "kind": "material",
        "content": content,
        "tool_id": tool_id,
        "title": title,
        "material_source": "read_tool_result",
    }


def protocol_receipt_should_enter_tool_fact(receipt):
    """Protocol receipts always produce a fact when they have a visible status."""
    if not isinstance(receipt, dict):
        return False
    status = str(receipt.get("status") or "").strip()
    if status == "submission_received":
        return False
    return bool(status)


def _fact_value(value):
    if value in (None, "", []):
        return ""
    if isinstance(value, list):
        return ",".join(str(item) for item in value if item not in (None, ""))
    text = str(value).replace("\r\n", "\n").replace("\n", " ").strip()
    if len(text) > 220:
        text = text[:217] + "..."
    return text


def _fact_multiline(value, limit=4000):
    if value in (None, "", []):
        return ""
    if isinstance(value, (dict, list)):
        text = json.dumps(value, ensure_ascii=False)
    else:
        text = str(value)
    text = text.replace("\r\n", "\n").strip()
    if len(text) > limit:
        text = text[:max(0, limit - 3)] + "..."
    return text


def _first_fact_value(mapping, keys):
    if not isinstance(mapping, dict):
        return ""
    for key in keys:
        value = _fact_value(mapping.get(key))
        if value:
            return value
    return ""


def _append_fact_line(lines, label, value):
    value = _fact_value(value)
    if value:
        lines.append(f"{label}：{value}。")


def _guide_fact_status(receipt):
    return str((receipt or {}).get("status") or "unknown").strip()


def _guide_fact_success(receipt):
    return _guide_fact_status(receipt) in {"ok", "accepted", "applied", "success"}


def _guide_submissions(receipt):
    return [
        item for item in (receipt or {}).get("accepted_submissions") or []
        if isinstance(item, dict)
    ]


def _submission_option_ids(submissions):
    return {
        str(item.get("option_id") or "").strip()
        for item in submissions or []
        if str(item.get("option_id") or "").strip()
    }


def _backend_receipts(receipt, tool_id=None):
    results = []
    for item in (receipt or {}).get("backend_receipts") or []:
        if not isinstance(item, dict):
            continue
        if tool_id and str(item.get("tool_id") or "").strip() != tool_id:
            continue
        results.append(item)
    return results


def _task_id_from_guide_receipt(receipt):
    for value in (
            receipt.get("task_id"),
            (receipt.get("task_completion") or {}).get("task_id")
            if isinstance(receipt.get("task_completion"), dict) else None,
            (receipt.get("task_update") or {}).get("task_id")
            if isinstance(receipt.get("task_update"), dict) else None,
            (receipt.get("pending_input_update") or {}).get("task_id")
            if isinstance(receipt.get("pending_input_update"), dict) else None,
    ):
        text = _fact_value(value)
        if text:
            return text
    guide_id = str(receipt.get("guide_id") or "").strip()
    if guide_id.startswith("task:"):
        return guide_id.split(":", 1)[1].strip()
    active_guide = str(receipt.get("active_guide") or "").strip()
    if active_guide.startswith("task:"):
        return active_guide.split(":", 1)[1].strip()
    return ""


def _load_task_guide_for_fact(receipt, fact_context=None):
    task_id = _task_id_from_guide_receipt(receipt)
    store = (fact_context or {}).get("workbench_store") if isinstance(fact_context, dict) else None
    if not task_id or store is None or not hasattr(store, "load_task_guide"):
        return task_id, {}
    try:
        guide = store.load_task_guide(task_id)
    except Exception:
        guide = {}
    return task_id, guide if isinstance(guide, dict) else {}


def _record_id(record, keys):
    if not isinstance(record, dict):
        return ""
    for key in keys:
        value = _fact_value(record.get(key))
        if value:
            return value
    return ""


def _record_text(record):
    return _first_fact_value(
        record,
        ("title", "description", "summary", "text", "name", "content"),
    )


def _append_records(lines, title, records, id_keys, *, include_status=True):
    rows = [item for item in records or [] if isinstance(item, dict)]
    if not rows:
        return
    lines.append(f"{title}：")
    for record in rows:
        parts = []
        record_id = _record_id(record, id_keys)
        if record_id:
            parts.append(record_id)
        if include_status:
            status = _fact_value(record.get("status"))
            if status:
                parts.append(f"status={status}")
        if "required" in record:
            parts.append(f"required={bool(record.get('required'))}")
        text = _record_text(record)
        if text:
            parts.append(text)
        for label, key in (
                ("requirements", "requirement_refs"),
                ("items", "item_refs"),
                ("sources", "source_refs"),
                ("evidence", "evidence_refs"),
                ("faults", "fault_refs"),
        ):
            value = _fact_value(record.get(key))
            if value:
                parts.append(f"{label}={value}")
        if parts:
            lines.append("- " + " | ".join(parts))


def _append_task_guide_snapshot(lines, task_id, guide):
    _append_fact_line(lines, "任务编号", task_id or (guide or {}).get("task_id"))
    _append_fact_line(lines, "任务标题", (guide or {}).get("task_title"))
    _append_fact_line(lines, "任务目标", (guide or {}).get("task_goal"))
    _append_records(
        lines,
        "来源需求",
        (guide or {}).get("source_requirements"),
        ("requirement_id", "id"),
        include_status=False,
    )
    _append_records(
        lines,
        "任务项",
        (guide or {}).get("items"),
        ("item_id", "id"),
    )
    _append_records(
        lines,
        "验收项",
        (guide or {}).get("acceptance"),
        ("acceptance_id", "id"),
    )
    _append_records(
        lines,
        "待整合输入",
        (guide or {}).get("pending_inputs"),
        ("pending_input_id", "pending_id", "id"),
    )
    _append_fact_line(lines, "来源引用", (guide or {}).get("source_refs"))
    _append_fact_line(lines, "风险备注", (guide or {}).get("risk_notes"))


def _format_task_guide_fact(receipt, fact_context=None):
    task_id, guide = _load_task_guide_for_fact(receipt, fact_context)
    task_completion = receipt.get("task_completion")
    task_update = receipt.get("task_update")
    pending_input_update = receipt.get("pending_input_update")
    submissions = _guide_submissions(receipt)
    option_ids = _submission_option_ids(submissions)
    if isinstance(task_completion, dict):
        title = "本轮任务清单完成事实"
        status_line = (
            "清单状态：required items/acceptance 已通过强证据验收，"
            "active task guide 已撤下。"
        )
    elif "submit_initial_guide" in option_ids and task_id:
        title = "本轮任务清单创建事实"
        status_line = "清单状态：已创建 task execution guide。"
    elif isinstance(pending_input_update, dict):
        title = "本轮任务输入整合事实"
        status_line = "清单状态：已处理 pending input 更新。"
    elif isinstance(task_update, dict):
        title = "本轮任务清单更新事实"
        status_line = "清单状态：已更新 task guide 进度。"
    else:
        return ""

    lines = [f"【{title}】", f"处理结果：{_guide_fact_status(receipt)}。"]
    if status_line:
        lines.append(status_line)
    _append_task_guide_snapshot(lines, task_id, guide)
    if not guide:
        if isinstance(task_update, dict):
            _append_records(
                lines,
                "本次更新任务项",
                task_update.get("updated_items"),
                ("item_id", "id"),
            )
            _append_records(
                lines,
                "本次更新验收项",
                task_update.get("updated_acceptance"),
                ("acceptance_id", "id"),
            )
    return "\n".join(line for line in lines if line).strip()


def _format_chronicle_guide_fact(receipt, submissions):
    chronicle_submissions = [
        item for item in submissions
        if str(item.get("option_id") or "").strip() == "write_chronicle"
    ]
    if not chronicle_submissions:
        return ""
    lines = [
        "【本轮编年史写入事实】",
        f"处理结果：{_guide_fact_status(receipt)}。",
    ]
    _append_fact_line(lines, "指南编号", receipt.get("guide_id"))
    for submission in chronicle_submissions:
        fields = submission.get("fields") if isinstance(submission.get("fields"), dict) else {}
        item_id = _fact_value(submission.get("item_id"))
        if item_id:
            lines.append(f"完成项：{item_id} / write_chronicle。")
        _append_fact_line(lines, "证据引用", submission.get("evidence_refs"))
        _append_fact_line(lines, "原因", fields.get("reason") or submission.get("reason"))
        content = _fact_multiline(fields.get("content"), limit=4000)
        if content:
            lines.append("本次写入正文：")
            lines.append(content)
    for backend in _backend_receipts(receipt, "chronicle_write"):
        _append_fact_line(lines, "写入层", backend.get("layer"))
        _append_fact_line(lines, "写入路径", backend.get("path") or backend.get("write_path"))
        _append_fact_line(lines, "写入轮次", backend.get("round_num"))
    _append_fact_line(lines, "已完成 flags", receipt.get("completed_flags"))
    return "\n".join(line for line in lines if line).strip()


def _format_alert_guide_fact(receipt, submissions):
    wanted_options = {"settle_alert", "record_fault"}
    alert_submissions = [
        item for item in submissions
        if str(item.get("option_id") or "").strip() in wanted_options
    ]
    if not alert_submissions:
        return ""
    lines = [
        "【本轮紧急/上下文清单事实】",
        f"处理结果：{_guide_fact_status(receipt)}。",
    ]
    _append_fact_line(lines, "指南编号", receipt.get("guide_id"))
    for submission in alert_submissions:
        fields = submission.get("fields") if isinstance(submission.get("fields"), dict) else {}
        option_id = _fact_value(submission.get("option_id"))
        item_id = _fact_value(submission.get("item_id"))
        head = " / ".join(part for part in (item_id, option_id) if part)
        if head:
            lines.append(f"处理项：{head}。")
        for label, key in (
                ("告警类型", "alert_type"),
                ("结算状态", "status"),
                ("摘要", "summary"),
                ("清理 flags", "clear_flags"),
                ("故障引用", "fault_refs"),
                ("后续关注", "next_attention"),
                ("故障类型", "fault_type"),
                ("严重级别", "severity"),
                ("发生步骤", "step"),
                ("来源", "source"),
                ("细节", "detail"),
                ("处理动作", "action"),
                ("关联工具", "related_tool_id"),
        ):
            _append_fact_line(lines, label, fields.get(key))
    for backend in _backend_receipts(receipt):
        tool_id = str(backend.get("tool_id") or "").strip()
        if tool_id not in {"alert_mode_settle", "fault_record"}:
            continue
        _append_fact_line(lines, "后台处理器", tool_id)
        _append_fact_line(lines, "后台状态", backend.get("status"))
        _append_fact_line(lines, "告警类型", backend.get("alert_type"))
        _append_fact_line(lines, "清理 flags", backend.get("cleared_flags"))
        _append_fact_line(lines, "故障编号", backend.get("fault_id"))
        _append_fact_line(lines, "故障引用", backend.get("fault_refs"))
    _append_fact_line(lines, "已完成 flags", receipt.get("completed_flags"))
    return "\n".join(line for line in lines if line).strip()


def _format_cache_compaction_guide_fact(receipt, submissions):
    cache_submissions = [
        item for item in submissions
        if str(item.get("option_id") or "").strip() == "submit_cache_compaction_shard"
    ]
    cache_status = receipt.get("cache_compaction")
    if not cache_submissions and not isinstance(cache_status, dict):
        return ""
    lines = [
        "【本轮缓存压缩清单事实】",
        f"处理结果：{_guide_fact_status(receipt)}。",
    ]
    _append_fact_line(lines, "指南编号", receipt.get("guide_id"))
    if isinstance(cache_status, dict):
        for label, key in (
                ("压缩状态", "status"),
                ("压缩前字符数", "before_chars"),
                ("目标字符数", "target_chars"),
                ("当前字符数", "current_chars"),
                ("已完成分片", "completed_shards"),
                ("剩余分片", "remaining_shards"),
                ("跳过分片", "skipped_shards"),
                ("目标达成", "target_met"),
                ("全部完成", "all_done"),
        ):
            _append_fact_line(lines, label, cache_status.get(key))
    for submission in cache_submissions:
        fields = submission.get("fields") if isinstance(submission.get("fields"), dict) else {}
        item_id = _fact_value(submission.get("item_id"))
        if item_id:
            lines.append(f"处理项：{item_id} / submit_cache_compaction_shard。")
        for label, key in (
                ("分片编号", "shard_id"),
                ("来源块", "source_block_ids"),
                ("压缩摘要", "summary"),
                ("输入字符数", "input_chars"),
                ("输出字符数", "output_chars"),
                ("压缩比例", "compact_ratio"),
        ):
            _append_fact_line(lines, label, fields.get(key))
    for backend in _backend_receipts(receipt, "cache_compact"):
        _append_fact_line(lines, "后台处理器", backend.get("tool_id"))
        _append_fact_line(lines, "后台状态", backend.get("status"))
        _append_fact_line(lines, "后台分片", backend.get("shard_id"))
    _append_fact_line(lines, "已完成 flags", receipt.get("completed_flags"))
    return "\n".join(line for line in lines if line).strip()


def _format_guide_submit_tool_fact(receipt, fact_context=None):
    failure_fact = _format_guide_submit_failure_fact(receipt)
    if failure_fact:
        return failure_fact
    if not _guide_fact_success(receipt):
        return ""
    sections = []
    task_fact = _format_task_guide_fact(receipt, fact_context=fact_context)
    if task_fact:
        sections.append(task_fact)
    submissions = _guide_submissions(receipt)
    for formatter in (
            _format_chronicle_guide_fact,
            _format_alert_guide_fact,
            _format_cache_compaction_guide_fact,
    ):
        fact = formatter(receipt, submissions)
        if fact:
            sections.append(fact)
    return "\n\n".join(sections).strip()


def _coerce_fact_ref_list(value, limit=None):
    if value is None:
        refs = []
    elif isinstance(value, list):
        refs = value
    elif isinstance(value, (tuple, set)):
        refs = list(value)
    else:
        refs = [value]
    result = []
    for item in refs:
        text = str(item or "").strip()
        if text and text not in result:
            result.append(text)
        if limit and len(result) >= limit:
            break
    return result


def _evidence_fact_rank(ref):
    text = str(ref or "").strip()
    if text.startswith("EV-"):
        return (0, len(text), text)
    if text.startswith("command:"):
        return (1, len(text), text)
    if text.startswith("call:") or re.match(r"^call_[A-Za-z0-9_-]+$", text):
        return (2, len(text), text)
    if text.startswith(("file:", "file_write:", "file_read:")):
        return (3, len(text), text)
    if "/" in text or "\\" in text:
        return (4, len(text), text)
    return (5, len(text), text)


def _usable_evidence_fact_refs(details, limit=8):
    if not isinstance(details, dict):
        return []
    refs = _coerce_fact_ref_list(details.get("known_evidence_refs"))
    refs.sort(key=_evidence_fact_rank)
    return refs[:limit]


def _usable_evidence_fact_items(details, limit=8):
    if not isinstance(details, dict):
        return []
    raw = details.get("known_evidence_items") or []
    if not isinstance(raw, list):
        return []
    items = []
    seen = set()
    for item in raw:
        if not isinstance(item, dict):
            continue
        ref = str(item.get("ref") or "").strip()
        if not ref or ref in seen:
            continue
        summary = str(item.get("summary") or "").strip()
        if not summary:
            tool_id = str(item.get("tool_id") or "").strip()
            summary = f"{tool_id} 证据" if tool_id else "Runtime evidence"
        items.append({"ref": ref, "summary": summary})
        seen.add(ref)
        if len(items) >= limit:
            break
    return items


def _append_fact_refs(lines, title, refs):
    if not refs:
        return
    lines.append(f"{title}：")
    for ref in refs:
        lines.append(f"- {ref}")


def _append_fact_evidence_items(lines, title, items):
    if not items:
        return
    lines.append(f"{title}：")
    for item in items:
        lines.append(f"- {item['ref']} — {item['summary']}")


def _format_guide_submit_failure_fact(receipt):
    status = str((receipt or {}).get("status") or "").strip()
    if status in {"ok", "accepted", "applied", "success", "guide_loaded"}:
        return ""
    reason = str((receipt or {}).get("reason") or "").strip()
    if not reason:
        return ""
    if reason == "pending_interaction_first":
        pending_ids = (receipt or {}).get("pending_input_ids") or []
        if not isinstance(pending_ids, list):
            pending_ids = [pending_ids]
        pending_text = ", ".join(
            str(item).strip() for item in pending_ids if str(item).strip()
        )
        lines = [
            "【本轮指南提交回执】",
            "处理结果：rejected。",
            "失败原因：pending_interaction_first。",
        ]
        if pending_text:
            lines.append(f"待整合输入：{pending_text}。")
        lines.append(
            "下一步：先用 guide_submit(task_progress, integrate_pending_input) "
            "结算上述 pending_inputs，再提交 update_task_status。"
        )
        return "\n".join(lines)
    if reason == "task_completion_evidence_not_found":
        details = (receipt or {}).get("details") or {}
        unknown = []
        if isinstance(details, dict):
            unknown = (
                details.get("unknown_evidence_refs")
                or details.get("unknown_refs")
                or details.get("unknown")
                or []
            )
        unknown_refs = _coerce_fact_ref_list(unknown, limit=8)
        usable_refs = _usable_evidence_fact_refs(details, limit=8)
        usable_items = _usable_evidence_fact_items(details, limit=8)
        lines = [
            "【本轮指南提交回执】",
            "处理结果：rejected。",
            "失败原因：task_completion_evidence_not_found。",
        ]
        _append_fact_refs(lines, "未知证据引用", unknown_refs)
        if usable_items:
            _append_fact_evidence_items(lines, "可改用证据（含来源）", usable_items)
        else:
            _append_fact_refs(lines, "可改用证据引用", usable_refs)
        hint = ""
        if isinstance(details, dict):
            hint = str(details.get("hint") or "").strip()
        if hint:
            lines.append(f"下一步：{hint}")
        else:
            lines.append("下一步：删除上方未知证据，或替换为可改用证据引用；不要重复提交同一未知证据。")
        return "\n".join(lines)
    if reason == "relay_task_guidance_not_pending_input":
        lines = [
            "【本轮指南提交回执】",
            "处理结果：rejected。",
            "失败原因：relay_task_guidance_not_pending_input。",
            "下一步：这是 relay 续航轮，不要把续航文本登记为新的 pending_input；继续当前任务执行，或用 update_task_status 提交已有任务进度。",
        ]
        return "\n".join(lines)
    return ""


def _memory_body_too_long_fact_lines(reason, receipt):
    max_chars = receipt.get("max_chars")
    actual_chars = receipt.get("actual_chars")
    over_by = receipt.get("over_by")
    if max_chars is None or actual_chars is None or over_by is None:
        max_chars, actual_chars, over_by = parse_memory_body_too_long(reason)
    if max_chars and actual_chars:
        first = (
            "memory_write.body 超出当前权重上限："
            f"actual={actual_chars}, max={max_chars}。"
        )
    else:
        first = "memory_write.body 超出当前权重上限。"
    return [
        "失败原因：memory_body_too_long。",
        first,
        "请压缩正文或调整 weight 后重新调用 memory_write。",
        "不要只因字数升权。",
    ]


SUBJECT_RESOLUTION_REASONS = {
    "identity_unresolved",
    "subject_not_in_relation_domain",
    "subject_not_confirmed",  # historical receipt compatibility
}


def _memory_subject_values(item):
    submitted = relation_public_name(_fact_value(
        item.get("submitted_subject") or item.get("subject") or "unknown"))
    confirmed = relation_public_name(_fact_value(item.get("confirmed_subject")))
    confirmed_subjects = item.get("confirmed_subjects")
    if not confirmed and isinstance(confirmed_subjects, list) and confirmed_subjects:
        confirmed = relation_public_name(_fact_value(confirmed_subjects[0]))
    return submitted, confirmed


def _memory_subject_fact_lines(reason, receipt):
    submitted, confirmed = _memory_subject_values(receipt)
    candidate = relation_public_name(_fact_value(receipt.get("subject")))
    call_id = _fact_value(receipt.get("call_id"))
    lines = [f"失败原因：{reason}。"]
    if call_id:
        lines.append(f"调用编号：{call_id}。")
    if submitted:
        lines.append(f"提交主题：{submitted}。")
    if reason == "identity_unresolved":
        lines.append("当前确认对象：未确认。")
    elif confirmed:
        lines.append(f"当前确认对象：{confirmed}。")
    else:
        lines.append("当前确认对象：未记录。")
    if reason == "subject_not_in_relation_domain":
        lines.append("下一步：该记忆主体不在活动关系域；不得改填当前对象来伪造归属。")
        if confirmed and candidate == confirmed:
            lines.append(
                "该主体就是当前直接交互对象；确需沉淀时，可先通过现有关系卡写入流程合法登记，等待成功回执后再重试。"
            )
        else:
            lines.append("不得为缺席或无关第三方自动创建关系卡。")
    else:
        lines.append(
            "下一步：身份/主题未确认时禁止写记忆或关系卡；不要靠继续写入来做自修。"
        )
    lines.append(
        "如果任务硬验收依赖这条记忆，当前为 NO-GO：自然回复请求用户确认，或说明阻断事实。"
    )
    return lines


def format_protocol_tool_fact(receipt, fact_context=None):
    """Render a short model-visible fact for protocol receipts."""
    if not isinstance(receipt, dict):
        return ""
    tool_id = str(receipt.get("tool_id") or "protocol_tool").strip()
    if tool_id == "pending_cancel":
        return format_pending_cancel_tool_fact(receipt)
    if tool_id == "guide_submit":
        guide_fact = _format_guide_submit_tool_fact(receipt, fact_context=fact_context)
        if guide_fact:
            return guide_fact
    status = str(receipt.get("status") or "unknown").strip()
    if status == "submission_received":
        return ""
    title = {
        "memory_write": "本轮记忆写入回执",
        "memory_content_read": "本轮记忆读取回执",
        "corpus_read": "本轮语料展开回执",
        "index_view": "本轮索引查看回执",
        "container_read": "本轮容器读取回执",
        "memory_container_create": "本轮容器创建回执",
        "memory_container_write": "本轮容器写入回执",
        "memory_link_update": "本轮记忆关联回执",
        "relation_read": "本轮关系读取回执",
        "relation_card_write": "本轮关系卡写入回执",
        "fault_record": "本轮故障记录回执",
        "relay_intent_settle": "本轮中继意图结算回执",
        "chronicle_write": "本轮编年史写入回执",
    }.get(tool_id, "本轮协议工具回执")
    lines = [f"【{title}】", f"处理结果：{status}。"]
    success_status = status in {"ok", "accepted", "applied", "success"}
    if tool_id != "guide_submit" and result_supports_evidence(receipt):
        handle = evidence_handle_for_result(receipt)
        if handle:
            lines.append(f"证据引用：{handle}。")
    if tool_id == "memory_write" and success_status and receipt.get("write_pending_resolved"):
        lines.append("这次写入已成功补写先前失败的记忆。")
    if not success_status:
        reason = str(receipt.get("reason") or "").strip()
        if tool_id == "relation_card_write" and reason == "relation_index_write_failed":
            card_id = _fact_value(receipt.get("card_id"))
            lines.append("关系卡与 Registry 已写入；只有可重建的关系关键词索引投影失败。")
            if card_id:
                lines.append(f"关系卡编号：{card_id}。")
            lines.append("不要重复创建或重复写入关系卡；等待宿主修复关系关键词索引。")
        elif tool_id == "memory_write" and reason.startswith("memory_body_too_long:"):
            lines.extend(_memory_body_too_long_fact_lines(reason, receipt))
            stage = str(receipt.get("write_pending_stage") or "").strip()
            pending_id = _fact_value(receipt.get("write_pending_id"))
            cancel_available = bool(receipt.get("write_pending_cancel_available"))
            if stage == "settlement_required" and pending_id:
                lines.append(
                    f"这次重写仍失败；下一次重试必须填写 resolves_pending_id={pending_id}。"
                )
                if cancel_available:
                    lines.append(
                        f"只有确认放弃这次写入意图时，才调用 pending_cancel(pending_id={pending_id})。"
                    )
                else:
                    lines.append("这条写入不能取消；请继续修正后重试，或阻塞收束。")
            else:
                lines.append("请先修正后重新调用 memory_write，当前不能收束。")
        elif tool_id == "memory_write" and reason in SUBJECT_RESOLUTION_REASONS:
            lines.extend(_memory_subject_fact_lines(reason, receipt))
        elif tool_id == "chronicle_write" and reason == "no_active_chronicle_focus":
            call_id = _fact_value(receipt.get("call_id"))
            lines.append("工具：chronicle_write。")
            if call_id:
                lines.append(f"调用编号：{call_id}。")
            lines.append("失败原因：no_active_chronicle_focus。")
            lines.append("当前没有编年史写入焦点；不要继续调用 chronicle_write。")
            lines.append(
                "请消费已有工具事实并继续推进；完成时直接自然语言回复用户，"
                "需要跨轮继续才调用 reaction_finalize(handoff_text)。"
            )
        else:
            lines.append("失败详情：请查看 POPUP 中相同 tool_id/call_id 的工具提醒。")
            lines.append("若 POPUP 与本工具事实的 tool_id/call_id 不对应，请忽略该 POPUP。")
    if str(receipt.get("reason") or "") in {
            "duplicate_protocol_read_satisfied",
            "duplicate_protocol_read_failure_repeated",
            "duplicate_container_focus_satisfied",
    }:
        lines.append("重复命中：本轮已有同一协议工具结果。")
        duplicate_of = _fact_value(receipt.get("duplicate_of_call_id"))
        if duplicate_of:
            lines.append(f"重复对象：{duplicate_of}。")
    for label, key in (
        ("记忆编号", "mem_id"),
        ("原始记忆编号", "raw_mem_id"),
        ("标题", "title"),
        ("权重", "weight"),
        ("记忆层", "memory_layer"),
        ("主题", "subject"),
        ("操作", "operation"),
        ("关联容器", "container_refs"),
        ("已挂接容器", "linked_containers"),
        ("容器编号", "container_id"),
        ("容器类型", "container_type"),
        ("目标文件", "target_file"),
        ("读取模式", "read_mode"),
        ("总行数", "total_lines"),
        ("总字符数", "total_chars"),
        ("写入路径", "write_path"),
        ("写入字数", "write_chars"),
        ("关键词", "keywords"),
        ("中继意图", "relay_intent_id"),
        ("中继状态", "final_status"),
        ("索引范围", "scope"),
        ("索引分区", "zone"),
        ("索引偏移", "offset"),
        ("索引数量", "limit"),
    ):
        value = _fact_value(receipt.get(key))
        if value:
            lines.append(f"{label}：{value}。")
    if success_status:
        value = _fact_value(receipt.get("reason"))
        if value:
            lines.append(f"原因：{value}。")
    return "\n".join(lines).strip()


def _unique_nonempty(values):
    result = []
    for value in values or []:
        text = str(value or "").strip()
        if text and text not in result:
            result.append(text)
    return result


def _file_read_ref(result):
    path = str(result.get("path") or "").strip()
    if not path:
        return ""
    start = result.get("start_line")
    end = result.get("end_line")
    if start not in (None, "") and end not in (None, ""):
        return f"{path}:{start}-{end}"
    return path


def enrich_reaction_finalize_settlement_refs(
        parsed_reaction,
        *,
        memory_write_receipts=None,
        general_tool_results=None,
        container_receipts=None):
    """Attach Runtime receipt refs without writing them into model form fields."""
    parsed = dict(parsed_reaction or {})
    memory_refs = _unique_nonempty(
        receipt.get("mem_id")
        for receipt in memory_write_receipts or []
        if isinstance(receipt, dict)
        and receipt.get("status") == "applied"
        and receipt.get("tool_id") == "memory_write"
    )
    read_refs = _unique_nonempty(
        _file_read_ref(result)
        for result in general_tool_results or []
        if isinstance(result, dict)
        and result.get("status") == "ok"
        and result.get("tool_id") == "file_read"
    )
    container_refs = _unique_nonempty(
        receipt.get("container_id")
        for receipt in container_receipts or []
        if isinstance(receipt, dict)
        and receipt.get("status") == "applied"
        and receipt.get("tool_id") in {
            "memory_container_create",
            "memory_container_write",
        }
    )
    parsed["runtime_settlement_refs"] = {
        "memory": memory_refs,
        "read": read_refs,
        "containers": container_refs,
    }
    return parsed


def settle_receipts_for_next_iteration(accumulated_messages, receipts):
    """Spec405: processor receipts no longer use accumulated_messages."""
    return []


def native_tool_failure_feedbacks(items):
    """把失败 receipt/result 渲染为 reaction POPUP warning 片段。"""
    feedbacks = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        call_id = str(item.get("call_id") or "").strip()
        if not call_id:
            continue
        status = str(item.get("status") or "").strip()
        reason = str(item.get("reason") or "").strip() or status
        try:
            result_count = int(item.get("result_count") or 0)
        except (TypeError, ValueError):
            result_count = -1
        if (
                item.get("tool_id") == "file_search"
                and status in {"ok", "success"}
                and result_count == 0):
            reason = "search_no_results"
        elif not reason or status in {"ok", "accepted", "applied", "guide_loaded", "success"}:
            continue
        elif (
                item.get("tool_id") == "relation_card_write"
                and status == "degraded"
                and reason == "relation_index_write_failed"):
            continue
        feedbacks.append(format_native_tool_failure_feedback(item, reason))
    return feedbacks


def format_native_tool_failure_feedback(item, reason):
    """渲染单条 native tool 失败纠错 POPUP。"""
    tool_id = safe_feedback_value(item.get("tool_id"))
    tool_label = native_tool_visible_label(tool_id)
    hint = model_visible_error_hint(item)
    next_action, message = native_tool_feedback_action(reason, item)
    if hint:
        if isinstance(item.get("error_hint"), dict):
            next_action = safe_feedback_value(hint.get("next_action"), limit=500)
        else:
            hint = {**hint, "next_action": next_action}
        message = list(message) + [
            f"kind={safe_feedback_value(hint.get('kind'))}",
            f"retry={safe_feedback_value(hint.get('retry'))}",
            f"attempted={safe_feedback_value(json.dumps(hint.get('attempted'), ensure_ascii=False))}",
            f"current={safe_feedback_value(json.dumps(hint.get('current'), ensure_ascii=False))}",
            f"expected={safe_feedback_value(json.dumps(hint.get('expected'), ensure_ascii=False))}",
            f"next_action={next_action}",
        ]
    duplicate_warning = reason in {
        "duplicate_tool_result_satisfied",
        "duplicate_tool_failure_repeated",
        "web_backend_exhausted_duplicate",
        "duplicate_protocol_read_satisfied",
        "duplicate_protocol_read_failure_repeated",
        "duplicate_container_focus_satisfied",
    }
    lines = [
        "- kind: native_tool_result",
        "  tier: warning",
        "  decision_required: false",
    ]
    if duplicate_warning:
        lines.append("  title: 工具循环警告")
    lines.extend([
        f"  tool_id: {tool_id}",
        f"  call_id: {safe_feedback_value(item.get('call_id'))}",
        f"  reason: {safe_feedback_value(reason)}",
    ])
    pending_id = ""
    if str(item.get("write_pending_stage") or "").strip() == "settlement_required":
        pending_id = safe_feedback_value(item.get("write_pending_id"))
    if pending_id:
        lines.append(f"  pending_id: {pending_id}")
    submitted_subject, confirmed_subject = _memory_subject_values(item)
    if submitted_subject:
        lines.append(f"  subject: {safe_feedback_value(submitted_subject)}")
    if confirmed_subject:
        lines.append(f"  confirmed_subject: {safe_feedback_value(confirmed_subject)}")
    if tool_id or reason or pending_id or submitted_subject:
        signature = "|".join([
            str(tool_id or ""),
            str(reason or ""),
            str(pending_id or ""),
            str(submitted_subject or ""),
        ])
        lines.append(f"  feedback_signature: {safe_feedback_value(signature, limit=240)}")
    for key in ("field", "expected", "actual"):
        value = safe_feedback_value(item.get(key))
        if value:
            lines.append(f"  {key}: {value}")
    lines.extend([
        f"  next_action: {next_action}",
        "  message: |",
    ])
    if duplicate_warning:
        lines.append(f"    {tool_label}已有同一工具结果；不要原样重复调用。")
    else:
        lines.append(f"    {tool_label}调用失败；不要声称该工具已经成功。")
    for line in message:
        lines.append(f"    {line}")
    lines.append("    下一次必须按上面的动作修正。")
    return "\n".join(lines)


def native_tool_visible_label(tool_id):
    value = str(tool_id or "").strip()
    labels = {
        "file_read": "文件读取工具",
        "file_search": "文件搜索工具",
        "file_edit": "文件编辑工具",
        "shell_command": "shell 命令工具",
        "subagent_dispatch": "子 agent 调度工具",
        "container_focus": "容器焦点工具",
        "container_read": "容器读取工具",
        "memory_write": "记忆写入工具",
    }
    return labels.get(value, "原生工具")


def _parent_path_hint(raw_path):
    text = str(raw_path or "").strip()
    if not text:
        return "", ""
    if "\\" in text or ":" in text:
        path = PureWindowsPath(text)
    else:
        path = PurePosixPath(text)
    parent = "" if str(path.parent) == "." else str(path.parent)
    return parent, path.name


def _candidate_keywords_list_example(raw_value):
    text = safe_feedback_value(raw_value, limit=160)
    if not text:
        return ""
    parts = [
        part.strip().strip("\"'")
        for part in re.split(r"[,，、;；\r\n]+", text)
    ]
    keywords = []
    seen = set()
    for part in parts:
        if not part or part in seen:
            continue
        seen.add(part)
        keywords.append(part)
    if not keywords:
        keywords = [text]
    return "candidate_keywords=" + json.dumps(keywords, ensure_ascii=False)


def native_tool_feedback_action(reason, item):
    """按失败原因给出下一迭代动作提示。"""
    field = str(item.get("field") or "").strip()
    if str(item.get("tool_id") or "").strip() == "memory_write" and reason in SUBJECT_RESOLUTION_REASONS:
        submitted, confirmed = _memory_subject_values(item)
        candidate = relation_public_name(_fact_value(item.get("subject")))
        tool_id = safe_feedback_value(item.get("tool_id")) or "memory_write"
        call_id = safe_feedback_value(item.get("call_id")) or "未记录"
        message = [
            f"失败定位：tool_id={tool_id}；call_id={call_id}；reason={safe_feedback_value(reason)}。",
        ]
        if reason == "identity_unresolved":
            message.append(
                f"当前没有已确认交互对象，memory_write.subject={submitted or 'unknown'} 不能落盘。"
            )
        else:
            message.append(
                f"本次 memory_write.subject={submitted or 'unknown'}，当前确认对象={confirmed or '未记录'}。"
            )
        if reason == "subject_not_in_relation_domain":
            message.append("该主体不在活动关系域；不要改填当前交互对象来伪造记忆归属。")
            if confirmed and candidate == confirmed:
                message.append(
                    "该主体是当前直接交互对象；确需作为记忆主体时，可先通过现有 relation_card_write 合法登记，等待 applied 回执后再重试。"
                )
            else:
                message.append("不得为缺席或无关第三方自动创建关系卡。")
            message.extend([
                "不要声称记忆已经写入成功；不要原样重试同一写入。",
                "如果任务不依赖该条记忆，可以说明阻断事实后自然收束。",
            ])
            return "choose_existing_relation_subject_or_register_current_object", message
        message.extend([
            "身份未确认或主题不匹配时，禁止继续调用 memory_write，也禁止通过关系卡写入自修。",
            "如果任务硬验收依赖这条记忆，当前为 NO-GO：自然回复请求用户确认，或说明阻断事实后收束。",
            "不要声称记忆已经写入成功；不要原样重试同一写入。",
        ])
        return "stop_memory_write_or_request_user_confirmation", message
    if str(reason or "").startswith("memory_body_too_long:"):
        max_chars = item.get("max_chars")
        actual_chars = item.get("actual_chars")
        over_by = item.get("over_by")
        if max_chars is None or actual_chars is None or over_by is None:
            max_chars, actual_chars, over_by = parse_memory_body_too_long(reason)
        pending_id = ""
        if str(item.get("write_pending_stage") or "").strip() == "settlement_required":
            pending_id = safe_feedback_value(item.get("write_pending_id"))
        cancel_available = bool(item.get("write_pending_cancel_available"))
        message = [
            (
                "memory_write.body 超出当前权重上限："
                f"actual={actual_chars}, max={max_chars}。"
            ),
            "请压缩正文或调整 weight 后重新调用 memory_write。",
            "不要只因字数升权。",
        ]
        if pending_id:
            message.append(
                "这条失败写入已有 pending_id："
                f"{pending_id}；压缩后重交必须填写 resolves_pending_id={pending_id}。"
            )
            if cancel_available:
                message.append("只有决定放弃这次写入意图时，才调用 pending_cancel。")
            else:
                message.append("这条写入不能取消；请继续修正后重试，或阻塞收束。")
        else:
            message.append("请先修正后重新调用 memory_write，当前不能收束。")
        return "compress_body_or_adjust_weight", message
    if reason == "native_argument_missing_required":
        if str(item.get("tool_id") or "").strip() == "reaction_finalize" and field == "handoff_text":
            return "provide_handoff_text_or_reply_naturally", [
                "reaction_finalize 只用于跨轮继续，必须填写非空 handoff_text。",
                "如果本轮已经完成，不要调用 reaction_finalize，直接用自然语言回复用户。",
            ]
        suffix = f"：`{field}`" if field else ""
        return "revise_arguments", [f"下一次调用必须填写该字段{suffix}。"]
    if reason == "reaction_finalize_retired_field":
        return "remove_retired_closeout_decision", [
            "reaction_finalize 的 closeout_decision 已退役；模型不再选择 finish、blocked 或 continue。",
            "完成时直接自然语言回复用户；只有跨轮继续时，才调用 reaction_finalize(handoff_text)。",
        ]
    if reason == "native_argument_unknown_field":
        target = f"`{field}` " if field else ""
        return "remove_unknown_field", [f"下一次调用必须删除未知字段 {target}，只保留 schema 暴露字段。"]
    if reason == "native_argument_invalid_type":
        if str(item.get("tool_id") or "").strip() == "memory_write" and field == "candidate_keywords":
            example = _candidate_keywords_list_example(item.get("actual_value_preview"))
            message = [
                "`candidate_keywords` 必须填写为关键词列表（字符串数组），不能写成逗号或顿号分隔的单个字符串。",
                "每个关键词单独作为一个列表元素；即使只有一个关键词也写成列表。",
            ]
            if example:
                message.append(f"请把你刚才提交的关键词改成这种格式：{example}")
                message.append("这个示例只使用你刚才提交过的关键词；不要新增刚才没有写过的新关键词。")
            return "revise_arguments", message
        target = f"`{field}` " if field else ""
        return "revise_arguments", [f"下一次调用必须把{target}改成期望类型。"]
    if reason == "native_argument_invalid_enum":
        target = f"`{field}` " if field else ""
        if str(item.get("tool_id") or "").strip() == "reaction_finalize" and field == "closeout_decision":
            actual = str(item.get("actual") or "").strip()
            return "remove_retired_closeout_decision", [
                f"closeout_decision={actual or '未记录'} 已退役；reaction_finalize 不再接收该字段。",
                "完成时直接自然语言回复用户；只有跨轮继续时，才调用 reaction_finalize(handoff_text)。",
            ]
        return "revise_arguments", [f"下一次调用{target}只能使用允许枚举值。"]
    if reason == "file_not_found":
        path = str(item.get("path") or "").strip()
        parent, name = _parent_path_hint(path)
        pattern = name.rsplit(".", 1)[0] + "*" if "." in name else (name or "*")
        search_line = (
            f"优先调用 file_search，root={parent}，pattern={pattern}，"
            "找到候选后用候选里的精确 path 重新调用 file_read。"
            if parent else
            "优先调用 file_search 搜索原路径附近的上级目录；找到候选后用精确 path 重新调用 file_read。"
        )
        return "search_parent_directory_or_retry_exact_path", [
            f"本次请求的路径不存在：{path or '未记录'}。",
            "先检查是否抄错点号、下划线、空格、中文文件名或扩展名。",
            search_line,
            "如果父目录也可能抄错，改搜上级目录；仍没有候选时再说明阻断。",
            "不要直接向用户要新路径，除非 file_search 也没有找到可用候选。",
        ]
    if reason == "search_no_results":
        root = str(item.get("root") or "").strip()
        pattern = str(item.get("pattern") or "").strip()
        return "change_search_arguments_or_finalize", [
            f"当前搜索窗口没有命中：root={root or '未记录'}；pattern={pattern or '未记录'}。",
            "下一次应换更宽的 pattern、换 root，或显式 recursive=true 搜索子目录。",
            "如果已经搜索过合理父目录和上级目录，提交 reaction_finalize 说明当前没有候选。",
            "不能声称整台机器都不存在，只能说当前搜索窗口没有命中。",
        ]
    if reason in {"guide_missing", "guide_not_loaded", "guide_required"}:
        return "stop_or_retry_with_valid_tool", [
            "guide 门禁已退役；改用当前已导出的 provider-native 工具，按 schema 修正参数，或停止调用旧路径。"
        ]
    if reason in {
        "capability_denied",
        "dangerous_shell_command",
        "outside_allowlist",
        "private_network_denied",
        "write_scope_missing",
    }:
        return "respect_capability_gate", [
            "遵守 ExecutionCapabilityGate，调整路径、命令、URL 或 write_scope 后再请求。"
        ]
    if reason == "duplicate_tool_result_satisfied":
        return "consume_existing_result_or_finalize", [
            "本轮已有同一工具结果，而且上一结果已经成功；不要重复调用同一工具和同一关键参数。",
            "请阅读上一条工具事实，用已有结果继续下一步；确实需要更多证据时换参数或换工具。",
            "如果任务已经够了，直接自然语言回复用户。",
        ]
    if reason == "duplicate_tool_failure_repeated":
        return "change_arguments_or_stop_retry", [
            "本轮同一工具和同一关键参数已经失败或被拒绝；原样重试不会产生新证据。",
            "请修正参数、换下一步工具，或自然说明当前无法继续的真实原因。",
        ]
    if reason == "web_backend_exhausted_duplicate":
        return "change_web_source_or_stop_retry", [
            "当前 URL 或查询的已知网页后端都已经失败；原样重试不会产生新证据。",
            "请换 URL、换搜索词、换非网页证据来源，或自然说明当前无法继续的真实原因。",
        ]
    if reason == "duplicate_container_focus_satisfied":
        return "consume_existing_focus_or_finalize", [
            "本轮同一容器焦点已经打开成功；不要重复调用 container_focus.open。",
            "请使用当前已可见的 WB focus 继续 memory_container_write；完成时直接自然语言回复用户。",
        ]
    if reason in {"native_argument_schema_missing", "native_protocol_write_not_enabled"}:
        return "stop_or_retry_with_valid_tool", [
            "停止调用未开通或缺少运行时契约的工具，改用当前已导出的 provider-native 工具。"
        ]
    if reason == "native_tool_call_required":
        tool_id = safe_feedback_value(item.get("tool_id")) or "当前步终端工具"
        return "call_required_native_tool", [
            f"下一次必须调用 provider-native `{tool_id}`；裸文本不能完成本步。"
        ]
    if reason == "assistant_text_tool_payload":
        return "remove_text_tool_payload", [
            "assistant_text 中的疑似工具载荷没有执行，也不能作为最终回复。",
            "下一次必须通过当前已导出的 provider-native 工具重新调用，或只输出普通自然语言。",
        ]
    return "inspect_failure", ["读取失败事实后再决定是否修正参数、改用有效 provider-native 工具或停止调用。"]


def parse_memory_body_too_long(reason):
    text = str(reason or "")
    max_chars = 0
    actual_chars = 0
    for part in text.split(":")[-1].split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        try:
            number = int(value)
        except ValueError:
            continue
        if key == "max":
            max_chars = number
        elif key == "actual":
            actual_chars = number
    return max_chars, actual_chars, max(0, actual_chars - max_chars)


def safe_feedback_value(value, limit=120):
    """清理 POPUP 中的短字段，避免表格/管道符污染。"""
    if value in (None, "", []):
        return ""
    if isinstance(value, (list, tuple, set)):
        text = ",".join(str(item) for item in value)
    elif isinstance(value, dict):
        text = ",".join(str(key) for key in sorted(value))
    else:
        text = str(value)
    text = " ".join(text.replace("|", "/").split())
    if len(text) > limit:
        return text[:limit - 3] + "..."
    return text


MOUNT_PAYLOAD_UPGRADE_FIELDS = {
    "source",
    "target_file",
    "path",
    "content",
    "read_mode",
    "range_requested",
    "range_applied",
    "total_lines",
    "total_chars",
}


def _upgrade_mount_request(existing, incoming):
    updated = dict(existing or {})
    has_existing_body = str(updated.get("content") or "") != ""
    for key in MOUNT_PAYLOAD_UPGRADE_FIELDS:
        if key not in incoming:
            continue
        value = incoming.get(key)
        if key == "content":
            if value not in (None, "") or not has_existing_body:
                updated[key] = value
            continue
        if value is None:
            continue
        if value == "" and updated.get(key) not in (None, ""):
            continue
        updated[key] = value
    return updated


def merge_mount_requests(existing, additions):
    """合并挂载请求并按 type/ids/mode 去重。"""
    merged = []
    seen = {}
    for item in list(existing or []) + list(additions or []):
        if not isinstance(item, dict):
            continue
        key = (
            str(item.get("type") or ""),
            str(item.get("ids") or ""),
            str(item.get("mode") or ""),
        )
        if key in seen:
            index = seen[key]
            merged[index] = _upgrade_mount_request(merged[index], item)
            continue
        seen[key] = len(merged)
        merged.append(item)
    return merged


def remove_memory_mount_requests(existing, mem_ids):
    """从本轮挂载投影中移除指定 memory id，不触碰持久层。"""
    targets = {str(item).strip() for item in mem_ids or [] if str(item).strip()}
    if not targets:
        return list(existing or [])
    remaining_mounts = []
    for item in existing or []:
        if not isinstance(item, dict) or item.get("type") != "memory":
            remaining_mounts.append(item)
            continue
        ids = [
            part.strip()
            for part in str(item.get("ids") or "").split(",")
            if part.strip()
        ]
        kept = [mem_id for mem_id in ids if mem_id not in targets]
        if not kept:
            continue
        updated = dict(item)
        updated["ids"] = ", ".join(kept)
        remaining_mounts.append(updated)
    return remaining_mounts


def append_reaction_loop_handoff_to_messages(messages, reaction_loop, targets=None):
    """旧 reaction_loop 中继不再写入 model-visible messages。"""
    return
