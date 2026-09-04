"""Readonly task board projection for high-frequency context."""

import json
import os

from logic.evidence_refs import (
    canonical_command_ref,
    evidence_handle_for_result,
    result_supports_evidence,
    shell_result_subcommands,
)
from logic.task_progress_copy import (
    TASK_ORIGINAL_GOAL_NON_SHRINK_REMINDER,
    task_board_instruction_lines,
)


ITEM_DONE_STATUSES = {"accepted", "applied", "complete", "completed", "done", "finish", "finished"}
ACCEPTANCE_DONE_STATUSES = ITEM_DONE_STATUSES | {"passed", "verified"}
BLOCKED_STATUSES = {"blocked"}
PENDING_INPUT_SETTLED_STATUSES = {
    "integrated", "deferred", "rejected", "split", "completed", "done"
}


def render_active_task_board(workbench_store, recent_context_entries=None):
    """Render the active task guide as a readonly task board."""
    task_id = _active_task_id(workbench_store)
    if not task_id:
        return ""
    guide = workbench_store.load_task_guide(task_id)
    if not isinstance(guide, dict):
        return ""
    title = _text(guide.get("task_title") or guide.get("title") or task_id)
    goal = _text(guide.get("task_goal") or guide.get("goal"))
    items = [item for item in guide.get("items") or [] if isinstance(item, dict)]
    acceptance = [
        item for item in guide.get("acceptance") or [] if isinstance(item, dict)
    ]
    pending_inputs = [
        item for item in guide.get("pending_inputs") or [] if isinstance(item, dict)
    ]
    evidence = _recent_evidence_index(recent_context_entries)

    lines = [
        "## 当前任务清单状态",
        *task_board_instruction_lines(
            task_id,
            item_ids=[item.get("item_id") for item in items],
            acceptance_ids=[
                item.get("acceptance_id") for item in acceptance
            ],
        ),
        f"- 任务ID：{task_id}",
    ]
    if title:
        lines.append(f"- 任务标题：{title}")
    if goal:
        lines.append(f"- 任务目标：{goal}")

    if pending_inputs:
        lines.append("")
        lines.append("### 待整合输入")
        for pending in pending_inputs:
            pending_id = _text(pending.get("pending_input_id") or "pending_input")
            status = _text(pending.get("status") or "pending")
            summary = _text(pending.get("summary"))
            lines.append(f"- {pending_id}：{status}；{summary or '无摘要'}")

    if evidence["artifacts"] or evidence["other"]:
        lines.append("")
        lines.append("### 最近已落地证据")
        for record in (evidence["artifacts"] + evidence["other"])[:8]:
            lines.append(f"- {record['label']}：{record['display']}；证据：{record['refs_text']}")

    if items:
        lines.append("")
        lines.append("### 任务项（按顺序）")
        for item in items:
            item_id = _text(item.get("item_id") or "item")
            title_text = _text(item.get("title") or item.get("description"))
            required = "必需" if item.get("required") is not False else "可选"
            projection = _record_projection(
                item,
                title_text,
                evidence,
                done_label="已完成",
                pending_label="待办",
                artifact_label="已产出，待登记",
            )
            lines.append(
                f"- {item_id}：{projection['status']}；{required}；{title_text or '无标题'}；{projection['detail']}"
            )

    if acceptance:
        lines.append("")
        lines.append("### 验收项（按顺序）")
        for item in acceptance:
            acc_id = _text(item.get("acceptance_id") or "acceptance")
            description = _text(
                item.get("description") or item.get("title") or item.get("target")
            )
            required = "必需" if item.get("required") is not False else "可选"
            projection = _record_projection(
                item,
                description,
                evidence,
                done_label="已通过",
                pending_label="待验收",
                artifact_label="有证据，待登记",
                acceptance=True,
            )
            lines.append(
                f"- {acc_id}：{projection['status']}；{required}；{description or '无描述'}；{projection['detail']}"
            )

    gaps = _task_board_gaps(items, acceptance, pending_inputs, evidence)
    if gaps:
        lines.append("")
        lines.append("### 下一批明显缺口")
        lines.extend(f"- {gap}" for gap in gaps[:12])
    else:
        lines.append("")
        lines.append("### 下一批明显缺口")
        lines.append("- 未发现 open/pending 的必需项；若证据已全部通过，可按 99_popup 指南收束。")

    return "\n".join(lines).strip()


def render_task_execution_action_guide(guide, workbench_store):
    guide = guide if isinstance(guide, dict) else {}
    guide_id = _text(guide.get("guide_id"))
    task_id = _text(guide.get("task_id"))
    try:
        task_guide = workbench_store.load_task_guide(task_id) if task_id else {}
    except Exception:
        task_guide = {}
    items = [item for item in task_guide.get("items") or [] if isinstance(item, dict)]
    acceptance = [
        item for item in task_guide.get("acceptance") or [] if isinstance(item, dict)
    ]
    pending_inputs = _open_pending_inputs([
        item for item in task_guide.get("pending_inputs") or [] if isinstance(item, dict)
    ])
    open_items = [
        item for item in items
        if _status(item, "open") not in ITEM_DONE_STATUSES | BLOCKED_STATUSES
        and item.get("required") is not False
    ]
    pending_acceptance = [
        item for item in acceptance
        if _status(item, "pending") not in ACCEPTANCE_DONE_STATUSES | BLOCKED_STATUSES
        and item.get("required") is not False
    ]
    blocked_records = [
        item for item in items + acceptance
        if _status(item, "") in BLOCKED_STATUSES
    ]

    lines = [
        f"任务执行指南｜行动卡：{guide_id}",
        "看板在 40_high_freq；本卡只管下一步行动。",
        "真实工作优先；证据后登记：缺产物用 file_write/file_edit，缺隔离执行或验证用 subagent_dispatch，缺来源正文用 file_read/file_grep/web_fetch/web_search。",
        "工具结果若真正改变来源、任务拆分、验收或风险，可用 guide_submit 的 task_progress/revise_task_plan 修订计划；提交需要替换的完整目标片段和外层 reason。不要每次调用后机械修订，也不要用它登记完成状态。",
        TASK_ORIGINAL_GOAL_NON_SHRINK_REMINDER,
        "工具调用走 native 通道；正文只写简短进展，不承载 DSML/JSON/完整参数。",
    ]
    if pending_inputs:
        pending_id_values = [
            _text(item.get("pending_input_id") or "pending_input")
            for item in pending_inputs
        ]
        pending_ids = ", ".join(pending_id_values)
        pending_example = json.dumps({
            "pending_inputs": [{
                "pending_input_id": pending_id_values[0],
                "status": "integrated",
                "summary": "已整合该输入",
            }]
        }, ensure_ascii=False, separators=(",", ":"))
        lines.extend([
            "当前有待整合输入：先处理用户追加内容，再继续普通进度更新。",
            f"当前待整合ID：{pending_ids}",
            "待整合 ID 也是不透明标识，必须逐字复制，不得改变连字符或下划线。",
            "填写形态：fields=" + pending_example,
            "整合入口：guide_submit",
            f"- guide_id={guide_id}",
            "- item_id=task_progress",
            "- option_id=integrate_pending_input",
        ])
    elif open_items or pending_acceptance:
        lines.extend([
            "当前看板仍有 open/pending 项；不要把更新账本当第一动作。",
            "先补真实产物/验证/来源；收束时若被任务验收 checkpoint 拦截，再按坐标批量更新账本。",
        ])
    if blocked_records:
        lines.extend([
            "存在已阻塞记录：若剩余事项确实不可继续，保留 blocked 与 reason；账本闭合后直接自然回复用户，不要反复空喊完成。",
            "验收项优先使用 passed 或 blocked；blocked 必须保留 reason。",
        ])
    lines.append(
        "任务验收 checkpoint 出现前，不需要反复操作账本；先让文件、命令、来源和工具事实真实落地。"
    )
    return "\n".join(lines).strip()


def _active_task_id(workbench_store):
    slots = workbench_store.active_guide_slots()
    guide_id = _text((slots or {}).get("work"))
    if guide_id.startswith("task:"):
        return guide_id.split(":", 1)[1]
    if guide_id:
        guide = workbench_store.load_guide(guide_id)
        task_id = _text((guide or {}).get("task_id"))
        if task_id:
            return task_id
    active = _text(workbench_store.get("base.active_task"))
    if active:
        return active
    return ""


def _task_board_gaps(items, acceptance, pending_inputs, evidence=None):
    evidence = evidence or _empty_evidence_index()
    gaps = []
    for pending in pending_inputs:
        if _status(pending, "pending") in {"pending", "open"}:
            pending_id = _text(pending.get("pending_input_id") or "pending_input")
            summary = _text(pending.get("summary"))
            gaps.append(f"待整合输入 {pending_id}：{summary or '无摘要'}")
    for item in items:
        if item.get("required") is False:
            continue
        status = _status(item, "open")
        if status in ITEM_DONE_STATUSES | BLOCKED_STATUSES:
            continue
        item_id = _text(item.get("item_id") or "item")
        title = _text(item.get("title") or item.get("description"))
        matches = _matching_records(title, evidence)
        if matches:
            gaps.append(
                f"任务项 {item_id} 已有产物但账本未登记：{_records_summary(matches)}；后续 checkpoint 登记，当前不要重复写。"
            )
        else:
            gaps.append(f"任务项 {item_id} 待办：{title or '无标题'}")
    for item in acceptance:
        if item.get("required") is False:
            continue
        status = _status(item, "pending")
        if status in ACCEPTANCE_DONE_STATUSES | BLOCKED_STATUSES:
            continue
        acc_id = _text(item.get("acceptance_id") or "acceptance")
        description = _text(item.get("description") or item.get("title") or item.get("target"))
        matches = _matching_records(description, evidence)
        if matches:
            gaps.append(
                f"验收项 {acc_id} 已有证据但账本未登记：{_records_summary(matches)}；后续 checkpoint 登记。"
            )
        else:
            gaps.append(f"验收项 {acc_id} 待验收：{description or '无描述'}")
    return gaps


def _open_pending_inputs(pending_inputs):
    return [
        item for item in pending_inputs or []
        if isinstance(item, dict)
        and _status(item, "pending") not in PENDING_INPUT_SETTLED_STATUSES
    ]


def _evidence_summary(raw):
    refs = raw if isinstance(raw, list) else [raw] if raw else []
    refs = [_text(ref) for ref in refs if _text(ref)]
    if not refs:
        return "无"
    if len(refs) <= 3:
        return "、".join(refs)
    return "、".join(refs[:3]) + f" 等 {len(refs)} 条"


def _record_projection(
        record,
        text,
        evidence,
        *,
        done_label,
        pending_label,
        artifact_label,
        acceptance=False):
    status = _status(record, "pending" if acceptance else "open")
    if status in (ACCEPTANCE_DONE_STATUSES if acceptance else ITEM_DONE_STATUSES):
        refs = _evidence_summary(record.get("evidence_refs"))
        return {"status": done_label, "detail": f"证据：{refs}"}
    if status in BLOCKED_STATUSES:
        reason = _text(record.get("reason"))
        detail = f"原因：{reason}" if reason else "原因：未记录"
        return {"status": "已阻塞", "detail": detail}
    own_refs = _evidence_summary(record.get("evidence_refs"))
    if own_refs != "无":
        return {"status": "有证据，待登记", "detail": f"证据：{own_refs}"}
    matches = _matching_records(text, evidence)
    if matches:
        return {"status": artifact_label, "detail": f"已有：{_records_summary(matches)}"}
    return {"status": pending_label, "detail": "已有：无"}


def _recent_evidence_index(recent_context_entries):
    records = []
    seen = set()
    for result in _recent_tool_results(recent_context_entries):
        if not result_supports_evidence(result):
            continue
        record = _record_from_tool_result(result)
        if not record:
            continue
        key = (record["kind"], record["target"])
        if key in seen:
            continue
        seen.add(key)
        records.append(record)
    artifacts = [record for record in records if record["kind"] == "artifact"]
    other = [record for record in records if record["kind"] != "artifact"]
    return {"artifacts": artifacts, "other": other}


def _empty_evidence_index():
    return {"artifacts": [], "other": []}


def _recent_tool_results(recent_context_entries):
    results = []
    for entry in recent_context_entries or []:
        if not isinstance(entry, dict):
            continue
        tool_result = entry.get("tool_result")
        if isinstance(tool_result, dict):
            results.append(tool_result)
        elif isinstance(entry.get("tool_id"), str):
            results.append(entry)
        protocol_receipt = entry.get("protocol_receipt")
        if isinstance(protocol_receipt, dict):
            results.append(protocol_receipt)
        for receipt in entry.get("protocol_receipts") or []:
            if isinstance(receipt, dict):
                results.append(receipt)
    return results


def _record_from_tool_result(result):
    tool_id = _text((result or {}).get("tool_id"))
    refs = _tool_result_refs(result)
    if tool_id in {"file_write", "file_edit"}:
        path = _text(
            result.get("path") or result.get("target_path") or result.get("file_path")
        )
        if not path:
            return {}
        return {
            "kind": "artifact",
            "label": "产物",
            "target": path,
            "display": _display_target(path),
            "basename": os.path.basename(path),
            "aliases": _record_aliases("artifact", path, os.path.basename(path)),
            "refs": refs,
            "refs_text": _compact_evidence_summary(refs),
        }
    if tool_id == "shell_command":
        command = canonical_command_ref(result.get("command"))
        if not command:
            return {}
        return {
            "kind": "command",
            "label": "命令",
            "target": command,
            "display": command,
            "basename": "",
            "aliases": _record_aliases(
                "command",
                command,
                *[
                    canonical_command_ref(item)
                    for item in shell_result_subcommands(result)
                ],
            ),
            "refs": refs,
            "refs_text": _compact_evidence_summary(refs),
        }
    if tool_id == "web_fetch":
        url = _text(result.get("source_url") or result.get("url"))
        if not url:
            return {}
        return {
            "kind": "source",
            "label": "网页正文",
            "target": url,
            "display": _display_record_target("source", url),
            "basename": "",
            "aliases": _record_aliases("source", url),
            "refs": refs,
            "refs_text": _compact_evidence_summary(refs),
        }
    return {}


def _tool_result_refs(result):
    refs = []
    handle = evidence_handle_for_result(result)
    if handle:
        refs.append(handle)
    refs.extend(_normalize_refs(result.get("evidence_refs")))
    tool_id = _text(result.get("tool_id"))
    path = _text(result.get("path") or result.get("target_path") or result.get("file_path"))
    if path:
        refs.extend([path, f"{tool_id}:{path}" if tool_id else path])
    if tool_id == "shell_command":
        command = canonical_command_ref(result.get("command"))
        if command:
            refs.append(f"command:{command}")
        for subcommand in shell_result_subcommands(result):
            canonical = canonical_command_ref(subcommand)
            if canonical:
                refs.append(f"command:{canonical}")
    return _normalize_refs(refs)


def _normalize_refs(value):
    raw = value if isinstance(value, list) else [value] if value else []
    refs = []
    for item in raw:
        text = _text(item)
        if text and text not in refs:
            refs.append(text)
    return refs


def _matching_records(text, evidence):
    haystack = _match_text(text)
    if not haystack:
        return []
    matches = []
    records = []
    for key in ("artifacts", "other"):
        records.extend((evidence or {}).get(key) or [])
    for record in records:
        for alias in record.get("aliases") or [record.get("basename"), record.get("target")]:
            needle = _match_text(alias)
            if needle and needle in haystack:
                matches.append(record)
                break
    return matches


def _record_aliases(kind, target, *extra):
    aliases = []
    for item in (target, *extra):
        text = _text(item)
        if text and text not in aliases:
            aliases.append(text)
        normalized = text.replace("\\", "/") if text else ""
        if normalized and normalized not in aliases:
            aliases.append(normalized)
    if kind == "source":
        normalized = _text(target).replace("\\", "/")
        without_scheme = normalized.split("://", 1)[-1] if "://" in normalized else normalized
        if without_scheme and without_scheme not in aliases:
            aliases.append(without_scheme)
    return aliases


def _match_text(value):
    return _text(value).replace("\\", "/").lower()


def _records_summary(records):
    labels = []
    for record in records or []:
        label = _text(record.get("basename")) or _text(record.get("target"))
        refs = _compact_evidence_summary(record.get("refs"))
        text = f"{label}（证据：{refs}）"
        if text not in labels:
            labels.append(text)
    if not labels:
        return "无"
    if len(labels) <= 3:
        return "、".join(labels)
    return "、".join(labels[:3]) + f" 等 {len(labels)} 条"


def _compact_evidence_summary(raw):
    refs = _normalize_refs(raw)
    if not refs:
        return "无"
    compact = []
    for ref in refs:
        item = _compact_ref(ref)
        if item and item not in compact:
            compact.append(item)
        if len(compact) >= 3:
            break
    if len(refs) <= 3:
        return "、".join(compact)
    return "、".join(compact) + f" 等 {len(refs)} 条"


def _compact_ref(ref):
    text = _text(ref)
    if not text:
        return ""
    if text.startswith("EV-"):
        return text
    if ":" in text:
        prefix, value = text.split(":", 1)
        prefix = prefix.strip()
        value = value.strip()
        if prefix in {"file", "file_edit", "file_read", "file_write"}:
            return f"{prefix}:{_display_target(value)}"
        if prefix == "shell_command":
            normalized = value.replace("\\", "/")
            return f"{prefix}:{normalized}"
        if prefix == "web_fetch":
            return f"{prefix}:{_display_record_target('source', value)}"
    return _display_target(text)


def _display_record_target(kind, value):
    text = _text(value).replace("\\", "/")
    if not text:
        return ""
    if kind == "source":
        return text
    if kind == "command":
        return text
    return _display_target(text)


def _display_target(value):
    text = _text(value)
    if not text:
        return ""
    normalized = text.replace("\\", "/")
    tail = normalized.rsplit("/", 1)[-1].strip()
    return tail or text


def _status(item, default):
    return _text((item or {}).get("status") or default).lower()


def _text(value):
    return str(value or "").strip()
