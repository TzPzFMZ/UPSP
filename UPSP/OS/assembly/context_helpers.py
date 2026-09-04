"""ContextAssembler 的纯投影 helper。

本模块只处理 now/lately 条目归一、临时输入 marker、POPUP 文本片段和索引折叠等
可复用渲染细节；不决定五模块装配顺序、不读写 persona 真源，也不改变缓存层语义。
"""
import hashlib
import json
import re

from assembly.popup import PopupManager
from constants import corpus_entry_timestamp
from logic.closeout_copy import closeout_final_reply_reminder


ACTIVE_CORPUS_ID_RE = re.compile(r"^C-[0-9]{5}$")


def format_round_id(value):
    try:
        return f"R{int(value):06d}"
    except (TypeError, ValueError):
        text = str(value or "").strip()
        return text if text else ""


def compact_display_text(text):
    """只清洗投喂/展示层，原始缓存和审计不在这里改。"""
    lines = str(text or "").replace("\r\n", "\n").replace("\r", "\n").split("\n")
    compacted = []
    blank_seen = False
    separator_seen = False
    for raw in lines:
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped:
            if not blank_seen:
                compacted.append("")
            blank_seen = True
            continue
        blank_seen = False
        if stripped == "---":
            if separator_seen:
                continue
            separator_seen = True
            compacted.append("---")
            continue
        separator_seen = False
        compacted.append(line)
    return "\n".join(compacted).strip()


def normalize_active_corpus_id(value):
    text = str(value or "").strip().upper()
    return text if ACTIVE_CORPUS_ID_RE.match(text) else ""


def active_corpus_ids_from_messages(messages):
    ids = []
    for message in messages or []:
        if not isinstance(message, dict):
            continue
        content = str(message.get("content") or "")
        if "语料短ID" not in content:
            continue
        for match in re.finditer(r"\bC-[0-9]{5}\b", content):
            corpus_id = match.group(0)
            if corpus_id not in ids:
                ids.append(corpus_id)
    return ids


def _entry_supports_active_corpus_id(entry):
    if not isinstance(entry, dict):
        return False
    if not compact_display_text(entry.get("content", "")):
        return False
    kind = str(entry.get("kind") or "").strip()
    if not kind:
        kind = "assistant_reply" if entry.get("role") == "assistant" else "interaction"
    if kind == "reasoning_context":
        return False
    return kind != "runtime_call_request"


def assign_active_corpus_ids(entries, start_index=1):
    try:
        next_index = int(start_index)
    except (TypeError, ValueError):
        next_index = 1
    next_index = max(1, next_index)
    existing_numbers = [
        int(corpus_id[2:])
        for corpus_id in (
            normalize_active_corpus_id(entry.get("active_corpus_id"))
            for entry in entries or []
            if isinstance(entry, dict)
        )
        if corpus_id
    ]
    next_index = max(next_index, max(existing_numbers, default=0) + 1)
    assigned = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            assigned.append(entry)
            continue
        copied = dict(entry)
        if _entry_supports_active_corpus_id(copied):
            corpus_id = normalize_active_corpus_id(copied.get("active_corpus_id"))
            if not corpus_id:
                corpus_id = f"C-{next_index:05d}"
                next_index += 1
            copied["active_corpus_id"] = corpus_id
        assigned.append(copied)
    return assigned, next_index


def corpus_entry_identity_key(entry):
    if not isinstance(entry, dict):
        return ""
    content = compact_display_text(entry.get("content", ""))
    if not content:
        return ""
    kind = str(entry.get("kind") or "").strip()
    if not kind:
        kind = "assistant_reply" if entry.get("role") == "assistant" else "interaction"
    identity = {
        "kind": kind,
        "role": str(entry.get("role") or "").strip(),
        "round": str(entry.get("round") or entry.get("source_round") or "").strip(),
        "step": str(entry.get("step") or "").strip(),
        "iter": str(entry.get("iter") or "").strip(),
        "sha256": hashlib.sha256(content.encode("utf-8")).hexdigest()[:24],
    }
    return json.dumps(identity, ensure_ascii=False, sort_keys=True)


def _int_or_none(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _should_fold_dialogue_progress(
        rendered,
        *,
        current_round=None,
        current_reaction_iteration=None,
        expand_once_entry_keys=None):
    if str(rendered.get("kind") or "").strip() != "dialogue_progress":
        return False
    entry_key = corpus_entry_identity_key(rendered)
    if entry_key and entry_key in set(expand_once_entry_keys or []):
        return False
    current_iter = _int_or_none(current_reaction_iteration)
    if current_iter is None:
        return False
    source_round = _entry_source_round(rendered, current_round=current_round)
    visible_round = _entry_visible_round(current_round)
    if not _is_current_round_entry(source_round, visible_round):
        return True
    entry_iter = _int_or_none(rendered.get("iter"))
    if entry_iter is None:
        return False
    return entry_iter < current_iter - 1


def messages_text(entries):
    """把缓存 message 条目压成审计文本；只读取 content 字段。"""
    parts = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        content = str(entry.get("content", ""))
        if not content:
            continue
        if str(entry.get("kind") or "").strip() == "runtime_call_request":
            if "【Runtime 调用占位】" not in content[:120]:
                try:
                    rendered = render_corpus_entry_for_context(
                        entry,
                        cache_source="now_cache.jsonl",
                    )
                    content = str(rendered.get("content") or content)
                except Exception:
                    pass
        parts.append(content)
    return "\n".join(parts)


def _first_non_empty(*values):
    for value in values:
        if value is None:
            continue
        text = str(value).strip()
        if text:
            return value
    return ""


def _entry_source_round(entry, current_round=None):
    value = _first_non_empty(
        entry.get("source_round"),
        entry.get("created_round"),
        entry.get("round"),
        current_round,
    )
    return format_round_id(value)


def _entry_visible_round(current_round=None):
    return format_round_id(current_round)

def _round_label(round_id):
    try:
        return str(int(str(round_id or "").strip().lstrip("Rr") or ""))
    except ValueError:
        text = str(round_id or "").strip()
        return text or "未知"


def _is_current_round_entry(source_round, visible_round):
    return bool(source_round and visible_round and source_round == visible_round)


def _identity_status_text(value):
    text = str(value or "").strip()
    mapping = {
        "declared": "已声明",
        "confirmed": "已确认",
        "unknown": "未知",
        "timeout": "超时未确认",
        "unregistered": "未登记",
    }
    return mapping.get(text, text or "未知")


def _interaction_source_text(value):
    text = str(value or "").strip()
    mapping = {
        "self_declaration": "对象自述",
        "current_user_message": "本轮输入",
        "context_continuity": "连续上下文",
        "tool_fact": "工具事实",
        "reaction_finalize": "反应收束",
        "system": "系统",
        "unresolved": "未解析",
    }
    return mapping.get(text, text or "未解析")


def _round_type_text(value):
    text = str(value or "").strip()
    return {
        "interactive": "交互轮",
        "rhythm": "节律轮",
        "relay": "中继轮",
        "autonomous": "自主轮",
        "standby": "待命轮",
    }.get(text, text or "未知轮型")


def _entry_ref_dict(entry):
    ref = entry.get("ref")
    return ref if isinstance(ref, dict) else {}


def _entry_ref_value(entry, key):
    if key in entry:
        return entry.get(key)
    ref = _entry_ref_dict(entry)
    if key in ref:
        return ref.get(key)
    return ""


def _minimum_commitment_round(content, source_round):
    match = re.search(r"R0*([0-9]+)", str(content or ""))
    if match:
        return match.group(1)
    return _round_label(source_round)


def _legacy_tool_field(text, key):
    match = re.search(rf"(?:^|[;\s]){re.escape(key)}=([^;]+)", str(text or ""))
    if not match:
        return ""
    value = match.group(1).strip()
    value = re.split(r"\s+[A-Za-z0-9_]+=", value, maxsplit=1)[0].strip()
    return value.rstrip(".。")


def _naturalize_legacy_tool_summary(line):
    text = str(line or "").strip()
    if not text:
        return ""
    stripped = text[2:].strip() if text.startswith("- ") else text
    looks_legacy = (
        "[file_read ok]" in stripped
        or "tool_id=file_read" in stripped
        or "tool_fact step=" in stripped
    )
    if not looks_legacy:
        return text
    path = _legacy_tool_field(stripped, "path")
    start_line = _legacy_tool_field(stripped, "start_line")
    end_line = _legacy_tool_field(stripped, "end_line")
    next_start_line = _legacy_tool_field(stripped, "next_start_line")
    status = _legacy_tool_field(stripped, "status") or (
        "ok" if "[file_read ok]" in stripped else ""
    )
    lines = []
    if path and status in {"", "ok", "success"}:
        lines.append(f"- 历史上曾经成功读取文件：{path}。")
        if start_line and end_line:
            lines.append(f"  读取范围：第 {start_line} 行到第 {end_line} 行。")
        if next_start_line:
            cursor_line = end_line or "上一段结束"
            lines.append(
                f"  历史读取游标：上次读到第 {cursor_line} 行；"
                f"续读游标第 {next_start_line} 行。"
            )
    else:
        lines.append("- 历史上曾经产生一条工具事实；它不代表本轮已经执行。")
    return "\n".join(lines)


def _legacy_setup_fields(content):
    fields = {}
    for raw_line in str(content or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw_line.strip()
        if not line or line == "[setup_fact]":
            continue
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()
        if key:
            fields[key] = value
    return fields


def _naturalize_setup_fact_body(content):
    text = str(content or "").strip()
    if not text:
        return ""
    if "[setup_fact]" in text or re.search(r"\bsecurity_verdict=", text):
        fields = _legacy_setup_fields(text)
        lines = []
        verdict = fields.get("security_verdict")
        if verdict:
            verdict_text = "通过" if verdict == "pass" else verdict
            lines.append(f"起手安全裁决：{verdict_text}。")
        round_type = fields.get("round_type")
        if round_type:
            lines.append(f"本轮类型：{_round_type_text(round_type)}。")
        round_confirm = fields.get("round_type_confirm")
        if round_confirm:
            lines.append(f"起手确认轮型：{_round_type_text(round_confirm)}。")
        standby = fields.get("standby_skip_reaction")
        if standby:
            lines.append(
                "待命跳过反应步：" + ("是。" if standby.lower() == "true" else "否。")
            )
        for key, value in fields.items():
            if key == "mount_request":
                lines.append(f"起手挂载请求：{value}。")
        interaction_object = fields.get("interaction_object")
        identity_status = fields.get("identity_status")
        interaction_source = fields.get("interaction_source")
        if interaction_object or identity_status or interaction_source:
            lines.append(
                "交互对象是"
                f"{interaction_object or '未知'}，"
                f"身份{_identity_status_text(identity_status)}，"
                f"来源为{_interaction_source_text(interaction_source)}。"
            )
        return "\n".join(lines).strip() or text
    if text.startswith("[心跳触发交接]"):
        body = text.removeprefix("[心跳触发交接]").strip()
        body = body.replace("本轮类型=", "本轮类型为 ")
        body = body.replace("触发flag=", "触发 flag 为 ")
        body = body.replace("提示flag=", "提示 flag 为 ")
        return "心跳触发本轮：" + body
    return text


def _cache_summary_body_text(content, compact_reason):
    lines = []
    for raw_line in str(content or "").replace("\r\n", "\n").replace("\r", "\n").split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        if line.startswith("历史工具事实摘要："):
            continue
        if line.startswith("这些记录只说明历史轮曾产生工具事实条"):
            continue
        if line.startswith("当前读写声明必须以本轮"):
            continue
        lines.append(_naturalize_legacy_tool_summary(line))
    return "\n".join(lines).strip()


def _corpus_header_lines(rendered, content, current_round=None):
    kind = str(rendered.get("kind") or "interaction").strip()
    visible_round = _entry_visible_round(current_round)
    source_round = _entry_source_round(rendered, current_round=current_round)
    current = _is_current_round_entry(source_round, visible_round)
    source_label = _round_label(source_round)
    interaction_object = str(rendered.get("interaction_object") or "unknown").strip()
    identity_status = _identity_status_text(rendered.get("identity_status"))
    interaction_source = _interaction_source_text(
        rendered.get("interaction_source") or rendered.get("source")
    )
    target = str(rendered.get("handoff_target") or rendered.get("target") or "").strip()
    compact_reason = str(rendered.get("compact_reason") or "").strip()

    if kind == "interaction":
        title = "【本轮交互】" if current else f"【历史交互，来自第 {source_label} 轮】"
        lines = [
            title,
            f"交互对象是{interaction_object}，身份{identity_status}，来源为{interaction_source}。",
        ]
        if not current:
            lines.append("注意：这是历史交互，不代表本轮已经发生。")
        return lines

    if kind == "assistant_reply":
        if current:
            return ["【最终回复记录】", "这是助手正式最终回复记录，只能作为对话历史参考。"]
        return [
            f"【历史回复，来自第 {source_label} 轮】",
            "这是历史助手回复，只能作为上下文参考。",
            "不能把其中的已读取、已完成或已写入当成本轮事实。",
        ]

    if kind == "dialogue_progress":
        if not current:
            return [
                f"【历史进展记录，来自第 {source_label} 轮】",
                "这是历史轮助手此前播报的过程进展，仅作对话连续性参考；"
                "不代表本轮仍在执行，也不是当前资料、工具事实或最终回复。",
            ]
        return [
            "【轮中进展记录】",
            "这是助手此前播报的过程进展，仅作对话连续性参考；不是当前资料、工具事实或最终回复。",
        ]

    if kind == "reasoning_context":
        if current:
            return ["【本轮推理上下文】"]
        return [f"【历史推理上下文，来自第 {source_label} 轮】"]

    if kind == "runtime_call_request":
        return [
            "【Runtime 调用占位】",
            "这不是用户原始输入，只是每次 provider 调用固定存在的短占位。",
        ]

    if kind == "setup_fact":
        title = "【本轮起手事实】" if current else f"【历史起手事实，来自第 {source_label} 轮】"
        lines = [title]
        if not current:
            lines.append("注意：这是历史起手事实，不代表本轮刚刚起手。")
        return lines

    if kind == "relay_handoff":
        title = (
            "【上轮交接任务】"
            if current
            else f"【历史交接任务，来自第 {source_label} 轮】"
        )
        return [
            title,
            "这不是用户原始输入，而是上一轮收束时交给后续轮次继续处理的任务。",
        ]

    if kind == "material":
        title = "【本轮资料】" if current else f"【历史资料，来自第 {source_label} 轮】"
        path = str(
            _entry_ref_value(rendered, "path")
            or _entry_ref_value(rendered, "title")
            or _entry_ref_value(rendered, "source_url")
            or _entry_ref_value(rendered, "url")
            or ""
        ).strip()
        lines = [title]
        if path:
            lines.append(f"资料：{path}。")
        if current:
            lines.append("状态：本轮挂载，下面正文可作为当前资料依据。")
        else:
            lines.append("注意：这是历史资料，不代表本轮已经重新读取。")
        return lines

    if kind == "tool_fact":
        if current:
            return ["【本轮工具事实】"]
        return [
            f"【历史工具事实，来自第 {source_label} 轮】",
            "这是历史工具事实，不代表本轮已经执行。",
        ]

    if kind == "minimum_commitment":
        return [f"【第 {_minimum_commitment_round(content, source_round)} 轮已闭合】"]

    if kind == "fault_note":
        if current:
            return ["【本轮故障记录】", "注意：这是故障记录，不是任务完成事实。"]
        return [
            f"【历史故障记录，来自第 {source_label} 轮】",
            "注意：这是历史故障记录，不代表本轮发生故障，也不是任务完成事实。",
        ]

    if kind == "interaction_summary":
        source_start = _int_or_none(rendered.get("source_round_start"))
        source_end = _int_or_none(rendered.get("source_round_end"))
        if source_start is not None and source_end is not None:
            source_range = (
                f"第 {source_start} 轮"
                if source_start == source_end
                else f"第 {source_start} 至 {source_end} 轮"
            )
            title = f"【历史交互摘要，来源{source_range}】"
        else:
            title = "【历史交互摘要】"
        return [
            title,
            "这是一次历史用户输入及其后续内容的压缩，不是当前用户输入或当前指令。",
            "需要精确事实时，应读取记忆、容器、原始语料或审计记录核验。",
        ]

    if kind == "cache_summary":
        if compact_reason == "round_retention_settlement":
            return [
                f"【历史工具事实摘要，来自第 {source_label} 轮】",
                "这只说明历史轮发生过工具事件，不证明本轮已经执行。",
            ]
        if compact_reason == "post_lately_trim":
            return [
                "【最近缓存压缩摘要】",
                "这是最近缓存水位触发后的语义压缩，用于保持上下文连续。",
                "需要精确事实时，应查看记忆、容器、审计记录或本轮工具回执。",
            ]
        if compact_reason == "progressive_lately_pressure":
            return [
                "【最近缓存压缩摘要】",
                "这是首次用户交互前的历史背景压缩，不是当前用户输入或当前指令。",
                "需要精确事实时，应读取记忆、容器、原始语料或审计记录核验。",
            ]
        return [
            f"【历史摘要，来自第 {source_label} 轮】",
            "这是历史语料摘要，只能作为背景参考。",
        ]

    return ["【语料块】"]


def render_corpus_entry_for_context(
        entry,
        current_round=None,
        cache_source="",
        current_reaction_iteration=None,
        expand_once_entry_keys=None,
        active_corpus_registry=None):
    """给单条可见语料块加中文头；只改展示副本，不改原始缓存条目。"""
    if not isinstance(entry, dict):
        return entry
    rendered = dict(entry)
    content = compact_display_text(rendered.get("content", ""))
    if not content:
        return rendered
    if not rendered.get("kind"):
        rendered["kind"] = (
            "assistant_reply"
            if rendered.get("role") == "assistant"
            else "interaction"
        )
    if rendered.get("kind") == "assistant_reply":
        source_round = _entry_source_round(rendered, current_round=current_round)
        visible_round = _entry_visible_round(current_round)
        if not _is_current_round_entry(source_round, visible_round):
            rendered["role"] = "system"
    if rendered.get("kind") == "setup_fact":
        content = _naturalize_setup_fact_body(content)
    header_lines = _corpus_header_lines(rendered, content, current_round)
    timestamp = corpus_entry_timestamp(rendered)
    if timestamp:
        header_lines = list(header_lines)
        header_lines.insert(1, f"语料时间：{timestamp}。")
    if (
        rendered.get("kind") == "dialogue_progress"
        and current_reaction_iteration is None
    ):
        content = _downrank_dialogue_progress_commitment(content)
    if rendered.get("kind") == "reasoning_context":
        content = re.sub(
            r"^【(?:本轮|历史)推理上下文[^】]*】\s*",
            "",
            content,
            count=1,
        ).strip()
    active_corpus_id = normalize_active_corpus_id(rendered.get("active_corpus_id"))
    if active_corpus_id:
        rendered["active_corpus_id"] = active_corpus_id
        header_lines = list(header_lines)
        header_lines.insert(1, f"语料短ID：{active_corpus_id}。")
        if isinstance(active_corpus_registry, dict):
            active_corpus_registry[active_corpus_id] = {
                "corpus_id": active_corpus_id,
                "kind": str(rendered.get("kind") or "").strip(),
                "entry_key": corpus_entry_identity_key(rendered),
                "cache_source": cache_source,
                "round": rendered.get("round") or rendered.get("source_round"),
                "step": rendered.get("step"),
                "iter": rendered.get("iter"),
                "content_chars": len(content),
            }
    header = "\n".join(header_lines)
    if _should_fold_dialogue_progress(
            rendered,
            current_round=current_round,
            current_reaction_iteration=current_reaction_iteration,
            expand_once_entry_keys=expand_once_entry_keys):
        corpus_hint = ""
        if active_corpus_id:
            corpus_hint = (
                f'\n需要查看正文时，调用 corpus_read(corpus_id="{active_corpus_id}")；'
                "下一次 provider 调用会在此处原位展开一次。"
            )
        rendered["content"] = (
            header
            + "\n\n"
            + f"轮中进展正文已折叠，原文长度 {len(content)} 字。"
            + corpus_hint
        )
        return rendered
    if rendered.get("kind") == "minimum_commitment":
        rendered["content"] = header
        return rendered
    if rendered.get("kind") == "cache_summary":
        body = _cache_summary_body_text(
            content,
            str(rendered.get("compact_reason") or "").strip(),
        )
        rendered["content"] = header if not body else header + "\n\n" + body
        return rendered
    rendered["content"] = header + "\n\n" + content
    return rendered


def _downrank_dialogue_progress_commitment(content):
    text = str(content or "").strip()
    if not text:
        return text
    markers = (
        "马上",
        "准备",
        "将",
        "会先",
        "接下来",
        "开始",
        "先",
        "再",
        "同时",
    )
    action_markers = (
        "guide_submit",
        "更新状态",
        "更新账本",
        "写入产物",
        "写文件",
        "运行命令",
        "file_write",
        "shell_command",
    )
    if not any(marker in text for marker in markers):
        return text
    if not any(marker in text for marker in action_markers):
        return text
    return (
        "计划性进展承诺已降噪：上一反应迭代表达过继续执行的意图，"
        "但这段自然语言不证明文件、命令或清单已经完成；"
        "后续判断必须以真实工具事实、文件写入、命令结果或 guide 完成事实为准。"
    )


def render_corpus_entries_for_context(
        entries,
        current_round=None,
        cache_source="",
        active_corpus_start=1,
        current_reaction_iteration=None,
        expand_once_entry_keys=None,
        active_corpus_registry=None):
    assigned_entries, _ = assign_active_corpus_ids(
        entries,
        start_index=active_corpus_start,
    )
    return [
        render_corpus_entry_for_context(
            entry,
            current_round=current_round,
            cache_source=cache_source,
            current_reaction_iteration=current_reaction_iteration,
            expand_once_entry_keys=expand_once_entry_keys,
            active_corpus_registry=active_corpus_registry,
        )
        for entry in assigned_entries or []
    ]


def normalize_layer_entries(entries, default_role="user"):
    """将 now 层输入归一为 message 条目，保留调用方给出的元数据。"""
    if not entries:
        return []
    normalized = []
    for entry in entries:
        if entry is None:
            continue
        if isinstance(entry, str):
            content = entry.strip()
            if content:
                normalized.append({"role": default_role, "content": content})
            continue
        if isinstance(entry, dict):
            content = str(entry.get("content", "")).strip()
            if not content:
                continue
            item = dict(entry)
            item.setdefault("role", default_role)
            item["content"] = content
            normalized.append(item)
    return normalized


def dedupe_layer_entries(entries):
    """按 now 层稳定键去重，避免同一交接/资料在装配内重复出现。"""
    deduped = []
    seen = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        key = (
            entry.get("round"),
            entry.get("role"),
            entry.get("kind"),
            entry.get("content"),
            entry.get("step"),
            entry.get("iter"),
        )
        if key in seen:
            continue
        seen.add(key)
        deduped.append(entry)
    return deduped


def format_step_guide_popup(
        kind, step, fields, message, source, guide_marker, guide,
        phase=None):
    """统一渲染三步车道 POPUP；结构信息留给审计，不进入模型可见正文。"""
    title = {
        ("setup", None): "起手步指南",
        ("setup", "standby"): "待命起手指南",
        ("reaction", "loop"): "反应循环指南",
        ("reaction", "closeout"): "反应循环指南",
        ("reaction", "final_reply"): "最终回复指南",
        ("cleanup", None): "善后步指南",
    }.get((step, phase), {
        "setup": "起手步指南",
        "reaction": "反应循环指南",
        "cleanup": "善后步指南",
    }.get(step, "步骤指南"))
    lines = [f"【{title}】", message]
    return compact_display_text("\n".join(lines))


def fold_marker(scope, total, offset, limit, zone=None):
    """渲染索引折叠提示；请求文本仍保持既有 index_view 口径。"""
    next_offset = int(offset or 0) + int(limit or 0)
    folded = max(0, int(total or 0) - next_offset)
    if folded <= 0:
        return ""
    zone_part = f" zone={zone};" if zone else ""
    request = (
        f"provider-native index_view(scope={scope};"
        f"{zone_part} offset={next_offset}; limit={int(limit or 0)})"
    )
    return f"（另有 {folded} 条已折叠；可调用：{request}）"


def slice_entries(entries, offset, limit):
    """按 index_view offset/limit 切片，limit 最小为 1。"""
    start = max(0, int(offset or 0))
    size = max(1, int(limit or 1))
    return list(entries or [])[start:start + size]


MEMORY_SETTLEMENT_REMINDER_MESSAGE = """本轮若出现会影响以后判断、行动、协作、关系或自我理解的真实非噪音主体更新，请主动考虑 `memory_write`，不要等待用户要求。
资料正文由 material/最近缓存承载；`dialogue_progress` 只用于用户可见进展，不是私有笔记或记忆替代。
只沉淀稳定变化和可复用判断，不抄资料、不写工具流水；轻量变化可使用 `weight=1/2`。
若没有主体更新，或用户/任务禁止长期记忆，则不写。只有 `MEM-*` 回执才算写入成功。"""


def build_static_memory_reminder_popup(step, current_reaction_iteration=None):
    """Render the fixed Spec576 memory reminder in the POPUP reminder tier."""
    if step != "reaction":
        return ""
    return (
        "- kind: memory_settlement_reminder\n"
        "  tier: reminder\n"
        "  decision_required: false\n"
        "  source: docs/protocol/base/popup.md\n"
        "  message: |\n"
        + "\n".join(
            f"    {line}" if line else ""
            for line in MEMORY_SETTLEMENT_REMINDER_MESSAGE.splitlines()
        )
    )


def build_reaction_step_guide_popup(step):
    """渲染 reaction 常驻主指南；中继正文只归属 relay_intents。"""
    if step != "reaction":
        return ""
    template = PopupManager.load_template("reaction_step_guide") or {}
    if template:
        decision_required = str(
            template.get("decision_required", "false")
        ).strip().lower() == "true"
        message = template.get("message") or "reaction step guide"
        return (
            "- kind: reaction_step_guide\n"
            f"  tier: {template.get('tier', 'guide')}\n"
            f"  decision_required: {'true' if decision_required else 'false'}\n"
            "  source: docs/protocol/base/popup.md\n"
            "  message: |\n"
            + "\n".join(f"    {line}" if line else "" for line in message.splitlines())
        )
    guide = f"""# 反应步：推理、工具调用、生成回复

你是本轮的核心动作。不是一步就完，是 0 到 N 次迭代循环后闭合。

## 迭代流程
接收上下文 -> 选择车道 -> 执行工具或输出 assistant_text 进展 -> 继续下一迭代 -> 自然语言最终回复或 reaction_finalize 中继。

每次迭代可以读文件、写记忆、操作容器、调用协议工具。直到本轮目标达成或需要用户输入时，直接自然语言回复用户；只有需要跨轮继续时才调用 reaction_finalize。

## 核验与询问
- 当用户明确要求“当前/最新”，或易变事实会影响本次结论或行动时，先用可用的搜索或读取工具核验权威来源；稳定事实和纯仓内任务不强制联网。无法核验时明确时效边界，不把旧知识当作当前事实。
- 只有缺失选择会实质改变交付结果或授权边界，且无法从上下文和已读材料核实时，才询问用户；其余轻微歧义采用范围最小、可回退的带界假设，说明后继续。

反应步活路径有三类输出：继续工具行动、assistant_text 轮中可见进展、自然语言最终回复。reaction loop 阶段带工具的自然语言文本是合法轮中进展；无工具调用的自然语言会被 Runtime 当作最终回复候选。它不是工具事实，不自动写长期记忆，也不是任务证据。

## 每轮记忆节奏
记忆与容器沉淀看 POPUP 提醒层的“记忆提醒”；工具字段、权重和回执纪律以 provider-native schema、processor 回执为准。

## 工具三轴
- 只读工具：file_read、file_glob、file_grep、memory_content_read、container_read、relation_read。
- 同步工具：memory_write、memory_link_update、relation_card_write、memory_container_create、memory_container_write。
- 行动工具：file_edit、file_write、shell_command、subagent_dispatch。

## 四容器自觉
- DC 辩证链：理解推进/判断修正，新 MEM 订正旧 MEM。
- EC 事件链：事件经过/打断/恢复，向后看。
- PRJ 项目：多步任务/专项整理，向前看。
- FUT 未来：预测性判断，二段跳（预测 -> 验证）。

## 退出
- 过程性用户可见进展直接输出自然语言；Runtime 会记为 assistant_text，它不是终点。
- 如果只是想继续执行，直接调用合法工具；不要把自然语言说明当作工具结果。
- {closeout_final_reply_reminder()}
- 需要下一轮继续时，单独调用 reaction_finalize(handoff_text)，写清下一轮继续做什么；合法表单由 Runtime 内部置位 continue_requested。
- blocked 不是模型出口；只有 Runtime 蓝屏类事故才派生 blocked。
- 不允许无声明悬挂；完成就自然回复，跨轮继续才 handoff。"""
    return (
        "- kind: reaction_step_guide\n"
        "  tier: guide\n"
        "  decision_required: false\n"
        "  source: rules/protocol/base/reaction.md\n"
        "  message: |\n"
        + "\n".join(f"    {line}" if line else "" for line in guide.splitlines())
    )


def build_current_runtime_guide_popup(step, state=None):
    """按 Runtime 当前状态渲染唯一指南；无特殊指南时回到反应步主指南。"""
    if step != "reaction":
        return ""
    try:
        from logic.rhythm_guidance import render_current_guide_popup
        base_state = (state or {}).get("base", {})
        flags = base_state.get("heartbeat_flags", {})
        runtime = base_state.get("runtime", {})
        completed_flags = runtime.get("guide_completed_flags", [])
        guide = render_current_guide_popup(
            flags,
            completed_flags=completed_flags,
        )
        if guide:
            return guide
    except Exception:
        pass
    return build_reaction_step_guide_popup(step)


def current_round_from_state(state):
    """从 state 中读取当前 round；缺失或异常时返回 None。"""
    try:
        total_round = state.get("base", {}).get("meta", {}).get("total_round")
        return int(total_round)
    except (AttributeError, TypeError, ValueError):
        return None


def build_protocol_tool_guide(tool_id):
    """Render a short active tool hint from provider-native schema metadata."""
    try:
        from logic.native_tool_calls import TOOL_ARGUMENT_SCHEMAS
        from logic.protocol_tools import normalize_tool_id, tool_metadata_for
        normalized = normalize_tool_id(tool_id)
        if normalized == "memory_write":
            return ""
        metadata = tool_metadata_for(normalized)
        schema = TOOL_ARGUMENT_SCHEMAS.get(normalized, {})
    except Exception:
        normalized = str(tool_id or "").strip()
        metadata = {}
        schema = {}
    if not normalized or not metadata:
        return ""
    properties = sorted((schema.get("properties") or {}).keys())
    label = {
        "memory_link_update": "记忆关联历史修复工具",
        "memory_container_create": "容器创建同步工具",
        "memory_container_write": "容器续写同步工具",
        "relation_read": "关系材料读取工具",
        "container_read": "容器材料读取工具",
    }.get(normalized, "provider-native 协议工具")
    lines = [
        f"{label}；直接调用 provider-native `{normalized}`，不请求完整 guide。",
        f"class={metadata.get('tool_class', '')}; risk={metadata.get('risk', '')}。",
    ]
    if properties:
        lines.append("参数：" + ", ".join(properties))
    lines.append("字段纪律以 native schema description 和 processor receipt 为准。")
    return (
        "- kind: protocol_tool_guide\n"
        "  tier: guide\n"
        "  decision_required: false\n"
        f"  tool_id: {normalized}\n"
        "  source: provider-native schema\n"
        "  message: |\n"
        + "\n".join(f"    {line}" if line else "" for line in lines)
    )


def build_general_tool_guide(tool_id):
    """Render a short active general-tool hint from provider-native schema."""
    try:
        from logic.native_tool_calls import TOOL_ARGUMENT_SCHEMAS
        from logic.protocol_tools import normalize_tool_id, tool_metadata_for
        normalized = normalize_tool_id(tool_id)
        metadata = tool_metadata_for(normalized)
        schema = TOOL_ARGUMENT_SCHEMAS.get(normalized, {})
    except Exception:
        normalized = str(tool_id or "").strip()
        metadata = {}
        schema = {}
    if not normalized or not metadata:
        return ""
    properties = sorted((schema.get("properties") or {}).keys())
    label = {
        "file_read": "文件读取通用工具",
        "file_glob": "文件名搜索通用工具",
        "file_grep": "文件正文搜索通用工具",
        "file_edit": "文件编辑通用工具",
        "web_fetch": "网页抓取通用工具",
        "web_search": "网页搜索通用工具",
        "shell_command": "shell 命令通用工具",
        "subagent_dispatch": "子 agent 调度通用工具",
    }.get(normalized, "provider-native 通用工具")
    lines = [
        f"{label}；直接调用 provider-native `{normalized}`，不请求完整 guide。",
        f"class={metadata.get('tool_class', '')}; risk={metadata.get('risk', '')}。",
    ]
    if properties:
        lines.append("参数：" + ", ".join(properties))
    if normalized == "shell_command":
        from logic.shell_backend import shell_model_contract
        contract = shell_model_contract()
        if not contract["available"]:
            return ""
        lines.append(contract["description"])
    if normalized == "file_glob":
        lines.append(
            "只搜索候选路径，不读取正文；默认不递归，只有显式 recursive=true 才搜索子目录。"
        )
    if normalized == "file_grep":
        lines.append(
            "只做字面正文搜索，默认递归且不区分大小写；coverage_complete=false 时"
            "零命中不能证明不存在。"
        )
    lines.append("仍需通过 ExecutionCapabilityGate；失败时按 native_tool_result 修正。")
    return (
        "- kind: general_tool_guide\n"
        "  tier: guide\n"
        "  decision_required: false\n"
        f"  tool_id: {normalized}\n"
        "  source: provider-native schema\n"
        "  message: |\n"
        + "\n".join(f"    {line}" if line else "" for line in lines)
    )


def build_native_tool_feedback_popup(step, native_tool_feedbacks=None):
    """把 Runtime native tool feedback 原样拼为 reaction POPUP 片段。"""
    if step != "reaction":
        return ""
    return "\n\n".join(
        str(item).strip()
        for item in native_tool_feedbacks or []
        if str(item or "").strip()
    )


def join_layer_blocks(blocks, separator="\n\n"):
    """Join model-visible blocks and return compact character-span metadata."""
    chunks = []
    cursor = 0
    index = []
    for block in blocks or []:
        if not isinstance(block, dict):
            continue
        content = str(block.get("content") or "")
        if not content:
            continue
        joiner = "" if not chunks else str(block.get("separator_before", separator))
        chunks.extend((joiner, content))
        start = cursor + len(joiner)
        cursor = start + len(content)
        item = {
            "block_id": str(block.get("block_id") or ""),
            "title": str(block.get("title") or ""),
            "char_start": start,
            "char_end": cursor,
        }
        for key in ("kind", "source_block_id"):
            value = str(block.get(key) or "")
            if value:
                item[key] = value
        index.append(item)
    return "".join(chunks), index
