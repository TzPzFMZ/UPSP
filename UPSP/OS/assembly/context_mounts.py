"""ContextAssembler 的 CONTENT 挂载读取 helper。

本模块只负责读取被挂载条目的正文片段。
这里不直接改写 persona 真源，也不改变挂载策略或上下文层顺序。
"""
import os

from assembly.context_helpers import (
    format_round_id,
    hide_empty_memory_annotation,
    join_layer_blocks,
)
from data.relation_store import relation_public_name

def _mount_marker(current_round=None, meta=None):
    parts = []
    visible = format_round_id(current_round)
    if visible:
        parts.append(f"当前可见轮次：{visible}")
    labels = {
        "source_round": "来源轮次",
        "created_round": "创建轮次",
        "last_recalled_round": "最近召回轮次",
    }
    for key, label in labels.items():
        value = format_round_id((meta or {}).get(key))
        if value:
            parts.append(f"{label}：{value}")
    if not parts:
        return ""
    return "<!-- " + "; ".join(parts) + " -->"


def _range_label(value):
    if not isinstance(value, dict):
        return ""
    range_type = value.get("type")
    if range_type == "line":
        return "line:{}-{}".format(
            value.get("line_start", ""),
            value.get("line_end", ""),
        )
    if range_type == "char":
        return "char:{}-{}".format(
            value.get("char_start", ""),
            value.get("char_end", ""),
        )
    return str(value)


def _mount_header(req):
    lines = []
    source = str(req.get("source") or "").strip()
    read_mode = str(req.get("read_mode") or "full").strip() or "full"
    if source:
        lines.append(f"来源工具：{source}")
    lines.append(f"读取模式：{read_mode}")
    if req.get("total_lines") not in (None, ""):
        lines.append(f"总行数：{req.get('total_lines')}")
    if req.get("total_chars") not in (None, ""):
        lines.append(f"总字符数：{req.get('total_chars')}")
    requested = _range_label(req.get("range_requested"))
    applied = _range_label(req.get("range_applied"))
    if requested:
        lines.append(f"请求范围：{requested}")
    if applied:
        lines.append(f"实际范围：{applied}")
    return "\n".join(lines)


def _has_read_payload_metadata(req):
    return any(
        req.get(key) not in (None, "")
        for key in (
            "source",
            "read_mode",
            "range_requested",
            "range_applied",
            "total_lines",
            "total_chars",
        )
    )


def _mount_payload(req):
    if not isinstance(req, dict):
        return ""
    if "content" not in req:
        return ""
    content = req.get("content")
    if content is None:
        return ""
    if content == "" and not _has_read_payload_metadata(req):
        return ""
    header = _mount_header(req)
    return "\n".join(item for item in (header, str(content)) if item)


def build_mounted_content(assembler, mount_ids, current_round=None):
    return join_layer_blocks(
        build_mounted_content_blocks(assembler, mount_ids, current_round),
    )[0]


def build_mounted_content_blocks(assembler, mount_ids, current_round=None):
    blocks = []
    for req in mount_ids or []:
        req_type = req.get("type", "")
        ids = req.get("ids", "")
        content = ""
        title = ""
        if req_type == "memory":
            from data.memory_store import project_memory_body
            meta = assembler._memory_mount_meta(ids)
            snapshot = _mount_payload(req)
            content = (
                project_memory_body(snapshot, meta)
                if snapshot else assembler._load_memory_content(ids)
            )
            if content:
                marker = _mount_marker(current_round, meta)
                title = f"记忆 {ids}"
                content = "\n".join(
                    item for item in (f"### {title}", marker, content) if item)
        elif req_type == "container":
            container_content = _mount_payload(req) or assembler._load_container_content(ids)
            if container_content:
                target = str(req.get("target_file") or "").strip()
                title = f"容器 {ids}" + (f" / {target}" if target else "")
                marker = _mount_marker(current_round, {})
                content = "\n".join(
                    item for item in (f"### {title}", marker, container_content)
                    if item)
        elif req_type == "relation":
            rel_content = _mount_payload(req) or assembler._load_relation_content(ids)
            if rel_content:
                marker = _mount_marker(current_round, {})
                visible_title = relation_public_name(
                    req.get("title")
                    or req.get("subject")
                    or ids
                )
                title = f"关系卡 {visible_title}"
                content = "\n".join(
                    item for item in (f"### {title}", marker, rel_content)
                    if item)
        elif req_type == "skill":
            skill_content = assembler._load_skill_content(ids)
            if skill_content:
                marker = _mount_marker(current_round, {})
                title = f"技能 {ids}"
                content = "\n".join(
                    item for item in (f"### {title}", marker, skill_content)
                    if item)
        if not content:
            continue
        if not blocks:
            content = "## CONTENT（已挂载正文）\n" + content
        block = {
            "block_id": f"mount:{req_type}:{ids}:{len(blocks) + 1}",
            "title": title,
            "kind": f"{req_type}_mount",
            "source_block_id": str(ids or ""),
            "content": content,
        }
        if blocks:
            block["separator_before"] = "\n"
        blocks.append(block)
    if not blocks:
        blocks.append({
            "block_id": "mount:empty",
            "title": "已挂载正文",
            "kind": "mounted_content_empty",
            "content": "## CONTENT（已挂载正文）\n（无内容被挂载）",
        })
    return blocks


def load_relation_content(relation_id):
    """加载关系卡内容。"""
    from data.relation_store import RelationStore
    rs = RelationStore()
    try:
        card = rs.read_card(relation_id)
        if card:
            name = card.get("name", relation_id)
            notes = card.get("notes", [])
            # notes 是 [{date, content}] dict 列表，取 content 字段
            note_texts = [n.get("content", str(n)) if isinstance(n, dict) else str(n)
                         for n in notes]
            summary = "\n".join(note_texts) if note_texts else str(card.get("summary", ""))
            if not summary:
                try:
                    path = rs.get_card_path(relation_id, card.get("category", "ours"))
                    if os.path.isfile(path):
                        with open(path, "r", encoding="utf-8") as f:
                            summary = f.read()
                except Exception:
                    summary = ""
            return f"{name}\n{summary}"
    except Exception:
        pass
    return ""


def load_skill_content(skill_id):
    """加载技能卡片内容。"""
    from data.container_store import ContainerStore
    cs = ContainerStore()
    try:
        return cs.read_entries(skill_id, file_name="card.md")
    except Exception:
        return ""


def load_memory_content(assembler, mem_ids_str):
    from data.memory_store import MemoryStore, project_memory_body
    ms = MemoryStore()
    ids = [s.strip() for s in mem_ids_str.split(",")]
    parts = []
    for mem_id in ids:
        try:
            try:
                meta = ms.read_meta_by_id(mem_id)
            except Exception:
                meta = ms.get_meta(mem_id)
            if not assembler._memory_meta_visible(meta):
                continue
            parts.append(project_memory_body(
                hide_empty_memory_annotation(ms.read_entry(mem_id)),
                meta,
            ))
        except Exception:
            pass
    return "\n\n".join(parts)


def memory_mount_meta(mem_ids_str):
    from data.memory_store import MemoryStore
    ms = MemoryStore()
    ids = [s.strip() for s in str(mem_ids_str or "").split(",") if s.strip()]
    merged = {}
    for mem_id in ids:
        try:
            meta = ms.read_meta_by_id(mem_id)
        except Exception:
            continue
        for key in (
            "source_round", "created_round", "created_at",
            "last_recalled_round", "last_recalled_at",
            "current_overview", "current_overview_updated_at",
            "linked_containers",
        ):
            if key in meta and key not in merged:
                merged[key] = meta.get(key)
    return merged


def load_container_content(container_id):
    from data.container_store import ContainerStore
    cs = ContainerStore()
    try:
        prefix = container_id.split("-")[0] if "-" in container_id else container_id[:3]
        primary_files = {"DC": "open.md", "EC": "open.md",
                        "PRJ": "plan.md", "SKL": "card.md",
                        "FUT": "objectives.md"}
        fname = primary_files.get(prefix, "open.md")
        return cs.read_entries(container_id, file_name=fname)
    except Exception:
        return "（容器为空）"

# ==============================================================
# 轴描述（高频层，数值隔离）
# ==============================================================
