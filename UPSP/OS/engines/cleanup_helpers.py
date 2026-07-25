"""CleanupPipeline 的善后落账 helper。

本模块集中放置不需要持有 pipeline 状态的解析、渲染和 LTM 文件整理函数。
主善后顺序仍由 CleanupPipeline 决定；这些 helper 只处理单一物理操作或文本投影。
"""
import json
import os
import re


def ltm_has_entry(mem_id):
    """LTM 任意层同编号即视为已归档，STM 遗忘时只删 STM 副本。"""
    from paths import (
        LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON,
        LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON,
        LTM_BACKUP_META_JSON,
    )

    for path in (
        LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON,
        LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON,
        LTM_BACKUP_META_JSON,
    ):
        if not os.path.isfile(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if mem_id in data:
                return True
        except Exception:
            continue
    return False


def extract_memory_field(text, label):
    """从 LTM 记忆正文中取 `**字段**：值`。"""
    pattern = rf"(?m)^\*\*{label}\*\*(?:（[^）]*）)?[:：](.*)$"
    match = re.search(pattern, text or "")
    return match.group(1).strip() if match else ""


def round_text(value):
    """把轮号渲染为中文短标签。"""
    try:
        return f"第{int(value)}轮"
    except Exception:
        return "未知"


def append_ltm_index(index_path, mem_id, entry_type, weight, title, subject, round_num):
    """向 LTM index.md 追加一行；已存在同 mem_id 时保持幂等。"""
    header = (
        f"<!-- {entry_type} 记忆索引 -->\n"
        "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 最后调用轮 | 注释 |\n"
        "|------|------|------|------|---------|-----------|------|\n"
    )
    if os.path.isfile(index_path):
        with open(index_path, "r", encoding="utf-8") as f:
            text = f.read().rstrip()
    else:
        text = header.rstrip()
    if not text.strip():
        text = header.rstrip()
    elif "| 编号 |" not in text and "| ID |" not in text:
        text = text + "\n" + header.rstrip()
    if mem_id in text:
        return
    row = (
        f"| {mem_id} | [{entry_type}] | {weight} | {title} | "
        f"{subject or '—'} | {int(round_num):05d} | null |"
    )
    with open(index_path, "w", encoding="utf-8") as f:
        f.write(text + "\n" + row + "\n")


def strip_memory_heading(text):
    """移除已有记忆标题行，避免 LTM 迁移时嵌套 ## MEM 标题。"""
    body = str(text or "").strip()
    if not body:
        return ""
    lines = body.splitlines()
    if lines and re.match(r"^##\s+MEM-[0-9A-F]{8}\b", lines[0].strip()):
        return "\n".join(lines[1:]).strip()
    return body


def remove_ltm_body_block(md_path, mem_id):
    """从 LTM 正文层物理移除指定记忆块。"""
    if not os.path.isfile(md_path):
        return
    try:
        with open(md_path, "r", encoding="utf-8") as f:
            content = f.read()
    except OSError:
        return
    clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
    pattern = re.compile(
        rf"(?ms)^##\s+MEM-{re.escape(clean_id)}\b.*?(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
    )
    new_content, count = pattern.subn("", content)
    if count == 0:
        return
    tmp = md_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(new_content.strip() + ("\n" if new_content.strip() else ""))
        os.replace(tmp, md_path)
    except OSError:
        if os.path.isfile(tmp):
            os.remove(tmp)


def remove_ltm_index_row(index_path, mem_id):
    """从 LTM index.md 移除指定记忆行。"""
    if not os.path.isfile(index_path):
        return
    try:
        with open(index_path, "r", encoding="utf-8") as f:
            lines = f.readlines()
    except OSError:
        return
    new_lines = [
        line for line in lines
        if not line.lstrip().startswith(f"| {mem_id} ")
    ]
    if len(new_lines) == len(lines):
        return
    tmp = index_path + ".tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.writelines(new_lines)
        os.replace(tmp, index_path)
    except OSError:
        if os.path.isfile(tmp):
            os.remove(tmp)


def move_ltm_keyword_tier(mem_id, source_tier, dest_tier):
    """LTM 降格时同步倒排索引层级标签。"""
    try:
        from data.memory_index import MemoryIndex
        mi = MemoryIndex()
        data = mi.load_ltm_index()
        old_tag = f"{mem_id}[{source_tier[0]}]"
        keywords = [
            kw for kw, tags in data.get("index", {}).items()
            if old_tag in tags
        ]
        mi.remove_ltm_entry(mem_id, tier=source_tier)
        if keywords:
            mi.add_ltm_keywords(mem_id, keywords, tier=dest_tier)
    except Exception:
        pass
