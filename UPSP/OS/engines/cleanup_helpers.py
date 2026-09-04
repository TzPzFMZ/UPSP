"""CleanupPipeline 的善后文本 helper。"""
import re


_MEMORY_FIELD_LABELS = (
    "交互对象", "入库", "最后调用", "入库轮次", "创建轮次", "入库时间",
    "最近调用轮次", "最近调用时间", "标题", "梗概", "摘要", "内容", "正文",
    "梦源", "现状概况", "标签", "感受词", "关联容器",
    "挂接备注", "挂接备注更新时间", "注释", "权重", "访问", "公开性",
)
_MEMORY_FIELD_PATTERN = "|".join(
    map(re.escape, sorted(_MEMORY_FIELD_LABELS, key=len, reverse=True))
)
_MEMORY_FIELD_LINE = re.compile(
    rf"^(?:\*\*(?:{_MEMORY_FIELD_PATTERN})\*\*|(?:{_MEMORY_FIELD_PATTERN}))"
    r"(?:（[^）]*）)?[:：].*$"
)


def extract_memory_field(text, label):
    """从记忆正文中取 `字段：值` 或 `**字段**：值`。"""
    escaped = re.escape(label)
    pattern = rf"(?m)^(?:\*\*{escaped}\*\*|{escaped})(?:（[^）]*）)?[:：](.*)$"
    match = re.search(pattern, text or "")
    return match.group(1).strip() if match else ""


def extract_memory_free_text(text):
    """剥离已知结构字段，返回条目中仍然存在的裸正文。"""
    return "\n".join(
        line for line in str(text or "").splitlines()
        if not _MEMORY_FIELD_LINE.match(line.strip())
    ).strip()


def round_text(value):
    """把轮号渲染为中文短标签。"""
    try:
        return f"第{int(value)}轮"
    except Exception:
        return "未知"


def strip_memory_heading(text):
    """移除已有记忆标题行，避免 LTM 迁移时嵌套 ## MEM 标题。"""
    body = str(text or "").strip()
    if not body:
        return ""
    lines = body.splitlines()
    if lines and re.match(r"^##\s+MEM-[0-9A-F]{8}\b", lines[0].strip()):
        return "\n".join(lines[1:]).strip()
    return body
