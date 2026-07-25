"""
进化集逻辑层 — pending 统计、任务上下文构造、LLM 标记块解析

logic 层不碰磁盘。真实落盘由 data.evolution_store 执行。
"""
import re
from collections import Counter


def summarize_pending(tacit_records, connection_records):
    action_counts = Counter()
    item_counts = Counter()
    word_counts = Counter()
    pair_counts = Counter()

    for record in tacit_records:
        for item in _iter_tacit_items(record):
            action = str(item.get("action", "") or "unknown")
            item_id = str(item.get("item_id", "") or item.get("memory_id", ""))
            action_counts[action] += 1
            if item_id:
                item_counts[item_id] += 1

    for record in connection_records:
        word_a = str(record.get("word_a", "") or "")
        word_b = str(record.get("word_b", "") or "")
        if word_a:
            word_counts[word_a] += 1
        if word_b:
            word_counts[word_b] += 1
        if word_a and word_b:
            pair_counts[f"{word_a}|{word_b}"] += 1

    return {
        "tacit_count": len(tacit_records),
        "connection_count": len(connection_records),
        "tacit_actions": dict(action_counts),
        "top_tacit_items": item_counts.most_common(8),
        "top_connection_words": word_counts.most_common(8),
        "top_connection_pairs": pair_counts.most_common(8),
    }


def extract_evolution_blocks(text):
    if not text:
        return []
    blocks = re.findall(
        r"<!--\s*EVOLUTION\s*-->\s*(.*?)\s*<!--\s*/EVOLUTION\s*-->",
        text,
        flags=re.DOTALL | re.IGNORECASE,
    )
    return [block.strip() for block in blocks if block.strip()]


def build_evolution_context(stats, tacit_records, connection_records, max_records=8):
    lines = [
        "# 进化集整理任务",
        "",
        "你正在自主轮中整理训练材料。请只基于下面 pending 材料做跨轮统计与模式识别，输出一个进化集中间产品。",
        "输出必须包含一个 `<!-- EVOLUTION -->...<!-- /EVOLUTION -->` 块；块外可以为空。",
        "",
        "## 统计",
        "",
        f"- 默契集 pending：{stats.get('tacit_count', 0)}",
        f"- 联系集 pending：{stats.get('connection_count', 0)}",
        f"- 默契集动作分布：{stats.get('tacit_actions', {})}",
        f"- 高频联系词：{stats.get('top_connection_words', [])}",
        f"- 高频联系词对：{stats.get('top_connection_pairs', [])}",
        "",
        "## 默契集样本",
        "",
    ]
    lines.extend(_format_records(tacit_records[:max_records]))
    lines.extend(["", "## 联系集样本", ""])
    lines.extend(_format_records(connection_records[:max_records]))
    lines.extend([
        "",
        "## 输出格式",
        "",
        "<!-- EVOLUTION -->",
        "（用 3-8 条 bullet 写出可被中枢后续吸收的模式、倾向或规则雏形）",
        "<!-- /EVOLUTION -->",
    ])
    return "\n".join(lines)


def _iter_tacit_items(record):
    items = record.get("items")
    if isinstance(items, list) and items:
        for item in items:
            if isinstance(item, dict):
                yield item
        return
    yield record


def _format_records(records):
    if not records:
        return ["- （无）"]
    lines = []
    for record in records:
        parts = []
        for key, value in record.items():
            if value in ("", None):
                continue
            parts.append(f"{key}={value}")
        lines.append("- " + "；".join(parts))
    return lines
