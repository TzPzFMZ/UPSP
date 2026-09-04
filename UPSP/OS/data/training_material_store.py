"""
训练材料存储层 — 默契集/联系集/联想集的持久化写入
DDS §31 训练材料 + cleanup.md §五

data 层独占文件 I/O，logic 层不碰磁盘。
"""
import json
import os
from constants import local_now


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)
    return path


def write_tacit_set(pending_path, processed_path, round_num, associations):
    if not associations:
        return 0
    ensure_dir(os.path.dirname(pending_path))
    now = local_now().isoformat()
    items = []
    by_action = {
        "kept": [],
        "dropped": [],
        "added": [],
    }
    for a in associations:
        action = a.get("action", "kept")
        item_id = a.get("item_id", "")
        item = {
            "item_id": item_id,
            "action": action,
            "note": a.get("note", ""),
        }
        for key in (
            "item_type",
            "origin",
            "selection_trigger",
            "evidence_refs",
            "drop_reason",
        ):
            if key in a:
                item[key] = a.get(key)
        items.append(item)
        if action in by_action and item_id:
            by_action[action].append(item_id)
    entry = {
        "round": round_num,
        "round_id": str(round_num),
        "timestamp": now,
        "kept": by_action["kept"],
        "dropped": by_action["dropped"],
        "added": by_action["added"],
        "items": items,
    }
    with open(pending_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
    return 1


def write_connection_set(pending_path, processed_path, round_num, bridges):
    """联系集写入：六字段 JSONL 逐行追加，每轮≤8条。
    格式: {word_a, entry_a, word_b, entry_b, round_id, timestamp}
    设计来源: 备忘录_联想联系索引体系_20260502.md §二"""
    if not bridges:
        return 0
    ensure_dir(os.path.dirname(pending_path))
    now = local_now().isoformat()
    count = 0
    with open(pending_path, "a", encoding="utf-8") as f:
        for b in bridges[:8]:
            entry = {
                "word_a": b.get("word_a", ""),
                "entry_a": b.get("entry_a", ""),
                "word_b": b.get("word_b", ""),
                "entry_b": b.get("entry_b", ""),
                "round_id": str(round_num),
                "timestamp": now,
            }
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")
            count += 1
    return count


def write_association_counts(association_dir, assoc_data):
    """联想集五表落盘——同条目内共现频次，纯脚本暴力计数。
    设计来源: 备忘录_联想联系索引体系_20260502.md §一
    assoc_data: {
        "assoc_kw_kw":       [(kw1, kw2), ...],   # 关键词×关键词
        "assoc_kw_ifeel":    [(kw, ifeel), ...],  # 关键词×交互感受词
        "assoc_kw_rfeel":    [(kw, rfeel), ...],  # 关键词×关系感受词
        "assoc_ifeel_rfeel": [(ifeel, rfeel), ...], # 交互感受×关系感受
        "assoc_object_rfeel": [(obj, rfeel), ...],  # 交互对象×关系感受
    }"""
    if not assoc_data:
        return
    ensure_dir(association_dir)
    table_files = {
        "assoc_kw_kw": "assoc_kw_kw.json",
        "assoc_kw_ifeel": "assoc_kw_ifeel.json",
        "assoc_kw_rfeel": "assoc_kw_rfeel.json",
        "assoc_ifeel_rfeel": "assoc_ifeel_rfeel.json",
        "assoc_object_rfeel": "assoc_object_rfeel.json",
    }
    for key, filename in table_files.items():
        pairs = assoc_data.get(key, [])
        if pairs:
            update_count_file(os.path.join(association_dir, filename), pairs)


def update_count_file(filepath, pairs):
    data = {}
    if os.path.exists(filepath):
        try:
            with open(filepath, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError):
            data = {}
    for a, b in pairs:
        key = f"{a}|||{b}"
        data[key] = data.get(key, 0) + 1
    tmp = filepath + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, filepath)


KEYWORD_DEGREE_TABLES = {
    "assoc_kw_kw.json": "symmetric",
    "assoc_kw_ifeel.json": "left_keyword",
    "assoc_kw_rfeel.json": "left_keyword",
}


def keyword_degree_snapshot(association_dir):
    """Return keyword -> distinct association partner count.

    Repeated pairs increase the pair count in the table, but do not increase
    keyword degree.
    """
    partners = {}
    for filename, mode in KEYWORD_DEGREE_TABLES.items():
        data = _read_count_file(os.path.join(association_dir, filename))
        for key in data:
            if "|||" not in key:
                continue
            left, right = key.split("|||", 1)
            if not left or not right:
                continue
            partners.setdefault(left, set()).add(f"{filename}:{right}")
            if mode == "symmetric":
                partners.setdefault(right, set()).add(f"{filename}:{left}")
    return {keyword: len(values) for keyword, values in partners.items()}


def _read_count_file(filepath):
    if not os.path.exists(filepath):
        return {}
    try:
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
    except (json.JSONDecodeError, OSError):
        return {}
    return data if isinstance(data, dict) else {}
