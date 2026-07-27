"""
关系卡 Schema
DDS §11 关系系统

关系六轴: 信任/安心/重视/投入/坦诚/共振
每轴 -100 ~ +100，21档

文件:
  relation/relation_registry.json — 关系卡注册表
  relation/{category}/{id}.md    — 单张关系卡
"""
from constants import local_now

# ============================================================
# 关系六轴定义（DDS §11）
# ============================================================

RELATION_AXES = {
    "trust":       {"name": "信任", "range": (-100, 100)},
    "safety":      {"name": "安心", "range": (-100, 100)},
    "value":       {"name": "重视", "range": (-100, 100)},
    "investment":  {"name": "投入", "range": (-100, 100)},
    "honesty":     {"name": "坦诚", "range": (-100, 100)},
    "resonance":   {"name": "共振", "range": (-100, 100)},
}

# ============================================================
# 关系卡字段定义
# ============================================================

RELATION_CARD_FIELDS = {
    "id":          ("str",  "关系卡内部编号"),
    "name":        ("str",  "对象名称"),
    "category":    ("str",  "self/ours/them/orgs"),
    "axes":        ("dict", "六轴值 {trust: 0, safety: 0, ...}"),
    "notes":       ("list", "交互笔记列表 [{date, content}]"),
    "history":     ("list", "轴值变动历史 [{date, axis, old, new}]"),
    "created_at":  ("str",  "ISO时间戳"),
    "updated_at":  ("str",  "ISO时间戳"),
    "status":      ("str",  "active/archived"),
    "tags":        ("list", "语义标签"),
    "aliases":     ("list", "别名"),
    "summary_resident": ("bool", "STATUSBAR 关系摘要跨轮常驻标记"),
    "body_resident": ("bool", "CONTENT 关系正文跨轮常驻标记"),
}

RELATION_LEGACY_PREFIX = "REL-"


def relation_public_name(value):
    """返回隐藏旧 REL-* 前缀的模型可见关系名。"""
    text = str(value or "").strip()
    while (
        text.startswith(RELATION_LEGACY_PREFIX)
        and len(text) > len(RELATION_LEGACY_PREFIX)
    ):
        text = text[len(RELATION_LEGACY_PREFIX):].strip()
    return text


def relation_card_label(card_or_id):
    if isinstance(card_or_id, dict):
        return relation_public_name(
            card_or_id.get("name")
            or card_or_id.get("subject")
            or card_or_id.get("id")
        )
    return relation_public_name(card_or_id)


def default_relation_card(card_id, name, category="ours"):
    """返回全新的关系卡"""
    now = local_now().isoformat()
    return {
        "id": card_id,
        "name": name,
        "category": category,
        "axes": {
            "trust": 0,
            "safety": 0,
            "value": 0,
            "investment": 0,
            "honesty": 0,
            "resonance": 0,
        },
        "notes": [],
        "history": [],
        "created_at": now,
        "updated_at": now,
        "status": "active",
        "tags": [],
        "aliases": [],
        "summary_resident": False,
        "body_resident": False,
    }


# ============================================================
# relation_registry.json
# ============================================================

def default_relation_registry():
    return {
        "_comment": "关系卡注册表",
        "cards": [],
    }


def default_registry_card_entry(card_id, name, category, path):
    """注册表中的一条关系卡记录。"""
    now = local_now().isoformat()
    return {
        "id": card_id,
        "name": name,
        "category": category,
        "path": path,
        "status": "active",
        "summary_resident": False,
        "body_resident": False,
        "aliases": [],
        "tags": [],
        "updated_at": now,
    }
