"""
记忆条目完整 Schema
DDS §9 记忆体系 + §9.2 记忆编号 + §10 LTM降格

四个关联文件：
  memory.md  — 记忆条目正文（追加式，## MEM-TTTTTNNN 段）
  heat.json  — 热度值（脚本独占管理）
  meta.json  — 24字段元数据（key=mem_id）
  keywords.json — 倒排索引（关键词→条目ID列表）
  index.md   — 索引行表格（人类可读）
"""
from datetime import datetime
from constants import local_now
from paths import ACTIVE_INSTANCE_ID

# ============================================================
# memory.md 条目格式（DDS §9.2）
# ============================================================

# 记忆条目模板
# ## MEM-TTTTTNNN
# **{title}**
# 摘要：{summary}
# 创建时间：{created_at}
# 入库时间：{stored_at}
# 权重：{weight}
# 标签：{tags}
# 关联容器：{linked_containers}

MEMORY_ENTRY_TEMPLATE = """## MEM-{mem_id}  [{morph}]  权重{weight}
**交互对象**：{subject}
**入库**：{created_round_text}
**最后调用**：{last_recalled_round_text}
**标题**：{title}
梦源：{dream_text}
现状概况：{current_overview}
{content_line}
创建时间：{created_at}
入库时间：{stored_at_text}
标签：{tags}
感受词：{feelings}
关联容器：{linked_containers}
"""

# ============================================================
# heat.json 条目格式（DDS §8-9）
# ============================================================

HEAT_ENTRY_FIELDS = {
    "H":              ("int",    "§8",  "当前热度 0-100"),
    "zone":           ("str",    "§8",  "显著/未定/衰减"),
    "AH_high":        ("int",    "§8",  "累计高峰到访次数"),
    "AH_low":         ("int",    "§8",  "累计低谷次数"),
    "last_heat_at":   ("str",    "§8",  "最后加热时间 ISO"),
    "last_high_at":   ("str|None","§8", "最后高峰时间"),
    "degrade":        ("bool",   "§9",  "AH_low≥3时脚本置true，触发遗忘分流"),
    "compression":    ("bool",   "§9",  "创建时由weight决定：[F][S]=true, [A]=false"),
    "heat_locked":    ("bool",   "§4.7", "STM热度锁定，true时固定H=80"),
}


def default_heat_entry(
    weight=2,
    initial_by_weight=None,
    significant_threshold=70,
    uncertain_threshold=40,
):
    """返回全新的 heat.json 条目"""
    now = local_now().isoformat()
    mapping = initial_by_weight or {
        "1": 40, "2": 50, "3": 60, "4": 70, "5": 80,
    }
    heat = mapping[str(max(1, min(5, int(weight))))]
    zone = (
        "显著" if heat >= significant_threshold
        else "未定" if heat >= uncertain_threshold
        else "衰减"
    )
    return {
        "H": heat,
        "zone": zone,
        "AH_high": 0,
        "AH_low": 0,
        "last_heat_at": now,
        "last_high_at": None,
        "degrade": False,
        "compression": weight >= 3,
        "heat_locked": False,
    }


def default_heat_json():
    return {"_comment": "STM 热度值（脚本独占管理）", "entries": {}}


# ============================================================
# meta.json 24字段元数据（DDS §9.2 + Spec193 + Spec724 + Spec732 + Spec746）
# ============================================================

META_ENTRY_FIELDS = {
    "id":                   ("str",    "§9.2", "MEM-TTTTTNNN"),
    "type":                 ("str",    "§9.2", "F/S/A/P，P为钉选锁定层"),
    "weight":               ("int",    "§9.2", "0-5，权重→形态映射"),
    "title":                ("str",    "§9.2", "≤16字"),
    "dream":                ("bool",   "Spec193", "是否由梦境素材升格而来"),
    "created_at":           ("str",    "§9.2", "ISO时间戳"),
    "stored_at":            ("str",    "§9.2", "正式入库时间；未入库为空"),
    "last_recalled_at":     ("str",    "§9.2", "ISO时间戳"),
    "created_round":        ("int|None","§9.2","创建轮号"),
    "created_instance_id":  ("str",    "Spec732", "创建分身"),
    "last_recalled_round":  ("int|None","§9.2","最后召回轮号"),
    "last_recalled_instance_id": ("str", "Spec732", "最后召回分身"),
    "source":               ("str",    "§9.2", "前端|终端|地点"),
    "model":                ("str",    "§9.2", "产出模型标识"),
    "subject":              ("str|None","§9.2","记忆涉及的活动关系主体规范 ID"),
    "access":               ("str",    "§9.2", "public/private"),
    "recalled":             ("bool",   "§9.2", "是否曾完成回忆重整"),
    "current_overview":     ("str",    "Spec193", "最新容器挂接语境下的现状概况"),
    "current_overview_updated_at": ("str", "Spec724", "现状概况最后真实变化时间；未知为空"),
    "tags":                 ("list",   "§9.2", "语义标签（软链接）"),
    "linked_containers":    ("list",   "§9.2", "关联工作容器编号（硬链接）"),
    "decay_period_days":    ("int",    "§9.2", "衰减总周期天数"),
    "decay_countdown_days": ("int",    "§9.2", "衰减倒计时"),
    "media":                ("list",   "§9.2", "附属媒体文件路径"),
}


def default_meta_entry(mem_id, title="", weight=2, subject=None, model=""):
    """返回全新的 meta.json 条目（24字段）"""
    now = local_now().isoformat()
    return {
        "id": mem_id,
        "type": "F" if weight >= 5 else "S" if weight >= 3 else "A",
        "weight": weight,
        "title": title[:16] if title else mem_id,
        "dream": False,
        "created_at": now,
        "stored_at": "",
        "last_recalled_at": now,
        "created_round": None,
        "created_instance_id": ACTIVE_INSTANCE_ID,
        "last_recalled_round": None,
        "last_recalled_instance_id": ACTIVE_INSTANCE_ID,
        "source": "",
        "model": model,
        "subject": subject,
        "access": "public",
        "recalled": False,
        "current_overview": "",
        "current_overview_updated_at": "",
        "tags": [],
        "linked_containers": [],
        "decay_period_days": 30,
        "decay_countdown_days": 30,
        "media": [],
    }


def default_meta_json():
    return {"_comment": "STM 元数据（24字段/条目）"}


# ============================================================
# keywords.json 倒排索引（DDS §26）
# ============================================================

def default_keywords_json():
    return {"_comment": "STM 倒排索引（关键词→条目ID列表）", "index": {}}


# ============================================================
# index.md 索引行（DDS §9）
# ============================================================

# 表格格式
# | 编号 | 类型 | 权重 | 标题 | 梦源 | 交互对象 | 创建轮/最后调用轮 | 现状概况 |

INDEX_HEADER = "| 编号 | 类型 | 权重 | 标题 | 梦源 | 交互对象 | 创建轮/最后调用轮 | 现状概况 |"
INDEX_SEPARATOR = "|------|------|------|------|------|---------|-------------------|----------|"
