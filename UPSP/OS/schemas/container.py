"""
工作容器 Schema — 9种容器 + container_registry
DDS §13-18

9种容器:
  DC- 辩证链 / EC- 事件链 / PRJ- 项目 / SKL- 技能
  IMM- 免疫 / CHR- 编年史 / COR- 语料库 / FUT- 未来 / ITR- 迭代

每种容器的目录结构:
  LTM/{ContainerType}/{container_id}/
    open.md     — 开放条目（DC/EC/PRJ/SKL/CHR/COR/ITR）
    closed.md   — 已关闭条目（DC/EC/PRJ/SKL/CHR/COR/ITR）
    active.md   — 活跃状态（IMM 专用）
    resolved.md — 已解决（IMM 专用）
    acquired.md — 已获得（IMM 专用）
    objectives.md / plans.md / predictions.md — FUT 三平级
    meta.json   — 容器元数据
    index.md    — 索引

必选8字段: id / type / title / status / created_at / updated_at / entries / tags
"""
from constants import local_now

# ============================================================
# 容器类型定义
# ============================================================

CONTAINER_TYPES = {
    "DC":  {"name": "辩证链",    "dir": "Dialectics", "desc": "辩证推理过程"},
    "EC":  {"name": "事件链",    "dir": "Events",     "desc": "事件序列"},
    "PRJ": {"name": "项目",      "dir": "Projects",   "desc": "项目/任务"},
    "SKL": {"name": "技能",      "dir": "Skills",     "desc": "技能卡"},
    "IMM": {"name": "免疫",      "dir": "Immune",     "desc": "安全/免疫记录"},
    "CHR": {"name": "编年史",    "dir": "Chronicle",  "desc": "编年/节志"},
    "COR": {"name": "语料库",    "dir": "Corpus",     "desc": "训练语料"},
    "FUT": {"name": "未来",      "dir": "Future",     "desc": "目标/计划/预测"},
    "ITR": {"name": "迭代",      "dir": "Iteration",  "desc": "版本迭代记录"},
}

# 注意：WB(调度台)不进 CONTAINER_TYPES，不是容器

CONTAINER_STATUS_MACHINES = {
    "DC": ("ongoing", "suspended", "concluded"),
    "EC": ("active", "interrupted", "restarted", "ended", "cancelled"),
    "PRJ": ("active", "paused", "ended"),
    "SKL": ("active", "expired", "planned"),
    "IMM": ("active_threat", "monitoring", "resolved", "acquired"),
    "CHR": ("open", "closed", "archived"),
    "COR": ("open", "closed", "archived"),
    "FUT": ("planned", "in_progress", "completed", "abandoned"),
    "ITR": ("collecting", "planned", "training", "deployed", "retired"),
}

DEFAULT_CONTAINER_STATUS = {
    prefix: statuses[0]
    for prefix, statuses in CONTAINER_STATUS_MACHINES.items()
}

# ============================================================
# 容器元数据 meta.json（8必选字段 + 扩展）
# ============================================================

CONTAINER_META_FIELDS = {
    "id":          ("str",  "容器编号，如 DC-001"),
    "type":        ("str",  "容器类型前缀，如 DC"),
    "title":       ("str",  "容器标题"),
    "status":      ("str",  "按容器类型状态机取值"),
    "created_at":  ("str",  "ISO时间戳"),
    "updated_at":  ("str",  "ISO时间戳"),
    "entries":     ("int",  "条目计数"),
    "tags":        ("list", "语义标签"),
    # 扩展字段
    "linked_memories": ("list", "关联记忆条目ID"),
    "description":     ("str",  "容器描述"),
}


def default_container_meta(container_id, ctype, title=""):
    """返回全新的容器 meta.json"""
    now = local_now().isoformat()
    return {
        "id": container_id,
        "type": ctype,
        "title": title or container_id,
        "status": DEFAULT_CONTAINER_STATUS.get(ctype, "open"),
        "created_at": now,
        "updated_at": now,
        "entries": 0,
        "tags": [],
        "linked_memories": [],
        "description": "",
    }
# ============================================================
# 标准条目格式（容器内的 ## 段）
# ============================================================

CONTAINER_ENTRY_TEMPLATE = """## {entry_id}
**{title}**
创建时间：{created_at}
标签：{tags}

{content}
"""


# ============================================================
# container_registry.json
# ============================================================

def default_container_registry():
    """返回全新的容器注册表"""
    return {
        "_comment": "容器类型注册表（9个工作容器类型声明，WB不进此表）",
        "_version": "Base-0.15.2",
        "containers": [
            {
                "prefix": prefix,
                "type": prefix,
                "name": info["name"],
                "dir": info["dir"],
                "desc": info["desc"],
                "builtin": True,
                "status": "enabled",
            }
            for prefix, info in CONTAINER_TYPES.items()
        ],
    }


def default_registry_entry(prefix, name, path, status=None):
    """返回注册表中的一个容器条目"""
    now = local_now().isoformat()
    return {
        "prefix": prefix,
        "name": name,
        "path": path,
        "status": status or DEFAULT_CONTAINER_STATUS.get(prefix, "open"),
        "created_at": now,
        "updated_at": now,
    }


# ============================================================
# 校验
# ============================================================

def validate_container_meta(meta):
    """校验容器 meta.json"""
    errors = []
    if "watched" in (meta or {}):
        errors.append("容器 meta 含退役字段 watched，请先运行迁移脚本")
    required = ["id", "type", "title", "status", "created_at", "updated_at", "entries", "tags"]
    for k in required:
        if k not in meta:
            errors.append(f"容器 meta 缺字段: {k}")
    ctype = meta.get("type")
    if ctype not in CONTAINER_TYPES:
        errors.append(f"未知容器类型: {ctype}")
    else:
        allowed_statuses = CONTAINER_STATUS_MACHINES.get(ctype, ())
        if meta.get("status") not in allowed_statuses:
            errors.append(f"容器状态非法: {meta.get('status')}")
    return (len(errors) == 0, errors)
