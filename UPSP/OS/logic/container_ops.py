"""
容器操作逻辑 — 创建/打开/关闭容器、linked_containers 写入、悬空检测
DDS §25 工作容器 + §25.8 挂靠分类 + §25.9 悬空检测

职责：纯逻辑层——决定"该做什么"，不直接写文件
写文件走 data/container_store.py
"""
from datetime import datetime


# 容器挂靠分类（DDS §25.8）
BINDING_REQUIRED = ["DC-", "EC-", "PRJ-", "SKL-", "FUT-"]

# 悬空弹窗格式
ORPHAN_POPUP_HEADER = "[!悬空容器] 以下容器缺少挂靠记忆条目："
ORPHAN_POPUP_FOOTER = "请在当轮或下轮通过反应步补写记忆条目并挂靠。"

def check_orphan(container_info, current_round_start, binding="required"):
    """
    检查单个容器是否悬空。
    container_info: 容器注册表条目 dict (id, type, status, entries, created_at...)
    current_round_start: 本轮开始时间 datetime
    返回: (is_orphan, reason) 或 (False, None)
    """
    if binding != "required":
        return False, None

    # 从容器 ID 提取类型前缀（DC/EC/PRJ/SKL/FUT + "-"）
    raw_id = container_info.get("id", "")
    prefix = raw_id.split("-")[0] + "-" if "-" in raw_id else raw_id[:3]
    entries = container_info.get("entries", [])
    status = container_info.get("status", "")
    created_at = container_info.get("created_at", "")

    # 排除本轮新建（created_at 在本轮时间窗口内）
    if created_at:
        try:
            created = datetime.fromisoformat(created_at)
            if created >= current_round_start:
                return False, None  # 刚创建，合法为空
        except (ValueError, TypeError):
            pass

    # DC/EC: 链内笔记无 entries
    if prefix in ("DC-", "EC-"):
        if not entries or len(entries) == 0:
            return True, f"{container_info.get('id','?')}（{container_info.get('title','')}）— 链内笔记无挂靠"
    # PRJ: entries 为空
    elif prefix == "PRJ-":
        if not entries or len(entries) == 0:
            return True, f"{container_info.get('id','?')}（{container_info.get('title','')}）— 项目零条目"
    # SKL: linked_containers 为空且 active
    elif prefix == "SKL-":
        if status == "active":
            linked = container_info.get("linked_containers", [])
            if not linked or len(linked) == 0:
                return True, f"{container_info.get('id','?')}（{container_info.get('title','')}）— 技能无关联记忆"
    # FUT: 内容条目无 linked_containers
    elif prefix == "FUT-":
        linked = container_info.get("linked_containers", [])
        if not linked or len(linked) == 0:
            return True, f"{container_info.get('id','?')}（{container_info.get('title','')}）— 未来条目无链接"

    return False, None


def scan_orphans(container_store, current_round_start):
    """
    扫描所有必挂容器，返回悬空列表。
    container_store: data/container_store 实例
    current_round_start: 本轮开始时间
    返回: (orphan_list, popup_text_or_none)
    """
    orphans = []

    for prefix in BINDING_REQUIRED:
        containers = container_store.list_containers(prefix=prefix)
        for c in containers:
            is_orphan, reason = check_orphan(c, current_round_start, binding="required")
            if is_orphan:
                orphans.append({"container_id": c.get("id"), "reason": reason})

    if not orphans:
        return [], None

    lines = [ORPHAN_POPUP_HEADER]
    for o in orphans:
        lines.append(f"  {o['reason']}")
    lines.append("")
    lines.append(ORPHAN_POPUP_FOOTER)
    return orphans, "\n".join(lines)
