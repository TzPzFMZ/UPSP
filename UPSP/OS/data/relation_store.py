"""
关系卡读写 — relation_registry.json + 关系卡文件
DDS §11 关系系统

关系六轴：信任/安心/重视/投入/坦诚/共振（-100~+100）
"""
import json
import os
import re
from datetime import datetime

from data.atomic_write import atomic_write_json, atomic_write_text
from paths import RELATION_DIR, RELATION_REGISTRY_JSON
from schemas.relation import (
    default_relation_card, default_relation_registry,
    default_registry_card_entry, relation_card_label, relation_public_name,
)
from errors import WriteError, ReadError
from constants import local_now


AXIS_NAMES = {
    "trust": "信任",
    "safety": "安心",
    "value": "重视",
    "investment": "投入",
    "honesty": "坦诚",
    "resonance": "共振",
}

class RelationStore:
    """关系卡读写管理"""

    def __init__(self):
        pass

    # ==============================================================
    # relation_registry.json
    # ==============================================================

    def load_registry(self):
        if not os.path.isfile(RELATION_REGISTRY_JSON):
            return default_relation_registry()
        try:
            with open(RELATION_REGISTRY_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(RELATION_REGISTRY_JSON, cause=e)

    def save_registry(self, reg):
        retired_fields = {"watched", "relation_watch"}
        if any(
            isinstance(card, dict) and retired_fields.intersection(card)
            for card in (reg or {}).get("cards", [])
        ):
            raise WriteError(
                RELATION_REGISTRY_JSON,
                message="关系注册表含退役字段 watched/relation_watch，请先运行迁移脚本",
            )
        atomic_write_json(RELATION_REGISTRY_JSON, reg)

    # ==============================================================
    # 关系卡 CRUD
    # ==============================================================

    def get_card_path(self, card_id, category="ours"):
        """关系卡文件路径（全部通过 paths.py 定义）"""
        return os.path.join(RELATION_DIR, category, f"{card_id}.md")

    def create_card(self, card_id, name, category="ours"):
        """创建新关系卡"""
        card = default_relation_card(card_id, name, category)
        path = self.get_card_path(card_id, category)
        os.makedirs(os.path.dirname(path), exist_ok=True)

        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(_format_relation_md(card))
            os.replace(tmp, path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(path, cause=e)

        # 注册表
        reg = self.load_registry()
        reg_entry = default_registry_card_entry(
            card_id, name, category,
            f"relation/{category}/{card_id}.md"
        )
        reg["cards"].append(reg_entry)
        self.save_registry(reg)

        return card

    def find_card(self, name_or_id):
        """按 id 或对象名查注册表条目，兼容旧注册表缺 name 的情况。"""
        needle = str(name_or_id or "").strip()
        if not needle:
            return None
        public_needle = relation_public_name(needle)
        reg = self.load_registry()
        for card in reg.get("cards", []):
            card_id = card.get("id", "")
            card_name = card.get("name") or card_id
            tokens = {
                card_id,
                card_name,
                relation_public_name(card_id),
                relation_public_name(card_name),
                f"REL-{relation_public_name(card_name)}",
            }
            tokens.update(str(x) for x in card.get("aliases") or [] if x)
            tokens.update(str(x) for x in card.get("tags") or [] if x)
            tokens.update(relation_public_name(x) for x in list(tokens) if x)
            if needle in tokens or public_needle in tokens:
                return card
        return None

    def resolve_active_subject(self, value):
        """Resolve one exact active relation id/name/alias to its canonical id."""
        needle = str(value or "").strip()
        if not needle:
            return None
        public_needle = relation_public_name(needle)
        matches = set()
        for card in self.load_registry().get("cards", []):
            if card.get("status", "active") != "active":
                continue
            card_id = str(card.get("id") or "").strip()
            card_name = str(card.get("name") or card_id).strip()
            tokens = {
                card_id,
                card_name,
                relation_public_name(card_id),
                relation_public_name(card_name),
                f"REL-{relation_public_name(card_name)}",
            }
            tokens.update(str(item).strip() for item in card.get("aliases") or [] if item)
            tokens.update(relation_public_name(item) for item in list(tokens) if item)
            if needle in tokens or public_needle in tokens:
                matches.add(card_id)
        return next(iter(matches)) if len(matches) == 1 else None

    def read_card(self, card_id, category=None):
        """读关系卡。category 未知时从注册表查"""
        resolved_id = str(card_id or "").strip()
        if category is None:
            reg = self.load_registry()
            for c in reg["cards"]:
                if c["id"] == card_id:
                    category = c["category"]
                    resolved_id = c["id"]
                    break
            if category is None:
                card = self.find_card(card_id)
                if card:
                    category = card.get("category") or "ours"
                    resolved_id = card.get("id") or resolved_id
                else:
                    category = "ours"

        path = self.get_card_path(resolved_id, category)
        if not os.path.isfile(path):
            return None
        try:
            card = _read_relation_md(path)
            card["id"] = resolved_id
            if category:
                card["category"] = category
            return card
        except OSError as e:
            raise ReadError(path, cause=e)


    def add_note(self, card_id, content):
        """追加交互笔记"""
        card = self.read_card(card_id)
        if card is None:
            return None
        observed_at = local_now().isoformat()
        note = {"date": observed_at, "content": content}
        path = self.get_card_path(card["id"], card.get("category", "ours"))
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = handle.read()
            raw = _replace_metadata_line(raw, "最后交互", observed_at)
            raw = _append_section_line(raw, "笔记", f"- {observed_at}：{content}")
            atomic_write_text(path, raw)
        except OSError as exc:
            raise WriteError(path, cause=exc)
        card["notes"].append(note)
        card["updated_at"] = observed_at
        return card

    def apply_state_settlement(self, card_id, axes, settlement_id, observed_at=None):
        """原子补丁关系六轴并记录结算 ID；同 ID 重试不重复累计。"""
        card = self.read_card(card_id)
        if card is None:
            raise ReadError(str(card_id), message="关系卡不存在")
        settlement_id = str(settlement_id or "").strip()
        if not settlement_id:
            raise ValueError("settlement_id_required")
        before = {
            axis: int(card.get("axes", {}).get(axis, 0))
            for axis in AXIS_NAMES
        }
        after = {
            axis: max(-100, min(100, int((axes or {}).get(axis, before[axis]))))
            for axis in AXIS_NAMES
        }
        if card.get("last_state_settlement_id") == settlement_id:
            return {
                "status": "already_applied",
                "card_id": card["id"],
                "settlement_id": settlement_id,
                "before": before,
                "after": before,
            }

        observed_at = observed_at or local_now().isoformat()
        path = self.get_card_path(card["id"], card.get("category", "ours"))
        try:
            with open(path, "r", encoding="utf-8") as handle:
                raw = handle.read()
            for axis, value in after.items():
                raw = _replace_axis_line(raw, axis, value)
            raw = _replace_metadata_line(raw, "最后状态结算", settlement_id)
            changes = "；".join(
                f"{AXIS_NAMES[axis]} {before[axis]:+d}→{after[axis]:+d}"
                for axis in AXIS_NAMES
                if before[axis] != after[axis]
            ) or "六轴无净变化"
            raw = _append_section_line(
                raw,
                "历史（History）",
                f"- {observed_at}：状态结算 {settlement_id}；{changes}",
            )
            atomic_write_text(path, raw)
        except OSError as exc:
            raise WriteError(path, cause=exc)
        return {
            "status": "applied",
            "card_id": card["id"],
            "settlement_id": settlement_id,
            "before": before,
            "after": after,
        }

    def list_cards(self, category=None, status="active"):
        """列出关系卡"""
        reg = self.load_registry()
        result = reg["cards"]
        if category:
            result = [c for c in result if c["category"] == category]
        if status:
            result = [c for c in result if c.get("status") == status]
        return result

    def set_summary_resident(self, card_id, enabled=True):
        """切换 STATUSBAR 关系摘要常驻标记。"""
        reg = self.load_registry()
        for c in reg.get("cards", []):
            if c.get("id") == card_id:
                c["summary_resident"] = bool(enabled)
                c["updated_at"] = local_now().isoformat()
                break
        self.save_registry(reg)
        return reg

    def set_body_resident(self, card_id, enabled=True):
        """切换 CONTENT 关系正文常驻标记；正文常驻时摘要同步常驻。"""
        reg = self.load_registry()
        for c in reg.get("cards", []):
            if c.get("id") == card_id:
                c["body_resident"] = bool(enabled)
                if enabled:
                    c["summary_resident"] = True
                c["updated_at"] = local_now().isoformat()
                break
        self.save_registry(reg)
        return reg


def _read_relation_md(path):
    """从 .md 文件读取关系卡 → dict"""
    with open(path, "r", encoding="utf-8") as f:
        content = f.read()
    card = {
        "id": "", "name": "", "category": "ours", "axes": {},
        "notes": [], "history": [], "status": "active", "tags": [],
        "created_at": "", "updated_at": "",
    }
    title_m = re.search(r'^#\s+(.+)$', content, re.MULTILINE)
    if title_m:
        card["name"] = title_m.group(1).strip()
    info_m = re.search(r'##\s*基础信息\s*\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
    if info_m:
        for line in info_m.group(1).strip().split("\n"):
            kv = re.match(r'-\s*([^：:]+)[：:]\s*(.*)', line.strip())
            if kv:
                k, v = kv.group(1), kv.group(2).strip()
                if k == "类别": card["category"] = v
                elif k == "创建时间": card["created_at"] = v
                elif k == "最后交互": card["updated_at"] = v
                elif k == "最后状态结算": card["last_state_settlement_id"] = v
    axes_m = re.search(r'##\s*关系六轴\s*\n(.*?)(?=\n##|\Z)', content, re.DOTALL)
    if axes_m:
        axis_map = {"信任": "trust", "安心": "safety", "重视": "value",
                    "投入": "investment", "坦诚": "honesty", "共振": "resonance"}
        for line in axes_m.group(1).strip().split("\n"):
            kv = re.match(r'-\s*(\S+)[：:]\s*([+-]?\d+)', line.strip())
            if kv:
                key = axis_map.get(kv.group(1), kv.group(1))
                card["axes"][key] = int(kv.group(2))
    # notes
    notes_m = re.search(r'##\s*笔记\s*\n((?:-.*\n?)*)', content, re.MULTILINE)
    if notes_m:
        for line in notes_m.group(1).strip().split("\n"):
            if line.strip().startswith("-") and "状态" not in line:
                card["notes"].append({"date": "", "content": line.strip().lstrip("- ")})
    # status
    status_m = re.search(r'[-*]\s*状态[：:]\s*(\w+)', content)
    if status_m:
        card["status"] = status_m.group(1)
    return card


def _format_relation_md(card):
    """关系卡 dict → .md 格式"""
    lines = [f"# {card.get('name', card.get('id', ''))}", "",
             "## 基础信息",
             f"- 类别：{card.get('category', 'ours')}",
             f"- 创建时间：{card.get('created_at', '')}",
             f"- 最后交互：{card.get('updated_at', '')}",
             "", "## 关系六轴"]
    if card.get("last_state_settlement_id"):
        lines.insert(6, f"- 最后状态结算：{card['last_state_settlement_id']}")
    for key, name in AXIS_NAMES.items():
        v = card.get("axes", {}).get(key, 0)
        lines.append(f"- {name}：{v:+d}")
    lines.append("")
    lines.append("## 历史（History）")
    for h in card.get("history", []):
        c = h.get("content", str(h)) if isinstance(h, dict) else str(h)
        d = h.get("date", "") if isinstance(h, dict) else ""
        lines.append(f"- {d}：{c}" if d else f"- {c}")
    # notes
    notes = card.get("notes", [])
    if notes:
        lines.append("")
        lines.append("## 笔记")
        for n in notes:
            c = n.get("content", str(n)) if isinstance(n, dict) else str(n)
            d = n.get("date", "") if isinstance(n, dict) else ""
            lines.append(f"- {d}：{c}" if d else f"- {c}")
    # status
    status = card.get("status", "active")
    lines.extend(["", f"- 状态：{status}"])
    return "\n".join(lines) + "\n"


def _replace_metadata_line(content, label, value):
    pattern = re.compile(rf"(?m)^-\s*{re.escape(label)}[：:].*$")
    replacement = f"- {label}：{value}"
    if pattern.search(content):
        return pattern.sub(replacement, content, count=1)
    section = re.search(r"(?ms)^##\s*基础信息\s*$.*?(?=^##\s|\Z)", content)
    if not section:
        raise ValueError("relation_basic_info_section_missing")
    block = section.group(0).rstrip() + "\n" + replacement + "\n\n"
    return content[:section.start()] + block + content[section.end():]


def _replace_axis_line(content, axis, value):
    if axis not in AXIS_NAMES:
        raise ValueError(f"unknown_relation_axis:{axis}")
    pattern = re.compile(rf"(?m)^-\s*{re.escape(AXIS_NAMES[axis])}[：:]\s*[+-]?\d+\s*$")
    matches = list(pattern.finditer(content))
    if len(matches) != 1:
        raise ValueError(f"relation_axis_line_count:{axis}:{len(matches)}")
    return pattern.sub(f"- {AXIS_NAMES[axis]}：{int(value):+d}", content, count=1)


def _append_section_line(content, heading, line):
    section = re.search(
        rf"(?ms)^##\s*{re.escape(heading)}\s*$.*?(?=^##\s|^-\s*状态[：:]|\Z)",
        content,
    )
    if section:
        block = section.group(0).rstrip() + "\n" + line + "\n\n"
        return content[:section.start()] + block + content[section.end():]
    status = re.search(r"(?m)^-\s*状态[：:].*$", content)
    insertion = f"## {heading}\n{line}\n\n"
    if status:
        return content[:status.start()] + insertion + content[status.start():]
    return content.rstrip() + "\n\n" + insertion


def _replace_status_line(content, status):
    pattern = re.compile(r"(?m)^-\s*状态[：:].*$")
    replacement = f"- 状态：{status}"
    if pattern.search(content):
        return pattern.sub(replacement, content, count=1)
    return content.rstrip() + "\n\n" + replacement + "\n"
