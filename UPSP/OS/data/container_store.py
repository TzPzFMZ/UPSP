"""
工作容器读写 — 9种容器 + container_registry.json
DDS §13-18

9种容器：DC/EC/PRJ/SKL/IMM/CHR/COR/FUT/ITR
WB 不进此表

容器目录结构（以 DC 为例）：
  LTM/Dialectics/DC-001/
    meta.json    — 容器元数据（8必选字段）
    open.md      — 开放条目
    closed.md    — 已关闭条目
    index.md     — 索引
"""
import json
import os
import re
from datetime import datetime
from pathlib import Path

from data.atomic_write import (
    atomic_write_json as _atomic_write_json,
    atomic_write_text as _atomic_write_text,
)
from utils.content_ranges import apply_explicit_range, range_kwargs_from_request
from paths import (
    LTM_DIR,
    CONTAINER_REGISTRY_JSON,
    CONTAINER_DIALECTICS_DIR, CONTAINER_EVENTS_DIR, CONTAINER_PROJECTS_DIR,
    CONTAINER_SKILLS_DIR, CONTAINER_IMMUNE_DIR, CONTAINER_CHRONICLE_DIR,
    CONTAINER_CORPUS_DIR, CONTAINER_FUTURE_DIR, CONTAINER_ITERATION_DIR,
)
from schemas.container import (
    CONTAINER_TYPES, default_container_meta, default_container_registry,
    default_registry_entry, validate_container_meta,
)
from errors import ContainerNotFoundError, WriteError, ReadError
from constants import local_now

# 前缀→目录映射
PREFIX_TO_DIR = {
    "DC":  CONTAINER_DIALECTICS_DIR,
    "EC":  CONTAINER_EVENTS_DIR,
    "PRJ": CONTAINER_PROJECTS_DIR,
    "SKL": CONTAINER_SKILLS_DIR,
    "IMM": CONTAINER_IMMUNE_DIR,
    "CHR": CONTAINER_CHRONICLE_DIR,
    "COR": CONTAINER_CORPUS_DIR,
    "FUT": CONTAINER_FUTURE_DIR,
    "ITR": CONTAINER_ITERATION_DIR,
}

# 安全校验
CONTAINER_ID_RE = re.compile(r"^(DC|EC|PRJ|SKL|IMM|CHR|COR|FUT|ITR)-[\w-]+$")
ALLOWED_CONTAINER_FILES = frozenset({
    "open.md", "closed.md", "index.md", "plan.md", "notes.md",
    "card.md", "changelog.md", "active.md", "resolved.md", "acquired.md",
    "objectives.md", "plans.md", "predictions.md",
})
SUPPORTED_FOCUS_CONTAINER_TYPES = frozenset({"DC", "EC", "PRJ", "SKL", "FUT"})
SKILL_CATEGORIES = frozenset({"habits", "procedures", "licenses", "patterns", "reflexes"})
SOURCE_SKILL_CATEGORIES = frozenset({"procedures", "patterns"})
SKILL_NAME_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")
FOCUS_TARGET_FILES = {
    "DC": frozenset({"open.md"}),
    "EC": frozenset({"open.md"}),
    "PRJ": frozenset({"plan.md", "notes.md"}),
    "SKL": frozenset({"card.md"}),
    "FUT": frozenset({"objectives.md", "plans.md", "predictions.md"}),
}
DEFAULT_FOCUS_TARGET = {
    "DC": "open.md",
    "EC": "open.md",
    "PRJ": "plan.md",
    "SKL": "card.md",
    "FUT": "plans.md",
}
LTM_INDEX_MD = os.path.join(LTM_DIR, "index.md")


def _safe_child(root, *parts):
    """路径拼接后校验仍在 root 子树内"""
    root_path = Path(root).resolve()
    path = root_path.joinpath(*parts).resolve()
    if path != root_path and root_path not in path.parents:
        raise ValueError(f"路径越界: {path}")
    return str(path)

def _load_json_or_default(path, default):
    if not os.path.isfile(path):
        return default
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        raise ReadError(path, cause=e)


class ContainerStore:
    """9种工作容器读写管理"""

    CORE_CONTAINER_ROOTS = ("DC", "EC", "PRJ", "SKL", "FUT")

    def __init__(self):
        self.ensure_core_container_roots()

    def ensure_core_container_roots(self):
        """Ensure core work-container roots exist in clean runtime layouts."""
        for prefix in self.CORE_CONTAINER_ROOTS:
            root = PREFIX_TO_DIR.get(prefix)
            if root:
                os.makedirs(root, exist_ok=True)

    # ==============================================================
    # container_registry.json 读写
    # ==============================================================

    def load_registry(self):
        if not os.path.isfile(CONTAINER_REGISTRY_JSON):
            return default_container_registry()
        try:
            with open(CONTAINER_REGISTRY_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(CONTAINER_REGISTRY_JSON, cause=e)

    def save_registry(self, reg):
        os.makedirs(os.path.dirname(CONTAINER_REGISTRY_JSON), exist_ok=True)
        tmp = CONTAINER_REGISTRY_JSON + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(reg, f, ensure_ascii=False, indent=2)
            os.replace(tmp, CONTAINER_REGISTRY_JSON)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(CONTAINER_REGISTRY_JSON, cause=e)


    def mount_focus(self, container_id, status_store):
        """挂载容器到 WB 焦点（调用 state_store 写 status.json）"""
        try:
            status_store.set("base.focus", container_id)
            # 同步更新容器的 focus 字段
            meta = self.read_meta(container_id)
            meta["focus"] = True
            self.save_meta(container_id, meta)
        except Exception:
            pass

    def unmount_focus(self, container_id, status_store):
        """从 WB 焦点卸挂"""
        try:
            # 保存 old_focus
            current = status_store.get("base.focus")
            if current:
                status_store.set("base.old_focus", current)
            status_store.set("base.focus", None)
            # 更新 focus
            meta = self.read_meta(container_id)
            meta["focus"] = False
            self.save_meta(container_id, meta)
        except Exception:
            pass

    def get_container_info(self, container_id):
        """从注册表获取容器信息"""
        for c in self.list_containers():
            if c.get("id") == container_id:
                return c
        raise ContainerNotFoundError(f"容器不在注册表中: {container_id}")

    def list_containers(self, prefix=None, status=None):
        """列出容器"""
        result = self._list_instance_containers()
        normalized_prefix = None
        if prefix:
            normalized_prefix = str(prefix).strip().rstrip("-")
            result = [c for c in result if c.get("prefix") == normalized_prefix]
        if status:
            result = [c for c in result if c.get("status") == status]
        return result

    # ==============================================================
    # 容器内容读写
    # ==============================================================

    def _get_container_dir(self, container_id):
        """根据容器 ID（如 DC-001）找到目录（安全校验）"""
        if not isinstance(container_id, str) or not CONTAINER_ID_RE.match(container_id):
            raise ValueError(f"非法容器ID: {container_id}")
        prefix = container_id.split("-")[0]
        if prefix not in PREFIX_TO_DIR:
            raise ValueError(f"未知前缀: {prefix}")
        if prefix == "SKL":
            parts = container_id.split("-", 2)
            if len(parts) != 3 or parts[1] not in SKILL_CATEGORIES or not parts[2]:
                raise ValueError("invalid_skill_container_id")
            return _safe_child(PREFIX_TO_DIR[prefix], parts[1], parts[2])
        return _safe_child(PREFIX_TO_DIR[prefix], container_id)

    def read_meta(self, container_id):
        """读容器 meta.json"""
        cdir = self._get_container_dir(container_id)
        meta_path = os.path.join(cdir, "meta.json")
        if not os.path.isfile(meta_path):
            raise ContainerNotFoundError(f"容器不存在: {container_id}")
        try:
            with open(meta_path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(meta_path, cause=e)

    def save_meta(self, container_id, meta):
        """写容器 meta.json"""
        cdir = self._get_container_dir(container_id)
        os.makedirs(cdir, exist_ok=True)
        meta_path = os.path.join(cdir, "meta.json")
        if "watched" in (meta or {}):
            raise WriteError(meta_path, message="容器 meta 含退役字段 watched，请先运行迁移脚本")
        meta["updated_at"] = local_now().isoformat()
        ok, errors = validate_container_meta(meta)
        if not ok:
            raise WriteError(meta_path, message="；".join(errors))
        tmp = meta_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(meta, f, ensure_ascii=False, indent=2)
            os.replace(tmp, meta_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(meta_path, cause=e)

    def read_entries(self, container_id, file_name="open.md"):
        """读容器条目文件（open.md/closed.md 等）"""
        if file_name not in ALLOWED_CONTAINER_FILES:
            raise ValueError(f"非法容器文件名: {file_name}")
        cdir = self._get_container_dir(container_id)
        path = _safe_child(cdir, file_name)
        if not os.path.isfile(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except OSError as e:
            raise ReadError(path, cause=e)

    def append_entry(self, container_id, title, content, file_name="open.md",
                     tags=None):
        """追加容器条目（原子）"""
        if file_name not in ALLOWED_CONTAINER_FILES:
            raise ValueError(f"非法容器文件名: {file_name}")
        cdir = self._get_container_dir(container_id)
        path = _safe_child(cdir, file_name)
        os.makedirs(cdir, exist_ok=True)

        now = local_now().isoformat()
        entry_text = f"\n## {title}\n创建时间：{now}\n\n{content}\n"

        if os.path.isfile(path):
            with open(path, "r", encoding="utf-8") as f:
                existing = f.read()
        else:
            existing = ""

        tmp = path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write((existing.rstrip() + "\n" + entry_text).strip() + "\n")
            os.replace(tmp, path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(path, cause=e)

        # 更新条目计数
        meta = self.read_meta(container_id)
        meta["entries"] = meta.get("entries", 0) + 1
        self.save_meta(container_id, meta)


    # ==============================================================
    # Spec 077: container_focus 工作容器焦点会话
    # ==============================================================

    def create_focus_container(self, container_type, title, target_file=None,
                               anchor_refs=None, round_num=0,
                               skill_category=None, skill_name=None):
        """创建首批可写焦点容器；实例不写入 LTM/container_registry.json。"""
        prefix = self._normalize_prefix(container_type)
        if prefix not in SUPPORTED_FOCUS_CONTAINER_TYPES:
            raise ValueError("unsupported_container_type")
        target = self._normalize_target_file(prefix, target_file)
        title = str(title or "").strip()
        if not title:
            raise ValueError("missing_title")
        anchor_refs = list(anchor_refs or [])

        if prefix in {"DC", "EC"}:
            container_id = self._next_numeric_container_id(prefix)
            path = self._create_chain_container(
                prefix, container_id, title, round_num=round_num)
        elif prefix == "PRJ":
            container_id = self._next_project_id()
            path = self._create_project_container(container_id, title)
        elif prefix == "SKL":
            container_id = self._skill_container_id(skill_category, skill_name)
            path = self._create_skill_container(
                container_id, title, skill_category, skill_name, anchor_refs)
        else:
            category = self._future_category_from_target(target)
            container_id = self._next_future_id(category)
            path = self._create_future_entry(container_id, title, category, anchor_refs)

        self.refresh_master_index()
        return {
            "status": "applied",
            "container_id": container_id,
            "container_type": prefix,
            "title": title,
            "target_file": target,
            "path": path,
            "link_required": not bool(anchor_refs),
        }

    def container_exists(self, container_id):
        try:
            self.get_container_info(container_id)
            return True
        except Exception:
            return False

    def resolve_container_type(self, container_id):
        if not isinstance(container_id, str) or "-" not in container_id:
            return ""
        return container_id.split("-", 1)[0].upper()

    def append_focus_content(self, container_id, target_file, title, content,
                             mem_id=None, round_num=0, ledger_status="applied"):
        prefix = self.resolve_container_type(container_id)
        if prefix not in SUPPORTED_FOCUS_CONTAINER_TYPES:
            raise ValueError("unsupported_container_type")
        target = self._normalize_target_file(prefix, target_file)
        content = str(content or "").strip()
        if not content:
            raise ValueError("missing_content_block")
        ledger_entry = self._container_write_ledger_entry(
            mem_id=mem_id,
            title=title or self._title_for_container(container_id),
            round_num=round_num,
            target_file=target,
            status=ledger_status,
        )

        if prefix == "FUT":
            root = PREFIX_TO_DIR[prefix]
            path = _safe_child(root, target)
            heading = title or self._title_for_container(container_id)
            self._append_markdown_block(path, heading, content)
            self._record_container_write_ledger(container_id, ledger_entry)
            self._update_future_entry(
                container_id,
                increment_entries=True,
                entry=ledger_entry,
            )
        elif prefix == "SKL":
            heading = title or self._title_for_container(container_id)
            path = _safe_child(self._get_container_dir(container_id), target)
            self._append_markdown_block(path, heading, content)
            self._record_container_write_ledger(container_id, ledger_entry)
            self._sync_instance_registry_entry(
                container_id,
                increment_entries=True,
                entry=ledger_entry,
            )
        else:
            heading = title or self._title_for_container(container_id)
            self.append_entry(container_id, heading, content, file_name=target)
            path = _safe_child(self._get_container_dir(container_id), target)
            self._record_container_write_ledger(container_id, ledger_entry)
            self._sync_instance_registry_entry(
                container_id,
                increment_entries=True,
                entry=ledger_entry,
            )
        self.refresh_master_index()
        return {"path": path, "chars_written": len(content)}

    def set_container_focus(self, container_id, focus):
        prefix = self.resolve_container_type(container_id)
        if prefix == "FUT":
            self._update_future_entry(container_id, focus=bool(focus))
            return
        try:
            meta = self.read_meta(container_id)
            meta["focus"] = bool(focus)
            self.save_meta(container_id, meta)
        except Exception:
            pass
        self._sync_instance_registry_entry(container_id, focus=bool(focus))

    def read_focus_projection(self, container_id):
        prefix = self.resolve_container_type(container_id)
        info = self.get_container_info(container_id)
        target = self._normalize_target_file(prefix, None)
        allowed_targets = sorted(FOCUS_TARGET_FILES.get(prefix, []))
        if prefix == "FUT":
            target = info.get("target_file") or target
            content = self._read_text_if_exists(_safe_child(PREFIX_TO_DIR[prefix], target))
        else:
            content = self.read_entries(container_id, file_name=target)
        ranged = apply_explicit_range(content, {})
        return {
            "container_id": container_id,
            "container_type": prefix,
            "status": info.get("status", ""),
            "title": info.get("title") or info.get("name") or container_id,
            "allowed_targets": allowed_targets,
            "default_target": target,
            "content": ranged["content"],
            "read_mode": ranged["read_mode"],
            "total_lines": ranged["total_lines"],
            "total_chars": ranged["total_chars"],
            "format_hint": "memory_container_write via visible WB focus",
        }

    def read_container_content(self, container_id, target_file=None, **range_request):
        """只读已有索引可见容器内容，不改变 WB focus。"""
        prefix = self.resolve_container_type(container_id)
        if prefix not in SUPPORTED_FOCUS_CONTAINER_TYPES:
            raise ValueError("unsupported_container_type")
        info = self.get_container_info(container_id)
        target = self._normalize_target_file(prefix, target_file)
        if prefix == "FUT":
            path = _safe_child(PREFIX_TO_DIR[prefix], target)
            content = self._read_text_if_exists(path)
        else:
            path = _safe_child(self._get_container_dir(container_id), target)
            content = self.read_entries(container_id, file_name=target)
        ranged = apply_explicit_range(content, range_kwargs_from_request(range_request))
        return {
            "container_id": container_id,
            "container_type": prefix,
            "status": info.get("status", ""),
            "title": info.get("title") or info.get("name") or container_id,
            "target_file": target,
            "path": path,
            "content": ranged["content"],
            "chars": len(ranged["content"]),
            "read_mode": ranged["read_mode"],
            "range_requested": ranged["range_requested"],
            "range_applied": ranged["range_applied"],
            "total_lines": ranged["total_lines"],
            "total_chars": ranged["total_chars"],
        }

    def refresh_master_index(self):
        containers = self._list_instance_containers()
        lines = ["# LTM 工作容器总索引", ""]
        if not containers:
            lines.append("（暂无工作容器实例）")
        for item in containers:
            title = item.get("title") or item.get("name") or ""
            status = item.get("status", "")
            path = item.get("path", "")
            focus = " [focus]" if item.get("focus") else ""
            lines.append(f"- {item.get('id')} {title} ({status}){focus} — {path}")
        _atomic_write_text(LTM_INDEX_MD, "\n".join(lines).rstrip() + "\n")
        self._refresh_skills_index()

    def _list_instance_containers(self):
        items = []
        seen = set()
        for item in self._legacy_instance_registry_entries():
            cid = item.get("id")
            if cid and cid not in seen:
                items.append(item)
                seen.add(cid)
        for prefix in ("DC", "EC"):
            for item in self._load_chain_registry(prefix).get("chains", []):
                cid = item.get("id")
                if not cid or cid in seen:
                    continue
                entry = dict(item)
                entry["prefix"] = prefix
                entry.setdefault("title", entry.get("name", cid))
                entry.setdefault("path", f"LTM/{CONTAINER_TYPES[prefix]['dir']}/{cid}/")
                items.append(entry)
                seen.add(cid)
        projects_root = PREFIX_TO_DIR["PRJ"]
        if os.path.isdir(projects_root):
            for name in sorted(os.listdir(projects_root)):
                if not name.startswith("PRJ-"):
                    continue
                reg_path = os.path.join(projects_root, name, "registry.json")
                if not os.path.isfile(reg_path):
                    continue
                reg = _load_json_or_default(reg_path, {})
                cid = reg.get("id", name)
                if cid in seen:
                    continue
                entry = dict(reg)
                entry["prefix"] = "PRJ"
                entry.setdefault("path", f"LTM/Projects/{cid}/")
                items.append(entry)
                seen.add(cid)
        for item in self._load_skills_registry().get("skills", []):
            cid = item.get("id")
            if not cid or cid in seen:
                continue
            entry = dict(item)
            entry["prefix"] = "SKL"
            entry.setdefault("title", entry.get("name", cid))
            items.append(entry)
            seen.add(cid)
        future_reg = self._load_future_registry()
        for item in future_reg.get("items", []):
            cid = item.get("id")
            if not cid or cid in seen:
                continue
            entry = dict(item)
            entry["prefix"] = "FUT"
            entry.setdefault("path", "LTM/Future/")
            items.append(entry)
            seen.add(cid)
        return items

    def _legacy_instance_registry_entries(self):
        try:
            reg = self.load_registry()
        except Exception:
            return []
        entries = []
        for item in reg.get("containers", []):
            cid = item.get("id")
            if not cid:
                continue
            entry = dict(item)
            entry.setdefault("prefix", self.resolve_container_type(cid))
            entry.setdefault("title", entry.get("name", cid))
            entries.append(entry)
        return entries

    def _normalize_prefix(self, value):
        return str(value or "").strip().upper().rstrip("-")

    def _normalize_target_file(self, prefix, target_file):
        target = os.path.basename(str(target_file or "").strip())
        if not target:
            target = DEFAULT_FOCUS_TARGET.get(prefix, "open.md")
        allowed = FOCUS_TARGET_FILES.get(prefix, frozenset())
        if target not in allowed:
            raise ValueError("invalid_target_file")
        return target

    def _read_text_if_exists(self, path):
        if not os.path.isfile(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8") as f:
                return f.read()
        except OSError as e:
            raise ReadError(path, cause=e)

    def _append_markdown_block(self, path, title, content):
        now = local_now().isoformat()
        block = f"\n## {title}\n创建时间：{now}\n\n{content}\n"
        existing = self._read_text_if_exists(path)
        _atomic_write_text(path, (existing.rstrip() + "\n" + block).strip() + "\n")

    def _container_write_ledger_entry(
            self, *, mem_id, title, round_num, target_file, status):
        try:
            round_value = int(round_num or 0)
        except (TypeError, ValueError):
            round_value = 0
        return {
            "mem_id": str(mem_id or "").strip(),
            "title": str(title or "").strip(),
            "round": round_value,
            "target_file": str(target_file or "").strip(),
            "status": str(status or "applied").strip() or "applied",
            "updated_at": local_now().isoformat(),
        }

    def _record_container_write_ledger(self, container_id, entry):
        entry = dict(entry or {})
        prefix = self.resolve_container_type(container_id)
        if prefix == "FUT":
            index_path = _safe_child(PREFIX_TO_DIR[prefix], "index.md")
        elif prefix == "SKL":
            index_path = _safe_child(
                self._get_container_dir(container_id), "changelog.md")
        else:
            index_path = _safe_child(self._get_container_dir(container_id), "index.md")
            mem_id = entry.get("mem_id")
            if mem_id:
                meta = self.read_meta(container_id)
                linked = list(meta.get("linked_memories") or [])
                if mem_id not in linked:
                    linked.append(mem_id)
                meta["linked_memories"] = linked
                self.save_meta(container_id, meta)
        line = (
            f"- mem_id={entry.get('mem_id', '')} | "
            f"title={entry.get('title', '')} | "
            f"round={entry.get('round', 0)} | "
            f"target_file={entry.get('target_file', '')} | "
            f"status={entry.get('status', '')} | "
            f"updated_at={entry.get('updated_at', '')}"
        )
        existing = self._read_text_if_exists(index_path)
        header = "# 技能变更账本\n" if prefix == "SKL" else "# 容器写入账本\n"
        if not existing.strip():
            existing = header
        elif header.strip() not in existing:
            existing = existing.rstrip() + "\n\n" + header
        _atomic_write_text(index_path, existing.rstrip() + "\n" + line + "\n")

    def _next_numeric_container_id(self, prefix):
        numbers = []
        pattern = re.compile(rf"^{re.escape(prefix)}-(\d+)$")
        for item in self.list_containers(prefix=prefix):
            match = pattern.match(str(item.get("id", "")))
            if match:
                numbers.append(int(match.group(1)))
        root = PREFIX_TO_DIR[prefix]
        if os.path.isdir(root):
            for name in os.listdir(root):
                match = pattern.match(name)
                if match:
                    numbers.append(int(match.group(1)))
        return f"{prefix}-{(max(numbers) if numbers else 0) + 1}"

    def _next_project_id(self):
        date = local_now().strftime("%Y%m%d")
        numbers = []
        pattern = re.compile(rf"^PRJ-{date}-(\d+)$")
        root = PREFIX_TO_DIR["PRJ"]
        if os.path.isdir(root):
            for name in os.listdir(root):
                match = pattern.match(name)
                if match:
                    numbers.append(int(match.group(1)))
        return f"PRJ-{date}-{(max(numbers) if numbers else 0) + 1:02d}"

    def _next_future_id(self, category):
        numbers = []
        pattern = re.compile(rf"^FUT-{re.escape(category)}-(\d+)$")
        for item in self._load_future_registry().get("items", []):
            match = pattern.match(str(item.get("id", "")))
            if match:
                numbers.append(int(match.group(1)))
        return f"FUT-{category}-{(max(numbers) if numbers else 0) + 1}"

    def _create_chain_container(self, prefix, container_id, title, round_num=0):
        cdir = self._get_container_dir(container_id)
        os.makedirs(cdir, exist_ok=True)
        for filename in ("open.md", "closed.md", "index.md"):
            path = os.path.join(cdir, filename)
            if not os.path.isfile(path):
                _atomic_write_text(path, "")
        self.save_meta(container_id, default_container_meta(container_id, prefix, title))
        entry = {
            "id": container_id,
            "title": title,
            "status": "ongoing" if prefix == "DC" else "active",
            "entries": [],
            "tags": [],
            "focus": False,
            "created_at": local_now().isoformat(),
            "updated_at": local_now().isoformat(),
            "source_round": round_num,
            "path": f"LTM/{CONTAINER_TYPES[prefix]['dir']}/{container_id}/",
        }
        if prefix == "EC":
            entry["severity"] = 3
        _atomic_write_json(os.path.join(cdir, "registry.json"), {"chains": [entry]})
        reg = self._load_chain_registry(prefix)
        reg.setdefault("chains", [])
        reg["chains"] = [item for item in reg["chains"] if item.get("id") != container_id]
        reg["chains"].append(entry)
        _atomic_write_json(self._chain_registry_path(prefix), reg)
        return cdir

    def _create_project_container(self, container_id, title):
        cdir = self._get_container_dir(container_id)
        os.makedirs(cdir, exist_ok=True)
        for filename in ("plan.md", "notes.md"):
            path = os.path.join(cdir, filename)
            if not os.path.isfile(path):
                _atomic_write_text(path, "")
        for dirname in ("materials", "drafts", "phases"):
            os.makedirs(os.path.join(cdir, dirname), exist_ok=True)
        _atomic_write_text(os.path.join(cdir, "phases", "_index.md"), "")
        meta = default_container_meta(container_id, "PRJ", title)
        self.save_meta(container_id, meta)
        now = local_now().isoformat()
        registry = {
            "id": container_id,
            "type": "project",
            "prefix": "PRJ",
            "title": title,
            "status": "active",
            "created_at": now,
            "updated_at": now,
            "entries": [],
            "tags": [],
            "focus": False,
            "path": f"LTM/Projects/{container_id}/",
            "progress": 0,
            "phase_count": 0,
            "completed_phases": 0,
            "current_phase": None,
        }
        _atomic_write_json(os.path.join(cdir, "registry.json"), registry)
        return cdir

    def _skill_container_id(self, category, name):
        raw_category = str(category or "").strip()
        raw_name = str(name or "").strip()
        category = raw_category.lower()
        name = raw_name.lower()
        if category not in SOURCE_SKILL_CATEGORIES:
            raise ValueError("invalid_skill_category")
        if raw_category != category:
            raise ValueError("invalid_skill_category")
        if raw_name != name or len(name) > 64 or not SKILL_NAME_RE.fullmatch(name):
            raise ValueError("invalid_skill_name")
        return f"SKL-{category}-{name}"

    def _create_skill_container(self, container_id, title, category, name, anchor_refs):
        cdir = self._get_container_dir(container_id)
        registry = self._load_skills_registry()
        if (
            os.path.exists(cdir)
            or any(item.get("id") == container_id for item in registry.get("skills", []))
        ):
            raise ValueError("container_already_exists")
        os.makedirs(cdir, exist_ok=False)
        _atomic_write_text(os.path.join(cdir, "card.md"), "")
        _atomic_write_text(os.path.join(cdir, "changelog.md"), "")
        now = local_now().isoformat()
        entry = {
            "id": container_id,
            "type": "skill",
            "prefix": "SKL",
            "name": str(name).strip().lower(),
            "title": title,
            "status": "active",
            "category": str(category).strip().lower(),
            "created_at": now,
            "updated_at": now,
            "entries": [],
            "tags": [],
            "focus": False,
            "linked_memories": list(anchor_refs or []),
            "path": f"LTM/Skills/{category}/{name}/",
        }
        registry.setdefault("skills", []).append(entry)
        _atomic_write_json(self._skills_registry_path(), registry)
        return cdir

    def _create_future_entry(self, container_id, title, category, anchor_refs):
        root = PREFIX_TO_DIR["FUT"]
        os.makedirs(root, exist_ok=True)
        for filename in ("objectives.md", "plans.md", "predictions.md"):
            path = os.path.join(root, filename)
            if not os.path.isfile(path):
                _atomic_write_text(path, "")
        registry = self._load_future_registry()
        now = local_now().isoformat()
        entry = {
            "id": container_id,
            "type": "future",
            "prefix": "FUT",
            "title": title,
            "status": "planned",
            "category": category,
            "target_file": f"{category}.md",
            "created_at": now,
            "updated_at": now,
            "entries": [],
            "tags": [],
            "focus": False,
            "linked_containers": list(anchor_refs or []),
            "path": "LTM/Future/",
        }
        registry.setdefault("items", [])
        registry["items"] = [
            item for item in registry.get("items", [])
            if item.get("id") != container_id
        ]
        registry["items"].append(entry)
        _atomic_write_json(self._future_registry_path(), registry)
        return root

    def _title_for_container(self, container_id):
        try:
            info = self.get_container_info(container_id)
            return info.get("title") or info.get("name") or container_id
        except Exception:
            return container_id

    def _future_category_from_target(self, target_file):
        return os.path.splitext(target_file)[0]

    def _chain_registry_path(self, prefix):
        return os.path.join(PREFIX_TO_DIR[prefix], "registry.json")

    def _load_chain_registry(self, prefix):
        default = {
            "_comment": f"{'辩证' if prefix == 'DC' else '事件'}链注册表",
            "chains": [],
        }
        return _load_json_or_default(self._chain_registry_path(prefix), default)

    def _future_registry_path(self):
        return os.path.join(PREFIX_TO_DIR["FUT"], "registry.json")

    def _load_future_registry(self):
        return _load_json_or_default(
            self._future_registry_path(),
            {"_comment": "未来注册表", "items": []},
        )

    def _skills_registry_path(self):
        return os.path.join(PREFIX_TO_DIR["SKL"], "registry.json")

    def _load_skills_registry(self):
        return _load_json_or_default(
            self._skills_registry_path(),
            {"_comment": "技能注册表", "skills": []},
        )

    def _refresh_skills_index(self):
        lines = ["# 技能索引", ""]
        skills = self._load_skills_registry().get("skills", [])
        if not skills:
            lines.append("（暂无技能容器）")
        for item in skills:
            focus = " [focus]" if item.get("focus") else ""
            lines.append(
                f"- {item.get('id')} {item.get('title') or item.get('name', '')} "
                f"({item.get('status', '')}){focus} — {item.get('path', '')}"
            )
        _atomic_write_text(
            os.path.join(PREFIX_TO_DIR["SKL"], "index.md"),
            "\n".join(lines).rstrip() + "\n",
        )

    def _update_future_entry(self, container_id, focus=None, increment_entries=False,
                             entry=None):
        registry = self._load_future_registry()
        now = local_now().isoformat()
        changed = False
        for item in registry.get("items", []):
            if item.get("id") != container_id:
                continue
            if focus is not None:
                item["focus"] = bool(focus)
            if increment_entries:
                entries = item.setdefault("entries", [])
                entries.append(dict(entry or {
                    "updated_at": now,
                    "target_file": item.get("target_file"),
                }))
            if entry and entry.get("mem_id"):
                linked = list(item.get("linked_memories") or [])
                if entry["mem_id"] not in linked:
                    linked.append(entry["mem_id"])
                item["linked_memories"] = linked
            item["updated_at"] = now
            changed = True
        if not changed:
            raise ContainerNotFoundError(f"容器不存在: {container_id}")
        _atomic_write_json(self._future_registry_path(), registry)

    def _sync_instance_registry_entry(self, container_id, focus=None,
                                      increment_entries=False, entry=None):
        prefix = self.resolve_container_type(container_id)
        now = local_now().isoformat()
        if prefix in {"DC", "EC"}:
            self._sync_chain_registry_entry(
                container_id, focus, increment_entries, now, entry=entry)
        elif prefix == "PRJ":
            self._sync_project_registry_entry(
                container_id, focus, increment_entries, now, entry=entry)
        elif prefix == "SKL":
            self._sync_skill_registry_entry(
                container_id, focus, increment_entries, now, entry=entry)

    def _sync_chain_registry_entry(
            self, container_id, focus, increment_entries, now, entry=None):
        prefix = self.resolve_container_type(container_id)
        paths = [
            self._chain_registry_path(prefix),
            os.path.join(self._get_container_dir(container_id), "registry.json"),
        ]
        for path in paths:
            registry = _load_json_or_default(path, {"chains": []})
            changed = False
            for item in registry.get("chains", []):
                if item.get("id") != container_id:
                    continue
                if focus is not None:
                    item["focus"] = bool(focus)
                if increment_entries:
                    item.setdefault("entries", []).append(dict(entry or {
                        "updated_at": now,
                    }))
                item["updated_at"] = now
                changed = True
            if changed:
                _atomic_write_json(path, registry)

    def _sync_project_registry_entry(
            self, container_id, focus, increment_entries, now, entry=None):
        path = os.path.join(self._get_container_dir(container_id), "registry.json")
        registry = _load_json_or_default(path, {})
        if focus is not None:
            registry["focus"] = bool(focus)
        if increment_entries:
            registry.setdefault("entries", []).append(dict(entry or {
                "updated_at": now,
            }))
        registry["updated_at"] = now
        _atomic_write_json(path, registry)

    def _sync_skill_registry_entry(
            self, container_id, focus, increment_entries, now, entry=None):
        registry = self._load_skills_registry()
        changed = False
        for item in registry.get("skills", []):
            if item.get("id") != container_id:
                continue
            if focus is not None:
                item["focus"] = bool(focus)
            if increment_entries:
                item.setdefault("entries", []).append(dict(entry or {
                    "updated_at": now,
                }))
            if entry and entry.get("mem_id"):
                linked = list(item.get("linked_memories") or [])
                if entry["mem_id"] not in linked:
                    linked.append(entry["mem_id"])
                item["linked_memories"] = linked
            item["updated_at"] = now
            changed = True
        if not changed:
            raise ContainerNotFoundError(f"容器不存在: {container_id}")
        _atomic_write_json(self._skills_registry_path(), registry)
