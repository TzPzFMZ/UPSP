"""
WB 调度台存储 — status.json + 三区任务物流
DDS §9.4 / §34

data 层独占 WB 文件 I/O。WB 本身只由脚本操作，LLM 只通过
装配后的上下文间接读取。
"""
import json
import os
import re
import shutil
from datetime import datetime

from constants import local_now
from data.atomic_write import atomic_write_json, atomic_write_text
from errors import ReadError, WriteError
from paths import WB_DIR


TASK_ID_RE = re.compile(r"^T-\d{8}-\d{2,}$")
ZONES = ("input", "process", "output")
GUIDE_SLOTS = ("rhythm", "work")
GUIDE_SLOT_PRIORITY = ("rhythm", "work")
RHYTHM_GUIDE_KINDS = {
    "main_axis_rhythm_guide",
    "calendar_rhythm_guide",
    "emergency_handling_guide",
    "context_pressure_rhythm_guide",
    "cache_compaction_rhythm_guide",
}


def default_workbench_status():
    """workbench/status.json 默认结构（DDS §9.4）"""
    return {
        "base": {
            "instance_id": "WB-main",
            "focus": None,
            "old_focus": None,
            "active_task": None,
            "active_guide": None,
            "active_guides": {
                "rhythm": None,
                "work": None,
            },
            "step_count": 0,
            "last_checkpoint": None,
            "pending_interrupt": None,
            "settlement": {
                "pending": False,
                "level": 0,
                "reason": None,
            },
        },
        "plus": {},
        "pro": {},
        "dlc": {},
        "mod": {},
    }


class WorkbenchStore:
    """WB status 与 input/process/output 三区任务读写管理"""

    def __init__(self, root_dir=None):
        self.root_dir = root_dir or WB_DIR
        self.status_path = os.path.join(self.root_dir, "status.json")
        self.source_evidence_path = os.path.join(
            self.root_dir,
            "source_evidence.jsonl",
        )
        self.zone_dirs = {
            "input": os.path.join(self.root_dir, "input"),
            "process": os.path.join(self.root_dir, "process"),
            "output": os.path.join(self.root_dir, "output"),
        }
        self.guides_dir = os.path.join(self.root_dir, "guides")

    # ============================================================
    # 初始化与 status.json
    # ============================================================

    def init_if_missing(self):
        """确保 WB 三区目录和 status.json 存在"""
        for path in self.zone_dirs.values():
            os.makedirs(path, exist_ok=True)
        if not os.path.isfile(self.status_path):
            self.save_status(default_workbench_status())
            return True
        return False

    def load_status(self):
        if not os.path.isfile(self.status_path):
            return default_workbench_status()
        try:
            with open(self.status_path, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(self.status_path, cause=e)

    def save_status(self, data):
        os.makedirs(os.path.dirname(self.status_path), exist_ok=True)
        self._atomic_write_json(self.status_path, data)

    def get(self, dotpath, default=None):
        data = self.load_status()
        cur = data
        for key in dotpath.split("."):
            if isinstance(cur, dict) and key in cur:
                cur = cur[key]
            else:
                return default
        return cur

    def set(self, dotpath, value):
        data = self.load_status()
        cur = data
        keys = dotpath.split(".")
        for key in keys[:-1]:
            if key not in cur or not isinstance(cur[key], dict):
                cur[key] = {}
            cur = cur[key]
        cur[keys[-1]] = value
        self.save_status(data)

    def mount_focus(self, container_id):
        data = self.load_status()
        base = data.setdefault("base", {})
        current = base.get("focus")
        if current and current != container_id:
            base["old_focus"] = current
        base["focus"] = container_id
        self.save_status(data)

    def unmount_focus(self, container_id=None):
        data = self.load_status()
        base = data.setdefault("base", {})
        current = base.get("focus")
        if current and (container_id is None or current == container_id):
            base["old_focus"] = current
            base["focus"] = None
        self.save_status(data)

    def restore_focus(self):
        data = self.load_status()
        base = data.setdefault("base", {})
        old_focus = base.get("old_focus")
        if old_focus:
            base["focus"] = old_focus
            base["old_focus"] = None
        self.save_status(data)
        return old_focus

    # ============================================================
    # 任务物流
    # ============================================================


    def move_to_output(self, task_id, result, progress=100):
        """process → output，写 result.md 并清空 active_task"""
        return self._move_task(
            task_id, "process", "output",
            progress=progress,
            content_name="result.md",
            content=result,
            active_task=None,
        )

    def load_manifest(self, task_id, zone=None):
        task_dir = self._find_task_dir(task_id, zone=zone)
        path = os.path.join(task_dir, "manifest.json")
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(path, cause=e)

    # ============================================================
    # Guide ledger
    # ============================================================

    def save_guide(self, guide, active=False):
        guide = dict(guide or {})
        guide_id = str(guide.get("guide_id") or "").strip()
        if not guide_id:
            raise ValueError("guide_id required")
        guide_dir = self._guide_dir(guide_id)
        os.makedirs(guide_dir, exist_ok=True)
        self._atomic_write_json(os.path.join(guide_dir, "guide.json"), guide)
        if active:
            self.set_active_guide_slot(self.guide_slot_for(guide), guide_id)
        return guide_id

    def load_guide(self, guide_id):
        path = os.path.join(self._guide_dir(guide_id), "guide.json")
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(path, cause=e)

    def load_active_guide(self):
        guide_id = self.current_active_guide_id()
        if not guide_id:
            return None
        return self.load_guide(guide_id)

    def guide_slot_for(self, guide):
        guide = guide if isinstance(guide, dict) else {}
        explicit = str(
            guide.get("guide_slot")
            or guide.get("slot")
            or guide.get("category")
            or ""
        ).strip()
        if explicit in GUIDE_SLOTS:
            return explicit
        kind = str(guide.get("kind") or "").strip()
        if kind in RHYTHM_GUIDE_KINDS:
            return "rhythm"
        return "work"

    def active_guide_slots(self):
        data = self.load_status()
        base = data.setdefault("base", {})
        return self._active_guide_slots_from_base(base)

    def current_active_guide_id(self):
        data = self.load_status()
        base = data.setdefault("base", {})
        slots = self._active_guide_slots_from_base(base)
        current = self._current_active_guide_id_from_slots(slots)
        if current:
            return current
        legacy = str(base.get("active_guide") or "").strip()
        return legacy or None

    def set_active_guide_slot(self, slot, guide_id):
        slot = str(slot or "").strip()
        if slot not in GUIDE_SLOTS:
            raise ValueError(f"unknown guide slot: {slot}")
        guide_id = str(guide_id or "").strip()
        if not guide_id:
            raise ValueError("guide_id required")
        data = self.load_status()
        base = data.setdefault("base", {})
        slots = self._active_guide_slots_from_base(base)
        slots[slot] = guide_id
        base["active_guides"] = slots
        self._sync_active_guide_projection(base)
        self.save_status(data)
        return guide_id


    def clear_active_guide(self, guide_id=None):
        expected = str(guide_id or "").strip()
        data = self.load_status()
        base = data.setdefault("base", {})
        slots = self._active_guide_slots_from_base(base)
        for slot in GUIDE_SLOTS:
            current = str(slots.get(slot) or "").strip()
            if current and (not expected or current == expected):
                slots[slot] = None
        if expected and str(base.get("active_guide") or "").strip() == expected:
            base["active_guide"] = None
        base["active_guides"] = slots
        self._sync_active_guide_projection(base)
        self.save_status(data)
        return base.get("active_guide")

    def append_guide_ledger(self, guide_id, entry):
        guide_id = str(guide_id or "").strip()
        if not guide_id:
            raise ValueError("guide_id required")
        guide_dir = self._guide_dir(guide_id)
        os.makedirs(guide_dir, exist_ok=True)
        ledger_path = os.path.join(guide_dir, "ledger.jsonl")
        payload = dict(entry or {})
        payload.setdefault("created_at", local_now().isoformat())
        try:
            with open(ledger_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except OSError as e:
            raise WriteError(ledger_path, cause=e)
        return payload

    def create_task_guide_task(self, task_title, task_goal="", guide=None):
        self.init_if_missing()
        task_id = self._next_task_id()
        self._validate_task_id(task_id)
        task_dir = self._task_dir("process", task_id)
        os.makedirs(task_dir, exist_ok=False)
        manifest = {
            "task_id": task_id,
            "title": task_title,
            "weight": 5,
            "priority": 3,
            "dispatch": "guide",
            "keywords": [],
            "target": None,
            "source": "guide_submit",
            "created_at": local_now().isoformat(),
            "status": "process",
            "progress": 0,
        }
        guide_doc = dict(guide or {})
        guide_doc.setdefault("task_id", task_id)
        guide_doc.setdefault("task_title", task_title)
        guide_doc.setdefault("task_goal", task_goal or "")
        self._atomic_write_json(os.path.join(task_dir, "manifest.json"), manifest)
        self._atomic_write_text(os.path.join(task_dir, "payload.md"), task_goal or "")
        self._atomic_write_json(os.path.join(task_dir, "task_guide.json"), guide_doc)
        self._atomic_write_text(os.path.join(task_dir, "acceptance_ledger.jsonl"), "")
        self.set("base.active_task", task_id)
        return task_id

    def load_task_guide(self, task_id):
        try:
            task_dir = self._find_task_dir(task_id, zone="process")
        except FileNotFoundError:
            task_dir = self._find_task_dir(task_id, zone="output")
        path = os.path.join(task_dir, "task_guide.json")
        try:
            with open(path, "r", encoding="utf-8-sig") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(path, cause=e)

    def complete_task_guide_task(self, task_id, result="", guide_id=None):
        self._validate_task_id(task_id)
        guide_id = str(guide_id or f"task:{task_id}").strip()
        task_dir = self._find_task_dir(task_id, zone="process")
        guide_dir = self._guide_dir(guide_id)
        if os.path.isdir(guide_dir):
            record_dir = os.path.join(task_dir, "guide_record")
            shutil.copytree(guide_dir, record_dir, dirs_exist_ok=True)
        manifest = self.move_to_output(
            task_id,
            result or "Task guide completed and accepted.",
            progress=100,
        )
        if os.path.isdir(guide_dir):
            shutil.rmtree(guide_dir)
        self.clear_active_guide(guide_id)
        return manifest

    def save_task_guide(self, task_id, guide):
        task_dir = self._find_task_dir(task_id, zone="process")
        path = os.path.join(task_dir, "task_guide.json")
        self._atomic_write_json(path, dict(guide or {}))
        return path

    def append_task_acceptance_ledger(self, task_id, entry):
        task_dir = self._find_task_dir(task_id, zone="process")
        ledger_path = os.path.join(task_dir, "acceptance_ledger.jsonl")
        payload = dict(entry or {})
        payload.setdefault("created_at", local_now().isoformat())
        try:
            with open(ledger_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except OSError as e:
            raise WriteError(ledger_path, cause=e)
        return payload

    # ============================================================
    # Source evidence ledger
    # ============================================================

    def append_source_read_evidence(self, entry):
        """记录跨轮可复用的来源已读证据；只写元数据，不写正文。"""
        payload = self._normalize_source_read_evidence(entry)
        if not payload:
            return None
        os.makedirs(self.root_dir, exist_ok=True)
        try:
            with open(self.source_evidence_path, "a", encoding="utf-8") as f:
                f.write(json.dumps(payload, ensure_ascii=False) + "\n")
        except OSError as e:
            raise WriteError(self.source_evidence_path, cause=e)
        return payload

    def load_source_read_evidence(self):
        if not os.path.isfile(self.source_evidence_path):
            return []
        entries = []
        try:
            with open(self.source_evidence_path, "r", encoding="utf-8-sig") as f:
                for line in f:
                    text = line.strip()
                    if not text:
                        continue
                    try:
                        item = json.loads(text)
                    except json.JSONDecodeError:
                        continue
                    if isinstance(item, dict):
                        entries.append(item)
        except OSError as e:
            raise ReadError(self.source_evidence_path, cause=e)
        return entries

    # ============================================================
    # 内部工具
    # ============================================================

    def init_dirs_only(self):
        for path in self.zone_dirs.values():
            os.makedirs(path, exist_ok=True)
        os.makedirs(self.guides_dir, exist_ok=True)

    def _move_task(self, task_id, src_zone, dst_zone, progress,
                   content_name=None, content=None, active_task=None):
        self._validate_task_id(task_id)
        src = self._task_dir(src_zone, task_id)
        dst = self._task_dir(dst_zone, task_id)
        if not os.path.isdir(src):
            raise FileNotFoundError(src)
        if os.path.exists(dst):
            raise ValueError(f"WB 任务目标目录已存在: {dst}")

        try:
            os.rename(src, dst)
        except OSError as e:
            raise WriteError(dst, cause=e)

        manifest = self.load_manifest(task_id, zone=dst_zone)
        manifest["status"] = dst_zone
        manifest["progress"] = int(progress)
        self._atomic_write_json(os.path.join(dst, "manifest.json"), manifest)
        if content_name and content is not None:
            self._atomic_write_text(os.path.join(dst, content_name), content)
        self.set("base.active_task", active_task)
        return manifest

    def _next_task_id(self):
        today = local_now().strftime("%Y%m%d")
        prefix = f"T-{today}-"
        max_seq = 0
        for zone in ZONES:
            zdir = self.zone_dirs[zone]
            if not os.path.isdir(zdir):
                continue
            for name in os.listdir(zdir):
                if name.startswith(prefix):
                    try:
                        max_seq = max(max_seq, int(name.rsplit("-", 1)[1]))
                    except ValueError:
                        pass
        guide_prefix = f"task__colon__{prefix}"
        if os.path.isdir(self.guides_dir):
            for name in os.listdir(self.guides_dir):
                if not name.startswith(guide_prefix):
                    continue
                try:
                    max_seq = max(max_seq, int(name.rsplit("-", 1)[1]))
                except ValueError:
                    pass
        return f"{prefix}{max_seq + 1:02d}"

    def _task_exists(self, task_id):
        return any(os.path.isdir(self._task_dir(zone, task_id)) for zone in ZONES)

    def _find_task_dir(self, task_id, zone=None):
        self._validate_task_id(task_id)
        zones = (zone,) if zone else ZONES
        for z in zones:
            if z not in ZONES:
                raise ValueError(f"未知 WB 分区: {z}")
            path = self._task_dir(z, task_id)
            if os.path.isdir(path):
                return path
        raise FileNotFoundError(f"WB 任务不存在: {task_id}")

    def _task_dir(self, zone, task_id):
        if zone not in ZONES:
            raise ValueError(f"未知 WB 分区: {zone}")
        self._validate_task_id(task_id)
        return os.path.join(self.zone_dirs[zone], task_id)

    def _guide_dir(self, guide_id):
        guide_id = str(guide_id or "").strip()
        if (
                not guide_id
                or os.path.isabs(guide_id)
                or "/" in guide_id
                or "\\" in guide_id
                or ".." in guide_id):
            raise ValueError(f"invalid guide_id: {guide_id}")
        safe_name = guide_id.replace(":", "__colon__")
        return os.path.join(self.guides_dir, safe_name)

    @staticmethod
    def _active_guide_slots_from_base(base):
        slots = base.get("active_guides")
        if not isinstance(slots, dict):
            slots = {}
        normalized = {}
        for slot in GUIDE_SLOTS:
            value = str(slots.get(slot) or "").strip()
            normalized[slot] = value or None
        legacy = str(base.get("active_guide") or "").strip()
        if legacy and all(not normalized.get(slot) for slot in GUIDE_SLOTS):
            normalized["work"] = legacy
        return normalized

    @staticmethod
    def _current_active_guide_id_from_slots(slots):
        for slot in GUIDE_SLOT_PRIORITY:
            guide_id = str((slots or {}).get(slot) or "").strip()
            if guide_id:
                return guide_id
        return None

    def _sync_active_guide_projection(self, base):
        slots = self._active_guide_slots_from_base(base)
        base["active_guides"] = slots
        base["active_guide"] = self._current_active_guide_id_from_slots(slots)

    @staticmethod
    def _validate_task_id(task_id):
        if not isinstance(task_id, str) or not TASK_ID_RE.match(task_id):
            raise ValueError(f"非法 WB 任务ID: {task_id}")

    @staticmethod
    def _normalize_source_read_evidence(entry):
        if not isinstance(entry, dict):
            return None
        tool_id = str(entry.get("tool_id") or "").strip()
        if tool_id not in {"file_read", "web_fetch"}:
            return None
        status = str(entry.get("status") or "").strip().lower()
        if status not in {"ok", "success", "accepted", "applied"}:
            return None
        source_ref = str(entry.get("source_ref") or "").strip()
        path = ""
        url = ""
        if tool_id == "file_read":
            path = str(entry.get("path") or entry.get("file_path") or "").strip()
            source_ref = source_ref or path
            if not source_ref:
                return None
        else:
            url = str(entry.get("url") or entry.get("source_url") or "").strip()
            source_ref = source_ref or url
            if not source_ref:
                return None
        payload = {
            "tool_id": tool_id,
            "status": status,
            "source_ref": source_ref,
            "created_at": local_now().isoformat(),
        }
        if path:
            payload["path"] = path
        if url:
            payload["url"] = url
        for key in ("round", "iteration", "task_root"):
            value = entry.get(key)
            if value is None or str(value).strip() == "":
                continue
            if key in {"round", "iteration"}:
                try:
                    payload[key] = int(value)
                except (TypeError, ValueError):
                    payload[key] = str(value).strip()
            else:
                payload[key] = str(value).strip()
        return payload

    @staticmethod
    def _atomic_write_json(path, data):
        atomic_write_json(path, data)

    @staticmethod
    def _atomic_write_text(path, content):
        atomic_write_text(path, content)
