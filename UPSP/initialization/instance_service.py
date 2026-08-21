"""Minimal multi-persona and single-active branch storage operations."""

from __future__ import annotations

import json
import os
import re
import secrets
import shutil
import threading
from datetime import datetime
from pathlib import Path

from .persona_initializer import PersonaInitializer, load_preset
from .windows_data import (
    ACTIVE_INSTANCE_FILENAME,
    ACTIVE_INSTANCE_SCHEMA,
    INSTANCE_MANIFEST_FILENAME,
    INSTANCE_MANIFEST_SCHEMA,
    META_INSTANCE_ID,
    ActiveLayout,
    DataRootError,
    _atomic_write_json,
    generate_pid,
    validate_instance_layout,
    validate_instance_id,
    validate_pid,
)


class InstanceServiceError(RuntimeError):
    pass


def generate_instance_id(now: datetime | None = None) -> str:
    stamp = (now or datetime.now().astimezone()).strftime("%Y%m%d-%H%M%S")
    return f"I{stamp}-{secrets.token_hex(2).upper()}"


def _now() -> str:
    return datetime.now().astimezone().isoformat()


def _safe_label(value: object) -> str:
    label = str(value or "").strip()
    if not 1 <= len(label) <= 40 or any(ord(char) < 32 for char in label):
        raise InstanceServiceError("instance_label_invalid")
    return label


def _read_json(path: Path) -> dict:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise InstanceServiceError("instance_data_invalid") from exc
    if not isinstance(value, dict):
        raise InstanceServiceError("instance_data_invalid")
    return value


def _reject_symlinks(root: Path) -> None:
    if root.is_symlink() or any(path.is_symlink() for path in root.rglob("*")):
        raise InstanceServiceError("instance_symlink_unsupported")


def _identity(core_path: Path) -> dict:
    try:
        text = core_path.read_text(encoding="utf-8")
    except (OSError, UnicodeError):
        return {}

    def field(name: str) -> str:
        match = re.search(rf"(?m)^{re.escape(name)}：\s*(.*?)\s*$", text)
        return match.group(1).strip() if match else ""

    return {
        "name_zh": field("中文名"),
        "name_en": field("英文名"),
        "abbreviation": field("缩写"),
    }


class InstanceService:
    """Directory scan plus atomic active-manifest updates; no merge registry."""

    def __init__(self, layout: ActiveLayout):
        self.layout = layout
        self._lock = threading.Lock()

    @property
    def manifest_path(self) -> Path:
        return self.layout.data_root / ACTIVE_INSTANCE_FILENAME

    def _write_active(self, pid: str, instance_id: str) -> None:
        _atomic_write_json(
            self.manifest_path,
            {
                "schema_version": ACTIVE_INSTANCE_SCHEMA,
                "pid": validate_pid(pid),
                "instance_id": validate_instance_id(instance_id),
            },
        )

    @staticmethod
    def _manifest(
        path: Path, *, expected_pid: str | None = None,
        expected_instance_id: str | None = None,
    ) -> dict:
        value = _read_json(path / INSTANCE_MANIFEST_FILENAME)
        if value.get("schema_version") != INSTANCE_MANIFEST_SCHEMA:
            raise InstanceServiceError("instance_manifest_invalid")
        pid = validate_pid(value.get("pid"))
        instance_id = validate_instance_id(value.get("instance_id"))
        kind = value.get("kind")
        source_round = value.get("source_round", 0)
        if (
            kind != ("meta" if instance_id == META_INSTANCE_ID else "branch")
            or isinstance(source_round, bool)
            or not isinstance(source_round, int)
            or source_round < 0
            or (expected_pid is not None and pid != expected_pid)
            or (
                expected_instance_id is not None
                and instance_id != expected_instance_id
            )
        ):
            raise InstanceServiceError("instance_manifest_invalid")
        if instance_id != META_INSTANCE_ID:
            _safe_label(value.get("label"))
            validate_instance_id(value.get("source_instance_id"))
            if value.get("mode") not in {"new", "fork"}:
                raise InstanceServiceError("instance_manifest_invalid")
        return value

    def _instance_root(self, pid: str, instance_id: str, *, archived=False) -> Path:
        pid = validate_pid(pid)
        instance_id = validate_instance_id(instance_id)
        parent = self.layout.personas_root / pid
        if archived:
            parent /= "_archive"
        target = parent / instance_id
        if target.resolve().parent != parent.resolve():
            raise InstanceServiceError("instance_path_invalid")
        return target

    def list_all(self) -> dict:
        personas = []
        root = self.layout.personas_root
        if root.is_dir():
            for pid_root in sorted(path for path in root.iterdir() if path.is_dir()):
                try:
                    pid = validate_pid(pid_root.name)
                except DataRootError:
                    continue
                meta = pid_root / META_INSTANCE_ID
                if not meta.is_dir():
                    raise InstanceServiceError("instance_manifest_invalid")
                instances = []
                for path in sorted(pid_root.iterdir()):
                    if not path.is_dir() or path.name == "_archive":
                        continue
                    try:
                        validate_instance_id(path.name)
                    except DataRootError:
                        continue
                    item = self._manifest(
                        path, expected_pid=pid,
                        expected_instance_id=path.name,
                    )
                    instances.append(self._project_instance(item, archived=False))
                archive_root = pid_root / "_archive"
                if archive_root.is_dir():
                    for path in sorted(archive_root.iterdir()):
                        if not path.is_dir():
                            continue
                        try:
                            validate_instance_id(path.name)
                        except DataRootError:
                            continue
                        item = self._manifest(
                            path, expected_pid=pid,
                            expected_instance_id=path.name,
                        )
                        instances.append(self._project_instance(item, archived=True))
                personas.append({
                    "pid": pid,
                    "identity": _identity(meta / "persona" / "core.md"),
                    "instances": instances,
                })
        return {
            "schema_version": "seed_gui_persona_catalog.v1",
            "active": {
                "pid": self.layout.pid,
                "instance_id": self.layout.instance_id,
            },
            "personas": personas,
        }

    @staticmethod
    def _project_instance(item: dict, *, archived: bool) -> dict:
        return {
            "instance_id": str(item.get("instance_id") or ""),
            "kind": str(item.get("kind") or "branch"),
            "label": str(item.get("label") or item.get("instance_id") or ""),
            "source_instance_id": str(item.get("source_instance_id") or ""),
            "source_round": int(item.get("source_round") or 0),
            "created_at": str(item.get("created_at") or ""),
            "archived": archived,
        }

    def activate(self, pid: object, instance_id: object) -> dict:
        with self._lock:
            pid = validate_pid(pid)
            instance_id = validate_instance_id(instance_id)
            target = self._instance_root(pid, instance_id)
            self._manifest(
                target, expected_pid=pid, expected_instance_id=instance_id
            )
            validate_instance_layout(self.layout, pid, instance_id)
            changed = (pid, instance_id) != (
                self.layout.pid, self.layout.instance_id
            )
            if changed:
                self._write_active(pid, instance_id)
            return {
                "schema_version": "seed_gui_instance_mutation_receipt.v1",
                "status": "activated" if changed else "already_active",
                "pid": pid,
                "instance_id": instance_id,
                "restart_required": changed,
            }

    def create_persona(
        self,
        *,
        mode: object,
        preset_id: object,
        profile: object,
        model_stamp: dict,
    ) -> dict:
        with self._lock:
            pid = generate_pid()
            pid_root = self.layout.personas_root / pid
            if pid_root.exists():
                raise InstanceServiceError("persona_create_conflict")
            meta_root = pid_root / META_INSTANCE_ID
            try:
                shutil.copytree(
                    self.layout.initialization_root / "os_template",
                    meta_root,
                    copy_function=shutil.copyfile,
                )
                (meta_root / "files").mkdir()
                (meta_root / "trash").mkdir()
                active_routing = self.layout.config_dir / "model_routing.json"
                if active_routing.is_file():
                    shutil.copyfile(
                        active_routing, meta_root / "config" / "model_routing.json"
                    )
                initializer = PersonaInitializer(
                    meta_root / "persona",
                    self.layout.initialization_root / "persona_template",
                    self.layout.initialization_root / "persona_presets",
                    pid=pid,
                )
                if mode == "preset":
                    if preset_id != "alyosha" or profile is not None:
                        raise InstanceServiceError("persona_request_invalid")
                    selected = load_preset(initializer.preset_dir, "alyosha")
                elif mode == "custom":
                    if preset_id is not None:
                        raise InstanceServiceError("persona_request_invalid")
                    selected = profile
                else:
                    raise InstanceServiceError("persona_request_invalid")
                receipt = initializer.create(selected, model_stamp)
                _atomic_write_json(
                    meta_root / INSTANCE_MANIFEST_FILENAME,
                    {
                        "schema_version": INSTANCE_MANIFEST_SCHEMA,
                        "pid": pid,
                        "instance_id": META_INSTANCE_ID,
                        "kind": "meta",
                        "label": "meta",
                        "created_at": _now(),
                    },
                )
                validate_instance_layout(self.layout, pid, META_INSTANCE_ID)
                self._write_active(pid, META_INSTANCE_ID)
            except Exception:
                if pid_root.is_dir() and pid_root.resolve().parent == self.layout.personas_root.resolve():
                    shutil.rmtree(pid_root)
                raise
            return {
                "schema_version": "seed_gui_instance_mutation_receipt.v1",
                "status": "persona_created",
                "persona": receipt,
                "pid": pid,
                "instance_id": META_INSTANCE_ID,
                "restart_required": True,
            }

    def create_branch(self, *, mode: object, label: object, source_instance_id: object = None) -> dict:
        with self._lock:
            mode = str(mode or "")
            if mode not in {"new", "fork"}:
                raise InstanceServiceError("branch_mode_invalid")
            label = _safe_label(label)
            source_id = (
                META_INSTANCE_ID
                if mode == "new"
                else validate_instance_id(source_instance_id or self.layout.instance_id)
            )
            source = self._instance_root(self.layout.pid, source_id)
            self._manifest(
                source, expected_pid=self.layout.pid,
                expected_instance_id=source_id,
            )
            _reject_symlinks(source)
            source_round = (
                self._settled_head(source)
                if mode == "fork"
                else self._state_round(source)
            )
            instance_id = generate_instance_id()
            target = self._instance_root(self.layout.pid, instance_id)
            try:
                source_persona = source / "persona"

                def ignore_shared(path: str, _names: list[str]) -> set[str]:
                    current = Path(path)
                    if current == source_persona:
                        return {"core.md", "rules", "docs"}
                    if current == source_persona / "LTM":
                        return {"Memory"}
                    return set()

                shutil.copytree(
                    source, target, copy_function=shutil.copyfile,
                    ignore=ignore_shared,
                )
                if mode == "new":
                    self._reset_new_branch(target)
                _atomic_write_json(
                    target / INSTANCE_MANIFEST_FILENAME,
                    {
                        "schema_version": INSTANCE_MANIFEST_SCHEMA,
                        "pid": self.layout.pid,
                        "instance_id": instance_id,
                        "kind": "branch",
                        "label": label,
                        "mode": mode,
                        "source_instance_id": source_id,
                        "source_round": source_round,
                        "created_at": _now(),
                    },
                )
                validate_instance_layout(self.layout, self.layout.pid, instance_id)
                self._write_active(self.layout.pid, instance_id)
            except Exception:
                if target.is_dir() and target.resolve().parent == self.layout.pid_root.resolve():
                    shutil.rmtree(target)
                raise
            return {
                "schema_version": "seed_gui_instance_mutation_receipt.v1",
                "status": "branch_created",
                "pid": self.layout.pid,
                "instance_id": instance_id,
                "mode": mode,
                "source_instance_id": source_id,
                "source_round": source_round,
                "restart_required": True,
            }

    def _reset_new_branch(self, target: Path) -> None:
        stm = target / "persona" / "STM"
        if stm.is_dir():
            shutil.rmtree(stm)
        shutil.copytree(
            self.layout.initialization_root / "persona_template" / "STM",
            stm,
            copy_function=shutil.copyfile,
        )
        for name in ("files", "trash"):
            path = target / name
            if path.is_dir():
                shutil.rmtree(path)
            path.mkdir()

    @staticmethod
    def _state_round(root: Path) -> int:
        state = _read_json(root / "persona" / "state.json")
        base = state.get("base") if isinstance(state.get("base"), dict) else {}
        meta = base.get("meta") if isinstance(base.get("meta"), dict) else {}
        value = meta.get("total_round", 0)
        if isinstance(value, bool) or not isinstance(value, int) or value < 0:
            raise InstanceServiceError("instance_state_invalid")
        return value

    def _settled_head(self, root: Path) -> int:
        round_num = self._state_round(root)
        if round_num == 0:
            return 0
        path = root / "persona" / "STM" / "context" / "round" / f"round_{round_num}.jsonl"
        try:
            events = [
                json.loads(line)
                for line in path.read_text(encoding="utf-8").splitlines()
                if line.strip()
            ]
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise InstanceServiceError("fork_source_not_settled") from exc
        if (
            not events
            or not isinstance(events[-1], dict)
            or events[-1].get("event_type") != "round_closed"
        ):
            raise InstanceServiceError("fork_source_not_settled")
        return round_num

    def archive(self, instance_id: object) -> dict:
        with self._lock:
            instance_id = validate_instance_id(instance_id)
            if instance_id == META_INSTANCE_ID:
                raise InstanceServiceError("meta_instance_cannot_archive")
            source = self._instance_root(self.layout.pid, instance_id)
            self._manifest(
                source, expected_pid=self.layout.pid,
                expected_instance_id=instance_id,
            )
            target = self._instance_root(self.layout.pid, instance_id, archived=True)
            if target.exists():
                raise InstanceServiceError("instance_archive_conflict")
            target.parent.mkdir(exist_ok=True)
            active = instance_id == self.layout.instance_id
            if active:
                self._write_active(self.layout.pid, META_INSTANCE_ID)
            try:
                os.replace(source, target)
            except Exception:
                if active:
                    self._write_active(self.layout.pid, instance_id)
                raise
            return {
                "schema_version": "seed_gui_instance_mutation_receipt.v1",
                "status": "archived",
                "pid": self.layout.pid,
                "instance_id": instance_id,
                "restart_required": active,
            }

    def restore(self, instance_id: object) -> dict:
        with self._lock:
            instance_id = validate_instance_id(instance_id)
            if instance_id == META_INSTANCE_ID:
                raise InstanceServiceError("instance_restore_invalid")
            source = self._instance_root(self.layout.pid, instance_id, archived=True)
            self._manifest(
                source, expected_pid=self.layout.pid,
                expected_instance_id=instance_id,
            )
            target = self._instance_root(self.layout.pid, instance_id)
            if target.exists():
                raise InstanceServiceError("instance_restore_conflict")
            os.replace(source, target)
            try:
                validate_instance_layout(self.layout, self.layout.pid, instance_id)
            except Exception:
                os.replace(target, source)
                raise
            return {
                "schema_version": "seed_gui_instance_mutation_receipt.v1",
                "status": "restored",
                "pid": self.layout.pid,
                "instance_id": instance_id,
                "restart_required": False,
            }
