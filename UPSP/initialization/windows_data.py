"""Windows data-root and active-instance ownership.

This module is deliberately stdlib-only.  It may be imported before the OS
runtime modules, so it must not import ``paths`` or any ConfigStore consumer.
"""

from __future__ import annotations

import ctypes
import hashlib
import json
import os
import re
import secrets
import shutil
import sys
import uuid
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Mapping


DATA_ROOT_ENV = "UPSP_DATA_ROOT"
LOCAL_STATE_ROOT_ENV = "UPSP_LOCAL_STATE_ROOT"
RETIRED_PERSONA_ROOT_ENV = "UPSP_PERSONA_DIR"
ACTIVE_INSTANCE_SCHEMA = "upsp_active_instance.v1"
ACTIVE_INSTANCE_FILENAME = "active_instance.json"
PRODUCT_DIRNAME = "UPSP"
PID_RE = re.compile(r"^B\d{8}-\d{6}-[0-9A-F]{4}-[0-9A-F]{2}$")
REQUIRED_OS_CONFIG_FILES = frozenset(
    {
        "system.json",
        "model_routing.json",
        "memory.json",
        "media.json",
        "relation.json",
        "organ_topology.json",
        "context/permanent.json",
        "context/periodic.json",
        "context/lately.json",
        "context/high_freq.json",
        "context/now.json",
        "context/statusbar.json",
        "context/popup.json",
    }
)

KNOWN_FOLDER_DOCUMENTS = uuid.UUID("FDD39AD0-238F-46AF-ADB4-6C85480369C7")
KNOWN_FOLDER_LOCAL_APP_DATA = uuid.UUID("F1B32785-6FBA-4FCF-9D55-7B8E7F157091")


class DataRootError(RuntimeError):
    """A stable, user-safe path or active-instance failure."""


class _GUID(ctypes.Structure):
    _fields_ = (
        ("Data1", ctypes.c_ulong),
        ("Data2", ctypes.c_ushort),
        ("Data3", ctypes.c_ushort),
        ("Data4", ctypes.c_ubyte * 8),
    )

    @classmethod
    def from_uuid(cls, value: uuid.UUID) -> "_GUID":
        fields = value.fields
        return cls(
            fields[0],
            fields[1],
            fields[2],
            (ctypes.c_ubyte * 8)(
                fields[3],
                fields[4],
                *fields[5].to_bytes(6, "big"),
            ),
        )


@dataclass(frozen=True)
class ActiveLayout:
    program_upsp_root: Path
    program_os_root: Path
    initialization_root: Path
    data_root: Path
    local_state_root: Path
    manifest_path: Path
    personas_root: Path
    pid: str
    instance_root: Path
    os_root: Path
    persona_dir: Path
    config_dir: Path
    files_dir: Path
    trash_dir: Path
    global_config_dir: Path
    audit_cache_dir: Path


def windows_known_folder(folder_id: uuid.UUID) -> Path:
    """Resolve one Windows known folder, including shell redirection."""
    if sys.platform != "win32":
        raise DataRootError("windows_known_folder_unavailable")
    shell32 = ctypes.windll.shell32
    ole32 = ctypes.windll.ole32
    shell32.SHGetKnownFolderPath.argtypes = (
        ctypes.POINTER(_GUID),
        ctypes.c_ulong,
        ctypes.c_void_p,
        ctypes.POINTER(ctypes.c_wchar_p),
    )
    shell32.SHGetKnownFolderPath.restype = ctypes.c_long
    ole32.CoTaskMemFree.argtypes = (ctypes.c_void_p,)
    pointer = ctypes.c_wchar_p()
    guid = _GUID.from_uuid(folder_id)
    result = shell32.SHGetKnownFolderPath(
        ctypes.byref(guid),
        0,
        None,
        ctypes.byref(pointer),
    )
    if result != 0 or not pointer.value:
        raise DataRootError("windows_known_folder_unavailable")
    try:
        return Path(pointer.value).resolve()
    finally:
        ole32.CoTaskMemFree(ctypes.cast(pointer, ctypes.c_void_p))


def generate_pid(now: datetime | None = None) -> str:
    """Allocate one stable persona identifier with an embedded checksum."""
    stamp = (now or datetime.now().astimezone()).strftime("%Y%m%d-%H%M%S")
    prefix = f"B{stamp}-{secrets.token_hex(2).upper()}"
    check = hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:2].upper()
    return f"{prefix}-{check}"


def validate_pid(value: object) -> str:
    pid = str(value or "").strip()
    if not PID_RE.fullmatch(pid):
        raise DataRootError("active_instance_pid_invalid")
    prefix, check = pid.rsplit("-", 1)
    expected = hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:2].upper()
    if check != expected:
        raise DataRootError("active_instance_pid_invalid")
    return pid


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
        return True
    except ValueError:
        return False


def _canonical_override(
    raw: str,
    *,
    field: str,
    program_upsp_root: Path,
) -> Path:
    expanded = Path(os.path.expandvars(os.path.expanduser(raw)))
    if not expanded.is_absolute():
        raise DataRootError(f"{field}_must_be_absolute")
    resolved = expanded.resolve()
    anchor = Path(resolved.anchor)
    if resolved == anchor:
        raise DataRootError(f"{field}_must_not_be_volume_root")
    if _is_within(resolved, program_upsp_root) or _is_within(
        program_upsp_root, resolved
    ):
        raise DataRootError(f"{field}_must_not_overlap_program")
    return resolved


def resolve_storage_roots(
    program_upsp_root: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
    known_folder: Callable[[uuid.UUID], Path] = windows_known_folder,
) -> tuple[Path, Path]:
    """Resolve durable Documents data and LocalAppData state roots."""
    env = os.environ if environ is None else environ
    program_root = Path(program_upsp_root).resolve()
    if str(env.get(RETIRED_PERSONA_ROOT_ENV) or "").strip():
        raise DataRootError("legacy_persona_dir_override_retired")

    data_raw = str(env.get(DATA_ROOT_ENV) or "").strip()
    local_raw = str(env.get(LOCAL_STATE_ROOT_ENV) or "").strip()
    data_candidate = (
        data_raw
        if data_raw
        else str(Path(known_folder(KNOWN_FOLDER_DOCUMENTS)) / PRODUCT_DIRNAME)
    )
    local_candidate = (
        local_raw
        if local_raw
        else str(Path(known_folder(KNOWN_FOLDER_LOCAL_APP_DATA)) / PRODUCT_DIRNAME)
    )
    data_root = _canonical_override(
        data_candidate,
        field="data_root",
        program_upsp_root=program_root,
    )
    local_root = _canonical_override(
        local_candidate,
        field="local_state_root",
        program_upsp_root=program_root,
    )
    if data_root == local_root or _is_within(data_root, local_root) or _is_within(
        local_root, data_root
    ):
        raise DataRootError("storage_roots_must_be_separate")
    return data_root, local_root


def _atomic_write_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.parent / f".{path.name}.{secrets.token_hex(8)}.tmp"
    try:
        temp.write_text(
            json.dumps(value, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        os.replace(temp, path)
    finally:
        try:
            temp.unlink()
        except FileNotFoundError:
            pass


def _read_manifest(path: Path) -> str:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as exc:
        raise DataRootError("active_instance_manifest_missing") from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise DataRootError("active_instance_manifest_invalid") from exc
    if (
        not isinstance(value, dict)
        or set(value) != {"schema_version", "pid"}
        or value.get("schema_version") != ACTIVE_INSTANCE_SCHEMA
    ):
        raise DataRootError("active_instance_manifest_invalid")
    return validate_pid(value.get("pid"))


def _config_template_files(initialization_root: Path) -> tuple[Path, ...]:
    config_root = initialization_root / "os_template" / "config"
    if not config_root.is_dir():
        raise DataRootError("os_config_template_missing")
    files = tuple(sorted(path for path in config_root.rglob("*") if path.is_file()))
    relative_files = {
        path.relative_to(config_root).as_posix()
        for path in files
    }
    if not REQUIRED_OS_CONFIG_FILES.issubset(relative_files):
        raise DataRootError("os_config_template_incomplete")
    for path in files:
        if path.suffix.lower() != ".json":
            raise DataRootError("os_config_template_invalid")
        try:
            value = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise DataRootError("os_config_template_invalid") from exc
        if not isinstance(value, dict):
            raise DataRootError("os_config_template_invalid")
    return files


def _build_layout(
    program_root: Path,
    data_root: Path,
    local_root: Path,
    pid: str,
) -> ActiveLayout:
    personas_root = data_root / "personas"
    instance_root = personas_root / pid
    expected_parent = personas_root.resolve()
    if instance_root.resolve().parent != expected_parent:
        raise DataRootError("active_instance_path_invalid")
    os_root = instance_root / "OS"
    return ActiveLayout(
        program_upsp_root=program_root,
        program_os_root=program_root / "OS",
        initialization_root=program_root / "initialization",
        data_root=data_root,
        local_state_root=local_root,
        manifest_path=data_root / ACTIVE_INSTANCE_FILENAME,
        personas_root=personas_root,
        pid=pid,
        instance_root=instance_root,
        os_root=os_root,
        persona_dir=os_root / "persona",
        config_dir=os_root / "config",
        files_dir=os_root / "files",
        trash_dir=os_root / "trash",
        global_config_dir=local_root / "config",
        audit_cache_dir=local_root / "cache" / "audit" / pid,
    )


def _validate_layout(layout: ActiveLayout) -> None:
    if not layout.instance_root.is_dir() or not layout.os_root.is_dir():
        raise DataRootError("active_instance_layout_incomplete")
    if not _is_within(
        layout.os_root.resolve(),
        layout.instance_root.resolve(),
    ):
        raise DataRootError("active_instance_path_invalid")
    for directory in (layout.config_dir, layout.files_dir, layout.trash_dir):
        if not directory.is_dir():
            raise DataRootError("active_instance_layout_incomplete")
        if not _is_within(directory.resolve(), layout.os_root.resolve()):
            raise DataRootError("active_instance_path_invalid")
    template_files = _config_template_files(layout.initialization_root)
    template_root = layout.initialization_root / "os_template" / "config"
    for template_file in template_files:
        relative = template_file.relative_to(template_root)
        target = layout.config_dir / relative
        if not target.is_file():
            raise DataRootError("active_instance_layout_incomplete")
        if not _is_within(target.resolve(), layout.config_dir.resolve()):
            raise DataRootError("active_instance_path_invalid")
        try:
            value = json.loads(target.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise DataRootError("active_instance_config_invalid") from exc
        if not isinstance(value, dict):
            raise DataRootError("active_instance_config_invalid")
    if layout.persona_dir.exists():
        if not layout.persona_dir.is_dir():
            raise DataRootError("active_instance_layout_incomplete")
        if not _is_within(
            layout.persona_dir.resolve(),
            layout.os_root.resolve(),
        ):
            raise DataRootError("active_instance_path_invalid")


def _create_active_layout(
    program_root: Path,
    data_root: Path,
    local_root: Path,
) -> ActiveLayout:
    initialization_root = program_root / "initialization"
    template_root = initialization_root / "os_template"
    _config_template_files(initialization_root)
    if not template_root.is_dir():
        raise DataRootError("os_template_missing")

    data_root.parent.mkdir(parents=True, exist_ok=True)
    if data_root.exists():
        if not data_root.is_dir():
            raise DataRootError("data_root_not_directory")
        if any(data_root.iterdir()):
            raise DataRootError("active_instance_manifest_missing")
        data_root.rmdir()

    temp_root = data_root.parent / f".{data_root.name}-init-{secrets.token_hex(8)}"
    pid = generate_pid()
    temp_layout = _build_layout(program_root, temp_root, local_root, pid)
    final_layout = _build_layout(program_root, data_root, local_root, pid)
    try:
        # 安装目录可能整体只读；用户数据副本不得继承模板文件的只读属性。
        shutil.copytree(
            template_root,
            temp_layout.os_root,
            copy_function=shutil.copyfile,
        )
        temp_layout.files_dir.mkdir(parents=True, exist_ok=True)
        temp_layout.trash_dir.mkdir(parents=True, exist_ok=True)
        _atomic_write_json(
            temp_layout.manifest_path,
            {"schema_version": ACTIVE_INSTANCE_SCHEMA, "pid": pid},
        )
        _validate_layout(temp_layout)
        os.replace(temp_root, data_root)
    except FileExistsError:
        if temp_root.exists():
            shutil.rmtree(temp_root, ignore_errors=True)
        if not final_layout.manifest_path.is_file():
            raise DataRootError("active_instance_create_conflict")
        pid = _read_manifest(final_layout.manifest_path)
        final_layout = _build_layout(program_root, data_root, local_root, pid)
    except Exception:
        if temp_root.exists():
            shutil.rmtree(temp_root, ignore_errors=True)
        raise
    _validate_layout(final_layout)
    return final_layout


def ensure_active_instance(
    program_upsp_root: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
    known_folder: Callable[[uuid.UUID], Path] = windows_known_folder,
) -> ActiveLayout:
    """Load the active instance, or atomically allocate the first draft OS."""
    program_root = Path(program_upsp_root).resolve()
    data_root, local_root = _resolve_layout_roots(
        program_root,
        environ=environ,
        known_folder=known_folder,
    )
    manifest = data_root / ACTIVE_INSTANCE_FILENAME
    if manifest.is_file():
        return _load_layout(program_root, data_root, local_root)
    return _create_active_layout(program_root, data_root, local_root)


def _resolve_layout_roots(
    program_root: Path,
    *,
    environ: Mapping[str, str] | None,
    known_folder: Callable[[uuid.UUID], Path],
) -> tuple[Path, Path]:
    if not (program_root / "OS").is_dir() or not (
        program_root / "initialization"
    ).is_dir():
        raise DataRootError("program_root_invalid")
    return resolve_storage_roots(
        program_root,
        environ=environ,
        known_folder=known_folder,
    )


def _load_layout(
    program_root: Path,
    data_root: Path,
    local_root: Path,
) -> ActiveLayout:
    pid = _read_manifest(data_root / ACTIVE_INSTANCE_FILENAME)
    layout = _build_layout(program_root, data_root, local_root, pid)
    _validate_layout(layout)
    return layout


def load_active_instance(
    program_upsp_root: str | Path,
    *,
    environ: Mapping[str, str] | None = None,
    known_folder: Callable[[uuid.UUID], Path] = windows_known_folder,
) -> ActiveLayout:
    """Read and validate the active instance without creating any files."""
    program_root = Path(program_upsp_root).resolve()
    data_root, local_root = _resolve_layout_roots(
        program_root,
        environ=environ,
        known_folder=known_folder,
    )
    return _load_layout(program_root, data_root, local_root)
