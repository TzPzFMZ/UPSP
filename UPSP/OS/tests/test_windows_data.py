import json
import hashlib
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys

import pytest

import initialization.windows_data as windows_data
from initialization.windows_data import (
    ACTIVE_INSTANCE_SCHEMA,
    DATA_ROOT_ENV,
    INSTANCE_MANIFEST_FILENAME,
    INSTANCE_MANIFEST_SCHEMA,
    KNOWN_FOLDER_DOCUMENTS,
    KNOWN_FOLDER_LOCAL_APP_DATA,
    LEGACY_ACTIVE_INSTANCE_SCHEMA,
    LOCAL_STATE_ROOT_ENV,
    META_INSTANCE_ID,
    DataRootError,
    ensure_active_instance,
    load_active_instance,
    resolve_storage_roots,
    validate_pid,
)


PROGRAM_ROOT = Path(__file__).resolve().parents[2]


def _roots(tmp_path):
    return {
        DATA_ROOT_ENV: str(tmp_path / "含 空格的文档" / "UPSP"),
        LOCAL_STATE_ROOT_ENV: str(tmp_path / "本机 状态" / "UPSP"),
    }


def test_known_folder_defaults_respect_redirected_locations(tmp_path):
    documents = tmp_path / "OneDrive - 示例" / "文档"
    local = tmp_path / "本机"

    def known_folder(folder_id):
        if folder_id == KNOWN_FOLDER_DOCUMENTS:
            return documents
        if folder_id == KNOWN_FOLDER_LOCAL_APP_DATA:
            return local
        raise AssertionError(folder_id)

    data_root, local_root = resolve_storage_roots(
        PROGRAM_ROOT,
        environ={},
        known_folder=known_folder,
    )

    assert data_root == (documents / "UPSP").resolve()
    assert local_root == (local / "UPSP").resolve()


@pytest.mark.parametrize(
    ("name", "value", "error"),
    [
        (DATA_ROOT_ENV, "relative-data", "data_root_must_be_absolute"),
        (LOCAL_STATE_ROOT_ENV, "relative-local", "local_state_root_must_be_absolute"),
    ],
)
def test_relative_root_override_is_rejected(tmp_path, name, value, error):
    environment = _roots(tmp_path)
    environment[name] = value
    with pytest.raises(DataRootError, match=error):
        resolve_storage_roots(PROGRAM_ROOT, environ=environment)


def test_program_internal_root_and_retired_persona_override_are_rejected(tmp_path):
    environment = _roots(tmp_path)
    environment[DATA_ROOT_ENV] = str(PROGRAM_ROOT / "runtime-data")
    with pytest.raises(DataRootError, match="data_root_must_not_overlap_program"):
        resolve_storage_roots(PROGRAM_ROOT, environ=environment)

    environment = _roots(tmp_path)
    environment["UPSP_PERSONA_DIR"] = str(tmp_path / "persona")
    with pytest.raises(DataRootError, match="legacy_persona_dir_override_retired"):
        resolve_storage_roots(PROGRAM_ROOT, environ=environment)


def test_volume_root_program_parent_and_overlapping_roots_are_rejected(tmp_path):
    environment = _roots(tmp_path)
    environment[DATA_ROOT_ENV] = PROGRAM_ROOT.anchor
    with pytest.raises(DataRootError, match="data_root_must_not_be_volume_root"):
        resolve_storage_roots(PROGRAM_ROOT, environ=environment)

    environment = _roots(tmp_path)
    environment[DATA_ROOT_ENV] = str(PROGRAM_ROOT.parent)
    with pytest.raises(DataRootError, match="data_root_must_not_overlap_program"):
        resolve_storage_roots(PROGRAM_ROOT, environ=environment)

    environment = _roots(tmp_path)
    environment[LOCAL_STATE_ROOT_ENV] = str(
        Path(environment[DATA_ROOT_ENV]) / "nested"
    )
    with pytest.raises(DataRootError, match="storage_roots_must_be_separate"):
        resolve_storage_roots(PROGRAM_ROOT, environ=environment)


def test_fresh_root_allocates_one_stable_draft_os(tmp_path):
    environment = _roots(tmp_path)
    first = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    second = ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert first.pid == second.pid
    assert validate_pid(first.pid) == first.pid
    assert first.persona_dir.exists() is False
    assert first.config_dir.is_dir()
    assert first.files_dir.is_dir()
    assert first.trash_dir.is_dir()
    assert (first.config_dir / "system.json").is_file()
    assert (first.config_dir / "model_routing.json").is_file()
    assert json.loads(first.manifest_path.read_text(encoding="utf-8")) == {
        "schema_version": ACTIVE_INSTANCE_SCHEMA,
        "pid": first.pid,
        "instance_id": META_INSTANCE_ID,
    }
    assert json.loads(
        (first.instance_root / INSTANCE_MANIFEST_FILENAME).read_text(encoding="utf-8")
    ) == {
        "schema_version": INSTANCE_MANIFEST_SCHEMA,
        "pid": first.pid,
        "instance_id": META_INSTANCE_ID,
        "kind": "meta",
    }


def test_legacy_os_layout_migrates_once_to_meta(tmp_path):
    environment = _roots(tmp_path)
    layout = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    legacy_root = layout.pid_root / "OS"
    os.replace(layout.meta_root, legacy_root)
    layout.manifest_path.write_text(
        json.dumps({
            "schema_version": LEGACY_ACTIVE_INSTANCE_SCHEMA,
            "pid": layout.pid,
        }),
        encoding="utf-8",
    )

    migrated = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    repeated = ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert migrated.instance_id == repeated.instance_id == META_INSTANCE_ID
    assert migrated.meta_root.is_dir()
    assert not legacy_root.exists()
    assert json.loads(migrated.manifest_path.read_text(encoding="utf-8")) == {
        "schema_version": ACTIVE_INSTANCE_SCHEMA,
        "pid": layout.pid,
        "instance_id": META_INSTANCE_ID,
    }


def test_legacy_os_and_meta_conflict_fails_closed(tmp_path):
    environment = _roots(tmp_path)
    layout = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    shutil.copytree(layout.meta_root, layout.pid_root / "OS")
    layout.manifest_path.write_text(
        json.dumps({
            "schema_version": LEGACY_ACTIVE_INSTANCE_SCHEMA,
            "pid": layout.pid,
        }),
        encoding="utf-8",
    )

    with pytest.raises(DataRootError, match="active_instance_legacy_migration_conflict"):
        ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert layout.meta_root.is_dir()
    assert (layout.pid_root / "OS").is_dir()


def test_legacy_migration_writes_v2_active_manifest_after_validation(
    tmp_path, monkeypatch
):
    environment = _roots(tmp_path)
    layout = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    os.replace(layout.meta_root, layout.pid_root / "OS")
    legacy = {
        "schema_version": LEGACY_ACTIVE_INSTANCE_SCHEMA,
        "pid": layout.pid,
    }
    layout.manifest_path.write_text(json.dumps(legacy), encoding="utf-8")

    def fail_validation(_layout):
        raise DataRootError("validation_failed")

    monkeypatch.setattr(windows_data, "_validate_layout", fail_validation)

    with pytest.raises(DataRootError, match="validation_failed"):
        ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert json.loads(layout.manifest_path.read_text(encoding="utf-8")) == legacy
    assert (layout.pid_root / "meta").is_dir()


def test_read_only_load_never_creates_missing_active_instance(tmp_path):
    environment = _roots(tmp_path)

    with pytest.raises(DataRootError, match="active_instance_manifest_missing"):
        load_active_instance(PROGRAM_ROOT, environ=environment)

    assert not Path(environment[DATA_ROOT_ENV]).exists()


def test_cli_help_does_not_create_active_instance(tmp_path):
    environment = dict(os.environ)
    environment.update(_roots(tmp_path))
    environment["PYTHONDONTWRITEBYTECODE"] = "1"

    completed = subprocess.run(
        [sys.executable, str(PROGRAM_ROOT.parent / "tools" / "upsp_cli.py"), "--help"],
        cwd=PROGRAM_ROOT.parent,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )

    assert completed.returncode == 0, completed.stderr
    assert not Path(environment[DATA_ROOT_ENV]).exists()


def test_os_root_escape_is_rejected(tmp_path):
    environment = _roots(tmp_path)
    layout = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    escaped_os = tmp_path / "escaped" / "OS"
    shutil.copytree(layout.os_root, escaped_os)
    escaped = windows_data.ActiveLayout(
        **{
            **layout.__dict__,
            "os_root": escaped_os,
            "persona_dir": escaped_os / "persona",
            "config_dir": escaped_os / "config",
            "files_dir": escaped_os / "files",
            "trash_dir": escaped_os / "trash",
        }
    )

    with pytest.raises(DataRootError, match="active_instance_path_invalid"):
        windows_data._validate_layout(escaped)


def test_nonempty_root_without_manifest_fails_closed(tmp_path):
    environment = _roots(tmp_path)
    data_root = Path(environment[DATA_ROOT_ENV])
    data_root.mkdir(parents=True)
    (data_root / "unexpected.txt").write_text("occupied", encoding="utf-8")

    with pytest.raises(DataRootError, match="active_instance_manifest_missing"):
        ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert (data_root / "unexpected.txt").read_text(encoding="utf-8") == "occupied"


@pytest.mark.parametrize(
    "payload",
    [
        {"schema_version": "wrong", "pid": "B20260724-000000-0000-00"},
        {"schema_version": ACTIVE_INSTANCE_SCHEMA, "pid": "../../escape"},
        {"schema_version": ACTIVE_INSTANCE_SCHEMA},
    ],
)
def test_bad_manifest_fails_closed_without_repair(tmp_path, payload):
    environment = _roots(tmp_path)
    data_root = Path(environment[DATA_ROOT_ENV])
    data_root.mkdir(parents=True)
    manifest = data_root / "active_instance.json"
    manifest.write_text(
        json.dumps(payload, ensure_ascii=False),
        encoding="utf-8",
    )
    before = manifest.read_bytes()

    with pytest.raises(DataRootError):
        ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert manifest.read_bytes() == before


def test_existing_draft_missing_required_config_fails_closed(tmp_path):
    environment = _roots(tmp_path)
    layout = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    (layout.config_dir / "system.json").unlink()

    with pytest.raises(DataRootError, match="active_instance_layout_incomplete"):
        ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert not (layout.config_dir / "system.json").exists()


def test_existing_draft_with_invalid_config_fails_closed_without_repair(tmp_path):
    environment = _roots(tmp_path)
    layout = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    target = layout.config_dir / "system.json"
    target.write_text("{bad json", encoding="utf-8")
    before = target.read_bytes()

    with pytest.raises(DataRootError, match="active_instance_config_invalid"):
        ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert target.read_bytes() == before


def test_incomplete_program_config_template_does_not_create_data_root(tmp_path):
    program = tmp_path / "程序" / "UPSP"
    (program / "OS").mkdir(parents=True)
    shutil.copytree(
        PROGRAM_ROOT / "initialization" / "os_template",
        program / "initialization" / "os_template",
    )
    shutil.copytree(
        PROGRAM_ROOT / "initialization" / "persona_template",
        program / "initialization" / "persona_template",
    )
    (program / "initialization" / "os_template" / "config" / "system.json").unlink()
    environment = {
        DATA_ROOT_ENV: str(tmp_path / "数据" / "UPSP"),
        LOCAL_STATE_ROOT_ENV: str(tmp_path / "本机" / "UPSP"),
    }

    with pytest.raises(DataRootError, match="os_config_template_incomplete"):
        ensure_active_instance(program, environ=environment)

    assert not Path(environment[DATA_ROOT_ENV]).exists()


def test_atomic_directory_replace_failure_leaves_no_partial_root(tmp_path, monkeypatch):
    environment = _roots(tmp_path)
    data_root = Path(environment[DATA_ROOT_ENV]).resolve()
    original_replace = windows_data.os.replace

    def fail_final_replace(source, target):
        if Path(target).resolve() == data_root:
            raise OSError("simulated final replace failure")
        return original_replace(source, target)

    monkeypatch.setattr(windows_data.os, "replace", fail_final_replace)
    with pytest.raises(OSError, match="simulated final replace failure"):
        ensure_active_instance(PROGRAM_ROOT, environ=environment)

    assert not data_root.exists()
    leftovers = list(data_root.parent.glob(f".{data_root.name}-init-*"))
    assert leftovers == []


def test_runtime_path_projection_separates_program_data_and_local_state():
    from paths import (
        ACTIVE_INSTANCE_ID,
        ACTIVE_PID,
        AUDIT_DIR,
        CONFIG_DIR,
        GLOBAL_CONFIG_DIR,
        OS_ROOT,
        PERSONA_DIR,
        PROGRAM_OS_ROOT,
        PROGRAM_UPSP_ROOT,
        UPSP_DATA_ROOT,
        UPSP_LOCAL_STATE_ROOT,
    )

    program = Path(PROGRAM_UPSP_ROOT).resolve()
    active_os = Path(OS_ROOT).resolve()
    data_root = Path(UPSP_DATA_ROOT).resolve()
    local_root = Path(UPSP_LOCAL_STATE_ROOT).resolve()

    assert Path(PROGRAM_OS_ROOT).resolve() == program / "OS"
    assert active_os == data_root / "personas" / ACTIVE_PID / ACTIVE_INSTANCE_ID
    assert Path(PERSONA_DIR).resolve() == active_os / "persona"
    assert Path(CONFIG_DIR).resolve() == active_os / "config"
    assert Path(GLOBAL_CONFIG_DIR).resolve() == local_root / "config"
    assert Path(AUDIT_DIR).resolve() == (
        local_root / "cache" / "audit" / ACTIVE_PID / ACTIVE_INSTANCE_ID
    )
    assert not active_os.is_relative_to(program)
    assert not local_root.is_relative_to(program)


def test_draft_creation_does_not_modify_read_only_program_template(tmp_path):
    program = tmp_path / "只读 程序" / "UPSP"
    (program / "OS").mkdir(parents=True)
    shutil.copytree(
        PROGRAM_ROOT / "initialization" / "os_template",
        program / "initialization" / "os_template",
    )
    shutil.copytree(
        PROGRAM_ROOT / "initialization" / "persona_template",
        program / "initialization" / "persona_template",
    )
    before = {}
    for path in sorted(item for item in program.rglob("*") if item.is_file()):
        before[path.relative_to(program).as_posix()] = hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        path.chmod(stat.S_IREAD)

    environment = {
        DATA_ROOT_ENV: str(tmp_path / "用户 文档" / "UPSP"),
        LOCAL_STATE_ROOT_ENV: str(tmp_path / "本机 设置" / "UPSP"),
    }
    layout = ensure_active_instance(program, environ=environment)

    after = {
        path.relative_to(program).as_posix(): hashlib.sha256(
            path.read_bytes()
        ).hexdigest()
        for path in sorted(item for item in program.rglob("*") if item.is_file())
    }
    for path in (item for item in program.rglob("*") if item.is_file()):
        path.chmod(stat.S_IWRITE | stat.S_IREAD)
    assert after == before
    assert layout.config_dir.is_dir()


def test_startup_syncs_all_common_persona_protocol_without_rewriting_matches(tmp_path, monkeypatch):
    environment = _roots(tmp_path)
    layout = ensure_active_instance(PROGRAM_ROOT, environ=environment)
    template = PROGRAM_ROOT / "initialization" / "persona_template"
    shutil.copytree(template, layout.persona_dir)

    local = layout.persona_dir / "docs" / "persona" / "glossary.md"
    local.write_text("persona-local", encoding="utf-8")
    protocol_sources = [
        source
        for relative in windows_data.PERSONA_PROTOCOL_ROOTS
        for source in sorted((template / relative).rglob("*"))
        if source.is_file()
    ]
    sources = [template / relative for relative in windows_data.PERSONA_PROTOCOL_FILES] + protocol_sources
    assert len(protocol_sources) == 39
    assert len(sources) == windows_data.PERSONA_PROTOCOL_FILE_COUNT == 41
    for index, source in enumerate(sources):
        target = layout.persona_dir / source.relative_to(template)
        if index % 2:
            target.unlink()
        else:
            target.write_text(f"stale-{index}", encoding="utf-8")

    ensure_active_instance(PROGRAM_ROOT, environ=environment)

    for source in sources:
        target = layout.persona_dir / source.relative_to(template)
        assert target.read_bytes() == source.read_bytes()
    assert local.read_text(encoding="utf-8") == "persona-local"

    replacements = []
    original_replace = windows_data.os.replace

    def record_replace(source, target):
        replacements.append((source, target))
        return original_replace(source, target)

    monkeypatch.setattr(windows_data.os, "replace", record_replace)
    ensure_active_instance(PROGRAM_ROOT, environ=environment)
    assert replacements == []


def test_incomplete_persona_protocol_template_fails_before_data_root_creation(tmp_path):
    program = tmp_path / "程序" / "UPSP"
    (program / "OS").mkdir(parents=True)
    shutil.copytree(PROGRAM_ROOT / "initialization" / "os_template", program / "initialization" / "os_template")
    shutil.copytree(PROGRAM_ROOT / "initialization" / "persona_template", program / "initialization" / "persona_template")
    protocol_root = program / "initialization" / "persona_template" / "rules" / "protocol" / "base"
    (protocol_root / "manifesto.md").unlink()
    (protocol_root / "untracked-backup.md").write_text("not a protocol asset", encoding="utf-8")
    environment = _roots(tmp_path / "isolated")

    with pytest.raises(DataRootError, match="persona_protocol_template_incomplete"):
        ensure_active_instance(program, environ=environment)

    assert not Path(environment[DATA_ROOT_ENV]).exists()
