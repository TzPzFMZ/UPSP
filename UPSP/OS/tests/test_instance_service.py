import json
from pathlib import Path

import pytest

from initialization.instance_service import InstanceService, InstanceServiceError
from initialization.persona_initializer import PersonaInitializer, load_preset
from initialization.windows_data import (
    DATA_ROOT_ENV,
    DataRootError,
    LOCAL_STATE_ROOT_ENV,
    META_INSTANCE_ID,
    ensure_active_instance,
    load_active_instance,
)


UPSP_ROOT = Path(__file__).resolve().parents[2]
MODEL_STAMP = {
    "profile_id": "spec732-model",
    "model_alias": "Spec732 model",
    "model": "spec732-model",
    "context_window": 128000,
}


def _environment(tmp_path):
    return {
        DATA_ROOT_ENV: str(tmp_path / "data" / "UPSP"),
        LOCAL_STATE_ROOT_ENV: str(tmp_path / "local" / "UPSP"),
    }


def _ready_layout(tmp_path):
    environment = _environment(tmp_path)
    layout = ensure_active_instance(UPSP_ROOT, environ=environment)
    PersonaInitializer(
        layout.shared_persona_dir,
        UPSP_ROOT / "initialization" / "persona_template",
        UPSP_ROOT / "initialization" / "persona_presets",
        pid=layout.pid,
    ).create(
        load_preset(UPSP_ROOT / "initialization" / "persona_presets", "alyosha"),
        MODEL_STAMP,
    )
    return ensure_active_instance(UPSP_ROOT, environ=environment), environment


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def test_new_branch_resets_stm_but_copies_meta_local_state(tmp_path):
    layout, environment = _ready_layout(tmp_path)
    state_path = layout.persona_dir / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["base"]["meta"]["total_round"] = 7
    _write_json(state_path, state)
    (layout.persona_dir / "STM" / "context" / "cache" / "now_cache.jsonl").write_text(
        "meta-current-context\n", encoding="utf-8"
    )
    project = layout.persona_dir / "LTM" / "Projects" / "PRJ-spec732"
    project.mkdir(parents=True)
    (project / "project.md").write_text("meta project", encoding="utf-8")
    ensure_active_instance(UPSP_ROOT, environ=environment)

    receipt = InstanceService(layout).create_branch(mode="new", label="new branch")
    branch = load_active_instance(UPSP_ROOT, environ=environment)

    assert receipt["source_instance_id"] == META_INSTANCE_ID
    assert branch.instance_id == receipt["instance_id"]
    assert json.loads((branch.persona_dir / "state.json").read_text(encoding="utf-8"))[
        "base"
    ]["meta"]["total_round"] == 7
    assert "meta-current-context" not in (
        branch.persona_dir / "STM" / "context" / "cache" / "now_cache.jsonl"
    ).read_text(encoding="utf-8")
    assert (branch.persona_dir / "LTM" / "Projects" / "PRJ-spec732" / "project.md").is_file()
    assert (branch.persona_dir / "LTM" / "memory_links.json").is_file()
    assert not (branch.persona_dir / "core.md").exists()
    assert not (branch.persona_dir / "rules").exists()
    assert not (branch.persona_dir / "docs").exists()
    assert not (branch.persona_dir / "LTM" / "Memory").exists()
    assert (branch.shared_persona_dir / "core.md").is_file()


def test_fork_copies_settled_context_and_archive_freezes_branch(tmp_path):
    layout, environment = _ready_layout(tmp_path)
    created = InstanceService(layout).create_branch(mode="new", label="source")
    source = load_active_instance(UPSP_ROOT, environ=environment)
    state_path = source.persona_dir / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["base"]["meta"]["total_round"] = 1
    _write_json(state_path, state)
    round_path = source.persona_dir / "STM" / "context" / "round" / "round_1.jsonl"
    round_path.parent.mkdir(parents=True, exist_ok=True)
    round_path.write_text('{"event_type":"round_closed"}\n', encoding="utf-8")
    marker = source.persona_dir / "STM" / "context" / "cache" / "now_cache.jsonl"
    marker.write_text("fork-context\n", encoding="utf-8")

    forked = InstanceService(source).create_branch(
        mode="fork",
        label="fork",
        source_instance_id=created["instance_id"],
    )
    fork = load_active_instance(UPSP_ROOT, environ=environment)

    assert forked["source_round"] == 1
    assert "fork-context" in (
        fork.persona_dir / "STM" / "context" / "cache" / "now_cache.jsonl"
    ).read_text(encoding="utf-8")
    archived = InstanceService(fork).archive(fork.instance_id)
    active = load_active_instance(UPSP_ROOT, environ=environment)
    assert archived["restart_required"] is True
    assert active.instance_id == META_INSTANCE_ID
    assert (active.pid_root / "_archive" / fork.instance_id).is_dir()

    restored = InstanceService(active).restore(fork.instance_id)
    assert restored["restart_required"] is False
    assert (active.pid_root / fork.instance_id).is_dir()


def test_new_persona_allocates_independent_pid_and_copies_routing(tmp_path):
    layout, environment = _ready_layout(tmp_path)
    routing = layout.config_dir / "model_routing.json"
    original = routing.read_bytes()

    receipt = InstanceService(layout).create_persona(
        mode="preset",
        preset_id="alyosha",
        profile=None,
        model_stamp=MODEL_STAMP,
    )
    created = load_active_instance(UPSP_ROOT, environ=environment)

    assert receipt["pid"] != layout.pid
    assert created.pid == receipt["pid"]
    assert created.instance_id == META_INSTANCE_ID
    assert created.config_dir.joinpath("model_routing.json").read_bytes() == original
    assert created.shared_persona_dir != layout.shared_persona_dir
    assert created.shared_persona_dir.joinpath("core.md").is_file()


def test_manifest_identity_must_match_its_directory(tmp_path):
    layout, _environment_value = _ready_layout(tmp_path)
    manifest_path = layout.instance_root / "instance.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["instance_id"] = "I20260810-120000-AAAA"
    _write_json(manifest_path, manifest)

    with pytest.raises(InstanceServiceError, match="instance_manifest_invalid"):
        InstanceService(layout).list_all()


def test_fork_requires_round_closed_to_be_the_terminal_event(tmp_path):
    layout, _environment_value = _ready_layout(tmp_path)
    state_path = layout.persona_dir / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["base"]["meta"]["total_round"] = 1
    _write_json(state_path, state)
    round_path = layout.persona_dir / "STM" / "context" / "round" / "round_1.jsonl"
    round_path.parent.mkdir(parents=True, exist_ok=True)
    round_path.write_text(
        '{"event_type":"round_closed"}\n{"event_type":"late_write"}\n',
        encoding="utf-8",
    )

    with pytest.raises(InstanceServiceError, match="fork_source_not_settled"):
        InstanceService(layout).create_branch(mode="fork", label="fork")


def test_activate_validates_target_before_committing_manifest(tmp_path):
    layout, environment = _ready_layout(tmp_path)
    created = InstanceService(layout).create_branch(mode="new", label="branch")
    branch = load_active_instance(UPSP_ROOT, environ=environment)
    InstanceService(branch).activate(branch.pid, META_INSTANCE_ID)
    meta = load_active_instance(UPSP_ROOT, environ=environment)
    (meta.pid_root / created["instance_id"] / "config" / "system.json").unlink()

    with pytest.raises(DataRootError, match="active_instance_layout_incomplete"):
        InstanceService(meta).activate(meta.pid, created["instance_id"])

    assert load_active_instance(UPSP_ROOT, environ=environment).instance_id == META_INSTANCE_ID


def test_restore_rolls_back_an_incomplete_archive(tmp_path):
    layout, environment = _ready_layout(tmp_path)
    created = InstanceService(layout).create_branch(mode="new", label="branch")
    branch_id = created["instance_id"]
    branch = load_active_instance(UPSP_ROOT, environ=environment)
    InstanceService(branch).archive(branch_id)
    meta = load_active_instance(UPSP_ROOT, environ=environment)
    archived = meta.pid_root / "_archive" / branch_id
    (archived / "config" / "system.json").unlink()

    with pytest.raises(DataRootError, match="active_instance_layout_incomplete"):
        InstanceService(meta).restore(branch_id)

    assert archived.is_dir()
    assert not (meta.pid_root / branch_id).exists()
