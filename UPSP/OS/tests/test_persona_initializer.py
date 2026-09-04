import hashlib
import json
import re
import shutil
from pathlib import Path

import pytest

from initialization.persona_initializer import (
    PersonaInitializationError,
    PersonaInitializer,
    generate_pid,
    load_preset,
    persona_code,
    validate_profile,
)
from schemas.state import FIELDS as STATE_FIELDS


REPO_ROOT = Path(__file__).resolve().parents[3]
TEMPLATE_ROOT = REPO_ROOT / "UPSP" / "initialization" / "persona_template"
PRESET_ROOT = REPO_ROOT / "UPSP" / "initialization" / "persona_presets"


def _stamp():
    return {
        "profile_id": "model_test",
        "model_alias": "测试模型",
        "model": "test-model",
        "context_window": 128000,
    }


def test_spec702_alyosha_preset_and_sixty_point_budget():
    profile = load_preset(PRESET_ROOT, "alyosha")
    assert profile["name_zh"] == "阿廖沙"
    assert profile["name_en"] == "Alyosha"
    assert profile["abbreviation"] == "ALY"
    assert profile["persona_code"] == "SCVAOK"
    assert sum(abs(value - 50) for value in profile["axes"].values()) == 60


def test_spec702_pid_shape_and_checksum():
    pid = generate_pid()
    assert re.fullmatch(r"B\d{8}-\d{6}-[0-9A-F]{4}-[0-9A-F]{2}", pid)
    prefix, check = pid.rsplit("-", 1)
    assert check == hashlib.sha256(prefix.encode("utf-8")).hexdigest()[:2].upper()


def test_spec702_custom_validation_is_exact_and_deterministic():
    profile = load_preset(PRESET_ROOT, "alyosha")
    assert validate_profile(profile)["persona_code"] == "SCVAOK"
    invalid = {**profile, "axes": {**profile["axes"], "S": 56}}
    with pytest.raises(PersonaInitializationError, match="persona_axis_budget_invalid"):
        validate_profile(invalid)
    assert persona_code({"S": 50, "C": 49, "V": 50, "A": 49, "R": 50, "B": 49}) == "XDXIXK"
    assert persona_code({"S": 80, "C": 20, "V": 50, "A": 50, "R": 50, "B": 50}) == "SDXXXX"


def test_spec702_atomic_alyosha_creation_has_neutral_state_and_self_card(tmp_path):
    target = tmp_path / "OS" / "persona"
    initializer = PersonaInitializer(target, TEMPLATE_ROOT, PRESET_ROOT)
    assert initializer.status()["state"] == "missing"
    receipt = initializer.create(load_preset(PRESET_ROOT, "alyosha"), _stamp())

    assert initializer.status() == {"state": "ready", "ready": True, "missing": []}
    assert receipt["persona_code"] == "SCVAOK"
    state = json.loads((target / "state.json").read_text(encoding="utf-8"))
    base = state["base"]
    assert base["meta"]["total_round"] == 0
    assert base["core_axes"] == {"S": 55, "C": 55, "V": 60, "A": 55, "R": 30, "B": 35}
    assert all(item["value"] == 0 for item in base["dynamic_axes"].values())
    assert set(base["comfort_zone"].values()) == {0}
    assert base["identity"]["confirmed"] is False
    assert base["identity"]["current_relation_id"] is None
    assert base["token_usage"]["window_size"] == 128000
    assert base["core_speed_wheel"]["current"] == 0
    assert base["core_speed_wheel"]["max"] in {64, 128, 256, 384, 512}

    registry = json.loads(
        (target / "relation/relation_registry.json").read_text(encoding="utf-8")
    )
    card = registry["cards"][0]
    assert card["id"] == receipt["pid"]
    assert card["category"] == "self"
    assert set(card["aliases"]) == {"阿廖沙", "Alyosha", "ALY", "我"}
    assert (target / card["path"]).is_file()
    core = (target / "core.md").read_text(encoding="utf-8")
    birth = (target / "LTM/Immune/birth.md").read_text(encoding="utf-8")
    assert "FMZ" not in core
    assert "## 0. 使用说明" in core
    assert "共 60 点" in core
    assert "@@UPSP_" not in core
    assert "@@UPSP_" not in birth
    assert base["meta"]["last_update"]
    assert base["fatigue"]["awake_since"] == base["meta"]["last_update"]
    assert not list(target.rglob("round_*.jsonl"))


def test_spec702_existing_or_incomplete_target_is_never_overwritten(tmp_path):
    target = tmp_path / "OS" / "persona"
    target.mkdir(parents=True)
    marker = target / "keep.txt"
    marker.write_text("do not overwrite", encoding="utf-8")
    initializer = PersonaInitializer(target, TEMPLATE_ROOT, PRESET_ROOT)
    assert initializer.status()["state"] == "incomplete"
    with pytest.raises(PersonaInitializationError, match="persona_already_exists"):
        initializer.create(load_preset(PRESET_ROOT, "alyosha"), _stamp())
    assert marker.read_text(encoding="utf-8") == "do not overwrite"
    assert not list(target.parent.glob(".persona-init-*"))


def test_spec702_ready_status_fails_closed_for_missing_or_corrupt_skeleton(tmp_path):
    target = tmp_path / "OS" / "persona"
    initializer = PersonaInitializer(target, TEMPLATE_ROOT, PRESET_ROOT)
    initializer.create(load_preset(PRESET_ROOT, "alyosha"), _stamp())

    docs_registry = target / "docs/docs_registry.json"
    docs_registry.unlink()
    status = initializer.status()
    assert status["state"] == "incomplete"
    assert "docs/docs_registry.json" in status["missing"]

    docs_registry.write_text("{broken", encoding="utf-8")
    status = initializer.status()
    assert status == {"state": "incomplete", "ready": False, "missing": []}


def test_spec762_known_state_migration_restores_persona_readiness(tmp_path):
    from data.state_store import StateStore

    target = tmp_path / "OS" / "persona"
    initializer = PersonaInitializer(target, TEMPLATE_ROOT, PRESET_ROOT)
    initializer.create(load_preset(PRESET_ROOT, "alyosha"), _stamp())
    state_path = target / "state.json"
    state = json.loads(state_path.read_text(encoding="utf-8"))
    state["base"]["heartbeat_flags"].pop("memory_compression_due")
    state_path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

    assert initializer.status() == {
        "state": "incomplete", "ready": False, "missing": [],
    }
    StateStore(str(state_path)).migrate_memory_compression_flags()
    assert initializer.status() == {"state": "ready", "ready": True, "missing": []}


def test_branch_readiness_uses_shared_persona_truth(tmp_path):
    shared = tmp_path / "meta" / "persona"
    receipt = PersonaInitializer(shared, TEMPLATE_ROOT, PRESET_ROOT).create(
        load_preset(PRESET_ROOT, "alyosha"), _stamp()
    )
    branch = tmp_path / "branch" / "persona"

    def ignore_shared(path, _names):
        current = Path(path)
        if current == shared:
            return {"core.md", "rules", "docs"}
        if current == shared / "LTM":
            return {"Memory"}
        return set()

    shutil.copytree(shared, branch, ignore=ignore_shared)
    status = PersonaInitializer(
        branch,
        TEMPLATE_ROOT,
        PRESET_ROOT,
        pid=receipt["pid"],
        shared_persona_dir=shared,
    ).status()

    assert status == {"state": "ready", "ready": True, "missing": []}
    (branch / "state.json").unlink()
    assert PersonaInitializer(
        branch, TEMPLATE_ROOT, PRESET_ROOT, shared_persona_dir=shared
    ).status()["missing"] == ["state.json"]


def test_spec702_state_store_cannot_create_default_half_persona(tmp_path, monkeypatch):
    from data import state_store as module

    state_path = tmp_path / "persona" / "state.json"
    monkeypatch.setattr(module, "STATE_JSON", str(state_path))
    store = module.StateStore()

    with pytest.raises(Exception, match="persona_initialization_required"):
        store.init_if_missing()
    assert not state_path.exists()


def test_spec702_template_contains_no_live_identity_or_runtime_records():
    assert not (REPO_ROOT / "UPSP" / "persona_template").exists()
    assert not (REPO_ROOT / "UPSP" / "persona_presets").exists()
    all_text = "\n".join(
        path.read_text(encoding="utf-8")
        for path in TEMPLATE_ROOT.rglob("*")
        if path.is_file() and path.suffix in {".md", ".json", ".jsonl"}
    )
    assert "零号广播员" not in all_text
    assert "TzPz" not in all_text
    assert "阿廖沙" not in all_text
    assert "Alyosha" not in all_text
    assert '"id": "FMZ"' not in all_text
    assert "30 点自由分配" not in all_text
    assert "60 点自由分配" in all_text
    core = (TEMPLATE_ROOT / "core.md").read_text(encoding="utf-8")
    birth = (TEMPLATE_ROOT / "LTM/Immune/birth.md").read_text(encoding="utf-8")
    state = json.loads((TEMPLATE_ROOT / "state.json").read_text(encoding="utf-8"))
    assert "## 0. 使用说明" in core
    assert "## 8. 实例补充说明" in core
    assert "共 60 点" in core
    assert "@@UPSP_PID@@" in core
    assert "@@UPSP_PID@@" in birth
    assert state["base"]["core_axes"] == {key: 50 for key in "SCVARB"}
    assert set(state["base"]["comfort_zone"].values()) == {0}
    assert all(value is False for value in state["base"]["heartbeat_flags"].values())
    for dotpath in STATE_FIELDS:
        value = state
        for part in dotpath.split("."):
            assert isinstance(value, dict) and part in value, dotpath
            value = value[part]
    connectivity = json.loads(
        (TEMPLATE_ROOT / "STM/health/base/connectivity.json").read_text(encoding="utf-8")
    )
    assert connectivity["recent_latencies"] == []
    assert not (TEMPLATE_ROOT / "STM/buffer/state_backups.jsonl").read_text(
        encoding="utf-8"
    ).strip()
    heat = json.loads(
        (TEMPLATE_ROOT / "STM/memory/heat.json").read_text(encoding="utf-8")
    )
    assert heat == {"_comment": "STM 热度值（脚本独占管理）", "entries": {}}


@pytest.mark.parametrize(
    ("relative_path", "rewrite", "error"),
    (
        (
            "core.md",
            lambda text: text.replace("@@UPSP_PID@@", "", 1),
            "persona_template_markers_invalid",
        ),
        (
            "core.md",
            lambda text: text + "\n@@UPSP_PID@@\n",
            "persona_template_markers_invalid",
        ),
        (
            "state.json",
            lambda _text: "{broken",
            "persona_state_template_invalid",
        ),
        (
            "state.json",
            lambda text: text.replace('"S": 50', '"S": 51', 1),
            "persona_state_template_not_neutral",
        ),
        (
            "relation/relation_registry.json",
            lambda _text: "{broken",
            "persona_template_invalid",
        ),
    ),
)
def test_spec702_invalid_templates_fail_before_target_creation(
    tmp_path, relative_path, rewrite, error
):
    template = tmp_path / "template"
    shutil.copytree(TEMPLATE_ROOT, template)
    path = template / relative_path
    path.write_text(rewrite(path.read_text(encoding="utf-8")), encoding="utf-8")
    target = tmp_path / "OS" / "persona"

    with pytest.raises(PersonaInitializationError, match=error):
        PersonaInitializer(target, template, PRESET_ROOT).create(
            load_preset(PRESET_ROOT, "alyosha"),
            _stamp(),
        )

    assert not target.exists()
    assert not list(target.parent.glob(".persona-init-*"))
