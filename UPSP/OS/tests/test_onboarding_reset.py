from __future__ import annotations

import importlib.util
from pathlib import Path
import os

import pytest


def _load_module():
    root = Path(__file__).resolve().parents[3]
    path = root / "tools" / "reset_onboarding_sandbox.py"
    spec = importlib.util.spec_from_file_location("reset_onboarding_sandbox", path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _git_stub(branch="codex/onboarding-baseline"):
    def run(root, *args):
        if args == ("rev-parse", "--show-toplevel"):
            return str(root)
        if args == ("branch", "--show-current"):
            return branch
        raise AssertionError(args)

    return run


def test_reset_requires_exact_branch_and_local_marker(tmp_path, monkeypatch):
    module = _load_module()
    monkeypatch.setattr(module, "_git", _git_stub("codex/seed-gui"))
    with pytest.raises(module.SandboxResetError, match="wrong_onboarding_branch"):
        module.reset_sandbox(tmp_path, "persona")

    monkeypatch.setattr(module, "_git", _git_stub())
    with pytest.raises(module.SandboxResetError, match="onboarding_marker_required"):
        module.reset_sandbox(tmp_path, "persona")


def test_persona_reset_moves_only_persona(tmp_path, monkeypatch):
    module = _load_module()
    monkeypatch.setattr(module, "_git", _git_stub())
    (tmp_path / module.MARKER_NAME).write_text("local guard\n", encoding="utf-8")
    data_root = tmp_path / "documents" / "UPSP"
    local_root = tmp_path / "local" / "UPSP"
    monkeypatch.setenv(module.DATA_ROOT_ENV, str(data_root))
    monkeypatch.setenv(module.LOCAL_STATE_ROOT_ENV, str(local_root))
    monkeypatch.setattr(
        module,
        "resolve_storage_roots",
        lambda _program, *, environ: (
            (data_root, local_root)
            if environ is os.environ
            else (tmp_path / "default-data", tmp_path / "default-local")
        ),
    )
    persona_file = data_root / "personas/PID/OS/persona/state.json"
    manifest = data_root / "active_instance.json"
    model_file = local_root / "config/models.json"
    persona_file.parent.mkdir(parents=True)
    model_file.parent.mkdir(parents=True)
    persona_file.write_text('{"total_round": 0}\n', encoding="utf-8")
    manifest.write_text('{"pid": "PID"}\n', encoding="utf-8")
    model_file.write_text('{"models": []}\n', encoding="utf-8")

    receipt = module.reset_sandbox(tmp_path, "persona", timestamp="20260723-120000")

    assert [item["scope"] for item in receipt["moved"]] == ["data_root"]
    assert not persona_file.exists()
    assert not manifest.exists()
    assert model_file.is_file()
    recovered = Path(receipt["moved"][0]["recovery_path"])
    recovered = recovered / "personas/PID/OS/persona/state.json"
    assert recovered.read_text(encoding="utf-8") == '{"total_round": 0}\n'


def test_full_reset_moves_data_and_local_state_roots(tmp_path, monkeypatch):
    module = _load_module()
    monkeypatch.setattr(module, "_git", _git_stub())
    (tmp_path / module.MARKER_NAME).write_text("local guard\n", encoding="utf-8")
    data_root = tmp_path / "documents" / "UPSP"
    local_root = tmp_path / "local" / "UPSP"
    monkeypatch.setenv(module.DATA_ROOT_ENV, str(data_root))
    monkeypatch.setenv(module.LOCAL_STATE_ROOT_ENV, str(local_root))
    monkeypatch.setattr(
        module,
        "resolve_storage_roots",
        lambda _program, *, environ: (
            (data_root, local_root)
            if environ is os.environ
            else (tmp_path / "default-data", tmp_path / "default-local")
        ),
    )
    targets = {
        data_root / "active_instance.json": "{}\n",
        data_root / "personas/PID/OS/persona/core.md": "# Test\n",
        data_root / "personas/PID/OS/config/model_routing.json": "{}\n",
        local_root / "config/interface.json": "{}\n",
        local_root / "config/models.json": "{}\n",
    }
    for path, content in targets.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding="utf-8")
    keep = tmp_path / "outside.txt"
    keep.write_text('{"keep": true}\n', encoding="utf-8")

    receipt = module.reset_sandbox(tmp_path, "full", timestamp="20260723-120001")

    assert {item["scope"] for item in receipt["moved"]} == {
        "data_root",
        "local_state_root",
    }
    assert keep.is_file()
    assert not data_root.exists()
    assert not local_root.exists()
    recovered_by_scope = {
        item["scope"]: Path(item["recovery_path"])
        for item in receipt["moved"]
    }
    assert (
        recovered_by_scope["data_root"]
        / "personas/PID/OS/persona/core.md"
    ).read_text(encoding="utf-8") == "# Test\n"
    assert (
        recovered_by_scope["local_state_root"]
        / "config/models.json"
    ).read_text(encoding="utf-8") == "{}\n"


def test_reset_rejects_missing_isolated_root_overrides(tmp_path, monkeypatch):
    module = _load_module()
    monkeypatch.setattr(module, "_git", _git_stub())
    (tmp_path / module.MARKER_NAME).write_text("local guard\n", encoding="utf-8")
    monkeypatch.delenv(module.DATA_ROOT_ENV, raising=False)
    monkeypatch.delenv(module.LOCAL_STATE_ROOT_ENV, raising=False)

    with pytest.raises(
        module.SandboxResetError,
        match="isolated_storage_roots_required",
    ):
        module.reset_sandbox(tmp_path, "persona")
