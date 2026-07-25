from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[3]

import sys

sys.path.insert(0, str(ROOT / "tools"))

from export_public_source import (  # noqa: E402
    MANIFEST_NAME,
    PublicSourceError,
    export_public_source,
    is_public_path,
)


def _git(repo: Path, *args: str) -> str:
    result = subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def _commit(repo: Path) -> str:
    _git(repo, "init", "-b", "main")
    _git(repo, "config", "user.name", "UPSP Test")
    _git(repo, "config", "user.email", "upsp-test@example.invalid")
    _git(repo, "add", ".")
    _git(repo, "commit", "-m", "snapshot")
    return _git(repo, "rev-parse", "HEAD")


def test_public_path_contract() -> None:
    assert is_public_path("UPSP/OS/main.py")
    assert is_public_path("UPSP/gui/src/app.ts")
    assert is_public_path("desktop/UPSP.Desktop/Program.cs")
    assert is_public_path("tools/export_public_source.py")
    assert not is_public_path(".speckit/specs/711/spec.md")
    assert not is_public_path("docs/codex/current_state.json")
    assert not is_public_path("UPSP/OS/persona/core.md")
    assert not is_public_path("UPSP/gui/AGENTS.md")
    assert not is_public_path("tools/upsp_visible_dogfood.py")
    assert not is_public_path("UPSP/OS/tests/test_dds_truth_audit.py")
    assert not is_public_path("UPSP/OS/tests/test_prompt_cache_planner.py")


def test_export_is_tracked_only_and_manifested(tmp_path: Path) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    (repo / "LICENSE").write_text("MIT\n", encoding="utf-8")
    source = repo / "UPSP" / "OS" / "main.py"
    source.parent.mkdir(parents=True)
    source.write_text("print('public')\n", encoding="utf-8")
    manual = repo / "UPSP" / "gui" / "manual" / "说明.md"
    manual.parent.mkdir(parents=True)
    manual.write_text("# 说明\n", encoding="utf-8")
    private = repo / ".speckit" / "evidence.json"
    private.parent.mkdir()
    private.write_text('{"private": true}\n', encoding="utf-8")
    commit = _commit(repo)
    (repo / "UPSP" / "OS" / "untracked.txt").write_text("not exported\n", encoding="utf-8")

    output = tmp_path / "public"
    result = export_public_source(repo, commit, output)

    assert (output / "LICENSE").read_text(encoding="utf-8") == "MIT\n"
    assert (output / "UPSP" / "OS" / "main.py").read_text(encoding="utf-8") == (
        "print('public')\n"
    )
    assert not (output / ".speckit").exists()
    assert not (output / "UPSP" / "OS" / "untracked.txt").exists()
    manifest = json.loads((output / MANIFEST_NAME).read_text(encoding="utf-8"))
    assert manifest["schema_version"] == "upsp_public_source_manifest.v1"
    assert manifest["source_commit"] == commit
    assert manifest["file_count"] == 3
    record = next(item for item in manifest["files"] if item["path"] == "LICENSE")
    assert record["sha256"] == hashlib.sha256(
        (output / "LICENSE").read_bytes()
    ).hexdigest()
    assert result["file_count"] == 3


@pytest.mark.parametrize(
    "secret",
    (
        "C:" + "\\Users\\Lenovo\\private",
        "E:" + "\\AI_workspace\\api\\" + "天" + "枢.txt",
        "sk-" + "proj-abcdefghijklmnopqrstuvwx",
        "-----BEGIN " + "PRIVATE KEY-----",
    ),
)
def test_export_rejects_private_or_secret_text(tmp_path: Path, secret: str) -> None:
    repo = tmp_path / "repo"
    repo.mkdir()
    source = repo / "UPSP" / "OS" / "main.py"
    source.parent.mkdir(parents=True)
    source.write_text(secret + "\n", encoding="utf-8")
    commit = _commit(repo)
    output = tmp_path / "public"

    with pytest.raises(PublicSourceError):
        export_public_source(repo, commit, output)

    assert not output.exists()
