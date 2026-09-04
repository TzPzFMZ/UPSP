#!/usr/bin/env python3
"""Recoverably reset isolated onboarding Windows data roots.

The tool has no arbitrary path flags.  It resolves the same two roots as the
product, requires an exact validation branch and a repository-local marker,
and moves whole roots instead of deleting selected files.
"""

from __future__ import annotations

import argparse
from datetime import datetime
import json
import os
from pathlib import Path
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
PROGRAM_UPSP_ROOT = REPO_ROOT / "UPSP"
if str(PROGRAM_UPSP_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_UPSP_ROOT))

from initialization.windows_data import (  # noqa: E402
    DATA_ROOT_ENV,
    LOCAL_STATE_ROOT_ENV,
    DataRootError,
    resolve_storage_roots,
)


EXPECTED_BRANCHES = {
    "codex/onboarding-baseline",
    "codex/windows-data-root",
}
MARKER_NAME = ".upsp-onboarding-sandbox"


class SandboxResetError(RuntimeError):
    pass


def _git(repo_root: Path, *args: str) -> str:
    completed = subprocess.run(
        ["git", "-C", str(repo_root), *args],
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
    )
    if completed.returncode:
        raise SandboxResetError("git_state_unavailable")
    return completed.stdout.strip()


def assert_sandbox(repo_root: Path) -> str:
    root = repo_root.resolve()
    top = Path(_git(root, "rev-parse", "--show-toplevel")).resolve()
    if top != root:
        raise SandboxResetError("not_onboarding_worktree_root")
    branch = _git(root, "branch", "--show-current")
    if branch not in EXPECTED_BRANCHES:
        raise SandboxResetError("wrong_onboarding_branch")
    if not (root / MARKER_NAME).is_file():
        raise SandboxResetError("onboarding_marker_required")
    return branch


def _recovery_target(source: Path, timestamp: str, label: str) -> Path:
    parent = source.parent.resolve()
    target = parent / f"{source.name}-recovery" / timestamp / label
    if target.exists():
        raise SandboxResetError("recovery_target_exists")
    return target


def reset_sandbox(
    repo_root: Path,
    mode: str,
    *,
    timestamp: str | None = None,
) -> dict:
    if mode not in {"persona", "full"}:
        raise SandboxResetError("invalid_reset_mode")
    root = repo_root.resolve()
    branch = assert_sandbox(root)
    if not str(os.environ.get(DATA_ROOT_ENV) or "").strip() or not str(
        os.environ.get(LOCAL_STATE_ROOT_ENV) or ""
    ).strip():
        raise SandboxResetError("isolated_storage_roots_required")
    try:
        data_root, local_root = resolve_storage_roots(
            root / "UPSP",
            environ=os.environ,
        )
        default_data_root, default_local_root = resolve_storage_roots(
            root / "UPSP",
            environ={},
        )
    except DataRootError as exc:
        raise SandboxResetError(str(exc)) from exc
    if data_root == default_data_root or local_root == default_local_root:
        raise SandboxResetError("isolated_storage_roots_required")

    recovery_name = timestamp or datetime.now().strftime("%Y%m%d-%H%M%S-%f")
    operations = []
    if data_root.exists():
        operations.append((
            "data_root",
            data_root,
            _recovery_target(data_root, recovery_name, "data"),
        ))
    if mode == "full" and local_root.exists():
        operations.append((
            "local_state_root",
            local_root,
            _recovery_target(local_root, recovery_name, "local_state"),
        ))

    moved = []
    try:
        for label, source, destination in operations:
            destination.parent.mkdir(parents=True, exist_ok=True)
            source.replace(destination)
            moved.append((label, source, destination))
    except OSError as exc:
        for _label, source, destination in reversed(moved):
            if destination.exists() and not source.exists():
                source.parent.mkdir(parents=True, exist_ok=True)
                destination.replace(source)
        raise SandboxResetError("reset_move_failed") from exc

    return {
        "schema_version": "upsp_onboarding_reset_receipt.v2",
        "mode": mode,
        "branch": branch,
        "moved": [
            {
                "scope": label,
                "source": str(source),
                "recovery_path": str(destination),
            }
            for label, source, destination in moved
        ],
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Move isolated onboarding data roots into recoverable locations."
    )
    parser.add_argument("mode", choices=("persona", "full"))
    args = parser.parse_args(argv)
    try:
        receipt = reset_sandbox(REPO_ROOT, args.mode)
    except SandboxResetError as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False))
        return 2
    print(json.dumps({"ok": True, "receipt": receipt}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
