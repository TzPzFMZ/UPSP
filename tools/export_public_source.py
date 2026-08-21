"""Export a tracked-only, privacy-bounded UPSP public source snapshot."""
from __future__ import annotations

import argparse
import hashlib
import io
import json
import re
import shutil
import subprocess
import tarfile
import tempfile
from pathlib import Path, PurePosixPath
from typing import Iterable


SCHEMA_VERSION = "upsp_public_source_manifest.v1"
MANIFEST_NAME = "PUBLIC_SOURCE_MANIFEST.json"

PUBLIC_ROOT_FILES = {
    ".editorconfig",
    ".gitattributes",
    ".gitignore",
    "LICENSE",
    "README.en.md",
    "README.md",
    "THIRD_PARTY_NOTICES.md",
    "UPSP_Base_DDS.md",
    "UPSP_CLI.cmd",
    "pyproject.toml",
    "pytest.ini",
}
PUBLIC_PREFIXES = (
    "UPSP/",
    "desktop/",
    "docs/public/",
)
PUBLIC_TOOLS = {
    "tools/analyze_prompt_prefix_cache.py",
    "tools/build_round_audit_static.py",
    "tools/build_windows_desktop.ps1",
    "tools/check_high_risk_general_tool_gate.py",
    "tools/check_native_tool_calling_acceptance.py",
    "tools/check_native_tool_calling_gate.py",
    "tools/check_protocol_tool_registration.py",
    "tools/export_public_source.py",
    "tools/inspect_native_tool_round.py",
    "tools/native_tool_evidence_model.py",
    "tools/replay_file_read_window.py",
    "tools/replay_material_retention.py",
    "tools/replay_prompt_cache_breakpoints.py",
    "tools/reset_onboarding_sandbox.py",
    "tools/round_context_acceptance.py",
    "tools/serve_docs_workbench.py",
    "tools/serve_round_audit.py",
    "tools/serve_round_live.py",
    "tools/serve_rules_workbench.py",
    "tools/serve_seed_gui.py",
    "tools/single_agent_survival_experiment.py",
    "tools/summarize_native_round_evidence.py",
    "tools/upsp_cli.py",
    "tools/upsp_cli_menu.ps1",
    "tools/workbench_server.py",
}
PRIVATE_TESTS = {
    "test_anti_conservative_hook.py",
    "test_codebase_governance_audit.py",
    "test_codex_efficiency_tools.py",
    "test_consistency_audit.py",
    "test_consistency_audit_scope.py",
    "test_current_anchor_checker.py",
    "test_dds_truth_audit.py",
    "test_dogfood_archive_inventory.py",
    "test_dogfood_result_report.py",
    "test_dogfood_runner_support.py",
    "test_dogfood_single_round_admission.py",
    "test_dogfood_visible_report.py",
    "test_encoding_guard.py",
    "test_locomo_benchmark.py",
    "test_locomo_failure_audit.py",
    "test_native_tooling_scripts.py",
    "test_prepare_dogfood_state.py",
    "test_prompt_cache_planner.py",
    "test_protocol_tool_registration_audit.py",
    "test_rules_workbench.py",
    "test_spec721_recovery_tool.py",
    "test_spec599_restore_portability.py",
    "test_visible_dogfood_context_profile.py",
    "test_visible_dogfood_entrypoint.py",
    "test_visible_dogfood_monitoring.py",
    "test_visible_dogfood_openai_reasoning.py",
    "test_visible_dogfood_popup_stop_loss.py",
}
FORBIDDEN_PREFIXES = (
    ".agents/",
    ".claude/",
    ".codex/",
    ".speckit/",
    ".tmp/",
    "docs/codex/",
    "draft/",
    "UPSP/OS/persona/",
)
FORBIDDEN_NAMES = {
    "AGENTS.md",
    "CODEX_AGENT_SESSION.md",
    "CODEX_CONTEXT_CHECKPOINT.md",
    "CODEX_MEMORY.md",
    "RUN_UPSP_VISIBLE_DOGFOOD.cmd",
}
FORBIDDEN_TEXT = (
    "C:" + "\\Users\\Lenovo",
    "C:" + "/Users/Lenovo",
    "D:" + "\\UPSP-VM",
    "D:" + "/UPSP-VM",
    "E:" + "\\AI_workspace",
    "E:" + "/AI_workspace",
    "9527" + "code.txt",
    "天" + "枢.txt",
)
SECRET_PATTERNS = (
    re.compile(r"(?<![A-Za-z0-9])sk-(?:proj-)?[A-Za-z0-9_-]{24,}"),
    re.compile(r"(?<![A-Za-z0-9])sk-ant-[A-Za-z0-9_-]{24,}"),
    re.compile(r"(?<![A-Za-z0-9])gh[pousr]_[A-Za-z0-9]{20,}"),
    re.compile(r"(?<![A-Za-z0-9])github_pat_[A-Za-z0-9_]{20,}"),
    re.compile(r"(?<![A-Za-z0-9])AIza[0-9A-Za-z_-]{30,}"),
    re.compile(r"(?<![A-Za-z0-9])xox[baprs]-[A-Za-z0-9-]{20,}"),
    re.compile(r"Bearer\s+[A-Za-z0-9._~-]{24,}", re.IGNORECASE),
    re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH )?PRIVATE KEY-----"),
)


class PublicSourceError(RuntimeError):
    """Public snapshot construction failed closed."""


def _git(repo_root: Path, *args: str, text: bool = True) -> str | bytes:
    options: dict[str, object] = {
        "check": False,
        "capture_output": True,
        "text": text,
    }
    if text:
        options["encoding"] = "utf-8"
    result = subprocess.run(["git", "-C", str(repo_root), *args], **options)
    if result.returncode:
        stderr = result.stderr if text else result.stderr.decode("utf-8", "replace")
        raise PublicSourceError(f"git_failed:{' '.join(args)}:{stderr.strip()}")
    return result.stdout


def resolve_commit(repo_root: Path, source_ref: str) -> str:
    return str(_git(repo_root, "rev-parse", "--verify", f"{source_ref}^{{commit}}")).strip()


def _tree_paths(repo_root: Path, commit: str) -> tuple[str, ...]:
    raw = _git(repo_root, "ls-tree", "-r", "-z", "--name-only", commit)
    return tuple(path for path in str(raw).split("\0") if path)


def is_public_path(relative: str) -> bool:
    path = PurePosixPath(relative)
    if path.is_absolute() or any(part in {"", ".", ".."} for part in path.parts):
        return False
    normalized = path.as_posix()
    if path.name in FORBIDDEN_NAMES:
        return False
    if any(normalized.startswith(prefix) for prefix in FORBIDDEN_PREFIXES):
        return False
    if normalized.startswith("UPSP/OS/tests/") and path.name in PRIVATE_TESTS:
        return False
    if normalized in PUBLIC_ROOT_FILES or normalized in PUBLIC_TOOLS:
        return True
    return any(normalized.startswith(prefix) for prefix in PUBLIC_PREFIXES)


def _scan_text(relative: str, data: bytes) -> None:
    text = data.decode("utf-8", "ignore")
    for marker in FORBIDDEN_TEXT:
        if marker.casefold() in text.casefold():
            raise PublicSourceError(f"private_text:{relative}:{marker}")
    for pattern in SECRET_PATTERNS:
        if pattern.search(text):
            raise PublicSourceError(f"secret_pattern:{relative}:{pattern.pattern}")


def _archive_roots(paths: Iterable[str]) -> list[str]:
    available = set(paths)
    roots = sorted(path for path in PUBLIC_ROOT_FILES if path in available)
    for prefix in ("UPSP/", "desktop/", "docs/public/", "tools/"):
        if any(path.startswith(prefix) for path in available):
            roots.append(prefix.rstrip("/"))
    return roots


def _write_manifest(output: Path, commit: str, records: list[dict[str, object]]) -> None:
    records.sort(key=lambda item: str(item["path"]))
    tree_digest = hashlib.sha256()
    for item in records:
        tree_digest.update(
            f"{item['path']}\0{item['bytes']}\0{item['sha256']}\n".encode("utf-8")
        )
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "source_commit": commit,
        "source_tree_sha256": tree_digest.hexdigest(),
        "file_count": len(records),
        "total_bytes": sum(int(item["bytes"]) for item in records),
        "files": records,
    }
    (output / MANIFEST_NAME).write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def export_public_source(repo_root: Path, source_ref: str, output: Path) -> dict[str, object]:
    repo_root = repo_root.resolve()
    output = output.resolve()
    if output.exists():
        raise PublicSourceError(f"output_exists:{output}")
    output.parent.mkdir(parents=True, exist_ok=True)

    commit = resolve_commit(repo_root, source_ref)
    paths = _tree_paths(repo_root, commit)
    selected = {path for path in paths if is_public_path(path)}
    if not selected:
        raise PublicSourceError("public_source_empty")

    archive = subprocess.run(
        [
            "git",
            "-C",
            str(repo_root),
            "archive",
            "--format=tar",
            commit,
            "--",
            *_archive_roots(paths),
        ],
        check=False,
        capture_output=True,
    )
    if archive.returncode:
        raise PublicSourceError(
            f"git_archive_failed:{archive.stderr.decode('utf-8', 'replace').strip()}"
        )

    staging = Path(tempfile.mkdtemp(prefix=".upsp-public-", dir=output.parent))
    records: list[dict[str, object]] = []
    found: set[str] = set()
    try:
        with tarfile.open(fileobj=io.BytesIO(archive.stdout), mode="r:") as bundle:
            for member in bundle:
                relative = PurePosixPath(member.name).as_posix()
                if relative not in selected:
                    continue
                if not member.isfile():
                    raise PublicSourceError(f"unsupported_archive_entry:{relative}")
                stream = bundle.extractfile(member)
                if stream is None:
                    raise PublicSourceError(f"archive_read_failed:{relative}")
                data = stream.read()
                _scan_text(relative, data)
                target = (staging / Path(*PurePosixPath(relative).parts)).resolve()
                if staging not in target.parents:
                    raise PublicSourceError(f"path_escape:{relative}")
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(data)
                found.add(relative)
                records.append(
                    {
                        "path": relative,
                        "bytes": len(data),
                        "sha256": hashlib.sha256(data).hexdigest(),
                    }
                )
        missing = sorted(selected - found)
        if missing:
            raise PublicSourceError(f"archive_missing:{','.join(missing[:8])}")
        _write_manifest(staging, commit, records)
        staging.rename(output)
    except Exception:
        shutil.rmtree(staging, ignore_errors=True)
        raise

    return {
        "schema_version": SCHEMA_VERSION,
        "source_commit": commit,
        "output": str(output),
        "file_count": len(records),
        "total_bytes": sum(int(item["bytes"]) for item in records),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--source-ref", default="HEAD")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    try:
        result = export_public_source(args.repo, args.source_ref, args.output)
    except PublicSourceError as error:
        print(json.dumps({"status": "error", "error": str(error)}, ensure_ascii=False))
        return 1
    print(json.dumps({"status": "ok", **result}, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
