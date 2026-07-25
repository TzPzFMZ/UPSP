#!/usr/bin/env python3
"""Run the provider-native tool-calling regression gate."""
from __future__ import annotations

import argparse
import json
import os
import re
import signal
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

TOOLS_ROOT = Path(__file__).resolve().parent
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

import native_tool_evidence_model  # noqa: E402


MATRIX_TEST_PATHS = (
    "OS/tests/test_native_round_inspector.py",
    "OS/tests/test_native_tool_calls.py",
)
MATRIX_SELECTOR = "spec143 or normalizes_non_native_tool_role or mixed_native"
DEFAULT_COMMAND_TIMEOUT_SECONDS = 180
TIMEOUT_EXIT_CODE = 124
SENSITIVE_MARKERS = (
    "arguments_json",
    "secret",
    "password",
    "token",
    "api_key",
    "sk-",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_round_dir(root: Path | None = None) -> Path:
    return native_tool_evidence_model.default_round_dir(root or repo_root())


def _round_num_from_path(path: Path) -> int | None:
    return native_tool_evidence_model.round_num_from_path(path)


def _unique(values: list[str]) -> list[str]:
    return native_tool_evidence_model.unique(values)


def _byte_len(value: Any) -> int:
    return native_tool_evidence_model.byte_len(value)


def _safe_text(value: Any) -> str:
    return native_tool_evidence_model.safe_text(value)


def _safe_list(values: Any) -> list[str]:
    return native_tool_evidence_model.safe_list(values)


def _safe_dict(values: Any) -> dict[str, Any]:
    return native_tool_evidence_model.safe_dict(values)


def _safe_bool(value: Any) -> bool:
    return native_tool_evidence_model.safe_bool(value)


def _merge_count_dict(target: dict[str, int], values: Any) -> None:
    native_tool_evidence_model.merge_count_dict(target, values)


def _parse_json_object(text: Any) -> dict[str, Any]:
    return native_tool_evidence_model.parse_json_object(text)


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run UPSP provider-native tool-calling regression checks."
    )
    parser.add_argument("--skip-matrix", action="store_true")
    parser.add_argument("--matrix-selector", default=MATRIX_SELECTOR)
    parser.add_argument("--round-file")
    parser.add_argument("--latest-round", action="store_true")
    parser.add_argument("--recent-rounds", type=_positive_int)
    parser.add_argument("--round-dir")
    parser.add_argument("--require-tool", action="append", default=[])
    parser.add_argument("--require-provider")
    parser.add_argument("--require-tool-result-status")
    parser.add_argument("--require-reading-dogfood", action="store_true")
    parser.add_argument("--require-dogfood-label", default="读书轮")
    parser.add_argument("--require-read-only-tools-only", action="store_true")
    parser.add_argument(
        "--require-settlement-quality",
        action="store_true",
        help="Reject retired or invalid reaction_finalize settlement fields.",
    )
    parser.add_argument("--allow-round-open", action="store_true")
    parser.add_argument("--allow-runtime-audit-missing", action="store_true")
    parser.add_argument("--allow-legacy-execution", action="store_true")
    parser.add_argument(
        "--allow-failed-round",
        action="append",
        default=[],
        help="Allow a known failed recent round by number, name, or path.",
    )
    parser.add_argument(
        "--fail-on-unused-allow-failed-round",
        action="store_true",
        help="Fail when any requested allow-failed-round does not explain a failed recent round.",
    )
    parser.add_argument(
        "--command-timeout",
        type=_positive_int,
        default=None,
        help="Seconds before each child command is killed with its process tree.",
    )
    return parser.parse_args(argv)


def _command_timeout_seconds(args: argparse.Namespace) -> int:
    if args.command_timeout is not None:
        return int(args.command_timeout)
    return DEFAULT_COMMAND_TIMEOUT_SECONDS


def build_matrix_command(args: argparse.Namespace) -> tuple[list[str], Path]:
    root = repo_root()
    command = [
        sys.executable,
        "-m",
        "pytest",
        *MATRIX_TEST_PATHS,
        "-q",
        "-k",
        args.matrix_selector,
    ]
    return command, root / "UPSP"


def _append_common_round_requirements(
    command: list[str],
    args: argparse.Namespace,
    *,
    include_tool_provider: bool,
) -> list[str]:
    if not args.allow_round_open:
        command.append("--require-round-closed")
    if not args.allow_runtime_audit_missing:
        command.append("--require-runtime-audit-ok")
    if args.allow_legacy_execution:
        command.append("--allow-legacy-execution")
    if args.require_tool_result_status:
        command.extend([
            "--require-tool-result-status",
            args.require_tool_result_status,
        ])
    if args.require_reading_dogfood:
        command.append("--require-reading-dogfood")
        command.extend(["--require-dogfood-label", args.require_dogfood_label])
    if args.require_read_only_tools_only:
        command.append("--require-read-only-tools-only")
    if args.require_settlement_quality:
        command.append("--require-settlement-quality")
    if include_tool_provider:
        for tool_id in args.require_tool:
            command.extend(["--require-tool", tool_id])
        if args.require_provider:
            command.extend(["--require-provider", args.require_provider])
    return command


def build_round_command(
    args: argparse.Namespace,
    *,
    round_file: str | Path | None = None,
    latest: bool = False,
    include_tool_provider: bool = True,
) -> tuple[list[str], Path]:
    root = repo_root()
    command = [
        sys.executable,
        "-X",
        "utf8",
        str(root / "tools" / "inspect_native_tool_round.py"),
    ]
    if round_file is not None:
        command.extend(["--round-file", str(round_file)])
    elif latest:
        command.append("--latest")
        command.extend(["--round-dir", str(args.round_dir or default_round_dir(root))])
    else:
        raise ValueError("round_file or latest is required")
    _append_common_round_requirements(
        command,
        args,
        include_tool_provider=include_tool_provider,
    )
    return command, root


def find_recent_rounds(round_dir: str | Path, limit: int) -> list[Path]:
    return native_tool_evidence_model.find_recent_rounds(round_dir, limit)


def _round_allow_keys(path: Path) -> set[str]:
    return native_tool_evidence_model.round_allow_keys(path)


def _requested_allowed_failed_rounds(args: argparse.Namespace) -> list[str]:
    return _unique([str(item) for item in args.allow_failed_round])


def _matched_allowed_failed_rounds(
    path: Path,
    args: argparse.Namespace,
) -> list[str]:
    keys = _round_allow_keys(path)
    return [item for item in _requested_allowed_failed_rounds(args)
            if item in keys]


def _is_allowed_failed_round(path: Path, args: argparse.Namespace) -> bool:
    return bool(_matched_allowed_failed_rounds(path, args))


def _terminate_process_tree(process: subprocess.Popen) -> None:
    if process.poll() is not None:
        return
    if os.name == "nt":
        try:
            subprocess.run(
                ["taskkill", "/PID", str(process.pid), "/T", "/F"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                timeout=10,
                check=False,
            )
            return
        except Exception:
            pass
    else:
        try:
            os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            process.wait(timeout=5)
            return
        except Exception:
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
                return
            except Exception:
                pass
    try:
        process.kill()
    except Exception:
        pass


def _run_subprocess_tree(command: list[str], **kwargs: Any) -> subprocess.CompletedProcess:
    timeout = kwargs.pop("timeout", None)
    capture_output = bool(kwargs.pop("capture_output", False))
    stdout_target = subprocess.PIPE if capture_output else None
    stderr_target = subprocess.PIPE if capture_output else None
    creationflags = int(kwargs.pop("creationflags", 0) or 0)
    popen_kwargs = dict(kwargs)
    if os.name == "nt":
        creationflags |= getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)
        popen_kwargs["creationflags"] = creationflags
    else:
        popen_kwargs["start_new_session"] = True
    process = subprocess.Popen(
        command,
        stdout=stdout_target,
        stderr=stderr_target,
        **popen_kwargs,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout)
    except subprocess.TimeoutExpired as exc:
        _terminate_process_tree(process)
        try:
            stdout, stderr = process.communicate(timeout=10)
        except Exception:
            stdout = exc.output
            stderr = exc.stderr
        raise subprocess.TimeoutExpired(
            command,
            timeout,
            output=stdout if stdout is not None else exc.output,
            stderr=stderr if stderr is not None else exc.stderr,
        ) from exc
    return subprocess.CompletedProcess(
        command,
        process.returncode,
        stdout=stdout,
        stderr=stderr,
    )


def _run_command(
    command: list[str],
    cwd: Path,
    runner: Callable[..., Any],
    timeout_seconds: int,
) -> tuple[Any, dict[str, Any]]:
    try:
        completed = runner(
            command,
            cwd=str(cwd),
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=timeout_seconds,
        )
    except subprocess.TimeoutExpired as exc:
        block = {
            "status": "timed_out",
            "exit_code": TIMEOUT_EXIT_CODE,
            "command": command,
            "cwd": str(cwd),
            "stdout_bytes": _byte_len(getattr(exc, "output", "")),
            "stderr_bytes": _byte_len(getattr(exc, "stderr", "")),
            "timed_out": True,
            "timeout_seconds": timeout_seconds,
        }
        return None, block
    except OSError:
        block = {
            "status": "failed",
            "exit_code": 1,
            "command": command,
            "cwd": str(cwd),
            "stdout_bytes": 0,
            "stderr_bytes": 0,
            "timeout_seconds": timeout_seconds,
        }
        return None, block
    exit_code = int(getattr(completed, "returncode", 1))
    block = {
        "status": "passed" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "command": command,
        "cwd": str(cwd),
        "stdout_bytes": _byte_len(getattr(completed, "stdout", "")),
        "stderr_bytes": _byte_len(getattr(completed, "stderr", "")),
        "timeout_seconds": timeout_seconds,
    }
    return completed, block


def run_matrix_check(
    args: argparse.Namespace,
    runner: Callable[..., Any],
) -> dict[str, Any]:
    if args.skip_matrix:
        return {"status": "skipped"}
    command, cwd = build_matrix_command(args)
    _, block = _run_command(
        command,
        cwd,
        runner,
        _command_timeout_seconds(args),
    )
    return block


def _round_block_from_completed(
    completed: Any,
    block: dict[str, Any],
) -> dict[str, Any]:
    parsed = _parse_json_object(getattr(completed, "stdout", "") if completed else "")
    block.update({
        "round_file": parsed.get("round_file"),
        "round_num": parsed.get("round_num"),
        "providers": _safe_list(parsed.get("providers")),
        "tool_ids": _safe_list(parsed.get("tool_ids")),
        "tool_result_statuses": _safe_dict(
            parsed.get("tool_result_statuses")),
        "tool_result_reasons": _safe_dict(
            parsed.get("tool_result_reasons")),
        "dogfood_label": _safe_text(parsed.get("dogfood_label")),
        "reading_dogfood": _safe_bool(parsed.get("reading_dogfood")),
        "read_only_tool_ids": _safe_list(parsed.get("read_only_tool_ids")),
        "non_read_only_tool_ids": _safe_list(
            parsed.get("non_read_only_tool_ids")),
        "tool_classes": _safe_dict(parsed.get("tool_classes")),
        "tool_families": _safe_dict(parsed.get("tool_families")),
        "settlement_issues": _safe_list(parsed.get("settlement_issues")),
        "issues": _safe_list(parsed.get("issues")),
    })
    if parsed and parsed.get("ok") is False:
        block["status"] = "failed"
        block["exit_code"] = block["exit_code"] or 1
    return block


def run_single_round_check(
    args: argparse.Namespace,
    runner: Callable[..., Any],
) -> dict[str, Any]:
    if not args.round_file and not args.latest_round:
        return {"status": "skipped"}
    command, cwd = build_round_command(
        args,
        round_file=args.round_file,
        latest=not args.round_file,
        include_tool_provider=True,
    )
    completed, block = _run_command(
        command,
        cwd,
        runner,
        _command_timeout_seconds(args),
    )
    return _round_block_from_completed(completed, block)


def run_recent_round_checks(
    args: argparse.Namespace,
    runner: Callable[..., Any],
) -> dict[str, Any]:
    if not args.recent_rounds:
        return {"status": "skipped"}

    root = repo_root()
    round_dir = Path(args.round_dir or default_round_dir(root))
    rounds = find_recent_rounds(round_dir, args.recent_rounds)
    requested_allowed_failed_rounds = _requested_allowed_failed_rounds(args)
    block: dict[str, Any] = {
        "status": "passed",
        "round_count": len(rounds),
        "round_files": [str(path) for path in rounds],
        "providers": [],
        "tool_ids": [],
        "read_only_tool_ids": [],
        "non_read_only_tool_ids": [],
        "reading_dogfood_rounds": 0,
        "tool_classes": {},
        "tool_families": {},
        "tool_result_statuses": {},
        "tool_result_reasons": {},
        "settlement_issues": [],
        "failed_rounds": [],
        "allowed_failed_rounds": [],
        "requested_allowed_failed_rounds": requested_allowed_failed_rounds,
        "matched_allowed_failed_rounds": [],
        "unused_allowed_failed_rounds": [],
        "stale_allowed_failed_rounds": [],
        "non_failing_allowed_failed_rounds": [],
    }
    if not rounds:
        block["status"] = "failed"
        block["issues"] = ["no_recent_rounds_found"]
        return block

    providers: list[str] = []
    tool_ids: list[str] = []
    read_only_tool_ids: list[str] = []
    non_read_only_tool_ids: list[str] = []
    reading_dogfood_rounds = 0
    tool_classes: dict[str, Any] = {}
    tool_families: dict[str, Any] = {}
    tool_result_statuses: dict[str, int] = {}
    tool_result_reasons: dict[str, int] = {}
    settlement_issues: list[str] = []
    failed_rounds: list[dict[str, Any]] = []
    scanned_allowed_matches: list[str] = []
    matched_allowed_matches: list[str] = []
    for path in rounds:
        allowed_matches = _matched_allowed_failed_rounds(path, args)
        scanned_allowed_matches.extend(allowed_matches)
        command, cwd = build_round_command(
            args,
            round_file=path,
            include_tool_provider=False,
        )
        completed, round_block = _run_command(
            command,
            cwd,
            runner,
            _command_timeout_seconds(args),
        )
        parsed = _parse_json_object(
            getattr(completed, "stdout", "") if completed else "")
        providers.extend(_safe_list(parsed.get("providers")))
        tool_ids.extend(_safe_list(parsed.get("tool_ids")))
        read_only_tool_ids.extend(_safe_list(parsed.get("read_only_tool_ids")))
        non_read_only_tool_ids.extend(_safe_list(
            parsed.get("non_read_only_tool_ids")))
        if _safe_bool(parsed.get("reading_dogfood")):
            reading_dogfood_rounds += 1
        tool_classes.update(_safe_dict(parsed.get("tool_classes")))
        tool_families.update(_safe_dict(parsed.get("tool_families")))
        parsed_tool_ids = set(_safe_list(parsed.get("tool_ids")))
        parsed_providers = set(_safe_list(parsed.get("providers")))
        parsed_issues = _safe_list(parsed.get("issues"))
        settlement_issues.extend(_safe_list(parsed.get("settlement_issues")))
        _merge_count_dict(
            tool_result_statuses,
            parsed.get("tool_result_statuses"),
        )
        _merge_count_dict(
            tool_result_reasons,
            parsed.get("tool_result_reasons"),
        )
        round_failed = round_block["status"] != "passed" or parsed.get("ok") is False
        if round_failed and allowed_matches:
            block["allowed_failed_rounds"].append(str(path))
            matched_allowed_matches.extend(allowed_matches)
            continue
        per_round_issues: list[str] = list(parsed_issues)
        if args.require_tool_result_status:
            for tool_id in args.require_tool:
                if tool_id not in parsed_tool_ids:
                    per_round_issues.append(
                        f"missing_required_tool_in_round:{tool_id}"
                    )
            if args.require_provider and args.require_provider not in parsed_providers:
                per_round_issues.append(
                    f"missing_required_provider_in_round:{args.require_provider}"
                )
        if per_round_issues and allowed_matches:
            block["allowed_failed_rounds"].append(str(path))
            matched_allowed_matches.extend(allowed_matches)
            continue
        if per_round_issues:
            failed_rounds.append({
                "round_file": str(path),
                "exit_code": round_block.get("exit_code"),
                "issues": per_round_issues,
            })
            continue
        if round_failed:
            failed_rounds.append({
                "round_file": str(path),
                "exit_code": round_block.get("exit_code"),
                "issues": parsed_issues,
            })

    block["providers"] = _unique(providers)
    block["tool_ids"] = _unique(tool_ids)
    block["read_only_tool_ids"] = _unique(read_only_tool_ids)
    block["non_read_only_tool_ids"] = _unique(non_read_only_tool_ids)
    block["reading_dogfood_rounds"] = reading_dogfood_rounds
    block["tool_classes"] = tool_classes
    block["tool_families"] = tool_families
    block["tool_result_statuses"] = tool_result_statuses
    block["tool_result_reasons"] = tool_result_reasons
    block["settlement_issues"] = _unique(settlement_issues)
    block["failed_rounds"] = failed_rounds
    matched_allowed = _unique(matched_allowed_matches)
    scanned_allowed = _unique(scanned_allowed_matches)
    block["matched_allowed_failed_rounds"] = matched_allowed
    block["unused_allowed_failed_rounds"] = [
        item for item in requested_allowed_failed_rounds
        if item not in set(matched_allowed)
    ]
    block["stale_allowed_failed_rounds"] = [
        item for item in requested_allowed_failed_rounds
        if item not in set(scanned_allowed)
    ]
    block["non_failing_allowed_failed_rounds"] = [
        item for item in scanned_allowed
        if item not in set(matched_allowed)
    ]
    issues: list[str] = []
    for tool_id in args.require_tool:
        if tool_id not in block["tool_ids"]:
            issues.append(f"missing_required_tool:{tool_id}")
    if args.require_provider and args.require_provider not in block["providers"]:
        issues.append(f"missing_required_provider:{args.require_provider}")
    if failed_rounds:
        issues.append("recent_round_failed")
    if (args.fail_on_unused_allow_failed_round
            and block["unused_allowed_failed_rounds"]):
        issues.append("unused_allowed_failed_round")
    if issues:
        block["status"] = "failed"
        block["issues"] = issues
    return block


def run_gate(
    argv: list[str] | None = None,
    *,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    args = parse_args(argv)
    command_runner = runner or _run_subprocess_tree
    checks = {
        "matrix": run_matrix_check(args, command_runner),
        "round": run_single_round_check(args, command_runner),
        "recent_rounds": run_recent_round_checks(args, command_runner),
    }
    issues: list[str] = []
    if checks["matrix"].get("status") in ("failed", "timed_out"):
        issues.append("matrix_failed")
    if checks["round"].get("status") in ("failed", "timed_out"):
        issues.append("round_failed")
    if checks["recent_rounds"].get("status") in ("failed", "timed_out"):
        issues.extend(checks["recent_rounds"].get("issues") or ["recent_rounds_failed"])
    issues = _unique([_safe_text(issue) for issue in issues])
    return {
        "ok": not issues,
        "checks": checks,
        "issues": issues,
    }


def main(argv: list[str] | None = None) -> int:
    summary = run_gate(argv)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
