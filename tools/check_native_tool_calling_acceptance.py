#!/usr/bin/env python3
"""Run the UPSP provider-native tool-calling acceptance chain."""
from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

TOOLS_ROOT = Path(__file__).resolve().parent
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

import native_tool_evidence_model  # noqa: E402
from check_native_tool_calling_gate import _run_subprocess_tree  # noqa: E402


DEFAULT_RECENT_ROUNDS = 10
DEFAULT_ALLOWED_FAILED_ROUNDS: tuple[str, ...] = ()
DEFAULT_COMMAND_TIMEOUT_SECONDS = 180
FULL_PYTEST_TIMEOUT_SECONDS = 420
TIMEOUT_EXIT_CODE = 124
DEFAULT_PYTEST_WORKERS = min(24, os.cpu_count() or 1)
DEFAULT_PYTEST_DIST = "worksteal"
PYTEST_DIST_CHOICES = ("loadscope", "loadfile", "worksteal")
DEFAULT_LATEST_REQUIRED_TOOL = "file_read"
DEFAULT_LATEST_REQUIRED_PROVIDER = "openai_responses"
DEFAULT_LATEST_REQUIRED_STATUS = "ok"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def _unique(values: list[str]) -> list[str]:
    return native_tool_evidence_model.unique(values)


def _byte_len(value: Any) -> int:
    return native_tool_evidence_model.byte_len(value)


def _parse_json_object(value: Any) -> dict[str, Any]:
    return native_tool_evidence_model.parse_json_object(value)


def _safe_list(value: Any) -> list[str]:
    return native_tool_evidence_model.safe_list(value, redact=False)


def _safe_dict(value: Any) -> dict[str, Any]:
    return native_tool_evidence_model.safe_dict(value, redact=False)


def _round_evidence_summary(parsed: dict[str, Any]) -> dict[str, Any]:
    latest = parsed.get("latest_positive_round")
    latest_block = latest if isinstance(latest, dict) else {}
    aggregate = parsed.get("aggregate")
    aggregate_block = aggregate if isinstance(aggregate, dict) else {}
    return {
        "ok": bool(parsed.get("ok")),
        "issues": _safe_list(parsed.get("issues")),
        "latest_positive_round": {
            "round_name": latest_block.get("round_name"),
            "round_num": latest_block.get("round_num"),
            "status": latest_block.get("status"),
            "providers": _safe_list(latest_block.get("providers")),
            "tool_ids": _safe_list(latest_block.get("tool_ids")),
            "tool_result_statuses": _safe_dict(
                latest_block.get("tool_result_statuses")),
        },
        "aggregate": {
            "providers": _safe_list(aggregate_block.get("providers")),
            "tool_ids": _safe_list(aggregate_block.get("tool_ids")),
            "tool_result_statuses": _safe_dict(
                aggregate_block.get("tool_result_statuses")),
            "tool_result_reasons": _safe_dict(
                aggregate_block.get("tool_result_reasons")),
        },
        "unexplained_failed_round_count": len(
            parsed.get("unexplained_failed_rounds") or []),
        "explained_failed_round_count": len(
            parsed.get("explained_failed_rounds") or []),
    }


def _native_gate_summary(parsed: dict[str, Any]) -> dict[str, Any]:
    checks = parsed.get("checks") if isinstance(parsed.get("checks"), dict) else {}
    recent = checks.get("recent_rounds") if isinstance(checks, dict) else {}
    recent_block = recent if isinstance(recent, dict) else {}
    round_check = checks.get("round") if isinstance(checks, dict) else {}
    round_block = round_check if isinstance(round_check, dict) else {}
    matrix = checks.get("matrix") if isinstance(checks, dict) else {}
    matrix_block = matrix if isinstance(matrix, dict) else {}
    return {
        "ok": bool(parsed.get("ok")),
        "issues": _safe_list(parsed.get("issues")),
        "matrix_status": matrix_block.get("status"),
        "round_status": round_block.get("status"),
        "recent_rounds_status": recent_block.get("status"),
        "recent_rounds": {
            "round_count": recent_block.get("round_count"),
            "tool_ids": _safe_list(recent_block.get("tool_ids")),
            "providers": _safe_list(recent_block.get("providers")),
            "tool_result_statuses": _safe_dict(
                recent_block.get("tool_result_statuses")),
            "tool_result_reasons": _safe_dict(
                recent_block.get("tool_result_reasons")),
            "matched_allowed_failed_rounds": _safe_list(
                recent_block.get("matched_allowed_failed_rounds")),
            "unused_allowed_failed_rounds": _safe_list(
                recent_block.get("unused_allowed_failed_rounds")),
            "stale_allowed_failed_rounds": _safe_list(
                recent_block.get("stale_allowed_failed_rounds")),
            "non_failing_allowed_failed_rounds": _safe_list(
                recent_block.get("non_failing_allowed_failed_rounds")),
        },
    }


def _high_risk_summary(parsed: dict[str, Any]) -> dict[str, Any]:
    cases = parsed.get("cases") if isinstance(parsed.get("cases"), list) else []
    failed_case_ids = []
    for case in cases:
        if not isinstance(case, dict):
            continue
        allowed = case.get("allowed")
        handler_called = case.get("handler_called")
        if allowed is False and handler_called is True:
            failed_case_ids.append(str(case.get("case_id") or "unknown"))
    return {
        "ok": bool(parsed.get("ok")),
        "issues": _safe_list(parsed.get("issues")),
        "case_count": len(cases),
        "failed_case_ids": failed_case_ids,
    }


def _parsed_summary(command_id: str, parsed: dict[str, Any]) -> dict[str, Any]:
    if not parsed:
        return {}
    if command_id == "consistency_audit":
        return {"summary": _safe_dict(parsed.get("summary"))}
    if command_id.startswith("native_gate_"):
        return _native_gate_summary(parsed)
    if command_id == "native_round_evidence_summary":
        return _round_evidence_summary(parsed)
    if command_id == "high_risk_general_tool_gate":
        return _high_risk_summary(parsed)
    return {
        "ok": parsed.get("ok") if isinstance(parsed.get("ok"), bool) else None,
        "issues": _safe_list(parsed.get("issues")),
    }


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run the UPSP provider-native tool-calling acceptance chain."
    )
    parser.add_argument("--skip-full-pytest", action="store_true")
    parser.add_argument("--serial-full-pytest", action="store_true")
    parser.add_argument(
        "--pytest-workers",
        type=_positive_int,
        default=DEFAULT_PYTEST_WORKERS,
        help="Number of pytest-xdist workers for the full pytest gate.",
    )
    parser.add_argument(
        "--pytest-dist",
        choices=PYTEST_DIST_CHOICES,
        default=DEFAULT_PYTEST_DIST,
        help="pytest-xdist distribution mode for the full pytest gate.",
    )
    parser.add_argument("--recent-rounds", type=_positive_int,
                        default=DEFAULT_RECENT_ROUNDS)
    parser.add_argument("--allow-failed-round", action="append", default=[])
    parser.add_argument("--latest-required-tool",
                        default=DEFAULT_LATEST_REQUIRED_TOOL)
    parser.add_argument("--latest-required-provider",
                        default=DEFAULT_LATEST_REQUIRED_PROVIDER)
    parser.add_argument("--latest-required-status",
                        default=DEFAULT_LATEST_REQUIRED_STATUS)
    parser.add_argument(
        "--command-timeout",
        type=_positive_int,
        default=None,
        help="Seconds before each child command is killed with its process tree.",
    )
    return parser.parse_args(argv)


def _allow_failed_rounds(args: argparse.Namespace) -> list[str]:
    if args.allow_failed_round:
        return _unique([str(item) for item in args.allow_failed_round])
    return list(DEFAULT_ALLOWED_FAILED_ROUNDS)


def build_command_specs(args: argparse.Namespace) -> list[dict[str, Any]]:
    root = repo_root()
    python = sys.executable
    allow_failed_rounds = _allow_failed_rounds(args)
    recent_command = [
        python,
        "-X",
        "utf8",
        "tools/check_native_tool_calling_gate.py",
        "--recent-rounds",
        str(args.recent_rounds),
    ]
    for item in allow_failed_rounds:
        recent_command.extend(["--allow-failed-round", item])
    evidence_summary_command = [
        python,
        "-X",
        "utf8",
        "tools/summarize_native_round_evidence.py",
        "--recent-rounds",
        str(args.recent_rounds),
        "--latest-required-tool",
        str(args.latest_required_tool),
        "--latest-required-provider",
        str(args.latest_required_provider),
        "--latest-required-status",
        str(args.latest_required_status),
    ]
    for item in allow_failed_rounds:
        evidence_summary_command.extend(["--allow-failed-round", item])
    specs = []
    if not args.skip_full_pytest:
        full_pytest_command = [python, "-m", "pytest", "OS/tests/"]
        if not args.serial_full_pytest:
            full_pytest_command.extend([
                "-n",
                str(args.pytest_workers),
                "--dist",
                str(args.pytest_dist),
                "--tb=short",
            ])
        specs.append({
            "id": "full_pytest",
            "command": full_pytest_command,
            "cwd": root / "UPSP",
            "cwd_label": "upsp_root",
            "timeout_seconds": FULL_PYTEST_TIMEOUT_SECONDS,
        })
    specs.extend([{
        "id": "consistency_audit",
        "command": [
            python,
            "-X",
            "utf8",
            "tools/audit_upsp_consistency.py",
            "--include-os-boundary",
        ],
        "cwd": root,
        "cwd_label": "repo_root",
    }, {
        "id": "native_gate_matrix",
        "command": [
            python,
            "-X",
            "utf8",
            "tools/check_native_tool_calling_gate.py",
        ],
        "cwd": root,
        "cwd_label": "repo_root",
    }, {
        "id": "native_gate_latest_native_positive",
        "command": [
            python,
            "-X",
            "utf8",
            "tools/check_native_tool_calling_gate.py",
            "--latest-round",
            "--require-tool",
            str(args.latest_required_tool),
            "--require-provider",
            str(args.latest_required_provider),
            "--require-tool-result-status",
            str(args.latest_required_status),
        ],
        "cwd": root,
        "cwd_label": "repo_root",
    }, {
        "id": "native_gate_recent_window",
        "command": recent_command,
        "cwd": root,
        "cwd_label": "repo_root",
    }, {
        "id": "native_round_evidence_summary",
        "command": evidence_summary_command,
        "cwd": root,
        "cwd_label": "repo_root",
    }, {
        "id": "high_risk_general_tool_gate",
        "command": [
            python,
            "-X",
            "utf8",
            "tools/check_high_risk_general_tool_gate.py",
        ],
        "cwd": root,
        "cwd_label": "repo_root",
    }])
    return specs


def _command_timeout_seconds(
    spec: dict[str, Any],
    args: argparse.Namespace,
) -> int:
    if args.command_timeout is not None:
        return int(args.command_timeout)
    return int(spec.get("timeout_seconds") or DEFAULT_COMMAND_TIMEOUT_SECONDS)


def _run_command(
    spec: dict[str, Any],
    runner: Callable[..., Any],
    timeout_seconds: int,
) -> dict[str, Any]:
    command = list(spec["command"])
    cwd = Path(spec["cwd"])
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
        return {
            "id": spec["id"],
            "command_id": spec["id"],
            "status": "timed_out",
            "exit_code": TIMEOUT_EXIT_CODE,
            "cwd_label": str(spec.get("cwd_label") or "repo_root"),
            "stdout_bytes": _byte_len(getattr(exc, "output", "")),
            "stderr_bytes": _byte_len(getattr(exc, "stderr", "")),
            "summary": {},
            "timed_out": True,
            "timeout_seconds": timeout_seconds,
        }
    except OSError:
        return {
            "id": spec["id"],
            "command_id": spec["id"],
            "status": "failed",
            "exit_code": 1,
            "cwd_label": str(spec.get("cwd_label") or "repo_root"),
            "stdout_bytes": 0,
            "stderr_bytes": 0,
            "summary": {},
            "timeout_seconds": timeout_seconds,
        }
    stdout = getattr(completed, "stdout", "")
    stderr = getattr(completed, "stderr", "")
    exit_code = int(getattr(completed, "returncode", 1))
    parsed = _parse_json_object(stdout)
    return {
        "id": spec["id"],
        "command_id": spec["id"],
        "status": "passed" if exit_code == 0 else "failed",
        "exit_code": exit_code,
        "cwd_label": str(spec.get("cwd_label") or "repo_root"),
        "stdout_bytes": _byte_len(stdout),
        "stderr_bytes": _byte_len(stderr),
        "summary": _parsed_summary(str(spec["id"]), parsed),
        "timeout_seconds": timeout_seconds,
    }


def run_acceptance(
    argv: list[str] | None = None,
    *,
    runner: Callable[..., Any] | None = None,
) -> dict[str, Any]:
    args = parse_args(argv)
    command_runner = runner or _run_subprocess_tree
    command_results = [
        _run_command(spec, command_runner, _command_timeout_seconds(spec, args))
        for spec in build_command_specs(args)
    ]
    issues = []
    for result in command_results:
        command_id = str(result["id"])
        if result.get("status") == "timed_out":
            issues.append(f"command_timed_out:{command_id}")
        elif result.get("status") != "passed":
            issues.append(f"command_failed:{command_id}")
    return {
        "ok": not issues,
        "schema_version": "native_tool_calling_acceptance.v1",
        "recent_rounds": args.recent_rounds,
        "allow_failed_rounds": _allow_failed_rounds(args),
        "commands": command_results,
        "issues": issues,
    }


def main(argv: list[str] | None = None) -> int:
    summary = run_acceptance(argv)
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
