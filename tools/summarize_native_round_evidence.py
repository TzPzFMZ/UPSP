#!/usr/bin/env python3
"""Build a safe handoff summary for provider-native round evidence."""
from __future__ import annotations

import argparse
import importlib.util
import json
import re
import sys
from pathlib import Path
from typing import Any, Callable

TOOLS_ROOT = Path(__file__).resolve().parent
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))

import native_tool_evidence_model  # noqa: E402


SCHEMA_VERSION = "native_round_evidence_summary.v1"
DEFAULT_DOGFOOD_LABEL = "读书轮"
DEFAULT_RECENT_ROUNDS = 10
DEFAULT_ALLOWED_FAILED_ROUNDS: tuple[str, ...] = ()
SUCCESS_TOOL_RESULT_STATUSES = {"ok", "accepted", "applied"}
REQUIRED_LATEST_TOOL = "file_read"
REQUIRED_LATEST_PROVIDER = "openai_responses"
REQUIRED_LATEST_TOOL_RESULT_STATUS = "ok"


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_round_dir(root: Path | None = None) -> Path:
    return native_tool_evidence_model.default_round_dir(root or repo_root())


def _load_tool_module(module_name: str) -> Any:
    path = repo_root() / "tools" / f"{module_name}.py"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load tool module: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _round_num_from_path(path: Path) -> int | None:
    return native_tool_evidence_model.round_num_from_path(path)


def _unique(values: list[str]) -> list[str]:
    return native_tool_evidence_model.unique(values)


def _safe_text(value: Any) -> str:
    return native_tool_evidence_model.safe_plain_text(value)


def _safe_bool(value: Any) -> bool:
    return native_tool_evidence_model.safe_bool(value)


def _safe_list(values: Any) -> list[str]:
    return _unique(native_tool_evidence_model.safe_list(values, redact=False))


def _safe_count_dict(values: Any) -> dict[str, int]:
    return native_tool_evidence_model.safe_count_dict(values)


def _safe_text_dict(values: Any) -> dict[str, str]:
    return {
        key: str(value)
        for key, value in native_tool_evidence_model.safe_dict(
            values,
            redact=False,
        ).items()
    }


def _safe_issue(value: Any) -> str:
    text = _safe_text(value)
    if text.startswith("tool_result_status_not_"):
        parts = text.split(":")
        if len(parts) >= 4:
            return ":".join([parts[0], parts[1], parts[-1]])
    text = re.sub(r":call_[^:]+", "", text)
    return text


def _safe_issue_list(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return _unique([_safe_issue(value) for value in values])


def _round_allow_keys(path: Path) -> set[str]:
    keys = native_tool_evidence_model.round_allow_keys(path)
    return {key for key in keys if key != str(path)}


def _is_allowed_round(path: Path, allow_failed_rounds: set[str]) -> bool:
    return bool(_round_allow_keys(path) & allow_failed_rounds)


def _safe_round_summary(path: Path, parsed: dict[str, Any]) -> dict[str, Any]:
    round_file = parsed.get("round_file")
    round_name = Path(str(round_file)).name if round_file else path.name
    round_num = parsed.get("round_num")
    if round_num is None:
        round_num = _round_num_from_path(path)
    return {
        "round_name": round_name,
        "round_num": round_num,
        "ok": _safe_bool(parsed.get("ok")),
        "providers": _safe_list(parsed.get("providers")),
        "tool_ids": _safe_list(parsed.get("tool_ids")),
        "tool_result_statuses": _safe_count_dict(
            parsed.get("tool_result_statuses")),
        "tool_result_reasons": _safe_count_dict(
            parsed.get("tool_result_reasons")),
        "dogfood_label": _safe_text(parsed.get("dogfood_label")),
        "reading_dogfood": _safe_bool(parsed.get("reading_dogfood")),
        "read_only_tool_ids": _safe_list(parsed.get("read_only_tool_ids")),
        "non_read_only_tool_ids": _safe_list(
            parsed.get("non_read_only_tool_ids")),
        "tool_classes": _safe_text_dict(parsed.get("tool_classes")),
        "tool_families": _safe_text_dict(parsed.get("tool_families")),
        "runtime_audit_status": _safe_text(parsed.get("runtime_audit_status")),
        "round_closed_status": _safe_text(parsed.get("round_closed_status")),
        "settlement_issues": _safe_issue_list(parsed.get("settlement_issues")),
        "issues": _safe_issue_list(parsed.get("issues")),
    }


def _has_non_ok_tool_result(parsed: dict[str, Any]) -> bool:
    statuses = _safe_count_dict(parsed.get("tool_result_statuses"))
    return any(
        status not in SUCCESS_TOOL_RESULT_STATUSES and count > 0
        for status, count in statuses.items()
    )


def _failure_kinds(parsed: dict[str, Any]) -> list[str]:
    kinds: list[str] = []
    if parsed.get("ok") is False:
        kinds.append("round_check_failed")
    if _has_non_ok_tool_result(parsed):
        kinds.append("non_ok_tool_result")
    return kinds


def _merge_count_dict(target: dict[str, int], values: dict[str, int]) -> None:
    for key, value in values.items():
        target[key] = target.get(key, 0) + value


def _default_find_recent_rounds(round_dir: Path, limit: int) -> list[Path]:
    return native_tool_evidence_model.find_recent_rounds(round_dir, limit)


def _default_inspect_round(round_file: Path, **kwargs: Any) -> dict[str, Any]:
    inspector = _load_tool_module("inspect_native_tool_round")
    return inspector.inspect_round_file(round_file, **kwargs)


def _suggested_recent_gate_command(recent_rounds: int,
                                   allow_failed_rounds: list[str]) -> str:
    command = [
        "python",
        "-X",
        "utf8",
        "tools/check_native_tool_calling_gate.py",
        "--recent-rounds",
        str(recent_rounds),
    ]
    for item in allow_failed_rounds:
        command.extend(["--allow-failed-round", item])
    return " ".join(command)


def _suggested_latest_gate_command(
        latest_required_tool: str,
        latest_required_provider: str,
        latest_required_tool_result_status: str) -> str:
    return " ".join([
        "python",
        "-X",
        "utf8",
        "tools/check_native_tool_calling_gate.py",
        "--latest-round",
        "--require-tool",
        latest_required_tool,
        "--require-provider",
        latest_required_provider,
        "--require-tool-result-status",
        latest_required_tool_result_status,
    ])


def _build_failure_entry(path: Path, safe_summary: dict[str, Any],
                         parsed: dict[str, Any]) -> dict[str, Any]:
    entry = {
        "round_name": safe_summary["round_name"],
        "round_num": safe_summary["round_num"],
        "failure_kinds": _failure_kinds(parsed),
        "tool_ids": safe_summary["tool_ids"],
        "tool_result_statuses": safe_summary["tool_result_statuses"],
        "tool_result_reasons": safe_summary["tool_result_reasons"],
        "issues": safe_summary["issues"],
    }
    if not entry["round_name"]:
        entry["round_name"] = path.name
    return entry


def build_evidence_summary(
    *,
    round_dir: str | Path | None = None,
    recent_rounds: int = DEFAULT_RECENT_ROUNDS,
    allow_failed_rounds: list[str] | None = None,
    dogfood_label: str = DEFAULT_DOGFOOD_LABEL,
    latest_required_tool: str = REQUIRED_LATEST_TOOL,
    latest_required_provider: str = REQUIRED_LATEST_PROVIDER,
    latest_required_tool_result_status: str = REQUIRED_LATEST_TOOL_RESULT_STATUS,
    inspect_func: Callable[..., dict[str, Any]] | None = None,
    find_recent_func: Callable[[Path, int], list[Path]] | None = None,
) -> dict[str, Any]:
    round_dir_path = Path(round_dir) if round_dir else default_round_dir()
    allow_list = _unique(list(allow_failed_rounds)
                         if allow_failed_rounds is not None
                         else list(DEFAULT_ALLOWED_FAILED_ROUNDS))
    allow_set = set(allow_list)
    inspect = inspect_func or _default_inspect_round
    find_recent = find_recent_func or _default_find_recent_rounds

    rounds = find_recent(round_dir_path, recent_rounds)
    summary: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "ok": True,
        "round_dir_name": round_dir_path.name,
        "recent_window": recent_rounds,
        "allowed_failed_rounds": allow_list,
        "latest_positive_round": None,
        "recent_rounds": [],
        "business_failed_rounds": [],
        "explained_failed_rounds": [],
        "unexplained_failed_rounds": [],
        "aggregate": {
            "providers": [],
            "tool_ids": [],
            "read_only_tool_ids": [],
            "non_read_only_tool_ids": [],
            "reading_dogfood_rounds": 0,
            "tool_result_statuses": {},
            "tool_result_reasons": {},
        },
        "suggested_commands": {
            "latest_positive_gate": _suggested_latest_gate_command(
                latest_required_tool,
                latest_required_provider,
                latest_required_tool_result_status),
            "recent_gate": _suggested_recent_gate_command(
                recent_rounds,
                allow_list),
        },
        "issues": [],
    }
    if not rounds:
        summary["ok"] = False
        summary["issues"] = ["no_recent_rounds_found"]
        return summary

    providers: list[str] = []
    tool_ids: list[str] = []
    read_only_tool_ids: list[str] = []
    non_read_only_tool_ids: list[str] = []
    tool_result_statuses: dict[str, int] = {}
    tool_result_reasons: dict[str, int] = {}
    settlement_issues: list[str] = []
    reading_dogfood_rounds = 0

    latest_path = rounds[-1]
    latest_parsed = inspect(
        latest_path,
        required_tools=[latest_required_tool],
        required_provider=latest_required_provider,
        required_tool_result_status=latest_required_tool_result_status,
        require_reading_dogfood=False,
        dogfood_label=dogfood_label,
        require_read_only_tools_only=False,
        require_round_closed=True,
        require_runtime_audit_ok=True,
    )
    latest_safe = _safe_round_summary(latest_path, latest_parsed)
    latest_safe["status"] = "passed" if latest_parsed.get("ok") is True else "failed"
    summary["latest_positive_round"] = latest_safe
    if latest_parsed.get("ok") is not True:
        summary["issues"].append("latest_positive_round_failed")

    for path in rounds:
        parsed = inspect(
            path,
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )
        safe = _safe_round_summary(path, parsed)
        summary["recent_rounds"].append(safe)
        providers.extend(safe["providers"])
        tool_ids.extend(safe["tool_ids"])
        read_only_tool_ids.extend(safe["read_only_tool_ids"])
        non_read_only_tool_ids.extend(safe["non_read_only_tool_ids"])
        settlement_issues.extend(safe["settlement_issues"])
        if safe["reading_dogfood"]:
            reading_dogfood_rounds += 1
        _merge_count_dict(tool_result_statuses, safe["tool_result_statuses"])
        _merge_count_dict(tool_result_reasons, safe["tool_result_reasons"])

        if not _failure_kinds(parsed):
            continue
        failure_entry = _build_failure_entry(path, safe, parsed)
        summary["business_failed_rounds"].append(failure_entry)
        if _is_allowed_round(path, allow_set):
            summary["explained_failed_rounds"].append(failure_entry)
        else:
            summary["unexplained_failed_rounds"].append(failure_entry)

    summary["aggregate"] = {
        "providers": _unique(providers),
        "tool_ids": _unique(tool_ids),
        "read_only_tool_ids": _unique(read_only_tool_ids),
        "non_read_only_tool_ids": _unique(non_read_only_tool_ids),
        "reading_dogfood_rounds": reading_dogfood_rounds,
        "tool_result_statuses": tool_result_statuses,
        "tool_result_reasons": tool_result_reasons,
        "settlement_issues": _unique(settlement_issues),
    }
    if summary["unexplained_failed_rounds"]:
        summary["issues"].append("unexplained_recent_round_failed")
    summary["issues"] = _unique(summary["issues"])
    summary["ok"] = not summary["issues"]
    return summary


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("must be a positive integer")
    return parsed


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Summarize safe provider-native round evidence for handoff."
    )
    parser.add_argument("--round-dir")
    parser.add_argument("--recent-rounds", type=_positive_int,
                        default=DEFAULT_RECENT_ROUNDS)
    parser.add_argument("--allow-failed-round", action="append", default=[])
    parser.add_argument("--no-default-allow-failed-rounds", action="store_true")
    parser.add_argument("--require-dogfood-label", default=DEFAULT_DOGFOOD_LABEL)
    parser.add_argument("--latest-required-tool", default=REQUIRED_LATEST_TOOL)
    parser.add_argument("--latest-required-provider",
                        default=REQUIRED_LATEST_PROVIDER)
    parser.add_argument("--latest-required-status",
                        default=REQUIRED_LATEST_TOOL_RESULT_STATUS)
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    allowed = list(args.allow_failed_round)
    if not args.no_default_allow_failed_rounds:
        allowed = _unique(list(DEFAULT_ALLOWED_FAILED_ROUNDS) + allowed)
    summary = build_evidence_summary(
        round_dir=args.round_dir,
        recent_rounds=args.recent_rounds,
        allow_failed_rounds=allowed,
        dogfood_label=args.require_dogfood_label,
        latest_required_tool=args.latest_required_tool,
        latest_required_provider=args.latest_required_provider,
        latest_required_tool_result_status=args.latest_required_status,
    )
    print(json.dumps(summary, ensure_ascii=False, indent=2, sort_keys=True))
    return 0 if summary.get("ok") else 1


if __name__ == "__main__":
    raise SystemExit(main())
