#!/usr/bin/env python3
"""Run the Spec603 offline single-agent survival experiment.

The experiment deliberately separates three kinds of evidence:

* existing production-contract pytest probes;
* the real isolated setup -> reaction -> cleanup acceptance runner;
* read-only message projections used to measure context burden.

Only the first two are behavior evidence.  The message projections never call a
provider and must not be presented as an implemented Thin Runtime profile.
"""
from __future__ import annotations

import argparse
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import time
from typing import Any, Iterable


ROOT = Path(__file__).resolve().parents[1]
ROUND_ACCEPTANCE_PATH = ROOT / "tools" / "round_context_acceptance.py"

SECRET_ENV_NAMES = {
    "AIPABOX_API_KEY",
    "ANTHROPIC_API_KEY",
    "DEEPSEEK_API_KEY",
    "OPENAI_API_KEY",
    "UPSP_EMERGENCY_KEY",
}

SCENARIOS: tuple[dict[str, Any], ...] = (
    {
        "id": "simple_conversation",
        "title": "简单对话按三步顺序闭轮",
        "evidence_level": "runtime_orchestration",
        "nodeids": [
            "UPSP/OS/tests/test_runtime_orchestration.py::"
            "test_runtime_orchestrates_setup_reaction_cleanup_in_order",
        ],
        "contract": "setup、reaction、cleanup 顺序执行并把真实交互元数据带到善后。",
    },
    {
        "id": "chunked_read",
        "title": "分段读取保持未闭合游标",
        "evidence_level": "reaction_contract",
        "nodeids": [
            "UPSP/OS/tests/test_reaction_obligations.py::"
            "test_spec251_file_read_completion_is_checked_against_general_tool_result",
            "UPSP/OS/tests/test_reaction_obligations.py::"
            "test_spec290_continue_handoff_text_settles_unfinished_file_cursor",
        ],
        "contract": "has_more/next_start_line 只能由工具事实闭合，跨轮继续保留唯一游标。",
    },
    {
        "id": "artifact_task",
        "title": "任务产物使用真实路径证据结算",
        "evidence_level": "processor_contract",
        "nodeids": [
            "UPSP/OS/tests/test_guide_submit.py::"
            "test_spec454_task_progress_accepts_existing_file_uri_under_artifact_root",
        ],
        "contract": "真实 artifact root 内的产物路径可以形成验收证据。",
    },
    {
        "id": "duplicate_tool_failure",
        "title": "相同失败工具请求不重复执行 handler",
        "evidence_level": "dispatcher_contract",
        "nodeids": [
            "UPSP/OS/tests/test_general_tools.py::"
            "test_spec303_dispatcher_rejects_duplicate_failure_without_execute",
        ],
        "contract": "exact duplicate failure 在 handler 前拒绝，execute 计数保持一次。",
    },
    {
        "id": "protocol_write_block",
        "title": "主题未确认的记忆写入形成一次 NO-GO",
        "evidence_level": "feedback_contract",
        "nodeids": [
            "UPSP/OS/tests/test_general_tools.py::"
            "test_memory_subject_domain_failure_guides_only_current_object_registration",
        ],
        "contract": "关系域外主体不伪装归属；只允许当前直接对象走合法建卡流程。",
    },
    {
        "id": "rhythm_user_task",
        "title": "节律与用户任务合轮后接回用户工作",
        "evidence_level": "full_round",
        "nodeids": [],
        "contract": "真实隔离 Runtime 完成 setup、reaction、cleanup，用户正文不丢失且节律清旗。",
    },
    {
        "id": "cross_round_continue",
        "title": "跨轮继续留下唯一工作点",
        "evidence_level": "reaction_contract",
        "nodeids": [
            "UPSP/OS/tests/test_reaction_obligations.py::"
            "test_spec290_continue_handoff_text_settles_unfinished_file_cursor",
            "UPSP/OS/tests/test_reaction_terminal_state.py::"
            "test_spec564_runtime_auto_continue_sets_relay_flag",
        ],
        "contract": "continue 必须带 handoff/relay receipt，不伪装为完成。",
    },
    {
        "id": "evidence_mismatch",
        "title": "证据不匹配返回可直接使用的证据映射",
        "evidence_level": "processor_contract",
        "nodeids": [
            "UPSP/OS/tests/test_guide_submit.py::"
            "test_spec597_task_progress_rejection_includes_usable_evidence_map",
        ],
        "contract": "拒绝回执给出 known_evidence_items，不要求模型猜未知别名。",
    },
    {
        "id": "unfinished_final_reply",
        "title": "未闭合读取在第一次自然回复前只提醒一次",
        "evidence_level": "reaction_loop",
        "nodeids": [
            "UPSP/OS/tests/test_runtime_reaction_general_tools_read.py::"
            "TestRuntimeReactionGeneralToolsRead::"
            "test_spec580_unfinished_file_read_reminds_before_first_natural_final_reply",
            "UPSP/OS/tests/test_runtime_reaction_general_tools_read.py::"
            "TestRuntimeReactionGeneralToolsRead::"
            "test_spec580_completed_followup_read_clears_unfinished_reminder_state",
        ],
        "contract": "未闭合读取不能包装成完整结论，补读完成后提醒状态清零。",
    },
)


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
        newline="\n",
    )


def _load_round_acceptance():
    spec = importlib.util.spec_from_file_location(
        "spec603_round_context_acceptance",
        ROUND_ACCEPTANCE_PATH,
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def scenario_manifest() -> list[dict[str, Any]]:
    """Return a detached, JSON-safe copy of the ten frozen scenarios."""
    return json.loads(json.dumps(SCENARIOS, ensure_ascii=False))


def _offline_env() -> dict[str, str]:
    env = dict(os.environ)
    for name in SECRET_ENV_NAMES:
        env.pop(name, None)
    env["PYTHONUTF8"] = "1"
    env["UPSP_SPEC603_OFFLINE"] = "1"
    return env


def _run_pytest_probe(nodeids: Iterable[str]) -> dict[str, Any]:
    nodeids = list(nodeids)
    started = time.perf_counter()
    command = [
        sys.executable,
        "-X",
        "utf8",
        "-m",
        "pytest",
        *nodeids,
        "-q",
        "--tb=short",
        "--timeout=120",
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=_offline_env(),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=max(120, 120 * max(1, len(nodeids))),
        check=False,
    )
    return {
        "passed": completed.returncode == 0,
        "returncode": completed.returncode,
        "duration_seconds": round(time.perf_counter() - started, 3),
        "command": command,
        "stdout": completed.stdout,
        "stderr": completed.stderr,
        "nodeids": nodeids,
        "provider_required": False,
    }


def _run_round_context_probe(output_dir: Path) -> dict[str, Any]:
    module = _load_round_acceptance()
    fixture = output_dir / "fixture_book.md"
    fixture.parent.mkdir(parents=True, exist_ok=True)
    fixture.write_text(
        "Spec603 deterministic material line one.\n"
        "Spec603 deterministic material line two.\n",
        encoding="utf-8",
        newline="\n",
    )
    report = module.run_acceptance(
        scenario="coalesced_calendar_book",
        mode="fake",
        output_dir=output_dir,
        book_path=fixture,
        strict=True,
    )
    return report


def _message_category(message: dict[str, Any]) -> str:
    content = str(message.get("content") or "")
    kind = str(message.get("kind") or "").strip()
    if "<!-- POPUP" in content:
        return "popup"
    if "<!-- STATUSBAR" in content:
        return "statusbar"
    if "<!-- 永固层" in content:
        return "permanent"
    if "<!-- 高频层" in content:
        return "high_frequency"
    if kind in {
        "runtime_call_request",
        "interaction",
        "setup_fact",
        "tool_fact",
        "material",
    }:
        return kind
    return kind or "other"


OPERATIONAL_CORE_CATEGORIES = {
    "runtime_call_request",
    "interaction",
    "setup_fact",
    "tool_fact",
    "material",
}


def _profile_messages(
        messages: list[dict[str, Any]], profile: str) -> list[dict[str, Any]]:
    if profile == "full":
        return list(messages)
    if profile == "popup_suppressed_projection":
        return [item for item in messages if _message_category(item) != "popup"]
    if profile == "thin_operational_lower_bound":
        return [
            item for item in messages
            if _message_category(item) in OPERATIONAL_CORE_CATEGORIES
        ]
    raise ValueError(f"unknown_profile:{profile}")


def analyze_context_profiles(calls: list[dict[str, Any]]) -> dict[str, Any]:
    profiles = (
        "full",
        "popup_suppressed_projection",
        "thin_operational_lower_bound",
    )
    rows = []
    totals = {
        profile: {"calls": 0, "messages": 0, "characters": 0}
        for profile in profiles
    }
    preservation_failures = []
    for call_index, call in enumerate(calls or []):
        messages = list(call.get("messages") or [])
        full_categories = [_message_category(item) for item in messages]
        row = {
            "call_index": call_index,
            "step": call.get("step"),
            "channel": call.get("channel"),
            "full_categories": full_categories,
            "profiles": {},
        }
        required_categories = {
            category for category in full_categories
            if category in {
                "interaction",
                "material",
                "setup_fact",
                "tool_fact",
            }
        }
        for profile in profiles:
            projected = _profile_messages(messages, profile)
            categories = [_message_category(item) for item in projected]
            characters = sum(len(str(item.get("content") or "")) for item in projected)
            row["profiles"][profile] = {
                "message_count": len(projected),
                "characters": characters,
                "categories": categories,
            }
            totals[profile]["calls"] += 1
            totals[profile]["messages"] += len(projected)
            totals[profile]["characters"] += characters
            missing = sorted(required_categories - set(categories))
            if missing:
                preservation_failures.append({
                    "call_index": call_index,
                    "profile": profile,
                    "missing_categories": missing,
                })
        rows.append(row)

    full_chars = totals["full"]["characters"]
    for profile in profiles:
        chars = totals[profile]["characters"]
        totals[profile]["character_reduction_vs_full"] = (
            round(1 - (chars / full_chars), 4) if full_chars else 0.0
        )
    return {
        "schema_version": "single_agent_context_ablation.v1",
        "behavior_claim": (
            "Only full is executed. The other profiles are read-only projections "
            "and are not evidence that a Thin Runtime exists or behaves correctly."
        ),
        "profiles": {
            "full": {"executed": True},
            "popup_suppressed_projection": {"executed": False},
            "thin_operational_lower_bound": {
                "executed": False,
                "excludes_persona_and_policy": True,
            },
        },
        "totals": totals,
        "calls": rows,
        "operational_fact_preservation_failures": preservation_failures,
    }


def _selected_scenarios(ids: Iterable[str] | None) -> list[dict[str, Any]]:
    selected = set(ids or [])
    scenarios = scenario_manifest()
    if not selected:
        return scenarios
    known = {item["id"] for item in scenarios}
    unknown = sorted(selected - known)
    if unknown:
        raise ValueError("unknown_scenarios:" + ",".join(unknown))
    return [item for item in scenarios if item["id"] in selected]


def run_experiment(
        *,
        output_dir: str | Path,
        repetitions: int = 2,
        scenario_ids: Iterable[str] | None = None) -> dict[str, Any]:
    if repetitions < 1:
        raise ValueError("repetitions_must_be_positive")
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    scenarios = _selected_scenarios(scenario_ids)
    runs = []
    ablations = []
    for repetition in range(1, repetitions + 1):
        repetition_dir = output_dir / f"repetition_{repetition:02d}"
        repetition_results = []
        for scenario in scenarios:
            scenario_dir = repetition_dir / scenario["id"]
            scenario_dir.mkdir(parents=True, exist_ok=True)
            if scenario["id"] == "rhythm_user_task":
                full_report = _run_round_context_probe(scenario_dir)
                probe = {
                    "passed": not full_report["summary"]["failed_checks"],
                    "failed_checks": full_report["summary"]["failed_checks"],
                    "report_path": full_report["report_path"],
                    "provider_calls": full_report["provider_calls"],
                    "live_persona_touched": full_report["live_persona_touched"],
                    "round_closed": full_report["diagnostics"]["round_closed"],
                    "provider_required": False,
                }
                ablation = analyze_context_profiles(full_report["calls"])
                ablation["repetition"] = repetition
                ablation_path = scenario_dir / "context_ablation.json"
                _json_dump(ablation_path, ablation)
                ablation["path"] = str(ablation_path)
                ablations.append(ablation)
            else:
                probe = _run_pytest_probe(scenario["nodeids"])
                _json_dump(scenario_dir / "probe.json", probe)
            repetition_results.append({
                **scenario,
                "repetition": repetition,
                "probe": probe,
                "passed": probe["passed"] is True,
            })
        runs.append({
            "repetition": repetition,
            "scenarios": repetition_results,
            "passed": all(item["passed"] for item in repetition_results),
        })

    failures = [
        {
            "repetition": run["repetition"],
            "scenario_id": item["id"],
            "title": item["title"],
        }
        for run in runs
        for item in run["scenarios"]
        if not item["passed"]
    ]
    evidence_counts: dict[str, int] = {}
    for scenario in scenarios:
        level = scenario["evidence_level"]
        evidence_counts[level] = evidence_counts.get(level, 0) + 1
    ablation_clean = all(
        not item["operational_fact_preservation_failures"]
        for item in ablations
    )
    baseline_established = not failures and ablation_clean
    full_round_count = evidence_counts.get("full_round", 0)
    full_round_coverage_sufficient = full_round_count == len(scenarios)
    report = {
        "schema_version": "single_agent_survival_experiment.v1",
        "created_at": datetime.now().isoformat(),
        "repo_root": str(ROOT),
        "python": sys.executable,
        "offline": True,
        "secret_env_removed": sorted(SECRET_ENV_NAMES),
        "provider_called": False,
        "live_persona_touched": False,
        "repetitions": repetitions,
        "scenario_count": len(scenarios),
        "scenario_manifest": scenarios,
        "evidence_level_counts": evidence_counts,
        "runs": runs,
        "context_ablations": ablations,
        "summary": {
            "baseline_established": baseline_established,
            "failed_scenarios": failures,
            "context_preservation_clean": ablation_clean,
            "full_round_scenario_count": full_round_count,
            "full_round_scenario_required": len(scenarios),
            "full_round_coverage_sufficient": full_round_coverage_sufficient,
            "three_step_runtime_admission": (
                "GO" if baseline_established and full_round_coverage_sufficient
                else "HOLD"
            ),
            "thin_runtime_implemented": False,
            "thin_runtime_admission": "HOLD",
            "decision": (
                "MEASUREMENT_BASELINE_ESTABLISHED"
                if baseline_established else
                "NO_GO_BASELINE_UNSTABLE"
            ),
            "decision_reason": (
                "All frozen offline probes passed, but full-round evidence does not "
                "cover every scenario and only the full profile was executed. Thin "
                "projections are burden measurements, so three-step and production "
                "Thin admission remain HOLD."
                if baseline_established else
                "At least one frozen offline probe or operational-fact preservation "
                "check failed. Do not repair during this measurement run."
            ),
        },
    }
    report_path = output_dir / "single_agent_survival_report.json"
    report["report_path"] = str(report_path)
    _json_dump(report_path, report)
    return report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the offline Spec603 single-agent survival experiment."
    )
    parser.add_argument("--output-dir")
    parser.add_argument("--repetitions", type=int, default=2)
    parser.add_argument("--scenario", action="append", dest="scenarios")
    parser.add_argument("--json", action="store_true")
    parser.add_argument("--list-scenarios", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    if args.list_scenarios:
        payload = {"scenarios": scenario_manifest()}
        print(json.dumps(payload, ensure_ascii=False, indent=2))
        return 0
    if not args.output_dir:
        _parser().error("--output-dir is required unless --list-scenarios is used")
    report = run_experiment(
        output_dir=args.output_dir,
        repetitions=args.repetitions,
        scenario_ids=args.scenarios,
    )
    if args.json:
        print(json.dumps({
            "ok": report["summary"]["baseline_established"],
            "report_path": report["report_path"],
            "summary": report["summary"],
        }, ensure_ascii=False, indent=2))
    else:
        print(report["summary"]["decision"])
        print(report["report_path"])
    return 0 if report["summary"]["baseline_established"] else 1


if __name__ == "__main__":
    raise SystemExit(main())
