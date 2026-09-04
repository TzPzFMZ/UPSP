from pathlib import Path
from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path


REPO_ROOT = Path(__file__).resolve().parents[3]
TOOL_PATH = REPO_ROOT / "tools" / "single_agent_survival_experiment.py"


def _load_module():
    return _load_module_from_path('single_agent_survival_experiment', TOOL_PATH)


def test_spec765_manifest_contains_only_reachable_offline_scenarios():
    experiment = _load_module()

    scenarios = experiment.scenario_manifest()

    assert len(scenarios) == 9
    assert len({item["id"] for item in scenarios}) == 9
    assert {item["id"] for item in scenarios} == {
        "simple_conversation",
        "chunked_read",
        "artifact_task",
        "duplicate_tool_failure",
        "protocol_write_block",
        "rhythm_user_task",
        "cross_round_continue",
        "evidence_mismatch",
        "unfinished_final_reply",
    }
    assert all(item["contract"] for item in scenarios)
    assert sum(item["evidence_level"] == "full_round" for item in scenarios) == 1


def test_spec603_context_ablation_preserves_operational_facts():
    experiment = _load_module()
    calls = [{
        "step": "reaction",
        "channel": "reaction.loop",
        "messages": [
            {"role": "system", "content": "<!-- 永固层 -->\npolicy"},
            {"role": "user", "kind": "interaction", "content": "task"},
            {"role": "system", "kind": "tool_fact", "content": "receipt"},
            {"role": "system", "kind": "material", "content": "source"},
            {"role": "system", "content": "<!-- POPUP（弹窗层，messages绝对末位） -->\nwarn"},
        ],
    }]

    result = experiment.analyze_context_profiles(calls)

    assert result["operational_fact_preservation_failures"] == []
    totals = result["totals"]
    assert totals["full"]["characters"] > totals["popup_suppressed_projection"]["characters"]
    assert (
        totals["popup_suppressed_projection"]["characters"]
        > totals["thin_operational_lower_bound"]["characters"]
    )
    assert result["profiles"]["thin_operational_lower_bound"]["executed"] is False
    assert "not evidence" in result["behavior_claim"]


def test_spec603_experiment_keeps_thin_admission_on_hold(tmp_path, monkeypatch):
    experiment = _load_module()
    monkeypatch.setattr(experiment, "_run_pytest_probe", lambda nodeids: {
        "passed": True,
        "returncode": 0,
        "duration_seconds": 0.0,
        "command": [],
        "stdout": "1 passed",
        "stderr": "",
        "nodeids": list(nodeids),
        "provider_required": False,
    })

    report = experiment.run_experiment(
        output_dir=tmp_path / "experiment",
        repetitions=1,
        scenario_ids=["simple_conversation"],
    )

    assert report["summary"]["baseline_established"] is True
    assert report["summary"]["full_round_coverage_sufficient"] is False
    assert report["summary"]["three_step_runtime_admission"] == "HOLD"
    assert report["summary"]["thin_runtime_implemented"] is False
    assert report["summary"]["thin_runtime_admission"] == "HOLD"
    assert Path(report["report_path"]).is_file()
