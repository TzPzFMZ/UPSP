import contextlib
import io
import json
from pathlib import Path
import pytest
from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path


REPO_ROOT = Path(__file__).resolve().parents[3]
TOOLS_DIR = REPO_ROOT / "tools"
CLI_PATH = TOOLS_DIR / "upsp_cli.py"
ACCEPTANCE_PATH = TOOLS_DIR / "round_context_acceptance.py"


def _load_module(name, path):
    return _load_module_from_path(name, path)


def _load_cli():
    return _load_module("upsp_cli", CLI_PATH)


def _run_cli(cli, args):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = cli.main(args)
    return code, stdout.getvalue(), stderr.getvalue()


def test_fake_round_acceptance_runner_reports_real_context(tmp_path):
    acceptance = _load_module("round_context_acceptance", ACCEPTANCE_PATH)
    book = tmp_path / "book" / "共格主体论_V5_6.1.md"
    book.parent.mkdir(parents=True)
    book.write_text("第一行：测试读书材料。\n第二行：通道化自由。\n", encoding="utf-8")

    report = acceptance.run_acceptance(
        scenario="coalesced_calendar_book",
        mode="fake",
        output_dir=tmp_path / "out",
        book_path=book,
    )

    assert report["schema_version"] == "round_context_acceptance.v1"
    assert report["scenario"] == "coalesced_calendar_book"
    assert report["mode"] == "fake"
    assert report["provider_calls"] == []
    assert report["live_persona_touched"] is False
    assert report["checks"]["user_input_visible"]["passed"] is True
    assert report["checks"]["chronicle_material_visible"]["passed"] is True
    assert report["checks"]["natural_final_reply_projected"]["passed"] is True
    assert report["checks"]["cleanup_preserves_or_clears_correctly"]["passed"] is True
    assert report["checks"]["calendar_rhythm_settled"]["passed"] is True
    assert (
        report["diagnostics"]["heartbeat_flags_after"]["calendar_day_due"]
        is False
    )
    assert {call["channel"] for call in report["calls"]} >= {
        "setup",
        "reaction.loop",
        "cleanup",
    }
    assert report["diagnostics"]["round_closed"]["final_response"]


def test_upsp_cli_acceptance_run_and_export_are_json(tmp_path):
    cli = _load_cli()
    book = tmp_path / "book.md"
    book.write_text("测试读书材料。\n", encoding="utf-8")
    out_dir = tmp_path / "acceptance"

    code, stdout, stderr = _run_cli(cli, [
        "--json",
        "acceptance",
        "run",
        "--scenario",
        "coalesced_calendar_book",
        "--mode",
        "fake",
        "--output-dir",
        str(out_dir),
        "--book-path",
        str(book),
    ])

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert payload["command"] == "acceptance run"
    assert payload["data"]["report_path"]
    assert Path(payload["data"]["report_path"]).is_file()
    assert payload["data"]["summary"]["failed_checks"] == []

    code, stdout, stderr = _run_cli(cli, [
        "--json",
        "acceptance",
        "export",
        "--scenario",
        "coalesced_calendar_book",
        "--output-dir",
        str(out_dir),
        "--book-path",
        str(book),
    ])

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert payload["command"] == "acceptance export"
    bundle = payload["data"]["bundle"]
    assert Path(bundle["context_bundle"]).is_file()
    assert Path(bundle["subagent_prompt"]).is_file()
    assert Path(bundle["spark_observation_prompt"]).is_file()


def test_spec470_lists_guide_scenario_dogfood_matrix():
    acceptance = _load_module("round_context_acceptance_spec470", ACCEPTANCE_PATH)

    scenarios = acceptance.available_scenarios()

    assert "guide_system_matrix" not in scenarios


def test_spec470_guide_system_matrix_fake_acceptance(tmp_path):
    acceptance = _load_module("round_context_acceptance_spec470_matrix", ACCEPTANCE_PATH)

    with pytest.raises(ValueError, match="unknown_acceptance_scenario"):
        acceptance.run_acceptance(
            scenario="guide_system_matrix",
            mode="fake",
            output_dir=tmp_path / "matrix",
        )


def test_spec470_cli_acceptance_run_guide_matrix_is_json(tmp_path):
    cli = _load_cli()

    code, stdout, stderr = _run_cli(cli, [
        "--json",
        "acceptance",
        "run",
        "--scenario",
        "guide_system_matrix",
        "--mode",
        "fake",
        "--output-dir",
        str(tmp_path / "matrix"),
    ])

    assert code != 0
    assert "guide_system_matrix" in (stdout + stderr)


def test_spark_report_import_attaches_observation(tmp_path):
    acceptance = _load_module("round_context_acceptance", ACCEPTANCE_PATH)
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps({
            "schema_version": "round_context_acceptance.v1",
            "scenario": "coalesced_calendar_book",
            "spark_observation": None,
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    spark_report = tmp_path / "spark_observation.json"
    spark_report.write_text(
        json.dumps({
            "schema_version": "spark_observation.v1",
            "seen_user_input": True,
            "seen_chronicle_material": True,
            "seen_natural_final_reply": True,
            "would_handle_user_task": True,
            "trapped_or_confused": False,
            "would_misroute_relay": False,
            "would_clear_user_task_too_early": False,
            "likely_next_action": "先结算节律账，再处理用户读书请求。",
            "risk_items": [],
            "notes": "上下文材料清晰。",
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    updated = acceptance.import_spark_report(report_path, spark_report)

    assert updated["spark_observation"]["seen_user_input"] is True
    assert updated["spark_observation"]["seen_chronicle_material"] is True


def test_spark_required_gate_rejects_missing_observation(tmp_path):
    acceptance = _load_module("round_context_acceptance", ACCEPTANCE_PATH)
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps({
            "schema_version": "round_context_acceptance.v1",
            "summary": {"failed_checks": []},
            "spark_observation": None,
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    result = acceptance.require_spark_observation(report_path)

    assert result["ok"] is False
    assert result["issues"] == [{
        "code": "spark_observation_missing",
        "message": "整轮验收报告缺少 Spark 真实观察报告。",
    }]


def test_spark_required_gate_accepts_imported_observation(tmp_path):
    cli = _load_cli()
    report_path = tmp_path / "report.json"
    report_path.write_text(
        json.dumps({
            "schema_version": "round_context_acceptance.v1",
            "summary": {"failed_checks": []},
            "spark_observation": None,
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    spark_report = tmp_path / "spark_observation.json"
    spark_report.write_text(
        json.dumps({
            "schema_version": "spark_observation.v1",
            "seen_user_input": True,
            "seen_chronicle_material": True,
            "seen_natural_final_reply": True,
            "would_handle_user_task": True,
            "trapped_or_confused": False,
            "would_misroute_relay": False,
            "would_clear_user_task_too_early": False,
            "likely_next_action": "先结算节律账，再处理用户读书请求。",
            "risk_items": [],
            "notes": "上下文材料清晰。",
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(cli, [
        "--json",
        "acceptance",
        "import-spark-report",
        "--report-path",
        str(report_path),
        "--spark-report",
        str(spark_report),
    ])
    assert code == 0, stderr

    code, stdout, stderr = _run_cli(cli, [
        "--json",
        "acceptance",
        "require-spark",
        "--report-path",
        str(report_path),
    ])

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert payload["command"] == "acceptance require-spark"
    assert payload["data"]["ok"] is True
    assert payload["data"]["issues"] == []


def test_spark_envelope_script_can_drive_fake_acceptance(tmp_path):
    acceptance = _load_module("round_context_acceptance", ACCEPTANCE_PATH)
    book = tmp_path / "book.md"
    book.write_text("测试读书材料。\n", encoding="utf-8")
    envelope = tmp_path / "spark_envelope.json"
    envelope.write_text(
        json.dumps({
            "schema_version": "spark_simulation_envelope.v1",
            "events": [
                {
                    "step": "reaction",
                    "channel": "assistant_text",
                    "text": "先处理节律，再处理用户请求。",
                },
                {
                    "step": "reaction",
                    "tool_calls": [{
                        "tool_id": "guide_submit",
                        "arguments": {
                            "guide_id": "rhythm:calendar_day:R000001",
                            "submissions": [{
                                "item_id": "calendar_day_due",
                                "option_id": "write_chronicle",
                                "fields": {
                                    "content": "Spark 信封模拟节律账。",
                                    "reason": "spark envelope rhythm",
                                },
                            }],
                        },
                    }],
                },
                {
                    "step": "reaction",
                    "tool_calls": [{
                        "tool_id": "file_read",
                        "arguments": {
                            "path": str(book),
                            "reason": "spark envelope read",
                        },
                    }],
                },
                {
                    "step": "reaction",
                    "text": "Spark 信封模拟最终回复。",
                },
            ],
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    report = acceptance.run_acceptance(
        scenario="coalesced_calendar_book",
        mode="spark-envelope",
        output_dir=tmp_path / "out",
        book_path=book,
        envelope_path=envelope,
    )

    assert report["mode"] == "spark-envelope"
    assert report["summary"]["failed_checks"] == []
    assert report["diagnostics"]["round_closed"]["final_response"] == (
        "Spark 信封模拟最终回复。"
    )


def test_invalid_spark_envelope_rejects_final_reply_tool_call():
    acceptance = _load_module("round_context_acceptance", ACCEPTANCE_PATH)

    issues = acceptance.validate_spark_envelope({
        "schema_version": "spark_simulation_envelope.v1",
        "events": [{
            "step": "final_reply",
            "tool_calls": [{
                "tool_id": "file_read",
                "arguments": {"path": "x.md"},
            }],
        }],
    })

    assert issues == [{
        "code": "final_reply.invalid_tool",
        "message": "assistant_text 只能是自然语言文本，不允许工具调用。",
    }]
