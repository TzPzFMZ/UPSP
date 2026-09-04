import contextlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path


REPO_ROOT = Path(__file__).resolve().parents[3]
CLI_PATH = REPO_ROOT / "tools" / "upsp_cli.py"


def _load_cli():
    module = _load_module_from_path('upsp_cli', CLI_PATH)
    module._configure_active_paths()
    return module


def _run_cli(cli, args):
    stdout = io.StringIO()
    stderr = io.StringIO()
    with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
        code = cli.main(args)
    return code, stdout.getvalue(), stderr.getvalue()


def _write_round(round_dir, round_num, events=None):
    round_dir.mkdir(parents=True, exist_ok=True)
    path = round_dir / f"round_{round_num}.jsonl"
    events = events or [
        {
            "schema_version": "round_audit.v1",
            "round": round_num,
            "event_index": 1,
            "event_id": f"R{round_num:06d}-000001",
            "recorded_at": "2026-06-16T10:00:00+08:00",
            "event_type": "round_closed",
            "payload": {
                "status": "closed",
                "final_response": "ok",
                "final_response_source": "reaction_finalize",
            },
        }
    ]
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for event in events:
            f.write(json.dumps(event, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def _write_model_configs(
        models_path: Path,
        routing_path: Path,
        env_name: str,
        *,
        model: str = "unit-model",
        inline_key: str = "") -> None:
    from schemas.config import default_model_routing_config, default_models_config

    catalog = default_models_config()
    catalog["connections"] = [{
        "id": "conn_unit",
        "alias": "单元测试连接",
        "protocol": "openai_chat",
        "url": "https://api.example/v1/chat/completions",
        "api_key_env": env_name,
        "api_key": inline_key,
    }]
    catalog["models"] = [{
        "id": "model_unit",
        "alias": "单元测试模型",
        "connection_id": "conn_unit",
        "model": model,
        "context_window": 1000000,
        "reasoning": {"supported": ["medium"], "default": "medium"},
        "streaming": {"enabled": True, "protocol": "openai_sse", "include_usage": True},
        "prompt_cache": {"profile": "off"},
        "request_overrides": {},
    }]
    routing = default_model_routing_config()
    routing["routes"]["setup"]["primary"] = {
        "model_id": "model_unit",
        "reasoning_effort": "medium",
    }
    models_path.write_text(json.dumps(catalog, ensure_ascii=False), encoding="utf-8")
    routing_path.write_text(json.dumps(routing, ensure_ascii=False), encoding="utf-8")


def _patch_model_paths(cli, monkeypatch, models_path: Path, routing_path: Path) -> None:
    from data import config_store as cfs

    monkeypatch.setitem(
        cfs._CONFIG_MAP,
        "models",
        (str(models_path), cfs.default_models_config),
    )
    monkeypatch.setitem(
        cfs._CONFIG_MAP,
        "model_routing",
        (str(routing_path), cfs.default_model_routing_config),
    )
    monkeypatch.setattr(cli, "MODELS_CONFIG", models_path)
    monkeypatch.setattr(cli, "MODEL_ROUTING_CONFIG", routing_path)


def test_spec702_cli_uses_canonical_persona_path():
    cli = _load_cli()
    from paths import PERSONA_DIR

    assert cli.PERSONA_DIR == Path(PERSONA_DIR)


def test_spec703_cli_status_honors_complete_root_overrides(tmp_path):
    from initialization.windows_data import ensure_active_instance

    data_root = tmp_path / "用户 文档" / "UPSP"
    local_root = tmp_path / "本机 状态" / "UPSP"
    environment = dict(os.environ)
    environment.update({
        "UPSP_DATA_ROOT": str(data_root),
        "UPSP_LOCAL_STATE_ROOT": str(local_root),
        "PYTHONDONTWRITEBYTECODE": "1",
        "PYTHONIOENCODING": "utf-8",
    })
    layout = ensure_active_instance(
        REPO_ROOT / "UPSP",
        environ=environment,
    )
    layout.persona_dir.mkdir(parents=True)
    (layout.persona_dir / "state.json").write_text(
        json.dumps({
            "base": {
                "meta": {"total_round": 703},
                "runtime": {"phase": "idle"},
                "heartbeat_flags": {"calendar_day_due": True},
            },
        }),
        encoding="utf-8",
    )

    completed = subprocess.run(
        [sys.executable, str(CLI_PATH), "--json", "status"],
        cwd=REPO_ROOT,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        encoding="utf-8",
        timeout=30,
    )

    assert completed.returncode == 0, completed.stderr
    payload = json.loads(completed.stdout)
    assert payload["data"]["total_round"] == 703
    assert payload["data"]["active_flags"] == ["calendar_day_due"]
    assert payload["data"]["round_type"] == "rhythm"


def test_doctor_json_is_structured_and_redacts_keys(tmp_path, monkeypatch):
    cli = _load_cli()
    secret = "sk-unit-test-secret-314"
    models_path = tmp_path / "models.json"
    routing_path = tmp_path / "model_routing.json"
    _write_model_configs(models_path, routing_path, "UNIT_API_KEY")
    _patch_model_paths(cli, monkeypatch, models_path, routing_path)
    monkeypatch.setenv("UNIT_API_KEY", secret)
    monkeypatch.setenv("OPENAI_API_KEY", secret)
    monkeypatch.setenv("DEEPSEEK_API_KEY", secret)

    code, stdout, stderr = _run_cli(cli, ["--json", "doctor"])

    assert code == 0
    assert secret not in stdout
    assert secret not in stderr
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert payload["command"] == "doctor"
    assert "python" in payload["data"]
    assert "runtime_import" in payload["data"]


def test_spec702_doctor_does_not_consume_retired_persona_specific_key_config(
        tmp_path, monkeypatch):
    cli = _load_cli()
    secret = "sk-retired-unit-secret-314"
    models_path = tmp_path / "models.json"
    routing_path = tmp_path / "model_routing.json"
    _write_model_configs(models_path, routing_path, "UNIT_RETIRED_API_KEY")
    retired_config = tmp_path / "config.json"
    retired_config.write_text(
        json.dumps({"llm": {"api_key": secret}}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.delenv("UNIT_RETIRED_API_KEY", raising=False)
    monkeypatch.setenv("UPSP_FMZ_CONFIG", str(retired_config))
    _patch_model_paths(cli, monkeypatch, models_path, routing_path)

    code, stdout, stderr = _run_cli(cli, ["--json", "doctor"])

    assert code == 0
    assert secret not in stdout
    assert secret not in stderr
    payload = json.loads(stdout)
    assert payload["ok"] is True
    endpoints = payload["data"]["api_config"]["endpoints"]
    assert endpoints["model_unit"]["api_key_env"] == "UNIT_RETIRED_API_KEY"
    assert endpoints["model_unit"]["api_key_source"] == "missing"


def test_status_json_returns_runtime_summary():
    cli = _load_cli()

    code, stdout, _stderr = _run_cli(cli, ["--json", "status"])

    assert code == 0
    payload = json.loads(stdout)
    assert payload["ok"] is True
    data = payload["data"]
    assert {"total_round", "active_flags", "round_type", "phase"} <= set(data)


def test_spec550_status_api_summary_uses_override_env_without_secret(tmp_path, monkeypatch):
    cli = _load_cli()
    secret = "sk-spec550-secret"
    override = {
        "endpoints": {
            "primary": {
                "url": "https://9527code.com/v1/chat/completions",
                "model": "deepseek-v4-flash",
                "api_key_env": "UPSP_9527CODE_API_KEY",
                "provider": "openai_chat",
                "context_window": 1000000,
            },
            "fallback": {
                "url": "https://9527code.com/v1/chat/completions",
                "model": "deepseek-v4-flash",
                "api_key_env": "UPSP_9527CODE_API_KEY",
                "provider": "openai_chat",
                "context_window": 1000000,
            },
        },
        "step_tiers": {
            "setup": "fallback",
            "reaction": "primary",
            "cleanup": "fallback",
        },
    }
    monkeypatch.setenv("UPSP_API_CONFIG_OVERRIDE_JSON", json.dumps(override, ensure_ascii=False))
    monkeypatch.setenv("UPSP_9527CODE_API_KEY", secret)

    code, stdout, stderr = _run_cli(cli, ["--json", "status"])

    assert code == 0
    assert secret not in stdout
    assert secret not in stderr
    payload = json.loads(stdout)
    summary = payload["data"]["api_config"]
    assert summary["override_env"] is True
    assert summary["endpoints"]["primary"]["url"] == "https://9527code.com/v1/chat/completions"
    assert summary["endpoints"]["primary"]["model"] == "deepseek-v4-flash"
    assert summary["endpoints"]["primary"]["api_key_env"] == "UPSP_9527CODE_API_KEY"
    assert summary["step_tiers"]["reaction"] == "primary"


def test_spec700_status_api_summary_uses_canonical_model_library(tmp_path, monkeypatch):
    monkeypatch.delenv("UPSP_API_CONFIG_OVERRIDE_JSON", raising=False)
    cli = _load_cli()
    models_path = tmp_path / "models.json"
    routing_path = tmp_path / "model_routing.json"
    _write_model_configs(
        models_path,
        routing_path,
        "SPEC700_KEY",
        model="spec700-canonical-model",
        inline_key="must-not-leak",
    )
    catalog = json.loads(models_path.read_text(encoding="utf-8"))
    catalog["connections"][0]["url"] = "https://canonical.example/v1/chat/completions"
    models_path.write_text(json.dumps(catalog), encoding="utf-8")
    _patch_model_paths(cli, monkeypatch, models_path, routing_path)

    code, stdout, stderr = _run_cli(cli, ["--json", "status"])

    assert code == 0
    assert stderr == ""
    summary = json.loads(stdout)["data"]["api_config"]
    assert summary["override_env"] is False
    assert summary["endpoints"]["model_unit"]["model"] == "spec700-canonical-model"
    assert summary["step_routes"]["reaction"]
    assert "must-not-leak" not in stdout


def test_spec510_status_exposes_work_debt_and_open_guides(monkeypatch):
    cli = _load_cli()
    monkeypatch.setattr(
        cli,
        "_workbench_pending_status",
        lambda: {
            "work_intent_debt": {"status": "open", "reason": "agent_eval"},
            "open_guides": [{"guide_id": "task_bootstrap", "status": "open"}],
            "pending_guides": [{"guide_id": "task_bootstrap", "status": "open"}],
            "active_task": "T-20260702-01",
            "open_pending_inputs": ["input_01"],
        },
    )

    code, stdout, _stderr = _run_cli(cli, ["--json", "status"])

    assert code == 0
    payload = json.loads(stdout)
    data = payload["data"]
    assert data["work_intent_debt"]["status"] == "open"
    assert data["open_guides"][0]["guide_id"] == "task_bootstrap"
    assert data["pending_guides"][0]["status"] == "open"
    assert data["active_task"] == "T-20260702-01"
    assert data["open_pending_inputs"] == ["input_01"]


def test_spec694_superseded_guide_is_not_reported_open(tmp_path, monkeypatch):
    cli = _load_cli()
    from data import workbench as workbench_module

    monkeypatch.setattr(workbench_module, "WB_DIR", str(tmp_path / "workbench"))
    store = workbench_module.WorkbenchStore()
    store.save_guide({
        "guide_id": "rhythm:old:R000001",
        "kind": "calendar_rhythm_guide",
        "status": "superseded",
        "items": [{"item_id": "calendar_day_due", "status": "open"}],
    })
    store.save_guide({
        "guide_id": "rhythm:current:R000001",
        "kind": "calendar_rhythm_guide",
        "status": "open",
        "items": [{"item_id": "calendar_week_due", "status": "open"}],
    })
    monkeypatch.setattr(cli, "STATE_JSON", tmp_path / "state.json")

    pending = cli._workbench_pending_status()

    assert [guide["guide_id"] for guide in pending["open_guides"]] == [
        "rhythm:current:R000001"
    ]
    assert pending["pending_guides"] == pending["open_guides"]
    assert pending["guide_status_inconsistencies"] == []


def test_spec516_terminal_state_detects_runtime_blocked_closed():
    cli = _load_cli()
    events = [
        {
            "event_type": "step_settlement",
            "phase": "reaction",
            "payload": {
                "settlement_ledgers": [{
                    "closeout_decision": "blocked",
                    "blocked_reason": "task_acceptance_blocked",
                    "blockers": ["item_02"],
                }],
                "protocol_tool_receipts": [{
                    "status": "task_acceptance_auto_blocked",
                    "reason": "task_acceptance_blocked",
                }],
            },
        },
        {
            "event_type": "round_closed",
            "payload": {"status": "closed", "final_response": "blocked"},
        },
    ]

    terminal = cli._terminal_state_from_round_events(events)

    assert terminal["status"] == "closed"
    assert terminal["classification"] == "runtime_blocked_closed"
    assert terminal["reason"] == "task_acceptance_blocked"


def test_spec522_terminal_state_ignores_recovered_intermediate_blocker():
    cli = _load_cli()
    events = [
        {
            "event_type": "step_settlement",
            "phase": "reaction",
            "payload": {
                "settlement_ledgers": [{
                    "closeout_decision": "finish",
                }],
                "reaction_loop_guard_receipts": [{
                    "status": "task_acceptance_blocked",
                    "reason": "task_acceptance_blocked",
                    "blockers": ["acc_01"],
                }],
            },
        },
        {
            "event_type": "round_closed",
            "payload": {
                "status": "closed",
                "final_response": "done",
                "final_response_source": "reaction.assistant_text",
            },
        },
    ]

    terminal = cli._terminal_state_from_round_events(events)

    assert terminal["status"] == "closed"
    assert "classification" not in terminal
    assert "reason" not in terminal


def test_spec510_work_intent_debt_reads_nested_state_dict():
    from logic.work_intent_debt import current_work_intent_debt

    debt = current_work_intent_debt({
        "base": {
            "runtime": {
                "work_intent_debt": {
                    "status": "open",
                    "reason": "agent_eval",
                    "source": "engineering_task_phase",
                }
            }
        }
    })

    assert debt["status"] == "open"
    assert debt["reason"] == "agent_eval"


def test_send_requires_live_flag():
    cli = _load_cli()

    code, stdout, _stderr = _run_cli(
        cli, ["--json", "send", "--message", "不要真实发送"]
    )

    assert code != 0
    payload = json.loads(stdout)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "live_required"


def test_relay_requires_live_flag():
    cli = _load_cli()

    code, stdout, _stderr = _run_cli(cli, ["--json", "relay"])

    assert code != 0
    payload = json.loads(stdout)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "live_required"


def test_tick_requires_live_flag():
    cli = _load_cli()

    code, stdout, _stderr = _run_cli(cli, ["--json", "tick"])

    assert code != 0
    payload = json.loads(stdout)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "live_required"


def test_spec430_send_sets_permission_level_for_live_call(monkeypatch):
    cli = _load_cli()
    seen = {}

    def run(kind, permission_level, *, message=None):
        seen.update({
            "kind": kind,
            "permission_level": permission_level,
            "message": message,
        })
        return {"status": "round_completed", "message": message}

    monkeypatch.setattr(cli, "_run_resident_command", run)
    monkeypatch.delenv("UPSP_EXECUTION_PERMISSION_LEVEL", raising=False)

    code, stdout, _stderr = _run_cli(
        cli,
        [
            "--json",
            "send",
            "--live",
            "--permission-level",
            "unlimited",
            "--message",
            "权限探针",
        ],
    )

    assert code == 0
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert seen == {
        "kind": "send",
        "permission_level": "unlimited",
        "message": "权限探针",
    }
    assert os.environ.get("UPSP_EXECUTION_PERMISSION_LEVEL") is None


def test_spec735_send_passes_optional_final_response_budget(monkeypatch):
    cli = _load_cli()
    seen = {}

    def run(kind, permission_level, *, message=None,
            final_response_max_chars=None):
        seen.update({
            "kind": kind,
            "permission_level": permission_level,
            "message": message,
            "final_response_max_chars": final_response_max_chars,
        })
        return {"status": "round_completed"}

    monkeypatch.setattr(cli, "_run_resident_command", run)
    code, stdout, _stderr = _run_cli(cli, [
        "--json", "send", "--live", "--message", "query",
        "--final-response-max-chars", "128",
    ])

    assert code == 0
    assert json.loads(stdout)["ok"] is True
    assert seen["final_response_max_chars"] == 128

    code, stdout, _stderr = _run_cli(cli, [
        "--json", "send", "--live", "--message", "query",
        "--final-response-max-chars", "0",
    ])
    assert code != 0
    assert json.loads(stdout)["error"]["code"] == (
        "invalid_final_response_max_chars")


def test_send_reads_utf8_message_file_for_live_call(tmp_path, monkeypatch):
    cli = _load_cli()
    seen = {}
    message_path = tmp_path / "message.txt"
    message_path.write_text(
        "请读取 D:\\AI_WORKSPACE\\base\\任务.md。\n这是中文入口提示。",
        encoding="utf-8",
    )

    def run(kind, permission_level, *, message=None):
        seen.update({
            "kind": kind,
            "permission_level": permission_level,
            "message": message,
        })
        return {"status": "round_completed", "message": message}

    monkeypatch.setattr(cli, "_run_resident_command", run)

    code, stdout, _stderr = _run_cli(
        cli,
        ["--json", "send", "--live", "--message-file", str(message_path)],
    )

    assert code == 0
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert seen["message"] == "请读取 D:\\AI_WORKSPACE\\base\\任务.md。\n这是中文入口提示。"
    assert seen["kind"] == "send"
    assert seen["permission_level"] == "limited"


def test_spec704_cli_routes_live_send_to_existing_resident_host(monkeypatch):
    cli = _load_cli()
    seen = {}

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self):
            return json.dumps({
                "ok": True,
                "command": "send",
                "data": {
                    "status": "round_completed",
                    "round_num": 704,
                },
            }).encode("utf-8")

    def open_request(request):
        seen["url"] = request.full_url
        seen["origin"] = request.get_header("Origin")
        seen["payload"] = json.loads(request.data.decode("utf-8"))
        return Response()

    monkeypatch.setattr(cli.urllib.request, "urlopen", open_request)

    result = cli._call_resident_host(
        {"address": "127.0.0.1", "port": 8770},
        "send",
        "limited",
        message="hello",
    )

    assert result == {"status": "round_completed", "round_num": 704}
    assert seen == {
        "url": "http://127.0.0.1:8770/api/runtime/send",
        "origin": "http://127.0.0.1:8770",
        "payload": {
            "message": "hello",
            "permission_level": "limited",
            "unlimited_confirmed": False,
        },
    }


def test_spec612_send_cli_rejects_retired_context_profile_argument():
    cli = _load_cli()
    try:
        cli.build_parser().parse_args([
            "send",
            "--live",
            "--context-profile",
            "popup_exception_only",
            "--message",
            "profile probe",
        ])
    except SystemExit as exc:
        assert exc.code == 2
    else:
        raise AssertionError("retired context-profile CLI switch must be rejected")


def test_relay_refuses_when_no_relay_pending(monkeypatch):
    cli = _load_cli()

    def no_pending(*_args, **_kwargs):
        raise cli.CliError("relay_not_pending", "relay_not_pending")

    monkeypatch.setattr(
        cli,
        "_run_resident_command",
        no_pending,
    )

    code, stdout, _stderr = _run_cli(cli, ["--json", "relay", "--live"])

    assert code != 0
    payload = json.loads(stdout)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "relay_not_pending"


def test_relay_live_runs_pending_relay_without_message(monkeypatch):
    cli = _load_cli()
    calls = []

    def run(kind, permission_level, *, message=None):
        calls.append((kind, permission_level, message))
        return {
            "status": "round_completed",
            "round_type": "relay",
            "round_num": 42,
            "round_file": "round_42.jsonl",
            "final_response": "ok",
            "is_interactive": False,
            "active_flags": ["continue_requested"],
        }

    monkeypatch.setattr(cli, "_run_resident_command", run)

    code, stdout, _stderr = _run_cli(cli, ["--json", "relay", "--live"])

    assert code == 0
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert payload["command"] == "relay"
    assert payload["data"]["round_type"] == "relay"
    assert payload["data"]["is_interactive"] is False
    assert calls == [("relay", "limited", None)]


def test_tick_live_runs_one_natural_pending_round_without_message(monkeypatch):
    cli = _load_cli()
    calls = []

    def run(kind, permission_level, *, message=None):
        calls.append((kind, permission_level, message))
        return {
            "status": "round_completed",
            "round_type": "rhythm",
            "round_num": 328,
            "round_file": "round_328.jsonl",
            "final_response": "节律轮完成",
            "is_interactive": False,
            "active_flags": ["rhythm_due"],
        }

    monkeypatch.setattr(cli, "_run_resident_command", run)

    code, stdout, _stderr = _run_cli(cli, ["--json", "tick", "--live"])

    assert code == 0
    payload = json.loads(stdout)
    assert payload["ok"] is True
    assert payload["command"] == "tick"
    assert payload["data"]["round_type"] == "rhythm"
    assert payload["data"]["is_interactive"] is False
    assert calls == [("tick", "limited", None)]


def test_tick_refuses_when_no_natural_round_pending(monkeypatch):
    cli = _load_cli()

    def no_pending(*_args, **_kwargs):
        raise cli.CliError("tick_not_pending", "tick_not_pending")

    monkeypatch.setattr(
        cli,
        "_run_resident_command",
        no_pending,
    )

    code, stdout, _stderr = _run_cli(cli, ["--json", "tick", "--live"])

    assert code != 0
    payload = json.loads(stdout)
    assert payload["ok"] is False
    assert payload["error"]["code"] == "tick_not_pending"


def test_relay_and_tick_return_real_stopped_round(monkeypatch):
    cli = _load_cli()
    monkeypatch.setattr(
        cli,
        "_run_resident_command",
        lambda kind, *_args, **_kwargs: {
            "status": "round_stopped",
            "round_type": "relay" if kind == "relay" else "rhythm",
            "round_num": 704,
            "settlement_status": "degraded",
        },
    )

    for command in ("relay", "tick"):
        code, stdout, _stderr = _run_cli(
            cli, ["--json", command, "--live"])
        payload = json.loads(stdout)
        assert code == 0
        assert payload["data"]["status"] == "round_stopped"


def test_rounds_list_returns_recent_rounds_desc(tmp_path, monkeypatch):
    cli = _load_cli()
    round_dir = tmp_path / "round"
    _write_round(round_dir, 2)
    _write_round(round_dir, 10)
    _write_round(round_dir, 7)
    monkeypatch.setattr(cli, "ROUND_DIR", round_dir)

    code, stdout, _stderr = _run_cli(cli, ["--json", "rounds", "list", "--limit", "2"])

    assert code == 0
    payload = json.loads(stdout)
    rounds = payload["data"]["rounds"]
    assert [item["round_num"] for item in rounds] == [10, 7]
    assert all(item["bytes"] > 0 for item in rounds)


def test_rounds_inspect_latest_uses_existing_round_inspector(tmp_path, monkeypatch):
    cli = _load_cli()
    round_dir = tmp_path / "round"
    _write_round(round_dir, 3)
    latest = _write_round(round_dir, 8)
    monkeypatch.setattr(cli, "ROUND_DIR", round_dir)

    code, stdout, _stderr = _run_cli(
        cli, ["--json", "rounds", "inspect", "--round", "latest"]
    )

    assert code == 0
    payload = json.loads(stdout)
    data = payload["data"]
    assert data["round_file"] == str(latest)
    assert data["round_num"] == 8
    assert data["round_closed_status"] == "closed"
