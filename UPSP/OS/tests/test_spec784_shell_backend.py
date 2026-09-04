import os
from pathlib import Path
import subprocess


ROOT = Path(__file__).resolve().parents[3]


def test_windows_shell_backend_prefers_valid_comspec(tmp_path):
    from logic.shell_backend import resolve_shell_backend

    comspec = tmp_path / "cmd.exe"
    comspec.write_bytes(b"")
    fallback = tmp_path / "Windows" / "System32" / "cmd.exe"
    fallback.parent.mkdir(parents=True)
    fallback.write_bytes(b"")

    backend = resolve_shell_backend(
        os_name="nt",
        environ={"COMSPEC": str(comspec), "SystemRoot": str(tmp_path / "Windows")},
    )

    assert backend.available is True
    assert backend.backend_id == "windows_cmd_v1"
    assert backend.dialect == "windows_cmd"
    assert backend.executable == str(comspec.resolve())
    assert backend.build_argv("cmd /c ver") == [
        str(comspec.resolve()), "/d", "/s", "/c", "cmd /c ver",
    ]
    assert backend.build_command_line('"C:\\Program Files\\tool.exe" --check') == (
        f'"{comspec.resolve()}" /d /s /c '
        '""C:\\Program Files\\tool.exe" --check"'
    )


def test_windows_shell_backend_rejects_invalid_comspec_and_uses_systemroot(tmp_path):
    from logic.shell_backend import resolve_shell_backend

    fallback = tmp_path / "Windows" / "System32" / "cmd.exe"
    fallback.parent.mkdir(parents=True)
    fallback.write_bytes(b"")

    backend = resolve_shell_backend(
        os_name="nt",
        environ={
            "COMSPEC": str(tmp_path / "powershell.exe"),
            "SystemRoot": str(tmp_path / "Windows"),
        },
    )

    assert backend.available is True
    assert backend.executable == str(fallback.resolve())


def test_unknown_platform_and_missing_windows_backend_are_unavailable(tmp_path):
    from logic.shell_backend import UNAVAILABLE_REASON, resolve_shell_backend

    unsupported = resolve_shell_backend(os_name="posix", environ={})
    missing = resolve_shell_backend(
        os_name="nt",
        environ={"COMSPEC": "relative\\cmd.exe", "SystemRoot": str(tmp_path)},
    )

    for backend in (unsupported, missing):
        assert backend.available is False
        assert backend.reason == UNAVAILABLE_REASON
        assert backend.backend_id == ""
        assert backend.dialect == ""


def test_shell_handler_uses_explicit_argv_and_preserves_command(tmp_path, monkeypatch):
    from logic import general_tools
    from logic.general_tools import execute_general_tool_call

    command = 'cmd /c echo "原样 命令" && echo second'
    calls = []

    def fake_run(argv, **kwargs):
        calls.append((argv, kwargs))
        return subprocess.CompletedProcess(argv, 0, stdout=b"ok\r\n", stderr=b"")

    monkeypatch.setattr(general_tools.subprocess, "run", fake_run)
    result = execute_general_tool_call(
        {
            "tool_id": "shell_command",
            "cwd": str(tmp_path),
            "command": command,
            "purpose": "verify explicit shell argv",
        },
        allowed_roots=[tmp_path],
    )

    assert result["status"] == "ok"
    assert result["shell_backend"] == "windows_cmd_v1"
    assert result["shell_dialect"] == "windows_cmd"
    command_line, kwargs = calls[0]
    assert command_line.endswith(f'/d /s /c "{command}"')
    assert Path(kwargs["executable"]).name.casefold() == "cmd.exe"
    assert kwargs["shell"] is False


def test_shell_handler_rejects_unavailable_backend_without_subprocess(tmp_path, monkeypatch):
    from logic import general_tools
    from logic.general_tools import execute_general_tool_call, format_general_tool_fact
    from logic.shell_backend import ShellBackend, UNAVAILABLE_REASON

    monkeypatch.setattr(
        general_tools,
        "resolve_shell_backend",
        lambda: ShellBackend("", "", "", False, UNAVAILABLE_REASON),
    )
    monkeypatch.setattr(
        general_tools.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )

    result = execute_general_tool_call(
        {
            "tool_id": "shell_command",
            "cwd": str(tmp_path),
            "command": "echo no",
            "purpose": "unsupported host probe",
        },
        allowed_roots=[tmp_path],
    )

    assert result["status"] == "rejected"
    assert result["reason"] == "shell_backend_unavailable"
    fact = format_general_tool_fact(result)
    assert "Shell 后端：不可用 / 未验收方言" in fact
    assert "shell_backend_unavailable" in fact


def test_shell_handler_rejects_multiline_before_subprocess(tmp_path, monkeypatch):
    from logic import general_tools
    from logic.general_tools import execute_general_tool_call

    monkeypatch.setattr(
        general_tools.subprocess,
        "run",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(AssertionError("must not run")),
    )
    for command in ("echo first\necho second", "echo first\r\necho second"):
        result = execute_general_tool_call(
            {
                "tool_id": "shell_command",
                "cwd": str(tmp_path),
                "command": command,
                "purpose": "prove multiline is not silently truncated",
            },
            allowed_roots=[tmp_path],
        )
        assert result["status"] == "rejected"
        assert result["reason"] == "shell_multiline_unsupported"
        assert result["command"] == command


def test_shell_is_not_exported_when_backend_is_unavailable(monkeypatch):
    from logic import native_tool_calls
    from logic.native_tool_calls import export_provider_tool_schemas
    from logic.shell_backend import ShellBackend, UNAVAILABLE_REASON

    monkeypatch.setattr(
        native_tool_calls,
        "resolve_shell_backend",
        lambda: ShellBackend("", "", "", False, UNAVAILABLE_REASON),
    )
    tools = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
        include_step_terminal_tools=["reaction_finalize"],
        execution_permission_level="unlimited",
    )

    assert "shell_command" not in {item["name"] for item in tools}


def test_windows_shell_schema_and_short_guide_report_same_backend():
    from assembly.context_helpers import build_general_tool_guide
    from logic.native_tool_calls import export_provider_tool_schemas

    tools = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
        include_step_terminal_tools=["reaction_finalize"],
        execution_permission_level="unlimited",
    )
    shell = next(item for item in tools if item["name"] == "shell_command")
    guide = build_general_tool_guide("shell_command")

    for text in (shell["description"], shell["parameters"]["properties"]["command"]["description"], guide):
        assert "shell_backend=windows_cmd_v1" in text
        assert "shell_dialect=windows_cmd" in text
        assert "powershell -NoProfile -Command" in text
        assert "&&" in text
        assert "command 必须是单行" in text
    assert "当前宿主用户权限" in shell["parameters"]["properties"]["cwd"]["description"]
    assert "当前 Windows 用户权限" not in shell["parameters"]["properties"]["cwd"]["description"]


def test_shell_backend_unavailable_feedback_stops_retries():
    from engines.reaction_helpers import native_tool_feedback_action

    action, messages = native_tool_feedback_action(
        "shell_backend_unavailable",
        {"tool_id": "shell_command"},
    )

    assert action == "stop_shell_calls_and_report_host_capability"
    assert any("停止" in message for message in messages)
    assert any("反复重试" in message for message in messages)


def test_shell_multiline_feedback_requires_single_line_rewrite():
    from engines.reaction_helpers import native_tool_feedback_action

    action, messages = native_tool_feedback_action(
        "shell_multiline_unsupported",
        {"tool_id": "shell_command"},
    )

    assert action == "rewrite_shell_as_single_line"
    assert any("单行" in message for message in messages)
    assert any("&&" in message for message in messages)


def test_windows_payload_build_has_unskippable_production_shell_smoke():
    script = (ROOT / "tools" / "build_windows_desktop.ps1").read_text(encoding="utf-8")

    skip_tests_end = script.index("& $dotnet restore")
    smoke_start = script.index("$shellSmoke = @'")
    manifest_start = script.index("$manifestItems =")
    assert skip_tests_end < smoke_start < manifest_start
    assert "from logic.general_tools import execute_general_tool_call" in script
    assert "WriteAllText($shellSmokeScript" in script
    assert "-X utf8 $shellSmokeScript" in script
    assert "-X utf8 -c $shellSmoke" not in script
    for evidence in (
        "echo UPSP_SHELL_SMOKE",
        "cmd /c ver",
        "powershell -NoProfile -Command",
        "shell smoke 中文",
        "UPSP_STDOUT_SMOKE",
        "UPSP_STDERR_SMOKE",
        "exit /b 7",
        "payload_shell_backend_smoke_failed",
    ):
        assert evidence in script


def test_r692_command_succeeds_with_current_windows_backend(tmp_path):
    if os.name != "nt":
        return
    from logic.general_tools import execute_general_tool_call

    result = execute_general_tool_call(
        {
            "tool_id": "shell_command",
            "cwd": str(tmp_path),
            "command": "cmd /c ver",
            "purpose": "R692 exact command regression",
        },
        allowed_roots=[tmp_path],
    )

    assert result["status"] == "ok"
    assert result["exit_code"] == 0
    assert "Windows" in result["stdout"]
