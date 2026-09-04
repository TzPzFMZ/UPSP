import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from native_tool_test_helpers import (
    _FakeCompletedProcess,
    _load_native_tool_calling_gate,
)


class TestSpec144NativeToolCallingGate:
    def test_spec144_gate_runs_default_matrix_without_round(self):
        gate = _load_native_tool_calling_gate()
        calls = []

        def runner(command, **kwargs):
            calls.append({"command": command, "kwargs": kwargs})
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([], runner=runner)

        assert summary["ok"] is True
        assert len(calls) == 1
        matrix = summary["checks"]["matrix"]
        assert matrix["status"] == "passed"
        assert "stdout" not in matrix
        assert matrix["stdout_bytes"] == len("matrix ok".encode("utf-8"))
        assert summary["checks"]["round"]["status"] == "skipped"
        assert summary["checks"]["recent_rounds"]["status"] == "skipped"
        command = calls[0]["command"]
        assert command[:3] == [sys.executable, "-m", "pytest"]
        assert command[3:] == [
            "OS/tests/test_native_round_inspector.py",
            "OS/tests/test_native_tool_calls.py",
            "-q",
            "-k",
            "spec143 or normalizes_non_native_tool_role or mixed_native",
        ]
        assert calls[0]["kwargs"]["cwd"].endswith(os.path.sep + "UPSP")

    def test_spec144_gate_adds_latest_round_requirements(self):
        gate = _load_native_tool_calling_gate()
        calls = []

        def runner(command, **kwargs):
            calls.append({"command": command, "kwargs": kwargs})
            if "inspect_native_tool_round.py" in " ".join(command):
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": "round_33.jsonl",
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--latest-round",
            "--require-tool",
            "file_read",
            "--require-provider",
            "openai_responses",
        ], runner=runner)

        assert summary["ok"] is True
        assert len(calls) == 2
        round_command = calls[1]["command"]
        assert any(part.endswith("inspect_native_tool_round.py")
                   for part in round_command)
        assert "--latest" in round_command
        assert "--require-tool" in round_command
        assert "file_read" in round_command
        assert "--require-provider" in round_command
        assert "openai_responses" in round_command
        assert "--require-round-closed" in round_command
        assert "--require-runtime-audit-ok" in round_command
        assert summary["checks"]["round"]["status"] == "passed"
        assert summary["checks"]["round"]["providers"] == ["openai_responses"]
        assert summary["checks"]["round"]["tool_ids"] == ["file_read"]

    def test_spec146_gate_passes_required_tool_result_status_to_round_checker(self):
        gate = _load_native_tool_calling_gate()
        calls = []

        def runner(command, **kwargs):
            calls.append({"command": command, "kwargs": kwargs})
            if "inspect_native_tool_round.py" in " ".join(command):
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": "round_146.jsonl",
                        "round_num": 146,
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "tool_result_statuses": {"ok": 1},
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--latest-round",
            "--require-tool",
            "file_read",
            "--require-provider",
            "openai_responses",
            "--require-tool-result-status",
            "ok",
        ], runner=runner)

        round_command = [
            call["command"] for call in calls
            if "inspect_native_tool_round.py" in " ".join(call["command"])
        ][0]
        assert "--require-tool-result-status" in round_command
        assert "ok" in round_command
        assert summary["checks"]["round"]["tool_result_statuses"] == {"ok": 1}

    def test_spec147_gate_passes_reading_dogfood_and_read_only_requirements(self):
        gate = _load_native_tool_calling_gate()
        calls = []

        def runner(command, **kwargs):
            calls.append({"command": command, "kwargs": kwargs})
            if "inspect_native_tool_round.py" in " ".join(command):
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": "round_147.jsonl",
                        "round_num": 147,
                        "dogfood_label": "读书轮",
                        "reading_dogfood": True,
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "read_only_tool_ids": ["file_read"],
                        "non_read_only_tool_ids": [],
                        "tool_classes": {"file_read": "read_tool"},
                        "tool_families": {"file_read": "general_tool"},
                        "tool_result_statuses": {"ok": 1},
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--latest-round",
            "--require-reading-dogfood",
            "--require-dogfood-label",
            "读书轮",
            "--require-read-only-tools-only",
            "--require-tool",
            "file_read",
            "--require-provider",
            "openai_responses",
            "--require-tool-result-status",
            "ok",
        ], runner=runner)

        round_command = [
            call["command"] for call in calls
            if "inspect_native_tool_round.py" in " ".join(call["command"])
        ][0]
        assert "--require-reading-dogfood" in round_command
        assert "--require-dogfood-label" in round_command
        assert "读书轮" in round_command
        assert "--require-read-only-tools-only" in round_command
        round_summary = summary["checks"]["round"]
        assert round_summary["dogfood_label"] == "读书轮"
        assert round_summary["reading_dogfood"] is True
        assert round_summary["read_only_tool_ids"] == ["file_read"]
        assert round_summary["non_read_only_tool_ids"] == []
        assert round_summary["tool_classes"] == {"file_read": "read_tool"}
        assert round_summary["tool_families"] == {"file_read": "general_tool"}

    def test_spec144_gate_scans_recent_rounds_and_aggregates_requirements(
            self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        for round_num in [40, 41, 42]:
            (round_dir / f"round_{round_num}.jsonl").write_text(
                "{}\n", encoding="utf-8")
        calls = []

        def runner(command, **kwargs):
            calls.append({"command": command, "kwargs": kwargs})
            if "inspect_native_tool_round.py" in " ".join(command):
                round_file = command[command.index("--round-file") + 1]
                tool_ids = ["file_read"] if round_file.endswith("round_42.jsonl") else []
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": round_file,
                        "providers": ["openai_responses"],
                        "tool_ids": tool_ids,
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "2",
            "--round-dir",
            str(round_dir),
            "--require-tool",
            "file_read",
            "--require-provider",
            "openai_responses",
        ], runner=runner)

        assert summary["ok"] is True
        recent = summary["checks"]["recent_rounds"]
        assert recent["status"] == "passed"
        assert recent["round_count"] == 2
        assert [os.path.basename(path) for path in recent["round_files"]] == [
            "round_41.jsonl",
            "round_42.jsonl",
        ]
        assert recent["providers"] == ["openai_responses"]
        assert recent["tool_ids"] == ["file_read"]
        assert recent["failed_rounds"] == []
        round_commands = [
            call["command"] for call in calls
            if "inspect_native_tool_round.py" in " ".join(call["command"])
        ]
        assert len(round_commands) == 2
        for command in round_commands:
            assert "--require-round-closed" in command
            assert "--require-runtime-audit-ok" in command
            assert "--require-tool" not in command
            assert "--require-provider" not in command

    def test_spec146_recent_gate_requires_tool_per_round_when_status_is_required(
            self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        for round_num in [50, 51]:
            (round_dir / f"round_{round_num}.jsonl").write_text(
                "{}\n", encoding="utf-8")

        def runner(command, **kwargs):
            if "inspect_native_tool_round.py" in " ".join(command):
                round_file = command[command.index("--round-file") + 1]
                tool_ids = ["file_read"] if round_file.endswith("round_51.jsonl") else []
                statuses = {"ok": 1} if tool_ids else {}
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": round_file,
                        "providers": ["openai_responses"],
                        "tool_ids": tool_ids,
                        "tool_result_statuses": statuses,
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "2",
            "--round-dir",
            str(round_dir),
            "--require-tool",
            "file_read",
            "--require-provider",
            "openai_responses",
            "--require-tool-result-status",
            "ok",
        ], runner=runner)

        assert summary["ok"] is False
        recent = summary["checks"]["recent_rounds"]
        assert recent["status"] == "failed"
        assert recent["failed_rounds"] == [{
            "round_file": os.path.join(str(round_dir), "round_50.jsonl"),
            "exit_code": 0,
            "issues": ["missing_required_tool_in_round:file_read"],
        }]
        assert "recent_round_failed" in summary["issues"]

    def test_spec147_recent_gate_does_not_hide_missing_reading_label_or_legacy(
            self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        for round_num in [60, 61]:
            (round_dir / f"round_{round_num}.jsonl").write_text(
                "{}\n", encoding="utf-8")

        def runner(command, **kwargs):
            if "inspect_native_tool_round.py" in " ".join(command):
                round_file = command[command.index("--round-file") + 1]
                if round_file.endswith("round_60.jsonl"):
                    return _FakeCompletedProcess(
                        returncode=1,
                        stdout=json.dumps({
                            "ok": False,
                            "round_file": round_file,
                            "providers": ["openai_responses"],
                            "tool_ids": [],
                            "read_only_tool_ids": [],
                            "non_read_only_tool_ids": [],
                            "reading_dogfood": False,
                            "issues": [
                                "reading_dogfood_label_missing:读书轮",
                                "legacy_text_request_seen",
                            ],
                        }),
                    )
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": round_file,
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "read_only_tool_ids": ["file_read"],
                        "non_read_only_tool_ids": [],
                        "reading_dogfood": True,
                        "tool_result_statuses": {"ok": 1},
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "2",
            "--round-dir",
            str(round_dir),
            "--require-reading-dogfood",
            "--require-read-only-tools-only",
            "--require-tool",
            "file_read",
            "--require-provider",
            "openai_responses",
            "--require-tool-result-status",
            "ok",
        ], runner=runner)

        assert summary["ok"] is False
        recent = summary["checks"]["recent_rounds"]
        assert recent["status"] == "failed"
        assert recent["failed_rounds"][0]["round_file"].endswith("round_60.jsonl")
        assert "reading_dogfood_label_missing:读书轮" in recent["failed_rounds"][0]["issues"]
        assert "legacy_text_request_seen" in recent["failed_rounds"][0]["issues"]
        assert "recent_round_failed" in summary["issues"]

    def test_spec144_gate_fails_without_recent_required_tool(self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        (round_dir / "round_44.jsonl").write_text("{}\n", encoding="utf-8")

        def runner(command, **kwargs):
            if "inspect_native_tool_round.py" in " ".join(command):
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": "round_44.jsonl",
                        "providers": ["openai_responses"],
                        "tool_ids": [],
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "1",
            "--round-dir",
            str(round_dir),
            "--require-tool",
            "file_read",
        ], runner=runner)

        assert summary["ok"] is False
        assert summary["checks"]["recent_rounds"]["status"] == "failed"
        assert "missing_required_tool:file_read" in summary["issues"]

    def test_spec144_gate_allows_explicit_recent_failed_round(self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        (round_dir / "round_32.jsonl").write_text("{}\n", encoding="utf-8")

        def runner(command, **kwargs):
            if "inspect_native_tool_round.py" in " ".join(command):
                return _FakeCompletedProcess(
                    returncode=1,
                    stdout=json.dumps({
                        "ok": False,
                        "round_file": "round_32.jsonl",
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "issues": ["runtime_audit_not_ok:missing"],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "1",
            "--round-dir",
            str(round_dir),
            "--allow-failed-round",
            "round_32",
        ], runner=runner)

        assert summary["ok"] is True
        recent = summary["checks"]["recent_rounds"]
        assert recent["status"] == "passed"
        assert recent["failed_rounds"] == []
        assert recent["allowed_failed_rounds"] == [
            os.path.join(str(round_dir), "round_32.jsonl")
        ]

    def test_spec152_recent_gate_reports_matched_allowed_failed_round(
            self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        (round_dir / "round_32.jsonl").write_text("{}\n", encoding="utf-8")

        def runner(command, **kwargs):
            if "inspect_native_tool_round.py" in " ".join(command):
                return _FakeCompletedProcess(
                    returncode=1,
                    stdout=json.dumps({
                        "ok": False,
                        "round_file": "round_32.jsonl",
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "issues": ["runtime_audit_not_ok:missing"],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "1",
            "--round-dir",
            str(round_dir),
            "--allow-failed-round",
            "round_32",
        ], runner=runner)

        recent = summary["checks"]["recent_rounds"]
        assert summary["ok"] is True
        assert recent["requested_allowed_failed_rounds"] == ["round_32"]
        assert recent["matched_allowed_failed_rounds"] == ["round_32"]
        assert recent["unused_allowed_failed_rounds"] == []
        assert recent["stale_allowed_failed_rounds"] == []
        assert recent["non_failing_allowed_failed_rounds"] == []

    def test_spec152_recent_gate_reports_stale_and_non_failing_allow(
            self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        for round_num in [50, 51]:
            (round_dir / f"round_{round_num}.jsonl").write_text(
                "{}\n", encoding="utf-8")

        def runner(command, **kwargs):
            if "inspect_native_tool_round.py" in " ".join(command):
                round_file = command[command.index("--round-file") + 1]
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": round_file,
                        "round_num": 51 if round_file.endswith("round_51.jsonl") else 50,
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "2",
            "--round-dir",
            str(round_dir),
            "--allow-failed-round",
            "round_49",
            "--allow-failed-round",
            "round_51",
        ], runner=runner)

        recent = summary["checks"]["recent_rounds"]
        assert summary["ok"] is True
        assert recent["requested_allowed_failed_rounds"] == [
            "round_49",
            "round_51",
        ]
        assert recent["matched_allowed_failed_rounds"] == []
        assert recent["unused_allowed_failed_rounds"] == [
            "round_49",
            "round_51",
        ]
        assert recent["stale_allowed_failed_rounds"] == ["round_49"]
        assert recent["non_failing_allowed_failed_rounds"] == ["round_51"]

    def test_spec152_recent_gate_can_fail_on_unused_allowed_round(
            self, tmp_path):
        gate = _load_native_tool_calling_gate()
        round_dir = tmp_path / "round"
        round_dir.mkdir()
        (round_dir / "round_51.jsonl").write_text("{}\n", encoding="utf-8")

        def runner(command, **kwargs):
            if "inspect_native_tool_round.py" in " ".join(command):
                return _FakeCompletedProcess(
                    returncode=0,
                    stdout=json.dumps({
                        "ok": True,
                        "round_file": "round_51.jsonl",
                        "round_num": 51,
                        "providers": ["openai_responses"],
                        "tool_ids": ["file_read"],
                        "issues": [],
                    }),
                )
            return _FakeCompletedProcess(returncode=0, stdout="matrix ok")

        summary = gate.run_gate([
            "--recent-rounds",
            "1",
            "--round-dir",
            str(round_dir),
            "--allow-failed-round",
            "round_51",
            "--fail-on-unused-allow-failed-round",
        ], runner=runner)

        recent = summary["checks"]["recent_rounds"]
        assert summary["ok"] is False
        assert recent["status"] == "failed"
        assert recent["unused_allowed_failed_rounds"] == ["round_51"]
        assert "unused_allowed_failed_round" in recent["issues"]
        assert "unused_allowed_failed_round" in summary["issues"]

    def test_spec144_gate_does_not_echo_sensitive_stdout(self):
        gate = _load_native_tool_calling_gate()

        def runner(command, **kwargs):
            return _FakeCompletedProcess(
                returncode=1,
                stdout="arguments_json sk-secret-value",
                stderr="token=secret",
            )

        summary = gate.run_gate([], runner=runner)
        rendered = json.dumps(summary, ensure_ascii=False)

        assert summary["ok"] is False
        assert summary["checks"]["matrix"]["status"] == "failed"
        assert "matrix_failed" in summary["issues"]
        assert "arguments_json" not in rendered
        assert "sk-secret-value" not in rendered
        assert "token=secret" not in rendered
        assert summary["checks"]["matrix"]["stdout_bytes"] > 0
        assert summary["checks"]["matrix"]["stderr_bytes"] > 0
