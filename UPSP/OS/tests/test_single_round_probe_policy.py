from __future__ import annotations

import os
import sys

import pytest


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
from runtime_test_helpers import RuntimeTestMixin


def _schema_name(schema: dict) -> str:
    return str(
        schema.get("name")
        or (schema.get("function") or {}).get("name")
        or ""
    )


def test_spec609_probe_schema_exposes_only_step_terminal_tools(monkeypatch) -> None:
    from engines.executor import APIExecutor

    monkeypatch.setenv("UPSP_SINGLE_ROUND_PROBE", "1")
    executor = object.__new__(APIExecutor)

    setup = executor._native_tools_for_step(
        "setup",
        "openai_responses",
        execution_permission_level="limited",
    )
    reaction = executor._native_tools_for_step(
        "reaction",
        "openai_responses",
        execution_permission_level="limited",
    )
    cleanup = executor._native_tools_for_step(
        "cleanup",
        "openai_responses",
        execution_permission_level="limited",
    )

    assert [_schema_name(item) for item in setup] == ["setup_finalize"]
    assert reaction == []
    assert [_schema_name(item) for item in cleanup] == ["cleanup_finalize"]


def test_spec609_probe_rejects_unadvertised_reaction_tool_call(monkeypatch) -> None:
    from engines.reaction_iteration import parse_reaction_iteration_result

    monkeypatch.setenv("UPSP_SINGLE_ROUND_PROBE", "1")
    result = parse_reaction_iteration_result({
        "response": "",
        "tool_call_envelopes": [{
            "tool_id": "memory_write",
            "tool_class": "write_tool",
            "call_id": "call_forbidden_memory",
            "source": "provider_tool_call",
            "parse_status": "ok",
            "arguments": {"title": "must-not-write"},
        }],
    }, active_protocol_tool_guides=[])

    parsed = result.parsed_reaction
    assert parsed.get("memory_write_declarations") == []
    assert any(
        item.get("reason") == "single_round_probe_tool_forbidden"
        for item in parsed.get("invalid_tool_requests") or []
    )


def test_spec611_probe_isolates_background_flags_before_round_choice() -> None:
    from logic.single_round_probe_policy import isolate_single_round_probe_flags

    class FakeStateStore:
        def __init__(self) -> None:
            self.writes = []

        def set_flag(self, name, value) -> None:
            self.writes.append((name, value))

    sm = FakeStateStore()
    flags, receipt = isolate_single_round_probe_flags(
        sm,
        {
            "calendar_day_due": True,
            "standby_due": True,
            "user_message_waiting": True,
            "api_degraded": False,
        },
        enabled=True,
    )

    assert flags["user_message_waiting"] is True
    assert flags["calendar_day_due"] is False
    assert flags["standby_due"] is False
    assert receipt == {
        "enabled": True,
        "status": "prepared",
        "suppressed_flags": ["calendar_day_due", "standby_due"],
        "active_flags": ["user_message_waiting"],
    }
    assert sm.writes == [
        ("calendar_day_due", False),
        ("standby_due", False),
    ]


def test_spec611_probe_does_not_hide_emergency_flags() -> None:
    from logic.single_round_probe_policy import isolate_single_round_probe_flags

    class FakeStateStore:
        def set_flag(self, name, value) -> None:
            raise AssertionError("emergency flags must not be silently cleared")

    flags, receipt = isolate_single_round_probe_flags(
        FakeStateStore(),
        {"api_degraded": True, "user_message_waiting": True},
        enabled=True,
    )

    assert flags["api_degraded"] is True
    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "single_round_probe_background_flags_present"


class TestSpec611SingleRoundProbeHardStop(RuntimeTestMixin):
    @staticmethod
    def _minimal_reaction_context(runtime, monkeypatch) -> None:
        assembler = runtime.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

    def test_probe_rejects_noninteractive_round_before_runtime_mutation(
            self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("UPSP_SINGLE_ROUND_PROBE", "1")
        rt = self._make_runtime(tmp_path)
        before_round = rt.sm.get_total_round()

        with pytest.raises(
                RuntimeError, match="single_round_probe_non_interactive_round"):
            rt._run_one_round(
                "rhythm",
                rt.sm.load(),
                {
                    "calendar_day_due": True,
                    "user_message_waiting": True,
                },
            )

        assert rt.sm.get_total_round() == before_round
        assert rt.sm.get("base.runtime.phase") == "idle"

    def test_probe_starts_at_most_one_reaction_provider_call(
            self, tmp_path, monkeypatch) -> None:
        monkeypatch.setenv("UPSP_SINGLE_ROUND_PROBE", "1")
        rt = self._make_runtime(tmp_path)
        self._minimal_reaction_context(rt, monkeypatch)

        class EmptyExecutor:
            def __init__(self) -> None:
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(step)
                return {"response": "", "tool_call_envelopes": []}

        executor = EmptyExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == ["reaction"]
        assert result["aborted"] is True
        assert result["error"] == "single_round_probe_reaction_call_limit"
        assert result["_single_round_probe_hard_stop"] == {
            "status": "hard_stop",
            "reason": "single_round_probe_reaction_call_limit",
            "provider_reaction_calls": 1,
            "limit": 1,
        }
        assert result["_reaction_iterations"][-1]["provider_call_started"] is False

    def test_controlled_dogfood_stops_before_reaction_call_limit_plus_one(
            self, tmp_path, monkeypatch) -> None:
        monkeypatch.delenv("UPSP_SINGLE_ROUND_PROBE", raising=False)
        monkeypatch.setenv("UPSP_REACTION_PROVIDER_CALL_LIMIT", "2")
        rt = self._make_runtime(tmp_path)
        self._minimal_reaction_context(rt, monkeypatch)

        class EmptyExecutor:
            def __init__(self) -> None:
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(step)
                return {"response": "", "tool_call_envelopes": []}

        executor = EmptyExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == ["reaction", "reaction"]
        assert result["aborted"] is True
        assert result["error"] == "reaction_provider_call_limit_reached"
        assert result["_provider_call_hard_stop"] == {
            "status": "hard_stop",
            "reason": "reaction_provider_call_limit_reached",
            "provider_reaction_calls": 2,
            "limit": 2,
            "source": "controlled_dogfood",
        }
        assert result["_single_round_probe_hard_stop"] == {}
        assert result["response"]
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "blocked"
        assert result["_reaction_iterations"][-1]["provider_call_started"] is False


def test_reaction_provider_call_limit_policy_fails_closed_on_invalid_value() -> None:
    from logic.reaction_call_limit import (
        configured_reaction_provider_call_limit,
        reaction_provider_call_limit_policy,
    )

    assert reaction_provider_call_limit_policy({}) == {
        "enabled": False,
        "limit": 0,
        "source": "default_off",
    }
    with pytest.raises(ValueError, match="reaction_provider_call_limit_invalid"):
        configured_reaction_provider_call_limit({
            "UPSP_REACTION_PROVIDER_CALL_LIMIT": "fourteen",
        })
    with pytest.raises(ValueError, match="reaction_provider_call_limit_out_of_range"):
        configured_reaction_provider_call_limit({
            "UPSP_REACTION_PROVIDER_CALL_LIMIT": "101",
        })
