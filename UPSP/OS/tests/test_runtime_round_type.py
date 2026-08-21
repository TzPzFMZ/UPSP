import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


class TestRuntimeRoundType(RuntimeTestMixin):
    def test_round_context_carries_frozen_context_window(self):
        from engines.round_context import RoundContext

        context = RoundContext(
            1,
            "interactive",
            {},
            {},
            context_window_tokens=1_000_000,
        )

        assert context.context_window_tokens == 1_000_000

    def test_determine_round_type_interactive(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        flags = {"user_message_waiting": True}
        assert rt._determine_round_type(flags) == "interactive"

    def test_determine_round_type_rhythm(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        flags = {"rhythm_due": True}
        assert rt._determine_round_type(flags) == "rhythm"

    def test_determine_round_type_relay(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        flags = {"continue_requested": True}
        assert rt._determine_round_type(flags) == "relay"

    def test_reserved_fatigue_flag_does_not_trigger_a_round(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        flags = {"fatigue_expired": True}
        assert rt._determine_round_type(flags) is None

    def test_retired_evolution_flag_does_not_trigger_a_round(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        flags = {"evolution_pending": True}

        assert rt._determine_round_type(flags, rt.sm.load()) is None

    def test_determine_round_type_standby(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        flags = {"standby_due": True}
        assert rt._determine_round_type(flags) == "standby"

    def test_heartbeat_trigger_groups_cover_all_state_flags(self):
        from engines.heartbeat import (
            HEARTBEAT_QUALIFIER_FLAGS,
            HEARTBEAT_TRIGGER_GROUPS,
            HEARTBEAT_HEALTH_ONLY_FLAGS,
            HEARTBEAT_LOCAL_MAINTENANCE_FLAGS,
        )
        from schemas.state import default_state

        reserved = {"fatigue_expired", "identity_timeout", "process_down"}
        state_flags = set(default_state()["base"]["heartbeat_flags"]) - reserved
        grouped = set()
        for flags in HEARTBEAT_TRIGGER_GROUPS.values():
            grouped.update(flags)
        covered = (
            grouped
            | set(HEARTBEAT_QUALIFIER_FLAGS)
            | set(HEARTBEAT_HEALTH_ONLY_FLAGS)
            | set(HEARTBEAT_LOCAL_MAINTENANCE_FLAGS)
        )

        assert covered == state_flags
        assert set(HEARTBEAT_TRIGGER_GROUPS) == {
            "interaction",
            "rhythm",
            "relay",
            "autonomous",
            "standby",
        }
        assert HEARTBEAT_LOCAL_MAINTENANCE_FLAGS == (
            "feeling_settle_due",
        )

    def test_health_and_shelve_flags_have_round_trigger_policy(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        assert rt._determine_round_type({"api_degraded": True}) == "rhythm"
        assert rt._determine_round_type({"process_down": True}) is None
        assert rt._determine_round_type({"token_usage_warning": True}) is None
        assert rt._determine_round_type({"context_pressure": True}) == "rhythm"
        assert rt._determine_round_type({"cache_compaction_due": True}) is None
        assert rt._determine_round_type({"shelve_timer_expired": True}) == "standby"
        assert rt._determine_round_type({"identity_timeout": True}) is None

    def test_spec351_emergency_guide_is_the_only_current_guide(self):
        from logic.rhythm_guidance import current_guide

        guide = current_guide({
            "api_degraded": True,
            "rhythm_due": True,
            "user_message_waiting": True,
        })

        assert guide["kind"] == "emergency_handling_guide"
        assert "API 异常处理" in guide["text"]
        assert "主轴节律" not in guide["text"]
        assert "用户交互" not in guide["text"]

    def test_spec352_emergency_attempt_budget_auto_defers_after_ten_calls(self):
        from logic.rhythm_guidance import emergency_attempt_decision

        assert emergency_attempt_decision("api_degraded", 7)["action"] == "continue"
        assert emergency_attempt_decision("api_degraded", 8)["action"] == "nudge_defer"
        decision = emergency_attempt_decision("api_degraded", 10)
        assert decision["action"] == "auto_defer"
        assert decision["defer_seconds"] == 3600

    def test_spec352_emergency_attempt_count_uses_actual_calls_not_tool_set(self):
        from engines.reaction_loop import ReactionLoopRunner

        assert ReactionLoopRunner._emergency_tool_attempt_count(
            ["fault_record", "fault_record", "reaction_finalize"],
            [{"tool_id": "shell_command"}, {"tool_id": "file_glob"}],
        ) == 4

    def test_spec352_alert_settled_requires_applied_receipt(self):
        from engines.reaction_loop import ReactionLoopRunner

        assert not ReactionLoopRunner._alert_settled_in_iteration(
            "api_degraded",
            [{"alert_type": "api_degraded", "status": "error"}],
        )
        assert ReactionLoopRunner._alert_settled_in_iteration(
            "api_degraded",
            [{"alert_type": "api_degraded", "status": "applied"}],
        )

    def test_fault_record_does_not_settle_reserved_process_flag(self):
        from engines.reaction_loop import ReactionLoopRunner

        state = {
            "base": {
                "heartbeat_flags": {
                    "process_down": True,
                    "rhythm_due": True,
                }
            }
        }

        completed = ReactionLoopRunner._guide_completed_flags_from_receipts(
            [{
                "tool_id": "fault_record",
                "status": "applied",
                "fault_type": "runtime_exception",
                "severity": "error",
            }],
            state=state,
            round_type="rhythm",
        )

        assert completed == set()

    def test_warning_rhythm_does_not_update_main_axis_counter(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set("base.meta.last_rhythm_round", 32)

        rt.cleanup_pipeline._finalize_flags(rt.sm.load(), "rhythm", 40)

        assert rt.sm.get("base.meta.last_rhythm_round") == 32
        assert rt.sm.get("base.heartbeat_flags.api_degraded") is True

    def test_spec358_alert_flag_clears_only_after_settle_receipt(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()
        state["base"]["heartbeat_flags"]["api_degraded"] = True

        rt.cleanup_pipeline._finalize_flags(
            state,
            "rhythm",
            41,
            result={
                "_alert_mode_settle_receipts": [{
                    "tool_id": "alert_mode_settle",
                    "status": "applied",
                    "alert_type": "api_degraded",
                    "alert_status": "deferred",
                    "cleared_flags": ["api_degraded"],
                }]
            },
        )

        assert rt.sm.get("base.heartbeat_flags.api_degraded") is False

    def test_finalize_flags_records_round_closed_at(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()

        rt.cleanup_pipeline._finalize_flags(state, "interactive", 41)

        assert rt.sm.get("base.meta.last_round_closed_at")

    def test_update_token_usage_uses_endpoint_context_window(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            def get_context_window_for_endpoint(self, endpoint):
                assert endpoint == "primary"
                return 1000

        rt.cfg = TokenConfig()

        rt._update_token_usage({
            "endpoint": "primary",
            "tokens_input": 600,
            "tokens_output": 200,
        })

        token = rt.sm.load()["base"]["token_usage"]
        assert token["window_size"] == 1000
        assert token["current_tokens"] == 600
        assert token["usage_ratio"] == 0.6

    def test_update_token_usage_prefers_frame_frozen_window(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return 4000

            @staticmethod
            def get_token_params():
                return {"warning_ratio": 0.9}

        rt.cfg = TokenConfig()
        rt._update_token_usage({
            "endpoint": "primary",
            "tokens_input": 1800,
            "tokens_output": 1,
            "provider_request_envelope": {"context_window_tokens": 2000},
        })

        assert rt.sm.get("base.token_usage.window_size") == 2000
        assert rt.services.cache_pressure_observation["usage_ratio"] == 0.9

    def test_update_token_usage_preserves_frame_frozen_unknown_window(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return 4000

        rt.cfg = TokenConfig()
        rt._update_token_usage({
            "endpoint": "primary",
            "tokens_input": 3600,
            "tokens_output": 1,
            "provider_request_envelope": {"context_window_tokens": None},
        })

        assert rt.services.cache_pressure_observation == {
            "kind": "unknown_window_fallback",
            "endpoint": "primary",
            "input_tokens": 3600,
            "context_window": None,
            "usage_ratio": None,
        }

    def test_invalid_runtime_watermark_cannot_create_pressure(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return 1000

            @staticmethod
            def get_token_params():
                return {"warning_ratio": float("nan")}

        rt.cfg = TokenConfig()
        rt._update_token_usage({
            "endpoint": "primary",
            "tokens_input": 1000,
            "tokens_output": 1,
        })

        assert rt.services.cache_pressure_observation == {}

    def test_cleanup_clears_applied_compaction_debt_after_flag(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        with pytest.raises(ValueError, match="cache_compaction_due"):
            rt.sm.set_flag("cache_compaction_due", True)
        assert rt._determine_round_type({"cache_compaction_due": True}) is None

    def test_token_pressure_uses_full_input_and_is_sticky(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(endpoint):
                assert endpoint == "backup-profile"
                return 100000

            @staticmethod
            def get_token_params():
                return {"warning_ratio": 0.9}

        rt.cfg = TokenConfig()
        rt._update_token_usage({
            "endpoint": "backup-profile",
            "tokens_input": 89999,
            "tokens_output": 500,
        })
        assert rt.services.cache_pressure_observation == {}
        rt._update_token_usage({
            "endpoint": "backup-profile",
            "tokens_input": 90000,
            "tokens_output": 500,
        })
        rt._update_token_usage({
            "endpoint": "backup-profile",
            "tokens_input": 100,
            "tokens_output": 1,
        })

        assert rt.services.cache_pressure_observation == {
            "kind": "token_ratio",
            "endpoint": "backup-profile",
            "input_tokens": 90000,
            "context_window": 100000,
            "round_context_window_tokens": 100000,
            "usage_ratio": 0.9,
            "threshold": 0.9,
        }

    def test_token_pressure_survives_state_projection_failure(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return 1000

            @staticmethod
            def get_token_params():
                return {"warning_ratio": 0.9}

        rt.cfg = TokenConfig()
        monkeypatch.setattr(
            rt.sm,
            "update_token_usage",
            lambda **_kwargs: (_ for _ in ()).throw(OSError("state busy")),
        )

        rt._update_token_usage(
            {"endpoint": "primary", "tokens_input": 900, "tokens_output": 1},
            round_num=742,
            phase="reaction",
            iteration=3,
        )

        assert rt.services.cache_pressure_observation == {
            "kind": "token_ratio",
            "endpoint": "primary",
            "input_tokens": 900,
            "context_window": 1000,
            "round_context_window_tokens": 1000,
            "usage_ratio": 0.9,
            "threshold": 0.9,
            "frame_id": "R000742:reaction:3",
        }

    def test_token_pressure_normalizes_anthropic_cached_input(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return 1000

            @staticmethod
            def get_token_params():
                return {"warning_ratio": 0.9}

        rt.cfg = TokenConfig()
        rt._update_token_usage({
            "endpoint": "anthropic-profile",
            "raw_usage": {
                "input_tokens": 100,
                "cache_creation_input_tokens": 300,
                "cache_read_input_tokens": 500,
            },
            "tokens_input": 100,
            "tokens_output": 20,
        })

        assert rt.sm.get("base.token_usage.current_tokens") == 900
        assert rt.services.cache_pressure_observation["usage_ratio"] == 0.9

    def test_invalid_usage_does_not_overwrite_last_real_sample(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return 1000

            @staticmethod
            def get_token_params():
                return {"warning_ratio": 0.9}

        rt.cfg = TokenConfig()
        rt._update_token_usage({
            "endpoint": "primary",
            "tokens_input": 400,
            "tokens_output": 10,
        })
        before = dict(rt.sm.load()["base"]["token_usage"])
        rt._update_token_usage({
            "endpoint": "primary",
            "tokens_input": True,
            "tokens_output": 0,
        })

        assert rt.sm.load()["base"]["token_usage"] == before
        assert rt.services.cache_pressure_observation == {}

    def test_unknown_window_records_character_fallback_without_fake_zero(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return None

        rt.cfg = TokenConfig()
        before = dict(rt.sm.load()["base"]["token_usage"])
        rt._update_token_usage({
            "endpoint": "unknown-profile",
            "tokens_input": 900,
            "tokens_output": 5,
        })

        assert rt.sm.load()["base"]["token_usage"] == before
        assert rt.services.cache_pressure_observation == {
            "kind": "unknown_window_fallback",
            "endpoint": "unknown-profile",
            "input_tokens": 900,
            "context_window": None,
            "usage_ratio": None,
        }

    def test_context_too_long_records_unmeasured_pressure(self, tmp_path,
                                                          monkeypatch):
        import pytest
        from errors import APIBridgeError

        rt = self._make_runtime(tmp_path)

        def fail(*_args, **_kwargs):
            raise APIBridgeError(
                "backup-profile",
                "HTTP 413: context_length_exceeded",
                status_code=413,
            )

        monkeypatch.setattr(rt.audit, "call_llm", fail)
        with pytest.raises(APIBridgeError):
            rt.setup_runner._call_llm_with_round_audit(
                "setup", "system", [], 1)

        assert rt.services.cache_pressure_observation == {
            "kind": "context_too_long",
            "frame_id": "R000001:setup:1",
            "endpoint": "backup-profile",
            "input_tokens": None,
            "context_window": None,
            "usage_ratio": None,
        }

    def test_local_context_window_validation_error_is_not_cache_pressure(
            self, tmp_path, monkeypatch):
        import pytest

        rt = self._make_runtime(tmp_path)

        def fail(*_args, **_kwargs):
            raise ValueError("invalid model context window detection")

        monkeypatch.setattr(rt.audit, "call_llm", fail)
        with pytest.raises(ValueError):
            rt.setup_runner._call_llm_with_round_audit(
                "setup", "system", [], 1)

        assert rt.services.cache_pressure_observation == {}

    def test_open_compaction_debt_absorbs_setup_and_reaction_pressure(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class TokenConfig:
            @staticmethod
            def get_context_window_for_endpoint(_endpoint):
                return 1000

            @staticmethod
            def get_token_params():
                return {"warning_ratio": 0.9}

        rt.cfg = TokenConfig()
        rt.ctx_store._write_json_atomic(
            rt.ctx_store.cache_compaction_debt_path(),
            {
                "schema_version": "cache_compaction_debt.v2",
                "status": "open",
                "phase": "staging",
                "compaction_plan": {"shards": [{"shard_id": "shard_01"}]},
            },
        )

        rt._update_token_usage(
            {"endpoint": "primary", "tokens_input": 950, "tokens_output": 1},
            round_num=742, phase="setup", iteration=1,
        )
        rt._update_token_usage(
            {"endpoint": "primary", "tokens_input": 950, "tokens_output": 1},
            round_num=742, phase="reaction", iteration=1,
        )

        assert rt.services.cache_pressure_observation == {}

    def test_next_round_type_does_not_bypass_heartbeat(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()
        state["base"]["runtime"]["next_round"] = {"type": "relay"}

        assert rt._determine_round_type({}, state) is None

    def test_determine_none_when_no_flags(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        assert rt._determine_round_type({}) is None

    def test_rhythm_over_interactive(self, tmp_path):
        """节律与交互可合轮，主轮型按节律处理。"""
        rt = self._make_runtime(tmp_path)
        flags = {"user_message_waiting": True, "rhythm_due": True}
        assert rt._determine_round_type(flags) == "rhythm"

    def test_spec356_round_decision_exposes_coalesced_guide_queue(self):
        from engines.heartbeat import round_decision_from_heartbeat_flags

        decision = round_decision_from_heartbeat_flags({
            "api_degraded": True,
            "process_down": True,
            "token_usage_warning": True,
            "context_pressure": True,
            "cache_compaction_due": True,
            "rhythm_due": True,
            "calendar_day_due": True,
            "calendar_week_due": True,
            "user_message_waiting": True,
            "continue_requested": True,
        })

        assert decision["round_type"] == "rhythm"
        assert decision["coalesced"] is True
        assert [item["kind"] for item in decision["guide_queue"]] == [
            "emergency",
            "context_pressure",
            "main_axis_rhythm",
            "calendar_day",
            "calendar_week",
            "interaction",
        ]
        assert decision["guide_queue"][0]["flags"] == ["api_degraded"]
        assert decision["guide_queue"][1]["flags"] == ["context_pressure"]
        assert decision["deferred_items"] == [
            {"kind": "relay", "flags": ["continue_requested"]}
        ]

    def test_spec767_active_memory_cycle_recovers_before_new_calendar_day(self):
        from logic.rhythm_guidance import current_guide

        flags = {
            "calendar_day_due": True,
            "memory_compression_due": True,
            "calendar_week_due": True,
            "calendar_month_due": True,
            "user_message_waiting": True,
        }

        memory = current_guide(flags)
        assert memory["kind"] == "memory_compression_rhythm_guide"
        assert memory["items"] == [{
            "flag": "memory_compression_due",
            "title": "记忆语义压缩",
        }]

        day = current_guide(flags, completed_flags={"memory_compression_due"})
        assert day["kind"] == "calendar_rhythm_guide"
        assert day["items"] == [{"flag": "calendar_day_due", "title": "日志"}]

        week = current_guide(flags, completed_flags={
            "calendar_day_due", "memory_compression_due"})
        assert week["kind"] == "calendar_rhythm_guide"
        assert week["items"] == [{"flag": "calendar_week_due", "title": "周志"}]

        interaction = current_guide(
            flags,
            completed_flags={
                "calendar_day_due",
                "memory_compression_due",
                "calendar_week_due",
                "calendar_month_due",
            },
        )
        assert interaction["kind"] == "interactive_guide"

    def test_spec767_normal_daily_cycle_still_writes_log_before_compression(self):
        from logic.rhythm_guidance import current_guide

        flags = {"calendar_day_due": True}
        day = current_guide(flags)
        assert day["kind"] == "calendar_rhythm_guide"
        assert day["items"] == [{"flag": "calendar_day_due", "title": "日志"}]

        flags["memory_compression_due"] = True
        memory = current_guide(flags, completed_flags={"calendar_day_due"})
        assert memory["kind"] == "memory_compression_rhythm_guide"

    def test_spec448_coalesced_calendar_then_interaction_guides_are_same_round(self):
        from logic.rhythm_guidance import current_guide

        flags = {
            "calendar_day_due": True,
            "user_message_waiting": True,
        }

        calendar = current_guide(flags)
        assert calendar["kind"] == "calendar_rhythm_guide"
        assert "user_message_waiting" in calendar["text"]
        assert "same-round" in calendar["text"]

        interaction = current_guide(flags, completed_flags={"calendar_day_due"})
        assert interaction["kind"] == "interactive_guide"
        assert "user_message_waiting" in interaction["text"]
        assert "relay" not in interaction["text"].lower()

    def test_spec448_rhythm_does_not_hide_interaction_after_calendar_settled(self):
        from logic.rhythm_guidance import current_guide

        flags = {
            "calendar_day_due": True,
            "user_message_waiting": True,
        }

        guide = current_guide(flags, completed_flags={"calendar_day_due"})

        assert guide["kind"] == "interactive_guide"
        assert guide.get("items") == [
            {"flag": "user_message_waiting", "title": "用户交互"}
        ]

    def test_spec448_workbench_guide_suppression_ends_after_rhythm_settled(self):
        from engines.reaction_loop import ReactionLoopRunner

        state = {
            "base": {
                "heartbeat_flags": {
                    "calendar_day_due": True,
                    "user_message_waiting": True,
                }
            }
        }

        assert ReactionLoopRunner._suppress_workbench_guides("rhythm", state) is True

        state["base"]["runtime"] = {"guide_completed_flags": ["calendar_day_due"]}

        assert ReactionLoopRunner._suppress_workbench_guides("rhythm", state) is False

    def test_spec356_relay_is_deferred_behind_interaction(self):
        from engines.heartbeat import round_decision_from_heartbeat_flags

        decision = round_decision_from_heartbeat_flags({
            "user_message_waiting": True,
            "continue_requested": True,
        })

        assert decision["round_type"] == "interactive"
        assert [item["kind"] for item in decision["guide_queue"]] == ["interaction"]
        assert decision["deferred_items"] == [
            {"kind": "relay", "flags": ["continue_requested"]}
        ]

    def test_spec471_context_pressure_is_a_rhythm_maintenance_guide(self):
        from engines.heartbeat import round_decision_from_heartbeat_flags
        from logic.rhythm_guidance import current_guide

        decision = round_decision_from_heartbeat_flags({
            "context_pressure": True,
        })

        assert decision["round_type"] == "rhythm"
        assert decision["guide_queue"] == [{
            "kind": "context_pressure",
            "flags": ["context_pressure"],
        }]
        guide = current_guide({"context_pressure": True})
        assert guide["kind"] == "context_pressure_rhythm_guide"
        assert guide["items"][0]["flag"] == "context_pressure"
        assert guide["items"][0]["title"] == "上下文压力处理"

    def test_identity_timeout_is_not_round_type_or_subtype(self, tmp_path):
        """身份超时只是交互对象提示事件，不制造新的交互轮 subtype。"""
        rt = self._make_runtime(tmp_path)
        flags = {"user_message_waiting": True, "identity_timeout": True}

        assert rt._determine_round_type(flags) == "interactive"
        assert "next_round" not in rt.sm.load()["base"]["runtime"]

    def test_write_heartbeat_handoff_uses_now_only_setup_fact_kind(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, content, *, kind, step, **kwargs):
                self.entries.append({
                    "round": round_num,
                    "role": role,
                    "content": content,
                    "kind": kind,
                    "step": step,
                    "kwargs": kwargs,
                })

        ctx = DummyContext()
        rt.ctx_store = ctx

        rt._write_heartbeat_handoff(
            42,
            "relay",
            {"continue_requested": True, "identity_timeout": True},
        )

        assert ctx.entries == [{
            "round": 42,
            "role": "system",
            "content": ctx.entries[0]["content"],
            "kind": "setup_fact",
            "step": "setup",
            "kwargs": {
                "interaction_object": "system",
                "identity_status": "system",
                "interaction_source": "heartbeat",
            },
        }]
        assert "本步不是确认上轮" in ctx.entries[0]["content"]
        assert "中继执行反应步" in ctx.entries[0]["content"]
        assert "先执行交接里的第一动作" in ctx.entries[0]["content"]
        assert "continue_requested" in ctx.entries[0]["content"]
        assert "identity_timeout" not in ctx.entries[0]["content"]
