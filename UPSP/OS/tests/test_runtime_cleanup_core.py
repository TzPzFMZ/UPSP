import os
import sys
import json

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


class TestRuntimeCleanupCore(RuntimeTestMixin):
    def _patch_minimal_cleanup(self, rt, monkeypatch):
        pipeline = rt.cleanup_pipeline
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", lambda *a, **kw: ("", []))
        monkeypatch.setattr(rt.executor, "call", lambda *a, **kw: {"response": ""})
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        monkeypatch.setattr(pipeline, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(pipeline, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(pipeline, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(pipeline, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(pipeline, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(pipeline, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)

    @staticmethod
    def _round_context(rt, round_num=1, round_type="interactive"):
        from engines.round_context import RoundContext, RuntimeTrigger

        return RoundContext(
            round_num=round_num,
            round_type=round_type,
            state=rt.sm.load(),
            flags={"user_message_waiting": True},
            trigger=RuntimeTrigger(
                "T00000001", 1, "2026-07-16T00:00:00+08:00",
                round_type, {"user_message_waiting": True}, ("hello",),
            ),
        )

    def test_cleanup_pipeline_returns_unsettled_when_finalization_fails(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        monkeypatch.setattr(
            rt.cleanup_pipeline,
            "_finalize_flags",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                RuntimeError("finalize failed")),
        )

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt), {"response": "done"})

        assert outcome["status"] == "unsettled"
        assert outcome["fatal_reasons"] == [
            "flag_finalization:finalize failed"]

    def test_state_settle_failure_keeps_feeling_flag_and_round_unsettled(
            self, tmp_path, monkeypatch):
        import engines.cleanup_pipeline as cleanup_module
        from logic.state_settlement import StateSettlementError

        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        rt.sm.set_flag("feeling_settle_due", True)
        receipt = {
            "schema_version": "state_settle_receipt.v1",
            "status": "error",
            "settlement_id": "SS-R000001",
            "reason": "simulated failure",
        }
        monkeypatch.setattr(
            cleanup_module,
            "settle_state",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                StateSettlementError(receipt)),
        )

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt), {"response": "done"})

        assert outcome["status"] == "unsettled"
        assert outcome["fatal_reasons"] == ["state_settle:simulated failure"]
        assert rt.sm.get_flags()["feeling_settle_due"] is True

    def test_cleanup_pipeline_returns_unsettled_after_api_emergency_save(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        monkeypatch.setattr(
            rt.executor,
            "call",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                RuntimeError("provider unavailable")),
        )

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt), {"response": "done"})

        assert outcome["status"] == "unsettled"
        assert outcome["fatal_reasons"] == [
            "cleanup_api:RuntimeError:provider unavailable"]

    @pytest.mark.parametrize(("owner_name", "method", "scope"), [
        ("heat", "tick_decay", "heat_decay"),
        ("runtime", "_build_forgetting_context", "forgetting_context"),
        ("runtime", "_process_forgetting_result", "forgetting_persist"),
        ("runtime", "_process_memory_lifecycle", "memory_lifecycle"),
        ("runtime", "_process_evolution_set", "evolution_set"),
        ("context", "save_round_to_cache", "round_cache_save"),
    ])
    def test_cleanup_required_obligation_failure_is_unsettled(
            self, tmp_path, monkeypatch, owner_name, method, scope):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        owner = {
            "heat": rt.heat,
            "runtime": rt.cleanup_pipeline,
            "context": rt.ctx_store,
        }[owner_name]
        monkeypatch.setattr(
            owner,
            method,
            lambda *args, **kwargs: (_ for _ in ()).throw(
                RuntimeError("simulated failure")),
        )

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt), {"response": "done"})

        assert outcome["status"] == "unsettled"
        assert any(
            reason.startswith(f"{scope}:RuntimeError:")
            for reason in outcome["fatal_reasons"]
        )

    def test_cleanup_phase_input_audit_failure_is_unsettled(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        store = rt._get_round_audit_store()
        monkeypatch.setattr(
            store,
            "_has_step_snapshot",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("audit unavailable")),
        )

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt), {"response": "done"})

        assert outcome["status"] == "unsettled"
        assert any(
            reason.startswith("phase_input_audit:OSError:")
            for reason in outcome["fatal_reasons"]
        )

    def test_rhythm_cleanup_distinguishes_fatal_and_degraded_failures(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        monkeypatch.setattr(
            rt.cleanup_pipeline,
            "_process_calendar_cleanup",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                RuntimeError("calendar unavailable")),
        )
        rt.on_round_complete = lambda *_args: (_ for _ in ()).throw(
            RuntimeError("callback unavailable"))

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt, round_type="rhythm"),
            {"response": "done"},
        )

        assert outcome["status"] == "unsettled"
        assert any(
            reason.startswith("calendar_cleanup:RuntimeError:")
            for reason in outcome["fatal_reasons"]
        )
        assert {
            reason.split(":", 1)[0]
            for reason in outcome["degraded_reasons"]
        } == {"round_complete_callback"}

    def test_rhythm_calendar_cleanup_happy_path_clears_applied_flags(
            self, tmp_path, monkeypatch):
        import data.chronicle_store as chronicle_module

        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        calls = []

        class FakeChronicleStore:
            def cleanup_expired(self):
                calls.append(("chronicle_cleanup",))

        class FakeCorpusStore:
            def merge_layer(self, source, target):
                calls.append(("merge", source, target))

            def cleanup_expired(self):
                calls.append(("corpus_cleanup",))

        monkeypatch.setattr(
            chronicle_module, "ChronicleStore", FakeChronicleStore)
        monkeypatch.setattr(
            chronicle_module, "CorpusStore", FakeCorpusStore)
        monkeypatch.setattr(
            rt.cleanup_pipeline, "_prepare_ltm_degradation_for_day",
            lambda *_args: None,
        )
        monkeypatch.setattr(
            rt.cleanup_pipeline, "_build_ltm_degradation_context", lambda: "")
        monkeypatch.setattr(
            rt.cleanup_pipeline, "_cleanup_trash",
            lambda: calls.append(("trash_cleanup",)),
        )

        flag_layers = {
            "calendar_day_due": "daily",
            "calendar_week_due": "weekly",
            "calendar_month_due": "monthly",
            "calendar_quarter_due": "quarterly",
        }
        for flag in flag_layers:
            rt.sm.set_flag(flag, True)
        result = {
            "response": "done",
            "_chronicle_write_receipts": [
                {
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": layer,
                    "round_type": "rhythm",
                }
                for layer in flag_layers.values()
            ],
        }

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt, round_type="rhythm"), result)

        assert outcome["status"] == "settled"
        assert not any(rt.sm.get_flags()[flag] for flag in flag_layers)
        assert ("chronicle_cleanup",) in calls
        assert ("corpus_cleanup",) in calls
        assert [call for call in calls if call[0] == "merge"] == [
            ("merge", "rhythms", "daily"),
            ("merge", "daily", "weekly"),
            ("merge", "weekly", "monthly"),
            ("merge", "monthly", "quarterly"),
        ]

    @pytest.mark.parametrize("flag", ["calendar_day_due", "rhythm_due"])
    def test_rhythm_without_applied_main_axis_does_not_archive_raw_log(
            self, tmp_path, monkeypatch, flag):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        rt.sm.set_flag(flag, True)
        archive_calls = []
        monkeypatch.setattr(
            rt.ctx_store,
            "archive_raw_log",
            lambda: archive_calls.append(True),
        )
        monkeypatch.setattr(
            rt.cleanup_pipeline, "_process_calendar_cleanup",
            lambda *_args, **_kwargs: None,
        )

        rt.cleanup_pipeline.run(
            self._round_context(rt, round_type="rhythm"),
            {"response": "done"},
        )

        assert archive_calls == []

    def test_main_axis_applied_receipt_archives_raw_log(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        rt.sm.set_flag("rhythm_due", True)
        archive_calls = []
        monkeypatch.setattr(
            rt.ctx_store,
            "archive_raw_log",
            lambda: archive_calls.append(True) or "rhythm.jsonl",
        )
        monkeypatch.setattr(
            rt.cleanup_pipeline, "_process_calendar_cleanup",
            lambda *_args, **_kwargs: None,
        )

        rt.cleanup_pipeline.run(
            self._round_context(rt, round_type="rhythm"),
            {
                "response": "done",
                "_chronicle_write_receipts": [{
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": "rhythms",
                    "round_type": "rhythm",
                }],
            },
        )

        assert archive_calls == [True]

    def test_calendar_merge_requires_matching_applied_receipt(
            self, tmp_path, monkeypatch):
        import data.chronicle_store as chronicle_module

        rt = self._make_runtime(tmp_path)
        calls = []

        class FakeChronicleStore:
            def cleanup_expired(self):
                calls.append(("chronicle_cleanup",))

        class FakeCorpusStore:
            def merge_layer(self, source, target):
                calls.append(("merge", source, target))

            def cleanup_expired(self):
                calls.append(("corpus_cleanup",))

        monkeypatch.setattr(
            chronicle_module, "ChronicleStore", FakeChronicleStore)
        monkeypatch.setattr(
            chronicle_module, "CorpusStore", FakeCorpusStore)
        monkeypatch.setattr(
            rt.cleanup_pipeline,
            "_cleanup_trash",
            lambda: calls.append(("trash_cleanup",)),
        )
        state = rt.sm.load()
        state["base"]["heartbeat_flags"]["calendar_day_due"] = True

        rt.cleanup_pipeline._process_calendar_cleanup(
            {"response": ""},
            1,
            state,
            settlement_result={},
        )

        assert ("merge", "rhythms", "daily") not in calls
        assert ("trash_cleanup",) not in calls

    def test_completed_year_attic_failure_propagates(
            self, tmp_path, monkeypatch):
        import data.chronicle_store as chronicle_module

        rt = self._make_runtime(tmp_path)

        class FakeChronicleStore:
            def cleanup_expired(self):
                return []

        class FakeCorpusStore:
            def merge_layer(self, _source, _target):
                return None

            def cleanup_expired(self):
                return []

            def move_to_attic(self):
                raise OSError("attic unavailable")

        monkeypatch.setattr(
            chronicle_module, "ChronicleStore", FakeChronicleStore)
        monkeypatch.setattr(
            chronicle_module, "CorpusStore", FakeCorpusStore)
        state = rt.sm.load()
        state["base"]["heartbeat_flags"]["calendar_year_due"] = True

        with pytest.raises(OSError, match="attic unavailable"):
            rt.cleanup_pipeline._process_calendar_cleanup(
                {"response": ""},
                1,
                state,
                settlement_result={
                    "_chronicle_write_receipts": [{
                        "tool_id": "chronicle_write",
                        "status": "applied",
                        "layer": "yearly",
                        "round_type": "rhythm",
                    }],
                },
            )

    def test_cleanup_natural_noop_remains_settled(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt), {"response": "done"})

        assert outcome["status"] == "settled"

    def test_spec704_user_stop_skips_provider_and_preserves_pending_obligations(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        provider_calls = []
        monkeypatch.setattr(
            rt.executor,
            "call",
            lambda *_args, **_kwargs: provider_calls.append(True),
        )
        for flag in (
                "user_message_waiting", "calendar_day_due",
                "rhythm_due", "standby_due"):
            rt.sm.set_flag(flag, True)

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt),
            {"response": "", "_user_stop_requested": True},
        )
        flags = rt.sm.get_flags()

        assert outcome["status"] == "degraded"
        assert outcome["degraded_reasons"] == ["user_stopped"]
        assert provider_calls == []
        assert flags["user_message_waiting"] is False
        assert flags["calendar_day_due"] is True
        assert flags["rhythm_due"] is True
        assert flags["standby_due"] is True

    def test_spec704_user_stop_local_obligation_failure_is_unsettled(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        monkeypatch.setattr(
            rt.ctx_store,
            "save_round_to_cache",
            lambda *_args, **_kwargs: (_ for _ in ()).throw(
                OSError("cache unavailable")),
        )

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt),
            {"response": "", "_user_stop_requested": True},
        )

        assert outcome["status"] == "unsettled"
        assert "user_stopped" in outcome["degraded_reasons"]
        assert any(
            reason.startswith("round_cache_save:OSError:")
            for reason in outcome["fatal_reasons"]
        )

    def test_spec704_user_stop_only_clears_calendar_work_already_applied(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        rt.sm.set_flag("calendar_day_due", True)
        rt.sm.set_flag("calendar_week_due", True)

        outcome = rt.cleanup_pipeline.run(
            self._round_context(rt, round_type="rhythm"),
            {
                "response": "",
                "_user_stop_requested": True,
                "_chronicle_write_receipts": [{
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": "daily",
                    "round_type": "rhythm",
                }],
            },
        )
        flags = rt.sm.get_flags()

        assert outcome["status"] == "degraded"
        assert flags["calendar_day_due"] is False
        assert flags["calendar_week_due"] is True

    def test_cleanup_finalizes_flags(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()
        rt.sm.set_flag("user_message_waiting", True)
        rt._finalize_flags(state, "interactive", 1)
        flags = rt.sm.get_flags()
        assert flags["user_message_waiting"] is False

    def test_spec366_rhythm_cleanup_clears_consumed_interaction_after_final_reply(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("rhythm_due", True)
        rt.sm.set_flag("user_message_waiting", True)

        rt._finalize_flags(
            rt.sm.load(),
            "rhythm",
            366,
            result={
                "_chronicle_write_receipts": [{
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": "rhythms",
                    "round_type": "rhythm",
                }],
                "_reaction_finalize_validated": True,
                "_final_reply_done": True,
                "_user_input_text": "请读取并内化这本书。",
            },
        )

        flags = rt.sm.get_flags()
        assert flags["rhythm_due"] is False
        assert flags["user_message_waiting"] is False

    def test_spec366_rhythm_cleanup_keeps_unconsumed_interaction_without_final_reply(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("rhythm_due", True)
        rt.sm.set_flag("user_message_waiting", True)

        rt._finalize_flags(
            rt.sm.load(),
            "rhythm",
            367,
            result={
                "_chronicle_write_receipts": [{
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": "rhythms",
                    "round_type": "rhythm",
                }],
                "_reaction_finalize_validated": True,
                "_final_reply_done": False,
            },
        )

        flags = rt.sm.get_flags()
        assert flags["rhythm_due"] is False
        assert flags["user_message_waiting"] is True

    def test_spec388_calendar_cleanup_clears_due_flag_from_daily_receipt(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("calendar_day_due", True)

        rt._finalize_flags(
            rt.sm.load(),
            "rhythm",
            388,
            result={
                "_chronicle_write_receipts": [{
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": "daily",
                    "round_type": "rhythm",
                }],
            },
        )

        flags = rt.sm.get_flags()
        assert flags["calendar_day_due"] is False

    def test_finalize_flags_clears_standby_but_not_active_health_flags(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set_flag("token_usage_warning", True)

        rt._finalize_flags(
            rt.sm.load(),
            "rhythm",
            8,
            result={
                "_chronicle_write_receipts": [{
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": "rhythms",
                    "round_type": "rhythm",
                }],
            },
        )

        flags = rt.sm.get_flags()
        assert flags["api_degraded"] is True
        assert flags["process_down"] is False
        assert flags["token_usage_warning"] is True

        rt.sm.set_flag("standby_due", True)
        rt.sm.set_flag("shelve_timer_expired", True)

        rt._finalize_flags(rt.sm.load(), "standby", 9)

        flags = rt.sm.get_flags()
        assert flags["standby_due"] is False
        assert flags["shelve_timer_expired"] is False

    def test_relay_cleanup_without_new_relay_receipt_consumes_continue_requested(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("continue_requested", True)

        rt._finalize_flags(rt.sm.load(), "relay", 42)

        assert rt.sm.get_flags()["continue_requested"] is False

    def test_spec363_relay_cleanup_rearms_when_open_relay_intents_remain(
            self, tmp_path):
        from logic.relay_intent_pool import (
            create_relay_intent,
            settle_relay_intent,
        )

        rt = self._make_runtime(tmp_path)
        first = create_relay_intent(rt.sm, source_round=50, handoff_text="任务甲")
        create_relay_intent(rt.sm, source_round=51, handoff_text="任务乙")
        settle_relay_intent(
            rt.sm,
            {"relay_intent_id": first["relay_intent_id"], "status": "completed"},
            round_num=52,
        )
        rt.sm.set_flag("continue_requested", True)

        result = {}
        rt._finalize_flags(rt.sm.load(), "relay", 52, result=result)

        assert rt.sm.get_flags()["continue_requested"] is True
        assert result["_heartbeat_rearm_receipts"][0]["status"] == (
            "continue_requested_rearmed_from_open_relay_intents"
        )

    def test_spec490_blocked_relay_closeout_consumes_open_intent_without_rearm(
            self, tmp_path):
        from logic.relay_intent_pool import create_relay_intent, open_relay_intents

        rt = self._make_runtime(tmp_path)
        create_relay_intent(
            rt.sm,
            source_round=573,
            handoff_text="继续完成 12 项任务。",
            reaction_finalize_id="call_previous_continue",
        )
        rt.sm.set_flag("continue_requested", True)
        result = {
            "_settlement_ledgers": [{
                "tool_id": "reaction_finalize",
                "closeout_decision": "blocked",
                "call_id": "call_blocked_relay",
            }],
            "_closeout_relay_receipts": [],
        }

        rt._finalize_flags(rt.sm.load(), "relay", 574, result=result)

        assert rt.sm.get_flags()["continue_requested"] is False
        assert open_relay_intents(rt.sm.load()) == []
        assert any(
            receipt.get("status") == "relay_terminal_closeout_intents_settled"
            and receipt.get("final_status") == "blocked"
            for receipt in result.get("_relay_intent_terminal_receipts", [])
        )
        assert not any(
            receipt.get("status") == "continue_requested_rearmed_from_open_relay_intents"
            for receipt in result.get("_heartbeat_rearm_receipts", [])
        )

    def test_spec490_finish_relay_closeout_consumes_open_intent_without_rearm(
            self, tmp_path):
        from logic.relay_intent_pool import create_relay_intent, open_relay_intents

        rt = self._make_runtime(tmp_path)
        create_relay_intent(
            rt.sm,
            source_round=574,
            handoff_text="继续完成上一轮任务。",
            reaction_finalize_id="call_previous_continue",
        )
        rt.sm.set_flag("continue_requested", True)
        result = {
            "_settlement_ledgers": [{
                "tool_id": "reaction_finalize",
                "closeout_decision": "finish",
                "call_id": "call_finished_relay",
            }],
            "_closeout_relay_receipts": [],
        }

        rt._finalize_flags(rt.sm.load(), "relay", 575, result=result)

        assert rt.sm.get_flags()["continue_requested"] is False
        assert open_relay_intents(rt.sm.load()) == []
        assert any(
            receipt.get("status") == "relay_terminal_closeout_intents_settled"
            and receipt.get("final_status") == "completed"
            for receipt in result.get("_relay_intent_terminal_receipts", [])
        )
        assert not any(
            receipt.get("status") == "continue_requested_rearmed_from_open_relay_intents"
            for receipt in result.get("_heartbeat_rearm_receipts", [])
        )

    def test_spec359_interactive_cleanup_keeps_deferred_continue_requested_on_finish(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("continue_requested", True)

        rt._finalize_flags(
            rt.sm.load(),
            "interactive",
            273,
            result={"response": "读完并收束", "_closeout_relay_receipts": []},
        )

        assert rt.sm.get_flags()["continue_requested"] is True

    def test_spec273_interactive_cleanup_rearms_continue_requested_from_current_closeout(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        rt.sm.set_flag("continue_requested", True)
        result = {
            "response": "继续推进",
            "_closeout_relay_receipts": [{
                "tool_id": "reaction_finalize",
                "status": "continue_requested_set",
                "source": "closeout_form",
                "set_flags": ["continue_requested"],
            }],
        }

        rt._run_cleanup("interactive", rt.sm.load(), result, 274)

        assert rt.sm.get_flags()["continue_requested"] is True
        assert result["_heartbeat_rearm_receipts"] == [{
            "tool_id": "cleanup_pipeline",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": "continue_requested_rearmed",
            "source": "closeout_form",
            "round_type": "interactive",
            "consumed_flags": [],
            "set_flags": ["continue_requested"],
            "relay_intent": result["_heartbeat_rearm_receipts"][0]["relay_intent"],
        }]
        assert result["_heartbeat_rearm_receipts"][0]["relay_intent"]["status"] == "open"

    def test_relay_cleanup_rearms_continue_requested_from_relay_receipt(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        rt.sm.set_flag("continue_requested", True)
        result = {
            "response": "relay closeout",
            "_closeout_relay_receipts": [{
                "tool_id": "reaction_finalize",
                "status": "continue_requested_set",
                "source": "closeout_form",
                "set_flags": ["continue_requested"],
            }],
        }

        rt._run_cleanup("relay", rt.sm.load(), result, 43)

        assert rt.sm.get_flags()["continue_requested"] is True
        assert result["_heartbeat_rearm_receipts"] == [{
            "tool_id": "cleanup_pipeline",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": "continue_requested_rearmed",
            "source": "closeout_form",
            "round_type": "relay",
            "consumed_flags": ["continue_requested"],
            "set_flags": ["continue_requested"],
            "relay_intent": result["_heartbeat_rearm_receipts"][0]["relay_intent"],
        }]
        assert result["_heartbeat_rearm_receipts"][0]["relay_intent"]["status"] == "open"
        audit_path = tmp_path / "context" / "round" / "round_43.jsonl"
        audit_events = [
            json.loads(line)
            for line in audit_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        assert any(
            event.get("event_type") == "heartbeat_rearm"
            and event.get("payload", {}).get("status") == "continue_requested_rearmed"
            for event in audit_events
        )

    def test_relay_cleanup_does_not_rearm_from_invalid_relay_receipt(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        self._patch_minimal_cleanup(rt, monkeypatch)
        rt.sm.set_flag("continue_requested", True)
        result = {
            "response": "bad relay closeout",
            "_closeout_relay_receipts": [{
                "tool_id": "reaction_finalize",
                "status": "continue_requested_error",
                "source": "closeout_form",
                "set_flags": [],
            }],
        }

        rt._run_cleanup("relay", rt.sm.load(), result, 44)

        assert rt.sm.get_flags()["continue_requested"] is False
        assert result.get("_heartbeat_rearm_receipts", []) == []

    def test_spec289_cleanup_ignores_retired_reaction_handoff(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        captured = {}

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def capture_cleanup(state, round_type, result, internal_handoff=None):
            captured["internal_handoff"] = list(internal_handoff or [])
            return "cleanup system", [{"role": "user", "content": "cleanup"}]

        monkeypatch.setattr(rt.assembler, "assemble_cleanup", capture_cleanup)
        monkeypatch.setattr(rt, "_call_llm_with_round_audit", lambda *a, **kw: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "cleanup_finalize",
                {},
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        })

        rt._run_cleanup(
            "relay",
            rt.sm.load(),
            {
                "response": "done",
                "_reaction_internal_handoff": "整理本轮回执。",
            },
            258,
        )

        combined = "\n".join(
            item.get("content", "") for item in captured["internal_handoff"]
        )
        assert "整理本轮回执。" not in combined
        assert "反应步内部交接" not in combined

    def test_spec288_cleanup_round_material_is_written_before_cleanup_and_cleared(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        events = []
        captured = {}

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def append_material(round_num, content, **kwargs):
            events.append("write_material")
            captured["material_round"] = round_num
            captured["material_content"] = content
            captured["material_kwargs"] = dict(kwargs)

        def clear_material(**kwargs):
            events.append("clear_material")
            captured["clear_kwargs"] = dict(kwargs)
            return {"now_removed": 1, "lately_removed": 0}

        def capture_cleanup(state, round_type, result, internal_handoff=None):
            events.append("assemble_cleanup")
            captured["internal_handoff"] = list(internal_handoff or [])
            return "cleanup system", [{"role": "user", "content": "cleanup"}]

        monkeypatch.setattr(rt.ctx_store, "append_cleanup_round_material", append_material)
        monkeypatch.setattr(rt.ctx_store, "clear_transient_entries", clear_material)
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", capture_cleanup)
        monkeypatch.setattr(rt, "_call_llm_with_round_audit", lambda *a, **kw: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "cleanup_finalize",
                {},
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        })

        rt._run_cleanup(
            "interactive",
            rt.sm.load(),
            {
                "response": "反应步最终回复",
                "_memory_write_receipts": [{
                    "status": "applied",
                    "mem_id": "MEM-288",
                    "title": "临时材料测试",
                    "keywords": ["cleanup"],
                }],
            },
            288,
            user_input_text="本轮用户输入",
        )

        assert events[:3] == ["write_material", "assemble_cleanup", "clear_material"]
        assert captured["material_round"] == 288
        assert "本轮用户输入" in captured["material_content"]
        assert "反应步最终回复" in captured["material_content"]
        assert "memory_write 同步回执" in captured["material_content"]
        assert captured["clear_kwargs"] == {
            "round_num": 288,
            "transient_scope": "cleanup_round",
            "transient_target_step": "cleanup",
        }
        assert all(
            "本轮用户输入" not in item.get("content", "")
            for item in captured["internal_handoff"]
        )

    def test_spec410_cleanup_internal_tasks_use_cleanup_round_material(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        events = []
        captured = {"materials": []}
        state = rt.sm.load()
        state["base"]["heartbeat_flags"]["calendar_day_due"] = True
        rt.sm.save(state)

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "STM遗忘任务正文")
        monkeypatch.setattr(rt, "_build_ltm_degradation_context", lambda: "LTM降格任务正文")
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def append_material(round_num, content, **kwargs):
            events.append("write_material")
            captured["materials"].append((round_num, content, dict(kwargs)))

        def clear_material(**kwargs):
            events.append("clear_material")
            captured["clear_kwargs"] = dict(kwargs)
            return {"now_removed": len(captured["materials"]), "lately_removed": 0}

        def capture_cleanup(state, round_type, result, internal_handoff=None):
            events.append("assemble_cleanup")
            captured["internal_handoff"] = list(internal_handoff or [])
            return "cleanup system", [{"role": "user", "content": "cleanup"}]

        monkeypatch.setattr(rt.ctx_store, "append_cleanup_round_material", append_material)
        monkeypatch.setattr(rt.ctx_store, "clear_transient_entries", clear_material)
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", capture_cleanup)
        monkeypatch.setattr(rt, "_call_llm_with_round_audit", lambda *a, **kw: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "cleanup_finalize",
                {},
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        })

        rt._run_cleanup(
            "rhythm",
            state,
            {"response": "反应步最终回复"},
            410,
            user_input_text="本轮用户输入",
        )

        assert events[:4] == [
            "write_material",
            "write_material",
            "write_material",
            "assemble_cleanup",
        ]
        assert captured["internal_handoff"] == []
        contents = [item[1] for item in captured["materials"]]
        sources = [item[2].get("interaction_source") for item in captured["materials"]]
        assert any("本轮用户输入" in content for content in contents)
        assert any("STM遗忘任务正文" in content for content in contents)
        assert any("LTM降格任务正文" in content for content in contents)
        assert "cleanup_round_material" in sources
        assert "cleanup_forgetting_task" in sources
        assert "cleanup_ltm_degradation_task" in sources
        assert captured["clear_kwargs"] == {
            "round_num": 410,
            "transient_scope": "cleanup_round",
            "transient_target_step": "cleanup",
        }

    def test_interactive_cleanup_can_rearm_from_relay_receipt_if_needed(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        result = {
            "_closeout_relay_receipts": [{
                "tool_id": "reaction_finalize",
                "status": "continue_requested_set",
                "source": "closeout_form",
                "set_flags": ["continue_requested"],
            }],
        }

        rt._finalize_flags(rt.sm.load(), "interactive", 45, result=result)

        assert rt.sm.get_flags()["continue_requested"] is True

    def test_cleanup_always_runs_heat_decay(self, tmp_path, monkeypatch):
        """善后步必定执行热度衰减"""
        rt = self._make_runtime(tmp_path)
        # mock API 调用
        monkeypatch.setattr(rt.executor, "call", lambda step, sys, msgs: {"response": "ok"})
        # 阻止写真实 persona/ 文件
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", lambda s, rt_type, r: ("", []))
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        rt._run_cleanup("interactive", rt.sm.load(), {"response": "test"}, 1)
        assert rt.heat.decayed is True
        assert rt.heat.last_decay_round_num == 1

    def test_cleanup_processes_stored_degrade_in_temp_persona(self, tmp_path, monkeypatch):
        """善后步集成链路：AH_low 到线的已入库 STM 副本会闭合删除"""
        import json
        from data import memory_heat as mh
        from data import memory_store as ms
        from data import memory_index as mi

        rt = self._make_runtime(tmp_path)
        memory_md = tmp_path / "memory.md"
        meta_json = tmp_path / "meta.json"
        index_md = tmp_path / "index.md"
        keywords_json = tmp_path / "keywords.json"
        heat_json = tmp_path / "heat.json"
        mem_id = "MEM-FEEDC0DE"

        memory_md.write_text(
            "<!-- STM 记忆条目正文 -->\n\n"
            f"## {mem_id}\n待删除正文\n",
            encoding="utf-8",
        )
        meta_json.write_text(json.dumps({
            mem_id: {"id": mem_id, "title": "删"},
        }, ensure_ascii=False), encoding="utf-8")
        index_md.write_text(
            "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 入库轮 |\n"
            "|------|------|------|------|---------|--------|\n"
            f"| {mem_id} | [F] | 5 | 删 | — | 00001 |\n",
            encoding="utf-8",
        )
        keywords_json.write_text(json.dumps({
            "_comment": "倒排索引（关键词→条目ID）",
            "index": {"测试": [mem_id]},
        }, ensure_ascii=False), encoding="utf-8")
        heat_json.write_text(json.dumps({
            "entries": {
                mem_id: {
                    "H": 1, "zone": "衰减", "AH_high": 0, "AH_low": 2,
                    "degrade": False, "stored": True, "compression": True,
                    "heat_locked": False,
                },
            }
        }, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(ms, "MEMORY_MD", str(memory_md))
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        monkeypatch.setattr(ms, "INDEX_MD", str(index_md))
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_json))

        rt.heat = mh.MemoryHeat()
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", lambda *a, **kw: ("", []))
        monkeypatch.setattr(rt.executor, "call", lambda *a, **kw: {"response": ""})
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)

        rt._run_cleanup("interactive", rt.sm.load(), {"response": "test"}, 1)

        assert mem_id not in memory_md.read_text(encoding="utf-8")
        assert mem_id not in json.loads(meta_json.read_text(encoding="utf-8"))
        assert mem_id not in index_md.read_text(encoding="utf-8")
        assert mem_id not in json.loads(keywords_json.read_text(encoding="utf-8"))["index"].get("测试", [])
        assert mem_id not in json.loads(heat_json.read_text(encoding="utf-8"))["entries"]

    def test_cleanup_api_failure_runs_l3_emergency_save(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)

        class FailingExecutor:
            def call(self, step, system, messages):
                raise RuntimeError("cleanup timeout")

        class CapturingContext:
            def __init__(self):
                self.saved = []
                self.cache_entries = []

            def save_round_to_cache(self, round_num, user_input="", response="", **kwargs):
                self.saved.append((round_num, user_input, response, kwargs))

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.cache_entries.append((round_num, role, text, kind, kwargs))


        class CapturingAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        class CapturingConnectivity:
            def __init__(self):
                self.entries = []

            def log_latency(self, endpoint, status, message=""):
                self.entries.append((endpoint, status, message))

        rt.executor = FailingExecutor()
        rt.ctx_store = CapturingContext()
        rt.alert_store = CapturingAlerts()
        rt.connectivity_store = CapturingConnectivity()

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", lambda s, rt_type, r, **kw: ("sys", []))
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)

        rt._run_cleanup(
            "interactive",
            rt.sm.load(),
            {"response": "reaction leftovers"},
            7,
            "user input",
        )

        assert "善后步API异常 R7" in rt.sm.get("base.meta.last_error")
        assert rt.ctx_store.saved == [(7, "user input", "reaction leftovers", {})]
        assert any(
            row[3] == "fault_note" and "[L3心跳急救]" in row[2] and "reaction leftovers" in row[2]
            for row in rt.ctx_store.cache_entries
        )
        assert rt.alert_store.entries == [{
            "round_num": 7,
            "step": "cleanup",
            "event_type": "l3_cleanup_api_failure",
            "detail": "cleanup timeout",
            "action": "script_emergency_save",
        }]
        assert rt.connectivity_store.entries == [
            ("cleanup", "error", "L3 cleanup API failure: cleanup timeout")
        ]

    def test_cleanup_persists_interaction_metadata(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)

        class CapturingContext:
            def __init__(self):
                self.saved = []

            def save_round_to_cache(self, round_num, user_input="", response="", **kwargs):
                self.saved.append((round_num, user_input, response, kwargs))

        rt.ctx_store = CapturingContext()
        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", lambda s, rt_type, r: ("", []))
        monkeypatch.setattr(rt.executor, "call", lambda step, sys, msgs: {"response": ""})
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)

        rt._run_cleanup(
            "interactive",
            rt.sm.load(),
            {
                "response": "FMZ 回复",
                "_interaction_meta": {
                    "interaction_object": "Codex",
                    "identity_status": "declared",
                    "interaction_source": "self_declaration",
                },
            },
            9,
            "我是 Codex，做验证",
        )

        assert rt.ctx_store.saved[0][3]["interaction_object"] == "Codex"
        assert rt.ctx_store.saved[0][3]["identity_status"] == "declared"

    def test_cleanup_output_table_does_not_override_reaction_response(self, tmp_path, monkeypatch):
        from logic import cleanup_processor as cp

        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        monkeypatch.setattr(rt.assembler.popup, "write_popup", lambda *a, **kw: None)

        result = {"response": "反应步最终回复"}

        rt._process_cleanup_output(
            {
                "response": """### 5. 成品输出
| 有回复 | 内容 |
|--------|------|
| 是 | cleanup 不应覆盖 |
"""
            },
            31,
            rt.sm.load(),
            result,
        )

        assert result["response"] == "反应步最终回复"

    def test_cleanup_retries_once_when_native_finalize_missing(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        calls = []
        parsed_events = []
        settlements = []
        processed = []

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(
            rt.assembler,
            "assemble_cleanup",
            lambda *a, **kw: ("cleanup system", [{"role": "user", "content": "cleanup"}]),
        )
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(
            rt,
            "_round_audit_parsed",
            lambda round_num, phase, iteration, parsed:
            parsed_events.append((phase, iteration, parsed)),
        )
        monkeypatch.setattr(
            rt,
            "_round_audit_settlement",
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement)),
        )
        monkeypatch.setattr(
            rt,
            "_process_cleanup_output",
            lambda cleanup_result, round_num, state, result, iteration=1:
            processed.append((iteration, cleanup_result)),
        )
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append((iteration, list(messages)))
            if iteration == 1:
                return {
                    "response": "我来整理训练材料。",
                    "tool_call_envelopes": [],
                }
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "cleanup_finalize",
                    "arguments": {},
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        monkeypatch.setattr(rt, "_call_llm_with_round_audit", fake_call)

        rt._run_cleanup("interactive", rt.sm.load(), {"response": "done"}, 301)

        assert [iteration for iteration, _messages in calls] == [1, 2]
        retry_content = calls[1][1][-1]["content"]
        retry_text = "\n".join(
            str(message.get("content") or "")
            for message in calls[1][1]
            if isinstance(message, dict)
        )
        assert "cleanup_finalize" in retry_content
        assert "只进 audit" in retry_content
        assert "不作为事实" in retry_content
        assert "我来整理训练材料" not in retry_text
        assert settlements[0][2]["retry_requested"] == "cleanup_finalize_missing_or_invalid"
        assert processed == [(2, {
            "response": "",
            "tool_call_envelopes": [{
                "tool_id": "cleanup_finalize",
                "arguments": {},
                "parse_status": "ok",
                "index": 0,
            }],
        })]
        assert any(
            phase == "cleanup"
            and iteration == 1
            and parsed.get("_terminal_finalize_missing") is True
            for phase, iteration, parsed in parsed_events
        )

    def test_spec515_cleanup_retries_completion_claim_when_task_blocked(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        calls = []
        settlements = []
        processed = []

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(
            rt.assembler,
            "assemble_cleanup",
            lambda *a, **kw: ("cleanup system", [{"role": "user", "content": "cleanup"}]),
        )
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(rt, "_round_audit_parsed", lambda *a, **kw: None)
        monkeypatch.setattr(
            rt,
            "_round_audit_settlement",
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement)),
        )
        monkeypatch.setattr(
            rt,
            "_process_cleanup_output",
            lambda cleanup_result, round_num, state, result, iteration=1:
            processed.append((iteration, cleanup_result)),
        )
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append((iteration, list(messages)))
            if iteration == 1:
                return {
                    "response": "",
                    "tool_call_envelopes": [{
                        "tool_id": "cleanup_finalize",
                        "arguments": {
                            "tacit_associations": [{
                                "item_id": "task",
                                "item_type": "task_summary",
                                "action": "kept",
                                "note": "12 项任务已经全部完成并交付。",
                                "evidence_refs": [],
                            }],
                        },
                        "parse_status": "ok",
                        "index": 0,
                    }],
                }
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "cleanup_finalize",
                    "arguments": {
                        "tacit_associations": [{
                            "item_id": "task",
                            "item_type": "task_summary",
                            "action": "kept",
                            "note": "完成 11 项，任务 02 blocked，现场已归档。",
                            "evidence_refs": [],
                        }],
                    },
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        monkeypatch.setattr(rt, "_call_llm_with_round_audit", fake_call)

        rt._run_cleanup(
            "interactive",
            rt.sm.load(),
            {
                "response": "blocked",
                "_settlement_ledgers": [{
                    "closeout_decision": "blocked",
                    "blocked_reason": "task_acceptance_blocked",
                    "blockers": ["item_02"],
                }],
                "_reaction_loop_guard_receipts": [{
                    "status": "task_acceptance_auto_blocked",
                    "reason": "task_acceptance_blocked",
                }],
            },
            515,
        )

        assert [iteration for iteration, _messages in calls] == [1, 2]
        assert settlements[0][2]["retry_requested"] == "cleanup_finalize_missing_or_invalid"
        assert settlements[0][2]["violation_reason"] == "cleanup_internalization_truth_violation"
        retry_text = "\n".join(
            str(message.get("content") or "")
            for message in calls[1][1]
            if isinstance(message, dict)
        )
        assert "未闭合任务" in retry_text
        assert processed == [(2, {
            "response": "",
            "tool_call_envelopes": [{
                "tool_id": "cleanup_finalize",
                "arguments": {
                    "tacit_associations": [{
                        "item_id": "task",
                        "item_type": "task_summary",
                        "action": "kept",
                        "note": "完成 11 项，任务 02 blocked，现场已归档。",
                        "evidence_refs": [],
                    }],
                },
                "parse_status": "ok",
                "index": 0,
            }],
        })]

    def test_spec522_cleanup_truth_guard_reads_open_workbench_task(
            self, tmp_path, monkeypatch):
        from data import workbench as workbench_module
        from data.workbench import WorkbenchStore
        from engines.cleanup_pipeline import CleanupPipeline

        wb_root = tmp_path / "workbench"
        monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
        store = WorkbenchStore(root_dir=str(wb_root))
        store.create_task_guide_task(
            "未闭合任务",
            "仍有验收未完成。",
            guide={
                "items": [{
                    "item_id": "item_01",
                    "status": "open",
                    "required": True,
                }],
                "acceptance": [{
                    "acceptance_id": "acc_01",
                    "status": "pending",
                    "required": True,
                }],
            },
        )

        violation = CleanupPipeline._cleanup_internalization_truth_violation(
            {
                "tacit_associations": [{
                    "item_id": "task",
                    "item_type": "task_summary",
                    "action": "kept",
                    "note": "12 项任务已经全部完成并交付。",
                }],
            },
            state={},
            result={},
        )

        assert violation["reason"] == "unclosed_task_completion_claim"
        assert violation["field"] == "tacit_associations[0].note"

    def test_spec538_cleanup_truth_guard_allows_completed_output_task(
            self, tmp_path, monkeypatch):
        from data import workbench as workbench_module
        from data.workbench import WorkbenchStore
        from engines.cleanup_pipeline import CleanupPipeline

        wb_root = tmp_path / "workbench"
        monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
        store = WorkbenchStore(root_dir=str(wb_root))
        task_id = store.create_task_guide_task(
            "已完成任务",
            "全部验收已通过。",
            guide={
                "items": [{
                    "item_id": "item_01",
                    "status": "done",
                    "required": True,
                    "evidence_refs": ["file:output/01.md"],
                }],
                "acceptance": [{
                    "acceptance_id": "acc_01",
                    "status": "passed",
                    "required": True,
                    "evidence_refs": ["file:output/01.md"],
                }],
            },
        )
        store.complete_task_guide_task(task_id, result="Task complete.")

        violation = CleanupPipeline._cleanup_internalization_truth_violation(
            {
                "tacit_associations": [{
                    "item_id": "task",
                    "item_type": "task_summary",
                    "action": "kept",
                    "note": "12 项任务已经全部完成并通过验收。",
                }],
            },
            state={},
            result={
                "_reaction_loop_guard_receipts": [{
                    "status": "blocked",
                    "reason": "task_acceptance_blocked",
                }],
            },
        )

        assert violation is None

    def test_cleanup_second_missing_finalize_is_not_processed_as_text(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        calls = []
        processed = []

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(
            rt.assembler,
            "assemble_cleanup",
            lambda *a, **kw: ("cleanup system", [{"role": "user", "content": "cleanup"}]),
        )
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(rt, "_round_audit_parsed", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_round_audit_settlement", lambda *a, **kw: None)
        monkeypatch.setattr(
            rt,
            "_process_cleanup_output",
            lambda cleanup_result, round_num, state, result, iteration=1:
            processed.append((iteration, cleanup_result)),
        )
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append(iteration)
            return {"response": "仍然裸文本", "tool_call_envelopes": []}

        monkeypatch.setattr(rt, "_call_llm_with_round_audit", fake_call)

        rt._run_cleanup("interactive", rt.sm.load(), {"response": "done"}, 302)

        assert calls == [1, 2, 3]
        assert processed == [(3, {
            "response": "",
            "tool_call_envelopes": [],
            "_terminal_finalize_missing": True,
            "_terminal_finalize_issue": "cleanup_finalize_missing_after_retry",
            "_terminal_invalids": [],
        })]

    def test_cleanup_retries_when_native_envelope_key_is_missing(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        calls = []
        processed = []
        settlements = []

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(
            rt.assembler,
            "assemble_cleanup",
            lambda *a, **kw: ("cleanup system", [{"role": "user", "content": "cleanup"}]),
        )
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(rt, "_round_audit_parsed", lambda *a, **kw: None)
        monkeypatch.setattr(
            rt,
            "_round_audit_settlement",
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement)),
        )
        monkeypatch.setattr(
            rt,
            "_process_cleanup_output",
            lambda cleanup_result, round_num, state, result, iteration=1:
            processed.append((iteration, cleanup_result)),
        )
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append(iteration)
            if iteration == 1:
                return {"response": "connection_bridge: 旧文本不应生效"}
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "cleanup_finalize",
                    "arguments": {},
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        monkeypatch.setattr(rt, "_call_llm_with_round_audit", fake_call)

        outcome = rt._run_cleanup(
            "interactive", rt.sm.load(), {"response": "done"}, 318)

        assert calls == [1, 2]
        assert settlements[0][2]["retry_requested"] == "cleanup_finalize_missing_or_invalid"
        assert settlements[0][2]["frame_ref"]["frame_id"] == (
            "R000318:cleanup:1")
        assert outcome["frame_ref"]["frame_id"] == "R000318:cleanup:2"
        assert outcome["frame_ref"]["caused_by"] == "R000318:cleanup:1"
        assert processed == [(2, {
            "response": "",
            "tool_call_envelopes": [{
                "tool_id": "cleanup_finalize",
                "arguments": {},
                "parse_status": "ok",
                "index": 0,
            }],
        })]

    def test_spec274_cleanup_allows_second_retry_to_recover(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        calls = []
        processed = []

        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt, "_build_forgetting_context", lambda: "")
        monkeypatch.setattr(
            rt.assembler,
            "assemble_cleanup",
            lambda *a, **kw: ("cleanup system", [{"role": "user", "content": "cleanup"}]),
        )
        monkeypatch.setattr(rt, "_update_token_usage", lambda result: None)
        monkeypatch.setattr(rt, "_round_audit_parsed", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_round_audit_settlement", lambda *a, **kw: None)
        monkeypatch.setattr(
            rt,
            "_process_cleanup_output",
            lambda cleanup_result, round_num, state, result, iteration=1:
            processed.append((iteration, cleanup_result)),
        )
        monkeypatch.setattr(rt, "_process_forgetting_result", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_memory_lifecycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_evolution_set", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.hb, "resume", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append(iteration)
            if iteration < 3:
                return {"response": "裸文本", "tool_call_envelopes": []}
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "cleanup_finalize",
                    "arguments": {},
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        monkeypatch.setattr(rt, "_call_llm_with_round_audit", fake_call)

        rt._run_cleanup("interactive", rt.sm.load(), {"response": "done"}, 303)

        assert calls == [1, 2, 3]
        assert processed == [(3, {
            "response": "",
            "tool_call_envelopes": [{
                "tool_id": "cleanup_finalize",
                "arguments": {},
                "parse_status": "ok",
                "index": 0,
            }],
        })]
