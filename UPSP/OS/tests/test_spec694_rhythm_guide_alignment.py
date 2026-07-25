import json
import os
import sys


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, logical_step


class TestSpec694RhythmGuideAlignment(RuntimeTestMixin):
    def _stub_round(self, rt, monkeypatch, captured):
        from engines.round_context import SetupResult

        def fake_setup(context):
            captured["setup"] = {
                "round": context.round_num,
                "round_type": context.round_type,
                "flags": dict(context.flags),
            }
            return SetupResult(
                raw_result={"response": ""},
                intent={
                    "security_verdict": "pass",
                    "mount_requests": [],
                    "task_guidance_required": False,
                },
                interaction_meta=self._confirmed_meta(),
                user_input_text="测试输入",
                setup_messages=[],
                internal_handoff=[],
            )

        def fake_reaction(*_args, **_kwargs):
            captured["active_guide"] = rt.workbench.load_active_guide()
            return {
                "response": "完成",
                "_reaction_finalize_validated": True,
                "_final_reply_done": True,
                "_interaction_meta": self._confirmed_meta(),
            }

        monkeypatch.setattr(rt.setup_runner, "run", fake_setup)
        monkeypatch.setattr(rt, "_run_reaction_loop", fake_reaction)
        monkeypatch.setattr(rt, "_run_cleanup", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

    def test_pre_setup_recovery_changes_api_plus_user_to_interactive(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set_flag("user_message_waiting", True)
        monkeypatch.setattr(rt.hb, "dequeue_messages", lambda: ["测试输入"])
        events = []
        clear_flags = rt.sm.clear_flags

        def recording_clear(names):
            events.append(("clear", rt.sm.get_total_round(), list(names)))
            return clear_flags(names)

        monkeypatch.setattr(rt.sm, "clear_flags", recording_clear)
        captured = {}
        self._stub_round(rt, monkeypatch, captured)

        rt._run_one_round("rhythm", rt.sm.load(), rt.sm.get_flags())

        assert events == [("clear", 0, ["api_degraded"])]
        assert captured["setup"]["round"] == 1
        assert captured["setup"]["round_type"] == "interactive"
        assert captured["setup"]["flags"]["api_degraded"] is False
        assert captured["setup"]["flags"]["user_message_waiting"] is True
        round_path = tmp_path / "context" / "round" / "round_1.jsonl"
        started = json.loads(round_path.read_text(encoding="utf-8").splitlines()[0])
        snapshot = started["payload"]["input_snapshot"]
        assert snapshot["trigger"]["round_type"] == "rhythm"
        assert snapshot["trigger"]["flags"]["api_degraded"] is True
        assert snapshot["flags"]["api_degraded"] is False
        assert snapshot["pre_setup_alert_recovery"] == {
            "cleared_flags": ["api_degraded"],
            "effective_round_type": "interactive",
        }

    def test_pre_setup_recovery_keeps_calendar_round_without_api_guide(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        for flag in ("api_degraded", "calendar_day_due", "calendar_week_due"):
            rt.sm.set_flag(flag, True)
        captured = {}
        self._stub_round(rt, monkeypatch, captured)

        rt._run_one_round("rhythm", rt.sm.load(), rt.sm.get_flags())

        assert captured["setup"]["round_type"] == "rhythm"
        assert captured["setup"]["flags"]["api_degraded"] is False
        assert captured["active_guide"]["kind"] == "calendar_rhythm_guide"
        assert captured["active_guide"]["items"][0]["item_id"] == "calendar_day_due"

    def test_pre_setup_recovery_fails_closed_when_state_write_fails(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set_flag("calendar_day_due", True)
        monkeypatch.setattr(
            rt.sm,
            "clear_flags",
            lambda _names: (_ for _ in ()).throw(OSError("state write failed")),
        )
        captured = {}
        self._stub_round(rt, monkeypatch, captured)

        rt._run_one_round("rhythm", rt.sm.load(), rt.sm.get_flags())

        assert captured["setup"]["round_type"] == "rhythm"
        assert captured["setup"]["flags"]["api_degraded"] is True
        assert captured["active_guide"]["kind"] == "emergency_handling_guide"
        assert rt.sm.get("base.heartbeat_flags.api_degraded") is True

    def test_pre_setup_recovery_clears_api_and_process_flags_atomically(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        for flag in ("api_degraded", "process_down", "user_message_waiting"):
            rt.sm.set_flag(flag, True)
        monkeypatch.setattr(rt.hb, "_check_process_down", lambda: False)
        cleared = []
        clear_flags = rt.sm.clear_flags

        def recording_clear(names):
            cleared.append(list(names))
            return clear_flags(names)

        monkeypatch.setattr(rt.sm, "clear_flags", recording_clear)
        captured = {}
        self._stub_round(rt, monkeypatch, captured)

        rt._run_one_round("rhythm", rt.sm.load(), rt.sm.get_flags())

        assert cleared == [["api_degraded", "process_down"]]
        assert captured["setup"]["round_type"] == "interactive"
        assert captured["setup"]["flags"]["api_degraded"] is False
        assert captured["setup"]["flags"]["process_down"] is False

    def test_api_flag_is_not_cleared_without_explicit_healthy_endpoint_evidence(
            self, tmp_path):
        from logic.rhythm_guide_materializer import (
            reconcile_recovered_emergency_flags,
        )

        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("api_degraded", True)

        effective, cleared = reconcile_recovered_emergency_flags(
            rt.sm.get_flags(),
            state_store=rt.sm,
            connectivity_store=rt.connectivity_store,
        )

        assert cleared == []
        assert effective["api_degraded"] is True
        assert rt.sm.get("base.heartbeat_flags.api_degraded") is True

    def test_pre_setup_recovery_skips_empty_round_and_resumes_heartbeat(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        rt.sm.set_flag("api_degraded", True)
        calls = []
        monkeypatch.setattr(
            rt.setup_runner,
            "run",
            lambda _context: (_ for _ in ()).throw(
                AssertionError("empty round must not run setup")
            ),
        )
        monkeypatch.setattr(rt.hb, "pause", lambda: calls.append("pause"))
        monkeypatch.setattr(rt.hb, "resume", lambda: calls.append("resume"))

        result = rt._run_one_round("rhythm", rt.sm.load(), rt.sm.get_flags())

        assert result["_round_skipped"] == "no_effective_trigger"
        assert result["_pre_setup_cleared_flags"] == ["api_degraded"]
        assert rt.sm.get_total_round() == 0
        assert calls == ["pause", "resume"]

    def test_materializer_supersedes_stale_calendar_then_restores_it_after_recovery(
            self, tmp_path):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        stale_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=576,
        )
        rt.connectivity_store.log_latency("primary", "timeout", "incident")
        for flag in ("api_degraded", "calendar_day_due", "calendar_week_due"):
            rt.sm.set_flag(flag, True)

        emergency_id = materialize_current_rhythm_guide(
            rt.workbench,
            rt.sm.get_flags(),
            round_num=577,
            state_store=rt.sm,
            connectivity_store=rt.connectivity_store,
        )

        assert rt.workbench.load_guide(stale_id)["status"] == "superseded"
        assert rt.workbench.load_guide(emergency_id)["kind"] == "emergency_handling_guide"
        ledger_path = os.path.join(
            rt.workbench._guide_dir(stale_id),
            "ledger.jsonl",
        )
        ledger = [json.loads(line) for line in open(ledger_path, encoding="utf-8")]
        assert ledger[-1]["event"] == "runtime_rhythm_guide_superseded"
        assert ledger[-1]["next_kind"] == "emergency_handling_guide"

        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        current_state = rt.sm.load()
        day_id = rt.reaction_loop_runner._materialize_next_runtime_rhythm_guide_if_needed(
            current_state,
            "rhythm",
            577,
            set(),
        )

        assert rt.sm.get("base.heartbeat_flags.api_degraded") is False
        assert current_state["base"]["heartbeat_flags"]["api_degraded"] is False
        assert rt.workbench.load_guide(emergency_id)["status"] == "superseded"
        assert rt.workbench.load_guide(day_id)["items"][0]["item_id"] == "calendar_day_due"
        focus = rt.reaction_loop_runner._sync_chronicle_focus_for_current_guide(
            round_type="rhythm",
            current_state=current_state,
            round_num=577,
            completed_flags=set(),
        )
        assert focus["layer"] == "daily"
        assert focus["calendar_flag"] == "calendar_day_due"

        week_id = rt.reaction_loop_runner._materialize_next_runtime_rhythm_guide_if_needed(
            current_state,
            "rhythm",
            577,
            {"calendar_day_due"},
        )
        assert rt.workbench.load_guide(day_id)["status"] == "superseded"
        assert rt.workbench.load_guide(week_id)["items"][0]["item_id"] == "calendar_week_due"

    def test_materializer_reuses_exact_active_kind_and_pending_items(self, tmp_path):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        first_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=576,
        )

        reused_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=577,
        )

        assert reused_id == first_id
        assert rt.workbench.load_guide(first_id)["status"] == "open"
        ledger_path = os.path.join(rt.workbench._guide_dir(first_id), "ledger.jsonl")
        events = [
            json.loads(line)["event"]
            for line in open(ledger_path, encoding="utf-8")
        ]
        assert events == ["runtime_rhythm_guide_materialized"]

    def test_reaction_frame_recovery_retires_emergency_without_changing_round_type(
            self, tmp_path):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set_flag("user_message_waiting", True)
        emergency_id = materialize_current_rhythm_guide(
            rt.workbench,
            rt.sm.get_flags(),
            round_num=1,
        )
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        current_state = rt.sm.load()

        replacement = rt.reaction_loop_runner._materialize_next_runtime_rhythm_guide_if_needed(
            current_state,
            "interactive",
            1,
            set(),
        )

        assert replacement is None
        assert current_state["base"]["heartbeat_flags"]["api_degraded"] is False
        assert current_state["base"]["heartbeat_flags"]["user_message_waiting"] is True
        assert rt.workbench.load_guide(emergency_id)["status"] == "superseded"
        assert rt.workbench.get("base.active_guides.rhythm") is None

    def test_round577_incident_path_has_one_emergency_then_one_day_and_week_write(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *_a, **_kw: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *_a, **_kw: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *_a, **_kw: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(
            rt.heat,
            "load_heat",
            lambda: {"entries": dict(rt.heat.entries)},
            raising=False,
        )
        monkeypatch.setattr(
            rt.heat,
            "save_heat",
            lambda data: setattr(rt.heat, "entries", dict(data.get("entries") or {})),
            raising=False,
        )
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)
        monkeypatch.setattr(rt.hb, "dequeue_messages", lambda: ["继续当前真实交互。"])

        rt.sm.set("base.meta.total_round", 576)
        stale_day_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=576,
        )
        rt.connectivity_store.log_latency("primary", "timeout", "primary timeout")
        rt.connectivity_store.log_latency("fallback", "ok", "fallback setup path")
        for flag in (
                "api_degraded",
                "calendar_day_due",
                "calendar_week_due",
                "user_message_waiting"):
            rt.sm.set_flag(flag, True)

        helper = self

        class IncidentExecutor:
            def __init__(self):
                self.reaction_calls = 0
                self.setup_messages = ""
                self.reaction_guides = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = logical_step(step, active_protocol_tool_guides)
                combined = "\n".join(str(item.get("content") or "") for item in messages)
                if logical == "setup":
                    self.setup_messages = combined
                    rt.connectivity_store.log_latency(
                        "fallback", "ok", "setup fallback succeeded"
                    )
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "setup_finalize",
                            {
                                "security_verdict": "pass",
                                "mount_requests": [],
                                "task_guidance_required": False,
                                "task_guidance_route": "none",
                                "task_guidance_reason": "",
                            },
                            tool_family="substrate_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if logical == "cleanup":
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "cleanup_finalize",
                            {},
                            tool_family="substrate_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if logical == "final_reply":
                    return {
                        "response": "节律写入各一次，继续回应当前交互。",
                        "tool_call_envelopes": [],
                    }

                self.reaction_calls += 1
                guide = rt.workbench.load_active_guide()
                kind = str((guide or {}).get("kind") or "")
                item_ids = [
                    item.get("item_id") for item in (guide or {}).get("items") or []
                ]
                self.reaction_guides.append((kind, item_ids, combined))
                if self.reaction_calls == 1:
                    assert kind == "emergency_handling_guide"
                    rt.connectivity_store.log_latency(
                        "primary", "ok", "first reaction recovered primary"
                    )
                    return {
                        "response": "已取得新的 API 恢复证据。",
                        "tool_call_envelopes": [],
                    }
                if self.reaction_calls in {2, 3}:
                    expected = (
                        "calendar_day_due"
                        if self.reaction_calls == 2
                        else "calendar_week_due"
                    )
                    assert kind == "calendar_rhythm_guide"
                    assert item_ids == [expected]
                    content = "日志仅写一次。" if expected == "calendar_day_due" else "周志仅写一次。"
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide["guide_id"],
                                "submissions": [{
                                    "item_id": expected,
                                    "option_id": "write_chronicle",
                                    "fields": {"content": content},
                                }],
                            },
                            call_id=f"call_{expected}",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "节律写入各一次，继续回应当前交互。",
                    "tool_call_envelopes": [],
                }

        executor = IncidentExecutor()
        rt.executor = executor
        reaction_result = {}
        run_reaction = rt._run_reaction_loop

        def capture_reaction(*args, **kwargs):
            result = run_reaction(*args, **kwargs)
            reaction_result.update(result)
            return result

        monkeypatch.setattr(rt, "_run_reaction_loop", capture_reaction)

        rt._run_one_round("rhythm", rt.sm.load(), rt.sm.get_flags())

        assert "GUIDE｜日历节律指南" not in executor.setup_messages
        assert [kind for kind, _items, _text in executor.reaction_guides[:3]] == [
            "emergency_handling_guide",
            "calendar_rhythm_guide",
            "calendar_rhythm_guide",
        ]
        assert executor.reaction_guides[0][1] == ["api_degraded"]
        assert executor.reaction_guides[1][1] == ["calendar_day_due"]
        assert executor.reaction_guides[2][1] == ["calendar_week_due"]
        assert rt.workbench.load_guide(stale_day_id)["status"] == "superseded"

        guide_receipts = reaction_result["_guide_submit_receipts"]
        assert len(guide_receipts) == 2
        backends = [
            receipt
            for guide_receipt in guide_receipts
            for receipt in guide_receipt.get("backend_receipts") or []
        ]
        chronicle_receipts = [
            receipt for receipt in backends
            if receipt.get("tool_id") == "chronicle_write"
        ]
        assert [(item["layer"], item["status"]) for item in chronicle_receipts] == [
            ("daily", "applied"),
            ("weekly", "applied"),
        ]
        assert not any(
            receipt.get("reason") == "no_active_chronicle_focus"
            for receipt in backends
        )
        assert not any(
            receipt.get("status") == "emergency_attempt_auto_deferred"
            for receipt in reaction_result["_reaction_loop_guard_receipts"]
        )
        flags = rt.sm.get_flags()
        assert flags["api_degraded"] is False
        assert flags["calendar_day_due"] is False
        assert flags["calendar_week_due"] is False
        assert flags["user_message_waiting"] is False
        round_path = tmp_path / "context" / "round" / "round_577.jsonl"
        events = [
            json.loads(line)
            for line in round_path.read_text(encoding="utf-8").splitlines()
        ]
        input_snapshot = events[0]["payload"]["input_snapshot"]
        assert input_snapshot["flags"]["api_degraded"] is True
        assert input_snapshot["pre_setup_alert_recovery"]["cleared_flags"] == []
        assert input_snapshot["pre_setup_alert_recovery"]["effective_round_type"] == "rhythm"
        assert any(event["event_type"] == "round_settled" for event in events)
        assert events[-1]["event_type"] == "round_closed"

    def test_no_active_chronicle_focus_never_completes_calendar_flag(self):
        from engines.reaction_loop import ReactionLoopRunner

        completed = ReactionLoopRunner._guide_completed_flags_from_receipts(
            [{
                "tool_id": "chronicle_write",
                "status": "rejected",
                "reason": "no_active_chronicle_focus",
            }],
            state={
                "base": {
                    "heartbeat_flags": {"calendar_day_due": True},
                }
            },
            round_type="rhythm",
        )

        assert completed == set()
