from pathlib import Path
import threading

import pytest

from assembly.context import ContextAssembler
from data.context_store import ContextStore
from data.state_store import StateStore
from engines.round_context import SetupResult
from engines.runtime import Runtime
from runtime_test_helpers import ConfigStoreStub


class FakeHeartbeat:
    def __init__(self):
        self.paused = 0
        self.resumed = 0
        self.messages = []

    def pause(self):
        self.paused += 1

    def resume(self):
        self.resumed += 1

    def enqueue_message(self, message):
        self.messages.append(message)

    def dequeue_messages(self):
        messages = list(self.messages)
        self.messages.clear()
        return messages


class FakeSetupRunner:
    def __init__(self, setup_result):
        self.setup_result = setup_result
        self.calls = []
        self.commits = []

    def run(self, context):
        self.calls.append(context)
        return self.setup_result

    def commit(self, context, setup_result):
        self.commits.append((context, setup_result))


class FakeReactionRunner:
    def __init__(self, result=None, error=None):
        self.result = result if result is not None else {"response": "reaction"}
        self.error = error
        self.calls = []

    def run(self, context, setup_result):
        self.calls.append((context, setup_result))
        if self.error:
            raise self.error
        return dict(self.result)


class FakeCleanupPipeline:
    def __init__(self, outcome=None):
        self.calls = []
        self.outcome = outcome or {"status": "settled"}

    def run(self, context, result):
        self.calls.append((context, dict(result)))
        return dict(self.outcome)


def _runtime(tmp_path, **runtime_kwargs):
    sm = StateStore(str(tmp_path / "state.json"))
    sm.init_if_missing()
    ctx_store = ContextStore(
        state_store=sm,
        cache_dir=str(tmp_path / "context_cache"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )
    assembler = ContextAssembler(
        state_store=sm,
        context_dir=str(tmp_path / "context"),
        context_store=ctx_store,
    )
    return Runtime(
        state_store=sm,
        heartbeat=FakeHeartbeat(),
        ctx_store=ctx_store,
        assembler=assembler,
        config_store=ConfigStoreStub(),
        **runtime_kwargs,
    )


def _setup_result(**overrides):
    data = {
        "raw_result": {"response": "setup"},
        "intent": {"mount_requests": [], "security_verdict": "pass"},
        "interaction_meta": {
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "test",
        },
        "user_input_text": "hello",
        "setup_messages": [{"role": "user", "content": "setup"}],
        "internal_handoff": [],
    }
    data.update(overrides)
    return SetupResult(**data)


def test_runtime_accepts_explicit_organ_topology_and_callable_registries(
        tmp_path):
    topology = tmp_path / "organ_topology.json"
    topology.write_text('''{
      "schema_version": "upsp_organ_topology.v1",
      "roles": [{
        "id": "observer", "version": "1", "enabled": true,
        "axes": ["setup"], "subscriptions": ["setup_frame_settled"],
        "requires": [], "provides": [], "context_mode": "assembled",
        "context_provider": "context", "handler": "observer",
        "product_tools": []
      }]
    }''', encoding="utf-8")

    rt = _runtime(
        tmp_path,
        organ_topology_path=topology,
        organ_handlers={"observer": lambda invocation: {}},
        organ_context_providers={"context": lambda event: {}},
    )

    assert tuple(rt.organ_runtime.roles) == ("observer",)


def test_runtime_orchestrates_setup_reaction_cleanup_in_order(tmp_path):
    rt = _runtime(tmp_path)
    organ_calls = []

    class OrganRuntime:
        topology_version = "test-topology"

        @staticmethod
        def dispatch(event_type, frame_ref, payload, runtime_context):
            organ_calls.append((event_type, dict(runtime_context)))
            return {"records": [], "receipts": []}

    rt.organ_runtime = OrganRuntime()
    setup = FakeSetupRunner(_setup_result(
        intent={
            "mount_requests": [{"type": "memory", "ids": "MEM-TEST0001"}],
            "security_verdict": "pass",
        },
        internal_handoff=[{"role": "user", "content": "from setup"}],
    ))
    reaction = FakeReactionRunner({"response": "done"})
    cleanup = FakeCleanupPipeline()
    rt.setup_runner = setup
    rt.reaction_loop_runner = reaction
    rt.cleanup_pipeline = cleanup

    rt._run_one_round("interactive", rt.sm.load(), {"user_message_waiting": True})

    assert rt.hb.paused == 1
    assert rt.hb.resumed == 1
    assert setup.calls[0].round_type == "interactive"
    assert setup.calls[0].topology_version == rt.organ_runtime.topology_version
    assert reaction.calls[0][0].round_num == setup.calls[0].round_num
    assert reaction.calls[0][1].intent["mount_requests"][0]["ids"] == "MEM-TEST0001"
    assert cleanup.calls[0][1]["response"] == "done"
    assert cleanup.calls[0][1]["_interaction_meta"]["interaction_object"] == "Codex"
    assert organ_calls[0][0] == "setup_frame_settled"
    assert organ_calls[0][1]["interaction_meta"]["interaction_object"] == "Codex"


def test_spec743_runtime_waits_for_reserved_idle_mutation_before_pre_setup(
        tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline()
    outcome = {}

    def run_round():
        try:
            outcome["result"] = rt._run_one_round(
                "interactive", rt.sm.load(), {"user_message_waiting": True})
        except Exception as exc:  # surfaced below
            outcome["error"] = exc

    assert rt.control.reserve_idle_mutation() is True
    before = rt.sm.get("base.meta.total_round")
    thread = threading.Thread(target=run_round)
    try:
        thread.start()
        thread.join(timeout=0.1)
        assert thread.is_alive() is True
        assert rt.setup_runner.calls == []
        assert rt.sm.get("base.meta.total_round") == before

        rt.control.release_idle_mutation()
        thread.join(timeout=2)

        assert thread.is_alive() is False
        assert "error" not in outcome
        assert rt.setup_runner.calls
    finally:
        rt.control.release_idle_mutation()
        thread.join(timeout=2)


def test_spec704_round_finished_callback_runs_before_admission_reopens(tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline()
    observed = []
    rt.on_round_finished = lambda *_args: observed.append(
        rt.runtime_status()["round_in_flight"])

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    assert observed == [True]
    assert rt.runtime_status()["round_in_flight"] is False


def test_spec611_probe_runtime_does_not_resume_heartbeat_after_cleanup(
        tmp_path, monkeypatch):
    monkeypatch.setenv("UPSP_SINGLE_ROUND_PROBE", "1")
    rt = _runtime(tmp_path)
    rt.sm.set_flag("user_message_waiting", True)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline()

    flags = rt.sm.get_flags()
    rt._run_one_round("interactive", rt.sm.load(), flags)

    assert rt.hb.paused == 1
    assert rt.hb.resumed == 0
    assert rt.cleanup_pipeline.calls


def test_runtime_skips_reaction_when_setup_rejects_but_cleanup_runs(tmp_path):
    rt = _runtime(tmp_path)
    setup = FakeSetupRunner(_setup_result(
        intent={"mount_requests": [], "security_verdict": "reject"},
    ))
    reaction = FakeReactionRunner()
    cleanup = FakeCleanupPipeline()
    rt.setup_runner = setup
    rt.reaction_loop_runner = reaction
    rt.cleanup_pipeline = cleanup

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    assert reaction.calls == []
    assert cleanup.calls
    assert cleanup.calls[0][1]["error"]
    assert cleanup.calls[0][1]["response"]
    assert "setup" in cleanup.calls[0][1]["response"]


def test_runtime_skips_standby_reaction_when_setup_requests_skip(tmp_path):
    rt = _runtime(tmp_path)
    setup = FakeSetupRunner(_setup_result(
        intent={
            "mount_requests": [],
            "security_verdict": "pass",
            "standby_skip_reaction": True,
        },
    ))
    reaction = FakeReactionRunner()
    cleanup = FakeCleanupPipeline()
    rt.setup_runner = setup
    rt.reaction_loop_runner = reaction
    rt.cleanup_pipeline = cleanup

    rt._run_one_round("standby", rt.sm.load(), {"standby_due": True})

    assert reaction.calls == []
    assert cleanup.calls[0][1]["standby_skipped_reaction"] is True
    assert "_standby_reaction_hint" not in cleanup.calls[0][1]


def test_runtime_runs_cleanup_and_resumes_heartbeat_after_reaction_exception(tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner(error=RuntimeError("boom"))
    rt.cleanup_pipeline = FakeCleanupPipeline()

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    assert rt.cleanup_pipeline.calls
    assert rt.cleanup_pipeline.calls[0][1]["aborted"] is True
    assert "reaction" in rt.cleanup_pipeline.calls[0][1]["error"]
    assert rt.hb.resumed == 1
    assert rt.sm.get("base.runtime.phase") == "idle"


def test_resident_runtime_latches_after_non_settled_round(tmp_path):
    for status in ("degraded", "unsettled"):
        rt = _runtime(tmp_path / status)
        rt.setup_runner = FakeSetupRunner(_setup_result())
        rt.reaction_loop_runner = FakeReactionRunner()
        rt.cleanup_pipeline = FakeCleanupPipeline({"status": status})

        rt._run_one_round(
            "rhythm", rt.sm.load(), {"api_degraded": True})

        assert rt.hb.paused >= 1
        assert rt.hb.resumed == 0
        assert rt.control.stop_latched is True
        assert rt.release_stop_latch() is True
        assert rt.control.stop_latched is False


def test_settlement_exception_fails_closed_and_keeps_heartbeat_latched(
        tmp_path, monkeypatch):
    import engines.runtime as runtime_module

    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})

    def fail_settlement(*_args, **_kwargs):
        raise RuntimeError("settlement failed")

    monkeypatch.setattr(runtime_module, "settle_round", fail_settlement)
    result = rt._run_one_round(
        "rhythm", rt.sm.load(), {"calendar_day_due": True})

    assert result["status"] == "runtime_failed"
    assert result["_settlement"] == {
        "status": "unsettled",
        "degraded_reasons": [],
        "fatal_reasons": ["cleanup_step_exception:RuntimeError"],
    }
    assert rt.hb.resumed == 0
    assert rt.control.stop_latched is True
    assert "settlement failed" in rt.sm.get("base.meta.last_error")


def test_spec704_stop_before_round_does_not_allocate_round(tmp_path):
    rt = _runtime(tmp_path)
    before = rt.sm.get("base.meta.total_round")
    rt.control.cancel_before_round(rt)

    result = rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    assert result["status"] == "round_stopped"
    assert result["_user_stop_requested"] is True
    assert rt.sm.get("base.meta.total_round") == before


def test_spec704_provider_cancel_enters_cleanup_without_fake_reply(tmp_path):
    from errors import ProviderCallCancelled

    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())

    class CancelReaction(FakeReactionRunner):
        def run(self, context, setup_result):
            rt.control.stop_requested.set()
            raise ProviderCallCancelled()

    rt.reaction_loop_runner = CancelReaction()
    rt.cleanup_pipeline = FakeCleanupPipeline({"status": "degraded"})

    result = rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    cleanup_input = rt.cleanup_pipeline.calls[0][1]
    assert result["_user_stop_requested"] is True
    assert cleanup_input["_user_stop_requested"] is True
    assert cleanup_input["response"] == ""
    assert rt.control.stop_latched is True
    assert rt.hb.resumed == 0


def test_spec704_stop_after_real_reply_preserves_reply_for_local_cleanup(tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())

    class StopAfterReply(FakeReactionRunner):
        def run(self, context, setup_result):
            rt.control.stop_requested.set()
            return {"response": "真实最终回复"}

    rt.reaction_loop_runner = StopAfterReply()
    rt.cleanup_pipeline = FakeCleanupPipeline({"status": "degraded"})

    result = rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    assert result["response"] == "真实最终回复"
    assert rt.cleanup_pipeline.calls[0][1]["response"] == "真实最终回复"
    assert result["_user_stop_requested"] is True


def test_runtime_trigger_keeps_all_messages_in_arrival_order(tmp_path):
    rt = _runtime(tmp_path)
    rt.hb.messages = ["first", "second", "third"]

    trigger = rt.enqueue_trigger({"user_message_waiting": True}, rt.sm.load())

    assert trigger.round_type == "interactive"
    assert trigger.messages == ("first", "second", "third")
    assert rt._trigger_queue[-1] is trigger


def test_qualifier_does_not_create_runtime_trigger(tmp_path):
    rt = _runtime(tmp_path)

    assert rt.enqueue_trigger({"identity_timeout": True}, rt.sm.load()) is None
    assert not rt._trigger_queue


def test_feeling_settlement_does_not_create_runtime_trigger(tmp_path):
    rt = _runtime(tmp_path)

    assert rt.enqueue_trigger(
        {"feeling_settle_due": True}, rt.sm.load()) is None
    assert not rt._trigger_queue


def test_all_active_trigger_groups_enter_setup(tmp_path):
    rt = _runtime(tmp_path)
    cases = [
        ({"user_message_waiting": True}, "interactive"),
        ({"rhythm_due": True}, "rhythm"),
        ({"memory_compression_due": True}, "rhythm"),
        ({"continue_requested": True}, "relay"),
        ({"standby_due": True}, "standby"),
    ]

    for flags, expected in cases:
        trigger = rt.enqueue_trigger(flags, rt.sm.load())
        assert trigger.round_type == expected
        assert rt._trigger_queue.pop() is trigger


def test_spec721_automatic_trigger_inherits_explicit_permission_chain(tmp_path):
    rt = _runtime(tmp_path)
    assert rt.submit_message("start", "unlimited") is True
    source = rt.enqueue_trigger(
        {"user_message_waiting": True}, rt.sm.load())
    assert source.execution_permission_level == "unlimited"

    rt.sm.set_flag("continue_requested", True)
    rt.permission_chain.finish(
        source.execution_permission_level,
        {"_settlement": {"status": "settled"}}, rt.sm)
    trigger = rt.enqueue_trigger(
        {"rhythm_due": True, "continue_requested": True}, rt.sm.load())

    assert trigger.round_type == "rhythm"
    assert trigger.execution_permission_level == "unlimited"

    rt.sm.clear_flags(("continue_requested",))
    rt.permission_chain.finish(
        trigger.execution_permission_level,
        {"_settlement": {"status": "settled"}}, rt.sm)
    unrelated = rt.enqueue_trigger({"standby_due": True}, rt.sm.load())
    assert unrelated.execution_permission_level == "guarded"


def test_spec721_new_user_permission_replaces_previous_chain(tmp_path):
    rt = _runtime(tmp_path)
    rt.permission_chain.authorize("unlimited")
    assert rt.submit_message("new", "limited") is True

    trigger = rt._new_trigger(
        "rhythm", {
            "rhythm_due": True,
            "user_message_waiting": True,
            "continue_requested": True,
        })

    assert trigger.execution_permission_level == "limited"
    assert trigger.messages == ("new",)


def test_spec735_final_response_budget_is_process_local_across_relay(tmp_path):
    from engines.runtime_rhythm import (
        park_interaction_for_api_probe,
        restore_interaction_after_api_probe,
    )
    from logic.relay_intent_pool import open_relay_intents

    rt = _runtime(tmp_path)
    assert rt.submit_message(
        "query", "limited", final_response_max_chars=128) is True
    source = rt._new_trigger(
        "interactive", {"user_message_waiting": True}, rt.sm.load())
    assert source.final_response_max_chars == 128
    assert source.as_dict()["final_response_max_chars"] == 128
    parked_messages = []
    rt.hb.prepend_messages = lambda messages: parked_messages.extend(messages)
    rt.hb.dequeue_messages = lambda: list(parked_messages)
    parked, was_parked = park_interaction_for_api_probe(
        rt, source, {"api_degraded": True})
    assert was_parked is True
    source, _flags = restore_interaction_after_api_probe(
        rt,
        parked,
        {"api_degraded": True},
        {"status": "ok"},
        was_parked,
    )
    assert source.final_response_max_chars == 128
    assert rt._pending_final_response_max_chars is None

    rt.cleanup_pipeline._rearm_continue_requested_from_closeout_form(
        {
            "_closeout_relay_receipts": [{
                "status": "continue_requested_set",
                "source": "closeout_form",
                "set_flags": ["continue_requested"],
                "handoff_text": "continue",
            }],
        },
        round_type="interactive",
        consumed_flags=[],
        round_num=735,
    )
    assert "final_response_max_chars" not in open_relay_intents(rt.sm.load())[-1]
    rt._continuation_final_response_budget = {
        "max_chars": 128,
        "rejections": 1,
    }
    relay = rt._new_trigger(
        "relay", {"continue_requested": True}, rt.sm.load())
    assert relay.final_response_max_chars == 128
    assert relay.final_response_length_rejections == 1

    restarted = _runtime(tmp_path)
    relay_after_restart = restarted._new_trigger(
        "relay", {"continue_requested": True}, restarted.sm.load())
    assert relay_after_restart.final_response_max_chars is None
    assert relay_after_restart.final_response_length_rejections == 0


def test_spec738_response_contract_is_process_local_across_relay(tmp_path):
    from engines.runtime_rhythm import (
        park_interaction_for_api_probe,
        restore_interaction_after_api_probe,
    )

    contract = {
        "language": "en",
        "answer_scope": "conclusion_only",
        "max_sentences": 1,
    }
    rt = _runtime(tmp_path)
    assert rt.submit_message(
        "query", "limited", response_contract=contract) is True
    source = rt._new_trigger(
        "interactive", {"user_message_waiting": True}, rt.sm.load())
    assert source.response_contract == contract
    assert source.as_dict()["response_contract"] == contract

    parked_messages = []
    rt.hb.prepend_messages = lambda messages: parked_messages.extend(messages)
    rt.hb.dequeue_messages = lambda: list(parked_messages)
    parked, was_parked = park_interaction_for_api_probe(
        rt, source, {"api_degraded": True})
    source, _flags = restore_interaction_after_api_probe(
        rt, parked, {"api_degraded": True}, {"status": "ok"}, was_parked)
    assert source.response_contract == contract

    rt._continuation_final_response_budget = {
        "response_contract": contract,
    }
    relay = rt._new_trigger(
        "relay", {"continue_requested": True}, rt.sm.load())
    assert relay.response_contract == contract

    restarted = _runtime(tmp_path)
    relay_after_restart = restarted._new_trigger(
        "relay", {"continue_requested": True}, restarted.sm.load())
    assert relay_after_restart.response_contract == {}


def test_spec738_task_guidance_suppression_is_process_local_across_relay(tmp_path):
    rt = _runtime(tmp_path)
    assert rt.submit_message(
        "query", "limited", task_guidance_enabled=False) is True
    source = rt._new_trigger(
        "interactive", {"user_message_waiting": True}, rt.sm.load())
    assert source.task_guidance_enabled is False
    assert source.as_dict()["task_guidance_enabled"] is False

    rt._continuation_final_response_budget = {
        "task_guidance_enabled": False,
    }
    relay = rt._new_trigger(
        "relay", {"continue_requested": True}, rt.sm.load())
    assert relay.task_guidance_enabled is False

    restarted = _runtime(tmp_path)
    relay_after_restart = restarted._new_trigger(
        "relay", {"continue_requested": True}, restarted.sm.load())
    assert relay_after_restart.task_guidance_enabled is True
    assert "task_guidance_enabled" not in relay_after_restart.as_dict()


def test_spec725_active_round_permission_changes_at_next_frame_boundary(tmp_path):
    rt = _runtime(tmp_path)
    rt.audit.start(725, "interactive", {})
    assert rt.control.establish_round("interactive", lambda: 725) == 725
    rt.permission_chain.apply("guarded")

    receipt = rt.permission_updates.request("unlimited")
    assert receipt["status"] == "pending"
    assert rt.permission_chain.current == "guarded"
    assert rt.runtime_status()["execution_permission"]["pending_level"] == "unlimited"

    applied = rt.permission_updates.apply(725, "reaction", 5)

    assert applied["previous"]["permission_level"] == "guarded"
    assert applied["current"]["permission_level"] == "unlimited"
    assert applied["effective_frame_id"] == "R000725:reaction:5"
    assert rt.permission_chain.current == "unlimited"
    assert rt.reaction_loop_runner.execution_permission_level == "unlimited"
    assert rt.runtime_status()["execution_permission"]["pending_level"] is None


def test_spec725_active_round_permission_rejects_cleanup_change(tmp_path):
    rt = _runtime(tmp_path)
    assert rt.control.establish_round("interactive", lambda: 725) == 725
    rt.control.set_stage("cleanup_model")

    with pytest.raises(ValueError, match="permission_change_too_late"):
        rt.permission_updates.request("limited")


def test_newer_queued_trigger_discards_stale_setup_result(tmp_path):
    rt = _runtime(tmp_path)
    setup = FakeSetupRunner(_setup_result())
    original_run = setup.run

    def run_and_queue_newer(context):
        rt.enqueue_trigger({"rhythm_due": True}, rt.sm.load())
        return original_run(context)

    setup.run = run_and_queue_newer
    reaction = FakeReactionRunner()
    cleanup = FakeCleanupPipeline()
    rt.setup_runner = setup
    rt.reaction_loop_runner = reaction
    rt.cleanup_pipeline = cleanup

    rt._run_one_round("interactive", rt.sm.load(), {"user_message_waiting": True})

    assert setup.commits == []
    assert reaction.calls == []
    assert cleanup.calls[0][1]["_stale_setup_discarded"] == "T00000001"


def test_missing_setup_result_never_enters_reaction(tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(None)
    reaction = FakeReactionRunner()
    cleanup = FakeCleanupPipeline()
    rt.reaction_loop_runner = reaction
    rt.cleanup_pipeline = cleanup

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    assert reaction.calls == []
    assert cleanup.calls[0][1]["_failed_phase"] == "setup"


def test_round_lifecycle_closes_only_after_cleanup_settlement(tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline()

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    events = rt.audit.get_store().read_events(1)
    lifecycle = [
        event["event_type"] for event in events
        if event["event_type"].startswith("round_")
        or event["event_type"].startswith("cleanup_obligation_")
    ]
    assert lifecycle == [
        "round_started",
        "round_close_requested",
        "cleanup_obligation_created",
        "cleanup_obligation_settled",
        "round_settled",
        "round_closed",
    ]
    assert rt.ctx_store.get_now_entries() == []
    assert any(
        entry.get("content") == "done"
        for entry in rt.ctx_store.get_lately_entries()
    )


def test_spec756_closeout_source_requires_verified_continue_rearm():
    from logic.runtime_channels import closeout_final_response_source

    pending = {
        "response": "",
        "_exit_signal": "continue_requested",
        "_closeout_relay_receipts": [{
            "status": "continue_requested_set",
            "set_flags": ["continue_requested"],
        }],
    }
    verified = {
        **pending,
        "_heartbeat_rearm_receipts": [{
            "status": "continue_requested_rearmed",
            "set_flags": ["continue_requested"],
            "relay_intent": {
                "status": "open",
                "relay_intent_id": "RI-756",
            },
        }],
    }

    assert closeout_final_response_source(pending) == "reaction.final_reply_text"
    assert closeout_final_response_source(verified) == "reaction.continue_handoff"


def test_spec756_round_closed_projects_verified_continue_handoff(tmp_path):
    class VerifiedContinueCleanup:
        def run(self, _context, result):
            result["_heartbeat_rearm_receipts"] = [{
                "status": "continue_requested_rearmed",
                "set_flags": ["continue_requested"],
                "relay_intent": {
                    "status": "open",
                    "relay_intent_id": "RI-756",
                },
            }]
            return {"status": "settled"}

    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({
        "response": "",
        "_exit_signal": "continue_requested",
    })
    rt.cleanup_pipeline = VerifiedContinueCleanup()

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    closed = next(
        event for event in rt.audit.get_store().read_events(1)
        if event["event_type"] == "round_closed"
    )
    assert closed["payload"]["final_response"] == ""
    assert closed["payload"]["final_response_source"] == (
        "reaction.continue_handoff"
    )


def test_round_lifecycle_rejects_unverified_cleanup_closeout_receipt(tmp_path):
    class CleanupWithBogusCloseout:
        def run(self, context, result):
            result["_current_cache_closeout"] = {"status": "noop"}
            return {"status": "settled"}

    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = CleanupWithBogusCloseout()

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    events = rt.audit.get_store().read_events(1)
    assert "round_closed" in [event["event_type"] for event in events]
    receipt = next(
        event["payload"] for event in events
        if event["event_type"] == "current_cache_transition"
    )
    assert receipt["schema_version"] == "current_cache_transition.v1"
    assert receipt["boundary"] == "round_closeout"
    assert rt.ctx_store.get_now_entries() == []


def test_round_lifecycle_cache_closeout_failure_never_false_closes(
        tmp_path, monkeypatch):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline()

    def fail_closeout(**_kwargs):
        raise OSError("disk full")

    monkeypatch.setattr(rt.ctx_store, "transition_current_cache", fail_closeout)
    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    events = rt.audit.get_store().read_events(1)
    types = [event["event_type"] for event in events]
    assert "round_unsettled" in types
    assert "round_closed" not in types
    assert any(
        entry.get("content") == "done"
        for entry in rt.ctx_store.get_now_entries()
    )
    unsettled = next(
        event for event in events if event["event_type"] == "round_unsettled")
    assert any(
        "current_cache_closeout:OSError:disk full" in reason
        for reason in unsettled["payload"]["fatal_reasons"]
    )


def test_unsettled_cleanup_records_debt_without_false_close(tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline({
        "status": "unsettled",
        "fatal_reasons": ["state_backup:disk_full"],
    })

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    events = rt.audit.get_store().read_events(1)
    types = [event["event_type"] for event in events]
    assert "cleanup_obligation_failed" in types
    assert "round_unsettled" in types
    assert "cleanup_obligation_settled" not in types
    assert "round_closed" not in types


def test_required_context_failure_still_runs_cleanup_without_false_close(tmp_path):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({
        "aborted": True,
        "response": "",
        "error": "required context failed",
        "_required_context_failure": {
            "receipt_type": "required_context_failure.v1",
            "status": "failed",
            "stage": "projection",
            "scope": "protocol_tool_fact",
            "error_type": "OSError",
        },
    })
    rt.cleanup_pipeline = FakeCleanupPipeline()

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    events = rt.audit.get_store().read_events(1)
    types = [event["event_type"] for event in events]
    assert rt.cleanup_pipeline.calls
    assert "cleanup_obligation_failed" in types
    assert "round_unsettled" in types
    assert "round_closed" not in types
    unsettled = next(
        event for event in events if event["event_type"] == "round_unsettled")
    assert "required_context:projection:protocol_tool_fact:OSError" in (
        unsettled["payload"]["fatal_reasons"])


def test_spec721_reaction_exception_reports_provider_and_local_causes_separately():
    from engines.runtime_control import RuntimeControl
    from errors import APIBridgeError, RequiredContextError

    provider = RuntimeControl.step_exception_result(
        "reaction", APIBridgeError("provider", "HTTP 503"))
    context = RuntimeControl.step_exception_result(
        "reaction", RequiredContextError("projection", "protocol_tool_fact", OSError()))
    local = RuntimeControl.step_exception_result(
        "reaction", RuntimeError("Corpus raw_log_key conflict"))

    assert provider["_local_blocked_reason"] == "blocked/provider_failure"
    assert "模型调用" in provider["response"]
    assert context["_local_blocked_reason"] == "blocked/required_context_failure"
    assert context["_required_context_failure"]["scope"] == "protocol_tool_fact"
    assert "上下文" in context["response"]
    assert local["_local_blocked_reason"] == "blocked/runtime_error"
    assert "Runtime 内部错误" in local["response"]


def test_round_audit_failure_does_not_skip_cleanup_or_false_close(
        tmp_path, monkeypatch):
    rt = _runtime(tmp_path)
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline()
    store = rt.audit.get_store()
    append_event = store.append_event
    failed = False

    def fail_first_close_request(round_num, event_type, payload=None, **kwargs):
        nonlocal failed
        if event_type == "round_close_requested" and not failed:
            failed = True
            raise OSError("audit unavailable")
        return append_event(round_num, event_type, payload, **kwargs)

    monkeypatch.setattr(store, "append_event", fail_first_close_request)
    monkeypatch.setattr(rt.audit, "_new_store", lambda: store)

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    events = store.read_events(1)
    types = [event["event_type"] for event in events]
    assert rt.cleanup_pipeline.calls
    assert "cleanup_obligation_failed" in types
    assert "round_unsettled" in types
    assert "round_closed" not in types
    unsettled = next(
        event for event in events if event["event_type"] == "round_unsettled")
    assert "round_audit:round_close_requested:OSError" in (
        unsettled["payload"]["fatal_reasons"])


def test_post_close_organ_notification_failure_does_not_reopen_round(tmp_path):
    rt = _runtime(tmp_path)

    class OrganRuntime:
        topology_version = "test-topology"

        @staticmethod
        def dispatch(event_type, *_args, **_kwargs):
            if event_type == "round_closed":
                raise RuntimeError("organ notification unavailable")
            return {"records": [], "receipts": []}

    rt.organ_runtime = OrganRuntime()
    rt.setup_runner = FakeSetupRunner(_setup_result())
    rt.reaction_loop_runner = FakeReactionRunner({"response": "done"})
    rt.cleanup_pipeline = FakeCleanupPipeline()

    rt._run_one_round(
        "interactive", rt.sm.load(), {"user_message_waiting": True})

    types = [
        event["event_type"] for event in rt.audit.get_store().read_events(1)]
    assert "round_closed" in types
    assert "round_unsettled" not in types


def test_runtime_py_is_lean_orchestrator_after_spec133():
    runtime_path = Path(__file__).parents[1] / "engines" / "runtime.py"
    text = runtime_path.read_text(encoding="utf-8")

    assert len(text.splitlines()) <= 600
    assert "_run_reaction_loop_impl" not in text
    assert "_run_cleanup_impl" not in text
    assert "parse_setup_output" not in text
    assert "parse_reaction_output" not in text
    assert "parse_cleanup_output" not in text
    assert "memory_write_declaration" not in text
