from pathlib import Path

from assembly.context import ContextAssembler
from data.context_store import ContextStore
from data.state_store import StateStore
from engines.round_context import SetupResult
from engines.runtime import Runtime


class FakeHeartbeat:
    def __init__(self):
        self.paused = 0
        self.resumed = 0
        self.messages = []

    def pause(self):
        self.paused += 1

    def resume(self):
        self.resumed += 1

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
        ({"continue_requested": True}, "relay"),
        ({"evolution_pending": True}, "autonomous"),
        ({"standby_due": True}, "standby"),
    ]

    for flags, expected in cases:
        trigger = rt.enqueue_trigger(flags, rt.sm.load())
        assert trigger.round_type == expected
        assert rt._trigger_queue.pop() is trigger


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
