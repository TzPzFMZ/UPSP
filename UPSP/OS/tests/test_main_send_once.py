import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def test_spec612_build_runtime_is_full_only_and_rejects_retired_profile(monkeypatch):
    import main as os_main

    seen = {}

    class FakeExecutor:
        def __init__(self, *, config_store):
            seen["config_store"] = config_store

    class FakeAssembler:
        def __init__(self, *, state_store, context_profile):
            seen["state_store"] = state_store
            seen["context_profile"] = context_profile

    class FakeHeartbeat:
        def __init__(self, *, state_store, config_store):
            seen["heartbeat"] = (state_store, config_store)

    class FakeRuntime:
        def __init__(self, **kwargs):
            seen["runtime"] = kwargs

    monkeypatch.setattr(os_main, "APIExecutor", FakeExecutor)
    monkeypatch.setattr(os_main, "ContextAssembler", FakeAssembler)
    monkeypatch.setattr(os_main, "HeartbeatManager", FakeHeartbeat)
    monkeypatch.setattr(os_main, "Runtime", FakeRuntime)

    sm = object()
    cfg = object()
    os_main.build_runtime(sm, cfg)

    assert seen["context_profile"] == "full"
    assert seen["state_store"] is sm
    assert seen["config_store"] is cfg

    try:
        os_main.build_runtime(sm, cfg, context_profile="popup_exception_only")
    except ValueError as exc:
        assert str(exc) == "retired_context_profile:popup_exception_only"
    else:
        raise AssertionError("retired POPUP profile must fail closed")

    try:
        os_main.build_runtime(sm, cfg, context_profile="mystery")
    except ValueError as exc:
        assert str(exc) == "unsupported_context_profile:mystery"
    else:
        raise AssertionError("unknown live context profile must fail closed")


def test_spec607_runtime_round_start_audits_actual_context_profile():
    from collections import deque
    from types import SimpleNamespace
    from engines.round_context import FrameRef
    from engines.runtime import Runtime
    from engines.runtime_control import RuntimeControl
    from logic.execution_permission import ExecutionPermissionChain

    seen = {}

    class FakeStateStore:
        def set_phase(self, value):
            seen.setdefault("phases", []).append(value)

        def increment_round(self):
            return 607

        def load(self):
            return {"base": {"heartbeat_flags": {}}}

        def set(self, *_args):
            pass

    class FakeHeartbeat:
        def pause(self):
            pass

        def resume(self):
            pass

    class FakeAudit:
        class Store:
            @staticmethod
            def append_event(*_args, **_kwargs):
                pass

            @staticmethod
            def close_round(*_args, **_kwargs):
                pass

        def reset(self):
            pass

        def start(self, round_num, round_type, input_snapshot):
            seen["audit"] = (round_num, round_type, input_snapshot)

        def get_store(self):
            return self.Store()

    runtime = object.__new__(Runtime)
    object.__setattr__(runtime, "services", SimpleNamespace(
        sm=FakeStateStore(),
        hb=FakeHeartbeat(),
        assembler=SimpleNamespace(context_profile="full"),
    ))
    object.__setattr__(runtime, "audit", FakeAudit())
    object.__setattr__(runtime, "control", RuntimeControl())
    object.__setattr__(runtime, "permission_chain", ExecutionPermissionChain())
    object.__setattr__(runtime, "on_round_started", None)
    object.__setattr__(runtime, "on_round_finished", None)
    object.__setattr__(runtime, "_trigger_queue", deque())
    object.__setattr__(runtime, "_trigger_seq", 0)
    object.__setattr__(runtime, "_latest_setup_trigger_seq", 0)
    object.__setattr__(runtime, "organ_runtime", SimpleNamespace(
        topology_version="test-topology",
        dispatch=lambda *_args, **_kwargs: {"records": [], "receipts": []},
    ))
    object.__setattr__(
        runtime,
        "setup_runner",
        SimpleNamespace(run=lambda _context: SimpleNamespace(
            raw_result={},
            user_input_text="profile probe",
            interaction_meta={},
            setup_messages=[],
            internal_handoff="",
            frame_ref=FrameRef.for_axis(
                607, "setup", 1, trigger_id="T00000001"),
            intent={
                "security_verdict": "reject",
                "reject_reason": "audit_probe_complete",
            },
        )),
    )
    object.__setattr__(runtime, "reaction_loop_runner", SimpleNamespace())
    object.__setattr__(runtime, "cleanup_pipeline", SimpleNamespace())
    object.__setattr__(runtime, "_wake_if_sleeping", lambda: None)
    object.__setattr__(runtime, "_update_daily_if_needed", lambda *_args: None)
    object.__setattr__(
        runtime,
        "_prepare_chronicle_focus_for_round",
        lambda *_args: None,
    )
    object.__setattr__(runtime, "_run_cleanup", lambda *_args, **_kwargs: None)
    object.__setattr__(
        runtime,
        "_record_cache_compaction_rhythm_if_needed",
        lambda *_args: None,
    )

    runtime._run_one_round(
        "interactive",
        {"base": {}},
        {"user_message_waiting": True},
    )

    round_num, round_type, audit_input = seen["audit"]
    assert (round_num, round_type) == (607, "interactive")
    assert audit_input["flags"] == {"user_message_waiting": True}
    assert audit_input["context_profile"] == "full"
    assert audit_input["trigger"]["trigger_id"] == "T00000001"
    assert audit_input["trigger"]["messages"] == []
    assert seen["phases"][-1] == "idle"


def test_spec356_send_message_once_uses_unified_round_decision_for_coalesced_rhythm(monkeypatch):
    import main as os_main

    calls = []

    class FakeStateStore:
        def __init__(self):
            self.flags = {
                "api_degraded": True,
                "calendar_day_due": True,
                "user_message_waiting": False,
            }

        def init_if_missing(self):
            return False

        def get_flags(self):
            return dict(self.flags)

        def load(self):
            return {"base": {"heartbeat_flags": dict(self.flags)}}

    class FakeHeartbeat:
        def __init__(self, sm):
            self.sm = sm

        def start(self):
            pass

        def stop(self):
            pass

        def enqueue_message(self, message):
            assert message == "hello"
            self.sm.flags["user_message_waiting"] = True
            self.sm.flags["api_degraded"] = True

        def wait_for_wakeup(self, timeout=None):
            return True

    class FakeRuntime:
        def __init__(self, sm):
            self.sm = sm
            self.hb = FakeHeartbeat(sm)
            self.on_round_complete = None

        def _determine_round_type(self, flags):
            assert flags["api_degraded"] is True
            return "rhythm"

        def _determine_round_decision(self, flags):
            assert flags["api_degraded"] is True
            return {
                "round_type": "rhythm",
                "guide_queue": [
                    {"kind": "emergency", "flags": ["api_degraded"]},
                    {"kind": "calendar", "flags": ["calendar_day_due"]},
                    {"kind": "interaction", "flags": ["user_message_waiting"]},
                ],
                "coalesced": True,
                "deferred_items": [],
            }

        def _run_one_round(self, round_type, state, flags):
            calls.append(round_type)
            self.on_round_complete(
                326,
                "ok",
                round_type == "interactive",
            )

    sm = FakeStateStore()
    seen_profiles = []
    monkeypatch.setattr(
        os_main,
        "build_runtime",
        lambda _sm, _cfg, *, context_profile="full": (
            seen_profiles.append(context_profile) or FakeRuntime(sm)
        ),
    )

    result = os_main.send_message_once(
        sm,
        object(),
        "hello",
    )

    assert calls == ["rhythm"]
    assert seen_profiles == ["full"]
    assert result["context_profile"] == "full"
    assert result["round_type"] == "rhythm"
    assert result["is_interactive"] is False
    assert result["coalesced"] is True
    assert [item["kind"] for item in result["guide_queue"]] == ["emergency", "calendar", "interaction"]
    assert "api_degraded" in result["active_flags"]


def test_spec611_send_once_probe_clears_tick_flags_before_round_choice(monkeypatch):
    import main as os_main

    monkeypatch.setenv("UPSP_SINGLE_ROUND_PROBE", "1")
    calls = []
    heartbeats = []

    class FakeStateStore:
        def __init__(self):
            self.flags = {
                "calendar_day_due": True,
                "standby_due": True,
                "user_message_waiting": False,
            }

        def init_if_missing(self):
            return False

        def get_flags(self):
            return dict(self.flags)

        def set_flag(self, name, value):
            self.flags[name] = value

        def load(self):
            return {"base": {"heartbeat_flags": dict(self.flags)}}

    class FakeHeartbeat:
        def __init__(self, sm):
            self.sm = sm
            self.started = False

        def start(self):
            self.started = True

        def stop(self):
            pass

        def enqueue_message(self, message):
            self.sm.flags["user_message_waiting"] = True

        def wait_for_wakeup(self, timeout=None):
            return True

    class FakeRuntime:
        def __init__(self, sm):
            self.sm = sm
            self.hb = FakeHeartbeat(sm)
            heartbeats.append(self.hb)
            self.on_round_complete = None

        def _determine_round_decision(self, flags):
            assert [name for name, value in flags.items() if value] == [
                "user_message_waiting"
            ]
            return {
                "round_type": "interactive",
                "guide_queue": [{"kind": "interaction", "flags": ["user_message_waiting"]}],
                "coalesced": False,
                "deferred_items": [],
            }

        def _run_one_round(self, round_type, state, flags, *, probe_policy=None):
            calls.append((round_type, dict(flags), dict(probe_policy or {})))
            self.on_round_complete(611, "ok", True)

    sm = FakeStateStore()
    monkeypatch.setattr(
        os_main,
        "build_runtime",
        lambda _sm, _cfg, *, context_profile="full": FakeRuntime(sm),
    )

    result = os_main.send_message_once(sm, object(), "probe")

    assert calls[0][0] == "interactive"
    assert calls[0][1]["calendar_day_due"] is False
    assert calls[0][1]["standby_due"] is False
    assert calls[0][2]["suppressed_flags"] == [
        "calendar_day_due",
        "standby_due",
    ]
    assert result["status"] == "round_completed"
    assert result["round_type"] == "interactive"
    assert result["probe_policy"]["status"] == "prepared"
    assert result["active_flags"] == ["user_message_waiting"]
    assert heartbeats[0].started is False


def test_spec363_explicit_relay_does_not_bypass_higher_priority_flags(monkeypatch):
    import main as os_main

    calls = []

    class FakeStateStore:
        def __init__(self):
            self.flags = {
                "api_degraded": True,
                "identity_timeout": True,
                "continue_requested": True,
            }

        def init_if_missing(self):
            return False

        def get_flags(self):
            return dict(self.flags)

        def load(self):
            return {"base": {"heartbeat_flags": dict(self.flags)}}

    class FakeHeartbeat:
        def start(self):
            pass

        def stop(self):
            pass

    class FakeRuntime:
        def __init__(self, sm):
            self.sm = sm
            self.hb = FakeHeartbeat()
            self.on_round_complete = None

        def _determine_round_type(self, flags):
            assert flags["api_degraded"] is True
            return "rhythm"

        def _determine_round_decision(self, flags):
            assert flags["api_degraded"] is True
            return {
                "round_type": "rhythm",
                "guide_queue": [
                    {"kind": "emergency", "flags": ["api_degraded"]},
                ],
                "coalesced": False,
                "deferred_items": [
                    {"kind": "relay", "flags": ["continue_requested"]},
                ],
            }

        def _run_one_round(self, round_type, state, flags):
            calls.append(round_type)
            self.on_round_complete(
                327,
                "ok",
                round_type == "interactive",
            )

    sm = FakeStateStore()
    monkeypatch.setattr(
        os_main,
        "build_runtime",
        lambda _sm, _cfg, *, context_profile="full": FakeRuntime(sm),
    )

    result = os_main.run_pending_once(sm, object(), required_round_type="relay")

    assert calls == []
    assert result["round_type"] == "rhythm"
    assert result["status"] == "wrong_round_type"
    assert result["is_interactive"] is False
    assert "api_degraded" in result["active_flags"]
    assert result["deferred_items"] == [
        {"kind": "relay", "flags": ["continue_requested"]},
    ]
    assert result["context_profile"] == "full"
