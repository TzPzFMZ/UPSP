import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import multiprocessing
from pathlib import Path
import threading
import time

import pytest

from engines.executor import APIExecutor
from engines.resident_runtime import (
    ResidentRuntimeService,
    RuntimeAlreadyRunning,
    RuntimeServiceError,
    RuntimeSupervisorCorrupt,
)
from engines.runtime_control import RuntimeControl
from errors import ProviderCallCancelled
from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub


class NoopConnectivity:
    def __init__(self):
        self.events = []

    def log_latency(self, endpoint, status, message=""):
        self.events.append((endpoint, status, message))


class TransportConfig(ConfigStoreStub):
    def get_request_timeout(self):
        return 180

    def get_stream_first_chunk_timeout(self):
        return 180

    def get_stream_idle_timeout(self):
        return 180

    def get_stream_content_overrun_chars(self):
        return 1_000_000


class BlockingProvider:
    def __init__(self, mode):
        self.mode = mode
        self.release = threading.Event()
        self.request_seen = threading.Event()
        self.client_closed = threading.Event()
        self.request_count = 0
        owner = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *_args):
                return

            def do_POST(self):  # noqa: N802
                owner.request_count += 1
                owner.request_seen.set()
                self.rfile.read(int(self.headers.get("Content-Length", "0")))
                if owner.mode == "ordinary":
                    owner.release.wait(10)
                    self._json()
                    return
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                if owner.mode == "first_chunk":
                    owner.release.wait(10)
                    return
                self._chunk("first")
                if owner.mode == "idle":
                    owner.release.wait(10)
                    return
                index = 0
                while not owner.release.wait(0.05):
                    index += 1
                    if not self._chunk(str(index)):
                        return

            def _json(self):
                body = json.dumps({
                    "choices": [{"message": {"content": "ok"}}],
                }).encode("utf-8")
                try:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(body)))
                    self.end_headers()
                    self.wfile.write(body)
                except (BrokenPipeError, ConnectionResetError):
                    pass

            def _chunk(self, content):
                data = json.dumps({
                    "choices": [{
                        "delta": {"content": content},
                        "finish_reason": None,
                    }],
                })
                try:
                    self.wfile.write(f"data: {data}\n\n".encode("utf-8"))
                    self.wfile.flush()
                    return True
                except (BrokenPipeError, ConnectionResetError):
                    owner.client_closed.set()
                    return False

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(
            target=self.server.serve_forever, daemon=True)
        self.thread.start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.server.server_address[1]}/v1/chat/completions"

    def close(self):
        self.release.set()
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)


@pytest.mark.parametrize("mode", ["ordinary", "first_chunk", "idle", "continuous"])
def test_spec704_real_transport_stops_all_waiting_modes_within_two_seconds(mode):
    provider = BlockingProvider(mode)
    executor = APIExecutor(
        TransportConfig(), connectivity_store=NoopConnectivity())
    outcome = {}
    payload = {
        "model": "local-blocking",
        "messages": [{"role": "user", "content": "wait"}],
        "stream": mode != "ordinary",
    }

    def call():
        try:
            executor._send_request_cancellable(provider.url, "secret", payload)
        except Exception as exc:  # assertion below checks the exact type
            outcome["error"] = exc

    thread = threading.Thread(target=call)
    thread.start()
    assert provider.request_seen.wait(30)
    started = time.monotonic()
    executor.request_cancel()
    thread.join(timeout=2)
    elapsed = time.monotonic() - started
    provider.close()

    assert not thread.is_alive()
    assert elapsed < 2
    assert isinstance(outcome.get("error"), ProviderCallCancelled)
    assert provider.request_count == 1
    assert executor.transport_active is False


def _provider_parent_harness(url):
    executor = APIExecutor(
        TransportConfig(), connectivity_store=NoopConnectivity())
    executor._send_request_cancellable(url, "secret", {
        "model": "local-blocking",
        "messages": [{"role": "user", "content": "wait"}],
        "stream": True,
    })


def test_spec704_provider_worker_exits_when_resident_parent_crashes():
    provider = BlockingProvider("continuous")
    process = multiprocessing.get_context("spawn").Process(
        target=_provider_parent_harness,
        args=(provider.url,),
    )
    process.start()
    assert provider.request_seen.wait(30)
    process.terminate()
    process.join(timeout=2)
    assert not process.is_alive()
    assert provider.client_closed.wait(3)
    provider.close()


def test_spec704_cancellation_bypasses_retry_breaker_and_connectivity(monkeypatch):
    connectivity = NoopConnectivity()
    executor = APIExecutor(
        TransportConfig(), connectivity_store=connectivity)
    calls = []

    class Breaker:
        def record_success(self):
            raise AssertionError("cancel must not record provider success")

        def record_failure(self):
            raise AssertionError("cancel must not record provider failure")

    def cancel(*_args):
        calls.append(True)
        raise ProviderCallCancelled()

    monkeypatch.setattr(executor, "_send_request_cancellable", cancel)
    with pytest.raises(ProviderCallCancelled):
        executor.call_prepared_once({
            "step": "setup",
            "tier": "primary",
            "health_endpoint": "profile-a",
            "endpoint_config": {
                "url": "http://127.0.0.1/never",
                "model": "unit",
                "provider": "openai_chat",
            },
            "breaker": Breaker(),
            "request_url": "http://127.0.0.1/never",
            "provider": "openai_chat",
            "model_name": "unit",
            "payload": {"messages": []},
        })

    assert calls == [True]
    assert connectivity.events == []


class FakeExecutor:
    def __init__(self):
        self.cancelled = 0

    def request_cancel(self):
        self.cancelled += 1

    def reset_cancellation(self):
        return None


class FakeHeartbeat:
    def __init__(self):
        self._paused = False
        self.pause_calls = 0
        self.resume_calls = 0

    def pause(self):
        self.pause_calls += 1
        self._paused = True

    def resume(self, run_tick=True):
        self.resume_calls += 1
        self._paused = False


class FakeAuditStore:
    def __init__(self, events=None):
        self.events = list(events or [])

    def append_event(self, round_num, event_type, payload=None, **_kwargs):
        event = {
            "round": round_num,
            "event_type": event_type,
            "payload": dict(payload or {}),
        }
        self.events.append(event)
        return event

    def read_events(self, _round_num):
        return list(self.events)


class FakeAudit:
    def __init__(self, store=None):
        self.store = store or FakeAuditStore()

    def get_store(self):
        return self.store


class FakeRuntime:
    def __init__(self):
        self.control = RuntimeControl()
        self.executor = FakeExecutor()
        self.hb = FakeHeartbeat()
        self.audit = FakeAudit()
        self.on_round_started = None
        self.on_round_finished = None
        self.on_round_released = None
        self._stopped = threading.Event()
        self.execution_permission_level = "unlimited"
        self.final_response_max_chars = None
        self.response_contract = {}
        self.task_guidance_enabled = True

    def set_execution_permission_level(self, level):
        self.execution_permission_level = level
        return level

    def run_forever(self):
        self._stopped.wait()

    def request_shutdown(self):
        self._stopped.set()

    def request_stop(self):
        return self.control.request_stop(self)

    def runtime_status(self):
        return self.control.snapshot(self.hb)

    def submit_message(self, message, execution_permission_level="guarded",
                       final_response_max_chars=None, response_contract=None,
                       task_guidance_enabled=True):
        self.execution_permission_level = execution_permission_level
        self.final_response_max_chars = final_response_max_chars
        self.response_contract = dict(response_contract or {})
        self.task_guidance_enabled = task_guidance_enabled
        number = self.control.establish_round("interactive", lambda: 1)
        if number is None:
            return False
        self.on_round_started(number, "interactive")
        result = {
            "response": f"reply:{message}",
            "_settlement": {"status": "settled"},
        }
        if message == "blocked":
            result.update({
                "response": "本地阻断说明",
                "_final_response_source": (
                    "reaction.runtime_auto_blocked_final_reply"),
                "_local_blocked_reason": "final_response_length_exhausted",
            })
        self.on_round_finished(number, "interactive", result)
        self.control.finish_round(False)
        self.on_round_released(number, "interactive", result)
        return True

    def cancel_pending_input(self):
        return True


def service(tmp_path, runtime=None, *, persona_ready=lambda: True):
    runtime = runtime or FakeRuntime()
    return ResidentRuntimeService(
        runtime_dir=tmp_path,
        active_pid="PID-704",
        persona_ready=persona_ready,
        environment_factory=lambda: (object(), object()),
        runtime_factory=lambda *_args: runtime,
    )


def test_spec704_resident_service_owns_one_lock_and_reports_host(tmp_path):
    first = service(tmp_path / "runtime")
    second = service(tmp_path / "runtime")
    first.start(host_address="127.0.0.1", port=8770)
    try:
        with pytest.raises(RuntimeAlreadyRunning) as exc:
            second.start(host_address="127.0.0.1", port=9000)
        assert exc.value.host == {"address": "127.0.0.1", "port": 8770}
    finally:
        first.close()


def test_spec743_periodic_memory_mutation_pauses_heartbeat_and_rechecks_idle(
        tmp_path, monkeypatch):
    runtime = FakeRuntime()
    runtime.memory_store = object()
    runtime.heat = object()
    runtime.assembler = object()
    runtime.cfg = object()
    calls = []

    class Processor:
        def __init__(self, **kwargs):
            calls.append(("init", kwargs))

        def apply(self, action, mem_id):
            assert runtime.hb._paused is True
            calls.append(("apply", action, mem_id))
            return {
                "schema_version": "periodic_memory_mount_receipt.v2",
                "tool_id": "periodic_memory_mount",
                "status": "applied",
                "action": action,
                "mem_id": mem_id,
            }

    monkeypatch.setattr(
        "logic.periodic_memory_mount.PeriodicMemoryMountProcessor", Processor)
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    try:
        result = resident.mutate_periodic_memory("mount", "MEM-74300001")
        assert result["schema_version"] == "seed_gui_periodic_memory_result.v1"
        assert result["submission_source"] == "seed_gui"
        assert calls[-1] == ("apply", "mount", "MEM-74300001")
        assert runtime.hb._paused is False
        assert resident.status()["operation_in_flight"] is False

        runtime.control.establish_round("interactive", lambda: 1)
        with pytest.raises(RuntimeServiceError, match="round_in_flight"):
            resident.mutate_periodic_memory("mount", "MEM-74300001")
        runtime.control.finish_round(False)
    finally:
        resident.close()


def test_spec743_periodic_mutation_reservation_blocks_late_round_start(
        tmp_path, monkeypatch):
    runtime = FakeRuntime()
    runtime.memory_store = object()
    runtime.heat = object()
    runtime.assembler = object()
    runtime.cfg = object()
    processor_entered = threading.Event()
    release_processor = threading.Event()
    preparation_entered = threading.Event()
    round_established = threading.Event()
    outcomes = {}

    class Processor:
        def __init__(self, **_kwargs):
            pass

        def apply(self, action, mem_id):
            processor_entered.set()
            assert runtime.control.snapshot(runtime.hb)["round_in_flight"] is False
            assert release_processor.wait(2)
            return {
                "schema_version": "periodic_memory_mount_receipt.v2",
                "tool_id": "periodic_memory_mount",
                "status": "applied",
                "action": action,
                "mem_id": mem_id,
            }

    monkeypatch.setattr(
        "logic.periodic_memory_mount.PeriodicMemoryMountProcessor", Processor)
    resident = service(tmp_path / "runtime", runtime)
    resident.start()

    def mutate():
        try:
            outcomes["mutation"] = resident.mutate_periodic_memory(
                "mount", "MEM-74300001")
        except Exception as exc:  # surfaced below
            outcomes["mutation_error"] = exc

    def start_round():
        try:
            outcomes["prepared"] = runtime.control.begin_round_preparation()
            preparation_entered.set()
            outcomes["round_num"] = runtime.control.establish_round(
                "interactive", lambda: 743)
            round_established.set()
        except Exception as exc:  # surfaced below
            outcomes["round_error"] = exc

    mutation_thread = threading.Thread(target=mutate)
    round_thread = threading.Thread(target=start_round)
    try:
        mutation_thread.start()
        assert processor_entered.wait(2)
        round_thread.start()
        assert preparation_entered.wait(0.1) is False
        assert round_established.is_set() is False
        assert runtime.control.snapshot(runtime.hb)["round_in_flight"] is False

        release_processor.set()
        mutation_thread.join(timeout=2)
        round_thread.join(timeout=2)

        assert mutation_thread.is_alive() is False
        assert round_thread.is_alive() is False
        assert "mutation_error" not in outcomes
        assert "round_error" not in outcomes
        assert outcomes["prepared"] is True
        assert outcomes["round_num"] == 743
        assert outcomes["mutation"]["receipt"]["status"] == "applied"
        runtime.control.finish_round(False)
    finally:
        release_processor.set()
        mutation_thread.join(timeout=2)
        round_thread.join(timeout=2)
        resident.close()


def test_spec743_round_preparation_rejects_periodic_mutation_without_heartbeat_side_effect(
        tmp_path, monkeypatch):
    runtime = FakeRuntime()
    runtime.memory_store = object()
    runtime.heat = object()
    runtime.assembler = object()
    runtime.cfg = object()
    applied = []

    class Processor:
        def __init__(self, **_kwargs):
            pass

        def apply(self, *_args):
            applied.append(True)

    monkeypatch.setattr(
        "logic.periodic_memory_mount.PeriodicMemoryMountProcessor", Processor)
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    try:
        assert runtime.control.begin_round_preparation() is True
        with pytest.raises(RuntimeServiceError, match="round_in_flight"):
            resident.mutate_periodic_memory("mount", "MEM-74300001")
        assert applied == []
        assert runtime.hb.pause_calls == 0
        assert runtime.hb.resume_calls == 0
        assert resident.status()["operation_in_flight"] is False
    finally:
        runtime.control.end_round_preparation()
        resident.close()


def test_spec743_periodic_mutation_failure_releases_idle_reservation(
        tmp_path, monkeypatch):
    runtime = FakeRuntime()
    runtime.memory_store = object()
    runtime.heat = object()
    runtime.assembler = object()
    runtime.cfg = object()

    class Processor:
        def __init__(self, **_kwargs):
            pass

        def apply(self, *_args):
            raise RuntimeError("periodic failure")

    monkeypatch.setattr(
        "logic.periodic_memory_mount.PeriodicMemoryMountProcessor", Processor)
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    try:
        with pytest.raises(RuntimeError, match="periodic failure"):
            resident.mutate_periodic_memory("mount", "MEM-74300001")
        assert runtime.hb._paused is False
        assert resident.status()["operation_in_flight"] is False
        assert runtime.control.reserve_idle_mutation() is True
        runtime.control.release_idle_mutation()
    finally:
        resident.close()


def test_spec743_instance_switch_reserves_idle_runtime_until_cancelled(tmp_path):
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)
    preparation_started = threading.Event()
    outcome = {}
    resident.start()

    def prepare_round():
        outcome["prepared"] = runtime.control.begin_round_preparation()
        preparation_started.set()

    thread = threading.Thread(target=prepare_round)
    try:
        resident.prepare_instance_switch()
        assert runtime.hb._paused is True
        assert runtime.hb.pause_calls == 1
        assert resident.status()["operation_in_flight"] is True

        thread.start()
        assert preparation_started.wait(0.1) is False
        assert runtime.control.snapshot(runtime.hb)["stage"] == "idle"

        resident.cancel_instance_switch()
        assert preparation_started.wait(2)
        thread.join(timeout=2)
        assert thread.is_alive() is False
        assert outcome == {"prepared": True}
        assert runtime.hb._paused is False
        assert runtime.hb.resume_calls == 1
        assert resident.status()["operation_in_flight"] is False
    finally:
        resident.cancel_instance_switch()
        runtime.control.end_round_preparation()
        thread.join(timeout=2)
        resident.close()


def test_spec743_instance_switch_preserves_existing_heartbeat_pause(tmp_path):
    runtime = FakeRuntime()
    runtime.hb._paused = True
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    try:
        resident.prepare_instance_switch()
        resident.cancel_instance_switch()

        assert runtime.hb._paused is True
        assert runtime.hb.pause_calls == 1
        assert runtime.hb.resume_calls == 0
        assert resident.status()["operation_in_flight"] is False
    finally:
        resident.cancel_instance_switch()
        resident.close()


def test_spec743_round_preparation_rejects_instance_switch_without_pause(tmp_path):
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    try:
        assert runtime.control.begin_round_preparation() is True
        with pytest.raises(RuntimeServiceError, match="round_in_flight"):
            resident.prepare_instance_switch()

        assert runtime.hb.pause_calls == 0
        assert runtime.hb.resume_calls == 0
        assert resident.status()["operation_in_flight"] is False
    finally:
        runtime.control.end_round_preparation()
        resident.close()


def test_spec743_shutdown_wakes_round_waiting_on_idle_mutation_reservation():
    runtime = FakeRuntime()
    finished = threading.Event()
    outcome = {}
    assert runtime.control.reserve_idle_mutation() is True

    def prepare_round():
        outcome["prepared"] = runtime.control.begin_round_preparation()
        finished.set()

    thread = threading.Thread(target=prepare_round)
    thread.start()
    try:
        assert finished.wait(0.1) is False
        runtime.control.request_shutdown(runtime)
        assert finished.wait(2)
        thread.join(timeout=2)
        assert outcome == {"prepared": False}
    finally:
        runtime.control.release_idle_mutation()
        thread.join(timeout=2)


def test_spec732_global_lock_blocks_a_second_persona_instance(tmp_path):
    lock_path = tmp_path / "runtime.lock"
    first = ResidentRuntimeService(
        runtime_dir=tmp_path / "PID-A" / "meta",
        lock_path=lock_path,
        active_pid="PID-A",
        active_instance_id="meta",
        persona_ready=lambda: True,
        environment_factory=lambda: (object(), object()),
        runtime_factory=lambda *_args: FakeRuntime(),
    )
    second = ResidentRuntimeService(
        runtime_dir=tmp_path / "PID-B" / "meta",
        lock_path=lock_path,
        active_pid="PID-B",
        active_instance_id="meta",
        persona_ready=lambda: True,
        environment_factory=lambda: (object(), object()),
        runtime_factory=lambda *_args: FakeRuntime(),
    )
    first.start()
    try:
        with pytest.raises(RuntimeAlreadyRunning):
            second.start()
    finally:
        first.close()


def test_spec731_retention_runs_after_lock_and_before_runtime_factory(tmp_path):
    order = []

    def retain():
        order.append("retention")
        return {"status": "ok"}

    def environment():
        order.append("environment")
        return object(), object()

    resident = ResidentRuntimeService(
        runtime_dir=tmp_path / "runtime",
        active_pid="PID-731",
        persona_ready=lambda: True,
        environment_factory=environment,
        runtime_factory=lambda *_args: FakeRuntime(),
        retention_enforcer=retain,
    )
    resident.start()
    try:
        assert order == ["retention", "environment"]
        assert resident.retention_receipt == {"status": "ok"}
    finally:
        resident.close()


def test_spec735_ltm_reconcile_runs_before_environment_and_fail_closed(tmp_path):
    order = []
    lock_path = tmp_path / "runtime.lock"
    resident = ResidentRuntimeService(
        runtime_dir=tmp_path / "first",
        lock_path=lock_path,
        active_pid="PID-735",
        persona_ready=lambda: True,
        retention_enforcer=lambda: order.append("retention") or {"status": "ok"},
        ltm_reconciler=lambda: order.append("ltm") or {"status": "ok"},
        environment_factory=lambda: order.append("environment") or (object(), object()),
        runtime_factory=lambda *_args: FakeRuntime(),
    )
    resident.start()
    try:
        assert order == ["retention", "ltm", "environment"]
    finally:
        resident.close()

    failing = ResidentRuntimeService(
        runtime_dir=tmp_path / "failing",
        lock_path=lock_path,
        active_pid="PID-735",
        persona_ready=lambda: True,
        ltm_reconciler=lambda: (_ for _ in ()).throw(ValueError("duplicate")),
        environment_factory=lambda: pytest.fail("runtime must not start"),
        runtime_factory=lambda *_args: FakeRuntime(),
    )
    with pytest.raises(RuntimeServiceError, match="ltm_projection_failed:duplicate"):
        failing.start()

    replacement = ResidentRuntimeService(
        runtime_dir=tmp_path / "replacement",
        lock_path=lock_path,
        active_pid="PID-735",
        persona_ready=lambda: True,
        environment_factory=lambda: (object(), object()),
        runtime_factory=lambda *_args: FakeRuntime(),
    )
    replacement.start()
    replacement.close()


def test_spec731_retention_failure_releases_lock_before_runtime_start(tmp_path):
    def fail():
        raise RuntimeServiceError("round_snapshot_delete_failed")

    runtime_dir = tmp_path / "runtime"
    failing = ResidentRuntimeService(
        runtime_dir=runtime_dir,
        active_pid="PID-731",
        persona_ready=lambda: True,
        environment_factory=lambda: pytest.fail("runtime must not start"),
        retention_enforcer=fail,
    )
    with pytest.raises(RuntimeServiceError, match="round_snapshot_delete_failed"):
        failing.start()

    replacement = service(runtime_dir)
    replacement.start()
    replacement.close()


def test_spec731_uninitialized_persona_defers_retention_until_ready(tmp_path):
    ready = False
    order = []

    resident = ResidentRuntimeService(
        runtime_dir=tmp_path / "runtime",
        active_pid="PID-731",
        persona_ready=lambda: ready,
        environment_factory=lambda: (order.append("environment") or (object(), object())),
        runtime_factory=lambda *_args: FakeRuntime(),
        retention_enforcer=lambda: order.append("retention") or {"status": "ok"},
    )
    resident.start()
    try:
        assert order == []
        ready = True
        assert resident.start_if_ready() is True
        assert order == ["retention", "environment"]
    finally:
        resident.close()


def test_spec721_resident_restart_falls_back_to_guarded(tmp_path):
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)

    resident.start()
    try:
        assert runtime.execution_permission_level == "guarded"
    finally:
        resident.close()


def test_spec729_resident_status_projects_small_runtime_truth(tmp_path):
    class State:
        @staticmethod
        def load():
            return {
                "base": {
                    "meta": {"total_round": 638},
                    "heartbeat_flags": {"user_message_waiting": True},
                    "runtime": {"phase": "main"},
                }
            }

    class Workbench:
        @staticmethod
        def active_guide_slots():
            return {"rhythm": "", "work": "task:T-729"}

        @staticmethod
        def get(key):
            return "T-729" if key == "base.active_task" else None

    runtime = FakeRuntime()
    runtime.sm = State()
    runtime.workbench = Workbench()
    resident = service(tmp_path / "runtime", runtime)

    resident.start()
    try:
        data = resident.status()["cli_data"]
        assert data["total_round"] == 638
        assert data["active_flags"] == ["user_message_waiting"]
        assert data["round_type"] == "interactive"
        assert data["phase"] == "main"
        assert data["active_guides"] == {
            "rhythm": "",
            "work": "task:T-729",
        }
        assert data["active_task"] == "T-729"
    finally:
        resident.close()


def test_spec704_corrupt_supervisor_is_preserved_and_fails_closed(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    supervisor = runtime_dir / "supervisor.json"
    original = b"{not-json"
    supervisor.write_bytes(original)

    with pytest.raises(RuntimeSupervisorCorrupt):
        service(runtime_dir).start()

    assert supervisor.read_bytes() == original


def test_spec704_startup_preserves_recovery_anchor_when_factory_fails(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    supervisor = runtime_dir / "supervisor.json"
    previous = {
        "schema_version": "upsp_runtime_supervisor.v2",
        "state": "running",
        "process_id": 111,
        "session_id": "old-session",
        "active_pid": "PID-704",
        "active_instance_id": "meta",
        "host": {"address": "127.0.0.1", "port": 8770},
        "current_round": 704,
        "round_type": "interactive",
        "phase": "reaction",
        "stop_requested": False,
        "heartbeat_suspended": False,
        "last_outcome": {},
        "updated_at": "2026-07-24T00:00:00+08:00",
    }
    supervisor.write_text(
        json.dumps(previous, ensure_ascii=False),
        encoding="utf-8",
    )
    original = supervisor.read_bytes()
    resident = ResidentRuntimeService(
        runtime_dir=runtime_dir,
        active_pid="PID-704",
        persona_ready=lambda: True,
        environment_factory=lambda: (_ for _ in ()).throw(
            RuntimeError("factory failed")),
        runtime_factory=lambda *_args: FakeRuntime(),
    )

    with pytest.raises(RuntimeError, match="factory failed"):
        resident.start()

    assert supervisor.read_bytes() == original


def test_spec705_restart_restores_latch_after_unsettled_round(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "supervisor.json").write_text(json.dumps({
        "schema_version": "upsp_runtime_supervisor.v2",
        "state": "stopped",
        "process_id": 111,
        "session_id": "old-session",
        "active_pid": "PID-704",
        "active_instance_id": "meta",
        "host": {"address": "127.0.0.1", "port": 8770},
        "current_round": None,
        "round_type": None,
        "phase": "idle",
        "stop_requested": False,
        "heartbeat_suspended": False,
        "last_outcome": {
            "status": "round_completed",
            "round_num": 704,
            "settlement_status": "unsettled",
        },
        "updated_at": "2026-07-24T00:00:00+08:00",
    }), encoding="utf-8")
    runtime = FakeRuntime()
    resident = service(runtime_dir, runtime)

    resident.start()
    try:
        assert runtime.control.stop_latched is True
        assert runtime.hb._paused is True
        assert resident.status()["runtime"]["heartbeat_suspended"] is True
    finally:
        resident.close()


def test_spec704_service_send_and_stop_receipts_are_resident(tmp_path):
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    try:
        result = resident.submit_message("hello", "limited")
        assert result["status"] == "round_completed"
        assert result["final_response"] == "reply:hello"
        assert result["classification"] == ""
        assert result["final_response_source"] == ""
        assert "final_response" not in resident.status()["last_outcome"]

        blocked = resident.submit_message("blocked", "limited")
        assert blocked["classification"] == "runtime_blocked_closed"
        assert blocked["reason"] == "final_response_length_exhausted"
        assert blocked["final_response_source"] == (
            "reaction.runtime_auto_blocked_final_reply")
        assert "hello" not in resident.supervisor_path.read_text(encoding="utf-8")

        resident.submit_message(
            "query", "limited", final_response_max_chars=128)
        assert runtime.final_response_max_chars == 128

        resident.submit_message(
            "format", "limited", response_contract={
                "language": "en",
                "answer_scope": "conclusion_only",
                "max_sentences": 1,
            })
        assert runtime.response_contract["max_sentences"] == 1

        resident.submit_message(
            "direct", "limited", task_guidance_enabled=False)
        assert runtime.task_guidance_enabled is False

        runtime.control.establish_round("interactive", lambda: 2)
        runtime.control.set_stage("reaction")
        first = resident.stop_round()
        second = resident.stop_round()
        assert first["schema_version"] == "seed_gui_runtime_stop_receipt.v1"
        assert first["reason"] == "stop_requested"
        assert second["reason"] == "stop_already_requested"
        assert runtime.executor.cancelled == 2
        supervisor = json.loads(
            resident.supervisor_path.read_text(encoding="utf-8"))
        assert supervisor["phase"] == "reaction"
        assert supervisor["stop_requested"] is True
    finally:
        runtime.control.finish_round(True)
        resident.close()


def test_spec704_runtime_error_callback_keeps_round_admission_closed():
    runtime = FakeRuntime()
    observed = []
    runtime.on_round_finished = lambda *_args: observed.append((
        "ledger", runtime.control.snapshot(runtime.hb)["round_in_flight"]))
    runtime.on_round_released = lambda *_args: observed.append((
        "released", runtime.control.snapshot(runtime.hb)["round_in_flight"]))
    runtime.control.establish_round("interactive", lambda: 704)

    runtime.control.handle_round_error(
        runtime, object(), RuntimeError("failed"))

    assert observed == [("ledger", True), ("released", False)]
    assert runtime.control.snapshot(runtime.hb)["round_in_flight"] is False
    assert runtime.control.stop_latched is True
    assert runtime.hb._paused is True


def test_spec778_resident_operation_is_signalled_only_after_runtime_release(
        tmp_path):
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    operation = resident._begin_operation("send", "limited")
    try:
        runtime.control.establish_round("interactive", lambda: 778)
        result = {"response": "done", "_settlement": {"status": "settled"}}

        resident._on_round_finished(778, "interactive", result)

        assert operation["event"].is_set() is False
        assert runtime.runtime_status()["round_in_flight"] is True

        runtime.control.finish_round(False)
        resident._on_round_released(778, "interactive", result)

        assert operation["event"].is_set() is True
        assert operation["result"]["round_num"] == 778
        assert runtime.runtime_status()["round_in_flight"] is False
    finally:
        resident._end_operation(operation)
        resident.close()


def test_spec788_resident_counts_only_closed_rounds_for_action_retention(tmp_path):
    runtime = FakeRuntime()
    observed = []

    class ActionStore:
        def note_round_closed(self, round_num):
            observed.append(round_num)

    runtime.action_recovery_store = ActionStore()
    resident = service(tmp_path / "runtime", runtime)
    resident.runtime = runtime
    try:
        resident._on_round_finished(
            788, "interactive", {"_settlement": {"status": "settled"}})
        resident._on_round_finished(
            789, "interactive", {"_settlement": {"status": "unsettled"}})

        assert observed == [788]
    finally:
        resident.close()


def test_spec789_action_retention_failure_retries_only_proven_closed_rounds(
        tmp_path):
    runtime = FakeRuntime()
    observed = []

    class ActionStore:
        def note_round_closed(self, round_num):
            observed.append(round_num)
            if observed == [788]:
                raise OSError("journal temporarily unavailable")

    runtime.action_recovery_store = ActionStore()
    resident = service(tmp_path / "runtime", runtime)
    resident.runtime = runtime
    try:
        resident._on_round_finished(
            788, "interactive", {"_settlement": {"status": "settled"}})
        assert resident.last_outcome[
            "action_recovery_retention_pending_rounds"] == [788]

        resident._on_round_finished(
            789, "interactive", {"_settlement": {"status": "unsettled"}})
        assert observed == [788]
        assert resident.last_outcome[
            "action_recovery_retention_pending_rounds"] == [788]

        resident._on_round_finished(
            790, "interactive", {"_settlement": {"status": "degraded"}})
        assert observed == [788, 788, 790]
        assert "action_recovery_retention_pending_rounds" not in (
            resident.last_outcome)
        assert "action_recovery_retention_error" not in resident.last_outcome
    finally:
        resident.close()


def test_spec789_startup_retries_persisted_action_retention_round(tmp_path):
    runtime_dir = tmp_path / "runtime"
    runtime_dir.mkdir()
    (runtime_dir / "supervisor.json").write_text(json.dumps({
        "schema_version": "upsp_runtime_supervisor.v2",
        "state": "stopped",
        "process_id": 111,
        "session_id": "old-session",
        "active_pid": "PID-704",
        "active_instance_id": "meta",
        "host": {"address": "127.0.0.1", "port": 8770},
        "current_round": None,
        "round_type": None,
        "phase": "idle",
        "stop_requested": False,
        "heartbeat_suspended": False,
        "last_outcome": {
            "action_recovery_retention_pending_rounds": [788],
        },
        "updated_at": "2026-07-24T00:00:00+08:00",
    }), encoding="utf-8")
    runtime = FakeRuntime()
    observed = []

    class ActionStore:
        def note_round_closed(self, round_num):
            observed.append(round_num)

        @staticmethod
        def summary():
            return {"pending": False, "round": None}

    runtime.action_recovery_store = ActionStore()
    resident = service(runtime_dir, runtime)
    resident.start()
    try:
        assert observed == [788]
        supervisor = json.loads(
            resident.supervisor_path.read_text(encoding="utf-8"))
        assert "action_recovery_retention_pending_rounds" not in (
            supervisor["last_outcome"])
    finally:
        resident.close()


def test_spec704_stop_during_local_cleanup_is_idempotent_and_does_not_kill_io(
        tmp_path):
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    try:
        runtime.control.establish_round("interactive", lambda: 3)
        runtime.control.set_stage("cleanup_local")
        receipt = resident.stop_round()
        assert receipt["reason"] == "local_cleanup_in_progress"
        assert receipt["stage"] == "cleanup_local"
        assert runtime.executor.cancelled == 0
    finally:
        runtime.control.finish_round(True)
        resident.close()


def test_spec704_stop_before_round_finishes_pending_send_without_round(tmp_path):
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    operation = resident._begin_operation("send", "limited")
    try:
        receipt = resident.stop_round()
        assert receipt["reason"] == "pending_input_cancelled"
        assert operation["event"].wait(1)
        assert operation["result"]["status"] == "round_stopped"
        assert operation["result"]["round_num"] is None
        assert runtime.control.stop_latched is True
        assert runtime.hb._paused is True
    finally:
        resident._end_operation(operation)
        resident.close()


def test_spec704_service_without_persona_stays_initialization_only(tmp_path):
    resident = service(tmp_path / "runtime", persona_ready=lambda: False)
    resident.start()
    try:
        assert resident.status()["runtime"]["stage"] == "initialization"
        with pytest.raises(RuntimeServiceError, match="no_round_in_flight"):
            resident.stop_round()
    finally:
        resident.close()


def test_spec704_crash_recovery_marks_one_unsettled_without_provider(
        tmp_path, monkeypatch):
    store = FakeAuditStore([{"event_type": "round_started"}])
    calls = []

    class State:
        def clear_flags(self, flags):
            calls.append(("clear", tuple(flags)))

        def set_phase(self, phase):
            calls.append(("phase", phase))

        def load(self):
            return {"base": {}}

    class Backups:
        def __init__(self):
            self.rows = []

        def read_backups(self):
            return list(self.rows)

        def append_backup(self, round_num, state, reason):
            calls.append(("backup", round_num, state, reason))
            self.rows.append({
                "round": round_num,
                "state": state,
                "reason": reason,
            })

    runtime = FakeRuntime()
    runtime.audit = FakeAudit(store)
    runtime.sm = State()
    runtime.state_backup_store = Backups()
    monkeypatch.setattr(
        "engines.resident_runtime.settle_open_relay_intents",
        lambda *_args, **kwargs: calls.append(("relay", kwargs)),
    )
    resident = service(tmp_path / "runtime", runtime)
    previous = {
        "state": "running",
        "current_round": 704,
        "process_id": 111,
        "session_id": "old-session",
    }

    resident._recover_interrupted_round(runtime, previous)
    resident._recover_interrupted_round(runtime, previous)

    event_types = [item["event_type"] for item in store.events]
    assert event_types.count("runtime_process_interrupted") == 1
    assert event_types.count("round_unsettled") == 1
    assert ("clear", ("user_message_waiting", "continue_requested")) in calls
    assert any(item[0] == "relay" and item[1]["status"] == "blocked" for item in calls)
    assert len([item for item in calls if item[0] == "backup"]) == 1
    assert runtime.control.stop_latched is True
    assert runtime.hb._paused is True
    assert runtime.executor.cancelled == 0


def test_spec788_crash_recovery_classifies_actions_before_unsettled_closeout(
        tmp_path, monkeypatch):
    from data.action_recovery_store import ActionRecoveryStore

    store = FakeAuditStore([{
        "event_type": "round_started",
        "payload": {"input_snapshot": {"trigger": {"message": "继续"}}},
    }])
    action_store = ActionRecoveryStore(
        tmp_path / "context" / "action_recovery_pending.json",
    )
    action_store.prepare_opaque(
        tool_id="shell_command",
        request_sha256="8" * 64,
        runtime_context={
            "round_num": 704,
            "iteration": 2,
            "frame_id": "R000704:reaction:2",
        },
        call_id="call-shell",
        target="workspace",
    )

    class State:
        def clear_flags(self, _flags):
            return None

        def set_phase(self, _phase):
            return None

        def load(self):
            return {"base": {}}

    class Backups:
        def __init__(self):
            self.rows = []

        def read_backups(self):
            return list(self.rows)

        def append_backup(self, round_num, state, reason):
            self.rows.append({
                "round": round_num,
                "state": state,
                "reason": reason,
            })

    runtime = FakeRuntime()
    runtime.audit = FakeAudit(store)
    runtime.sm = State()
    runtime.state_backup_store = Backups()
    runtime.action_recovery_store = action_store
    monkeypatch.setattr(
        "engines.resident_runtime.settle_open_relay_intents",
        lambda *_args, **_kwargs: None,
    )

    resident = service(tmp_path / "runtime", runtime)
    previous = {
        "state": "running",
        "current_round": 704,
        "process_id": 111,
        "session_id": "old-session",
    }
    resident._recover_interrupted_round(runtime, previous)
    resident._recover_interrupted_round(runtime, previous)

    event_types = [item["event_type"] for item in store.events]
    assert event_types.index("action_recovery_classified") < event_types.index(
        "runtime_process_interrupted")
    assert action_store.summary()["outcome_unknown"] == 1
    assert runtime.control.stop_latched is True
    assert runtime.executor.cancelled == 0


def test_spec789_closed_round_recovery_finishes_action_retention(tmp_path):
    from data.action_recovery_store import ActionRecoveryStore

    store = FakeAuditStore([
        {"event_type": "round_started"},
        {"event_type": "round_closed"},
    ])
    action_store = ActionRecoveryStore(
        tmp_path / "context" / "action_recovery_pending.json",
    )
    action_id = action_store.prepare_opaque(
        tool_id="shell_command",
        request_sha256="9" * 64,
        runtime_context={
            "round_num": 704,
            "iteration": 2,
            "frame_id": "R000704:reaction:2",
        },
        call_id="call-shell",
        target="workspace",
    )
    result = {"action_id": action_id, "status": "ok", "reason": ""}
    action_store.record_results([result])
    action_store.settle_results([result])

    runtime = FakeRuntime()
    runtime.audit = FakeAudit(store)
    runtime.action_recovery_store = action_store
    resident = service(tmp_path / "runtime", runtime)
    resident._recover_interrupted_round(runtime, {
        "state": "running",
        "current_round": 704,
        "process_id": 111,
        "session_id": "old-session",
    })

    assert action_store.load()["items"] == []
    assert [item["event_type"] for item in store.events] == [
        "round_started", "round_closed"]


def test_spec788_classified_interruption_without_actions_still_repairs_local_recovery(
        tmp_path, monkeypatch):
    store = FakeAuditStore([
        {
            "event_type": "action_recovery_classified",
            "payload": {"pending": False, "round": 704},
        },
        {"event_type": "runtime_process_interrupted"},
        {"event_type": "round_unsettled"},
    ])
    calls = []

    class State:
        def clear_flags(self, flags):
            calls.append(("clear", tuple(flags)))

        def set_phase(self, phase):
            calls.append(("phase", phase))

        def load(self):
            return {"base": {}}

    class Backups:
        def read_backups(self):
            return []

        def append_backup(self, round_num, state, reason):
            calls.append(("backup", round_num, state, reason))

    runtime = FakeRuntime()
    runtime.audit = FakeAudit(store)
    runtime.sm = State()
    runtime.state_backup_store = Backups()
    runtime.action_recovery_store = type("NoActionRecovery", (), {
        "summary": staticmethod(lambda: {"pending": False, "round": None}),
        "recovery_receipt": staticmethod(lambda: None),
    })()
    monkeypatch.setattr(
        "engines.resident_runtime.settle_open_relay_intents",
        lambda *_args, **kwargs: calls.append(("relay", kwargs)),
    )

    service(tmp_path / "runtime", runtime)._recover_interrupted_round(runtime, {
        "state": "running",
        "current_round": 704,
        "process_id": 111,
        "session_id": "old-session",
    })

    assert ("clear", ("user_message_waiting", "continue_requested")) in calls
    assert any(item[0] == "backup" for item in calls)
    assert runtime.control.stop_latched is True


def test_spec704_close_keeps_instance_lock_until_runtime_thread_exits(tmp_path):
    class StubbornRuntime(FakeRuntime):
        def request_shutdown(self):
            return None

    runtime_dir = tmp_path / "runtime"
    stubborn = StubbornRuntime()
    first = service(runtime_dir, stubborn)
    second = service(runtime_dir)
    first.start()
    first.close(wait_seconds=0)
    with pytest.raises(RuntimeAlreadyRunning):
        second.start()

    stubborn._stopped.set()
    first.runtime_thread.join(timeout=2)
    assert not first.runtime_thread.is_alive()

    deadline = time.monotonic() + 2
    while True:
        try:
            second.start()
            break
        except RuntimeAlreadyRunning:
            if time.monotonic() >= deadline:
                raise
            time.sleep(0.01)
    second.close()
