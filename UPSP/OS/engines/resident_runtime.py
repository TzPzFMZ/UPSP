"""One managed Runtime instance for the local GUI host."""
from __future__ import annotations

from datetime import datetime
import json
import os
from pathlib import Path
import threading
import uuid

from constants import local_now
from data.atomic_write import atomic_write_json
from logic.relay_intent_pool import settle_open_relay_intents
from paths import ACTIVE_PID, PERSONA_DIR, UPSP_LOCAL_STATE_ROOT

try:
    import msvcrt
except ImportError:  # pragma: no cover - Windows product only
    msvcrt = None


SUPERVISOR_SCHEMA = "upsp_runtime_supervisor.v1"


class RuntimeServiceError(RuntimeError):
    pass


class RuntimeAlreadyRunning(RuntimeServiceError):
    def __init__(self, host=None):
        super().__init__("runtime_already_running")
        self.host = dict(host or {})


class RuntimeSupervisorCorrupt(RuntimeServiceError):
    pass


class _InstanceLock:
    def __init__(self, path):
        self.path = Path(path)
        self.handle = None
        self._guard = threading.Lock()

    def acquire(self):
        with self._guard:
            if msvcrt is None:
                raise RuntimeServiceError("windows_runtime_lock_unavailable")
            self.path.parent.mkdir(parents=True, exist_ok=True)
            self.handle = self.path.open("a+b")
            if self.handle.tell() == 0:
                self.handle.write(b"\0")
                self.handle.flush()
            self.handle.seek(0)
            try:
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError:
                self.handle.close()
                self.handle = None
                return False
            return True

    def release(self):
        with self._guard:
            if self.handle is None:
                return
            try:
                self.handle.seek(0)
                msvcrt.locking(self.handle.fileno(), msvcrt.LK_UNLCK, 1)
            finally:
                self.handle.close()
                self.handle = None


class ResidentRuntimeService:
    def __init__(
            self, *, runtime_dir=None, active_pid=ACTIVE_PID,
            persona_ready=None, environment_factory=None, runtime_factory=None,
            default_permission_level=None):
        self.active_pid = str(active_pid)
        self.runtime_dir = Path(
            runtime_dir
            or Path(UPSP_LOCAL_STATE_ROOT) / "runtime" / self.active_pid
        )
        self.supervisor_path = self.runtime_dir / "supervisor.json"
        self.instance_lock = _InstanceLock(self.runtime_dir / "runtime.lock")
        self.persona_ready = persona_ready or (
            lambda: (
                (Path(PERSONA_DIR) / "core.md").is_file()
                and (Path(PERSONA_DIR) / "state.json").is_file()
            )
        )
        self.environment_factory = environment_factory
        self.runtime_factory = runtime_factory
        self.default_permission_level = default_permission_level
        self.runtime = None
        self.runtime_thread = None
        self.session_id = uuid.uuid4().hex
        self.host = {"address": "127.0.0.1", "port": 0}
        self._lock = threading.Lock()
        self._supervisor_lock = threading.Lock()
        self._operation = None
        self._started = False
        self._closed = False
        self._previous_supervisor = None
        self._recovering = False
        self.supervisor_state = "new"
        self.last_outcome = {}

    def start(self, *, host_address="127.0.0.1", port=0):
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        previous = self._read_supervisor()
        recovery_pending = self._recovery_round(previous) is not None
        if not self.instance_lock.acquire():
            host = (previous or {}).get("host") or {}
            raise RuntimeAlreadyRunning(host)
        try:
            self._previous_supervisor = previous
            self.host = {"address": str(host_address), "port": int(port)}
            self._started = True
            if not recovery_pending:
                self._write_supervisor("initializing")
            ready = self.start_if_ready()
            if recovery_pending and not ready:
                raise RuntimeServiceError("runtime_recovery_persona_unavailable")
            if self.runtime is None:
                self._write_supervisor("awaiting_persona")
        except Exception:
            self.instance_lock.release()
            self._started = False
            raise
        return self.status()

    def start_if_ready(self):
        with self._lock:
            if self.runtime is not None:
                return True
            if not self.persona_ready():
                return False
            if self.environment_factory is None or self.runtime_factory is None:
                from main import build_runtime, init_environment
                self.environment_factory = self.environment_factory or init_environment
                self.runtime_factory = self.runtime_factory or build_runtime
            sm, cfg = self.environment_factory()
            runtime = self.runtime_factory(sm, cfg)
            runtime.on_round_started = self._on_round_started
            runtime.on_round_finished = self._on_round_finished
            runtime.control.on_change = self._on_runtime_state_changed
            self.runtime = runtime
            self._recovering = True
            try:
                self._recover_interrupted_round(
                    runtime, self._previous_supervisor)
                if self._previous_requires_explicit_release(
                        self._previous_supervisor):
                    runtime.control.latch_until_explicit(runtime)
            except Exception:
                self.runtime = None
                raise
            finally:
                self._recovering = False
            self.runtime_thread = threading.Thread(
                target=self._run_runtime,
                name=f"upsp-runtime-{self.active_pid}",
                daemon=True,
            )
            set_permission = getattr(
                runtime, "set_execution_permission_level", None)
            if getattr(runtime, "permission_chain", None) is not None:
                runtime.permission_chain.apply("guarded")
            elif callable(set_permission):
                set_permission("guarded")
            self.runtime_thread.start()
            self._write_supervisor("running")
            return True

    def _run_runtime(self):
        try:
            self.runtime.run_forever()
        except BaseException as exc:
            self.last_outcome = {
                "kind": "runtime_process_failed",
                "error_type": type(exc).__name__,
                "recorded_at": self._now(),
            }
            self._write_supervisor("crashed")
            self._finish_operation({
                "status": "runtime_failed",
                "error": type(exc).__name__,
            })
        else:
            if not self._closed:
                self._write_supervisor("stopped")
        finally:
            if self._closed:
                self.instance_lock.release()

    def submit_message(self, message, permission_level, *, timeout=None):
        runtime = self._require_runtime()
        operation = self._begin_operation("send", permission_level)
        try:
            if not runtime.submit_message(
                    message, execution_permission_level=permission_level):
                raise RuntimeServiceError("round_in_flight")
            if not operation["event"].wait(timeout):
                raise RuntimeServiceError("runtime_wait_timeout")
            result = dict(operation["result"] or {})
            if result.get("status") == "runtime_failed":
                raise RuntimeServiceError(
                    str(result.get("error") or "runtime_process_failed"))
            return result
        finally:
            self._end_operation(operation)

    def submit_pending(self, kind, permission_level, *, timeout=None):
        if kind not in {"relay", "tick"}:
            raise ValueError("invalid_pending_kind")
        runtime = self._require_runtime()
        operation = self._begin_operation(kind, permission_level)
        try:
            runtime.release_stop_latch()
            flags = runtime.sm.get_flags()
            decision = runtime._determine_round_decision(flags)
            round_type = decision.get("round_type")
            if not round_type or (kind == "relay" and round_type != "relay"):
                raise RuntimeServiceError(
                    "relay_not_pending" if kind == "relay" else "tick_not_pending"
                )
            authorize = getattr(runtime, "authorize_pending_execution", None)
            if getattr(runtime, "permission_chain", None) is not None:
                runtime.permission_chain.authorize(permission_level)
            elif callable(authorize):
                authorize(permission_level)
            runtime.hb.resume()
            wake = getattr(runtime.hb, "wake", None)
            if callable(wake):
                wake()
            if not operation["event"].wait(timeout):
                raise RuntimeServiceError("runtime_wait_timeout")
            result = dict(operation["result"] or {})
            if result.get("status") == "runtime_failed":
                raise RuntimeServiceError(
                    str(result.get("error") or "runtime_process_failed"))
            return result
        finally:
            self._end_operation(operation)

    def stop_round(self):
        if self.runtime is None and not self.start_if_ready():
            raise RuntimeServiceError("no_round_in_flight")
        runtime = self.runtime
        receipt = runtime.request_stop()
        if receipt.get("accepted"):
            return self._stop_receipt(receipt)
        with self._lock:
            operation = self._operation
        if operation and operation.get("kind") == "send":
            runtime.cancel_pending_input()
            runtime.control.cancel_before_round(runtime)
            result = {
                "status": "round_stopped",
                "round_num": None,
                "round_type": None,
                "final_response": "",
                "reason": "user_stopped_before_round",
            }
            self._finish_operation(result)
            return self._stop_receipt({
                "accepted": True,
                "reason": "pending_input_cancelled",
                "stage": "pending_input",
            })
        raise RuntimeServiceError("no_round_in_flight")

    def resolve_tool_approval(self, approval_id, decision):
        runtime = self._require_runtime()
        return runtime.resolve_tool_approval(approval_id, decision)

    def status(self):
        runtime_status = (
            self.runtime.runtime_status()
            if self.runtime is not None else {
                "round_in_flight": False,
                "current_round": None,
                "round_type": None,
                "stage": "initialization",
                "stop_requested": False,
                "stop_latched": False,
                "can_stop": False,
                "heartbeat_suspended": True,
                "pending_tool_approval": None,
            }
        )
        with self._lock:
            operation = self._operation
        return {
            "session_id": self.session_id,
            "process_id": os.getpid(),
            "supervisor_schema": SUPERVISOR_SCHEMA,
            "supervisor_state": self.supervisor_state,
            "supervisor_path": str(self.supervisor_path),
            "host": dict(self.host),
            "operation_in_flight": bool(operation),
            "send_in_flight": bool(operation and operation["kind"] == "send"),
            "relay_in_flight": bool(operation and operation["kind"] == "relay"),
            "runtime": runtime_status,
            "last_outcome": dict(self.last_outcome),
        }

    def close(self, wait_seconds=10):
        if self._closed:
            return
        self._closed = True
        self._write_supervisor("stopping")
        if self.runtime is not None:
            self.runtime.request_shutdown()
        if self.runtime_thread is not None:
            self.runtime_thread.join(timeout=max(0.0, float(wait_seconds)))
        state = (
            "stopped"
            if self.runtime_thread is None or not self.runtime_thread.is_alive()
            else "interrupted"
        )
        self._write_supervisor(state)
        if self.runtime_thread is None or not self.runtime_thread.is_alive():
            self.instance_lock.release()

    def _begin_operation(self, kind, permission_level):
        with self._lock:
            if self._operation is not None:
                raise RuntimeServiceError("round_in_flight")
            if self.runtime and self.runtime.runtime_status()["round_in_flight"]:
                raise RuntimeServiceError("round_in_flight")
            operation = {
                "kind": kind,
                "permission_level": permission_level,
                "event": threading.Event(),
                "result": None,
            }
            self._operation = operation
            return operation

    def _finish_operation(self, result):
        with self._lock:
            operation = self._operation
            if operation is None:
                return
            operation["result"] = dict(result or {})
            operation["event"].set()

    def _end_operation(self, operation):
        with self._lock:
            if self._operation is operation:
                self._operation = None

    def _on_round_started(self, round_num, round_type):
        self._write_supervisor(
            "running",
            current_round=int(round_num),
            round_type=str(round_type),
        )

    def _on_runtime_state_changed(self):
        if self._started and not self._closed and not self._recovering:
            self._write_supervisor("running")

    def _on_round_finished(self, round_num, round_type, result):
        stopped = bool((result or {}).get("_user_stop_requested"))
        settlement = (result or {}).get("_settlement") or {}
        payload = {
            "status": (
                "round_stopped" if stopped
                else str((result or {}).get("status") or "round_completed")
            ),
            "round_num": int(round_num) if round_num is not None else None,
            "round_type": str(round_type or ""),
            "final_response": str((result or {}).get("response") or ""),
            "settlement_status": str(settlement.get("status") or ""),
            "degraded_reasons": list(settlement.get("degraded_reasons") or []),
            "fatal_reasons": list(settlement.get("fatal_reasons") or []),
        }
        self.last_outcome = {
            key: value
            for key, value in payload.items()
            if key != "final_response"
        }
        self._write_supervisor("running", current_round=None, round_type=None)
        self._finish_operation(payload)

    def _recover_interrupted_round(self, runtime, previous):
        round_num = self._recovery_round(previous)
        if round_num is None:
            return
        store = runtime.audit.get_store()
        try:
            events = store.read_events(round_num)
        except FileNotFoundError as exc:
            raise RuntimeServiceError(
                "runtime_recovery_round_missing") from exc
        types = [item.get("event_type") for item in events]
        if "round_closed" in types:
            return
        payload = {
            "reason": "runtime_process_interrupted",
            "previous_process_id": previous.get("process_id"),
            "previous_session_id": previous.get("session_id"),
        }
        if "runtime_process_interrupted" not in types:
            store.append_event(
                round_num, "runtime_process_interrupted", payload)
        recovery_backed_up = any(
            int(item.get("round") or 0) == round_num
            and item.get("reason") == "runtime_process_interrupted"
            for item in runtime.state_backup_store.read_backups()
        )
        if not recovery_backed_up:
            runtime.sm.clear_flags(
                ["user_message_waiting", "continue_requested"])
            settle_open_relay_intents(
                runtime.sm,
                status="blocked",
                round_num=round_num,
                note="runtime_process_interrupted",
                source="runtime.crash_recovery",
            )
            runtime.sm.set_phase("idle")
            runtime.state_backup_store.append_backup(
                round_num,
                runtime.sm.load(),
                reason="runtime_process_interrupted",
            )
        runtime.control.latch_until_explicit(runtime)
        if "round_unsettled" not in types:
            store.append_event(round_num, "round_unsettled", {
                **payload,
                "status": "unsettled",
                "fatal_reasons": ["runtime_process_interrupted"],
            })
        self.last_outcome = {
            "status": "recovered_unsettled",
            "round_num": round_num,
            "reason": "runtime_process_interrupted",
        }

    @staticmethod
    def _recovery_round(previous):
        if not isinstance(previous, dict):
            return None
        if previous.get("state") not in {
                "running", "stopping", "crashed", "interrupted"}:
            return None
        round_num = previous.get("current_round")
        return round_num if isinstance(round_num, int) and round_num > 0 else None

    @staticmethod
    def _previous_requires_explicit_release(previous):
        if not isinstance(previous, dict):
            return False
        outcome = previous.get("last_outcome")
        if not isinstance(outcome, dict):
            return False
        return (
            outcome.get("settlement_status") in {"degraded", "unsettled"}
            or outcome.get("status") in {
                "runtime_failed", "recovered_unsettled", "round_stopped",
            }
        )

    def _require_runtime(self):
        if self.runtime is None and not self.start_if_ready():
            raise RuntimeServiceError("persona_initialization_required")
        if self.runtime_thread is None or not self.runtime_thread.is_alive():
            raise RuntimeServiceError("runtime_process_not_running")
        return self.runtime

    def _read_supervisor(self):
        if not self.supervisor_path.exists():
            return None
        try:
            payload = json.loads(self.supervisor_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeSupervisorCorrupt("runtime_supervisor_corrupt") from exc
        host = payload.get("host") if isinstance(payload, dict) else None
        current_round = payload.get("current_round") if isinstance(payload, dict) else None
        if not (
                isinstance(payload, dict)
                and payload.get("schema_version") == SUPERVISOR_SCHEMA
                and payload.get("state") in {
                    "initializing", "awaiting_persona", "running",
                    "stopping", "stopped", "crashed", "interrupted",
                }
                and isinstance(payload.get("process_id"), int)
                and isinstance(payload.get("session_id"), str)
                and bool(payload.get("session_id"))
                and payload.get("active_pid") == self.active_pid
                and isinstance(host, dict)
                and host.get("address") == "127.0.0.1"
                and isinstance(host.get("port"), int)
                and 0 <= host["port"] <= 65535
                and (
                    current_round is None
                    or isinstance(current_round, int) and current_round > 0
                )
                and (
                    payload.get("round_type") is None
                    or isinstance(payload.get("round_type"), str)
                )
                and isinstance(payload.get("phase"), str)
                and isinstance(payload.get("stop_requested"), bool)
                and isinstance(payload.get("heartbeat_suspended"), bool)
                and isinstance(payload.get("last_outcome"), dict)
                and isinstance(payload.get("updated_at"), str)):
            raise RuntimeSupervisorCorrupt("runtime_supervisor_corrupt")
        return payload

    def _write_supervisor(
            self, state, *, current_round=None, round_type=None):
        if not self._started:
            return
        with self._supervisor_lock:
            self.supervisor_state = str(state)
            runtime_status = (
                self.runtime.runtime_status() if self.runtime is not None else {}
            )
            payload = {
                "schema_version": SUPERVISOR_SCHEMA,
                "state": str(state),
                "process_id": os.getpid(),
                "session_id": self.session_id,
                "active_pid": self.active_pid,
                "host": dict(self.host),
                "current_round": (
                    current_round
                    if current_round is not None
                    else runtime_status.get("current_round")
                ),
                "round_type": (
                    round_type
                    if round_type is not None
                    else runtime_status.get("round_type")
                ),
                "phase": runtime_status.get("stage", "initialization"),
                "stop_requested": bool(runtime_status.get("stop_requested")),
                "heartbeat_suspended": bool(
                    runtime_status.get("heartbeat_suspended", True)),
                "last_outcome": dict(self.last_outcome),
                "updated_at": self._now(),
            }
            atomic_write_json(
                self.supervisor_path,
                payload,
                trailing_newline=True,
            )

    def _stop_receipt(self, result):
        return {
            "schema_version": "seed_gui_runtime_stop_receipt.v1",
            "accepted": True,
            "reason": result.get("reason"),
            "stage": result.get("stage"),
            "round": self.status()["runtime"].get("current_round"),
            "recorded_at": self._now(),
        }

    @staticmethod
    def _now():
        return local_now().isoformat()
