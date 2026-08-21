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
from engines.heartbeat import round_decision_from_heartbeat_flags
from logic.relay_intent_pool import settle_open_relay_intents
from paths import (
    ACTIVE_INSTANCE_ID,
    ACTIVE_PID,
    PERSONA_DIR,
    SHARED_PERSONA_DIR,
    UPSP_LOCAL_STATE_ROOT,
)

try:
    import msvcrt
except ImportError:  # pragma: no cover - Windows product only
    msvcrt = None


SUPERVISOR_SCHEMA = "upsp_runtime_supervisor.v2"


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
            self, *, runtime_dir=None, lock_path=None, active_pid=ACTIVE_PID,
            active_instance_id=ACTIVE_INSTANCE_ID,
            persona_ready=None, environment_factory=None, runtime_factory=None,
            default_permission_level=None, retention_enforcer=None,
            ltm_reconciler=None):
        self.active_pid = str(active_pid)
        self.active_instance_id = str(active_instance_id)
        default_runtime_root = Path(UPSP_LOCAL_STATE_ROOT) / "runtime"
        self.runtime_dir = Path(
            runtime_dir
            or default_runtime_root / self.active_pid / self.active_instance_id
        )
        self.supervisor_path = self.runtime_dir / "supervisor.json"
        self.legacy_supervisor_path = (
            default_runtime_root / self.active_pid / "supervisor.json"
            if runtime_dir is None and self.active_instance_id == "meta"
            else None
        )
        self.instance_lock = _InstanceLock(
            lock_path
            or (default_runtime_root / "runtime.lock" if runtime_dir is None
                else self.runtime_dir / "runtime.lock")
        )
        self.persona_ready = persona_ready or (
            lambda: (
                (Path(SHARED_PERSONA_DIR) / "core.md").is_file()
                and (Path(PERSONA_DIR) / "state.json").is_file()
            )
        )
        self.environment_factory = environment_factory
        self.runtime_factory = runtime_factory
        self._uses_default_runtime = (
            environment_factory is None and runtime_factory is None)
        self.default_permission_level = default_permission_level
        self.retention_enforcer = retention_enforcer or (
            lambda: {"status": "not_configured"}
        )
        self.retention_receipt = None
        self.ltm_reconciler = ltm_reconciler
        self.ltm_reconciliation_receipt = None
        self.runtime = None
        self.runtime_thread = None
        self.session_id = uuid.uuid4().hex
        self.host = {"address": "127.0.0.1", "port": 0}
        self._lock = threading.Lock()
        self._retention_lock = threading.Lock()
        self._supervisor_lock = threading.Lock()
        self._operation = None
        self._started = False
        self._closed = False
        self._previous_supervisor = None
        self._recovering = False
        self._instance_switch = None
        self.supervisor_state = "new"
        self.last_outcome = {}

    def start(self, *, host_address="127.0.0.1", port=0):
        self.runtime_dir.mkdir(parents=True, exist_ok=True)
        if not self.instance_lock.acquire():
            previous = self._read_supervisor()
            host = (previous or {}).get("host") or {}
            raise RuntimeAlreadyRunning(host)
        try:
            self._migrate_legacy_supervisor()
            previous = self._read_supervisor()
            recovery_pending = self._recovery_round(previous) is not None
            self._previous_supervisor = previous
            self.host = {"address": str(host_address), "port": int(port)}
            self._started = True
            if self.persona_ready():
                self.enforce_round_retention()
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

    def enforce_round_retention(self):
        with self._retention_lock:
            try:
                self.retention_receipt = self.retention_enforcer()
            except Exception as exc:
                raise RuntimeServiceError(
                    f"round_retention_failed:{exc}"
                ) from exc
            return dict(self.retention_receipt or {})

    def start_if_ready(self):
        with self._lock:
            if self.runtime is not None:
                return True
            if not self.persona_ready():
                return False
            if self.retention_receipt is None:
                self.enforce_round_retention()
            if self.ltm_reconciliation_receipt is None:
                try:
                    if self.ltm_reconciler is not None:
                        self.ltm_reconciliation_receipt = self.ltm_reconciler()
                    elif self._uses_default_runtime:
                        from data.memory_store import MemoryStore
                        self.ltm_reconciliation_receipt = (
                            MemoryStore().reconcile_ltm_projections())
                    else:
                        self.ltm_reconciliation_receipt = {
                            "status": "not_configured"}
                except Exception as exc:
                    raise RuntimeServiceError(
                        f"ltm_projection_failed:{exc}") from exc
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
                name=f"upsp-runtime-{self.active_pid}-{self.active_instance_id}",
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

    def submit_message(self, message, permission_level, *, timeout=None,
                       final_response_max_chars=None, response_contract=None,
                       task_guidance_enabled=True):
        runtime = self._require_runtime()
        operation = self._begin_operation("send", permission_level)
        try:
            kwargs = {}
            if final_response_max_chars is not None:
                kwargs["final_response_max_chars"] = final_response_max_chars
            if response_contract:
                kwargs["response_contract"] = response_contract
            if not task_guidance_enabled:
                kwargs["task_guidance_enabled"] = False
            if not runtime.submit_message(
                    message,
                    execution_permission_level=permission_level,
                    **kwargs):
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

    def update_execution_permission(self, permission_level):
        runtime = self._require_runtime()
        try:
            return runtime.permission_updates.request(permission_level)
        except ValueError as exc:
            raise RuntimeServiceError(str(exc)) from exc

    def mutate_periodic_memory(self, action, mem_id):
        """Apply one GUI-only periodic mount while Runtime is provably idle."""
        runtime = self._require_runtime()
        operation = self._begin_operation("periodic_memory", "local")
        heartbeat = runtime.hb
        was_paused = bool(getattr(heartbeat, "_paused", False))
        reserved = False
        heartbeat_paused = False
        try:
            if not runtime.control.reserve_idle_mutation():
                raise RuntimeServiceError("round_in_flight")
            reserved = True
            heartbeat.pause()
            heartbeat_paused = True
            status = runtime.runtime_status()
            if (
                status.get("round_in_flight")
                or status.get("pending_tool_approval")
                or str(status.get("stage") or "idle") != "idle"
            ):
                raise RuntimeServiceError("round_in_flight")
            from logic.periodic_memory_mount import PeriodicMemoryMountProcessor

            receipt = PeriodicMemoryMountProcessor(
                memory_store=runtime.memory_store,
                heat=runtime.heat,
                assembler=runtime.assembler,
                config_store=runtime.cfg,
                instance_id=self.active_instance_id,
            ).apply(action, mem_id)
            return {
                "schema_version": "seed_gui_periodic_memory_result.v1",
                "submission_source": "seed_gui",
                "receipt": receipt,
            }
        finally:
            if reserved:
                runtime.control.release_idle_mutation()
            if heartbeat_paused and not was_paused:
                try:
                    heartbeat.resume(run_tick=False)
                except TypeError:  # compatibility with injected test doubles
                    heartbeat.resume()
            self._end_operation(operation)

    def prepare_instance_switch(self):
        """Reserve an idle Runtime until the desktop restarts or mutation fails."""
        operation = self._begin_operation("instance_switch", "local")
        runtime = self.runtime
        switch = {
            "operation": operation,
            "runtime": runtime,
            "reserved": False,
            "heartbeat_paused": False,
            "was_paused": bool(runtime and getattr(runtime.hb, "_paused", False)),
        }
        try:
            if runtime is not None:
                if not runtime.control.reserve_idle_mutation():
                    raise RuntimeServiceError("round_in_flight")
                switch["reserved"] = True
                runtime.hb.pause()
                switch["heartbeat_paused"] = True
                status = runtime.runtime_status()
                if (
                    status.get("round_in_flight")
                    or status.get("pending_tool_approval")
                    or str(status.get("stage") or "idle") != "idle"
                ):
                    raise RuntimeServiceError("round_in_flight")
            with self._lock:
                self._instance_switch = switch
        except Exception:
            if switch["reserved"]:
                runtime.control.release_idle_mutation()
            if switch["heartbeat_paused"] and not switch["was_paused"]:
                try:
                    runtime.hb.resume(run_tick=False)
                except TypeError:  # compatibility with injected test doubles
                    runtime.hb.resume()
            self._end_operation(operation)
            raise

    def cancel_instance_switch(self):
        with self._lock:
            switch = self._instance_switch
            self._instance_switch = None
        if switch is None:
            return
        runtime = switch["runtime"]
        if switch["reserved"]:
            runtime.control.release_idle_mutation()
        if switch["heartbeat_paused"] and not switch["was_paused"]:
            try:
                runtime.hb.resume(run_tick=False)
            except TypeError:  # compatibility with injected test doubles
                runtime.hb.resume()
        self._end_operation(switch["operation"])

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
            "active_pid": self.active_pid,
            "active_instance_id": self.active_instance_id,
            "supervisor_state": self.supervisor_state,
            "supervisor_path": str(self.supervisor_path),
            "host": dict(self.host),
            "operation_in_flight": bool(operation),
            "send_in_flight": bool(operation and operation["kind"] == "send"),
            "relay_in_flight": bool(operation and operation["kind"] == "relay"),
            "runtime": runtime_status,
            "cli_data": self._gui_cli_projection(runtime_status),
            "last_outcome": dict(self.last_outcome),
        }

    def _gui_cli_projection(self, runtime_status):
        runtime = self.runtime
        if runtime is None:
            return {
                "total_round": 0,
                "active_flags": [],
                "round_type": runtime_status.get("round_type"),
                "guide_queue": [],
                "coalesced": False,
                "deferred_items": [],
                "phase": "initialization",
                "active_guides": {"rhythm": "", "work": ""},
                "active_task": "",
            }
        try:
            state = runtime.sm.load()
            base = state.get("base", {}) if isinstance(state, dict) else {}
            meta = base.get("meta", {}) if isinstance(base, dict) else {}
            flags = base.get("heartbeat_flags", {}) if isinstance(base, dict) else {}
            flags = flags if isinstance(flags, dict) else {}
            decision = round_decision_from_heartbeat_flags(flags)
            slots = runtime.workbench.active_guide_slots()
            slots = slots if isinstance(slots, dict) else {}
            return {
                "total_round": meta.get("total_round", 0) if isinstance(meta, dict) else 0,
                "active_flags": [name for name, value in flags.items() if value],
                "round_type": decision.get("round_type"),
                "guide_queue": decision.get("guide_queue") or [],
                "coalesced": bool(decision.get("coalesced")),
                "deferred_items": decision.get("deferred_items") or [],
                "phase": (
                    (base.get("runtime") or {}).get("phase", "idle")
                    if isinstance(base.get("runtime"), dict)
                    else "idle"
                ),
                "active_guides": {
                    key: str(slots.get(key) or "").strip()
                    for key in ("rhythm", "work")
                },
                "active_task": str(
                    runtime.workbench.get("base.active_task") or ""
                ).strip(),
            }
        except Exception:
            return {
                "total_round": 0,
                "active_flags": [],
                "round_type": runtime_status.get("round_type"),
                "guide_queue": [],
                "coalesced": False,
                "deferred_items": [],
                "phase": runtime_status.get("stage") or "idle",
                "active_guides": {"rhythm": "", "work": ""},
                "active_task": "",
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
        final_response_source = str(
            (result or {}).get("_final_response_source") or "")
        blocked_reason = str(
            (result or {}).get("_local_blocked_reason") or "")
        runtime_blocked = bool(
            blocked_reason
            or final_response_source
            == "reaction.runtime_auto_blocked_final_reply"
        )
        payload = {
            "status": (
                "round_stopped" if stopped
                else str((result or {}).get("status") or "round_completed")
            ),
            "round_num": int(round_num) if round_num is not None else None,
            "round_type": str(round_type or ""),
            "final_response": str((result or {}).get("response") or ""),
            "final_response_source": final_response_source,
            "classification": (
                "runtime_blocked_closed" if runtime_blocked else ""
            ),
            "reason": (
                blocked_reason
                or ("runtime_auto_blocked_final_reply" if runtime_blocked else "")
            ),
            "response_contract": dict(
                (result or {}).get("_response_contract") or {}),
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
                and payload.get("active_instance_id") == self.active_instance_id
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
                "active_instance_id": self.active_instance_id,
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

    def _migrate_legacy_supervisor(self):
        path = self.legacy_supervisor_path
        if path is None or self.supervisor_path.exists() or not path.is_file():
            return
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise RuntimeSupervisorCorrupt("runtime_supervisor_corrupt") from exc
        if not isinstance(payload, dict) or payload.get("active_pid") != self.active_pid:
            raise RuntimeSupervisorCorrupt("runtime_supervisor_corrupt")
        payload["schema_version"] = SUPERVISOR_SCHEMA
        payload["active_instance_id"] = self.active_instance_id
        atomic_write_json(self.supervisor_path, payload, trailing_newline=True)
        path.unlink()

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
