"""Thread-safe stop and shutdown control for the resident Runtime."""
import threading

from errors import APIBridgeError, RequiredContextError


class RuntimeControl:
    def __init__(self):
        self.shutdown = threading.Event()
        self.stop_requested = threading.Event()
        self.lock = threading.Lock()
        self._condition = threading.Condition(self.lock)
        self._idle_mutation_reserved = False
        self.round_in_flight = False
        self.round_num = None
        self.round_type = None
        self.stage = "idle"
        self.stop_latched = False
        self.on_change = None

    def _notify(self):
        if callable(self.on_change):
            self.on_change()

    def establish_round(self, round_type, establish):
        with self._condition:
            while self._idle_mutation_reserved and not self.shutdown.is_set():
                self._condition.wait()
            if (
                self.shutdown.is_set()
                or self.stop_requested.is_set()
                or self.round_in_flight
            ):
                return None
            round_num = int(establish())
            self.round_in_flight = True
            self.round_num = round_num
            self.round_type = str(round_type)
            self.stage = "setup"
        self._notify()
        return round_num

    def reserve_idle_mutation(self):
        """Atomically reserve idle Runtime state for one local mutation."""
        with self._condition:
            if (
                self._idle_mutation_reserved
                or self.round_in_flight
                or self.stage != "idle"
                or self.stop_requested.is_set()
                or self.shutdown.is_set()
            ):
                return False
            self._idle_mutation_reserved = True
            return True

    def release_idle_mutation(self):
        with self._condition:
            if not self._idle_mutation_reserved:
                return False
            self._idle_mutation_reserved = False
            self._condition.notify_all()
            return True

    def begin_round_preparation(self):
        """Enter pre-setup only after any reserved idle mutation completes."""
        with self._condition:
            while self._idle_mutation_reserved and not self.shutdown.is_set():
                self._condition.wait()
            if (
                self.shutdown.is_set()
                or self.stop_requested.is_set()
                or self.round_in_flight
                or self.stage != "idle"
            ):
                return False
            self.stage = "pre_setup_probe"
        self._notify()
        return True

    def end_round_preparation(self):
        changed = False
        with self._condition:
            if not self.round_in_flight and self.stage == "pre_setup_probe":
                self.stage = "idle"
                changed = True
        if changed:
            self._notify()

    def finish_round(self, latch_until_explicit):
        with self.lock:
            self.round_in_flight = False
            self.round_num = None
            self.round_type = None
            self.stage = "idle"
            self.stop_latched = bool(latch_until_explicit)
        self._notify()

    def set_stage(self, stage):
        with self.lock:
            self.stage = str(stage or "idle")
        self._notify()

    def request_shutdown(self, runtime):
        with self._condition:
            self.shutdown.set()
            self._condition.notify_all()
        self.request_stop(runtime)
        wake = getattr(runtime.hb, "wake", None)
        if callable(wake):
            wake()

    def cancel_pending_input(self, runtime):
        runtime._trigger_queue.clear()
        discard = getattr(runtime.hb, "discard_messages", None)
        if callable(discard):
            discard()
        try:
            runtime.sm.clear_flags(["user_message_waiting"])
        except Exception:
            return False
        return True

    def handle_round_error(self, runtime, trigger, exc):
        with self.lock:
            round_num = self.round_num
            round_type = self.round_type or getattr(trigger, "round_type", None)
        try:
            runtime.sm.set_phase("idle")
        except Exception:
            pass
        callback = getattr(runtime, "on_round_finished", None)
        try:
            if callable(callback):
                callback(round_num, round_type, {
                    "status": "runtime_failed",
                    "response": "",
                    "error": f"{type(exc).__name__}: {exc}",
                    "_settlement": {"status": "unsettled"},
                })
        finally:
            with self.lock:
                self.round_in_flight = False
                self.round_num = None
                self.round_type = None
                self.stage = "idle"
            self._notify()
            self.latch_until_explicit(runtime)

    def release_stop_latch(self, executor):
        with self.lock:
            if self.round_in_flight or self.stage == "pre_setup_probe":
                return False
            self.stop_latched = False
            self.stop_requested.clear()
        reset = getattr(executor, "reset_cancellation", None)
        if callable(reset):
            reset()
        self._notify()
        return True

    def cancel_before_round(self, runtime):
        with self.lock:
            if self.round_in_flight:
                return False
            self.stop_requested.set()
            self.stop_latched = True
            self.stage = "idle"
        runtime.executor.request_cancel()
        runtime.hb.pause()
        self._notify()
        return True

    def latch_until_explicit(self, runtime):
        with self.lock:
            if self.round_in_flight:
                return False
            self.stop_requested.clear()
            self.stop_latched = True
            self.stage = "idle"
        runtime.hb.pause()
        self._notify()
        return True

    def request_stop(self, runtime):
        with self.lock:
            if not self.round_in_flight and self.stage != "pre_setup_probe":
                return {
                    "accepted": False,
                    "reason": "no_round_in_flight",
                    "stage": self.stage,
                }
            if self.stage == "cleanup_local":
                return {
                    "accepted": True,
                    "reason": "local_cleanup_in_progress",
                    "stage": self.stage,
                }
            repeated = self.stop_requested.is_set()
            self.stop_requested.set()
            stage = self.stage
            round_num = self.round_num
        runtime.executor.request_cancel()
        try:
            runtime.audit.get_store().append_event(
                round_num,
                "runtime_stop_requested",
                {
                    "reason": "user_stopped",
                    "stage": stage,
                    "repeated": repeated,
                },
                phase=runtime.sm.get_phase(),
            )
        except Exception:
            pass
        self._notify()
        return {
            "accepted": True,
            "reason": "stop_already_requested" if repeated else "stop_requested",
            "stage": stage,
        }

    def snapshot(self, heartbeat):
        with self.lock:
            return {
                "round_in_flight": self.round_in_flight,
                "current_round": self.round_num,
                "round_type": self.round_type,
                "stage": self.stage,
                "stop_requested": self.stop_requested.is_set(),
                "stop_latched": self.stop_latched,
                "can_stop": bool(
                    self.stage == "pre_setup_probe"
                    or (
                        self.round_in_flight
                        and self.stage in {
                            "setup", "reaction", "tool_approval", "cleanup_model"
                        }
                    )
                ),
                "heartbeat_suspended": bool(
                    getattr(heartbeat, "_paused", False)
                ),
            }

    @staticmethod
    def step_exception_result(stage, exc):
        reaction = stage == "reaction"
        provider_failure = reaction and isinstance(exc, APIBridgeError)
        required_context_failure = (
            exc.as_dict() if isinstance(exc, RequiredContextError) else {}
        )
        if provider_failure:
            response = (
                "本轮模型调用未能在既定重试预算内完成，Runtime 已停止继续调用。"
                "活动任务和未完成项仍保留，可在服务恢复后继续。"
            )
            blocked_reason = "blocked/provider_failure"
        elif reaction and required_context_failure:
            response = (
                "本轮因必需上下文读取或投影失败而停止；活动任务和未完成项仍保留。"
                "请根据 Runtime 技术详情修复本地数据或实现后再继续。"
            )
            blocked_reason = "blocked/required_context_failure"
        elif reaction:
            response = (
                "本轮因 Runtime 内部错误而停止；活动任务和未完成项仍保留。"
                "请保留技术详情并修复本地实现后再继续。"
            )
            blocked_reason = "blocked/runtime_error"
        else:
            response = ""
            blocked_reason = ""
        return {
            "aborted": True,
            "response": response,
            "error": f"{stage} step exception: {exc}",
            "_failed_phase": stage,
            "_local_blocked_reason": blocked_reason,
            "_required_context_failure": required_context_failure,
        }
