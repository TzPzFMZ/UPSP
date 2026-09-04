"""
心跳闹钟 — UPSP 的时间感知器
DDS §23.5 心跳机制

硬约束：
  1. 不计轮数（轮数由 runtime.py 管）
  2. 不调 API（零网络请求）
  3. 不注入 LLM（不碰上下文）
  4. 不判断业务（只看数值/时间/待处理事实源，不看语义）
  5. 不回滚（只置位，不消费）
  6. 轮内暂停（runtime 调 pause()，善后步调 resume()）

活动标记按 HEARTBEAT_TRIGGER_GROUPS、HEARTBEAT_QUALIFIER_FLAGS 与
HEARTBEAT_LOCAL_MAINTENANCE_FLAGS 分组。fatigue_expired、identity_timeout
和 process_down 只保留在 state schema 中，Seed 固定为 false，心跳不读取、
不置位，也不据此创建 Round。

engines/ vs scripts/ 边界：
  心跳只管 WHEN（到了该检查的时间）→ 置位 flag
  脚本处理 HOW（具体怎么处理 flag）→ 消费 flag
"""
import threading
from datetime import datetime

from data.state_store import StateStore
from data.config_store import ConfigStore
from data.connectivity_store import ConnectivityStore
from data.memory_heat import MemoryHeat
from constants import local_now
from constants import (
    HEARTBEAT_DEFAULT_INTERVAL,
    RHYTHM_INTERVAL_ROUNDS,
    STANDBY_IDLE_MINUTES,
)


HEARTBEAT_TRIGGER_GROUPS = {
    "interaction": (
        "user_message_waiting",
    ),
    "rhythm": (
        "rhythm_due",
        "calendar_day_due",
        "calendar_week_due",
        "calendar_month_due",
        "calendar_quarter_due",
        "calendar_year_due",
        "memory_compression_due",
        "api_degraded",
        "context_pressure",
    ),
    "relay": (
        "continue_requested",
    ),
    "autonomous": (),
    "standby": (
        "standby_due",
        "shelve_timer_expired",
    ),
}

HEARTBEAT_TRIGGER_PRIORITY = (
    "rhythm",
    "interaction",
    "relay",
    "autonomous",
    "standby",
)

HEARTBEAT_GROUP_ROUND_TYPES = {
    "interaction": "interactive",
    "rhythm": "rhythm",
    "relay": "relay",
    "autonomous": "autonomous",
    "standby": "standby",
}

HEARTBEAT_QUALIFIER_FLAGS = ()

HEARTBEAT_HEALTH_ONLY_FLAGS = ("token_usage_warning",)

HEARTBEAT_LOCAL_MAINTENANCE_FLAGS = (
    "feeling_settle_due",
)


EMERGENCY_GUIDE_FLAGS = (
    "api_degraded",
)

CONTEXT_PRESSURE_GUIDE_FLAGS = ("context_pressure",)

CACHE_COMPACTION_GUIDE_FLAGS = ()

MAIN_AXIS_GUIDE_FLAGS = (
    "rhythm_due",
)

CALENDAR_GUIDE_ITEMS = (
    ("calendar_day", "calendar_day_due"),
    ("calendar_week", "calendar_week_due"),
    ("calendar_month", "calendar_month_due"),
    ("calendar_quarter", "calendar_quarter_due"),
    ("calendar_year", "calendar_year_due"),
)

CALENDAR_GUIDE_FLAGS = tuple(flag for _kind, flag in CALENDAR_GUIDE_ITEMS)


def _active_flags(flags, names):
    flags = flags or {}
    return [name for name in names if flags.get(name)]


def _decision_round_type(flags):
    flags = flags or {}
    for group in HEARTBEAT_TRIGGER_PRIORITY:
        if any(flags.get(flag) for flag in HEARTBEAT_TRIGGER_GROUPS[group]):
            return HEARTBEAT_GROUP_ROUND_TYPES[group]
    return None


def round_type_from_heartbeat_flags(flags):
    """把 heartbeat flags 解释为五类轮触发；qualifier 不单独起轮。"""
    return _decision_round_type(flags)


def round_decision_from_heartbeat_flags(flags):
    """返回 Runtime/CLI/GUIDE 共用的轮型判定结果。"""
    flags = flags or {}
    round_type = _decision_round_type(flags)
    guide_queue = []

    emergency_flags = _active_flags(flags, EMERGENCY_GUIDE_FLAGS)
    if emergency_flags:
        guide_queue.append({"kind": "emergency", "flags": emergency_flags})

    context_pressure_flags = _active_flags(flags, CONTEXT_PRESSURE_GUIDE_FLAGS)
    if context_pressure_flags:
        guide_queue.append({
            "kind": "context_pressure",
            "flags": context_pressure_flags,
        })

    cache_compaction_flags = _active_flags(flags, CACHE_COMPACTION_GUIDE_FLAGS)
    if cache_compaction_flags:
        guide_queue.append({
            "kind": "cache_compaction",
            "flags": cache_compaction_flags,
        })

    main_flags = _active_flags(flags, MAIN_AXIS_GUIDE_FLAGS)
    if main_flags:
        guide_queue.append({"kind": "main_axis_rhythm", "flags": main_flags})

    # An already-active compression cycle is recovery debt from an earlier
    # successful daily chronicle.  It must close before a newly due day can
    # create another cycle.  In the normal path this flag is still false until
    # today's chronicle has been written, so the day -> compression order is
    # unchanged.
    if flags.get("memory_compression_due"):
        guide_queue.append({
            "kind": "memory_compression",
            "flags": ["memory_compression_due"],
        })

    for kind, flag in CALENDAR_GUIDE_ITEMS:
        if flags.get(flag):
            guide_queue.append({"kind": kind, "flags": [flag]})

    if flags.get("user_message_waiting"):
        interaction_item = {"kind": "interaction", "flags": ["user_message_waiting"]}
        if round_type in {"rhythm", "interactive"}:
            guide_queue.append(interaction_item)

    deferred_items = []
    if flags.get("continue_requested") and round_type in {"rhythm", "interactive"}:
        deferred_items.append({"kind": "relay", "flags": ["continue_requested"]})
    elif flags.get("continue_requested") and round_type == "relay":
        guide_queue.append({"kind": "relay", "flags": ["continue_requested"]})

    if flags.get("standby_due") and round_type not in {None, "standby"}:
        deferred_items.append({"kind": "standby", "flags": ["standby_due"]})
    elif flags.get("standby_due") and round_type == "standby":
        guide_queue.append({"kind": "standby", "flags": ["standby_due"]})

    return {
        "round_type": round_type,
        "active_flags": [name for name, value in flags.items() if value],
        "guide_queue": guide_queue,
        "coalesced": len(guide_queue) > 1,
        "deferred_items": deferred_items,
    }


class HeartbeatManager:
    """
    心跳闹钟管理器

    后台线程每隔 interval 秒 tick。
    每次 tick 只做活动布尔检查，有满足就置位。
    善后步是 flag 的唯一消费者（清零）。
    """

    def __init__(self, state_store=None, config_store=None, interval=None,
                 memory_heat=None, connectivity_store=None):
        self.sm = state_store or StateStore()
        self.cfg = config_store or ConfigStore()
        active_endpoint_ids = getattr(self.cfg, "get_active_model_profile_ids", None)
        setup_endpoint_ids = getattr(
            self.cfg, "get_model_profile_ids_for_phase", None)
        self.conn = connectivity_store or ConnectivityStore(
            active_endpoint_ids=active_endpoint_ids,
            recovery_endpoint_ids=(
                (lambda: setup_endpoint_ids("setup"))
                if callable(setup_endpoint_ids)
                else None
            ),
        )
        self.interval = interval or self._load_interval()
        self.memory_heat = memory_heat or MemoryHeat()

        # 线程控制
        self._running = False
        self._paused = False
        self._thread = None
        self._wakeup = threading.Event()
        self._pause_ev = threading.Event()
        self._pause_ev.set()
        self._stop_ev = threading.Event()
        self._standby_started_at = None

        # 消息队列
        self._msg_queue = []
        self._msg_lock = threading.Lock()

    def _load_interval(self):
        try:
            return self.cfg.get_heartbeat_interval()
        except Exception:
            return HEARTBEAT_DEFAULT_INTERVAL

    def _load_rhythm_interval(self):
        try:
            return int(self.cfg.get_rhythm_interval() or RHYTHM_INTERVAL_ROUNDS)
        except Exception:
            return RHYTHM_INTERVAL_ROUNDS

    def _load_standby_threshold(self):
        try:
            return int(self.cfg.get_standby_threshold() or STANDBY_IDLE_MINUTES)
        except Exception:
            return STANDBY_IDLE_MINUTES

    # ----------------------------------------------------------
    # 启动/停止
    # ----------------------------------------------------------

    def start(self):
        if self._running:
            return
        self._standby_started_at = local_now()
        self.sm.set_flag("standby_due", False)
        self._running = True
        self._stop_ev.clear()
        self._thread = threading.Thread(
            target=self._tick_loop, name="heartbeat", daemon=True)
        self._thread.start()

    def stop(self):
        self._running = False
        self._stop_ev.set()
        self._pause_ev.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=self.interval + 1)

    # ----------------------------------------------------------
    # 暂停/恢复（轮内控制）
    # ----------------------------------------------------------

    def pause(self):
        self._paused = True
        self._pause_ev.clear()

    def resume(self, run_tick=True):
        self._paused = False
        self._pause_ev.set()
        if not run_tick:
            return
        try:
            if self._do_tick():
                self._wakeup.set()
        except Exception:
            pass

    def wait_for_wakeup(self, timeout=None):
        result = self._wakeup.wait(timeout=timeout)
        self._wakeup.clear()
        return result

    def wake(self):
        self._wakeup.set()

    # ----------------------------------------------------------
    # 外部消息
    # ----------------------------------------------------------

    def enqueue_message(self, message):
        with self._msg_lock:
            self._msg_queue.append(message)
        # P1-1: 外部输入到达时写时间戳→起手步读此字段判断是否加载 relation 场景规则
        now = local_now()
        try:
            self.sm.update_many({"base.meta.last_external_input_at": now.isoformat()})
        except Exception:
            pass
        # 直接置位唤醒，不等下次心跳 tick（竞态修复）
        try:
            self.sm.set_flag("user_message_waiting", True)
            self._do_tick()
            self._wakeup.set()
        except Exception:
            pass

    def prepend_messages(self, messages):
        with self._msg_lock:
            self._msg_queue[:0] = list(messages or [])

    def dequeue_messages(self):
        with self._msg_lock:
            msgs = self._msg_queue[:]
            self._msg_queue.clear()
            return msgs

    def discard_messages(self):
        return self.dequeue_messages()

    # ----------------------------------------------------------
    # 核心 tick 循环
    # ----------------------------------------------------------

    def _tick_loop(self):
        while self._running:
            self._pause_ev.wait()
            if self._stop_ev.is_set():
                break

            any_set = self._do_tick()
            if any_set:
                self._wakeup.set()

            # 更新最后心跳时间
            try:
                self.sm.set("base.meta.last_heartbeat_at",
                            local_now().isoformat())
            except Exception:
                pass

            self._stop_ev.wait(timeout=self.interval)

    def _do_tick(self):
        """一次 tick 的全部布尔检查（异常记录到 state.last_error）"""
        try:
            state = self.sm.load()
        except Exception as e:
            try:
                self.sm._set_internal("base.meta.last_error",
                    f"心跳tick读state失败: {e}")
            except Exception:
                pass
            return False

        base = state.get("base", {})
        meta = base.get("meta", {})
        flags = base.get("heartbeat_flags", {})
        alert_deferrals = base.get("alert_deferrals", {})

        new_flags = {}
        clear_flags = []
        tick_errors = []
        now = local_now()

        # --- 2. feeling_settle_due ---
        next_settle = meta.get("next_settle_at")
        if next_settle and not flags.get("feeling_settle_due"):
            try:
                if now >= datetime.fromisoformat(next_settle):
                    new_flags["feeling_settle_due"] = True
            except (ValueError, TypeError):
                pass

        # --- 3. api_degraded ---
        api_degraded = self._check_api_degraded()
        if self._alert_deferred(alert_deferrals, "api_degraded", now):
            if flags.get("api_degraded"):
                clear_flags.append("api_degraded")
        elif api_degraded:
            if not flags.get("api_degraded"):
                new_flags["api_degraded"] = True
        elif flags.get("api_degraded") and self._check_api_recovered():
            clear_flags.append("api_degraded")

        # --- 4. memory_compression_due：只投影已冻结的共享日周期 ---
        try:
            from data.memory_compression_store import MemoryCompressionManager

            compression_due = MemoryCompressionManager().has_active_cycle()
            if compression_due and not flags.get("memory_compression_due"):
                new_flags["memory_compression_due"] = True
            elif not compression_due and flags.get("memory_compression_due"):
                clear_flags.append("memory_compression_due")
        except Exception as exc:
            tick_errors.append(f"memory_compression_due: {exc}")

        # --- 6. user_message_waiting ---
        if not flags.get("user_message_waiting"):
            with self._msg_lock:
                if self._msg_queue:
                    new_flags["user_message_waiting"] = True

        # --- 7. rhythm_due ---
        total = meta.get("total_round", 0)
        last_rhythm = meta.get("last_rhythm_round", 0)
        rhythm_interval = self._load_rhythm_interval()
        rhythm_due = total - last_rhythm >= rhythm_interval and total > 0
        if rhythm_due:
            if not flags.get("rhythm_due"):
                new_flags["rhythm_due"] = True
        elif flags.get("rhythm_due"):
            clear_flags.append("rhythm_due")

        # --- 8. standby_due ---
        ref_time = (
            meta.get("last_round_closed_at")
            or meta.get("last_external_input_at")
            or meta.get("last_update")
        )
        if ref_time or self._standby_started_at:
            try:
                ref_at = datetime.fromisoformat(ref_time) if ref_time else None
                if self._standby_started_at and (
                        ref_at is None or ref_at < self._standby_started_at):
                    ref_at = self._standby_started_at
                idle_min = (now - ref_at).total_seconds() / 60
                standby_due = idle_min >= self._load_standby_threshold()
                if standby_due:
                    if not flags.get("standby_due"):
                        new_flags["standby_due"] = True
                elif flags.get("standby_due"):
                    clear_flags.append("standby_due")
            except (ValueError, TypeError):
                pass

        # --- 9. shelve_timer_expired ---
        shelve_at = meta.get("shelve_timer_at")
        if shelve_at and not flags.get("shelve_timer_expired"):
            try:
                if now > datetime.fromisoformat(shelve_at):
                    new_flags["shelve_timer_expired"] = True
            except (ValueError, TypeError):
                pass

        meta_updates = {}

        # --- 11. 日历五项（V5: 日/周/月/季/年）---
        last_cal = meta.get("last_calendar_check_at")
        if last_cal:
            try:
                last_date = datetime.fromisoformat(last_cal).date()
                today = now.date()
                if today > last_date:
                    new_flags["calendar_day_due"] = True
                if today.isocalendar()[:2] != last_date.isocalendar()[:2]:
                    new_flags["calendar_week_due"] = True
                if today.month != last_date.month or today.year != last_date.year:
                    new_flags["calendar_month_due"] = True
                if (today.year != last_date.year or
                    (today.month - 1) // 3 != (last_date.month - 1) // 3):
                    new_flags["calendar_quarter_due"] = True
                if today.year != last_date.year:
                    new_flags["calendar_year_due"] = True
            except (ValueError, TypeError):
                pass
        else:
            meta_updates["base.meta.last_calendar_check_at"] = now.isoformat()

        # 旧泛化告警已退役；真实输入水位只生成 cache_compaction_due。
        if flags.get("token_usage_warning"):
            clear_flags.append("token_usage_warning")

        # 批量置位
        updates = {}
        if new_flags:
            updates.update({f"base.heartbeat_flags.{k}": v for k, v in new_flags.items()})
        if clear_flags:
            updates.update({f"base.heartbeat_flags.{k}": False for k in clear_flags})
        updates.update(meta_updates)
        if updates:
            try:
                self.sm.update_many(updates)
            except Exception as e:
                try:
                    self.sm._set_internal("base.meta.last_error",
                        f"心跳tick写flags失败: {e}")
                except Exception:
                    pass
            final_flags = dict(flags)
            for flag in clear_flags:
                final_flags[flag] = False
            final_flags.update(new_flags)
            return (
                bool(new_flags)
                or bool(round_type_from_heartbeat_flags(final_flags))
                or bool(final_flags.get("feeling_settle_due"))
            )
        if tick_errors:
            try:
                self.sm._set_internal("base.meta.last_error",
                    f"心跳tick异常: {'; '.join(tick_errors[:3])}")
            except Exception:
                pass

        return (
            bool(round_type_from_heartbeat_flags(flags))
            or bool(flags.get("feeling_settle_due"))
        )

    # ----------------------------------------------------------
    # 辅助检查
    # ----------------------------------------------------------

    def _check_api_degraded(self):
        """检查 connectivity.json 是否有 API 降级（通过 ConnectivityStore）"""
        try:
            return self.conn.has_degraded()
        except Exception:
            return False

    def _check_api_recovered(self):
        try:
            checker = getattr(self.conn, "has_recovered", None)
            return bool(checker()) if callable(checker) else not self.conn.has_degraded()
        except Exception:
            return False

    @staticmethod
    def _alert_deferred(alert_deferrals, alert_type, now):
        """判断某类紧急项是否仍在搁置窗口内。"""
        if not isinstance(alert_deferrals, dict):
            return False
        item = alert_deferrals.get(alert_type)
        if not isinstance(item, dict):
            return False
        if str(item.get("status") or "").strip().lower() != "deferred":
            return False
        defer_until = item.get("defer_until")
        if not defer_until:
            return False
        try:
            until = datetime.fromisoformat(str(defer_until))
        except Exception:
            return False
        return now < until


# ============================================================
# 自检
# ============================================================

if __name__ == "__main__":
    sm = StateStore()
    sm.init_if_missing()
    hb = HeartbeatManager(sm, interval=2)
    print(f"心跳间隔：{hb.interval}s，18 项检查")
    print("启动心跳（Ctrl+C 停止）...")
    hb.start()
    try:
        while True:
            if hb.wait_for_wakeup(timeout=10):
                flags = sm.get_flags()
                active = [k for k, v in flags.items() if v]
                print(f"  活跃 flags: {active}")
            else:
                print("  tick...（无新 flag）")
    except KeyboardInterrupt:
        print("\n停止...")
        hb.stop()
