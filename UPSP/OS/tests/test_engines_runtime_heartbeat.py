import os
import sys
import time

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

class TestCircuitBreaker:
    def test_initial_state_closed(self):
        from engines.executor import CircuitBreaker
        cb = CircuitBreaker()
        assert cb.state == "closed"
        assert cb.allow_request() is True

    def test_opens_after_max_failures(self):
        from engines.executor import CircuitBreaker
        cb = CircuitBreaker(max_failures=2, cooldown_seconds=0.1)
        cb.record_failure()
        assert cb.state == "closed"
        cb.record_failure()
        assert cb.state == "open"

    def test_blocks_when_open(self):
        from engines.executor import CircuitBreaker
        cb = CircuitBreaker(max_failures=1, cooldown_seconds=60)
        cb.record_failure()
        assert cb.state == "open"
        assert cb.allow_request() is False

    def test_half_open_after_cooldown(self):
        from engines.executor import CircuitBreaker
        cb = CircuitBreaker(max_failures=1, cooldown_seconds=0.01)
        cb.record_failure()
        assert cb.state == "open"
        time.sleep(0.02)
        assert cb.allow_request() is True
        assert cb.state == "half_open"

    def test_recovery_after_success(self):
        from engines.executor import CircuitBreaker
        cb = CircuitBreaker(max_failures=2, cooldown_seconds=0.01)
        cb.record_failure()
        cb.record_failure()
        assert cb.state == "open"
        time.sleep(0.02)
        assert cb.allow_request() is True  # half_open
        assert cb.state == "half_open"
        cb.record_success()
        assert cb.state == "closed"


class TestHeartbeat:
    def test_init(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 5)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        hb = HeartbeatManager(sm)
        assert hb.interval == 5

    def test_start_stop(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 1)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        hb = HeartbeatManager(sm, interval=0.1)
        hb.start()
        assert hb._running is True
        hb.stop()
        assert hb._running is False

    def test_pause_resume(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 1)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        hb = HeartbeatManager(sm, interval=0.1)
        hb.pause()
        assert hb._paused is True
        hb.resume()
        assert hb._paused is False

    def test_tick_sets_rhythm_due(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.update_many({
            "base.meta.total_round": 33,
            "base.meta.last_rhythm_round": 0,
        })
        hb = HeartbeatManager(sm, interval=0.1)
        hb._do_tick()
        flags = sm.get_flags()
        assert flags["rhythm_due"] is True

    def test_tick_sets_token_warning(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.token_usage.usage_ratio", 0.75)
        hb = HeartbeatManager(sm, interval=0.1)
        hb._do_tick()
        flags = sm.get_flags()
        assert flags["token_usage_warning"] is True

    def test_tick_clears_stale_api_degraded_when_connectivity_recovers(
            self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        class MockHeat:
            def has_pending_degrade(self): return False

        class MockEvolution:
            def should_trigger(self, thresholds): return False

        class MockConnectivity:
            def has_degraded(self): return False

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set_flag("api_degraded", True)
        hb = HeartbeatManager(
            sm,
            interval=0.1,
            memory_heat=MockHeat(),
            connectivity_store=MockConnectivity(),
            evolution_store=MockEvolution(),
        )

        assert hb._do_tick() is False
        assert sm.get("base.heartbeat_flags.api_degraded") is False

    def test_tick_clears_stale_token_warning_when_usage_is_below_threshold(
            self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.update_many({
            "base.heartbeat_flags.token_usage_warning": True,
            "base.token_usage.current_tokens": 100,
            "base.token_usage.window_size": 1000,
            "base.token_usage.usage_ratio": 0.1,
        })
        hb = HeartbeatManager(sm, interval=0.1)
        monkeypatch.setattr(hb, "_check_api_degraded", lambda: False)

        assert hb._do_tick() is False
        assert sm.get("base.heartbeat_flags.token_usage_warning") is False

    def test_tick_respects_disabled_fatigue_config(
            self, tmp_path, monkeypatch):
        from datetime import datetime, timedelta
        from constants import TZ_SHANGHAI
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        class DisabledFatigueConfig:
            def get_heartbeat_interval(self): return 1

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        expired = datetime.now(TZ_SHANGHAI) - timedelta(hours=72)
        sm.set("base.fatigue.awake_since", expired.isoformat())
        hb = HeartbeatManager(sm, config_store=DisabledFatigueConfig(), interval=0.1)

        hb._do_tick()

        assert sm.get("base.heartbeat_flags.fatigue_expired") is False

    def test_tick_does_not_set_fatigue_expired_even_when_legacy_config_enabled(
            self, tmp_path, monkeypatch):
        from datetime import datetime, timedelta
        from constants import TZ_SHANGHAI
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        class LegacyFatigueConfig:
            def get_heartbeat_interval(self): return 1

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        expired = datetime.now(TZ_SHANGHAI) - timedelta(hours=72)
        sm.set("base.fatigue.awake_since", expired.isoformat())
        hb = HeartbeatManager(sm, config_store=LegacyFatigueConfig(), interval=0.1)

        hb._do_tick()

        assert sm.get("base.heartbeat_flags.fatigue_expired") is False

    def test_tick_existing_continue_requested_wakes_next_round(
            self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set_flag("continue_requested", True)
        hb = HeartbeatManager(sm, interval=0.1)

        assert hb._do_tick() is True

    def test_tick_does_not_set_identity_timeout_from_expired_confirmation(
            self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        from constants import TZ_SHANGHAI
        from datetime import datetime, timedelta
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        expired = datetime.now(TZ_SHANGHAI) - timedelta(seconds=7200)
        sm.update_many({
            "base.identity.confirmed": True,
            "base.identity.confirmed_at": expired.isoformat(),
        })
        hb = HeartbeatManager(sm, interval=0.1)
        hb._do_tick()
        flags = sm.get_flags()
        assert flags["identity_timeout"] is False
        assert sm.get("base.identity.confirmed") is True
        assert sm.get("base.identity.confirmed_at") == expired.isoformat()

    def test_tick_clears_stale_identity_timeout_flag(
            self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.heartbeat_flags.identity_timeout", True)
        hb = HeartbeatManager(sm, interval=0.1)

        hb._do_tick()

        assert sm.get_flags()["identity_timeout"] is False

    def test_enqueue_keeps_identity_confirmed_across_external_input_gap(
            self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        from constants import TZ_SHANGHAI
        from datetime import datetime, timedelta
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        expired = datetime.now(TZ_SHANGHAI) - timedelta(seconds=7200)
        sm.update_many({
            "base.identity.confirmed": True,
            "base.identity.confirmed_at": expired.isoformat(),
            "base.meta.last_external_input_at": expired.isoformat(),
        })
        hb = HeartbeatManager(sm, interval=0.1)

        hb.enqueue_message("我是 Codex，继续验证。")

        flags = sm.get_flags()
        assert flags["identity_timeout"] is False
        assert sm.get("base.identity.confirmed") is True
        assert sm.get("base.identity.confirmed_at") == expired.isoformat()

    def test_enqueue_message_recomputes_stale_facts_before_wakeup(
            self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        class DisabledFatigueConfig:
            def get_heartbeat_interval(self): return 1
            def get_standby_threshold(self): return 30

        class MockHeat:
            def has_pending_degrade(self): return False

        class MockEvolution:
            def should_trigger(self, thresholds): return False

        class MockConnectivity:
            def has_degraded(self): return False

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set_flag("fatigue_expired", True)
        sm.set_flag("standby_due", True)
        hb = HeartbeatManager(
            sm,
            config_store=DisabledFatigueConfig(),
            interval=0.1,
            memory_heat=MockHeat(),
            connectivity_store=MockConnectivity(),
            evolution_store=MockEvolution(),
        )

        hb.enqueue_message("我是 Codex。")

        flags = sm.get_flags()
        assert flags["user_message_waiting"] is True
        assert flags["fatigue_expired"] is False
        assert flags["standby_due"] is False

    def test_tick_sets_all_calendar_flags_after_year_boundary(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.meta.last_calendar_check_at", "2025-12-31T00:00:00+08:00")
        hb = HeartbeatManager(sm, interval=0.1)

        hb._do_tick()

        flags = sm.get_flags()
        assert flags["calendar_day_due"] is True
        assert flags["calendar_week_due"] is True
        assert flags["calendar_month_due"] is True
        assert flags["calendar_quarter_due"] is True
        assert flags["calendar_year_due"] is True

    def test_tick_initializes_calendar_baseline_without_due_flags(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)

        class MockHeat:
            def has_pending_degrade(self): return False

        class MockEvolution:
            def should_trigger(self, thresholds): return False

        class MockConnectivity:
            def has_degraded(self): return False

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.meta.last_calendar_check_at", None)
        hb = HeartbeatManager(
            sm,
            interval=0.1,
            memory_heat=MockHeat(),
            connectivity_store=MockConnectivity(),
            evolution_store=MockEvolution(),
        )

        assert hb._do_tick() is False

        flags = sm.get_flags()
        assert flags["calendar_day_due"] is False
        assert flags["calendar_week_due"] is False
        assert flags["calendar_month_due"] is False
        assert flags["calendar_quarter_due"] is False
        assert flags["calendar_year_due"] is False
        assert sm.get("base.meta.last_calendar_check_at")

    def test_tick_does_not_set_false_positives(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        # 用 mock 替代 MemoryHeat，避免读真实 heat.json
        class MockHeat:
            def has_pending_degrade(self): return False
        class MockEvolution:
            def should_trigger(self, thresholds): return False
        class MockConnectivity:
            def has_degraded(self): return False
        hb = HeartbeatManager(
            sm,
            interval=0.1,
            memory_heat=MockHeat(),
            connectivity_store=MockConnectivity(),
            evolution_store=MockEvolution(),
        )
        hb._do_tick()
        flags = sm.get_flags()
        all_false = all(not v for v in flags.values())
        assert all_false, f"有非预期的 flag 被置位: {[k for k,v in flags.items() if v]}"

    def test_message_queue(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        monkeypatch.setattr(HeartbeatManager, "_load_interval", lambda s: 2)
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        hb = HeartbeatManager(sm, interval=0.1)
        hb.enqueue_message("用户消息")
        hb._do_tick()
        flags = sm.get_flags()
        assert flags["user_message_waiting"] is True

    def test_tick_sets_evolution_pending_when_material_threshold_reached(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore
        from data.evolution_store import EvolutionStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        evolution = EvolutionStore(str(tmp_path / "Iteration"))
        pending = tmp_path / "Iteration" / "Raw" / "Tacit" / "pending.jsonl"
        pending.parent.mkdir(parents=True)
        pending.write_text('{"round":1,"kept":["MEM-A"]}\n', encoding="utf-8")
        monkeypatch.setattr(HeartbeatManager, "_load_evolution_thresholds", lambda s: {
            "tacit_pending_threshold": 1,
            "connection_pending_threshold": 99,
        })

        hb = HeartbeatManager(sm, interval=0.1, evolution_store=evolution)
        hb._do_tick()

        flags = sm.get_flags()
        assert flags["evolution_pending"] is True

    def test_tick_ignores_legacy_next_round_relay_hint(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        state = sm.load()
        state["base"]["runtime"]["next_round"] = {
            "type": "relay",
            "brief": "继续上轮任务",
        }
        sm.save(state)
        monkeypatch.setattr(HeartbeatManager, "_check_api_degraded", lambda s: False)
        monkeypatch.setattr(HeartbeatManager, "_check_process_down", lambda s: False)
        hb = HeartbeatManager(sm, interval=0.1)

        assert hb._do_tick() is False

        assert sm.get("base.heartbeat_flags.continue_requested") is False
        assert "next_round" not in sm.load()["base"]["runtime"]

    def test_tick_ignores_legacy_next_round_rhythm_hint(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        state = sm.load()
        state["base"]["runtime"]["next_round"] = {
            "type": "rhythm",
            "subtype": "token_pressure",
            "brief": "token usage critical",
        }
        sm.save(state)
        monkeypatch.setattr(HeartbeatManager, "_check_api_degraded", lambda s: False)
        monkeypatch.setattr(HeartbeatManager, "_check_process_down", lambda s: False)
        hb = HeartbeatManager(sm, interval=0.1)

        assert hb._do_tick() is False

        assert sm.get("base.heartbeat_flags.rhythm_due") is False
        assert "next_round" not in sm.load()["base"]["runtime"]

    def test_tick_ignores_legacy_next_round_interactive_hint(self, tmp_path, monkeypatch):
        from engines.heartbeat import HeartbeatManager
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        state = sm.load()
        state["base"]["runtime"]["next_round"] = {
            "type": "interactive",
            "brief": "无外部输入不得合成交互轮",
        }
        sm.save(state)
        monkeypatch.setattr(HeartbeatManager, "_check_api_degraded", lambda s: False)
        monkeypatch.setattr(HeartbeatManager, "_check_process_down", lambda s: False)
        hb = HeartbeatManager(sm, interval=0.1)

        assert hb._do_tick() is False

        assert "next_round" not in sm.load()["base"]["runtime"]
        assert not any(sm.get_flags().values())
