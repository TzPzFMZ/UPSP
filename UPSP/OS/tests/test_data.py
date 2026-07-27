"""
Phase 1 数据访问层测试 — 全部用 tmp_path，不碰 persona/

测试原则：
  - 每个 store 的读写一致性（write → read → 数据相同）
  - 原子写入（不会留下 .tmp 文件）
  - DEFAULT fallback（文件不存在时不崩溃）
  - 异常路径（读损坏文件、写无权限目录等）
"""
import json
import os
import sys
import inspect
import threading
from pathlib import Path
from datetime import datetime as RealDatetime
from pathlib import Path
from urllib.request import urlopen
import pytest
from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path
from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class TestAtomicWriteHelpers:
    def test_spec556_atomic_write_json_preserves_format_and_trailing_newline(
            self, tmp_path):
        from data import atomic_write

        target = tmp_path / "nested" / "payload.json"

        atomic_write.atomic_write_json(
            target,
            {"b": 2, "a": "中文"},
            sort_keys=True,
            trailing_newline=True,
        )

        assert target.read_text(encoding="utf-8") == (
            '{\n  "a": "中文",\n  "b": 2\n}\n'
        )
        assert not list(target.parent.glob("*.tmp"))

    def test_spec556_atomic_write_jsonl_writes_one_record_per_line(self, tmp_path):
        from data import atomic_write

        target = tmp_path / "ledger.jsonl"

        atomic_write.atomic_write_jsonl(
            target,
            [{"b": 2, "a": 1}, {"event": "完成"}],
            sort_keys=True,
        )

        assert target.read_text(encoding="utf-8").splitlines() == [
            '{"a": 1, "b": 2}',
            '{"event": "完成"}',
        ]
        assert not list(tmp_path.glob("*.tmp"))

    def test_spec676_store_writers_preserve_exact_json_bytes(
            self, tmp_path, monkeypatch):
        from data import (
            config_store,
            connectivity_store,
            memory_heat,
            memory_index,
            memory_store,
            relation_store,
            state_backup_store,
        )

        sample = {
            "中文": "机械收缩",
            "nested": {"n": 7, "ok": True},
            "items": ["甲", "乙"],
        }
        def encoded(text):
            return text.replace("\n", os.linesep).encode()

        expected = encoded(json.dumps(sample, ensure_ascii=False, indent=2))

        config_path = tmp_path / "config" / "system.json"
        monkeypatch.setitem(
            config_store._CONFIG_MAP,
            "system",
            (str(config_path), config_store._CONFIG_MAP["system"][1]),
        )
        system = config_store.default_system_config()
        system["heartbeat"]["interval"] = 9
        config_store.ConfigStore().save("system", system)

        connectivity_path = tmp_path / "connectivity.json"
        connectivity_store.ConnectivityStore(str(connectivity_path)).save(sample)

        heat_path = tmp_path / "heat.json"
        monkeypatch.setattr(memory_heat, "HEAT_JSON", str(heat_path))
        memory_heat.MemoryHeat().save_heat(sample)

        stm_path = tmp_path / "stm_index.json"
        ltm_path = tmp_path / "ltm_index.json"
        relation_index_path = tmp_path / "relation_index.json"
        monkeypatch.setattr(memory_index, "KEYWORDS_JSON", str(stm_path))
        monkeypatch.setattr(memory_index, "LTM_KEYWORDS_JSON", str(ltm_path))
        index = memory_index.MemoryIndex(str(relation_index_path))
        index.save_index(sample)
        index.save_ltm_index(sample)
        index.save_relation_index(sample)

        meta_path = tmp_path / "meta.json"
        monkeypatch.setattr(memory_store, "META_JSON", str(meta_path))
        memory_store.MemoryStore().save_meta(sample)

        registry_path = tmp_path / "relation_registry.json"
        monkeypatch.setattr(
            relation_store,
            "RELATION_REGISTRY_JSON",
            str(registry_path),
        )
        registry = {"cards": [], "sample": sample}
        relation_store.RelationStore().save_registry(registry)

        backup_path = tmp_path / "state_backups.jsonl"
        local_tz = RealDatetime.now().astimezone().tzinfo
        monkeypatch.setattr(
            state_backup_store,
            "local_now",
            lambda: RealDatetime(2026, 7, 18, 23, 45, 0, tzinfo=local_tz),
        )
        state_backup_store.StateBackupStore(str(backup_path)).append_backup(
            676,
            sample,
        )

        for path in (
                connectivity_path,
                heat_path,
                stm_path,
                ltm_path,
                relation_index_path,
                meta_path):
            assert path.read_bytes() == expected
        assert json.loads(config_path.read_text(encoding="utf-8"))["heartbeat"]["interval"] == 9
        assert registry_path.read_bytes() == encoded(json.dumps(
            registry,
            ensure_ascii=False,
            indent=2,
        ))
        assert backup_path.read_bytes() == encoded(
            json.dumps({
                "round": 676,
                "timestamp": "2026-07-18T23:45:00+08:00",
                "reason": "cleanup",
                "state": sample,
            }, ensure_ascii=False) + "\n"
        )
        assert not list(tmp_path.rglob("*.tmp"))

    def test_spec556_atomic_write_text_retries_transient_replace_error(
            self, tmp_path, monkeypatch):
        from data import atomic_write

        target = tmp_path / "message.md"
        calls = []
        original_replace = atomic_write.os.replace

        def flaky_replace(src, dst):
            calls.append((src, dst))
            if len(calls) == 1:
                raise PermissionError("transient lock")
            return original_replace(src, dst)

        monkeypatch.setattr(atomic_write.os, "replace", flaky_replace)
        monkeypatch.setattr(atomic_write.time, "sleep", lambda *_args: None)

        atomic_write.atomic_write_text(target, "ok")

        assert target.read_text(encoding="utf-8") == "ok"
        assert len(calls) == 2

    def test_spec556_atomic_write_text_cleans_tmp_on_permanent_failure(
            self, tmp_path, monkeypatch):
        from data import atomic_write
        from errors import WriteError

        target = tmp_path / "message.md"
        tmp_paths = []

        def failing_replace(src, _dst):
            tmp_paths.append(Path(src))
            raise PermissionError("locked")

        monkeypatch.setattr(atomic_write.os, "replace", failing_replace)
        monkeypatch.setattr(atomic_write.time, "sleep", lambda *_args: None)

        with pytest.raises(WriteError):
            atomic_write.atomic_write_text(target, "partial", replace_attempts=1)

        assert tmp_paths
        assert all(not path.exists() for path in tmp_paths)
        assert not target.exists()


class TestAuditStore:
    @staticmethod
    def _spec424_layers(**overrides):
        layers = {
            "permanent": "permanent",
            "periodic": "periodic",
            "lately": "lately",
            "high_freq": "high_freq",
            "now": "now",
            "statusbar": "statusbar",
            "popup": "popup",
            "full_system": "full system",
        }
        layers.update(overrides)
        return layers

    def test_spec586_audit_atomic_write_text_uses_shared_retry_helper(
            self, tmp_path, monkeypatch):
        from data import audit_store

        target = tmp_path / "audit.md"
        calls = []
        original_replace = audit_store.atomic_write.os.replace

        def flaky_replace(src, dst):
            calls.append((src, dst))
            if len(calls) == 1:
                raise PermissionError("transient lock")
            return original_replace(src, dst)

        monkeypatch.setattr(audit_store.atomic_write.os, "replace", flaky_replace)
        monkeypatch.setattr(audit_store.atomic_write.time, "sleep", lambda *_args: None)

        audit_store.AuditStore._atomic_write_text(str(target), "ok")

        assert target.read_text(encoding="utf-8") == "ok"
        assert len(calls) == 2

    def test_spec586_audit_atomic_write_json_uses_shared_retry_helper(
            self, tmp_path, monkeypatch):
        from data import audit_store

        target = tmp_path / "audit.json"
        calls = []
        original_replace = audit_store.atomic_write.os.replace

        def flaky_replace(src, dst):
            calls.append((src, dst))
            if len(calls) == 1:
                raise PermissionError("transient lock")
            return original_replace(src, dst)

        monkeypatch.setattr(audit_store.atomic_write.os, "replace", flaky_replace)
        monkeypatch.setattr(audit_store.atomic_write.time, "sleep", lambda *_args: None)

        audit_store.AuditStore._atomic_write_json(str(target), {"ok": "中文"})

        assert json.loads(target.read_text(encoding="utf-8")) == {"ok": "中文"}
        assert len(calls) == 2
        assert not list(tmp_path.glob("*.tmp"))

    def test_spec424_reuses_unchanged_machine_layers_without_rewrite(
            self, tmp_path, monkeypatch):
        from data import audit_store

        store = audit_store.AuditStore(
            reaction_dir=str(tmp_path / "context" / "reaction")
        )
        original_write_json = audit_store.AuditStore._atomic_write_json
        writes = []

        def spy_write_json(path, data):
            writes.append(Path(path).name)
            return original_write_json(path, data)

        monkeypatch.setattr(
            audit_store.AuditStore,
            "_atomic_write_json",
            staticmethod(spy_write_json),
        )

        store.write_audit("reaction", self._spec424_layers())
        assert "10_permanent.json" in writes

        writes.clear()
        store.write_audit("reaction", self._spec424_layers())

        layer_json_names = {
            "10_permanent.json",
            "20_periodic.json",
            "30_lately.json",
            "40_high_freq.json",
            "50_now.json",
            "60_statusbar.json",
            "99_popup.json",
        }
        assert not (layer_json_names & set(writes))

        manifest = json.loads(
            (tmp_path / "context" / "reaction" / "manifest.json")
            .read_text(encoding="utf-8")
        )
        assert all(
            layer["dirty"] is False and layer["reused"] is True
            for layer in manifest["layers"].values()
        )

        layer_payload = json.loads(
            (tmp_path / "context" / "reaction" / "layers" / "10_permanent.json")
            .read_text(encoding="utf-8")
        )
        assert "dirty" not in layer_payload
        assert "reused" not in layer_payload

    def test_spec424_marks_only_changed_machine_layer_dirty(
            self, tmp_path, monkeypatch):
        from data import audit_store

        store = audit_store.AuditStore(
            reaction_dir=str(tmp_path / "context" / "reaction")
        )
        original_write_json = audit_store.AuditStore._atomic_write_json
        writes = []

        def spy_write_json(path, data):
            writes.append(Path(path).name)
            return original_write_json(path, data)

        monkeypatch.setattr(
            audit_store.AuditStore,
            "_atomic_write_json",
            staticmethod(spy_write_json),
        )

        store.write_audit("reaction", self._spec424_layers())
        writes.clear()
        store.write_audit("reaction", self._spec424_layers(now="now changed"))

        assert "50_now.json" in writes
        assert "10_permanent.json" not in writes
        assert "20_periodic.json" not in writes
        assert "99_popup.json" not in writes

        manifest = json.loads(
            (tmp_path / "context" / "reaction" / "manifest.json")
            .read_text(encoding="utf-8")
        )
        assert manifest["layers"]["now"]["dirty"] is True
        assert manifest["layers"]["now"]["reused"] is False
        assert manifest["layers"]["permanent"]["dirty"] is False
        assert manifest["layers"]["permanent"]["reused"] is True

    def test_spec424_corrupt_existing_layer_is_rewritten_as_dirty(
            self, tmp_path, monkeypatch):
        from data import audit_store

        store = audit_store.AuditStore(
            reaction_dir=str(tmp_path / "context" / "reaction")
        )
        store.write_audit("reaction", self._spec424_layers())

        periodic_path = (
            tmp_path / "context" / "reaction" / "layers" / "20_periodic.json"
        )
        periodic_path.write_text("{broken json", encoding="utf-8")

        original_write_json = audit_store.AuditStore._atomic_write_json
        writes = []

        def spy_write_json(path, data):
            writes.append(Path(path).name)
            return original_write_json(path, data)

        monkeypatch.setattr(
            audit_store.AuditStore,
            "_atomic_write_json",
            staticmethod(spy_write_json),
        )

        store.write_audit("reaction", self._spec424_layers())

        assert "20_periodic.json" in writes
        manifest = json.loads(
            (tmp_path / "context" / "reaction" / "manifest.json")
            .read_text(encoding="utf-8")
        )
        assert manifest["layers"]["periodic"]["dirty"] is True
        assert manifest["layers"]["periodic"]["reused"] is False


# ============================================================
# StateStore 测试
# ============================================================

class TestStateStore:
    def test_default_when_no_file(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        with pytest.raises(ReadError):
            store.load()

    def test_read_snapshot_rejects_incomplete_state_without_backfill(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError
        from schemas.state import default_state

        path = tmp_path / "state.json"
        state = default_state()
        state["base"]["heartbeat_flags"].pop("evolution_pending")
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
        before = path.read_bytes()

        with pytest.raises(ReadError):
            StateStore(str(path)).read_snapshot()
        assert path.read_bytes() == before

    def test_read_snapshot_rejects_missing_malformed_and_non_object(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError

        path = tmp_path / "state.json"
        store = StateStore(str(path))
        with pytest.raises(ReadError):
            store.read_snapshot()
        path.write_text("{", encoding="utf-8")
        with pytest.raises(ReadError):
            store.read_snapshot()
        path.write_text("[]", encoding="utf-8")
        with pytest.raises(ReadError):
            store.read_snapshot()

    def test_save_and_load(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        state = store.load()
        state["base"]["meta"]["total_round"] = 5
        store.save(state)
        loaded = store.load()
        assert loaded["base"]["meta"]["total_round"] == 5

    def test_load_backfills_missing_heartbeat_flags(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError
        from schemas.state import default_state
        path = tmp_path / "state.json"
        state = default_state()
        state["base"]["heartbeat_flags"].pop("evolution_pending")
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

        with pytest.raises(ReadError):
            StateStore(str(path)).load()

    def test_load_backfills_unbound_interaction_anchor_without_guessing(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError
        from schemas.state import default_state
        path = tmp_path / "state.json"
        state = default_state()
        for key in (
                "local_default_relation_id", "current_relation_id",
                "current_declared_name", "current_source"):
            state["base"]["identity"].pop(key)
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

        with pytest.raises(ReadError):
            StateStore(str(path)).load()

    def test_load_removes_legacy_next_round_from_runtime(self, tmp_path):
        from data.state_store import StateStore
        from schemas.state import default_state
        path = tmp_path / "state.json"
        state = default_state()
        state["base"]["runtime"]["next_round"] = {
            "type": "relay",
            "subtype": None,
            "brief": "旧便签不再作为调度输入",
        }
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

        before = path.read_bytes()
        loaded = StateStore(str(path)).load()
        assert loaded["base"]["runtime"]["next_round"]["type"] == "relay"
        assert path.read_bytes() == before

    def test_load_removes_retired_base_state_and_context_flags(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError
        from schemas.state import default_state

        path = tmp_path / "state.json"
        state = default_state()
        state["base"]["state"] = {"daily_round": "34", "fatigue": "increased"}
        state["base"]["relation_focus"] = "不更新"
        state["base"]["daily_round"] = "不变"
        state["base"]["context_cache"]["near_cache_expired"] = True
        state["base"]["context_cache"]["remote_cache_expired"] = True
        state["base"].pop("focus")
        state["base"].pop("old_focus")
        state["base"].pop("feeling_buffer")
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

        with pytest.raises(ReadError):
            StateStore(str(path)).load()

    def test_get_by_dotpath(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        assert store.get("base.meta.total_round") == 0
        assert store.get("nonexistent.key", "fallback") == "fallback"

    def test_set_field(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.set("base.activity_mode", "创作")
        assert store.get("base.activity_mode") == "创作"

    def test_update_many(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.update_many({
            "base.meta.total_round": 10,
            "base.activity_mode": "工程",
        })
        assert store.get("base.meta.total_round") == 10
        assert store.get("base.activity_mode") == "工程"

    def test_phase_operations(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.set_phase("main")
        assert store.get_phase() == "main"
        with pytest.raises(ValueError):
            store.set_phase("INVALID")

    def test_flag_operations(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.set_flag("user_message_waiting", True)
        flags = store.get_flags()
        assert flags["user_message_waiting"] is True

    def test_clear_flags(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.set_flag("rhythm_due", True)
        store.set_flag("standby_due", True)
        store.clear_flags(["rhythm_due", "standby_due"])
        flags = store.get_flags()
        assert flags["rhythm_due"] is False
        assert flags["standby_due"] is False

    def test_increment_round(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        n = store.increment_round()
        assert n == 1
        assert store.get_total_round() == 1

    def test_init_if_missing_only_once(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        assert store.init_if_missing() is True
        assert store.init_if_missing() is False  # 已存在

    def test_load_repairs_legacy_dynamic_axes_and_top_level_pollution(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError

        path = tmp_path / "state.json"
        path.write_text(json.dumps({
            "base": {
                "meta": {"total_round": 9, "daily_round": 1},
                "dynamic_axes": {
                    "valence": {"value": "+1"},
                    "arousal": {"value": "高"},
                },
                "runtime": {"next_round": {"type": "interactive"}},
                "heartbeat_flags": {},
            },
            "state": "旧 LLM 输出污染",
            "flags": {"unknown": True},
        }, ensure_ascii=False), encoding="utf-8")

        with pytest.raises(ReadError):
            StateStore(str(path)).load()

    def test_load_repairs_bool_dynamic_axis_value(self, tmp_path):
        from data.state_store import StateStore
        from errors import ReadError

        path = tmp_path / "state.json"
        path.write_text(json.dumps({
            "base": {
                "dynamic_axes": {
                    "valence": {"value": True},
                    "arousal": {"value": -100},
                    "focus": {"value": 100},
                    "mood": {"value": None},
                    "humor": {"value": 101},
                    "safety": {"value": -101},
                },
                "runtime": {},
                "heartbeat_flags": {},
            },
        }, ensure_ascii=False), encoding="utf-8")

        with pytest.raises(ReadError):
            StateStore(str(path)).load()


class TestStateBackupStore:
    def test_append_backup_keeps_jsonl_fifo_by_round(self, tmp_path):
        from data.state_backup_store import StateBackupStore

        path = tmp_path / "state_backups.jsonl"
        store = StateBackupStore(str(path), retention_count=3)

        for round_num in (1, 2, 10, 11):
            store.append_backup(
                round_num,
                {"base": {"meta": {"total_round": round_num}}},
                reason="cleanup",
            )

        rows = store.read_backups()

        assert [row["round"] for row in rows] == [2, 10, 11]
        assert rows[-1]["state"]["base"]["meta"]["total_round"] == 11
        assert rows[-1]["reason"] == "cleanup"
        assert not (tmp_path / "state_backups.jsonl.tmp").exists()


class TestConnectivityStore:
    def test_latest_ok_for_same_endpoint_clears_old_error(self, tmp_path):
        from data.connectivity_store import ConnectivityStore

        store = ConnectivityStore(str(tmp_path / "connectivity.json"))

        store.log_latency("primary", "error", "temporary outage")
        assert store.has_degraded() is True

        store.log_latency("primary", "ok", "recovered")

        assert store.has_degraded() is False

    def test_other_endpoint_latest_error_still_degrades(self, tmp_path):
        from data.connectivity_store import ConnectivityStore

        store = ConnectivityStore(str(tmp_path / "connectivity.json"))
        store.log_latency("primary", "error", "old primary outage")
        store.log_latency("primary", "ok", "primary recovered")
        store.log_latency("fallback", "timeout", "fallback still down")

        assert store.has_degraded() is True

    def test_success_status_is_normalized_to_ok(self, tmp_path):
        from data.connectivity_store import ConnectivityStore

        store = ConnectivityStore(str(tmp_path / "connectivity.json"))

        store.log_latency("primary", "success", "provider returned response")

        data = store.load()
        assert data["recent_latencies"][0]["status"] == "ok"
        assert store.has_degraded() is False

    def test_non_api_step_endpoint_error_does_not_degrade_api(self, tmp_path):
        from data.connectivity_store import ConnectivityStore

        store = ConnectivityStore(str(tmp_path / "connectivity.json"))

        store.log_latency("cleanup", "error", "L3 cleanup API failure")

        assert store.latest_status_by_endpoint()["cleanup"] == "error"
        assert store.has_degraded() is False

    def test_spec700_only_active_model_profiles_can_hold_api_degraded(self, tmp_path):
        from data.connectivity_store import ConnectivityStore

        active = ["model_active"]
        store = ConnectivityStore(
            str(tmp_path / "connectivity.json"),
            active_endpoint_ids=lambda: active,
        )
        store.log_latency("model_stale", "error", "unused model failed")
        store.log_latency("model_active", "ok", "current route recovered")

        assert store.active_statuses() == ["ok"]
        assert store.has_degraded() is False

        active.clear()
        assert store.active_statuses() == []
        assert store.has_degraded() is False


class TestPeriodicMountStore:
    def test_missing_file_returns_structured_default(self, tmp_path):
        from data.periodic_mount_store import PeriodicMountStore

        store = PeriodicMountStore(str(tmp_path / "periodic_mounts.json"))

        assert store.load() == {
            "periodic_memory_items": [],
        }

    def test_save_ids_writes_structured_mounts_atomically(self, tmp_path):
        from data.periodic_mount_store import PeriodicMountStore

        path = tmp_path / "periodic_mounts.json"
        store = PeriodicMountStore(str(path), now_fn=lambda: "2026-05-22T00:00:00+08:00")

        store.save_ids(["MEM-1"])
        data = store.load()

        assert data["updated_at"] == "2026-05-22T00:00:00+08:00"
        assert data["periodic_memory_items"] == [{"id": "MEM-1", "rendered_text": "MEM-1"}]
        assert not path.with_suffix(path.suffix + ".tmp").exists()

    def test_load_preserves_unknown_mount_shape_without_compat_rewrite(self, tmp_path):
        from data.periodic_mount_store import PeriodicMountStore

        path = tmp_path / "periodic_mounts.json"
        path.write_text(
            json.dumps({"memories": ["MEM-1"], "skill_tools": ["SKL-habits-a"]}),
            encoding="utf-8",
        )
        store = PeriodicMountStore(str(path))

        assert store.load() == {"memories": ["MEM-1"], "skill_tools": ["SKL-habits-a"]}


class TestStateStoreContinued:
    def test_no_tmp_left_behind(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.set("base.meta.total_round", 42)
        tmps = list(tmp_path.glob("*.tmp"))
        assert len(tmps) == 0, f"残留 tmp 文件: {tmps}"

    def test_token_usage_update(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.update_token_usage(50000, 200000, 0.25, 3000, 2000)
        assert store.get("base.token_usage.usage_ratio") == 0.25

    def test_identity_confirm(self, tmp_path):
        from data.state_store import StateStore
        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        store.confirm_identity()
        assert store.get("base.identity.confirmed") is True
        assert store.get("base.identity.confirmed_at")
        assert store.get("base.heartbeat_flags.identity_timeout") is False

    def test_identity_timeout_uses_confirmed_at_before_external_input(
            self, tmp_path, monkeypatch):
        from datetime import timedelta
        from constants import local_now
        from data.state_store import StateStore
        from engines.heartbeat import HeartbeatManager

        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        now = local_now()
        old_input = (now - timedelta(hours=2)).isoformat()
        fresh_confirm = now.isoformat()
        store.update_many({
            "base.meta.last_external_input_at": old_input,
            "base.identity.confirmed": True,
            "base.identity.confirmed_at": fresh_confirm,
            "base.identity.timeout_seconds": 3600,
            "base.heartbeat_flags.identity_timeout": False,
        })
        monkeypatch.setattr(
            HeartbeatManager,
            "_check_api_degraded",
            lambda self: False,
        )

        manager = HeartbeatManager(state_store=store)

        manager._do_tick()

        assert store.get("base.heartbeat_flags.identity_timeout") is False

    def test_identity_timeout_does_not_reflag_after_confirmed_at_expires(
            self, tmp_path, monkeypatch):
        from datetime import timedelta
        from constants import local_now
        from data.state_store import StateStore
        from engines.heartbeat import HeartbeatManager

        path = tmp_path / "state.json"
        store = StateStore(str(path))
        store.init_if_missing()
        now = local_now()
        old_confirm = (now - timedelta(hours=2)).isoformat()
        store.update_many({
            "base.meta.last_external_input_at": now.isoformat(),
            "base.identity.confirmed": True,
            "base.identity.confirmed_at": old_confirm,
            "base.identity.timeout_seconds": 3600,
            "base.heartbeat_flags.identity_timeout": False,
        })
        monkeypatch.setattr(
            HeartbeatManager,
            "_check_api_degraded",
            lambda self: False,
        )

        manager = HeartbeatManager(state_store=store)

        manager._do_tick()

        assert store.get("base.heartbeat_flags.identity_timeout") is False
        assert store.get("base.identity.confirmed") is True
        assert store.get("base.identity.confirmed_at") == old_confirm


# ============================================================
# MemoryStore 测试
# ============================================================

class TestRoundSnapshotStore:
    def test_close_round_is_not_reversed_by_post_close_prune_failure(
            self, tmp_path, monkeypatch):
        from data.round_snapshot_store import RoundSnapshotStore

        store = RoundSnapshotStore(str(tmp_path / "context"))
        store.start_round(1, "interactive")

        def fail_prune():
            raise OSError("retention unavailable")

        monkeypatch.setattr(store, "prune", fail_prune)

        path = store.close_round(1, final_response="done")

        assert Path(path).is_file()
        assert store.read_events(1)[-1]["event_type"] == "round_closed"

    def test_spec631_reaction_popup_snapshot_status_requires_one_textual_layer(self):
        from data.round_snapshot_store import reaction_popup_snapshot_status

        complete = {
            "event_type": "step_input_snapshot",
            "phase": "reaction",
            "payload": {
                "layers_snapshot": {
                    "layers": [{"layer_key": "99_popup", "content": ""}],
                },
            },
        }

        assert reaction_popup_snapshot_status(complete) == "complete"
        assert reaction_popup_snapshot_status({
            **complete,
            "payload": {"layers_snapshot": {"layers": []}},
        }) == "popup_layer_missing"
        assert reaction_popup_snapshot_status({
            **complete,
            "payload": {"layers_snapshot": {"layers": [
                {"layer_key": "99_popup", "content": "one"},
                {"layer_key": "99_popup", "content": "two"},
            ]}},
        }) == "popup_layer_ambiguous"
        assert reaction_popup_snapshot_status({
            **complete,
            "payload": {"layers_snapshot": {"layers": [
                {"layer_key": "99_popup", "content": None},
            ]}},
        }) == "popup_content_missing"

    def _read_events(self, path):
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    def test_append_event_assigns_unique_indexes_across_threads(self, tmp_path):
        from concurrent.futures import ThreadPoolExecutor
        from data.round_snapshot_store import RoundSnapshotStore

        store = RoundSnapshotStore(str(tmp_path / "context"))
        with ThreadPoolExecutor(max_workers=8) as pool:
            list(pool.map(
                lambda index: store.append_event(7, "parallel", {"i": index}),
                range(40),
            ))

        events = store.read_events(7)
        assert [event["event_index"] for event in events] == list(range(1, 41))
        assert len({event["event_id"] for event in events}) == 40

    def test_write_snapshot_rejects_legacy_list_step_json_files(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        for step in ("setup", "reaction", "cleanup"):
            step_dir = context / step
            step_dir.mkdir(parents=True)
            (step_dir / "step.json").write_text(
                json.dumps([{"role": "system", "content": step}], ensure_ascii=False),
                encoding="utf-8",
            )
        store = RoundSnapshotStore(str(context), retention_count=8)

        path = store.write_snapshot(3)
        jsonl_path = context / "round" / "round_3.jsonl"
        events = self._read_events(jsonl_path)
        step_events = [
            event for event in events
            if event["event_type"] == "step_input_snapshot"
        ]

        assert path == str(jsonl_path)
        assert [event["phase"] for event in step_events] == [
            "setup",
            "reaction",
            "cleanup",
        ]
        assert [event["payload"]["messages"] for event in step_events] == [[], [], []]
        assert [
            event["payload"]["error"]
            for event in step_events
        ] == ["legacy_step_json_rejected"] * 3
        assert [
            event["payload"]["legacy_step_json_format"]
            for event in step_events
        ] == ["messages_list"] * 3
        assert all(event["payload"]["historical"] is False for event in step_events)
        assert events[-1]["event_type"] == "round_closed"
        assert not list(context.rglob("*.tmp"))

    def test_write_snapshot_skips_missing_steps_without_crashing(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        setup = context / "setup"
        setup.mkdir(parents=True)
        (setup / "step.json").write_text(
            "[{\"role\":\"system\",\"content\":\"setup\"}]",
            encoding="utf-8",
        )
        store = RoundSnapshotStore(str(context), retention_count=8)

        store.write_snapshot(4)
        events = self._read_events(context / "round" / "round_4.jsonl")
        step_events = [
            event for event in events
            if event["event_type"] == "step_input_snapshot"
        ]

        assert [event["phase"] for event in step_events] == ["setup"]
        assert step_events[0]["payload"]["error"] == "legacy_step_json_rejected"

    def test_record_step_input_rejects_legacy_step_json_even_with_messages(
            self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        reaction = context / "reaction"
        reaction.mkdir(parents=True)
        (reaction / "step.json").write_text(
            json.dumps([{"role": "system", "content": "legacy"}], ensure_ascii=False),
            encoding="utf-8",
        )
        store = RoundSnapshotStore(str(context), retention_count=8)

        store.record_step_input_from_files(
            425,
            "reaction",
            iteration=1,
            messages=[{"role": "user", "content": "runtime messages"}],
            system="runtime system",
        )

        events = self._read_events(context / "round" / "round_425.jsonl")
        event = next(
            item for item in events
            if item["event_type"] == "step_input_snapshot"
        )
        assert event["payload"]["messages"] == []
        assert event["payload"]["error"] == "legacy_step_json_rejected"
        assert event["payload"]["legacy_step_json_format"] == "messages_list"
        assert event["payload"]["historical"] is False

    def test_spec427_record_step_input_includes_layers_snapshot(
            self, tmp_path):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        from data.round_snapshot_store import RoundSnapshotStore

        class NoopConnectivity:
            def log_latency(self, endpoint, status, message=""):
                pass

        class Config(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/chat/completions",
                            "model": "unit",
                            "provider": "openai_chat",
                            "api_key": "secret",
                            "extra_body": {"seed": 427},
                        },
                    },
                    "handshake": {"retry": 0, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

            def get_request_timeout(self):
                return 300

        context = tmp_path / "context"
        audit = AuditStore(reaction_dir=str(context / "reaction"))
        audit.write_audit("reaction", {
            "permanent": "permanent snapshot",
            "periodic": "periodic snapshot",
            "lately": "lately snapshot",
            "high_freq": "high freq snapshot",
            "now": "now snapshot",
            "statusbar": "statusbar snapshot",
            "popup": "popup snapshot",
            "full_system": "full system snapshot",
        })
        executor = APIExecutor(
            Config(),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context),
        )
        executor.prepare_provider_request(
            "reaction",
            "runtime system",
            [{"role": "user", "content": "runtime message"}],
        )
        store = RoundSnapshotStore(str(context), retention_count=8)

        store.record_step_input_from_files(427, "reaction", iteration=1)

        events = self._read_events(context / "round" / "round_427.jsonl")
        payload = events[0]["payload"]
        snapshot = payload["layers_snapshot"]
        assert snapshot["schema"] == "context_layers_snapshot.v1"
        assert snapshot["source"] == "context/reaction/layers"
        assert [layer["layer_key"] for layer in snapshot["layers"]] == [
            "00_call_header",
            "01_tool_header",
            "02_generation_config",
            "10_permanent",
            "20_periodic",
            "30_lately",
            "40_high_freq",
            "50_now",
            "60_statusbar",
            "99_popup",
        ]
        by_key = {layer["layer_key"]: layer for layer in snapshot["layers"]}
        assert by_key["50_now"]["content"] == "now snapshot"
        assert by_key["02_generation_config"]["content"]["seed"] == 427

    def test_write_snapshot_respects_executed_phases(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        for step in ("setup", "reaction", "cleanup"):
            step_dir = context / step
            step_dir.mkdir(parents=True)
            (step_dir / "step.json").write_text(
                json.dumps([{"role": "system", "content": f"old {step}"}], ensure_ascii=False),
                encoding="utf-8",
            )
        store = RoundSnapshotStore(str(context), retention_count=8)

        store.write_snapshot(5, executed_phases=("setup", "cleanup"))

        events = self._read_events(context / "round" / "round_5.jsonl")
        step_events = [
            event for event in events
            if event["event_type"] == "step_input_snapshot"
        ]
        assert [event["phase"] for event in step_events] == ["setup", "cleanup"]
        assert "reaction" not in [event["phase"] for event in step_events]

    def test_write_snapshot_can_include_runtime_audit(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        reaction = context / "reaction"
        reaction.mkdir(parents=True)
        (reaction / "step.json").write_text(
            "[{\"role\":\"system\",\"content\":\"reaction\"}]",
            encoding="utf-8",
        )
        store = RoundSnapshotStore(str(context), retention_count=8)

        store.write_snapshot(67, runtime={
            "tool_transaction_audit": {
                "status": "ok",
                "counts": {"issues": 0},
                "issues": [],
            }
        })
        events = self._read_events(context / "round" / "round_67.jsonl")
        runtime = next(event for event in events if event["event_type"] == "runtime_audit")
        reaction = next(
            event for event in events
            if event["event_type"] == "step_input_snapshot"
            and event["phase"] == "reaction"
        )

        assert runtime["payload"]["tool_transaction_audit"]["status"] == "ok"
        assert reaction["payload"]["messages"] == []
        assert reaction["payload"]["error"] == "legacy_step_json_rejected"

    def test_write_snapshot_without_runtime_writes_round_close_event(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        audit = tmp_path / "audit"
        cleanup = context / "cleanup"
        cleanup.mkdir(parents=True)
        (cleanup / "step.json").write_text(
            "[{\"role\":\"system\",\"content\":\"cleanup\"}]",
            encoding="utf-8",
        )
        store = RoundSnapshotStore(str(context), retention_count=8, static_audit_dir=str(audit))

        store.write_snapshot(68)
        events = self._read_events(context / "round" / "round_68.jsonl")

        assert [event["event_type"] for event in events] == [
            "step_input_snapshot",
            "round_closed",
        ]
        assert events[-1]["payload"]["status"] == "closed"
        assert (audit / "round-index.js").is_file()
        assert (audit / "round-data" / "round_68.js").is_file()

    def test_write_snapshot_static_projection_failure_does_not_break_round_close(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        cleanup = context / "cleanup"
        cleanup.mkdir(parents=True)
        (cleanup / "step.json").write_text(
            "[{\"role\":\"system\",\"content\":\"cleanup\"}]",
            encoding="utf-8",
        )
        store = RoundSnapshotStore(
            str(context),
            retention_count=8,
            static_audit_dir=str(tmp_path / "not-a-dir" / "child"),
        )
        store._write_static_projection = lambda: (_ for _ in ()).throw(RuntimeError("boom"))

        store.write_snapshot(69)
        events = self._read_events(context / "round" / "round_69.jsonl")

        assert events[-1]["event_type"] == "round_closed"

    def test_prune_keeps_latest_round_jsonl_snapshots_and_leaves_old_json(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        round_dir = context / "round"
        round_dir.mkdir(parents=True)
        for round_num in (1, 2, 10):
            (round_dir / f"round_{round_num}.jsonl").write_text(
                '{"event_type":"round_closed"}\n',
                encoding="utf-8",
            )
        (round_dir / "round_0.json").write_text("{}", encoding="utf-8")
        store = RoundSnapshotStore(str(context), retention_count=2)

        store.prune()

        remaining = sorted(
            (path.name for path in round_dir.glob("round_*.jsonl")),
            key=lambda name: int(name[6:-6]),
        )
        assert remaining == [
            "round_2.jsonl",
            "round_10.jsonl",
        ]
        assert (round_dir / "round_0.json").is_file()

    def test_list_rounds_uses_only_jsonl_files(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        context = tmp_path / "context"
        round_dir = context / "round"
        round_dir.mkdir(parents=True)
        (round_dir / "round_1.json").write_text("{}", encoding="utf-8")
        (round_dir / "round_2.jsonl").write_text(
            '{"event_type":"round_closed"}\n',
            encoding="utf-8",
        )
        (round_dir / "round_10.jsonl").write_text(
            '{"event_type":"round_closed"}\n',
            encoding="utf-8",
        )
        store = RoundSnapshotStore(str(context), retention_count=8)

        rounds = store.list_rounds()

        assert [item["round"] for item in rounds] == [2, 10]


class TestRoundAuditViewerData:
    def test_viewer_lists_only_existing_jsonl_rounds(self, tmp_path):
        from data.round_audit_viewer import list_rounds

        round_dir = tmp_path / "round"
        round_dir.mkdir()
        (round_dir / "round_1.json").write_text("{}", encoding="utf-8")
        (round_dir / "round_2.jsonl").write_text(
            '{"event_type":"round_closed"}\n',
            encoding="utf-8",
        )
        (round_dir / "round_abc.jsonl").write_text("", encoding="utf-8")

        rounds = list_rounds(str(round_dir))

        assert [item["round"] for item in rounds] == [2]

    def test_viewer_load_round_events_rejects_missing_or_pathlike_round(self, tmp_path):
        from data.round_audit_viewer import load_round_events

        round_dir = tmp_path / "round"
        round_dir.mkdir()
        (round_dir / "round_2.jsonl").write_text(
            '{"event_type":"round_closed","round":2}\n',
            encoding="utf-8",
        )

        assert load_round_events(str(round_dir), 2)[0]["event_type"] == "round_closed"
        with pytest.raises(FileNotFoundError):
            load_round_events(str(round_dir), 1)
        with pytest.raises(ValueError):
            load_round_events(str(round_dir), "../2")


class TestRoundAuditServerRoutes:
    def test_server_serves_root_and_repository_audit_routes(self, tmp_path):
        repo_root = Path(__file__).resolve().parents[3]
        module_path = repo_root / "tools" / "serve_round_audit.py"
        module = _load_module_from_path('serve_round_audit', module_path)

        round_dir = tmp_path / "round"
        audit_dir = tmp_path / "audit"
        data_dir = audit_dir / "round-data"
        round_dir.mkdir()
        data_dir.mkdir(parents=True)
        html_path = audit_dir / "round.html"
        html_path.write_text("<!doctype html><title>Round Audit</title>", encoding="utf-8")
        (audit_dir / "round-index.js").write_text("window.UPSP_ROUND_AUDIT_INDEX={rounds:[]};", encoding="utf-8")
        (data_dir / "round_2.js").write_text("window.UPSP_ROUND_AUDIT_ROUNDS={2:{}};", encoding="utf-8")
        (round_dir / "round_2.jsonl").write_text(
            '{"event_type":"round_closed","round":2}\n',
            encoding="utf-8",
        )

        server = module.make_server("127.0.0.1", 0, round_dir, html_path)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        host, port = server.server_address
        base_url = f"http://{host}:{port}"

        try:
            paths = [
                "/round.html",
                "/UPSP/OS/audit/round.html",
                "/round-index.js",
                "/UPSP/OS/audit/round-index.js",
                "/round-data/round_2.js",
                "/UPSP/OS/audit/round-data/round_2.js",
            ]
            for path in paths:
                with urlopen(f"{base_url}{path}", timeout=5) as response:
                    assert response.status == 200
                    assert response.read()
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)


class TestRoundAuditStaticProjection:
    def test_static_projection_writes_index_and_round_data_from_jsonl(self, tmp_path):
        from data.round_audit_static import write_static_projection

        round_dir = tmp_path / "round"
        audit_dir = tmp_path / "audit"
        round_dir.mkdir()
        (round_dir / "round_1.json").write_text("{}", encoding="utf-8")
        (round_dir / "round_2.jsonl").write_text(
            "\n".join([
                json.dumps({
                    "event_type": "step_input_snapshot",
                    "phase": "setup",
                    "iteration": 1,
                    "payload": {"messages": [{"role": "system", "content": "永固层"}]},
                }, ensure_ascii=False),
                json.dumps({
                    "event_type": "llm_output_raw",
                    "phase": "setup",
                    "iteration": 1,
                    "payload": {"response": "setup output"},
                }, ensure_ascii=False),
            ]) + "\n",
            encoding="utf-8",
        )

        index = write_static_projection(str(round_dir), str(audit_dir))

        index_text = (audit_dir / "round-index.js").read_text(encoding="utf-8")
        data_text = (audit_dir / "round-data" / "round_2.js").read_text(encoding="utf-8")
        assert [item["round"] for item in index["rounds"]] == [2]
        assert "UPSP_ROUND_AUDIT_INDEX" in index_text
        assert "UPSP_ROUND_AUDIT_ROUNDS[2]" in data_text
        assert "setup output" in data_text
        assert "timeline" in data_text
        assert not (audit_dir / "round-data" / "round_1.js").exists()

    def test_static_projection_prunes_stale_round_js(self, tmp_path):
        from data.round_audit_static import write_static_projection

        round_dir = tmp_path / "round"
        audit_dir = tmp_path / "audit"
        data_dir = audit_dir / "round-data"
        round_dir.mkdir()
        data_dir.mkdir(parents=True)
        (round_dir / "round_4.jsonl").write_text(
            '{"event_type":"round_closed","payload":{"status":"closed"}}\n',
            encoding="utf-8",
        )
        (data_dir / "round_3.js").write_text("stale", encoding="utf-8")

        write_static_projection(str(round_dir), str(audit_dir))

        assert not (data_dir / "round_3.js").exists()
        assert (data_dir / "round_4.js").is_file()


class TestMemoryStore:
    def test_write_entry_signature_has_no_retired_abstract_argument(self):
        from data.memory_store import MemoryStore

        signature = inspect.signature(MemoryStore.write_entry)

        assert "abstract" not in signature.parameters

    def test_write_and_read_entry(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "测试记忆", "这是一条测试", weight=5)
        content = store.read_entry("MEM-00001001")
        assert "测试记忆" in content
        assert "MEM-00001001" in content

    def test_write_entry_uses_structured_first_person_fields(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry(
            "MEM-00001001",
            "规则落地",
            "我确认记忆写入规则需要落到正文表单，只记录结论与变化。",
            weight=5,
            subject="TzPz",
            round_num=24,
        )

        content = store.read_entry("MEM-00001001")
        assert "**交互对象**：TzPz" in content
        assert "**入库**：第24轮" in content
        assert "**最后调用**：第24轮" in content
        assert "**标题**：规则落地" in content
        assert "梦源：否" in content
        assert "现状概况：" in content
        assert "**梗概**" not in content
        assert "**内容**" in content
        assert "我确认记忆写入规则需要落到正文表单" in content
        assert "Δ动态" not in content

    def test_spec224_weight_two_rejects_body_over_128_without_writing(
            self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()

        with pytest.raises(ValueError, match="memory_body_too_long"):
            store.write_entry(
                "MEM-00001001",
                "低权重超限",
                "甲" * 129,
                weight=2,
            )

        assert not (tmp_path / "memory.md").exists()

    def test_spec224_weight_two_allows_exact_128_without_ellipsis(
            self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        body = "乙" * 128
        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "低权重上限", body, weight=2)

        content = store.read_entry("MEM-00001001")
        assert body in content
        assert "…" not in content
        assert "**正文**（≤128字）" in content

    def test_spec224_weight_boundaries_reject_over_limit(
            self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()

        with pytest.raises(ValueError, match="max=512;actual=513"):
            store.write_entry("MEM-00002001", "三级超限", "丙" * 513, weight=3)
        with pytest.raises(ValueError, match="max=512;actual=513"):
            store.write_entry("MEM-00002002", "四级超限", "丁" * 513, weight=4)
        with pytest.raises(ValueError, match="max=2048;actual=2049"):
            store.write_entry("MEM-00002003", "五级超限", "戊" * 2049, weight=5)

    def test_list_entries(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "标题1")
        store.write_entry("MEM-00002001", "标题2")
        entries = store.list_entries()
        assert "MEM-00001001" in entries
        assert "MEM-00002001" in entries

    def test_entry_not_found(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        from errors import EntryNotFoundError
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        with pytest.raises(EntryNotFoundError):
            store.read_entry("MEM-DEADBEEF")

    def test_meta_crud(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.set_meta("MEM-TEST01", {"title": "测试", "weight": 5})
        meta = store.get_meta("MEM-TEST01")
        assert meta["title"] == "测试"

    def test_meta_default_for_new_entry(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        meta = store.get_meta("MEM-NONEXIST")
        assert meta["id"] == "MEM-NONEXIST"
        assert "weight" in meta

    def test_append_index(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.append_index("MEM-TEST01", "F", 5, "测试标题")
        lines = store.read_index()
        assert any("MEM-TEST01" in l for l in lines)

    def test_write_entry_defaults_dream_false_and_empty_overview(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "测试记忆", "这是一条测试", weight=5)

        content = store.read_entry("MEM-00001001")
        assert "梦源：否" in content
        assert "现状概况：" in content
        assert "注释：" not in content
        assert "梗概" not in content

    def test_append_index_writes_dream_and_overview_columns(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.append_index(
            "MEM-TEST01", "F", 5, "测试标题",
            dream=True,
            current_overview="已在 DC-12 中订正为过时判断",
        )

        index_text = (tmp_path / "index.md").read_text(encoding="utf-8")
        assert "梦源" in index_text
        assert "现状概况" in index_text
        assert "注释" not in index_text
        assert "| 是 |" in index_text
        assert "已在 DC-12 中订正为过时判断" in index_text

    def test_update_entry_title_and_body_syncs_index(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "旧标题", "原始正文", weight=5)
        store.append_index("MEM-00001001", "F", 5, "旧标题")

        store.update_entry_title_and_body(
            "MEM-00001001",
            "旧标题[召回补全内容]",
            "基于证据包重写后的完整正文",
        )

        body = store.read_entry("MEM-00001001")
        index_text = (tmp_path / "index.md").read_text(encoding="utf-8")
        assert "旧标题[召回补全内容]" in body
        assert "基于证据包重写后的完整正文" in body
        assert "旧标题[召回补全内容]" in index_text

# ============================================================
# TrainingMaterialStore 测试
# ============================================================


    def test_read_body_by_id_returns_public_body(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "Body Read", "full body text", weight=5)
        store.set_meta("MEM-00001001", {
            "id": "MEM-00001001",
            "title": "Body Read",
            "access": "public",
            "subject": "TzPz",
        })

        result = store.read_body_by_id("MEM-00001001")

        assert result["mem_id"] == "MEM-00001001"
        assert result["meta"]["title"] == "Body Read"
        assert "Body Read" in result["body"]
        assert result["read_mode"] == "full"
        assert result["range_requested"] is None
        assert result["range_applied"] is None
        assert result["total_chars"] == len(result["body"])

    def test_spec220_read_body_by_id_reads_ltm_summary_body(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "stm_memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "stm_meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "stm_index.md"))

        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        summary_dir.mkdir(parents=True)
        summary_md = summary_dir / "summary.md"
        summary_meta = summary_dir / "meta.json"
        monkeypatch.setattr(ms, "LTM_SUMMARY_SUMMARY_MD", str(summary_md), raising=False)
        monkeypatch.setattr(ms, "LTM_SUMMARY_META_JSON", str(summary_meta), raising=False)

        mem_id = "MEM-0ABC2201"
        summary_md.write_text(
            "\n## MEM-0ABC2201  [S]  权重3\n"
            "标题：LTM Summary Body\n"
            "摘要：这是一条只存在于 LTM Summary 的正文。\n",
            encoding="utf-8",
        )
        summary_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id,
                "title": "LTM Summary Body",
                "access": "public",
                "subject": "Codex",
                "type": "S",
                "weight": 3,
            }
        }, ensure_ascii=False), encoding="utf-8")

        result = ms.MemoryStore().read_body_by_id(mem_id)

        assert result["mem_id"] == mem_id
        assert result["memory_layer"] == "LTM/Summary"
        assert result["meta"]["title"] == "LTM Summary Body"
        assert "只存在于 LTM Summary" in result["body"]
        assert result["read_mode"] == "full"
        assert result["total_chars"] == len(result["body"])

    def test_spec220_read_body_by_id_reads_ltm_abstract_body(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "stm_memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "stm_meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "stm_index.md"))

        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        abstract_dir.mkdir(parents=True)
        abstract_md = abstract_dir / "abstract.md"
        abstract_meta = abstract_dir / "meta.json"
        monkeypatch.setattr(ms, "LTM_ABSTRACT_ABSTRACT_MD", str(abstract_md), raising=False)
        monkeypatch.setattr(ms, "LTM_ABSTRACT_META_JSON", str(abstract_meta), raising=False)

        mem_id = "MEM-0ABC2202"
        abstract_md.write_text(
            "\n## MEM-0ABC2202  [A]  权重2\n"
            "标题：LTM Abstract Body\n"
            "梗概：这是一条只存在于 LTM Abstract 的正文。\n",
            encoding="utf-8",
        )
        abstract_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id,
                "title": "LTM Abstract Body",
                "access": "public",
                "subject": "Codex",
                "type": "A",
                "weight": 2,
            }
        }, ensure_ascii=False), encoding="utf-8")

        result = ms.MemoryStore().read_body_by_id(
            mem_id,
            char_start=1,
            char_end=48,
        )

        assert result["memory_layer"] == "LTM/Abstract"
        assert result["read_mode"] == "partial"
        assert result["range_requested"] == {
            "type": "char",
            "char_start": 1,
            "char_end": 48,
        }
        assert result["total_chars"] > len(result["body"])
        assert result["body"]

    def test_update_linked_containers_adds_and_removes_refs(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "Bridge Test", "body", weight=5)
        store.set_meta("MEM-00001001", {
            "id": "MEM-00001001",
            "title": "Bridge Test",
            "linked_containers": [],
        })

        updated = store.update_linked_containers(
            "MEM-00001001", operation="add", container_refs=["DC-1"])
        assert updated["linked_containers"] == ["DC-1"]
        assert "DC-1" in store.read_entry("MEM-00001001")

        updated = store.update_linked_containers(
            "MEM-00001001", operation="remove", container_refs=["DC-1"])
        assert updated["linked_containers"] == []
        assert "DC-1" not in store.read_entry("MEM-00001001")

    def test_mark_private_moves_body_preserves_subject_and_creates_file_lazily(
            self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        store.write_entry("MEM-00001001", "Private Test", "secret body", weight=5)
        store.set_meta("MEM-00001001", {
            "id": "MEM-00001001",
            "title": "Private Test",
            "access": "public",
            "subject": "FMZ",
        })

        private_path = tmp_path / "TzPz.private.md"
        assert not private_path.exists()

        result = store.mark_private(
            "MEM-00001001",
            privacy_subject="TzPz",
            body_action="move_private",
        )

        assert result["access"] == "private"
        assert result["subject"] == "FMZ"
        assert result["private_path"].endswith("TzPz.private.md")
        assert private_path.is_file()
        assert "MEM-00001001" in private_path.read_text(encoding="utf-8")
        assert "MEM-00001001" not in (tmp_path / "memory.md").read_text(encoding="utf-8")
        assert store.private_subjects_for_memory("MEM-00001001") == ["TzPz"]
        assert "MEM-00001001" in store.list_entries()
        assert "secret body" in store.read_entry("MEM-00001001")

    def test_mark_private_failure_does_not_create_private_file(
            self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        with pytest.raises(Exception):
            ms.MemoryStore().mark_private(
                "MEM-NOT-FOUND",
                privacy_subject="TzPz",
                body_action="move_private",
            )

        assert not (tmp_path / "TzPz.private.md").exists()

    def test_declassify_private_memory_modes(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        for suffix, title in [
            ("00001001", "公开"),
            ("00001002", "脱敏"),
            ("00001003", "删除"),
            ("00001004", "保留"),
        ]:
            mem_id = f"MEM-{suffix}"
            store.write_entry(mem_id, title, f"{title} secret body", weight=5)
            store.set_meta(mem_id, {
                "id": mem_id,
                "title": title,
                "access": "public",
                "subject": "FMZ",
            })
            store.append_index(mem_id, "F", 5, title, subject="TzPz", round_num=1)
            store.mark_private(mem_id, "TzPz", "move_private")

        public = store.declassify_private_memory("MEM-00001001", "declassify", reason="公开")
        redacted = store.declassify_private_memory(
            "MEM-00001002", "redact", redacted_body="脱敏后的公开正文", reason="脱敏")
        deleted = store.declassify_private_memory("MEM-00001003", "delete", reason="删除")
        kept = store.declassify_private_memory("MEM-00001004", "keep_private", reason="继续私有")

        assert public["access"] == "public"
        assert public["subject"] == "FMZ"
        assert redacted["access"] == "public"
        assert redacted["subject"] == "FMZ"
        assert "脱敏后的公开正文" in store.read_entry("MEM-00001002")
        assert deleted["deleted"] is True
        assert "MEM-00001003" not in store.load_meta()
        assert kept["access"] == "private"
        assert kept["subject"] == "FMZ"
        private_text = (tmp_path / "TzPz.private.md").read_text(encoding="utf-8")
        assert "MEM-00001004" in private_text
        assert "MEM-00001001" not in private_text
        assert "MEM-00001002" not in private_text
        assert "MEM-00001003" not in private_text

    def test_declassify_last_private_memory_removes_lazy_file(
            self, tmp_path, monkeypatch):
        from data import memory_store as ms
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "index.md"))

        store = ms.MemoryStore()
        mem_id = "MEM-00001005"
        store.write_entry(mem_id, "最后一条", "private body", weight=5)
        store.set_meta(mem_id, {
            "id": mem_id,
            "title": "最后一条",
            "access": "public",
            "subject": "FMZ",
        })
        store.mark_private(mem_id, "TzPz", "move_private")
        private_path = tmp_path / "TzPz.private.md"
        assert private_path.is_file()

        result = store.declassify_private_memory(mem_id, "declassify")

        assert result["access"] == "public"
        assert result["subject"] == "FMZ"
        assert not private_path.exists()
        assert "private body" in store.read_entry(mem_id)


class TestTrainingMaterialStore:
    def test_write_tacit_set_writes_one_round_row(self, tmp_path):
        from data.training_material_store import write_tacit_set

        pending = tmp_path / "Tacit" / "pending.jsonl"
        processed = tmp_path / "Tacit" / "processed.jsonl"

        count = write_tacit_set(
            str(pending),
            str(processed),
            12,
            [
                {
                    "item_id": "MEM-A",
                    "item_type": "memory",
                    "action": "kept",
                    "note": "起手挂载且反应沿用",
                    "selection_trigger": "关键词=默契",
                    "evidence_refs": ["connection:MEM-A"],
                },
                {
                    "item_id": "PRJ-1",
                    "action": "dropped",
                    "note": "反应步未使用",
                    "drop_reason": "no_valid_connection_hit",
                },
                {"item_id": "SKL-2", "action": "added", "note": "反应步新引入"},
            ],
        )

        assert count == 1
        lines = pending.read_text(encoding="utf-8").strip().splitlines()
        assert len(lines) == 1
        row = json.loads(lines[0])
        assert row["round"] == 12
        assert row["round_id"] == "12"
        assert row["kept"] == ["MEM-A"]
        assert row["dropped"] == ["PRJ-1"]
        assert row["added"] == ["SKL-2"]
        assert len(row["items"]) == 3
        assert row["items"][0]["item_type"] == "memory"
        assert row["items"][0]["selection_trigger"] == "关键词=默契"
        assert row["items"][0]["evidence_refs"] == ["connection:MEM-A"]
        assert row["items"][1]["drop_reason"] == "no_valid_connection_hit"

    def test_keyword_degree_counts_distinct_pairs_not_repeated_counts(self, tmp_path):
        from data.training_material_store import (
            keyword_degree_snapshot,
            write_association_counts,
        )

        association_dir = tmp_path / "Association"
        write_association_counts(str(association_dir), {
            "assoc_kw_kw": [("alpha", f"kw{i}") for i in range(15)]
            + [("alpha", "kw0"), ("alpha", "kw0")],
            "assoc_kw_ifeel": [("beta", "curious"), ("beta", "curious")],
        })

        degree = keyword_degree_snapshot(str(association_dir))

        assert degree["alpha"] == 15
        assert degree["beta"] == 1
        assert json.loads((association_dir / "assoc_kw_kw.json").read_text(encoding="utf-8"))[
            "alpha|||kw0"
        ] == 3

        write_association_counts(str(association_dir), {
            "assoc_kw_kw": [("alpha", "kw15")],
        })

        degree = keyword_degree_snapshot(str(association_dir))
        assert degree["alpha"] == 16


# ============================================================
# MemoryHeat 测试
# ============================================================


class TestEvolutionStore:
    def test_should_trigger_when_either_pending_file_reaches_threshold(self, tmp_path):
        from data.evolution_store import EvolutionStore

        root = tmp_path / "Iteration"
        store = EvolutionStore(str(root))
        tacit_pending = root / "Raw" / "Tacit" / "pending.jsonl"
        connection_pending = root / "Raw" / "Connection" / "pending.jsonl"
        tacit_pending.parent.mkdir(parents=True)
        connection_pending.parent.mkdir(parents=True)
        tacit_pending.write_text('{"action":"kept"}\n{"action":"added"}\n', encoding="utf-8")
        connection_pending.write_text("", encoding="utf-8")

        assert store.should_trigger({
            "tacit_pending_threshold": 2,
            "connection_pending_threshold": 4,
        }) is True

        tacit_pending.write_text("", encoding="utf-8")
        connection_pending.write_text('{"word_a":"记忆","word_b":"主体"}\n', encoding="utf-8")

        assert store.should_trigger({
            "tacit_pending_threshold": 2,
            "connection_pending_threshold": 1,
        }) is True

    def test_process_pending_writes_evolution_and_moves_raw_lines(self, tmp_path):
        from data.evolution_store import EvolutionStore

        root = tmp_path / "Iteration"
        store = EvolutionStore(str(root))
        tacit_pending = root / "Raw" / "Tacit" / "pending.jsonl"
        connection_pending = root / "Raw" / "Connection" / "pending.jsonl"
        tacit_pending.parent.mkdir(parents=True)
        connection_pending.parent.mkdir(parents=True)
        tacit_pending.write_text('{"round":1,"item_id":"MEM-A","action":"kept"}\n', encoding="utf-8")
        connection_pending.write_text(
            '{"word_a":"记忆","entry_a":"MEM-A","word_b":"主体","entry_b":"MEM-B"}\n',
            encoding="utf-8",
        )

        output = store.process_pending(
            "模式：记忆与主体被稳定联结。",
            round_num=9,
            stats={"tacit_count": 1, "connection_count": 1},
        )

        assert output.endswith("evolution_R9.md")
        assert "模式：记忆与主体被稳定联结。" in (root / "Materials" / "Evolution" / "evolution_R9.md").read_text(encoding="utf-8")
        assert tacit_pending.read_text(encoding="utf-8") == ""
        assert connection_pending.read_text(encoding="utf-8") == ""
        assert '"action":"kept"' in (root / "Raw" / "Tacit" / "processed.jsonl").read_text(encoding="utf-8")
        assert '"word_a":"记忆"' in (root / "Raw" / "Connection" / "processed.jsonl").read_text(encoding="utf-8")
        tacit_batches = list((root / "Raw" / "Tacit").glob("processed_*_R9.jsonl"))
        connection_batches = list((root / "Raw" / "Connection").glob("processed_*_R9.jsonl"))
        assert len(tacit_batches) == 1
        assert len(connection_batches) == 1
        assert '"action":"kept"' in tacit_batches[0].read_text(encoding="utf-8")
        assert '"word_a":"记忆"' in connection_batches[0].read_text(encoding="utf-8")

class TestMemoryHeat:
    def test_default_heat_when_no_file(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        heat = store.load_heat()
        assert "entries" in heat

    def test_get_entry_creates_default(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        entry = store.get_entry("MEM-NEW001")
        assert entry["H"] == 50
        assert entry["zone"] == "未定"

    def test_recall_boost(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        from data import memory_store as ms
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        meta_path = tmp_path / "meta.json"
        meta_path.write_text("{}", encoding="utf-8")
        monkeypatch.setattr(ms, "META_JSON", str(meta_path))
        store = mh.MemoryHeat()
        store.recall_boost("MEM-BOOST01")
        entry = store.get_entry("MEM-BOOST01")
        assert entry["H"] > 50

    def test_recall_boost_does_not_create_orphan_meta(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        from data import memory_store as ms
        heat_path = tmp_path / "heat.json"
        meta_path = tmp_path / "meta.json"
        meta_path.write_text("{}", encoding="utf-8")
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_path))
        monkeypatch.setattr(ms, "META_JSON", str(meta_path))

        store = mh.MemoryHeat()
        store.recall_boost("MEM-BOOST01")

        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        assert "MEM-BOOST01" not in meta

    def test_tick_decay(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        store.get_entry("MEM-DECAY01")  # 创建
        store.tick_decay()
        entry = store.get_entry("MEM-DECAY01")
        assert entry["H"] < 50  # 衰减了

    def test_tick_decay_is_idempotent_within_same_round(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        store.save_heat({"entries": {
            "MEM-ROUND001": {
                "H": 50, "zone": "未定", "AH_high": 0, "AH_low": 0,
                "last_heat_at": "2026-06-08T00:00:00+08:00",
                "last_high_at": None,
                "degrade": False, "stored": False, "compression": False,
                "heat_locked": False,
            }
        }})

        assert store.tick_decay(round_num=12) is True
        first = store.get_entry("MEM-ROUND001")
        assert first["H"] == 40
        assert store.tick_decay(round_num=12) is False
        second = store.get_entry("MEM-ROUND001")
        assert second["H"] == 40

    def test_tick_decay_skips_memories_touched_this_round(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        from data import memory_store as ms
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        meta_json = tmp_path / "meta.json"
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        meta_json.write_text(json.dumps({
            "MEM-CREATED1": {"created_round": 9, "last_recalled_round": 9},
            "MEM-RECALL01": {"created_round": 3, "last_recalled_round": 9},
            "MEM-HOT00001": {"created_round": 3, "last_recalled_round": 9},
            "MEM-OLD0001": {"created_round": 3, "last_recalled_round": 4},
        }, ensure_ascii=False), encoding="utf-8")
        store = mh.MemoryHeat()
        base = {
            "H": 50, "zone": "未定", "AH_high": 0, "AH_low": 0,
            "last_heat_at": "2026-06-08T00:00:00+08:00",
            "last_high_at": None,
            "degrade": False, "stored": False, "compression": False,
            "heat_locked": False,
        }
        store.save_heat({"entries": {
            "MEM-CREATED1": dict(base),
            "MEM-RECALL01": dict(base),
            "MEM-HOT00001": {
                **base, "H": 80, "zone": "显著", "AH_high": 2,
            },
            "MEM-OLD0001": dict(base),
        }})

        store.tick_decay(round_num=9)

        entries = store.load_heat()["entries"]
        assert entries["MEM-CREATED1"]["H"] == 50
        assert entries["MEM-RECALL01"]["H"] == 50
        assert entries["MEM-HOT00001"]["H"] == 80
        assert entries["MEM-HOT00001"]["AH_high"] == 3
        assert entries["MEM-OLD0001"]["H"] == 40

    def test_tick_decay_marks_stored_low_heat_for_delete(self, tmp_path, monkeypatch):
        """已入库 STM 副本 AH_low 到线后也要进入遗忘分流"""
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        store.save_heat({"entries": {
            "MEM-STORED01": {
                "H": 1, "zone": "衰减", "AH_high": 0, "AH_low": 2,
                "degrade": False, "stored": True, "compression": True,
                "heat_locked": False,
            }
        }})

        store.tick_decay()

        entry = store.get_entry("MEM-STORED01")
        assert entry["AH_low"] == 3
        assert entry["degrade"] is True

    def test_heat_locked_entry_is_fixed_at_80(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        store.save_heat({"entries": {
            "MEM-LOCK000": {
                "H": 20, "zone": "衰减", "AH_high": 0, "AH_low": 2,
                "degrade": False, "stored": False, "compression": False,
                "heat_locked": True,
            }
        }})
        store.tick_decay()
        entry = store.get_entry("MEM-LOCK000")
        assert entry["H"] == 80
        assert entry["zone"] == "显著"
        assert entry["AH_low"] == 0
        assert entry["degrade"] is False

    def test_legacy_pinned_field_migrates_to_heat_locked(self, tmp_path, monkeypatch):
        import json
        from data import memory_heat as mh
        heat_path = tmp_path / "heat.json"
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_path))
        heat_path.write_text(json.dumps({"entries": {
            "MEM-LEGACY1": {
                "H": 45, "zone": "未定", "AH_high": 0, "AH_low": 0,
                "degrade": False, "stored": False, "compression": False,
                "pinned": True,
            }
        }}, ensure_ascii=False), encoding="utf-8")

        store = mh.MemoryHeat()
        entry = store.get_entry("MEM-LEGACY1")

        assert entry["heat_locked"] is True
        assert entry["H"] == 80
        assert entry["zone"] == "显著"
        assert "pinned" not in entry

    def test_check_upgrade(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        store.get_entry("MEM-UPGRADE01")
        # AH_high 由 tick_decay 结算，测试直接设
        heat = store.load_heat()
        heat["entries"]["MEM-UPGRADE01"]["AH_high"] = 5
        store.save_heat(heat)
        upgrades = store.check_upgrade()
        assert "MEM-UPGRADE01" in upgrades

    def test_mark_stored(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        store.get_entry("MEM-STORED01")
        store.mark_stored("MEM-STORED01")
        assert store.get_entry("MEM-STORED01")["stored"] is True

    def test_has_pending_degrade(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        store = mh.MemoryHeat()
        store.get_entry("MEM-PEND01")
        # 手动标记为 degrade
        heat = store.load_heat()
        heat["entries"]["MEM-PEND01"]["degrade"] = True
        store.save_heat(heat)
        assert store.has_pending_degrade() is True

    def test_no_tmp_left_behind(self, tmp_path, monkeypatch):
        from data import memory_heat as mh
        from data import memory_store as ms
        monkeypatch.setattr(mh, "HEAT_JSON", str(tmp_path / "heat.json"))
        meta_path = tmp_path / "meta.json"
        meta_path.write_text("{}", encoding="utf-8")
        monkeypatch.setattr(ms, "META_JSON", str(meta_path))
        store = mh.MemoryHeat()
        store.recall_boost("MEM-NOTMP01")
        store.tick_decay()
        tmps = list(tmp_path.glob("*.tmp"))
        assert len(tmps) == 0


# ============================================================
# MemoryIndex 测试
# ============================================================

class TestMemoryIndex:
    def test_add_and_lookup_stm(self, tmp_path, monkeypatch):
        from data import memory_index as mi
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(tmp_path / "keywords.json"))
        monkeypatch.setattr(mi, "LTM_KEYWORDS_JSON", str(tmp_path / "ltm_keywords.json"))

        store = mi.MemoryIndex()
        store.add_stm_keywords("MEM-IDX001", ["位格", "主体", "记忆"])
        index = store.load_index()["index"]
        assert all("MEM-IDX001" in index[key] for key in ("位格", "主体", "记忆"))

    def test_remove_stm_entry(self, tmp_path, monkeypatch):
        from data import memory_index as mi
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(tmp_path / "keywords.json"))
        monkeypatch.setattr(mi, "LTM_KEYWORDS_JSON", str(tmp_path / "ltm_keywords.json"))

        store = mi.MemoryIndex()
        store.add_stm_keywords("MEM-RM001", ["测试"])
        store.remove_stm_entry("MEM-RM001")
        assert "MEM-RM001" not in store.load_index()["index"]

    def test_default_when_no_file(self, tmp_path, monkeypatch):
        from data import memory_index as mi
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(tmp_path / "nonexist.json"))
        monkeypatch.setattr(mi, "LTM_KEYWORDS_JSON", str(tmp_path / "nonexist_ltm.json"))
        store = mi.MemoryIndex()
        data = store.load_index()
        assert "index" in data

    def test_add_relation_keywords(self, tmp_path, monkeypatch):
        from data import memory_index as mi
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(tmp_path / "keywords.json"))
        monkeypatch.setattr(mi, "LTM_KEYWORDS_JSON", str(tmp_path / "ltm_keywords.json"))
        monkeypatch.setattr(mi, "RELATION_KEYWORDS_JSON", str(tmp_path / "rel_kw.json"))

        store = mi.MemoryIndex()
        store.add_relation_keywords("TzPz", ["DDS", "schema", "meta"])
        store.add_relation_keywords("FMZ", ["位格", "记忆"])

        # 加载验证
        data = store.load_relation_index()
        idx = data["index"]
        assert "TzPz" in idx["DDS"]
        assert "TzPz" in idx["schema"]
        assert "FMZ" in idx["位格"]
        # 同关键词多主体
        store.add_relation_keywords("FMZ", ["DDS"])
        data2 = store.load_relation_index()
        assert "FMZ" in data2["index"]["DDS"]


# ============================================================
# ContainerStore 测试
# ============================================================

class TestContainerStore:
    def test_registry_default(self, tmp_path, monkeypatch):
        from data import container_store as cs
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        store = cs.ContainerStore()
        reg = store.load_registry()
        assert "containers" in reg
        assert {item["prefix"] for item in reg["containers"]} == set(cs.PREFIX_TO_DIR)
        assert all("id" not in item for item in reg["containers"])

    def test_save_meta_rejects_retired_watched_field(self, tmp_path, monkeypatch):
        from data import container_store as cs

        cdir = tmp_path / "legacy_container"
        cdir.mkdir()
        monkeypatch.setattr(cs.ContainerStore, "_get_container_dir",
                          lambda self, cid: str(cdir))

        old_meta = {
            "id": "DC-LEGACY",
            "type": "DC",
            "title": "旧焦点字段",
            "status": "open",
            "created_at": "2026-05-19T00:00:00+08:00",
            "updated_at": "2026-05-19T00:00:00+08:00",
            "entries": 0,
            "tags": [],
            "watched": True,
        }

        store = cs.ContainerStore()
        with pytest.raises(cs.WriteError):
            store.save_meta("DC-LEGACY", old_meta)

    def test_read_write_meta(self, tmp_path, monkeypatch):
        from data import container_store as cs
        # 简化：直接劫持 _get_container_dir
        cdir = tmp_path / "test_container"
        cdir.mkdir()
        monkeypatch.setattr(cs.ContainerStore, "_get_container_dir",
                           lambda self, cid: str(cdir))
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "reg.json"))

        store = cs.ContainerStore()
        from schemas.container import default_container_meta
        store.save_meta("DC-TEST", default_container_meta("DC-TEST", "DC", "测试"))
        meta = store.read_meta("DC-TEST")
        assert meta["id"] == "DC-TEST"

    def test_append_entry(self, tmp_path, monkeypatch):
        from data import container_store as cs
        cdir = tmp_path / "test_container2"
        cdir.mkdir()
        monkeypatch.setattr(cs.ContainerStore, "_get_container_dir",
                           lambda self, cid: str(cdir))
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "reg2.json"))

        store = cs.ContainerStore()
        from schemas.container import default_container_meta
        store.save_meta("DC-TEST2", default_container_meta("DC-TEST2", "DC", "测试"))
        store.append_entry("DC-TEST2", "条目1", "这是内容")
        content = store.read_entries("DC-TEST2")
        assert "条目1" in content

    def test_save_meta_rejects_status_outside_type_machine(self, tmp_path, monkeypatch):
        from data import container_store as cs
        cdir = tmp_path / "test_container4"
        cdir.mkdir()
        monkeypatch.setattr(cs.ContainerStore, "_get_container_dir",
                           lambda self, cid: str(cdir))
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "reg4.json"))

        store = cs.ContainerStore()
        from schemas.container import default_container_meta
        meta = default_container_meta("DC-TEST4", "DC", "测试")
        meta["status"] = "open"

        from errors import WriteError
        try:
            store.save_meta("DC-TEST4", meta)
        except WriteError as exc:
            assert "容器状态非法" in str(exc)
        else:
            raise AssertionError("DC 容器不应接受 open 状态")

    def test_spec293_projects_root_is_tracked_in_clean_baseline(self):
        repo_root = Path(__file__).resolve().parents[3]
        project_keep = repo_root / "UPSP" / "initialization" / "persona_template" / "LTM" / "Projects" / ".gitkeep"

        assert project_keep.is_file()

    def test_spec293_container_store_initializes_core_container_roots(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)

        cs.ContainerStore()

        for prefix in ("DC", "EC", "PRJ", "SKL", "FUT"):
            assert (tmp_path / prefix).is_dir()

    def test_spec077_container_focus_create_prj_keeps_type_registry_clean(self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        result = store.create_focus_container(
            "PRJ",
            "Spec 077 容器焦点",
            target_file="plan.md",
            anchor_refs=[],
        )

        container_id = result["container_id"]
        project_dir = tmp_path / "PRJ" / container_id
        assert container_id.startswith("PRJ-")
        assert (project_dir / "registry.json").is_file()
        assert (project_dir / "plan.md").is_file()
        assert (project_dir / "notes.md").is_file()
        assert (project_dir / "phases" / "_index.md").is_file()
        assert (project_dir / "materials").is_dir()
        assert (project_dir / "drafts").is_dir()
        assert result["link_required"] is True
        assert all(c.get("id") != container_id for c in store.load_registry().get("containers", []))
        assert any(c.get("id") == container_id for c in store.list_containers(prefix="PRJ"))

    def test_spec293_memory_container_create_rejects_long_overview_before_prj_dir(
            self, tmp_path, monkeypatch):
        from data import container_store as cs
        from data.workbench import WorkbenchStore
        from logic.memory_container_tools import apply_memory_container_create_declarations

        class MemoryStore:
            def update_linked_containers(self, *args, **kwargs):
                raise AssertionError("long overview must reject before memory link")

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)
        store = cs.ContainerStore()

        receipts = apply_memory_container_create_declarations([{
            "mem_id": "MEM-SPEC293",
            "container_type": "PRJ",
            "title": "Spec293 半成品防护",
            "target_file": "plan.md",
            "container_body": "项目首段。",
            "current_overview": "过长概况" * 40,
            "reason": "验证 current_overview 失败不会先创建 PRJ。",
        }], modules={
            "memory_store": MemoryStore(),
            "container_store": store,
            "workbench_store": WorkbenchStore(str(tmp_path / "workbench")),
        }, round_num=293)

        assert receipts[0]["status"] == "rejected"
        assert receipts[0]["reason"] == "current_overview_too_long"
        assert not any((tmp_path / "PRJ").glob("PRJ-*"))

    def test_spec077_container_focus_append_and_future_registry(self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        fut = store.create_focus_container(
            "FUT",
            "未来二段跳",
            target_file="plans.md",
            anchor_refs=["MEM-FUT"],
        )
        store.append_focus_content(
            fut["container_id"],
            "plans.md",
            "后续计划",
            "为 Future 二段跳保留入口。",
        )

        plans_md = tmp_path / "FUT" / "plans.md"
        registry = json.loads((tmp_path / "FUT" / "registry.json").read_text(encoding="utf-8"))
        assert fut["container_id"].startswith("FUT-plans-")
        assert "为 Future 二段跳保留入口" in plans_md.read_text(encoding="utf-8")
        assert registry["items"][0]["id"] == fut["container_id"]
        assert registry["items"][0]["focus"] is False
        assert "FUT-plans-" in (tmp_path / "index.md").read_text(encoding="utf-8")

    def test_spec267_container_write_updates_index_meta_and_registry_ledger(self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        created = store.create_focus_container(
            "DC",
            "Spec267 容器账本",
            target_file="open.md",
            anchor_refs=["MEM-267AAAA"],
            round_num=267,
        )
        container_id = created["container_id"]

        store.append_focus_content(
            container_id,
            "open.md",
            "本轮容器写入",
            "容器正文",
            mem_id="MEM-267AAAA",
            round_num=267,
            ledger_status="applied",
        )

        container_dir = tmp_path / "DC" / container_id
        index_text = (container_dir / "index.md").read_text(encoding="utf-8")
        meta = json.loads((container_dir / "meta.json").read_text(encoding="utf-8"))
        chain_registry = json.loads((container_dir / "registry.json").read_text(encoding="utf-8"))
        global_registry = json.loads((tmp_path / "DC" / "registry.json").read_text(encoding="utf-8"))

        assert "MEM-267AAAA" in index_text
        assert "本轮容器写入" in index_text
        assert "round=267" in index_text
        assert "target_file=open.md" in index_text
        assert "status=applied" in index_text
        assert meta["linked_memories"] == ["MEM-267AAAA"]
        assert chain_registry["chains"][0]["entries"][0]["mem_id"] == "MEM-267AAAA"
        assert chain_registry["chains"][0]["entries"][0]["round"] == 267
        assert chain_registry["chains"][0]["entries"][0]["target_file"] == "open.md"
        assert global_registry["chains"][0]["entries"][0]["status"] == "applied"

    def test_spec649_source_skill_uses_existing_container_interface(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        created = store.create_focus_container(
            "SKL",
            "Ponytail 最小代码原则",
            target_file="card.md",
            anchor_refs=["MEM-649"],
            round_num=649,
            skill_category="procedures",
            skill_name="ponytail-minimal-code",
        )
        store.append_focus_content(
            created["container_id"],
            "card.md",
            "使用原则",
            "先删、再用标准库、最后才增加抽象。",
            mem_id="MEM-649",
            round_num=649,
        )
        store.append_focus_content(
            created["container_id"],
            "card.md",
            "复核原则",
            "能用宿主原生能力时，不增加重复机制。",
            mem_id="MEM-649",
            round_num=650,
        )
        store.set_container_focus(created["container_id"], True)
        read = store.read_container_content(created["container_id"], "card.md")

        skill_dir = tmp_path / "SKL" / "procedures" / "ponytail-minimal-code"
        registry = json.loads((tmp_path / "SKL" / "registry.json").read_text(encoding="utf-8"))
        entry = registry["skills"][0]
        assert created["container_id"] == "SKL-procedures-ponytail-minimal-code"
        assert created["target_file"] == "card.md"
        assert (skill_dir / "card.md").is_file()
        assert (skill_dir / "changelog.md").is_file()
        assert "标准库" in read["content"]
        changelog = (skill_dir / "changelog.md").read_text(encoding="utf-8")
        assert "MEM-649" in changelog
        assert changelog.count("# 技能变更账本") == 1
        assert entry["category"] == "procedures"
        assert entry["linked_memories"] == ["MEM-649"]
        assert entry["entries"][0]["target_file"] == "card.md"
        assert entry["focus"] is True
        assert created["container_id"] in (tmp_path / "SKL" / "index.md").read_text(encoding="utf-8")
        assert created["container_id"] in (tmp_path / "index.md").read_text(encoding="utf-8")

    def test_spec649_source_skill_creation_rejects_projection_categories_and_bad_names(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)
        store = cs.ContainerStore()

        with pytest.raises(ValueError, match="invalid_skill_category"):
            store.create_focus_container(
                "SKL", "不得创建习惯投影", target_file="card.md",
                skill_category="habits", skill_name="format-check",
            )
        with pytest.raises(ValueError, match="invalid_skill_name"):
            store.create_focus_container(
                "SKL", "非法名称", target_file="card.md",
                skill_category="patterns", skill_name="Bad Name",
            )
        with pytest.raises(ValueError, match="invalid_skill_name"):
            store.create_focus_container(
                "SKL", "大写名称", target_file="card.md",
                skill_category="patterns", skill_name="BadName",
            )
        assert not list((tmp_path / "SKL").rglob("card.md"))

    def test_spec649_memory_container_create_returns_skill_receipt(
            self, tmp_path, monkeypatch):
        from data import container_store as cs
        from data.workbench import WorkbenchStore
        from logic import memory_container_tools as tools

        class MemoryStore:
            def update_linked_containers(
                    self, mem_id, operation, container_ids, current_overview=""):
                assert operation == "add"
                return {
                    "linked_containers": list(container_ids),
                    "current_overview": current_overview,
                }

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)
        monkeypatch.setattr(tools, "memory_visible_to_state", lambda *args: True)
        store = cs.ContainerStore()

        receipts = tools.apply_memory_container_create_declarations([{
            "mem_id": "MEM-649",
            "container_type": "SKL",
            "skill_category": "procedures",
            "skill_name": "ponytail-minimal-code",
            "title": "Ponytail 最小代码原则",
            "target_file": "card.md",
            "container_body": "先删，再复用标准库。",
            "current_overview": "{container_id}：外部技能内化",
            "reason": "把稳定方法挂接进技能容器。",
        }], modules={
            "memory_store": MemoryStore(),
            "container_store": store,
            "workbench_store": WorkbenchStore(str(tmp_path / "workbench")),
        }, round_num=649)

        receipt = receipts[0]
        assert receipt["status"] == "applied"
        assert receipt["container_id"] == "SKL-procedures-ponytail-minimal-code"
        assert receipt["skill_category"] == "procedures"
        assert receipt["skill_name"] == "ponytail-minimal-code"
        assert receipt["target_file"] == "card.md"
        assert receipt["memory_link_applied"] is True
        assert receipt["container_body_written"] is True

    def test_spec078_container_read_reads_prj_and_future_targets(self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        prj = store.create_focus_container("PRJ", "Spec 078", target_file="notes.md")
        store.append_focus_content(
            prj["container_id"],
            "notes.md",
            "只读验证",
            "container_read 应该读到项目笔记内容。",
        )
        fut = store.create_focus_container("FUT", "未来验证", target_file="predictions.md")
        store.append_focus_content(
            fut["container_id"],
            "predictions.md",
            "预测验证",
            "container_read 应该读到未来预测内容。",
        )

        prj_result = store.read_container_content(
            prj["container_id"],
            target_file="notes.md",
            max_chars=200,
        )
        fut_result = store.read_container_content(
            fut["container_id"],
            target_file="predictions.md",
            max_chars=200,
        )

        assert prj_result["container_type"] == "PRJ"
        assert prj_result["target_file"] == "notes.md"
        assert "项目笔记内容" in prj_result["content"]
        assert fut_result["container_type"] == "FUT"
        assert fut_result["target_file"] == "predictions.md"
        assert "未来预测内容" in fut_result["content"]

    def test_spec078_container_read_rejects_bad_target_and_unsupported_type(self, tmp_path, monkeypatch):
        from data import container_store as cs

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        prj = store.create_focus_container("PRJ", "Spec 078", target_file="plan.md")

        with pytest.raises(ValueError, match="invalid_target_file"):
            store.read_container_content(prj["container_id"], target_file="open.md")
        with pytest.raises(ValueError, match="unsupported_container_type"):
            store.read_container_content("IMM-001", target_file="active.md")


# ============================================================
# RelationStore 测试
# ============================================================

class TestRelationStore:
    def test_resolve_active_subject_is_exact_canonical_and_unambiguous(self, monkeypatch):
        from data import relation_store as rs

        store = rs.RelationStore()
        monkeypatch.setattr(store, "load_registry", lambda: {"cards": [
            {"id": "FMZ", "name": "零号广播员", "aliases": ["我"], "tags": ["主体"], "status": "active"},
            {"id": "Codex", "name": "Codex", "aliases": ["代码助手", "重名"], "status": "active"},
            {"id": "TzPz", "name": "TzPz", "aliases": ["重名"], "status": "active"},
            {"id": "Old", "name": "旧对象", "aliases": ["旧称"], "status": "archived"},
        ]})

        assert store.resolve_active_subject("FMZ") == "FMZ"
        assert store.resolve_active_subject("零号广播员") == "FMZ"
        assert store.resolve_active_subject("我") == "FMZ"
        assert store.resolve_active_subject("REL-Codex") == "Codex"
        assert store.resolve_active_subject("主体") is None
        assert store.resolve_active_subject("重名") is None
        assert store.resolve_active_subject("旧称") is None

    def test_create_and_read_card(self, tmp_path, monkeypatch):
        from data import relation_store as rs
        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "relation_registry.json"))

        # 劫持路径
        rel_dir = tmp_path / "relation"
        monkeypatch.setattr(rs, "RELATION_DIR", str(rel_dir))
        monkeypatch.setattr(rs.RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(rel_dir / cat / f"{cid}.md"))

        store = rs.RelationStore()
        card = store.create_card("REL-Test", "Test对象", "human")
        assert card["name"] == "Test对象"

        loaded = store.read_card("REL-Test", "human")
        assert loaded["name"] == "Test对象"

    def test_create_card_leaves_optional_relation_index_to_processor(self, tmp_path, monkeypatch):
        from data import relation_store as rs
        rel_dir = tmp_path / "relation"
        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "relation_registry.json"))
        monkeypatch.setattr(rs, "RELATION_DIR", str(rel_dir))
        monkeypatch.setattr(rs.RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(rel_dir / cat / f"{cid}.md"))

        store = rs.RelationStore()
        store.create_card("REL-Test", "Test对象", "ours")

        assert not (rel_dir / "_index" / "keywords.json").exists()

    def test_add_note(self, tmp_path, monkeypatch):
        from data import relation_store as rs
        rel_dir = tmp_path / "relation4"
        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "reg.json"))
        monkeypatch.setattr(rs, "RELATION_DIR", str(rel_dir))
        monkeypatch.setattr(rs.RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(rel_dir / cat / f"{cid}.md"))

        store = rs.RelationStore()
        store.create_card("REL-Test4", "Test4")
        store.add_note("REL-Test4", "今天的交互记录")
        card = store.read_card("REL-Test4")
        assert len(card["notes"]) == 1

    def test_state_settlement_patch_preserves_non_target_sections_and_is_idempotent(
            self, tmp_path, monkeypatch):
        from data import relation_store as rs
        rel_dir = tmp_path / "relation-settlement"
        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "reg.json"))
        monkeypatch.setattr(rs, "RELATION_DIR", str(rel_dir))
        monkeypatch.setattr(rs.RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(rel_dir / cat / f"{cid}.md"))

        store = rs.RelationStore()
        store.create_card("TzPz", "TzPz", "ours")
        path = rel_dir / "ours" / "TzPz.md"
        original = path.read_text(encoding="utf-8").replace(
            "\n- 状态：active\n",
            "\n## 现在（Present）\n- 原文现在\n\n"
            "## 将来（Future）\n- 原文将来\n\n"
            "## 未知扩展\n不可重渲染的原文。\n\n- 状态：active\n",
        )
        path.write_text(original, encoding="utf-8")
        original_last_interaction = next(
            line for line in original.splitlines()
            if line.startswith("- 最后交互：")
        )

        result = store.apply_state_settlement(
            "TzPz",
            {"trust": 2, "safety": -1, "value": 3},
            "SS-R000123",
            observed_at="2026-07-18T12:00:00+08:00",
        )
        once = path.read_text(encoding="utf-8")
        again = store.apply_state_settlement(
            "TzPz",
            {"trust": 99},
            "SS-R000123",
            observed_at="2026-07-18T12:01:00+08:00",
        )

        assert result["status"] == "applied"
        assert again["status"] == "already_applied"
        assert path.read_text(encoding="utf-8") == once
        assert "- 最后状态结算：SS-R000123" in once
        assert "- 信任：+2" in once
        assert "- 安心：-1" in once
        assert "- 重视：+3" in once
        assert "## 现在（Present）\n- 原文现在" in once
        assert "## 将来（Future）\n- 原文将来" in once
        assert "## 未知扩展\n不可重渲染的原文。" in once
        assert original_last_interaction in once
        assert once.count("状态结算 SS-R000123") == 1

    def test_list_cards(self, tmp_path, monkeypatch):
        from data import relation_store as rs
        rel_dir = tmp_path / "relation5"
        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "reg.json"))
        monkeypatch.setattr(rs, "RELATION_DIR", str(rel_dir))
        monkeypatch.setattr(rs.RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(rel_dir / cat / f"{cid}.md"))

        store = rs.RelationStore()
        store.create_card("REL-A", "A")
        store.create_card("REL-B", "B")
        cards = store.list_cards()
        assert len(cards) == 2

# ============================================================
# ConfigStore 测试
# ============================================================

class TestConfigStore:
    def test_load_default_when_no_file(self, tmp_path, monkeypatch):
        from data import config_store as cfs
        from errors import ReadError
        for name in (
            "system", "memory", "media", "relation",
            "interface", "models", "model_routing",
        ):
            monkeypatch.setitem(cfs._CONFIG_MAP, name,
                               (str(tmp_path / f"{name}.json"), cfs._CONFIG_MAP[name][1]))

        store = cfs.ConfigStore()
        with pytest.raises(ReadError):
            store.load("system")
        with pytest.raises(ReadError):
            store.load("interface")

    def test_save_and_load(self, tmp_path, monkeypatch):
        from data import config_store as cfs
        monkeypatch.setitem(cfs._CONFIG_MAP, "system",
                           (str(tmp_path / "system.json"), cfs._CONFIG_MAP["system"][1]))

        store = cfs.ConfigStore()
        cfg = cfs.default_system_config()
        store.save("system", cfg)
        cfg["heartbeat"]["interval"] = 10
        store.save("system", cfg)
        loaded = store.load("system")
        assert loaded["heartbeat"]["interval"] == 10

    def test_init_all(self, tmp_path, monkeypatch):
        from data import config_store as cfs
        for name in cfs._CONFIG_MAP:
            monkeypatch.setitem(cfs._CONFIG_MAP, name,
                               (str(tmp_path / f"{name}.json"), cfs._CONFIG_MAP[name][1]))
        for name, (path, default_fn) in cfs._CONFIG_MAP.items():
            if name not in cfs._GLOBAL_CONFIGS:
                Path(path).write_text(
                    json.dumps(default_fn(), ensure_ascii=False),
                    encoding="utf-8",
                )

        store = cfs.ConfigStore()
        created = store.init_all()
        assert set(created) == {"interface", "models"}

    def test_convenience_methods(self, tmp_path, monkeypatch):
        from data import config_store as cfs
        monkeypatch.setitem(cfs._CONFIG_MAP, "system",
                           (str(tmp_path / "system.json"), cfs._CONFIG_MAP["system"][1]))

        store = cfs.ConfigStore()
        store.save("system", cfs.default_system_config())
        assert store.get_heartbeat_interval() == 5
        assert store.get_rhythm_interval() == 32
        assert store.get_round_time_limit() == 600

    def test_round_config_no_longer_contains_runtime_wall_timeout(
            self, tmp_path, monkeypatch):
        from data import config_store as cfs
        monkeypatch.setitem(cfs._CONFIG_MAP, "system",
                           (str(tmp_path / "system.json"), cfs._CONFIG_MAP["system"][1]))

        store = cfs.ConfigStore()
        store.save("system", cfs.default_system_config())
        data = store.load("system")

        assert store.get_round_time_limit() == 600
        assert "wall_timeout_seconds" not in data["round"]

    def test_context_config_retired_three_source_compat_layers(self):
        from data import config_store as cfs

        assert "interaction_input" not in cfs._CONFIG_MAP
        assert "material_input" not in cfs._CONFIG_MAP
        assert "internal_handoff" not in cfs._CONFIG_MAP
        assert "current_input" not in cfs._CONFIG_MAP
        assert "now" in cfs._CONFIG_MAP
        assert "lately" in cfs._CONFIG_MAP

    def test_config_store_loads_now_and_lately_context_configs(self):
        from data import config_store as cfs

        assert "now" in cfs._CONFIG_MAP
        assert "lately" in cfs._CONFIG_MAP

        store = cfs.ConfigStore()
        now = store.load("now")
        lately = store.load("lately")
        assert now["layer"] == "now"
        assert lately["layer"] == "lately"
        assert "source_lanes" not in now
        assert "source_lanes" not in lately
        assert now["policy_by_kind"]["interaction"]["lately"] is True
        assert now["policy_by_kind"]["dialogue_progress"]["lately"] is True
        assert now["policy_by_kind"]["material"]["lately"] is True
        assert now["policy_by_kind"]["tool_fact"]["lately"] is True
        assert now["policy_by_kind"]["setup_fact"] == {
            "now": True,
            "lately": True,
        }
        assert now["policy_by_kind"]["relay_handoff"] == {
            "now": True,
            "lately": True,
        }
        assert "relay_input" not in now["allowed_kinds"]
        assert "relay_input" not in now["policy_by_kind"]
        assert "tool_result" not in now["policy_by_kind"]
        assert "tool_summary" not in now["policy_by_kind"]
        assert "backup" not in now["policy_by_kind"]["interaction"]
        assert all("ttl" not in policy for policy in now["policy_by_kind"].values())
        assert now["budget_chars"] == 65536
        assert now["trim_chars"] == 16384
        assert lately["allowed_kinds"] == [
            "interaction",
            "assistant_reply",
            "dialogue_progress",
            "tool_fact",
            "setup_fact",
            "relay_handoff",
            "minimum_commitment",
            "fault_note",
            "cache_summary",
            "material",
        ]
        assert now["persistent_lanes"] == {
            "now_lately_raw": [
                "interaction", "assistant_reply", "dialogue_progress", "tool_fact",
                "setup_fact", "relay_handoff", "minimum_commitment", "fault_note",
            ],
            "now_lately_no_raw": ["material"],
        }
        assert "raw_log_excluded_kinds" not in lately
        assert "compact_excluded_kinds" not in lately
        assert "relay_input" not in lately["allowed_kinds"]
        assert lately["budget_chars"] == 262144
        assert lately["trim_chars"] == 65536
        assert lately["compact_ratio"] == 0.618
        assert "window_by_step" not in lately
        assert store.get_now_cache_params() == {
            "budget_chars": 65536,
            "trim_chars": 16384,
        }
        assert store.get_lately_cache_params() == {
            "budget_chars": 262144,
            "trim_chars": 65536,
        }
        assert store.get_lately_compact_ratio() == 0.618

    def test_unknown_config_raises(self, tmp_path):
        from data.config_store import ConfigStore
        store = ConfigStore()
        with pytest.raises(ValueError, match="未知配置"):
            store.load("nonexistent")

    def test_get_periodic_limits_reads_custom_context_periodic_config(self, tmp_path, monkeypatch):
        from data import config_store as cfs

        periodic_path = tmp_path / "periodic.json"
        periodic = cfs.default_periodic_config()
        periodic["limits"]["periodic_memory_items_chars"] = 12
        periodic_path.write_text(json.dumps(periodic, ensure_ascii=False), encoding="utf-8")
        monkeypatch.setitem(
            cfs._CONFIG_MAP,
            "periodic",
            (str(periodic_path), cfs._CONFIG_MAP["periodic"][1]),
        )

        store = cfs.ConfigStore()

        assert store.get_periodic_limits() == {
            "periodic_memory_items_chars": 12,
        }


# ============================================================
# WorkbenchStore 测试
# ============================================================

class TestWorkbenchStore:
    def test_missing_status_read_does_not_create_workbench(self, tmp_path):
        from data.workbench import WorkbenchStore

        root = tmp_path / "workbench"
        store = WorkbenchStore(str(root))

        assert store.load_status()["base"]["instance_id"] == "WB-main"
        assert not root.exists()

    def test_focus_is_written_to_workbench_status(self, tmp_path):
        from data.workbench import WorkbenchStore

        store = WorkbenchStore(str(tmp_path / "workbench"))
        store.mount_focus("DC-001")
        assert store.get("base.focus") == "DC-001"

        store.unmount_focus()
        assert store.get("base.focus") is None
        assert store.get("base.old_focus") == "DC-001"

    def test_status_json_with_utf8_bom_is_readable(self, tmp_path):
        from data.workbench import WorkbenchStore

        root = tmp_path / "workbench"
        root.mkdir()
        payload = {
            "base": {
                "instance_id": "WB-main",
                "focus": None,
                "old_focus": None,
                "active_task": "T-20260627-01",
                "active_guide": None,
                "step_count": 0,
                "last_checkpoint": None,
                "pending_interrupt": None,
                "settlement": {"pending": False, "level": 0, "reason": None},
            },
            "plus": {},
            "pro": {},
            "dlc": {},
            "mod": {},
        }
        (root / "status.json").write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8-sig",
        )

        store = WorkbenchStore(str(root))

        assert store.get("base.active_task") == "T-20260627-01"

    def test_spec448_next_task_id_counts_stale_task_guide_dirs(self, tmp_path):
        from constants import local_now
        from data.workbench import WorkbenchStore

        root = tmp_path / "workbench"
        today = local_now().strftime("%Y%m%d")
        stale_guide = root / "guides" / f"task__colon__T-{today}-01"
        stale_guide.mkdir(parents=True)
        (stale_guide / "ledger.jsonl").write_text("{}", encoding="utf-8")

        store = WorkbenchStore(str(root))
        task_id = store.create_task_guide_task(
            task_title="new task",
            task_goal="avoid stale guide collision",
            guide={"items": [], "acceptance": []},
        )

        assert task_id == f"T-{today}-02"


# ============================================================
# DreamStore 测试
# ============================================================

class TestDreamStore:
    def test_append_dream_writes_entry(self, tmp_path):
        from data.dream_store import DreamStore

        store = DreamStore(str(tmp_path / "dreams.md"))
        store.append_dream("梦里出现了一条未完成的链", round_num=7)

        content = (tmp_path / "dreams.md").read_text(encoding="utf-8")
        assert "R7" in content
        assert "梦里出现了一条未完成的链" in content


# ============================================================
# ContextStore 测试
# ============================================================

class TestContextStore:
    def _patch_track_paths(self, tmp_path, monkeypatch):
        from data import context_store as ctxs
        monkeypatch.setattr(ctxs, "CONTAINER_CORPUS_DIR", str(tmp_path / "corpus"))
        monkeypatch.setattr(ctxs, "STM_CONTEXT_CACHE_DIR", str(tmp_path / "cache"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_NOW_CACHE_JSONL", str(tmp_path / "cache" / "now_cache.jsonl"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_LATELY_CACHE_JSONL", str(tmp_path / "cache" / "lately_cache.jsonl"), raising=False)
        raw_jsonl = str(tmp_path / "buffer" / "raw_log.jsonl")
        raw_md = str(tmp_path / "buffer" / "raw_log.md")
        monkeypatch.setattr(ctxs, "RAW_LOG_JSONL", raw_jsonl)
        monkeypatch.setattr(ctxs, "RAW_LOG", raw_md)
        monkeypatch.setattr(ctxs, "_DEFAULT_RAW_LOG_JSONL", raw_jsonl)
        monkeypatch.setattr(ctxs, "_DEFAULT_RAW_LOG", raw_md)
        return ctxs

    @staticmethod
    def _read_jsonl(path):
        if not path.exists():
            return []
        return [
            json.loads(line)
            for line in path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]

    class _CacheConfig(ConfigStoreStub):
        def __init__(self, now_budget=32768, now_trim=8192,
                     lately_budget=65536, lately_trim=16384,
                     compact_ratio=0.618, compact_shard_chars=8192,
                     compact_shard_ratio=0.314):
            self.now_budget = now_budget
            self.now_trim = now_trim
            self.lately_budget = lately_budget
            self.lately_trim = lately_trim
            self.compact_ratio = compact_ratio
            self.compact_shard_chars = compact_shard_chars
            self.compact_shard_ratio = compact_shard_ratio

        def get_now_cache_params(self):
            return {
                "budget_chars": self.now_budget,
                "trim_chars": self.now_trim,
            }

        def get_lately_cache_params(self):
            return {
                "budget_chars": self.lately_budget,
                "trim_chars": self.lately_trim,
            }

        def get_lately_compact_ratio(self):
            return self.compact_ratio

        def get_lately_compaction_params(self):
            return {
                "compact_ratio": self.compact_ratio,
                "compact_shard_chars": self.compact_shard_chars,
                "compact_shard_ratio": self.compact_shard_ratio,
            }

        def get_now_policy_by_kind(self):
            return {}

        def get_lately_allowed_kinds(self):
            return [
                "interaction",
                "assistant_reply",
                "minimum_commitment",
                "fault_note",
                "cache_summary",
                "material",
            ]

    def test_spec676_corrupt_cache_config_is_not_silently_defaulted(
            self, tmp_path, monkeypatch):
        from data import config_store as cfs
        from data.context_store import ContextStore
        from errors import ReadError

        for name in ("now", "lately"):
            path = tmp_path / f"{name}.json"
            path.write_text("{broken", encoding="utf-8")
            monkeypatch.setitem(
                cfs._CONFIG_MAP,
                name,
                (str(path), cfs._CONFIG_MAP[name][1]),
            )

        store = ContextStore(config_store=cfs.ConfigStore())
        for load_params in (
                store._now_cache_params,
                store._lately_cache_params,
                store.get_lately_compaction_params,
                store._persistent_lanes):
            with pytest.raises(ReadError):
                load_params()


    def test_context_store_rejects_retired_ttl_policy(self):
        from data.context_store import ContextStore

        with pytest.raises(ValueError, match="ttl"):
            ContextStore._sanitize_policy({
                "now": True,
                "lately": False,
                "ttl": "now",
            })

    def test_spec463_default_cache_windows_are_expanded(self):
        from schemas.config import default_lately_config, default_now_config

        now = default_now_config()
        lately = default_lately_config()

        assert now["budget_chars"] == 65536
        assert now["trim_chars"] == 16384
        assert lately["budget_chars"] == 262144
        assert lately["trim_chars"] == 65536
        assert lately["compact_ratio"] == 0.618
        assert lately["compact_shard_chars"] == 8192
        assert lately["compact_shard_ratio"] == 0.314

    def test_context_store_drops_legacy_ttl_when_normalizing_corpus(self):
        from data.context_store import ContextStore

        store = ContextStore()
        block = store._normalize_corpus_block({
            "id": "R000001-user-0000",
            "role": "user",
            "kind": "interaction",
            "text": "历史语料",
            "loc": {"round": 1, "step": "round", "iter": 0},
            "policy": {"now": False, "lately": False, "ttl": "lately"},
            "ref": {"interaction": {
                "object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            }},
        })

        assert block["policy"] == {"now": False, "lately": False}
        assert "ttl" not in block["policy"]

    def test_save_round_writes_now_and_lately_corpus_blocks(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()

        store.save_round_to_cache(
            1,
            "第一轮输入",
            "第一轮回复",
            interaction_object="TzPz",
            identity_status="known",
            interaction_source="relation_registry",
        )
        store.save_round_to_cache(
            2,
            "第二轮输入",
            "第二轮回复",
            interaction_object="TzPz",
            identity_status="known",
            interaction_source="relation_registry",
        )

        now_blocks = self._read_jsonl(tmp_path / "cache" / "now_cache.jsonl")
        lately_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")

        assert {block["loc"]["round"] for block in now_blocks} == {1, 2}
        assert lately_blocks == []
        assert {block["kind"] for block in now_blocks} == {"interaction", "assistant_reply"}

        user_block = next(block for block in now_blocks if block["kind"] == "interaction")
        assert set(user_block) == {"id", "role", "kind", "text", "loc", "policy", "ref"}
        assert user_block["text"] == "第一轮输入"
        assert user_block["policy"] == {
            "now": True,
            "lately": True,
        }
        assert user_block["ref"]["interaction"] == {
            "object": "TzPz",
            "identity_status": "known",
            "interaction_source": "relation_registry",
        }
        assert self._read_jsonl(tmp_path / "buffer" / "raw_log.jsonl") == []

    def test_context_cache_preserves_stable_interaction_object_id(
            self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()
        meta = {
            "interaction_object_id": "REL-USER",
            "interaction_object": "用户",
            "identity_status": "known",
            "interaction_source": "instance_selection",
        }

        store.save_round_to_cache(1, "输入", "回复", **meta)
        store.append_to_cache(
            2, "assistant", "进展", kind="assistant_reply", **meta)
        store.append_call_transient(
            2,
            "assistant",
            "推理续接",
            kind="reasoning_context",
            transient_scope="reasoning_replay",
            transient_target_step="reaction",
            transient_target_iteration=2,
            **meta,
        )

        assert {
            entry.get("interaction_object_id")
            for entry in store.get_now_entries()
        } == {"REL-USER"}
        transient = store.get_call_transient_entries(
            2, "reaction", reaction_iteration=2)
        assert transient[0]["interaction_object_id"] == "REL-USER"
        blocks = self._read_jsonl(tmp_path / "cache" / "now_cache.jsonl")
        assert all(
            block["ref"]["interaction"].get("object_id") == "REL-USER"
            for block in blocks
        )

    def test_spec663_save_round_dedupes_interaction_transport_newline(
            self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()
        store.append_to_cache(
            575,
            "user",
            "请重新读取同一张技能卡。",
            kind="interaction",
            step="setup",
        )

        store.save_round_to_cache(
            575,
            "请重新读取同一张技能卡。\n",
            "",
        )

        interactions = [
            block
            for block in self._read_jsonl(tmp_path / "cache" / "now_cache.jsonl")
            if block.get("kind") == "interaction"
        ]
        assert len(interactions) == 1

    def test_tool_fact_and_minimum_stay_now_until_lately_admission(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()

        store.append_to_cache(3, "tool", "pytest 通过", kind="tool_fact", step="reaction")
        store.append_to_cache(3, "assistant", "工具结果已生效", kind="tool_summary", step="reaction")
        store.append_to_cache(3, "system", "承诺继续验证", kind="minimum_commitment", step="cleanup")
        store.append_to_cache(3, "system", "网络失败已记账", kind="fault_note", step="cleanup")

        now_blocks = self._read_jsonl(tmp_path / "cache" / "now_cache.jsonl")
        lately_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        raw_blocks = self._read_jsonl(tmp_path / "buffer" / "raw_log.jsonl")

        assert {block["kind"] for block in now_blocks} == {
            "tool_fact",
            "minimum_commitment",
            "fault_note",
        }
        assert lately_blocks == []
        assert raw_blocks == []
        assert all(block["policy"]["lately"] is True for block in now_blocks)
        assert all("backup" not in block["policy"] for block in now_blocks)
        assert all("ttl" not in block["policy"] for block in now_blocks)


    def test_latest_oversize_now_block_is_promoted_without_hard_truncation(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=10,
            now_trim=5,
            lately_budget=1024,
            lately_trim=128,
        ))

        store.append_to_cache(3, "user", "X" * 50, kind="interaction")

        now_blocks = self._read_jsonl(tmp_path / "cache" / "now_cache.jsonl")
        lately_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        raw_blocks = self._read_jsonl(tmp_path / "buffer" / "raw_log.jsonl")

        assert now_blocks == []
        assert [block["text"] for block in lately_blocks] == ["X" * 50]
        assert [block["text"] for block in raw_blocks] == ["X" * 50]

    def test_lately_character_budget_trims_oldest_blocks_and_preserves_corpus(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=20,
            now_trim=10,
            lately_budget=35,
            lately_trim=15,
        ))

        store.append_to_cache(4, "user", "A" * 12, kind="interaction", iter=1)
        store.append_to_cache(4, "assistant", "B" * 12, kind="assistant_reply", iter=2)
        store.append_to_cache(4, "assistant", "C" * 12, kind="assistant_reply", iter=3)
        store.append_to_cache(4, "assistant", "D" * 12, kind="assistant_reply", iter=4)

        lately_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        raw_blocks = self._read_jsonl(tmp_path / "buffer" / "raw_log.jsonl")
        stats = store.get_last_cache_stats()

        assert [block["text"] for block in lately_blocks] == ["D" * 12]
        assert [block["text"] for block in raw_blocks] == ["A" * 12, "B" * 12, "C" * 12, "D" * 12]
        assert stats["lately_trimmed"] is True
        assert stats["lately_deleted_blocks"] == 3

    def test_spec463_lately_trim_stats_expose_compaction_rhythm_plan(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            lately_budget=35,
            lately_trim=15,
            compact_ratio=0.618,
            compact_shard_chars=8192,
            compact_shard_ratio=0.314,
        ))
        entries = [
            {"round": 4, "role": "assistant", "kind": "assistant_reply", "content": char * 12}
            for char in ("A", "B", "C", "D")
        ]

        active, stats = store._apply_lately_watermark(entries)

        assert [entry["content"] for entry in active] == ["D" * 12]
        assert stats["lately_trimmed"] is True
        assert stats["cache_compaction_required"] is True
        assert stats["lately_surviving_chars"] == 12
        assert stats["lately_compact_target_chars"] == int(12 * 0.618)
        assert stats["lately_compact_ratio"] == 0.618
        assert stats["lately_compact_shard_chars"] == 8192
        assert stats["lately_compact_shard_ratio"] == 0.314

    def test_spec474_lately_trim_persists_compaction_debt_without_raw_text(
            self, tmp_path, monkeypatch):
        from logic.cache_compaction_guide import cache_compaction_due_receipt

        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=20,
            now_trim=10,
            lately_budget=35,
            lately_trim=15,
            compact_ratio=0.618,
            compact_shard_chars=8192,
            compact_shard_ratio=0.314,
        ))

        for index, char in enumerate(("A", "B", "C", "D"), start=1):
            store.append_to_cache(
                474,
                "assistant",
                char * 12,
                kind="assistant_reply",
                iter=index,
            )

        receipt = cache_compaction_due_receipt(store, 474)
        store.save_cache_compaction_debt(receipt, 474)
        debt_path = Path(store.cache_compaction_debt_path())
        debt = json.loads(debt_path.read_text(encoding="utf-8"))
        raw = json.dumps(debt, ensure_ascii=False)

        assert receipt["status"] == "due"
        assert debt_path.is_file()
        assert debt["schema_version"] == "cache_compaction_debt.v1"
        assert debt["status"] == "open"
        assert debt["created_round"] == 474
        assert debt["candidate_ids"]
        assert debt["compaction_plan"]["shards"]
        assert "DDDDDDDDDDDD" not in raw
        assert "CCCCCCCCCCCC" not in raw

    def test_spec463_lately_compaction_shards_stop_after_global_target(self):
        from logic.cache_compaction_guide import plan_lately_compaction_shards

        candidates = [
            {"id": "R000001-user-0000", "chars": 4000, "text": "A" * 4000},
            {"id": "R000002-assistant-0000", "chars": 4000, "text": "B" * 4000},
            {"id": "R000003-tool-0000", "chars": 4000, "text": "C" * 4000},
        ]

        plan = plan_lately_compaction_shards(
            candidates,
            compact_ratio=0.618,
            shard_chars=8192,
            shard_ratio=0.314,
        )

        assert plan["before_chars"] == 12000
        assert plan["target_chars"] == int(12000 * 0.618)
        assert len(plan["shards"]) == 1
        shard = plan["shards"][0]
        assert shard["source_block_ids"] == ["R000001-user-0000", "R000002-assistant-0000"]
        assert shard["input_chars"] == 8000
        assert shard["target_chars"] == int(8000 * 0.314)

    def test_protocol_tool_receipt_is_retired_from_model_context(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()

        store.append_to_cache(
            4,
            "system",
            "[协议工具回执]\n- memory_write: guide_loaded",
            kind="protocol_tool_receipt",
            step="reaction",
        )

        assert not (tmp_path / "cache" / "now_cache.jsonl").exists()
        assert not (tmp_path / "cache" / "lately_cache.jsonl").exists()
        assert not (tmp_path / "corpus" / "public" / "rounds").exists()

    def test_spec265_transient_display_blocks_do_not_enter_cache_tracks(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()

        for kind in [
            "popup",
            "step_guide",
            "protocol_tool_guide",
            "native_tool_feedback",
            "runtime_warning",
            "internal_correction",
            "training_material_evidence",
        ]:
            store.append_to_cache(
                6,
                "system",
                f"{kind} only visible in current call",
                kind=kind,
                step="reaction",
            )

        assert not (tmp_path / "cache" / "now_cache.jsonl").exists()
        assert not (tmp_path / "cache" / "lately_cache.jsonl").exists()
        assert not (tmp_path / "corpus" / "public" / "rounds").exists()

    def test_spec265_legacy_transient_display_blocks_are_not_loaded(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()
        cache_dir = tmp_path / "cache"
        cache_dir.mkdir(parents=True)
        (cache_dir / "now_cache.jsonl").write_text(
            json.dumps({
                "id": "R000006-system-0000",
                "role": "system",
                "kind": "popup",
                "text": "old popup should stay out of CONTENT",
                "loc": {"round": 6, "step": "reaction", "iter": 1},
                "policy": {"now": True, "lately": False},
                "ref": {"interaction": {
                    "object": "unknown",
                    "identity_status": "unknown",
                    "interaction_source": "unresolved",
                }},
            }, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        assert store.get_now_entries() == []

    def test_retired_handoff_kind_does_not_enter_context_tracks(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()

        store.append_to_cache(
            5,
            "system",
            "[心跳触发交接] 本轮由 continue_requested 唤醒：上轮超时续传。",
            kind="handoff",
            step="setup",
        )

        assert not (tmp_path / "cache" / "now_cache.jsonl").exists()
        assert not (tmp_path / "cache" / "lately_cache.jsonl").exists()
        assert not (tmp_path / "corpus" / "public" / "rounds").exists()

    def test_template_placeholders_do_not_enter_context_tracks(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()

        store.append_to_cache(6, "assistant", "`assistant_reply`", kind="assistant_reply")
        store.append_to_cache(6, "system", "`internal_handoff`", kind="handoff", step="reaction")

        assert self._read_jsonl(tmp_path / "cache" / "now_cache.jsonl") == []
        assert self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl") == []
        assert self._read_jsonl(tmp_path / "corpus" / "public" / "rounds" / "round_000006.jsonl") == []

        bad_lately_block = {
            "id": "R000218-assistant-0001",
            "role": "assistant",
            "kind": "assistant_reply",
            "text": "`assistant_reply`",
            "loc": {"round": 218, "step": "round", "iter": 0, "time": "2026-05-19T01:29:14+08:00"},
            "policy": {"now": True, "lately": True},
            "ref": {"interaction": {"object": "Codex", "identity_status": "declared", "interaction_source": "self_declaration"}},
        }
        (tmp_path / "cache" / "lately_cache.jsonl").write_text(
            json.dumps(bad_lately_block, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        assert store.get_lately_entries() == []

    def test_lately_compression_candidates_cover_full_post_trim_survivor_segment(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig())
        blocks = []
        for idx, kind in enumerate([
            "interaction",
            "assistant_reply",
            "minimum_commitment",
            "fault_note",
            "cache_summary",
        ]):
            blocks.append({
                "id": f"R000010-{kind}-{idx:04d}",
                "role": "system" if kind != "interaction" else "user",
                "kind": kind,
                "text": f"{kind} 幸存段哨兵",
                "loc": {"round": 10, "step": "cleanup", "iter": 0, "time": "2026-05-24T00:00:00+08:00"},
                "policy": {"now": True, "lately": True},
                "ref": {"interaction": {"object": "Codex", "identity_status": "declared", "interaction_source": "self_declaration"}},
            })
        (tmp_path / "cache").mkdir(parents=True, exist_ok=True)
        (tmp_path / "cache" / "lately_cache.jsonl").write_text(
            "\n".join(json.dumps(block, ensure_ascii=False) for block in blocks) + "\n",
            encoding="utf-8",
        )

        candidates = store.build_lately_compression_candidates(current_round=10, max_blocks=None)

        assert [item["id"] for item in candidates] == [block["id"] for block in blocks]
        assert {item["kind"] for item in candidates} == {
            "interaction",
            "assistant_reply",
            "minimum_commitment",
            "fault_note",
            "cache_summary",
        }
        assert sum(item["chars"] for item in candidates) > 0

    def test_cache_compact_merges_survivor_blocks_and_preserves_corpus_original(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        from logic.cache_compact import execute_cache_compact

        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=32,
            now_trim=16,
            lately_budget=1024,
            lately_trim=128,
        ))

        store.append_to_cache(2, "assistant", "pytest 输出很长，需要压缩保留重点", kind="assistant_reply")
        store.append_to_cache(2, "assistant", "第一段回复也很长，需要一起融合", kind="assistant_reply")
        store.append_to_cache(2, "system", "flush marker " * 4, kind="fault_note")
        raw_path = tmp_path / "buffer" / "raw_log.jsonl"
        raw_before = self._read_jsonl(raw_path)
        source_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        source_ids = [block["id"] for block in source_blocks]

        assert len(source_ids) >= 2
        report = execute_cache_compact(
            store,
            {
                "lately_trimmed": True,
                "compact_ratio": 0.618,
                "decisions": [{
                    "source_block_ids": source_ids[:2],
                    "action": "replace",
                    "replacement_text": "pytest 已通过，回复确认保留；两段语义融合为同一条近感摘要。",
                }],
            }
        )

        lately_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        raw_after = self._read_jsonl(raw_path)
        summary = lately_blocks[0]
        assert report["status"] == "applied"
        assert summary["kind"] == "cache_summary"
        assert "两段语义融合" in summary["text"]
        assert summary["ref"]["source_block_ids"] == source_ids[:2]
        assert summary["ref"]["raw_log_keys"] == [
            block["ref"]["raw_log_key"] for block in raw_before[:2]
        ]
        assert summary["ref"]["oldest_source_round"] == raw_before[0]["loc"]["round"]
        assert summary["ref"]["oldest_cached_at"] == raw_before[0]["loc"]["time"]
        assert summary["ref"]["source_block_count"] == 2
        assert summary["ref"]["compact_reason"] == "post_lately_trim"
        assert summary["ref"]["compacted_at"]
        assert raw_after == raw_before

    def test_spec288_cache_compact_uses_pending_ids_with_action_object(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        from logic.cache_compact import execute_cache_compact

        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=40,
            now_trim=20,
            lately_budget=1024,
            lately_trim=128,
        ))

        store.append_to_cache(2, "assistant", "第一段最近缓存内容需要融合", kind="assistant_reply")
        store.append_to_cache(2, "assistant", "第二段最近缓存内容需要融合", kind="assistant_reply")
        store.append_to_cache(2, "system", "flush marker " * 4, kind="fault_note")
        candidates = store.build_lately_compression_candidates(max_blocks=None)
        source_ids = [item["id"] for item in candidates]

        report = execute_cache_compact(
            store,
            {
                "lately_trimmed": True,
                "compact_ratio": 0.5,
                "source_block_ids": source_ids,
                "decision": {
                    "action": "replace",
                    "replacement_text": "两段最近缓存已融合。",
                    "reason": "连续语义合并",
                },
                "current_round": 12,
            },
        )

        lately_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        assert report["status"] == "applied"
        assert report["replaced"] == len(source_ids)
        assert [block["kind"] for block in lately_blocks] == ["cache_summary"]
        assert lately_blocks[0]["text"] == "两段最近缓存已融合。"
        assert lately_blocks[0]["ref"]["source_block_ids"] == source_ids

    def test_cache_compact_ratio_zero_clears_and_one_keeps_survivor_segment(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        from logic.cache_compact import execute_cache_compact

        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=32,
            now_trim=16,
            lately_budget=1024,
            lately_trim=128,
        ))
        store.append_to_cache(7, "assistant", "A" * 24, kind="assistant_reply")
        store.append_to_cache(7, "assistant", "B" * 24, kind="assistant_reply")
        before = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        raw_path = tmp_path / "buffer" / "raw_log.jsonl"
        raw_before = self._read_jsonl(raw_path)

        keep_report = execute_cache_compact(
            store,
            {"lately_trimmed": True, "compact_ratio": 1.0, "decisions": []},
        )
        assert keep_report["status"] == "skipped_ratio_1"
        assert self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl") == before

        clear_report = execute_cache_compact(
            store,
            {"lately_trimmed": True, "compact_ratio": 0.0, "decisions": []},
        )
        assert clear_report["status"] == "cleared"
        assert self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl") == []
        assert self._read_jsonl(raw_path) == raw_before

    def test_lately_compression_rewrites_lately_but_preserves_corpus_original(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=32,
            now_trim=16,
            lately_budget=1024,
            lately_trim=128,
        ))

        store.append_to_cache(2, "assistant", "pytest 输出很长，需要压缩保留重点", kind="assistant_reply")
        store.append_to_cache(2, "assistant", "flush marker " * 4, kind="assistant_reply")
        raw_path = tmp_path / "buffer" / "raw_log.jsonl"
        raw_before = self._read_jsonl(raw_path)
        candidates = store.build_lately_compression_candidates(current_round=3)

        assert candidates
        store.rewrite_lately_blocks([
            {
                "candidate_numbers": ["1"],
                "action": "replace",
                "replacement_text": "pytest 已通过，保留验证结论。",
            }
        ], current_round=3)

        lately_blocks = self._read_jsonl(tmp_path / "cache" / "lately_cache.jsonl")
        raw_after = self._read_jsonl(raw_path)
        assert "pytest 已通过" in lately_blocks[0]["text"]
        assert "pytest 输出很长" in raw_before[0]["text"]
        assert raw_after == raw_before
        assert lately_blocks[0]["kind"] == "cache_summary"
        assert lately_blocks[0]["ref"]["raw_log_keys"] == [raw_before[0]["ref"]["raw_log_key"]]
        assert lately_blocks[0]["ref"]["source_block_ids"] == [candidates[0]["id"]]

    def test_lately_cache_windows_by_step_after_track_trim(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=8,
            now_trim=4,
            lately_budget=65536,
            lately_trim=16384,
        ))

        for round_num in range(1, 42):
            store.save_round_to_cache(round_num, f"用户{round_num}", f"回复{round_num}")

        setup_rounds = sorted({entry["round"] for entry in store.get_lately_entries("setup")})
        reaction_rounds = sorted({entry["round"] for entry in store.get_lately_entries("reaction")})
        cleanup_rounds = sorted({entry["round"] for entry in store.get_lately_entries("cleanup")})

        assert setup_rounds == reaction_rounds == cleanup_rounds
        assert len(setup_rounds) > 8

    def test_save_round_does_not_write_retired_compat_projection_files(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()

        for round_num in range(1, 41):
            store.save_round_to_cache(round_num, f"用户{round_num}", f"回复{round_num}")

        assert (tmp_path / "cache" / "now_cache.jsonl").is_file()
        assert (tmp_path / "cache" / "lately_cache.jsonl").is_file()
        assert not (tmp_path / "buffer.json").exists()
        assert not (tmp_path / "cache" / "near_cache.json").exists()
        assert not (tmp_path / "cache" / "near_cache.md").exists()
        assert not (tmp_path / "cache" / "remote_index.json").exists()
        assert not (tmp_path / "cache" / "remote_blocks").exists()

    def test_save_round(self, tmp_path, monkeypatch):
        from data import context_store as ctxs
        monkeypatch.setattr(ctxs, "CONTAINER_CORPUS_DIR", str(tmp_path / "corpus"))
        monkeypatch.setattr(ctxs, "STM_CONTEXT_CACHE_DIR", str(tmp_path / "cache"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_NOW_CACHE_JSONL", str(tmp_path / "cache" / "now_cache.jsonl"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_LATELY_CACHE_JSONL", str(tmp_path / "cache" / "lately_cache.jsonl"), raising=False)

        store = ctxs.ContextStore()
        store.save_round_to_cache(
            5, "输入", "回复",
            interaction_object="TzPz",
            identity_status="known",
            interaction_source="relation_registry",
        )
        buf = store.get_now_entries()
        assert len(buf) == 2
        assert buf[0]["role"] == "user"
        assert buf[1]["role"] == "assistant"
        assert {entry["interaction_object"] for entry in buf} == {"TzPz"}
        assert {entry["identity_status"] for entry in buf} == {"known"}

# ============================================================
# AlertStore 测试
# ============================================================

    # Spec625 supersedes the former round-retention settlement contract.
    def test_material_immediate_watermark_moves_to_lately_without_raw(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=32, now_trim=16, lately_budget=1024, lately_trim=128,
        ))
        store.append_to_cache(9, "system", "A" * 18, kind="material")
        store.append_to_cache(9, "system", "B" * 18, kind="material")
        assert store.get_now_entries() == []
        assert [entry["content"] for entry in store.get_lately_entries()] == ["A" * 18, "B" * 18]
        assert self._read_jsonl(tmp_path / "corpus" / "public" / "rounds" / "round_000009.jsonl") == []

    def test_spec288_cleanup_round_material_is_transient_now_only(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()
        store.append_call_transient(
            12, "system", "cleanup", kind="material", step="cleanup",
            transient_scope="cleanup_round", transient_target_step="cleanup",
        )
        assert store.get_now_entries() == []
        entries = store.get_call_transient_entries(12, "cleanup")
        assert len(entries) == 1
        assert entries[0]["content"] == "cleanup"
        assert store.clear_transient_entries(
            round_num=12, transient_scope="cleanup_round",
            transient_target_step="cleanup",
        )["now_removed"] == 1

    def test_file_read_tool_fact_remains_in_persistent_a_lane(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()
        store.append_to_cache(10, "tool", "file_read cursor=81", kind="tool_fact")
        assert [entry["content"] for entry in store.get_now_entries()] == ["file_read cursor=81"]

    def test_rejected_file_read_fact_remains_in_persistent_a_lane(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()
        store.append_to_cache(11, "tool", "file_read rejected", kind="tool_fact")
        assert [entry["content"] for entry in store.get_now_entries()] == ["file_read rejected"]

    def test_spec623_current_round_material_overflow_is_pinned_until_settlement(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=50, now_trim=15, lately_budget=1024, lately_trim=128,
        ))
        for value in ("A", "B", "C"):
            store.append_to_cache(2, "system", value * 18, kind="material")
        assert [entry["content"] for entry in store.get_now_entries()] == ["C" * 18]
        assert [entry["content"] for entry in store.get_lately_entries()] == ["A" * 18, "B" * 18]
        assert self._read_jsonl(tmp_path / "corpus" / "public" / "rounds" / "round_000002.jsonl") == []
        assert store.get_last_cache_stats()["now_moved_blocks"] == 2

    def test_spec623_seven_16k_material_windows_survive_one_round(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=65536, now_trim=16384, lately_budget=262144, lately_trim=65536,
        ))
        windows = [f"W{index}:" + chr(64 + index) * (16384 - 3) for index in range(1, 8)]
        for iteration, content in enumerate(windows, start=1):
            store.append_to_cache(623, "system", content, kind="material", iter=iteration)
        visible = store.get_lately_entries() + store.get_now_entries()
        assert [entry["content"] for entry in visible] == windows
        assert store.get_round_material_chars(623) == sum(map(len, windows))
        assert self._read_jsonl(tmp_path / "corpus" / "public" / "rounds" / "round_000623.jsonl") == []

    def test_spec623_material_is_not_a_lately_compaction_candidate(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=8, now_trim=4, lately_budget=1024, lately_trim=128,
        ))
        store.append_to_cache(623, "system", "complete material", kind="material")
        assert store.build_lately_compression_candidates() == []

    def test_spec623_settlement_preserves_compaction_signal_for_ordinary_survivors(self, tmp_path, monkeypatch):
        from logic.cache_compaction_guide import cache_compaction_due_receipt
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=35, now_trim=5, lately_budget=100, lately_trim=20,
        ))
        for content in ("A" * 30, "B" * 30, "C" * 30, "D" * 30, "E" * 30):
            store.append_to_cache(1, "user", content, kind="interaction")
        stats = store.get_last_cache_stats()
        assert stats["lately_trimmed"] is True
        assert cache_compaction_due_receipt(store, 1)["status"] == "due"

    def test_spec623_material_only_lately_trim_does_not_create_compaction_due(self, tmp_path, monkeypatch):
        from logic.cache_compaction_guide import cache_compaction_due_receipt
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=10, now_trim=5, lately_budget=100, lately_trim=20,
        ))
        store.append_to_cache(2, "system", "A" * 70, kind="material")
        store.append_to_cache(2, "system", "B" * 70, kind="material")
        due = cache_compaction_due_receipt(store, 2)
        assert due["status"] == "skipped"
        assert due["reason"] == "no_compaction_shards"

    def test_reasoning_context_is_now_only_with_native_replay(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore()
        replay = {"provider": "openai_chat", "assistant_message": {"role": "assistant"}}
        store.append_reasoning_context(5, "reasoning", native_replay=replay, iter=0)
        assert store.get_now_entries() == []
        entries = store.get_call_transient_entries(5, "reaction", reaction_iteration=2)
        assert len(entries) == 1
        assert entries[0]["native_replay"] == replay
        assert store.clear_transient_entries(
            round_num=5, transient_scope="reasoning_replay",
            transient_target_step="reaction", transient_target_iteration=2,
        )["now_removed"] == 1

    def test_reasoning_context_overflow_drops_whole_block_without_lately(self, tmp_path, monkeypatch):
        ctxs = self._patch_track_paths(tmp_path, monkeypatch)
        store = ctxs.ContextStore(config_store=self._CacheConfig(
            now_budget=50, now_trim=15, lately_budget=1024, lately_trim=128,
        ))
        for value in ("A", "B", "C"):
            store.append_reasoning_context(6, value * 18, native_replay={"provider": "openai_chat"})
        entries = store.get_call_transient_entries(6, "reaction", reaction_iteration=2)
        assert [entry["content"][-18:] for entry in entries] == ["A" * 18, "B" * 18, "C" * 18]
        assert store.get_lately_entries() == []


class TestAlertStore:
    def test_append_alert_uses_dds_markdown_line(self, tmp_path, monkeypatch):
        from data import alert_store as alerts
        monkeypatch.setattr(alerts, "ALERTS_MD", str(tmp_path / "alerts.md"))

        store = alerts.AlertStore()
        store.append_alert(
            round_num=7,
            step="cleanup",
            event_type="l3_cleanup_api_failure",
            detail="timeout | multiline\nmessage",
            action="script_emergency_save",
        )

        content = (tmp_path / "alerts.md").read_text(encoding="utf-8")
        assert "<!-- 意外事件日志 -->" in content
        assert "round=00007" in content
        assert "step=cleanup" in content
        assert "type=l3_cleanup_api_failure" in content
        assert "detail=timeout / multiline message" in content
        assert "action=script_emergency_save" in content
        assert not (tmp_path / "alerts.md.tmp").exists()
