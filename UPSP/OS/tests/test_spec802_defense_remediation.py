"""故障必须暴露，未提交的状态不能伪装为完成。"""
import json
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from UPSP.OS.tests.runtime_test_helpers import RuntimeTestMixin


def test_metadata_read_failure_does_not_commit_heat_decay(tmp_path, monkeypatch):
    from data import memory_heat, memory_store
    from errors import ReadError

    heat_path, meta_path = tmp_path / "heat.json", tmp_path / "meta.json"
    monkeypatch.setattr(memory_heat, "HEAT_JSON", str(heat_path))
    monkeypatch.setattr(memory_store, "META_JSON", str(meta_path))
    store = memory_heat.MemoryHeat(memory_store=memory_store.MemoryStore())
    entry = store.new_entry(weight=5)
    entry.update(H=80, zone="显著")
    store.set_entry("MEM-TEST802", entry)
    meta_path.write_text("{broken", encoding="utf-8")
    before = heat_path.read_bytes()

    with pytest.raises(ReadError):
        store.tick_decay(round_num=802)
    assert heat_path.read_bytes() == before

    meta_path.write_text(json.dumps({
        "MEM-TEST802": {"created_round": 1, "last_recalled_round": 802},
    }), encoding="utf-8")
    assert store.tick_decay(round_num=802) is True
    assert store.load_heat()["entries"]["MEM-TEST802"]["H"] == 80
    assert store.tick_decay(round_num=802) is False


def test_admission_metadata_failure_is_not_empty_truth(tmp_path, monkeypatch):
    from data import memory_heat

    def fail():
        raise OSError("metadata unavailable")

    monkeypatch.setattr(memory_heat, "HEAT_JSON", str(tmp_path / "heat.json"))
    store = memory_heat.MemoryHeat(memory_store=SimpleNamespace(active_ltm_meta_by_id=fail))
    with pytest.raises(OSError, match="metadata unavailable"):
        store.check_upgrade()


def test_stm_index_read_failure_is_not_empty_index(tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data import memory_heat
    from errors import ReadError

    heat_path = tmp_path / "heat.json"
    monkeypatch.setattr(memory_heat, "HEAT_JSON", str(heat_path))
    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
    assert "无 STM 条目" in assembler.build_index_view("stm_heat")["content"]
    heat_path.write_text("{broken", encoding="utf-8")
    with pytest.raises(ReadError):
        assembler.build_index_view("stm_heat")
    assert "读取失败" in assembler._build_high_freq(
        {}, "reaction", "interactive", content_scope="none", mount_ids=[])


def test_periodic_config_failure_blocks_required_assembly(monkeypatch):
    from assembly.context import ContextAssembler
    from data.periodic_mount_store import PeriodicMountStore
    from errors import RequiredContextError
    from logic.periodic_memory_mount import PeriodicMemoryMountError, PeriodicMemoryMountProcessor

    def fail():
        raise OSError("configuration unavailable")

    cfg = SimpleNamespace(get_periodic_limits=fail)
    monkeypatch.setattr(PeriodicMountStore, "load", lambda self: {"periodic_memory_items": []})
    assembler = ContextAssembler(config_store=cfg)
    with pytest.raises(RequiredContextError, match="periodic_memory_mounts") as caught:
        assembler._build_periodic({}, "reaction", "interactive")
    assert isinstance(caught.value.__cause__, OSError)
    manager = PeriodicMemoryMountProcessor(
        memory_store=assembler.memory_store, heat=assembler.memory_heat,
        assembler=assembler, config_store=cfg)
    with pytest.raises(PeriodicMemoryMountError, match="periodic_memory_limit_unavailable"):
        manager._limit()


class TestRelayCommit(RuntimeTestMixin):
    @pytest.mark.parametrize("source", ["closeout_form", "runtime_auto_continue"])
    @pytest.mark.parametrize("round_type", ["interactive", "relay"])
    def test_intent_sequence_flag_and_consumed_input_commit_together(self, tmp_path, monkeypatch, source, round_type):
        from engines.reaction_terminal_state import build_reaction_handoff_relay_receipt

        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("user_message_waiting", True)
        request = build_reaction_handoff_relay_receipt(
            {"handoff_text": "继续已验证任务"}, trace={"source": source})
        assert rt.sm.get_flags()["continue_requested"] is False
        result = {"_closeout_relay_receipts": [request]}
        if round_type == "relay":
            rt.cleanup_pipeline._finalize_flags(rt.sm.load(), "interactive", 801, dict(result))
        before = rt.sm.read_snapshot()
        original_save = rt.sm.save

        def fail_commit(state):
            runtime = state["base"]["runtime"]
            count = 2 if round_type == "relay" else 1
            assert runtime["relay_intent_seq"] == count
            assert len(runtime["relay_intents"]) == count
            if round_type == "relay":
                assert runtime["relay_intents"][0]["status"] == "merged"
            assert state["base"]["heartbeat_flags"]["continue_requested"] is True
            raise OSError("commit unavailable")

        monkeypatch.setattr(rt.sm, "save", fail_commit)
        with pytest.raises(OSError, match="commit unavailable"):
            rt.cleanup_pipeline._finalize_flags(rt.sm.load(), round_type, 802, result)
        assert rt.sm.read_snapshot() == before
        assert rt.sm.load() == before
        assert "_heartbeat_rearm_receipts" not in result

        monkeypatch.setattr(rt.sm, "save", original_save)
        rt.cleanup_pipeline._finalize_flags(rt.sm.load(), round_type, 802, result)
        state = rt.sm.read_snapshot()["base"]
        assert state["runtime"]["relay_intent_seq"] == (2 if round_type == "relay" else 1)
        assert state["runtime"]["relay_intents"][-1]["status"] == "open"
        assert state["heartbeat_flags"]["continue_requested"] is True
        assert state["heartbeat_flags"]["user_message_waiting"] is False
        assert result["_heartbeat_rearm_receipts"][0]["status"] == "continue_requested_rearmed"
        assert result["_heartbeat_rearm_receipts"][0]["source"] == source
