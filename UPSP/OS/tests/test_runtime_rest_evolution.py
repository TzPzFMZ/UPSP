import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


class TestRuntimeRestEvolution(RuntimeTestMixin):
    def test_runtime_default_assembler_uses_runtime_state_store(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        rt = Runtime(state_store=sm)

        assert rt.assembler.state_store is sm
        assert rt.assembler.popup.state_store is sm

    def test_runtime_accepts_workbench_store_for_focus(self, tmp_path):
        from engines.runtime import Runtime
        from data.workbench import WorkbenchStore

        wb = WorkbenchStore(str(tmp_path / "workbench"))
        rt = Runtime(workbench_store=wb)

        assert rt.workbench is wb

    def test_runtime_rest_cycle_clears_fatigue_after_non_sleep_round(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore
        from data.dream_store import DreamStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.fatigue.value", 44.0)
        sm.set("base.fatigue.awake_since", "2026-07-01T00:00:00+08:00")
        sm.set("base.sleep_state.level", "awake")
        rt = Runtime(state_store=sm, dream_store=DreamStore(str(tmp_path / "dreams.md")))

        rt._process_rest_cycle("interactive", sm.load(), {"tokens_input": 1000}, 1)

        assert sm.get("base.fatigue.value") == 0
        assert sm.get("base.fatigue.awake_since") is None
        assert sm.get("base.sleep_state.level") == "awake"

    def test_runtime_rest_cycle_clears_fatigue_expired_without_sleep(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore
        from data.dream_store import DreamStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.fatigue.value", 60.0)
        state = sm.load()
        state["base"]["heartbeat_flags"]["fatigue_expired"] = True
        sm.save(state)
        rt = Runtime(state_store=sm, dream_store=DreamStore(str(tmp_path / "dreams.md")))

        rt._process_rest_cycle("autonomous", sm.load(), {"response": ""}, 2)

        assert sm.get("base.sleep_state.level") == "awake"
        assert sm.get("base.sleep_state.entered_at") is None
        assert sm.get("base.fatigue.value") == 0
        assert sm.get("base.heartbeat_flags.fatigue_expired") is False

    def test_runtime_rest_cycle_does_not_write_dreams(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore
        from data.dream_store import DreamStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.fatigue.value", 60.0)
        state = sm.load()
        state["base"]["heartbeat_flags"]["fatigue_expired"] = True
        sm.save(state)
        dreams = tmp_path / "dreams.md"
        rt = Runtime(state_store=sm, dream_store=DreamStore(str(dreams)))

        rt._process_rest_cycle(
            "autonomous",
            sm.load(),
            {"response": "<!-- DREAM -->梦里有一张待整理的 WB 面单<!-- /DREAM -->"},
            3,
        )

        assert not dreams.exists() or "梦里有一张待整理的 WB 面单" not in dreams.read_text(encoding="utf-8")
        assert sm.get("base.sleep_state.level") == "awake"

    def test_runtime_wake_if_sleeping_clears_legacy_fatigue_pressure(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.update_many({
            "base.sleep_state.level": "moderate",
            "base.sleep_state.entered_at": "2026-07-01T00:00:00+08:00",
            "base.fatigue.value": 77.0,
            "base.fatigue.awake_since": "2026-07-01T00:00:00+08:00",
            "base.heartbeat_flags.fatigue_expired": True,
        })
        rt = Runtime(state_store=sm)

        rt._wake_if_sleeping()

        assert sm.get("base.sleep_state.level") == "awake"
        assert sm.get("base.sleep_state.entered_at") is None
        assert sm.get("base.fatigue.value") == 0
        assert sm.get("base.fatigue.awake_since") is None
        assert sm.get("base.heartbeat_flags.fatigue_expired") is False

    def test_chronicle_state_sample_no_longer_projects_fatigue(self):
        from engines.runtime import Runtime

        sample = Runtime._chronicle_state_sample({
            "fatigue": {"value": 80},
            "workhood_index": {"value": 3},
        })

        assert "fatigue" not in sample

    @pytest.mark.xfail(
        strict=True,
        reason=(
            "YELLOW Spec410: 进化集整理仍是未上线早期 markdown 块设计，"
            "不得按旧 internal_handoff 路径补回；后续另开 spec 重设。"
        ),
    )
    def test_runtime_autonomous_reaction_includes_evolution_context(self, tmp_path, monkeypatch):
        from engines.runtime import Runtime
        from data.state_store import StateStore
        from data.evolution_store import EvolutionStore

        class CapturingExecutor:
            def __init__(self):
                self.messages = None
                self.calls = []

            def call(self, step, system, messages):
                self.messages = messages
                self.calls.append(list(messages))
                return {"response": "#done"}

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        evolution = EvolutionStore(str(tmp_path / "Iteration"))
        pending = tmp_path / "Iteration" / "Raw" / "Tacit" / "pending.jsonl"
        pending.parent.mkdir(parents=True)
        pending.write_text('{"item_id":"MEM-A","action":"kept"}\n', encoding="utf-8")

        rt = self._make_runtime(tmp_path)
        rt.evolution_store = evolution
        executor = CapturingExecutor()
        rt.executor = executor
        def fake_assemble_reaction(*args, **kwargs):
            return "sys", list(kwargs.get("internal_handoff") or [])

        monkeypatch.setattr(rt.assembler, "assemble_reaction", fake_assemble_reaction)
        monkeypatch.setattr(rt, "_load_evolution_thresholds", lambda: {
            "tacit_pending_threshold": 1,
            "connection_pending_threshold": 99,
        })

        result = rt._run_reaction_loop(sm.load(), "autonomous", [])

        assert result["_evolution_requested"] is True
        assert any(
            "进化集整理任务" in m.get("content", "")
            for call in executor.calls
            for m in call
        )

    def test_runtime_processes_evolution_block_and_moves_pending(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore
        from data.evolution_store import EvolutionStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        evolution = EvolutionStore(str(tmp_path / "Iteration"))
        pending = tmp_path / "Iteration" / "Raw" / "Tacit" / "pending.jsonl"
        pending.parent.mkdir(parents=True)
        pending.write_text('{"item_id":"MEM-A","action":"kept"}\n', encoding="utf-8")
        rt = Runtime(state_store=sm, evolution_store=evolution)

        rt._process_evolution_set(
            "autonomous",
            sm.load(),
            {
                "response": "<!-- EVOLUTION -->\n稳定模式：MEM-A 被持续沿用。\n<!-- /EVOLUTION -->",
                "_evolution_requested": True,
                "_evolution_stats": {"tacit_count": 1, "connection_count": 0},
            },
            10,
        )

        assert (tmp_path / "Iteration" / "Materials" / "Evolution" / "evolution_R10.md").is_file()
        assert pending.read_text(encoding="utf-8") == ""
        assert '"item_id":"MEM-A"' in (
            tmp_path / "Iteration" / "Raw" / "Tacit" / "processed.jsonl"
        ).read_text(encoding="utf-8")
        batches = list((tmp_path / "Iteration" / "Raw" / "Tacit").glob("processed_*_R10.jsonl"))
        assert len(batches) == 1
        assert '"item_id":"MEM-A"' in batches[0].read_text(encoding="utf-8")
