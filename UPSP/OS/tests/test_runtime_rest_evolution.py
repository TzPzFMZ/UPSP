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

    def test_runtime_fatigue_hooks_are_inert(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        before = rt.sm.load()
        assert rt._process_rest_cycle() is None
        assert rt._wake_if_sleeping() is None
        assert rt.sm.load() == before

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
