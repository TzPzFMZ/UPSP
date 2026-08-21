import os
import sys
from pathlib import Path

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

    def test_runtime_has_no_active_evolution_surface(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        rt = Runtime(state_store=sm)

        assert "evolution_store" not in Runtime._SERVICE_ATTRS
        assert not hasattr(rt, "evolution_store")
        assert not hasattr(rt, "_process_evolution_set")
        assert "evolution_pending" not in sm.get_flags()

    def test_historical_evolution_marker_is_plain_text(self):
        production_root = Path(__file__).parents[1]
        sources = "\n".join(
            path.read_text(encoding="utf-8")
            for path in production_root.rglob("*.py")
            if "tests" not in path.parts and "__pycache__" not in path.parts
        )

        assert "<!-- EVOLUTION" not in sources
        assert "<!-- FORGET:" not in sources
        assert "<!-- LTM_DEGRADE:" not in sources

    def test_cleanup_no_longer_writes_retired_tacit_or_connection_raw(self):
        cleanup_source = (
            Path(__file__).parents[1] / "engines" / "cleanup_pipeline.py"
        ).read_text(encoding="utf-8")

        assert "write_tacit_set" not in cleanup_source
        assert "write_connection_set" not in cleanup_source
        assert "TACIT_SET_DIR" not in cleanup_source
        assert "CONNECTION_SET_DIR" not in cleanup_source
