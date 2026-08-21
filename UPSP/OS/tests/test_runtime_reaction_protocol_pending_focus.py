import os
import sys


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin
from test_runtime_reaction_protocol_write_tools import _logical_step


class TestRuntimeReactionProtocolFocus(RuntimeTestMixin):
    def test_spec338_duplicate_container_focus_open_is_satisfied_guarded(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(
            assembler, "_cached_or_build", lambda *args, **kwargs: ""
        )
        monkeypatch.setattr(
            assembler, "_build_high_freq", lambda *args, **kwargs: ""
        )
        monkeypatch.setattr(
            assembler, "_get_lately_entries", lambda *args, **kwargs: []
        )
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        new_dirs = {
            prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR
        }
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(
            cs,
            "CONTAINER_REGISTRY_JSON",
            str(tmp_path / "container_registry.json"),
        )
        monkeypatch.setattr(
            cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False
        )
        store = cs.ContainerStore()
        container_id = store.create_focus_container(
            "DC", "Spec338 focus guard", target_file="open.md"
        )["container_id"]
        rt.container_store = store
        helper = self

        class DuplicateFocusExecutor:
            def __init__(self):
                self.calls = []

            def call(
                    self, step, system, messages,
                    active_protocol_tool_guides=None):
                logical_step = _logical_step(
                    step, active_protocol_tool_guides
                )
                self.calls.append((logical_step, list(messages)))
                reaction_count = len([
                    call for call in self.calls if call[0] == "reaction"
                ])
                if reaction_count in {1, 2}:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "container_focus",
                            {
                                "action": "open",
                                "container_id": container_id,
                                "reason": (
                                    "first open" if reaction_count == 1
                                    else "same focus again"
                                ),
                            },
                            call_id=f"call_focus_open_{reaction_count}",
                            tool_family="protocol_tool",
                            tool_class="focus_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "duplicate focus guarded",
                    "tool_call_envelopes": [],
                }

        executor = DuplicateFocusExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        receipts = [
            receipt for receipt in result["_container_focus_receipts"]
            if receipt.get("tool_id") == "container_focus"
        ]
        assert [receipt.get("status") for receipt in receipts] == [
            "applied", "rejected"
        ]
        assert receipts[1]["reason"] == "duplicate_container_focus_satisfied"
        assert receipts[1]["duplicate_of_call_id"] == "call_focus_open_1"
        assert rt.workbench.get("base.focus") == container_id
        reaction_text = "\n".join(
            message.get("content", "")
            for step, messages in executor.calls
            if step == "reaction"
            for message in messages
        )
        assert "工具循环警告" in reaction_text
        assert "容器焦点工具已有同一工具结果" in reaction_text
