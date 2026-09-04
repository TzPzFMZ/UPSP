import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


def _minimal_reaction_context(runtime, monkeypatch):
    assembler = runtime.assembler
    monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
    monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")


def test_spec561_empty_response_is_not_missing_reaction_finalize():
    from engines.reaction_iteration import parse_reaction_iteration_result

    parsed = parse_reaction_iteration_result(
        {"response": "", "tool_call_envelopes": []},
        active_protocol_tool_guides=[],
    ).parsed_reaction
    invalids = parsed.get("invalid_tool_requests") or []

    assert any(item.get("reason") == "reaction_empty_output" for item in invalids)
    assert not any(
        item.get("source") == "missing_provider_tool_call"
        and item.get("tool_id") == "reaction_finalize"
        for item in invalids
    )


class TestSpec561CloseoutLaneRetirement(RuntimeTestMixin):
    def test_empty_response_retries_then_provider_model_empty_output_blocked(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)

        class EmptyExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(step)
                self.guides.append(list(active_protocol_tool_guides or []))
                if step == "final_reply":
                    raise AssertionError("empty output auto-block must not call final_reply")
                assert "legacy-retired-guide" not in list(active_protocol_tool_guides or [])
                return {"response": "", "tool_call_envelopes": []}

        executor = EmptyExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == ["reaction", "reaction", "reaction"]
        assert all("legacy-retired-guide" not in guides for guides in executor.guides)
        assert result["_exit_signal"] == "done"
        assert "provider_model_format_empty_output" in result["response"]
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "blocked"
        assert result["_settlement_ledgers"][-1]["blocked_reason"] == (
            "provider_model_format_empty_output"
        )
        assert [
            receipt.get("correction")
            for receipt in result["_reaction_loop_guard_receipts"]
            if receipt.get("status") == "reaction_empty_output"
        ] == ["reminder", "warning", "auto_block"]
        assert any(
            receipt.get("status") == "reaction_empty_output_auto_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert not any(
            receipt.get("status") == "reaction_closeout_protocol_auto_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
