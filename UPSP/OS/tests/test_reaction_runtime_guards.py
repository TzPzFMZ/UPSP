import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def test_spec565_empty_output_feedback_and_detection():
    from engines.reaction_runtime_guards import (
        has_reaction_empty_output,
        reaction_empty_output_feedback,
    )

    parsed = {
        "invalid_tool_requests": [
            {"tool_id": "reaction", "reason": "reaction_empty_output"}
        ]
    }

    assert has_reaction_empty_output(parsed) is True
    warning = reaction_empty_output_feedback("warning")
    assert "空输出警告" in warning
    assert "provider_model_format_empty_output" in warning
    assert "reaction_finalize(handoff_text)" in warning


def test_spec565_duplicate_guard_helpers_group_and_strip_feedback():
    from engines.reaction_runtime_guards import (
        format_general_tool_duplicate_guard_feedback,
        group_duplicate_general_tool_results,
        remove_general_tool_duplicate_feedbacks,
        with_guard_duplicate_reference,
    )

    prior = {
        "tool_id": "file_edit",
        "call_id": "call_old",
        "status": "rejected",
        "reason": "duplicate_tool_failure_repeated",
        "duplicate_guard_key": "file_edit:x",
        "duplicate_guard_payload": {
            "tool_id": "file_edit",
            "arguments": {"path": "x.txt"},
        },
    }
    current = with_guard_duplicate_reference(
        {
            "tool_id": "file_edit",
            "call_id": "call_new",
            "status": "rejected",
            "reason": "duplicate_tool_failure_repeated",
            "duplicate_guard_key": "file_edit:x",
            "duplicate_guard_payload": {
                "tool_id": "file_edit",
                "arguments": {"path": "x.txt"},
            },
        },
        [prior],
    )

    assert current["duplicate_of_call_id"] == "call_old"
    grouped = group_duplicate_general_tool_results([current])
    assert list(grouped) == ["file_edit:x"]
    feedback = format_general_tool_duplicate_guard_feedback(
        items=[current],
        tier="warning",
        streak_count=2,
        effective_new_progress=False,
        multi_signature_count=1,
        has_main_chain=True,
        closeout_next=False,
    )
    assert "工具重复警告" in feedback
    assert "重复对象：call_old。" in feedback
    assert remove_general_tool_duplicate_feedbacks([
        feedback,
        "keep this feedback",
    ]) == ["keep this feedback"]


def test_spec774_file_edit_guard_tracks_only_handler_failures():
    from engines.reaction_runtime_guards import (
        general_tool_guard_failure_trackable,
    )

    base = {
        "tool_id": "file_edit",
        "status": "rejected",
        "reason": "patch_apply_failed",
        "duplicate_guard_key": "same-path-and-reason",
    }

    assert general_tool_guard_failure_trackable(
        dict(base, dispatch_stage="handler")) is True
    assert general_tool_guard_failure_trackable(
        dict(base, dispatch_stage="capability_gate")) is False
    assert general_tool_guard_failure_trackable(
        dict(base, dispatch_stage="frame_budget")) is False
    assert general_tool_guard_failure_trackable(base) is True


def test_spec565_provider_interruption_recovery_sets_continue_flag():
    from engines.reaction_runtime_guards import recover_provider_interruption_if_possible

    class Workbench:
        def get(self, key):
            if key == "base.active_task":
                return "T-1"
            return ""

    class StateManager:
        def __init__(self):
            self.flags = {}
            self.state = {"base": {"runtime": {}}}

        def load(self):
            return self.state

        def _set_internal(self, path, payload):
            assert path == "base.runtime.provider_interruption_recovery"
            self.state["base"]["runtime"]["provider_interruption_recovery"] = payload

        def set_flag(self, name, value):
            self.flags[name] = value

    facts = []

    receipt = recover_provider_interruption_if_possible(
        RuntimeError("provider_native_tool_empty_output"),
        state_manager=StateManager(),
        workbench=Workbench(),
        append_to_context_cache=lambda *args, **kwargs: facts.append((args, kwargs)),
        round_num=7,
        iteration=2,
        general_tool_results=[{"status": "ok", "tool_id": "file_read"}],
        interaction_meta={},
    )

    assert receipt["status"] == "provider_interruption_recovered"
    assert receipt["provider_error_kind"] == "provider_native_tool_empty_output"
    assert receipt["set_flags"] == ["continue_requested"]
    assert facts
