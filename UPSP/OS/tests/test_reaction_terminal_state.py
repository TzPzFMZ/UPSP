import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def test_spec564_project_auto_blocked_empty_output_final_response():
    from engines.reaction_terminal_state import project_reaction_terminal_response

    text, done, source = project_reaction_terminal_response(
        [
            {
                "closeout_decision": "blocked",
                "auto_blocked": True,
                "blocked_reason": "provider_model_format_empty_output",
                "blockers": ["reaction_empty_output"],
            }
        ],
        assistant_text="",
    )

    assert done is True
    assert source == "reaction.runtime_auto_blocked_final_reply"
    assert "provider_model_format_empty_output" in text
    assert "连续没有返回工具调用" in text


def test_spec564_runtime_auto_continue_sets_relay_flag():
    from engines.reaction_terminal_state import build_runtime_auto_continue_closeout

    class StateManager:
        def __init__(self):
            self.flags = {}

        def set_flag(self, name, value):
            self.flags[name] = value

    sm = StateManager()
    relay, ledger, guard = build_runtime_auto_continue_closeout(
        sm,
        elapsed_seconds=1900,
        time_limit_seconds=600,
    )

    assert sm.flags["continue_requested"] is True
    assert relay["status"] == "continue_requested_set"
    assert relay["source"] == "runtime_auto_continue"
    assert ledger["closeout_decision"] == "continue"
    assert ledger["source"] == "runtime_auto_continue"
    assert guard["set_flags"] == ["continue_requested"]


def test_spec564_natural_final_reply_candidate_blocks_write_pending():
    from engines.reaction_terminal_state import validate_natural_final_reply_candidate

    class WritePendingTracker:
        def finalize_blocker(self):
            return {
                "blocked": True,
                "reason": "write_pending_unresolved",
                "pendings": [
                    {
                        "pending_id": "WP-1",
                        "settlement_stage": "settlement_required",
                    }
                ],
            }

    result = validate_natural_final_reply_candidate(
        closeout_form_validator=lambda form: {
            "blocked": False,
            "settlement_ledger": {"validated": True},
        },
        write_pending_tracker=WritePendingTracker(),
        current_state={},
        round_type="interactive",
        runtime_guide_completed_flags=set(),
        current_runtime_guide_pending_flags=lambda state, completed_flags=None: ({}, []),
        task_closeout_acceptance=lambda form: {"allowed": True},
    )

    assert result["allowed"] is False
    assert result["status"] == "write_pending_blocked"
    assert result["source"] == "natural_final_reply_candidate"
    assert "失败写入结算门禁" in result["feedback"]
    assert "WP-1" in result["feedback"]


def test_spec606_natural_blocked_reply_records_deferred_subject_resolution():
    from engines.reaction_terminal_state import validate_natural_final_reply_candidate
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=606, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "subject_not_confirmed",
        "call_id": "call_spec606_terminal",
        "title": "对象未确认",
        "subject": "Other",
        "confirmed_subject": "TzPz",
    }])

    result = validate_natural_final_reply_candidate(
        closeout_form_validator=lambda form: {
            "blocked": False,
            "settlement_ledger": {"validated": True},
        },
        write_pending_tracker=tracker,
        current_state={},
        round_type="interactive",
        runtime_guide_completed_flags=set(),
        current_runtime_guide_pending_flags=(
            lambda state, completed_flags=None: ({}, [])
        ),
        task_closeout_acceptance=lambda form: {"allowed": True},
    )

    assert result["allowed"] is True
    ledger = result["settlement_ledger"]
    assert ledger["write_status"] == "subject_resolution_waiting_for_user"
    assert ledger["write_applied"] is False
    assert ledger["deferred_write_reasons"] == ["subject_not_confirmed"]


def test_spec571_missing_access_evidence_allows_task_bootstrap_natural_reply():
    from engines.reaction_terminal_state import validate_natural_final_reply_candidate

    class WritePendingTracker:
        def finalize_blocker(self):
            return {"blocked": False}

    result = validate_natural_final_reply_candidate(
        closeout_form_validator=lambda form: {
            "blocked": False,
            "settlement_ledger": {"validated": True},
        },
        write_pending_tracker=WritePendingTracker(),
        current_state={},
        round_type="interactive",
        runtime_guide_completed_flags=set(),
        current_runtime_guide_pending_flags=lambda state, completed_flags=None: ({}, []),
        task_closeout_acceptance=lambda form: {
            "allowed": False,
            "reason": "task_bootstrap_pending",
            "blockers": ["task_bootstrap"],
        },
        prior_general_tool_results=[
            {
                "tool_id": "file_read",
                "status": "rejected",
                "reason": "outside_allowlist",
                "path": r"D:\secret\book.md",
                "call_id": "call_denied_read",
            }
        ],
    )

    assert result["allowed"] is True
    ledger = result["settlement_ledger"]
    assert ledger["closeout_decision"] == "finish"
    assert ledger["source"] == "natural_final_reply_candidate"
    assert ledger["task_bootstrap_missing_access_final_reply"] is True
    assert ledger["missing_access_tool_id"] == "file_read"
    assert ledger["missing_access_reason"] == "outside_allowlist"


def test_spec571_task_bootstrap_still_blocks_without_access_failure_evidence():
    from engines.reaction_terminal_state import validate_natural_final_reply_candidate

    class WritePendingTracker:
        def finalize_blocker(self):
            return {"blocked": False}

    result = validate_natural_final_reply_candidate(
        closeout_form_validator=lambda form: {
            "blocked": False,
            "settlement_ledger": {"validated": True},
        },
        write_pending_tracker=WritePendingTracker(),
        current_state={},
        round_type="interactive",
        runtime_guide_completed_flags=set(),
        current_runtime_guide_pending_flags=lambda state, completed_flags=None: ({}, []),
        task_closeout_acceptance=lambda form: {
            "allowed": False,
            "reason": "task_bootstrap_pending",
            "blockers": ["task_bootstrap"],
        },
        prior_general_tool_results=[
            {
                "tool_id": "file_read",
                "status": "rejected",
                "reason": "file_not_found",
                "path": r"D:\missing\book.md",
            }
        ],
    )

    assert result["allowed"] is False
    assert result["status"] == "task_acceptance_blocked"
    assert result["reason"] == "task_bootstrap_pending"


def test_spec613_natural_no_go_closes_evidenced_terminal_blocked_task():
    from engines.reaction_terminal_state import validate_natural_final_reply_candidate

    class WritePendingTracker:
        def finalize_blocker(self):
            return {"blocked": False}

    result = validate_natural_final_reply_candidate(
        closeout_form_validator=lambda form: {
            "blocked": False,
            "settlement_ledger": {"validated": True},
        },
        write_pending_tracker=WritePendingTracker(),
        current_state={},
        round_type="interactive",
        runtime_guide_completed_flags=set(),
        current_runtime_guide_pending_flags=lambda state, completed_flags=None: ({}, []),
        task_closeout_acceptance=lambda form: {
            "allowed": False,
            "reason": "task_acceptance_blocked",
            "blockers": ["item_memory", "acc_memory"],
            "terminal_blocked": True,
        },
    )

    assert result["allowed"] is True
    assert result["closeout_form"]["closeout_decision"] == "blocked"
    ledger = result["settlement_ledger"]
    assert ledger["closeout_decision"] == "blocked"
    assert ledger["runtime_derived_blocked"] is True
    assert ledger["blocked_reason"] == "task_acceptance_blocked"
    assert ledger["blockers"] == ["item_memory", "acc_memory"]
