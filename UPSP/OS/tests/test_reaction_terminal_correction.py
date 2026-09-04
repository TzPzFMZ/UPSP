import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def test_spec568_terminal_invalids_are_marked_only_when_no_other_activity():
    from engines.reaction_terminal_correction import (
        corrected_reaction_terminal_invalid_requests,
        tag_correctable_reaction_terminal_invalids,
    )

    invalids = [{
        "tool_id": "reaction_finalize",
        "source": "provider_native",
        "reason": "native_argument_missing_required",
    }]

    tagged = tag_correctable_reaction_terminal_invalids(invalids, {}, "loop")

    assert tagged[0]["reaction_loop_phase"] == "loop"
    assert tagged[0]["correctable_terminal_attempt"] is True
    corrected = corrected_reaction_terminal_invalid_requests(tagged)
    assert corrected[0]["correction_reason"] == "valid_terminal_action_after_feedback"

    blocked = tag_correctable_reaction_terminal_invalids(
        invalids,
        {"memory_write_declarations": [{"content": "still doing work"}]},
        "loop",
    )
    assert "correctable_terminal_attempt" not in blocked[0]
