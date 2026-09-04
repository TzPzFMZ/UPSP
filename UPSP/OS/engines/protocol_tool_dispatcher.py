"""Protocol tool audit receipt helper.

Provider-native tools are exported and called directly. Submission receipts
remain live: reaction_loop records them through this helper for audit continuity.
This module must not load guides or authorize submissions.
"""
from logic.protocol_tools import (
    attach_registered_tool_metadata,
    normalize_tool_id,
)


class ProtocolToolDispatcher:
    def build_submission_receipts(self, submissions, invalid_submissions=None):
        receipts = []
        for submission in submissions or []:
            tool_id = normalize_tool_id(submission)
            receipts.append({
                "tool_id": tool_id,
                "status": "submission_received",
                "source": submission,
            })
        for submission in invalid_submissions or []:
            tool_id = normalize_tool_id(submission)
            receipts.append({
                "tool_id": tool_id,
                "status": "invalid_tool_request",
                "reason": "retired_text_protocol_submission",
                "source": submission,
            })
        return attach_registered_tool_metadata(receipts)
