"""Protocol tool audit receipt helper.

Provider-native tools are exported and called directly. Submission receipts
remain live: reaction_loop records them through this helper for audit continuity.
This module must not load guides or authorize submissions.
"""
from logic.protocol_tools import normalize_tool_id, tool_metadata_for


class ProtocolToolDispatcher:
    def build_submission_receipts(self, submissions, invalid_submissions=None):
        receipts = []
        for submission in submissions or []:
            tool_id = normalize_tool_id(submission)
            tool_meta = tool_metadata_for(tool_id)
            receipts.append({
                "tool_id": tool_id,
                "tool_family": tool_meta.get("tool_family", ""),
                "tool_class": tool_meta.get("tool_class", ""),
                "status": "submission_received",
                "source": submission,
            })
        for submission in invalid_submissions or []:
            tool_id = normalize_tool_id(submission)
            tool_meta = tool_metadata_for(tool_id)
            receipts.append({
                "tool_id": tool_id,
                "tool_family": tool_meta.get("tool_family", ""),
                "tool_class": tool_meta.get("tool_class", ""),
                "status": "invalid_tool_request",
                "reason": "retired_text_protocol_submission",
                "source": submission,
            })
        return receipts
