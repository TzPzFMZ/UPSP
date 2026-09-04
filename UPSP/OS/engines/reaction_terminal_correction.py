"""Correction helpers for reaction terminal and handoff attempts."""

from logic.protocol_tools import normalize_tool_id


CORRECTABLE_REACTION_FINALIZE_REASONS = {
    "invalid_json",
    "native_argument_missing_required",
    "native_argument_unknown_field",
    "native_argument_invalid_type",
    "native_argument_invalid_enum",
}


REACTION_ACTIVITY_KEYS = (
    "protocol_tool_requests",
    "protocol_tool_submissions",
    "invalid_protocol_tool_submissions",
    "general_tool_requests",
    "tool_summaries",
    "relation_card_declarations",
    "memory_write_declarations",
    "memory_link_update_declarations",
    "memory_container_create_declarations",
    "memory_container_write_declarations",
    "memory_privacy_declarations",
    "memory_privacy_declassify_declarations",
    "chronicle_write_declarations",
    "alert_mode_settle_declarations",
    "fault_record_declarations",
    "mount_cancel_requests",
)


def reaction_has_activity_besides_terminal_invalid(parsed_reaction):
    parsed_reaction = parsed_reaction or {}
    return any(parsed_reaction.get(key) for key in REACTION_ACTIVITY_KEYS)


def is_correctable_reaction_finalize_invalid(item):
    if not isinstance(item, dict):
        return False
    if normalize_tool_id(item.get("tool_id", "")) != "reaction_finalize":
        return False
    if str(item.get("source") or "") in {
        "missing_provider_tool_call",
        "text_tool_request",
        "final_reply",
    }:
        return False
    return str(item.get("reason") or "").strip() in (
        CORRECTABLE_REACTION_FINALIZE_REASONS
    )


def tag_correctable_reaction_terminal_invalids(
        invalid_requests,
        parsed_reaction,
        reaction_loop_phase):
    tagged = [
        dict(item) if isinstance(item, dict) else item
        for item in (invalid_requests or [])
    ]
    if not tagged:
        return tagged
    correctable = (
        all(is_correctable_reaction_finalize_invalid(item) for item in tagged)
        and not reaction_has_activity_besides_terminal_invalid(parsed_reaction)
    )
    for item in tagged:
        if not isinstance(item, dict):
            continue
        item.setdefault("reaction_loop_phase", reaction_loop_phase)
        if correctable:
            item["correctable_terminal_attempt"] = True
    return tagged


def corrected_reaction_terminal_invalid_requests(invalid_requests):
    corrected = []
    for item in invalid_requests or []:
        if not isinstance(item, dict):
            continue
        if (
                not item.get("correctable_terminal_attempt")
                and not item.get("correctable_text_channel_attempt")):
            continue
        corrected_item = dict(item)
        if item.get("correctable_text_channel_attempt"):
            corrected_item["correction_reason"] = (
                "valid_native_tool_or_finalize_after_text_channel_feedback"
            )
        else:
            corrected_item["correction_reason"] = (
                "valid_terminal_action_after_feedback"
            )
        corrected.append(corrected_item)
    return corrected
