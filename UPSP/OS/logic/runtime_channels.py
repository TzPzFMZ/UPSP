"""Runtime call-channel and message-channel definitions."""
from dataclasses import dataclass


REACTION_FINAL_REPLY_TEXT_GUIDE = "__final_reply_text__"
REACTION_CONTINUE_HANDOFF_SOURCE = "reaction.continue_handoff"

TEXT_POLICY_ILLEGAL = "illegal"
TEXT_POLICY_PROGRESS = "progress"
TEXT_POLICY_FINAL_REPLY = "final_reply"
TEXT_POLICY_EMPTY_RETRY = "empty_retry"

TOOL_MODE_FREE = "free"
TOOL_MODE_REQUIRED = "required"


def closeout_final_response_source(result, *, user_stop=False):
    """Return a truthful source for the final round_closed projection."""
    result = result if isinstance(result, dict) else {}
    explicit = str(result.get("_final_response_source") or "").strip()
    if explicit:
        return explicit
    response = str(result.get("response") or "").strip()
    if user_stop and not response:
        return "runtime.user_stop"
    if not response:
        for receipt in result.get("_heartbeat_rearm_receipts") or []:
            if not isinstance(receipt, dict):
                continue
            status = str(receipt.get("status") or "").strip()
            flags = {str(flag) for flag in receipt.get("set_flags") or []}
            if "continue_requested" not in flags:
                continue
            if status == "continue_requested_rearmed":
                relay_intent = receipt.get("relay_intent")
                if (
                    isinstance(relay_intent, dict)
                    and str(relay_intent.get("status") or "").strip() == "open"
                ):
                    return REACTION_CONTINUE_HANDOFF_SOURCE
            if (
                status == "continue_requested_rearmed_from_open_relay_intents"
                and receipt.get("open_relay_intent_ids")
            ):
                return REACTION_CONTINUE_HANDOFF_SOURCE
    return "reaction.final_reply_text"


@dataclass(frozen=True)
class RuntimeCallChannel:
    name: str
    step: str
    phase: str
    terminal_tool: str = ""
    tool_mode: str = ""
    include_standard_tools: bool = False
    include_protocol_writes: bool = False
    popup_template: str = ""
    popup_kind: str = ""
    popup_fields: str = ""
    natural_text_policy: str = TEXT_POLICY_ILLEGAL
    natural_text_channel: str = ""
    prompt_cache_lane: str = ""


CALL_CHANNELS = {
    "setup": RuntimeCallChannel(
        name="setup",
        step="setup",
        phase="setup",
        terminal_tool="setup_finalize",
        tool_mode=TOOL_MODE_REQUIRED,
        popup_template="setup",
        popup_kind="setup_handoff",
        popup_fields="mount_requests, security_verdict, suggested_mode, relation_reminder",
        natural_text_channel="setup.illegal_text",
        prompt_cache_lane="setup_finalize",
    ),
    "reaction.loop": RuntimeCallChannel(
        name="reaction.loop",
        step="reaction",
        phase="loop",
        terminal_tool="reaction_finalize",
        tool_mode=TOOL_MODE_FREE,
        include_standard_tools=True,
        include_protocol_writes=True,
        popup_template="reaction_loop",
        popup_kind="reaction_loop",
        popup_fields="general/protocol tools, reaction_finalize, assistant_text",
        natural_text_policy=TEXT_POLICY_PROGRESS,
        natural_text_channel="assistant_text",
        prompt_cache_lane="reaction_loop_tools",
    ),
    "final_reply": RuntimeCallChannel(
        name="final_reply",
        step="reaction",
        phase="final_reply",
        popup_template="final_reply",
        popup_kind="final_reply",
        popup_fields="final_reply_text",
        natural_text_policy=TEXT_POLICY_FINAL_REPLY,
        natural_text_channel="final_reply.text",
        prompt_cache_lane="reaction_final_reply_text",
    ),
    "cleanup": RuntimeCallChannel(
        name="cleanup",
        step="cleanup",
        phase="cleanup",
        terminal_tool="cleanup_finalize",
        tool_mode=TOOL_MODE_REQUIRED,
        popup_template="cleanup",
        popup_kind="cleanup_handoff",
        popup_fields="connection_materials, tacit_materials, cache_compaction",
        natural_text_channel="cleanup.illegal_text",
        prompt_cache_lane="cleanup_finalize",
    ),
}

STEP_TERMINAL_TOOLS = {
    channel.step: channel.terminal_tool
    for channel in CALL_CHANNELS.values()
    if channel.terminal_tool and channel.step in {"setup", "reaction", "cleanup"}
}
STEP_TERMINAL_TOOL_IDS = set(STEP_TERMINAL_TOOLS.values())


def channel_for_step(step, reaction_loop_phase="loop", active_protocol_tool_guides=None):
    step = str(step or "").strip().lower()
    active_guides = {
        str(item or "").strip()
        for item in (active_protocol_tool_guides or [])
    }
    if step == "setup":
        return CALL_CHANNELS["setup"]
    if step == "cleanup":
        return CALL_CHANNELS["cleanup"]
    if step == "final_reply":
        return CALL_CHANNELS["final_reply"]
    if step == "reaction":
        phase = str(reaction_loop_phase or "loop").strip().lower()
        if phase == "final_reply" or REACTION_FINAL_REPLY_TEXT_GUIDE in active_guides:
            return CALL_CHANNELS["final_reply"]
        return CALL_CHANNELS["reaction.loop"]
    return RuntimeCallChannel(
        name=step or "unknown",
        step=step,
        phase=str(reaction_loop_phase or "").strip().lower(),
    )


MESSAGE_CHANNELS = {
    "assistant_text": {
        "visibility": "user_visible",
        "block_kind": "dialogue_progress",
        "context_policy": "short_term_dialogue",
        "cache_policy": "dialogue_recent",
        "audit_policy": "record_text",
        "tool_fact_material": False,
        "final_reply_material": False,
        "long_term_memory": "memory_tool_only",
        "redact_text": False,
    },
    "reaction.progress": {
        "visibility": "user_visible",
        "block_kind": "dialogue_progress",
        "context_policy": "short_term_dialogue",
        "cache_policy": "dialogue_recent",
        "audit_policy": "record_text",
        "tool_fact_material": False,
        "final_reply_material": False,
        "long_term_memory": "memory_tool_only",
        "redact_text": False,
    },
    "final_reply.text": {
        "visibility": "user_visible",
        "block_kind": "final_reply",
        "context_policy": "final_response",
        "cache_policy": "dialogue_recent",
        "audit_policy": "record_text",
        "tool_fact_material": False,
        "final_reply_material": True,
        "long_term_memory": "memory_tool_only",
        "redact_text": False,
    },
    "setup.illegal_text": {
        "visibility": "runtime_only",
        "block_kind": "illegal_text_event",
        "context_policy": "abstract_feedback_only",
        "cache_policy": "never",
        "audit_policy": "marker_only",
        "tool_fact_material": False,
        "final_reply_material": False,
        "long_term_memory": "never",
        "redact_text": True,
    },
    "cleanup.illegal_text": {
        "visibility": "runtime_only",
        "block_kind": "illegal_text_event",
        "context_policy": "abstract_feedback_only",
        "cache_policy": "never",
        "audit_policy": "marker_only",
        "tool_fact_material": False,
        "final_reply_material": False,
        "long_term_memory": "never",
        "redact_text": True,
    },
    "final_reply.empty": {
        "visibility": "runtime_only",
        "block_kind": "final_reply_failure",
        "context_policy": "retry_notice",
        "cache_policy": "never",
        "audit_policy": "marker_only",
        "tool_fact_material": False,
        "final_reply_material": False,
        "long_term_memory": "never",
        "redact_text": True,
    },
    "final_reply.invalid_tool": {
        "visibility": "runtime_only",
        "block_kind": "final_reply_failure",
        "context_policy": "fail",
        "cache_policy": "never",
        "audit_policy": "marker_only",
        "tool_fact_material": False,
        "final_reply_material": False,
        "long_term_memory": "never",
        "redact_text": True,
    },
}


def discarded_text_marker(text):
    return f"<discarded_non_final_text chars={len(str(text or '').strip())}>"


def build_message_envelope(
        channel,
        *,
        text="",
        phase="",
        round_num=None,
        iteration=None,
        source="provider_text"):
    channel = str(channel or "").strip()
    policy = dict(MESSAGE_CHANNELS.get(channel) or {})
    if not policy:
        raise ValueError(f"unknown_message_channel:{channel}")
    raw_text = str(text or "").strip()
    redact = bool(policy.get("redact_text"))
    envelope = {
        "schema_version": "runtime_message_envelope.v1",
        "channel": channel,
        "phase": str(phase or "").strip(),
        "round": round_num,
        "iteration": iteration,
        "source": str(source or "").strip() or "provider_text",
        "text_chars": len(raw_text),
        "text": "" if redact else raw_text,
        "redacted_marker": discarded_text_marker(raw_text) if redact else "",
    }
    envelope.update(policy)
    return envelope
