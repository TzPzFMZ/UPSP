"""Explicit reaction-loop result state and its compatibility projection."""
from dataclasses import dataclass, field


@dataclass
class ReactionLoopResultState:
    write_pending_tracker: object = None
    reaction_obligations: object = None
    provider_call_hard_stop: dict = field(default_factory=dict)
    single_round_probe_hard_stop: dict = field(default_factory=dict)
    required_context_failure: dict = field(default_factory=dict)
    local_blocked_reason: str = ""
    final_response: str = ""
    mounted_mem_ids: list = field(default_factory=list)
    preselection_evidence: list = field(default_factory=list)
    all_created_containers: list = field(default_factory=list)
    all_relation_declarations: list = field(default_factory=list)
    all_relation_card_receipts: list = field(default_factory=list)
    all_protocol_tool_requests: list = field(default_factory=list)
    all_protocol_tool_submissions: list = field(default_factory=list)
    all_native_protocol_tool_submissions: list = field(default_factory=list)
    all_invalid_protocol_tool_submissions: list = field(default_factory=list)
    all_protocol_tool_receipts: list = field(default_factory=list)
    all_native_tool_feedbacks: list = field(default_factory=list)
    all_invalid_tool_requests: list = field(default_factory=list)
    corrected_invalid_tool_requests: list = field(default_factory=list)
    all_general_tool_requests: list = field(default_factory=list)
    all_general_tool_results: list = field(default_factory=list)
    tool_transaction_audit: dict = field(default_factory=dict)
    reaction_loop_guard_receipts: list = field(default_factory=list)
    all_settlement_ledgers: list = field(default_factory=list)
    all_tool_summaries: list = field(default_factory=list)
    all_assistant_progress: list = field(default_factory=list)
    all_message_envelopes: list = field(default_factory=list)
    all_closeout_relay_receipts: list = field(default_factory=list)
    pending_relay_target_for_next: dict = field(default_factory=dict)
    all_memory_write_declarations: list = field(default_factory=list)
    all_memory_write_receipts: list = field(default_factory=list)
    all_memory_content_read_requests: list = field(default_factory=list)
    all_memory_content_read_receipts: list = field(default_factory=list)
    all_corpus_read_requests: list = field(default_factory=list)
    all_corpus_read_receipts: list = field(default_factory=list)
    all_container_read_requests: list = field(default_factory=list)
    all_container_read_receipts: list = field(default_factory=list)
    all_index_view_requests: list = field(default_factory=list)
    all_index_view_receipts: list = field(default_factory=list)
    all_relation_read_requests: list = field(default_factory=list)
    all_relation_read_receipts: list = field(default_factory=list)
    all_mount_cancel_requests: list = field(default_factory=list)
    all_mount_cancel_receipts: list = field(default_factory=list)
    all_pending_cancel_requests: list = field(default_factory=list)
    all_pending_cancel_receipts: list = field(default_factory=list)
    all_relay_intent_settle_requests: list = field(default_factory=list)
    all_relay_intent_settle_receipts: list = field(default_factory=list)
    all_guide_submit_requests: list = field(default_factory=list)
    all_guide_submit_receipts: list = field(default_factory=list)
    all_memory_link_update_declarations: list = field(default_factory=list)
    all_memory_link_update_receipts: list = field(default_factory=list)
    all_memory_container_create_declarations: list = field(default_factory=list)
    all_memory_container_create_receipts: list = field(default_factory=list)
    all_memory_container_write_declarations: list = field(default_factory=list)
    all_memory_container_write_receipts: list = field(default_factory=list)
    all_memory_privacy_declarations: list = field(default_factory=list)
    all_memory_privacy_receipts: list = field(default_factory=list)
    all_memory_privacy_declassify_declarations: list = field(default_factory=list)
    all_memory_privacy_declassify_receipts: list = field(default_factory=list)
    all_chronicle_write_declarations: list = field(default_factory=list)
    all_chronicle_write_receipts: list = field(default_factory=list)
    all_alert_mode_settle_declarations: list = field(default_factory=list)
    all_alert_mode_settle_receipts: list = field(default_factory=list)
    all_fault_record_declarations: list = field(default_factory=list)
    all_fault_record_receipts: list = field(default_factory=list)
    all_container_focus_declarations: list = field(default_factory=list)
    all_container_focus_receipts: list = field(default_factory=list)
    all_identity_resolutions: list = field(default_factory=list)
    all_memory_annotation_declarations: list = field(default_factory=list)
    all_memory_annotation_receipts: list = field(default_factory=list)
    all_memory_recall_completion_requests: list = field(default_factory=list)
    all_memory_recall_completion_receipts: list = field(default_factory=list)
    last_reaction_loop: dict = field(default_factory=dict)
    iteration_records: list = field(default_factory=list)
    frame_settlements: list = field(default_factory=list)
    exit_signal: str = "done"
    reaction_finalize_validated: bool = False
    final_reply_pending: bool = False
    final_reply_done: bool = False
    final_response_source: str = ""
    interaction_meta: dict = field(default_factory=dict)
    evolution_context: str = ""
    evolution_stats: dict = field(default_factory=dict)


def build_reaction_loop_result(state):
    provider_hard_stop = state.provider_call_hard_stop or {}
    probe_hard_stop = state.single_round_probe_hard_stop or {}
    context_failure = state.required_context_failure or {}
    local_blocked_reason = str(state.local_blocked_reason or "").strip()
    return {
        "aborted": bool(provider_hard_stop or context_failure or local_blocked_reason),
        "response": state.final_response,
        "error": (
            provider_hard_stop.get("reason")
            or context_failure.get("reason")
            or local_blocked_reason
        ),
        "_provider_call_hard_stop": provider_hard_stop,
        "_single_round_probe_hard_stop": probe_hard_stop,
        "_required_context_failure": context_failure,
        "_local_blocked_reason": local_blocked_reason,
        "_mounted_memories": state.mounted_mem_ids,
        "_preselection_evidence": state.preselection_evidence,
        "_created_containers": list(dict.fromkeys(state.all_created_containers)),
        "_relation_card_declarations": state.all_relation_declarations,
        "_relation_card_receipts": state.all_relation_card_receipts,
        "_protocol_tool_requests": state.all_protocol_tool_requests,
        "_protocol_tool_submissions": state.all_protocol_tool_submissions,
        "_native_protocol_tool_submissions": state.all_native_protocol_tool_submissions,
        "_protocol_tool_receipts": state.all_protocol_tool_receipts,
        "_native_tool_feedbacks": state.all_native_tool_feedbacks,
        "_invalid_tool_requests": state.all_invalid_tool_requests,
        "_corrected_invalid_tool_requests": state.corrected_invalid_tool_requests,
        "_general_tool_requests": state.all_general_tool_requests,
        "_general_tool_results": state.all_general_tool_results,
        "_tool_transaction_audit": state.tool_transaction_audit,
        "_reaction_loop_guard_receipts": state.reaction_loop_guard_receipts,
        "_reaction_obligations": state.reaction_obligations.audit_state(),
        "_settlement_ledgers": state.all_settlement_ledgers,
        "_tool_summaries": state.all_tool_summaries,
        "_assistant_progress": state.all_assistant_progress,
        "_message_envelopes": state.all_message_envelopes,
        "_closeout_relay_receipts": state.all_closeout_relay_receipts,
        "_pending_relay_target": state.pending_relay_target_for_next,
        "_memory_write_declarations": state.all_memory_write_declarations,
        "_memory_write_receipts": state.all_memory_write_receipts,
        "_memory_content_read_requests": state.all_memory_content_read_requests,
        "_memory_content_read_receipts": state.all_memory_content_read_receipts,
        "_corpus_read_requests": state.all_corpus_read_requests,
        "_corpus_read_receipts": state.all_corpus_read_receipts,
        "_container_read_requests": state.all_container_read_requests,
        "_container_read_receipts": state.all_container_read_receipts,
        "_index_view_requests": state.all_index_view_requests,
        "_index_view_receipts": state.all_index_view_receipts,
        "_relation_read_requests": state.all_relation_read_requests,
        "_relation_read_receipts": state.all_relation_read_receipts,
        "_mount_cancel_requests": state.all_mount_cancel_requests,
        "_mount_cancel_receipts": state.all_mount_cancel_receipts,
        "_pending_cancel_requests": state.all_pending_cancel_requests,
        "_pending_cancel_receipts": state.all_pending_cancel_receipts,
        "_relay_intent_settle_requests": state.all_relay_intent_settle_requests,
        "_relay_intent_settle_receipts": state.all_relay_intent_settle_receipts,
        "_guide_submit_requests": state.all_guide_submit_requests,
        "_guide_submit_receipts": state.all_guide_submit_receipts,
        "_write_pending_settlement": state.write_pending_tracker.audit(),
        "_memory_link_update_declarations": state.all_memory_link_update_declarations,
        "_memory_link_update_receipts": state.all_memory_link_update_receipts,
        "_memory_container_create_declarations": state.all_memory_container_create_declarations,
        "_memory_container_create_receipts": state.all_memory_container_create_receipts,
        "_memory_container_write_declarations": state.all_memory_container_write_declarations,
        "_memory_container_write_receipts": state.all_memory_container_write_receipts,
        "_memory_privacy_declarations": state.all_memory_privacy_declarations,
        "_memory_privacy_receipts": state.all_memory_privacy_receipts,
        "_memory_privacy_declassify_declarations": state.all_memory_privacy_declassify_declarations,
        "_memory_privacy_declassify_receipts": state.all_memory_privacy_declassify_receipts,
        "_chronicle_write_declarations": state.all_chronicle_write_declarations,
        "_chronicle_write_receipts": state.all_chronicle_write_receipts,
        "_alert_mode_settle_declarations": state.all_alert_mode_settle_declarations,
        "_alert_mode_settle_receipts": state.all_alert_mode_settle_receipts,
        "_fault_record_declarations": state.all_fault_record_declarations,
        "_fault_record_receipts": state.all_fault_record_receipts,
        "_container_focus_declarations": state.all_container_focus_declarations,
        "_container_focus_receipts": state.all_container_focus_receipts,
        "_identity_resolutions": state.all_identity_resolutions,
        "_interaction_meta": state.interaction_meta or {},
        "_memory_annotation_declarations": state.all_memory_annotation_declarations,
        "_memory_annotation_receipts": state.all_memory_annotation_receipts,
        "_memory_recall_completion_requests": state.all_memory_recall_completion_requests,
        "_memory_recall_completion_receipts": state.all_memory_recall_completion_receipts,
        "_reaction_loop": state.last_reaction_loop,
        "_reaction_iterations": state.iteration_records,
        "_frame_settlements": state.frame_settlements,
        "_exit_signal": state.exit_signal,
        "_reaction_finalize_validated": state.reaction_finalize_validated,
        "_final_reply_pending": state.final_reply_pending,
        "_final_reply_done": state.final_reply_done,
        "_final_response_source": state.final_response_source,
        "_evolution_requested": bool(state.evolution_context),
        "_evolution_stats": state.evolution_stats or {},
    }
