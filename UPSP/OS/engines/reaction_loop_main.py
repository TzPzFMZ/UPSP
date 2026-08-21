import copy
import time
from dataclasses import dataclass, field

from assembly.popup import PopupManager
from constants import REACTION_EXIT_SIGNALS
from engines.general_tool_dispatcher import DUPLICATE_GENERAL_TOOL_REASONS
from engines.reaction_helpers import (
    append_reaction_loop_handoff_to_messages,
    enrich_reaction_finalize_settlement_refs,
    native_tool_failure_feedbacks,
    reaction_identity_has_blocked_activity,
    reaction_identity_requires_resolution,
    reaction_loop_has_other_activity,
    reaction_loop_has_protocol_submission_activity,
)
from engines.reaction_iteration import (
    assistant_text_has_tool_payload,
    collect_mount_preselection,
    parse_reaction_iteration_result,
)
from engines.reaction_loop_result import (
    ReactionLoopResultState,
    build_reaction_loop_result,
)
from engines.reaction_runtime_guards import (
    ProtocolReadDuplicateGuard,
    format_general_tool_duplicate_guard_feedback as _format_general_tool_duplicate_guard_feedback,
    general_tool_guard_failure_trackable as _general_tool_guard_failure_trackable,
    general_tool_result_success as _general_tool_result_success,
    group_duplicate_general_tool_results as _group_duplicate_general_tool_results,
    has_effective_protocol_progress as _has_effective_protocol_progress,
    has_reaction_empty_output as _has_reaction_empty_output,
    reaction_empty_output_feedback as _reaction_empty_output_feedback,
    remove_general_tool_duplicate_feedbacks as _remove_general_tool_duplicate_feedbacks,
    with_guard_duplicate_reference as _with_guard_duplicate_reference,
)
from engines.reaction_terminal_correction import (
    corrected_reaction_terminal_invalid_requests as _corrected_terminal_invalid_requests,
    tag_correctable_reaction_terminal_invalids as _tag_correctable_terminal_invalids,
)
from engines.reaction_terminal_state import (
    apply_task_bootstrap_missing_access_terminal_settlement,
)
from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
from engines.round_context import FrameRef
from logic.closeout_copy import closeout_final_reply_reminder
from logic.file_read_window import provider_material_context_issue
from logic.protocol_tools import normalize_tool_id
from logic.periodic_memory_mount import PeriodicMemoryMountProcessor
from logic.memory_reconsolidation import (
    MemoryReconsolidationError,
    MemoryReconsolidationProcessor,
    MemoryReconsolidationTracker,
)
from logic.memory_write_rewrite import MemoryWriteRewriteTracker
from logic.progressive_cache_compaction import (
    current_batch as current_cache_compaction_batch,
    render_guide as render_cache_compaction_guide,
    render_materials as render_cache_compaction_materials,
)
from logic.reaction_call_limit import (
    reaction_provider_call_hard_stop,
    reaction_provider_call_limit_policy,
)
from logic.reaction_obligations import ReactionObligationTracker
from logic.reaction_time_policy import reaction_time_milestone_seconds
from logic.relay_target import (
    file_read_target_satisfied,
    normalize_pending_target,
    pending_target_from_file_reads,
    target_feedback,
)
from logic.rhythm_guidance import active_emergency_items, emergency_attempt_decision
from logic.runtime_channels import build_message_envelope
from logic.task_guide import BLOCKER_EVIDENCE_STATUSES
from logic.tool_transaction_audit import audit_tool_transactions
from errors import RequiredContextError


@dataclass
class ReactionLoopState:
    runner: object
    state: object
    round_num: int
    round_type: str
    mount_ids: object
    interaction_meta: object = None
    trigger_id: str = ""
    caused_by: str = ""
    topology_version: str = ""
    final_response_max_chars: int | None = None
    final_response_length_rejections: int = 0
    response_contract: dict = field(default_factory=dict)
    memory_heat_boosted_ids: set = field(default_factory=set)
    memory_reconsolidation_tracker: object = None
    memory_write_rewrite_tracker: object = None


def _current_work_guide_id(workbench):
    try:
        if hasattr(workbench, "active_guide_slots"):
            return str((workbench.active_guide_slots() or {}).get("work") or "").strip()
        return str(workbench.get("base.active_guide") or "").strip()
    except Exception:
        return ""


def _terminal_blocked_ledger(task_acceptance, source):
    return {
        "closeout_decision": "blocked",
        "handoff_text": "",
        "runtime_derived_blocked": True,
        "blocked_reason": (
            task_acceptance.get("reason") or "task_acceptance_blocked"
        ),
        "blockers": list(task_acceptance.get("blockers") or []),
        "source": source,
    }


def _new_blocker_evidence_refs(seen_refs, *result_groups):
    new_refs = set()
    for item in (
            result
            for group in result_groups
            for result in (group or [])):
        if not isinstance(item, dict):
            continue
        if str(item.get("tool_id") or "").strip() == "guide_submit":
            continue
        status = str(item.get("status") or "").strip().lower()
        reason = str(item.get("reason") or "").strip()
        if status == "rejected" or reason in DUPLICATE_GENERAL_TOOL_REASONS:
            continue
        call_id = str(
            item.get("call_id")
            or item.get("tool_call_id")
            or item.get("id")
            or ""
        ).strip()
        if status in BLOCKER_EVIDENCE_STATUSES and call_id:
            ref = f"call:{call_id}"
            if ref not in seen_refs:
                new_refs.add(ref)
    return new_refs


@dataclass(frozen=True)
class ReactionFrameSettlement:
    frame_ref: FrameRef
    status: str
    exit_signal: str
    provider_call_started: bool
    protocol_receipt_count: int = 0
    general_tool_result_count: int = 0
    invalid_tool_request_count: int = 0

    def as_dict(self):
        return {
            "settlement_scope": "frame",
            "frame_ref": self.frame_ref.as_dict(),
            "frame_id": self.frame_ref.frame_id,
            "status": self.status,
            "exit_signal": self.exit_signal,
            "provider_call_started": self.provider_call_started,
            "protocol_receipt_count": self.protocol_receipt_count,
            "general_tool_result_count": self.general_tool_result_count,
            "invalid_tool_request_count": self.invalid_tool_request_count,
        }


@dataclass
class ReactionSession:
    loop_state: ReactionLoopState
    result_state: ReactionLoopResultState = field(
        default_factory=ReactionLoopResultState)
    result: dict = field(default_factory=dict)
    completed: bool = False
    pending_frame_ref: object = None
    pending_frame_counts: object = None
    deferred_error: object = None
    _frames: object = field(init=False, repr=False)

    def __post_init__(self):
        self._frames = _run_reaction_frames(self)

    def run_frame(self):
        if self.completed:
            return None
        if self.deferred_error is not None:
            error = self.deferred_error
            self.deferred_error = None
            self.completed = True
            raise error
        try:
            return next(self._frames)
        except StopIteration as done:
            self.result = done.value or {}
            self.completed = True
            return None
        except RequiredContextError as exc:
            settlement = None
            if self.pending_frame_ref is not None:
                frame_ref = self.pending_frame_ref
                counts = self.pending_frame_counts
                self.pending_frame_ref = None
                self.pending_frame_counts = None
                settlement = _settle_reaction_frame(
                    self.loop_state.runner,
                    self.result_state,
                    frame_ref,
                    counts,
                    "required_context_failure",
                )
            self.result_state.required_context_failure = exc.as_dict()
            self.result_state.exit_signal = "required_context_failure"
            self.result = build_reaction_loop_result(self.result_state)
            self.completed = True
            self._frames.close()
            return settlement
        except BaseException as exc:
            if self.pending_frame_ref is not None:
                frame_ref = self.pending_frame_ref
                counts = self.pending_frame_counts
                self.pending_frame_ref = None
                self.pending_frame_counts = None
                self.deferred_error = exc
                return _settle_reaction_frame(
                    self.loop_state.runner,
                    self.result_state,
                    frame_ref,
                    counts,
                    "frame_exception",
                )
            self.completed = True
            raise


def _assistant_text_tool_payload_warning(text):
    if not assistant_text_has_tool_payload(text):
        return ""
    return "\n".join([
        "工具通道卫生警告：",
        "上一次 assistant_text 中出现疑似工具载荷（DSML 或 JSON 工具调用）。工具参数必须走 provider-native 工具通道；不要把工具调用载荷写进自然语言正文。",
        "自然语言正文只写简短进展；Runtime 不会解析这段正文为工具，也不会放宽 native tool JSON 校验。",
    ])


def _drain_accumulated_feedbacks(accumulated_messages):
    feedbacks = []
    for item in list(accumulated_messages or []):
        if isinstance(item, dict):
            content = str(item.get("content") or "").strip()
        else:
            content = str(item or "").strip()
        if content:
            feedbacks.append(content)
    accumulated_messages[:] = []
    return feedbacks


def _has_task_guide_completed_feedback(feedbacks):
    for item in feedbacks or []:
        text = str(item or "")
        if (
            "task_guide_completed:" in text
            or "task_guide_completed_final_reply_available" in text
        ):
            return True
    return False


def _settle_reaction_frame(
        runner,
        result_state,
        frame_ref,
        start_counts,
        fallback_exit_signal):
    record_start = start_counts[3]
    records = result_state.iteration_records[record_start:]
    record = records[-1] if records else {}
    exit_signal = str(
        record.get("exit_signal") or fallback_exit_signal or "done")
    status = (
        "degraded"
        if "hard_stop" in exit_signal
        or "recovered" in exit_signal
        or "auto_blocked" in exit_signal
        or "exception" in exit_signal
        or exit_signal == "required_context_failure"
        else "settled"
    )
    settlement = ReactionFrameSettlement(
        frame_ref=frame_ref,
        status=status,
        exit_signal=exit_signal,
        provider_call_started=bool(record.get("provider_call_started", True)),
        protocol_receipt_count=(
            len(result_state.all_protocol_tool_receipts) - start_counts[0]),
        general_tool_result_count=(
            len(result_state.all_general_tool_results) - start_counts[1]),
        invalid_tool_request_count=(
            len(result_state.all_invalid_tool_requests) - start_counts[2]),
    )
    payload = settlement.as_dict()
    result_state.frame_settlements.append(payload)
    runner._round_audit_settlement(
        frame_ref.round_num,
        frame_ref.axis,
        frame_ref.sequence,
        payload,
    )
    organ_runtime = getattr(runner, "organ_runtime", None)
    if organ_runtime is not None:
        try:
            visible_focus_id = runner.workbench.get("base.focus") or ""
        except Exception:
            visible_focus_id = ""
        organ_runtime.dispatch(
            "reaction_frame_settled",
            frame_ref,
            payload,
            {
                "round_num": frame_ref.round_num,
                "interaction_meta": getattr(
                    runner, "_current_interaction_meta", {}),
                "pending_memory_ids": {},
                "visible_focus_id": visible_focus_id,
                "chronicle_store": getattr(runner, "chronicle_store", None),
                "chronicle_focus": getattr(runner, "chronicle_focus", None),
                "memory_heat_boosted_ids": getattr(
                    runner, "_current_memory_heat_boosted_ids", set()),
            },
        )
    return settlement


def _run_reaction_frames(session):
    loop_state = session.loop_state
    result_state = session.result_state
    self = loop_state.runner
    state = loop_state.state
    round_type = loop_state.round_type
    mount_ids = loop_state.mount_ids
    interaction_meta = loop_state.interaction_meta
    round_num = loop_state.round_num
    self._current_round_type = round_type
    self._current_interaction_meta = dict(interaction_meta or {})
    reconsolidation_tracker = loop_state.memory_reconsolidation_tracker
    if not isinstance(reconsolidation_tracker, MemoryReconsolidationTracker):
        reconsolidation_tracker = MemoryReconsolidationTracker(round_num)
        loop_state.memory_reconsolidation_tracker = reconsolidation_tracker
    rewrite_tracker = loop_state.memory_write_rewrite_tracker
    if not isinstance(rewrite_tracker, MemoryWriteRewriteTracker):
        rewrite_tracker = MemoryWriteRewriteTracker(round_num)
        loop_state.memory_write_rewrite_tracker = rewrite_tracker
    reaction_obligations = ReactionObligationTracker(
        memory_reconsolidation_tracker=reconsolidation_tracker,
        memory_write_rewrite_tracker=rewrite_tracker,
        context_store=self.ctx_store,
    )
    result_state.reaction_obligations = reaction_obligations

    boosted_memory_ids = loop_state.memory_heat_boosted_ids
    self._current_memory_heat_boosted_ids = boosted_memory_ids
    periodic_processor = PeriodicMemoryMountProcessor(
        memory_store=self.memory_store,
        heat=self.heat,
        assembler=self.assembler,
        config_store=self.cfg,
        instance_id=getattr(self.memory_recall, "instance_id", "meta"),
    )
    reconsolidation_processor = MemoryReconsolidationProcessor(
        memory_store=self.memory_store,
        assembler=self.assembler,
    )
    self._current_memory_reconsolidation_tracker = reconsolidation_tracker
    self._current_memory_write_rewrite_tracker = rewrite_tracker
    self._current_memory_reconsolidation_processor = reconsolidation_processor
    self._current_periodic_mount_processor = periodic_processor
    periodic_reconsolidation_ids = []
    try:
        pending_items = periodic_processor.mount_store.load().get(
            "pending_memory_items", [])
        for item in pending_items:
            mem_id = str(item.get("id") or "").strip()
            if not mem_id or item.get("status") != "awaiting_completion":
                continue
            inspected = self.memory_recall.inspect(mem_id)
            if periodic_processor.is_alignment_ready(inspected):
                try:
                    periodic_processor.apply("mount", mem_id)
                except Exception as exc:
                    periodic_processor.mark_pending_blocked(mem_id, str(exc))
                    reaction_obligations.add_periodic_mount_blocked(
                        mem_id, str(exc)
                    )
                continue
            self.memory_recall.recall(
                mem_id,
                round_num=round_num,
                boosted_ids=boosted_memory_ids,
                reconsolidation_tracker=reconsolidation_tracker,
                periodic_requested=True,
            )
            if reconsolidation_tracker.get(mem_id) is None:
                raise MemoryReconsolidationError(
                    "periodic_memory_reconsolidation_not_registered"
                )
            periodic_reconsolidation_ids.append(mem_id)
    except Exception as exc:
        raise RequiredContextError(
            "recall", "periodic_memory_reconsolidation", exc
        ) from exc
    if periodic_reconsolidation_ids:
        mount_ids = list(mount_ids or []) + [
            {
                "type": "memory", "ids": mem_id,
                "source": "periodic_mount_reconsolidation",
                "read_mode": "full",
            }
            for mem_id in periodic_reconsolidation_ids
        ]

    # 记录初态挂载记忆 ID，并把 setup 预选转成训练材料证据。
    mounted_mem_ids, preselection_evidence = collect_mount_preselection(
        mount_ids,
        self._existing_stm_memory_ids(),
    )
    result_state.mounted_mem_ids = mounted_mem_ids
    result_state.preselection_evidence = preselection_evidence

    # 挂载 = 正反馈：每轮被加载进CONTEXT的记忆+10热度（DDS §4.4）
    for mid in mounted_mem_ids:
        self._boost_mounted_memory_once(
            mid,
            round_num,
            boosted_memory_ids,
            reconsolidation_tracker=reconsolidation_tracker,
        )
    for evidence in preselection_evidence:
        if evidence.get("item_type") != "memory":
            continue
        mem_id = str(evidence.get("item_id") or "").strip()
        if not mem_id or mem_id in boosted_memory_ids:
            continue
        try:
            meta = self.memory_store.read_meta_by_id(mem_id)
        except Exception as exc:
            raise RequiredContextError(
                "recall", f"memory:{mem_id}", exc
            ) from exc
        self._boost_mounted_memory_once(
            mem_id,
            round_num,
            boosted_memory_ids,
            meta.get("_memory_layer") or "STM",
            reconsolidation_tracker=reconsolidation_tracker,
        )

    accumulated_messages = []
    iteration_records = result_state.iteration_records
    final_response = ""
    final_response_source = ""
    reaction_finalize_validated = False
    final_reply_pending = False
    final_reply_done = False
    closeout_projection_text = ""
    all_created_containers = result_state.all_created_containers
    all_relation_declarations = result_state.all_relation_declarations
    all_protocol_tool_requests = result_state.all_protocol_tool_requests
    all_protocol_tool_submissions = result_state.all_protocol_tool_submissions
    all_native_protocol_tool_submissions = result_state.all_native_protocol_tool_submissions
    all_invalid_protocol_tool_submissions = result_state.all_invalid_protocol_tool_submissions
    all_invalid_tool_requests = result_state.all_invalid_tool_requests
    corrected_invalid_tool_requests = result_state.corrected_invalid_tool_requests
    all_protocol_tool_receipts = result_state.all_protocol_tool_receipts
    all_general_tool_requests = result_state.all_general_tool_requests
    all_general_tool_results = result_state.all_general_tool_results
    all_tool_summaries = result_state.all_tool_summaries
    all_memory_write_declarations = result_state.all_memory_write_declarations
    all_memory_write_receipts = result_state.all_memory_write_receipts
    all_memory_annotation_declarations = result_state.all_memory_annotation_declarations
    all_memory_annotation_receipts = result_state.all_memory_annotation_receipts
    all_memory_content_read_requests = result_state.all_memory_content_read_requests
    all_memory_content_read_receipts = result_state.all_memory_content_read_receipts
    all_corpus_read_requests = result_state.all_corpus_read_requests
    all_corpus_read_receipts = result_state.all_corpus_read_receipts
    all_container_read_requests = result_state.all_container_read_requests
    all_container_read_receipts = result_state.all_container_read_receipts
    all_index_view_requests = result_state.all_index_view_requests
    all_index_view_receipts = result_state.all_index_view_receipts
    all_memory_search_requests = result_state.all_memory_search_requests
    all_memory_search_receipts = result_state.all_memory_search_receipts
    all_relation_read_requests = result_state.all_relation_read_requests
    all_relation_read_receipts = result_state.all_relation_read_receipts
    all_memory_link_update_declarations = result_state.all_memory_link_update_declarations
    all_memory_link_update_receipts = result_state.all_memory_link_update_receipts
    all_memory_container_create_declarations = result_state.all_memory_container_create_declarations
    all_memory_container_create_receipts = result_state.all_memory_container_create_receipts
    all_memory_container_write_declarations = result_state.all_memory_container_write_declarations
    all_memory_container_write_receipts = result_state.all_memory_container_write_receipts
    all_memory_privacy_declarations = result_state.all_memory_privacy_declarations
    all_memory_privacy_receipts = result_state.all_memory_privacy_receipts
    all_memory_privacy_declassify_declarations = result_state.all_memory_privacy_declassify_declarations
    all_memory_privacy_declassify_receipts = result_state.all_memory_privacy_declassify_receipts
    all_chronicle_write_declarations = result_state.all_chronicle_write_declarations
    all_chronicle_write_receipts = result_state.all_chronicle_write_receipts
    all_alert_mode_settle_declarations = result_state.all_alert_mode_settle_declarations
    all_alert_mode_settle_receipts = result_state.all_alert_mode_settle_receipts
    all_fault_record_declarations = result_state.all_fault_record_declarations
    all_fault_record_receipts = result_state.all_fault_record_receipts
    all_container_focus_declarations = result_state.all_container_focus_declarations
    all_container_focus_receipts = result_state.all_container_focus_receipts
    all_mount_cancel_requests = result_state.all_mount_cancel_requests
    all_mount_cancel_receipts = result_state.all_mount_cancel_receipts
    all_relay_intent_settle_requests = result_state.all_relay_intent_settle_requests
    all_relay_intent_settle_receipts = result_state.all_relay_intent_settle_receipts
    all_relation_card_receipts = result_state.all_relation_card_receipts
    all_guide_submit_requests = result_state.all_guide_submit_requests
    all_guide_submit_receipts = result_state.all_guide_submit_receipts
    all_identity_resolutions = result_state.all_identity_resolutions
    active_protocol_tool_guides = (
        []
        if self._suppress_workbench_guides(round_type, state)
        else self._active_guide_protocol_tools()
    )
    active_general_tool_guides = self._active_general_tool_guides()
    last_reaction_loop = {}
    all_assistant_progress = result_state.all_assistant_progress
    all_message_envelopes = result_state.all_message_envelopes
    all_settlement_ledgers = result_state.all_settlement_ledgers
    all_closeout_relay_receipts = result_state.all_closeout_relay_receipts
    reaction_loop_guard_receipts = result_state.reaction_loop_guard_receipts
    closeout_conflict_count = 0
    reaction_progress_repeat_count = 0
    reaction_empty_output_count = 0
    reaction_finalize_invalid_correction_count = 0
    closeout_task_finish_block_signature = ""
    closeout_task_finish_block_count = 0
    final_response_length_rejections = int(
        loop_state.final_response_length_rejections or 0)
    general_tool_duplicate_last_signature = ""
    general_tool_duplicate_streak_count = 0
    general_tool_duplicate_closeout_pending = False
    general_tool_duplicate_closeout_info = {}
    protocol_read_duplicate_guard = ProtocolReadDuplicateGuard()
    emergency_attempt_counts = {}
    emergency_attempt_nudged = set()
    emergency_auto_deferred = set()
    relay_execution_correction_count = 0
    relay_execution_progress_seen = False
    chronicle_no_active_focus_rejections = 0
    guide_correction_active_id = _current_work_guide_id(self.workbench)
    guide_correction_rejections = []
    guide_correction_rejection_frames = 0
    guide_correction_evidence_refs = set()
    pending_relay_target = normalize_pending_target(
        state.get("base", {}).get("runtime", {}).get(
            "pending_relay_target"))
    if round_type != "relay":
        pending_relay_target = {}
    relay_target_satisfied = not pending_relay_target
    relay_target_correction_count = 0
    pending_relay_target_for_next = {}
    identity_resolution_correction_count = 0
    exit_signal = "done"
    i = 0
    round_start = time.time()
    time_limit = self._load_time_milestones()
    pending_memory_ids = {}
    pending_native_tool_feedbacks = []
    periodic_obligation_prompt = reaction_obligations.render_prompt()
    if periodic_obligation_prompt:
        pending_native_tool_feedbacks.append(periodic_obligation_prompt)
    all_native_tool_feedbacks = result_state.all_native_tool_feedbacks

    def reject_final_candidate(candidate, source, status, details, iteration):
        self._get_round_audit_store().append_event(
            round_num,
            "final_response_candidate_rejected",
            {"candidate": candidate, "source": source, "status": status, **details},
            phase="reaction",
            iteration=iteration,
        )
        for index in range(len(all_message_envelopes) - 1, -1, -1):
            envelope = all_message_envelopes[index]
            if (
                    str(envelope.get("channel") or "") == "assistant_text"
                    and str(envelope.get("text") or "").strip() == candidate):
                del all_message_envelopes[index]
                break

    def guard_final_response(candidate, source, parsed, iteration):
        """Apply the optional character budget after normal terminal validation."""
        nonlocal final_response_length_rejections
        candidate = str(candidate or "").strip()
        if not candidate:
            return ""
        limit = loop_state.final_response_max_chars
        if limit is not None and len(candidate) > limit:
            final_response_length_rejections += 1
            exhausted = final_response_length_rejections >= 2
            status = (
                "blocked/final_response_length_exhausted"
                if exhausted else "final_response_too_long"
            )
            reaction_loop_guard_receipts.append({
                "tool_id": "final_reply",
                "tool_family": "message_channel",
                "tool_class": "runtime_guard",
                "status": status,
                "source": source,
                "actual_chars": len(candidate),
                "max_chars": limit,
                "rejection_count": final_response_length_rejections,
            })
            reject_final_candidate(
                candidate, source, status, {
                    "actual_chars": len(candidate),
                    "max_chars": limit,
                    "rejection_count": final_response_length_rejections,
                }, iteration)
            if exhausted:
                ledger = {
                    "closeout_decision": "blocked",
                    "handoff_text": "",
                    "auto_blocked": True,
                    "blocked_reason": "blocked/final_response_length_exhausted",
                    "blockers": ["blocked/final_response_length_exhausted"],
                }
                parsed["settlement_ledger"] = ledger
                all_settlement_ledgers.append(ledger)
                return "blocked"
            feedback = (
                "final_response_too_long: 你刚才的最终回复为 "
                f"{len(candidate)} 个字符，超过本次上限 {limit}。"
                "请保留结论与必要证据，立即改写为不超过上限的一条自然语言最终回复。"
                "不要继续执行工具；Runtime 不会替你截断，若再次超限本轮将被阻断。"
            )
            pending_native_tool_feedbacks.append(feedback)
            all_native_tool_feedbacks.append(feedback)
            return "retry"

        return ""

    def settle_final_response_guards(
            candidate, source, parsed, iteration, containers_created=None):
        nonlocal reaction_loop, reaction_finalize_validated
        nonlocal final_reply_pending, exit_signal, i
        status = guard_final_response(candidate, source, parsed, iteration)
        if not status:
            return ""
        reaction_loop = dict(reaction_loop)
        reaction_loop["reaction_loop_done"] = False
        if status == "blocked":
            reaction_finalize_validated = True
            final_reply_pending = True
            exit_signal = "final_reply_pending"
        iteration_records.append({
            "index": i,
            "response": "",
            "containers_created": list(containers_created or []),
            "exit_signal": (
                "final_response_length_exhausted"
                if status == "blocked" else "final_response_too_long"
            ),
        })
        i += 1
        return status
    hidden_stm_memory_ids = set()
    tool_settlement = ReactionToolSettlementDispatcher(self)
    runtime_guide_completed_flags = set()
    reminded_unfinished_read_signatures = set()
    provider_call_limit_policy = reaction_provider_call_limit_policy()
    provider_call_hard_stop = {}
    single_round_probe_hard_stop = {}
    reaction_provider_calls = 0
    caused_by = loop_state.caused_by

    while True:
        if session.pending_frame_ref is not None:
            frame_ref = session.pending_frame_ref
            counts = session.pending_frame_counts
            session.pending_frame_ref = None
            session.pending_frame_counts = None
            settlement = _settle_reaction_frame(
                self,
                result_state,
                frame_ref,
                counts,
                exit_signal,
            )
            caused_by = frame_ref.frame_id
            yield settlement
        self._current_interaction_meta = dict(interaction_meta or {})
        if getattr(self, "assembler", None) is not None:
            self.assembler._current_interaction_meta = dict(
                interaction_meta or {})
        provider_call_hard_stop, single_round_probe_hard_stop = (
            reaction_provider_call_hard_stop(
                provider_call_limit_policy, reaction_provider_calls)
        )
        if provider_call_hard_stop:
            reaction_loop_guard_receipts.append(
                dict(provider_call_hard_stop)
            )
            all_settlement_ledgers.append({
                "closeout_decision": "blocked",
                "auto_blocked": True,
                "blocked_reason": provider_call_hard_stop["reason"],
                "source": "reaction_provider_call_limit",
            })
            iteration_records.append({
                "index": i,
                "response": "",
                "containers_created": [],
                "exit_signal": "provider_call_hard_stop",
                "reason": provider_call_hard_stop["reason"],
                "provider_call_started": False,
            })
            reaction_finalize_validated = True
            final_reply_pending = True
            exit_signal = "provider_call_hard_stop"
            break
        # 重载 state（确保 focus 等字段是最新的）
        current_state = self.sm.load()
        if runtime_guide_completed_flags and isinstance(current_state, dict):
            current_state = copy.deepcopy(current_state)
            base_state = current_state.setdefault("base", {})
            runtime_state = base_state.setdefault("runtime", {})
            runtime_state["guide_completed_flags"] = sorted(
                runtime_guide_completed_flags)
        self._materialize_next_runtime_rhythm_guide_if_needed(
            current_state,
            round_type,
            round_num,
            runtime_guide_completed_flags,
        )
        self._sync_chronicle_focus_for_current_guide(
            round_type=round_type,
            current_state=current_state,
            round_num=round_num,
            completed_flags=runtime_guide_completed_flags,
        )
        runtime_focus_entries = []
        chronicle_focus_projection = self._chronicle_focus_content_projection()
        if chronicle_focus_projection:
            runtime_focus_entries.append(chronicle_focus_projection)
        mount_ids_current = mount_ids
        try:
            visible_focus_id = self.workbench.get("base.focus") or ""
        except Exception:
            visible_focus_id = ""

        internal_handoff = []

        # 时间线只治理注意力与事务边界：不切 closeout-only，不收窄工具面。
        elapsed = time.time() - round_start
        _reminder_at, _warning_at, auto_relay_at = reaction_time_milestone_seconds(
            time_limit
        )
        if auto_relay_at > 0 and elapsed >= auto_relay_at:
            relay_receipt, settlement_ledger, guard_receipt = (
                self._build_runtime_auto_continue_closeout(
                    elapsed_seconds=elapsed,
                    time_limit_seconds=time_limit,
                )
            )
            if relay_receipt:
                all_closeout_relay_receipts.append(relay_receipt)
            all_settlement_ledgers.append(settlement_ledger)
            reaction_loop_guard_receipts.append(guard_receipt)
            iteration_records.append({
                "index": i,
                "response": "",
                "containers_created": [],
                "exit_signal": "runtime_auto_continue",
                "elapsed_seconds": int(elapsed),
            })
            reaction_finalize_validated = True
            exit_signal = "continue_requested"
            i += 1
            break
        time_reminder_feedback = self._reaction_time_feedback(
            elapsed_seconds=elapsed,
            time_limit_seconds=time_limit,
        )

        drained_feedbacks = _drain_accumulated_feedbacks(accumulated_messages)
        if drained_feedbacks:
            pending_native_tool_feedbacks.extend(drained_feedbacks)
            all_native_tool_feedbacks.extend(drained_feedbacks)

        iteration_native_tool_feedbacks = list(pending_native_tool_feedbacks)
        if loop_state.final_response_max_chars is not None:
            rewrite_state = (
                "已使用一次重写机会；再次超限将阻断本轮。"
                if final_response_length_rejections
                else "首次超限会要求重写，第二次超限将阻断本轮。"
            )
            iteration_native_tool_feedbacks.append(
                "【最终回复字符预算】本次最终回复不得超过 "
                f"{loop_state.final_response_max_chars} 个字符。该限制只约束已经通过"
                "现有终态校验的最终回复，不限制工具调用或审计；Runtime 不会截断。"
                + rewrite_state
            )
        if time_reminder_feedback:
            iteration_native_tool_feedbacks.append(time_reminder_feedback)

        # 装配 reaction 步 messages（含远/近缓存+三源末位输入），并渲染 step.md 供审计。
        audit_iteration = i + 1
        permission_boundary = getattr(self, "permission_boundary_callback", None)
        if callable(permission_boundary):
            permission_boundary(round_num, "reaction", audit_iteration)
        reconsolidation_pending = reconsolidation_tracker.has_pending()
        rewrite_pending_at_frame_start = rewrite_tracker.has_pending()
        cache_compaction_debt = self.ctx_store.load_cache_compaction_debt()
        cache_compaction_pending = (
            cache_compaction_debt.get("schema_version")
            == "cache_compaction_debt.v3"
        )
        if cache_compaction_pending:
            active_protocol_tool_guides = []
            active_guide_feedback = ""
            try:
                compaction_discipline = PopupManager.load_guide_template(
                    "cache_compaction"
                )
                if not compaction_discipline:
                    raise ValueError("cache_compaction_guide_missing")
                iteration_native_tool_feedbacks.append(
                    render_cache_compaction_guide(
                        cache_compaction_debt, compaction_discipline
                    )
                )
            except Exception as exc:
                raise RequiredContextError(
                    "read", "cache_compaction_guide", exc
                ) from exc
        elif reconsolidation_pending:
            active_protocol_tool_guides = []
            active_guide_feedback = ""
            try:
                reconsolidation_discipline = PopupManager.load_guide_template(
                    "memory_reconsolidation"
                )
                reconsolidation_feedback = reconsolidation_tracker.render_guide(
                    reconsolidation_discipline
                )
            except Exception as exc:
                raise RequiredContextError(
                    "read", "memory_reconsolidation_guide", exc
                ) from exc
            iteration_native_tool_feedbacks.append(
                reconsolidation_feedback
            )
        elif rewrite_pending_at_frame_start:
            active_protocol_tool_guides = []
            active_guide_feedback = ""
            try:
                rewrite_discipline = PopupManager.load_guide_template(
                    "memory_write_rewrite"
                )
                rewrite_feedback = rewrite_tracker.render_guide(
                    rewrite_discipline
                )
            except Exception as exc:
                raise RequiredContextError(
                    "read", "memory_write_rewrite_guide", exc
                ) from exc
            iteration_native_tool_feedbacks.append(rewrite_feedback)
        elif self._should_suppress_active_guide_feedback(round_type, current_state):
            active_protocol_tool_guides = []
            active_guide_feedback = ""
        else:
            active_protocol_tool_guides = self._active_guide_protocol_tools()
            active_guide_feedback = self._active_guide_feedback()
        active_general_tool_guides = self._active_general_tool_guides()
        if active_guide_feedback:
            iteration_native_tool_feedbacks.append(active_guide_feedback)
        resident_feedback = "" if (
            cache_compaction_pending
            or reconsolidation_pending or rewrite_pending_at_frame_start
        ) else (
            self._reaction_resident_guide_feedback(
                suppress_task_entry=_has_task_guide_completed_feedback(
                    iteration_native_tool_feedbacks
                )
            )
        )
        if resident_feedback:
            iteration_native_tool_feedbacks.append(resident_feedback)

        # 当前反应迭代号必须进入上下文装配，供 dialogue_progress
        # 判断“新产生后下一次展开、之后折叠”的生命周期。
        frame_ref = FrameRef.for_axis(
            round_num,
            "reaction",
            audit_iteration,
            trigger_id=loop_state.trigger_id,
            caused_by=caused_by,
            topology_version=loop_state.topology_version,
            role_id="reaction",
        )
        organ_runtime = getattr(self, "organ_runtime", None)
        organ_materials = (
            organ_runtime.begin_frame_materials(frame_ref)
            if organ_runtime is not None else ()
        )
        frame_materials = list(organ_materials or ())
        if cache_compaction_pending:
            frame_materials.extend(
                render_cache_compaction_materials(cache_compaction_debt)
            )
        elif rewrite_pending_at_frame_start and not reconsolidation_pending:
            frame_materials.extend(rewrite_tracker.render_materials())
        try:
            active_rhythm_id = str(
                self.workbench.get("base.active_guides.rhythm") or ""
            ).strip()
            active_rhythm = (
                self.workbench.load_guide(active_rhythm_id)
                if active_rhythm_id else {}
            )
            if (
                not cache_compaction_pending
                and not reconsolidation_pending
                and not rewrite_pending_at_frame_start
                and str(active_rhythm.get("kind") or "").strip()
                == "memory_compression_rhythm_guide"
            ):
                from data.memory_compression_store import MemoryCompressionManager

                compression_material = (
                    MemoryCompressionManager(
                        memory_store=self.memory_store,
                    ).render_current_batch_material()
                )
                if not compression_material:
                    raise ValueError("memory_compression_batch_missing")
                frame_materials.append(compression_material)
        except Exception as exc:
            if 'active_rhythm' in locals() and str(
                    (active_rhythm or {}).get("kind") or "").strip() == (
                    "memory_compression_rhythm_guide"):
                raise RequiredContextError(
                    "read", "memory_compression_pending", exc) from exc
        assemble_kwargs = {
            "internal_handoff": internal_handoff,
            "protocol_tool_guides": active_protocol_tool_guides,
            "general_tool_guides": active_general_tool_guides,
            "reaction_loop_phase": "loop",
            "native_tool_feedbacks": iteration_native_tool_feedbacks,
            "hidden_stm_memory_ids": hidden_stm_memory_ids,
            "runtime_focus_entries": runtime_focus_entries,
            "current_reaction_iteration": audit_iteration,
            "response_contract": loop_state.response_contract,
        }
        if frame_materials:
            assemble_kwargs["material_inputs"] = frame_materials
        if cache_compaction_pending:
            assemble_kwargs["hidden_lately_block_ids"] = [
                block_id
                for shard in current_cache_compaction_batch(cache_compaction_debt)
                for block_id in shard.get("source_block_ids") or []
            ]
        system, step_messages = self.assembler.assemble_reaction(
            current_state, round_type, mount_ids_current,
            **assemble_kwargs)
        pending_native_tool_feedbacks = []
        messages = step_messages

        material_context_issue = None if cache_compaction_pending else (
            provider_material_context_issue(
                self.sm, self.ctx_store, round_num, audit_iteration,
            )
        )
        if material_context_issue:
            reaction_loop_guard_receipts.append(dict(material_context_issue))
            iteration_records.append({
                "index": i,
                "response": "",
                "containers_created": [],
                "exit_signal": "material_context_budget_hard_stop",
                "reason": material_context_issue["reason"],
                "provider_call_started": False,
            })
            exit_signal = "material_context_budget_hard_stop"
            break

        # 调用 LLM
        executor_protocol_tool_guides = active_protocol_tool_guides
        session.pending_frame_ref = frame_ref
        session.pending_frame_counts = (
            len(all_protocol_tool_receipts),
            len(all_general_tool_results),
            len(all_invalid_tool_requests),
            len(iteration_records),
        )
        try:
            reaction_provider_calls += 1
            iter_result = self._call_llm_with_round_audit(
                "reaction",
                system,
                messages,
                round_num,
                iteration=audit_iteration,
                active_protocol_tool_guides=executor_protocol_tool_guides,
                cache_compaction_call=cache_compaction_pending,
            )
        except Exception as exc:
            recovery_receipt = self._recover_provider_interruption_if_possible(
                exc,
                round_num=round_num,
                iteration=audit_iteration,
                general_tool_results=all_general_tool_results,
                protocol_receipts=all_protocol_tool_receipts,
                guide_submit_receipts=all_guide_submit_receipts,
                memory_write_receipts=all_memory_write_receipts,
                interaction_meta=interaction_meta or {},
            )
            if not recovery_receipt:
                raise
            if recovery_receipt.get("terminal"):
                reaction_loop_guard_receipts.append(recovery_receipt)
                raise RuntimeError(
                    "provider_model_format_instability: "
                    f"{recovery_receipt.get('reason')}"
                ) from exc
            reason = str(recovery_receipt.get("reason") or "").strip()
            relay_receipt = self._apply_closeout_relay_receipt(
                {
                    "handoff_text": (
                        "Provider/model 中断已恢复为下一轮继续；"
                        "请从已有工具事实、产物和任务看板继续执行。"
                    ),
                    "reason": reason,
                },
                trace={
                    "recovery_source": "provider_interruption_recovery",
                    "provider_error_kind": recovery_receipt.get(
                        "provider_error_kind"
                    ),
                },
            )
            if relay_receipt:
                all_closeout_relay_receipts.append(relay_receipt)
            all_settlement_ledgers.append({
                "closeout_decision": "continue",
                "handoff_text": (
                    "Provider/model 中断已恢复为下一轮继续；"
                    "请从已有工具事实、产物和任务看板继续执行。"
                ),
                "reason": reason,
                "source": "provider_interruption_recovery",
            })
            reaction_loop_guard_receipts.append(recovery_receipt)
            iteration_records.append({
                "index": i,
                "response": "",
                "containers_created": [],
                "exit_signal": "provider_interruption_recovered",
                "provider_error_kind": recovery_receipt.get(
                    "provider_error_kind"
                ),
            })
            reaction_finalize_validated = True
            exit_signal = "continue_requested"
            i += 1
            break
        self._transition_current_cache(
            round_num,
            boundary="reaction_provider_return",
            consumer_frame_id=frame_ref.frame_id,
            phase="reaction",
            iteration=audit_iteration,
            expire_call_transients=True,
        )
        self._clear_provider_interruption_recovery_state()
        self._update_token_usage(
            iter_result,
            round_num=round_num,
            phase="reaction",
            iteration=audit_iteration,
        )

        iteration_parse = parse_reaction_iteration_result(
            iter_result,
            executor_protocol_tool_guides,
        )
        all_message_envelopes.extend(iteration_parse.message_envelopes)
        response_text = iteration_parse.response_text
        iter_native_tool_call_envelopes = iteration_parse.native_tool_call_envelopes
        native_terminal_finalize_only = (
            iteration_parse.native_terminal_finalize_only
        )
        parsed_reaction = iteration_parse.parsed_reaction
        parsed_reaction = enrich_reaction_finalize_settlement_refs(
            parsed_reaction,
            memory_write_receipts=all_memory_write_receipts,
            general_tool_results=all_general_tool_results,
            container_receipts=(
                all_memory_container_create_receipts
                + all_memory_container_write_receipts
            ),
        )
        mixed_reaction_finalize = (
            parsed_reaction.get("mixed_reaction_finalize")
            if isinstance(parsed_reaction.get("mixed_reaction_finalize"), dict)
            else {}
        )
        parsed_has_other_activity = reaction_loop_has_other_activity(
            parsed_reaction)
        if parsed_has_other_activity or native_terminal_finalize_only:
            reaction_progress_repeat_count = 0
        self._round_audit_parsed(
            round_num,
            "reaction",
            audit_iteration,
            parsed_reaction,
        )
        elapsed = time.time() - round_start  # API 调用后重算真实耗时
        assistant_progress = str(
            parsed_reaction.get("assistant_progress") or "").strip()
        if assistant_progress:
            all_assistant_progress.append(assistant_progress)
        channel_hygiene_feedback_added = False
        for message_envelope in iteration_parse.message_envelopes:
            if (
                    message_envelope.get("channel")
                    in {"assistant_text", "reaction.progress"}
                    and not message_envelope.get("terminal_text_candidate")):
                warning = _assistant_text_tool_payload_warning(
                    message_envelope.get("text") or "")
                if warning and not channel_hygiene_feedback_added:
                    pending_native_tool_feedbacks.append(warning)
                    all_native_tool_feedbacks.append(warning)
                    channel_hygiene_feedback_added = True
                if not warning:
                    self._write_reaction_progress(
                        message_envelope,
                        round_num,
                        audit_iteration,
                        interaction_meta or {},
                    )

        if _has_reaction_empty_output(parsed_reaction):
            raw_invalid_tool_requests = list(
                parsed_reaction.get("invalid_tool_requests", []) or []
            )
            all_invalid_tool_requests.extend(raw_invalid_tool_requests)
            reaction_empty_output_count += 1
            correction_mode = (
                "reminder"
                if reaction_empty_output_count == 1
                else (
                    "warning"
                    if reaction_empty_output_count == 2
                    else "auto_block"
                )
            )
            reaction_loop_guard_receipts.append({
                "tool_id": "reaction",
                "tool_family": "message_channel",
                "tool_class": "runtime_guard",
                "status": "reaction_empty_output",
                "source": "reaction_loop_empty_output",
                "correction": correction_mode,
                "reason": "reaction_empty_output",
                "provider_error_kind": "provider_model_format_empty_output",
                "empty_output_count": reaction_empty_output_count,
            })
            if reaction_empty_output_count <= 2:
                feedback = _reaction_empty_output_feedback(correction_mode)
                pending_native_tool_feedbacks.append(feedback)
                all_native_tool_feedbacks.append(feedback)
                iteration_records.append({
                    "index": i,
                    "response": "",
                    "containers_created": [],
                    "exit_signal": "reaction_empty_output_retry",
                })
                i += 1
                continue
            auto_blocked_ledger = {
                "closeout_decision": "blocked",
                "handoff_text": "",
                "auto_blocked": True,
                "blocked_reason": "provider_model_format_empty_output",
                "blockers": ["reaction_empty_output"],
                "source": "reaction_empty_output",
                "provider_error_kind": "provider_model_format_empty_output",
            }
            parsed_reaction["settlement_ledger"] = auto_blocked_ledger
            all_settlement_ledgers.append(auto_blocked_ledger)
            reaction_loop_guard_receipts.append({
                "tool_id": "reaction",
                "tool_family": "message_channel",
                "tool_class": "runtime_guard",
                "status": "reaction_empty_output_auto_blocked",
                "source": "reaction_loop_empty_output",
                "reason": "provider_model_format_empty_output",
                "blockers": ["reaction_empty_output"],
                "empty_output_count": reaction_empty_output_count,
            })
            reaction_finalize_validated = True
            final_reply_pending = True
            exit_signal = "final_reply_pending"
            iteration_records.append({
                "index": i,
                "response": "",
                "containers_created": [],
                "exit_signal": "reaction_empty_output_auto_blocked",
            })
            i += 1
            break
        reaction_empty_output_count = 0

        reaction_loop = parsed_reaction.get("reaction_loop") or {}
        if reaction_loop:
            last_reaction_loop = dict(reaction_loop)

        if (
            round_type == "interactive"
            and reaction_identity_requires_resolution(interaction_meta)
        ):
            resolution = parsed_reaction.get("identity_resolution") or {}
            if resolution.get("action") == "confirm":
                interaction_meta = self._apply_reaction_identity_resolution(
                    resolution,
                    interaction_meta,
                )
                self._current_interaction_meta = dict(interaction_meta)
                if getattr(self, "assembler", None) is not None:
                    self.assembler._current_interaction_meta = interaction_meta
                all_identity_resolutions.append(dict(resolution))
                accumulated_messages.append({
                    "role": "user",
                    "content": (
                        "identity_resolved: "
                        f"interaction_object={interaction_meta.get('interaction_object')}; "
                        f"identity_status={interaction_meta.get('identity_status')}; "
                        "continue the task using this interaction_meta."
                    ),
                })
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": [],
                    "exit_signal": "identity_confirmed",
                })
                i += 1
                continue
            if resolution.get("action") == "ask_user":
                question = str(
                    resolution.get("question") or "Who are you?").strip()
                final_response = question
                all_identity_resolutions.append(dict(resolution))
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": [],
                    "exit_signal": "identity_question",
                })
                exit_signal = "done"
                i += 1
                break

            if reaction_identity_has_blocked_activity(parsed_reaction):
                identity_invalid = {
                    "tool_id": "identity_resolution_card",
                    "tool_family": "substrate_tool",
                    "reason": "identity_unresolved",
                    "source": "reaction_identity_gate",
                }
                all_invalid_tool_requests.append(identity_invalid)
                native_identity_invalids = (
                    self._identity_blocked_native_requests(
                        iter_native_tool_call_envelopes)
                )
                all_invalid_tool_requests.extend(native_identity_invalids)
                identity_feedbacks = native_tool_failure_feedbacks(
                    native_identity_invalids)
                if identity_feedbacks:
                    pending_native_tool_feedbacks.extend(identity_feedbacks)
                    all_native_tool_feedbacks.extend(identity_feedbacks)
                if identity_resolution_correction_count >= 2:
                    final_response = "I need to confirm who is speaking before I act. Who are you?"
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": "identity_question",
                    })
                    exit_signal = "done"
                    i += 1
                    break
                identity_resolution_correction_count += 1
                accumulated_messages.append({
                    "role": "user",
                    "content": (
                        "identity_unresolved: high-impact action was blocked. "
                        "Do not call tools, write memory, write relation cards, or edit containers "
                        "until the interaction object is clear. Ask the user to confirm identity "
                        "in ordinary natural language, or proceed only with "
                        "low-impact conversation. Use reaction_finalize(handoff_text) "
                        "only when the work must continue in a later round."
                    ),
                })
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": [],
                    "exit_signal": "identity_resolution_required",
                })
                i += 1
                continue

        if reaction_loop.get("reaction_loop_done") is True:
            natural_final_reply_candidate = str(
                parsed_reaction.get("natural_final_reply_candidate") or ""
            ).strip()
            if (
                natural_final_reply_candidate
                and not native_terminal_finalize_only
                and not reaction_loop_has_other_activity(parsed_reaction)
            ):
                candidate_check = self._validate_natural_final_reply_candidate(
                    reaction_obligations=reaction_obligations,
                    current_state=current_state,
                    round_type=round_type,
                    runtime_guide_completed_flags=runtime_guide_completed_flags,
                    prior_general_tool_results=all_general_tool_results,
                    reminded_unfinished_read_signatures=(
                        reminded_unfinished_read_signatures
                    ),
                )
                if not candidate_check.get("allowed", True):
                    if candidate_check.get("status") == "task_acceptance_blocked":
                        block_signature = self._task_acceptance_block_signature(candidate_check)
                        if block_signature == closeout_task_finish_block_signature:
                            closeout_task_finish_block_count += 1
                        else:
                            closeout_task_finish_block_signature = block_signature
                            closeout_task_finish_block_count = 1
                        if closeout_task_finish_block_count >= 3:
                            blockers = candidate_check.get("blockers") or []
                            auto_blocked_ledger = {
                                "closeout_decision": "blocked",
                                "handoff_text": "",
                                "auto_blocked": True,
                                "blocked_reason": candidate_check.get("reason") or "task_acceptance_blocked",
                                "blockers": blockers,
                            }
                            parsed_reaction["settlement_ledger"] = auto_blocked_ledger
                            all_settlement_ledgers.append(auto_blocked_ledger)
                            reaction_loop_guard_receipts.append({
                                "tool_id": "final_reply",
                                "tool_family": "message_channel",
                                "tool_class": "runtime_guard",
                                "status": "task_acceptance_auto_blocked",
                                "source": "natural_final_reply_candidate",
                                "reason": candidate_check.get("reason"),
                                "blockers": blockers,
                                "repeated_finish_count": closeout_task_finish_block_count,
                            })
                            reaction_finalize_validated = True
                            final_reply_pending = True
                            exit_signal = "final_reply_pending"
                            iteration_records.append({
                                "index": i,
                                "response": "",
                                "containers_created": [],
                                "exit_signal": "task_acceptance_auto_blocked",
                            })
                            i += 1
                            break
                    reminder_signature = str(
                        candidate_check.get("signature") or ""
                    ).strip()
                    if (
                        candidate_check.get("status")
                        == "unfinished_file_read_final_reply_reminder"
                        and reminder_signature
                    ):
                        reminded_unfinished_read_signatures.add(
                            reminder_signature
                        )
                    guard_receipt = {
                        "tool_id": "final_reply",
                        "tool_family": "message_channel",
                        "tool_class": "runtime_guard",
                        "status": candidate_check.get("status"),
                        "source": candidate_check.get(
                            "source",
                            "natural_final_reply_candidate",
                        ),
                    }
                    for key in (
                            "reason",
                            "reasons",
                            "blockers",
                            "guide_kind",
                            "pendings",
                            "signature",
                            "read_refs",
                            "shown_count",
                            "total_count"):
                        value = candidate_check.get(key)
                        if value not in (None, "", []):
                            guard_receipt[key] = value
                    reaction_loop_guard_receipts.append(guard_receipt)
                    feedback = str(candidate_check.get("feedback") or "").strip()
                    if feedback:
                        accumulated_messages.append({
                            "role": "user",
                            "content": feedback,
                        })
                    obligation_prompt = reaction_obligations.render_prompt()
                    if obligation_prompt:
                        accumulated_messages.append({
                            "role": "user",
                            "content": obligation_prompt,
                        })
                    reaction_loop = dict(reaction_loop)
                    reaction_loop["reaction_loop_done"] = False
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": candidate_check.get("status")
                        or "final_reply_candidate_blocked",
                    })
                    i += 1
                    continue
                budget_status = settle_final_response_guards(
                    natural_final_reply_candidate,
                    "natural_final_reply_candidate",
                    parsed_reaction,
                    audit_iteration,
                )
                if budget_status:
                    if budget_status == "blocked":
                        break
                    continue
                settlement_ledger = candidate_check.get(
                    "settlement_ledger") or {}
                if settlement_ledger:
                    parsed_reaction["settlement_ledger"] = settlement_ledger
                    all_settlement_ledgers.append(settlement_ledger)
                reaction_finalize_validated = True
                final_reply_pending = True
                closeout_projection_text = natural_final_reply_candidate
                exit_signal = "final_reply_pending"
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": [],
                    "exit_signal": "natural_final_reply_candidate",
                })
                i += 1
                break
            if (
                native_terminal_finalize_only
                and not reaction_loop_has_other_activity(parsed_reaction)
            ):
                finalize_errors = parsed_reaction.get("reaction_finalize_errors") or []
                if finalize_errors:
                    status = "reaction_finalize_invalid"
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": status,
                        "source": "reaction_finalize",
                        "reasons": list(finalize_errors),
                    })
                    accumulated_messages.append({
                        "role": "user",
                        "content": (
                            "reaction_finalize 中继交接参数非法："
                            + "；".join(str(item) for item in finalize_errors)
                            + "\n只有跨轮继续时才调用 reaction_finalize(handoff_text)；"
                            "完成时直接自然语言回复用户。"
                        ),
                    })
                    reaction_loop = dict(reaction_loop)
                    reaction_loop["reaction_loop_done"] = False
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": status,
                    })
                    i += 1
                    continue
                validation = reaction_obligations.validate_closeout_form(
                    parsed_reaction.get("closeout_form"))
                settlement_ledger = validation.get("settlement_ledger") or {}
                rhythm_acceptance = {"allowed": True}
                if not validation.get("blocked"):
                    rhythm_acceptance = self._rhythm_guide_closeout_acceptance(
                        parsed_reaction.get("closeout_form"),
                        current_state,
                        round_type,
                        runtime_guide_completed_flags,
                    )
                if not rhythm_acceptance.get("allowed", True):
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": "rhythm_guide_blocked",
                        "source": "reaction_finalize",
                        "reason": rhythm_acceptance.get("reason"),
                        "guide_kind": rhythm_acceptance.get("guide_kind"),
                        "blockers": rhythm_acceptance.get("blockers", []),
                    })
                    accumulated_messages.append({
                        "role": "user",
                        "content": self._rhythm_guide_acceptance_feedback(
                            rhythm_acceptance),
                    })
                    reaction_loop = dict(reaction_loop)
                    reaction_loop["reaction_loop_done"] = False
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": "rhythm_guide_blocked",
                    })
                    i += 1
                    continue
                task_acceptance = self._task_closeout_acceptance(
                    parsed_reaction.get("closeout_form"))
                if not task_acceptance.get("allowed", True):
                    if task_acceptance.get("terminal_blocked") is True:
                        budget_status = settle_final_response_guards(
                            response_text,
                            "reaction_finalize_terminal_blocked",
                            parsed_reaction,
                            audit_iteration,
                        )
                        if budget_status:
                            if budget_status == "blocked":
                                break
                            continue
                        settlement_ledger = _terminal_blocked_ledger(
                            task_acceptance,
                            "reaction_finalize",
                        )
                        parsed_reaction["settlement_ledger"] = settlement_ledger
                        all_settlement_ledgers.append(settlement_ledger)
                        reaction_loop_guard_receipts.append({
                            "tool_id": "reaction_finalize",
                            "tool_family": "substrate_tool",
                            "tool_class": "sync_tool",
                            "status": "task_acceptance_terminal_blocked",
                            "source": "reaction_finalize",
                            "reason": task_acceptance.get("reason"),
                            "blockers": task_acceptance.get("blockers", []),
                        })
                        reaction_finalize_validated = True
                        final_reply_pending = True
                        closeout_projection_text = str(response_text or "").strip()
                        exit_signal = "final_reply_pending"
                        iteration_records.append({
                            "index": i,
                            "response": response_text[:200],
                            "containers_created": [],
                            "exit_signal": "task_acceptance_terminal_blocked",
                        })
                        i += 1
                        break
                    closeout_decision = str(
                        (parsed_reaction.get("closeout_form") or {}).get(
                            "closeout_decision") or ""
                    ).strip().lower()
                    block_signature = self._task_acceptance_block_signature(
                        task_acceptance)
                    if closeout_decision == "finish":
                        if block_signature == closeout_task_finish_block_signature:
                            closeout_task_finish_block_count += 1
                        else:
                            closeout_task_finish_block_signature = block_signature
                            closeout_task_finish_block_count = 1
                    if (
                            closeout_decision == "finish"
                            and closeout_task_finish_block_count >= 3):
                        blockers = task_acceptance.get("blockers") or []
                        auto_blocked_ledger = {
                            "closeout_decision": "blocked",
                            "handoff_text": "",
                            "auto_blocked": True,
                            "blocked_reason": (
                                task_acceptance.get("reason")
                                or "task_acceptance_blocked"
                            ),
                            "blockers": blockers,
                        }
                        parsed_reaction["settlement_ledger"] = auto_blocked_ledger
                        all_settlement_ledgers.append(auto_blocked_ledger)
                        reaction_loop_guard_receipts.append({
                            "tool_id": "reaction_finalize",
                            "tool_family": "substrate_tool",
                            "tool_class": "sync_tool",
                            "status": "task_acceptance_auto_blocked",
                            "source": "reaction_finalize",
                            "reason": task_acceptance.get("reason"),
                            "blockers": blockers,
                            "repeated_finish_count": (
                                closeout_task_finish_block_count),
                        })
                        reaction_finalize_validated = True
                        final_reply_pending = True
                        exit_signal = "final_reply_pending"
                        iteration_records.append({
                            "index": i,
                            "response": response_text[:200],
                            "containers_created": [],
                            "exit_signal": "task_acceptance_auto_blocked",
                        })
                        i += 1
                        break
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": "task_acceptance_blocked",
                        "source": "reaction_finalize",
                        "reason": task_acceptance.get("reason"),
                        "blockers": task_acceptance.get("blockers", []),
                    })
                    accumulated_messages.append({
                        "role": "user",
                        "content": self._task_acceptance_feedback(
                            task_acceptance),
                    })
                    reaction_loop = dict(reaction_loop)
                    reaction_loop["reaction_loop_done"] = False
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": "task_acceptance_blocked",
                    })
                    i += 1
                    continue
                if validation.get("blocked"):
                    blocked_event = reaction_obligations.record_blocked_finalize(
                        validation)
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": "closeout_form_blocked",
                        "source": "reaction_finalize",
                        "reasons": list(validation.get("reasons") or []),
                    })
                    accumulated_messages.append({
                        "role": "user",
                        "content": (
                            "Spec272 reaction_finalize 被拦截："
                            + "；".join(blocked_event.get("reasons") or [])
                            + "\n请重新提交平铺 closeout_form；模型只填判断，Runtime 会用 settlement_ledger 验账。"
                        ),
                    })
                    obligation_prompt = reaction_obligations.render_prompt()
                    if obligation_prompt:
                        accumulated_messages.append({
                            "role": "user",
                            "content": obligation_prompt,
                        })
                    reaction_loop = dict(reaction_loop)
                    reaction_loop["reaction_loop_done"] = False
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": "reaction_finalize_blocked",
                    })
                    i += 1
                    continue
            if native_terminal_finalize_only:
                iter_tool_summaries = parsed_reaction.get("tool_summaries", [])
                all_tool_summaries.extend(iter_tool_summaries)
                self._write_reaction_tool_summaries(
                    iter_tool_summaries,
                    round_num,
                    audit_iteration,
                    interaction_meta or {},
                )
                if (
                    round_type == "relay"
                    and settlement_ledger.get("relay_receipt")
                    and pending_relay_target
                    and not relay_target_satisfied
                ):
                    reaction_loop_guard_receipts.append(
                        self._relay_target_unfulfilled_receipt(
                            "reaction_finalize",
                            pending_relay_target,
                            relay_target_correction_count,
                        ))
                    if relay_target_correction_count < 2:
                        relay_target_correction_count += 1
                        reaction_finalize_invalid_correction_count = 0
                        accumulated_messages.append({
                            "role": "user",
                            "content": target_feedback(
                                pending_relay_target,
                                relay_target_correction_count),
                        })
                        reaction_loop = dict(reaction_loop)
                        reaction_loop["reaction_loop_done"] = False
                        iteration_records.append({
                            "index": i,
                            "response": response_text[:200],
                            "containers_created": [],
                            "exit_signal": "relay_target_unfulfilled",
                        })
                        i += 1
                        continue
                    all_invalid_tool_requests.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": "rejected",
                        "source": "reaction_loop_guard",
                        "reason": "relay_target_unfulfilled",
                        "target": pending_relay_target,
                    })
                    exit_signal = "relay_target_unfulfilled"
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": "relay_target_unfulfilled",
                    })
                    i += 1
                    break
                if (
                    round_type == "relay"
                    and settlement_ledger.get("relay_receipt")
                    and not relay_execution_progress_seen
                ):
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": "relay_execution_missing",
                        "source": "reaction_finalize",
                        "reasons": ["closeout_continue_without_in_round_progress"],
                        "correction_count": relay_execution_correction_count,
                    })
                    if relay_execution_correction_count < 2:
                        relay_execution_correction_count += 1
                        accumulated_messages.append({
                            "role": "user",
                            "content": self._relay_execution_missing_feedback(
                                relay_execution_correction_count),
                        })
                        reaction_loop = dict(reaction_loop)
                        reaction_loop["reaction_loop_done"] = False
                        iteration_records.append({
                            "index": i,
                            "response": response_text[:200],
                            "containers_created": [],
                            "exit_signal": "relay_execution_missing",
                        })
                        i += 1
                        continue
                    all_invalid_tool_requests.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": "rejected",
                        "source": "reaction_loop_guard",
                        "reason": "relay_execution_missing",
                    })
                    exit_signal = "relay_execution_missing"
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": "relay_execution_missing",
                    })
                    i += 1
                    break
                relay_receipt = self._apply_closeout_relay_receipt(
                    settlement_ledger.get("relay_receipt"),
                    parsed_reaction.get("reaction_finalize_trace"),
                )
                if relay_receipt:
                    all_closeout_relay_receipts.append(relay_receipt)
                    pending_relay_target_for_next = (
                        pending_target_from_file_reads(
                            all_general_tool_results) or {}
                    )
                closeout_decision = str(
                    settlement_ledger.get("closeout_decision") or ""
                ).strip().lower()
                budget_status = ""
                if closeout_decision in {"finish", "blocked"}:
                    budget_status = settle_final_response_guards(
                        response_text,
                        "reaction_finalize",
                        parsed_reaction,
                        audit_iteration,
                    )
                if budget_status:
                    if budget_status == "blocked":
                        break
                    continue
                if settlement_ledger:
                    parsed_reaction["settlement_ledger"] = settlement_ledger
                    all_settlement_ledgers.append(settlement_ledger)
                reaction_finalize_validated = True
                final_reply_pending = True
                closeout_projection_text = str(response_text or "").strip()
                exit_signal = "final_reply_pending"
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": [],
                    "exit_signal": "native_finalize_form_validated",
                })
                i += 1
                break
            defer_closeout = False
            if reaction_loop_has_other_activity(parsed_reaction):
                if reaction_loop_has_protocol_submission_activity(parsed_reaction):
                    closeout_conflict_count += 1
                    status = "closeout_submission_deferred"
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_loop",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": status,
                        "source": "reaction_loop_done",
                    })
                    accumulated_messages.append({
                        "role": "user",
                        "content": (
                            "反应循环结束判定提醒：已记录本次协议工具提交，"
                            "但不要把 reaction_loop_done=true 和工具提交混写。"
                            "下一次迭代只做结束判定，不再声明工具或写入表。"
                        ),
                    })
                    reaction_loop = dict(reaction_loop)
                    reaction_loop["reaction_loop_done"] = False
                    defer_closeout = True
                else:
                    closeout_conflict_count += 1
                    status = (
                        "closeout_conflict_warning"
                        if closeout_conflict_count > 1
                        else "closeout_conflict_reminder"
                    )
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_loop",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": status,
                        "source": "reaction_loop_done",
                    })
                    accumulated_messages.append({
                        "role": "user",
                        "content": (
                            "反应循环结束判定提醒：若要结束反应循环，"
                            "本次迭代只能填写 reaction_loop_done=true，"
                            "不得同时声明工具、容器或写入表。"
                        ),
                    })
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": [],
                        "exit_signal": status,
                    })
                    i += 1
                    continue

            if not defer_closeout:
                accumulated_messages.append({
                    "role": "user",
                    "content": (
                        "反应循环结束判定已通过。若要闭合本轮，请直接自然语言回复用户；"
                        "若需要中继，调用 reaction_finalize(handoff_text)。"
                        "若还要补工具、清单或证据，可以继续在普通 reaction loop 中处理。"
                    ),
                })
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": [],
                    "exit_signal": "reaction_finalize_prompt_pending",
                })
                i += 1
                continue

        terminal_invalid_tool_requests = _tag_correctable_terminal_invalids(
            parsed_reaction.get("invalid_tool_requests", []),
            parsed_reaction,
            "loop",
        )
        correctable_terminal_invalids = [
            item for item in terminal_invalid_tool_requests
            if isinstance(item, dict)
            and item.get("correctable_terminal_attempt")
        ]
        if correctable_terminal_invalids:
            all_invalid_tool_requests.extend(correctable_terminal_invalids)
            can_retry_invalid = reaction_finalize_invalid_correction_count < 2
            reaction_loop_guard_receipts.append({
                "tool_id": "reaction_finalize",
                "tool_family": "substrate_tool",
                "tool_class": "sync_tool",
                "status": "reaction_finalize_invalid",
                "source": "reaction_finalize_invalid",
                "correction": "retry" if can_retry_invalid else "hard_fail",
                "reasons": [
                    str(item.get("reason") or "").strip()
                    for item in correctable_terminal_invalids
                    if str(item.get("reason") or "").strip()
                ],
            })
            if can_retry_invalid:
                reaction_finalize_invalid_correction_count += 1
                terminal_feedbacks = native_tool_failure_feedbacks(
                    correctable_terminal_invalids)
                pending_native_tool_feedbacks.extend(terminal_feedbacks)
                all_native_tool_feedbacks.extend(terminal_feedbacks)
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": [],
                    "exit_signal": "reaction_finalize_invalid_retry",
                })
                i += 1
                continue
            exit_signal = "reaction_finalize_invalid"
            iteration_records.append({
                "index": i,
                "response": response_text[:200],
                "containers_created": [],
                "exit_signal": "reaction_finalize_invalid",
            })
            i += 1
            break
        iter_created = []
        protocol_receipt_start = len(all_protocol_tool_receipts)

        iter_relation_declarations = parsed_reaction.get(
            "relation_card_declarations", [])
        all_relation_declarations.extend(iter_relation_declarations)
        all_protocol_tool_requests.extend(
            parsed_reaction.get("protocol_tool_requests", []))
        iter_invalid_tool_requests = _tag_correctable_terminal_invalids(
            parsed_reaction.get("invalid_tool_requests", []),
            parsed_reaction,
            "loop",
        )
        all_invalid_tool_requests.extend(iter_invalid_tool_requests)
        iter_native_feedbacks = native_tool_failure_feedbacks(
            iter_invalid_tool_requests)
        iter_protocol_submissions = parsed_reaction.get(
            "protocol_tool_submissions", [])
        iter_native_protocol_submissions = parsed_reaction.get(
            "native_protocol_tool_submissions", [])
        iter_accepted_tools = {
            normalize_tool_id(submission)
            for submission in (
                list(iter_protocol_submissions or [])
                + list(iter_native_protocol_submissions or [])
            )
        }
        all_protocol_tool_submissions.extend(iter_protocol_submissions)
        all_native_protocol_tool_submissions.extend(
            iter_native_protocol_submissions)
        all_invalid_protocol_tool_submissions.extend(
            parsed_reaction.get("invalid_protocol_tool_submissions", []))
        all_protocol_tool_receipts.extend(
            self.protocol_tool_dispatcher.build_submission_receipts(
                parsed_reaction.get("protocol_tool_submissions", []),
                parsed_reaction.get("invalid_protocol_tool_submissions", []),
            )
        )
        iter_general_tool_requests = parsed_reaction.get("general_tool_requests", [])
        all_general_tool_requests.extend(iter_general_tool_requests)
        prior_general_tool_results = list(all_general_tool_results)
        iter_general_tool_results = tool_settlement.handle_general_tool_results(
            iter_general_tool_requests=iter_general_tool_requests,
            active_general_tool_guides=active_general_tool_guides,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_general_tool_results=all_general_tool_results,
            iter_native_feedbacks=iter_native_feedbacks,
            round_num=round_num,
            iteration=audit_iteration,
            interaction_meta=interaction_meta,
        )
        duplicate_general_tool_results = []
        guard_trackable_general_tool_failures = []
        iter_general_tool_has_effective_progress = False
        if iter_general_tool_results:
            reaction_obligations.observe_general_tool_results(
                iter_general_tool_results)
            if pending_relay_target and file_read_target_satisfied(
                    pending_relay_target, all_general_tool_results):
                if not relay_target_satisfied:
                    reaction_finalize_invalid_correction_count = 0
                relay_target_satisfied = True
            duplicate_general_tool_results = [
                item for item in iter_general_tool_results
                if str(item.get("reason") or "").strip()
                in DUPLICATE_GENERAL_TOOL_REASONS
            ]
            guard_trackable_general_tool_failures = [
                item for item in iter_general_tool_results
                if item not in duplicate_general_tool_results
                and _general_tool_guard_failure_trackable(item)
            ]
            if guard_trackable_general_tool_failures:
                guard_failure_groups = _group_duplicate_general_tool_results(
                    guard_trackable_general_tool_failures)
                if (
                        general_tool_duplicate_last_signature
                        and general_tool_duplicate_last_signature
                        in guard_failure_groups):
                    duplicate_general_tool_results.extend(
                        _with_guard_duplicate_reference(
                            item, all_general_tool_results)
                        for item in guard_failure_groups[
                            general_tool_duplicate_last_signature]
                    )
                elif len(guard_failure_groups) == 1:
                    only_items = next(iter(guard_failure_groups.values()))
                    if len(only_items) > 1:
                        duplicate_general_tool_results.extend(
                            _with_guard_duplicate_reference(
                                item, all_general_tool_results)
                            for item in only_items
                        )
            iter_general_tool_has_effective_progress = any(
                item not in duplicate_general_tool_results
                and _general_tool_result_success(item)
                for item in iter_general_tool_results
            )
        iter_tool_summaries = tool_settlement.handle_tool_summaries(
            parsed_reaction=parsed_reaction,
            all_tool_summaries=all_tool_summaries,
            round_num=round_num,
            iteration=audit_iteration,
            interaction_meta=interaction_meta,
        )
        iter_memory_write_declarations = parsed_reaction.get(
            "memory_write_declarations", [])
        all_memory_write_declarations.extend(iter_memory_write_declarations)
        iter_memory_annotation_declarations = parsed_reaction.get(
            "memory_annotation_declarations", [])
        all_memory_annotation_declarations.extend(
            iter_memory_annotation_declarations)
        iter_memory_link_update_declarations = parsed_reaction.get(
            "memory_link_update_declarations", [])
        all_memory_link_update_declarations.extend(
            iter_memory_link_update_declarations)
        iter_memory_container_create_declarations = parsed_reaction.get(
            "memory_container_create_declarations", [])
        all_memory_container_create_declarations.extend(
            iter_memory_container_create_declarations)
        iter_memory_container_write_declarations = parsed_reaction.get(
            "memory_container_write_declarations", [])
        all_memory_container_write_declarations.extend(
            iter_memory_container_write_declarations)
        iter_memory_privacy_declarations = parsed_reaction.get(
            "memory_privacy_declarations", [])
        all_memory_privacy_declarations.extend(
            iter_memory_privacy_declarations)
        iter_memory_privacy_declassify_declarations = parsed_reaction.get(
            "memory_privacy_declassify_declarations", [])
        all_memory_privacy_declassify_declarations.extend(
            iter_memory_privacy_declassify_declarations)
        iter_chronicle_write_declarations = parsed_reaction.get(
            "chronicle_write_declarations", [])
        all_chronicle_write_declarations.extend(
            iter_chronicle_write_declarations)
        iter_alert_mode_settle_declarations = parsed_reaction.get(
            "alert_mode_settle_declarations", [])
        all_alert_mode_settle_declarations.extend(
            iter_alert_mode_settle_declarations)
        iter_fault_record_declarations = parsed_reaction.get(
            "fault_record_declarations", [])
        all_fault_record_declarations.extend(
            iter_fault_record_declarations)
        iter_guide_submit_requests = parsed_reaction.get(
            "guide_submit_requests", [])
        all_guide_submit_requests.extend(iter_guide_submit_requests)
        guide_submit_receipt_start = len(all_guide_submit_receipts)
        iter_container_focus_declarations = parsed_reaction.get(
            "container_focus_declarations", [])
        all_container_focus_declarations.extend(
            iter_container_focus_declarations)
        iter_mount_cancel_requests = parsed_reaction.get(
            "mount_cancel_requests", [])
        all_mount_cancel_requests.extend(iter_mount_cancel_requests)
        iter_relay_intent_settle_requests = parsed_reaction.get(
            "relay_intent_settle_requests", [])
        all_relay_intent_settle_requests.extend(
            iter_relay_intent_settle_requests)
        iter_memory_content_read_requests = [
            request for request in parsed_reaction.get("protocol_tool_requests", [])
            if normalize_tool_id(request.get("tool_id") if isinstance(request, dict) else request)
            == "memory_content_read"
        ]
        all_memory_content_read_requests.extend(iter_memory_content_read_requests)
        iter_corpus_read_requests = [
            request for request in parsed_reaction.get("protocol_tool_requests", [])
            if normalize_tool_id(request.get("tool_id") if isinstance(request, dict) else request)
            == "corpus_read"
        ]
        all_corpus_read_requests.extend(iter_corpus_read_requests)
        iter_container_read_requests = [
            request for request in parsed_reaction.get("protocol_tool_requests", [])
            if normalize_tool_id(request.get("tool_id") if isinstance(request, dict) else request)
            == "container_read"
        ]
        all_container_read_requests.extend(iter_container_read_requests)
        iter_index_view_requests = [
            request for request in parsed_reaction.get("protocol_tool_requests", [])
            if normalize_tool_id(request.get("tool_id") if isinstance(request, dict) else request)
            == "index_view"
        ]
        iter_memory_search_requests = [
            request for request in parsed_reaction.get("protocol_tool_requests", [])
            if normalize_tool_id(request.get("tool_id") if isinstance(request, dict) else request)
            == "memory_search"
        ]
        iter_relation_read_requests = [
            request for request in parsed_reaction.get("protocol_tool_requests", [])
            if normalize_tool_id(request.get("tool_id") if isinstance(request, dict) else request)
            == "relation_read"
        ]
        all_index_view_requests.extend(iter_index_view_requests)
        all_memory_search_requests.extend(iter_memory_search_requests)
        all_relation_read_requests.extend(iter_relation_read_requests)
        tool_settlement.handle_guide_submit(
            current_reaction_iteration=audit_iteration,
            iter_accepted_tools=iter_accepted_tools,
            iter_guide_submit_requests=iter_guide_submit_requests,
            current_general_tool_requests=iter_general_tool_requests,
            prior_general_tool_results=prior_general_tool_results,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_guide_submit_receipts=all_guide_submit_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        iter_guide_submit_receipts = all_guide_submit_receipts[
            guide_submit_receipt_start:]
        frame_guide_pending_memory_ids = {}
        mount_ids = tool_settlement.settle_guide_memory_writes(
            iter_guide_submit_receipts,
            round_num=round_num,
            accumulated_messages=accumulated_messages,
            all_memory_write_receipts=all_memory_write_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
            pending_memory_ids=pending_memory_ids,
            hidden_stm_memory_ids=hidden_stm_memory_ids,
            boosted_memory_ids=boosted_memory_ids,
            mount_ids=mount_ids,
            frame_pending_memory_ids=frame_guide_pending_memory_ids,
        )
        if (
            not mixed_reaction_finalize
            and any(
                isinstance(receipt, dict)
                and str(receipt.get("next_action") or "").strip()
                == "natural_final_reply"
                for receipt in iter_guide_submit_receipts
            )
        ):
            feedback = (
                "task_guide_completed: 当前任务清单已由 "
                "guide_submit(update_task_status) 自动结算并撤下。"
                "如果本轮事务已经闭合，下一次直接自然语言回复用户；"
                f"{closeout_final_reply_reminder(task_delivery=True)}"
                "如果还要补工具、清单或说明，可以继续普通 reaction loop。"
            )
            pending_native_tool_feedbacks.append(feedback)
            all_native_tool_feedbacks.append(feedback)
            reaction_loop_guard_receipts.append({
                "tool_id": "guide_submit",
                "tool_family": "protocol_tool",
                "tool_class": "runtime_guard",
                "status": "task_guide_completed_final_reply_available",
                "source": "reaction_loop",
                "next_action": "natural_final_reply",
            })
        mount_ids = tool_settlement.handle_memory_write(
            iter_accepted_tools=iter_accepted_tools,
            iter_memory_write_declarations=iter_memory_write_declarations,
            interaction_meta=interaction_meta,
            round_num=round_num,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_write_receipts=all_memory_write_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
            pending_memory_ids=pending_memory_ids,
            hidden_stm_memory_ids=hidden_stm_memory_ids,
            boosted_memory_ids=boosted_memory_ids,
            mount_ids=mount_ids,
            memory_write_rewrite_tracker=rewrite_tracker,
            rewrite_pending_at_frame_start=rewrite_pending_at_frame_start,
        )
        tool_settlement.handle_memory_link_update(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_memory_link_update_declarations=iter_memory_link_update_declarations,
            interaction_meta=interaction_meta,
            pending_memory_ids=pending_memory_ids,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_link_update_receipts=all_memory_link_update_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_memory_container_create(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_memory_container_create_declarations=iter_memory_container_create_declarations,
            interaction_meta=interaction_meta,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_container_create_receipts=all_memory_container_create_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
            all_created_containers=all_created_containers,
            pending_memory_ids=frame_guide_pending_memory_ids,
        )
        tool_settlement.handle_memory_container_write(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_memory_container_write_declarations=iter_memory_container_write_declarations,
            interaction_meta=interaction_meta,
            visible_focus_id=visible_focus_id,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_container_write_receipts=all_memory_container_write_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
            pending_memory_ids=frame_guide_pending_memory_ids,
        )
        tool_settlement.handle_corpus_read(
            iter_corpus_read_requests=iter_corpus_read_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_corpus_read_receipts=all_corpus_read_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_index_view(
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_index_view_requests=iter_index_view_requests,
            current_state=current_state,
            round_type=round_type,
            mount_ids_current=mount_ids_current,
            interaction_meta=interaction_meta,
            hidden_stm_memory_ids=hidden_stm_memory_ids,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_index_view_receipts=all_index_view_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_memory_search(
            iter_memory_search_requests=iter_memory_search_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_search_receipts=all_memory_search_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        mount_ids = tool_settlement.handle_relation_read(
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_relation_read_requests=iter_relation_read_requests,
            mount_ids=mount_ids,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_relation_read_receipts=all_relation_read_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_container_focus(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_container_focus_declarations=iter_container_focus_declarations,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            accumulated_messages=accumulated_messages,
            all_container_focus_receipts=all_container_focus_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
            all_created_containers=all_created_containers,
        )
        mount_ids = tool_settlement.handle_memory_content_read(
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_memory_content_read_requests=iter_memory_content_read_requests,
            interaction_meta=interaction_meta,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            accumulated_messages=accumulated_messages,
            all_memory_content_read_receipts=all_memory_content_read_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
            hidden_stm_memory_ids=hidden_stm_memory_ids,
            boosted_memory_ids=boosted_memory_ids,
            memory_reconsolidation_tracker=reconsolidation_tracker,
            round_num=round_num,
            mount_ids=mount_ids,
        )
        mount_ids = tool_settlement.handle_mount_cancel(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_mount_cancel_requests=iter_mount_cancel_requests,
            mount_ids=mount_ids,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_mount_cancel_receipts=all_mount_cancel_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        mount_ids = tool_settlement.handle_container_read(
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_container_read_requests=iter_container_read_requests,
            mount_ids=mount_ids,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_container_read_receipts=all_container_read_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_memory_privacy_mark(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_memory_privacy_declarations=iter_memory_privacy_declarations,
            interaction_meta=interaction_meta,
            pending_memory_ids=pending_memory_ids,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_privacy_receipts=all_memory_privacy_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_memory_privacy_declassify(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_memory_privacy_declassify_declarations=iter_memory_privacy_declassify_declarations,
            interaction_meta=interaction_meta,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_privacy_declassify_receipts=all_memory_privacy_declassify_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_memory_annotation_update(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_memory_annotation_declarations=iter_memory_annotation_declarations,
            interaction_meta=interaction_meta,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_memory_annotation_receipts=all_memory_annotation_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_chronicle_write(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_chronicle_write_declarations=iter_chronicle_write_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_chronicle_write_receipts=all_chronicle_write_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        alert_receipt_start = len(all_alert_mode_settle_receipts)
        tool_settlement.handle_alert_mode_settle(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_alert_mode_settle_declarations=iter_alert_mode_settle_declarations,
            interaction_meta=interaction_meta,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_alert_mode_settle_receipts=all_alert_mode_settle_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_fault_record(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_fault_record_declarations=iter_fault_record_declarations,
            interaction_meta=interaction_meta,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_fault_record_receipts=all_fault_record_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_relay_intent_settle(
            iter_accepted_tools=iter_accepted_tools,
            iter_relay_intent_settle_requests=iter_relay_intent_settle_requests,
            round_num=round_num,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_relay_intent_settle_receipts=all_relay_intent_settle_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        tool_settlement.handle_relation_card_write(
            iter_accepted_tools=iter_accepted_tools,
            active_protocol_tool_guides=active_protocol_tool_guides,
            iter_relation_declarations=iter_relation_declarations,
            interaction_meta=interaction_meta,
            mount_ids_current=mount_ids_current,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            all_relation_card_receipts=all_relation_card_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        iteration_protocol_receipts = all_protocol_tool_receipts[
            protocol_receipt_start:]
        if any(
                receipt.get("tool_id") == "chronicle_write"
                and receipt.get("reason") == "no_active_chronicle_focus"
                for receipt in iteration_protocol_receipts
                if isinstance(receipt, dict)):
            chronicle_no_active_focus_rejections += 1
            if chronicle_no_active_focus_rejections >= 2:
                feedback = (
                    "- kind: native_tool_result\n"
                    "  tier: warning\n"
                    "  decision_required: true\n"
                    "  tool_id: chronicle_write\n"
                    "  reason: no_active_chronicle_focus\n"
                    "  next_action: stop_repeating_or_reply_naturally\n"
                    "  message: |\n"
                    "    当前没有编年史写入焦点；不要重复提交当前编年 guide 选项。\n"
                    "    下一迭代停止重复提交；完成就自然回复用户，确需跨轮继续才调用 reaction_finalize(handoff_text)。"
                )
                pending_native_tool_feedbacks.append(feedback)
                all_native_tool_feedbacks.append(feedback)
        emergency_attempts_this_iter = self._emergency_tool_attempt_count(
            iter_protocol_submissions,
            iter_general_tool_requests,
        )
        if emergency_attempts_this_iter:
            current_flags = (
                (current_state or {}).get("base", {}).get(
                    "heartbeat_flags", {})
            )
            for emergency_item in active_emergency_items(current_flags):
                alert_type = emergency_item["flag"]
                if alert_type in emergency_auto_deferred:
                    continue
                if self._alert_settled_in_iteration(
                        alert_type,
                        all_alert_mode_settle_receipts[alert_receipt_start:]):
                    continue
                emergency_attempt_counts[alert_type] = (
                    emergency_attempt_counts.get(alert_type, 0)
                    + emergency_attempts_this_iter
                )
                decision = emergency_attempt_decision(
                    alert_type,
                    emergency_attempt_counts[alert_type],
                )
                if (
                    decision["action"] == "nudge_defer"
                    and alert_type not in emergency_attempt_nudged
                ):
                    iter_native_feedbacks.append(
                        self._emergency_attempt_notice(
                            alert_type,
                            emergency_attempt_counts[alert_type],
                        )
                    )
                    emergency_attempt_nudged.add(alert_type)
                elif decision["action"] == "auto_defer":
                    tool_settlement.record_alert_auto_defer(
                        alert_type=alert_type,
                        interaction_meta=interaction_meta,
                        accumulated_messages=accumulated_messages,
                        all_alert_mode_settle_receipts=all_alert_mode_settle_receipts,
                        all_protocol_tool_receipts=all_protocol_tool_receipts,
                    )
                    emergency_auto_deferred.add(alert_type)
                    reaction_loop_guard_receipts.append({
                        "tool_id": "alert_mode_settle",
                        "tool_family": "protocol_tool",
                        "tool_class": "runtime_guard",
                        "status": "emergency_attempt_auto_deferred",
                        "source": "reaction_loop",
                        "alert_type": alert_type,
                        "attempt_count": emergency_attempt_counts[alert_type],
                        "defer_seconds": decision["defer_seconds"],
                    })
        iter_protocol_receipts = all_protocol_tool_receipts[protocol_receipt_start:]
        if iter_protocol_receipts:
            self._write_protocol_tool_receipts(
                iter_protocol_receipts,
                round_num,
                audit_iteration,
                interaction_meta or {},
            )
        self._write_reasoning_context_if_needed(
            iter_native_tool_call_envelopes,
            iter_general_tool_results,
            iter_protocol_receipts,
            round_num,
            audit_iteration,
            interaction_meta or {},
        )
        runtime_guide_completed_flags.update(
            self._guide_completed_flags_from_receipts(
                iter_protocol_receipts,
                state=current_state,
                round_type=round_type,
                completed_flags=runtime_guide_completed_flags,
            ))
        runtime_guide_completed_flags.difference_update(
            self._guide_reopened_flags_from_receipts(iter_protocol_receipts))
        if (
            round_type == "relay"
            and self._relay_iteration_has_execution_progress(
                parsed_reaction,
                iter_general_tool_results,
                iter_protocol_receipts,
            )
        ):
            if not relay_execution_progress_seen:
                reaction_finalize_invalid_correction_count = 0
            relay_execution_progress_seen = True
        if iter_protocol_receipts:
            reaction_obligations.observe_receipts(iter_protocol_receipts)
            obligation_prompt = reaction_obligations.render_prompt()
            if obligation_prompt:
                accumulated_messages.append({
                    "role": "user",
                    "content": obligation_prompt,
                })
        iter_native_feedbacks.extend(
            native_tool_failure_feedbacks(iter_protocol_receipts))
        if mixed_reaction_finalize:
            finalize_errors = list(
                mixed_reaction_finalize.get("reaction_finalize_errors") or []
            )
            if finalize_errors:
                status = "reaction_finalize_invalid"
                invalid_request = {
                    "tool_id": "reaction_finalize",
                    "tool_family": "substrate_tool",
                    "tool_class": "sync_tool",
                    "status": "rejected",
                    "source": "reaction_finalize_mixed_post_settlement",
                    "reason": ";".join(str(item) for item in finalize_errors),
                    "call_id": (
                        (mixed_reaction_finalize.get(
                            "reaction_finalize_trace") or {}).get("call_id")
                    ),
                }
                all_invalid_tool_requests.append(invalid_request)
                closeout_feedbacks = native_tool_failure_feedbacks([
                    invalid_request
                ])
                iter_native_feedbacks.extend(closeout_feedbacks)
                reaction_loop_guard_receipts.append({
                    "tool_id": "reaction_finalize",
                    "tool_family": "substrate_tool",
                    "tool_class": "sync_tool",
                    "status": status,
                    "source": "reaction_finalize_mixed_post_settlement",
                    "reasons": list(finalize_errors),
                })

        current_work_guide_id = _current_work_guide_id(self.workbench)
        new_blocker_evidence_refs = _new_blocker_evidence_refs(
            guide_correction_evidence_refs,
            iter_general_tool_results,
            iter_protocol_receipts,
        )
        correction_progress = (
            iter_general_tool_has_effective_progress
            or _has_effective_protocol_progress(iter_protocol_receipts)
            or bool(new_blocker_evidence_refs)
        )
        if current_work_guide_id != guide_correction_active_id:
            guide_correction_active_id = current_work_guide_id
            guide_correction_rejections = []
            guide_correction_rejection_frames = 0
            guide_correction_evidence_refs = set()
        elif correction_progress:
            guide_correction_rejections = []
            guide_correction_rejection_frames = 0
        guide_correction_evidence_refs.update(new_blocker_evidence_refs)
        rejected_guide_receipts = [
            receipt
            for receipt in iter_guide_submit_receipts
            if isinstance(receipt, dict)
            and str(receipt.get("status") or "").strip() == "rejected"
            and str(receipt.get("guide_id") or "").strip() == current_work_guide_id
        ]
        if (
                current_work_guide_id
                and not correction_progress
                and rejected_guide_receipts):
            guide_correction_rejection_frames += 1
            guide_correction_rejections.extend({
                "reason": str(receipt.get("reason") or "guide_submission_rejected").strip(),
                "error_hint": copy.deepcopy(receipt.get("error_hint") or {}),
            } for receipt in rejected_guide_receipts)
            guide_correction_rejections = guide_correction_rejections[-3:]

        if guide_correction_rejection_frames >= 3:
            blocked_reason = "blocked/task_guide_correction_exhausted"
            blockers = [item["reason"] for item in guide_correction_rejections]
            auto_blocked_ledger = {
                "closeout_decision": "blocked",
                "handoff_text": "",
                "auto_blocked": True,
                "blocked_reason": blocked_reason,
                "blockers": blockers,
                "error_hints": [
                    copy.deepcopy(item["error_hint"])
                    for item in guide_correction_rejections
                ],
                "source": "reaction_task_guide_correction",
            }
            parsed_reaction["settlement_ledger"] = auto_blocked_ledger
            all_settlement_ledgers.append(auto_blocked_ledger)
            reaction_loop_guard_receipts.append({
                "tool_id": "guide_submit",
                "tool_family": "protocol_tool",
                "tool_class": "runtime_guard",
                "status": "task_guide_correction_exhausted_auto_blocked",
                "source": "reaction_task_guide_correction",
                "reason": blocked_reason,
                "rejection_count": guide_correction_rejection_frames,
                "rejected_receipt_count": len(guide_correction_rejections),
                "blockers": blockers,
                "error_hints": [
                    copy.deepcopy(item["error_hint"])
                    for item in guide_correction_rejections
                ],
            })
            all_native_tool_feedbacks.extend(iter_native_feedbacks)
            reaction_finalize_validated = True
            final_reply_pending = True
            result_state.local_blocked_reason = blocked_reason
            exit_signal = "final_reply_pending"
            iteration_records.append({
                "index": i,
                "response": response_text[:200],
                "containers_created": iter_created,
                "exit_signal": "task_guide_correction_exhausted_auto_blocked",
            })
            i += 1
            break

        effective_tool_progress = (
            iter_general_tool_has_effective_progress
            or _has_effective_protocol_progress(iter_protocol_receipts)
        )
        protocol_read_block = protocol_read_duplicate_guard.observe(
            iter_protocol_receipts,
            effective_tool_progress,
        )
        if protocol_read_block:
            blocked_reason = protocol_read_block["blocked_reason"]
            auto_blocked_ledger = protocol_read_block["settlement_ledger"]
            parsed_reaction["settlement_ledger"] = auto_blocked_ledger
            all_settlement_ledgers.append(auto_blocked_ledger)
            reaction_loop_guard_receipts.append(
                protocol_read_block["guard_receipt"])
            all_native_tool_feedbacks.extend(iter_native_feedbacks)
            reaction_finalize_validated = True
            final_reply_pending = True
            result_state.local_blocked_reason = blocked_reason
            exit_signal = "final_reply_pending"
            iteration_records.append({
                "index": i,
                "response": response_text[:200],
                "containers_created": iter_created,
                "exit_signal": "protocol_read_correction_exhausted_auto_blocked",
            })
            i += 1
            break

        if duplicate_general_tool_results:
            iter_native_feedbacks = _remove_general_tool_duplicate_feedbacks(
                iter_native_feedbacks)
            duplicate_groups = _group_duplicate_general_tool_results(
                duplicate_general_tool_results)
            effective_new_progress = (
                iter_general_tool_has_effective_progress
                or _has_effective_protocol_progress(iter_protocol_receipts)
            )
            selected_signature = ""
            selected_items = []
            duplicate_has_main_chain = False
            if effective_new_progress:
                general_tool_duplicate_last_signature = ""
                general_tool_duplicate_streak_count = 0
                selected_signature = next(iter(duplicate_groups), "")
                selected_items = duplicate_groups.get(selected_signature, [])
            elif (
                general_tool_duplicate_last_signature
                and general_tool_duplicate_last_signature in duplicate_groups
            ):
                selected_signature = general_tool_duplicate_last_signature
                selected_items = duplicate_groups[selected_signature]
                general_tool_duplicate_streak_count += 1
                duplicate_has_main_chain = True
            elif len(duplicate_groups) == 1:
                selected_signature = next(iter(duplicate_groups), "")
                selected_items = duplicate_groups[selected_signature]
                general_tool_duplicate_last_signature = selected_signature
                general_tool_duplicate_streak_count = 1
                duplicate_has_main_chain = True
            else:
                general_tool_duplicate_last_signature = ""
                general_tool_duplicate_streak_count = 0
                selected_signature = next(iter(duplicate_groups), "")
                selected_items = duplicate_groups.get(selected_signature, [])

            if not selected_items:
                selected_items = list(duplicate_general_tool_results)
            selected_guard_failure_chain = any(
                _general_tool_guard_failure_trackable(item)
                and str(item.get("reason") or "").strip()
                not in DUPLICATE_GENERAL_TOOL_REASONS
                for item in selected_items
            )
            closeout_threshold = 2 if selected_guard_failure_chain else 3
            closeout_next = (
                not effective_new_progress
                and bool(selected_signature)
                and general_tool_duplicate_streak_count >= closeout_threshold
            )
            tier = (
                "warning"
                if closeout_next or general_tool_duplicate_streak_count >= 2
                else "reminder"
            )
            iter_native_feedbacks.append(
                _format_general_tool_duplicate_guard_feedback(
                    items=selected_items,
                    tier=tier,
                    streak_count=general_tool_duplicate_streak_count,
                    effective_new_progress=effective_new_progress,
                    multi_signature_count=len(duplicate_groups),
                    has_main_chain=duplicate_has_main_chain,
                    closeout_next=closeout_next,
                )
            )
            reasons = sorted({
                str(item.get("reason") or "").strip()
                for item in duplicate_general_tool_results
                if str(item.get("reason") or "").strip()
            })
            reaction_loop_guard_receipts.append({
                "tool_id": "general_tool",
                "tool_family": "general_tool",
                "tool_class": "runtime_guard",
                "status": "general_tool_duplicate_guard",
                "source": "reaction_loop",
                "duplicate_signature": selected_signature,
                "duplicate_streak_count": general_tool_duplicate_streak_count,
                "same_iteration_duplicate_count": len(selected_items),
                "duplicate_signature_count": len(duplicate_groups),
                "effective_new_progress": effective_new_progress,
                "reasons": reasons,
            })
            if closeout_next:
                general_tool_duplicate_closeout_pending = True
                general_tool_duplicate_closeout_info = {
                    "duplicate_signature": selected_signature,
                    "duplicate_streak_count": general_tool_duplicate_streak_count,
                    "same_iteration_duplicate_count": len(selected_items),
                    "reasons": reasons,
                }
        elif (
            iter_general_tool_has_effective_progress
            or _has_effective_protocol_progress(iter_protocol_receipts)
        ):
            general_tool_duplicate_last_signature = ""
            general_tool_duplicate_streak_count = 0
        elif guard_trackable_general_tool_failures:
            guard_failure_groups = _group_duplicate_general_tool_results(
                guard_trackable_general_tool_failures)
            if len(guard_failure_groups) == 1:
                general_tool_duplicate_last_signature = next(
                    iter(guard_failure_groups), "")
                general_tool_duplicate_streak_count = 0
            else:
                general_tool_duplicate_last_signature = ""
                general_tool_duplicate_streak_count = 0
        if iter_native_feedbacks:
            pending_native_tool_feedbacks.extend(iter_native_feedbacks)
            all_native_tool_feedbacks.extend(iter_native_feedbacks)
            if general_tool_duplicate_closeout_pending:
                accumulated_messages.append({
                    "role": "user",
                    "content": (
                        "检测到同一通用工具重复链已达到纠偏阈值。"
                        "请停止原样复读，改参数推进，或直接自然回复说明当前结果。"
                        "确需跨轮继续时调用 reaction_finalize(handoff_text)。"
                    ),
                })
                reaction_loop_guard_receipts.append({
                    "tool_id": "general_tool",
                    "tool_family": "general_tool",
                    "tool_class": "runtime_guard",
                    "status": "general_tool_duplicate_stop_or_finalize",
                    "source": "reaction_loop",
                    **general_tool_duplicate_closeout_info,
                })
                general_tool_duplicate_closeout_pending = False
                general_tool_duplicate_closeout_info = {}
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "general_tool_duplicate_stop_or_finalize",
                })
                i += 1
                continue
            iteration_records.append({
                "index": i,
                "response": response_text[:200],
                "containers_created": iter_created,
                "exit_signal": "native_tool_feedback_retry",
            })
            i += 1
            continue

        if mixed_reaction_finalize:
            validation = reaction_obligations.validate_closeout_form(
                mixed_reaction_finalize.get("closeout_form"))
            settlement_ledger = validation.get("settlement_ledger") or {}
            if validation.get("blocked"):
                reasons = list(validation.get("reasons") or [])
                reaction_loop_guard_receipts.append({
                    "tool_id": "reaction_finalize",
                    "tool_family": "substrate_tool",
                    "tool_class": "sync_tool",
                    "status": "reaction_finalize_invalid",
                    "source": "reaction_finalize_mixed_post_settlement",
                    "reasons": reasons,
                })
                pending_native_tool_feedbacks.append(
                    "reaction_finalize 中继交接参数非法："
                    + "；".join(str(item) for item in reasons)
                    + "\n完成时直接自然语言回复用户；跨轮继续时调用 reaction_finalize(handoff_text)。"
                )
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "reaction_finalize_invalid",
                })
                i += 1
                continue
            task_acceptance = self._task_closeout_acceptance(
                mixed_reaction_finalize.get("closeout_form"))
            if not task_acceptance.get("allowed", True):
                if task_acceptance.get("terminal_blocked") is True:
                    budget_status = settle_final_response_guards(
                        response_text,
                        "reaction_finalize_mixed_terminal_blocked",
                        parsed_reaction,
                        audit_iteration,
                        iter_created,
                    )
                    if budget_status:
                        if budget_status == "blocked":
                            break
                        continue
                    settlement_ledger = _terminal_blocked_ledger(
                        task_acceptance,
                        "reaction_finalize_mixed_post_settlement",
                    )
                    parsed_reaction["settlement_ledger"] = settlement_ledger
                    all_settlement_ledgers.append(settlement_ledger)
                    reaction_loop_guard_receipts.append({
                        "tool_id": "reaction_finalize",
                        "tool_family": "substrate_tool",
                        "tool_class": "sync_tool",
                        "status": "task_acceptance_terminal_blocked",
                        "source": "reaction_finalize_mixed_post_settlement",
                        "reason": task_acceptance.get("reason"),
                        "blockers": task_acceptance.get("blockers", []),
                    })
                    reaction_finalize_validated = True
                    final_reply_pending = True
                    closeout_projection_text = str(response_text or "").strip()
                    exit_signal = "final_reply_pending"
                    iteration_records.append({
                        "index": i,
                        "response": response_text[:200],
                        "containers_created": iter_created,
                        "exit_signal": "task_acceptance_terminal_blocked",
                    })
                    i += 1
                    break
                pending_native_tool_feedbacks.append(
                    self._task_acceptance_feedback(task_acceptance))
                reaction_loop_guard_receipts.append({
                    "tool_id": "reaction_finalize",
                    "tool_family": "substrate_tool",
                    "tool_class": "sync_tool",
                    "status": "task_acceptance_blocked",
                    "source": "reaction_finalize_mixed_post_settlement",
                    "reason": task_acceptance.get("reason"),
                    "blockers": task_acceptance.get("blockers", []),
                })
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "task_acceptance_blocked",
                })
                i += 1
                continue
            if (
                round_type == "relay"
                and settlement_ledger.get("relay_receipt")
                and pending_relay_target
                and not relay_target_satisfied
            ):
                reaction_loop_guard_receipts.append(
                    self._relay_target_unfulfilled_receipt(
                        "reaction_finalize",
                        pending_relay_target,
                        relay_target_correction_count,
                    ))
                pending_native_tool_feedbacks.append(
                    target_feedback(
                        pending_relay_target,
                        relay_target_correction_count + 1,
                    ))
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "relay_target_unfulfilled",
                })
                i += 1
                continue
            if (
                round_type == "relay"
                and settlement_ledger.get("relay_receipt")
                and not relay_execution_progress_seen
            ):
                reaction_loop_guard_receipts.append({
                    "tool_id": "reaction_finalize",
                    "tool_family": "substrate_tool",
                    "tool_class": "sync_tool",
                    "status": "relay_execution_missing",
                    "source": "reaction_finalize_mixed_post_settlement",
                    "reasons": ["closeout_continue_without_in_round_progress"],
                    "correction_count": relay_execution_correction_count,
                })
                pending_native_tool_feedbacks.append(
                    self._relay_execution_missing_feedback(
                        relay_execution_correction_count + 1))
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "relay_execution_missing",
                })
                i += 1
                continue
            relay_receipt = self._apply_closeout_relay_receipt(
                settlement_ledger.get("relay_receipt"),
                mixed_reaction_finalize.get("reaction_finalize_trace"),
            )
            if relay_receipt:
                all_closeout_relay_receipts.append(relay_receipt)
                pending_relay_target_for_next = (
                    pending_target_from_file_reads(
                        all_general_tool_results) or {}
                )
            closeout_decision = str(
                settlement_ledger.get("closeout_decision") or ""
            ).strip().lower()
            budget_status = ""
            if closeout_decision in {"finish", "blocked"}:
                budget_status = settle_final_response_guards(
                    response_text,
                    "reaction_finalize_mixed_post_settlement",
                    parsed_reaction,
                    audit_iteration,
                    iter_created,
                )
            if budget_status:
                if budget_status == "blocked":
                    break
                continue
            if settlement_ledger:
                parsed_reaction["settlement_ledger"] = settlement_ledger
                all_settlement_ledgers.append(settlement_ledger)
            reaction_loop_guard_receipts.append({
                "tool_id": "reaction_finalize",
                "tool_family": "substrate_tool",
                "tool_class": "sync_tool",
                "status": "mixed_reaction_finalize_post_settled",
                "source": "reaction_finalize_mixed_post_settlement",
                "has_relay_receipt": bool(relay_receipt),
            })
            reaction_finalize_validated = True
            final_reply_pending = True
            closeout_projection_text = str(response_text or "").strip()
            exit_signal = "final_reply_pending"
            iteration_records.append({
                "index": i,
                "response": response_text[:200],
                "containers_created": iter_created,
                "exit_signal": "mixed_reaction_finalize_post_settled",
            })
            i += 1
            break

        progress_only = (
            bool(assistant_progress)
            and not native_terminal_finalize_only
            and not parsed_has_other_activity
        )
        if progress_only:
            reaction_progress_repeat_count += 1
            if reaction_progress_repeat_count == 1:
                progress_correction = "recorded"
            elif reaction_progress_repeat_count == 2:
                progress_correction = "reminder"
            elif reaction_progress_repeat_count == 3:
                progress_correction = "warning"
            else:
                progress_correction = "auto_block"
            reaction_loop_guard_receipts.append({
                "tool_id": "reaction.progress",
                "tool_family": "message_channel",
                "tool_class": "runtime_guard",
                    "status": "reaction_progress_only",
                    "source": "reaction_message_channel",
                    "correction": progress_correction,
                    "repeat_count": reaction_progress_repeat_count,
                "counter_basis": "structure_only",
            })
            if progress_correction == "reminder":
                pending_native_tool_feedbacks.append(
                    "reaction_progress_repeat_reminder: 你已经开始重复轮中进展。"
                    "如有其他事务，请继续调用合法工具；如需结束本轮，请直接自然语言回复用户；"
                    "如需跨轮继续，才调用 reaction_finalize(handoff_text)。"
                )
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "reaction_progress_repeat_reminder",
                })
                i += 1
                continue
            if progress_correction == "warning":
                pending_native_tool_feedbacks.append(
                    "reaction_progress_repeat_warning: 你仍在重复轮中进展。"
                    "下一次若继续复读，Runtime 将按重复进展事故停止当前轮。"
                    "当前仍可继续调用合法工具；完成时直接自然语言回复用户；"
                    "跨轮继续才调用 reaction_finalize(handoff_text)。"
                )
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "reaction_progress_repeat_warning",
                })
                i += 1
                continue
            if progress_correction == "auto_block":
                auto_blocked_ledger = {
                    "closeout_decision": "blocked",
                    "handoff_text": "",
                    "auto_blocked": True,
                    "blocked_reason": "reaction_progress_repeat",
                    "blockers": ["reaction_progress_repeat"],
                }
                parsed_reaction["settlement_ledger"] = auto_blocked_ledger
                all_settlement_ledgers.append(auto_blocked_ledger)
                reaction_loop_guard_receipts.append({
                    "tool_id": "reaction.progress",
                    "tool_family": "message_channel",
                    "tool_class": "runtime_guard",
                    "status": "reaction_progress_repeat_auto_blocked",
                    "source": "reaction_message_channel",
                    "reason": "reaction_progress_repeat",
                    "repeat_count": reaction_progress_repeat_count,
                })
                reaction_finalize_validated = True
                final_reply_pending = True
                exit_signal = "final_reply_pending"
                iteration_records.append({
                    "index": i,
                    "response": response_text[:200],
                    "containers_created": iter_created,
                    "exit_signal": "reaction_progress_repeat_auto_blocked",
                })
                i += 1
                break

        # 解析退出信号
        exit_signal = parsed_reaction.get("exit_signal") or "done"

        # 记录迭代
        iteration_records.append({
            "index": i,
            "response": response_text[:200],
            "containers_created": iter_created,
            "exit_signal": exit_signal,
        })
        i += 1

        # 退出信号→循环终止
        if (
            reaction_loop.get("reaction_loop_done") is False
        ):
            append_reaction_loop_handoff_to_messages(
                accumulated_messages,
                reaction_loop,
                targets={"next_reaction_iter"},
            )
            continue

        if exit_signal in REACTION_EXIT_SIGNALS:
            break

    if reaction_finalize_validated and final_reply_pending:
        final_response, final_reply_done, final_response_source = (
            self._project_reaction_terminal_response(
                all_settlement_ledgers,
                closeout_projection_text,
            )
        )
        final_reply_pending = False
        if final_reply_done:
            exit_signal = "done"
            cleanup_receipt = (
                apply_task_bootstrap_missing_access_terminal_settlement(
                    self.workbench,
                    self.sm,
                    all_settlement_ledgers[-1] if all_settlement_ledgers else {},
                )
            )
            if cleanup_receipt:
                reaction_loop_guard_receipts.append(cleanup_receipt)
            for item in all_message_envelopes:
                if (
                        str((item or {}).get("channel") or "").strip()
                        == "assistant_text"
                        and item.get("terminal_text_candidate")
                        and str((item or {}).get("text") or "").strip()
                        == str(final_response or "").strip()):
                    item["channel"] = "final_reply.text"
                    item["phase"] = "loop"
                    item["source"] = final_response_source
                    item["round_num"] = round_num
                    item["iteration"] = item.get("iteration") or 1
                    item.pop("terminal_text_candidate", None)
                    item.pop("terminal_decision", None)
                    break
            already_projected = any(
                str((item or {}).get("channel") or "").strip()
                == "final_reply.text"
                and str((item or {}).get("text") or "").strip()
                == str(final_response or "").strip()
                for item in all_message_envelopes
            )
            if not already_projected:
                all_message_envelopes.append(build_message_envelope(
                    "final_reply.text",
                    text=final_response,
                    phase="loop",
                    round_num=round_num,
                    iteration=1,
                    source=final_response_source,
                ))
        else:
            latest_ledger = (
                all_settlement_ledgers[-1]
                if all_settlement_ledgers else {}
            )
            if (
                isinstance(latest_ledger, dict)
                and str(latest_ledger.get("closeout_decision") or "").strip()
                == "continue"
            ):
                exit_signal = "continue_requested"
            else:
                exit_signal = "closeout_done_without_response"
        reaction_loop_guard_receipts.append({
            "tool_id": "reaction_finalize",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": "reaction_terminal_projected",
            "source": "reaction_loop",
            "final_response_source": final_response_source,
            "has_final_response": bool(final_response),
        })
        if final_response_source == "reaction.runtime_auto_blocked_final_reply":
            reaction_loop_guard_receipts.append({
                "tool_id": "final_reply",
                "tool_family": "substrate_tool",
                "tool_class": "sync_tool",
                "status": "runtime_auto_blocked_final_reply",
                "source": "reaction_loop",
                "reasons": ["auto_blocked_settlement_truth_guard"],
            })

    if reaction_finalize_validated and final_reply_done:
        corrected_invalid_tool_requests = _corrected_terminal_invalid_requests(
            all_invalid_tool_requests)

    tool_transaction_audit = audit_tool_transactions(
        requests=all_protocol_tool_requests,
        submissions=all_protocol_tool_submissions,
        invalid_submissions=all_invalid_protocol_tool_submissions,
        invalid_requests=all_invalid_tool_requests,
        corrected_invalid_requests=corrected_invalid_tool_requests,
        receipts=all_protocol_tool_receipts,
        active_guides=active_protocol_tool_guides,
    )

    result_state.provider_call_hard_stop = provider_call_hard_stop or {}
    result_state.single_round_probe_hard_stop = (
        single_round_probe_hard_stop or {})
    result_state.final_response = final_response
    result_state.corrected_invalid_tool_requests = (
        corrected_invalid_tool_requests)
    result_state.tool_transaction_audit = tool_transaction_audit
    result_state.pending_relay_target_for_next = pending_relay_target_for_next
    result_state.last_reaction_loop = last_reaction_loop
    result_state.exit_signal = exit_signal
    result_state.reaction_finalize_validated = reaction_finalize_validated
    result_state.final_reply_pending = final_reply_pending
    result_state.final_reply_done = final_reply_done
    result_state.final_response_source = final_response_source
    result_state.final_response_length_rejections = (
        final_response_length_rejections)
    result_state.response_contract = dict(loop_state.response_contract)
    result_state.interaction_meta = interaction_meta or {}

    if session.pending_frame_ref is not None:
        frame_ref = session.pending_frame_ref
        counts = session.pending_frame_counts
        session.pending_frame_ref = None
        session.pending_frame_counts = None
        settlement = _settle_reaction_frame(
            self,
            result_state,
            frame_ref,
            counts,
            exit_signal,
        )
        yield settlement

    self._round_audit_settlement(
        round_num,
        "reaction",
        max(len(iteration_records), 1),
        {
            "exit_signal": exit_signal,
            "protocol_tool_requests": all_protocol_tool_requests,
            "protocol_tool_submissions": all_protocol_tool_submissions,
            "native_protocol_tool_submissions": all_native_protocol_tool_submissions,
            "invalid_tool_requests": all_invalid_tool_requests,
            "corrected_invalid_tool_requests": corrected_invalid_tool_requests,
            "protocol_tool_receipts": all_protocol_tool_receipts,
            "native_tool_feedbacks": all_native_tool_feedbacks,
            "native_tool_feedback_count": len(all_native_tool_feedbacks),
            "general_tool_requests": all_general_tool_requests,
            "general_tool_results": all_general_tool_results,
            "tool_summaries": all_tool_summaries,
            "tool_transaction_audit": tool_transaction_audit,
            "reaction_obligations": reaction_obligations.audit_state(),
            "settlement_ledgers": all_settlement_ledgers,
            "pending_relay_target": pending_relay_target,
            "pending_relay_target_for_next": pending_relay_target_for_next,
            "corpus_read_requests": all_corpus_read_requests,
            "corpus_read_receipts": all_corpus_read_receipts,
            "index_view_requests": all_index_view_requests,
            "index_view_receipts": all_index_view_receipts,
            "memory_search_requests": all_memory_search_requests,
            "memory_search_receipts": all_memory_search_receipts,
            "relation_read_requests": all_relation_read_requests,
            "relation_read_receipts": all_relation_read_receipts,
            "mount_cancel_requests": all_mount_cancel_requests,
            "mount_cancel_receipts": all_mount_cancel_receipts,
            "relay_intent_settle_requests": all_relay_intent_settle_requests,
            "relay_intent_settle_receipts": all_relay_intent_settle_receipts,
            "guide_submit_requests": all_guide_submit_requests,
            "guide_submit_receipts": all_guide_submit_receipts,
            "memory_write_rewrite": rewrite_tracker.audit_state(),
            "chronicle_write_declarations": all_chronicle_write_declarations,
            "chronicle_write_receipts": all_chronicle_write_receipts,
            "alert_mode_settle_declarations": all_alert_mode_settle_declarations,
            "alert_mode_settle_receipts": all_alert_mode_settle_receipts,
            "reaction_iterations": iteration_records,
        },
    )

    return build_reaction_loop_result(result_state)
