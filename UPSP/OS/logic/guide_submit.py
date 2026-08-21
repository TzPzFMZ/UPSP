"""Apply Spec434 guide_submit answer sheets."""

import fnmatch
import glob
import os
import re
from pathlib import Path

from errors import ReadError
from logic.alert_mode_settle import apply_alert_mode_settlement_declarations
from logic.chronicle_write import apply_chronicle_write_declarations
from logic.fault_record import apply_fault_record_declarations
from logic.memory_reconsolidation import (
    GUIDE_ID_PREFIX as MEMORY_RECONSOLIDATION_GUIDE_PREFIX,
    apply_memory_reconsolidation_guide,
)
from logic.memory_write_rewrite import (
    GUIDE_ID_PREFIX as MEMORY_WRITE_REWRITE_GUIDE_PREFIX,
    apply_memory_write_rewrite_guide,
)
from logic.task_acceptance import validate_task_closeout
from logic.task_guide import (
    BOOTSTRAP_ITEM_ID,
    BOOTSTRAP_SUBMIT_OPTION_ID,
    TASK_PENDING_INPUT_OPTION_ID,
    TASK_PROGRESS_ITEM_ID,
    TASK_PROGRESS_UPDATE_OPTION_ID,
    _adapt_pending_input_update_fields,
    append_task_pending_input,
    apply_pending_input_integration,
    apply_task_status_update,
    create_task_bootstrap_guide,
    has_open_pending_inputs,
    materialize_initial_task_guide,
    open_pending_input_ids,
    refresh_task_execution_active_guide,
)
from logic.reaction_resident_guide import (
    REACTION_LOOP_GUIDE_ID,
    reaction_loop_resident_guide,
)
from logic.work_intent_debt import clear_work_intent_debt

RHYTHM_GUIDE_KINDS = {
    "main_axis_rhythm_guide",
    "calendar_rhythm_guide",
    "emergency_handling_guide",
    "context_pressure_rhythm_guide",
    "memory_compression_rhythm_guide",
}

GUIDE_SUBMIT_RESERVED_ARGUMENT_KEYS = {
    "guide_id",
    "submissions",
    "item_id",
    "option_id",
    "fields",
    "evidence_refs",
    "reason",
    # Provider/native envelope metadata added by the parser. These are not
    # model-authored guide fields and must not enter the answer sheet.
    "call_id",
    "provider",
    "response_id",
    "provider_item_id",
    "provider_item_id_is_synthetic",
    "index",
    "tool_id",
    "tool_family",
    "tool_class",
    "risk",
    "parse_status",
}


def apply_guide_submit(workbench_store, arguments, evidence_context=None):
    arguments = arguments if isinstance(arguments, dict) else {}
    evidence_context = dict(evidence_context or {})
    evidence_context.setdefault("workbench_store", workbench_store)
    guide_id = str(arguments.get("guide_id") or "").strip()
    context_store = evidence_context.get("context_store")
    progressive_debt = {}
    if context_store is not None:
        loader = getattr(context_store, "load_cache_compaction_debt", None)
        progressive_debt = loader() if callable(loader) else {}
    if progressive_debt.get("schema_version") == "cache_compaction_debt.v3":
        expected_guide_id = (
            "cache_compaction:" + str(progressive_debt.get("compaction_id") or "")
        )
        item_id = str(arguments.get("item_id") or "").strip()
        option_id = str(arguments.get("option_id") or "").strip()
        if (
                guide_id != expected_guide_id
                or item_id != "cache_compaction_due"
                or option_id != "submit_cache_compaction_batch"):
            return _reject(
                guide_id,
                "cache_compaction_pending",
                {
                    "current": {
                        "guide_id": expected_guide_id,
                        "item_id": "cache_compaction_due",
                        "option_id": "submit_cache_compaction_batch",
                    },
                    "next_action": "先按当前最近缓存压缩指南提交本批分片。",
                },
            )
        fields = arguments.get("fields") if isinstance(
            arguments.get("fields"), dict) else {}
        stager = getattr(context_store, "stage_progressive_cache_compaction", None)
        if not callable(stager):
            return _reject(guide_id, "cache_compaction_v3_stager_missing", {})
        report = stager(
            fields.get("results"),
            current_round=evidence_context.get("round_num"),
            current_reaction_iteration=evidence_context.get(
                "current_reaction_iteration"
            ),
        )
        receipt = _base_receipt(guide_id)
        receipt.update({
            "status": report.get("status", "rejected"),
            "reason": report.get("reason", ""),
            "action": "cache_compaction_batch_settled",
            "accepted_submissions": (
                [_normalize_submission(arguments)]
                if report.get("status") == "applied" else []
            ),
            "backend_receipts": [{
                "schema_version": "cache_compaction_batch_receipt.v3",
                "operation_id": "progressive_cache_compaction",
                **report,
            }],
            "cache_compaction": report,
            "completed_ids": report.get("completed_ids") or [],
            "remaining_ids": report.get("remaining_ids") or [],
        })
        return receipt
    reconsolidation_tracker = evidence_context.get(
        "memory_reconsolidation_tracker"
    )
    reconsolidation_pending = bool(
        reconsolidation_tracker is not None
        and callable(getattr(reconsolidation_tracker, "has_pending", None))
        and reconsolidation_tracker.has_pending()
    )
    if reconsolidation_pending:
        if guide_id != reconsolidation_tracker.guide_id:
            return _reject(
                guide_id,
                "memory_reconsolidation_pending",
                {
                    "attempted": {"guide_id": guide_id},
                    "current": {
                        "guide_id": reconsolidation_tracker.guide_id
                    },
                    "next_action": (
                        "先按当前回忆重整指南提交全部待处理记忆。"
                    ),
                },
            )
        backend = apply_memory_reconsolidation_guide(
            arguments, evidence_context
        )
        receipt = _base_receipt(guide_id)
        receipt.update({
            "status": backend.get("status", "rejected"),
            "reason": backend.get("reason", ""),
            "action": "memory_reconsolidation_settled",
            "accepted_submissions": (
                [_normalize_submission(arguments)]
                if backend.get("status") == "applied" else []
            ),
            "backend_receipts": backend.get("backend_receipts") or [],
            "completed_ids": backend.get("completed_ids") or [],
            "remaining_ids": backend.get("remaining_ids") or [],
        })
        if receipt["status"] == "rejected":
            receipt["error_hint"] = _guide_error_hint(
                receipt["reason"],
                {
                    "current": {"guide_id": guide_id},
                    "expected": {
                        "guide_id": reconsolidation_tracker.guide_id,
                        "pending_ids": reconsolidation_tracker.pending_ids(),
                    },
                    "next_action": (
                        "按回执修正 semantic_content/final_keywords，"
                        "并重新提交仍待处理的全部 ID。"
                    ),
                },
            )
        return receipt
    if guide_id.startswith(f"{MEMORY_RECONSOLIDATION_GUIDE_PREFIX}:"):
        return _reject(
            guide_id,
            "memory_reconsolidation_guide_not_active",
            {"current": {"guide_id": ""}},
        )
    rewrite_tracker = evidence_context.get("memory_write_rewrite_tracker")
    rewrite_pending = bool(
        rewrite_tracker is not None
        and callable(getattr(rewrite_tracker, "has_pending", None))
        and rewrite_tracker.has_pending()
    )
    if rewrite_pending:
        if guide_id != rewrite_tracker.guide_id:
            return _reject(
                guide_id,
                "memory_write_rewrite_pending",
                {
                    "attempted": {"guide_id": guide_id},
                    "current": {"guide_id": rewrite_tracker.guide_id},
                    "next_action": "先按当前记忆写入重写指南结清全部待办。",
                },
            )
        backend = apply_memory_write_rewrite_guide(
            arguments, evidence_context
        )
        receipt = _base_receipt(guide_id)
        receipt.update({
            "status": backend.get("status", "rejected"),
            "reason": backend.get("reason", ""),
            "action": "memory_write_rewrites_settled",
            "accepted_submissions": (
                [_normalize_submission(arguments)]
                if backend.get("status") == "applied" else []
            ),
            "backend_receipts": backend.get("backend_receipts") or [],
            "completed_ids": backend.get("completed_ids") or [],
            "remaining_ids": backend.get("remaining_ids") or [],
            "created_memory_ids": backend.get("created_memory_ids") or [],
            "not_written_ids": backend.get("not_written_ids") or [],
        })
        if receipt["status"] == "rejected":
            receipt["error_hint"] = _guide_error_hint(
                receipt["reason"],
                {
                    "current": {"guide_id": guide_id},
                    "expected": {
                        "guide_id": rewrite_tracker.guide_id,
                        "pending_ids": rewrite_tracker.pending_ids(),
                    },
                    "next_action": (
                        "按回执修正 action/semantic_content，并重新提交"
                        "仍待处理的全部 rewrite_id。"
                    ),
                },
            )
        return receipt
    if guide_id.startswith(f"{MEMORY_WRITE_REWRITE_GUIDE_PREFIX}:"):
        return _reject(
            guide_id,
            "memory_write_rewrite_guide_not_active",
            {"current": {"guide_id": ""}},
        )
    if guide_id == REACTION_LOOP_GUIDE_ID:
        return _apply_reaction_loop_resident_submit(
            workbench_store,
            arguments,
            evidence_context=evidence_context,
        )
    if hasattr(workbench_store, "current_active_guide_id"):
        active_guide_id = str(workbench_store.current_active_guide_id() or "").strip()
    else:
        active_guide_id = str(workbench_store.get("base.active_guide") or "").strip()
    if not guide_id or guide_id != active_guide_id:
        expected = _guide_coordinates(workbench_store, active_guide_id)
        return _reject(
            guide_id,
            "guide_not_active",
            {
                "active_guide": active_guide_id,
                "attempted": {"guide_id": guide_id},
                "current": {"guide_id": active_guide_id},
                "expected": expected,
                "next_action": "只使用当前 active guide_id 及其合法 item_id/option_id 重新提交。",
                "error_kind": "state_conflict",
                "retry": "after_correction",
            },
        )

    try:
        guide = workbench_store.load_guide(guide_id)
    except (ReadError, FileNotFoundError, ValueError):
        return _reject(guide_id, "guide_not_active", {"active_guide": active_guide_id})

    if str(guide.get("kind") or "").strip() == "cache_compaction_rhythm_guide":
        return _reject(guide_id, "retired_cache_compaction_guide")

    submissions = _coerce_guide_submissions(arguments)
    if not isinstance(submissions, list):
        return _reject(guide_id, "invalid_guide_submission", {"field": "submissions"})

    validation = _validate_submissions(guide, submissions)
    if validation.get("status") == "rejected":
        validation["tool_id"] = "guide_submit"
        validation["tool_family"] = "protocol_tool"
        validation["tool_class"] = "sync_tool"
        validation["protocol_tool_receipt"] = True
        validation["source"] = "guide_submit"
        validation["guide_id"] = guide_id
        workbench_store.append_guide_ledger(guide_id, {
            "event": "guide_submission_rejected",
            "reason": validation.get("reason"),
            "details": validation.get("details") or {},
        })
        return validation

    accepted = []
    task_id = None
    task_update = None
    pending_input_update = None
    backend_receipts = []
    completed_flags = []
    reopened_flags = []
    cache_compaction_status = None
    backend_applied = False
    for submission in submissions:
        normalized = _normalize_submission(submission)
        if (
                guide.get("kind") == "task_bootstrap"
                and normalized.get("item_id") == BOOTSTRAP_ITEM_ID
                and normalized.get("option_id") == "not_a_task"):
            _dismiss_task_bootstrap_guide(
                workbench_store,
                guide,
                guide_id,
                normalized.get("fields") or {},
            )
            _clear_work_intent_debt_if_present(evidence_context)
            accepted.append(normalized)
            workbench_store.append_guide_ledger(guide_id, {
                "event": "guide_submission_accepted",
                "item_id": normalized.get("item_id"),
                "option_id": normalized.get("option_id"),
                "fields": normalized.get("fields") or {},
                "evidence_refs": normalized.get("evidence_refs") or [],
                "reason": normalized.get("reason") or "",
            })
            receipt = _base_receipt(guide_id)
            receipt.update({
                "status": "accepted",
                "action": "task_bootstrap_dismissed",
                "accepted_submissions": accepted,
                "active_guide": None,
            })
            return receipt
        if (
                guide.get("kind") == "task_bootstrap"
                and normalized.get("item_id") == BOOTSTRAP_ITEM_ID
                and normalized.get("option_id") == BOOTSTRAP_SUBMIT_OPTION_ID):
            normalized = _adapt_task_bootstrap_submission(
                normalized,
                evidence_context,
            )
            bootstrap_validation = _validate_bootstrap_source_evidence(
                guide,
                normalized,
                evidence_context,
            )
            if bootstrap_validation.get("status") == "rejected":
                workbench_store.append_guide_ledger(guide_id, {
                    "event": "guide_submission_rejected",
                    "reason": bootstrap_validation.get("reason"),
                    "details": bootstrap_validation.get("details") or {},
                })
                return _rejection_receipt(
                    guide_id,
                    bootstrap_validation,
                    guide=guide,
                )
            task_id = materialize_initial_task_guide(
                workbench_store,
                normalized.get("fields") or {},
                evidence_refs=normalized.get("evidence_refs") or [],
                round_num=(evidence_context or {}).get("round_num"),
            )
            _mark_guide_completed(workbench_store, guide, guide_id)
            _clear_work_intent_debt_if_present(evidence_context)
        elif str(guide.get("kind") or "").strip() == "task_bootstrap":
            details = {
                "item_id": normalized.get("item_id"),
                "option_id": normalized.get("option_id"),
            }
            workbench_store.append_guide_ledger(guide_id, {
                "event": "guide_submission_rejected",
                "reason": "unsupported_task_bootstrap_submission",
                "details": details,
            })
            return _reject(
                guide_id,
                "unsupported_task_bootstrap_submission",
                details,
            )
        elif (
                guide.get("kind") == "task_execution"
                and normalized.get("item_id") == TASK_PROGRESS_ITEM_ID
                and normalized.get("option_id") == TASK_PROGRESS_UPDATE_OPTION_ID):
            pending_block = _pending_interaction_first_block(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
            )
            if pending_block:
                return _task_pending_first_receipt(
                    workbench_store,
                    guide_id,
                    pending_block,
                    status="rejected",
                )
            task_update = apply_task_status_update(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
                normalized.get("fields") or {},
                evidence_refs=normalized.get("evidence_refs") or [],
                evidence_context=evidence_context,
            )
            if task_update.get("status") == "rejected":
                workbench_store.append_guide_ledger(guide_id, {
                    "event": "guide_submission_rejected",
                    "reason": task_update.get("reason"),
                    "details": task_update.get("details") or {},
                })
                return _rejection_receipt(guide_id, task_update, guide=guide)
            refresh_task_execution_active_guide(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
            )

        elif (
                guide.get("kind") == "task_execution"
                and normalized.get("item_id") == TASK_PROGRESS_ITEM_ID
                and normalized.get("option_id") == TASK_PENDING_INPUT_OPTION_ID):
            pending_input_update = apply_pending_input_integration(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
                normalized.get("fields") or {},
                evidence_refs=normalized.get("evidence_refs") or [],
                evidence_context=evidence_context,
            )
            if pending_input_update.get("status") == "rejected":
                workbench_store.append_guide_ledger(guide_id, {
                    "event": "guide_submission_rejected",
                    "reason": pending_input_update.get("reason"),
                    "details": pending_input_update.get("details") or {},
                })
                return _rejection_receipt(
                    guide_id,
                    pending_input_update,
                    guide=guide,
                )
            refresh_task_execution_active_guide(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
            )

        elif str(guide.get("kind") or "").strip() == "task_execution":
            direct_fields = _direct_task_checklist_update_fields(
                guide,
                normalized,
            )
            if direct_fields is None:
                return _reject(
                    guide_id,
                    "unsupported_task_guide_submission",
                    {
                        "item_id": normalized.get("item_id"),
                        "option_id": normalized.get("option_id"),
                    },
                )
            pending_block = _pending_interaction_first_block(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
            )
            if pending_block:
                return _task_pending_first_receipt(
                    workbench_store,
                    guide_id,
                    pending_block,
                    status="rejected",
                )
            task_update = apply_task_status_update(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
                direct_fields,
                evidence_refs=normalized.get("evidence_refs") or [],
                evidence_context=evidence_context,
            )
            if task_update.get("status") == "rejected":
                workbench_store.append_guide_ledger(guide_id, {
                    "event": "guide_submission_rejected",
                    "reason": task_update.get("reason"),
                    "details": task_update.get("details") or {},
                })
                return _rejection_receipt(guide_id, task_update, guide=guide)
            refresh_task_execution_active_guide(
                workbench_store,
                str(guide.get("task_id") or "").strip(),
            )

        elif str(guide.get("kind") or "").strip() in RHYTHM_GUIDE_KINDS:
            backend = _apply_rhythm_guide_submission(
                guide,
                normalized,
                evidence_context=evidence_context,
                workbench_store=workbench_store,
            )
            if backend.get("status") == "rejected":
                workbench_store.append_guide_ledger(guide_id, {
                    "event": "guide_submission_rejected",
                    "reason": backend.get("reason"),
                    "details": backend.get("details") or {},
                })
                return _rejection_receipt(
                    guide_id,
                    backend,
                    guide=guide,
                    extra={
                        "backend_receipts": backend.get("backend_receipts") or [],
                    },
                )
            backend_receipts.extend(backend.get("backend_receipts") or [])
            completed_flags.extend(backend.get("completed_flags") or [])
            reopened_flags.extend(backend.get("reopened_flags") or [])
            if backend.get("cache_compaction"):
                cache_compaction_status = backend.get("cache_compaction")
            if backend.get("status") == "applied":
                backend_applied = True

        accepted.append(normalized)
        workbench_store.append_guide_ledger(guide_id, {
            "event": "guide_submission_accepted",
            "item_id": normalized.get("item_id"),
            "option_id": normalized.get("option_id"),
            "fields": normalized.get("fields") or {},
            "evidence_refs": normalized.get("evidence_refs") or [],
            "reason": normalized.get("reason") or "",
        })

    receipt = _base_receipt(guide_id)
    receipt.update({
        "status": "accepted",
        "accepted_submissions": accepted,
    })
    if backend_receipts:
        receipt["backend_receipts"] = backend_receipts
        receipt["completed_flags"] = _unique(completed_flags)
        if reopened_flags:
            receipt["reopened_flags"] = _unique(reopened_flags)
        if cache_compaction_status:
            receipt["cache_compaction"] = cache_compaction_status
        if backend_applied:
            receipt["status"] = "applied"
        if completed_flags:
            receipt["status"] = "applied"
            _mark_guide_items_completed(
                workbench_store,
                guide,
                guide_id,
                completed_flags,
            )
            if _guide_should_clear_after_completed_flags(
                    guide,
                    completed_flags,
                    evidence_context):
                _mark_guide_completed(workbench_store, guide, guide_id)
                receipt["active_guide"] = _clear_active_guide(workbench_store, guide_id)
    if task_id:
        receipt["task_id"] = task_id
        receipt["active_guide"] = f"task:{task_id}"
    if task_update:
        receipt["task_update"] = task_update
        task_acceptance = validate_task_closeout(
            workbench_store,
            {"closeout_decision": "finish"},
        )
        receipt["task_acceptance"] = task_acceptance
        if task_acceptance.get("allowed") is True:
            receipt["active_guide"] = _clear_active_guide(workbench_store, guide_id)
            receipt["task_completion"] = _complete_task_guide(
                workbench_store, guide, "task_update")
            receipt["next_action"] = "natural_final_reply"
    if pending_input_update:
        receipt["pending_input_update"] = pending_input_update
        task_acceptance = validate_task_closeout(
            workbench_store,
            {"closeout_decision": "finish"},
        )
        receipt["task_acceptance"] = task_acceptance
        if task_acceptance.get("allowed") is True:
            receipt["active_guide"] = _clear_active_guide(workbench_store, guide_id)
            receipt["task_completion"] = _complete_task_guide(
                workbench_store, guide, "pending_input_update")
            receipt["next_action"] = "natural_final_reply"
    return receipt


def _direct_task_checklist_update_fields(guide, submission):
    record_type = _task_checklist_record_type(
        guide,
        str(submission.get("item_id") or "").strip(),
    )
    if record_type not in {"item", "acceptance"}:
        return None
    status = str(submission.get("option_id") or "").strip().lower()
    fields = dict(submission.get("fields") or {})
    evidence_refs = _submission_evidence_refs(submission)
    reason = str(fields.get("reason") or submission.get("reason") or "").strip()
    if record_type == "item":
        record = {
            "item_id": str(submission.get("item_id") or "").strip(),
            "status": status,
        }
        if evidence_refs:
            record["evidence_refs"] = evidence_refs
        if reason:
            record["reason"] = reason
        return {"items": [record]}
    record = {
        "acceptance_id": str(submission.get("item_id") or "").strip(),
        "status": status,
    }
    if evidence_refs:
        record["evidence_refs"] = evidence_refs
    if reason:
        record["reason"] = reason
    return {"acceptance": [record]}


TASK_EXECUTION_DONE_ALIASES = {"complete", "completed", "finished"}


def _task_execution_option_alias(guide, item, option_id):
    if str((guide or {}).get("kind") or "").strip() != "task_execution":
        return str(option_id or "").strip()
    option = str(option_id or "").strip().lower()
    if option not in TASK_EXECUTION_DONE_ALIASES:
        return str(option_id or "").strip()
    record_type = str((item or {}).get("task_record_type") or "").strip()
    if record_type == "item":
        return "done"
    if record_type == "acceptance":
        return "passed"
    return str(option_id or "").strip()


def _task_checklist_record_type(guide, item_id):
    if not item_id:
        return ""
    for item in guide.get("items") or []:
        if not isinstance(item, dict):
            continue
        if str(item.get("item_id") or "").strip() != item_id:
            continue
        return str(item.get("task_record_type") or "").strip()
    return ""


def _submission_evidence_refs(submission):
    refs = []
    for value in (
            submission.get("evidence_refs"),
            (submission.get("fields") or {}).get("evidence_refs"),
    ):
        for item in _as_sequence(value):
            ref = str(item or "").strip()
            if ref and ref not in refs:
                refs.append(ref)
    return refs


def _apply_reaction_loop_resident_submit(
        workbench_store,
        arguments,
        evidence_context=None):
    guide_id = REACTION_LOOP_GUIDE_ID
    submissions = arguments.get("submissions")
    if not isinstance(submissions, list) or len(submissions) != 1:
        return _reject(
            guide_id,
            "invalid_guide_submission",
            {"field": "submissions", "expected_count": 1},
        )
    guide = reaction_loop_resident_guide()
    validation = _validate_submissions(guide, submissions)
    if validation.get("status") == "rejected":
        validation["tool_id"] = "guide_submit"
        validation["tool_family"] = "protocol_tool"
        validation["tool_class"] = "sync_tool"
        validation["protocol_tool_receipt"] = True
        validation["source"] = "guide_submit"
        validation["guide_id"] = guide_id
        _append_reaction_resident_ledger(
            workbench_store,
            "guide_submission_rejected",
            {
                "reason": validation.get("reason"),
                "details": validation.get("details") or {},
            },
        )
        return validation

    normalized = _normalize_submission(submissions[0])
    source_refs = _reaction_resident_source_refs(evidence_context)
    active_task = str(workbench_store.get("base.active_task") or "").strip()
    if active_task:
        if _context_round_type(evidence_context) == "relay":
            _append_reaction_resident_ledger(
                workbench_store,
                "guide_submission_rejected",
                {
                    "reason": "relay_task_guidance_not_pending_input",
                    "task_id": active_task,
                    "source_refs": source_refs,
                },
            )
            receipt = _reject(
                guide_id,
                "relay_task_guidance_not_pending_input",
                {
                    "task_id": active_task,
                    "next_action": "继续当前任务，不要在 relay 中重新请求任务指南。",
                },
            )
            receipt.update({
                "task_id": active_task,
                "next_action": "continue_current_task",
            })
            return receipt
        pending = append_task_pending_input(
            workbench_store,
            active_task,
            source_refs=source_refs,
            summary="反应循环请求任务指南。",
            input_kind="reaction_task_guidance",
            round_num=_context_round_num(evidence_context),
            task_guidance_route="current_work",
        )
        _append_reaction_resident_ledger(
            workbench_store,
            "guide_submission_accepted",
            {
                "item_id": normalized.get("item_id"),
                "option_id": normalized.get("option_id"),
                "action": "registered_pending_input",
                "task_id": active_task,
                "pending_input_id": pending.get("pending_input_id"),
                "source_refs": source_refs,
            },
        )
        receipt = _base_receipt(guide_id)
        receipt.update({
            "status": "applied",
            "action": "registered_pending_input",
            "task_id": active_task,
            "pending_input": pending,
            "next_action": "integrate_pending_input",
        })
        return receipt

    work_guide = _active_work_guide_id(workbench_store)
    if work_guide:
        _append_reaction_resident_ledger(
            workbench_store,
            "guide_submission_blocked",
            {
                "reason": "task_guidance_already_active",
                "work_guide": work_guide,
                "next_action": "submit_existing_task_bootstrap",
            },
        )
        receipt = _base_receipt(guide_id)
        receipt.update({
            "status": "blocked",
            "reason": "task_guidance_already_active",
            "work_guide": work_guide,
            "next_action": "submit_existing_task_bootstrap",
            "message": (
                "任务清单创建指南已经存在，不要再次请求 reaction_loop_guide。"
                f" 请直接使用当前工作指南：guide_id={work_guide}，"
                "item_id=build_initial_task_guide，option_id=submit_initial_guide。"
            ),
            "details": {
                "work_guide": work_guide,
                "next_action": "直接提交现有 task_bootstrap，不要重复创建指南。",
            },
            "error_hint": _guide_error_hint(
                "task_guidance_already_active",
                {
                    "work_guide": work_guide,
                    "next_action": "直接提交现有 task_bootstrap，不要重复创建指南。",
                },
            ),
        })
        return receipt

    guide = create_task_bootstrap_guide(
        workbench_store,
        reason="reaction_loop_task_guidance_requested",
        source_refs=source_refs,
    )
    _append_reaction_resident_ledger(
        workbench_store,
        "guide_submission_accepted",
        {
            "item_id": normalized.get("item_id"),
            "option_id": normalized.get("option_id"),
            "action": "created_task_bootstrap",
            "work_guide": guide.get("guide_id"),
            "source_refs": source_refs,
        },
    )
    receipt = _base_receipt(guide_id)
    current_active = (
        workbench_store.current_active_guide_id()
        if hasattr(workbench_store, "current_active_guide_id")
        else workbench_store.get("base.active_guide")
    )
    receipt.update({
        "status": "applied",
        "action": "created_task_bootstrap",
        "work_guide": guide.get("guide_id"),
        "active_guide": current_active,
        "next_action": "submit_initial_guide",
    })
    return receipt


def _active_work_guide_id(workbench_store):
    if hasattr(workbench_store, "active_guide_slots"):
        try:
            slots = workbench_store.active_guide_slots()
            return str((slots or {}).get("work") or "").strip()
        except Exception:
            return ""
    return str(workbench_store.get("base.active_guide") or "").strip()


def _context_round_num(evidence_context=None):
    context = evidence_context if isinstance(evidence_context, dict) else {}
    value = context.get("round_num")
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _context_round_type(evidence_context=None):
    context = evidence_context if isinstance(evidence_context, dict) else {}
    return str(context.get("round_type") or "").strip().lower()


def _reaction_resident_source_refs(evidence_context=None):
    round_num = _context_round_num(evidence_context)
    if round_num is None:
        return []
    return [f"round:{round_num}:reaction"]


def _append_reaction_resident_ledger(workbench_store, event, payload=None):
    try:
        workbench_store.append_guide_ledger(
            REACTION_LOOP_GUIDE_ID,
            {"event": event, **(payload or {})},
        )
    except Exception:
        pass


def _clear_work_intent_debt_if_present(evidence_context=None):
    context = evidence_context if isinstance(evidence_context, dict) else {}
    state_store = context.get("state_store")
    if state_store is None:
        return
    try:
        clear_work_intent_debt(state_store)
    except Exception:
        pass


def _pending_interaction_first_block(workbench_store, task_id):
    task_id = str(task_id or "").strip()
    if not task_id:
        return None
    try:
        if not has_open_pending_inputs(workbench_store, task_id):
            return None
        pending_ids = open_pending_input_ids(workbench_store, task_id)
    except Exception:
        return None
    return {
        "reason": "pending_interaction_first",
        "task_id": task_id,
        "blockers": ["pending_inputs"],
        "pending_input_ids": pending_ids,
        "next_option_id": TASK_PENDING_INPUT_OPTION_ID,
    }


def _guide_should_clear_after_completed_flags(guide, completed_flags, evidence_context=None):
    if str((guide or {}).get("kind") or "").strip() not in RHYTHM_GUIDE_KINDS:
        return True
    item_ids = _guide_item_ids(guide)
    if not item_ids:
        return True
    completed = _completed_flags_for_guide(completed_flags, evidence_context)
    return item_ids.issubset(completed)


def _guide_item_ids(guide):
    item_ids = set()
    for item in (guide or {}).get("items") or []:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("item_id") or "").strip()
        if item_id:
            item_ids.add(item_id)
    return item_ids


def _completed_flags_for_guide(completed_flags, evidence_context=None):
    completed = {
        str(flag or "").strip()
        for flag in (completed_flags or [])
        if str(flag or "").strip()
    }
    context = evidence_context if isinstance(evidence_context, dict) else {}
    completed.update(
        str(flag or "").strip()
        for flag in context.get("completed_flags") or []
        if str(flag or "").strip()
    )
    return completed


def _mark_guide_items_completed(workbench_store, guide, guide_id, completed_flags):
    completed = {
        str(flag or "").strip()
        for flag in completed_flags or []
        if str(flag or "").strip()
    }
    if not completed:
        return
    updated = dict(guide or {})
    items = []
    changed = False
    for item in updated.get("items") or []:
        if not isinstance(item, dict):
            items.append(item)
            continue
        next_item = dict(item)
        item_id = str(next_item.get("item_id") or "").strip()
        if item_id in completed and next_item.get("status") != "completed":
            next_item["status"] = "completed"
            changed = True
        items.append(next_item)
    if not changed:
        return
    updated["items"] = items
    try:
        workbench_store.save_guide(updated, active=False)
    except Exception:
        try:
            workbench_store.append_guide_ledger(guide_id, {
                "event": "guide_item_status_update_failed",
                "completed_flags": sorted(completed),
            })
        except Exception:
            pass


def _task_pending_first_receipt(workbench_store, guide_id, block, *, status):
    reason = str((block or {}).get("reason") or "pending_interaction_first")
    details = {
        "task_id": (block or {}).get("task_id"),
        "blockers": (block or {}).get("blockers") or [],
        "pending_input_ids": (block or {}).get("pending_input_ids") or [],
        "next_option_id": (block or {}).get("next_option_id"),
    }
    try:
        workbench_store.append_guide_ledger(guide_id, {
            "event": (
                "guide_submission_rejected"
                if status == "rejected"
                else "guide_submission_blocked"
            ),
            "reason": reason,
            "blockers": (block or {}).get("blockers") or [],
            "next_option_id": (block or {}).get("next_option_id"),
        })
    except Exception:
        pass
    receipt = _base_receipt(guide_id)
    receipt.update({
        "status": status,
        "reason": reason,
        "task_id": details["task_id"],
        "blockers": details["blockers"],
        "pending_input_ids": details["pending_input_ids"],
        "next_option_id": details["next_option_id"],
        "details": details,
        "error_hint": _guide_error_hint(reason, details),
    })
    return receipt


def _apply_rhythm_guide_submission(
        guide,
        submission,
        evidence_context=None,
        workbench_store=None):
    evidence_context = evidence_context if isinstance(evidence_context, dict) else {}
    kind = str((guide or {}).get("kind") or "").strip()
    item_id = str((submission or {}).get("item_id") or "").strip()
    option_id = str((submission or {}).get("option_id") or "").strip()
    fields = (submission or {}).get("fields") or {}
    if kind in {"main_axis_rhythm_guide", "calendar_rhythm_guide"}:
        if option_id != "write_chronicle":
            return {"status": "accepted", "backend_receipts": [], "completed_flags": []}
        receipts = apply_chronicle_write_declarations(
            [{
                "content": fields.get("content"),
                "reason": fields.get("reason") or submission.get("reason") or "",
            }],
            {
                "chronicle_store": evidence_context.get("chronicle_store"),
                "chronicle_focus": evidence_context.get("chronicle_focus"),
            },
        )
        if item_id == "calendar_day_due" and any(
                str(receipt.get("status") or "").strip().lower() == "applied"
                for receipt in receipts if isinstance(receipt, dict)):
            from data.memory_compression_store import MemoryCompressionManager
            from constants import local_now
            import hashlib
            import json

            manager = MemoryCompressionManager(
                memory_store=evidence_context.get("memory_store"),
            )
            chronicle_hash = hashlib.sha256(json.dumps(
                receipts, ensure_ascii=False, sort_keys=True,
                separators=(",", ":"), default=str,
            ).encode("utf-8")).hexdigest()
            maintenance = manager.prepare_daily_cycle(
                local_date=local_now().date().isoformat(),
                round_num=evidence_context.get("round_num"),
                chronicle_receipt_hash=chronicle_hash,
            )
            receipts.append({
                "tool_id": "memory_compression_daily_maintenance",
                **maintenance,
            })
            state_store = evidence_context.get("state_store")
            compression_active = manager.has_active_cycle()
            if state_store is not None:
                state_store.set_flag(
                    "memory_compression_due",
                    compression_active,
                )
            result = _backend_result(
                item_id,
                receipts,
                applied_tools={"chronicle_write"},
            )
            if compression_active:
                # A prior cycle may have completed earlier in this Round.
                # Reopen the flag for the newly created daily cycle.
                result["reopened_flags"] = ["memory_compression_due"]
            return result
        return _backend_result(item_id, receipts, applied_tools={"chronicle_write"})

    if kind in {"emergency_handling_guide", "context_pressure_rhythm_guide"}:
        if option_id == "settle_alert":
            alert_type = str(fields.get("alert_type") or item_id).strip()
            receipts = apply_alert_mode_settlement_declarations(
                [{
                    "alert_type": alert_type,
                    "status": fields.get("status"),
                    "summary": fields.get("summary"),
                    "clear_flags": fields.get("clear_flags") or [alert_type],
                    "fault_refs": fields.get("fault_refs") or [],
                    "next_attention": fields.get("next_attention") or "",
                    "reason": fields.get("reason") or submission.get("reason") or "",
                }],
                evidence_context.get("round_num"),
                {
                    "state_store": evidence_context.get("state_store"),
                    "alert_store": evidence_context.get("alert_store"),
                    "context_store": evidence_context.get("context_store"),
                },
                interaction_meta=evidence_context.get("interaction_meta"),
            )
            return _backend_result(item_id, receipts, applied_tools={"alert_mode_settle"})
        if option_id == "record_fault":
            receipts = apply_fault_record_declarations(
                [{
                    "fault_type": fields.get("fault_type"),
                    "severity": fields.get("severity"),
                    "step": fields.get("step"),
                    "source": fields.get("source"),
                    "detail": fields.get("detail"),
                    "action": fields.get("action") or "needs_review",
                    "related_tool_id": fields.get("related_tool_id") or "",
                }],
                evidence_context.get("round_num"),
                {
                    "alert_store": evidence_context.get("alert_store"),
                    "context_store": evidence_context.get("context_store"),
                },
                interaction_meta=evidence_context.get("interaction_meta"),
            )
            return _backend_result(item_id, receipts, applied_tools={"fault_record"})
        return {"status": "accepted", "backend_receipts": [], "completed_flags": []}

    if kind == "memory_compression_rhythm_guide":
        if option_id != "submit_memory_compressions":
            return _backend_reject("unsupported_rhythm_guide_option")
        from data.memory_compression_store import MemoryCompressionManager

        manager = MemoryCompressionManager(
            memory_store=evidence_context.get("memory_store"),
        )
        try:
            receipt = manager.apply_batch(
                fields.get("results"),
                expected_batch_id=str(fields.get("batch_id") or ""),
                round_num=evidence_context.get("round_num"),
            )
        except Exception as exc:
            return _backend_reject(
                str(exc) or "memory_compression_batch_rejected",
                {"error_type": type(exc).__name__},
            )
        remaining = manager.has_active_cycle()
        state_store = evidence_context.get("state_store")
        if state_store is not None:
            state_store.set_flag("memory_compression_due", remaining)
        return {
            "status": "applied",
            "backend_receipts": [receipt],
            "completed_flags": [] if remaining else ["memory_compression_due"],
        }

    return _backend_reject("unsupported_rhythm_guide_kind")


def _backend_result(item_id, receipts, *, applied_tools):
    """Aggregate deterministic rhythm backend receipts."""
    receipts = [receipt for receipt in receipts or [] if isinstance(receipt, dict)]
    applied = [
        receipt for receipt in receipts
        if str(receipt.get("tool_id") or "").strip() in applied_tools
        and str(receipt.get("status") or "").strip() == "applied"
    ]
    if applied:
        return {
            "status": "applied",
            "backend_receipts": receipts,
            "completed_flags": [item_id],
        }
    return {
        "status": "rejected",
        "reason": "rhythm_guide_backend_not_applied",
        "backend_receipts": receipts,
    }


def _backend_reject(reason, details=None):
    return {
        "status": "rejected",
        "reason": reason,
        "details": details or {},
        "backend_receipts": [],
    }


def _unique(values):
    return [
        item for item in dict.fromkeys(
            str(value or "").strip()
            for value in values or []
            if str(value or "").strip()
        )
    ]


def _validate_submissions(guide, submissions):
    item_map = {
        str(item.get("item_id") or ""): item
        for item in guide.get("items") or []
        if isinstance(item, dict)
    }
    for submission in submissions:
        if not isinstance(submission, dict):
            return _validation_reject("invalid_guide_submission")
        item_id = str(submission.get("item_id") or "").strip()
        option_id = str(submission.get("option_id") or "").strip()
        item = item_map.get(item_id)
        if not item:
            return _validation_reject(
                "guide_item_not_found",
                {"item_id": item_id},
                hint_details={
                    "attempted": {"item_id": item_id},
                    "expected": _guide_definition_coordinates(guide),
                },
            )
        canonical_option_id = _task_execution_option_alias(guide, item, option_id)
        if canonical_option_id != option_id:
            submission["option_id"] = canonical_option_id
            option_id = canonical_option_id
        option = _find_option(item, option_id)
        if not option and _legacy_acceptance_done_option(guide, item, option_id):
            option = {
                "option_id": "done",
                "required_fields": [],
                "allowed_fields": ["evidence_refs", "reason"],
            }
        if not option:
            return _validation_reject(
                "guide_option_not_found",
                {"item_id": item_id, "option_id": option_id},
                hint_details={
                    "attempted": {
                        "item_id": item_id,
                        "option_id": option_id,
                    },
                    "expected": {
                        "item_id": item_id,
                        "option_ids": [
                            str(candidate.get("option_id") or "").strip()
                            for candidate in item.get("options") or []
                            if isinstance(candidate, dict)
                            and str(candidate.get("option_id") or "").strip()
                        ],
                    },
                },
            )
        fields = submission.get("fields") or {}
        if not isinstance(fields, dict):
            return _validation_reject(
                "invalid_guide_submission",
                {"field": "fields"},
            )
        if (
                str(guide.get("kind") or "").strip() == "task_execution"
                and item_id == TASK_PROGRESS_ITEM_ID
                and option_id == TASK_PENDING_INPUT_OPTION_ID):
            fields = _adapt_pending_input_update_fields(fields)
            submission["fields"] = fields
        required_fields = [
            str(field)
            for field in option.get("required_fields") or []
            if str(field)
        ]
        reason = str(submission.get("reason") or "").strip()
        if "reason" in required_fields and "reason" not in fields and reason:
            fields["reason"] = reason
            submission["fields"] = fields
        if "allowed_fields" in option:
            raw_allowed_fields = option.get("allowed_fields") or []
        else:
            raw_allowed_fields = required_fields
        allowed_fields = [
            str(field)
            for field in raw_allowed_fields
            if str(field)
        ]
        missing = [
            field
            for field in required_fields
            if field not in fields or fields.get(field) in (None, "")
        ]
        undeclared = sorted(set(fields) - set(allowed_fields))
        if missing:
            details = {"fields": missing}
            hint_details = {
                "missing_fields": missing,
                "undeclared_fields": undeclared,
                "required_fields": required_fields,
                "allowed_fields": allowed_fields,
                "attempted": {"fields": sorted(fields)},
                "expected": {
                    "required_fields": required_fields,
                    "allowed_fields": allowed_fields,
                },
            }
            if (
                    str(guide.get("kind") or "").strip() == "task_bootstrap"
                    and item_id == BOOTSTRAP_ITEM_ID
                    and option_id == BOOTSTRAP_SUBMIT_OPTION_ID):
                details.update({
                    "message": (
                        "submit_initial_guide creates the initial task ledger in "
                        "one complete submission. Include task_title, items, "
                        "acceptance, and source_requirements when file/URL "
                        "sources are used; do not submit only source_refs or a "
                        "partial first batch."
                    ),
                    "required_shape": (
                        "fields={task_title, source_refs, source_requirements, "
                        "items, acceptance}"
                    ),
                })
            return _validation_reject(
                "missing_guide_fields",
                details,
                hint_details=hint_details,
            )
        if undeclared:
            return _validation_reject(
                "undeclared_guide_fields",
                {"fields": undeclared},
                hint_details={
                    "undeclared_fields": undeclared,
                    "allowed_fields": allowed_fields,
                    "attempted": {"fields": sorted(fields)},
                    "expected": {"allowed_fields": allowed_fields},
                },
            )
    return {"status": "accepted"}


def _coerce_guide_submissions(arguments):
    submissions = arguments.get("submissions")
    if isinstance(submissions, list):
        return submissions
    item_id = str(arguments.get("item_id") or "").strip()
    option_id = str(arguments.get("option_id") or "").strip()
    if not item_id or not option_id:
        return submissions
    fields = {}
    if isinstance(arguments.get("fields"), dict):
        fields.update(arguments.get("fields") or {})
    for key, value in arguments.items():
        if key in GUIDE_SUBMIT_RESERVED_ARGUMENT_KEYS:
            continue
        fields[key] = value
    submission = {
        "item_id": item_id,
        "option_id": option_id,
        "fields": fields,
    }
    if "evidence_refs" in arguments:
        submission["evidence_refs"] = arguments.get("evidence_refs")
    if "reason" in arguments:
        submission["reason"] = arguments.get("reason")
    return [submission]


def _find_option(item, option_id):
    for option in item.get("options") or []:
        if (
                isinstance(option, dict)
                and str(option.get("option_id") or "").strip() == option_id):
            return option
    return None


def _legacy_acceptance_done_option(guide, item, option_id):
    return (
        str((guide or {}).get("kind") or "").strip() == "task_execution"
        and str((item or {}).get("task_record_type") or "").strip() == "acceptance"
        and str(option_id or "").strip() == "done"
    )


def _normalize_submission(submission):
    return {
        "item_id": str(submission.get("item_id") or "").strip(),
        "option_id": str(submission.get("option_id") or "").strip(),
        "fields": dict(submission.get("fields") or {}),
        "evidence_refs": [
            str(item) for item in submission.get("evidence_refs") or [] if item
        ],
        "reason": str(submission.get("reason") or "").strip(),
    }


def _adapt_task_bootstrap_submission(submission, evidence_context=None):
    normalized = dict(submission or {})
    fields = dict(normalized.get("fields") or {})
    if "source_refs" in fields:
        fields["source_refs"] = _normalize_bootstrap_source_refs(
            fields.get("source_refs"),
            evidence_context,
        )
    if "source_requirements" in fields:
        fields["source_requirements"] = [
            _adapt_source_requirement_record(record, index, evidence_context)
            for index, record in enumerate(_as_sequence(fields.get("source_requirements")), start=1)
            if isinstance(record, dict)
        ]
    if "items" in fields:
        fields["items"] = [
            _adapt_task_item_record(record, index)
            for index, record in enumerate(_as_sequence(fields.get("items")), start=1)
        ]
    if "acceptance" in fields:
        fields["acceptance"] = [
            _adapt_acceptance_record(record, index)
            for index, record in enumerate(_as_sequence(fields.get("acceptance")), start=1)
        ]
    _apply_bootstrap_ref_defaults(fields, evidence_context)
    normalized["fields"] = fields
    return normalized


def _normalize_bootstrap_source_refs(value, evidence_context=None):
    refs = []
    for ref in _as_sequence(value):
        text = _strip_source_ref_annotation(str(ref or "").strip())
        if not text:
            continue
        resolved = _resolve_source_ref_alias(text, evidence_context)
        if resolved and resolved not in refs:
            refs.append(resolved)
        elif not resolved and text not in refs:
            refs.append(text)
    return refs


def _adapt_source_requirement_record(record, index, evidence_context=None):
    adapted = dict(record or {})
    requirement_id = str(
        adapted.get("requirement_id")
        or adapted.get("req_id")
        or adapted.get("id")
        or ""
    ).strip()
    if not requirement_id:
        requirement_id = f"req_{index:02d}"
    adapted["requirement_id"] = requirement_id
    for alias in ("req_id", "id"):
        adapted.pop(alias, None)
    source_ref = _strip_source_ref_annotation(
        str(adapted.get("source_ref") or "").strip()
    )
    if source_ref:
        adapted["source_ref"] = _resolve_source_ref_alias(
            source_ref,
            evidence_context,
        ) or source_ref
    summary = str(adapted.get("summary") or "").strip()
    if not summary:
        summary = str(
            adapted.get("title")
            or adapted.get("description")
            or adapted.get("text")
            or ""
        ).strip()
    if summary:
        adapted["summary"] = summary
    return adapted


def _strip_source_ref_annotation(value):
    text = str(value or "").strip()
    if not text:
        return ""
    cjk_pattern = re.compile(r"^(?P<body>.+?)[\s\u3000]*(?:（[^（）\n]{1,120}）)$")
    ascii_pattern = re.compile(r"^(?P<body>.+?)[\s\u3000]+(?:\([^()\n]{1,120}\))$")
    while True:
        match = cjk_pattern.match(text) or ascii_pattern.match(text)
        if not match:
            return text
        body = match.group("body").strip()
        if not _looks_like_source_ref(body):
            return text
        text = body


def _looks_like_source_ref(value):
    text = str(value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered.startswith(("http://", "https://")):
        return True
    if re.match(r"^[a-zA-Z]:[\\/]", text):
        return True
    if text.startswith(("\\\\", "/", ".")):
        return True
    if any(char in text for char in ("\\", "/", "*", "?")):
        return True
    return bool(re.search(r"\.[A-Za-z0-9]{1,8}(?:#.*)?$", text))


def _adapt_task_item_record(record, index):
    if not isinstance(record, dict):
        return record
    adapted = dict(record)
    item_id = str(adapted.get("item_id") or adapted.get("id") or "").strip()
    if not item_id:
        item_id = f"item_{index:02d}"
    adapted["item_id"] = item_id
    adapted.pop("id", None)
    title = str(adapted.get("title") or "").strip()
    if not title:
        title = str(
            adapted.get("summary")
            or adapted.get("description")
            or adapted.get("text")
            or adapted.get("name")
            or ""
        ).strip()
    if title:
        adapted["title"] = title
    return adapted


def _adapt_acceptance_record(record, index):
    if not isinstance(record, dict):
        return record
    adapted = dict(record)
    acceptance_id = str(
        adapted.get("acceptance_id")
        or adapted.get("acc_id")
        or adapted.get("id")
        or ""
    ).strip()
    if not acceptance_id:
        acceptance_id = f"acc_{index:02d}"
    adapted["acceptance_id"] = acceptance_id
    for alias in ("acc_id", "id"):
        adapted.pop(alias, None)
    description = str(adapted.get("description") or "").strip()
    if not description:
        description = str(
            adapted.get("summary")
            or adapted.get("title")
            or adapted.get("text")
            or adapted.get("target")
            or ""
        ).strip()
    if description:
        adapted["description"] = description
    return adapted


def _apply_bootstrap_ref_defaults(fields, evidence_context=None):
    if not isinstance(fields, dict):
        return
    _default_single_source_ref_to_requirements(fields)
    _default_task_definition_source_ref_to_requirements(fields, evidence_context)
    _default_one_to_one_item_requirement_refs(fields)
    _default_one_to_one_acceptance_item_refs(fields)


def _default_single_source_ref_to_requirements(fields):
    source_refs = [
        str(ref).strip()
        for ref in _as_sequence(fields.get("source_refs"))
        if str(ref).strip()
    ]
    requirements = fields.get("source_requirements")
    if len(source_refs) != 1 or not isinstance(requirements, list):
        return
    source_ref = source_refs[0]
    for requirement in requirements:
        if not isinstance(requirement, dict):
            continue
        if not str(requirement.get("source_ref") or "").strip():
            requirement["source_ref"] = source_ref


def _default_task_definition_source_ref_to_requirements(fields, evidence_context=None):
    source_refs = [
        str(ref).strip()
        for ref in _as_sequence(fields.get("source_refs"))
        if str(ref).strip()
    ]
    requirements = fields.get("source_requirements")
    if len(source_refs) <= 1 or not isinstance(requirements, list) or not requirements:
        return
    if any(
            isinstance(requirement, dict)
            and str(requirement.get("source_ref") or "").strip()
            for requirement in requirements):
        return
    candidate = _single_task_definition_source_ref(source_refs, evidence_context)
    if not candidate:
        return
    for requirement in requirements:
        if isinstance(requirement, dict):
            requirement["source_ref"] = candidate


def _single_task_definition_source_ref(source_refs, evidence_context=None):
    task_root = ""
    if isinstance(evidence_context, dict):
        task_root = str(evidence_context.get("task_root") or "").strip()
    candidates = []
    for ref in source_refs:
        text = str(ref or "").strip()
        if not text:
            continue
        path_part = _strip_ref_fragment(text).replace("\\", "/")
        basename = path_part.rsplit("/", 1)[-1].lower()
        if not basename:
            continue
        if not (
                "task" in basename
                or "requirement" in basename
                or "brief" in basename
                or "eval" in basename):
            continue
        if task_root:
            root = task_root.replace("\\", "/").rstrip("/")
            parent = path_part.rsplit("/", 1)[0] if "/" in path_part else ""
            if root and not _is_task_root_direct_parent(parent, root):
                continue
        candidates.append(text)
    unique = []
    for item in candidates:
        if item not in unique:
            unique.append(item)
    return unique[0] if len(unique) == 1 else ""


def _is_task_root_direct_parent(parent, root):
    parent = str(parent or "").replace("\\", "/").rstrip("/")
    root = str(root or "").replace("\\", "/").rstrip("/")
    if not root:
        return True
    if not parent:
        return True
    if parent == root:
        return True
    root_name = root.rsplit("/", 1)[-1]
    return bool(root_name and parent == root_name)


def _default_one_to_one_item_requirement_refs(fields):
    requirements = fields.get("source_requirements")
    items = fields.get("items")
    if (
            not isinstance(requirements, list)
            or not isinstance(items, list)
            or not requirements
            or len(requirements) != len(items)):
        return
    requirement_ids = []
    for index, requirement in enumerate(requirements, start=1):
        if not isinstance(requirement, dict):
            return
        requirement_id = str(
            requirement.get("requirement_id")
            or requirement.get("req_id")
            or requirement.get("id")
            or f"req_{index:02d}"
        ).strip()
        if not requirement_id:
            return
        requirement_ids.append(requirement_id)
    for item, requirement_id in zip(items, requirement_ids):
        if not isinstance(item, dict):
            continue
        if _submitted_requirement_refs(item):
            continue
        item["requirement_refs"] = [requirement_id]


def _default_one_to_one_acceptance_item_refs(fields):
    items = fields.get("items")
    acceptance = fields.get("acceptance")
    if (
            not isinstance(items, list)
            or not isinstance(acceptance, list)
            or not items
            or len(items) != len(acceptance)):
        return
    item_ids = []
    for index, item in enumerate(items, start=1):
        if not isinstance(item, dict):
            return
        item_id = str(item.get("item_id") or item.get("id") or f"item_{index:02d}").strip()
        if not item_id:
            return
        item_ids.append(item_id)
    for acceptance_item, item_id in zip(acceptance, item_ids):
        if not isinstance(acceptance_item, dict):
            continue
        if (
                _submitted_list_field(acceptance_item, "item_refs")
                or _submitted_requirement_refs(acceptance_item)):
            continue
        acceptance_item["item_refs"] = [item_id]


def _validate_bootstrap_source_evidence(guide, submission, evidence_context):
    context = evidence_context if isinstance(evidence_context, dict) else {}
    current_tool_ids = _current_general_tool_ids(context)
    if current_tool_ids:
        return _validation_reject(
            "bootstrap_wait_for_tool_results",
            {
                "current_tool_ids": current_tool_ids,
                "message": (
                    "submit_initial_guide must be based on prior visible "
                    "tool results; do not submit it in the same response as "
                    "read, search, shell, or write tools."
                ),
            },
        )

    source_refs = _bootstrap_source_refs(guide, submission)
    refs_requiring_read = [
        ref for ref in source_refs if _source_ref_requires_prior_evidence(ref)
    ]
    if not refs_requiring_read:
        return {"status": "accepted"}
    prior_refs = _prior_source_evidence_refs(context)
    missing_details = [
        detail for ref in refs_requiring_read
        for detail in [_missing_source_ref_detail(ref, prior_refs, context)]
        if detail is not None
    ]
    missing = [item["source_ref"] for item in missing_details]
    if missing:
        unread_glob_matches = []
        for detail in missing_details:
            unread_glob_matches.extend(detail.get("unread_glob_matches") or [])
        return _validation_reject(
            "bootstrap_source_not_read",
            {
                "missing_source_refs": missing,
                **(
                    {"unread_glob_matches": unread_glob_matches}
                    if unread_glob_matches else {}
                ),
                "prior_source_refs": sorted(prior_refs),
                "message": (
                    "File or URL source_refs in submit_initial_guide must "
                    "already be visible from a previous successful read result. "
                    "For a local path, call file_read first; for a URL, call "
                    "web_fetch first. After the result is visible, submit "
                    "submit_initial_guide."
                ),
                "repair_hint": (
                    "先对 missing_source_refs 调用 file_read/web_fetch；等待读取回执"
                    "进入下一次反应后，再用已读来源提交 submit_initial_guide。"
                ),
            },
        )
    coverage = _validate_bootstrap_source_requirement_coverage(
        submission,
        refs_requiring_read,
        context,
    )
    if coverage.get("status") == "rejected":
        return coverage
    return {"status": "accepted"}


def _current_general_tool_ids(context):
    values = []
    for request in context.get("current_general_tool_requests") or []:
        if not isinstance(request, dict):
            continue
        tool_id = str(request.get("tool_id") or "").strip()
        if tool_id and tool_id not in values:
            values.append(tool_id)
    return values


def _bootstrap_source_refs(guide, submission):
    refs = []
    fields = submission.get("fields") or {}
    for value in (
            fields.get("source_refs"),
            submission.get("evidence_refs"),
            guide.get("source_refs") if isinstance(guide, dict) else None,
    ):
        refs.extend(str(item).strip() for item in _as_sequence(value) if str(item).strip())
    result = []
    for ref in refs:
        if ref not in result:
            result.append(ref)
    return result


def _as_sequence(value):
    if value is None:
        return []
    if isinstance(value, list):
        return value
    return [value]


def _missing_source_ref_detail(ref, prior_refs, evidence_context=None):
    if _source_ref_satisfied(ref, prior_refs, evidence_context):
        return None
    detail = {"source_ref": str(ref or "").strip()}
    glob_status = _glob_source_ref_status(ref, prior_refs, evidence_context)
    if glob_status.get("is_glob") and glob_status.get("unread_glob_matches"):
        detail["unread_glob_matches"] = glob_status.get("unread_glob_matches")
    return detail


def _source_ref_requires_prior_evidence(ref):
    text = str(ref or "").strip()
    lowered = text.lower()
    if lowered == "interaction_debt":
        return False
    if lowered.startswith(("round:", "task_root:", "calendar:")):
        return False
    if lowered.startswith(("http://", "https://")):
        return True
    if re.match(r"^[a-zA-Z]:[\\/]", text):
        return True
    if text.startswith(("\\\\", "/")):
        return True
    if _source_ref_has_glob(text):
        return True
    if re.search(r"\.[A-Za-z0-9]{1,8}$", _strip_ref_fragment(text)):
        return True
    return "\\" in text or "/" in text


def _validate_bootstrap_source_requirement_coverage(
        submission,
        refs_requiring_read,
        evidence_context=None):
    fields = submission.get("fields") or {}
    requirements = fields.get("source_requirements")
    if not isinstance(requirements, list) or not requirements:
        return _validation_reject(
            "bootstrap_source_requirements_required",
            {
                "source_refs": refs_requiring_read,
                "message": (
                    "File or URL source_refs require source_requirements. "
                    "The model must state the requirements it understood from "
                    "the source before creating task items and acceptance."
                ),
                "repair_hint": (
                    "补 source_requirements=[{requirement_id, source_ref, summary}]；"
                    "source_ref 必须指向已读 source_refs。"
                ),
            },
        )

    source_keys = _declared_source_refs_for_coverage(
        refs_requiring_read,
        evidence_context,
    )
    requirement_ids = []
    invalid_requirements = []
    unknown_source_refs = []
    for index, requirement in enumerate(requirements, start=1):
        if not isinstance(requirement, dict):
            invalid_requirements.append(_invalid_requirement_detail(
                f"req_{index:02d}",
                index,
                ["object"],
            ))
            continue
        requirement_id = str(
            requirement.get("requirement_id")
            or requirement.get("req_id")
            or requirement.get("id")
            or ""
        ).strip()
        source_ref = str(requirement.get("source_ref") or "").strip()
        summary = str(
            requirement.get("summary")
            or requirement.get("title")
            or requirement.get("description")
            or requirement.get("text")
            or ""
        ).strip()
        missing_fields = []
        if not requirement_id:
            missing_fields.append("requirement_id")
        if not source_ref:
            missing_fields.append("source_ref")
        if not summary:
            missing_fields.append("summary")
        if missing_fields:
            invalid_requirements.append(_invalid_requirement_detail(
                requirement_id or f"req_{index:02d}",
                index,
                missing_fields,
            ))
            continue
        if requirement_id in requirement_ids:
            invalid_requirements.append(_invalid_requirement_detail(
                requirement_id,
                index,
                ["unique_requirement_id"],
            ))
            continue
        if not _source_ref_satisfied(source_ref, source_keys):
            unknown_source_refs.append(source_ref)
            continue
        requirement_ids.append(requirement_id)
    if invalid_requirements:
        return _validation_reject(
            "bootstrap_invalid_source_requirements",
            {
                "invalid_requirements": invalid_requirements,
                "message": (
                    "Each source requirement needs a stable id, source_ref, "
                    "and Chinese natural-language summary/title/description."
                ),
                "repair_hint": (
                    "逐条补 source_requirements=[{requirement_id, source_ref, summary}]；"
                    "source_ref 用已读来源，summary/title/description 用中文自然语言。"
                ),
            },
        )
    if unknown_source_refs:
        example_ref = next(iter(sorted(source_keys)), "")
        return _validation_reject(
            "bootstrap_source_requirement_ref_unknown",
            {
                "unknown_source_refs": unknown_source_refs,
                "source_refs": refs_requiring_read,
                "prior_source_refs": sorted(
                    _prior_source_evidence_refs(evidence_context or {})
                ),
                "corrected_example": {
                    "source_requirements": [{
                        "requirement_id": "req_01",
                        "source_ref": example_ref,
                        "summary": "概括该已读来源中与任务有关的要求",
                    }],
                },
                "next_action": (
                    "只引用已读来源；目录声明可由其已读后代文件满足，"
                    "然后完整重提 bootstrap。"
                ),
            },
        )
    items = _as_sequence(fields.get("items"))
    item_ids = []
    requirement_covered_by_items = set()
    items_missing_requirement_refs = []
    unknown_requirement_refs = []
    for index, item in enumerate(items, start=1):
        item_id = _submitted_item_id(item, index)
        item_ids.append(item_id)
        refs = _submitted_requirement_refs(item)
        if not refs:
            items_missing_requirement_refs.append(item_id)
            continue
        for ref in refs:
            if ref in requirement_ids:
                requirement_covered_by_items.add(ref)
            elif ref not in unknown_requirement_refs:
                unknown_requirement_refs.append(ref)
    if items_missing_requirement_refs:
        return _validation_reject(
            "bootstrap_item_requirement_refs_required",
            {"items": items_missing_requirement_refs},
            hint_details={
                "known_requirements": requirement_ids,
            },
        )
    if unknown_requirement_refs:
        return _validation_reject(
            "bootstrap_unknown_requirement_refs",
            {
                "requirement_refs": unknown_requirement_refs,
                "known_requirements": requirement_ids,
            },
        )
    missing_requirement_coverage = [
        requirement_id for requirement_id in requirement_ids
        if requirement_id not in requirement_covered_by_items
    ]
    if missing_requirement_coverage:
        return _validation_reject(
            "bootstrap_source_requirement_coverage_missing",
            {"requirement_ids": missing_requirement_coverage},
        )

    acceptance = _as_sequence(fields.get("acceptance"))
    acceptance_missing_refs = []
    unknown_acceptance_item_refs = []
    unknown_acceptance_requirement_refs = []
    item_covered_by_acceptance = set()
    for index, entry in enumerate(acceptance, start=1):
        acc_id = _submitted_acceptance_id(entry, index)
        item_refs = _submitted_list_field(entry, "item_refs")
        requirement_refs = _submitted_requirement_refs(entry)
        if not item_refs and not requirement_refs:
            acceptance_missing_refs.append(acc_id)
            continue
        for ref in item_refs:
            if ref in item_ids:
                item_covered_by_acceptance.add(ref)
            elif ref not in unknown_acceptance_item_refs:
                unknown_acceptance_item_refs.append(ref)
        for ref in requirement_refs:
            if ref not in requirement_ids and ref not in unknown_acceptance_requirement_refs:
                unknown_acceptance_requirement_refs.append(ref)
    if acceptance_missing_refs:
        return _validation_reject(
            "bootstrap_acceptance_refs_required",
            {"acceptance": acceptance_missing_refs},
            hint_details={
                "known_items": item_ids,
                "known_requirements": requirement_ids,
            },
        )
    if unknown_acceptance_item_refs or unknown_acceptance_requirement_refs:
        return _validation_reject(
            "bootstrap_acceptance_refs_unknown",
            {
                "item_refs": unknown_acceptance_item_refs,
                "requirement_refs": unknown_acceptance_requirement_refs,
            },
            hint_details={
                "known_items": item_ids,
                "known_requirements": requirement_ids,
            },
        )
    missing_item_acceptance = [
        item_id for item_id in item_ids
        if item_id not in item_covered_by_acceptance
    ]
    if missing_item_acceptance:
        return _validation_reject(
            "bootstrap_item_acceptance_coverage_missing",
            {"items": missing_item_acceptance},
        )
    return {"status": "accepted"}


def _declared_source_refs_for_coverage(refs_requiring_read, evidence_context=None):
    source_keys = {
        _source_ref_key(ref)
        for ref in refs_requiring_read
        if _source_ref_key(ref)
    }
    prior_refs = _prior_source_evidence_refs(evidence_context or {})
    for ref in refs_requiring_read or []:
        key = _source_ref_key(ref)
        if _source_ref_is_directory(ref, evidence_context):
            source_keys.update(
                prior for prior in prior_refs if _path_is_under_root(prior, key)
            )
    for ref in refs_requiring_read or []:
        if not _source_ref_has_glob(ref):
            continue
        for candidate in _glob_source_candidate_paths(ref, evidence_context):
            key = _source_ref_key(candidate)
            if key:
                source_keys.add(key)
    return source_keys


def _submitted_item_id(item, index):
    if isinstance(item, dict):
        item_id = str(item.get("item_id") or item.get("id") or "").strip()
        if item_id:
            return item_id
    return f"item_{index:02d}"


def _submitted_acceptance_id(item, index):
    if isinstance(item, dict):
        acceptance_id = str(
            item.get("acceptance_id")
            or item.get("acc_id")
            or item.get("id")
            or ""
        ).strip()
        if acceptance_id:
            return acceptance_id
    return f"acc_{index:02d}"


def _invalid_requirement_detail(requirement_id, index, missing_fields):
    missing = [str(item) for item in missing_fields or [] if str(item).strip()]
    return {
        "requirement_id": str(requirement_id or f"req_{index:02d}").strip(),
        "index": index,
        "missing_fields": missing,
        "repair_hint": _requirement_repair_hint(missing),
    }


def _requirement_repair_hint(missing_fields):
    if "summary" in (missing_fields or []):
        return "为该来源需求补充中文自然语言 summary/title/description。"
    if "source_ref" in (missing_fields or []):
        return "为该来源需求补充已读取来源的 source_ref。"
    if "requirement_id" in (missing_fields or []):
        return "为该来源需求补充稳定 ID，例如 req_01。"
    if "unique_requirement_id" in (missing_fields or []):
        return "为该来源需求换一个不重复的稳定 ID。"
    return "按 task_bootstrap 提示补齐该来源需求。"


def _submitted_requirement_refs(item):
    refs = _submitted_list_field(item, "requirement_refs")
    if refs:
        return refs
    return _submitted_list_field(item, "source_requirement_refs")


def _submitted_list_field(item, field):
    if not isinstance(item, dict):
        return []
    return [
        str(value).strip()
        for value in _as_sequence(item.get(field))
        if str(value).strip()
    ]


def _source_ref_key(ref):
    text = str(ref or "").strip()
    if text.lower().startswith(("http://", "https://")):
        return _canonical_url_ref(text)
    return _canonical_file_ref(text)


def _prior_source_evidence_refs(context):
    refs = set()
    for result in context.get("prior_general_tool_results") or []:
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        if status not in {"ok", "success", "accepted", "applied"}:
            continue
        tool_id = str(result.get("tool_id") or "").strip()
        if tool_id == "file_read":
            path = str(result.get("path") or "").strip()
            key = _canonical_file_ref(path)
            if key:
                refs.add(key)
        elif tool_id == "web_fetch":
            url = str(result.get("url") or "").strip()
            key = _canonical_url_ref(url)
            if key:
                refs.add(key)
    refs.update(_workbench_source_evidence_refs(context))
    return refs


def _workbench_source_evidence_refs(context):
    if not isinstance(context, dict):
        return set()
    workbench_store = context.get("workbench_store")
    if not hasattr(workbench_store, "load_source_read_evidence"):
        return set()
    try:
        entries = workbench_store.load_source_read_evidence()
    except Exception:
        return set()
    refs = set()
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        status = str(entry.get("status") or "").strip().lower()
        if status not in {"ok", "success", "accepted", "applied"}:
            continue
        tool_id = str(entry.get("tool_id") or "").strip()
        if tool_id == "file_read":
            value = (
                entry.get("path")
                or entry.get("file_path")
                or entry.get("source_ref")
            )
            key = _canonical_file_ref(value)
        elif tool_id == "web_fetch":
            value = (
                entry.get("url")
                or entry.get("source_url")
                or entry.get("source_ref")
            )
            key = _canonical_url_ref(value)
        else:
            key = ""
        if key and _source_evidence_matches_task_root(entry, key, tool_id, context):
            refs.add(key)
    return refs


def _source_evidence_matches_task_root(entry, key, tool_id, context):
    task_root = _canonical_file_ref((context or {}).get("task_root"))
    if not task_root:
        return True
    entry_root = _canonical_file_ref((entry or {}).get("task_root"))
    if entry_root:
        return entry_root == task_root
    if tool_id != "file_read":
        return True
    if not os.path.isabs(key):
        return False
    return _path_is_under_root(key, task_root)


def _path_is_under_root(path, root):
    try:
        path = _canonical_file_ref(path)
        root = _canonical_file_ref(root)
        return os.path.commonpath([path, root]) == root
    except (OSError, ValueError):
        return False


def _source_ref_satisfied(ref, prior_refs, evidence_context=None):
    glob_status = _glob_source_ref_status(ref, prior_refs, evidence_context)
    if glob_status.get("is_glob"):
        return bool(glob_status.get("satisfied"))
    key = _source_ref_key(ref)
    if not key:
        return False
    if key in prior_refs:
        return True
    if str(ref or "").strip().lower().startswith(("http://", "https://")):
        return False
    if _source_ref_is_directory(ref, evidence_context):
        return any(_path_is_under_root(prior, key) for prior in prior_refs)
    if os.path.isabs(key):
        return False
    basename = os.path.basename(key)
    if not basename:
        return False
    basename_matches = [
        prior for prior in prior_refs
        if os.path.basename(prior) == basename
    ]
    if len(basename_matches) == 1:
        return True
    return _relative_source_path_suffix_known(key, prior_refs)


def _source_ref_is_directory(ref, evidence_context=None):
    key = _source_ref_key(ref)
    if not key or str(ref or "").strip().lower().startswith(("http://", "https://")):
        return False
    task_root = _canonical_file_ref((evidence_context or {}).get("task_root"))
    return (
        key == task_root
        or str(ref or "").rstrip().endswith(("/", "\\"))
        or os.path.isdir(key)
    )


def _resolve_source_ref_alias(ref, evidence_context=None):
    text = str(ref or "").strip()
    if not text:
        return ""
    # Keep the model-authored visible ref stable. This function exists as the
    # adapter boundary: it verifies common aliases are acceptable without
    # rewriting task content or inventing source semantics.
    return text


def _relative_source_path_suffix_known(canonical_path, prior_refs):
    return any(
        _relative_source_path_suffix_matches(canonical_path, prior)
        for prior in prior_refs or []
    )


def _relative_source_path_suffix_matches(canonical_path, prior):
    if not canonical_path or os.path.isabs(canonical_path):
        return False
    if "\\" not in canonical_path and "/" not in canonical_path:
        return False
    prior_path = _canonical_file_ref(prior)
    if not prior_path or not os.path.isabs(prior_path):
        return False
    suffix = os.sep + canonical_path.strip("\\/")
    if prior_path.endswith(suffix):
        return True
    parts = canonical_path.strip("\\/").split(os.sep)
    if len(parts) > 1:
        trimmed = os.sep.join(parts[1:])
        return prior_path.endswith(os.sep + trimmed)
    return False


def _glob_source_ref_status(ref, prior_refs, evidence_context=None):
    text = str(ref or "").strip()
    if not _source_ref_has_glob(text):
        return {"is_glob": False, "satisfied": False}
    candidates = _glob_source_candidate_paths(text, evidence_context)
    if candidates:
        unread = [
            candidate for candidate in candidates
            if not _source_ref_satisfied(candidate, prior_refs)
        ]
        return {
            "is_glob": True,
            "satisfied": not unread,
            "unread_glob_matches": unread,
        }
    patterns = _glob_source_patterns(text, evidence_context)
    matched_prior = []
    for prior in prior_refs or []:
        for pattern in patterns:
            if fnmatch.fnmatch(prior, pattern):
                matched_prior.append(prior)
                break
    return {
        "is_glob": True,
        "satisfied": bool(matched_prior),
        "unread_glob_matches": [],
    }


def _glob_source_candidate_paths(ref, evidence_context=None):
    candidates = []
    known = set()
    for pattern in _glob_source_patterns(ref, evidence_context):
        for match in glob.glob(pattern):
            try:
                path = Path(match).resolve()
            except (OSError, RuntimeError, ValueError):
                continue
            if not path.is_file():
                continue
            canonical = _canonical_file_ref(str(path))
            if canonical and canonical not in known:
                known.add(canonical)
                candidates.append(str(path))
    return candidates


def _glob_source_patterns(ref, evidence_context=None):
    text = _strip_ref_fragment(str(ref or "").strip())
    if not text:
        return []
    raw = text.replace("\\", os.sep).replace("/", os.sep)
    patterns = []
    if os.path.isabs(raw) or re.match(r"^[a-zA-Z]:[\\/]", text):
        patterns.append(_canonical_file_ref(raw))
    else:
        context = evidence_context if isinstance(evidence_context, dict) else {}
        task_root_value = str(context.get("task_root") or "").strip()
        task_root = None
        if task_root_value:
            try:
                task_root = Path(task_root_value).resolve()
            except (OSError, RuntimeError, ValueError):
                task_root = None
        roots = []
        if task_root is not None:
            roots.extend([task_root, task_root.parent])
        if not roots:
            patterns.append(_canonical_file_ref(raw))
        for root in roots:
            patterns.append(_canonical_file_ref(str(root / raw)))
    result = []
    for pattern in patterns:
        if pattern and pattern not in result:
            result.append(pattern)
    return result


def _source_ref_has_glob(value):
    text = _strip_ref_fragment(str(value or "").strip())
    return any(char in text for char in ("*", "?"))


def _canonical_url_ref(value):
    text = str(value or "").strip()
    if not text.lower().startswith(("http://", "https://")):
        return ""
    return _strip_ref_fragment(text).rstrip("/").lower()


def _canonical_file_ref(value):
    text = _strip_ref_fragment(str(value or "").strip())
    if not text:
        return ""
    text = re.sub(r":\d+(?:-\d+)?$", "", text)
    return os.path.normcase(os.path.normpath(text))


def _strip_ref_fragment(value):
    text = str(value or "").strip()
    if "#" not in text:
        return text
    return text.split("#", 1)[0].strip()


def _base_receipt(guide_id):
    return {
        "tool_id": "guide_submit",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "source": "guide_submit",
        "protocol_tool_receipt": True,
        "guide_id": guide_id,
    }


def _clear_active_guide(workbench_store, guide_id):
    if hasattr(workbench_store, "clear_active_guide"):
        return workbench_store.clear_active_guide(guide_id)
    workbench_store.set("base.active_guide", None)
    return None


def _mark_guide_completed(workbench_store, guide, guide_id):
    guide_id = str(guide_id or "").strip()
    if not guide_id:
        return
    updated = dict(guide or {})
    try:
        updated = workbench_store.load_guide(guide_id)
    except Exception:
        pass
    updated = dict(updated or {})
    updated["status"] = "completed"
    try:
        workbench_store.save_guide(updated, active=False)
    except Exception:
        pass
    try:
        workbench_store.append_guide_ledger(guide_id, {
            "event": "guide_completed",
            "status": "completed",
        })
    except Exception:
        pass


def _dismiss_task_bootstrap_guide(workbench_store, guide, guide_id, fields=None):
    guide_id = str(guide_id or "").strip()
    if not guide_id:
        return
    updated = dict(guide or {})
    try:
        updated = workbench_store.load_guide(guide_id)
    except Exception:
        pass
    updated = dict(updated or {})
    updated["status"] = "dismissed"
    reason = str((fields or {}).get("reason") or "").strip()
    if reason:
        updated["dismiss_reason"] = reason
    try:
        workbench_store.save_guide(updated, active=False)
    except Exception:
        pass
    try:
        workbench_store.append_guide_ledger(guide_id, {
            "event": "task_bootstrap_dismissed",
            "status": "dismissed",
            "reason": reason,
        })
    except Exception:
        pass
    _clear_active_guide(workbench_store, guide_id)


def _complete_task_guide(workbench_store, guide, reason):
    task_id = str((guide or {}).get("task_id") or "").strip()
    if not task_id or not hasattr(workbench_store, "complete_task_guide_task"):
        return {}
    _mark_guide_completed(workbench_store, guide, f"task:{task_id}")
    manifest = workbench_store.complete_task_guide_task(
        task_id,
        result=f"Task guide accepted via {reason}.",
        guide_id=f"task:{task_id}",
    )
    return {
        "status": "completed",
        "task_id": task_id,
        "zone": manifest.get("status"),
        "reason": reason,
    }


def _rejection_receipt(guide_id, result, *, guide=None, extra=None):
    result = result if isinstance(result, dict) else {}
    details = dict(result.get("details") or {})
    hint_details = dict(details)
    if isinstance(guide, dict):
        hint_details.setdefault("legal_coordinates", _guide_definition_coordinates(guide))
        hint_details.setdefault("known_pending_input_ids", [
            str(item.get("pending_input_id") or "").strip()
            for item in guide.get("pending_inputs") or []
            if isinstance(item, dict)
            and str(item.get("pending_input_id") or "").strip()
        ])
        hint_details.setdefault("known_item_ids", [
            str(item.get("item_id") or "").strip()
            for item in guide.get("items") or []
            if isinstance(item, dict)
            and str(item.get("task_record_type") or "").strip() == "item"
            and str(item.get("item_id") or "").strip()
        ])
        hint_details.setdefault("known_acceptance_ids", [
            str(item.get("item_id") or "").strip()
            for item in guide.get("items") or []
            if isinstance(item, dict)
            and str(item.get("task_record_type") or "").strip() == "acceptance"
            and str(item.get("item_id") or "").strip()
        ])
    reason = str(result.get("reason") or "guide_submission_rejected").strip()
    receipt = _base_receipt(guide_id)
    receipt.update({
        "status": str(result.get("status") or "rejected").strip(),
        "reason": reason,
        "details": details,
        "error_hint": (
            result.get("error_hint")
            if isinstance(result.get("error_hint"), dict)
            else _guide_error_hint(reason, hint_details)
        ),
    })
    receipt.update(extra or {})
    return receipt


def _reject(guide_id, reason, details=None):
    details = details or {}
    receipt = _base_receipt(guide_id)
    receipt.update({
        "status": "rejected",
        "reason": reason,
        "details": details,
        "error_hint": _guide_error_hint(reason, details),
    })
    return receipt


def _guide_coordinates(workbench_store, guide_id):
    if not guide_id:
        return []
    try:
        guide = workbench_store.load_guide(guide_id)
    except (ReadError, FileNotFoundError, ValueError):
        return []
    return _guide_definition_coordinates(guide)


def _guide_definition_coordinates(guide):
    return [
        {
            "item_id": str(item.get("item_id") or item.get("id") or ""),
            "option_ids": [str(option.get("option_id") or option.get("id") or "")
                           for option in item.get("options") or []],
        }
        for item in guide.get("items") or []
        if isinstance(item, dict)
    ]


def _validation_reject(reason, details=None, *, hint_details=None):
    details = details or {}
    hint_source = {**details, **(hint_details or {})}
    return {
        "status": "rejected",
        "reason": reason,
        "details": details,
        "error_hint": _guide_error_hint(reason, hint_source),
    }


def _guide_error_hint(reason, details=None):
    """Return the single model-action contract for a handled guide rejection."""
    reason = str(reason or "").strip()
    details = details if isinstance(details, dict) else {}
    kind = str(details.get("error_kind") or "validation").strip()
    retry = str(details.get("retry") or "after_correction").strip()
    attempted = details.get("attempted") or {}
    current = details.get("current") or {}
    expected = details.get("expected") or {}
    next_action = str(details.get("next_action") or "").strip()

    if reason == "guide_not_active":
        kind = "state_conflict"
        attempted = attempted or {"guide_id": details.get("guide_id", "")}
        current = current or {"guide_id": details.get("active_guide", "")}
        expected = expected or details.get("legal_coordinates") or []
        next_action = next_action or "只使用当前 active guide_id 及其合法 item_id/option_id 重新提交。"
    elif reason == "guide_item_not_found":
        attempted = attempted or {"item_id": details.get("item_id", "")}
        next_action = "从 expected 中选择合法 item_id，再完整重提本次提交。"
    elif reason == "guide_option_not_found":
        attempted = attempted or {
            "item_id": details.get("item_id", ""),
            "option_id": details.get("option_id", ""),
        }
        next_action = "保留 item_id，并从 expected.option_ids 中选择合法 option_id 后重提。"
    elif reason == "invalid_guide_submission":
        attempted = attempted or {
            "field": details.get("field", "submission"),
        }
        expected = expected or {
            "submission": "object",
            **(
                {"expected_count": details.get("expected_count")}
                if details.get("expected_count") is not None else {}
            ),
        }
        next_action = "按 expected 的对象形状重新构造提交，不要原样重试。"
    elif reason == "missing_guide_fields":
        attempted = attempted or {"fields": details.get("fields") or []}
        expected = expected or {
            "required_fields": details.get("required_fields") or details.get("fields") or [],
            "allowed_fields": details.get("allowed_fields") or [],
        }
        undeclared = details.get("undeclared_fields") or []
        next_action = (
            "一次补齐 missing_fields，并删除 undeclared_fields；reason/evidence_refs 应放在 submission 外层。"
            if undeclared else
            "一次补齐 missing_fields 后完整重提；reason/evidence_refs 应放在 submission 外层。"
        )
    elif reason == "undeclared_guide_fields":
        attempted = attempted or {
            "fields": details.get("undeclared_fields") or details.get("fields") or [],
        }
        expected = expected or {"allowed_fields": details.get("allowed_fields") or []}
        misplaced = set(details.get("undeclared_fields") or details.get("fields") or []) & {
            "reason", "evidence_refs"
        }
        next_action = (
            "从 fields 删除 reason/evidence_refs，并把它们放到同一 submission 外层；其余字段只保留 allowed_fields。"
            if misplaced else
            "删除未声明字段，只保留 expected.allowed_fields 后完整重提。"
        )
    elif reason == "bootstrap_wait_for_tool_results":
        retry = "next_frame"
        attempted = attempted or {"current_tool_ids": details.get("current_tool_ids") or []}
        current = current or {"tool_results_visible": False}
        expected = expected or {
            "current_tool_ids": [],
            "submission_frame": "next_reaction",
        }
        next_action = "等待本帧工具结果进入下一帧，再基于可见结果提交 submit_initial_guide。"
    elif reason == "bootstrap_source_not_read":
        retry = "after_tool_result"
        attempted = attempted or {
            "source_refs": details.get("missing_source_refs") or [],
        }
        current = current or {"read_source_refs": details.get("prior_source_refs") or []}
        expected = expected or {"read_source_refs": details.get("missing_source_refs") or []}
        next_action = "先逐项 file_read/web_fetch missing_source_refs，等待结果进入下一帧，再用真实已读来源完整重提。"
    elif reason == "bootstrap_source_requirements_required":
        attempted = attempted or {"source_refs": details.get("source_refs") or []}
        expected = expected or {
            "source_requirements": [{
                "requirement_id": "req_01",
                "source_ref": "已读 source_ref",
                "summary": "从该来源实际读到的要求",
            }],
        }
        next_action = "为每个已读 source_ref 提交 requirement_id/source_ref/summary，再完整重提 bootstrap。"
    elif reason == "bootstrap_invalid_source_requirements":
        attempted = attempted or {
            "invalid_requirements": details.get("invalid_requirements") or [],
        }
        expected = expected or {
            "required_fields": ["requirement_id", "source_ref", "summary"],
        }
        next_action = "逐项修正 invalid_requirements，保证 ID 唯一且三个必填字段非空。"
    elif reason == "bootstrap_source_requirement_ref_unknown":
        attempted = attempted or {
            "source_refs": details.get("unknown_source_refs") or [],
        }
        current = current or {"read_source_refs": details.get("prior_source_refs") or []}
        expected = expected or {
            "source_refs": details.get("source_refs") or [],
            "corrected_example": details.get("corrected_example") or {},
        }
        next_action = next_action or "只引用已读真实来源，按 corrected_example 完整重提 bootstrap。"
    elif reason == "bootstrap_item_requirement_refs_required":
        attempted = attempted or {"items": details.get("items") or []}
        expected = expected or {"requirement_refs": "每个 item 至少一个已声明 requirement_id"}
        next_action = "为 attempted.items 中每个任务项补上 requirement_refs 后完整重提。"
    elif reason == "bootstrap_unknown_requirement_refs":
        attempted = attempted or {"requirement_refs": details.get("requirement_refs") or []}
        expected = expected or {"requirement_refs": details.get("known_requirements") or []}
        next_action = "把未知 requirement_refs 替换为 expected 中已声明的 requirement_id。"
    elif reason == "bootstrap_source_requirement_coverage_missing":
        attempted = attempted or {"uncovered_requirement_ids": details.get("requirement_ids") or []}
        expected = expected or {"item_requirement_refs_must_cover": details.get("requirement_ids") or []}
        next_action = "让至少一个任务项引用每个 uncovered requirement_id 后完整重提。"
    elif reason == "bootstrap_acceptance_refs_required":
        attempted = attempted or {"acceptance": details.get("acceptance") or []}
        expected = expected or {"acceptance_refs": "每个 acceptance 至少包含 item_refs 或 requirement_refs"}
        next_action = "为 attempted.acceptance 中每个验收项补上 item_refs 或 requirement_refs。"
    elif reason == "bootstrap_acceptance_refs_unknown":
        attempted = attempted or {
            "item_refs": details.get("item_refs") or [],
            "requirement_refs": details.get("requirement_refs") or [],
        }
        expected = expected or {
            "item_refs": details.get("known_items") or [],
            "requirement_refs": details.get("known_requirements") or [],
        }
        next_action = "将未知引用替换为 expected 中已声明的 item_id/requirement_id。"
    elif reason == "bootstrap_item_acceptance_coverage_missing":
        attempted = attempted or {"uncovered_item_ids": details.get("items") or []}
        expected = expected or {"acceptance_item_refs_must_cover": details.get("items") or []}
        next_action = "在 acceptance.item_refs 中覆盖每个 uncovered_item_id 后完整重提。"
    elif reason in {"unsupported_task_bootstrap_submission", "unsupported_task_guide_submission"}:
        attempted = attempted or {
            "item_id": details.get("item_id", ""),
            "option_id": details.get("option_id", ""),
        }
        expected = expected or details.get("legal_coordinates") or []
        next_action = "只按当前 guide 的合法 item_id/option_id 坐标重新提交。"
    elif reason in {"pending_interaction_first", "relay_task_guidance_not_pending_input", "task_guidance_already_active"}:
        kind = "state_conflict"
        retry = "after_state_change"
        attempted = attempted or {"guide_id": details.get("guide_id", "")}
        current = current or {
            key: details.get(key)
            for key in ("task_id", "pending_input_ids", "work_guide")
            if details.get(key) not in (None, "", [])
        }
        expected = expected or {"next_state": details.get("next_option_id") or details.get("work_guide") or "当前阻断解除"}
        next_action = next_action or "先处理 current 指出的活动状态，再按当前 guide 坐标提交。"
    elif reason in {"missing_pending_input_ids", "unknown_pending_inputs", "invalid_pending_input_status"}:
        attempted = attempted or {"pending_inputs": details.get("pending_inputs") or []}
        expected = expected or {
            "pending_input_ids": details.get("known_pending_input_ids") or [],
            "statuses": ["integrated", "deferred", "rejected", "split"],
        }
        next_action = "使用 expected 中的真实 pending_input_id 和合法 status 修正后重提。"
    elif reason in {"task_status_update_empty", "missing_task_guide_record_ids", "unknown_task_guide_records"}:
        attempted = attempted or {
            "items": details.get("items") or [],
            "acceptance": details.get("acceptance") or [],
        }
        expected = expected or details.get("expected_fields") or {
            "item_ids": details.get("known_item_ids") or [],
            "acceptance_ids": details.get("known_acceptance_ids") or [],
        }
        next_action = "使用 expected 中的真实记录 ID，提交结构化 items/acceptance 状态更新。"
    elif reason == "task_completion_evidence_required":
        retry = "after_new_evidence"
        attempted = attempted or {"records": details.get("missing_evidence_refs") or []}
        current = current or {"known_evidence_refs": details.get("known_evidence_refs") or []}
        expected = expected or {"evidence_refs": details.get("known_evidence_items") or details.get("known_evidence_refs") or []}
        next_action = "先取得或选择真实 evidence ref，再补到对应完成项；不要猜造 EV-*。"
    elif reason == "task_completion_evidence_not_found":
        retry = "after_correction"
        attempted = attempted or {"evidence_refs": details.get("unknown_evidence_refs") or []}
        current = current or {"known_evidence_refs": details.get("known_evidence_refs") or []}
        expected = expected or {"evidence_refs": details.get("known_evidence_items") or details.get("known_evidence_refs") or []}
        next_action = "用 expected 中 Runtime 已登记的真实 evidence ref 替换未知引用。"
    elif reason in {
            "task_blocked_reason_required",
            "task_blocked_evidence_required",
            "task_blocked_evidence_not_found"}:
        retry = "after_correction"
        attempted = attempted or {
            "records": (
                details.get("missing_reasons")
                or details.get("missing_evidence_refs")
                or []
            ),
            "evidence_refs": details.get("unknown_evidence_refs") or [],
        }
        expected = expected or {
            "blocker_evidence_items": details.get("blocker_evidence_items") or [],
            "correction_example": details.get("correction_example") or {},
        }
        next_action = (
            "为 blocked 记录同时填写非空 reason，并从 expected.blocker_evidence_items "
            "选择真实 call:<call_id> 后按 correction_example 重提；不要把失败调用写成 EV-*。"
        )
    elif reason == "duplicate_task_guide_record_ids":
        attempted = attempted or {"message": details.get("message", "")}
        expected = expected or {"record_ids": "unique"}
        next_action = "为新增任务、验收或来源要求使用不重复的稳定 ID 后重提。"
    else:
        attempted = attempted or {
            key: details.get(key)
            for key in ("field", "fields", "item_id", "option_id", "task_id")
            if details.get(key) not in (None, "", [])
        }
        next_action = next_action or "根据 reason 和 details 修正提交；不要原样重试或声称已成功。"

    return {
        "kind": kind if kind in {
            "validation", "state_conflict", "permission_security",
            "transient_external", "unknown_internal",
        } else "validation",
        "retry": retry,
        "attempted": attempted,
        "current": current,
        "expected": expected,
        "next_action": next_action,
    }
