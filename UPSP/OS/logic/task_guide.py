"""Task-guide helpers for Spec434."""

import os
import re
from pathlib import Path

from logic.evidence_refs import (
    command_evidence_refs,
    evidence_handle_for_result,
    shell_result_subcommands,
)


BOOTSTRAP_GUIDE_ID = "task_bootstrap"
BOOTSTRAP_ITEM_ID = "build_initial_task_guide"
BOOTSTRAP_SUBMIT_OPTION_ID = "submit_initial_guide"
TASK_PROGRESS_ITEM_ID = "task_progress"
TASK_PROGRESS_UPDATE_OPTION_ID = "update_task_status"
TASK_PENDING_INPUT_OPTION_ID = "integrate_pending_input"

COMPLETION_STATUS_ALIASES = {
    "complete",
    "completed",
    "finish",
    "finished",
}

ITEM_COMPLETION_STATUSES = {
    "accepted",
    "applied",
    "complete",
    "completed",
    "done",
    "finish",
    "finished",
}

ACCEPTANCE_COMPLETION_STATUSES = ITEM_COMPLETION_STATUSES | {
    "passed",
    "verified",
}
PENDING_INPUT_SETTLED_STATUSES = {
    "integrated",
    "deferred",
    "rejected",
    "split",
}

SUCCESS_EVIDENCE_STATUSES = {"ok", "success", "accepted", "applied"}
BLOCKED_TASK_STATUSES = {"blocked"}
BLOCKER_EVIDENCE_STATUSES = {
    "blocked", "rejected", "error", "failed", "timeout", "not_found", "degraded",
}
ACTIVE_CORPUS_REF_RE = re.compile(r"^C-[0-9]{5}$")
PENDING_INPUT_ALIAS_FIELD_RE = re.compile(
    r"^(?P<pending_id>(?:pending_)?input_\d+)_(?P<field>status|summary|reason)$"
)
PATH_EVIDENCE_ALIAS_PREFIXES = {
    "file",
    "file_edit",
    "file_glob",
    "file_grep",
    "file_read",
    "file_write",
    "shell_command",
}
COMMAND_EVIDENCE_ALIAS_PREFIXES = {
    "call",
    "call_id",
    "cmd",
    "command",
    "run",
    "shell",
    "shell_command",
}


def create_task_bootstrap_guide(workbench_store, reason="", source_refs=None):
    """Create the first active guide that asks the model to define the task."""
    guide = {
        "guide_id": BOOTSTRAP_GUIDE_ID,
        "kind": "task_bootstrap",
        "reason": reason or "",
        "source_refs": list(source_refs or []),
        "items": [
            {
                "item_id": BOOTSTRAP_ITEM_ID,
                "mandatory": True,
                "options": [
                    {
                        "option_id": BOOTSTRAP_SUBMIT_OPTION_ID,
                        "required_fields": [
                            "task_title",
                            "items",
                            "acceptance",
                        ],
                        "allowed_fields": [
                            "task_title",
                            "task_goal",
                            "source_requirements",
                            "items",
                            "acceptance",
                            "risk_notes",
                            "source_refs",
                        ],
                    },
                    {
                        "option_id": "not_a_task",
                        "required_fields": ["reason"],
                        "allowed_fields": ["reason"],
                    },
                ],
            },
        ],
    }
    workbench_store.save_guide(guide, active=True)
    return guide


def materialize_initial_task_guide(
        workbench_store, fields, evidence_refs=None, round_num=None):
    """Create an active WB task and task-execution guide from bootstrap fields."""
    fields = dict(fields or {})
    task_title = str(fields.get("task_title") or "").strip()
    task_goal = str(fields.get("task_goal") or "").strip()
    source_requirements = _normalize_source_requirements(
        fields.get("source_requirements"))
    items = _normalize_task_items(fields.get("items"))
    acceptance = _normalize_acceptance(fields.get("acceptance"))
    risk_notes = _normalize_sequence(fields.get("risk_notes"))
    source_refs = _normalize_sequence(fields.get("source_refs"))
    if evidence_refs:
        source_refs.extend(str(item) for item in evidence_refs if item)

    task_guide = {
        "task_title": task_title,
        "task_goal": task_goal,
        "source_requirements": source_requirements,
        "items": items,
        "acceptance": acceptance,
        "pending_inputs": _normalize_pending_inputs(fields.get("pending_inputs")),
        "risk_notes": risk_notes,
        "source_refs": source_refs,
    }
    if isinstance(round_num, int) and not isinstance(round_num, bool) and round_num > 0:
        task_guide["created_round"] = round_num
    task_id = workbench_store.create_task_guide_task(
        task_title=task_title,
        task_goal=task_goal,
        guide=task_guide,
    )
    refresh_task_execution_active_guide(workbench_store, task_id)
    return task_id


def refresh_task_execution_active_guide(workbench_store, task_id):
    """Rebuild the visible task_execution guide from canonical task_guide.json."""
    task_id = str(task_id or "").strip()
    if not task_id:
        return {}
    guide = workbench_store.load_task_guide(task_id)
    task_title = str(guide.get("task_title") or guide.get("title") or "").strip()
    task_goal = str(guide.get("task_goal") or guide.get("goal") or "").strip()
    source_requirements = _normalize_source_requirements(
        guide.get("source_requirements"))
    items = _normalize_task_items(guide.get("items"))
    acceptance = _normalize_acceptance(guide.get("acceptance"))
    pending_inputs = _normalize_pending_inputs(guide.get("pending_inputs"))
    risk_notes = _normalize_sequence(guide.get("risk_notes"))
    source_refs = _normalize_sequence(guide.get("source_refs"))
    active_guide = {
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "task_title": task_title,
        "task_goal": task_goal,
        "source_requirements": source_requirements,
        "items": _task_execution_active_items(items, acceptance),
        "acceptance": acceptance,
        "pending_inputs": pending_inputs,
        "risk_notes": risk_notes,
        "source_refs": source_refs,
    }
    workbench_store.save_guide(active_guide, active=True)
    return active_guide


def _task_execution_active_items(items, acceptance):
    guide_items = [_task_progress_active_item()]
    guide_items.extend(_task_item_active_entries(items))
    guide_items.extend(_acceptance_active_entries(acceptance))
    return guide_items


def _task_progress_active_item():
    return {
        "item_id": TASK_PROGRESS_ITEM_ID,
        "mandatory": True,
        "options": [
            {
                "option_id": TASK_PROGRESS_UPDATE_OPTION_ID,
                "required_fields": [],
                "allowed_fields": ["items", "acceptance"],
            },
            {
                "option_id": TASK_PENDING_INPUT_OPTION_ID,
                "required_fields": ["pending_inputs"],
                "allowed_fields": [
                    "pending_inputs",
                    "source_requirements",
                    "items",
                    "acceptance",
                ],
            },
        ],
    }


def _task_item_active_entries(items):
    result = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        item_id = str(item.get("item_id") or "").strip()
        if not item_id:
            continue
        title = str(
            item.get("title") or item.get("description") or item_id
        ).strip()
        entry = {
            "item_id": item_id,
            "task_record_type": "item",
            "title": title,
            "mandatory": item.get("required") is not False,
            "options": [
                {
                    "option_id": "done",
                    "required_fields": [],
                    "allowed_fields": ["evidence_refs", "reason"],
                },
                {
                    "option_id": "blocked",
                    "required_fields": [],
                    "allowed_fields": ["evidence_refs", "reason"],
                },
            ],
        }
        _copy_task_record_projection_fields(entry, item)
        result.append(entry)
    return result


def _acceptance_active_entries(acceptance):
    result = []
    for item in acceptance or []:
        if not isinstance(item, dict):
            continue
        acceptance_id = str(item.get("acceptance_id") or "").strip()
        if not acceptance_id:
            continue
        description = str(
            item.get("description")
            or item.get("title")
            or item.get("target")
            or acceptance_id
        ).strip()
        entry = {
            "item_id": acceptance_id,
            "task_record_type": "acceptance",
            "description": description,
            "mandatory": item.get("required") is not False,
            "options": [
                {
                    "option_id": "passed",
                    "required_fields": [],
                    "allowed_fields": ["evidence_refs", "reason"],
                },
                {
                    "option_id": "blocked",
                    "required_fields": [],
                    "allowed_fields": ["evidence_refs", "reason"],
                },
            ],
        }
        _copy_task_record_projection_fields(entry, item)
        result.append(entry)
    return result


def _copy_task_record_projection_fields(entry, record):
    status = str((record or {}).get("status") or "").strip()
    if status:
        entry["status"] = status
    evidence_refs = _normalize_evidence_refs((record or {}).get("evidence_refs"))
    if evidence_refs:
        entry["evidence_refs"] = evidence_refs
    reason = str((record or {}).get("reason") or "").strip()
    if reason:
        entry["reason"] = reason


def append_task_pending_input(
        workbench_store,
        task_id,
        *,
        source_refs=None,
        summary="",
        input_kind="interaction",
        round_num=None,
        task_guidance_route=""):
    """Register a pointer to new input that an active task must integrate."""
    guide = workbench_store.load_task_guide(task_id)
    pending_inputs = guide.setdefault("pending_inputs", [])
    normalized_refs = _normalize_sequence(source_refs)
    normalized_refs = [
        str(item).strip() for item in normalized_refs if str(item).strip()
    ]
    if normalized_refs:
        ref_set = set(normalized_refs)
        for item in pending_inputs:
            if not isinstance(item, dict):
                continue
            if set(_normalize_sequence(item.get("source_refs"))) == ref_set:
                return dict(item)
    pending_id = _next_pending_input_id(pending_inputs)
    record = {
        "pending_input_id": pending_id,
        "status": "pending",
        "input_kind": str(input_kind or "interaction").strip() or "interaction",
        "source_refs": normalized_refs,
        "summary": str(summary or "").strip(),
    }
    route = str(task_guidance_route or "").strip()
    if route:
        record["task_guidance_route"] = route
    if round_num is not None:
        record["created_round"] = int(round_num or 0)
    pending_inputs.append(record)
    workbench_store.save_task_guide(task_id, guide)
    return dict(record)


def has_open_pending_inputs(workbench_store, task_id):
    return bool(open_pending_input_ids(workbench_store, task_id))


def open_pending_input_ids(workbench_store, task_id):
    guide = workbench_store.load_task_guide(task_id)
    pending_ids = []
    for item in guide.get("pending_inputs") or []:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "pending").strip().lower()
        if status not in PENDING_INPUT_SETTLED_STATUSES:
            pending_id = str(item.get("pending_input_id") or "").strip()
            if pending_id:
                pending_ids.append(pending_id)
    return pending_ids


def apply_pending_input_integration(
        workbench_store,
        task_id,
        fields,
        *,
        evidence_refs=None,
        evidence_context=None):
    fields = _adapt_pending_input_update_fields(fields)
    guide = workbench_store.load_task_guide(task_id)
    pending_updates = _normalize_pending_input_updates(fields.get("pending_inputs"))
    missing = _missing_update_record_ids(
        pending_updates,
        label="pending_inputs",
        id_key="pending_input_id",
    )
    if missing:
        return {
            "status": "rejected",
            "reason": "missing_pending_input_ids",
            "details": {"pending_inputs": missing},
        }
    unknown = _unknown_record_ids(
        guide.setdefault("pending_inputs", []),
        pending_updates,
        id_key="pending_input_id",
    )
    if unknown:
        return {
            "status": "rejected",
            "reason": "unknown_pending_inputs",
            "details": {"pending_inputs": unknown},
        }
    invalid = []
    for update in pending_updates:
        status = str(update.get("status") or "").strip().lower()
        if not status:
            update["status"] = "integrated"
        elif status not in PENDING_INPUT_SETTLED_STATUSES:
            invalid.append(str(update.get("pending_input_id") or "pending_input"))
        else:
            update["status"] = status
    if invalid:
        return {
            "status": "rejected",
            "reason": "invalid_pending_input_status",
            "details": {"pending_inputs": invalid},
        }

    changed_pending = _merge_records(
        guide["pending_inputs"],
        pending_updates,
        id_key="pending_input_id",
    )
    integration_evidence_refs = _pending_input_evidence_refs(
        guide.get("pending_inputs"),
        changed_pending,
    )
    status_update_fields = _pending_existing_status_update_fields(
        guide,
        fields,
    )
    try:
        added_requirements = _append_unique_records(
            guide.setdefault("source_requirements", []),
            _normalize_source_requirements(fields.get("source_requirements")),
            "requirement_id",
        )
        added_items = _append_unique_records(
            guide.setdefault("items", []),
            _pending_new_records(
                fields.get("items"),
                "item_id",
                _existing_record_ids(guide.get("items"), "item_id"),
                _normalize_task_items,
            ),
            "item_id",
        )
        added_acceptance = _append_unique_records(
            guide.setdefault("acceptance", []),
            _pending_new_records(
                fields.get("acceptance"),
                "acceptance_id",
                _existing_record_ids(guide.get("acceptance"), "acceptance_id"),
                _normalize_acceptance,
            ),
            "acceptance_id",
        )
    except ValueError as exc:
        return {
            "status": "rejected",
            "reason": "duplicate_task_guide_record_ids",
            "details": {"message": str(exc)},
        }
    workbench_store.save_task_guide(task_id, guide)
    status_update = None
    if status_update_fields:
        status_evidence_refs = (
            integration_evidence_refs
            or _normalize_evidence_refs(evidence_refs)
        )
        status_update = apply_task_status_update(
            workbench_store,
            task_id,
            status_update_fields,
            evidence_refs=status_evidence_refs,
            evidence_context=_with_pending_input_evidence(
                evidence_context,
                integration_evidence_refs,
            ),
        )
        if status_update.get("status") == "rejected":
            return status_update
    return {
        "status": "accepted",
        "task_id": task_id,
        "updated_pending_inputs": changed_pending,
        "added_source_requirements": added_requirements,
        "added_items": added_items,
        "added_acceptance": added_acceptance,
        "status_update": status_update,
    }


def _existing_record_ids(records, id_key):
    return {
        str(item.get(id_key) or "").strip()
        for item in records or []
        if isinstance(item, dict) and str(item.get(id_key) or "").strip()
    }


def _pending_existing_status_update_fields(guide, fields):
    fields = dict(fields or {})
    result = {}
    item_ids = _existing_record_ids(guide.get("items"), "item_id")
    acceptance_ids = _existing_record_ids(
        guide.get("acceptance"), "acceptance_id")
    item_updates = _pending_existing_status_updates(
        fields.get("items"),
        "item_id",
        item_ids,
    )
    acceptance_updates = _pending_existing_status_updates(
        fields.get("acceptance"),
        "acceptance_id",
        acceptance_ids,
    )
    if item_updates:
        result["items"] = item_updates
    if acceptance_updates:
        result["acceptance"] = acceptance_updates
    return result


def _pending_existing_status_updates(value, id_key, existing_ids):
    updates = _normalize_record_updates(value, id_key)
    if not updates:
        return []
    if isinstance(value, dict) and not _looks_like_record_object(value, id_key):
        return updates
    return [
        update
        for update in updates
        if str(update.get(id_key) or "").strip() in existing_ids
    ]


def _normalize_pending_input_updates(value):
    if value is None:
        return []
    if isinstance(value, list):
        records = []
        for item in value:
            if isinstance(item, dict):
                record = dict(item)
                pending_id = str(
                    record.get("pending_input_id") or record.get("id") or ""
                ).strip()
            else:
                record = {}
                pending_id = str(item or "").strip()
            if not pending_id:
                continue
            record["pending_input_id"] = pending_id
            record.pop("id", None)
            record.setdefault("status", "integrated")
            records.append(record)
        return records
    updates = _normalize_record_updates(value, "pending_input_id")
    for update in updates:
        if isinstance(update, dict):
            update.setdefault("status", "integrated")
    return updates


def _adapt_pending_input_update_fields(fields):
    fields = dict(fields or {})
    if fields.get("pending_inputs") is not None:
        return fields

    pending_id = str(
        fields.get("pending_input_id")
        or fields.get("input_id")
        or fields.get("id")
        or ""
    ).strip()
    if pending_id:
        record = {"pending_input_id": pending_id}
        for key in ("status", "summary", "reason"):
            value = fields.get(key)
            if value not in (None, ""):
                record[key] = value
        fields["pending_inputs"] = [record]
        for key in ("pending_input_id", "input_id", "id", "status", "summary", "reason"):
            fields.pop(key, None)
        return fields

    grouped = {}
    consumed_keys = []
    for key, value in list(fields.items()):
        match = PENDING_INPUT_ALIAS_FIELD_RE.match(str(key))
        if not match:
            continue
        consumed_keys.append(key)
        pending_key = match.group("pending_id")
        record = grouped.setdefault(
            pending_key,
            {"pending_input_id": pending_key},
        )
        if value not in (None, ""):
            record[match.group("field")] = value
    if grouped:
        fields["pending_inputs"] = list(grouped.values())
        for key in consumed_keys:
            fields.pop(key, None)
    return fields


def _pending_new_records(value, id_key, existing_ids, normalizer):
    if value is None:
        return []
    if isinstance(value, dict) and not _looks_like_record_object(value, id_key):
        return []
    records = normalizer(value)
    return [
        record
        for record in records
        if str(record.get(id_key) or "").strip() not in existing_ids
    ]


def _looks_like_record_object(value, id_key):
    if not isinstance(value, dict):
        return False
    object_keys = {
        id_key,
        "id",
        "title",
        "description",
        "text",
        "name",
        "target",
        "required",
        "mandatory",
        "status",
        "evidence_refs",
    }
    return any(key in value for key in object_keys)


def _pending_input_evidence_refs(pending_inputs, changed_pending):
    changed = {
        str(item or "").strip()
        for item in changed_pending or []
        if str(item or "").strip()
    }
    refs = []
    for item in pending_inputs or []:
        if not isinstance(item, dict):
            continue
        pending_id = str(item.get("pending_input_id") or "").strip()
        if pending_id not in changed:
            continue
        for ref in (
                [f"pending_input:{pending_id}"]
                + _normalize_evidence_refs(item.get("source_refs"))):
            if ref and ref not in refs:
                refs.append(ref)
    return refs


def _with_pending_input_evidence(evidence_context, refs):
    refs = _normalize_evidence_refs(refs)
    if evidence_context is None or not refs:
        return evidence_context
    context = dict(evidence_context if isinstance(evidence_context, dict) else {})
    prior = list(context.get("prior_general_tool_results") or [])
    prior.append({
        "tool_id": "guide_submit",
        "status": "accepted",
        "evidence_refs": refs,
    })
    context["prior_general_tool_results"] = prior
    return context


def apply_task_status_update(
        workbench_store,
        task_id,
        fields,
        *,
        evidence_refs=None,
        evidence_context=None):
    fields = dict(fields or {})
    guide = workbench_store.load_task_guide(task_id)
    item_updates = _normalize_record_updates(fields.get("items"), "item_id")
    acceptance_updates = _normalize_record_updates(
        fields.get("acceptance"), "acceptance_id")
    _normalize_update_statuses(item_updates)
    _normalize_update_statuses(acceptance_updates)
    if not item_updates and not acceptance_updates:
        return {
            "status": "rejected",
            "reason": "task_status_update_empty",
            "details": {
                "hint": (
                    "update_task_status 必须提交结构字段；不要只写 reason，"
                    "reason 不会改变账本状态。任务项写 "
                    'fields.items={"task_01":{"status":"done",'
                    '"evidence_refs":["EV-..."]}}；验收项写 '
                    'fields.acceptance={"acc_01":{"status":"passed",'
                    '"evidence_refs":["EV-..."]}}。'
                ),
                "expected_fields": {
                    "items": {
                        "task_01": {
                            "status": "done",
                            "evidence_refs": ["EV-..."],
                        }
                    },
                    "acceptance": {
                        "acc_01": {
                            "status": "passed",
                            "evidence_refs": ["EV-..."],
                        }
                    },
                },
            },
        }
    _apply_submission_evidence_refs(
        item_updates,
        evidence_refs,
        final_statuses=ITEM_COMPLETION_STATUSES | BLOCKED_TASK_STATUSES,
    )
    _apply_submission_evidence_refs(
        acceptance_updates,
        evidence_refs,
        final_statuses=ACCEPTANCE_COMPLETION_STATUSES | BLOCKED_TASK_STATUSES,
    )
    missing_items = _missing_update_record_ids(
        item_updates,
        label="items",
        id_key="item_id",
    )
    missing_acceptance = _missing_update_record_ids(
        acceptance_updates,
        label="acceptance",
        id_key="acceptance_id",
    )
    if missing_items or missing_acceptance:
        return {
            "status": "rejected",
            "reason": "missing_task_guide_record_ids",
            "details": {
                "items": missing_items,
                "acceptance": missing_acceptance,
            },
        }
    unknown_items = _unknown_record_ids(
        guide.setdefault("items", []),
        item_updates,
        id_key="item_id",
    )
    unknown_acceptance = _unknown_record_ids(
        guide.setdefault("acceptance", []),
        acceptance_updates,
        id_key="acceptance_id",
    )
    if unknown_items or unknown_acceptance:
        return {
            "status": "rejected",
            "reason": "unknown_task_guide_records",
            "details": {
                "items": unknown_items,
                "acceptance": unknown_acceptance,
            },
        }
    missing_evidence = _missing_completion_evidence_refs(
        item_updates,
        id_key="item_id",
        label="items",
        final_statuses=ITEM_COMPLETION_STATUSES,
    ) + _missing_completion_evidence_refs(
        acceptance_updates,
        id_key="acceptance_id",
        label="acceptance",
        final_statuses=ACCEPTANCE_COMPLETION_STATUSES,
    )
    known_evidence_refs = _known_task_evidence_refs(evidence_context)
    blocker_evidence_refs = _known_blocker_evidence_refs(
        evidence_context,
        guide,
    )
    missing_blocked_reason = _missing_blocked_reasons(
        item_updates,
        id_key="item_id",
        label="items",
    ) + _missing_blocked_reasons(
        acceptance_updates,
        id_key="acceptance_id",
        label="acceptance",
    )
    missing_blocked_evidence = _missing_completion_evidence_refs(
        item_updates,
        id_key="item_id",
        label="items",
        final_statuses=BLOCKED_TASK_STATUSES,
    ) + _missing_completion_evidence_refs(
        acceptance_updates,
        id_key="acceptance_id",
        label="acceptance",
        final_statuses=BLOCKED_TASK_STATUSES,
    )
    blocker_items = _feedback_blocker_evidence_items(
        evidence_context,
        blocker_evidence_refs,
        guide=guide,
    )
    if missing_blocked_reason:
        return {
            "status": "rejected",
            "reason": "task_blocked_reason_required",
            "details": {
                "missing_reasons": missing_blocked_reason,
                "blocker_evidence_items": blocker_items,
                "correction_example": _blocked_correction_example(
                    missing_blocked_reason,
                    blocker_items,
                ),
            },
        }
    if missing_blocked_evidence:
        return {
            "status": "rejected",
            "reason": "task_blocked_evidence_required",
            "details": {
                "missing_evidence_refs": missing_blocked_evidence,
                "blocker_evidence_items": blocker_items,
                "correction_example": _blocked_correction_example(
                    missing_blocked_evidence,
                    blocker_items,
                ),
            },
        }
    if missing_evidence:
        details = {"missing_evidence_refs": missing_evidence}
        if known_evidence_refs:
            details.update({
                "known_evidence_refs": _feedback_known_evidence_refs(
                    known_evidence_refs
                ),
                "known_evidence_items": _feedback_known_evidence_items(
                    evidence_context,
                    known_evidence_refs,
                ),
                "hint": (
                    "不要猜造 EV-*；直接按 known_evidence_items 的来源选择 ref，"
                    "补入对应完成项后重新提交。"
                ),
            })
        return {
            "status": "rejected",
            "reason": "task_completion_evidence_required",
            "details": details,
        }
    unknown_evidence, unknown_blocker_evidence = _unknown_task_evidence_refs(
        item_updates,
        acceptance_updates,
        known_evidence_refs or set(),
        blocker_evidence_refs or set(),
        evidence_context,
    )
    if unknown_blocker_evidence:
        return {
            "status": "rejected",
            "reason": "task_blocked_evidence_not_found",
            "details": {
                "unknown_evidence_refs": unknown_blocker_evidence,
                "blocker_evidence_items": blocker_items,
                "correction_example": _blocked_correction_example(
                    _blocked_record_labels(
                        item_updates,
                        acceptance_updates,
                    ),
                    blocker_items,
                ),
            },
        }
    if known_evidence_refs is not None:
        if unknown_evidence:
            return {
                "status": "rejected",
                "reason": "task_completion_evidence_not_found",
                "details": {
                    "unknown_evidence_refs": unknown_evidence,
                    "known_evidence_refs": _feedback_known_evidence_refs(
                        known_evidence_refs
                    ),
                    "known_evidence_items": _feedback_known_evidence_items(
                        evidence_context,
                        known_evidence_refs,
                    ),
                    "hint": (
                        "报告正文里自写的 EV-* 不是 Runtime evidence；"
                        "直接按来源选用 known_evidence_items / 上方可改用证据里的 ref 替换；"
                        "不要全盘搜索 EV 字符串。"
                    ),
                },
            }
    changed_items = _merge_records(
        guide["items"],
        item_updates,
        id_key="item_id",
    )
    changed_acceptance = _merge_records(
        guide["acceptance"],
        acceptance_updates,
        id_key="acceptance_id",
    )
    workbench_store.save_task_guide(task_id, guide)
    for item in acceptance_updates:
        if not isinstance(item, dict):
            continue
        workbench_store.append_task_acceptance_ledger(task_id, {
            "event": "acceptance_updated",
            "acceptance_id": str(item.get("acceptance_id") or item.get("id") or ""),
            "status": item.get("status"),
            "evidence_refs": item.get("evidence_refs") or [],
            "details": item,
        })
    return {
        "status": "accepted",
        "task_id": task_id,
        "updated_items": changed_items,
        "updated_acceptance": changed_acceptance,
    }


def _normalize_sequence(value):
    if value is None:
        return []
    if isinstance(value, list):
        return [item for item in value]
    return [value]


def _normalize_record_updates(value, id_key):
    if value is None:
        return []
    if isinstance(value, dict):
        records = []
        for record_id, update in value.items():
            record_id = str(record_id or "").strip()
            if not record_id:
                continue
            if isinstance(update, dict):
                record = dict(update)
                record[id_key] = record_id
            else:
                record = {
                    id_key: record_id,
                    "status": str(update or "").strip(),
                }
            _normalize_record_update_aliases(record)
            records.append(record)
        return records
    if isinstance(value, list):
        records = []
        for item in value:
            if not isinstance(item, dict):
                continue
            record = dict(item)
            record_id = _submitted_record_update_id(record, id_key)
            if record_id:
                record[id_key] = record_id
            _normalize_record_update_aliases(record)
            records.append(record)
        return records
    return []


def _normalize_record_update_aliases(record):
    if not isinstance(record, dict) or record.get("evidence_refs"):
        return
    for key in ("evidence", "evidence_ref", "evidenceRefs"):
        if key in record:
            record["evidence_refs"] = record.get(key)
            return


def _submitted_record_update_id(record, id_key):
    if not isinstance(record, dict):
        return ""
    for key in (id_key, "id"):
        value = str(record.get(key) or "").strip()
        if value:
            return value
    if id_key == "acceptance_id":
        value = str(record.get("item_id") or "").strip()
        if value:
            return value
    return ""


def _missing_update_record_ids(updates, *, label, id_key):
    missing = []
    for index, update in enumerate(updates or [], start=1):
        if not isinstance(update, dict):
            continue
        if not str(update.get(id_key) or "").strip():
            missing.append(f"{label}[{index}]")
    return missing


def _normalize_task_items(value):
    records = []
    for index, item in enumerate(_normalize_sequence(value), start=1):
        if isinstance(item, dict):
            record = dict(item)
            record_id = str(record.get("item_id") or record.get("id") or "").strip()
            if not record_id:
                record_id = f"item_{index:02d}"
            record["item_id"] = record_id
            record.pop("id", None)
            if not str(record.get("title") or "").strip():
                title = (
                    record.get("summary")
                    or record.get("description")
                    or record.get("text")
                    or record.get("name")
                    or record_id
                )
                record["title"] = str(title)
            record.setdefault("required", True)
            record.setdefault("status", "open")
        else:
            record = {
                "item_id": f"item_{index:02d}",
                "title": str(item),
                "required": True,
                "status": "open",
            }
        records.append(record)
    return records


def _normalize_source_requirements(value):
    records = []
    for index, item in enumerate(_normalize_sequence(value), start=1):
        if not isinstance(item, dict):
            continue
        record = dict(item)
        requirement_id = str(
            record.get("requirement_id")
            or record.get("req_id")
            or record.get("id")
            or ""
        ).strip()
        if not requirement_id:
            requirement_id = f"req_{index:02d}"
        record["requirement_id"] = requirement_id
        for alias in ("req_id", "id"):
            record.pop(alias, None)
        if not str(record.get("summary") or "").strip():
            summary = (
                record.get("title")
                or record.get("text")
                or record.get("description")
                or requirement_id
            )
            record["summary"] = str(summary)
        records.append(record)
    return records


def _normalize_acceptance(value):
    records = []
    for index, item in enumerate(_normalize_sequence(value), start=1):
        if isinstance(item, dict):
            record = dict(item)
            record_id = str(
                record.get("acceptance_id")
                or record.get("acc_id")
                or record.get("id")
                or ""
            ).strip()
            if not record_id:
                record_id = f"acc_{index:02d}"
            record["acceptance_id"] = record_id
            for alias in ("acc_id", "id"):
                record.pop(alias, None)
            if not str(record.get("description") or "").strip():
                description = (
                    record.get("summary")
                    or record.get("title")
                    or record.get("text")
                    or record.get("target")
                    or record_id
                )
                record["description"] = str(description)
            record.setdefault("required", True)
            record.setdefault("status", "pending")
        else:
            record = {
                "acceptance_id": f"acc_{index:02d}",
                "description": str(item),
                "required": True,
                "status": "pending",
            }
        records.append(record)
    return records


def _normalize_pending_inputs(value):
    records = []
    for index, item in enumerate(_normalize_sequence(value), start=1):
        if not isinstance(item, dict):
            continue
        record = dict(item)
        record_id = str(
            record.get("pending_input_id") or record.get("id") or ""
        ).strip()
        if not record_id:
            record_id = f"input_{index:02d}"
        record["pending_input_id"] = record_id
        record.pop("id", None)
        record.setdefault("status", "pending")
        refs = _normalize_sequence(record.get("source_refs"))
        record["source_refs"] = [
            str(ref).strip() for ref in refs if str(ref).strip()
        ]
        if "text" in record:
            record.pop("text", None)
        if "content" in record:
            record.pop("content", None)
        records.append(record)
    return records


def _next_pending_input_id(existing):
    max_seq = 0
    for item in existing or []:
        if not isinstance(item, dict):
            continue
        match = re.match(
            r"^input_(\d+)$",
            str(item.get("pending_input_id") or "").strip(),
        )
        if match:
            max_seq = max(max_seq, int(match.group(1)))
    return f"input_{max_seq + 1:02d}"


def _append_unique_records(existing, records, id_key):
    if not records:
        return []
    known = {
        str(item.get(id_key) or "").strip()
        for item in existing or []
        if isinstance(item, dict)
    }
    duplicate_ids = [
        str(record.get(id_key) or "").strip()
        for record in records
        if str(record.get(id_key) or "").strip() in known
    ]
    if duplicate_ids:
        raise ValueError(
            "duplicate_task_guide_record_ids: " + ", ".join(duplicate_ids)
        )
    existing.extend(records)
    return [
        str(record.get(id_key) or "").strip()
        for record in records
        if str(record.get(id_key) or "").strip()
    ]


def _unknown_record_ids(existing, updates, id_key):
    known = {
        str(item.get(id_key) or "").strip()
        for item in existing or []
        if isinstance(item, dict)
    }
    unknown = []
    for update in updates or []:
        if not isinstance(update, dict):
            continue
        record_id = str(update.get(id_key) or "").strip()
        if record_id and record_id not in known:
            unknown.append(record_id)
    return unknown


def _merge_records(existing, updates, id_key):
    index = {
        str(item.get(id_key) or ""): item
        for item in existing
        if isinstance(item, dict)
    }
    changed = []
    for update in updates or []:
        if not isinstance(update, dict):
            continue
        record_id = str(update.get(id_key) or "").strip()
        if not record_id:
            continue
        target = index.get(record_id)
        if target is None:
            target = {id_key: record_id}
            existing.append(target)
            index[record_id] = target
        target.update(update)
        changed.append(record_id)
    return changed


def _normalize_update_statuses(updates):
    for update in updates or []:
        if not isinstance(update, dict) or "status" not in update:
            continue
        status = str(update.get("status") or "").strip().lower()
        if status in COMPLETION_STATUS_ALIASES:
            update["status"] = "done"
        else:
            update["status"] = status


def _normalize_evidence_refs(value):
    if value is None:
        return []
    if isinstance(value, list):
        raw = value
    else:
        raw = [value]
    result = []
    for item in raw:
        ref = str(item or "").strip()
        if ref and ref not in result:
            result.append(ref)
    return result


def _apply_submission_evidence_refs(updates, evidence_refs, *, final_statuses):
    shared_refs = _normalize_evidence_refs(evidence_refs)
    for update in updates or []:
        if not isinstance(update, dict):
            continue
        update_refs = _normalize_evidence_refs(update.get("evidence_refs"))
        if update_refs:
            update["evidence_refs"] = update_refs
            continue
        status = str(update.get("status") or "").strip().lower()
        if status in final_statuses and shared_refs:
            update["evidence_refs"] = list(shared_refs)


def _missing_completion_evidence_refs(updates, *, id_key, label, final_statuses):
    missing = []
    for update in updates or []:
        if not isinstance(update, dict):
            continue
        status = str(update.get("status") or "").strip().lower()
        if status not in final_statuses:
            continue
        refs = _normalize_evidence_refs(update.get("evidence_refs"))
        if not refs:
            missing.append(f"{label}:{str(update.get(id_key) or '').strip()}")
    return missing


def _missing_blocked_reasons(updates, *, id_key, label):
    return [
        f"{label}:{str(update.get(id_key) or '').strip()}"
        for update in updates or []
        if isinstance(update, dict)
        and str(update.get("status") or "").strip().lower() in BLOCKED_TASK_STATUSES
        and not str(update.get("reason") or "").strip()
    ]


def _unknown_task_evidence_refs(
        item_updates,
        acceptance_updates,
        known_evidence_refs,
        blocker_evidence_refs,
        evidence_context=None):
    unknown = []
    unknown_blockers = []
    for update in list(item_updates or []) + list(acceptance_updates or []):
        if not isinstance(update, dict):
            continue
        blocked = (
            str(update.get("status") or "").strip().lower()
            in BLOCKED_TASK_STATUSES
        )
        known = blocker_evidence_refs if blocked else known_evidence_refs
        for ref in _normalize_evidence_refs(update.get("evidence_refs")):
            if (
                    not _evidence_ref_known(
                        ref,
                        known or set(),
                        evidence_context,
                        allow_grant_paths=not blocked,
                    )):
                target = unknown_blockers if blocked else unknown
                if ref not in target:
                    target.append(ref)
    return unknown, unknown_blockers


def _known_task_evidence_refs(
        evidence_context,
        *,
        cache_min_round=None,
        include_cache=True,
        include_active_corpus=True):
    if evidence_context is None:
        return None
    context = evidence_context if isinstance(evidence_context, dict) else {}
    refs = set()
    if include_active_corpus:
        for ref in _normalize_evidence_refs(context.get("active_corpus_ids")):
            text = ref.upper()
            if ACTIVE_CORPUS_REF_RE.match(text):
                refs.add(text)
    for result in _task_evidence_results(
            context,
            cache_min_round=cache_min_round,
            include_cache=include_cache):
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        if status not in SUCCESS_EVIDENCE_STATUSES:
            continue
        tool_id = str(result.get("tool_id") or "").strip()
        call_id = str(
            result.get("call_id")
            or result.get("tool_call_id")
            or result.get("id")
            or ""
        ).strip()
        if call_id:
            refs.update({call_id, f"call:{call_id}"})
            if tool_id:
                refs.add(f"{tool_id}:{call_id}")
        handle = evidence_handle_for_result(result)
        if handle:
            refs.add(handle)
            refs.add(f"evidence:{handle}")
        for ref in _normalize_evidence_refs(result.get("evidence_refs")):
            refs.add(ref)
        for key in ("path", "file_path", "target_path", "root", "cwd"):
            _add_path_evidence_refs(refs, tool_id, result.get(key))
        for key in ("url", "source_url"):
            _add_url_evidence_refs(refs, tool_id, result.get(key))
        if tool_id == "shell_command":
            _add_command_evidence_refs(refs, result.get("command"))
            for subcommand in shell_result_subcommands(result):
                _add_command_evidence_refs(refs, subcommand)
    return refs


def _known_blocker_evidence_refs(evidence_context, guide=None):
    if evidence_context is None:
        return None
    guide = guide if isinstance(guide, dict) else {}
    refs = set(_normalize_evidence_refs(guide.get("source_refs")))
    for records in (guide.get("items") or [], guide.get("acceptance") or []):
        for record in records:
            if isinstance(record, dict):
                refs.update(_normalize_evidence_refs(record.get("evidence_refs")))
    created_round = _positive_round(guide.get("created_round"))
    refs.update(_known_task_evidence_refs(
        evidence_context,
        cache_min_round=created_round,
        include_cache=created_round is not None,
        include_active_corpus=False,
    ) or set())
    context = evidence_context if isinstance(evidence_context, dict) else {}
    for result in _task_evidence_results(
            context,
            cache_min_round=created_round,
            include_cache=created_round is not None):
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        if status not in BLOCKER_EVIDENCE_STATUSES:
            continue
        call_id = str(
            result.get("call_id")
            or result.get("tool_call_id")
            or result.get("id")
            or ""
        ).strip()
        if call_id:
            refs.add(f"call:{call_id}")
    return refs


def _feedback_known_evidence_refs(known_evidence_refs, limit=30):
    refs = sorted(str(ref) for ref in known_evidence_refs or [] if str(ref).strip())
    priority = []
    for ref in refs:
        if ref.startswith("EV-"):
            priority.append((0, ref))
        elif ref.startswith("call:") or re.match(r"^call_[A-Za-z0-9_-]+$", ref):
            priority.append((1, ref))
        elif ref.startswith("command:"):
            priority.append((2, ref))
        elif ref.startswith(("file:", "file_write:", "shell_command:")):
            priority.append((3, ref))
        else:
            priority.append((4, ref))
    selected = []
    for _, ref in sorted(priority):
        if ref not in selected:
            selected.append(ref)
        if len(selected) >= limit:
            break
    return selected


def _feedback_known_evidence_items(
        evidence_context,
        known_evidence_refs,
        limit=12,
        *,
        cache_min_round=None,
        include_cache=True):
    if evidence_context is None:
        return []
    context = evidence_context if isinstance(evidence_context, dict) else {}
    known = set(str(ref) for ref in known_evidence_refs or [] if str(ref).strip())
    items = []
    seen = set()

    for ref in _normalize_evidence_refs(context.get("active_corpus_ids")):
        text = ref.upper()
        if not ACTIVE_CORPUS_REF_RE.match(text) or text not in known:
            continue
        _append_feedback_evidence_item(
            items,
            seen,
            ref=text,
            tool_id="corpus",
            summary=f"当前可见语料块: {text}",
            limit=limit,
        )

    for result in _task_evidence_results(
            context,
            cache_min_round=cache_min_round,
            include_cache=include_cache):
        if len(items) >= limit:
            break
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        if status not in SUCCESS_EVIDENCE_STATUSES:
            continue
        primary_refs = _feedback_primary_refs_for_result(result)
        summary = _feedback_result_summary(result)
        tool_id = str(result.get("tool_id") or "").strip()
        for ref in primary_refs:
            if ref not in known:
                continue
            _append_feedback_evidence_item(
                items,
                seen,
                ref=ref,
                tool_id=tool_id,
                summary=summary,
                limit=limit,
            )
            break
    return items


def _feedback_blocker_evidence_items(
        evidence_context, blocker_evidence_refs, limit=12, guide=None):
    if evidence_context is None:
        return []
    context = evidence_context if isinstance(evidence_context, dict) else {}
    known = set(str(ref) for ref in blocker_evidence_refs or [] if str(ref).strip())
    items = []
    seen = set()
    guide = guide if isinstance(guide, dict) else {}
    created_round = _positive_round(guide.get("created_round"))
    include_cache = created_round is not None
    for result in _task_evidence_results(
            context,
            cache_min_round=created_round,
            include_cache=include_cache):
        if len(items) >= limit:
            break
        if not isinstance(result, dict):
            continue
        status = str(result.get("status") or "").strip().lower()
        call_id = str(
            result.get("call_id")
            or result.get("tool_call_id")
            or result.get("id")
            or ""
        ).strip()
        ref = f"call:{call_id}" if call_id else ""
        if (
                status not in BLOCKER_EVIDENCE_STATUSES
                or not ref
                or ref not in known
                or ref in seen):
            continue
        seen.add(ref)
        tool_id = str(result.get("tool_id") or "").strip()
        reason = _truncate_feedback_text(result.get("reason") or status, 120)
        items.append({
            "ref": ref,
            "tool_id": tool_id,
            "status": status,
            "reason": reason,
            "summary": _truncate_feedback_text(
                f"{tool_id or 'tool'} {status}: {reason}",
            ),
        })
    for item in _feedback_known_evidence_items(
            evidence_context,
            blocker_evidence_refs,
            limit=limit,
            cache_min_round=created_round,
            include_cache=include_cache):
        if len(items) >= limit:
            break
        ref = str(item.get("ref") or "").strip()
        if not ref or ref in seen:
            continue
        seen.add(ref)
        item["status"] = "success"
        item["reason"] = ""
        items.append(item)
    return items


def _blocked_record_labels(item_updates, acceptance_updates):
    labels = []
    for updates, key, label in (
            (item_updates, "item_id", "items"),
            (acceptance_updates, "acceptance_id", "acceptance")):
        labels.extend(
            f"{label}:{str(update.get(key) or '').strip()}"
            for update in updates or []
            if isinstance(update, dict)
            and str(update.get("status") or "").strip().lower()
            in BLOCKED_TASK_STATUSES
        )
    return labels


def _blocked_correction_example(records, blocker_items):
    record = str((records or ["items:item_01"])[0] or "items:item_01")
    section, _, record_id = record.partition(":")
    section = "acceptance" if section == "acceptance" else "items"
    record_id = record_id or ("acc_01" if section == "acceptance" else "item_01")
    ref = str(((blocker_items or [{}])[0]).get("ref") or "call:<call_id>")
    id_key = "acceptance_id" if section == "acceptance" else "item_id"
    return {
        section: [{
            id_key: record_id,
            "status": "blocked",
            "reason": "说明可复核的阻塞事实",
            "evidence_refs": [ref],
        }]
    }


def _positive_round(value):
    if isinstance(value, bool):
        return None
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _task_evidence_results(
        context, *, cache_min_round=None, include_cache=True):
    results = list(context.get("prior_general_tool_results") or [])
    results.extend(
        receipt
        for receipt in context.get("prior_protocol_tool_receipts") or []
        if isinstance(receipt, dict)
        and str(receipt.get("tool_id") or "").strip() != "guide_submit"
    )
    if not include_cache:
        return results
    context_store = context.get("context_store")
    for getter_name in ("get_lately_entries", "get_now_entries"):
        getter = getattr(context_store, getter_name, None)
        if not callable(getter):
            continue
        try:
            entries = getter()
        except (OSError, TypeError, ValueError):
            continue
        for entry in entries or []:
            if not isinstance(entry, dict):
                continue
            if cache_min_round is not None:
                loc = entry.get("loc") if isinstance(entry.get("loc"), dict) else {}
                if _positive_round(loc.get("round")) is None:
                    continue
                if _positive_round(loc.get("round")) < cache_min_round:
                    continue
            tool_result = entry.get("tool_result")
            if isinstance(tool_result, dict):
                results.append(tool_result)
            receipts = [entry.get("protocol_receipt")]
            receipts.extend(entry.get("protocol_receipts") or [])
            results.extend(
                receipt
                for receipt in receipts
                if isinstance(receipt, dict)
                and str(receipt.get("tool_id") or "").strip() != "guide_submit"
            )
    return results


def _append_feedback_evidence_item(items, seen, *, ref, tool_id, summary, limit):
    text = str(ref or "").strip()
    if not text or text in seen or len(items) >= limit:
        return
    seen.add(text)
    entry = {
        "ref": text,
        "summary": _truncate_feedback_text(summary),
    }
    tool = str(tool_id or "").strip()
    if tool:
        entry["tool_id"] = tool
    items.append(entry)


def _feedback_primary_refs_for_result(result):
    refs = []
    handle = evidence_handle_for_result(result)
    if handle:
        refs.append(handle)
    call_id = str(
        result.get("call_id")
        or result.get("tool_call_id")
        or result.get("id")
        or ""
    ).strip()
    if call_id:
        refs.extend([call_id, f"call:{call_id}"])
    refs.extend(_normalize_evidence_refs(result.get("evidence_refs")))
    selected = []
    for ref in refs:
        text = str(ref or "").strip()
        if text and text not in selected:
            selected.append(text)
    return selected


def _feedback_result_summary(result):
    result = result if isinstance(result, dict) else {}
    tool_id = str(result.get("tool_id") or "").strip()
    if tool_id in {"file_write", "file_edit"}:
        path = _first_present(result, "path", "file_path", "target_path")
        return f"{tool_id} 写入: {path}" if path else f"{tool_id} 成功"
    if tool_id == "file_read":
        path = _first_present(result, "path", "file_path")
        return f"file_read 读取: {path}" if path else "file_read 成功"
    if tool_id == "file_glob":
        root = _first_present(result, "root", "cwd")
        pattern = _first_present(result, "pattern", "query")
        if root and pattern:
            return f"file_glob 搜索: {root} :: {pattern}"
        return f"file_glob 搜索: {root or pattern}" if (root or pattern) else "file_glob 成功"
    if tool_id == "file_grep":
        root = _first_present(result, "root", "path")
        query = _first_present(result, "query")
        if root and query:
            return f"file_grep 正文搜索: {root} :: {query}"
        return f"file_grep 正文搜索: {root or query}" if (root or query) else "file_grep 成功"
    if tool_id == "shell_command":
        command = _first_present(result, "command")
        return f"shell_command 运行: {command}" if command else "shell_command 成功"
    if tool_id == "web_fetch":
        url = _first_present(result, "url", "source_url")
        return f"web_fetch 获取: {url}" if url else "web_fetch 成功"
    if tool_id == "web_search":
        query = _first_present(result, "query")
        return f"web_search 搜索: {query}" if query else "web_search 成功"
    if tool_id == "memory_write":
        mem_id = _first_present(result, "mem_id", "memory_id", "id")
        title = _first_present(result, "title")
        if mem_id and title:
            return f"memory_write 写入: {mem_id} / {title}"
        return f"memory_write 写入: {mem_id or title}" if (mem_id or title) else "memory_write 成功"
    path = _first_present(result, "path", "file_path", "target_path", "url", "source_url", "command")
    return f"{tool_id or 'tool'} 成功: {path}" if path else f"{tool_id or 'tool'} 成功"


def _first_present(mapping, *keys):
    for key in keys:
        value = str((mapping or {}).get(key) or "").strip()
        if value:
            return value
    return ""


def _truncate_feedback_text(text, limit=180):
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 1].rstrip() + "…"


def _add_path_evidence_refs(refs, tool_id, value):
    text = str(value or "").strip()
    if not text:
        return
    canonical = _canonical_file_ref(text)
    refs.add(text)
    if canonical:
        refs.add(canonical)
    if tool_id:
        refs.add(f"{tool_id}:{text}")
        if canonical:
            refs.add(f"{tool_id}:{canonical}")


def _add_url_evidence_refs(refs, tool_id, value):
    text = str(value or "").strip()
    if not text:
        return
    canonical = _canonical_url_ref(text)
    refs.add(text)
    if canonical:
        refs.add(canonical)
    if tool_id:
        refs.add(f"{tool_id}:{text}")
        if canonical:
            refs.add(f"{tool_id}:{canonical}")


def _add_command_evidence_refs(refs, value):
    refs.update(command_evidence_refs(value))


def _evidence_ref_known(
        ref,
        known_evidence_refs,
        evidence_context=None,
        *,
        allow_grant_paths=True):
    active_corpus_ref = str(ref or "").strip().upper()
    if (
            ACTIVE_CORPUS_REF_RE.match(active_corpus_ref)
            and active_corpus_ref in known_evidence_refs):
        return True
    for text in _evidence_ref_candidates(ref):
        if text in known_evidence_refs:
            return True
        canonical_command = _canonical_command_ref(text)
        if canonical_command and f"command:{canonical_command}" in known_evidence_refs:
            return True
        lowered_url = _canonical_url_ref(text)
        if lowered_url and lowered_url in known_evidence_refs:
            return True
        canonical_path = _canonical_file_ref(text)
        if not canonical_path:
            continue
        if canonical_path in known_evidence_refs:
            return True
        if _relative_path_suffix_known(canonical_path, known_evidence_refs):
            return True
        if (
                allow_grant_paths
                and _artifact_path_exists_in_grant(
                    canonical_path, evidence_context)):
            return True
    return False


def _evidence_ref_candidates(value):
    text = str(value or "").strip()
    if not text:
        return []
    candidates = [text]
    alias = _strip_path_evidence_alias_prefix(text)
    if alias and alias not in candidates:
        candidates.append(alias)
    command_alias = _strip_command_evidence_alias_prefix(text)
    if command_alias and command_alias not in candidates:
        candidates.append(command_alias)
    return candidates


def _strip_path_evidence_alias_prefix(text):
    match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*):(.*)$", text)
    if not match:
        return ""
    prefix = match.group(1).strip().lower()
    if prefix not in PATH_EVIDENCE_ALIAS_PREFIXES:
        return ""
    value = match.group(2).strip()
    return value


def _strip_command_evidence_alias_prefix(text):
    match = re.match(r"^([a-zA-Z_][a-zA-Z0-9_]*):(.*)$", text)
    if not match:
        return ""
    prefix = match.group(1).strip().lower()
    if prefix not in COMMAND_EVIDENCE_ALIAS_PREFIXES:
        return ""
    value = match.group(2).strip()
    return value


def _relative_path_suffix_known(canonical_path, known_evidence_refs):
    if not canonical_path or os.path.isabs(canonical_path):
        return False
    if "\\" not in canonical_path and "/" not in canonical_path:
        return False
    suffix = os.sep + canonical_path.strip("\\/")
    for known in known_evidence_refs or []:
        for candidate in _evidence_ref_candidates(known):
            known_path = _canonical_file_ref(candidate)
            if known_path and os.path.isabs(known_path) and known_path.endswith(suffix):
                return True
    return False


def _artifact_path_exists_in_grant(canonical_path, evidence_context):
    if not canonical_path:
        return False
    context = evidence_context if isinstance(evidence_context, dict) else {}
    roots = _artifact_roots(context)
    if not roots:
        return False
    candidates = []
    raw_path = Path(canonical_path)
    if raw_path.is_absolute():
        candidates.append(raw_path)
    else:
        task_root = _resolved_path(context.get("task_root"))
        if task_root is not None:
            candidates.append(task_root / canonical_path)
        candidates.extend(root / canonical_path for root in roots)
    for candidate in candidates:
        resolved = _resolved_path(candidate)
        if resolved is None:
            continue
        try:
            if not resolved.is_file():
                continue
        except OSError:
            continue
        if any(resolved.is_relative_to(root) for root in roots):
            return True
    return False


def _artifact_roots(context):
    roots = []
    for key in ("artifact_roots", "write_paths"):
        for value in _normalize_evidence_refs(context.get(key)):
            resolved = _resolved_path(value)
            if resolved is not None and resolved not in roots:
                roots.append(resolved)
    return roots


def _resolved_path(value):
    if value is None:
        return None
    try:
        return Path(value).expanduser().resolve()
    except (OSError, RuntimeError, ValueError):
        return None

def _canonical_url_ref(value):
    text = str(value or "").strip()
    if not text.lower().startswith(("http://", "https://")):
        return ""
    return text.rstrip("/").lower()


def _canonical_file_ref(value):
    text = str(value or "").strip()
    if not text:
        return ""
    if re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*:", text) and not re.match(
            r"^[a-zA-Z]:[\\/]", text):
        return ""
    text = re.sub(r":\d+(?:-\d+)?$", "", text)
    return os.path.normcase(os.path.normpath(text))


def _canonical_command_ref(value):
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    slash_normalized = text.replace("\\", "/")
    return slash_normalized
