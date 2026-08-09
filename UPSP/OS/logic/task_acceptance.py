"""Task closeout acceptance checks for Spec434."""


DONE_ITEM_STATUSES = {
    "accepted",
    "applied",
    "complete",
    "completed",
    "done",
    "finish",
    "finished",
}
PASSED_ACCEPTANCE_STATUSES = {
    "accepted",
    "complete",
    "completed",
    "done",
    "finish",
    "finished",
    "passed",
    "verified",
}
SETTLED_PENDING_INPUT_STATUSES = {
    "integrated",
    "deferred",
    "rejected",
    "split",
}
BLOCKED_STATUSES = {"blocked"}


def validate_task_closeout(workbench_store, closeout_form):
    closeout_form = closeout_form if isinstance(closeout_form, dict) else {}
    decision = str(closeout_form.get("closeout_decision") or "").strip().lower()
    active_guide = str(workbench_store.get("base.active_guide") or "").strip()
    active_work_guide = ""
    try:
        slots = workbench_store.active_guide_slots()
        if isinstance(slots, dict):
            active_work_guide = str(slots.get("work") or "").strip()
    except Exception:
        active_work_guide = ""
    if (
            decision == "finish"
            and (
                active_guide == "task_bootstrap"
                or active_work_guide == "task_bootstrap")):
        return {
            "allowed": False,
            "reason": "task_bootstrap_pending",
            "blockers": ["task_bootstrap"],
        }

    task_id = str(workbench_store.get("base.active_task") or "").strip()
    if not task_id:
        return {
            "allowed": True,
            "reason": "no_active_task" if decision == "finish" else "not_finish",
        }

    guide = workbench_store.load_task_guide(task_id)
    ledger_state = _task_ledger_state(guide)
    blockers = (
        _blocked_pending_inputs(guide)
        + _blocked_required_items(guide)
        + _blocked_required_acceptance(guide)
    )
    terminal_blocked = _task_requirements_terminal_blocked(guide)
    if terminal_blocked:
        return {
            "allowed": False,
            "reason": "task_acceptance_blocked",
            "task_id": task_id,
            "blockers": blockers,
            "terminal_blocked": True,
            "ledger_state": ledger_state,
        }
    if decision != "finish":
        return {
            "allowed": True,
            "reason": "not_finish",
            "task_id": task_id,
            "blockers": blockers,
            "terminal_blocked": False,
            "ledger_state": ledger_state,
        }
    if blockers:
        return {
            "allowed": False,
            "reason": "task_acceptance_blocked",
            "task_id": task_id,
            "blockers": blockers,
            "terminal_blocked": False,
            "ledger_state": ledger_state,
        }
    return {
        "allowed": True,
        "reason": "task_acceptance_passed",
        "task_id": task_id,
        "ledger_state": ledger_state,
    }


def _blocked_required_items(guide):
    blockers = []
    for item in guide.get("items") or []:
        if not isinstance(item, dict):
            continue
        if not (item.get("required") is True or item.get("mandatory") is True):
            continue
        status = str(item.get("status") or "open").strip().lower()
        item_id = str(item.get("item_id") or item.get("id") or "item")
        if status not in DONE_ITEM_STATUSES:
            blockers.append(item_id)
            continue
        if not _has_evidence_refs(item):
            blockers.append(f"{item_id}:evidence_refs")
    return blockers


def _blocked_pending_inputs(guide):
    blockers = []
    for item in guide.get("pending_inputs") or []:
        if not isinstance(item, dict):
            continue
        status = str(item.get("status") or "pending").strip().lower()
        if status in SETTLED_PENDING_INPUT_STATUSES:
            continue
        pending_id = str(
            item.get("pending_input_id") or item.get("id") or "pending_input")
        blockers.append(f"{pending_id}:pending_input")
    return blockers


def _blocked_required_acceptance(guide):
    blockers = []
    for item in guide.get("acceptance") or []:
        if not isinstance(item, dict):
            continue
        if item.get("required") is False:
            continue
        status = str(item.get("status") or "pending").strip().lower()
        acceptance_id = str(
            item.get("acceptance_id") or item.get("id") or "acceptance")
        if status not in PASSED_ACCEPTANCE_STATUSES:
            blockers.append(acceptance_id)
            continue
        if not _has_evidence_refs(item):
            blockers.append(f"{acceptance_id}:evidence_refs")
    return blockers


def _has_evidence_refs(record):
    refs = record.get("evidence_refs") if isinstance(record, dict) else None
    if isinstance(refs, list):
        return any(str(item or "").strip() for item in refs)
    return bool(str(refs or "").strip())


def _task_requirements_terminal_blocked(guide):
    """Return true only when the task ledger is fully settled with a real block.

    A terminal blocked task is different from an unfinished task: every required
    item and acceptance record must already be either successful with evidence or
    explicitly blocked with evidence, and no pending input may remain open.
    """
    if _blocked_pending_inputs(guide):
        return False

    saw_blocked = False
    for item in guide.get("items") or []:
        if not isinstance(item, dict):
            continue
        if not (item.get("required") is True or item.get("mandatory") is True):
            continue
        status = str(item.get("status") or "open").strip().lower()
        if status in BLOCKED_STATUSES:
            if not _has_evidence_refs(item) or not _has_blocked_reason(item):
                return False
            saw_blocked = True
            continue
        if status not in DONE_ITEM_STATUSES or not _has_evidence_refs(item):
            return False

    for item in guide.get("acceptance") or []:
        if not isinstance(item, dict) or item.get("required") is False:
            continue
        status = str(item.get("status") or "pending").strip().lower()
        if status in BLOCKED_STATUSES:
            if not _has_evidence_refs(item) or not _has_blocked_reason(item):
                return False
            saw_blocked = True
            continue
        if status not in PASSED_ACCEPTANCE_STATUSES or not _has_evidence_refs(item):
            return False

    return saw_blocked


def _has_blocked_reason(record):
    return bool(str((record or {}).get("reason") or "").strip())


def _task_ledger_state(guide):
    state = {"items": [], "acceptance": [], "pending_inputs": []}
    for key, id_key, required in (
            ("items", "item_id", lambda item: item.get("required") is True or item.get("mandatory") is True),
            ("acceptance", "acceptance_id", lambda item: item.get("required") is not False)):
        for item in guide.get(key) or []:
            if not isinstance(item, dict) or not required(item):
                continue
            state[key].append({
                "id": str(item.get(id_key) or item.get("id") or "").strip(),
                "status": str(item.get("status") or "").strip().lower(),
                "evidence_refs": sorted({
                    str(ref or "").strip()
                    for ref in (
                        item.get("evidence_refs")
                        if isinstance(item.get("evidence_refs"), list)
                        else [item.get("evidence_refs")]
                    )
                    if str(ref or "").strip()
                }),
            })
    for item in guide.get("pending_inputs") or []:
        if not isinstance(item, dict):
            continue
        state["pending_inputs"].append({
            "id": str(
                item.get("pending_input_id") or item.get("id") or ""
            ).strip(),
            "status": str(item.get("status") or "pending").strip().lower(),
        })
    return state
