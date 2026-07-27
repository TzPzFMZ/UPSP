"""Internal work-intent debt for task-guide hard entry."""

from datetime import datetime

from constants import local_now


WORK_INTENT_DEBT_PATH = "base.runtime.work_intent_debt"
WORK_INTENT_DEBT_GUIDE_ID = "__work_intent_debt__"


def _clean(value):
    return str(value or "").strip()


def normalize_work_intent_debt(value):
    if not isinstance(value, dict):
        return {}
    status = _clean(value.get("status")).lower()
    if status != "open":
        return {}
    return {
        "status": "open",
        "created_round": int(value.get("created_round") or 0),
        "reason": _clean(value.get("reason")),
        "source": _clean(value.get("source")),
        "source_refs": [
            _clean(item) for item in value.get("source_refs") or []
            if _clean(item)
        ],
        "task_phase": _clean(value.get("task_phase")),
        "task_root": _clean(value.get("task_root")),
        "created_at": _clean(value.get("created_at")),
    }


def current_work_intent_debt(state_or_store):
    if isinstance(state_or_store, dict):
        state = state_or_store
        base = state.get("base", {}) if isinstance(state, dict) else {}
        runtime = base.get("runtime", {}) if isinstance(base, dict) else {}
        return normalize_work_intent_debt(runtime.get("work_intent_debt") or {})
    if hasattr(state_or_store, "get"):
        try:
            return normalize_work_intent_debt(
                state_or_store.get(WORK_INTENT_DEBT_PATH) or {}
            )
        except Exception:
            return {}
    return {}


def has_open_work_intent_debt(state_or_store):
    return bool(current_work_intent_debt(state_or_store))


def create_work_intent_debt(
        state_store,
        *,
        round_num=0,
        reason="",
        source="",
        source_refs=None,
        task_phase="",
        task_root=""):
    debt = {
        "status": "open",
        "created_round": int(round_num or 0),
        "reason": _clean(reason) or "当前工作需要先建立任务指南清单",
        "source": _clean(source) or "runtime",
        "source_refs": [
            _clean(item) for item in source_refs or [] if _clean(item)
        ],
        "task_phase": _clean(task_phase),
        "task_root": _clean(task_root),
        "created_at": local_now().isoformat(),
    }
    state_store.set(WORK_INTENT_DEBT_PATH, debt)
    return debt


def clear_work_intent_debt(state_store):
    state_store.set(WORK_INTENT_DEBT_PATH, {})
    return {}
