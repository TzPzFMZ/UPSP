"""Materialize runtime rhythm guides into Workbench guide records."""

from datetime import datetime

from constants import TZ_SHANGHAI
from logic.cache_compaction_guide import (
    CACHE_COMPACTION_ITEM_ID,
    materialize_cache_compaction_rhythm_guide,
)
from logic.rhythm_guidance import current_guide


RHYTHM_GUIDE_KINDS = {
    "main_axis_rhythm_guide",
    "calendar_rhythm_guide",
    "emergency_handling_guide",
    "context_pressure_rhythm_guide",
}


def materialize_current_rhythm_guide(
        workbench_store,
        flags,
        *,
        round_num,
        completed_flags=None,
        context_store=None,
        state_store=None,
        connectivity_store=None,
        process_health_checker=None):
    flags, _cleared_flags = reconcile_recovered_emergency_flags(
        flags,
        state_store=state_store,
        connectivity_store=connectivity_store,
        process_health_checker=process_health_checker,
    )
    guide = current_guide(flags, completed_flags=completed_flags)
    kind = str((guide or {}).get("kind") or "").strip()
    desired_items = _desired_item_ids(kind, guide)
    active = _active_rhythm_guide_id(workbench_store)
    superseded = ""
    if active:
        active_guide = workbench_store.load_guide(active)
        if _active_guide_matches(active_guide, kind, desired_items):
            return active
        superseded = _supersede_active_guide(
            workbench_store,
            active_guide,
            round_num=round_num,
            desired_kind=kind,
            desired_items=desired_items,
        )
    if kind == "cache_compaction_rhythm_guide":
        if context_store is None:
            return None
        guide_id = f"cache_compaction:R{int(round_num or 0):06d}"
        if guide_id == superseded:
            guide_id = _next_guide_revision(workbench_store, guide_id)
        return materialize_cache_compaction_rhythm_guide(
            workbench_store,
            context_store,
            round_num,
            state_store=state_store,
            guide_id=guide_id,
        )
    if kind not in RHYTHM_GUIDE_KINDS:
        return None
    guide_id = _guide_id(kind, round_num, guide)
    if guide_id == superseded:
        guide_id = _next_guide_revision(workbench_store, guide_id)
    guide_doc = {
        "guide_id": guide_id,
        "kind": kind,
        "guide_slot": "rhythm",
        "title": _title_for(kind),
        "status": "open",
        "created_at": datetime.now(TZ_SHANGHAI).isoformat(),
        "reason": "runtime heartbeat rhythm guide materialized",
        "source_refs": [
            f"round:{int(round_num or 0)}",
            "heartbeat_flags",
        ],
        "runtime_guide_text": str(guide.get("text") or ""),
        "items": [
            _materialize_item(kind, item)
            for item in guide.get("items") or []
            if isinstance(item, dict)
        ],
    }
    guide_doc["items"] = [item for item in guide_doc["items"] if item]
    if not guide_doc["items"]:
        return None
    workbench_store.save_guide(guide_doc, active=True)
    workbench_store.append_guide_ledger(guide_id, {
        "event": "runtime_rhythm_guide_materialized",
        "round": int(round_num or 0),
        "kind": kind,
        "flags": list(flags.keys()) if isinstance(flags, dict) else [],
    })
    return guide_id


def reconcile_recovered_emergency_flags(
        flags,
        *,
        state_store=None,
        connectivity_store=None,
        process_health_checker=None):
    """Clear recovered emergency flags atomically; keep them on write failure."""
    flags = dict(flags or {})
    clear_flags = []
    if flags.get("api_degraded") and _api_health_recovered(connectivity_store):
        clear_flags.append("api_degraded")
    if flags.get("process_down") and _process_health_recovered(process_health_checker):
        clear_flags.append("process_down")
    if not clear_flags:
        return flags, []
    if state_store is None:
        return flags, []
    try:
        state_store.clear_flags(clear_flags)
    except Exception:
        return flags, []
    for flag in clear_flags:
        flags[flag] = False
    return flags, clear_flags


def _active_rhythm_guide_id(workbench_store):
    try:
        return str(
            workbench_store.get("base.active_guides.rhythm") or ""
        ).strip()
    except Exception:
        return ""


def _desired_item_ids(kind, guide):
    if kind == "cache_compaction_rhythm_guide":
        return [CACHE_COMPACTION_ITEM_ID]
    if kind not in RHYTHM_GUIDE_KINDS:
        return []
    return [
        str(item.get("flag") or "").strip()
        for item in (guide or {}).get("items") or []
        if isinstance(item, dict) and str(item.get("flag") or "").strip()
    ]


def _active_guide_matches(active_guide, desired_kind, desired_items):
    active_guide = active_guide if isinstance(active_guide, dict) else {}
    if str(active_guide.get("status") or "open").strip().lower() != "open":
        return False
    if str(active_guide.get("kind") or "").strip() != desired_kind:
        return False
    pending = [
        str(item.get("item_id") or "").strip()
        for item in active_guide.get("items") or []
        if isinstance(item, dict)
        and str(item.get("status") or "open").strip().lower()
        not in {"completed", "done", "superseded"}
        and str(item.get("item_id") or "").strip()
    ]
    return pending == list(desired_items or [])


def _supersede_active_guide(
        workbench_store,
        active_guide,
        *,
        round_num,
        desired_kind,
        desired_items):
    guide = dict(active_guide or {})
    guide_id = str(guide.get("guide_id") or "").strip()
    if not guide_id:
        return ""
    superseded_at = datetime.now(TZ_SHANGHAI).isoformat()
    guide["status"] = "superseded"
    guide["superseded_at"] = superseded_at
    guide["superseded_by"] = {
        "round": int(round_num or 0),
        "kind": desired_kind or None,
        "items": list(desired_items or []),
    }
    workbench_store.save_guide(guide, active=False)
    workbench_store.append_guide_ledger(guide_id, {
        "event": "runtime_rhythm_guide_superseded",
        "round": int(round_num or 0),
        "previous_kind": str(guide.get("kind") or "").strip(),
        "previous_items": [
            str(item.get("item_id") or "").strip()
            for item in guide.get("items") or []
            if isinstance(item, dict) and str(item.get("item_id") or "").strip()
        ],
        "next_kind": desired_kind or None,
        "next_items": list(desired_items or []),
        "superseded_at": superseded_at,
    })
    workbench_store.clear_active_guide(guide_id)
    return guide_id


def _next_guide_revision(workbench_store, guide_id):
    revision = 2
    while True:
        candidate = f"{guide_id}:rev{revision}"
        try:
            workbench_store.load_guide(candidate)
        except Exception:
            return candidate
        revision += 1


def _api_health_recovered(connectivity_store):
    if connectivity_store is None:
        return False
    try:
        data = connectivity_store.load()
        if hasattr(connectivity_store, "active_statuses"):
            statuses = connectivity_store.active_statuses(data)
        else:
            latest = connectivity_store.latest_status_by_endpoint(data)
            endpoint_tiers = getattr(
                connectivity_store,
                "API_ENDPOINT_TIERS",
                {"primary", "fallback", "emergency"},
            )
            statuses = [
                status for endpoint, status in latest.items()
                if endpoint in endpoint_tiers
            ]
        return (
            bool(statuses)
            and all(status == "ok" for status in statuses)
            and not bool(connectivity_store.has_degraded())
        )
    except Exception:
        return False


def _process_health_recovered(process_health_checker):
    if not callable(process_health_checker):
        return False
    try:
        return not bool(process_health_checker())
    except Exception:
        return False


def _guide_id(kind, round_num, guide=None):
    current_round = int(round_num or 0)
    if kind == "main_axis_rhythm_guide":
        name = "main_axis"
    elif kind == "calendar_rhythm_guide":
        first_flag = ""
        for item in (guide or {}).get("items") or []:
            if isinstance(item, dict):
                first_flag = str(item.get("flag") or "").strip()
                if first_flag:
                    break
        name = first_flag.replace("_due", "") or "calendar"
    elif kind == "context_pressure_rhythm_guide":
        name = "context_pressure"
    else:
        name = "emergency"
    return f"rhythm:{name}:R{current_round:06d}"


def _title_for(kind):
    if kind == "main_axis_rhythm_guide":
        return "主轴节律清单"
    if kind == "calendar_rhythm_guide":
        return "日历节律清单"
    if kind == "context_pressure_rhythm_guide":
        return "上下文压力维护清单"
    return "紧急处理清单"


def _materialize_item(kind, item):
    flag = str(item.get("flag") or "").strip()
    if not flag:
        return None
    title = str(item.get("title") or flag).strip()
    if kind in {"main_axis_rhythm_guide", "calendar_rhythm_guide"}:
        return {
            "item_id": flag,
            "title": title,
            "status": "open",
            "options": [{
                "option_id": "write_chronicle",
                "required_fields": ["content"],
                "allowed_fields": ["content", "reason"],
            }],
        }
    return {
        "item_id": flag,
        "title": title,
        "status": "open",
        "options": [
            {
                "option_id": "settle_alert",
                "required_fields": ["status", "summary"],
                "allowed_fields": [
                    "alert_type",
                    "status",
                    "summary",
                    "clear_flags",
                    "fault_refs",
                    "next_attention",
                    "reason",
                ],
            },
            {
                "option_id": "record_fault",
                "required_fields": ["fault_type", "severity", "step", "source", "detail"],
                "allowed_fields": [
                    "fault_type",
                    "severity",
                    "step",
                    "source",
                    "detail",
                    "action",
                    "related_tool_id",
                ],
            },
        ],
    }
