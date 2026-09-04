"""Alert mode settlement protocol processor."""

from datetime import datetime, timedelta

from constants import local_now
from logic.heartbeat_flags import KNOWN_HEARTBEAT_FLAGS
from logic.interaction_meta import cache_interaction_meta


VALID_ALERT_TYPES = frozenset({
    "api_degraded",
    "token_usage_warning",
    "context_pressure",
    "standby_due",
})

VALID_ALERT_STATUSES = frozenset({
    "recovered",
    "deferred",
    "needs_human",
})

ALERT_CLEAR_FLAGS = frozenset({
    "api_degraded",
    "token_usage_warning",
    "context_pressure",
    "standby_due",
})

DEFAULT_DEFER_SECONDS = 3600


def _clean_text(value, *, limit=500):
    text = str(value or "").strip().strip("`")
    text = text.replace("\r", " ").replace("\n", " ")
    text = text.replace("|", "/")
    return " ".join(text.split())[:limit]


def _clean_list(value, *, limit=16):
    if value is None:
        return []
    raw_items = value if isinstance(value, (list, tuple, set)) else [value]
    items = []
    for item in raw_items:
        text = _clean_text(item, limit=160)
        if text and text not in items:
            items.append(text)
        if len(items) >= limit:
            break
    return items


def _receipt(status, *, alert_type="", settle_status="", reason="",
             cleared_flags=None, fault_refs=None, next_attention=""):
    return {
        "tool_id": "alert_mode_settle",
        "status": status,
        "source": "alert_mode_settle",
        "alert_type": alert_type,
        "alert_status": settle_status,
        "cleared_flags": list(cleared_flags or []),
        "fault_refs": list(fault_refs or []),
        "next_attention": next_attention,
        "reason": reason,
    }


def _normalize_declaration(declaration):
    if not isinstance(declaration, dict):
        return None
    return {
        "alert_type": _clean_text(declaration.get("alert_type")).lower(),
        "status": _clean_text(declaration.get("status")).lower(),
        "summary": _clean_text(declaration.get("summary"), limit=800),
        "clear_flags": _clean_list(declaration.get("clear_flags")),
        "fault_refs": _clean_list(declaration.get("fault_refs")),
        "next_attention": _clean_text(
            declaration.get("next_attention"), limit=500),
        "reason": _clean_text(declaration.get("reason"), limit=500),
    }


def _fault_note_text(item):
    refs = ",".join(item["fault_refs"]) if item["fault_refs"] else "none"
    cleared = ",".join(item["clear_flags"]) if item["clear_flags"] else "none"
    return (
        "[警戒模式结算] "
        f"type={item['alert_type']}; "
        f"status={item['status']}; "
        f"cleared={cleared}; "
        f"refs={refs}; "
        f"next_attention={item['next_attention']}; "
        f"summary={item['summary']}"
    )


def _write_alert_settlement(item, round_num, stores, interaction_meta):
    state_store = (stores or {}).get("state_store")
    alert_store = (stores or {}).get("alert_store")
    context_store = (stores or {}).get("context_store")
    if state_store is None or alert_store is None:
        raise ValueError("missing_alert_mode_settle_store")
    clear_flags = list(item["clear_flags"])
    if item["status"] == "deferred" and item["alert_type"] in ALERT_CLEAR_FLAGS:
        if item["alert_type"] not in clear_flags:
            clear_flags.append(item["alert_type"])
    if clear_flags:
        state_store.clear_flags(clear_flags)
    if item["status"] == "deferred":
        now = local_now()
        defer_until = now + timedelta(seconds=DEFAULT_DEFER_SECONDS)
        def defer(data):
            base = data.setdefault("base", {})
            deferrals = base.setdefault("alert_deferrals", {})
            deferrals[item["alert_type"]] = {
                "status": "deferred",
                "defer_seconds": DEFAULT_DEFER_SECONDS,
                "defer_until": defer_until.isoformat(),
                "reason": item["reason"],
                "summary": item["summary"],
                "round_num": round_num,
            }

        state_store.mutate(defer)
    alert_store.append_alert(
        round_num=round_num,
        step="reaction",
        event_type=f"{item['alert_type']}:{item['status']}",
        detail=item["summary"],
        action=item["reason"],
    )
    if context_store is not None:
        meta = cache_interaction_meta(interaction_meta)
        context_store.append_to_cache(
            round_num,
            "system",
            _fault_note_text(item),
            kind="fault_note",
            step="reaction",
            **meta,
        )


def apply_alert_mode_settlement_declarations(
        declarations, round_num, stores=None, interaction_meta=None):
    """Settle alert mode, clear flags, and write health alert evidence."""
    receipts = []
    for declaration in declarations or []:
        item = _normalize_declaration(declaration)
        if not item:
            receipts.append(_receipt("error", reason="invalid_declaration"))
            continue
        if item["alert_type"] not in VALID_ALERT_TYPES:
            receipts.append(_receipt(
                "error",
                alert_type=item["alert_type"],
                settle_status=item["status"],
                reason="invalid_alert_type",
                cleared_flags=item["clear_flags"],
                fault_refs=item["fault_refs"],
                next_attention=item["next_attention"],
            ))
            continue
        if item["status"] not in VALID_ALERT_STATUSES:
            receipts.append(_receipt(
                "error",
                alert_type=item["alert_type"],
                settle_status=item["status"],
                reason="invalid_alert_status",
                cleared_flags=item["clear_flags"],
                fault_refs=item["fault_refs"],
                next_attention=item["next_attention"],
            ))
            continue
        unknown_flags = [
            flag for flag in item["clear_flags"]
            if flag not in KNOWN_HEARTBEAT_FLAGS
        ]
        if unknown_flags:
            receipts.append(dict(
                _receipt(
                    "error",
                    alert_type=item["alert_type"],
                    settle_status=item["status"],
                    reason="unknown_heartbeat_flag",
                    cleared_flags=item["clear_flags"],
                    fault_refs=item["fault_refs"],
                    next_attention=item["next_attention"],
                ),
                unknown_flags=unknown_flags,
            ))
            continue
        invalid_clear_flags = [
            flag for flag in item["clear_flags"]
            if flag not in ALERT_CLEAR_FLAGS
        ]
        if invalid_clear_flags:
            receipts.append(dict(
                _receipt(
                    "error",
                    alert_type=item["alert_type"],
                    settle_status=item["status"],
                    reason="invalid_alert_clear_flag",
                    cleared_flags=item["clear_flags"],
                    fault_refs=item["fault_refs"],
                    next_attention=item["next_attention"],
                ),
                invalid_clear_flags=invalid_clear_flags,
            ))
            continue
        if not item["summary"]:
            receipts.append(_receipt(
                "error",
                alert_type=item["alert_type"],
                settle_status=item["status"],
                reason="empty_alert_summary",
                cleared_flags=item["clear_flags"],
                fault_refs=item["fault_refs"],
                next_attention=item["next_attention"],
            ))
            continue
        try:
            _write_alert_settlement(item, round_num, stores, interaction_meta)
        except Exception as exc:
            receipts.append(_receipt(
                "error",
                alert_type=item["alert_type"],
                settle_status=item["status"],
                reason=str(exc),
                cleared_flags=item["clear_flags"],
                fault_refs=item["fault_refs"],
                next_attention=item["next_attention"],
            ))
            continue
        cleared_flags = list(item["clear_flags"])
        if item["status"] == "deferred" and item["alert_type"] in ALERT_CLEAR_FLAGS:
            if item["alert_type"] not in cleared_flags:
                cleared_flags.append(item["alert_type"])
        receipts.append(_receipt(
            "applied",
            alert_type=item["alert_type"],
            settle_status=item["status"],
            reason=item["reason"],
            cleared_flags=cleared_flags,
            fault_refs=item["fault_refs"],
            next_attention=item["next_attention"],
        ))
    return receipts
