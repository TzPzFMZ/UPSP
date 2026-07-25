"""Fault record protocol processor."""

from logic.interaction_meta import cache_interaction_meta


VALID_FAULT_TYPES = {
    "tool_failure",
    "parse_failure",
    "external_dependency",
    "api_degraded",
    "data_format",
    "runtime_exception",
}
VALID_SEVERITIES = {"info", "warning", "error", "critical"}
VALID_STEPS = {"setup", "reaction", "cleanup", "heartbeat", "runtime"}


def _clean_text(value, *, limit=500):
    text = str(value or "").strip().strip("`")
    text = text.replace("\r", " ").replace("\n", " ")
    text = text.replace("|", "/")
    return " ".join(text.split())[:limit]


def _normalize(value):
    return _clean_text(value).lower()


def _receipt(status, declaration, *, reason="", fault_type="", severity="",
             step=""):
    return {
        "tool_id": "fault_record",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "status": status,
        "source": "fault_record_table",
        "fault_type": fault_type,
        "severity": severity,
        "step": step,
        "reason": reason,
    }


def _normalize_declaration(declaration):
    if not isinstance(declaration, dict):
        return None
    item = {
        "fault_type": _normalize(declaration.get("fault_type")),
        "severity": _normalize(declaration.get("severity")),
        "step": _normalize(declaration.get("step")),
        "source": _clean_text(declaration.get("source"), limit=120),
        "detail": _clean_text(declaration.get("detail"), limit=500),
        "action": _clean_text(declaration.get("action"), limit=120),
        "related_tool_id": _clean_text(
            declaration.get("related_tool_id"), limit=120),
    }
    if not item["action"]:
        item["action"] = "needs_review"
    return item


def _is_valid(item):
    return (
        item
        and item["fault_type"] in VALID_FAULT_TYPES
        and item["severity"] in VALID_SEVERITIES
        and item["step"] in VALID_STEPS
        and bool(item["source"])
        and bool(item["detail"])
    )


def _fault_note_text(item):
    related = (
        f"; related_tool_id={item['related_tool_id']}"
        if item.get("related_tool_id") else ""
    )
    return (
        "[故障记账] "
        f"severity={item['severity']}; "
        f"type={item['fault_type']}; "
        f"step={item['step']}; "
        f"source={item['source']}; "
        f"action={item['action']}"
        f"{related}; "
        f"detail={item['detail']}"
    )


def _write_fault_record(item, round_num, stores, interaction_meta):
    context_store = (stores or {}).get("context_store")
    alert_store = (stores or {}).get("alert_store")
    if context_store is None or alert_store is None:
        raise ValueError("missing_fault_record_store")
    meta = cache_interaction_meta(interaction_meta)
    alert_store.append_alert(
        round_num=round_num,
        step=item["step"],
        event_type=f"{item['fault_type']}:{item['severity']}",
        detail=f"{item['source']}: {item['detail']}",
        action=item["action"],
    )
    context_store.append_to_cache(
        round_num,
        "system",
        _fault_note_text(item),
        kind="fault_note",
        step=item["step"],
        **meta,
    )


def apply_fault_record_declarations(declarations, round_num, stores,
                                    interaction_meta=None):
    """Validate fault records and write alerts.md plus fault_note blocks."""
    receipts = []
    for declaration in declarations or []:
        item = _normalize_declaration(declaration)
        if not _is_valid(item):
            receipts.append(_receipt(
                "error",
                declaration if isinstance(declaration, dict) else {},
                reason="invalid_fault_record",
                fault_type=(item or {}).get("fault_type", ""),
                severity=(item or {}).get("severity", ""),
                step=(item or {}).get("step", ""),
            ))
            continue
        try:
            _write_fault_record(item, round_num, stores, interaction_meta)
        except Exception as exc:
            receipts.append(_receipt(
                "error",
                declaration,
                reason=str(exc),
                fault_type=item["fault_type"],
                severity=item["severity"],
                step=item["step"],
            ))
            continue
        receipts.append(_receipt(
            "applied",
            declaration,
            reason=item["detail"],
            fault_type=item["fault_type"],
            severity=item["severity"],
            step=item["step"],
        ))
    return receipts
