"""Chronicle write protocol processor."""

from data.chronicle_store import ChronicleStore


VALID_CHRONICLE_LAYERS = frozenset({
    "rhythms",
    "daily",
    "weekly",
    "monthly",
    "quarterly",
    "yearly",
})

VALID_ROUND_TYPES = frozenset({
    "interactive",
    "rhythm",
    "relay",
    "autonomous",
    "standby",
})


def _clean_text(value, *, limit=4000):
    text = str(value or "").strip()
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return text[:limit]


def _clean_list(value, *, limit=16):
    if value is None:
        return []
    raw_items = value if isinstance(value, (list, tuple, set)) else [value]
    items = []
    for item in raw_items:
        text = str(item or "").strip()
        if text and text not in items:
            items.append(text[:160])
        if len(items) >= limit:
            break
    return items


def _receipt(status, *, layer="", path="", reason="", round_num=None,
             round_type="", source_refs=None):
    receipt = {
        "tool_id": "chronicle_write",
        "status": status,
        "source": "chronicle_write",
        "layer": layer,
        "path": path,
        "reason": reason,
    }
    if round_num not in (None, ""):
        receipt["round_num"] = round_num
    if round_type:
        receipt["round_type"] = round_type
    if source_refs is not None:
        receipt["source_refs"] = list(source_refs)
    return receipt


def _normalize_declaration(declaration):
    if not isinstance(declaration, dict):
        return None
    return {
        "content": _clean_text(declaration.get("content")),
        "reason": _clean_text(declaration.get("reason"), limit=500),
    }


def apply_chronicle_write_declarations(declarations, stores=None):
    """Validate model prose and commit the frozen Runtime write scope."""
    stores = stores or {}
    chronicle_store = stores.get("chronicle_store") or ChronicleStore()
    write_scope = stores.get("chronicle_write_scope")
    receipts = []
    for declaration in declarations or []:
        item = _normalize_declaration(declaration)
        if not item:
            receipts.append(_receipt("error", reason="invalid_declaration"))
            continue
        if not isinstance(write_scope, dict) or not write_scope:
            receipts.append(_receipt(
                "rejected",
                reason="no_active_chronicle_write_scope",
            ))
            continue
        layer = str(write_scope.get("layer") or "").strip()
        if layer not in VALID_CHRONICLE_LAYERS:
            receipts.append(_receipt(
                "error",
                layer=layer,
                path="",
                reason="invalid_chronicle_layer",
            ))
            continue
        if not item["content"]:
            receipts.append(_receipt(
                "error",
                layer=layer,
                path="",
                reason="empty_chronicle_content",
            ))
            continue
        round_num = write_scope.get("round_num")
        round_type = str(write_scope.get("round_type") or "").strip()
        source_refs = _clean_list(write_scope.get("source_refs"))
        if round_type and round_type not in VALID_ROUND_TYPES:
            receipts.append(_receipt(
                "error",
                layer=layer,
                path="",
                reason="invalid_round_type",
                round_num=round_num,
                round_type=round_type,
                source_refs=source_refs,
            ))
            continue
        try:
            path = chronicle_store.commit_write_scope(
                write_scope, item["content"]
            )
        except Exception as exc:
            receipts.append(_receipt(
                "error",
                layer=layer,
                path="",
                reason=str(exc),
                round_num=round_num,
                round_type=round_type,
                source_refs=source_refs,
            ))
            continue
        receipts.append(_receipt(
            "applied",
            layer=layer,
            path=path,
            reason=item["reason"],
            round_num=round_num,
            round_type=round_type,
            source_refs=source_refs,
        ))
    return receipts
