"""Reaction-step memory_link_update protocol tool processor."""

from logic.memory_privacy import memory_visible_to_state


ALLOWED_OPERATIONS = {"remove"}


def _clean_text(value):
    return str(value or "").strip().strip("`")


def _clean_refs(value):
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.replace("，", ",").replace("、", ",").replace(";", ",").split(",")
    else:
        raw_items = value
    result = []
    for item in raw_items:
        text = _clean_text(item)
        if text and text not in result:
            result.append(text)
    return result


def _resolve_mem_id(raw_mem_id, pending_memory_ids):
    mem_id = _clean_text(raw_mem_id)
    if mem_id.startswith("PENDING"):
        mapped = (pending_memory_ids or {}).get(mem_id)
        if not mapped:
            return mem_id, None
        return mem_id, mapped
    return mem_id, mem_id


def _overview_from_entry(entry):
    if not isinstance(entry, dict):
        return ""
    return str(entry.get("current_overview") or "").strip()


def _receipt(status, declaration, reason="", mem_id=None, linked_containers=None,
             current_overview=""):
    raw_mem_id = _clean_text(declaration.get("mem_id")) if isinstance(declaration, dict) else ""
    return {
        "tool_id": "memory_link_update",
        "status": status,
        "source": "memory_link_update_declaration",
        "mem_id": mem_id or raw_mem_id,
        "raw_mem_id": raw_mem_id,
        "operation": _clean_text(declaration.get("operation")) if isinstance(declaration, dict) else "",
        "container_refs": _clean_refs(declaration.get("container_refs")) if isinstance(declaration, dict) else [],
        "linked_containers": linked_containers or [],
        "current_overview": current_overview,
        "reason": reason,
    }


def apply_memory_link_update_declarations(
    declarations,
    data_modules,
    pending_memory_ids=None,
    state=None,
):
    """Validate and apply memory_link_update declarations."""
    receipts = []
    if not declarations:
        return receipts
    memory_store = data_modules["memory_store"]
    for declaration in declarations:
        if not isinstance(declaration, dict):
            receipts.append(_receipt("error", {}, reason="invalid_declaration"))
            continue
        raw_mem_id, mem_id = _resolve_mem_id(declaration.get("mem_id"), pending_memory_ids)
        if not raw_mem_id:
            receipts.append(_receipt("error", declaration, reason="missing_mem_id"))
            continue
        if mem_id is None:
            receipts.append(_receipt("invalid_pending_mem_id", declaration, mem_id=raw_mem_id))
            continue
        operation = _clean_text(declaration.get("operation")).lower()
        if operation not in ALLOWED_OPERATIONS:
            receipts.append(_receipt("error", declaration, reason="invalid_operation", mem_id=mem_id))
            continue
        refs = _clean_refs(declaration.get("container_refs"))
        if not refs:
            receipts.append(_receipt("error", declaration, reason="missing_container_refs", mem_id=mem_id))
            continue
        try:
            if not memory_visible_to_state(
                    memory_store, mem_id, state,
                    data_modules.get("relation_store")):
                receipts.append(_receipt(
                    "private_memory_not_visible",
                    declaration,
                    reason="private_memory_not_visible",
                    mem_id=mem_id,
                ))
                continue
            updated = memory_store.update_linked_containers(
                mem_id, "remove", refs, current_overview=None)
            receipts.append(_receipt(
                "applied",
                declaration,
                mem_id=mem_id,
                linked_containers=list((updated or {}).get("linked_containers") or []),
                current_overview=_overview_from_entry(updated),
            ))
        except Exception as exc:
            receipts.append(_receipt("error", declaration, reason=str(exc), mem_id=mem_id))
    return receipts
