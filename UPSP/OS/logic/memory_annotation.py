"""Reaction-step memory_annotation_update protocol tool processor."""

from logic.memory_privacy import (
    can_see_memory,
    confirmed_subjects_from_state,
    privacy_subjects_for_memory,
)


ANNOTATION_LIMIT = 64
CHAIN_PREFIXES = ("DC-", "EC-")


def _clean_text(value):
    return str(value or "").strip().strip("`")


def _clean_refs(value):
    if value is None:
        return []
    if isinstance(value, str):
        raw_items = value.replace("；", ",").replace("，", ",").replace("、", ",").split(",")
    else:
        raw_items = value
    result = []
    for item in raw_items:
        text = _clean_text(item)
        if text and text not in result:
            result.append(text)
    return result


def _receipt(status, declaration, reason=""):
    return {
        "tool_id": "memory_annotation_update",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "status": status,
        "source": "memory_annotation_declaration",
        "mem_id": _clean_text(declaration.get("mem_id")) if isinstance(declaration, dict) else "",
        "annotation_kind": _clean_text(declaration.get("annotation_kind")) if isinstance(declaration, dict) else "",
        "annotation": declaration.get("annotation") if isinstance(declaration, dict) else None,
        "container_refs": _clean_refs(declaration.get("container_refs")) if isinstance(declaration, dict) else [],
        "reason": reason,
    }


def _linked_containers(meta):
    return set(_clean_refs((meta or {}).get("linked_containers")))


def _is_chain_ref(ref):
    return any(str(ref or "").startswith(prefix) for prefix in CHAIN_PREFIXES)


def _requires_chain_ref(kind):
    return kind in {"correction", "caution", "error", "outdated", "订正", "纠错", "过时", "警示"}


def _validate_declaration(
        declaration, memory_store, confirmed_subjects=None,
        relation_store=None):
    mem_id = _clean_text(declaration.get("mem_id"))
    if not mem_id:
        return None, None, "missing_mem_id"
    if "annotation" not in declaration:
        return None, None, "missing_annotation_field"

    annotation = declaration.get("annotation")
    if annotation is not None:
        annotation = _clean_text(annotation)
        if len(annotation) > ANNOTATION_LIMIT:
            return None, None, "annotation_too_long"

    try:
        meta = memory_store.get_meta(mem_id)
    except Exception:
        return None, None, "memory_not_found"
    owners = privacy_subjects_for_memory(memory_store, mem_id)
    if not can_see_memory(
            meta, confirmed_subjects, relation_store,
            privacy_subjects=owners):
        return mem_id, None, "private_memory_not_visible"

    refs = _clean_refs(declaration.get("container_refs"))
    if annotation:
        if not refs:
            return None, None, "missing_container_refs"
        linked = _linked_containers(meta)
        if any(ref not in linked for ref in refs):
            return None, None, "unlinked_container_ref"
        kind = _clean_text(declaration.get("annotation_kind")).lower()
        if _requires_chain_ref(kind) and not any(_is_chain_ref(ref) for ref in refs):
            return None, None, "missing_chain_ref"

    return mem_id, annotation, ""


def apply_memory_annotation_declarations(declarations, data_modules, state=None):
    """Validate and apply memory annotation updates, returning protocol receipts."""
    receipts = []
    if not declarations:
        return receipts

    memory_store = data_modules["memory_store"]
    relation_store = data_modules.get("relation_store")
    confirmed_subjects = confirmed_subjects_from_state(
        state, relation_store=relation_store)
    for declaration in declarations:
        if not isinstance(declaration, dict):
            receipts.append(_receipt("error", {}, reason="invalid_declaration"))
            continue
        mem_id, annotation, reason = _validate_declaration(
            declaration,
            memory_store,
            confirmed_subjects=confirmed_subjects,
            relation_store=relation_store,
        )
        if reason:
            status = "private_memory_not_visible" if reason == "private_memory_not_visible" else "error"
            receipts.append(_receipt(status, declaration, reason=reason))
            continue
        try:
            memory_store.update_annotation(mem_id, annotation)
            receipts.append(_receipt("applied", declaration))
        except Exception as exc:
            receipts.append(_receipt("error", declaration, reason=str(exc)))
    return receipts
