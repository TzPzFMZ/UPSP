"""Memory privacy helpers and protocol processors."""


ALLOWED_BODY_ACTIONS = {"move_private"}
ALLOWED_DECLASSIFY_MODES = {"declassify", "redact", "delete", "keep_private"}
MEMORY_PRIVACY_ENABLED = False


def _clean_text(value):
    return str(value or "").strip().strip("`")


def _as_list(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        raw_items = value
    else:
        raw_items = [value]
    result = []
    for item in raw_items:
        text = _clean_text(item)
        if text and text not in result:
            result.append(text)
    return result


def _canonical_subject(value, relation_store=None):
    text = _clean_text(value)
    if not text or relation_store is None:
        return text
    resolver = getattr(relation_store, "resolve_active_subject", None)
    return _clean_text(resolver(text)) if callable(resolver) else ""


def confirmed_subjects_from_state(
        state=None, interaction_meta=None, relation_store=None):
    """Extract currently confirmed present subjects from runtime state/metadata."""
    confirmed = []
    state = state or {}
    presence = state.get("presence", {}) if isinstance(state, dict) else {}
    confirmed.extend(_as_list(presence.get("confirmed_subjects")))
    base = state.get("base", {}) if isinstance(state, dict) else {}
    confirmed.extend(_as_list(base.get("confirmed_subjects")))
    if isinstance(interaction_meta, dict):
        subject = _clean_text(interaction_meta.get("interaction_object"))
        status = _clean_text(interaction_meta.get("identity_status")).lower()
        if subject and subject != "unknown" and status != "unknown":
            confirmed.append(subject)
    result = []
    for item in confirmed:
        canonical = _canonical_subject(item, relation_store)
        if canonical and canonical not in result:
            result.append(canonical)
    return result


def can_see_memory(
        meta, confirmed_subjects=None, relation_store=None,
        privacy_subjects=None):
    access = _clean_text((meta or {}).get("access") or "public").lower()
    if access != "private":
        return True
    if not MEMORY_PRIVACY_ENABLED:
        return False
    subjects = {
        canonical for subject in _as_list(privacy_subjects)
        if (canonical := _canonical_subject(subject, relation_store))
    }
    confirmed = {
        canonical for subject in _as_list(confirmed_subjects)
        if (canonical := _canonical_subject(subject, relation_store))
    }
    return bool(subjects and any(subject in confirmed for subject in subjects))


def privacy_subjects_for_memory(memory_store, mem_id):
    resolver = getattr(memory_store, "private_subjects_for_memory", None)
    return _as_list(resolver(mem_id)) if callable(resolver) else []


def memory_visible_to_state(memory_store, mem_id, state=None, relation_store=None):
    meta = memory_store.get_meta(mem_id)
    confirmed = confirmed_subjects_from_state(
        state, relation_store=relation_store)
    owners = privacy_subjects_for_memory(memory_store, mem_id)
    return can_see_memory(
        meta, confirmed, relation_store, privacy_subjects=owners)


def _resolve_mem_id(raw_mem_id, pending_memory_ids):
    mem_id = _clean_text(raw_mem_id)
    if mem_id.startswith("PENDING"):
        mapped = (pending_memory_ids or {}).get(mem_id)
        if not mapped:
            return mem_id, None
        return mem_id, mapped
    return mem_id, mem_id


def _receipt(status, declaration, reason="", mem_id=None, private_path=None):
    raw_mem_id = _clean_text(declaration.get("mem_id")) if isinstance(declaration, dict) else ""
    return {
        "tool_id": "memory_privacy_mark",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "status": status,
        "source": "memory_privacy_declaration",
        "mem_id": mem_id or raw_mem_id,
        "raw_mem_id": raw_mem_id,
        "privacy_subject": _clean_text(declaration.get("privacy_subject")) if isinstance(declaration, dict) else "",
        "body_action": _clean_text(declaration.get("body_action")) if isinstance(declaration, dict) else "",
        "private_path": private_path,
        "reason": reason,
    }


def _declassify_receipt(status, declaration, reason="", mem_id=None, result=None):
    raw_mem_id = _clean_text(declaration.get("mem_id")) if isinstance(declaration, dict) else ""
    mode = _clean_text(declaration.get("mode")) if isinstance(declaration, dict) else ""
    receipt = {
        "tool_id": "memory_privacy_declassify",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "status": status,
        "source": "memory_privacy_declassify_declaration",
        "mem_id": mem_id or raw_mem_id,
        "raw_mem_id": raw_mem_id,
        "mode": mode,
        "reason": reason,
    }
    if isinstance(result, dict):
        receipt.update({
            key: value for key, value in result.items()
            if key in {"access", "deleted", "private_path"}
        })
    return receipt


def private_memory_not_visible_receipt(tool_id, source, mem_id, tool_class="sync_tool"):
    return {
        "tool_id": tool_id,
        "tool_family": "protocol_tool",
        "tool_class": tool_class,
        "status": "private_memory_not_visible",
        "source": source,
        "mem_id": mem_id,
        "reason": "private_memory_not_visible",
    }


def apply_memory_privacy_declarations(
    declarations,
    state,
    data_modules,
    pending_memory_ids=None,
):
    """Validate and apply memory_privacy_mark declarations."""
    receipts = []
    if not declarations:
        return receipts
    if not MEMORY_PRIVACY_ENABLED:
        return [
            _receipt("feature_deferred", declaration, reason="feature_deferred")
            for declaration in declarations
        ]
    memory_store = data_modules["memory_store"]
    relation_store = data_modules.get("relation_store")
    confirmed = confirmed_subjects_from_state(
        state, relation_store=relation_store)

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
        raw_privacy_subject = _clean_text(declaration.get("privacy_subject"))
        if not raw_privacy_subject:
            receipts.append(_receipt("error", declaration, reason="missing_privacy_subject", mem_id=mem_id))
            continue
        privacy_subject = _canonical_subject(
            raw_privacy_subject, relation_store)
        if privacy_subject not in confirmed:
            receipts.append(_receipt(
                "privacy_subject_not_confirmed",
                declaration,
                reason="privacy_subject_not_confirmed",
                mem_id=mem_id,
            ))
            continue
        body_action = _clean_text(declaration.get("body_action")) or "move_private"
        if body_action not in ALLOWED_BODY_ACTIONS:
            receipts.append(_receipt("error", declaration, reason="invalid_body_action", mem_id=mem_id))
            continue
        try:
            result = memory_store.mark_private(mem_id, privacy_subject, body_action)
            normalized = dict(declaration)
            normalized["privacy_subject"] = privacy_subject
            receipts.append(_receipt(
                "applied",
                normalized,
                mem_id=mem_id,
                private_path=result.get("private_path"),
            ))
        except Exception as exc:
            receipts.append(_receipt("error", declaration, reason=str(exc), mem_id=mem_id))
    return receipts


def apply_memory_privacy_declassify_declarations(
    declarations,
    state,
    data_modules,
    config=None,
):
    """Validate and apply memory_privacy_declassify declarations."""
    receipts = []
    if not declarations:
        return receipts
    if not MEMORY_PRIVACY_ENABLED:
        return [
            _declassify_receipt(
                "feature_deferred", declaration, reason="feature_deferred")
            for declaration in declarations
        ]
    memory_store = data_modules["memory_store"]
    relation_store = data_modules.get("relation_store")
    confirmed = confirmed_subjects_from_state(
        state, relation_store=relation_store)
    cfg = config or {}
    review_modes = set(cfg.get("requires_review_modes", []))

    for declaration in declarations:
        if not isinstance(declaration, dict):
            receipts.append(_declassify_receipt("error", {}, reason="invalid_declaration"))
            continue
        mem_id = _clean_text(declaration.get("mem_id"))
        if not mem_id:
            receipts.append(_declassify_receipt("error", declaration, reason="missing_mem_id"))
            continue
        mode = _clean_text(declaration.get("mode")) or "declassify"
        if mode not in ALLOWED_DECLASSIFY_MODES:
            receipts.append(_declassify_receipt("error", declaration, reason="invalid_declassify_mode", mem_id=mem_id))
            continue
        try:
            meta = memory_store.get_meta(mem_id)
        except Exception as exc:
            receipts.append(_declassify_receipt("error", declaration, reason=str(exc), mem_id=mem_id))
            continue
        owners = privacy_subjects_for_memory(memory_store, mem_id)
        if not can_see_memory(
                meta, confirmed, relation_store,
                privacy_subjects=owners):
            receipts.append(private_memory_not_visible_receipt(
                "memory_privacy_declassify",
                "memory_privacy_declassify_declaration",
                mem_id,
            ))
            continue
        if mode in review_modes:
            receipts.append(_declassify_receipt(
                "needs_review",
                declaration,
                reason="review_required",
                mem_id=mem_id,
            ))
            continue
        redacted_body = _clean_text(declaration.get("redacted_body"))
        reason = _clean_text(declaration.get("reason"))
        try:
            result = memory_store.declassify_private_memory(
                mem_id,
                mode,
                redacted_body=redacted_body,
                reason=reason,
            )
            receipts.append(_declassify_receipt(
                "applied",
                declaration,
                mem_id=mem_id,
                result=result,
            ))
        except Exception as exc:
            receipts.append(_declassify_receipt("error", declaration, reason=str(exc), mem_id=mem_id))
    return receipts
