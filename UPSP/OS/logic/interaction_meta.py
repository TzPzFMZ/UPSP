"""Shared helpers for interaction metadata boundaries."""

CACHE_INTERACTION_META_KEYS = (
    "interaction_object_id",
    "interaction_object",
    "identity_status",
    "interaction_source",
)


def active_relation_card(relation_store, value):
    """Return the one active card resolved by exact id/name/alias."""
    resolver = getattr(relation_store, "resolve_active_subject", None)
    card_id = resolver(value) if callable(resolver) else None
    if not card_id:
        return None
    for card in relation_store.load_registry().get("cards", []):
        if card.get("id") == card_id and card.get("status", "active") == "active":
            return card
    return None


def interaction_meta_for_card(card, source):
    if not isinstance(card, dict):
        return None
    card_id = str(card.get("id") or "").strip()
    if not card_id:
        return None
    return {
        "interaction_object_id": card_id,
        "interaction_object": str(card.get("name") or card_id).strip(),
        "identity_status": "known",
        "interaction_source": str(source or "instance_selection"),
    }


def interaction_meta_from_anchor(state, relation_store):
    """Resolve current instance, default, or an unregistered declaration."""
    identity = (state or {}).get("base", {}).get("identity", {})
    current_id = str(identity.get("current_relation_id") or "").strip()
    if current_id:
        card = active_relation_card(relation_store, current_id)
        if card:
            return interaction_meta_for_card(
                card, identity.get("current_source") or "instance_selection")
        return {
            "interaction_object_id": current_id,
            "interaction_object": "unknown",
            "identity_status": "unknown",
            "interaction_source": "stale_relation_anchor",
        }
    declared_name = str(identity.get("current_declared_name") or "").strip()
    if declared_name:
        return {
            "interaction_object": declared_name,
            "identity_status": "unregistered",
            "interaction_source": identity.get("current_source") or "self_declaration",
        }
    default_id = str(identity.get("local_default_relation_id") or "").strip()
    if default_id:
        card = active_relation_card(relation_store, default_id)
        if card:
            return interaction_meta_for_card(card, "local_default")
    return None


def _anchor_receipt(action, status, card=None, reason=""):
    card = card if isinstance(card, dict) else {}
    return {
        "schema": "interaction_anchor_receipt.v1",
        "action": action,
        "status": status,
        "relation_id": str(card.get("id") or ""),
        "display_name": str(card.get("name") or card.get("id") or ""),
        "reason": reason,
    }


def set_local_default_relation(state_store, relation_store, card_id):
    card = active_relation_card(relation_store, card_id)
    if not card:
        return _anchor_receipt(
            "set_local_default_relation", "rejected",
            reason="active_relation_card_not_found")
    canonical_id = card["id"]
    updates = {"base.identity.local_default_relation_id": canonical_id}
    if not state_store.get("base.identity.current_relation_id") and not state_store.get(
            "base.identity.current_declared_name"):
        updates.update({
            "base.identity.current_relation_id": canonical_id,
            "base.identity.current_declared_name": None,
            "base.identity.current_source": "local_default",
        })
    state_store.update_many(updates)
    return _anchor_receipt("set_local_default_relation", "applied", card)


def begin_interaction_instance(state_store, relation_store, card_id=None):
    target = card_id or state_store.get("base.identity.local_default_relation_id")
    if not target:
        state_store.set_interaction_anchor(source="unbound")
        return _anchor_receipt(
            "begin_interaction_instance", "applied", reason="unbound")
    card = active_relation_card(relation_store, target)
    if not card:
        return _anchor_receipt(
            "begin_interaction_instance", "rejected",
            reason="active_relation_card_not_found")
    state_store.set_interaction_anchor(
        relation_id=card["id"], source=(
            "instance_selection" if card_id else "local_default"))
    return _anchor_receipt("begin_interaction_instance", "applied", card)


def switch_interaction_relation(state_store, relation_store, card_id):
    card = active_relation_card(relation_store, card_id)
    if not card:
        return _anchor_receipt(
            "switch_interaction_relation", "rejected",
            reason="active_relation_card_not_found")
    state_store.set_interaction_anchor(
        relation_id=card["id"], source="instance_selection")
    return _anchor_receipt("switch_interaction_relation", "applied", card)


def cache_interaction_meta(interaction_meta):
    """Return only corpus-cache interaction fields.

    Tool/finalize projections may carry explanatory fields such as ``basis``.
    Cache blocks keep only the four stable interaction identity fields.
    """
    if not isinstance(interaction_meta, dict):
        return {}
    return {
        key: interaction_meta[key]
        for key in CACHE_INTERACTION_META_KEYS
        if key in interaction_meta
    }
