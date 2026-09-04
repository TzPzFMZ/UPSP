"""Spec781 READY migration for the retired WB/container focus state."""

from __future__ import annotations

from copy import deepcopy
import threading

from errors import RequiredContextError
from constants import RESIDENT_LIST_CHAR_LIMIT


_LOCK = threading.RLock()


def _append_resident_relation(document, card_id):
    key = ("relation", str(card_id or "").strip(), "")
    for item in document.get("items") or []:
        current = (
            str(item.get("item_type") or "").strip(),
            str(item.get("item_id") or "").strip(),
            str(item.get("target_file") or "").strip(),
        )
        if current == key:
            return False
    document["items"].append({
        "item_type": "relation",
        "item_id": key[1],
    })
    document["revision"] += 1
    return True


def migrate_focus_retirement(
        *, state_store, workbench, container_store, relation_store,
        resident_store, assembler):
    """Migrate one active instance atomically; dormant instances migrate later."""
    with _LOCK:
        state_before = state_store.load()
        workbench_before = workbench.load_status()
        relation_before = relation_store.load_registry()

        state_after = deepcopy(state_before)
        workbench_after = deepcopy(workbench_before)
        relation_after = deepcopy(relation_before)
        resident_reconcile = resident_store.preview_reconcile()
        resident_after = resident_reconcile["document"]

        state_base = state_after.setdefault("base", {})
        state_changed = any(
            key in state_base for key in ("focus", "old_focus"))
        state_base.pop("focus", None)
        state_base.pop("old_focus", None)

        workbench_base = workbench_after.setdefault("base", {})
        workbench_changed = any(
            key in workbench_base for key in ("focus", "old_focus"))
        workbench_base.pop("focus", None)
        workbench_base.pop("old_focus", None)

        migrated_relations = []
        relation_changed = False
        for card in relation_after.get("cards") or []:
            if not isinstance(card, dict):
                raise ValueError("relation_registry_card_invalid")
            if "body_resident" not in card:
                continue
            enabled = card.pop("body_resident")
            relation_changed = True
            if enabled is not False and enabled is not True:
                raise ValueError("relation_body_resident_invalid")
            if not enabled:
                continue
            card_id = str(card.get("id") or "").strip()
            if not card_id:
                raise ValueError("relation_body_resident_id_missing")
            if _append_resident_relation(resident_after, card_id):
                migrated_relations.append(card_id)

        if sum(
            1 for item in resident_after.get("items") or []
            if item.get("item_type") == "relation"
        ) > 3:
            raise ValueError("relation_body_limit_exceeded")

        legacy_top_level = bool(
            state_changed or workbench_changed or relation_changed
            or resident_reconcile.get("status") == "applied"
        )
        # A completed Spec781 migration removed every top-level marker in one
        # transaction.  The normal READY path therefore checks only the shared
        # registry/derived indexes and never walks every container body.
        container_required = (
            legacy_top_level
            or container_store.has_retired_focus_fields(lightweight=True)
        )
        changed = bool(
            state_changed or workbench_changed or relation_changed
            or migrated_relations or container_required
            or resident_reconcile.get("status") == "applied"
        )
        if not changed:
            return {
                "schema_version": "focus_retirement_migration_receipt.v1",
                "status": "noop",
                "state_fields_removed": False,
                "workbench_fields_removed": False,
                "container_files_changed": 0,
                "relation_cards_migrated": [],
                "resident_reconciled": False,
                "resident_revision": resident_after["revision"],
                "resident_chars": None,
            }

        resident_changed = bool(
            resident_reconcile.get("status") == "applied"
            or migrated_relations
        )
        resident_chars = None
        if resident_changed:
            # Full-body rendering is needed only when this migration changes
            # resident references; a clean startup must remain metadata-only.
            resident_chars = assembler.resident_projection_chars(resident_after)
            if resident_chars > RESIDENT_LIST_CHAR_LIMIT:
                raise ValueError(
                    "resident_list_char_limit_exceeded:"
                    f"max={RESIDENT_LIST_CHAR_LIMIT};actual={resident_chars}")

        resident_before = (
            resident_store.snapshot_bytes() if resident_changed else None)
        container_before = (
            container_store.snapshot_mutation_files()
            if container_required else None
        )
        try:
            if resident_changed:
                resident_store.replace(resident_after)
            if relation_changed:
                relation_store.save_registry(relation_after)
            if workbench_changed:
                workbench.save_status(workbench_after)
            if state_changed:
                state_store.save(state_after)
            container_receipt = (
                container_store.retire_focus_fields(snapshot=container_before)
                if container_required else {"changed_paths": []}
            )

            if state_store.load().get("base", {}).keys() & {"focus", "old_focus"}:
                raise RuntimeError("state_focus_readback_failed")
            if workbench.load_status().get("base", {}).keys() & {"focus", "old_focus"}:
                raise RuntimeError("workbench_focus_readback_failed")
            if any(
                    "body_resident" in card
                    for card in relation_store.load_registry().get("cards") or []):
                raise RuntimeError("relation_body_resident_readback_failed")
            if resident_changed and resident_store.load() != resident_after:
                raise RuntimeError("resident_list_readback_failed")
        except Exception as exc:
            rollback_errors = []
            restores = [
                ("state", lambda: state_store.save(state_before)),
                ("workbench", lambda: workbench.save_status(workbench_before)),
                ("relation", lambda: relation_store.save_registry(relation_before)),
            ]
            if resident_changed:
                restores.append((
                    "resident",
                    lambda: resident_store.restore_bytes(resident_before),
                ))
            if container_before is not None:
                restores.insert(0, (
                    "containers",
                    lambda: container_store.restore_mutation_files(container_before),
                ))
            for label, restore in restores:
                try:
                    restore()
                except Exception as rollback_exc:
                    rollback_errors.append(
                        f"{label}:{type(rollback_exc).__name__}")
            reason = f"focus_retirement_migration_failed:{type(exc).__name__}"
            if rollback_errors:
                reason += ";rollback=" + ",".join(rollback_errors)
            raise RequiredContextError("write", "focus_retirement", reason) from exc

        return {
            "schema_version": "focus_retirement_migration_receipt.v1",
            "status": "applied" if changed else "noop",
            "state_fields_removed": state_changed,
            "workbench_fields_removed": workbench_changed,
            "container_files_changed": len(
                container_receipt.get("changed_paths") or []),
            "relation_cards_migrated": migrated_relations,
            "resident_reconciled": resident_reconcile.get("status") == "applied",
            "resident_revision": resident_after["revision"],
            "resident_chars": resident_chars,
        }
