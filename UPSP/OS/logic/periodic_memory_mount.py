"""Transactional GUI processor for manual periodic-memory mounts."""

from __future__ import annotations

from copy import deepcopy
import os
import re
import uuid

import paths

from constants import local_now
from data.atomic_write import atomic_write_text
from data.memory_store import (
    MEMORY_MUTATION_LOCK,
    memory_target_tier,
    project_periodic_memory_body,
)
from data.periodic_mount_store import (
    PERIODIC_MOUNTS_SCHEMA,
    PeriodicMountStore,
)
from data.periodic_pin_owner_store import PeriodicPinOwnerStore
from errors import ReadError


MEMORY_ID_RE = re.compile(r"MEM-[0-9A-F]{8}")
RECEIPT_SCHEMA = "periodic_memory_mount_receipt.v2"


class PeriodicMemoryMountError(ValueError):
    """Stable local rejection; no provider call is involved."""

    def __init__(self, reason, *, http_status=409):
        self.reason = str(reason or "periodic_memory_mutation_rejected")
        self.http_status = int(http_status)
        super().__init__(self.reason)


class PeriodicMemoryMountProcessor:
    def __init__(
            self, *, memory_store, heat, assembler, config_store,
            mount_store=None, owner_store=None, instance_id="meta",
            now_fn=None, fault_hook=None):
        self.memory_store = memory_store
        self.heat = heat
        self.assembler = assembler
        self.config_store = config_store
        self.mount_store = mount_store or PeriodicMountStore()
        self.owner_store = owner_store or PeriodicPinOwnerStore()
        self.instance_id = str(instance_id or "meta")
        self.now_fn = now_fn or (lambda: local_now().isoformat())
        self.fault_hook = fault_hook
        self._character_limit = None

    def apply(self, action, mem_id):
        if action not in {"mount", "unmount"}:
            raise PeriodicMemoryMountError(
                "invalid_periodic_memory_action", http_status=400)
        if not isinstance(mem_id, str) or not MEMORY_ID_RE.fullmatch(mem_id):
            raise PeriodicMemoryMountError(
                "invalid_periodic_memory_id", http_status=400)

        with MEMORY_MUTATION_LOCK:
            mounts = self.mount_store.load()
            if mounts.get("schema_version") != PERIODIC_MOUNTS_SCHEMA:
                raise PeriodicMemoryMountError("periodic_mounts_legacy_read_only")
            recorded_instance = str(mounts.get("instance_id") or "")
            if recorded_instance and recorded_instance != self.instance_id:
                raise PeriodicMemoryMountError("periodic_mounts_instance_conflict")
            owners = self.owner_store.load()
            self._validate_local_ownership(mounts, owners)
            return (
                self._mount(mem_id, mounts, owners)
                if action == "mount"
                else self._unmount(mem_id, mounts, owners)
            )

    def mark_pending_blocked(self, mem_id, reason):
        """Keep a completed request retryable when final pinning fails."""
        with MEMORY_MUTATION_LOCK:
            mounts = self.mount_store.load()
            pending = list(mounts.get("pending_memory_items", []))
            found = False
            for item in pending:
                if item.get("id") != mem_id:
                    continue
                item["status"] = "mount_blocked"
                item["reason"] = str(reason or "periodic_mount_failed")
                found = True
                break
            if not found:
                raise PeriodicMemoryMountError("periodic_pending_not_found")
            mounts["pending_memory_items"] = pending
            self.mount_store.save_document(mounts)
            return pending

    def _mount(self, mem_id, mounts, owners):
        current_items = list(mounts["periodic_memory_items"])
        current_ids = [item["id"] for item in current_items]
        pending_items = list(mounts.get("pending_memory_items", []))
        pending = next((item for item in pending_items if item["id"] == mem_id), None)
        state = self._memory_state(mem_id)
        self._require_public_active(state)
        source_meta = state["ltm"]["meta"]

        owner_before = deepcopy(owners["entries"].get(mem_id))
        if mem_id in current_ids:
            if not owner_before or self.instance_id not in owner_before["owners"]:
                raise PeriodicMemoryMountError("periodic_mount_ownership_conflict")
            if not state["ltm"] or state["ltm"]["tier"] != "Pinned":
                raise PeriodicMemoryMountError("periodic_memory_not_pinned")
            chars = self._mounted_chars(current_items, enforce_limit=False)
            return self._receipt(
                "noop", "mount", mem_id,
                before=self._projection_state(state, True, owner_before),
                after=self._projection_state(state, True, owner_before),
                owners_before=owner_before["owners"],
                owners_after=owner_before["owners"],
                chars_before=chars, chars_after=chars,
                cache_invalidated=False,
            )

        aligned = self.is_alignment_ready(state)
        if not aligned:
            if pending:
                chars = self._mounted_chars(current_items, enforce_limit=False)
                return self._receipt(
                    "noop", "mount", mem_id,
                    before=self._projection_state(
                        state, False, owner_before, pending),
                    after=self._projection_state(
                        state, False, owner_before, pending),
                    owners_before=(owner_before or {}).get("owners", []),
                    owners_after=(owner_before or {}).get("owners", []),
                    chars_before=chars, chars_after=chars,
                    cache_invalidated=False,
                    mount_status=pending["status"], outcome="pending",
                    pending_reason=pending.get("reason", ""),
                )
            requested_at = self.now_fn()
            pending = {
                "id": mem_id,
                "source": "user_manual",
                "requested_at": requested_at,
                "status": "awaiting_completion",
                "reason": "memory_alignment_required",
            }
            snapshots = self._snapshots()
            cache_snapshot = self._snapshot_periodic_cache()
            try:
                mounts["instance_id"] = self.instance_id
                mounts["pending_memory_items"] = pending_items + [pending]
                self.mount_store.save_document(mounts)
                self._fault("after_pending")
            except Exception as exc:
                self._rollback(snapshots, cache_snapshot, exc)
                raise
            chars = self._mounted_chars(current_items, enforce_limit=False)
            return self._receipt(
                "applied", "mount", mem_id,
                before=self._projection_state(state, False, owner_before),
                after=self._projection_state(state, False, owner_before, pending),
                owners_before=(owner_before or {}).get("owners", []),
                owners_after=(owner_before or {}).get("owners", []),
                chars_before=chars, chars_after=chars,
                cache_invalidated=False,
                mount_status="awaiting_completion", outcome="pending",
                pending_reason="memory_alignment_required",
            )

        if owner_before and self.instance_id in owner_before["owners"]:
            raise PeriodicMemoryMountError("periodic_mount_ownership_conflict")
        if owner_before and (not state["ltm"] or state["ltm"]["tier"] != "Pinned"):
            raise PeriodicMemoryMountError("periodic_pin_owner_tier_conflict")

        candidate_text = project_periodic_memory_body(
            state["ltm"]["body"],
            source_meta,
        )
        chars_before = self._mounted_chars(current_items)
        chars_after = chars_before + len(candidate_text)
        limit = self._limit()
        if chars_after > limit:
            raise PeriodicMemoryMountError("periodic_memory_budget_exceeded")

        pin_source = (
            owner_before.get("pin_source") if owner_before
            else "preexisting" if state["ltm"] and state["ltm"]["tier"] == "Pinned"
            else "periodic"
        )
        owner_after = deepcopy(owner_before) if owner_before else {
            "pin_source": pin_source,
            "owners": [],
            "created_at": self.now_fn(),
            "alignment_verified": True,
        }
        owner_after["alignment_verified"] = True
        owner_after["owners"].append(self.instance_id)
        mounted_at = pending.get("requested_at") if pending else self.now_fn()
        new_item = {
            "id": mem_id,
            "source": "user_manual",
            "mounted_at": mounted_at,
        }
        new_items = current_items + [new_item]
        before_projection = self._projection_state(state, False, owner_before)
        snapshots = self._snapshots()
        cache_snapshot = self._snapshot_periodic_cache()
        verified_chars_after = chars_after
        try:
            from data.memory_compression_store import MemoryCompressionManager

            MemoryCompressionManager(
                memory_store=self.memory_store,
                instance_id=self.instance_id,
            ).cancel_for_pin(mem_id)
            self._fault("after_compression_cancel")
            self.memory_store.admit_ltm_entry(mem_id)
            self._fault("after_admission")
            if not state["ltm"] or state["ltm"]["tier"] != "Pinned":
                source = self.memory_store.ltm_entry_state(
                    mem_id, include_backup=False)
                self.memory_store.store_ltm_entry(
                    "Pinned", mem_id, source["body"], source["meta"],
                    source_tier=source["tier"],
                )
            self._fault("after_pinned")
            owners["entries"][mem_id] = owner_after
            self.owner_store.save_document(owners)
            self._fault("after_owners")
            mounts["instance_id"] = self.instance_id
            mounts["periodic_memory_items"] = new_items
            mounts["pending_memory_items"] = [
                item for item in pending_items if item["id"] != mem_id
            ]
            self.mount_store.save_document(mounts)
            self._fault("after_mounts")
            self.assembler.invalidate_layer("periodic", strict=True)
            self._fault("after_invalidate")
            final_state = self._memory_state(mem_id)
            self._verify_mount_applied(
                mem_id, final_state, new_items, owner_after)
            verified_chars_after = self._mounted_chars(new_items)
            self._fault("after_verify")
        except Exception as exc:
            self._rollback(snapshots, cache_snapshot, exc)
            raise

        return self._receipt(
            "applied", "mount", mem_id,
            before=before_projection,
            after=self._projection_state(final_state, True, owner_after),
            owners_before=(owner_before or {}).get("owners", []),
            owners_after=owner_after["owners"],
            chars_before=chars_before, chars_after=verified_chars_after,
            cache_invalidated=True,
            mount_status="mounted", outcome="mounted",
        )

    def _unmount(self, mem_id, mounts, owners):
        current_items = list(mounts["periodic_memory_items"])
        current_ids = [item["id"] for item in current_items]
        pending_items = list(mounts.get("pending_memory_items", []))
        pending = next((item for item in pending_items if item["id"] == mem_id), None)
        state = self._memory_state(mem_id)
        self._require_public_active(state)
        owner_before = deepcopy(owners["entries"].get(mem_id))
        if pending:
            snapshots = self._snapshots()
            cache_snapshot = self._snapshot_periodic_cache()
            try:
                mounts["instance_id"] = self.instance_id
                mounts["pending_memory_items"] = [
                    item for item in pending_items if item["id"] != mem_id
                ]
                self.mount_store.save_document(mounts)
                self._fault("after_pending_cancel")
            except Exception as exc:
                self._rollback(snapshots, cache_snapshot, exc)
                raise
            chars = self._mounted_chars(current_items, enforce_limit=False)
            return self._receipt(
                "applied", "unmount", mem_id,
                before=self._projection_state(state, False, owner_before, pending),
                after=self._projection_state(state, False, owner_before),
                owners_before=(owner_before or {}).get("owners", []),
                owners_after=(owner_before or {}).get("owners", []),
                chars_before=chars, chars_after=chars,
                cache_invalidated=False,
                mount_status="unmounted", outcome="pending_cancelled",
            )
        if mem_id not in current_ids:
            if owner_before and self.instance_id in owner_before["owners"]:
                raise PeriodicMemoryMountError("periodic_mount_ownership_conflict")
            chars = self._mounted_chars(current_items, enforce_limit=False)
            return self._receipt(
                "noop", "unmount", mem_id,
                before=self._projection_state(state, False, owner_before),
                after=self._projection_state(state, False, owner_before),
                owners_before=(owner_before or {}).get("owners", []),
                owners_after=(owner_before or {}).get("owners", []),
                chars_before=chars, chars_after=chars,
                cache_invalidated=False,
            )
        if (
            not owner_before
            or self.instance_id not in owner_before["owners"]
            or not state["ltm"]
            or state["ltm"]["tier"] != "Pinned"
        ):
            raise PeriodicMemoryMountError("periodic_mount_ownership_conflict")

        chars_before = self._mounted_chars(
            current_items, enforce_limit=False)
        new_items = [item for item in current_items if item["id"] != mem_id]
        chars_after = self._mounted_chars(new_items, enforce_limit=False)
        owner_after = deepcopy(owner_before)
        owner_after["owners"] = [
            owner for owner in owner_after["owners"]
            if owner != self.instance_id
        ]
        move_back = (
            not owner_after["owners"]
            and owner_after["pin_source"] == "periodic"
            and owner_after.get("alignment_verified") is True
        )
        target_tier = self._tier_for_weight(
            state["ltm"]["meta"].get("weight")) if move_back else "Pinned"
        before_projection = self._projection_state(state, True, owner_before)
        snapshots = self._snapshots()
        cache_snapshot = self._snapshot_periodic_cache()
        try:
            mounts["instance_id"] = self.instance_id
            mounts["periodic_memory_items"] = new_items
            self.mount_store.save_document(mounts)
            self._fault("after_mounts")
            if owner_after["owners"]:
                owners["entries"][mem_id] = owner_after
            else:
                owners["entries"].pop(mem_id, None)
            self.owner_store.save_document(owners)
            self._fault("after_owners")
            if move_back:
                self.memory_store.store_ltm_entry(
                    target_tier, mem_id,
                    state["ltm"]["body"], state["ltm"]["meta"],
                    source_tier="Pinned",
                )
            self._fault("after_pinned")
            self.assembler.invalidate_layer("periodic", strict=True)
            self._fault("after_invalidate")
            final_state = self._memory_state(mem_id)
            expected = target_tier if move_back else "Pinned"
            if not final_state["ltm"] or final_state["ltm"]["tier"] != expected:
                raise ValueError("periodic_unmount_tier_unverified")
            saved_mounts = self.mount_store.load()
            if mem_id in [
                    item["id"] for item in
                    saved_mounts["periodic_memory_items"]]:
                raise ValueError("periodic_unmount_unverified")
            if (
                saved_mounts.get("instance_id") != self.instance_id
                or saved_mounts["periodic_memory_items"] != new_items
            ):
                raise ValueError("periodic_unmount_ledger_unverified")
            saved_owner = self.owner_store.load()["entries"].get(mem_id)
            expected_owner = owner_after if owner_after["owners"] else None
            if saved_owner != expected_owner:
                raise ValueError("periodic_unmount_owner_unverified")
            self._fault("after_verify")
        except Exception as exc:
            self._rollback(snapshots, cache_snapshot, exc)
            raise

        return self._receipt(
            "applied", "unmount", mem_id,
            before=before_projection,
            after=self._projection_state(final_state, False, owner_after),
            owners_before=owner_before["owners"],
            owners_after=owner_after["owners"],
            chars_before=chars_before, chars_after=chars_after,
            cache_invalidated=True,
            mount_status="unmounted", outcome="unmounted",
        )

    def _memory_state(self, mem_id):
        stm = self.memory_store.stm_entry_state(mem_id)
        body_present = stm.get("body") is not None
        meta_present = stm.get("meta") is not None
        if body_present != meta_present:
            raise PeriodicMemoryMountError("stm_memory_incomplete")
        if body_present and stm.get("heat") is None:
            raise PeriodicMemoryMountError("stm_heat_missing")
        ltm = self.memory_store.ltm_entry_state(mem_id, include_backup=True)
        return {
            "stm": stm,
            "stm_present": body_present and meta_present,
            "ltm": ltm,
        }

    @staticmethod
    def _require_public_active(state):
        if state.get("stm_present") and str(
                state["stm"]["meta"].get("access") or "public"
        ).strip().lower() != "public":
            raise PeriodicMemoryMountError("private_memory_deferred")
        if state["ltm"] and state["ltm"]["tier"] == "Backup":
            raise PeriodicMemoryMountError("backup_memory_not_mountable")
        if state["ltm"] is None:
            raise PeriodicMemoryMountError(
                "periodic_memory_not_found", http_status=404)
        source_meta = state["ltm"]["meta"]
        if str(source_meta.get("access") or "public").strip().lower() != "public":
            raise PeriodicMemoryMountError("private_memory_deferred")

    def _validate_local_ownership(self, mounts, owners):
        for item in mounts.get("periodic_memory_items", []):
            entry = owners.get("entries", {}).get(item["id"])
            if not entry or self.instance_id not in entry.get("owners", []):
                raise PeriodicMemoryMountError("periodic_mount_ownership_conflict")

    @staticmethod
    def is_alignment_ready(state):
        ltm = state.get("ltm")
        if not ltm:
            raise PeriodicMemoryMountError("periodic_memory_not_found", http_status=404)
        tier = ltm.get("tier")
        if tier == "Pinned":
            weight = int(ltm.get("meta", {}).get("weight"))
            expected = "内容" if weight == 5 else "摘要" if weight >= 3 else "梗概"
            labels = re.findall(
                r"(?m)^\s*(?:\*\*)?(内容|摘要|正文|梗概)(?:\*\*)?[^：:]*[：:]",
                str(ltm.get("body") or ""),
            )
            if len(labels) != 1:
                raise PeriodicMemoryMountError("periodic_memory_semantic_shape_invalid")
            if labels[0] == expected or (expected == "梗概" and labels[0] == "正文"):
                return True
            raise PeriodicMemoryMountError("periodic_memory_alignment_conflict")
        if tier not in {"Full", "Summary", "Abstract"}:
            raise PeriodicMemoryMountError("periodic_memory_layer_invalid")
        target = memory_target_tier(ltm.get("meta", {}).get("weight"))
        if tier == target:
            return True
        rank = {"Abstract": 1, "Summary": 2, "Full": 3}
        if rank[tier] < rank[target]:
            return False
        raise PeriodicMemoryMountError("periodic_memory_alignment_conflict")

    def _mounted_chars(self, items, *, enforce_limit=True):
        ids = [item["id"] for item in items]
        if not ids:
            return 0
        by_id = {
            item["id"]: item
            for item in self.memory_store.list_public_ltm_entries()
        }
        used = 0
        for mem_id in ids:
            source = by_id.get(mem_id)
            if source is None:
                raise PeriodicMemoryMountError(f"periodic_memory_missing:{mem_id}")
            if source.get("memory_layer") != "LTM/Pinned":
                raise PeriodicMemoryMountError(f"periodic_memory_not_pinned:{mem_id}")
            used += len(project_periodic_memory_body(source["body"], source))
        if enforce_limit and used > self._limit():
            raise PeriodicMemoryMountError("periodic_memory_budget_exceeded")
        return used

    def _limit(self):
        if self._character_limit is not None:
            return self._character_limit
        try:
            value = self.config_store.get_periodic_limits()[
                "periodic_memory_items_chars"]
        except Exception as exc:
            raise PeriodicMemoryMountError("periodic_memory_limit_unavailable") from exc
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise PeriodicMemoryMountError("periodic_memory_limit_invalid")
        self._character_limit = value
        return self._character_limit

    def _verify_mount_applied(self, mem_id, state, expected_items, expected_owner):
        if not state["ltm"] or state["ltm"]["tier"] != "Pinned":
            raise ValueError("periodic_memory_not_pinned")
        saved_mounts = self.mount_store.load()
        if (
            saved_mounts.get("instance_id") != self.instance_id
            or saved_mounts["periodic_memory_items"] != expected_items
            or mem_id in {
                item["id"] for item in saved_mounts.get("pending_memory_items", [])
            }
        ):
            raise ValueError("periodic_mount_ledger_unverified")
        if self.owner_store.load()["entries"].get(mem_id) != expected_owner:
            raise ValueError("periodic_pin_owner_unverified")

    def _snapshots(self):
        return {
            "stm": self.memory_store.snapshot_stm_files(),
            "ltm": self.memory_store.snapshot_ltm_files(),
            "files": self._snapshot_files(
                (
                    self.mount_store.path,
                    self.owner_store.path,
                    paths.MEMORY_COMPRESSION_PENDING_JSON,
                )),
        }

    @staticmethod
    def _snapshot_files(paths):
        snapshot = {}
        for path in paths:
            if not os.path.isfile(path):
                snapshot[path] = None
                continue
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    snapshot[path] = handle.read()
            except OSError as exc:
                raise ReadError(path, cause=exc)
        return snapshot

    @staticmethod
    def _restore_files(snapshot):
        for path, text in snapshot.items():
            if text is None:
                if os.path.isfile(path):
                    os.remove(path)
            else:
                atomic_write_text(path, text)

    def _snapshot_periodic_cache(self):
        cache = {
            key: value for key, value in self.assembler._layer_cache.items()
            if key[1] == "periodic"
        }
        blocks = {
            key: deepcopy(value)
            for key, value in self.assembler._layer_block_cache.items()
            if key[1] == "periodic"
        }
        expired = None
        state_store = getattr(self.assembler, "state_store", None)
        if state_store is not None:
            state = state_store.load()
            expired = bool(
                ((state.get("base") or {}).get("context_cache") or {}).get(
                    "periodic_expired", True))
        return {"cache": cache, "blocks": blocks, "expired": expired}

    def _restore_periodic_cache(self, snapshot):
        for key in list(self.assembler._layer_cache):
            if key[1] == "periodic":
                del self.assembler._layer_cache[key]
        for key in list(self.assembler._layer_block_cache):
            if key[1] == "periodic":
                del self.assembler._layer_block_cache[key]
        self.assembler._layer_cache.update(snapshot["cache"])
        self.assembler._layer_block_cache.update(snapshot["blocks"])
        if snapshot["expired"] is not None:
            self.assembler.state_store._set_internal(
                "base.context_cache.periodic_expired", snapshot["expired"])

    def _rollback(self, snapshots, cache_snapshot, cause):
        try:
            self.memory_store.restore_stm_files(snapshots["stm"])
            self.memory_store.restore_ltm_files(snapshots["ltm"])
            self._restore_files(snapshots["files"])
            self._restore_periodic_cache(cache_snapshot)
        except Exception as restore_exc:
            raise RuntimeError(
                f"periodic_memory_rollback_failed:{type(restore_exc).__name__}"
            ) from cause

    def _projection_state(self, state, mounted, owner, pending=None):
        ltm_layer = (
            f"LTM/{state['ltm']['tier']}" if state.get("ltm") else "")
        layers = (["STM"] if state.get("stm_present") else [])
        if ltm_layer:
            layers.append(ltm_layer)
        return {
            "memory_layers": layers,
            "stm_present": bool(state.get("stm_present")),
            "ltm_layer": ltm_layer,
            "periodic_mounted": bool(mounted),
            "periodic_mount_status": (
                "mounted" if mounted else
                str((pending or {}).get("status") or "unmounted")
            ),
            "periodic_mount_reason": str((pending or {}).get("reason") or ""),
            "periodic_pin_owned": bool(
                mounted and owner and owner.get("pin_source") == "periodic"
                and self.instance_id in owner.get("owners", [])),
            "pin_source": str((owner or {}).get("pin_source") or ""),
        }

    def _receipt(
            self, status, action, mem_id, *, before, after,
            owners_before, owners_after, chars_before, chars_after,
            cache_invalidated, mount_status=None, outcome=None,
            pending_reason=""):
        return {
            "schema_version": RECEIPT_SCHEMA,
            "tool_id": "periodic_memory_mount",
            "receipt_id": uuid.uuid4().hex,
            "status": status,
            "action": action,
            "mem_id": mem_id,
            "instance_id": self.instance_id,
            "before": before,
            "after": after,
            "owners_before": list(owners_before),
            "owners_after": list(owners_after),
            "periodic_chars_before": int(chars_before),
            "periodic_chars_after": int(chars_after),
            "periodic_chars_limit": self._limit(),
            "cache_invalidated": bool(cache_invalidated),
            "mount_status": str(
                mount_status or after.get("periodic_mount_status") or "unmounted"),
            "outcome": str(outcome or status),
            "pending_reason": str(
                pending_reason or after.get("periodic_mount_reason") or ""),
            "provider_called": False,
            "recall_applied": False,
            "recorded_at": self.now_fn(),
        }

    def _fault(self, stage):
        if callable(self.fault_hook):
            self.fault_hook(stage)

    @staticmethod
    def _tier_for_weight(value):
        try:
            weight = int(value)
        except (TypeError, ValueError):
            weight = 0
        if weight >= 5:
            return "Full"
        if weight >= 3:
            return "Summary"
        return "Abstract"
