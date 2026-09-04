"""Atomic public-memory recall and reconsolidation-trigger lifecycle."""

from copy import deepcopy

import paths as runtime_paths
from constants import local_now
from data.memory_store import (
    MEMORY_MUTATION_LOCK,
    MEMORY_OVERLAY_FIELDS,
    _normalise_meta_entry,
    _read_memory_overlay,
    project_memory_body,
    write_memory_overlay_entry,
)
from errors import EntryNotFoundError


RECALL_MUTATION_LOCK = MEMORY_MUTATION_LOCK


class MemoryRecallError(RuntimeError):
    """Stable fail-closed error raised before a body can be mounted."""


class MemoryRecallProcessor:
    """Synchronize canonical memory truth into current-branch STM atomically."""

    def __init__(
            self, *, memory_store, heat,
            now_fn=local_now, instance_id=None, fault_hook=None,
            assembler=None):
        self.memory_store = memory_store
        self.heat = heat
        self.now_fn = now_fn
        self.instance_id = str(
            instance_id or runtime_paths.ACTIVE_INSTANCE_ID or "meta"
        )
        self.fault_hook = fault_hook
        self.assembler = assembler

    def recall(
            self, mem_id, *, round_num=None, boosted_ids=None,
            reconsolidation_tracker=None, periodic_requested=False,
            transaction_commit=None, transaction_rollback=None):
        """Recall one public memory and return an auditable lifecycle result."""
        return self._apply(
            mem_id,
            round_num=round_num,
            boosted_ids=boosted_ids,
            reconsolidation_tracker=reconsolidation_tracker,
            periodic_requested=periodic_requested,
            transaction_commit=transaction_commit,
            transaction_rollback=transaction_rollback,
        )

    def inspect(self, mem_id):
        """Return validated canonical and current-branch residence state."""
        ltm = self.memory_store.ltm_entry_state(mem_id, include_backup=True)
        stm = self.memory_store.stm_entry_state(mem_id)
        parts = (
            stm.get("body") is not None,
            stm.get("meta") is not None,
            stm.get("heat") is not None,
        )
        if ltm is not None and ltm.get("tier") == "Backup":
            raise MemoryRecallError("backup_memory_not_active")
        if ltm is not None:
            if any(parts):
                if not all(parts):
                    raise MemoryRecallError("stm_ltm_residence_incomplete")
                if str(
                        stm["meta"].get("access") or "public"
                ).strip().lower() != "public":
                    raise MemoryRecallError("stm_ltm_access_conflict")
            source_meta = dict(ltm["meta"])
            if str(source_meta.get("access") or "public").strip().lower() != "public":
                raise MemoryRecallError("private_memory_deferred")
            return {
                "source": "ltm",
                "source_memory_layer": ltm["memory_layer"],
                "ltm": ltm,
                "stm": stm,
                "stm_present": all(parts),
                "body": ltm["body"],
                "meta": source_meta,
            }
        if not any(parts):
            raise EntryNotFoundError(mem_id)
        if not all(parts):
            raise MemoryRecallError("stm_residence_incomplete")
        raise MemoryRecallError("stm_without_ltm_canonical_truth")

    def _apply(
            self, mem_id, *, round_num, boosted_ids,
            reconsolidation_tracker, periodic_requested,
            transaction_commit, transaction_rollback):
        clean_id = str(mem_id or "").strip()
        if not clean_id:
            raise MemoryRecallError("missing_mem_id")
        ledger = boosted_ids if isinstance(boosted_ids, set) else set()

        with RECALL_MUTATION_LOCK:
            from data.memory_compression_store import MemoryCompressionManager

            compression_manager = MemoryCompressionManager(
                memory_store=self.memory_store,
                now_fn=self.now_fn,
                instance_id=self.instance_id,
            )
            pre_stm_snapshot = self.memory_store.snapshot_stm_files()
            pre_ltm_snapshot = self.memory_store.snapshot_ltm_files()
            compression_ledger_snapshot = compression_manager.ledger.snapshot()
            state = self.inspect(clean_id)
            source_meta = dict(state["meta"])
            weight = self._weight(source_meta.get("weight"))
            target_body = state["body"]
            target_tier = state["ltm"]["tier"]

            stm_snapshot = pre_stm_snapshot
            ltm_snapshot = pre_ltm_snapshot
            heat_boost = clean_id not in ledger
            now = self._now()
            decay_countdown_before = source_meta.get("decay_countdown_days")
            decay_countdown_after = decay_countdown_before
            ltm_decay_reset_applied = False
            try:
                compression_override = compression_manager.cancel_for_recall(
                    clean_id)
                state = self.inspect(clean_id)
                source_meta = dict(state["meta"])
                self._fault("before_write")
                if state["source"] == "ltm" and state["stm_present"]:
                    write_memory_overlay_entry(clean_id, state["stm"]["meta"])
                    self._fault("after_overlay")

                updated_shared = dict(source_meta)
                updated_shared["last_recalled_at"] = now
                if round_num is not None:
                    updated_shared["last_recalled_round"] = round_num
                updated_shared["last_recalled_instance_id"] = self.instance_id
                if (
                    state["source"] == "ltm"
                    and target_tier in {"Full", "Summary", "Abstract"}
                    and str(updated_shared.get("stored_at") or "").strip()
                ):
                    period = updated_shared.get("decay_period_days")
                    if (
                        not isinstance(period, int)
                        or isinstance(period, bool)
                        or period <= 0
                    ):
                        raise MemoryRecallError("invalid_decay_period_days")
                    updated_shared["decay_countdown_days"] = period
                    decay_countdown_after = period
                    ltm_decay_reset_applied = True

                if self.assembler is not None:
                    prospective_meta = dict(updated_shared)
                    if state["source"] == "ltm":
                        overlay = _read_memory_overlay()["entries"].get(
                            clean_id, {})
                        prospective_meta.update({
                            key: overlay.get(
                                key, [] if key == "linked_containers" else ""
                            )
                            for key in MEMORY_OVERLAY_FIELDS
                        })
                    prospective_meta = _normalise_meta_entry(prospective_meta)
                    self.assembler.preflight_resident_source_update(
                        {"item_type": "memory", "item_id": clean_id},
                        project_memory_body(target_body, prospective_meta),
                    )

                if state["source"] == "ltm":
                    tier = state["ltm"]["tier"]
                    self.memory_store.replace_ltm_entry(
                        tier, clean_id, target_body, updated_shared)
                    canonical = self.memory_store.ltm_entry_state(
                        clean_id, include_backup=False)
                    target_body = canonical["body"]
                    updated_shared = dict(canonical["meta"])
                    self._fault("after_ltm_write")

                branch_meta = dict(updated_shared)
                if state["source"] == "ltm":
                    overlay = _read_memory_overlay()["entries"].get(clean_id, {})
                    branch_meta.update({
                        key: overlay.get(
                            key, [] if key == "linked_containers" else ""
                        )
                        for key in MEMORY_OVERLAY_FIELDS
                    })
                branch_meta = _normalise_meta_entry(branch_meta)

                heat_entry = self._next_heat(
                    state, weight=weight, now=now, boost=heat_boost)
                self.memory_store.replace_stm_body(clean_id, target_body)
                self._fault("after_stm_body")
                self.memory_store.replace_stm_meta(
                    clean_id, branch_meta, canonical_sync=True)
                self._fault("after_stm_meta")
                self.memory_store.rebuild_stm_index()
                self._fault("after_stm_index")
                self.memory_store.rebuild_stm_keywords()
                self._fault("after_stm_keywords")
                self.heat.set_entry(clean_id, heat_entry)
                self._fault("after_stm_heat")

                self._verify(
                    clean_id,
                    source_layer=f"LTM/{target_tier}",
                    body=target_body,
                    meta=branch_meta,
                    heat=heat_entry,
                    expected_decay_countdown=(
                        decay_countdown_after
                        if ltm_decay_reset_applied else None
                    ),
                )
                self._fault("after_verify")
                if callable(transaction_commit):
                    transaction_commit()
                    self._fault("after_transaction_commit")
                reconsolidation_item = None
                if reconsolidation_tracker is not None:
                    reconsolidation_item = reconsolidation_tracker.register(
                        self.inspect(clean_id),
                        periodic_requested=periodic_requested,
                    )
            except Exception as exc:
                external_rollback_error = None
                if callable(transaction_rollback):
                    try:
                        transaction_rollback()
                    except Exception as rollback_exc:
                        external_rollback_error = rollback_exc
                self._rollback(
                    stm_snapshot, ltm_snapshot, exc,
                    compression_manager=compression_manager,
                    compression_ledger_snapshot=compression_ledger_snapshot,
                )
                if external_rollback_error is not None:
                    raise RuntimeError(
                        "memory_recall_external_rollback_failed:"
                        f"{type(external_rollback_error).__name__}"
                    ) from exc
                raise

            if heat_boost:
                ledger.add(clean_id)
            return {
                "schema_version": "memory_recall_lifecycle_receipt.v1",
                "status": "applied",
                "mem_id": clean_id,
                "memory_layer": f"LTM/{target_tier}",
                "source_memory_layer": state["source_memory_layer"],
                "target_memory_layer": f"LTM/{target_tier}",
                "stm_present": True,
                "stm_created": not state["stm_present"],
                "heat_boost_applied": bool(heat_boost),
                "heat_boost_deduplicated": not heat_boost,
                "ltm_decay_reset_applied": ltm_decay_reset_applied,
                "ltm_decay_countdown_before": decay_countdown_before,
                "ltm_decay_countdown_after": decay_countdown_after,
                "resolved_meta": dict(branch_meta),
                "completed": False,
                "completed_body_chars": None,
                "completed_body_limit": None,
                "periodic_cache_invalidated": False,
                "memory_compression_override": compression_override,
                "reconsolidation_required": bool(reconsolidation_item),
                "reconsolidation_guide_id": (
                    reconsolidation_tracker.guide_id
                    if reconsolidation_item is not None else ""
                ),
            }

    def _next_heat(self, state, *, weight, now, boost):
        existing = state["stm"].get("heat")
        info = deepcopy(existing) if isinstance(existing, dict) else self.heat.new_entry(
            weight=weight)
        info["AH_low"] = 0
        info["degrade"] = False
        info.setdefault("heat_locked", False)
        if not boost:
            if info.get("heat_locked"):
                self.heat._apply_heat_lock(info)
            return info
        if info.get("heat_locked"):
            self.heat._apply_heat_lock(info)
            new_h = info.get("H", 0)
        else:
            new_h, ah_high, zone = self.heat.calculator.recall_boost(
                info.get("H", 0), info.get("AH_high", 0))
            info.update(H=new_h, AH_high=ah_high, zone=zone)
        info["last_heat_at"] = now
        significant = self.heat.config["zone_thresholds"]["significant"]
        if new_h >= significant:
            info["last_high_at"] = now
        return info

    def _verify(
            self, mem_id, *, source_layer, body, meta, heat,
            expected_decay_countdown=None):
        stm = self.memory_store.stm_entry_state(mem_id)
        if (
            stm.get("body") != body
            or _normalise_meta_entry(stm.get("meta") or {}) != meta
            or stm.get("heat") != heat
        ):
            raise MemoryRecallError("stm_rehydration_unverified")
        if source_layer.startswith("LTM/"):
            ltm = self.memory_store.ltm_entry_state(mem_id, include_backup=False)
            if ltm is None or ltm.get("memory_layer") != source_layer:
                raise MemoryRecallError("ltm_truth_unverified")
            if (
                expected_decay_countdown is not None
                and ltm.get("meta", {}).get("decay_countdown_days")
                != expected_decay_countdown
            ):
                raise MemoryRecallError("ltm_decay_reset_unverified")

    def _rollback(
            self, stm_snapshot, ltm_snapshot, cause,
            *, compression_manager=None, compression_ledger_snapshot=None):
        try:
            self.memory_store.restore_stm_files(stm_snapshot)
            self.memory_store.restore_ltm_files(ltm_snapshot)
            if compression_manager is not None:
                compression_manager.ledger.restore(
                    compression_ledger_snapshot)
        except Exception as restore_exc:
            raise RuntimeError(
                "memory_recall_rollback_failed:"
                f"{type(restore_exc).__name__}"
            ) from cause

    def _fault(self, stage):
        if callable(self.fault_hook):
            self.fault_hook(stage)

    def _now(self):
        value = self.now_fn()
        return value.isoformat() if hasattr(value, "isoformat") else str(value)

    @staticmethod
    def _weight(value):
        if isinstance(value, bool):
            raise MemoryRecallError("invalid_memory_weight")
        try:
            weight = int(value)
        except (TypeError, ValueError) as exc:
            raise MemoryRecallError("invalid_memory_weight") from exc
        if weight < 0 or weight > 5:
            raise MemoryRecallError("invalid_memory_weight")
        return weight
