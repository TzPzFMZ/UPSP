"""Reaction-owned memory and association metabolism."""

import os

import paths
from data.memory_compression_store import MemoryCompressionManager
from data.training_material_store import write_association_counts
from engines.runtime_services import EngineComponent
from logic.state_settlement import (
    StateSettlementError,
    commit_reaction_entry,
    prepare_reaction_entry,
)


TERMINAL_RECEIPT_SCHEMA = "reaction_terminal_metabolism_receipt.v1"
EMPTY_CELLS = frozenset({"", "无", "—", "-"})


def _pairwise(values):
    values = [
        str(value or "").strip()
        for value in values or []
        if str(value or "").strip()
    ]
    return [
        (left, right)
        for index, left in enumerate(values)
        for right in values[index + 1:]
        if left != right
    ]


def build_association_counts_from_receipts(memory_write_receipts):
    """Build the five deterministic association tables from applied writes."""
    counts = {
        "assoc_kw_kw": [],
        "assoc_kw_ifeel": [],
        "assoc_kw_rfeel": [],
        "assoc_ifeel_rfeel": [],
        "assoc_object_rfeel": [],
    }
    for receipt in memory_write_receipts or []:
        if receipt.get("status") != "applied":
            continue
        keywords = list(dict.fromkeys(receipt.get("keywords") or []))
        interaction_feelings = list(dict.fromkeys(
            receipt.get("interaction_feelings") or []))
        relationship_feelings = []
        for item in receipt.get("relationship_feelings") or []:
            if not isinstance(item, dict):
                continue
            subject = str(item.get("subject") or "").strip()
            feeling = str(item.get("word") or "").strip()
            if subject and feeling and (subject, feeling) not in relationship_feelings:
                relationship_feelings.append((subject, feeling))
        if not relationship_feelings:
            legacy_subject = str(receipt.get("subject") or "").strip()
            relationship_feelings = [
                (legacy_subject, str(feeling or "").strip())
                for feeling in receipt.get("relation_feelings") or []
                if legacy_subject and str(feeling or "").strip()
            ]
        relation_feelings = list(dict.fromkeys(
            feeling for _subject, feeling in relationship_feelings))
        counts["assoc_kw_kw"].extend(_pairwise(keywords))
        counts["assoc_kw_ifeel"].extend(
            (keyword, feeling)
            for keyword in keywords
            for feeling in interaction_feelings
        )
        counts["assoc_kw_rfeel"].extend(
            (keyword, feeling)
            for keyword in keywords
            for feeling in relation_feelings
        )
        counts["assoc_ifeel_rfeel"].extend(
            (interaction, relation)
            for interaction in interaction_feelings
            for relation in relation_feelings
        )
        counts["assoc_object_rfeel"].extend(
            (subject, feeling)
            for subject, feeling in relationship_feelings
            if subject not in EMPTY_CELLS
        )
    return {key: value for key, value in counts.items() if value}


class MemoryLifecycleSettlementError(RuntimeError):
    """Carries the truthful per-item cursor when a lifecycle batch fails."""

    def __init__(self, message, receipts):
        super().__init__(message)
        self.receipts = list(receipts)


class ReactionMetabolism(EngineComponent):
    """Deterministic terminal metabolism; Cleanup must not call this class."""

    @staticmethod
    def mark_exception(context, result, last_phase, exc):
        if getattr(context, "reaction_entry_metabolism_failed", False):
            result["_reaction_entry_metabolism_failed"] = True
        elif last_phase == "reaction" and isinstance(exc, StateSettlementError):
            result["_reaction_frame_metabolism_failed"] = True

    def prepare_entry(self, context):
        if str(context.round_type or "").strip().lower() == "standby":
            return None
        try:
            preparation = prepare_reaction_entry(
                self.sm,
                self.relation_store,
                self._get_round_audit_store(),
                context.round_num,
                context.round_type,
                external_interaction=bool(
                    context.trigger and context.trigger.messages
                ),
            )
            context.reaction_entry_state_settle_preparation = preparation
            context.state = preparation["preview_state"]
            return preparation
        except Exception:
            context.reaction_entry_metabolism_failed = True
            raise

    def commit_entry(self, context):
        if str(context.round_type or "").strip().lower() == "standby":
            return None
        try:
            receipt = commit_reaction_entry(
                self.sm,
                self.relation_store,
                self._get_round_audit_store(),
                context.round_num,
                context.round_type,
                getattr(context, "reaction_entry_state_settle_preparation", None),
            )
            context.reaction_entry_state_settle_receipt = receipt
            context.state = self.sm.load()
            return receipt
        except Exception:
            context.reaction_entry_metabolism_failed = True
            raise

    def settle_result(self, context, result, setup_result):
        result["_setup_messages"] = setup_result.setup_messages
        result["_reaction_entry_state_settle_receipt"] = getattr(
            context, "reaction_entry_state_settle_receipt", None
        )
        result["_user_input_text"] = setup_result.user_input_text
        context.interaction_meta = result.get(
            "_interaction_meta", context.interaction_meta
        )
        result["_interaction_meta"] = context.interaction_meta
        terminal_kind = self.terminal_kind(
            result, round_type=context.round_type
        )
        if not terminal_kind:
            return context.interaction_meta
        try:
            result["_reaction_terminal_metabolism_receipt"] = self.settle_terminal(
                context.round_num,
                result.get("_memory_write_receipts", []),
                terminal_kind=terminal_kind,
            )
        except Exception as exc:
            result.update(
                _reaction_terminal_metabolism_failed=True,
                _failed_phase="reaction",
                aborted=True,
                error=(
                    "reaction terminal metabolism failed: "
                    f"{type(exc).__name__}:{exc}"
                ),
            )
        return context.interaction_meta

    @staticmethod
    def terminal_kind(result, *, round_type=""):
        result = result if isinstance(result, dict) else {}
        if (
            str(round_type or "").strip().lower() == "standby"
            or result.get("_user_stop_requested")
            or result.get("_provider_call_hard_stop")
            or result.get("_required_context_failure")
            or result.get("_single_round_probe_hard_stop")
        ):
            return ""
        if any(
            str(item.get("closeout_decision") or "").strip() == "blocked"
            for item in result.get("_settlement_ledgers", [])
            if isinstance(item, dict)
        ):
            return "terminal_blocked"
        if (
            str(result.get("_exit_signal") or "") == "continue_requested"
            and result.get("_reaction_finalize_validated")
        ):
            return "continue_requested"
        if result.get("_final_reply_done") or str(result.get("response") or "").strip():
            return "natural_final"
        return ""

    def _existing_terminal_receipt(self, round_num):
        try:
            events = self._get_round_audit_store().read_events(round_num)
        except FileNotFoundError:
            return None
        for event in reversed(events):
            payload = event.get("payload") if isinstance(event, dict) else None
            if (
                event.get("event_type") == "reaction_terminal_metabolism_receipt"
                and isinstance(payload, dict)
                and payload.get("status") == "applied"
            ):
                return payload
        return None

    def settle_terminal(self, round_num, memory_write_receipts, *, terminal_kind):
        existing = self._existing_terminal_receipt(round_num)
        if existing is not None:
            return existing

        steps = []
        lifecycle_receipts = []
        try:
            changed = self.heat.tick_decay(round_num=round_num)
            steps.append({"step": "heat_decay", "status": "applied", "changed": bool(changed)})

            try:
                forgetting = self.process_forgetting(round_num)
            except MemoryLifecycleSettlementError as exc:
                lifecycle_receipts.extend(exc.receipts)
                raise
            lifecycle_receipts.extend(forgetting)
            steps.append({
                "step": "stm_forgetting",
                "status": "applied",
                "receipt_count": len(forgetting),
            })

            try:
                admissions = self.process_admission(round_num)
            except MemoryLifecycleSettlementError as exc:
                lifecycle_receipts.extend(exc.receipts)
                raise
            lifecycle_receipts.extend(admissions)
            steps.append({
                "step": "memory_admission",
                "status": "applied",
                "receipt_count": len(admissions),
            })

            association_counts = build_association_counts_from_receipts(
                memory_write_receipts
            )
            association_snapshot = self._association_snapshot()
            try:
                write_association_counts(paths.ASSOCIATION_SET_DIR, association_counts)
                steps.append({
                    "step": "association_counts",
                    "status": "applied",
                    "table_count": len(association_counts),
                })
                receipt = {
                    "schema_version": TERMINAL_RECEIPT_SCHEMA,
                    "status": "applied",
                    "round": int(round_num),
                    "terminal_kind": str(terminal_kind),
                    "steps": steps,
                    "memory_lifecycle_receipts": lifecycle_receipts,
                }
                self._get_round_audit_store().append_event(
                    round_num,
                    "reaction_terminal_metabolism_receipt",
                    receipt,
                    phase="reaction",
                )
            except Exception:
                self._restore_association_snapshot(association_snapshot)
                raise
            return receipt
        except Exception as exc:
            error_receipt = {
                "schema_version": TERMINAL_RECEIPT_SCHEMA,
                "status": "error",
                "round": int(round_num),
                "terminal_kind": str(terminal_kind),
                "steps": steps,
                "memory_lifecycle_receipts": lifecycle_receipts,
                "reason": f"{type(exc).__name__}:{exc}",
            }
            try:
                self._get_round_audit_store().append_event(
                    round_num,
                    "reaction_terminal_metabolism_receipt",
                    error_receipt,
                    phase="reaction",
                )
            except Exception:
                pass
            raise RuntimeError(error_receipt["reason"]) from exc

    @staticmethod
    def _association_paths():
        return [
            os.path.join(paths.ASSOCIATION_SET_DIR, name)
            for name in (
                "assoc_kw_kw.json",
                "assoc_kw_ifeel.json",
                "assoc_kw_rfeel.json",
                "assoc_ifeel_rfeel.json",
                "assoc_object_rfeel.json",
            )
        ]

    def _association_snapshot(self):
        snapshot = {}
        for path in self._association_paths():
            if os.path.isfile(path):
                with open(path, "rb") as handle:
                    snapshot[path] = handle.read()
            else:
                snapshot[path] = None
        return snapshot

    @staticmethod
    def _restore_association_snapshot(snapshot):
        for path, content in snapshot.items():
            if content is None:
                if os.path.isfile(path):
                    os.remove(path)
                continue
            os.makedirs(os.path.dirname(path), exist_ok=True)
            tmp = path + ".rollback"
            with open(tmp, "wb") as handle:
                handle.write(content)
            os.replace(tmp, path)

    def process_admission(self, round_num, settlement_result=None):
        candidates = self.heat.check_upgrade()
        receipts = []
        failures = []
        for mem_id in candidates:
            try:
                source = self.memory_store.read_stm_meta_by_id(mem_id)
                if str(source.get("access") or "public").strip().lower() != "public":
                    raise ValueError("private_memory_deferred")
                ltm = self.memory_store.ltm_entry_state(
                    mem_id, include_backup=False
                )
                if ltm is None:
                    raise ValueError("ltm_canonical_truth_missing")
                if int(ltm["meta"].get("weight")) != int(source.get("weight")):
                    raise ValueError("memory_weight_conflict")
                admission = self.memory_store.admit_ltm_entry(mem_id)
                receipts.append({
                    "event": "memory_lifecycle_stored",
                    "status": "stored",
                    "mem_id": mem_id,
                    "tier": ltm["tier"],
                    "stored_at": admission["stored_at"],
                    "stm_retained": True,
                })
            except Exception as exc:
                failures.append(mem_id)
                receipts.append({
                    "event": "memory_lifecycle_failed",
                    "status": "failed",
                    "mem_id": mem_id,
                    "tier": "Full",
                    "reason": f"{type(exc).__name__}:{exc}",
                })
        if isinstance(settlement_result, dict) and receipts:
            settlement_result.setdefault("_memory_lifecycle_receipts", []).extend(receipts)
        if failures:
            raise MemoryLifecycleSettlementError(
                "memory_lifecycle_failed:" + ",".join(failures), receipts
            )
        return receipts

    def process_forgetting(self, round_num, settlement_result=None):
        to_delete, to_abstract, need_compress = self._forgetting_candidates()
        ordered = list(dict.fromkeys(
            list(to_delete) + list(to_abstract) + list(need_compress)
        ))
        manager = MemoryCompressionManager(
            memory_store=self.memory_store,
            instance_id=getattr(self.memory_store, "instance_id", None),
        )
        receipts = []
        failures = []
        for mem_id in ordered:
            try:
                settled = manager.settle_stm_forgetting(
                    mem_id, round_num=round_num
                )
                receipts.append({
                    "event": "memory_lifecycle_settled",
                    "status": "applied",
                    **settled,
                })
            except Exception as exc:
                failures.append(mem_id)
                receipts.append({
                    "event": "memory_lifecycle_failed",
                    "status": "failed",
                    "mem_id": mem_id,
                    "reason": f"{type(exc).__name__}:{exc}",
                })
        if isinstance(settlement_result, dict) and receipts:
            settlement_result.setdefault("_memory_lifecycle_receipts", []).extend(receipts)
        if failures:
            raise MemoryLifecycleSettlementError(
                "memory_lifecycle_failed:" + ",".join(failures), receipts
            )
        return receipts

    def _forgetting_candidates(self):
        from data.stm_heat_calculator import STMHeatCalculator
        from logic.memory_privacy import MEMORY_PRIVACY_ENABLED

        loader = getattr(self.heat, "load_heat", None)
        heat = loader() if callable(loader) else {
            "entries": dict(getattr(self.heat, "entries", {}) or {})
        }
        entries = heat.get("entries", {})
        canonical_meta = self.memory_store.active_ltm_meta_by_id()
        public_entries = {}
        public_meta = {}
        for mem_id, heat_entry in entries.items():
            if not heat_entry.get("degrade"):
                continue
            meta_entry = canonical_meta.get(mem_id)
            if not isinstance(meta_entry, dict):
                raise ValueError(f"ltm_canonical_truth_missing:{mem_id}")
            access = str(meta_entry.get("access") or "public").strip().lower()
            if MEMORY_PRIVACY_ENABLED or access != "private":
                public_entries[mem_id] = heat_entry
                public_meta[mem_id] = meta_entry
        calculator = getattr(self.heat, "calculator", None) or STMHeatCalculator()
        return calculator.process_forgetting(public_entries, public_meta)
