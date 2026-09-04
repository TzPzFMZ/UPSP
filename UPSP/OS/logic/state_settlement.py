"""Seed Reaction 入口、Frame 与 idle timer 的主体状态结算。"""

from copy import deepcopy
from datetime import datetime
import json
import os
import re

from constants import local_now
from constants import DYNAMIC_AXIS_KEYS, RELATION_AXIS_KEYS
from data.atomic_write import atomic_write_json
from logic.feeling_buffer import (
    collect_receipt_effects,
    earliest_settle_at,
    settle_pending,
)
from logic.gravity import apply_dynamic, clamp, core_gravity, relation_gravity
from logic.workhood import compute_workhood, speed_wheel_max
from paths import STATE_SETTLEMENT_JOURNAL_JSON


PLAN_SCHEMA = "state_settle_plan.v1"
RECEIPT_SCHEMA = "state_settle_receipt.v2"
LOCAL_PLAN_SCHEMA = "state_settle_local_plan.v1"
LOCAL_RECEIPT_SCHEMA = "state_settle_local_receipt.v1"
LOCAL_TRANSACTION_SCHEMA = "state_settle_local_transaction.v1"
ENTRY_PREPARATION_SCHEMA = "state_settle_entry_preparation.v1"


class StateSettlementError(RuntimeError):
    def __init__(self, receipt):
        self.receipt = receipt
        super().__init__(str(receipt.get("reason") or "state_settlement_failed"))


def _raise_settlement_error(round_store, round_num, settlement_id, cause,
                            relation_receipts=None, settlement_scope=None):
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "status": "error",
        "settlement_id": settlement_id,
        "round": int(round_num),
        "settlement_scope": settlement_scope or "legacy_round",
        "reason": f"{type(cause).__name__}:{cause}",
        "relations": list(relation_receipts or []),
    }
    try:
        round_store.append_event(
            round_num, "state_settle_receipt", receipt, phase="reaction")
    except Exception as audit_exc:
        receipt["audit_write_error"] = (
            f"{type(audit_exc).__name__}:{audit_exc}")
    raise StateSettlementError(receipt) from cause


def _merge_axes(target, incoming):
    for axis, delta in (incoming or {}).items():
        if delta:
            target[axis] = target.get(axis, 0) + delta


def _merge_relations(target, incoming):
    for subject, axes in (incoming or {}).items():
        _merge_axes(target.setdefault(subject, {}), axes)


def _axis_values(dynamic_axes):
    return {
        axis: {"value": dynamic_axes.get(axis, {}).get("value", 0)}
        for axis in DYNAMIC_AXIS_KEYS
    }


def _state_slice(state):
    base = state.get("base", {})
    meta = base.get("meta", {})
    return {
        "core_axes": deepcopy(base.get("core_axes", {})),
        "dynamic_axes": _axis_values(base.get("dynamic_axes", {})),
        "comfort_zone": deepcopy(base.get("comfort_zone", {})),
        "workhood_index": deepcopy(base.get("workhood_index", {})),
        "core_speed_wheel": deepcopy(base.get("core_speed_wheel", {})),
        "feeling_buffer": deepcopy(base.get("feeling_buffer", [])),
        "feeling_settle_due": bool(
            base.get("heartbeat_flags", {}).get("feeling_settle_due")
        ),
        "next_settle_at": meta.get("next_settle_at"),
        "last_state_settlement_id": meta.get("last_state_settlement_id"),
    }


def _apply_state_slice(state, values, *, clear_due=True):
    base = state.setdefault("base", {})
    meta = base.setdefault("meta", {})
    for key in (
        "core_axes", "dynamic_axes", "comfort_zone", "workhood_index",
        "core_speed_wheel", "feeling_buffer",
    ):
        base[key] = deepcopy(values[key])
    meta["next_settle_at"] = values.get("next_settle_at")
    meta["last_state_settlement_id"] = values.get(
        "last_state_settlement_id")
    flags = base.setdefault("heartbeat_flags", {})
    if clear_due:
        flags["feeling_settle_due"] = False


def _existing_plan(round_store, round_num, settlement_id):
    try:
        events = round_store.read_events(round_num)
    except FileNotFoundError:
        return None
    for event in reversed(events):
        payload = event.get("payload") if isinstance(event, dict) else None
        if (
            event.get("event_type") == "state_settle_plan"
            and isinstance(payload, dict)
            and payload.get("settlement_id") == settlement_id
        ):
            return payload
    return None


def _existing_receipt(round_store, round_num, settlement_id):
    try:
        events = round_store.read_events(round_num)
    except FileNotFoundError:
        return None
    for event in reversed(events):
        payload = event.get("payload") if isinstance(event, dict) else None
        if (
            event.get("event_type") == "state_settle_receipt"
            and isinstance(payload, dict)
            and payload.get("settlement_id") == settlement_id
            and payload.get("status") in {"applied", "already_applied"}
        ):
            return payload
    return None


def _build_plan(state, relation_store, round_num, round_type, receipts,
                interactive_round, observed_at, settlement_id=None,
                schema_version=PLAN_SCHEMA, settlement_scope="legacy_round",
                settle_buffer=True, natural_return=True):
    settlement_id = settlement_id or f"SS-R{int(round_num):06d}"
    incoming = collect_receipt_effects(receipts, observed_at=observed_at)
    if settle_buffer:
        due = settle_pending(
            state.get("base", {}).get("feeling_buffer", []),
            interactive_round=interactive_round,
            observed_at=observed_at,
        )
    else:
        due = {
            "dynamic": {},
            "relations": {},
            "remaining": deepcopy(
                state.get("base", {}).get("feeling_buffer", [])),
            "settled": [],
        }
    dynamic_deltas = dict(incoming["dynamic"])
    relation_deltas = deepcopy(incoming["relations"])
    _merge_axes(dynamic_deltas, due["dynamic"])
    _merge_relations(relation_deltas, due["relations"])

    relation_plans = []
    relation_after = {}
    for subject in sorted(relation_deltas):
        if relation_store.resolve_active_subject(subject) != subject:
            raise ValueError(f"subject_not_active_for_settlement:{subject}")
        card = relation_store.read_card(subject)
        if card is None:
            raise ValueError(f"relation_card_missing:{subject}")
        before_axes = {
            axis: int(card.get("axes", {}).get(axis, 0))
            for axis in RELATION_AXIS_KEYS
        }
        after_axes = {
            axis: int(clamp(
                before_axes[axis] + relation_deltas[subject].get(axis, 0)))
            for axis in RELATION_AXIS_KEYS
        }
        relation_after[subject] = after_axes
        relation_plans.append({
            "subject": subject,
            "before": before_axes,
            "after": after_axes,
        })

    after_state = deepcopy(state)
    base = after_state.setdefault("base", {})
    core_axes = deepcopy(base.get("core_axes", {}))
    core_pulls, comfort = core_gravity(core_axes)
    relation_pulls = relation_gravity(relation_after)
    dynamic_after = apply_dynamic(
        base.get("dynamic_axes", {}),
        comfort,
        dynamic_deltas,
        core_pulls,
        relation_pulls,
        natural_return=natural_return,
    )
    buffer_after = list(due["remaining"]) + list(incoming["pending"])
    workhood = compute_workhood(core_axes, dynamic_after)
    wheel = deepcopy(base.get("core_speed_wheel", {}))
    wheel["current"] = int(wheel.get("current", 0))
    wheel["max"] = speed_wheel_max(workhood["value"])

    after_values = {
        "core_axes": core_axes,
        "dynamic_axes": dynamic_after,
        "comfort_zone": comfort,
        "workhood_index": workhood,
        "core_speed_wheel": wheel,
        "feeling_buffer": buffer_after,
        "feeling_settle_due": False if settle_buffer else bool(
            base.get("heartbeat_flags", {}).get("feeling_settle_due")
        ),
        "next_settle_at": earliest_settle_at(buffer_after),
        "last_state_settlement_id": settlement_id,
    }
    _apply_state_slice(after_state, after_values, clear_due=settle_buffer)
    return {
        "schema_version": schema_version,
        "settlement_id": settlement_id,
        "round": int(round_num) if round_num is not None else None,
        "round_type": str(round_type),
        "settlement_scope": settlement_scope,
        "interactive_round": bool(interactive_round),
        "observed_at": observed_at.isoformat(),
        "source_memory_ids": incoming["source_memory_ids"],
        "settled_buffer_entries": due["settled"],
        "direct_dynamic_deltas": dynamic_deltas,
        "direct_relation_deltas": relation_deltas,
        "state": {
            "before": _state_slice(state),
            "after": after_values,
        },
        "relations": relation_plans,
    }


def _round_marker_number(marker):
    if not marker:
        return 0
    match = re.fullmatch(r"SS-R(\d{6})(?:-E|-F\d{6})?", marker)
    if match:
        return int(match.group(1))
    if marker.startswith("SS-T") and marker[4:].isdigit():
        return 0
    raise ValueError(f"invalid_state_settlement_id:{marker}")


def _settle_round_scope(state_store, relation_store, round_store, round_num,
                        round_type, *, settlement_id, settlement_scope,
                        memory_write_receipts=None, observed_at=None,
                        external_interaction=False, settle_buffer=True,
                        natural_return=True, prepared_plan=None):
    """计划先行，逐卡提交，最后一次性保存 state。"""
    observed_at = observed_at or local_now()
    try:
        local_journal = _read_local_journal(
            _local_journal_path(state_store, None))
        if local_journal and local_journal.get("status") == "planned":
            settle_due_state(
                state_store,
                relation_store,
                observed_at=observed_at,
            )
    except Exception as exc:
        _raise_settlement_error(
            round_store, round_num, settlement_id, exc,
            settlement_scope=settlement_scope)
    state = state_store.load()
    meta = state.get("base", {}).get("meta", {})
    marker = str(meta.get("last_state_settlement_id") or "")
    try:
        existing = _existing_receipt(round_store, round_num, settlement_id)
        if existing is not None:
            return existing
        current_round = max(
            int(meta.get("total_round") or 0),
            _round_marker_number(marker),
        )
    except Exception as exc:
        _raise_settlement_error(
            round_store, round_num, settlement_id, exc,
            settlement_scope=settlement_scope)
    if current_round > int(round_num):
        try:
            existing = _existing_receipt(round_store, round_num, settlement_id)
        except Exception as exc:
            _raise_settlement_error(
                round_store, round_num, settlement_id, exc,
                settlement_scope=settlement_scope)
        if existing is not None:
            return existing
        _raise_settlement_error(
            round_store,
            round_num,
            settlement_id,
            RuntimeError(
                "stale_state_settlement_replay:"
                f"requested={int(round_num)};current={current_round}"
            ),
            settlement_scope=settlement_scope,
        )
    if marker == settlement_id:
        try:
            existing = _existing_receipt(round_store, round_num, settlement_id)
        except Exception as exc:
            _raise_settlement_error(
                round_store, round_num, settlement_id, exc,
                settlement_scope=settlement_scope)
        if existing is not None:
            return existing
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "status": "already_applied",
            "settlement_id": settlement_id,
            "round": int(round_num),
            "settlement_scope": settlement_scope,
        }
        try:
            round_store.append_event(
                round_num, "state_settle_receipt", receipt, phase="reaction")
        except Exception as exc:
            raise StateSettlementError({
                **receipt,
                "status": "error",
                "reason": f"receipt_audit:{type(exc).__name__}:{exc}",
            }) from exc
        return receipt

    relation_receipts = []
    try:
        plan = _existing_plan(round_store, round_num, settlement_id)
        if prepared_plan is not None:
            if not isinstance(prepared_plan, dict):
                raise ValueError("state_settlement_prepared_plan_invalid")
            if (
                prepared_plan.get("schema_version") != PLAN_SCHEMA
                or prepared_plan.get("settlement_id") != settlement_id
                or prepared_plan.get("settlement_scope") != settlement_scope
                or int(prepared_plan.get("round") or 0) != int(round_num)
            ):
                raise ValueError("state_settlement_prepared_plan_mismatch")
            if plan is not None and plan != prepared_plan:
                raise ValueError("state_settlement_prepared_plan_conflict")
            if plan is None:
                if _state_slice(state) != prepared_plan["state"]["before"]:
                    raise ValueError("state_settlement_prepared_state_drift")
                plan = prepared_plan
                round_store.append_event(
                    round_num, "state_settle_plan", plan, phase="reaction")
        if plan is None:
            plan = _build_plan(
                state,
                relation_store,
                round_num,
                round_type,
                memory_write_receipts,
                bool(external_interaction),
                observed_at,
                settlement_id=settlement_id,
                settlement_scope=settlement_scope,
                settle_buffer=settle_buffer,
                natural_return=natural_return,
            )
            round_store.append_event(
                round_num, "state_settle_plan", plan, phase="reaction")

        for relation_plan in plan.get("relations", []):
            relation_receipts.append(relation_store.apply_state_settlement(
                relation_plan["subject"],
                relation_plan["after"],
                settlement_id,
                observed_at=plan.get("observed_at"),
            ))

        state_store.mutate(
            lambda after_state: _apply_state_slice(
                after_state,
                plan["state"]["after"],
                clear_due=settle_buffer,
            )
        )
    except Exception as exc:
        _raise_settlement_error(
            round_store,
            round_num,
            settlement_id,
            exc,
            relation_receipts,
            settlement_scope=settlement_scope,
        )

    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "status": "applied",
        "settlement_id": settlement_id,
        "round": int(round_num),
        "settlement_scope": settlement_scope,
        "source_memory_ids": list(plan.get("source_memory_ids") or []),
        "relations": relation_receipts,
        "before": plan["state"]["before"],
        "after": plan["state"]["after"],
    }
    try:
        round_store.append_event(
            round_num, "state_settle_receipt", receipt, phase="reaction")
    except Exception as exc:
        _raise_settlement_error(
            round_store, round_num, settlement_id, exc, relation_receipts,
            settlement_scope=settlement_scope)
    return receipt


def prepare_reaction_entry(state_store, relation_store, round_store, round_num,
                           round_type, *, external_interaction=False,
                           observed_at=None):
    """Freeze the entry plan without advancing state or writing Round audit."""
    observed_at = observed_at or local_now()
    settlement_id = f"SS-R{int(round_num):06d}-E"
    try:
        local_journal = _read_local_journal(
            _local_journal_path(state_store, None))
        if local_journal and local_journal.get("status") == "planned":
            settle_due_state(
                state_store,
                relation_store,
                observed_at=observed_at,
            )
        state = state_store.load()
        meta = state.get("base", {}).get("meta", {})
        marker = str(meta.get("last_state_settlement_id") or "")
        existing = _existing_receipt(round_store, round_num, settlement_id)
        current_round = max(
            int(meta.get("total_round") or 0),
            _round_marker_number(marker),
        )
        if current_round > int(round_num) and existing is None:
            raise RuntimeError(
                "stale_state_settlement_replay:"
                f"requested={int(round_num)};current={current_round}"
            )
        plan = _existing_plan(round_store, round_num, settlement_id)
        if existing is None and marker != settlement_id and plan is None:
            plan = _build_plan(
                state,
                relation_store,
                round_num,
                round_type,
                None,
                bool(external_interaction),
                observed_at,
                settlement_id=settlement_id,
                settlement_scope="reaction_entry",
                settle_buffer=True,
                natural_return=True,
            )
        preview_state = deepcopy(state)
        if existing is None and marker != settlement_id and plan is not None:
            _apply_state_slice(
                preview_state,
                plan["state"]["after"],
                clear_due=True,
            )
        return {
            "schema_version": ENTRY_PREPARATION_SCHEMA,
            "settlement_id": settlement_id,
            "round": int(round_num),
            "settlement_scope": "reaction_entry",
            "plan": plan,
            "existing_receipt": existing,
            "preview_state": preview_state,
        }
    except Exception as exc:
        _raise_settlement_error(
            round_store,
            round_num,
            settlement_id,
            exc,
            settlement_scope="reaction_entry",
        )


def commit_reaction_entry(state_store, relation_store, round_store, round_num,
                          round_type, preparation):
    """Commit a previously frozen entry plan at the provider-call boundary."""
    if (
        not isinstance(preparation, dict)
        or preparation.get("schema_version") != ENTRY_PREPARATION_SCHEMA
        or int(preparation.get("round") or 0) != int(round_num)
        or preparation.get("settlement_id")
        != f"SS-R{int(round_num):06d}-E"
        or preparation.get("settlement_scope") != "reaction_entry"
    ):
        raise StateSettlementError({
            "schema_version": RECEIPT_SCHEMA,
            "status": "error",
            "settlement_id": f"SS-R{int(round_num):06d}-E",
            "round": int(round_num),
            "settlement_scope": "reaction_entry",
            "reason": "state_settlement_preparation_invalid",
        })
    existing = preparation.get("existing_receipt")
    if isinstance(existing, dict):
        return existing
    plan = preparation.get("plan")
    if isinstance(plan, dict) and str(plan.get("round_type")) != str(round_type):
        raise StateSettlementError({
            "schema_version": RECEIPT_SCHEMA,
            "status": "error",
            "settlement_id": f"SS-R{int(round_num):06d}-E",
            "round": int(round_num),
            "settlement_scope": "reaction_entry",
            "reason": "state_settlement_preparation_round_type_mismatch",
        })
    if plan is None:
        marker = str(state_store.get(
            "base.meta.last_state_settlement_id", "") or "")
        if marker != f"SS-R{int(round_num):06d}-E":
            raise StateSettlementError({
                "schema_version": RECEIPT_SCHEMA,
                "status": "error",
                "settlement_id": f"SS-R{int(round_num):06d}-E",
                "round": int(round_num),
                "settlement_scope": "reaction_entry",
                "reason": "state_settlement_preparation_plan_missing",
            })
    observed_at = None
    if isinstance(plan, dict) and plan.get("observed_at"):
        observed_at = datetime.fromisoformat(str(plan["observed_at"]))
    return _settle_round_scope(
        state_store,
        relation_store,
        round_store,
        round_num,
        round_type,
        settlement_id=f"SS-R{int(round_num):06d}-E",
        settlement_scope="reaction_entry",
        observed_at=observed_at,
        settle_buffer=True,
        natural_return=True,
        prepared_plan=plan,
    )


def settle_reaction_frame(state_store, relation_store, round_store, round_num,
                          round_type, frame_iteration, memory_write_receipts,
                          *, observed_at=None):
    return _settle_round_scope(
        state_store,
        relation_store,
        round_store,
        round_num,
        round_type,
        settlement_id=(
            f"SS-R{int(round_num):06d}-F{int(frame_iteration):06d}"
        ),
        settlement_scope="reaction_frame",
        memory_write_receipts=memory_write_receipts,
        observed_at=observed_at,
        settle_buffer=False,
        natural_return=False,
    )


def _local_journal_path(state_store, journal_path):
    if journal_path:
        return str(journal_path)
    state_path = os.path.abspath(str(getattr(state_store, "path", "") or ""))
    if state_path:
        return os.path.join(
            os.path.dirname(state_path),
            "STM",
            "buffer",
            "state_settlement_journal.json",
        )
    return STATE_SETTLEMENT_JOURNAL_JSON


def _read_local_journal(path):
    if not os.path.isfile(path):
        return None
    try:
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise StateSettlementError({
            "schema_version": LOCAL_RECEIPT_SCHEMA,
            "status": "error",
            "reason": f"local_journal_read:{type(exc).__name__}:{exc}",
        }) from exc
    if (
        not isinstance(payload, dict)
        or payload.get("schema_version") != LOCAL_TRANSACTION_SCHEMA
        or payload.get("status") not in {"planned", "applied"}
    ):
        raise StateSettlementError({
            "schema_version": LOCAL_RECEIPT_SCHEMA,
            "status": "error",
            "reason": "local_journal_invalid",
        })
    return payload


def _local_error(settlement_id, cause, relation_receipts=None):
    return StateSettlementError({
        "schema_version": LOCAL_RECEIPT_SCHEMA,
        "status": "error",
        "settlement_id": settlement_id,
        "reason": f"{type(cause).__name__}:{cause}",
        "relations": list(relation_receipts or []),
    })


def settle_due_state(state_store, relation_store, *, journal_path=None,
                     observed_at=None):
    """Settle an idle due feeling buffer without creating a Round."""
    observed_at = observed_at or local_now()
    path = _local_journal_path(state_store, journal_path)
    journal = _read_local_journal(path)
    if journal and journal.get("status") == "planned":
        plan = journal.get("plan")
        if not isinstance(plan, dict):
            raise StateSettlementError({
                "schema_version": LOCAL_RECEIPT_SCHEMA,
                "status": "error",
                "reason": "local_journal_plan_missing",
            })
        settlement_id = str(plan.get("settlement_id") or "")
    else:
        settlement_id = f"SS-T{observed_at.strftime('%Y%m%d%H%M%S%f')}"
        try:
            state = state_store.load()
            flags = state.get("base", {}).get("heartbeat_flags", {})
            if not flags.get("feeling_settle_due"):
                return {
                    "schema_version": LOCAL_RECEIPT_SCHEMA,
                    "status": "skipped",
                    "reason": "feeling_settlement_not_due",
                }
            plan = _build_plan(
                state,
                relation_store,
                None,
                "local_timer",
                None,
                False,
                observed_at,
                settlement_id=settlement_id,
                schema_version=LOCAL_PLAN_SCHEMA,
                settlement_scope="idle_timer",
                settle_buffer=True,
                natural_return=False,
            )
            if not plan.get("settled_buffer_entries"):
                state_store.update_many({
                    "base.heartbeat_flags.feeling_settle_due": False,
                    "base.meta.next_settle_at": (
                        plan["state"]["after"]["next_settle_at"]
                    ),
                })
                return {
                    "schema_version": LOCAL_RECEIPT_SCHEMA,
                    "status": "skipped",
                    "reason": "no_due_feeling_buffer",
                }
            journal = {
                "schema_version": LOCAL_TRANSACTION_SCHEMA,
                "status": "planned",
                "plan": plan,
            }
            atomic_write_json(path, journal, trailing_newline=True)
        except Exception as exc:
            if isinstance(exc, StateSettlementError):
                raise
            raise _local_error(settlement_id, exc) from exc

    if not settlement_id.startswith("SS-T") or not settlement_id[4:].isdigit():
        raise StateSettlementError({
            "schema_version": LOCAL_RECEIPT_SCHEMA,
            "status": "error",
            "reason": "local_settlement_id_invalid",
        })

    relation_receipts = []
    try:
        current = state_store.load()
        current_marker = str(
            current.get("base", {}).get("meta", {}).get(
                "last_state_settlement_id") or ""
        )
        before_marker = str(
            plan.get("state", {}).get("before", {}).get(
                "last_state_settlement_id") or ""
        )
        if current_marker not in {before_marker, settlement_id}:
            raise RuntimeError(
                "stale_local_state_settlement:"
                f"planned={before_marker};current={current_marker}"
            )
        if current_marker != settlement_id:
            for relation_plan in plan.get("relations", []):
                relation_receipts.append(
                    relation_store.apply_state_settlement(
                        relation_plan["subject"],
                        relation_plan["after"],
                        settlement_id,
                        observed_at=plan.get("observed_at"),
                    )
                )
            state_store.mutate(
                lambda after_state: _apply_state_slice(
                    after_state, plan["state"]["after"], clear_due=True
                )
            )
        else:
            def clear_due(recovered_state):
                recovered_state.setdefault("base", {}).setdefault(
                    "heartbeat_flags", {})["feeling_settle_due"] = False

            state_store.mutate(clear_due)
    except Exception as exc:
        raise _local_error(
            settlement_id, exc, relation_receipts) from exc

    try:
        receipt = {
            "schema_version": LOCAL_RECEIPT_SCHEMA,
            "status": "applied",
            "settlement_id": settlement_id,
            "source": "heartbeat_timer",
            "settlement_scope": "idle_timer",
            "relations": relation_receipts,
            "before": plan["state"]["before"],
            "after": plan["state"]["after"],
        }
    except (KeyError, TypeError) as exc:
        raise _local_error(settlement_id, exc, relation_receipts) from exc
    try:
        atomic_write_json(path, {
            "schema_version": LOCAL_TRANSACTION_SCHEMA,
            "status": "applied",
            "plan": plan,
            "receipt": receipt,
        }, trailing_newline=True)
    except Exception as exc:
        try:
            state_store.set_flag("feeling_settle_due", True)
        except Exception:
            pass
        raise _local_error(settlement_id, exc, relation_receipts) from exc
    return receipt
