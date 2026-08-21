"""Seed 每轮唯一的主体状态结算入口。"""

from copy import deepcopy
from datetime import datetime
import json
import os

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
RECEIPT_SCHEMA = "state_settle_receipt.v1"
LOCAL_PLAN_SCHEMA = "state_settle_local_plan.v1"
LOCAL_RECEIPT_SCHEMA = "state_settle_local_receipt.v1"
LOCAL_TRANSACTION_SCHEMA = "state_settle_local_transaction.v1"


class StateSettlementError(RuntimeError):
    def __init__(self, receipt):
        self.receipt = receipt
        super().__init__(str(receipt.get("reason") or "state_settlement_failed"))


def _raise_settlement_error(round_store, round_num, settlement_id, cause,
                            relation_receipts=None):
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "status": "error",
        "settlement_id": settlement_id,
        "round": int(round_num),
        "reason": f"{type(cause).__name__}:{cause}",
        "relations": list(relation_receipts or []),
    }
    try:
        round_store.append_event(
            round_num, "state_settle_receipt", receipt, phase="cleanup")
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
        "next_settle_at": meta.get("next_settle_at"),
        "last_state_settlement_id": meta.get("last_state_settlement_id"),
    }


def _apply_state_slice(state, values):
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
    base.setdefault("heartbeat_flags", {})["feeling_settle_due"] = False


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
                schema_version=PLAN_SCHEMA):
    settlement_id = settlement_id or f"SS-R{int(round_num):06d}"
    incoming = collect_receipt_effects(receipts, observed_at=observed_at)
    due = settle_pending(
        state.get("base", {}).get("feeling_buffer", []),
        interactive_round=interactive_round,
        observed_at=observed_at,
    )
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
        "next_settle_at": earliest_settle_at(buffer_after),
        "last_state_settlement_id": settlement_id,
    }
    _apply_state_slice(after_state, after_values)
    return {
        "schema_version": schema_version,
        "settlement_id": settlement_id,
        "round": int(round_num) if round_num is not None else None,
        "round_type": str(round_type),
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
    if marker.startswith("SS-R") and marker[4:].isdigit():
        return int(marker[4:])
    if marker.startswith("SS-T") and marker[4:].isdigit():
        return 0
    raise ValueError(f"invalid_state_settlement_id:{marker}")


def settle_state(state_store, relation_store, round_store, round_num,
                 round_type, memory_write_receipts=None, user_input_text="",
                 observed_at=None, external_interaction=None):
    """计划先行，逐卡提交，最后一次性保存 state。"""
    observed_at = observed_at or local_now()
    settlement_id = f"SS-R{int(round_num):06d}"
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
            round_store, round_num, settlement_id, exc)
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
            round_store, round_num, settlement_id, exc)
    if current_round > int(round_num):
        try:
            existing = _existing_receipt(round_store, round_num, settlement_id)
        except Exception as exc:
            _raise_settlement_error(
                round_store, round_num, settlement_id, exc)
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
        )
    if marker == settlement_id:
        try:
            existing = _existing_receipt(round_store, round_num, settlement_id)
        except Exception as exc:
            _raise_settlement_error(
                round_store, round_num, settlement_id, exc)
        if existing is not None:
            return existing
        receipt = {
            "schema_version": RECEIPT_SCHEMA,
            "status": "already_applied",
            "settlement_id": settlement_id,
            "round": int(round_num),
        }
        try:
            round_store.append_event(
                round_num, "state_settle_receipt", receipt, phase="cleanup")
        except Exception as exc:
            raise StateSettlementError({
                **receipt,
                "status": "error",
                "reason": f"receipt_audit:{type(exc).__name__}:{exc}",
            }) from exc
        return receipt

    # Runtime 显式传入 trigger.messages 事实；None 只供旧的直接调用入口
    # 兼容，不能把节律/中继内部文本误算成外部交互。
    interactive_round = (
        bool(external_interaction)
        if external_interaction is not None
        else bool(
            round_type == "interactive"
            and str(user_input_text or "").strip()
        )
    )
    relation_receipts = []
    try:
        plan = _existing_plan(round_store, round_num, settlement_id)
        if plan is None:
            plan = _build_plan(
                state,
                relation_store,
                round_num,
                round_type,
                memory_write_receipts,
                interactive_round,
                observed_at,
            )
            round_store.append_event(
                round_num, "state_settle_plan", plan, phase="cleanup")

        for relation_plan in plan.get("relations", []):
            relation_receipts.append(relation_store.apply_state_settlement(
                relation_plan["subject"],
                relation_plan["after"],
                settlement_id,
                observed_at=plan.get("observed_at"),
            ))

        state_store.mutate(
            lambda after_state: _apply_state_slice(
                after_state, plan["state"]["after"]
            )
        )
    except Exception as exc:
        _raise_settlement_error(
            round_store,
            round_num,
            settlement_id,
            exc,
            relation_receipts,
        )

    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "status": "applied",
        "settlement_id": settlement_id,
        "round": int(round_num),
        "source_memory_ids": list(plan.get("source_memory_ids") or []),
        "relations": relation_receipts,
        "before": plan["state"]["before"],
        "after": plan["state"]["after"],
    }
    try:
        round_store.append_event(
            round_num, "state_settle_receipt", receipt, phase="cleanup")
    except Exception as exc:
        _raise_settlement_error(
            round_store, round_num, settlement_id, exc, relation_receipts)
    return receipt


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
                    after_state, plan["state"]["after"]
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
