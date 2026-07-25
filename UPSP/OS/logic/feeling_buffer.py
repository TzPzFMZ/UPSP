"""感受脉冲的纯计算。

每条成功记忆回执先按 ``memory/domain/subject`` 合并逐轴净值；净值总是立即
生效一次，注意级和震撼级再进入双触发缓冲。模块不读写磁盘。
"""

from datetime import datetime, timedelta

from constants import DYNAMIC_AXIS_KEYS, RELATION_AXIS_KEYS, TZ_SHANGHAI
from logic.feeling_lookup import FeelingWordTable


PULSE_ROUNDS = 2
PULSE_DELAY = timedelta(minutes=5)


def _add(target, axis, delta):
    if delta:
        target[axis] = target.get(axis, 0) + delta


def _add_subject(target, subject, axis, delta):
    if delta:
        axes = target.setdefault(subject, {})
        axes[axis] = axes.get(axis, 0) + delta


def _buffer_entry(mem_id, domain, subject, axis, delta, observed_at):
    intensity, remaining = pulse_profile(delta)
    return {
        "buffer_id": ":".join((
            "FB", str(mem_id), str(domain), str(subject or "_"), str(axis),
        )),
        "source_mem_id": str(mem_id),
        "domain": domain,
        "subject": subject or None,
        "axis": axis,
        "delta": delta,
        "intensity": intensity,
        "remaining_settlements": remaining,
        "next_settle_at": (observed_at + PULSE_DELAY).isoformat(),
        "settle_rounds": PULSE_ROUNDS,
        "interactive_rounds_elapsed": 0,
    }


def pulse_profile(delta):
    magnitude = abs(delta)
    if magnitude <= 2:
        return "ordinary", 0
    if magnitude <= 4:
        return "attention", 1
    return "shock", 2


def collect_receipt_effects(receipts, observed_at=None):
    """把成功 ``memory_write`` 回执变成即时变化和新缓冲。

    同一个 ``mem_id`` 即使被投影多次也只消费一次。
    """
    observed_at = observed_at or datetime.now(TZ_SHANGHAI)
    table = FeelingWordTable()
    dynamic = {}
    relations = {}
    pending = []
    source_ids = []
    seen = set()

    for receipt in receipts or []:
        if not isinstance(receipt, dict) or receipt.get("status") != "applied":
            continue
        mem_id = str(receipt.get("mem_id") or "").strip()
        if not mem_id or mem_id in seen:
            continue
        seen.add(mem_id)
        source_ids.append(mem_id)

        interaction = table.merge_deltas(
            table.lookup_interaction(receipt.get("interaction_feelings") or [])
        )
        for axis, delta in interaction.items():
            _add(dynamic, axis, delta)
            if abs(delta) >= 3:
                pending.append(_buffer_entry(
                    mem_id, "dynamic", None, axis, delta, observed_at))

        by_subject = {}
        for item in receipt.get("relationship_feelings") or []:
            if not isinstance(item, dict):
                continue
            subject = str(item.get("subject") or "").strip()
            lookup = table.lookup_relation(item.get("word"))
            if not subject or not lookup:
                continue
            subject_axes = by_subject.setdefault(subject, {})
            for axis, delta in lookup["deltas"].items():
                _add(subject_axes, axis, delta)
        for subject, axes in by_subject.items():
            for axis, delta in axes.items():
                _add_subject(relations, subject, axis, delta)
                if abs(delta) >= 3:
                    pending.append(_buffer_entry(
                        mem_id, "relation", subject, axis, delta, observed_at))

    return {
        "dynamic": dynamic,
        "relations": relations,
        "pending": pending,
        "source_memory_ids": source_ids,
    }


def settle_pending(entries, interactive_round=False, observed_at=None):
    """结算到期脉冲；时间或两个真实交互轮任一先到即生效。"""
    observed_at = observed_at or datetime.now(TZ_SHANGHAI)
    dynamic = {}
    relations = {}
    remaining = []
    settled = []

    for index, original in enumerate(entries or []):
        if not isinstance(original, dict):
            raise ValueError(f"invalid_feeling_buffer_entry:{index}")
        entry = dict(original)
        try:
            due_at = datetime.fromisoformat(str(entry["next_settle_at"]))
            rounds = int(entry.get("interactive_rounds_elapsed", 0))
            if interactive_round:
                rounds += 1
            settle_rounds = int(entry.get("settle_rounds", PULSE_ROUNDS))
            due = observed_at >= due_at or rounds >= settle_rounds
            domain = entry["domain"]
            subject = entry.get("subject")
            axis = str(entry["axis"])
            delta = entry["delta"]
            left = int(entry["remaining_settlements"])
            if (
                not isinstance(delta, (int, float))
                or isinstance(delta, bool)
                or delta == 0
                or left <= 0
                or settle_rounds <= 0
                or rounds < 0
            ):
                raise ValueError("invalid_numeric_field")
            if domain == "dynamic" and axis not in DYNAMIC_AXIS_KEYS:
                raise ValueError("invalid_dynamic_axis")
            if domain == "relation" and (
                not subject or axis not in RELATION_AXIS_KEYS
            ):
                raise ValueError("invalid_relation_axis")
        except (KeyError, TypeError, ValueError) as exc:
            raise ValueError(
                f"invalid_feeling_buffer_entry:{index}") from exc

        if not due:
            entry["interactive_rounds_elapsed"] = rounds
            remaining.append(entry)
            continue

        if domain == "dynamic":
            _add(dynamic, axis, delta)
        elif domain == "relation":
            _add_subject(relations, str(subject), axis, delta)
        else:
            raise ValueError(f"invalid_feeling_buffer_domain:{index}:{domain}")
        settled.append({
            "buffer_id": entry.get("buffer_id"),
            "domain": domain,
            "subject": subject,
            "axis": axis,
            "delta": delta,
        })
        left -= 1
        if left > 0:
            entry["remaining_settlements"] = left
            entry["interactive_rounds_elapsed"] = 0
            entry["next_settle_at"] = (observed_at + PULSE_DELAY).isoformat()
            remaining.append(entry)

    return {
        "dynamic": dynamic,
        "relations": relations,
        "remaining": remaining,
        "settled": settled,
    }


def earliest_settle_at(entries):
    values = [
        str(entry.get("next_settle_at"))
        for entry in entries or []
        if isinstance(entry, dict) and entry.get("next_settle_at")
    ]
    return min(values) if values else None
