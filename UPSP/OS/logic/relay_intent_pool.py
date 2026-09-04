"""Runtime truth source for relay intentions."""

from copy import deepcopy
from datetime import datetime

from constants import local_now


OPEN_STATUSES = {"open"}
SETTLED_STATUSES = {
    "completed": "completed",
    "merged": "merged",
    "question": "question",
    "deferred": "deferred",
    "blocked": "blocked",
}


def _clean_text(value):
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _relay_pool_from_state(state):
    runtime = ((state or {}).get("base") or {}).get("runtime") or {}
    pool = runtime.get("relay_intents")
    return pool if isinstance(pool, list) else []


def _load_pool(sm):
    try:
        pool = sm.get("base.runtime.relay_intents", [])
    except Exception:
        pool = _relay_pool_from_state(sm.load())
    return pool if isinstance(pool, list) else []


def _store_pool(sm, pool):
    sm.set("base.runtime.relay_intents", pool)


def _next_intent_id(sm, source_round):
    source_round = int(source_round or 0)
    try:
        current = int(sm.get("base.runtime.relay_intent_seq", 0) or 0)
    except Exception:
        current = 0
    next_seq = current + 1
    try:
        sm.set("base.runtime.relay_intent_seq", next_seq)
    except Exception:
        pass
    return f"RLY-R{source_round:06d}-N{next_seq:03d}"


def create_relay_intent(sm, *, source_round, handoff_text,
                        reaction_finalize_id="", user_input_ref="",
                        progress_fingerprint="", progress_evidence=None):
    pool = list(_load_pool(sm))
    now = local_now().isoformat()
    intent = {
        "relay_intent_id": _next_intent_id(sm, source_round),
        "status": "open",
        "source_round": int(source_round or 0),
        "reaction_finalize_id": _clean_text(reaction_finalize_id),
        "user_input_ref": _clean_text(user_input_ref),
        "handoff_text": _clean_text(handoff_text),
        "created_at": now,
        "updated_at": now,
        "settlement": {},
    }
    if _clean_text(progress_fingerprint):
        intent["progress_fingerprint"] = _clean_text(progress_fingerprint)
    evidence = sorted({
        _clean_text(item) for item in (progress_evidence or [])
        if _clean_text(item)
    })
    if evidence:
        intent["progress_evidence"] = evidence
    pool.append(intent)
    _store_pool(sm, pool)
    return deepcopy(intent)


def open_relay_intents(state):
    result = []
    for item in _relay_pool_from_state(state):
        if not isinstance(item, dict):
            continue
        if _clean_text(item.get("status")) in OPEN_STATUSES:
            result.append(deepcopy(item))
    return result


def mark_relay_handoff_projected(sm, relay_intent_id, *, round_num):
    intent_id = _clean_text(relay_intent_id)
    if not intent_id:
        return None
    pool = list(_load_pool(sm))
    now = local_now().isoformat()
    for item in pool:
        if not isinstance(item, dict):
            continue
        if item.get("relay_intent_id") != intent_id:
            continue
        item["handoff_projected_round"] = int(round_num or 0)
        item["handoff_projected_at"] = now
        item["updated_at"] = now
        _store_pool(sm, pool)
        return deepcopy(item)
    return None


def render_open_relay_intents_for_context(state):
    intents = open_relay_intents(state)
    if not intents:
        return ""
    runtime = ((state or {}).get("base") or {}).get("runtime") or {}
    pending_target = runtime.get("pending_relay_target")
    if isinstance(pending_target, dict) and pending_target and len(intents) == 1:
        return ""
    lines = [
        "- kind: relay_intent_pool",
        "  tier: reminder",
        "  decision_required: false",
        "  source: runtime.relay_intent_pool",
        "  message: |",
        "    REMINDER｜中继规划池",
        "    当前还有未完成的中继意图。此处只显示指针，不展示交接正文；具体事务以当前中继目标卡或对应 relay intent 的隐藏 payload 为准。",
    ]
    for item in intents:
        intent_id = _clean_text(item.get("relay_intent_id"))
        source_round = item.get("source_round", "")
        lines.append(
            f"    - {intent_id}：来源轮 R{int(source_round or 0):06d}；status=open；"
            "具体事务以当前中继目标卡或对应中继意图 payload 为准；若该意图已结清请忽略。"
        )
    return "\n".join(lines)


def settle_relay_intent(sm, request, *, round_num):
    request = request or {}
    intent_id = _clean_text(request.get("relay_intent_id"))
    status = _clean_text(request.get("status"))
    note = _clean_text(request.get("note"))
    pool = list(_load_pool(sm))
    base = {
        "tool_id": "relay_intent_settle",
        "protocol_tool_receipt": True,
        "relay_intent_id": intent_id,
        "status_requested": status,
        "note": note,
        "round_num": int(round_num or 0),
    }
    if status not in SETTLED_STATUSES:
        return {
            **base,
            "status": "rejected",
            "reason": "invalid_relay_intent_status",
        }
    for item in pool:
        if not isinstance(item, dict):
            continue
        if item.get("relay_intent_id") != intent_id:
            continue
        item["status"] = SETTLED_STATUSES[status]
        item["updated_at"] = local_now().isoformat()
        item["settlement"] = {
            "status": SETTLED_STATUSES[status],
            "note": note,
            "round_num": int(round_num or 0),
        }
        _store_pool(sm, pool)
        return {
            **base,
            "status": "applied",
            "reason": "relay_intent_settled",
            "final_status": item["status"],
        }
    return {
        **base,
        "status": "not_found",
        "reason": "relay_intent_not_found",
    }


def settle_open_relay_intents(sm, *, status, round_num, note="", source="runtime"):
    final_status = _clean_text(status)
    note = _clean_text(note)
    source = _clean_text(source)
    if final_status not in SETTLED_STATUSES:
        return {
            "tool_id": "relay_intent_settle",
            "protocol_tool_receipt": True,
            "status": "rejected",
            "reason": "invalid_relay_intent_status",
            "status_requested": final_status,
            "round_num": int(round_num or 0),
            "settled_relay_intent_ids": [],
        }
    pool = list(_load_pool(sm))
    now = local_now().isoformat()
    settled = []
    for item in pool:
        if not isinstance(item, dict):
            continue
        if _clean_text(item.get("status")) not in OPEN_STATUSES:
            continue
        item["status"] = SETTLED_STATUSES[final_status]
        item["updated_at"] = now
        item["settlement"] = {
            "status": SETTLED_STATUSES[final_status],
            "note": note,
            "round_num": int(round_num or 0),
            "source": source,
        }
        settled.append(_clean_text(item.get("relay_intent_id")))
    _store_pool(sm, pool)
    return {
        "tool_id": "relay_intent_settle",
        "protocol_tool_receipt": True,
        "status": "applied",
        "reason": "open_relay_intents_settled",
        "final_status": SETTLED_STATUSES[final_status],
        "note": note,
        "round_num": int(round_num or 0),
        "source": source,
        "settled_relay_intent_ids": settled,
    }
