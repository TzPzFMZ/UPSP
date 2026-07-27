"""Cache compaction rhythm guide helpers for Spec463."""

from datetime import datetime

from constants import local_now


CACHE_COMPACTION_ITEM_ID = "compress_lately_cache"
CACHE_COMPACTION_OPTION_ID = "submit_cache_compaction_shard"


def plan_lately_compaction_shards(
        candidates,
        *,
        compact_ratio=0.618,
        shard_chars=8192,
        shard_ratio=0.314):
    """Plan oldest-first shard compaction until the global target is reachable."""
    items = [_normalize_candidate(item) for item in candidates or []]
    items = [item for item in items if item["chars"] > 0 and item["id"]]
    before_chars = sum(item["chars"] for item in items)
    global_ratio = _ratio(compact_ratio, 0.618)
    per_shard_ratio = _ratio(shard_ratio, 0.314)
    shard_limit = max(1, _int(shard_chars, 8192))
    target_chars = int(before_chars * global_ratio)
    reduction_needed = max(0, before_chars - target_chars)
    plan = {
        "before_chars": before_chars,
        "target_chars": target_chars,
        "compact_ratio": global_ratio,
        "compact_shard_chars": shard_limit,
        "compact_shard_ratio": per_shard_ratio,
        "shards": [],
    }
    if reduction_needed <= 0:
        return plan

    current = []
    current_chars = 0
    reduced = 0

    def flush():
        nonlocal current, current_chars, reduced
        if not current:
            return
        shard_target = int(current_chars * per_shard_ratio)
        expected_reduction = max(0, current_chars - shard_target)
        plan["shards"].append({
            "shard_id": f"shard_{len(plan['shards']) + 1:02d}",
            "source_block_ids": [item["id"] for item in current],
            "input_chars": current_chars,
            "target_chars": shard_target,
            "expected_reduction_chars": expected_reduction,
        })
        reduced += expected_reduction
        current = []
        current_chars = 0

    for item in items:
        if current and current_chars + item["chars"] > shard_limit:
            flush()
            if reduced >= reduction_needed:
                break
        current.append(item)
        current_chars += item["chars"]
        if current_chars >= shard_limit:
            flush()
            if reduced >= reduction_needed:
                break
    if reduced < reduction_needed:
        flush()
    plan["planned_reduction_chars"] = reduced
    return plan


def cache_compaction_due_receipt(context_store, round_num):
    """Return a due receipt when lately watermark deletion requires compaction."""
    stats = context_store.get_last_cache_stats()
    if isinstance(stats, dict) and stats.get("lately_trimmed"):
        if stats.get("cache_compaction_required") is False:
            return {"status": "skipped", "reason": "compaction_not_required"}

        candidates = list(
            context_store.build_lately_compression_candidates(
                max_blocks=None,
            ) or []
        )
        params = _compaction_params(context_store, stats)
        plan = plan_lately_compaction_shards(
            candidates,
            compact_ratio=params["compact_ratio"],
            shard_chars=params["compact_shard_chars"],
            shard_ratio=params["compact_shard_ratio"],
        )
        if not plan.get("shards"):
            return {"status": "skipped", "reason": "no_compaction_shards", "plan": plan}
        return {
            "status": "due",
            "reason": "cache_compaction_due",
            "source": "last_cache_stats",
            "round": int(round_num or 0),
            "flag": "cache_compaction_due",
            "cache_stats": dict(stats),
            "candidate_ids": [item.get("id") for item in candidates if isinstance(item, dict)],
            "plan": plan,
        }

    debt = context_store.load_cache_compaction_debt()
    if isinstance(debt, dict) and debt:
        plan = debt.get("compaction_plan") if isinstance(debt.get("compaction_plan"), dict) else {}
        if plan.get("shards"):
            return {
                "status": "due",
                "reason": "cache_compaction_debt",
                "source": "cache_compaction_debt",
                "round": int(round_num or debt.get("created_round") or 0),
                "flag": "cache_compaction_due",
                "cache_stats": dict(debt.get("cache_stats") or {}),
                "candidate_ids": list(debt.get("candidate_ids") or []),
                "completed_shards": list(debt.get("completed_shards") or []),
                "plan": plan,
            }
        return {"status": "skipped", "reason": "invalid_cache_compaction_debt", "debt": debt}
    return {"status": "skipped", "reason": "no_lately_trim"}


def materialize_cache_compaction_rhythm_guide(
        workbench_store,
        context_store,
        round_num,
        *,
        state_store=None,
        guide_id=None):
    """Create the cache compaction rhythm guide when the agenda reaches it."""
    due = cache_compaction_due_receipt(context_store, round_num)
    if due.get("status") != "due":
        if state_store is not None:
            try:
                state_store.clear_flags(["cache_compaction_due"])
            except Exception:
                pass
        return None
    guide_id = str(guide_id or f"cache_compaction:R{int(round_num or 0):06d}")
    try:
        active_rhythm = str(workbench_store.get("base.active_guides.rhythm") or "").strip()
    except Exception:
        active_rhythm = ""
    if active_rhythm:
        return active_rhythm
    stats = due.get("cache_stats") or {}
    plan = due.get("plan") or {}
    source = str(due.get("source") or "").strip()

    guide = {
        "guide_id": guide_id,
        "kind": "cache_compaction_rhythm_guide",
        "guide_slot": "rhythm",
        "title": "最近缓存压缩节律",
        "status": "open",
        "created_at": local_now().isoformat(),
        "reason": "lately cache watermark trim triggered semantic compaction rhythm",
        "source_refs": (
            ["cache_compaction_debt"]
            if source == "cache_compaction_debt"
            else [f"round:{int(round_num or 0)}", "context_store.last_cache_stats"]
        ),
        "cache_stats": dict(stats),
        "compaction_plan": plan,
        "completed_shards": list(due.get("completed_shards") or []),
        "items": [{
            "item_id": CACHE_COMPACTION_ITEM_ID,
            "title": "按分片压缩最近缓存幸存段",
            "status": "open",
            "options": [{
                "option_id": CACHE_COMPACTION_OPTION_ID,
                "required_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
                "allowed_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                    "blocked_reason",
                ],
            }],
        }],
    }
    workbench_store.save_guide(guide, active=True)
    workbench_store.append_guide_ledger(guide_id, {
        "event": "cache_compaction_rhythm_created",
        "round": int(round_num or 0),
        "cache_stats": dict(stats),
        "compaction_plan": plan,
    })
    return guide_id


def _normalize_candidate(item):
    item = item if isinstance(item, dict) else {}
    text = str(item.get("text") or "")
    chars = _int(item.get("chars"), len(text))
    return {
        "id": str(item.get("id") or "").strip(),
        "chars": chars if chars > 0 else len(text),
    }


def _compaction_params(context_store, stats):
    raw = context_store.get_lately_compaction_params()
    raw = raw if isinstance(raw, dict) else {}
    return {
        "compact_ratio": _ratio(
            raw.get("compact_ratio", stats.get("lately_compact_ratio", 0.618)),
            0.618,
        ),
        "compact_shard_chars": max(1, _int(
            raw.get("compact_shard_chars", stats.get("lately_compact_shard_chars", 8192)),
            8192,
        )),
        "compact_shard_ratio": _ratio(
            raw.get("compact_shard_ratio", stats.get("lately_compact_shard_ratio", 0.314)),
            0.314,
        ),
    }


def _int(value, default):
    try:
        return int(value)
    except (TypeError, ValueError):
        return int(default)


def _ratio(value, default):
    try:
        ratio = float(value)
    except (TypeError, ValueError):
        ratio = float(default)
    return min(1.0, max(0.0, ratio))
