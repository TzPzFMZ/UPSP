"""最近缓存删后幸存段压缩执行器。

cache_compact 是基座工具：cleanup LLM 只产语义摘要/合并候选，
脚本在这里执行 lately_cache.jsonl 重写；raw_log 与 Corpus 节归档原文不随压缩改写。
"""


def _coerce_ratio(value, default=0.618):
    try:
        ratio = float(value)
    except (TypeError, ValueError):
        ratio = default
    return min(1.0, max(0.0, ratio))


def _ratio_from_context(context_store):
    getter = getattr(context_store, "get_lately_compact_ratio", None)
    if not getter:
        return 0.618
    return _coerce_ratio(getter())


def _survivor_candidates(context_store):
    getter = getattr(context_store, "build_lately_compression_candidates", None)
    if not getter:
        return []
    return list(getter(max_blocks=None) or [])


def execute_cache_compact(context_store, plan=None):
    """执行 cache_compact 基座动作。

    plan 字段：
    - lately_trimmed: 本轮是否发生 lately 字符履带删除
    - compact_ratio: 0.0~1.0，默认从 lately 配置读取
    - decision: cleanup LLM 的最近缓存压缩动作对象
    - source_block_ids: Runtime 结构化 pending 中记录的待处理 lately 块
    - current_round: 当前轮号，仅用于新摘要块审计 loc
    """
    plan = dict(plan or {})
    if not plan.get("lately_trimmed"):
        return {"status": "skipped_no_lately_trim"}

    ratio = _coerce_ratio(plan.get("compact_ratio", _ratio_from_context(context_store)))
    candidates = _survivor_candidates(context_store)
    pending_source_ids = [
        str(item or "").strip()
        for item in plan.get("source_block_ids") or []
        if str(item or "").strip()
    ]
    before_chars = sum(int(item.get("chars", 0) or 0) for item in candidates)
    target_chars = int(before_chars * ratio)
    base_report = {
        "tool_id": "cache_compact",
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "compact_reason": "post_lately_trim",
        "compact_ratio": ratio,
        "before_chars": before_chars,
        "target_chars": target_chars,
        "candidate_blocks": len(candidates),
    }

    if ratio >= 1.0:
        return {**base_report, "status": "skipped_ratio_1"}

    if ratio <= 0.0:
        source_ids = pending_source_ids or [
            item.get("id", "") for item in candidates if item.get("id")
        ]
        rewrite_report = context_store.rewrite_lately_blocks(
            [{"source_block_ids": source_ids, "action": "drop"}],
            current_round=plan.get("current_round"),
        )
        return {
            **base_report,
            **rewrite_report,
            "status": "cleared",
            "after_chars": 0,
        }

    decision = plan.get("decision")
    if isinstance(decision, dict) and decision:
        source_ids = pending_source_ids or [
            item.get("id", "") for item in candidates if item.get("id")
        ]
        decision = {**decision, "source_block_ids": source_ids}
        decisions = [decision]
    else:
        decisions = list(plan.get("decisions") or [])
    if not decisions:
        return {**base_report, "status": "skipped_no_decisions"}

    rewrite_report = context_store.rewrite_lately_blocks(
        decisions,
        current_round=plan.get("current_round"),
    )
    return {
        **base_report,
        **rewrite_report,
        "status": "applied",
    }
