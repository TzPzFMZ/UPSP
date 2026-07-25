"""
善后步处理管线 — 分派 provider-native cleanup 投影到各 data/ logic 模块
DDS §23.2 善后步 + cleanup.md §五

职责：pure logic —— 校验 + 分派，不直接写文件
"""
import re
from datetime import datetime

from data.relation_store import relation_public_name
from logic.container_ops import scan_orphans
from logic.interaction_meta import cache_interaction_meta
TACIT_ACTIONS = frozenset({"kept", "dropped", "added"})
EMPTY_CELLS = frozenset({"", "无", "—", "-"})
PREWORK_EVIDENCE_PREFIXES = (
    "memory_write:",
    "memory_content_read:",
    "memory_link_update:",
    "relation_card_write:",
    "handoff:",
    "explicit_added:",
)
SKIP_PRESELECTION_ITEMS = frozenset({"", "-", "—", "无", "确认", "放行", "pass", "PASS"})


def _minimum_commitment_marker(round_num):
    """最小承诺是脚本边界标记，不消费善后 LLM payload。"""
    return f"[最小承诺] R{int(round_num):06d} / post / status=closed"


def _drop_retired_cleanup_fields(parsed):
    """Spec 041/062：善后 LLM 退役写端解析到也不再生效。"""
    parsed["keywords"] = []
    parsed["state_updates"] = []
    parsed["archive_title"] = ""
    parsed["archive_subject"] = None
    parsed["archive_weight"] = None
    parsed["archive_body"] = ""
    parsed["archive_type"] = "memory"
    parsed["archive_linked"] = []
    parsed["faults"] = ""
    parsed["_legacy_interaction"] = []
    parsed["_legacy_relation"] = []


def validate_contact_connectivity(bridges, current_round_mem_ids):
    """光锥约束校验：所有联系集词对必须能通过图连通到本轮记忆条目。
    设计来源: 备忘录_联想联系索引体系_20260502.md §三
    BFS 从本轮记忆ID出发，两边都不在visited中的词对→拒绝。
    返回 (valid_bridges, invalid_bridges)。"""
    if not bridges:
        return [], []
    if not current_round_mem_ids:
        return [], bridges
    # 建无向图: {entry_id: {connected_entry_ids}}
    graph = {}
    for b in bridges:
        ea = b.get("entry_a", "")
        eb = b.get("entry_b", "")
        if ea and eb:
            graph.setdefault(ea, set()).add(eb)
            graph.setdefault(eb, set()).add(ea)
    # BFS 从本轮记忆ID出发
    visited = set()
    queue = list(current_round_mem_ids)
    while queue:
        node = queue.pop(0)
        if node in visited:
            continue
        visited.add(node)
        for neighbor in graph.get(node, []):
            if neighbor not in visited:
                queue.append(neighbor)
    # 校验：至少一边在visited中 → 合法
    valid, invalid = [], []
    for b in bridges:
        ea = b.get("entry_a", "")
        eb = b.get("entry_b", "")
        if (not ea and not eb) or ea in visited or eb in visited:
            valid.append(b)
        else:
            invalid.append(b)
    return valid, invalid


def connection_entry_ids(bridges):
    entry_ids = set()
    for bridge in bridges or []:
        for key in ("entry_a", "entry_b"):
            value = str(bridge.get(key) or "").strip()
            if value and value not in EMPTY_CELLS:
                entry_ids.add(value)
    return entry_ids


def build_training_material_evidence(result):
    """收集善后训练材料事实证据包；脚本只收事实，不做语义自评。"""
    result = result if isinstance(result, dict) else {}
    preselection = []
    seen_preselected = set()
    for selection in result.get("_preselection_evidence", []) or []:
        item_id = str(selection.get("item_id") or "").strip()
        if item_id in SKIP_PRESELECTION_ITEMS or item_id in seen_preselected:
            continue
        seen_preselected.add(item_id)
        preselection.append({
            "item_id": item_id,
            "item_type": str(selection.get("item_type") or ""),
            "origin": str(selection.get("origin") or "setup_preselection"),
            "selection_trigger": str(selection.get("selection_trigger") or "setup_mount"),
            "surface": str(selection.get("surface") or item_id),
            "reaction_adoption_signals": list(selection.get("reaction_adoption_signals") or []),
            "evidence_refs": list(selection.get("evidence_refs") or [f"setup_mount:{item_id}"]),
            "privacy_scope": str(selection.get("privacy_scope") or "runtime_visible"),
        })
    for mem_id in result.get("_mounted_memories", []) or []:
        mem_id = str(mem_id or "").strip()
        if mem_id in SKIP_PRESELECTION_ITEMS or mem_id in seen_preselected:
            continue
        seen_preselected.add(mem_id)
        preselection.append({
            "item_id": mem_id,
            "item_type": "memory",
            "origin": "setup_preselection",
            "selection_trigger": "setup_mount",
            "surface": mem_id,
            "reaction_adoption_signals": [],
            "evidence_refs": [f"setup_mount:{mem_id}"],
            "privacy_scope": "runtime_visible",
        })

    candidates = []
    added_traces = []
    seen_candidates = set()
    seen_traces = set()

    def add_candidate(entry_id, source, title="", keywords=None, anchor=False, receipt_ref=""):
        entry_id = str(entry_id or "").strip()
        if not entry_id or entry_id in seen_candidates:
            return
        seen_candidates.add(entry_id)
        candidates.append({
            "entry_id": entry_id,
            "entry_source": source,
            "title_or_summary": str(title or entry_id),
            "keywords": list(keywords or []),
            "is_current_anchor": bool(anchor),
            "source_receipt": receipt_ref,
            "evidence_refs": [receipt_ref] if receipt_ref else [],
        })

    def add_trace(item_id, item_type, source, evidence_ref):
        item_id = str(item_id or "").strip()
        if not item_id or item_id in seen_preselected or evidence_ref in seen_traces:
            return
        seen_traces.add(evidence_ref)
        added_traces.append({
            "item_id": item_id,
            "item_type": item_type,
            "source": source,
            "evidence_ref": evidence_ref,
        })

    for receipt in result.get("_memory_write_receipts", []) or []:
        if receipt.get("status") != "applied" or not receipt.get("mem_id"):
            continue
        mem_id = receipt.get("mem_id")
        ref = f"memory_write:{mem_id}"
        add_candidate(
            mem_id,
            "memory_write_receipt",
            title=receipt.get("title", ""),
            keywords=receipt.get("keywords") or [],
            anchor=True,
            receipt_ref=ref,
        )
        add_trace(mem_id, "memory", "memory_write_receipt", ref)

    for receipt in result.get("_memory_content_read_receipts", []) or []:
        if receipt.get("status") not in {"accepted", "applied"} or not receipt.get("mem_id"):
            continue
        mem_id = receipt.get("mem_id")
        ref = f"memory_content_read:{mem_id}"
        meta = receipt.get("meta") if isinstance(receipt.get("meta"), dict) else {}
        add_candidate(
            mem_id,
            "memory_content_read_receipt",
            title=meta.get("title", ""),
            keywords=meta.get("tags") or [],
            anchor=False,
            receipt_ref=ref,
        )
        add_trace(mem_id, "memory", "memory_content_read_receipt", ref)

    return {
        "preselection_evidence": preselection,
        "connection_candidate_entries": candidates,
        "added_prework_traces": added_traces,
    }


def _prework_trace_index(result):
    evidence = build_training_material_evidence(result)
    item_ids = {
        trace["item_id"]
        for trace in evidence.get("added_prework_traces", [])
        if trace.get("item_id")
    }
    refs = {
        trace["evidence_ref"]
        for trace in evidence.get("added_prework_traces", [])
        if trace.get("evidence_ref")
    }
    return item_ids, refs


def _pairwise(values):
    values = [str(value or "").strip() for value in values or [] if str(value or "").strip()]
    pairs = []
    for index, left in enumerate(values):
        for right in values[index + 1:]:
            if left != right:
                pairs.append((left, right))
    return pairs


def build_association_counts_from_receipts(memory_write_receipts):
    """联想集五表：只从本轮有效 memory_write 回执做脚本计数。"""
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
        interaction_feelings = list(dict.fromkeys(receipt.get("interaction_feelings") or []))
        relationship_feelings = []
        for item in receipt.get("relationship_feelings") or []:
            if not isinstance(item, dict):
                continue
            subject = str(item.get("subject") or "").strip()
            feeling = str(item.get("word") or "").strip()
            if subject and feeling and (subject, feeling) not in relationship_feelings:
                relationship_feelings.append((subject, feeling))
        # 历史 receipt 只有 relation_feelings 字符串，归属回退到记忆 subject。
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


def _has_prework_ref(item_id, evidence_refs, prework_ids, prework_refs):
    if item_id in prework_ids:
        return True
    for ref in evidence_refs or []:
        if ref in prework_refs:
            return True
        if str(ref).startswith(PREWORK_EVIDENCE_PREFIXES):
            return True
    return False


def validate_tacit_associations(tacit_associations, valid_connection_bridges, result):
    """联系集先行后校验默契集：kept 看承接，dropped 看明放弃/未命中，added 看前置痕迹。"""
    connection_ids = connection_entry_ids(valid_connection_bridges)
    prework_ids, prework_refs = _prework_trace_index(result)
    valid = []
    warnings = []
    for item in tacit_associations or []:
        item = dict(item)
        item_id = str(item.get("item_id") or "").strip()
        action = item.get("action", "")
        evidence_refs = list(item.get("evidence_refs") or [])
        drop_reason = str(item.get("drop_reason") or "").strip()
        if not item_id or action not in TACIT_ACTIONS:
            continue
        if action == "kept":
            has_connection = item_id in connection_ids or f"connection:{item_id}" in evidence_refs
            has_prework = _has_prework_ref(item_id, evidence_refs, prework_ids, prework_refs)
            if not has_connection and not has_prework:
                warnings.append(f"默契集kept无承接证据: {item_id}")
                continue
        elif action == "dropped":
            explicit = any(token in str(item.get("note") or "") for token in ("明确放弃", "取消挂载", "放弃"))
            if not drop_reason and item_id not in connection_ids and not explicit:
                item["drop_reason"] = "no_valid_connection_hit"
            elif item_id in connection_ids and not explicit and drop_reason != "explicit_abandon":
                warnings.append(f"默契集dropped仍在联系图命中: {item_id}")
                continue
        elif action == "added":
            if not _has_prework_ref(item_id, evidence_refs, prework_ids, prework_refs):
                warnings.append(f"默契集added缺少前置痕迹: {item_id}")
                continue
        if "item_type" not in item:
            item["item_type"] = "memory" if item_id.startswith("MEM-") else ""
        if "evidence_refs" not in item:
            item["evidence_refs"] = evidence_refs
        if "drop_reason" not in item:
            item["drop_reason"] = drop_reason
        valid.append(item)
    return valid, warnings


def _apply_declared_relation_card(declaration, direct_object, identity_status,
                                  relation_store=None):
    """执行反应步显式建卡声明，拒绝从语料元数据推断建卡。"""
    if relation_store is None or not isinstance(declaration, dict):
        return None
    name = relation_public_name(declaration.get("name"))
    category = str(declaration.get("category") or "them").strip() or "them"
    if not name or name in {"无", "—", "-", "unknown", "未知"}:
        return None
    if identity_status not in {"declared", "known", "unregistered"}:
        return None

    direct = relation_public_name(direct_object)
    if not direct or direct in {"无", "—", "-", "unknown", "未知"}:
        return None
    if identity_status == "unregistered" and direct != name:
        return None
    if category != "orgs" and direct and direct != name:
        return None

    card_entry = None
    if hasattr(relation_store, "find_card"):
        card_entry = relation_store.find_card(name)
    if card_entry:
        card_id = card_entry.get("id")
    else:
        card_id = name
        relation_store.create_card(card_id, name, category)

    note = str(declaration.get("note") or "").strip()
    if note and hasattr(relation_store, "add_note"):
        relation_store.add_note(card_id, note[:512])

    return card_id


def _is_archive_scaffold_only(value):
    normalized = re.sub(r"\s+", "", str(value or "").strip())
    normalized = normalized.strip("。.:：；;，,")
    return normalized in {"记忆条目", "新建记忆条目", "归档条目", "记忆归档"}


def process_cleanup(parsed, state, round_num, result, data_modules):
    """
    执行善后步落盘——分派到各 data/ logic 管线。
    data_modules: {"state_store", "memory_store", "memory_index",
                    "container_store", "context_store", "feeling_table", "feeling_buffer"}
    返回: {"errors": [...], "warnings": [...], "memory_ids": [...], "orphans": [...]}
    """
    parsed = dict(parsed or {})
    _drop_retired_cleanup_fields(parsed)
    report = {"errors": [], "warnings": [], "memory_ids": [], "orphans": [], "popup": None}
    ms = data_modules["memory_store"]
    mi = data_modules["memory_index"]
    cs = data_modules["container_store"]
    ctx = data_modules["context_store"]
    interaction_meta = result.get("_interaction_meta", {}) if isinstance(result, dict) else {}
    cache_meta = cache_interaction_meta(interaction_meta)
    for receipt in result.get("_memory_write_receipts", []) if isinstance(result, dict) else []:
        if receipt.get("status") == "applied" and receipt.get("mem_id"):
            report["memory_ids"].append(receipt["mem_id"])
            report.setdefault("_memory_write_receipts", []).append(receipt)
    association_counts = build_association_counts_from_receipts(
        report.get("_memory_write_receipts", []))
    if association_counts:
        report["_association_counts"] = association_counts
    for receipt in result.get("_relation_card_receipts", []) if isinstance(result, dict) else []:
        if receipt.get("status") in {"applied", "degraded"} and receipt.get("card_id"):
            report.setdefault("_relation_card_receipts", []).append(receipt)
    connection_anchor_ids = list(report["memory_ids"])
    for receipt in result.get("_memory_content_read_receipts", []) if isinstance(result, dict) else []:
        if receipt.get("status") in {"accepted", "applied"} and receipt.get("mem_id"):
            connection_anchor_ids.append(receipt["mem_id"])

    now = datetime.now()

    # --- 1. 联系集连通性校验（基于 reaction memory_write 回执中的本轮记忆） ---
    if parsed.get("connection_bridges"):
        valid, invalid = validate_contact_connectivity(
            parsed["connection_bridges"], connection_anchor_ids)
        if invalid:
            report["warnings"].append(
                f"联系集光锥校验: 过滤 {len(invalid)} 条未连通或无本轮记忆回执词对")
        report["_connection_bridges"] = valid
    else:
        report["_connection_bridges"] = []

    # --- 2. 默契集：联系图先行后落 kept/dropped/added ---
    if parsed.get("tacit_associations"):
        tacit_valid, tacit_warnings = validate_tacit_associations(
            parsed["tacit_associations"],
            report.get("_connection_bridges", []),
            result,
        )
        report["warnings"].extend(tacit_warnings)
        if tacit_valid:
            report["_tacit_associations"] = tacit_valid

    # --- 脚本边界标记：最小承诺不再由善后 LLM 填表 ---
    try:
        if not hasattr(ctx, "append_to_cache"):
            report["warnings"].append("最小承诺写端缺失")
        else:
            ctx.append_to_cache(
                round_num,
                "system",
                _minimum_commitment_marker(round_num),
                kind="minimum_commitment",
                step="cleanup",
                **cache_meta)
    except Exception as e:
        report["errors"].append(f"最小承诺写入失败: {e}")

    # --- 悬空容器检测（DDS §25.9） ---
    try:
        round_start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        orphans, popup_text = scan_orphans(cs, round_start)
        if orphans:
            report["orphans"] = orphans
            report["popup"] = popup_text
    except Exception as e:
        report["errors"].append(f"悬空检测异常: {e}")

    # --- 容器创建校验：验证本轮建的容器有内容挂载 ---
    created = result.get("_created_containers", [])
    for cid in created:
        try:
            info = cs.get_container_info(cid)
            entries = info.get("entries", [])
            if not entries or len(entries) == 0:
                report["warnings"].append(f"容器 {cid} 创建后无内容挂载")
            else:
                report.setdefault("containers_created", []).append(cid)
        except Exception:
            report["warnings"].append(f"容器 {cid} 注册信息读取失败")

    return report
