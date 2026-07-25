"""反应步 memory_write 协议工具落盘管线。"""
import re

from logic.feeling_lookup import FeelingWordTable
from logic.mem_id import generate_mem_id, make_heat_entry, make_meta_template


EMPTY_VALUES = {"", "无", "暂无", "略", "不需要", "免", "none", "None", "-", "—"}
SUBJECT_RESOLUTION_ERRORS = {
    "identity_unresolved",
    "subject_not_in_relation_domain",
    "subject_not_confirmed",  # historical receipt compatibility
}


def _is_empty(value):
    return str(value or "").strip() in EMPTY_VALUES


def _clean_list(values):
    if values is None:
        return []
    if isinstance(values, str):
        values = re.split(r"[,，、;；\n]+", values)
    result = []
    for value in values:
        item = str(value or "").strip().strip("`")
        if not _is_empty(item) and item not in result:
            result.append(item)
    return result


def _presence_subjects(state):
    if not isinstance(state, dict) or "presence" not in state:
        return None
    presence = state.get("presence") or {}
    if not isinstance(presence, dict):
        return []
    return _clean_list(presence.get("confirmed_subjects"))


def _resolve_subject(declaration, state, relation_store):
    raw_subject = str(declaration.get("subject") or "unknown").strip() or "unknown"
    confirmed_subjects = _presence_subjects(state) or []
    interaction_object = confirmed_subjects[0] if confirmed_subjects else ""
    if raw_subject in {"unknown", "Unknown", "UNKNOWN"} or _is_empty(raw_subject):
        if not interaction_object:
            return raw_subject, "identity_unresolved"
        candidate = interaction_object
    else:
        candidate = raw_subject
    resolver = getattr(relation_store, "resolve_active_subject", None)
    resolved = resolver(candidate) if callable(resolver) else None
    if not resolved:
        return candidate, "subject_not_in_relation_domain"
    return resolved, ""


def _subject_resolution_context(declaration, state, reason, resolved_subject=""):
    if reason not in SUBJECT_RESOLUTION_ERRORS:
        return {}
    submitted = str(declaration.get("subject") or "unknown").strip() or "unknown"
    confirmed_subjects = _presence_subjects(state)
    if confirmed_subjects is None:
        confirmed_subjects = []
    confirmed = confirmed_subjects[0] if confirmed_subjects else ""
    return {
        "submitted_subject": submitted,
        "interaction_object": confirmed,
        "confirmed_subject": confirmed,
        "confirmed_subjects": confirmed_subjects,
        "resolved_subject": resolved_subject,
    }


def _weight(value):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 2
    return max(0, min(5, number))


def _keyword_limit(weight):
    if weight >= 5:
        return 8
    if weight >= 3:
        return 6
    return 4


def _normalize_keywords(declaration, weight):
    """清洗候选关键词并按 F/S/A 上限裁剪，不替 LLM 补语义词。"""
    keywords = _clean_list(declaration.get("candidate_keywords"))
    return keywords[:_keyword_limit(weight)]


def _feeling_rejection(domain, index, reason, **details):
    rejection = {"domain": domain, "index": index, "reason": reason}
    rejection.update({key: value for key, value in details.items() if value not in (None, "")})
    return rejection


def _normalize_feelings(declaration, relation_store):
    """逐项保留合法感受；局部错误不阻断记忆正文。"""
    table = FeelingWordTable()
    interaction = []
    relationships = []
    rejections = []

    raw_interaction = declaration.get("interaction_feelings")
    if raw_interaction is None:
        raw_interaction = []
    if not isinstance(raw_interaction, list):
        rejections.append(_feeling_rejection(
            "interaction", 0, "invalid_shape", submitted=raw_interaction))
        raw_interaction = []
    for index, value in enumerate(raw_interaction):
        if not isinstance(value, str) or not value.strip():
            rejections.append(_feeling_rejection(
                "interaction", index, "invalid_word", submitted=value))
            continue
        word = value.strip()
        if not table.lookup_interaction([word]):
            rejections.append(_feeling_rejection(
                "interaction", index, "feeling_not_in_table", word=word))
            continue
        if word in interaction:
            rejections.append(_feeling_rejection(
                "interaction", index, "duplicate_feeling", word=word))
            continue
        if len(interaction) >= 3:
            rejections.append(_feeling_rejection(
                "interaction", index, "interaction_limit_exceeded", word=word))
            continue
        interaction.append(word)

    if "relation_feelings" in declaration:
        rejections.append(_feeling_rejection(
            "relationship", 0, "legacy_field_not_accepted"))
    raw_relationships = declaration.get("relationship_feelings")
    if raw_relationships is None:
        raw_relationships = []
    if not isinstance(raw_relationships, list):
        rejections.append(_feeling_rejection(
            "relationship", 0, "invalid_shape", submitted=raw_relationships))
        raw_relationships = []

    per_subject = {}
    seen = set()
    resolver = getattr(relation_store, "resolve_active_subject", None)
    for index, value in enumerate(raw_relationships):
        if not isinstance(value, dict) or set(value) != {"subject", "word"}:
            rejections.append(_feeling_rejection(
                "relationship", index, "invalid_shape", submitted=value))
            continue
        submitted_subject = str(value.get("subject") or "").strip()
        word = str(value.get("word") or "").strip()
        if not submitted_subject or not word:
            rejections.append(_feeling_rejection(
                "relationship", index, "missing_subject_or_word",
                submitted_subject=submitted_subject, word=word))
            continue
        subject = resolver(submitted_subject) if callable(resolver) else None
        if not subject:
            rejections.append(_feeling_rejection(
                "relationship", index, "subject_not_in_relation_domain",
                submitted_subject=submitted_subject, word=word))
            continue
        if table.lookup_relation(word) is None:
            rejections.append(_feeling_rejection(
                "relationship", index, "feeling_not_in_table",
                submitted_subject=submitted_subject, resolved_subject=subject, word=word))
            continue
        key = (subject, word)
        if key in seen:
            rejections.append(_feeling_rejection(
                "relationship", index, "duplicate_feeling",
                resolved_subject=subject, word=word))
            continue
        if per_subject.get(subject, 0) >= 2:
            rejections.append(_feeling_rejection(
                "relationship", index, "per_subject_limit_exceeded",
                resolved_subject=subject, word=word))
            continue
        seen.add(key)
        per_subject[subject] = per_subject.get(subject, 0) + 1
        relationships.append({"subject": subject, "word": word})

    return interaction, relationships, rejections


def _receipt(status, declaration, mem_id=None, keywords=None, reason="",
             subject_context=None):
    receipt = {
        "tool_id": "memory_write",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "status": status,
        "source": "memory_write_declaration",
        "mem_id": mem_id,
        "title": str(declaration.get("title") or "")[:16],
        "weight": declaration.get("weight"),
        "subject": declaration.get("subject"),
        "keywords": keywords or [],
        "reason": reason,
    }
    if reason in SUBJECT_RESOLUTION_ERRORS:
        receipt.update(subject_context or {})
    if str(reason or "").startswith("memory_body_too_long:"):
        receipt.update(_body_too_long_feedback(reason))
    interaction_feelings = declaration.get("interaction_feelings") or []
    relationship_feelings = declaration.get("relationship_feelings") or []
    feeling_rejections = declaration.get("feeling_rejections") or []
    if interaction_feelings:
        receipt["interaction_feelings"] = list(interaction_feelings)
    if relationship_feelings:
        receipt["relationship_feelings"] = [dict(item) for item in relationship_feelings]
    if feeling_rejections:
        receipt["feeling_rejections"] = [dict(item) for item in feeling_rejections]
    return receipt


def _body_too_long_feedback(reason):
    match = re.search(r"max=(\d+);actual=(\d+)", str(reason or ""))
    if not match:
        return {
            "next_action": "compress_body_or_adjust_weight",
            "retry_instruction": (
                "memory_write.body 超出当前权重上限。"
                "请压缩正文或调整 weight 后重新调用 memory_write。"
                "不要只因字数升权。"
            ),
        }
    max_chars = int(match.group(1))
    actual_chars = int(match.group(2))
    over_by = max(0, actual_chars - max_chars)
    target_chars = max(0, max_chars - min(8, max_chars))
    reduce_by = max(0, actual_chars - target_chars)
    result = {
        "max_chars": max_chars,
        "actual_chars": actual_chars,
        "over_by": over_by,
        "target_chars": target_chars,
        "reduce_by": reduce_by,
        "next_action": "compress_body_or_adjust_weight",
        "retry_instruction": (
            "memory_write.body 超出当前权重上限："
            f"actual={actual_chars}, max={max_chars}。"
            "请压缩正文或调整 weight 后重新调用 memory_write。"
            "不要只因字数升权。"
        ),
    }
    return result


def apply_memory_write_declarations(declarations, state, round_num, data_modules):
    """逐条校验并写入 memory_write 声明，返回同步回执列表。"""
    receipts = []
    if not declarations:
        return receipts

    memory_store = data_modules["memory_store"]
    memory_index = data_modules["memory_index"]
    memory_heat = data_modules["memory_heat"]
    container_store = data_modules.get("container_store")
    relation_store = data_modules.get("relation_store")

    for declaration in declarations:
        if not isinstance(declaration, dict):
            continue
        title = str(declaration.get("title") or "").strip()[:16]
        body = str(declaration.get("body") or "").strip()
        weight = _weight(declaration.get("weight"))
        subject, subject_error = _resolve_subject(
            declaration, state, relation_store)
        subject_context = _subject_resolution_context(
            declaration, state, subject_error,
            resolved_subject=subject if not subject_error else "")
        keywords = _normalize_keywords(declaration, weight)
        linked = []
        interaction_feelings, relationship_feelings, feeling_rejections = (
            _normalize_feelings(declaration, relation_store)
        )
        feelings = interaction_feelings + [
            f"{item['subject']}:{item['word']}"
            for item in relationship_feelings
        ]

        normalized = dict(declaration)
        normalized["title"] = title
        normalized["weight"] = weight
        normalized["subject"] = subject
        normalized["interaction_feelings"] = interaction_feelings
        normalized["relationship_feelings"] = relationship_feelings
        normalized["feeling_rejections"] = feeling_rejections
        normalized.pop("relation_feelings", None)

        if weight <= 0:
            receipts.append(_receipt("skipped", normalized, keywords=keywords, reason="weight_zero"))
            continue
        if subject_error:
            receipts.append(_receipt(
                "error",
                normalized,
                keywords=keywords,
                reason=subject_error,
                subject_context=subject_context))
            continue
        if _is_empty(title) or _is_empty(body):
            receipts.append(_receipt("error", normalized, keywords=keywords, reason="missing_title_or_body"))
            continue
        if not keywords:
            receipts.append(_receipt("error", normalized, keywords=[], reason="missing_keywords"))
            continue
        if "dream" in declaration:
            receipts.append(_receipt("error", normalized, keywords=keywords, reason="unsupported_field_dream"))
            continue

        try:
            mem_id = generate_mem_id()
            memory_store.write_entry(
                mem_id,
                title,
                summary=body,
                weight=weight,
                tags=keywords,
                linked_containers=linked,
                feelings=feelings if feelings else None,
                delta_desc="",
                subject=subject,
                round_num=round_num,
                dream=False,
                current_overview="",
            )

            meta = make_meta_template(mem_id, title=title, weight=weight,
                                      subject=subject)
            meta["tags"] = keywords
            meta["created_round"] = round_num
            meta["last_recalled_round"] = round_num
            meta["linked_containers"] = linked
            memory_store.set_meta(mem_id, meta)
            memory_store.append_index(
                mem_id, meta.get("type", "memory"), weight, title,
                subject=subject, round_num=round_num,
                dream=False, current_overview="")
            memory_heat.set_entry(mem_id, make_heat_entry(weight=weight))
            if keywords:
                memory_index.add_stm_keywords(mem_id, keywords)

            for container_id in linked:
                if container_store is None:
                    continue
                try:
                    container_store.append_entry(container_id, mem_id, title,
                                                 file_name="open.md")
                except Exception:
                    pass

            receipts.append(_receipt("applied", normalized, mem_id=mem_id, keywords=keywords))
        except Exception as exc:
            receipts.append(_receipt("error", normalized, keywords=keywords, reason=str(exc)))

    return receipts
