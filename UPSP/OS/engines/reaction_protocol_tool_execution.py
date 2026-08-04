"""Execution helpers for reaction protocol read/write tools."""

import json
import re

from assembly.context_helpers import normalize_active_corpus_id
from data.memory_index import MemoryIndex
from data.relation_store import RelationStore
from logic.cleanup_processor import _apply_declared_relation_card


RELATION_STATE_OR_AXIS_WRITE_FIELDS = {
    "axes", "axis", "relation_axes", "relation_axis", "score", "scores",
    "status", "state", "trust", "safety", "value", "intimacy",
    "conflict", "repair", "关系六轴", "关系轴", "关系状态", "状态",
    "数值", "分数", "信任", "安全", "价值", "亲密", "冲突", "修复",
}


def apply_corpus_read_requests(assembler, requests):
    receipts = []
    registry = getattr(assembler, "_active_corpus_registry", {}) or {}
    pending_keys = getattr(assembler, "_pending_corpus_expand_once_keys", None)
    if pending_keys is None:
        pending_keys = set()
        setattr(assembler, "_pending_corpus_expand_once_keys", pending_keys)

    for request in requests or []:
        corpus_id = normalize_active_corpus_id(
            request.get("corpus_id") if isinstance(request, dict) else ""
        )
        if not isinstance(request, dict) or not corpus_id:
            receipts.append({
                "tool_id": "corpus_read",
                "tool_family": "protocol_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "source": "protocol_tool_request",
                "reason": "missing_corpus_id",
                "protocol_tool_receipt": True,
            })
            continue
        target = registry.get(corpus_id)
        if not isinstance(target, dict):
            receipts.append({
                "tool_id": "corpus_read",
                "tool_family": "protocol_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "source": "protocol_tool_request",
                "reason": "corpus_id_not_active",
                "corpus_id": corpus_id,
                "protocol_tool_receipt": True,
            })
            continue
        target_kind = str(target.get("kind") or "").strip()
        if target_kind != "dialogue_progress":
            receipts.append({
                "tool_id": "corpus_read",
                "tool_family": "protocol_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "source": "protocol_tool_request",
                "reason": "corpus_kind_not_expandable",
                "corpus_id": corpus_id,
                "target_kind": target_kind,
                "protocol_tool_receipt": True,
            })
            continue
        entry_key = str(target.get("entry_key") or "").strip()
        if not entry_key:
            receipts.append({
                "tool_id": "corpus_read",
                "tool_family": "protocol_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "source": "protocol_tool_request",
                "reason": "corpus_entry_key_missing",
                "corpus_id": corpus_id,
                "protocol_tool_receipt": True,
            })
            continue
        pending_keys.add(entry_key)
        receipts.append({
            "tool_id": "corpus_read",
            "tool_family": "protocol_tool",
            "tool_class": "read_tool",
            "status": "accepted",
            "source": "protocol_tool_request",
            "corpus_id": corpus_id,
            "target_kind": "dialogue_progress",
            "expand_lifecycle": "next_provider_call_once",
            "protocol_tool_receipt": True,
        })
    return receipts


def apply_index_view_requests(
    assembler,
    requests,
    state,
    round_type,
    mount_ids,
    interaction_meta,
    hidden_stm_memory_ids=None,
):
    receipts = []
    for request in requests or []:
        if not isinstance(request, dict):
            receipts.append({
                "tool_id": "index_view",
                "tool_family": "protocol_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "source": "protocol_tool_request",
                "reason": "invalid_request",
            })
            continue
        try:
            receipt = assembler.build_index_view(
                scope=request.get("scope", ""),
                zone=request.get("zone", ""),
                offset=request.get("offset", 0),
                limit=request.get("limit", 8),
                current_input_text=getattr(assembler, "_current_input_text", None),
                interaction_meta=interaction_meta or {},
                input_keywords=assembler._derive_input_keywords(
                    state, "reaction", mount_ids),
                hidden_stm_memory_ids=hidden_stm_memory_ids,
            )
        except Exception as exc:
            receipt = {
                "tool_id": "index_view",
                "tool_family": "protocol_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "source": "protocol_tool_request",
                "reason": str(exc) or "index_view_failed",
                "content": "",
            }
        receipts.append(receipt)
    return receipts


def apply_relation_card_declarations(
    declarations,
    interaction_meta,
    *,
    guard,
    visible_relation_body_ids=None,
    relation_store_factory=RelationStore,
    relation_index_factory=MemoryIndex,
    apply_declared_relation_card=_apply_declared_relation_card,
):
    declarations = declarations or []
    if not declarations:
        return []
    guard = guard or {}
    base_receipt = {
        "tool_id": "relation_card_write",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "source": "relation_card_declaration",
    }
    if guard.get("single_declaration", True) and len(declarations) > 1:
        return [dict(
            base_receipt,
            status="multiple_relation_card_declarations",
            reason="multiple_relation_card_declarations",
        )]

    receipts = []
    relation_store_factory = relation_store_factory or RelationStore
    relation_index_factory = relation_index_factory or MemoryIndex
    apply_declared_relation_card = (
        apply_declared_relation_card or _apply_declared_relation_card
    )
    relation_store = relation_store_factory()
    visible_relation_body_ids = {
        str(item or "").strip()
        for item in (visible_relation_body_ids or set())
        if str(item or "").strip()
    }
    direct_object = str(
        (interaction_meta or {}).get("interaction_object") or "").strip()
    identity_status = str(
        (interaction_meta or {}).get("identity_status") or "").strip()
    for declaration in declarations or []:
        receipt = {
            "tool_id": "relation_card_write",
            "tool_family": "protocol_tool",
            "tool_class": "sync_tool",
            "status": "rejected",
            "source": "relation_card_declaration",
        }
        forbidden_fields = set(declaration.get("forbidden_fields") or [])
        forbidden_fields.update(
            key for key in declaration
            if key in RELATION_STATE_OR_AXIS_WRITE_FIELDS
        )
        if forbidden_fields:
            receipt["status"] = "rejected_state_or_axis_write"
            receipt["reason"] = "rejected_state_or_axis_write"
            receipt["forbidden_fields"] = sorted(forbidden_fields)
            receipts.append(receipt)
            continue
        name = str(declaration.get("name") or "").strip()
        if guard.get("single_target", True) and re.search(r"[,，、\n]", name):
            receipt["status"] = "multiple_relation_targets"
            receipt["reason"] = "multiple_relation_targets"
            receipts.append(receipt)
            continue
        existing_card = None
        if name and hasattr(relation_store, "find_card"):
            existing_card = relation_store.find_card(name)
        if existing_card:
            card_id = str(existing_card.get("id") or "").strip()
            card_name = str(existing_card.get("name") or "").strip()
            action = str(declaration.get("action") or "append_note").strip().lower()
            if action == "create":
                receipt["status"] = "relation_card_exists"
                receipt["reason"] = "relation_card_exists"
                receipt["card_id"] = card_id
                receipts.append(receipt)
                continue
            visible_aliases = {card_id, card_name, f"REL-{card_name}" if card_name else ""}
            if not (visible_relation_body_ids & {item for item in visible_aliases if item}):
                receipt["status"] = "relation_body_not_visible"
                receipt["reason"] = "relation_body_not_visible"
                receipt["card_id"] = card_id
                receipts.append(receipt)
                continue
        delta_chars = len(str(declaration.get("note") or ""))
        max_delta_chars = int(guard.get("max_delta_chars", 0) or 0)
        if (
            guard.get("large_delta_guard", False)
            and max_delta_chars > 0
            and delta_chars > max_delta_chars
        ):
            receipt["status"] = "needs_review"
            receipt["reason"] = "large_delta_guard"
            receipt["delta_chars"] = delta_chars
            receipt["max_delta_chars"] = max_delta_chars
            receipts.append(receipt)
            continue
        try:
            card_id = apply_declared_relation_card(
                declaration,
                direct_object,
                identity_status,
                relation_store=relation_store,
            )
            if card_id:
                receipt["status"] = "applied"
                receipt["card_id"] = card_id
                try:
                    relation_index_factory().add_relation_keywords(name, [name])
                except Exception as exc:
                    receipt.update({
                        "status": "degraded",
                        "reason": "relation_index_write_failed",
                        "repair_debt": {
                            "kind": "relation_keyword_index_rebuild",
                            "card_id": card_id,
                            "error_type": type(exc).__name__,
                        },
                    })
            else:
                receipt["reason"] = "identity_or_subject_mismatch"
        except Exception as exc:
            receipt["status"] = "processor_error"
            receipt["detail"] = str(exc)
        receipts.append(receipt)
    return receipts


def visible_relation_body_ids_from_mounts(mount_ids):
    visible = set()
    for item in mount_ids or []:
        if not isinstance(item, dict) or item.get("type") != "relation":
            continue
        ids = item.get("ids")
        values = ids if isinstance(ids, (list, tuple, set)) else [ids]
        for value in values:
            text = str(value or "").strip()
            if text:
                visible.add(text)
    return visible


def reasoning_context_native_replay(envelopes, general_results, protocol_receipts):
    replay_envelopes = [
        envelope for envelope in envelopes or []
        if isinstance(envelope, dict)
        and str(envelope.get("reasoning_content") or "").strip()
    ]
    if not replay_envelopes:
        return {}
    reasoning_content = str(
        replay_envelopes[0].get("reasoning_content") or "").strip()
    if not reasoning_content:
        return {}
    tool_calls = []
    tool_results = []
    for envelope in replay_envelopes:
        tool_call = native_replay_tool_call(envelope)
        tool_result = native_replay_tool_result(
            envelope,
            general_results,
            protocol_receipts,
        )
        if not tool_call or not tool_result:
            return {}
        tool_calls.append(tool_call)
        tool_results.append(tool_result)
    if not tool_calls or not tool_results:
        return {}
    assistant_message = {
        "role": "assistant",
        "content": str(replay_envelopes[0].get("message_content") or ""),
        "reasoning_content": reasoning_content,
        "tool_calls": tool_calls,
    }
    return {
        "provider": "openai_chat",
        "assistant_message": assistant_message,
        "tool_results": tool_results,
    }


def native_replay_tool_call(envelope):
    native_call = envelope.get("native_tool_call")
    if isinstance(native_call, dict):
        return dict(native_call)
    call_id = str(envelope.get("call_id") or "").strip()
    tool_id = str(envelope.get("tool_id") or "").strip()
    arguments = envelope.get("arguments_json")
    if not call_id or not tool_id:
        return {}
    if not isinstance(arguments, str) or not arguments.strip():
        try:
            arguments = json.dumps(
                envelope.get("arguments") or {},
                ensure_ascii=False,
                sort_keys=True,
            )
        except (TypeError, ValueError):
            arguments = "{}"
    return {
        "id": call_id,
        "type": "function",
        "function": {
            "name": tool_id,
            "arguments": arguments,
        },
    }


def native_replay_tool_result(envelope, general_results, protocol_receipts):
    call_id = str(envelope.get("call_id") or "").strip()
    if not call_id:
        return {}
    result = matching_tool_result(call_id, general_results, protocol_receipts)
    if result is None:
        return {}
    content = minimal_native_tool_result_content(
        result,
        fallback_tool_id=str(envelope.get("tool_id") or "").strip(),
    )
    return {
        "role": "tool",
        "tool_call_id": call_id,
        "content": content,
    }


def matching_tool_result(call_id, general_results, protocol_receipts):
    for item in list(general_results or []) + list(protocol_receipts or []):
        if not isinstance(item, dict):
            continue
        item_call_id = str(
            item.get("call_id")
            or item.get("tool_call_id")
            or item.get("provider_item_id")
            or ""
        ).strip()
        if item_call_id == call_id:
            return item
    return None


def minimal_native_tool_result_content(result, fallback_tool_id=""):
    result = result if isinstance(result, dict) else {}
    payload = {
        "tool_id": str(result.get("tool_id") or fallback_tool_id or "").strip(),
        "status": str(result.get("status") or "processed").strip(),
    }
    for key in ("reason", "path", "url", "command", "evidence_refs"):
        value = result.get(key)
        if value not in (None, "", []):
            payload[key] = value
    hint = model_visible_error_hint(result)
    if hint:
        payload["error_hint"] = hint
    return json.dumps(payload, ensure_ascii=False, sort_keys=True)


def model_visible_error_hint(result):
    """Project one safe, actionable contract from any failed native receipt."""
    result = result if isinstance(result, dict) else {}
    status = str(result.get("status") or "").lower()
    reason = str(result.get("reason") or "").lower()
    if status in {"ok", "success", "accepted", "applied", "guide_loaded"}:
        return {}
    if not status and not reason:
        return {}
    details = result.get("details") if isinstance(result.get("details"), dict) else {}
    existing = result.get("error_hint") if isinstance(result.get("error_hint"), dict) else {}
    haystack = " ".join((status, reason))
    kind = str(existing.get("kind") or details.get("error_kind") or "")
    if kind not in {"validation", "state_conflict", "permission_security",
                    "transient_external", "unknown_internal"}:
        if any(word in haystack for word in ("permission", "security", "forbidden", "auth")):
            kind = "permission_security"
        elif any(word in haystack for word in ("timeout", "connection", "dns", "tls", "rate", "upstream", "temporar")):
            kind = "transient_external"
        elif any(word in haystack for word in ("not_active", "stale", "conflict", "already", "closed")):
            kind = "state_conflict"
        elif any(word in haystack for word in ("invalid", "missing", "required", "unknown", "not_read", "not_found", "unsupported", "schema")):
            kind = "validation"
        else:
            kind = "unknown_internal"
    defaults = {
        "validation": ("after_correction", "按 expected 修正参数后再提交。"),
        "state_conflict": ("after_correction", "刷新当前状态，只按 current/expected 坐标重新提交。"),
        "permission_security": ("after_authorization", "停止当前动作，取得明确授权或降低动作范围。"),
        "transient_external": ("later", "保留 reason，稍后重试；不要声称已成功。"),
        "unknown_internal": ("never_without_new_evidence", "不要原样重试；保留 reason，停止当前动作并报告。"),
    }
    retry, next_action = defaults[kind]
    attempted = existing.get("attempted", details.get("attempted"))
    if attempted in (None, ""):
        attempted = {key: result[key] for key in ("guide_id", "item_id", "option_id", "field")
                     if result.get(key) not in (None, "")}
    hint = {
        "kind": kind,
        "retry": _safe_error_hint_value(existing.get("retry") or details.get("retry") or retry),
        "attempted": _safe_error_hint_value(attempted or {}),
        "current": _safe_error_hint_value(existing.get("current") or details.get("current") or details.get("active_guide") or {}),
        "expected": _safe_error_hint_value(existing.get("expected") or details.get("expected") or result.get("expected") or {}),
        "next_action": _safe_error_hint_value(existing.get("next_action") or details.get("next_action") or result.get("repair_hint") or next_action),
    }
    return hint


def _safe_error_hint_value(value):
    allowed_keys = {
        "guide_id", "item_id", "option_id", "option_ids", "field", "fields",
        "allowed", "source_ref", "missing_source_refs", "unknown_source_refs",
        "prior_source_refs", "legal_coordinates", "requirement_id",
        "tool_id",
    }
    if isinstance(value, dict):
        return {str(key): _safe_error_hint_value(child) for key, child in value.items()
                if str(key) in allowed_keys}
    if isinstance(value, list):
        return [_safe_error_hint_value(child) for child in value[:20]]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value[:500] if isinstance(value, str) else value
    return str(value)[:500]
