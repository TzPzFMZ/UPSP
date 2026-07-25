"""Reaction-step memory_recall_complete protocol tool processor."""

import re
from datetime import datetime

from constants import TZ_SHANGHAI
from logic.memory_privacy import (
    can_see_memory,
    confirmed_subjects_from_state,
    privacy_subjects_for_memory,
)


TITLE_MARKER = "[召回补全内容]"


def _clean_text(value):
    return str(value or "").strip().strip("`")


def _receipt(status, request, reason="", evidence=None):
    return {
        "tool_id": "memory_recall_complete",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "status": status,
        "source": "memory_recall_completion_request",
        "mem_id": _clean_text(request.get("mem_id")) if isinstance(request, dict) else "",
        "title": _clean_text(request.get("title")) if isinstance(request, dict) else "",
        "reason": reason,
        "evidence": evidence or {},
    }


def _extract_current_overview(entry_text, index_line, meta=None):
    meta_overview = _clean_text((meta or {}).get("current_overview"))
    if meta_overview:
        return meta_overview[:128]
    if index_line:
        cells = [part.strip() for part in index_line.strip().strip("|").split("|")]
        if len(cells) >= 8 and cells[7] and cells[7].lower() != "null":
            return cells[7][:128]
        if len(cells) == 7 and cells[6] and cells[6].lower() != "null":
            return cells[6][:128]
    for pattern in (r"(?m)^现状概况：(.+)$", r"(?m)^注释：(.+)$"):
        match = re.search(pattern, entry_text or "")
        if match:
            value = match.group(1).strip()
            return "" if value.lower() == "null" else value[:128]
    value = ""
    return "" if value.lower() == "null" else value


def _index_line_for(mem_id, memory_store):
    for line in memory_store.read_index() or []:
        if str(line).lstrip().startswith(f"| {mem_id} |"):
            return line
    return ""


def _container_notes(container_store, container_id):
    if container_store is None:
        return []
    for method_name in ("read_recent_notes", "get_recent_notes", "read_notes"):
        method = getattr(container_store, method_name, None)
        if method is None:
            continue
        try:
            notes = method(container_id, limit=3)
        except TypeError:
            notes = method(container_id)
        if notes is None:
            return []
        if isinstance(notes, str):
            return [notes] if notes.strip() else []
        return [str(note) for note in notes if str(note).strip()]
    return []


def build_recall_completion_evidence(mem_id, data_modules):
    """Build a non-recursive evidence package for one memory recall completion."""
    memory_store = data_modules["memory_store"]
    container_store = data_modules.get("container_store")
    meta = memory_store.get_meta(mem_id)
    entry_text = memory_store.read_entry(mem_id)
    index_line = _index_line_for(mem_id, memory_store)
    linked = list((meta or {}).get("linked_containers") or [])
    related_notes = []
    for container_id in linked:
        related_notes.extend(_container_notes(container_store, container_id))
    return {
        "mem_id": mem_id,
        "meta": dict(meta or {}),
        "original_body": entry_text,
        "index_line": index_line,
        "current_overview": _extract_current_overview(entry_text, index_line, meta),
        "linked_containers": linked,
        "related_container_notes": related_notes,
    }


def _completed_title(meta):
    title = _clean_text((meta or {}).get("title"))
    if not title:
        title = _clean_text((meta or {}).get("id"))
    if TITLE_MARKER not in title:
        title = f"{title}{TITLE_MARKER}"
    return title


def _has_minimum_evidence(evidence):
    return bool(
        evidence.get("original_body")
        or evidence.get("index_line")
        or evidence.get("linked_containers")
        or evidence.get("related_container_notes")
    )


def apply_memory_recall_completion_requests(requests, data_modules, round_num=None, state=None):
    """Apply non-recursive recall completion rewrites and return protocol receipts."""
    receipts = []
    if not requests:
        return receipts

    memory_store = data_modules["memory_store"]
    relation_store = data_modules.get("relation_store")
    confirmed_subjects = confirmed_subjects_from_state(
        state, relation_store=relation_store)
    for request in requests:
        if not isinstance(request, dict):
            receipts.append(_receipt("error", {}, reason="invalid_request"))
            continue
        mem_id = _clean_text(request.get("mem_id"))
        if not mem_id:
            receipts.append(_receipt("error", request, reason="missing_mem_id"))
            continue
        completed_body = _clean_text(request.get("completed_body") or request.get("body"))
        if not completed_body:
            receipts.append(_receipt("error", request, reason="missing_completed_body"))
            continue
        try:
            meta_for_gate = memory_store.get_meta(mem_id)
        except Exception:
            receipts.append(_receipt("error", request, reason="memory_not_found"))
            continue
        owners = privacy_subjects_for_memory(memory_store, mem_id)
        if not can_see_memory(
                meta_for_gate, confirmed_subjects, relation_store,
                privacy_subjects=owners):
            receipts.append(_receipt(
                "private_memory_not_visible",
                request,
                reason="private_memory_not_visible",
            ))
            continue
        try:
            evidence = build_recall_completion_evidence(mem_id, data_modules)
        except Exception:
            receipts.append(_receipt("error", request, reason="memory_not_found"))
            continue
        if not _has_minimum_evidence(evidence):
            receipts.append(_receipt("error", request, reason="insufficient_evidence", evidence=evidence))
            continue

        meta = dict(evidence.get("meta") or {})
        title = _completed_title(meta)
        try:
            memory_store.update_entry_title_and_body(mem_id, title, completed_body)
            meta["title"] = title
            meta["recalled"] = True
            meta["last_recalled_at"] = datetime.now(TZ_SHANGHAI).isoformat()
            if round_num is not None:
                meta["last_recalled_round"] = round_num
            memory_store.set_meta(mem_id, meta)
            receipt = _receipt("applied", request, evidence=evidence)
            receipt["title"] = title
            receipts.append(receipt)
        except Exception as exc:
            receipts.append(_receipt("error", request, reason=str(exc), evidence=evidence))
    return receipts
