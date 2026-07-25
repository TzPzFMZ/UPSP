"""Reaction-step memory_content_read protocol tool processor."""

from utils.content_ranges import range_kwargs_from_request
from logic.memory_privacy import (
    can_see_memory,
    confirmed_subjects_from_state,
    privacy_subjects_for_memory,
)


def _clean_text(value):
    return str(value or "").strip().strip("`")


def _receipt(status, request, reason="", body="", meta=None, memory_layer="",
             read_mode="", range_requested=None, range_applied=None,
             total_lines=0, total_chars=0):
    return {
        "tool_id": "memory_content_read",
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "status": status,
        "source": "protocol_tool_request",
        "mem_id": _clean_text(request.get("mem_id")) if isinstance(request, dict) else "",
        "mount_mode": _clean_text(request.get("mount_mode")) if isinstance(request, dict) else "",
        "body": body,
        "meta": meta or {},
        "memory_layer": memory_layer,
        "read_mode": read_mode,
        "range_requested": range_requested,
        "range_applied": range_applied,
        "total_lines": total_lines,
        "total_chars": total_chars,
        "reason": reason,
    }


def apply_memory_content_read_requests(requests, state, data_modules):
    """Return body receipts plus round-local CONTENT mount changes."""
    receipts = []
    mounts = []
    unmounts = []
    if not requests:
        return receipts, mounts, unmounts
    memory_store = data_modules["memory_store"]
    relation_store = data_modules.get("relation_store")
    confirmed = confirmed_subjects_from_state(
        state, relation_store=relation_store)

    for request in requests:
        if not isinstance(request, dict):
            receipts.append(_receipt("error", {}, reason="invalid_request"))
            continue
        mem_id = _clean_text(request.get("mem_id"))
        if not mem_id:
            receipts.append(_receipt("error", request, reason="missing_mem_id"))
            continue
        mount_mode = _clean_text(request.get("mount_mode")) or "temporary"
        if mount_mode not in {"temporary", "resident", "none"}:
            receipts.append(_receipt("error", request, reason="invalid_mount_mode"))
            continue
        if mount_mode == "none":
            receipts.append(_receipt("accepted", request, reason="unmounted"))
            unmounts.append(mem_id)
            continue
        try:
            read_meta = getattr(memory_store, "read_meta_by_id", None)
            meta = read_meta(mem_id) if callable(read_meta) else memory_store.get_meta(mem_id)
        except Exception:
            receipts.append(_receipt("memory_not_found", request, reason="memory_not_found"))
            continue
        layer = _clean_text(meta.get("_memory_layer")) if isinstance(meta, dict) else ""
        owners = privacy_subjects_for_memory(memory_store, mem_id)
        if not can_see_memory(
                meta, confirmed, relation_store,
                privacy_subjects=owners):
            receipts.append(_receipt(
                "private_memory_not_visible",
                request,
                reason="private_memory_not_visible",
                memory_layer=layer,
            ))
            continue
        try:
            result = memory_store.read_body_by_id(mem_id, **range_kwargs_from_request(request))
        except ValueError as exc:
            receipts.append(_receipt("rejected", request, reason=str(exc)))
            continue
        except Exception:
            receipts.append(_receipt("memory_not_found", request, reason="memory_not_found"))
            continue
        receipt_meta = dict(result.get("meta") or {})
        result_layer = _clean_text(result.get("memory_layer")) or _clean_text(
            receipt_meta.pop("_memory_layer", "")
        ) or layer
        receipts.append(_receipt(
            "accepted",
            request,
            body=result.get("body", ""),
            meta=receipt_meta,
            memory_layer=result_layer,
            read_mode=result.get("read_mode") or "full",
            range_requested=result.get("range_requested"),
            range_applied=result.get("range_applied"),
            total_lines=result.get("total_lines", 0),
            total_chars=result.get("total_chars", 0),
        ))
        mount = {
            "type": "memory",
            "ids": mem_id,
            "mode": mount_mode,
            "source": "memory_content_read",
            "content": result.get("body", ""),
            "read_mode": result.get("read_mode") or "full",
            "range_requested": result.get("range_requested"),
            "range_applied": result.get("range_applied"),
            "total_lines": result.get("total_lines", 0),
            "total_chars": result.get("total_chars", 0),
        }
        mounts.append({
            key: value for key, value in mount.items() if value is not None
        })
    return receipts, mounts, unmounts
