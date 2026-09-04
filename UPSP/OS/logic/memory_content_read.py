"""Reaction-step memory_content_read protocol tool processor."""

from data.memory_store import project_memory_body
from constants import RESIDENT_LIST_CHAR_LIMIT
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
             total_lines=0, total_chars=0, source_memory_layer="",
             stm_present=False, heat_boost_applied=False,
             heat_boost_deduplicated=False,
             ltm_decay_reset_applied=False,
             ltm_decay_countdown_before=None,
             ltm_decay_countdown_after=None):
    return {
        "tool_id": "memory_content_read",
        "status": status,
        "source": "protocol_tool_request",
        "mem_id": _clean_text(request.get("mem_id")) if isinstance(request, dict) else "",
        "mount_mode": _clean_text(request.get("mount_mode")) if isinstance(request, dict) else "",
        "body": body,
        "meta": meta or {},
        "memory_layer": memory_layer,
        "source_memory_layer": source_memory_layer or memory_layer,
        "stm_present": bool(stm_present),
        "heat_boost_applied": bool(heat_boost_applied),
        "heat_boost_deduplicated": bool(heat_boost_deduplicated),
        "ltm_decay_reset_applied": bool(ltm_decay_reset_applied),
        "ltm_decay_countdown_before": ltm_decay_countdown_before,
        "ltm_decay_countdown_after": ltm_decay_countdown_after,
        "read_mode": read_mode,
        "range_requested": range_requested,
        "range_applied": range_applied,
        "total_lines": total_lines,
        "total_chars": total_chars,
        "reason": reason,
    }


def apply_memory_content_read_requests(
        requests, state, data_modules, *, round_num=None,
        memory_heat_boosted_ids=None,
        memory_reconsolidation_tracker=None):
    """Return body receipts plus round-local CONTENT mount changes."""
    receipts = []
    mounts = []
    unmounts = []
    if not requests:
        return receipts, mounts, unmounts
    memory_store = data_modules["memory_store"]
    memory_recall = data_modules.get("memory_recall")
    assembler = data_modules.get("assembler")
    resident_store = data_modules.get("resident_store")
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
            try:
                if resident_store is None:
                    raise RuntimeError("resident_context_unavailable")
                removed = resident_store.remove_matching(
                    item_type="memory",
                    item_id=mem_id,
                )
            except Exception as exc:
                receipts.append(_receipt(
                    "error", request,
                    reason=str(exc) or "resident_list_write_failed",
                ))
                continue
            receipt = _receipt("accepted", request, reason="unmounted")
            receipt["resident_persisted"] = False
            receipt["resident_revision"] = removed.get("revision")
            receipts.append(receipt)
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
        if memory_recall is None:
            receipts.append(_receipt(
                "error",
                request,
                reason="memory_recall_processor_unavailable",
                memory_layer=layer,
                source_memory_layer=layer,
            ))
            continue
        try:
            # Resolve and range the canonical body before recall mutates any
            # lifecycle state. Invalid ranges and read failures stay side-effect
            # free; a successful partial read still rehydrates the full body.
            result = memory_store.read_body_by_id(
                mem_id, **range_kwargs_from_request(request))
            full_result = memory_store.read_body_by_id(mem_id)
        except ValueError as exc:
            receipts.append(_receipt("rejected", request, reason=str(exc)))
            continue
        except Exception:
            receipts.append(_receipt(
                "memory_not_found", request, reason="memory_not_found"))
            continue
        lifecycle = {
            "source_memory_layer": layer,
            "stm_present": layer == "STM",
            "heat_boost_applied": False,
            "heat_boost_deduplicated": False,
        }
        resident_result = {}
        resident_snapshot = None
        resident_item = {"item_type": "memory", "item_id": mem_id}
        projected_full_body = project_memory_body(
            full_result.get("body", ""), full_result.get("meta") or {}
        )
        if mount_mode == "resident":
            try:
                if assembler is None or resident_store is None:
                    raise RuntimeError("resident_context_unavailable")
                resident_snapshot = resident_store.snapshot_bytes()
                resident_preflight = assembler.preflight_resident_add(
                    resident_item,
                    content_overrides={
                        ("memory", mem_id, ""): projected_full_body,
                    },
                )
            except Exception as exc:
                receipts.append(_receipt(
                    "rejected", request,
                    reason=str(exc) or "resident_list_preflight_failed",
                    memory_layer=layer,
                    source_memory_layer=layer,
                ))
                continue

        def _commit_resident():
            if mount_mode != "resident":
                return
            current_full = memory_store.read_body_by_id(mem_id)
            current_projected = project_memory_body(
                current_full.get("body", ""), current_full.get("meta") or {}
            )
            current_chars = assembler.resident_projection_chars(
                document=resident_preflight["document"],
                content_overrides={
                    ("memory", mem_id, ""): current_projected,
                },
            )
            if current_chars > RESIDENT_LIST_CHAR_LIMIT:
                raise ValueError(
                    "resident_list_char_limit_exceeded:"
                    f"max={RESIDENT_LIST_CHAR_LIMIT};actual={current_chars}"
                )
            resident_result.update(resident_store.add(
                resident_item,
                candidate=resident_preflight["document"],
                expected_revision=resident_preflight["expected_revision"],
            ))
            resident_result["resident_chars"] = current_chars
            resident_result["projected_body"] = current_projected
            resident_result["total_lines"] = current_full.get("total_lines", 0)
            resident_result["total_chars"] = current_full.get("total_chars", 0)

        def _rollback_resident():
            if mount_mode == "resident":
                resident_store.restore_bytes(resident_snapshot)

        try:
            lifecycle = memory_recall.recall(
                mem_id,
                round_num=round_num,
                boosted_ids=memory_heat_boosted_ids,
                reconsolidation_tracker=memory_reconsolidation_tracker,
                transaction_commit=(
                    _commit_resident if mount_mode == "resident" else None
                ),
                transaction_rollback=(
                    _rollback_resident if mount_mode == "resident" else None
                ),
            )
        except Exception as exc:
            receipts.append(_receipt(
                "error",
                request,
                reason=str(exc) or type(exc).__name__,
                memory_layer=layer,
                source_memory_layer=layer,
            ))
            continue
        receipt_meta = dict(
            lifecycle.get("resolved_meta") or result.get("meta") or {})
        result_layer = _clean_text(result.get("memory_layer")) or _clean_text(
            receipt_meta.pop("_memory_layer", "")
        ) or layer
        receipt = _receipt(
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
            source_memory_layer=lifecycle.get("source_memory_layer") or result_layer,
            stm_present=lifecycle.get("stm_present", False),
            heat_boost_applied=lifecycle.get("heat_boost_applied", False),
            heat_boost_deduplicated=lifecycle.get(
                "heat_boost_deduplicated", False),
            ltm_decay_reset_applied=lifecycle.get(
                "ltm_decay_reset_applied", False),
            ltm_decay_countdown_before=lifecycle.get(
                "ltm_decay_countdown_before"),
            ltm_decay_countdown_after=lifecycle.get(
                "ltm_decay_countdown_after"),
        )
        receipt["resident_persisted"] = mount_mode == "resident"
        receipt["resident_revision"] = resident_result.get("revision")
        receipt["resident_chars"] = resident_result.get("resident_chars", 0)
        receipts.append(receipt)
        current_projected = str(resident_result.get("projected_body") or "")
        mount = {
            "type": "memory",
            "ids": mem_id,
            "mode": mount_mode,
            "source": "memory_content_read",
            "content_projected": True if mount_mode == "resident" else None,
            "content": (
                current_projected
                if mount_mode == "resident" else result.get("body", "")
            ),
            "read_mode": (
                "full" if mount_mode == "resident"
                else result.get("read_mode") or "full"
            ),
            "range_requested": (
                None if mount_mode == "resident"
                else result.get("range_requested")
            ),
            "range_applied": (
                None if mount_mode == "resident"
                else result.get("range_applied")
            ),
            "total_lines": (
                resident_result.get("total_lines", 0)
                if mount_mode == "resident"
                else result.get("total_lines", 0)
            ),
            "total_chars": (
                resident_result.get("total_chars", 0)
                if mount_mode == "resident"
                else result.get("total_chars", 0)
            ),
        }
        mounts.append({
            key: value for key, value in mount.items() if value is not None
        })
    return receipts, mounts, unmounts
