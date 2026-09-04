"""mount_cancel protocol tool processor."""


VALID_MOUNT_AREAS = {"resident_list", "instant_list"}
VALID_ITEM_TYPES = {
    "auto",
    "memory",
    "container",
    "relation",
}


def _clean(value):
    return str(value or "").strip().strip("`")


def _receipt(status, request, *, reason="", removed=False, item_type=""):
    request = request if isinstance(request, dict) else {}
    return {
        "tool_id": "mount_cancel",
        "status": status,
        "source": "mount_cancel",
        "mount_area": _clean(request.get("mount_area")),
        "item_type": item_type or _clean(request.get("item_type")) or "auto",
        "item_id": _clean(request.get("item_id")),
        "removed": bool(removed),
        "reason": reason or _clean(request.get("reason")),
        "protocol_tool_receipt": True,
    }


def _split_ids(value):
    if isinstance(value, (list, tuple, set)):
        raw = value
    else:
        raw = str(value or "").split(",")
    return [str(item).strip() for item in raw if str(item).strip()]


def _join_ids(ids):
    return ", ".join(ids)


def _mount_type_matches(mount_type, item_type):
    if item_type == "auto":
        return True
    return mount_type == item_type


def _remove_mount_item(
        existing, item_id, item_type="auto", target_file=""):
    remaining = []
    removed = False
    clean_target = _clean(target_file)
    for item in existing or []:
        if not isinstance(item, dict):
            remaining.append(item)
            continue
        mount_type = _clean(item.get("type"))
        if not _mount_type_matches(mount_type, item_type):
            remaining.append(item)
            continue
        if (
            mount_type == "container"
            and clean_target
            and _clean(item.get("target_file")) != clean_target
        ):
            remaining.append(item)
            continue
        ids = _split_ids(item.get("ids"))
        if item_id not in ids:
            remaining.append(item)
            continue
        removed = True
        kept = [value for value in ids if value != item_id]
        if not kept:
            continue
        updated = dict(item)
        updated["ids"] = _join_ids(kept)
        remaining.append(updated)
    return remaining, removed


def _infer_item_type(item_id, mount_ids, resident_store=None):
    for item in mount_ids or []:
        if not isinstance(item, dict):
            continue
        if item_id in _split_ids(item.get("ids")):
            mount_type = _clean(item.get("type"))
            if mount_type in VALID_ITEM_TYPES:
                return mount_type
    if item_id.startswith("MEM-"):
        return "memory"
    if item_id.startswith("REL-"):
        return "relation"
    if resident_store is not None:
        for item in resident_store.load().get("items", []):
            if item.get("item_id") == item_id:
                return item.get("item_type") or "auto"
    return "auto"


def _cancel_list_item(request, modules, mount_ids):
    item_id = _clean(request.get("item_id"))
    if not item_id:
        return (
            _receipt("rejected", request, reason="missing_item_id"),
            list(mount_ids or []),
        )
    item_type = _clean(request.get("item_type")) or "auto"
    resident_store = modules.get("resident_store")
    if request.get("mount_area") == "resident_list" and resident_store is not None:
        try:
            if item_type == "auto":
                item_type = _infer_item_type(
                    item_id, mount_ids, resident_store)
        except Exception:
            return (
                _receipt(
                    "error",
                    request,
                    reason="resident_list_read_failed",
                    item_type=item_type,
                ),
                list(mount_ids or []),
            )
    elif item_type == "auto":
        item_type = _infer_item_type(item_id, mount_ids)

    if request.get("mount_area") == "resident_list":
        if item_type not in {"memory", "container", "relation"}:
            return (
                _receipt("not_found", request, reason="resident_item_not_found"),
                list(mount_ids or []),
            )
        if resident_store is None:
            return (
                _receipt("error", request, reason="resident_context_unavailable"),
                list(mount_ids or []),
            )
        try:
            result = resident_store.remove_matching(
                item_type=item_type,
                item_id=item_id,
                target_file=_clean(request.get("target_file")),
            )
        except Exception as exc:
            return (
                _receipt(
                    "error",
                    request,
                    reason=str(exc) or "resident_list_write_failed",
                    item_type=item_type,
                ),
                list(mount_ids or []),
            )
        remaining, local_removed = _remove_mount_item(
            mount_ids,
            item_id,
            item_type,
            _clean(request.get("target_file")),
        )
        removed = bool(result.get("removed") or local_removed)
        receipt = _receipt(
            "applied" if removed else "not_found",
            request,
            reason=(
                _clean(request.get("reason"))
                or ("resident_removed" if removed else "resident_item_not_found")
            ),
            removed=removed,
            item_type=item_type,
        )
        receipt["resident_revision"] = result.get("revision")
        receipt["removed_items"] = result.get("removed_items", [])
        return receipt, remaining

    remaining, removed = _remove_mount_item(
        mount_ids,
        item_id,
        item_type,
        _clean(request.get("target_file")),
    )
    if removed:
        return (
            _receipt(
                "applied",
                request,
                reason=_clean(request.get("reason")) or "mount_removed",
                removed=True,
                item_type=item_type,
            ),
            remaining,
        )
    return (
        _receipt("not_found", request, reason="mount_not_found", item_type=item_type),
        remaining,
    )


def apply_mount_cancel_requests(requests, modules=None, mount_ids=None):
    modules = modules or {}
    updated_mount_ids = list(mount_ids or [])
    receipts = []
    for request in requests or []:
        if not isinstance(request, dict):
            receipts.append(_receipt("rejected", {}, reason="invalid_request"))
            continue
        mount_area = _clean(request.get("mount_area"))
        item_type = _clean(request.get("item_type")) or "auto"
        if mount_area not in VALID_MOUNT_AREAS:
            receipts.append(_receipt("rejected", request, reason="invalid_mount_area"))
            continue
        if item_type not in VALID_ITEM_TYPES:
            receipts.append(_receipt("rejected", request, reason="invalid_item_type"))
            continue
        receipt, updated_mount_ids = _cancel_list_item(
            request,
            modules,
            updated_mount_ids,
        )
        receipts.append(receipt)
    return receipts, updated_mount_ids
