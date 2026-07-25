"""mount_cancel protocol tool processor."""


VALID_MOUNT_AREAS = {"focus", "resident_list", "instant_list"}
VALID_ITEM_TYPES = {
    "auto",
    "memory",
    "container",
    "relation",
    "relation_summary",
}


def _clean(value):
    return str(value or "").strip().strip("`")


def _receipt(status, request, *, reason="", removed=False, item_type=""):
    request = request if isinstance(request, dict) else {}
    return {
        "tool_id": "mount_cancel",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
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


def _remove_mount_item(existing, item_id, item_type="auto"):
    remaining = []
    removed = False
    for item in existing or []:
        if not isinstance(item, dict):
            remaining.append(item)
            continue
        mount_type = _clean(item.get("type"))
        if not _mount_type_matches(mount_type, item_type):
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


def _infer_item_type(item_id, mount_ids):
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
    return "auto"


def _cancel_focus(request, modules, mount_ids):
    workbench_store = modules.get("workbench_store")
    if workbench_store is None:
        from data.workbench import WorkbenchStore
        workbench_store = WorkbenchStore()
    container_store = modules.get("container_store")
    if container_store is None:
        from data.container_store import ContainerStore
        container_store = ContainerStore()

    target_id = _clean(request.get("item_id"))
    current_focus = _clean(workbench_store.get("base.focus"))
    if not target_id:
        target_id = current_focus
    if not target_id:
        return _receipt("not_found", request, reason="focus_not_found"), mount_ids
    if current_focus and current_focus != target_id:
        return _receipt("not_found", request, reason="focus_not_matched"), mount_ids

    workbench_store.unmount_focus(target_id)
    try:
        container_store.set_container_focus(target_id, False)
    except Exception:
        return _receipt(
            "applied",
            request,
            reason="focus_unmounted_container_flag_failed",
            removed=True,
            item_type="container",
        ), mount_ids
    receipt = _receipt(
        "applied",
        request,
        reason=_clean(request.get("reason")) or "focus_unmounted",
        removed=True,
        item_type="container",
    )
    try:
        receipt["container_type"] = container_store.resolve_container_type(target_id)
    except Exception:
        receipt["container_type"] = ""
    receipt["item_id"] = target_id
    return receipt, mount_ids


def _cancel_relation_resident(request, modules, *, summary=False):
    relation_store = modules.get("relation_store")
    if relation_store is None:
        from data.relation_store import RelationStore
        relation_store = RelationStore()
    item_id = _clean(request.get("item_id"))
    if not item_id:
        return _receipt("rejected", request, reason="missing_item_id")
    try:
        if summary:
            relation_store.set_summary_resident(item_id, False)
        else:
            relation_store.set_body_resident(item_id, False)
    except Exception:
        return _receipt("error", request, reason="relation_resident_clear_failed")
    item_type = "relation_summary" if summary else "relation"
    return _receipt(
        "applied",
        request,
        reason=_clean(request.get("reason")) or "resident_cleared",
        removed=True,
        item_type=item_type,
    )


def _cancel_list_item(request, modules, mount_ids):
    item_id = _clean(request.get("item_id"))
    if not item_id:
        return (
            _receipt("rejected", request, reason="missing_item_id"),
            list(mount_ids or []),
        )
    item_type = _clean(request.get("item_type")) or "auto"
    if item_type == "auto":
        item_type = _infer_item_type(item_id, mount_ids)

    if request.get("mount_area") == "resident_list":
        if item_type == "relation":
            return _cancel_relation_resident(request, modules), list(mount_ids or [])
        if item_type == "relation_summary":
            return (
                _cancel_relation_resident(request, modules, summary=True),
                list(mount_ids or []),
            )
        if item_type == "container":
            return (
                _receipt("not_found", request, reason="container_resident_not_implemented"),
                list(mount_ids or []),
            )

    remaining, removed = _remove_mount_item(mount_ids, item_id, item_type)
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
        if mount_area == "focus":
            receipt, updated_mount_ids = _cancel_focus(
                request,
                modules,
                updated_mount_ids,
            )
        else:
            receipt, updated_mount_ids = _cancel_list_item(
                request,
                modules,
                updated_mount_ids,
            )
        receipts.append(receipt)
    return receipts, updated_mount_ids
