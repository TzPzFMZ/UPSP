"""container_focus 协议工具处理器。"""

from data.container_store import ContainerStore
from errors import ContainerNotFoundError


def apply_container_focus_declarations(declarations, modules=None, round_num=0):
    modules = modules or {}
    container_store = modules.get("container_store") or ContainerStore()
    workbench_store = modules.get("workbench_store")
    if workbench_store is None:
        from data.workbench import WorkbenchStore
        workbench_store = WorkbenchStore()

    receipts = []
    for declaration in declarations or []:
        receipts.append(_apply_one(
            declaration,
            container_store=container_store,
            workbench_store=workbench_store,
            round_num=round_num,
        ))
    return receipts


def _base_receipt(declaration):
    return {
        "tool_id": "container_focus",
        "tool_family": "protocol_tool",
        "tool_class": "focus_tool",
        "source": "provider_native_container_focus",
        "action": str(declaration.get("action") or "").lower(),
        "container_type": str(declaration.get("container_type") or "").upper(),
        "container_id": declaration.get("container_id") or "",
        "target_file": declaration.get("target_file") or "",
        "write_mode": declaration.get("write_mode") or "",
        "protocol_tool_receipt": True,
    }


def _reject(receipt, reason):
    receipt["status"] = "rejected"
    receipt["reason"] = reason
    return receipt


def _reject_missing_container(receipt, container_id):
    receipt["status"] = "rejected"
    receipt["reason"] = "container_not_found"
    receipt["message"] = (
        f"容器实例 {container_id or '未指定'} 不存在；不要继续 open 这个 ID；"
        "如需新建请调用 memory_container_create。"
    )
    receipt["recovery_tool"] = "memory_container_create"
    return receipt


def _prefix_from_declaration(declaration, container_store):
    prefix = str(declaration.get("container_type") or "").strip().upper().rstrip("-")
    if prefix:
        return prefix
    container_id = declaration.get("container_id") or ""
    return container_store.resolve_container_type(container_id)


def _apply_one(declaration, container_store, workbench_store, round_num):
    receipt = _base_receipt(declaration)
    action = receipt["action"]
    prefix = _prefix_from_declaration(declaration, container_store)
    if prefix:
        receipt["container_type"] = prefix

    if action in {"create", "write"}:
        return _reject(receipt, "retired_container_focus_action")

    if action not in {"open", "close", "restore"}:
        return _reject(receipt, "unsupported_action")

    if action == "open":
        return _apply_open(declaration, receipt, container_store, workbench_store)
    if action == "close":
        return _apply_close(declaration, receipt, container_store, workbench_store)
    return _apply_restore(receipt, container_store, workbench_store)


def _apply_open(declaration, receipt, container_store, workbench_store):
    container_id = declaration.get("container_id") or ""
    if not container_id:
        return _reject(receipt, "missing_container_id")
    if not container_store.container_exists(container_id):
        return _reject_missing_container(receipt, container_id)
    focus_result = _mount_focus(container_id, container_store, workbench_store)
    receipt.update({
        "status": "applied",
        "container_id": container_id,
        "container_type": container_store.resolve_container_type(container_id),
        "previous_focus": focus_result["previous_focus"],
    })
    if focus_result["stale_previous_focus"]:
        receipt["stale_previous_focus"] = focus_result["stale_previous_focus"]
    return receipt


def _apply_close(declaration, receipt, container_store, workbench_store):
    container_id = declaration.get("container_id") or workbench_store.get("base.focus")
    if not container_id:
        return _reject(receipt, "missing_focus")
    workbench_store.unmount_focus(container_id)
    stale_focus = _set_container_focus_or_stale(
        container_store, container_id, False)
    _clear_stale_old_focus(workbench_store, stale_focus)
    receipt.update({
        "status": "applied",
        "container_id": container_id,
        "container_type": container_store.resolve_container_type(container_id),
    })
    if stale_focus:
        receipt["stale_focus_cleared"] = stale_focus
    return receipt


def _apply_restore(receipt, container_store, workbench_store):
    current = workbench_store.get("base.focus")
    restored = workbench_store.get("base.old_focus")
    if not restored:
        return _reject(receipt, "missing_old_focus")
    if not container_store.container_exists(restored):
        _clear_stale_old_focus(workbench_store, restored)
        return _reject(receipt, "container_not_found")
    if current:
        stale_current = _set_container_focus_or_stale(
            container_store, current, False)
    else:
        stale_current = ""
    workbench_store.restore_focus()
    container_store.set_container_focus(restored, True)
    receipt.update({
        "status": "applied",
        "container_id": restored,
        "container_type": container_store.resolve_container_type(restored),
    })
    if stale_current:
        receipt["stale_previous_focus"] = stale_current
    return receipt


def _mount_focus(container_id, container_store, workbench_store):
    current = workbench_store.get("base.focus")
    stale_previous_focus = ""
    if current and current != container_id:
        stale_previous_focus = _set_container_focus_or_stale(
            container_store, current, False)
    workbench_store.mount_focus(container_id)
    _clear_stale_old_focus(workbench_store, stale_previous_focus)
    container_store.set_container_focus(container_id, True)
    return {
        "previous_focus": current or "",
        "stale_previous_focus": stale_previous_focus,
    }


def _set_container_focus_or_stale(container_store, container_id, focus):
    try:
        container_store.set_container_focus(container_id, focus)
    except ContainerNotFoundError:
        return container_id
    return ""


def _clear_stale_old_focus(workbench_store, stale_container_id):
    if not stale_container_id or not hasattr(workbench_store, "set"):
        return
    if workbench_store.get("base.old_focus") == stale_container_id:
        workbench_store.set("base.old_focus", None)
