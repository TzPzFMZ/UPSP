"""引用式容器挂接创建/挂接写入处理器。"""

from data.container_store import (
    CONTAINER_MUTATION_LOCK,
    ContainerStore,
    CONTAINER_TARGET_FILES,
    SUPPORTED_RESIDENT_CONTAINER_TYPES,
)
from data.memory_store import project_memory_body
from logic.memory_privacy import memory_visible_to_state


def _clean_text(value):
    return str(value or "").strip().strip("`")


def _normalize_mem_id(value):
    return _clean_text(value)


def _resolve_mem_id(value, pending_memory_ids):
    raw_mem_id = _normalize_mem_id(value)
    if not raw_mem_id.startswith("PENDING"):
        return raw_mem_id, raw_mem_id
    resolved = _normalize_mem_id((pending_memory_ids or {}).get(raw_mem_id))
    return raw_mem_id, resolved or raw_mem_id


def _base_receipt(tool_id, source, declaration):
    return {
        "tool_id": tool_id,
        "source": source,
        "mem_id": _clean_text(declaration.get("mem_id")) if isinstance(declaration, dict) else "",
        "container_id": _clean_text(declaration.get("container_id")) if isinstance(declaration, dict) else "",
        "container_type": _clean_text(declaration.get("container_type")).upper() if isinstance(declaration, dict) else "",
        "skill_category": _clean_text(declaration.get("skill_category")).lower() if isinstance(declaration, dict) else "",
        "skill_name": _clean_text(declaration.get("skill_name")).lower() if isinstance(declaration, dict) else "",
        "target_file": _clean_text(declaration.get("target_file")) if isinstance(declaration, dict) else "",
        "title": _clean_text(declaration.get("title")) if isinstance(declaration, dict) else "",
        "reason": _clean_text(declaration.get("reason")) if isinstance(declaration, dict) else "",
        "protocol_tool_receipt": True,
    }


def _reject(receipt, reason):
    receipt["status"] = "rejected"
    receipt["reason"] = reason
    return receipt


def _reject_invalid_target_file(receipt, container_type):
    receipt["allowed_target_files"] = sorted(
        CONTAINER_TARGET_FILES.get(str(container_type or "").upper(), frozenset())
    )
    return _reject(receipt, "invalid_target_file")


def _require_real_mem_id(receipt, mem_id):
    if not mem_id:
        return _reject(receipt, "missing_mem_id")
    if mem_id.startswith("PENDING"):
        return _reject(receipt, "pending_mem_id_not_allowed")
    if not mem_id.startswith("MEM-"):
        return _reject(receipt, "invalid_mem_id")
    return None


def _overview_for_container(current_overview, container_id):
    overview = _clean_text(current_overview).replace("{container_id}", container_id)
    if not overview:
        return ""
    if container_id not in overview:
        overview = f"{container_id}：{overview}"
    return overview


def _preview_container_id(
        container_store, container_type, target_file,
        skill_category=None, skill_name=None):
    try:
        if container_type in {"DC", "EC"}:
            return container_store._next_numeric_container_id(container_type)
        if container_type == "PRJ":
            return container_store._next_project_id()
        if container_type == "FUT":
            target = container_store._normalize_target_file(container_type, target_file)
            category = container_store._future_category_from_target(target)
            return container_store._next_future_id(category)
        if container_type == "SKL":
            return container_store._skill_container_id(skill_category, skill_name)
    except Exception:
        return "{container_id}"
    return "{container_id}"


def _validate_common(receipt, declaration, *, require_container_id=False):
    mem_id = _normalize_mem_id(declaration.get("mem_id"))
    invalid = _require_real_mem_id(receipt, mem_id)
    if invalid:
        return invalid
    if require_container_id and not _clean_text(declaration.get("container_id")):
        return _reject(receipt, "missing_container_id")
    if not _clean_text(declaration.get("title")):
        return _reject(receipt, "missing_title")
    if not _clean_text(declaration.get("target_file")):
        return _reject(receipt, "missing_target_file")
    if not _clean_text(declaration.get("container_body")):
        return _reject(receipt, "missing_container_body")
    if not _clean_text(declaration.get("current_overview")):
        return _reject(receipt, "missing_current_overview")
    if not _clean_text(declaration.get("reason")):
        return _reject(receipt, "missing_reason")
    return None


def _modules(modules):
    modules = modules or {}
    container_store = modules.get("container_store") or ContainerStore()
    memory_store = modules.get("memory_store")
    if memory_store is None:
        raise KeyError("memory_store")
    assembler = modules.get("assembler")
    if assembler is None:
        raise KeyError("assembler")
    resident_store = modules.get("resident_store")
    if resident_store is None:
        raise KeyError("resident_store")
    return memory_store, container_store, assembler, resident_store


def _apply_memory_link(memory_store, mem_id, container_id, overview):
    return memory_store.update_linked_containers(
        mem_id,
        "add",
        [container_id],
        current_overview=overview,
    )


def _append_container_content_with_ledger(
        container_store, container_id, target_file, title, content, *,
        mem_id, round_num):
    try:
        return container_store.append_container_content(
            container_id,
            target_file,
            title,
            content,
            mem_id=mem_id,
            round_num=round_num,
            ledger_status="applied",
        )
    except TypeError:
        return container_store.append_container_content(
            container_id,
            target_file,
            title,
            content,
        )


def _fault(modules, stage):
    hook = (modules or {}).get("fault_hook")
    if callable(hook):
        hook(stage)


def _restore_transaction(
        *, container_store, container_snapshot, memory_store,
        ltm_snapshot, stm_snapshot, resident_store, resident_snapshot):
    errors = []
    for label, restore in (
        ("resident", lambda: resident_store.restore_bytes(resident_snapshot)),
        ("stm", lambda: memory_store.restore_stm_files(stm_snapshot)),
        ("ltm", lambda: memory_store.restore_ltm_files(ltm_snapshot)),
        ("container", lambda: container_store.restore_mutation_files(
            container_snapshot)),
    ):
        try:
            restore()
        except Exception as exc:
            errors.append(f"{label}:{type(exc).__name__}")
    if errors:
        raise RuntimeError("container_transaction_restore_failed:" + ",".join(errors))


def _run_container_transaction(
        *, container_store, memory_store, resident_store,
        rollback_label, operation):
    """Run one container mutation against the three owned stores."""
    with CONTAINER_MUTATION_LOCK:
        snapshots = {
            "container": container_store.snapshot_mutation_files(),
            "ltm": memory_store.snapshot_ltm_files(),
            "stm": memory_store.snapshot_stm_files(),
            "resident": resident_store.snapshot_bytes(),
        }
        try:
            return operation()
        except Exception as exc:
            try:
                _restore_transaction(
                    container_store=container_store,
                    container_snapshot=snapshots["container"],
                    memory_store=memory_store,
                    ltm_snapshot=snapshots["ltm"],
                    stm_snapshot=snapshots["stm"],
                    resident_store=resident_store,
                    resident_snapshot=snapshots["resident"],
                )
            except Exception as rollback_exc:
                raise RuntimeError(
                    f"{rollback_label}_rollback_failed:"
                    f"{type(rollback_exc).__name__}"
                ) from exc
            raise


def apply_memory_container_create_declarations(
        declarations, modules=None, round_num=0, state=None):
    """Atomically create, seed, link and persist a resident container target."""
    receipts = []
    if not declarations:
        return receipts
    memory_store, container_store, assembler, resident_store = _modules(modules)

    for declaration in declarations or []:
        if not isinstance(declaration, dict):
            receipts.append(_reject(
                _base_receipt("memory_container_create", "memory_container_create_declaration", {}),
                "invalid_declaration",
            ))
            continue
        receipt = _base_receipt(
            "memory_container_create",
            "memory_container_create_declaration",
            declaration,
        )
        raw_mem_id, mem_id = _resolve_mem_id(
            declaration.get("mem_id"),
            (modules or {}).get("pending_memory_ids"),
        )
        resolved_declaration = dict(declaration, mem_id=mem_id)
        if raw_mem_id != mem_id:
            receipt["requested_mem_id"] = raw_mem_id
            receipt["mem_id"] = mem_id
        invalid = _validate_common(receipt, resolved_declaration)
        if invalid:
            receipts.append(invalid)
            continue
        container_type = _clean_text(declaration.get("container_type")).upper().rstrip("-")
        if container_type not in SUPPORTED_RESIDENT_CONTAINER_TYPES:
            receipts.append(_reject(receipt, "unsupported_container_type"))
            continue
        target_file = _clean_text(declaration.get("target_file"))
        if target_file not in CONTAINER_TARGET_FILES.get(container_type, frozenset()):
            receipts.append(_reject_invalid_target_file(receipt, container_type))
            continue
        skill_category = _clean_text(declaration.get("skill_category"))
        skill_name = _clean_text(declaration.get("skill_name"))
        if container_type == "SKL":
            if not skill_category:
                receipts.append(_reject(receipt, "missing_skill_category"))
                continue
            if not skill_name:
                receipts.append(_reject(receipt, "missing_skill_name"))
                continue
        elif skill_category or skill_name:
            receipts.append(_reject(receipt, "skill_fields_require_skl"))
            continue

        preview_container_id = _preview_container_id(
            container_store,
            container_type,
            target_file,
            skill_category,
            skill_name,
        )
        preview_overview = _overview_for_container(
            declaration.get("current_overview"),
            preview_container_id,
        )
        if not preview_overview:
            receipts.append(_reject(receipt, "missing_current_overview"))
            continue
        if len(preview_overview) > 128:
            receipts.append(_reject(receipt, "current_overview_too_long"))
            continue
        try:
            if not memory_visible_to_state(
                    memory_store, mem_id, state,
                    (modules or {}).get("relation_store")):
                receipts.append(_reject(receipt, "private_memory_not_visible"))
                continue
            def create_transaction():
                    create_kwargs = {
                        "target_file": target_file,
                        "anchor_refs": [mem_id],
                        "round_num": round_num,
                    }
                    if container_type == "SKL":
                        create_kwargs.update({
                            "skill_category": skill_category,
                            "skill_name": skill_name,
                        })
                    created = container_store.create_container(
                        container_type,
                        _clean_text(declaration.get("title")),
                        **create_kwargs,
                    )
                    container_id = created["container_id"]
                    _fault(modules, "after_container_create")
                    overview = _overview_for_container(
                        declaration.get("current_overview"),
                        container_id,
                    )
                    if not overview:
                        raise ValueError("missing_current_overview")
                    if len(overview) > 128:
                        raise ValueError("current_overview_too_long")
                    write_result = _append_container_content_with_ledger(
                        container_store,
                        container_id,
                        target_file,
                        _clean_text(declaration.get("title")),
                        _clean_text(declaration.get("container_body")),
                        mem_id=mem_id,
                        round_num=round_num,
                    )
                    _fault(modules, "after_container_body")
                    linked = _apply_memory_link(
                        memory_store, mem_id, container_id, overview)
                    _fault(modules, "after_memory_link")
                    full = container_store.read_container_content(
                        container_id, target_file=target_file)
                    memory = memory_store.read_body_by_id(mem_id)
                    resident_item = {
                        "item_type": "container",
                        "item_id": container_id,
                        "target_file": target_file,
                    }
                    preflight = assembler.preflight_resident_add(
                        resident_item,
                        content_overrides={
                            ("container", container_id, target_file):
                                full.get("content", ""),
                            ("memory", mem_id, ""):
                                project_memory_body(
                                    memory.get("body", ""),
                                    memory.get("meta") or {},
                                ),
                        },
                    )
                    resident = resident_store.add(
                        resident_item,
                        candidate=preflight["document"],
                        expected_revision=preflight["expected_revision"],
                    )
                    resident["resident_chars"] = preflight["chars"]
                    _fault(modules, "after_resident_write")
                    if not resident_store.contains(
                            item_type="container",
                            item_id=container_id,
                            target_file=target_file):
                        raise RuntimeError("resident_write_unverified")
                    if container_id not in list(
                            (linked or {}).get("linked_containers") or []):
                        raise RuntimeError("memory_link_unverified")
                    _fault(modules, "after_verify")
                    return created, container_id, overview, write_result, linked, resident

            (
                created, container_id, overview, write_result, linked, resident,
            ) = _run_container_transaction(
                container_store=container_store,
                memory_store=memory_store,
                resident_store=resident_store,
                rollback_label="container_create",
                operation=create_transaction,
            )
            receipt.update(created)
            receipt.update({
                "status": "applied",
                "mem_id": mem_id,
                "container_id": container_id,
                "container_type": container_type,
                "target_file": target_file,
                "write_path": write_result.get("path"),
                "write_chars": write_result.get("chars_written"),
                "container_body_written": True,
                "memory_link_applied": True,
                "linked_containers": list(
                    (linked or {}).get("linked_containers") or []),
                "current_overview": str(
                    (linked or {}).get("current_overview") or overview),
                "visibility_verified": False,
                "resident_persisted": True,
                "resident_revision": resident.get("revision"),
            })
            receipts.append(receipt)
        except Exception as exc:
            if str(exc) == "invalid_target_file":
                receipts.append(_reject_invalid_target_file(receipt, container_type))
            else:
                receipts.append(_reject(receipt, str(exc)))
    return receipts


def apply_memory_container_write_declarations(
        declarations, modules=None, round_num=0, state=None):
    """Write only container targets visible at the current Frame boundary."""
    receipts = []
    if not declarations:
        return receipts
    memory_store, container_store, assembler, resident_store = _modules(modules)
    visible_targets = set((modules or {}).get("visible_container_targets") or ())

    for declaration in declarations or []:
        if not isinstance(declaration, dict):
            receipts.append(_reject(
                _base_receipt("memory_container_write", "memory_container_write_declaration", {}),
                "invalid_declaration",
            ))
            continue
        receipt = _base_receipt(
            "memory_container_write",
            "memory_container_write_declaration",
            declaration,
        )
        raw_mem_id, mem_id = _resolve_mem_id(
            declaration.get("mem_id"),
            (modules or {}).get("pending_memory_ids"),
        )
        resolved_declaration = dict(declaration, mem_id=mem_id)
        if raw_mem_id != mem_id:
            receipt["requested_mem_id"] = raw_mem_id
            receipt["mem_id"] = mem_id
        invalid = _validate_common(
            receipt, resolved_declaration, require_container_id=True
        )
        if invalid:
            receipts.append(invalid)
            continue
        container_id = _clean_text(declaration.get("container_id"))
        if not container_store.container_exists(container_id):
            receipts.append(_reject(receipt, "container_not_found"))
            continue

        container_type = container_store.resolve_container_type(container_id)
        if container_type not in SUPPORTED_RESIDENT_CONTAINER_TYPES:
            receipts.append(_reject(receipt, "unsupported_container_type"))
            continue
        target_file = _clean_text(declaration.get("target_file"))
        if target_file not in CONTAINER_TARGET_FILES.get(container_type, frozenset()):
            receipts.append(_reject_invalid_target_file(receipt, container_type))
            continue
        if (container_id, target_file) not in visible_targets:
            receipts.append(_reject(
                receipt, "container_not_visible_at_frame_start"))
            continue
        overview = _overview_for_container(
            declaration.get("current_overview"),
            container_id,
        )
        if len(overview) > 128:
            receipts.append(_reject(receipt, "current_overview_too_long"))
            continue
        try:
            if not memory_visible_to_state(
                    memory_store, mem_id, state,
                    (modules or {}).get("relation_store")):
                receipts.append(_reject(receipt, "private_memory_not_visible"))
                continue
            def write_transaction():
                    write_result = _append_container_content_with_ledger(
                        container_store,
                        container_id,
                        target_file,
                        _clean_text(declaration.get("title")),
                        _clean_text(declaration.get("container_body")),
                        mem_id=mem_id,
                        round_num=round_num,
                    )
                    _fault(modules, "after_container_body")
                    linked = _apply_memory_link(
                        memory_store, mem_id, container_id, overview)
                    _fault(modules, "after_memory_link")
                    full = container_store.read_container_content(
                        container_id, target_file=target_file)
                    memory = memory_store.read_body_by_id(mem_id)
                    container_resident = resident_store.contains(
                        item_type="container",
                        item_id=container_id,
                        target_file=target_file,
                    )
                    projection = assembler.preflight_resident_source_updates(
                        {
                            ("container", container_id, target_file):
                                full.get("content", ""),
                            ("memory", mem_id, ""):
                                project_memory_body(
                                    memory.get("body", ""),
                                    memory.get("meta") or {},
                                ),
                        },
                        required_keys=(
                            {("container", container_id, target_file)}
                            if container_resident else set()
                        ),
                    )
                    _fault(modules, "after_resident_preflight")
                    if container_id not in list(
                            (linked or {}).get("linked_containers") or []):
                        raise RuntimeError("memory_link_unverified")
                    _fault(modules, "after_verify")
                    return write_result, linked, container_resident, projection

            (
                write_result, linked, container_resident, projection,
            ) = _run_container_transaction(
                container_store=container_store,
                memory_store=memory_store,
                resident_store=resident_store,
                rollback_label="container_write",
                operation=write_transaction,
            )
            receipt.update({
                "status": "applied",
                "mem_id": mem_id,
                "container_id": container_id,
                "container_type": container_type,
                "target_file": target_file,
                "write_path": write_result.get("path"),
                "write_chars": write_result.get("chars_written"),
                "container_body_written": True,
                "memory_link_applied": True,
                "linked_containers": list((linked or {}).get("linked_containers") or []),
                "current_overview": str((linked or {}).get("current_overview") or overview),
                "visibility_verified": True,
                "resident_persisted": container_resident,
                "resident_revision": resident_store.load().get("revision"),
                "resident_chars": projection.get("chars", 0),
            })
            receipts.append(receipt)
        except Exception as exc:
            if str(exc) == "invalid_target_file":
                receipts.append(
                    _reject_invalid_target_file(
                        receipt,
                        container_store.resolve_container_type(container_id),
                    )
                )
            else:
                receipts.append(_reject(receipt, str(exc)))
    return receipts
