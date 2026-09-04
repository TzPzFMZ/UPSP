"""relation_read 协议只读工具处理器。"""

from copy import deepcopy

from assembly.context_mounts import project_relation_content
from data.relation_store import RelationStore, relation_card_label, relation_public_name
from utils.content_ranges import apply_explicit_range, range_kwargs_from_request


MODES = {"none", "temporary", "resident"}
MODE_RANK = {"none": 0, "temporary": 1, "resident": 2}


def _clean(value):
    return str(value or "").strip().strip("`")


def _mode(value, default="none"):
    text = _clean(value).lower()
    return text if text in MODES else default


def _resolve_card(store, request):
    card_id = _clean(request.get("card_id"))
    subject = _clean(request.get("subject"))
    registry = store.load_registry()
    cards = registry.get("cards", [])
    if card_id:
        for card in cards:
            if card.get("id") == card_id and card.get("status") != "archived":
                return card
        if hasattr(store, "find_card"):
            card = store.find_card(card_id)
            if card and card.get("status") != "archived":
                return card
    if subject:
        for card in cards:
            if card.get("status") == "archived":
                continue
            candidates = {
                _clean(card.get("id")),
                _clean(card.get("name")),
                f"REL-{_clean(card.get('name'))}" if card.get("name") else "",
            }
            candidates.update(_clean(item) for item in card.get("aliases", []) or [])
            candidates.update(_clean(item) for item in card.get("tags", []) or [])
            if subject in candidates:
                return card
    return None


def _summary_from_card(card):
    return _clean(card.get("summary") or card.get("name") or card.get("id"))


def _body_from_card(card, request):
    notes = card.get("notes", []) if isinstance(card, dict) else []
    lines = [f"# {card.get('name') or card.get('id')}"]
    if notes:
        lines.append("")
        lines.append("## 笔记")
        for note in notes:
            content = note.get("content") if isinstance(note, dict) else str(note)
            content = _clean(content)
            if content:
                lines.append(f"- {content}")
    text = "\n".join(lines).strip()
    return apply_explicit_range(text, range_kwargs_from_request(request))


def _receipt(request, card, summary_mode, body_mode, status="accepted", reason=""):
    card = card or {}
    return {
        "tool_id": "relation_read",
        "status": status,
        "source": "protocol_tool_request",
        "card_id": card.get("id", _clean(request.get("card_id"))),
        "subject": relation_card_label(card) or relation_public_name(_clean(request.get("subject"))),
        "summary_mode": summary_mode,
        "body_mode": body_mode,
        "reason": reason or _clean(request.get("reason")),
        "summary": "",
        "body": "",
        "read_mode": "",
        "range_requested": None,
        "range_applied": None,
        "total_lines": 0,
        "total_chars": 0,
        "protocol_tool_receipt": True,
    }


class RelationResidentRollbackError(RuntimeError):
    pass


def _summary_resident_value(store, card_id):
    for card in store.load_registry().get("cards", []):
        if _clean(card.get("id")) == card_id:
            return bool(card.get("summary_resident"))
    raise RuntimeError("relation_card_not_found_during_readback")


def _apply_resident_transaction(
        store,
        assembler,
        resident_store,
        card_id,
        *,
        body_action="",
        resident_body="",
        summary_action=""):
    """Settle relation body resident state and STATUSBAR summary together."""
    if not body_action and not summary_action:
        return {}
    if body_action and (assembler is None or resident_store is None):
        raise RuntimeError("resident_context_unavailable")

    body_store = resident_store if body_action else None
    resident_before = (
        body_store.snapshot_bytes() if body_store is not None else None)
    registry_before = (
        deepcopy(store.load_registry()) if summary_action else None)
    body_result = {}
    try:
        if body_action == "remove":
            body_result = body_store.remove_matching(
                item_type="relation",
                item_id=card_id,
            )
        elif body_action == "resident":
            item = {"item_type": "relation", "item_id": card_id}
            preflight = assembler.preflight_resident_add(
                item,
                content_overrides={
                    ("relation", card_id, ""): resident_body,
                },
            )
            body_result = body_store.add(
                item,
                candidate=preflight["document"],
                expected_revision=preflight["expected_revision"],
            )
            body_result["resident_chars"] = preflight["chars"]

        if summary_action:
            store.set_summary_resident(
                card_id, summary_action == "resident")

        if body_store is not None and hasattr(body_store, "contains"):
            expected = body_action == "resident"
            actual = body_store.contains(
                item_type="relation", item_id=card_id)
            if actual != expected:
                raise RuntimeError("relation_resident_body_readback_failed")
        if summary_action:
            expected = summary_action == "resident"
            if _summary_resident_value(store, card_id) != expected:
                raise RuntimeError("relation_resident_summary_readback_failed")
        return body_result
    except Exception as exc:
        rollback_errors = []
        if summary_action:
            try:
                store.save_registry(registry_before)
                if store.load_registry() != registry_before:
                    raise RuntimeError("relation_registry_restore_mismatch")
            except Exception as rollback_exc:
                rollback_errors.append(
                    f"relation:{type(rollback_exc).__name__}")
        if body_store is not None:
            try:
                body_store.restore_bytes(resident_before)
                if body_store.snapshot_bytes() != resident_before:
                    raise RuntimeError("resident_list_restore_mismatch")
            except Exception as rollback_exc:
                rollback_errors.append(
                    f"resident:{type(rollback_exc).__name__}")
        if rollback_errors:
            raise RelationResidentRollbackError(
                "relation_resident_rollback_failed:"
                + ",".join(rollback_errors)
            ) from exc
        raise RuntimeError(
            f"relation_resident_transaction_failed:{type(exc).__name__}"
        ) from exc


def apply_relation_read_requests(requests, modules=None):
    modules = modules or {}
    store = modules.get("relation_store") or RelationStore()
    assembler = modules.get("assembler")
    resident_store = modules.get("resident_store")
    receipts = []
    mounts = []
    unmounts = []
    body_count = 0

    for request in requests or []:
        if not isinstance(request, dict):
            receipts.append(_receipt({}, {}, "none", "none", status="rejected", reason="invalid_request"))
            continue
        summary_explicit = "summary" in request
        body_explicit = "body" in request
        summary_mode = _mode(request.get("summary"), "temporary")
        body_mode = _mode(request.get("body"), "none")
        if (
                body_mode != "none"
                and MODE_RANK[summary_mode] < MODE_RANK[body_mode]):
            summary_mode = body_mode
        card = _resolve_card(store, request)
        if not card:
            receipts.append(_receipt(
                request,
                {},
                summary_mode,
                body_mode,
                status="rejected",
                reason="relation_card_not_found",
            ))
            continue

        card_id = card.get("id", "")
        category = card.get("category")
        full_card = None
        try:
            full_card = store.read_card(card_id, category=category)
        except TypeError:
            full_card = store.read_card(card_id)
        except Exception:
            full_card = None
        card_payload = full_card or card
        receipt = _receipt(request, card, summary_mode, body_mode)
        receipt["resident_persisted"] = False
        receipt["resident_revision"] = None
        body_result = None
        resident_body = ""

        if body_mode != "none":
            if body_count >= 3:
                receipt["status"] = "rejected"
                receipt["reason"] = "relation_body_limit_exceeded"
                receipts.append(receipt)
                continue
            try:
                body_result = _body_from_card(card_payload, request)
                load_resident_body = getattr(
                    assembler, "_load_relation_content", None)
                resident_body = (
                    load_resident_body(card_id)
                    if body_mode == "resident" and callable(load_resident_body)
                    else project_relation_content(card_payload, card_id)
                )
            except ValueError as exc:
                receipt["status"] = "rejected"
                receipt["reason"] = str(exc)
                receipts.append(receipt)
                continue
            body_count += 1

        body_action = (
            "remove"
            if body_explicit and body_mode == "none"
            else "resident" if body_mode == "resident" else ""
        )
        summary_action = ""
        if hasattr(store, "set_summary_resident"):
            if summary_explicit and summary_mode == "none":
                summary_action = "remove"
            elif summary_mode == "resident":
                summary_action = "resident"
        try:
            resident_result = _apply_resident_transaction(
                store,
                assembler,
                resident_store,
                card_id,
                body_action=body_action,
                resident_body=resident_body,
                summary_action=summary_action,
            )
        except RelationResidentRollbackError:
            raise
        except Exception as exc:
            receipt["status"] = "rejected"
            receipt["reason"] = str(exc) or "relation_resident_transaction_failed"
            receipts.append(receipt)
            continue

        if body_action == "remove":
            receipt["resident_persisted"] = False
            receipt["resident_revision"] = resident_result.get("revision")
            unmounts.append({
                "item_type": "relation",
                "item_id": card_id,
            })
        elif body_action == "resident":
            receipt["resident_persisted"] = True
            receipt["resident_revision"] = resident_result.get("revision")
            receipt["resident_chars"] = resident_result.get(
                "resident_chars", 0)
        if summary_action == "remove":
            receipt["resident_summary_write"] = "cleared"
            unmounts.append({
                "item_type": "relation_summary",
                "item_id": card_id,
            })

        if summary_mode != "none":
            receipt["summary"] = _summary_from_card(card_payload)
            mounts.append({
                "type": "relation_summary",
                "ids": card_id,
                "mode": summary_mode,
                "subject": relation_card_label(card),
            })

        if body_mode != "none":
            receipt["body"] = body_result.get("content", "")
            receipt["read_mode"] = body_result.get("read_mode") or "full"
            receipt["range_requested"] = body_result.get("range_requested")
            receipt["range_applied"] = body_result.get("range_applied")
            receipt["total_lines"] = body_result.get("total_lines", 0)
            receipt["total_chars"] = body_result.get("total_chars", 0)
            mount = {
                "type": "relation",
                "ids": card_id,
                "mode": body_mode,
                "source": "relation_read",
                "subject": relation_card_label(card),
                "content": (
                    resident_body
                    if body_mode == "resident"
                    else receipt.get("body", "")
                ),
                "read_mode": (
                    "full" if body_mode == "resident"
                    else receipt.get("read_mode") or "full"
                ),
                "range_requested": (
                    None if body_mode == "resident"
                    else receipt.get("range_requested")
                ),
                "range_applied": (
                    None if body_mode == "resident"
                    else receipt.get("range_applied")
                ),
                "total_lines": (
                    len(resident_body.splitlines())
                    if body_mode == "resident"
                    else receipt.get("total_lines", 0)
                ),
                "total_chars": (
                    len(resident_body)
                    if body_mode == "resident"
                    else receipt.get("total_chars", 0)
                ),
            }
            mounts.append({
                key: value for key, value in mount.items() if value is not None
            })
        receipts.append(receipt)

    return receipts, mounts, unmounts
