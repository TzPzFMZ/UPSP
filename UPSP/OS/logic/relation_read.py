"""relation_read 协议只读工具处理器。"""

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
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
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


def apply_relation_read_requests(requests, modules=None):
    modules = modules or {}
    store = modules.get("relation_store") or RelationStore()
    receipts = []
    mounts = []
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
        body_result = None

        if body_mode != "none":
            if body_count >= 3:
                receipt["status"] = "rejected"
                receipt["reason"] = "relation_body_limit_exceeded"
                receipts.append(receipt)
                continue
            try:
                body_result = _body_from_card(card_payload, request)
            except ValueError as exc:
                receipt["status"] = "rejected"
                receipt["reason"] = str(exc)
                receipts.append(receipt)
                continue
            body_count += 1

        if body_explicit and body_mode == "none" and hasattr(store, "set_body_resident"):
            try:
                store.set_body_resident(card_id, False)
                receipt["resident_body_write"] = "cleared"
            except Exception:
                receipt["resident_body_write"] = "failed"

        if summary_explicit and summary_mode == "none" and hasattr(store, "set_summary_resident"):
            try:
                store.set_summary_resident(card_id, False)
                receipt["resident_summary_write"] = "cleared"
            except Exception:
                receipt["resident_summary_write"] = "failed"

        if summary_mode != "none":
            receipt["summary"] = _summary_from_card(card_payload)
            mounts.append({
                "type": "relation_summary",
                "ids": card_id,
                "mode": summary_mode,
                "subject": relation_card_label(card),
            })
            if summary_mode == "resident" and hasattr(store, "set_summary_resident"):
                try:
                    store.set_summary_resident(card_id, True)
                except Exception:
                    receipt["resident_summary_write"] = "failed"

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
                "content": receipt.get("body", ""),
                "read_mode": receipt.get("read_mode") or "full",
                "range_requested": receipt.get("range_requested"),
                "range_applied": receipt.get("range_applied"),
                "total_lines": receipt.get("total_lines", 0),
                "total_chars": receipt.get("total_chars", 0),
            }
            mounts.append({
                key: value for key, value in mount.items() if value is not None
            })
            if body_mode == "resident" and hasattr(store, "set_body_resident"):
                try:
                    store.set_body_resident(card_id, True)
                except Exception:
                    receipt["resident_body_write"] = "failed"

        receipts.append(receipt)

    return receipts, mounts
