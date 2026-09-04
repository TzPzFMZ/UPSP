"""Round-local guide for rewriting oversized memory_write bodies."""

from copy import deepcopy
import hashlib

from logic.memory_write import apply_memory_write_declarations


GUIDE_ID_PREFIX = "memory_write_rewrite"
GUIDE_ITEM_ID = "memory_write_rewrite_due"
GUIDE_OPTION_ID = "submit_memory_write_rewrites"


def _text(value):
    return str(value or "").strip()


def _body_sha(body):
    return hashlib.sha256(_text(body).encode("utf-8")).hexdigest()


def _rejection(rewrite_id, reason, *, index=None):
    receipt = {
        "schema_version": "memory_write_rewrite_item_receipt.v1",
        "tool_id": "memory_write_rewrite",
        "status": "rejected",
        "rewrite_id": _text(rewrite_id),
        "reason": _text(reason),
    }
    if index is not None:
        receipt["index"] = index
    return receipt


def memory_write_rewrite_pending_receipts(declarations):
    """Reject direct writes while the current rewrite guide is active."""
    receipts = []
    for declaration in declarations or []:
        if not isinstance(declaration, dict):
            continue
        receipts.append({
            "tool_id": "memory_write",
            "status": "error",
            "source": "memory_write_declaration",
            "mem_id": None,
            "title": _text(declaration.get("title"))[:16],
            "weight": declaration.get("weight"),
            "subject": declaration.get("subject"),
            "keywords": list(declaration.get("candidate_keywords") or []),
            "reason": "memory_write_rewrite_pending_use_guide",
            "next_action": "use_memory_write_rewrite_guide",
        })
    return receipts


class MemoryWriteRewriteTracker:
    """Keep oversized writes only for the lifetime of one Round."""

    def __init__(self, round_num):
        self.round_num = int(round_num)
        self._next_sequence = 1
        self._pending = {}

    @property
    def guide_id(self):
        return f"{GUIDE_ID_PREFIX}:R{self.round_num:06d}"

    def has_pending(self):
        return bool(self._pending)

    def pending_ids(self):
        return list(self._pending)

    def get(self, rewrite_id):
        item = self._pending.get(_text(rewrite_id))
        return deepcopy(item) if item is not None else None

    def complete(self, rewrite_id):
        return self._pending.pop(_text(rewrite_id), None)

    def register_receipts(self, declarations, receipts):
        """Freeze declarations whose sole processor error is body length."""
        created = []
        for declaration, receipt in zip(declarations or [], receipts or []):
            if not isinstance(declaration, dict) or not isinstance(receipt, dict):
                continue
            reason = _text(receipt.get("reason"))
            if not reason.startswith("memory_body_too_long:"):
                continue
            rewrite_id = (
                f"MWR-R{self.round_num:06d}-N{self._next_sequence:03d}"
            )
            self._next_sequence += 1
            body = _text(declaration.get("body"))
            frozen = {
                "title": _text(receipt.get("title")),
                "weight": int(receipt.get("weight") or 0),
                "subject": _text(receipt.get("subject")),
                "candidate_keywords": list(receipt.get("keywords") or []),
                "interaction_feelings": list(
                    receipt.get("interaction_feelings") or []
                ),
                "relationship_feelings": [
                    dict(item)
                    for item in receipt.get("relationship_feelings") or []
                    if isinstance(item, dict)
                ],
                "reason": _text(declaration.get("reason")),
            }
            source = {
                key: receipt.get(key)
                for key in (
                    "call_id",
                    "provider",
                    "response_id",
                    "provider_item_id",
                    "index",
                    "iteration",
                )
                if receipt.get(key) not in (None, "")
            }
            item = {
                "rewrite_id": rewrite_id,
                "declaration": frozen,
                "original_body": body,
                "original_chars": len(body),
                "body_limit": int(receipt.get("max_chars") or 0),
                "body_sha256": _body_sha(body),
                "source": source,
            }
            self._pending[rewrite_id] = item
            created.append(self._public_item(item))
        return created

    def render_guide(self, discipline):
        discipline = _text(discipline)
        if not discipline:
            raise ValueError("memory_write_rewrite_guide_template_missing")
        if not self._pending:
            return ""
        lines = [
            "## GUIDE｜记忆写入重写指南",
            discipline,
            "",
            "调用坐标：",
            f"- guide_id={self.guide_id}",
            f"- item_id={GUIDE_ITEM_ID}",
            f"- option_id={GUIDE_OPTION_ID}",
            "",
            "当前待办：",
        ]
        for item in self._pending.values():
            source = item.get("source") or {}
            coordinate = _text(source.get("call_id")) or "—"
            lines.append(
                f"- {item['rewrite_id']}｜{item['declaration']['title']}｜"
                f"actual={item['original_chars']}｜max={item['body_limit']}｜"
                f"source_call={coordinate}"
            )
        example_id = self.pending_ids()[0]
        lines.extend([
            "",
            "使用 guide_submit 的 fields.results 覆盖全部当前 rewrite_id；"
            "每项只能使用 rewrite_id、action、semantic_content 三个字段。",
            (
                '精确形状：fields={"results":[{"rewrite_id":"'
                f'{example_id}","action":"rewrite","semantic_content":'
                '"不超过冻结上限的纯语义正文"}]}。'
            ),
            (
                "选择 not_written 时仍使用同一三个字段，"
                '并令 action="not_written"、semantic_content=""；'
                "不要使用 body 或 content 代替 semantic_content。"
            ),
        ])
        return "\n".join(lines)

    def render_materials(self):
        materials = []
        for item in self._pending.values():
            materials.append({
                "role": "system",
                "kind": "material",
                "source": "memory_write_rewrite",
                "source_block_id": item["rewrite_id"],
                "content": "\n".join([
                    "记忆写入超限重写材料",
                    f"rewrite_id: {item['rewrite_id']}",
                    f"title: {item['declaration']['title']}",
                    f"body_limit: {item['body_limit']}",
                    f"original_chars: {item['original_chars']}",
                    f"body_sha256: {item['body_sha256']}",
                    "原正文：",
                    item["original_body"],
                ]),
            })
        return materials

    def audit_state(self):
        return {
            "guide_id": self.guide_id,
            "pending_items": [
                self._public_item(item) for item in self._pending.values()
            ],
        }

    @staticmethod
    def _public_item(item):
        return {
            "rewrite_id": item["rewrite_id"],
            "title": item["declaration"]["title"],
            "weight": item["declaration"]["weight"],
            "subject": item["declaration"]["subject"],
            "keywords": list(item["declaration"]["candidate_keywords"]),
            "original_chars": item["original_chars"],
            "body_limit": item["body_limit"],
            "body_sha256": item["body_sha256"],
            "source": dict(item.get("source") or {}),
        }


def apply_memory_write_rewrite_guide(arguments, evidence_context):
    """Settle every currently pending rewrite independently."""
    arguments = arguments if isinstance(arguments, dict) else {}
    context = dict(evidence_context or {})
    tracker = context.get("memory_write_rewrite_tracker")
    if not isinstance(tracker, MemoryWriteRewriteTracker):
        return _result("rejected", "memory_write_rewrite_context_unavailable")
    if _text(arguments.get("guide_id")) != tracker.guide_id:
        return _result(
            "rejected", "memory_write_rewrite_guide_not_active", tracker
        )
    if _text(arguments.get("item_id")) != GUIDE_ITEM_ID:
        return _result("rejected", "memory_write_rewrite_item_invalid", tracker)
    if _text(arguments.get("option_id")) != GUIDE_OPTION_ID:
        return _result("rejected", "memory_write_rewrite_option_invalid", tracker)
    fields = arguments.get("fields")
    if not isinstance(fields, dict) or set(fields) != {"results"}:
        return _result("rejected", "memory_write_rewrite_fields_invalid", tracker)
    results = fields.get("results")
    if not isinstance(results, list):
        return _result("rejected", "memory_write_rewrite_results_invalid", tracker)

    submitted = {}
    backend_receipts = []
    for index, item in enumerate(results):
        if not isinstance(item, dict):
            backend_receipts.append(_rejection(
                "", "memory_write_rewrite_result_invalid", index=index
            ))
            continue
        rewrite_id = _text(item.get("rewrite_id"))
        if set(item) != {"rewrite_id", "action", "semantic_content"}:
            backend_receipts.append(_rejection(
                rewrite_id,
                "memory_write_rewrite_result_fields_invalid",
                index=index,
            ))
            continue
        submitted.setdefault(rewrite_id, []).append(item)

    completed_ids = []
    created_memory_ids = []
    not_written_ids = []
    state_store = context.get("state_store")
    data_modules = {
        "memory_store": context.get("memory_store"),
        "memory_heat": context.get("memory_heat"),
        "relation_store": context.get("relation_store"),
    }
    round_num = int(context.get("round_num") or tracker.round_num)
    pending_ids = tracker.pending_ids()
    for rewrite_id in pending_ids:
        matches = submitted.get(rewrite_id) or []
        if len(matches) != 1:
            backend_receipts.append(_rejection(
                rewrite_id,
                (
                    "memory_write_rewrite_result_missing"
                    if not matches
                    else "memory_write_rewrite_result_duplicate"
                ),
            ))
            continue
        result = matches[0]
        action = _text(result.get("action"))
        semantic = _text(result.get("semantic_content"))
        if action == "not_written":
            if semantic:
                backend_receipts.append(_rejection(
                    rewrite_id, "memory_write_rewrite_not_written_body_not_empty"
                ))
                continue
            tracker.complete(rewrite_id)
            completed_ids.append(rewrite_id)
            not_written_ids.append(rewrite_id)
            backend_receipts.append({
                "schema_version": "memory_write_rewrite_item_receipt.v1",
                "tool_id": "memory_write_rewrite",
                "status": "applied",
                "action": "not_written",
                "rewrite_id": rewrite_id,
                "mem_id": None,
            })
            continue
        if action != "rewrite":
            backend_receipts.append(_rejection(
                rewrite_id, "memory_write_rewrite_action_invalid"
            ))
            continue
        frozen = tracker.get(rewrite_id)
        if not semantic:
            backend_receipts.append(_rejection(
                rewrite_id, "memory_write_rewrite_body_empty"
            ))
            continue
        if len(semantic) > int(frozen.get("body_limit") or 0):
            backend_receipts.append(_rejection(
                rewrite_id, "memory_write_rewrite_body_too_long"
            ))
            continue
        if state_store is None or any(
            data_modules[key] is None
            for key in ("memory_store", "memory_heat", "relation_store")
        ):
            backend_receipts.append(_rejection(
                rewrite_id, "memory_write_rewrite_processor_unavailable"
            ))
            continue
        declaration = dict(frozen["declaration"])
        declaration["body"] = semantic
        receipts = apply_memory_write_declarations(
            [declaration], state_store.load(), round_num, data_modules
        )
        receipt = dict(receipts[0] if receipts else {})
        receipt.update({
            "rewrite_id": rewrite_id,
            "rewrite_source": "memory_write_rewrite_guide",
            "original_body_sha256": frozen["body_sha256"],
            "source_call": dict(frozen.get("source") or {}),
        })
        backend_receipts.append(receipt)
        if receipt.get("status") != "applied" or not receipt.get("mem_id"):
            continue
        tracker.complete(rewrite_id)
        completed_ids.append(rewrite_id)
        created_memory_ids.append(receipt["mem_id"])

    for rewrite_id, matches in submitted.items():
        if rewrite_id in pending_ids:
            continue
        for _item in matches:
            backend_receipts.append(_rejection(
                rewrite_id, "memory_write_rewrite_result_unknown"
            ))

    return {
        "status": "applied" if completed_ids else "rejected",
        "reason": "" if completed_ids else "memory_write_rewrite_no_item_completed",
        "backend_receipts": backend_receipts,
        "completed_ids": completed_ids,
        "remaining_ids": tracker.pending_ids(),
        "created_memory_ids": created_memory_ids,
        "not_written_ids": not_written_ids,
    }


def _result(status, reason, tracker=None):
    return {
        "status": status,
        "reason": reason,
        "backend_receipts": [],
        "completed_ids": [],
        "remaining_ids": (
            tracker.pending_ids()
            if isinstance(tracker, MemoryWriteRewriteTracker)
            else []
        ),
        "created_memory_ids": [],
        "not_written_ids": [],
    }
