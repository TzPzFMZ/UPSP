"""Failed write pending settlement for reaction protocol tools."""

from copy import deepcopy

from data.relation_store import relation_public_name


WRITE_PENDING_TOOLS = {
    "memory_write",
    "chronicle_write",
}
CANCELLABLE_WRITE_TOOLS = {"memory_write"}

SUCCESS_STATUSES = {"applied", "ok", "accepted", "success", "completed"}
NON_PENDING_STATUSES = {
    "skipped",
    "submission_received",
    "guide_loaded",
}
NON_PENDING_REASONS = {
    "duplicate_container_focus_satisfied",
    "no_active_chronicle_focus",
}
CANCEL_REASON_CODES = (
    "wrong_target",
    "obsolete_intent",
    "duplicate_intent",
    "low_value",
    "user_not_confirmed",
    "unsafe_or_invalid",
    "superseded_by_retry",
)

def _clean_text(value):
    return str(value or "").replace("\r\n", "\n").replace("\r", "\n").strip()


def _tool_id(receipt):
    return _clean_text((receipt or {}).get("tool_id"))


def _status(receipt):
    return _clean_text((receipt or {}).get("status")).lower()


def _is_memory_body_too_long(reason):
    return str(reason or "").startswith("memory_body_too_long:")


SUBJECT_RESOLUTION_REASONS = {
    "identity_unresolved",
    "subject_not_in_relation_domain",
    "subject_not_confirmed",  # historical receipt compatibility
}


def _is_subject_resolution_failure(reason):
    return _clean_text(reason) in SUBJECT_RESOLUTION_REASONS


def _parse_memory_body_too_long(reason):
    max_chars = 0
    actual_chars = 0
    for part in str(reason or "").split(":")[-1].split(";"):
        if "=" not in part:
            continue
        key, value = part.split("=", 1)
        try:
            number = int(value)
        except ValueError:
            continue
        if key == "max":
            max_chars = number
        elif key == "actual":
            actual_chars = number
    return max_chars, actual_chars


def _memory_body_too_long_lines(reason):
    max_chars, actual_chars = _parse_memory_body_too_long(reason)
    if max_chars and actual_chars:
        first = (
            "memory_write.body 超出当前权重上限："
            f"actual={actual_chars}, max={max_chars}。"
        )
    else:
        first = "memory_write.body 超出当前权重上限。"
    return [
        first,
        "请压缩正文或调整 weight 后重新调用 memory_write。",
        "不要只因字数升权。",
    ]


def _subject_resolution_values(pending):
    if not isinstance(pending, dict):
        pending = {}
    receipt = pending.get("receipt")
    if not isinstance(receipt, dict):
        receipt = {}
    submitted = relation_public_name(_clean_text(
        receipt.get("submitted_subject")
        or pending.get("submitted_subject")
        or receipt.get("subject")
        or "unknown"))
    confirmed = relation_public_name(_clean_text(
        receipt.get("confirmed_subject")
        or pending.get("confirmed_subject")))
    confirmed_subjects = (
        receipt.get("confirmed_subjects")
        if isinstance(receipt.get("confirmed_subjects"), list)
        else pending.get("confirmed_subjects")
    )
    if not confirmed and isinstance(confirmed_subjects, list) and confirmed_subjects:
        confirmed = relation_public_name(_clean_text(confirmed_subjects[0]))
    return submitted, confirmed


def _subject_resolution_lines(pending, *, allow_natural_blocked=False):
    reason = _clean_text(pending.get("reason"))
    submitted, confirmed = _subject_resolution_values(pending)
    lines = [f"失败原因：{reason}。"]
    if submitted:
        lines.append(f"提交主题：{submitted}。")
    if reason == "identity_unresolved":
        lines.append("当前确认对象：未确认。")
    elif confirmed:
        lines.append(f"当前确认对象：{confirmed}。")
    else:
        lines.append("当前确认对象：未记录。")
    if reason == "subject_not_in_relation_domain":
        receipt = pending.get("receipt") if isinstance(pending, dict) else {}
        receipt = receipt if isinstance(receipt, dict) else {}
        candidate = relation_public_name(_clean_text(receipt.get("subject")))
        lines.append("该记忆主体不在活动关系域；不得改填当前对象来伪造归属。")
        if confirmed and candidate == confirmed:
            lines.append(
                "该主体是当前直接交互对象；确需沉淀时，可先合法登记关系卡并等待成功回执。"
            )
        else:
            lines.append("不得为缺席或无关第三方自动创建关系卡。")
    else:
        lines.append(
            "身份/主题未确认时禁止写记忆或关系卡；不要靠继续写入来做自修。"
        )
    if allow_natural_blocked:
        lines.append(
            "如果任务硬验收依赖这条记忆，当前为 NO-GO："
            "可以自然回复请求用户确认，或说明阻断事实。"
        )
    else:
        lines.append(
            "当前仍为 NO-GO；如果需要用户确认，先按显式 pending 出口结算当前失败意图；"
            "不得继续写入自修。"
        )
    return lines


def _settlement_stage(pending):
    return _clean_text((pending or {}).get("settlement_stage")) or "settlement_required"


def _subject_resolution_shadow_can_defer(pending):
    """Allow one unresolved-subject failure to wait for a user reply.

    This is deliberately narrower than a general failed-write bypass: only the
    first, ID-less retry shadow for memory_write qualifies.  A second failure
    has an explicit pending ID and remains settlement-blocking.
    """
    pending = pending if isinstance(pending, dict) else {}
    return (
        pending.get("status") == "open"
        and _settlement_stage(pending) == "retry_required"
        and not _clean_text(pending.get("pending_id"))
        and _clean_text(pending.get("target_tool_id")) == "memory_write"
        and _is_subject_resolution_failure(pending.get("reason"))
        and not bool(pending.get("mandatory"))
    )


def _starts_retry_required(receipt):
    return (
        _tool_id(receipt) == "memory_write"
        and (
            _is_memory_body_too_long(_clean_text((receipt or {}).get("reason")))
            or _is_subject_resolution_failure((receipt or {}).get("reason"))
        )
    )


def _key_text(value):
    return " ".join(_clean_text(value).casefold().split())


def _write_intent_key(receipt):
    """Stable structural key for retryable memory_write failures.

    Body is intentionally excluded: compression rewrites should still be the
    same write intent.  No semantic parsing here; only tool/title/subject.
    """
    if _tool_id(receipt) != "memory_write":
        return ""
    title = _key_text((receipt or {}).get("title"))
    subject = _key_text(
        (receipt or {}).get("submitted_subject")
        or (receipt or {}).get("subject"))
    return "\x1f".join([_tool_id(receipt), title, subject])


def is_write_receipt(receipt):
    return isinstance(receipt, dict) and _tool_id(receipt) in WRITE_PENDING_TOOLS


def write_receipt_applied(receipt):
    return _status(receipt) in SUCCESS_STATUSES


def write_receipt_failed(receipt):
    if not is_write_receipt(receipt):
        return False
    status = _status(receipt)
    if not status or status in SUCCESS_STATUSES or status in NON_PENDING_STATUSES:
        return False
    if _clean_text(receipt.get("reason")) in NON_PENDING_REASONS:
        return False
    return True


def is_mandatory_write(receipt, round_type=""):
    if not isinstance(receipt, dict):
        return False
    tool_id = _tool_id(receipt)
    if (
            tool_id == "chronicle_write"
            and _clean_text(receipt.get("layer")) in {"rhythms", "daily"}
            and (
                _clean_text(receipt.get("round_type")) == "rhythm"
                or _clean_text(round_type) == "rhythm"
            )):
        return True
    if tool_id == "fault_record":
        if _clean_text(receipt.get("action")) == "emergency_save":
            return True
        if _clean_text(receipt.get("fault_type")) in {
                "runtime_exception",
                "api_degraded",
        }:
            return True
        if _clean_text(receipt.get("severity")) in {"error", "critical"}:
            return True
    if tool_id == "alert_mode_settle":
        if receipt.get("clear_flags"):
            return True
        if _clean_text(receipt.get("alert_status")) in {
                "recovered",
                "deferred",
                "needs_human",
        }:
            return True
    return False


def rhythm_chronicle_write_applied(result):
    if not isinstance(result, dict):
        return False
    receipts = []
    for key in ("_chronicle_write_receipts", "_protocol_tool_receipts"):
        receipts.extend(result.get(key) or [])
    for receipt in _iter_receipts_with_backend(receipts):
        if not isinstance(receipt, dict):
            continue
        if (
                _tool_id(receipt) == "chronicle_write"
                and _status(receipt) == "applied"
                and _clean_text(receipt.get("layer")) == "rhythms"
                and _clean_text(receipt.get("round_type")) == "rhythm"):
            return True
    return False


def _iter_receipts_with_backend(receipts):
    stack = list(receipts or [])
    while stack:
        receipt = stack.pop(0)
        yield receipt
        if isinstance(receipt, dict):
            stack[0:0] = list(receipt.get("backend_receipts") or [])


class WritePendingTracker:
    """Track failed write intents that must be retried, resolved, or cancelled."""

    def __init__(self, round_num, round_type=""):
        self.round_num = int(round_num or 0)
        self.round_type = _clean_text(round_type)
        self._pendings = {}
        self._order = []
        self._retry_shadows = {}
        self._retry_shadow_order = []
        self._counter = 0
        self._cancel_fact_unconsumed = False
        self._recently_resolved_pendings = []

    def _new_pending_id(self):
        self._counter += 1
        return f"PEND-R{self.round_num:06d}-N{self._counter:03d}"

    def _pending_from_receipt(
            self,
            receipt,
            *,
            settlement_stage=None,
            retry_count=0,
            retry_key=""):
        pending_id = self._new_pending_id()
        mandatory = is_mandatory_write(receipt, self.round_type)
        stage = _clean_text(settlement_stage) or "settlement_required"
        pending = {
            "pending_id": pending_id,
            "status": "open",
            "settlement_stage": stage,
            "cancel_available": (
                stage == "settlement_required"
                and not mandatory
                and _tool_id(receipt) in CANCELLABLE_WRITE_TOOLS
            ),
            "retry_count": int(retry_count or 0),
            "retry_key": retry_key or _write_intent_key(receipt),
            "target_tool_id": _tool_id(receipt),
            "reason": _clean_text(receipt.get("reason")),
            "call_id": _clean_text(receipt.get("call_id")),
            "call_ids": [
                item for item in [_clean_text(receipt.get("call_id"))] if item
            ],
            "source_status": _clean_text(receipt.get("status")),
            "mandatory": mandatory,
            "title": _clean_text(receipt.get("title")),
            "subject": _clean_text(
                receipt.get("submitted_subject") or receipt.get("subject")),
            "receipt": deepcopy(receipt),
            "resolution": {},
        }
        return pending

    def _stamp_receipt_for_pending(self, receipt, pending):
        receipt["write_pending_id"] = pending["pending_id"]
        receipt["write_pending_mandatory"] = bool(pending.get("mandatory"))
        receipt["write_pending_stage"] = _settlement_stage(pending)
        receipt["write_pending_cancel_available"] = bool(
            pending.get("cancel_available")
        )

    def _shadow_from_receipt(self, receipt):
        retry_key = _write_intent_key(receipt)
        if not retry_key:
            return None
        shadow = {
            "status": "open",
            "settlement_stage": "retry_required",
            "cancel_available": False,
            "retry_count": 0,
            "retry_key": retry_key,
            "target_tool_id": _tool_id(receipt),
            "reason": _clean_text(receipt.get("reason")),
            "call_id": _clean_text(receipt.get("call_id")),
            "call_ids": [
                item for item in [_clean_text(receipt.get("call_id"))] if item
            ],
            "source_status": _clean_text(receipt.get("status")),
            "mandatory": is_mandatory_write(receipt, self.round_type),
            "title": _clean_text(receipt.get("title")),
            "subject": _clean_text(
                receipt.get("submitted_subject") or receipt.get("subject")),
            "receipt": deepcopy(receipt),
            "resolution": {},
        }
        return shadow

    def _open_shadow_for_receipt(self, receipt):
        retry_key = _write_intent_key(receipt)
        if not retry_key:
            return None
        shadow = self._retry_shadows.get(retry_key)
        if shadow and shadow.get("status") == "open":
            return shadow
        return None

    def _clear_shadow_for_receipt(self, receipt):
        retry_key = _write_intent_key(receipt)
        if not retry_key:
            return False
        shadow = self._retry_shadows.get(retry_key)
        if not shadow or shadow.get("status") != "open":
            return False
        shadow["status"] = "resolved"
        shadow["resolution"] = {
            "mode": "resolved_by_write",
            "tool_id": _tool_id(receipt),
            "status": _clean_text(receipt.get("status")),
            "call_id": _clean_text(receipt.get("call_id")),
        }
        return True

    def _open_pending_for_receipt(self, receipt):
        retry_key = _write_intent_key(receipt)
        if not retry_key:
            return None
        for pending_id in self._order:
            pending = self._pendings.get(pending_id)
            if not pending or pending.get("status") != "open":
                continue
            if pending.get("retry_key") == retry_key:
                return pending
        return None

    def _update_failed_retry(self, pending_id, receipt):
        pending_id = _clean_text(pending_id)
        pending = self._pendings.get(pending_id)
        if not pending or pending.get("status") != "open":
            return None
        retry_count = int(pending.get("retry_count") or 0) + 1
        target_tool_id = _tool_id(receipt) or pending.get("target_tool_id", "")
        reason = _clean_text(receipt.get("reason"))
        pending["settlement_stage"] = "settlement_required"
        pending["cancel_available"] = (
            pending["settlement_stage"] == "settlement_required"
            and not bool(pending.get("mandatory"))
            and target_tool_id in CANCELLABLE_WRITE_TOOLS
        )
        pending["reason"] = _clean_text(receipt.get("reason"))
        previous_call_ids = list(pending.get("call_ids") or [])
        previous_call_id = _clean_text(pending.get("call_id"))
        new_call_id = _clean_text(receipt.get("call_id"))
        for call_id in (previous_call_id, new_call_id):
            if call_id and call_id not in previous_call_ids:
                previous_call_ids.append(call_id)
        pending["call_id"] = new_call_id
        pending["call_ids"] = previous_call_ids
        pending["source_status"] = _clean_text(receipt.get("status"))
        pending["target_tool_id"] = target_tool_id
        if _write_intent_key(receipt):
            pending["retry_key"] = _write_intent_key(receipt)
        pending["title"] = _clean_text(receipt.get("title"))
        pending["subject"] = _clean_text(
            receipt.get("submitted_subject") or receipt.get("subject"))
        pending["receipt"] = deepcopy(receipt)
        pending["retry_count"] = retry_count
        pending["resolution"] = {
            "mode": "retry_failed",
            "tool_id": _tool_id(receipt),
            "status": _clean_text(receipt.get("status")),
            "call_id": _clean_text(receipt.get("call_id")),
            "retry_count": retry_count,
        }
        self._stamp_receipt_for_pending(receipt, pending)
        pending["receipt"] = deepcopy(receipt)
        return deepcopy(pending)

    def _promote_shadow_to_pending(self, shadow, receipt):
        retry_key = _write_intent_key(receipt) or shadow.get("retry_key", "")
        pending = self._pending_from_receipt(
            receipt,
            settlement_stage="settlement_required",
            retry_count=int(shadow.get("retry_count") or 0) + 1,
            retry_key=retry_key,
        )
        shadow_call_id = _clean_text(shadow.get("call_id"))
        if shadow_call_id and shadow_call_id not in pending.get("call_ids", []):
            pending["call_ids"] = [shadow_call_id] + list(
                pending.get("call_ids") or [])
        pending["resolution"] = {
            "mode": "retry_failed",
            "tool_id": _tool_id(receipt),
            "status": _clean_text(receipt.get("status")),
            "call_id": _clean_text(receipt.get("call_id")),
            "retry_count": pending.get("retry_count", 1),
        }
        self._stamp_receipt_for_pending(receipt, pending)
        pending["receipt"] = deepcopy(receipt)
        self._pendings[pending["pending_id"]] = pending
        self._order.append(pending["pending_id"])
        if retry_key in self._retry_shadows:
            self._retry_shadows[retry_key]["status"] = "promoted"
            self._retry_shadows[retry_key]["resolution"] = {
                "mode": "promoted_to_pending",
                "pending_id": pending["pending_id"],
            }
        return deepcopy(pending)

    def observe_receipts(self, receipts):
        created = []
        for receipt in receipts or []:
            if not isinstance(receipt, dict):
                continue
            if not is_write_receipt(receipt):
                continue
            resolves_pending_id = _clean_text(receipt.get("resolves_pending_id"))
            if resolves_pending_id and write_receipt_applied(receipt):
                if self.resolve_pending(resolves_pending_id, receipt):
                    receipt["write_pending_resolved"] = True
                continue
            if not resolves_pending_id and write_receipt_applied(receipt):
                if self._clear_shadow_for_receipt(receipt):
                    receipt["write_retry_shadow_resolved"] = True
                continue
            if (
                    resolves_pending_id
                    and write_receipt_failed(receipt)):
                updated = self._update_failed_retry(resolves_pending_id, receipt)
                if updated:
                    created.append(updated)
                    continue
            if not resolves_pending_id and write_receipt_failed(receipt):
                pending = self._open_pending_for_receipt(receipt)
                if pending:
                    updated = self._update_failed_retry(
                        pending["pending_id"], receipt)
                    if updated:
                        created.append(updated)
                        continue
                shadow = self._open_shadow_for_receipt(receipt)
                if shadow:
                    promoted = self._promote_shadow_to_pending(shadow, receipt)
                    created.append(promoted)
                    continue
                if _starts_retry_required(receipt):
                    shadow = self._shadow_from_receipt(receipt)
                    if shadow:
                        retry_key = shadow["retry_key"]
                        self._retry_shadows[retry_key] = shadow
                        if retry_key not in self._retry_shadow_order:
                            self._retry_shadow_order.append(retry_key)
                        created.append(deepcopy(shadow))
                        continue
            if not write_receipt_failed(receipt):
                continue
            pending = self._pending_from_receipt(receipt)
            self._stamp_receipt_for_pending(receipt, pending)
            pending["receipt"] = deepcopy(receipt)
            self._pendings[pending["pending_id"]] = pending
            self._order.append(pending["pending_id"])
            created.append(deepcopy(pending))
        return created

    def resolve_pending(self, pending_id, receipt):
        pending_id = _clean_text(pending_id)
        pending = self._pendings.get(pending_id)
        if not pending or pending.get("status") != "open":
            return False
        pending["status"] = "resolved"
        pending["resolution"] = {
            "mode": "resolved_by_write",
            "tool_id": _tool_id(receipt),
            "status": _clean_text(receipt.get("status")),
            "call_id": _clean_text(receipt.get("call_id")),
        }
        self._recently_resolved_pendings.append(deepcopy(pending))
        return True

    def consume_recently_resolved_pendings(self):
        result = [deepcopy(item) for item in self._recently_resolved_pendings]
        self._recently_resolved_pendings = []
        return result

    def cancel_pending(self, request):
        request = request or {}
        pending_id = _clean_text(request.get("pending_id"))
        reason_code = _clean_text(request.get("reason_code"))
        note = _clean_text(request.get("note"))
        base = {
            "tool_id": "pending_cancel",
            "tool_family": "protocol_tool",
            "tool_class": "sync_tool",
            "protocol_tool_receipt": True,
            "source": "pending_cancel",
            "pending_id": pending_id,
            "reason_code": reason_code,
            "note": note,
        }
        pending = self._pendings.get(pending_id)
        if not pending:
            return {
                **base,
                "status": "not_found_or_settled",
                "reason": "pending_not_found_or_settled",
            }
        base["target_tool_id"] = pending.get("target_tool_id", "")
        base["target_call_id"] = pending.get("call_id", "")
        if pending.get("status") != "open":
            return {
                **base,
                "status": "not_found_or_settled",
                "reason": "pending_not_found_or_settled",
            }
        if pending.get("mandatory"):
            return {
                **base,
                "status": "rejected",
                "reason": "pending_cancel_forbidden",
                "mandatory": True,
            }
        if pending.get("target_tool_id") not in CANCELLABLE_WRITE_TOOLS:
            return {
                **base,
                "status": "rejected",
                "reason": "pending_cancel_unsupported_tool",
            }
        if _settlement_stage(pending) == "retry_required":
            return {
                **base,
                "status": "rejected",
                "reason": "pending_cancel_not_available_yet",
            }
        if reason_code not in CANCEL_REASON_CODES:
            return {
                **base,
                "status": "rejected",
                "reason": "invalid_reason_code",
            }
        pending["status"] = "cancelled"
        pending["resolution"] = {
            "mode": "cancelled",
            "reason_code": reason_code,
            "note": note,
        }
        self._cancel_fact_unconsumed = True
        return {
            **base,
            "status": "applied",
            "reason": "pending_cancelled",
            "cancel_effect": "original_write_not_applied",
            "target_tool_id": pending.get("target_tool_id", ""),
            "target_call_id": pending.get("call_id", ""),
        }

    def has_unconsumed_cancel_fact(self):
        return self._cancel_fact_unconsumed

    def mark_cancel_facts_consumed(self):
        self._cancel_fact_unconsumed = False

    def open_pendings(self):
        result = []
        for pending_id in self._order:
            pending = self._pendings.get(pending_id)
            if pending and pending.get("status") == "open":
                result.append(deepcopy(pending))
        return result

    def open_retry_shadows(self):
        result = []
        for retry_key in self._retry_shadow_order:
            shadow = self._retry_shadows.get(retry_key)
            if shadow and shadow.get("status") == "open":
                result.append(deepcopy(shadow))
        return result

    def open_write_obligations(self):
        return self.open_retry_shadows() + self.open_pendings()

    def cancelled_pendings(self):
        result = []
        for pending_id in self._order:
            pending = self._pendings.get(pending_id)
            if pending and pending.get("status") == "cancelled":
                result.append(deepcopy(pending))
        return result

    def audit(self):
        return {
            "round_num": self.round_num,
            "round_type": self.round_type,
            "pendings": [deepcopy(self._pendings[item]) for item in self._order],
            "retry_shadows": [
                deepcopy(self._retry_shadows[item])
                for item in self._retry_shadow_order
            ],
            "cancel_fact_unconsumed": self._cancel_fact_unconsumed,
        }

    def finalize_blocker(self):
        if self._cancel_fact_unconsumed:
            return {
                "blocked": True,
                "reason": "pending_cancel_result_not_consumed",
                "pendings": self.open_write_obligations(),
            }
        open_pendings = self.open_write_obligations()
        deferred_subject_resolution = [
            item for item in open_pendings
            if _subject_resolution_shadow_can_defer(item)
        ]
        blocking_pendings = [
            item for item in open_pendings
            if not _subject_resolution_shadow_can_defer(item)
        ]
        if blocking_pendings:
            reason = (
                "mandatory_write_pending_unresolved"
                if any(item.get("mandatory") for item in blocking_pendings)
                else "write_pending_unresolved"
            )
            return {
                "blocked": True,
                "reason": reason,
                "pendings": blocking_pendings,
                "deferred_subject_resolution": deferred_subject_resolution,
            }
        if deferred_subject_resolution:
            deferred_reasons = {
                _clean_text(item.get("reason"))
                for item in deferred_subject_resolution
            }
            return {
                "blocked": False,
                "reason": (
                    "subject_resolution_unresolved"
                    if "subject_not_in_relation_domain" in deferred_reasons
                    else "subject_resolution_waiting_for_user"
                ),
                "pendings": [],
                "deferred_subject_resolution": deferred_subject_resolution,
            }
        return {"blocked": False, "reason": "", "pendings": []}


def format_pending_cancel_tool_fact(receipt):
    if not isinstance(receipt, dict):
        return ""
    status = _clean_text(receipt.get("status"))
    pending_id = _clean_text(receipt.get("pending_id"))
    note = _clean_text(receipt.get("note"))
    if status == "applied":
        lines = [
            "【本轮失败写入取消回执】",
            "这次取消已经生效。",
            "原写入没有成功；已经明确放弃这次写入意图。",
            "最终回复不能说已经写入、已经记录或已经保存。",
        ]
        if pending_id:
            lines.append(f"取消对象：{pending_id}。")
        if note:
            lines.append(f"取消说明：{note}")
        return "\n".join(lines)
    reason = _clean_text(receipt.get("reason")) or "unknown"
    if status == "not_found_or_settled" or reason == "pending_not_found_or_settled":
        return "\n".join([
            "【本轮失败写入取消回执】",
            "未发现此提醒，或此提醒已结清。",
        ])
    lines = [
        "【本轮失败写入取消回执】",
        f"取消没有生效，处理结果：{status or 'unknown'}。",
        "失败详情：请查看 POPUP 中相同 tool_id/call_id 的工具提醒。",
    ]
    if reason == "pending_cancel_forbidden":
        lines.append("这个 pending 属于必须结算的写入，不能取消；请重试、改参数或阻塞收束。")
    elif reason == "pending_cancel_not_available_yet":
        lines.append("这次失败写入需要先修正后重写。")
        lines.append("请压缩正文或调整 weight 后重新调用 memory_write。")
    return "\n".join(lines)


def format_write_pending_notice(pendings):
    lines = []
    for pending in pendings or []:
        if not isinstance(pending, dict):
            continue
        pending_id = _clean_text(pending.get("pending_id"))
        tool_id = _clean_text(pending.get("target_tool_id"))
        reason = _clean_text(pending.get("reason")) or "unknown"
        stage = _settlement_stage(pending)
        explicit = stage == "settlement_required" and bool(pending_id)
        lines.append("【失败写入待结算】" if explicit else "【失败写入需要重写】")
        body_too_long = _is_memory_body_too_long(reason)
        subject_resolution = _is_subject_resolution_failure(reason)
        if explicit and body_too_long:
            lines.append(
                f"{tool_id or '写入工具'} 重新写入后仍没有成功，已登记为 {pending_id}。"
            )
        elif explicit and subject_resolution:
            lines.append(
                f"{tool_id or '写入工具'} 重新写入后仍没有成功，已登记为 {pending_id}。"
            )
        elif explicit:
            lines.append(
                f"{tool_id or '写入工具'} 没有成功，已登记为 {pending_id}。"
            )
        else:
            lines.append(
                f"{tool_id or '写入工具'} 本次写入没有成功。"
            )
        if body_too_long:
            lines.extend(_memory_body_too_long_lines(reason))
        elif subject_resolution:
            lines.extend(_subject_resolution_lines(
                pending,
                allow_natural_blocked=not explicit,
            ))
        else:
            lines.append("失败详情请查看 POPUP 中相同 tool_id/call_id 的工具提醒。")
        cancel_available = bool(pending.get("cancel_available"))
        if pending.get("mandatory"):
            lines.append(
                "这个 pending 属于必须结算的写入，不能取消；"
                "请重试、修正参数，或在无法完成时阻塞收束。"
            )
        elif subject_resolution:
            lines.append("身份/主题确认前不能结算这次记忆写入；请请求用户确认或按 NO-GO 阻断收束。")
        elif not explicit:
            lines.append("请先修正后重新调用 memory_write；当前不能收束。")
        elif cancel_available:
            lines.append(
                "下一步要么重试并填写 resolves_pending_id，"
                "要么调用 pending_cancel 明确取消；"
                "在结算前不能自然声称完成，也不能中继。"
            )
        else:
            lines.append(
                "下一步必须重试并填写 resolves_pending_id；"
                "无法完成时自然说明真实阻塞原因；Runtime 会按门禁或事故态派生状态。"
            )
    return "\n".join(lines).strip()


def write_pending_notice_call_ids(pendings):
    call_ids = []
    for pending in pendings or []:
        if not isinstance(pending, dict):
            continue
        raw_items = []
        raw_call_ids = pending.get("call_ids") or []
        if isinstance(raw_call_ids, str):
            raw_call_ids = [raw_call_ids]
        raw_items.extend(raw_call_ids)
        raw_items.append(pending.get("call_id"))
        receipt = pending.get("receipt")
        if isinstance(receipt, dict):
            raw_items.append(receipt.get("call_id"))
            receipt_call_ids = receipt.get("call_ids") or []
            if isinstance(receipt_call_ids, str):
                receipt_call_ids = [receipt_call_ids]
            raw_items.extend(receipt_call_ids)
        for item in raw_items:
            call_id = _clean_text(item)
            if call_id and call_id not in call_ids:
                call_ids.append(call_id)
    return call_ids


def render_open_write_pending_warning(pendings):
    parts = []
    for pending in pendings or []:
        if not isinstance(pending, dict):
            continue
        pending_id = _clean_text(pending.get("pending_id"))
        tool_id = _clean_text(pending.get("target_tool_id")) or "write_tool"
        reason = _clean_text(pending.get("reason")) or "unknown"
        stage = _settlement_stage(pending)
        explicit = stage == "settlement_required" and bool(pending_id)
        subject_resolution = _is_subject_resolution_failure(reason)
        lines = [
            "- kind: write_pending_open" if explicit else "- kind: write_retry_required",
            "  tier: warning",
            "  decision_required: true",
            f"  tool_id: {tool_id}",
            "  next_action: settle_failed_write_pending" if explicit else "  next_action: retry_write_after_fix",
            "  message: |",
        ]
        body_too_long = _is_memory_body_too_long(reason)
        if body_too_long or subject_resolution:
            lines.insert(4, f"  reason: {reason}")
        if explicit:
            lines.insert(5, f"  pending_id: {pending_id}")
            if body_too_long:
                lines.append(
                    f"    {tool_id} 重新写入后仍没有成功，{pending_id} 是这次失败写入提醒 ID。"
                )
            elif subject_resolution:
                lines.append(
                    f"    {tool_id} 重新写入后仍没有成功，{pending_id} 是这次失败写入提醒 ID。"
                )
            else:
                lines.append(
                    f"    {tool_id} 本次写入没有成功，{pending_id} 是这次失败写入提醒 ID。"
                )
        else:
            lines.append(f"    {tool_id} 本次写入没有成功。")
        if body_too_long:
            for line in _memory_body_too_long_lines(reason):
                lines.append(f"    {line}")
        elif subject_resolution:
            for line in _subject_resolution_lines(
                    pending,
                    allow_natural_blocked=not explicit):
                lines.append(f"    {line}")
        else:
            lines.append("    失败详情请查看 POPUP 中相同 tool_id/call_id 的工具提醒。")
        cancel_available = bool(pending.get("cancel_available"))
        if pending.get("mandatory"):
            lines.append("    这是必须结算的写入，不能取消。请改参数或重试；无法完成时自然说明真实阻塞原因。")
        elif explicit and cancel_available:
            lines.append(
                "    下一步只能二选一：带 resolves_pending_id 重试；"
                f"或调用 pending_cancel(pending_id={pending_id}) 放弃。"
            )
        elif explicit:
            lines.append(
                "    下一步必须带 resolves_pending_id 重试；"
                "无法完成时自然说明真实阻塞原因；Runtime 会按门禁或事故态派生状态。"
            )
        elif subject_resolution:
            lines.append("    身份/主题确认前不能结算这次记忆写入；请请求用户确认或按 NO-GO 阻断收束。")
        else:
            lines.append("    请先修正后重新调用 memory_write；当前不能收束。")
        if explicit:
            lines.append("    在这条 pending 结算前不能自然声称完成，也不能中继。")
        elif subject_resolution:
            lines.append(
                "    可以自然回复请求用户确认或说明 NO-GO 阻断；"
                "不得声称写入成功，也不得把失败中继为已完成。"
            )
        else:
            lines.append("    在这次失败写入修正前不能自然声称完成，也不能中继。")
        parts.append("\n".join(lines))
    return "\n".join(parts).strip()


def render_cancelled_write_warning(pendings):
    lines = []
    for pending in pendings or []:
        if not isinstance(pending, dict):
            continue
        pending_id = _clean_text(pending.get("pending_id"))
        if not pending_id:
            continue
        tool_id = _clean_text(pending.get("target_tool_id")) or "write_tool"
        reason = _clean_text(pending.get("reason")) or "unknown"
        lines.extend([
            "- kind: cancelled_write_pending",
            "  tier: warning",
            "  decision_required: true",
            f"  tool_id: {tool_id}",
            f"  pending_id: {pending_id}",
            f"  reason: {reason}",
            "  next_action: avoid_cancelled_write_success_claim",
            "  message: |",
            f"    {pending_id} 对应的 {tool_id} 失败写入已经取消，但没有补写成功。",
            "    取消不是补写；对应记忆不存在，对应挂接或提交事实也不存在。",
            "    reaction_finalize 和最终回复绝不能声称它已写入、已记录、已保存、已沉淀、已提交或已挂接。",
            "    合规说法：这次失败写入已取消，没有补写；对应记忆不存在。",
        ])
    return "\n".join(lines).strip()


def render_write_pending_blocker(blocker):
    blocker = blocker or {}
    reason = _clean_text(blocker.get("reason"))
    pendings = blocker.get("pendings") or []
    if reason == "pending_cancel_result_not_consumed":
        return (
            "失败写入结算门禁：你刚刚调用了 pending_cancel，取消回执已经进入本轮工具事实，"
            "但同一响应内不能马上收束。下一迭代先阅读这条取消事实，确认原写入没有成功、"
            "写入意图已取消，然后再自然回复用户或按需中继。"
        )
    if reason == "mandatory_write_pending_unresolved":
        return (
            "失败写入结算门禁：存在必须落账的写入 pending 尚未解决，不能取消。"
            "请重试、修正参数；确认无法完成时自然说明真实阻塞原因，"
            "但不能把本轮说成已经正常写入。"
        )
    if reason == "write_pending_unresolved":
        retry_required = [
            item for item in pendings
            if isinstance(item, dict)
            and _settlement_stage(item) == "retry_required"
        ]
        if retry_required and len(retry_required) == len(pendings):
            return (
                "失败写入结算门禁：本轮存在失败写入需要重写。"
                "请先修正后重新调用 memory_write，当前不能收束。"
            )
        ids = [
            _clean_text(item.get("pending_id"))
            for item in pendings
            if isinstance(item, dict)
            and _settlement_stage(item) == "settlement_required"
            and _clean_text(item.get("pending_id"))
        ]
        listed = "、".join(ids) if ids else "未结算写入"
        cancellable = [
            item for item in pendings
            if isinstance(item, dict)
            and _settlement_stage(item) == "settlement_required"
            and bool(item.get("cancel_available"))
        ]
        if cancellable:
            return (
                f"失败写入结算门禁：{listed} 还没有结算。"
                "请重试并填写 resolves_pending_id，或调用 pending_cancel 明确取消；"
                "取消成功后必须等下一迭代看到取消工具事实，再自然回复用户或按需中继。"
            )
        return (
            f"失败写入结算门禁：{listed} 还没有结算。"
            "请重试并填写 resolves_pending_id；"
            "无法完成时自然说明真实阻塞原因；Runtime 会按门禁或事故态派生状态。"
        )
    return "失败写入结算门禁：仍有写入 pending 未完成，请先结算再收束。"
