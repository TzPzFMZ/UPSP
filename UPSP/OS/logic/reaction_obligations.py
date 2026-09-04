"""Spec202 reaction-step memory/container obligation tracker."""

from copy import deepcopy


WRITABLE_CONTAINER_TYPES = {"DC", "EC", "PRJ"}
FUT_CONTAINER_TYPE = "FUT"
MEMORY_ROUTE_SOFT_PROMPT_LIMIT = 3
MEMORY_SETTLEMENT_STATUSES = {
    "written",
    "covered_by_existing",
    "weight_zero",
    "deferred",
    "failed",
}
READ_SETTLEMENT_STATUSES = {
    "complete",
    "partial_continue",
    "partial_user_wait",
    "not_applicable",
}
CLOSEOUT_DECISIONS = {"finish", "continue", "blocked"}
PENDING_STATUSES = {"none", "resolved", "deferred", "blocked"}
TERMINAL_BLOCKED_OBLIGATION_TYPES = {"periodic_memory_mount_blocked"}


def _text(value):
    return str(value or "").strip()


def _tool_id(receipt):
    return _text((receipt or {}).get("tool_id")).lower()


def _status(receipt):
    return _text((receipt or {}).get("status")).lower()


def _refs(value):
    if value is None:
        return []
    if isinstance(value, str):
        value = value.replace("，", ",").replace("、", ",").replace(";", ",").split(",")
    result = []
    for item in value:
        ref = _text(item)
        if ref and ref not in result:
            result.append(ref)
    return result


def _read_ref(result):
    path = _text(result.get("path")) or "unknown"
    next_start_line = _text(result.get("next_line_start") or result.get("next_start_line"))
    if next_start_line:
        return f"file_read:{path}:{next_start_line}"
    return f"file_read:{path}"


def _container_type(container_id):
    text = _text(container_id)
    if "-" not in text:
        return ""
    return text.split("-", 1)[0].upper()


def _make_obligation(
    obligation_type,
    target_refs,
    *,
    reason="",
    required_refs=None,
    skippable=True,
    anchor_mem_id="",
):
    return {
        "obligation_type": obligation_type,
        "target_refs": _refs(target_refs),
        "required_refs": _refs(required_refs),
        "reason": _text(reason),
        "skippable": bool(skippable),
        "anchor_mem_id": _text(anchor_mem_id),
    }


class ReactionObligationTracker:
    """Tracks Spec202 obligations within one reaction loop only."""

    def __init__(self, memory_reconsolidation_tracker=None,
                 memory_write_rewrite_tracker=None, context_store=None):
        self.pending = []
        self.memory_reconsolidation_tracker = memory_reconsolidation_tracker
        self.memory_write_rewrite_tracker = memory_write_rewrite_tracker
        self.context_store = context_store
        self.memory_write_seen = False
        self.applied_memory_ids = []
        self.unfinished_file_reads = []
        self.blocked_finalize_events = []

    def pending_types(self):
        result = [item["obligation_type"] for item in self.pending]
        if self._cache_compaction_pending():
            result.append("cache_compaction_pending")
        if self._reconsolidation_pending():
            result.append("memory_reconsolidation_pending")
        if self._memory_write_rewrite_pending():
            result.append("memory_write_rewrite_pending")
        return result

    def add_periodic_mount_blocked(self, mem_id, reason=""):
        self._add_obligation(_make_obligation(
            "periodic_memory_mount_blocked",
            [mem_id],
            reason=reason,
            skippable=False,
        ))

    def observe_receipts(self, receipts):
        for receipt in receipts or []:
            if not isinstance(receipt, dict):
                continue
            tool_id = _tool_id(receipt)
            status = _status(receipt)
            if (
                    tool_id == "relation_card_write"
                    and status == "degraded"
                    and _text(receipt.get("card_id"))):
                self._close_type("relation_card_pending")
                continue
            if status != "applied":
                continue
            if tool_id == "memory_write":
                self._observe_memory_write(receipt)
            elif tool_id in {"memory_container_create", "memory_container_write"}:
                self._observe_memory_container_receipt(receipt)
            elif tool_id == "memory_link_update":
                self._observe_memory_link_update(receipt)
            elif tool_id == "relation_card_write":
                self._close_type("relation_card_pending")
            elif tool_id == "guide_submit":
                self._observe_guide_submit(receipt)

    def observe_general_tool_results(self, results):
        for result in results or []:
            if not isinstance(result, dict) or _status(result) != "ok":
                continue
            if _tool_id(result) != "file_read":
                continue
            path = _text(result.get("path"))
            if path:
                self.unfinished_file_reads = [
                    item for item in self.unfinished_file_reads
                    if _text(item.get("path")) != path
                ]
            if (
                    bool(result.get("has_more"))
                    or bool(result.get("truncated"))
                    or bool(result.get("content_truncated"))):
                self.unfinished_file_reads.append({
                    "tool_id": "file_read",
                    "path": path,
                    "start_line": result.get("start_line"),
                    "end_line": result.get("end_line"),
                    "total_lines": result.get("total_lines"),
                    "has_more": bool(result.get("has_more")),
                    "truncated": bool(result.get("truncated")),
                    "content_truncated": bool(result.get("content_truncated")),
                    "next_line_start": (
                        result.get("next_line_start")
                        if result.get("next_line_start") not in (None, "")
                        else result.get("next_start_line")
                    ),
                    "call_id": _text(result.get("call_id")),
                    "read_ref": _read_ref(result),
                })

    def validate_closeout_form(self, closeout_form):
        """Validate flat closeout form and generate the Runtime truth ledger."""
        form = closeout_form if isinstance(closeout_form, dict) else {}
        reasons = []
        corrections = []
        effective_pending = self._effective_pending()
        terminal_blocked_pending = [
            item for item in effective_pending
            if item.get("obligation_type") in TERMINAL_BLOCKED_OBLIGATION_TYPES
        ]
        actionable_unskippable = [
            item for item in effective_pending
            if (
                not item.get("skippable", True)
                and item.get("obligation_type")
                not in TERMINAL_BLOCKED_OBLIGATION_TYPES
            )
        ]

        decision = _text(form.get("closeout_decision")).lower()
        handoff_text = _text(form.get("handoff_text"))
        memory_status_model = _text(form.get("memory_status")).lower()
        read_status_model = _text(form.get("read_status")).lower()
        pending_status_model = _text(form.get("pending_status")).lower()

        if not decision:
            reasons.append("closeout_decision_required")
        elif decision not in CLOSEOUT_DECISIONS:
            reasons.append(f"closeout_decision_invalid:{decision}")
        if memory_status_model and memory_status_model not in (
            MEMORY_SETTLEMENT_STATUSES | {"not_applicable"}
        ):
            reasons.append(f"memory_status_invalid:{memory_status_model}")
        if read_status_model and read_status_model not in READ_SETTLEMENT_STATUSES:
            reasons.append(f"read_status_invalid:{read_status_model}")
        if pending_status_model and pending_status_model not in PENDING_STATUSES:
            reasons.append(f"pending_status_invalid:{pending_status_model}")
        runtime_terminal_blocked = bool(
            terminal_blocked_pending and not actionable_unskippable
        )
        if runtime_terminal_blocked:
            if decision != "blocked":
                corrections.append(
                    f"closeout_decision_corrected:{decision or '<missing>'}->blocked"
                )
            decision = "blocked"
            handoff_text = ""
        elif decision == "continue" and not handoff_text:
            reasons.append("closeout_continue_requires_handoff_text")

        memory_refs = list(self.applied_memory_ids)
        memory_status = "written" if memory_refs else "not_applicable"
        if memory_refs and memory_status_model and memory_status_model != "written":
            memory_status = "written"
            corrections.append(
                f"memory_status_corrected:{memory_status_model or '<missing>'}->written"
            )
        elif not memory_refs and memory_status_model == "written":
            corrections.append("memory_status_ignored:written_without_receipt")

        read_refs = [item["read_ref"] for item in self.unfinished_file_reads]
        if self.unfinished_file_reads:
            if decision == "continue" and handoff_text:
                if read_status_model and read_status_model != "partial_continue":
                    corrections.append(
                        "read_status_corrected:"
                        f"{read_status_model or '<missing>'}->partial_continue"
                    )
                read_status = "partial_continue"
            else:
                read_status = "partial_user_wait"
                if read_status_model == "complete":
                    corrections.append(
                        "read_status_corrected:complete->partial_user_wait")
        elif read_status_model == "partial_continue" and decision != "continue":
            reasons.append("read_partial_continue_requires_continue_decision")
            read_status = "not_applicable"
        else:
            read_status = "not_applicable"

        if actionable_unskippable:
            pending_resolution_result = "blocked"
            for obligation in actionable_unskippable:
                refs = ",".join(obligation.get("target_refs") or [])
                reasons.append(f"{obligation['obligation_type']}_unresolved:{refs}")
        elif terminal_blocked_pending:
            pending_resolution_result = "blocked"
        elif effective_pending:
            pending_resolution_result = "open"
        else:
            pending_resolution_result = "clear"
        if pending_resolution_result == "blocked":
            pending_status = "blocked"
        elif pending_resolution_result == "open":
            pending_status = "deferred"
        else:
            pending_status = "none"

        relay_receipt = {}
        if decision == "continue" and handoff_text:
            relay_receipt = {"handoff_text": handoff_text}

        ledger = {
            "closeout_decision": decision,
            "model_memory_status": memory_status_model,
            "memory_status": memory_status,
            "memory_reason": "",
            "memory_refs": memory_refs,
            "model_read_status": read_status_model,
            "read_status": read_status,
            "read_reason": "",
            "read_refs": read_refs,
            "model_pending_status": pending_status_model,
            "pending_status": pending_status,
            "pending_reason": "",
            "pending_obligations": deepcopy(effective_pending),
            "pending_resolution_result": pending_resolution_result,
            "relay_receipt": relay_receipt,
            "corrections": corrections,
            "validation_reasons": list(reasons),
        }
        return {
            "blocked": bool(reasons),
            "reasons": reasons,
            "resolved_types": [],
            "settlement_ledger": ledger,
        }

    def render_prompt(self):
        if not self.pending:
            return ""
        has_unskippable = any(
            not obligation.get("skippable", True)
            and obligation.get("obligation_type")
            not in TERMINAL_BLOCKED_OBLIGATION_TYPES
            for obligation in self.pending
        )
        has_terminal_blocked = any(
            obligation.get("obligation_type")
            in TERMINAL_BLOCKED_OBLIGATION_TYPES
            for obligation in self.pending
        )
        memory_route_count = sum(
            1 for obligation in self.pending
            if obligation["obligation_type"] == "memory_route_pending"
        )
        lines = [
            "## REMINDER｜提醒",
        ]
        if has_unskippable:
            lines.append(
                "本轮还有必须收束的记忆/容器引导；请处理不可跳过项后再结束反应步。"
            )
        elif has_terminal_blocked:
            lines.append(
                "本轮存在只能由用户侧解除的 Runtime 阻塞；无需重试，"
                "请直接说明阻塞，Runtime 将按 blocked 闭合本轮。"
            )
        elif memory_route_count:
            lines.append(
                "本轮还有可延期的记忆路由引导；必须分别检查 DC、EC、PRJ、FUT。"
                "只有逐项确认均不满足永固触发条件，才可自然语言回复；"
                "Runtime 会把未处理项记为 deferred/open。"
            )
        else:
            lines.append(
                "本轮还有可延期的记忆/容器引导；请逐项按下方说明核验，"
                "Runtime 会把未处理项记为 deferred/open。"
            )
        if memory_route_count >= MEMORY_ROUTE_SOFT_PROMPT_LIMIT:
            lines.append(
                f"同轮已有 {memory_route_count} 条未路由记忆：停止新增 memory_write；"
                "优先只处理已有 pending；分别检查四类触发条件，均不满足时才自然语言回复收束。"
            )
        for obligation in self.pending:
            refs = "、".join(obligation.get("target_refs") or [])
            otype = obligation["obligation_type"]
            if otype == "memory_route_pending":
                line = "- 记忆条目"
                if refs:
                    line += f" {refs}"
                line += self._memory_route_prompt_body()
            elif otype == "container_link_pending":
                line = "- 容器"
                if refs:
                    line += f" {refs}"
                line += " 已写入：需审计相关记忆是否已挂接到该容器；跳过需写明理由。"
            elif otype == "future_jump_pending":
                line = "- 当前一阶容器"
                if refs:
                    line += f" {refs}"
                line += " 已更新：请判断是否影响未来判断；若是，写入 FUT（预测/承诺/待验证）。"
            elif otype == "future_anchor_pending":
                reqs = "、".join(obligation.get("required_refs") or [])
                line = "- FUT 已写入：必须再写一条锚点记忆，并把它同时挂接到 FUT 与前因容器"
                if reqs:
                    line += f" {reqs}"
                line += "。"
            elif otype == "relation_card_pending":
                line = "- 当前交互对象在关系域里不存在：若需要记住对方，请创建新的关系卡；若不需要，请说明理由。"
            elif otype == "periodic_memory_mount_blocked":
                line = (
                    f"- 记忆 {refs} 已完成回忆重整，但自动定期挂载失败；"
                    "本轮按可审计阻塞收束，等待用户取消请求或调整容量后重试。"
                )
            else:
                continue
            lines.append(line)
        return "\n".join(lines)

    def audit_state(self):
        return {
            "memory_write_seen": self.memory_write_seen,
            "applied_memory_ids": list(self.applied_memory_ids),
            "unfinished_file_reads": deepcopy(self.unfinished_file_reads),
            "pending_obligations": deepcopy(self._effective_pending()),
            "memory_reconsolidation": (
                self.memory_reconsolidation_tracker.audit_state()
                if self.memory_reconsolidation_tracker is not None else {}
            ),
            "memory_write_rewrite": (
                self.memory_write_rewrite_tracker.audit_state()
                if self.memory_write_rewrite_tracker is not None else {}
            ),
            "blocked_finalize_events": deepcopy(self.blocked_finalize_events),
        }

    def record_blocked_finalize(self, validation):
        event = {
            "reasons": list((validation or {}).get("reasons") or []),
            "pending_obligations": deepcopy(self._effective_pending()),
        }
        self.blocked_finalize_events.append(event)
        return event

    def _observe_memory_write(self, receipt):
        mem_id = _text(receipt.get("mem_id"))
        if not mem_id:
            return
        self.memory_write_seen = True
        if mem_id not in self.applied_memory_ids:
            self.applied_memory_ids.append(mem_id)

        anchor = self._first_pending("future_anchor_pending")
        if anchor and not anchor.get("anchor_mem_id"):
            anchor["anchor_mem_id"] = mem_id
            return

        self._add_obligation(_make_obligation(
            "memory_route_pending",
            [mem_id],
            reason="memory_write applied; decide DC/EC/PRJ route",
        ))

    def _observe_reconsolidation(self, receipt):
        if _text(receipt.get("action")) != "memory_reconsolidation_settled":
            return
        for backend in receipt.get("backend_receipts") or []:
            if not isinstance(backend, dict):
                continue
            if _text(backend.get("periodic_mount_outcome")) != "mount_blocked":
                continue
            self.add_periodic_mount_blocked(
                _text(backend.get("mem_id")),
                _text(backend.get("periodic_mount_reason")),
            )

    def _observe_guide_submit(self, receipt):
        self._observe_reconsolidation(receipt)
        if _text(receipt.get("action")) != "memory_write_rewrites_settled":
            return
        for backend in receipt.get("backend_receipts") or []:
            if not isinstance(backend, dict):
                continue
            if _tool_id(backend) == "memory_write" and _status(backend) == "applied":
                self._observe_memory_write(backend)

    def _reconsolidation_pending(self):
        tracker = self.memory_reconsolidation_tracker
        return bool(
            tracker is not None
            and callable(getattr(tracker, "has_pending", None))
            and tracker.has_pending()
        )

    def _memory_write_rewrite_pending(self):
        tracker = self.memory_write_rewrite_tracker
        return bool(
            tracker is not None
            and callable(getattr(tracker, "has_pending", None))
            and tracker.has_pending()
        )

    def _effective_pending(self):
        result = deepcopy(self.pending)
        if self._cache_compaction_pending():
            debt = self.context_store.load_cache_compaction_debt()
            result.append(_make_obligation(
                "cache_compaction_pending",
                [str(debt.get("compaction_id") or "")],
                reason="lately pressure requires progressive semantic compaction",
                skippable=False,
            ))
        if self._reconsolidation_pending():
            result.append(_make_obligation(
                "memory_reconsolidation_pending",
                self.memory_reconsolidation_tracker.pending_ids(),
                reason="real recall requires semantic reconsolidation",
                skippable=False,
            ))
        if self._memory_write_rewrite_pending():
            result.append(_make_obligation(
                "memory_write_rewrite_pending",
                self.memory_write_rewrite_tracker.pending_ids(),
                reason="oversized memory body requires rewrite or not_written",
                skippable=False,
            ))
        return result

    def _cache_compaction_pending(self):
        loader = getattr(self.context_store, "load_cache_compaction_debt", None)
        if not callable(loader):
            return False
        debt = loader()
        return bool(debt.get("schema_version") == "cache_compaction_debt.v3")

    def _observe_memory_container_receipt(self, receipt):
        mem_id = _text(receipt.get("mem_id"))
        container_id = _text(receipt.get("container_id"))
        ctype = _text(receipt.get("container_type")).upper() or _container_type(container_id)
        if mem_id:
            self.pending = [
                obligation for obligation in self.pending
                if not (
                    obligation["obligation_type"] == "memory_route_pending"
                    and mem_id in (obligation.get("target_refs") or [])
                )
            ]
        else:
            self._close_type("memory_route_pending")
        if not container_id:
            return
        if ctype == FUT_CONTAINER_TYPE:
            self._close_type("future_jump_pending")
            return
        if ctype in WRITABLE_CONTAINER_TYPES:
            self._add_obligation(_make_obligation(
                "future_jump_pending",
                [container_id],
                reason="DC/EC/PRJ may affect future judgment",
            ))

    def _observe_memory_link_update(self, receipt):
        mem_id = _text(receipt.get("mem_id"))
        linked = _refs(receipt.get("linked_containers")) or _refs(receipt.get("container_refs"))
        if not linked:
            return
        self.pending = [
            obligation for obligation in self.pending
            if not (
                obligation["obligation_type"] == "container_link_pending"
                and any(ref in linked for ref in obligation.get("target_refs") or [])
            )
        ]
        remaining = []
        for obligation in self.pending:
            if obligation["obligation_type"] != "future_anchor_pending":
                remaining.append(obligation)
                continue
            required = obligation.get("required_refs") or []
            anchor_mem_id = obligation.get("anchor_mem_id")
            if anchor_mem_id and mem_id == anchor_mem_id and all(ref in linked for ref in required):
                continue
            remaining.append(obligation)
        self.pending = remaining

    def _memory_route_prompt_body(self):
        return (
            " 已写入。本轮理解若是对已有知识的推进/订正/补充，应挂入 DC 辩证链或 EC 事件链；"
            "若识别到多步任务/专项整理需求，应创建或挂入 PRJ 项目；"
            "若有预测性判断，写入 FUT 启动二段跳。"
            "需要挂接时用 memory_container_create 挂接创建；"
            "已有合适容器时先 container_read；下一帧正文已在 CONTENT 可见后用 memory_container_write 挂接写入；"
            "分别检查 DC、EC、PRJ、FUT 后，只有确实均不满足永固触发条件才可自然语言回复收束；"
            "Runtime 会把该项记为 deferred/open。"
        )

    def _add_obligation(self, obligation):
        key = (
            obligation["obligation_type"],
            tuple(obligation.get("target_refs") or []),
        )
        for existing in self.pending:
            existing_key = (
                existing["obligation_type"],
                tuple(existing.get("target_refs") or []),
            )
            if existing_key == key:
                return
        self.pending.append(obligation)

    def _close_type(self, obligation_type):
        self.pending = [
            obligation for obligation in self.pending
            if obligation["obligation_type"] != obligation_type
        ]

    def _first_pending(self, obligation_type):
        for obligation in self.pending:
            if obligation["obligation_type"] == obligation_type:
                return obligation
        return None
