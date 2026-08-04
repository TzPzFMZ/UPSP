"""Reaction loop tool settlement dispatcher."""

import json

from assembly.context_helpers import active_corpus_ids_from_messages
from engines.reaction_helpers import (
    attach_native_trace_to_receipts,
    merge_mount_requests,
    native_tool_failure_feedbacks,
    record_pending_memory_ids,
    remove_memory_mount_requests,
    settle_receipts_for_next_iteration,
)
from engines.reaction_protocol_tool_execution import (
    apply_corpus_read_requests,
    apply_index_view_requests,
    visible_relation_body_ids_from_mounts,
)
from engines.product_committer import RuntimeProductCommitter
from logic.container_read import apply_container_read_requests
from logic.container_focus import apply_container_focus_declarations
from logic.file_read_window import runtime_file_read_context
from logic.guide_submit import apply_guide_submit
from logic.sandbox_grant import load_sandbox_grant
from logic.memory_annotation import apply_memory_annotation_declarations
from logic.memory_content_read import apply_memory_content_read_requests
from logic.mount_cancel import apply_mount_cancel_requests
from logic.memory_privacy import (
    apply_memory_privacy_declarations,
    apply_memory_privacy_declassify_declarations,
)
from logic.relation_read import apply_relation_read_requests
from logic.relay_intent_pool import settle_relay_intent


PROTOCOL_READ_SUCCESS_STATUSES = {"accepted", "ok", "success", "applied", "guide_loaded"}
PROTOCOL_READ_SIGNATURE_FIELDS = {
    "corpus_read": (
        "tool_id",
        "corpus_id",
    ),
    "index_view": (
        "tool_id",
        "scope",
        "zone",
        "offset",
        "limit",
    ),
    "memory_content_read": (
        "tool_id",
        "mem_id",
        "mount_mode",
        "line_start",
        "line_end",
        "char_start",
        "char_end",
    ),
    "container_read": (
        "tool_id",
        "container_id",
        "target_file",
        "line_start",
        "line_end",
        "char_start",
        "char_end",
    ),
    "relation_read": (
        "tool_id",
        "card_id",
        "subject",
        "summary",
        "summary_mode",
        "body",
        "body_mode",
        "line_start",
        "line_end",
        "char_start",
        "char_end",
    ),
}


def _read_signature(tool_id, payload):
    fields = PROTOCOL_READ_SIGNATURE_FIELDS.get(tool_id, ("tool_id",))
    payload = payload or {}
    data = {}
    for field in fields:
        if field == "tool_id":
            value = tool_id
        else:
            value = payload.get(field)
        if value is None:
            value = ""
        data[field] = value
    return json.dumps(data, ensure_ascii=False, sort_keys=True)


def _duplicate_read_receipt(tool_id, request, prior, pending_signature=""):
    prior = prior or {}
    prior_status = str(prior.get("status") or "").strip()
    satisfied = prior_status in PROTOCOL_READ_SUCCESS_STATUSES
    receipt = {
        "tool_id": tool_id,
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "status": "rejected",
        "source": "protocol_tool_request",
        "reason": (
            "duplicate_protocol_read_pending"
            if pending_signature else
            "duplicate_protocol_read_satisfied"
            if satisfied else "duplicate_protocol_read_failure_repeated"
        ),
        "protocol_tool_receipt": True,
        "protocol_read_signature": _read_signature(tool_id, request),
        "duplicate_of_call_id": prior.get("call_id") or request.get("_duplicate_of_call_id", ""),
        "previous_status": prior_status,
    }
    for key in (
            "mem_id",
            "container_id",
            "target_file",
            "card_id",
            "subject",
            "summary",
            "body",
            "mount_mode",
            "corpus_id",
            "scope",
            "zone",
            "line_start",
            "line_end",
            "char_start",
            "char_end"):
        if request.get(key) not in (None, ""):
            receipt[key] = request.get(key)
    if pending_signature:
        receipt["_pending_duplicate_signature"] = pending_signature
    return receipt


class ReactionToolSettlementDispatcher:
    """Settle reaction-loop tool actions."""

    def __init__(self, runner):
        self.runner = runner

    def _commit(self, tool_id, declarations, **kwargs):
        committer = getattr(self.runner, "product_committer", None)
        if committer is None:
            committer = RuntimeProductCommitter(self.runner.services)
        return committer.commit(tool_id, declarations, **kwargs)

    def _record_receipts(
        self,
        *,
        receipts,
        declarations,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        specific_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        attach_native_trace_to_receipts(receipts, declarations)
        specific_receipts.extend(receipts)
        all_protocol_tool_receipts.extend(receipts)
        settle_receipts_for_next_iteration(
            accumulated_messages,
            receipts,
        )
        return receipts

    def handle_pending_cancel(
        self,
        *,
        iter_accepted_tools,
        iter_pending_cancel_requests,
        write_pending_tracker,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_pending_cancel_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "pending_cancel" in iter_accepted_tools
            and iter_pending_cancel_requests
        ):
            return []
        receipts = [
            write_pending_tracker.cancel_pending(request)
            for request in iter_pending_cancel_requests or []
            if isinstance(request, dict)
        ]
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_pending_cancel_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_pending_cancel_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_relay_intent_settle(
        self,
        *,
        iter_accepted_tools,
        iter_relay_intent_settle_requests,
        round_num,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_relay_intent_settle_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "relay_intent_settle" in iter_accepted_tools
            and iter_relay_intent_settle_requests
        ):
            return []
        receipts = [
            settle_relay_intent(runner.sm, request, round_num=round_num)
            for request in iter_relay_intent_settle_requests or []
            if isinstance(request, dict)
        ]
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_relay_intent_settle_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_relay_intent_settle_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def _filter_duplicate_protocol_reads(self, tool_id, requests, prior_receipts):
        prior_by_signature = {}
        for receipt in prior_receipts or []:
            if not isinstance(receipt, dict):
                continue
            if receipt.get("tool_id") != tool_id:
                continue
            signature = receipt.get("protocol_read_signature")
            if signature:
                prior_by_signature.setdefault(signature, receipt)

        executable = []
        duplicate_receipts = []
        duplicate_requests = []
        current_seen = {}
        for request in requests or []:
            if not isinstance(request, dict):
                executable.append(request)
                continue
            signature = _read_signature(tool_id, request)
            prior = prior_by_signature.get(signature)
            if prior:
                duplicate_receipts.append(_duplicate_read_receipt(tool_id, request, prior))
                duplicate_requests.append(request)
                continue
            previous_current = current_seen.get(signature)
            if previous_current:
                duplicate_request = dict(request)
                duplicate_request["_duplicate_of_call_id"] = previous_current.get("call_id", "")
                duplicate_receipts.append(
                    _duplicate_read_receipt(
                        tool_id,
                        duplicate_request,
                        {},
                        pending_signature=signature,
                    )
                )
                duplicate_requests.append(request)
                continue
            current_seen[signature] = request
            executable.append(request)
        return executable, duplicate_receipts, duplicate_requests

    def _attach_protocol_read_signatures(self, tool_id, receipts, requests):
        for receipt, request in zip(receipts or [], requests or []):
            if isinstance(receipt, dict) and isinstance(request, dict):
                receipt["protocol_read_signature"] = _read_signature(tool_id, request)
        return receipts

    def _resolve_pending_duplicate_read_receipts(self, duplicate_receipts, receipts):
        first_by_signature = {}
        for receipt in receipts or []:
            if not isinstance(receipt, dict):
                continue
            signature = receipt.get("protocol_read_signature")
            if signature:
                first_by_signature.setdefault(signature, receipt)
        for duplicate in duplicate_receipts or []:
            if not isinstance(duplicate, dict):
                continue
            pending_signature = duplicate.pop("_pending_duplicate_signature", "")
            if not pending_signature:
                continue
            prior = first_by_signature.get(pending_signature) or {}
            prior_status = str(prior.get("status") or "").strip()
            satisfied = prior_status in PROTOCOL_READ_SUCCESS_STATUSES
            duplicate["reason"] = (
                "duplicate_protocol_read_satisfied"
                if satisfied else "duplicate_protocol_read_failure_repeated"
            )
            duplicate["previous_status"] = prior_status
            if prior.get("call_id"):
                duplicate["duplicate_of_call_id"] = prior.get("call_id")

    def _finalize_protocol_read_receipts(
            self,
            tool_id,
            receipts,
            executable_requests,
            duplicate_receipts):
        runner = self.runner
        self._attach_protocol_read_signatures(tool_id, receipts, executable_requests)
        attach_native_trace_to_receipts(receipts, executable_requests)
        self._resolve_pending_duplicate_read_receipts(duplicate_receipts, receipts)
        return list(receipts or []) + list(duplicate_receipts or [])

    def _container_focus_open_duplicate_receipt(self, request, prior):
        return {
            "tool_id": "container_focus",
            "tool_family": "protocol_tool",
            "tool_class": "focus_tool",
            "source": "provider_native_container_focus",
            "protocol_tool_receipt": True,
            "status": "rejected",
            "reason": "duplicate_container_focus_satisfied",
            "previous_status": str(prior.get("status") or ""),
            "duplicate_of_call_id": prior.get("call_id") or "",
            "action": str(request.get("action") or "").lower(),
            "container_id": request.get("container_id") or prior.get("container_id") or "",
            "container_type": prior.get("container_type") or "",
            "target_file": request.get("target_file") or "",
        }

    def _filter_duplicate_container_focus_opens(self, requests, prior_receipts):
        successful_opens = {}
        for receipt in prior_receipts or []:
            if not isinstance(receipt, dict):
                continue
            if receipt.get("tool_id") != "container_focus":
                continue
            if receipt.get("status") != "applied":
                continue
            if str(receipt.get("action") or "").lower() != "open":
                continue
            container_id = str(receipt.get("container_id") or "").strip()
            if container_id:
                successful_opens.setdefault(container_id, receipt)

        executable = []
        duplicate_receipts = []
        duplicate_requests = []
        for request in requests or []:
            if not isinstance(request, dict):
                executable.append(request)
                continue
            action = str(request.get("action") or "").lower()
            container_id = str(request.get("container_id") or "").strip()
            prior = successful_opens.get(container_id)
            if action == "open" and container_id and prior:
                duplicate_receipts.append(
                    self._container_focus_open_duplicate_receipt(request, prior)
                )
                duplicate_requests.append(request)
                continue
            executable.append(request)
        return executable, duplicate_receipts, duplicate_requests

    def handle_general_tool_results(
        self,
        *,
        iter_general_tool_requests,
        active_general_tool_guides,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_general_tool_results,
        iter_native_feedbacks,
        round_num,
        iteration,
        interaction_meta,
    ):
        runner = self.runner
        runtime_context = runtime_file_read_context(
            runner.sm,
            getattr(runner, "ctx_store", None),
            round_num,
        )
        runtime_context.update({
            "round_num": round_num,
            "iteration": iteration,
            "frame_id": f"R{int(round_num):06d}:reaction:{int(iteration)}",
            "execution_permission_level": getattr(
                runner, "execution_permission_level", "guarded"),
        })
        iter_general_tool_results = runner.general_tool_dispatcher.handle_requests(
            iter_general_tool_requests,
            active_general_tool_guides,
            prior_results=all_general_tool_results,
            runtime_context=runtime_context,
        )
        all_general_tool_results.extend(iter_general_tool_results)
        iter_native_feedbacks.extend(
            native_tool_failure_feedbacks(iter_general_tool_results))
        runner._write_general_tool_results(
            iter_general_tool_results,
            round_num,
            iteration,
            interaction_meta or {},
        )
        return iter_general_tool_results

    def handle_tool_summaries(
        self,
        *,
        parsed_reaction,
        all_tool_summaries,
        round_num,
        iteration,
        interaction_meta,
    ):
        runner = self.runner
        iter_tool_summaries = parsed_reaction.get("tool_summaries", [])
        all_tool_summaries.extend(iter_tool_summaries)
        runner._write_reaction_tool_summaries(
            iter_tool_summaries,
            round_num,
            iteration,
            interaction_meta or {},
        )
        return iter_tool_summaries

    def handle_memory_write(
        self,
        *,
        iter_accepted_tools,
        iter_memory_write_declarations,
        interaction_meta,
        round_num,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_write_receipts,
        all_protocol_tool_receipts,
        pending_memory_ids,
        hidden_stm_memory_ids,
        boosted_memory_ids,
        mount_ids,
    ):
        runner = self.runner
        if not (
            "memory_write" in iter_accepted_tools
            and iter_memory_write_declarations
        ):
            return mount_ids

        memory_write_receipts = self._commit(
            "memory_write",
            iter_memory_write_declarations,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta,
        )
        attach_native_trace_to_receipts(
            memory_write_receipts,
            iter_memory_write_declarations,
        )
        all_memory_write_receipts.extend(memory_write_receipts)
        all_protocol_tool_receipts.extend(memory_write_receipts)
        record_pending_memory_ids(
            pending_memory_ids,
            memory_write_receipts,
        )
        memory_write_mounts = []
        for receipt in memory_write_receipts:
            mem_id = str(receipt.get("mem_id") or "").strip()
            if receipt.get("status") == "applied" and mem_id:
                hidden_stm_memory_ids.add(mem_id)
                memory_write_mounts.append({
                    "type": "memory",
                    "ids": mem_id,
                    "mode": "temporary",
                    "source": "memory_write",
                })
                runner._boost_mounted_memory_once(
                    mem_id,
                    round_num,
                    boosted_memory_ids,
                )
        if memory_write_mounts:
            mount_ids = merge_mount_requests(
                mount_ids,
                memory_write_mounts,
            )
        settle_receipts_for_next_iteration(
            accumulated_messages,
            memory_write_receipts,
        )
        return mount_ids

    def handle_memory_link_update(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_memory_link_update_declarations,
        interaction_meta,
        pending_memory_ids,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_link_update_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "memory_link_update" in iter_accepted_tools
            and iter_memory_link_update_declarations
        ):
            return []
        receipts = self._commit(
            "memory_link_update",
            iter_memory_link_update_declarations,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta,
            pending_memory_ids=pending_memory_ids,
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_memory_link_update_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_memory_link_update_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_guide_submit(
        self,
        *,
        iter_accepted_tools,
        iter_guide_submit_requests,
        current_general_tool_requests=None,
        prior_general_tool_results=None,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_guide_submit_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "guide_submit" in iter_accepted_tools
            and iter_guide_submit_requests
        ):
            return []
        evidence_context = {
            "current_general_tool_requests": list(current_general_tool_requests or []),
            "prior_general_tool_results": list(prior_general_tool_results or []),
            "prior_protocol_tool_receipts": list(all_protocol_tool_receipts or []),
            "active_corpus_ids": active_corpus_ids_from_messages(accumulated_messages),
            "round_num": runner.sm.get_total_round(),
            "round_type": getattr(runner, "_current_round_type", ""),
            "state_store": runner.sm,
            "context_store": runner.ctx_store,
            "alert_store": runner.alert_store,
            "chronicle_store": getattr(runner, "chronicle_store", None),
            "chronicle_focus": getattr(runner, "chronicle_focus", None),
            "workbench_store": runner.workbench,
            "interaction_meta": getattr(runner, "_current_interaction_meta", {}),
        }
        sandbox_grant = load_sandbox_grant()
        if sandbox_grant:
            evidence_context["task_phase"] = sandbox_grant.get("phase")
            evidence_context["task_root"] = sandbox_grant.get("task_root")
            evidence_context["artifact_roots"] = list(
                sandbox_grant.get("write_paths") or [])
        receipts = [
            apply_guide_submit(
                runner.workbench,
                request,
                evidence_context=evidence_context,
            )
            for request in iter_guide_submit_requests or []
            if isinstance(request, dict)
        ]
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_guide_submit_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_guide_submit_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_memory_container_create(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_memory_container_create_declarations,
        interaction_meta,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_container_create_receipts,
        all_protocol_tool_receipts,
        all_created_containers,
    ):
        runner = self.runner
        if not (
            "memory_container_create" in iter_accepted_tools
            and iter_memory_container_create_declarations
        ):
            return []
        receipts = self._commit(
            "memory_container_create",
            iter_memory_container_create_declarations,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta,
        )
        applied_ids = [
            receipt.get("container_id")
            for receipt in receipts
            if receipt.get("status") == "applied" and receipt.get("container_id")
        ]
        all_created_containers.extend(applied_ids)
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_memory_container_create_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_memory_container_create_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_memory_container_write(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_memory_container_write_declarations,
        interaction_meta,
        visible_focus_id,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_container_write_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "memory_container_write" in iter_accepted_tools
            and iter_memory_container_write_declarations
        ):
            return []
        receipts = self._commit(
            "memory_container_write",
            iter_memory_container_write_declarations,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta,
            visible_focus_id=visible_focus_id,
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_memory_container_write_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_memory_container_write_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_index_view(
        self,
        *,
        active_protocol_tool_guides,
        iter_index_view_requests,
        current_state,
        round_type,
        mount_ids_current,
        interaction_meta,
        hidden_stm_memory_ids,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_index_view_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not iter_index_view_requests:
            return []
        (
            executable_requests,
            duplicate_receipts,
            duplicate_requests,
        ) = self._filter_duplicate_protocol_reads(
            "index_view",
            iter_index_view_requests,
            all_index_view_receipts,
        )
        receipts = apply_index_view_requests(
            runner.assembler,
            executable_requests,
            current_state,
            round_type,
            mount_ids_current,
            interaction_meta or {},
            hidden_stm_memory_ids,
        )
        receipts = self._finalize_protocol_read_receipts(
            "index_view",
            receipts,
            executable_requests,
            duplicate_receipts,
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=list(executable_requests or []) + duplicate_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_index_view_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_corpus_read(
        self,
        *,
        iter_corpus_read_requests,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_corpus_read_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not iter_corpus_read_requests:
            return []
        executable_requests = list(iter_corpus_read_requests or [])
        receipts = apply_corpus_read_requests(runner.assembler, executable_requests)
        receipts = self._finalize_protocol_read_receipts(
            "corpus_read",
            receipts,
            executable_requests,
            [],
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=executable_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_corpus_read_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_relation_read(
        self,
        *,
        active_protocol_tool_guides,
        iter_relation_read_requests,
        mount_ids,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_relation_read_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not iter_relation_read_requests:
            return mount_ids
        (
            executable_requests,
            duplicate_receipts,
            duplicate_requests,
        ) = self._filter_duplicate_protocol_reads(
            "relation_read",
            iter_relation_read_requests,
            all_relation_read_receipts,
        )
        receipts, relation_mounts = apply_relation_read_requests(
            executable_requests,
            {"relation_store": runner.relation_store},
        )
        receipts = self._finalize_protocol_read_receipts(
            "relation_read",
            receipts,
            executable_requests,
            duplicate_receipts,
        )
        self._record_receipts(
            receipts=receipts,
            declarations=list(executable_requests or []) + duplicate_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_relation_read_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        return merge_mount_requests(mount_ids, relation_mounts)

    def handle_container_focus(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_container_focus_declarations,
        iter_native_tool_call_envelopes,
        accumulated_messages,
        all_container_focus_receipts,
        all_protocol_tool_receipts,
        all_created_containers,
    ):
        runner = self.runner
        if not (
            "container_focus" in iter_accepted_tools
            and iter_container_focus_declarations
        ):
            return []

        (
            executable_declarations,
            duplicate_receipts,
            duplicate_declarations,
        ) = self._filter_duplicate_container_focus_opens(
            iter_container_focus_declarations,
            all_container_focus_receipts,
        )
        container_focus_receipts = apply_container_focus_declarations(
            executable_declarations,
            {
                "container_store": runner.container_store,
                "workbench_store": runner.workbench,
            },
            round_num=runner.sm.get_total_round(),
        )
        container_focus_receipts = list(container_focus_receipts or []) + list(
            duplicate_receipts or []
        )
        traced_declarations = list(executable_declarations or []) + list(
            duplicate_declarations or []
        )
        attach_native_trace_to_receipts(
            container_focus_receipts,
            traced_declarations,
        )
        all_container_focus_receipts.extend(container_focus_receipts)
        all_protocol_tool_receipts.extend(container_focus_receipts)
        all_created_containers.extend([
            receipt.get("container_id")
            for receipt in container_focus_receipts
            if (
                receipt.get("status") == "applied"
                and receipt.get("action") == "create"
                and receipt.get("container_id")
            )
        ])
        settle_receipts_for_next_iteration(
            accumulated_messages,
            container_focus_receipts,
        )
        return container_focus_receipts

    def handle_memory_content_read(
        self,
        *,
        active_protocol_tool_guides,
        iter_memory_content_read_requests,
        interaction_meta,
        iter_native_tool_call_envelopes,
        accumulated_messages,
        all_memory_content_read_receipts,
        all_protocol_tool_receipts,
        hidden_stm_memory_ids,
        boosted_memory_ids,
        round_num,
        mount_ids,
    ):
        runner = self.runner
        if not iter_memory_content_read_requests:
            return mount_ids

        (
            executable_requests,
            duplicate_receipts,
            duplicate_requests,
        ) = self._filter_duplicate_protocol_reads(
            "memory_content_read",
            iter_memory_content_read_requests,
            all_memory_content_read_receipts,
        )
        memory_content_read_receipts = apply_memory_content_read_requests(
            executable_requests,
            runner._build_protocol_processor_state(interaction_meta),
            {
                "memory_store": runner.memory_store,
                "relation_store": runner.relation_store,
            },
        )
        (
            memory_content_read_receipts,
            memory_content_mounts,
            memory_content_unmounts,
        ) = memory_content_read_receipts
        memory_content_read_receipts = self._finalize_protocol_read_receipts(
            "memory_content_read",
            memory_content_read_receipts,
            executable_requests,
            duplicate_receipts,
        )
        if memory_content_unmounts:
            mount_ids = remove_memory_mount_requests(
                mount_ids,
                memory_content_unmounts,
            )
            hidden_stm_memory_ids.difference_update(memory_content_unmounts)
        if memory_content_mounts:
            mount_ids = merge_mount_requests(
                mount_ids,
                memory_content_mounts,
            )
            for receipt in memory_content_read_receipts:
                if (
                    receipt.get("status") == "accepted"
                    and receipt.get("mount_mode") != "none"
                    and str(receipt.get("memory_layer") or "STM").strip() == "STM"
                ):
                    runner._boost_mounted_memory_once(
                        receipt.get("mem_id"),
                        round_num,
                        boosted_memory_ids,
                    )
        attach_native_trace_to_receipts(
            memory_content_read_receipts,
            list(executable_requests or []) + duplicate_requests,
        )
        all_memory_content_read_receipts.extend(memory_content_read_receipts)
        all_protocol_tool_receipts.extend(memory_content_read_receipts)
        settle_receipts_for_next_iteration(
            accumulated_messages,
            memory_content_read_receipts,
        )
        return mount_ids

    def handle_container_read(
        self,
        *,
        active_protocol_tool_guides,
        iter_container_read_requests,
        mount_ids,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_container_read_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not iter_container_read_requests:
            return mount_ids
        (
            executable_requests,
            duplicate_receipts,
            duplicate_requests,
        ) = self._filter_duplicate_protocol_reads(
            "container_read",
            iter_container_read_requests,
            all_container_read_receipts,
        )
        receipts, container_mounts = apply_container_read_requests(
            executable_requests,
            {"container_store": runner.container_store},
        )
        receipts = self._finalize_protocol_read_receipts(
            "container_read",
            receipts,
            executable_requests,
            duplicate_receipts,
        )
        self._record_receipts(
            receipts=receipts,
            declarations=list(executable_requests or []) + duplicate_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_container_read_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
        if container_mounts:
            mount_ids = merge_mount_requests(mount_ids, container_mounts)
        return mount_ids

    def handle_mount_cancel(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_mount_cancel_requests,
        mount_ids,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_mount_cancel_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "mount_cancel" in iter_accepted_tools
            and iter_mount_cancel_requests
        ):
            return mount_ids
        receipts, updated_mount_ids = apply_mount_cancel_requests(
            iter_mount_cancel_requests,
            {
                "workbench_store": runner.workbench,
                "container_store": runner.container_store,
                "relation_store": runner.relation_store,
            },
            mount_ids=mount_ids,
        )
        attach_native_trace_to_receipts(
            receipts,
            iter_mount_cancel_requests,
        )
        all_mount_cancel_receipts.extend(receipts)
        all_protocol_tool_receipts.extend(receipts)
        settle_receipts_for_next_iteration(
            accumulated_messages,
            receipts,
        )
        return updated_mount_ids

    def handle_memory_privacy_mark(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_memory_privacy_declarations,
        interaction_meta,
        pending_memory_ids,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_privacy_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "memory_privacy_mark" in iter_accepted_tools
            and iter_memory_privacy_declarations
        ):
            return []
        receipts = apply_memory_privacy_declarations(
            iter_memory_privacy_declarations,
            runner._build_protocol_processor_state(interaction_meta),
            {
                "memory_store": runner.memory_store,
                "relation_store": runner.relation_store,
            },
            pending_memory_ids=pending_memory_ids,
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_memory_privacy_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_memory_privacy_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_memory_privacy_declassify(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_memory_privacy_declassify_declarations,
        interaction_meta,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_privacy_declassify_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "memory_privacy_declassify" in iter_accepted_tools
            and iter_memory_privacy_declassify_declarations
        ):
            return []
        receipts = apply_memory_privacy_declassify_declarations(
            iter_memory_privacy_declassify_declarations,
            runner._build_protocol_processor_state(interaction_meta),
            {
                "memory_store": runner.memory_store,
                "relation_store": runner.relation_store,
            },
            config=runner.cfg.get_memory_privacy_declassify_config(),
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_memory_privacy_declassify_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_memory_privacy_declassify_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_memory_annotation_update(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_memory_annotation_declarations,
        interaction_meta,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_annotation_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "memory_annotation_update" in iter_accepted_tools
            and iter_memory_annotation_declarations
        ):
            return []
        receipts = apply_memory_annotation_declarations(
            iter_memory_annotation_declarations,
            {
                "memory_store": runner.memory_store,
                "relation_store": runner.relation_store,
            },
            state=runner._build_protocol_processor_state(interaction_meta),
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_memory_annotation_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_memory_annotation_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_memory_recall_complete(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_memory_recall_completion_requests,
        interaction_meta,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_memory_recall_completion_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "memory_recall_complete" in iter_accepted_tools
            and iter_memory_recall_completion_requests
        ):
            return []
        receipts = self._commit(
            "memory_recall_complete",
            iter_memory_recall_completion_requests,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta,
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_memory_recall_completion_requests,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_memory_recall_completion_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_fault_record(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_fault_record_declarations,
        interaction_meta,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_fault_record_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "fault_record" in iter_accepted_tools
            and iter_fault_record_declarations
        ):
            return []
        receipts = self._commit(
            "fault_record",
            iter_fault_record_declarations,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta or {},
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_fault_record_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_fault_record_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_chronicle_write(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_chronicle_write_declarations,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_chronicle_write_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "chronicle_write" in iter_accepted_tools
            and iter_chronicle_write_declarations
        ):
            return []
        receipts = self._commit(
            "chronicle_write",
            iter_chronicle_write_declarations,
            round_num=runner.sm.get_total_round(),
            chronicle_store=getattr(runner, "chronicle_store", None),
            chronicle_focus=getattr(runner, "chronicle_focus", None),
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_chronicle_write_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_chronicle_write_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_alert_mode_settle(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_alert_mode_settle_declarations,
        interaction_meta,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_alert_mode_settle_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "alert_mode_settle" in iter_accepted_tools
            and iter_alert_mode_settle_declarations
        ):
            return []
        receipts = self._commit(
            "alert_mode_settle",
            iter_alert_mode_settle_declarations,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta or {},
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_alert_mode_settle_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_alert_mode_settle_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def record_alert_auto_defer(
        self,
        *,
        alert_type,
        interaction_meta,
        accumulated_messages,
        all_alert_mode_settle_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        declaration = {
            "alert_type": alert_type,
            "status": "deferred",
            "summary": (
                f"{alert_type} 紧急处理超过 10 次工具动作仍未结算，Runtime 自动搁置 1 小时。"
            ),
            "clear_flags": [],
            "fault_refs": [],
            "next_attention": "1 小时后由心跳重新检查该紧急项。",
            "reason": "emergency_attempt_budget_exceeded",
        }
        receipts = self._commit(
            "alert_mode_settle",
            [declaration],
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta or {},
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=[declaration],
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=[],
            specific_receipts=all_alert_mode_settle_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )

    def handle_relation_card_write(
        self,
        *,
        iter_accepted_tools,
        active_protocol_tool_guides,
        iter_relation_declarations,
        interaction_meta,
        mount_ids_current,
        accumulated_messages,
        iter_native_tool_call_envelopes,
        all_relation_card_receipts,
        all_protocol_tool_receipts,
    ):
        runner = self.runner
        if not (
            "relation_card_write" in iter_accepted_tools
            and iter_relation_declarations
        ):
            return []
        receipts = self._commit(
            "relation_card_write",
            iter_relation_declarations,
            round_num=runner.sm.get_total_round(),
            interaction_meta=interaction_meta or {},
            visible_relation_body_ids=visible_relation_body_ids_from_mounts(
                mount_ids_current),
        )
        return self._record_receipts(
            receipts=receipts,
            declarations=iter_relation_declarations,
            accumulated_messages=accumulated_messages,
            iter_native_tool_call_envelopes=iter_native_tool_call_envelopes,
            specific_receipts=all_relation_card_receipts,
            all_protocol_tool_receipts=all_protocol_tool_receipts,
        )
