"""Runtime-owned commit path shared by reaction and organ products."""

from engines.reaction_protocol_tool_execution import apply_relation_card_declarations
from logic.alert_mode_settle import apply_alert_mode_settlement_declarations
from logic.chronicle_write import apply_chronicle_write_declarations
from logic.fault_record import apply_fault_record_declarations
from logic.memory_container_tools import (
    apply_memory_container_create_declarations,
    apply_memory_container_write_declarations,
)
from logic.memory_link_update import apply_memory_link_update_declarations
from logic.memory_write import apply_memory_write_declarations
from logic.interaction_meta import active_relation_card, interaction_meta_for_card


ORGAN_PRODUCT_TOOLS = frozenset({
    "memory_write",
    "relation_card_write",
    "memory_link_update",
    "memory_container_create",
    "memory_container_write",
    "chronicle_write",
    "alert_mode_settle",
    "fault_record",
})


def build_protocol_processor_state(services, interaction_meta):
    state = services.sm.load()
    meta = interaction_meta if isinstance(interaction_meta, dict) else {}
    subject = str(meta.get("interaction_object") or "").strip()
    status = str(meta.get("identity_status") or "").strip()
    source = str(meta.get("interaction_source") or "").strip()
    confirmed = (
        [subject]
        if subject and subject != "unknown"
        and status not in {"unknown", "timeout", "unregistered", ""}
        else []
    )
    if source not in {"no_external_input", "system"}:
        state = dict(state or {})
        state["presence"] = {"confirmed_subjects": confirmed}
    return state


class RuntimeProductCommitter:
    def __init__(self, services):
        self.services = services

    def commit(
            self, tool_id, declarations, *, round_num, interaction_meta=None,
            pending_memory_ids=None, visible_focus_id="",
            visible_relation_body_ids=(), chronicle_store=None,
            chronicle_focus=None, memory_heat_boosted_ids=None):
        s = self.services
        tool_id = str(tool_id or "").strip()
        declarations = list(declarations or [])
        if tool_id not in ORGAN_PRODUCT_TOOLS:
            return [self._rejected(tool_id) for _ in declarations or [None]]
        state = build_protocol_processor_state(s, interaction_meta)
        if tool_id == "memory_write":
            return apply_memory_write_declarations(declarations, state, round_num, {
                "memory_store": s.memory_store,
                "memory_index": s.memory_index,
                "memory_heat": s.heat,
                "container_store": s.container_store,
                "relation_store": s.relation_store,
            })
        if tool_id == "memory_link_update":
            return apply_memory_link_update_declarations(declarations, {
                "memory_store": s.memory_store,
                "container_store": s.container_store,
                "relation_store": s.relation_store,
            }, pending_memory_ids=pending_memory_ids or {}, state=state)
        if tool_id == "memory_container_create":
            return apply_memory_container_create_declarations(declarations, {
                "memory_store": s.memory_store,
                "container_store": s.container_store,
                "workbench_store": s.workbench,
                "relation_store": s.relation_store,
                "pending_memory_ids": pending_memory_ids or {},
            }, round_num=round_num, state=state)
        if tool_id == "memory_container_write":
            return apply_memory_container_write_declarations(declarations, {
                "memory_store": s.memory_store,
                "container_store": s.container_store,
                "workbench_store": s.workbench,
                "visible_focus_id": visible_focus_id,
                "relation_store": s.relation_store,
                "pending_memory_ids": pending_memory_ids or {},
            }, round_num=round_num, state=state)
        if tool_id == "chronicle_write":
            return apply_chronicle_write_declarations(declarations, {
                "chronicle_store": chronicle_store,
                "chronicle_focus": chronicle_focus,
            })
        if tool_id == "alert_mode_settle":
            return apply_alert_mode_settlement_declarations(declarations, round_num, {
                "state_store": s.sm,
                "context_store": s.ctx_store,
                "alert_store": s.alert_store,
            }, interaction_meta=interaction_meta or {})
        if tool_id == "fault_record":
            return apply_fault_record_declarations(declarations, round_num, {
                "context_store": s.ctx_store,
                "alert_store": s.alert_store,
            }, interaction_meta=interaction_meta or {})
        receipts = apply_relation_card_declarations(
            declarations,
            interaction_meta or {},
            guard=s.cfg.get_relation_card_write_guard(),
            visible_relation_body_ids=visible_relation_body_ids,
            relation_store_factory=lambda: s.relation_store,
            relation_index_factory=lambda: s.memory_index,
        )
        meta = interaction_meta if isinstance(interaction_meta, dict) else {}
        if str(meta.get("identity_status") or "") == "unregistered":
            submitted = str(meta.get("interaction_object") or "").strip()
            for receipt in receipts:
                if (
                        receipt.get("status") not in {"applied", "degraded"}
                        or not receipt.get("card_id")):
                    continue
                card = active_relation_card(s.relation_store, receipt["card_id"])
                if not card or submitted not in {
                        str(card.get("id") or ""), str(card.get("name") or "")}:
                    continue
                s.sm.set_interaction_anchor(
                    relation_id=card["id"], source="relation_card_created")
                meta.update(interaction_meta_for_card(
                    card, "relation_card_created"))
                break
        return receipts

    def commit_product(
            self, product, *, frame_ref, role_id, sequence, allowed_tools,
            round_num, interaction_meta=None, pending_memory_ids=None,
            visible_focus_id="", visible_relation_body_ids=(),
            chronicle_store=None, chronicle_focus=None,
            memory_heat_boosted_ids=None):
        product = product if isinstance(product, dict) else {}
        tool_id = str(product.get("tool_id") or "").strip()
        frame = frame_ref.as_dict() if hasattr(frame_ref, "as_dict") else dict(frame_ref or {})
        meta = {
            "product_id": f"{frame.get('frame_id', '')}:product:{int(sequence)}",
            "frame_id": frame.get("frame_id", ""),
            "trigger_id": frame.get("trigger_id", ""),
            "role_id": str(role_id or ""),
            "caused_by": frame.get("frame_id", ""),
        }
        if tool_id not in set(allowed_tools or ()):
            return [{**self._rejected(tool_id, "organ_role_product_not_allowed"), **meta}]
        receipts = self.commit(
            tool_id,
            [product.get("arguments") if isinstance(product.get("arguments"), dict) else {}],
            round_num=round_num,
            interaction_meta=interaction_meta,
            pending_memory_ids=pending_memory_ids,
            visible_focus_id=visible_focus_id,
            visible_relation_body_ids=visible_relation_body_ids,
            chronicle_store=chronicle_store,
            chronicle_focus=chronicle_focus,
            memory_heat_boosted_ids=memory_heat_boosted_ids,
        )
        return [{**dict(receipt or {}), **meta} for receipt in receipts]

    @staticmethod
    def _rejected(tool_id, reason="organ_product_tool_not_allowed"):
        return {
            "tool_id": tool_id,
            "tool_family": "protocol_tool",
            "tool_class": "write_tool",
            "status": "rejected",
            "reason": reason,
            "source": "organ_product_committer",
            "protocol_tool_receipt": True,
        }
