def test_spec650_task_ledger_accepts_prior_successful_protocol_receipts(tmp_path):
    from types import SimpleNamespace

    from data.workbench import WorkbenchStore
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
    from logic.evidence_refs import evidence_handle_for_result
    from logic.task_guide import (
        _feedback_known_evidence_items,
        _known_task_evidence_refs,
        materialize_initial_task_guide,
    )

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(store, {
        "task_title": "协议工具验收",
        "task_goal": "用真实协议回执验收记忆写入",
        "items": [{"item_id": "memory", "description": "写入记忆"}],
        "acceptance": [{
            "acceptance_id": "receipts",
            "description": "记忆写入有成功回执",
            "item_refs": ["memory"],
        }],
    })
    successful = {
        "tool_id": "memory_write",
        "status": "applied",
        "call_id": "call_memory_write",
        "mem_id": "MEM-SPEC650",
    }
    protocol_receipts = [
        successful,
        {
            "tool_id": "memory_write",
            "status": "rejected",
            "call_id": "call_rejected_memory",
        },
        {
            "tool_id": "guide_submit",
            "status": "accepted",
            "call_id": "call_prior_guide_submit",
        },
    ]
    runner = SimpleNamespace(
        workbench=store,
        sm=SimpleNamespace(get_total_round=lambda: 650),
        ctx_store=None,
        alert_store=None,
        _current_round_type="interactive",
        _current_interaction_meta={},
        _attach_native_trace_to_receipts=lambda receipts, declarations: receipts,
        _settle_receipts_for_next_iteration=lambda messages, receipts: None,
    )
    evidence_ref = evidence_handle_for_result(successful)
    receipt = ReactionToolSettlementDispatcher(runner).handle_guide_submit(
        iter_accepted_tools={"guide_submit"},
        iter_guide_submit_requests=[{
            "guide_id": f"task:{task_id}",
            "submissions": [{
                "item_id": "task_progress",
                "option_id": "update_task_status",
                "fields": {
                    "items": {
                        "memory": {"status": "done", "evidence_refs": [evidence_ref]},
                    },
                    "acceptance": {
                        "receipts": {"status": "passed", "evidence_refs": [evidence_ref]}
                    },
                },
            }],
        }],
        current_general_tool_requests=[],
        prior_general_tool_results=[],
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_guide_submit_receipts=[],
        all_protocol_tool_receipts=protocol_receipts,
    )[0]

    assert receipt["status"] == "accepted"
    known = _known_task_evidence_refs({
        "prior_protocol_tool_receipts": protocol_receipts,
    })
    assert evidence_ref in known
    assert "call_rejected_memory" not in known
    assert "call_prior_guide_submit" not in known
    visible = _feedback_known_evidence_items(
        {"prior_protocol_tool_receipts": protocol_receipts},
        known,
    )
    assert any(item["tool_id"] == "memory_write" for item in visible)
    assert any("MEM-SPEC650" in item["summary"] for item in visible)


def test_spec662_missing_evidence_feedback_lists_protocol_receipt_refs(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_helpers import format_protocol_tool_fact
    from logic.evidence_refs import evidence_handle_for_result
    from logic.task_guide import (
        apply_task_status_update,
        materialize_initial_task_guide,
    )

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(store, {
        "task_title": "跨轮技能验收",
        "task_goal": "用真实协议回执结清任务",
        "items": [
            {"item_id": "item_01", "description": "写入记忆"},
            {"item_id": "item_02", "description": "创建技能卡"},
            {"item_id": "item_03", "description": "读回技能卡"},
            {"item_id": "item_04", "description": "结算中继"},
        ],
        "acceptance": [{
            "acceptance_id": "acc_01",
            "description": "全部协议动作有真实证据",
            "item_refs": ["item_01", "item_02", "item_03", "item_04"],
        }],
    })
    successful = [
        {
            "tool_id": "memory_write",
            "status": "applied",
            "call_id": "call_memory",
            "mem_id": "MEM-SPEC662",
        },
        {
            "tool_id": "memory_container_create",
            "status": "applied",
            "call_id": "call_container",
            "container_id": "SKL-procedures-spec662",
        },
        {
            "tool_id": "container_read",
            "status": "accepted",
            "call_id": "call_readback",
            "container_id": "SKL-procedures-spec662",
        },
        {
            "tool_id": "relay_intent_settle",
            "status": "applied",
            "call_id": "call_relay",
            "relay_intent_id": "RLY-R000662-N001",
        },
    ]
    evidence_context = {
        "prior_protocol_tool_receipts": successful + [
            {
                "tool_id": "memory_write",
                "status": "rejected",
                "call_id": "call_failed",
            },
            {
                "tool_id": "guide_submit",
                "status": "accepted",
                "call_id": "call_guide",
            },
        ],
    }
    empty_evidence_fields = {
        "items": {
            item_id: {"status": "done", "evidence_refs": []}
            for item_id in ("item_01", "item_02", "item_03", "item_04")
        },
        "acceptance": {
            "acc_01": {"status": "passed", "evidence_refs": []},
        },
    }

    rejected = apply_task_status_update(
        store,
        task_id,
        empty_evidence_fields,
        evidence_context=evidence_context,
    )

    assert rejected["status"] == "rejected"
    assert rejected["reason"] == "task_completion_evidence_required"
    assert rejected["details"]["missing_evidence_refs"] == [
        "items:item_01",
        "items:item_02",
        "items:item_03",
        "items:item_04",
        "acceptance:acc_01",
    ]
    expected_refs = {
        evidence_handle_for_result(result)
        for result in successful
    }
    for result in successful:
        assert (
            f"证据引用：{evidence_handle_for_result(result)}。"
            in format_protocol_tool_fact(result)
        )
    assert "证据引用：" not in format_protocol_tool_fact(
        evidence_context["prior_protocol_tool_receipts"][-2]
    )
    assert "证据引用：" not in format_protocol_tool_fact(
        evidence_context["prior_protocol_tool_receipts"][-1]
    )
    assert expected_refs <= set(rejected["details"]["known_evidence_refs"])
    visible = rejected["details"]["known_evidence_items"]
    assert {item["tool_id"] for item in visible} == {
        "memory_write",
        "memory_container_create",
        "container_read",
        "relay_intent_settle",
    }
    assert "call_failed" not in str(rejected["details"])
    assert "call_guide" not in str(rejected["details"])
    assert "不要猜造 EV-*" in rejected["details"]["hint"]

    usable_ref = visible[0]["ref"]
    accepted = apply_task_status_update(
        store,
        task_id,
        {
            "items": {
                item_id: {"status": "done", "evidence_refs": [usable_ref]}
                for item_id in ("item_01", "item_02", "item_03", "item_04")
            },
            "acceptance": {
                "acc_01": {"status": "passed", "evidence_refs": [usable_ref]},
            },
        },
        evidence_context=evidence_context,
    )
    assert accepted["status"] == "accepted"


def test_spec663_task_ledger_accepts_visible_prior_round_tool_fact(tmp_path):
    from types import SimpleNamespace

    from data.workbench import WorkbenchStore
    from logic.evidence_refs import evidence_handle_for_result
    from logic.task_guide import (
        _known_task_evidence_refs,
        apply_task_status_update,
        materialize_initial_task_guide,
    )

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(store, {
        "task_title": "跨轮来源验收",
        "task_goal": "用上一轮仍在上下文中的真实读取证据结清任务",
        "items": [{"item_id": "source", "description": "完整读取来源"}],
        "acceptance": [{
            "acceptance_id": "readback",
            "description": "来源读取有真实证据",
            "item_refs": ["source"],
        }],
    })
    read_result = {
        "tool_id": "file_read",
        "status": "ok",
        "call_id": "call_prior_round_read",
        "path": str(tmp_path / "source.md"),
    }
    evidence_ref = evidence_handle_for_result(read_result)
    context_store = SimpleNamespace(
        get_lately_entries=lambda: [],
        get_now_entries=lambda: [
            {
                "kind": "tool_fact",
                "content": f"证据引用：{evidence_ref}。",
                "tool_result": read_result,
            },
            {
                "kind": "assistant_reply",
                "content": "自写证据引用：EV-SELFWRITTEN。",
            },
        ],
    )
    evidence_context = {
        "prior_general_tool_results": [],
        "prior_protocol_tool_receipts": [],
        "context_store": context_store,
    }

    known = _known_task_evidence_refs(evidence_context)
    assert evidence_ref in known
    assert "EV-SELFWRITTEN" not in known

    receipt = apply_task_status_update(
        store,
        task_id,
        {
            "items": {
                "source": {"status": "done", "evidence_refs": [evidence_ref]},
            },
            "acceptance": {
                "readback": {"status": "passed", "evidence_refs": [evidence_ref]},
            },
        },
        evidence_context=evidence_context,
    )
    assert receipt["status"] == "accepted"


def test_spec272_runtime_receipts_attach_refs_without_model_self_certification():
    from engines.reaction_helpers import enrich_reaction_finalize_settlement_refs

    parsed = {
        "closeout_form": {
            "closeout_decision": "finish",
            "memory_status": "written",
            "read_status": "complete",
        },
    }

    enriched = enrich_reaction_finalize_settlement_refs(
        parsed,
        memory_write_receipts=[{
            "tool_id": "memory_write",
            "status": "applied",
            "mem_id": "MEM-REAL0001",
        }],
        general_tool_results=[{
            "tool_id": "file_read",
            "status": "ok",
            "path": "D:/book/theory.md",
            "start_line": 1,
            "end_line": 120,
        }],
        container_receipts=[{
            "tool_id": "memory_container_write",
            "status": "applied",
            "container_id": "DC-REAL",
            "mem_id": "MEM-REAL0001",
            "target_file": "open.md",
        }],
    )

    assert "memory_settlement" not in enriched
    assert "read_settlement" not in enriched
    assert enriched["runtime_settlement_refs"]["memory"] == ["MEM-REAL0001"]
    assert enriched["runtime_settlement_refs"]["read"] == ["D:/book/theory.md:1-120"]
    assert enriched["runtime_settlement_refs"]["containers"] == ["DC-REAL"]


class _Spec566NoPrivateApplyRunner:
    def __init__(self):
        self.assembler = _Spec566Assembler()
        self.cfg = _Spec566Config()
        self.services = self
        self.sm = self

    @staticmethod
    def load():
        return {}

    @staticmethod
    def get_total_round():
        return 0

    def _attach_native_trace_to_receipts(self, receipts, declarations):
        return None

    def _settle_receipts_for_next_iteration(self, messages, receipts):
        return None


class _Spec566Assembler:
    def __init__(self):
        self._active_corpus_registry = {
            "C-00001": {
                "kind": "dialogue_progress",
                "entry_key": "dialogue-progress-key",
            }
        }
        self._pending_corpus_expand_once_keys = set()
        self._current_input_text = "query"

    def _derive_input_keywords(self, state, step, mount_ids):
        return ["query"]

    def build_index_view(self, **kwargs):
        return {
            "tool_id": "index_view",
            "tool_family": "protocol_tool",
            "tool_class": "read_tool",
            "status": "accepted",
            "source": "protocol_tool_request",
            "scope": kwargs.get("scope"),
            "content": "INDEX",
        }


class _Spec566Config:
    def get_relation_card_write_guard(self):
        return {"single_declaration": True, "single_target": True}


def test_spec566_dispatcher_executes_corpus_read_without_runner_private_apply():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher

    runner = _Spec566NoPrivateApplyRunner()
    dispatcher = ReactionToolSettlementDispatcher(runner=runner)
    receipts = dispatcher.handle_corpus_read(
        iter_corpus_read_requests=[{"tool_id": "corpus_read", "corpus_id": "C-00001"}],
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_corpus_read_receipts=[],
        all_protocol_tool_receipts=[],
    )

    assert receipts[0]["status"] == "accepted"
    assert "dialogue-progress-key" in runner.assembler._pending_corpus_expand_once_keys


def test_spec566_dispatcher_executes_index_view_without_runner_private_apply():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher

    dispatcher = ReactionToolSettlementDispatcher(runner=_Spec566NoPrivateApplyRunner())
    receipts = dispatcher.handle_index_view(
        active_protocol_tool_guides=[],
        iter_index_view_requests=[{"tool_id": "index_view", "scope": "ltm_heat"}],
        current_state={},
        round_type="interactive",
        mount_ids_current=[],
        interaction_meta={},
        hidden_stm_memory_ids=set(),
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_index_view_receipts=[],
        all_protocol_tool_receipts=[],
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["protocol_read_signature"]


def test_spec566_dispatcher_executes_relation_card_write_without_runner_private_apply():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher

    dispatcher = ReactionToolSettlementDispatcher(runner=_Spec566NoPrivateApplyRunner())
    receipts = dispatcher.handle_relation_card_write(
        iter_accepted_tools=["relation_card_write"],
        active_protocol_tool_guides=[],
        iter_relation_declarations=[
            {"name": "Codex", "category": "them"},
            {"name": "TzPz", "category": "ours"},
        ],
        interaction_meta={},
        mount_ids_current=[],
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_relation_card_receipts=[],
        all_protocol_tool_receipts=[],
    )

    assert receipts == [{
        "tool_id": "relation_card_write",
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "status": "multiple_relation_card_declarations",
        "source": "relation_card_declaration",
        "reason": "multiple_relation_card_declarations",
    }]
