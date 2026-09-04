def test_spec779_task_bootstrap_and_general_results_settle_independently(tmp_path):
    from types import SimpleNamespace

    from data.workbench import WorkbenchStore
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务来源后执行")
    source_url = "https://example.com/task-brief"
    prior_read = {
        "tool_id": "web_fetch",
        "status": "ok",
        "call_id": "call_prior_source",
        "url": source_url,
    }
    current_general_result = {
        "tool_id": "web_search",
        "status": "ok",
        "call_id": "call_same_frame_search",
        "query": "unrelated current lookup",
    }
    all_general_results = [prior_read]
    all_protocol_receipts = []
    general_dispatcher = SimpleNamespace(
        handle_requests=lambda requests, guides, prior_results, runtime_context: [
            current_general_result
        ],
    )
    runner = SimpleNamespace(
        workbench=store,
        sm=SimpleNamespace(
            get_total_round=lambda: 691,
            get=lambda key, default=None: default,
        ),
        ctx_store=None,
        alert_store=None,
        services=SimpleNamespace(),
        general_tool_dispatcher=general_dispatcher,
        action_recovery_store=None,
        _current_round_type="interactive",
        _current_interaction_meta={},
        _write_general_tool_results=lambda *args, **kwargs: None,
    )

    dispatcher = ReactionToolSettlementDispatcher(runner)
    prior_results = list(all_general_results)
    iter_results = dispatcher.handle_general_tool_results(
        iter_general_tool_requests=[{
            "tool_id": "web_search",
            "query": "unrelated current lookup",
        }],
        active_general_tool_guides=[],
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_general_tool_results=all_general_results,
        iter_native_feedbacks=[],
        round_num=691,
        iteration=4,
        interaction_meta={},
    )
    receipts = dispatcher.handle_guide_submit(
        iter_accepted_tools={"guide_submit"},
        iter_guide_submit_requests=[{
            "guide_id": "task_bootstrap",
            "submissions": [{
                "item_id": "build_initial_task_guide",
                "option_id": "submit_initial_guide",
                "fields": {
                    "task_title": "同帧共存回归",
                    "task_goal": "按已读任务来源完成交付",
                    "source_refs": [source_url],
                    "source_requirements": [{
                        "requirement_id": "req_01",
                        "source_ref": source_url,
                        "summary": "完成来源要求的交付",
                    }],
                    "items": [{
                        "item_id": "item_01",
                        "title": "完成交付",
                        "requirement_refs": ["req_01"],
                    }],
                    "acceptance": [{
                        "acceptance_id": "acc_01",
                        "description": "交付结果可核验",
                        "item_refs": ["item_01"],
                    }],
                },
            }],
        }],
        prior_general_tool_results=prior_results,
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_guide_submit_receipts=[],
        all_protocol_tool_receipts=all_protocol_receipts,
        current_reaction_iteration=4,
    )

    assert receipts[0]["status"] == "accepted"
    assert all_protocol_receipts == receipts
    assert iter_results == [current_general_result]
    assert all_general_results == [prior_read, current_general_result]
    assert store.get("base.active_task")


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
        self.last_index_kwargs = None

    def _derive_input_keywords(self, state, step, mount_ids):
        return ["query"]

    def build_index_view(self, **kwargs):
        self.last_index_kwargs = kwargs
        return {
            "tool_id": "index_view",
            "tool_class": "read_tool",
            "status": "accepted",
            "source": "protocol_tool_request",
            "scope": kwargs.get("scope"),
            "content": "INDEX",
        }

    def build_memory_search(self, **kwargs):
        self.last_index_kwargs = kwargs
        return {
            "tool_id": "memory_search",
            "tool_class": "read_tool",
            "status": "accepted",
            "source": "protocol_tool_request",
            "content": "MEMORY SEARCH",
            "locator_only": True,
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


def test_spec756_dispatcher_executes_memory_search_without_runner_private_apply():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher

    dispatcher = ReactionToolSettlementDispatcher(runner=_Spec566NoPrivateApplyRunner())
    receipts = dispatcher.handle_memory_search(
        iter_memory_search_requests=[{
            "tool_id": "memory_search",
            "query_terms": ["露营"],
        }],
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_memory_search_receipts=[],
        all_protocol_tool_receipts=[],
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["protocol_read_signature"]
    assert dispatcher.runner.assembler.last_index_kwargs["query_terms"] == ["露营"]


def test_spec756_memory_search_signature_uses_normalized_query_terms():
    from engines.reaction_tool_settlement import _read_signature

    first = _read_signature("memory_search", {
        "query_terms": [" ＣＡＭＰ ", "孩子"],
        "offset": 0, "limit": 8,
    })
    same = _read_signature("memory_search", {
        "query_terms": ["孩子", "camp", "camp"],
        "offset": 0, "limit": 8,
    })
    next_page = _read_signature("memory_search", {
        "query_terms": ["camp", "孩子"],
        "offset": 8, "limit": 8,
    })
    implicit_page = _read_signature("memory_search", {
        "query_terms": ["camp", "孩子"],
    })
    clamped = _read_signature("memory_search", {
        "query_terms": ["camp", "孩子"],
        "offset": "0", "limit": 100,
    })
    max_page = _read_signature("memory_search", {
        "query_terms": ["camp", "孩子"],
        "offset": 0, "limit": 32,
    })

    assert first == same
    assert implicit_page == first
    assert clamped == max_page
    assert next_page != first


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
        "status": "multiple_relation_card_declarations",
        "source": "relation_card_declaration",
        "reason": "multiple_relation_card_declarations",
    }]


def test_action_recovery_pending_input_uses_direct_completion_and_blocker_refs(tmp_path):
    import hashlib

    from data.action_recovery_store import ActionRecoveryStore
    from data.workbench import WorkbenchStore
    from logic.action_recovery import attach_pending_task
    from logic.task_guide import (
        apply_pending_input_integration, apply_task_status_update,
        materialize_initial_task_guide)

    recovery = ActionRecoveryStore(tmp_path / "action_recovery_pending.json")
    context = {"round_num": 12, "iteration": 3,
               "frame_id": "R000012:reaction:3"}

    def prepare_file(name, call):
        target = tmp_path / name
        target.write_bytes(b"before")
        action_id = recovery.prepare_file(
            tool_id="file_write",
            request_sha256=hashlib.sha256(call.encode()).hexdigest(),
            runtime_context=context, call_id=call, target_path=str(target),
            before_bytes=b"before", candidate_bytes=b"after")
        recovery.commit_file(action_id, target, b"before", b"after")
        return action_id

    old_id = prepare_file("old.txt", "old")
    recovery.classify_interrupted(12)
    recovery.mark_disclosed(12)
    applied_id = prepare_file("output.txt", "current")
    unknown_id = recovery.prepare_opaque(
        tool_id="shell_command", request_sha256="f" * 64,
        runtime_context=context | {"iteration": 4}, call_id="shell", target="shell")
    recovery.classify_interrupted(12)
    receipt = recovery.recovery_receipt(pending_only=True)
    assert f"action:{old_id}" not in receipt["source_refs"]

    workbench = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(workbench, {
        "task_title": "resume", "task_goal": "finish safely",
        "items": [
            {"item_id": "written", "description": "write output"},
            {"item_id": "shell", "description": "run command"},
            {"item_id": "old", "description": "unrelated prior action"}],
        "acceptance": []})
    assert attach_pending_task(workbench, receipt, 13) is True
    assert attach_pending_task(workbench, receipt, 13) is True
    pending = workbench.load_task_guide(task_id)["pending_inputs"]
    assert len(pending) == 1
    assert pending[0]["source_refs"] == receipt["source_refs"]
    evidence = {"action_recovery_receipt": recovery.recovery_receipt()}

    rejected = apply_pending_input_integration(workbench, task_id, {
        "pending_inputs": [pending[0]["pending_input_id"]],
        "items": {"shell": {"status": "done",
                            "evidence_refs": [f"action:{unknown_id}"]}}},
        evidence_context=evidence)
    assert rejected["reason"] == "task_completion_evidence_not_found"
    assert not any(ref.startswith("EV-") for ref in rejected["details"]["known_evidence_refs"])

    accepted = apply_task_status_update(workbench, task_id, {"items": {
        "written": {"status": "done",
                    "evidence_refs": [f"action:{applied_id}"]},
        "shell": {"status": "blocked", "reason": "outcome unknown",
                  "evidence_refs": [f"action:{unknown_id}"]}}},
        evidence_context=evidence)
    assert accepted["status"] == "accepted"
    rejected = apply_task_status_update(
        workbench, task_id,
        {"items": {"old": {"status": "done",
                            "evidence_refs": [f"action:{old_id}"]}}},
        evidence_context=evidence)
    assert rejected["reason"] == "task_completion_evidence_not_found"


def test_action_result_and_frame_settlement_fail_closed_after_effect():
    import pytest

    from data.action_recovery_store import ActionRecoveryEffectError
    from engines.reaction_loop_main import _settle_reaction_frame
    from engines.reaction_loop_result import ReactionLoopResultState
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
    from engines.round_context import FrameRef

    class Dispatcher:
        @staticmethod
        def handle_requests(*_args, **_kwargs):
            return [{"tool_id": "file_write", "status": "ok",
                     "action_id": "ACT-R000001-F000001-A001"}]

    for failure_site in ("tool_fact", "journal"):
        class ActionStore:
            @staticmethod
            def record_results(_results):
                if failure_site == "journal":
                    raise OSError("journal offline")

        class Runner:
            sm = type("State", (), {"get": staticmethod(lambda *_args: None)})()
            general_tool_dispatcher = Dispatcher()
            action_recovery_store = ActionStore()
            execution_permission_level = "guarded"

            @staticmethod
            def _write_general_tool_results(*_args, **_kwargs):
                if failure_site == "tool_fact":
                    raise OSError("tool fact offline")

        with pytest.raises(ActionRecoveryEffectError, match="result_record_failed"):
            ReactionToolSettlementDispatcher(Runner()).handle_general_tool_results(
                iter_general_tool_requests=[], active_general_tool_guides=[],
                accumulated_messages=[], iter_native_tool_call_envelopes=[],
                all_general_tool_results=[], iter_native_feedbacks=[], round_num=1,
                iteration=1, interaction_meta={})

    class AuditFailureRunner:
        action_recovery_store = None
        organ_runtime = None

        @staticmethod
        def _round_audit_settlement(*_args, **_kwargs):
            raise OSError("audit offline")

    state = ReactionLoopResultState(all_general_tool_results=[{
        "tool_id": "file_write", "status": "ok",
        "action_id": "ACT-R000001-F000001-A001"}])
    with pytest.raises(ActionRecoveryEffectError, match="frame_settlement_failed"):
        _settle_reaction_frame(
            AuditFailureRunner(), state, FrameRef.for_axis(1, "reaction", 1),
            (0, 0, 0, 0), "continue_requested")
