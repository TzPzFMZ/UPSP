import pytest

from tests.test_spec744_ltm_first_recall import recall_layout


class _StateStore:
    def load(self):
        return {"presence": {"confirmed_subjects": ["TzPz"]}}


class _RelationStore:
    @staticmethod
    def resolve_active_subject(value):
        return str(value or "").strip() or None


def _declaration(body, *, weight=2, title="超限候选", keywords=None):
    return {
        "title": title,
        "weight": weight,
        "subject": "TzPz",
        "body": body,
        "candidate_keywords": list(keywords or ["候选", "边界"]),
        "interaction_feelings": [],
        "relationship_feelings": [],
        "reason": "耐久事实",
    }


def _data_modules(env):
    return {
        "memory_store": env["store"],
        "memory_heat": env["heat"],
        "relation_store": _RelationStore(),
    }


@pytest.mark.parametrize(
    ("weight", "limit"),
    [(2, 128), (4, 512), (5, 2048)],
)
def test_spec759_exact_memory_limits_apply_and_plus_one_registers(
        recall_layout, monkeypatch, weight, limit):
    from engines.reaction_helpers import attach_native_trace_to_receipts
    from logic import memory_write
    from logic.memory_write_rewrite import MemoryWriteRewriteTracker

    env = recall_layout
    monkeypatch.setattr(memory_write, "generate_mem_id", lambda: "MEM-75900001")
    exact = _declaration("甲" * limit, weight=weight)
    exact_receipt = memory_write.apply_memory_write_declarations(
        [exact], _StateStore().load(), 759, _data_modules(env)
    )
    assert exact_receipt[0]["status"] == "applied"

    oversized = _declaration("乙" * (limit + 1), weight=weight)
    oversized["call_id"] = "call-over-limit"
    rejected = memory_write.apply_memory_write_declarations(
        [oversized], _StateStore().load(), 759, _data_modules(env)
    )
    attach_native_trace_to_receipts(rejected, [oversized])
    tracker = MemoryWriteRewriteTracker(759)
    created = tracker.register_receipts([oversized], rejected)

    assert rejected[0]["reason"] == (
        f"memory_body_too_long:max={limit};actual={limit + 1}"
    )
    assert created[0]["rewrite_id"] == "MWR-R000759-N001"
    assert created[0]["source"]["call_id"] == "call-over-limit"


def test_spec759_other_validation_errors_do_not_register(recall_layout):
    from logic.memory_write import apply_memory_write_declarations
    from logic.memory_write_rewrite import MemoryWriteRewriteTracker

    env = recall_layout
    declaration = _declaration("甲" * 129, keywords=[])
    declaration["candidate_keywords"] = []
    receipts = apply_memory_write_declarations(
        [declaration], _StateStore().load(), 759, _data_modules(env)
    )
    tracker = MemoryWriteRewriteTracker(759)
    tracker.register_receipts([declaration], receipts)

    assert receipts[0]["reason"] == "missing_keywords"
    assert tracker.pending_ids() == []


def test_spec759_tracker_renders_guide_and_c_material_without_body_in_audit(
        recall_layout):
    from logic.memory_write import apply_memory_write_declarations
    from logic.memory_write_rewrite import MemoryWriteRewriteTracker

    env = recall_layout
    declarations = [
        _declaration("甲" * 129, title="第一条"),
        _declaration("乙" * 129, title="第二条"),
    ]
    receipts = apply_memory_write_declarations(
        declarations, _StateStore().load(), 759, _data_modules(env)
    )
    tracker = MemoryWriteRewriteTracker(759)
    tracker.register_receipts(declarations, receipts)

    assert tracker.pending_ids() == [
        "MWR-R000759-N001", "MWR-R000759-N002"
    ]
    guide = tracker.render_guide("按冻结字段重写；上限不是目标篇幅。")
    materials = tracker.render_materials()
    assert "submit_memory_write_rewrites" in guide
    assert "甲" * 129 not in guide
    assert materials[0]["source"] == "memory_write_rewrite"
    assert materials[0]["source_block_id"] == "MWR-R000759-N001"
    assert "甲" * 129 in materials[0]["content"]
    assert "original_body" not in str(tracker.audit_state())


def test_spec759_guide_rewrite_and_not_written_settle_independently(
        recall_layout, monkeypatch):
    from logic import memory_write
    from logic.memory_write_rewrite import (
        MemoryWriteRewriteTracker,
        apply_memory_write_rewrite_guide,
    )

    env = recall_layout
    monkeypatch.setattr(memory_write, "generate_mem_id", lambda: "MEM-75900002")
    declarations = [
        _declaration("甲" * 129, title="要重写"),
        _declaration("乙" * 129, title="不写了"),
    ]
    rejected = memory_write.apply_memory_write_declarations(
        declarations, _StateStore().load(), 759, _data_modules(env)
    )
    tracker = MemoryWriteRewriteTracker(759)
    tracker.register_receipts(declarations, rejected)
    receipt = apply_memory_write_rewrite_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_write_rewrite_due",
            "option_id": "submit_memory_write_rewrites",
            "fields": {"results": [
                {
                    "rewrite_id": "MWR-R000759-N001",
                    "action": "rewrite",
                    "semantic_content": "保留耐久事实，删除重复流水。",
                },
                {
                    "rewrite_id": "MWR-R000759-N002",
                    "action": "not_written",
                    "semantic_content": "",
                },
            ]},
        },
        {
            "memory_write_rewrite_tracker": tracker,
            "state_store": _StateStore(),
            "round_num": 759,
            **_data_modules(env),
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["completed_ids"] == [
        "MWR-R000759-N001", "MWR-R000759-N002"
    ]
    assert receipt["created_memory_ids"] == ["MEM-75900002"]
    assert receipt["not_written_ids"] == ["MWR-R000759-N002"]
    assert tracker.pending_ids() == []
    assert env["store"].ltm_entry_state("MEM-75900002")["tier"] == "Abstract"


def test_spec759_partial_submission_keeps_invalid_item(recall_layout, monkeypatch):
    from logic import memory_write
    from logic.memory_write_rewrite import (
        MemoryWriteRewriteTracker,
        apply_memory_write_rewrite_guide,
    )

    env = recall_layout
    monkeypatch.setattr(memory_write, "generate_mem_id", lambda: "MEM-75900003")
    declarations = [
        _declaration("甲" * 129, title="合法项"),
        _declaration("乙" * 129, title="仍超限"),
    ]
    rejected = memory_write.apply_memory_write_declarations(
        declarations, _StateStore().load(), 759, _data_modules(env)
    )
    tracker = MemoryWriteRewriteTracker(759)
    tracker.register_receipts(declarations, rejected)
    receipt = apply_memory_write_rewrite_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_write_rewrite_due",
            "option_id": "submit_memory_write_rewrites",
            "fields": {"results": [
                {
                    "rewrite_id": "MWR-R000759-N001",
                    "action": "rewrite",
                    "semantic_content": "合法短正文。",
                },
                {
                    "rewrite_id": "MWR-R000759-N002",
                    "action": "rewrite",
                    "semantic_content": "乙" * 129,
                },
            ]},
        },
        {
            "memory_write_rewrite_tracker": tracker,
            "state_store": _StateStore(),
            "round_num": 759,
            **_data_modules(env),
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["completed_ids"] == ["MWR-R000759-N001"]
    assert receipt["remaining_ids"] == ["MWR-R000759-N002"]


def test_spec759_rewrite_storage_failure_rolls_back_and_keeps_pending(
        recall_layout, monkeypatch):
    from logic import memory_write
    from logic.memory_write_rewrite import (
        MemoryWriteRewriteTracker,
        apply_memory_write_rewrite_guide,
    )

    env = recall_layout
    monkeypatch.setattr(memory_write, "generate_mem_id", lambda: "MEM-75900006")
    declaration = _declaration("甲" * 129, title="事务回滚")
    rejected = memory_write.apply_memory_write_declarations(
        [declaration], _StateStore().load(), 759, _data_modules(env)
    )
    tracker = MemoryWriteRewriteTracker(759)
    tracker.register_receipts([declaration], rejected)

    original_set_entry = env["heat"].set_entry

    def fault_after_heat(mem_id, entry):
        original_set_entry(mem_id, entry)
        raise RuntimeError("injected_after_heat")

    monkeypatch.setattr(env["heat"], "set_entry", fault_after_heat)

    receipt = apply_memory_write_rewrite_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_write_rewrite_due",
            "option_id": "submit_memory_write_rewrites",
            "fields": {"results": [{
                "rewrite_id": "MWR-R000759-N001",
                "action": "rewrite",
                "semantic_content": "回滚后不得留下任何半写状态。",
            }]},
        },
        {
            "memory_write_rewrite_tracker": tracker,
            "state_store": _StateStore(),
            "round_num": 759,
            **_data_modules(env),
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["remaining_ids"] == ["MWR-R000759-N001"]
    assert env["store"].ltm_entry_state("MEM-75900006") is None
    stm = env["store"].stm_entry_state("MEM-75900006")
    assert stm["body"] is None
    assert stm["meta"] is None
    assert stm["heat"] is None
    assert "MEM-75900006" not in env["heat"].load_heat()["entries"]


def test_spec759_guide_backend_memory_write_satisfies_normal_obligation():
    from logic.memory_write_rewrite import MemoryWriteRewriteTracker
    from logic.reaction_obligations import ReactionObligationTracker

    rewrite = MemoryWriteRewriteTracker(759)
    tracker = ReactionObligationTracker(memory_write_rewrite_tracker=rewrite)
    tracker.observe_receipts([{
        "tool_id": "guide_submit",
        "status": "applied",
        "action": "memory_write_rewrites_settled",
        "backend_receipts": [{
            "tool_id": "memory_write",
            "status": "applied",
            "mem_id": "MEM-75900004",
        }],
    }])

    assert tracker.memory_write_seen is True
    assert tracker.applied_memory_ids == ["MEM-75900004"]
    assert "memory_route_pending" in tracker.pending_types()


def test_spec759_old_tool_and_field_are_closed_schema():
    from logic.native_tool_calls import (
        NATIVE_PROTOCOL_DECLARATION_FIELDS,
        SUPPORTED_NATIVE_PROTOCOL_WRITE_TOOLS,
        TOOL_ARGUMENT_SCHEMAS,
    )
    from logic.protocol_tools import TOOL_DEFINITIONS

    assert "pending_cancel" not in TOOL_DEFINITIONS
    assert "pending_cancel" not in SUPPORTED_NATIVE_PROTOCOL_WRITE_TOOLS
    assert "pending_cancel" not in NATIVE_PROTOCOL_DECLARATION_FIELDS
    assert "pending_cancel" not in TOOL_ARGUMENT_SCHEMAS
    assert "resolves_pending_id" not in (
        TOOL_ARGUMENT_SCHEMAS["memory_write"]["properties"]
    )


def test_spec759_historical_pending_cancel_receipt_remains_readable():
    from engines.reaction_helpers import format_protocol_tool_fact

    fact = format_protocol_tool_fact({
        "tool_id": "pending_cancel",
        "status": "applied",
        "reason": "pending_cancelled",
        "pending_id": "PEND-R000001-N001",
    })

    assert "本轮协议工具回执" in fact
    assert "处理结果：applied" in fact


def test_spec759_direct_memory_write_is_rejected_while_guide_was_visible():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
    from logic.memory_write_rewrite import MemoryWriteRewriteTracker

    class _SM:
        @staticmethod
        def get_total_round():
            return 759

    class _Runner:
        sm = _SM()

        @staticmethod
        def _boost_mounted_memory_once(*_args):
            raise AssertionError("rejected write must not heat")

    dispatcher = ReactionToolSettlementDispatcher(_Runner())
    all_memory = []
    all_protocol = []
    mounts = dispatcher.handle_memory_write(
        iter_accepted_tools=["memory_write"],
        iter_memory_write_declarations=[_declaration("短正文")],
        interaction_meta={},
        round_num=759,
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_memory_write_receipts=all_memory,
        all_protocol_tool_receipts=all_protocol,
        pending_memory_ids={},
        hidden_stm_memory_ids=set(),
        boosted_memory_ids=set(),
        mount_ids=[],
        memory_write_rewrite_tracker=MemoryWriteRewriteTracker(759),
        rewrite_pending_at_frame_start=True,
    )

    assert mounts == []
    assert all_memory[0]["reason"] == "memory_write_rewrite_pending_use_guide"
    assert all_protocol == all_memory


def test_spec759_guide_backend_uses_normal_post_settlement_exit():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher

    boosted = []

    class _Runner:
        @staticmethod
        def _boost_mounted_memory_once(mem_id, round_num, boosted_ids):
            boosted.append((mem_id, round_num))
            boosted_ids.add(mem_id)

    dispatcher = ReactionToolSettlementDispatcher(_Runner())
    all_memory = []
    pending = {}
    hidden = set()
    boost_ids = set()
    frame_pending = {}
    mounts = dispatcher.settle_guide_memory_writes(
        [{
            "tool_id": "guide_submit",
            "status": "applied",
            "backend_receipts": [{
                "tool_id": "memory_write",
                "status": "applied",
                "mem_id": "MEM-75900005",
            }],
        }],
        round_num=759,
        accumulated_messages=[],
        all_memory_write_receipts=all_memory,
        all_protocol_tool_receipts=[],
        pending_memory_ids=pending,
        hidden_stm_memory_ids=hidden,
        boosted_memory_ids=boost_ids,
        mount_ids=[],
        frame_pending_memory_ids=frame_pending,
    )

    assert all_memory[0]["mem_id"] == "MEM-75900005"
    assert pending["PENDING"] == "MEM-75900005"
    assert frame_pending["PENDING"] == "MEM-75900005"
    assert hidden == {"MEM-75900005"}
    assert boost_ids == {"MEM-75900005"}
    assert boosted == [("MEM-75900005", 759)]
    assert mounts == [{
        "type": "memory",
        "ids": "MEM-75900005",
        "mode": "temporary",
        "source": "memory_write",
    }]


def test_spec759_same_frame_container_resolves_guide_pending_alias():
    from types import SimpleNamespace

    from engines.product_committer import RuntimeProductCommitter
    from tests.test_spec243_memory_container_tools import (
        DummyContainerStore,
        DummyMemoryStore,
        DummyWorkbenchStore,
    )

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-759GUIDE"] = {
        "id": "MEM-759GUIDE",
        "title": "指南重写记忆",
        "linked_containers": [],
    }
    services = SimpleNamespace(
        sm=_StateStore(),
        memory_store=memory_store,
        memory_index=None,
        heat=None,
        container_store=DummyContainerStore(),
        workbench=DummyWorkbenchStore(focus="DC-OLD"),
        relation_store=_RelationStore(),
    )
    committer = RuntimeProductCommitter(services)
    declaration = {
        "mem_id": "PENDING",
        "container_type": "PRJ",
        "title": "同帧挂接",
        "target_file": "plan.md",
        "container_body": "以刚完成重写的新记忆作为项目引用源。",
        "current_overview": "{container_id}：同帧指南重写后挂接。",
        "reason": "验证指南后结算先于容器结算",
    }

    applied = committer.commit(
        "memory_container_create",
        [declaration],
        round_num=759,
        pending_memory_ids={"PENDING": "MEM-759GUIDE"},
    )[0]
    assert applied["status"] == "applied"
    assert applied["requested_mem_id"] == "PENDING"
    assert applied["mem_id"] == "MEM-759GUIDE"

    rejected = committer.commit(
        "memory_container_create",
        [declaration],
        round_num=759,
        pending_memory_ids={},
    )[0]
    assert rejected["status"] == "rejected"
    assert rejected["reason"] == "pending_mem_id_not_allowed"


def test_spec759_pending_numbering_preserves_each_applied_memory():
    from engines.reaction_helpers import record_pending_memory_ids

    pending = {}
    record_pending_memory_ids(pending, [{
        "status": "applied", "mem_id": "MEM-759FIRST",
    }, {
        "status": "applied", "mem_id": "MEM-759SECOND",
    }])

    assert pending == {
        "PENDING": "MEM-759SECOND",
        "PENDING-1": "MEM-759FIRST",
        "PENDING-2": "MEM-759SECOND",
    }


def test_spec759_rewrite_material_is_visible_once_without_cache_persistence(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.context_store import ContextStore
    from logic.memory_write_rewrite import MemoryWriteRewriteTracker

    cache_dir = tmp_path / "cache"
    raw_jsonl = tmp_path / "buffer" / "raw_log.jsonl"
    raw_md = tmp_path / "buffer" / "raw_log.md"
    store = ContextStore(
        cache_dir=str(cache_dir),
        raw_log_jsonl=str(raw_jsonl),
        raw_log_md=str(raw_md),
    )
    store.append_to_cache(
        759, "user", "持久起手包。", kind="interaction", step="setup"
    )
    tracker = MemoryWriteRewriteTracker(759)
    original = "SPEC759_C_TRACK_SENTINEL_" + ("正文" * 80)
    declaration = _declaration(original)
    tracker.register_receipts([declaration], [{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=128;actual=183",
        "title": declaration["title"],
        "weight": declaration["weight"],
        "subject": declaration["subject"],
        "keywords": declaration["candidate_keywords"],
        "max_chars": 128,
    }])

    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"), context_store=store
    )
    monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
    monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
    monkeypatch.setattr(
        assembler, "_build_high_freq", lambda *args, **kwargs: "高频"
    )
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

    _system, messages = assembler.assemble_reaction(
        {"base": {"meta": {"total_round": 759},
                  "runtime": {"total_round": 759}}},
        "interactive",
        material_inputs=tracker.render_materials(),
        reaction_loop_phase="loop",
    )
    visible = "\n".join(str(item.get("content") or "") for item in messages)
    assert "SPEC759_C_TRACK_SENTINEL_" in visible

    store.transition_current_cache(
        boundary="reaction_provider_return",
        consumer_frame_id="R000759-reaction-001",
    )
    persisted = "\n".join(
        path.read_text(encoding="utf-8") if path.exists() else ""
        for path in (
            cache_dir / "now_cache.jsonl",
            cache_dir / "lately_cache.jsonl",
            raw_jsonl,
            raw_md,
        )
    )
    assert "SPEC759_C_TRACK_SENTINEL_" not in persisted


def test_spec759_reconsolidation_guide_preempts_write_rewrite():
    from logic.guide_submit import apply_guide_submit
    from logic.memory_write_rewrite import MemoryWriteRewriteTracker

    class _Reconsolidation:
        guide_id = "memory_reconsolidation:R000759"

        @staticmethod
        def has_pending():
            return True

    rewrite = MemoryWriteRewriteTracker(759)
    rewrite._pending["MWR-R000759-N001"] = {}
    receipt = apply_guide_submit(
        None,
        {"guide_id": rewrite.guide_id},
        evidence_context={
            "memory_reconsolidation_tracker": _Reconsolidation(),
            "memory_write_rewrite_tracker": rewrite,
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "memory_reconsolidation_pending"
