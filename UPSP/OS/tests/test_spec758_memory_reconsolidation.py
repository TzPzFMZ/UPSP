import pytest

from tests.test_spec744_ltm_first_recall import (
    _add_ltm,
    _processor,
    _set_ltm_meta,
    recall_layout,
)


def _recalled_mismatch(env, mem_id, *, tier="Summary", weight=5, title="久远记忆"):
    from logic.memory_reconsolidation import MemoryReconsolidationTracker

    _add_ltm(env, tier, mem_id, weight=weight, title=title)
    _set_ltm_meta(env, mem_id, decay_countdown_days=0)
    tracker = MemoryReconsolidationTracker(123)
    heat_ids = set()
    receipt = _processor(env).recall(
        mem_id,
        round_num=123,
        boosted_ids=heat_ids,
        reconsolidation_tracker=tracker,
    )
    return tracker, receipt, heat_ids


def test_spec758_real_recall_registers_one_round_local_guide(recall_layout):
    env = recall_layout
    mem_id = "MEM-75800001"
    tracker, receipt, heat_ids = _recalled_mismatch(env, mem_id)

    assert receipt["reconsolidation_required"] is True
    assert receipt["reconsolidation_guide_id"] == "memory_reconsolidation:R000123"
    assert heat_ids == {mem_id}
    assert tracker.pending_ids() == [mem_id]
    assert tracker.pending_items()[0]["target_tier"] == "Full"

    second = _processor(env).recall(
        mem_id,
        round_num=123,
        boosted_ids=heat_ids,
        reconsolidation_tracker=tracker,
    )
    assert second["heat_boost_applied"] is False
    assert second["heat_boost_deduplicated"] is True
    assert tracker.pending_ids() == [mem_id]

    guide = tracker.render_guide("只按证据重整；证据不足时诚实说明模糊。")
    assert "memory_reconsolidation_due" in guide
    assert "submit_memory_reconsolidations" in guide
    assert mem_id in guide
    assert (
        f'"mem_id":"{mem_id}","semantic_content"'
    ) in guide
    assert '"final_keywords"' in guide


@pytest.mark.parametrize(
    ("tier", "weight", "stored_at"),
    [
        ("Full", 5, "2026-08-13T07:00:00+08:00"),
        ("Summary", 4, "2026-08-13T07:00:00+08:00"),
        ("Abstract", 2, "2026-08-13T07:00:00+08:00"),
        ("Summary", 5, ""),
        ("Pinned", 5, "2026-08-13T07:00:00+08:00"),
    ],
)
def test_spec758_non_trigger_states_do_not_register(
        recall_layout, tier, weight, stored_at):
    from logic.memory_reconsolidation import MemoryReconsolidationTracker

    env = recall_layout
    mem_id = "MEM-75800002"
    _add_ltm(env, tier, mem_id, weight=weight)
    _set_ltm_meta(env, mem_id, stored_at=stored_at)
    tracker = MemoryReconsolidationTracker(124)

    receipt = _processor(env).recall(
        mem_id,
        round_num=124,
        boosted_ids=set(),
        reconsolidation_tracker=tracker,
    )

    assert receipt["reconsolidation_required"] is False
    assert tracker.pending_ids() == []


def test_spec758_processor_restores_target_without_second_recall(recall_layout):
    from logic.memory_reconsolidation import MemoryReconsolidationProcessor

    env = recall_layout
    mem_id = "MEM-75800003"
    tracker, _receipt, _heat_ids = _recalled_mismatch(env, mem_id)
    before = env["store"].stm_entry_state(mem_id)
    before_heat = before["heat"]
    before_meta = before["meta"]

    receipt = MemoryReconsolidationProcessor(memory_store=env["store"]).apply(
        tracker.get(mem_id),
        "这段旧记忆仍能确认主体与事件，但具体日期已经模糊。",
        ["主体", "旧事件", "细节模糊"],
    )

    ltm = env["store"].ltm_entry_state(mem_id)
    stm = env["store"].stm_entry_state(mem_id)
    assert receipt["status"] == "applied"
    assert ltm["tier"] == "Full"
    assert "这段旧记忆仍能确认主体与事件，但具体日期已经模糊。" in ltm["body"]
    assert ltm["meta"]["tags"] == ["主体", "旧事件", "细节模糊"]
    assert ltm["meta"]["title"] == "久远记忆[回忆重整]"
    assert ltm["meta"]["recalled"] is True
    assert stm["body"] == ltm["body"]
    assert stm["heat"] == before_heat
    for field in (
        "weight", "stored_at", "last_recalled_at", "last_recalled_round",
        "last_recalled_instance_id",
    ):
        assert ltm["meta"][field] == before_meta[field]


def test_spec758_legacy_title_marker_is_not_doubled(recall_layout):
    from logic.memory_reconsolidation import MemoryReconsolidationProcessor

    env = recall_layout
    mem_id = "MEM-75800004"
    tracker, _receipt, _heat_ids = _recalled_mismatch(
        env, mem_id, title="旧标题[召回补全内容]"
    )
    MemoryReconsolidationProcessor(memory_store=env["store"]).apply(
        tracker.get(mem_id), "仍能确认的旧事实。", ["旧事实"]
    )
    assert env["store"].ltm_entry_state(mem_id)["meta"]["title"] == (
        "旧标题[召回补全内容]"
    )


def test_spec758_guide_submit_partially_applies_and_keeps_failures(recall_layout):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        MemoryReconsolidationTracker,
        apply_memory_reconsolidation_guide,
    )

    env = recall_layout
    tracker = MemoryReconsolidationTracker(125)
    ids = ["MEM-75800005", "MEM-75800006"]
    for mem_id in ids:
        _add_ltm(env, "Abstract", mem_id, weight=4)
        _processor(env).recall(
            mem_id,
            round_num=125,
            boosted_ids=set(),
            reconsolidation_tracker=tracker,
        )

    receipt = apply_memory_reconsolidation_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_reconsolidation_due",
            "option_id": "submit_memory_reconsolidations",
            "fields": {
                "results": [
                    {
                        "mem_id": ids[0],
                        "semantic_content": "可确认的重整正文。",
                        "final_keywords": ["可确认"],
                    },
                    {
                        "mem_id": ids[1],
                        "semantic_content": "超" * 513,
                        "final_keywords": ["超限"],
                    },
                ]
            },
        },
        {
            "memory_reconsolidation_tracker": tracker,
            "memory_reconsolidation_processor": MemoryReconsolidationProcessor(
                memory_store=env["store"]
            ),
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["completed_ids"] == [ids[0]]
    assert receipt["remaining_ids"] == [ids[1]]
    assert env["store"].ltm_entry_state(ids[0])["tier"] == "Summary"
    assert env["store"].ltm_entry_state(ids[1])["tier"] == "Abstract"


def test_spec758_top_level_guide_submit_retries_periodic_after_semantic_commit(
        recall_layout):
    from logic.guide_submit import apply_guide_submit
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        MemoryReconsolidationTracker,
    )

    env = recall_layout
    mem_id = "MEM-75800008"
    _add_ltm(env, "Summary", mem_id, weight=5)
    tracker = MemoryReconsolidationTracker(126)
    _processor(env).recall(
        mem_id,
        round_num=126,
        boosted_ids=set(),
        reconsolidation_tracker=tracker,
        periodic_requested=True,
    )

    class Periodic:
        def __init__(self):
            self.blocked = []

        def apply(self, action, target):
            assert (action, target) == ("mount", mem_id)
            raise ValueError("periodic_memory_budget_exceeded")

        def mark_pending_blocked(self, target, reason):
            self.blocked.append((target, reason))

    periodic = Periodic()
    receipt = apply_guide_submit(
        object(),
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_reconsolidation_due",
            "option_id": "submit_memory_reconsolidations",
            "fields": {"results": [{
                "mem_id": mem_id,
                "semantic_content": "已经恢复的真实正文。",
                "final_keywords": ["真实正文"],
            }]},
        },
        evidence_context={
            "memory_reconsolidation_tracker": tracker,
            "memory_reconsolidation_processor": MemoryReconsolidationProcessor(
                memory_store=env["store"]
            ),
            "periodic_mount_processor": periodic,
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["action"] == "memory_reconsolidation_settled"
    assert receipt["completed_ids"] == [mem_id]
    assert receipt["remaining_ids"] == []
    assert receipt["backend_receipts"][0]["periodic_mount_outcome"] == (
        "mount_blocked"
    )
    assert periodic.blocked[0][0] == mem_id
    assert env["store"].ltm_entry_state(mem_id)["tier"] == "Full"


def test_spec758_periodic_mount_succeeds_after_reconsolidation(recall_layout):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        apply_memory_reconsolidation_guide,
    )

    env = recall_layout
    mem_id = "MEM-7580000A"
    tracker, _receipt, _heat_ids = _recalled_mismatch(env, mem_id)
    tracker._pending[mem_id]["periodic_requested"] = True

    class Periodic:
        def apply(self, action, target):
            assert (action, target) == ("mount", mem_id)
            return {"status": "applied", "outcome": "mounted"}

    receipt = apply_memory_reconsolidation_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_reconsolidation_due",
            "option_id": "submit_memory_reconsolidations",
            "fields": {"results": [{
                "mem_id": mem_id,
                "semantic_content": "仍可确认的旧事件。",
                "final_keywords": ["旧事件"],
            }]},
        },
        {
            "memory_reconsolidation_tracker": tracker,
            "memory_reconsolidation_processor": MemoryReconsolidationProcessor(
                memory_store=env["store"]
            ),
            "periodic_mount_processor": Periodic(),
        },
    )

    assert receipt["remaining_ids"] == []
    assert receipt["backend_receipts"][0]["periodic_mount_outcome"] == "mounted"


def test_spec758_periodic_request_without_processor_is_mount_blocked(
        recall_layout):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        apply_memory_reconsolidation_guide,
    )

    env = recall_layout
    mem_id = "MEM-7580000C"
    tracker, _receipt, _heat_ids = _recalled_mismatch(env, mem_id)
    tracker._pending[mem_id]["periodic_requested"] = True
    receipt = apply_memory_reconsolidation_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_reconsolidation_due",
            "option_id": "submit_memory_reconsolidations",
            "fields": {"results": [{
                "mem_id": mem_id,
                "semantic_content": "仍可确认的旧事件。",
                "final_keywords": ["旧事件"],
            }]},
        },
        {
            "memory_reconsolidation_tracker": tracker,
            "memory_reconsolidation_processor": MemoryReconsolidationProcessor(
                memory_store=env["store"]
            ),
        },
    )

    backend = receipt["backend_receipts"][0]
    assert backend["periodic_mount_outcome"] == "mount_blocked"
    assert backend["periodic_mount_reason"] == (
        "periodic_mount_processor_unavailable"
    )


def test_spec758_changed_source_and_extra_fields_remain_pending(recall_layout):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        apply_memory_reconsolidation_guide,
    )

    env = recall_layout
    mem_id = "MEM-7580000B"
    tracker, _receipt, _heat_ids = _recalled_mismatch(env, mem_id)
    _set_ltm_meta(env, mem_id, title="外部变化后的标题")
    processor = MemoryReconsolidationProcessor(memory_store=env["store"])

    extra = apply_memory_reconsolidation_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_reconsolidation_due",
            "option_id": "submit_memory_reconsolidations",
            "fields": {"results": [], "unexpected": True},
        },
        {
            "memory_reconsolidation_tracker": tracker,
            "memory_reconsolidation_processor": processor,
        },
    )
    changed = apply_memory_reconsolidation_guide(
        {
            "guide_id": tracker.guide_id,
            "item_id": "memory_reconsolidation_due",
            "option_id": "submit_memory_reconsolidations",
            "fields": {"results": [{
                "mem_id": mem_id,
                "semantic_content": "不会写入的正文。",
                "final_keywords": ["正文"],
            }]},
        },
        {
            "memory_reconsolidation_tracker": tracker,
            "memory_reconsolidation_processor": processor,
        },
    )

    assert extra["reason"] == "memory_reconsolidation_fields_invalid"
    assert extra["completed_ids"] == []
    assert extra["remaining_ids"] == [mem_id]
    assert changed["status"] == "rejected"
    assert "memory_reconsolidation_source_changed" in (
        changed["backend_receipts"][0]["reason"]
    )
    assert tracker.pending_ids() == [mem_id]


def test_spec758_pending_reconsolidation_blocks_other_guide_and_closeout():
    from logic.guide_submit import apply_guide_submit
    from logic.memory_reconsolidation import MemoryReconsolidationTracker
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = MemoryReconsolidationTracker(127)
    tracker._pending["MEM-75800009"] = {
        "mem_id": "MEM-75800009",
        "semantic_fingerprint": "frozen",
    }
    receipt = apply_guide_submit(
        object(),
        {"guide_id": "task:T-1", "item_id": "x", "option_id": "done"},
        evidence_context={"memory_reconsolidation_tracker": tracker},
    )
    closeout = ReactionObligationTracker(
        memory_reconsolidation_tracker=tracker
    ).validate_closeout_form({"closeout_decision": "finish"})

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "memory_reconsolidation_pending"
    assert closeout["blocked"] is True
    assert any(
        reason.startswith("memory_reconsolidation_pending_unresolved")
        for reason in closeout["reasons"]
    )


def test_spec758_popup_discipline_is_loaded_from_protocol_truth():
    from assembly.popup import PopupManager

    discipline = PopupManager.load_guide_template("memory_reconsolidation")
    assert discipline
    assert "不得取消、跳过、延后、降权" in discipline
    assert "Full 最多 2048 字" in discipline


def test_spec758_item_failure_rolls_back_every_memory_file(recall_layout):
    from logic.memory_reconsolidation import MemoryReconsolidationProcessor

    env = recall_layout
    mem_id = "MEM-75800007"
    tracker, _receipt, _heat_ids = _recalled_mismatch(env, mem_id)
    before_ltm = env["store"].snapshot_ltm_files()
    before_stm = env["store"].snapshot_stm_files()

    def fault(stage):
        if stage == "after_stm_meta":
            raise OSError("injected")

    with pytest.raises(OSError, match="injected"):
        MemoryReconsolidationProcessor(
            memory_store=env["store"], fault_hook=fault
        ).apply(tracker.get(mem_id), "可确认正文。", ["关键词"])

    assert env["store"].snapshot_ltm_files() == before_ltm
    assert env["store"].snapshot_stm_files() == before_stm


def test_spec758_old_tool_and_alias_are_not_active():
    from logic.native_tool_calls import export_provider_tool_schemas
    from logic.protocol_tools import normalize_tool_id, tool_metadata_for

    names = {
        item["name"]
        for item in export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
            execution_permission_level="unlimited",
        )
    }
    assert "memory_recall_complete" not in names
    assert normalize_tool_id("memory_recall_completion_request") == (
        "memory_recall_completion_request"
    )
    assert tool_metadata_for("memory_recall_completion_request") == {}
