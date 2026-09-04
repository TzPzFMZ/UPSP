import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def _receipt(tool_id, status="applied", **kwargs):
    payload = {"tool_id": tool_id, "status": status}
    payload.update(kwargs)
    return payload


def _closeout_form(**overrides):
    payload = {
        "closeout_decision": "finish",
        "memory_status": "weight_zero",
        "memory_reason": "unit test",
        "read_status": "not_applicable",
        "read_reason": "unit test",
        "pending_status": "none",
        "pending_reason": "",
    }
    payload.update(overrides)
    return payload


def test_spec202_memory_write_opens_route_obligation():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt("memory_write", mem_id="MEM-1", title="偏好"),
    ])

    assert tracker.pending_types() == ["memory_route_pending"]
    assert tracker.pending[0]["target_refs"] == ["MEM-1"]
    result = tracker.validate_closeout_form(_closeout_form(
        memory_status="written",
        pending_status="deferred",
        pending_reason="孤立记忆，不进入容器。",
    ))
    assert result["blocked"] is False
    ledger = result["settlement_ledger"]
    assert ledger["memory_refs"] == ["MEM-1"]
    assert ledger["pending_resolution_result"] == "open"


def test_spec781_container_create_closes_route_and_opens_future_check():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt("memory_write", mem_id="MEM-1"),
        _receipt(
            "memory_container_create",
            container_type="PRJ",
            container_id="PRJ-1",
            mem_id="MEM-1",
        ),
    ])

    assert tracker.pending_types() == ["future_jump_pending"]
    assert tracker.pending[0]["target_refs"] == ["PRJ-1"]


def test_spec781_container_write_has_no_redundant_link_obligation():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt(
            "memory_container_write",
            container_type="DC",
            container_id="DC-1",
            mem_id="MEM-1",
        ),
        _receipt(
            "memory_link_update",
            mem_id="MEM-1",
            linked_containers=["DC-1"],
        ),
    ])

    assert tracker.pending_types() == ["future_jump_pending"]


def test_spec781_future_container_closes_future_check_atomically():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt(
            "memory_container_write",
            container_type="DC",
            container_id="DC-1",
            mem_id="MEM-1",
        ),
        _receipt(
            "memory_container_create",
            container_type="FUT",
            container_id="FUT-plans-1",
            mem_id="MEM-FUT",
        ),
    ])

    assert tracker.pending_types() == []

    result = tracker.validate_closeout_form(_closeout_form(
        memory_reason="容器事务已同时闭合正文和记忆链接。",
        pending_status="none",
    ))
    assert result["blocked"] is False


def test_spec781_future_container_receipt_does_not_create_anchor_debt():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt(
            "memory_container_write",
            container_type="PRJ",
            container_id="PRJ-1",
            mem_id="MEM-1",
        ),
        _receipt(
            "memory_container_write",
            container_type="FUT",
            container_id="FUT-plans-1",
            mem_id="MEM-FUT",
        ),
        _receipt("memory_write", mem_id="MEM-FUT"),
        _receipt(
            "memory_link_update",
            mem_id="MEM-FUT",
            linked_containers=["FUT-plans-1", "PRJ-1"],
        ),
    ])

    assert "future_anchor_pending" not in tracker.pending_types()


def test_spec202_closeout_form_requires_memory_status_without_memory():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()

    missing = tracker.validate_closeout_form(_closeout_form(memory_status=""))
    assert missing["blocked"] is False
    assert missing["reasons"] == []
    assert missing["settlement_ledger"]["memory_status"] == "not_applicable"

    provided = tracker.validate_closeout_form(_closeout_form(
        memory_status="weight_zero",
        memory_reason="本轮只有健康检查和连接测试，属于权重0噪音。",
    ))
    assert provided["blocked"] is False


def test_spec222_closeout_form_rejects_invalid_memory_status():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()

    result = tracker.validate_closeout_form(_closeout_form(
        memory_status="memory_no_write_reason",
    ))

    assert result["blocked"] is True
    assert "memory_status_invalid:memory_no_write_reason" in result["reasons"]


def test_spec222_closeout_form_accepts_weight_zero_or_existing_memory_reason():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()

    weight_zero = tracker.validate_closeout_form(_closeout_form(
        memory_status="weight_zero",
        memory_reason="本轮只有无内容短句确认，属于权重0噪音。",
    ))
    covered = tracker.validate_closeout_form(_closeout_form(
        memory_status="covered_by_existing",
        memory_reason="本轮材料已由现有记忆覆盖，不新建。",
    ))

    assert weight_zero["blocked"] is False
    assert covered["blocked"] is False


def test_spec251_written_memory_status_requires_applied_receipt():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()

    missing_receipt = tracker.validate_closeout_form(_closeout_form(
        memory_status="written",
        memory_reason="本轮已写入。",
    ))

    assert missing_receipt["blocked"] is False
    assert missing_receipt["settlement_ledger"]["memory_status"] == "not_applicable"
    assert "memory_status_ignored:written_without_receipt" in (
        missing_receipt["settlement_ledger"]["corrections"]
    )

    tracker.observe_receipts([_receipt("memory_write", mem_id="MEM-REAL0001")])
    matched = tracker.validate_closeout_form(_closeout_form(
        memory_status="written",
        memory_reason="本轮已写入。",
        pending_status="deferred",
        pending_reason="本轮只验证写入结算，不挂接容器。",
    ))

    assert matched["blocked"] is False
    assert matched["settlement_ledger"]["memory_refs"] == ["MEM-REAL0001"]


def test_spec251_file_read_completion_is_checked_against_general_tool_result():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_general_tool_results([{
        "tool_id": "file_read",
        "status": "ok",
        "path": "book.md",
        "truncated": True,
        "start_line": 1,
        "end_line": 120,
        "next_start_line": 121,
    }])

    complete = tracker.validate_closeout_form(_closeout_form(
        memory_reason="只测试读书结算。",
        read_status="complete",
        read_reason="已经读完。",
    ))
    assert complete["blocked"] is False
    assert complete["settlement_ledger"]["read_status"] == "partial_user_wait"

    continue_without_relay = tracker.validate_closeout_form(_closeout_form(
        closeout_decision="continue",
        memory_reason="只测试读书结算。",
        read_status="partial_continue",
        read_reason="下一轮继续。",
    ))
    assert continue_without_relay["blocked"] is True
    assert "closeout_continue_requires_handoff_text" in continue_without_relay["reasons"]

    continue_with_relay = tracker.validate_closeout_form(_closeout_form(
        closeout_decision="continue",
        memory_reason="只测试读书结算。",
        read_status="partial_continue",
        read_reason="下一轮继续。",
        handoff_text="下一轮从 book.md 第 121 行继续读取。",
    ))
    assert continue_with_relay["blocked"] is False
    assert continue_with_relay["settlement_ledger"]["read_refs"] == [
        "file_read:book.md:121"
    ]


def test_spec202_regular_obligations_are_recorded_as_open_pending():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt("memory_write", mem_id="MEM-1"),
    ])

    result = tracker.validate_closeout_form(_closeout_form(
        memory_status="written",
        memory_reason="本轮已写入。",
        pending_status="deferred",
        pending_reason="只是孤立偏好，不进入容器。",
    ))

    assert result["blocked"] is False
    assert result["settlement_ledger"]["pending_resolution_result"] == "open"


def test_spec202_prompt_is_visible_without_machine_field_lines():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt("memory_write", mem_id="MEM-1"),
    ])

    prompt = tracker.render_prompt()

    assert "## REMINDER｜提醒" in prompt
    assert "记忆条目" in prompt
    assert "MEM-1" in prompt
    assert "pending_obligations" not in prompt
    assert "memory_route_pending" not in prompt
    assert "kind:" not in prompt
    assert "tier:" not in prompt
    assert "source:" not in prompt


def test_spec340_skippable_memory_route_prompt_is_not_hard_block():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt("memory_write", mem_id="MEM-1"),
    ])

    prompt = tracker.render_prompt()

    assert "请先处理后再结束反应步" not in prompt
    assert "可延期" in prompt
    assert "分别检查 DC、EC、PRJ、FUT" in prompt
    assert "确实均不满足永固触发条件才可自然语言回复收束" in prompt
    assert "reaction_finalize" not in prompt
    assert "deferred/open" in prompt


def test_spec340_many_memory_route_pending_warns_to_stop_new_memory_writes():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt("memory_write", mem_id="MEM-1"),
        _receipt("memory_write", mem_id="MEM-2"),
        _receipt("memory_write", mem_id="MEM-3"),
    ])

    prompt = tracker.render_prompt()

    assert "停止新增 memory_write" in prompt
    assert "只处理已有 pending" in prompt
    assert "均不满足时才自然语言回复收束" in prompt
    assert "reaction_finalize" not in prompt


def test_spec781_atomic_future_container_does_not_leave_hard_block_language():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([
        _receipt(
            "memory_container_write",
            container_type="DC",
            container_id="DC-1",
            mem_id="MEM-1",
        ),
        _receipt(
            "memory_container_write",
            container_type="FUT",
            container_id="FUT-1",
            mem_id="MEM-FUT",
        ),
    ])

    prompt = tracker.render_prompt()

    assert "必须收束" not in prompt
    assert "处理不可跳过项" not in prompt


def test_spec272_closeout_form_generates_settlement_ledger_from_receipts():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([_receipt("memory_write", mem_id="MEM-REAL0001")])

    result = tracker.validate_closeout_form({
        "closeout_decision": "finish",
        "memory_status": "written",
        "memory_reason": "本轮已有真实写入回执。",
        "read_status": "not_applicable",
        "read_reason": "无读取任务。",
        "pending_status": "none",
        "pending_reason": "",
    })

    assert result["blocked"] is False
    assert result["settlement_ledger"]["memory_refs"] == ["MEM-REAL0001"]
    assert result["settlement_ledger"]["memory_status"] == "written"
    assert result["settlement_ledger"]["model_memory_status"] == "written"
    assert result["settlement_ledger"]["pending_resolution_result"] == "open"


def test_spec272_closeout_form_ledger_corrects_read_status_from_file_cursor():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_general_tool_results([{
        "tool_id": "file_read",
        "status": "ok",
        "path": "book.md",
        "truncated": True,
        "start_line": 1,
        "end_line": 120,
        "next_start_line": 121,
    }])

    result = tracker.validate_closeout_form({
        "closeout_decision": "continue",
        "handoff_text": "下一轮从 book.md 第 121 行继续读取。",
    })

    assert result["blocked"] is False
    assert result["settlement_ledger"]["read_status"] == "partial_continue"
    assert result["settlement_ledger"]["model_read_status"] == ""
    assert result["settlement_ledger"]["corrections"] == []


def test_spec272_closeout_form_blocks_continue_without_relay_fields():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()

    result = tracker.validate_closeout_form({
        "closeout_decision": "continue",
        "memory_status": "weight_zero",
        "memory_reason": "unit test",
        "read_status": "partial_continue",
        "read_reason": "unit test",
        "pending_status": "none",
        "pending_reason": "",
    })

    assert result["blocked"] is True
    assert "closeout_continue_requires_handoff_text" in result["reasons"]


def test_spec290_closeout_form_generates_ledger_without_model_status_fields():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([_receipt("memory_write", mem_id="MEM-REAL0002")])

    result = tracker.validate_closeout_form({
        "closeout_decision": "finish",
    })

    assert result["blocked"] is False
    ledger = result["settlement_ledger"]
    assert ledger["closeout_decision"] == "finish"
    assert ledger["memory_refs"] == ["MEM-REAL0002"]
    assert ledger["memory_status"] == "written"
    assert ledger["model_memory_status"] == ""
    assert ledger["read_status"] == "not_applicable"
    assert ledger["pending_status"] == "deferred"
    assert ledger["pending_resolution_result"] == "open"


def test_spec758_periodic_mount_blocked_closes_as_runtime_blocked():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.add_periodic_mount_blocked(
        "MEM-1234ABCD", "periodic_memory_budget_exceeded"
    )

    result = tracker.validate_closeout_form({
        "closeout_decision": "finish",
    })

    assert result["blocked"] is False
    ledger = result["settlement_ledger"]
    assert ledger["closeout_decision"] == "blocked"
    assert ledger["pending_status"] == "blocked"
    assert ledger["pending_resolution_result"] == "blocked"
    assert ledger["pending_obligations"][0]["obligation_type"] == (
        "periodic_memory_mount_blocked"
    )
    assert "closeout_decision_corrected:finish->blocked" in ledger["corrections"]
    prompt = tracker.render_prompt()
    assert "无需重试" in prompt
    assert "按 blocked 闭合本轮" in prompt


def test_spec758_actionable_reconsolidation_still_blocks_mount_failure_closeout():
    from logic.reaction_obligations import ReactionObligationTracker

    class PendingReconsolidation:
        def has_pending(self):
            return True

        def pending_ids(self):
            return ["MEM-AAAABBBB"]

        def audit_state(self):
            return {"pending_ids": self.pending_ids()}

    tracker = ReactionObligationTracker(
        memory_reconsolidation_tracker=PendingReconsolidation()
    )
    tracker.add_periodic_mount_blocked(
        "MEM-1234ABCD", "periodic_memory_budget_exceeded"
    )

    result = tracker.validate_closeout_form({
        "closeout_decision": "finish",
    })

    assert result["blocked"] is True
    assert "memory_reconsolidation_pending_unresolved:MEM-AAAABBBB" in (
        result["reasons"]
    )


def test_spec290_continue_handoff_text_settles_unfinished_file_cursor():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_general_tool_results([{
        "tool_id": "file_read",
        "status": "ok",
        "path": "book.md",
        "truncated": True,
        "start_line": 1,
        "end_line": 120,
        "next_start_line": 121,
    }])

    result = tracker.validate_closeout_form({
        "closeout_decision": "continue",
        "handoff_text": "下一轮从 book.md 第 121 行继续读取。",
    })

    assert result["blocked"] is False
    ledger = result["settlement_ledger"]
    assert ledger["read_refs"] == ["file_read:book.md:121"]
    assert ledger["read_status"] == "partial_continue"
    assert ledger["relay_receipt"] == {
        "handoff_text": "下一轮从 book.md 第 121 行继续读取。",
    }


def test_spec408_memory_route_prompt_does_not_use_runtime_mode_suggestion():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([_receipt("memory_write", mem_id="MEM-PRJ0001")])

    prompt = tracker.render_prompt()

    assert "工程任务优先判断是否需要 PRJ 项目容器" not in prompt
    assert "PRJ 记录项目目标、阶段、计划、交付物、验收和剩余工作" not in prompt
    assert "本轮理解若是对已有知识的推进/订正/补充" in prompt
    assert "若识别到多步任务/专项整理需求，应创建或挂入 PRJ 项目" in prompt
    assert "expected_container_focus" not in prompt
    assert "activity_mode" not in prompt


def test_spec290_continue_without_handoff_text_is_blocked_by_obligation_tracker():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()

    result = tracker.validate_closeout_form({
        "closeout_decision": "continue",
    })

    assert result["blocked"] is True
    assert "closeout_continue_requires_handoff_text" in result["reasons"]


def test_spec758_reconsolidation_is_unskippable_until_tracker_clears():
    from logic.reaction_obligations import ReactionObligationTracker

    class ReconsolidationTracker:
        pending = ["MEM-7460ABCD"]

        def has_pending(self):
            return bool(self.pending)

        def pending_ids(self):
            return list(self.pending)

        def audit_state(self):
            return {"pending_items": list(self.pending)}

    reconsolidation = ReconsolidationTracker()
    tracker = ReactionObligationTracker(
        memory_reconsolidation_tracker=reconsolidation
    )

    blocked = tracker.validate_closeout_form({"closeout_decision": "finish"})
    assert blocked["blocked"] is True
    assert "memory_reconsolidation_pending" in tracker.pending_types()

    reconsolidation.pending.clear()
    assert "memory_reconsolidation_pending" not in tracker.pending_types()
