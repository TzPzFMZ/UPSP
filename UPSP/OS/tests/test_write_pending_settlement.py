import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


def _schema_by_name(provider_tools, name):
    for tool in provider_tools:
        if tool.get("name") == name:
            return tool.get("parameters") or {}
        function = tool.get("function") or {}
        if function.get("name") == name:
            return function.get("parameters") or {}
        if tool.get("name") == name and tool.get("input_schema"):
            return tool.get("input_schema") or {}
    raise AssertionError(f"missing schema: {name}")


def test_spec341_schema_exports_pending_cancel_and_write_resolution_fields():
    from logic.native_tool_calls import export_provider_tool_schemas

    tools = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
    )
    names = {tool.get("name") for tool in tools}

    assert "pending_cancel" in names
    cancel_schema = _schema_by_name(tools, "pending_cancel")
    assert cancel_schema["required"] == ["pending_id", "reason_code"]
    assert cancel_schema["additionalProperties"] is False
    assert set(cancel_schema["properties"]) == {"pending_id", "reason_code", "note"}

    schema = _schema_by_name(tools, "memory_write")
    assert "resolves_pending_id" in schema["properties"]
    assert "resolves_pending_id" not in schema.get("required", [])


def test_spec341_failed_write_pending_cancel_fact_is_natural_language():
    from engines.reaction_helpers import format_protocol_tool_fact
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=635, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "missing_keywords",
        "call_id": "call_memory_write",
        "title": "错误写入",
        "subject": "unknown",
    }])

    pending = tracker.open_pendings()[0]
    assert pending["pending_id"] == "PEND-R000635-N001"
    receipt = tracker.cancel_pending({
        "pending_id": pending["pending_id"],
        "reason_code": "wrong_target",
        "note": "对象没有确认，放弃这次写入。",
    })

    assert receipt["status"] == "applied"
    assert tracker.open_pendings() == []

    fact = format_protocol_tool_fact(receipt)
    assert "这次取消已经生效" in fact
    assert "原写入没有成功" in fact
    assert "已经明确放弃这次写入意图" in fact
    assert "最终回复不能说已经写入" in fact
    assert "pending_id=" not in fact
    assert "reason_code=" not in fact
    assert "target_tool_id=" not in fact


def test_spec341_same_batch_cancel_must_be_consumed_before_finalize():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=635, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "missing_keywords",
        "call_id": "call_memory_write",
        "title": "对象错误写入",
        "subject": "unknown",
    }])
    pending_id = tracker.open_pendings()[0]["pending_id"]
    tracker.cancel_pending({
        "pending_id": pending_id,
        "reason_code": "obsolete_intent",
        "note": "焦点不可见，本次写入意图取消。",
    })

    blocker = tracker.finalize_blocker()
    assert blocker["blocked"] is True
    assert blocker["reason"] == "pending_cancel_result_not_consumed"

    tracker.mark_cancel_facts_consumed()
    assert tracker.finalize_blocker()["blocked"] is False


def test_spec595_retryable_failure_second_attempt_creates_one_pending_then_updates_it():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=635, round_type="interactive")
    first = [{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }]
    created = tracker.observe_receipts(first)

    assert len(created) == 1
    assert created[0].get("pending_id") in (None, "")
    assert "write_pending_id" not in first[0]
    assert tracker.open_pendings() == []
    assert len(tracker.open_retry_shadows()) == 1

    created = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "memory_body_too_long:max=512;actual=800",
        "call_id": "call_memory_write_retry",
        "title": "超长写入",
        "subject": "Codex",
    }])

    assert len(created) == 1
    pending_id = created[0]["pending_id"]
    assert created[0]["pending_id"] == pending_id
    open_pendings = tracker.open_pendings()
    assert len(open_pendings) == 1
    assert open_pendings[0]["pending_id"] == pending_id
    assert open_pendings[0]["reason"] == "memory_body_too_long:max=512;actual=800"
    assert open_pendings[0]["call_id"] == "call_memory_write_retry"
    assert open_pendings[0]["receipt"]["write_pending_id"] == pending_id
    assert open_pendings[0]["settlement_stage"] == "settlement_required"
    assert open_pendings[0]["retry_count"] == 1

    updated = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "memory_body_too_long:max=512;actual=700",
        "call_id": "call_memory_write_third",
        "title": "超长写入",
        "subject": "Codex",
    }])

    assert len(updated) == 1
    open_pendings = tracker.open_pendings()
    assert len(open_pendings) == 1
    assert open_pendings[0]["pending_id"] == pending_id
    assert open_pendings[0]["reason"] == "memory_body_too_long:max=512;actual=700"
    assert open_pendings[0]["call_id"] == "call_memory_write_third"
    assert open_pendings[0]["settlement_stage"] == "settlement_required"


def test_spec595_repeated_same_key_failures_do_not_stack_pending_ids():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=695, round_type="interactive")
    for idx, actual in enumerate([1444, 900, 850, 800, 700], start=1):
        created = tracker.observe_receipts([{
            "tool_id": "memory_write",
            "status": "error",
            "reason": f"memory_body_too_long:max=512;actual={actual}",
            "call_id": f"call_memory_write_{idx}",
            "title": "SealGate-01 综合试炼推进方法",
            "subject": "Codex",
        }])
        if idx == 1:
            assert tracker.open_pendings() == []
            assert len(tracker.open_retry_shadows()) == 1
            assert created[0].get("pending_id") in (None, "")
        else:
            assert len(created) == 1
            assert created[0]["pending_id"] == "PEND-R000695-N001"
            assert created[0]["settlement_stage"] == "settlement_required"
            assert tracker.open_retry_shadows() == []

    open_pendings = tracker.open_pendings()
    assert len(open_pendings) == 1
    assert open_pendings[0]["pending_id"] == "PEND-R000695-N001"
    assert open_pendings[0]["call_id"] == "call_memory_write_5"
    assert open_pendings[0]["reason"] == "memory_body_too_long:max=512;actual=700"


def test_spec595_different_title_or_subject_failures_keep_separate_retry_chains():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=696, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1000",
        "call_id": "call_memory_write_a1",
        "title": "任务清单推进方法",
        "subject": "Codex",
    }])
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1000",
        "call_id": "call_memory_write_b1",
        "title": "读书沉淀方法",
        "subject": "Codex",
    }])

    assert tracker.open_pendings() == []
    assert len(tracker.open_retry_shadows()) == 2

    created = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=800",
        "call_id": "call_memory_write_a2",
        "title": "任务清单推进方法",
        "subject": "Codex",
    }])

    assert len(created) == 1
    assert created[0]["pending_id"] == "PEND-R000696-N001"
    assert len(tracker.open_pendings()) == 1
    assert len(tracker.open_retry_shadows()) == 1
    assert tracker.open_retry_shadows()[0]["title"] == "读书沉淀方法"


def test_spec354_first_memory_body_too_long_keeps_visible_guidance_minimal():
    from engines.reaction_helpers import format_protocol_tool_fact
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_write_pending_notice,
        render_open_write_pending_warning,
    )

    tracker = WritePendingTracker(round_num=653, round_type="interactive")
    receipts = [{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }]
    created = tracker.observe_receipts(receipts)

    assert len(created) == 1
    assert created[0].get("pending_id") in (None, "")
    assert "write_pending_id" not in receipts[0]
    assert "resolves_pending_id" not in receipts[0]
    assert tracker.open_pendings() == []
    shadows = tracker.open_retry_shadows()
    assert len(shadows) == 1
    assert shadows[0]["settlement_stage"] == "retry_required"

    fact = format_protocol_tool_fact(receipts[0])
    notice = format_write_pending_notice(created)
    warning = render_open_write_pending_warning(shadows)
    visible = "\n".join([fact, notice, warning])

    assert "memory_write.body 超出当前权重上限" in visible
    assert "actual=1444, max=512" in visible
    assert "压缩正文" in visible
    assert "调整 weight" in visible
    assert "不要只因字数升权" in visible
    assert "PEND-R" not in visible
    assert "resolves_pending_id" not in visible
    assert "pending_cancel" not in visible
    assert "价值依据" not in visible
    assert "取消出口" not in visible
    assert "提醒 ID" not in visible
    assert "自动消失" not in visible
    assert "不进入上下文" not in visible


def test_spec354_retry_required_blocker_only_says_rewrite_and_cannot_finalize():
    from logic.write_pending_settlement import (
        WritePendingTracker,
        render_write_pending_blocker,
    )

    tracker = WritePendingTracker(round_num=654, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }])

    blocker = tracker.finalize_blocker()
    text = render_write_pending_blocker(blocker)

    assert "失败写入需要重写" in text
    assert "重新调用 memory_write" in text
    assert "不能收束" in text
    assert "pending_cancel" not in text
    assert "取消" not in text
    assert "取消出口" not in text


def test_spec417_subject_failure_first_time_requires_rewrite_without_pending_id():
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_write_pending_notice,
        render_open_write_pending_warning,
    )

    tracker = WritePendingTracker(round_num=653, round_type="interactive")
    created = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "subject_not_confirmed",
        "call_id": "call_memory_write",
        "title": "对象未确认",
        "subject": "Other",
        "submitted_subject": "Other",
        "confirmed_subject": "TzPz",
        "confirmed_subjects": ["TzPz"],
    }])

    assert len(created) == 1
    assert created[0].get("pending_id") in (None, "")
    assert tracker.open_pendings() == []
    shadows = tracker.open_retry_shadows()
    assert len(shadows) == 1
    assert shadows[0]["settlement_stage"] == "retry_required"

    visible = "\n".join([
        format_write_pending_notice(created),
        render_open_write_pending_warning(shadows),
    ])

    assert "PEND-R" not in visible
    assert "pending_cancel" not in visible
    assert "resolves_pending_id" not in visible
    assert "提交主题：Other" in visible
    assert "当前确认对象：TzPz" in visible
    assert "NO-GO" in visible
    assert "identity_resolution" not in visible
    assert "relation_read" not in visible
    assert "relation_card_write" not in visible
    assert "重新调用 memory_write" not in visible
    assert "查看 POPUP" not in visible
    assert "memory_body_too_long" not in visible


def test_spec606_first_subject_failure_can_defer_to_user_without_success_claim():
    from logic.write_pending_settlement import (
        WritePendingTracker,
        render_open_write_pending_warning,
    )

    tracker = WritePendingTracker(round_num=606, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "subject_not_in_relation_domain",
        "call_id": "call_spec606_subject",
        "title": "对象未确认",
        "subject": "Other",
        "submitted_subject": "Other",
        "confirmed_subject": "Codex",
        "confirmed_subjects": ["Codex"],
    }])

    blocker = tracker.finalize_blocker()
    shadows = tracker.open_retry_shadows()
    warning = render_open_write_pending_warning(shadows)

    assert blocker["blocked"] is False
    assert blocker["reason"] == "subject_resolution_unresolved"
    assert len(blocker["deferred_subject_resolution"]) == 1
    assert len(shadows) == 1
    assert shadows[0]["status"] == "open"
    assert shadows[0]["source_status"] == "rejected"
    assert "可以自然回复请求用户确认或说明 NO-GO 阻断" in warning
    assert "不得声称写入成功" in warning
    assert "不得为缺席或无关第三方自动创建关系卡" in warning
    assert "在这次失败写入修正前不能自然声称完成" not in warning


def test_spec606_identity_unresolved_first_shadow_can_defer_but_other_failures_block():
    from logic.write_pending_settlement import WritePendingTracker

    identity = WritePendingTracker(round_num=606, round_type="interactive")
    identity.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "identity_unresolved",
        "call_id": "call_spec606_identity",
        "title": "身份未确认",
        "subject": "unknown",
    }])
    assert identity.finalize_blocker()["blocked"] is False

    body = WritePendingTracker(round_num=606, round_type="interactive")
    body.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=900",
        "call_id": "call_spec606_body",
        "title": "超长写入",
        "subject": "Codex",
    }])
    assert body.finalize_blocker()["blocked"] is True


def test_spec606_second_subject_failure_explicit_pending_still_blocks():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=606, round_type="interactive")
    for index in (1, 2):
        tracker.observe_receipts([{
            "tool_id": "memory_write",
            "status": "rejected",
            "reason": "subject_not_confirmed",
            "call_id": f"call_spec606_subject_{index}",
            "title": "对象未确认",
            "subject": "Other",
            "submitted_subject": "Other",
            "confirmed_subject": "TzPz",
        }])

    blocker = tracker.finalize_blocker()

    assert blocker["blocked"] is True
    assert blocker["reason"] == "write_pending_unresolved"
    assert len(blocker["pendings"]) == 1
    assert blocker["pendings"][0]["pending_id"].startswith("PEND-R")


def test_spec417_subject_failure_second_time_escalates_with_actionable_pending():
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_write_pending_notice,
        render_open_write_pending_warning,
    )

    tracker = WritePendingTracker(round_num=653, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "subject_not_confirmed",
        "call_id": "call_memory_write_first",
        "title": "对象未确认",
        "subject": "Other",
        "submitted_subject": "Other",
        "confirmed_subject": "TzPz",
        "confirmed_subjects": ["TzPz"],
    }])

    escalated = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "subject_not_confirmed",
        "call_id": "call_memory_write_second",
        "title": "对象未确认",
        "subject": "Other",
        "submitted_subject": "Other",
        "confirmed_subject": "TzPz",
        "confirmed_subjects": ["TzPz"],
    }])

    open_pendings = tracker.open_pendings()
    pending_id = open_pendings[0]["pending_id"]
    assert len(escalated) == 1
    assert escalated[0]["pending_id"] == pending_id
    assert open_pendings[0]["settlement_stage"] == "settlement_required"

    visible = "\n".join([
        format_write_pending_notice(escalated),
        render_open_write_pending_warning(open_pendings),
    ])

    assert pending_id in visible
    assert "resolves_pending_id" in visible
    assert "pending_cancel" in visible
    assert "提交主题：Other" in visible
    assert "当前确认对象：TzPz" in visible
    assert "NO-GO" in visible
    assert "identity_resolution" not in visible
    assert "relation_read" not in visible
    assert "relation_card_write" not in visible
    assert "查看 POPUP" not in visible


def test_spec406_chronicle_no_active_focus_does_not_create_pending():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=617, round_type="rhythm")
    created = tracker.observe_receipts([{
        "tool_id": "chronicle_write",
        "status": "rejected",
        "reason": "no_active_chronicle_focus",
        "call_id": "call_chronicle_stale_focus",
        "layer": "daily",
        "round_type": "rhythm",
    }])

    assert created == []
    assert tracker.open_pendings() == []
    assert tracker.finalize_blocker()["blocked"] is False


def test_spec406_non_cancellable_pending_never_mentions_pending_cancel():
    from logic.write_pending_settlement import (
        format_write_pending_notice,
        render_open_write_pending_warning,
        render_write_pending_blocker,
    )

    pending = {
        "pending_id": "PEND-R000617-N001",
        "status": "open",
        "settlement_stage": "settlement_required",
        "cancel_available": False,
        "target_tool_id": "chronicle_write",
        "reason": "no_active_chronicle_focus",
        "call_id": "call_chronicle_stale_focus",
        "mandatory": False,
    }
    visible = "\n".join([
        format_write_pending_notice([pending]),
        render_open_write_pending_warning([pending]),
        render_write_pending_blocker({
            "blocked": True,
            "reason": "write_pending_unresolved",
            "pendings": [pending],
        }),
    ])

    assert "pending_cancel" not in visible
    assert "取消" not in visible
    assert "resolves_pending_id" in visible


def test_spec361_relation_body_not_visible_is_precondition_not_cancellable_pending():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=662, round_type="interactive")
    created = tracker.observe_receipts([{
        "tool_id": "relation_card_write",
        "status": "rejected",
        "reason": "relation_body_not_visible",
        "call_id": "call_relation_write",
        "card_id": "REL-CODEX-TZPZ",
    }])

    assert created == []
    assert tracker.open_pendings() == []
    assert tracker.finalize_blocker()["blocked"] is False


def test_spec595_successful_same_key_retry_clears_shadow_without_id():
    from engines.reaction_helpers import format_protocol_tool_fact
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=653, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }])
    assert tracker.open_pendings() == []
    assert len(tracker.open_retry_shadows()) == 1

    retry_receipts = [{
        "tool_id": "memory_write",
        "status": "applied",
        "call_id": "call_memory_write_retry",
        "mem_id": "MEM-00065301",
        "title": "超长写入",
        "weight": 3,
        "subject": "Codex",
    }]
    created = tracker.observe_receipts(retry_receipts)

    assert created == []
    assert tracker.open_pendings() == []
    assert tracker.open_retry_shadows() == []
    resolved = tracker.consume_recently_resolved_pendings()
    assert resolved == []

    fact = format_protocol_tool_fact(retry_receipts[0])
    assert "MEM-00065301" in fact
    assert "PEND-R" not in fact
    assert "resolves_pending_id" not in fact
    assert "pending_cancel" not in fact


def test_spec595_successful_retry_removes_first_shadow_notice_without_id():
    from engines.reaction_helpers import (
        attach_native_trace_to_receipts,
        remove_settled_write_pending_context,
    )
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_write_pending_notice,
        write_pending_notice_call_ids,
    )

    tracker = WritePendingTracker(round_num=653, round_type="interactive")
    accumulated_messages = []
    pending_feedbacks = []

    first = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }])
    accumulated_messages.append({
        "role": "user",
        "kind": "tool_fact",
        "content": format_write_pending_notice(first),
        "_tool_fact_receipt_call_ids": write_pending_notice_call_ids(first),
    })

    second = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=900",
        "call_id": "call_memory_write_second",
        "title": "超长写入",
        "subject": "Codex",
    }])
    pending_id = tracker.open_pendings()[0]["pending_id"]
    accumulated_messages.append({
        "role": "user",
        "kind": "tool_fact",
        "content": format_write_pending_notice(second),
        "_tool_fact_receipt_call_ids": write_pending_notice_call_ids(second),
    })

    retry_receipts = [{
        "tool_id": "memory_write",
        "status": "applied",
        "call_id": "call_memory_write_retry",
        "mem_id": "MEM-00065301",
        "title": "超长写入",
        "weight": 3,
        "subject": "Codex",
    }]
    attach_native_trace_to_receipts(retry_receipts, [{
        "call_id": "call_memory_write_retry",
        "resolves_pending_id": pending_id,
    }])
    tracker.observe_receipts(retry_receipts)

    remove_settled_write_pending_context(
        accumulated_messages,
        pending_feedbacks,
        tracker.consume_recently_resolved_pendings(),
    )

    visible_context = "\n".join(
        item["content"] for item in accumulated_messages
    )
    assert "失败写入需要重写" not in visible_context
    assert "失败写入待结算" not in visible_context
    assert "memory_write.body 超出当前权重上限" not in visible_context
    assert pending_id not in visible_context


def test_spec595_second_memory_body_too_long_escalates_to_explicit_pending():
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_write_pending_notice,
        render_open_write_pending_warning,
    )

    tracker = WritePendingTracker(round_num=653, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }])
    assert tracker.open_pendings() == []
    assert len(tracker.open_retry_shadows()) == 1

    second = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=900",
        "call_id": "call_memory_write_second",
        "title": "超长写入",
        "subject": "Codex",
    }])

    assert len(second) == 1
    pending_id = second[0]["pending_id"]
    assert second[0]["pending_id"] == pending_id
    assert second[0]["settlement_stage"] == "settlement_required"

    updated = tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=850",
        "call_id": "call_memory_write_third",
        "title": "超长写入",
        "subject": "Codex",
    }])

    assert len(updated) == 1
    assert updated[0]["pending_id"] == pending_id
    assert updated[0]["settlement_stage"] == "settlement_required"
    open_pendings = tracker.open_pendings()
    assert len(open_pendings) == 1
    assert open_pendings[0]["pending_id"] == pending_id
    assert open_pendings[0]["reason"] == "memory_body_too_long:max=512;actual=850"

    visible = "\n".join([
        format_write_pending_notice(updated),
        render_open_write_pending_warning(open_pendings),
    ])
    assert pending_id in visible
    assert "resolves_pending_id" in visible
    assert "pending_cancel" in visible
    assert "压缩正文" in visible
    assert "不要只因字数升权" in visible
    assert "价值依据" not in visible


def test_spec595_first_retry_shadow_has_no_cancel_target():
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_pending_cancel_tool_fact,
    )

    tracker = WritePendingTracker(round_num=653, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }])
    assert tracker.open_pendings() == []
    assert len(tracker.open_retry_shadows()) == 1

    receipt = tracker.cancel_pending({
        "pending_id": "PEND-R000653-N001",
        "reason_code": "obsolete_intent",
    })

    assert receipt["status"] == "not_found_or_settled"
    assert receipt["reason"] == "pending_not_found_or_settled"
    assert tracker.open_pendings() == []
    assert len(tracker.open_retry_shadows()) == 1
    fact = format_pending_cancel_tool_fact(receipt)
    assert "未发现此提醒" in fact
    assert "提醒 ID" not in fact


def test_spec347_successful_retry_resolves_pending_and_hides_reminder_id():
    from engines.reaction_helpers import (
        attach_native_trace_to_receipts,
        format_protocol_tool_fact,
        remove_settled_write_pending_context,
    )
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_write_pending_notice,
        render_open_write_pending_warning,
    )

    tracker = WritePendingTracker(round_num=645, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1797",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }])
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1200",
        "call_id": "call_memory_write_second",
        "title": "超长写入",
        "subject": "Codex",
    }])
    pending = tracker.open_pendings()[0]
    pending_id = pending["pending_id"]
    assert pending_id == "PEND-R000645-N001"

    retry_receipts = [{
        "tool_id": "memory_write",
        "status": "applied",
        "call_id": "call_memory_write_retry",
        "mem_id": "MEM-0552E3A9",
        "title": "超长写入",
        "weight": 4,
        "subject": "Codex",
    }]
    attach_native_trace_to_receipts(retry_receipts, [{
        "call_id": "call_memory_write_retry",
        "resolves_pending_id": pending_id,
    }])
    tracker.observe_receipts(retry_receipts)

    assert tracker.open_pendings() == []
    assert tracker.cancelled_pendings() == []
    assert format_write_pending_notice(tracker.open_pendings()) == ""
    assert render_open_write_pending_warning(tracker.open_pendings()) == ""

    fact = format_protocol_tool_fact(retry_receipts[0])
    assert "成功补写" in fact
    assert "MEM-0552E3A9" in fact
    assert pending_id not in fact
    assert "失败写入待结算" not in fact

    accumulated_messages = [{
        "role": "user",
        "kind": "tool_fact",
        "content": "【本轮记忆写入回执】\n处理结果：error。\n标题：超长写入。",
        "_tool_fact_receipt_call_ids": ["call_memory_write_first"],
    }, {
        "role": "user",
        "kind": "tool_fact",
        "content": f"【失败写入待结算】\nmemory_write 没有成功，已登记为 {pending_id}。",
    }, {
        "role": "user",
        "kind": "tool_fact",
        "content": fact,
    }]
    popup_feedbacks = [
        f"memory_write 本次写入没有成功，{pending_id} 是这次失败写入提醒 ID。"
    ]
    remove_settled_write_pending_context(
        accumulated_messages,
        popup_feedbacks,
        tracker.consume_recently_resolved_pendings(),
    )

    visible_context = "\n".join(
        [item["content"] for item in accumulated_messages] + popup_feedbacks
    )
    assert "处理结果：error" not in visible_context
    assert "失败写入待结算" not in visible_context
    assert pending_id not in visible_context
    assert "MEM-0552E3A9" in visible_context


def test_spec352_alert_settlement_mandatory_uses_alert_status_not_recorded():
    from logic.write_pending_settlement import is_mandatory_write

    assert is_mandatory_write({
        "tool_id": "alert_mode_settle",
        "status": "error",
        "alert_status": "deferred",
        "reason": "processor failed after deferred intent",
    }) is True

    assert is_mandatory_write({
        "tool_id": "alert_mode_settle",
        "status": "error",
        "alert_status": "recorded",
        "reason": "retired alert status",
    }) is False


def test_spec347_cancel_resolved_or_missing_pending_is_short_noop():
    from engines.reaction_helpers import attach_native_trace_to_receipts
    from logic.write_pending_settlement import (
        WritePendingTracker,
        format_pending_cancel_tool_fact,
        render_cancelled_write_warning,
    )

    tracker = WritePendingTracker(round_num=645, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1797",
        "call_id": "call_memory_write_first",
        "title": "超长写入",
        "subject": "Codex",
    }])
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "error",
        "reason": "memory_body_too_long:max=512;actual=1200",
        "call_id": "call_memory_write_second",
        "title": "超长写入",
        "subject": "Codex",
    }])
    pending_id = tracker.open_pendings()[0]["pending_id"]
    retry_receipts = [{
        "tool_id": "memory_write",
        "status": "applied",
        "call_id": "call_memory_write_retry",
        "mem_id": "MEM-0552E3A9",
        "title": "超长写入",
        "weight": 4,
        "subject": "Codex",
    }]
    attach_native_trace_to_receipts(retry_receipts, [{
        "call_id": "call_memory_write_retry",
        "resolves_pending_id": pending_id,
    }])
    tracker.observe_receipts(retry_receipts)

    receipt = tracker.cancel_pending({
        "pending_id": pending_id,
        "reason_code": "superseded_by_retry",
        "note": "试图取消已补写的提醒。",
    })

    assert receipt["status"] == "not_found_or_settled"
    assert receipt["reason"] == "pending_not_found_or_settled"
    assert tracker.cancelled_pendings() == []
    assert tracker.finalize_blocker()["blocked"] is False
    fact = format_pending_cancel_tool_fact(receipt)
    assert "未发现此提醒" in fact
    assert "已结清" in fact
    assert "取消已经生效" not in fact
    assert pending_id not in fact
    assert render_cancelled_write_warning(tracker.cancelled_pendings()) == ""

    missing_receipt = tracker.cancel_pending({
        "pending_id": "PEND-R000645-N999",
        "reason_code": "wrong_target",
    })
    assert missing_receipt["status"] == "not_found_or_settled"
    missing_fact = format_pending_cancel_tool_fact(missing_receipt)
    assert "未发现此提醒" in missing_fact
    assert "PEND-R000645-N999" not in missing_fact


def test_spec343_cancelled_pending_warning_state_forbids_success_claims():
    from logic.write_pending_settlement import (
        WritePendingTracker,
        render_cancelled_write_warning,
    )

    tracker = WritePendingTracker(round_num=638, round_type="interactive")
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "memory_body_too_long:max=512;actual=1444",
        "call_id": "call_memory_write_first",
        "title": "位置路径阈值",
        "subject": "Codex",
    }])
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "memory_body_too_long:max=512;actual=900",
        "call_id": "call_memory_write_second",
        "title": "位置路径阈值",
        "subject": "Codex",
    }])
    pending_id = tracker.open_pendings()[0]["pending_id"]
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "rejected",
        "reason": "memory_body_too_long:max=512;actual=850",
        "call_id": "call_memory_write_third",
        "title": "位置路径阈值",
        "subject": "Codex",
    }])

    tracker.cancel_pending({
        "pending_id": pending_id,
        "reason_code": "low_value",
        "note": "放弃这次写入。",
    })

    cancelled = tracker.cancelled_pendings()
    assert len(cancelled) == 1
    assert cancelled[0]["pending_id"] == pending_id
    warning = render_cancelled_write_warning(cancelled)
    assert pending_id in warning
    assert "没有补写" in warning
    assert "对应记忆不存在" in warning


def test_spec341_mandatory_rhythm_chronicle_pending_cannot_cancel():
    from logic.write_pending_settlement import WritePendingTracker

    tracker = WritePendingTracker(round_num=635, round_type="rhythm")
    tracker.observe_receipts([{
        "tool_id": "chronicle_write",
        "status": "rejected",
        "reason": "write_error",
        "call_id": "call_chronicle",
        "layer": "rhythms",
        "round_type": "rhythm",
    }])

    pending = tracker.open_pendings()[0]
    assert pending["mandatory"] is True
    receipt = tracker.cancel_pending({
        "pending_id": pending["pending_id"],
        "reason_code": "obsolete_intent",
        "note": "节律编年史不能取消。",
    })

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "pending_cancel_forbidden"
    assert tracker.open_pendings()[0]["pending_id"] == pending["pending_id"]


class TestSpec341CleanupFlags(RuntimeTestMixin):
    def test_rhythm_chronicle_failure_does_not_clear_rhythm_due(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.sm.set("base.heartbeat_flags.rhythm_due", True)
        rt.sm.set("base.meta.last_rhythm_round", 632)

        rt.cleanup_pipeline._finalize_flags(
            rt.sm.load(),
            "rhythm",
            633,
            result={
                "_chronicle_write_receipts": [{
                    "tool_id": "chronicle_write",
                    "status": "rejected",
                    "layer": "rhythms",
                    "round_type": "rhythm",
                }],
            },
        )

        assert rt.sm.get("base.heartbeat_flags.rhythm_due") is True
        assert rt.sm.get("base.meta.last_rhythm_round") == 632
