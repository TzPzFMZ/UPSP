import hashlib
import json
from pathlib import Path


def _create_task(store, *, evidence_refs=None):
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    create_task_bootstrap_guide(store, reason="用户交付一个可执行任务")
    receipt = apply_guide_submit(store, {
        "guide_id": "task_bootstrap",
        "item_id": "build_initial_task_guide",
        "option_id": "submit_initial_guide",
        "fields": {
            "task_title": "建立报告",
            "task_goal": "根据材料输出报告",
            "source_refs": ["input/brief.md"],
            "items": [{"item_id": "item_01", "title": "读取并整理材料"}],
            "acceptance": [{
                "acceptance_id": "acc_01",
                "description": "报告内容完整",
                "item_refs": ["item_01"],
            }],
        },
        "evidence_refs": list(evidence_refs or []),
    })
    assert receipt["status"] == "accepted"
    return store.get("base.active_task")


def _revise(store, task_id, fields, *, reason="工具结果改变了任务结构"):
    from logic.guide_submit import apply_guide_submit

    return apply_guide_submit(store, {
        "guide_id": f"task:{task_id}",
        "item_id": "task_progress",
        "option_id": "revise_task_plan",
        "reason": reason,
        "fields": fields,
    })


def _task_guide_path(store, task_id):
    return Path(store._find_task_dir(task_id, zone="process")) / "task_guide.json"


def test_spec780_bootstrap_source_pointer_is_not_evidence(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store, evidence_refs=["EV-OUTER"])
    guide = store.load_task_guide(task_id)

    assert guide["source_refs"] == ["input/brief.md"]
    assert "EV-OUTER" not in guide["source_refs"]
    assert guide["source_requirements"] == []
    assert store.load_source_read_evidence() == []
    options = store.load_active_guide()["items"][0]["options"]
    assert "revise_task_plan" in {item["option_id"] for item in options}


def test_spec780_unread_plan_source_cannot_set_blocked_status(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "item_id": "task_progress",
            "option_id": "update_task_status",
            "fields": {
                "items": {
                    "item_01": {
                        "status": "blocked",
                        "reason": "来源尚未读取",
                        "evidence_refs": ["input/brief.md"],
                    },
                },
            },
        },
        evidence_context={"prior_general_tool_results": []},
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_blocked_evidence_not_found"
    assert store.load_task_guide(task_id)["items"][0]["status"] == "open"


def test_spec780_revision_applies_full_sections_and_audits_diff(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    receipt = _revise(store, task_id, {
        "source_refs": ["input/brief.md", "input/data.csv"],
        "source_requirements": [{
            "requirement_id": "req_01",
            "source_ref": "input/data.csv",
            "summary": "核验数据表",
        }],
        "items": [
            {
                "item_id": "item_02",
                "title": "核验数据",
                "requirement_refs": ["req_01"],
            },
            {"item_id": "item_01", "title": "读取并整理材料"},
        ],
        "acceptance": [
            {
                "acceptance_id": "acc_02",
                "description": "数据已经核验",
                "item_refs": ["item_02"],
                "requirement_refs": ["req_01"],
            },
            {
                "acceptance_id": "acc_01",
                "description": "报告内容完整",
                "item_refs": ["item_01"],
            },
        ],
        "risk_notes": ["数据缺失时明确报告边界"],
    })

    assert receipt["status"] == "accepted"
    revision = receipt["task_plan_revision"]
    assert revision["schema_version"] == "task_plan_revision.v1"
    assert revision["action"] == "applied"
    assert revision["before_sha256"] != revision["after_sha256"]
    assert "--- task-plan:before" in revision["unified_diff"]
    assert "+      \"item_id\": \"item_02\"" in revision["unified_diff"]

    guide = store.load_task_guide(task_id)
    assert guide["task_title"] == "建立报告"
    assert guide["task_goal"] == "根据材料输出报告"
    assert [item["item_id"] for item in guide["items"]] == ["item_02", "item_01"]
    assert guide["items"][0]["status"] == "open"
    assert guide["acceptance"][0]["status"] == "pending"

    ledger_path = Path(store.guides_dir) / f"task__colon__{task_id}" / "ledger.jsonl"
    ledger = [json.loads(line) for line in ledger_path.read_text(
        encoding="utf-8"
    ).splitlines() if line.strip()]
    assert ledger[-1]["task_plan_revision"] == revision


def test_spec780_revision_preserves_existing_lifecycle_and_accepts_noop(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    current = store.load_task_guide(task_id)
    current["items"][0].update({
        "status": "blocked",
        "evidence_refs": ["call:blocked-1"],
        "reason": "等待权限",
    })
    store.save_task_guide(task_id, current)

    receipt = _revise(store, task_id, {
        "items": [{"item_id": "item_01", "title": "读取并整理材料"}],
    })

    assert receipt["task_plan_revision"]["action"] == "noop"
    item = store.load_task_guide(task_id)["items"][0]
    assert item["status"] == "blocked"
    assert item["evidence_refs"] == ["call:blocked-1"]
    assert item["reason"] == "等待权限"


def test_spec780_revision_cannot_remove_or_change_settled_record(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    current = store.load_task_guide(task_id)
    current["items"][0].update({
        "status": "done",
        "evidence_refs": ["EV-DONE"],
    })
    store.save_task_guide(task_id, current)
    before = _task_guide_path(store, task_id).read_bytes()

    removed = _revise(store, task_id, {
        "items": [{"item_id": "item_02", "title": "替代项"}],
        "acceptance": [{
            "acceptance_id": "acc_01",
            "description": "报告内容完整",
            "item_refs": ["item_02"],
        }],
    })

    assert removed["status"] == "rejected"
    assert removed["reason"] == "task_plan_revision_settled_record_immutable"
    assert removed["details"] == {"items": ["item_01"]}
    assert _task_guide_path(store, task_id).read_bytes() == before

    changed = _revise(store, task_id, {
        "items": [{"item_id": "item_01", "title": "改写已完成语义"}],
    })
    assert changed["reason"] == "task_plan_revision_settled_record_immutable"
    assert _task_guide_path(store, task_id).read_bytes() == before


def test_spec780_legacy_verified_item_is_also_protected(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    current = store.load_task_guide(task_id)
    current["items"][0].update({
        "status": "verified",
        "evidence_refs": ["EV-LEGACY"],
    })
    store.save_task_guide(task_id, current)

    receipt = _revise(store, task_id, {
        "items": [{"item_id": "item_01", "title": "试图改写旧终态"}],
    })

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_plan_revision_settled_record_immutable"


def test_spec780_revision_cannot_remove_or_change_passed_acceptance(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    current = store.load_task_guide(task_id)
    current["acceptance"][0].update({
        "status": "passed",
        "evidence_refs": ["EV-PASSED"],
        "reason": "已经核验",
    })
    store.save_task_guide(task_id, current)
    before = _task_guide_path(store, task_id).read_bytes()

    removed = _revise(store, task_id, {
        "acceptance": [{
            "acceptance_id": "acc_02",
            "description": "替代验收",
            "item_refs": ["item_01"],
        }],
    })
    assert removed["reason"] == "task_plan_revision_settled_record_immutable"
    assert removed["details"] == {"acceptance": ["acc_01"]}

    changed = _revise(store, task_id, {
        "acceptance": [{
            "acceptance_id": "acc_01",
            "description": "改写已经通过的验收语义",
            "item_refs": ["item_01"],
        }],
    })
    assert changed["reason"] == "task_plan_revision_settled_record_immutable"
    assert _task_guide_path(store, task_id).read_bytes() == before


def test_spec780_revision_rejects_lifecycle_and_dangling_refs_without_write(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    path = _task_guide_path(store, task_id)
    before_sha = hashlib.sha256(path.read_bytes()).hexdigest()

    lifecycle = _revise(store, task_id, {
        "items": [{
            "item_id": "item_01",
            "title": "读取并整理材料",
            "status": "done",
            "evidence_refs": ["EV-FAKE"],
        }],
    })
    assert lifecycle["reason"] == "task_plan_revision_lifecycle_fields_forbidden"

    dangling = _revise(store, task_id, {
        "acceptance": [{
            "acceptance_id": "acc_01",
            "description": "报告内容完整",
            "item_refs": ["item_missing"],
        }],
    })
    assert dangling["reason"] == "bootstrap_acceptance_refs_unknown"
    assert hashlib.sha256(path.read_bytes()).hexdigest() == before_sha


def test_spec780_source_requirement_mixed_shape_is_not_silently_dropped(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户交付一个可执行任务")
    receipt = apply_guide_submit(store, {
        "guide_id": "task_bootstrap",
        "item_id": "build_initial_task_guide",
        "option_id": "submit_initial_guide",
        "fields": {
            "task_title": "拒绝混合来源需求",
            "source_refs": ["input/brief.md"],
            "source_requirements": [
                {
                    "requirement_id": "req_01",
                    "source_ref": "input/brief.md",
                    "summary": "读取任务说明",
                },
                "这不是合法来源需求对象",
            ],
            "items": [{
                "item_id": "item_01",
                "title": "执行任务",
                "requirement_refs": ["req_01"],
            }],
            "acceptance": [{
                "acceptance_id": "acc_01",
                "description": "任务完成",
                "item_refs": ["item_01"],
            }],
        },
    })

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_invalid_source_requirements"
    assert store.get("base.active_task") in (None, "")


def test_spec780_source_refs_reject_non_string_values(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户交付一个可执行任务")
    receipt = apply_guide_submit(store, {
        "guide_id": "task_bootstrap",
        "item_id": "build_initial_task_guide",
        "option_id": "submit_initial_guide",
        "fields": {
            "task_title": "拒绝伪来源字符串",
            "source_refs": [{"path": "input/brief.md"}],
            "items": [{"item_id": "item_01", "title": "执行任务"}],
            "acceptance": [{
                "acceptance_id": "acc_01",
                "description": "任务完成",
                "item_refs": ["item_01"],
            }],
        },
    })

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_plan_text_list_invalid"


def test_spec780_revision_requires_reason_and_rolls_back_write_failure(tmp_path, monkeypatch):
    from data.workbench import WorkbenchStore
    from errors import WriteError

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    path = _task_guide_path(store, task_id)
    before = path.read_bytes()

    missing_reason = _revise(
        store,
        task_id,
        {"risk_notes": ["新增风险"]},
        reason="",
    )
    assert missing_reason["reason"] == "task_plan_revision_reason_required"

    def fail_save(*_args, **_kwargs):
        raise WriteError(str(path))

    monkeypatch.setattr(store, "save_task_guide", fail_save)
    failed = _revise(store, task_id, {"risk_notes": ["新增风险"]})
    assert failed["reason"] == "task_plan_revision_write_failed"
    assert path.read_bytes() == before


def test_spec780_internal_revision_rejects_unknown_fields(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import apply_task_plan_revision

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    path = _task_guide_path(store, task_id)
    before = path.read_bytes()

    receipt = apply_task_plan_revision(
        store,
        task_id,
        {"task_goal": "绕过活动 guide schema 改写目标"},
        reason="不应生效",
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_plan_revision_fields_unknown"
    assert receipt["details"]["fields"] == ["task_goal"]
    assert path.read_bytes() == before


def test_spec780_revision_fails_closed_when_current_truth_is_missing(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import apply_task_plan_revision

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    receipt = apply_task_plan_revision(
        store,
        "T-20990101-99",
        {"risk_notes": ["无法读取真源"]},
        reason="尝试修订不存在的任务",
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_plan_current_invalid"
    assert receipt["details"]["error_type"] == "FileNotFoundError"


def test_spec780_bootstrap_rejects_whitespace_only_title(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户交付一个可执行任务")
    receipt = apply_guide_submit(store, {
        "guide_id": "task_bootstrap",
        "item_id": "build_initial_task_guide",
        "option_id": "submit_initial_guide",
        "fields": {
            "task_title": "   ",
            "items": [{"item_id": "item_01", "title": "执行任务"}],
            "acceptance": [{
                "acceptance_id": "acc_01",
                "description": "任务完成",
                "item_refs": ["item_01"],
            }],
        },
    })

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_plan_title_required"
    assert store.get("base.active_task") in (None, "")


def test_spec780_open_and_blocked_records_are_revisable_and_removable(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    current = store.load_task_guide(task_id)
    current["items"][0].update({
        "status": "blocked",
        "evidence_refs": ["call:blocked-1"],
        "reason": "旧阻塞",
    })
    current["items"].append({
        "item_id": "item_02",
        "title": "可删除的开放项",
        "required": True,
        "status": "open",
    })
    current["acceptance"][0]["item_refs"] = ["item_01", "item_02"]
    store.save_task_guide(task_id, current)

    receipt = _revise(store, task_id, {
        "items": [{"item_id": "item_01", "title": "按新证据调整阻塞项"}],
        "acceptance": [{
            "acceptance_id": "acc_01",
            "description": "调整后的任务可核验",
            "item_refs": ["item_01"],
        }],
    })

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert [item["item_id"] for item in guide["items"]] == ["item_01"]
    assert guide["items"][0]["title"] == "按新证据调整阻塞项"
    assert guide["items"][0]["status"] == "blocked"
    assert guide["items"][0]["evidence_refs"] == ["call:blocked-1"]
    assert guide["items"][0]["reason"] == "旧阻塞"


def test_spec780_revision_blocks_until_pending_input_is_integrated(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import append_task_pending_input

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    append_task_pending_input(
        store,
        task_id,
        source_refs=["round:2:interaction"],
        summary="用户补充了新要求",
    )
    before = _task_guide_path(store, task_id).read_bytes()

    receipt = _revise(store, task_id, {"risk_notes": ["新增风险"]})

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "pending_interaction_first"
    assert _task_guide_path(store, task_id).read_bytes() == before


def test_spec780_revision_keeps_status_update_and_restart_truth(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.evidence_refs import evidence_handle_for_result
    from logic.guide_submit import apply_guide_submit

    root = tmp_path / "workbench"
    store = WorkbenchStore(root_dir=str(root))
    task_id = _create_task(store)
    revised = _revise(store, task_id, {
        "items": [{"item_id": "item_02", "title": "生成最终报告"}],
        "acceptance": [{
            "acceptance_id": "acc_02",
            "description": "最终报告已经写入",
            "item_refs": ["item_02"],
        }],
    })
    assert revised["status"] == "accepted"

    result = {
        "tool_id": "file_write",
        "status": "ok",
        "call_id": "call-report",
        "path": "output/report.md",
    }
    evidence_ref = evidence_handle_for_result(result)
    updated = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "item_id": "task_progress",
            "option_id": "update_task_status",
            "fields": {
                "items": {
                    "item_02": {
                        "status": "done",
                        "evidence_refs": [evidence_ref],
                    },
                },
                "acceptance": {
                    "acc_02": {
                        "status": "passed",
                        "evidence_refs": [evidence_ref],
                    },
                },
            },
        },
        evidence_context={"prior_general_tool_results": [result]},
    )
    assert updated["status"] == "accepted"

    reopened = WorkbenchStore(root_dir=str(root))
    guide = reopened.load_task_guide(task_id)
    assert guide["items"][0]["status"] == "done"
    assert guide["acceptance"][0]["status"] == "passed"
    active = reopened.load_active_guide()
    assert active is None


def test_spec780_revision_fact_exposes_action_and_hashes(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_helpers import format_protocol_tool_fact

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = _create_task(store)
    receipt = _revise(store, task_id, {"risk_notes": ["关注输入完整性"]})

    fact = format_protocol_tool_fact(
        receipt,
        fact_context={"workbench_store": store},
    )

    assert "【本轮任务计划修订事实】" in fact
    assert "修订动作：applied" in fact
    assert receipt["task_plan_revision"]["before_sha256"] in fact
    assert receipt["task_plan_revision"]["after_sha256"] in fact
