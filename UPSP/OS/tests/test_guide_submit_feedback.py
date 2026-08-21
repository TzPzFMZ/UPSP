from pathlib import Path

from UPSP.OS.tests.test_guide_submit import _successful_tool_evidence_context

def test_spec503_task_execution_feedback_renders_natural_state_without_duplicate_warning(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_guide = {
        "task_title": "12项日常能力测试",
        "task_goal": "完成 12 项任务并保存产物。",
        "items": [
            {
                "item_id": "item_01",
                "title": "查找 3 个 agent 框架",
                "status": "done",
                "required": True,
                "evidence_refs": ["output/01_frameworks.md"],
            },
            {
                "item_id": "item_02",
                "title": "整理低成本文本模型价格",
                "status": "blocked",
                "required": True,
                "evidence_refs": ["web_search unavailable"],
            },
            {
                "item_id": "item_03",
                "title": "整理 inbox 零散笔记",
                "status": "open",
                "required": True,
            },
        ],
        "acceptance": [
            {
                "acceptance_id": "acc_01",
                "description": "01_frameworks.md 存在且来源可追溯",
                "status": "passed",
                "required": True,
                "item_refs": ["item_01"],
                "evidence_refs": ["output/01_frameworks.md"],
            },
            {
                "acceptance_id": "acc_02",
                "description": "02_model_pricing.md 标明日期和单位口径",
                "status": "pending",
                "required": True,
                "item_refs": ["item_02"],
            },
        ],
    }
    task_id = store.create_task_guide_task(
        task_title=task_guide["task_title"],
        task_goal=task_guide["task_goal"],
        guide=task_guide,
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [{
                    "option_id": "update_task_status",
                    "required_fields": [],
                    "allowed_fields": ["items", "acceptance"],
                }],
            }
        ],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))

    feedback = runner._active_guide_feedback()

    assert "任务执行指南｜行动卡" in feedback
    assert "当前任务执行指南：" not in feedback
    assert "看板在 40_high_freq；本卡只管下一步行动" in feedback
    assert "真实工作优先；证据后登记" in feedback
    assert "file_write" in feedback
    assert "subagent_dispatch" in feedback
    assert "shell_command" not in feedback
    assert "web_fetch" in feedback
    assert "任务验收 checkpoint" in feedback
    assert "已有真实证据后，再用 guide_submit 更新账本" not in feedback
    assert "option_id=update_task_status" not in feedback
    assert "item_id=task_progress" not in feedback
    assert "当前任务清单明细" not in feedback
    assert "任务项：" not in feedback
    assert "验收项：" not in feedback
    assert "item_03 open" not in feedback
    assert "重复" not in feedback
    assert "duplicate" not in feedback.lower()


def test_spec518_task_execution_feedback_explains_blocked_closeout(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_guide = {
        "task_title": "部分完成任务",
        "task_goal": "完成可完成部分，无法访问的来源说明阻塞。",
        "items": [
            {
                "item_id": "item_01",
                "title": "已完成项",
                "status": "done",
                "required": True,
                "evidence_refs": ["output/done.md"],
            },
            {
                "item_id": "item_02",
                "title": "无法访问来源",
                "status": "blocked",
                "required": True,
                "reason": "官方页不可访问",
            },
        ],
        "acceptance": [
            {
                "acceptance_id": "acc_01",
                "description": "已完成项验收",
                "status": "passed",
                "required": True,
                "item_refs": ["item_01"],
                "evidence_refs": ["output/done.md"],
            },
            {
                "acceptance_id": "acc_02",
                "description": "阻塞项说明",
                "status": "blocked",
                "required": True,
                "item_refs": ["item_02"],
                "reason": "官方页不可访问",
            },
        ],
    }
    task_id = store.create_task_guide_task(
        task_title=task_guide["task_title"],
        task_goal=task_guide["task_goal"],
        guide=task_guide,
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [{"item_id": "task_progress", "options": []}],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))

    feedback = runner._active_guide_feedback()

    assert "存在已阻塞记录" in feedback
    assert "账本闭合后直接自然回复用户" in feedback
    assert "不要反复空喊完成" in feedback
    assert "验收项优先使用 passed 或 blocked" in feedback


def test_spec518_acceptance_direct_entries_hide_done_option(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import refresh_task_execution_active_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="验收状态口径",
        task_goal="确认 direct entry 只鼓励 passed/blocked。",
        guide={
            "items": [
                {"item_id": "item_01", "title": "生成报告", "status": "open"}
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "报告已验证",
                    "status": "pending",
                }
            ],
        },
    )

    active = refresh_task_execution_active_guide(store, task_id)
    item_entry = next(item for item in active["items"] if item["item_id"] == "item_01")
    acceptance_entry = next(
        item for item in active["items"] if item["item_id"] == "acc_01"
    )

    assert [item["option_id"] for item in item_entry["options"]] == ["done", "blocked"]
    assert [item["option_id"] for item in acceptance_entry["options"]] == [
        "passed",
        "blocked",
    ]


def test_spec476_reaction_resident_task_guidance_hidden_while_rhythm_front(tmp_path):
    from data.workbench import WorkbenchStore
    from data.state_store import StateStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "calendar:R476",
        "kind": "calendar_rhythm_guide",
        "guide_slot": "rhythm",
        "items": [{
            "item_id": "calendar_day_due",
            "options": [{
                "option_id": "write_chronicle",
                "required_fields": ["content"],
                "allowed_fields": ["content"],
            }],
        }],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(
        state_store=state_store,
        workbench_store=store,
    ))

    assert runner._reaction_resident_guide_feedback() == ""


def test_spec470_rhythm_active_guide_feedback_is_actionable_in_coalesced_round(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "calendar:R470",
        "kind": "calendar_rhythm_guide",
        "guide_slot": "rhythm",
        "items": [{
            "item_id": "calendar_day_due",
            "options": [{
                "option_id": "write_chronicle",
                "required_fields": ["content"],
                "allowed_fields": ["content", "reason"],
            }],
        }],
    }, active=True)
    store.save_guide({
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "guide_slot": "work",
        "items": [{
            "item_id": "build_initial_task_guide",
            "options": [{
                "option_id": "submit_initial_guide",
                "required_fields": ["task_title"],
                "allowed_fields": ["task_title"],
            }],
        }],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))
    state = {
        "base": {
            "heartbeat_flags": {
                "calendar_day_due": True,
                "user_message_waiting": True,
            },
        },
    }

    assert runner._should_suppress_active_guide_feedback("rhythm", state) is False
    assert runner._active_guide_protocol_tools() == ["guide_submit"]
    feedback = runner._active_guide_feedback()

    assert "当前指南：节律指南" in feedback
    assert "调用坐标：guide_id=calendar:R470" in feedback
    assert "item_id=calendar_day_due" in feedback
    assert "option_id=write_chronicle" in feedback
    assert "需要填写：content" in feedback
    waiting = "等待中的任务指南：先完成当前节律指南，完成后任务指南会重新显示。"
    assert waiting in feedback
    assert feedback.index(waiting) < feedback.index("当前指南：节律指南")
    assert "可补充：reason" not in feedback
    assert "不要把可见标题当作 guide_id" in feedback
    assert "Active guide:" not in feedback
    assert "required_fields=" not in feedback
    assert "fields example" not in feedback


def test_spec452_task_bootstrap_feedback_requires_explicit_source_ledger_mapping(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "source_refs": ["D:/workspace/task.md"],
        "items": [
            {
                "item_id": "build_initial_task_guide",
                "options": [
                    {
                        "option_id": "submit_initial_guide",
                        "required_fields": ["task_title"],
                        "allowed_fields": ["task_title", "source_requirements"],
                    }
                ],
            }
        ],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))

    feedback = runner._active_guide_feedback()

    assert "建账专用卡：" in feedback
    assert "一次提交完整初始账本" in feedback
    assert "source_refs=已读材料目录" in feedback
    assert "source_requirements=任务需求账" in feedback
    assert "items=执行项" in feedback
    assert "acceptance=验收项" in feedback
    assert "先读材料：路径/URL/文件名只是入口" in feedback
    assert "读取材料和 submit_initial_guide 不同次提交" in feedback
    assert "中文自然语言" in feedback
    assert "工具调用走 native 通道" in feedback
    assert "不承载 DSML/JSON/完整参数" in feedback
    assert "来源需求账" in feedback
    assert "自然字段名" not in feedback
    assert "req_id / requirement_id / id" not in feedback
    assert "acc_id / acceptance_id / id" not in feedback
    assert "Runtime 会把这些字段正规化成内部任务账本" not in feedback
    assert "提交前自检" not in feedback
    assert "任务验收 checkpoint" not in feedback
    assert "memory_write" not in feedback
    assert "Requirement ledger contract:" not in feedback
    assert "Before guide_submit, self-check:" not in feedback


def test_spec508_popup_doc_records_guide_naturalization_policy():
    popup_doc = (
        Path(__file__).resolve().parents[2]
        / "initialization" / "persona_template" / "docs" / "protocol" / "base" / "popup.md"
    ).read_text(encoding="utf-8")

    assert "指南清单前台文案" in popup_doc
    assert "中文动作卡" in popup_doc
    assert "guide_id / item_id / option_id" in popup_doc
    assert "required_fields=" in popup_doc
    assert "不要把 `Active guide:`" in popup_doc
    assert "task_bootstrap 必须保留任务源锚定" in popup_doc
    assert "task_bootstrap 是建账专用卡" in popup_doc
    assert "长别名清单、执行期证据登记、任务验收 checkpoint、记忆/容器提示不放进建账卡" in popup_doc
    assert "`source_refs` 是已读材料目录" in popup_doc
    assert "`items` 是执行项" in popup_doc
    assert "`acceptance` 是验收项" in popup_doc
    assert "task_bootstrap 必须提示分批整理而不是半账本提交" not in popup_doc
    assert "约 `4K tokens`" not in popup_doc
    assert "工具参数必须走 provider-native 工具通道" in popup_doc
    assert "DSML/JSON/完整参数" in popup_doc
    assert "`40_high_freq` 的任务看板顶部必须固定说明" in popup_doc
    assert "任务项状态只用 `done / blocked`" in popup_doc
    assert "验收项状态只用 `passed / blocked`" in popup_doc
    assert "guide_submit(guide_id=<当前task>, item_id=task_progress, option_id=update_task_status)" in popup_doc
    assert 'fields.pending_inputs=[{"pending_input_id":"input_01"' in popup_doc
    assert "option_id=integrate_pending_input" in popup_doc
    assert "resident reaction guide 只是短入口" in popup_doc
    assert "无工作债务" not in popup_doc
    assert "没有 active work guide 时默认显示" in popup_doc


def test_spec434_reaction_runner_task_feedback_shows_checklist_and_finish_exit(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="验证实现",
        task_goal="生成报告",
        guide={
            "items": [
                {
                    "item_id": "item_01",
                    "title": "读取相关文件",
                    "required": True,
                    "status": "open",
                }
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "报告文件存在",
                    "required": True,
                    "status": "pending",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))

    feedback = runner._active_guide_feedback()

    assert "任务执行指南｜行动卡" in feedback
    assert "当前任务执行指南：" not in feedback
    assert "40_high_freq" in feedback
    assert "本卡只管下一步行动" in feedback
    assert "真实工作优先；证据后登记" in feedback
    assert "file_write" in feedback
    assert "subagent_dispatch" in feedback
    assert "shell_command" not in feedback
    assert "工具调用走 native 通道" in feedback
    assert "正文只写简短进展" in feedback
    assert "任务验收 checkpoint" in feedback
    assert "已有真实证据后，再用 guide_submit 更新账本" not in feedback
    assert "option_id=update_task_status" not in feedback
    assert "Runtime 会自动撤下这份任务清单" not in feedback
    assert "读取相关文件" not in feedback
    assert "报告文件存在" not in feedback


def test_spec471_pending_input_must_be_integrated_before_task_progress(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="已有任务",
        task_goal="处理文件并输出报告",
        guide={
            "pending_inputs": [{
                "pending_input_id": "input_01",
                "status": "pending",
                "source_refs": ["round:7:interaction"],
                "summary": "用户追加了新的输出要求",
            }],
            "items": [{
                "item_id": "item_01",
                "title": "输出报告",
                "required": True,
                "status": "open",
            }],
            "acceptance": [{
                "acceptance_id": "acc_01",
                "description": "报告已验证",
                "required": True,
                "status": "pending",
            }],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [{
            "item_id": "task_progress",
            "options": [
                {
                    "option_id": "update_task_status",
                    "required_fields": [],
                    "allowed_fields": ["items", "acceptance"],
                },
                {
                    "option_id": "integrate_pending_input",
                    "required_fields": ["pending_inputs"],
                    "allowed_fields": [
                        "pending_inputs",
                        "source_requirements",
                        "items",
                        "acceptance",
                    ],
                },
            ],
        }],
    }, active=True)

    update = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [{
                "item_id": "task_progress",
                "option_id": "update_task_status",
                "fields": {"items": {"item_01": "done"}},
                "evidence_refs": ["call_write_report"],
            }],
        },
        evidence_context=_successful_tool_evidence_context(),
    )
    assert update["status"] == "rejected"
    assert update["reason"] == "pending_interaction_first"

    integrated = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [{
                "item_id": "task_progress",
                "option_id": "integrate_pending_input",
                "fields": {
                    "pending_inputs": [{
                        "pending_input_id": "input_01",
                        "status": "integrated",
                    }],
                    "items": [{
                        "item_id": "item_02",
                        "title": "加入中文摘要",
                        "required": True,
                        "status": "open",
                    }],
                    "acceptance": [{
                        "acceptance_id": "acc_02",
                        "description": "中文摘要存在",
                        "required": True,
                        "status": "pending",
                    }],
                },
            }],
        },
        evidence_context=_successful_tool_evidence_context(),
    )
    assert integrated["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["pending_inputs"][0]["status"] == "integrated"
    assert any(item["item_id"] == "item_02" for item in guide["items"])


def test_spec434_reaction_runner_blocks_finish_when_task_acceptance_pending(tmp_path):
    from data.state_store import StateStore
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.create_task_guide_task(
        task_title="整理周报",
        task_goal="生成周报并验证数据",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [],
        },
    )
    runner = ReactionLoopRunner(RuntimeServices.create(
        state_store=StateStore(str(tmp_path / "state.json")),
        workbench_store=store,
    ))

    result = runner._task_closeout_acceptance({
        "closeout_decision": "finish",
    })

    assert result["allowed"] is False
    assert result["reason"] == "task_acceptance_blocked"


def test_spec447_closeout_task_acceptance_feedback_stays_in_closeout_lane(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    runner = ReactionLoopRunner(
        RuntimeServices.create(
            workbench_store=WorkbenchStore(root_dir=str(tmp_path / "workbench"))
        )
    )

    feedback = runner._task_acceptance_feedback(
        {
            "reason": "task_acceptance_blocked",
            "blockers": ["item_01", "acc_01"],
        },
    )

    assert "任务验收 checkpoint" in feedback
    assert "guide_submit" in feedback
    assert "option_id=update_task_status" in feedback
    assert "closeout_decision=continue" not in feedback
    assert "closeout_decision=blocked" not in feedback
    assert "账本闭合后直接自然回复用户" in feedback
    assert "item_01" in feedback
    assert "acc_01" in feedback
