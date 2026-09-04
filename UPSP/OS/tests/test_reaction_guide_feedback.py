import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def test_spec563_rhythm_action_card_puts_waiting_task_first():
    from engines.reaction_guide_feedback import render_active_guide_feedback

    guide = {
        "guide_id": "rhythm:calendar_week:R000572",
        "kind": "calendar_rhythm_guide",
        "reason": "runtime heartbeat rhythm guide materialized",
        "source_refs": ["round:572", "heartbeat_flags"],
        "items": [
            {
                "item_id": "calendar_week_due",
                "options": [
                    {
                        "option_id": "write_chronicle",
                        "required_fields": ["content"],
                        "allowed_fields": ["content", "reason"],
                    }
                ],
            }
        ],
    }

    feedback = render_active_guide_feedback(
        guide,
        active_slots={
            "rhythm": "rhythm:calendar_week:R000572",
            "work": "task_bootstrap",
        },
    )

    waiting = "等待中的任务指南：先完成当前节律指南，完成后任务指南会重新显示。"
    current = "当前指南：节律指南"
    assert waiting in feedback
    assert current in feedback
    assert feedback.index(waiting) < feedback.index(current)
    assert "处理对象：节律编年史写入。" in feedback
    assert "调用坐标：guide_id=rhythm:calendar_week:R000572" in feedback
    assert "item_id=calendar_week_due" in feedback
    assert "option_id=write_chronicle：写入本次节律编年史。" in feedback
    assert "需要填写：content（正文）。" in feedback
    assert "可补充：reason" not in feedback
    assert "触发原因" not in feedback
    assert "来源引用" not in feedback
    assert "heartbeat_flags" not in feedback


def test_spec571_task_bootstrap_feedback_hides_retired_escape_options(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_guide_feedback import render_active_guide_feedback
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = create_task_bootstrap_guide(store, reason="读取材料后建清单")
    feedback = render_active_guide_feedback(guide)

    assert "当前指南：任务清单创建指南" in feedback
    assert "- kind: active_task_bootstrap_guide" in feedback
    assert "建账专用卡：" in feedback
    assert "调用坐标：guide_id=task_bootstrap" in feedback
    assert "option_id=submit_initial_guide" in feedback
    assert "option_id=blocked_by_missing_access" not in feedback
    assert "option_id=need_more_sources" not in feedback
    assert "初始清单是可修订工作计划" in feedback
    assert "source_refs=计划来源坐标" in feedback
    assert "不要求在未知信息下假装一次写对" not in feedback
    assert "不要在未知信息下假装已经读过材料" in feedback
    assert "source_refs 只是计划准备读取或核验的稳定坐标，不是读取成功证据" in feedback
    assert "后续结构变化用 revise_task_plan" in feedback
    assert "不要把用户原始目标改写成更小的阶段性目标" in feedback
    assert "只完成部分内容" in feedback
    assert "reaction_finalize(handoff_text)" in feedback
    assert "执行期" not in feedback
    assert "任务验收 checkpoint" not in feedback
    assert "memory_write" not in feedback
    assert "memory_container" not in feedback
    assert "提交前自检" not in feedback
    assert "任务清单创建要求" not in feedback
    assert "req_id / requirement_id" not in feedback
    assert "blocked_by_missing_access" not in feedback
    assert "source_refs 必须命中此前成功读取证据" not in feedback


def test_spec563_task_execution_delegates_to_existing_renderer():
    from engines.reaction_guide_feedback import render_active_guide_feedback

    feedback = render_active_guide_feedback(
        {"guide_id": "task:T-1", "kind": "task_execution"},
        workbench=object(),
        task_execution_renderer=lambda guide, workbench: (
            f"delegated:{guide['guide_id']}:{workbench.__class__.__name__}"
        ),
    )

    assert "- kind: active_task_execution_guide" in feedback
    assert "delegated:task:T-1:object" in feedback
