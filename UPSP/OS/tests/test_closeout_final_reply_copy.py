import json
import os
import sys
from pathlib import Path

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, logical_step


def test_spec559_time_feedback_uses_generic_final_reply_reminder_only():
    from engines.reaction_loop import ReactionLoopRunner
    from logic.closeout_copy import (
        CLOSEOUT_FINAL_REPLY_REMINDER,
        TASK_DELIVERY_CLOSEOUT_REMINDER,
    )

    reminder = ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=600,
        time_limit_seconds=600,
    )
    warning = ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=1200,
        time_limit_seconds=600,
    )

    assert "【时间提醒】" in reminder
    assert CLOSEOUT_FINAL_REPLY_REMINDER in reminder
    assert TASK_DELIVERY_CLOSEOUT_REMINDER not in reminder
    assert "【时间警告】" in warning
    assert CLOSEOUT_FINAL_REPLY_REMINDER in warning
    assert TASK_DELIVERY_CLOSEOUT_REMINDER not in warning


def test_spec594_runtime_auto_continue_handoff_text_uses_thirty_minutes():
    from engines.reaction_terminal_state import build_runtime_auto_continue_closeout

    class DummyStateManager:
        def __init__(self):
            self.flags = {}

        def set_flag(self, key, value):
            self.flags[key] = value

    sm = DummyStateManager()

    relay_receipt, settlement_ledger, guard_receipt = (
        build_runtime_auto_continue_closeout(
            sm,
            elapsed_seconds=1801,
            time_limit_seconds=600,
        )
    )

    assert sm.flags["continue_requested"] is True
    assert "30分钟" in relay_receipt["handoff_text"]
    assert "30分钟" in settlement_ledger["handoff_text"]
    assert "15分钟" not in relay_receipt["handoff_text"]
    assert "15分钟" not in settlement_ledger["handoff_text"]
    assert guard_receipt["source"] == "runtime_auto_continue"


def test_spec634_dds_keeps_only_current_closeout_time_ladder():
    repo_root = Path(__file__).resolve().parents[3]
    dds = (repo_root / "UPSP_Base_DDS.md").read_text(encoding="utf-8")

    assert "round.time_limit` 当前为 600 秒" in dds
    assert "600 秒在场提醒、1200 秒收束警告、1800 秒自动存档并置位中继" in dds
    assert "提醒和警告不收窄工具面" in dds
    assert "默认 300 秒" not in dds


def test_spec559_task_acceptance_feedback_uses_task_delivery_reminder():
    from engines.reaction_task_acceptance import task_acceptance_feedback
    from logic.closeout_copy import (
        CLOSEOUT_FINAL_REPLY_REMINDER,
        TASK_DELIVERY_CLOSEOUT_REMINDER,
    )

    feedback = task_acceptance_feedback({
        "reason": "task_acceptance_blocked",
        "guide_id": "task:T-559",
        "blockers": ["task_01", "acc_01"],
    })

    assert "任务验收 checkpoint" in feedback
    assert CLOSEOUT_FINAL_REPLY_REMINDER in feedback
    assert TASK_DELIVERY_CLOSEOUT_REMINDER in feedback


def test_spec559_reaction_popup_uses_generic_final_reply_reminder_only(tmp_path):
    from assembly.context import ContextAssembler
    from logic.closeout_copy import (
        CLOSEOUT_FINAL_REPLY_REMINDER,
        TASK_DELIVERY_CLOSEOUT_REMINDER,
    )

    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
    popup = assembler._build_handoff_popup("reaction", "interactive")

    assert "当前是反应步循环" in popup
    assert CLOSEOUT_FINAL_REPLY_REMINDER in popup
    assert TASK_DELIVERY_CLOSEOUT_REMINDER not in popup


class TestSpec559CloseoutFinalReplyCopy(RuntimeTestMixin):
    def test_task_guide_completed_reminds_same_response_final_reply(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from logic.closeout_copy import (
            CLOSEOUT_FINAL_REPLY_REMINDER,
            TASK_DELIVERY_CLOSEOUT_REMINDER,
        )
        from logic.sandbox_grant import SANDBOX_GRANT_ENV
        from logic.task_guide import materialize_initial_task_guide

        rt = self._make_runtime(tmp_path)
        task_root = tmp_path / "task"
        output_root = task_root / "output"
        output_root.mkdir(parents=True)
        monkeypatch.setenv(
            SANDBOX_GRANT_ENV,
            json.dumps({
                "phase": "spec559",
                "task_root": str(task_root),
                "read_paths": [str(task_root)],
                "write_paths": [str(output_root)],
                "shell_cwd": str(task_root),
                "allowed_tools": ["file_write"],
            }),
        )
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        task_id = materialize_initial_task_guide(
            rt.workbench,
            {
                "task_title": "Spec559 final reply reminder task",
                "task_goal": "Close after one artifact.",
                "source_requirements": [
                    {"requirement_id": "req_01", "summary": "Write one artifact."}
                ],
                "items": [
                    {
                        "item_id": "item_01",
                        "description": "Write output/report.md.",
                        "requirement_refs": ["req_01"],
                    }
                ],
                "acceptance": [
                    {
                        "acceptance_id": "acc_01",
                        "description": "The file write receipt is cited.",
                        "item_refs": ["item_01"],
                    }
                ],
            },
        )

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "write_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_write_handler",
                "permission_scope": "workspace_write_allowlist",
                "result_kind": "general_tool_result",
                "call_id": request.get("call_id"),
                "path": request.get("path"),
            }

        class TaskGuideCompletedExecutor:
            def __init__(self):
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = logical_step(step, active_protocol_tool_guides)
                if logical == "final_reply":
                    raise AssertionError("finish should not call final_reply")
                self.reaction_calls += 1
                combined = "\n".join(
                    str(message.get("content", "")) for message in messages)
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_write",
                            {
                                "path": str(output_root / "report.md"),
                                "content": "done",
                                "purpose": "Spec559 evidence",
                            },
                            call_id="call_spec559_write",
                            tool_family="general_tool",
                            tool_class="write_tool",
                            risk="medium",
                        )],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": f"task:{task_id}",
                                "submissions": [{
                                    "item_id": "task_progress",
                                    "option_id": "update_task_status",
                                    "fields": {
                                        "items": {
                                            "item_01": {
                                                "status": "done",
                                                "evidence_refs": ["call_spec559_write"],
                                            },
                                        },
                                        "acceptance": {
                                            "acc_01": {
                                                "status": "passed",
                                                "evidence_refs": ["call_spec559_write"],
                                            },
                                        },
                                    },
                                }],
                            },
                            call_id="call_spec559_guide_done",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                assert "task_guide_completed" in combined
                assert CLOSEOUT_FINAL_REPLY_REMINDER in combined
                assert TASK_DELIVERY_CLOSEOUT_REMINDER in combined
                return {
                    "response": "报告已写入 output/report.md；没有剩余问题。",
                    "tool_call_envelopes": [],
                }

        executor = TaskGuideCompletedExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "done"
        assert result["response"] == "报告已写入 output/report.md；没有剩余问题。"
        assert executor.reaction_calls == 3
