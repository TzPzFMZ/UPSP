import json
import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import (
    RuntimeTestMixin,
    ScriptedExecutor,
    logical_step as _logical_step,
)


def test_spec536_dsml_text_payload_generates_channel_hygiene_warning():
    from engines.reaction_loop_main import _assistant_text_tool_payload_warning
    from assembly.context_helpers import build_native_tool_feedback_popup

    warning = _assistant_text_tool_payload_warning(
        "我会提交任务清单。\n<｜DSML｜tool_calls><｜DSML｜invoke name=\"guide_submit\">..."
    )

    assert "工具通道卫生警告" in warning
    assert "provider-native 工具通道" in warning
    assert "Runtime 不会解析这段正文为工具" in warning
    popup = build_native_tool_feedback_popup("reaction", [warning])
    assert "不要把工具调用载荷写进自然语言正文" in popup


def test_spec536_tool_json_text_payload_generates_channel_hygiene_warning():
    from engines.reaction_loop_main import _assistant_text_tool_payload_warning

    warning = _assistant_text_tool_payload_warning(
        '进展：{"tool_calls":[{"function":{"name":"guide_submit","arguments":"{}"}}]}'
    )

    assert "工具通道卫生警告" in warning
    assert "JSON 工具调用" in warning


def test_spec536_plain_progress_with_native_tool_is_not_channel_mixing():
    from engines.reaction_loop_main import _assistant_text_tool_payload_warning

    assert _assistant_text_tool_payload_warning(
        "我先建立清单，然后继续写文件。"
    ) == ""


def test_spec594_reaction_time_feedback_uses_10_20_30_presence_milestones():
    from engines.reaction_loop import ReactionLoopRunner

    assert ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=599,
        time_limit_seconds=600,
    ) == ""

    reminder = ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=600,
        time_limit_seconds=600,
    )
    assert "【时间提醒】" in reminder
    assert "在场" in reminder
    assert "memory_write" in reminder
    assert "closeout_only" not in reminder

    assert "【时间警告】" not in ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=1199,
        time_limit_seconds=600,
    )

    warning = ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=1200,
        time_limit_seconds=600,
    )
    assert "【时间警告】" in warning
    assert "进入收束" in warning
    assert "紧迫" not in warning
    assert "立即收束" not in warning
    assert "memory_write" in warning
    assert "工具面不会" not in warning
    assert "closeout_only" not in warning


def test_spec574_task_guide_completed_feedback_suppresses_resident_entry(tmp_path):
    from data.state_store import StateStore
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.reaction_loop_main import _has_task_guide_completed_feedback
    from engines.runtime_services import RuntimeServices
    from logic.work_intent_debt import create_work_intent_debt

    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    runner = ReactionLoopRunner(RuntimeServices.create(
        state_store=state_store,
        workbench_store=store,
    ))
    create_work_intent_debt(
        runner.sm,
        round_num=574,
        reason="spec574 resident suppress after task guide completed",
        source="test",
    )

    assert runner._reaction_resident_guide_feedback()
    feedbacks = [
        "task_guide_completed: 当前任务清单已由 guide_submit(update_task_status) 自动结算并撤下。"
    ]
    assert _has_task_guide_completed_feedback(feedbacks) is True
    assert runner._reaction_resident_guide_feedback(suppress_task_entry=True) == ""


class TestRuntimeReactionProtocolWriteTools(RuntimeTestMixin):
    def test_spec613_terminal_blocked_task_natural_reply_closes_once(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        task_id = rt.workbench.create_task_guide_task(
            task_title="SealGate NO-GO",
            task_goal="完成可行项并如实登记不可继续项",
            guide={
                "items": [
                    {
                        "item_id": "item_output",
                        "required": True,
                        "status": "done",
                        "evidence_refs": ["EV-output"],
                    },
                    {
                        "item_id": "item_memory",
                        "required": True,
                        "status": "blocked",
                        "evidence_refs": ["EV-identity-unresolved"],
                    },
                ],
                "acceptance": [
                    {
                        "acceptance_id": "acc_output",
                        "required": True,
                        "status": "passed",
                        "evidence_refs": ["EV-output"],
                    },
                    {
                        "acceptance_id": "acc_memory",
                        "required": True,
                        "status": "blocked",
                        "evidence_refs": ["EV-identity-unresolved"],
                    },
                ],
            },
        )
        rt.workbench.save_guide({
            "guide_id": f"task:{task_id}",
            "kind": "task_execution",
            "task_id": task_id,
            "items": [],
        }, active=True)

        class NaturalNoGoExecutor:
            def __init__(self):
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                assert _logical_step(step, active_protocol_tool_guides) == "reaction"
                self.reaction_calls += 1
                if self.reaction_calls > 1:
                    raise AssertionError("terminal blocked NO-GO must close on first reply")
                return {
                    "response": "NO-GO：身份未确认，记忆硬验收未闭合。",
                    "tool_call_envelopes": [],
                }

        executor = NaturalNoGoExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.reaction_calls == 1
        assert result["response"] == "NO-GO：身份未确认，记忆硬验收未闭合。"
        assert result["_exit_signal"] == "done"
        assert result["_final_reply_done"] is True
        assert result["_final_response_source"] == "reaction.natural_final_reply"
        ledger = result["_settlement_ledgers"][-1]
        assert ledger["closeout_decision"] == "blocked"
        assert ledger["runtime_derived_blocked"] is True
        assert ledger["blockers"] == ["item_memory", "acc_memory"]

    def test_spec497_finish_uses_same_response_text_without_final_reply_call(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        class FinishExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                self.calls.append(logical)
                if logical == "final_reply":
                    return {"response": "旧 final_reply 不该被调用", "tool_call_envelopes": []}
                return {
                    "response": "本轮已经完成，产物已写入 output。",
                    "tool_call_envelopes": [],
                }

        executor = FinishExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == ["reaction"]
        assert result["response"] == "本轮已经完成，产物已写入 output。"
        assert result["_final_reply_done"] is True
        assert result["_final_response_source"] == "reaction.natural_final_reply"

    def test_spec571_missing_access_natural_reply_clears_task_bootstrap(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from logic.task_guide import create_task_bootstrap_guide
        from logic.work_intent_debt import create_work_intent_debt

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        create_work_intent_debt(
            rt.sm,
            round_num=571,
            reason="用户要求读取受限材料后执行任务",
            source="setup_finalize",
            source_refs=["round:571:interaction"],
        )
        create_task_bootstrap_guide(
            rt.workbench,
            reason="用户要求读取受限材料后执行任务",
            source_refs=[r"D:\secret\book.md"],
        )
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "source": "general_tool_call",
                "reason": "outside_allowlist",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
            }

        class MissingAccessExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                self.calls.append(logical)
                if logical == "final_reply":
                    raise AssertionError("Spec571 must use natural final reply")
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {"path": r"D:\secret\book.md"},
                            call_id="call_spec571_access_denied",
                        )],
                    }
                return {
                    "response": "我无法读取用户给出的受限路径，所以不能基于材料继续执行。",
                    "tool_call_envelopes": [],
                }

        executor = MissingAccessExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == ["reaction", "reaction"]
        assert result["_final_reply_done"] is True
        assert result["_final_response_source"] == "reaction.natural_final_reply"
        assert result["response"] == "我无法读取用户给出的受限路径，所以不能基于材料继续执行。"
        assert rt.workbench.current_active_guide_id() in (None, "")
        assert rt.workbench.get("base.active_guides.work") in (None, "")
        assert rt.sm.get("base.runtime.work_intent_debt") == {}
        bootstrap = rt.workbench.load_guide("task_bootstrap")
        assert bootstrap["status"] == "dismissed"
        assert bootstrap["dismiss_reason"] == "missing_access_final_reply"
        assert result["_settlement_ledgers"][-1][
            "task_bootstrap_missing_access_final_reply"
        ] is True

    def test_spec497_continue_keeps_text_as_progress_without_closeout_response(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class ContinueExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                self.calls.append(logical)
                if logical == "final_reply":
                    return {"response": "旧 final_reply 不该被调用", "tool_call_envelopes": []}
                return {
                    "response": "我已经整理好下一步，下一轮继续处理剩余产物。",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_spec497_continue",
                        handoff_text="下一轮继续处理剩余产物。",
                    )],
                }

        executor = ContinueExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == ["reaction"]
        assert result["response"] == ""
        assert result["_assistant_progress"] == ["我已经整理好下一步，下一轮继续处理剩余产物。"]
        assert result["_final_reply_done"] is False
        assert result["_final_response_source"] == ""
        assert result["_closeout_relay_receipts"]

    def test_spec594_reaction_ten_minute_reminder_is_popup_visible_without_closeout(
            self, tmp_path, monkeypatch):
        import inspect
        import engines.reaction_loop_main as reaction_loop_main_mod
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(rt, "_load_time_limit", lambda: 600)
        time_state = {"run_loop_calls": 0}

        def fake_time():
            for frame in inspect.stack():
                if frame.function == "_run_reaction_frames":
                    time_state["run_loop_calls"] += 1
                    if time_state["run_loop_calls"] == 1:
                        return 0
                    return 601
            return 601

        monkeypatch.setattr(reaction_loop_main_mod.time, "time", fake_time)
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "start_line": int(request.get("line_start") or 1),
                "end_line": 20,
                "has_more": False,
                "read_mode": "bounded",
                "content": "file content",
            }

        class ReminderExecutor:
            def __init__(self):
                self.reaction_messages = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                if logical == "final_reply":
                    return {"response": "done", "tool_call_envelopes": []}
                self.reaction_calls += 1
                self.reaction_messages.append(list(messages))
                self.guides.append(list(active_protocol_tool_guides or []))
                combined = "\n".join(str(message.get("content", "")) for message in messages)
                assert "__closeout_only__" not in self.guides[-1]
                assert "【时间提醒】" in combined
                assert "在场" in combined
                assert "优先推进并完成当前事务" in combined
                assert "memory_write" in combined
                assert "reaction_finalize" in combined
                assert "handoff_text" in combined
                assert "closeout_only" not in combined
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "UPSP_Base_DDS.md",
                                "line_start": 1,
                                "reason": "keep working after reminder",
                            },
                            call_id="call_read_after_reminder",
                        )],
                    }
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        executor = ReminderExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_reaction_finalize_validated"] is True
        assert len(executor.reaction_messages) == 2

    def test_spec594_reaction_twenty_minute_warning_stays_soft_after_provider_return(
            self, tmp_path, monkeypatch):
        import engines.reaction_loop_main as reaction_loop_main_mod
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(rt, "_load_time_limit", lambda: 600)
        time_state = {"started": False, "now": 599}

        def fake_time():
            if not time_state["started"]:
                time_state["started"] = True
                return 0
            return time_state["now"]

        monkeypatch.setattr(reaction_loop_main_mod.time, "time", fake_time)
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "start_line": int(request.get("line_start") or 1),
                "end_line": 20,
                "has_more": False,
                "read_mode": "bounded",
                "content": "file content",
            }

        class TimeLimitExecutor:
            def __init__(self):
                self.guides = []
                self.combined_messages = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                if logical == "final_reply":
                    return {"response": "closed", "tool_call_envelopes": []}
                self.reaction_calls += 1
                guides = list(active_protocol_tool_guides or [])
                combined = "\n".join(str(message.get("content", "")) for message in messages)
                self.guides.append(guides)
                self.combined_messages.append(combined)
                if self.reaction_calls == 1:
                    assert "__closeout_only__" not in guides
                    assert "本轮已运行约" not in combined
                    assert "【时间提醒】" not in combined
                    time_state["now"] = 1201
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "UPSP_Base_DDS.md",
                                "line_start": 1,
                                "reason": "keep working before warning",
                            },
                            call_id="call_read_before_warning",
                        )],
                    }
                assert "__closeout_only__" not in guides
                assert "【时间警告】" in combined
                assert "进入收束" in combined
                assert "紧迫" not in combined
                assert "立即收束" not in combined
                assert "memory_write" in combined
                assert "停止无效扩张" in combined
                assert "工具面不会" not in combined
                assert "closeout_only" not in combined
                assert "handoff_text" in combined
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        executor = TimeLimitExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_reaction_finalize_validated"] is True
        assert "__closeout_only__" not in executor.guides[0]
        assert "__closeout_only__" not in executor.guides[1]

    def test_spec594_runtime_auto_continue_after_thirty_minutes(
            self, tmp_path, monkeypatch):
        import engines.reaction_loop_main as reaction_loop_main_mod

        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(rt, "_load_time_limit", lambda: 600)
        time_state = {"started": False}

        def fake_time():
            if not time_state["started"]:
                time_state["started"] = True
                return 0
            return 1801

        monkeypatch.setattr(reaction_loop_main_mod.time, "time", fake_time)

        class NoProviderCallExecutor:
            calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls += 1
                raise AssertionError("30 分钟自动中继不应再发起 provider 调用")

        executor = NoProviderCallExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == 0
        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "continue_requested"
        assert rt.sm.get_flags().get("continue_requested") is True
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "continue"
        assert result["_settlement_ledgers"][-1]["source"] == "runtime_auto_continue"
        assert result["_closeout_relay_receipts"][-1]["source"] == "runtime_auto_continue"

    def test_spec447_closeout_task_acceptance_block_keeps_finalize_exit(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        task_id = rt.workbench.create_task_guide_task(
            task_title="Daily eval",
            task_goal="Create artifacts and verify them",
            guide={
                "items": [{"item_id": "item_01", "required": True, "status": "open"}],
                "acceptance": [
                    {"acceptance_id": "acc_01", "required": True, "status": "pending"}
                ],
            },
        )
        rt.workbench.save_guide({
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
                        },
                    ],
                }
            ],
        }, active=True)
        helper = self

        class CloseoutTaskAcceptanceExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.combined_messages = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                self.calls.append(logical)
                guides = list(active_protocol_tool_guides or [])
                self.guides.append(guides)
                combined = "\n".join(str(message.get("content", "")) for message in messages)
                self.combined_messages.append(combined)
                if logical == "final_reply":
                    return {"response": "continued", "tool_call_envelopes": []}
                self.reaction_calls += 1
                assert "__closeout_only__" not in guides
                if self.reaction_calls == 1:
                    return {
                        "response": "任务已经完成。",
                        "tool_call_envelopes": [],
                    }
                assert "guide_submit" in combined
                assert "item_01" in combined
                assert "acc_01" in combined
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_continue_after_task_block",
                        handoff_text="下一轮继续补齐任务账本证据。",
                    )],
                }

        executor = CloseoutTaskAcceptanceExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "continue_requested"
        assert result["response"] == ""
        assert executor.reaction_calls == 2
        assert "__closeout_only__" not in executor.guides[-1]
        assert result["_closeout_relay_receipts"]
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "continue"
        assert any(
            receipt.get("source") == "natural_final_reply_candidate"
            and receipt.get("status") == "task_acceptance_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec477_task_blocker_natural_reply_then_handoff_without_closeout_ladder(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        task_id = rt.workbench.create_task_guide_task(
            task_title="Daily eval",
            task_goal="Create artifacts and verify them",
            guide={
                "items": [{"item_id": "item_01", "required": True, "status": "open"}],
                "acceptance": [
                    {"acceptance_id": "acc_01", "required": True, "status": "pending"}
                ],
            },
        )
        rt.workbench.save_guide({
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
                        },
                    ],
                }
            ],
        }, active=True)
        helper = self

        class NaturalFinishThenHandoffExecutor:
            def __init__(self):
                self.reaction_calls = 0
                self.final_reply_calls = 0
                self.feedback = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                if logical == "final_reply":
                    self.final_reply_calls += 1
                    return {"response": "should not be called", "tool_call_envelopes": []}
                assert "__closeout_only__" not in list(active_protocol_tool_guides or [])
                self.reaction_calls += 1
                combined = "\n".join(str(message.get("content", "")) for message in messages)
                self.feedback.append(combined)
                if self.reaction_calls == 1:
                    return {"response": "任务已经完成。", "tool_call_envelopes": []}
                assert "WARNING｜任务账本未闭合" in combined
                assert "fields.items" in combined
                assert "fields.acceptance" in combined
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_handoff_after_task_blocker",
                        handoff_text="任务账本未闭合，下一轮继续登记证据。",
                    )],
                }

        executor = NaturalFinishThenHandoffExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "continue_requested"
        assert result["response"] == ""
        assert executor.reaction_calls == 2
        assert executor.final_reply_calls == 0
        assert result["_closeout_relay_receipts"]
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "continue"
        assert any(
            receipt.get("source") == "natural_final_reply_candidate"
            and receipt.get("status") == "task_acceptance_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert not any(
            receipt.get("status") in {
                "task_acceptance_auto_blocked",
                "reaction_closeout_protocol_auto_blocked",
            }
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec505_task_blocker_keeps_loop_open_without_closeout_ladder(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        task_id = rt.workbench.create_task_guide_task(
            task_title="Daily eval",
            task_goal="Create artifacts and verify them",
            guide={
                "items": [{"item_id": "item_01", "required": True, "status": "open"}],
                "acceptance": [
                    {"acceptance_id": "acc_01", "required": True, "status": "pending"}
                ],
            },
        )
        rt.workbench.save_guide({
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
                        },
                    ],
                }
            ],
        }, active=True)
        helper = self

        class RetiredFinishThenNaturalBlockerExecutor:
            def __init__(self):
                self.reaction_calls = 0
                self.final_reply_calls = 0
                self.feedback = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                if logical == "final_reply":
                    self.final_reply_calls += 1
                    return {"response": "should not be called", "tool_call_envelopes": []}
                assert "__closeout_only__" not in list(active_protocol_tool_guides or [])
                self.reaction_calls += 1
                combined = "\n".join(str(message.get("content", "")) for message in messages)
                self.feedback.append(combined)
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_old_finish_field",
                            closeout_decision="finish",
                        )],
                    }
                if self.reaction_calls == 2:
                    assert "closeout_decision" in combined
                    assert "已退役" in combined
                    return {"response": "任务已经完成。", "tool_call_envelopes": []}
                assert "WARNING｜任务账本未闭合" in combined
                assert "fields.items" in combined
                assert "fields.acceptance" in combined
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_handoff_after_natural_task_blocker",
                        handoff_text="任务账本未闭合，下一轮继续登记证据。",
                    )],
                }

        executor = RetiredFinishThenNaturalBlockerExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "continue_requested"
        assert result["response"] == ""
        assert executor.reaction_calls == 3
        assert executor.final_reply_calls == 0
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "continue"
        assert any(
            item.get("reason") == "reaction_finalize_retired_field"
            for item in result["_invalid_tool_requests"]
        )
        assert any(
            receipt.get("source") == "natural_final_reply_candidate"
            and receipt.get("status") == "task_acceptance_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert not any(
            receipt.get("status") in {
                "task_acceptance_auto_blocked",
                "reaction_closeout_protocol_auto_blocked",
            }
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec505_invalid_finalize_feedback_does_not_forbid_followup_tools(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "path": request.get("path"),
                "content": "task text",
            }

        class CloseoutForbiddenToolExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                if logical == "final_reply":
                    raise AssertionError("finish should use same reaction response or fallback")
                combined = "\n".join(str(message.get("content", "")) for message in messages)
                self.calls.append((logical, combined))
                self.guides.append(list(active_protocol_tool_guides or []))
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_spec493_enter_closeout",
                            closeout_decision="complete",
                        )],
                    }
                assert "__closeout_only__" not in self.guides[-1]
                if len(self.calls) > 2:
                    return {
                        "response": "已经读取材料，完成。",
                        "tool_call_envelopes": [],
                    }
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_tool_envelope(
                        "file_read",
                        {"path": "DFT_AGENT_EVAL/agent_eval_tasks.md"},
                        call_id="call_tool_after_invalid_finalize",
                        tool_family="general_tool",
                        tool_class="read_tool",
                        risk="low",
                    )],
                }

        executor = CloseoutForbiddenToolExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in executor.calls] == [
            "reaction",
            "reaction",
            "reaction",
        ]
        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "done"
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert not any(
            item.get("reason") == "closeout_only_tool_not_allowed"
            for item in result["_invalid_tool_requests"]
        )

    def test_spec505_plain_text_after_invalid_finalize_finishes_without_closeout_only(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class CloseoutTextExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                if logical == "final_reply":
                    raise AssertionError("finish should not call final_reply")
                self.calls.append(logical)
                self.guides.append(list(active_protocol_tool_guides or []))
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_spec493_enter_closeout_text",
                            closeout_decision="complete",
                        )],
                    }
                assert "__closeout_only__" not in self.guides[-1]
                if len(self.calls) == 2:
                    return {"response": "我已经完成了。", "tool_call_envelopes": []}
                raise AssertionError("natural final reply should finish on the second reaction call")

        executor = CloseoutTextExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == ["reaction", "reaction"]
        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "done"
        assert result["response"] == "我已经完成了。"
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert not any(
            receipt.get("status") == "reaction_closeout_protocol_auto_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec477_task_acceptance_feedback_shows_update_answer_shape(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)

        feedback = rt.reaction_loop_runner._task_acceptance_feedback({
            "reason": "task_acceptance_blocked",
            "blockers": ["item_01", "acc_01"],
        })

        assert "guide_submit/update_task_status" in feedback
        assert "fields.items" in feedback
        assert "fields.acceptance" in feedback
        assert "status:'done'" in feedback
        assert "evidence_refs" in feedback
        assert "Runtime 会自动撤下任务清单" in feedback
        assert "settle_task_completed" not in feedback

    def test_spec551_task_acceptance_blocker_is_popup_warning_with_concrete_fields(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)

        feedback = rt.reaction_loop_runner._task_acceptance_feedback({
            "reason": "task_acceptance_blocked",
            "guide_id": "task:T-20260704-01",
            "blockers": ["task_01", "acc_01"],
        })

        assert "WARNING｜任务账本未闭合" in feedback
        assert "任务验收 checkpoint" in feedback
        assert "guide_id=task:T-20260704-01" in feedback
        assert "fields.items={\"task_01\":{\"status\":\"done\"" in feedback
        assert "fields.acceptance={\"acc_01\":{\"status\":\"passed\"" in feedback
        assert "不要只写 reason" in feedback
        assert "reason 不会改变账本状态" in feedback

    def test_spec553_task_acceptance_checkpoint_uses_shared_task_progress_copy(
            self, tmp_path):
        from logic.task_progress_copy import (
            TASK_ACCEPTANCE_UPDATE_EXAMPLE,
            TASK_ITEM_UPDATE_EXAMPLE,
        )

        rt = self._make_runtime(tmp_path)

        feedback = rt.reaction_loop_runner._task_acceptance_feedback({
            "reason": "task_acceptance_blocked",
            "guide_id": "task:T-553",
            "blockers": ["task_01", "acc_01"],
        })

        assert TASK_ITEM_UPDATE_EXAMPLE in feedback
        assert TASK_ACCEPTANCE_UPDATE_EXAMPLE in feedback
        assert "reason 不会改变账本状态" in feedback

    def test_spec554_task_acceptance_helpers_live_outside_reaction_loop(
            self, tmp_path):
        from data.workbench import WorkbenchStore
        from engines.reaction_task_acceptance import (
            check_task_closeout_acceptance,
            task_acceptance_block_signature,
            task_acceptance_feedback,
        )

        rt = self._make_runtime(tmp_path)
        feedback = task_acceptance_feedback({
            "reason": "task_acceptance_blocked",
            "guide_id": "task:T-554",
            "blockers": ["task_01", "acc_01"],
        })

        assert "guide_submit/update_task_status" in feedback
        assert "fields.items={\"task_01\":{\"status\":\"done\"" in feedback
        assert "fields.acceptance={\"acc_01\":{\"status\":\"passed\"" in feedback
        assert "reason 不会改变账本状态" in feedback
        assert task_acceptance_block_signature({
            "blockers": [" task_01 ", "acc_01"],
        }) == "task_01|acc_01"

        store = WorkbenchStore(root_dir=str(tmp_path / "workbench-554"))
        allowed = check_task_closeout_acceptance(
            rt.sm,
            store,
            {"closeout_decision": "finish"},
        )

        assert allowed["allowed"] is True
        assert allowed["reason"] == "no_active_task"
        assert rt.reaction_loop_runner._task_acceptance_feedback({
            "reason": "task_acceptance_blocked",
            "guide_id": "task:T-554",
            "blockers": ["task_01"],
        }) == task_acceptance_feedback({
            "reason": "task_acceptance_blocked",
            "guide_id": "task:T-554",
            "blockers": ["task_01"],
        })

    def test_spec419_submission_received_is_internal_audit_not_tool_fact(self):
        from engines.reaction_helpers import (
            format_protocol_tool_fact,
            protocol_receipt_should_enter_tool_fact,
        )

        receipt = {
            "tool_id": "memory_write",
            "status": "submission_received",
            "source": "memory_write_declaration",
        }

        assert protocol_receipt_should_enter_tool_fact(receipt) is False
        assert format_protocol_tool_fact(receipt) == ""

    def test_spec480_task_bootstrap_fact_keeps_created_checklist_visible(
            self, tmp_path):
        from data.workbench import WorkbenchStore
        from engines.reaction_helpers import format_protocol_tool_fact
        from logic.guide_submit import apply_guide_submit
        from logic.task_guide import (
            BOOTSTRAP_GUIDE_ID,
            BOOTSTRAP_ITEM_ID,
            BOOTSTRAP_SUBMIT_OPTION_ID,
            create_task_bootstrap_guide,
        )

        store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
        create_task_bootstrap_guide(store, reason="spec480")
        receipt = apply_guide_submit(
            store,
            {
                "guide_id": BOOTSTRAP_GUIDE_ID,
                "submissions": [
                    {
                        "item_id": BOOTSTRAP_ITEM_ID,
                        "option_id": BOOTSTRAP_SUBMIT_OPTION_ID,
                        "fields": {
                            "task_title": "Spec480 daily eval",
                            "task_goal": "Preserve checklist context after guide removal.",
                            "source_requirements": [
                                {
                                    "requirement_id": "req_01",
                                    "summary": "Read the task source.",
                                }
                            ],
                            "items": [
                                {
                                    "item_id": "item_01",
                                    "description": "Create the report file.",
                                    "requirement_refs": ["req_01"],
                                }
                            ],
                            "acceptance": [
                                {
                                    "acceptance_id": "acc_01",
                                    "description": "Report path is cited.",
                                    "item_refs": ["item_01"],
                                }
                            ],
                            "source_refs": ["task_root:DFT_AGENT_EVAL"],
                        },
                    }
                ],
            },
        )

        fact = format_protocol_tool_fact(
            receipt,
            fact_context={"workbench_store": store},
        )

        assert "Spec480 daily eval" in fact
        assert "req_01" in fact
        assert "Read the task source." in fact
        assert "item_01" in fact
        assert "Create the report file." in fact
        assert "acc_01" in fact
        assert "Report path is cited." in fact
        assert "reaction_finalize_finish" not in fact
        assert "next_action" not in fact

    def test_spec480_task_completion_fact_keeps_final_checklist_and_evidence(
            self, tmp_path):
        from data.workbench import WorkbenchStore
        from engines.reaction_helpers import format_protocol_tool_fact
        from logic.guide_submit import apply_guide_submit
        from logic.task_guide import materialize_initial_task_guide

        store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
        task_id = materialize_initial_task_guide(
            store,
            {
                "task_title": "Spec480 final task",
                "task_goal": "Complete with evidence.",
                "source_requirements": [
                    {"requirement_id": "req_01", "summary": "Produce artifact."}
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
                        "description": "Artifact evidence is attached.",
                        "item_refs": ["item_01"],
                    }
                ],
            },
        )
        evidence_context = {
            "prior_general_tool_results": [
                {
                    "tool_id": "file_write",
                    "status": "ok",
                    "call_id": "call_write_report",
                    "path": r"D:\AI_WORKSPACE\base\spec480\output\report.md",
                }
            ],
            "current_general_tool_requests": [],
        }

        receipt = apply_guide_submit(
            store,
            {
                "guide_id": f"task:{task_id}",
                "submissions": [
                    {
                        "item_id": "task_progress",
                        "option_id": "update_task_status",
                        "evidence_refs": ["call_write_report"],
                        "fields": {
                            "items": {"item_01": "done"},
                            "acceptance": {"acc_01": "done"},
                        },
                    }
                ],
            },
            evidence_context=evidence_context,
        )

        fact = format_protocol_tool_fact(
            receipt,
            fact_context={"workbench_store": store},
        )

        assert "Spec480 final task" in fact
        assert "item_01" in fact
        assert "done" in fact
        assert "acc_01" in fact
        assert "call_write_report" in fact
        assert "active task guide" in fact
        assert "reaction_finalize_finish" not in fact
        assert "next_action" not in fact

    def test_spec575_mixed_finalize_with_guide_submit_post_settles_continue(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from logic.sandbox_grant import SANDBOX_GRANT_ENV
        from logic.task_guide import materialize_initial_task_guide

        rt = self._make_runtime(tmp_path)
        task_root = tmp_path / "task"
        output_root = task_root / "output"
        output_root.mkdir(parents=True)
        monkeypatch.setenv(
            SANDBOX_GRANT_ENV,
            json.dumps({
                "phase": "spec481",
                "task_root": str(task_root),
                "read_paths": [str(task_root)],
                "write_paths": [str(output_root)],
                "shell_cwd": str(task_root),
                "allowed_tools": ["file_read", "file_write", "file_search"],
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
                "task_title": "Spec481 mixed finalize task",
                "task_goal": "Prove mixed reaction_finalize is settled after tools.",
                "source_requirements": [
                    {"requirement_id": "req_01", "summary": "Produce one artifact."}
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

        class MixedFinalizeGuideExecutor:
            def __init__(self):
                self.calls = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                self.calls.append((logical, list(active_protocol_tool_guides or []), list(messages)))
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_write",
                            {
                                "path": str(output_root / "report.md"),
                                "content": "done",
                                "purpose": "Spec481 evidence",
                            },
                            call_id="call_spec481_write",
                            tool_family="general_tool",
                            tool_class="write_tool",
                            risk="medium",
                        )],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "Spec575 mixed response settled tools before handoff.",
                        "tool_call_envelopes": [
                            helper._native_reaction_finalize(
                                call_id="call_spec575_mixed_handoff",
                                handoff_text="下一轮从已完成任务账本继续检查后续材料。",
                            ),
                            helper._native_tool_envelope(
                                "guide_submit",
                                {
                                    "guide_id": f"task:{task_id}",
                                    "submissions": [{
                                        "item_id": "task_progress",
                                        "option_id": "update_task_status",
                                        "evidence_refs": ["call_spec481_write"],
                                        "fields": {
                                            "items": {"item_01": "done"},
                                            "acceptance": {"acc_01": "done"},
                                        },
                                    }],
                                },
                                call_id="call_spec481_guide_done",
                                tool_family="protocol_tool",
                                tool_class="sync_tool",
                                risk="medium",
                                index=1,
                            ),
                        ],
                    }
                raise AssertionError("Spec575 should accept mixed handoff after tool settlement")

        executor = MixedFinalizeGuideExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_reaction_finalize_validated"] is True
        assert result["_exit_signal"] == "continue_requested"
        assert result["response"] == ""
        assert rt.sm.get_flags().get("continue_requested") is True
        assert result["_guide_submit_receipts"][0]["next_action"] == "natural_final_reply"
        assert len(executor.calls) == 2
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "continue"
        assert result["_closeout_relay_receipts"][-1]["handoff_text"] == (
            "下一轮从已完成任务账本继续检查后续材料。"
        )
        assert not any(
            request.get("tool_id") == "reaction_finalize"
            for request in result["_invalid_tool_requests"]
        )

    def test_spec480_chronicle_guide_fact_includes_written_entry_content(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        receipt = {
            "tool_id": "guide_submit",
            "status": "applied",
            "guide_id": "calendar:R480",
            "accepted_submissions": [
                {
                    "item_id": "calendar_day_due",
                    "option_id": "write_chronicle",
                    "fields": {
                        "content": "2026-06-30 handled Spec480 chronicle entry.",
                        "reason": "day rhythm",
                    },
                    "evidence_refs": ["C-48001"],
                }
            ],
            "backend_receipts": [
                {
                    "tool_id": "chronicle_write",
                    "status": "applied",
                    "layer": "chronicle",
                    "path": r"WB\chronicle\2026-06-30.md",
                    "round_num": 480,
                }
            ],
            "completed_flags": ["calendar_day_due"],
            "active_guide": None,
        }

        fact = format_protocol_tool_fact(receipt)

        assert "calendar:R480" in fact
        assert "calendar_day_due" in fact
        assert "2026-06-30 handled Spec480 chronicle entry." in fact
        assert r"WB\chronicle\2026-06-30.md" in fact
        assert "C-48001" in fact
        assert "reaction_finalize_finish" not in fact
        assert "next_action" not in fact

    def test_spec489_task_evidence_rejection_fact_names_unknown_refs_and_usable_refs(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        fact = format_protocol_tool_fact({
            "tool_id": "guide_submit",
            "status": "rejected",
            "reason": "task_completion_evidence_not_found",
            "details": {
                "unknown_evidence_refs": [
                    "EV-2A31C36818",
                    "command:python --version && node --version && git --version",
                ],
                "known_evidence_refs": [
                    "EV-B4761A05E4",
                    "EV-CD332F4D42",
                    "command:python output/04_sales_report.py",
                    "command:python output/05_task_sort_fixed.py",
                    "command:dir /b output && python -c \"long verifier\"",
                ],
            },
        })

        assert "task_completion_evidence_not_found" in fact
        assert "未知证据引用" in fact
        assert "EV-2A31C36818" in fact
        assert "command:python --version && node --version && git --version" in fact
        assert "可改用证据引用" in fact
        assert "EV-B4761A05E4" in fact
        assert "EV-CD332F4D42" in fact
        assert "command:python output/04_sales_report.py" in fact
        assert "删除上方未知证据" in fact

    def test_spec597_task_evidence_rejection_fact_shows_usable_evidence_map(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        fact = format_protocol_tool_fact({
            "tool_id": "guide_submit",
            "status": "rejected",
            "reason": "task_completion_evidence_not_found",
            "details": {
                "unknown_evidence_refs": ["EV-SELFWRITTEN"],
                "known_evidence_refs": ["EV-GOODFILE", "call_run_report"],
                "known_evidence_items": [
                    {
                        "ref": "EV-GOODFILE",
                        "tool_id": "file_write",
                        "summary": "file_write 写入: output/report.md",
                    },
                    {
                        "ref": "call_run_report",
                        "tool_id": "shell_command",
                        "summary": "shell_command 运行: python output/report.py",
                    },
                ],
                "hint": (
                    "报告正文里自写的 EV-* 不是 Runtime evidence；"
                    "直接选用下方可改用证据，不要全盘搜索 EV 字符串。"
                ),
            },
        })

        assert "未知证据引用" in fact
        assert "EV-SELFWRITTEN" in fact
        assert "可改用证据（含来源）" in fact
        assert "EV-GOODFILE — file_write 写入: output/report.md" in fact
        assert "call_run_report — shell_command 运行: python output/report.py" in fact
        assert "报告正文里自写的 EV-* 不是 Runtime evidence" in fact
        assert "不要全盘搜索 EV 字符串" in fact

    def test_spec480_alert_and_cache_guide_facts_keep_settlement_snapshot(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        alert_fact = format_protocol_tool_fact({
            "tool_id": "guide_submit",
            "status": "applied",
            "guide_id": "emergency:R480",
            "accepted_submissions": [
                {
                    "item_id": "api_degraded",
                    "option_id": "settle_alert",
                    "fields": {
                        "alert_type": "api_degraded",
                        "status": "recovered",
                        "summary": "Provider recovered after retry.",
                        "clear_flags": ["api_degraded"],
                        "fault_refs": ["FAULT-480"],
                    },
                },
                {
                    "item_id": "process_down",
                    "option_id": "record_fault",
                    "fields": {
                        "fault_type": "process_down",
                        "severity": "warning",
                        "step": "reaction",
                        "detail": "Worker restarted cleanly.",
                        "action": "keep monitoring",
                    },
                },
            ],
            "backend_receipts": [
                {
                    "tool_id": "alert_mode_settle",
                    "status": "applied",
                    "alert_type": "api_degraded",
                    "cleared_flags": ["api_degraded"],
                    "fault_refs": ["FAULT-480"],
                },
                {
                    "tool_id": "fault_record",
                    "status": "applied",
                    "fault_type": "process_down",
                    "severity": "warning",
                    "fault_id": "FAULT-481",
                },
            ],
            "completed_flags": ["api_degraded", "process_down"],
        })

        assert "api_degraded" in alert_fact
        assert "Provider recovered after retry." in alert_fact
        assert "FAULT-480" in alert_fact
        assert "process_down" in alert_fact
        assert "Worker restarted cleanly." in alert_fact
        assert "reaction_finalize_finish" not in alert_fact
        assert "next_action" not in alert_fact

        cache_fact = format_protocol_tool_fact({
            "tool_id": "guide_submit",
            "status": "applied",
            "guide_id": "cache_compaction:R480",
            "accepted_submissions": [
                {
                    "item_id": "cache_compaction_due",
                    "option_id": "submit_cache_compaction_shard",
                    "fields": {
                        "shard_id": "shard_001",
                        "source_block_ids": ["C-old01", "C-old02"],
                        "summary": "Compressed old tool facts and repeated search logs.",
                        "output_chars": 1024,
                    },
                }
            ],
            "backend_receipts": [
                {
                    "tool_id": "cache_compact",
                    "status": "applied",
                    "shard_id": "shard_001",
                }
            ],
            "cache_compaction": {
                "status": "completed",
                "before_chars": 270000,
                "target_chars": 166860,
                "current_chars": 160000,
                "completed_shards": ["shard_001"],
                "all_done": True,
            },
            "completed_flags": ["cache_compaction_due"],
        })

        assert "cache_compaction:R480" in cache_fact
        assert "shard_001" in cache_fact
        assert "C-old01" in cache_fact
        assert "Compressed old tool facts" in cache_fact
        assert "270000" in cache_fact
        assert "166860" in cache_fact
        assert "160000" in cache_fact
        assert "reaction_finalize_finish" not in cache_fact
        assert "next_action" not in cache_fact

    def test_spec419_protocol_read_receipts_keep_total_size_visible(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        fact = format_protocol_tool_fact({
            "tool_id": "container_read",
            "status": "accepted",
            "container_id": "DC-SPEC419",
            "target_file": "open.md",
            "total_lines": 12,
            "total_chars": 345,
            "content": "正文不应进入工具事实",
        })

        assert "容器编号：DC-SPEC419。" in fact
        assert "目标文件：open.md。" in fact
        assert "总行数：12。" in fact
        assert "总字符数：345。" in fact
        assert "正文不应进入工具事实" not in fact

    def test_spec338_failed_protocol_tool_fact_points_to_popup_only(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        fact = format_protocol_tool_fact({
            "tool_id": "memory_container_write",
            "status": "rejected",
            "reason": "focus_not_visible_at_iteration_start",
            "call_id": "call_write_failed",
            "container_id": "DC-338",
        })

        assert "处理结果：rejected。" in fact
        assert "失败详情：请查看 POPUP 中相同 tool_id/call_id 的工具提醒。" in fact
        assert "若 POPUP 与本工具事实的 tool_id/call_id 不对应，请忽略该 POPUP。" in fact
        assert "focus_not_visible_at_iteration_start" not in fact
        assert "不要" not in fact
        assert "下一次必须" not in fact

        duplicate_fact = format_protocol_tool_fact({
            "tool_id": "container_focus",
            "status": "rejected",
            "reason": "duplicate_container_focus_satisfied",
            "call_id": "call_focus_duplicate",
            "duplicate_of_call_id": "call_focus_first",
            "container_id": "DC-338",
        })

        assert "重复命中：本轮已有同一协议工具结果。" in duplicate_fact
        assert "重复对象：call_focus_first。" in duplicate_fact
        assert "工具循环警告" not in duplicate_fact
        assert "duplicate_container_focus_satisfied" not in duplicate_fact
        assert "不要原样重复调用" not in duplicate_fact

    def test_spec406_chronicle_no_active_focus_fact_is_actionable(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        fact = format_protocol_tool_fact({
            "tool_id": "chronicle_write",
            "status": "rejected",
            "reason": "no_active_chronicle_focus",
            "call_id": "call_chronicle_stale_focus",
        })

        assert "chronicle_write" in fact
        assert "call_chronicle_stale_focus" in fact
        assert "no_active_chronicle_focus" in fact
        assert "当前没有编年史写入焦点" in fact
        assert "不要继续调用 chronicle_write" in fact
        assert "reaction_finalize" in fact

    def test_spec417_memory_subject_failure_fact_is_actionable_not_empty_pointer(self):
        from engines.reaction_helpers import format_protocol_tool_fact

        fact = format_protocol_tool_fact({
            "tool_id": "memory_write",
            "status": "error",
            "reason": "subject_not_confirmed",
            "call_id": "call_memory_subject",
            "title": "对象未确认",
            "subject": "Other",
            "submitted_subject": "Other",
            "confirmed_subject": "TzPz",
            "confirmed_subjects": ["TzPz"],
        })

        assert "处理结果：error。" in fact
        assert "失败原因：subject_not_confirmed。" in fact
        assert "提交主题：Other。" in fact
        assert "当前确认对象：TzPz。" in fact
        assert "NO-GO" in fact
        assert "identity_resolution" not in fact
        assert "relation_read" not in fact
        assert "relation_card_write" not in fact
        assert "重新调用 memory_write" not in fact
        assert "失败详情：请查看 POPUP" not in fact

    def test_spec396_protocol_read_fact_excludes_body_material_projects_index_view(self):
        from engines.reaction_helpers import (
            format_protocol_tool_fact,
            format_protocol_tool_material_entry,
        )

        memory_fact = format_protocol_tool_fact({
            "tool_id": "memory_content_read",
            "status": "accepted",
            "mem_id": "MEM-SPEC396",
            "memory_layer": "STM",
            "body": "记忆正文不应进入工具事实",
        })
        assert "记忆编号：MEM-SPEC396。" in memory_fact
        assert "记忆正文不应进入工具事实" not in memory_fact
        assert "下面是本轮实际读到的内容" not in memory_fact

        container_fact = format_protocol_tool_fact({
            "tool_id": "container_read",
            "status": "accepted",
            "container_id": "DC-SPEC396",
            "target_file": "open.md",
            "content": "容器正文不应进入工具事实",
        })
        assert "容器编号：DC-SPEC396。" in container_fact
        assert "目标文件：open.md。" in container_fact
        assert "容器正文不应进入工具事实" not in container_fact

        relation_fact = format_protocol_tool_fact({
            "tool_id": "relation_read",
            "status": "accepted",
            "card_id": "REL-SPEC396",
            "subject": "Codex",
            "summary": "关系摘要不应进入工具事实",
            "body": "关系正文不应进入工具事实",
        })
        assert "主题：Codex。" in relation_fact
        assert "REL-SPEC396" not in relation_fact
        assert "关系摘要不应进入工具事实" not in relation_fact
        assert "关系正文不应进入工具事实" not in relation_fact

        index_receipt = {
            "tool_id": "index_view",
            "status": "accepted",
            "scope": "ltm_inverted",
            "offset": 8,
            "limit": 8,
            "content": "INDEX_VIEW_EXPANDED_ROW",
        }
        index_fact = format_protocol_tool_fact(index_receipt)
        material = format_protocol_tool_material_entry(index_receipt)
        assert "INDEX_VIEW_EXPANDED_ROW" not in index_fact
        assert material["kind"] == "material"
        assert material["role"] == "system"
        assert material["tool_id"] == "index_view"
        assert material["content"] == "INDEX_VIEW_EXPANDED_ROW"
        assert material["material_source"] == "read_tool_result"





    def test_spec255_relay_finalize_without_progress_is_blocked(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("continue_requested", False)
        helper = self

        class NoProgressRelayExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "should not become final reply", "tool_call_envelopes": []}
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id=f"call_no_progress_{len(self.calls)}",
                        handoff_text=(
                            "下一轮起手：repeat setup\n"
                            "下一轮反应：repeat reaction\n"
                            "中继原因：no progress but asks relay again"
                        ),
                    )],
                }

        executor = NoProgressRelayExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])
        flags = rt.sm.get_flags()

        assert [step for step, _ in executor.calls] == ["reaction", "reaction", "reaction"]
        assert result["response"] == ""
        assert flags["continue_requested"] is False
        assert result["_closeout_relay_receipts"] == []
        assert any(
            receipt.get("status") == "relay_execution_missing"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert any(
            item.get("reason") == "relay_execution_missing"
            for item in result["_invalid_tool_requests"]
        )
        assert result["_tool_transaction_audit"]["status"] == "issues_found"

    def test_spec381_relay_progress_counts_as_progress_before_relay(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("continue_requested", False)
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "start_line": int(request.get("line_start") or 1),
                "end_line": 20,
                "has_more": False,
                "read_mode": "bounded",
                "content": "file content",
            }

        class ProgressThenRelayExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "progress accepted", "tool_call_envelopes": []}
                if len([call for call in self.calls if call[0] == "reaction"]) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "UPSP_Base_DDS.md",
                                "line_start": 1,
                                "reason": "relay progress before handoff",
                            },
                            call_id="call_relay_progress_read",
                        )],
                    }
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_progress_relay_final",
                        handoff_text=(
                            "下一轮起手：setup next\n"
                            "下一轮反应：reaction next\n"
                            "中继原因：progress happened"
                        ),
                    )],
                }

        rt.executor = ProgressThenRelayExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])

        assert result["response"] == ""
        assert result["_closeout_relay_receipts"][0]["status"] == "continue_requested_set"
        assert not any(
            receipt.get("status") == "relay_execution_missing"
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec256_relay_file_read_target_blocks_wrong_kind_progress(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()
        state["base"]["runtime"]["pending_relay_target"] = {
            "kind": "tool",
            "tool_id": "file_read",
            "path": "book.md",
            "next_start_line": 891,
            "source": "file_read_result",
            "source_call_id": "call_previous_read",
        }
        rt.sm.save(state)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("continue_requested", False)
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_search_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "query": request.get("query"),
                "matches": ["old material"],
            }

        class WrongKindProgressExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "should not finalize", "tool_call_envelopes": []}
                reaction_count = len([item for item in self.calls if item[0] == "reaction"])
                if reaction_count == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_search",
                            {
                                "query": "旧材料",
                                "reason": "wrong kind progress before relay",
                            },
                            call_id="call_wrong_kind_file_search",
                        )],
                    }
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id=f"call_wrong_kind_finalize_{reaction_count}",
                        handoff_text=(
                            "下一轮起手：setup continue\n"
                            "下一轮反应：continue reading\n"
                            "中继原因：asks to continue without file_read"
                        ),
                    )],
                }

        executor = WrongKindProgressExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])

        assert [step for step, _ in executor.calls] == [
            "reaction", "reaction", "reaction", "reaction"]
        assert result["response"] == ""
        assert result["_closeout_relay_receipts"] == []
        assert any(
            receipt.get("status") == "relay_target_unfulfilled"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert any(
            item.get("reason") == "relay_target_unfulfilled"
            for item in result["_invalid_tool_requests"]
        )

    def test_spec256_cleanup_persists_next_relay_target(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        target = {
            "kind": "tool",
            "tool_id": "file_read",
            "path": "book.md",
            "next_start_line": 891,
            "source": "file_read_result",
            "source_call_id": "call_read",
        }
        result = {
            "_closeout_relay_receipts": [{
                "status": "continue_requested_set",
                "source": "closeout_form",
                "set_flags": ["continue_requested"],
            }],
            "_pending_relay_target": dict(target),
        }

        rt.cleanup_pipeline._rearm_continue_requested_from_closeout_form(
            result,
            round_type="relay",
            consumed_flags=["continue_requested"],
            round_num=1,
        )

        assert rt.sm.load()["base"]["runtime"]["pending_relay_target"] == target
        assert any(
            receipt.get("status") == "pending_relay_target_set"
            for receipt in result["_relay_target_state_receipts"]
        )

    def test_spec255_relay_complete_without_new_relay_is_not_blocked(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class CompleteRelayExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "relay completed", "tool_call_envelopes": []}
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        rt.executor = CompleteRelayExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])

        assert result["response"] == "本轮已完成。"
        assert result["_closeout_relay_receipts"] == []
        assert not any(
            receipt.get("status") == "relay_execution_missing"
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec240_reaction_finalize_relay_closeout_sets_continue_requested(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("user_message_waiting", True)
        rt.sm.set_flag("continue_requested", False)
        helper = self
        handoff_text = "读书任务尚未完成；下一轮挂载读书上下文，并从第581行继续读。"

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_reaction_finalize(
                    call_id="call_relay_final",
                    handoff_text=handoff_text,
                )],
            },
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        flags = rt.sm.get_flags()

        assert result["_protocol_tool_submissions"] == []
        assert result["_closeout_relay_receipts"] == [{
            "tool_id": "reaction_finalize",
            "tool_family": "substrate_tool",
            "tool_class": "sync_tool",
            "status": "continue_requested_set",
            "source": "closeout_form",
            "set_flags": ["continue_requested"],
            "reason": handoff_text,
            "handoff_text": handoff_text,
            "call_id": "call_relay_final",
            "provider": "openai_responses",
            "response_id": "resp_call_relay_final",
            "provider_item_id": "fc_call_relay_final",
            "index": 0,
        }]
        assert flags["continue_requested"] is True

    def test_spec375_reaction_finalize_handoff_text_creates_intent_not_visible_relay_input(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("continue_requested", False)
        helper = self
        handoff_text = "下一轮继续读取 book.md 第 121 行，并判断是否需要写入读书收获。"

        class HandoffTextExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "handoff text recorded", "tool_call_envelopes": []}
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_spec290_handoff",
                        handoff_text=handoff_text,
                    )],
                }

        rt.executor = HandoffTextExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        flags = rt.sm.get_flags()

        assert result["response"] == ""
        assert flags["continue_requested"] is True
        assert result["_closeout_relay_receipts"][0]["status"] == "continue_requested_set"
        assert result["_closeout_relay_receipts"][0]["reason"] == handoff_text
        assert not any(
            entry.get("kind") == "relay_input"
            for entry in rt.ctx_store.get_now_entries()
        )
        assert not any(
            entry.get("kind") == "relay_input"
            for entry in rt.ctx_store.get_lately_entries("reaction")
        )
        rt.cleanup_pipeline._rearm_continue_requested_from_closeout_form(
            result,
            round_type="interactive",
            consumed_flags=[],
            round_num=rt.sm.get_total_round(),
        )
        relay_intents = rt.sm.get("base.runtime.relay_intents", [])
        assert len(relay_intents) == 1
        assert relay_intents[0]["handoff_text"] == handoff_text
        assert relay_intents[0]["relay_intent_id"].startswith("RLY-")

    def test_spec240_incomplete_relay_closeout_is_rewritten_before_setting_flag(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("continue_requested", False)
        helper = self

        class IncompleteRelayExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "改写后普通收束。", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_bad_relay",
                            closeout_decision="continue",
                            memory_settlement={"status": "weight_zero", "reason": "unit test"},
                            read_settlement={"status": "not_applicable", "reason": "unit test"},
                        )],
                    }
                return {
                    "response": "改写后普通收束。",
                    "tool_call_envelopes": [],
                }

        executor = IncompleteRelayExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        flags = rt.sm.get_flags()

        assert len(executor.calls) == 2
        assert result["response"] == "改写后普通收束。"
        assert flags["continue_requested"] is False
        assert any(
            request.get("tool_id") == "reaction_finalize"
            for request in result["_invalid_tool_requests"]
        )

    def test_spec060_reaction_fault_record_submission_writes_fault_note_and_alert(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        rt.sm.set_flag("api_degraded", True)
        guide_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"api_degraded": True},
            round_num=rt.sm.get_total_round(),
        )

        class FaultRecordExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "fault recorded", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide_id,
                                "submissions": [{
                                    "item_id": "api_degraded",
                                    "option_id": "record_fault",
                                    "fields": {
                                        "fault_type": "tool_failure",
                                        "severity": "error",
                                        "step": "reaction",
                                        "source": "web_search",
                                        "detail": "external tool timeout",
                                        "action": "fallback",
                                        "related_tool_id": "web_search",
                                    },
                                }],
                            },
                            call_id="call_fault_record",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                return {
                    "response": "fault recorded",
                    "tool_call_envelopes": [],
                }

        class CapturingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

        class CapturingAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        rt.executor = FaultRecordExecutor()
        rt.ctx_store = CapturingContext()
        rt.alert_store = CapturingAlerts()

        result = rt._run_reaction_loop(rt.sm.load(), "rhythm", [])

        assert result["_protocol_tool_submissions"] == ["guide_submit"]
        backend = result["_guide_submit_receipts"][0]["backend_receipts"][0]
        assert backend["tool_id"] == "fault_record"
        assert backend["status"] == "applied"
        assert rt.alert_store.entries == [{
            "round_num": rt.sm.get_total_round(),
            "step": "reaction",
            "event_type": "tool_failure:error",
            "detail": "web_search: external tool timeout",
            "action": "fallback",
        }]
        assert any(entry[3] == "fault_note" for entry in rt.ctx_store.entries)
        assert any("external tool timeout" in entry[2] for entry in rt.ctx_store.entries)
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert result["_tool_transaction_audit"]["issues"] == []

    def test_reaction_chronicle_write_submission_writes_chronicle_entry(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        rt.sm.set_flag("rhythm_due", True)
        guide_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"rhythm_due": True},
            round_num=238,
        )

        class CapturingChronicleStore:
            def __init__(self):
                self.entries = []

            def write_entry(self, layer, content):
                self.entries.append((layer, content))
                return str(tmp_path / "Chronicle" / layer / "R-000.md")

            def write_focused_entry(self, focus, content):
                layer = (focus or {}).get("layer") or "rhythms"
                return self.write_entry(layer, content)

        class ChronicleExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "processed tool result", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide_id,
                                "submissions": [{
                                    "item_id": "rhythm_due",
                                    "option_id": "write_chronicle",
                                    "fields": {
                                        "content": "主轴节律完成一次自检。",
                                        "reason": "main axis rhythm",
                                    },
                                }],
                            },
                            call_id="call_chronicle_write",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "chronicle written", "tool_call_envelopes": []}

        chronicle_store = CapturingChronicleStore()
        rt.reaction_loop_runner.chronicle_store = chronicle_store
        rt.reaction_loop_runner.chronicle_focus = {
            "layer": "rhythms",
            "round_num": 238,
            "round_type": "rhythm",
            "source_refs": ["round:238"],
        }
        rt.executor = ChronicleExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "rhythm", [])

        assert result["_protocol_tool_submissions"] == ["guide_submit"]
        backend = result["_guide_submit_receipts"][0]["backend_receipts"][0]
        assert backend["tool_id"] == "chronicle_write"
        assert backend["status"] == "applied"
        assert backend["path"].endswith("R-000.md")
        assert chronicle_store.entries == [
            ("rhythms", "主轴节律完成一次自检。")
        ]

    def test_spec443_rhythm_finish_waits_for_calendar_guide_settlement(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("calendar_day_due", True)
        helper = self
        guide_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=rt.sm.get_total_round(),
        )

        class CapturingChronicleStore:
            def __init__(self):
                self.refreshed = []
                self.entries = []

            def refresh_active_calendar(self, **kwargs):
                self.refreshed.append(dict(kwargs))
                path = tmp_path / "Chronicle" / "daily" / "D-active.md"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("今日待归档节律材料。", encoding="utf-8")
                return str(path)

            def write_focused_entry(self, focus, content):
                self.entries.append((dict(focus), content))
                return str(tmp_path / "Chronicle" / "daily" / "D-000.md")

        class CalendarExecutor:
            def __init__(self):
                self.calls = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                self.calls.append((logical, list(messages)))
                if logical == "final_reply":
                    return {"response": "日历节律已结算。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "too early",
                        "tool_call_envelopes": [],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide_id,
                                "submissions": [{
                                    "item_id": "calendar_day_due",
                                    "option_id": "write_chronicle",
                                    "fields": {
                                        "content": "今日节律事项已经归档。",
                                        "reason": "calendar day guide settlement",
                                    },
                                }],
                            },
                            call_id="call_spec443_calendar_write",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "calendar settled",
                    "tool_call_envelopes": [],
                }

        chronicle_store = CapturingChronicleStore()
        rt.reaction_loop_runner.chronicle_store = chronicle_store
        rt.executor = CalendarExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "rhythm", [])

        assert result["_reaction_finalize_validated"] is True
        assert chronicle_store.refreshed
        assert len(chronicle_store.entries) == 1
        assert chronicle_store.entries[0][0]["calendar_flag"] == "calendar_day_due"
        assert chronicle_store.entries[0][1] == "今日节律事项已经归档。"
        backend = result["_guide_submit_receipts"][0]["backend_receipts"][0]
        assert backend["status"] == "applied"
        assert backend["layer"] == "daily"
        assert any(
            receipt.get("status") == "rhythm_guide_blocked"
            and "calendar_day_due" in receipt.get("blockers", [])
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert result["_exit_signal"] == "done"

    def test_spec443_memory_write_can_interleave_before_rhythm_finish(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        memory_store, memory_index, _container_store = self._patch_memory_immediate_stores(
            monkeypatch,
            runtime=rt,
        )
        rt.sm.set_flag("calendar_day_due", True)
        helper = self
        guide_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=rt.sm.get_total_round(),
        )

        class CapturingChronicleStore:
            def __init__(self):
                self.entries = []

            def refresh_active_calendar(self, **kwargs):
                path = tmp_path / "Chronicle" / "daily" / "D-active-memory.md"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("今日节律焦点材料。", encoding="utf-8")
                return str(path)

            def write_focused_entry(self, focus, content):
                self.entries.append((dict(focus), content))
                return str(tmp_path / "Chronicle" / "daily" / "D-memory.md")

        class MemoryThenRhythmExecutor:
            def __init__(self):
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "记忆和节律均已结算。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "节律插写",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "节律处理中穿插的主体沉淀不应被清单剥夺。",
                                "candidate_keywords": ["Spec443", "memory"],
                            },
                            call_id="call_spec443_memory_interleave",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide_id,
                                "submissions": [{
                                    "item_id": "calendar_day_due",
                                    "option_id": "write_chronicle",
                                    "fields": {
                                        "content": "节律处理中允许先沉淀记忆，随后归档日历。",
                                        "reason": "calendar settlement after memory",
                                    },
                                }],
                            },
                            call_id="call_spec443_calendar_after_memory",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "settled",
                    "tool_call_envelopes": [],
                }

        chronicle_store = CapturingChronicleStore()
        rt.reaction_loop_runner.chronicle_store = chronicle_store
        rt.executor = MemoryThenRhythmExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "rhythm",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        assert result["_reaction_finalize_validated"] is True
        assert result["_memory_write_receipts"][0]["status"] == "applied"
        assert memory_store.entries[0][1] == "节律插写"
        assert memory_index.keywords == [("MEM-131000AA", ["Spec443", "memory"])]
        backend = result["_guide_submit_receipts"][0]["backend_receipts"][0]
        assert backend["tool_id"] == "chronicle_write"
        assert backend["status"] == "applied"
        assert chronicle_store.entries[0][1] == "节律处理中允许先沉淀记忆，随后归档日历。"
        assert not any(
            receipt.get("status") == "rhythm_guide_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec470_rhythm_continue_waits_for_guide_settlement(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("calendar_day_due", True)
        rt.sm.set_flag("continue_requested", False)
        helper = self
        guide_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=rt.sm.get_total_round(),
        )

        class CapturingChronicleStore:
            def __init__(self):
                self.entries = []

            def refresh_active_calendar(self, **kwargs):
                path = tmp_path / "Chronicle" / "daily" / "D-active-calendar.md"
                path.parent.mkdir(parents=True, exist_ok=True)
                path.write_text("calendar focus", encoding="utf-8")
                return str(path)

            def write_focused_entry(self, focus, content):
                self.entries.append((dict(focus), content))
                return str(tmp_path / "Chronicle" / "daily" / "D-calendar.md")

        class ContinueThenSettleExecutor:
            def __init__(self):
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "节律清单已结算。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_spec470_bad_continue_before_rhythm",
                            handoff_text="错误地把未结算节律清单交给下一轮。",
                    )],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide_id,
                                "submissions": [{
                                    "item_id": "calendar_day_due",
                                    "option_id": "write_chronicle",
                                    "fields": {
                                        "content": "已通过 guide_submit 结算日历节律。",
                                    },
                                }],
                            },
                            call_id="call_spec470_calendar_after_continue_block",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "settled",
                    "tool_call_envelopes": [],
                }

        chronicle_store = CapturingChronicleStore()
        rt.reaction_loop_runner.chronicle_store = chronicle_store
        rt.executor = ContinueThenSettleExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "rhythm",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        assert result["_reaction_finalize_validated"] is True
        assert chronicle_store.entries[0][1] == "已通过 guide_submit 结算日历节律。"
        assert any(
            receipt.get("status") == "rhythm_guide_blocked"
            and "calendar_day_due" in receipt.get("blockers", [])
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert result["_closeout_relay_receipts"] == []
        assert rt.sm.get_flags().get("continue_requested") is not True

    def test_spec443_emergency_finish_waits_for_auditable_settlement(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("api_degraded", True)
        helper = self
        guide_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"api_degraded": True},
            round_num=rt.sm.get_total_round(),
        )

        class CapturingAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        class EmergencyExecutor:
            def __init__(self):
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "紧急清单已结算。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "too early",
                        "tool_call_envelopes": [],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide_id,
                                "submissions": [{
                                    "item_id": "api_degraded",
                                    "option_id": "settle_alert",
                                    "fields": {
                                        "alert_type": "api_degraded",
                                        "status": "recovered",
                                        "summary": "API 已恢复，清理紧急标记。",
                                        "clear_flags": ["api_degraded"],
                                        "fault_refs": [],
                                        "next_attention": "继续正常任务。",
                                        "reason": "settled for Spec443",
                                    },
                                }],
                            },
                            call_id="call_spec443_alert_settle",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "emergency settled",
                    "tool_call_envelopes": [],
                }

        rt.alert_store = CapturingAlerts()
        rt.executor = EmergencyExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "rhythm", [])

        assert result["_reaction_finalize_validated"] is True
        backend = result["_guide_submit_receipts"][0]["backend_receipts"][0]
        assert backend["tool_id"] == "alert_mode_settle"
        assert backend["status"] == "applied"
        assert any(
            receipt.get("status") == "rhythm_guide_blocked"
            and "api_degraded" in receipt.get("blockers", [])
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert rt.sm.get("base.heartbeat_flags.api_degraded") is False

    def test_spec406_chronicle_focus_is_high_freq_content_not_cache(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler, "_build_container_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_step_toolbelt_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_content_mounts_with_triple_hits", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        focus_path = tmp_path / "Chronicle" / "daily" / "D-active-calendar.md"
        focus_path.parent.mkdir(parents=True)
        focus_path.write_text("今日节律焦点正文。", encoding="utf-8")
        rt.sm.set_flag("calendar_day_due", True)
        materialize_current_rhythm_guide(
            rt.workbench,
            {"calendar_day_due": True},
            round_num=617,
        )

        class ExistingFocusChronicleStore:
            @staticmethod
            def refresh_active_calendar(**_kwargs):
                return str(focus_path)

        rt.reaction_loop_runner.chronicle_store = ExistingFocusChronicleStore()
        rt.reaction_loop_runner.chronicle_focus = {
            "layer": "daily",
            "path": str(focus_path),
            "round_num": 617,
            "round_type": "rhythm",
            "calendar_flag": "calendar_day_due",
            "title": "日节律",
            "source_refs": ["calendar:calendar_day_due", "round:617"],
        }

        current_state = rt.sm.load()
        rt.reaction_loop_runner._sync_chronicle_focus_for_current_guide(
            round_type="rhythm",
            current_state=current_state,
            round_num=617,
            completed_flags=set(),
        )
        projection = rt.reaction_loop_runner._chronicle_focus_content_projection()
        _system, messages = assembler.assemble_reaction(
            current_state,
            "rhythm",
            runtime_focus_entries=[projection],
        )
        first_messages = "\n".join(
            msg.get("content", "") for msg in messages
        )
        assert "<!-- 高频层 -->" in first_messages
        assert "编年史写入焦点（Runtime 预填）" in first_messages
        assert "当前调用临时输入" not in first_messages
        cache_text = ""
        for path in (
                tmp_path / "context_cache" / "now_cache.jsonl",
                tmp_path / "context_cache" / "lately_cache.jsonl",
                tmp_path / "buffer" / "raw_log.jsonl",
        ):
            if path.exists():
                cache_text += path.read_text(encoding="utf-8")
        assert "chronicle_focus" not in cache_text
        assert "今日节律焦点正文" not in cache_text

    def test_spec468_retired_chronicle_write_direct_call_returns_native_warning(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class StaleChronicleExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if len(self.calls) <= 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "chronicle_write",
                            {
                                "content": "重复写入没有焦点的编年史。",
                                "reason": "stale focus",
                            },
                            call_id=f"call_stale_chronicle_{len(self.calls)}",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "blocked",
                    "tool_call_envelopes": [],
                }

        rt.executor = StaleChronicleExecutor()
        result = rt._run_reaction_loop(rt.sm.load(), "rhythm", [])

        assert result["_reaction_finalize_validated"] is True
        assert len(rt.executor.calls) >= 3
        second_call_text = "\n".join(
            msg.get("content", "") for msg in rt.executor.calls[1]
        )
        assert "原生工具调用警告" in second_call_text
        assert "停止调用未开通或缺少运行时契约的工具" in second_call_text
        assert result["_chronicle_write_receipts"] == []
        assert result["_write_pending_settlement"]["pendings"] == []

    def test_spec350_chronicle_write_rejects_without_active_focus(self):
        from logic.chronicle_write import apply_chronicle_write_declarations

        class CapturingChronicleStore:
            def write_entry(self, *args, **kwargs):
                raise AssertionError("chronicle write without focus must not write")

        receipts = apply_chronicle_write_declarations(
            [{
                "content": "bad focus",
                "reason": "no active focus",
            }],
            {"chronicle_store": CapturingChronicleStore()},
        )

        assert receipts[0]["status"] == "rejected"
        assert receipts[0]["reason"] == "no_active_chronicle_focus"

    def test_spec363_runtime_prepares_chronicle_focus_for_main_axis_rhythm(
            self, tmp_path):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)

        class CapturingChronicleStore:
            def __init__(self):
                self.refreshed = []

            def refresh_active_rhythm(self, **kwargs):
                self.refreshed.append(dict(kwargs))
                return str(tmp_path / "Chronicle" / "rhythms" / "R-active-main-axis.md")

        store = CapturingChronicleStore()
        rt.reaction_loop_runner.chronicle_store = store
        rt.sm.set("base.meta.total_round", 363)
        materialize_current_rhythm_guide(
            rt.workbench,
            {"rhythm_due": True},
            round_num=363,
        )

        focus = rt._prepare_chronicle_focus_for_round("rhythm", rt.sm.load(), 363)

        assert store.refreshed
        assert focus["layer"] == "rhythms"
        assert focus["round_type"] == "rhythm"
        assert focus["round_num"] == 363
        assert rt.reaction_loop_runner.chronicle_focus == focus

    def test_spec350_chronicle_write_uses_active_focus_metadata(self):
        from logic.chronicle_write import apply_chronicle_write_declarations

        class CapturingChronicleStore:
            def __init__(self):
                self.focus_entries = []

            def write_focused_entry(self, focus, content):
                self.focus_entries.append((dict(focus), content))
                return "Chronicle/rhythms/R-active.md"

        store = CapturingChronicleStore()
        focus = {
            "layer": "rhythms",
            "round_num": 352,
            "round_type": "rhythm",
            "source_refs": ["active-rhythm"],
        }
        receipts = apply_chronicle_write_declarations(
            [{"content": "节志正文", "reason": "write current focus"}],
            {"chronicle_store": store, "chronicle_focus": focus},
        )

        assert receipts[0]["status"] == "applied"
        assert receipts[0]["layer"] == "rhythms"
        assert receipts[0]["round_num"] == 352
        assert store.focus_entries == [(focus, "节志正文")]

    def test_spec350_active_rhythm_file_refresh_records_weight_counts(self, tmp_path):
        from data.chronicle_store import ChronicleStore

        store = ChronicleStore(chronicle_dir=str(tmp_path / "Chronicle"))
        path = store.refresh_active_rhythm(
            round_num=12,
            closed_at="2026-06-18T12:00:00+08:00",
            state_sample={"dynamic_axes": {"focus": 6}, "workhood_index": {"value": 61}},
            memory_stats={"total": 3, "weights": {"F": 1, "S": 2, "A": 0, "P": 0}},
        )

        text = open(path, "r", encoding="utf-8").read()
        assert "range_end_round: 12" in text
        assert "新增记忆总数: 3" in text
        assert "F: 1" in text
        assert "S: 2" in text

    def test_reaction_alert_mode_settle_clears_flags_and_writes_alert(
            self, tmp_path, monkeypatch):
        from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("api_degraded", True)
        helper = self
        guide_id = materialize_current_rhythm_guide(
            rt.workbench,
            {"api_degraded": True},
            round_num=rt.sm.get_total_round(),
        )

        class CapturingAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        class AlertExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "processed tool result", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "guide_submit",
                            {
                                "guide_id": guide_id,
                                "submissions": [{
                                    "item_id": "api_degraded",
                                    "option_id": "settle_alert",
                                    "fields": {
                                        "alert_type": "api_degraded",
                                        "status": "recovered",
                                        "summary": "接口已恢复，旧标记清理。",
                                        "clear_flags": ["api_degraded"],
                                        "fault_refs": ["STM/health/base/alerts.md"],
                                        "next_attention": "按上下文判断是否提醒。",
                                        "reason": "stale alert settled",
                                    },
                                }],
                            },
                            call_id="call_alert_settle",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "alert settled", "tool_call_envelopes": []}

        rt.alert_store = CapturingAlerts()
        rt.executor = AlertExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "rhythm", [])

        assert result["_protocol_tool_submissions"] == ["guide_submit"]
        backend = result["_guide_submit_receipts"][0]["backend_receipts"][0]
        assert backend["tool_id"] == "alert_mode_settle"
        assert backend["status"] == "applied"
        assert backend["alert_status"] == "recovered"
        assert rt.sm.get("base.heartbeat_flags.api_degraded") is False
        assert rt.alert_store.entries == [{
            "round_num": rt.sm.get_total_round(),
            "step": "reaction",
            "event_type": "api_degraded:recovered",
            "detail": "接口已恢复，旧标记清理。",
            "action": "stale alert settled",
        }]

    def test_alert_mode_settle_rejects_main_axis_rhythm_clear(self, tmp_path):
        from data.state_store import StateStore
        from logic.alert_mode_settle import (
            apply_alert_mode_settlement_declarations,
        )

        class CapturingAlerts:
            def append_alert(self, **kwargs):
                raise AssertionError("invalid alert clear must not write")

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set_flag("rhythm_due", True)

        receipts = apply_alert_mode_settlement_declarations(
            [{
                "alert_type": "api_degraded",
                "status": "recovered",
                "summary": "不能用警戒结算清主轴节律。",
                "clear_flags": ["rhythm_due"],
                "fault_refs": [],
                "next_attention": "交给主轴节律善后。",
                "reason": "wrong flag",
            }],
            1,
            {"state_store": sm, "alert_store": CapturingAlerts()},
        )

        assert receipts[0]["status"] == "error"
        assert receipts[0]["reason"] == "invalid_alert_clear_flag"
        assert receipts[0]["invalid_clear_flags"] == ["rhythm_due"]
        assert sm.get("base.heartbeat_flags.rhythm_due") is True

    def test_spec352_alert_mode_deferred_sets_default_one_hour_snooze(self, tmp_path):
        from data.state_store import StateStore
        from logic.alert_mode_settle import apply_alert_mode_settlement_declarations

        class CapturingAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        receipts = apply_alert_mode_settlement_declarations(
            [{
                "alert_type": "api_degraded",
                "status": "deferred",
                "summary": "主通道异常，备用通道可跑，稍后复查。",
                "clear_flags": [],
                "fault_refs": ["fault:api"],
                "next_attention": "一小时后复查。",
                "reason": "fallback usable",
            }],
            1,
            {"state_store": sm, "alert_store": CapturingAlerts()},
        )

        assert receipts[0]["status"] == "applied"
        assert receipts[0]["alert_status"] == "deferred"
        deferral = sm.get("base.alert_deferrals", {})["api_degraded"]
        assert deferral["status"] == "deferred"
        assert deferral["defer_seconds"] == 3600
        assert deferral["defer_until"]

    def test_spec352_runtime_auto_defer_records_receipt_and_deferral(
            self, tmp_path):
        from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher

        class CapturingAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("api_degraded", True)
        rt.alert_store = CapturingAlerts()
        dispatcher = ReactionToolSettlementDispatcher(rt)
        accumulated = []
        specific_receipts = []
        all_receipts = []

        receipts = dispatcher.record_alert_auto_defer(
            alert_type="api_degraded",
            interaction_meta={},
            accumulated_messages=accumulated,
            all_alert_mode_settle_receipts=specific_receipts,
            all_protocol_tool_receipts=all_receipts,
        )

        assert receipts[0]["status"] == "applied"
        assert receipts[0]["alert_status"] == "deferred"
        assert receipts[0]["cleared_flags"] == ["api_degraded"]
        assert specific_receipts == receipts
        assert all_receipts == receipts
        assert rt.sm.get("base.heartbeat_flags.api_degraded") is False
        deferral = rt.sm.get("base.alert_deferrals", {})["api_degraded"]
        assert deferral["status"] == "deferred"
        assert deferral["defer_seconds"] == 3600
        assert "emergency_attempt_budget_exceeded" in deferral["reason"]
        assert accumulated == []

    def test_reaction_text_tool_declaration_is_rejected_without_becoming_progress(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "start_line": int(request.get("line_start") or 1),
                "end_line": 20,
                "has_more": False,
                "read_mode": "bounded",
                "content": "file content",
            }

        class CrossIterationExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "声明未越过迭代门禁。", "tool_call_envelopes": []}
                combined = "\n".join(
                    str(message.get("content", "")) for message in messages
                )
                if len([call for call in self.calls]) == 2:
                    assert "assistant_text 中的疑似工具载荷没有执行" in combined
                    assert "【轮中进展记录】" not in combined
                    assert "本轮回复记录" not in combined
                    assert "本轮已播报进展" not in combined
                    return {
                        "response": "声明未越过迭代门禁。",
                        "tool_call_envelopes": [],
                    }
                return {
                    "response": """| field | value | note |
|------|----|------|
| protocol_tool_submission | memory_write_declaration | retired text |
| memory_write_declaration | title=early; weight=4; body=text-only | ignored |
""",
                    "tool_call_envelopes": [helper._native_tool_envelope(
                        "file_read",
                        {
                            "path": "UPSP_Base_DDS.md",
                            "line_start": 1,
                            "reason": "keep loop open for declaration feedback",
                        },
                        call_id="call_declaration_feedback_read",
                    )],
                }

        rt.executor = CrossIterationExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(rt.executor.calls) == 2
        second_context = "\n".join(
            str(msg.get("content", ""))
            for msg in rt.executor.calls[1]
        )
        assert "reaction_progress_recorded" not in second_context
        assert "reaction_progress_emit" not in second_context
        assert "assistant_text 中的疑似工具载荷没有执行" in second_context
        assert "【轮中进展记录】" not in second_context
        assert "本轮已播报进展" not in second_context
        assert result["_protocol_tool_submissions"] == []
        assert result["_memory_write_declarations"] == []
        assert not any(
            receipt.get("tool_id") == "memory_write" and receipt.get("status") == "applied"
            for receipt in result["_protocol_tool_receipts"]
        )
        assert [
            (item["tool_id"], item["reason"])
            for item in result["_invalid_tool_requests"]
        ] == [("assistant_text", "assistant_text_tool_payload")]
        assert result["_exit_signal"] == "done"
        assert not any(
            "memory_write_declaration | title=early" in item
            for item in result["_assistant_progress"]
        )
        audit = result["_tool_transaction_audit"]
        assert audit["status"] == "issues_found"

    def test_spec260_missing_reaction_finalize_retries_without_raw_text_leak(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class CorrectedExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "纠偏后自然回复", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "我已调用 file_read 并写入 MEM-FAKE。",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("plain natural final reply should close")

        executor = CorrectedExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in executor.calls] == ["reaction"]
        assert "__closeout_only__" not in executor.guides[0]
        assert result["response"] == "我已调用 file_read 并写入 MEM-FAKE。"
        assert result["_exit_signal"] == "done"
        assert result["_final_reply_done"] is True
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert result["_tool_transaction_audit"]["counts"]["corrected_invalid_requests"] == 0

    def test_spec561_bare_text_after_tools_finishes_without_closeout_only(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        leaked_text = "错误裸文本不应回灌"

        class ContinueReadAfterBareTextExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "读书继续执行后正常收束。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "line_start": 164,
                                "reason": "first reading window",
                            },
                            call_id="call_read_164",
                        )],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "line_start": 364,
                                "reason": "second reading window",
                            },
                            call_id="call_read_364",
                        )],
                    }
                if self.reaction_calls == 3:
                    return {"response": leaked_text, "tool_call_envelopes": []}
                if self.reaction_calls == 4:
                    assert "__closeout_only__" not in self.guides[-1]
                    combined = "\n".join(
                        str(message.get("content", "")) for message in messages
                    )
                    assert "reaction_progress_recorded" not in combined
                    assert "reaction_progress_emit" not in combined
                    assert leaked_text in combined
                    assert "【轮中进展记录】" in combined
                    assert "本轮已播报进展" not in combined
                    assert "不要复述已有进展文本" not in combined
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "line_start": 553,
                                "reason": "continue reading after channel correction",
                            },
                            call_id="call_read_553",
                        )],
                    }
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_finish_after_read",
                        closeout_decision="finish",
                    )],
                }

        def fake_execute(request):
            start_line = int(request.get("line_start") or 1)
            return {
                "tool_id": "file_read",
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "start_line": start_line,
                "end_line": start_line + 80,
                "has_more": start_line < 553,
                "read_mode": "bounded",
                "content": f"read from {start_line}",
            }

        executor = ContinueReadAfterBareTextExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in executor.calls] == [
            "reaction",
            "reaction",
            "reaction",
        ]
        assert result["response"] == leaked_text
        assert result["_exit_signal"] == "done"
        assert [
            item.get("call_id") for item in result["_general_tool_results"]
        ] == ["call_read_164", "call_read_364"]
        assert not any(
            receipt.get("status") == "reaction_progress_only"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert leaked_text in repr(result["_reaction_iterations"])
        assert any(
            envelope.get("channel") == "final_reply.text"
            and envelope.get("text") == leaked_text
            for envelope in result["_message_envelopes"]
        )

    def test_spec561_plain_text_finishes_without_closeout_only(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        final_text = "自然语言最终回复。"

        class PlainTextExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec561 must not call final_reply")
                assert "__closeout_only__" not in self.guides[-1]
                return {"response": final_text, "tool_call_envelopes": []}

        executor = PlainTextExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in executor.calls] == ["reaction"]
        assert "__closeout_only__" not in executor.guides[-1]
        assert result["response"] == final_text
        assert result["_exit_signal"] == "done"
        assert not any(
            receipt.get("status") == "reaction_progress_only"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert final_text in repr(result["_reaction_iterations"])

    def test_spec561_relay_plain_text_finishes_without_continue_requested(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("continue_requested", False)

        class RelayCloseoutRecoveryExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "中继已收束，等待用户下一步指令。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "我先用裸文本总结已完成的中继任务。",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("plain text should finish the relay round")

        executor = RelayCloseoutRecoveryExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])
        flags = rt.sm.get_flags()

        assert [step for step, _ in executor.calls] == ["reaction"]
        assert result["response"] == "我先用裸文本总结已完成的中继任务。"
        assert result["_exit_signal"] == "done"
        assert flags["continue_requested"] is not True
        assert result["_closeout_relay_receipts"] == []
        assert not any(
            receipt.get("status") == "relay_execution_missing"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert [
            envelope.get("channel") for envelope in result["_message_envelopes"]
        ][:1] == ["final_reply.text"]
        assert result["_tool_transaction_audit"]["status"] == "ok"

    def test_spec371_closeout_invalid_enum_retired_field_retries_then_natural_reply(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RetiredDecisionThenNaturalReplyExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec561 must not call final_reply")
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    assert "__closeout_only__" not in self.guides[-1]
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_complete_invalid_enum",
                            closeout_decision="complete",
                            handoff_text="已经完成，等待用户下一步。",
                        )],
                    }
                combined = "\n".join(
                    str(msg.get("content", ""))
                    for msg in messages
                )
                assert "closeout_decision" in combined
                assert "已退役" in combined
                assert "完成时直接自然语言回复用户" in combined
                assert "__closeout_only__" not in self.guides[-1]
                return {
                    "response": "已经完成阅读，等待下一步。",
                    "tool_call_envelopes": [],
                }

        executor = RetiredDecisionThenNaturalReplyExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in executor.calls] == [
            "reaction",
            "reaction",
        ]
        assert result["response"] == "已经完成阅读，等待下一步。"
        assert result["_exit_signal"] == "done"
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert any(
            item.get("reason") == "reaction_finalize_retired_field"
            and item.get("reaction_loop_phase") == "loop"
            for item in result["_invalid_tool_requests"]
        )
        assert result["_tool_transaction_audit"]["status"] == "issues_found"

    def test_spec372_closeout_deferred_retired_field_points_to_natural_reply_or_handoff(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class DeferredThenNaturalReplyExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec561 must not call final_reply")
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    assert "__closeout_only__" not in self.guides[-1]
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_deferred_invalid_enum",
                            closeout_decision="deferred",
                            handoff_text="等待用户明确下一步。",
                        )],
                    }
                combined = "\n".join(
                    str(msg.get("content", ""))
                    for msg in messages
                )
                assert "closeout_decision" in combined
                assert "已退役" in combined
                assert "只有跨轮继续时" in combined
                assert "__closeout_only__" not in self.guides[-1]
                return {
                    "response": "当前需要等待用户明确下一步。",
                    "tool_call_envelopes": [],
                }

        executor = DeferredThenNaturalReplyExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])

        assert [step for step, _ in executor.calls] == [
            "reaction",
            "reaction",
        ]
        assert result["response"] == "当前需要等待用户明确下一步。"
        assert result["_exit_signal"] == "done"
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert any(
            item.get("reason") == "reaction_finalize_retired_field"
            and item.get("reaction_loop_phase") == "loop"
            for item in result["_invalid_tool_requests"]
        )
        assert result["_tool_transaction_audit"]["status"] == "issues_found"

    def test_spec373_closeout_invalid_enum_retired_field_feedback_does_not_list_old_values(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RetiredDecisionThenNaturalReplyExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec561 must not call final_reply")
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    assert "__closeout_only__" not in self.guides[-1]
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_invalid_enum",
                            closeout_decision="complete",
                            handoff_text="已经完成。",
                        )],
                    }
                combined = "\n".join(
                    str(msg.get("content", ""))
                    for msg in messages
                )
                assert "closeout_decision" in combined
                assert "已退役" in combined
                assert "finish / continue / blocked" not in combined
                assert "__closeout_only__" not in self.guides[-1]
                return {
                    "response": "已经完成。",
                    "tool_call_envelopes": [],
                }

        executor = RetiredDecisionThenNaturalReplyExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])

        assert [step for step, _ in executor.calls] == [
            "reaction",
            "reaction",
        ]
        assert result["response"] == "已经完成。"
        assert result["_exit_signal"] == "done"
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert any(
            item.get("reason") == "reaction_finalize_retired_field"
            and item.get("reaction_loop_phase") == "loop"
            for item in result["_invalid_tool_requests"]
        )
        assert result["_tool_transaction_audit"]["status"] == "issues_found"

    def test_spec561_plain_text_is_not_missing_finalize(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class PlainTextExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec561 must not call final_reply")
                assert "__closeout_only__" not in list(active_protocol_tool_guides or [])
                return {
                    "response": "我已经完成当前回应。",
                    "tool_call_envelopes": [],
                }

        executor = PlainTextExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in executor.calls] == ["reaction"]
        assert result["_exit_signal"] == "done"
        assert result["response"] == "我已经完成当前回应。"
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert not any(
            item.get("reason") == "native_tool_call_required"
            and item.get("tool_id") == "reaction_finalize"
            for item in result["_invalid_tool_requests"]
        )

    def test_spec505_invalid_finalize_does_not_switch_to_closeout_only(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "path": request.get("path"),
                "content": "still allowed after invalid finalize",
            }

        class InvalidFinalizeThenToolThenFinalizeExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "合法收束后的最终回复", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_invalid_closeout_entry",
                            closeout_decision="complete",
                        )],
                    }
                if len(self.calls) == 2:
                    assert "__closeout_only__" not in self.guides[-1]
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {"path": "task_materials/practice_evidence.jsonl"},
                            call_id="call_file_read_after_invalid_finalize",
                            tool_family="general_tool",
                            tool_class="read_tool",
                            risk="low",
                        )],
                    }
                assert "__closeout_only__" not in self.guides[-1]
                return {
                    "response": "普通工具仍开放，已完成检查。",
                    "tool_call_envelopes": [],
                }

        executor = InvalidFinalizeThenToolThenFinalizeExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in executor.calls] == [
            "reaction",
            "reaction",
            "reaction",
        ]
        assert result["response"] == "普通工具仍开放，已完成检查。"
        assert result["_exit_signal"] == "done"
        assert result["_tool_transaction_audit"]["status"] == "issues_found"
        assert any(
            issue.get("detail") == "reaction_finalize_retired_field"
            for issue in result["_tool_transaction_audit"].get("issues") or []
        )
        assert any(
            item.get("tool_id") == "file_read"
            and item.get("status") == "ok"
            for item in result["_general_tool_results"]
        )
        assert not any(
            item.get("reason") == "closeout_only_tool_not_allowed"
            for item in result["_tool_transaction_audit"]["corrected_invalid_requests"]
        )
        assert not any(
            item.get("reason") == "closeout_only_tool_not_allowed"
            for item in result["_invalid_tool_requests"]
        )
        assert not any(
            str(item.get("status") or "").startswith("general_tool_duplicate")
            for item in result["_reaction_loop_guard_receipts"]
        )

    def test_reaction_progress_and_loop_handoffs_enter_context_tracks(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class StructuredExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "processed tool result", "tool_call_envelopes": []}
                return {
                    "response": "processed tool result, continue to closeout",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_progress_final",
                        handoff_text=(
                            "下一轮起手：no setup handoff\n"
                            "下一轮反应：no reaction handoff"
                        ),
                    )],
                }

        executor = StructuredExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(executor.calls) == 1
        assert result["response"] == ""
        assert result["_assistant_progress"] == [
            "processed tool result, continue to closeout"
        ]
        assert result["_tool_summaries"] == []
        now_handoffs = [
            entry["content"]
            for entry in rt.ctx_store.get_now_entries()
            if entry.get("kind") == "handoff"
        ]
        assert not any("目标：cleanup。" in text for text in now_handoffs)
        assert not any("目标：next_setup。" in text for text in now_handoffs)
        assert not any("目标：next_reaction。" in text for text in now_handoffs)
        assert result["_closeout_relay_receipts"][0]["reason"] == (
            "下一轮起手：no setup handoff\n下一轮反应：no reaction handoff"
        )
        assert not any(
            entry.get("kind") == "relay_input"
            for entry in rt.ctx_store.get_now_entries()
        )
        lately_kinds = [entry["kind"] for entry in rt.ctx_store.get_lately_entries("reaction")]
        assert "handoff" not in lately_kinds
        assert "relay_input" not in lately_kinds

    def test_reaction_loop_handoff_writes_relay_receipts_without_final_reply(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class StructuredRouteExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "structured handoff handled", "tool_call_envelopes": []}
                return {
                    "response": "text handoff pollution must be ignored",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_route_final",
                        handoff_text=(
                            "下一轮起手：next setup may mount MEM-001\n"
                            "下一轮反应：continue checking native tool tables"
                        ),
                    )],
                }

        executor = StructuredRouteExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(executor.calls) == 1
        assert result["response"] == ""
        assert "to_cleanup" not in result["_reaction_loop"]
        now_handoffs = [
            entry["content"]
            for entry in rt.ctx_store.get_now_entries()
            if entry.get("kind") == "handoff"
        ]
        assert not any("目标：cleanup。" in text for text in now_handoffs)
        assert not any("目标：next_setup。" in text for text in now_handoffs)
        assert not any("目标：next_reaction。" in text for text in now_handoffs)
        assert result["_closeout_relay_receipts"][0]["reason"] == (
            "下一轮起手：next setup may mount MEM-001\n"
            "下一轮反应：continue checking native tool tables"
        )
        assert not any(
            entry.get("kind") == "relay_input"
            for entry in rt.ctx_store.get_now_entries()
        )
        assert not any(
            entry.get("kind") == "relay_handoff"
            for entry in rt.ctx_store.get_now_entries()
        )
        assert not any(
            entry.get("kind") == "handoff"
            for entry in rt.ctx_store.get_lately_entries("reaction")
        )
        assert not any(
            entry.get("kind") == "relay_input"
            for entry in rt.ctx_store.get_lately_entries("reaction")
        )
        assert not any(
            entry.get("kind") == "relay_handoff"
            for entry in rt.ctx_store.get_lately_entries("reaction")
        )

    def test_spec397_reaction_progress_is_not_final_reply_material(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        progress_text = "本轮过程进展不应该作为 dialogue_progress 进入最终回复上下文。"

        class ProgressThenFinishExecutor:
            def __init__(self):
                self.calls = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append({
                    "step": _logical_step(step, active_protocol_tool_guides),
                    "messages": list(messages),
                })
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "clean final reply", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": progress_text,
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("plain natural final reply should close")

        executor = ProgressThenFinishExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        assert [call["step"] for call in executor.calls] == ["reaction"]
        assert result["response"] == progress_text
        assert result["_assistant_progress"] == []

    def test_spec525_reaction_loop_passes_iteration_for_progress_folding(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class ProgressFoldExecutor:
            def __init__(self):
                self.calls = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = _logical_step(step, active_protocol_tool_guides)
                self.calls.append({
                    "step": logical,
                    "messages_text": "\n".join(
                        str((message or {}).get("content") or "")
                        for message in messages
                    ),
                })
                if logical == "final_reply":
                    return {"response": "旧 final_reply 不该被调用", "tool_call_envelopes": []}
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "FIRST_PROGRESS_FULL_TEXT",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {"path": "progress_1.txt", "reason": "keep loop open"},
                            call_id="call_spec525_read_1",
                        )],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "SECOND_PROGRESS_FULL_TEXT",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {"path": "progress_2.txt", "reason": "keep loop open"},
                            call_id="call_spec525_read_2",
                        )],
                    }
                return {
                    "response": "FINAL_TEXT",
                    "tool_call_envelopes": [],
                }

        def fake_execute(request):
            return {
                "tool_id": "file_read",
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "content": "read ok",
            }

        executor = ProgressFoldExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [call["step"] for call in executor.calls] == [
            "reaction",
            "reaction",
            "reaction",
        ]
        assert "FIRST_PROGRESS_FULL_TEXT" in executor.calls[1]["messages_text"]
        assert "FIRST_PROGRESS_FULL_TEXT" not in executor.calls[2]["messages_text"]
        assert "轮中进展正文已折叠" in executor.calls[2]["messages_text"]
        assert "SECOND_PROGRESS_FULL_TEXT" in executor.calls[2]["messages_text"]
        assert result["response"] == "FINAL_TEXT"

    def test_spec408_final_reply_keeps_interaction_cache_and_call_placeholder(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.ctx_store.append_to_cache(
            0,
            "user",
            "请读取并内化这本书。path=D:\\AI_WORKSPACE\\base\\book\\共格主体论_V5_6.1.md",
            kind="interaction",
            interaction_object="Codex",
            identity_status="declared",
            interaction_source="setup_interaction",
        )

        class FinishExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append({"step": _logical_step(step, active_protocol_tool_guides), "messages": list(messages)})
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec560 should not call final_reply")
                return {
                    "response": "已经先处理节律，然后继续读书。",
                    "tool_call_envelopes": [],
                }

        executor = FinishExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "rhythm", [])
        assert [call["step"] for call in executor.calls] == ["reaction"]
        assert result["response"] == "已经先处理节律，然后继续读书。"
        assert not any(
            entry.get("kind") == "final_reply_query"
            for entry in rt.ctx_store.get_now_entries()
        )
        assert not any(
            entry.get("kind") == "final_reply_query"
            for entry in rt.ctx_store.get_lately_entries("reaction")
        )

    def test_spec292_final_reply_sees_transient_closeout_material(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class FinishExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append({
                    "step": _logical_step(step, active_protocol_tool_guides),
                    "messages": list(messages),
                })
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec560 should not call final_reply")
                return {
                    "response": "用户可见最终回复",
                    "tool_call_envelopes": [],
                }

        executor = FinishExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        assert [call["step"] for call in executor.calls] == ["reaction"]
        assert result["response"] == "用户可见最终回复"
        assert not any(
            entry.get("transient_scope") == "final_reply"
            for entry in rt.ctx_store.get_now_entries()
        )

    def test_spec294_final_reply_empty_retries_once_and_uses_retry_text(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class EmptyThenTextExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append({"step": _logical_step(step, active_protocol_tool_guides), "messages": list(messages)})
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec560 should not call final_reply")
                return {
                    "response": "已完成，本轮可以进入下一轮。",
                    "tool_call_envelopes": [],
                }

        executor = EmptyThenTextExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        final_reply_calls = [call for call in executor.calls if call["step"] == "final_reply"]
        assert len(final_reply_calls) == 0
        assert result["response"] == "已完成，本轮可以进入下一轮。"
        assert result["_final_reply_done"] is True
        assert result["_final_response_source"] == "reaction.natural_final_reply"

    def test_spec294_final_reply_empty_twice_keeps_empty_failure(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class AlwaysEmptyExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(_logical_step(step, active_protocol_tool_guides))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec560 should not call final_reply")
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        rt.executor = AlwaysEmptyExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert rt.executor.calls == ["reaction"]
        assert result["response"] == "本轮已完成。"
        assert result["_exit_signal"] == "done"
        assert result["_final_reply_done"] is True
        assert result["_final_response_source"] == "reaction.natural_final_reply"

    def test_spec294_final_reply_tool_call_does_not_retry(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class ToolCallingFinalReplyExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(_logical_step(step, active_protocol_tool_guides))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec560 should not call final_reply")
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        rt.executor = ToolCallingFinalReplyExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert rt.executor.calls == ["reaction"]
        assert result["_exit_signal"] == "done"
        assert result["response"] == "本轮已完成。"
        assert not any(
            str(receipt.get("status", "")).startswith("final_reply_empty_retry")
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec575_mixed_invalid_finalize_with_file_read_keeps_tool_result(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class ConflictExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if len(self.calls) == 1:
                    return {
                        "response": "closed after file_read result",
                        "tool_call_envelopes": [
                            helper._native_tool_envelope(
                                "file_read",
                                {"path": "example.txt", "reason": "read first"},
                                call_id="call_conflict_read",
                                index=0,
                            ),
                            helper._native_reaction_finalize(
                                call_id="call_conflict_premature",
                                closeout_decision="finish",
                            ),
                        ],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "finished after file_read result",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("Spec561 should close with a later natural reply")

        def fake_execute(request):
            return {
                "tool_id": "file_read",
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "content": "read ok",
            }

        executor = ConflictExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(executor.calls) == 2
        assert result["response"] == "finished after file_read result"
        assert result["_general_tool_results"][0]["call_id"] == "call_conflict_read"
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert result["_closeout_relay_receipts"] == []
        assert rt.sm.get_flags().get("continue_requested") is not True
        assert any(
            item.get("tool_id") == "reaction_finalize"
            and item.get("reason") == "reaction_finalize_retired_field"
            and item.get("field") == "closeout_decision"
            for item in result["_invalid_tool_requests"]
        )

    def test_reaction_tool_request_without_guide_does_not_enable_submission(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class MissingGuideExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "unknown tools rejected", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [
                            helper._native_tool_envelope(
                                "state_update",
                                {},
                                call_id="call_state_update",
                                tool_family="",
                                tool_class="",
                                risk="",
                                parse_status="unknown_tool_id",
                                index=0,
                            ),
                            helper._native_tool_envelope(
                                "relation_content_read",
                                {},
                                call_id="call_relation_content_read",
                                tool_family="",
                                tool_class="",
                                risk="",
                                parse_status="unknown_tool_id",
                                index=1,
                            ),
                        ],
                    }
                return {
                    "response": "unknown tools rejected",
                    "tool_call_envelopes": [],
                }

        rt.executor = MissingGuideExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_protocol_tool_submissions"] == []
        assert result["_protocol_tool_receipts"] == []
        assert [
            (item.get("tool_id"), item.get("reason"))
            for item in result["_invalid_tool_requests"]
        ] == [
            ("state_update", "unknown_tool_id"),
            ("relation_content_read", "unknown_tool_id"),
        ]
