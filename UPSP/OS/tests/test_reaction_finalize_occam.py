import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, ScriptedExecutor


def _minimal_reaction_context(runtime, monkeypatch):
    assembler = runtime.assembler
    monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
    monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")


def test_spec560_reaction_finalize_schema_is_continue_handoff_only():
    from logic.native_tool_calls import export_provider_tool_schemas

    tools = export_provider_tool_schemas(
        provider="openai_responses",
        include_standard_tools=False,
        include_step_terminal_tools=["reaction_finalize"],
    )
    by_name = {item["name"]: item["parameters"] for item in tools}

    params = by_name["reaction_finalize"]
    assert params["additionalProperties"] is False
    assert params["required"] == ["handoff_text"]
    assert set(params["properties"]) == {"handoff_text"}
    assert "closeout_decision" not in params["properties"]


def test_spec560_reaction_finalize_rejects_empty_or_retired_decision():
    from logic.native_tool_calls import terminal_finalize_from_envelopes

    helper = RuntimeTestMixin()
    missing = helper._native_tool_envelope(
        "reaction_finalize",
        {},
        call_id="call_missing_handoff",
        tool_family="substrate_tool",
        tool_class="sync_tool",
        risk="high",
    )
    empty = helper._native_tool_envelope(
        "reaction_finalize",
        {"handoff_text": ""},
        call_id="call_empty_handoff",
        tool_family="substrate_tool",
        tool_class="sync_tool",
        risk="high",
    )
    old_decision = helper._native_tool_envelope(
        "reaction_finalize",
        {"closeout_decision": "finish"},
        call_id="call_retired_decision",
        tool_family="substrate_tool",
        tool_class="sync_tool",
        risk="high",
    )

    missing_parsed, _, missing_invalids = terminal_finalize_from_envelopes(
        [missing],
        "reaction",
    )
    empty_parsed, _, empty_invalids = terminal_finalize_from_envelopes(
        [empty],
        "reaction",
    )
    old_parsed, _, old_invalids = terminal_finalize_from_envelopes(
        [old_decision],
        "reaction",
    )

    assert missing_parsed is None
    assert missing_invalids[0]["reason"] == "native_argument_missing_required"
    assert missing_invalids[0]["field"] == "handoff_text"
    assert not empty_invalids
    assert "reaction_finalize.handoff_text_required" in empty_parsed[
        "reaction_finalize_errors"
    ]
    assert old_parsed is None
    assert old_invalids[0]["reason"] == "reaction_finalize_retired_field"
    assert old_invalids[0]["field"] == "closeout_decision"


class TestSpec560ReactionFinalizeOccam(RuntimeTestMixin):
    def test_reaction_session_advances_and_settles_one_frame(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)

        rt.executor = ScriptedExecutor(
            {
                "response": "单帧完成。",
                "tool_call_envelopes": [],
            },
        )
        session = rt.reaction_loop_runner.start_session(
            rt.sm.load(),
            "interactive",
            [],
            trigger_id="T00000001",
            caused_by="R000000:setup:1",
        )

        settlement = rt.reaction_loop_runner.run_frame(session)

        assert settlement.frame_ref.frame_id == "R000000:reaction:1"
        assert settlement.frame_ref.trigger_id == "T00000001"
        assert settlement.frame_ref.caused_by == "R000000:setup:1"
        assert settlement.status == "settled"
        assert session.completed is False
        assert session.result_state.final_response == "单帧完成。"
        assert rt.reaction_loop_runner.run_frame(session) is None
        assert session.completed is True
        assert session.result["response"] == "单帧完成。"
        assert session.result["_frame_settlements"] == [settlement.as_dict()]
        events = rt.audit.get_store()._read_events_quiet(0)
        reaction_events = [
            event for event in events if event.get("phase") == "reaction"
        ]
        assert reaction_events
        assert {
            event.get("frame_id") for event in reaction_events
        } == {settlement.frame_ref.frame_id}
        assert any(
            event.get("event_type") == "step_settlement"
            and (event.get("payload") or {}).get("settlement_scope") == "frame"
            for event in reaction_events
        )

    def test_reaction_frame_consumes_sourced_organ_signal_as_user_material(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)

        class OrganRuntime:
            def __init__(self):
                self.frames = []

            def begin_frame_materials(self, frame_ref):
                self.frames.append(frame_ref)
                return ({
                    "role": "user",
                    "kind": "organ_signal",
                    "content": "[organ_signal source_role=memory]\nnext-frame",
                    "source_role": "memory",
                    "caused_by": "R000000:setup:1",
                },)

            @staticmethod
            def dispatch(*_args, **_kwargs):
                return {"records": [], "receipts": []}

        class Executor:
            seen = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.seen = list(messages)
                return {"response": "done", "tool_call_envelopes": []}

        organ_runtime = OrganRuntime()
        executor = Executor()
        rt.reaction_loop_runner.organ_runtime = organ_runtime
        rt.executor = executor
        session = rt.reaction_loop_runner.start_session(
            rt.sm.load(), "interactive", [],
            trigger_id="T00000001", caused_by="R000000:setup:1")

        settlement = rt.reaction_loop_runner.run_frame(session)

        visible = [item for item in executor.seen
                   if "next-frame" in str(item.get("content") or "")]
        assert settlement.frame_ref == organ_runtime.frames[0]
        assert len(visible) == 1
        assert visible[0]["role"] == "user"

    def test_reaction_provider_exception_still_settles_its_frame(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        monkeypatch.setattr(
            rt.reaction_loop_runner,
            "_recover_provider_interruption_if_possible",
            lambda *args, **kwargs: None,
        )

        class Executor:
            @staticmethod
            def call(*_args, **_kwargs):
                raise RuntimeError("provider down")

        rt.executor = Executor()
        session = rt.reaction_loop_runner.start_session(
            rt.sm.load(), "interactive", [],
            trigger_id="T00000001", caused_by="R000000:setup:1")

        settlement = rt.reaction_loop_runner.run_frame(session)

        assert settlement.status == "degraded"
        assert settlement.exit_signal == "frame_exception"
        assert settlement.provider_call_started is True
        assert session.completed is False
        with pytest.raises(RuntimeError, match="provider down"):
            rt.reaction_loop_runner.run_frame(session)
        assert session.completed is True

    def test_plain_natural_text_finishes_without_final_reply_provider(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)

        class Executor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(step)
                if step == "final_reply":
                    raise AssertionError("Spec560 must not call final_reply")
                if len(self.calls) > 1:
                    raise AssertionError("plain final text should close the loop")
                return {"response": "已经处理完毕，没有剩余问题。", "tool_call_envelopes": []}

        executor = Executor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_exit_signal"] == "done"
        assert result["_reaction_finalize_validated"] is True
        assert result["_final_reply_done"] is True
        assert result["response"] == "已经处理完毕，没有剩余问题。"
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert result["_final_response_source"] == "reaction.natural_final_reply"
        assert executor.calls == ["reaction"]

    def test_active_task_blocks_plain_final_text_candidate(
            self, tmp_path, monkeypatch):
        from logic.task_guide import materialize_initial_task_guide

        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        materialize_initial_task_guide(
            rt.workbench,
            {
                "task_title": "Spec560 task blocker",
                "task_goal": "Keep required item open.",
                "source_requirements": [
                    {"requirement_id": "req_01", "summary": "Do one thing."}
                ],
                "items": [
                    {"item_id": "item_01", "description": "Still open."}
                ],
                "acceptance": [
                    {"acceptance_id": "acc_01", "description": "Still pending."}
                ],
            },
        )

        class Executor:
            def __init__(self):
                self.calls = 0
                self.seen_feedback = ""

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls += 1
                combined = "\n".join(str(m.get("content", "")) for m in messages)
                if self.calls == 1:
                    return {"response": "已经全部完成。", "tool_call_envelopes": []}
                self.seen_feedback = combined
                return {
                    "response": "",
                    "tool_call_envelopes": [RuntimeTestMixin()._native_reaction_finalize(
                        call_id="call_spec560_continue",
                        handoff_text="下一轮继续补齐任务账本和验收证据。",
                    )],
                }

        executor = Executor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == 2
        assert "任务账本未闭合" in executor.seen_feedback
        assert result["_exit_signal"] == "continue_requested"
        assert result["_final_reply_done"] is False
        assert result["response"] == ""
        frames = result["_frame_settlements"]
        assert len(frames) == 2
        assert frames[1]["frame_ref"]["caused_by"] == frames[0]["frame_id"]

    def test_reaction_finalize_handoff_only_sets_continue_without_final_response(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        helper = self

        class Executor:
            def call(self, step, system, messages, active_protocol_tool_guides=None):
                if step == "final_reply":
                    raise AssertionError("continue handoff must not call final_reply")
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_spec560_handoff",
                        handoff_text="下一轮继续从审计报告第三项开始。",
                    )],
                }

        rt.executor = Executor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_exit_signal"] == "continue_requested"
        assert result["_reaction_finalize_validated"] is True
        assert result["_final_reply_done"] is False
        assert result["response"] == ""
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "continue"
        assert result["_closeout_relay_receipts"]

    def test_spec575_mixed_tool_and_reaction_finalize_post_settles_continue(
            self, tmp_path, monkeypatch):
        import json
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from logic.sandbox_grant import SANDBOX_GRANT_ENV

        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        monkeypatch.setenv(
            SANDBOX_GRANT_ENV,
            json.dumps({
                "phase": "spec560",
                "task_root": str(tmp_path),
                "read_paths": [str(tmp_path)],
                "write_paths": [],
                "allowed_tools": ["file_read"],
            }),
        )
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "path": request.get("path"),
                "content": "ok",
                "source": "general_tool_call",
            }

        class Executor:
            def __init__(self):
                self.calls = 0
                self.feedback = ""

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls += 1
                combined = "\n".join(str(m.get("content", "")) for m in messages)
                if self.calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [
                            helper._native_tool_envelope(
                                "file_read",
                                {"path": str(tmp_path / "missing.txt")},
                                call_id="call_spec560_file_read",
                                tool_family="general_tool",
                                tool_class="read_tool",
                                risk="low",
                            ),
                            helper._native_reaction_finalize(
                                call_id="call_spec560_mixed_handoff",
                                handoff_text="下一轮继续。",
                            ),
                        ],
                    }
                self.feedback = combined
                return {"response": "工具已处理完毕。", "tool_call_envelopes": []}

        executor = Executor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == 1
        assert executor.feedback == ""
        assert result["_general_tool_results"][0]["call_id"] == "call_spec560_file_read"
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert result["_exit_signal"] == "continue_requested"
        assert result["_reaction_finalize_validated"] is True
        assert result["response"] == ""
        assert rt.sm.get_flags().get("continue_requested") is True
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "continue"
        assert result["_closeout_relay_receipts"][-1]["handoff_text"] == "下一轮继续。"

    def test_spec575_mixed_tool_failure_does_not_set_continue(
            self, tmp_path, monkeypatch):
        import json
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from logic.sandbox_grant import SANDBOX_GRANT_ENV

        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        monkeypatch.setenv(
            SANDBOX_GRANT_ENV,
            json.dumps({
                "phase": "spec575",
                "task_root": str(tmp_path),
                "read_paths": [str(tmp_path)],
                "write_paths": [],
                "allowed_tools": ["file_read"],
            }),
        )
        helper = self

        def fake_execute(request):
            return {
                "tool_id": request["tool_id"],
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "rejected",
                "reason": "file_not_found",
                "path": request.get("path"),
                "source": "general_tool_call",
            }

        class Executor:
            def __init__(self):
                self.calls = 0
                self.feedback = ""

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls += 1
                combined = "\n".join(str(m.get("content", "")) for m in messages)
                if self.calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [
                            helper._native_tool_envelope(
                                "file_read",
                                {"path": str(tmp_path / "missing.txt")},
                                call_id="call_spec575_file_read_rejected",
                                tool_family="general_tool",
                                tool_class="read_tool",
                                risk="low",
                            ),
                            helper._native_reaction_finalize(
                                call_id="call_spec575_failed_tool_handoff",
                                handoff_text="下一轮继续。",
                            ),
                        ],
                    }
                self.feedback = combined
                return {"response": "读取失败，已说明。", "tool_call_envelopes": []}

        executor = Executor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert executor.calls == 2
        assert "file_not_found" in executor.feedback
        assert result["_general_tool_results"][0]["status"] == "rejected"
        assert result["_closeout_relay_receipts"] == []
        assert rt.sm.get_flags().get("continue_requested") is not True
        assert result["_exit_signal"] == "done"

    def test_required_now_read_failure_stops_before_provider(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        monkeypatch.setattr(
            rt.ctx_store,
            "get_now_entries",
            lambda: (_ for _ in ()).throw(OSError("now unavailable")),
        )
        rt.executor = ScriptedExecutor({"response": "must not run"})

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert rt.executor.calls == []
        assert result["aborted"] is True
        assert result["_required_context_failure"]["scope"] == "now_cache"
        assert result["_exit_signal"] == "required_context_failure"

    def test_general_result_survives_source_projection_failure(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        helper = self
        result_payload = {
            "tool_id": "file_read",
            "tool_family": "general_tool",
            "tool_class": "read_tool",
            "status": "ok",
            "path": "book.md",
            "content": "read body",
            "protocol_tool_receipt": False,
        }
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda _tool_id: "file_read guide",
            execute_fn=lambda _request: dict(result_payload),
        )
        monkeypatch.setattr(
            rt.workbench,
            "append_source_read_evidence",
            lambda _payload: (_ for _ in ()).throw(
                OSError("source evidence unavailable")),
        )
        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "file_read",
                    {"path": "book.md"},
                    call_id="call_required_source_projection",
                )],
            },
            {"response": "must not run"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(rt.executor.calls) == 1
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert result["_required_context_failure"]["scope"] == (
            "source_read_evidence")
        assert result["_frame_settlements"][-1]["status"] == "degraded"
        assert result["_frame_settlements"][-1]["exit_signal"] == (
            "required_context_failure")

    def test_protocol_receipt_survives_fact_projection_failure(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        _minimal_reaction_context(rt, monkeypatch)
        helper = self
        monkeypatch.setattr(
            rt.reaction_loop_runner,
            "_append_to_context_cache",
            lambda *_args, **_kwargs: False,
        )
        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "relation_read",
                    {
                        "subject": "missing",
                        "summary": "temporary",
                        "body": "none",
                    },
                    call_id="call_required_protocol_projection",
                    tool_family="protocol_tool",
                    tool_class="read_tool",
                    risk="low",
                )],
            },
            {"response": "must not run"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(rt.executor.calls) == 1
        assert result["_protocol_tool_receipts"][0]["tool_id"] == "relation_read"
        assert result["_required_context_failure"]["scope"] == (
            "protocol_tool_fact")
        assert result["_exit_signal"] == "required_context_failure"
