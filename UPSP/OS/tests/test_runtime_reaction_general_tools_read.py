import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, logical_step


class TestRuntimeReactionGeneralToolsRead(RuntimeTestMixin):
    def test_spec580_unfinished_file_read_reminds_before_first_natural_final_reply(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        bad_final_text = "我已经读完整本书，可以总结全文。"
        corrected_final_text = "我只基于已读的第 1-120 行回答；文件后续内容还没有读完。"

        class UnfinishedReadThenNaturalReplyExecutor:
            def __init__(self):
                self.calls = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = logical_step(step, active_protocol_tool_guides)
                self.calls.append({
                    "step": logical,
                    "messages": list(messages),
                    "text": "\n".join(
                        str((message or {}).get("content") or "")
                        for message in messages
                    ),
                })
                if logical == "final_reply":
                    raise AssertionError("Spec580 must not call retired final_reply provider")
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "line_start": 1,
                                "reason": "read the requested book",
                            },
                            call_id="call_spec580_read_1",
                        )],
                    }
                if self.reaction_calls == 2:
                    assert "最终回复事实提醒" not in self.calls[-1]["text"]
                    return {
                        "response": bad_final_text,
                        "tool_call_envelopes": [],
                    }
                if self.reaction_calls == 3:
                    combined = self.calls[-1]["text"]
                    assert "最终回复事实提醒" in combined
                    assert "book.md" in combined
                    assert "第 1-120 行" in combined
                    assert "共 1000 行" in combined
                    assert "has_more=true" in combined
                    assert "第 121 行" in combined
                    assert "如果用户原始任务要求完整阅读、完整处理或完整核对" in combined
                    assert bad_final_text not in combined
                    return {
                        "response": corrected_final_text,
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("Spec580 should accept the second natural reply")

        def fake_execute(request):
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
                "start_line": 1,
                "end_line": 120,
                "total_lines": 1000,
                "next_line_start": 121,
                "has_more": True,
                "read_mode": "bounded",
                "content": "read window",
                "protocol_tool_receipt": False,
            }

        executor = UnfinishedReadThenNaturalReplyExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        assert [call["step"] for call in executor.calls] == [
            "reaction",
            "reaction",
            "reaction",
        ]
        assert result["response"] == corrected_final_text
        assert result["_final_reply_done"] is True
        assert result["_final_response_source"] == "reaction.natural_final_reply"
        assert any(
            receipt.get("status") == "unfinished_file_read_final_reply_reminder"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert result["_settlement_ledgers"][-1]["read_status"] == "partial_user_wait"
        assert not any(
            envelope.get("channel") == "final_reply.text"
            and envelope.get("text") == bad_final_text
            for envelope in result["_message_envelopes"]
        )
        cache_entries = (
            rt.ctx_store.get_now_entries()
            + rt.ctx_store.get_lately_entries("reaction")
        )
        assert not any(
            bad_final_text in str(entry.get("content") or "")
            for entry in cache_entries
        )

    def test_spec580_completed_followup_read_clears_unfinished_reminder_state(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        final_text = "这次已经读到文件末尾，可以基于完整读取结果回复。"

        class CompletedFollowupReadExecutor:
            def __init__(self):
                self.calls = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                logical = logical_step(step, active_protocol_tool_guides)
                self.calls.append({
                    "step": logical,
                    "text": "\n".join(
                        str((message or {}).get("content") or "")
                        for message in messages
                    ),
                })
                if logical == "final_reply":
                    raise AssertionError("Spec580 must not call retired final_reply provider")
                self.reaction_calls += 1
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {"path": "book.md", "line_start": 1},
                            call_id="call_spec580_read_1",
                        )],
                    }
                if self.reaction_calls == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {"path": "book.md", "line_start": 121},
                            call_id="call_spec580_read_121",
                        )],
                    }
                if self.reaction_calls == 3:
                    assert "最终回复事实提醒" not in self.calls[-1]["text"]
                    return {"response": final_text, "tool_call_envelopes": []}
                raise AssertionError("completed read should allow natural final reply")

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
                "end_line": 120 if start_line == 1 else 200,
                "total_lines": 200,
                "next_line_start": 121 if start_line == 1 else None,
                "has_more": start_line == 1,
                "read_mode": "bounded",
                "content": f"read from {start_line}",
                "protocol_tool_receipt": False,
            }

        executor = CompletedFollowupReadExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        assert [call["step"] for call in executor.calls] == [
            "reaction",
            "reaction",
            "reaction",
        ]
        assert result["response"] == final_text
        assert result["_final_reply_done"] is True
        assert not any(
            receipt.get("status") == "unfinished_file_read_final_reply_reminder"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert result["_reaction_obligations"]["unfinished_file_reads"] == []

    def test_spec419_file_read_fact_shows_total_progress_and_completion(self):
        from logic.general_tools import format_general_tool_fact

        fact = format_general_tool_fact({
            "tool_id": "file_read",
            "status": "ok",
            "path": "book.md",
            "read_mode": "bounded",
            "start_line": 3148,
            "end_line": 5004,
            "total_lines": 10030,
            "total_chars": 235645,
            "has_more": True,
            "next_line_start": 5005,
        })

        assert "文件总量：10030 行，235645 字符。" in fact
        assert "读取进度：第 3148-5004 行 / 共 10030 行。" in fact
        assert "file_read(path=book.md, line_start=5005)" in fact
        assert "当前文件读取已到末尾" not in fact

        done_fact = format_general_tool_fact({
            "tool_id": "file_read",
            "status": "ok",
            "path": "short.md",
            "read_mode": "bounded",
            "start_line": 1,
            "end_line": 3,
            "total_lines": 3,
            "total_chars": 18,
            "has_more": False,
        })

        assert "文件总量：3 行，18 字符。" in done_fact
        assert "读取进度：第 1-3 行 / 共 3 行。" in done_fact
        assert "当前文件读取已到末尾。" in done_fact

    def test_spec419_web_fetch_result_and_fact_show_total_progress(self):
        from logic.general_tools import (
            execute_general_tool_call,
            format_general_tool_fact,
        )

        body = "\n".join(f"line {index:04d}" for index in range(700))

        def fake_fetch(url, timeout_ms):
            return {
                "url": url,
                "source_url": url,
                "title": "Long Page",
                "status_code": 200,
                "content_type": "text/plain",
                "content": body,
                "source_bytes_incomplete": False,
            }

        result = execute_general_tool_call(
            {
                "tool_id": "web_fetch",
                "url": "https://example.test/page",
                "reason": "read page",
            },
            web_fetch_fn=fake_fetch,
        )

        assert result["status"] == "ok"
        assert result["total_lines"] == 700
        assert result["total_chars"] == len(body)
        assert result["has_more"] is True
        assert result["next_char_start"] not in (None, "")

        fact = format_general_tool_fact(result)

        assert f"已取得正文总量：700 行，{len(body)} 字符。" in fact
        assert "正文进度：第 " in fact
        assert "字符进度：" in fact
        assert "web_fetch" in fact
        assert f"char_start={result['next_char_start']}" in fact

    def test_spec376_relay_target_progress_starts_new_finalize_missing_lifecycle(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()
        state["base"]["runtime"]["pending_relay_target"] = {
            "kind": "tool",
            "tool_id": "file_read",
            "path": "book.md",
            "next_start_line": 2078,
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

        class RelayTargetLifecycleExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "中继读取已执行，等待用户下一步。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                combined = "\n".join(
                    str(message.get("content", ""))
                    for message in messages
                )
                if self.reaction_calls == 1:
                    return {
                        "response": "我先说明一下，但还没收束。",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_retired_finish_before_target",
                            closeout_decision="finish",
                        )],
                    }
                if self.reaction_calls == 2:
                    assert "__closeout_only__" not in self.guides[-1]
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_continue_before_target",
                            handoff_text="继续读 book.md 第 2078 行。",
                        )],
                    }
                if self.reaction_calls == 3:
                    assert "relay_target_unfulfilled" in combined
                    assert "line_start=2078" in combined
                    assert "cursor=line:2078" not in combined
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "line_start": 2078,
                                "reason": "fulfill relay target",
                            },
                            call_id="call_read_2078",
                        )],
                    }
                if self.reaction_calls == 4:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "line_start": 2300,
                                "reason": "continue local read",
                            },
                            call_id="call_read_2300",
                        )],
                    }
                if self.reaction_calls == 5:
                    return {
                        "response": "已完成本段读取。",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("natural final reply should close the relay round")

        def fake_execute(request):
            start_line = int(request.get("line_start") or 1)
            result = {
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
                "end_line": start_line + 120,
                "has_more": start_line == 2078,
                "read_mode": "bounded",
                "content": f"read from {start_line}",
            }
            if start_line == 2078:
                result["next_start_line"] = 2300
            return result

        executor = RelayTargetLifecycleExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])

        assert [step for step, _ in executor.calls] == [
            "reaction",
            "reaction",
            "reaction",
            "reaction",
            "reaction",
        ]
        assert result["response"] == "已完成本段读取。"
        assert any(
            receipt.get("status") == "relay_target_unfulfilled"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert result["_assistant_progress"] == ["我先说明一下，但还没收束。"]
        assert result["_exit_signal"] == "done"

    def test_spec255_relay_file_read_counts_as_progress_before_relay(
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

        class ReadThenRelayExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((logical_step(step, active_protocol_tool_guides), list(messages)))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "read progress accepted", "tool_call_envelopes": []}
                reaction_calls = [call for call in self.calls if call[0] == "reaction"]
                if len(reaction_calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "example.txt",
                                "reason": "read one more page",
                            },
                            call_id="call_relay_file_read",
                        )],
                    }
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_relay_after_read",
                        handoff_text=(
                            "下一轮起手：setup next read\n"
                            "下一轮反应：reaction next read\n"
                            "中继原因：file_read advanced the relay"
                        ),
                    )],
                }

        def fake_execute(request):
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
                "start_line": 1,
                "end_line": 120,
                "next_start_line": 121,
                "has_more": True,
                "read_mode": "bounded",
                "content": "read ok",
            }

        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)
        rt.executor = ReadThenRelayExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "relay", [])

        assert result["response"] == ""
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert result["_closeout_relay_receipts"][0]["status"] == "continue_requested_set"
        assert result["_pending_relay_target"] == {
            "kind": "tool",
            "tool_id": "file_read",
            "path": "example.txt",
            "next_start_line": 121,
            "source": "file_read_result",
            "source_call_id": "call_relay_file_read",
        }
        assert not any(
            receipt.get("status") == "relay_execution_missing"
            for receipt in result["_reaction_loop_guard_receipts"]
        )

    def test_spec069_general_tool_file_read_uses_independent_result_channel(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class GeneralToolExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "file read observed", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "example.txt",
                                "reason": "read example",
                            },
                            call_id="call_file_read",
                        )],
                    }
                return {
                    "response": "file read observed",
                    "tool_call_envelopes": [],
                }

        class RecordingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                }

            def load_cache_compaction_debt(self):
                return {}

        def fake_execute(request):
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
                "content": "read ok",
                "has_more": False,
                "read_mode": "bounded",
                "protocol_tool_receipt": False,
            }

        rt.executor = GeneralToolExecutor()
        rt.ctx_store = RecordingContext()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "已读取文件：example.txt。" in second_call_text
        assert "[file_read ok]" not in second_call_text
        assert "general_tool_result" not in second_call_text
        assert "read ok" in second_call_text
        tool_messages = [
            m for m in rt.executor.calls[1]
            if "已读取文件：example.txt。" in str(m.get("content", ""))
        ]
        assert any(
            m.get("kind") == "tool_fact"
            and m.get("round") == rt.sm.get_total_round()
            for m in tool_messages
        )
        assert result["_general_tool_requests"][0]["tool_id"] == "file_read"
        assert result["_general_tool_requests"][0]["call_id"] == "call_file_read"
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert result["_general_tool_results"][0]["call_id"] == "call_file_read"
        assert result["_protocol_tool_receipts"] == []
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert all(entry[3] != "protocol_tool_receipt" for entry in rt.ctx_store.entries)
        assert any(entry[3] == "tool_fact" for entry in rt.ctx_store.entries)

    def test_spec334_bounded_file_read_blocks_complete_read_settlement(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class TruncatedReadExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "invalid tool rejected", "tool_call_envelopes": []}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "reason": "read book",
                            },
                            call_id="call_file_read",
                        )],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_bad_finalize",
                        )],
                    }
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_reaction_finalize(
                        call_id="call_good_finalize",
                        handoff_text=(
                            "下一轮起手：继续 book.md\n"
                            "下一轮反应：从 121 行继续读取。\n"
                            "中继原因：file_read has_more with next_start_line=121"
                        ),
                    )],
                }

        def fake_execute(request):
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
                "content": "read ok",
                "has_more": True,
                "read_mode": "bounded",
                "start_line": 1,
                "end_line": 120,
                "next_start_line": 121,
                "protocol_tool_receipt": False,
            }

        rt.executor = TruncatedReadExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_settlement_ledgers"][0]["read_status"] == "partial_continue"
        assert result["_settlement_ledgers"][0]["model_read_status"] == ""
        assert result["_closeout_relay_receipts"][0]["status"] == "continue_requested_set"

    def test_spec397_file_read_has_more_injects_action_state_without_progress_replay(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        self._patch_memory_immediate_stores(monkeypatch, runtime=rt)
        helper = self
        progress_text = "我已阅读这个窗口，接下来继续推进。"

        class CurrentActionStateExecutor:
            def __init__(self):
                self.calls = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append({"step": logical_step(step, active_protocol_tool_guides), "messages": list(messages)})
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    combined = "\n".join(
                        str(message.get("content", "")) for message in messages
                    )
                    assert progress_text in combined
                    assert "【轮中进展记录】" in combined
                    assert "[progress]" not in combined
                    assert "本轮回复记录" not in combined
                    assert "当前行动状态" not in combined
                    assert "已读取文件：book.md。" in combined
                    assert "line_start=164" in combined
                    assert "cursor=line:164" not in combined
                    assert "不代表全文已读" in combined
                    return {"response": "已根据已读窗口沉淀记忆，并保留续读状态。", "tool_call_envelopes": []}
                self.reaction_calls += 1
                combined = "\n".join(
                    str(message.get("content", "")) for message in messages
                )
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "book.md",
                                "line_start": 1,
                                "reason": "read first window",
                            },
                            call_id="call_spec397_read",
                        )],
                    }
                if self.reaction_calls == 2:
                    assert "当前行动状态" not in combined
                    assert "已读取文件：book.md。" in combined
                    assert "读取范围：第 1 行到第 163 行" in combined
                    assert "line_start=164" in combined
                    assert "cursor=line:164" not in combined
                    return {"response": progress_text, "tool_call_envelopes": []}
                if self.reaction_calls == 3:
                    assert "最终回复事实提醒" in combined
                    assert "has_more=true" in combined
                    assert "line_start=164" in combined
                    assert progress_text not in combined
                    assert "【轮中进展记录】" not in combined
                    assert "[progress]" not in combined
                    assert "本轮回复记录" not in combined
                    assert "本轮已播报进展" not in combined
                    assert "当前行动状态" not in combined
                    assert "已读取文件：book.md。" in combined
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "Spec397",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "基于已读窗口记录一条测试记忆。",
                                "candidate_keywords": ["Spec397", "progress"],
                            },
                            call_id="call_spec397_memory_write",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                assert "当前行动状态" not in combined
                assert "已读取文件：book.md。" in combined
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        def fake_execute(request):
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
                "start_line": 1,
                "end_line": 163,
                "next_start_line": 164,
                "has_more": True,
                "read_mode": "bounded",
                "content": "read window",
                "protocol_tool_receipt": False,
            }

        executor = CurrentActionStateExecutor()
        rt.executor = executor
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        assert result["response"] == "本轮已完成。"
        assert [item.get("tool_id") for item in result["_general_tool_results"]] == [
            "file_read",
        ]
        assert result["_memory_write_receipts"][0]["status"] == "applied"
        assert result["_tool_transaction_audit"]["status"] == "ok"

    def test_spec086_invalid_tool_requests_enter_runtime_audit_and_settlement(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        settlements = []

        def capture_settlement(round_num, phase, iteration, settlement):
            settlements.append((round_num, phase, iteration, settlement))

        monkeypatch.setattr(rt, "_round_audit_settlement", capture_settlement)

        class InvalidToolExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "file read observed after summary",
                        "tool_call_envelopes": [],
                    }
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [
                            helper._native_tool_envelope(
                                "cache_compact",
                                {"reason": "should not be reaction callable"},
                                call_id="call_bad_substrate",
                                tool_family="substrate_tool",
                                tool_class="sync_tool",
                                risk="high",
                            ),
                            helper._native_tool_envelope(
                                "not_a_real_tool",
                                {},
                                call_id="call_unknown",
                                tool_family="",
                                tool_class="",
                                risk="",
                                parse_status="unknown_tool_id",
                                index=1,
                            ),
                        ],
                    }
                return {
                    "response": "invalid tool rejected",
                    "tool_call_envelopes": [],
                }

        rt.executor = InvalidToolExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        expected_invalid = [
            {
                "tool_id": "cache_compact",
                "tool_family": "substrate_tool",
                "reason": "unsupported_tool_family",
            },
            {
                "tool_id": "not_a_real_tool",
                "tool_family": None,
                "reason": "unknown_tool_id",
            },
        ]
        assert [
            {
                "tool_id": item.get("tool_id"),
                "tool_family": item.get("tool_family"),
                "reason": item.get("reason"),
            }
            for item in result["_invalid_tool_requests"]
        ] == expected_invalid
        assert result["_protocol_tool_requests"] == []
        assert result["_protocol_tool_receipts"] == []
        assert result["_general_tool_requests"] == []
        assert result["_general_tool_results"] == []

        reaction_settlement = [
            item for item in settlements if item[1] == "reaction"
        ][-1][3]
        assert [
            {
                "tool_id": item.get("tool_id"),
                "tool_family": item.get("tool_family"),
                "reason": item.get("reason"),
            }
            for item in reaction_settlement["invalid_tool_requests"]
        ] == expected_invalid

        audit = result["_tool_transaction_audit"]
        assert audit["status"] == "issues_found"
        assert audit["counts"]["invalid_requests"] == 2
        assert [
            issue["tool_id"] for issue in audit["issues"]
            if issue["code"] == "invalid_request_rejected"
        ] == ["cache_compact", "not_a_real_tool"]

    def test_retired_text_tool_payload_feedback_next_iteration(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class TextPayloadExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "text payload feedback observed", "tool_call_envelopes": []}
                combined = "\n".join(
                    str(message.get("content", "")) for message in messages
                )
                if len(self.calls) == 2:
                    assert "assistant_text 中的疑似工具载荷没有执行" in combined
                    assert "provider-native 工具" in combined
                    return {
                        "response": "text payload feedback observed",
                        "tool_call_envelopes": [],
                    }
                return {"response": """| field | value | note |
|------|----|------|
| exit_signal | done | hallucinated complete |
| assistant_reply | tool call succeeded, file was read. | visible |
| protocol_tool_request | container_read: container_id=PRJ-1; target_file=notes.md | retired |
| general_tool_request | file_read: path=README.md; reason=inspect docs | retired |
"""}

        class RecordingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                }

            def load_cache_compaction_debt(self):
                return {}

        rt.executor = TextPayloadExecutor()
        rt.ctx_store = RecordingContext()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(rt.executor.calls) == 2
        second_context = "\n".join(
            str(message.get("content", "")) for message in rt.executor.calls[1]
        )
        assert "reaction_progress_recorded" not in second_context
        assert "reaction_progress_emit" not in second_context
        assert "assistant_text 中的疑似工具载荷没有执行" in second_context
        assert result["_protocol_tool_requests"] == []
        assert result["_general_tool_requests"] == []
        assert result["_general_tool_results"] == []
        assert [
            (item["tool_id"], item["reason"])
            for item in result["_invalid_tool_requests"]
        ] == [("assistant_text", "assistant_text_tool_payload")]
        assert result["_exit_signal"] == "done"
        assert not any(entry[3] == "protocol_tool_receipt" for entry in rt.ctx_store.entries)
        audit = result["_tool_transaction_audit"]
        assert audit["status"] == "issues_found"

    def test_spec076_general_tool_result_is_summarized_before_closeout(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class ClosureExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "file read observed after summary",
                        "tool_call_envelopes": [],
                    }
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "example.txt",
                                "reason": "read example",
                            },
                            call_id="call_summary_file_read",
                        )],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "file_read ok: example.txt returned read ok",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("natural final reply should close the reaction")

        class RecordingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                }

            def load_cache_compaction_debt(self):
                return {}

        def fake_execute(request):
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
                "content": "read ok",
                "has_more": False,
                "read_mode": "bounded",
                "protocol_tool_receipt": False,
            }

        rt.executor = ClosureExecutor()
        rt.ctx_store = RecordingContext()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "已读取文件：example.txt。" in second_call_text
        assert "[file_read ok]" not in second_call_text
        assert "general_tool_result" not in second_call_text
        assert "read ok" in second_call_text
        assert result["response"] == "file_read ok: example.txt returned read ok"
        assert [r["status"] for r in result["_general_tool_results"]] == ["ok"]
        assert all(not r.get("protocol_tool_receipt") for r in result["_general_tool_results"])
        assert result["_protocol_tool_receipts"] == []
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert result["_assistant_progress"] == []
        assert any(entry[3] == "tool_fact" for entry in rt.ctx_store.entries)
        assert all(entry[3] != "protocol_tool_receipt" for entry in rt.ctx_store.entries)

    def test_spec070_web_general_tools_use_independent_result_channel(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class WebToolExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "web_fetch",
                            {
                                "url": "https://example.com/page",
                                "reason": "read page",
                            },
                            call_id="call_web_fetch",
                        )],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "web_search",
                            {
                                "query": "UPSP general tool",
                                "reason": "find candidates",
                            },
                            call_id="call_web_search",
                        )],
                    }
                return {"response": "web tools observed", "tool_call_envelopes": []}

        class RecordingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                }

            def load_cache_compaction_debt(self):
                return {}

        def fake_execute(request):
            if request["tool_id"] == "web_fetch":
                return {
                    "tool_id": "web_fetch",
                    "tool_family": "general_tool",
                    "tool_class": "read_tool",
                    "status": "ok",
                    "source": "general_tool_call",
                    "backend_type": "python",
                    "handler": "web_fetch_handler",
                    "permission_scope": "public_web_read",
                    "result_kind": "general_tool_result",
                    "source_url": request.get("url"),
                    "title": "Example",
                    "content": "fetch ok",
                    "truncated": False,
                    "protocol_tool_receipt": False,
                }
            return {
                "tool_id": "web_search",
                "tool_family": "general_tool",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "web_search_handler",
                "permission_scope": "public_web_read",
                "result_kind": "general_tool_result",
                "query": request.get("query"),
                "results": [{
                    "title": "Candidate",
                    "url": "https://example.com/candidate",
                    "snippet": "search ok",
                }],
                "result_count": 1,
                "protocol_tool_receipt": False,
            }

        rt.executor = WebToolExecutor()
        rt.ctx_store = RecordingContext()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: (
                f"{tool_id} guide" if tool_id in {"web_fetch", "web_search"} else ""
            ),
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])
        third_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[2])
        after_search_text = "\n".join(
            m.get("content", "")
            for call in rt.executor.calls[2:]
            for m in call
        )

        assert "本轮已经成功读取网页：https://example.com/page。" in second_call_text
        assert "fetch ok" in second_call_text
        assert "本轮已经完成网页搜索。" in after_search_text
        assert "search ok" in after_search_text
        assert "general_tool_result" not in second_call_text
        assert "general_tool_result" not in third_call_text
        assert [r["status"] for r in result["_general_tool_results"]] == [
            "ok", "ok",
        ]
        assert {r["tool_id"] for r in result["_general_tool_results"]} == {
            "web_fetch", "web_search",
        }
        assert result["_protocol_tool_receipts"] == []
        assert all(entry[3] != "protocol_tool_receipt" for entry in rt.ctx_store.entries)
        assert sum(1 for entry in rt.ctx_store.entries if entry[3] == "tool_fact") == 2
