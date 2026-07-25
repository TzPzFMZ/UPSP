import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin
from test_runtime_reaction_protocol_write_tools import _logical_step


class TestRuntimeReactionProtocolPendingAndFocus(RuntimeTestMixin):
    def test_spec338_duplicate_container_focus_open_is_satisfied_guarded(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)
        store = cs.ContainerStore()
        container_id = store.create_focus_container(
            "DC", "Spec338 focus guard", target_file="open.md"
        )["container_id"]
        rt.container_store = store
        helper = self

        class DuplicateFocusExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                reaction_count = len([
                    call for call in self.calls if call[0] == "reaction"
                ])
                if reaction_count == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "container_focus",
                            {
                                "action": "open",
                                "container_id": container_id,
                                "reason": "first open",
                            },
                            call_id="call_focus_open_1",
                            tool_family="protocol_tool",
                            tool_class="focus_tool",
                            risk="medium",
                        )],
                    }
                if reaction_count == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "container_focus",
                            {
                                "action": "open",
                                "container_id": container_id,
                                "reason": "same focus again",
                            },
                            call_id="call_focus_open_2",
                            tool_family="protocol_tool",
                            tool_class="focus_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "duplicate focus guarded", "tool_call_envelopes": []}

        executor = DuplicateFocusExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        receipts = [
            receipt for receipt in result["_container_focus_receipts"]
            if receipt.get("tool_id") == "container_focus"
        ]
        assert [receipt.get("status") for receipt in receipts] == ["applied", "rejected"]
        assert receipts[1]["reason"] == "duplicate_container_focus_satisfied"
        assert receipts[1]["duplicate_of_call_id"] == "call_focus_open_1"
        assert rt.workbench.get("base.focus") == container_id
        third_call_text = "\n".join(
            message.get("content", "")
            for step, messages in executor.calls
            if step == "reaction"
            for message in messages
        )
        assert "工具循环警告" in third_call_text
        assert "容器焦点工具已有同一工具结果" in third_call_text

    def test_spec338_focus_open_supersedes_prior_focus_visibility_failure(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)
        store = cs.ContainerStore()
        container_id = store.create_focus_container(
            "DC", "Spec338 focus supersede", target_file="open.md"
        )["container_id"]
        rt.container_store = store
        helper = self

        class FocusVisibilityRecoveryExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                reaction_count = len([
                    call for call in self.calls if call[0] == "reaction"
                ])
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "focus failure was cancelled after focus open",
                        "tool_call_envelopes": [],
                    }
                if reaction_count == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_container_write",
                            {
                                "mem_id": "MEM-338B001",
                                "container_id": container_id,
                                "target_file": "open.md",
                                "title": "write before focus",
                                "container_body": "body before focus",
                                "current_overview": f"{container_id}：unit test",
                                "reason": "will fail before focus is visible",
                            },
                            call_id="call_container_write_before_focus",
                            tool_family="protocol_tool",
                            tool_class="focus_tool",
                            risk="medium",
                        )],
                    }
                if reaction_count == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "container_focus",
                            {
                                "action": "open",
                                "container_id": container_id,
                                "reason": "make focus visible",
                            },
                            call_id="call_focus_open_after_failure",
                            tool_family="protocol_tool",
                            tool_class="focus_tool",
                            risk="medium",
                        )],
                    }
                if reaction_count == 3:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "pending_cancel",
                            {
                                "pending_id": "PEND-R000000-N001",
                                "reason_code": "obsolete_intent",
                                "note": "焦点已打开，但原容器写入没有成功，取消那次写入意图。",
                            },
                            call_id="call_pending_cancel_after_focus",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "focus failure was superseded", "tool_call_envelopes": []}

        executor = FocusVisibilityRecoveryExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        write_receipts = result["_memory_container_write_receipts"]
        assert write_receipts[0]["reason"] == "focus_not_visible_at_iteration_start"
        assert rt.workbench.get("base.focus") == container_id
        third_reaction_messages = [
            messages for step, messages in executor.calls
            if step == "reaction"
        ][2]
        third_call_text = "\n".join(
            message.get("content", "") for message in third_reaction_messages
        )
        assert "本轮容器写入回执" in third_call_text
        assert "处理结果：rejected。" in third_call_text
        assert "失败详情：请查看 POPUP 中相同 tool_id/call_id 的工具提醒。" in third_call_text
        assert "focus_not_visible_at_iteration_start" not in third_call_text
        assert f"容器编号：{container_id}" in third_call_text
        assert "PEND-R000000-N001" not in third_call_text
        assert "pending_cancel" not in third_call_text
        assert result["_pending_cancel_receipts"][0]["status"] == "not_found_or_settled"
        now_cache_text = "\n".join(
            entry.get("content", "") for entry in rt.ctx_store.get_now_entries()
        )
        assert "本轮容器写入回执" in now_cache_text
        assert "处理结果：rejected。" in now_cache_text
        assert "focus_not_visible_at_iteration_start" not in now_cache_text
        assert any(
            "focus_not_visible_at_iteration_start" in feedback
            and "call_container_write_before_focus" in feedback
            for feedback in result["_native_tool_feedbacks"]
        )

    def test_spec361_cancel_after_third_failure_allows_honest_finalize(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        memory_store, _memory_index, _container_store = self._patch_memory_immediate_stores(
            monkeypatch,
            runtime=rt,
        )

        def reject_long_memory(mem_id, title, summary, **kwargs):
            if len(summary) > 512:
                raise ValueError(
                    f"memory_body_too_long:max=512;actual={len(summary)}"
                )
            memory_store.entries.append((mem_id, title, summary, kwargs))

        memory_store.write_entry = reject_long_memory
        helper = self
        long_body = "超长记忆正文" * 120

        class CancelledWriteFalseHandoffExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "写入失败后已取消，没有补写新记忆。", "tool_call_envelopes": []}
                reaction_count = len([
                    call for call in self.calls if call[0] == "reaction"
                ])
                if reaction_count == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "超长写入",
                                "weight": 4,
                                "subject": "Codex",
                                "body": long_body,
                                "candidate_keywords": ["Spec343", "pending"],
                            },
                            call_id="call_spec343_memory_write_long",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if reaction_count == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "超长写入",
                                "weight": 4,
                                "subject": "Codex",
                                "body": long_body,
                                "candidate_keywords": ["Spec343", "pending"],
                            },
                            call_id="call_spec343_memory_write_retry_long",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if reaction_count == 3:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "超长写入",
                                "weight": 4,
                                "subject": "Codex",
                                "body": long_body,
                                "candidate_keywords": ["Spec361", "pending"],
                            },
                            call_id="call_spec361_memory_write_third_long",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if reaction_count == 4:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "pending_cancel",
                            {
                                "pending_id": "PEND-R000000-N001",
                                "reason_code": "low_value",
                                "note": "这次超长写入不再补写，放弃。",
                            },
                            call_id="call_spec361_pending_cancel",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "写入失败后已取消，没有写入新记忆。",
                    "tool_call_envelopes": [],
                }

        executor = CancelledWriteFalseHandoffExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        assert result["response"] == "写入失败后已取消，没有写入新记忆。"
        assert rt.sm.get_flags().get("continue_requested") is not True
        assert [
            step for step, _messages in executor.calls
            if step == "reaction"
        ] == ["reaction", "reaction", "reaction", "reaction", "reaction"]
        fourth_context = "\n".join(
            message.get("content", "")
            for step, messages in executor.calls
            if step == "reaction"
            for message in messages
        )
        assert "对应记忆不存在" in fourth_context
        assert "PEND-R000000-N001" in fourth_context

    def test_spec361_cancelled_write_claim_uses_warning_not_broad_final_parser(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        memory_store, _memory_index, _container_store = self._patch_memory_immediate_stores(
            monkeypatch,
            runtime=rt,
        )

        def reject_long_memory(mem_id, title, summary, **kwargs):
            if len(summary) > 512:
                raise ValueError(
                    f"memory_body_too_long:max=512;actual={len(summary)}"
                )
            memory_store.entries.append((mem_id, title, summary, kwargs))

        memory_store.write_entry = reject_long_memory
        helper = self
        long_body = "超长记忆正文" * 120

        class FinalReplyFalseClaimExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "两条记忆已写入并已挂接 DC-28。", "tool_call_envelopes": []}
                reaction_count = len([
                    call for call in self.calls if call[0] == "reaction"
                ])
                if reaction_count == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "超长写入",
                                "weight": 4,
                                "subject": "Codex",
                                "body": long_body,
                                "candidate_keywords": ["Spec343", "final"],
                            },
                            call_id="call_spec343_final_memory_write_long",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if reaction_count == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "超长写入",
                                "weight": 4,
                                "subject": "Codex",
                                "body": long_body,
                                "candidate_keywords": ["Spec343", "final"],
                            },
                            call_id="call_spec343_final_memory_write_retry_long",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if reaction_count == 3:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "超长写入",
                                "weight": 4,
                                "subject": "Codex",
                                "body": long_body,
                                "candidate_keywords": ["Spec361", "final"],
                            },
                            call_id="call_spec361_final_memory_write_third_long",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if reaction_count == 4:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "pending_cancel",
                            {
                                "pending_id": "PEND-R000000-N001",
                                "reason_code": "low_value",
                                "note": "这次超长写入不再补写，放弃。",
                            },
                            call_id="call_spec361_final_pending_cancel",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {
                    "response": "写入失败后已取消，没有写入新记忆。",
                    "tool_call_envelopes": [],
                }

        executor = FinalReplyFalseClaimExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        assert result["response"] == "写入失败后已取消，没有写入新记忆。"
        assert not any(step == "final_reply" for step, _messages in executor.calls)
