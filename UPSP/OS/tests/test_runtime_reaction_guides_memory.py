import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, logical_step


class TestRuntimeReactionGuidesMemory(RuntimeTestMixin):
    def test_spec407_reaction_progress_enters_dialogue_progress_cache(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class CapturingExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "second reaction final"}
                if len(self.calls) == 1:
                    return {
                        "response": "first reaction progress",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_retired_progress_finalize",
                            closeout_decision="finish",
                        )],
                    }
                return {
                    "response": "second reaction final",
                    "tool_call_envelopes": [],
                }

        rt.executor = CapturingExecutor()

        rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert any("first reaction progress" in m.get("content", "") for m in rt.executor.calls[1])
        assert any("【轮中进展记录】" in m.get("content", "") for m in rt.executor.calls[1])
        round_events = [
            json.loads(line)
            for line in (tmp_path / "context" / "round" / "round_0.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        ]
        reaction_inputs = [
            event for event in round_events
            if event["event_type"] == "step_input_snapshot"
            and event.get("phase") == "reaction"
        ]
        reaction_raw_outputs = [
            event for event in round_events
            if event["event_type"] == "llm_output_raw"
            and event.get("phase") == "reaction"
        ]
        assert [event["iteration"] for event in reaction_inputs[:2]] == [1, 2]
        assert all("messages" not in event["payload"] for event in reaction_inputs)
        assert reaction_raw_outputs[0]["payload"]["response"] == "first reaction progress"
        assert reaction_raw_outputs[0]["payload"]["tool_call_envelopes"][0]["tool_id"] == (
            "reaction_finalize")
        assert reaction_raw_outputs[1]["payload"]["tool_call_envelopes"] == []

    def test_reaction_loop_consumes_structured_result_table(self, tmp_path, monkeypatch):
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
                self.calls.append(logical_step(step, active_protocol_tool_guides))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "native structured reply"}
                return {
                    "response": "table-like text must not drive parsing",
                    "tool_call_envelopes": [],
                }

        rt.executor = StructuredExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["response"] == "table-like text must not drive parsing"
        assert result["_exit_signal"] == "done"
        assert rt.executor.calls == ["reaction"]
        assert "_reaction_internal_handoff" not in result
        assert result["_protocol_tool_requests"] == []
        assert result["_tool_summaries"] == []
        assert result["_relation_card_declarations"] == []
        assert result["_memory_write_declarations"] == []

    def test_reaction_tool_guide_request_is_retired_invalid(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class ToolGuideExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "guide observed"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_protocol_guide_request(
                            "memory_link_update",
                            call_id="call_memory_link_guide",
                        )],
                    }
                return {"response": "guide observed", "tool_call_envelopes": []}

        rt.executor = ToolGuideExecutor()

        class ReceiptContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                    "boundary": kwargs["boundary"],
                }

            def load_cache_compaction_debt(self):
                return {}

        rt.ctx_store = ReceiptContext()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])
        assert "原生工具调用警告" in second_call_text
        assert "provider-native 工具" in second_call_text
        assert result["_protocol_tool_requests"] == []
        assert result["_protocol_tool_receipts"] == []
        assert result["_invalid_tool_requests"][0]["tool_id"] == (
            "protocol_tool_guide_request"
        )
        assert result["_invalid_tool_requests"][0]["reason"] == (
            "protocol_tool_guide_request_retired"
        )
        assert result["_tool_transaction_audit"]["status"] == "issues_found"
        assert not any(entry[3] == "protocol_tool_receipt" for entry in rt.ctx_store.entries)

    def test_spec131_memory_write_receipt_returns_to_next_iteration(
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
        helper = self

        class MemoryWriteImmediateExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "receipt observed"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "Spec131",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "Immediate memory body",
                                "candidate_keywords": ["Spec131", "immediate"],
                            },
                            call_id="call_memory_write",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                return {
                    "response": "receipt observed",
                    "tool_call_envelopes": [],
                }

        rt.executor = MemoryWriteImmediateExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert memory_store.entries[0][0] == "MEM-131000AA"
        assert memory_store.ltm["MEM-131000AA"]["meta"]["tags"] == [
            "Spec131", "immediate"]
        assert "memory_write" in second_call_text
        assert "applied" in second_call_text
        assert "MEM-131000AA" in second_call_text
        assert result["_memory_write_receipts"][0]["status"] == "applied"
        assert result["response"] == "receipt observed"

    def test_spec221_memory_write_auto_mounts_and_hides_new_memory(
            self, tmp_path, monkeypatch):
        import logic.memory_write as memory_write_mod
        from data import memory_store as memory_store_mod

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")

        hidden_snapshots = []

        def fake_stm_heat_index(*args, **kwargs):
            hidden = set(getattr(assembler, "_hidden_stm_memory_ids", set()))
            hidden_snapshots.append(hidden)
            if "MEM-221WRITE" in hidden:
                return "## STM 索引"
            return "## STM 索引\n- MEM-221WRITE [hot] SHOULD_NOT_LEAK"

        monkeypatch.setattr(assembler, "_build_stm_heat_index", fake_stm_heat_index)

        store, _memory_index, _container_store = self._patch_memory_immediate_stores(
            monkeypatch, runtime=rt)
        monkeypatch.setattr(memory_write_mod, "generate_mem_id", lambda: "MEM-221WRITE")
        monkeypatch.setattr(memory_store_mod, "MemoryStore", lambda: store)
        heat = rt.heat
        helper = self

        class MemoryWriteMountExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "write mount observed"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "Spec221 write",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "Spec221 write body",
                                "candidate_keywords": ["Spec221", "mount"],
                            },
                            call_id="call_spec221_memory_write",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                return {"response": "write mount observed", "tool_call_envelopes": []}

        rt.executor = MemoryWriteMountExecutor()

        rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "## CONTENT（已挂载正文）" in second_call_text
        assert "### 记忆 MEM-221WRITE" in second_call_text
        assert "Spec221 write body" in second_call_text
        assert "SHOULD_NOT_LEAK" not in second_call_text
        assert {"MEM-221WRITE"} in hidden_snapshots
        assert heat.boosted == [("MEM-221WRITE", 0)]

    def test_spec221_unmount_new_memory_restores_stm_index_projection(
            self, tmp_path, monkeypatch):
        import logic.memory_write as memory_write_mod
        from data import memory_store as memory_store_mod

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")

        hidden_snapshots = []

        def fake_stm_heat_index(*args, **kwargs):
            hidden = set(getattr(assembler, "_hidden_stm_memory_ids", set()))
            hidden_snapshots.append(hidden)
            if "MEM-221WRITE" in hidden:
                return "## STM 索引"
            return "## STM 索引\n- MEM-221WRITE [hot] SHOULD_RESTORE"

        monkeypatch.setattr(assembler, "_build_stm_heat_index", fake_stm_heat_index)

        store, _memory_index, _container_store = self._patch_memory_immediate_stores(
            monkeypatch, runtime=rt)
        monkeypatch.setattr(memory_write_mod, "generate_mem_id", lambda: "MEM-221WRITE")
        monkeypatch.setattr(memory_store_mod, "MemoryStore", lambda: store)
        helper = self

        class MemoryWriteThenUnmountExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "unmount observed"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "Spec221 write",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "Spec221 write body",
                                "candidate_keywords": ["Spec221", "unmount"],
                            },
                            call_id="call_spec221_memory_write",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_content_read",
                            {
                                "mem_id": "MEM-221WRITE",
                                "mount_mode": "none",
                                "reason": "remove current-round body mount",
                            },
                            call_id="call_spec221_memory_unmount",
                            tool_family="protocol_tool",
                            tool_class="read_tool",
                            risk="low",
                        )],
                    }
                return {"response": "unmount observed", "tool_call_envelopes": []}

        rt.executor = MemoryWriteThenUnmountExecutor()

        rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])
        third_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[2])

        assert "### 记忆 MEM-221WRITE" in second_call_text
        assert "### 记忆 MEM-221WRITE" not in third_call_text
        assert "SHOULD_RESTORE" in third_call_text
        assert {"MEM-221WRITE"} in hidden_snapshots
        assert set() in hidden_snapshots

    def test_spec243_memory_link_update_add_is_retired_in_runtime(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        memory_store, _memory_index, container_store = self._patch_memory_immediate_stores(
            monkeypatch,
            runtime=rt,
        )
        helper = self

        class MemoryLinkImmediateExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "retired link receipt observed"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [
                            helper._native_tool_envelope(
                                "memory_write",
                                {
                                    "title": "Spec131 link",
                                    "weight": 4,
                                    "subject": "Codex",
                                    "body": "Immediate link body",
                                    "candidate_keywords": ["Spec131", "link"],
                                },
                                call_id="call_memory_link_write",
                                tool_family="protocol_tool",
                                tool_class="sync_tool",
                                risk="high",
                                index=1,
                            ),
                        ],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_link_update",
                            {
                                "mem_id": "MEM-131000AA",
                                "operation": "add",
                                "container_refs": ["DC-1"],
                                "reason": "bridge",
                            },
                            call_id="call_memory_link_update",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                return {
                    "response": "retired link receipt observed",
                    "tool_call_envelopes": [],
                }

        rt.executor = MemoryLinkImmediateExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )
        third_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[2])

        assert memory_store.meta["MEM-131000AA"]["linked_containers"] == []
        assert container_store.entries == []
        assert "memory_write" in third_call_text
        assert "memory_link_update" in third_call_text
        assert "remove" in third_call_text
        assert result["_memory_write_receipts"][0]["status"] == "applied"
        assert result["_memory_link_update_receipts"] == []
        assert result["_invalid_tool_requests"][0]["reason"] == (
            "native_argument_invalid_enum"
        )
        assert result["response"] == "retired link receipt observed"

    def test_spec131_closeout_submission_defers_until_memory_receipt_visible(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        self._patch_memory_immediate_stores(monkeypatch, runtime=rt)
        helper = self

        class CloseoutWithMemoryExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "final after receipt"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_write",
                            {
                                "title": "Spec131 close",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "Closeout should wait for receipt",
                                "candidate_keywords": ["Spec131", "closeout"],
                            },
                            call_id="call_close_memory_write",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "final after receipt",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("natural final reply should close after receipt")

        rt.executor = CloseoutWithMemoryExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert not any(
            receipt.get("status") == "obligation_finalize_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert "memory_write" in second_call_text
        assert "MEM-131000AA" in second_call_text
        assert result["_final_reply_done"] is True
        assert result["response"] == "final after receipt"
