import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, ScriptedExecutor, logical_step
from engines.reaction_protocol_tool_execution import apply_relation_card_declarations


class TestRuntimeReactionMemoryRelationTools(RuntimeTestMixin):
    def test_reaction_memory_annotation_tool_is_retired_before_cleanup(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RetiredAnnotationExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "待命轮无用户回复。"}
                if len(self.calls) == 1:
                    return {
                        "response": "retired annotation text must not be parsed",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_annotation_update",
                            {},
                            call_id="call_retired_annotation",
                            tool_family="",
                            tool_class="",
                            risk="",
                            parse_status="unknown_tool_id",
                        )],
                    }
                return {"response": "retired annotation rejected", "tool_call_envelopes": []}

        rt.executor = RetiredAnnotationExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_memory_annotation_declarations"] == []
        assert result["_memory_annotation_receipts"] == []
        assert result["_protocol_tool_receipts"] == []
        assert {
            "tool_id": "memory_annotation_update",
            "reason": "unknown_tool_id",
        } in [
            {
                "tool_id": item.get("tool_id"),
                "reason": item.get("reason"),
            }
            for item in result["_invalid_tool_requests"]
        ]

    def test_reaction_memory_recall_completion_submission_is_retired(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RecallExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "待命轮无用户回复。"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_recall_complete",
                            {
                                "mem_id": "MEM-041000AA",
                                "completed_body": "Completed recall body",
                                "reason": "compressed body lacked evidence",
                            },
                            call_id="call_recall_complete",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                return {"response": "recall completion observed", "tool_call_envelopes": []}

        rt.executor = RecallExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_protocol_tool_receipts"] == []
        assert {
            "tool_id": "memory_recall_complete",
            "reason": "native_protocol_write_not_enabled",
        } in [
            {
                "tool_id": item.get("tool_id"),
                "reason": item.get("reason"),
            }
            for item in result["_invalid_tool_requests"]
        ]

    @pytest.mark.parametrize("index_fails, expected_status", [
        (False, "applied"),
        (True, "degraded"),
    ])
    def test_reaction_relation_card_submission_commits_before_optional_index(
            self, tmp_path, monkeypatch, index_fails, expected_status):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class DummyMemoryIndex:
            def __init__(self):
                self.relation_calls = []

            def add_relation_keywords(self, *args, **kwargs):
                self.relation_calls.append((args, kwargs))
                if index_fails:
                    raise OSError("relation index unavailable")

        class DummyRelationStore:
            def __init__(self):
                self.created = []

            def load_registry(self):
                return {"cards": [
                    {"id": card_id, "name": name, "category": category,
                     "status": "active", "aliases": []}
                    for card_id, name, category in self.created
                ]}

            def resolve_active_subject(self, value):
                text = str(value or "").strip()
                for card_id, name, _category in self.created:
                    if text in {card_id, name}:
                        return card_id
                return None

            def read_card(self, *args, **kwargs):
                return None

            def find_card(self, subject):
                return None

            def create_card(self, card_id, name, category):
                self.created.append((card_id, name, category))
                return {"id": card_id, "name": name, "category": category}

        relation_store = DummyRelationStore()
        memory_index = DummyMemoryIndex()
        rt.relation_store = relation_store
        rt.memory_index = memory_index

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "relation_card_write",
                    {
                        "name": "Codex",
                        "category": "them",
                        "reason": "confirmed direct interaction",
                    },
                    call_id="call_relation_write",
                    tool_family="protocol_tool",
                    tool_class="sync_tool",
                    risk="high",
                )],
            },
            {"response": "relation card observed", "tool_call_envelopes": []},
        )

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta={
                "interaction_object": "Codex",
                "identity_status": "unregistered",
                "interaction_source": "self_declaration",
            },
        )

        assert relation_store.created == [("Codex", "Codex", "them")]
        assert memory_index.relation_calls == [(("Codex", ["Codex"]), {})]
        assert result["_relation_card_receipts"][0]["tool_id"] == "relation_card_write"
        receipt = result["_relation_card_receipts"][0]
        assert receipt["status"] == expected_status
        assert result["_relation_card_receipts"][0]["call_id"] == "call_relation_write"
        assert result["_protocol_tool_receipts"][-1]["status"] == expected_status
        assert rt.sm.get("base.identity.current_relation_id") == "Codex"
        assert rt.sm.get("base.identity.current_source") == "relation_card_created"
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert all(
            item["obligation_type"] != "relation_card_pending"
            for item in result["_reaction_obligations"]["pending_obligations"]
        )
        assert rt.ctx_store.get_now_entries() == []
        lately_text = "\n".join(
            entry["content"] for entry in rt.ctx_store.get_lately_entries())
        assert "【本轮关系卡写入回执】" in lately_text
        assert f"处理结果：{expected_status}。" in lately_text
        assert "REL-Codex" not in lately_text
        if index_fails:
            assert receipt["reason"] == "relation_index_write_failed"
            assert receipt["repair_debt"] == {
                "kind": "relation_keyword_index_rebuild",
                "card_id": "Codex",
                "error_type": "OSError",
            }
            assert "关系卡与 Registry 已写入" in lately_text
            assert "不要重复创建或重复写入关系卡" in lately_text
            assert not result["_native_tool_feedbacks"]

    def test_relation_card_truth_failure_does_not_run_optional_index(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class FailingRelationStore:
            def find_card(self, _subject):
                return None

            def create_card(self, *_args, **_kwargs):
                raise OSError("relation registry unavailable")

        class UnexpectedRelationIndex:
            def add_relation_keywords(self, *_args, **_kwargs):
                raise AssertionError("optional index must wait for relation truth")

        receipts = apply_relation_card_declarations(
            [{"name": "Codex", "category": "them", "action": "create"}],
            {"interaction_object": "Codex", "identity_status": "declared"},
            guard=rt.cfg.get_relation_card_write_guard(),
            relation_store_factory=FailingRelationStore,
            relation_index_factory=UnexpectedRelationIndex,
        )

        assert receipts == [{
            "tool_id": "relation_card_write",
            "tool_family": "protocol_tool",
            "tool_class": "sync_tool",
            "status": "processor_error",
            "source": "relation_card_declaration",
            "detail": "relation registry unavailable",
        }]

    def test_relation_card_write_rejects_multiple_declarations(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)

        class DummyRelationStore:
            def create_card(self, *args, **kwargs):
                raise AssertionError("multiple declarations must not write")

            def find_card(self, *args, **kwargs):
                return None

        rt.relation_store = DummyRelationStore()
        receipts = apply_relation_card_declarations(
            [
                {"name": "Codex", "category": "them"},
                {"name": "TzPz", "category": "ours"},
            ],
            {"interaction_object": "Codex", "identity_status": "declared"},
            guard=rt.cfg.get_relation_card_write_guard(),
            relation_store_factory=lambda: rt.relation_store,
            relation_index_factory=lambda: rt.memory_index,
        )

        assert receipts == [{
            "tool_id": "relation_card_write",
            "tool_family": "protocol_tool",
            "tool_class": "sync_tool",
            "status": "multiple_relation_card_declarations",
            "source": "relation_card_declaration",
            "reason": "multiple_relation_card_declarations",
        }]

    def test_unregistered_object_cannot_create_a_different_org_card(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class DummyRelationStore:
            def find_card(self, _subject):
                return None

            def create_card(self, *_args, **_kwargs):
                raise AssertionError("mismatched unregistered target must not write")

        rt.relation_store = DummyRelationStore()
        receipts = apply_relation_card_declarations(
            [{"name": "其他组织", "category": "orgs", "action": "create"}],
            {
                "interaction_object": "张三",
                "identity_status": "unregistered",
                "interaction_source": "self_declaration",
            },
            guard=rt.cfg.get_relation_card_write_guard(),
            relation_store_factory=lambda: rt.relation_store,
            relation_index_factory=lambda: rt.memory_index,
        )

        assert receipts[0]["status"] == "rejected"
        assert receipts[0]["reason"] == "identity_or_subject_mismatch"

    def test_relation_card_write_large_delta_guard_returns_needs_review(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(rt.cfg, "get_relation_card_write_guard", lambda: {
            "large_delta_guard": True,
            "max_delta_chars": 8,
        })

        class DummyRelationStore:
            def find_card(self, subject):
                return {"id": "REL-Codex", "name": "Codex", "category": "them"}

            def create_card(self, *args, **kwargs):
                raise AssertionError("guarded write must not create")

            def add_note(self, *args, **kwargs):
                raise AssertionError("guarded write must not add note")

        rt.relation_store = DummyRelationStore()

        receipts = apply_relation_card_declarations(
            [{
                "name": "Codex",
                "category": "them",
                "note": "这是一段超过八个字的关系卡笔记",
                "reason": "large note",
            }],
            {"interaction_object": "Codex", "identity_status": "declared"},
            guard=rt.cfg.get_relation_card_write_guard(),
            visible_relation_body_ids={"REL-Codex"},
            relation_store_factory=lambda: rt.relation_store,
            relation_index_factory=lambda: rt.memory_index,
        )

        assert receipts[0]["status"] == "needs_review"
        assert receipts[0]["delta_chars"] > 8

    def test_spec349_existing_relation_update_requires_visible_body(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)

        class DummyRelationStore:
            def find_card(self, subject):
                return {"id": "REL-Codex", "name": "Codex", "category": "them"}

            def add_note(self, *args, **kwargs):
                raise AssertionError("existing relation card update must require visible body")

        rt.relation_store = DummyRelationStore()

        receipts = apply_relation_card_declarations(
            [{
                "name": "Codex",
                "category": "them",
                "action": "append_note",
                "note": "补充一条关系事实。",
                "reason": "update existing relation",
            }],
            {"interaction_object": "Codex", "identity_status": "declared"},
            guard=rt.cfg.get_relation_card_write_guard(),
            relation_store_factory=lambda: rt.relation_store,
            relation_index_factory=lambda: rt.memory_index,
        )

        assert receipts[0]["status"] == "relation_body_not_visible"
        assert receipts[0]["reason"] == "relation_body_not_visible"
        assert receipts[0]["card_id"] == "REL-Codex"

    @pytest.mark.parametrize("index_fails, expected_status", [
        (False, "applied"),
        (True, "degraded"),
    ])
    def test_spec349_existing_relation_update_after_visible_body_settles_once(
            self, tmp_path, monkeypatch, index_fails, expected_status):
        rt = self._make_runtime(tmp_path)

        class DummyMemoryIndex:
            def __init__(self):
                self.relation_calls = []

            def add_relation_keywords(self, *args, **kwargs):
                self.relation_calls.append((args, kwargs))
                if index_fails:
                    raise OSError("relation index unavailable")

        class DummyRelationStore:
            def __init__(self):
                self.notes = []

            def find_card(self, subject):
                return {"id": "REL-Codex", "name": "Codex", "category": "them"}

            def add_note(self, card_id, note):
                self.notes.append((card_id, note))

        relation_store = DummyRelationStore()
        memory_index = DummyMemoryIndex()
        rt.relation_store = relation_store
        rt.memory_index = memory_index

        receipts = apply_relation_card_declarations(
            [{
                "name": "Codex",
                "category": "them",
                "action": "append_note",
                "note": "补充一条关系事实。",
                "reason": "update existing relation",
            }],
            {"interaction_object": "Codex", "identity_status": "declared"},
            guard=rt.cfg.get_relation_card_write_guard(),
            visible_relation_body_ids={"REL-Codex"},
            relation_store_factory=lambda: rt.relation_store,
            relation_index_factory=lambda: rt.memory_index,
        )

        assert receipts[0]["status"] == expected_status
        assert relation_store.notes == [("REL-Codex", "补充一条关系事实。")]
        assert memory_index.relation_calls == [(("Codex", ["Codex"]), {})]
        if index_fails:
            assert receipts[0]["reason"] == "relation_index_write_failed"
            assert receipts[0]["repair_debt"]["card_id"] == "REL-Codex"

    def test_spec349_existing_relation_create_action_is_rejected(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)

        class DummyRelationStore:
            def find_card(self, subject):
                return {"id": "REL-Codex", "name": "Codex", "category": "them"}

            def add_note(self, *args, **kwargs):
                raise AssertionError("existing create action must be rejected")

        rt.relation_store = DummyRelationStore()

        receipts = apply_relation_card_declarations(
            [{
                "name": "Codex",
                "category": "them",
                "action": "create",
                "note": "不能用 create 更新已有关系卡。",
            }],
            {"interaction_object": "Codex", "identity_status": "declared"},
            guard=rt.cfg.get_relation_card_write_guard(),
            visible_relation_body_ids={"REL-Codex"},
            relation_store_factory=lambda: rt.relation_store,
            relation_index_factory=lambda: rt.memory_index,
        )

        assert receipts[0]["status"] == "relation_card_exists"
        assert receipts[0]["reason"] == "relation_card_exists"

    def test_spec053_relation_card_write_rejects_state_or_axis_fields(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)

        class DummyRelationStore:
            def find_card(self, *args, **kwargs):
                raise AssertionError("state or axis write must not read cards")

            def create_card(self, *args, **kwargs):
                raise AssertionError("state or axis write must not create cards")

            def add_note(self, *args, **kwargs):
                raise AssertionError("state or axis write must not add notes")

        rt.relation_store = DummyRelationStore()

        receipts = apply_relation_card_declarations(
            [{
                "name": "Codex",
                "category": "them",
                "axes": {"trust": 80},
                "status": "active",
            }],
            {"interaction_object": "Codex", "identity_status": "declared"},
            guard=rt.cfg.get_relation_card_write_guard(),
            relation_store_factory=lambda: rt.relation_store,
            relation_index_factory=lambda: rt.memory_index,
        )

        assert receipts == [{
            "tool_id": "relation_card_write",
            "tool_family": "protocol_tool",
            "tool_class": "sync_tool",
            "status": "rejected_state_or_axis_write",
            "source": "relation_card_declaration",
            "reason": "rejected_state_or_axis_write",
            "forbidden_fields": ["axes", "status"],
        }]

    def test_reaction_loop_rewrites_empty_reply_and_ignores_retired_internal_handoff(
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
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {"response": "待命轮无用户回复。"}
                if len(self.calls) == 1:
                    return {
                        "response": "retired internal_handoff text must not be parsed",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_empty_reply_final",
                            memory_settlement={"status": "weight_zero", "reason": "standby empty reply unit test"},
                        )],
                    }
                return {
                    "response": "待命轮无用户回复。",
                    "tool_call_envelopes": [],
                }

        executor = StructuredExecutor()
        rt.executor = executor

        result = rt._run_reaction_loop(rt.sm.load(), "standby", [])

        assert len(executor.calls) == 2
        assert result["response"] == "待命轮无用户回复。"
        assert "_reaction_internal_handoff" not in result
        assert not any(
            receipt.get("status") == "final_closeout_invalid"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
