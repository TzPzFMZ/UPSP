import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, ScriptedExecutor
from engines.reaction_protocol_tool_execution import apply_corpus_read_requests


class TestRuntimeReactionReadTools(RuntimeTestMixin):
    def test_spec781_stateful_read_dedup_rechecks_current_resident_state(self):
        from types import SimpleNamespace

        from engines.reaction_tool_settlement import (
            ReactionToolSettlementDispatcher,
            _read_signature,
        )

        class ResidentStore:
            def __init__(self):
                self.items = set()

            def contains(self, *, item_type, item_id, target_file=""):
                return (item_type, item_id, target_file) in self.items

        class RelationStore:
            summary_resident = False

            def load_registry(self):
                return {"cards": [{
                    "id": "REL-Codex",
                    "summary_resident": self.summary_resident,
                }]}

        resident = ResidentStore()
        relation = RelationStore()
        runner = SimpleNamespace(
            assembler=SimpleNamespace(),
            resident_store=resident,
            relation_store=relation,
        )
        dispatcher = ReactionToolSettlementDispatcher(runner)

        memory_request = {
            "tool_id": "memory_content_read",
            "mem_id": "MEM-781READ",
            "mount_mode": "resident",
        }
        memory_receipt = {
            "tool_id": "memory_content_read",
            "status": "accepted",
            "mem_id": "MEM-781READ",
            "mount_mode": "resident",
            "protocol_read_signature": _read_signature(
                "memory_content_read", memory_request),
        }
        executable, duplicates, _requests = (
            dispatcher._filter_duplicate_protocol_reads(
                "memory_content_read", [memory_request], [memory_receipt])
        )
        assert executable == [memory_request]
        assert duplicates == []

        resident.items.add(("memory", "MEM-781READ", ""))
        executable, duplicates, _requests = (
            dispatcher._filter_duplicate_protocol_reads(
                "memory_content_read", [memory_request], [memory_receipt])
        )
        assert executable == []
        assert duplicates[0]["reason"] == "duplicate_protocol_read_satisfied"

        relation_request = {
            "tool_id": "relation_read",
            "card_id": "REL-Codex",
            "summary": "resident",
            "body": "resident",
        }
        relation_receipt = {
            "tool_id": "relation_read",
            "status": "accepted",
            "card_id": "REL-Codex",
            "summary_mode": "resident",
            "body_mode": "resident",
            "protocol_read_signature": _read_signature(
                "relation_read", relation_request),
        }
        executable, duplicates, _requests = (
            dispatcher._filter_duplicate_protocol_reads(
                "relation_read", [relation_request], [relation_receipt])
        )
        assert executable == [relation_request]
        assert duplicates == []

        resident.items.add(("relation", "REL-Codex", ""))
        relation.summary_resident = True
        executable, duplicates, _requests = (
            dispatcher._filter_duplicate_protocol_reads(
                "relation_read", [relation_request], [relation_receipt])
        )
        assert executable == []
        assert duplicates[0]["reason"] == "duplicate_protocol_read_satisfied"

        failed_receipt = dict(memory_receipt, status="rejected")
        resident.items.clear()
        executable, duplicates, _requests = (
            dispatcher._filter_duplicate_protocol_reads(
                "memory_content_read", [memory_request], [failed_receipt])
        )
        assert executable == []
        assert duplicates[0]["reason"] \
            == "duplicate_protocol_read_failure_repeated"

    def test_spec724_ltm_mount_updates_its_recall_coordinates(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        calls = []

        class MemoryStore:
            def mark_recalled(self, mem_id, round_num=None):
                calls.append((mem_id, round_num))

        rt.memory_store = MemoryStore()

        class MemoryRecall:
            memory_store = rt.memory_store
            heat = rt.heat

            def recall(self, mem_id, *, round_num=None, boosted_ids=None,
                       reconsolidation_tracker=None):
                self.memory_store.mark_recalled(mem_id, round_num=round_num)
                boosted_ids.add(mem_id)

        rt.memory_recall = MemoryRecall()
        rt._boost_mounted_memory_once(
            "MEM-ABCDEF12",
            724,
            set(),
            "LTM/Summary",
        )

        assert calls == [("MEM-ABCDEF12", 724)]

    def test_spec724_setup_preselection_updates_ltm_recall(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(rt.assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(rt.assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(rt.assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(rt.assembler.popup, "read_popup", lambda: "")
        calls = []

        class MemoryStore:
            def list_entries(self):
                return []

            def read_meta_by_id(self, mem_id):
                return {"id": mem_id, "_memory_layer": "LTM/Summary"}

            def mark_recalled(self, mem_id, round_num=None):
                calls.append((mem_id, round_num))

        rt.memory_store = MemoryStore()

        class MemoryRecall:
            memory_store = rt.memory_store
            heat = rt.heat

            def recall(self, mem_id, *, round_num=None, boosted_ids=None,
                       reconsolidation_tracker=None):
                self.memory_store.mark_recalled(mem_id, round_num=round_num)
                boosted_ids.add(mem_id)

        rt.memory_recall = MemoryRecall()
        rt.executor = ScriptedExecutor({"response": "done"})
        rt._run_reaction_loop(rt.sm.load(), "interactive", [{
            "type": "memory",
            "ids": "MEM-ABCDEF12",
            "source": "setup_mount",
        }])

        assert calls == [("MEM-ABCDEF12", rt.sm.get_total_round())]

    def test_spec781_resident_memory_recalls_without_training_preselection(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(rt.assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(rt.assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(rt.assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(rt.assembler.popup, "read_popup", lambda: "")
        rt.resident_store.add({
            "item_type": "memory",
            "item_id": "MEM-ABCDEF12",
        })
        calls = []

        class MemoryStore:
            @staticmethod
            def list_entries():
                return []

        rt.memory_store = MemoryStore()

        class MemoryRecall:
            memory_store = rt.memory_store
            heat = rt.heat

            @staticmethod
            def recall(mem_id, *, round_num=None, boosted_ids=None,
                       reconsolidation_tracker=None):
                calls.append((mem_id, round_num))
                boosted_ids.add(mem_id)

        rt.memory_recall = MemoryRecall()
        rt.executor = ScriptedExecutor({"response": "done"})

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert calls == [("MEM-ABCDEF12", rt.sm.get_total_round())]
        assert result["_mounted_memories"] == []
        assert result["_preselection_evidence"] == []

    def test_natural_language_container_declaration_no_longer_creates_container(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {"response": "new project: old natural language path"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_created_containers"] == []
        assert result["_created_containers"] == []
        assert result["_invalid_tool_requests"] == []
        assert result["_assistant_progress"] == []
        assert result["response"] == "new project: old natural language path"

    def test_spec781_memory_container_create_creates_resident_container_and_receipt(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        class DummyMemoryStore:
            def list_entries(self):
                return ["MEM-243RUNTIME"]

            def get_meta(self, mem_id):
                return {
                    "id": mem_id,
                    "subject": "FMZ",
                    "access": "public",
                }

            def read_body_by_id(self, mem_id):
                return {
                    "body": "内容\nRuntime source",
                    "meta": self.get_meta(mem_id),
                }

            def update_linked_containers(
                    self, mem_id, operation, refs, current_overview=None):
                return {
                    "id": mem_id,
                    "title": "Runtime source",
                    "linked_containers": list(refs),
                    "current_overview": current_overview,
                }

            def snapshot_ltm_files(self):
                return {}

            def snapshot_stm_files(self):
                return {}

            def restore_ltm_files(self, _snapshot):
                return None

            def restore_stm_files(self, _snapshot):
                return None

        rt.memory_store = DummyMemoryStore()

        class ContainerCreateExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append(list(messages))
                if step == "final_reply":
                    return {"response": "container focus observed"}
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "memory_container_create",
                            {
                                "mem_id": "MEM-243RUNTIME",
                                "container_type": "PRJ",
                                "title": "Spec 077 verification",
                                "target_file": "plan.md",
                                "container_body": "initial verification content",
                                "current_overview": "{container_id}：runtime verification",
                                "reason": "runtime reaction loop verification",
                            },
                            call_id="call_focus_create",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="high",
                        )],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_focus_premature",
                        )],
                    }
                return {"response": "container focus observed", "tool_call_envelopes": []}

        rt.executor = ContainerCreateExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        receipts = [
            receipt for receipt in result["_protocol_tool_receipts"]
            if receipt.get("tool_id") == "memory_container_create"
        ]
        assert any(receipt.get("status") == "applied" for receipt in receipts), receipts
        applied = [receipt for receipt in receipts if receipt.get("status") == "applied"][0]
        container_id = applied["container_id"]
        assert result["_created_containers"] == [container_id]
        resident_items = rt.resident_store.load()["items"]
        assert any(
            item.get("item_type") == "container"
            and item.get("item_id") == container_id
            and item.get("target_file") == "plan.md"
            for item in resident_items
        )
        assert (tmp_path / "PRJ" / container_id / "registry.json").is_file()
        assert "initial verification content" in (
            tmp_path / "PRJ" / container_id / "plan.md"
        ).read_text(encoding="utf-8")
        assert not any(
            receipt.get("status") == "obligation_finalize_blocked"
            for receipt in result["_reaction_loop_guard_receipts"]
        )
        assert applied.get("container_body_written") is True
        assert applied.get("memory_link_applied") is True
        assert applied["tool_class"] == "sync_tool"
        assert applied["resident_persisted"] is True
        assert applied["visibility_verified"] is False
        assert applied["call_id"] == "call_focus_create"
        assert "general_tool_result" not in str(receipts)

    def test_spec781_container_read_reads_and_persists_resident_target(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        project = store.create_container("PRJ", "Spec 078", target_file="notes.md")
        store.append_container_content(
            project["container_id"],
            "notes.md",
            "read-only verification",
            "container_read receipt body",
        )
        rt.container_store = store

        class ContainerReadExecutor:
            def __init__(self, container_id):
                self.container_id = container_id
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append(list(messages))
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "container_read",
                            {
                                "container_id": self.container_id,
                                "target_file": "notes.md",
                                "reason": "read back verification",
                            },
                            call_id="call_container_read",
                            tool_family="protocol_tool",
                            tool_class="read_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "container read observed", "tool_call_envelopes": []}

        rt.executor = ContainerReadExecutor(project["container_id"])

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        receipts = [
            receipt for receipt in result["_protocol_tool_receipts"]
            if receipt.get("tool_id") == "container_read"
        ]
        assert any(receipt.get("status") == "accepted" for receipt in receipts)
        accepted = [receipt for receipt in receipts if receipt.get("status") == "accepted"][0]
        assert "container_read receipt body" in accepted["content"]
        assert accepted["call_id"] == "call_container_read"
        assert accepted["resident_persisted"] is True
        assert (project["container_id"], "notes.md") in {
            (item.get("item_id"), item.get("target_file"))
            for item in rt.resident_store.load()["items"]
        }
        assert result["_general_tool_results"] == []

    def test_spec220_memory_read_layer_enters_protocol_receipt_cache(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)

        rt._write_protocol_tool_receipts([{
            "tool_id": "memory_content_read",
            "tool_class": "read_tool",
            "status": "accepted",
            "source": "protocol_tool_request",
            "mem_id": "MEM-ABCDEF12",
            "memory_layer": "LTM/Summary",
        }])

        entries = rt.ctx_store.get_now_entries()
        assert entries[-1]["kind"] == "tool_fact"
        assert "【本轮记忆读取回执】" in entries[-1]["content"]
        assert "处理结果：accepted。" in entries[-1]["content"]
        assert "记忆编号：MEM-ABCDEF12。" in entries[-1]["content"]
        assert "记忆层：LTM/Summary。" in entries[-1]["content"]

    def test_spec221_memory_content_read_returns_mount_and_unmount(self):
        from logic.memory_content_read import apply_memory_content_read_requests

        class DummyMemoryStore:
            def read_meta_by_id(self, mem_id):
                return {"id": mem_id, "title": "Spec221 Memory", "access": "public"}

            def read_body_by_id(self, mem_id):
                body = f"## {mem_id}\nSpec221 mounted body"
                return {
                    "body": body,
                    "meta": {"id": mem_id, "title": "Spec221 Memory"},
                    "memory_layer": "STM",
                    "read_mode": "full",
                    "total_lines": len(body.splitlines()),
                    "total_chars": len(body),
                }

        class DummyMemoryRecall:
            @staticmethod
            def recall(_mem_id, **_kwargs):
                return {
                    "source_memory_layer": "STM",
                    "stm_present": True,
                    "heat_boost_applied": False,
                    "heat_boost_deduplicated": False,
                }

        store = DummyMemoryStore()
        class DummyAssembler:
            class ResidentStore:
                def remove_matching(self, **_kwargs):
                    return {"removed": True, "revision": 3}

            resident_store = ResidentStore()

        receipts, mounts, unmounts = apply_memory_content_read_requests(
            [{
                "tool_id": "memory_content_read",
                "mem_id": "MEM-221READ",
                "mount_mode": "temporary",
            }],
            {"presence": {"confirmed_subjects": ["Codex"]}},
            {"memory_store": store, "memory_recall": DummyMemoryRecall()},
        )

        assert receipts[0]["status"] == "accepted"
        assert mounts == [{
            "type": "memory",
            "ids": "MEM-221READ",
            "mode": "temporary",
            "source": "memory_content_read",
            "content": receipts[0]["body"],
            "read_mode": "full",
            "total_lines": receipts[0]["total_lines"],
            "total_chars": receipts[0]["total_chars"],
        }]
        assert unmounts == []

        receipts, mounts, unmounts = apply_memory_content_read_requests(
            [{
                "tool_id": "memory_content_read",
                "mem_id": "MEM-221READ",
                "mount_mode": "none",
                "reason": "本轮不再需要正文",
            }],
            {"presence": {"confirmed_subjects": ["Codex"]}},
            {
                "memory_store": DummyMemoryStore(),
                "resident_store": DummyAssembler.resident_store,
            },
        )

        assert receipts[0]["status"] == "accepted"
        assert receipts[0]["reason"] == "unmounted"
        assert mounts == []
        assert unmounts == ["MEM-221READ"]

    def test_spec221_memory_content_read_temporary_mount_enters_next_content(
            self, tmp_path, monkeypatch):
        from data import memory_store as memory_store_mod

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")

        class DummyMemoryStore:
            def read_meta_by_id(self, mem_id):
                return {"id": mem_id, "title": "Spec221 Read", "access": "public"}

            def get_meta(self, mem_id):
                return {"id": mem_id, "title": "Spec221 Read", "access": "public"}

            def read_body_by_id(self, mem_id):
                body = f"## {mem_id}\nSpec221 read body"
                return {
                    "body": body,
                    "meta": {"id": mem_id, "title": "Spec221 Read"},
                    "memory_layer": "STM",
                    "read_mode": "full",
                    "total_lines": len(body.splitlines()),
                    "total_chars": len(body),
                }

            def read_entry(self, mem_id):
                return f"## {mem_id}\nSpec221 read body"

        class DummyHeat:
            def __init__(self):
                self.boosted = []

            def recall_boost(self, mem_id, round_num=None):
                self.boosted.append((mem_id, round_num))

        store = DummyMemoryStore()
        heat = DummyHeat()

        class DummyMemoryRecall:
            def __init__(self, memory_store, memory_heat):
                self.memory_store = memory_store
                self.heat = memory_heat

            def recall(self, mem_id, *, round_num=None, boosted_ids=None,
                       reconsolidation_tracker=None, **_transaction):
                self.heat.recall_boost(mem_id, round_num=round_num)
                if isinstance(boosted_ids, set):
                    boosted_ids.add(mem_id)
                return {
                    "source_memory_layer": "STM",
                    "stm_present": True,
                    "heat_boost_applied": True,
                    "heat_boost_deduplicated": False,
                }

        monkeypatch.setattr(memory_store_mod, "MemoryStore", lambda: store)
        rt.memory_store = store
        rt.heat = heat
        rt.memory_recall = DummyMemoryRecall(store, heat)
        helper = self

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "memory_content_read",
                        {
                            "mem_id": "MEM-221READ",
                            "mount_mode": "temporary",
                            "reason": "read and mount",
                        },
                    call_id="call_memory_content_read",
                    tool_family="protocol_tool",
                    tool_class="read_tool",
                    risk="low",
                )],
            },
            {"response": "memory read mount observed", "tool_call_envelopes": []},
        )

        rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "## CONTENT（已挂载正文）" in second_call_text
        assert "### 记忆 MEM-221READ" in second_call_text
        assert "Spec221 read body" in second_call_text
        assert heat.boosted == [("MEM-221READ", 0)]

    def test_spec089_relation_read_request_returns_provider_native_receipt(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "relation_read",
                    {
                        "subject": "Codex",
                        "summary": "temporary",
                        "body": "none",
                        "reason": "need current relation context",
                    },
                    call_id="call_relation_read",
                    tool_family="protocol_tool",
                    tool_class="read_tool",
                    risk="low",
                )],
            },
            {"response": "relation read observed", "tool_call_envelopes": []},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "relation_read" in second_call_text
        assert result["_protocol_tool_submissions"] == []
        receipt = result["_protocol_tool_receipts"][0]
        assert receipt["tool_id"] == "relation_read"
        assert receipt["tool_class"] == "read_tool"
        assert receipt["status"] == "rejected"
        assert receipt["reason"] == "relation_card_not_found"
        assert receipt["call_id"] == "call_relation_read"
        assert receipt["provider"] == "openai_responses"
        assert receipt["response_id"] == "resp_call_relation_read"
        assert receipt["provider_item_id"] == "fc_call_relation_read"
        assert receipt["index"] == 0

    def test_spec088_index_view_result_is_available_to_next_reaction_iteration(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(
            assembler,
            "build_index_view",
            lambda **kwargs: {
                "status": "accepted",
                "tool_id": "index_view",
                "content": "INDEX_VIEW_EXPANDED_ROW",
                "scope": kwargs.get("scope"),
            },
            raising=False,
        )
        helper = self

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "index_view",
                    {
                        "scope": "ltm_inverted",
                        "offset": 8,
                        "limit": 8,
                        "reason": "inspect index view",
                    },
                    call_id="call_index_view",
                    tool_family="protocol_tool",
                    tool_class="read_tool",
                    risk="low",
                )],
            },
            {"response": "index view observed", "tool_call_envelopes": []},
        )

        rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "INDEX_VIEW_EXPANDED_ROW" in second_call_text

    def test_spec524_corpus_read_queues_one_shot_dialogue_progress_expansion(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.assembler._active_corpus_registry = {
            "C-00001": {
                "corpus_id": "C-00001",
                "kind": "dialogue_progress",
                "entry_key": "dialogue-progress-key",
            },
            "C-00002": {
                "corpus_id": "C-00002",
                "kind": "tool_fact",
                "entry_key": "tool-fact-key",
            },
        }

        accepted = apply_corpus_read_requests(rt.assembler, [
            {"tool_id": "corpus_read", "corpus_id": "C-00001"}
        ])
        rejected = apply_corpus_read_requests(rt.assembler, [
            {"tool_id": "corpus_read", "corpus_id": "C-00002"},
            {"tool_id": "corpus_read", "corpus_id": "C-99999"},
        ])

        assert accepted[0]["status"] == "accepted"
        assert accepted[0]["expand_lifecycle"] == "next_provider_call_once"
        assert "dialogue-progress-key" in rt.assembler._pending_corpus_expand_once_keys
        assert rejected[0]["reason"] == "corpus_kind_not_expandable"
        assert rejected[1]["reason"] == "corpus_id_not_active"

    def test_spec543_corpus_read_reaction_settlement_does_not_crash(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.assembler._active_corpus_registry = {
            "C-00001": {
                "corpus_id": "C-00001",
                "kind": "dialogue_progress",
                "entry_key": "dialogue-progress-key",
            }
        }
        helper = self

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "corpus_read",
                    {"corpus_id": "C-00001"},
                    call_id="call_corpus_read",
                    tool_family="protocol_tool",
                    tool_class="read_tool",
                    risk="low",
                )],
            },
            {"response": "corpus read observed", "tool_call_envelopes": []},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        receipts = [
            receipt for receipt in result["_protocol_tool_receipts"]
            if receipt.get("tool_id") == "corpus_read"
        ]
        assert receipts
        assert receipts[0]["status"] in {"accepted", "rejected"}
        assert receipts[0]["protocol_read_signature"]

    def test_spec781_native_mount_cancel_clears_resident_reference_and_records_receipt(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        resident_item = {
            "item_type": "container",
            "item_id": "PRJ-305",
            "target_file": "plan.md",
        }
        preflight = rt.assembler.preflight_resident_add(
            resident_item,
            content_overrides={
                ("container", "PRJ-305", "plan.md"): "resident body",
            },
        )
        rt.assembler.resident_store.add(
            resident_item,
            candidate=preflight["document"],
            expected_revision=preflight["expected_revision"],
        )
        monkeypatch.setattr(
            assembler,
            "_load_container_content",
            lambda container_id, target_file=None: "resident body",
        )
        helper = self

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "mount_cancel",
                    {
                        "mount_area": "resident_list",
                        "item_type": "container",
                        "item_id": "PRJ-305",
                        "target_file": "plan.md",
                        "reason": "remove resident target",
                    },
                    call_id="call_mount_cancel",
                    tool_family="protocol_tool",
                    tool_class="sync_tool",
                    risk="medium",
                )],
            },
            {"response": "mount cancel observed", "tool_call_envelopes": []},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert rt.assembler.resident_store.load()["items"] == []
        receipt = result["_mount_cancel_receipts"][0]
        assert receipt["tool_id"] == "mount_cancel"
        assert receipt["status"] == "applied"
        assert receipt["call_id"] == "call_mount_cancel"
        applied_protocol_receipts = [
            item for item in result["_protocol_tool_receipts"]
            if item.get("tool_id") == "mount_cancel"
            and item.get("status") == "applied"
        ]
        assert applied_protocol_receipts

    def test_spec332_duplicate_container_read_is_blocked_in_same_round(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        project = store.create_container("DC", "Spec332", target_file="open.md")
        store.append_container_content(
            project["container_id"],
            "open.md",
            "duplicate read",
            "Spec332 container content",
        )
        rt.container_store = store
        helper = self

        class DuplicateContainerReadExecutor:
            def __init__(self, container_id):
                self.container_id = container_id
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append(list(messages))
                if len(self.calls) in {1, 2}:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "container_read",
                            {
                                "container_id": self.container_id,
                                "target_file": "open.md",
                                "reason": "same read again",
                            },
                            call_id=f"call_container_read_{len(self.calls)}",
                            tool_family="protocol_tool",
                            tool_class="read_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "duplicate read blocked", "tool_call_envelopes": []}

        rt.executor = DuplicateContainerReadExecutor(project["container_id"])

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        container_receipts = [
            receipt for receipt in result["_container_read_receipts"]
            if receipt.get("tool_id") == "container_read"
        ]
        statuses = [receipt.get("status") for receipt in container_receipts]
        reasons = [receipt.get("reason") for receipt in container_receipts]
        assert statuses == ["accepted", "rejected"]
        assert reasons[-1] == "duplicate_protocol_read_satisfied"
        assert container_receipts[-1]["duplicate_of_call_id"] == "call_container_read_1"
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[2])
        assert "工具循环警告" in second_call_text
        assert "不要原样重复调用" in second_call_text

    def test_spec781_container_read_can_remount_after_resident_cancel(
            self, tmp_path, monkeypatch):
        from data import container_store as cs

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(
            cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(
            cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        project = store.create_container("DC", "Spec781 remount", target_file="open.md")
        store.append_container_content(
            project["container_id"],
            "open.md",
            "state-aware read",
            "remount after cancel",
        )
        rt.container_store = store
        helper = self

        class ReadCancelReadExecutor:
            def __init__(self, container_id):
                self.container_id = container_id
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append(list(messages))
                if len(self.calls) in {1, 3}:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "container_read",
                            {
                                "container_id": self.container_id,
                                "target_file": "open.md",
                                "reason": "read current container body",
                            },
                            call_id=f"call_container_read_{len(self.calls)}",
                            tool_family="protocol_tool",
                            tool_class="read_tool",
                            risk="medium",
                        )],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "mount_cancel",
                            {
                                "mount_area": "resident_list",
                                "item_type": "container",
                                "item_id": self.container_id,
                                "target_file": "open.md",
                                "reason": "cancel before remount",
                            },
                            call_id="call_container_cancel",
                            tool_family="protocol_tool",
                            tool_class="sync_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "remounted", "tool_call_envelopes": []}

        rt.executor = ReadCancelReadExecutor(project["container_id"])
        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        reads = result["_container_read_receipts"]
        assert [item["status"] for item in reads] == ["accepted", "accepted"]
        assert all(
            item.get("reason") != "duplicate_protocol_read_satisfied"
            for item in reads
        )
        assert result["_mount_cancel_receipts"][0]["status"] == "applied"
        assert assembler.resident_store.contains(
            item_type="container",
            item_id=project["container_id"],
            target_file="open.md",
        )

    def test_spec734_three_duplicate_protocol_read_frames_block_next_retry(
            self, tmp_path, monkeypatch):
        from engines import reaction_tool_settlement as settlement

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        def accepted_reads(requests, _modules):
            assembler = _modules["assembler"]
            for request in requests:
                item = {
                    "item_type": "container",
                    "item_id": request["container_id"],
                    "target_file": request["target_file"],
                }
                preflight = assembler.preflight_resident_add(
                    item,
                    content_overrides={
                        (
                            "container",
                            request["container_id"],
                            request["target_file"],
                        ): "Spec734 resident body",
                    },
                )
                assembler.resident_store.add(
                    item,
                    candidate=preflight["document"],
                    expected_revision=preflight["expected_revision"],
                )
            return ([{
                "tool_id": "container_read",
                "tool_class": "read_tool",
                "status": "accepted",
                "source": "container_read",
                "container_id": request["container_id"],
                "target_file": request["target_file"],
                "resident_persisted": True,
            } for request in requests], [])

        monkeypatch.setattr(
            settlement,
            "apply_container_read_requests",
            accepted_reads,
        )
        helper = self

        class RepeatingReadExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append(list(messages))
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_tool_envelope(
                        "container_read",
                        {
                            "container_id": "PRJ-SPEC734",
                            "target_file": "open.md",
                            "reason": "read the same content again",
                        },
                        call_id=f"call_repeat_{len(self.calls)}",
                        tool_family="protocol_tool",
                        tool_class="read_tool",
                        risk="medium",
                    )],
                }

        rt.executor = RepeatingReadExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert len(rt.executor.calls) == 4
        assert result["aborted"] is True
        assert result["error"] == "blocked/protocol_read_correction_exhausted"
        assert "protocol_read_correction_exhausted" in result["response"]
        duplicate_receipts = [
            item for item in result["_container_read_receipts"]
            if item.get("reason") == "duplicate_protocol_read_satisfied"
        ]
        assert len(duplicate_receipts) == 3
        guard = next(
            item for item in result["_reaction_loop_guard_receipts"]
            if item.get("status")
            == "protocol_read_correction_exhausted_auto_blocked"
        )
        assert guard["rejection_count"] == 3
        assert guard["rejected_receipt_count"] == 3

    def test_spec734_duplicate_protocol_read_streak_requires_consecutive_frames(self):
        from engines.reaction_runtime_guards import ProtocolReadDuplicateGuard

        guard = ProtocolReadDuplicateGuard()
        duplicate = {
            "reason": "duplicate_protocol_read_satisfied",
            "protocol_read_signature": "memory_content_read:MEM-734",
        }

        assert guard.observe([duplicate], False) == {}
        assert guard.observe([], False) == {}
        assert guard.observe([duplicate], False) == {}
        assert guard.observe([duplicate], False) == {}
        assert guard.observe([duplicate], False)["blocked_reason"] \
            == "blocked/protocol_read_correction_exhausted"

    def test_spec333_same_response_duplicate_container_read_follows_first_failure(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")

        helper = self

        class SameResponseDuplicateFailureExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append(list(messages))
                if len(self.calls) == 1:
                    arguments = {
                        "container_id": "DC-MISSING-333",
                        "target_file": "open.md",
                        "reason": "same response duplicate failure",
                    }
                    return {
                        "response": "",
                        "tool_call_envelopes": [
                            helper._native_tool_envelope(
                                "container_read",
                                arguments,
                                call_id="call_container_read_missing_1",
                                tool_family="protocol_tool",
                                tool_class="read_tool",
                                risk="medium",
                                index=0,
                            ),
                            helper._native_tool_envelope(
                                "container_read",
                                arguments,
                                call_id="call_container_read_missing_2",
                                tool_family="protocol_tool",
                                tool_class="read_tool",
                                risk="medium",
                                index=1,
                            ),
                        ],
                    }
                return {"response": "duplicate read failure observed", "tool_call_envelopes": []}

        rt.executor = SameResponseDuplicateFailureExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        container_receipts = [
            receipt for receipt in result["_container_read_receipts"]
            if receipt.get("tool_id") == "container_read"
        ]
        statuses = [receipt.get("status") for receipt in container_receipts]
        reasons = [receipt.get("reason") for receipt in container_receipts]
        assert statuses == ["rejected", "rejected"]
        assert reasons == ["container_not_found", "duplicate_protocol_read_failure_repeated"]
        assert container_receipts[-1]["duplicate_of_call_id"] == "call_container_read_missing_1"
        assert container_receipts[-1]["previous_status"] == "rejected"
