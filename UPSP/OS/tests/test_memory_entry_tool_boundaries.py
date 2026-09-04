import os
import sys
import json

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class TestMemoryEntryToolBoundaries:
    def test_memory_entry_tools_registered_with_correct_classes(self):
        from logic import protocol_tools

        assert protocol_tools.tool_class_for("memory_content_read") == "read_tool"
        assert protocol_tools.tool_class_for("memory_link_update") == "sync_tool"
        assert protocol_tools.tool_class_for("memory_privacy_mark") == "sync_tool"
        assert protocol_tools.tool_class_for("memory_privacy_declassify") == "sync_tool"
        assert protocol_tools.tool_metadata_for("memory_privacy_mark")["status"] == "disabled"
        assert protocol_tools.tool_metadata_for("memory_privacy_declassify")["status"] == "disabled"

    def test_spec054_handoff_and_heartbeat_tool_boundaries(self):
        from logic import protocol_tools

        assert protocol_tools.tool_metadata_for("setup_handoff")["execution_route"] == "substrate"
        assert protocol_tools.tool_class_for("setup_handoff") == "sync_tool"
        assert protocol_tools.tool_metadata_for("reaction_loop")["execution_route"] == "substrate"
        assert protocol_tools.tool_class_for("reaction_loop") == "sync_tool"
        assert protocol_tools.tool_metadata_for("cleanup_handoff")["execution_route"] == "substrate"
        assert protocol_tools.tool_class_for("cleanup_handoff") == "sync_tool"
        assert protocol_tools.tool_metadata_for("internal_handoff_route") == {}
        assert protocol_tools.tool_metadata_for("heartbeat_restart")["execution_route"] == "substrate"
        assert protocol_tools.tool_class_for("heartbeat_restart") == "sync_tool"
        assert protocol_tools.tool_metadata_for("cache_compact") == {}
        assert protocol_tools.tool_class_for("cache_compact") == ""
        assert protocol_tools.tool_metadata_for("connection_material_settle")["execution_route"] == "substrate"
        assert protocol_tools.tool_class_for("connection_material_settle") == "sync_tool"
        assert protocol_tools.tool_metadata_for("tacit_material_settle")["execution_route"] == "substrate"
        assert protocol_tools.tool_class_for("tacit_material_settle") == "sync_tool"
        assert protocol_tools.tool_metadata_for("association_count_update")["execution_route"] == "substrate"
        assert protocol_tools.tool_class_for("association_count_update") == "sync_tool"
        assert protocol_tools.tool_metadata_for("heartbeat_settle") == {}
        assert protocol_tools.tool_class_for("heartbeat_settle") == ""

    def test_spec066_setup_workflow_tools_are_substrate_only(self):
        from logic import protocol_tools

        expected = {
            "setup_mount_apply": ("substrate", "read_tool", "context"),
            "setup_security_gate": ("substrate", "sync_tool", "security"),
            "setup_handoff": ("substrate", "sync_tool", "setup"),
            "standby_setup_handoff": ("substrate", "sync_tool", "setup"),
        }

        for tool_id, (route, tool_class, domain) in expected.items():
            meta = protocol_tools.tool_metadata_for(tool_id)
            assert meta["execution_route"] == route
            assert meta["tool_class"] == tool_class
            assert meta["domain"] == domain

    def test_spec067_tool_transaction_audit_is_substrate_only(self):
        from logic import protocol_tools

        meta = protocol_tools.tool_metadata_for("tool_transaction_audit")

        assert meta["execution_route"] == "substrate"
        assert meta["tool_class"] == "sync_tool"
        assert meta["domain"] == "audit"
        assert meta["risk"] == "high"
        assert meta["result_kind"] == "round_snapshot_runtime"

    def test_tool_metadata_uses_hidden_routes_and_three_model_postures(self):
        from logic import protocol_tools

        routes = {
            meta.get("execution_route")
            for meta in protocol_tools.TOOL_DEFINITIONS.values()
        }
        postures = {
            meta.get("tool_class")
            for meta in protocol_tools.TOOL_DEFINITIONS.values()
        }
        assert routes == {"internal_processor", "host_dispatch", "substrate"}
        assert postures == {"read_tool", "sync_tool", "action_tool"}
        assert all(
            "tool_family" not in meta
            for meta in protocol_tools.TOOL_DEFINITIONS.values()
        )

    def test_spec067_audit_tool_transactions_reports_ok_for_closed_chain(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            requests=[{"tool_id": "memory_write", "reason": "需要写记忆"}],
            submissions=["memory_write_declaration"],
            receipts=[
                {
                    "tool_id": "memory_write",
                    "tool_class": "sync_tool",
                    "status": "guide_loaded",
                    "source": "protocol_tool_request",
                },
                {
                    "tool_id": "memory_write",
                    "tool_class": "sync_tool",
                    "status": "submission_received",
                    "source": "memory_write_declaration",
                },
                {
                    "tool_id": "memory_write",
                    "tool_class": "sync_tool",
                    "status": "applied",
                    "source": "memory_write_declaration",
                    "mem_id": "MEM-067000AA",
                },
            ],
            active_guides=["memory_write"],
        )

        assert report["tool_id"] == "tool_transaction_audit"
        assert report["tool_class"] == "sync_tool"
        assert report["execution_route"] == "substrate"
        assert report["status"] == "ok"
        assert report["issues"] == []
        assert report["counts"]["submissions"] == 1

    def test_spec067_audit_tool_transactions_reports_missing_processor_receipt(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            requests=[{"tool_id": "memory_write"}],
            submissions=["memory_write_declaration"],
            receipts=[
                {
                    "tool_id": "memory_write",
                    "tool_class": "sync_tool",
                    "status": "guide_loaded",
                    "source": "protocol_tool_request",
                },
                {
                    "tool_id": "memory_write",
                    "tool_class": "sync_tool",
                    "status": "submission_received",
                    "source": "memory_write_declaration",
                },
            ],
            active_guides=["memory_write"],
        )

        assert report["status"] == "issues_found"
        assert report["issues"][0]["code"] == "missing_processor_receipt"
        assert report["issues"][0]["tool_id"] == "memory_write"

    def test_spec434_audit_tool_transactions_accepts_guide_submit_receipt(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            submissions=["guide_submit"],
            receipts=[
                {
                    "tool_id": "guide_submit",
                    "tool_class": "sync_tool",
                    "status": "submission_received",
                    "source": "guide_submit",
                },
                {
                    "tool_id": "guide_submit",
                    "tool_class": "sync_tool",
                    "status": "accepted",
                    "source": "guide_submit",
                    "guide_id": "task:T-20260627-01",
                },
            ],
            active_guides=["guide_submit"],
        )

        assert report["status"] == "ok"
        assert report["issues"] == []

    def test_spec446_audit_accepts_relation_precondition_terminal_receipts(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        for terminal_status in ("relation_body_not_visible", "relation_card_exists"):
            report = audit_tool_transactions(
                submissions=["relation_card_declaration"],
                receipts=[
                    {
                        "tool_id": "relation_card_write",
                        "tool_class": "sync_tool",
                        "status": "submission_received",
                        "source": "relation_card_declaration",
                    },
                    {
                        "tool_id": "relation_card_write",
                        "tool_class": "sync_tool",
                        "status": terminal_status,
                        "source": "relation_card_declaration",
                        "reason": terminal_status,
                    },
                ],
                active_guides=["relation_card_write"],
            )

            assert report["status"] == "ok"
            assert report["issues"] == []

    def test_spec470_audit_accepts_relay_intent_not_found_terminal_receipt(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            submissions=["relay_intent_settle"],
            receipts=[
                {
                    "tool_id": "relay_intent_settle",
                    "tool_class": "sync_tool",
                    "status": "submission_received",
                    "source": "relay_intent_settle",
                },
                {
                    "tool_id": "relay_intent_settle",
                    "tool_class": "sync_tool",
                    "status": "not_found",
                    "source": "relay_intent_settle",
                    "reason": "relay_intent_not_found",
                },
            ],
            active_guides=["relay_intent_settle"],
        )

        assert report["status"] == "ok"
        assert report["issues"] == []

    def test_spec067_audit_tool_transactions_records_invalid_and_metadata_mismatch(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            requests=[],
            submissions=[],
            invalid_submissions=["cache_compact"],
            receipts=[
                {
                    "tool_id": "cache_compact",
                    "tool_class": "sync_tool",
                    "status": "rejected_missing_guide",
                    "source": "cache_compact",
                },
                {
                    "tool_id": "memory_write",
                    "tool_class": "read_tool",
                    "status": "submission_received",
                    "source": "memory_write_declaration",
                },
            ],
            active_guides=[],
        )

        codes = [issue["code"] for issue in report["issues"]]
        assert "invalid_submission_rejected" in codes
        assert "metadata_mismatch" in codes

    def test_spec086_audit_tool_transactions_records_invalid_requests(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            requests=[],
            submissions=[],
            invalid_requests=[
                {
                    "tool_id": "cache_compact",
                    "reason": "unsupported_tool_family",
                },
                {
                    "tool_id": "not_a_real_tool",
                    "reason": "unknown_tool_id",
                },
            ],
            receipts=[],
            active_guides=[],
        )

        assert report["status"] == "issues_found"
        assert report["counts"]["invalid_requests"] == 2
        issues = [
            issue for issue in report["issues"]
            if issue["code"] == "invalid_request_rejected"
        ]
        assert [issue["tool_id"] for issue in issues] == [
            "cache_compact",
            "not_a_real_tool",
        ]
        assert [issue["source"] for issue in issues] == [
            "tool_request",
            "tool_request",
        ]
        assert [issue["detail"] for issue in issues] == [
            "unsupported_tool_family",
            "unknown_tool_id",
        ]

    def test_spec254_corrected_invalid_reaction_finalize_does_not_red_audit(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            requests=[],
            submissions=[],
            invalid_requests=[
                {
                    "tool_id": "reaction_finalize",
                    "source": "provider_tool_call",
                    "reason": "invalid_json",
                    "call_id": "call_bad_closeout",
                },
            ],
            corrected_invalid_requests=[
                {
                    "tool_id": "reaction_finalize",
                    "source": "provider_tool_call",
                    "reason": "invalid_json",
                    "call_id": "call_bad_closeout",
                    "correction_reason": "valid_terminal_action_after_feedback",
                },
            ],
            receipts=[],
            active_guides=[],
        )

        assert report["status"] == "ok"
        assert report["issues"] == []
        assert report["counts"]["invalid_requests"] == 1
        assert report["counts"]["corrected_invalid_requests"] == 1
        assert report["corrected_invalid_requests"][0]["call_id"] == "call_bad_closeout"

    def test_spec254_non_corrected_invalid_request_still_reds_audit(self):
        from logic.tool_transaction_audit import audit_tool_transactions

        report = audit_tool_transactions(
            requests=[],
            submissions=[],
            invalid_requests=[
                {
                    "tool_id": "reaction_finalize",
                    "source": "provider_tool_call",
                    "reason": "invalid_json",
                    "call_id": "call_bad_closeout",
                },
                {
                    "tool_id": "memory_write",
                    "source": "provider_tool_call",
                    "reason": "native_argument_missing_required",
                    "call_id": "call_bad_memory",
                },
            ],
            corrected_invalid_requests=[
                {
                    "tool_id": "reaction_finalize",
                    "source": "provider_tool_call",
                    "reason": "invalid_json",
                    "call_id": "call_bad_closeout",
                    "correction_reason": "valid_terminal_action_after_feedback",
                },
            ],
            receipts=[],
            active_guides=[],
        )

        assert report["status"] == "issues_found"
        assert report["counts"]["invalid_requests"] == 2
        assert report["counts"]["corrected_invalid_requests"] == 1
        assert [
            issue["tool_id"] for issue in report["issues"]
            if issue["code"] == "invalid_request_rejected"
        ] == ["memory_write"]

    def test_spec240_heartbeat_settle_guide_is_retired(self):
        from logic import protocol_tools
        from paths import DOCS_PROTOCOL_TOOLS

        with open(DOCS_PROTOCOL_TOOLS, "r", encoding="utf-8") as f:
            tools_doc = f.read()

        assert not hasattr(protocol_tools, "load_protocol_tool_guide")
        assert "tool_id: heartbeat_settle" not in tools_doc
        assert "heartbeat_settlement_table" not in tools_doc
        heartbeat = protocol_tools.tool_metadata_for("heartbeat_tick")
        assert heartbeat["execution_route"] == "substrate"
        assert heartbeat["tool_class"] == "sync_tool"
        assert "心跳轮" not in tools_doc
        assert "心跳 tick" not in tools_doc

    def test_spec240_known_heartbeat_flags_remain_available_without_llm_tool(self):
        from logic.heartbeat_flags import KNOWN_HEARTBEAT_FLAGS

        assert "continue_requested" in KNOWN_HEARTBEAT_FLAGS
        assert "rhythm_due" in KNOWN_HEARTBEAT_FLAGS

    def test_spec060_fault_record_is_not_provider_exported_without_a_guide(self):
        from logic import protocol_tools
        from paths import DOCS_PROTOCOL_TOOLS
        from logic.native_tool_calls import export_provider_tool_schemas

        with open(DOCS_PROTOCOL_TOOLS, "r", encoding="utf-8") as f:
            tools_doc = f.read()

        exported = {
            item.get("name")
            for item in export_provider_tool_schemas(include_protocol_writes=True)
        }
        assert not hasattr(protocol_tools, "load_protocol_tool_guide")
        assert "fault_record" not in exported
        assert "| fault_record | sync_tool | fault" not in tools_doc
        assert "`01_tool_header` 是唯一活动工具清单" in tools_doc
        assert "chronicle_write" not in tools_doc
        assert "alert_mode_settle" not in tools_doc

    def test_spec060_apply_fault_record_declaration(self):
        from logic.fault_record import apply_fault_record_declarations

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

        class DummyAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        ctx = DummyContext()
        alerts = DummyAlerts()
        receipts = apply_fault_record_declarations(
            [{
                "fault_type": "tool_failure",
                "severity": "error",
                "step": "reaction",
                "source": "web_search",
                "detail": "外部工具超时",
                "action": "fallback",
                "related_tool_id": "web_search",
            }],
            260,
            {"context_store": ctx, "alert_store": alerts},
            interaction_meta={
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            },
        )

        assert receipts == [{
            "tool_id": "fault_record",
            "status": "applied",
            "source": "fault_record_table",
            "fault_type": "tool_failure",
            "severity": "error",
            "step": "reaction",
            "reason": "外部工具超时",
        }]
        assert alerts.entries == [{
            "round_num": 260,
            "step": "reaction",
            "event_type": "tool_failure:error",
            "detail": "web_search: 外部工具超时",
            "action": "fallback",
        }]
        assert ctx.entries == [(
            260,
            "system",
            "[故障记账] severity=error; type=tool_failure; step=reaction; source=web_search; action=fallback; related_tool_id=web_search; detail=外部工具超时",
            "fault_note",
            {
                "step": "reaction",
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            },
        )]

        invalid = apply_fault_record_declarations(
            [{"fault_type": "bad", "severity": "error", "detail": "x"}],
            260,
            {"context_store": ctx, "alert_store": alerts},
        )

        assert invalid[0]["status"] == "error"
        assert invalid[0]["reason"] == "invalid_fault_record"

    def test_memory_content_read_returns_receipt_without_writing(self):
        from logic.memory_content_read import apply_memory_content_read_requests
        from utils.content_ranges import apply_explicit_range

        class DummyMemoryStore:
            def get_meta(self, mem_id):
                return {"id": mem_id, "title": "Visible", "access": "public"}

            def read_body_by_id(self, mem_id, **range_request):
                body = "BODY" * 80
                ranged = apply_explicit_range(body, range_request)
                return {
                    "mem_id": mem_id,
                    "meta": self.get_meta(mem_id),
                    "body": ranged["content"],
                    "read_mode": ranged["read_mode"],
                    "range_requested": ranged["range_requested"],
                    "range_applied": ranged["range_applied"],
                    "total_lines": ranged["total_lines"],
                    "total_chars": ranged["total_chars"],
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
        receipts, mounts, unmounts = apply_memory_content_read_requests(
            [{
                "tool_id": "memory_content_read",
                "mem_id": "MEM-041000AA",
                "mount_mode": "temporary",
                "char_start": 1,
                "char_end": 16,
                "reason": "need body",
            }],
            state={"presence": {"confirmed_subjects": []}},
            data_modules={
                "memory_store": store,
                "memory_recall": DummyMemoryRecall(),
            },
        )

        assert receipts[0]["tool_id"] == "memory_content_read"
        assert "tool_class" not in receipts[0]
        assert receipts[0]["status"] == "accepted"
        assert receipts[0]["body"] == "BODYBODYBODYBODY"
        assert receipts[0]["read_mode"] == "partial"
        assert receipts[0]["range_requested"] == {
            "type": "char",
            "char_start": 1,
            "char_end": 16,
        }
        assert mounts[0]["type"] == "memory"
        assert mounts[0]["ids"] == "MEM-041000AA"
        assert mounts[0]["mode"] == "temporary"
        assert mounts[0]["source"] == "memory_content_read"
        assert mounts[0]["content"] == receipts[0]["body"]
        assert unmounts == []

    def test_spec220_memory_content_read_accepts_ltm_abstract_body(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        from logic.memory_content_read import apply_memory_content_read_requests

        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "stm_memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "stm_meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "stm_index.md"))

        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        abstract_dir.mkdir(parents=True)
        abstract_md = abstract_dir / "abstract.md"
        abstract_meta = abstract_dir / "meta.json"
        monkeypatch.setattr(ms, "LTM_ABSTRACT_ABSTRACT_MD", str(abstract_md), raising=False)
        monkeypatch.setattr(ms, "LTM_ABSTRACT_META_JSON", str(abstract_meta), raising=False)

        mem_id = "MEM-0ABC2203"
        abstract_md.write_text(
            "\n## MEM-0ABC2203  [A]  权重3\n"
            "标题：索引读取不一致\n"
            "梗概：Spec220 复现 LTM Abstract 可见正文。\n",
            encoding="utf-8",
        )
        abstract_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id,
                "title": "索引读取不一致",
                "access": "public",
                "subject": "Codex",
                "type": "A",
                "weight": 3,
            }
        }, ensure_ascii=False), encoding="utf-8")

        store = ms.MemoryStore()

        class DummyMemoryRecall:
            @staticmethod
            def recall(_mem_id, **_kwargs):
                return {
                    "source_memory_layer": "LTM/Abstract",
                    "stm_present": True,
                    "heat_boost_applied": False,
                    "heat_boost_deduplicated": False,
                }

        receipts, mounts, unmounts = apply_memory_content_read_requests(
            [{
                "tool_id": "memory_content_read",
                "mem_id": mem_id,
                "mount_mode": "temporary",
                "reason": "ltm heat index showed this id",
            }],
            state={"presence": {"confirmed_subjects": []}},
            data_modules={
                "memory_store": store,
                "memory_recall": DummyMemoryRecall(),
            },
        )

        assert receipts[0]["status"] == "accepted"
        assert receipts[0]["memory_layer"] == "LTM/Abstract"
        assert receipts[0]["meta"]["title"] == "索引读取不一致"
        assert "Spec220 复现" in receipts[0]["body"]
        assert mounts[0]["type"] == "memory"
        assert mounts[0]["ids"] == mem_id
        assert mounts[0]["mode"] == "temporary"
        assert mounts[0]["source"] == "memory_content_read"
        assert mounts[0]["content"] == receipts[0]["body"]
        assert mounts[0]["read_mode"] == "full"
        assert unmounts == []

    def test_spec220_memory_content_read_blocks_direct_private_mem_id_before_body(self, tmp_path, monkeypatch):
        from data import memory_store as ms
        from logic.memory_content_read import apply_memory_content_read_requests

        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "stm_memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "stm_meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "stm_index.md"))

        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        summary_dir.mkdir(parents=True)
        summary_md = summary_dir / "summary.md"
        summary_meta = summary_dir / "meta.json"
        monkeypatch.setattr(ms, "LTM_SUMMARY_SUMMARY_MD", str(summary_md), raising=False)
        monkeypatch.setattr(ms, "LTM_SUMMARY_META_JSON", str(summary_meta), raising=False)

        mem_id = "MEM-0ABC2204"
        summary_md.write_text(
            "\n## MEM-0ABC2204  [S]  权重3\n"
            "标题：私有 LTM 记忆\n"
            "摘要：这段正文不应在未确认对象时泄露。\n",
            encoding="utf-8",
        )
        (summary_dir / "TzPz.private.md").write_text(
            summary_md.read_text(encoding="utf-8"),
            encoding="utf-8",
        )
        summary_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id,
                "title": "私有 LTM 记忆",
                "access": "private",
                "subject": ["TzPz"],
                "type": "S",
                "weight": 3,
            }
        }, ensure_ascii=False), encoding="utf-8")

        receipts, mounts, unmounts = apply_memory_content_read_requests(
            [{"tool_id": "memory_content_read", "mem_id": mem_id}],
            state={"presence": {"confirmed_subjects": []}},
            data_modules={"memory_store": ms.MemoryStore()},
        )

        assert receipts[0]["status"] == "private_memory_not_visible"
        assert receipts[0]["memory_layer"] == "LTM/Summary"
        assert receipts[0]["body"] == ""
        assert "不应" not in str(receipts[0])
        assert mounts == []
        assert unmounts == []

    def test_private_memory_remains_hidden_when_former_owner_is_present(self):
        from logic.memory_content_read import apply_memory_content_read_requests

        class DummyRelationStore:
            def resolve_active_subject(self, value):
                return {"TzPz": "TzPz", "伙伴": "TzPz", "Codex": "Codex"}.get(value)

        class DummyMemoryStore:
            def get_meta(self, mem_id):
                return {"id": mem_id, "access": "private", "subject": "FMZ"}

            def private_subjects_for_memory(self, mem_id):
                return ["伙伴"]

            def read_body_by_id(self, mem_id, **kwargs):
                return {"meta": self.get_meta(mem_id), "body": "private", "read_mode": "full"}

        modules = {
            "memory_store": DummyMemoryStore(),
            "relation_store": DummyRelationStore(),
        }
        hidden, _, _ = apply_memory_content_read_requests(
            [{"mem_id": "MEM-PRIVATE", "mount_mode": "temporary"}],
            {"presence": {"confirmed_subjects": ["Codex"]}},
            modules,
        )
        still_hidden, _, _ = apply_memory_content_read_requests(
            [{"mem_id": "MEM-PRIVATE", "mount_mode": "temporary"}],
            {"presence": {"confirmed_subjects": ["伙伴"]}},
            modules,
        )

        assert hidden[0]["status"] == "private_memory_not_visible"
        assert still_hidden[0]["status"] == "private_memory_not_visible"

    def test_memory_link_update_rejects_void_pending_id(self):
        from logic.memory_link_update import apply_memory_link_update_declarations

        class DummyMemoryStore:
            def update_linked_containers(self, *args, **kwargs):
                raise AssertionError("void pending id must not write")

        receipts = apply_memory_link_update_declarations(
            [{
                "mem_id": "PENDING-1",
                "operation": "add",
                "container_refs": ["DC-1"],
                "reason": "bridge",
            }],
            data_modules={"memory_store": DummyMemoryStore()},
            pending_memory_ids={},
        )

        assert receipts[0]["status"] == "invalid_pending_mem_id"

    def test_memory_link_update_schema_only_accepts_remove(self):
        from logic.memory_link_update import apply_memory_link_update_declarations

        class DummyMemoryStore:
            def update_linked_containers(self, *args, **kwargs):
                raise AssertionError("retired add/set must not write")

        base = {
            "mem_id": "MEM-041000AA",
            "operation": "add",
            "container_refs": ["DC-12"],
            "reason": "bridge",
        }

        receipts = apply_memory_link_update_declarations(
            [base],
            data_modules={"memory_store": DummyMemoryStore()},
        )
        assert receipts[0]["status"] == "error"
        assert receipts[0]["reason"] == "invalid_operation"

        receipts = apply_memory_link_update_declarations(
            [dict(base, operation="set", current_overview="DC-12：旧 set 路径")],
            data_modules={"memory_store": DummyMemoryStore()},
        )
        assert receipts[0]["status"] == "error"
        assert receipts[0]["reason"] == "invalid_operation"

    def test_memory_link_update_remove_preserves_historical_repair_path(self):
        from logic.memory_link_update import apply_memory_link_update_declarations

        class DummyMemoryStore:
            def __init__(self):
                self.calls = []

            def get_meta(self, mem_id):
                return {"id": mem_id, "access": "public", "subject": "TzPz"}

            def update_linked_containers(
                self, mem_id, operation, container_refs,
                current_overview=None,
            ):
                self.calls.append((mem_id, operation, container_refs, current_overview))
                return {
                    "id": mem_id,
                    "title": "测试记忆",
                    "linked_containers": ["DC-12"] if operation != "remove" else [],
                    "current_overview": current_overview or "保留旧概况",
                }

        store = DummyMemoryStore()
        receipts = apply_memory_link_update_declarations(
            [{
                "mem_id": "MEM-041000AA",
                "operation": "add",
                "container_refs": ["DC-12"],
                "current_overview": "已在 DC-12 中订正为过时判断",
                "reason": "bridge",
            }, {
                "mem_id": "MEM-041000AA",
                "operation": "remove",
                "container_refs": ["DC-12"],
                "reason": "unlink",
            }],
            data_modules={"memory_store": store},
        )

        assert receipts[0]["status"] == "error"
        assert receipts[0]["reason"] == "invalid_operation"
        assert receipts[1]["status"] == "applied"
        assert receipts[1]["current_overview"] == "保留旧概况"
        assert store.calls == [("MEM-041000AA", "remove", ["DC-12"], None)]

    def test_memory_privacy_mark_returns_feature_deferred_without_storage_access(self):
        from logic.memory_privacy import apply_memory_privacy_declarations

        class DummyMemoryStore:
            def mark_private(self, *args, **kwargs):
                raise AssertionError("deferred privacy feature must not write")

        receipts = apply_memory_privacy_declarations(
            [{
                "mem_id": "MEM-041000AA",
                "privacy_subject": "Other",
                "basis": "user_explicit",
                "body_action": "move_private",
                "reason": "private",
            }],
            state={"presence": {"confirmed_subjects": ["TzPz"]}},
            data_modules={"memory_store": DummyMemoryStore()},
        )

        assert receipts[0]["status"] == "feature_deferred"
        assert receipts[0]["reason"] == "feature_deferred"

    def test_memory_privacy_declassify_returns_feature_deferred_without_storage_access(self):
        from logic.memory_privacy import apply_memory_privacy_declassify_declarations

        class DummyMemoryStore:
            def __getattr__(self, name):
                raise AssertionError(f"deferred privacy feature accessed store: {name}")

        receipts = apply_memory_privacy_declassify_declarations(
            [{"mem_id": "MEM-041000AA", "mode": "declassify", "reason": "ok"}],
            state={"presence": {"confirmed_subjects": []}},
            data_modules={"memory_store": DummyMemoryStore()},
        )

        assert receipts[0]["status"] == "feature_deferred"
        assert receipts[0]["reason"] == "feature_deferred"

    def test_private_memory_does_not_create_reconsolidation_candidate(self):
        from logic.memory_reconsolidation import reconsolidation_candidate

        assert reconsolidation_candidate({
            "source": "ltm",
            "ltm": {"tier": "Abstract"},
            "body": "私密正文不得进入指南",
            "meta": {
                "id": "MEM-041000AA",
                "title": "Private",
                "access": "private",
                "weight": 4,
                "stored_at": "2026-08-01T00:00:00+08:00",
                "tags": ["私密"],
            },
        }) is None
