import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from native_tool_test_helpers import (
    _load_native_round_inspector,
    _write_round_jsonl,
)


class TestSpec143NativeToolRoundInspector:
    def test_spec263_inspector_rejects_empty_final_response_when_required(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed", "final_response": ""},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            require_round_closed=True,
            require_final_response=True,
            require_runtime_audit_ok=True,
        )

        assert "final_response_empty" in summary["issues"]
        assert summary["ok"] is False

    def test_spec756_inspector_accepts_verified_continue_handoff(self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "heartbeat_rearm",
                "phase": "cleanup",
                "payload": {
                    "status": "continue_requested_rearmed",
                    "set_flags": ["continue_requested"],
                    "relay_intent": {
                        "status": "open",
                        "relay_intent_id": "RI-756",
                    },
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {
                    "status": "closed",
                    "final_response": "",
                    "final_response_source": "reaction.continue_handoff",
                },
            },
        ], round_num=756)

        summary = inspector.inspect_round_file(
            str(round_file),
            require_round_closed=True,
            require_final_response=True,
            require_runtime_audit_ok=True,
        )

        assert summary["verified_continue_handoff"] is True
        assert "final_response_empty" not in summary["issues"]
        assert summary["ok"] is True

    def test_spec324_inspector_prefers_top_level_runtime_status(self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "relay"}},
            {
                "event_type": "runtime_audit",
                "payload": {
                    "status": "issues",
                    "issues": ["runtime_exception:reaction"],
                    "tool_transaction_audit": {"status": "ok"},
                    "runtime_exception": {
                        "failed_phase": "reaction",
                        "error": "reaction step exception: HTTP 402",
                    },
                },
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ], round_num=324)

        summary = inspector.inspect_round_file(
            str(round_file),
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["runtime_audit_status"] == "issues"
        assert "runtime_audit_not_ok:issues" in summary["issues"]
        assert summary["ok"] is False

    def test_spec143_inspector_accepts_reaction_native_file_read_round(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "setup",
                "iteration": 1,
                "payload": {
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 0,
                    },
                    "tool_call_envelopes": [],
                },
            },
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_143",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_143",
                        "call_id": "call_143",
                        "provider_item_id": "fc_143",
                        "index": 0,
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "user",
                        "content": "历史缓存：| tool_request | file_read | old |",
                    }, {
                        "role": "tool",
                        "kind": "native_tool_result",
                        "content": "provider_native_tool_result: call_143",
                        "native_tool_call_envelopes": [{
                            "call_id": "call_143",
                            "tool_id": "file_read",
                        }],
                        "native_tool_outputs": [{
                            "call_id": "call_143",
                            "tool_id": "file_read",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is True
        assert summary["providers"] == ["openai_responses"]
        assert summary["tool_ids"] == ["file_read"]
        assert summary["call_ids"] == ["call_143"]
        assert summary["native_output_call_ids"] == ["call_143"]
        assert summary["matched_call_ids"] == ["call_143"]
        assert summary["tool_result_statuses"] == {"ok": 1}
        assert summary["legacy_text_requests_seen"] is False
        assert summary["legacy_text_request_sources"] == []

    def test_spec284_inspector_accepts_general_tool_results_without_native_outputs(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_chat",
                        "response_id": "resp_284",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_chat",
                        "response_id": "resp_284",
                        "call_id": "call_284_read",
                        "provider_item_id": "call_284_read",
                        "index": 0,
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_settlement",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "general_tool_results": [{
                        "call_id": "call_284_read",
                        "tool_id": "file_read",
                        "status": "ok",
                        "result_kind": "general_tool_result",
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_chat",
            require_round_closed=True,
            require_runtime_audit_ok=True,
            required_tool_result_status="ok",
        )

        assert summary["ok"] is True
        assert summary["native_output_call_ids"] == []
        assert summary["tool_result_call_ids"] == ["call_284_read"]
        assert summary["matched_call_ids"] == ["call_284_read"]
        assert summary["unmatched_envelope_call_ids"] == []
        assert summary["tool_result_statuses"] == {"ok": 1}

    def test_inspector_reads_reaction_progress_from_raw_response(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "进度已记录",
                    "provider_response_meta": {
                        "provider": "openai_chat",
                        "response_id": "resp_progress",
                        "raw_tool_call_count": 0,
                    },
                    "tool_call_envelopes": [],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed", "final_response": "进度已记录"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_provider="openai_chat",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is True
        assert summary["tool_ids"] == []
        assert summary["unmatched_envelope_call_ids"] == []

    def test_spec251_inspector_rejects_retired_memory_no_write_reason(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_parsed",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "exit_signal": "done",
                    "memory_no_write_reason": (
                        "本轮只是读书复述，没有新裁决、工程结论或关系变化，"
                        "因此不写新记忆。"
                    ),
                    "obligation_resolutions": [],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {"event_type": "round_closed", "payload": {"status": "closed"}},
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            require_round_closed=True,
            require_runtime_audit_ok=True,
            require_settlement_quality=True,
        )

        assert summary["ok"] is False
        assert "memory_no_write_reason_retired" in summary["issues"]
        assert summary["settlement_issues"] == ["memory_no_write_reason_retired"]

    def test_spec272_inspector_accepts_settlement_ledgers(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "step_settlement",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "exit_signal": "done",
                    "settlement_ledgers": [{
                        "closeout_decision": "finish",
                        "memory_status": "weight_zero",
                        "memory_refs": [],
                        "memory_reason": "本轮只有连接测试和无内容短句确认。",
                        "read_status": "not_applicable",
                        "read_refs": [],
                        "read_reason": "本轮没有读文件任务。",
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {"event_type": "round_closed", "payload": {"status": "closed"}},
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            require_round_closed=True,
            require_runtime_audit_ok=True,
            require_settlement_quality=True,
        )

        assert summary["ok"] is True
        assert summary["settlement_issues"] == []
        assert summary["memory_settlements"] == []
        assert summary["read_settlements"] == []
        assert summary["settlement_ledgers"][0]["memory_status"] == "weight_zero"
        assert summary["settlement_ledgers"][0]["read_status"] == "not_applicable"

    def test_spec147_inspector_accepts_reading_dogfood_read_only_file_read(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {
                "event_type": "round_started",
                "payload": {
                    "round_type": "interactive",
                    "user_input": "【读书轮】请只读 README.md",
                },
            },
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_147",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_147",
                        "call_id": "call_147",
                        "provider_item_id": "fc_147",
                        "index": 0,
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_147",
                            "tool_id": "file_read",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            required_tool_result_status="ok",
            require_round_closed=True,
            require_runtime_audit_ok=True,
            require_reading_dogfood=True,
            require_read_only_tools_only=True,
        )

        assert summary["ok"] is True
        assert summary["reading_dogfood"] is True
        assert summary["dogfood_label"] == "读书轮"
        assert summary["read_only_tool_ids"] == ["file_read"]
        assert summary["non_read_only_tool_ids"] == []
        assert summary["tool_classes"] == {"file_read": "read_tool"}
        assert "tool_families" not in summary

    def test_spec249_inspector_accepts_machine_dogfood_label_in_input_snapshot(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {
                "event_type": "round_started",
                "payload": {
                    "round_type": "interactive",
                    "input_snapshot": {
                        "flags": {},
                        "dogfood_label": "读书轮",
                    },
                },
            },
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_249",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_249",
                        "call_id": "call_249",
                        "provider_item_id": "fc_249",
                        "index": 0,
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_249",
                            "tool_id": "file_read",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            required_tool_result_status="ok",
            require_round_closed=True,
            require_runtime_audit_ok=True,
            require_reading_dogfood=True,
            require_read_only_tools_only=True,
        )

        assert summary["ok"] is True
        assert summary["reading_dogfood"] is True
        assert summary["dogfood_label"] == "读书轮"
        assert summary["dogfood_label_texts"] == ["读书轮"]

    def test_spec147_inspector_rejects_missing_reading_dogfood_label(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_missing_label",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_missing_label",
                        "call_id": "call_missing_label",
                        "provider_item_id": "fc_missing_label",
                        "index": 0,
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_missing_label",
                            "tool_id": "file_read",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            required_tool_result_status="ok",
            require_round_closed=True,
            require_runtime_audit_ok=True,
            require_reading_dogfood=True,
        )

        assert summary["ok"] is False
        assert summary["reading_dogfood"] is False
        assert "reading_dogfood_label_missing:读书轮" in summary["issues"]

    def test_spec147_inspector_rejects_non_read_only_native_tool(self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {
                "event_type": "round_started",
                "payload": {
                    "round_type": "interactive",
                    "user_input": "【读书轮】请只读 README.md",
                },
            },
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_not_read_only",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_not_read_only",
                        "call_id": "call_not_read_only",
                        "provider_item_id": "fc_not_read_only",
                        "index": 0,
                        "tool_id": "memory_write",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_not_read_only",
                            "tool_id": "memory_write",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
            require_reading_dogfood=True,
            require_read_only_tools_only=True,
        )

        assert summary["ok"] is False
        assert summary["read_only_tool_ids"] == []
        assert summary["non_read_only_tool_ids"] == ["memory_write"]
        assert summary["tool_classes"] == {"memory_write": "sync_tool"}
        assert "non_read_only_tool_seen:memory_write" in summary["issues"]

    def test_spec146_inspector_rejects_required_ok_when_native_output_rejected(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_146",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_146",
                        "call_id": "call_146",
                        "provider_item_id": "fc_146",
                        "index": 0,
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_146",
                            "tool_id": "file_read",
                            "status": "rejected",
                            "reason": "file_not_found",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
            required_tool_result_status="ok",
        )

        assert summary["ok"] is False
        assert summary["tool_result_statuses"] == {"rejected": 1}
        assert summary["tool_result_reasons"] == {"file_not_found": 1}
        assert "tool_result_status_not_ok:file_read:call_146:rejected" in summary["issues"]

    def test_spec143_inspector_rejects_current_reaction_legacy_text_request(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "| tool_request | file_read | path=README.md |",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 0,
                    },
                    "tool_call_envelopes": [],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is False
        assert summary["legacy_text_requests_seen"] is True
        assert summary["legacy_text_request_sources"] == ["reaction:1"]
        assert "legacy_text_request_seen" in summary["issues"]
        assert "missing_required_tool:file_read" in summary["issues"]

    def test_spec156_inspector_allows_protocol_receipt_source_text(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": (
                        "[协议工具回执]\n"
                        "- fault_record: guide_loaded "
                        "(protocol_tool_request)"
                    ),
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_156_receipt",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_156_receipt",
                        "call_id": "call_156_receipt",
                        "provider_item_id": "fc_156_receipt",
                        "index": 0,
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_156_receipt",
                            "tool_id": "file_read",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is True
        assert summary["legacy_text_requests_seen"] is False
        assert summary["legacy_text_request_sources"] == []

    def test_spec143_inspector_reports_count_mismatch_and_unmatched_call_id(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                    "raw_tool_call_count": 2,
                    },
                    "tool_call_envelopes": [{
                        "call_id": "call_missing_output",
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is False
        assert summary["raw_tool_call_count"] == 2
        assert summary["raw_tool_call_count_mismatch"] is True
        assert summary["unmatched_envelope_call_ids"] == ["call_missing_output"]
        assert "raw_tool_call_count_mismatch" in summary["issues"]
        assert "unmatched_tool_result:call_missing_output" in summary["issues"]

    def test_spec243_inspector_matches_protocol_guide_request_receipt(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "call_id": "call_guide",
                        "tool_id": "protocol_tool_guide_request",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_settlement",
                "phase": "reaction",
                "payload": {
                    "protocol_tool_receipts": [{
                        "call_id": "call_guide",
                        "tool_id": "memory_link_update",
                        "status": "guide_loaded",
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is True
        assert summary["guide_loaded_call_ids"] == ["call_guide"]
        assert summary["matched_call_ids"] == ["call_guide"]
        assert summary["unmatched_envelope_call_ids"] == []
        assert "unmatched_tool_result:call_guide" not in summary["issues"]

    def test_spec243_inspector_still_reports_unmatched_missing_guide_receipt(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "call_id": "call_missing_guide_receipt",
                        "tool_id": "protocol_tool_guide_request",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is False
        assert summary["guide_loaded_call_ids"] == []
        assert summary["unmatched_envelope_call_ids"] == [
            "call_missing_guide_receipt",
        ]
        assert (
            "unmatched_tool_result:call_missing_guide_receipt"
            in summary["issues"]
        )

    def test_spec292_inspector_treats_corrected_closeout_invalid_as_settled(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        corrected = {
            "tool_id": "file_read",
            "call_id": "call_terminal_read",
            "source": "terminal_correction",
            "reason": "native_argument_missing_required",
            "correctable_terminal_attempt": True,
            "correction_reason": "valid_terminal_action_after_feedback",
        }
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [
                        {
                            "call_id": "call_terminal_read",
                            "tool_id": "file_read",
                            "parse_status": "ok",
                        },
                    ],
                },
            },
            {
                "event_type": "step_settlement",
                "phase": "reaction",
                "payload": {
                    "invalid_tool_requests": [dict(corrected)],
                    "corrected_invalid_tool_requests": [dict(corrected)],
                    "tool_transaction_audit": {
                        "status": "ok",
                        "corrected_invalid_requests": [dict(corrected)],
                    },
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        assert summary["ok"] is True
        assert summary["corrected_invalid_call_ids"] == ["call_terminal_read"]
        assert summary["matched_call_ids"] == ["call_terminal_read"]
        assert summary["unmatched_envelope_call_ids"] == []
        assert "unmatched_tool_result:call_closeout_read" not in summary["issues"]

    def test_spec184_inspector_exempts_step_terminal_from_native_output_match(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "response_id": "resp_184",
                        "raw_tool_call_count": 2,
                    },
                    "tool_call_envelopes": [
                        {
                            "provider": "openai_responses",
                            "response_id": "resp_184",
                            "call_id": "call_184_read",
                            "provider_item_id": "fc_184_read",
                            "index": 0,
                            "tool_id": "file_read",
                            "parse_status": "ok",
                        },
                        {
                            "provider": "openai_responses",
                            "response_id": "resp_184",
                            "call_id": "call_184_terminal",
                            "provider_item_id": "fc_184_terminal",
                            "index": 1,
                            "tool_id": "reaction_finalize",
                            "parse_status": "ok",
                        },
                    ],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_184_read",
                            "tool_id": "file_read",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["file_read"],
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
            required_tool_result_status="ok",
        )

        assert summary["ok"] is True
        assert summary["tool_ids"] == ["file_read", "reaction_finalize"]
        assert summary["matched_call_ids"] == ["call_184_read"]
        assert summary["unmatched_envelope_call_ids"] == []
        assert "unmatched_tool_result:call_184_terminal" not in summary["issues"]

    def test_spec143_inspector_counts_parse_statuses_and_supports_cli_latest(
            self, tmp_path, capsys):
        inspector = _load_native_round_inspector()
        _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "call_id": "call_unknown",
                        "tool_id": "not_a_tool",
                        "parse_status": "unknown_tool_id",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_unknown",
                            "tool_id": "not_a_tool",
                            "status": "invalid_tool_request",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ], round_num=142)

        exit_code = inspector.main([
            "--latest",
            "--round-dir",
            str(tmp_path / "round"),
            "--require-provider",
            "openai_responses",
            "--require-round-closed",
            "--require-runtime-audit-ok",
        ])

        captured = capsys.readouterr()
        summary = json.loads(captured.out)
        assert exit_code == 0
        assert summary["round_num"] == 142
        assert summary["parse_statuses"] == {"unknown_tool_id": 1}
        assert summary["matched_call_ids"] == ["call_unknown"]

    def test_spec143_inspector_cli_rejects_runtime_audit_failure(
            self, tmp_path, capsys):
        inspector = _load_native_round_inspector()
        _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "call_id": "call_bad_audit",
                        "tool_id": "file_read",
                        "parse_status": "ok",
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_bad_audit",
                            "tool_id": "file_read",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "issues"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ], round_num=143)

        exit_code = inspector.main([
            "--latest",
            "--round-dir",
            str(tmp_path / "round"),
            "--require-tool",
            "file_read",
            "--require-provider",
            "openai_responses",
            "--require-round-closed",
            "--require-runtime-audit-ok",
        ])

        captured = capsys.readouterr()
        summary = json.loads(captured.out)
        assert exit_code == 1
        assert summary["ok"] is False
        assert summary["runtime_audit_status"] == "issues"
        assert "runtime_audit_not_ok:issues" in summary["issues"]

    def test_spec143_inspector_summary_does_not_echo_sensitive_arguments(
            self, tmp_path):
        inspector = _load_native_round_inspector()
        round_file = _write_round_jsonl(tmp_path, [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "response": "",
                    "provider_response_meta": {
                        "provider": "openai_responses",
                        "raw_tool_call_count": 1,
                    },
                    "tool_call_envelopes": [{
                        "call_id": "call_secret",
                        "tool_id": "web_fetch",
                        "parse_status": "ok",
                        "arguments_json": "{\"api_key\":\"sk-secret-value\"}",
                        "arguments": {"api_key": "sk-secret-value"},
                    }],
                },
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "messages": [{
                        "role": "tool",
                        "kind": "native_tool_result",
                        "native_tool_outputs": [{
                            "call_id": "call_secret",
                            "tool_id": "web_fetch",
                            "status": "ok",
                        }],
                    }],
                },
            },
            {
                "event_type": "runtime_audit",
                "payload": {"tool_transaction_audit": {"status": "ok"}},
            },
            {
                "event_type": "round_closed",
                "payload": {"status": "closed"},
            },
        ])

        summary = inspector.inspect_round_file(
            str(round_file),
            required_tools=["web_fetch"],
            required_provider="openai_responses",
            require_round_closed=True,
            require_runtime_audit_ok=True,
        )

        serialized = json.dumps(summary, ensure_ascii=False)
        assert summary["ok"] is True
        assert "sk-secret-value" not in serialized
        assert "arguments_json" not in serialized
