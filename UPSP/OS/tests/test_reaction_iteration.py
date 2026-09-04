def test_collect_mount_preselection_filters_and_dedupes_items():
    from engines.reaction_iteration import collect_mount_preselection

    mounted, evidence = collect_mount_preselection(
        [
            {
                "type": "memory",
                "ids": "MEM-ABCDEF12, MEM-DEADBEEF, 无, MEM-ABCDEF12",
                "source": "setup_mount",
            },
            {
                "type": "container",
                "ids": "WKS-1 WKS-1 pass -",
                "source": "setup_container",
            },
        ],
        existing_stm_memory_ids={"MEM-ABCDEF12"},
    )

    assert mounted == ["MEM-ABCDEF12", "MEM-ABCDEF12"]
    assert [item["item_id"] for item in evidence] == [
        "MEM-ABCDEF12",
        "MEM-DEADBEEF",
        "WKS-1",
    ]
    assert evidence[0]["origin"] == "setup_preselection"
    assert evidence[0]["selection_trigger"] == "setup_mount"


def test_parse_reaction_iteration_result_rejects_retired_finish_finalize():
    from engines.reaction_iteration import parse_reaction_iteration_result

    result = parse_reaction_iteration_result(
        {
            "response": "",
            "tool_call_envelopes": [
                {
                    "tool_id": "reaction_finalize",
                    "arguments": {
                        "closeout_decision": "finish",
                    },
                    "parse_status": "ok",
                    "index": 0,
                }
            ],
        },
        active_protocol_tool_guides=[],
    )

    assert result.native_mode is True
    assert result.native_terminal_finalize_only is False
    assert result.parsed_reaction["assistant_reply"] == ""
    assert result.parsed_reaction["reaction_loop"] == {}
    assert result.parsed_reaction["invalid_tool_requests"] == [{
        "tool_id": "reaction_finalize",
        "index": 0,
        "reason": "reaction_finalize_retired_field",
        "field": "closeout_decision",
        "expected": ["handoff_text"],
        "actual": "closeout_decision",
    }]


def test_parse_reaction_iteration_result_accepts_native_ordinary_tool_call():
    from engines.reaction_iteration import parse_reaction_iteration_result

    result = parse_reaction_iteration_result(
        {
            "response": "",
            "tool_call_envelopes": [
                {
                    "tool_id": "file_read",
                    "tool_class": "read_tool",
                    "arguments": {
                        "path": "UPSP/OS/persona/docs/protocol/base/tools.md",
                    },
                    "parse_status": "ok",
                    "index": 0,
                }
            ],
        },
        active_protocol_tool_guides=[],
    )

    assert result.native_mode is True
    assert result.native_terminal_finalize_only is False
    assert result.parsed_reaction["exit_signal"] == "waiting_tool"
    assert result.parsed_reaction["general_tool_requests"][0]["tool_id"] == "file_read"


def test_spec568_retired_guide_does_not_reject_ordinary_tool_call():
    from engines.reaction_iteration import parse_reaction_iteration_result

    result = parse_reaction_iteration_result(
        {
            "response": "",
            "tool_call_envelopes": [
                {
                    "tool_id": "file_read",
                    "tool_class": "read_tool",
                    "arguments": {"path": "task_materials/practice_evidence.jsonl"},
                    "parse_status": "ok",
                    "call_id": "call_forbidden_read",
                    "index": 0,
                }
            ],
        },
        active_protocol_tool_guides=["legacy-retired-guide"],
    )

    assert result.native_mode is True
    assert result.native_terminal_finalize_only is False
    assert result.parsed_reaction["general_tool_requests"][0]["tool_id"] == "file_read"
    assert result.parsed_reaction["invalid_tool_requests"] == []


def test_spec525_text_with_retired_finish_finalize_remains_progress_until_corrected():
    from engines.reaction_iteration import parse_reaction_iteration_result

    result = parse_reaction_iteration_result(
        {
            "response": "这是最终回复正文。",
            "tool_call_envelopes": [
                {
                    "tool_id": "reaction_finalize",
                    "arguments": {
                        "closeout_decision": "finish",
                    },
                    "parse_status": "ok",
                    "index": 0,
                }
            ],
        },
        active_protocol_tool_guides=[],
    )

    assert result.message_envelopes[0]["channel"] == "assistant_text"
    assert result.message_envelopes[0]["phase"] == "loop"
    assert "terminal_text_candidate" not in result.message_envelopes[0]
    assert result.parsed_reaction["assistant_progress"] == "这是最终回复正文。"
    assert result.parsed_reaction["invalid_tool_requests"][0]["reason"] == (
        "reaction_finalize_retired_field"
    )


def test_parse_reaction_iteration_result_rejects_retired_text_control_payload():
    from engines.reaction_iteration import parse_reaction_iteration_result

    response = """| 字段 | 英文字段 | 值 |
|---|---|---|
| 退出信号 | `exit_signal` | done |
| 对外回复 | `assistant_reply` | 本轮已实际读取指定路径。 |
| 反应循环结束 | `reaction_loop_done` | true |
| 工具唤醒入口 | `tool_request` | 无 |
"""

    result = parse_reaction_iteration_result(
        {"response": response},
        active_protocol_tool_guides=[],
    )

    assert result.native_mode is True
    assert result.native_terminal_finalize_only is False
    assert result.parsed_reaction["exit_signal"] == "continue_reaction"
    assert result.parsed_reaction["reaction_loop"] == {"reaction_loop_done": False}
    assert "natural_final_reply_candidate" not in result.parsed_reaction
    assert result.parsed_reaction["invalid_tool_requests"] == [{
        "tool_id": "assistant_text",
        "status": "rejected",
        "source": "assistant_text",
        "call_id": "assistant_text_tool_payload",
        "reason": "assistant_text_tool_payload",
    }]
    assert "terminal_text_candidate" not in result.message_envelopes[0]


def test_parse_reaction_iteration_result_maps_tool_free_natural_text_to_final_candidate():
    from engines.reaction_iteration import parse_reaction_iteration_result

    response = "我读完了。"

    result = parse_reaction_iteration_result(
        {"response": response},
        active_protocol_tool_guides=[],
    )

    assert result.native_mode is True
    assert result.native_terminal_finalize_only is False
    assert result.parsed_reaction.get("assistant_reply") in ("", None)
    assert result.parsed_reaction["general_tool_requests"] == []
    assert result.parsed_reaction["invalid_tool_requests"] == []
    assert result.parsed_reaction["assistant_progress"] == ""
    assert result.parsed_reaction["natural_final_reply_candidate"] == response.strip()
    assert result.parsed_reaction["exit_signal"] == "done"
    assert result.message_envelopes[0]["channel"] == "assistant_text"
    assert result.message_envelopes[0]["text"] == response.strip()
    assert result.message_envelopes[0]["terminal_text_candidate"] is True


def test_settle_receipts_does_not_echo_memory_read_body_to_next_iteration():
    from engines.reaction_helpers import settle_receipts_for_next_iteration

    messages = []
    projections = settle_receipts_for_next_iteration(
        messages,
        [{
            "tool_id": "memory_content_read",
            "status": "accepted",
            "source": "protocol_tool_request",
            "call_id": "call-read",
            "mem_id": "MEM-ABCDEF12",
            "memory_layer": "LTM/Summary",
            "body": "正文证据片段",
        }],
    )

    assert messages == []
    assert projections == []


def test_settle_receipts_for_next_iteration_keeps_unmatched_receipt_without_projection():
    from engines.reaction_helpers import settle_receipts_for_next_iteration

    messages = []
    projections = settle_receipts_for_next_iteration(
        messages,
        [{
            "tool_id": "memory_write",
            "status": "applied",
            "call_id": "call-1",
            "mem_id": "MEM-ABCDEF12",
        }],
    )

    assert messages == []
    assert projections == []
