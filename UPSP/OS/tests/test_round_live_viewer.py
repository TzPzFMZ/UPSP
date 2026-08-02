import json
from pathlib import Path

import pytest

from data.round_live_viewer import build_live_state, events_after
from data.round_audit_viewer import latest_event_index


def _event(index, event_type, payload=None, phase=None, iteration=1):
    event = {
        "schema_version": "round_audit.v1",
        "round": 614,
        "event_index": index,
        "event_id": f"R000614-{index:06d}",
        "event_type": event_type,
        "recorded_at": "2026-06-22T00:00:00+08:00",
        "payload": payload or {},
    }
    if phase:
        event["phase"] = phase
        event["iteration"] = iteration
    return event


LAYER_KEYS = [
    "00_call_header",
    "01_tool_header",
    "02_generation_config",
    "10_permanent",
    "20_periodic",
    "30_lately",
    "40_high_freq",
    "50_now",
    "60_statusbar",
    "99_popup",
]


def _layers_snapshot(projections=None, **contents):
    projections = projections or {}
    layers = []
    for order, layer_key in enumerate(LAYER_KEYS):
        content = contents.get(layer_key, f"{layer_key} content")
        layer = {
            "schema": "context_layer.v1",
            "layer_key": layer_key,
            "layer_id": layer_key[3:],
            "order": order if order < 3 else int(layer_key[:2]),
            "source": "unit",
            "chars": len(str(content)),
            "sha256": f"sha-{layer_key}",
            "content": content,
        }
        if layer_key in projections:
            layer["projection"] = projections[layer_key]
        layers.append(layer)
    return {
        "schema": "context_layers_snapshot.v1",
        "source": "unit",
        "layer_order": LAYER_KEYS,
        "layers": layers,
    }


def test_live_viewer_prefers_explicit_frame_id():
    event = _event(1, "llm_call_started", {}, phase="reaction", iteration=3)
    event["frame_id"] = "R000614:reaction:explicit-3"

    state = build_live_state([event])

    assert state["call_frames"][0]["frame_id"] == event["frame_id"]
    assert state["latest_frame_id"] == event["frame_id"]


def test_spec427_live_projection_uses_layers_snapshot_for_ten_panes():
    events = [
        _event(1, "step_input_snapshot", {
            "messages": [
                {"role": "system", "content": "plain text says POPUP but is not a pane marker"}
            ],
            "layers_snapshot": _layers_snapshot(
                **{
                    "50_now": "now layer truth",
                    "99_popup": "popup layer truth",
                }
            ),
        }, phase="reaction"),
    ]

    state = build_live_state(events)

    assert [pane["id"] for pane in state["context_panes"]] == LAYER_KEYS
    by_id = {pane["id"]: pane for pane in state["context_panes"]}
    assert by_id["50_now"]["content_md"] == "now layer truth"
    assert by_id["99_popup"]["content_md"] == "popup layer truth"
    assert "plain text says POPUP" not in by_id["99_popup"]["content_md"]
    assert state["call_frames"][0]["layer_source"] == "layers_snapshot"


def test_spec683_statusbar_projection_passes_through_structured_snapshot_only():
    projection = {
        "schema": "statusbar_snapshot.v1",
        "round": {"id": "R000614", "progress": "运行中"},
        "mode": "实践",
    }
    events = [
        _event(1, "step_input_snapshot", {
            "layers_snapshot": _layers_snapshot(
                projections={"60_statusbar": projection},
                **{"60_statusbar": "## STATUSBAR\n伪造 mode=理论"},
            ),
        }, phase="reaction"),
    ]

    state = build_live_state(events)

    assert state["statusbar_projection"] == projection
    assert state["statusbar_projection"] is not projection


def test_spec683_round_lifecycle_projects_closed_degraded_settlement():
    events = [
        _event(1, "round_started", {"round_type": "interactive"}),
        _event(2, "round_close_requested"),
        _event(3, "state_settle_receipt", {
            "degraded_reasons": ["relation_index:unavailable"],
        }),
        _event(4, "cleanup_obligation_created"),
        _event(5, "round_settled", {
            "status": "degraded",
        }),
        _event(6, "round_closed", {"status": "closed", "final_response": "完成"}),
    ]

    lifecycle = build_live_state(events)["round_lifecycle"]

    assert lifecycle == {
        "state": "closed",
        "settlement_status": "degraded",
        "event_indexes": {
            "round_started": 1,
            "round_close_requested": 2,
            "cleanup_obligation_created": 4,
            "round_settled": 5,
            "round_closed": 6,
        },
        "fatal_reasons": [],
        "degraded_reasons": ["relation_index:unavailable"],
    }


def test_spec683_round_lifecycle_projects_unsettled_without_false_close():
    events = [
        _event(1, "round_started"),
        _event(2, "round_close_requested"),
        _event(3, "cleanup_obligation_created"),
        _event(4, "cleanup_obligation_failed", {
            "status": "unsettled",
            "fatal_reasons": ["state_backup:disk_full"],
        }),
        _event(5, "round_unsettled", {
            "status": "unsettled",
            "fatal_reasons": ["state_backup:disk_full"],
        }),
    ]

    lifecycle = build_live_state(events)["round_lifecycle"]

    assert lifecycle["state"] == "unsettled"
    assert lifecycle["settlement_status"] == "unsettled"
    assert lifecycle["fatal_reasons"] == ["state_backup:disk_full"]
    assert "round_closed" not in lifecycle["event_indexes"]


def test_spec704_stop_events_project_degraded_closed_round_without_fake_reply():
    events = [
        _event(1, "round_started", {"round_type": "interactive"}),
        _event(2, "runtime_stop_requested", {"stage": "reaction"}, phase="reaction"),
        _event(3, "provider_call_cancelled", {"reason": "user_stopped"}, phase="reaction"),
        _event(4, "round_close_requested"),
        _event(5, "cleanup_obligation_settled", {
            "status": "degraded",
            "degraded_reasons": ["user_stopped"],
        }),
        _event(6, "round_settled", {
            "status": "degraded",
            "degraded_reasons": ["user_stopped"],
        }),
        _event(7, "round_closed", {
            "status": "closed",
            "final_response": "",
            "final_response_source": "runtime.user_stop",
        }),
    ]

    state = build_live_state(events)
    lifecycle = state["round_lifecycle"]
    event_types = [card["event_type"] for card in state["conversation"]]

    assert lifecycle["state"] == "closed"
    assert lifecycle["settlement_status"] == "degraded"
    assert lifecycle["degraded_reasons"] == ["user_stopped"]
    assert "runtime_stop_requested" in event_types
    assert "provider_call_cancelled" in event_types
    assert not any(card["type"] == "assistant-final" for card in state["conversation"])


def test_spec704_crash_recovery_projects_unsettled_without_false_close():
    events = [
        _event(1, "round_started", {"round_type": "interactive"}),
        _event(2, "runtime_process_interrupted", {
            "fatal_reasons": ["runtime_process_interrupted"],
        }),
        _event(3, "round_unsettled", {
            "status": "unsettled",
            "fatal_reasons": ["runtime_process_interrupted"],
        }),
    ]

    state = build_live_state(events)
    lifecycle = state["round_lifecycle"]

    assert lifecycle["state"] == "unsettled"
    assert lifecycle["settlement_status"] == "unsettled"
    assert lifecycle["fatal_reasons"] == ["runtime_process_interrupted"]
    assert "round_closed" not in lifecycle["event_indexes"]
    assert any(
        card["event_type"] == "runtime_process_interrupted"
        for card in state["conversation"]
    )


def test_spec427_latest_frame_can_read_active_layers(tmp_path):
    from data.audit_store import AuditStore

    context = tmp_path / "context"
    audit = AuditStore(reaction_dir=str(context / "reaction"))
    audit.write_audit("reaction", {
        "permanent": "live permanent",
        "periodic": "live periodic",
        "lately": "live lately",
        "high_freq": "live high freq",
        "now": "live now truth",
        "statusbar": "live statusbar",
        "popup": "live popup",
        "full_system": "live full",
    })
    audit.write_call_layers(
        "reaction",
        call={"step": "reaction", "channel": "reaction.loop"},
        provider={"provider": "openai_chat", "model": "unit"},
        endpoint={"tier": "primary", "url": "https://example.invalid"},
        tool_header={"tool_mode": "free", "tools": []},
        generation_config={"temperature": 0.7},
    )
    events = [
        _event(1, "step_input_snapshot", {
            "layers_snapshot": _layers_snapshot(**{"50_now": "snapshot now"}),
        }, phase="reaction"),
    ]

    state = build_live_state(
        events,
        live_context_root=str(context),
        use_live_layers=True,
    )

    by_id = {pane["id"]: pane for pane in state["context_panes"]}
    assert by_id["50_now"]["content_md"] == "live now truth"
    assert state["call_frames"][0]["layer_source"] == "live_layers"


def test_spec427_legacy_without_layers_snapshot_is_marked_historical():
    events = [
        _event(1, "step_input_snapshot", {
            "messages": [
                {"role": "system", "content": "NOW: legacy now"},
            ],
        }, phase="reaction"),
    ]

    state = build_live_state(events)

    assert [pane["id"] for pane in state["context_panes"]] == LAYER_KEYS
    assert state["call_frames"][0]["layer_source"] == "legacy_messages_fallback"
    assert state["call_frames"][0]["historical"] is True


def test_live_projection_renders_eight_context_panes_with_tool_header_first():
    events = [
        _event(1, "step_input_snapshot", {
            "messages": [
                {"role": "system", "content": "PERMANENT｜永固层\n永固纪律"},
                {"role": "system", "content": "PERIODIC｜定期层\n节律材料"},
                {"role": "system", "content": "LATELY｜最近缓存\n最近语料"},
                {"role": "system", "content": "HIGH_FREQ｜高频层\n高频指南"},
                {"role": "system", "content": "NOW｜当前缓存\n当前用户任务"},
                {"role": "system", "content": "STATUSBAR｜状态栏\nphase=reaction"},
                {"role": "system", "content": "POPUP｜弹窗层\n当前是反应步"},
            ],
        }, phase="reaction"),
        _event(2, "llm_call_started", {
            "call_channel": "reaction.loop",
            "phase": "loop",
            "request_contract_audit": {
                "step": "reaction",
                "provider": "openai_chat",
                "model": "agnes-2.0-flash",
                "tool_names": ["file_read", "reaction_finalize"],
                    "terminal_tool": "reaction_finalize",
                "tool_mode": "free",
                "tools_transmitted": True,
                "standard_tools_enabled": True,
                "prompt_cache_lane": "reaction_loop_tools",
                "prompt_cache_key": "upsp:reaction_loop_tools",
                "prompt_cache_key_applied": True,
                "prompt_cache_retention": "24h",
                "prompt_cache_profile": "gpt56_explicit_tiered",
                "prompt_cache_mode": "explicit",
                "prompt_cache_plan": {
                    "targets": ["10_permanent", "30_lately"],
                },
            },
        }, phase="reaction"),
    ]

    state = build_live_state(events)

    assert state["schema_version"] == "round_live_state.v2"
    assert [pane["id"] for pane in state["context_panes"]] == LAYER_KEYS
    panes = {pane["id"]: pane for pane in state["context_panes"]}
    tool_header = panes["01_tool_header"]["content_md"]
    tool_header_raw = panes["01_tool_header"]["content_raw"]
    assert "## 实际调用工具头" in tool_header
    assert "提供商" in tool_header
    assert "模型" in tool_header
    assert "工具模式" in tool_header
    assert "工具模式" in tool_header
    assert "tool_choice:" not in tool_header_raw
    assert "tool_choice:" not in tool_header
    assert "openai_chat" in tool_header
    assert "agnes-2.0-flash" in tool_header
    assert "file_read" in tool_header
    assert "reaction_finalize" in tool_header
    assert "仅收束通道" not in tool_header
    assert "free" in tool_header_raw
    assert "reaction_loop_tools" in tool_header
    assert "gpt56_explicit_tiered" in tool_header
    assert "explicit" in tool_header
    assert "10_permanent, 30_lately" in tool_header
    assert "永固纪律" in panes["10_permanent"]["content_md"]
    assert "phase=reaction" in panes["60_statusbar"]["content_md"]
    assert "当前是反应步" in panes["99_popup"]["content_md"]


def test_live_projection_tool_header_prefers_provider_request_envelope():
    events = [
        _event(1, "step_input_snapshot", {
            "messages": [
                {"role": "system", "content": "NOW｜当前缓存\n任务"},
                {"role": "system", "content": "POPUP｜弹窗层\n指南"},
            ],
        }, phase="reaction"),
        _event(2, "llm_call_started", {
            "provider_request_envelope": {
                "schema": "provider_request.v1",
                "call": {
                    "step": "reaction",
                    "channel": "reaction.loop",
                    "phase": "loop",
                    "iteration": None,
                    "attempt": None,
                },
                "request_contract_audit": {
                    "step": "reaction",
                    "provider": "openai_chat",
                    "model": "gemini-3-flash",
                    "tool_names": ["file_read", "reaction_finalize"],
                    "terminal_tool": "reaction_finalize",
                    "tool_mode": "free",
                    "tools_transmitted": True,
                    "standard_tools_enabled": True,
                },
                "request_body_sha256": "abc123",
                "request_body": {"model": "gemini-3-flash", "messages": []},
            },
        }, phase="reaction"),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}
    tool_header = panes["01_tool_header"]["content_md"]
    tool_header_raw = panes["01_tool_header"]["content_raw"]

    assert "reaction.loop" in tool_header
    assert "reaction.loop" in tool_header_raw
    assert "request_body_sha256" in tool_header
    assert "abc123" in tool_header_raw
    assert "gemini-3-flash" in tool_header


def test_live_projection_keeps_all_runtime_events_in_one_rolling_conversation():
    events = [
        _event(1, "round_started", {"round_type": "rhythm"}),
        _event(2, "step_input_snapshot", {
            "messages": [
                {"role": "user", "content": "请读取并内化这本书。"},
                {"role": "system", "content": "NOW｜当前缓存\n用户任务：读书"},
            ],
        }, phase="reaction"),
        _event(3, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "assistant_text", "text": "我先结算节律，再处理你的读书请求。"}
            ],
            "parse_status": "ok",
        }, phase="reaction"),
        _event(4, "llm_output_raw", {
            "tool_call_envelopes": [
                {"tool_id": "file_read", "arguments": {"path": "book.md", "line_start": 1}}
            ],
        }, phase="reaction"),
        _event(5, "step_settlement", {
            "general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "summary": "已读到第 552 行"}
            ],
            "settlement": {"calendar_day_due": "cleared"},
        }, phase="reaction"),
        _event(6, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "assistant_text", "text": "我已经完成这轮读书处理。"}
            ],
            "parse_status": "ok",
        }, phase="final_reply"),
        _event(7, "round_closed", {"status": "failed", "final_response": ""}),
    ]

    state = build_live_state(events)
    cards = state["conversation"]
    card_types = [card["type"] for card in cards]

    assert "user" in card_types
    assert "assistant-progress" in card_types
    assert "assistant-final" in card_types
    assert "tool-call" in card_types
    assert "tool-result" in card_types
    assert "runtime-parse" in card_types
    assert "settlement" in card_types
    assert "warning-error" in card_types
    assert "event_stream" not in state
    assert "event_panel" not in state
    assert "assistant_panel" not in state
    assert len({card.get("stream", "conversation") for card in cards}) == 1
    assert all("content_md" in card and "content_raw" in card for card in cards)
    cards_by_type = {card["type"]: card for card in cards}
    for card_type in ("user", "assistant-progress", "assistant-final"):
        assert cards_by_type[card_type]["collapsible"] is False
        assert cards_by_type[card_type]["default_collapsed"] is False
        assert cards_by_type[card_type]["summary"]
    for card_type in ("context-update", "runtime-parse", "tool-call", "tool-result", "settlement"):
        assert cards_by_type[card_type]["collapsible"] is True
        assert cards_by_type[card_type]["default_collapsed"] is True
        assert "#" in cards_by_type[card_type]["summary"]
    assert cards_by_type["warning-error"]["collapsible"] is True
    assert cards_by_type["warning-error"]["default_collapsed"] is False
    assert "轮次事故标记" in cards_by_type["warning-error"]["summary"]


@pytest.mark.parametrize(("raw_error", "kind", "statuses", "target"), [
    (
        'HTTP 500: {"error":{"message":"[openai-compatible-chat][502]: fetch failed (cause: ECONNREFUSED: connect ECONNREFUSED 192.168.1.12:20128)"}}',
        "connection_refused", [500, 502], "192.168.1.12:20128",
    ),
    ('HTTP 401: {"error":"bad credentials"}', "authentication", [401], None),
    ("HTTP 403: forbidden", "permission_denied", [403], None),
    ('HTTP 404: {"code":"model_not_found"}', "model_unavailable", [404], None),
    ("HTTP 404: route missing", "endpoint_not_found", [404], None),
    ("HTTP 413: context_length_exceeded", "context_too_long", [413], None),
    ("HTTP 429: rate limit exceeded", "rate_limit_or_quota", [429], None),
    ("HTTP 502: bad gateway", "upstream_unavailable", [502], None),
    ("status_code=503, Service temporarily unavailable", "upstream_unavailable", [503], None),
    ("HTTP 504: gateway did not answer", "upstream_unavailable", [504], None),
    ("getaddrinfo failed for relay.invalid", "dns_error", [], None),
    ("TLS handshake failed: CERTIFICATE_VERIFY_FAILED", "tls_error", [], None),
    ("provider_request_timeout after 30s", "timeout", [], None),
    ("provider_stream_first_chunk_timeout after 45s", "timeout", [], None),
    ("provider_stream_idle_timeout after 90s", "timeout", [], None),
    ("socket closed: ECONNRESET", "connection_interrupted", [], None),
    ("provider_response_invalid_json: JSON decode error", "invalid_response", [], None),
    ("invalid SSE frame: data was truncated", "invalid_response", [], None),
    ("HTTP 400: invalid parameter", "request_rejected", [400], None),
    ("HTTP 400: invalid JSON in request body", "request_rejected", [400], None),
    ("HTTP 400: credits must be an integer", "request_rejected", [400], None),
    ("HTTP 422: invalid payload", "request_rejected", [422], None),
    ("HTTP 500: internal failure", "service_error", [500], None),
    ("provider_call_cancelled by user", "cancelled", [], None),
    ("opaque relay failure ref=abc", "unknown", [], None),
])
def test_spec717_llm_error_adds_display_hint_without_changing_raw_error(
    raw_error, kind, statuses, target,
):
    state = build_live_state([_event(1, "llm_error", {"error": raw_error}, phase="cleanup")])
    card = next(card for card in state["conversation"] if card["event_type"] == "llm_error")

    assert card["provider_error_hint"] == {
        "kind": kind,
        "http_statuses": statuses,
        **({"target": target} if target else {}),
    }
    assert card["content_md"] == raw_error
    assert json.loads(card["content_raw"])["error"] == raw_error


def test_spec502_live_projection_renders_stream_progress_in_streaming_card():
    events = [
        _event(1, "llm_stream_first_chunk", {
            "protocol": "openai_sse",
            "first_chunk_latency_ms": 38000,
            "elapsed_ms": 38000,
            "content_chars": 2,
            "reasoning_chars": 12,
            "tool_argument_chars": 0,
            "tool_call_count": 0,
        }, phase="reaction"),
        _event(2, "llm_stream_delta", {
            "protocol": "openai_sse",
            "elapsed_ms": 41000,
            "content_chars": 18,
            "reasoning_chars": 90,
            "tool_argument_chars": 33,
            "tool_call_count": 1,
        }, phase="reaction"),
        _event(3, "llm_stream_done", {
            "protocol": "openai_sse",
            "elapsed_ms": 47000,
            "content_chars": 22,
            "reasoning_chars": 120,
            "tool_argument_chars": 41,
            "tool_call_count": 1,
            "finish_reason": "tool_calls",
        }, phase="reaction"),
    ]

    state = build_live_state(events)
    stream_cards = [
        card for card in state["conversation"]
        if card["type"] == "assistant-streaming"
    ]

    assert len(stream_cards) == 1
    assert stream_cards[0]["title"] == "AI 正在输出"
    assert "正在生成工具调用" in stream_cards[0]["content_md"]
    assert "首字 38.0s" in stream_cards[0]["content_md"]
    assert "工具参数 41 字" in stream_cards[0]["content_md"]
    assert "工具调用 1" in stream_cards[0]["content_md"]
    assert "完成 tool_calls" in stream_cards[0]["content_md"]


def test_spec539_live_cards_and_context_blocks_keep_provenance():
    events = [
        _event(1, "step_input_snapshot", {
            "layers_snapshot": _layers_snapshot(**{
                "50_now": [
                    {
                        "kind": "tool_fact",
                        "content": "【本轮工具事实】\nfile_read 已完成。",
                        "source_block_id": "R000614-system-0007",
                        "step": "reaction",
                        "iter": 3,
                    }
                ],
            }),
        }, phase="reaction", iteration=3),
        _event(2, "llm_call_started", {
            "call_channel": "reaction.loop",
            "request_contract_audit": {
                "step": "reaction",
                "provider": "openai_chat",
                "model": "unit",
                "tool_names": ["file_read"],
                "tool_mode": "auto",
            },
        }, phase="reaction", iteration=3),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}
    block = panes["50_now"]["content_blocks"][0]
    runtime_card = [
        card for card in state["conversation"]
        if card["event_type"] == "llm_call_started"
    ][0]

    assert block["source_block_id"] == "R000614-system-0007"
    assert block["provenance"]["kind"] == "tool_fact"
    assert block["provenance"]["step"] == "reaction"
    assert block["provenance"]["iter"] == 3
    assert runtime_card["event_id"] == "R000614-000002"
    assert runtime_card["provenance"]["source_event_id"] == "R000614-000002"
    assert runtime_card["provenance"]["renderer"] == "round_live_viewer._cards_for_event"


def test_spec504_live_projection_coalesces_stream_deltas_into_assistant_card():
    events = [
        _event(1, "llm_stream_first_chunk", {
            "protocol": "openai_sse",
            "first_chunk_latency_ms": 1700,
            "elapsed_ms": 1700,
            "content_chars": 2,
            "reasoning_chars": 4,
            "tool_argument_chars": 0,
            "tool_call_count": 0,
            "content_delta": "he",
            "reasoning_delta": "想想",
        }, phase="reaction", iteration=2),
        _event(2, "llm_stream_delta", {
            "protocol": "openai_sse",
            "elapsed_ms": 2200,
            "content_chars": 5,
            "reasoning_chars": 8,
            "tool_argument_chars": 0,
            "tool_call_count": 0,
            "content_delta": "llo",
            "reasoning_delta": "继续",
        }, phase="reaction", iteration=2),
        _event(3, "llm_stream_done", {
            "protocol": "openai_sse",
            "elapsed_ms": 2600,
            "content_chars": 5,
            "reasoning_chars": 8,
            "tool_argument_chars": 0,
            "tool_call_count": 0,
            "finish_reason": "stop",
        }, phase="reaction", iteration=2),
    ]

    state = build_live_state(events)
    stream_cards = [
        card for card in state["conversation"]
        if card["type"] == "assistant-streaming"
    ]

    assert len(stream_cards) == 1
    assert stream_cards[0]["title"] == "AI 正在输出"
    assert stream_cards[0]["content_raw"] == "hello"
    assert "hello" in stream_cards[0]["content_md"]
    assert "流式状态" in stream_cards[0]["content_md"]
    assert not [
        card for card in state["conversation"]
        if card["title"] == "LLM 流式进展"
    ]


def test_spec535_live_projection_hides_dsml_tool_call_content_from_chat_card():
    dsml = (
        "好的，我会先提交任务清单。\n\n"
        "<｜DSML｜tool_calls><｜DSML｜invoke name=\"guide_submit\">"
        "{\"guide_id\":\"task_bootstrap\",\"submissions\":[{\"item_id\":\"build_initial_task_guide\"}]}"
    )
    events = [
        _event(1, "llm_stream_first_chunk", {
            "protocol": "openai_sse",
            "elapsed_ms": 1000,
            "content_delta": dsml[:24],
        }, phase="reaction", iteration=1),
        _event(2, "llm_stream_delta", {
            "protocol": "openai_sse",
            "elapsed_ms": 1400,
            "content_delta": dsml[24:],
        }, phase="reaction", iteration=1),
        _event(3, "llm_stream_done", {
            "protocol": "openai_sse",
            "elapsed_ms": 1800,
            "finish_reason": "tool_calls",
        }, phase="reaction", iteration=1),
    ]

    state = build_live_state(events)
    stream_cards = [
        card for card in state["conversation"]
        if card["type"] == "assistant-streaming"
    ]

    assert len(stream_cards) == 1
    card = stream_cards[0]
    assert "好的，我会先提交任务清单。" in card["content_md"]
    assert "<｜DSML｜" not in card["content_md"]
    assert "guide_submit" not in card["content_md"]
    assert "<｜DSML｜" not in card["content_raw"]
    assert "guide_submit" not in card["content_raw"]
    assert "工具调用" in card["content_md"] or "宸ュ叿璋冪敤" in card["content_md"]


def test_spec504_parsed_assistant_text_replaces_completed_streaming_card():
    events = [
        _event(1, "llm_stream_first_chunk", {
            "protocol": "openai_sse",
            "elapsed_ms": 1000,
            "content_chars": 5,
            "content_delta": "完成了。",
        }, phase="reaction", iteration=1),
        _event(2, "llm_stream_done", {
            "protocol": "openai_sse",
            "elapsed_ms": 1200,
            "content_chars": 5,
            "finish_reason": "stop",
        }, phase="reaction", iteration=1),
        _event(3, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "assistant_text", "text": "完成了。"}
            ],
            "parse_status": "ok",
        }, phase="reaction", iteration=1),
    ]

    state = build_live_state(events)

    assert not [
        card for card in state["conversation"]
        if card["type"] == "assistant-streaming"
    ]
    assistant_cards = [
        card for card in state["conversation"]
        if card["type"] == "assistant-progress"
    ]
    assert len(assistant_cards) == 1
    assert assistant_cards[0]["content_raw"] == "完成了。"


def test_spec710_latest_stream_attempt_replaces_failed_attempt_without_concatenation():
    events = [
        _event(1, "llm_stream_first_chunk", {
            "stream_id": "attempt-a",
            "protocol": "openai_chat",
            "content_delta": "旧尝试",
        }, phase="reaction", iteration=1),
        _event(2, "llm_stream_error", {
            "stream_id": "attempt-a",
            "protocol": "openai_chat",
            "reason": "provider_stream_interrupted",
        }, phase="reaction", iteration=1),
        _event(3, "llm_stream_first_chunk", {
            "stream_id": "attempt-b",
            "protocol": "openai_chat",
            "content_delta": "新尝试",
        }, phase="reaction", iteration=1),
    ]

    stream_cards = [
        card for card in build_live_state(events)["conversation"]
        if card["type"] == "assistant-streaming"
    ]

    assert len(stream_cards) == 1
    assert stream_cards[0]["stream_id"] == "attempt-b"
    assert stream_cards[0]["content_raw"] == "新尝试"


def test_spec710_empty_failed_retries_keep_last_real_partial_answer():
    events = [
        _event(1, "llm_stream_first_chunk", {
            "stream_id": "attempt-a",
            "protocol": "openai_chat",
            "content_delta": "real partial",
        }, phase="reaction", iteration=1),
        _event(2, "llm_stream_error", {
            "stream_id": "attempt-a",
            "protocol": "openai_chat",
            "reason": "provider_stream_interrupted",
        }, phase="reaction", iteration=1),
        _event(3, "llm_stream_error", {
            "stream_id": "attempt-b",
            "protocol": "openai_chat",
            "reason": "provider_stream_first_chunk_timeout",
        }, phase="reaction", iteration=1),
        _event(4, "llm_stream_error", {
            "stream_id": "attempt-c",
            "protocol": "openai_chat",
            "reason": "provider_stream_first_chunk_timeout",
        }, phase="reaction", iteration=1),
    ]

    stream_cards = [
        card for card in build_live_state(events)["conversation"]
        if card["type"] == "assistant-streaming"
    ]

    assert len(stream_cards) == 1
    assert stream_cards[0]["stream_id"] == "attempt-a"
    assert stream_cards[0]["content_raw"] == "real partial"
    assert stream_cards[0]["stream_state"] == "interrupted"


def test_spec710_parsed_frame_replaces_stream_even_when_text_is_normalized_differently():
    events = [
        _event(1, "llm_stream_first_chunk", {
            "stream_id": "attempt-a",
            "protocol": "openai_responses",
            "content_delta": "**正在完成",
        }, phase="final_reply", iteration=1),
        _event(2, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "final_reply.text", "text": "已经完成。"}
            ],
            "parse_status": "ok",
        }, phase="final_reply", iteration=1),
    ]

    state = build_live_state(events)

    assert not [
        card for card in state["conversation"]
        if card["type"] == "assistant-streaming"
    ]
    assert [
        card for card in state["conversation"]
        if card["type"] == "assistant-final"
        and card["content_raw"] == "已经完成。"
    ]


def test_spec710_user_stop_keeps_partial_stream_as_stopped_not_final():
    events = [
        _event(1, "llm_stream_first_chunk", {
            "stream_id": "attempt-stop",
            "protocol": "anthropic_messages",
            "content_delta": "已经收到的半截正文",
        }, phase="reaction", iteration=1),
        _event(2, "round_settled", {
            "status": "degraded",
            "degraded_reasons": ["user_stopped"],
        }, phase="cleanup"),
    ]

    state = build_live_state(events)
    stream = next(
        card for card in state["conversation"]
        if card["type"] == "assistant-streaming"
    )

    assert stream["content_raw"] == "已经收到的半截正文"
    assert stream["stream_state"] == "stopped"
    assert not [
        card for card in state["conversation"]
        if card["type"] == "assistant-final"
    ]


def test_live_projection_deduplicates_raw_assistant_text_when_parsed_envelope_repeats_it():
    events = [
        _event(1, "llm_output_raw", {
            "response": "Continue reading from line 553.",
        }, phase="reaction", iteration=2),
        _event(2, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "assistant_text", "text": "Continue reading from line 553."}
            ],
            "parse_status": "ok",
        }, phase="reaction", iteration=2),
    ]

    state = build_live_state(events)
    assistant_cards = [
        card for card in state["conversation"]
        if card["type"] == "assistant-progress"
    ]

    assert [card["content_md"] for card in assistant_cards] == [
        "Continue reading from line 553."
    ]
    assert assistant_cards[0]["event_type"] == "llm_output_parsed"


def test_live_projection_deduplicates_round_closed_final_response_already_shown():
    final_text = "本轮最终回复已经生成。"
    events = [
        _event(1, "llm_output_raw", {
            "response": final_text,
        }, phase="final_reply", iteration=1),
        _event(2, "round_closed", {
            "status": "closed",
            "final_response": final_text,
        }),
    ]

    state = build_live_state(events)
    assistant_cards = [
        card for card in state["conversation"]
        if card["type"] == "assistant-final"
    ]

    assert [card["content_md"] for card in assistant_cards] == [final_text]
    assert assistant_cards[0]["event_type"] == "llm_output_raw"


def test_spec417_live_projection_prefers_parsed_final_reply_over_raw_and_round_closed_duplicates():
    final_text = "本轮最终回复已经生成。"
    events = [
        _event(1, "llm_output_raw", {
            "response": final_text,
        }, phase="final_reply", iteration=1),
        _event(2, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "assistant_text", "text": final_text}
            ],
            "parse_status": "ok",
        }, phase="final_reply", iteration=1),
        _event(3, "round_closed", {
            "status": "closed",
            "final_response": final_text,
        }),
    ]

    state = build_live_state(events)
    assistant_cards = [
        card for card in state["conversation"]
        if card["type"] == "assistant-final"
    ]

    assert [card["content_md"] for card in assistant_cards] == [final_text]
    assert assistant_cards[0]["event_type"] == "llm_output_parsed"


def test_natural_final_reply_stays_single_when_cleanup_later_closes_round():
    final_text = "这条自然最终回复第一次出现后应当保留。"
    events = [
        _event(1, "llm_output_raw", {
            "response": final_text,
        }, phase="reaction", iteration=1),
        _event(2, "llm_output_parsed", {
            "message_envelopes": [{
                "channel": "assistant_text",
                "phase": "loop",
                "terminal_decision": "finish",
                "terminal_text_candidate": True,
                "text": final_text,
            }],
            "natural_final_reply_candidate": final_text,
            "parse_status": "ok",
        }, phase="reaction", iteration=1),
        _event(3, "cleanup_obligation_created", {
            "status": "pending",
        }),
        _event(4, "round_settled", {
            "status": "settled",
        }),
        _event(5, "round_closed", {
            "status": "closed",
            "final_response": final_text,
            "final_response_source": "reaction.natural_final_reply",
        }),
    ]

    pending_state = build_live_state(events[:2])
    pending_assistant = [
        card for card in pending_state["conversation"]
        if card["type"] in {"assistant-progress", "assistant-final"}
    ]
    state = build_live_state(events)
    assistant_cards = [
        card for card in state["conversation"]
        if card["type"] in {"assistant-progress", "assistant-final"}
    ]

    assert [card["type"] for card in pending_assistant] == ["assistant-progress"]
    assert [card["content_md"] for card in assistant_cards] == [final_text]
    assert assistant_cards[0]["type"] == "assistant-final"
    assert assistant_cards[0]["event_type"] == "llm_output_parsed"


def test_live_projection_deduplicates_repeated_user_input_snapshots():
    events = [
        _event(1, "step_input_snapshot", {
            "messages": [{"role": "user", "content": "Read this book."}],
        }, phase="setup", iteration=1),
        _event(2, "step_input_snapshot", {
            "messages": [{"role": "user", "content": "Read this book."}],
        }, phase="reaction", iteration=1),
        _event(3, "step_input_snapshot", {
            "messages": [{"role": "user", "content": "Read this book."}],
        }, phase="final_reply", iteration=1),
    ]

    state = build_live_state(events)
    user_cards = [card for card in state["conversation"] if card["type"] == "user"]

    assert [card["content_md"] for card in user_cards] == ["Read this book."]


def test_live_projection_recovers_now_pane_from_real_snapshot_without_explicit_now_marker():
    events = [
        _event(1, "step_input_snapshot", {
            "manifest": {
                "layers": {
                    "high_freq": {"chars": 20},
                    "now": {"chars": 300},
                    "statusbar": {"chars": 20},
                    "popup": {"chars": 20},
                },
            },
            "messages": [
                {
                    "role": "system",
                    "content": "<!-- 高频层 -->\n## 容器索引\n- [CHR] 编年史",
                },
                {
                    "role": "system",
                    "content": (
                        "【历史工具事实摘要，来自第 617 轮】\n"
                        "历史读取游标：R000617 轮读取 book.md，范围第 1 行到第 163 行。"
                    ),
                },
                {"role": "system", "content": "【第 617 轮已闭合】"},
                {
                    "role": "user",
                    "content": (
                        "【本轮交互】\n"
                        "我是Codex。\n"
                        "请读取并内化这本书。\n"
                        "path=D:\\AI_WORKSPACE\\base\\book\\共格主体论_V5_6.1.md"
                    ),
                },
                {
                    "role": "assistant",
                    "content": (
                        "【轮中进展记录】\n"
                        "# 共格主体论 V5 6.1 内化进度报告"
                    ),
                },
                {
                    "role": "user",
                    "content": "【Runtime 调用占位】\n请根据上下文继续本次调用。",
                },
                {
                    "role": "system",
                    "content": "<!-- STATUSBAR（状态栏层） -->\n## STATUSBAR\n当前轮：R000617",
                },
                {
                    "role": "system",
                    "content": "<!-- POPUP（弹窗层，messages绝对末位） -->\n## GUIDE｜指南\n当前是起手步。",
                },
            ],
        }, phase="setup", iteration=1),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}

    assert panes["50_now"]["chars"] > 0
    assert "【本轮交互】" in panes["50_now"]["content_md"]
    assert "【轮中进展记录】" in panes["50_now"]["content_md"]
    assert "【Runtime 调用占位】" in panes["50_now"]["content_md"]


def test_live_projection_recovers_unmarked_lately_and_now_by_message_position():
    events = [
        _event(1, "step_input_snapshot", {
            "manifest": {
                "layers": {
                    "permanent": {"chars": 20},
                    "lately": {"chars": 80},
                    "high_freq": {"chars": 20},
                    "now": {"chars": 6000},
                    "statusbar": {"chars": 20},
                    "popup": {"chars": 20},
                },
            },
            "messages": [
                {"role": "system", "content": "<!-- 永固层 -->\n永固纪律"},
                {
                    "role": "system",
                    "content": "【历史工具事实摘要，来自第 617 轮】\n历史读取游标。",
                },
                {"role": "system", "content": "【第 617 轮已闭合】"},
                {
                    "role": "user",
                    "content": "【历史交互，来自第 617 轮】\n请读取并内化这本书。",
                },
                {
                    "role": "system",
                    "content": "【历史回复，来自第 617 轮】\n历史回复正文。",
                },
                {"role": "system", "content": "<!-- 高频层 -->\n## 容器索引"},
                {
                    "role": "system",
                    "content": "【本轮工具事实】\n本轮已经成功读取文件。",
                },
                {
                    "role": "system",
                    "content": "【本轮资料】\n" + ("正文材料。" * 1000),
                },
                {
                    "role": "assistant",
                    "content": "【轮中进展记录】\n正在处理。",
                },
                {
                    "role": "system",
                    "content": "【最终回复记录】\n已完成本轮回复。",
                },
                {"role": "system", "content": "<!-- STATUSBAR（状态栏层） -->\n## STATUSBAR"},
                {"role": "system", "content": "<!-- POPUP（弹窗层，messages绝对末位） -->\n## GUIDE"},
            ],
        }, phase="reaction", iteration=10),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}

    assert panes["30_lately"]["chars"] > 0
    assert "【历史工具事实摘要" in panes["30_lately"]["content_md"]
    assert "【历史交互" in panes["30_lately"]["content_md"]
    assert "【历史回复" in panes["30_lately"]["content_md"]
    assert panes["50_now"]["chars"] > 5000
    assert "【本轮工具事实】" in panes["50_now"]["content_md"]
    assert "正文材料。" in panes["50_now"]["content_md"]
    assert "【轮中进展记录】" in panes["50_now"]["content_md"]
    assert "【最终回复记录】" in panes["50_now"]["content_md"]
    assert "STATUSBAR" not in panes["50_now"]["content_md"]
    assert "GUIDE｜指南" not in panes["50_now"]["content_md"]


def test_spec507_live_projection_exposes_corpus_blocks_for_pane_cards():
    events = [
        _event(1, "step_input_snapshot", {
            "layers_snapshot": _layers_snapshot(
                **{
                    "50_now": (
                        "【本轮工具事实】\n"
                        "file_read 已读取 agent_eval_tasks.md。\n\n"
                        "【本轮资料】\n"
                        "任务文件包含 12 项 agent 日常能力测试。\n\n"
                        "【轮中进展记录】\n"
                        "模型正在创建任务清单。"
                    ),
                }
            ),
        }, phase="reaction", iteration=2),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}
    blocks = panes["50_now"]["content_blocks"]

    assert panes["01_tool_header"]["content_blocks"] == []
    assert [block["title"] for block in blocks] == [
        "本轮工具事实",
        "本轮资料",
        "轮中进展记录",
    ]
    assert [block["index"] for block in blocks] == [1, 2, 3]
    assert blocks[0]["block_id"] == "50_now:B01"
    assert blocks[0]["tone"] != blocks[1]["tone"]
    assert "file_read 已读取" in blocks[0]["content_md"]
    assert "12 项 agent 日常能力测试" in blocks[1]["content_md"]
    assert "模型正在创建任务清单" in blocks[2]["content_md"]


def test_spec508_live_projection_uses_layer_json_entries_for_cards():
    snapshot = _layers_snapshot()
    for layer in snapshot["layers"]:
        if layer["layer_key"] != "50_now":
            continue
        layer["content"] = [
            {
                "role": "user",
                "kind": "runtime_call_request",
                "content": "【Runtime 调用占位】\n请根据上下文继续本次调用。",
            },
            {
                "role": "user",
                "kind": "interaction",
                "content": "【本轮交互】\n我是 Codex。请读取任务文件。",
            },
            {
                "role": "system",
                "kind": "setup_fact",
                "content": "【本轮起手事实】\n起手确认存在任务型交互。",
            },
        ]
        layer["content_markdown"] = (
            "【Runtime 调用占位】\n"
            "请根据上下文继续本次调用。\n"
            "我是 Codex。请读取任务文件。\n"
            "起手确认存在任务型交互。"
        )
    events = [
        _event(1, "step_input_snapshot", {
            "layers_snapshot": snapshot,
        }, phase="reaction", iteration=1),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}
    blocks = panes["50_now"]["content_blocks"]

    assert [block["title"] for block in blocks] == [
        "Runtime 调用占位",
        "本轮交互",
        "本轮起手事实",
    ]
    assert "我是 Codex" in blocks[1]["content_md"]
    assert "起手确认" in blocks[2]["content_md"]


def test_live_projection_does_not_treat_popup_word_in_tool_fact_as_popup_marker():
    events = [
        _event(1, "step_input_snapshot", {
            "manifest": {
                "layers": {
                    "permanent": {"chars": 20},
                    "high_freq": {"chars": 20},
                    "now": {"chars": 220},
                    "statusbar": {"chars": 20},
                    "popup": {"chars": 20},
                },
            },
            "messages": [
                {"role": "system", "content": "<!-- 永固层 -->\n永固纪律"},
                {"role": "system", "content": "<!-- 高频层 -->\n## 内容窗口"},
                {
                    "role": "system",
                    "content": (
                        "【本轮工具事实】\n"
                        "处理结果：error。\n"
                        "下一步：按 POPUP 中相同 tool_id/call_id 的纠偏路线重交。"
                    ),
                },
                {"role": "system", "content": "<!-- STATUSBAR（状态栏层） -->\n## STATUSBAR"},
                {"role": "system", "content": "<!-- POPUP（弹窗层，messages绝对末位） -->\n## GUIDE｜指南\n真正弹窗。"},
            ],
        }, phase="reaction", iteration=3),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}

    assert "【本轮工具事实】" in panes["50_now"]["content_md"]
    assert "POPUP 中相同 tool_id/call_id" in panes["50_now"]["content_md"]
    assert "【本轮工具事实】" not in panes["99_popup"]["content_md"]
    assert "真正弹窗" in panes["99_popup"]["content_md"]


def test_spec417_live_projection_does_not_guess_popup_layer_from_plain_popup_label():
    events = [
        _event(1, "step_input_snapshot", {
            "manifest": {
                "layers": {
                    "now": {"chars": 220},
                    "popup": {"chars": 20},
                },
            },
            "messages": [
                {
                    "role": "system",
                    "content": (
                        "popup 1113 字\n"
                        "【本轮工具事实】\n"
                        "处理结果：error。\n"
                        "下一步：按 POPUP 中相同 tool_id/call_id 的纠偏路线重交。"
                    ),
                },
                {"role": "system", "content": "<!-- POPUP（弹窗层，messages绝对末位） -->\n## GUIDE｜指南\n真正弹窗。"},
            ],
        }, phase="reaction", iteration=3),
    ]

    state = build_live_state(events)
    panes = {pane["id"]: pane for pane in state["context_panes"]}

    assert "【本轮工具事实】" in panes["50_now"]["content_md"]
    assert "【本轮工具事实】" not in panes["99_popup"]["content_md"]
    assert "真正弹窗" in panes["99_popup"]["content_md"]


def test_live_projection_builds_call_frames_and_marks_latest_frame():
    events = [
        _event(1, "round_started", {"round_type": "interactive"}),
        _event(2, "step_input_snapshot", {
            "messages": [
                {"role": "system", "content": "POPUP｜弹窗层\n当前是起手步"},
            ],
        }, phase="setup", iteration=1),
        _event(3, "llm_call_started", {
            "call_channel": "setup",
            "request_contract_audit": {
                "step": "setup",
                "provider": "openai_chat",
                "model": "agnes-2.0-flash",
                "tool_names": ["setup_finalize"],
                    "terminal_tool": "setup_finalize",
                "tool_mode": "required",
                "tools_transmitted": True,
            },
        }, phase="setup", iteration=1),
        _event(4, "step_input_snapshot", {
            "messages": [
                {"role": "system", "content": "NOW｜当前缓存\n用户任务：读书"},
                {"role": "user", "content": "Read this book, please. <b>do not translate me</b>"},
            ],
        }, phase="reaction", iteration=1),
        _event(5, "llm_call_started", {
            "call_channel": "reaction.loop",
            "request_contract_audit": {
                "step": "reaction",
                "provider": "openai_chat",
                "model": "agnes-2.0-flash",
                "tool_names": ["file_read", "reaction_finalize"],
                    "terminal_tool": "reaction_finalize",
                "tool_mode": "free",
                "tools_transmitted": True,
            },
        }, phase="reaction", iteration=1),
        _event(6, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "assistant_text", "text": "继续读取。"}
            ],
            "parse_status": "ok",
        }, phase="reaction", iteration=1),
        _event(7, "step_input_snapshot", {
            "messages": [
                {"role": "system", "content": "POPUP｜弹窗层\n当前是最终回复阶段"},
            ],
        }, phase="final_reply", iteration=1),
        _event(8, "llm_output_parsed", {
            "message_envelopes": [
                {"channel": "assistant_text", "text": "完成。"}
            ],
            "parse_status": "ok",
        }, phase="final_reply", iteration=1),
    ]

    state = build_live_state(events)

    frames = state["call_frames"]
    assert [frame["call_channel"] for frame in frames] == [
        "setup",
        "reaction.loop",
        "final_reply",
    ]
    assert state["latest_frame_id"] == frames[-1]["frame_id"]
    assert frames[1]["label"] == "R614 反应循环 1"
    reaction_panes = {pane["id"]: pane for pane in frames[1]["context_panes"]}
    assert "用户任务：读书" in reaction_panes["50_now"]["content_md"]
    assert frames[1]["context_panes"][0]["id"] == "00_call_header"
    assert any(card["frame_id"] == frames[1]["frame_id"] for card in state["conversation"])
    user_card = next(card for card in state["conversation"] if card["type"] == "user")
    assert user_card["content_md"] == "Read this book, please. <b>do not translate me</b>"


def test_live_projection_translates_structured_cards_to_chinese_markdown_without_translating_body():
    events = [
        _event(1, "llm_output_raw", {
            "tool_call_envelopes": [
                {
                    "tool_id": "file_read",
                    "arguments": {"path": "book.md", "line_start": 553},
                }
            ],
        }, phase="reaction", iteration=2),
        _event(2, "step_settlement", {
            "general_tool_results": [
                {
                    "tool_id": "file_read",
                    "status": "ok",
                    "summary": "已读到第 700 行",
                }
            ],
            "settlement": {"calendar_day_due": "cleared"},
        }, phase="reaction", iteration=2),
    ]

    state = build_live_state(events)
    tool_call = next(card for card in state["conversation"] if card["type"] == "tool-call")
    tool_result = next(card for card in state["conversation"] if card["type"] == "tool-result")

    assert "## 工具调用" in tool_call["content_md"]
    assert "工具 ID" in tool_call["content_md"]
    assert "参数" in tool_call["content_md"]
    assert "tool_id" in tool_call["content_raw"]
    assert "tool_id" not in tool_call["content_md"]
    assert "## 工具结果" in tool_result["content_md"]
    assert "状态" in tool_result["content_md"]
    assert "已读到第 700 行" in tool_result["content_md"]


def test_live_input_box_is_present_but_does_not_send_provider():
    state = build_live_state([])

    assert state["input_box"]["label"] == "用户输入"
    assert state["input_box"]["editable"] is True
    assert state["input_box"]["send_live_provider"] is False


def test_events_after_returns_incremental_events_and_state():
    events = [
        _event(1, "round_started", {"round_type": "interactive"}),
        _event(2, "round_closed", {"status": "closed", "final_response": "完成"}),
    ]

    payload = events_after(events, after=1)

    assert payload["events"] == [events[1]]
    assert payload["last_event_index"] == 2
    assert payload["state"]["last_event_index"] == 2


def test_events_after_can_return_lightweight_no_state_payload_when_no_new_events():
    events = [
        _event(1, "round_started", {"round_type": "interactive"}),
        _event(2, "round_closed", {"status": "closed", "final_response": "完成"}),
    ]

    payload = events_after(events, after=2, include_state=False)

    assert payload["events"] == []
    assert payload["last_event_index"] == 2
    assert payload["state"] is None


def test_latest_event_index_reads_round_jsonl_last_event(tmp_path):
    round_dir = tmp_path / "round"
    round_dir.mkdir()
    path = round_dir / "round_617.jsonl"
    path.write_text(
        "\n".join([
            '{"event_index": 1, "event_type": "round_started"}',
            '',
            '{"event_index": 23, "event_type": "llm_call_started"}',
        ]),
        encoding="utf-8",
    )

    assert latest_event_index(str(round_dir), 617) == 23


def test_round_live_html_declares_local_markdown_it_and_single_conversation_stream():
    html_path = Path(__file__).resolve().parents[1] / "audit" / "round_live.html"
    html = html_path.read_text(encoding="utf-8")

    assert 'src="vendor/markdown-it.min.js"' in html
    assert "html: false" in html
    assert "disableImageRule" in html
    assert 'id="roundStrip"' in html
    assert 'id="frameStrip"' in html
    assert 'class="live-main"' in html
    assert 'id="conversation"' in html
    assert 'id="eventCollapseToggle"' in html
    assert "grid-template-columns: minmax(0, 2fr) minmax(320px, 1fr);" in html
    assert "grid-template-columns: repeat(12, minmax(0, 1fr));" in html
    assert ".context-grid > .pane:nth-child(-n + 4)" in html
    assert ".context-grid > .pane:nth-child(n + 5):nth-child(-n + 7)" in html
    assert ".context-grid > .pane:nth-child(n + 8):nth-child(-n + 10)" in html
    assert "manualCollapseState" in html
    assert "default_collapsed" in html
    assert "collapsible" in html
    assert ".corpus-block-card" in html
    assert "renderPaneContent" in html
    assert "scrollConversationToBottom" in html
    assert "const contentChanged = renderPaneContent(body, pane);" in html
    assert "if (!contentChanged) {" in html
    assert "const userMovedScroll" in html
    assert "if (userMovedScroll) {" in html
    assert "let changed = false;" in html
    assert "if (pinnedToBottom && liveMode && changed)" in html
    assert 'scrollConversationToBottom({ smooth: false })' in html
    assert 'behavior: smooth ? "smooth" : "auto"' in html
    assert "scroll-behavior: smooth" not in html
    assert "refreshPaused" in html
    assert "暂停刷新" in html
    assert "if (refreshPaused)" in html
    assert "if (!payload.state)" in html
    assert "eventPanel" not in html
    assert "assistantPanel" not in html
