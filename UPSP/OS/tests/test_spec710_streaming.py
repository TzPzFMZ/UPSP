import json
import threading
import time

import pytest

from engines.executor import APIExecutor
from errors import APIBridgeError
from logic.native_tool_calls import extract_tool_call_envelopes


class _Config:
    def get_stream_content_overrun_chars(self):
        return 100_000


class _Response:
    headers = {"Content-Type": "text/event-stream"}

    def __init__(self, events):
        self._lines = [
            f"data: {event if isinstance(event, str) else json.dumps(event, ensure_ascii=False)}\n".encode("utf-8")
            for event in events
        ]

    def __iter__(self):
        return iter(self._lines)


def _read(provider, events):
    executor = object.__new__(APIExecutor)
    executor.cfg = _Config()
    executor._monotonic = lambda: 1.0
    executor._stream_id = f"stream-{provider}"
    emitted = []
    executor._stream_event_sink = (
        lambda event_type, payload: emitted.append((event_type, payload))
    )
    response = executor._read_provider_sse_response(
        _Response(events),
        f"https://example.test/v1/{'responses' if provider == 'openai_responses' else 'messages'}",
        provider=provider,
        first_chunk_timeout=180,
        idle_timeout=180,
    )
    return response, emitted


def test_spec710_openai_responses_streams_text_refusal_and_tool_arguments():
    response, emitted = _read("openai_responses", [
        {"type": "response.created", "response": {"id": "resp_1", "model": "unit"}},
        {
            "type": "response.output_item.added",
            "output_index": 0,
            "item": {
                "id": "call_1",
                "type": "function_call",
                "call_id": "call_1",
                "name": "file_read",
                "arguments": "",
            },
        },
        {
            "type": "response.function_call_arguments.delta",
            "output_index": 0,
            "item_id": "call_1",
            "delta": '{"path":"',
        },
        {
            "type": "response.function_call_arguments.delta",
            "output_index": 0,
            "item_id": "call_1",
            "delta": 'README.md"}',
        },
        {"type": "response.output_text.delta", "delta": "正在读取。"},
        {"type": "response.refusal.delta", "delta": "不能继续。"},
        {
            "type": "response.completed",
            "response": {
                "id": "resp_1",
                "status": "completed",
                "usage": {"input_tokens": 3, "output_tokens": 5},
            },
        },
    ])

    assert response["output_text"] == "正在读取。不能继续。"
    assert response["usage"]["output_tokens"] == 5
    envelopes = extract_tool_call_envelopes(
        response,
        provider="openai_responses",
        endpoint="unit",
    )
    assert envelopes[0]["arguments"] == {"path": "README.md"}
    done = next(payload for event, payload in emitted if event == "llm_stream_done")
    assert done["content_delta"] == "正在读取。不能继续。"
    assert done["protocol"] == "openai_responses"
    assert done["stream_id"] == "stream-openai_responses"
    assert done["attempt_status"] == "completed"


def test_spec710_openai_chat_refusal_is_visible_before_completion():
    response, emitted = _read("openai_chat", [
        {"choices": [{"delta": {"refusal": "request refused"}}]},
        {"choices": [{"delta": {}, "finish_reason": "stop"}]},
        "[DONE]",
    ])

    assert response["choices"][0]["message"]["refusal"] == "request refused"
    first = next(payload for event, payload in emitted if event == "llm_stream_first_chunk")
    assert first["content_delta"] == "request refused"
    assert first["content_chars"] == len("request refused")


@pytest.mark.parametrize(
    ("provider", "events", "expected_channels"),
    [
        (
            "openai_chat",
            [
                {"choices": [{"index": 0, "delta": {"reasoning_content": "先想"}}]},
                {"choices": [{"index": 0, "delta": {"content": "进展"}}]},
                {"choices": [{"index": 0, "delta": {"reasoning_content": "再想"}}]},
                {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                "[DONE]",
            ],
            ["reasoning", "content", "reasoning"],
        ),
        (
            "openai_responses",
            [
                {"type": "response.reasoning_text.delta", "output_index": 0, "item_id": "r1", "delta": "先想"},
                {"type": "response.output_text.delta", "output_index": 1, "item_id": "m1", "delta": "进展"},
                {"type": "response.reasoning_text.delta", "output_index": 2, "item_id": "r2", "delta": "再想"},
                {"type": "response.completed", "response": {"status": "completed"}},
            ],
            ["reasoning", "content", "reasoning"],
        ),
        (
            "anthropic_messages",
            [
                {"type": "message_start", "message": {"id": "m1", "content": []}},
                {"type": "content_block_start", "index": 0, "content_block": {"type": "thinking", "thinking": ""}},
                {"type": "content_block_delta", "index": 0, "delta": {"type": "thinking_delta", "thinking": "先想"}},
                {"type": "content_block_start", "index": 1, "content_block": {"type": "text", "text": ""}},
                {"type": "content_block_delta", "index": 1, "delta": {"type": "text_delta", "text": "进展"}},
                {"type": "content_block_start", "index": 2, "content_block": {"type": "thinking", "thinking": ""}},
                {"type": "content_block_delta", "index": 2, "delta": {"type": "thinking_delta", "thinking": "再想"}},
                {"type": "message_stop"},
            ],
            ["reasoning", "content", "reasoning"],
        ),
    ],
)
def test_spec771_stream_segments_preserve_provider_channel_order(provider, events, expected_channels):
    _response, emitted = _read(provider, events)
    segments = [
        segment
        for _event_type, payload in emitted
        for segment in payload.get("stream_segments") or []
    ]

    assert [segment["channel"] for segment in segments] == expected_channels
    assert [segment["delta"] for segment in segments] == ["先想", "进展", "再想"]
    assert [segment["segment_id"] for segment in segments] == ["seg-0001", "seg-0002", "seg-0003"]
    assert all(isinstance(segment["provider_block"], dict) for segment in segments)


@pytest.mark.parametrize(
    ("provider", "events", "expected_tool"),
    [
        (
            "openai_chat",
            [
                {"choices": [{"index": 0, "delta": {"content": "工具前"}}]},
                {"choices": [{"index": 0, "delta": {"tool_calls": [{
                    "index": 0,
                    "id": "call-chat",
                    "function": {"name": "file_read", "arguments": "{}"},
                }]}}]},
                {"choices": [{"index": 0, "delta": {"content": "工具后"}}]},
                {"choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}]},
                "[DONE]",
            ],
            ("file_read", "call-chat"),
        ),
        (
            "openai_responses",
            [
                {"type": "response.output_text.delta", "output_index": 0, "item_id": "m1", "delta": "工具前"},
                {"type": "response.output_item.added", "output_index": 1, "item": {
                    "id": "item-call", "type": "function_call", "call_id": "call-responses",
                    "name": "file_read", "arguments": "{}",
                }},
                {"type": "response.output_text.delta", "output_index": 2, "item_id": "m2", "delta": "工具后"},
                {"type": "response.completed", "response": {"status": "completed"}},
            ],
            ("file_read", "call-responses"),
        ),
        (
            "anthropic_messages",
            [
                {"type": "message_start", "message": {"id": "m1", "content": []}},
                {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
                {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "工具前"}},
                {"type": "content_block_start", "index": 1, "content_block": {
                    "type": "tool_use", "id": "call-anthropic", "name": "file_read", "input": {},
                }},
                {"type": "content_block_start", "index": 2, "content_block": {"type": "text", "text": ""}},
                {"type": "content_block_delta", "index": 2, "delta": {"type": "text_delta", "text": "工具后"}},
                {"type": "message_stop"},
            ],
            ("file_read", "call-anthropic"),
        ),
    ],
)
def test_spec771_tool_boundaries_split_visible_text_without_exposing_arguments(
        provider, events, expected_tool):
    _response, emitted = _read(provider, events)
    ordered = []
    for _event_type, payload in emitted:
        ordered.extend(
            (int(item["sequence"]), "text", item["delta"])
            for item in payload.get("stream_segments") or []
        )
        ordered.extend(
            (int(item["sequence"]), "tool", (item["tool_id"], item["call_id"]))
            for item in payload.get("stream_tool_boundaries") or []
        )

    assert [item[1:] for item in sorted(ordered)] == [
        ("text", "工具前"),
        ("tool", expected_tool),
        ("text", "工具后"),
    ]
    assert "arguments" not in json.dumps(ordered, ensure_ascii=False)


def test_spec771_openai_responses_reasoning_summary_is_visible_reasoning():
    _response, emitted = _read("openai_responses", [
        {
            "type": "response.reasoning_summary_text.delta",
            "output_index": 0,
            "item_id": "reasoning-1",
            "summary_index": 2,
            "delta": "摘要思考",
        },
        {"type": "response.completed", "response": {"status": "completed"}},
    ])
    segments = [
        item
        for _event_type, payload in emitted
        for item in payload.get("stream_segments") or []
    ]

    assert [(item["channel"], item["delta"]) for item in segments] == [
        ("reasoning", "摘要思考")
    ]
    assert segments[0]["provider_block"]["summary_index"] == 2
    assert segments[0]["provider_block"]["event_type"] == (
        "response.reasoning_summary_text.delta")


def test_spec772_openai_responses_reasoning_item_is_not_visible_text():
    executor = object.__new__(APIExecutor)
    response = {
        "output": [
            {
                "id": "reasoning-1",
                "type": "reasoning",
                "content": [{"type": "reasoning_text", "text": "内部推理"}],
            },
            {
                "id": "tool-1",
                "type": "function_call",
                "call_id": "call-1",
                "name": "file_read",
                "arguments": "{}",
            },
        ],
    }

    assert executor._response_text(response) == ""
    envelopes = extract_tool_call_envelopes(
        response,
        provider="openai_responses",
        endpoint="unit",
    )
    assert [(item["tool_id"], item["call_id"]) for item in envelopes] == [
        ("file_read", "call-1")
    ]


def test_spec772_openai_responses_reads_only_typed_message_content():
    executor = object.__new__(APIExecutor)
    response = {
        "output": [
            {
                "type": "reasoning",
                "content": [{"type": "reasoning_text", "text": "不要泄漏"}],
            },
            {
                "type": "message",
                "role": "assistant",
                "content": [
                    {"type": "output_text", "text": "可见正文"},
                    {"type": "refusal", "refusal": "拒绝说明"},
                    {"type": "reasoning_text", "text": "仍不可见"},
                ],
            },
        ],
    }

    assert executor._response_text(response) == "可见正文\n拒绝说明"


def test_spec772_other_provider_visible_text_boundaries_remain_closed():
    executor = object.__new__(APIExecutor)

    assert executor._response_text({
        "choices": [{"message": {
            "content": "Chat正文",
            "reasoning_content": "Chat推理",
        }}],
    }) == "Chat正文"
    assert executor._response_text({
        "content": [
            {"type": "thinking", "thinking": "Anthropic推理"},
            {"type": "text", "text": "Anthropic正文"},
        ],
    }) == "Anthropic正文"


def test_spec710_custom_url_uses_explicit_protocol_instead_of_guessing():
    executor = object.__new__(APIExecutor)
    executor.cfg = _Config()
    executor._monotonic = lambda: 1.0
    executor._stream_id = "custom-protocol"
    executor._stream_event_sink = lambda *_args: None

    response = executor._read_openai_sse_or_json_response(
        _Response([
            {
                "type": "message_start",
                "message": {"id": "msg_custom", "role": "assistant", "content": []},
            },
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "text", "text": ""},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "text_delta", "text": "custom endpoint"},
            },
            {"type": "message_stop"},
        ]),
        "https://example.test/custom-stream",
        provider="anthropic_messages",
        first_chunk_timeout=180,
        idle_timeout=180,
    )

    assert response["id"] == "msg_custom"
    assert response["content"][0]["text"] == "custom endpoint"


def test_spec710_subthreshold_tail_flushes_while_provider_is_paused():
    delta_emitted = threading.Event()

    class PausedResponse(_Response):
        def __iter__(self):
            yield self._lines[0]
            yield self._lines[1]
            assert delta_emitted.wait(1.5)
            yield self._lines[2]

    executor = object.__new__(APIExecutor)
    executor.cfg = _Config()
    executor._monotonic = time.monotonic
    executor._stream_id = "paused-stream"
    emitted = []

    def capture(event_type, payload):
        emitted.append((event_type, payload))
        if event_type == "llm_stream_delta":
            delta_emitted.set()

    executor._stream_event_sink = capture
    response = executor._read_provider_sse_response(
        PausedResponse([
            {"choices": [{"delta": {"content": "a"}}]},
            {"choices": [{"delta": {"content": "b"}}]},
            "[DONE]",
        ]),
        "https://example.test/v1/chat/completions",
        provider="openai_chat",
        first_chunk_timeout=2,
        idle_timeout=2,
    )

    assert response["choices"][0]["message"]["content"] == "ab"
    assert next(
        payload for event, payload in emitted if event == "llm_stream_delta"
    )["content_delta"] == "b"


def test_spec710_anthropic_messages_streams_multiple_text_blocks_and_tool_input():
    response, emitted = _read("anthropic_messages", [
        {
            "type": "message_start",
            "message": {"id": "msg_1", "role": "assistant", "content": [], "usage": {"input_tokens": 4}},
        },
        {"type": "content_block_start", "index": 0, "content_block": {"type": "text", "text": ""}},
        {"type": "content_block_delta", "index": 0, "delta": {"type": "text_delta", "text": "第一段"}},
        {"type": "content_block_start", "index": 1, "content_block": {"type": "text", "text": ""}},
        {"type": "content_block_delta", "index": 1, "delta": {"type": "text_delta", "text": "第二段"}},
        {
            "type": "content_block_start",
            "index": 2,
            "content_block": {"type": "tool_use", "id": "tool_1", "name": "file_read", "input": {}},
        },
        {
            "type": "content_block_delta",
            "index": 2,
            "delta": {"type": "input_json_delta", "partial_json": '{"path":"README.md"}'},
        },
        {
            "type": "message_delta",
            "delta": {"stop_reason": "tool_use"},
            "usage": {"output_tokens": 8},
        },
        {"type": "message_stop"},
    ])

    assert [item["text"] for item in response["content"] if item["type"] == "text"] == ["第一段", "第二段"]
    envelopes = extract_tool_call_envelopes(
        response,
        provider="anthropic_messages",
        endpoint="unit",
    )
    assert envelopes[0]["arguments"] == {"path": "README.md"}
    assert response["usage"] == {"input_tokens": 4, "output_tokens": 8}
    done = next(payload for event, payload in emitted if event == "llm_stream_done")
    assert done["content_delta"] == "第一段第二段"
    assert done["tool_argument_chars"] > 0


@pytest.mark.parametrize("provider", ["openai_chat", "openai_responses", "anthropic_messages"])
def test_spec710_streaming_config_applies_to_all_supported_protocols(provider):
    payload = APIExecutor._apply_streaming_config_to_payload(
        {"model": "unit"},
        {"streaming": {"enabled": True, "protocol": "openai_sse", "include_usage": True}},
        provider,
    )
    assert payload["stream"] is True
    assert ("stream_options" in payload) is (provider == "openai_chat")


def test_spec710_unknown_events_are_ignored_but_terminal_is_required():
    response, _ = _read("anthropic_messages", [
        {"type": "message_start", "message": {"id": "msg_2", "content": []}},
        {"type": "future_event", "value": "ignored"},
        {"type": "message_stop"},
    ])
    assert response["id"] == "msg_2"

    with pytest.raises(Exception, match="ended before terminal event"):
        _read("openai_responses", [
            {"type": "response.output_text.delta", "delta": "partial"},
        ])


def test_spec710_incomplete_anthropic_tool_arguments_fail_closed():
    with pytest.raises(Exception, match="provider_stream_incomplete_tool_call"):
        _read("anthropic_messages", [
            {
                "type": "content_block_start",
                "index": 0,
                "content_block": {"type": "tool_use", "id": "tool_2", "name": "file_read", "input": {}},
            },
            {
                "type": "content_block_delta",
                "index": 0,
                "delta": {"type": "input_json_delta", "partial_json": '{"path":'},
            },
            {"type": "message_stop"},
        ])


def test_spec710_bad_sse_json_is_classified_as_interrupted():
    executor = object.__new__(APIExecutor)
    executor.cfg = _Config()
    executor._monotonic = lambda: 1.0
    executor._stream_id = "bad-json"
    executor._stream_event_sink = lambda *_args: None
    response = _Response([])
    response._lines = [b"data: {not-json}\n"]

    with pytest.raises(APIBridgeError, match="invalid SSE JSON"):
        executor._read_openai_sse_or_json_response(
            response,
            "https://example.test/v1/responses",
            first_chunk_timeout=180,
            idle_timeout=180,
        )


@pytest.mark.parametrize("path", ["chat/completions", "responses", "messages"])
def test_spec710_streaming_request_can_fall_back_to_plain_json(path):
    class PlainResponse:
        headers = {"Content-Type": "application/json"}

        @staticmethod
        def read():
            return b'{"id":"plain","output_text":"complete"}'

    executor = object.__new__(APIExecutor)
    response = executor._read_openai_sse_or_json_response(
        PlainResponse(),
        f"https://example.test/v1/{path}",
        first_chunk_timeout=180,
        idle_timeout=180,
    )

    assert response["id"] == "plain"
