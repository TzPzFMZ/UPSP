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
