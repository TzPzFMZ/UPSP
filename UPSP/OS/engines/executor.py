"""
API 执行器 — 三档路由 + 熔断 + 握手 + 超时
DDS §21 API调用 + §38 应急冗余

三档 API:
  档位一（容灾档）：三步全用同一 API
  档位二（分家档）：起手+善后走便宜 API，反应步走主力
  档位三（豪华档）：三步各走各的

熔断器：连续失败 N 次 → 冷却 M 秒
握手协议：超时10s，重试2次

engines/ vs scripts/ 边界：
  executor 只关心 WHEN（什么时候调 API）和 WHO（调哪个 endpoint）
  不关心 WHAT（传给 API 什么内容）—— 内容由 assembly/ 提供
"""
import time
import json
import hashlib
import multiprocessing
import os
import queue
import socket
import sys
import tempfile
import threading
import urllib.request
import urllib.error
import uuid
from datetime import datetime

from data.config_store import ConfigStore
from data.connectivity_store import ConnectivityStore
from data.audit_store import AuditStore
from constants import local_now
from errors import APIBridgeError, APITimeoutError, ProviderCallCancelled
from logic.native_tool_calls import (
    build_provider_response_meta,
    export_provider_tool_schemas,
    extract_tool_call_envelopes,
    provider_for_url,
)
from logic.execution_permission import (
    execution_permission_audit,
    load_execution_permission_level,
)
from logic.runtime_channels import (
    CALL_CHANNELS,
    STEP_TERMINAL_TOOLS,
    channel_for_step,
)
from engines.prompt_cache_planner import (
    EXPLICIT_PROFILES,
    apply_explicit_breakpoints,
    profile_settings,
)

_SYSTEM_SLEEP = time.sleep


class _ProviderTransportConfig:
    """Only the four transport values needed inside the killable worker."""

    def __init__(self, values):
        self.values = dict(values or {})

    def get_request_timeout(self):
        return int(self.values["request_timeout"])

    def get_stream_first_chunk_timeout(self):
        return int(self.values["first_chunk_timeout"])

    def get_stream_idle_timeout(self):
        return int(self.values["idle_timeout"])

    def get_stream_content_overrun_chars(self):
        return int(self.values["content_overrun_chars"])


def _provider_transport_worker(connection, request):
    """Run one blocking urllib request outside the resident Runtime process."""
    finished = threading.Event()

    def stop_with_parent():
        parent = multiprocessing.parent_process()
        while not finished.wait(0.25):
            if parent is not None and not parent.is_alive():
                os._exit(70)

    threading.Thread(
        target=stop_with_parent,
        name="provider-parent-watch",
        daemon=True,
    ).start()
    try:
        executor = object.__new__(APIExecutor)
        executor.cfg = _ProviderTransportConfig(request["timeouts"])
        executor._monotonic = time.monotonic
        executor._stream_id = uuid.uuid4().hex
        executor._transport_provider = str(request.get("provider") or "")
        executor._stream_event_sink = lambda event_type, payload: connection.send({
            "type": "event",
            "event_type": str(event_type),
            "payload": dict(payload or {}),
        })
        response = executor._send_request(
            request["url"],
            request["api_key"],
            request["payload"],
        )
        connection.send({"type": "result", "response": response})
    except APITimeoutError as exc:
        connection.send({
            "type": "error",
            "kind": "timeout",
            "message": str(exc),
            "endpoint": str(getattr(exc, "endpoint", "") or ""),
            "timeout_seconds": getattr(exc, "timeout_seconds", None),
        })
    except APIBridgeError as exc:
        connection.send({
            "type": "error",
            "kind": "api",
            "message": str(exc),
            "endpoint": str(getattr(exc, "endpoint", "") or ""),
            "status_code": getattr(exc, "status_code", None),
        })
    except Exception as exc:
        connection.send({
            "type": "error",
            "kind": "worker",
            "message": f"{type(exc).__name__}: {exc}",
            "endpoint": str(request.get("url") or ""),
        })
    finally:
        finished.set()
        try:
            connection.close()
        except OSError:
            pass


class CircuitBreaker:
    """熔断器"""

    def __init__(self, max_failures=3, cooldown_seconds=900):
        self.max_failures = max_failures
        self.cooldown = cooldown_seconds
        self.failure_count = 0
        self.last_failure_at = None
        self.state = "closed"  # closed → open → half_open → closed

    def record_failure(self):
        self.failure_count += 1
        self.last_failure_at = local_now()
        if self.failure_count >= self.max_failures:
            self.state = "open"

    def record_success(self):
        if self.state == "half_open":
            self.state = "closed"
        self.failure_count = 0

    def allow_request(self):
        if self.state == "closed":
            return True
        if self.state == "open":
            if self.last_failure_at:
                elapsed = (local_now() - self.last_failure_at).total_seconds()
                if elapsed >= self.cooldown:
                    self.state = "half_open"
                    return True
            return False
        return True  # half_open 放行一次试试


class OpenAIChatSSEAccumulator:
    """Accumulate OpenAI-compatible chat SSE chunks into one completion."""

    def __init__(self):
        self.response_id = ""
        self.object_type = "chat.completion"
        self.created = None
        self.model = ""
        self.system_fingerprint = ""
        self.role = "assistant"
        self.content_parts = []
        self.reasoning_parts = []
        self.refusal_parts = []
        self.finish_reason = None
        self.usage = {}
        self._tool_calls = {}
        self._emitted_content_chars = 0
        self._emitted_reasoning_chars = 0

    def add_chunk(self, chunk):
        if not isinstance(chunk, dict):
            return
        self._update_top_level(chunk)
        usage = chunk.get("usage")
        if isinstance(usage, dict):
            self.usage = dict(usage)
        for choice in chunk.get("choices") or []:
            if not isinstance(choice, dict):
                continue
            finish_reason = choice.get("finish_reason")
            if finish_reason not in (None, ""):
                self.finish_reason = finish_reason
            delta = choice.get("delta") or {}
            if not isinstance(delta, dict):
                continue
            role = delta.get("role")
            if role:
                self.role = str(role)
            self._append_text(delta.get("content"), self.content_parts)
            self._append_text(delta.get("reasoning_content"), self.reasoning_parts)
            self._append_text(delta.get("refusal"), self.refusal_parts)
            for tool_call in delta.get("tool_calls") or []:
                self._add_tool_call_delta(tool_call)

    def _update_top_level(self, chunk):
        if chunk.get("id"):
            self.response_id = str(chunk.get("id"))
        if chunk.get("object"):
            self.object_type = str(chunk.get("object"))
        if chunk.get("created") is not None:
            self.created = chunk.get("created")
        if chunk.get("model"):
            self.model = str(chunk.get("model"))
        if chunk.get("system_fingerprint"):
            self.system_fingerprint = str(chunk.get("system_fingerprint"))

    @staticmethod
    def _append_text(value, parts):
        if value is None:
            return
        text = str(value)
        if text:
            parts.append(text)

    def _add_tool_call_delta(self, tool_call):
        if not isinstance(tool_call, dict):
            return
        try:
            index = int(tool_call.get("index") or 0)
        except (TypeError, ValueError):
            index = 0
        slot = self._tool_calls.setdefault(index, {
            "index": index,
            "id": "",
            "type": "function",
            "function": {
                "name": "",
                "arguments": "",
            },
        })
        if tool_call.get("id"):
            slot["id"] = str(tool_call.get("id"))
        if tool_call.get("type"):
            slot["type"] = str(tool_call.get("type"))
        function = tool_call.get("function") or {}
        if not isinstance(function, dict):
            return
        if function.get("name"):
            slot["function"]["name"] = str(function.get("name"))
        if function.get("arguments") is not None:
            slot["function"]["arguments"] += str(function.get("arguments"))

    def summary(self, *, include_delta=False):
        content = "".join(self.content_parts) + "".join(self.refusal_parts)
        reasoning = "".join(self.reasoning_parts)
        payload = {
            "content_chars": len(content),
            "reasoning_chars": len(reasoning),
            "tool_argument_chars": sum(
                len((item.get("function") or {}).get("arguments") or "")
                for item in self._tool_calls.values()
            ),
            "tool_call_count": len(self._tool_calls),
            "tool_names": [
                str((item.get("function") or {}).get("name") or "")
                for item in self._sorted_tool_calls()
                if (item.get("function") or {}).get("name")
            ],
            "finish_reason": self.finish_reason,
        }
        if include_delta:
            content_delta = content[self._emitted_content_chars:]
            reasoning_delta = reasoning[self._emitted_reasoning_chars:]
            if content_delta:
                payload["content_delta"] = content_delta
            if reasoning_delta:
                payload["reasoning_delta"] = reasoning_delta
            self._emitted_content_chars = len(content)
            self._emitted_reasoning_chars = len(reasoning)
        return payload

    def validate_tool_arguments(self):
        for tool_call in self._sorted_tool_calls():
            function = tool_call.get("function") or {}
            arguments = function.get("arguments")
            if arguments in (None, ""):
                raise ValueError("empty tool arguments")
            try:
                json.loads(str(arguments))
            except json.JSONDecodeError as exc:
                raise ValueError(str(exc)) from exc

    def to_chat_completion(self):
        message = {
            "role": self.role or "assistant",
            "content": "".join(self.content_parts),
        }
        reasoning = "".join(self.reasoning_parts)
        if reasoning:
            message["reasoning_content"] = reasoning
        refusal = "".join(self.refusal_parts)
        if refusal:
            message["refusal"] = refusal
        tool_calls = self._sorted_tool_calls()
        if tool_calls:
            message["tool_calls"] = tool_calls
        choice = {
            "index": 0,
            "message": message,
            "finish_reason": self.finish_reason,
        }
        response = {
            "id": self.response_id,
            "object": "chat.completion",
            "created": self.created,
            "model": self.model,
            "choices": [choice],
            "usage": self.usage or {},
        }
        if self.system_fingerprint:
            response["system_fingerprint"] = self.system_fingerprint
        return response

    def _sorted_tool_calls(self):
        calls = []
        for index in sorted(self._tool_calls):
            item = self._tool_calls[index]
            calls.append({
                "id": item.get("id") or f"call_stream_{index}",
                "type": item.get("type") or "function",
                "function": {
                    "name": (item.get("function") or {}).get("name") or "",
                    "arguments": (item.get("function") or {}).get("arguments") or "",
                },
            })
        return calls


class OpenAIResponsesSSEAccumulator:
    """Accumulate Responses API SSE events into one response object."""

    def __init__(self):
        self.response = {}
        self.content_parts = []
        self.refusal_parts = []
        self.reasoning_parts = []
        self._items = {}
        self._emitted_content_chars = 0
        self._emitted_reasoning_chars = 0
        self.finish_reason = None

    def add_event(self, event):
        if not isinstance(event, dict):
            return False
        event_type = str(event.get("type") or "")
        response = event.get("response")
        if isinstance(response, dict):
            self.response.update(response)
            if response.get("status"):
                self.finish_reason = response.get("status")
        if event_type == "response.output_item.added":
            self._store_item(event.get("output_index"), event.get("item"))
        elif event_type == "response.output_item.done":
            self._store_item(event.get("output_index"), event.get("item"), replace=True)
        elif event_type == "response.output_text.delta":
            self._append(event.get("delta"), self.content_parts)
        elif event_type == "response.refusal.delta":
            self._append(event.get("delta"), self.refusal_parts)
        elif event_type == "response.reasoning_text.delta":
            self._append(event.get("delta"), self.reasoning_parts)
        elif event_type == "response.function_call_arguments.delta":
            slot = self._item_slot(event.get("output_index"), event.get("item_id"))
            slot["type"] = "function_call"
            slot["arguments"] = str(slot.get("arguments") or "") + str(event.get("delta") or "")
        return event_type == "response.completed"

    @staticmethod
    def _append(value, parts):
        if value not in (None, ""):
            parts.append(str(value))

    def _item_slot(self, output_index=None, item_id=None):
        try:
            key = int(output_index)
        except (TypeError, ValueError):
            key = len(self._items)
        slot = self._items.setdefault(key, {
            "id": str(item_id or ""),
            "type": "",
        })
        if item_id:
            slot["id"] = str(item_id)
        return slot

    def _store_item(self, output_index, item, *, replace=False):
        if not isinstance(item, dict):
            return
        slot = self._item_slot(output_index, item.get("id"))
        if replace:
            slot.clear()
        slot.update(item)

    def _output_items(self):
        output = self.response.get("output")
        if isinstance(output, list) and output:
            return output
        items = []
        visible = "".join(self.content_parts)
        refusal = "".join(self.refusal_parts)
        if visible or refusal:
            content = []
            if visible:
                content.append({"type": "output_text", "text": visible})
            if refusal:
                content.append({"type": "refusal", "refusal": refusal})
            items.append({"type": "message", "role": "assistant", "content": content})
        items.extend(dict(self._items[index]) for index in sorted(self._items))
        return items

    def summary(self, *, include_delta=False):
        content = "".join(self.content_parts) + "".join(self.refusal_parts)
        reasoning = "".join(self.reasoning_parts)
        tool_items = [
            item for item in self._output_items()
            if isinstance(item, dict) and item.get("type") == "function_call"
        ]
        payload = {
            "content_chars": len(content),
            "reasoning_chars": len(reasoning),
            "tool_argument_chars": sum(len(str(item.get("arguments") or "")) for item in tool_items),
            "tool_call_count": len(tool_items),
            "tool_names": [str(item.get("name") or "") for item in tool_items if item.get("name")],
            "finish_reason": self.finish_reason,
        }
        if include_delta:
            content_delta = content[self._emitted_content_chars:]
            reasoning_delta = reasoning[self._emitted_reasoning_chars:]
            if content_delta:
                payload["content_delta"] = content_delta
            if reasoning_delta:
                payload["reasoning_delta"] = reasoning_delta
            self._emitted_content_chars = len(content)
            self._emitted_reasoning_chars = len(reasoning)
        return payload

    def validate_tool_arguments(self):
        for item in self._output_items():
            if not isinstance(item, dict) or item.get("type") != "function_call":
                continue
            arguments = item.get("arguments")
            if arguments in (None, ""):
                raise ValueError("empty tool arguments")
            json.loads(str(arguments))

    def to_response(self):
        response = dict(self.response)
        response.setdefault("object", "response")
        response.setdefault("status", self.finish_reason or "completed")
        response["output"] = self._output_items()
        visible = "".join(self.content_parts) + "".join(self.refusal_parts)
        if visible and not response.get("output_text"):
            response["output_text"] = visible
        return response


class AnthropicMessagesSSEAccumulator:
    """Accumulate Anthropic Messages SSE events into one message."""

    def __init__(self):
        self.message = {}
        self.blocks = {}
        self.finish_reason = None
        self.usage = {}
        self._emitted_content_chars = 0
        self._emitted_reasoning_chars = 0

    def add_event(self, event):
        if not isinstance(event, dict):
            return False
        event_type = str(event.get("type") or "")
        if event_type == "message_start" and isinstance(event.get("message"), dict):
            self.message.update(event.get("message"))
            self.usage.update(self.message.get("usage") or {})
            for index, block in enumerate(self.message.get("content") or []):
                if isinstance(block, dict):
                    self.blocks[index] = dict(block)
        elif event_type == "content_block_start":
            self.blocks[self._index(event)] = dict(event.get("content_block") or {})
        elif event_type == "content_block_delta":
            self._add_delta(self._index(event), event.get("delta") or {})
        elif event_type == "message_delta":
            delta = event.get("delta") or {}
            if isinstance(delta, dict) and delta.get("stop_reason") not in (None, ""):
                self.finish_reason = delta.get("stop_reason")
            if isinstance(event.get("usage"), dict):
                self.usage.update(event.get("usage"))
        return event_type == "message_stop"

    @staticmethod
    def _index(event):
        try:
            return int(event.get("index") or 0)
        except (TypeError, ValueError):
            return 0

    def _add_delta(self, index, delta):
        if not isinstance(delta, dict):
            return
        slot = self.blocks.setdefault(index, {"type": ""})
        delta_type = str(delta.get("type") or "")
        if delta_type == "text_delta":
            slot["type"] = slot.get("type") or "text"
            slot["text"] = str(slot.get("text") or "") + str(delta.get("text") or "")
        elif delta_type == "input_json_delta":
            slot["type"] = "tool_use"
            slot["_input_json"] = str(slot.get("_input_json") or "") + str(delta.get("partial_json") or "")
        elif delta_type == "thinking_delta":
            slot["type"] = slot.get("type") or "thinking"
            slot["thinking"] = str(slot.get("thinking") or "") + str(delta.get("thinking") or "")
        elif delta_type == "signature_delta":
            slot["signature"] = str(slot.get("signature") or "") + str(delta.get("signature") or "")

    def _content(self, *, validate=False):
        content = []
        for index in sorted(self.blocks):
            item = dict(self.blocks[index])
            raw_input = item.pop("_input_json", None)
            if item.get("type") == "tool_use" and raw_input is not None:
                if not raw_input:
                    if validate:
                        raise ValueError("empty tool arguments")
                    item["input"] = {}
                else:
                    try:
                        item["input"] = json.loads(raw_input)
                    except json.JSONDecodeError:
                        if validate:
                            raise
                        item["input"] = {}
            content.append(item)
        return content

    def summary(self, *, include_delta=False):
        content = "".join(
            str(item.get("text") or "")
            for item in self.blocks.values()
            if item.get("type") == "text"
        )
        reasoning = "".join(
            str(item.get("thinking") or "")
            for item in self.blocks.values()
            if item.get("type") == "thinking"
        )
        tools = [item for item in self.blocks.values() if item.get("type") == "tool_use"]
        payload = {
            "content_chars": len(content),
            "reasoning_chars": len(reasoning),
            "tool_argument_chars": sum(len(str(item.get("_input_json") or "")) for item in tools),
            "tool_call_count": len(tools),
            "tool_names": [str(item.get("name") or "") for item in tools if item.get("name")],
            "finish_reason": self.finish_reason,
        }
        if include_delta:
            content_delta = content[self._emitted_content_chars:]
            reasoning_delta = reasoning[self._emitted_reasoning_chars:]
            if content_delta:
                payload["content_delta"] = content_delta
            if reasoning_delta:
                payload["reasoning_delta"] = reasoning_delta
            self._emitted_content_chars = len(content)
            self._emitted_reasoning_chars = len(reasoning)
        return payload

    def validate_tool_arguments(self):
        self._content(validate=True)

    def to_message(self):
        message = dict(self.message)
        message["type"] = "message"
        message.setdefault("role", "assistant")
        message["content"] = self._content(validate=True)
        message["stop_reason"] = self.finish_reason or message.get("stop_reason")
        message["usage"] = dict(self.usage)
        return message


class APIExecutor:
    """API 调用执行器"""

    DEFAULT_PROVIDER_CALL_INTERVAL_SECONDS = 5.0
    PROMPT_CACHE_DEFAULT_LANES = {
        channel.name: channel.prompt_cache_lane
        for channel in CALL_CHANNELS.values()
        if channel.prompt_cache_lane
    }
    PROMPT_CACHE_DEFAULT_LANES["reaction.final_reply"] = (
        CALL_CHANNELS["final_reply"].prompt_cache_lane
    )
    OPENAI_PROMPT_CACHE_PROVIDERS = {"openai_responses", "openai_chat"}
    PROVIDER_FORMAT_INSTABILITY = "provider_model_format_instability"
    PROVIDER_FORMAT_ERROR_KINDS = {
        "provider_stream_idle_timeout",
        "provider_stream_incomplete_tool_call",
        "provider_stream_content_overrun",
        "provider_native_tool_empty_output",
    }
    PROVIDER_RECOVERABLE_FORMAT_KINDS = {
        "provider_stream_idle_timeout",
    }

    def __init__(self, config_store=None, connectivity_store=None,
                 context_dir=None):
        self.cfg = config_store or ConfigStore()
        active_endpoint_ids = getattr(self.cfg, "get_active_model_profile_ids", None)
        self.conn = connectivity_store or ConnectivityStore(
            active_endpoint_ids=active_endpoint_ids,
        )
        if context_dir is None and os.environ.get("PYTEST_CURRENT_TEST"):
            context_dir = tempfile.mkdtemp(prefix="upsp-provider-request-")
        self.bind_context_dir(context_dir)
        self.breakers = {}
        self._provider_call_interval_seconds = (
            self._load_provider_call_interval_seconds()
        )
        self._last_provider_call_finished_at = None
        self._monotonic = lambda: time.monotonic()
        self._sleep = lambda seconds: time.sleep(seconds)
        self._default_sleep = self._sleep
        self._stream_event_sink = None
        self._cancel_event = threading.Event()
        self._transport_lock = threading.Lock()
        self._active_transport_process = None

    def reset_cancellation(self):
        self._cancel_event.clear()

    def request_cancel(self):
        self._cancel_event.set()
        with self._transport_lock:
            process = self._active_transport_process
        if process is not None and process.is_alive():
            process.terminate()

    @property
    def cancellation_requested(self):
        return self._cancel_event.is_set()

    @property
    def transport_active(self):
        with self._transport_lock:
            process = self._active_transport_process
        return bool(process is not None and process.is_alive())

    def _raise_if_cancelled(self):
        if self.cancellation_requested:
            raise ProviderCallCancelled()

    def bind_context_dir(self, context_dir):
        if context_dir is None:
            self.audit_store = AuditStore()
            return self
        self.audit_store = AuditStore(
            setup_dir=os.path.join(context_dir, "setup"),
            reaction_dir=os.path.join(context_dir, "reaction"),
            cleanup_dir=os.path.join(context_dir, "cleanup"),
        )
        return self

    def bind_stream_event_sink(self, sink):
        previous = self._stream_event_sink
        self._stream_event_sink = sink if callable(sink) else None
        return previous

    def _emit_stream_event(self, event_type, payload):
        sink = self._stream_event_sink
        if not callable(sink):
            return
        try:
            sink(event_type, dict(payload or {}))
        except Exception:
            pass

    @staticmethod
    def _decode_response_body(body):
        text = str(body or "")
        stripped = text.strip()
        if stripped.startswith("data:"):
            chunks = []
            for line in stripped.splitlines():
                line = line.strip()
                if not line.startswith("data:"):
                    continue
                data = line[5:].strip()
                if data and data != "[DONE]":
                    chunks.append(data)
            if len(chunks) == 1:
                return json.loads(chunks[0])
            if chunks:
                return {"stream_chunks": [json.loads(chunk) for chunk in chunks]}
        return json.loads(text)

    @staticmethod
    def _canonical_json_bytes(value):
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    @classmethod
    def _request_body_sha256(cls, request_body):
        return hashlib.sha256(cls._canonical_json_bytes(request_body)).hexdigest()

    def _provider_request_envelope(
            self,
            *,
            step,
            channel,
            tier,
            request_url,
            provider,
            model_name,
            endpoint_config,
            payload,
            request_contract_audit,
            attempt=None):
        endpoint = {
            "tier": tier,
            "url": request_url,
        }
        if isinstance(endpoint_config, dict):
            endpoint["api_format"] = endpoint_config.get("api_format")
            endpoint["provider_hint"] = endpoint_config.get("provider")
        request_body = payload if isinstance(payload, dict) else {}
        return {
            "schema": "provider_request.v1",
            "created_at": local_now().isoformat(),
            "call": {
                "step": getattr(channel, "step", step) or step,
                "channel": getattr(channel, "name", step) or step,
                "phase": getattr(channel, "phase", "") or "",
                "iteration": None,
                "attempt": attempt,
            },
            "provider": {
                "provider": provider,
                "model": model_name,
            },
            "endpoint": endpoint,
            "request_contract_audit": request_contract_audit,
            "request_body": request_body,
            "request_body_sha256": self._request_body_sha256(request_body),
        }

    def _call_header_for_layers(self, step, channel, attempt):
        return {
            "step": getattr(channel, "step", step) or step,
            "channel": getattr(channel, "name", step) or step,
            "phase": getattr(channel, "phase", "") or "",
            "iteration": None,
            "attempt": attempt,
        }

    @staticmethod
    def _endpoint_header_for_layers(tier, request_url, endpoint_config):
        endpoint = {
            "tier": tier,
            "url": request_url,
        }
        if isinstance(endpoint_config, dict):
            endpoint["api_format"] = endpoint_config.get("api_format")
            endpoint["provider_hint"] = endpoint_config.get("provider")
            extra_body = endpoint_config.get("extra_body")
            reasoning_replay_enabled = (
                endpoint_config.get("reasoning_replay_enabled") is True
                or (
                    isinstance(extra_body, dict)
                    and extra_body.get("enable_thinking") is True
                )
            )
            if reasoning_replay_enabled:
                endpoint["reasoning_replay_enabled"] = True
        return endpoint

    def _tool_header_for_layers(
            self,
            native_tools,
            channel,
            request_contract_audit,
            execution_permission_level=None):
        audit = request_contract_audit if isinstance(request_contract_audit, dict) else {}
        header = {
            "terminal_tool": audit.get("terminal_tool"),
            "tool_mode": audit.get("tool_mode"),
            "tool_names": audit.get("tool_names") or [],
            "tools_transmitted": bool(audit.get("tools_transmitted")),
            "standard_tools_enabled": bool(audit.get("standard_tools_enabled")),
            "native_tool_mode": audit.get("native_tool_mode"),
            "tools": list(native_tools or []),
        }
        header.update(execution_permission_audit(execution_permission_level))
        return header

    def _generation_config_for_layers(self, provider, step, endpoint_config,
                                      prompt_cache_scope=None,
                                      model_name=""):
        if provider == "openai_responses":
            payload = {
                "max_output_tokens": 4096,
                "reasoning": {"effort": "none"},
                "text": {
                    "format": {"type": "text"},
                    "verbosity": "low",
                },
                "temperature": 0.7,
            }
        elif provider == "anthropic_messages":
            payload = {"max_tokens": 4096}
        else:
            payload = {"temperature": 0.7}
        self._apply_prompt_cache_config(
            payload,
            provider or "openai_chat",
            step,
            endpoint_config,
            prompt_cache_scope=prompt_cache_scope,
            model_name=model_name,
        )
        self._apply_endpoint_payload_overrides(payload, endpoint_config)
        return payload

    def _ensure_context_layers_from_inputs(self, context_step, system_prompt,
                                           messages):
        """Bootstrap layers for direct executor use when no assembler ran."""
        ctx_dir = self.audit_store.step_dirs.get(context_step)
        if not ctx_dir:
            return
        layers_dir = os.path.join(ctx_dir, "layers")
        manifest_path = os.path.join(ctx_dir, "manifest.json")
        existing_context_layers = [
            os.path.join(layers_dir, f"{layer_key}.json")
            for layer_key, _layer_id, _order in (
                ("10_permanent", "permanent", 10),
                ("20_periodic", "periodic", 20),
                ("30_lately", "lately", 30),
                ("40_high_freq", "high_freq", 40),
                ("50_now", "now", 50),
                ("60_statusbar", "statusbar", 60),
                ("99_popup", "popup", 99),
            )
        ]
        if os.path.isfile(manifest_path) or any(
                os.path.exists(path) for path in existing_context_layers):
            return
        permanent_content = (
            [{"role": "system", "content": str(system_prompt or "")}]
            if str(system_prompt or "").strip()
            else ""
        )
        now_content = self._messages_to_context_layer_messages(messages or [])
        now_text = self._messages_to_context_layer_text(messages or [])
        full_system = "\n\n---\n\n".join(
            part for part in (str(system_prompt or "").strip(), now_text)
            if part
        )
        self.audit_store.write_audit(context_step, {
            "permanent": permanent_content,
            "periodic": "",
            "lately": "",
            "high_freq": "",
            "now": now_content,
            "statusbar": "",
            "popup": "",
            "full_system": full_system,
        })

    @staticmethod
    def _messages_to_context_layer_text(messages):
        parts = []
        for message in messages or []:
            if not isinstance(message, dict):
                parts.append(f"**user**: {message}")
                continue
            role = str(message.get("role") or "user").strip() or "user"
            content = message.get("content")
            if isinstance(content, (dict, list)):
                content = json.dumps(content, ensure_ascii=False, sort_keys=True)
            parts.append(f"**{role}**: {content or ''}")
        return "\n\n".join(parts)

    @staticmethod
    def _messages_to_context_layer_messages(messages):
        normalized = []
        for message in messages or []:
            if not isinstance(message, dict):
                normalized.append({"role": "user", "content": str(message)})
                continue
            data = {}
            for key, value in message.items():
                if key.startswith("native_"):
                    continue
                data[key] = value
            data["role"] = str(data.get("role") or "user").strip() or "user"
            if "content" not in data:
                data["content"] = ""
            normalized.append(data)
        return normalized

    def _compile_request_body_from_layers(
            self,
            context_step,
            provider,
            *,
            endpoint_config=None,
            prompt_cache_scope=None):
        payloads = self.audit_store.read_context_layers(context_step)
        by_key = {payload.get("layer_key"): payload.get("content") for payload in payloads}
        call_header = by_key.get("00_call_header")
        tool_header = by_key.get("01_tool_header")
        generation_config = by_key.get("02_generation_config")
        call_header = call_header if isinstance(call_header, dict) else {}
        tool_header = tool_header if isinstance(tool_header, dict) else {}
        generation_config = (
            generation_config if isinstance(generation_config, dict) else {}
        )
        provider_meta = (
            call_header.get("provider")
            if isinstance(call_header.get("provider"), dict)
            else {}
        )
        endpoint_meta = (
            call_header.get("endpoint")
            if isinstance(call_header.get("endpoint"), dict)
            else {}
        )
        model_name = provider_meta.get("model") or ""
        tools = tool_header.get("tools") if isinstance(tool_header.get("tools"), list) else []
        system_prompt, messages, message_layers = (
            self._prompt_and_messages_from_context_layers(by_key)
        )
        payload_message_layers = list(message_layers)

        if provider == "openai_responses":
            payload = {
                "model": model_name,
                "input": self._responses_input(messages, provider),
            }
            if str(system_prompt or "").strip():
                payload["instructions"] = str(system_prompt).strip()
        elif provider == "anthropic_messages":
            anthropic_system, anthropic_messages = self._anthropic_messages(
                messages,
                system_prompt=system_prompt,
            )
            payload = {
                "model": model_name,
                "messages": anthropic_messages,
            }
            if anthropic_system:
                payload["system"] = anthropic_system
        else:
            payload_messages = self._chat_messages(
                messages,
                provider,
                model_name=model_name,
                endpoint_config=endpoint_meta,
            )
            if str(system_prompt or "").strip():
                payload_messages = [
                    {"role": "system", "content": str(system_prompt).strip()}
                ] + payload_messages
                payload_message_layers = ["10_permanent"] + payload_message_layers
            while len(payload_message_layers) < len(payload_messages):
                payload_message_layers.append("50_now")
            payload = {"model": model_name, "messages": payload_messages}
        payload.update(self._generation_config_payload(generation_config))
        if tools:
            payload["tools"] = tools
        prompt_cache_plan = {}
        settings = self._prompt_cache_settings(
            context_step,
            provider,
            endpoint_config,
            prompt_cache_scope=prompt_cache_scope,
            model_name=model_name,
        )
        if settings.get("profile") in EXPLICIT_PROFILES:
            payload, prompt_cache_plan = apply_explicit_breakpoints(
                payload,
                profile=settings["profile"],
                message_layers=payload_message_layers,
                layer_contents=by_key,
            )
        return payload, prompt_cache_plan

    @staticmethod
    def _generation_config_payload(generation_config):
        reserved = {
            "messages",
            "input",
            "instructions",
            "system",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
        }
        return {
            key: value
            for key, value in (generation_config or {}).items()
            if key not in reserved
        }

    @staticmethod
    def _prompt_and_messages_from_context_layers(by_key):
        role_by_key = {
            "10_permanent": "system",
            "20_periodic": "system",
            "30_lately": "system",
            "40_high_freq": "system",
            "50_now": "user",
            "60_statusbar": "system",
            "99_popup": "system",
        }
        system_parts = []
        messages = []
        message_layers = []
        for layer_key in (
                "10_permanent",
                "20_periodic",
                "30_lately",
                "40_high_freq",
                "50_now",
                "60_statusbar",
                "99_popup"):
            content = by_key.get(layer_key)
            if isinstance(content, list):
                for message in content:
                    if not isinstance(message, dict):
                        continue
                    role = str(message.get("role") or "user").strip() or "user"
                    value = message.get("content")
                    if isinstance(value, (dict, list)):
                        value = json.dumps(
                            value,
                            ensure_ascii=False,
                            sort_keys=True,
                        )
                    if layer_key == "10_permanent" and role in {"system", "developer"}:
                        if str(value or "").strip():
                            system_parts.append(str(value).strip())
                    else:
                        item = {
                            "role": role,
                            "content": value or "",
                        }
                        kind = str(message.get("kind") or "").strip()
                        if kind:
                            item["kind"] = kind
                        native_replay = message.get("native_replay")
                        if isinstance(native_replay, dict):
                            item["native_replay"] = dict(native_replay)
                        messages.append(item)
                        message_layers.append(layer_key)
                continue
            if isinstance(content, (dict, list)):
                content = json.dumps(content, ensure_ascii=False, sort_keys=True)
            content = str(content or "").strip()
            if content:
                if layer_key == "10_permanent":
                    system_parts.append(content)
                else:
                    messages.append({
                        "role": role_by_key[layer_key],
                        "content": content,
                    })
                    message_layers.append(layer_key)
        return "\n\n".join(system_parts), messages, message_layers

    # ==============================================================
    # 调用入口
    # ==============================================================

    def preview_request_contract(
            self,
            step,
            system_prompt="",
            messages=None,
            model=None,
            endpoint=None,
            active_protocol_tool_guides=None):
        """Build the sanitized LLM call contract without sending a request."""
        tier = endpoint or self._select_tier(step)
        ep = self._get_endpoint(tier)
        if not ep:
            raise APIBridgeError("no_endpoint", "无可用的 API endpoint")

        url = ep.get("url", "")
        model_name = model or ep.get("model", "")
        provider = provider_for_url(url, ep)
        request_url = self._resolved_request_url(url, provider)
        execution_permission_level = load_execution_permission_level(self.cfg)
        native_tools = self._native_tools_for_step(
            step,
            provider,
            active_protocol_tool_guides=active_protocol_tool_guides,
            execution_permission_level=execution_permission_level,
        )
        channel = channel_for_step(
            step,
            active_protocol_tool_guides=active_protocol_tool_guides,
        )
        prompt_cache_scope = self._prompt_cache_scope_for_channel(channel)
        payload = self._build_payload(
            url,
            model_name,
            system_prompt,
            messages or [],
            tools=native_tools,
            provider=provider,
            endpoint_config=ep,
            step=step,
            prompt_cache_scope=prompt_cache_scope,
        )
        payload = self._apply_streaming_config_to_payload(payload, ep, provider)
        request_contract_audit = self._request_contract_audit(
            step,
            provider,
            model_name,
            native_tools,
            payload,
            channel=channel,
            endpoint_config=ep,
            prompt_cache_scope=prompt_cache_scope,
        )
        envelope = self._provider_request_envelope(
            step=step,
            channel=channel,
            tier=tier,
            request_url=request_url,
            provider=provider,
            model_name=model_name,
            endpoint_config=ep,
            payload=payload,
            request_contract_audit=request_contract_audit,
        )
        return {
            "call_channel": channel.name,
            "phase": channel.phase,
            "tier": tier,
            "request_contract_audit": request_contract_audit,
            "provider_request_envelope": envelope,
        }

    def prepare_provider_request(self, step, system_prompt, messages, model=None,
                                 endpoint=None, active_protocol_tool_guides=None,
                                 attempt=1):
        """Compile, persist, and re-read the actual provider request body."""
        tier = endpoint or self._select_tier(step)
        ep = self._get_endpoint(tier)
        if not ep:
            raise APIBridgeError("no_endpoint", "no available API endpoint")

        health_endpoint = str(ep.get("profile_id") or tier)
        breaker = self._get_breaker(tier)
        if not breaker.allow_request():
            fallback_tier = self._fallback_tier(
                tier, current_endpoint=ep, step=step
            )
            if fallback_tier:
                return self.prepare_provider_request(
                    step,
                    system_prompt,
                    messages,
                    model=model,
                    endpoint=fallback_tier,
                    active_protocol_tool_guides=active_protocol_tool_guides,
                    attempt=attempt,
                )
            raise APIBridgeError(tier, "all endpoints are circuit-open")

        url = ep.get("url", "")
        api_key = self._get_api_key(ep)
        model_name = model or ep.get("model", "")
        provider = provider_for_url(url, ep)
        request_url = self._resolved_request_url(url, provider)
        execution_permission_level = load_execution_permission_level(self.cfg)
        native_tools = self._native_tools_for_step(
            step,
            provider,
            active_protocol_tool_guides=active_protocol_tool_guides,
            execution_permission_level=execution_permission_level,
        )
        channel = channel_for_step(
            step,
            active_protocol_tool_guides=active_protocol_tool_guides,
        )
        prompt_cache_scope = self._prompt_cache_scope_for_channel(channel)
        context_step = getattr(channel, "step", step) or step
        call_header = self._call_header_for_layers(step, channel, attempt)
        provider_header = {
            "provider": provider,
            "model": model_name,
        }
        endpoint_header = self._endpoint_header_for_layers(
            tier,
            request_url,
            ep,
        )
        self._ensure_context_layers_from_inputs(
            context_step,
            system_prompt,
            messages or [],
        )
        generation_config = self._generation_config_for_layers(
            provider,
            step,
            ep,
            prompt_cache_scope=prompt_cache_scope,
            model_name=model_name,
        )
        provisional_payload = dict(generation_config)
        if native_tools:
            provisional_payload["tools"] = list(native_tools or [])
        provisional_audit = self._request_contract_audit(
            step,
            provider,
            model_name,
            native_tools,
            provisional_payload,
            channel=channel,
            endpoint_config=ep,
            prompt_cache_scope=prompt_cache_scope,
        )
        call_layer_statuses = self.audit_store.write_call_layers(
            context_step,
            call=call_header,
            provider=provider_header,
            endpoint=endpoint_header,
            tool_header=self._tool_header_for_layers(
                native_tools,
                channel,
                provisional_audit,
                execution_permission_level=execution_permission_level,
            ),
            generation_config=generation_config,
        )
        payload, prompt_cache_plan = self._compile_request_body_from_layers(
            context_step,
            provider,
            endpoint_config=ep,
            prompt_cache_scope=prompt_cache_scope,
        )
        payload = self._apply_streaming_config_to_payload(payload, ep, provider)
        request_contract_audit = self._request_contract_audit(
            step,
            provider,
            model_name,
            native_tools,
            payload,
            channel=channel,
            endpoint_config=ep,
            prompt_cache_scope=prompt_cache_scope,
            prompt_cache_plan=prompt_cache_plan,
        )
        provider_request_envelope = self._provider_request_envelope(
            step=step,
            channel=channel,
            tier=tier,
            request_url=request_url,
            provider=provider,
            model_name=model_name,
            endpoint_config=ep,
            payload=payload,
            request_contract_audit=request_contract_audit,
            attempt=attempt,
        )
        provider_request_envelope = self.audit_store.write_compiled_provider_request(
            context_step,
            provider_request_envelope,
            call_layer_statuses=call_layer_statuses,
        )
        payload = self.audit_store.read_provider_request_body(context_step)
        return {
            "step": step,
            "system_prompt": system_prompt,
            "messages": list(messages or []),
            "model": model,
            "endpoint": tier,
            "active_protocol_tool_guides": active_protocol_tool_guides,
            "tier": tier,
            "health_endpoint": health_endpoint,
            "endpoint_config": ep,
            "breaker": breaker,
            "request_url": request_url,
            "api_key": api_key,
            "provider": provider,
            "model_name": model_name,
            "native_tools": native_tools,
            "channel": channel,
            "payload": payload,
            "request_contract_audit": request_contract_audit,
            "provider_request_envelope": provider_request_envelope,
            "call_channel": channel.name,
            "phase": channel.phase,
            "attempt": attempt,
        }

    def call_prepared(self, prepared):
        return self._call_prepared_internal(prepared, allow_fallback=True)

    def call_prepared_once(self, prepared):
        return self._call_prepared_internal(prepared, allow_fallback=False)

    def probe_model_profile(self, profile_id):
        """Test exactly one configured model without persona context or audit layers."""
        profile_id = str(profile_id or "").strip()
        if not profile_id:
            raise APIBridgeError("no_endpoint", "model profile is required")
        api_cfg = self.cfg.load("api")
        endpoint_items = api_cfg.get("endpoints") or {}
        tier, ep = next(
            (
                (key, value)
                for key, value in endpoint_items.items()
                if isinstance(value, dict)
                and str(value.get("profile_id") or "") == profile_id
            ),
            (profile_id, {}),
        )
        if not ep.get("url"):
            raise APIBridgeError(profile_id, "model profile is not configured")
        breaker = self._get_breaker(tier, endpoint=ep)
        if not breaker.allow_request():
            raise APIBridgeError(profile_id, "model profile circuit is open")

        provider = provider_for_url(ep.get("url", ""), ep)
        request_url = self._resolved_request_url(ep.get("url", ""), provider)
        model_name = str(ep.get("model") or "").strip()
        payload = self._build_payload(
            ep.get("url", ""),
            model_name,
            "",
            [{"role": "user", "content": "请只回复：连接成功"}],
            tools=[],
            provider=provider,
            endpoint_config=ep,
            step="setup",
            prompt_cache_scope=None,
        )
        if provider == "openai_responses":
            payload["max_output_tokens"] = 64
        elif provider == "anthropic_messages":
            payload["max_tokens"] = 64
        elif "max_completion_tokens" in payload:
            payload["max_completion_tokens"] = 64
        else:
            payload["max_tokens"] = 64
        payload = self._apply_streaming_config_to_payload(payload, ep, provider)
        result = self.call_prepared_once({
            "step": "setup",
            "system_prompt": "",
            "messages": [{"role": "user", "content": "请只回复：连接成功"}],
            "model": model_name,
            "endpoint": tier,
            "tier": tier,
            "health_endpoint": profile_id,
            "endpoint_config": ep,
            "breaker": breaker,
            "request_url": request_url,
            "api_key": self._get_api_key(ep),
            "provider": provider,
            "model_name": model_name,
            "native_tools": [],
            "payload": payload,
            "request_contract_audit": {},
            "provider_request_envelope": {},
        })
        if not str(result.get("response") or "").strip():
            raise APIBridgeError(profile_id, "provider returned an empty response")
        return result

    def _call_prepared_internal(self, prepared, *, allow_fallback):
        prepared = dict(prepared or {})
        step = prepared.get("step")
        system_prompt = prepared.get("system_prompt", "")
        messages = list(prepared.get("messages") or [])
        model = prepared.get("model")
        active_protocol_tool_guides = prepared.get("active_protocol_tool_guides")
        tier = prepared.get("tier") or prepared.get("endpoint")
        ep = prepared.get("endpoint_config") or self._get_endpoint(tier)
        health_endpoint = str(
            prepared.get("health_endpoint") or ep.get("profile_id") or tier
        )
        breaker = prepared.get("breaker") or self._get_breaker(tier)
        request_url = prepared.get("request_url")
        api_key = prepared.get("api_key", "")
        provider = prepared.get("provider", "")
        model_name = prepared.get("model_name", "")
        native_tools = prepared.get("native_tools") or []
        payload = prepared.get("payload") or {}
        request_contract_audit = prepared.get("request_contract_audit") or {}
        provider_request_envelope = prepared.get("provider_request_envelope") or {}
        start_time = time.time()
        last_error = None
        max_attempts = min(3, max(1, 1 + self.cfg.get_handshake_retry()))
        for attempt in range(max_attempts):
            try:
                self._raise_if_cancelled()
                self._wait_before_provider_call(
                    step=step,
                    tier=tier,
                    provider=provider,
                    model_name=model_name,
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    payload=payload,
                )
                self._log_api_progress(
                    "start",
                    step=step,
                    tier=tier,
                    provider=provider,
                    model_name=model_name,
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    payload=payload,
                )
                try:
                    response_data = self._send_request_cancellable(
                        request_url,
                        api_key,
                        payload,
                        provider,
                    )
                finally:
                    self._mark_provider_call_finished()
                latency_ms = int((time.time() - start_time) * 1000)
                envelopes = extract_tool_call_envelopes(
                    response_data,
                    provider=provider,
                    endpoint=health_endpoint,
                )
                response_text = self._response_text(response_data)
                if self._native_tool_empty_output(native_tools, response_text, envelopes):
                    error_message = self._provider_format_error_message(
                        "provider_native_tool_empty_output",
                        (
                            "native tools requested but provider returned no text "
                            "and no tool calls"
                        ),
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                    )
                    self._log_api_progress(
                        "empty",
                        step=step,
                        tier=tier,
                        provider=provider,
                        model_name=model_name,
                        attempt=attempt + 1,
                        max_attempts=max_attempts,
                        payload=payload,
                        detail="action=raise",
                    )
                    raise APIBridgeError(tier, error_message)
                breaker.record_success()
                self._log_connectivity(health_endpoint, "ok", "")
                self._log_api_progress(
                    "ok",
                    step=step,
                    tier=tier,
                    provider=provider,
                    model_name=model_name,
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    payload=payload,
                    detail=(
                        f"latency_ms={latency_ms} "
                        f"tool_calls={len(envelopes or [])} "
                        f"response_chars={len(response_text or '')}"
                    ),
                )
                return {
                    "response": response_text,
                    "tool_call_envelopes": envelopes,
                    "provider_response_meta": build_provider_response_meta(
                        response_data,
                        provider=provider,
                        envelopes=envelopes,
                    ),
                    "model": model_name,
                    "endpoint": health_endpoint,
                    "tokens_input": self._usage_input_tokens(response_data),
                    "tokens_output": self._usage_output_tokens(response_data),
                    "latency_ms": latency_ms,
                    "raw_usage": response_data.get("usage", {}),
                    "request_contract_audit": request_contract_audit,
                    "provider_request_envelope": provider_request_envelope,
                }
            except Exception as e:
                if self._is_api_timeout_error(e):
                    e = self._as_api_timeout_error(e, tier)
                elif isinstance(e, ConnectionError):
                    e = APIBridgeError(tier, f"网络错误: {e}")
                last_error = e
                self._log_api_progress(
                    "error",
                    step=step,
                    tier=tier,
                    provider=provider,
                    model_name=model_name,
                    attempt=attempt + 1,
                    max_attempts=max_attempts,
                    payload=payload,
                    detail=f"type={type(e).__name__} message={str(e)[:160]}",
                )
                if not isinstance(e, APIBridgeError):
                    raise
                if not self._is_transient_provider_error(e):
                    breaker.record_failure()
                    self._log_connectivity(health_endpoint, "error", str(e))
                    raise
                if attempt < max_attempts - 1:
                    self._sleep_between_provider_attempts(attempt + 1)
                    continue
                break

        breaker.record_failure()
        connectivity_status = (
            "timeout" if self._is_api_timeout_error(last_error) else "error"
        )
        self._log_connectivity(health_endpoint, connectivity_status, str(last_error))

        tried_fingerprints = {
            tuple(item)
            for item in prepared.get("_tried_endpoint_fingerprints", [])
        }
        tried_fingerprints.add(self._endpoint_fingerprint(ep))
        fallback_tier = self._fallback_tier(
            tier,
            current_endpoint=ep,
            excluded_fingerprints=tried_fingerprints,
            step=step,
        )
        if fallback_tier and allow_fallback:
            fallback_prepared = self.prepare_provider_request(
                step,
                system_prompt,
                messages,
                model=model,
                endpoint=fallback_tier,
                active_protocol_tool_guides=active_protocol_tool_guides,
                attempt=int(prepared.get("attempt") or 1) + 1,
            )
            fallback_prepared["_tried_endpoint_fingerprints"] = list(
                tried_fingerprints
            )
            return self.call_prepared(fallback_prepared)

        raise last_error

    def call(self, step, system_prompt, messages, model=None, endpoint=None,
             active_protocol_tool_guides=None):
        prepared = self.prepare_provider_request(
            step,
            system_prompt,
            messages,
            model=model,
            endpoint=endpoint,
            active_protocol_tool_guides=active_protocol_tool_guides,
        )
        return self.call_prepared(prepared)

    # ==============================================================
    # 端点选择
    # ==============================================================

    def _select_tier(self, step):
        """选择当前阶段有效模型链的首项。"""
        try:
            api_cfg = self.cfg.load("api")
        except Exception:
            return None

        step_routes = api_cfg.get("step_routes", {})
        route = step_routes.get(step, []) if isinstance(step_routes, dict) else []
        endpoints = api_cfg.get("endpoints", {})
        for candidate in route:
            if endpoints.get(candidate, {}).get("url"):
                return candidate

        # 兼容进程级旧格式覆盖。
        step_tiers = api_cfg.get("step_tiers", {})
        tier = step_tiers.get(step, "primary")
        if endpoints.get(tier, {}).get("url"):
            return tier

        # 指定 tier 无 URL，回退到 primary
        if endpoints.get("primary", {}).get("url"):
            return "primary"

        return None

    def _fallback_tier(
            self,
            tier,
            current_endpoint=None,
            excluded_fingerprints=None,
            step=None):
        """沿当前阶段冻结的有效模型链选择下一模型。"""
        try:
            api_cfg = self.cfg.load("api")
        except Exception:
            api_cfg = {}
        route_map = api_cfg.get("step_routes") or {}
        chain = list(route_map.get(step) or []) if step else []
        if not chain:
            chain = ["primary", "fallback", "emergency"]
        excluded = {
            tuple(item) for item in (excluded_fingerprints or [])
        }
        if current_endpoint is not None:
            excluded.add(self._endpoint_fingerprint(current_endpoint))
        try:
            idx = chain.index(tier)
            for candidate in chain[idx + 1:]:
                endpoint = self._get_endpoint(candidate)
                if not endpoint.get("url"):
                    continue
                if self._endpoint_fingerprint(endpoint) in excluded:
                    continue
                return candidate
            return None
        except ValueError:
            return None

    @staticmethod
    def _endpoint_fingerprint(endpoint):
        if not isinstance(endpoint, dict):
            return ("", "", "", "", "")
        return (
            str(endpoint.get("url") or "").strip().rstrip("/").lower(),
            str(endpoint.get("model") or "").strip(),
            str(endpoint.get("provider") or "").strip().lower(),
            str(endpoint.get("api_format") or "").strip().lower(),
            str(endpoint.get("api_key_env") or endpoint.get("api_key") or "").strip(),
        )

    def _get_endpoint(self, tier):
        try:
            api_cfg = self.cfg.load("api")
            endpoint = api_cfg.get("endpoints", {}).get(tier, {}) or {}
            if not endpoint.get("url"):
                return {}
            return endpoint
        except Exception:
            return {}

    def _get_api_key(self, endpoint):
        """从配置或环境变量获取 API key（优先环境变量）"""
        # 先试环境变量
        env_var = endpoint.get("api_key_env", "")
        if env_var:
            key = os.environ.get(env_var, "")
            if key:
                return key
        # 再试配置文件里的 api_key 字段
        key = endpoint.get("api_key", "")
        if key:
            return key
        # 最后试 _env_map 里的 key
        for ek in ["DEEPSEEK_API_KEY", "OPENAI_API_KEY", "UPSP_API_KEY"]:
            key = os.environ.get(ek, "")
            if key:
                return key
        return ""

    def _get_breaker(self, tier, endpoint=None):
        endpoint = endpoint or self._get_endpoint(tier)
        breaker_key = str(endpoint.get("profile_id") or tier)
        if breaker_key not in self.breakers:
            try:
                api_cfg = self.cfg.load("api")
                cb = api_cfg.get("circuit_breaker", {})
                self.breakers[breaker_key] = CircuitBreaker(
                    max_failures=cb.get("max_failures", 3),
                    cooldown_seconds=cb.get("cooldown_seconds", 900),
                )
            except Exception:
                self.breakers[breaker_key] = CircuitBreaker()
        return self.breakers[breaker_key]

    # ==============================================================
    # HTTP 请求
    # ==============================================================

    def _uses_responses_api(self, url):
        return str(url or "").rstrip("/").endswith("/responses")

    @staticmethod
    def _resolved_request_url(url, provider):
        text = str(url or "").strip().rstrip("/")
        if not text:
            return text
        if text.endswith("/v1"):
            if provider == "openai_responses":
                return f"{text}/responses"
            if provider == "openai_chat":
                return f"{text}/chat/completions"
            if provider == "anthropic_messages":
                return f"{text}/messages"
        return text

    def _messages_to_input_text(self, messages):
        parts = []
        for message in messages or []:
            normalized = self._strip_native_message_keys(message)
            role = str(normalized.get("role") or "user").strip() or "user"
            content = str(normalized.get("content") or "")
            parts.append(f"{role}: {content}")
        return "\n\n".join(parts)

    def _build_payload(self, url, model_name, system_prompt, messages, tools=None,
                       provider=None, endpoint_config=None,
                       step=None, prompt_cache_scope=None):
        if provider == "openai_responses" or self._uses_responses_api(url):
            payload = {
                "model": model_name,
                "input": self._responses_input(messages, provider),
                "max_output_tokens": 4096,
                "reasoning": {"effort": "none"},
                "text": {
                    "format": {"type": "text"},
                    "verbosity": "low",
                },
                "temperature": 0.7,
            }
            if str(system_prompt or "").strip():
                payload["instructions"] = str(system_prompt)
            if tools:
                payload["tools"] = tools
            self._apply_prompt_cache_config(
                payload,
                "openai_responses",
                step,
                endpoint_config,
                prompt_cache_scope=prompt_cache_scope,
                model_name=model_name,
            )
            self._apply_endpoint_payload_overrides(payload, endpoint_config)
            return payload

        if provider == "anthropic_messages":
            anthropic_system, anthropic_messages = self._anthropic_messages(
                messages,
                system_prompt=system_prompt,
            )
            payload = {
                "model": model_name,
                "messages": anthropic_messages,
                "max_tokens": 4096,
            }
            if anthropic_system:
                payload["system"] = anthropic_system
            if tools:
                payload["tools"] = tools
            self._apply_prompt_cache_config(
                payload,
                "anthropic_messages",
                step,
                endpoint_config,
                prompt_cache_scope=prompt_cache_scope,
                model_name=model_name,
            )
            self._apply_endpoint_payload_overrides(payload, endpoint_config)
            return payload

        payload_messages = self._chat_messages(
            messages,
            provider,
            model_name=model_name,
            endpoint_config=endpoint_config,
        )
        if str(system_prompt or "").strip():
            payload_messages = [{"role": "system", "content": system_prompt}] + payload_messages
        payload = {
            "model": model_name,
            "messages": payload_messages,
            "temperature": 0.7,
        }
        if tools:
            payload["tools"] = tools
        self._apply_prompt_cache_config(
            payload,
            provider or "openai_chat",
            step,
            endpoint_config,
            prompt_cache_scope=prompt_cache_scope,
            model_name=model_name,
        )
        self._apply_endpoint_payload_overrides(payload, endpoint_config)
        settings = self._prompt_cache_settings(
            step,
            provider or "openai_chat",
            endpoint_config,
            prompt_cache_scope=prompt_cache_scope,
            model_name=model_name,
        )
        if settings.get("profile") in EXPLICIT_PROFILES:
            payload_message_layers = ["50_now"] * len(payload_messages)
            if str(system_prompt or "").strip():
                payload_message_layers[0] = "10_permanent"
            payload, _plan = apply_explicit_breakpoints(
                payload,
                profile=settings["profile"],
                message_layers=payload_message_layers,
                layer_contents={},
                promoted_min_chars=4096,
            )
        return payload

    @classmethod
    def _apply_streaming_config_to_payload(cls, payload, endpoint_config, provider):
        if not isinstance(payload, dict):
            return payload
        settings = cls._streaming_settings(endpoint_config, provider)
        if not settings.get("enabled"):
            return payload
        updated = dict(payload)
        updated["stream"] = True
        if provider == "openai_chat" and settings.get("include_usage"):
            options = updated.get("stream_options")
            options = dict(options) if isinstance(options, dict) else {}
            options["include_usage"] = True
            updated["stream_options"] = options
        return updated

    @staticmethod
    def _streaming_settings(endpoint_config, provider):
        if provider not in {"openai_chat", "openai_responses", "anthropic_messages"}:
            return {"enabled": False}
        if not isinstance(endpoint_config, dict):
            return {"enabled": False}
        streaming = endpoint_config.get("streaming") or {}
        if not isinstance(streaming, dict):
            return {"enabled": False}
        protocol = str(streaming.get("protocol") or "openai_sse").strip().lower()
        if protocol != "openai_sse":
            return {"enabled": False}
        return {
            "enabled": streaming.get("enabled") is True,
            "protocol": protocol,
            "include_usage": streaming.get("include_usage", True) is not False,
        }

    @staticmethod
    def _is_api_timeout_error(exc):
        if isinstance(exc, APITimeoutError):
            return True
        if isinstance(exc, (TimeoutError, socket.timeout)):
            return True
        reason = getattr(exc, "reason", None)
        if isinstance(reason, (TimeoutError, socket.timeout)):
            return True
        text = str(exc or "").lower()
        return "timed out" in text or "timeout" in text

    @classmethod
    def _is_transient_provider_error(cls, exc):
        if cls._is_api_timeout_error(exc):
            return True
        try:
            status_code = int(getattr(exc, "status_code", None))
        except (TypeError, ValueError):
            status_code = 0
        if status_code in {408, 429} or 500 <= status_code <= 599:
            return True
        text = str(exc or "").lower()
        return any(marker in text for marker in (
            "网络错误:",
            "provider_stream_interrupted",
            "provider_native_tool_empty_output",
        ))

    def _as_api_timeout_error(self, exc, endpoint):
        if isinstance(exc, APITimeoutError):
            return exc
        try:
            timeout = self.cfg.get_request_timeout()
        except Exception:
            timeout = 180
        return APITimeoutError(
            endpoint,
            f"API timeout after {timeout} seconds: {exc}",
            timeout_seconds=timeout,
        )

    @staticmethod
    def _stream_first_chunk_timeout_error(url, timeout_seconds, exc):
        return APITimeoutError(
            url,
            "provider_stream_first_chunk_timeout: "
            f"no HTTP/SSE response before first chunk within {timeout_seconds} seconds: {exc}",
            timeout_seconds=timeout_seconds,
        )

    @classmethod
    def _provider_format_error_message(
            cls,
            kind,
            message,
            *,
            attempt=None,
            max_attempts=None):
        parts = [
            f"{kind}:",
            cls.PROVIDER_FORMAT_INSTABILITY,
            f"kind={kind}",
        ]
        if attempt is not None and max_attempts is not None:
            parts.append(f"attempt={attempt}/{max_attempts}")
        parts.append(str(message or "").strip())
        return " ".join(part for part in parts if part)

    @classmethod
    def _load_provider_call_interval_seconds(cls):
        raw = os.environ.get("UPSP_PROVIDER_CALL_INTERVAL_SECONDS")
        if raw is None or str(raw).strip() == "":
            return cls.DEFAULT_PROVIDER_CALL_INTERVAL_SECONDS
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return cls.DEFAULT_PROVIDER_CALL_INTERVAL_SECONDS
        if value < 0:
            return cls.DEFAULT_PROVIDER_CALL_INTERVAL_SECONDS
        return value

    def _wait_before_provider_call(self, *, step, tier, provider, model_name,
                                   attempt, max_attempts, payload):
        self._raise_if_cancelled()
        interval = max(0.0, float(self._provider_call_interval_seconds or 0.0))
        if interval <= 0 or self._last_provider_call_finished_at is None:
            return
        now = float(self._monotonic())
        elapsed = now - float(self._last_provider_call_finished_at)
        wait_seconds = max(0.0, interval - elapsed)
        if wait_seconds <= 0:
            return
        self._log_api_progress(
            "throttle",
            step=step,
            tier=tier,
            provider=provider,
            model_name=model_name,
            attempt=attempt,
            max_attempts=max_attempts,
            payload=payload,
            detail=f"wait_seconds={wait_seconds:.1f}",
        )
        self._interruptible_sleep(wait_seconds)

    def _mark_provider_call_finished(self):
        self._last_provider_call_finished_at = float(self._monotonic())

    def _sleep_between_provider_attempts(self, seconds):
        wait_seconds = max(0.0, float(seconds or 0.0))
        if wait_seconds <= 0:
            return
        before = float(self._monotonic())
        self._interruptible_sleep(wait_seconds)
        after = float(self._monotonic())
        observed_elapsed = max(0.0, after - before)
        missing_elapsed = max(0.0, wait_seconds - observed_elapsed)
        if (
                missing_elapsed > 0
                and self._last_provider_call_finished_at is not None):
            self._last_provider_call_finished_at -= missing_elapsed

    def _interruptible_sleep(self, seconds):
        wait_seconds = max(0.0, float(seconds or 0.0))
        if wait_seconds <= 0:
            self._raise_if_cancelled()
            return
        if self._sleep is self._default_sleep and time.sleep is _SYSTEM_SLEEP:
            if self._cancel_event.wait(wait_seconds):
                raise ProviderCallCancelled()
            return
        self._sleep(wait_seconds)
        self._raise_if_cancelled()

    @staticmethod
    def _apply_endpoint_payload_overrides(payload, endpoint_config=None):
        if not isinstance(endpoint_config, dict):
            return
        reserved = {
            "model",
            "messages",
            "input",
            "instructions",
            "system",
            "tools",
            "tool_choice",
            "parallel_tool_calls",
            "max_tokens",
            "prompt_cache_key",
            "prompt_cache_options",
            "prompt_cache_retention",
        }
        overrides = {}
        for field in ("extra_body", "payload_overrides"):
            value = endpoint_config.get(field)
            if isinstance(value, dict):
                overrides.update(value)
        prompt_cache = endpoint_config.get("prompt_cache")
        profile_configured = (
            isinstance(prompt_cache, dict)
            and "profile" in prompt_cache
        )
        cache_override_fields = sorted({
            "prompt_cache_key",
            "prompt_cache_options",
            "prompt_cache_retention",
        } & set(overrides))
        if profile_configured and cache_override_fields:
            raise ValueError(
                "prompt_cache_payload_override_conflict:"
                + ",".join(cache_override_fields)
            )
        for field in ("thinking", "reasoning_effort"):
            if field in endpoint_config:
                overrides[field] = endpoint_config[field]
        for key, value in overrides.items():
            if key in reserved:
                continue
            payload[key] = value

    @classmethod
    def _apply_prompt_cache_config(cls, payload, provider, step,
                                   endpoint_config=None,
                                   prompt_cache_scope=None,
                                   model_name=""):
        settings = cls._prompt_cache_settings(
            step,
            provider,
            endpoint_config,
            prompt_cache_scope=prompt_cache_scope,
            model_name=model_name,
        )
        if not settings:
            return
        if provider not in cls.OPENAI_PROMPT_CACHE_PROVIDERS:
            return
        payload["prompt_cache_key"] = settings["key"]
        if settings.get("mode") == "explicit":
            payload["prompt_cache_options"] = {
                "mode": "explicit",
                "ttl": settings.get("ttl") or "30m",
            }
        if settings.get("retention"):
            payload["prompt_cache_retention"] = settings["retention"]

    @classmethod
    def _prompt_cache_settings(cls, step, provider, endpoint_config=None,
                               prompt_cache_scope=None,
                               model_name=""):
        if not isinstance(endpoint_config, dict):
            return {}
        raw = endpoint_config.get("prompt_cache")
        if raw is True:
            cfg = {}
        elif isinstance(raw, dict):
            if raw.get("enabled") is False:
                return {}
            cfg = raw
        else:
            return {}

        normalized_step = str(step or "").strip().lower()
        normalized_scope = cls._normalize_prompt_cache_scope(
            prompt_cache_scope or normalized_step
        )
        lanes = cfg.get("lanes")
        lanes = lanes if isinstance(lanes, dict) else {}
        lane = str(
            lanes.get(normalized_scope)
            or cfg.get(normalized_scope)
            or cls.PROMPT_CACHE_DEFAULT_LANES.get(normalized_scope)
            or normalized_scope
            or "default"
        ).strip()
        if not lane:
            return {}

        resolved_profile = profile_settings(
            cfg,
            provider=provider,
            model_name=(
                model_name
                or str(endpoint_config.get("model") or "")
            ),
            lane=lane,
        )
        if resolved_profile is not None:
            return resolved_profile

        key = str(cfg.get("key") or "").strip()
        if not key:
            prefix = str(
                cfg.get("key_prefix")
                or cfg.get("prefix")
                or ""
            ).strip()
            key = f"{prefix}:{lane}" if prefix else lane

        settings = {
            "lane": lane,
            "key": key,
            "applied": provider in cls.OPENAI_PROMPT_CACHE_PROVIDERS,
        }
        retention = str(cfg.get("retention") or "").strip()
        if retention:
            settings["retention"] = retention
        return settings

    @classmethod
    def _prompt_cache_scope_for_channel(cls, channel):
        if getattr(channel, "name", "") == "final_reply":
            return "reaction.final_reply"
        return str(getattr(channel, "name", "") or "").strip().lower()

    @staticmethod
    def _normalize_prompt_cache_scope(scope):
        normalized = str(scope or "").strip().lower()
        if normalized == "reaction":
            return "reaction.loop"
        if normalized == "final_reply":
            return "reaction.final_reply"
        return normalized

    def _responses_input(self, messages, provider):
        return self._messages_to_input_text(messages)

    def _chat_messages(self, messages, provider, *, model_name="",
                       endpoint_config=None):
        expand_reasoning_replay = self._should_expand_reasoning_native_replay(
            provider,
            model_name=model_name,
            endpoint_config=endpoint_config,
        )
        payload_messages = []
        for message in messages or []:
            if expand_reasoning_replay:
                native_replay_messages = self._native_replay_messages_for_provider(
                    message,
                    provider,
                )
                if native_replay_messages:
                    payload_messages.extend(native_replay_messages)
                    continue
            payload_messages.append(self._strip_native_message_keys(message))
        if provider == "openai_chat" and payload_messages:
            has_user = any(
                str(message.get("role") or "user").strip() == "user"
                for message in payload_messages
                if isinstance(message, dict)
            )
            if not has_user:
                payload_messages.append({
                    "role": "user",
                    "content": (
                        "【Host query】请根据以上上下文生成当前阶段所需的自然语言输出。"
                    ),
                })
        return payload_messages

    @staticmethod
    def _should_expand_reasoning_native_replay(
            provider, *, model_name="", endpoint_config=None):
        if provider != "openai_chat":
            return False
        endpoint_config = endpoint_config if isinstance(endpoint_config, dict) else {}
        model_lc = str(model_name or "").lower()
        extra_body = endpoint_config.get("extra_body")
        thinking_enabled = (
            isinstance(extra_body, dict)
            and extra_body.get("enable_thinking") is True
        )
        return (
            "deepseek" in model_lc
            or endpoint_config.get("reasoning_replay_enabled") is True
            or thinking_enabled
        )

    @classmethod
    def _native_replay_messages_for_provider(cls, message, provider):
        if provider != "openai_chat":
            return []
        if not isinstance(message, dict):
            return []
        native_replay = message.get("native_replay")
        if not isinstance(native_replay, dict):
            return []
        replay_provider = str(native_replay.get("provider") or "").strip()
        if replay_provider and replay_provider != "openai_chat":
            return []
        assistant = native_replay.get("assistant_message")
        if not isinstance(assistant, dict):
            return []
        replay_messages = []
        assistant_message = cls._sanitize_native_replay_assistant_message(
            assistant)
        if assistant_message:
            replay_messages.append(assistant_message)
        for item in native_replay.get("tool_results") or []:
            tool_message = cls._sanitize_native_replay_tool_message(item)
            if tool_message:
                replay_messages.append(tool_message)
        return replay_messages

    @staticmethod
    def _sanitize_native_replay_assistant_message(message):
        if not isinstance(message, dict):
            return {}
        tool_calls = message.get("tool_calls")
        if not isinstance(tool_calls, list):
            tool_calls = []
        sanitized = {
            "role": "assistant",
            "content": message.get("content") or "",
        }
        reasoning_content = str(message.get("reasoning_content") or "").strip()
        if reasoning_content:
            sanitized["reasoning_content"] = reasoning_content
        if tool_calls:
            sanitized["tool_calls"] = [
                item for item in tool_calls if isinstance(item, dict)
            ]
        if "reasoning_content" not in sanitized and "tool_calls" not in sanitized:
            return {}
        return sanitized

    @staticmethod
    def _sanitize_native_replay_tool_message(message):
        if not isinstance(message, dict):
            return {}
        tool_call_id = str(message.get("tool_call_id") or "").strip()
        if not tool_call_id:
            return {}
        content = message.get("content")
        if isinstance(content, (dict, list)):
            content = json.dumps(content, ensure_ascii=False, sort_keys=True)
        return {
            "role": "tool",
            "tool_call_id": tool_call_id,
            "content": str(content or ""),
        }

    def _anthropic_messages(self, messages, system_prompt=""):
        system_parts = []
        if str(system_prompt or "").strip():
            system_parts.append(str(system_prompt).strip())
        payload_messages = []
        dialogue_started = False
        for message in messages or []:
            normalized = self._strip_native_message_keys(message)
            role = str(normalized.get("role") or "user").strip() or "user"
            content = normalized.get("content")
            if role in {"system", "developer"}:
                text = self._anthropic_content_text(content)
                if text and not dialogue_started:
                    system_parts.append(text)
                elif text:
                    payload_messages.append({
                        "role": "user",
                        "content": text,
                    })
                    dialogue_started = True
                continue
            if role not in {"user", "assistant"}:
                role = "user"
            payload_messages.append({
                "role": role,
                "content": self._anthropic_content(content),
            })
            dialogue_started = True
        if not payload_messages:
            payload_messages.append({"role": "user", "content": "继续。"})
        return "\n\n".join(system_parts), payload_messages

    @staticmethod
    def _anthropic_content(content):
        if isinstance(content, list):
            return content
        return str(content or "")

    @staticmethod
    def _anthropic_content_text(content):
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict):
                    if item.get("type") == "text" and item.get("text"):
                        parts.append(str(item.get("text")))
                elif item:
                    parts.append(str(item))
            return "\n".join(parts).strip()
        return str(content or "").strip()

    @staticmethod
    def _strip_native_message_keys(message):
        if not isinstance(message, dict):
            return {"role": "user", "content": str(message)}
        role = str(message.get("role") or "user").strip() or "user"
        if role == "tool":
            return {
                "role": "system",
                "content": (
                    "【Runtime 工具结果占位】"
                    "工具结果已由 Runtime 结构化回执处理；此处不作为用户输入。"
                ),
            }
        return {
            key: value
            for key, value in message.items()
            if not key.startswith("native_")
            and key not in {
                "function_call_output",
                "provider_native_tool_result",
                "tool_call_id",
                "kind",
                "active_corpus_id",
                "ref",
            }
        }

    def _native_tools_for_step(
            self,
            step,
            provider,
            active_protocol_tool_guides=None,
            execution_permission_level=None):
        channel = channel_for_step(
            step,
            active_protocol_tool_guides=active_protocol_tool_guides,
        )
        terminal_tools = [channel.terminal_tool] if channel.terminal_tool else []
        if not channel.include_standard_tools:
            schemas = export_provider_tool_schemas(
                provider=provider,
                include_step_terminal_tools=terminal_tools,
                include_standard_tools=False,
                execution_permission_level=execution_permission_level,
            )
        else:
            schemas = export_provider_tool_schemas(
                provider=provider,
                include_protocol_writes=channel.include_protocol_writes,
                include_step_terminal_tools=terminal_tools,
                active_protocol_tool_guides=active_protocol_tool_guides,
                execution_permission_level=execution_permission_level,
            )
        from logic.single_round_probe_policy import filter_provider_tool_schemas
        return filter_provider_tool_schemas(step, schemas)

    @staticmethod
    def _native_tool_schema_name(tool):
        if not isinstance(tool, dict):
            return ""
        name = str(tool.get("name") or "").strip()
        if name:
            return name
        function = tool.get("function")
        if isinstance(function, dict):
            return str(function.get("name") or "").strip()
        return ""

    @classmethod
    def _request_contract_audit(cls, step, provider, model_name, native_tools, payload,
                                channel=None, endpoint_config=None,
                                prompt_cache_scope=None,
                                prompt_cache_plan=None):
        step = str(step or "").strip().lower()
        channel = channel or channel_for_step(step)
        tool_names = [
            cls._native_tool_schema_name(tool)
            for tool in native_tools or []
            if cls._native_tool_schema_name(tool)
        ]
        payload = payload if isinstance(payload, dict) else {}
        tool_mode = str(getattr(channel, "tool_mode", "") or "").strip() or None
        audit = {
            "step": step,
            "provider": provider,
            "model": model_name,
            "tool_names": tool_names,
            "terminal_tool": getattr(channel, "terminal_tool", "") or None,
            "tool_mode": tool_mode,
            "tools_transmitted": bool(payload.get("tools")),
            "standard_tools_enabled": bool(
                getattr(channel, "include_standard_tools", False)
            ),
        }
        settings = cls._prompt_cache_settings(
            step,
            provider,
            endpoint_config,
            prompt_cache_scope=prompt_cache_scope,
            model_name=model_name,
        )
        payload_key = str(payload.get("prompt_cache_key") or "").strip()
        payload_retention = str(
            payload.get("prompt_cache_retention") or ""
        ).strip()
        if settings or payload_key or payload_retention:
            audit["prompt_cache_lane"] = settings.get("lane")
            audit["prompt_cache_key"] = payload_key or settings.get("key")
            audit["prompt_cache_key_applied"] = bool(payload_key)
            if settings.get("profile"):
                audit["prompt_cache_profile"] = settings.get("profile")
            if settings.get("mode"):
                audit["prompt_cache_mode"] = settings.get("mode")
            retention = payload_retention or settings.get("retention")
            if retention:
                audit["prompt_cache_retention"] = retention
        if isinstance(prompt_cache_plan, dict) and prompt_cache_plan:
            audit["prompt_cache_plan"] = dict(prompt_cache_plan)
        return audit

    @staticmethod
    def _api_progress_enabled():
        value = str(os.environ.get("UPSP_VISIBLE_API_PROGRESS") or "").strip().lower()
        return value in {"1", "true", "yes", "on"}

    @classmethod
    def _log_api_progress(cls, event, *, step, tier, provider, model_name,
                          attempt, max_attempts, payload, detail=""):
        if not cls._api_progress_enabled():
            return
        parts = [
            f"[UPSP API] {event}",
            f"step={step}",
            f"tier={tier}",
            f"provider={provider}",
            f"model={model_name}",
            f"attempt={attempt}/{max_attempts}",
        ]
        if detail:
            parts.append(str(detail).replace("\n", " "))
        try:
            print(" ".join(parts), file=sys.stderr, flush=True)
        except OSError:
            return

    def _response_text(self, response_data):
        choices = response_data.get("choices")
        if choices:
            message = choices[0].get("message", {}) or {}
            return message.get("content") or message.get("refusal") or ""

        output_text = response_data.get("output_text")
        if output_text:
            return str(output_text)

        content = response_data.get("content")
        if isinstance(content, list):
            parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text")
                    if text:
                        parts.append(str(text))
                elif isinstance(item, str):
                    parts.append(item)
            if parts:
                return "\n".join(parts)

        parts = []
        for item in response_data.get("output", []) or []:
            for content in item.get("content", []) or []:
                if isinstance(content, dict):
                    text = content.get("text") or content.get("refusal")
                    if text:
                        parts.append(str(text))
                elif isinstance(content, str):
                    parts.append(content)
        return "\n".join(parts)

    @staticmethod
    def _native_tool_empty_output(native_tools, response_text, envelopes):
        if not native_tools:
            return False
        if str(response_text or "").strip():
            return False
        if envelopes:
            return False
        return True

    def _usage_input_tokens(self, response_data):
        usage = response_data.get("usage", {}) or {}
        return usage.get("prompt_tokens", usage.get("input_tokens", 0))

    def _usage_output_tokens(self, response_data):
        usage = response_data.get("usage", {}) or {}
        return usage.get("completion_tokens", usage.get("output_tokens", 0))

    def _send_request(self, url, api_key, payload):
        """发送 HTTP POST 请求到 API"""
        data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=data, method="POST")
        req.add_header("Content-Type", "application/json; charset=utf-8")
        req.add_header("Accept-Charset", "utf-8")
        streaming_requested = (
            isinstance(payload, dict)
            and payload.get("stream") is True
        )
        if streaming_requested:
            req.add_header("Accept", "text/event-stream")
        if api_key:
            req.add_header("Authorization", f"Bearer {api_key}")
            if str(url or "").split("?", 1)[0].rstrip("/").endswith("/messages"):
                req.add_header("x-api-key", api_key)
                req.add_header("anthropic-version", "2023-06-01")
        req.add_header("User-Agent", "UPSP-Base/2.0")

        try:
            timeout = self.cfg.get_request_timeout()
        except Exception:
            timeout = 180
        open_timeout = (
            self.cfg.get_stream_first_chunk_timeout()
            if streaming_requested
            else timeout
        )

        try:
            with urllib.request.urlopen(req, timeout=open_timeout) as resp:
                if streaming_requested:
                    return self._read_openai_sse_or_json_response(
                        resp,
                        url,
                        first_chunk_timeout=open_timeout,
                        idle_timeout=self.cfg.get_stream_idle_timeout(),
                    )
                try:
                    body = resp.read().decode("utf-8")
                except (TimeoutError, socket.timeout) as e:
                    raise APITimeoutError(
                        url,
                        f"API response read timed out after {timeout} seconds: {e}",
                        timeout_seconds=timeout,
                    ) from e
                return self._decode_response_body(body)
        except urllib.error.HTTPError as e:
            body = e.read().decode("utf-8", errors="replace")[:500]
            raise APIBridgeError(url, f"HTTP {e.code}: {body}", status_code=e.code)
        except urllib.error.URLError as e:
            if self._is_api_timeout_error(e):
                if streaming_requested:
                    raise self._stream_first_chunk_timeout_error(
                        url,
                        open_timeout,
                        getattr(e, "reason", e),
                    ) from e
                raise APITimeoutError(
                    url,
                    f"API request timed out after {timeout} seconds: {e.reason}",
                    timeout_seconds=timeout,
                ) from e
            raise APIBridgeError(url, f"网络错误: {e.reason}")
        except (TimeoutError, socket.timeout) as e:
            if streaming_requested:
                raise self._stream_first_chunk_timeout_error(
                    url,
                    open_timeout,
                    e,
                ) from e
            raise APITimeoutError(
                url,
                f"API request timed out after {timeout} seconds: {e}",
                timeout_seconds=timeout,
            ) from e
        except json.JSONDecodeError:
            raise APIBridgeError(url, "JSON 解析失败")

    def _send_request_cancellable(self, url, api_key, payload, provider=None):
        """Use a worker only for the unmodified production transport method.

        Existing deterministic tests and adapters that replace ``_send_request``
        remain in-process; the real urllib implementation is the killable path.
        """
        self._raise_if_cancelled()
        bound = self._send_request
        if getattr(bound, "__func__", None) is not APIExecutor._send_request:
            return bound(url, api_key, payload)
        request = {
            "url": str(url or ""),
            "api_key": str(api_key or ""),
            "payload": payload,
            "provider": str(provider or provider_for_url(url)),
            "timeouts": {
                "request_timeout": self.cfg.get_request_timeout(),
                "first_chunk_timeout": self.cfg.get_stream_first_chunk_timeout(),
                "idle_timeout": self.cfg.get_stream_idle_timeout(),
                "content_overrun_chars": self.cfg.get_stream_content_overrun_chars(),
            },
        }
        context = multiprocessing.get_context("spawn")
        receiver, sender = context.Pipe(duplex=False)
        process = context.Process(
            target=_provider_transport_worker,
            args=(sender, request),
            name="upsp-provider-transport",
        )
        process.daemon = False
        terminal = None
        try:
            process.start()
            sender.close()
            with self._transport_lock:
                self._active_transport_process = process
            while True:
                if self.cancellation_requested:
                    self._terminate_transport(process)
                    raise ProviderCallCancelled()
                if receiver.poll(0.05):
                    try:
                        message = receiver.recv()
                    except EOFError:
                        break
                    if message.get("type") == "event":
                        self._emit_stream_event(
                            message.get("event_type"),
                            message.get("payload"),
                        )
                        continue
                    terminal = message
                    break
                if not process.is_alive():
                    break
            process.join(timeout=2)
            if self.cancellation_requested:
                raise ProviderCallCancelled()
            if not isinstance(terminal, dict):
                raise APIBridgeError(
                    url,
                    f"provider_transport_worker_exited:{process.exitcode}",
                )
            if terminal.get("type") == "result":
                return terminal.get("response")
            error_kind = terminal.get("kind")
            if error_kind == "timeout":
                raise APITimeoutError(
                    terminal.get("endpoint") or url,
                    terminal.get("message"),
                    timeout_seconds=terminal.get("timeout_seconds"),
                )
            raise APIBridgeError(
                terminal.get("endpoint") or url,
                terminal.get("message") or "provider_transport_worker_failed",
                status_code=terminal.get("status_code"),
            )
        finally:
            with self._transport_lock:
                if self._active_transport_process is process:
                    self._active_transport_process = None
            self._terminate_transport(process)
            receiver.close()
            try:
                sender.close()
            except OSError:
                pass

    @staticmethod
    def _terminate_transport(process):
        if process is None or process.pid is None:
            return
        if process.is_alive():
            process.terminate()
            process.join(timeout=1.5)
        if process.is_alive() and hasattr(process, "kill"):
            process.kill()
            process.join(timeout=0.5)

    def _read_openai_sse_or_json_response(
            self,
            resp,
            url,
            *,
            provider=None,
            first_chunk_timeout,
            idle_timeout):
        content_type = self._response_content_type(resp)
        if content_type and "text/event-stream" not in content_type:
            body = self._read_plain_json_response(resp)
            return self._decode_response_body(body)
        try:
            return self._read_provider_sse_response(
                resp,
                url,
                provider=(
                    provider
                    or getattr(self, "_transport_provider", "")
                    or provider_for_url(url)
                ),
                first_chunk_timeout=first_chunk_timeout,
                idle_timeout=idle_timeout,
            )
        except json.JSONDecodeError as exc:
            raise APIBridgeError(
                url,
                f"provider_stream_interrupted: invalid SSE JSON: {exc}",
            ) from exc

    @staticmethod
    def _response_content_type(resp):
        headers = getattr(resp, "headers", None)
        if isinstance(headers, dict):
            return str(headers.get("Content-Type") or headers.get("content-type") or "").lower()
        getter = getattr(headers, "get", None)
        if callable(getter):
            return str(getter("Content-Type") or getter("content-type") or "").lower()
        return ""

    @staticmethod
    def _read_plain_json_response(resp):
        body = b""
        reader = getattr(resp, "read", None)
        if callable(reader):
            body = reader() or b""
        if not body:
            try:
                body = b"".join(
                    item if isinstance(item, bytes) else str(item).encode("utf-8")
                    for item in resp
                )
            except TypeError:
                body = b""
        return body.decode("utf-8")

    def _read_openai_sse_response(
            self,
            resp,
            url,
            *,
            first_chunk_timeout,
            idle_timeout):
        return self._read_provider_sse_response(
            resp,
            url,
            provider="openai_chat",
            first_chunk_timeout=first_chunk_timeout,
            idle_timeout=idle_timeout,
        )

    def _read_provider_sse_response(
            self,
            resp,
            url,
            *,
            provider,
            first_chunk_timeout,
            idle_timeout):
        self._set_response_socket_timeout(resp, idle_timeout)
        start = float(self._monotonic())
        first_chunk_at = None
        done_seen = False
        accumulator = self._stream_accumulator(provider)
        last_delta_emit_at = start
        last_delta_chars = 0
        last_data_at = start
        line_queue = queue.Queue(maxsize=1)

        def read_lines():
            try:
                for raw_line in resp:
                    line_queue.put(("line", raw_line))
            except BaseException as exc:
                line_queue.put(("error", exc))
            else:
                line_queue.put(("end", None))

        threading.Thread(
            target=read_lines,
            name="provider-sse-reader",
            daemon=True,
        ).start()

        try:
            while True:
                now = float(self._monotonic())
                timeout = first_chunk_timeout if first_chunk_at is None else idle_timeout
                timeout_started_at = start if first_chunk_at is None else last_data_at
                timeout_remaining = max(0.0, timeout - (now - timeout_started_at))
                summary = accumulator.summary()
                total_chars = (
                    int(summary.get("content_chars") or 0)
                    + int(summary.get("reasoning_chars") or 0)
                    + int(summary.get("tool_argument_chars") or 0)
                )
                flush_remaining = (
                    max(0.0, 0.5 - (now - last_delta_emit_at))
                    if total_chars > last_delta_chars
                    else timeout_remaining
                )
                try:
                    item_type, item = line_queue.get(
                        timeout=min(timeout_remaining, flush_remaining)
                    )
                except queue.Empty:
                    now = float(self._monotonic())
                    if total_chars > last_delta_chars and now - last_delta_emit_at >= 0.5:
                        self._emit_stream_progress(
                            "llm_stream_delta",
                            accumulator,
                            start,
                            first_chunk_at=first_chunk_at,
                            protocol=provider,
                        )
                        last_delta_emit_at = now
                        last_delta_chars = total_chars
                    if now - timeout_started_at >= timeout:
                        raise socket.timeout()
                    continue
                if item_type == "error":
                    raise item
                if item_type == "end":
                    break
                raw_line = item
                last_data_at = float(self._monotonic())
                line = self._decode_sse_line(raw_line)
                if not line:
                    continue
                if line.startswith(("event:", "id:", "retry:", ":")):
                    continue
                if not line.startswith("data:"):
                    plain_parts = [line]
                    while True:
                        item_type, item = line_queue.get(timeout=idle_timeout)
                        if item_type == "error":
                            raise item
                        if item_type == "end":
                            break
                        plain_parts.append(self._decode_sse_line(item))
                    plain_body = "".join(plain_parts)
                    return self._decode_response_body(plain_body)
                data = line[5:].strip()
                if not data:
                    continue
                now = float(self._monotonic())
                if provider == "openai_chat" and data == "[DONE]":
                    done_seen = True
                    break
                is_first_chunk = first_chunk_at is None
                chunk = json.loads(data)
                if self._stream_event_is_error(provider, chunk):
                    self._emit_stream_error(
                        "provider_stream_interrupted",
                        accumulator,
                        start,
                        first_chunk_at=first_chunk_at,
                        protocol=provider,
                    )
                    raise APIBridgeError(
                        url,
                        "provider_stream_interrupted: provider emitted an error event",
                    )
                done_seen = self._add_stream_chunk(provider, accumulator, chunk)
                if is_first_chunk:
                    first_chunk_at = now
                    self._emit_stream_progress(
                        "llm_stream_first_chunk",
                        accumulator,
                        start,
                        first_chunk_at=first_chunk_at,
                        protocol=provider,
                    )
                summary = accumulator.summary()
                plain_text_chars = (
                    int(summary.get("content_chars") or 0)
                    + int(summary.get("reasoning_chars") or 0)
                )
                content_overrun_chars = self.cfg.get_stream_content_overrun_chars()
                if (
                        content_overrun_chars > 0
                        and plain_text_chars > content_overrun_chars
                        and int(summary.get("tool_call_count") or 0) == 0):
                    self._emit_stream_error(
                        "provider_stream_content_overrun",
                        accumulator,
                        start,
                        first_chunk_at=first_chunk_at,
                        protocol=provider,
                    )
                    raise APIBridgeError(
                        url,
                        "provider_stream_content_overrun: "
                        f"plain assistant text exceeded {content_overrun_chars} chars "
                        "without tool calls",
                    )
                total_chars = (
                    int(summary.get("content_chars") or 0)
                    + int(summary.get("reasoning_chars") or 0)
                    + int(summary.get("tool_argument_chars") or 0)
                )
                if is_first_chunk:
                    last_delta_emit_at = now
                    last_delta_chars = total_chars
                if now - last_delta_emit_at >= 0.5 or total_chars - last_delta_chars >= 256:
                    self._emit_stream_progress(
                        "llm_stream_delta",
                        accumulator,
                        start,
                        first_chunk_at=first_chunk_at,
                        protocol=provider,
                    )
                    last_delta_emit_at = now
                    last_delta_chars = total_chars
                if done_seen:
                    break
        except (TimeoutError, socket.timeout) as exc:
            if first_chunk_at is None:
                self._emit_stream_error(
                    "provider_stream_first_chunk_timeout",
                    accumulator,
                    start,
                    first_chunk_at=first_chunk_at,
                    protocol=provider,
                )
                raise APITimeoutError(
                    url,
                    "provider_stream_first_chunk_timeout: "
                    f"no SSE chunk within {first_chunk_timeout} seconds",
                    timeout_seconds=first_chunk_timeout,
                ) from exc
            self._emit_stream_error(
                "provider_stream_idle_timeout",
                accumulator,
                start,
                first_chunk_at=first_chunk_at,
                protocol=provider,
            )
            raise APITimeoutError(
                url,
                "provider_stream_idle_timeout: "
                f"no SSE data within {idle_timeout} seconds",
                timeout_seconds=idle_timeout,
            ) from exc

        if not done_seen:
            self._emit_stream_error(
                "provider_stream_interrupted",
                accumulator,
                start,
                first_chunk_at=first_chunk_at,
                protocol=provider,
            )
            raise APIBridgeError(
                url,
                f"provider_stream_interrupted: {provider} SSE ended before terminal event",
            )
        try:
            accumulator.validate_tool_arguments()
        except ValueError as exc:
            self._emit_stream_error(
                "provider_stream_incomplete_tool_call",
                accumulator,
                start,
                first_chunk_at=first_chunk_at,
                protocol=provider,
            )
            raise APIBridgeError(
                url,
                f"provider_stream_incomplete_tool_call: {exc}",
            ) from exc
        self._emit_stream_progress(
            "llm_stream_done",
            accumulator,
            start,
            first_chunk_at=first_chunk_at,
            protocol=provider,
        )
        return self._stream_response(provider, accumulator)

    @staticmethod
    def _stream_accumulator(provider):
        if provider == "openai_responses":
            return OpenAIResponsesSSEAccumulator()
        if provider == "anthropic_messages":
            return AnthropicMessagesSSEAccumulator()
        return OpenAIChatSSEAccumulator()

    @staticmethod
    def _add_stream_chunk(provider, accumulator, chunk):
        if provider == "openai_chat":
            accumulator.add_chunk(chunk)
            return False
        return bool(accumulator.add_event(chunk))

    @staticmethod
    def _stream_event_is_error(provider, chunk):
        event_type = str((chunk or {}).get("type") or "")
        if provider == "openai_responses":
            return event_type in {"error", "response.failed", "response.incomplete"}
        if provider == "anthropic_messages":
            return event_type == "error"
        return False

    @staticmethod
    def _stream_response(provider, accumulator):
        if provider == "openai_responses":
            return accumulator.to_response()
        if provider == "anthropic_messages":
            return accumulator.to_message()
        return accumulator.to_chat_completion()

    @staticmethod
    def _decode_sse_line(raw_line):
        if isinstance(raw_line, bytes):
            return raw_line.decode("utf-8").strip()
        return str(raw_line or "").strip()

    @staticmethod
    def _read_remaining_response_text(resp):
        parts = []
        try:
            for raw_line in resp:
                if isinstance(raw_line, bytes):
                    parts.append(raw_line.decode("utf-8"))
                else:
                    parts.append(str(raw_line))
        except TypeError:
            pass
        return "".join(parts)

    @staticmethod
    def _set_response_socket_timeout(resp, timeout_seconds):
        candidates = [
            getattr(resp, "fp", None),
            getattr(getattr(resp, "fp", None), "raw", None),
            getattr(getattr(getattr(resp, "fp", None), "raw", None), "_sock", None),
        ]
        for candidate in candidates:
            setter = getattr(candidate, "settimeout", None)
            if callable(setter):
                try:
                    setter(timeout_seconds)
                    return
                except Exception:
                    continue

    def _emit_stream_progress(
            self,
            event_type,
            accumulator,
            start,
            *,
            first_chunk_at=None,
            protocol="openai_sse"):
        now = float(self._monotonic())
        payload = dict(accumulator.summary(include_delta=True))
        payload.update({
            "protocol": protocol,
            "stream_id": str(getattr(self, "_stream_id", "") or ""),
            "attempt_status": {
                "llm_stream_first_chunk": "started",
                "llm_stream_delta": "running",
                "llm_stream_done": "completed",
            }.get(str(event_type or ""), "running"),
            "elapsed_ms": int(max(0.0, now - start) * 1000),
        })
        payload["cumulative_chars"] = sum(
            int(payload.get(key) or 0)
            for key in ("content_chars", "reasoning_chars", "tool_argument_chars")
        )
        if first_chunk_at is not None:
            payload["first_chunk_latency_ms"] = int(
                max(0.0, float(first_chunk_at) - start) * 1000
            )
        self._emit_stream_event(event_type, payload)

    def _emit_stream_error(
            self,
            reason,
            accumulator,
            start,
            *,
            first_chunk_at=None,
            protocol="openai_chat"):
        now = float(self._monotonic())
        payload = dict(accumulator.summary(include_delta=True))
        provider_error_kind = str(reason or "").strip()
        payload.update({
            "protocol": protocol,
            "stream_id": str(getattr(self, "_stream_id", "") or ""),
            "attempt_status": "failed",
            "reason": provider_error_kind,
            "elapsed_ms": int(max(0.0, now - start) * 1000),
        })
        payload["cumulative_chars"] = sum(
            int(payload.get(key) or 0)
            for key in ("content_chars", "reasoning_chars", "tool_argument_chars")
        )
        if provider_error_kind in self.PROVIDER_FORMAT_ERROR_KINDS:
            payload.update({
                "provider_error_kind": provider_error_kind,
                "provider_error_classification": self.PROVIDER_FORMAT_INSTABILITY,
                "provider_error_recoverable": (
                    provider_error_kind in self.PROVIDER_RECOVERABLE_FORMAT_KINDS
                ),
            })
        if first_chunk_at is not None:
            payload["first_chunk_latency_ms"] = int(
                max(0.0, float(first_chunk_at) - start) * 1000
            )
        self._emit_stream_event("llm_stream_error", payload)

    # ==============================================================
    # 连通性日志
    # ==============================================================

    def _log_connectivity(self, endpoint, status, message=""):
        """记录 API 连通性（通过 ConnectivityStore，不直接操作文件）"""
        try:
            self.conn.log_latency(endpoint, status, message)
        except Exception:
            pass
