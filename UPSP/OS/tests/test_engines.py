"""
Phase 4 编排引擎层测试

测试原则：
  - mock 外部依赖（API 调用、文件读写）
  - 验证状态变迁正确性
  - 善后步不跳过
"""
import sys
import os
import json
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
import time
import pytest
from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub


class NoopConnectivity:
    def log_latency(self, endpoint, status, message=""):
        pass




# ============================================================
# CircuitBreaker 测试
# ============================================================



# ============================================================
# Heartbeat 测试
# ============================================================



# ============================================================
# APIExecutor 测试
# ============================================================

class TestAPIExecutor:
    class _RetryConfig(ConfigStoreStub):
        def load(self, name):
            if name != "api":
                return super().load(name)
            return {
                "endpoints": {
                    "primary": {
                        "url": "https://api.example/v1/chat/completions",
                        "model": "unit",
                        "provider": "openai_chat",
                        "profile_id": "model_probe",
                        "api_key": "test-only",
                        "streaming": {"enabled": False},
                    },
                },
                "handshake": {"retry": 2},
                "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
            }

    def test_spec702_probe_reuses_three_attempt_policy_without_audit_or_tools(
            self, tmp_path, monkeypatch):
        from engines.executor import APIExecutor

        calls = []
        ex = APIExecutor(
            self._RetryConfig(),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "must-not-be-written"),
        )
        ex._provider_call_interval_seconds = 0
        ex._sleep = lambda _seconds: None

        def fake_send(url, api_key, payload):
            calls.append((url, api_key, json.loads(json.dumps(payload))))
            if len(calls) < 3:
                raise TimeoutError("probe timeout")
            return {
                "choices": [{"message": {"content": "连接成功"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)
        result = ex.probe_model_profile("model_probe")

        assert result["response"] == "连接成功"
        assert len(calls) == 3
        assert calls[0][2] == calls[1][2] == calls[2][2]
        payload = calls[0][2]
        assert payload["messages"] == [{"role": "user", "content": "请只回复：连接成功"}]
        assert payload["max_tokens"] == 64
        assert "tools" not in payload
        assert not (tmp_path / "must-not-be-written").exists()

    def test_spec702_probe_does_not_retry_nontransient_401(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        calls = []
        ex = APIExecutor(
            self._RetryConfig(),
            connectivity_store=NoopConnectivity(),
        )
        ex._provider_call_interval_seconds = 0
        ex._sleep = lambda _seconds: None

        def fail_401(*_args):
            calls.append(True)
            raise APIBridgeError("model_probe", "HTTP 401: unauthorized")

        monkeypatch.setattr(ex, "_send_request", fail_401)
        with pytest.raises(APIBridgeError, match="401"):
            ex.probe_model_profile("model_probe")
        assert len(calls) == 1

    def test_spec631_reaction_snapshot_failure_blocks_provider_before_call(self):
        from types import SimpleNamespace
        from engines.round_audit import RoundAuditRecorder

        class Executor:
            def __init__(self):
                self.provider_calls = 0

            def prepare_provider_request(self, *_args, **_kwargs):
                return {}

            def call_prepared_once(self, *_args, **_kwargs):
                self.provider_calls += 1
                return {"response": "must not run"}

        class Store:
            def record_step_input_from_files(self, *_args, **_kwargs):
                return {
                    "event_type": "step_input_snapshot",
                    "phase": "reaction",
                    "payload": {"layers_snapshot": {"layers": []}},
                }

        executor = Executor()
        recorder = RoundAuditRecorder(SimpleNamespace(
            executor=executor,
            assembler=SimpleNamespace(_context_dir="unused"),
            audit_params=lambda: {"round_snapshot_retention": 8},
        ))
        recorder._store = Store()
        recorder._executor_uses_prepared_requests = lambda: True

        with pytest.raises(
                RuntimeError,
                match="reaction_popup_snapshot_incomplete:popup_layer_missing"):
            recorder.call_llm(
                "reaction",
                "system",
                [{"role": "user", "content": "test"}],
                round_num=631,
                iteration=1,
            )

        assert executor.provider_calls == 0

    def test_spec631_reaction_snapshot_write_error_blocks_provider_before_call(self):
        from types import SimpleNamespace
        from engines.round_audit import RoundAuditRecorder

        class Executor:
            def __init__(self):
                self.provider_calls = 0

            def prepare_provider_request(self, *_args, **_kwargs):
                return {}

            def call_prepared_once(self, *_args, **_kwargs):
                self.provider_calls += 1
                return {"response": "must not run"}

        class Store:
            def record_step_input_from_files(self, *_args, **_kwargs):
                raise OSError("audit path unavailable")

        executor = Executor()
        recorder = RoundAuditRecorder(SimpleNamespace(
            executor=executor,
            assembler=SimpleNamespace(_context_dir="unused"),
            audit_params=lambda: {"round_snapshot_retention": 8},
        ))
        recorder._store = Store()
        recorder._executor_uses_prepared_requests = lambda: True

        with pytest.raises(
                RuntimeError,
                match="reaction_popup_snapshot_record_failed"):
            recorder.call_llm(
                "reaction",
                "system",
                [{"role": "user", "content": "test"}],
                round_num=631,
                iteration=1,
            )

        assert executor.provider_calls == 0

    class _PayloadTruthConfig(ConfigStoreStub):
        def __init__(self, provider, *, url=None, model=None, extra_body=None,
                     reasoning_effort="", output_token_limit=0):
            self.provider = provider
            self.url = url
            self.model = model
            self.extra_body = extra_body
            self.reasoning_effort = reasoning_effort
            self.output_token_limit = output_token_limit

        def load(self, name):
            if name == "system":
                return super().load(name)
            assert name == "api"
            provider = self.provider
            urls = {
                "openai_chat": "https://example.invalid/v1/chat/completions",
                "openai_responses": "https://example.invalid/v1/responses",
                "anthropic_messages": "https://example.invalid/v1/messages",
            }
            return {
                "endpoints": {
                    "primary": {
                        "url": self.url or urls[provider],
                        "model": self.model or f"unit-{provider}",
                        "provider": provider,
                        "context_window": 123456,
                        "output_token_limit": self.output_token_limit,
                        "api_key": "secret-key",
                        "reasoning_effort": self.reasoning_effort,
                        "extra_body": self.extra_body or {
                            "seed": 422,
                            "tool_choice": "must_not_override",
                            "parallel_tool_calls": True,
                        },
                        "prompt_cache": {
                            "enabled": True,
                            "key_prefix": "unit-upsp",
                            "retention": "24h",
                        },
                    },
                },
                "handshake": {"retry": 0, "request_timeout_seconds": 300},
                "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
            }

        def get_request_timeout(self):
            return 300

    @staticmethod
    def _fake_response_for_provider(provider):
        if provider == "openai_responses":
            return {"output_text": "ok", "usage": {}}
        if provider == "anthropic_messages":
            return {"content": [{"type": "text", "text": "ok"}], "usage": {}}
        return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

    @pytest.mark.parametrize(
        "provider",
        ["openai_chat", "openai_responses", "anthropic_messages"],
    )
    @pytest.mark.parametrize("reasoning_effort", ["", "high"])
    def test_spec422_sends_body_from_step_json_request_body(
            self, provider, reasoning_effort, tmp_path, monkeypatch):
        from engines.executor import APIExecutor
        import hashlib
        import json

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig(
                provider, reasoning_effort=reasoning_effort,
            ),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(url, api_key, payload):
            sent["url"] = url
            sent["api_key"] = api_key
            sent["payload"] = payload
            return self._fake_response_for_provider(provider)

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call(
            "reaction",
            "system rules",
            [{"role": "user", "content": "hello"}],
        )

        step_json = tmp_path / "context" / "reaction" / "step.json"
        envelope = json.loads(step_json.read_text(encoding="utf-8"))
        assert result["response"] == "ok"
        assert envelope["schema"] == "provider_request.v1"
        assert envelope["call"]["step"] == "reaction"
        assert envelope["call"]["channel"] == "reaction.loop"
        assert envelope["provider"]["provider"] == provider
        assert envelope["context_window_tokens"] == 123456
        assert envelope["request_body"] == sent["payload"]
        assert envelope["request_body"]["seed"] == 422
        assert "tool_choice" not in envelope["request_body"]
        assert "parallel_tool_calls" not in envelope["request_body"]
        canonical = json.dumps(
            envelope["request_body"],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        assert envelope["request_body_sha256"] == hashlib.sha256(canonical).hexdigest()
        assert envelope["wire_body_encoding"] == "canonical_json_utf8.v1"
        assert envelope["wire_body_sha256"] == envelope["request_body_sha256"]
        assert envelope["wire_body_bytes"] == len(canonical)
        assert envelope["request_body_source_map"]["wire_body_sha256"] == (
            envelope["wire_body_sha256"]
        )
        assert "request_body_source_map" not in envelope["request_body"]
        effort_fields = {
            "openai_chat": ("reasoning_effort", "high"),
            "openai_responses": ("reasoning", {"effort": "high"}),
            "anthropic_messages": ("output_config", {"effort": "high"}),
        }
        expected_effort = effort_fields[provider] if reasoning_effort else None
        for field, value in effort_fields.values():
            if expected_effort and field == expected_effort[0]:
                assert envelope["request_body"][field] == expected_effort[1]
            else:
                assert field not in envelope["request_body"]

        for filename in [
            "00_call_header.json",
            "01_tool_header.json",
            "02_generation_config.json",
        ]:
            assert (tmp_path / "context" / "reaction" / "layers" / filename).is_file()

    def test_spec426_request_body_is_compiled_from_layer_files(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "layer permanent truth",
            "periodic": "layer periodic truth",
            "lately": "layer lately truth",
            "high_freq": "layer high freq truth",
            "now": "layer now truth",
            "statusbar": "layer statusbar truth",
            "popup": "layer popup truth",
            "full_system": "rendered audit only",
        })

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call(
            "reaction",
            "runtime system must not be payload truth",
            [{"role": "user", "content": "runtime message must not be payload truth"}],
        )

        envelope = json.loads(
            (context_dir / "reaction" / "step.json").read_text(encoding="utf-8")
        )
        body_text = json.dumps(
            envelope["request_body"], ensure_ascii=False, sort_keys=True
        )
        assert sent["payload"] == envelope["request_body"]
        assert "layer permanent truth" in body_text
        assert "layer now truth" in body_text
        assert "layer popup truth" in body_text
        assert "runtime system must not be payload truth" not in body_text
        assert "runtime message must not be payload truth" not in body_text

    @pytest.mark.parametrize(
        ("provider", "field", "ordinary_value"),
        [
            ("openai_chat", "max_tokens", None),
            ("openai_responses", "max_output_tokens", None),
            ("anthropic_messages", "max_tokens", 32000),
        ],
    )
    def test_spec742_cache_compaction_alone_uses_fixed_output_limit(
            self, provider, field, ordinary_value, tmp_path):
        from engines.executor import APIExecutor
        import json

        ordinary_executor = APIExecutor(
            self._PayloadTruthConfig(provider),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "ordinary"),
        )
        ordinary = ordinary_executor.prepare_provider_request(
            "reaction", "system",
            [{"role": "user", "content": "hello"}],
        )
        compact = APIExecutor(
            self._PayloadTruthConfig(provider, extra_body={
                "max_tokens": 456,
                "max_output_tokens": 777,
                "max_completion_tokens": 123,
            }),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "compact"),
        ).prepare_provider_request(
            "reaction",
            "system",
            [{"role": "user", "content": "hello"}],
            cache_compaction_call=True,
        )

        if ordinary_value is None:
            assert field not in ordinary["payload"]
        else:
            assert ordinary["payload"][field] == ordinary_value
        assert compact["payload"][field] == 65536
        assert {
            key for key in (
                "max_tokens", "max_output_tokens", "max_completion_tokens"
            ) if key in compact["payload"]
        } == {field}
        layer = json.loads(
            (
                tmp_path / "compact" / "reaction" / "layers"
                / "02_generation_config.json"
            ).read_text(encoding="utf-8")
        )
        assert layer["content"][field] == 65536

    @pytest.mark.parametrize(
        ("provider", "field"),
        [
            ("openai_chat", "max_tokens"),
            ("openai_responses", "max_output_tokens"),
            ("anthropic_messages", "max_tokens"),
        ],
    )
    def test_spec749_ordinary_output_limit_is_only_sent_when_configured(
            self, provider, field, tmp_path):
        from engines.executor import APIExecutor
        import json

        prepared = APIExecutor(
            self._PayloadTruthConfig(provider, output_token_limit=16384),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / provider),
        ).prepare_provider_request(
            "reaction", "system", [{"role": "user", "content": "hello"}],
        )

        assert prepared["payload"][field] == 16384
        layer = json.loads(
            (
                tmp_path / provider / "reaction" / "layers"
                / "02_generation_config.json"
            ).read_text(encoding="utf-8")
        )
        assert layer["content"][field] == 16384

    @pytest.mark.parametrize(
        ("provider", "field"),
        [
            ("openai_chat", "max_tokens"),
            ("openai_responses", "max_output_tokens"),
            ("anthropic_messages", "max_tokens"),
        ],
    )
    def test_spec742_output_limit_aliases_are_normalized_only_for_compaction(
            self, provider, field):
        from engines.executor import APIExecutor

        original = {
            "max_tokens": 456,
            "max_output_tokens": 777,
            "max_completion_tokens": 123,
            "seed": 422,
        }
        ordinary = dict(original)
        APIExecutor._apply_cache_compaction_output_limit(
            ordinary, provider, False,
        )
        assert ordinary == original

        compact = dict(original)
        APIExecutor._apply_cache_compaction_output_limit(
            compact, provider, True,
        )
        assert compact["seed"] == 422
        assert {
            key for key in (
                "max_tokens", "max_output_tokens", "max_completion_tokens"
            ) if key in compact
        } == {field}
        assert compact[field] == 65536


    def test_spec725_pre_send_wire_verification_rejects_mutated_payload(
            self, tmp_path, monkeypatch):
        from engines.executor import APIExecutor

        executor = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        executor._provider_call_interval_seconds = 0
        prepared = executor.prepare_provider_request(
            "reaction",
            "system",
            [{"role": "user", "content": "hello"}],
        )
        prepared["payload"]["temperature"] = 0.8
        calls = []
        monkeypatch.setattr(
            executor,
            "_send_request",
            lambda *_args: calls.append(True),
        )
        with pytest.raises(ValueError, match="provider_request_wire_mismatch"):
            executor.call_prepared_once(prepared)
        assert calls == []

    @pytest.mark.parametrize(
        "provider", ["openai_chat", "openai_responses", "anthropic_messages"])
    def test_spec723_block_index_never_changes_provider_body(
            self, provider, tmp_path):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        layers = {
            "permanent": "CORE\n\nRULE",
            "periodic": "",
            "lately": [],
            "high_freq": "HIGH",
            "now": [],
            "statusbar": "STATUS",
            "popup": "POPUP",
            "full_system": "rendered audit only",
        }
        store.write_audit("reaction", layers)
        executor = APIExecutor(
            self._PayloadTruthConfig(provider),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        before = executor.prepare_provider_request(
            "reaction", "ignored", [{"role": "user", "content": "ignored"}])

        store.write_audit("reaction", {
            **layers,
            "permanent_block_index": [
                {"block_id": "core", "title": "Core", "char_start": 0, "char_end": 4},
                {"block_id": "rule", "title": "Rule", "char_start": 6, "char_end": 10},
            ],
        })
        after = executor.prepare_provider_request(
            "reaction", "ignored", [{"role": "user", "content": "ignored"}])

        assert after["payload"] == before["payload"]
        assert (
            after["provider_request_envelope"]["request_body_sha256"]
            == before["provider_request_envelope"]["request_body_sha256"]
        )
        assert "block_index" not in json.dumps(after["payload"], ensure_ascii=False)
        assert after["provider_request_envelope"]["created_at"]
        assert "created_at" not in after["payload"]

    @pytest.mark.parametrize(
        ("provider", "expected_roles", "marker"),
        [
            ("openai_chat", ["user", "assistant", "system"], "prompt_cache_breakpoint"),
            ("openai_responses", ["user", "assistant", "system"], "prompt_cache_breakpoint"),
            ("anthropic_messages", ["user", "assistant", "user"], "cache_control"),
        ],
    )
    def test_spec723_lately_blocks_reach_each_provider_in_order_with_b1(
            self, provider, expected_roles, marker, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor

        lately = [
            {
                "role": "user",
                "kind": "interaction",
                "active_corpus_id": "C-00101",
                "content": "【历史交互】\n语料短ID：C-00101。\n" + "甲" * 4100,
            },
            {
                "role": "assistant",
                "kind": "dialogue_progress",
                "active_corpus_id": "C-00102",
                "content": "【历史进展记录，来自第 722 轮】\n语料短ID：C-00102。\nLATELY_SECOND",
            },
            {
                "role": "system",
                "kind": "tool_fact",
                "active_corpus_id": "C-00103",
                "content": "【历史工具事实摘要】\n语料短ID：C-00103。\nLATELY_THIRD",
            },
        ]
        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "permanent truth",
            "periodic": "",
            "lately": lately,
            "lately_markdown": "\n".join(entry["content"] for entry in lately),
            "high_freq": "",
            "now": [{"role": "user", "content": "NOW_DYNAMIC"}],
            "statusbar": "",
            "popup": "",
            "full_system": "rendered audit only",
        })
        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig(provider, model="gpt-5.6-test"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent["payload"] = payload
            return self._fake_response_for_provider(provider)

        monkeypatch.setattr(ex, "_send_request", fake_send)
        ex.call("reaction", "ignored", [{"role": "user", "content": "ignored"}])

        envelope = json.loads(
            (context_dir / "reaction" / "step.json").read_text(encoding="utf-8")
        )
        layer = json.loads(
            (context_dir / "reaction" / "layers" / "30_lately.json")
            .read_text(encoding="utf-8")
        )
        wire = sent["payload"].get("input") or sent["payload"]["messages"]

        def text_of(message):
            content = message.get("content")
            if isinstance(content, str):
                return content
            return "".join(
                str(block.get("text") or "")
                for block in content or []
                if isinstance(block, dict)
            )

        lately_wire = [message for message in wire if "语料短ID：C-001" in text_of(message)]
        assert layer["content"] == lately
        assert [message["role"] for message in lately_wire] == expected_roles
        assert [f"C-0010{index}" in text_of(message) for index, message in enumerate(
            lately_wire, start=1,
        )] == [True, True, True]
        assert marker not in str(lately_wire[-2]["content"])
        assert marker in str(lately_wire[-1]["content"])
        assert envelope["request_body"] == sent["payload"]

    def test_spec491_openai_chat_expands_reasoning_context_native_replay(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        native_replay = {
            "provider": "openai_chat",
            "assistant_message": {
                "role": "assistant",
                "content": "",
                "reasoning_content": "需要保留本次工具调用前的推理上下文。",
                "tool_calls": [
                    {
                        "id": "call_read",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"agent_eval_tasks.md\"}",
                        },
                    }
                ],
            },
            "tool_results": [
                {
                    "role": "tool",
                    "tool_call_id": "call_read",
                    "content": "{\"status\":\"ok\"}",
                }
            ],
        }
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "permanent",
            "periodic": "",
            "lately": "",
            "high_freq": "",
            "now": [
                {
                    "role": "assistant",
                    "kind": "reasoning_context",
                    "content": "【本轮推理上下文】\n需要保留本次工具调用前的推理上下文。",
                    "native_replay": native_replay,
                }
            ],
            "statusbar": "",
            "popup": "",
            "full_system": "rendered audit only",
        })

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig(
                "openai_chat",
                url="https://api.siliconflow.cn/v1",
                model="deepseek-ai/DeepSeek-V4-Flash",
                extra_body={"enable_thinking": True},
            ),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "runtime system", [{"role": "user", "content": "ignored"}])

        messages = sent["payload"]["messages"]
        assistant = next(
            message for message in messages
            if message.get("role") == "assistant"
            and message.get("reasoning_content")
        )
        tool_result = next(
            message for message in messages
            if message.get("role") == "tool"
        )

        assert assistant["reasoning_content"] == "需要保留本次工具调用前的推理上下文。"
        assert assistant["tool_calls"][0]["id"] == "call_read"
        assert tool_result["tool_call_id"] == "call_read"
        assert tool_result["content"] == "{\"status\":\"ok\"}"
        assert all(
            "native_replay" not in json.dumps(message, ensure_ascii=False)
            for message in messages
        )

    def test_spec491_plain_openai_chat_keeps_reasoning_context_as_text(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        native_replay = {
            "provider": "openai_chat",
            "assistant_message": {
                "role": "assistant",
                "content": "",
                "reasoning_content": "普通 openai_chat 不应展开。",
                "tool_calls": [
                    {
                        "id": "call_read",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"task.md\"}",
                        },
                    }
                ],
            },
            "tool_results": [
                {
                    "role": "tool",
                    "tool_call_id": "call_read",
                    "content": "{\"status\":\"ok\"}",
                }
            ],
        }
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "",
            "periodic": "",
            "lately": "",
            "high_freq": "",
            "now": [
                {
                    "role": "assistant",
                    "kind": "reasoning_context",
                    "content": "【本轮推理上下文】\n普通 openai_chat 不应展开。",
                    "native_replay": native_replay,
                }
            ],
            "statusbar": "",
            "popup": "",
            "full_system": "",
        })

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "", [])

        body = json.dumps(sent["payload"], ensure_ascii=False)
        assert "普通 openai_chat 不应展开。" in body
        assert "reasoning_content" not in body
        assert "tool_call_id" not in body
        assert "call_read" not in body
        assert "native_replay" not in body

    def test_spec492_reasoning_replay_requires_real_tool_receipt(self):
        from engines.reaction_protocol_tool_execution import reasoning_context_native_replay
        import json

        replay = reasoning_context_native_replay([
            {
                "call_id": "call_missing",
                "tool_id": "file_read",
                "reasoning_content": "需要读取任务文件。",
                "message_content": "",
                "native_tool_call": {
                    "id": "call_missing",
                    "type": "function",
                    "function": {
                        "name": "file_read",
                        "arguments": "{\"path\":\"task.md\"}",
                    },
                },
            }
        ], [], [])

        assert replay == {}, json.dumps(replay, ensure_ascii=False)

    def test_spec626_runner_writes_and_consumes_reasoning_replay_c_track(
            self, tmp_path):
        from types import SimpleNamespace
        from data.context_store import ContextStore
        from engines.reaction_loop import ReactionLoopRunner

        store = ContextStore(
            cache_dir=str(tmp_path / "cache"),
            now_cache_jsonl=str(tmp_path / "cache" / "now_cache.jsonl"),
            lately_cache_jsonl=str(tmp_path / "cache" / "lately_cache.jsonl"),
            raw_log_jsonl=str(tmp_path / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "raw_log.md"),
        )
        runner = ReactionLoopRunner.__new__(ReactionLoopRunner)
        runner.ctx_store = store
        runner.assembler = SimpleNamespace(context_store=store)
        result = runner._write_reasoning_context_if_needed(
            [{
                "call_id": "call_read",
                "tool_id": "file_read",
                "reasoning_content": "先读取任务文件。",
                "message_content": "",
                "native_tool_call": {
                    "id": "call_read",
                    "type": "function",
                    "function": {
                        "name": "file_read",
                        "arguments": "{\"path\":\"task.md\"}",
                    },
                },
            }],
            [{"call_id": "call_read", "tool_id": "file_read", "status": "ok"}],
            [],
            round_num=17,
            iteration=1,
            interaction_meta={},
        )

        assert result == {"status": "applied", "chars": len("先读取任务文件。")}
        assert store.get_now_entries() == []
        entries = store.get_call_transient_entries(
            17, "reaction", reaction_iteration=2)
        assert len(entries) == 1
        assert entries[0]["content"] == "【本轮推理上下文】\n\n先读取任务文件。"
        assert entries[0]["native_replay"]["tool_results"][0]["tool_call_id"] == "call_read"
        receipt = store.transition_current_cache(
            boundary="reaction_provider_return",
            consumer_frame_id="R000017:reaction:2",
            expire_call_transients=True,
        )
        assert receipt["expired_c_blocks"] == 1
        assert store.get_call_transient_entries(
            17, "reaction", reaction_iteration=2) == []

    def test_spec626_reasoning_replay_write_failure_is_not_applied(self):
        from engines.reaction_loop import ReactionLoopRunner

        class FailingStore:
            def append_reasoning_context(self, *args, **kwargs):
                raise RuntimeError("write failed")

        runner = ReactionLoopRunner.__new__(ReactionLoopRunner)
        runner.ctx_store = FailingStore()
        runner.assembler = None
        result = runner._write_reasoning_context_if_needed(
            [{
                "call_id": "call_read",
                "tool_id": "file_read",
                "reasoning_content": "先读取任务文件。",
                "message_content": "",
                "native_tool_call": {
                    "id": "call_read",
                    "type": "function",
                    "function": {
                        "name": "file_read",
                        "arguments": "{\"path\":\"task.md\"}",
                    },
                },
            }],
            [{"call_id": "call_read", "tool_id": "file_read", "status": "ok"}],
            [],
            round_num=17,
            iteration=0,
            interaction_meta={},
        )

        assert result == {
            "status": "failed",
            "reason": "context_store_write_failed",
            "chars": len("先读取任务文件。"),
        }

    def test_spec492_enable_thinking_gate_survives_payload_truth_layers(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        native_replay = {
            "provider": "openai_chat",
            "assistant_message": {
                "role": "assistant",
                "content": "",
                "reasoning_content": "显式 thinking 端点需要 replay。",
                "tool_calls": [
                    {
                        "id": "call_read",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"task.md\"}",
                        },
                    }
                ],
            },
            "tool_results": [
                {
                    "role": "tool",
                    "tool_call_id": "call_read",
                    "content": "{\"status\":\"ok\"}",
                }
            ],
        }
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "",
            "periodic": "",
            "lately": "",
            "high_freq": "",
            "now": [
                {
                    "role": "assistant",
                    "kind": "reasoning_context",
                    "content": "【本轮推理上下文】\n显式 thinking 端点需要 replay。",
                    "native_replay": native_replay,
                }
            ],
            "statusbar": "",
            "popup": "",
            "full_system": "",
        })

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig(
                "openai_chat",
                url="https://example.invalid/v1/chat/completions",
                model="unit-thinking-model",
                extra_body={"enable_thinking": True},
            ),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "", [])

        body = json.dumps(sent["payload"], ensure_ascii=False)
        assert "reasoning_content" in body
        assert "call_read" in body
        assert "tool_call_id" in body

    def test_spec492_siliconflow_url_without_thinking_does_not_expand_replay(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        native_replay = {
            "provider": "openai_chat",
            "assistant_message": {
                "role": "assistant",
                "content": "",
                "reasoning_content": "只有 vendor URL 不应展开。",
                "tool_calls": [
                    {
                        "id": "call_read",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"task.md\"}",
                        },
                    }
                ],
            },
            "tool_results": [
                {
                    "role": "tool",
                    "tool_call_id": "call_read",
                    "content": "{\"status\":\"ok\"}",
                }
            ],
        }
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "",
            "periodic": "",
            "lately": "",
            "high_freq": "",
            "now": [
                {
                    "role": "assistant",
                    "kind": "reasoning_context",
                    "content": "【本轮推理上下文】\n只有 vendor URL 不应展开。",
                    "native_replay": native_replay,
                }
            ],
            "statusbar": "",
            "popup": "",
            "full_system": "",
        })

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig(
                "openai_chat",
                url="https://api.siliconflow.cn/v1",
                model="meituan-longcat/LongCat-2.0",
                extra_body={"seed": 492},
            ),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "", [])

        body = json.dumps(sent["payload"], ensure_ascii=False)
        assert "只有 vendor URL 不应展开。" in body
        assert "reasoning_content" not in body
        assert "tool_call_id" not in body
        assert "call_read" not in body
        assert "native_replay" not in body

    def test_spec491_chat_tool_envelope_keeps_reasoning_and_native_call(self):
        from logic.native_tool_calls import extract_tool_call_envelopes

        response = {
            "id": "resp_491",
            "choices": [
                {
                    "message": {
                        "content": "",
                        "reasoning_content": "先读文件再写结果。",
                        "tool_calls": [
                            {
                                "id": "call_read",
                                "type": "function",
                                "function": {
                                    "name": "file_read",
                                    "arguments": "{\"path\":\"task.md\"}",
                                },
                            }
                        ],
                    }
                }
            ],
        }

        envelopes = extract_tool_call_envelopes(
            response,
            "openai_chat",
            "primary",
        )

        assert len(envelopes) == 1
        assert envelopes[0]["reasoning_content"] == "先读文件再写结果。"
        assert envelopes[0]["message_content"] == ""
        assert envelopes[0]["native_tool_call"]["id"] == "call_read"

    def test_spec491_context_assembly_hides_native_replay_from_markdown(
            self, tmp_path):
        from assembly.context import ContextAssembler
        from data.context_store import ContextStore
        import json

        native_replay = {
            "provider": "openai_chat",
            "assistant_message": {
                "role": "assistant",
                "content": "",
                "reasoning_content": "先读任务，再调用工具。",
                "tool_calls": [
                    {
                        "id": "call_read",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\":\"task.md\"}",
                        },
                    }
                ],
            },
            "tool_results": [
                {
                    "role": "tool",
                    "tool_call_id": "call_read",
                    "content": "{\"status\":\"ok\"}",
                }
            ],
        }
        ctx = ContextStore(
            cache_dir=str(tmp_path / "cache"),
            now_cache_jsonl=str(tmp_path / "cache" / "now_cache.jsonl"),
            lately_cache_jsonl=str(tmp_path / "cache" / "lately_cache.jsonl"),
            raw_log_jsonl=str(tmp_path / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "raw_log.md"),
        )
        ctx.append_reasoning_context(
            7,
            "先读任务，再调用工具。",
            native_replay=native_replay,
            step="reaction",
            iter=1,
        )
        context_dir = tmp_path / "context"
        assembler = ContextAssembler(
            context_dir=str(context_dir),
            context_store=ctx,
        )

        assembler.assemble_reaction(
            {"base": {"meta": {"total_round": 7}, "context_cache": {}}},
            "interactive",
            current_reaction_iteration=2,
        )

        step_md = (context_dir / "reaction" / "step.md").read_text(
            encoding="utf-8")
        now_md = (
            context_dir / "reaction" / "layers" / "50_now.md"
        ).read_text(encoding="utf-8")
        now_layer = json.loads(
            (context_dir / "reaction" / "layers" / "50_now.json").read_text(
                encoding="utf-8")
        )
        from data.round_live_viewer import build_live_state
        live_state = build_live_state([{
            "event_index": 1,
            "event_type": "step_input_snapshot",
            "round": 7,
            "phase": "reaction",
            "iteration": 1,
            "payload": {
                "layers_snapshot": {
                    "schema": "context_layers_snapshot.v1",
                    "source": "test",
                    "layers": [now_layer],
                }
            },
        }])
        live_now = next(
            pane for pane in live_state["context_panes"]
            if pane["id"] == "50_now"
        )["content_md"]

        assert "【本轮推理上下文】" in step_md
        assert "先读任务，再调用工具。" in step_md
        for visible in (now_md, live_now):
            assert "【本轮推理上下文】" in visible
            assert "先读任务，再调用工具。" in visible
            assert "native_replay" not in visible
            assert "tool_calls" not in visible
            assert "tool_results" not in visible
            assert "call_read" not in visible
        assert "native_replay" not in step_md
        assert "call_read" not in step_md
        assert isinstance(now_layer["content"], list)
        reasoning_entry = next(
            item for item in now_layer["content"]
            if item.get("kind") == "reasoning_context"
        )
        assert reasoning_entry["native_replay"] == native_replay

    def test_spec491_non_openai_provider_keeps_reasoning_context_as_text(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "permanent",
            "periodic": "",
            "lately": "",
            "high_freq": "",
            "now": [
                {
                    "role": "assistant",
                    "kind": "reasoning_context",
                    "content": "【本轮推理上下文】\n这是自然语言推理上下文。",
                    "native_replay": {
                        "provider": "openai_chat",
                        "assistant_message": {
                            "role": "assistant",
                            "content": "",
                            "reasoning_content": "这是自然语言推理上下文。",
                            "tool_calls": [],
                        },
                        "tool_results": [],
                    },
                }
            ],
            "statusbar": "",
            "popup": "",
            "full_system": "rendered audit only",
        })

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig("anthropic_messages"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent["payload"] = payload
            return {"content": [{"type": "text", "text": "ok"}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "runtime system", [{"role": "user", "content": "ignored"}])

        body = json.dumps(sent["payload"], ensure_ascii=False)
        assert "【本轮推理上下文】" in body
        assert "这是自然语言推理上下文。" in body
        assert "reasoning_content" not in body
        assert "tool_calls" not in body
        assert "native_replay" not in body

    def test_spec426_layer_hash_mismatch_blocks_provider_send(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "permanent",
            "periodic": "periodic",
            "lately": "lately",
            "high_freq": "high_freq",
            "now": "original now",
            "statusbar": "statusbar",
            "popup": "popup",
            "full_system": "full system",
        })
        now_layer = context_dir / "reaction" / "layers" / "50_now.json"
        payload = json.loads(now_layer.read_text(encoding="utf-8"))
        payload["content"] = "tampered now"
        now_layer.write_text(
            json.dumps(payload, ensure_ascii=False),
            encoding="utf-8",
        )

        sent = []
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0
        monkeypatch.setattr(
            ex,
            "_send_request",
            lambda *_args: sent.append(True) or {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {},
            },
        )

        with pytest.raises(ValueError, match="context_truth_corrupted"):
            ex.call("reaction", "system", [{"role": "user", "content": "hello"}])

        assert sent == []

    def test_spec424_layers_manifest_reports_real_reuse_without_body_pollution(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor
        import json

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        layers = {
            "permanent": "permanent",
            "periodic": "periodic",
            "lately": "lately",
            "high_freq": "high_freq",
            "now": "now",
            "statusbar": "statusbar",
            "popup": "popup",
            "full_system": "full system",
        }
        store.write_audit("reaction", layers)

        sent = []
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            sent.append(payload)
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "system rules", [{"role": "user", "content": "hello"}])
        store.write_audit("reaction", layers)
        ex.call("reaction", "system rules", [{"role": "user", "content": "hello"}])

        envelope = json.loads(
            (context_dir / "reaction" / "step.json").read_text(encoding="utf-8")
        )
        assert sent[-1] == envelope["request_body"]
        assert "dirty" not in json.dumps(
            envelope["request_body"], ensure_ascii=False
        )
        assert "reused" not in json.dumps(
            envelope["request_body"], ensure_ascii=False
        )

        layer_entries = envelope["layers_manifest"]["layers"]
        assert [entry["layer_key"] for entry in layer_entries] == [
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
        by_key = {entry["layer_key"]: entry for entry in layer_entries}
        assert all("dirty" in entry and "reused" in entry for entry in layer_entries)
        assert by_key["00_call_header"]["dirty"] is False
        assert by_key["00_call_header"]["reused"] is True
        assert by_key["01_tool_header"]["dirty"] is False
        assert by_key["01_tool_header"]["reused"] is True
        assert by_key["02_generation_config"]["dirty"] is False
        assert by_key["02_generation_config"]["reused"] is True
        assert by_key["10_permanent"]["dirty"] is False
        assert by_key["10_permanent"]["reused"] is True

    def test_spec422_final_reply_uses_reaction_dir_and_no_tools(
            self, tmp_path, monkeypatch):
        from engines.executor import APIExecutor
        import json

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "final"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("final_reply", "system", [{"role": "user", "content": "close"}])

        envelope = json.loads(
            (tmp_path / "context" / "reaction" / "step.json").read_text(
                encoding="utf-8"
            )
        )
        assert envelope["call"]["step"] == "reaction"
        assert envelope["call"]["channel"] == "final_reply"
        assert "tools" not in sent["payload"]
        tool_header = json.loads(
            (tmp_path / "context" / "reaction" / "layers" / "01_tool_header.json")
            .read_text(encoding="utf-8")
        )
        assert tool_header["content"]["tool_mode"] is None
        assert tool_header["content"]["tools_transmitted"] is False

    def test_spec430_reaction_tool_header_respects_limited_permission(
            self, tmp_path, monkeypatch):
        from engines.executor import APIExecutor
        import json

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        ex._provider_call_interval_seconds = 0
        monkeypatch.setenv("UPSP_EXECUTION_PERMISSION_LEVEL", "limited")

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "system", [{"role": "user", "content": "work"}])

        tool_header = json.loads(
            (tmp_path / "context" / "reaction" / "layers" / "01_tool_header.json")
            .read_text(encoding="utf-8")
        )["content"]
        tool_names = set(tool_header["tool_names"])

        assert tool_header["permission_level"] == "limited"
        assert tool_header["permission_label"] == "只读"
        assert "file_read" in tool_names
        assert "file_write" not in tool_names
        assert "file_edit" not in tool_names
        assert "shell_command" not in tool_names
        assert "subagent_dispatch" not in tool_names
        assert "file_write" not in {
            item["function"]["name"] for item in sent["payload"]["tools"]
        }

    @pytest.mark.parametrize(("permission", "label"), [
        ("guarded", "受限"),
        ("unlimited", "放行"),
    ])
    def test_spec721_reaction_tool_header_reports_actual_full_permission(
            self, tmp_path, monkeypatch, permission, label):
        from engines.executor import APIExecutor
        import json

        sent = {}
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        ex._provider_call_interval_seconds = 0
        monkeypatch.setenv("UPSP_EXECUTION_PERMISSION_LEVEL", permission)

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "system", [{"role": "user", "content": "work"}])

        tool_header = json.loads(
            (tmp_path / "context" / "reaction" / "layers" / "01_tool_header.json")
            .read_text(encoding="utf-8")
        )["content"]
        tool_names = set(tool_header["tool_names"])

        assert tool_header["permission_level"] == permission
        assert tool_header["permission_label"] == label
        assert {"file_write", "file_edit", "subagent_dispatch"} <= tool_names
        assert "shell_command" in tool_names
        functions = {
            item["function"]["name"]: item["function"]
            for item in sent["payload"]["tools"]
        }
        for tool_id in ("file_write", "file_edit", "subagent_dispatch"):
            description = functions[tool_id]["description"]
            assert "受限档" in description
            assert "放行档" in description

    def test_spec422_legacy_list_step_json_is_rejected(self, tmp_path):
        from data.audit_store import AuditStore
        import json

        store = AuditStore(setup_dir=str(tmp_path / "setup"))
        setup_dir = tmp_path / "setup"
        setup_dir.mkdir()
        (setup_dir / "step.json").write_text(
            json.dumps([{"role": "user", "content": "legacy"}], ensure_ascii=False),
            encoding="utf-8",
        )

        with pytest.raises(ValueError, match="legacy_step_json_rejected"):
            store.read_provider_request_body("setup")

    def test_spec423_injected_api_executor_binds_to_assembler_context(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from engines.executor import APIExecutor
        from engines.runtime_services import RuntimeServices

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        executor = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "wrong-context"),
        )
        executor._provider_call_interval_seconds = 0
        monkeypatch.setattr(
            executor,
            "_send_request",
            lambda *_args: {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {},
            },
        )

        services = RuntimeServices.create(
            assembler=assembler,
            executor=executor,
            config_store=executor.cfg,
            connectivity_store=NoopConnectivity(),
        )

        assert services.assembler.config_store is executor.cfg

        services.executor.call(
            "reaction",
            "system",
            [{"role": "user", "content": "hello"}],
        )

        assert (tmp_path / "context" / "reaction" / "step.json").is_file()
        assert not (tmp_path / "wrong-context" / "reaction" / "step.json").exists()

    def test_spec765_runtime_services_preserves_explicit_assembler_config(
            self, tmp_path):
        from assembly.context import ContextAssembler
        from engines.runtime_services import RuntimeServices

        runtime_config = self._PayloadTruthConfig("openai_chat")
        explicit_config = object()
        assembler = ContextAssembler(
            context_dir=str(tmp_path / "context"),
            config_store=explicit_config,
        )

        services = RuntimeServices.create(
            assembler=assembler,
            config_store=runtime_config,
            connectivity_store=NoopConnectivity(),
        )

        assert services.cfg is runtime_config
        assert services.assembler.config_store is explicit_config

    def test_spec423_round_audit_started_uses_actual_fallback_envelope(
            self, tmp_path, monkeypatch):
        import json
        from types import SimpleNamespace
        from engines.executor import APIExecutor
        from engines.round_audit import RoundAuditRecorder
        from data.round_audit_codec import read_round_audit_file

        class FallbackConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://primary.invalid/v1/chat/completions",
                            "model": "preview-primary",
                            "provider": "openai_chat",
                            "api_key": "secret-key",
                        },
                        "fallback": {
                            "url": "https://fallback.invalid/v1/chat/completions",
                            "model": "actual-fallback",
                            "provider": "openai_chat",
                            "api_key": "secret-key",
                        },
                    },
                    "handshake": {"retry": 0, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

            def get_request_timeout(self):
                return 300

            def get_audit_params(self):
                return {"round_snapshot_retention": 8}

        executor = APIExecutor(
            FallbackConfig(),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        executor._provider_call_interval_seconds = 0
        executor._get_breaker("primary").record_failure()
        monkeypatch.setattr(
            executor,
            "_send_request",
            lambda *_args: {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {},
            },
        )
        services = SimpleNamespace(
            executor=executor,
            assembler=SimpleNamespace(_context_dir=str(tmp_path / "context")),
            audit_params=lambda: {"round_snapshot_retention": 8},
        )
        recorder = RoundAuditRecorder(services)

        recorder.call_llm(
            "reaction",
            "system",
            [{"role": "user", "content": "hello"}],
            round_num=423,
            iteration=1,
        )

        round_path = tmp_path / "context" / "round" / "round_423.jsonl"
        events = read_round_audit_file(round_path)
        started = next(
            event for event in events
            if event["event_type"] == "llm_call_started"
        )
        output = next(
            event for event in events
            if event["event_type"] == "llm_output_raw"
        )
        snapshots = {
            event["event_id"]: event["payload"]["provider_request_envelope"]
            for event in events if event["event_type"] == "step_input_snapshot"
        }
        started_envelope = snapshots[started["payload"]["request_snapshot_event_id"]]
        output_envelope = snapshots[output["payload"]["request_snapshot_event_id"]]

        assert started_envelope["endpoint"]["tier"] == "fallback"
        assert started_envelope["provider"]["model"] == "actual-fallback"
        assert started_envelope["request_body_sha256"] == output_envelope["request_body_sha256"]
        assert "provider_request_envelope" not in started["payload"]
        assert "provider_request_envelope" not in output["payload"]

    def test_spec425_round_audit_records_started_for_send_failure_fallback(
            self, tmp_path, monkeypatch):
        import json
        from types import SimpleNamespace
        from engines.executor import APIExecutor
        from engines.round_audit import RoundAuditRecorder
        from data.round_audit_codec import read_round_audit_file

        class FallbackConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://primary.invalid/v1/chat/completions",
                            "model": "primary-model",
                            "provider": "openai_chat",
                            "api_key": "secret-key",
                        },
                        "fallback": {
                            "url": "https://fallback.invalid/v1/chat/completions",
                            "model": "fallback-model",
                            "provider": "openai_chat",
                            "api_key": "secret-key",
                            "extra_body": {"seed": 425},
                        },
                    },
                    "handshake": {"retry": 2, "request_timeout_seconds": 180},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

            def get_request_timeout(self):
                return 300

            def get_audit_params(self):
                return {"round_snapshot_retention": 8}

        executor = APIExecutor(
            FallbackConfig(),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        executor._provider_call_interval_seconds = 0
        sends = []

        def fake_send(url, _api_key, payload):
            sends.append((url, payload))
            if "primary.invalid" in url:
                from errors import APIBridgeError
                raise APIBridgeError(
                    "primary",
                    "HTTP 503: primary failed",
                    status_code=503,
                )
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(executor, "_send_request", fake_send)
        services = SimpleNamespace(
            executor=executor,
            assembler=SimpleNamespace(_context_dir=str(tmp_path / "context")),
            audit_params=lambda: {"round_snapshot_retention": 8},
        )
        recorder = RoundAuditRecorder(services)

        recorder.call_llm(
            "reaction",
            "system",
            [{"role": "user", "content": "hello"}],
            round_num=425,
            iteration=1,
        )

        round_path = tmp_path / "context" / "round" / "round_425.jsonl"
        events = read_round_audit_file(round_path)
        started_events = [
            event for event in events
            if event["event_type"] == "llm_call_started"
        ]
        output = next(
            event for event in events
            if event["event_type"] == "llm_output_raw"
        )
        snapshots = {
            event["event_id"]: event["payload"]["provider_request_envelope"]
            for event in events if event["event_type"] == "step_input_snapshot"
        }
        started_envelopes = [
            snapshots[event["payload"]["request_snapshot_event_id"]]
            for event in started_events
        ]
        output_envelope = snapshots[output["payload"]["request_snapshot_event_id"]]

        assert [url for url, _payload in sends] == [
            "https://primary.invalid/v1/chat/completions",
            "https://primary.invalid/v1/chat/completions",
            "https://primary.invalid/v1/chat/completions",
            "https://fallback.invalid/v1/chat/completions",
        ]
        assert [
            envelope["endpoint"]["tier"]
            for envelope in started_envelopes
        ] == ["primary", "fallback"]
        assert [event["payload"]["route_slot"] for event in started_events] == [1, 2]
        assert len({
            event["payload"]["logical_call_id"] for event in started_events
        }) == 1
        assert started_envelopes[-1]["request_body_sha256"] == (
            output_envelope["request_body_sha256"]
        )
        assert output_envelope["provider"]["model"] == "fallback-model"
        failed_attempts = [
            event for event in events
            if event["event_type"] == "llm_call_failed"
        ]
        assert len(failed_attempts) == 1
        failed_envelope = snapshots[
            failed_attempts[0]["payload"]["request_snapshot_event_id"]
        ]
        assert failed_envelope["endpoint"]["tier"] == "primary"
        http_attempts = [
            event["payload"] for event in events
            if event["event_type"] == "llm_http_attempt"
        ]
        assert [item["route_slot"] for item in http_attempts] == [1, 1, 1, 2]
        assert [item["attempt"] for item in http_attempts] == [1, 2, 3, 1]
        assert [item["status_code"] for item in http_attempts] == [503, 503, 503, 200]
        assert len({item["logical_call_id"] for item in http_attempts}) == 1
        assert not [
            event for event in events
            if event["event_type"] == "llm_error"
        ]

    def test_spec502_round_audit_records_stream_events(
            self, tmp_path, monkeypatch):
        import json
        from types import SimpleNamespace
        from engines.executor import APIExecutor
        from engines.round_audit import RoundAuditRecorder

        class StreamConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://stream.invalid/v1/chat/completions",
                            "model": "stream-model",
                            "provider": "openai_chat",
                            "streaming": {
                                "enabled": True,
                                "protocol": "openai_sse",
                                "include_usage": True,
                            },
                        },
                    },
                    "handshake": {"retry": 0, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

            def get_request_timeout(self):
                return 300

            def get_audit_params(self):
                return {"round_snapshot_retention": 8}

        executor = APIExecutor(
            StreamConfig(),
            connectivity_store=NoopConnectivity(),
            context_dir=str(tmp_path / "context"),
        )
        executor._provider_call_interval_seconds = 0

        def fake_send(_url, _api_key, payload):
            assert payload["stream"] is True
            assert payload["stream_options"]["include_usage"] is True
            executor._emit_stream_event("llm_stream_first_chunk", {
                "protocol": "openai_sse",
                "first_chunk_latency_ms": 17,
                "elapsed_ms": 17,
                "content_chars": 2,
                "reasoning_chars": 0,
                "tool_argument_chars": 0,
                "tool_call_count": 0,
            })
            return {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(executor, "_send_request", fake_send)
        services = SimpleNamespace(
            executor=executor,
            assembler=SimpleNamespace(_context_dir=str(tmp_path / "context")),
            audit_params=lambda: {"round_snapshot_retention": 8},
        )

        RoundAuditRecorder(services).call_llm(
            "reaction",
            "system",
            [{"role": "user", "content": "hello"}],
            round_num=502,
            iteration=3,
        )

        events = [
            json.loads(line)
            for line in (tmp_path / "context" / "round" / "round_502.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        ]
        stream_event = next(
            event for event in events
            if event["event_type"] == "llm_stream_first_chunk"
        )

        assert stream_event["phase"] == "reaction"
        assert stream_event["iteration"] == 3
        assert stream_event["payload"]["first_chunk_latency_ms"] == 17

    def test_spec425_corrupt_context_manifest_blocks_provider_send(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "permanent",
            "periodic": "periodic",
            "lately": "lately",
            "high_freq": "high_freq",
            "now": "now",
            "statusbar": "statusbar",
            "popup": "popup",
            "full_system": "full system",
        })
        (context_dir / "reaction" / "manifest.json").write_text(
            "{broken", encoding="utf-8"
        )
        sent = []
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0
        monkeypatch.setattr(
            ex,
            "_send_request",
            lambda *_args: sent.append(True) or {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {},
            },
        )

        with pytest.raises(ValueError, match="context_truth_corrupted"):
            ex.call("reaction", "system", [{"role": "user", "content": "hello"}])

        assert sent == []

    def test_spec425_context_layer_without_reuse_status_blocks_provider_send(
            self, tmp_path, monkeypatch):
        import json
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "permanent",
            "periodic": "periodic",
            "lately": "lately",
            "high_freq": "high_freq",
            "now": "now",
            "statusbar": "statusbar",
            "popup": "popup",
            "full_system": "full system",
        })
        manifest_path = context_dir / "reaction" / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        manifest["layers"]["now"].pop("dirty")
        manifest_path.write_text(
            json.dumps(manifest, ensure_ascii=False),
            encoding="utf-8",
        )
        sent = []
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0
        monkeypatch.setattr(
            ex,
            "_send_request",
            lambda *_args: sent.append(True) or {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {},
            },
        )

        with pytest.raises(ValueError, match="context_truth_corrupted"):
            ex.call("reaction", "system", [{"role": "user", "content": "hello"}])

        assert sent == []

    def test_spec425_corrupt_context_layer_blocks_provider_send(
            self, tmp_path, monkeypatch):
        from data.audit_store import AuditStore
        from engines.executor import APIExecutor

        context_dir = tmp_path / "context"
        store = AuditStore(reaction_dir=str(context_dir / "reaction"))
        store.write_audit("reaction", {
            "permanent": "permanent",
            "periodic": "periodic",
            "lately": "lately",
            "high_freq": "high_freq",
            "now": "now",
            "statusbar": "statusbar",
            "popup": "popup",
            "full_system": "full system",
        })
        (context_dir / "reaction" / "layers" / "50_now.json").write_text(
            "{broken", encoding="utf-8"
        )
        sent = []
        ex = APIExecutor(
            self._PayloadTruthConfig("openai_chat"),
            connectivity_store=NoopConnectivity(),
            context_dir=str(context_dir),
        )
        ex._provider_call_interval_seconds = 0
        monkeypatch.setattr(
            ex,
            "_send_request",
            lambda *_args: sent.append(True) or {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {},
            },
        )

        with pytest.raises(ValueError, match="context_truth_corrupted"):
            ex.call("reaction", "system", [{"role": "user", "content": "hello"}])

        assert sent == []

    def test_provider_call_throttle_waits_between_successive_calls(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/chat/completions",
                            "model": "unit",
                            "provider": "openai_chat",
                            "api_key": "unit-key",
                        },
                    },
                    "handshake": {"retry": 0, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        clock = {"now": 100.0}
        sleeps = []
        send_times = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._provider_call_interval_seconds = 5.0
        ex._monotonic = lambda: clock["now"]

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        def fake_send(url, api_key, payload):
            send_times.append(clock["now"])
            clock["now"] += 0.25
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        ex._sleep = fake_sleep
        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("final_reply", "system", [{"role": "user", "content": "reply"}])
        ex.call("final_reply", "system", [{"role": "user", "content": "reply again"}])

        assert sleeps == [5.0]
        assert send_times == [100.0, 105.25]

    def test_provider_call_throttle_waits_before_regular_retry(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/chat/completions",
                            "model": "unit",
                            "provider": "openai_chat",
                            "api_key": "unit-key",
                        },
                    },
                    "handshake": {"retry": 1, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        clock = {"now": 200.0}
        sleeps = []
        send_times = []
        responses = [
            {"choices": [{"message": {"content": ""}}], "usage": {}},
            {"choices": [{"message": {"content": "done"}}], "usage": {}},
        ]
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._provider_call_interval_seconds = 5.0
        ex._monotonic = lambda: clock["now"]

        def fake_sleep(seconds):
            sleeps.append(seconds)
            clock["now"] += seconds

        def fake_send(url, api_key, payload):
            send_times.append(clock["now"])
            clock["now"] += 0.1
            return responses.pop(0)

        ex._sleep = fake_sleep
        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("setup", "system", [{"role": "user", "content": "start"}])

        assert result["response"] == "done"
        assert sleeps == [1.0, 4.0]
        assert send_times == [200.0, 205.1]

    def test_default_provider_timeouts_are_three_minutes(self):
        from data.config_store import ConfigStore
        from schemas.config import default_models_config

        handshake = default_models_config()["transport"]["handshake"]
        assert handshake["request_timeout_seconds"] == 180
        assert handshake["stream_first_chunk_timeout_seconds"] == 180
        assert handshake["stream_idle_timeout_seconds"] == 180

        cfg = ConfigStore.__new__(ConfigStore)
        cfg.load = lambda name: {"handshake": {}} if name == "api" else {}
        assert cfg.get_request_timeout() == 180
        assert cfg.get_stream_first_chunk_timeout() == 180
        assert cfg.get_stream_idle_timeout() == 180
        assert cfg.get_stream_content_overrun_chars() == 65536

        cfg.load = lambda name: {
            "handshake": {
                "stream_content_overrun_chars": 2048,
                "retry": 99,
            },
        } if name == "api" else {}
        assert cfg.get_stream_content_overrun_chars() == 2048
        assert cfg.get_handshake_retry() == 2

        for invalid in ("bad", 0):
            cfg.load = lambda name, value=invalid: {
                "handshake": {
                    "request_timeout_seconds": value,
                    "stream_first_chunk_timeout_seconds": value,
                    "stream_idle_timeout_seconds": value,
                },
            } if name == "api" else {}
            assert cfg.get_request_timeout() == 180
            assert cfg.get_stream_first_chunk_timeout() == 180
            assert cfg.get_stream_idle_timeout() == 180

    def test_handshake_retry_controls_regular_error_attempts(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/chat/completions",
                            "model": "unit",
                            "provider": "openai_chat",
                        },
                    },
                    "handshake": {"retry": 0, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sends = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sends.append(url)
            raise APIBridgeError("primary", "boom")

        monkeypatch.setattr(ex, "_send_request", fake_send)
        monkeypatch.setattr("engines.executor.time.sleep", lambda seconds: None)

        with pytest.raises(APIBridgeError):
            ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert len(sends) == 1

    def test_api_timeout_retries_three_times_and_logs_endpoint_once(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.aipaibox.com/v1",
                            "model": "claude-haiku-4-5-20251001",
                            "provider": "anthropic_messages",
                            "api_key_env": "AIPAIBOX_API_KEY",
                        },
                        "fallback": {
                            "url": "https://api.aipaibox.com/v1",
                            "model": "claude-haiku-4-5-20251001",
                            "provider": "anthropic_messages",
                            "api_key_env": "AIPAIBOX_API_KEY",
                        },
                    },
                    "handshake": {"retry": 2, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        class FakeConnectivity:
            def __init__(self):
                self.rows = []

            def log_latency(self, endpoint, status, message=""):
                self.rows.append((endpoint, status, message))

        sends = []
        conn = FakeConnectivity()
        ex = APIExecutor(FakeConfig(), connectivity_store=conn)

        def fake_send(url, api_key, payload):
            sends.append(url)
            raise TimeoutError("The read operation timed out")

        monkeypatch.setattr(ex, "_send_request", fake_send)
        monkeypatch.setattr("engines.executor.time.sleep", lambda seconds: None)

        with pytest.raises(APIBridgeError):
            ex.call("final_reply", "system", [{"role": "user", "content": "reply"}])

        assert sends == ["https://api.aipaibox.com/v1/messages"] * 3
        assert len(conn.rows) == 1
        assert conn.rows[0][0] == "primary"
        assert conn.rows[0][1] == "timeout"

    def test_stream_idle_timeout_retries_same_payload_until_success(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APITimeoutError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "deepseek-ai/DeepSeek-V4-Flash",
                            "provider": "openai_chat",
                        },
                    },
                    "handshake": {
                        "retry": 2,
                        "request_timeout_seconds": 300,
                        "stream_idle_timeout_seconds": 300,
                    },
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sends = []
        sleeps = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sends.append(payload)
            if len(sends) < 3:
                raise APITimeoutError(
                    url,
                    "provider_stream_idle_timeout: no SSE data within 300 seconds",
                    timeout_seconds=300,
                )
            return {
                "choices": [{"message": {"content": "ok after idle retry"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)
        ex._sleep_between_provider_attempts = sleeps.append

        result = ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert result["response"] == "ok after idle retry"
        assert len(sends) == 3
        assert sleeps == [1, 2]

    def test_stream_idle_timeout_stops_on_third_attempt(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APITimeoutError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "deepseek-ai/DeepSeek-V4-Flash",
                            "provider": "openai_chat",
                        },
                    },
                    "handshake": {
                        "retry": 2,
                        "request_timeout_seconds": 300,
                        "stream_idle_timeout_seconds": 300,
                    },
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sends = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sends.append(payload)
            raise APITimeoutError(
                url,
                "provider_stream_idle_timeout: no SSE data within 300 seconds",
                timeout_seconds=300,
            )

        monkeypatch.setattr(ex, "_send_request", fake_send)
        monkeypatch.setattr("engines.executor.time.sleep", lambda seconds: None)

        with pytest.raises(APITimeoutError) as exc_info:
            ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert "provider_stream_idle_timeout" in str(exc_info.value)
        assert len(sends) == 3

    def test_stream_first_chunk_timeout_retries_three_times(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APITimeoutError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "deepseek-ai/DeepSeek-V4-Flash",
                            "provider": "openai_chat",
                        },
                    },
                    "handshake": {"retry": 2, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sends = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sends.append(payload)
            raise APITimeoutError(
                url,
                "provider_stream_first_chunk_timeout: no SSE chunk within 300 seconds",
                timeout_seconds=300,
            )

        monkeypatch.setattr(ex, "_send_request", fake_send)

        with pytest.raises(APITimeoutError) as exc_info:
            ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert "provider_stream_first_chunk_timeout" in str(exc_info.value)
        assert len(sends) == 3

    @pytest.mark.parametrize("kind", ["request", "first_chunk", "idle"])
    @pytest.mark.parametrize("succeeds", [True, False])
    def test_all_timeout_lanes_share_three_attempt_budget(
            self, monkeypatch, kind, succeeds):
        from engines.executor import APIExecutor
        from errors import APITimeoutError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "unit",
                            "provider": "openai_chat",
                        },
                    },
                    "handshake": {"retry": 2},
                    "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
                }

        markers = {
            "request": "API request timed out after 180 seconds",
            "first_chunk": "provider_stream_first_chunk_timeout: no SSE chunk within 180 seconds",
            "idle": "provider_stream_idle_timeout: no SSE data within 180 seconds",
        }
        sends = []
        sleeps = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._provider_call_interval_seconds = 0

        def fake_send(url, api_key, payload):
            sends.append(json.dumps(payload, sort_keys=True))
            if succeeds and len(sends) == 3:
                return {
                    "choices": [{"message": {"content": "third attempt ok"}}],
                    "usage": {},
                }
            raise APITimeoutError(url, markers[kind], timeout_seconds=180)

        monkeypatch.setattr(ex, "_send_request", fake_send)
        ex._sleep_between_provider_attempts = sleeps.append

        if succeeds:
            assert ex.call("reaction", "system", [{"role": "user", "content": "go"}])["response"] == "third attempt ok"
        else:
            with pytest.raises(APITimeoutError, match=markers[kind].split(":", 1)[0]):
                ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert len(sends) == 3
        assert len(set(sends)) == 1
        assert sleeps == [1, 2]

    @pytest.mark.parametrize("status_code", [408, 429, 500, 503])
    def test_transient_http_status_retries_until_third_success(
            self, monkeypatch, status_code):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        ex = APIExecutor(self._RetryConfig(), connectivity_store=NoopConnectivity())
        ex._provider_call_interval_seconds = 0
        sends = []
        sleeps = []

        def fake_send(url, api_key, payload):
            sends.append(json.dumps(payload, sort_keys=True))
            if len(sends) < 3:
                raise APIBridgeError(
                    "primary",
                    f"HTTP {status_code}: transient",
                    status_code=status_code,
                )
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)
        ex._sleep_between_provider_attempts = sleeps.append

        assert ex.call("reaction", "system", [{"role": "user", "content": "go"}])["response"] == "ok"
        assert len(sends) == 3
        assert len(set(sends)) == 1
        assert sleeps == [1, 2]

    def test_network_disconnect_retries_until_third_success(self, monkeypatch):
        from engines.executor import APIExecutor

        ex = APIExecutor(self._RetryConfig(), connectivity_store=NoopConnectivity())
        ex._provider_call_interval_seconds = 0
        sends = []

        def fake_send(url, api_key, payload):
            sends.append(payload)
            if len(sends) < 3:
                raise ConnectionResetError("peer disconnected")
            return {"choices": [{"message": {"content": "ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)
        ex._sleep_between_provider_attempts = lambda _seconds: None

        assert ex.call("reaction", "system", [{"role": "user", "content": "go"}])["response"] == "ok"
        assert len(sends) == 3

    def test_provider_response_connection_reset_is_structured_transient(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        ex = APIExecutor(self._RetryConfig(), connectivity_store=NoopConnectivity())

        class ResettingResponse:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                raise ConnectionResetError(10054, "peer reset")

        monkeypatch.setattr("urllib.request.urlopen", lambda *_args, **_kwargs: ResettingResponse())

        with pytest.raises(APIBridgeError) as caught:
            ex._send_request("https://api.example/v1/chat/completions", "key", {})

        assert caught.value.transient is True
        assert "provider_transport_error:ConnectionResetError" in str(caught.value)

    def test_provider_worker_preserves_transient_transport_flag(self, monkeypatch):
        from engines.executor import APIExecutor, _provider_transport_worker
        from errors import APIBridgeError

        class Connection:
            def __init__(self):
                self.messages = []

            def send(self, payload):
                self.messages.append(payload)

            def close(self):
                pass

        def reset(*_args, **_kwargs):
            raise APIBridgeError(
                "https://api.example/v1/chat/completions",
                "provider_transport_error:ConnectionResetError: peer reset",
                transient=True,
                allow_fallback=True,
                affects_connectivity=False,
            )

        monkeypatch.setattr(APIExecutor, "_send_request", reset)
        connection = Connection()
        _provider_transport_worker(connection, {
            "url": "https://api.example/v1/chat/completions",
            "api_key": "key",
            "payload": {},
            "provider": "openai",
            "timeouts": {
                "request_timeout": 180,
                "first_chunk_timeout": 30,
                "idle_timeout": 60,
                "content_overrun_chars": 4096,
            },
        })

        terminal = connection.messages[-1]
        assert terminal["kind"] == "api"
        assert terminal["transient"] is True
        assert terminal["allow_fallback"] is True
        assert terminal["affects_connectivity"] is False

    def test_spec749_round_audit_allows_declared_nontransient_failover(self):
        from engines.round_audit import RoundAuditRecorder
        from errors import APIBridgeError

        class Executor:
            @staticmethod
            def _allows_provider_failover(error):
                return error.allow_fallback

            @staticmethod
            def _fallback_tier(*_args, **_kwargs):
                return "fallback"

        error = APIBridgeError(
            "primary", "provider_output_limit_reached:max_output_tokens",
            allow_fallback=True, affects_connectivity=False,
        )
        assert RoundAuditRecorder._fallback_tier_for_prepared_error(
            Executor(),
            {
                "tier": "primary", "endpoint_config": {"model": "primary"},
                "step": "reaction",
            },
            error,
        ) == "fallback"

    def test_spec749_parent_drains_terminal_after_worker_exit(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeReceiver:
            def __init__(self):
                self.polls = iter([True, False, True])
                self.messages = [
                    {
                        "type": "event",
                        "event_type": "llm_stream_first_chunk",
                        "payload": {"stream_id": "unit"},
                    },
                    {
                        "type": "result",
                        "response": {
                            "choices": [{"message": {"content": "drained"}}],
                            "usage": {},
                        },
                    },
                ]

            def poll(self, _timeout):
                return next(self.polls)

            def recv(self):
                return self.messages.pop(0)

            def close(self):
                pass

        class FakeSender:
            def close(self):
                pass

        class FakeProcess:
            pid = 749
            exitcode = 0
            daemon = False

            def start(self):
                pass

            def is_alive(self):
                return False

            def join(self, timeout=None):
                pass

            def terminate(self):
                raise AssertionError("completed worker must not be terminated")

        class FakeContext:
            def Pipe(self, duplex=False):
                assert duplex is False
                return FakeReceiver(), FakeSender()

            def Process(self, **_kwargs):
                return FakeProcess()

        ex = APIExecutor(self._RetryConfig(), connectivity_store=NoopConnectivity())
        events = []
        ex.bind_stream_event_sink(
            lambda event_type, payload: events.append((event_type, payload))
        )
        monkeypatch.setattr(
            "engines.executor.multiprocessing.get_context",
            lambda _method: FakeContext(),
        )

        response = ex._send_request_cancellable(
            "https://api.example/v1/chat/completions",
            "key",
            {"model": "unit"},
            "openai_chat",
        )

        assert response["choices"][0]["message"]["content"] == "drained"
        assert events == [("llm_stream_first_chunk", {"stream_id": "unit"})]

    def test_spec749_output_limit_skips_same_endpoint_retries_and_fails_over(
            self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FallbackConfig(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://primary.invalid/v1/responses",
                            "model": "primary-model",
                            "provider": "openai_responses",
                        },
                        "fallback": {
                            "url": "https://fallback.invalid/v1/chat/completions",
                            "model": "fallback-model",
                            "provider": "openai_chat",
                        },
                    },
                    "step_routes": {"reaction": ["primary", "fallback"]},
                    "handshake": {"retry": 2, "request_timeout_seconds": 180},
                    "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
                }

        sends = []
        ex = APIExecutor(FallbackConfig(), connectivity_store=NoopConnectivity())
        ex._provider_call_interval_seconds = 0

        def fake_send(url, _api_key, _payload):
            sends.append(url)
            if "primary.invalid" in url:
                raise APIBridgeError(
                    url,
                    "provider_output_limit_reached:max_output_tokens",
                    allow_fallback=True,
                    affects_connectivity=False,
                )
            return {"choices": [{"message": {"content": "fallback ok"}}], "usage": {}}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert result["response"] == "fallback ok"
        assert sends == [
            "https://primary.invalid/v1/responses",
            "https://fallback.invalid/v1/chat/completions",
        ]

    @pytest.mark.parametrize("status_code", [401, 403])
    def test_permanent_auth_status_fails_once(self, monkeypatch, status_code):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        ex = APIExecutor(self._RetryConfig(), connectivity_store=NoopConnectivity())
        ex._provider_call_interval_seconds = 0
        sends = []

        def fake_send(url, api_key, payload):
            sends.append(payload)
            raise APIBridgeError(
                "primary",
                f"HTTP {status_code}: denied",
                status_code=status_code,
            )

        monkeypatch.setattr(ex, "_send_request", fake_send)

        with pytest.raises(APIBridgeError, match=str(status_code)):
            ex.call("reaction", "system", [{"role": "user", "content": "go"}])
        assert len(sends) == 1

    def test_distinct_endpoints_each_receive_three_attempts(
            self, tmp_path, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FallbackConfig(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                return {
                    "endpoints": {
                        tier: {
                            "url": f"https://{tier}.example/v1/chat/completions",
                            "model": f"unit-{tier}",
                            "provider": "openai_chat",
                            "api_key_env": f"{tier.upper()}_KEY",
                        }
                        for tier in ("primary", "fallback", "emergency")
                    },
                    "handshake": {"retry": 2},
                    "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
                }

        class FakeConnectivity:
            def __init__(self):
                self.rows = []

            def log_latency(self, endpoint, status, message=""):
                self.rows.append((endpoint, status, message))

        sends = []
        sleeps = []
        connectivity = FakeConnectivity()
        ex = APIExecutor(
            FallbackConfig(),
            connectivity_store=connectivity,
            context_dir=str(tmp_path / "context"),
        )
        ex._provider_call_interval_seconds = 0

        def fake_send(url, api_key, payload):
            sends.append((url, json.dumps(payload, sort_keys=True)))
            raise APIBridgeError(url, "HTTP 503: unavailable", status_code=503)

        monkeypatch.setattr(ex, "_send_request", fake_send)
        ex._sleep_between_provider_attempts = sleeps.append

        with pytest.raises(APIBridgeError, match="503"):
            ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert [url for url, _payload in sends] == [
            *(["https://primary.example/v1/chat/completions"] * 3),
            *(["https://fallback.example/v1/chat/completions"] * 3),
            *(["https://emergency.example/v1/chat/completions"] * 3),
        ]
        for offset in (0, 3, 6):
            assert len({payload for _url, payload in sends[offset:offset + 3]}) == 1
        assert sleeps == [1, 2, 1, 2, 1, 2]
        assert [(endpoint, status) for endpoint, status, _message in connectivity.rows] == [
            ("primary", "error"),
            ("fallback", "error"),
            ("emergency", "error"),
        ]

    def test_fallback_skips_same_endpoint_fingerprint(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.aipaibox.com/v1",
                            "model": "claude-haiku-4-5-20251001",
                            "provider": "anthropic_messages",
                            "api_key_env": "AIPAIBOX_API_KEY",
                        },
                        "fallback": {
                            "url": "https://api.aipaibox.com/v1",
                            "model": "claude-haiku-4-5-20251001",
                            "provider": "anthropic_messages",
                            "api_key_env": "AIPAIBOX_API_KEY",
                        },
                    },
                    "handshake": {"retry": 2, "request_timeout_seconds": 180},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sends = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sends.append(url)
            raise APIBridgeError(
                "primary",
                "HTTP 503: transient",
                status_code=503,
            )

        monkeypatch.setattr(ex, "_send_request", fake_send)
        monkeypatch.setattr("engines.executor.time.sleep", lambda seconds: None)

        with pytest.raises(APIBridgeError):
            ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert sends == ["https://api.aipaibox.com/v1/messages"] * 3

    def test_open_breaker_skips_same_endpoint_fingerprint(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.aipaibox.com/v1",
                            "model": "claude-haiku-4-5-20251001",
                            "provider": "anthropic_messages",
                            "api_key_env": "AIPAIBOX_API_KEY",
                        },
                        "fallback": {
                            "url": "https://api.aipaibox.com/v1",
                            "model": "claude-haiku-4-5-20251001",
                            "provider": "anthropic_messages",
                            "api_key_env": "AIPAIBOX_API_KEY",
                        },
                    },
                    "handshake": {"retry": 0, "request_timeout_seconds": 300},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sends = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._get_breaker("primary").record_failure()

        def fake_send(url, api_key, payload):
            sends.append(url)
            return {"choices": [{"message": {"content": "should not call"}}]}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        with pytest.raises(APIBridgeError):
            ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert sends == []

    def test_connectivity_timeout_sets_api_degraded_flag(self, tmp_path):
        from data.state_store import StateStore
        from data.connectivity_store import ConnectivityStore
        from engines.heartbeat import HeartbeatManager

        class MockHeat:
            def has_pending_degrade(self): return False

        class MockEvolution:
            def should_trigger(self, thresholds): return False

        state_path = tmp_path / "state.json"
        conn_path = tmp_path / "connectivity.json"
        sm = StateStore(str(state_path))
        sm.init_if_missing()
        conn = ConnectivityStore(str(conn_path))
        conn.log_latency("primary", "timeout", "response timeout")

        hb = HeartbeatManager(
            state_store=sm,
            connectivity_store=conn,
            interval=999,
            memory_heat=MockHeat(),
        )
        hb._do_tick()

        assert sm.get("base.heartbeat_flags.api_degraded") is True

    def test_init(self):
        from engines.executor import APIExecutor
        ex = APIExecutor(connectivity_store=NoopConnectivity())
        assert ex.breakers == {}

    def test_openai_base_url_resolves_to_provider_endpoint(self):
        from engines.executor import APIExecutor

        assert APIExecutor._resolved_request_url(
            "https://api.aipaibox.com/v1",
            "openai_responses",
        ) == "https://api.aipaibox.com/v1/responses"
        assert APIExecutor._resolved_request_url(
            "https://api.aipaibox.com/v1",
            "openai_chat",
        ) == "https://api.aipaibox.com/v1/chat/completions"
        assert APIExecutor._resolved_request_url(
            "https://api.aipaibox.com/v1/responses",
            "openai_responses",
        ) == "https://api.aipaibox.com/v1/responses"

    def test_sse_data_response_body_decodes_single_json_chunk(self):
        from engines.executor import APIExecutor

        body = 'data: {"choices":[{"message":{"content":"ok"}}]}\n\ndata: [DONE]\n'

        assert APIExecutor._decode_response_body(body) == {
            "choices": [{"message": {"content": "ok"}}],
        }

    def test_spec502_openai_sse_accumulator_merges_text_reasoning_tools_and_usage(self):
        from engines.executor import OpenAIChatSSEAccumulator
        from logic.native_tool_calls import extract_tool_call_envelopes

        acc = OpenAIChatSSEAccumulator()
        acc.add_chunk({
            "id": "chatcmpl_stream",
            "model": "unit-stream",
            "choices": [{
                "index": 0,
                "delta": {
                    "role": "assistant",
                    "reasoning_content": "先分析。",
                    "tool_calls": [{
                        "index": 0,
                        "id": "call_1",
                        "type": "function",
                        "function": {
                            "name": "file_read",
                            "arguments": "{\"path\"",
                        },
                    }],
                },
            }],
        })
        acc.add_chunk({
            "choices": [{
                "index": 0,
                "delta": {
                    "content": "我会读取文件。",
                    "reasoning_content": "再调用工具。",
                    "tool_calls": [{
                        "index": 0,
                        "function": {
                            "arguments": ":\"agent_eval_tasks.md\"}",
                        },
                    }],
                },
                "finish_reason": "tool_calls",
            }],
            "usage": {"prompt_tokens": 10, "completion_tokens": 3},
        })

        response = acc.to_chat_completion()
        message = response["choices"][0]["message"]

        assert message["content"] == "我会读取文件。"
        assert message["reasoning_content"] == "先分析。再调用工具。"
        assert response["usage"]["prompt_tokens"] == 10
        envelopes = extract_tool_call_envelopes(
            response,
            provider="openai_chat",
            endpoint="primary",
        )
        assert envelopes[0]["tool_id"] == "file_read"
        assert envelopes[0]["arguments"] == {"path": "agent_eval_tasks.md"}

    def test_spec502_streaming_send_request_reads_sse_incrementally_not_full_body(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 120

            def get_stream_idle_timeout(self):
                return 90

        class FakeStreamResponse:
            headers = {"Content-Type": "text/event-stream"}

            def __init__(self):
                self.lines = [
                    b"event: message\n",
                    b'data: {"id":"chatcmpl_stream","choices":[{"delta":{"content":"he"}}]}\n',
                    b"\n",
                    b'data: {"choices":[{"delta":{"content":"llo"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}\n',
                    b"data: [DONE]\n",
                ]

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter(self.lines)

            def read(self):
                raise AssertionError("streaming path must not read full response body")

        captured = {}

        def fake_urlopen(req, timeout=None):
            captured["body_bytes"] = req.data
            captured["body"] = req.data.decode("utf-8")
            captured["timeout"] = timeout
            return FakeStreamResponse()

        monkeypatch.setattr("engines.executor.urllib.request.urlopen", fake_urlopen)
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        payload = {"model": "unit", "messages": [], "stream": True}
        wire_body = APIExecutor._canonical_json_bytes(payload)
        response = ex._send_request(
            "https://api.example/v1/chat/completions",
            "unit-key",
            payload,
            wire_body=wire_body,
        )

        assert captured["timeout"] == 120
        assert captured["body_bytes"] == wire_body
        assert '"stream":true' in captured["body"]
        assert response["choices"][0]["message"]["content"] == "hello"
        assert response["usage"]["completion_tokens"] == 1

    def test_spec504_streaming_events_include_text_deltas_and_done_flush(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 120

            def get_stream_idle_timeout(self):
                return 90

        class FakeStreamResponse:
            headers = {"Content-Type": "text/event-stream"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter([
                    b'data: {"id":"chatcmpl_stream","choices":[{"delta":{"content":"he","reasoning_content":"r1"}}]}\n',
                    b'data: {"choices":[{"delta":{"content":"llo","reasoning_content":"r2"},"finish_reason":"stop"}],"usage":{"prompt_tokens":2,"completion_tokens":1}}\n',
                    b"data: [DONE]\n",
                ])

            def read(self):
                raise AssertionError("streaming path must not read full response body")

        monkeypatch.setattr(
            "engines.executor.urllib.request.urlopen",
            lambda req, timeout=None: FakeStreamResponse(),
        )
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        events = []
        ex.bind_stream_event_sink(
            lambda event_type, payload: events.append((event_type, payload))
        )

        response = ex._send_request(
            "https://api.example/v1/chat/completions",
            "",
            {"model": "unit", "messages": [], "stream": True},
        )

        first = next(payload for event_type, payload in events
                     if event_type == "llm_stream_first_chunk")
        done = next(payload for event_type, payload in events
                    if event_type == "llm_stream_done")
        assert response["choices"][0]["message"]["content"] == "hello"
        assert first["content_delta"] == "he"
        assert first["reasoning_delta"] == "r1"
        assert done["content_delta"] == "llo"
        assert done["reasoning_delta"] == "r2"
        assert "tool_argument_delta" not in first
        assert "tool_argument_delta" not in done

    def test_spec502_streaming_plain_json_falls_back_to_json_decoder(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 120

            def get_stream_idle_timeout(self):
                return 90

        class FakeJsonResponse:
            headers = {"Content-Type": "application/json"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter([b'{"choices":[{"message":{"content":"json ok"}}]}'])

            def read(self):
                return b''

        monkeypatch.setattr(
            "engines.executor.urllib.request.urlopen",
            lambda req, timeout=None: FakeJsonResponse(),
        )
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        response = ex._send_request(
            "https://api.example/v1/chat/completions",
            "",
            {"model": "unit", "messages": [], "stream": True},
        )

        assert response["choices"][0]["message"]["content"] == "json ok"

    def test_spec749_responses_incomplete_preserves_reason_without_retry_label(
            self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 120

            def get_stream_idle_timeout(self):
                return 90

            def get_stream_content_overrun_chars(self):
                return 65536

        class FakeStreamResponse:
            headers = {"Content-Type": "text/event-stream"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter([
                    b'data: {"type":"response.reasoning_text.delta","delta":"thinking"}\n',
                    b'data: {"type":"response.incomplete","response":{"status":"incomplete","incomplete_details":{"reason":"max_output_tokens"}}}\n',
                ])

            def read(self):
                raise AssertionError("streaming path must not read full response body")

        monkeypatch.setattr(
            "engines.executor.urllib.request.urlopen",
            lambda req, timeout=None: FakeStreamResponse(),
        )
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._transport_provider = "openai_responses"
        events = []
        ex.bind_stream_event_sink(
            lambda event_type, payload: events.append((event_type, payload))
        )

        with pytest.raises(APIBridgeError, match="provider_output_limit_reached") as caught:
            ex._send_request(
                "https://api.example/v1/responses",
                "",
                {"model": "unit", "input": [], "stream": True},
            )

        assert caught.value.transient is False
        assert caught.value.allow_fallback is True
        assert caught.value.affects_connectivity is False
        assert ex._provider_error_kind(caught.value) == "output_limit"
        error = next(payload for kind, payload in events if kind == "llm_stream_error")
        assert error["reason"] == "provider_output_limit_reached"
        assert error["provider_event_type"] == "response.incomplete"
        assert error["incomplete_reason"] == "max_output_tokens"

    def test_spec749_responses_failed_keeps_sanitized_provider_error(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self): return 300
            def get_stream_first_chunk_timeout(self): return 120
            def get_stream_idle_timeout(self): return 90
            def get_stream_content_overrun_chars(self): return 65536

        class FakeStreamResponse:
            headers = {"Content-Type": "text/event-stream"}

            def __enter__(self): return self
            def __exit__(self, exc_type, exc, tb): return False
            def __iter__(self):
                return iter([
                    b'data: {"type":"response.failed","response":{"status":"failed","error":{"code":"server_error","type":"server_error","message":"temporary upstream failure sk-secret123456789"}}}\n',
                ])
            def read(self): raise AssertionError("streaming path")

        monkeypatch.setattr(
            "engines.executor.urllib.request.urlopen",
            lambda req, timeout=None: FakeStreamResponse(),
        )
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._transport_provider = "openai_responses"
        events = []
        ex.bind_stream_event_sink(
            lambda event_type, payload: events.append((event_type, payload))
        )

        with pytest.raises(APIBridgeError, match="provider_response_failed") as caught:
            ex._send_request(
                "https://api.example/v1/responses", "",
                {"model": "unit", "input": [], "stream": True},
            )

        assert caught.value.transient is True
        assert ex._provider_error_kind(caught.value) == "provider_failed"
        error = next(payload for kind, payload in events if kind == "llm_stream_error")
        assert error["provider_error_code"] == "server_error"
        assert error["provider_error_type"] == "server_error"
        assert error["provider_error_message"] == (
            "temporary upstream failure sk-[redacted]"
        )

    def test_spec502_streaming_incomplete_tool_arguments_are_rejected(
            self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 120

            def get_stream_idle_timeout(self):
                return 90

        class FakeStreamResponse:
            headers = {"Content-Type": "text/event-stream"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter([
                    b'data: {"choices":[{"delta":{"tool_calls":[{"index":0,"id":"call_1","type":"function","function":{"name":"file_read","arguments":"{\\"path\\":"}}]}}]}\n',
                    b"data: [DONE]\n",
                ])

            def read(self):
                raise AssertionError("streaming path must not read full response body")

        monkeypatch.setattr(
            "engines.executor.urllib.request.urlopen",
            lambda req, timeout=None: FakeStreamResponse(),
        )
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        stream_events = []
        ex.bind_stream_event_sink(
            lambda event_type, payload: stream_events.append((event_type, payload))
        )

        with pytest.raises(APIBridgeError) as exc_info:
            ex._send_request(
                "https://api.example/v1/chat/completions",
                "",
                {"model": "unit", "messages": [], "stream": True},
            )

        assert "provider_stream_incomplete_tool_call" in str(exc_info.value)
        error_payload = next(
            payload for event_type, payload in stream_events
            if event_type == "llm_stream_error"
        )
        assert error_payload["provider_error_kind"] == "provider_stream_incomplete_tool_call"
        assert error_payload["provider_error_classification"] == (
            "provider_model_format_instability"
        )
        assert error_payload["provider_error_recoverable"] is False

    def test_spec524_streaming_plain_text_overrun_is_rejected(
            self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 120

            def get_stream_idle_timeout(self):
                return 90

            def get_stream_content_overrun_chars(self):
                return 1000

        class FakeStreamResponse:
            headers = {"Content-Type": "text/event-stream"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                chunk = "x" * 700
                return iter([
                    ('data: {"choices":[{"delta":{"content":"%s"}}]}\n' % chunk).encode("utf-8"),
                    ('data: {"choices":[{"delta":{"content":"%s"}}]}\n' % chunk).encode("utf-8"),
                    b"data: [DONE]\n",
                ])

            def read(self):
                raise AssertionError("streaming path must not read full response body")

        monkeypatch.setattr(
            "engines.executor.urllib.request.urlopen",
            lambda req, timeout=None: FakeStreamResponse(),
        )
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        stream_events = []
        ex.bind_stream_event_sink(
            lambda event_type, payload: stream_events.append((event_type, payload))
        )

        with pytest.raises(APIBridgeError) as exc_info:
            ex._send_request(
                "https://api.example/v1/chat/completions",
                "",
                {"model": "unit", "messages": [], "stream": True},
            )

        assert "provider_stream_content_overrun" in str(exc_info.value)
        error_payload = next(
            payload for event_type, payload in stream_events
            if event_type == "llm_stream_error"
        )
        assert error_payload["provider_error_kind"] == "provider_stream_content_overrun"
        assert error_payload["provider_error_classification"] == (
            "provider_model_format_instability"
        )
        assert error_payload["provider_error_recoverable"] is False
        assert error_payload["content_chars"] == 1400

    def test_spec527_provider_interruption_recovers_after_committed_progress(
            self, tmp_path):
        from data.context_store import ContextStore
        from data.state_store import StateStore
        from data.workbench import WorkbenchStore
        from engines.reaction_loop import ReactionLoopRunner
        from engines.runtime_services import RuntimeServices

        state = StateStore(path=str(tmp_path / "state.json"))
        state.init_if_missing()
        ctx = ContextStore(
            state_store=state,
            cache_dir=str(tmp_path / "cache"),
            raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
        )
        runner = ReactionLoopRunner(
            RuntimeServices.create(
                state_store=state,
                ctx_store=ctx,
                workbench_store=WorkbenchStore(root_dir=str(tmp_path / "workbench")),
            )
        )

        receipt = runner._recover_provider_interruption_if_possible(
            RuntimeError("provider_stream_idle_timeout: no SSE data"),
            round_num=527,
            iteration=2,
            general_tool_results=[{
                "tool_id": "file_write",
                "status": "applied",
                "path": "output/report.md",
            }],
        )

        assert receipt["status"] == "provider_interruption_recovered"
        assert receipt["provider_error_kind"] == "provider_stream_idle_timeout"
        assert state.get("base.heartbeat_flags.continue_requested") is True
        now_text = (tmp_path / "cache" / "now_cache.jsonl").read_text(
            encoding="utf-8"
        )
        assert "provider 中断可恢复" in now_text
        assert "continue_requested" in now_text

    def test_spec527_provider_interruption_without_progress_is_terminal(
            self, tmp_path):
        from data.context_store import ContextStore
        from data.state_store import StateStore
        from data.workbench import WorkbenchStore
        from engines.reaction_loop import ReactionLoopRunner
        from engines.runtime_services import RuntimeServices

        state = StateStore(path=str(tmp_path / "state.json"))
        state.init_if_missing()
        runner = ReactionLoopRunner(
            RuntimeServices.create(
                state_store=state,
                ctx_store=ContextStore(
                    state_store=state,
                    cache_dir=str(tmp_path / "cache"),
                ),
                workbench_store=WorkbenchStore(root_dir=str(tmp_path / "workbench")),
            )
        )

        receipt = runner._recover_provider_interruption_if_possible(
            RuntimeError("provider_stream_interrupted"),
            round_num=527,
            iteration=1,
        )

        assert receipt is None
        assert state.get("base.heartbeat_flags.continue_requested") is False

    def test_spec527_provider_interruption_third_recovery_is_terminal(
            self, tmp_path):
        from data.context_store import ContextStore
        from data.state_store import StateStore
        from data.workbench import WorkbenchStore
        from engines.reaction_loop import ReactionLoopRunner
        from engines.runtime_services import RuntimeServices

        state = StateStore(path=str(tmp_path / "state.json"))
        state.init_if_missing()
        state._set_internal("base.runtime.provider_interruption_recovery", {
            "kind": "provider_native_tool_empty_output",
            "task_id": "",
            "count": 2,
        })
        runner = ReactionLoopRunner(
            RuntimeServices.create(
                state_store=state,
                ctx_store=ContextStore(
                    state_store=state,
                    cache_dir=str(tmp_path / "cache"),
                ),
                workbench_store=WorkbenchStore(root_dir=str(tmp_path / "workbench")),
            )
        )

        receipt = runner._recover_provider_interruption_if_possible(
            RuntimeError("provider_native_tool_empty_output"),
            round_num=527,
            iteration=3,
            general_tool_results=[{"tool_id": "file_write", "status": "applied"}],
        )

        assert receipt["terminal"] is True
        assert receipt["status"] == "provider_model_format_instability"
        assert receipt["provider_interruption_count"] == 3

    def test_spec502_streaming_first_chunk_timeout_is_classified(
            self, monkeypatch):
        import socket
        from engines.executor import APIExecutor
        from errors import APITimeoutError

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 7

            def get_stream_idle_timeout(self):
                return 90

        class TimeoutStreamResponse:
            headers = {"Content-Type": "text/event-stream"}

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                raise socket.timeout("first chunk timed out")

            def read(self):
                raise AssertionError("streaming path must not read full response body")

        monkeypatch.setattr(
            "engines.executor.urllib.request.urlopen",
            lambda req, timeout=None: TimeoutStreamResponse(),
        )
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        with pytest.raises(APITimeoutError) as exc_info:
            ex._send_request(
                "https://api.example/v1/chat/completions",
                "",
                {"model": "unit", "messages": [], "stream": True},
            )

        assert "provider_stream_first_chunk_timeout" in str(exc_info.value)
        assert exc_info.value.timeout_seconds == 7

    def test_spec577_streaming_urlopen_timeout_uses_first_chunk_timeout(
            self, monkeypatch):
        import socket
        from engines.executor import APIExecutor
        from errors import APITimeoutError

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 7

            def get_stream_idle_timeout(self):
                return 90

        def raise_timeout(req, timeout=None):
            assert timeout == 7
            raise socket.timeout("first response timed out")

        monkeypatch.setattr("engines.executor.urllib.request.urlopen", raise_timeout)
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        with pytest.raises(APITimeoutError) as exc_info:
            ex._send_request(
                "https://api.example/v1/chat/completions",
                "",
                {"model": "unit", "messages": [], "stream": True},
            )

        error_text = str(exc_info.value)
        assert "provider_stream_first_chunk_timeout" in error_text
        assert "7 seconds" in error_text
        assert "after 300 seconds" not in error_text
        assert exc_info.value.timeout_seconds == 7

    def test_spec577_non_streaming_urlopen_timeout_keeps_request_timeout(
            self, monkeypatch):
        import socket
        from engines.executor import APIExecutor
        from errors import APITimeoutError

        class FakeConfig(ConfigStoreStub):
            def get_request_timeout(self):
                return 300

            def get_stream_first_chunk_timeout(self):
                return 7

            def get_stream_idle_timeout(self):
                return 90

        def raise_timeout(req, timeout=None):
            assert timeout == 300
            raise socket.timeout("request timed out")

        monkeypatch.setattr("engines.executor.urllib.request.urlopen", raise_timeout)
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        with pytest.raises(APITimeoutError) as exc_info:
            ex._send_request(
                "https://api.example/v1/chat/completions",
                "",
                {"model": "unit", "messages": []},
            )

        error_text = str(exc_info.value)
        assert "API request timed out after 300 seconds" in error_text
        assert "provider_stream_first_chunk_timeout" not in error_text
        assert exc_info.value.timeout_seconds == 300

    def test_select_tier_returns_current_phase_model_profile(self):
        from engines.executor import APIExecutor

        class FakeConfig:
            @staticmethod
            def load(name):
                assert name == "api"
                return {
                    "endpoints": {"model_a": {"url": "https://example.invalid"}},
                    "step_routes": {"setup": ["model_a"]},
                }

        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        assert ex._select_tier("setup") == "model_a"

    def test_step_tiers_route_flash_setup_cleanup_and_pro_reaction(self):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/pro",
                            "model": "deepseek-v4-pro",
                        },
                        "fallback": {
                            "url": "https://example.invalid/flash",
                            "model": "deepseek-v4-flash",
                        },
                        "emergency": {},
                    },
                    "step_tiers": {
                        "setup": "fallback",
                        "reaction": "primary",
                        "cleanup": "fallback",
                    },
                }

        ex = APIExecutor(FakeConfig())

        assert ex._select_tier("setup") == "fallback"
        assert ex._select_tier("reaction") == "primary"
        assert ex._select_tier("cleanup") == "fallback"
        assert ex._get_endpoint("fallback").get("model") == "deepseek-v4-flash"
        assert ex._get_endpoint("primary").get("model") == "deepseek-v4-pro"

    def test_successful_call_logs_ok_connectivity(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/api",
                            "model": "unit-model",
                        },
                    },
                }

        class FakeConnectivity:
            def __init__(self):
                self.rows = []

            def log_latency(self, endpoint, status, message=""):
                self.rows.append({
                    "endpoint": endpoint,
                    "status": status,
                    "message": message,
                })

        conn = FakeConnectivity()
        ex = APIExecutor(FakeConfig(), connectivity_store=conn)

        def fake_send(url, api_key, payload):
            return {
                "choices": [{"message": {"content": "ok"}}],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("setup", "", [{"role": "user", "content": "ping"}])

        assert result["response"] == "ok"
        assert conn.rows == [{
            "endpoint": "primary",
            "status": "ok",
            "message": "",
        }]

    def test_responses_endpoint_extracts_output_text_and_usage(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/responses",
                            "model": "unit-responses",
                            "reasoning_effort": "high",
                        },
                    },
                }

        class FakeConnectivity:
            def __init__(self):
                self.rows = []

            def log_latency(self, endpoint, status, message=""):
                self.rows.append((endpoint, status, message))

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=FakeConnectivity())

        def fake_send(url, api_key, payload):
            sent["url"] = url
            sent["payload"] = payload
            return {
                "output": [{
                    "type": "message",
                    "role": "assistant",
                    "content": [{
                        "type": "output_text",
                        "text": "responses ok",
                    }],
                }],
                "usage": {
                    "input_tokens": 11,
                    "output_tokens": 3,
                    "total_tokens": 14,
                },
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("setup", "transport", [{"role": "user", "content": "ping"}])

        assert sent["url"].endswith("/v1/responses")
        assert {
            key: value
            for key, value in sent["payload"].items()
            if key not in {"tools", "prompt_cache_key"}
        } == {
            "model": "unit-responses",
            "input": "user: ping",
            "instructions": "transport",
            "reasoning": {"effort": "high"},
        }
        assert [tool["name"] for tool in sent["payload"]["tools"]] == ["setup_finalize"]
        assert "tool_choice" not in sent["payload"]
        assert "parallel_tool_calls" not in sent["payload"]
        assert result["response"] == "responses ok"
        assert result["tokens_input"] == 11
        assert result["tokens_output"] == 3

    def test_setup_call_returns_request_contract_audit(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/responses",
                            "model": "unit-responses",
                        },
                    },
                }

        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            return {
                "output": [{
                    "id": "fc_setup",
                    "type": "function_call",
                    "call_id": "call_setup",
                    "name": "setup_finalize",
                    "arguments": "{\"security_verdict\":\"pass\"}",
                }],
                "usage": {"input_tokens": 1, "output_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("setup", "transport", [{"role": "user", "content": "ping"}])

        expected = {
            "step": "setup",
            "provider": "openai_responses",
            "model": "unit-responses",
            "tool_names": ["setup_finalize"],
            "terminal_tool": "setup_finalize",
            "tool_mode": "required",
            "tools_transmitted": True,
            "standard_tools_enabled": False,
        }
        assert expected.items() <= result["request_contract_audit"].items()
        assert result["request_contract_audit"]["prompt_cache_profile"] == "automatic_tiered"
        assert result["request_contract_audit"]["prompt_cache_key_applied"] is True

    def test_openai_prompt_cache_config_uses_reaction_call_channel_lanes(self):
        from engines.executor import APIExecutor
        from logic.runtime_channels import channel_for_step

        endpoint = {
            "url": "https://example.invalid/v1/responses",
            "model": "unit-responses",
            "prompt_cache": {
                "enabled": True,
                "key_prefix": "unit-upsp",
                "retention": "24h",
            },
        }

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {"primary": endpoint},
                    "circuit_breaker": {
                        "max_failures": 1,
                        "cooldown_seconds": 900,
                    },
                }

        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        expected = [
            ("setup", None, "setup_finalize"),
            ("reaction", None, "reaction_loop_tools"),
            ("reaction", ["legacy-retired-guide"], "reaction_loop_tools"),
            ("final_reply", None, "reaction_final_reply_text"),
            ("cleanup", None, "cleanup_finalize"),
        ]
        for step, guides, lane in expected:
            contract = ex.preview_request_contract(
                step,
                "system",
                [{"role": "user", "content": "ping"}],
                active_protocol_tool_guides=guides,
            )
            audit = contract["request_contract_audit"]

            assert audit["prompt_cache_lane"] == lane
            assert audit["prompt_cache_key"].endswith(f":{lane}:context-v43")
            assert audit["prompt_cache_key_applied"] is True
            assert audit["prompt_cache_profile"] == "automatic_tiered"

    def test_openai_chat_nested_function_tool_names_are_audited(self):
        from engines.executor import APIExecutor

        ex = APIExecutor(connectivity_store=NoopConnectivity())
        native_tools = [
            {"type": "function", "function": {"name": "file_read", "parameters": {}}},
            {"type": "function", "function": {"name": "reaction_finalize", "parameters": {}}},
        ]

        audit = ex._request_contract_audit(
            "reaction",
            "openai_chat",
            "unit-openai-chat",
            native_tools,
            {"tools": native_tools},
            None,
        )

        assert audit["tool_names"] == ["file_read", "reaction_finalize"]

    def test_spec568_retired_guide_exports_normal_reaction_tools(self):
        from engines.executor import APIExecutor
        from logic.runtime_channels import channel_for_step

        ex = APIExecutor(connectivity_store=NoopConnectivity())
        native_tools = ex._native_tools_for_step(
            "reaction",
            "openai_chat",
            active_protocol_tool_guides=["legacy-retired-guide"],
        )
        names = [
            ex._native_tool_schema_name(tool)
            for tool in native_tools
            if ex._native_tool_schema_name(tool)
        ]

        assert "reaction_finalize" in names
        assert "file_read" in names
        audit = ex._request_contract_audit(
            "reaction",
            "openai_chat",
            "unit-openai-chat",
            native_tools,
            {"tools": native_tools},
            channel_for_step(
                "reaction",
                active_protocol_tool_guides=["legacy-retired-guide"],
            ),
        )
        assert "native_tool_mode" not in audit
        assert audit["standard_tools_enabled"] is True

    def test_anthropic_prompt_cache_lane_is_audited_without_openai_field(self):
        from engines.executor import APIExecutor

        endpoint = {
            "url": "https://example.invalid/v1/messages",
            "model": "unit-anthropic",
            "provider": "anthropic_messages",
            "reasoning_effort": "high",
            "prompt_cache": {
                "enabled": True,
                "key_prefix": "unit-upsp",
            },
        }
        ex = APIExecutor(connectivity_store=NoopConnectivity())

        payload = ex._build_payload(
            "https://example.invalid/v1/messages",
            "unit-anthropic",
            "system",
            [{"role": "user", "content": "ping"}],
            tools=[],
            provider="anthropic_messages",
            endpoint_config=endpoint,
            step="final_reply",
        )
        audit = ex._request_contract_audit(
            "final_reply",
            "anthropic_messages",
            "unit-anthropic",
            [],
            payload,
            None,
            endpoint_config=endpoint,
        )

        assert "prompt_cache_key" not in payload
        assert payload["output_config"] == {"effort": "high"}
        assert audit["prompt_cache_lane"] == "reaction_final_reply_text"
        assert audit["prompt_cache_key"].endswith(
            ":reaction_final_reply_text:context-v43"
        )
        assert audit["prompt_cache_profile"] == "automatic_tiered"
        assert audit["prompt_cache_key_applied"] is False

    def test_native_tool_empty_output_raises_api_bridge_error(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/responses",
                            "model": "unit-responses",
                        },
                    },
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._sleep = lambda _seconds: None

        def fake_send(url, api_key, payload):
            return {
                "id": "resp_empty_tool",
                "status": "completed",
                "output": [],
                "usage": {
                    "input_tokens": 11,
                    "output_tokens": 18,
                    "total_tokens": 29,
                },
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        try:
            ex.call("setup", "transport", [{"role": "user", "content": "ping"}])
        except APIBridgeError as exc:
            assert "provider_native_tool_empty_output" in str(exc)
        else:
            raise AssertionError("expected APIBridgeError")

    def test_native_tool_empty_output_uses_shared_three_attempt_cap(
            self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "deepseek-v4-flash",
                            "provider": "openai_chat",
                            "api_format": "chat",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                    "handshake": {"retry": 2},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sent_payloads = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent_payloads.append(payload)
            return {
                "choices": [{
                    "message": {"content": ""},
                    "finish_reason": "stop",
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 0},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)
        monkeypatch.setattr(
            ex,
            "_sleep_between_provider_attempts",
            lambda _seconds: None,
        )
        monkeypatch.setattr(ex, "_sleep", lambda _seconds: None)

        try:
            ex.call("setup", "system", [{"role": "user", "content": "setup"}])
        except APIBridgeError as exc:
            assert "provider_native_tool_empty_output" in str(exc)
            assert "provider_model_format_instability" in str(exc)
            assert "attempt=3/3" in str(exc)
        else:
            raise AssertionError("expected APIBridgeError")

        assert len(sent_payloads) == 3

    def test_terminal_required_tool_payload_omits_tool_choice(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "unit-chat",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sent_payloads = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent_payloads.append(payload)
            if payload.get("tool_choice"):
                raise AssertionError("Runtime must not send tool_choice")
            return {
                "choices": [{
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call_setup",
                            "type": "function",
                            "function": {
                                "name": "setup_finalize",
                                "arguments": "{\"security_verdict\":\"pass\"}",
                            },
                        }],
                    },
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("setup", "system", [{"role": "user", "content": "setup"}])

        assert len(sent_payloads) == 1
        assert [item["function"]["name"] for item in sent_payloads[0]["tools"]] == [
            "setup_finalize"
        ]
        assert "tool_choice" not in sent_payloads[0]
        assert "parallel_tool_calls" not in sent_payloads[0]
        assert result["tool_call_envelopes"][0]["tool_id"] == "setup_finalize"
        audit = result["request_contract_audit"]
        assert audit["terminal_tool"] == "setup_finalize"
        assert audit["tool_mode"] == "required"
        assert audit["tools_transmitted"] is True

    def test_spec505_closeout_required_mode_keeps_normal_reaction_tools(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.deepseek.com/chat/completions",
                            "model": "deepseek-v4-flash",
                            "provider": "openai_chat",
                            "api_format": "chat",
                            "tool_call_provider": "openai_chat",
                            "extra_body": {"thinking": {"type": "enabled"}},
                            "reasoning_effort": "high",
                        },
                    },
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sent_payloads = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent_payloads.append(payload)
            if payload.get("tool_choice"):
                raise AssertionError("Runtime must not send tool_choice")
            return {
                "choices": [{
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call_reaction_finalize",
                            "type": "function",
                            "function": {
                                "name": "reaction_finalize",
                                "arguments": "{\"closeout_decision\":\"finish\"}",
                            },
                        }],
                    },
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call(
            "reaction",
            "system",
            [{"role": "user", "content": "retired guide"}],
            active_protocol_tool_guides=["legacy-retired-guide"],
        )

        assert len(sent_payloads) == 1
        sent_tool_names = [item["function"]["name"] for item in sent_payloads[0]["tools"]]
        assert "reaction_finalize" in sent_tool_names
        assert "file_read" in sent_tool_names
        assert sent_payloads[0]["thinking"] == {"type": "enabled"}
        assert sent_payloads[0]["reasoning_effort"] == "high"
        assert "temperature" not in sent_payloads[0]
        assert "reasoning" not in sent_payloads[0]
        assert "text" not in sent_payloads[0]
        assert "tool_choice" not in sent_payloads[0]
        assert "parallel_tool_calls" not in sent_payloads[0]
        assert result["tool_call_envelopes"][0]["tool_id"] == "reaction_finalize"
        audit = result["request_contract_audit"]
        assert audit.get("terminal_tool") == "reaction_finalize"
        assert audit["tool_mode"] == "free"
        assert "native_tool_mode" not in audit

    def test_terminal_empty_output_retries_without_mutating_tool_header(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "agnes-2.0-flash",
                            "provider": "openai_chat",
                            "api_format": "chat",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                    "handshake": {"retry": 1},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sent_payloads = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._sleep = lambda _seconds: None

        def fake_send(url, api_key, payload):
            sent_payloads.append(payload)
            if len(sent_payloads) == 1:
                return {
                    "choices": [{
                        "message": {"content": ""},
                        "finish_reason": "stop",
                    }],
                    "usage": {"prompt_tokens": 1, "completion_tokens": 0},
                }
            return {
                "choices": [{
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call_setup",
                            "type": "function",
                            "function": {
                                "name": "setup_finalize",
                                "arguments": "{\"security_verdict\":\"pass\"}",
                            },
                        }],
                    },
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("setup", "system", [{"role": "user", "content": "setup"}])

        assert len(sent_payloads) == 2
        for payload in sent_payloads:
            assert [item["function"]["name"] for item in payload["tools"]] == [
                "setup_finalize"
            ]
            assert "tool_choice" not in payload
            assert "parallel_tool_calls" not in payload
        assert result["tool_call_envelopes"][0]["tool_id"] == "setup_finalize"
        audit = result["request_contract_audit"]
        assert audit["terminal_tool"] == "setup_finalize"
        assert audit["tool_mode"] == "required"

    def test_visible_api_progress_logs_attempts_to_stderr_only(
            self, monkeypatch, capsys):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "agnes-2.0-flash",
                            "provider": "openai_chat",
                            "api_format": "chat",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                    "handshake": {"retry": 0},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            return {
                "choices": [{
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call_setup_finalize",
                            "type": "function",
                            "function": {
                                "name": "setup_finalize",
                                "arguments": "{\"security_verdict\":\"pass\"}",
                            },
                        }],
                    },
                    "finish_reason": "tool_calls",
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)
        monkeypatch.setenv("UPSP_VISIBLE_API_PROGRESS", "1")

        ex.call("setup", "system", [{"role": "user", "content": "setup"}])
        captured = capsys.readouterr()

        assert captured.out == ""
        assert "[UPSP API] start" in captured.err
        assert "step=setup" in captured.err
        assert "attempt=1/1" in captured.err
        assert "tool_choice=" not in captured.err
        assert "[UPSP API] ok" in captured.err

    def test_visible_api_progress_ignores_broken_stderr(self, monkeypatch):
        from engines.executor import APIExecutor

        class BrokenStderr:
            def write(self, _text):
                raise OSError(22, "Invalid argument")

            def flush(self):
                raise OSError(22, "Invalid argument")

        monkeypatch.setenv("UPSP_VISIBLE_API_PROGRESS", "1")
        monkeypatch.setattr("engines.executor.sys.stderr", BrokenStderr())

        APIExecutor._log_api_progress(
            "start",
            step="reaction",
            tier="primary",
            provider="openai_chat",
            model_name="gemini-3-flash",
            attempt=1,
            max_attempts=1,
            payload={},
        )

    def test_rate_limit_429_uses_shared_retry_interval(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.siliconflow.cn/v1/chat/completions",
                            "model": "nex-agi/Nex-N2-Pro",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        sends = []
        sleeps = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sends.append((url, payload))
            if len(sends) == 1:
                raise APIBridgeError(
                    "primary",
                    "HTTP 429: Too Many Requests",
                    status_code=429,
                )
            return {
                "choices": [{
                    "message": {"content": "ok after wait"},
                }],
                "usage": {"prompt_tokens": 1, "completion_tokens": 1},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)
        monkeypatch.setattr("engines.executor.time.sleep", sleeps.append)

        result = ex.call("reaction", "system", [{"role": "user", "content": "go"}])

        assert result["response"] == "ok after wait"
        assert len(sends) == 2
        assert sleeps == [1]

    def test_fallback_chain(self):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {"url": "https://example.invalid/primary"},
                        "fallback": {"url": "https://example.invalid/fallback"},
                        "emergency": {"url": "https://example.invalid/emergency"},
                    },
                }

        ex = APIExecutor(FakeConfig())
        assert ex._fallback_tier("primary") == "fallback"
        assert ex._fallback_tier("fallback") == "emergency"
        assert ex._fallback_tier("emergency") is None

    def test_fallback_chain_skips_unconfigured_empty_endpoint(self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {"url": "", "model": ""},
                        "fallback": {"url": "https://example.invalid/api", "model": "unit"},
                        "emergency": {"url": "", "model": ""},
                    },
                    "step_tiers": {"setup": "fallback"},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

            def get_request_timeout(self):
                return 1

        class FakeConnectivity:
            def log_latency(self, endpoint, status, message=""):
                pass

        ex = APIExecutor(FakeConfig(), connectivity_store=FakeConnectivity())
        ex._sleep = lambda _seconds: None
        attempted_urls = []

        def fake_send(url, api_key, payload):
            attempted_urls.append(url)
            raise APIBridgeError(
                "fallback",
                "HTTP 503: unit upstream failure",
                status_code=503,
            )

        monkeypatch.setattr(ex, "_send_request", fake_send)

        with pytest.raises(Exception) as exc_info:
            ex.call("setup", "", [{"role": "user", "content": "ping"}])

        assert "unit upstream failure" in str(exc_info.value)
        assert attempted_urls == ["https://example.invalid/api"] * 3

    def test_circuit_breaker_integration(self, tmp_path, monkeypatch):
        from engines.executor import APIExecutor
        ex = APIExecutor(connectivity_store=NoopConnectivity())
        breaker = ex._get_breaker("primary")
        assert breaker.allow_request() is True

    def test_does_not_prepend_empty_system_prompt(self, monkeypatch):
        from engines.executor import APIExecutor

        sent = {}
        ex = APIExecutor(connectivity_store=NoopConnectivity())

        class FakeBreaker:
            def allow_request(self):
                return True
            def record_success(self):
                pass

        def fake_send(url, key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "{}"}}]}

        monkeypatch.setattr(ex, "_select_tier", lambda step: "primary")
        monkeypatch.setattr(ex, "_get_endpoint", lambda tier: {"url": "http://fake", "model": "fake"})
        monkeypatch.setattr(ex, "_get_api_key", lambda ep: "k")
        monkeypatch.setattr(ex, "_get_breaker", lambda tier: FakeBreaker())
        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "", [{"role": "system", "content": "<!-- 永固层 -->\n正文"}])

        assert sent["payload"]["messages"] == [
            {"role": "system", "content": "<!-- 永固层 -->\n正文"},
            {
                "role": "user",
                "content": "【Host query】请根据以上上下文生成当前阶段所需的自然语言输出。",
            },
        ]
        assert all(
            str(message.get("content") or "").strip()
            for message in sent["payload"]["messages"]
        )

    def test_prepends_non_empty_system_prompt(self, monkeypatch):
        from engines.executor import APIExecutor

        sent = {}
        ex = APIExecutor(connectivity_store=NoopConnectivity())

        class FakeBreaker:
            def allow_request(self):
                return True
            def record_success(self):
                pass

        def fake_send(url, key, payload):
            sent["payload"] = payload
            return {"choices": [{"message": {"content": "{}"}}]}

        monkeypatch.setattr(ex, "_select_tier", lambda step: "primary")
        monkeypatch.setattr(ex, "_get_endpoint", lambda tier: {"url": "http://fake", "model": "fake"})
        monkeypatch.setattr(ex, "_get_api_key", lambda ep: "k")
        monkeypatch.setattr(ex, "_get_breaker", lambda tier: FakeBreaker())
        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "transport", [{"role": "user", "content": "body"}])

        assert sent["payload"]["messages"][0] == {"role": "system", "content": "transport"}
        assert sent["payload"]["messages"][1] == {"role": "user", "content": "body"}
