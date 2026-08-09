import json
import os
import sys


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from constants import local_now
from runtime_test_helpers import RuntimeTestMixin


class TestSpec719ApiRecoveryProbe(RuntimeTestMixin):
    @staticmethod
    def _configure_recovery_chain(runtime, *profile_ids):
        runtime.connectivity_store._recovery_endpoint_ids = lambda: profile_ids

    def _stub_round(self, runtime, monkeypatch, captured):
        from engines.round_context import SetupResult

        def fake_setup(context):
            captured["setup"] = {
                "round_type": context.round_type,
                "flags": dict(context.flags),
                "trigger_messages": list(context.trigger.messages),
            }
            return SetupResult(
                raw_result={"response": ""},
                intent={
                    "security_verdict": "pass",
                    "mount_requests": [],
                    "task_guidance_required": False,
                },
                interaction_meta=self._confirmed_meta(),
                user_input_text="测试输入",
                setup_messages=[],
                internal_handoff=[],
            )

        monkeypatch.setattr(runtime.setup_runner, "run", fake_setup)
        monkeypatch.setattr(runtime, "_run_reaction_loop", lambda *_a, **_k: {
            "response": "完成",
            "_reaction_finalize_validated": True,
            "_final_reply_done": True,
            "_interaction_meta": self._confirmed_meta(),
        })
        monkeypatch.setattr(runtime, "_run_cleanup", lambda *_a, **_k: None)
        monkeypatch.setattr(runtime.hb, "pause", lambda: None)
        monkeypatch.setattr(runtime.hb, "resume", lambda: None)

    def test_setup_chain_is_healthy_when_any_candidate_is_ok(self, tmp_path):
        from data.connectivity_store import ConnectivityStore

        store = ConnectivityStore(
            str(tmp_path / "connectivity.json"),
            active_endpoint_ids=lambda: ["setup_a", "setup_b", "reaction_a"],
            recovery_endpoint_ids=lambda: ["setup_a", "setup_b"],
        )
        store.log_latency("setup_a", "error", "primary failed")
        store.log_latency("setup_b", "ok", "fallback works")
        store.log_latency("reaction_a", "error", "not a setup route")

        assert store.has_degraded() is False
        assert sorted(store.recovery_statuses()) == ["error", "ok"]

        store.log_latency("setup_b", "timeout", "fallback failed")
        assert store.has_degraded() is True

    def test_probe_chain_uses_one_attempt_and_stops_at_first_success(
            self, monkeypatch):
        from engines.executor import APIExecutor
        from errors import APIBridgeError
        from runtime_test_helpers import ConfigStoreStub

        class Config(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                endpoints = {}
                route = []
                for index, profile_id in enumerate(("model_a", "model_b", "model_c")):
                    tier = f"setup:{index}:{profile_id}"
                    route.append(tier)
                    endpoints[tier] = {
                        "url": "https://api.example/v1/chat/completions",
                        "model": profile_id,
                        "provider": "openai_chat",
                        "profile_id": profile_id,
                    }
                return {
                    "endpoints": endpoints,
                    "step_routes": {"setup": route},
                    "handshake": {"retry": 2},
                    "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
                }

        executor = APIExecutor(Config())
        calls = []

        def probe(profile_id, **kwargs):
            calls.append((profile_id, kwargs))
            if profile_id == "model_a":
                raise APIBridgeError(profile_id, "temporary failure")
            return {
                "response": "连接成功",
                "latency_ms": 12,
                "tokens_input": 3,
                "tokens_output": 1,
            }

        monkeypatch.setattr(executor, "probe_model_profile", probe)
        result = executor.probe_setup_route_once()

        assert [item[0] for item in calls] == ["model_a", "model_b"]
        assert all(item[1]["max_attempts"] == 1 for item in calls)
        assert all(
            item[1]["connectivity_source"] == "pre_setup_recovery_probe"
            for item in calls
        )
        assert result["status"] == "recovered"
        assert result["selected_profile_id"] == "model_b"
        assert result["tokens_input"] == 3
        assert result["tokens_output"] == 1

    def test_probe_payload_disables_cache_for_every_protocol(self, monkeypatch):
        from engines.executor import APIExecutor
        from runtime_test_helpers import ConfigStoreStub

        profiles = {
            "chat_explicit": ("openai_chat", "gpt-5.6-terra", "chat/completions"),
            "responses_explicit": ("openai_responses", "gpt-5.6-terra", "responses"),
            "anthropic_explicit": ("anthropic_messages", "claude-sonnet-4-5", "messages"),
            "chat_implicit": ("openai_chat", "gpt-4.1", "chat/completions"),
        }

        class Config(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                return {"endpoints": {
                    profile_id: {
                        "url": f"https://api.example/v1/{path}",
                        "model": model,
                        "provider": provider,
                        "profile_id": profile_id,
                    }
                    for profile_id, (provider, model, path) in profiles.items()
                }}

        executor = APIExecutor(Config())
        prepared = []
        monkeypatch.setattr(
            executor,
            "call_prepared_once",
            lambda item: prepared.append(item) or {"response": "ok"},
        )

        for profile_id in profiles:
            executor.probe_model_profile(profile_id, max_attempts=1)

        assert len(prepared) == len(profiles)
        for item in prepared:
            wire = json.dumps(item["payload"], ensure_ascii=False)
            assert "prompt_cache" not in wire
            assert "cache_control" not in wire

    def test_recovery_probe_override_disables_transport_retries_and_marks_source(
            self, tmp_path, monkeypatch):
        from data.connectivity_store import ConnectivityStore
        from engines.executor import APIExecutor
        from errors import APITimeoutError
        from runtime_test_helpers import ConfigStoreStub

        class Config(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                return {
                    "endpoints": {
                        "setup:0:model_a": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "a",
                            "provider": "openai_chat",
                            "profile_id": "model_a",
                            "streaming": {"enabled": False},
                        },
                    },
                    "step_routes": {"setup": ["setup:0:model_a"]},
                    "handshake": {"retry": 2},
                    "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
                }

        connectivity = ConnectivityStore(str(tmp_path / "connectivity.json"))
        executor = APIExecutor(Config(), connectivity_store=connectivity)
        executor._provider_call_interval_seconds = 0
        calls = []

        def timeout(*_args):
            calls.append(True)
            raise APITimeoutError("model_a", "probe timeout")

        monkeypatch.setattr(executor, "_send_request", timeout)

        try:
            executor.probe_model_profile(
                "model_a",
                max_attempts=1,
                connectivity_source="pre_setup_recovery_probe",
            )
        except APITimeoutError:
            pass

        assert len(calls) == 1
        entry = connectivity.load()["recent_latencies"][-1]
        assert entry["status"] == "timeout"
        assert entry["source"] == "pre_setup_recovery_probe"

    def test_empty_probe_response_never_leaves_false_ok(
            self, tmp_path, monkeypatch):
        from data.connectivity_store import ConnectivityStore
        from engines.executor import APIExecutor
        from runtime_test_helpers import ConfigStoreStub

        class Config(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                return {
                    "endpoints": {"setup:0:model_a": {
                        "url": "https://api.example/v1/chat/completions",
                        "model": "a",
                        "provider": "openai_chat",
                        "profile_id": "model_a",
                        "streaming": {"enabled": False},
                    }},
                    "step_routes": {"setup": ["setup:0:model_a"]},
                    "handshake": {"retry": 0},
                    "circuit_breaker": {
                        "max_failures": 3,
                        "cooldown_seconds": 900,
                    },
                }

        connectivity = ConnectivityStore(
            str(tmp_path / "connectivity.json"),
            recovery_endpoint_ids=lambda: ["model_a"],
        )
        connectivity.log_latency("model_a", "error", "down")
        executor = APIExecutor(Config(), connectivity_store=connectivity)
        executor._provider_call_interval_seconds = 0
        monkeypatch.setattr(executor, "_send_request", lambda *_args: {
            "choices": [{"message": {"content": ""}}],
            "usage": {},
        })

        assert executor.probe_setup_route_once()["status"] == "failed"
        assert connectivity.recovery_statuses() == ["error"]
        assert connectivity.has_degraded() is True

    def test_missing_recovery_evidence_does_not_clear_flag(self, tmp_path):
        runtime = self._make_runtime(tmp_path)
        self._configure_recovery_chain(runtime, "missing_model")
        runtime.sm.set_flag("api_degraded", True)

        runtime.hb._do_tick()

        assert runtime.connectivity_store.has_recovered() is False
        assert runtime.sm.get("base.heartbeat_flags.api_degraded") is True

    def test_probe_chain_skips_open_breaker_before_cooldown(self, monkeypatch):
        from engines.executor import APIExecutor
        from runtime_test_helpers import ConfigStoreStub

        class Config(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                return {
                    "endpoints": {
                        "setup:0:model_a": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "a",
                            "provider": "openai_chat",
                            "profile_id": "model_a",
                        },
                        "setup:1:model_b": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "b",
                            "provider": "openai_chat",
                            "profile_id": "model_b",
                        },
                    },
                    "step_routes": {
                        "setup": ["setup:0:model_a", "setup:1:model_b"],
                    },
                    "handshake": {"retry": 2},
                    "circuit_breaker": {"max_failures": 1, "cooldown_seconds": 900},
                }

        executor = APIExecutor(Config())
        breaker = executor._get_breaker("setup:0:model_a")
        breaker.state = "open"
        breaker.last_failure_at = local_now()
        calls = []
        monkeypatch.setattr(
            executor,
            "probe_model_profile",
            lambda profile_id, **_kwargs: (
                calls.append(profile_id)
                or {"response": "ok", "tokens_input": 0, "tokens_output": 0}
            ),
        )

        result = executor.probe_setup_route_once()

        assert calls == ["model_b"]
        assert result["selected_profile_id"] == "model_b"
        assert result["skipped"] == [
            {"profile_id": "model_a", "reason": "breaker_open"},
        ]

    def test_probe_chain_never_swallows_user_cancellation(self, monkeypatch):
        import pytest
        from engines.executor import APIExecutor
        from errors import ProviderCallCancelled
        from runtime_test_helpers import ConfigStoreStub

        class Config(ConfigStoreStub):
            def load(self, name):
                if name != "api":
                    return super().load(name)
                endpoints = {
                    f"setup:{index}:model_{index}": {
                        "url": "https://api.example/v1/chat/completions",
                        "model": f"model_{index}",
                        "provider": "openai_chat",
                        "profile_id": f"model_{index}",
                    }
                    for index in range(2)
                }
                return {
                    "endpoints": endpoints,
                    "step_routes": {"setup": list(endpoints)},
                    "handshake": {"retry": 2},
                    "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
                }

        executor = APIExecutor(Config())
        calls = []

        def cancel(profile_id, **_kwargs):
            calls.append(profile_id)
            raise ProviderCallCancelled()

        monkeypatch.setattr(executor, "probe_model_profile", cancel)

        with pytest.raises(ProviderCallCancelled):
            executor.probe_setup_route_once()
        assert calls == ["model_0"]

    def test_success_clears_before_interactive_setup_and_is_audited(
            self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        self._configure_recovery_chain(runtime, "model_a", "model_b")
        runtime.connectivity_store.log_latency("model_a", "error", "down")
        runtime.connectivity_store.log_latency("model_b", "error", "down")

        class Probe:
            def probe_setup_route_once(inner_self):
                runtime.connectivity_store.log_latency(
                    "model_b", "ok", source="pre_setup_recovery_probe")
                return {
                    "status": "recovered",
                    "attempted_profile_ids": ["model_a", "model_b"],
                    "selected_profile_id": "model_b",
                    "elapsed_ms": 15,
                    "tokens_input": 3,
                    "tokens_output": 1,
                }

        runtime.executor = Probe()
        runtime.sm.set_flag("api_degraded", True)
        runtime.sm.set_flag("user_message_waiting", True)
        monkeypatch.setattr(runtime.hb, "dequeue_messages", lambda: ["测试输入"])
        captured = {}
        self._stub_round(runtime, monkeypatch, captured)

        runtime._run_one_round("rhythm", runtime.sm.load(), runtime.sm.get_flags())

        assert captured["setup"]["round_type"] == "interactive"
        assert captured["setup"]["flags"]["api_degraded"] is False
        events = [
            json.loads(line)
            for line in (
                tmp_path / "context" / "round" / "round_1.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        receipt = events[0]["payload"]["input_snapshot"]["pre_setup_api_probe"]
        assert receipt["selected_profile_id"] == "model_b"
        assert receipt["flag_cleared"] is True
        assert "response" not in receipt

    def test_success_with_only_api_flag_skips_round(self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        self._configure_recovery_chain(runtime, "model_a")
        runtime.connectivity_store.log_latency("model_a", "error", "down")

        class Probe:
            def probe_setup_route_once(inner_self):
                runtime.connectivity_store.log_latency(
                    "model_a", "ok", source="pre_setup_recovery_probe")
                return {
                    "status": "recovered",
                    "attempted_profile_ids": ["model_a"],
                    "selected_profile_id": "model_a",
                    "elapsed_ms": 1,
                    "tokens_input": 1,
                    "tokens_output": 1,
                }

        runtime.executor = Probe()
        runtime.sm.set_flag("api_degraded", True)
        monkeypatch.setattr(runtime.hb, "pause", lambda: None)
        monkeypatch.setattr(runtime.hb, "resume", lambda: None)

        result = runtime._run_one_round(
            "rhythm", runtime.sm.load(), runtime.sm.get_flags())

        assert result["_round_skipped"] == "no_effective_trigger"
        assert result["_pre_setup_api_probe"]["flag_cleared"] is True
        assert runtime.sm.get_total_round() == 0
        assert not (tmp_path / "context" / "round").exists()

    def test_failed_probe_keeps_existing_emergency_path(
            self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        self._configure_recovery_chain(runtime, "model_a")
        runtime.connectivity_store.log_latency("model_a", "error", "down")

        class Probe:
            @staticmethod
            def probe_setup_route_once():
                return {
                    "status": "failed",
                    "attempted_profile_ids": ["model_a"],
                    "selected_profile_id": None,
                    "elapsed_ms": 2,
                    "tokens_input": 0,
                    "tokens_output": 0,
                }

        runtime.executor = Probe()
        runtime.sm.set_flag("api_degraded", True)
        captured = {}
        self._stub_round(runtime, monkeypatch, captured)

        runtime._run_one_round("rhythm", runtime.sm.load(), runtime.sm.get_flags())

        assert captured["setup"]["round_type"] == "rhythm"
        assert captured["setup"]["flags"]["api_degraded"] is True
        assert runtime.sm.get("base.heartbeat_flags.api_degraded") is True

    def test_failed_probe_defers_user_input_outside_emergency_round(
            self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        self._configure_recovery_chain(runtime, "model_a")
        runtime.connectivity_store.log_latency("model_a", "error", "down")

        class Probe:
            @staticmethod
            def probe_setup_route_once():
                runtime.hb.enqueue_message("更新消息")
                return {
                    "status": "failed",
                    "attempted_profile_ids": ["model_a"],
                    "selected_profile_id": None,
                    "elapsed_ms": 2,
                    "tokens_input": 0,
                    "tokens_output": 0,
                }

        runtime.executor = Probe()
        runtime.sm.set_flag("api_degraded", True)
        runtime.sm.set_flag("user_message_waiting", True)
        monkeypatch.setattr(runtime.hb, "dequeue_messages", lambda: ["稍后处理"])
        captured = {}
        self._stub_round(runtime, monkeypatch, captured)

        runtime._run_one_round("rhythm", runtime.sm.load(), runtime.sm.get_flags())

        assert captured["setup"]["round_type"] == "rhythm"
        assert captured["setup"]["flags"]["user_message_waiting"] is False
        assert captured["setup"]["trigger_messages"] == []
        assert runtime.hb._msg_queue == ["稍后处理", "更新消息"]
        events = [
            json.loads(line)
            for line in (
                tmp_path / "context" / "round" / "round_1.jsonl"
            ).read_text(encoding="utf-8").splitlines()
        ]
        trigger = events[0]["payload"]["input_snapshot"]["trigger"]
        assert trigger["messages"] == []

    def test_pre_setup_probe_is_stoppable_and_blocks_new_submission(
            self, tmp_path):
        runtime = self._make_runtime(tmp_path)

        assert runtime.control.begin_pre_setup_probe() is True
        assert runtime.runtime_status()["can_stop"] is True
        assert runtime.submit_message("不能插队") is False
        result = runtime.request_stop()

        assert result["accepted"] is True
        assert result["stage"] == "pre_setup_probe"
        assert runtime.executor.cancellation_requested is True
        assert runtime.hb._msg_queue == []
        runtime.control.end_pre_setup_probe()

    def test_successful_probe_fails_closed_when_flag_write_fails(
            self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        self._configure_recovery_chain(runtime, "model_a")
        runtime.connectivity_store.log_latency("model_a", "error", "down")

        class Probe:
            def probe_setup_route_once(inner_self):
                runtime.connectivity_store.log_latency(
                    "model_a", "ok", source="pre_setup_recovery_probe")
                return {
                    "status": "recovered",
                    "attempted_profile_ids": ["model_a"],
                    "selected_profile_id": "model_a",
                    "elapsed_ms": 1,
                    "tokens_input": 1,
                    "tokens_output": 1,
                }

        runtime.executor = Probe()
        runtime.sm.set_flag("api_degraded", True)
        monkeypatch.setattr(
            runtime.sm,
            "clear_flags",
            lambda _names: (_ for _ in ()).throw(OSError("write failed")),
        )
        captured = {}
        self._stub_round(runtime, monkeypatch, captured)

        runtime._run_one_round("rhythm", runtime.sm.load(), runtime.sm.get_flags())

        assert captured["setup"]["flags"]["api_degraded"] is True
        assert runtime.sm.get("base.heartbeat_flags.api_degraded") is True
