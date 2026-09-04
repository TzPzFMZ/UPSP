import json
from pathlib import Path

import pytest

import data.context_store as context_store_module
from data.context_store import ContextStore
from errors import ReadError, WriteError
from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub, RuntimeTestMixin


class CacheConfig(ConfigStoreStub):
    def get_lately_cache_params(self):
        return {"budget_chars": 262144, "trim_chars": 1}

    def get_lately_compaction_params(self):
        return {
            "compact_ratio": 0.618,
            "compact_shard_chars": 196608,
            "compact_shard_ratio": 0.314,
        }


def make_store(tmp_path):
    return ContextStore(
        config_store=CacheConfig(),
        cache_dir=str(tmp_path / "cache"),
        now_cache_jsonl=str(tmp_path / "cache" / "now_cache.jsonl"),
        lately_cache_jsonl=str(tmp_path / "cache" / "lately_cache.jsonl"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )


def test_frame_transition_moves_a_and_b_but_keeps_targeted_c(tmp_path):
    store = make_store(tmp_path)
    store.append_to_cache(1, "user", "hello", kind="interaction", step="setup")
    store.append_to_cache(1, "system", "document", kind="material", step="reaction", iter=0)
    store.append_call_transient(
        1,
        "assistant",
        "reasoning",
        kind="reasoning_context",
        transient_scope="reasoning_replay",
        transient_target_step="reaction",
        transient_target_iteration=2,
    )

    assert [entry["content"] for entry in store.get_now_entries()] == [
        "hello", "document",
    ]
    assert store.get_lately_entries() == []

    receipt = store.transition_current_cache(
        boundary="reaction_provider_return",
        consumer_frame_id="R000001:reaction:1",
    )

    assert receipt["schema_version"] == "current_cache_transition.v1"
    assert receipt["moved_blocks"] == 2
    assert receipt["lane_a_blocks"] == 1
    assert receipt["lane_b_blocks"] == 1
    assert store.get_now_entries() == []
    assert [entry["content"] for entry in store.get_call_transient_entries(1, "reaction", 2)] == [
        "reasoning",
    ]
    assert [entry["content"] for entry in store.get_lately_entries()] == [
        "hello", "document",
    ]
    raw = store._read_jsonl_strict(store._raw_log_jsonl(), label="raw_log")
    assert [block["text"] for block in raw] == ["hello"]


def test_round_closeout_drains_persistent_and_expires_c(tmp_path):
    store = make_store(tmp_path)
    store.append_to_cache(
        2, "assistant", "progress", kind="dialogue_progress", step="reaction", iter=1)
    store.append_call_transient(
        2,
        "system",
        "cleanup material",
        kind="material",
        transient_scope="cleanup_round",
        transient_target_step="cleanup",
    )

    receipt = store.transition_current_cache(
        boundary="round_closeout",
        consumer_frame_id="R000002:closeout",
        expire_call_transients=True,
    )

    assert receipt["moved_blocks"] == 1
    assert receipt["expired_c_blocks"] == 1
    assert store._current_now_blocks() == []
    assert [entry["content"] for entry in store.get_lately_entries()] == ["progress"]


def test_provider_transition_expires_only_the_consumed_c_target(tmp_path):
    store = make_store(tmp_path)
    for round_num, step, iteration, content in (
        (8, "reaction", 1, "reaction-one"),
        (8, "reaction", 2, "reaction-two"),
        (8, "cleanup", None, "cleanup"),
        (7, "reaction", 1, "prior-round"),
    ):
        store.append_call_transient(
            round_num,
            "system",
            content,
            kind="material",
            transient_scope="test",
            transient_target_step=step,
            transient_target_iteration=iteration,
        )

    receipt = store.transition_current_cache(
        boundary="reaction_provider_return",
        consumer_frame_id="R000008:reaction:1",
        expire_call_transients=True,
        transient_round=8,
        transient_target_step="reaction",
        transient_target_iteration=1,
    )

    assert receipt["expired_c_blocks"] == 1
    remaining = {
        entry["content"] for entry in store._all_now_entries()
        if store._is_call_transient(entry)
    }
    assert remaining == {"reaction-two", "cleanup", "prior-round"}


def test_startup_migrates_legacy_now_once_then_preserves_crash_package(tmp_path):
    store = make_store(tmp_path)
    store.append_to_cache(3, "user", "legacy", kind="interaction", step="setup")

    migrated = store.reconcile_now_cache_lifecycle_on_startup()
    assert migrated["boundary"] == "startup_legacy_migration"
    assert store.get_now_entries() == []
    assert store._load_now_cache_lifecycle() == {
        "schema_version": "now_cache_lifecycle.v1",
    }

    store.append_to_cache(4, "assistant", "crash tail", kind="dialogue_progress")
    restarted = make_store(tmp_path)
    recovered = restarted.reconcile_now_cache_lifecycle_on_startup()
    assert recovered["status"] == "noop"
    assert [entry["content"] for entry in restarted.get_now_entries()] == ["crash tail"]


def test_startup_disambiguates_distinct_legacy_raw_facts_with_same_text(tmp_path):
    store = make_store(tmp_path)
    common = {
        "role": "system",
        "kind": "tool_fact",
        "text": "同一通用回执正文",
        "loc": {"round": 9, "step": "reaction", "iter": 0},
        "policy": {"now": True, "lately": True},
    }
    existing = {
        **common,
        "id": "R000009-system-0001",
        "ref": {
            "source_block_id": "R000009-system-0001",
            "protocol_receipts": [{"reason": "first"}],
        },
    }
    legacy_key = store._legacy_raw_log_key(existing)
    existing["ref"]["raw_log_key"] = legacy_key
    incoming = {
        **common,
        "id": "R000009-system-0002",
        "ref": {
            "source_block_id": "R000009-system-0002",
            "protocol_receipts": [{"reason": "second"}],
        },
    }
    store._write_jsonl_atomic(store._raw_log_jsonl(), [existing])
    store._write_jsonl_atomic(store._now_cache_jsonl(), [incoming])

    receipt = store.reconcile_now_cache_lifecycle_on_startup()

    raw = store._current_raw_log_blocks()
    lately = store._current_lately_blocks()
    assert receipt["moved_blocks"] == 1
    assert len(raw) == 2
    assert raw[0]["ref"]["raw_log_key"] == legacy_key
    assert raw[1]["ref"]["raw_log_key"] != legacy_key
    assert lately[0]["ref"]["raw_log_key"] == raw[1]["ref"]["raw_log_key"]


def test_startup_keeps_unproven_raw_key_conflict_fail_closed(tmp_path):
    store = make_store(tmp_path)
    common = {
        "role": "system",
        "kind": "tool_fact",
        "text": "同一正文",
        "loc": {"round": 10, "step": "reaction", "iter": 0},
        "policy": {"now": True, "lately": True},
    }
    existing = {
        **common,
        "id": "R000010-system-0001",
        "ref": {
            "source_block_id": "R000010-system-0001",
            "raw_log_key": "forced-key",
            "protocol_receipts": [{"reason": "first"}],
        },
    }
    incoming = {
        **common,
        "id": "R000010-system-0002",
        "ref": {
            "source_block_id": "R000010-system-0002",
            "raw_log_key": "forced-key",
            "protocol_receipts": [{"reason": "second"}],
        },
    }
    store._write_jsonl_atomic(store._raw_log_jsonl(), [existing])
    store._write_jsonl_atomic(store._now_cache_jsonl(), [incoming])
    before_raw = Path(store._raw_log_jsonl()).read_bytes()
    before_now = Path(store._now_cache_jsonl()).read_bytes()

    with pytest.raises(ValueError, match="raw_log_key conflict"):
        store.reconcile_now_cache_lifecycle_on_startup()

    assert Path(store._raw_log_jsonl()).read_bytes() == before_raw
    assert Path(store._now_cache_jsonl()).read_bytes() == before_now


def test_startup_rejects_same_active_id_with_different_body(tmp_path):
    store = make_store(tmp_path)
    store.append_to_cache(5, "user", "first", kind="interaction")
    store.transition_current_cache(
        boundary="startup_legacy_migration",
        write_lifecycle_marker=True,
    )
    block = dict(store._current_lately_blocks()[0])
    block["text"] = "conflict"
    store._write_jsonl_atomic(store._now_cache_jsonl(), [block])

    with pytest.raises(ReadError, match="now_lately_active_corpus_conflict"):
        make_store(tmp_path).reconcile_now_cache_lifecycle_on_startup()


def test_startup_removes_proven_same_block_tail_without_touching_lately(tmp_path):
    store = make_store(tmp_path)
    store.append_to_cache(5, "user", "same", kind="interaction")
    store.transition_current_cache(
        boundary="startup_legacy_migration",
        write_lifecycle_marker=True,
    )
    lately_before = Path(store._lately_cache_jsonl()).read_bytes()
    store._write_jsonl_atomic(
        store._now_cache_jsonl(), [dict(store._current_lately_blocks()[0])])

    receipt = make_store(tmp_path).reconcile_now_cache_lifecycle_on_startup()

    assert receipt["status"] == "recovered"
    assert receipt["recovered_duplicate_blocks"] == 1
    assert receipt["moved_blocks"] == 0
    assert receipt["moved_chars"] == 0
    assert receipt["lane_a_blocks"] == 0
    assert receipt["lane_b_blocks"] == 0
    assert make_store(tmp_path)._current_now_blocks() == []
    assert Path(store._lately_cache_jsonl()).read_bytes() == lately_before


@pytest.mark.parametrize("failing_target", [
    "raw_log.jsonl",
    "raw_log.md",
    "lately_cache.jsonl",
    "now_cache.jsonl",
    "now_cache_lifecycle.json",
])
def test_transition_rolls_back_every_file_stage(tmp_path, monkeypatch, failing_target):
    store = make_store(tmp_path)
    store.append_to_cache(6, "user", "rollback", kind="interaction")
    store.append_call_transient(
        6,
        "assistant",
        "rollback-c",
        kind="reasoning_context",
        transient_scope="reasoning_replay",
        transient_target_step="reaction",
        transient_target_iteration=1,
    )
    before = {
        path: Path(path).read_bytes() if Path(path).is_file() else None
        for path in (
            store._raw_log_jsonl(), store._raw_log_md(),
            store._lately_cache_jsonl(), store._now_cache_jsonl(),
            store.now_cache_lifecycle_path(),
        )
    }
    original_jsonl = store._write_jsonl_atomic
    original_json = store._write_json_atomic
    original_text = context_store_module.atomic_write_text
    failed = {"done": False}

    def fail_jsonl(path, items):
        if Path(path).name == failing_target and not failed["done"]:
            failed["done"] = True
            raise WriteError(path, message="injected")
        return original_jsonl(path, items)

    def fail_text(path, text, **kwargs):
        if Path(path).name == failing_target and not failed["done"]:
            failed["done"] = True
            raise WriteError(path, message="injected")
        return original_text(path, text, **kwargs)

    def fail_json(path, value):
        if Path(path).name == failing_target and not failed["done"]:
            failed["done"] = True
            raise WriteError(path, message="injected")
        return original_json(path, value)

    monkeypatch.setattr(store, "_write_jsonl_atomic", fail_jsonl)
    monkeypatch.setattr(store, "_write_json_atomic", fail_json)
    monkeypatch.setattr("data.context_store.atomic_write_text", fail_text)
    with pytest.raises(WriteError, match="injected"):
        store.transition_current_cache(
            boundary="reaction_provider_return",
            consumer_frame_id="R000006:reaction:1",
            expire_call_transients=True,
            write_lifecycle_marker=(failing_target == "now_cache_lifecycle.json"),
        )

    for path, payload in before.items():
        target = Path(path)
        assert (target.read_bytes() if target.is_file() else None) == payload


def test_material_chars_are_scoped_to_exact_producer_iteration(tmp_path):
    store = make_store(tmp_path)
    store.append_to_cache(7, "system", "old", kind="material", step="reaction", iter=0)
    store.transition_current_cache(boundary="reaction_provider_return")
    store.append_to_cache(7, "system", "newer", kind="material", step="reaction", iter=1)

    assert store.get_round_material_chars(7, iteration=0) == 3
    assert store.get_round_material_chars(7, iteration=1) == 5
    assert store.get_round_material_chars(7, iteration=2) == 0


def test_invalid_lifecycle_marker_fails_closed(tmp_path):
    store = make_store(tmp_path)
    marker = Path(store.now_cache_lifecycle_path())
    marker.parent.mkdir(parents=True, exist_ok=True)
    marker.write_text(json.dumps({"schema_version": "unknown"}), encoding="utf-8")

    with pytest.raises(ReadError, match="now_cache_lifecycle_invalid"):
        store.reconcile_now_cache_lifecycle_on_startup()


def test_invalid_utf8_cache_fails_before_transaction_without_rewrite(tmp_path):
    store = make_store(tmp_path)
    path = Path(store._now_cache_jsonl())
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = b"\xff\xfeinvalid"
    path.write_bytes(payload)

    with pytest.raises(ReadError):
        store.transition_current_cache(boundary="reaction_provider_return")

    assert path.read_bytes() == payload


class TestSpec745RuntimeAudit(RuntimeTestMixin):
    @staticmethod
    def _layer_text(event, layer_key):
        layers = event["payload"]["layers_snapshot"]["layers"]
        layer = next(item for item in layers if item["layer_key"] == layer_key)
        return "\n".join((
            str(layer.get("content_markdown") or ""),
            json.dumps(layer.get("content"), ensure_ascii=False, sort_keys=True),
        ))

    @staticmethod
    def _write_fake_call_layers(runtime):
        runtime.assembler.audit.write_call_layers(
            "reaction",
            call={"phase": "reaction"},
            provider={"provider": "isolated_fake"},
            endpoint={"endpoint": "test"},
            tool_header={},
            generation_config={},
        )

    def test_three_reaction_frames_shift_exact_packages_in_ten_layer_audit(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        runtime = self._make_runtime(tmp_path)
        assembler = runtime.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class ThreeFrameExecutor:
            def __init__(self):
                self.calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls += 1
                if self.calls < 3:
                    return {
                        "response": f"FRAME_{self.calls}_PROGRESS",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": f"frame-{self.calls}.txt",
                                "reason": "exercise frame cache shift",
                            },
                            call_id=f"call_frame_{self.calls}",
                        )],
                    }
                return {
                    "response": "FRAME_3_FINAL",
                    "tool_call_envelopes": [],
                }

        def fake_execute(request):
            path = request["path"]
            return {
                "tool_id": "file_read",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": path,
                "content": f"MATERIAL_FROM_{path}",
                "has_more": False,
                "read_mode": "bounded",
                "protocol_tool_receipt": False,
            }

        round_num = 745
        runtime.sm.set("base.meta.total_round", round_num)
        runtime.audit.start(round_num, "interactive", {"source": "spec745_test"})
        runtime.ctx_store.append_to_cache(
            round_num, "user", "START_USER_INPUT",
            kind="interaction", step="setup")
        runtime.ctx_store.append_to_cache(
            round_num, "user", "START_RELAY_HANDOFF",
            kind="relay_handoff", step="setup")
        runtime.ctx_store.append_to_cache(
            round_num, "system", "START_SETUP_FACT",
            kind="setup_fact", step="setup")
        self._write_fake_call_layers(runtime)
        runtime.executor = ThreeFrameExecutor()
        runtime.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = runtime._run_reaction_loop(
            runtime.sm.load(),
            "interactive",
            [],
            interaction_meta=self._confirmed_meta(),
        )

        snapshots = [
            event for event in runtime.audit.get_store().read_events(round_num)
            if event["event_type"] == "step_input_snapshot"
            and event.get("phase") == "reaction"
        ]
        assert len(snapshots) == 3
        assert all(len(event["payload"]["layers_snapshot"]["layers"]) == 10
                   for event in snapshots)

        lately_1 = self._layer_text(snapshots[0], "30_lately")
        now_1 = self._layer_text(snapshots[0], "50_now")
        assert "START_USER_INPUT" not in lately_1
        assert "START_RELAY_HANDOFF" not in lately_1
        assert "START_SETUP_FACT" not in lately_1
        assert "START_USER_INPUT" in now_1
        assert "START_RELAY_HANDOFF" in now_1
        assert "START_SETUP_FACT" in now_1

        lately_2 = self._layer_text(snapshots[1], "30_lately")
        now_2 = self._layer_text(snapshots[1], "50_now")
        assert "START_USER_INPUT" in lately_2
        assert "START_RELAY_HANDOFF" in lately_2
        assert "START_SETUP_FACT" in lately_2
        assert "FRAME_1_PROGRESS" in now_2
        assert "MATERIAL_FROM_frame-1.txt" in now_2
        assert "FRAME_2_PROGRESS" not in now_2

        lately_3 = self._layer_text(snapshots[2], "30_lately")
        now_3 = self._layer_text(snapshots[2], "50_now")
        assert "frame-1.txt" in lately_3
        assert "MATERIAL_FROM_frame-1.txt" in lately_3
        assert "FRAME_2_PROGRESS" in now_3
        assert "MATERIAL_FROM_frame-2.txt" in now_3
        assert "FRAME_1_PROGRESS" not in now_3

        assert result["response"] == "FRAME_3_FINAL"
        assert runtime.ctx_store.get_now_entries() == []
        lately_entries = runtime.ctx_store.get_lately_entries("reaction")
        assert any(entry.get("content") == "FRAME_1_PROGRESS"
                   for entry in lately_entries)
        raw_text = Path(runtime.ctx_store._raw_log_md()).read_text(encoding="utf-8")
        assert "START_USER_INPUT" in raw_text
        assert "FRAME_1_PROGRESS" in raw_text
        assert "MATERIAL_FROM_frame-1.txt" not in raw_text

    def test_provider_transport_failure_does_not_consume_now(
            self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        monkeypatch.setattr(runtime.assembler, "_cached_or_build",
                            lambda *args, **kwargs: "")
        monkeypatch.setattr(runtime.assembler, "_build_high_freq",
                            lambda *args, **kwargs: "")
        monkeypatch.setattr(runtime.assembler.popup, "read_popup", lambda: "")
        runtime.sm.set("base.meta.total_round", 746)
        runtime.audit.start(746, "interactive", {})
        runtime.ctx_store.append_to_cache(
            746, "user", "UNCONSUMED_ON_FAILURE",
            kind="interaction", step="setup")
        self._write_fake_call_layers(runtime)

        class FailingExecutor:
            @staticmethod
            def call(*args, **kwargs):
                raise RuntimeError("isolated_transport_failure")

        runtime.executor = FailingExecutor()
        with pytest.raises(RuntimeError, match="isolated_transport_failure"):
            runtime._run_reaction_loop(
                runtime.sm.load(), "interactive", [],
                interaction_meta=self._confirmed_meta())

        assert [entry["content"] for entry in runtime.ctx_store.get_now_entries()] == [
            "UNCONSUMED_ON_FAILURE",
        ]
        assert runtime.ctx_store.get_lately_entries("reaction") == []

    def test_runtime_provider_boundary_passes_exact_c_target(
            self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        captured = {}

        def fake_transition(**kwargs):
            captured.update(kwargs)
            return {
                "schema_version": "current_cache_transition.v1",
                "status": "noop",
                "boundary": kwargs["boundary"],
            }

        monkeypatch.setattr(
            runtime.ctx_store, "transition_current_cache", fake_transition)
        runtime.reaction_loop_runner._transition_current_cache(
            745,
            boundary="reaction_provider_return",
            consumer_frame_id="R000745:reaction:3",
            phase="reaction",
            iteration=3,
            expire_call_transients=True,
        )

        assert captured["transient_round"] == 745
        assert captured["transient_target_step"] == "reaction"
        assert captured["transient_target_iteration"] == 3

    def test_cleanup_retry_reassembles_layers_and_expires_first_call_c_track(
            self, tmp_path, monkeypatch):
        runtime = self._make_runtime(tmp_path)
        runtime.sm.set("base.meta.total_round", 748)
        runtime.audit.start(748, "interactive", {})
        calls = []
        helper = self

        class CleanupRetryExecutor:
            def call(self, step, system, messages, **kwargs):
                calls.append(list(messages))
                if len(calls) == 1:
                    return {"response": "", "tool_call_envelopes": []}
                return {
                    "response": "",
                    "tool_call_envelopes": [helper._native_tool_envelope(
                        "cleanup_finalize",
                        {},
                        tool_family="substrate_tool",
                        tool_class="sync_tool",
                    )],
                }

        runtime.executor = CleanupRetryExecutor()
        monkeypatch.setattr(runtime.heat, "tick_decay", lambda **kwargs: None)
        monkeypatch.setattr(runtime, "_build_forgetting_context", lambda: "", raising=False)
        monkeypatch.setattr(runtime, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(runtime, "_process_rest_cycle", lambda *a, **kw: None)

        result = {"response": "FINAL", "_interaction_meta": self._confirmed_meta()}
        outcome = runtime._run_cleanup(
            "interactive",
            runtime.sm.load(),
            result,
            748,
            "C_TRACK_ONLY_ON_FIRST_CLEANUP_FRAME",
        )

        assert outcome["status"] in {"settled", "degraded"}
        assert len(calls) == 2
        first = "\n".join(item.get("content", "") for item in calls[0])
        second = "\n".join(item.get("content", "") for item in calls[1])
        assert "C_TRACK_ONLY_ON_FIRST_CLEANUP_FRAME" in first
        assert "C_TRACK_ONLY_ON_FIRST_CLEANUP_FRAME" not in second
        assert "上一轮 cleanup 输出缺少" in second
        assert "<!-- POPUP" in second
        assert runtime.ctx_store._current_now_blocks() == []

    @pytest.mark.parametrize("first_result", [
        {"response": "", "tool_call_envelopes": []},
        {
            "response": "",
            "tool_call_envelopes": [{
                "tool_id": "file_read",
                "call_id": "call_bad",
                "parse_status": "arguments_invalid",
            }],
        },
    ], ids=["empty", "malformed"])
    def test_successful_empty_or_malformed_frame_consumes_before_correction(
            self, tmp_path, monkeypatch, first_result):
        runtime = self._make_runtime(tmp_path)
        monkeypatch.setattr(runtime.assembler, "_cached_or_build",
                            lambda *args, **kwargs: "")
        monkeypatch.setattr(runtime.assembler, "_build_high_freq",
                            lambda *args, **kwargs: "")
        monkeypatch.setattr(runtime.assembler.popup, "read_popup", lambda: "")
        runtime.sm.set("base.meta.total_round", 747)
        runtime.audit.start(747, "interactive", {})
        runtime.ctx_store.append_to_cache(
            747, "user", "CONSUMED_AFTER_SUCCESS",
            kind="interaction", step="setup")
        self._write_fake_call_layers(runtime)

        class SuccessThenStopExecutor:
            def __init__(self):
                self.calls = 0

            def call(self, *args, **kwargs):
                self.calls += 1
                if self.calls == 1:
                    return first_result
                raise RuntimeError("stop_after_success")

        runtime.executor = SuccessThenStopExecutor()
        with pytest.raises(RuntimeError, match="stop_after_success"):
            runtime._run_reaction_loop(
                runtime.sm.load(), "interactive", [],
                interaction_meta=self._confirmed_meta())

        assert runtime.ctx_store.get_now_entries() == []
        assert [entry["content"]
                for entry in runtime.ctx_store.get_lately_entries("reaction")] == [
            "CONSUMED_AFTER_SUCCESS",
        ]
