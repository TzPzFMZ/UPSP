import json
import threading
from types import SimpleNamespace

import pytest


@pytest.mark.parametrize("encoding", ["utf-8\x00", "base64_codec"])
@pytest.mark.parametrize("tool_id", ["file_read", "file_write", "file_edit", "file_grep"])
def test_encoding_argument_returns_rejection(tmp_path, encoding, tool_id):
    from logic.general_tools import execute_general_tool_call
    target = tmp_path / "encoding.txt"
    result = execute_general_tool_call({"tool_id": tool_id, "path": str(target),
        "encoding": encoding, "content": "hello", "purpose": "isolated review"},
        allowed_roots=[tmp_path])
    assert result["status"] == "rejected"
    assert not target.exists()


@pytest.mark.parametrize("kind", ["uncertain", "context", "provider"])
def test_frame_audit_failure_keeps_original_uncertain_effect(tmp_path, monkeypatch, kind):
    from UPSP.OS.tests.runtime_test_helpers import RuntimeTestMixin
    from UPSP.OS.tests.test_reaction_finalize_occam import _minimal_reaction_context
    from errors import APIBridgeError, RequiredContextError, UncertainEffectError
    from engines.runtime_control import RuntimeControl
    rt = RuntimeTestMixin()._make_runtime(tmp_path)
    _minimal_reaction_context(rt, monkeypatch)
    runner = rt.reaction_loop_runner
    monkeypatch.setattr(runner, "_recover_provider_interruption_if_possible", lambda *a, **k: None)
    original = UncertainEffectError("write_rollback_failed", affected_ids=["MEM-80800001"],
        cause=OSError("write full"), rollback_causes=[OSError("restore full")],
        partial_receipts=[{"tool_id": "memory_write", "status": "error", "mem_id": "MEM-80800001"}])
    receipts = original.partial_receipts
    if kind == "context":
        original = RequiredContextError("projection", "frame", OSError("context full"))
    elif kind == "provider":
        original = APIBridgeError("provider unavailable")
        original.__cause__ = OSError("transport unavailable")
    def fail_effect(*a, **k):
        raise original
    def fail_audit(*a, **k):
        raise OSError("audit full")
    rt.executor = SimpleNamespace(call=fail_effect)
    monkeypatch.setattr(runner, "_round_audit_settlement", fail_audit)
    session = runner.start_session(rt.sm.load(), "interactive", [],
        trigger_id="T00000001", caused_by="R000000:setup:1")
    session.result_state.all_protocol_tool_receipts.extend(receipts)
    with pytest.raises(Exception) as caught:
        runner.run_frame(session)
    result = RuntimeControl.step_exception_result("reaction", caught.value)
    print("frame failure projection:", json.dumps(result, ensure_ascii=False))
    assert caught.value is original
    if kind == "uncertain":
        assert result["_persistent_effect_failure"]["affected_ids"] == ["MEM-80800001"]
    elif kind == "context":
        assert result["_required_context_failure"] == original.as_dict()
    else:
        assert result["_local_blocked_reason"] == "blocked/runtime_error"
        assert str(caught.value.__cause__) == "transport unavailable"
    assert result.get("protocol_tool_receipts") == receipts
    assert result["_persistent_effect_failure"]["settlement_failure"]["cause"] == "audit full"
    assert result["_persistent_effect_uncertain"]
    assert session.completed and runner.run_frame(session) is None


def test_guide_wrapper_preserves_nested_recovery_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.rhythm_guide_materializer import materialize_current_rhythm_guide
    from errors import UncertainEffectError
    store = WorkbenchStore(str(tmp_path / "workbench"))
    guide_id = materialize_current_rhythm_guide(store, {"rhythm_due": True}, round_num=808)
    original = UncertainEffectError("chronicle_write_rollback_failed", affected_ids=["scope-808"],
        cause=OSError("original write full"), rollback_causes=[OSError("restore denied")])
    def commit(*a, **k):
        raise original
    with pytest.raises(UncertainEffectError) as caught:
        apply_guide_submit(store, {"guide_id": guide_id, "item_id": "rhythm_due",
            "option_id": "write_chronicle", "fields": {"content": "isolated"}}, {
            "round_num": 808, "chronicle_store": SimpleNamespace(commit_write_scope=commit),
            "chronicle_write_scope": {"schema_version": "chronicle_write_scope.v1", "scope_id": "scope-808",
                "layer": "rhythms", "round_num": 808, "round_type": "rhythm", "source_refs": ["round:808"]}})
    evidence = caught.value.as_dict()
    print("guide recovery projection:", json.dumps(evidence, ensure_ascii=False))
    assert "scope-808" in evidence["affected_ids"]
    assert "restore denied" in evidence["rollback_causes"]
    assert evidence["cause_details"]["cause"] == "original write full"
    assert caught.value.partial_receipts[0]["backend_receipts"]


def test_first_resident_write_retains_affected_id(tmp_path, monkeypatch):
    from data.resident_list_store import ResidentListStore
    from errors import UncertainEffectError
    store = ResidentListStore(tmp_path / "missing-resident.json")
    assert store.snapshot_bytes() is None
    write = store._write_verified
    def fail_write(document):
        write(document)
        raise OSError("write/readback unavailable")
    def fail_restore(before):
        raise OSError("restore unavailable")
    monkeypatch.setattr(store, "_write_verified", fail_write)
    monkeypatch.setattr(store, "restore_bytes", fail_restore)
    with pytest.raises(UncertainEffectError) as caught:
        store.add({"item_type": "memory", "item_id": "MEM-80800001"})
    print("first resident recovery:", caught.value.as_dict())
    assert caught.value.affected_ids == ["MEM-80800001"]


def test_http_catalog_failure_keeps_recovery_details_and_blocks_followup(tmp_path, monkeypatch):
    from UPSP.OS.tests.test_seed_gui_server import _close, _json_headers, _request, _server
    from UPSP.OS.tests.test_resident_runtime import service as resident_service
    from errors import WriteError
    resident = resident_service(tmp_path / "runtime")
    server, thread = _server(tmp_path / "http", resident)
    service = server.RequestHandlerClass.settings_service
    save = service.configs.save
    resolve = service.configs.resolve_model_routes
    calls = []
    def fail_restore(name, data):
        calls.append(name)
        if name == "models" and calls.count("models") == 2:
            raise WriteError("models", message="rollback unavailable")
        return save(name, data)
    def fail_verify():
        raise TypeError("verify fault after write")
    monkeypatch.setattr(service.configs, "save", fail_restore)
    monkeypatch.setattr(service.configs, "resolve_model_routes", fail_verify)
    def update(alias):
        return _request(server, "POST", "/api/settings/model-catalog", body=json.dumps({
            "entity": "model", "action": "update", "id": "model_test", "values": {"alias": alias},
            "revision": service.configs.revision("models")}), headers=_json_headers(server))
    try:
        status, failure = update("unconfirmed change")
        monkeypatch.setattr(service.configs, "save", save)
        monkeypatch.setattr(service.configs, "resolve_model_routes", resolve)
        next_status, _ = update("followup change")
        print("catalog HTTP failure:", status, failure, "followup:", next_status)
        assert status == 503
        assert next_status == 409, "Unconfirmed catalog mutation must block subsequent writes"
        assert failure["persistent_effect_failure"]["affected_ids"] == ["model_test", "models", "model_routing"]
        saved = json.loads(resident.supervisor_path.read_text(encoding="utf-8"))
        assert saved["last_outcome"]["persistent_effect_failure"] == failure["persistent_effect_failure"]
        assert resident.status()["interrupted_recovery"]["pending"]
        assert resident.runtime.control.stop_latched
        assert resident.runtime.hb._paused
        read_status, read_result = _request(server, "GET", "/api/runtime/status")
        assert read_status == 200 and read_result["interrupted_recovery"]["pending"]
        _, stop_result = _request(server, "POST", "/api/runtime/stop", body="{}", headers=_json_headers(server))
        assert stop_result.get("error") != "persistent_effect_recovery_required"
        resident.close()
        restarted = resident_service(tmp_path / "runtime")
        restarted.start()
        try:
            assert restarted.status()["interrupted_recovery"]["pending"]
            assert restarted.last_outcome["persistent_effect_failure"] == failure["persistent_effect_failure"]
            with pytest.raises(Exception, match="persistent_effect_recovery_required"):
                restarted.submit_message("retry", "limited")
        finally:
            restarted.close()
    finally:
        _close(server, thread)


@pytest.mark.parametrize("active", [False, True])
@pytest.mark.parametrize("persist_fails", [False, True])
def test_uncertain_mutation_stops_runtime_even_if_supervisor_fails(tmp_path, monkeypatch, active, persist_fails):
    from UPSP.OS.tests.test_resident_runtime import FakeRuntime, service
    from engines.resident_runtime import RuntimeServiceError
    from errors import UncertainEffectError
    runtime = FakeRuntime()
    resident = service(tmp_path / "runtime", runtime)
    resident.start()
    failure = UncertainEffectError("model_catalog_recovery_required", affected_ids=["models"],
        cause=OSError("write failed"), rollback_causes=[OSError("restore failed")])
    try:
        if active:
            runtime.control.establish_round("interactive", lambda: 808)
        with monkeypatch.context() as patch:
            if persist_fails:
                def fail(*args, **kwargs):
                    raise OSError("supervisor full")
                patch.setattr(resident, "_write_supervisor", fail)
                with pytest.raises(UncertainEffectError) as caught:
                    resident.record_uncertain_effect(failure)
                assert caught.value.affected_ids == ["models"]
                assert "supervisor full" in caught.value.as_dict()["rollback_causes"]
            else:
                resident.record_uncertain_effect(failure)
            assert runtime.hb._paused
            if active:
                assert runtime.control.stop_requested.is_set() and runtime.executor.cancelled == 1
            else:
                assert runtime.control.stop_latched
            with pytest.raises(RuntimeServiceError, match="persistent_effect_recovery_required"):
                resident._begin_operation("send", "limited")
            assert resident.status()["interrupted_recovery"]["pending"]
    finally:
        resident.close()


@pytest.mark.parametrize("round_has_failure", [False, True])
def test_round_completion_cannot_overwrite_concurrent_catalog_recovery(tmp_path, round_has_failure):
    from UPSP.OS.tests.test_resident_runtime import service
    from errors import UncertainEffectError
    resident = service(tmp_path / "runtime")
    resident.start()
    read_started, release_read, recorded = threading.Event(), threading.Event(), threading.Event()
    failures = []

    class PausedOutcome(dict):
        def get(self, key, default=None):
            value = super().get(key, default)
            if key == "persistent_effect_failure" and threading.current_thread().name == "round-finish":
                read_started.set()
                assert release_read.wait(5)
            return value

    resident.last_outcome = PausedOutcome()
    result = {"_settlement": {"status": "unsettled"}}
    if round_has_failure:
        result["_persistent_effect_failure"] = {"reason": "memory_write_failed", "affected_ids": ["MEM-808"]}

    def finish():
        try:
            resident._on_round_finished(808, "interactive", result)
        except Exception as exc:
            failures.append(exc)

    def record():
        try:
            resident.record_uncertain_effect(UncertainEffectError(
                "model_catalog_recovery_required", affected_ids=["models"], cause=OSError("catalog restore failed")))
        except Exception as exc:
            failures.append(exc)
        finally:
            recorded.set()

    finishing = threading.Thread(target=finish, name="round-finish")
    recording = threading.Thread(target=record)
    try:
        finishing.start()
        assert read_started.wait(5)
        recording.start()
        recorded.wait(0.2)
        release_read.set()
        finishing.join(5)
        recording.join(5)
        assert not finishing.is_alive() and not recording.is_alive() and not failures
        saved = json.loads(resident.supervisor_path.read_text(encoding="utf-8"))
        affected = saved["last_outcome"]["persistent_effect_failure"]["affected_ids"]
        assert "models" in affected
        if round_has_failure:
            assert "MEM-808" in affected
        assert resident.status()["interrupted_recovery"]["pending"]
    finally:
        release_read.set()
        resident.close()
