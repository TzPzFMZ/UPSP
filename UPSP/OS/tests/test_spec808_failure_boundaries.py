"""跨工具失败必须保全已完成结果；全部数据由 conftest 和 tmp_path 隔离。"""
import io
import socket
import subprocess
from http.client import HTTPResponse
from types import SimpleNamespace

import pytest

from data.action_recovery_store import ActionRecoveryStore
from engines.general_tool_dispatcher import GeneralToolDispatcher
from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
from errors import RequiredContextError
from logic import general_tools as gt


def test_alert_append_serializes_shared_read_modify_write(tmp_path, monkeypatch):
    from concurrent.futures import ThreadPoolExecutor
    import time
    from data import alert_store
    path = tmp_path / "alerts.md"
    monkeypatch.setattr(alert_store, "ALERTS_MD", str(path))
    original = alert_store.AlertStore.read
    def slow_read(store):
        text = original(store)
        time.sleep(0.005)
        return text
    monkeypatch.setattr(alert_store.AlertStore, "read", slow_read)
    with ThreadPoolExecutor(max_workers=4) as pool:
        list(pool.map(lambda n: alert_store.AlertStore().append_alert(n, "test", "fault", f"item-{n}", "preserve"), range(20)))
    assert len([line for line in path.read_text(encoding="utf-8").splitlines() if line.startswith("- `")]) == 20
    path.write_bytes(b"\xff")
    from errors import ReadError
    with pytest.raises(ReadError):
        alert_store.AlertStore().append_alert(21, "test", "fault", "bad input", "preserve")
    assert path.read_bytes() == b"\xff"


@pytest.mark.parametrize("tool_id", ["file_read", "file_write"])
def test_unknown_codec_is_a_receipt(tmp_path, tool_id):
    path = tmp_path / "source.txt"
    path.write_text("hello", encoding="utf-8")
    result = gt.execute_general_tool_call(
        {"tool_id": tool_id, "path": str(path), "encoding": "no-such-codec",
         "content": "hello", "purpose": "probe"}, allowed_roots=[tmp_path])
    assert result["status"] in {"rejected", "failed"}
    assert path.read_text() == "hello"


@pytest.mark.parametrize("url", ["https://[broken/", "https://example.com:bad/", "https://example.com:99999/"])
def test_invalid_url_rejected_before_network(url, monkeypatch):
    monkeypatch.setattr(gt.socket, "getaddrinfo", lambda *a, **k: pytest.fail("must reject before DNS"))
    assert gt.execute_general_tool_call({"tool_id": "web_fetch", "url": url})["status"] == "rejected"


def test_search_dns_failure_is_structured(tmp_path, monkeypatch):
    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(tmp_path / "health.json"))
    monkeypatch.setattr(gt, "_ordered_web_backends", lambda *a, **k: ["html_fallback"])
    calls = []
    def dns(host, *args, **kwargs):
        calls.append(host)
        raise socket.gaierror("synthetic")
    monkeypatch.setattr(gt.socket, "getaddrinfo", dns)
    result = gt.execute_general_tool_call({"tool_id": "web_search", "query": "probe"})
    assert result["status"] == "failed"
    assert "www.bing.com" in calls
    assert "host_resolution_failed" in result["backend_attempts"][0]["detail"]


@pytest.mark.parametrize("tool_id", ["web_fetch", "web_search"])
def test_truncated_http_is_structured(tmp_path, monkeypatch, tool_id):
    class Opener:
        def open(self, request, timeout):
            response = HTTPResponse(SimpleNamespace(makefile=lambda *a: io.BytesIO(
                b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nab")))
            response.begin()
            response.url = request.full_url
            return response
    monkeypatch.setenv("UPSP_WEB_BACKEND_HEALTH_PATH", str(tmp_path / "health.json"))
    monkeypatch.setattr(gt.socket, "getaddrinfo", lambda *a, **k: [(2, 1, 6, "", ("93.184.216.34", 443))])
    monkeypatch.setattr(gt, "build_opener", lambda *a: Opener())
    monkeypatch.setattr(gt, "_ordered_web_backends", lambda *a, **k: ["html_fallback"] if tool_id == "web_search" else ["direct_http"])
    result = gt.execute_general_tool_call({"tool_id": tool_id, "query": "probe", "url": "https://example.com/"})
    assert result["status"] == "failed"


def test_bad_search_candidate_does_not_discard_valid_candidate():
    result = gt.execute_general_tool_call({"tool_id": "web_search", "query": "probe"},
        web_search_fn=lambda *a: [{"url": "https://[broken/"}, {"url": "https://example.com/", "title": "good"}])
    assert result["status"] == "ok"
    assert [r["title"] for r in result["results"]] == ["good"]
    assert result["rejected_result_count"] == 1


@pytest.mark.parametrize("tool_id,field,extra", [
    ("file_read", "path", {}), ("file_write", "path", {"content": "x", "purpose": "probe"}),
    ("file_edit", "path", {"patch": "x", "purpose": "probe"}),
    ("file_glob", "root", {"pattern": "*"}), ("file_grep", "root", {"query": "x"}),
    ("shell_command", "cwd", {"command": "echo x", "purpose": "probe"}),
])
def test_nul_path_rejected_by_real_dispatcher(tmp_path, tool_id, field, extra):
    request = {"tool_id": tool_id, field: str(tmp_path / "bad") + "\x00", **extra}
    result = GeneralToolDispatcher(approval_fn=lambda _: pytest.fail("invalid input must not ask approval")).handle_requests(
        [request], {}, runtime_context={"execution_permission_level": "guarded"})
    assert result[0]["status"] == "rejected"
    assert result[0].get("reason") == "invalid_path"


def _runner(tmp_path):
    ledger = ActionRecoveryStore(tmp_path / "ledger.json")
    return SimpleNamespace(sm=None, execution_permission_level="guarded", action_recovery_store=ledger,
        general_tool_dispatcher=GeneralToolDispatcher(action_recovery_store=ledger, approval_fn=lambda _: "allow_once"),
        _write_general_tool_results=lambda *a: None)


def _settle(runner, requests, results):
    return ReactionToolSettlementDispatcher(runner).handle_general_tool_results(
        iter_general_tool_requests=requests, active_general_tool_guides={}, accumulated_messages=[],
        iter_native_tool_call_envelopes=[], all_general_tool_results=results, iter_native_feedbacks=[],
        round_num=808, iteration=1, interaction_meta={})


def test_later_unexpected_failure_keeps_prior_effect_result(tmp_path, monkeypatch):
    runner = _runner(tmp_path)
    target = tmp_path / "effect.txt"
    def crash(*args, **kwargs): raise RuntimeError("unexpected handler defect")
    monkeypatch.setattr(gt, "_execute_web_search", crash)
    results = []
    with pytest.raises(RuntimeError, match="unexpected handler defect"):
        _settle(runner, [{"tool_id": "file_write", "path": str(target), "content": "written", "purpose": "probe"},
                         {"tool_id": "web_search", "query": "probe"}], results)
    assert target.read_text() == "written"
    assert len(results) == 1 and results[0]["status"] == "ok"
    assert runner.action_recovery_store.load()["items"][0]["phase"] == "result_recorded"


def test_projection_failure_preserves_result_and_cause_stops_next_tool(tmp_path, monkeypatch):
    runner = _runner(tmp_path)
    def fail_projection(*a): raise RequiredContextError("projection", "general_tool_fact", OSError("full"))
    runner._write_general_tool_results = fail_projection
    monkeypatch.setattr(gt, "_execute_web_search", lambda *a, **k: pytest.fail("next tool must not execute"))
    results = []
    with pytest.raises(RequiredContextError):
        _settle(runner, [{"tool_id": "file_write", "path": str(tmp_path / "effect.txt"), "content": "x", "purpose": "probe"},
                         {"tool_id": "web_search", "query": "probe"}], results)
    assert len(results) == 1
    assert runner.action_recovery_store.load()["items"][0]["result"]["status"] == "ok"


def test_callback_typeerror_does_not_execute_twice():
    calls = []
    def execute(call, **kwargs):
        calls.append(call)
        raise TypeError("callback body failed")
    with pytest.raises(TypeError, match="callback body failed"):
        GeneralToolDispatcher(execute_fn=execute)._execute_call({"tool_id": "web_search"})
    assert len(calls) == 1


@pytest.mark.parametrize("backend_result", [None, [], {}, ["bad"], {"status": []},
    {"status": "invented"}, {"status": "ok", "modified_files": {"bad": True}},
    {"status": "ok", "modified_files": [{}]}])
def test_native_subagent_array_reaches_backend_and_validates_result(tmp_path, backend_result):
    request = {"tool_id": "subagent_dispatch", "task_goal": "probe", "allowed_paths": [str(tmp_path)],
               "expected_artifacts": "report", "task_mode": "read_only"}
    from logic.native_tool_calls import _native_argument_validation_error, _request_from_envelope
    envelope = {"tool_id": "subagent_dispatch", "arguments": {k: v for k, v in request.items() if k != "tool_id"}}
    assert _native_argument_validation_error(envelope) is None
    request = _request_from_envelope(envelope)
    result = gt.execute_general_tool_call(request, allowed_roots=[tmp_path], subagent_dispatch_fn=lambda *a: backend_result)
    assert result["status"] == "failed"
    assert result["reason"] == "invalid_subagent_result"


def test_malformed_subagent_result_preserves_unknown_action(tmp_path):
    from data.action_recovery_store import ActionRecoveryEffectError
    ledger = ActionRecoveryStore(tmp_path / "ledger.json")
    with pytest.raises(ActionRecoveryEffectError, match="subagent_result_unconfirmed"):
        gt.execute_general_tool_call({"tool_id": "subagent_dispatch", "task_goal": "probe",
            "allowed_paths": [str(tmp_path)], "expected_artifacts": "report", "task_mode": "read_only"},
            allowed_roots=[tmp_path], subagent_dispatch_fn=lambda *a: [], action_recovery={
                "store": ledger, "request_sha256": "8" * 64,
                "runtime_context": {"round_num": 808, "iteration": 1}, "call_id": "bad-backend"})
    item = ledger.load()["items"][0]
    assert item["phase"] == "launching" and item["result"] is None
    assert ledger.classify_pending()["outcome_unknown"] == 1


def test_fault_record_keeps_alert_if_context_write_fails():
    from logic.fault_record import apply_fault_record_declarations
    from errors import UncertainEffectError
    alerts = []
    def fail(*a, **k): raise OSError("context full")
    request = {"fault_type": "tool_failure", "severity": "error", "step": "reaction", "source": "probe", "detail": "isolated"}
    with pytest.raises(UncertainEffectError) as caught:
        apply_fault_record_declarations([request, request], 808, {
            "alert_store": SimpleNamespace(append_alert=lambda **row: alerts.append(row)),
            "context_store": SimpleNamespace(append_to_cache=fail)})
    assert len(alerts) == 1
    assert len(caught.value.partial_receipts) == 1
    assert str(caught.value.cause) == "context full"


@pytest.mark.parametrize("bad_id,category", [("../outside", "ours"), ("safe", "../outside"), ("nul\x00", "ours")])
def test_relation_path_rejects_before_any_write(bad_id, category):
    from data.relation_store import RelationStore
    with pytest.raises(ValueError, match="invalid_relation_"):
        RelationStore().get_card_path(bad_id, category)
