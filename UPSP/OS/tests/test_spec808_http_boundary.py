import http.client
import json
import socket

import pytest

from UPSP.OS.tests.test_seed_gui_server import FakeCli, _close, _json_headers, _request, _server


def test_lost_http_reply_is_not_replayed_and_restart_is_fenced(tmp_path):
    runtime = FakeCli()
    server, thread = _server(tmp_path, runtime)
    body = json.dumps({"message": "isolated", "permission_level": "guarded", "unlimited_confirmed": False})
    headers = {**_json_headers(server), "X-UPSP-Request-Id": "808-1111111111111111",
               "X-UPSP-Request-Session": server.request_session}
    handler = server.RequestHandlerClass
    configs = handler.settings_service.configs
    models = configs.load("models")
    models["connections"][0]["api_key"] = "isolated-fake-key"
    configs.save("models", models)
    original = handler._send_json

    def lose_reply(request, status, payload):
        if isinstance(payload, dict) and payload.get("command") == "send":
            request.connection.shutdown(socket.SHUT_RDWR)
            request.connection.close()
            return
        original(request, status, payload)

    handler._send_json = lose_reply
    try:
        with pytest.raises(http.client.RemoteDisconnected):
            _request(server, "POST", "/api/runtime/send", body=body, headers=headers)
        assert len(runtime.calls) == 1
        status, result = _request(server, "POST", "/api/runtime/send", body=body, headers=headers)
        assert (status, result["error"]) == (409, "submission_outcome_unconfirmed")
        assert len(runtime.calls) == 1
        old_session = server.request_session
    finally:
        _close(server, thread)
    second, thread = _server(tmp_path / "restarted", runtime)
    try:
        assert second.request_session != old_session
        headers.update(_json_headers(second))
        status, result = _request(second, "POST", "/api/runtime/send", body=body, headers=headers)
        assert (status, result["error"]) == (409, "submission_outcome_unconfirmed")
        assert len(runtime.calls) == 1
    finally:
        _close(second, thread)


def test_catalog_rollback_attempts_both_files_and_returns_uncertain(tmp_path, monkeypatch):
    from errors import UncertainEffectError, WriteError
    server, thread = _server(tmp_path, FakeCli())
    service = server.RequestHandlerClass.settings_service
    original = service.configs.save
    calls = []
    def save(name, data):
        calls.append(name)
        if calls.count("models") == 2 and name == "models":
            raise WriteError("models", message="rollback unavailable")
        return original(name, data)
    monkeypatch.setattr(service.configs, "save", save)
    monkeypatch.setattr(service.configs, "resolve_model_routes",
                        lambda: (_ for _ in ()).throw(TypeError("original implementation fault")))
    try:
        with pytest.raises(UncertainEffectError) as caught:
            service.update_model_catalog("model", "update", "model_test", {"alias": "changed"},
                                         service.configs.revision("models"))
        assert calls[-2:] == ["models", "model_routing"]
        assert isinstance(caught.value.cause, TypeError)
        assert "model_test" in caught.value.affected_ids
        monkeypatch.setattr(service, "update_model_catalog",
                            lambda *args: (_ for _ in ()).throw(caught.value))
        status, result = _request(server, "POST", "/api/settings/model-catalog",
            body=json.dumps({"revision": "isolated", "entity": "model", "action": "update", "id": "model_test", "values": {}}),
            headers=_json_headers(server))
        assert (status, result["error"]) == (503, "model_catalog_recovery_required")
    finally:
        _close(server, thread)
