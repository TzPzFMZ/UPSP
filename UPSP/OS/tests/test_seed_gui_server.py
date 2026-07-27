import http.client
import hashlib
import json
import os
import re
import socket
import threading
from pathlib import Path
from types import SimpleNamespace

import pytest

from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path
from initialization.persona_initializer import PersonaInitializer


REPO_ROOT = Path(__file__).resolve().parents[3]
SERVER_PATH = REPO_ROOT / "tools" / "serve_seed_gui.py"
MEMORY_STORE_PATH = REPO_ROOT / "UPSP" / "OS" / "data" / "memory_store.py"
GUI_ROOT = REPO_ROOT / "UPSP" / "gui"
GUI_TS_MODULES = (
    "bootstrap.ts",
    "contracts.ts",
    "state.ts",
    "i18n.ts",
    "markdown.ts",
    "markdown-mermaid.ts",
    "view.ts",
    "runtime.ts",
    "events.ts",
    "app.ts",
)


class IsolatedConfigStore:
    """Exercise the settings boundary without touching the working checkout."""

    template_root = (
        REPO_ROOT / "UPSP" / "initialization" / "os_template" / "config"
    )
    source_paths = {
        "system": template_root / "system.json",
        "now": template_root / "context" / "now.json",
        "lately": template_root / "context" / "lately.json",
        "periodic": template_root / "context" / "periodic.json",
        "high_freq": template_root / "context" / "high_freq.json",
        "relation": template_root / "relation.json",
        "memory": template_root / "memory.json",
    }

    def __init__(self, root, canonical_cls):
        self.root = Path(root)
        self.canonical_cls = canonical_cls
        self.use_api_environment = False
        self.root.mkdir(parents=True, exist_ok=True)
        self.paths = {}
        for name, source in self.source_paths.items():
            target = self.root / f"{name}.json"
            target.write_bytes(source.read_bytes())
            self.paths[name] = target
        defaults = {
            "interface": {"schema_version": "upsp_interface_settings.v1", "locale": "system"},
            "models": {
                "schema_version": "upsp_model_catalog.v1",
                "connections": [{
                    "id": "conn_test", "alias": "测试连接", "protocol": "openai_chat",
                    "url": "https://example.invalid/v1/chat/completions",
                    "api_key_env": "UPSP_TEST_KEY", "api_key": "",
                }],
                "models": [{
                    "id": "model_test", "alias": "测试模型", "connection_id": "conn_test",
                    "model": "test-model", "context_window": 100000,
                    "reasoning": {"supported": ["medium"], "default": "medium"},
                    "streaming": {"enabled": True, "protocol": "openai_sse", "include_usage": True},
                    "prompt_cache": {"profile": "off"}, "request_overrides": {},
                }],
                "transport": {
                    "handshake": {
                        "timeout_seconds": 10, "retry": 2,
                        "request_timeout_seconds": 180,
                        "stream_first_chunk_timeout_seconds": 180,
                        "stream_idle_timeout_seconds": 180,
                        "stream_content_overrun_chars": 65536,
                    },
                    "circuit_breaker": {"max_failures": 3, "cooldown_seconds": 900},
                },
            },
            "model_routing": {
                "schema_version": "upsp_persona_model_routing.v1",
                "cross_phase_failover_enabled": True,
                "routes": {
                    "setup": {"primary": {"model_id": "model_test", "reasoning_effort": "medium"}, "backups": [None, None]},
                    "reaction": {"primary": None, "backups": [None, None]},
                    "cleanup": {"primary": None, "backups": [None, None]},
                },
            },
        }
        for name, payload in defaults.items():
            target = self.root / f"{name}.json"
            target.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
            self.paths[name] = target

    def load(self, name):
        if name == "api":
            override = os.environ.get("UPSP_API_CONFIG_OVERRIDE_JSON", "").strip()
            if self.use_api_environment and override:
                return self.canonical_cls._normalise_api_override(json.loads(override))
            return self.canonical_cls._build_api_projection(self)
        return json.loads(self.paths[name].read_text(encoding="utf-8"))

    def save(self, name, payload):
        if name in {"interface", "models", "model_routing", "memory"}:
            self.canonical_cls._validate(name, payload)
        self.paths[name].write_text(
            json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    def revision(self, name):
        if name == "api":
            payload = json.dumps(self.load("api"), ensure_ascii=False, sort_keys=True).encode("utf-8")
            return hashlib.sha256(payload).hexdigest()
        return hashlib.sha256(self.paths[name].read_bytes()).hexdigest()

    def init_all(self):
        return []

    def __getattr__(self, name):
        descriptor = self.canonical_cls.__dict__.get(name)
        if isinstance(descriptor, staticmethod):
            return descriptor.__func__
        attribute = getattr(self.canonical_cls, name)
        return attribute.__get__(self, type(self)) if hasattr(attribute, "__get__") else attribute


def _gui_ts_source(*modules):
    names = modules or GUI_TS_MODULES
    return "\n".join(
        (GUI_ROOT / "src" / name).read_text(encoding="utf-8")
        for name in names
    )


class FakeCli:
    def __init__(
        self,
        *,
        send_returncode=0,
        relay_returncode=0,
        relay_error_code="",
        fail_with=None,
        stop_receipt=None,
    ):
        self.calls = []
        self.message_bytes = b""
        self.message_path = None
        self.send_returncode = send_returncode
        self.relay_returncode = relay_returncode
        self.relay_error_code = relay_error_code
        self.fail_with = fail_with
        self.stop_receipt = stop_receipt
        self.runtime_error = RuntimeError
        self.host = {"address": "127.0.0.1", "port": 0}
        self.closed = False

    def start(self, *, host_address, port):
        self.host = {"address": host_address, "port": port}
        return self.status()

    def start_if_ready(self):
        return True

    def close(self):
        self.closed = True

    def status(self):
        return {
            "session_id": "fake-session",
            "process_id": os.getpid(),
            "supervisor_schema": "upsp_runtime_supervisor.v1",
            "supervisor_state": "running",
            "supervisor_path": "isolated/supervisor.json",
            "host": dict(self.host),
            "operation_in_flight": False,
            "send_in_flight": False,
            "relay_in_flight": False,
            "runtime": {
                "round_in_flight": False,
                "current_round": None,
                "round_type": None,
                "stage": "idle",
                "stop_requested": False,
                "stop_latched": False,
                "can_stop": False,
                "heartbeat_suspended": False,
            },
            "last_outcome": {},
        }

    def submit_message(self, message, permission_level):
        self.calls.append((["resident", "send", permission_level], {}))
        self.message_bytes = str(message).encode("utf-8")
        if self.fail_with:
            raise self.runtime_error("runtime_host_failed")
        if self.send_returncode:
            raise self.runtime_error("runtime_send_failed")
        return {
            "status": "round_completed",
            "round_num": 1,
            "round_type": "interactive",
            "final_response": "ok",
        }

    def submit_pending(self, kind, permission_level):
        self.calls.append((["resident", kind, permission_level], {}))
        if self.fail_with:
            raise self.runtime_error("runtime_host_failed")
        if self.relay_returncode:
            raise self.runtime_error(
                self.relay_error_code or "runtime_relay_failed")
        return {
            "status": "round_completed",
            "round_num": 1,
            "round_type": kind,
            "final_response": "ok",
        }

    def stop_round(self):
        if self.stop_receipt is not None:
            return dict(self.stop_receipt)
        raise self.runtime_error("no_round_in_flight")


def _gui_root(tmp_path):
    root = tmp_path / "gui"
    manual = root / "manual"
    source = root / "src"
    markdown_assets = root / "assets" / "markdown"
    manual.mkdir(parents=True)
    source.mkdir()
    markdown_assets.mkdir(parents=True)
    (root / "assets" / "upsp-logo.png").write_bytes(b"png")
    (root / "index.html").write_text("<!doctype html><title>Seed GUI</title>", encoding="utf-8")
    (root / "styles.css").write_text("body{}", encoding="utf-8")
    (root / "markdown.css").write_text(".md-document{}", encoding="utf-8")
    (root / "app.js").write_text("void 0;", encoding="utf-8")
    (root / "markdown-mermaid.js").write_text("export {};", encoding="utf-8")
    (markdown_assets / "KaTeX_Test.woff2").write_bytes(b"woff2")
    (markdown_assets / "secret.txt").write_text("not served", encoding="utf-8")
    (manual / "intro.md").write_text("# Intro", encoding="utf-8")
    (manual / "intro.en-US.md").write_text("# Introduction", encoding="utf-8")
    (source / "app.ts").write_text("document.title = 'not served';", encoding="utf-8")
    (root / "package.json").write_text("{}", encoding="utf-8")
    (root / "tsconfig.json").write_text("{}", encoding="utf-8")
    (root / "secret.txt").write_text("not served", encoding="utf-8")
    return root


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("version", "0.1"),
        ("windows_file_version", "0.1-alpha"),
        ("repository_url", "javascript:alert(1)"),
        ("releases_url", "file:///tmp/releases"),
    ),
)
def test_spec707_product_manifest_rejects_invalid_release_identity(
    tmp_path, monkeypatch, field, value
):
    module = _load_module_from_path(
        f"serve_seed_gui_bad_product_{field}", SERVER_PATH)
    product = json.loads(
        (REPO_ROOT / "UPSP" / "product.json").read_text(encoding="utf-8"))
    product[field] = value
    manifest = tmp_path / "product.json"
    manifest.write_text(json.dumps(product), encoding="utf-8")
    monkeypatch.setattr(module, "PRODUCT_MANIFEST_PATH", manifest)
    with pytest.raises(RuntimeError, match="product_manifest_invalid"):
        module._load_product_manifest()


def _server(
    tmp_path,
    fake_cli,
    deposition_reader=None,
    protocol_reader=None,
    persona_reader=None,
    bootstrap_service=None,
    bootstrap_factory=None,
    desktop_control_token=None,
    desktop_session_id=None,
):
    module = _load_module_from_path("serve_seed_gui", SERVER_PATH)
    fake_cli.runtime_error = module.RuntimeServiceError
    round_dir = tmp_path / "round"
    round_dir.mkdir(parents=True)
    settings_service = module.SettingsService(
        IsolatedConfigStore(tmp_path / "settings", module.ConfigStore)
    )
    if bootstrap_factory is not None:
        bootstrap_service = bootstrap_factory(module, settings_service)
    elif bootstrap_service is None:
        initializer = SimpleNamespace(status=lambda: {
            "state": "ready", "ready": True, "missing": [],
        })
        bootstrap_service = module.BootstrapService(
            settings_service,
            initializer=initializer,
            probe_runner=lambda _profile_id: {"response": "ok", "latency_ms": 1},
        )
    server = module.make_server(
        0,
        round_dir=round_dir,
        gui_root=_gui_root(tmp_path),
        runtime_service=fake_cli,
        deposition_reader=deposition_reader,
        protocol_reader=protocol_reader,
        persona_reader=persona_reader,
        settings_service=settings_service,
        bootstrap_service=bootstrap_service,
        desktop_control_token=desktop_control_token,
        desktop_session_id=desktop_session_id,
    )
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    return server, thread


def _request(server, method, path, *, body=None, headers=None):
    host, port = server.server_address
    connection = http.client.HTTPConnection(host, port, timeout=5)
    request_headers = {"Host": f"127.0.0.1:{port}", **(headers or {})}
    connection.request(method, path, body=body, headers=request_headers)
    response = connection.getresponse()
    payload = response.read()
    connection.close()
    try:
        decoded = json.loads(payload)
    except json.JSONDecodeError:
        decoded = payload
    return response.status, decoded


def _json_headers(server):
    return {
        "Content-Type": "application/json",
        "Origin": f"http://127.0.0.1:{server.server_address[1]}",
    }


def _close(server, thread):
    server.shutdown()
    server.server_close()
    thread.join(timeout=5)


def test_spec683_static_whitelist_polling_and_runtime_status(tmp_path):
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    try:
        assert _request(server, "GET", "/")[0] == 200
        assert _request(server, "GET", "/styles.css")[0] == 200
        assert _request(server, "GET", "/markdown.css")[0] == 200
        assert _request(server, "GET", "/markdown-mermaid.js")[0] == 200
        assert _request(server, "GET", "/assets/markdown/KaTeX_Test.woff2")[0] == 200
        assert _request(server, "GET", "/assets/upsp-logo.png")[0] == 200
        assert _request(server, "GET", "/manual/intro.md")[0] == 200
        assert _request(server, "GET", "/manual/intro.en-US.md")[0] == 200
        assert _request(server, "GET", "/secret.txt")[0] == 404
        assert _request(server, "GET", "/src/app.ts")[0] == 404
        assert _request(server, "GET", "/src/bootstrap.ts")[0] == 404
        assert _request(server, "GET", "/src/markdown.ts")[0] == 404
        assert _request(server, "GET", "/src/i18n.ts")[0] == 404
        assert _request(server, "GET", "/assets/markdown/secret.txt")[0] == 404
        assert _request(server, "GET", "/package.json")[0] == 404
        assert _request(server, "GET", "/tsconfig.json")[0] == 404
        assert _request(server, "GET", "/manual/%2e%2e/secret.txt")[0] == 404
        assert _request(
            server,
            "GET",
            "/%2e%2e/initialization/persona_template/core.md",
        )[0] == 404
        assert _request(server, "GET", "/round_live.html")[0] == 404

        status, rounds = _request(server, "GET", "/api/rounds")
        assert status == 200
        assert rounds == {"rounds": []}
        status, live = _request(server, "GET", "/api/live/state")
        assert status == 200
        assert live["round"] is None
        assert live["state"]["schema_version"] == "round_live_state.v2"

        status, runtime = _request(server, "GET", "/api/runtime/status")
        assert status == 200
        assert runtime["schema_version"] == "seed_gui_runtime_status.v2"
        assert runtime["host"]["connected"] is True

        status, about = _request(server, "GET", "/api/about")
        assert status == 200
        assert about["schema_version"] == "seed_gui_about.v1"
        assert about["product"]["version"] == "0.1.0-alpha.5"
        assert about["product"]["author"]["zh-CN"] == (
            "由 TzPzFMZ 发起、设计并与 AI 协作开发"
        )
        assert about["links"]["repository"] == "https://github.com/TzPzFMZ/UPSP"
        assert about["build"]["signature_status"] == "unsigned"
        assert about["data_policy"]["persona_location"] == "Documents\\UPSP"
        serialized_about = json.dumps(about, ensure_ascii=False)
        assert "api_key" not in serialized_about
        assert "127.0.0.1" not in serialized_about
        assert str(tmp_path) not in serialized_about
        assert _request(server, "GET", "/api/about?debug=1")[0] == 400
        assert runtime["host_session"] == "fake-session"
        assert runtime["supervisor"]["state"] == "running"
        assert runtime["stage"] == "idle"
        assert runtime["can_stop"] is False
        assert runtime["send_in_flight"] is False
        assert runtime["cli"]["command"] == "status"
    finally:
        _close(server, thread)


def test_spec702_bootstrap_requires_explicit_probe_and_atomically_creates_alyosha(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("UPSP_TEST_KEY", "test-only-key")
    fake_cli = FakeCli()
    holder = {}
    persona_dir = tmp_path / "persona"

    def factory(module, settings_service):
        initializer = PersonaInitializer(
            persona_dir,
            REPO_ROOT / "UPSP" / "initialization" / "persona_template",
            REPO_ROOT / "UPSP" / "initialization" / "persona_presets",
        )
        calls = []

        def probe(profile_id):
            calls.append(profile_id)
            return {"response": "连接成功", "latency_ms": 12}

        service = module.BootstrapService(
            settings_service,
            initializer=initializer,
            probe_runner=probe,
        )
        holder.update(service=service, calls=calls)
        return service

    server, thread = _server(
        tmp_path,
        fake_cli,
        bootstrap_factory=factory,
    )
    try:
        status, bootstrap = _request(server, "GET", "/api/bootstrap/status")
        assert status == 200
        assert bootstrap["schema_version"] == "seed_gui_bootstrap_status.v1"
        assert bootstrap["persona"]["state"] == "missing"
        assert bootstrap["preset"]["name_zh"] == "阿廖沙"
        assert bootstrap["provider_test"]["valid"] is False
        assert holder["calls"] == []
        assert not persona_dir.exists()
        assert _request(server, "GET", "/api/deposition") == (
            409,
            {"error": "persona_initialization_required"},
        )
        assert _request(server, "GET", "/api/protocol/catalog") == (
            409,
            {"error": "persona_initialization_required"},
        )
        assert not persona_dir.exists()

        status, blocked = _request(
            server,
            "POST",
            "/api/runtime/send",
            body=json.dumps({
                "message": "不应发送",
                "permission_level": "limited",
                "unlimited_confirmed": False,
            }),
            headers=_json_headers(server),
        )
        assert status == 409
        assert blocked["error"] == "persona_initialization_required"
        assert fake_cli.calls == []

        status, tested = _request(
            server,
            "POST",
            "/api/bootstrap/provider-test",
            body="{}",
            headers=_json_headers(server),
        )
        assert status == 200
        assert tested["schema_version"] == "seed_gui_provider_test_receipt.v1"
        assert tested["status"] == "passed"
        assert holder["calls"] == ["model_test"]
        assert not persona_dir.exists()
        assert not list(tmp_path.rglob("round_*.jsonl"))

        status, created = _request(
            server,
            "POST",
            "/api/bootstrap/persona",
            body=json.dumps({
                "mode": "preset",
                "preset_id": "alyosha",
                "profile": None,
                "test_token": tested["test_token"],
                "skip_model_setup": False,
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        assert created["schema_version"] == "seed_gui_persona_init_receipt.v1"
        assert created["persona"]["name_zh"] == "阿廖沙"
        assert persona_dir.is_dir()
        assert _request(server, "GET", "/api/bootstrap/status")[1]["persona"]["ready"] is True
    finally:
        _close(server, thread)


def test_spec702_bootstrap_can_skip_model_setup_without_calling_provider(tmp_path):
    fake_cli = FakeCli()
    persona_dir = tmp_path / "persona"
    calls = []

    def factory(module, settings_service):
        return module.BootstrapService(
            settings_service,
            initializer=PersonaInitializer(
                persona_dir,
                REPO_ROOT / "UPSP" / "initialization" / "persona_template",
                REPO_ROOT / "UPSP" / "initialization" / "persona_presets",
            ),
            probe_runner=lambda profile_id: calls.append(profile_id),
        )

    server, thread = _server(
        tmp_path,
        fake_cli,
        bootstrap_factory=factory,
    )
    try:
        settings = _request(server, "GET", "/api/settings")[1]
        assert settings["persona"]["setup_model_ready"] is False

        status, created = _request(
            server,
            "POST",
            "/api/bootstrap/persona",
            body=json.dumps({
                "mode": "preset",
                "preset_id": "alyosha",
                "profile": None,
                "test_token": None,
                "skip_model_setup": True,
            }),
            headers=_json_headers(server),
        )

        assert status == 200
        assert created["model_setup"] == "skipped"
        assert created["persona"]["model_profile_id"] == "unbound"
        assert calls == []
        assert fake_cli.calls == []
        assert "未绑定" in (persona_dir / "core.md").read_text(encoding="utf-8")
        state = json.loads((persona_dir / "state.json").read_text(encoding="utf-8"))
        assert state["base"]["token_usage"]["window_size"] == 0
        assert _request(server, "GET", "/api/bootstrap/status")[1]["persona"]["ready"] is True
        blocked_status, blocked = _request(
            server,
            "POST",
            "/api/runtime/send",
            body=json.dumps({
                "message": "不应进入 Runtime",
                "permission_level": "limited",
                "unlimited_confirmed": False,
            }),
            headers=_json_headers(server),
        )
        assert blocked_status == 409
        assert blocked["error"] == "model_setup_required"
        assert fake_cli.calls == []
    finally:
        _close(server, thread)


def test_spec702_bootstrap_test_stamp_expires_and_invalidates_on_settings_change(
    tmp_path,
    monkeypatch,
):
    monkeypatch.setenv("UPSP_TEST_KEY", "test-only-key")
    fake_cli = FakeCli()
    clock = [100.0]
    holder = {}

    def factory(module, settings_service):
        service = module.BootstrapService(
            settings_service,
            initializer=PersonaInitializer(
                tmp_path / "persona",
                REPO_ROOT / "UPSP" / "initialization" / "persona_template",
                REPO_ROOT / "UPSP" / "initialization" / "persona_presets",
            ),
            probe_runner=lambda _profile_id: {"response": "ok", "latency_ms": 1},
            monotonic=lambda: clock[0],
        )
        holder["service"] = service
        return service

    server, thread = _server(tmp_path, fake_cli, bootstrap_factory=factory)
    try:
        status, tested = _request(
            server,
            "POST",
            "/api/bootstrap/provider-test",
            body="{}",
            headers=_json_headers(server),
        )
        assert status == 200
        clock[0] += holder["service"].TOKEN_TTL_SECONDS + 1
        status, expired = _request(
            server,
            "POST",
            "/api/bootstrap/persona",
            body=json.dumps({
                "mode": "preset",
                "preset_id": "alyosha",
                "profile": None,
                "test_token": tested["test_token"],
                "skip_model_setup": False,
            }),
            headers=_json_headers(server),
        )
        assert status == 409
        assert expired["error"] == "provider_test_required"

        clock[0] = 2000.0
        tested = _request(
            server,
            "POST",
            "/api/bootstrap/provider-test",
            body="{}",
            headers=_json_headers(server),
        )[1]
        settings = _request(server, "GET", "/api/settings")[1]
        status, _saved = _request(
            server,
            "POST",
            "/api/settings",
            body=json.dumps({
                "revision": settings["interface"]["revision"],
                "file": "interface",
                "changes": {"locale": "zh-CN"},
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        # Interface changes do not affect the model test binding.
        assert _request(server, "GET", "/api/bootstrap/status")[1]["provider_test"]["valid"] is True

        settings = _request(server, "GET", "/api/settings")[1]
        models = settings["model_catalog"]
        changed_transport = json.loads(json.dumps(models["transport"]))
        changed_transport["handshake"]["timeout_seconds"] += 1
        status, _saved = _request(
            server,
            "POST",
            "/api/settings",
            body=json.dumps({
                "revision": models["revision"],
                "file": "models",
                "changes": {"transport": changed_transport},
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        assert _request(server, "GET", "/api/bootstrap/status")[1]["provider_test"]["valid"] is False
        status, stale = _request(
            server,
            "POST",
            "/api/bootstrap/persona",
            body=json.dumps({
                "mode": "preset",
                "preset_id": "alyosha",
                "profile": None,
                "test_token": tested["test_token"],
                "skip_model_setup": False,
            }),
            headers=_json_headers(server),
        )
        assert status == 409
        assert stale["error"] == "provider_test_required"
    finally:
        _close(server, thread)


def test_spec702_failed_retest_invalidates_previous_success_stamp(tmp_path, monkeypatch):
    monkeypatch.setenv("UPSP_TEST_KEY", "test-only-key")
    fake_cli = FakeCli()
    calls = []

    def factory(module, settings_service):
        def probe(profile_id):
            calls.append(profile_id)
            if len(calls) == 1:
                return {"response": "ok", "latency_ms": 1}
            raise module.APIBridgeError(profile_id, "HTTP 503")

        return module.BootstrapService(
            settings_service,
            initializer=PersonaInitializer(
                tmp_path / "persona",
                REPO_ROOT / "UPSP" / "initialization" / "persona_template",
                REPO_ROOT / "UPSP" / "initialization" / "persona_presets",
            ),
            probe_runner=probe,
        )

    server, thread = _server(tmp_path, fake_cli, bootstrap_factory=factory)
    try:
        first_status, first = _request(
            server,
            "POST",
            "/api/bootstrap/provider-test",
            body="{}",
            headers=_json_headers(server),
        )
        assert first_status == 200
        assert first["status"] == "passed"
        assert _request(server, "GET", "/api/bootstrap/status")[1]["provider_test"]["valid"] is True

        second_status, second = _request(
            server,
            "POST",
            "/api/bootstrap/provider-test",
            body="{}",
            headers=_json_headers(server),
        )
        assert second_status == 502
        assert second["error"] == "provider_test_failed"
        assert _request(server, "GET", "/api/bootstrap/status")[1]["provider_test"]["valid"] is False
    finally:
        _close(server, thread)


def _nested_keys(value):
    if isinstance(value, dict):
        return set(value).union(*( _nested_keys(item) for item in value.values()))
    if isinstance(value, list):
        return set().union(*( _nested_keys(item) for item in value))
    return set()


def test_spec700_settings_v3_projects_global_and_persona_truth(tmp_path):
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    service = server.RequestHandlerClass.settings_service
    system_path = service.configs.paths["system"]
    try:
        status, initial = _request(server, "GET", "/api/settings")
        assert status == 200
        assert initial["schema_version"] == "seed_gui_settings.v3"
        assert set(initial["files"]) == {
            "system", "now", "lately", "periodic", "high_freq", "relation", "memory",
        }
        assert "api_key" not in _nested_keys(initial)
        assert initial["interface"]["values"]["locale"] == "system"
        assert initial["model_catalog"]["connections"][0]["id"] == "conn_test"
        assert initial["model_catalog"]["connections"][0]["key_source"] == "missing"
        assert initial["persona"]["effective_routes"]["phases"]["reaction"][0]["model_id"] == "model_test"
        assert "paths" not in initial
        assert str(tmp_path) not in json.dumps(initial, ensure_ascii=False)
        assert _request(server, "GET", "/api/settings?path=api.json")[0] == 400

        status, saved = _request(
            server,
            "POST",
            "/api/settings",
            body=json.dumps({
                "revision": initial["files"]["system"]["revision"],
                "file": "system",
                "changes": {
                    "heartbeat.interval": 11,
                    "rhythm.period": 48,
                },
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        assert saved["files"]["system"]["values"]["heartbeat.interval"] == 11
        assert saved["files"]["system"]["values"]["rhythm.period"] == 48
        on_disk = json.loads(system_path.read_text(encoding="utf-8"))
        assert on_disk["heartbeat"]["interval"] == 11
        assert on_disk["rhythm"]["period"] == 48
        assert "instance" not in on_disk
    finally:
        _close(server, thread)


def test_spec700_provider_key_is_connection_scoped_and_never_echoed(tmp_path, monkeypatch):
    monkeypatch.delenv("UPSP_API_CONFIG_OVERRIDE_JSON", raising=False)
    monkeypatch.delenv("UPSP_TIANSHU_API_KEY", raising=False)
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    service = server.RequestHandlerClass.settings_service
    sentinel = "sentinel-provider-key"
    try:
        status, initial = _request(server, "GET", "/api/settings")
        assert status == 200
        assert initial["model_catalog"]["key_sources"]["conn_test"] == "missing"
        assert initial["persona"]["setup_model_ready"] is False

        status, saved = _request(
            server,
            "POST",
            "/api/settings/provider-key",
            body=json.dumps({
                "connection_id": "conn_test",
                "action": "set",
                "key": sentinel,
                "revision": initial["model_catalog"]["revision"],
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        assert saved["model_catalog"]["key_sources"]["conn_test"] == "config"
        assert saved["persona"]["setup_model_ready"] is True
        assert sentinel not in json.dumps(saved, ensure_ascii=False)
        on_disk = service.configs.load("models")
        assert on_disk["connections"][0]["api_key"] == sentinel
        assert _request(
            server,
            "POST",
            "/api/settings/provider-key",
            body=json.dumps({
                "connection_id": "conn_test", "action": "delete", "key": "not-empty",
                "revision": saved["model_catalog"]["revision"],
            }),
            headers=_json_headers(server),
        )[0] == 400
        assert _request(
            server,
            "POST",
            "/api/settings/provider-key",
            body=json.dumps({
                "connection_id": "unknown", "action": "set", "key": sentinel,
                "revision": saved["model_catalog"]["revision"],
            }),
            headers=_json_headers(server),
        )[0] == 404
        assert _request(
            server,
            "POST",
            "/api/settings/provider-key",
            body=json.dumps({
                "connection_id": "conn_test", "action": "set", "key": sentinel,
                "revision": saved["model_catalog"]["revision"],
            }),
            headers={"Content-Type": "application/json"},
        )[0] == 403

        status, deleted = _request(
            server,
            "POST",
            "/api/settings/provider-key",
            body=json.dumps({
                "connection_id": "conn_test", "action": "delete", "key": "",
                "revision": saved["model_catalog"]["revision"],
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        assert deleted["model_catalog"]["key_sources"]["conn_test"] == "missing"
        assert deleted["persona"]["setup_model_ready"] is False
    finally:
        _close(server, thread)


def test_spec700_model_catalog_crud_auto_binding_and_reference_guards(tmp_path):
    server, thread = _server(tmp_path, FakeCli())
    service = server.RequestHandlerClass.settings_service
    empty_models = service.configs.load("models")
    empty_models["connections"] = []
    empty_models["models"] = []
    empty_routes = service.configs.load("model_routing")
    for row in empty_routes["routes"].values():
        row["primary"] = None
        row["backups"] = [None, None]
    service.configs.save("models", empty_models)
    service.configs.save("model_routing", empty_routes)
    try:
        _, initial = _request(server, "GET", "/api/settings")
        status, with_connection = _request(
            server, "POST", "/api/settings/model-catalog",
            body=json.dumps({
                "revision": initial["model_catalog"]["revision"],
                "entity": "connection", "action": "create", "id": None,
                "values": {
                    "alias": "天枢测试", "protocol": "openai_chat",
                    "url": "https://example.invalid/v1/chat/completions",
                    "api_key_env": "UPSP_TEST_KEY",
                },
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        connection_id = with_connection["model_catalog"]["connections"][0]["id"]
        status, with_model = _request(
            server, "POST", "/api/settings/model-catalog",
            body=json.dumps({
                "revision": with_connection["model_catalog"]["revision"],
                "entity": "model", "action": "create", "id": None,
                "values": {
                    "alias": "Terra 测试", "connection_id": connection_id,
                    "model": "gpt-5.6-terra", "context_window": 1000000,
                    "reasoning_supported": ["medium"], "reasoning_default": "medium",
                    "streaming_enabled": True, "streaming_include_usage": True,
                    "prompt_cache_profile": "off", "request_overrides": {},
                },
            }),
            headers=_json_headers(server),
        )
        assert status == 200
        model_id = with_model["model_catalog"]["models"][0]["id"]
        assert with_model["persona"]["model_routing"]["values"]["routes"]["setup"]["primary"]["model_id"] == model_id
        assert with_model["persona"]["model_routing"]["values"]["routes"]["reaction"]["primary"] is None
        assert _request(
            server, "POST", "/api/settings/model-catalog",
            body=json.dumps({
                "revision": with_model["model_catalog"]["revision"],
                "entity": "connection", "action": "delete", "id": connection_id,
                "values": {},
            }), headers=_json_headers(server),
        )[0] == 409
        assert _request(
            server, "POST", "/api/settings/model-catalog",
            body=json.dumps({
                "revision": with_model["model_catalog"]["revision"],
                "entity": "model", "action": "delete", "id": model_id,
                "values": {},
            }), headers=_json_headers(server),
        )[0] == 409
    finally:
        _close(server, thread)


def test_spec700_settings_validation_revision_and_lock_fail_closed(tmp_path):
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    try:
        _, initial = _request(server, "GET", "/api/settings")
        status, invalid = _request(
            server,
            "POST",
            "/api/settings",
            body=json.dumps({
                "revision": initial["files"]["memory"]["revision"],
                "file": "memory",
                "changes": {
                    "heat.zone_thresholds.significant": 30,
                    "heat.zone_thresholds.uncertain": 40,
                },
            }),
            headers=_json_headers(server),
        )
        assert status == 400
        assert invalid["error"] == "settings_memory_heat_invalid"

        status, secret = _request(
            server,
            "POST",
            "/api/settings",
            body=json.dumps({
                "revision": initial["model_catalog"]["revision"],
                "file": "models",
                "changes": {"api_key": "must-not-be-accepted"},
            }),
            headers=_json_headers(server),
        )
        assert status == 400
        assert secret["error"] == "settings_fields_invalid"

        first_status, first = _request(
            server,
            "POST",
            "/api/settings",
            body=json.dumps({
                "revision": initial["files"]["system"]["revision"],
                "file": "system",
                "changes": {"heartbeat.interval": 6},
            }),
            headers=_json_headers(server),
        )
        assert first_status == 200
        stale_status, stale = _request(
            server,
            "POST",
            "/api/settings",
            body=json.dumps({
                "revision": initial["files"]["system"]["revision"],
                "file": "system",
                "changes": {"heartbeat.interval": 7},
            }),
            headers=_json_headers(server),
        )
        assert stale_status == 409
        assert stale["error"] == "settings_revision_conflict"

        lock = server.RequestHandlerClass.mutation_lock
        assert lock.acquire(blocking=False)
        try:
            locked_status, locked = _request(
                server,
                "POST",
                "/api/settings",
                body=json.dumps({
                    "revision": first["files"]["system"]["revision"],
                    "file": "system",
                    "changes": {"heartbeat.interval": 7},
                }),
                headers=_json_headers(server),
            )
        finally:
            lock.release()
        assert locked_status == 409
        assert locked["error"] == "mutation_in_flight"
    finally:
        _close(server, thread)


def test_spec699_environment_override_remains_runtime_precedence(tmp_path, monkeypatch):
    monkeypatch.setenv("UPSP_API_CONFIG_OVERRIDE_JSON", "{}")
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    try:
        server.RequestHandlerClass.settings_service.configs.use_api_environment = True
        _, initial = _request(server, "GET", "/api/settings")
        assert initial["environment_override"] is True
        assert initial["persona"]["setup_model_ready"] is False

        monkeypatch.setenv("UPSP_OVERRIDE_KEY", "test-only-key")
        monkeypatch.setenv("UPSP_API_CONFIG_OVERRIDE_JSON", json.dumps({
            "endpoints": {
                "primary": {
                    "url": "https://example.invalid/v1/chat/completions",
                    "model": "override-model",
                    "api_key_env": "UPSP_OVERRIDE_KEY",
                },
            },
            "step_tiers": {"setup": "primary"},
        }))
        _, ready = _request(server, "GET", "/api/settings")
        assert ready["persona"]["setup_model_ready"] is True

        assert _request(server, "GET", "/api/runtime/status")[0] == 200
        assert fake_cli.calls == []
    finally:
        _close(server, thread)


def test_spec698_protocol_catalog_and_documents_are_registry_whitelisted(tmp_path):
    module = _load_module_from_path("serve_seed_gui_protocol", SERVER_PATH)
    reader = module.ProtocolCatalogReader(
        REPO_ROOT / "UPSP" / "initialization" / "persona_template" / "rules",
        REPO_ROOT / "UPSP" / "initialization" / "persona_template" / "docs",
    )
    catalog = reader.catalog()
    assert catalog["schema_version"] == "seed_gui_protocol_catalog.v1"
    assert catalog["rules"]["total"] == 20
    assert [item["count"] for item in catalog["rules"]["categories"]] == [8, 8, 0, 0, 4]
    assert catalog["docs"]["registrations"] == 28
    assert catalog["docs"]["total"] == 24
    docs = {item["id"]: item for item in catalog["docs"]["entries"]}
    assert docs["protocol/base/tools.md"]["categories"] == ["lookup", "popup.guide"]
    assert docs["protocol/base/schema.md"]["categories"] == ["lookup", "popup.guide"]
    assert docs["protocol/base/popup.md"]["categories"] == [
        "popup.guide", "popup.reminder", "popup.warning",
    ]

    server, thread = _server(tmp_path, FakeCli(), protocol_reader=reader)
    try:
        status, projected = _request(server, "GET", "/api/protocol/catalog")
        assert status == 200
        assert projected == catalog

        rule_id = catalog["rules"]["categories"][0]["entries"][0]["id"]
        status, rule = _request(
            server,
            "GET",
            f"/api/protocol/document?kind=rule&id={rule_id.replace(':', '%3A').replace('/', '%2F')}",
        )
        assert status == 200
        assert rule["schema_version"] == "seed_gui_protocol_document.v1"
        assert rule["kind"] == "rule"
        assert rule["content_md"].strip()
        assert not Path(rule["source_ref"]).is_absolute()

        doc_id = "protocol/base/schema.md"
        status, doc = _request(
            server,
            "GET",
            f"/api/protocol/document?kind=doc&id={doc_id.replace('/', '%2F')}",
        )
        assert status == 200
        assert doc["id"] == doc_id
        assert doc["content_md"].strip()
        assert _request(server, "GET", "/api/protocol/catalog?path=rules_registry.json")[0] == 400
        assert _request(server, "GET", "/api/protocol/document?kind=other&id=x")[0] == 400
        assert _request(server, "GET", "/api/protocol/document?kind=doc&id=missing.md")[0] == 404
        assert _request(server, "GET", "/api/protocol/document?kind=doc&id=..%2Fsecret.md")[0] == 404
        assert _request(server, "GET", "/api/protocol/document?kind=doc&id=x&path=y")[0] == 400
    finally:
        _close(server, thread)


def test_spec698_protocol_registry_and_document_fail_closed(tmp_path):
    module = _load_module_from_path("serve_seed_gui_protocol_failures", SERVER_PATH)

    def roots(name):
        base = tmp_path / name
        rules = base / "rules"
        docs = base / "docs"
        rules.mkdir(parents=True)
        docs.mkdir(parents=True)
        return rules, docs

    def minimal_docs(path="protocol/base/doc.md"):
        return {
            "_version": "historical",
            "inject": [{"file": "doc.md", "path": path, "description": "Doc"}],
            "lookup": [],
            "popup": {"guide": [], "reminder": [], "warning": []},
            "persona": [],
        }

    missing_rules, missing_docs = roots("missing")
    (missing_rules / "rules_registry.json").write_text(json.dumps({
        "_version": "historical",
        "permanent": [], "passive_read": [], "step_level": [], "periodic": [], "on_demand": [],
    }), encoding="utf-8")
    (missing_docs / "docs_registry.json").write_text(json.dumps(minimal_docs()), encoding="utf-8")
    missing_reader = module.ProtocolCatalogReader(missing_rules, missing_docs)
    server, thread = _server(tmp_path / "missing-server", FakeCli(), protocol_reader=missing_reader)
    try:
        assert _request(server, "GET", "/api/protocol/catalog")[0] == 200
        assert _request(
            server,
            "GET",
            "/api/protocol/document?kind=doc&id=protocol%2Fbase%2Fdoc.md",
        )[0] == 404
    finally:
        _close(server, thread)


    large_rules, large_docs = roots("large")
    (large_rules / "rules_registry.json").write_text(json.dumps({
        "_version": "historical",
        "permanent": [], "passive_read": [], "step_level": [], "periodic": [], "on_demand": [],
    }), encoding="utf-8")
    (large_docs / "docs_registry.json").write_text(json.dumps(minimal_docs()), encoding="utf-8")
    large_path = large_docs / "protocol" / "base" / "doc.md"
    large_path.parent.mkdir(parents=True)
    large_path.write_bytes(b"x" * (module.MAX_PROTOCOL_DOCUMENT_BYTES + 1))
    server, thread = _server(
        tmp_path / "large-server",
        FakeCli(),
        protocol_reader=module.ProtocolCatalogReader(large_rules, large_docs),
    )
    try:
        assert _request(
            server,
            "GET",
            "/api/protocol/document?kind=doc&id=protocol%2Fbase%2Fdoc.md",
        )[0] == 503
    finally:
        _close(server, thread)

    bad_rules, bad_docs = roots("bad")
    (bad_rules / "rules_registry.json").write_text("{", encoding="utf-8")
    (bad_docs / "docs_registry.json").write_text(json.dumps(minimal_docs()), encoding="utf-8")
    server, thread = _server(
        tmp_path / "bad-server",
        FakeCli(),
        protocol_reader=module.ProtocolCatalogReader(bad_rules, bad_docs),
    )
    try:
        assert _request(server, "GET", "/api/protocol/catalog")[0] == 503
    finally:
        _close(server, thread)


def test_spec701_persona_projection_is_complete_read_only_and_query_free(tmp_path):
    module = _load_module_from_path("serve_seed_gui_persona_projection", SERVER_PATH)
    from schemas.state import FIELDS, default_state

    persona_root = tmp_path / "persona"
    persona_root.mkdir()
    core_path = persona_root / "core.md"
    state_path = persona_root / "state.json"
    core_path.write_text("# 当前位格\n\n真实核心档案。\n", encoding="utf-8")
    snapshot = default_state()
    snapshot["base"]["meta"]["last_error"] = None
    snapshot["base"]["not_registered"] = "must-not-leak"
    snapshot["base"]["meta"]["total_round"] = 701
    state_path.write_text(json.dumps(snapshot, ensure_ascii=False), encoding="utf-8")
    core_before = core_path.read_bytes()
    state_before = state_path.read_bytes()
    reader = module.PersonaProjectionReader(
        core_path,
        module.StateStore(str(state_path)),
    )
    server, thread = _server(tmp_path / "server", FakeCli(), persona_reader=reader)
    try:
        status, core = _request(server, "GET", "/api/persona/core")
        assert status == 200
        assert core == {
            "schema_version": "seed_gui_persona_core.v1",
            "source_ref": "persona/core.md",
            "content_md": "# 当前位格\n\n真实核心档案。\n",
        }
        status, state = _request(server, "GET", "/api/persona/state")
        assert status == 200
        assert state["schema_version"] == "seed_gui_persona_state.v1"
        assert state["source_ref"] == "persona/state.json"
        assert isinstance(state["observed_at"], str) and state["observed_at"]
        assert set(state["dynamic_descriptions"]) == {
            "valence", "arousal", "focus", "mood", "humor", "safety",
        }
        assert all(state["dynamic_descriptions"].values())
        assert [item["path"] for item in state["fields"]] == list(FIELDS)
        values = {item["path"]: item["value"] for item in state["fields"]}
        assert values["base.meta.total_round"] == 701
        assert "base.not_registered" not in values
        assert _request(server, "GET", "/api/persona/core?path=other.md")[0] == 400
        assert _request(server, "GET", "/api/persona/state?field=base.meta.total_round")[0] == 400
        assert core_path.read_bytes() == core_before
        assert state_path.read_bytes() == state_before
    finally:
        _close(server, thread)


def test_spec701_persona_projection_fails_closed_on_incomplete_sources(tmp_path):
    module = _load_module_from_path("serve_seed_gui_persona_failures", SERVER_PATH)
    from schemas.state import default_state

    persona_root = tmp_path / "persona"
    persona_root.mkdir()
    core_path = persona_root / "core.md"
    state_path = persona_root / "state.json"
    core_path.write_text("# Core", encoding="utf-8")
    incomplete = default_state()
    incomplete["base"]["runtime"].pop("phase")
    state_path.write_text(json.dumps(incomplete), encoding="utf-8")
    reader = module.PersonaProjectionReader(core_path, module.StateStore(str(state_path)))
    server, thread = _server(tmp_path / "server", FakeCli(), persona_reader=reader)
    try:
        assert _request(server, "GET", "/api/persona/core")[0] == 200
        assert _request(server, "GET", "/api/persona/state")[0] == 503
        core_path.write_text("", encoding="utf-8")
        assert _request(server, "GET", "/api/persona/core")[0] == 503
        state_path.write_text("{", encoding="utf-8")
        assert _request(server, "GET", "/api/persona/state")[0] == 503
    finally:
        _close(server, thread)


def test_spec683_source_and_send_validation_fail_closed(tmp_path):
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    try:
        assert _request(server, "GET", "/", headers={"Host": "localhost:8770"})[0] == 403
        payload = json.dumps({
            "message": "hello",
            "permission_level": "limited",
            "unlimited_confirmed": False,
        })
        assert _request(
            server,
            "POST",
            "/api/runtime/send",
            body=payload,
            headers={"Content-Type": "application/json"},
        )[0] == 403
        headers = _json_headers(server)
        assert _request(server, "POST", "/api/runtime/send", body="{}", headers=headers)[0] == 400
        invalid_permission = json.dumps({
            "message": "hello",
            "permission_level": "admin",
            "unlimited_confirmed": False,
        })
        assert _request(
            server,
            "POST",
            "/api/runtime/send",
            body=invalid_permission,
            headers=headers,
        )[0] == 400
        unlimited = json.dumps({
            "message": "hello",
            "permission_level": "unlimited",
            "unlimited_confirmed": False,
        })
        assert _request(server, "POST", "/api/runtime/send", body=unlimited, headers=headers)[0] == 403
        oversized = b"{" + b" " * (1024 * 1024)
        assert _request(
            server,
            "POST",
            "/api/runtime/send",
            body=oversized,
            headers=headers,
        )[0] == 400
        assert fake_cli.calls == []
    finally:
        _close(server, thread)


def test_spec704_send_uses_resident_runtime_without_temp_file(tmp_path, monkeypatch):
    monkeypatch.setenv("UPSP_TEST_KEY", "test-only-key")
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    try:
        payload = json.dumps({
            "message": "你好，GUI-1",
            "permission_level": "limited",
            "unlimited_confirmed": False,
        })
        status, response = _request(
            server,
            "POST",
            "/api/runtime/send",
            body=payload.encode("utf-8"),
            headers=_json_headers(server),
        )

        assert status == 200
        assert response["ok"] is True
        assert len(fake_cli.calls) == 1
        command = fake_cli.calls[0][0]
        assert command == ["resident", "send", "limited"]
        assert fake_cli.message_bytes == "你好，GUI-1".encode("utf-8")
        assert fake_cli.message_path is None
    finally:
        _close(server, thread)


def test_spec683_single_send_lock_keeps_get_available(tmp_path, monkeypatch):
    monkeypatch.setenv("UPSP_TEST_KEY", "test-only-key")
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    lock = server.RequestHandlerClass.send_lock
    lock.acquire()
    try:
        payload = json.dumps({
            "message": "hello",
            "permission_level": "limited",
            "unlimited_confirmed": False,
        })
        assert _request(
            server,
            "POST",
            "/api/runtime/send",
            body=payload,
            headers=_json_headers(server),
        )[0] == 409
        status, runtime = _request(server, "GET", "/api/runtime/status")
        assert status == 200
        assert runtime["send_in_flight"] is True
    finally:
        lock.release()
        _close(server, thread)


def test_spec704_resident_host_failures_do_not_retry(tmp_path, monkeypatch):
    monkeypatch.setenv("UPSP_TEST_KEY", "test-only-key")
    for index, (fake_cli, expected) in enumerate((
        (FakeCli(send_returncode=2), 503),
        (FakeCli(fail_with=OSError("host unavailable")), 503),
    )):
        case_root = tmp_path / str(index)
        case_root.mkdir()
        server, thread = _server(case_root, fake_cli)
        try:
            payload = json.dumps({
                "message": "once",
                "permission_level": "limited",
                "unlimited_confirmed": False,
            })
            assert _request(
                server,
                "POST",
                "/api/runtime/send",
                body=payload,
                headers=_json_headers(server),
            )[0] == expected
            assert len(fake_cli.calls) == 1
            assert fake_cli.message_path is None
        finally:
            _close(server, thread)


def test_spec684_context_layer_controls_open_real_detail_without_clipping():
    app_source = _gui_ts_source("view.ts", "events.ts")
    styles = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    markup_start = app_source.index('<div class="layer-ledger">')
    layer_markup = app_source[markup_start:app_source.index("</div>`;", markup_start)]
    handler_start = app_source.index("const runtimePaneButton")
    pane_handler = app_source[handler_start:app_source.index("const depositionButton", handler_start)]
    rule_start = styles.index(".layer-ledger-row small")
    preview_rule = styles[rule_start:styles.index("}", rule_start)]

    assert 'role="listitem"' not in layer_markup
    assert 'aria-label="${escapeHtml(t("查看 {title} 层真实内容", { title: contextPaneLabel(pane.id) }))}"' in layer_markup
    assert 'setActivePageTab("context", "content");' in pane_handler
    assert all(rule in preview_rule for rule in ("overflow: hidden", "text-overflow: ellipsis", "white-space: nowrap"))


class FakeMemoryStore:
    def list_public_entries(self):
        return [
            {
                "id": "MEM-PUBLIC01",
                "title": "公开条目",
                "access": "public",
                "memory_layer": "STM",
                "linked_containers": ["PRJ-001"],
                "path": "must-not-leak",
            },
            {
                "id": "MEM-PRIVATE1",
                "title": "私密条目",
                "access": "private",
                "memory_layer": "STM",
            },
        ]

    def read_body_by_id(self, mem_id):
        assert mem_id == "MEM-PUBLIC01"
        return {
            "meta": {"access": "public"},
            "body": "公开正文",
            "total_lines": 1,
            "total_chars": 4,
            "path": "must-not-leak",
        }


class FakeContainerStore:
    def __init__(self):
        self.focus_calls = []

    def list_containers(self):
        return [
            {
                "id": "PRJ-001",
                "prefix": "PRJ",
                "title": "项目",
                "status": "active",
                "path": "must-not-leak",
                "entries": [{"mem_id": "MEM-PUBLIC01", "target_file": "plan.md"}],
            },
            {
                "id": "DC-002",
                "prefix": "DC",
                "title": "辩证链",
                "status": "active",
                "entries": [],
            },
        ]

    def container_exists(self, container_id):
        return container_id in {"PRJ-001", "DC-002"}

    def resolve_container_type(self, container_id):
        return container_id.split("-", 1)[0]

    def set_container_focus(self, container_id, focus):
        if not self.container_exists(container_id):
            from errors import ContainerNotFoundError
            raise ContainerNotFoundError(container_id)
        self.focus_calls.append((container_id, focus))

    def read_focus_projection(self, container_id):
        assert container_id == "PRJ-001"
        return {
            "content": "项目正文",
            "allowed_targets": ["notes.md", "plan.md"],
            "default_target": "plan.md",
            "total_lines": 1,
            "total_chars": 4,
            "path": "must-not-leak",
        }


class FakeRelationStore:
    def list_cards(self, status="active"):
        assert status == "active"
        return [{
            "id": "REL-USER",
            "name": "当前对象",
            "category": "ours",
            "status": "active",
            "path": "must-not-leak",
        }]

    def read_card(self, card_id, category):
        assert (card_id, category) == ("REL-USER", "ours")
        return {
            "id": card_id,
            "name": "当前对象",
            "category": category,
            "axes": {"trust": 80},
            "notes": [{"date": "today", "content": "真实笔记", "path": "must-not-leak"}],
            "history": [],
            "status": "active",
            "tags": ["anchor"],
        }


class FakeWorkbenchStore:
    def __init__(self, current="", previous="", task_guide=None):
        self.current = current or None
        self.previous = previous or None
        self.task_guide = task_guide

    def get(self, dotpath, default=None):
        return {
            "base.focus": self.current,
            "base.old_focus": self.previous,
            "base.active_task": "T-001" if self.task_guide else None,
        }.get(dotpath, default)

    def active_guide_slots(self):
        return {
            "rhythm": None,
            "work": "task:T-001" if self.task_guide else None,
        }

    def load_task_guide(self, task_id):
        assert task_id == "T-001"
        return self.task_guide

    def set(self, dotpath, value):
        if dotpath == "base.focus":
            self.current = value
        elif dotpath == "base.old_focus":
            self.previous = value

    def mount_focus(self, container_id):
        if self.current and self.current != container_id:
            self.previous = self.current
        self.current = container_id

    def unmount_focus(self, container_id=None):
        if self.current and (container_id is None or container_id == self.current):
            self.previous = self.current
            self.current = None

    def restore_focus(self):
        restored = self.previous
        if restored:
            self.current = restored
            self.previous = None
        return restored


def _deposition_reader(workbench_store=None, container_store=None):
    module = _load_module_from_path("serve_seed_gui_deposition", SERVER_PATH)
    return module.DepositionReader(
        memory_store=FakeMemoryStore(),
        container_store=container_store or FakeContainerStore(),
        relation_store=FakeRelationStore(),
        workbench_store=workbench_store or FakeWorkbenchStore(),
    )


def test_spec685_deposition_projection_filters_private_and_paths():
    reader = _deposition_reader()
    index = reader.index()

    assert index["schema_version"] == "seed_gui_deposition_index.v1"
    assert index["focus"] == {"current": "", "previous": ""}
    assert [item["id"] for item in index["memory"]] == ["MEM-PUBLIC01"]
    assert "PRIVATE" not in json.dumps(index)
    assert "path" not in json.dumps(index)
    assert reader.detail("memory", "MEM-PUBLIC01")["item"]["body"] == "公开正文"
    assert reader.detail("container", "PRJ-001")["item"]["content"] == "项目正文"
    relation = reader.detail("relation", "REL-USER")["item"]
    assert relation["axes"] == {
        "trust": 80,
        "safety": 0,
        "value": 0,
        "investment": 0,
        "honesty": 0,
        "resonance": 0,
    }
    assert "path" not in json.dumps(relation)

    try:
        reader.detail("memory", "MEM-PRIVATE1")
    except KeyError:
        pass
    else:
        raise AssertionError("private memory must fail closed")


def test_spec685_gui_labels_store_and_static_sources_truthfully():
    app_source = _gui_ts_source()
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")

    assert "Seed GUI 本地版" not in index_source
    assert "GUI-1 LOCAL" not in index_source
    assert 't("静态设计页｜尚未接入运行时")' in app_source
    assert "depositionProjection.index" in app_source


def test_spec709_top_identity_hierarchy_contract():
    app_source = _gui_ts_source()
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    css_source = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    i18n_source = (GUI_ROOT / "src" / "i18n.ts").read_text(encoding="utf-8")

    assert 'class="upsp-logo" src="./assets/upsp-logo.png"' in index_source
    assert 'class="global-row"' in index_source
    assert 'class="persona-row"' in index_source
    assert 'class="command-row"' in index_source
    assert 'id="personaNameSelector"' in index_source
    assert 'data-i18n="主实例">主实例</strong>' in index_source
    assert 'id="statusReadouts"' in index_source
    assert "prototype-badge" not in index_source
    assert "persona-metrics" not in index_source
    assert "personaTabs" not in index_source
    assert "alertStrip" not in index_source
    assert "--top-global-height: 34px;" in css_source
    assert "--top-persona-height: 32px;" in css_source
    assert "--top-command-height: 70px;" in css_source
    assert "--top-height: 136px;" in css_source
    assert 'const personaNameStoragePrefix = "upsp.seed_gui.persona_name_variant.v1:"' in app_source
    assert "`${personaNameStoragePrefix}${identity.pid}`" in app_source
    assert '["name_zh", "name_en", "abbreviation"]' in app_source
    identity_renderer = app_source.split("export function renderIdentity()", 1)[1].split(
        "export function renderNavigation()", 1
    )[0]
    assert '"Base / 串行"' not in identity_renderer
    assert '"Base / Serial"' not in identity_renderer
    assert '{ label: t("轮型"), value: runtimeTerm(roundType || t("未投影")) }' in identity_renderer
    assert all(f'{kind}: "{label}"' in i18n_source for kind, label in (
        ("interactive", "交互轮"),
        ("rhythm", "节律轮"),
        ("relay", "中继轮"),
        ("autonomous", "自主轮"),
        ("standby", "待命轮"),
    ))
    assert 'fetchRuntimeJson<AboutPayload>("./api/about")' in app_source


def test_spec709_runtime_console_uses_product_labels_not_backend_fields():
    app_source = _gui_ts_source()
    state_source = (GUI_ROOT / "src" / "state.ts").read_text(encoding="utf-8")
    i18n_source = (GUI_ROOT / "src" / "i18n.ts").read_text(encoding="utf-8")

    assert 'title: "运行台"' in state_source
    assert "Base 串行运行台" not in state_source
    assert 'label: "回执与结算"' in state_source
    assert "队列回执" not in state_source
    run_renderer = app_source.split("function renderRuntimeRunPage()", 1)[1].split(
        "function contextPaneMarkdown(", 1
    )[0]
    assert 'runtimeTerm(eventType)' in run_renderer
    assert 'runtimeTerm(statusbar.round?.type || t("轮型未投影"))' in run_renderer
    assert 'runtimeTerm(statusbar.mode || t("模式未投影"))' not in run_renderer
    assert 'runtimeTerm(frame.call_channel || frame.phase || "unknown")' in app_source
    assert "live.schema_version" not in run_renderer
    assert "statusbar.schema" not in run_renderer
    assert all(f'{event_type}: "{label}"' in i18n_source for event_type, label in (
        ("round_started", "轮次开始"),
        ("round_close_requested", "请求闭合"),
        ("cleanup_obligation_created", "建立善后义务"),
        ("cleanup_obligation_settled", "善后义务已结算"),
        ("cleanup_obligation_failed", "善后义务失败"),
        ("round_settled", "轮次已结算"),
        ("round_closed", "轮次已闭合"),
        ("round_unsettled", "轮次未结算"),
    ))


def test_spec710_chat_consumes_visible_streams_and_polls_fast_only_when_active():
    app_source = (GUI_ROOT / "src" / "app.ts").read_text(encoding="utf-8")
    runtime_source = (GUI_ROOT / "src" / "runtime.ts").read_text(encoding="utf-8")
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    styles = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")

    assert '["reaction", "final_reply"].includes(String(card.phase || ""))' in view_source
    assert 'card.type === "assistant-streaming" ? card.content_raw || ""' in view_source
    assert "chatMessageAnchor(item.round, card, position)" in view_source
    assert "stream-state-${escapeHtml(streamState)}" in view_source
    assert 't("输出中断")' in view_source
    assert 't("已停止")' in view_source
    assert "reasoning_content" not in view_source
    assert "content_raw" in view_source
    assert "export function runtimePollingActive()" in runtime_source
    assert "if (!runtimePollingActive()) void pollRuntime();" in app_source
    assert "if (runtimePollingActive()) void pollRuntime();" in app_source
    assert "}, 500);" in app_source
    assert "}, 1500);" in app_source
    assert ".stream-state-active .markdown-body::after" in styles


def test_spec685_memory_public_index_spans_active_layers_and_fails_private_closed(tmp_path, monkeypatch):
    module = _load_module_from_path("memory_store_spec685", MEMORY_STORE_PATH)
    paths = {
        "MEMORY_MD": tmp_path / "stm.md",
        "META_JSON": tmp_path / "stm-meta.json",
        "LTM_FULL_FULL_MD": tmp_path / "full.md",
        "LTM_FULL_META_JSON": tmp_path / "full-meta.json",
        "LTM_SUMMARY_SUMMARY_MD": tmp_path / "summary.md",
        "LTM_SUMMARY_META_JSON": tmp_path / "summary-meta.json",
        "LTM_ABSTRACT_ABSTRACT_MD": tmp_path / "abstract.md",
        "LTM_ABSTRACT_META_JSON": tmp_path / "abstract-meta.json",
        "LTM_PINNED_PINNED_MD": tmp_path / "pinned.md",
        "LTM_PINNED_META_JSON": tmp_path / "pinned-meta.json",
    }
    for name, path in paths.items():
        monkeypatch.setattr(module, name, str(path))

    paths["MEMORY_MD"].write_text(
        "## MEM-AAAABBBB\n公开\n\n## MEM-CCCCDDDD\n不得回退成公开\n",
        encoding="utf-8",
    )
    paths["META_JSON"].write_text(json.dumps({
        "MEM-AAAABBBB": {"title": "STM public", "access": "public", "created_at": "2026-01-01"},
        "MEM-CCCCDDDD": {"title": "private", "access": "private", "created_at": "2026-01-03"},
    }), encoding="utf-8")
    paths["LTM_FULL_FULL_MD"].write_text("## MEM-EEEEFFFF\nLTM public\n", encoding="utf-8")
    paths["LTM_FULL_META_JSON"].write_text(json.dumps({
        "MEM-EEEEFFFF": {"title": "LTM public", "access": "public", "created_at": "2026-01-02"},
    }), encoding="utf-8")

    entries = module.MemoryStore().list_public_entries()
    assert [item["id"] for item in entries] == ["MEM-EEEEFFFF", "MEM-AAAABBBB"]
    assert [item["memory_layer"] for item in entries] == ["LTM/Full", "STM"]


def test_spec685_deposition_http_contract_and_errors(tmp_path):
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli, _deposition_reader())
    try:
        status, index = _request(server, "GET", "/api/deposition")
        assert status == 200
        assert index["schema_version"] == "seed_gui_deposition_index.v1"

        for kind, item_id in (
            ("memory", "MEM-PUBLIC01"),
            ("container", "PRJ-001"),
            ("relation", "REL-USER"),
        ):
            status, detail = _request(server, "GET", f"/api/deposition/{kind}?id={item_id}")
            assert status == 200
            assert detail["schema_version"] == "seed_gui_deposition_detail.v1"
            assert detail["kind"] == kind

        assert _request(server, "GET", "/api/deposition?path=secret")[0] == 400
        assert _request(server, "GET", "/api/deposition/memory?path=secret")[0] == 400
        assert _request(server, "GET", "/api/deposition/memory?id=MEM-PRIVATE1")[0] == 404
        assert _request(server, "GET", "/api/deposition/relation?id=missing")[0] == 404
    finally:
        _close(server, thread)


def test_spec685_deposition_store_failure_is_503(tmp_path):
    class BrokenReader:
        def index(self):
            raise OSError("unavailable")

        def detail(self, _kind, _item_id):
            raise OSError("unavailable")

    server, thread = _server(tmp_path, FakeCli(), BrokenReader())
    try:
        assert _request(server, "GET", "/api/deposition")[0] == 503
        assert _request(server, "GET", "/api/deposition/memory?id=MEM-X")[0] == 503
    finally:
        _close(server, thread)


def test_spec686_container_focus_processor_receipt_and_store_reread(tmp_path):
    workbench = FakeWorkbenchStore()
    container_store = FakeContainerStore()
    reader = _deposition_reader(workbench, container_store)
    server, thread = _server(tmp_path, FakeCli(), reader)
    try:
        headers = _json_headers(server)

        status, opened = _request(
            server,
            "POST",
            "/api/container/focus",
            body=json.dumps({"action": "open", "container_id": "PRJ-001"}),
            headers=headers,
        )
        assert status == 200
        assert opened["schema_version"] == "seed_gui_container_focus_result.v1"
        assert opened["submission_source"] == "seed_gui"
        assert opened["receipt"]["tool_id"] == "container_focus"
        assert opened["receipt"]["protocol_tool_receipt"] is True
        assert opened["receipt"]["status"] == "applied"
        assert opened["focus"] == {"current": "PRJ-001", "previous": ""}

        status, index = _request(server, "GET", "/api/deposition")
        assert status == 200
        assert index["focus"]["current"] == "PRJ-001"
        assert {item["id"]: item["focus"] for item in index["containers"]} == {
            "PRJ-001": True,
            "DC-002": False,
        }

        status, closed = _request(
            server,
            "POST",
            "/api/container/focus",
            body=json.dumps({"action": "close", "container_id": "PRJ-001"}),
            headers=headers,
        )
        assert status == 200
        assert closed["focus"] == {"current": "", "previous": "PRJ-001"}

        status, restored = _request(
            server,
            "POST",
            "/api/container/focus",
            body=json.dumps({"action": "restore", "container_id": ""}),
            headers=headers,
        )
        assert status == 200
        assert restored["receipt"]["action"] == "restore"
        assert restored["focus"] == {"current": "PRJ-001", "previous": ""}
        assert container_store.focus_calls == [
            ("PRJ-001", True),
            ("PRJ-001", False),
            ("PRJ-001", True),
        ]
    finally:
        _close(server, thread)


def test_spec686_container_focus_validation_conflict_and_shared_lock(tmp_path):
    workbench = FakeWorkbenchStore(current="PRJ-001")
    reader = _deposition_reader(workbench)
    server, thread = _server(tmp_path, FakeCli(), reader)
    headers = _json_headers(server)
    try:
        request = lambda action, container_id: _request(
            server,
            "POST",
            "/api/container/focus",
            body=json.dumps({"action": action, "container_id": container_id}),
            headers=headers,
        )
        assert _request(
            server,
            "POST",
            "/api/container/focus",
            body=json.dumps({"action": "open", "container_id": "PRJ-001"}),
            headers={"Content-Type": "application/json"},
        )[0] == 403
        assert request("write", "PRJ-001")[0] == 400
        assert request("open", "")[0] == 400
        assert request("restore", "PRJ-001")[0] == 400
        assert request("open", "PRJ-MISSING")[0] == 404
        assert request("close", "DC-002")[0] == 409
        assert workbench.current == "PRJ-001"

        lock = server.RequestHandlerClass.mutation_lock
        lock.acquire()
        try:
            assert request("open", "DC-002")[0] == 409
            status, index = _request(server, "GET", "/api/deposition")
            assert status == 200
            assert index["focus"]["current"] == "PRJ-001"
        finally:
            lock.release()

        workbench.current = None
        workbench.previous = None
        assert request("close", "PRJ-001")[0] == 409
        assert request("restore", "")[0] == 409

        workbench.current = "PRJ-001"
        workbench.previous = "PRJ-MISSING"
        status, rejected = request("restore", "")
        assert status == 404
        assert rejected["error"] == "container_not_found"
        assert rejected["receipt"]["tool_id"] == "container_focus"
        assert rejected["receipt"]["status"] == "rejected"
        assert rejected["focus"] == {"current": "PRJ-001", "previous": ""}
    finally:
        _close(server, thread)


def _task_guide():
    return {
        "guide_id": "task:T-001",
        "kind": "task_execution",
        "task_id": "T-001",
        "task_title": "结构化 GUI 任务",
        "task_goal": "只显示 Workbench 真账，不从对话猜进度。",
        "source_requirements": [{
            "requirement_id": "req_01",
            "source_ref": r"E:\private\source.md",
            "summary": "保留真实证据边界。",
        }],
        "items": [
            {
                "item_id": "task_progress",
                "mandatory": True,
                "options": [],
            },
            {
                "item_id": "item_01",
                "task_record_type": "item",
                "title": "读取真源",
                "mandatory": True,
                "status": "done",
                "evidence_refs": ["EV-READ0001", r"file_read:E:\private\source.md"],
            },
            {
                "item_id": "item_02",
                "task_record_type": "item",
                "title": "完成投影",
                "mandatory": True,
                "status": "open",
            },
        ],
        "acceptance": [{
            "acceptance_id": "acc_01",
            "description": "投影可追溯",
            "required": True,
            "status": "pending",
        }],
        "pending_inputs": [{
            "pending_input_id": "input_01",
            "status": "pending",
            "summary": "用户追加 GUI 约束。",
            "source_refs": ["round:572:reaction", r"E:\private\input.txt"],
        }],
        "risk_notes": ["不得用普通文案冒充完成。"],
    }


def test_spec687_task_projection_is_structured_and_path_free():
    reader = _deposition_reader(
        workbench_store=FakeWorkbenchStore(task_guide=_task_guide())
    )
    projection = reader.task_projection()

    assert projection["schema_version"] == "seed_gui_task_projection.v1"
    assert projection["active_task"] == "T-001"
    assert projection["active_guides"] == {
        "rhythm": "",
        "work": "task:T-001",
    }
    assert [item["id"] for item in projection["task"]["items"]] == [
        "item_01", "item_02",
    ]
    assert projection["task"]["items"][0]["evidence_refs"] == ["EV-READ0001"]
    assert projection["task"]["pending_inputs"][0]["source_refs"] == [
        "round:572:reaction",
    ]
    assert projection["summary"] == {
        "state": "open",
        "open_items": 1,
        "pending_acceptance": 1,
        "open_pending_inputs": 1,
        "blocked_records": 0,
        "evidence_refs": 1,
    }
    encoded = json.dumps(projection)
    assert "E:\\\\private" not in encoded
    assert '"source_ref":' not in encoded


def test_spec687_task_http_and_relay_contract(tmp_path):
    fake_cli = FakeCli()
    reader = _deposition_reader(
        workbench_store=FakeWorkbenchStore(task_guide=_task_guide())
    )
    server, thread = _server(tmp_path, fake_cli, reader)
    try:
        status, task = _request(server, "GET", "/api/workbench/task")
        assert status == 200
        assert task["active_task"] == "T-001"
        assert _request(server, "GET", "/api/workbench/task?path=secret")[0] == 400

        body = json.dumps({
            "permission_level": "limited",
            "unlimited_confirmed": False,
        })
        status, response = _request(
            server,
            "POST",
            "/api/runtime/relay",
            body=body,
            headers=_json_headers(server),
        )
        assert status == 200
        assert response["ok"] is True
        assert len(fake_cli.calls) == 1
        command = fake_cli.calls[0][0]
        assert command == ["resident", "relay", "limited"]
    finally:
        _close(server, thread)


def test_spec704_tick_and_stop_http_contracts(tmp_path):
    fake_cli = FakeCli(stop_receipt={
        "schema_version": "seed_gui_runtime_stop_receipt.v1",
        "accepted": True,
        "reason": "stop_requested",
        "stage": "reaction",
        "round": 704,
        "recorded_at": "2026-07-24T00:00:00+08:00",
    })
    server, thread = _server(tmp_path, fake_cli)
    permission = json.dumps({
        "permission_level": "limited",
        "unlimited_confirmed": False,
    })
    try:
        status, response = _request(
            server,
            "POST",
            "/api/runtime/tick",
            body=permission,
            headers=_json_headers(server),
        )
        assert status == 200
        assert response["command"] == "tick"
        assert fake_cli.calls[-1][0] == ["resident", "tick", "limited"]

        status, receipt = _request(
            server,
            "POST",
            "/api/runtime/stop",
            body="{}",
            headers=_json_headers(server),
        )
        assert status == 200
        assert receipt["schema_version"] == "seed_gui_runtime_stop_receipt.v1"
        assert receipt["reason"] == "stop_requested"
        assert _request(
            server,
            "POST",
            "/api/runtime/stop",
            body='{"unexpected":true}',
            headers=_json_headers(server),
        )[0] == 400
        assert _request(
            server,
            "POST",
            "/api/runtime/stop",
            body="{}",
            headers={"Content-Type": "application/json"},
        )[0] == 403
    finally:
        _close(server, thread)


def test_spec704_stop_without_round_is_conflict(tmp_path):
    server, thread = _server(tmp_path, FakeCli())
    try:
        status, failure = _request(
            server,
            "POST",
            "/api/runtime/stop",
            body="{}",
            headers=_json_headers(server),
        )
        assert status == 409
        assert failure["error"] == "no_round_in_flight"
    finally:
        _close(server, thread)


def test_spec705_desktop_shutdown_is_token_guarded_and_desktop_only(tmp_path):
    token = "a" * 64
    session_id = "b" * 32

    ordinary, ordinary_thread = _server(tmp_path / "ordinary", FakeCli())
    try:
        assert _request(
            ordinary,
            "POST",
            "/api/desktop/shutdown",
            body="{}",
            headers=_json_headers(ordinary),
        )[0] == 404
    finally:
        _close(ordinary, ordinary_thread)

    fake_runtime = FakeCli()
    server, thread = _server(
        tmp_path / "desktop",
        fake_runtime,
        desktop_control_token=token,
        desktop_session_id=session_id,
    )
    headers = {
        **_json_headers(server),
        "X-UPSP-Desktop-Control": token,
    }
    assert _request(
        server,
        "POST",
        "/api/desktop/shutdown",
        body="{}",
        headers={"Content-Type": "application/json"},
    )[0] == 403
    assert _request(
        server,
        "POST",
        "/api/desktop/shutdown",
        body="{}",
        headers={
            **_json_headers(server),
            "X-UPSP-Desktop-Control": "c" * 64,
        },
    )[0] == 403
    assert _request(
        server,
        "POST",
        "/api/desktop/shutdown",
        body='{"unexpected":true}',
        headers=headers,
    )[0] == 400

    status, receipt = _request(
        server,
        "POST",
        "/api/desktop/shutdown",
        body="{}",
        headers=headers,
    )
    assert status == 202
    assert receipt == {
        "schema_version": "upsp_desktop_shutdown_receipt.v1",
        "accepted": True,
        "session_id": session_id,
    }
    assert token not in json.dumps(receipt)
    thread.join(timeout=5)
    assert not thread.is_alive()
    server.server_close()
    assert fake_runtime.closed is True


def test_spec705_desktop_environment_and_ready_record(tmp_path, monkeypatch):
    module = _load_module_from_path("serve_seed_gui", SERVER_PATH)
    monkeypatch.setenv("UPSP_DESKTOP_CONTROL_TOKEN", "a" * 64)
    monkeypatch.setenv("UPSP_DESKTOP_SESSION_ID", "b" * 32)
    assert module._desktop_environment() == ("a" * 64, "b" * 32)
    assert "UPSP_DESKTOP_CONTROL_TOKEN" not in os.environ
    assert "UPSP_DESKTOP_SESSION_ID" not in os.environ

    fake_runtime = FakeCli()
    server, thread = _server(tmp_path, fake_runtime)
    try:
        record = module._desktop_ready_record(server, "b" * 32)
        assert record == {
            "schema_version": "upsp_desktop_ready.v1",
            "process_id": os.getpid(),
            "session_id": "b" * 32,
            "origin": f"http://127.0.0.1:{server.server_address[1]}",
            "product_version": "0.1.0-alpha.5",
        }
    finally:
        _close(server, thread)

    monkeypatch.setenv("UPSP_DESKTOP_CONTROL_TOKEN", "short")
    with pytest.raises(ValueError, match="desktop_control_token_invalid"):
        module._desktop_environment()


def test_spec705_desktop_ready_is_the_only_stdout_record(
        tmp_path, monkeypatch, capsys):
    module = _load_module_from_path("serve_seed_gui_stdout", SERVER_PATH)
    monkeypatch.setenv("UPSP_DESKTOP_CONTROL_TOKEN", "a" * 64)
    monkeypatch.setenv("UPSP_DESKTOP_SESSION_ID", "b" * 32)

    class FakeServer:
        server_address = ("127.0.0.1", 8770)

        @staticmethod
        def serve_forever():
            print("[UPSP] runtime output")

        @staticmethod
        def server_close():
            return None

    def make_server(*_args, **_kwargs):
        print("[UPSP] 初始化运行环境...")
        return FakeServer()

    monkeypatch.setattr(module, "make_server", make_server)
    assert module.main([
        "--desktop",
        "--port", "8770",
        "--round-dir", str(tmp_path / "round"),
        "--gui-root", str(GUI_ROOT),
    ]) == 0

    captured = capsys.readouterr()
    ready_lines = captured.out.splitlines()
    assert len(ready_lines) == 1
    assert json.loads(ready_lines[0])["schema_version"] == (
        "upsp_desktop_ready.v1"
    )
    assert "[UPSP] 初始化运行环境..." in captured.err
    assert "[UPSP] runtime output" in captured.err


def test_spec704_port_conflict_fails_without_starting_runtime(tmp_path):
    module = _load_module_from_path("serve_seed_gui_port_conflict", SERVER_PATH)
    blocker = socket.socket()
    blocker.bind(("127.0.0.1", 0))
    blocker.listen()
    port = blocker.getsockname()[1]
    fake = FakeCli()
    round_dir = tmp_path / "round"
    round_dir.mkdir()
    try:
        with pytest.raises(OSError):
            module.make_server(
                port,
                round_dir=round_dir,
                gui_root=_gui_root(tmp_path),
                runtime_service=fake,
            )
        assert fake.host["port"] == 0
        assert fake.closed is False
    finally:
        blocker.close()


def test_spec687_relay_validation_lock_and_status_are_fail_closed(tmp_path):
    fake_cli = FakeCli()
    server, thread = _server(tmp_path, fake_cli)
    body = json.dumps({
        "permission_level": "limited",
        "unlimited_confirmed": False,
    })
    try:
        assert _request(
            server,
            "POST",
            "/api/runtime/relay",
            body=body,
            headers={"Content-Type": "application/json"},
        )[0] == 403
        assert _request(
            server,
            "POST",
            "/api/runtime/relay",
            body="{}",
            headers=_json_headers(server),
        )[0] == 400
        assert _request(
            server,
            "POST",
            "/api/runtime/relay",
            body=json.dumps({
                "permission_level": "unlimited",
                "unlimited_confirmed": False,
            }),
            headers=_json_headers(server),
        )[0] == 403
        assert fake_cli.calls == []

        lock = server.RequestHandlerClass.relay_lock
        lock.acquire()
        try:
            assert _request(
                server,
                "POST",
                "/api/runtime/relay",
                body=body,
                headers=_json_headers(server),
            )[0] == 409
            status, runtime = _request(server, "GET", "/api/runtime/status")
            assert status == 200
            assert runtime["relay_in_flight"] is True
            assert runtime["mutation_in_flight"] is False
        finally:
            lock.release()
    finally:
        _close(server, thread)


def test_spec704_relay_service_failures_do_not_retry(tmp_path):
    cases = (
        (FakeCli(relay_returncode=2), 503, "runtime_host_failed"),
        (FakeCli(relay_returncode=2, relay_error_code="relay_not_pending"), 409, "relay_not_pending"),
        (FakeCli(fail_with=OSError("host unavailable")), 503, "runtime_host_failed"),
    )
    for index, (fake_cli, expected_status, expected_error) in enumerate(cases):
        case_root = tmp_path / str(index)
        case_root.mkdir()
        server, thread = _server(case_root, fake_cli)
        try:
            status, response = _request(
                server,
                "POST",
                "/api/runtime/relay",
                body=json.dumps({
                    "permission_level": "limited",
                    "unlimited_confirmed": False,
                }),
                headers=_json_headers(server),
            )
            assert status == expected_status
            assert response["error"] == expected_error
            assert len(fake_cli.calls) == 1
        finally:
            _close(server, thread)


def test_spec688_seed_gui_response_boundary_suppresses_only_client_disconnects(monkeypatch):
    module = _load_module_from_path("serve_seed_gui_spec688_disconnect", SERVER_PATH)
    handler = object.__new__(module.SeedGuiHandler)

    def raise_error(error_type):
        def raising(*_args, **_kwargs):
            raise error_type("client left")
        return raising

    calls = (
        ("_send_json", (200, {"ok": True})),
        ("_send_file", (Path("unused"), "text/plain")),
    )
    for method_name, args in calls:
        for error_type in module.CLIENT_DISCONNECT_ERRORS:
            monkeypatch.setattr(
                module.RoundLiveHandler,
                method_name,
                raise_error(error_type),
            )
            getattr(handler, method_name)(*args)

        monkeypatch.setattr(
            module.RoundLiveHandler,
            method_name,
            raise_error(RuntimeError),
        )
        try:
            getattr(handler, method_name)(*args)
        except RuntimeError as exc:
            assert str(exc) == "client left"
        else:
            raise AssertionError("non-disconnect errors must stay visible")


def test_spec688_existing_open_launcher_opens_local_url_and_closes_server(tmp_path, monkeypatch):
    module = _load_module_from_path("serve_seed_gui_spec688_launcher", SERVER_PATH)
    opened = []

    class FakeServer:
        server_address = ("127.0.0.1", 43125)
        served = False
        closed = False

        def serve_forever(self):
            self.served = True
            raise KeyboardInterrupt

        def server_close(self):
            self.closed = True

    server = FakeServer()
    monkeypatch.setattr(module, "make_server", lambda *_args: server)
    monkeypatch.setattr(module.webbrowser, "open", lambda url: opened.append(url))

    class ImmediateTimer:
        def __init__(self, _delay, callback):
            self.callback = callback

        def start(self):
            self.callback()

    monkeypatch.setattr(module.threading, "Timer", ImmediateTimer)

    assert module.main([
        "--port", "0",
        "--round-dir", str(tmp_path / "round"),
        "--gui-root", str(tmp_path / "gui"),
        "--open",
    ]) == 0
    assert opened == ["http://127.0.0.1:43125/"]
    assert server.served is True
    assert server.closed is True


def test_spec688_gui_incremental_recovery_accessibility_and_export_contract():
    gui_root = GUI_ROOT
    app_source = _gui_ts_source()
    css_source = (gui_root / "styles.css").read_text(encoding="utf-8")
    index_source = (gui_root / "index.html").read_text(encoding="utf-8")
    intro = (gui_root / "manual" / "intro.md").read_text(encoding="utf-8")
    audit_manual = (gui_root / "manual" / "audit-tools.md").read_text(encoding="utf-8")

    assert "./api/live/events?round=latest&after=${after}" in app_source
    assert "return fetchFullLiveProjection();" in app_source
    assert "if (polling.runtime)" in app_source
    assert "if (polling.task)" in app_source
    assert "if (polling.deposition)" in app_source
    assert "document.hidden" in app_source
    assert 'document.addEventListener("visibilitychange"' in app_source
    assert 'data-retry-projection="${escapeHtml(retryTarget)}"' in app_source
    assert 'schema_version: "seed_gui_evidence_export.v1"' in app_source
    assert "URL.createObjectURL(new Blob" in app_source
    assert 'role="tabpanel"' in app_source
    assert 'aria-controls="systemWindowPanel"' in app_source
    assert "input:focus-visible" in css_source
    assert "textarea:focus-visible" in css_source
    assert 'aria-describedby="sendFeedback"' in index_source
    assert "python tools/serve_seed_gui.py --open" in intro
    assert "seed_gui_evidence_export.v1" in audit_manual


def test_spec690_gui_subject_centered_reduction_contract():
    gui_root = GUI_ROOT
    app_source = _gui_ts_source()
    css_source = (gui_root / "styles.css").read_text(encoding="utf-8")
    index_source = (gui_root / "index.html").read_text(encoding="utf-8")
    intro = (gui_root / "manual" / "intro.md").read_text(encoding="utf-8")
    base_serial = (gui_root / "manual" / "base-serial.md").read_text(encoding="utf-8")

    system_rule = css_source.split(".system-window {", 1)[1].split("}", 1)[0]
    overview_rule = css_source.split(".overview-pane {", 1)[1].split("}", 1)[0]
    dialogue_rule = css_source.split(".dialogue-backplane {", 1)[1].split("}", 1)[0]

    assert "min(50vw, 820px)" in css_source
    assert "--conversation-width: min(75vh, 960px);" in css_source
    assert "position: absolute;" in system_rule
    assert "position: absolute;" in overview_rule
    assert "background: transparent;" in dialogue_rule
    assert "overviewCollapsed: true" in app_source
    assert "systemWindowOpen: false" in app_source
    assert "overviewSectionsCollapsed: new Set()" in app_source
    assert 'fromSurfaceNav || pageButton.matches(".persona-avatar")' in app_source
    assert "togglesSystemWindow && isActiveTarget" in app_source
    assert "*::-webkit-scrollbar-track" in css_source
    assert "*::-webkit-scrollbar-button" in css_source
    assert 'data-overview-section="${escapeHtml(section.id)}"' in app_source
    assert 'aria-expanded="${collapsed ? "false" : "true"}"' in app_source
    assert 'aria-controls="overview-section-${escapeHtml(section.id)}"' in app_source
    assert 'class="app-shell overview-collapsed"' in index_source
    assert 'aria-hidden="true" inert' in index_source
    assert 'data-i18n="当前对话">当前对话</span>' in index_source
    assert 'data-i18n="运行概览">运行概览</span>' in index_source
    assert "tactical-backplane" not in index_source
    assert "tactical-backplane" not in css_source
    assert "alpha / 主体镜像" not in app_source
    assert "Workbench Task" not in app_source
    assert "GUI 顶栏中的同一位格对话线程入口" in intro
    assert "右侧只展示真实 Round 与 Frames 投影" in base_serial


def test_spec692_gui_tool_trace_compaction_contract():
    gui_root = GUI_ROOT
    app_source = _gui_ts_source()
    css_source = (gui_root / "styles.css").read_text(encoding="utf-8")
    audit_manual = (gui_root / "manual" / "audit-tools.md").read_text(encoding="utf-8")

    trace_renderer = app_source.split("function renderChatTraceStep", 1)[1].split("function renderChat()", 1)[0]
    trace_step_renderer = trace_renderer.split("function renderChatTraceGroup", 1)[0]

    assert '"tool-call", "tool-result"' in app_source
    disclosure_filter = app_source.split("function isChatDisclosureCard", 1)[1].split("function chatTraceSummary", 1)[0]
    assert "settlement" not in disclosure_filter
    assert "receipt" not in disclosure_filter
    assert "function buildChatItems(" in app_source
    assert 'card.type === "user" && text.startsWith("【本轮交互】")' in app_source
    assert 'text.startsWith("【本轮交互】")' in app_source
    assert 'items.push({ type: "tool-trace", cards: trace })' in app_source
    assert '<details class="chat-tool-group"' in trace_renderer
    assert '<details class="chat-tool-step"' in trace_renderer
    assert 't("工具轨迹 · {count} 次调用", { count: callCount })' in trace_renderer
    assert 'memory_write: "写入记忆"' in app_source
    assert 'memory_container_create: "创建记忆容器"' in app_source
    assert 'card.content_md || card.content_raw || ""' in trace_renderer
    assert "renderMarkdownDocument(documentId, chatTraceCode(content))" in trace_renderer
    assert 'content.indexOf("```")' in app_source
    assert "content_raw" in trace_step_renderer
    assert 'conversationDisclosure: new Map()' in app_source
    assert 'details[data-conversation-card-key]' in app_source
    assert "state.conversationDisclosure.set" in app_source
    assert 'els.chatThread.addEventListener("keydown"' in app_source
    assert '["Enter", " "].includes(event.key)' in app_source
    assert ".chat-tool-group {" in css_source
    assert ".chat-tool-step { border: 0; background: transparent; }" in css_source
    assert ".chat-disclosure" not in css_source
    assert "max-height: min(50vh, 560px);" in css_source
    assert "主对话只把工具调用与工具结果按自然语言边界合并成一条两级原生折叠轨迹" in audit_manual


def test_spec693_gui_retained_round_conversation_contract():
    gui_root = GUI_ROOT
    app_source = _gui_ts_source()
    css_source = (gui_root / "styles.css").read_text(encoding="utf-8")
    intro = (gui_root / "manual" / "intro.md").read_text(encoding="utf-8")

    history_sync = app_source.split("async function syncConversationHistory", 1)[1].split("async function fetchFullLiveProjection", 1)[0]
    chat_renderer = app_source.split("function retainedConversationItems", 1)[1].split("function renderStage", 1)[0]

    assert "conversationRounds: new Map()" in app_source
    assert "conversationRoundOrder: []" in app_source
    assert '"./api/rounds"' in history_sync
    assert "./api/live/state?round=${round}" in history_sync
    assert ".sort((left, right) => left - right)" in history_sync
    assert "runtimeProjection.conversationRounds.delete(round)" in history_sync
    assert "buildChatItems(projection?.conversation || [])" in chat_renderer
    assert "function renderChatTraceGroup(" in app_source
    assert '`${round ?? "none"}:trace:' in app_source
    assert "较早对话未完全载入" in chat_renderer
    assert 'data-retry-projection="history"' in chat_renderer
    assert "wasAtLatest" in chat_renderer
    assert "previousAnchorKey" in chat_renderer
    assert "data-chat-anchor" in chat_renderer
    assert "localStorage" not in history_sync
    assert ".chat-history-warning button { padding: 0; border: 0;" in css_source
    assert "当前保留的 Round 会按时间顺序组成连续对话" in intro


def test_spec704_gui_stop_control_uses_resident_status_and_empty_post():
    runtime_source = _gui_ts_source("runtime.ts")
    view_source = _gui_ts_source("view.ts")
    event_source = _gui_ts_source("events.ts")
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    server_source = SERVER_PATH.read_text(encoding="utf-8")

    assert 'id="stopButton"' in index_source
    assert '"seed_gui_runtime_status.v2"' in runtime_source
    assert '"./api/runtime/stop"' in runtime_source
    assert 'body: "{}"' in runtime_source
    assert 'receipt.schema_version !== "seed_gui_runtime_stop_receipt.v1"' in runtime_source
    assert 'target.closest("#stopButton")' in event_source
    assert 'stage !== "cleanup_local"' in view_source
    assert "stopRequested && runtimeProjection.status?.current_round != null" in view_source
    assert "els.stopButton.disabled" in view_source
    assert "subprocess" not in server_source
    assert "message-file" not in server_source


def test_spec694_gui_substage_animation_only_tracks_open_transition():
    css_source = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    system_rule = css_source.split(".system-window {", 1)[1].split("}", 1)[0]
    open_rule = css_source.split(".app-shell.system-open .stage-page {", 1)[1].split("}", 1)[0]

    assert "system-window-in" not in system_rule
    assert "animation: system-window-in 180ms var(--ease-out) both;" in open_rule
    assert ".system-window.closing { animation: system-window-out 140ms ease-in both; }" in css_source


def test_spec694_gui_substage_open_system_page_reflows_dialogue_without_moving_nav_anchor():
    css_source = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    shell_rule = css_source.split(".app-shell {", 1)[1].split("}", 1)[0]
    nav_expanded_rule = css_source.split(".left-rail.nav-expanded {", 1)[1].split("}", 1)[0]
    desktop_open_rule = css_source.split("@media (min-width: 761px) {", 1)[1].split("}", 1)[0]
    reading_column_rule = css_source.split(".dialogue-backplane > .pane-head,", 1)[1].split("}", 1)[0]

    assert "grid-template-columns: var(--rail-collapsed) minmax(0, 1fr);" in shell_rule
    assert "width: var(--rail-expanded);" in nav_expanded_rule
    assert "grid-template-columns" not in nav_expanded_rule
    assert ".app-shell.system-open .dialogue-backplane {" in desktop_open_rule
    assert "left: var(--system-window-width);" in desktop_open_rule
    assert "padding-inline: var(--space-lg);" in desktop_open_rule
    assert "max-width: 100%;" in reading_column_rule


def test_spec694_gui_substage_three_by_four_stages_when_room_allows():
    css_source = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    shell_rule = css_source.split(".app-shell {", 1)[1].split("}", 1)[0]

    assert "--conversation-width: min(75vh, 960px);" in css_source
    assert "--system-window-width: clamp(" in shell_rule
    assert "75vh" in shell_rule
    assert "100vw - var(--rail-collapsed) - var(--conversation-width)" in shell_rule
    assert "min(50vw, 820px)" in shell_rule


def test_spec695_typescript_is_the_only_gui_source_and_bundle_stays_host_compatible():
    package = json.loads((GUI_ROOT / "package.json").read_text(encoding="utf-8"))
    tsconfig = json.loads((GUI_ROOT / "tsconfig.json").read_text(encoding="utf-8"))
    source_names = {path.name for path in (GUI_ROOT / "src").glob("*.ts")}
    bundle = (GUI_ROOT / "app.js").read_text(encoding="utf-8")
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    app_source = (GUI_ROOT / "src" / "app.ts").read_text(encoding="utf-8")

    assert package["private"] is True
    assert package["devDependencies"]["esbuild"] == "0.28.1"
    assert package["devDependencies"]["typescript"] == "7.0.2"
    assert package["scripts"]["build:check"] == "node scripts/build.mjs --check"
    assert tsconfig["compilerOptions"]["strict"] is True
    assert tsconfig["compilerOptions"]["noEmit"] is True
    assert source_names == {
        "app.ts", "bootstrap.ts", "contracts.ts", "events.ts", "markdown-mermaid.ts",
        "i18n.ts", "markdown.ts", "runtime.ts", "state.ts", "view.ts",
    }
    assert bundle.startswith("/* Generated from src/app.ts. Do not edit app.js directly. */")
    assert "sourceMappingURL" not in bundle
    assert 'from "./runtime"' not in view_source
    assert "initEvents();" in app_source
    assert "pollRuntime();" in app_source
    assert "@ts-nocheck" not in _gui_ts_source()
    assert ": any" not in _gui_ts_source()


def test_spec696_rich_markdown_uses_one_sanitized_typed_pipeline():
    package = json.loads((GUI_ROOT / "package.json").read_text(encoding="utf-8"))
    markdown_source = (GUI_ROOT / "src" / "markdown.ts").read_text(encoding="utf-8")
    mermaid_source = (GUI_ROOT / "src" / "markdown-mermaid.ts").read_text(encoding="utf-8")
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    markdown_css = (GUI_ROOT / "markdown.css").read_text(encoding="utf-8")
    host_source = SERVER_PATH.read_text(encoding="utf-8")

    for dependency in (
        "unified", "remark-parse", "remark-gfm", "remark-math", "remark-rehype",
        "rehype-sanitize", "rehype-katex", "rehype-highlight", "rehype-stringify",
        "highlight.js", "katex", "mermaid",
    ):
        assert dependency in package["devDependencies"]
    assert "allowDangerousHtml: false" in markdown_source
    assert markdown_source.index(".use(rehypeSanitize") < markdown_source.index(".use(rehypeKatex")
    assert 'clobberPrefix: documentPrefix(documentId)' in markdown_source
    assert 'footnoteLabel: t("脚注")' in markdown_source
    assert 'plainText: ["mermaid", "math", "text", "plaintext"]' in markdown_source
    assert "languages: { bash, css, diff, javascript, json, markdown, powershell, python, typescript, xml, yaml }" in markdown_source
    assert 'dataMarkdownImageUrl' in markdown_source
    assert 'approvedImageDocuments = new Set<string>()' in markdown_source
    assert "localStorage" not in markdown_source
    assert 'const path = "./markdown-mermaid.js"' in markdown_source
    assert 'securityLevel: "strict"' in mermaid_source
    assert "startOnLoad: false" in mermaid_source
    assert 'card.type === "assistant-streaming" ? card.content_raw || "" : card.content_md || card.content_raw || ""' in view_source
    assert 'card.type === "user"' in view_source
    assert "function renderMarkdown(" not in view_source
    assert '<link rel="stylesheet" href="./markdown.css" />' in index_source
    assert ".md-document" in markdown_css
    assert "/markdown-mermaid.js" in host_source
    assert "/assets/markdown/" in host_source
    assert (GUI_ROOT / "markdown-mermaid.js").is_file()
    assert len(list((GUI_ROOT / "assets" / "markdown").glob("*.woff2"))) == 20


def test_spec697_context_review_reuses_rich_markdown_without_restructuring():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    markdown_source = (GUI_ROOT / "src" / "markdown.ts").read_text(encoding="utf-8")
    context_renderer = view_source.split("function contextPaneMarkdown", 1)[1].split(
        "function renderRuntimeAuditPage", 1
    )[0]

    assert 'pane?.content_md || pane?.content_raw || ""' in context_renderer
    assert '["00_call_header", "01_tool_header", "02_generation_config"]' in context_renderer
    assert "JSON.parse(raw)" in context_renderer
    assert '`context:${round}:${frame.frame_id}:${pane?.id || "empty"}`' in context_renderer
    assert "renderMarkdownDocument(" in context_renderer
    assert "content_blocks" not in context_renderer
    assert "hydrateMarkdownDocuments(contextScroll, contextScroll)" in view_source
    assert ".runtime-context-workspace article" in markdown_source


def test_spec699_context_review_has_two_tabs_and_keeps_assembly_details():
    state_source = (GUI_ROOT / "src" / "state.ts").read_text(encoding="utf-8")
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    context_tabs = state_source.split("context: [", 1)[1].split("],", 1)[0]

    assert 'id: "guide", label: "分层导览"' in context_tabs
    assert 'id: "content", label: "内容详情"' in context_tabs
    assert all(label not in context_tabs for label in ("真账投影", "内容窗口", "装配说明"))
    assert '<details class="runtime-assembly context-assembly-details"><summary>${t("装配详情")}</summary>' in view_source
    assert 'if (tab === "assembly")' not in view_source


def test_spec700_memory_body_uses_shared_markdown_renderer():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")

    assert 'documentId: `memory:${itemId}`' in view_source
    assert 'contentMd: memoryBodyMarkdown(detail.body)' in view_source
    assert 'escapeHtml(detail.body || "正文为空。")' not in view_source


def test_spec702_memory_body_fields_render_as_wrapping_table():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    markdown_source = (GUI_ROOT / "src" / "markdown.ts").read_text(encoding="utf-8")
    markdown_css = (GUI_ROOT / "src" / "markdown.css").read_text(encoding="utf-8")

    assert "memoryBodyMarkdown" in view_source
    assert '`| ${t("字段")} | ${t("内容")} |`' in markdown_source
    assert 'normalized.indexOf("：")' in markdown_source
    assert 'normalized.slice(separator + 1).trim() || "—"' in markdown_source
    assert '[data-markdown-document-id^="memory:"] table' in markdown_css
    assert "table-layout: fixed" in markdown_css
    assert "overflow-wrap: anywhere" in markdown_css


def test_spec703_context_headers_use_instruments_and_json_tables():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    css_source = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")

    assert 'const contextInstrumentPaneIds = ["00_call_header", "01_tool_header", "02_generation_config"]' in view_source
    assert '"00_call_header": "A_调用头"' in view_source
    assert '"99_popup": "7_弹窗层"' in view_source
    assert "contextPaneLabel(pane.id)" in view_source
    assert 'class="context-instrument-cluster"' in view_source
    assert "panes.slice(0, 3).map(renderContextInstrument)" in view_source
    assert "panes.slice(3).map" in view_source
    assert "hydrateLedgerJsonTables(contextScroll)" in view_source
    assert "contextInstrumentPaneIds.includes(state.activeRuntimePane)" in view_source
    assert ".context-instrument {" in css_source
    assert "clip-path: polygon" in css_source
    assert ".context-instrument:focus-visible" in css_source


def test_spec704_context_round_selector_and_tool_annotations():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    state_source = (GUI_ROOT / "src" / "state.ts").read_text(encoding="utf-8")
    events_source = (GUI_ROOT / "src" / "events.ts").read_text(encoding="utf-8")
    runtime_source = (GUI_ROOT / "src" / "runtime.ts").read_text(encoding="utf-8")

    assert "selectedContextRound: null" in state_source
    assert 'closest<HTMLSelectElement>("[data-context-round]")' in events_source
    assert "state.selectedContextRound !== null && !retained.has(state.selectedContextRound)" in runtime_source
    assert "runtimeProjection.conversationRoundOrder" in view_source
    assert ".filter((round) => round !== latest).reverse()" in view_source
    assert "data-context-round" in view_source
    assert 'context:${round}:${frame.frame_id}:${pane?.id || "empty"}' in view_source
    assert "record.description" in view_source
    assert "contextToolAnnotations" in view_source


def test_runtime_composer_clears_immediately_and_restores_failed_submission():
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    runtime_source = (GUI_ROOT / "src" / "runtime.ts").read_text(encoding="utf-8")

    assert 'placeholder="交互输入"' in index_source
    clear_at = runtime_source.index('els.messageInput.value = "";')
    send_at = runtime_source.index('await fetchRuntimeJson<JsonObject>("./api/runtime/send"')
    assert clear_at < send_at
    assert 'if (!els.messageInput.value) els.messageInput.value = message;' in runtime_source


def test_spec705_context_frame_selector_uses_embedded_frame_snapshots():
    gui_root = GUI_ROOT
    contracts_source = (gui_root / "src" / "contracts.ts").read_text(encoding="utf-8")
    state_source = (gui_root / "src" / "state.ts").read_text(encoding="utf-8")
    events_source = (gui_root / "src" / "events.ts").read_text(encoding="utf-8")
    view_source = (gui_root / "src" / "view.ts").read_text(encoding="utf-8")

    assert "context_panes?: ContextPane[]" in contracts_source
    assert "manifest?: JsonObject" in contracts_source
    assert "selectedContextFrame: null" in state_source
    assert 'closest<HTMLSelectElement>("[data-context-frame]")' in events_source
    assert "state.selectedContextFrame = null" in events_source
    assert "frame.context_panes || []" in view_source
    assert "JSON.stringify(frame.manifest || {}, null, 2)" in view_source
    assert "renderRuntimeFrames([frame])" in view_source
    assert "data-context-frame" in view_source
    assert ".filter((frame) => frame.frame_id !== frames.at(-1)?.frame_id).reverse()" in view_source


def test_spec706_context_tools_open_individual_shared_detail_dialogs():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    events_source = (GUI_ROOT / "src" / "events.ts").read_text(encoding="utf-8")
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")

    assert 'class="context-tool-index"' in view_source
    assert 'aria-haspopup="dialog"' in view_source
    assert 'data-context-tool="${escapeHtml(tool.name)}"' in view_source
    assert "查看详情" in view_source
    assert 'export function openContextToolAnnotation' in view_source
    assert 'sourceType: "TOOL"' in view_source
    assert 'parameters: record.parameters ?? {}' in view_source
    assert '## ${t("参数")}' in view_source
    assert 'JSON.stringify(tool.parameters, null, 2)' in view_source
    assert 'ledgerJson: true' in view_source
    assert 'documentId: `context-tool:${selectedRound.round}:${frame.frame_id}:${tool.name}`' in view_source
    assert 'closest<HTMLElement>("[data-context-tool]")' in events_source
    assert 'openContextToolAnnotation(contextTool.dataset.contextTool || "")' in events_source
    assert index_source.count('id="manualOverlay"') == 1


def test_spec707_tool_details_move_parameters_out_of_inline_header():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")

    assert "const { tools: _tools, ...metadata }" in view_source
    assert "JSON.stringify(metadata, null, 2)" in view_source
    assert 'aria-label="${t("工具详情目录")}"' in view_source
    assert "查看详情" in view_source
    assert 'summary: t("当前帧次工具注释与参数")' in view_source
    assert 'parameters: record.parameters ?? {}' in view_source
    assert '## ${t("参数")}' in view_source
    assert 'JSON.stringify(tool.parameters, null, 2)' in view_source
    assert 'ledgerJson: true' in view_source


def test_spec708_tool_header_uses_chinese_summary_instruments():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    css_source = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")

    assert 'class="context-tool-summary"' in view_source
    assert 'aria-label="${t("工具调用总览")}"' in view_source
    assert '[t("工具数量"), `${Array.isArray(data.tool_names) ? data.tool_names.length' in view_source
    assert '[t("权限级别"), data.permission_level === "limited" ? t("受限")' in view_source
    assert '[t("终端工具"), data.terminal_tool === "reaction_finalize" ? t("反应阶段收束")' in view_source
    assert 'toolSummary ? "" : renderMarkdownDocument' in view_source
    assert ".context-tool-summary dl" in css_source
    assert "grid-template-columns: repeat(4, minmax(0, 1fr))" in css_source


def test_spec701_memory_index_opens_shared_detail_dialog():
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    events_source = (GUI_ROOT / "src" / "events.ts").read_text(encoding="utf-8")
    runtime_source = (GUI_ROOT / "src" / "runtime.ts").read_text(encoding="utf-8")
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")

    assert 'class="deposition-workspace memory-index-only"' in view_source
    assert 'aria-haspopup="dialog"' in view_source
    assert 'export function openMemoryDetail' in view_source
    assert 'sourceType: "MEMORY"' in view_source
    assert 'data-retry-memory-detail' in view_source
    assert 'openMemoryDetail(itemId)' in events_source
    assert '{ render: false }' in events_source
    assert 'render = true' in runtime_source
    assert 'if (render && state.activePage === depositionPage(kind))' in runtime_source
    assert index_source.count('id="manualOverlay"') == 1


def test_spec698_protocol_center_uses_one_dialog_and_runtime_only_json_tables():
    state_source = (GUI_ROOT / "src" / "state.ts").read_text(encoding="utf-8")
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    runtime_source = (GUI_ROOT / "src" / "runtime.ts").read_text(encoding="utf-8")
    events_source = (GUI_ROOT / "src" / "events.ts").read_text(encoding="utf-8")
    markdown_source = (GUI_ROOT / "src" / "markdown.ts").read_text(encoding="utf-8")
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    host_source = SERVER_PATH.read_text(encoding="utf-8")

    audit_tabs = state_source.split("audit: [", 1)[1].split("],", 1)[0]
    assert 'code: "PROTOCOL.center", title: "协议中心"' in state_source
    assert [label in audit_tabs for label in ('label: "动态账本"', 'label: "规则"', 'label: "文档"')] == [True, True, True]
    assert all(old not in audit_tabs for old in ('id: "queue"', 'id: "receipts"', 'id: "logs"', 'id: "blockers"'))
    assert 'data-ledger-round' in view_source
    assert "round !== latest).reverse()" in view_source
    assert 'state.selectedLedgerRound ?? runtimeProjection.round' in runtime_source
    assert 'document_id=ledger' not in view_source
    assert '`ledger:${round}:${cardId}`' in view_source
    assert '`protocol:${kind}:${itemId}`' in view_source
    assert 'typeof card.content_md === "string"' in view_source
    ledger_detail = view_source.split("export function openLedgerEvent", 1)[1].split(
        "export async function openProtocolDocument", 1
    )[0]
    assert "content_raw" not in ledger_detail
    assert "ledgerJson: true" in ledger_detail
    assert "if (ledgerJson) hydrateLedgerJsonTables(els.manualBody);" in view_source
    assert "renderStructuredJsonSource" in markdown_source
    assert 'data-ledger-json-raw="true"' in markdown_source
    assert 'data-markdown-copy' in markdown_source
    assert index_source.count('id="manualOverlay"') == 1
    assert "openProtocolDocument" in events_source
    assert '"[data-ledger-event], [data-protocol-document]"' in events_source
    assert '["Enter", " ", "Spacebar"].includes(event.key)' in events_source
    assert '"/api/protocol/catalog"' in host_source
    assert '"/api/protocol/document"' in host_source
    assert "MAX_PROTOCOL_DOCUMENT_BYTES = 1024 * 1024" in host_source
    assert 'self.protocol_reader.document(kind, item_id)' in host_source


def test_spec700_global_settings_and_persona_routing_frontend_contract():
    contracts = (GUI_ROOT / "src" / "contracts.ts").read_text(encoding="utf-8")
    state_source = (GUI_ROOT / "src" / "state.ts").read_text(encoding="utf-8")
    runtime_source = (GUI_ROOT / "src" / "runtime.ts").read_text(encoding="utf-8")
    events_source = (GUI_ROOT / "src" / "events.ts").read_text(encoding="utf-8")
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    i18n_source = (GUI_ROOT / "src" / "i18n.ts").read_text(encoding="utf-8")
    styles = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    host_source = SERVER_PATH.read_text(encoding="utf-8")

    assert 'schema_version: "seed_gui_settings.v3"' in contracts
    assert '"audit", "settings"' in state_source
    assert 'title: "位格设置"' in state_source
    assert '{ id: "runtime", label: "运行设置"' in state_source
    assert '{ id: "routing", label: "模型路由"' in state_source
    assert '{ id: "context", label: "上下文与存储"' in state_source
    settings_tabs = state_source.split("settings: [", 1)[1].split("],", 1)[0]
    assert settings_tabs.index('id: "routing"') < settings_tabs.index('id: "context"') < settings_tabs.index('id: "runtime"')
    assert "模型服务" not in settings_tabs
    assert "界面与语言" not in settings_tabs
    assert 'id="globalSettingsToggle"' in index_source
    bottom_ledger = index_source.split('<footer class="bottom-ledger">', 1)[1].split("</footer>", 1)[0]
    assert 'id="globalSettingsToggle"' in bottom_ledger
    assert "flex: 0 0 var(--bottom-height);" in styles
    assert 'id="globalSettingsOverlay"' in index_source
    assert 'data-global-settings-tab="interface"' in index_source
    assert 'data-global-settings-tab="models"' in index_source
    assert 'data-global-settings-tab="about"' in index_source
    assert index_source.index('data-global-settings-tab="models"') < index_source.index('data-global-settings-tab="interface"')
    assert 'globalSettingsTab: "models"' in state_source
    assert 'fetchRuntimeJson<SettingsPayload>("./api/settings"' in runtime_source
    assert 'fetchRuntimeJson<AboutPayload>("./api/about"' in runtime_source
    assert 'fetchRuntimeJson<SettingsPayload>("./api/settings/model-catalog"' in runtime_source
    assert 'fetchRuntimeJson<SettingsPayload>("./api/settings/provider-key"' in runtime_source
    assert 'data-settings-form' in view_source
    assert 'data-routing-settings-form' in view_source
    assert 'data-cross-phase-failover' in view_source
    assert '<div class="settings-switch cross-phase-switch"><span>' in view_source
    assert 'data-cross-phase-failover aria-label=' in view_source
    assert '.cross-phase-switch { justify-content: space-between;' in styles
    assert 'data-route-model' in view_source
    assert 'data-route-effort' in view_source
    assert '${explicit ? "" : "disabled"} aria-label="${t(routePhaseLabel(phase))} ${t(routeSlotLabel(slot))} ${t("推理强度")}"' in view_source
    assert '${shown ? "" : "disabled"} aria-label="${t("推理强度")}"' not in view_source
    assert 'data-model-catalog-form="connection"' in view_source
    assert 'data-model-catalog-form="model"' in view_source
    assert 'data-interface-settings-form' in view_source
    assert 'value="system"' in view_source
    assert 'type="password"' in view_source
    assert 'autocomplete="new-password"' in view_source
    assert 'data-provider-key-input' in view_source
    assert 'closest<HTMLElement>("[data-provider-key-action]")' in events_source
    assert 'data-context-settings-form' in view_source
    assert "localStorage" not in i18n_source
    assert '"/api/settings/model-catalog"' in host_source
    assert 'payload = self._json_object({"connection_id", "action", "key", "revision"})' in host_source
    assert 'seed_gui_settings.v3' in host_source
    assert ".settings-grid" in styles
    assert ".global-settings-overlay" in styles
    assert ".route-matrix" in styles
    assert ".catalog-editor" in styles
    assert "aboutDiagnosticText" in view_source
    assert 'src="./assets/upsp-logo.png"' in view_source
    assert '"/api/about"' in host_source


def test_spec702_onboarding_frontend_gates_runtime_until_persona_is_ready():
    app_source = (GUI_ROOT / "src" / "app.ts").read_text(encoding="utf-8")
    bootstrap_source = (GUI_ROOT / "src" / "bootstrap.ts").read_text(encoding="utf-8")
    contracts_source = (GUI_ROOT / "src" / "contracts.ts").read_text(encoding="utf-8")
    state_source = (GUI_ROOT / "src" / "state.ts").read_text(encoding="utf-8")
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    styles = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    host_source = SERVER_PATH.read_text(encoding="utf-8")

    assert 'id="bootstrapRoot"' in index_source
    assert 'id="appShell" hidden' in index_source
    assert 'await Promise.all([' in app_source
    assert "pollSettings({ force: true })" in app_source
    assert "pollBootstrapStatus()" in app_source
    assert "if (bootstrapReady())" in app_source
    assert "startRuntimeUi();" in app_source
    assert 'window.setInterval(() => void pollBootstrapStatus(), 1500)' in app_source
    assert "const wasReady = bootstrapProjection.data?.persona.ready;" in bootstrap_source
    assert "if (wasReady === false && payload.persona.ready)" in bootstrap_source
    assert "window.location.reload();" in bootstrap_source
    assert "function profileStarted(draft: BootstrapDraft): boolean" in bootstrap_source
    assert "profileProblem && profileStarted(bootstrapProjection.draft)" in bootstrap_source
    assert 'aria-label="${t("社会定位")} ${index + 1}"' in bootstrap_source
    assert 'aria-label="${t("三项特点")} ${index + 1}"' in bootstrap_source
    assert "function axisBounds(values: Record<AxisKey, number>, key: AxisKey): [number, number]" in bootstrap_source
    assert "draft.axes[key] = Math.max(minimum, Math.min(maximum, Number(input.value)))" in bootstrap_source
    assert "input.value = String(draft.axes[key])" in bootstrap_source
    assert 'schema_version: "seed_gui_bootstrap_status.v1"' in contracts_source
    assert "bootstrapProjection" in state_source
    assert '"./api/bootstrap/status"' in bootstrap_source
    assert '"./api/bootstrap/provider-test"' in bootstrap_source
    assert '"./api/bootstrap/persona"' in bootstrap_source
    assert 't("测试起手主模型（将产生一次付费请求）")' in bootstrap_source
    assert "data-bootstrap-skip-model" in bootstrap_source
    assert "skip_model_setup: skipped" in bootstrap_source
    assert "skipModelSetup" in contracts_source
    assert 'id="configureModelButton"' in index_source
    assert '"skip_model_setup"' in host_source
    assert "window.confirm" not in bootstrap_source
    assert "localStorage" not in bootstrap_source
    assert ".bootstrap-root {" in styles
    assert ".global-settings-overlay { position: fixed; inset: 0; z-index: 140;" in styles
    assert "z-index: 120;" in styles
    assert "@media (max-width: 760px)" in styles
    assert '"/api/bootstrap/status"' in host_source
    assert '"/api/bootstrap/provider-test"' in host_source
    assert '"/api/bootstrap/persona"' in host_source
    assert '"persona_initialization_required"' in host_source


def test_spec701_persona_projection_frontend_contract():
    index_source = (GUI_ROOT / "index.html").read_text(encoding="utf-8")
    contracts = (GUI_ROOT / "src" / "contracts.ts").read_text(encoding="utf-8")
    state_source = (GUI_ROOT / "src" / "state.ts").read_text(encoding="utf-8")
    runtime_source = (GUI_ROOT / "src" / "runtime.ts").read_text(encoding="utf-8")
    events_source = (GUI_ROOT / "src" / "events.ts").read_text(encoding="utf-8")
    view_source = (GUI_ROOT / "src" / "view.ts").read_text(encoding="utf-8")
    styles = (GUI_ROOT / "styles.css").read_text(encoding="utf-8")
    host_source = SERVER_PATH.read_text(encoding="utf-8")
    store_source = (REPO_ROOT / "UPSP" / "OS" / "data" / "state_store.py").read_text(encoding="utf-8")

    persona_tabs = state_source.split("persona: [", 1)[1].split("],", 1)[0]
    assert 'id: "core", label: "核心档案"' in persona_tabs
    assert 'id: "state", label: "生命状态"' in persona_tabs
    assert all(old not in persona_tabs for old in (
        'id: "overview"', 'id: "body"', 'id: "protocol"',
        'label: "核心总览"', 'label: "体界系统"', 'label: "权限协议"',
    ))
    assert 'data-page="persona" data-tab="core"' in index_source
    assert 'data-i18n-aria-label="打开位格主体"' in index_source
    assert "打开位格主体核心总览" not in index_source
    assert 'schema_version: "seed_gui_persona_core.v1"' in contracts
    assert 'schema_version: "seed_gui_persona_state.v1"' in contracts
    assert 'fetchRuntimeJson<PersonaCorePayload>("./api/persona/core")' in runtime_source
    assert 'fetchRuntimeJson<PersonaStatePayload>("./api/persona/state")' in runtime_source
    assert 'window.setInterval(pollPersonaState, 6000)' in (GUI_ROOT / "src" / "app.ts").read_text(encoding="utf-8")
    assert "function personaCoreAxes" in view_source
    assert 'class="persona-id-card"' in view_source
    assert 'class="persona-id-fields"' in view_source
    assert 'class="persona-role-list"' in view_source
    assert "function personaRoleLabel" in view_source
    assert 'state.locale === "en-US" ? match[2] : match[1]' in view_source
    assert 'class="persona-core-axes"' in view_source
    assert 'renderMarkdownDocument("persona:core:source"' in view_source
    assert 'class="persona-state-all persona-core-source"' in view_source
    assert "renderStructuredJson(value)" in view_source
    assert 'data-persona-state-group' in view_source
    assert 'base.dynamic_axes.valence.value' in view_source
    assert 'projection.state?.dynamic_descriptions[axis] || t("未投影")' in view_source
    assert '<code>${escapeHtml(path)}</code>' not in view_source
    assert '<small>${escapeHtml(caption)}</small>' not in view_source
    assert 'if (pageId === "persona") void pollPersonaProjection();' in events_source
    assert 'pollPersonaCore({ force: true })' in events_source
    assert 'pollPersonaState({ force: true, ignoreVisibility: true })' in events_source
    persona_renderer = view_source.split("function renderPersonaCore", 1)[1].split("function renderMemoryPage", 1)[0]
    assert "3 MOCK" not in persona_renderer
    assert "静态设计页｜尚未接入运行时" not in persona_renderer
    assert "零号广播员" not in persona_renderer
    assert "persona-core-quick" not in persona_renderer
    assert 't("规范版本")' not in persona_renderer
    assert 't("当前模型戳")' not in persona_renderer
    assert 'path == "/api/persona/core"' in host_source
    assert 'path == "/api/persona/state"' in host_source
    assert "MAX_PERSONA_CORE_BYTES = 1024 * 1024" in host_source
    assert "def read_snapshot(self):" in store_source
    read_snapshot = store_source.split("def read_snapshot(self):", 1)[1].split("def save(self", 1)[0]
    assert "self.save" not in read_snapshot
    assert ".persona-id-card" in styles
    assert ".persona-id-fields" in styles
    assert "list-style: decimal-leading-zero" in styles
    assert ".persona-core-axes" in styles
    assert ".persona-core-axis-track" in styles
    assert ".persona-core-source" in styles
    assert ".persona-state-table" in styles
