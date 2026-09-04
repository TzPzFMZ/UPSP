import json
import sys
from pathlib import Path

import pytest

from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path


REPO_ROOT = Path(__file__).resolve().parents[3]
OS_ROOT = REPO_ROOT / "UPSP" / "OS"


def _load_config_store():
    if str(OS_ROOT) not in sys.path:
        sys.path.insert(0, str(OS_ROOT))
    return _load_module_from_path(
        "config_store_spec700",
        OS_ROOT / "data" / "config_store.py",
        register=True,
    )


def _catalog(module, *, duplicate=False):
    connections = [{
        "id": "conn_a",
        "alias": "连接 A",
        "protocol": "openai_chat",
        "url": "https://a.example/v1/chat/completions",
        "api_key_env": "UPSP_A_KEY",
        "api_key": "",
    }, {
        "id": "conn_b",
        "alias": "连接 B",
        "protocol": "openai_chat",
        "url": "https://a.example/v1/chat/completions" if duplicate else "https://b.example/v1/chat/completions",
        "api_key_env": "UPSP_A_KEY" if duplicate else "UPSP_B_KEY",
        "api_key": "",
    }, {
        "id": "conn_c",
        "alias": "连接 C",
        "protocol": "openai_responses",
        "url": "https://c.example/v1/responses",
        "api_key_env": "UPSP_C_KEY",
        "api_key": "",
    }]
    models = []
    for suffix in ("a", "b", "c"):
        models.append({
            "id": f"model_{suffix}",
            "alias": f"模型 {suffix.upper()}",
            "connection_id": f"conn_{suffix}",
            "model": "same-model" if duplicate and suffix in {"a", "b"} else f"model-{suffix}",
            "context_window": 100000,
            "reasoning": {"supported": ["low", "medium", "high"], "default": "medium"},
            "streaming": {"enabled": True, "protocol": "openai_sse", "include_usage": True},
            "prompt_cache": {"profile": "off"},
            "request_overrides": {},
        })
    result = module.default_models_config()
    result["connections"] = connections
    result["models"] = models
    return result


def _isolated_store(module, monkeypatch, tmp_path, *, catalog=None, routing=None):
    paths = {
        "models": tmp_path / "models.json",
        "model_routing": tmp_path / "model_routing.json",
        "interface": tmp_path / "interface.json",
    }
    paths["models"].write_text(
        json.dumps(catalog or _catalog(module), ensure_ascii=False), encoding="utf-8"
    )
    paths["model_routing"].write_text(
        json.dumps(routing or module.default_model_routing_config(), ensure_ascii=False),
        encoding="utf-8",
    )
    paths["interface"].write_text(
        json.dumps(module.default_interface_config(), ensure_ascii=False), encoding="utf-8"
    )
    monkeypatch.setattr(module, "_CONFIG_MAP", {
        "models": (str(paths["models"]), module.default_models_config),
        "model_routing": (str(paths["model_routing"]), module.default_model_routing_config),
        "interface": (str(paths["interface"]), module.default_interface_config),
    })
    return module.ConfigStore(use_api_environment=False), paths


def test_spec723_context_detection_preserves_legacy_and_caps_runtime_limit():
    module = _load_config_store()
    legacy = _catalog(module)

    module.ConfigStore._validate("models", legacy)

    detected = _catalog(module)
    detected["models"][0].update({
        "detected_context_window": 200000,
        "context_window_source": "provider",
        "context_window_checked_at": "2026-08-06T00:00:00+00:00",
    })
    detected = module.ConfigStore._normalise_models(detected)
    assert "context_window_checked_at" not in detected["models"][0]
    module.ConfigStore._validate("models", detected)
    detected["models"][0]["context_window"] = 200001

    with pytest.raises(ValueError, match="exceeds detected capacity"):
        module.ConfigStore._validate("models", detected)


def test_spec700_api_override_is_process_only_and_normalized(monkeypatch, tmp_path):
    module = _load_config_store()
    store, _paths = _isolated_store(module, monkeypatch, tmp_path)
    override = {
        "endpoints": {"primary": {
            "url": "https://override.example/v1/chat/completions",
            "model": "override-model",
            "api_key_env": "UPSP_OVERRIDE_KEY",
        }},
        "step_tiers": {"reaction": "primary"},
    }
    monkeypatch.setenv(module.API_CONFIG_OVERRIDE_ENV, json.dumps(override))

    loaded = module.ConfigStore().load("api")
    assert loaded["environment_override"] is True
    assert loaded["endpoints"]["primary"]["profile_id"] == "override:primary"
    assert loaded["step_routes"]["reaction"][0] == "primary"
    assert store.load("models")["models"][0]["model"] == "model-a"


def test_private_memory_controls_default_to_disabled(monkeypatch, tmp_path):
    module = _load_config_store()
    from errors import ReadError
    monkeypatch.setattr(module, "_CONFIG_MAP", {
        "memory": (str(tmp_path / "missing-memory.json"), module.default_memory_config),
    })
    with pytest.raises(ReadError):
        module.ConfigStore().get_memory_privacy_declassify_config()


def test_spec700_config_store_writes_canonical_file_and_revision(monkeypatch, tmp_path):
    module = _load_config_store()
    path = tmp_path / "system.json"
    path.write_text(json.dumps(module.default_system_config()), encoding="utf-8")
    monkeypatch.setattr(module, "_CONFIG_MAP", {
        "system": (str(path), module.default_system_config),
    })
    store = module.ConfigStore()
    before = store.revision("system")
    payload = store.load("system")
    payload["heartbeat"]["interval"] = 7
    payload["rhythm"]["period"] = 41
    store.save("system", payload)
    assert store.revision("system") != before
    assert store.get_rhythm_interval() == 41
    assert store.get_heartbeat_interval() == 7
    assert json.loads(path.read_text(encoding="utf-8"))["heartbeat"]["interval"] == 7


def test_spec700_vertical_inheritance_explicit_backups_and_cross_phase_order(monkeypatch, tmp_path):
    module = _load_config_store()
    routing = module.default_model_routing_config()
    routing["routes"]["setup"]["primary"] = {"model_id": "model_a", "reasoning_effort": "high"}
    routing["routes"]["reaction"]["backups"][0] = {"model_id": "model_b", "reasoning_effort": "low"}
    routing["routes"]["cleanup"]["primary"] = {"model_id": "model_c", "reasoning_effort": "medium"}
    store, _paths = _isolated_store(module, monkeypatch, tmp_path, routing=routing)

    resolved = store.resolve_model_routes()
    assert resolved["primary_sources"] == {
        "setup": "setup", "reaction": "setup", "cleanup": "cleanup",
    }
    assert [(item["model_id"], item["reasoning_effort"]) for item in resolved["phases"]["reaction"]] == [
        ("model_a", "high"), ("model_b", "low"), ("model_c", "medium"),
    ]
    assert [item["model_id"] for item in resolved["phases"]["setup"]] == ["model_a", "model_c"]
    routing["cross_phase_failover_enabled"] = False
    store.save("model_routing", routing)
    assert [item["model_id"] for item in store.resolve_model_routes()["phases"]["reaction"]] == [
        "model_a", "model_b",
    ]


def test_spec700_route_deduplicates_actual_url_model_key_fingerprint(monkeypatch, tmp_path):
    module = _load_config_store()
    routing = module.default_model_routing_config()
    routing["routes"]["setup"]["primary"] = {"model_id": "model_a", "reasoning_effort": "medium"}
    routing["routes"]["setup"]["backups"] = [
        {"model_id": "model_b", "reasoning_effort": "medium"},
        {"model_id": "model_c", "reasoning_effort": "medium"},
    ]
    store, _paths = _isolated_store(
        module, monkeypatch, tmp_path, catalog=_catalog(module, duplicate=True), routing=routing
    )
    assert [item["model_id"] for item in store.resolve_model_routes()["phases"]["setup"]] == [
        "model_a", "model_c",
    ]


def test_spec700_route_effort_and_compatibility_secrets_fail_closed(monkeypatch, tmp_path):
    module = _load_config_store()
    routing = module.default_model_routing_config()
    routing["routes"]["setup"]["primary"] = {
        "model_id": "model_a",
        "reasoning_effort": "ultra",
    }
    store, _paths = _isolated_store(module, monkeypatch, tmp_path, routing=routing)
    with pytest.raises(ValueError, match="unsupported reasoning effort"):
        store.resolve_model_routes()

    catalog = store.load("models")
    catalog["models"][0]["request_overrides"] = {
        "headers": {"Authorization": "Bearer must-not-leak"},
    }
    with pytest.raises(ValueError, match="must not contain secrets"):
        store.save("models", catalog)


def test_spec700_legacy_api_migrates_once_and_malformed_new_file_fails_closed(monkeypatch, tmp_path):
    module = _load_config_store()
    from schemas.config import legacy_default_api_config

    legacy_path = tmp_path / "api.json"
    models_path = tmp_path / "models.json"
    routing_path = tmp_path / "model_routing.json"
    interface_path = tmp_path / "interface.json"
    legacy = legacy_default_api_config()
    for endpoint in legacy["endpoints"].values():
        endpoint.update({
            "url": "https://api.tian-shu.org/v1/chat/completions",
            "model": "gpt-5.6-terra",
            "api_key_env": "UPSP_TIANSHU_API_KEY",
            "reasoning_effort": "medium",
            "context_window": 1000000,
        })
    legacy_path.write_text(json.dumps(legacy, ensure_ascii=False), encoding="utf-8")
    routing_path.write_text(
        json.dumps(module.default_model_routing_config(), ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(module, "LEGACY_CONFIG_API", str(legacy_path))
    monkeypatch.setattr(module, "GLOBAL_MODELS_CONFIG", str(models_path))
    monkeypatch.setattr(module, "CONFIG_MODEL_ROUTING", str(routing_path))
    monkeypatch.setattr(module, "_CONFIG_MAP", {
        "models": (str(models_path), module.default_models_config),
        "model_routing": (str(routing_path), module.default_model_routing_config),
        "interface": (str(interface_path), module.default_interface_config),
    })
    store = module.ConfigStore(use_api_environment=False)
    assert set(store.init_all()) == {"models", "interface"}
    assert len(store.load("models")["connections"]) == 1
    assert len(store.load("models")["models"]) == 1
    assert store.load("model_routing")["routes"]["setup"]["primary"] is None
    assert store.load("model_routing")["routes"]["reaction"]["primary"] is None

    models_path.write_text("{broken", encoding="utf-8")
    with pytest.raises(Exception):
        store.init_all()
    assert models_path.read_text(encoding="utf-8") == "{broken"
