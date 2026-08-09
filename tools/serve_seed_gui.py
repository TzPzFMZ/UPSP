#!/usr/bin/env python
"""Serve the canonical Seed GUI and its local Runtime bridge."""
from __future__ import annotations

import argparse
from contextlib import nullcontext, redirect_stdout
from copy import deepcopy
from datetime import datetime, timezone
import hmac
import json
import os
import platform
import re
import sys
import threading
import uuid
import webbrowser
from http.server import ThreadingHTTPServer
from pathlib import Path
from urllib import request as urllib_request
from urllib.parse import parse_qs, quote, unquote, urlparse, urlsplit

REPO_ROOT = Path(__file__).resolve().parents[1]
TOOLS_ROOT = REPO_ROOT / "tools"
UPSP_ROOT = REPO_ROOT / "UPSP"
PROGRAM_OS_ROOT = REPO_ROOT / "UPSP" / "OS"
if str(TOOLS_ROOT) not in sys.path:
    sys.path.insert(0, str(TOOLS_ROOT))
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))
if str(UPSP_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSP_ROOT))

from initialization.windows_data import ensure_active_instance  # noqa: E402

ensure_active_instance(UPSP_ROOT)

from serve_round_live import RoundLiveHandler, default_round_dir  # noqa: E402
from initialization.bootstrap_service import BootstrapService  # noqa: E402
from initialization.persona_initializer import PersonaInitializationError  # noqa: E402
from assembly.statusbar import StatusBarBuilder  # noqa: E402
from data.container_store import ContainerStore  # noqa: E402
from data.config_store import API_CONFIG_OVERRIDE_ENV, ConfigStore  # noqa: E402
from data.memory_store import MemoryStore, project_memory_body  # noqa: E402
from data.relation_store import AXIS_NAMES, RelationStore  # noqa: E402
from data.request_prefix_diff import build_request_prefix_diff  # noqa: E402
from data.state_store import StateStore  # noqa: E402
from data.workbench import WorkbenchStore  # noqa: E402
from engines.resident_runtime import (  # noqa: E402
    ResidentRuntimeService,
    RuntimeAlreadyRunning,
    RuntimeServiceError,
    RuntimeSupervisorCorrupt,
)
from engines.tool_approval import ToolApprovalConflict  # noqa: E402
from logic.container_focus import apply_container_focus_declarations  # noqa: E402
from errors import APIBridgeError, ReadError, WriteError  # noqa: E402
from paths import DOCS_DIR, PERSONA_DIR, RULES_DIR  # noqa: E402
from schemas.state import FIELDS as STATE_FIELDS  # noqa: E402

GUI_ROOT = REPO_ROOT / "UPSP" / "gui"
PRODUCT_MANIFEST_PATH = REPO_ROOT / "UPSP" / "product.json"
BUILD_INFO_PATH = REPO_ROOT / "metadata" / "build-info.json"
MAX_REQUEST_BYTES = 1024 * 1024
MAX_DEPOSITION_CONTENT_CHARS = 64 * 1024
MAX_PROTOCOL_DOCUMENT_BYTES = 1024 * 1024
MAX_PERSONA_CORE_BYTES = 1024 * 1024
MAX_MODEL_METADATA_BYTES = 256 * 1024
MODEL_CONTEXT_WINDOW_REGISTRY_PATH = (
    PROGRAM_OS_ROOT / "data" / "model_context_windows.json"
)
DESKTOP_CONTROL_TOKEN_ENV = "UPSP_DESKTOP_CONTROL_TOKEN"
DESKTOP_SESSION_ID_ENV = "UPSP_DESKTOP_SESSION_ID"
DESKTOP_CONTROL_HEADER = "X-UPSP-Desktop-Control"
DESKTOP_TOKEN_RE = re.compile(r"^[0-9a-f]{64}$")
DESKTOP_SESSION_RE = re.compile(r"^[0-9a-f]{32}$")
PERSONA_ROOT = Path(PERSONA_DIR)
PERSONA_CORE_MD = PERSONA_ROOT / "core.md"
RULES_ROOT = Path(RULES_DIR)
DOCS_ROOT = Path(DOCS_DIR)
TASK_DONE_STATUSES = {
    "accepted", "applied", "complete", "completed", "done", "finish", "finished",
}
TASK_ACCEPTED_STATUSES = TASK_DONE_STATUSES | {"passed", "verified"}
TASK_SETTLED_INPUT_STATUSES = {
    "integrated", "deferred", "rejected", "split", "completed", "done",
}
CLIENT_DISCONNECT_ERRORS = (
    BrokenPipeError,
    ConnectionResetError,
    ConnectionAbortedError,
)


def _load_product_manifest() -> dict:
    try:
        value = json.loads(PRODUCT_MANIFEST_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeError("product_manifest_invalid") from exc
    required = {
        "schema_version", "name", "version", "windows_file_version", "channel",
        "build_number", "author", "repository_url", "releases_url", "license",
        "copyright",
    }
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != "upsp_product_manifest.v1"
        or set(value) != required
        or not all(isinstance(value.get(key), str) and value[key] for key in (
            "name", "version", "windows_file_version", "channel",
            "repository_url", "releases_url", "license", "copyright",
        ))
        or not isinstance(value.get("build_number"), int)
        or value["build_number"] < 1
        or not isinstance(value.get("author"), dict)
        or set(value["author"]) != {"zh-CN", "en-US"}
        or not all(isinstance(item, str) and item for item in value["author"].values())
        or re.fullmatch(r"\d+\.\d+\.\d+-[a-z]+(?:\.\d+)+", value["version"]) is None
        or re.fullmatch(r"\d+\.\d+\.\d+\.\d+", value["windows_file_version"]) is None
        or value["channel"] != "alpha"
        or not value["repository_url"].startswith("https://")
        or not value["releases_url"].startswith("https://")
    ):
        raise RuntimeError("product_manifest_invalid")
    return value


PRODUCT = _load_product_manifest()
PRODUCT_VERSION = PRODUCT["version"]


def _build_identity() -> dict:
    if BUILD_INFO_PATH.is_file():
        try:
            value = json.loads(BUILD_INFO_PATH.read_text(encoding="utf-8"))
            return {
                "git_head": str(value["git_head"]),
                "source_dirty": bool(value["source_dirty"]),
                "architecture": str(value["architecture"]),
                "signature_status": str(value.get("signature_status", "unsigned")),
            }
        except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
            pass
    return {
        "git_head": "unavailable",
        "source_dirty": True,
        "architecture": "win-x64" if platform.machine().lower() in {"amd64", "x86_64"} else platform.machine(),
        "signature_status": "unsigned",
    }


ABOUT_PROJECTION = {
    "schema_version": "seed_gui_about.v1",
    "product": {
        "name": PRODUCT["name"],
        "version": PRODUCT["version"],
        "channel": PRODUCT["channel"],
        "build_number": PRODUCT["build_number"],
        "author": PRODUCT["author"],
        "license": PRODUCT["license"],
        "copyright": PRODUCT["copyright"],
    },
    "links": {
        "repository": PRODUCT["repository_url"],
        "releases": PRODUCT["releases_url"],
    },
    "build": _build_identity(),
    "data_policy": {
        "persona_location": r"Documents\UPSP",
        "local_state_location": r"LocalAppData\UPSP",
        "uninstall_preserves_user_data": True,
        "key_storage": "ignored_json_or_environment",
    },
}

SETTINGS_FILE_IDS = (
    "system", "memory", "now", "lately", "periodic", "high_freq", "relation",
)
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
MISSING = object()


class SettingsValidationError(ValueError):
    pass


class SettingsConflictError(ValueError):
    pass


class SettingsNotFoundError(ValueError):
    pass


def _setting(path, kind, minimum=None, maximum=None, choices=()):
    return {
        "path": tuple(path.split(".")),
        "kind": kind,
        "minimum": minimum,
        "maximum": maximum,
        "choices": tuple(choices),
    }


SETTINGS_FIELDS = {
    "system": {
        "heartbeat.interval": _setting("heartbeat.interval", "int", 1, 3600),
        "round.time_limit": _setting("round.time_limit", "int", 60, 86400),
        "rhythm.period": _setting("rhythm.period", "int", 1, 100000),
        "standby.idle_threshold_min": _setting("standby.idle_threshold_min", "int", 1, 10080),
        "token_usage.warning_ratio": _setting("token_usage.warning_ratio", "float", 0.01, 1.0),
        "audit.round_snapshot_retention": _setting("audit.round_snapshot_retention", "int", 1, 4096),
        "audit.state_backup_retention": _setting("audit.state_backup_retention", "int", 1, 4096),
        "autonomous_trigger.tacit_pending_threshold": _setting("autonomous_trigger.tacit_pending_threshold", "int", 1, 1000000),
        "autonomous_trigger.connection_pending_threshold": _setting("autonomous_trigger.connection_pending_threshold", "int", 1, 1000000),
        "general_tools.file_read_window_chars": _setting("general_tools.file_read_window_chars", "int", 1, 16777216),
        "general_tools.web_fetch_window_chars": _setting("general_tools.web_fetch_window_chars", "int", 1, 16777216),
        "general_tools.web_search_window_results": _setting("general_tools.web_search_window_results", "int", 1, 1000),
    },
    "now": {
        "budget_chars": _setting("budget_chars", "int", 1, 16777216),
        "trim_chars": _setting("trim_chars", "int", 1, 16777216),
    },
    "lately": {
        "budget_chars": _setting("budget_chars", "int", 1, 16777216),
        "trim_chars": _setting("trim_chars", "int", 1, 16777216),
        "compact_ratio": _setting("compact_ratio", "float", 0.0, 1.0),
        "compact_shard_chars": _setting("compact_shard_chars", "int", 1, 16777216),
        "compact_shard_ratio": _setting("compact_shard_ratio", "float", 0.0, 1.0),
    },
    "periodic": {
        "limits.periodic_memory_items_chars": _setting("limits.periodic_memory_items_chars", "int", 1, 16777216),
    },
    "high_freq": {
        "content_limits.reference_window_chars": _setting("content_limits.reference_window_chars", "int", 1, 16777216),
    },
    "relation": {
        "relation_focus.max_slots": _setting("relation_focus.max_slots", "int", 1, 32),
    },
    "memory": {
        "heat.zone_thresholds.significant": _setting("heat.zone_thresholds.significant", "int", 1, 100),
        "heat.zone_thresholds.uncertain": _setting("heat.zone_thresholds.uncertain", "int", 0, 99),
        "heat.decay_rates.significant": _setting("heat.decay_rates.significant", "int", -100, 0),
        "heat.decay_rates.uncertain": _setting("heat.decay_rates.uncertain", "int", -100, 0),
        "heat.decay_rates.decay": _setting("heat.decay_rates.decay", "int", -100, 0),
        "heat.initial_by_weight.1": _setting("heat.initial_by_weight.1", "int", 0, 100),
        "heat.initial_by_weight.2": _setting("heat.initial_by_weight.2", "int", 0, 100),
        "heat.initial_by_weight.3": _setting("heat.initial_by_weight.3", "int", 0, 100),
        "heat.initial_by_weight.4": _setting("heat.initial_by_weight.4", "int", 0, 100),
        "heat.initial_by_weight.5": _setting("heat.initial_by_weight.5", "int", 0, 100),
        "heat.recall_boost": _setting("heat.recall_boost", "int", 0, 100),
        "heat.upgrade_high_rounds": _setting("heat.upgrade_high_rounds", "int", 1, 100000),
        "heat.locked_value": _setting("heat.locked_value", "int", 0, 100),
    },
}

for _name in (
    "container_index", "ltm_heat_index", "stm_heat_index", "skills_inverted",
    "relation_inverted", "relation_domain", "ltm_inverted", "stm_inverted",
    "association_index",
):
    _path = f"index_display_limits.{_name}"
    SETTINGS_FIELDS["high_freq"][_path] = _setting(_path, "int", 1, 1000)


class SettingsService:
    """Safe GUI boundary over global settings and the current persona config."""

    def __init__(self, config_store=None):
        self.configs = config_store or ConfigStore(use_api_environment=False)
        self.configs.init_all()

    @staticmethod
    def _get(config: dict, path: tuple[str, ...]):
        value = config
        for key in path:
            if not isinstance(value, dict) or key not in value:
                return MISSING
            value = value[key]
        return value

    @staticmethod
    def _set(config: dict, path: tuple[str, ...], value: object) -> None:
        target = config
        for key in path[:-1]:
            child = target.get(key)
            if not isinstance(child, dict):
                raise SettingsValidationError("settings_config_incomplete")
            target = child
        if path[-1] not in target:
            raise SettingsValidationError("settings_config_incomplete")
        target[path[-1]] = value

    @staticmethod
    def _normalize(spec: dict, value: object):
        kind = spec["kind"]
        if kind == "bool":
            if not isinstance(value, bool):
                raise SettingsValidationError("settings_value_not_bool")
            result = value
        elif kind == "int":
            if isinstance(value, bool) or not isinstance(value, int):
                raise SettingsValidationError("settings_value_not_int")
            result = value
        elif kind == "float":
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                raise SettingsValidationError("settings_value_not_number")
            result = float(value)
        elif kind in {"string", "enum"}:
            if not isinstance(value, str):
                raise SettingsValidationError("settings_value_not_string")
            result = value.strip()
        else:
            raise SettingsValidationError("settings_field_kind_unknown")
        if spec["choices"] and result not in spec["choices"]:
            raise SettingsValidationError("settings_value_not_allowed")
        if spec["minimum"] is not None and result < spec["minimum"]:
            raise SettingsValidationError("settings_value_too_small")
        if spec["maximum"] is not None:
            comparable = len(result) if isinstance(result, str) else result
            if comparable > spec["maximum"]:
                raise SettingsValidationError("settings_value_too_large")
        return result

    @staticmethod
    def _validate_document(file_id: str, config: dict) -> None:
        if file_id in {"now", "lately"}:
            if config.get("trim_chars") > config.get("budget_chars"):
                raise SettingsValidationError(f"settings_{file_id}_trim_exceeds_budget")
        if file_id == "memory":
            heat = config["heat"]
            thresholds = heat["zone_thresholds"]
            initial = list(heat["initial_by_weight"].values())
            if (
                thresholds["uncertain"] >= thresholds["significant"]
                or initial != sorted(initial)
                or not thresholds["uncertain"] <= initial[0] <= initial[-1] <= 100
            ):
                raise SettingsValidationError("settings_memory_heat_invalid")

    def _values(self, file_id: str, config: dict) -> dict:
        result = {}
        for key, spec in SETTINGS_FIELDS[file_id].items():
            value = self._get(config, spec["path"])
            if value is MISSING:
                raise SettingsValidationError("settings_config_incomplete")
            result[key] = self._normalize(spec, value)
        return result

    @staticmethod
    def _key_source(connection: object) -> str:
        if not isinstance(connection, dict):
            return "missing"
        env_name = _text(connection.get("api_key_env")).strip()
        if env_name and os.environ.get(env_name):
            return "env"
        if _text(connection.get("api_key")).strip():
            return "config"
        return "missing"

    @classmethod
    def _provider_context_window(
            cls, connection: dict, model: str, timeout: int) -> tuple[int, str] | None:
        env_name = _text(connection.get("api_key_env")).strip()
        key = _text(
            (os.environ.get(env_name) if env_name else "") or connection.get("api_key")
        ).strip()
        if connection.get("protocol") != "anthropic_messages" or not key:
            return None
        base_url = _text(connection.get("url")).strip().rstrip("/")
        if not base_url.endswith("/v1/messages"):
            return None
        base_url = base_url.removesuffix("/v1/messages")
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None
        exact_url = f"{base_url}/v1/models/{quote(model, safe='')}"
        try:
            request = urllib_request.Request(exact_url, headers={
                "Accept": "application/json",
                "User-Agent": "UPSP-Base/2.0",
                "x-api-key": key,
                "anthropic-version": "2023-06-01",
            })
            with urllib_request.urlopen(request, timeout=timeout) as response:
                body = response.read(MAX_MODEL_METADATA_BYTES + 1)
            if len(body) > MAX_MODEL_METADATA_BYTES:
                return None
            payload = json.loads(body.decode("utf-8"))
        except Exception:
            return None
        if not isinstance(payload, dict):
            return None
        capacity = payload.get("max_input_tokens")
        if (
                payload.get("id") != model
                or isinstance(capacity, bool)
                or not isinstance(capacity, int)
                or not 0 < capacity <= 100000000):
            return None
        return capacity, exact_url

    @staticmethod
    def _registry_context_window(model: str) -> tuple[int, str] | None:
        try:
            registry = json.loads(MODEL_CONTEXT_WINDOW_REGISTRY_PATH.read_text(encoding="utf-8"))
            if (
                    not isinstance(registry, dict)
                    or registry.get("schema_version") != "model_context_window_registry.v1"):
                return None
            item = (registry.get("models") or {}).get(model)
            if not isinstance(item, dict):
                return None
            capacity = item.get("context_window")
            source_ref = _text(item.get("source_url")).strip()
            if (
                isinstance(capacity, bool)
                or not isinstance(capacity, int)
                or not 0 < capacity <= 100000000
                or not source_ref.startswith("https://")
            ):
                return None
            return capacity, source_ref
        except (OSError, TypeError, ValueError, json.JSONDecodeError):
            return None

    def resolve_model_context_window(
            self, connection_id: object, model: object) -> dict:
        connection_id = _text(connection_id).strip()
        model = _text(model).strip()
        if not connection_id or len(connection_id) > 160 or not model or len(model) > 512:
            raise SettingsValidationError("model_context_resolution_invalid")
        catalog = self.configs.load("models")
        connection = next(
            (
                item for item in catalog.get("connections", [])
                if item.get("id") == connection_id
            ),
            None,
        )
        if connection is None:
            raise SettingsNotFoundError("model_connection_not_found")
        handshake = (catalog.get("transport") or {}).get("handshake") or {}
        try:
            timeout = max(1, min(5, int(handshake.get("timeout_seconds") or 5)))
        except (TypeError, ValueError, OverflowError):
            timeout = 5
        provider = self._provider_context_window(
            connection, model, timeout
        )
        if provider is not None:
            capacity, source_ref = provider
            return {
                "schema_version": "seed_gui_model_context_resolution.v1",
                "model": model,
                "detected_context_window": capacity,
                "source": "provider",
                "source_ref": source_ref,
            }
        registry = self._registry_context_window(model)
        if registry is not None:
            capacity, source_ref = registry
            return {
                "schema_version": "seed_gui_model_context_resolution.v1",
                "model": model,
                "detected_context_window": capacity,
                "source": "registry",
                "source_ref": source_ref,
            }
        return {
            "schema_version": "seed_gui_model_context_resolution.v1",
            "model": model,
            "detected_context_window": None,
            "source": "unknown",
        }

    @staticmethod
    def _public_route_resolution(resolved: dict) -> dict:
        phases = {}
        for phase, items in (resolved.get("phases") or {}).items():
            phases[phase] = [
                {
                    key: item.get(key)
                    for key in (
                        "model_id", "model_alias", "connection_id",
                        "connection_alias", "reasoning_effort", "source_phase",
                        "slot", "inherited",
                    )
                }
                for item in items
            ]
        return {
            "cross_phase_failover_enabled": resolved.get("cross_phase_failover_enabled"),
            "effective_primaries": deepcopy(resolved.get("effective_primaries") or {}),
            "primary_sources": deepcopy(resolved.get("primary_sources") or {}),
            "phases": phases,
        }

    def _environment_override_setup_ready(self) -> bool:
        if not os.environ.get(API_CONFIG_OVERRIDE_ENV, "").strip():
            return False
        try:
            api = self.configs.load("api")
        except (ReadError, ValueError):
            return False
        setup_route = (api.get("step_routes") or {}).get("setup") or []
        endpoint = (api.get("endpoints") or {}).get(setup_route[0]) if setup_route else None
        if not isinstance(endpoint, dict):
            return False
        key_env = _text(endpoint.get("api_key_env")).strip()
        key_present = bool(
            _text(endpoint.get("api_key")).strip()
            or (key_env and os.environ.get(key_env))
        )
        return bool(
            _text(endpoint.get("url")).strip()
            and _text(endpoint.get("model")).strip()
            and key_present
        )

    def projection(self) -> dict:
        loaded = {file_id: self.configs.load(file_id) for file_id in SETTINGS_FILE_IDS}
        files = {
            file_id: {
                "revision": self.configs.revision(file_id),
                "values": self._values(file_id, loaded[file_id]),
            }
            for file_id in SETTINGS_FILE_IDS
        }
        interface = self.configs.load("interface")
        catalog = self.configs.load("models")
        routing = self.configs.load("model_routing")
        connections = []
        key_sources = {}
        for item in catalog.get("connections", []):
            source = self._key_source(item)
            key_sources[item["id"]] = source
            public = {
                key: deepcopy(value)
                for key, value in item.items()
                if key != "api_key"
            }
            public["key_source"] = source
            public["key_present"] = source != "missing"
            connections.append(public)
        resolved = self._public_route_resolution(self.configs.resolve_model_routes())
        setup_primary = resolved["effective_primaries"].get("setup")
        setup_route = resolved["phases"].get("setup") or []
        setup_connection_id = setup_route[0].get("connection_id") if setup_route else ""
        setup_model_ready = self._environment_override_setup_ready() or bool(
            isinstance(setup_primary, dict)
            and setup_connection_id
            and setup_route[0].get("slot") == "primary"
            and setup_route[0].get("source_phase") == "setup"
            and key_sources.get(setup_connection_id) != "missing"
        )
        return {
            "schema_version": "seed_gui_settings.v3",
            "environment_override": bool(os.environ.get(API_CONFIG_OVERRIDE_ENV, "").strip()),
            "files": files,
            "interface": {
                "revision": self.configs.revision("interface"),
                "values": deepcopy(interface),
            },
            "model_catalog": {
                "revision": self.configs.revision("models"),
                "connections": connections,
                "models": deepcopy(catalog.get("models") or []),
                "transport": deepcopy(catalog.get("transport") or {}),
                "key_sources": key_sources,
            },
            "persona": {
                "model_routing": {
                    "revision": self.configs.revision("model_routing"),
                    "values": deepcopy(routing),
                },
                "effective_routes": resolved,
                "setup_model_ready": setup_model_ready,
            },
            "ready": bool(catalog.get("models")) and any(
                resolved["phases"].get(phase) for phase in ("setup", "reaction", "cleanup")
            ),
        }

    def update(self, file_id: object, changes: object, revision: object) -> None:
        if not isinstance(revision, str):
            raise SettingsValidationError("settings_update_invalid")
        if not isinstance(changes, dict) or not changes:
            raise SettingsValidationError("settings_changes_invalid")
        if file_id == "interface":
            if set(changes) != {"locale"}:
                raise SettingsValidationError("settings_fields_invalid")
            if revision != self.configs.revision("interface"):
                raise SettingsConflictError("settings_revision_conflict")
            self.configs.save("interface", {
                "schema_version": "upsp_interface_settings.v1",
                "locale": changes.get("locale"),
            })
            return
        if file_id == "model_routing":
            if not set(changes).issubset({"cross_phase_failover_enabled", "routes"}):
                raise SettingsValidationError("settings_fields_invalid")
            if revision != self.configs.revision("model_routing"):
                raise SettingsConflictError("settings_revision_conflict")
            old_config = deepcopy(self.configs.load("model_routing"))
            config = deepcopy(old_config)
            config.update(deepcopy(changes))
            try:
                self.configs.save("model_routing", config)
                self.configs.resolve_model_routes()
            except ValueError as exc:
                self.configs.save("model_routing", old_config)
                raise SettingsValidationError(str(exc)) from exc
            return
        if file_id == "models":
            if set(changes) != {"transport"} or not isinstance(changes.get("transport"), dict):
                raise SettingsValidationError("settings_fields_invalid")
            if revision != self.configs.revision("models"):
                raise SettingsConflictError("settings_revision_conflict")
            config = deepcopy(self.configs.load("models"))
            config["transport"] = deepcopy(changes["transport"])
            try:
                self.configs.save("models", config)
            except ValueError as exc:
                raise SettingsValidationError(str(exc)) from exc
            return
        if file_id not in SETTINGS_FIELDS:
            raise SettingsValidationError("settings_update_invalid")
        specs = SETTINGS_FIELDS[file_id]
        if not set(changes).issubset(specs):
            raise SettingsValidationError("settings_fields_invalid")
        if revision != self.configs.revision(file_id):
            raise SettingsConflictError("settings_revision_conflict")
        config = deepcopy(self.configs.load(file_id))
        self._values(file_id, config)
        for key, value in changes.items():
            try:
                normalized = self._normalize(specs[key], value)
            except SettingsValidationError as exc:
                raise SettingsValidationError(f"{exc}:{key}") from exc
            self._set(config, specs[key]["path"], normalized)
        self._validate_document(file_id, config)
        self.configs.save(file_id, config)

    @staticmethod
    def _connection_values(values: object, current=None) -> dict:
        if not isinstance(values, dict):
            raise SettingsValidationError("model_catalog_values_invalid")
        allowed = {"alias", "protocol", "url", "api_key_env"}
        if not set(values).issubset(allowed):
            raise SettingsValidationError("model_catalog_fields_invalid")
        result = deepcopy(current or {})
        result.update({key: _text(value).strip() for key, value in values.items()})
        if not result.get("alias") or result.get("protocol") not in {
            "openai_chat", "openai_responses", "anthropic_messages",
        }:
            raise SettingsValidationError("model_connection_invalid")
        parsed = urlsplit(result.get("url", ""))
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise SettingsValidationError("settings_endpoint_url_invalid")
        if parsed.username or parsed.password:
            raise SettingsValidationError("settings_endpoint_credentials_forbidden")
        env_name = result.get("api_key_env", "")
        if env_name and not ENV_NAME_RE.fullmatch(env_name):
            raise SettingsValidationError("settings_endpoint_env_invalid")
        return result

    @staticmethod
    def _model_values(values: object, current=None) -> dict:
        if not isinstance(values, dict):
            raise SettingsValidationError("model_catalog_values_invalid")
        allowed = {
            "alias", "model", "connection_id", "context_window",
            "detected_context_window", "context_window_source",
            "reasoning_supported", "reasoning_default", "streaming_enabled",
            "streaming_include_usage", "request_overrides",
        }
        if not set(values).issubset(allowed):
            raise SettingsValidationError("model_catalog_fields_invalid")
        result = deepcopy(current or {})
        if "alias" in values:
            result["alias"] = _text(values["alias"]).strip()
        if "model" in values:
            result["model"] = _text(values["model"]).strip()
        if "connection_id" in values:
            result["connection_id"] = _text(values["connection_id"]).strip()
        if "context_window" in values:
            value = values["context_window"]
            if isinstance(value, bool) or not isinstance(value, int) or not 1 <= value <= 100000000:
                raise SettingsValidationError("model_context_window_invalid")
            result["context_window"] = value
        if "detected_context_window" in values:
            value = values["detected_context_window"]
            if isinstance(value, bool) or not isinstance(value, int) or not 0 <= value <= 100000000:
                raise SettingsValidationError("model_detected_context_window_invalid")
            result["detected_context_window"] = value
        if "context_window_source" in values:
            source = _text(values["context_window_source"]).strip()
            if source not in {"provider", "registry", "legacy_manual", "unknown"}:
                raise SettingsValidationError("model_context_window_source_invalid")
            result["context_window_source"] = source
        supported = values.get("reasoning_supported", MISSING)
        default = values.get("reasoning_default", MISSING)
        if supported is not MISSING or default is not MISSING:
            reasoning = deepcopy(result.get("reasoning") or {})
            if supported is not MISSING:
                if not isinstance(supported, list):
                    raise SettingsValidationError("model_reasoning_invalid")
                reasoning["supported"] = [_text(item).strip() for item in supported]
            if default is not MISSING:
                reasoning["default"] = _text(default).strip()
            result["reasoning"] = reasoning
        streaming = deepcopy(result.get("streaming") or {
            "enabled": True, "protocol": "openai_sse", "include_usage": True,
        })
        if "streaming_enabled" in values:
            if not isinstance(values["streaming_enabled"], bool):
                raise SettingsValidationError("model_streaming_invalid")
            streaming["enabled"] = values["streaming_enabled"]
        if "streaming_include_usage" in values:
            if not isinstance(values["streaming_include_usage"], bool):
                raise SettingsValidationError("model_streaming_invalid")
            streaming["include_usage"] = values["streaming_include_usage"]
        result["streaming"] = streaming
        if "request_overrides" in values:
            if not isinstance(values["request_overrides"], dict):
                raise SettingsValidationError("model_request_overrides_invalid")
            result["request_overrides"] = deepcopy(values["request_overrides"])
        result["prompt_cache"] = {"profile": "automatic_tiered"}
        result.setdefault("request_overrides", {})
        if "detected_context_window" not in result:
            result["detected_context_window"] = 0
            result["context_window_source"] = (
                "legacy_manual" if result.get("context_window") else "unknown"
            )
        if not result.get("alias") or not result.get("model") or not result.get("connection_id"):
            raise SettingsValidationError("model_profile_invalid")
        return result

    def update_model_catalog(self, entity, action, item_id, values, revision):
        if entity not in {"connection", "model"} or action not in {"create", "update", "delete"}:
            raise SettingsValidationError("model_catalog_update_invalid")
        if not isinstance(revision, str) or revision != self.configs.revision("models"):
            raise SettingsConflictError("settings_revision_conflict")
        catalog = deepcopy(self.configs.load("models"))
        old_catalog = deepcopy(catalog)
        old_routing = deepcopy(self.configs.load("model_routing"))
        new_routing = deepcopy(old_routing)
        collection_name = "connections" if entity == "connection" else "models"
        collection = catalog[collection_name]
        existing = next((item for item in collection if item.get("id") == item_id), None)
        if action == "create":
            if item_id not in {None, ""}:
                raise SettingsValidationError("model_catalog_create_id_forbidden")
            item_id = f"{'conn' if entity == 'connection' else 'model'}_{uuid.uuid4().hex[:12]}"
            item = self._connection_values(values) if entity == "connection" else self._model_values(values)
            item["id"] = item_id
            if entity == "connection":
                item["api_key"] = ""
            collection.append(item)
        elif existing is None:
            raise SettingsNotFoundError("model_catalog_item_not_found")
        elif action == "update":
            item = (
                self._connection_values(values, existing)
                if entity == "connection"
                else self._model_values(values, existing)
            )
            item["id"] = existing["id"]
            if entity == "connection":
                item["api_key"] = existing.get("api_key", "")
            collection[collection.index(existing)] = item
            if entity == "model":
                supported = (item.get("reasoning") or {}).get("supported") or []
                fallback = _text((item.get("reasoning") or {}).get("default"))
                if supported:
                    for row in (new_routing.get("routes") or {}).values():
                        slots = [row.get("primary"), *(row.get("backups") or [])]
                        for slot in slots:
                            if (
                                isinstance(slot, dict)
                                and slot.get("model_id") == item_id
                                and _text(slot.get("reasoning_effort")) not in supported
                            ):
                                slot["reasoning_effort"] = fallback
        else:
            if entity == "connection" and any(
                model.get("connection_id") == item_id for model in catalog["models"]
            ):
                raise SettingsConflictError("model_connection_in_use")
            if entity == "model":
                routing_text = json.dumps(old_routing, ensure_ascii=False)
                if f'"model_id": "{item_id}"' in routing_text:
                    raise SettingsConflictError("model_profile_in_use")
            collection.remove(existing)
        try:
            self.configs.save("models", catalog)
            if action == "create" and entity == "model":
                routes = new_routing.get("routes") or {}
                all_empty = all(
                    not row.get("primary") and not any(row.get("backups") or [])
                    for row in routes.values()
                )
                if all_empty:
                    profile = next(item for item in catalog["models"] if item["id"] == item_id)
                    routes["setup"]["primary"] = {
                        "model_id": item_id,
                        "reasoning_effort": _text((profile.get("reasoning") or {}).get("default")),
                    }
                    self.configs.save("model_routing", new_routing)
            elif new_routing != old_routing:
                self.configs.save("model_routing", new_routing)
            self.configs.resolve_model_routes()
        except (ValueError, ReadError, WriteError) as exc:
            try:
                self.configs.save("models", old_catalog)
                self.configs.save("model_routing", old_routing)
            except Exception:
                pass
            if isinstance(exc, (ReadError, WriteError)):
                raise
            raise SettingsValidationError(str(exc)) from exc
        return item_id

    @staticmethod
    def _validate_key(value: object) -> str:
        if not isinstance(value, str):
            raise SettingsValidationError("provider_key_invalid")
        key = value.strip()
        if (
            not key
            or len(key.encode("utf-8")) > 16 * 1024
            or any(ord(character) < 32 or ord(character) == 127 for character in key)
        ):
            raise SettingsValidationError("provider_key_invalid")
        return key

    def update_key(self, connection_id: object, action: object, key: object, revision: object) -> None:
        if not isinstance(connection_id, str) or action not in {"set", "delete"}:
            raise SettingsValidationError("provider_key_update_invalid")
        if not isinstance(revision, str):
            raise SettingsValidationError("provider_key_update_invalid")
        if revision != self.configs.revision("models"):
            raise SettingsConflictError("settings_revision_conflict")
        if action == "delete":
            if key != "":
                raise SettingsValidationError("provider_key_delete_body_invalid")
            normalized = ""
        else:
            normalized = self._validate_key(key)
        config = deepcopy(self.configs.load("models"))
        connection = next(
            (item for item in config.get("connections", []) if item.get("id") == connection_id),
            None,
        )
        if connection is None:
            raise SettingsNotFoundError("model_connection_not_found")
        connection["api_key"] = normalized
        self.configs.save("models", config)


def _text(value: object) -> str:
    return str(value or "")


def _string_list(value: object) -> list[str]:
    return [_text(item) for item in value] if isinstance(value, list) else []


def _identifier_refs(value: object) -> list[str]:
    """Keep stable references while excluding path-shaped or free-form values."""
    refs = value if isinstance(value, list) else [value] if value else []
    return [
        text for item in refs
        if (text := _text(item).strip())
        and len(text) <= 160
        and "/" not in text
        and "\\" not in text
    ]


class ProtocolCatalogReader:
    """Registry-only, read-only projection over persona rules and docs."""

    rule_categories = ("permanent", "passive_read", "step_level", "periodic", "on_demand")

    def __init__(self, rules_root: Path = RULES_ROOT, docs_root: Path = DOCS_ROOT):
        self.rules_root = rules_root.resolve()
        self.docs_root = docs_root.resolve()

    @staticmethod
    def _registry(root: Path, name: str) -> dict:
        payload = json.loads((root / name).read_text(encoding="utf-8"))
        if not isinstance(payload, dict):
            raise ValueError("protocol_registry_not_object")
        return payload

    @staticmethod
    def _safe_path(root: Path, value: object) -> tuple[str, Path]:
        relative = _text(value).strip()
        if (
            not relative
            or "\\" in relative
            or Path(relative).is_absolute()
            or Path(relative).suffix.lower() != ".md"
            or any(part in {"", ".", ".."} for part in relative.split("/"))
        ):
            raise ValueError("invalid_protocol_registry_path")
        resolved = (root / Path(relative)).resolve()
        try:
            resolved.relative_to(root)
        except ValueError as exc:
            raise ValueError("protocol_registry_path_escape") from exc
        return relative, resolved

    @staticmethod
    def _metadata(raw: dict, keys: tuple[str, ...]) -> dict:
        return {
            key: value
            for key in keys
            if (value := _text(raw.get(key)).strip())
        }

    def _rule_catalog(self) -> dict:
        registry = self._registry(self.rules_root, "rules_registry.json")
        categories = []
        total = 0
        for category in self.rule_categories:
            raw_entries = registry.get(category)
            if not isinstance(raw_entries, list):
                raise ValueError("invalid_rules_registry_category")
            entries = []
            for raw in raw_entries:
                if not isinstance(raw, dict):
                    raise ValueError("invalid_rules_registry_entry")
                relative, _resolved = self._safe_path(self.rules_root, raw.get("path"))
                entries.append({
                    "id": f"{category}:{relative}",
                    "kind": "rule",
                    "category": category,
                    "file": _text(raw.get("file") or Path(relative).name),
                    "path": relative,
                    "source_ref": f"rules/{relative}",
                    **self._metadata(raw, ("description", "layer", "scope", "load", "trigger")),
                })
            total += len(entries)
            categories.append({"id": category, "count": len(entries), "entries": entries})
        return {
            "registry_version": _text(registry.get("_version")),
            "registry_note": _text(registry.get("_note")),
            "total": total,
            "categories": categories,
        }

    def _doc_catalog(self) -> dict:
        registry = self._registry(self.docs_root, "docs_registry.json")
        registrations: list[tuple[str, dict]] = []
        for category in ("inject", "lookup"):
            raw_entries = registry.get(category)
            if not isinstance(raw_entries, list):
                raise ValueError("invalid_docs_registry_category")
            registrations.extend((category, raw) for raw in raw_entries)
        popup = registry.get("popup")
        if not isinstance(popup, dict):
            raise ValueError("invalid_docs_popup_registry")
        for tier in ("guide", "reminder", "warning"):
            raw_entries = popup.get(tier)
            if not isinstance(raw_entries, list):
                raise ValueError("invalid_docs_popup_tier")
            registrations.extend((f"popup.{tier}", raw) for raw in raw_entries)
        persona = registry.get("persona")
        if not isinstance(persona, list):
            raise ValueError("invalid_docs_persona_registry")
        registrations.extend(("persona", raw) for raw in persona)

        merged: dict[str, dict] = {}
        for category, raw in registrations:
            if not isinstance(raw, dict):
                raise ValueError("invalid_docs_registry_entry")
            relative, _resolved = self._safe_path(self.docs_root, raw.get("path"))
            entry = merged.setdefault(relative, {
                "id": relative,
                "kind": "doc",
                "file": _text(raw.get("file") or Path(relative).name),
                "path": relative,
                "source_ref": f"docs/{relative}",
                "description": _text(raw.get("description")),
                "categories": [],
                "uses": [],
            })
            entry["categories"].append(category)
            entry["uses"].append({
                "category": category,
                **self._metadata(raw, ("target", "trigger", "usage", "tier", "source_mode", "consume")),
            })
        return {
            "registry_version": _text(registry.get("_version")),
            "registry_note": _text(registry.get("_note")),
            "registrations": len(registrations),
            "total": len(merged),
            "entries": list(merged.values()),
        }

    def catalog(self) -> dict:
        return {
            "schema_version": "seed_gui_protocol_catalog.v1",
            "rules": self._rule_catalog(),
            "docs": self._doc_catalog(),
        }

    def document(self, kind: str, item_id: str) -> dict:
        catalog = self.catalog()
        if kind == "rule":
            entries = [
                entry
                for category in catalog["rules"]["categories"]
                for entry in category["entries"]
            ]
            root = self.rules_root
        elif kind == "doc":
            entries = catalog["docs"]["entries"]
            root = self.docs_root
        else:
            raise ValueError("invalid_protocol_kind")
        entry = next((candidate for candidate in entries if candidate["id"] == item_id), None)
        if entry is None:
            raise KeyError(item_id)
        _relative, path = self._safe_path(root, entry["path"])
        try:
            size = path.stat().st_size
        except FileNotFoundError:
            raise
        if size > MAX_PROTOCOL_DOCUMENT_BYTES:
            raise OSError("protocol_document_too_large")
        content = path.read_text(encoding="utf-8")
        return {
            "schema_version": "seed_gui_protocol_document.v1",
            "kind": kind,
            "id": item_id,
            "title": entry.get("file") or item_id,
            "description": entry.get("description") or "",
            "source_ref": entry["source_ref"],
            "categories": entry.get("categories") or [entry.get("category")],
            "content_md": content,
        }


class PersonaProjectionReader:
    """Strict, read-only projection over the current persona source files."""

    def __init__(self, core_path: Path = PERSONA_CORE_MD, state_store=None):
        self.core_path = Path(core_path).resolve()
        self.state_store = state_store or StateStore()

    @staticmethod
    def _field(snapshot: dict, dotpath: str):
        value = snapshot
        for part in dotpath.split("."):
            if not isinstance(value, dict) or part not in value:
                raise ValueError(f"persona_state_field_missing:{dotpath}")
            value = value[part]
        return deepcopy(value)

    def core(self) -> dict:
        try:
            if self.core_path.stat().st_size > MAX_PERSONA_CORE_BYTES:
                raise OSError("persona_core_too_large")
            content = self.core_path.read_text(encoding="utf-8")
        except (OSError, UnicodeError) as exc:
            raise ReadError(str(self.core_path), cause=exc) from exc
        if not content.strip():
            raise ValueError("persona_core_empty")
        return {
            "schema_version": "seed_gui_persona_core.v1",
            "source_ref": "persona/core.md",
            "content_md": content,
        }

    def state(self) -> dict:
        snapshot = self.state_store.read_snapshot()
        return {
            "schema_version": "seed_gui_persona_state.v1",
            "observed_at": datetime.now(timezone.utc).isoformat(),
            "source_ref": "persona/state.json",
            "dynamic_descriptions": StatusBarBuilder().dynamic_axes_to_map(
                self._field(snapshot, "base.dynamic_axes")
            ),
            "fields": [
                {"path": dotpath, "value": self._field(snapshot, dotpath)}
                for dotpath in STATE_FIELDS
            ],
        }


class DepositionReader:
    """Narrow, path-free projection over the existing Seed stores/processors."""

    def __init__(
        self,
        memory_store=None,
        container_store=None,
        relation_store=None,
        workbench_store=None,
        focus_processor=apply_container_focus_declarations,
    ):
        self.memory_store = memory_store or MemoryStore()
        self.container_store = container_store or ContainerStore()
        self.relation_store = relation_store or RelationStore()
        self.workbench_store = workbench_store or WorkbenchStore()
        self.focus_processor = focus_processor

    @staticmethod
    def _memory_summary(raw: dict) -> dict:
        return {
            "id": _text(raw.get("id")),
            "memory_layer": _text(raw.get("memory_layer")),
            "title": _text(raw.get("title")),
            "type": _text(raw.get("type")),
            "weight": raw.get("weight"),
            "subject": _text(raw.get("subject")),
            "access": "public",
            "current_overview": _text(raw.get("current_overview")),
            "current_overview_updated_at": _text(
                raw.get("current_overview_updated_at")
            ),
            "tags": _string_list(raw.get("tags")),
            "linked_containers": _string_list(raw.get("linked_containers")),
            "created_round": raw.get("created_round"),
            "last_recalled_round": raw.get("last_recalled_round"),
            "created_at": _text(raw.get("created_at")),
            "last_recalled_at": _text(raw.get("last_recalled_at")),
        }

    @staticmethod
    def _container_summary(raw: dict) -> dict:
        entries = []
        for item in raw.get("entries") or []:
            if not isinstance(item, dict):
                continue
            entries.append({
                "mem_id": _text(item.get("mem_id")),
                "title": _text(item.get("title")),
                "round": item.get("round"),
                "target_file": _text(item.get("target_file")),
                "status": _text(item.get("status")),
                "updated_at": _text(item.get("updated_at")),
            })
        return {
            "id": _text(raw.get("id")),
            "type": _text(raw.get("type")),
            "prefix": _text(raw.get("prefix")),
            "title": _text(raw.get("title") or raw.get("name")),
            "status": _text(raw.get("status")),
            "focus": bool(raw.get("focus")),
            "created_at": _text(raw.get("created_at")),
            "updated_at": _text(raw.get("updated_at")),
            "entries": entries,
        }

    @staticmethod
    def _relation_summary(raw: dict) -> dict:
        return {
            "id": _text(raw.get("id")),
            "name": _text(raw.get("name") or raw.get("id")),
            "category": _text(raw.get("category")),
            "status": _text(raw.get("status") or "active"),
            "updated_at": _text(raw.get("updated_at")),
        }

    def index(self) -> dict:
        focus = self.focus_projection()
        memories = [
            self._memory_summary(item)
            for item in self.memory_store.list_public_entries()
            if isinstance(item, dict)
            and str(item.get("access") or "public").strip().lower() == "public"
        ]
        containers = [
            self._container_summary(item)
            for item in self.container_store.list_containers()
            if isinstance(item, dict)
        ]
        for item in containers:
            item["focus"] = bool(item.get("id") and item["id"] == focus["current"])
        relations = [
            self._relation_summary(item)
            for item in self.relation_store.list_cards(status="active")
            if isinstance(item, dict)
        ]
        return {
            "schema_version": "seed_gui_deposition_index.v1",
            "memory": memories,
            "containers": containers,
            "relations": relations,
            "focus": focus,
        }

    def focus_projection(self) -> dict:
        return {
            "current": _text(self.workbench_store.get("base.focus")),
            "previous": _text(self.workbench_store.get("base.old_focus")),
        }

    def container_exists(self, container_id: str) -> bool:
        return bool(self.container_store.container_exists(container_id))

    def apply_focus(self, action: str, container_id: str) -> dict:
        declaration = {"action": action}
        if container_id:
            declaration["container_id"] = container_id
        receipts = self.focus_processor(
            [declaration],
            {
                "container_store": self.container_store,
                "workbench_store": self.workbench_store,
            },
            round_num=0,
        )
        receipt = receipts[0] if receipts else {
            "tool_id": "container_focus",
            "status": "rejected",
            "reason": "missing_processor_receipt",
        }
        return {
            "schema_version": "seed_gui_container_focus_result.v1",
            "submission_source": "seed_gui",
            "receipt": receipt,
            "focus": self.focus_projection(),
        }

    @staticmethod
    def _task_record(raw: dict, *, acceptance: bool = False) -> dict:
        record_id = _text(
            raw.get("acceptance_id") if acceptance else raw.get("item_id")
        ).strip()
        status = _text(raw.get("status") or ("pending" if acceptance else "open"))
        return {
            "id": record_id,
            "title": _text(
                raw.get("description") if acceptance
                else raw.get("title") or raw.get("description")
            ),
            "required": raw.get("required", raw.get("mandatory", True)) is not False,
            "status": status.strip().lower(),
            "evidence_refs": _identifier_refs(raw.get("evidence_refs")),
            "reason": _text(raw.get("reason")),
        }

    def task_projection(self) -> dict:
        slots = self.workbench_store.active_guide_slots()
        slots = slots if isinstance(slots, dict) else {}
        active_task = _text(self.workbench_store.get("base.active_task")).strip()
        active_guides = {
            key: _text(slots.get(key)).strip()
            for key in ("rhythm", "work")
        }
        empty = {
            "schema_version": "seed_gui_task_projection.v1",
            "active_task": active_task,
            "active_guides": active_guides,
            "task": None,
            "summary": {
                "state": "empty",
                "open_items": 0,
                "pending_acceptance": 0,
                "open_pending_inputs": 0,
                "blocked_records": 0,
                "evidence_refs": 0,
            },
        }
        if not active_task:
            return empty

        guide = self.workbench_store.load_task_guide(active_task)
        if not isinstance(guide, dict):
            raise ValueError("active_task_guide_missing")

        items = []
        for raw in guide.get("items") or []:
            if not isinstance(raw, dict):
                continue
            item_id = _text(raw.get("item_id")).strip()
            if item_id == "task_progress" or raw.get("task_record_type") == "acceptance":
                continue
            record = self._task_record(raw)
            if record["id"]:
                items.append(record)
        acceptance = [
            self._task_record(raw, acceptance=True)
            for raw in guide.get("acceptance") or []
            if isinstance(raw, dict) and _text(raw.get("acceptance_id")).strip()
        ]
        pending_inputs = [
            {
                "id": _text(raw.get("pending_input_id") or raw.get("id")).strip(),
                "status": _text(raw.get("status") or "pending").strip().lower(),
                "summary": _text(raw.get("summary")),
                "source_refs": _identifier_refs(raw.get("source_refs")),
            }
            for raw in guide.get("pending_inputs") or []
            if isinstance(raw, dict)
        ]
        source_requirements = [
            {
                "id": _text(raw.get("requirement_id") or raw.get("id")).strip(),
                "summary": _text(raw.get("summary")),
            }
            for raw in guide.get("source_requirements") or []
            if isinstance(raw, dict)
        ]

        open_items = sum(
            record["required"]
            and record["status"] not in TASK_DONE_STATUSES | {"blocked"}
            for record in items
        )
        pending_acceptance = sum(
            record["required"]
            and record["status"] not in TASK_ACCEPTED_STATUSES | {"blocked"}
            for record in acceptance
        )
        open_pending_inputs = sum(
            record["status"] not in TASK_SETTLED_INPUT_STATUSES
            for record in pending_inputs
        )
        blocked_records = sum(
            record["status"] == "blocked" for record in items + acceptance
        )
        evidence_refs = {
            ref
            for record in items + acceptance
            for ref in record["evidence_refs"]
        }
        state = (
            "blocked" if blocked_records
            else "open" if open_items or pending_acceptance or open_pending_inputs
            else "settled"
        )
        return {
            **empty,
            "task": {
                "id": active_task,
                "guide_id": _text(guide.get("guide_id")),
                "kind": _text(guide.get("kind")),
                "title": _text(guide.get("task_title") or guide.get("title") or active_task),
                "goal": _text(guide.get("task_goal") or guide.get("goal")),
                "source_requirements": source_requirements,
                "items": items,
                "acceptance": acceptance,
                "pending_inputs": pending_inputs,
                "risk_notes": _string_list(guide.get("risk_notes")),
            },
            "summary": {
                "state": state,
                "open_items": open_items,
                "pending_acceptance": pending_acceptance,
                "open_pending_inputs": open_pending_inputs,
                "blocked_records": blocked_records,
                "evidence_refs": len(evidence_refs),
            },
        }

    def detail(self, kind: str, item_id: str) -> dict:
        index = self.index()
        key = {"memory": "memory", "container": "containers", "relation": "relations"}.get(kind)
        if key is None:
            raise ValueError("invalid_deposition_kind")
        summary = next((item for item in index[key] if item.get("id") == item_id), None)
        if summary is None:
            raise KeyError(item_id)

        if kind == "memory":
            raw = self.memory_store.read_body_by_id(item_id)
            meta = raw.get("meta") if isinstance(raw.get("meta"), dict) else {}
            if str(meta.get("access") or "public").strip().lower() != "public":
                raise KeyError(item_id)
            item = {
                **summary,
                "body": project_memory_body(raw.get("body"), meta),
                "total_lines": raw.get("total_lines"),
                "total_chars": raw.get("total_chars"),
            }
        elif kind == "container":
            raw = self.container_store.read_focus_projection(item_id)
            content = _text(raw.get("content"))
            item = {
                **summary,
                "allowed_targets": _string_list(raw.get("allowed_targets")),
                "default_target": _text(raw.get("default_target")),
                "content": content[:MAX_DEPOSITION_CONTENT_CHARS],
                "content_truncated": len(content) > MAX_DEPOSITION_CONTENT_CHARS,
                "total_lines": raw.get("total_lines"),
                "total_chars": raw.get("total_chars"),
            }
        else:
            raw = self.relation_store.read_card(item_id, summary.get("category"))
            if not isinstance(raw, dict):
                raise KeyError(item_id)
            axes = raw.get("axes") if isinstance(raw.get("axes"), dict) else {}
            item = {
                **summary,
                "name": _text(raw.get("name") or summary.get("name")),
                "axes": {
                    axis: int(axes.get(axis, 0))
                    for axis in AXIS_NAMES
                    if isinstance(axes.get(axis, 0), (int, float))
                },
                "notes": [
                    {"date": _text(note.get("date")), "content": _text(note.get("content"))}
                    for note in raw.get("notes") or []
                    if isinstance(note, dict)
                ],
                "history": [
                    {"date": _text(entry.get("date")), "content": _text(entry.get("content"))}
                    for entry in raw.get("history") or []
                    if isinstance(entry, dict)
                ],
                "tags": _string_list(raw.get("tags")),
                "created_at": _text(raw.get("created_at")),
                "updated_at": _text(raw.get("updated_at")),
            }
        return {
            "schema_version": "seed_gui_deposition_detail.v1",
            "kind": kind,
            "item": item,
        }


class LazyDepositionReader:
    """Do not construct live stores until an initialized persona is read."""

    def __init__(self, factory=DepositionReader):
        self.factory = factory
        self._reader = None
        self._lock = threading.Lock()

    def _resolved(self):
        if self._reader is None:
            with self._lock:
                if self._reader is None:
                    self._reader = self.factory()
        return self._reader

    def __getattr__(self, name):
        return getattr(self._resolved(), name)


def _static_files(gui_root: Path) -> dict[str, tuple[Path, str]]:
    root = gui_root.resolve()
    files = {}

    def add(url: str, path: Path, content_type: str) -> None:
        resolved = path.resolve()
        try:
            resolved.relative_to(root)
        except ValueError:
            return
        if resolved.is_file():
            files[url] = (resolved, content_type)

    add("/", root / "index.html", "text/html; charset=utf-8")
    add("/index.html", root / "index.html", "text/html; charset=utf-8")
    add("/styles.css", root / "styles.css", "text/css; charset=utf-8")
    add("/markdown.css", root / "markdown.css", "text/css; charset=utf-8")
    add("/app.js", root / "app.js", "application/javascript; charset=utf-8")
    add("/markdown-mermaid.js", root / "markdown-mermaid.js", "application/javascript; charset=utf-8")
    add("/assets/upsp-logo.png", root / "assets" / "upsp-logo.png", "image/png")
    markdown_assets = root / "assets" / "markdown"
    if markdown_assets.is_dir():
        content_types = {
            ".woff2": "font/woff2",
            ".woff": "font/woff",
            ".ttf": "font/ttf",
        }
        for path in markdown_assets.iterdir():
            content_type = content_types.get(path.suffix.lower())
            if path.is_file() and content_type:
                add(f"/assets/markdown/{path.name}", path, content_type)
    manual_root = root / "manual"
    if manual_root.is_dir():
        for path in manual_root.glob("*.md"):
            add(f"/manual/{path.name}", path, "text/markdown; charset=utf-8")
    return files


class SeedGuiHandler(RoundLiveHandler):
    gui_root = GUI_ROOT
    static_files = _static_files(GUI_ROOT)
    send_lock = threading.Lock()
    relay_lock = threading.Lock()
    mutation_lock = threading.Lock()
    deposition_reader = None
    protocol_reader = None
    persona_reader = None
    settings_service = None
    bootstrap_service = None
    runtime_service = None
    desktop_control_token = None
    desktop_session_id = None

    def log_message(self, fmt: str, *args: object) -> None:
        if (
            self.desktop_control_token is not None
            and len(args) >= 2
            and str(args[1]).startswith(("2", "3"))
        ):
            return
        sys.stderr.write("[seed-gui] " + fmt % args + "\n")

    def _send_json(self, status: int, payload: object) -> None:
        try:
            super()._send_json(status, payload)
        except CLIENT_DISCONNECT_ERRORS:
            return

    def _send_file(self, path: Path, content_type: str) -> None:
        try:
            super()._send_file(path, content_type)
        except CLIENT_DISCONNECT_ERRORS:
            return

    def _request_path(self) -> str:
        return unquote(urlparse(self.path).path)

    def _same_host(self, *, require_origin: bool = False) -> bool:
        expected = f"127.0.0.1:{self.server.server_address[1]}"
        if self.headers.get("Host", "") != expected:
            return False
        origin = self.headers.get("Origin")
        if require_origin:
            return origin == f"http://{expected}"
        return origin in {None, f"http://{expected}"}

    def _error(self, status: int, code: str, detail: str = "") -> None:
        payload = {"error": code}
        if detail:
            payload["detail"] = detail
        self._send_json(status, payload)

    def _discard_bounded_request_body(self) -> None:
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return
        if 0 < content_length <= MAX_REQUEST_BYTES:
            self.rfile.read(content_length)

    def _drain_oversized_request_prefix(self, content_length: int) -> None:
        """Avoid a Windows TCP reset for the common one-byte-over-limit case."""
        original_timeout = self.connection.gettimeout()
        remaining = min(content_length, MAX_REQUEST_BYTES + 1)
        try:
            self.connection.settimeout(0.05)
            while remaining > 0:
                chunk = self.rfile.read(min(64 * 1024, remaining))
                if not chunk:
                    break
                remaining -= len(chunk)
        except (OSError, TimeoutError):
            pass
        finally:
            try:
                self.connection.settimeout(original_timeout)
            except OSError:
                pass

    def _json_object(self, required_keys: set[str]) -> dict | None:
        if self.headers.get_content_type() != "application/json":
            self._error(400, "invalid_content_type")
            return None
        try:
            content_length = int(self.headers.get("Content-Length", ""))
        except ValueError:
            self._error(400, "invalid_content_length")
            return None
        if content_length < 0:
            self._error(400, "invalid_content_length")
            return None
        if content_length > MAX_REQUEST_BYTES:
            self._drain_oversized_request_prefix(content_length)
            self._error(400, "invalid_content_length")
            return None
        try:
            payload = json.loads(self.rfile.read(content_length))
        except (UnicodeDecodeError, json.JSONDecodeError):
            self._error(400, "invalid_json")
            return None
        if not isinstance(payload, dict) or set(payload) != required_keys:
            self._error(400, "invalid_request")
            return None
        return payload

    def _runtime_status(self) -> None:
        try:
            service = self.runtime_service.status()
        except Exception as exc:
            self._error(503, "runtime_host_failed", str(exc))
            return
        runtime = service["runtime"]
        data = service.get("cli_data") or {
            "active_flags": [],
            "round_type": runtime.get("round_type"),
            "active_guides": {"rhythm": "", "work": ""},
        }
        self._send_json(200, {
            "schema_version": "seed_gui_runtime_status.v2",
            "host": {
                "connected": True,
                "address": "127.0.0.1",
                "port": self.server.server_address[1],
            },
            "host_session": service["session_id"],
            "process_id": service["process_id"],
            "supervisor": {
                "schema_version": service["supervisor_schema"],
                "state": service["supervisor_state"],
                "path": service["supervisor_path"],
            },
            "current_round": runtime["current_round"],
            "round_type": runtime["round_type"],
            "stage": runtime["stage"],
            "stop_requested": runtime["stop_requested"],
            "can_stop": runtime["can_stop"],
            "heartbeat_suspended": runtime["heartbeat_suspended"],
            "pending_tool_approval": runtime.get("pending_tool_approval"),
            "last_outcome": service["last_outcome"],
            "send_in_flight": (
                service["send_in_flight"] or self.send_lock.locked()),
            "relay_in_flight": (
                service["relay_in_flight"] or self.relay_lock.locked()),
            "mutation_in_flight": self.mutation_lock.locked(),
            "cli": {
                "ok": True,
                "command": "status",
                "data": data,
                "warnings": [],
            },
        })

    def _require_initialized_persona(self) -> bool:
        try:
            ready = self.bootstrap_service.initializer.status()["ready"]
        except (OSError, ValueError):
            self._error(503, "bootstrap_status_failed")
            return False
        if ready:
            return True
        self._error(409, "persona_initialization_required")
        return False

    def _mutation_conflict_code(self) -> str:
        if self.send_lock.locked():
            return "send_in_flight"
        if self.relay_lock.locked():
            return "relay_in_flight"
        return "mutation_in_flight"

    def _permission_payload(self) -> tuple[str, bool] | None:
        payload = self._json_object({"permission_level", "unlimited_confirmed"})
        if payload is None:
            return None
        permission = payload.get("permission_level")
        confirmed = payload.get("unlimited_confirmed")
        if permission not in {"limited", "guarded", "unlimited"} or not isinstance(confirmed, bool):
            self._error(400, "invalid_permission")
            return None
        if permission == "unlimited" and confirmed is not True:
            self._error(403, "unlimited_confirmation_required")
            return None
        return permission, confirmed

    def _deposition_detail(self, kind: str) -> None:
        query = parse_qs(urlparse(self.path).query, keep_blank_values=True)
        if set(query) != {"id"} or len(query["id"]) != 1:
            self._error(400, "invalid_deposition_request")
            return
        item_id = query["id"][0].strip()
        if not item_id or len(item_id) > 128:
            self._error(400, "invalid_deposition_id")
            return
        try:
            payload = self.deposition_reader.detail(kind, item_id)
        except (KeyError, ValueError):
            self._error(404, "deposition_item_not_found")
            return
        except Exception:
            self._error(503, "deposition_read_failed")
            return
        self._send_json(200, payload)

    def _protocol_document(self) -> None:
        query = parse_qs(urlparse(self.path).query, keep_blank_values=True)
        if set(query) != {"kind", "id"} or any(len(values) != 1 for values in query.values()):
            self._error(400, "invalid_protocol_document_request")
            return
        kind = query["kind"][0].strip()
        item_id = query["id"][0].strip()
        if kind not in {"rule", "doc"} or not item_id or len(item_id) > 512:
            self._error(400, "invalid_protocol_document_request")
            return
        try:
            payload = self.protocol_reader.document(kind, item_id)
        except (KeyError, FileNotFoundError):
            self._error(404, "protocol_document_not_found")
            return
        except Exception:
            self._error(503, "protocol_document_read_failed")
            return
        self._send_json(200, payload)

    def _context_request_prefix_diff(self) -> None:
        query = parse_qs(urlparse(self.path).query, keep_blank_values=True)
        if set(query) != {"round", "frame_id"} or any(
                len(values) != 1 for values in query.values()):
            self._error(400, "invalid_context_diff_request")
            return
        raw_round = query["round"][0].strip()
        frame_id = query["frame_id"][0].strip()
        if not raw_round.isdigit() or not frame_id or len(frame_id) > 128:
            self._error(400, "invalid_context_diff_request")
            return
        try:
            payload = build_request_prefix_diff(
                self.round_dir,
                int(raw_round),
                frame_id,
            )
        except Exception:
            self._error(503, "context_diff_read_failed")
            return
        self._send_json(200, payload)

    def do_GET(self) -> None:  # noqa: N802
        if not self._same_host():
            self._error(403, "source_forbidden")
            return
        path = self._request_path()
        if path == "/api/about":
            if urlparse(self.path).query:
                self._error(400, "invalid_about_request")
                return
            self._send_json(200, ABOUT_PROJECTION)
            return
        if path == "/api/bootstrap/status":
            if urlparse(self.path).query:
                self._error(400, "invalid_bootstrap_status_request")
                return
            try:
                self._send_json(200, self.bootstrap_service.status())
            except (ReadError, ValueError, OSError):
                self._error(503, "bootstrap_status_failed")
            return
        if path == "/api/settings":
            if urlparse(self.path).query:
                self._error(400, "invalid_settings_request")
                return
            try:
                self._send_json(200, self.settings_service.projection())
            except (ReadError, ValueError):
                self._error(503, "settings_read_failed")
            return
        if path == "/api/runtime/status":
            self._runtime_status()
            return
        if path == "/api/persona/core":
            if urlparse(self.path).query:
                self._error(400, "invalid_persona_core_request")
                return
            try:
                payload = self.persona_reader.core()
            except Exception:
                self._error(503, "persona_core_read_failed")
                return
            self._send_json(200, payload)
            return
        if path == "/api/persona/state":
            if urlparse(self.path).query:
                self._error(400, "invalid_persona_state_request")
                return
            try:
                payload = self.persona_reader.state()
            except Exception:
                self._error(503, "persona_state_read_failed")
                return
            self._send_json(200, payload)
            return
        if path == "/api/protocol/catalog":
            if urlparse(self.path).query:
                self._error(400, "invalid_protocol_catalog_request")
                return
            if not self._require_initialized_persona():
                return
            try:
                payload = self.protocol_reader.catalog()
            except Exception:
                self._error(503, "protocol_catalog_read_failed")
                return
            self._send_json(200, payload)
            return
        if path == "/api/protocol/document":
            if not self._require_initialized_persona():
                return
            self._protocol_document()
            return
        if path == "/api/deposition":
            if urlparse(self.path).query:
                self._error(400, "invalid_deposition_request")
                return
            if not self._require_initialized_persona():
                return
            try:
                payload = self.deposition_reader.index()
            except Exception:
                self._error(503, "deposition_read_failed")
                return
            self._send_json(200, payload)
            return
        if path == "/api/workbench/task":
            if urlparse(self.path).query:
                self._error(400, "invalid_task_request")
                return
            if not self._require_initialized_persona():
                return
            try:
                payload = self.deposition_reader.task_projection()
            except Exception:
                self._error(503, "task_projection_failed")
                return
            self._send_json(200, payload)
            return
        if path == "/api/context/request-prefix-diff":
            self._context_request_prefix_diff()
            return
        detail_kind = {
            "/api/deposition/memory": "memory",
            "/api/deposition/container": "container",
            "/api/deposition/relation": "relation",
        }.get(path)
        if detail_kind is not None:
            if not self._require_initialized_persona():
                return
            self._deposition_detail(detail_kind)
            return
        if path in {"/api/rounds", "/api/live/state", "/api/live/events"}:
            super().do_GET()
            return
        static_file = self.static_files.get(path)
        if static_file is not None:
            self._send_file(*static_file)
            return
        self._error(404, "not_found")

    def do_POST(self) -> None:  # noqa: N802
        path = self._request_path()
        if path == "/api/desktop/shutdown":
            if self.desktop_control_token is None:
                self._error(404, "not_found")
                return
            if not self._same_host(require_origin=True):
                self._discard_bounded_request_body()
                self._error(403, "source_forbidden")
                return
            self._desktop_shutdown()
            return
        if path not in {
            "/api/bootstrap/provider-test",
            "/api/bootstrap/persona",
            "/api/runtime/send",
            "/api/runtime/stop",
            "/api/runtime/tool-approval",
            "/api/runtime/execution-permission",
            "/api/runtime/relay",
            "/api/runtime/tick",
            "/api/container/focus",
            "/api/settings",
            "/api/settings/model-catalog",
            "/api/settings/model-context-window/resolve",
            "/api/settings/provider-key",
        }:
            self._error(404, "not_found")
            return
        if not self._same_host(require_origin=True):
            self._discard_bounded_request_body()
            self._error(403, "source_forbidden")
            return
        if path == "/api/bootstrap/provider-test":
            self._bootstrap_provider_test()
            return
        if path == "/api/bootstrap/persona":
            self._bootstrap_persona()
            return
        if path == "/api/settings":
            self._settings_update()
            return
        if path == "/api/settings/model-catalog":
            self._model_catalog_update()
            return
        if path == "/api/settings/model-context-window/resolve":
            self._model_context_window_resolve()
            return
        if path == "/api/settings/provider-key":
            self._provider_key_update()
            return
        if path == "/api/runtime/stop":
            self._runtime_stop()
            return
        if path == "/api/runtime/tool-approval":
            self._runtime_tool_approval()
            return
        if path == "/api/runtime/execution-permission":
            self._runtime_execution_permission()
            return
        if not self.bootstrap_service.initializer.status()["ready"]:
            self._discard_bounded_request_body()
            self._error(409, "persona_initialization_required")
            return
        if path == "/api/container/focus":
            self._container_focus()
            return
        if path in {"/api/runtime/relay", "/api/runtime/tick"}:
            self._runtime_pending(path.rsplit("/", 1)[-1])
            return
        payload = self._json_object({"message", "permission_level", "unlimited_confirmed"})
        if payload is None:
            return
        message = payload.get("message")
        permission = payload.get("permission_level")
        confirmed = payload.get("unlimited_confirmed")
        if not isinstance(message, str) or not message.strip():
            self._error(400, "message_required")
            return
        if permission not in {"limited", "guarded", "unlimited"} or not isinstance(confirmed, bool):
            self._error(400, "invalid_permission")
            return
        if permission == "unlimited" and confirmed is not True:
            self._error(403, "unlimited_confirmation_required")
            return
        try:
            model_ready = self.settings_service.projection()["persona"]["setup_model_ready"]
        except (ReadError, SettingsValidationError, ValueError):
            self._error(503, "settings_read_failed")
            return
        if not model_ready:
            self._error(409, "model_setup_required")
            return
        if not self.mutation_lock.acquire(blocking=False):
            self._error(409, self._mutation_conflict_code())
            return
        if not self.send_lock.acquire(blocking=False):
            self.mutation_lock.release()
            self._error(409, "send_in_flight")
            return
        response_status = 502
        response_payload = None
        response_error = ("runtime_send_failed", "")
        try:
            try:
                result = self.runtime_service.submit_message(
                    message,
                    permission,
                )
            except RuntimeServiceError as exc:
                code = str(exc)
                if code == "round_in_flight":
                    response_status = 409
                    response_error = ("send_in_flight", "")
                elif code == "persona_initialization_required":
                    response_status = 409
                    response_error = (code, "")
                else:
                    response_status = 503
                    response_error = ("runtime_host_failed", code)
            else:
                response_status = 200
                response_payload = {
                    "ok": True,
                    "command": "send",
                    "data": result,
                }
        finally:
            self.send_lock.release()
            self.mutation_lock.release()
        if response_status == 200:
            self._send_json(200, response_payload)
        else:
            self._error(response_status, *response_error)

    def _bootstrap_provider_test(self) -> None:
        payload = self._json_object(set())
        if payload is None:
            return
        try:
            receipt = self.bootstrap_service.test_provider()
        except PersonaInitializationError as exc:
            code = str(exc)
            status = 409 if code in {
                "provider_test_in_flight",
                "persona_already_initialized",
                "persona_directory_incomplete",
            } else 503
            self._error(status, code)
            return
        except APIBridgeError as exc:
            self._error(502, "provider_test_failed", str(exc))
            return
        except (ReadError, ValueError, OSError) as exc:
            self._error(503, "provider_test_unavailable", str(exc))
            return
        self._send_json(200, receipt)

    def _bootstrap_persona(self) -> None:
        payload = self._json_object({
            "mode", "preset_id", "profile", "test_token", "skip_model_setup",
        })
        if payload is None:
            return
        if not self.mutation_lock.acquire(blocking=False):
            self._error(409, self._mutation_conflict_code())
            return
        try:
            receipt = self.bootstrap_service.create_persona(
                payload.get("mode"),
                payload.get("preset_id"),
                payload.get("profile"),
                payload.get("test_token"),
                payload.get("skip_model_setup", False),
            )
        except PersonaInitializationError as exc:
            code = str(exc)
            status = 409 if code in {
                "persona_initialization_in_flight",
                "persona_already_exists",
                "persona_directory_incomplete",
                "provider_test_required",
            } else 400
            self._error(status, code)
            return
        except (ReadError, WriteError, OSError) as exc:
            self._error(503, "persona_initialization_failed", str(exc))
            return
        finally:
            self.mutation_lock.release()
        try:
            self.runtime_service.start_if_ready()
        except Exception as exc:
            self._error(503, "runtime_start_failed", str(exc))
            return
        self._send_json(200, receipt)

    def _desktop_shutdown(self) -> None:
        supplied = self.headers.get(DESKTOP_CONTROL_HEADER, "")
        if not hmac.compare_digest(supplied, self.desktop_control_token):
            self._discard_bounded_request_body()
            self._error(403, "desktop_control_forbidden")
            return
        payload = self._json_object(set())
        if payload is None:
            return
        self._send_json(202, {
            "schema_version": "upsp_desktop_shutdown_receipt.v1",
            "accepted": True,
            "session_id": self.desktop_session_id,
        })
        self.server.request_desktop_shutdown()

    def _runtime_tool_approval(self) -> None:
        payload = self._json_object({"approval_id", "decision"})
        if payload is None:
            return
        approval_id = payload.get("approval_id")
        decision = payload.get("decision")
        if (
                not isinstance(approval_id, str)
                or not approval_id.strip()
                or decision not in {"allow_once", "skip"}):
            self._error(400, "invalid_tool_approval")
            return
        try:
            receipt = self.runtime_service.resolve_tool_approval(
                approval_id.strip(), decision
            )
        except ToolApprovalConflict as exc:
            self._error(409, str(exc))
            return
        except (RuntimeServiceError, ValueError) as exc:
            self._error(409, str(exc))
            return
        self._send_json(200, {
            "schema_version": "general_tool_approval_receipt.v1",
            **receipt,
        })

    def _runtime_stop(self) -> None:
        payload = self._json_object(set())
        if payload is None:
            return
        try:
            receipt = self.runtime_service.stop_round()
        except RuntimeServiceError as exc:
            code = str(exc)
            if code == "no_round_in_flight":
                self._error(409, code)
            else:
                self._error(503, "runtime_stop_failed", code)
            return
        self._send_json(200, receipt)

    def _runtime_execution_permission(self) -> None:
        permission_payload = self._permission_payload()
        if permission_payload is None:
            return
        permission, _confirmed = permission_payload
        try:
            receipt = self.runtime_service.update_execution_permission(permission)
        except RuntimeServiceError as exc:
            self._error(409, str(exc))
            return
        self._send_json(200, {
            "schema_version": "seed_gui_execution_permission_receipt.v1",
            **receipt,
        })

    def _runtime_pending(self, kind: str) -> None:
        permission_payload = self._permission_payload()
        if permission_payload is None:
            return
        permission, _confirmed = permission_payload
        if not self.mutation_lock.acquire(blocking=False):
            self._error(409, self._mutation_conflict_code())
            return
        if not self.relay_lock.acquire(blocking=False):
            self.mutation_lock.release()
            self._error(409, "relay_in_flight")
            return
        response_status = 502
        response_payload = None
        response_error = ("runtime_relay_failed", "")
        try:
            try:
                result = self.runtime_service.submit_pending(
                    kind,
                    permission,
                )
            except RuntimeServiceError as exc:
                code = str(exc)
                if code == f"{kind}_not_pending":
                    response_status = 409
                    response_error = (code, "")
                elif code == "round_in_flight":
                    response_status = 409
                    response_error = ("relay_in_flight", "")
                else:
                    response_status = 503
                    response_error = ("runtime_host_failed", code)
            else:
                response_status = 200
                response_payload = {
                    "ok": True,
                    "command": kind,
                    "data": result,
                }
        finally:
            self.relay_lock.release()
            self.mutation_lock.release()
        if response_status == 200:
            self._send_json(200, response_payload)
        else:
            self._error(response_status, *response_error)

    def _container_focus(self) -> None:
        payload = self._json_object({"action", "container_id"})
        if payload is None:
            return
        action = payload.get("action")
        container_id = payload.get("container_id")
        if action not in {"open", "close", "restore"}:
            self._error(400, "invalid_focus_action")
            return
        if not isinstance(container_id, str) or len(container_id) > 128:
            self._error(400, "invalid_container_id")
            return
        container_id = container_id.strip()
        if action in {"open", "close"} and not container_id:
            self._error(400, "container_id_required")
            return
        if action == "restore" and container_id:
            self._error(400, "restore_container_id_forbidden")
            return
        if not self.mutation_lock.acquire(blocking=False):
            self._error(409, self._mutation_conflict_code())
            return
        response_status = 200
        response_payload = None
        response_error = ""
        try:
            focus = self.deposition_reader.focus_projection()
            if action == "open" and not self.deposition_reader.container_exists(container_id):
                response_status = 404
                response_error = "container_not_found"
            elif action == "close":
                if not focus["current"]:
                    response_status = 409
                    response_error = "missing_focus"
                elif focus["current"] != container_id:
                    response_status = 409
                    response_error = "focus_conflict"
            elif action == "restore" and not focus["previous"]:
                response_status = 409
                response_error = "missing_old_focus"
            if not response_error:
                result = self.deposition_reader.apply_focus(action, container_id)
                receipt = result.get("receipt") or {}
                if receipt.get("status") == "applied":
                    response_payload = result
                else:
                    response_status = (
                        404 if receipt.get("reason") == "container_not_found" else 409
                    )
                    response_payload = {
                        **result,
                        "error": receipt.get("reason") or "container_focus_rejected",
                    }
        except Exception:
            response_status = 503
            response_error = "container_focus_failed"
        finally:
            self.mutation_lock.release()
        if response_payload is not None:
            self._send_json(response_status, response_payload)
        else:
            self._error(response_status, response_error)

    def _settings_update(self) -> None:
        payload = self._json_object({"revision", "file", "changes"})
        if payload is None:
            return
        if not self.mutation_lock.acquire(blocking=False):
            self._error(409, self._mutation_conflict_code())
            return
        response_status = 200
        response_payload = None
        response_error = ("settings_write_failed", "")
        try:
            self.settings_service.update(
                payload.get("file"),
                payload.get("changes"),
                payload.get("revision"),
            )
            response_payload = self.settings_service.projection()
        except SettingsConflictError:
            response_status = 409
            response_error = ("settings_revision_conflict", "")
        except SettingsValidationError as exc:
            response_status = 400
            response_error = (str(exc).split(":", 1)[0], str(exc))
        except ReadError:
            response_status = 503
            response_error = ("settings_read_failed", "")
        except WriteError:
            response_status = 503
        finally:
            self.mutation_lock.release()
        if response_status == 200:
            self._send_json(200, response_payload)
        else:
            self._error(response_status, *response_error)

    def _provider_key_update(self) -> None:
        payload = self._json_object({"connection_id", "action", "key", "revision"})
        if payload is None:
            return
        if not self.mutation_lock.acquire(blocking=False):
            self._error(409, self._mutation_conflict_code())
            return
        response_status = 200
        response_payload = None
        response_error = "provider_key_write_failed"
        try:
            self.settings_service.update_key(
                payload.get("connection_id"),
                payload.get("action"),
                payload.get("key"),
                payload.get("revision"),
            )
            response_payload = self.settings_service.projection()
        except SettingsConflictError as exc:
            response_status = 409
            response_error = str(exc) or "settings_revision_conflict"
        except SettingsNotFoundError as exc:
            response_status = 404
            response_error = str(exc) or "model_connection_not_found"
        except SettingsValidationError as exc:
            response_status = 400
            response_error = str(exc).split(":", 1)[0]
        except ReadError:
            response_status = 503
            response_error = "provider_key_read_failed"
        except WriteError:
            response_status = 503
            response_error = "provider_key_write_failed"
        finally:
            self.mutation_lock.release()
        if response_status == 200:
            self._send_json(200, response_payload)
        else:
            self._error(response_status, response_error)

    def _model_catalog_update(self) -> None:
        payload = self._json_object({"revision", "entity", "action", "id", "values"})
        if payload is None:
            return
        if not self.mutation_lock.acquire(blocking=False):
            self._error(409, self._mutation_conflict_code())
            return
        status = 200
        error = "model_catalog_write_failed"
        response = None
        try:
            self.settings_service.update_model_catalog(
                payload.get("entity"),
                payload.get("action"),
                payload.get("id"),
                payload.get("values"),
                payload.get("revision"),
            )
            response = self.settings_service.projection()
        except SettingsConflictError as exc:
            status = 409
            error = str(exc) or "settings_revision_conflict"
        except SettingsNotFoundError as exc:
            status = 404
            error = str(exc) or "model_catalog_item_not_found"
        except SettingsValidationError as exc:
            status = 400
            error = str(exc).split(":", 1)[0]
        except ReadError:
            status = 503
            error = "model_catalog_read_failed"
        except WriteError:
            status = 503
        finally:
            self.mutation_lock.release()
        if status == 200:
            self._send_json(200, response)
        else:
            self._error(status, error)

    def _model_context_window_resolve(self) -> None:
        payload = self._json_object({"connection_id", "model"})
        if payload is None:
            return
        try:
            response = self.settings_service.resolve_model_context_window(
                payload.get("connection_id"), payload.get("model")
            )
        except SettingsNotFoundError as exc:
            self._error(404, str(exc) or "model_connection_not_found")
            return
        except SettingsValidationError as exc:
            self._error(400, str(exc) or "model_context_resolution_invalid")
            return
        except ReadError:
            self._error(503, "model_catalog_read_failed")
            return
        self._send_json(200, response)


class SeedGuiServer(ThreadingHTTPServer):
    runtime_service = None

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.desktop_shutdown_requested = threading.Event()

    def request_desktop_shutdown(self):
        if self.desktop_shutdown_requested.is_set():
            return
        self.desktop_shutdown_requested.set()
        threading.Thread(
            target=self.shutdown,
            name="upsp-desktop-shutdown",
            daemon=True,
        ).start()

    def server_close(self):
        if self.runtime_service is not None:
            self.runtime_service.close()
        super().server_close()


def make_server(
    port: int,
    round_dir: Path = default_round_dir(),
    gui_root: Path = GUI_ROOT,
    deposition_reader=None,
    protocol_reader=None,
    persona_reader=None,
    settings_service=None,
    bootstrap_service=None,
    runtime_service=None,
    desktop_control_token=None,
    desktop_session_id=None,
) -> ThreadingHTTPServer:
    resolved_settings = settings_service or SettingsService()
    resolved_bootstrap = bootstrap_service or BootstrapService(resolved_settings)
    handler = type(
        "ConfiguredSeedGuiHandler",
        (SeedGuiHandler,),
        {
            "round_dir": round_dir.resolve(),
            "gui_root": gui_root.resolve(),
            "static_files": _static_files(gui_root),
            "send_lock": threading.Lock(),
            "relay_lock": threading.Lock(),
            "mutation_lock": threading.Lock(),
            "deposition_reader": deposition_reader or LazyDepositionReader(),
            "protocol_reader": protocol_reader or ProtocolCatalogReader(),
            "persona_reader": persona_reader or PersonaProjectionReader(),
            "settings_service": resolved_settings,
            "bootstrap_service": resolved_bootstrap,
            "desktop_control_token": desktop_control_token,
            "desktop_session_id": desktop_session_id,
        },
    )
    server = SeedGuiServer(("127.0.0.1", port), handler)
    service = runtime_service or ResidentRuntimeService(
        persona_ready=lambda: bool(
            resolved_bootstrap.initializer.status().get("ready")
        ),
        default_permission_level="guarded",
    )
    handler.runtime_service = service
    server.runtime_service = service
    try:
        service.start(
            host_address="127.0.0.1",
            port=server.server_address[1],
        )
    except (RuntimeAlreadyRunning, RuntimeSupervisorCorrupt, RuntimeServiceError):
        server.server_close()
        raise
    return server


def _desktop_environment() -> tuple[str, str]:
    token = os.environ.pop(DESKTOP_CONTROL_TOKEN_ENV, "").strip().lower()
    session_id = os.environ.pop(DESKTOP_SESSION_ID_ENV, "").strip().lower()
    if not DESKTOP_TOKEN_RE.fullmatch(token):
        raise ValueError("desktop_control_token_invalid")
    if not DESKTOP_SESSION_RE.fullmatch(session_id):
        raise ValueError("desktop_session_id_invalid")
    return token, session_id


def _desktop_ready_record(server, session_id: str) -> dict:
    _host, port = server.server_address
    return {
        "schema_version": "upsp_desktop_ready.v1",
        "process_id": os.getpid(),
        "session_id": session_id,
        "origin": f"http://127.0.0.1:{port}",
        "product_version": PRODUCT_VERSION,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve the UPSP Seed GUI locally.")
    parser.add_argument("--port", type=int, default=8770)
    parser.add_argument("--round-dir", type=Path, default=default_round_dir())
    parser.add_argument("--gui-root", type=Path, default=GUI_ROOT)
    parser.add_argument("--open", action="store_true")
    parser.add_argument("--desktop", action="store_true")
    args = parser.parse_args(argv)

    desktop_token = None
    desktop_session_id = None
    if args.desktop:
        if args.open:
            parser.error("--desktop and --open are mutually exclusive")
        try:
            desktop_token, desktop_session_id = _desktop_environment()
        except ValueError as exc:
            parser.error(str(exc))

    desktop_options = {}
    if args.desktop:
        desktop_options = {
            "desktop_control_token": desktop_token,
            "desktop_session_id": desktop_session_id,
        }
    desktop_stdout = sys.stdout
    output_scope = (
        redirect_stdout(sys.stderr) if args.desktop else nullcontext()
    )
    with output_scope:
        server = make_server(
            args.port,
            args.round_dir,
            args.gui_root,
            **desktop_options,
        )
        _host, port = server.server_address
        url = f"http://127.0.0.1:{port}/"
        if args.desktop:
            print(json.dumps(
                _desktop_ready_record(server, desktop_session_id),
                ensure_ascii=False,
                separators=(",", ":"),
            ), file=desktop_stdout, flush=True)
            sys.stderr.write(f"[seed-gui] Desktop host ready: {url}\n")
        else:
            print(f"Serving UPSP Seed GUI: {url}")
            print(f"Round dir: {args.round_dir}")
        if args.open and not args.desktop:
            threading.Timer(0.6, lambda: webbrowser.open(url)).start()
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            pass
        finally:
            server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
