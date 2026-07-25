"""Host-neutral bootstrap state, provider test stamp, and persona creation."""

from __future__ import annotations

import hashlib
import os
import threading
import time
import uuid

from data.persona_identity import public_identity
from engines.executor import APIExecutor
from errors import APIBridgeError, ReadError
from paths import ACTIVE_PID, PERSONA_DIR, PERSONA_PRESETS_DIR, PERSONA_TEMPLATE_DIR

from .persona_initializer import (
    PersonaInitializationError,
    PersonaInitializer,
    load_preset,
)


class MemoryConnectivitySink:
    """Connectivity interface for pre-persona probes; never writes a file."""

    def __init__(self):
        self.entries = []

    def log_latency(self, endpoint, status, message=""):
        self.entries.append({
            "endpoint": str(endpoint or ""),
            "status": str(status or ""),
            "message": str(message or "")[:200],
        })


class BootstrapService:
    """Initialization state, one-model probe stamp, and atomic persona creation."""

    TOKEN_TTL_SECONDS = 15 * 60
    UNBOUND_MODEL_STAMP = {
        "profile_id": "unbound",
        "model_alias": "未绑定",
        "model": "未绑定",
        "context_window": 0,
    }

    def __init__(
        self,
        settings_service,
        initializer=None,
        probe_runner=None,
        monotonic=None,
    ):
        self.settings = settings_service
        self.configs = settings_service.configs
        self.initializer = initializer or PersonaInitializer(
            PERSONA_DIR,
            PERSONA_TEMPLATE_DIR,
            PERSONA_PRESETS_DIR,
            pid=ACTIVE_PID,
        )
        self.probe_runner = probe_runner or self._run_probe
        self.monotonic = monotonic or time.monotonic
        self.test_lock = threading.Lock()
        self.create_lock = threading.Lock()
        self._test_stamp = None

    @staticmethod
    def _connection_fingerprint(item):
        profile = item["profile"]
        connection = item["connection"]
        env_name = str(connection.get("api_key_env") or "").strip()
        key = os.environ.get(env_name, "") if env_name else ""
        key = key or str(connection.get("api_key") or "")
        value = "\n".join((
            str(connection.get("url") or "").strip().rstrip("/").lower(),
            str(connection.get("protocol") or "").strip().lower(),
            str(profile.get("model") or "").strip(),
            key,
        ))
        return hashlib.sha256(value.encode("utf-8")).hexdigest()

    def _current_setup(self):
        resolved = self.configs.resolve_model_routes()
        primary = (resolved.get("effective_primaries") or {}).get("setup")
        routes = (resolved.get("phases") or {}).get("setup") or []
        if not isinstance(primary, dict) or not routes:
            raise PersonaInitializationError("setup_primary_model_required")
        item = routes[0]
        if item.get("slot") != "primary" or item.get("source_phase") != "setup":
            raise PersonaInitializationError("setup_primary_model_required")
        connection = item["connection"]
        if self.settings._key_source(connection) == "missing":
            raise PersonaInitializationError("setup_primary_key_required")
        binding = {
            "profile_id": item["model_id"],
            "models_revision": self.configs.revision("models"),
            "routing_revision": self.configs.revision("model_routing"),
            "connection_fingerprint": self._connection_fingerprint(item),
        }
        return item, binding

    def _stamp_valid(self):
        stamp = self._test_stamp
        if not isinstance(stamp, dict) or stamp["expires_at"] <= self.monotonic():
            return False
        try:
            _item, binding = self._current_setup()
        except (PersonaInitializationError, ReadError, ValueError):
            return False
        return all(stamp.get(key) == value for key, value in binding.items())

    def status(self):
        persona = self.initializer.status()
        try:
            preset = load_preset(self.initializer.preset_dir, "alyosha")
        except PersonaInitializationError:
            preset = None
        setup = None
        setup_error = ""
        try:
            item, _binding = self._current_setup()
            setup = {
                "profile_id": item["model_id"],
                "model_alias": item["model_alias"],
                "model": item["profile"]["model"],
                "connection_alias": item["connection_alias"],
                "context_window": item["profile"].get("context_window", 0),
                "reasoning_effort": item["reasoning_effort"],
            }
        except (PersonaInitializationError, ReadError, ValueError) as exc:
            setup_error = str(exc)
        return {
            "schema_version": "seed_gui_bootstrap_status.v1",
            "persona": persona,
            "identity": (
                public_identity(str(self.initializer.persona_dir / "core.md"))
                if persona["ready"]
                else None
            ),
            "preset": preset,
            "setup_primary": setup,
            "setup_error": setup_error,
            "provider_test": {
                "valid": self._stamp_valid(),
                "ttl_seconds": self.TOKEN_TTL_SECONDS,
            },
        }

    def _run_probe(self, profile_id):
        sink = MemoryConnectivitySink()
        executor = APIExecutor(
            config_store=self.configs,
            connectivity_store=sink,
        )
        return executor.probe_model_profile(profile_id)

    def test_provider(self):
        persona_status = self.initializer.status()
        if persona_status["state"] != "missing":
            raise PersonaInitializationError(
                "persona_already_initialized"
                if persona_status["ready"]
                else "persona_directory_incomplete"
            )
        if not self.test_lock.acquire(blocking=False):
            raise PersonaInitializationError("provider_test_in_flight")
        try:
            self._test_stamp = None
            item, binding = self._current_setup()
            result = self.probe_runner(item["model_id"])
            if not isinstance(result, dict) or not str(result.get("response") or "").strip():
                raise APIBridgeError(item["model_id"], "provider returned empty response")
            token = uuid.uuid4().hex
            self._test_stamp = {
                **binding,
                "token": token,
                "expires_at": self.monotonic() + self.TOKEN_TTL_SECONDS,
            }
            return {
                "schema_version": "seed_gui_provider_test_receipt.v1",
                "status": "passed",
                "model_profile_id": item["model_id"],
                "model_alias": item["model_alias"],
                "model": item["profile"]["model"],
                "latency_ms": int(result.get("latency_ms") or 0),
                "test_token": token,
                "expires_in_seconds": self.TOKEN_TTL_SECONDS,
            }
        finally:
            self.test_lock.release()

    def create_persona(
        self,
        mode,
        preset_id,
        profile,
        test_token,
        skip_model_setup=False,
    ):
        if not self.create_lock.acquire(blocking=False):
            raise PersonaInitializationError("persona_initialization_in_flight")
        try:
            status = self.initializer.status()
            if status["state"] != "missing":
                raise PersonaInitializationError(
                    "persona_already_exists"
                    if status["ready"]
                    else "persona_directory_incomplete"
                )
            if not isinstance(skip_model_setup, bool):
                raise PersonaInitializationError("persona_request_invalid")
            if skip_model_setup:
                if test_token is not None:
                    raise PersonaInitializationError("persona_request_invalid")
                model_stamp = dict(self.UNBOUND_MODEL_STAMP)
            else:
                if not isinstance(test_token, str) or not self._stamp_valid():
                    raise PersonaInitializationError("provider_test_required")
                if test_token != self._test_stamp.get("token"):
                    raise PersonaInitializationError("provider_test_required")
                item, _binding = self._current_setup()
                model_stamp = {
                    "profile_id": item["model_id"],
                    "model_alias": item["model_alias"],
                    "model": item["profile"]["model"],
                    "context_window": item["profile"].get("context_window", 0),
                }
            if mode == "preset":
                if preset_id != "alyosha" or profile is not None:
                    raise PersonaInitializationError("persona_request_invalid")
                selected = load_preset(self.initializer.preset_dir, preset_id)
            elif mode == "custom":
                if preset_id is not None:
                    raise PersonaInitializationError("persona_request_invalid")
                selected = profile
            else:
                raise PersonaInitializationError("persona_request_invalid")
            receipt = self.initializer.create(selected, model_stamp)
            self._test_stamp = None
            return {
                "schema_version": "seed_gui_persona_init_receipt.v1",
                "status": "created",
                "model_setup": "skipped" if skip_model_setup else "tested",
                "persona": receipt,
            }
        finally:
            self.create_lock.release()
