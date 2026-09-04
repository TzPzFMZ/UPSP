"""
配置文件读写 — Windows 本机全局配置与当前活动位格配置
DDS §32 config/

文件：
  system.json   — 系统参数
  LocalAppData/UPSP/config/models.json — 全局服务连接与模型库
  LocalAppData/UPSP/config/interface.json — 全局界面语言
  model_routing.json — 当前位格三阶段模型路由
  memory.json   — 记忆参数
  media.json    — 媒体参数
  relation.json — 关系参数
  context/periodic.json — 定期层限额
  context/now.json — 当前缓存语料块配置
  context/lately.json — 最近缓存语料块配置
"""
import hashlib
import json
import math
import os
import re
from copy import deepcopy
from urllib.parse import urlparse

from data.atomic_write import atomic_write_json, atomic_write_text
from paths import (
    CONFIG_SYSTEM, CONFIG_MEMORY,
    CONFIG_MEDIA, CONFIG_RELATION,
    CONFIG_MODEL_ROUTING, GLOBAL_INTERFACE_CONFIG, GLOBAL_MODELS_CONFIG,
    LEGACY_CONFIG_API,
    CONTEXT_PERMANENT_JSON, CONTEXT_PERIODIC_JSON,
    CONTEXT_HIGH_FREQ_JSON,
    CONTEXT_NOW_JSON, CONTEXT_LATELY_JSON,
    CONTEXT_POPUP_JSON,
)
from schemas.config import (
    default_system_config, default_memory_config,
    default_media_config, default_relation_config,
    default_interface_config, default_models_config, default_model_routing_config,
    default_permanent_config, default_periodic_config,
    default_high_freq_config,
    default_now_config, default_lately_config,
    default_popup_config,
)
from errors import ReadError, WriteError
from constants import (
    REACTION_AUTO_RELAY_SECONDS,
    REACTION_REMINDER_SECONDS,
    REACTION_WARNING_SECONDS,
)

API_CONFIG_OVERRIDE_ENV = "UPSP_API_CONFIG_OVERRIDE_JSON"
MODEL_PHASES = ("setup", "reaction", "cleanup")
SUPPORTED_PROVIDER_PROTOCOLS = {
    "openai_chat", "openai_responses", "anthropic_messages",
}
SUPPORTED_REASONING_EFFORTS = {
    "", "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
}
AUTOMATIC_PROMPT_CACHE = {"profile": "automatic_tiered"}
ROUND_SNAPSHOT_POLICY_VERSION = 2
TOKEN_WATERMARK_POLICY_VERSION = 2
SYSTEM_CONFIG_VERSION = "Base-0.48.17"
KNOWN_SYSTEM_CONFIG_VERSIONS = {
    "Base-0.47.9", "Base-0.48.14", "Base-0.48.15", "Base-0.48.16",
    SYSTEM_CONFIG_VERSION,
}
NOW_CACHE_POLICY_VERSION = "Base-0.12.0"
LATELY_CACHE_POLICY_VERSION = "Base-0.13.0"
SUPPORTED_PROMPT_CACHE_PROFILES = {
    "automatic_tiered", "off", "key_only",
    "gpt56_explicit_permanent", "gpt56_explicit_tiered",
}
ENV_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
SECRET_OVERRIDE_FIELDS = {
    "api_key", "apikey", "authorization", "x_api_key", "access_token",
}

# 文件→默认模板映射
_CONFIG_MAP = {
    "system":   (CONFIG_SYSTEM,   default_system_config),
    "memory":   (CONFIG_MEMORY,   default_memory_config),
    "media":    (CONFIG_MEDIA,    default_media_config),
    "relation": (CONFIG_RELATION, default_relation_config),
    "interface": (GLOBAL_INTERFACE_CONFIG, default_interface_config),
    "models": (GLOBAL_MODELS_CONFIG, default_models_config),
    "model_routing": (CONFIG_MODEL_ROUTING, default_model_routing_config),
    # context/ 装配规则
    "permanent":     (CONTEXT_PERMANENT_JSON,     default_permanent_config),
    "periodic":      (CONTEXT_PERIODIC_JSON,      default_periodic_config),
    "high_freq":     (CONTEXT_HIGH_FREQ_JSON,     default_high_freq_config),
    "now":           (CONTEXT_NOW_JSON,            default_now_config),
    "lately":        (CONTEXT_LATELY_JSON,         default_lately_config),
    "popup":         (CONTEXT_POPUP_JSON,         default_popup_config),
}
_GLOBAL_CONFIGS = {"interface", "models"}
_PERSONA_TEMPLATE_CONFIGS = set(_CONFIG_MAP) - _GLOBAL_CONFIGS


class ConfigStore:
    """配置文件读写管理"""

    def __init__(self, use_api_environment=True):
        self.use_api_environment = bool(use_api_environment)

    # ==============================================================
    # 通用读写
    # ==============================================================

    def load(self, name):
        """读取指定配置文件；单位格配置缺失或损坏时明确失败。"""
        if name == "api":
            if self.use_api_environment:
                override = os.environ.get(API_CONFIG_OVERRIDE_ENV, "").strip()
                if override:
                    try:
                        loaded = json.loads(override)
                        if not isinstance(loaded, dict):
                            raise ValueError("api override must be a JSON object")
                        return self._normalise_api_override(loaded)
                    except (json.JSONDecodeError, ValueError) as e:
                        raise ReadError(f"env:{API_CONFIG_OVERRIDE_ENV}", cause=e)
            return self._build_api_projection()
        if name not in _CONFIG_MAP:
            raise ValueError(
                f"未知配置: {name}，已知: {list(_CONFIG_MAP.keys()) + ['api']}"
            )
        path, _ = _CONFIG_MAP[name]

        try:
            if not os.path.isfile(path):
                raise FileNotFoundError(path)
            with open(path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if not isinstance(loaded, dict):
                raise ValueError("config must be a JSON object")
            if name == "system":
                loaded = self._normalise_system(loaded)
            elif name == "models":
                loaded = self._normalise_models(loaded)
            elif name == "periodic":
                loaded = self._normalise_periodic(loaded)
            elif name == "relation":
                loaded = self._normalise_relation(loaded)
            elif name == "now":
                loaded = self._normalise_now(loaded)
            elif name == "lately":
                loaded = self._normalise_lately(loaded)
            self._validate(name, loaded)
            return loaded
        except (json.JSONDecodeError, OSError, ValueError) as e:
            raise ReadError(path, cause=e)

    def save(self, name, data):
        """写入指定配置文件（原子）"""
        if name not in _CONFIG_MAP:
            raise ValueError(f"未知配置: {name}")
        path, _ = _CONFIG_MAP[name]
        if name == "system":
            data = self._normalise_system(data)
        elif name == "models":
            data = self._normalise_models(data)
        elif name == "periodic":
            data = self._normalise_periodic(data)
        elif name == "relation":
            data = self._normalise_relation(data)
        elif name == "now":
            data = self._normalise_now(data)
        elif name == "lately":
            data = self._normalise_lately(data)
        self._validate(name, data)
        atomic_write_json(path, data)

    def migrate_system_audit_policy(self):
        """Persist one-time system policy migrations before Runtime starts."""
        path, _ = _CONFIG_MAP["system"]
        try:
            with open(path, "r", encoding="utf-8") as handle:
                original_text = handle.read()
            current = json.loads(original_text)
            if not isinstance(current, dict):
                raise ValueError("config must be a JSON object")
        except (json.JSONDecodeError, OSError, ValueError) as exc:
            raise ReadError(path, cause=exc)
        token_usage = current.get("token_usage") if isinstance(
            current.get("token_usage"), dict) else {}
        legacy_pressure = token_usage.get("warning_ratio")
        migrated_pressure = None
        if (
                not isinstance(legacy_pressure, bool)
                and isinstance(legacy_pressure, (int, float))
                and math.isfinite(legacy_pressure)
                and 0 < float(legacy_pressure) <= 1):
            migrated_pressure = (
                0.9 if float(legacy_pressure) == 0.7 else float(legacy_pressure)
            )
        normalized = self._normalise_system(current)
        self._validate("system", normalized)
        if migrated_pressure is not None:
            self._migrated_lately_pressure_ratio = migrated_pressure
        if normalized != current:
            atomic_write_json(path, normalized)
            try:
                verified = self._read_json_object(path)
                self._validate("system", verified)
                if verified != normalized:
                    raise ValueError("system migration readback mismatch")
            except Exception:
                atomic_write_text(path, original_text)
                raise
        return normalized, normalized != current

    def migrate_periodic_policy(self):
        """Upgrade the known Base-0.10.0 policy before Runtime starts."""
        path, _ = _CONFIG_MAP["periodic"]
        current = self._read_json_object(path)
        try:
            normalized = self._normalise_periodic(current)
            self._validate("periodic", normalized)
            if normalized != current:
                atomic_write_json(path, normalized)
            return normalized, normalized != current
        except (OSError, ValueError) as exc:
            raise ReadError(path, cause=exc)

    def migrate_relation_context_policy(self):
        """Upgrade only the known relation_focus shape to relation_context."""
        path, _ = _CONFIG_MAP["relation"]
        current = self._read_json_object(path)
        original_text = None
        try:
            with open(path, "r", encoding="utf-8") as handle:
                original_text = handle.read()
            normalized = self._normalise_relation(current)
            self._validate("relation", normalized)
            if normalized != current:
                atomic_write_json(path, normalized)
                verified = self._read_json_object(path)
                if verified != normalized:
                    raise ValueError("relation migration readback mismatch")
            return normalized, normalized != current
        except Exception as exc:
            if original_text is not None:
                try:
                    atomic_write_text(path, original_text)
                except Exception as rollback_exc:
                    raise WriteError(
                        path,
                        message=(
                            "relation_migration_rollback_failed:"
                            f"{type(rollback_exc).__name__}"
                        ),
                    ) from rollback_exc
            if isinstance(exc, (ReadError, WriteError)):
                raise
            raise ReadError(path, cause=exc) from exc

    def migrate_now_policy(self):
        """Replace the one known watermark-era shape before Runtime starts."""
        path, _ = _CONFIG_MAP["now"]
        current = self._read_json_object(path)
        try:
            normalized = self._normalise_now(current)
            self._validate("now", normalized)
            if normalized != current:
                atomic_write_json(path, normalized)
            return normalized, normalized != current
        except (OSError, ValueError) as exc:
            raise ReadError(path, cause=exc)

    def migrate_lately_policy(self):
        """Upgrade the one known pre-Spec760 lately policy."""
        path, _ = _CONFIG_MAP["lately"]
        current = self._read_json_object(path)
        try:
            normalized = self._normalise_lately(current)
            migrated_pressure = getattr(
                self, "_migrated_lately_pressure_ratio", None)
            if migrated_pressure is None:
                system_path, _ = _CONFIG_MAP["system"]
                system = self._read_json_object(system_path)
                system_normalized = self._normalise_system(system)
                self._validate("system", system_normalized)
                token_usage = system.get("token_usage") if isinstance(
                    system.get("token_usage"), dict) else {}
                candidate = token_usage.get("warning_ratio")
                if (not isinstance(candidate, bool)
                        and isinstance(candidate, (int, float))
                        and math.isfinite(candidate)
                        and 0 < float(candidate) <= 1):
                    migrated_pressure = (
                        0.9 if float(candidate) == 0.7 else float(candidate)
                    )
            if (
                    current.get("_version") != LATELY_CACHE_POLICY_VERSION
                    and migrated_pressure is not None):
                normalized["pressure_ratio"] = migrated_pressure
            self._validate("lately", normalized)
            if normalized != current:
                atomic_write_json(path, normalized)
            return normalized, normalized != current
        except (OSError, ValueError) as exc:
            raise ReadError(path, cause=exc)

    @staticmethod
    def _normalise_system(data):
        if not isinstance(data, dict):
            raise ValueError("system config must be an object")
        result = deepcopy(data)
        version = result.get("_version")
        if version not in KNOWN_SYSTEM_CONFIG_VERSIONS:
            raise ValueError("unknown system config version")
        round_config = result.get("round")
        if not isinstance(round_config, dict):
            raise ValueError("round must be an object")
        round_keys = set(round_config)
        legacy_round_keys = {"time_limit"}
        current_round_keys = {
            "reminder_seconds", "warning_seconds", "auto_relay_seconds",
        }
        if round_keys == legacy_round_keys:
            if version == SYSTEM_CONFIG_VERSION:
                raise ValueError("current system config uses legacy round shape")
            legacy_limit = round_config.get("time_limit")
            if (
                    isinstance(legacy_limit, bool)
                    or not isinstance(legacy_limit, int)
                    or not 60 <= legacy_limit <= 86400):
                raise ValueError("round.time_limit cannot migrate")
            result["round"] = {
                "reminder_seconds": legacy_limit,
                "warning_seconds": legacy_limit * 2,
                "auto_relay_seconds": legacy_limit * 3,
            }
        elif round_keys != current_round_keys:
            raise ValueError("unknown round timing shape")
        retired = result.get("autonomous_trigger")
        if retired is not None:
            if (
                not isinstance(retired, dict)
                or set(retired) != {
                    "tacit_pending_threshold",
                    "connection_pending_threshold",
                }
                or any(
                    isinstance(value, bool)
                    or not isinstance(value, int)
                    or not 1 <= value <= 1000000
                    for value in retired.values()
                )
            ):
                raise ValueError("unknown autonomous_trigger shape")
            result.pop("autonomous_trigger", None)
        response_anchor = result.setdefault("response_anchor", {"prompt": ""})
        if not isinstance(response_anchor, dict):
            raise ValueError("response_anchor must be an object")
        response_anchor.setdefault("prompt", "")
        audit = result.setdefault("audit", {})
        if not isinstance(audit, dict):
            raise ValueError("audit must be an object")
        policy_version = audit.get("round_snapshot_policy_version")
        if policy_version is None:
            retention = audit.get("round_snapshot_retention")
            if type(retention) is int and retention == 64:
                audit["round_snapshot_retention"] = 8
            audit.setdefault("round_snapshot_max_mib", 256)
            audit["round_snapshot_policy_version"] = ROUND_SNAPSHOT_POLICY_VERSION
        token_usage = result.get("token_usage")
        if not isinstance(token_usage, dict):
            raise ValueError("token_usage must be an object")
        if token_usage.get("watermark_policy_version") is None:
            warning_ratio = token_usage.get("warning_ratio")
            if (
                    not isinstance(warning_ratio, bool)
                    and isinstance(warning_ratio, (int, float))
                    and float(warning_ratio) == 0.7):
                token_usage["warning_ratio"] = 0.9
            token_usage["watermark_policy_version"] = (
                TOKEN_WATERMARK_POLICY_VERSION
            )
        if "warning_ratio" in token_usage:
            warning_ratio = token_usage["warning_ratio"]
            if (
                    isinstance(warning_ratio, bool)
                    or not isinstance(warning_ratio, (int, float))
                    or not math.isfinite(warning_ratio)
                    or not 0 < float(warning_ratio) <= 1):
                raise ValueError("token_usage.warning_ratio out of range")
            token_usage.pop("warning_ratio")
        result["_version"] = SYSTEM_CONFIG_VERSION
        return result

    @staticmethod
    def _normalise_relation(data):
        if not isinstance(data, dict):
            raise ValueError("relation config must be an object")
        keys = set(data)
        shared = {"_comment", "_version", "relation_card_write"}
        old_keys = shared | {"relation_focus"}
        new_keys = shared | {"relation_context"}
        if keys == old_keys and data.get("_version") == "Base-0.10.0":
            result = deepcopy(data)
            result["_version"] = "Base-0.11.0"
            result["relation_context"] = result.pop("relation_focus")
            return result
        if keys == new_keys and data.get("_version") == "Base-0.11.0":
            return deepcopy(data)
        if "relation_focus" in data and "relation_context" in data:
            raise ValueError("mixed relation focus/context shape")
        raise ValueError("unknown relation config shape")

    @staticmethod
    def _normalise_models(data):
        result = deepcopy(data)
        for model in result.get("models") or []:
            if isinstance(model, dict):
                model["prompt_cache"] = deepcopy(AUTOMATIC_PROMPT_CACHE)
                model.setdefault("output_token_limit", 0)
                model.pop("context_window_checked_at", None)
        return result

    @staticmethod
    def _normalise_periodic(data):
        result = deepcopy(data)
        if not isinstance(result, dict):
            return result
        if result.get("_version") != "Base-0.10.0":
            return result
        limits = result.get("limits")
        if not isinstance(limits, dict):
            return result
        normalized = default_periodic_config()
        normalized["limits"]["periodic_memory_items_chars"] = limits.get(
            "periodic_memory_items_chars"
        )
        return normalized

    @staticmethod
    def _normalise_now(data):
        result = deepcopy(data)
        if not isinstance(result, dict):
            return result
        if result.get("_version") == NOW_CACHE_POLICY_VERSION:
            return result
        if "_version" in result or not ConfigStore._is_known_legacy_now(result):
            return result
        return default_now_config()

    @staticmethod
    def _normalise_lately(data):
        result = deepcopy(data)
        if not isinstance(result, dict):
            return result
        if result.get("_version") == LATELY_CACHE_POLICY_VERSION:
            return result
        if "_version" in result or not ConfigStore._is_known_legacy_lately(result):
            return result
        normalized = default_lately_config()
        normalized["pressure_ratio"] = 0.9
        return normalized

    @staticmethod
    def _is_known_legacy_lately(data):
        if not isinstance(data, dict):
            return False
        expected = {
            "layer", "description", "store", "allowed_kinds",
            "budget_chars", "trim_chars", "compact_ratio",
            "compact_shard_chars", "compact_shard_ratio", "fifo_policy",
            "retired_round_window",
        }
        if set(data) != expected:
            return False
        if data.get("layer") != "lately" or data.get("fifo_policy") != "complete_block":
            return False
        numeric = (
            data.get("budget_chars"), data.get("trim_chars"),
            data.get("compact_shard_chars"),
        )
        if any(isinstance(value, bool) or not isinstance(value, int) or value < 1
               for value in numeric):
            return False
        ratios = (data.get("compact_ratio"), data.get("compact_shard_ratio"))
        return all(
            not isinstance(value, bool)
            and isinstance(value, (int, float))
            and math.isfinite(value)
            and 0 < float(value) <= 1
            for value in ratios
        )

    @staticmethod
    def _is_known_legacy_now(data):
        target = default_now_config()
        expected_keys = (set(target) - {"_version"}) | {
            "budget_chars", "trim_chars", "fifo_policy", "policy_by_kind",
        }
        if set(data) != expected_keys:
            return False
        if any(data.get(key) != target.get(key) for key in (
                "layer", "store", "allowed_kinds", "persistent_lanes")):
            return False
        if not isinstance(data.get("description"), str):
            return False
        if data.get("fifo_policy") != "complete_block":
            return False
        budget = data.get("budget_chars")
        trim = data.get("trim_chars")
        if (
                isinstance(budget, bool) or not isinstance(budget, int)
                or isinstance(trim, bool) or not isinstance(trim, int)
                or budget < 1 or trim < 1 or trim > budget):
            return False
        lane_kinds = (
            target["persistent_lanes"]["now_lately_raw"]
            + target["persistent_lanes"]["now_lately_no_raw"]
        )
        expected_policy = {
            kind: {"now": True, "lately": True} for kind in lane_kinds
        }
        if data.get("policy_by_kind") != expected_policy:
            return False
        source_kinds = data.get("source_kinds")
        return (
            isinstance(source_kinds, dict)
            and set(source_kinds) == set(target["source_kinds"])
            and all(isinstance(value, str) for value in source_kinds.values())
        )

    def revision(self, name):
        """Return the canonical file-byte SHA used by the GUI write boundary."""
        if name == "api":
            payload = json.dumps(
                self.load("api"), ensure_ascii=False, sort_keys=True
            ).encode("utf-8")
            return hashlib.sha256(payload).hexdigest()
        if name not in _CONFIG_MAP:
            raise ValueError(f"未知配置: {name}")
        path, _ = _CONFIG_MAP[name]
        try:
            with open(path, "rb") as f:
                payload = f.read()
            return hashlib.sha256(payload).hexdigest()
        except OSError as e:
            raise ReadError(path, cause=e)

    def init_global(self):
        """Initialize only machine-global control-plane configuration."""
        created = []
        models_missing = not os.path.isfile(GLOBAL_MODELS_CONFIG)
        migrated_models = None
        if models_missing and os.path.isfile(LEGACY_CONFIG_API):
            legacy = self._read_json_object(LEGACY_CONFIG_API)
            migrated_models, _ = self._migrate_legacy_api(legacy)

        for name in _GLOBAL_CONFIGS:
            spec = _CONFIG_MAP.get(name)
            if spec is None:
                continue
            path, default_fn = spec
            if not os.path.isfile(path):
                value = default_fn()
                if name == "models" and migrated_models is not None:
                    value = migrated_models
                self.save(name, value)
                created.append(name)
            else:
                self.load(name)
        return created

    def init_persona(self):
        """Validate and migrate the active persona without creating it."""
        lately_spec = _CONFIG_MAP.get("lately")
        system_spec = _CONFIG_MAP.get("system")
        try:
            if (lately_spec and system_spec
                    and os.path.isfile(lately_spec[0])
                    and os.path.isfile(system_spec[0])):
                # Destination first: a crash cannot discard a custom legacy ratio.
                self.migrate_lately_policy()
        except (ReadError, WriteError, OSError, ValueError) as exc:
            raise self._persona_init_error("lately", lately_spec[0], exc) from exc

        for name, spec in _CONFIG_MAP.items():
            if name in _GLOBAL_CONFIGS:
                continue
            path, _default_fn = spec
            try:
                if not os.path.isfile(path):
                    raise FileNotFoundError(path)
                if name == "system":
                    self.migrate_system_audit_policy()
                elif name == "periodic":
                    self.migrate_periodic_policy()
                elif name == "relation":
                    self.migrate_relation_context_policy()
                elif name == "now":
                    self.migrate_now_policy()
                elif name == "lately":
                    self.migrate_lately_policy()
                else:
                    self.load(name)  # 损坏文件必须显式失败，禁止静默覆盖
            except (ReadError, WriteError, OSError, ValueError) as exc:
                raise self._persona_init_error(name, path, exc) from exc
        return []

    @staticmethod
    def _persona_init_error(name, path, cause):
        error = ReadError(
            path,
            message=f"persona_config_invalid:{name}",
            cause=cause,
        )
        error.config_name = str(name)
        return error

    def init_all(self):
        """Initialize globals, then validate/migrate the active persona."""
        created = self.init_global()
        self.init_persona()
        return created

    @staticmethod
    def _read_json_object(path):
        try:
            with open(path, "r", encoding="utf-8") as handle:
                value = json.load(handle)
            if not isinstance(value, dict):
                raise ValueError("config must be a JSON object")
            return value
        except (json.JSONDecodeError, OSError, ValueError) as exc:
            raise ReadError(path, cause=exc)

    @staticmethod
    def _validate(name, data):
        if not isinstance(data, dict):
            raise ValueError("config must be a JSON object")
        if name in _PERSONA_TEMPLATE_CONFIGS:
            ConfigStore._validate_template_shape(
                data, _CONFIG_MAP[name][1](), path=name
            )
        if name == "now" and data.get("_version") != NOW_CACHE_POLICY_VERSION:
            raise ValueError("now._version invalid")
        if name == "lately":
            if data.get("_version") != LATELY_CACHE_POLICY_VERSION:
                raise ValueError("lately._version invalid")
            pressure = data["pressure_ratio"]
            summary = data["semantic_summary_ratio"]
            target = data["cycle_target_ratio"]
            protected = data["protected_interaction_count"]
            batch_chars = data["batch_source_chars"]
            if any(
                    isinstance(value, bool)
                    or not isinstance(value, (int, float))
                    or not math.isfinite(value)
                    for value in (pressure, summary, target)):
                raise ValueError("lately ratios invalid")
            if not 0.50 <= float(pressure) <= 0.99:
                raise ValueError("lately.pressure_ratio out of range")
            if not 0.01 <= float(summary) <= 0.50:
                raise ValueError("lately.semantic_summary_ratio out of range")
            if not 0.05 <= float(target) <= 0.80:
                raise ValueError("lately.cycle_target_ratio out of range")
            if not float(summary) < float(target) < float(pressure):
                raise ValueError("lately ratios order invalid")
            if (
                    isinstance(protected, bool) or not isinstance(protected, int)
                    or not 0 <= protected <= 128):
                raise ValueError("lately.protected_interaction_count out of range")
            if (
                    isinstance(batch_chars, bool) or not isinstance(batch_chars, int)
                    or not 1024 <= batch_chars <= 262144):
                raise ValueError("lately.batch_source_chars out of range")
        if name == "memory":
            heat = data["heat"]
            significant = heat["zone_thresholds"]["significant"]
            uncertain = heat["zone_thresholds"]["uncertain"]
            if not 0 < uncertain < significant <= 100:
                raise ValueError("memory heat thresholds must satisfy 0 < uncertain < significant <= 100")
            if any(
                not -100 <= value <= 0
                for value in heat["decay_rates"].values()
            ):
                raise ValueError("memory heat decay rates must be between -100 and 0")
            initial = heat["initial_by_weight"]
            if set(initial) != {"1", "2", "3", "4", "5"} or any(
                not 0 <= value <= 100 for value in initial.values()
            ):
                raise ValueError("memory heat initial_by_weight must define 1..5 in range 0..100")
            if not 0 <= heat["recall_boost"] <= 100:
                raise ValueError("memory heat recall_boost out of range")
            if not 1 <= heat["upgrade_high_rounds"] <= 100000:
                raise ValueError("memory heat upgrade_high_rounds out of range")
            if not 0 <= heat["locked_value"] <= 100:
                raise ValueError("memory heat locked_value out of range")
        if (
            name == "system"
            and data["connectivity"]["max_latency_records"] < 1
        ):
            raise ValueError("connectivity.max_latency_records must be positive")
        if name == "system":
            round_config = data["round"]
            reminder = round_config["reminder_seconds"]
            warning = round_config["warning_seconds"]
            auto_relay = round_config["auto_relay_seconds"]
            if any(
                    isinstance(value, bool) or not isinstance(value, int)
                    for value in (reminder, warning, auto_relay)):
                raise ValueError("round timing values out of range")
            if not (
                    60 <= reminder <= 86400
                    and 60 <= warning <= 172800
                    and 60 <= auto_relay <= 259200):
                raise ValueError("round timing values out of range")
            if not reminder < warning < auto_relay:
                raise ValueError("round timing values must be strictly increasing")
            prompt = data["response_anchor"]["prompt"]
            if not isinstance(prompt, str) or len(prompt) > 512:
                raise ValueError("response_anchor.prompt must be a string of at most 512 characters")
            audit = data["audit"]
            retention = audit["round_snapshot_retention"]
            max_mib = audit["round_snapshot_max_mib"]
            if (
                    isinstance(retention, bool)
                    or not isinstance(retention, int)
                    or not 1 <= retention <= 64):
                raise ValueError("audit.round_snapshot_retention out of range")
            if (
                    isinstance(max_mib, bool)
                    or not isinstance(max_mib, int)
                    or not 1 <= max_mib <= 4096):
                raise ValueError("audit.round_snapshot_max_mib out of range")
            if audit["round_snapshot_policy_version"] != 2:
                raise ValueError("audit.round_snapshot_policy_version invalid")
            if data["token_usage"]["watermark_policy_version"] != 2:
                raise ValueError("token_usage.watermark_policy_version invalid")
        if name == "interface":
            if data.get("locale") not in {"system", "zh-CN", "en-US"}:
                raise ValueError("interface.locale must be system, zh-CN or en-US")
        elif name == "models":
            connections = data.get("connections")
            models = data.get("models")
            if not isinstance(connections, list) or not isinstance(models, list):
                raise ValueError("models connections/models must be arrays")
            connection_ids = set()
            connection_aliases = set()
            for item in connections:
                if not isinstance(item, dict):
                    raise ValueError("connection must be an object")
                cid = str(item.get("id") or "").strip()
                alias = str(item.get("alias") or "").strip()
                protocol = str(item.get("protocol") or "").strip()
                url = str(item.get("url") or "").strip()
                if not cid or cid in connection_ids or not alias:
                    raise ValueError("connection id/alias must be non-empty and id unique")
                folded = alias.casefold()
                if folded in connection_aliases:
                    raise ValueError("connection alias must be unique")
                if protocol not in SUPPORTED_PROVIDER_PROTOCOLS:
                    raise ValueError("unsupported connection protocol")
                parsed = urlparse(url)
                if (
                    parsed.scheme not in {"http", "https"}
                    or not parsed.netloc
                    or parsed.username
                    or parsed.password
                ):
                    raise ValueError("connection url must be http/https")
                env_name = str(item.get("api_key_env") or "").strip()
                if env_name and not ENV_NAME_RE.fullmatch(env_name):
                    raise ValueError("invalid connection api_key_env")
                if not isinstance(item.get("api_key", ""), str):
                    raise ValueError("connection api_key must be a string")
                connection_ids.add(cid)
                connection_aliases.add(folded)
            model_ids = set()
            model_aliases = set()
            for item in models:
                if not isinstance(item, dict):
                    raise ValueError("model must be an object")
                mid = str(item.get("id") or "").strip()
                alias = str(item.get("alias") or "").strip()
                model_name = str(item.get("model") or "").strip()
                if not mid or mid in model_ids or not alias or not model_name:
                    raise ValueError("model id/alias/name must be non-empty and id unique")
                folded = alias.casefold()
                if folded in model_aliases:
                    raise ValueError("model alias must be unique")
                if item.get("connection_id") not in connection_ids:
                    raise ValueError("model references an unknown connection")
                context_window = item.get("context_window", 0)
                if (
                    isinstance(context_window, bool)
                    or not isinstance(context_window, int)
                    or not 0 <= context_window <= 100000000
                ):
                    raise ValueError("invalid model context_window")
                output_token_limit = item.get("output_token_limit", 0)
                if (
                    isinstance(output_token_limit, bool)
                    or not isinstance(output_token_limit, int)
                    or not 0 <= output_token_limit <= 1000000
                ):
                    raise ValueError("invalid model output_token_limit")
                detection_fields = {"detected_context_window", "context_window_source"}
                if detection_fields.intersection(item):
                    if not detection_fields.issubset(item):
                        raise ValueError("incomplete model context window detection")
                    detected = item.get("detected_context_window")
                    source = str(item.get("context_window_source") or "")
                    if (
                        isinstance(detected, bool)
                        or not isinstance(detected, int)
                        or not 0 <= detected <= 100000000
                        or source not in {
                            "provider", "registry", "legacy_manual", "unknown",
                        }
                    ):
                        raise ValueError("invalid model context window detection")
                    if (source in {"provider", "registry"}) != (detected > 0):
                        raise ValueError("invalid detected model context window")
                    if detected > 0 and context_window > detected:
                        raise ValueError("model context_window exceeds detected capacity")
                reasoning = item.get("reasoning") or {}
                supported = reasoning.get("supported") or []
                default = str(reasoning.get("default") or "")
                if not isinstance(supported, list) or any(
                    str(value) not in SUPPORTED_REASONING_EFFORTS
                    for value in supported
                ):
                    raise ValueError("invalid supported reasoning effort")
                if default not in SUPPORTED_REASONING_EFFORTS:
                    raise ValueError("invalid default reasoning effort")
                if supported and default and default not in supported:
                    raise ValueError("default reasoning effort is not supported")
                request_overrides = item.get("request_overrides", {})
                if not isinstance(request_overrides, dict):
                    raise ValueError("request_overrides must be an object")
                if ConfigStore._contains_secret_override(request_overrides):
                    raise ValueError("request_overrides must not contain secrets")
                streaming = item.get("streaming")
                if not isinstance(streaming, dict):
                    raise ValueError("model streaming must be an object")
                if not isinstance(streaming.get("enabled"), bool):
                    raise ValueError("model streaming.enabled must be boolean")
                if str(streaming.get("protocol") or "") != "openai_sse":
                    raise ValueError("unsupported model streaming protocol")
                if not isinstance(streaming.get("include_usage"), bool):
                    raise ValueError("model streaming.include_usage must be boolean")
                prompt_cache = item.get("prompt_cache")
                if not isinstance(prompt_cache, dict):
                    raise ValueError("model prompt_cache must be an object")
                if str(prompt_cache.get("profile") or "") not in SUPPORTED_PROMPT_CACHE_PROFILES:
                    raise ValueError("unsupported model prompt cache profile")
                model_ids.add(mid)
                model_aliases.add(folded)
            transport = data.get("transport")
            if not isinstance(transport, dict):
                raise ValueError("models transport must be an object")
            handshake = transport.get("handshake")
            breaker = transport.get("circuit_breaker")
            if not isinstance(handshake, dict) or not isinstance(breaker, dict):
                raise ValueError("models transport sections are required")
            bounded = {
                "timeout_seconds": (1, 3600),
                "retry": (0, 2),
                "request_timeout_seconds": (1, 3600),
                "stream_first_chunk_timeout_seconds": (1, 3600),
                "stream_idle_timeout_seconds": (1, 3600),
                "stream_content_overrun_chars": (0, 16777216),
            }
            for key, (minimum, maximum) in bounded.items():
                value = handshake.get(key)
                if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
                    raise ValueError(f"invalid transport handshake field: {key}")
            for key, minimum, maximum in (
                ("max_failures", 1, 100),
                ("cooldown_seconds", 0, 86400),
            ):
                value = breaker.get(key)
                if isinstance(value, bool) or not isinstance(value, int) or not minimum <= value <= maximum:
                    raise ValueError(f"invalid circuit breaker field: {key}")
        elif name == "model_routing":
            routes = data.get("routes")
            if not isinstance(routes, dict):
                raise ValueError("model_routing.routes must be an object")
            for phase in MODEL_PHASES:
                row = routes.get(phase)
                if not isinstance(row, dict):
                    raise ValueError(f"missing route row: {phase}")
                backups = row.get("backups")
                if not isinstance(backups, list) or len(backups) != 2:
                    raise ValueError(f"{phase}.backups must contain two slots")
                for slot in [row.get("primary"), *backups]:
                    if slot is None:
                        continue
                    if not isinstance(slot, dict) or not str(slot.get("model_id") or "").strip():
                        raise ValueError("route slot must contain model_id")
                    if str(slot.get("reasoning_effort") or "") not in SUPPORTED_REASONING_EFFORTS:
                        raise ValueError("invalid route reasoning effort")

    @staticmethod
    def _validate_template_shape(value, template, *, path):
        if isinstance(template, dict):
            if not isinstance(value, dict):
                raise ValueError(f"{path} must be an object")
            if set(value) != set(template):
                missing = sorted(set(template) - set(value))
                extra = sorted(set(value) - set(template))
                raise ValueError(
                    f"{path} shape mismatch: missing={missing}, extra={extra}"
                )
            for key, child in template.items():
                ConfigStore._validate_template_shape(
                    value[key], child, path=f"{path}.{key}"
                )
            return
        if isinstance(template, list):
            if not isinstance(value, list):
                raise ValueError(f"{path} must be an array")
            return
        if template is None:
            return
        expected = type(template)
        if expected is int:
            valid = isinstance(value, int) and not isinstance(value, bool)
        elif expected is float:
            valid = (
                isinstance(value, (int, float))
                and not isinstance(value, bool)
            )
        else:
            valid = isinstance(value, expected)
        if not valid:
            raise ValueError(f"{path} has invalid type")

    @staticmethod
    def _contains_secret_override(value):
        if isinstance(value, dict):
            for key, child in value.items():
                normalized = str(key).strip().lower().replace("-", "_")
                if normalized in SECRET_OVERRIDE_FIELDS:
                    return True
                if ConfigStore._contains_secret_override(child):
                    return True
        elif isinstance(value, list):
            return any(ConfigStore._contains_secret_override(item) for item in value)
        return False

    @staticmethod
    def _stable_id(prefix, value):
        digest = hashlib.sha256(
            json.dumps(value, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()[:12]
        return f"{prefix}_{digest}"

    def _migrate_legacy_api(self, legacy):
        models_cfg = default_models_config()
        routing_cfg = default_model_routing_config()
        endpoints = legacy.get("endpoints") if isinstance(legacy, dict) else {}
        endpoints = endpoints if isinstance(endpoints, dict) else {}
        profile_by_tier = {}
        connection_by_fingerprint = {}
        model_by_fingerprint = {}
        for tier in ("primary", "fallback", "emergency"):
            endpoint = endpoints.get(tier) or {}
            if not isinstance(endpoint, dict) or not str(endpoint.get("url") or "").strip():
                continue
            protocol = str(endpoint.get("provider") or "openai_chat")
            if protocol not in SUPPORTED_PROVIDER_PROTOCOLS:
                protocol = "openai_chat"
            connection_fp = {
                "protocol": protocol,
                "url": str(endpoint.get("url") or "").strip(),
                "api_key_env": str(endpoint.get("api_key_env") or "").strip(),
                "api_key": str(endpoint.get("api_key") or ""),
            }
            fp_key = json.dumps(connection_fp, ensure_ascii=False, sort_keys=True)
            connection = connection_by_fingerprint.get(fp_key)
            if connection is None:
                host = (urlparse(connection_fp["url"]).hostname or "").lower()
                alias = "天枢" if "tian-shu" in host else f"服务 {len(connection_by_fingerprint) + 1}"
                connection = {
                    "id": self._stable_id("conn", connection_fp),
                    "alias": alias,
                    **connection_fp,
                }
                connection_by_fingerprint[fp_key] = connection
                models_cfg["connections"].append(connection)
            model_fp = {
                "connection_id": connection["id"],
                "model": str(endpoint.get("model") or "").strip(),
                "context_window": int(endpoint.get("context_window") or 0),
                "output_token_limit": int(endpoint.get("output_token_limit") or 0),
                "reasoning_effort": str(endpoint.get("reasoning_effort") or ""),
                "streaming": deepcopy(endpoint.get("streaming") or {"enabled": False}),
                "prompt_cache": deepcopy(AUTOMATIC_PROMPT_CACHE),
                "request_overrides": deepcopy(endpoint.get("extra_body") or {}),
            }
            model_key = json.dumps(model_fp, ensure_ascii=False, sort_keys=True)
            profile = model_by_fingerprint.get(model_key)
            if profile is None:
                model_name = model_fp["model"] or f"model-{len(model_by_fingerprint) + 1}"
                effort = model_fp["reasoning_effort"]
                alias = "Terra" if model_name == "gpt-5.6-terra" else model_name
                profile = {
                    "id": self._stable_id("model", model_fp),
                    "alias": alias,
                    "connection_id": connection["id"],
                    "model": model_name,
                    "context_window": model_fp["context_window"],
                    "output_token_limit": model_fp["output_token_limit"],
                    "reasoning": {
                        "supported": [effort] if effort else [],
                        "default": effort,
                    },
                    "streaming": model_fp["streaming"],
                    "prompt_cache": model_fp["prompt_cache"],
                    "request_overrides": model_fp["request_overrides"],
                }
                model_by_fingerprint[model_key] = profile
                models_cfg["models"].append(profile)
            profile_by_tier[tier] = profile

        transport = models_cfg["transport"]
        if isinstance(legacy.get("handshake"), dict):
            transport["handshake"].update(deepcopy(legacy["handshake"]))
        if isinstance(legacy.get("circuit_breaker"), dict):
            transport["circuit_breaker"].update(deepcopy(legacy["circuit_breaker"]))
        step_tiers = legacy.get("step_tiers") or {}
        for phase in MODEL_PHASES:
            tier = str(step_tiers.get(phase) or ("primary" if phase == "reaction" else "fallback"))
            profile = profile_by_tier.get(tier)
            if profile is not None:
                routing_cfg["routes"][phase]["primary"] = {
                    "model_id": profile["id"],
                    "reasoning_effort": profile["reasoning"]["default"],
                }
        # 当前三档若实际完全同源，只保存起手主模型，其余两阶段动态继承。
        selected = [
            routing_cfg["routes"][phase]["primary"]
            for phase in MODEL_PHASES
            if routing_cfg["routes"][phase]["primary"] is not None
        ]
        if selected and len({item["model_id"] for item in selected}) == 1:
            routing_cfg["routes"]["setup"]["primary"] = selected[0]
            routing_cfg["routes"]["reaction"]["primary"] = None
            routing_cfg["routes"]["cleanup"]["primary"] = None
        return models_cfg, routing_cfg

    @staticmethod
    def _normalise_api_override(value):
        result = deepcopy(value)
        endpoints = result.get("endpoints") or {}
        if not isinstance(endpoints, dict):
            endpoints = {}
            result["endpoints"] = endpoints
        for tier, endpoint in endpoints.items():
            if isinstance(endpoint, dict):
                endpoint.setdefault("profile_id", f"override:{tier}")
        if not isinstance(result.get("step_routes"), dict):
            chain = [name for name in ("primary", "fallback", "emergency") if name in endpoints]
            step_tiers = result.get("step_tiers") or {}
            routes = {}
            for phase in MODEL_PHASES:
                first = str(step_tiers.get(phase) or ("primary" if phase == "reaction" else "fallback"))
                ordered = [first, *[name for name in chain if name != first]]
                routes[phase] = ordered[:3]
            result["step_routes"] = routes
        result["environment_override"] = True
        return result

    @staticmethod
    def _model_fingerprint(profile, connection, environment=None):
        environment = os.environ if environment is None else environment
        env_name = str(connection.get("api_key_env") or "").strip()
        key = environment.get(env_name, "") if env_name else ""
        key = key or str(connection.get("api_key") or "")
        return (
            str(connection.get("url") or "").strip().rstrip("/").lower(),
            str(profile.get("model") or "").strip(),
            key,
        )

    @classmethod
    def resolve_model_route_documents(cls, catalog, routing, *, environment=None):
        """从已校验文档解析路由，供 Runtime 与只读 admission 共用。"""
        profiles = {item["id"]: item for item in catalog["models"]}
        connections = {item["id"]: item for item in catalog["connections"]}
        rows = routing["routes"]
        effective_primary = {}
        primary_source = {}
        for phase in MODEL_PHASES:
            explicit = rows[phase].get("primary")
            if explicit is not None:
                effective_primary[phase] = deepcopy(explicit)
                primary_source[phase] = phase
            elif phase == "reaction":
                effective_primary[phase] = deepcopy(effective_primary.get("setup"))
                primary_source[phase] = primary_source.get("setup")
            elif phase == "cleanup":
                effective_primary[phase] = deepcopy(effective_primary.get("reaction"))
                primary_source[phase] = primary_source.get("reaction")
            else:
                effective_primary[phase] = None
                primary_source[phase] = None

        def resolved_slot(slot, *, source, slot_name):
            if slot is None:
                return None
            model_id = str(slot.get("model_id") or "")
            profile = profiles.get(model_id)
            if profile is None:
                raise ValueError(f"route references unknown model: {model_id}")
            connection = connections.get(profile.get("connection_id"))
            if connection is None:
                raise ValueError(f"model references unknown connection: {model_id}")
            effort = str(slot.get("reasoning_effort") or profile.get("reasoning", {}).get("default") or "")
            supported = profile.get("reasoning", {}).get("supported") or []
            if effort not in SUPPORTED_REASONING_EFFORTS or (supported and effort and effort not in supported):
                raise ValueError(f"unsupported reasoning effort for model: {model_id}")
            return {
                "model_id": model_id,
                "model_alias": profile.get("alias") or profile.get("model"),
                "connection_id": connection["id"],
                "connection_alias": connection.get("alias") or connection["id"],
                "reasoning_effort": effort,
                "source_phase": source,
                "slot": slot_name,
                "inherited": slot_name == "primary" and source != phase,
                "profile": profile,
                "connection": connection,
            }

        phase_routes = {}
        cross_enabled = routing.get("cross_phase_failover_enabled") is not False
        cycle = {
            "setup": ("reaction", "cleanup"),
            "reaction": ("cleanup", "setup"),
            "cleanup": ("setup", "reaction"),
        }
        for phase in MODEL_PHASES:
            candidates = []
            primary = effective_primary.get(phase)
            if primary is not None:
                candidates.append((primary, primary_source.get(phase), "primary"))
            for index, backup in enumerate(rows[phase].get("backups") or []):
                if backup is not None:
                    candidates.append((backup, phase, f"backup_{index + 1}"))
            if cross_enabled:
                for other in cycle[phase]:
                    inherited = effective_primary.get(other)
                    if inherited is not None:
                        candidates.append((inherited, primary_source.get(other), f"cross_phase:{other}"))
            resolved = []
            seen_models = set()
            seen_fingerprints = set()
            for slot, source, slot_name in candidates:
                item = resolved_slot(slot, source=source, slot_name=slot_name)
                if item is None:
                    continue
                fingerprint = cls._model_fingerprint(
                    item["profile"], item["connection"], environment
                )
                if item["model_id"] in seen_models or fingerprint in seen_fingerprints:
                    continue
                seen_models.add(item["model_id"])
                seen_fingerprints.add(fingerprint)
                resolved.append(item)
                if len(resolved) == 3:
                    break
            phase_routes[phase] = resolved
        return {
            "cross_phase_failover_enabled": cross_enabled,
            "effective_primaries": effective_primary,
            "primary_sources": primary_source,
            "phases": phase_routes,
        }

    def resolve_model_routes(self):
        return self.resolve_model_route_documents(
            self.load("models"),
            self.load("model_routing"),
            environment=os.environ,
        )

    @staticmethod
    def _endpoint_from_route(item):
        profile = item["profile"]
        connection = item["connection"]
        protocol = connection["protocol"]
        return {
            "url": connection["url"],
            "model": profile["model"],
            "api_key_env": connection.get("api_key_env", ""),
            "api_key": connection.get("api_key", ""),
            "provider": protocol,
            "api_format": {
                "openai_chat": "chat",
                "openai_responses": "responses",
                "anthropic_messages": "anthropic_messages",
            }[protocol],
            "tool_call_provider": protocol,
            "context_window": profile.get("context_window", 0),
            "output_token_limit": profile.get("output_token_limit", 0),
            "reasoning_effort": item["reasoning_effort"],
            "streaming": deepcopy(profile.get("streaming") or {"enabled": False}),
            "prompt_cache": deepcopy(AUTOMATIC_PROMPT_CACHE),
            "extra_body": deepcopy(profile.get("request_overrides") or {}),
            "profile_id": profile["id"],
            "connection_id": connection["id"],
        }

    @classmethod
    def project_runtime_api_documents(cls, catalog, routing, *, environment=None):
        """把全局模型目录与单位格路由投影为既有 executor 内部合同。"""
        resolved = cls.resolve_model_route_documents(
            catalog, routing, environment=environment
        )
        endpoints = {}
        step_routes = {}
        for phase in MODEL_PHASES:
            keys = []
            for index, item in enumerate(resolved["phases"][phase]):
                key = f"{phase}:{index}:{item['model_id']}"
                endpoints[key] = cls._endpoint_from_route(item)
                keys.append(key)
            step_routes[phase] = keys
        transport = catalog.get("transport") or {}
        return {
            "schema_version": "upsp_runtime_model_projection.v1",
            "endpoints": endpoints,
            "step_routes": step_routes,
            "step_tiers": {
                phase: (keys[0] if keys else "")
                for phase, keys in step_routes.items()
            },
            "handshake": deepcopy(transport.get("handshake") or {}),
            "circuit_breaker": deepcopy(transport.get("circuit_breaker") or {}),
            "environment_override": False,
        }

    def _build_api_projection(self):
        return self.project_runtime_api_documents(
            self.load("models"),
            self.load("model_routing"),
            environment=os.environ,
        )

    def get_active_model_profile_ids(self):
        api = self.load("api")
        result = []
        for phase in MODEL_PHASES:
            for profile_id in self.get_model_profile_ids_for_phase(
                    phase, api=api):
                if profile_id not in result:
                    result.append(profile_id)
        return result

    def get_model_profile_ids_for_phase(self, phase, *, api=None):
        api = api if isinstance(api, dict) else self.load("api")
        endpoints = api.get("endpoints") or {}
        result = []
        for key in (api.get("step_routes") or {}).get(str(phase or ""), []):
            endpoint = endpoints.get(key) or {}
            profile_id = str(endpoint.get("profile_id") or key).strip()
            if profile_id and profile_id not in result:
                result.append(profile_id)
        return result

    def get_round_context_window_tokens(self):
        """Return the minimum positive primary window across setup/reaction/cleanup."""
        api = self.load("api")
        endpoints = api.get("endpoints") or {}
        routes = api.get("step_routes") or {}
        windows = []
        for phase in MODEL_PHASES:
            route = routes.get(phase) if isinstance(routes, dict) else None
            key = str((route or [""])[0] or "").strip()
            endpoint = endpoints.get(key) if key else None
            try:
                window = int((endpoint or {}).get("context_window") or 0)
            except (TypeError, ValueError, OverflowError):
                window = 0
            if window <= 0:
                raise ValueError(f"primary_context_window_unknown:{phase}")
            windows.append(window)
        return min(windows)

    # ==============================================================
    # 便捷方法
    # ==============================================================

    def get_heartbeat_interval(self):
        cfg = self.load("system")
        return cfg["heartbeat"]["interval"]

    def get_token_params(self):
        return {"warning_ratio": self.load("lately")["pressure_ratio"]}

    def get_response_anchor_prompt(self):
        return self.load("system")["response_anchor"]["prompt"].strip()

    def get_context_window_for_endpoint(self, endpoint):
        cfg = self.load("api")
        endpoints = cfg.get("endpoints", {})
        endpoint_id = str(endpoint or "").strip()
        item = endpoints.get(endpoint_id, {}) or {}
        if not item:
            item = next(
                (
                    value for value in endpoints.values()
                    if isinstance(value, dict)
                    and str(value.get("profile_id") or "") == endpoint_id
                ),
                {},
            )
        try:
            window = int(item.get("context_window") or 0)
        except (TypeError, ValueError):
            window = 0
        return window if window > 0 else None


    def get_rhythm_interval(self):
        cfg = self.load("system")
        return cfg.get("rhythm", {}).get("period", 32)

    def get_round_time_milestones(self):
        cfg = self.load("system")
        round_config = cfg.get("round", {})
        values = tuple(round_config.get(key) for key in (
            "reminder_seconds", "warning_seconds", "auto_relay_seconds",
        ))
        if all(
                isinstance(value, int) and not isinstance(value, bool)
                for value in values):
            return values
        return (
            REACTION_REMINDER_SECONDS,
            REACTION_WARNING_SECONDS,
            REACTION_AUTO_RELAY_SECONDS,
        )

    def get_audit_params(self):
        cfg = self.load("system")
        audit = cfg.get("audit", {})
        return {
            "round_snapshot_retention": audit.get("round_snapshot_retention", 8),
            "round_snapshot_max_mib": audit.get("round_snapshot_max_mib", 256),
            "state_backup_retention": audit.get("state_backup_retention", 8),
        }

    def get_request_timeout(self):
        cfg = self.load("api")
        raw = cfg.get("handshake", {}).get("request_timeout_seconds", 180)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 180
        return value if value > 0 else 180

    def get_stream_first_chunk_timeout(self):
        cfg = self.load("api")
        raw = cfg.get("handshake", {}).get(
            "stream_first_chunk_timeout_seconds",
            180,
        )
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 180
        return value if value > 0 else 180

    def get_stream_idle_timeout(self):
        cfg = self.load("api")
        raw = cfg.get("handshake", {}).get(
            "stream_idle_timeout_seconds",
            180,
        )
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 180
        return value if value > 0 else 180

    def get_stream_content_overrun_chars(self):
        cfg = self.load("api")
        raw = cfg.get("handshake", {}).get("stream_content_overrun_chars", 65536)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 65536
        return max(0, value)

    def get_handshake_retry(self):
        cfg = self.load("api")
        raw = cfg.get("handshake", {}).get("retry", 2)
        try:
            value = int(raw)
        except (TypeError, ValueError):
            value = 2
        return min(2, max(0, value))

    def get_standby_threshold(self):
        cfg = self.load("system")
        return cfg.get("standby", {}).get("idle_threshold_min", 30)


    def get_general_tool_window_params(self):
        """通用信息获取工具 bounded 窗口配置。"""
        cfg = self.load("system")
        tools = cfg.get("general_tools", {})
        return {
            "file_read_window_chars": tools.get("file_read_window_chars", 16384),
            "web_fetch_window_chars": tools.get("web_fetch_window_chars", 4096),
            "web_search_window_results": tools.get("web_search_window_results", 5),
        }

    def get_execution_permission_level(self):
        """通用执行权限档位：limited=只读，guarded=受限，unlimited=放行。"""
        cfg = self.load("system")
        permission = cfg.get("execution_permission", {})
        if isinstance(permission, dict):
            return permission.get("level", "unlimited")
        return "unlimited"

    def get_api_endpoint(self, tier="primary"):
        cfg = self.load("api")
        endpoints = cfg.get("endpoints", {})
        direct = endpoints.get(tier, {})
        if direct:
            return direct
        return next(
            (
                value for value in endpoints.values()
                if isinstance(value, dict)
                and str(value.get("profile_id") or "") == str(tier or "")
            ),
            {},
        )


    def get_memory_privacy_declassify_config(self):
        """Dormant privacy-memory configuration; Seed keeps both entries disabled."""
        cfg = self.load("memory").get("privacy_declassify", {})
        return {
            "manual_enabled": cfg.get("manual_enabled", False),
            "auto_enabled": cfg.get("auto_enabled", False),
            "frequency": cfg.get("frequency", "monthly"),
            "auto_modes": cfg.get("auto_modes", ["redact"]),
            "max_items_per_run": cfg.get("max_items_per_run", 20),
            "requires_review_modes": cfg.get("requires_review_modes", ["delete"]),
        }

    def get_relation_params(self):
        """P1-6: 关系域参数（DDS §9 / config/relation.json）"""
        cfg = self.load("relation")
        return {"max_slots": cfg["relation_context"]["max_slots"]}

    def get_relation_card_write_guard(self):
        """关系卡写入护栏配置。Base 默认关闭大改动拦截。"""
        cfg = self.load("relation")
        return dict(cfg["relation_card_write"])

    def get_periodic_limits(self):
        """定期层字符限额（DDS §21.1 / §19.5）"""
        cfg = self.load("periodic")
        return {"periodic_memory_items_chars": cfg["limits"]["periodic_memory_items_chars"]}

    def get_lately_cache_params(self):
        """Spec760 最近缓存渐进压缩参数。"""
        cfg = self.load("lately")
        return {
            "pressure_ratio": float(cfg["pressure_ratio"]),
            "protected_interaction_count": int(
                cfg["protected_interaction_count"]),
            "semantic_summary_ratio": float(cfg["semantic_summary_ratio"]),
            "cycle_target_ratio": float(cfg["cycle_target_ratio"]),
            "batch_source_chars": int(cfg["batch_source_chars"]),
        }

    def get_context_persistent_lanes(self):
        """Spec625 持久语料轨道；C 轨由 ContextStore 专用入口承接。"""
        cfg = self.load("now")
        source = cfg["persistent_lanes"]
        result = {}
        for lane in ("now_lately_raw", "now_lately_no_raw"):
            kinds = source.get(lane, []) if isinstance(source, dict) else []
            result[lane] = [
                kind for kind in kinds if isinstance(kind, str) and kind
            ]
        return result

    def get_lately_allowed_kinds(self):
        """最近缓存允许进入履带的语料 kind（DDS §19 / Spec 038）。"""
        cfg = self.load("lately")
        kinds = cfg["allowed_kinds"]
        return [kind for kind in kinds if isinstance(kind, str) and kind]

    def get_high_freq_params(self):
        """高频层参数（DDS §19.3 / §19.5）"""
        cfg = self.load("high_freq")
        return {
            "index_display_limits": cfg["index_display_limits"],
            "reference_window_chars": cfg["content_limits"]["reference_window_chars"],
        }
