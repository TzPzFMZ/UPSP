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
import os
import re
from copy import deepcopy
from urllib.parse import urlparse

from data.atomic_write import atomic_write_json
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
from errors import ReadError
from constants import REACTION_TIME_LIMIT

API_CONFIG_OVERRIDE_ENV = "UPSP_API_CONFIG_OVERRIDE_JSON"
MODEL_PHASES = ("setup", "reaction", "cleanup")
SUPPORTED_PROVIDER_PROTOCOLS = {
    "openai_chat", "openai_responses", "anthropic_messages",
}
SUPPORTED_REASONING_EFFORTS = {
    "", "none", "minimal", "low", "medium", "high", "xhigh", "max", "ultra",
}
SUPPORTED_PROMPT_CACHE_PROFILES = {
    "off", "key_only", "gpt56_explicit_permanent", "gpt56_explicit_tiered",
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


class ConfigStore:
    """配置文件读写管理"""

    def __init__(self, use_api_environment=True):
        self.use_api_environment = bool(use_api_environment)

    # ==============================================================
    # 通用读写
    # ==============================================================

    def load(self, name):
        """读取指定配置文件。文件不存在返回默认模板"""
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
        path, default_fn = _CONFIG_MAP[name]

        try:
            if not os.path.isfile(path):
                loaded = default_fn()
            else:
                with open(path, "r", encoding="utf-8") as f:
                    loaded = json.load(f)
            if not isinstance(loaded, dict):
                raise ValueError("config must be a JSON object")
            self._validate(name, loaded)
            return loaded
        except (json.JSONDecodeError, OSError, ValueError) as e:
            raise ReadError(path, cause=e)

    def save(self, name, data):
        """写入指定配置文件（原子）"""
        if name not in _CONFIG_MAP:
            raise ValueError(f"未知配置: {name}")
        path, _ = _CONFIG_MAP[name]
        self._validate(name, data)
        atomic_write_json(path, data)

    def revision(self, name):
        """Return the canonical file-byte SHA used by the GUI write boundary."""
        if name == "api":
            payload = json.dumps(
                self.load("api"), ensure_ascii=False, sort_keys=True
            ).encode("utf-8")
            return hashlib.sha256(payload).hexdigest()
        if name not in _CONFIG_MAP:
            raise ValueError(f"未知配置: {name}")
        path, default_fn = _CONFIG_MAP[name]
        try:
            if os.path.isfile(path):
                with open(path, "rb") as f:
                    payload = f.read()
            else:
                payload = json.dumps(
                    default_fn(), ensure_ascii=False, sort_keys=True
                ).encode("utf-8")
            return hashlib.sha256(payload).hexdigest()
        except OSError as e:
            raise ReadError(path, cause=e)

    def init_all(self):
        """初始化缺失配置；旧 api.json 只在新模型真源缺失时读取一次。"""
        created = []
        models_missing = not os.path.isfile(GLOBAL_MODELS_CONFIG)
        routing_missing = not os.path.isfile(CONFIG_MODEL_ROUTING)
        migrated_models = None
        migrated_routing = None
        if (models_missing or routing_missing) and os.path.isfile(LEGACY_CONFIG_API):
            legacy = self._read_json_object(LEGACY_CONFIG_API)
            migrated_models, migrated_routing = self._migrate_legacy_api(legacy)

        for name, (path, default_fn) in _CONFIG_MAP.items():
            if not os.path.isfile(path):
                value = default_fn()
                if name == "models" and migrated_models is not None:
                    value = migrated_models
                elif (
                    name == "model_routing"
                    and migrated_routing is not None
                    and models_missing
                ):
                    value = migrated_routing
                self.save(name, value)
                created.append(name)
            else:
                self.load(name)  # 损坏文件必须显式失败，禁止静默覆盖
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
                "reasoning_effort": str(endpoint.get("reasoning_effort") or ""),
                "streaming": deepcopy(endpoint.get("streaming") or {"enabled": False}),
                "prompt_cache": deepcopy(endpoint.get("prompt_cache") or {"profile": "off"}),
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
            "reasoning_effort": item["reasoning_effort"],
            "streaming": deepcopy(profile.get("streaming") or {"enabled": False}),
            "prompt_cache": deepcopy(profile.get("prompt_cache") or {"profile": "off"}),
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
            for key in (api.get("step_routes") or {}).get(phase, []):
                endpoint = (api.get("endpoints") or {}).get(key) or {}
                profile_id = str(endpoint.get("profile_id") or key)
                if profile_id and profile_id not in result:
                    result.append(profile_id)
        return result

    # ==============================================================
    # 便捷方法
    # ==============================================================

    def get_heartbeat_interval(self):
        cfg = self.load("system")
        return cfg.get("heartbeat", {}).get("interval", 5)

    def get_token_params(self):
        cfg = self.load("system")
        token = cfg.get("token_usage") or cfg.get("token", {})
        return {
            "warning_ratio": token.get("warning_ratio", 0.7),
            "urgent_ratio": token.get("urgent_ratio", token.get("critical_ratio", 0.85)),
        }

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

    def get_round_time_limit(self):
        cfg = self.load("system")
        raw = cfg.get("round", {}).get("time_limit", 300)
        try:
            if raw and isinstance(raw, (int, float)) and raw > 0:
                return int(raw)
        except (TypeError, ValueError, OverflowError):
            pass
        return REACTION_TIME_LIMIT

    def get_audit_params(self):
        cfg = self.load("system")
        audit = cfg.get("audit", {})
        return {
            "round_snapshot_retention": audit.get("round_snapshot_retention", 8),
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


    def get_autonomous_trigger_params(self):
        """自主轮触发参数（DDS §25.4 / config/system.json autonomous_trigger）"""
        cfg = self.load("system")
        trigger = cfg.get("autonomous_trigger", {})
        return {
            "tacit_pending_threshold": trigger.get("tacit_pending_threshold", 512),
            "connection_pending_threshold": trigger.get("connection_pending_threshold", 512),
        }

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
        """通用执行权限档位：limited=受限档，unlimited=放行档。"""
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
        focus = cfg.get("relation_focus", {})
        return {
            "max_slots": focus.get("max_slots", 3),
        }

    def get_relation_card_write_guard(self):
        """关系卡写入护栏配置。Base 默认关闭大改动拦截。"""
        cfg = self.load("relation")
        guard = cfg.get("relation_card_write", {})
        return {
            "single_declaration": guard.get("single_declaration", True),
            "single_target": guard.get("single_target", True),
            "large_delta_guard": guard.get("large_delta_guard", False),
            "max_delta_chars": guard.get("max_delta_chars", 800),
        }

    def get_periodic_limits(self):
        """定期层字符限额（DDS §21.1 / §19.5）"""
        cfg = self.load("periodic")
        limits = cfg.get("limits", {})
        return {
            "periodic_memory_items_chars": limits.get("periodic_memory_items_chars", 65536),
        }

    def get_now_cache_params(self):
        """当前缓存字符窗口参数（DDS §19 / Spec 061）"""
        return self._cache_params("now", 65536, 16384)

    def get_lately_cache_params(self):
        """最近缓存字符窗口参数（DDS §19 / Spec 061）"""
        return self._cache_params("lately", 262144, 65536)

    def _cache_params(self, name, default_budget, default_trim):
        cfg = self.load(name)
        try:
            budget = max(1, int(cfg.get("budget_chars", default_budget)))
        except (TypeError, ValueError):
            budget = default_budget
        try:
            trim = max(1, int(cfg.get("trim_chars", default_trim)))
        except (TypeError, ValueError):
            trim = default_trim
        return {"budget_chars": budget, "trim_chars": min(trim, budget)}

    def get_lately_compact_ratio(self):
        """最近缓存删后幸存段语义压缩比例（DDS §19 / Spec 063）"""
        return self.get_lately_compaction_params()["compact_ratio"]

    def get_lately_compaction_params(self):
        """最近缓存压缩节律参数（DDS §19 / Spec463）"""
        cfg = self.load("lately")

        def ratio(key, default):
            try:
                value = float(cfg.get(key, default))
            except (TypeError, ValueError):
                value = default
            return min(1.0, max(0.0, value))

        def integer(key, default):
            try:
                value = int(cfg.get(key, default))
            except (TypeError, ValueError):
                value = default
            return max(1, value)

        return {
            "compact_ratio": ratio("compact_ratio", 0.618),
            "compact_shard_chars": integer("compact_shard_chars", 8192),
            "compact_shard_ratio": ratio("compact_shard_ratio", 0.314),
        }

    def get_now_policy_by_kind(self):
        """当前缓存各 kind 的进入策略（DDS §19 / Spec 038）。"""
        cfg = self.load("now")
        policies = cfg.get("policy_by_kind", {})
        return {kind: dict(policy) for kind, policy in policies.items()}

    def get_context_persistent_lanes(self):
        """Spec625 持久语料轨道；C 轨由 ContextStore 专用入口承接。"""
        cfg = self.load("now")
        source = cfg.get("persistent_lanes", {})
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
        kinds = cfg.get("allowed_kinds", [])
        return [kind for kind in kinds if isinstance(kind, str) and kind]

    def get_high_freq_params(self):
        """高频层参数（DDS §19.3 / §19.5）"""
        cfg = self.load("high_freq")
        return {
            "index_display_limits": cfg.get("index_display_limits", {}),
            "reference_window_chars": cfg.get("content_limits", {}).get("reference_window_chars", 65536),
        }
