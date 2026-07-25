"""
配置文件 Schema
DDS §32 config/

文件:
  config/system.json   — 系统参数（心跳间隔/时区/休眠/连接超时）
  ../../config/models.json — UPSP 全局服务连接、模型库与传输参数
  ../../config/interface.json — UPSP 全局界面语言
  config/model_routing.json — 当前位格三阶段模型路由
  config/memory.json   — 记忆参数（衰减速率/配额/阈值）
  config/media.json    — 媒体参数
  config/relation.json — 关系参数
  config/context/periodic.json — 定期记忆投影限额（periodic_memory_items_chars）
"""
from datetime import timezone, timedelta

TZ = timezone(timedelta(hours=8))

# ============================================================
# system.json（DDS §32）
# ============================================================

SYSTEM_CONFIG_FIELDS = {
    "heartbeat.interval":         ("int",  "心跳 tick 间隔秒数，默认5"),
    "timezone":                   ("str",  "时区，默认 Asia/Shanghai"),
    "sleep.light_after_min":      ("int",  "轻度休眠触发分钟数"),
    "sleep.moderate_after_min":   ("int",  "中度休眠触发分钟数"),
    "sleep.deep_after_min":       ("int",  "深度休眠触发分钟数"),
    "standby.idle_threshold_min": ("int",  "待命轮触发空闲分钟数，默认30"),
    "rhythm.period":              ("int",  "节律周期轮数，默认32"),
    "identity.timeout_seconds":   ("int",  "身份确认超时秒数，默认3600"),
    "token_usage.warning_ratio":  ("float","token 预警比例，默认0.7"),
    "token_usage.critical_ratio": ("float","token 紧急比例，默认0.85"),
    "fatigue.force_sleep_hours":  ("int",  "连续清醒强制疲劳触发小时数；0 表示关闭"),
    "autonomous_trigger.tacit_pending_threshold": ("int", "默契集 pending 行数触发阈值，默认512"),
    "autonomous_trigger.connection_pending_threshold": ("int", "联系集 pending 行数触发阈值，默认512"),
    "audit.round_snapshot_retention": ("int", "round_{N}.jsonl FIFO retained audit streams"),
    "audit.state_backup_retention": ("int", "STM/buffer/state_backups.jsonl FIFO retained rows"),
    "general_tools.file_read_window_chars": ("int", "通用 file_read 自适应 bounded 字符窗口上限，默认16384"),
    "general_tools.web_fetch_window_chars": ("int", "通用 web_fetch bounded 字符窗口，默认4096"),
    "general_tools.web_search_window_results": ("int", "通用 web_search 结果窗口，默认5"),
}


def default_system_config():
    return {
        "_comment": "UPSP Base 系统配置文件",
        "heartbeat": {
            "interval": 5,
        },
        "timezone": "Asia/Shanghai",
        "sleep": {
            "light_after_min": 15,
            "moderate_after_min": 45,
            "deep_after_min": 120,
        },
        "standby": {
            "idle_threshold_min": 30,
        },
        "rhythm": {
            "period": 32,
        },
        "identity": {
            "timeout_seconds": 3600,
        },
        "token_usage": {
            "warning_ratio": 0.7,
            "critical_ratio": 0.85,
            "forced_rhythm_on_usage": False,
        },
        "fatigue": {
            "force_sleep_hours": 0,
        },
        "round": {
            "time_limit": 600,
        },
        "autonomous_trigger": {
            "tacit_pending_threshold": 512,
            "connection_pending_threshold": 512,
        },
        "audit": {
            "round_snapshot_retention": 8,
            "state_backup_retention": 8,
        },
        "general_tools": {
            "file_read_window_chars": 16384,
            "web_fetch_window_chars": 4096,
            "web_search_window_results": 5,
        },
        "execution_permission": {
            "level": "unlimited",
        },
    }


# ============================================================
# 旧 api.json（仅供升级迁移）
# ============================================================

def legacy_default_api_config():
    return {
        "_comment": "API 端点配置（三档：主力/备用/应急）",
        "endpoints": {
            "primary": {
                "url": "",
                "model": "",
                "api_key_env": "UPSP_PRIMARY_KEY",
                "api_key": "",
                "provider": "openai_chat",
                "api_format": "chat",
                "tool_call_provider": "openai_chat",
                "context_window": 0,
                "reasoning_effort": "",
                "streaming": {
                    "enabled": False,
                    "protocol": "openai_sse",
                    "include_usage": True,
                },
                "prompt_cache": {"profile": "off"},
            },
            "fallback": {
                "url": "",
                "model": "",
                "api_key_env": "UPSP_FALLBACK_KEY",
                "api_key": "",
                "provider": "openai_chat",
                "api_format": "chat",
                "tool_call_provider": "openai_chat",
                "context_window": 0,
                "reasoning_effort": "",
                "streaming": {
                    "enabled": False,
                    "protocol": "openai_sse",
                    "include_usage": True,
                },
                "prompt_cache": {"profile": "off"},
            },
            "emergency": {
                "url": "",
                "model": "",
                "api_key_env": "UPSP_EMERGENCY_KEY",
                "api_key": "",
                "provider": "openai_chat",
                "api_format": "chat",
                "tool_call_provider": "openai_chat",
                "context_window": 0,
                "reasoning_effort": "",
                "streaming": {
                    "enabled": False,
                    "protocol": "openai_sse",
                    "include_usage": True,
                },
                "prompt_cache": {"profile": "off"},
            },
        },
        "step_tiers": {
            "setup": "fallback",
            "reaction": "primary",
            "cleanup": "fallback",
        },
        "circuit_breaker": {
            "max_failures": 3,
            "cooldown_seconds": 900,
        },
        "handshake": {
            "timeout_seconds": 10,
            "retry": 2,
            "request_timeout_seconds": 180,
            "stream_first_chunk_timeout_seconds": 180,
            "stream_idle_timeout_seconds": 180,
        },
    }


# ============================================================
# LocalAppData 全局配置与当前活动位格 OS/config（DDS §21 / §32）
# ============================================================

def default_interface_config():
    """跨位格界面设置；system 表示每次按浏览器语言裁决。"""
    return {
        "schema_version": "upsp_interface_settings.v1",
        "locale": "system",
    }


def default_models_config():
    """全局模型库与共享传输参数。密钥只允许存在于 ignored 本机文件。"""
    return {
        "schema_version": "upsp_model_catalog.v1",
        "connections": [],
        "models": [],
        "transport": {
            "handshake": {
                "timeout_seconds": 10,
                "retry": 2,
                "request_timeout_seconds": 180,
                "stream_first_chunk_timeout_seconds": 180,
                "stream_idle_timeout_seconds": 180,
                "stream_content_overrun_chars": 65536,
            },
            "circuit_breaker": {
                "max_failures": 3,
                "cooldown_seconds": 900,
            },
        },
    }


def default_model_routing_config():
    """单位格三阶段三槽位路由；主模型空值按阶段向下继承。"""
    def empty_row():
        return {"primary": None, "backups": [None, None]}

    return {
        "schema_version": "upsp_persona_model_routing.v1",
        "cross_phase_failover_enabled": True,
        "routes": {
            "setup": empty_row(),
            "reaction": empty_row(),
            "cleanup": empty_row(),
        },
    }


# ============================================================
# memory.json（DDS §9）
# ============================================================

def default_memory_config():
    return {
        "_comment": "记忆系统参数",
        "decay_rates": {
            "显著": -5,
            "未定": -10,
            "衰减": -15,
        },
        "zone_thresholds": {
            "显著": 70,
            "未定": 40,
        },
        "upgrade": {
            "AH_high_min": 5,
        },
        "ltm_limits": {
            "Full": 2048,
            "Summary": 512,
            "Abstract": 128,
        },
        "recall_boost": 10,
        "initial_heat": 50,
        "max_heat": 100,
        "privacy_declassify": {
            "manual_enabled": False,
            "auto_enabled": False,
            "frequency": "monthly",
            "auto_modes": ["redact"],
            "max_items_per_run": 20,
            "requires_review_modes": ["delete"],
        },
    }


# ============================================================
# media.json
# ============================================================

def default_media_config():
    return {
        "_comment": "媒体参数",
        "max_attachments": 4,
        "max_size_mb": 10,
        "allowed_types": ["image/png", "image/jpeg", "image/gif", "text/plain"],
    }


# ============================================================
# relation.json
# ============================================================

def default_relation_config():
    return {
        "_comment": "关系系统参数",
        "gravity": {
            "core_to_dynamic": 2,   # 核心引力五档 ±2
            "relation_to_dynamic": 1,  # 关系引力三档 ±1×2
        },
        "comfort_zone_range": [-40, 40],
        "max_axes_per_round": 3,
        "relation_card_write": {
            "single_declaration": True,
            "single_target": True,
            "large_delta_guard": False,
            "max_delta_chars": 800,
        },
    }


# ============================================================
# context/ 六层装配规则（DDS §32）
# ============================================================

def default_permanent_config():
    """永固层装配规则"""
    return {
        "_comment": "永固层装配规则（Attention Sink区）",
        "_version": "Base-0.10.0",
        "_dds_ref": "§19.2 / §19.7 / §22.4",
        "layer": {
            "frequency": "permanent",
            "refresh": "session",
            "cache_hit_rate": "~100%",
            "attention_position": "first (Attention Sink)",
        },
        "content": {
            "resident_rules": [
                "manifesto.md", "boundaries.md",
                "modes.md (协议层)", "round.md (基础部分)",
            ],
            "step_rules": {
                "setup": "setup.md",
                "reaction": "reaction.md",
                "cleanup": "cleanup.md",
            },
        },
        "trim_policy": "never",
    }


def default_periodic_config():
    """定期层装配规则"""
    return {
        "_comment": "定期层装配规则（32轮级刷新，periodic_mounts.json机器源）",
        "_version": "Base-0.10.0",
        "_dds_ref": "§19.5 / §21.1",
        "layer": {
            "frequency": "periodic",
            "refresh": "rhythm_round",
            "cache_hit_rate": "~100%",
            "attention_position": "mid-front (after permanent)",
        },
        "limits": {
            "periodic_memory_items_chars": 65536,
        },
        "content": {
            "machine_source": "periodic_mounts.json",
        },
        "trim_policy": "trim_after_popup_and_rules",
    }


def default_high_freq_config():
    """高频层装配规则"""
    return {
        "_comment": "高频层装配规则（每轮刷新）",
        "_version": "Base-0.10.0",
        "_dds_ref": "§19.3 / §19.5 / §21.1",
        "layer": {
            "frequency": "high_freq",
            "refresh": "every_round",
            "cache_hit_rate": "~0%",
            "attention_position": "mid-rear",
        },
        "index_display_limits": {
            "container_index": 1,
            "ltm_heat_index": 16,
            "stm_heat_index": 16,
            "skills_inverted": 8,
            "relation_inverted": 8,
            "relation_domain": 8,
            "ltm_inverted": 8,
            "stm_inverted": 8,
            "association_index": 8,
        },
        "content_limits": {
            "reference_window_chars": 65536,
        },
        "trim_policy": "by_module_tag",
    }


def default_now_config():
    """当前缓存 now 主源配置"""
    return {
        "layer": "now",
        "description": "当前缓存：当前步/当前轮高注意力语料块集合；主源为 STM/context/cache/now_cache.jsonl",
        "store": {
            "path": "STM/context/cache/now_cache.jsonl",
            "format": "jsonl",
            "schema": "corpus_block.v1",
        },
        "allowed_kinds": [
            "interaction",
            "assistant_reply",
            "dialogue_progress",
            "material",
            "reasoning_context",
            "tool_fact",
            "setup_fact",
            "relay_handoff",
            "minimum_commitment",
            "fault_note",
        ],
        "budget_chars": 65536,
        "trim_chars": 16384,
        "fifo_policy": "complete_block",
        "persistent_lanes": {
            "now_lately_raw": [
                "interaction", "assistant_reply", "dialogue_progress",
                "tool_fact", "setup_fact", "relay_handoff",
                "minimum_commitment", "fault_note",
            ],
            "now_lately_no_raw": ["material"],
        },
        "policy_by_kind": {
            "interaction": {"now": True, "lately": True},
            "assistant_reply": {"now": True, "lately": True},
            "dialogue_progress": {"now": True, "lately": True},
            "tool_fact": {"now": True, "lately": True},
            "setup_fact": {"now": True, "lately": True},
            "relay_handoff": {"now": True, "lately": True},
            "minimum_commitment": {"now": True, "lately": True},
            "fault_note": {"now": True, "lately": True},
            "material": {"now": True, "lately": True},
        },
        "source_kinds": {
            "interaction": "用户/频道交互语料，携带 ref.interaction 三字段",
            "assistant_reply": "反应步对外回复或可进入语料履带的回复摘要",
            "dialogue_progress": "反应步用户可见轮中进展，不是私有笔记、资料正文、工具事实或最终回复",
            "material": "文件、网页、搜索、图片说明等外部只读资料；走 now→lately、不进 raw 的 B 轨",
            "reasoning_context": "接口推理续接，仅走单次目标 reaction 调用的 C 轨",
            "tool_fact": "工具执行或协议动作的短事实条，只保留模型继续工作需要的信息",
            "setup_fact": "起手步产生的短事实证明，以自然语言进入语料履带",
            "relay_handoff": "下一轮 relay setup 从 relay_intents[] 投影的跨轮交接任务，role=user，但不是用户原始输入；同轮 handoff_text 不直接写 cache",
            "minimum_commitment": "善后步最小承诺边界语料",
            "fault_note": "故障记账语料块，alerts.md 同步保留告警索引",
        },
    }


def default_lately_config():
    """最近缓存 lately 主源配置"""
    return {
        "layer": "lately",
        "description": "最近缓存：允许进入语料履带的近期语料块集合；主源为 STM/context/cache/lately_cache.jsonl",
        "store": {
            "path": "STM/context/cache/lately_cache.jsonl",
            "format": "jsonl",
            "schema": "corpus_block.v1",
        },
        "allowed_kinds": [
            "interaction",
            "assistant_reply",
            "dialogue_progress",
            "tool_fact",
            "setup_fact",
            "relay_handoff",
            "minimum_commitment",
            "fault_note",
            "cache_summary",
            "material",
        ],
        "budget_chars": 262144,
        "trim_chars": 65536,
        "compact_ratio": 0.618,
        "compact_shard_chars": 8192,
        "compact_shard_ratio": 0.314,
        "fifo_policy": "complete_block",
        "retired_round_window": {
            "window_by_step": "retired by Spec 061; lately length is character-managed",
            "retention_rounds": "retired by Spec 061; batch deletion uses trim_chars",
        },
    }


def default_popup_config():
    """POPUP装配规则"""
    return {
        "_comment": "POPUP装配规则（事件驱动，messages绝对末位）",
        "_version": "Base-0.10.0",
        "_dds_ref": "§19.3 / §21.1 / §24",
        "layer": {
            "frequency": "event",
            "refresh": "event_driven",
            "cache_hit_rate": "~0%",
            "attention_position": "absolute_last (strongest attention)",
        },
        "lifecycle": {
            "states": ["active", "consumed", "expired"],
        },
        "popup_types": [
            "api_fault", "process_anomaly",
            "identity_prompt", "security_review", "structure_warning", "failover",
        ],
        "merge_rule": "多事件合并为一条POPUP，按kind区分义务；安全裁决类按严重程度靠后显示",
        "budget_chars": 500,
    }
