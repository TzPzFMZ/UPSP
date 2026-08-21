"""
配置文件 Schema
DDS §32 config/

文件:
  config/system.json   — 系统参数（心跳/节律/审计/连接记录）
  ../../config/models.json — UPSP 全局服务连接、模型库与传输参数
  ../../config/interface.json — UPSP 全局界面语言
  config/model_routing.json — 当前位格三阶段模型路由
  config/memory.json   — 记忆参数（衰减速率/配额/阈值）
  config/media.json    — 媒体参数
  config/relation.json — 关系参数
  config/context/periodic.json — 定期记忆投影限额（periodic_memory_items_chars）
"""
import json
from pathlib import Path


_OS_TEMPLATE_CONFIG_DIR = (
    Path(__file__).resolve().parents[2] / "initialization" / "os_template" / "config"
)


def load_os_template_config(relative_path):
    """读取 tracked 单位格配置模板；这里不维护第二份 Python 默认对象。"""
    path = _OS_TEMPLATE_CONFIG_DIR / relative_path
    with path.open("r", encoding="utf-8") as handle:
        value = json.load(handle)
    if not isinstance(value, dict):
        raise ValueError(f"配置模板必须是 JSON object: {path}")
    return value

# ============================================================
# system.json（DDS §32）
# ============================================================

SYSTEM_CONFIG_FIELDS = {
    "heartbeat.interval":         ("int",  "心跳 tick 间隔秒数，默认5"),
    "standby.idle_threshold_min": ("int",  "待命轮触发空闲分钟数，默认30"),
    "round.reminder_seconds":     ("int",  "Reaction 在场提醒时间，默认600秒"),
    "round.warning_seconds":      ("int",  "Reaction 收束警告时间，默认1200秒"),
    "round.auto_relay_seconds":   ("int",  "Reaction 自动中继时间，默认1800秒"),
    "rhythm.period":              ("int",  "节律周期轮数，默认32"),
    "token_usage.watermark_policy_version": ("int", "lately watermark policy migration marker"),
    "response_anchor.prompt":     ("str",  "每个 Reaction Frame 的 STATUSBAR 回答锚点；仅提示，不执行回复判罚"),
    "audit.round_snapshot_retention": ("int", "round_{N}.jsonl FIFO retained audit streams"),
    "audit.round_snapshot_max_mib": ("int", "round audit total-size soft limit in MiB"),
    "audit.round_snapshot_policy_version": ("int", "round audit retention policy migration marker"),
    "audit.state_backup_retention": ("int", "STM/buffer/state_backups.jsonl FIFO retained rows"),
    "general_tools.file_read_window_chars": ("int", "通用 file_read 自适应 bounded 字符窗口上限，默认16384"),
    "general_tools.web_fetch_window_chars": ("int", "通用 web_fetch bounded 字符窗口，默认4096"),
    "general_tools.web_search_window_results": ("int", "通用 web_search 结果窗口，默认5"),
    "connectivity.max_latency_records": ("int", "连通性记录 FIFO 上限"),
}

LATELY_CONFIG_FIELDS = {
    "pressure_ratio": ("float", "压缩触发比例，默认0.9"),
    "protected_interaction_count": ("int", "保护最近用户交互原文数量，默认16"),
    "semantic_summary_ratio": ("float", "单分片语义摘要上限比例，默认0.125"),
    "cycle_target_ratio": ("float", "单周期 lately 目标比例，默认0.25"),
    "batch_source_chars": ("int", "单 Reaction Frame 源材料字符上限，默认65536"),
}


def default_system_config():
    return load_os_template_config("system.json")


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
                "prompt_cache": {"profile": "automatic_tiered"},
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
                "prompt_cache": {"profile": "automatic_tiered"},
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
                "prompt_cache": {"profile": "automatic_tiered"},
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
    return load_os_template_config("model_routing.json")


# ============================================================
# memory.json（DDS §9）
# ============================================================

def default_memory_config():
    return load_os_template_config("memory.json")


# ============================================================
# media.json
# ============================================================

def default_media_config():
    return load_os_template_config("media.json")


# ============================================================
# relation.json
# ============================================================

def default_relation_config():
    return load_os_template_config("relation.json")


# ============================================================
# context/ 六层装配规则（DDS §32）
# ============================================================

def default_permanent_config():
    return load_os_template_config("context/permanent.json")


def default_periodic_config():
    return load_os_template_config("context/periodic.json")


def default_high_freq_config():
    return load_os_template_config("context/high_freq.json")


def default_now_config():
    return load_os_template_config("context/now.json")


def default_lately_config():
    return load_os_template_config("context/lately.json")


def default_popup_config():
    return load_os_template_config("context/popup.json")
