"""Execution permission levels for general tools."""

import os


EXECUTION_PERMISSION_ENV = "UPSP_EXECUTION_PERMISSION_LEVEL"
LIMITED = "limited"
UNLIMITED = "unlimited"
DEFAULT_LEVEL = UNLIMITED

LEVEL_LABELS = {
    LIMITED: "受限档",
    UNLIMITED: "放行档",
}

LIMITED_BLOCKED_TOOLS = {
    "file_edit",
    "file_write",
    "shell_command",
    "subagent_dispatch",
}


def normalize_execution_permission_level(value, default=DEFAULT_LEVEL):
    text = str(value or "").strip().lower()
    if text in {LIMITED, "restricted", "safe", "read_only", "readonly"}:
        return LIMITED
    if text in {UNLIMITED, "full", "allow", "allow_all", "write"}:
        return UNLIMITED
    return default


def execution_permission_label(level):
    return LEVEL_LABELS.get(
        normalize_execution_permission_level(level),
        LEVEL_LABELS[DEFAULT_LEVEL],
    )


def load_execution_permission_level(config_store=None, default=DEFAULT_LEVEL):
    env_value = os.environ.get(EXECUTION_PERMISSION_ENV)
    if str(env_value or "").strip():
        return normalize_execution_permission_level(env_value, default=default)
    if config_store is None:
        from data.config_store import ConfigStore

        config_store = ConfigStore()
    getter = getattr(config_store, "get_execution_permission_level", None)
    if callable(getter):
        return normalize_execution_permission_level(getter(), default=default)
    cfg = config_store.load("system")
    raw = (cfg.get("execution_permission") or {}).get("level")
    return normalize_execution_permission_level(raw, default=default)


def tool_allowed_by_execution_permission(tool_id, level):
    normalized = str(tool_id or "").strip()
    level = normalize_execution_permission_level(level)
    if level != LIMITED:
        return True
    return normalized not in LIMITED_BLOCKED_TOOLS


def execution_permission_audit(level):
    normalized = normalize_execution_permission_level(level)
    return {
        "permission_level": normalized,
        "permission_label": execution_permission_label(normalized),
    }


def render_execution_permission_status(level=None):
    normalized = normalize_execution_permission_level(level)
    return f"执行权限：{execution_permission_label(normalized)}"


def render_execution_permission_guide(level=None):
    normalized = normalize_execution_permission_level(level)
    if normalized == LIMITED:
        return (
            "### 执行权限：受限档\n"
            "当前不开放 file_edit、file_write、shell_command、subagent_dispatch。"
            "如任务需要写文件、改文件、运行命令或调用子代理，先说明需要放行档；"
            "不要假装已经完成这些动作。"
        )
    return (
        "### 执行权限：放行档\n"
        "当前可在工作区内写文件、改文件、运行安全命令和调用子代理。"
        "位格真源、Git 内部数据、密钥类路径和灾难命令仍会被 Runtime 硬拒绝。"
    )
