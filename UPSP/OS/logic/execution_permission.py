"""Execution permission levels for general tools."""

import os


EXECUTION_PERMISSION_ENV = "UPSP_EXECUTION_PERMISSION_LEVEL"
LIMITED = "limited"
GUARDED = "guarded"
UNLIMITED = "unlimited"
DEFAULT_LEVEL = GUARDED

LEVEL_LABELS = {
    LIMITED: "只读",
    GUARDED: "受限",
    UNLIMITED: "放行",
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
    if text in {GUARDED, "approval", "approve", "ask"}:
        return GUARDED
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


class ExecutionPermissionChain:
    """Keep one explicit permission across its interaction/relay chain only."""

    def __init__(self, *components):
        self.components = components
        self.pending = None
        self.continuation = None
        self.current = self.apply(DEFAULT_LEVEL)

    def apply(self, level):
        self.current = normalize_execution_permission_level(level)
        for component in self.components:
            setattr(component, "execution_permission_level", self.current)
        return self.current

    def consume(self, messages, flags):
        if messages:
            level, self.pending = self.pending, None
            return normalize_execution_permission_level(level)
        if (flags or {}).get("continue_requested"):
            return normalize_execution_permission_level(self.continuation)
        return DEFAULT_LEVEL

    def queue(self, level):
        self.pending = normalize_execution_permission_level(level)

    def authorize(self, level):
        self.continuation = normalize_execution_permission_level(level)
        return self.continuation

    def cancel_pending(self):
        self.pending = None

    def finish(self, level, result, state_store):
        try:
            flags = state_store.get_flags()
        except Exception:
            flags = {}
        settlement = str((result.get("_settlement") or {}).get("status") or "")
        self.continuation = (
            normalize_execution_permission_level(level)
            if not result.get("_user_stop_requested")
            and settlement not in {"degraded", "unsettled"}
            and bool(flags.get("continue_requested"))
            else None
        )
        return self.apply(DEFAULT_LEVEL)


def render_execution_permission_status(level=None):
    normalized = normalize_execution_permission_level(level)
    return f"执行权限：{execution_permission_label(normalized)}"


def render_execution_permission_guide(level=None):
    normalized = normalize_execution_permission_level(level)
    if normalized == LIMITED:
        return (
            "### 执行权限：只读\n"
            "当前不开放 file_edit、file_write、shell_command、subagent_dispatch。"
            "如任务需要写文件、改文件、运行命令或调用子代理，说明需要切换受限或放行；"
            "不要假装已经完成这些动作。"
        )
    if normalized == GUARDED:
        return (
            "### 执行权限：受限\n"
            "读取和搜索可直接执行；file_edit、file_write、shell_command、"
            "subagent_dispatch 应直接发起调用。Runtime 会在 handler 执行前暂停，"
            "并在当前对话中显示逐次审批卡；不要先等待一条不存在的预授权消息。"
        )
    return (
        "### 执行权限：放行\n"
        "当前可在工作区内写文件、改文件、运行安全命令和调用子代理。"
        "位格真源、Git 内部数据、密钥类路径和灾难命令仍会被 Runtime 硬拒绝。"
    )
