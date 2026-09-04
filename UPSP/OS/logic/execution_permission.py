"""Execution permission levels for general tools."""

import os
import threading

from constants import local_now


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


class RuntimePermissionUpdates:
    """Apply GUI permission changes only at audited Frame boundaries."""

    def __init__(self, chain, audit, control, heartbeat):
        self.chain = chain
        self.audit = audit
        self.control = control
        self.heartbeat = heartbeat
        self._lock = threading.Lock()
        self._pending = None

    def request(self, level):
        with self._lock:
            status = self.control.snapshot(self.heartbeat)
            if not status.get("round_in_flight"):
                raise ValueError("no_round_in_flight")
            if str(status.get("stage") or "").startswith("cleanup"):
                raise ValueError("permission_change_too_late")
            update = {
                "permission_level": normalize_execution_permission_level(level),
                "requested_at": local_now().isoformat(),
            }
            self._pending = update
        return {
            "status": "pending",
            **execution_permission_audit(update["permission_level"]),
            "effective_after": "next_frame_boundary",
            "requested_at": update["requested_at"],
        }

    def attach_status(self, status):
        with self._lock:
            pending = dict(self._pending or {})
        status["execution_permission"] = {
            **execution_permission_audit(self.chain.current),
            "pending_level": pending.get("permission_level"),
            "requested_at": pending.get("requested_at"),
        }
        return status

    def apply(self, round_num, phase, iteration):
        with self._lock:
            update = self._pending
        if not update:
            return None
        previous = self.chain.current
        current = normalize_execution_permission_level(update["permission_level"])
        frame_id = f"R{int(round_num):06d}:{phase}:{int(iteration)}"
        payload = {
            "previous": execution_permission_audit(previous),
            "current": execution_permission_audit(current),
            "requested_at": update["requested_at"],
            "applied_at": local_now().isoformat(),
            "effective_frame_id": frame_id,
        }
        self.audit.get_store().append_event(
            round_num,
            "execution_permission_changed",
            payload,
            phase=phase,
            iteration=iteration,
        )
        self.chain.apply(current)
        with self._lock:
            if self._pending is update:
                self._pending = None
        return payload


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
        "当前可写文件、改文件、运行命令和调用子代理。"
        "文件工具仍服从各自路径门；shell_command 以当前宿主用户权限执行，"
        "不会被命令字符串检查限制在工作区内。"
    )
