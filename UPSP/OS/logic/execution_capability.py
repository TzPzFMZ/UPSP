"""Capability gate for general_tool requests before handler execution."""
import re

from logic.general_tools import (
    _clean,
    _dangerous_command_reason,
    _file_read_allowed_roots,
    _permission_denial,
    _resolve_request_path,
    _split_list_text,
    _url_denial,
    UNRESTRICTED_ALLOWED_ROOTS,
)
from logic.protocol_tools import normalize_tool_id
from logic.execution_permission import (
    LIMITED_BLOCKED_TOOLS,
    execution_permission_label,
    normalize_execution_permission_level,
)
from logic.sandbox_grant import (
    normalize_sandbox_grant,
    sandbox_decision_details,
    sandbox_roots_for_tool,
    sandbox_tool_allowed,
)
from logic.task_guide import BOOTSTRAP_GUIDE_ID
from logic.work_intent_debt import WORK_INTENT_DEBT_GUIDE_ID


WRITE_TASK_MODES = {
    "write",
    "edit",
    "modify",
    "patch",
    "write_enabled",
    "code_change",
}

TASK_BOOTSTRAP_BLOCKED_TOOLS = {
    "file_edit",
    "file_write",
    "shell_command",
    "subagent_dispatch",
}
TASK_BOOTSTRAP_REQUIRED_GUIDES = {
    BOOTSTRAP_GUIDE_ID,
    WORK_INTENT_DEBT_GUIDE_ID,
}


def _decision(tool_id, phase, allowed, reason="", details=None):
    return {
        "tool_id": tool_id,
        "phase": phase,
        "allowed": bool(allowed),
        "reason": "" if allowed else reason,
        "details": details or {},
    }


def _path_denial(raw_path, allowed_roots=None):
    path = _resolve_request_path(raw_path, allowed_roots)
    if path is None:
        return "", "missing_path", ""
    denial = _permission_denial(path, allowed_roots)
    return str(path), denial, ""


def _stable_path_reason(denial):
    if denial == "outside_allowlist":
        return "outside_allowlist"
    return "capability_denied"


def _remote_script_pipe_reason(command):
    lowered = (command or "").strip().lower()
    if not re.search(r"\b(curl|wget|invoke-webrequest|iwr)\b", lowered):
        return ""
    if re.search(r"\|\s*(bash|sh|pwsh|powershell|iex|invoke-expression|python|node)\b", lowered):
        return "remote_script_pipe"
    return ""


def _check_shell_command(request, tool_id, phase, allowed_roots=None):
    command = str(request.get("command") or "").strip()
    if not command:
        return _decision(
            tool_id,
            phase,
            False,
            "capability_denied",
            {"denial": "missing_command"},
        )
    danger_reason = _remote_script_pipe_reason(command) or _dangerous_command_reason(command)
    if danger_reason:
        return _decision(
            tool_id,
            phase,
            False,
            "dangerous_shell_command",
            {"danger_reason": danger_reason, "command": command},
        )
    cwd = request.get("cwd")
    if _clean(cwd):
        path, denial, _ = _path_denial(cwd, allowed_roots)
        if denial:
            return _decision(
                tool_id,
                phase,
                False,
                _stable_path_reason(denial),
                {"denial": denial, "path": path},
            )
    return _decision(tool_id, phase, True)


def _check_file_read(request, tool_id, phase, allowed_roots=None):
    path, denial, _ = _path_denial(
        request.get("path"),
        _file_read_allowed_roots(allowed_roots),
    )
    if denial:
        return _decision(
            tool_id,
            phase,
            False,
            _stable_path_reason(denial),
            {"denial": denial, "path": path},
        )
    return _decision(tool_id, phase, True)


def _check_file_search(request, tool_id, phase, allowed_roots=None):
    path, denial, _ = _path_denial(request.get("root"), allowed_roots)
    if denial:
        return _decision(
            tool_id,
            phase,
            False,
            _stable_path_reason(denial),
            {"denial": denial, "path": path},
        )
    return _decision(tool_id, phase, True)


def _check_file_edit(request, tool_id, phase, allowed_roots=None):
    path, denial, _ = _path_denial(request.get("path"), allowed_roots)
    if denial:
        return _decision(
            tool_id,
            phase,
            False,
            _stable_path_reason(denial),
            {"denial": denial, "path": path},
        )
    patch = request.get("patch") or request.get("diff")
    if not _clean(patch):
        return _decision(
            tool_id,
            phase,
            False,
            "capability_denied",
            {"denial": "missing_patch", "path": path},
        )
    patch_text = str(patch)
    if "--- " not in patch_text or "+++ " not in patch_text or "@@" not in patch_text:
        return _decision(
            tool_id,
            phase,
            False,
            "capability_denied",
            {"denial": "non_patch_write", "path": path},
        )
    return _decision(tool_id, phase, True)


def _check_file_write(request, tool_id, phase, allowed_roots=None):
    path, denial, _ = _path_denial(request.get("path"), allowed_roots)
    if denial:
        return _decision(
            tool_id,
            phase,
            False,
            _stable_path_reason(denial),
            {"denial": denial, "path": path},
        )
    if "content" not in request:
        return _decision(
            tool_id,
            phase,
            False,
            "capability_denied",
            {"denial": "missing_content", "path": path},
        )
    return _decision(tool_id, phase, True)


def _check_web_fetch(request, tool_id, phase):
    denial, url = _url_denial(request.get("url"))
    if not denial:
        return _decision(tool_id, phase, True)
    if denial == "local_or_private_host_denied":
        reason = "private_network_denied"
    elif denial in {"missing_url", "invalid_url", "invalid_url_scheme"}:
        reason = "capability_denied"
    else:
        reason = "outside_allowlist"
    return _decision(
        tool_id,
        phase,
        False,
        reason,
        {"denial": denial, "url": url},
    )


def _path_list_paths_and_denial(raw_value, allowed_roots=None):
    paths = []
    for raw_path in _split_list_text(raw_value):
        path = _resolve_request_path(raw_path, allowed_roots)
        if path is None:
            continue
        denial = _permission_denial(path, allowed_roots)
        if denial:
            return [], denial, str(path)
        paths.append(path)
    return paths, "", ""


def _check_subagent_dispatch(request, tool_id, phase, allowed_roots=None):
    task_goal = _clean(request.get("task_goal"))
    if not task_goal:
        return _decision(
            tool_id,
            phase,
            False,
            "capability_denied",
            {"denial": "missing_task_goal"},
        )
    expected_artifacts = _clean(request.get("expected_artifacts"))
    if not expected_artifacts:
        return _decision(
            tool_id,
            phase,
            False,
            "capability_denied",
            {"denial": "missing_expected_artifacts"},
        )
    task_mode = _clean(request.get("task_mode") or request.get("mode")) or "read_only"
    write_scope = request.get("write_scope")
    write_requested = task_mode.lower() in WRITE_TASK_MODES or bool(_clean(write_scope))
    if write_requested and not _clean(write_scope):
        return _decision(
            tool_id,
            phase,
            False,
            "write_scope_missing",
            {"task_mode": task_mode},
        )
    allowed_paths, denial, path = _path_list_paths_and_denial(
        request.get("allowed_paths") or request.get("paths") or request.get("scope"),
        allowed_roots,
    )
    if denial:
        return _decision(
            tool_id,
            phase,
            False,
            _stable_path_reason(denial),
            {"denial": denial, "path": path},
        )
    if write_requested:
        write_paths, denial, path = _path_list_paths_and_denial(
            write_scope, allowed_roots
        )
        if denial:
            return _decision(
                tool_id,
                phase,
                False,
                _stable_path_reason(denial),
                {"denial": denial, "path": path},
            )
        for write_path in write_paths:
            if allowed_paths and not any(
                    write_path.is_relative_to(allowed_path)
                    for allowed_path in allowed_paths):
                return _decision(
                    tool_id,
                    phase,
                    False,
                    "outside_allowlist",
                    {"denial": "write_scope_outside_allowed_paths", "path": str(write_path)},
                )
    return _decision(tool_id, phase, True)


def check_general_tool_request(
        request,
        phase,
        active_guides,
        sandbox_grant=None,
        execution_permission_level=None):
    """Return an allow/reject decision for a general_tool request before execution."""
    request = request or {}
    tool_id = normalize_tool_id(_clean(request.get("tool_id")))
    normalized_phase = _clean(phase) or "reaction"
    if normalized_phase != "reaction":
        return _decision(
            tool_id,
            normalized_phase,
            False,
            "capability_denied",
            {"denial": "phase_not_allowed"},
        )
    active_guide_ids = {
        str(item or "").strip()
        for item in active_guides or []
        if str(item or "").strip()
    }
    task_bootstrap_required = active_guide_ids & TASK_BOOTSTRAP_REQUIRED_GUIDES
    if task_bootstrap_required and tool_id in TASK_BOOTSTRAP_BLOCKED_TOOLS:
        return _decision(
            tool_id,
            normalized_phase,
            False,
            "task_bootstrap_required_before_execution",
            {
                "active_guide": sorted(task_bootstrap_required)[0],
                "allowed_before_bootstrap": [
                    "file_read",
                    "file_search",
                    "memory_write",
                    "web_fetch",
                    "web_search",
                ],
                "next_action": "先通过 task_bootstrap 的 submit_initial_guide 建立任务拆解与验收标准。",
            },
        )
    permission_level = normalize_execution_permission_level(
        execution_permission_level
    )
    if permission_level == "limited" and tool_id in LIMITED_BLOCKED_TOOLS:
        return _decision(
            tool_id,
            normalized_phase,
            False,
            "permission_level_required",
            {
                "current_level": "limited",
                "current_label": execution_permission_label("limited"),
                "required_level": "unlimited",
                "required_label": execution_permission_label("unlimited"),
                "next_action": "切换到放行档后再请求写入、修改、命令或子代理工具。",
            },
        )
    sandbox_grant = normalize_sandbox_grant(sandbox_grant)
    if sandbox_grant and not sandbox_tool_allowed(sandbox_grant, tool_id):
        return _decision(
            tool_id,
            normalized_phase,
            False,
            "sandbox_tool_not_allowed",
            sandbox_decision_details(sandbox_grant),
        )
    allowed_roots = (
        sandbox_roots_for_tool(sandbox_grant, tool_id)
        if sandbox_grant else UNRESTRICTED_ALLOWED_ROOTS
    )
    if tool_id == "file_read":
        return _check_file_read(request, tool_id, normalized_phase, allowed_roots)
    if tool_id == "file_search":
        return _check_file_search(request, tool_id, normalized_phase, allowed_roots)
    if tool_id == "shell_command":
        return _check_shell_command(request, tool_id, normalized_phase, allowed_roots)
    if tool_id == "file_edit":
        return _check_file_edit(request, tool_id, normalized_phase, allowed_roots)
    if tool_id == "file_write":
        return _check_file_write(request, tool_id, normalized_phase, allowed_roots)
    if tool_id == "web_fetch":
        return _check_web_fetch(request, tool_id, normalized_phase)
    if tool_id == "subagent_dispatch":
        return _check_subagent_dispatch(
            request, tool_id, normalized_phase, allowed_roots
        )
    return _decision(tool_id, normalized_phase, True)
