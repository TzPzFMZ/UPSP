"""Engineering task sandbox grant shared by prompts, gates and handlers."""
import json
import os
from pathlib import Path


SANDBOX_GRANT_ENV = "UPSP_ENGINEERING_SANDBOX_GRANT_JSON"
DEFAULT_ALLOWED_TOOLS = (
    "file_read",
    "file_glob",
    "file_grep",
    "file_edit",
    "file_write",
)


def _clean(value):
    return str(value or "").strip()


def _list(value):
    if value in (None, ""):
        return []
    if isinstance(value, (list, tuple)):
        return [_clean(item) for item in value if _clean(item)]
    return [_clean(value)]


def _path(value, base=None):
    text = _clean(value)
    if not text:
        return None
    path = Path(text)
    if not path.is_absolute() and base is not None:
        path = Path(base) / path
    return path.resolve()


def _parts_start_with(parts, prefix):
    if not prefix or len(parts) < len(prefix):
        return False
    return tuple(str(part).casefold() for part in parts[:len(prefix)]) == tuple(
        str(part).casefold() for part in prefix
    )


def _alias_prefix_text(alias_rel):
    return str(alias_rel).replace("\\", "/").strip("/")


def _relative_output_alias_candidate(raw_path, write_root, alias_rel):
    raw = _clean(raw_path)
    if not raw:
        return None
    path = Path(raw)
    if path.is_absolute():
        return None
    raw_parts = path.parts
    alias_parts = alias_rel.parts
    if not _parts_start_with(raw_parts, alias_parts):
        return None
    remaining = raw_parts[len(alias_parts):]
    return (write_root.joinpath(*remaining)).resolve()


def _nested_output_alias_candidate(raw_path, write_root, alias_rel):
    raw = _clean(raw_path)
    if not raw:
        return None
    path = Path(raw)
    resolved = path.resolve() if path.is_absolute() else (write_root / path).resolve()
    nested_prefix = (write_root / alias_rel).resolve()
    try:
        remaining = resolved.relative_to(nested_prefix)
    except ValueError:
        return None
    return (write_root / remaining).resolve()


def _paths(values, default, base):
    raw_items = _list(values)
    if not raw_items:
        raw_items = _list(default)
    paths = []
    for item in raw_items:
        path = _path(item, base=base)
        if path is not None:
            paths.append(str(path))
    return paths


def normalize_sandbox_grant(raw):
    if not isinstance(raw, dict):
        return {}
    task_root = _path(raw.get("task_root"))
    if task_root is None:
        return {}
    allowed_tools = _list(raw.get("allowed_tools")) or list(DEFAULT_ALLOWED_TOOLS)
    grant = {
        "phase": _clean(raw.get("phase")) or "engineering",
        "task_root": str(task_root),
        "read_paths": _paths(raw.get("read_paths"), [task_root], task_root),
        "denied_read_paths": _paths(
            raw.get("denied_read_paths"), [], task_root
        ),
        "write_paths": _paths(raw.get("write_paths"), [task_root], task_root),
        "shell_cwd": str(_path(raw.get("shell_cwd") or task_root, base=task_root)),
        "allowed_tools": allowed_tools,
        "validation_commands": _list(raw.get("validation_commands")),
    }
    return grant


def normalize_sandbox_tool_path_alias(grant, tool_id, request):
    """Normalize deterministic engineering write-root aliases before dispatch.

    Engineering dogfood commonly exposes both:
    - shell cwd = task_root, where `output/<run>/...` is natural; and
    - file_write/file_edit allowed root = task_write_root.

    If a model reuses the shell-facing `output/<run>/...` prefix in a write
    tool, resolving it relative to task_write_root creates
    `task_write_root/output/<run>/...`.  This helper only removes that
    mechanically derivable alias. It does not inspect task content.
    """
    grant = normalize_sandbox_grant(grant)
    if not grant or _clean(tool_id) not in {"file_write", "file_edit"}:
        return dict(request or {}), {}
    original = dict(request or {})
    raw_path = _clean(original.get("path"))
    if not raw_path:
        return original, {}
    task_root = Path(grant.get("task_root")).resolve()
    for item in grant.get("write_paths") or []:
        write_root = Path(item).resolve()
        try:
            alias_rel = write_root.relative_to(task_root)
        except ValueError:
            continue
        if not alias_rel.parts:
            continue
        alias_prefix = _alias_prefix_text(alias_rel)
        candidate = _relative_output_alias_candidate(raw_path, write_root, alias_rel)
        reason = "task_write_root_alias"
        if candidate is None:
            candidate = _nested_output_alias_candidate(raw_path, write_root, alias_rel)
            reason = "nested_task_write_root_alias"
        if candidate is None:
            continue
        normalized = dict(original)
        normalized["path"] = str(candidate)
        return normalized, {
            "path_normalized_from": raw_path,
            "path_normalization_reason": reason,
            "path_alias_prefix": alias_prefix,
            "path_normalized_to": str(candidate),
        }
    return original, {}


def load_sandbox_grant(env=None):
    env = env or os.environ
    raw = _clean(env.get(SANDBOX_GRANT_ENV))
    if not raw:
        return {}
    try:
        decoded = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return normalize_sandbox_grant(decoded)


def sandbox_roots_for_tool(grant, tool_id):
    grant = normalize_sandbox_grant(grant)
    if not grant:
        return None
    tool_id = _clean(tool_id)
    if tool_id in {"file_read", "file_glob", "file_grep"}:
        return tuple(Path(item).resolve() for item in grant.get("read_paths") or [])
    if tool_id in {"file_edit", "file_write"}:
        return tuple(Path(item).resolve() for item in grant.get("write_paths") or [])
    if tool_id == "shell_command":
        return (Path(grant.get("shell_cwd")).resolve(),)
    return tuple(Path(item).resolve() for item in grant.get("read_paths") or [])


def sandbox_denied_roots_for_tool(grant, tool_id):
    grant = normalize_sandbox_grant(grant)
    if not grant or _clean(tool_id) not in {"file_read", "file_glob", "file_grep"}:
        return ()
    return tuple(
        Path(item).resolve() for item in grant.get("denied_read_paths") or []
    )


def sandbox_tool_allowed(grant, tool_id):
    grant = normalize_sandbox_grant(grant)
    if not grant:
        return True
    return _clean(tool_id) in set(grant.get("allowed_tools") or [])


def sandbox_decision_details(grant):
    grant = normalize_sandbox_grant(grant)
    if not grant:
        return {}
    return {
        "sandbox_phase": grant.get("phase", ""),
        "task_root": grant.get("task_root", ""),
    }


def render_sandbox_grant_guide(grant=None):
    grant = normalize_sandbox_grant(grant or load_sandbox_grant())
    if not grant:
        return ""
    lines = [
        "### 工程任务 Sandbox 授权",
        f"phase={grant.get('phase', '')}",
        f"task_root={grant.get('task_root', '')}",
        "read_paths:",
    ]
    lines.extend(f"- {item}" for item in grant.get("read_paths") or [])
    if grant.get("denied_read_paths"):
        lines.append("denied_read_paths:")
        lines.extend(f"- {item}" for item in grant.get("denied_read_paths") or [])
    lines.append("write_paths:")
    lines.extend(f"- {item}" for item in grant.get("write_paths") or [])
    lines.append(f"shell_cwd={grant.get('shell_cwd', '')}")
    lines.append("allowed_tools=" + ", ".join(grant.get("allowed_tools") or []))
    task_root = Path(grant.get("task_root")).resolve()
    alias_prefixes = []
    for item in grant.get("write_paths") or []:
        write_root = Path(item).resolve()
        try:
            alias_rel = write_root.relative_to(task_root)
        except ValueError:
            continue
        if alias_rel.parts:
            alias_prefixes.append(_alias_prefix_text(alias_rel))
    if alias_prefixes:
        rendered = " / ".join(alias_prefixes)
        lines.append("路径口径：file_write/file_edit 相对路径默认按 write_paths 解析。")
        lines.append(
            f"若你按 shell 视角使用 {rendered}/...，写入工具会归一到 write_root/...；"
            f"后续同一输出根优先直接写去掉 {rendered} 前缀的路径。"
        )
    if grant.get("validation_commands"):
        lines.append("validation_commands:")
        lines.extend(f"- {item}" for item in grant.get("validation_commands") or [])
    lines.append("这张授权只对当前工程任务生效；grant 外路径不得写入或执行。")
    return "\n".join(lines)
