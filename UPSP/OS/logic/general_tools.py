"""General tool execution helpers for external action tools."""
import hashlib
import html
import ipaddress
import json
import locale
import os
import re
import socket
import subprocess
from fnmatch import fnmatch
from collections import deque
from datetime import datetime
from html.parser import HTMLParser
from pathlib import Path, PureWindowsPath
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qs, quote_plus, unquote, urljoin, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from constants import local_now
from paths import (
    PROGRAM_UPSP_ROOT,
    UPSP_DATA_ROOT,
    ACTIVE_PID,
    GLOBAL_CONFIG_DIR,
    WEB_BACKEND_HEALTH_JSON,
)
from utils.content_ranges import apply_explicit_range, range_kwargs_from_request
from logic.evidence_refs import (
    attach_evidence_handle,
    canonical_command_ref,
    evidence_handle_for_result,
    result_supports_evidence,
    shell_result_subcommands,
)
from logic.file_read_window import RUNTIME_CONTEXT_KEY, plan_file_read_window
from logic.protocol_tools import tool_metadata_for
from utils.read_tool_material import read_tool_material_content


WORKSPACE_ROOT = Path(PROGRAM_UPSP_ROOT).resolve()
PERSONAS_ROOT = (Path(UPSP_DATA_ROOT).resolve() / "personas").resolve()
DEFAULT_ALLOWED_ROOTS = (WORKSPACE_ROOT,)
UNRESTRICTED_ALLOWED_ROOTS = object()
DEFAULT_DENIED_ROOTS = ()
DEFAULT_DENIED_FILES = set()
DEFAULT_CREDENTIAL_ROOTS = tuple(
    path.resolve()
    for path in (
        Path(GLOBAL_CONFIG_DIR),
        Path.home() / ".ssh",
        Path.home() / ".aws",
        Path.home() / ".azure",
        Path.home() / ".config" / "gcloud",
        Path.home() / ".config" / "gh",
    )
)
DEFAULT_CREDENTIAL_FILES = {
    path.resolve()
    for path in (
        Path.home() / ".codex" / "auth.json",
        Path.home() / ".docker" / "config.json",
        Path.home() / ".git-credentials",
        Path.home() / ".netrc",
        Path.home() / "_netrc",
        Path.home() / ".npmrc",
        Path.home() / ".pypirc",
    )
}
EXTRA_FILE_READ_ROOTS_ENV = "UPSP_FILE_READ_EXTRA_ROOTS"
SECRET_SUFFIXES = {
    ".env",
    ".key",
    ".pem",
    ".pfx",
    ".p12",
    ".crt",
    ".cer",
}
SECRET_NAME_FRAGMENTS = (
    "secret",
    "token",
    "credential",
    "credentials",
    "apikey",
    "api_key",
    "password",
)
PERSONA_READ_ALIAS = "persona://"
PUBLIC_WEB_READ_SCOPE = "public_web_read"
WORKSPACE_PATCH_SCOPE = "workspace_patch_allowlist"
WORKSPACE_SHELL_SCOPE = "workspace_shell_allowlist"
SUBAGENT_TASK_SCOPE = "subagent_task_scope"
SHELL_OUTPUT_LIMIT = 12000
SHELL_UNDECODABLE_TEMPLATE = "[无法可靠解码 {stream_name}，原始字节已记录长度与 sha256]"
SHELL_REPLACEMENT_HIDDEN = "[输出含无法解码字符，已隐藏乱码片段]"
SUBAGENT_WRITE_MODES = {
    "write",
    "edit",
    "modify",
    "patch",
    "write_enabled",
    "code_change",
}
WEB_DOWNLOAD_SUFFIXES = {
    ".7z",
    ".apk",
    ".bin",
    ".dmg",
    ".doc",
    ".docx",
    ".exe",
    ".gz",
    ".iso",
    ".msi",
    ".pdf",
    ".ppt",
    ".pptx",
    ".rar",
    ".tar",
    ".tgz",
    ".xls",
    ".xlsx",
    ".zip",
}
WEB_LOGIN_FRAGMENTS = (
    "/account",
    "/auth",
    "/checkout",
    "/login",
    "/oauth",
    "/signin",
    "/signup",
    "/sso",
)
WEB_HIDDEN_TAGS = {"script", "style", "noscript", "svg", "canvas"}
WEB_MAX_BYTES = 250_000
WEB_FIND_TEXT_MAX_CHARS = 256
DEFAULT_FILE_READ_WINDOW_CHARS = 16384
DEFAULT_WEB_FETCH_WINDOW_CHARS = 4096
DEFAULT_WEB_SEARCH_WINDOW_RESULTS = 5
FILE_GLOB_DEFAULT_MAX_RESULTS = 20
FILE_GLOB_MAX_RESULTS = 100
FILE_GREP_DEFAULT_MAX_RESULTS = 20
FILE_GREP_MAX_RESULTS = 100
FILE_GREP_MAX_QUERY_CHARS = 1024
FILE_GREP_MAX_FILES = 10_000
FILE_GREP_MAX_BYTES = 256 * 1024 * 1024
FILE_GREP_MAX_FILE_BYTES = 64 * 1024 * 1024
FILE_GREP_MAX_CONTEXT_LINES = 3
FILE_GREP_MAX_DISPLAY_LINE_CHARS = 2000
WEB_BACKEND_HEALTH_ENV = "UPSP_WEB_BACKEND_HEALTH_PATH"
WEB_FETCH_BACKENDS = ("direct_fetch", "jina_reader")
WEB_SEARCH_BACKENDS = ("ddgs", "html_fallback")


def web_backend_ids_for_tool(tool_id):
    if tool_id == "web_fetch":
        return WEB_FETCH_BACKENDS
    if tool_id == "web_search":
        return WEB_SEARCH_BACKENDS
    return ()


def _clean(value):
    return str(value or "").strip().strip("`").strip('"').strip("'")

def _resolved_roots(roots):
    if roots is UNRESTRICTED_ALLOWED_ROOTS:
        return DEFAULT_ALLOWED_ROOTS
    return tuple(Path(root).resolve() for root in roots or DEFAULT_ALLOWED_ROOTS)


def _is_foreign_windows_path_syntax(raw_path, native_is_absolute=None):
    text = _clean(raw_path)
    if not text:
        return False
    if native_is_absolute is None:
        native_is_absolute = Path(text).is_absolute()
    if native_is_absolute:
        return False
    return bool(PureWindowsPath(text).drive)


def _foreign_windows_path_placeholder(raw_path):
    marker = re.sub(r"[^A-Za-z0-9._-]+", "_", _clean(raw_path)).strip("_")
    marker = marker or "path"
    return (Path(os.path.abspath(os.sep)) / "__upsp_foreign_windows_path__" / marker).resolve()


def _extra_file_read_roots():
    raw = os.environ.get(EXTRA_FILE_READ_ROOTS_ENV, "")
    roots = []
    for item in raw.split(os.pathsep):
        text = _clean(item)
        if text:
            roots.append(Path(text).resolve())
    return tuple(roots)


def _file_read_allowed_roots(allowed_roots):
    if allowed_roots is UNRESTRICTED_ALLOWED_ROOTS:
        return UNRESTRICTED_ALLOWED_ROOTS
    if allowed_roots is not None:
        return allowed_roots
    return DEFAULT_ALLOWED_ROOTS + (PERSONAS_ROOT,) + _extra_file_read_roots()


def _resolve_request_path(raw_path, allowed_roots):
    text = _clean(raw_path)
    if not text:
        return None
    if text.lower().startswith(PERSONA_READ_ALIAS):
        return _resolve_persona_alias(text)
    path = Path(text)
    if _is_foreign_windows_path_syntax(text, native_is_absolute=path.is_absolute()):
        return _foreign_windows_path_placeholder(text)
    if not path.is_absolute():
        path = _resolved_roots(allowed_roots)[0] / path
    return path.resolve()


def _resolve_persona_alias(raw_path):
    text = _clean(raw_path)
    if not text.lower().startswith(PERSONA_READ_ALIAS):
        return None
    suffix = text[len(PERSONA_READ_ALIAS):]
    if "\\" in suffix or "\x00" in suffix:
        return None
    parts = [part for part in suffix.split("/") if part]
    if any(part in {".", ".."} or ":" in part for part in parts):
        return None
    if parts and parts[0].casefold() == "active":
        parts[0] = str(ACTIVE_PID)
    candidate = PERSONAS_ROOT.joinpath(*parts)
    try:
        resolved = candidate.resolve()
        resolved.relative_to(PERSONAS_ROOT)
    except (OSError, ValueError):
        return None
    return resolved


def _display_path(path):
    resolved = Path(path).resolve()
    try:
        relative = resolved.relative_to(PERSONAS_ROOT)
    except ValueError:
        return str(resolved)
    suffix = relative.as_posix()
    return PERSONA_READ_ALIAS + suffix if suffix else PERSONA_READ_ALIAS


def _persona_alias_invalid(raw_path):
    text = _clean(raw_path)
    return text.lower().startswith(PERSONA_READ_ALIAS) and _resolve_persona_alias(text) is None


def _bounded_int(value, default, minimum, maximum):
    try:
        number = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, min(maximum, number))


def _general_tool_window_params():
    try:
        from data.config_store import ConfigStore

        cfg = ConfigStore().get_general_tool_window_params()
    except Exception:
        cfg = {}
    return {
        "file_read_window_chars": _bounded_int(
            cfg.get("file_read_window_chars"),
            DEFAULT_FILE_READ_WINDOW_CHARS,
            1,
            20000,
        ),
        "web_fetch_window_chars": _bounded_int(
            cfg.get("web_fetch_window_chars"),
            DEFAULT_WEB_FETCH_WINDOW_CHARS,
            1,
            20000,
        ),
        "web_search_window_results": _bounded_int(
            cfg.get("web_search_window_results"),
            DEFAULT_WEB_SEARCH_WINDOW_RESULTS,
            1,
            10,
        ),
    }


def _web_backend_health_path():
    raw = _clean(os.environ.get(WEB_BACKEND_HEALTH_ENV))
    return Path(raw).resolve() if raw else Path(WEB_BACKEND_HEALTH_JSON).resolve()


def _load_web_backend_health():
    path = _web_backend_health_path()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def _save_web_backend_health(health):
    if not health:
        return
    path = _web_backend_health_path()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(health or {}, ensure_ascii=False, indent=2, sort_keys=True),
            encoding="utf-8",
        )
    except OSError:
        pass


def _web_backend_fail_count(health, tool_id, backend_id):
    try:
        return int(
            ((health or {}).get(tool_id) or {})
            .get(backend_id, {})
            .get("hard_fail_count", 0)
            or 0
        )
    except (TypeError, ValueError):
        return 0


def _ordered_web_backends(tool_id, skip_backend_ids=None, ignore_health=False):
    backend_ids = list(web_backend_ids_for_tool(tool_id))
    skip = {str(item or "").strip() for item in (skip_backend_ids or []) if str(item or "").strip()}
    if skip:
        backend_ids = [backend_id for backend_id in backend_ids if backend_id not in skip]
    health = {} if ignore_health else _load_web_backend_health()
    default_order = {backend_id: index for index, backend_id in enumerate(web_backend_ids_for_tool(tool_id))}
    return sorted(
        backend_ids,
        key=lambda backend_id: (
            _web_backend_fail_count(health, tool_id, backend_id),
            default_order.get(backend_id, 999),
        ),
    )


def _record_web_backend_hard_fail(health, tool_id, backend_id, reason="", detail=""):
    if not isinstance(health, dict):
        return 0, 1
    tool_health = health.setdefault(tool_id, {})
    backend_health = tool_health.setdefault(backend_id, {})
    before = _web_backend_fail_count(health, tool_id, backend_id)
    after = before + 1
    backend_health["hard_fail_count"] = after
    backend_health["last_failed_at"] = local_now().isoformat()
    if reason:
        backend_health["last_reason"] = _clean(reason)
    if detail:
        backend_health["last_detail"] = _clean(detail)[:500]
    return before, after


def _web_backend_attempt(backend_id, status, reason="", detail="", **extra):
    attempt = {
        "backend_id": backend_id,
        "status": status,
    }
    if reason:
        attempt["reason"] = _clean(reason)
    if detail:
        attempt["detail"] = _clean(detail)[:500]
    for key, value in extra.items():
        if value not in (None, "", []):
            attempt[key] = value
    return attempt


def _web_attempt_summary(attempts):
    parts = []
    for attempt in attempts or []:
        if not isinstance(attempt, dict):
            continue
        backend_id = _clean(attempt.get("backend_id"))
        status = _clean(attempt.get("status"))
        reason = _clean(attempt.get("reason"))
        if not backend_id:
            continue
        if reason:
            parts.append(f"{backend_id}:{status}/{reason}")
        else:
            parts.append(f"{backend_id}:{status}")
    return "; ".join(parts)


def _int_or_none(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError("invalid_range_value")


def _base_result(tool_id="file_read", status="rejected", reason=""):
    meta = tool_metadata_for(tool_id)
    result = {
        "tool_id": tool_id,
        "tool_family": meta.get("tool_family", "general_tool"),
        "tool_class": meta.get("tool_class", "read_tool"),
        "status": status,
        "source": "general_tool_call",
        "backend_type": meta.get("backend_type", "python"),
        "handler": meta.get("handler", ""),
        "permission_scope": meta.get("permission_scope", ""),
        "result_kind": meta.get("result_kind", "general_tool_result"),
        "protocol_tool_receipt": False,
        "executed_at": local_now().isoformat(),
    }
    if reason:
        result["reason"] = reason
    return result


def _is_persona_truth_path(path):
    try:
        relative = Path(path).resolve().relative_to(PERSONAS_ROOT)
    except ValueError:
        return False
    parts = [part.casefold() for part in relative.parts]
    return len(parts) >= 3 and parts[2] == "persona"


def _is_memory_storage_path(path):
    lowered = [part.casefold() for part in Path(path).parts]
    return any(
        lowered[index:index + 2] in (["stm", "memory"], ["ltm", "memory"])
        for index in range(max(0, len(lowered) - 1))
    )


def _contains_private_access(value):
    if isinstance(value, dict):
        if str(value.get("access") or "").strip().casefold() == "private":
            return True
        return any(_contains_private_access(item) for item in value.values())
    if isinstance(value, list):
        return any(_contains_private_access(item) for item in value)
    return False


def _private_read_denial(path):
    path = Path(path)
    lowered_name = path.name.casefold()
    if lowered_name.endswith(".private.md"):
        return "private_persona_denied"
    if not (
            lowered_name == "meta.json"
            and _is_memory_storage_path(path)
            and path.exists()
            and path.is_file()):
        return ""
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError):
        return "private_scope_unverified"
    if _contains_private_access(value):
        return "private_persona_denied"
    return ""


def _permission_denial(
        path,
        allowed_roots,
        denied_roots=None,
        denied_files=None,
        *,
        access="read"):
    path = Path(path).resolve()
    lowered_parts = [part.lower() for part in path.parts]
    if path in DEFAULT_CREDENTIAL_FILES or any(
            path.is_relative_to(root) for root in DEFAULT_CREDENTIAL_ROOTS):
        return "secret_like_path"

    if allowed_roots is not UNRESTRICTED_ALLOWED_ROOTS:
        allowed = _resolved_roots(allowed_roots)
        if not any(path.is_relative_to(root) for root in allowed):
            return "outside_allowlist"

    effective_denied_files = set(
        DEFAULT_DENIED_FILES if denied_files is None else denied_files
    )
    if path in effective_denied_files:
        return "path_denied"
    effective_denied_roots = _resolved_roots(
        DEFAULT_DENIED_ROOTS if denied_roots is None else denied_roots
    ) if (denied_roots is not None or DEFAULT_DENIED_ROOTS) else ()
    if any(path.is_relative_to(root) for root in effective_denied_roots):
        return "path_denied"

    if ".git" in lowered_parts:
        return "git_internal_denied"

    lowered_name = path.name.lower()
    if lowered_name in SECRET_SUFFIXES or path.suffix.lower() in SECRET_SUFFIXES:
        return "secret_like_path"
    if any(fragment in lowered_name for fragment in SECRET_NAME_FRAGMENTS):
        return "secret_like_path"
    if access == "read":
        return _private_read_denial(path)
    if access in {"write", "shell"} and _is_persona_truth_path(path):
        return "persona_write_denied"
    return ""


def _read_text_file(path, encoding):
    data = path.read_bytes()
    if b"\x00" in data[:4096]:
        raise UnicodeError("binary_nul_detected")
    return data.decode(encoding or "utf-8").replace("\r\n", "\n")


def _line_number_at_char(text, char_index):
    text = str(text or "")
    if not text:
        return 0
    index = max(0, min(len(text), int(char_index or 1) - 1))
    return text.count("\n", 0, index) + 1


def _line_offsets(text):
    lines = str(text or "").splitlines(keepends=True)
    offsets = []
    offset = 0
    for line in lines:
        offsets.append(offset)
        offset += len(line)
    return lines, offsets


def _line_char_bounds(text, line_no):
    lines, offsets = _line_offsets(text)
    if not lines:
        return 1, 0
    index = min(max(1, int(line_no or 1)), len(lines)) - 1
    start = offsets[index] + 1
    end = offsets[index] + len(lines[index])
    return start, end


def _line_bounded_window(text, start_line, end_line, window_chars, allow_line_char=True):
    lines, line_offsets = _line_offsets(text)
    total_lines = len(lines)
    total_chars = len(str(text or ""))
    if total_lines == 0:
        return {
            "content": "",
            "start_line": 0,
            "end_line": 0,
            "has_more": False,
            "next_start_line": None,
            "next_char_start": None,
            "next_char_end": None,
            "window_boundary": "whole_line",
            "line_overlong": False,
            "char_start": None,
            "char_end": None,
        }
    applied_start = min(max(1, int(start_line or 1)), total_lines + 1)
    requested_end = int(end_line or total_lines)
    requested_end = max(applied_start, requested_end)
    applied_end_limit = min(requested_end, total_lines)
    selected = []
    returned = 0
    applied_end = applied_start - 1
    line_overlong_seen = False
    for line_no in range(applied_start, applied_end_limit + 1):
        line = lines[line_no - 1]
        if selected and returned + len(line) > window_chars:
            content = "".join(selected)
            return {
                "content": content,
                "start_line": applied_start,
                "end_line": applied_end,
                "has_more": True,
                "next_start_line": line_no,
                "next_char_start": None,
                "next_char_end": None,
                "window_boundary": "whole_line",
                "line_overlong": line_overlong_seen,
                "char_start": None,
                "char_end": None,
            }
        if allow_line_char and not selected and len(line) > window_chars:
            char_start = line_offsets[line_no - 1] + 1
            char_end = min(line_offsets[line_no - 1] + len(line), char_start + window_chars - 1)
            content = line[: max(0, char_end - char_start + 1)]
            next_char_start = char_end + 1 if char_end < line_offsets[line_no - 1] + len(line) else None
            return {
                "content": content,
                "start_line": line_no,
                "end_line": line_no,
                "has_more": bool(next_char_start or line_no < applied_end_limit),
                "next_start_line": None,
                "next_char_start": next_char_start,
                "next_char_end": (
                    min(total_chars, next_char_start + window_chars - 1)
                    if next_char_start is not None
                    else None
                ),
                "window_boundary": "line_char",
                "line_overlong": True,
                "char_start": char_start,
                "char_end": char_end,
            }
        if len(line) > window_chars:
            line_overlong_seen = True
        selected.append(line)
        returned += len(line)
        applied_end = line_no
        if returned >= window_chars:
            break
    content = "".join(selected)
    has_more = applied_end < applied_end_limit
    return {
        "content": content,
        "start_line": applied_start,
        "end_line": applied_end,
        "has_more": has_more,
        "next_start_line": applied_end + 1 if has_more else None,
        "next_char_start": None,
        "next_char_end": None,
        "window_boundary": "whole_line",
        "line_overlong": line_overlong_seen,
        "char_start": None,
        "char_end": None,
    }


def _char_bounded_window(text, start_char, end_char, window_chars):
    text = str(text or "")
    total_chars = len(text)
    if total_chars == 0:
        return {
            "content": "",
            "start_line": 0,
            "end_line": 0,
            "has_more": False,
            "next_start_line": None,
            "next_char_start": None,
            "next_char_end": None,
            "window_boundary": "line_char",
            "line_overlong": False,
            "char_start": 0,
            "char_end": 0,
        }
    applied_start = min(max(1, _int_or_none(start_char) or 1), total_chars + 1)
    requested_end = _int_or_none(end_char) or total_chars
    requested_end = max(applied_start, requested_end)
    applied_end_limit = min(requested_end, total_chars)
    applied_end = min(applied_end_limit, applied_start + window_chars - 1)
    content = text[applied_start - 1:applied_end]
    start_line = _line_number_at_char(text, applied_start)
    end_line = _line_number_at_char(text, applied_end)
    line_start_char, line_end_char = _line_char_bounds(text, start_line)
    line_overlong = line_end_char - line_start_char + 1 > window_chars
    has_more = applied_end < applied_end_limit
    next_char_start = applied_end + 1 if has_more else None
    return {
        "content": content,
        "start_line": start_line,
        "end_line": end_line,
        "has_more": has_more,
        "next_start_line": None,
        "next_char_start": next_char_start,
        "next_char_end": (
            min(total_chars, next_char_start + window_chars - 1)
            if next_char_start is not None
            else None
        ),
        "window_boundary": "line_char",
        "line_overlong": line_overlong,
        "char_start": applied_start,
        "char_end": applied_end,
    }


def _file_read_range_request(request):
    line_start = (request or {}).get("line_start")
    if line_start in (None, ""):
        return None, None
    return {"line_start": line_start}, {
        "type": "line_start",
        "line_start": max(1, _int_or_none(line_start) or 1),
    }

def _execute_file_read(request, allowed_roots=None, denied_roots=None, denied_files=None):
    read_roots = _file_read_allowed_roots(allowed_roots)
    raw_path = request.get("path")
    path = _resolve_request_path(request.get("path"), read_roots)
    if path is None:
        reason = "invalid_persona_alias" if _persona_alias_invalid(raw_path) else "missing_path"
        return _base_result(status="rejected", reason=reason)

    display_path = _display_path(path)

    denial = _permission_denial(
        path, read_roots, denied_roots, denied_files, access="read"
    )
    if denial:
        result = _base_result(status="rejected", reason=denial)
        result["path"] = display_path
        return result

    if not path.exists():
        result = _base_result(status="rejected", reason="file_not_found")
        result["path"] = display_path
        return result
    if not path.is_file():
        result = _base_result(status="rejected", reason="not_a_file")
        result["path"] = display_path
        return result

    encoding = _clean(request.get("encoding")) or "utf-8"
    try:
        text = _read_text_file(path, encoding)
    except (OSError, UnicodeError) as exc:
        result = _base_result(status="rejected", reason="text_read_failed")
        result["path"] = display_path
        result["detail"] = str(exc)
        return result

    try:
        range_request, requested_range = _file_read_range_request(request)
    except ValueError as exc:
        result = _base_result(status="rejected", reason=str(exc))
        result["path"] = display_path
        return result

    total_lines = len(text.splitlines())
    total_chars = len(text)
    window_plan = plan_file_read_window(
        _general_tool_window_params()["file_read_window_chars"],
        request.get(RUNTIME_CONTEXT_KEY),
    )
    window_chars = window_plan["window_chars"]
    if requested_range and requested_range.get("type") == "line_start":
        requested_start_line = requested_range.get("line_start")
        window = _line_bounded_window(
            text,
            requested_start_line,
            total_lines,
            window_chars,
            allow_line_char=False,
        )
        range_requested = requested_range
        range_applied = {
            "type": "line",
            "line_start": window["start_line"],
            "line_end": window["end_line"],
        }
    else:
        window = _line_bounded_window(
            text,
            1,
            total_lines,
            window_chars,
            allow_line_char=False,
        )
        range_requested = None
        range_applied = {
            "type": "line",
            "line_start": window["start_line"],
            "line_end": window["end_line"],
        }
    if (
        not window["has_more"]
        and window["next_char_start"] is None
        and window["window_boundary"] == "whole_line"
        and window["end_line"] not in (None, "")
        and window["end_line"] < total_lines
    ):
        window["has_more"] = True
        window["next_start_line"] = window["end_line"] + 1
    result = _base_result(status="ok")
    result.update({
        "path": display_path,
        "content": window["content"],
        "chars": len(window["content"]),
        "read_mode": "bounded",
        "window_kind": "file_read_bounded",
        "window_boundary": window["window_boundary"],
        "returned_chars": len(window["content"]),
        "window_chars": window_chars,
        "has_more": bool(window["has_more"]),
        "line_overlong": bool(window["line_overlong"]),
        "range_requested": range_requested,
        "range_applied": range_applied,
        "total_lines": total_lines,
        "total_chars": total_chars,
        "requested_start_line": (
            (range_requested or {}).get("line_start")
            if (range_requested or {}).get("type") == "line_start"
            else None
        ),
        "requested_end_line": None,
        "line_start": window["start_line"],
        "line_end": window["end_line"],
        "start_line": window["start_line"],
        "end_line": window["end_line"],
        "char_start": window["char_start"],
        "char_end": window["char_end"],
        "next_line_start": window["next_start_line"],
        "next_start_line": window["next_start_line"],
        "next_char_start": window["next_char_start"],
        "next_char_end": window["next_char_end"],
        "encoding": encoding,
        "evidence_refs": [f"file_read:{display_path}"],
        **window_plan,
    })
    if display_path.startswith(PERSONA_READ_ALIAS):
        result["host_path"] = str(path)
    if _is_memory_storage_path(path):
        result["read_semantics"] = "raw_inspection"
        result["memory_lifecycle_effects"] = []
    return result


def _file_glob_pattern_denial(pattern):
    text = str(pattern or "").strip()
    if not text:
        return "missing_pattern"
    if "\x00" in text:
        return "invalid_pattern"
    path_like = Path(text)
    if path_like.is_absolute() or ".." in path_like.parts:
        return "path_pattern_denied"
    if "/" in text or "\\" in text:
        return "path_pattern_denied"
    return ""


def _execute_file_glob(request, allowed_roots=None, denied_roots=None, denied_files=None):
    read_roots = _file_read_allowed_roots(allowed_roots)
    raw_root = request.get("root")
    root = _resolve_request_path(raw_root, read_roots)
    if root is None:
        reason = "invalid_persona_alias" if _persona_alias_invalid(raw_root) else "missing_root"
        return _base_result(tool_id="file_glob", status="rejected", reason=reason)

    display_root = _display_path(root)
    denial = _permission_denial(
        root, read_roots, denied_roots, denied_files, access="read"
    )
    if denial:
        result = _base_result(tool_id="file_glob", status="rejected", reason=denial)
        result["root"] = display_root
        return result
    if not root.exists():
        result = _base_result(tool_id="file_glob", status="rejected", reason="root_not_found")
        result["root"] = display_root
        return result
    if not root.is_dir():
        result = _base_result(tool_id="file_glob", status="rejected", reason="not_a_directory")
        result["root"] = display_root
        return result

    pattern = _clean(request.get("pattern"))
    pattern_denial = _file_glob_pattern_denial(pattern)
    if pattern_denial:
        result = _base_result(tool_id="file_glob", status="rejected", reason=pattern_denial)
        result["root"] = display_root
        return result

    recursive = bool(request.get("recursive") is True)
    max_results = _bounded_int(
        request.get("max_results"),
        FILE_GLOB_DEFAULT_MAX_RESULTS,
        1,
        FILE_GLOB_MAX_RESULTS,
    )
    matches = []
    has_more = False
    discovery_stats = {"restricted_skipped": 0}
    for candidate in _iter_glob_matches(
            root, pattern, recursive, discovery_stats):
        try:
            resolved = candidate.resolve()
            resolved.relative_to(root)
        except OSError:
            discovery_stats["restricted_skipped"] += 1
            continue
        except ValueError:
            discovery_stats["restricted_skipped"] += 1
            continue
        if _permission_denial(
                resolved,
                read_roots,
                denied_roots,
                denied_files,
                access="read"):
            discovery_stats["restricted_skipped"] += 1
            continue
        if len(matches) >= max_results:
            has_more = True
            break
        matches.append({
            "path": _display_path(resolved),
            "name": resolved.name,
            "is_file": resolved.is_file(),
            "is_dir": resolved.is_dir(),
        })

    result = _base_result(tool_id="file_glob", status="ok")
    result.update({
        "root": display_root,
        "pattern": pattern,
        "recursive": recursive,
        "matches": matches,
        "result_count": len(matches),
        "max_results": max_results,
        "has_more": has_more,
        "restricted_skipped": discovery_stats["restricted_skipped"],
        "read_mode": "bounded",
        "window_kind": "file_glob_bounded",
        "evidence_refs": [f"file_glob:{display_root}:{pattern}"],
    })
    if display_root.startswith(PERSONA_READ_ALIAS):
        result["host_root"] = str(root)
    if not matches:
        result["reason"] = "search_no_results"
    return result


def _grep_text_window(value, match_span=None, limit=FILE_GREP_MAX_DISPLAY_LINE_CHARS):
    text = str(value or "").rstrip("\r\n")
    if len(text) <= limit:
        return text, False, 0, len(text)
    if match_span is None:
        return text[:limit - 1] + "…", True, 0, limit - 1

    match_start, match_end = match_span
    room = max(1, limit - 2)
    start = max(0, min(match_start - room // 3, len(text) - room))
    end = min(len(text), start + room)
    prefix = "…" if start else ""
    suffix = "…" if end < len(text) else ""
    return prefix + text[start:end] + suffix, True, start, end


def _literal_match_span(text, query, case_sensitive):
    if case_sensitive:
        start = text.find(query)
        return None if start < 0 else (start, start + len(query))

    folded_text = text.casefold()
    folded_query = query.casefold()
    folded_start = folded_text.find(folded_query)
    if folded_start < 0:
        return None
    if len(folded_text) == len(text):
        return folded_start, folded_start + len(folded_query)

    folded_end = folded_start + len(folded_query)
    source_start = None
    source_end = len(text)
    cursor = 0
    for index, char in enumerate(text):
        next_cursor = cursor + len(char.casefold())
        if source_start is None and folded_start < next_cursor:
            source_start = index
        if folded_end <= next_cursor:
            source_end = index + 1
            break
        cursor = next_cursor
    return source_start or 0, source_end


def _iter_glob_matches(root, pattern, recursive, stats, *, files_only=False):
    if root.is_file():
        yield root
        return

    def walk(directory):
        try:
            entries = sorted(
                os.scandir(directory),
                key=lambda item: (item.name.casefold(), item.name),
            )
        except OSError:
            stats["restricted_skipped"] += 1
            return
        for entry in entries:
            try:
                if entry.is_symlink():
                    stats["restricted_skipped"] += 1
                    continue
                is_dir = entry.is_dir(follow_symlinks=False)
                is_file = entry.is_file(follow_symlinks=False)
            except OSError:
                stats["restricted_skipped"] += 1
                continue
            path = Path(entry.path)
            if fnmatch(entry.name, pattern) and (is_file or (is_dir and not files_only)):
                yield path
            if recursive and is_dir:
                yield from walk(path)

    yield from walk(root)


def _execute_file_grep(request, allowed_roots=None, denied_roots=None, denied_files=None):
    read_roots = _file_read_allowed_roots(allowed_roots)
    raw_root = request.get("root")
    root = _resolve_request_path(raw_root, read_roots)
    if root is None:
        reason = "invalid_persona_alias" if _persona_alias_invalid(raw_root) else "missing_root"
        return _base_result(tool_id="file_grep", status="rejected", reason=reason)

    display_root = _display_path(root)
    denial = _permission_denial(
        root, read_roots, denied_roots, denied_files, access="read"
    )
    if denial:
        result = _base_result(tool_id="file_grep", status="rejected", reason=denial)
        result["root"] = display_root
        return result
    if not root.exists():
        result = _base_result(tool_id="file_grep", status="rejected", reason="root_not_found")
        result["root"] = display_root
        return result
    if not (root.is_file() or root.is_dir()):
        result = _base_result(tool_id="file_grep", status="rejected", reason="invalid_root_type")
        result["root"] = display_root
        return result

    query = str(request.get("query") or "")
    if not query:
        result = _base_result(tool_id="file_grep", status="rejected", reason="missing_query")
        result["root"] = display_root
        return result
    if (
            len(query) > FILE_GREP_MAX_QUERY_CHARS
            or "\x00" in query
            or "\r" in query
            or "\n" in query):
        result = _base_result(tool_id="file_grep", status="rejected", reason="invalid_query")
        result["root"] = display_root
        return result

    file_pattern = _clean(request.get("file_pattern")) or "*"
    pattern_denial = _file_glob_pattern_denial(file_pattern)
    if pattern_denial:
        result = _base_result(tool_id="file_grep", status="rejected", reason=pattern_denial)
        result["root"] = display_root
        return result
    recursive = request.get("recursive") is not False
    case_sensitive = request.get("case_sensitive") is True
    context_lines = _bounded_int(
        request.get("context_lines"), 0, 0, FILE_GREP_MAX_CONTEXT_LINES
    )
    max_results = _bounded_int(
        request.get("max_results"),
        FILE_GREP_DEFAULT_MAX_RESULTS,
        1,
        FILE_GREP_MAX_RESULTS,
    )
    encoding = _clean(request.get("encoding")) or "utf-8"
    matches = []
    scanned_files = 0
    scanned_bytes = 0
    restricted_skipped = 0
    skipped_files = 0
    stop_reason = ""
    raw_memory_match_count = 0
    discovered_files = 0
    discovery_stats = {"restricted_skipped": 0}
    for candidate in _iter_glob_matches(
            root, file_pattern, recursive, discovery_stats, files_only=True):
        if discovered_files >= FILE_GREP_MAX_FILES:
            stop_reason = "max_files"
            break
        discovered_files += 1
        try:
            path = candidate.resolve()
            if root.is_dir():
                path.relative_to(root)
        except (OSError, ValueError):
            restricted_skipped += 1
            continue
        if _permission_denial(
                path,
                read_roots,
                denied_roots,
                denied_files,
                access="read"):
            restricted_skipped += 1
            continue
        try:
            size = path.stat().st_size
        except OSError:
            skipped_files += 1
            continue
        if size > FILE_GREP_MAX_FILE_BYTES:
            skipped_files += 1
            continue
        if scanned_bytes + size > FILE_GREP_MAX_BYTES:
            stop_reason = "max_bytes"
            break

        scanned_files += 1
        scanned_bytes += size
        file_match_start = len(matches)
        file_raw_memory_start = raw_memory_match_count
        previous = deque(maxlen=context_lines)
        pending = []
        limit_reached = False
        try:
            with path.open("r", encoding=encoding, errors="strict") as handle:
                for line_number, line in enumerate(handle, start=1):
                    if "\x00" in line:
                        raise UnicodeError("binary_nul_detected")
                    context_line, context_was_truncated, _, _ = _grep_text_window(line)
                    for item in list(pending):
                        item["context_after"].append(context_line)
                        if context_was_truncated:
                            item["context_truncated"] = True
                        item["_remaining"] -= 1
                        if item["_remaining"] <= 0:
                            item.pop("_remaining", None)
                            pending.remove(item)
                    match_span = _literal_match_span(line, query, case_sensitive)
                    if not limit_reached and match_span is not None:
                        display_line, line_truncated, line_char_start, line_char_end = (
                            _grep_text_window(line, match_span=match_span)
                        )
                        display_path = _display_path(path)
                        item = {
                            "path": display_path,
                            "line_number": line_number,
                            "line": display_line,
                            "line_truncated": line_truncated,
                            "line_char_start": line_char_start,
                            "line_char_end": line_char_end,
                            "match_char_start": match_span[0],
                            "match_char_end": match_span[1],
                            "context_before": [value for value, _ in previous],
                            "context_after": [],
                        }
                        if any(was_truncated for _, was_truncated in previous):
                            item["context_truncated"] = True
                        matches.append(item)
                        if _is_memory_storage_path(path):
                            raw_memory_match_count += 1
                        if context_lines:
                            item["_remaining"] = context_lines
                            pending.append(item)
                        if len(matches) >= max_results:
                            limit_reached = True
                            stop_reason = "max_results"
                    previous.append((context_line, context_was_truncated))
        except (OSError, UnicodeError, LookupError):
            del matches[file_match_start:]
            raw_memory_match_count = file_raw_memory_start
            skipped_files += 1
            continue
        finally:
            for item in pending:
                item.pop("_remaining", None)
        if limit_reached:
            break

    restricted_skipped += discovery_stats["restricted_skipped"]
    coverage_complete = not stop_reason and not restricted_skipped and not skipped_files
    result = _base_result(tool_id="file_grep", status="ok")
    result.update({
        "root": display_root,
        "query": query,
        "file_pattern": file_pattern,
        "recursive": recursive,
        "case_sensitive": case_sensitive,
        "context_lines": context_lines,
        "encoding": encoding,
        "matches": matches,
        "result_count": len(matches),
        "max_results": max_results,
        "coverage_complete": coverage_complete,
        "has_more": bool(stop_reason),
        "stop_reason": stop_reason,
        "scanned_files": scanned_files,
        "scanned_bytes": scanned_bytes,
        "restricted_skipped": restricted_skipped,
        "skipped_files": skipped_files,
        "read_mode": "bounded",
        "window_kind": "file_grep_bounded",
        "evidence_refs": [f"file_grep:{display_root}:{query}"],
    })
    if display_root.startswith(PERSONA_READ_ALIAS):
        result["host_root"] = str(root)
    if raw_memory_match_count:
        result["read_semantics"] = "raw_inspection"
        result["raw_memory_match_count"] = raw_memory_match_count
        result["memory_lifecycle_effects"] = []
    if not matches:
        result["reason"] = (
            "search_no_results" if coverage_complete else "search_no_results_partial"
        )
    return result


def _is_git_tracked(path):
    try:
        relative = path.relative_to(WORKSPACE_ROOT)
    except ValueError:
        return False
    try:
        completed = subprocess.run(
            [
                "git",
                "-C",
                str(WORKSPACE_ROOT),
                "ls-files",
                "--error-unmatch",
                relative.as_posix(),
            ],
            capture_output=True,
            text=True,
            timeout=5,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return False
    return completed.returncode == 0


def _normalise_patch_text(raw_patch):
    text = str(raw_patch or "").strip()
    if not text:
        return ""
    if "\\n" in text:
        text = text.replace("\\n", "\n")
    lines = text.splitlines()
    if lines and lines[0].strip().startswith("```"):
        lines = lines[1:]
    if lines and lines[-1].strip().startswith("```"):
        lines = lines[:-1]
    return "\n".join(lines).strip()


def _split_text_lines(text):
    if text == "":
        return [], False
    final_newline = text.endswith("\n")
    body = text[:-1] if final_newline else text
    if body == "":
        return [], final_newline
    return body.split("\n"), final_newline


def _parse_hunk_header(line):
    match = re.match(
        r"^@@ -(?P<old_start>\d+)(?:,(?P<old_count>\d+))? "
        r"\+(?P<new_start>\d+)(?:,(?P<new_count>\d+))? @@",
        line,
    )
    if not match:
        raise ValueError("invalid_patch_format")
    return int(match.group("old_start"))


def _apply_unified_diff(text, patch):
    patch_lines = [line.rstrip("\r") for line in patch.splitlines()]
    original, final_newline = _split_text_lines(text)
    output = []
    cursor = 0
    index = 0
    hunk_seen = False
    added = 0
    removed = 0

    while index < len(patch_lines):
        line = patch_lines[index]
        if not line.startswith("@@"):
            index += 1
            continue

        hunk_seen = True
        old_start = _parse_hunk_header(line)
        target = max(0, old_start - 1)
        if target < cursor:
            raise ValueError("patch_hunk_overlap")
        output.extend(original[cursor:target])
        cursor = target
        index += 1

        while index < len(patch_lines) and not patch_lines[index].startswith("@@"):
            diff_line = patch_lines[index]
            if diff_line.startswith("\\ No newline"):
                index += 1
                continue
            if diff_line == "":
                raise ValueError("invalid_patch_line")
            marker = diff_line[0]
            body = diff_line[1:]
            if marker == " ":
                if cursor >= len(original) or original[cursor] != body:
                    raise ValueError("patch_context_mismatch")
                output.append(original[cursor])
                cursor += 1
            elif marker == "-":
                if cursor >= len(original) or original[cursor] != body:
                    raise ValueError("patch_context_mismatch")
                cursor += 1
                removed += 1
            elif marker == "+":
                output.append(body)
                added += 1
            elif diff_line.startswith(("--- ", "+++ ")):
                pass
            else:
                raise ValueError("invalid_patch_line")
            index += 1

    if not hunk_seen:
        raise ValueError("invalid_patch_format")

    output.extend(original[cursor:])
    result = "\n".join(output)
    if final_newline:
        result += "\n"
    return result, added, removed


def _execute_file_edit(request, allowed_roots=None, denied_roots=None, denied_files=None):
    raw_path = request.get("path")
    if _clean(raw_path).lower().startswith(PERSONA_READ_ALIAS):
        return _base_result(
            tool_id="file_edit", status="rejected", reason="persona_alias_read_only"
        )
    path = _resolve_request_path(raw_path, allowed_roots)
    if path is None:
        return _base_result(tool_id="file_edit", status="rejected", reason="missing_path")

    denial = _permission_denial(
        path, allowed_roots, denied_roots, denied_files, access="write"
    )
    if denial:
        result = _base_result(tool_id="file_edit", status="rejected", reason=denial)
        result["path"] = str(path)
        return result

    if allowed_roots is None and not _is_git_tracked(path):
        result = _base_result(
            tool_id="file_edit",
            status="rejected",
            reason="not_tracked_or_allowlisted",
        )
        result["path"] = str(path)
        return result

    if not path.exists():
        result = _base_result(tool_id="file_edit", status="rejected", reason="file_not_found")
        result["path"] = str(path)
        return result
    if not path.is_file():
        result = _base_result(tool_id="file_edit", status="rejected", reason="not_a_file")
        result["path"] = str(path)
        return result

    purpose = _clean(request.get("purpose") or request.get("reason"))
    if not purpose:
        result = _base_result(tool_id="file_edit", status="rejected", reason="missing_purpose")
        result["path"] = str(path)
        return result

    patch = _normalise_patch_text(request.get("patch") or request.get("diff"))
    if not patch:
        result = _base_result(tool_id="file_edit", status="rejected", reason="missing_patch")
        result["path"] = str(path)
        return result

    encoding = _clean(request.get("encoding")) or "utf-8"
    try:
        original = _read_text_file(path, encoding)
    except (OSError, UnicodeError) as exc:
        result = _base_result(tool_id="file_edit", status="rejected", reason="text_read_failed")
        result["path"] = str(path)
        result["detail"] = str(exc)
        return result

    try:
        updated, added, removed = _apply_unified_diff(original, patch)
    except ValueError as exc:
        result = _base_result(tool_id="file_edit", status="rejected", reason=str(exc))
        result["path"] = str(path)
        return result

    try:
        path.write_text(updated, encoding=encoding, newline="\n")
    except OSError as exc:
        result = _base_result(tool_id="file_edit", status="failed", reason="write_failed")
        result["path"] = str(path)
        result["detail"] = str(exc)
        return result

    result = _base_result(tool_id="file_edit", status="ok")
    result.update({
        "path": str(path),
        "purpose": purpose,
        "risk_level": _clean(request.get("risk_level")) or "high",
        "change_summary": "applied unified diff",
        "lines_added": added,
        "lines_removed": removed,
        "chars_before": len(original),
        "chars_after": len(updated),
        "encoding": encoding,
        "evidence_refs": [f"file_edit:{path}"],
    })
    return result


def _execute_file_write(request, allowed_roots=None, denied_roots=None, denied_files=None):
    raw_path = request.get("path")
    if _clean(raw_path).lower().startswith(PERSONA_READ_ALIAS):
        return _base_result(
            tool_id="file_write", status="rejected", reason="persona_alias_read_only"
        )
    path = _resolve_request_path(raw_path, allowed_roots)
    if path is None:
        return _base_result(tool_id="file_write", status="rejected", reason="missing_path")

    denial = _permission_denial(
        path, allowed_roots, denied_roots, denied_files, access="write"
    )
    if denial:
        result = _base_result(tool_id="file_write", status="rejected", reason=denial)
        result["path"] = str(path)
        return result

    if path.exists() and not path.is_file():
        result = _base_result(tool_id="file_write", status="rejected", reason="not_a_file")
        result["path"] = str(path)
        return result

    purpose = _clean(request.get("purpose") or request.get("reason"))
    if not purpose:
        result = _base_result(tool_id="file_write", status="rejected", reason="missing_purpose")
        result["path"] = str(path)
        return result

    if "content" not in request:
        result = _base_result(tool_id="file_write", status="rejected", reason="missing_content")
        result["path"] = str(path)
        return result

    content = str(request.get("content") or "")
    encoding = _clean(request.get("encoding")) or "utf-8"
    existed = path.exists()
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content, encoding=encoding, newline="\n")
    except (OSError, UnicodeError) as exc:
        result = _base_result(tool_id="file_write", status="failed", reason="write_failed")
        result["path"] = str(path)
        result["detail"] = str(exc)
        return result

    result = _base_result(tool_id="file_write", status="ok")
    result.update({
        "path": str(path),
        "purpose": purpose,
        "risk_level": _clean(request.get("risk_level")) or "high",
        "change_summary": "wrote file content",
        "created": not existed,
        "overwritten": existed,
        "chars_written": len(content),
        "encoding": encoding,
        "evidence_refs": [f"file_write:{path}"],
    })
    return result


def _coerce_output_text(value):
    if value is None:
        return ""
    if isinstance(value, bytes):
        text, _meta = _decode_shell_stream(value, "output")
        return text
    return _sanitize_model_visible_output(str(value))


def _sanitize_model_visible_output(text):
    text = str(text or "").replace("\r\n", "\n").replace("\r", "\n")
    if "\ufffd" in text:
        return SHELL_REPLACEMENT_HIDDEN
    return text


def _unique_encodings(values):
    encodings = []
    seen = set()
    for value in values:
        encoding = str(value or "").strip()
        if not encoding:
            continue
        key = encoding.lower()
        if key in seen:
            continue
        seen.add(key)
        encodings.append(encoding)
    return encodings


def _device_encoding(fd):
    try:
        return os.device_encoding(fd)
    except (OSError, ValueError):
        return ""


def _preferred_encoding():
    try:
        return locale.getpreferredencoding(False)
    except (LookupError, ValueError):
        return ""


def _shell_output_encoding_candidates():
    candidates = ["utf-8-sig", "utf-8"]
    if os.name == "nt":
        candidates.extend([
            _device_encoding(2),
            _device_encoding(1),
            "oem",
            "mbcs",
            _preferred_encoding(),
            "gbk",
            "cp936",
        ])
    else:
        candidates.append(_preferred_encoding())
    return _unique_encodings(candidates)


def _decode_shell_stream(value, stream_name):
    meta = {}
    if value is None:
        return "", meta
    if not isinstance(value, bytes):
        return _sanitize_model_visible_output(str(value)), meta

    meta[f"{stream_name}_bytes_len"] = len(value)
    meta[f"{stream_name}_bytes_sha256"] = hashlib.sha256(value).hexdigest()
    if not value:
        meta[f"{stream_name}_encoding"] = "empty"
        return "", meta

    for encoding in _shell_output_encoding_candidates():
        try:
            text = value.decode(encoding, errors="strict")
        except (LookupError, UnicodeDecodeError):
            continue
        meta[f"{stream_name}_encoding"] = encoding.lower()
        return _sanitize_model_visible_output(text), meta

    meta[f"{stream_name}_encoding"] = "undecodable"
    return SHELL_UNDECODABLE_TEMPLATE.format(stream_name=stream_name), meta


def _truncate_output(text, limit=SHELL_OUTPUT_LIMIT):
    text = _coerce_output_text(text).replace("\r\n", "\n")
    if len(text) <= limit:
        return text, False
    return text[:limit], True


def _decode_and_truncate_shell_stream(value, stream_name):
    text, meta = _decode_shell_stream(value, stream_name)
    text, truncated = _truncate_output(text)
    return text, truncated, meta


def _resolve_shell_cwd(raw_cwd, allowed_roots):
    if _clean(raw_cwd):
        return _resolve_request_path(raw_cwd, allowed_roots)
    return _resolved_roots(allowed_roots)[0]


def _split_list_text(value):
    if value in (None, ""):
        return []
    return [
        item.strip()
        for item in re.split(r"[,，、;；|\n]+", str(value))
        if item.strip()
    ]


def _clean_list(values):
    if isinstance(values, (list, tuple)):
        return [str(item) for item in values if str(item or "").strip()]
    return _split_list_text(values)


def _validate_path_list(
        raw_value,
        allowed_roots,
        denied_roots,
        denied_files,
        field_name,
        *,
        access="read"):
    paths = []
    for raw_path in _split_list_text(raw_value):
        if (
                access != "read"
                and _clean(raw_path).lower().startswith(PERSONA_READ_ALIAS)):
            return [], f"{field_name}_persona_alias_read_only"
        path = _resolve_request_path(raw_path, allowed_roots)
        if path is None:
            continue
        denial = _permission_denial(
            path,
            allowed_roots,
            denied_roots,
            denied_files,
            access=access,
        )
        if denial:
            return [], f"{field_name}_{denial}"
        if not path.exists():
            return [], f"{field_name}_not_found"
        paths.append(path)
    if not paths:
        return [], f"missing_{field_name}"
    return paths, ""


def _execute_shell_command(
        request,
        allowed_roots=None,
        denied_roots=None,
        denied_files=None):
    raw_cwd = request.get("cwd")
    cwd = _resolve_shell_cwd(raw_cwd, allowed_roots)
    if cwd is None:
        return _base_result(tool_id="shell_command", status="rejected", reason="missing_cwd")

    denial = _permission_denial(
        cwd, allowed_roots, denied_roots, denied_files, access="shell"
    )
    if denial:
        result = _base_result(tool_id="shell_command", status="rejected", reason=denial)
        result["cwd"] = str(cwd)
        return result

    if not cwd.exists():
        result = _base_result(tool_id="shell_command", status="rejected", reason="cwd_not_found")
        result["cwd"] = str(cwd)
        return result
    if not cwd.is_dir():
        result = _base_result(tool_id="shell_command", status="rejected", reason="cwd_not_directory")
        result["cwd"] = str(cwd)
        return result

    command = str(request.get("command") or "").strip()
    if not command:
        result = _base_result(tool_id="shell_command", status="rejected", reason="missing_command")
        result["cwd"] = str(cwd)
        return result

    purpose = _clean(request.get("purpose") or request.get("reason"))
    if not purpose:
        result = _base_result(tool_id="shell_command", status="rejected", reason="missing_purpose")
        result["cwd"] = str(cwd)
        result["command"] = command
        return result

    timeout_ms = _bounded_int(request.get("timeout_ms"), 10000, 500, 30000)
    try:
        completed = subprocess.run(
            command,
            cwd=str(cwd),
            shell=True,
            capture_output=True,
            timeout=timeout_ms / 1000,
            check=False,
        )
    except subprocess.TimeoutExpired as exc:
        stdout, stdout_truncated, stdout_meta = _decode_and_truncate_shell_stream(exc.stdout, "stdout")
        stderr, stderr_truncated, stderr_meta = _decode_and_truncate_shell_stream(exc.stderr, "stderr")
        result = _base_result(
            tool_id="shell_command",
            status="timeout",
            reason="command_timeout",
        )
        result.update({
            "cwd": str(cwd),
            "command": command,
            "purpose": purpose,
            "risk_level": _clean(request.get("risk_level")) or "low",
            "timeout_ms": timeout_ms,
            "stdout": stdout,
            "stderr": stderr,
            "stdout_truncated": stdout_truncated,
            "stderr_truncated": stderr_truncated,
        })
        result.update(stdout_meta)
        result.update(stderr_meta)
        return result
    except OSError as exc:
        result = _base_result(tool_id="shell_command", status="failed", reason="command_failed")
        result.update({"cwd": str(cwd), "command": command, "detail": str(exc)})
        return result

    stdout, stdout_truncated, stdout_meta = _decode_and_truncate_shell_stream(completed.stdout, "stdout")
    stderr, stderr_truncated, stderr_meta = _decode_and_truncate_shell_stream(completed.stderr, "stderr")
    status = "ok" if completed.returncode == 0 else "failed"
    result = _base_result(tool_id="shell_command", status=status)
    result.update({
        "cwd": str(cwd),
        "command": command,
        "purpose": purpose,
        "risk_level": _clean(request.get("risk_level")) or "low",
        "timeout_ms": timeout_ms,
        "exit_code": completed.returncode,
        "stdout": stdout,
        "stderr": stderr,
        "stdout_truncated": stdout_truncated,
        "stderr_truncated": stderr_truncated,
        "evidence_refs": [f"shell_command:{cwd}"],
    })
    result.update(stdout_meta)
    result.update(stderr_meta)
    if status != "ok":
        result["reason"] = "nonzero_exit"
    return result


def _subagent_dispatch_result(status="rejected", reason="", task_goal="", **extra):
    result = _base_result(
        tool_id="subagent_dispatch",
        status=status,
        reason=reason,
    )
    if task_goal:
        result["task_goal"] = task_goal
    result.update(extra)
    return result


def _subagent_write_requested(task_mode, write_scope_raw):
    return task_mode.lower() in SUBAGENT_WRITE_MODES or bool(_clean(write_scope_raw))


def _subagent_write_paths(request, task_goal, allowed_paths,
                          allowed_roots, denied_roots, denied_files):
    task_mode = _clean(request.get("task_mode") or request.get("mode")) or "read_only"
    write_scope_raw = request.get("write_scope")
    if not _subagent_write_requested(task_mode, write_scope_raw):
        return task_mode, [], None
    if not _clean(write_scope_raw):
        return task_mode, [], _subagent_dispatch_result(
            reason="missing_write_scope",
            task_goal=task_goal,
        )

    write_paths, denial = _validate_path_list(
        write_scope_raw,
        allowed_roots,
        denied_roots,
        denied_files,
        "write_scope",
        access="write",
    )
    if denial:
        return task_mode, [], _subagent_dispatch_result(
            reason=denial,
            task_goal=task_goal,
        )
    for path in write_paths:
        if not any(path.is_relative_to(root) for root in allowed_paths):
            return task_mode, [], _subagent_dispatch_result(
                reason="write_scope_outside_allowed_paths",
                task_goal=task_goal,
                write_scope=[str(item) for item in write_paths],
            )
    return task_mode, write_paths, None


def _build_subagent_dispatch_payload(request, allowed_roots, denied_roots, denied_files):
    task_goal = _clean(request.get("task_goal") or request.get("task") or request.get("goal"))
    if not task_goal:
        return None, None, _subagent_dispatch_result(reason="missing_task_goal")

    expected_artifacts = _clean(request.get("expected_artifacts"))
    if not expected_artifacts:
        return None, None, _subagent_dispatch_result(
            reason="missing_expected_artifacts",
            task_goal=task_goal,
        )
    allowed_paths, denial = _validate_path_list(
        request.get("allowed_paths") or request.get("paths") or request.get("scope"),
        _file_read_allowed_roots(allowed_roots),
        denied_roots,
        denied_files,
        "allowed_paths",
        access="read",
    )
    if denial:
        return None, None, _subagent_dispatch_result(
            reason=denial,
            task_goal=task_goal,
        )
    task_mode, write_paths, rejection = _subagent_write_paths(
        request,
        task_goal,
        allowed_paths,
        allowed_roots,
        denied_roots,
        denied_files,
    )
    if rejection:
        return None, None, rejection

    purpose = _clean(request.get("purpose") or request.get("reason"))
    timeout_ms = _bounded_int(request.get("timeout_ms"), 300000, 10000, 600000)
    payload = {
        "task_goal": task_goal,
        "input_materials": _clean(request.get("input_materials")),
        "allowed_paths": [str(path) for path in allowed_paths],
        "forbidden": _clean(request.get("forbidden")),
        "expected_artifacts": expected_artifacts,
        "validation_commands": _clean(request.get("validation_commands")),
        "task_mode": task_mode,
        "write_scope": [str(path) for path in write_paths],
        "agent_role": _clean(request.get("agent_role") or request.get("role")),
        "max_turns": _clean(request.get("max_turns")),
        "handoff_required": _clean(request.get("handoff_required")),
        "purpose": purpose,
    }
    return payload, timeout_ms, None


def _call_subagent_backend(payload, timeout_ms, subagent_dispatch_fn):
    if subagent_dispatch_fn is None:
        return None, _subagent_dispatch_result(
            reason="backend_unavailable",
            task_goal=payload["task_goal"],
            task_mode=payload["task_mode"],
            allowed_paths=payload["allowed_paths"],
            expected_artifacts=payload["expected_artifacts"],
        )

    try:
        backend_result = subagent_dispatch_fn(payload, timeout_ms)
    except TimeoutError as exc:
        return None, _subagent_dispatch_result(
            status="timeout",
            reason="subagent_timeout",
            task_goal=payload["task_goal"],
            timeout_ms=timeout_ms,
            detail=str(exc),
        )
    except Exception as exc:
        return None, _subagent_dispatch_result(
            status="failed",
            reason="subagent_backend_failed",
            task_goal=payload["task_goal"],
            detail=str(exc),
        )
    return backend_result or {}, None


def _subagent_backend_report(payload, timeout_ms, backend_result):
    status = _clean(backend_result.get("status")) or "ok"
    result = _base_result(tool_id="subagent_dispatch", status=status)
    result.update({
        "task_goal": payload["task_goal"],
        "task_mode": payload["task_mode"],
        "allowed_paths": payload["allowed_paths"],
        "write_scope": payload["write_scope"],
        "expected_artifacts": payload["expected_artifacts"],
        "validation_commands": payload["validation_commands"],
        "timeout_ms": timeout_ms,
        "backend_session_id": _clean(backend_result.get("backend_session_id")),
        "conclusion": _clean(backend_result.get("conclusion")),
        "modified_files": _clean_list(backend_result.get("modified_files")),
        "test_evidence": _clean_list(backend_result.get("test_evidence")),
        "risks": _clean_list(backend_result.get("risks")),
        "unfinished": _clean_list(backend_result.get("unfinished")),
        "evidence_refs": [f"subagent_dispatch:{payload['task_goal']}"],
    })
    if status != "ok" and backend_result.get("reason"):
        result["reason"] = _clean(backend_result.get("reason"))
    return result


def _execute_subagent_dispatch(
        request,
        allowed_roots=None,
        denied_roots=None,
        denied_files=None,
        subagent_dispatch_fn=None):
    payload, timeout_ms, rejection = _build_subagent_dispatch_payload(
        request,
        allowed_roots,
        denied_roots,
        denied_files,
    )
    if rejection:
        return rejection
    backend_result, failure = _call_subagent_backend(
        payload,
        timeout_ms,
        subagent_dispatch_fn,
    )
    if failure:
        return failure
    return _subagent_backend_report(payload, timeout_ms, backend_result)


class _VisibleTextExtractor(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.hidden_depth = 0
        self.in_title = False
        self.title_parts = []
        self.text_parts = []

    def handle_starttag(self, tag, attrs):
        tag = tag.lower()
        if tag in WEB_HIDDEN_TAGS:
            self.hidden_depth += 1
        if tag == "title":
            self.in_title = True
        if tag in {"p", "br", "div", "section", "article", "li", "tr", "h1",
                   "h2", "h3", "h4", "h5", "h6"}:
            self.text_parts.append("\n")

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag in WEB_HIDDEN_TAGS and self.hidden_depth:
            self.hidden_depth -= 1
        if tag == "title":
            self.in_title = False
        if tag in {"p", "div", "section", "article", "li", "tr"}:
            self.text_parts.append("\n")

    def handle_data(self, data):
        text = " ".join(str(data or "").split())
        if not text:
            return
        if self.in_title:
            self.title_parts.append(text)
        if not self.hidden_depth:
            self.text_parts.append(text)
            self.text_parts.append(" ")

    @property
    def title(self):
        return " ".join(self.title_parts).strip()

    @property
    def text(self):
        text = "".join(self.text_parts)
        lines = [" ".join(line.split()) for line in text.splitlines()]
        return "\n".join(line for line in lines if line).strip()


class _DuckDuckGoParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.results = []
        self._link_href = ""
        self._link_text = []
        self._snippet_text = []
        self._in_result_link = False
        self._in_snippet = False

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs or [])
        classes = attrs.get("class", "")
        if tag.lower() == "a" and "result__a" in classes:
            self._in_result_link = True
            self._link_href = attrs.get("href", "")
            self._link_text = []
        if "result__snippet" in classes:
            self._in_snippet = True
            self._snippet_text = []

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "a" and self._in_result_link:
            title = " ".join(" ".join(self._link_text).split())
            url = _unwrap_duckduckgo_url(self._link_href)
            if title and url:
                self.results.append({"title": title, "url": url, "snippet": ""})
            self._in_result_link = False
            self._link_href = ""
            self._link_text = []
        if self._in_snippet and tag in {"a", "div", "td"}:
            snippet = " ".join(" ".join(self._snippet_text).split())
            if snippet and self.results and not self.results[-1].get("snippet"):
                self.results[-1]["snippet"] = snippet
            self._in_snippet = False
            self._snippet_text = []

    def handle_data(self, data):
        if self._in_result_link:
            self._link_text.append(str(data or ""))
        if self._in_snippet:
            self._snippet_text.append(str(data or ""))


class _BingParser(HTMLParser):
    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.results = []
        self._in_result = False
        self._in_title_link = False
        self._in_snippet = False
        self._link_href = ""
        self._title_parts = []
        self._snippet_parts = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs or [])
        classes = attrs.get("class", "")
        tag = tag.lower()
        if tag == "li" and "b_algo" in classes:
            self._in_result = True
            self._link_href = ""
            self._title_parts = []
            self._snippet_parts = []
            self._in_title_link = False
            self._in_snippet = False
        if self._in_result and tag == "a" and not self._link_href:
            href = attrs.get("href", "")
            if href:
                self._in_title_link = True
                self._link_href = href
        if self._in_result and tag == "p":
            self._in_snippet = True

    def handle_endtag(self, tag):
        tag = tag.lower()
        if tag == "a" and self._in_title_link:
            self._in_title_link = False
        if tag == "p" and self._in_snippet:
            self._in_snippet = False
        if tag == "li" and self._in_result:
            title = " ".join(" ".join(self._title_parts).split())
            snippet = " ".join(" ".join(self._snippet_parts).split())
            url = html.unescape(_clean(self._link_href))
            if title and url:
                self.results.append({
                    "title": title,
                    "url": url,
                    "snippet": snippet,
                    "source": "bing_html",
                })
            self._in_result = False
            self._in_title_link = False
            self._in_snippet = False
            self._link_href = ""
            self._title_parts = []
            self._snippet_parts = []

    def handle_data(self, data):
        if self._in_title_link:
            self._title_parts.append(str(data or ""))
        if self._in_snippet:
            self._snippet_parts.append(str(data or ""))


class _SearchBlockedError(RuntimeError):
    pass


def _looks_like_search_challenge(body_text):
    text = " ".join(str(body_text or "").lower().split())
    if not text:
        return False
    challenge_markers = (
        "unfortunately, bots use duckduckgo too",
        "please complete the following challenge",
        "complete the following challenge",
        "anomaly-modal",
        "bot verification",
    )
    return any(marker in text for marker in challenge_markers)


def _unwrap_duckduckgo_url(raw_url):
    url = html.unescape(_clean(raw_url))
    parsed = urlparse(url)
    if parsed.netloc.endswith("duckduckgo.com") and parsed.path.startswith("/l/"):
        nested = parse_qs(parsed.query).get("uddg", [""])[0]
        return nested or url
    return url


def _is_private_or_local_host(host):
    lowered = (host or "").strip().lower().strip("[]")
    if lowered in {"localhost", "localhost.localdomain"}:
        return True
    try:
        ip = ipaddress.ip_address(lowered)
    except ValueError:
        return False
    return (
        ip.is_loopback
        or ip.is_link_local
        or ip.is_private
        or ip.is_reserved
        or ip.is_multicast
        or ip.is_unspecified
    )


def _url_denial(raw_url, missing_reason="missing_url"):
    url = _clean(raw_url)
    if not url:
        return missing_reason, ""
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"}:
        return "invalid_url_scheme", url
    if not parsed.hostname:
        return "invalid_url", url
    if parsed.username or parsed.password:
        return "credentials_in_url_denied", url
    if _is_private_or_local_host(parsed.hostname):
        return "local_or_private_host_denied", url

    path = unquote(parsed.path or "").lower()
    suffix = Path(path).suffix.lower()
    if suffix in WEB_DOWNLOAD_SUFFIXES:
        return "download_like_url", url
    padded_path = path if path.startswith("/") else f"/{path}"
    if any(fragment in padded_path for fragment in WEB_LOGIN_FRAGMENTS):
        return "interactive_or_login_page_denied", url
    return "", url


class _UnsafeWebTargetError(ValueError):
    def __init__(self, reason, url):
        self.reason = str(reason or "unsafe_web_target")
        self.url = str(url or "")
        super().__init__(f"{self.reason}:{self.url}")


def _resolved_url_denial(raw_url):
    denial, url = _url_denial(raw_url)
    if denial:
        return denial, url
    parsed = urlparse(url)
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    try:
        addresses = {
            item[4][0]
            for item in socket.getaddrinfo(
                parsed.hostname,
                port,
                type=socket.SOCK_STREAM,
            )
            if item and len(item) > 4 and item[4]
        }
    except OSError:
        return "host_resolution_failed", url
    if not addresses:
        return "host_resolution_failed", url
    if any(_is_private_or_local_host(address) for address in addresses):
        return "local_or_private_host_denied", url
    return "", url


def _require_public_web_target(raw_url):
    denial, url = _resolved_url_denial(raw_url)
    if denial:
        raise _UnsafeWebTargetError(denial, url)
    return url


class _PublicWebRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        safe_url = _require_public_web_target(urljoin(req.full_url, newurl))
        return super().redirect_request(req, fp, code, msg, headers, safe_url)


def _decode_body(body_bytes, content_type):
    content_type = content_type or ""
    match = re.search(r"charset=([\w.-]+)", content_type, flags=re.I)
    encoding = match.group(1) if match else "utf-8"
    try:
        return body_bytes.decode(encoding, errors="replace")
    except LookupError:
        return body_bytes.decode("utf-8", errors="replace")


def _extract_visible_text(raw_text, content_type):
    if "html" not in (content_type or "").lower():
        return "", " ".join(str(raw_text or "").split())
    parser = _VisibleTextExtractor()
    parser.feed(raw_text or "")
    return parser.title, parser.text


def _default_fetch_url(url, timeout_ms):
    timeout = _bounded_int(timeout_ms, 5000, 500, 15000) / 1000
    url = _require_public_web_target(url)
    request = Request(
        url,
        headers={
            "User-Agent": "UPSP-GeneralTool/1.0 (+https://local.invalid)",
            "Accept": "text/html,text/plain;q=0.9,*/*;q=0.1",
        },
    )
    opener = build_opener(_PublicWebRedirectHandler())
    with opener.open(request, timeout=timeout) as response:
        final_url = _require_public_web_target(response.geturl() or url)
        content_type = response.headers.get("Content-Type", "")
        body = response.read(WEB_MAX_BYTES + 1)
        return {
            "status_code": getattr(response, "status", response.getcode()),
            "content_type": content_type,
            "final_url": final_url,
            "body_bytes": body[:WEB_MAX_BYTES],
            "source_bytes_incomplete": len(body) > WEB_MAX_BYTES,
        }


def _jina_reader_fetch_url(url, timeout_ms):
    reader_url = f"https://r.jina.ai/{url}"
    payload = _default_fetch_url(reader_url, timeout_ms)
    payload = dict(payload or {})
    payload["final_url"] = url
    payload["reader_url"] = reader_url
    return payload


def _normalise_fetch_payload(payload, fallback_url):
    payload = payload or {}
    content_type = _clean(payload.get("content_type")) or "text/plain"
    if (
            "html" not in content_type.lower()
            and "text/plain" not in content_type.lower()
            and "text/" not in content_type.lower()):
        return None, "unsupported_content_type"

    title = _clean(payload.get("title"))
    content = payload.get("content") or payload.get("text")
    if content is None and "body_bytes" in payload:
        raw_text = _decode_body(payload.get("body_bytes") or b"", content_type)
        extracted_title, content = _extract_visible_text(raw_text, content_type)
        title = title or extracted_title
    content = str(content or "").replace("\r\n", "\n").strip()
    return {
        "status_code": payload.get("status_code"),
        "content_type": content_type,
        "source_url": _clean(payload.get("final_url")) or fallback_url,
        "title": title,
        "content": content,
        "source_bytes_incomplete": bool(
            payload.get("source_bytes_incomplete") or payload.get("truncated_bytes")
        ),
    }, ""


def _classify_web_fetch_content_quality(payload):
    payload = payload or {}
    content = str(payload.get("content") or "").strip()
    title = str(payload.get("title") or "").strip()
    lowered = re.sub(r"\s+", " ", content.lower())
    if not content:
        return "empty", "empty_body"
    js_markers = (
        "enable javascript",
        "requires javascript",
        "javascript is required",
        "you need to enable javascript",
        "please enable javascript",
        "app shell",
    )
    if any(marker in lowered for marker in js_markers):
        return "js_shell", "javascript_required_or_app_shell"
    blocked_markers = (
        "access denied",
        "captcha",
        "cloudflare",
        "are you a robot",
        "bot detection",
        "please complete the security check",
    )
    if any(marker in lowered for marker in blocked_markers):
        return "blocked_or_antibot", "blocked_or_antibot_page"
    if len(content) < 10:
        return "insufficient_text", "body_too_short"
    return "ok", ""


def _web_fetch_backend_functions(web_fetch_fn=None):
    return {
        "direct_fetch": web_fetch_fn or _default_fetch_url,
        "jina_reader": _jina_reader_fetch_url,
    }


def _execute_web_fetch(request, web_fetch_fn=None):
    denial, url = _url_denial(request.get("url"))
    if denial:
        result = _base_result(tool_id="web_fetch", status="rejected", reason=denial)
        if url:
            result["url"] = url
        return result

    try:
        char_start = _int_or_none(request.get("char_start"))
    except ValueError as exc:
        result = _base_result(tool_id="web_fetch", status="rejected", reason=str(exc))
        result["url"] = url
        return result
    raw_find_text = request.get("find_text")
    find_text = str(raw_find_text or "").strip()
    if raw_find_text is not None and not find_text:
        result = _base_result(
            tool_id="web_fetch", status="rejected", reason="invalid_find_text"
        )
        result["url"] = url
        return result
    if char_start is not None and find_text:
        result = _base_result(
            tool_id="web_fetch",
            status="rejected",
            reason="char_start_find_text_conflict",
        )
        result["url"] = url
        return result
    if char_start is not None and char_start < 1:
        result = _base_result(
            tool_id="web_fetch", status="rejected", reason="char_start_out_of_range"
        )
        result["url"] = url
        return result
    if len(find_text) > WEB_FIND_TEXT_MAX_CHARS:
        result = _base_result(
            tool_id="web_fetch", status="rejected", reason="find_text_too_long"
        )
        result.update({"url": url, "find_text_max_chars": WEB_FIND_TEXT_MAX_CHARS})
        return result
    expected_content_sha256 = str(
        request.get("source_content_sha256") or ""
    ).strip().lower()
    if expected_content_sha256 and not re.fullmatch(
            r"[0-9a-f]{64}", expected_content_sha256):
        result = _base_result(
            tool_id="web_fetch",
            status="rejected",
            reason="invalid_source_content_sha256",
        )
        result["url"] = url
        return result
    if char_start is not None and not expected_content_sha256:
        result = _base_result(
            tool_id="web_fetch",
            status="rejected",
            reason="source_content_sha256_required_for_continuation",
        )
        result["url"] = url
        return result
    timeout_ms = _bounded_int(request.get("timeout_ms"), 5000, 500, 15000)
    if web_fetch_fn is None:
        network_denial, _ = _resolved_url_denial(url)
        if network_denial:
            result = _base_result(
                tool_id="web_fetch",
                status=(
                    "rejected"
                    if network_denial == "local_or_private_host_denied"
                    else "failed"
                ),
                reason=network_denial,
            )
            result["url"] = url
            return result
    window_chars = _general_tool_window_params()["web_fetch_window_chars"]
    backend_functions = _web_fetch_backend_functions(web_fetch_fn=web_fetch_fn)
    skip_backend_ids = request.get("_web_skip_backend_ids") or []
    use_persistent_health = web_fetch_fn is None
    backend_ids = _ordered_web_backends(
        "web_fetch",
        skip_backend_ids=skip_backend_ids,
        ignore_health=not use_persistent_health,
    )
    attempts = []
    health = _load_web_backend_health() if use_persistent_health else {}
    payload = None
    selected_backend = ""
    for backend_id in backend_ids:
        fetch_fn = backend_functions.get(backend_id)
        if not fetch_fn:
            before, after = _record_web_backend_hard_fail(
                health,
                "web_fetch",
                backend_id,
                reason="backend_missing",
            )
            attempts.append(_web_backend_attempt(
                backend_id,
                "failed",
                reason="backend_missing",
                fail_count_before=before,
                fail_count_after=after,
            ))
            continue
        try:
            raw_payload = fetch_fn(url, timeout_ms)
        except _UnsafeWebTargetError as exc:
            result = _base_result(
                tool_id="web_fetch",
                status="rejected",
                reason=exc.reason,
            )
            result["url"] = exc.url or url
            return result
        except HTTPError as exc:
            before, after = _record_web_backend_hard_fail(
                health,
                "web_fetch",
                backend_id,
                reason="http_error",
                detail=str(exc),
            )
            attempts.append(_web_backend_attempt(
                backend_id,
                "failed",
                reason="http_error",
                detail=str(exc),
                status_code=exc.code,
                fail_count_before=before,
                fail_count_after=after,
            ))
            continue
        except (OSError, URLError, TimeoutError) as exc:
            before, after = _record_web_backend_hard_fail(
                health,
                "web_fetch",
                backend_id,
                reason="fetch_failed",
                detail=str(exc),
            )
            attempts.append(_web_backend_attempt(
                backend_id,
                "failed",
                reason="fetch_failed",
                detail=str(exc),
                fail_count_before=before,
                fail_count_after=after,
            ))
            continue

        normalised_payload, denial = _normalise_fetch_payload(raw_payload, url)
        if denial:
            before, after = _record_web_backend_hard_fail(
                health,
                "web_fetch",
                backend_id,
                reason=denial,
            )
            attempts.append(_web_backend_attempt(
                backend_id,
                "failed",
                reason=denial,
                fail_count_before=before,
                fail_count_after=after,
            ))
            continue
        payload = normalised_payload
        selected_backend = backend_id
        attempts.append(_web_backend_attempt(backend_id, "ok"))
        break
    if use_persistent_health:
        _save_web_backend_health(health)

    if payload is None:
        result = _base_result(
            tool_id="web_fetch",
            status="failed",
            reason="web_backend_exhausted",
        )
        result.update({
            "url": url,
            "selected_backend": "",
            "backend_attempts": attempts,
            "detail": _web_attempt_summary(attempts),
        })
        return result

    content = payload["content"]
    source_content_sha256 = hashlib.sha256(content.encode("utf-8")).hexdigest()
    source_bytes_incomplete = bool(payload["source_bytes_incomplete"])
    if (
            expected_content_sha256
            and expected_content_sha256 != source_content_sha256):
        result = _base_result(
            tool_id="web_fetch", status="source_changed", reason="source_changed"
        )
        result.update({
            "url": url,
            "source_url": payload["source_url"],
            "selected_backend": selected_backend,
            "backend_attempts": attempts,
            "title": payload["title"],
            "status_code": payload["status_code"],
            "content_type": payload["content_type"],
            "source_content_sha256": source_content_sha256,
            "expected_source_content_sha256": expected_content_sha256,
            "source_bytes_incomplete": source_bytes_incomplete,
            "has_more": False,
            "next_char_start": None,
            "fetched_at": result["executed_at"],
            "evidence_refs": [],
        })
        if char_start is not None:
            result["char_start"] = char_start
        if find_text:
            result["find_text"] = find_text
        return result
    total_lines = len(content.splitlines())
    total_chars = len(content)
    if char_start is not None and char_start > total_chars:
        result = _base_result(
            tool_id="web_fetch",
            status="rejected",
            reason="char_start_out_of_range",
        )
        result.update({
            "url": url,
            "source_url": payload["source_url"],
            "selected_backend": selected_backend,
            "backend_attempts": attempts,
            "source_content_sha256": source_content_sha256,
            "source_bytes_incomplete": source_bytes_incomplete,
            "total_chars": total_chars,
            "char_start": char_start,
            "has_more": False,
            "next_char_start": None,
            "evidence_refs": [],
        })
        return result
    if char_start is not None:
        window = _char_bounded_window(content, char_start, len(content), window_chars)
        range_requested = {"type": "char_cursor", "char_start": max(1, char_start)}
    elif find_text:
        match = re.search(re.escape(find_text), content, flags=re.IGNORECASE)
        if match is None:
            result = _base_result(
                tool_id="web_fetch", status="not_found", reason="find_text_not_found"
            )
            result.update({
                "url": url,
                "source_url": payload["source_url"],
                "selected_backend": selected_backend,
                "backend_attempts": attempts,
                "title": payload["title"],
                "status_code": payload["status_code"],
                "content_type": payload["content_type"],
                "find_text": find_text,
                "match_found": False,
                "source_content_sha256": source_content_sha256,
                "source_bytes_incomplete": source_bytes_incomplete,
                "has_more": False,
                "next_char_start": None,
                "fetched_at": result["executed_at"],
                "evidence_refs": [],
            })
            return result
        line_start = content.rfind("\n", 0, match.start()) + 1
        if match.end() - line_start > window_chars:
            line_start = match.start()
        window = _char_bounded_window(
            content, line_start + 1, len(content), window_chars
        )
        range_requested = {
            "type": "find_text",
            "find_text": find_text,
            "match_char_start": match.start() + 1,
            "match_char_end": match.end(),
        }
    else:
        window = _line_bounded_window(
            content,
            1,
            len(content.splitlines()),
            window_chars,
        )
        range_requested = None
    applied_char_start = window["char_start"]
    applied_char_end = window["char_end"]
    next_char_start = window["next_char_start"]
    next_char_end = window["next_char_end"]
    if applied_char_start in (None, "") and window["content"]:
        applied_char_start = max(1, char_start or 1)
        applied_char_end = applied_char_start + len(window["content"]) - 1
    if (
            next_char_start in (None, "")
            and bool(window["has_more"])
            and applied_char_end not in (None, "")
            and int(applied_char_end) < total_chars):
        next_char_start = int(applied_char_end) + 1
        next_char_end = min(total_chars, next_char_start + window_chars - 1)

    content_quality, content_quality_reason = _classify_web_fetch_content_quality(payload)
    evidence_refs = (
        [f"web_fetch:{payload['source_url']}"]
        if content_quality == "ok"
        else []
    )
    result = _base_result(tool_id="web_fetch", status="ok")
    result.update({
        "url": url,
        "source_url": payload["source_url"],
        "selected_backend": selected_backend,
        "backend_attempts": attempts,
        "title": payload["title"],
        "status_code": payload["status_code"],
        "content_type": payload["content_type"],
        "content": window["content"],
        "chars": len(window["content"]),
        "read_mode": "bounded",
        "window_kind": "web_fetch_bounded",
        "window_boundary": window["window_boundary"],
        "returned_chars": len(window["content"]),
        "window_chars": window_chars,
        "has_more": next_char_start not in (None, ""),
        "total_lines": total_lines,
        "total_chars": total_chars,
        "source_content_sha256": source_content_sha256,
        "source_bytes_incomplete": source_bytes_incomplete,
        "content_quality": content_quality,
        "content_quality_reason": content_quality_reason,
        "evidence_disabled_reason": (
            f"web_fetch_content_quality:{content_quality}"
            if content_quality != "ok"
            else ""
        ),
        "range_requested": range_requested,
        "range_applied": {
            "type": "char",
            "char_start": applied_char_start,
            "char_end": applied_char_end,
        } if applied_char_start not in (None, "") else None,
        "start_line": window["start_line"],
        "end_line": window["end_line"],
        "char_start": applied_char_start,
        "char_end": applied_char_end,
        "line_overlong": bool(window["line_overlong"]),
        "next_start_line": window["next_start_line"],
        "next_char_start": next_char_start,
        "next_char_end": next_char_end,
        "fetched_at": result["executed_at"],
        "evidence_refs": evidence_refs,
    })
    if find_text:
        result.update({
            "find_text": find_text,
            "match_found": True,
            "match_char_start": range_requested["match_char_start"],
            "match_char_end": range_requested["match_char_end"],
        })
    return result


def _default_search_web(query, max_results, timeout_ms):
    blocked_sources = []
    failed_sources = []

    search_url = f"https://duckduckgo.com/html/?q={quote_plus(query)}"
    try:
        payload = _default_fetch_url(search_url, timeout_ms)
        body_text = _decode_body(
            payload.get("body_bytes") or b"",
            payload.get("content_type", ""),
        )
        if _looks_like_search_challenge(body_text):
            blocked_sources.append("duckduckgo_html")
        else:
            parser = _DuckDuckGoParser()
            parser.feed(body_text)
            results = parser.results[:max_results]
            for item in results:
                item.setdefault("source", "duckduckgo_html")
            if results:
                return results
    except (OSError, URLError, TimeoutError) as exc:
        failed_sources.append(f"duckduckgo_html:{exc}")

    search_url = f"https://www.bing.com/search?q={quote_plus(query)}"
    try:
        payload = _default_fetch_url(search_url, timeout_ms)
        body_text = _decode_body(
            payload.get("body_bytes") or b"",
            payload.get("content_type", ""),
        )
        if _looks_like_search_challenge(body_text):
            blocked_sources.append("bing_html")
        else:
            parser = _BingParser()
            parser.feed(body_text)
            if parser.results:
                return parser.results[:max_results]
    except (OSError, URLError, TimeoutError) as exc:
        failed_sources.append(f"bing_html:{exc}")

    if blocked_sources:
        detail = "blocked_sources=" + ",".join(blocked_sources)
        if failed_sources:
            detail += "; failed_sources=" + ";".join(failed_sources)
        raise _SearchBlockedError(detail)
    if failed_sources:
        raise URLError("; ".join(failed_sources))
    return []


def _html_search_web(query, max_results, timeout_ms):
    return _default_search_web(query, max_results, timeout_ms)


def _ddgs_search_web(query, max_results, timeout_ms):
    try:
        try:
            from ddgs import DDGS
        except ImportError:
            from duckduckgo_search import DDGS
    except ImportError as exc:
        raise URLError(f"ddgs package unavailable: {exc}") from exc

    results = []
    timeout_seconds = _bounded_int(timeout_ms, 5000, 500, 15000) / 1000
    try:
        with DDGS(timeout=timeout_seconds) as ddgs:
            for item in ddgs.text(query, max_results=max_results):
                if not isinstance(item, dict):
                    continue
                results.append({
                    "title": item.get("title") or item.get("heading") or "",
                    "url": item.get("href") or item.get("url") or "",
                    "snippet": item.get("body") or item.get("snippet") or "",
                    "source": "ddgs",
                })
                if len(results) >= max_results:
                    break
    except Exception as exc:
        raise URLError(f"ddgs search failed: {exc}") from exc
    return results


WEB_SEARCH_STOPWORDS = frozenset({
    "api",
    "docs",
    "doc",
    "model",
    "models",
    "pricing",
    "price",
    "prices",
    "official",
    "page",
    "public",
    "text",
    "agent",
    "framework",
    "benchmark",
    "compare",
    "comparison",
    "the",
    "and",
    "for",
    "with",
})


def _url_domain(url):
    try:
        parsed = urlparse(str(url or ""))
    except ValueError:
        return ""
    host = str(parsed.netloc or "").lower()
    if "@" in host:
        host = host.rsplit("@", 1)[-1]
    host = host.split(":", 1)[0].strip(".")
    if host.startswith("www."):
        host = host[4:]
    return host


def _query_domain_tokens(query):
    tokens = []
    for token in re.findall(r"[a-z0-9][a-z0-9-]{2,}", str(query or "").lower()):
        if token in WEB_SEARCH_STOPWORDS:
            continue
        if token.isdigit():
            continue
        tokens.append(token.replace("-", ""))
    return tokens


def _official_candidate_for_query(item, query):
    domain = _url_domain((item or {}).get("url"))
    if not domain:
        return False
    compact_domain = re.sub(r"[^a-z0-9]", "", domain.lower())
    for token in _query_domain_tokens(query):
        if token and token in compact_domain:
            return True
    return False


def _normalise_search_results(items, max_results, query=""):
    results = []
    for item in items or []:
        if not isinstance(item, dict):
            continue
        url = _clean(item.get("url"))
        denial, clean_url = _url_denial(url, missing_reason="missing_result_url")
        if denial:
            continue
        title = _clean(item.get("title")) or clean_url
        snippet = " ".join(str(item.get("snippet") or "").split())
        domain = _url_domain(clean_url)
        source = _clean(item.get("source")) or "web_search"
        official_candidate = _official_candidate_for_query({"url": clean_url}, query)
        results.append({
            "title": title,
            "url": clean_url,
            "domain": domain,
            "snippet": snippet,
            "source": source,
            "source_backend": source,
            "candidate_kind": (
                "official_domain_candidate"
                if official_candidate else "search_candidate"
            ),
            "official_candidate": official_candidate,
        })
    results = sorted(
        enumerate(results),
        key=lambda pair: (0 if pair[1].get("official_candidate") else 1, pair[0]),
    )
    return [item for _index, item in results[:max_results]]


def _web_search_backend_functions(web_search_fn=None):
    return {
        "ddgs": web_search_fn or _ddgs_search_web,
        "html_fallback": _html_search_web,
    }


def _execute_web_search(request, web_search_fn=None):
    query = _clean(request.get("query"))
    if not query:
        return _base_result(tool_id="web_search", status="rejected", reason="missing_query")
    window_results = _general_tool_window_params()["web_search_window_results"]
    timeout_ms = _bounded_int(request.get("timeout_ms"), 5000, 500, 15000)
    backend_functions = _web_search_backend_functions(web_search_fn=web_search_fn)
    skip_backend_ids = request.get("_web_skip_backend_ids") or []
    use_persistent_health = web_search_fn is None
    backend_ids = _ordered_web_backends(
        "web_search",
        skip_backend_ids=skip_backend_ids,
        ignore_health=not use_persistent_health,
    )
    attempts = []
    health = _load_web_backend_health() if use_persistent_health else {}
    raw_results = None
    selected_backend = ""
    for backend_id in backend_ids:
        search_fn = backend_functions.get(backend_id)
        if not search_fn:
            before, after = _record_web_backend_hard_fail(
                health,
                "web_search",
                backend_id,
                reason="backend_missing",
            )
            attempts.append(_web_backend_attempt(
                backend_id,
                "failed",
                reason="backend_missing",
                fail_count_before=before,
                fail_count_after=after,
            ))
            continue
        try:
            raw_results = search_fn(query, window_results, timeout_ms)
        except _SearchBlockedError as exc:
            before, after = _record_web_backend_hard_fail(
                health,
                "web_search",
                backend_id,
                reason="search_blocked",
                detail=str(exc),
            )
            attempts.append(_web_backend_attempt(
                backend_id,
                "failed",
                reason="search_blocked",
                detail=str(exc),
                fail_count_before=before,
                fail_count_after=after,
            ))
            continue
        except (OSError, URLError, TimeoutError) as exc:
            before, after = _record_web_backend_hard_fail(
                health,
                "web_search",
                backend_id,
                reason="search_failed",
                detail=str(exc),
            )
            attempts.append(_web_backend_attempt(
                backend_id,
                "failed",
                reason="search_failed",
                detail=str(exc),
                fail_count_before=before,
                fail_count_after=after,
            ))
            continue
        selected_backend = backend_id
        attempts.append(_web_backend_attempt(backend_id, "ok"))
        break
    if use_persistent_health:
        _save_web_backend_health(health)

    if raw_results is None:
        result = _base_result(
            tool_id="web_search",
            status="failed",
            reason="web_backend_exhausted",
        )
        result.update({
            "query": query,
            "detail": _web_attempt_summary(attempts),
            "results": [],
            "result_count": 0,
            "selected_backend": "",
            "backend_attempts": attempts,
        })
        return result
    results = _normalise_search_results(raw_results, window_results, query=query)
    content_lines = []
    for index, item in enumerate(results, start=1):
        content_lines.append(f"{index}. {item['title']}")
        content_lines.append(f"   url={item['url']}")
        if item.get("domain"):
            content_lines.append(f"   domain={item['domain']}")
        content_lines.append(f"   kind={item.get('candidate_kind') or 'search_candidate'}")
        if item.get("source_backend"):
            content_lines.append(f"   source_backend={item.get('source_backend')}")
        if item.get("snippet"):
            content_lines.append(f"   snippet={item['snippet']}")

    result = _base_result(tool_id="web_search", status="ok")
    result.update({
        "query": query,
        "selected_backend": selected_backend,
        "backend_attempts": attempts,
        "results": results,
        "result_count": len(results),
        "window_results": window_results,
        "read_mode": "bounded",
        "window_kind": "web_search_bounded",
        "window_boundary": "result_record",
        "has_more": "unknown",
        "content": "\n".join(content_lines),
        "fetched_at": result["executed_at"],
        "evidence_refs": [f"web_search:{query}"],
    })
    return result


def execute_general_tool_call(
        request,
        allowed_roots=None,
        denied_roots=None,
        denied_files=None,
        web_fetch_fn=None,
        web_search_fn=None,
        subagent_dispatch_fn=None):
    """Execute an enabled general tool call and return a general_tool_result."""
    request = request or {}
    tool_id = _clean(request.get("tool_id")) or "file_read"
    if tool_id == "file_read":
        return attach_evidence_handle(
            _execute_file_read(request, allowed_roots, denied_roots, denied_files)
        )
    if tool_id == "file_glob":
        return attach_evidence_handle(
            _execute_file_glob(request, allowed_roots, denied_roots, denied_files)
        )
    if tool_id == "file_grep":
        return attach_evidence_handle(
            _execute_file_grep(request, allowed_roots, denied_roots, denied_files)
        )
    if tool_id == "file_edit":
        return attach_evidence_handle(
            _execute_file_edit(request, allowed_roots, denied_roots, denied_files)
        )
    if tool_id == "file_write":
        return attach_evidence_handle(
            _execute_file_write(request, allowed_roots, denied_roots, denied_files)
        )
    if tool_id == "shell_command":
        return attach_evidence_handle(
            _execute_shell_command(
                request,
                allowed_roots,
                denied_roots,
                denied_files,
            )
        )
    if tool_id == "web_fetch":
        return attach_evidence_handle(
            _with_web_fetch_error_hint(
                _execute_web_fetch(request, web_fetch_fn=web_fetch_fn)
            )
        )
    if tool_id == "web_search":
        return attach_evidence_handle(
            _execute_web_search(request, web_search_fn=web_search_fn)
        )
    if tool_id == "subagent_dispatch":
        return attach_evidence_handle(
            _execute_subagent_dispatch(
                request,
                allowed_roots,
                denied_roots,
                denied_files,
                subagent_dispatch_fn=subagent_dispatch_fn,
            )
        )
    return attach_evidence_handle(
        _base_result(tool_id=tool_id, status="rejected", reason="handler_missing")
    )


def _with_web_fetch_error_hint(result):
    """Attach the executable correction contract at the web-fetch producer."""
    result = dict(result or {})
    reason = str(result.get("reason") or "").strip()
    if reason == "source_changed":
        current_hash = str(result.get("source_content_sha256") or "").strip()
        result["error_hint"] = {
            "kind": "state_conflict",
            "retry": "after_correction",
            "attempted": {
                key: result.get(key)
                for key in (
                    "url", "char_start", "find_text",
                    "expected_source_content_sha256",
                )
                if result.get(key) not in (None, "")
            },
            "current": {
                "url": result.get("url"),
                "source_content_sha256": current_hash,
                "source_bytes_incomplete": bool(
                    result.get("source_bytes_incomplete")
                ),
            },
            "expected": {
                "url": result.get("url"),
                "source_content_sha256": current_hash,
                "fields": ["url", "source_content_sha256"],
            },
            "next_action": (
                "使用相同 URL 和 current.source_content_sha256，且不带 "
                "char_start/find_text，重新读取首窗。"
            ),
        }
    elif reason == "find_text_not_found":
        current_hash = str(result.get("source_content_sha256") or "").strip()
        result["error_hint"] = {
            "kind": "validation",
            "retry": "after_correction",
            "attempted": {
                "url": result.get("url"),
                "find_text": result.get("find_text"),
                "source_content_sha256": current_hash,
            },
            "current": {
                "source_content_sha256": current_hash,
                "source_bytes_incomplete": bool(
                    result.get("source_bytes_incomplete")
                ),
            },
            "expected": {
                "fields": ["url", "find_text", "source_content_sha256"],
                "source_content_sha256": current_hash,
            },
            "next_action": (
                "不要原样重复定位词；改用另一字面定位词、其他公开来源，"
                "或基于已取得证据继续。"
            ),
        }
    elif reason in {"web_backend_exhausted", "host_resolution_failed"}:
        result["error_hint"] = {
            "kind": "transient_external",
            "retry": "later",
            "attempted": {
                "url": result.get("url"),
                "backend_ids": [
                    str(item.get("backend_id") or "")
                    for item in result.get("backend_attempts") or []
                    if isinstance(item, dict) and item.get("backend_id")
                ],
            },
            "current": {"reason": reason},
            "expected": {},
            "next_action": (
                "保留失败事实，稍后重试或改用其他公开来源；不要声称已读取成功。"
            ),
        }
    elif reason == "local_or_private_host_denied":
        result["error_hint"] = {
            "kind": "permission_security",
            "retry": "never_without_new_evidence",
            "attempted": {"url": result.get("url")},
            "current": {"reason": reason},
            "expected": {"next_state": "public_http_or_https_url"},
            "next_action": "停止访问该目标；只使用可公开解析的 HTTP(S) 地址。",
        }
    return result


def format_general_tool_result(result):
    """Render a compact result block for now-cache and next-iteration handoff."""
    result = result or {}
    lines = [
        "[general_tool_result]",
        f"tool_id={result.get('tool_id', '')}",
        f"tool_family={result.get('tool_family', '')}",
        f"tool_class={result.get('tool_class', '')}",
        f"status={result.get('status', '')}",
        f"source={result.get('source', '')}",
        f"result_kind={result.get('result_kind', '')}",
        f"backend_type={result.get('backend_type', '')}",
        f"handler={result.get('handler', '')}",
    ]
    for key in (
            "permission_scope",
            "evidence_handle",
            "path",
            "root",
            "pattern",
            "file_pattern",
            "recursive",
            "case_sensitive",
            "context_lines",
            "cwd",
            "command",
            "task_goal",
            "url",
            "source_url",
            "source_content_sha256",
            "expected_source_content_sha256",
            "query",
            "title",
            "status_code",
            "content_type",
            "result_count",
            "returned_chars",
            "window_chars",
            "window_results",
            "window_boundary",
            "max_chars",
            "max_results",
            "has_more",
            "coverage_complete",
            "stop_reason",
            "scanned_files",
            "scanned_bytes",
            "restricted_skipped",
            "skipped_files",
            "read_semantics",
            "raw_memory_match_count",
            "purpose",
            "requested_start_line",
            "requested_end_line",
            "line_start",
            "line_end",
            "char_start",
            "char_end",
            "line_overlong",
            "next_line_start",
            "next_char_start",
            "next_char_end",
            "find_text",
            "match_found",
            "match_char_start",
            "match_char_end",
            "source_bytes_incomplete",
            "risk_level",
            "change_summary",
            "lines_added",
            "lines_removed",
            "chars_before",
            "chars_after",
            "chars_written",
            "created",
            "overwritten",
            "exit_code",
            "timeout_ms",
            "backend_session_id",
            "stdout_truncated",
            "stderr_truncated",
            "danger_reason",
            "reason",
            "detail",
            "truncated"):
        if key in result and result.get(key) not in (None, ""):
            lines.append(f"{key}={result.get(key)}")
    for key in (
            "evidence_refs",
            "allowed_paths",
            "write_scope",
            "modified_files",
            "test_evidence",
            "risks",
            "unfinished"):
        if result.get(key):
            lines.append(f"{key}:")
            for item in result.get(key) or []:
                lines.append(f"- {item}")
    if result.get("conclusion"):
        lines.append("conclusion:")
        lines.append(str(result.get("conclusion")))
    for key in ("stdout", "stderr"):
        if result.get(key):
            lines.append(f"{key}:")
            lines.append(str(result.get(key)))
    if result.get("results"):
        lines.append("results:")
        for index, item in enumerate(result.get("results") or [], start=1):
            lines.append(
                f"{index}. title={item.get('title', '')}; "
                f"url={item.get('url', '')}; "
                f"snippet={item.get('snippet', '')}"
            )
    if result.get("matches"):
        lines.append("matches:")
        for index, item in enumerate(result.get("matches") or [], start=1):
            if result.get("tool_id") == "file_grep":
                lines.append(
                    f"{index}. path={item.get('path', '')}; "
                    f"line_number={item.get('line_number', '')}; "
                    f"line={item.get('line', '')}"
                )
            else:
                lines.append(
                    f"{index}. name={item.get('name', '')}; "
                    f"path={item.get('path', '')}; "
                    f"is_file={item.get('is_file', '')}; "
                    f"is_dir={item.get('is_dir', '')}"
                )
    content = result.get("content")
    if content:
        lines.append("content:")
        lines.append(str(content))
    return "\n".join(lines)


def _one_line(value, limit=220):
    text = re.sub(r"\s+", " ", str(value or "")).strip()
    if len(text) <= limit:
        return text
    return text[:max(0, limit - 3)] + "..."


def _line_count(value):
    text = str(value or "")
    if not text:
        return 0
    return len(text.splitlines())


def format_general_tool_material_entry(result):
    """Project read-tool body/candidates as material, separate from tool facts."""
    result = result or {}
    tool_id = _tool_id(result)
    status = _tool_status(result)
    if status not in {"ok", "success"}:
        return None
    content = read_tool_material_content(result)
    title = ""
    if tool_id == "file_read":
        title = str(result.get("path") or "").strip()
    elif tool_id == "file_glob":
        title = "file_glob root={} pattern={}".format(
            result.get("root") or "",
            result.get("pattern") or "",
        ).strip()
    elif tool_id == "file_grep":
        title = "file_grep root={} query={}".format(
            result.get("root") or "",
            result.get("query") or "",
        ).strip()
    elif tool_id == "web_fetch":
        title = str(
            result.get("title") or result.get("source_url") or result.get("url") or ""
        ).strip()
    elif tool_id == "web_search":
        title = "web_search query={}".format(result.get("query") or "").strip()
    else:
        return None
    if not content:
        return None
    entry = {
        "role": "system",
        "kind": "material",
        "content": content,
        "tool_id": tool_id,
        "material_source": "read_tool_result",
    }
    if title:
        entry["title"] = title
    path = str(result.get("path") or "").strip()
    if path:
        entry["path"] = path
    return entry


def _stream_fact_summary(result):
    result = result or {}
    parts = []
    stdout_lines = _line_count(result.get("stdout"))
    stderr_lines = _line_count(result.get("stderr"))
    if stdout_lines:
        text = f"标准输出约 {stdout_lines} 行"
        if result.get("stdout_truncated"):
            text += "，已截断"
        parts.append(text)
    if stderr_lines:
        text = f"错误输出约 {stderr_lines} 行"
        if result.get("stderr_truncated"):
            text += "，已截断"
        parts.append(text)
    if parts:
        return "；".join(parts)
    return result.get("reason") or result.get("detail") or _status_label(result.get("status")) or ""


def _visible_stream_excerpt(value, *, source_truncated=False, limit=520):
    text = _coerce_output_text(value)
    if not text:
        return ""
    text = text.replace("\r\n", "\n").replace("\r", "\n").strip("\n")
    if not text:
        return ""
    truncated = bool(source_truncated) or len(text) > limit
    excerpt = text[:limit].rstrip()
    if truncated:
        excerpt = excerpt + "\n...（摘录已截断）"
    return excerpt


def _fact_pairs(*pairs):
    return "; ".join(
        f"{key}={value}"
        for key, value in pairs
        if value not in (None, "", [])
    )


def _tool_status(result):
    return str((result or {}).get("status") or "unknown").strip() or "unknown"


def _tool_id(result):
    return str((result or {}).get("tool_id") or "general_tool").strip() or "general_tool"


def _status_label(status):
    value = str(status or "").strip().lower()
    return {
        "ok": "成功",
        "success": "成功",
        "failed": "失败",
        "error": "失败",
        "denied": "被拒绝",
        "rejected": "被拒绝",
        "invalid": "无效",
        "not_found": "未找到",
        "source_changed": "来源正文已变化",
        "unknown": "未知",
    }.get(value, str(status or "未知"))


def _tool_name_label(tool_id):
    return {
        "file_read": "文件读取",
        "file_glob": "文件名搜索",
        "file_grep": "文件正文搜索",
        "file_edit": "文件编辑",
        "file_write": "文件写入",
        "shell_command": "shell 命令",
        "web_fetch": "网页读取",
        "web_search": "网页搜索",
        "subagent_dispatch": "子 agent 调度",
    }.get(str(tool_id or "").strip(), "通用工具")


def _duplicate_tool_argument_summary(result):
    payload = result.get("tool_signature_payload")
    if not isinstance(payload, dict):
        return ""
    parts = []
    tool_id = str(payload.get("tool_id") or result.get("tool_id") or "").strip()
    if tool_id:
        parts.append(("tool_id", tool_id))
    arguments = payload.get("arguments")
    if isinstance(arguments, dict):
        for key in arguments:
            value = arguments.get(key)
            if value not in (None, "", []):
                parts.append((str(key), value))
    else:
        for key in sorted(payload):
            if key == "tool_id":
                continue
            value = payload.get(key)
            if value not in (None, "", []):
                parts.append((str(key), value))
    return _fact_pairs(*parts)


def _format_duplicate_general_tool_fact(result):
    reason = str(result.get("reason") or "").strip()
    if reason not in {
        "duplicate_tool_result_satisfied",
        "duplicate_tool_failure_repeated",
        "web_backend_exhausted_duplicate",
    }:
        return ""
    tool_id = _tool_id(result)
    status = _tool_status(result)
    lines = [
        f"本轮 {_tool_name_label(tool_id)}重复调用被拒绝。",
        f"处理结果：{status}。",
        f"失败原因：{reason}。",
    ]
    duplicate_of = str(result.get("duplicate_of_call_id") or "").strip()
    if duplicate_of:
        lines.append(f"重复对象：{duplicate_of}。")
    previous_status = str(result.get("previous_status") or "").strip()
    if previous_status:
        lines.append(f"上次状态：{previous_status}。")
    previous_reason = str(result.get("previous_reason") or "").strip()
    if previous_reason:
        lines.append(f"上次原因：{previous_reason}。")
    summary = _duplicate_tool_argument_summary(result)
    if summary:
        lines.append(f"参数摘要：{summary}。")
    if reason == "web_backend_exhausted_duplicate":
        lines.append("下一步：当前参数的已知网页后端都已失败；请换来源、换查询，或说明阻塞。")
    elif reason == "duplicate_tool_failure_repeated":
        lines.append("下一步：修正参数、换工具，或停止这条路径。")
    else:
        lines.append("下一步：直接使用已有工具事实，或改参数推进下一步。")
    return "\n".join(lines)


def _evidence_handle_fact_lines(result):
    if not result_supports_evidence(result):
        return []
    handle = evidence_handle_for_result(result)
    if not handle:
        return []
    return [f"证据引用：{handle}。"]


def _shell_command_evidence_fact_lines(result):
    if not result_supports_evidence(result):
        return []
    command = result.get("command")
    canonical = canonical_command_ref(command)
    lines = []
    if canonical:
        lines.append(f"可引用整条命令：command:{canonical}。")
    subcommands = shell_result_subcommands(result)
    if subcommands:
        rendered = []
        for item in subcommands[:6]:
            canonical_item = canonical_command_ref(item)
            if canonical_item:
                rendered.append(f"command:{canonical_item}")
        if rendered:
            lines.append(f"可引用子命令：{'；'.join(rendered)}。")
    return lines


def _path_normalization_fact_lines(result):
    original = str((result or {}).get("path_normalized_from") or "").strip()
    normalized = str((result or {}).get("path_normalized_to") or (result or {}).get("path") or "").strip()
    prefix = str((result or {}).get("path_alias_prefix") or "").strip()
    if not original or not normalized:
        return []
    lines = [
        f"路径已归一：{original} -> {normalized}。",
    ]
    if prefix:
        lines.append(f"后续写入同一输出根时不要重复加 {prefix} 前缀。")
    return lines


def _format_general_tool_fact_body(result):
    """Render the short model-visible fact for a general tool result."""
    result = result or {}
    duplicate_fact = _format_duplicate_general_tool_fact(result)
    if duplicate_fact:
        return duplicate_fact
    tool_id = _tool_id(result)
    status = _tool_status(result)
    if tool_id == "file_read":
        start = result.get("start_line")
        end = result.get("end_line")
        path = str(result.get("path") or "未指定文件").strip()
        lines = []
        if status == "ok":
            lines.append(f"已读取文件：{path}。")
            read_mode = str(result.get("read_mode") or "full").strip()
            total_lines = result.get("total_lines")
            total_chars = result.get("total_chars")
            if total_lines not in (None, "") and total_chars not in (None, ""):
                lines.append(f"文件总量：{total_lines} 行，{total_chars} 字符。")
            elif total_lines not in (None, ""):
                lines.append(f"文件总量：{total_lines} 行。")
            elif total_chars not in (None, ""):
                lines.append(f"文件总量：{total_chars} 字符。")
            if start not in (None, "") and end not in (None, ""):
                lines.append(f"读取范围：第 {start} 行到第 {end} 行。")
                if total_lines not in (None, ""):
                    lines.append(f"读取进度：第 {start}-{end} 行 / 共 {total_lines} 行。")
            if read_mode == "bounded":
                if result.get("line_overlong"):
                    lines.append("本次读取命中单行过长内容，已按完整行返回。")
                lines.append("本次读取只是一段工具窗口，不代表全文已读。")
                next_line_start = (
                    result.get("next_line_start")
                    if result.get("next_line_start") not in (None, "")
                    else result.get("next_start_line")
                )
                if result.get("has_more") and next_line_start not in (None, ""):
                    lines.append(
                        "继续读取请调用 "
                        f"file_read(path={path}, line_start={next_line_start})。"
                    )
                elif result.get("has_more") is False:
                    lines.append("当前文件读取已到末尾。")
            elif read_mode == "partial":
                lines.append("读取结果：模型显式范围读取。")
            else:
                lines.append("读取结果：全文。")
            if result.get("read_semantics") == "raw_inspection":
                lines.append(
                    "本次读取原始记忆文件只算 raw_inspection，不触发召回、"
                    "STM 重建、加热、续期、挂载或调用坐标更新。"
                )
        else:
            verdict = "被拒绝" if status in {"denied", "rejected"} else "失败"
            lines.extend([
                f"本轮尝试读取文件：{path}。",
                f"读取结果：{verdict}。",
            ])
            reason = str(result.get("reason") or "").strip()
            if reason:
                lines.append(f"失败原因：{reason}。")
            lines.append("这不能作为已读取证据。")
        return "\n".join(lines)

    if tool_id == "file_glob":
        status_label = _status_label(status)
        root = str(result.get("root") or "未指定目录").strip()
        pattern = str(result.get("pattern") or "未指定模式").strip()
        lines = []
        if status == "ok":
            lines.append("本轮已经完成文件名搜索。")
            lines.append(f"搜索目录：{root}。")
            lines.append(f"搜索模式：{pattern}。")
            lines.append(f"递归搜索：{bool(result.get('recursive'))}。")
            result_count = result.get("result_count")
            window_results = result.get("window_results", result.get("max_results"))
            if result_count not in (None, "") and window_results not in (None, ""):
                lines.append(f"结果窗口：{result_count}/{window_results}。")
            if result.get("has_more"):
                lines.append("当前结果窗口已满，仍可能有更多候选。")
            if not result.get("matches"):
                lines.append("当前搜索窗口没有找到候选文件。")
                lines.append(
                    "下一步可换更宽的 pattern、换 root，或显式 recursive=true 搜索子目录。"
                )
                lines.append("这不代表文件在整台机器上不存在。")
                return "\n".join(lines)
            lines.append("文件名搜索只返回候选路径，不代表文件正文已读。")
            lines.append("如需正文，应继续调用 file_read 并使用候选中的精确 path。")
            return "\n".join(lines)
        lines.extend([
            f"本轮尝试搜索文件：root={root}；pattern={pattern}。",
            f"搜索结果：{status_label}。",
        ])
        reason = str(result.get("reason") or result.get("detail") or "").strip()
        if reason:
            lines.append(f"失败原因：{reason}。")
        lines.append("这不能作为已经找到候选文件的证据。")
        return "\n".join(lines)

    if tool_id == "file_grep":
        status_label = _status_label(status)
        root = str(result.get("root") or "未指定路径").strip()
        query = str(result.get("query") or "未指定文本").strip()
        lines = []
        if status == "ok":
            lines.extend((
                "本轮已经完成文件正文字面搜索。",
                f"搜索根：{root}。",
                f"查询文本：{query}。",
                f"命中行数：{result.get('result_count', 0)}。",
                f"扫描覆盖完整：{bool(result.get('coverage_complete'))}。",
            ))
            if result.get("stop_reason"):
                lines.append(f"扫描停止原因：{result.get('stop_reason')}。")
            if result.get("restricted_skipped") or result.get("skipped_files"):
                lines.append("部分受限、二进制、超限或无法解码文件未被搜索。")
            if not result.get("matches"):
                lines.append("当前扫描范围没有命中正文。")
                if not result.get("coverage_complete"):
                    lines.append("覆盖不完整，因此不能据此声称目标在全部文件中不存在。")
            else:
                lines.append("命中正文已作为 material 返回；需要更完整上下文时再用 file_read 精确读取。")
            if result.get("read_semantics") == "raw_inspection":
                lines.append("命中原始记忆文件只算 raw_inspection，不触发召回、加热、续期、挂载或调用坐标更新。")
            return "\n".join(lines)
        lines.extend((
            f"本轮尝试搜索文件正文：root={root}；query={query}。",
            f"搜索结果：{status_label}。",
        ))
        reason = str(result.get("reason") or result.get("detail") or "").strip()
        if reason:
            lines.append(f"失败原因：{reason}。")
        lines.append("这不能作为已经读取或找到正文的证据。")
        return "\n".join(lines)

    if tool_id == "web_fetch":
        status_label = _status_label(status)
        lines = []
        if status == "ok":
            lines.append(f"本轮已经成功读取网页：{result.get('source_url') or result.get('url') or '未记录'}。")
            if result.get("selected_backend"):
                lines.append(f"读取后端：{result.get('selected_backend')}。")
            if result.get("title"):
                lines.append(f"标题：{result.get('title')}。")
            if result.get("source_content_sha256"):
                lines.append(
                    "正文身份：source_content_sha256="
                    f"{result.get('source_content_sha256')}。"
                )
            content_quality = str(result.get("content_quality") or "ok").strip()
            if content_quality and content_quality != "ok":
                reason = str(result.get("content_quality_reason") or "").strip()
                if reason:
                    lines.append(f"内容质量：{content_quality}（{reason}）。")
                else:
                    lines.append(f"内容质量：{content_quality}。")
                lines.append("这不能作为可靠网页正文证据；请换官方 URL、换搜索词，或改用更合适的来源。")
            total_lines = result.get("total_lines")
            total_chars = result.get("total_chars")
            if total_lines not in (None, "") and total_chars not in (None, ""):
                lines.append(f"已取得正文总量：{total_lines} 行，{total_chars} 字符。")
            returned = result.get("returned_chars")
            window_chars = result.get("window_chars", result.get("max_chars"))
            if returned not in (None, "") and window_chars not in (None, ""):
                lines.append(f"窗口字符：{returned}/{window_chars}。")
            start = result.get("start_line")
            end = result.get("end_line")
            if start not in (None, "") and end not in (None, ""):
                lines.append(f"正文范围：第 {start} 行到第 {end} 行。")
                if total_lines not in (None, ""):
                    lines.append(f"正文进度：第 {start}-{end} 行 / 共 {total_lines} 行。")
            if result.get("line_overlong"):
                lines.append("本次窗口命中单行过长内容，已按该行内 bounded 字符窗口返回。")
            char_start = result.get("char_start")
            char_end = result.get("char_end")
            if char_start not in (None, "") and char_end not in (None, ""):
                if total_chars not in (None, ""):
                    lines.append(f"字符进度：{char_start}-{char_end} / 共 {total_chars} 字符。")
                else:
                    lines.append(f"字符范围：{char_start}-{char_end}。")
            if result.get("source_bytes_incomplete"):
                lines.append("底层来源字节未完全返回；上述总量仅表示本次已取得正文。")
            if result.get("match_found"):
                lines.append(
                    f"定位词 {result.get('find_text')!r} 命中字符 "
                    f"{result.get('match_char_start')}-{result.get('match_char_end')}。"
                )
            if result.get("has_more"):
                lines.append("当前已取得正文仍有可执行的后续 bounded 窗口。")
                lines.append(
                    "本轮如需继续，应调用 web_fetch 并同时传 "
                    f"char_start={result.get('next_char_start')}、"
                    "source_content_sha256="
                    f"{result.get('source_content_sha256')}。"
                )
            else:
                lines.append("当前已取得正文读取已到末尾。")
                if result.get("source_bytes_incomplete"):
                    lines.extend([
                        "当前抓取结果已经没有可继续的字符游标；不要重复最后一个 char_start。",
                        "底层来源不完整，因此不能据此断言原网页已经完整读完。",
                        "如仍需定位内容，请改用 find_text、其他公开来源，或基于已取得证据继续。",
                    ])
            lines.append("这是网页正文窗口，不代表整页、整站或外部事实已经完整读取。")
            return "\n".join(lines)
        if status == "source_changed":
            lines.extend([
                f"网页正文已变化：{result.get('source_url') or result.get('url') or '未记录'}。",
                "旧定位参数绑定的正文哈希与本次重新抓取结果不一致，未使用旧坐标读取新正文。",
                "请使用相同 URL、当前 source_content_sha256，且不带 char_start/find_text，重新读取首窗。",
            ])
            if result.get("source_content_sha256"):
                lines.append(
                    "当前正文身份：source_content_sha256="
                    f"{result.get('source_content_sha256')}。"
                )
            return "\n".join(lines)
        if status == "not_found":
            lines.append(
                f"网页正文中未找到字面文本 {result.get('find_text')!r}。"
            )
            if result.get("source_content_sha256"):
                lines.append(
                    "本次检索正文身份：source_content_sha256="
                    f"{result.get('source_content_sha256')}。"
                )
            if result.get("source_bytes_incomplete"):
                lines.append("底层来源字节未完全返回；本次未命中不等于原网页不存在该文本。")
            lines.append("请更换定位词、改用其他公开来源，或基于已取得证据继续。")
            return "\n".join(lines)
        lines.extend([
            f"本轮尝试读取网页：{result.get('url') or '未记录'}。",
            f"读取结果：{status_label}。",
        ])
        reason = str(result.get("reason") or result.get("detail") or "").strip()
        if reason:
            lines.append(f"失败原因：{reason}。")
        if reason == "source_content_sha256_required_for_continuation":
            lines.append("续读必须同时原样复制上一结果的 source_content_sha256。")
        elif reason == "char_start_find_text_conflict":
            lines.append("char_start 与 find_text 只能选择一种定位方式。")
        elif reason == "find_text_too_long":
            lines.append(f"请把 find_text 缩短到 {WEB_FIND_TEXT_MAX_CHARS} 个字符以内。")
        elif reason == "char_start_out_of_range":
            lines.append("当前游标不在本次正文范围内；请从首窗重新读取或改用 find_text。")
        elif reason == "invalid_source_content_sha256":
            lines.append("请原样使用上一结果给出的 64 位 source_content_sha256。")
        if result.get("backend_attempts"):
            lines.append(f"后端尝试：{_web_attempt_summary(result.get('backend_attempts'))}。")
        lines.append("这不能作为已读取网页正文证据。")
        return "\n".join(lines)

    if tool_id == "web_search":
        status_label = _status_label(status)
        lines = []
        if status == "ok":
            lines.append("本轮已经完成网页搜索。")
            if result.get("query"):
                lines.append(f"查询：{result.get('query')}。")
            if result.get("selected_backend"):
                lines.append(f"搜索后端：{result.get('selected_backend')}。")
            result_count = result.get("result_count")
            window_results = result.get("window_results", result.get("max_results"))
            if result_count not in (None, "") and window_results not in (None, ""):
                lines.append(f"结果窗口：{result_count}/{window_results}。")
            lines.append("搜索结果只是候选来源，不代表网页正文已读。")
            lines.append("如需正文，应继续调用 web_fetch。")
            lines.append("官方资料、价格或技术文档任务应优先对官方域名候选调用 web_fetch。")
            lines.append("搜索不准时，换官方域名、已知 URL 或更具体查询重试。")
            return "\n".join(lines)
        lines.extend([
            "本轮尝试网页搜索。",
            f"搜索结果：{status_label}。",
        ])
        reason = str(result.get("reason") or result.get("detail") or "").strip()
        if reason:
            lines.append(f"失败原因：{reason}。")
        if result.get("backend_attempts"):
            lines.append(f"后端尝试：{_web_attempt_summary(result.get('backend_attempts'))}。")
        lines.append("这不能作为已读取网页正文证据。")
        return "\n".join(lines)

    if tool_id == "shell_command":
        summary = _stream_fact_summary(result) or status
        status_label = _status_label(status)
        exit_code = result.get("exit_code")
        lines = [
            f"本轮 shell 命令执行{status_label}。",
            f"工作目录：{result.get('cwd') or '未记录'}。",
            f"命令：{result.get('command') or '未记录'}。",
            f"退出码：{exit_code if exit_code not in (None, '') else '未记录'}。",
            f"结果摘要：{summary}。",
        ]
        lines.extend(_evidence_handle_fact_lines(result))
        lines.extend(_shell_command_evidence_fact_lines(result))
        stdout_excerpt = _visible_stream_excerpt(
            result.get("stdout"),
            source_truncated=result.get("stdout_truncated"),
        )
        stderr_excerpt = _visible_stream_excerpt(
            result.get("stderr"),
            source_truncated=result.get("stderr_truncated"),
        )
        if stdout_excerpt:
            lines.extend(["标准输出摘录：", stdout_excerpt])
        if stderr_excerpt:
            lines.extend(["错误输出摘录：", stderr_excerpt])
        return "\n".join(lines)

    if tool_id == "file_edit":
        summary = (
            result.get("change_summary")
            or result.get("summary")
            or result.get("reason")
            or status
        )
        lines = [
            f"本轮已经尝试编辑文件：{result.get('path') or '未记录'}。",
            f"编辑结果：{_status_label(status)}。",
            f"变更摘要：{summary}。",
        ]
        lines.extend(_path_normalization_fact_lines(result))
        lines.extend(_evidence_handle_fact_lines(result))
        if result.get("lines_added") not in (None, ""):
            lines.append(f"新增行数：{result.get('lines_added')}。")
        if result.get("lines_removed") not in (None, ""):
            lines.append(f"删除行数：{result.get('lines_removed')}。")
        return "\n".join(lines)

    if tool_id == "file_write":
        summary = (
            result.get("change_summary")
            or result.get("summary")
            or result.get("reason")
            or status
        )
        lines = [
            f"本轮已经写入文件：{result.get('path') or '未记录'}。",
            f"写入结果：{_status_label(status)}。",
            f"写入摘要：{summary}。",
        ]
        lines.extend(_path_normalization_fact_lines(result))
        lines.extend(_evidence_handle_fact_lines(result))
        if result.get("chars_written") not in (None, ""):
            lines.append(f"写入字符数：{result.get('chars_written')}。")
        if result.get("created") is True:
            lines.append("文件状态：本次新建。")
        elif result.get("overwritten") is True:
            lines.append("文件状态：本次覆盖既有文件。")
        return "\n".join(lines)

    summary = (
        result.get("conclusion")
        or result.get("content")
        or _first_result_snippet(result)
        or result.get("title")
        or result.get("reason")
        or result.get("detail")
        or result.get("message")
        or status
    )
    lines = [
        f"本轮已经执行{_tool_name_label(tool_id)}。",
        f"执行结果：{_status_label(status)}。",
    ]
    target = result.get("path") or result.get("url") or result.get("source_url")
    if target:
        lines.append(f"目标：{target}。")
    if result.get("query"):
        lines.append(f"查询：{result.get('query')}。")
    lines.append(f"摘要：{summary}。")
    return "\n".join(lines)


def format_general_tool_fact(result):
    """Render one general-tool fact with its stable evidence handle when usable."""
    fact = _format_general_tool_fact_body(result)
    evidence_lines = _evidence_handle_fact_lines(result)
    missing_lines = [line for line in evidence_lines if line not in fact]
    if missing_lines:
        return "\n".join([fact, *missing_lines])
    return fact


def _first_result_snippet(result):
    results = result.get("results") if isinstance(result, dict) else None
    if not isinstance(results, list):
        return ""
    for item in results:
        if not isinstance(item, dict):
            continue
        for key in ("snippet", "summary", "title"):
            value = str(item.get(key) or "").strip()
            if value:
                return value
    return ""
