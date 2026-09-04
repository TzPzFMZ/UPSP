"""Pure projection helpers for model-visible read-tool material text."""

from __future__ import annotations

from typing import Any


SUCCESS_STATUSES = {"ok", "success"}


def _tool_id(result: dict[str, Any]) -> str:
    return str(result.get("tool_id") or "").strip()


def _status(result: dict[str, Any]) -> str:
    return str(result.get("status") or "").strip().lower()


def _file_glob_material_content(result: dict[str, Any]) -> str:
    matches = result.get("matches")
    if not isinstance(matches, list) or not matches:
        return ""
    lines = ["候选路径："]
    for index, item in enumerate(matches, start=1):
        if not isinstance(item, dict):
            continue
        marker = "file" if item.get("is_file") else "dir"
        lines.append(
            f"{index}. [{marker}] {item.get('name') or ''} - {item.get('path') or ''}"
        )
    return "\n".join(lines).strip()


def _file_grep_material_content(result: dict[str, Any]) -> str:
    matches = result.get("matches")
    if not isinstance(matches, list) or not matches:
        return ""
    lines = ["正文命中："]
    for index, item in enumerate(matches, start=1):
        if not isinstance(item, dict):
            continue
        path = str(item.get("path") or "").strip()
        line_number = item.get("line_number")
        lines.append(f"{index}. {path}:{line_number}")
        for before in item.get("context_before") or []:
            lines.append(f"   - {before}")
        lines.append(f"   > {item.get('line') or ''}")
        for after in item.get("context_after") or []:
            lines.append(f"   + {after}")
    return "\n".join(lines).strip()


def _web_fetch_material_content(result: dict[str, Any]) -> str:
    content = str(result.get("content") or "")
    if not content:
        return ""
    content_quality = str(result.get("content_quality") or "ok").strip()
    if content_quality and content_quality != "ok":
        reason = str(result.get("content_quality_reason") or "").strip()
        prefix = (
            f"【网页正文质量提示】content_quality={content_quality}"
            + (f"; reason={reason}" if reason else "")
            + "。本材料不能作为可靠正文证据。\n\n"
        )
        content = prefix + content
    return content


def _web_search_material_content(result: dict[str, Any]) -> str:
    content = str(result.get("content") or "").strip()
    if content:
        return "候选来源（搜索结果不是正文）：\n" + content
    results = result.get("results")
    if not isinstance(results, list) or not results:
        return ""
    lines = ["候选来源（搜索结果不是正文）："]
    for index, item in enumerate(results, start=1):
        if not isinstance(item, dict):
            continue
        title = str(item.get("title") or item.get("url") or "").strip()
        url = str(item.get("url") or "").strip()
        domain = str(item.get("domain") or "").strip()
        kind = str(item.get("candidate_kind") or "search_candidate").strip()
        source_backend = str(
            item.get("source_backend") or item.get("source") or ""
        ).strip()
        snippet = str(item.get("snippet") or "").strip()
        if title:
            lines.append(f"{index}. {title}")
        if url:
            lines.append(f"   url={url}")
        if domain:
            lines.append(f"   domain={domain}")
        if kind:
            lines.append(f"   kind={kind}")
        if source_backend:
            lines.append(f"   source_backend={source_backend}")
        if snippet:
            lines.append(f"   snippet={snippet}")
    return "\n".join(lines).strip()


def read_tool_material_content(result: Any) -> str:
    """Return the exact text admitted as a read-tool ``material`` block."""
    if not isinstance(result, dict) or _status(result) not in SUCCESS_STATUSES:
        return ""
    tool_id = _tool_id(result)
    if tool_id == "file_read":
        return str(result.get("content") or "")
    if tool_id == "file_glob":
        return _file_glob_material_content(result)
    if tool_id == "file_grep":
        return _file_grep_material_content(result)
    if tool_id == "web_fetch":
        return _web_fetch_material_content(result)
    if tool_id == "web_search":
        return _web_search_material_content(result)
    return ""
