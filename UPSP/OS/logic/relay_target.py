"""Relay target ledger helpers.

The free-text relay handoff remains a note for the next round. This module
keeps the executable target grounded in tool receipts.
"""
import os

from logic.protocol_tools import normalize_tool_id


def _text(value):
    return str(value or "").strip()


def _path_key(value):
    text = _text(value)
    if not text:
        return ""
    return os.path.normcase(os.path.normpath(text))


def _int_or_none(value):
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _status_ok(result):
    return _text(result.get("status")).lower() == "ok"


def _tool_id(result):
    return normalize_tool_id(result.get("tool_id", ""))


def pending_target_from_file_reads(results):
    """Derive the next relay target from the latest unfinished file_read."""
    target = None
    for result in results or []:
        if not isinstance(result, dict):
            continue
        if _tool_id(result) != "file_read" or not _status_ok(result):
            continue
        path = _text(result.get("path"))
        next_start_line = _int_or_none(
            result.get("next_line_start")
            if result.get("next_line_start") not in (None, "")
            else result.get("next_start_line")
        )
        if not path or next_start_line is None:
            continue
        target = {
            "kind": "tool",
            "tool_id": "file_read",
            "path": path,
            "next_start_line": next_start_line,
            "source": "file_read_result",
            "source_call_id": _text(result.get("call_id")),
        }
    return target


def normalize_pending_target(target):
    if not isinstance(target, dict):
        return {}
    kind = _text(target.get("kind"))
    tool_id = normalize_tool_id(target.get("tool_id", ""))
    if kind != "tool" or tool_id != "file_read":
        return {}
    path = _text(target.get("path"))
    next_start_line = _int_or_none(target.get("next_start_line"))
    if not path or next_start_line is None:
        return {}
    normalized = dict(target)
    normalized["kind"] = "tool"
    normalized["tool_id"] = "file_read"
    normalized["path"] = path
    normalized["next_start_line"] = next_start_line
    normalized.setdefault("source", "runtime")
    normalized.setdefault("source_call_id", "")
    return normalized


def file_read_target_satisfied(target, results):
    target = normalize_pending_target(target)
    if not target:
        return True
    target_path = _path_key(target.get("path"))
    target_line = _int_or_none(target.get("next_start_line"))
    for result in results or []:
        if not isinstance(result, dict):
            continue
        if _tool_id(result) != "file_read" or not _status_ok(result):
            continue
        if _path_key(result.get("path")) != target_path:
            continue
        start_line = _int_or_none(result.get("start_line"))
        end_line = _int_or_none(result.get("end_line"))
        if start_line is None:
            continue
        if start_line == target_line:
            return True
        if end_line is not None and start_line <= target_line <= end_line:
            return True
    return False


def target_feedback(target, correction_count):
    target = normalize_pending_target(target)
    path = target.get("path", "")
    line = target.get("next_start_line", "")
    return (
        "relay_target_unfulfilled: 本轮是中继执行轮，上一轮留下了未完成执行目标。"
        f"目标要求先调用 provider-native `file_read`，path={path}，"
        f"line_start={line}。写记忆、挂容器、复述旧内容或普通 progress "
        "都不能满足这个目标。请先执行该 file_read；若现实上不能执行，"
        "说明阻塞并收束，不要再次请求继续中继。"
        f"本提示是第 {correction_count}/2 次纠偏。"
    )


def render_target_popup(target):
    target = normalize_pending_target(target)
    if not target:
        return ""
    return (
        "- kind: relay_target_card\n"
        "  tier: reminder\n"
        "  decision_required: true\n"
        "  source: runtime/pending_relay_target\n"
        "  message: |\n"
        "    本轮存在上一轮留下的中继执行目标。\n"
        f"    第一动作：调用 provider-native `file_read`，path={target['path']}，line_start={target['next_start_line']}。\n"
        "    未完成该目标前，不要把旧 CONTENT / 常驻正文 / 旧记忆当成本轮新执行结果，也不要再次请求继续中继。"
    )
