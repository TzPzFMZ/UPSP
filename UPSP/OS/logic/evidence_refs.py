"""Evidence reference helpers for model-visible tool receipts."""

import hashlib
import re


SUCCESS_EVIDENCE_STATUSES = {"ok", "success", "accepted", "applied"}
EVIDENCE_HANDLE_RE = re.compile(r"^EV-[0-9A-F]{10}$")


def normalize_evidence_refs(value):
    if value is None:
        return []
    raw = value if isinstance(value, list) else [value]
    refs = []
    for item in raw:
        ref = str(item or "").strip()
        if ref and ref not in refs:
            refs.append(ref)
    return refs


def evidence_handle_for_result(result):
    result = result if isinstance(result, dict) else {}
    existing = str(result.get("evidence_handle") or "").strip().upper()
    if EVIDENCE_HANDLE_RE.match(existing):
        return existing
    parts = []
    for key in (
            "tool_id",
            "call_id",
            "tool_call_id",
            "id",
            "path",
            "file_path",
            "target_path",
            "root",
            "cwd",
            "url",
            "source_url",
            "query",
            "command"):
        value = str(result.get(key) or "").strip()
        if value:
            parts.append(f"{key}={value}")
    if not parts:
        return ""
    digest = hashlib.sha1("\n".join(parts).encode("utf-8")).hexdigest()
    return f"EV-{digest[:10].upper()}"


def result_supports_evidence(result):
    result = result if isinstance(result, dict) else {}
    if result.get("evidence_disabled_reason"):
        return False
    status = str(result.get("status") or "").strip().lower()
    return status in SUCCESS_EVIDENCE_STATUSES


def attach_evidence_handle(result):
    if not isinstance(result, dict) or not result_supports_evidence(result):
        return result
    handle = evidence_handle_for_result(result)
    if not handle:
        return result
    result["evidence_handle"] = handle
    refs = normalize_evidence_refs(result.get("evidence_refs"))
    if handle not in refs:
        refs.insert(0, handle)
    result["evidence_refs"] = refs
    return result


def safe_shell_subcommands(command):
    text = str(command or "").strip()
    if not text:
        return []
    parts = _split_on_double_ampersand_outside_quotes(text)
    if len(parts) < 2:
        return []
    cleaned = [part.strip() for part in parts]
    if any(not part for part in cleaned):
        return []
    return cleaned


def shell_result_subcommands(result):
    result = result if isinstance(result, dict) else {}
    if str(result.get("tool_id") or "").strip() != "shell_command":
        return []
    if not result_supports_evidence(result):
        return []
    exit_code = str(result.get("exit_code") if result.get("exit_code") is not None else "").strip()
    if exit_code and exit_code != "0":
        return []
    if not exit_code:
        return []
    return safe_shell_subcommands(result.get("command"))


def canonical_command_ref(value):
    text = str(value or "").strip()
    if not text:
        return ""
    text = re.sub(r"\s+", " ", text)
    return text.replace("\\", "/")


def command_evidence_refs(value):
    text = str(value or "").strip()
    if not text:
        return []
    refs = []
    canonical = canonical_command_ref(text)
    for ref in (text, canonical):
        if ref and ref not in refs:
            refs.append(ref)
    for ref in (
            f"command:{canonical}",
            f"shell_command:{canonical}",
            f"call:{canonical}",
            f"call_id:{canonical}",
            f"command:{text}",
            f"shell_command:{text}",
            f"call:{text}",
            f"call_id:{text}"):
        if ref not in refs:
            refs.append(ref)
    return refs


def _split_on_double_ampersand_outside_quotes(text):
    parts = []
    current = []
    quote = ""
    index = 0
    while index < len(text):
        char = text[index]
        if char in {"'", '"'}:
            if quote == char:
                quote = ""
            elif not quote:
                quote = char
            current.append(char)
            index += 1
            continue
        if not quote:
            if text.startswith("&&", index):
                parts.append("".join(current).strip())
                current = []
                index += 2
                continue
            if char in {"&", "|", ";", "<", ">", "\n", "\r"}:
                return [text]
        current.append(char)
        index += 1
    if quote:
        return [text]
    parts.append("".join(current).strip())
    return parts
