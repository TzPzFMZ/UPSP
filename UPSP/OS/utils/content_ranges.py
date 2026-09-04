"""Shared explicit range handling for read tools.

No range means full text. A partial read is only produced when the model
supplies line or character range fields in the tool arguments.
"""


RANGE_FIELDS = ("line_start", "line_end", "char_start", "char_end")
EMPTY_RANGE_STRINGS = {"", "none", "null", "undefined"}


def is_empty_range_placeholder(value):
    if value is None:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.lower() in EMPTY_RANGE_STRINGS:
            return True
        try:
            return int(stripped) == 0
        except ValueError:
            return False
    try:
        return int(value) == 0
    except (TypeError, ValueError):
        return False


def normalize_range_value(value):
    if is_empty_range_placeholder(value):
        return None
    if isinstance(value, str):
        return value.strip()
    return value


def _int_or_none(value):
    value = normalize_range_value(value)
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        raise ValueError("invalid_range_value")


def _has_pair(start_value, end_value):
    start_present = start_value not in (None, "")
    end_present = end_value not in (None, "")
    if start_present != end_present:
        raise ValueError("range_pair_required")
    return start_present and end_present


def range_kwargs_from_request(request):
    request = request or {}
    result = {}
    for key in RANGE_FIELDS:
        value = normalize_range_value(request.get(key))
        if value is not None:
            result[key] = value
    return result


def _line_range(text, start_value, end_value):
    lines = str(text or "").splitlines()
    total = len(lines)
    start = max(1, _int_or_none(start_value) or 1)
    requested_end = _int_or_none(end_value)
    end = requested_end if requested_end is not None else total
    end = max(start, end)
    applied_start = min(start, total + 1)
    applied_end = min(end, total)
    if applied_start > applied_end:
        content = ""
    else:
        content = "\n".join(lines[applied_start - 1:applied_end])
    return {
        "content": content,
        "range_requested": {
            "type": "line",
            "line_start": start,
            "line_end": requested_end,
        },
        "range_applied": {
            "type": "line",
            "line_start": applied_start,
            "line_end": applied_end,
        },
    }


def _char_range(text, start_value, end_value):
    text = str(text or "")
    total = len(text)
    start = max(1, _int_or_none(start_value) or 1)
    requested_end = _int_or_none(end_value)
    end = requested_end if requested_end is not None else total
    end = max(start, end)
    applied_start = min(start, total + 1)
    applied_end = min(end, total)
    if applied_start > applied_end:
        content = ""
    else:
        content = text[applied_start - 1:applied_end]
    return {
        "content": content,
        "range_requested": {
            "type": "char",
            "char_start": start,
            "char_end": requested_end,
        },
        "range_applied": {
            "type": "char",
            "char_start": applied_start,
            "char_end": applied_end,
        },
    }


def apply_explicit_range(text, range_request=None):
    text = str(text or "")
    range_request = range_kwargs_from_request(range_request or {})
    line_any = (
        range_request.get("line_start") not in (None, "")
        or range_request.get("line_end") not in (None, "")
    )
    char_any = (
        range_request.get("char_start") not in (None, "")
        or range_request.get("char_end") not in (None, "")
    )
    if line_any and char_any:
        raise ValueError("range_mode_conflict")
    has_line = _has_pair(
        range_request.get("line_start"),
        range_request.get("line_end"),
    )
    has_char = _has_pair(
        range_request.get("char_start"),
        range_request.get("char_end"),
    )
    result = {
        "content": text,
        "read_mode": "full",
        "range_requested": None,
        "range_applied": None,
        "total_lines": len(text.splitlines()),
        "total_chars": len(text),
    }
    if not has_line and not has_char:
        return result

    if has_line:
        selected = _line_range(
            text,
            range_request.get("line_start"),
            range_request.get("line_end"),
        )
    else:
        selected = _char_range(
            text,
            range_request.get("char_start"),
            range_request.get("char_end"),
        )
    result.update(selected)
    result["read_mode"] = "partial"
    return result
