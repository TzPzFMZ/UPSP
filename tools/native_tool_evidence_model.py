#!/usr/bin/env python3
"""Shared helpers for UPSP native/general tool evidence scripts."""

from __future__ import annotations

import json
import re
from pathlib import Path
import sys
from typing import Any


SENSITIVE_MARKERS = (
    "arguments_json",
    "secret",
    "password",
    "token",
    "api_key",
    "sk-",
)


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


def default_round_dir(root: Path | None = None) -> Path:
    base = (root or repo_root()).resolve()
    if root is None or base == repo_root().resolve():
        program_os_root = base / "UPSP" / "OS"
        if str(program_os_root) not in sys.path:
            sys.path.insert(0, str(program_os_root))
        from paths import STM_CTX_ROUND_DIR

        return Path(STM_CTX_ROUND_DIR)
    return base / "UPSP" / "OS" / "persona" / "STM" / "context" / "round"


def round_num_from_path(path: Path) -> int | None:
    match = re.search(r"round_(\d+)\.jsonl$", path.name)
    if not match:
        return None
    return int(match.group(1))


def find_recent_rounds(round_dir: str | Path, limit: int) -> list[Path]:
    directory = Path(round_dir)
    candidates: list[tuple[int, float, Path]] = []
    for path in directory.glob("round_*.jsonl"):
        round_num = round_num_from_path(path)
        if round_num is None:
            continue
        candidates.append((round_num, path.stat().st_mtime, path))
    candidates.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in candidates[-limit:]]


def round_allow_keys(path: Path) -> set[str]:
    round_num = round_num_from_path(path)
    keys = {str(path), path.name, path.stem}
    if round_num is not None:
        keys.add(str(round_num))
    return keys


def unique(values: list[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        if not value or value in seen:
            continue
        seen.add(value)
        result.append(value)
    return result


def byte_len(value: Any) -> int:
    if value is None:
        return 0
    if isinstance(value, bytes):
        return len(value)
    return len(str(value).encode("utf-8", errors="replace"))


def parse_json_object(value: Any) -> dict[str, Any]:
    if not value:
        return {}
    try:
        parsed = json.loads(str(value))
    except json.JSONDecodeError:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def safe_text(value: Any) -> str:
    text = str(value or "")
    lowered = text.lower()
    if any(marker in lowered for marker in SENSITIVE_MARKERS):
        return "[redacted]"
    return text


def safe_plain_text(value: Any) -> str:
    return str(value or "")


def safe_bool(value: Any) -> bool:
    return bool(value) if isinstance(value, bool) else False


def safe_list(value: Any, *, redact: bool = True) -> list[str]:
    if not isinstance(value, list):
        return []
    text_fn = safe_text if redact else safe_plain_text
    return [text_fn(item) for item in value if item is not None]


def safe_dict(value: Any, *, redact: bool = True) -> dict[str, Any]:
    if not isinstance(value, dict):
        return {}
    text_fn = safe_text if redact else safe_plain_text
    safe: dict[str, Any] = {}
    for key, item in value.items():
        key_text = text_fn(key)
        if not key_text:
            continue
        if isinstance(item, int):
            safe[key_text] = item
        elif isinstance(item, (str, bool)) or item is None:
            safe[key_text] = text_fn(item)
    return safe


def safe_count_dict(value: Any) -> dict[str, int]:
    if not isinstance(value, dict):
        return {}
    safe: dict[str, int] = {}
    for key, item in value.items():
        key_text = safe_plain_text(key)
        if not key_text:
            continue
        try:
            count = int(item)
        except (TypeError, ValueError):
            continue
        safe[key_text] = safe.get(key_text, 0) + count
    return safe


def merge_count_dict(target: dict[str, int], values: Any) -> None:
    for key, value in safe_count_dict(values).items():
        target[key] = target.get(key, 0) + value
