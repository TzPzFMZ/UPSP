"""Shared atomic file write helpers for UPSP data stores."""

from __future__ import annotations

import json
import os
import tempfile
import time
from pathlib import Path
from typing import Any, Iterable

from errors import WriteError


def _target_path(path: str | os.PathLike[str]) -> str:
    return str(Path(path))


def _replace_with_retry(
        tmp: str,
        target: str,
        *,
        replace_attempts: int,
        retry_base_seconds: float) -> None:
    attempts = max(1, int(replace_attempts))
    for attempt in range(attempts):
        try:
            os.replace(tmp, target)
            return
        except PermissionError as exc:
            if attempt >= attempts - 1:
                raise WriteError(
                    target,
                    message=f"os.replace 失败（重试{attempt + 1}次）",
                    cause=exc,
                ) from exc
            time.sleep(retry_base_seconds * (attempt + 1))
        except OSError as exc:
            raise WriteError(target, cause=exc) from exc


def atomic_write_text(
        path: str | os.PathLike[str],
        text: Any,
        *,
        encoding: str = "utf-8",
        newline: str | None = None,
        replace_attempts: int = 5,
        retry_base_seconds: float = 0.05) -> None:
    target = _target_path(path)
    directory = os.path.dirname(target) or "."
    os.makedirs(directory, exist_ok=True)
    fd, tmp = tempfile.mkstemp(
        prefix=f"{os.path.basename(target)}.",
        suffix=".tmp",
        dir=directory,
    )
    try:
        with os.fdopen(fd, "w", encoding=encoding, newline=newline) as handle:
            handle.write(str(text))
        _replace_with_retry(
            tmp,
            target,
            replace_attempts=replace_attempts,
            retry_base_seconds=retry_base_seconds,
        )
        tmp = ""
    except WriteError:
        raise
    except OSError as exc:
        raise WriteError(target, cause=exc) from exc
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


def atomic_write_json(
        path: str | os.PathLike[str],
        data: Any,
        *,
        indent: int | None = 2,
        sort_keys: bool = False,
        trailing_newline: bool = False,
        replace_attempts: int = 5) -> None:
    text = json.dumps(
        data,
        ensure_ascii=False,
        indent=indent,
        sort_keys=sort_keys,
    )
    if trailing_newline:
        text += "\n"
    atomic_write_text(path, text, replace_attempts=replace_attempts)


def atomic_write_jsonl(
        path: str | os.PathLike[str],
        records: Iterable[Any],
        *,
        sort_keys: bool = False,
        replace_attempts: int = 5) -> None:
    lines = [
        json.dumps(record, ensure_ascii=False, sort_keys=sort_keys)
        for record in records
    ]
    text = "\n".join(lines)
    if lines:
        text += "\n"
    atomic_write_text(path, text, replace_attempts=replace_attempts)
