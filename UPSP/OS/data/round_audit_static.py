"""Static JS projection for the OS round audit viewer.

The JSONL files remain the audit source of truth. This module only writes a
regenerable browser-friendly projection so OS/audit/round.html can be opened
directly with file:// when the local HTTP service is not running.
"""
from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

from data.atomic_write import atomic_write_text
from data.round_audit_viewer import build_step_timeline, list_rounds, load_round_events

SCHEMA_VERSION = "round_audit_static.v1"
STATIC_ROUND_RE = re.compile(r"^round_(\d+)\.js$")


def _now() -> str:
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")

def _json_for_js(payload: object) -> str:
    text = json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str)
    return (
        text
        .replace("</", "<\\/")
        .replace("\u2028", "\\u2028")
        .replace("\u2029", "\\u2029")
    )


def _assignment(target: str, payload: object) -> str:
    return (
        "/* Generated from STM/context/round/round_*.jsonl. Do not edit. */\n"
        f"{target} = {_json_for_js(payload)};\n"
    )


def _round_data_js(round_num: int, payload: object) -> str:
    return (
        "/* Generated from STM/context/round/round_*.jsonl. Do not edit. */\n"
        "window.UPSP_ROUND_AUDIT_ROUNDS = window.UPSP_ROUND_AUDIT_ROUNDS || {};\n"
        f"window.UPSP_ROUND_AUDIT_ROUNDS[{int(round_num)}] = {_json_for_js(payload)};\n"
    )


def _write_index(audit_dir: Path, rounds: list[dict], generated_at: str) -> dict:
    index = {
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "source": "round_jsonl_static_projection",
        "rounds": rounds,
    }
    atomic_write_text(
        audit_dir / "round-index.js",
        _assignment("window.UPSP_ROUND_AUDIT_INDEX", index),
        newline="\n",
    )
    return index


def write_static_round_index(round_dir, audit_dir) -> dict:
    audit_dir = Path(audit_dir)
    rounds = []
    for item in list_rounds(str(round_dir)):
        indexed = dict(item)
        static_file = f"round-data/round_{int(item['round'])}.js"
        if not (audit_dir / static_file).is_file():
            continue
        indexed["static_file"] = static_file
        rounds.append(indexed)
    return _write_index(audit_dir, rounds, _now())


def prune_stale_round_js(data_dir: str | os.PathLike[str], expected_names: set[str]) -> list[dict]:
    """Remove only generated per-Round projections absent from the live FIFO."""
    requested_dir = Path(data_dir)
    if requested_dir.is_symlink():
        raise RuntimeError("round_static_projection_directory_unsafe")
    data_dir = requested_dir.resolve()
    if not data_dir.is_dir():
        return []
    deleted = []
    for path in data_dir.iterdir():
        if not STATIC_ROUND_RE.fullmatch(path.name):
            continue
        resolved = path.resolve()
        if path.is_symlink() or resolved.parent != data_dir or not resolved.is_file():
            raise RuntimeError(f"round_static_projection_path_unsafe:{path.name}")
        if path.name in expected_names:
            continue
        size = resolved.stat().st_size
        try:
            resolved.unlink()
        except OSError as exc:
            raise RuntimeError(
                f"round_static_projection_delete_failed:{path.name}:{exc}"
            ) from exc
        deleted.append({"file": path.name, "bytes": size})
    return deleted


def write_static_projection(round_dir: str | os.PathLike[str], audit_dir: str | os.PathLike[str]) -> dict:
    """Write round-index.js and per-round JS files from existing JSONL rounds."""
    round_dir = Path(round_dir)
    audit_dir = Path(audit_dir)
    data_dir = audit_dir / "round-data"
    rounds = list_rounds(str(round_dir))
    generated_at = _now()
    expected_round_files: set[str] = set()
    index_rounds = []

    for item in rounds:
        round_num = int(item["round"])
        events = load_round_events(str(round_dir), round_num)
        timeline = build_step_timeline(events)
        data_file = f"round_{round_num}.js"
        expected_round_files.add(data_file)
        payload = {
            "schema_version": SCHEMA_VERSION,
            "generated_at": generated_at,
            "round": round_num,
            "source_file": item.get("file"),
            "events": events,
            "timeline": timeline,
        }
        atomic_write_text(
            data_dir / data_file,
            _round_data_js(round_num, payload),
            newline="\n",
        )
        indexed = dict(item)
        indexed["static_file"] = f"round-data/{data_file}"
        index_rounds.append(indexed)

    prune_stale_round_js(data_dir, expected_round_files)
    return _write_index(audit_dir, index_rounds, generated_at)
