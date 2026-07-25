"""Offline replay for deterministic ``file_read`` window planning."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from logic.file_read_window import plan_file_read_window  # noqa: E402
from logic.general_tools import _line_bounded_window  # noqa: E402


SCHEMA_VERSION = "file_read_window_replay.v1"
BASELINE_WINDOW_CHARS = 4096


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _normalized_text(path: Path, encoding: str) -> tuple[bytes, str]:
    raw = path.read_bytes()
    text = raw.decode(encoding).replace("\r\n", "\n")
    return raw, text


def replay_text(text: str, window_chars: int) -> dict[str, Any]:
    total_lines = len(text.splitlines())
    next_line = 1
    windows: list[dict[str, Any]] = []
    bodies: list[str] = []
    while next_line <= total_lines:
        window = _line_bounded_window(
            text,
            next_line,
            total_lines,
            window_chars,
            allow_line_char=False,
        )
        body = str(window.get("content") or "")
        entry = {
            "index": len(windows) + 1,
            "line_start": int(window.get("start_line") or 0),
            "line_end": int(window.get("end_line") or 0),
            "returned_chars": len(body),
            "has_more": bool(window.get("has_more")),
            "next_line_start": window.get("next_start_line"),
            "line_overlong": bool(window.get("line_overlong")),
            "content_sha256": _sha256_bytes(body.encode("utf-8")),
        }
        windows.append(entry)
        bodies.append(body)
        if not entry["has_more"]:
            break
        candidate = entry["next_line_start"]
        if not isinstance(candidate, int) or candidate <= next_line:
            raise ValueError("file_read replay cursor did not advance")
        next_line = candidate

    stitched = "".join(bodies)
    continuous = all(
        current["line_start"] == previous["line_end"] + 1
        for previous, current in zip(windows, windows[1:])
    )
    within_plan = all(
        item["returned_chars"] <= window_chars or item["line_overlong"]
        for item in windows
    )
    return {
        "window_chars": int(window_chars),
        "window_count": len(windows),
        "returned_chars_total": sum(item["returned_chars"] for item in windows),
        "stitched_sha256": _sha256_bytes(stitched.encode("utf-8")),
        "exact_text_match": stitched == text,
        "line_continuity": continuous,
        "within_window_or_overlong_line": within_plan,
        "windows": windows,
    }


def build_replay(
    path: Path,
    *,
    encoding: str = "utf-8",
    configured_max_chars: int = 16384,
    current_tokens: int = 61683,
    context_window: int = 1_000_000,
    eof_marker: str = "SEALGATE_EOF_TOKEN",
) -> dict[str, Any]:
    raw, text = _normalized_text(path, encoding)
    normalized_sha = _sha256_bytes(text.encode("utf-8"))
    plan = plan_file_read_window(
        configured_max_chars,
        {
            "current_tokens": current_tokens,
            "context_window": context_window,
        },
    )
    baseline = replay_text(text, BASELINE_WINDOW_CHARS)
    adaptive = replay_text(text, int(plan["window_chars"]))
    baseline_window_count = int(baseline["window_count"])
    reduction_ratio = (
        round(1 - adaptive["window_count"] / baseline_window_count, 6)
        if baseline_window_count > 0
        else 0.0
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "analysis_boundary": {
            "provider_called": False,
            "dogfood_run": False,
            "source_body_committed": False,
        },
        "source": {
            "path": str(path.resolve()),
            "name": path.name,
            "encoding": encoding,
            "raw_bytes": len(raw),
            "normalized_chars": len(text),
            "total_lines": len(text.splitlines()),
            "raw_sha256": _sha256_bytes(raw),
            "normalized_sha256": normalized_sha,
            "eof_marker": eof_marker,
            "eof_marker_present": eof_marker in text,
        },
        "budget": {
            "current_tokens": int(current_tokens),
            "context_window": int(context_window),
            "configured_max_chars": int(configured_max_chars),
        },
        "adaptive_plan": plan,
        "baseline": baseline,
        "adaptive": adaptive,
        "comparison": {
            "provider_turn_shape_avoided": max(
                baseline["window_count"] - adaptive["window_count"],
                0,
            ),
            "window_count_reduction_ratio": reduction_ratio,
            "now_body_chars_not_increased": (
                adaptive["returned_chars_total"]
                == baseline["returned_chars_total"]
                == len(text)
            ),
            "source_sha_matches_baseline": baseline["stitched_sha256"] == normalized_sha,
            "source_sha_matches_adaptive": adaptive["stitched_sha256"] == normalized_sha,
        },
    }


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
        newline="\n",
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--path", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--encoding", default="utf-8")
    parser.add_argument("--configured-max-chars", type=int, default=16384)
    parser.add_argument("--current-tokens", type=int, default=61683)
    parser.add_argument("--context-window", type=int, default=1_000_000)
    parser.add_argument("--eof-marker", default="SEALGATE_EOF_TOKEN")
    args = parser.parse_args()
    payload = build_replay(
        args.path,
        encoding=args.encoding,
        configured_max_chars=args.configured_max_chars,
        current_tokens=args.current_tokens,
        context_window=args.context_window,
        eof_marker=args.eof_marker,
    )
    _write_json(args.output, payload)
    print(json.dumps({
        "baseline_windows": payload["baseline"]["window_count"],
        "adaptive_windows": payload["adaptive"]["window_count"],
        "exact": payload["adaptive"]["exact_text_match"],
    }, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
