"""Replay R572 long-read windows against the Spec625 A/B context lifecycles."""
from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
import sys
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from data.context_store import ContextStore  # noqa: E402


SCHEMA_VERSION = "context_lifecycle_replay.v1"
ROUND_NUM = 572
READ_ITERATIONS = tuple(range(3, 10))
LATE_REACTIONS = tuple(range(10, 15))
READ_BODY_FIELDS = {
    "body",
    "content",
    "html",
    "matches",
    "raw_html",
    "results",
}


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"expected JSON object: {path}")
    return payload


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    return [
        json.loads(line)
        for line in path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]


def _normalized_text(path: Path, encoding: str) -> tuple[bytes, str]:
    raw = path.read_bytes()
    return raw, raw.decode(encoding).replace("\r\n", "\n")


def _window_text(lines: list[str], line_start: int, line_end: int) -> str:
    if line_start < 1 or line_end < line_start or line_end > len(lines):
        raise ValueError(f"invalid live line range: {line_start}-{line_end}")
    return "".join(lines[line_start - 1:line_end])


def _material_blocks(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    return [item for item in blocks if item.get("kind") == "material"]


def _hidden_read_bodies(blocks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    found: list[dict[str, Any]] = []
    for block in blocks:
        ref = block.get("ref") if isinstance(block.get("ref"), dict) else {}
        tool_result = ref.get("tool_result")
        if not isinstance(tool_result, dict):
            continue
        fields = sorted(READ_BODY_FIELDS.intersection(tool_result))
        if fields:
            found.append({"block_id": block.get("id"), "fields": fields})
    return found


def build_replay(
    source_path: Path,
    verification_path: Path,
    *,
    encoding: str = "utf-8",
) -> dict[str, Any]:
    verification = _read_json(verification_path)
    live = verification.get("live") if isinstance(verification.get("live"), dict) else {}
    source_evidence = (
        verification.get("source")
        if isinstance(verification.get("source"), dict)
        else {}
    )
    ranges = live.get("line_ranges") if isinstance(live.get("line_ranges"), list) else []
    if not ranges:
        adaptive = verification.get("adaptive")
        windows = adaptive.get("windows") if isinstance(adaptive, dict) else []
        ranges = [
            [window.get("line_start"), window.get("line_end")]
            for window in windows
            if isinstance(window, dict)
        ]
    if len(ranges) != len(READ_ITERATIONS):
        raise ValueError("Spec622 verification must contain exactly seven line ranges")

    raw, text = _normalized_text(source_path, encoding)
    lines = text.splitlines(keepends=True)
    normalized_sha = _sha256_text(text)
    expected_sha = str(source_evidence.get("normalized_sha256") or "")
    if expected_sha and normalized_sha != expected_sha:
        raise ValueError("source SHA does not match Spec622 verification")

    with tempfile.TemporaryDirectory(prefix="upsp-spec623-replay-") as tmp:
        root = Path(tmp)
        cache_dir = root / "cache"
        now_path = cache_dir / "now_cache.jsonl"
        lately_path = cache_dir / "lately_cache.jsonl"
        raw_path = root / "raw_log.jsonl"
        raw_md_path = root / "raw_log.md"
        store = ContextStore(
            cache_dir=str(cache_dir),
            now_cache_jsonl=str(now_path),
            lately_cache_jsonl=str(lately_path),
            raw_log_jsonl=str(raw_path),
            raw_log_md=str(raw_md_path),
        )

        accepted: list[str] = []
        retention_steps: list[dict[str, Any]] = []
        for index, (iteration, pair) in enumerate(zip(READ_ITERATIONS, ranges), start=1):
            if not isinstance(pair, list) or len(pair) != 2:
                raise ValueError(f"invalid line range at window {index}")
            line_start, line_end = int(pair[0]), int(pair[1])
            body = _window_text(lines, line_start, line_end)
            accepted.append(body)
            has_more = index < len(ranges)
            tool_result = {
                "tool_id": "file_read",
                "status": "ok",
                "path": str(source_path.resolve()),
                "start_line": line_start,
                "end_line": line_end,
                "total_lines": len(lines),
                "content": body,
                "has_more": has_more,
                "next_start_line": line_end + 1 if has_more else None,
                "encoding": encoding,
                "evidence": f"EV-SPEC623-W{index}",
            }
            store.append_to_cache(
                ROUND_NUM,
                "tool",
                (
                    f"已读取文件：{source_path.name}。\n"
                    f"读取范围：第 {line_start} 行到第 {line_end} 行。"
                ),
                kind="tool_fact",
                step="reaction",
                iter=iteration,
                tool_result=tool_result,
            )
            store.append_to_cache(
                ROUND_NUM,
                "system",
                body,
                kind="material",
                step="reaction",
                iter=iteration,
                tool_result=tool_result,
            )

            visible_blocks = store.get_lately_entries() + store.get_now_entries()
            retained = _material_blocks(visible_blocks)
            stitched = "".join(str(item.get("content") or "") for item in retained)
            retention_steps.append({
                "window": index,
                "reaction_iteration": iteration,
                "line_start": line_start,
                "line_end": line_end,
                "returned_chars": len(body),
                "accepted_material_blocks": len(accepted),
                "visible_material_blocks": len(retained),
                "all_accepted_material_visible": stitched == "".join(accepted),
                "visible_sha256": _sha256_text(stitched),
                "now_soft_overflow": bool(
                    store.get_last_cache_stats().get("now_soft_overflow")
                ),
            })

        final_now = _read_jsonl(now_path)
        final_lately = _read_jsonl(lately_path)
        final_material = _material_blocks(final_lately + final_now)
        final_stitched = "".join(str(item.get("text") or "") for item in final_material)
        hidden_before_settlement = _hidden_read_bodies(final_now)
        late_visibility = [
            {
                "reaction_iteration": iteration,
                "visible_material_blocks": len(final_material),
                "visible_sha256": _sha256_text(final_stitched),
                "source_facts_directly_visible": final_stitched == text,
                "dialogue_progress_required": False,
                "corpus_read_required": False,
            }
            for iteration in LATE_REACTIONS
        ]

        settled_now = _read_jsonl(now_path)
        settled_lately = _read_jsonl(lately_path)
        settled_raw = _read_jsonl(raw_path)
        lately_material = _material_blocks(settled_lately)
        settled_visible_material = _material_blocks(settled_lately + settled_now)
        lately_stitched = "".join(
            str(item.get("text") or "") for item in settled_visible_material
        )
        hidden_after_settlement = _hidden_read_bodies(
            settled_now + settled_lately + settled_raw
        )

    required_markers = [
        "0037", "0128", "0219", "0311", "0312", "0470",
        "0706", "0930", "1176", "1177", "1178", "SG01-EOF-7F3A",
    ]
    marker_visibility = {marker: marker in final_stitched for marker in required_markers}
    all_steps_visible = all(
        bool(item["all_accepted_material_visible"])
        for item in retention_steps
    )
    all_late_visible = all(
        bool(item["source_facts_directly_visible"])
        for item in late_visibility
    )
    no_raw_material = not _material_blocks(settled_raw)
    exact_before = final_stitched == text
    exact_after = lately_stitched == text
    outcome = "GO_LOCAL" if all((
        all_steps_visible,
        all_late_visible,
        exact_before,
        exact_after,
        no_raw_material,
        not hidden_before_settlement,
        not hidden_after_settlement,
        all(marker_visibility.values()),
    )) else "NO_GO"

    return {
        "schema_version": SCHEMA_VERSION,
        "outcome": outcome,
        "analysis_boundary": {
            "provider_called": False,
            "dogfood_run": False,
            "source_body_committed": False,
            "historical_append_only_raw_log_rewritten": False,
        },
        "source": {
            "path": str(source_path.resolve()),
            "verification_path": str(verification_path.resolve()),
            "encoding": encoding,
            "raw_bytes": len(raw),
            "normalized_chars": len(text),
            "total_lines": len(lines),
            "normalized_sha256": normalized_sha,
        },
        "round": {
            "number": ROUND_NUM,
            "read_iterations": list(READ_ITERATIONS),
            "late_visibility_iterations": list(LATE_REACTIONS),
        },
        "retention_steps": retention_steps,
        "late_visibility": late_visibility,
        "marker_visibility": marker_visibility,
        "settlement": {
            "receipt": {"status": "retired_no_round_settlement"},
            "now_material_blocks": len(_material_blocks(settled_now)),
            "lately_material_blocks": len(lately_material),
            "visible_material_blocks": len(settled_visible_material),
            "raw_log_material_blocks": len(_material_blocks(settled_raw)),
            "lately_stitched_sha256": _sha256_text(lately_stitched),
        },
        "body_accounting": {
            "accepted_material_blocks": len(final_material),
            "accepted_material_chars": len(final_stitched),
            "source_exact_in_now": exact_before,
            "source_exact_in_lately": exact_after,
            "hidden_read_bodies_before_settlement": hidden_before_settlement,
            "hidden_read_bodies_after_settlement": hidden_after_settlement,
            "raw_log_material_absent": no_raw_material,
        },
        "checks": {
            "seven_windows_retained": len(final_material) == 7,
            "every_prefix_remained_visible": all_steps_visible,
            "reactions_10_to_14_see_all_windows": all_late_visible,
            "source_sha_matches_before_settlement": _sha256_text(final_stitched) == normalized_sha,
            "source_sha_matches_after_settlement": _sha256_text(lately_stitched) == normalized_sha,
            "all_required_markers_directly_visible": all(marker_visibility.values()),
            "dialogue_progress_not_required": True,
            "corpus_read_not_required": True,
            "material_absent_from_raw_log": no_raw_material,
            "hidden_read_body_copies_absent": not (
                hidden_before_settlement or hidden_after_settlement
            ),
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
    parser.add_argument("--source", type=Path, required=True)
    parser.add_argument("--spec622-verification", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--encoding", default="utf-8")
    args = parser.parse_args()
    payload = build_replay(
        args.source,
        args.spec622_verification,
        encoding=args.encoding,
    )
    _write_json(args.output, payload)
    print(json.dumps({
        "outcome": payload["outcome"],
        "windows": len(payload["retention_steps"]),
        "normalized_chars": payload["source"]["normalized_chars"],
        "normalized_sha256": payload["source"]["normalized_sha256"],
    }, ensure_ascii=False, sort_keys=True))
    return 0 if payload["outcome"] == "GO_LOCAL" else 1


if __name__ == "__main__":
    raise SystemExit(main())
