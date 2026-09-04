#!/usr/bin/env python
"""Serve the UPSP live round viewer from local JSONL events."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = REPO_ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from data.round_audit_viewer import list_rounds, load_round_events  # noqa: E402
from data.round_live_viewer import build_live_detail, build_live_state, events_after  # noqa: E402
from paths import AUDIT_HTML_DIR, STM_CTX_ROUND_DIR  # noqa: E402

AUDIT_ROUTE_PREFIX = "/UPSP/OS/audit"


def default_round_dir() -> Path:
    return Path(STM_CTX_ROUND_DIR)


def default_html_path() -> Path:
    return Path(AUDIT_HTML_DIR) / "round_live.html"


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


class RoundLiveCacheLimitError(RuntimeError):
    """The lightweight live projection exceeded its bounded resident cache."""


class RoundLiveHandler(BaseHTTPRequestHandler):
    round_dir: Path = default_round_dir()
    html_path: Path = default_html_path()
    MAX_LATEST_EVENT_CACHE_EVENTS = 20_000
    MAX_LATEST_EVENT_CACHE_BYTES = 8 * 1024 * 1024
    _event_cache_lock = threading.Lock()
    _event_cache: dict[str, object] = {}

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("[round-live] " + fmt % args + "\n")

    def _send_json(self, status: int, payload: object) -> None:
        body = _json_bytes(payload)
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _send_file(self, path: Path, content_type: str) -> None:
        try:
            body = path.read_bytes()
        except OSError:
            self._send_json(404, {"error": "not_found"})
            return
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _viewer_path(self) -> tuple[str, dict[str, list[str]]]:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == AUDIT_ROUTE_PREFIX:
            path = "/"
        elif path.startswith(f"{AUDIT_ROUTE_PREFIX}/"):
            path = path[len(AUDIT_ROUTE_PREFIX):] or "/"
        return path, parse_qs(parsed.query)

    def _resolve_round(self, raw_round: str | None):
        rounds = list_rounds(str(self.round_dir))
        if not raw_round or raw_round == "latest":
            if not rounds:
                return None
            return int(rounds[-1]["round"])
        try:
            return int(raw_round)
        except (TypeError, ValueError):
            raise ValueError("round must be digits or latest")

    def _load_events_for_query(self, query: dict[str, list[str]]):
        raw_round = (query.get("round") or ["latest"])[0]
        round_num = self._resolve_round(raw_round)
        if round_num is None:
            return None, []
        if self._is_latest_round_number(round_num):
            return round_num, self._load_cached_round_events(round_num)
        return round_num, self._load_lightweight_round_events(round_num)

    def _round_path(self, round_num: int) -> Path:
        path = (self.round_dir / f"round_{int(round_num)}.jsonl").resolve()
        if path.parent != self.round_dir.resolve():
            raise ValueError("round path escapes round_dir")
        return path

    def _load_lightweight_round_events(self, round_num: int) -> list[dict]:
        path = self._round_path(round_num)
        events = []
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    events.append(self._compact_live_event(json.loads(line)))
        return events

    @staticmethod
    def _compact_live_event(event: dict) -> dict:
        result = {
            key: event.get(key)
            for key in (
                "schema_version", "event_id", "event_index", "event_type",
                "round", "phase", "iteration", "frame_id", "recorded_at",
            )
            if key in event
        }
        payload = event.get("payload")
        payload = payload if isinstance(payload, dict) else {}
        event_type = str(event.get("event_type") or "")
        if event_type == "step_input_snapshot":
            result["payload"] = {
                key: payload.get(key)
                for key in ("manifest", "frame_projection")
                if key in payload
            }
        elif event_type == "round_started":
            compact = {
                key: payload.get(key)
                for key in ("round_type", "dialogue_projection_schema")
                if key in payload
            }
            snapshot = payload.get("input_snapshot")
            trigger = snapshot.get("trigger") if isinstance(snapshot, dict) else None
            if isinstance(trigger, dict):
                compact["input_snapshot"] = {"trigger": {
                    key: trigger.get(key)
                    for key in ("messages", "final_response_max_chars")
                    if key in trigger
                }}
            result["payload"] = compact
        else:
            compact = {
                key: value
                for key, value in payload.items()
                if key not in {
                    "audit_snapshot", "provider_request_envelope",
                    "provider_request", "layers_snapshot", "manifest",
                    "messages", "request_body", "request_body_source_map",
                }
            }
            if event_type == "llm_output_raw":
                compact.pop("response", None)
                compact["tool_call_envelopes"] = [
                    {
                        key: envelope.get(key)
                        for key in ("tool_id", "name", "call_id")
                        if key in envelope
                    }
                    for envelope in payload.get("tool_call_envelopes") or []
                    if isinstance(envelope, dict)
                ]
            if event_type == "llm_stream_delta" and "stream_segments" in payload:
                compact["stream_segments"] = (
                    RoundLiveHandler._fold_adjacent_stream_segments(
                        payload.get("stream_segments"))
                )
            if event_type == "step_settlement":
                for field in (
                    "general_tool_results", "protocol_tool_receipts",
                    "native_tool_result_projections", "tool_results",
                ):
                    if field not in payload:
                        continue
                    compact[field] = [
                        {
                            key: item.get(key)
                            for key in ("tool_id", "tool", "name", "call_id", "status")
                            if key in item
                        }
                        for item in payload.get(field) or []
                        if isinstance(item, dict)
                    ]
            result["payload"] = compact
        return result

    @staticmethod
    def _fold_adjacent_stream_segments(value):
        if not isinstance(value, list):
            return value
        folded = []
        identity_fields = (
            "sequence", "segment_id", "channel", "provider_block")
        for segment in value:
            if not isinstance(segment, dict):
                folded.append(segment)
                continue
            current = dict(segment)
            previous = folded[-1] if folded else None
            same_identity = bool(
                isinstance(previous, dict)
                and all(
                    field in previous
                    and field in current
                    and previous[field] == current[field]
                    for field in identity_fields
                )
            )
            same_metadata = bool(
                isinstance(previous, dict)
                and {
                    key: item
                    for key, item in previous.items()
                    if key != "delta"
                } == {
                    key: item
                    for key, item in current.items()
                    if key != "delta"
                }
            )
            if (
                    same_identity
                    and same_metadata
                    and isinstance(previous.get("delta"), str)
                    and isinstance(current.get("delta"), str)):
                previous["delta"] += current["delta"]
                continue
            folded.append(current)
        return folded

    def _is_latest_round_number(self, round_num: int | None) -> bool:
        if round_num is None:
            return False
        rounds = list_rounds(str(self.round_dir))
        if not rounds:
            return False
        return int(rounds[-1]["round"]) == int(round_num)

    @classmethod
    def _load_cached_round_events(cls, round_num: int) -> list[dict]:
        path = (cls.round_dir / f"round_{int(round_num)}.jsonl").resolve()
        if path.parent != cls.round_dir.resolve():
            raise ValueError("round path escapes round_dir")
        with cls._event_cache_lock:
            stat = path.stat()
            identity = (stat.st_dev, stat.st_ino or stat.st_ctime_ns)
            cache = cls._event_cache
            if (
                    cache.get("path") != str(path)
                    or cache.get("identity") != identity
                    or stat.st_size < int(cache.get("offset") or 0)
                    or (
                        stat.st_size == int(cache.get("offset") or 0)
                        and cache.get("mtime_ns") is not None
                        and stat.st_mtime_ns != int(cache.get("mtime_ns"))
                    )):
                cache = {
                    "path": str(path),
                    "identity": identity,
                    "offset": 0,
                    "pending": b"",
                    "tail": b"",
                    "events": [],
                    "event_bytes": 0,
                }
            if cache.get("overflow"):
                raise RoundLiveCacheLimitError(
                    "round_live_event_cache_limit_exceeded")
            offset = int(cache.get("offset") or 0)
            tail = bytes(cache.get("tail") or b"")
            with path.open("rb") as handle:
                if offset and tail:
                    handle.seek(max(0, offset - len(tail)))
                    if handle.read(len(tail)) != tail:
                        cache = {
                            "path": str(path),
                            "identity": identity,
                            "offset": 0,
                            "pending": b"",
                            "tail": b"",
                            "events": [],
                        }
                        offset = 0
                if stat.st_size > offset:
                    handle.seek(offset)
                    chunk = handle.read(stat.st_size - offset)
                    data = bytes(cache.get("pending") or b"") + chunk
                    lines = data.split(b"\n")
                    cache["pending"] = lines.pop()
                    events = list(cache.get("events") or [])
                    event_bytes = int(cache.get("event_bytes") or 0)
                    for line in lines:
                        line = line.strip()
                        if line:
                            compact = cls._compact_live_event(
                                json.loads(line.decode("utf-8")))
                            events.append(compact)
                            event_bytes += len(json.dumps(
                                compact,
                                ensure_ascii=False,
                                separators=(",", ":"),
                            ).encode("utf-8"))
                    cache["events"] = events
                    cache["event_bytes"] = event_bytes
                    cache["offset"] = offset + len(chunk)
                marker_offset = int(cache.get("offset") or 0)
                handle.seek(max(0, marker_offset - 256))
                cache["tail"] = handle.read(min(256, marker_offset))
            cache["mtime_ns"] = stat.st_mtime_ns
            events = list(cache.get("events") or [])
            if (
                    len(events) > cls.MAX_LATEST_EVENT_CACHE_EVENTS
                    or int(cache.get("event_bytes") or 0)
                    > cls.MAX_LATEST_EVENT_CACHE_BYTES):
                # Do not trade a resident-memory bound for a 500 ms full-file
                # reparse loop.  Keep only a stable overflow sentinel until
                # this file is replaced or truncated; the complete audit is
                # still available through the explicit detail endpoint.
                cls._event_cache = {
                    "path": str(path),
                    "identity": identity,
                    "offset": stat.st_size,
                    "mtime_ns": stat.st_mtime_ns,
                    "tail": bytes(cache.get("tail") or b""),
                    "overflow": {
                        "events": len(events),
                        "event_bytes": int(cache.get("event_bytes") or 0),
                    },
                }
                raise RoundLiveCacheLimitError(
                    "round_live_event_cache_limit_exceeded")
            cls._event_cache = cache
            return events

    def _load_full_events(self, round_num: int) -> tuple[list[dict], str]:
        path = self._round_path(round_num)
        before = path.stat()
        events = load_round_events(str(self.round_dir), round_num)
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        after = path.stat()
        if (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns) != (
                after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns):
            raise ValueError("round_changed_during_detail_read")
        return events, digest

    def do_GET(self) -> None:  # noqa: N802
        path, query = self._viewer_path()
        if path == "/":
            self.send_response(302)
            self.send_header("Location", "/round_live.html")
            self.end_headers()
            return
        if path == "/round_live.html":
            self._send_file(self.html_path, "text/html; charset=utf-8")
            return
        if path.startswith("/vendor/"):
            name = Path(path).name
            vendor_path = self.html_path.parent / "vendor" / name
            content_type = "application/javascript; charset=utf-8"
            if name.lower().endswith(".license") or name.lower().endswith(".txt"):
                content_type = "text/plain; charset=utf-8"
            self._send_file(vendor_path, content_type)
            return
        if path == "/api/rounds":
            self._send_json(200, {"rounds": list_rounds(str(self.round_dir))})
            return
        if path == "/api/live/detail":
            if set(query) - {"round", "kind", "ref"}:
                self._send_json(400, {"error": "live_detail_query_invalid"})
                return
            raw_round = (query.get("round") or [""])[0]
            kind = str((query.get("kind") or [""])[0]).strip()
            ref = str((query.get("ref") or [""])[0]).strip()
            allowed = {
                "frame", "ledger", "event", "timeline_node",
                "legacy_conversation", "evidence",
            }
            if not raw_round or kind not in allowed:
                self._send_json(400, {"error": "live_detail_request_invalid"})
                return
            if kind in {"frame", "event", "timeline_node"} and not ref:
                self._send_json(400, {"error": "live_detail_ref_required"})
                return
            if kind not in {"frame", "event", "timeline_node"} and ref:
                self._send_json(400, {"error": "live_detail_ref_forbidden"})
                return
            try:
                round_num = self._resolve_round(raw_round)
                if round_num is None:
                    raise FileNotFoundError(raw_round)
                events, source_sha = self._load_full_events(round_num)
                detail = build_live_detail(
                    events,
                    kind=kind,
                    ref=ref,
                    live_context_root=str(self.round_dir.parent),
                    use_live_layers=(
                        kind == "frame" and self._is_latest_round_number(round_num)
                    ),
                )
                detail["source_sha"] = source_sha
            except FileNotFoundError:
                self._send_json(404, {"error": "round_not_found"})
                return
            except ValueError as exc:
                self._send_json(409, {"error": str(exc)})
                return
            self._send_json(200, detail)
            return
        if path == "/api/live/state":
            try:
                round_num, events = self._load_events_for_query(query)
            except RoundLiveCacheLimitError as exc:
                self._send_json(409, {"error": str(exc)})
                return
            except ValueError as exc:
                self._send_json(400, {"error": "bad_round", "detail": str(exc)})
                return
            except FileNotFoundError:
                self._send_json(404, {"error": "round_not_found"})
                return
            state = build_live_state(
                events,
                live_context_root=str(self.round_dir.parent),
                use_live_layers=self._is_latest_round_number(round_num),
            )
            self._send_json(200, {"round": round_num, "state": state})
            return
        if path == "/api/live/events":
            try:
                raw_round = (query.get("round") or ["latest"])[0]
                round_num = self._resolve_round(raw_round)
                if round_num is None:
                    self._send_json(200, {
                        "schema_version": "round_live_events.v2",
                        "after": 0,
                        "last_event_index": 0,
                        "events": [],
                        "state": None,
                        "round": None,
                    })
                    return
            except ValueError as exc:
                self._send_json(400, {"error": "bad_round", "detail": str(exc)})
                return
            raw_after = (query.get("after") or ["0"])[0]
            try:
                after = int(raw_after or 0)
            except ValueError:
                self._send_json(400, {"error": "bad_after"})
                return
            try:
                events = self._load_cached_round_events(round_num)
            except RoundLiveCacheLimitError as exc:
                self._send_json(409, {"error": str(exc)})
                return
            except FileNotFoundError:
                self._send_json(404, {"error": "round_not_found"})
                return
            latest_index = int(events[-1].get("event_index") or 0) if events else 0
            if after >= latest_index:
                self._send_json(200, {
                    "schema_version": "round_live_events.v2",
                    "after": after,
                    "last_event_index": latest_index,
                    "events": [],
                    "state": None,
                    "round": round_num,
                })
                return
            payload = events_after(
                events,
                after=after,
                live_context_root=str(self.round_dir.parent),
                use_live_layers=self._is_latest_round_number(round_num),
            )
            payload.pop("events", None)
            payload["round"] = round_num
            self._send_json(200, payload)
            return
        self._send_json(404, {"error": "not_found"})


def make_server(host: str, port: int, round_dir: Path, html_path: Path) -> ThreadingHTTPServer:
    handler = type(
        "ConfiguredRoundLiveHandler",
        (RoundLiveHandler,),
        {
            "round_dir": round_dir.resolve(),
            "html_path": html_path.resolve(),
        },
    )
    return ThreadingHTTPServer((host, port), handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve UPSP live round viewer.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8768)
    parser.add_argument("--round-dir", type=Path, default=default_round_dir())
    parser.add_argument("--html", type=Path, default=default_html_path())
    parser.add_argument("--open", action="store_true", help="Open the viewer in the default browser.")
    args = parser.parse_args(argv)

    server = make_server(args.host, args.port, args.round_dir, args.html)
    host, port = server.server_address
    url = f"http://{host}:{port}/round_live.html"
    print(f"Serving UPSP round live viewer: {url}")
    print(f"Round dir: {args.round_dir}")
    if args.open:
        threading.Timer(0.6, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
