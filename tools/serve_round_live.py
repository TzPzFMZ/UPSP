#!/usr/bin/env python
"""Serve the UPSP live round viewer from local JSONL events."""
from __future__ import annotations

import argparse
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
from data.round_live_viewer import build_live_state, events_after  # noqa: E402
from paths import AUDIT_HTML_DIR, STM_CTX_ROUND_DIR  # noqa: E402

AUDIT_ROUTE_PREFIX = "/UPSP/OS/audit"


def default_round_dir() -> Path:
    return Path(STM_CTX_ROUND_DIR)


def default_html_path() -> Path:
    return Path(AUDIT_HTML_DIR) / "round_live.html"


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


class RoundLiveHandler(BaseHTTPRequestHandler):
    round_dir: Path = default_round_dir()
    html_path: Path = default_html_path()
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
        return round_num, load_round_events(str(self.round_dir), round_num)

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
                    or stat.st_size < int(cache.get("offset") or 0)):
                cache = {
                    "path": str(path),
                    "identity": identity,
                    "offset": 0,
                    "pending": b"",
                    "tail": b"",
                    "events": [],
                }
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
                    for line in lines:
                        line = line.strip()
                        if line:
                            events.append(json.loads(line.decode("utf-8")))
                    cache["events"] = events
                    cache["offset"] = offset + len(chunk)
                marker_offset = int(cache.get("offset") or 0)
                handle.seek(max(0, marker_offset - 256))
                cache["tail"] = handle.read(min(256, marker_offset))
            cls._event_cache = cache
            return list(cache.get("events") or [])

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
        if path == "/api/live/state":
            try:
                round_num, events = self._load_events_for_query(query)
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
                        "schema_version": "round_live_events.v1",
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
            except FileNotFoundError:
                self._send_json(404, {"error": "round_not_found"})
                return
            latest_index = int(events[-1].get("event_index") or 0) if events else 0
            if after >= latest_index:
                self._send_json(200, {
                    "schema_version": "round_live_events.v1",
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
