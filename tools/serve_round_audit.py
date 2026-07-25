#!/usr/bin/env python
"""Serve the UPSP round audit viewer from local JSONL files."""
from __future__ import annotations

import argparse
import json
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

REPO_ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = REPO_ROOT / "UPSP" / "OS"
if str(PROGRAM_OS_ROOT) not in sys.path:
    sys.path.insert(0, str(PROGRAM_OS_ROOT))

from data.round_audit_viewer import build_step_timeline, list_rounds, load_round_events  # noqa: E402
from paths import AUDIT_DIR, AUDIT_HTML_DIR, STM_CTX_ROUND_DIR  # noqa: E402

AUDIT_ROUTE_PREFIX = "/UPSP/OS/audit"


def default_round_dir() -> Path:
    return Path(STM_CTX_ROUND_DIR)


def default_html_path() -> Path:
    return Path(AUDIT_HTML_DIR) / "round.html"


def default_static_dir() -> Path:
    return Path(AUDIT_DIR)


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


class RoundAuditHandler(BaseHTTPRequestHandler):
    round_dir: Path = default_round_dir()
    html_path: Path = default_html_path()
    static_dir: Path = default_static_dir()

    def log_message(self, fmt: str, *args: object) -> None:
        sys.stderr.write("[round-audit] " + fmt % args + "\n")

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

    def _viewer_path(self) -> str:
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == AUDIT_ROUTE_PREFIX:
            return "/"
        if path.startswith(f"{AUDIT_ROUTE_PREFIX}/"):
            return path[len(AUDIT_ROUTE_PREFIX):] or "/"
        return path

    def do_GET(self) -> None:  # noqa: N802
        path = self._viewer_path()
        if path == "/":
            self.send_response(302)
            self.send_header("Location", "/round.html")
            self.end_headers()
            return
        if path == "/round.html":
            self._send_file(self.html_path, "text/html; charset=utf-8")
            return
        if path == "/round-index.js":
            self._send_file(self.static_dir / "round-index.js", "application/javascript; charset=utf-8")
            return
        if path.startswith("/round-data/") and path.endswith(".js"):
            name = Path(path).name
            if not name.startswith("round_"):
                self._send_json(404, {"error": "not_found"})
                return
            self._send_file(self.static_dir / "round-data" / name, "application/javascript; charset=utf-8")
            return
        if path == "/api/rounds":
            self._send_json(200, {"rounds": list_rounds(str(self.round_dir))})
            return
        if path.startswith("/api/rounds/"):
            raw_round = path.rsplit("/", 1)[-1]
            try:
                events = load_round_events(str(self.round_dir), raw_round)
            except ValueError as exc:
                self._send_json(400, {"error": "bad_round", "detail": str(exc)})
                return
            except FileNotFoundError:
                self._send_json(404, {"error": "round_not_found"})
                return
            self._send_json(200, {
                "round": int(raw_round),
                "events": events,
                "timeline": build_step_timeline(events),
            })
            return
        self._send_json(404, {"error": "not_found"})


def make_server(
    host: str,
    port: int,
    round_dir: Path,
    html_path: Path,
    static_dir: Path | None = None,
) -> ThreadingHTTPServer:
    handler = type(
        "ConfiguredRoundAuditHandler",
        (RoundAuditHandler,),
        {
            "round_dir": round_dir.resolve(),
            "html_path": html_path.resolve(),
            "static_dir": (
                static_dir if static_dir is not None else html_path.parent
            ).resolve(),
        },
    )
    return ThreadingHTTPServer((host, port), handler)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Serve UPSP OS/audit/round.html")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--round-dir", type=Path, default=default_round_dir())
    parser.add_argument("--html", type=Path, default=default_html_path())
    parser.add_argument("--static-dir", type=Path, default=default_static_dir())
    args = parser.parse_args(argv)

    server = make_server(
        args.host,
        args.port,
        args.round_dir,
        args.html,
        args.static_dir,
    )
    host, port = server.server_address
    print(f"Serving round audit viewer: http://{host}:{port}/round.html")
    print(f"Round dir: {args.round_dir}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
