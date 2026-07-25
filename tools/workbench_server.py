#!/usr/bin/env python
"""Shared HTTP shell for UPSP visual workbenches.

这个模块只承载 rules/docs 工作台共用的本地 HTTP 壳：路由、JSON
响应、静态 HTML 响应和草案 POST 处理。具体索引、文档读取、diff log 与
草案保存仍由各自 data adapter 负责，避免把领域语义塞进通用 server。
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Callable
from urllib.parse import unquote, urlparse


IndexLoader = Callable[[Path], object]
DocumentLoader = Callable[[str, Path], object]
DiffLogReader = Callable[[Path], object]
DraftSaver = Callable[[str, str, str, Path], object]


@dataclass(frozen=True)
class WorkbenchServerConfig:
    """Workbench HTTP 壳的全部可变口径。

    新增工作台时应优先提供一个 config，而不是复制一份 server 脚本。
    """

    name: str
    log_prefix: str
    description: str
    html_route: str
    api_route: str
    files_route_prefix: str
    default_port: int
    default_repo_root: Path
    default_html_path: Path
    index_loader: IndexLoader
    document_loader: DocumentLoader
    diff_log_reader: DiffLogReader
    draft_saver: DraftSaver
    bad_path_error: str


def _json_bytes(payload: object) -> bytes:
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


class WorkbenchRequestHandler(BaseHTTPRequestHandler):
    """Configured by `make_server`; do not instantiate this base class directly."""

    config: WorkbenchServerConfig
    repo_root: Path
    html_path: Path

    def log_message(self, fmt: str, *args: object) -> None:
        if sys.stderr is not None:
            sys.stderr.write(f"[{self.config.log_prefix}] " + fmt % args + "\n")

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

    def _read_json_body(self) -> dict[str, object]:
        length = int(self.headers.get("Content-Length") or "0")
        if length <= 0:
            return {}
        if length > 2_000_000:
            raise ValueError("request body too large")
        raw = self.rfile.read(length)
        return json.loads(raw.decode("utf-8"))

    def _file_path_from_api(self, path: str, suffix: str = "") -> str:
        prefix = self.config.files_route_prefix
        if not path.startswith(prefix):
            raise ValueError("bad file route")
        rel = path[len(prefix):]
        if suffix:
            if not rel.endswith(suffix):
                raise ValueError("bad file route suffix")
            rel = rel[:-len(suffix)]
        return unquote(rel)

    def do_GET(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path == "/":
            self.send_response(302)
            self.send_header("Location", self.config.html_route)
            self.end_headers()
            return
        if path == self.config.html_route:
            self._send_file(self.html_path, "text/html; charset=utf-8")
            return
        if path == self.config.api_route:
            self._send_json(200, self.config.index_loader(self.repo_root))
            return
        if path == f"{self.config.api_route}/diff-log":
            self._send_json(200, {"records": self.config.diff_log_reader(self.repo_root)})
            return
        if path.startswith(self.config.files_route_prefix):
            try:
                rel_path = self._file_path_from_api(path)
                self._send_json(200, self.config.document_loader(rel_path, self.repo_root))
            except ValueError as exc:
                self._send_json(400, {"error": self.config.bad_path_error, "detail": str(exc)})
            return
        self._send_json(404, {"error": "not_found"})

    def do_POST(self) -> None:  # noqa: N802
        parsed = urlparse(self.path)
        path = parsed.path.rstrip("/") or "/"
        if path.startswith(self.config.files_route_prefix) and path.endswith("/draft"):
            try:
                rel_path = self._file_path_from_api(path, "/draft")
                payload = self._read_json_body()
                content = str(payload.get("content") or "")
                note = str(payload.get("note") or "")
                result = self.config.draft_saver(rel_path, content, note, self.repo_root)
                self._send_json(200, result)
            except json.JSONDecodeError as exc:
                self._send_json(400, {"error": "bad_json", "detail": str(exc)})
            except ValueError as exc:
                self._send_json(400, {"error": "bad_request", "detail": str(exc)})
            return
        self._send_json(404, {"error": "not_found"})


def make_server(
    config: WorkbenchServerConfig,
    host: str,
    port: int,
    repo_root: Path,
    html_path: Path,
) -> ThreadingHTTPServer:
    handler = type(
        f"Configured{config.log_prefix.title().replace('-', '')}Handler",
        (WorkbenchRequestHandler,),
        {
            "config": config,
            "repo_root": repo_root.resolve(),
            "html_path": html_path.resolve(),
        },
    )
    return ThreadingHTTPServer((host, port), handler)


def serve_workbench(config: WorkbenchServerConfig, argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=config.description)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=config.default_port)
    parser.add_argument("--repo-root", type=Path, default=config.default_repo_root)
    parser.add_argument("--html", type=Path, default=config.default_html_path)
    args = parser.parse_args(argv)

    server = make_server(config, args.host, args.port, args.repo_root, args.html)
    host, port = server.server_address
    print(f"Serving {config.name}: http://{host}:{port}{config.html_route}")
    print(f"Repo root: {args.repo_root}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
    return 0
