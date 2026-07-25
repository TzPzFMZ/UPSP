#!/usr/bin/env python
"""Serve the UPSP docs visual workbench."""
from __future__ import annotations

import sys
from http.server import ThreadingHTTPServer
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
PROGRAM_OS_ROOT = REPO_ROOT / "UPSP" / "OS"
TOOLS_ROOT = Path(__file__).resolve().parent
for _path in (PROGRAM_OS_ROOT, TOOLS_ROOT):
    if str(_path) not in sys.path:
        sys.path.insert(0, str(_path))

from data.docs_workbench import (  # noqa: E402
    build_workbench_index,
    load_doc_document,
    read_diff_log,
    save_doc_draft,
)
from workbench_server import (  # noqa: E402
    WorkbenchServerConfig,
    make_server as make_workbench_server,
    serve_workbench,
)


def default_html_path() -> Path:
    return PROGRAM_OS_ROOT / "audit" / "docs_workbench.html"


CONFIG = WorkbenchServerConfig(
    name="docs workbench",
    log_prefix="docs-workbench",
    description="Serve UPSP docs visual workbench",
    html_route="/docs_workbench.html",
    api_route="/api/docs-workbench",
    files_route_prefix="/api/docs-workbench/files/",
    default_port=8768,
    default_repo_root=REPO_ROOT,
    default_html_path=default_html_path(),
    index_loader=build_workbench_index,
    document_loader=load_doc_document,
    diff_log_reader=read_diff_log,
    draft_saver=save_doc_draft,
    bad_path_error="bad_doc_path",
)


def make_server(host: str, port: int, repo_root: Path, html_path: Path) -> ThreadingHTTPServer:
    return make_workbench_server(CONFIG, host, port, repo_root, html_path)


def main(argv: list[str] | None = None) -> int:
    return serve_workbench(CONFIG, argv)


if __name__ == "__main__":
    raise SystemExit(main())
