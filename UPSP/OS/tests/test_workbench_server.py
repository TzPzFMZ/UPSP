import json
import sys
import threading
from pathlib import Path
from urllib.request import Request, urlopen


def test_workbench_server_routes_index_file_diff_and_draft(tmp_path):
    repo_root = tmp_path / "repo"
    repo_root.mkdir()
    html_path = tmp_path / "workbench.html"
    html_path.write_text("<!doctype html><title>Workbench</title>", encoding="utf-8")
    saved = []

    tools_root = Path(__file__).resolve().parents[3] / "tools"
    if str(tools_root) not in sys.path:
        sys.path.insert(0, str(tools_root))

    from workbench_server import WorkbenchServerConfig, make_server

    def index_loader(root):
        assert root == repo_root.resolve()
        return {"files": ["protocol/base/tools.md"]}

    def document_loader(rel_path, root):
        assert root == repo_root.resolve()
        return {"rel_path": rel_path, "content": "body"}

    def diff_log_reader(root):
        assert root == repo_root.resolve()
        return [{"path": "protocol/base/tools.md"}]

    def draft_saver(rel_path, content, note, root):
        assert root == repo_root.resolve()
        saved.append((rel_path, content, note))
        return {"saved": rel_path}

    config = WorkbenchServerConfig(
        name="test workbench",
        log_prefix="test-workbench",
        description="Serve test workbench",
        html_route="/workbench.html",
        api_route="/api/test-workbench",
        files_route_prefix="/api/test-workbench/files/",
        default_port=0,
        default_repo_root=repo_root,
        default_html_path=html_path,
        index_loader=index_loader,
        document_loader=document_loader,
        diff_log_reader=diff_log_reader,
        draft_saver=draft_saver,
        bad_path_error="bad_test_path",
    )
    server = make_server(config, "127.0.0.1", 0, repo_root, html_path)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    host, port = server.server_address
    base_url = f"http://{host}:{port}"

    try:
        with urlopen(f"{base_url}/workbench.html", timeout=5) as response:
            assert response.status == 200
            assert b"Workbench" in response.read()

        with urlopen(f"{base_url}/api/test-workbench", timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert payload == {"files": ["protocol/base/tools.md"]}

        with urlopen(f"{base_url}/api/test-workbench/diff-log", timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert payload == {"records": [{"path": "protocol/base/tools.md"}]}

        with urlopen(f"{base_url}/api/test-workbench/files/protocol%2Fbase%2Ftools.md", timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert payload == {"rel_path": "protocol/base/tools.md", "content": "body"}

        body = json.dumps({"content": "draft body", "note": "test note"}).encode("utf-8")
        request = Request(
            f"{base_url}/api/test-workbench/files/protocol%2Fbase%2Ftools.md/draft",
            data=body,
            headers={"Content-Type": "application/json; charset=utf-8"},
            method="POST",
        )
        with urlopen(request, timeout=5) as response:
            payload = json.loads(response.read().decode("utf-8"))
            assert payload == {"saved": "protocol/base/tools.md"}
        assert saved == [("protocol/base/tools.md", "draft body", "test note")]
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
