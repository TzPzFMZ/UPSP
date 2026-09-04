import hashlib
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys


REPO_ROOT = Path(__file__).resolve().parents[3]
PROGRAM_ROOT = REPO_ROOT / "UPSP"


def _file_manifest(root: Path) -> dict[str, str]:
    return {
        path.relative_to(root).as_posix(): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in sorted(item for item in root.rglob("*") if item.is_file())
    }


def _restore_writable(root: Path) -> None:
    for path in (item for item in root.rglob("*") if item.is_file()):
        path.chmod(stat.S_IREAD | stat.S_IWRITE)


def test_read_only_program_root_supports_bootstrap_gui_and_mock_round(tmp_path):
    install_root = tmp_path / "只读 安装目录"
    program_root = install_root / "UPSP"
    tools_root = install_root / "tools"
    shutil.copytree(
        PROGRAM_ROOT,
        program_root,
        ignore=shutil.ignore_patterns("node_modules", "__pycache__"),
    )
    tools_root.mkdir(parents=True)
    for name in ("serve_seed_gui.py", "serve_round_live.py", "upsp_cli.py"):
        shutil.copy2(REPO_ROOT / "tools" / name, tools_root / name)

    before = _file_manifest(install_root)
    for path in (item for item in install_root.rglob("*") if item.is_file()):
        path.chmod(stat.S_IREAD)

    data_root = tmp_path / "用户 文档" / "UPSP"
    local_root = tmp_path / "本机 设置" / "UPSP"
    environment = dict(os.environ)
    environment.update(
        {
            "UPSP_DATA_ROOT": str(data_root),
            "UPSP_LOCAL_STATE_ROOT": str(local_root),
            "UPSP_PROVIDER_CALL_INTERVAL_SECONDS": "0",
            "PYTHONDONTWRITEBYTECODE": "1",
            "PYTHONIOENCODING": "utf-8",
            "PYTHONPATH": os.pathsep.join(
                (
                    str(program_root / "OS"),
                    str(program_root),
                    str(tools_root),
                )
            ),
        }
    )
    script = r"""
import http.client
import json
import threading

from pathlib import Path
from initialization.windows_data import ensure_active_instance

ensure_active_instance(Path.cwd() / "UPSP")

from data.config_store import ConfigStore
from initialization.persona_initializer import PersonaInitializer, load_preset
from paths import (
    ACTIVE_PID,
    GLOBAL_CONFIG_DIR,
    OS_ROOT,
    PERSONA_DIR,
    PERSONA_PRESETS_DIR,
    PERSONA_TEMPLATE_DIR,
    STM_CTX_ROUND_DIR,
)
import serve_seed_gui


def get(server, path):
    host, port = server.server_address
    connection = http.client.HTTPConnection(host, port, timeout=10)
    connection.request("GET", path, headers={"Host": f"127.0.0.1:{port}"})
    response = connection.getresponse()
    body = response.read()
    connection.close()
    try:
        return response.status, json.loads(body)
    except json.JSONDecodeError:
        return response.status, body.decode("utf-8")


server = serve_seed_gui.make_server(0)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    root_status, _root_body = get(server, "/")
    bootstrap_status, bootstrap = get(server, "/api/bootstrap/status")
finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=10)
assert root_status == 200
assert bootstrap_status == 200
assert bootstrap["persona"]["ready"] is False

configs = ConfigStore(use_api_environment=False)
configs.init_all()
interface = configs.load("interface")
interface["locale"] = "zh-CN"
configs.save("interface", interface)

initializer = PersonaInitializer(
    PERSONA_DIR,
    PERSONA_TEMPLATE_DIR,
    PERSONA_PRESETS_DIR,
    pid=ACTIVE_PID,
)
initializer.create(
    load_preset(PERSONA_PRESETS_DIR, "alyosha"),
    {
        "profile_id": "unbound",
        "model_alias": "未绑定",
        "model": "未绑定",
        "context_window": 0,
    },
)

import main as os_main


class MockExecutor:
    def __init__(self, *, config_store):
        self.config_store = config_store

    def call(self, step, system, messages, active_protocol_tool_guides=None):
        from logic.runtime_channels import channel_for_step

        channel = channel_for_step(
            step,
            active_protocol_tool_guides=active_protocol_tool_guides,
        )
        if step == "setup":
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "setup_finalize",
                    "arguments": {
                        "security_verdict": "pass",
                    },
                    "parse_status": "ok",
                    "index": 0,
                }],
                "tokens_input": 10,
                "tokens_output": 5,
            }
        if step == "reaction" and channel.name == "reaction.loop":
            return {
                "response": "只读程序根 mock 回复。",
                "tool_call_envelopes": [],
                "tokens_input": 10,
                "tokens_output": 5,
            }
        if step == "cleanup":
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "cleanup_finalize",
                    "arguments": {},
                    "parse_status": "ok",
                    "index": 0,
                }],
                "tokens_input": 10,
                "tokens_output": 5,
            }
        return {"response": "", "tool_call_envelopes": []}


os_main.APIExecutor = MockExecutor
state_store, config_store = os_main.init_environment()
config_store.get_round_context_window_tokens = lambda: 1_000_000
result = os_main.send_message_once(
    state_store,
    config_store,
    "只读程序根 mock 输入",
    wait_timeout=1,
)
assert result["status"] == "round_completed"
assert result["round_num"] == 1
assert result["final_response"] == "只读程序根 mock 回复。"

server = serve_seed_gui.make_server(0)
thread = threading.Thread(target=server.serve_forever, daemon=True)
thread.start()
try:
    bootstrap_status, bootstrap = get(server, "/api/bootstrap/status")
    rounds_status, rounds = get(server, "/api/rounds")
    persona_status, persona = get(server, "/api/persona/core")
finally:
    server.shutdown()
    server.server_close()
    thread.join(timeout=10)
assert bootstrap_status == 200, (bootstrap_status, bootstrap)
assert bootstrap["persona"]["ready"] is True, bootstrap
assert rounds_status == 200, (rounds_status, rounds)
assert [entry["round"] for entry in rounds["rounds"]] == [1], rounds
assert persona_status == 200, (persona_status, persona)
assert persona["schema_version"] == "seed_gui_persona_core.v1", persona

print("SPEC703_RESULT=" + json.dumps(
    {
        "pid": ACTIVE_PID,
        "os_root": OS_ROOT,
        "persona_dir": PERSONA_DIR,
        "global_config_dir": GLOBAL_CONFIG_DIR,
        "round_dir": STM_CTX_ROUND_DIR,
    },
    ensure_ascii=False,
))
"""
    try:
        completed = subprocess.run(
            [sys.executable, "-c", script],
            cwd=install_root,
            env=environment,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
            timeout=120,
        )
        assert completed.returncode == 0, (
            f"stdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
        result_line = next(
            line.removeprefix("SPEC703_RESULT=")
            for line in completed.stdout.splitlines()
            if line.startswith("SPEC703_RESULT=")
        )
        result = json.loads(result_line)
        assert Path(result["os_root"]).resolve().is_relative_to(data_root.resolve())
        assert Path(result["persona_dir"]).resolve().is_relative_to(data_root.resolve())
        assert Path(result["round_dir"]).resolve().is_relative_to(data_root.resolve())
        assert Path(result["global_config_dir"]).resolve().is_relative_to(
            local_root.resolve()
        )
        assert (local_root / "config" / "interface.json").is_file()
        assert (local_root / "config" / "models.json").is_file()
        assert (data_root / "active_instance.json").is_file()
        assert list(data_root.glob("personas/*/meta/persona/core.md"))
        assert list(data_root.glob("personas/*/meta/config/model_routing.json"))
    finally:
        _restore_writable(install_root)

    assert _file_manifest(install_root) == before
