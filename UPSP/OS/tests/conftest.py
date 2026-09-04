import os
from pathlib import Path
import shutil
import sys
import tempfile

import pytest


OS_ROOT = Path(__file__).resolve().parents[1]
UPSP_ROOT = OS_ROOT.parent
TEST_RUNTIME_PARENT = Path(
    tempfile.mkdtemp(prefix="upsp-pytest-roots-")
).resolve()
TEST_DATA_ROOT = TEST_RUNTIME_PARENT / "文档 数据" / "UPSP"
TEST_LOCAL_STATE_ROOT = TEST_RUNTIME_PARENT / "本机 状态" / "UPSP"

os.environ["UPSP_DATA_ROOT"] = str(TEST_DATA_ROOT)
os.environ["UPSP_LOCAL_STATE_ROOT"] = str(TEST_LOCAL_STATE_ROOT)
if str(OS_ROOT) not in sys.path:
    sys.path.insert(0, str(OS_ROOT))
if str(UPSP_ROOT) not in sys.path:
    sys.path.insert(0, str(UPSP_ROOT))

from initialization.persona_initializer import PersonaInitializer, load_preset  # noqa: E402
from initialization.windows_data import ensure_active_instance  # noqa: E402

TEST_LAYOUT = ensure_active_instance(UPSP_ROOT)
TEST_PERSONA_DIR = TEST_LAYOUT.persona_dir
from data.config_store import ConfigStore  # noqa: E402
ConfigStore().init_all()

PersonaInitializer(
    TEST_PERSONA_DIR,
    UPSP_ROOT / "initialization" / "persona_template",
    UPSP_ROOT / "initialization" / "persona_presets",
    pid=TEST_LAYOUT.pid,
).create(
    load_preset(UPSP_ROOT / "initialization" / "persona_presets", "alyosha"),
    {
        "profile_id": "pytest-model",
        "model_alias": "pytest model",
        "model": "pytest-model",
        "context_window": 128000,
    },
)

LIVE_PERSONA_GUARDED_FILES = [
    TEST_PERSONA_DIR / "LTM" / "Chronicle" / "rhythms" / "R-active-main-axis.md",
    TEST_PERSONA_DIR / "STM" / "context" / "cache" / "now_cache.jsonl",
    TEST_PERSONA_DIR / "STM" / "health" / "base" / "alerts.md",
    TEST_PERSONA_DIR / "STM" / "health" / "base" / "connectivity.json",
    TEST_PERSONA_DIR / "STM" / "memory" / "heat.json",
    TEST_PERSONA_DIR / "STM" / "memory" / "index.md",
    TEST_PERSONA_DIR / "STM" / "memory" / "keywords.json",
    TEST_PERSONA_DIR / "STM" / "memory" / "memory.md",
    TEST_PERSONA_DIR / "STM" / "memory" / "meta.json",
    TEST_PERSONA_DIR / "relation" / "relation_registry.json",
]


@pytest.fixture(autouse=True)
def guard_live_persona_files(monkeypatch):
    monkeypatch.setenv("UPSP_PROVIDER_CALL_INTERVAL_SECONDS", "0")
    snapshots = {
        path: path.read_bytes() if path.exists() else None
        for path in LIVE_PERSONA_GUARDED_FILES
    }
    yield
    changed = []
    for path, before in snapshots.items():
        after = path.read_bytes() if path.exists() else None
        if after == before:
            continue
        changed.append(path)
        if before is None:
            try:
                path.unlink()
            except FileNotFoundError:
                pass
        else:
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(before)
    if changed:
        rel_paths = ", ".join(
            str(path.relative_to(TEST_PERSONA_DIR)).replace("\\", "/")
            for path in changed
        )
        pytest.fail(f"测试污染隔离 persona 基线文件: {rel_paths}")


def pytest_sessionfinish(session, exitstatus):
    temp_root = Path(tempfile.gettempdir()).resolve()
    try:
        TEST_RUNTIME_PARENT.relative_to(temp_root)
    except ValueError:
        return
    if TEST_RUNTIME_PARENT.name.startswith("upsp-pytest-roots-"):
        shutil.rmtree(TEST_RUNTIME_PARENT, ignore_errors=True)
