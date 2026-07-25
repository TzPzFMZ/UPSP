import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def test_find_recent_child_prefers_last_registry_chain(tmp_path):
    from assembly.context_indexes import find_recent_child

    (tmp_path / "registry.json").write_text(
        json.dumps({
            "chains": [
                {"id": "DC-001", "title": "旧条目", "status": "closed"},
                {"id": "DC-002", "title": "新条目", "status": "active"},
            ]
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    assert find_recent_child(str(tmp_path), "DC") == "新条目 (active)"


def test_find_recent_child_reads_latest_dc_open_title(tmp_path):
    from assembly.context_indexes import find_recent_child

    older = tmp_path / "DC-001"
    newer = tmp_path / "DC-002"
    older.mkdir()
    newer.mkdir()
    (older / "open.md").write_text("# 旧标题\n", encoding="utf-8")
    (newer / "open.md").write_text("# 新标题\n", encoding="utf-8")
    os.utime(str(older), (1, 1))
    os.utime(str(newer), (2, 2))

    assert find_recent_child(str(tmp_path), "DC") == "DC-002 → 新标题"
