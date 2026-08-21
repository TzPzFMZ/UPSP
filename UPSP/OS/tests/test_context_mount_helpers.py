import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class _FakeMountAdapter:
    def _memory_mount_meta(self, ids):
        return {}

    def _load_memory_content(self, ids):
        return f"memory:{ids}"

    def _load_container_content(self, ids):
        return f"container:{ids}"

    def _load_relation_content(self, ids):
        return f"relation:{ids}"

    def _load_skill_content(self, ids):
        return f"skill:{ids}"


def test_build_mounted_content_preserves_request_order():
    from assembly.context_mounts import build_mounted_content

    text = build_mounted_content(_FakeMountAdapter(), [
        {"type": "memory", "ids": "MEM-1"},
        {"type": "container", "ids": "DC-1"},
        {"type": "relation", "ids": "REL-Codex"},
        {"type": "skill", "ids": "SKL-1"},
    ])

    assert text.index("memory:MEM-1") < text.index("container:DC-1")
    assert text.index("container:DC-1") < text.index("relation:REL-Codex")
    assert text.index("relation:REL-Codex") < text.index("skill:SKL-1")


def test_build_mounted_content_ignores_relation_summary_body_mount():
    from assembly.context_mounts import build_mounted_content

    text = build_mounted_content(_FakeMountAdapter(), [
        {"type": "relation_summary", "ids": "REL-Codex"},
    ])

    assert "relation:REL-Codex" not in text
    assert "无内容被挂载" in text


def test_spec724_memory_mount_replaces_stale_snapshot_metadata():
    from assembly.context_mounts import build_mounted_content

    adapter = _FakeMountAdapter()
    adapter._memory_mount_meta = lambda _ids: {
        "created_round": 1,
        "created_instance_id": "meta",
        "created_at": "2026-08-01T01:02:03+08:00",
        "last_recalled_round": 9,
        "last_recalled_instance_id": "I20260806-120000-ABCD",
        "last_recalled_at": "2026-08-06T12:00:00+08:00",
        "current_overview": "新备注",
        "current_overview_updated_at": "2026-08-06T11:00:00+08:00",
        "linked_containers": ["DC-2"],
    }
    text = build_mounted_content(adapter, [{
        "type": "memory",
        "ids": "MEM-1",
        "content": "## MEM-1\n**最后调用**：第1轮\n现状概况：旧备注\n关联容器：DC-1\n正文",
    }])

    assert "旧备注" not in text and "DC-1" not in text
    assert "**最近调用轮次**：I20260806-120000-ABCD/R000009" in text
    assert "**挂接备注**：新备注" in text
    assert "**关联容器**：DC-2" in text
