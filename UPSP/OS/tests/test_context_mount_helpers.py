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
