import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class _FakeConfig:
    def __init__(self, memory_limit=65536):
        self.memory_limit = memory_limit

    def get_periodic_limits(self):
        return {
            "periodic_memory_items_chars": self.memory_limit,
        }


def test_periodic_item_text_preserves_existing_text_rules():
    from assembly.context_periodic import periodic_item_text

    assert periodic_item_text("  plain text  ") == "plain text"
    assert periodic_item_text({"id": "MEM-A", "rendered_text": " rendered "}) == "rendered"
    assert periodic_item_text({"id": "MEM-B", "title": "标题"}) == "- MEM-B 标题"
    assert periodic_item_text({"id": "MEM-C"}) == "- MEM-C"
    assert periodic_item_text({"unknown": "value"}) == ""
    assert periodic_item_text(None) == ""


def test_render_structured_periodic_applies_memory_budget():
    from assembly.context_periodic import render_structured_periodic

    text = render_structured_periodic(
        {
            "periodic_memory_items": [
                {"id": "MEM-TOO-LONG", "rendered_text": "memory-too-long"},
            ],
        },
        memory_limit=3,
    )

    assert "memory-too-long" not in text


def test_render_structured_periodic_ignores_removed_simple_schema():
    from assembly.context_periodic import render_structured_periodic

    text = render_structured_periodic(
        {
            "memories": ["MEM-LEGACY"],
            "skill_tools": ["SKL-LEGACY"],
        },
        memory_limit=65536,
    )

    assert text is None


def test_context_assembler_periodic_wrapper_uses_new_helper(monkeypatch):
    from assembly.context import ContextAssembler
    from data.periodic_mount_store import PeriodicMountStore

    monkeypatch.setattr(PeriodicMountStore, "load", lambda self: {
        "periodic_memory_items": [
            {"id": "MEM-A", "rendered_text": "memory-a"},
            {"id": "MEM-B", "rendered_text": "memory-b-overflow"},
        ],
    })
    assembler = ContextAssembler(config_store=_FakeConfig(
        memory_limit=len("memory-a"),
    ))

    periodic = assembler._build_periodic({}, "setup", "interactive")

    assert "memory-a" in periodic
    assert "memory-b-overflow" not in periodic
    assert "定期记忆投影" in periodic
