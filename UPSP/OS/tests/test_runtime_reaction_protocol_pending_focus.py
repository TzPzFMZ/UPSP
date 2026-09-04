"""Spec781 regression: the old focus protocol cannot return to active Runtime."""

import os
import sys


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def test_spec781_container_focus_is_not_registered_or_exported():
    from logic.native_tool_calls import export_provider_tool_schemas
    from logic.protocol_tools import tool_metadata_for

    names = {
        item["name"]
        for item in export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
            execution_permission_level="unlimited",
        )
    }

    assert "container_focus" not in names
    assert tool_metadata_for("container_focus") == {}


def test_spec781_active_tool_classes_have_no_focus_tool():
    from logic.protocol_tools import TOOL_DEFINITIONS

    assert {item["tool_class"] for item in TOOL_DEFINITIONS.values()} == {
        "read_tool", "sync_tool", "action_tool",
    }
