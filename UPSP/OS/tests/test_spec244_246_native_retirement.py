from pathlib import Path


def _envelope(tool_id, arguments=None, tool_class="sync_tool", index=0):
    return {
        "schema_version": "tool_call_envelope.v1",
        "source": "provider_tool_call",
        "provider": "openai_responses",
        "endpoint": "primary",
        "response_id": f"resp_{index}",
        "call_id": f"call_{tool_id}_{index}",
        "provider_item_id": f"fc_{index}",
        "index": index,
        "raw_type": "function_call",
        "tool_id": tool_id,
        "arguments": arguments or {},
        "arguments_json": "{}",
        "tool_class": tool_class,
        "risk": "high",
        "parse_status": "ok",
    }


def test_spec244_native_exports_protocol_tools_without_guide_gate():
    from logic.native_tool_calls import export_provider_tool_schemas

    tools = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
        active_protocol_tool_guides=[],
    )
    names = {item["name"] for item in tools}

    assert "protocol_tool_guide_request" not in names
    assert "memory_write" in names
    assert "memory_container_create" in names
    assert "memory_container_write" in names
    assert "container_focus" not in names
    assert "relation_card_write" in names


def test_spec244_native_protocol_write_no_longer_requires_loaded_guide():
    from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

    routed = apply_native_tool_calls_to_parsed_reaction({}, [_envelope(
        "memory_link_update",
        {
            "mem_id": "MEM-1",
            "operation": "remove",
            "container_refs": ["DC-1"],
            "reason": "retire old route",
        },
        tool_class="sync_tool",
    )], native_mode=True, active_protocol_tool_guides=[])

    assert routed["invalid_tool_requests"] == []
    assert routed["protocol_tool_submissions"] == ["memory_link_update"]
    assert routed["native_protocol_tool_submissions"] == ["memory_link_update"]
    assert routed["memory_link_update_declarations"][0]["mem_id"] == "MEM-1"


def test_spec244_docs_do_not_expose_retired_guide_markers():
    root = Path(__file__).resolve().parents[2] / "initialization" / "persona_template" / "docs" / "protocol" / "base"
    tools = (root / "tools.md").read_text(encoding="utf-8")
    schema = (root / "schema.md").read_text(encoding="utf-8")
    popup = (root / "popup.md").read_text(encoding="utf-8")

    assert "PROTOCOL_TOOL_GUIDE:" not in tools
    assert "GENERAL_TOOL_GUIDE:" not in tools
    for retired in (
        "protocol_tool_guide_request",
        "guide_required",
        "guide_loaded",
        "guide_missing",
        "rejected_missing_guide",
    ):
        assert retired not in schema
        assert retired not in popup
    assert "反应循环指南" in popup


def test_spec781_multiple_container_sync_tools_route_without_focus_conflict():
    from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

    routed = apply_native_tool_calls_to_parsed_reaction({}, [
        _envelope(
            "memory_container_create",
            {
                "mem_id": "MEM-1",
                "container_type": "PRJ",
                "title": "create",
                "target_file": "plan.md",
                "container_body": "body",
                "current_overview": "overview",
                "reason": "create resident container",
            },
            tool_class="sync_tool",
            index=0,
        ),
        _envelope(
            "memory_container_write",
            {
                "mem_id": "MEM-1",
                "container_id": "PRJ-1",
                "target_file": "plan.md",
                "title": "write",
                "container_body": "body",
                "current_overview": "overview",
                "reason": "same iteration conflict",
            },
            tool_class="sync_tool",
            index=1,
        ),
    ], native_mode=True, active_protocol_tool_guides=[
        "memory_container_create",
        "memory_container_write",
    ])

    assert routed["invalid_tool_requests"] == []
    assert routed["protocol_tool_submissions"] == [
        "memory_container_create", "memory_container_write",
    ]
    assert routed["memory_container_create_declarations"]
    assert routed["memory_container_write_declarations"]


def test_spec781_container_tool_headers_have_no_focus_batching_rule():
    from logic.native_tool_calls import export_provider_tool_schemas

    schemas = export_provider_tool_schemas(
        include_protocol_writes=True,
        include_step_terminal_tools={"reaction_finalize"},
        execution_permission_level="limited",
    )
    descriptions = {
        item.get("name"): str(item.get("description") or "")
        for item in schemas
    }
    assert "container_focus" not in descriptions
    for tool_id in ("memory_container_create", "memory_container_write"):
        description = descriptions[tool_id]
        assert "焦点工具" not in description
        assert "最多调用一个" not in description


def test_spec246_multiple_sync_tools_can_route_in_one_iteration():
    from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

    routed = apply_native_tool_calls_to_parsed_reaction({}, [
        _envelope(
            "memory_link_update",
            {
                "mem_id": "MEM-1",
                "operation": "remove",
                "container_refs": ["DC-1"],
                "reason": "sync one",
            },
            tool_class="sync_tool",
            index=0,
        ),
        _envelope(
            "relay_intent_settle",
            {
                "relay_intent_id": "RI-1",
                "status": "completed",
            },
            tool_class="sync_tool",
            index=1,
        ),
    ], native_mode=True, active_protocol_tool_guides=[
        "memory_link_update",
        "relay_intent_settle",
    ])

    assert routed["invalid_tool_requests"] == []
    assert routed["protocol_tool_submissions"] == [
        "memory_link_update",
        "relay_intent_settle",
    ]
    assert routed["memory_link_update_declarations"]
    assert routed["relay_intent_settle_requests"]
