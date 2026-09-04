import json
import hashlib
import os
import sys
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from UPSP.OS.tests.runtime_test_helpers import (
    ConfigStoreStub,
    ScriptedExecutor,
    logical_step as _logical_step,
)

class NoopConnectivity:
    def log_latency(self, endpoint, status, message=""):
        pass


def _without_descriptions(value):
    if isinstance(value, dict):
        return {
            key: _without_descriptions(item)
            for key, item in value.items()
            if key != "description"
        }
    if isinstance(value, list):
        return [_without_descriptions(item) for item in value]
    return value


class TestNativeToolCallAdapter:
    def test_spec719_only_four_stable_top_level_tool_headers(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        for provider in ("openai_responses", "openai_chat", "anthropic_messages"):
            reaction_headers = []
            for permission in ("limited", "guarded", "unlimited"):
                without_guide = export_provider_tool_schemas(
                    provider=provider,
                    include_protocol_writes=True,
                    active_protocol_tool_guides=[],
                    execution_permission_level=permission,
                )
                with_guide = export_provider_tool_schemas(
                    provider=provider,
                    include_protocol_writes=True,
                    active_protocol_tool_guides=["task:T-current"],
                    execution_permission_level=permission,
                )
                assert without_guide == with_guide
                reaction_headers.append(without_guide)

            setup = export_provider_tool_schemas(
                provider=provider, include_standard_tools=False,
                include_step_terminal_tools=["setup_finalize"],
            )
            cleanup = export_provider_tool_schemas(
                provider=provider, include_standard_tools=False,
                include_step_terminal_tools=["cleanup_finalize"],
            )
            assert reaction_headers[1] == reaction_headers[2]
            headers = {json.dumps(value, ensure_ascii=False, sort_keys=True)
                       for value in (setup, *reaction_headers, cleanup)}
            assert len(headers) == 4

            guide = next(item for item in reaction_headers[0]
                         if (item.get("name") or item.get("function", {}).get("name")) == "guide_submit")
            parameters = guide.get("parameters") or guide.get("input_schema") or guide["function"]["parameters"]
            assert "enum" not in parameters["properties"]["guide_id"]

    def test_spec719_failed_native_result_has_fixed_error_hint(self):
        from engines.reaction_protocol_tool_execution import minimal_native_tool_result_content

        payload = json.loads(minimal_native_tool_result_content({
            "tool_id": "guide_submit",
            "status": "rejected",
            "reason": "guide_not_active",
            "details": {
                "attempted": {"guide_id": "old"},
                "current": {"guide_id": "new"},
                "expected": [{"item_id": "task_progress"}],
                "next_action": "use new",
            },
        }))
        assert set(payload["error_hint"]) == {
            "kind", "retry", "attempted", "current", "expected", "next_action"
        }
        assert payload["error_hint"]["kind"] == "state_conflict"

        denied = json.loads(minimal_native_tool_result_content({
            "tool_id": "shell_command",
            "status": "denied",
            "reason": "permission_required",
        }))
        assert denied["error_hint"]["kind"] == "permission_security"
        assert denied["error_hint"]["retry"] == "after_authorization"

    def test_spec727_processor_hint_is_identical_in_tool_result_and_popup(self):
        from engines.reaction_helpers import format_native_tool_failure_feedback
        from engines.reaction_protocol_tool_execution import minimal_native_tool_result_content

        receipt = {
            "tool_id": "guide_submit",
            "call_id": "call_spec727",
            "status": "rejected",
            "reason": "undeclared_guide_fields",
            "error_hint": {
                "kind": "validation",
                "retry": "after_correction",
                "attempted": {"fields": ["reason"]},
                "current": {},
                "expected": {"allowed_fields": ["task_title", "items", "acceptance"]},
                "next_action": "把 reason 移到 submission 外层后重提。",
            },
        }

        payload = json.loads(minimal_native_tool_result_content(receipt))
        hint = payload["error_hint"]
        popup = format_native_tool_failure_feedback(receipt, receipt["reason"])

        assert f"next_action: {hint['next_action']}" in popup
        assert f"kind={hint['kind']}" in popup
        assert f"retry={hint['retry']}" in popup
        for key in ("attempted", "current", "expected"):
            encoded = json.dumps(hint[key], ensure_ascii=False)
            assert f"{key}={encoded}" in popup
        assert f"next_action={hint['next_action']}" in popup

    def test_spec727_processor_hint_projection_drops_sensitive_or_oversized_facts(self):
        from engines.reaction_protocol_tool_execution import minimal_native_tool_result_content

        secret = "sk-secret-value"
        payload_text = minimal_native_tool_result_content({
            "tool_id": "guide_submit",
            "status": "rejected",
            "reason": "invalid_guide_submission",
            "error_hint": {
                "kind": "validation",
                "retry": "after_correction",
                "attempted": {"command": secret, "field": "fields"},
                "current": {},
                "expected": {"summary": "x" * 800},
                "next_action": "修正字段",
            },
        })
        payload = json.loads(payload_text)

        assert secret not in payload_text
        assert payload["error_hint"]["attempted"] == {"field": "fields"}
        assert len(payload["error_hint"]["expected"]["summary"]) == 500

    def test_responses_function_call_becomes_tool_call_envelope(self):
        from logic.native_tool_calls import extract_tool_call_envelopes

        response_data = {
            "id": "resp_001",
            "output": [{
                "id": "fc_001",
                "type": "function_call",
                "call_id": "call_001",
                "name": "file_read",
                "arguments": "{\"path\":\"README.md\",\"reason\":\"inspect\"}",
            }],
        }

        envelopes = extract_tool_call_envelopes(
            response_data,
            provider="openai_responses",
            endpoint="primary",
        )

        assert envelopes == [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_001",
            "call_id": "call_001",
            "provider_item_id": "fc_001",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "file_read",
            "arguments": {"path": "README.md", "reason": "inspect"},
            "arguments_json": "{\"path\":\"README.md\",\"reason\":\"inspect\"}",
            "tool_class": "read_tool",
            "execution_route": "host_dispatch",
            "risk": "medium",
            "parse_status": "ok",
            "requires_guide": False,
            "audit_projection": "原生工具调用：file_read",
        }]

    def test_chat_tool_call_becomes_tool_call_envelope(self):
        from logic.native_tool_calls import extract_tool_call_envelopes

        response_data = {
            "id": "chatcmpl_001",
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "content": None,
                    "tool_calls": [{
                        "id": "chat_call_001",
                        "type": "function",
                        "function": {
                            "name": "web_search",
                            "arguments": "{\"query\":\"UPSP\",\"reason\":\"lookup\"}",
                        },
                    }],
                },
            }],
        }

        envelopes = extract_tool_call_envelopes(
            response_data,
            provider="openai_chat",
            endpoint="fallback",
        )

        assert envelopes[0]["provider"] == "openai_chat"
        assert envelopes[0]["call_id"] == "chat_call_001"
        assert envelopes[0]["provider_item_id"] == "chat_call_001"
        assert envelopes[0]["tool_id"] == "web_search"
        assert envelopes[0]["arguments"] == {"query": "UPSP", "reason": "lookup"}
        assert envelopes[0]["parse_status"] == "ok"

    def test_text_pseudo_tool_call_does_not_create_envelope(self):
        from logic.native_tool_calls import extract_tool_call_envelopes

        response_data = {
            "id": "resp_text",
            "output_text": "| tool_request | file_read | path=README.md |",
        }

        assert extract_tool_call_envelopes(
            response_data,
            provider="openai_responses",
            endpoint="primary",
        ) == []

    def test_unknown_and_substrate_tools_are_invalid_envelopes(self):
        from logic.native_tool_calls import extract_tool_call_envelopes

        response_data = {
            "id": "resp_invalid",
            "output": [
                {
                    "id": "fc_unknown",
                    "type": "function_call",
                    "call_id": "call_unknown",
                    "name": "not_a_tool",
                    "arguments": "{}",
                },
                {
                    "id": "fc_substrate",
                    "type": "function_call",
                    "call_id": "call_substrate",
                    "name": "context_assemble",
                    "arguments": "{}",
                },
            ],
        }

        envelopes = extract_tool_call_envelopes(
            response_data,
            provider="openai_responses",
            endpoint="primary",
        )

        assert [item["parse_status"] for item in envelopes] == [
            "unknown_tool_id",
            "unsupported_execution_route",
        ]
        assert envelopes[1]["execution_route"] == "substrate"

    def test_invalid_arguments_json_is_preserved_but_not_executable(self):
        from logic.native_tool_calls import extract_tool_call_envelopes

        response_data = {
            "id": "resp_bad_json",
            "output": [{
                "id": "fc_bad_json",
                "type": "function_call",
                "call_id": "call_bad_json",
                "name": "file_read",
                "arguments": "{\"path\":",
            }],
        }

        envelope = extract_tool_call_envelopes(
            response_data,
            provider="openai_responses",
            endpoint="primary",
        )[0]

        assert envelope["tool_id"] == "file_read"
        assert envelope["arguments"] == {}
        assert envelope["arguments_json"] == "{\"path\":"
        assert envelope["parse_status"] == "invalid_json"

    def test_registry_schema_export_includes_enabled_tools_and_excludes_substrate(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        responses_tools = export_provider_tool_schemas(provider="openai_responses")
        names = {item["name"] for item in responses_tools}

        assert "file_read" in names
        assert "file_glob" in names
        assert "file_grep" in names
        assert "web_search" in names
        assert "context_assemble" not in names
        assert all(item["type"] == "function" for item in responses_tools)

        chat_tools = export_provider_tool_schemas(provider="openai_chat")
        search_tool = next(
            item for item in chat_tools
            if item["function"]["name"] == "file_glob"
        )
        search_schema = search_tool["function"]["parameters"]
        assert search_schema["required"] == ["root", "pattern"]
        assert search_schema["additionalProperties"] is False
        assert "recursive" in search_schema["properties"]
        file_read_tool = next(
            item for item in chat_tools
            if item["function"]["name"] == "file_read"
        )
        assert file_read_tool["type"] == "function"
        assert file_read_tool["function"]["parameters"]["type"] == "object"

    def test_spec430_permission_level_filters_provider_general_tools(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        limited = export_provider_tool_schemas(
            provider="openai_responses",
            execution_permission_level="limited",
        )
        limited_names = {item["name"] for item in limited}

        assert {"file_read", "file_glob", "file_grep", "web_fetch", "web_search"} <= limited_names
        assert "file_edit" not in limited_names
        assert "file_write" not in limited_names
        assert "shell_command" not in limited_names
        assert "subagent_dispatch" not in limited_names

        unlimited = export_provider_tool_schemas(
            provider="openai_responses",
            execution_permission_level="unlimited",
        )
        unlimited_names = {item["name"] for item in unlimited}

        assert "file_write" in unlimited_names
        assert {"file_edit", "subagent_dispatch"} <= unlimited_names
        assert "shell_command" in unlimited_names

    def test_reaction_schema_exports_supported_protocol_writes_without_guide_request(self):
        from logic.native_tool_calls import (
            MEMORY_WRITE_WEIGHT_TABLE,
            _memory_write_feeling_words_description,
            export_provider_tool_schemas,
        )

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
            execution_permission_level="unlimited",
        )
        names = {item["name"] for item in tools}

        assert "protocol_tool_guide_request" not in names
        assert "memory_write" in names
        assert "memory_content_read" in names
        assert "index_view" in names
        assert "relation_read" in names
        assert "container_read" in names
        assert "mount_cancel" in names
        assert {
            "memory_link_update",
            "relation_card_write",
            "memory_container_create",
            "memory_container_write",
        } <= names
        assert "container_focus" not in names
        assert "memory_recall_complete" not in names
        assert {
            "chronicle_write",
            "alert_mode_settle",
            "fault_record",
            "cache_compact",
            "memory_privacy_mark",
            "memory_privacy_declassify",
        }.isdisjoint(names)
        assert "heartbeat_settle" not in names
        assert "skill_projection_settlement" not in names
        assert "context_assemble" not in names

        by_name = {item["name"]: item for item in tools}
        memory_tool = by_name["memory_write"]
        assert "不可覆写" in memory_tool["description"]
        assert "主体更新" in memory_tool["description"]
        assert "日常事实" in memory_tool["description"]
        assert "applied 回执" in memory_tool["description"]
        assert "MEM-*" in memory_tool["description"]
        assert "交互感受词" in memory_tool["description"]
        assert "关系感受词" in memory_tool["description"]
        assert json.dumps(memory_tool, ensure_ascii=False).count("权重表") == 1
        assert len(_memory_write_feeling_words_description()) == 640
        assert hashlib.sha256(
            _memory_write_feeling_words_description().encode("utf-8")
        ).hexdigest() == "7c2cf7848005740805c63587de47b9c6f86d48fe9e00132c313cf3167482e2e4"
        assert hashlib.sha256(MEMORY_WRITE_WEIGHT_TABLE.encode("utf-8")).hexdigest() == (
            "81a19f04db87a0623f88cf163d1e55cc402009bd514f2173f12d7ab6ca17ed5a"
        )

        file_edit_description = by_name["file_edit"]["description"]
        assert "unified diff patch" in file_edit_description
        assert "path/patch/purpose" in file_edit_description
        assert "tracked/allowlist" in file_edit_description
        assert "file_write" in file_edit_description
        assert "status=ok" in file_edit_description

        subagent_description = by_name["subagent_dispatch"]["description"]
        assert "task_goal/allowed_paths/expected_artifacts" in subagent_description
        assert "code_change" in subagent_description
        assert "write_scope" in subagent_description
        assert "backend_unavailable" in subagent_description
        assert "不得声称完成" in subagent_description

        assert all(
            "family=" not in item["description"]
            and "class=" not in item["description"]
            and "domain=" not in item["description"]
            and "risk=" not in item["description"]
            for item in tools
        )
        expected_structure_sha = {
            ("openai_responses", "limited"): "4c321cd2407002b6b75d7462c75c7c2047b505c8bfcce9239b8ac46d725a9013",
            ("openai_responses", "unlimited"): "34ce0655c30744e7d683e4240056826c6e89a82eeb674b938f3100842a0e0af7",
            ("openai_chat", "limited"): "18da64b1e4600c96df02c7f2abd092fe122f4ab4aa80faed25922e90ff4f12d9",
            ("openai_chat", "unlimited"): "026f79d4544ed8d2dc3ad8a742dfc340e3c048011edbceffeb99190b1fae5005",
            ("anthropic_messages", "limited"): "ffd5b0f3c324d04443bdda35ca54bc1effdd612c7c9e381ab17a205c2a303a10",
            ("anthropic_messages", "unlimited"): "9cf908d2f8da73b17cd8e827fe47a6ddddf505faed9d9933920c7e1b746deffa",
        }
        for (provider, permission), expected_sha in expected_structure_sha.items():
            provider_tools = export_provider_tool_schemas(
                provider=provider,
                include_protocol_writes=True,
                include_step_terminal_tools=["reaction_finalize"],
                execution_permission_level=permission,
            )
            assert len(provider_tools) == (20 if permission == "limited" else 24)
            structure_json = json.dumps(
                _without_descriptions(provider_tools),
                ensure_ascii=False,
                separators=(",", ":"),
                sort_keys=True,
            )
            assert hashlib.sha256(structure_json.encode("utf-8")).hexdigest() == expected_sha

    def test_spec748_all_exported_named_parameters_have_nearby_descriptions(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = []
        tools.extend(export_provider_tool_schemas(
            provider="openai_responses",
            include_standard_tools=False,
            include_step_terminal_tools=["setup_finalize"],
        ))
        tools.extend(export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
            execution_permission_level="unlimited",
        ))
        tools.extend(export_provider_tool_schemas(
            provider="openai_responses",
            include_standard_tools=False,
            include_step_terminal_tools=["cleanup_finalize"],
        ))

        def assert_described(tool_id, schema, path):
            if "enum" in schema:
                description = str(schema.get("description") or "")
                for value in schema["enum"]:
                    assert str(value) in description, (tool_id, path, value)
            for name, child in (schema.get("properties") or {}).items():
                child_path = f"{path}.{name}"
                assert str(child.get("description") or "").strip(), (
                    tool_id, child_path
                )
                assert_described(tool_id, child, child_path)
            items = schema.get("items")
            if schema.get("type") == "array" and isinstance(items, dict):
                assert_described(tool_id, items, f"{path}[]")

        by_name = {item["name"]: item for item in tools}
        assert len(by_name) == 26
        for tool_id, tool in by_name.items():
            assert_described(tool_id, tool["parameters"], tool_id)

        assert {
            "rules_selection", "round_type_confirm",
        }.isdisjoint(by_name["setup_finalize"]["parameters"]["properties"])
        mount_item = by_name["setup_finalize"]["parameters"]["properties"][
            "mount_requests"
        ]["items"]
        assert "source" not in mount_item["properties"]
        assert "suggested_mode" in by_name["setup_finalize"]["parameters"]["properties"]
        assert "risk_level" not in by_name["file_write"]["parameters"]["properties"]
        assert "shell_command" in by_name
        assert by_name["memory_link_update"]["parameters"]["properties"][
            "operation"
        ]["enum"] == ["remove"]
        assert "current_overview" not in by_name["memory_link_update"][
            "parameters"
        ]["properties"]
        assert "subject" not in by_name["relation_card_write"]["parameters"][
            "properties"
        ]
        for retired in ("chronicle_write", "alert_mode_settle", "fault_record"):
            assert retired not in by_name

    def test_spec292_index_view_and_container_schema_do_not_suggest_registry_lookup(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
        )
        by_name = {item["name"]: item for item in tools}
        index_schema_text = json.dumps(by_name["index_view"], ensure_ascii=False)
        container_read_props = by_name["container_read"]["parameters"]["properties"]

        assert "container_registry" not in index_schema_text
        assert "不提供容器注册表视图" in index_schema_text
        assert "具体容器编号" in container_read_props["container_id"]["description"]
        assert "EC、DC、PRJ、SKL、FUT 只是容器类型" in container_read_props["container_id"]["description"]
        assert "container_focus" not in by_name

    def test_spec216_memory_write_schema_keeps_low_weight_daily_memory_open(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        memory_tool = by_name["memory_write"]
        memory_params = memory_tool["parameters"]["properties"]
        finalize_params = by_name["reaction_finalize"]["parameters"]["properties"]

        assert "非噪音" in memory_tool["description"]
        assert "日常事实" in memory_tool["description"]
        assert "轻量交互" in memory_params["weight"]["description"]
        assert "日常事件" in memory_params["weight"]["description"]
        assert "长期保留时调用" not in memory_tool["description"]
        assert "memory_no_write_reason" not in finalize_params
        assert "memory_status" not in finalize_params

    def test_spec582_memory_write_schema_exposes_subject_update_trigger(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        memory_tool = by_name["memory_write"]
        memory_params = memory_tool["parameters"]["properties"]

        assert "主体更新" in memory_tool["description"]
        assert "偏好、边界、关系、判断、方法、环境约束" in memory_tool["description"]
        assert "无需等待用户要求" in memory_tool["description"]
        assert "轻量变化可用低权重" in memory_tool["description"]
        assert "workaround" not in memory_tool["description"]
        assert "主体更新依据" in memory_params["reason"]["description"]

    def test_spec247_memory_schema_uses_value_first_weight_language(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        memory_tool = by_name["memory_write"]
        memory_params = memory_tool["parameters"]["properties"]
        finalize_desc = by_name["reaction_finalize"]["description"]
        finalize_params = by_name["reaction_finalize"]["parameters"]["properties"]

        assert "非噪音主体更新" in memory_tool["description"]
        assert "权重按沉淀价值判断，不按材料来源判断" in memory_params["weight"]["description"]
        assert "3=有效沉淀" in memory_params["weight"]["description"]
        assert "可复用理解、判断、方法、路线感" in memory_params["weight"]["description"]
        assert "读书收获默认" not in memory_params["body"]["description"]
        assert "默认写成低权重梗概" not in memory_params["body"]["description"]
        assert "settlement_ledger" in finalize_desc
        assert "真实回执" in finalize_desc
        assert "memory_status" not in finalize_params

    def test_spec227_reading_memory_and_progress_boundary_are_explicit(self):
        from logic.native_tool_calls import export_provider_tool_schemas
        from logic.runtime_channels import MESSAGE_CHANNELS

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        memory_tool = by_name["memory_write"]
        memory_props = memory_tool["parameters"]["properties"]
        finalize_props = by_name["reaction_finalize"]["parameters"]["properties"]
        finalize_desc = by_name["reaction_finalize"]["description"]
        progress_channel = MESSAGE_CHANNELS["assistant_text"]
        combined_memory = json.dumps(
            {
                "tool": memory_tool,
                "weight": memory_props["weight"],
                "body": memory_props["body"],
                "reason": memory_props["reason"],
            },
            ensure_ascii=False,
        )

        assert "权重按沉淀价值判断，不按材料来源判断" in combined_memory
        assert "按权重限长" in combined_memory
        assert "3=有效沉淀" in combined_memory
        assert "可复用理解、判断、方法、路线感" in combined_memory
        assert "2=一般有效记录" in combined_memory
        assert "1=轻量但非噪音" in combined_memory
        assert "1/2=[A]≤128字" in combined_memory
        assert "3/4=[S]≤512字" in combined_memory
        assert "5=[F]≤2048字" in combined_memory
        assert "读书收获默认写成低权重梗概" not in combined_memory
        assert "文件读取后的梗概、日常事实和普通任务结果默认写成低权重梗概" not in combined_memory
        assert "权重1/2读书梗概建议" not in combined_memory
        assert set(finalize_props) == {"handoff_text"}
        assert "普通完成直接自然语言回复" in finalize_desc
        assert "模型不选择 finish/blocked" in finalize_desc
        assert "settlement_ledger" in finalize_desc
        assert "真实回执" in finalize_desc
        assert "读取游标" in finalize_desc
        assert "pending tracker" in finalize_desc
        assert "reaction_progress_emit" not in by_name
        assert progress_channel["block_kind"] == "dialogue_progress"
        assert progress_channel["tool_fact_material"] is False
        assert progress_channel["long_term_memory"] == "memory_tool_only"

    def test_spec759_memory_body_too_long_feedback_requests_rewrite_guide(self):
        from engines.reaction_helpers import native_tool_failure_feedbacks

        feedbacks = native_tool_failure_feedbacks([{
            "tool_id": "memory_write",
            "call_id": "call_mem",
            "status": "error",
            "reason": "memory_body_too_long:max=128;actual=176",
            "max_chars": 128,
            "actual_chars": 176,
            "over_by": 48,
            "target_chars": 120,
            "reduce_by": 56,
        }])

        assert len(feedbacks) == 1
        feedback = feedbacks[0]
        assert "next_action: use_memory_write_rewrite_guide" in feedback
        assert "actual=176, max=128" in feedback
        assert "记忆写入重写指南" in feedback
        assert "不要直接重试 memory_write" in feedback
        assert "价值依据" not in feedback
        assert "取消出口" not in feedback
        assert "提醒 ID" not in feedback
        assert "weight=3" not in feedback
        assert "优先升权重" not in feedback

    def test_spec219_memory_write_entry_text_uses_auto_rules_without_limit_gate(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        repo_root = Path(__file__).resolve().parents[2]
        memory_rule = (
            repo_root
            / "initialization" / "persona_template"
            / "rules"
            / "protocol"
            / "base"
            / "memory.md"
        ).read_text(encoding="utf-8")
        tools_doc = (
            repo_root
            / "initialization" / "persona_template"
            / "docs"
            / "protocol"
            / "base"
            / "tools.md"
        ).read_text(encoding="utf-8")
        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        schema_text = json.dumps(
            {
                "memory_write": by_name["memory_write"],
                "memory_search": by_name["memory_search"],
                "index_view": by_name["index_view"],
                "reaction_finalize": by_name["reaction_finalize"],
            },
            ensure_ascii=False,
        )
        combined = "\n".join([memory_rule, tools_doc, schema_text])

        assert "噪音过滤" in memory_rule
        assert "<!-- PROTOCOL_TOOL_GUIDE:memory_write_START -->" not in tools_doc
        assert "memory_entry_guide" not in tools_doc
        assert "内容字段禁止复述对话过程，只记结论与变化" in combined
        assert "记叙文六要素" in combined
        assert "不要求每条硬凑齐六项" in combined
        assert "非事件型记忆不套记叙模板" in combined
        assert "不得冒充事件时间" in combined
        assert "禁止补造精确日期" in combined
        assert "不同主体、不同时间锚点或彼此独立的事实应分别写入" in combined
        assert "工作容器负责组织多条记忆的演进关系" in combined
        assert "不等同于当前 `interaction_object`" in combined
        assert "关系域无卡、歧义或 archived 会拒绝" in combined
        assert "每轮建议写入不超过 3 条" in combined
        assert "有边界的历史会话" in combined
        assert "轻量生活事实已经处理" in combined
        assert "同时保留相对时间与可换算的绝对日期或范围" in schema_text
        assert "片段不是事实证据" in schema_text
        assert "memory_content_read" in schema_text
        assert "不需要把全部 LTM 正文一次性塞进上下文" in memory_rule
        forbidden = [
            "超过上限时",
            "同一反应步迭代最多成功写入一条记忆",
            "multiple_memory_write_declarations",
            "长期记忆沉淀",
            "长期沉淀",
            "完整研究结论",
            "超过三条",
        ]
        for phrase in forbidden:
            assert phrase not in combined
        assert "权重按沉淀价值判断，不按材料来源判断" in combined
        assert "3=有效沉淀" in combined
        assert "可复用理解、判断、方法、路线感" in combined
        assert "具体 MEM-*" in combined
        assert "先调用 memory_write" in combined
        assert "1/2=[A]≤128字" in combined
        assert "3/4=[S]≤512字" in combined
        assert "5=[F]≤2048字" in combined
        assert "权重1/2读书梗概建议" not in combined
        assert "读书收获默认写成低权重梗概" not in combined
        assert "默认写成低权重梗概" not in combined
        assert "按权重限长" in combined
        assert "即时重写指南" in combined
        assert "无需为了接近上限扩写、补齐或重复" in combined
        assert "长文读书应通过 file_read 按 bounded 工具窗口分段读取" not in schema_text
        assert "长文读书默认可通过 file_read 全文读取" not in schema_text
        assert "next_line_start" not in schema_text
        assert "cursor" not in schema_text
        assert "line_start/line_end" not in schema_text
        for stale_phrase in ("纯文件读取", "临时工作材料", "外部书稿", "等讨论后再沉淀"):
            assert stale_phrase not in combined
        assert "不得" in combined

    def test_active_protocol_guide_no_longer_filters_write_schema(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            active_protocol_tool_guides=["memory_link_update"],
        )
        names = {item["name"] for item in tools}

        assert "protocol_tool_guide_request" not in names
        assert "memory_write" in names
        assert "memory_link_update" in names
        assert "container_focus" not in names

    def test_file_read_schema_is_tool_specific_and_closed(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(provider="openai_responses")
        file_read = next(item for item in tools if item["name"] == "file_read")
        parameters = file_read["parameters"]
        properties = parameters["properties"]

        assert parameters["required"] == ["path"]
        assert parameters["additionalProperties"] is False
        assert set(properties) == {
            "path",
            "line_start",
            "encoding",
            "reason",
        }
        assert {"cursor", "line_end", "char_start", "char_end"}.isdisjoint(properties)
        assert "max_chars" not in properties
        assert "start_line" not in properties
        assert "end_line" not in properties
        assert "command" not in properties
        assert "url" not in properties
        assert "container_id" not in properties
        assert "task_goal" not in properties

    def test_spec400_file_read_native_retired_fields_are_dropped(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "parse_status": "ok",
            "tool_id": "file_read",
            "tool_class": "read_tool",
            "risk": "medium",
            "arguments": {
                "path": "book.md",
                "line_start": 121,
                "cursor": "line:121",
                "line_end": 0,
                "char_start": 4096,
                "char_end": "5000",
                "max_chars": 1,
                "reason": "Spec400 retired field pollution",
            },
            "call_id": "call_spec400_retired",
            "provider": "openai_chat",
            "index": 0,
        }], native_mode=True)

        assert routed["invalid_tool_requests"] == []
        request = routed["general_tool_requests"][0]
        assert request["tool_id"] == "file_read"
        assert request["path"] == "book.md"
        assert request["line_start"] == 121
        assert "cursor" not in request
        assert "line_end" not in request
        assert "char_start" not in request
        assert "char_end" not in request
        assert "max_chars" not in request

    def test_spec400_file_read_native_positive_retired_fields_are_ignored(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "parse_status": "ok",
            "tool_id": "file_read",
            "tool_class": "read_tool",
            "risk": "medium",
            "arguments": {
                "path": "book.md",
                "line_start": 121,
                "char_start": 4096,
                "reason": "still invalid retired field",
            },
            "call_id": "call_spec400_positive_retired",
            "provider": "openai_chat",
            "index": 0,
        }], native_mode=True)

        assert routed["invalid_tool_requests"] == []
        request = routed["general_tool_requests"][0]
        assert request["tool_id"] == "file_read"
        assert request["path"] == "book.md"
        assert request["line_start"] == 121
        assert "char_start" not in request

    def test_spec485_flat_guide_submit_routes_without_submissions_array(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "parse_status": "ok",
            "tool_id": "guide_submit",
            "tool_class": "sync_tool",
            "risk": "high",
            "arguments": {
                "guide_id": "task_bootstrap",
                "item_id": "build_initial_task_guide",
                "option_id": "submit_initial_guide",
                "task_title": "Daily agent eval",
                "items": [{"item_id": "item_01", "title": "Read source"}],
                "acceptance": [{
                    "acceptance_id": "acc_01",
                    "description": "Source is read.",
                    "item_refs": ["item_01"],
                }],
            },
            "call_id": "call_flat_guide_submit",
            "provider": "openai_chat",
            "index": 0,
        }], native_mode=True)

        assert routed["invalid_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == ["guide_submit"]
        request = routed["guide_submit_requests"][0]
        assert request["guide_id"] == "task_bootstrap"
        assert request["item_id"] == "build_initial_task_guide"
        assert request["option_id"] == "submit_initial_guide"
        assert request["task_title"] == "Daily agent eval"

    def test_flat_not_a_task_reason_survives_provider_route_and_processor(self, tmp_path):
        from data.state_store import StateStore
        from data.workbench import WorkbenchStore
        from logic.guide_submit import apply_guide_submit
        from logic.native_tool_calls import (
            apply_native_tool_calls_to_parsed_reaction,
            export_provider_tool_schemas,
            extract_tool_call_envelopes,
        )
        from logic.task_guide import create_task_bootstrap_guide
        from logic.work_intent_debt import create_work_intent_debt

        schema = next(
            item for item in export_provider_tool_schemas(
                provider="openai_chat",
                include_protocol_writes=True,
                active_protocol_tool_guides=["guide_submit"],
            )
            if item["function"]["name"] == "guide_submit"
        )
        assert "reason" in schema["function"]["parameters"]["properties"]

        arguments = {
            "guide_id": "task_bootstrap",
            "item_id": "build_initial_task_guide",
            "option_id": "not_a_task",
            "reason": "这只是一次有界只读核验。",
        }
        response = {
            "id": "chatcmpl_flat_not_a_task",
            "choices": [{
                "finish_reason": "tool_calls",
                "message": {
                    "content": None,
                    "tool_calls": [{
                        "id": "call_flat_not_a_task",
                        "type": "function",
                        "function": {
                            "name": "guide_submit",
                            "arguments": json.dumps(arguments, ensure_ascii=False),
                        },
                    }],
                },
            }],
        }
        calls = extract_tool_call_envelopes(
            response,
            provider="openai_chat",
            endpoint="test",
        )
        routed = apply_native_tool_calls_to_parsed_reaction({}, calls, native_mode=True)

        store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
        state_store = StateStore(str(tmp_path / "state.json"))
        state_store.init_if_missing()
        create_work_intent_debt(
            state_store,
            round_num=664,
            reason="误判为任务",
            source="setup_finalize",
        )
        create_task_bootstrap_guide(store, reason="误判为任务")
        receipt = apply_guide_submit(
            store,
            routed["guide_submit_requests"][0],
            evidence_context={"state_store": state_store},
        )

        assert receipt["status"] == "accepted"
        assert receipt["action"] == "task_bootstrap_dismissed"
        assert state_store.get("base.runtime.work_intent_debt") == {}

    def test_protocol_write_schemas_have_required_fields_and_enums(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            active_protocol_tool_guides=[
                "memory_write",
                "memory_container_create",
                "memory_container_write",
            ],
        )
        by_name = {item["name"]: item["parameters"] for item in tools}

        memory_write = by_name["memory_write"]
        assert memory_write["additionalProperties"] is False
        assert memory_write["required"] == [
            "title",
            "weight",
            "subject",
            "body",
            "candidate_keywords",
        ]
        assert memory_write["properties"]["title"]["type"] == "string"
        assert "<=16字" in memory_write["properties"]["title"]["description"]
        assert memory_write["properties"]["weight"]["type"] == "integer"
        assert "1-5" in memory_write["properties"]["weight"]["description"]
        assert "权重0不应调用" in memory_write["properties"]["weight"]["description"]
        assert "第一人称" in memory_write["properties"]["body"]["description"]
        assert "只写结论与变化" in memory_write["properties"]["body"]["description"]
        assert "不写对话/工具流水" in memory_write["properties"]["body"]["description"]
        assert memory_write["properties"]["candidate_keywords"]["type"] == "array"
        assert memory_write["properties"]["candidate_keywords"]["items"] == {"type": "string"}
        keyword_description = memory_write["properties"]["candidate_keywords"]["description"]
        assert "至少1项" in keyword_description
        assert "字符串数组" in keyword_description
        assert "每个关键词单列" in keyword_description
        assert "禁止用分隔符" in keyword_description
        relationship_feelings = memory_write["properties"]["relationship_feelings"]
        assert relationship_feelings["type"] == "array"
        assert relationship_feelings["items"] == {
            "type": "object",
            "properties": {
                "subject": {
                    "type": "string",
                    "description": "关系感受所属的活动关系卡稳定 ID/name/alias。",
                },
                "word": {
                    "type": "string",
                    "description": "只从 memory_write 说明中的关系感受词清单选择。",
                },
            },
            "required": ["subject", "word"],
            "additionalProperties": False,
        }
        assert "linked_containers" not in memory_write["properties"]

        assert "container_focus" not in by_name

        create_props = by_name["memory_container_create"]["properties"]
        assert by_name["memory_container_create"]["required"] == [
            "mem_id",
            "container_type",
            "title",
            "target_file",
            "container_body",
            "current_overview",
            "reason",
        ]
        assert create_props["container_type"]["enum"] == ["DC", "EC", "PRJ", "SKL", "FUT"]
        assert create_props["skill_category"]["enum"] == ["procedures", "patterns"]
        assert "container_type=SKL" in create_props["skill_category"]["description"]
        assert "最长64字符" in create_props["skill_name"]["description"]
        target_description = create_props["target_file"]["description"]
        assert "DC/EC=open.md" in target_description
        assert "PRJ=plan.md/notes.md" in target_description
        assert "SKL=card.md" in target_description
        assert "FUT=objectives.md/plans.md/predictions.md" in target_description
        assert "不是复制 MEM" in create_props["container_body"]["description"]

        write_props = by_name["memory_container_write"]["properties"]
        assert by_name["memory_container_write"]["required"] == [
            "mem_id",
            "container_id",
            "target_file",
            "title",
            "container_body",
            "current_overview",
            "reason",
        ]
        assert "本 Frame 起点" in write_props["container_id"]["description"]
        assert "本 Frame 起点已可见" in write_props["target_file"]["description"]
        assert "DC/EC=open.md" in write_props["target_file"]["description"]

        assert "fault_record" not in by_name
        assert "heartbeat_settle" not in by_name

    def test_spec148_protocol_read_schemas_are_exported_and_closed(self):
        from logic.native_tool_calls import export_provider_tool_schemas
        from logic import protocol_tools

        tools = export_provider_tool_schemas(provider="openai_responses")
        by_name = {item["name"]: item["parameters"] for item in tools}
        expected = {
            "corpus_read",
            "index_view",
            "memory_search",
            "relation_read",
            "memory_content_read",
            "container_read",
        }

        assert expected <= set(by_name)
        for tool_id in expected:
            meta = protocol_tools.tool_metadata_for(tool_id)
            assert meta["tool_class"] == "read_tool"
            assert meta["execution_route"] == "internal_processor"
            assert meta["result_kind"] == "protocol_tool_receipt"
            assert by_name[tool_id]["additionalProperties"] is False

        assert by_name["index_view"]["required"] == ["scope"]
        assert by_name["index_view"]["properties"]["scope"]["enum"] == [
            "ltm_heat",
            "stm_heat",
            "skills_inverted",
            "ltm_inverted",
            "stm_inverted",
            "association",
            "relation_inverted",
            "relation_domain",
        ]
        assert "query_terms" not in by_name["index_view"]["properties"]
        assert by_name["memory_search"]["required"] == ["query_terms"]
        assert by_name["memory_search"]["properties"]["query_terms"]["maxItems"] == 8
        assert by_name["memory_search"]["properties"]["query_terms"]["items"]["maxLength"] == 64
        assert by_name["corpus_read"]["required"] == ["corpus_id"]
        assert sorted(by_name["corpus_read"]["properties"]) == ["corpus_id"]
        assert by_name["memory_content_read"]["required"] == ["mem_id"]
        assert by_name["memory_content_read"]["properties"]["mount_mode"]["enum"] == [
            "temporary",
            "resident",
            "none",
        ]
        assert by_name["container_read"]["required"] == ["container_id"]
        assert by_name["relation_read"].get("required", []) == []

    def test_spec305_mount_cancel_schema_is_exported_and_closed(self):
        from logic.native_tool_calls import export_provider_tool_schemas
        from logic import protocol_tools

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
        )
        by_name = {item["name"]: item["parameters"] for item in tools}

        assert "mount_cancel" in by_name
        meta = protocol_tools.tool_metadata_for("mount_cancel")
        assert meta["tool_class"] == "sync_tool"
        assert meta["execution_route"] == "internal_processor"
        assert meta["domain"] == "context_mount"
        assert meta["risk"] == "medium"
        assert meta["result_kind"] == "protocol_tool_receipt"
        assert by_name["mount_cancel"]["required"] == ["mount_area", "item_id"]
        assert by_name["mount_cancel"]["additionalProperties"] is False
        assert by_name["mount_cancel"]["properties"]["mount_area"]["enum"] == [
            "resident_list",
            "instant_list",
        ]
        assert by_name["mount_cancel"]["properties"]["item_type"]["enum"] == [
            "auto",
            "memory",
            "container",
            "relation",
        ]

    def test_spec305_native_mount_cancel_routes_to_request_declaration(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_mount_cancel",
            "call_id": "call_mount_cancel",
            "provider_item_id": "fc_mount_cancel",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "mount_cancel",
            "arguments": {
                "mount_area": "instant_list",
                "item_type": "memory",
                "item_id": "MEM-305",
                "reason": "no longer needed",
            },
            "arguments_json": "{}",
            "tool_class": "sync_tool",
            "risk": "medium",
            "parse_status": "ok",
        }], native_mode=True)

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == ["mount_cancel"]
        assert routed["native_protocol_tool_submissions"] == ["mount_cancel"]
        assert routed["mount_cancel_requests"] == [{
            "mount_area": "instant_list",
            "item_type": "memory",
            "item_id": "MEM-305",
            "reason": "no longer needed",
            "call_id": "call_mount_cancel",
            "provider": "openai_responses",
            "response_id": "resp_mount_cancel",
            "provider_item_id": "fc_mount_cancel",
            "index": 0,
        }]
        assert routed["invalid_tool_requests"] == []
        assert routed["exit_signal"] == "waiting_tool"

    def test_spec148_native_protocol_read_tools_route_without_submissions(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        envelopes = []
        for index, (tool_id, arguments) in enumerate([
            ("corpus_read", {"corpus_id": "C-00001"}),
            ("index_view", {"scope": "ltm_heat", "limit": 2}),
            ("memory_search", {"query_terms": ["figurines"], "limit": 2}),
            ("relation_read", {"subject": "TzPz", "summary": "temporary"}),
            ("memory_content_read", {"mem_id": "MEM-1"}),
            ("container_read", {"container_id": "PRJ-1", "target_file": "notes.md"}),
        ]):
            envelopes.append({
                "source": "provider_tool_call",
                "provider": "openai_responses",
                "response_id": f"resp_spec148_{index}",
                "call_id": f"call_spec148_{index}",
                "provider_item_id": f"fc_spec148_{index}",
                "index": index,
                "tool_id": tool_id,
                "tool_class": "read_tool",
                "risk": "low",
                "parse_status": "ok",
                "arguments": arguments,
            })

        routed = apply_native_tool_calls_to_parsed_reaction(
            {"exit_signal": "done"},
            envelopes,
            native_mode=True,
        )

        assert [request["tool_id"] for request in routed["protocol_tool_requests"]] == [
            "corpus_read",
            "index_view",
            "memory_search",
            "relation_read",
            "memory_content_read",
            "container_read",
        ]
        assert routed["general_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == []
        assert routed["native_protocol_tool_submissions"] == []
        assert routed["invalid_tool_requests"] == []
        assert routed["exit_signal"] == "waiting_tool"

    def test_chat_schema_uses_same_closed_parameters(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(provider="openai_chat")
        file_read = next(
            item["function"] for item in tools
            if item["function"]["name"] == "file_read"
        )

        assert file_read["parameters"]["required"] == ["path"]
        assert file_read["parameters"]["additionalProperties"] is False

    def test_all_exported_native_schemas_have_tool_contracts(self):
        from logic.native_tool_calls import (
            TOOL_ARGUMENT_SCHEMAS,
            export_provider_tool_schemas,
        )

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            active_protocol_tool_guides=[
                "memory_write",
                "memory_link_update",
                "memory_privacy_mark",
                "memory_privacy_declassify",
                "relation_card_write",
                "fault_record",
            ],
        )
        exported_names = {item["name"] for item in tools}

        assert exported_names <= set(TOOL_ARGUMENT_SCHEMAS)
        for tool in tools:
            assert tool["parameters"]["additionalProperties"] is False

    def test_spec184_executor_exports_step_terminal_native_tools(self):
        from engines.executor import APIExecutor

        executor = APIExecutor.__new__(APIExecutor)

        def names_for(step, active_guides=None):
            return [
                item["name"]
                for item in executor._native_tools_for_step(
                    step,
                    provider="openai_responses",
                    active_protocol_tool_guides=active_guides,
                )
            ]

        assert names_for("setup") == ["setup_finalize"]
        reaction_names = names_for("reaction")
        assert "reaction_finalize" in reaction_names
        assert "reaction_progress_emit" not in reaction_names
        assert "protocol_tool_guide_request" not in reaction_names
        assert "file_read" in reaction_names
        assert "memory_write" in reaction_names
        assert "memory_link_update" in reaction_names
        guided_reaction_names = names_for(
            "reaction",
            active_guides=["memory_link_update"],
        )
        assert "memory_write" in guided_reaction_names
        assert "memory_link_update" in guided_reaction_names
        assert set(guided_reaction_names) == set(reaction_names) | {"guide_submit"}
        assert names_for("cleanup") == ["cleanup_finalize"]

    def test_spec458_step_terminal_tools_are_protocol_native_terminals(self):
        from logic.native_tool_calls import export_provider_tool_schemas
        from logic.protocol_tools import tool_metadata_for

        expected_steps = {
            "setup_finalize": "setup",
            "reaction_finalize": "reaction",
            "cleanup_finalize": "cleanup",
        }
        for tool_id, step in expected_steps.items():
            meta = tool_metadata_for(tool_id)
            assert meta["execution_route"] == "internal_processor"
            assert meta["result_kind"] == "protocol_tool_receipt"
            assert meta["native_only"] is True
            assert meta["step_terminal"] == step

        setup_tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_step_terminal_tools=["setup_finalize"],
            include_standard_tools=False,
        )
        assert [item["name"] for item in setup_tools] == ["setup_finalize"]

        normal_tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_standard_tools=False,
        )
        assert [item["name"] for item in normal_tools] == []

    def test_spec443_rhythm_guides_do_not_remove_subject_write_tools(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        normal_names = {
            item["name"]
            for item in export_provider_tool_schemas(
                provider="openai_responses",
                include_protocol_writes=True,
                include_step_terminal_tools=["reaction_finalize"],
                include_standard_tools=True,
                execution_permission_level="unlimited",
            )
        }
        rhythm_guided_names = {
            item["name"]
            for item in export_provider_tool_schemas(
                provider="openai_responses",
                include_protocol_writes=True,
                include_step_terminal_tools=["reaction_finalize"],
                include_standard_tools=True,
                active_protocol_tool_guides=["chronicle_write"],
                execution_permission_level="unlimited",
            )
        }

        assert rhythm_guided_names == normal_names | {"guide_submit"}
        assert "memory_write" in rhythm_guided_names
        assert "relation_card_write" in rhythm_guided_names
        assert "container_focus" not in rhythm_guided_names
        assert "guide_submit" in rhythm_guided_names
        assert "chronicle_write" not in rhythm_guided_names

    def test_spec466_retired_rhythm_native_writes_are_not_provider_visible(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
            active_protocol_tool_guides=[
                "guide_submit",
                "chronicle_write",
                "alert_mode_settle",
                "fault_record",
            ],
        )
        names = {item["name"] for item in tools}

        assert "guide_submit" in names
        assert "memory_write" in names
        assert {"chronicle_write", "alert_mode_settle", "fault_record"}.isdisjoint(
            names
        )

    def test_spec466_retired_rhythm_native_write_call_is_rejected(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        parsed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_retired",
            "call_id": "call_chronicle",
            "provider_item_id": "fc_chronicle",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "chronicle_write",
            "arguments": {"content": "旧入口不再可见。"},
            "arguments_json": "{\"content\":\"旧入口不再可见。\"}",
            "tool_class": "sync_tool",
            "risk": "medium",
            "parse_status": "ok",
        }], native_mode=True)

        assert parsed.get("chronicle_write_declarations", []) == []
        assert parsed["invalid_tool_requests"][0]["tool_id"] == "chronicle_write"
        assert parsed["invalid_tool_requests"][0]["reason"] == (
            "native_protocol_write_not_enabled"
        )

    def test_spec592_setup_finalize_schema_restores_task_guidance_fields(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_step_terminal_tools=["setup_finalize"],
            include_standard_tools=False,
        )
        setup = {item["name"]: item["parameters"] for item in tools}["setup_finalize"]
        props = setup["properties"]

        assert props["task_guidance_required"]["type"] == "boolean"
        assert set(props["task_guidance_route"]["enum"]) == {
            "none",
            "new_work",
            "current_work",
        }
        assert props["task_guidance_reason"]["type"] == "string"
        assert "interaction_object" in props
        assert "standby_skip_reaction" in props
        assert "task_guidance_required" in tools[0]["description"]
        assert "task_guidance_route" in tools[0]["description"]
        assert "读取材料" in tools[0]["description"]
        assert "PRJ 因跨轮而必为 true" in tools[0]["description"]
        assert "单轮有界 memory_write 或 DC/EC/FUT" in tools[0]["description"]
        assert "内部工具步骤不等于用户派发检索任务" in tools[0]["description"]
        assert "不得据此豁免整个任务" in tools[0]["description"]

        required_help = props["task_guidance_required"]["description"]
        assert "multi-step or multi-source" in required_help
        assert "PRJ work is cross-round and therefore true" in required_help
        assert "bounded single-round memory_write or DC/EC/FUT" in required_help
        assert "internal tool use is not itself a user-assigned research task" in required_help
        assert "does not exempt that larger task" in required_help

    def test_spec592_setup_finalize_projects_task_guidance_payload(self):
        from logic.native_tool_calls import terminal_finalize_from_envelopes

        intent, ordinary, invalids = terminal_finalize_from_envelopes(
            [{
                "tool_id": "setup_finalize",
                "arguments": {
                    "security_verdict": "pass",
                    "task_guidance_required": True,
                    "task_guidance_route": "current_work",
                    "task_guidance_reason": "用户追加现有任务验收要求",
                },
                "parse_status": "ok",
            }],
            "setup",
        )

        assert intent["task_guidance_required"] is True
        assert intent["task_guidance_route"] == "current_work"
        assert intent["task_guidance_reason"] == "用户追加现有任务验收要求"
        assert ordinary == []
        assert invalids == []

    def test_non_reaction_steps_reject_ordinary_native_tools(self):
        from logic.native_tool_calls import terminal_finalize_from_envelopes

        setup_intent, ordinary, invalids = terminal_finalize_from_envelopes(
            [{
                "provider": "openai_chat",
                "response_id": "resp_setup",
                "call_id": "call_file_read",
                "provider_item_id": "call_file_read",
                "index": 0,
                "raw_type": "function",
                "tool_id": "file_read",
                "arguments": {"path": "README.md"},
                "arguments_json": "{\"path\":\"README.md\"}",
                "tool_class": "read_tool",
                "parse_status": "ok",
            }],
            "setup",
        )

        assert setup_intent is None
        assert ordinary == []
        assert invalids[0]["tool_id"] == "file_read"
        assert invalids[0]["reason"] == "step_non_reaction_tool_not_allowed"

    def test_spec184_spec285_native_terminal_schemas_use_flat_setup_identity_fields(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_step_terminal_tools=["setup_finalize", "reaction_finalize", "cleanup_finalize"],
        )
        tool_by_name = {item["name"]: item for item in tools}
        by_name = {item["name"]: item["parameters"] for item in tools}

        assert by_name["setup_finalize"]["additionalProperties"] is False
        assert by_name["setup_finalize"]["properties"]["security_verdict"]["enum"] == [
            "pass",
            "reject",
        ]
        assert "interaction_meta" not in by_name["setup_finalize"]["properties"]
        assert by_name["setup_finalize"]["properties"]["interaction_object"]["type"] == "string"
        assert by_name["setup_finalize"]["properties"]["identity_status"]["enum"] == [
            "known",
            "declared",
            "unknown",
            "timeout",
        ]
        assert by_name["setup_finalize"]["properties"]["interaction_source"]["type"] == "string"
        assert by_name["setup_finalize"]["properties"]["interaction_basis"]["type"] == "string"
        assert "note" not in by_name["setup_finalize"]["properties"]
        assert "standby_reaction_hint" not in by_name["setup_finalize"]["properties"]
        assert "只能通过 provider-native setup_finalize 生效" in tool_by_name["setup_finalize"]["description"]
        assert "裸文本、旧表格和自然语言判断不生效" in tool_by_name["setup_finalize"]["description"]
        assert "起手步不读取材料、不建任务账本、不执行用户任务" in tool_by_name["setup_finalize"]["description"]
        assert by_name["reaction_finalize"]["additionalProperties"] is False
        assert set(by_name["reaction_finalize"]["properties"]) == {
            "handoff_text",
        }
        for retired in (
                "closeout_decision",
                "reaction_loop_done",
                "to_next_reaction_iter",
                "to_next_setup",
                "to_next_reaction",
                "relay_reason",
                "assistant_reply",
                "memory_no_write_reason",
                "memory_status",
                "memory_reason",
                "read_status",
                "read_reason",
                "pending_status",
                "pending_reason",
                "identity_action",
                "identity_object",
                "identity_status",
                "identity_note",
                "final_closeout",
                "relay_closeout",
                "memory_settlement",
                "read_settlement",
                "obligation_resolutions",
                "assistant_progress",
                "tool_summary"):
            assert retired not in by_name["reaction_finalize"]["properties"]
        finalize_props = by_name["reaction_finalize"]["properties"]
        assert by_name["reaction_finalize"]["required"] == [
            "handoff_text",
        ]
        assert "跨轮继续" in finalize_props["handoff_text"]["description"]
        assert "普通完成直接自然语言回复" in tool_by_name["reaction_finalize"]["description"]
        assert "模型不选择 finish/blocked" in tool_by_name["reaction_finalize"]["description"]
        assert "final_reply 阶段" not in tool_by_name["reaction_finalize"]["description"]
        assert "settlement_ledger" in tool_by_name["reaction_finalize"]["description"]
        assert "真实回执" in tool_by_name["reaction_finalize"]["description"]
        assert "reaction_progress_emit" not in by_name
        assert by_name["cleanup_finalize"]["additionalProperties"] is False
        assert by_name["cleanup_finalize"]["properties"]["connection_bridges"]["items"]["type"] == "object"
        assert "lately_compression" not in by_name["cleanup_finalize"]["properties"]
        assert "handoff_note" not in by_name["cleanup_finalize"]["properties"]

    def test_spec575_reaction_finalize_description_allows_post_settled_mixed_handoff(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        description = by_name["reaction_finalize"]["description"]

        assert "必填 handoff_text" in description
        assert "可与最后一批普通工具" in description
        assert "Runtime 先结算普通工具再结算中继" in description
        assert "必须单独提交 handoff_text" not in description
        assert "不要与 guide_submit" not in description
        assert "拒绝本次中继收束" not in description
        assert "closeout-only 车道中只能提交 reaction_finalize" not in description
        assert "settlement_ledger" in description

    def test_spec285_setup_finalize_projects_flat_identity_fields(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("setup", {
            "security_verdict": "pass",
            "interaction_object": "TzPz",
            "identity_status": "known",
            "interaction_source": "context_continuity",
            "interaction_basis": "recent reading task continuity",
        })

        assert parsed["interaction_meta"] == {
            "interaction_object": "TzPz",
            "identity_status": "known",
            "interaction_source": "context_continuity",
            "basis": "recent reading task continuity",
        }

    def test_spec285_setup_finalize_rejects_legacy_interaction_meta_string(self):
        from logic.native_tool_calls import terminal_finalize_from_envelopes

        setup_intent, ordinary, invalids = terminal_finalize_from_envelopes(
            [{
                "provider": "openai_responses",
                "response_id": "resp_setup_legacy_meta",
                "call_id": "call_setup_legacy_meta",
                "provider_item_id": "fc_setup_legacy_meta",
                "index": 0,
                "raw_type": "function_call",
                "tool_id": "setup_finalize",
                "arguments": {
                    "security_verdict": "pass",
                    "interaction_meta": (
                        '{"interaction_object":"TzPz",'
                        '"identity_status":"known",'
                        '"interaction_source":"context_continuity"}'
                    ),
                },
                "arguments_json": "{}",
                "tool_class": "sync_tool",
                "parse_status": "ok",
            }],
            "setup",
        )

        assert setup_intent is None
        assert ordinary == []
        assert invalids[0]["reason"] == "native_argument_unknown_field"
        assert invalids[0]["field"] == "interaction_meta"

    def test_spec184_native_mode_clears_text_terminal_fields(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        parsed = {
            "assistant_reply": "text reply must not become final_response",
            "exit_signal": "done",
            "reaction_loop": {"reaction_loop_done": True},
            "protocol_tool_requests": [],
            "general_tool_requests": [{"tool_id": "file_read", "path": "README.md"}],
            "invalid_tool_requests": [],
        }

        routed = apply_native_tool_calls_to_parsed_reaction(
            parsed,
            [],
            native_mode=True,
        )

        assert routed.get("assistant_reply") in (None, "")
        assert routed.get("reaction_loop") in (None, {})
        assert routed["general_tool_requests"] == []
        assert routed["invalid_tool_requests"][0]["reason"] == "native_tool_call_required"

    def test_spec184_reaction_finalize_projects_handoff_to_parsed_reaction(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("reaction", {
            "handoff_text": "下一轮继续处理。",
        })

        assert "assistant_reply" not in parsed
        assert parsed["reaction_loop"] == {
            "reaction_loop_done": True,
        }
        assert parsed["closeout_form"] == {
            "closeout_decision": "continue",
            "handoff_text": "下一轮继续处理。",
        }
        assert "memory_settlement" not in parsed
        assert "read_settlement" not in parsed
        assert "obligation_resolutions" not in parsed

    def test_spec268_reaction_finalize_ignores_model_supplied_refs(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_step_terminal_tools=["reaction_finalize"],
            include_standard_tools=False,
        )
        params = {tool["name"]: tool["parameters"] for tool in tools}[
            "reaction_finalize"
        ]

        assert "memory_refs" not in params["properties"]
        assert "read_refs" not in params["properties"]
        assert "obligation_resolutions" not in params["properties"]

    def test_spec253_reaction_finalize_schema_is_form_not_reply_container(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_step_terminal_tools=["reaction_finalize"],
            include_standard_tools=False,
        )
        by_name = {tool["name"]: tool["parameters"] for tool in tools}

        reaction_finalize = by_name["reaction_finalize"]
        assert "assistant_reply" not in reaction_finalize["properties"]
        assert "final_closeout" not in reaction_finalize["properties"]
        assert "to_cleanup" not in reaction_finalize["properties"]
        assert reaction_finalize["required"] == ["handoff_text"]
        assert set(reaction_finalize["properties"]) == {"handoff_text"}

    def test_spec253_reaction_finalize_rejects_retired_assistant_reply(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("reaction", {
            "assistant_reply": "旧字段不再承载用户最终回复。",
            "handoff_text": "下一轮继续。",
        })

        assert "assistant_reply" not in parsed
        assert "reaction_finalize.retired_field:assistant_reply" in (
            parsed["reaction_finalize_errors"]
        )

    def test_spec272_reaction_finalize_schema_is_flat_closeout_form(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_step_terminal_tools=["reaction_finalize"],
            include_standard_tools=False,
        )
        tools_by_name = {tool["name"]: tool for tool in tools}
        by_name = {tool["name"]: tool["parameters"] for tool in tools}

        params = by_name["reaction_finalize"]
        assert "final_closeout" not in params["properties"]
        assert "relay_closeout" not in params["properties"]
        assert "obligation_resolutions" not in params["properties"]
        assert "memory_settlement" not in params["properties"]
        assert "read_settlement" not in params["properties"]
        assert "to_cleanup" not in params["properties"]
        assert params["required"] == ["handoff_text"]
        assert set(params["properties"]) == {"handoff_text"}
        assert "settlement_ledger" in tools_by_name["reaction_finalize"]["description"]

    def test_spec290_reaction_finalize_schema_is_occam_closeout_tool(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_step_terminal_tools=["reaction_finalize"],
            include_standard_tools=False,
        )
        tool = {item["name"]: item for item in tools}["reaction_finalize"]
        params = tool["parameters"]

        assert params["additionalProperties"] is False
        assert set(params["properties"]) == {"handoff_text"}
        assert params["required"] == ["handoff_text"]
        for retired in (
                "memory_status",
                "memory_reason",
                "read_status",
                "read_reason",
                "pending_status",
                "pending_reason",
                "identity_action",
                "identity_object",
                "identity_status",
                "identity_note",
                "to_next_setup",
                "to_next_reaction",
                "relay_reason",
                "closeout_note"):
            assert retired not in params["properties"]
        assert "handoff_text" in tool["description"]
        assert "settlement_ledger" in tool["description"]

    def test_spec272_reaction_finalize_projects_flat_closeout_form(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("reaction", {
            "handoff_text": "下一轮挂载读书上下文，从第 961 行继续读取。",
        })

        assert parsed["reaction_loop"] == {
            "reaction_loop_done": True,
        }
        assert parsed["closeout_form"] == {
            "closeout_decision": "continue",
            "handoff_text": "下一轮挂载读书上下文，从第 961 行继续读取。",
        }
        assert "memory_settlement" not in parsed
        assert "read_settlement" not in parsed
        assert "relay_closeout" not in parsed
        assert "obligation_resolutions" not in parsed

    def test_spec290_reaction_finalize_projects_handoff_text_only(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("reaction", {
            "handoff_text": "下一轮继续读取 book.md 第 121 行。",
        })

        assert parsed["reaction_loop"] == {"reaction_loop_done": True}
        assert parsed["closeout_form"] == {
            "closeout_decision": "continue",
            "handoff_text": "下一轮继续读取 book.md 第 121 行。",
        }
        assert "to_next_setup" not in parsed["reaction_loop"]
        assert "to_next_reaction" not in parsed["reaction_loop"]
        assert "relay_reason" not in parsed["closeout_form"]
        assert "memory_status" not in parsed["closeout_form"]
        assert "read_status" not in parsed["closeout_form"]
        assert "pending_status" not in parsed["closeout_form"]
        assert "reaction_finalize_errors" not in parsed

    def test_spec290_reaction_finalize_continue_requires_handoff_text(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("reaction", {})

        assert parsed["reaction_finalize_errors"] == [
            "reaction_finalize.handoff_text_required",
        ]

    def test_spec272_reaction_finalize_rejects_retired_nested_fields(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("reaction", {
            "final_closeout": {
                "to_cleanup": "旧表单。",
                "memory_settlement": {"status": "weight_zero"},
                "read_settlement": {"status": "not_applicable"},
            },
            "relay_closeout": {
                "to_next_setup": "旧中继",
                "to_next_reaction": "旧中继",
                "reason": "旧中继",
            },
            "obligation_resolutions": [],
        })

        assert parsed["reaction_finalize_errors"] == [
            "reaction_finalize.retired_field:final_closeout",
            "reaction_finalize.retired_field:relay_closeout",
            "reaction_finalize.retired_field:obligation_resolutions",
            "reaction_finalize.handoff_text_required",
        ]

    def test_spec289_reaction_finalize_rejects_retired_to_cleanup_field(self):
        from logic.native_tool_calls import terminal_finalize_from_envelopes

        projected, ordinary, invalids = terminal_finalize_from_envelopes([{
            "tool_id": "reaction_finalize",
            "tool_class": "sync_tool",
            "parse_status": "ok",
            "provider": "openai_responses",
            "arguments": {
                "handoff_text": "下一轮继续。",
                "to_cleanup": "旧善后交接字段。",
            },
        }], "reaction")

        assert projected is None
        assert ordinary == []
        assert invalids[0]["reason"] == "native_argument_unknown_field"
        assert invalids[0]["field"] == "to_cleanup"

    def test_spec290_reaction_finalize_rejects_retired_model_settlement_fields(self):
        from logic.native_tool_calls import terminal_finalize_from_envelopes

        projected, ordinary, invalids = terminal_finalize_from_envelopes([{
            "tool_id": "reaction_finalize",
            "tool_class": "sync_tool",
            "parse_status": "ok",
            "provider": "openai_responses",
            "arguments": {
                "handoff_text": "下一轮继续。",
                "memory_status": "weight_zero",
            },
        }], "reaction")

        assert projected is None
        assert ordinary == []
        assert invalids[0]["reason"] == "native_argument_unknown_field"
        assert invalids[0]["field"] == "memory_status"

    def test_anthropic_reaction_finalize_flat_fields_project_to_closeout_form(self):
        from logic.native_tool_calls import terminal_finalize_from_envelopes

        projected, ordinary, invalids = terminal_finalize_from_envelopes([{
            "tool_id": "reaction_finalize",
            "tool_class": "sync_tool",
            "parse_status": "ok",
            "provider": "anthropic_messages",
            "arguments": {
                "handoff_text": "下一轮继续。",
            },
        }], "reaction")

        assert ordinary == []
        assert invalids == []
        assert "assistant_reply" not in projected
        assert projected["reaction_loop"] == {
            "reaction_loop_done": True,
        }
        assert projected["closeout_form"] == {
            "closeout_decision": "continue",
            "handoff_text": "下一轮继续。",
        }

    def test_spec240_reaction_finalize_continue_projects_to_relay_route(self):
        from logic.native_tool_calls import project_step_finalize

        parsed = project_step_finalize("reaction", {
            "handoff_text": "下一轮继续挂载读书上下文，从第581行继续读取。",
        })

        assert "assistant_reply" not in parsed
        assert parsed["reaction_loop"] == {
            "reaction_loop_done": True,
        }
        assert parsed["closeout_form"]["closeout_decision"] == "continue"
        assert (
            parsed["closeout_form"]["handoff_text"]
            == "下一轮继续挂载读书上下文，从第581行继续读取。"
        )

    def test_spec381_retired_reaction_progress_emit_is_rejected(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction(
            {},
            [{
                "tool_id": "reaction_progress_emit",
                "tool_class": "sync_tool",
                "arguments": {"message": "still reading"},
                "parse_status": "ok",
                "index": 0,
            }],
            native_mode=True,
        )

        assert routed["assistant_progress"] == ""
        assert routed["reaction_loop"] == {}
        assert routed["invalid_tool_requests"][0]["tool_id"] == "reaction_progress_emit"
        assert routed["invalid_tool_requests"][0]["reason"] == "unsupported_execution_route"

    def test_native_envelopes_replace_text_tool_requests_for_runtime_routing(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        parsed = {
            "assistant_reply": "",
            "exit_signal": "done",
            "protocol_tool_requests": [{"tool_id": "relation_read"}],
            "general_tool_requests": [{"tool_id": "shell_command", "command": "del x"}],
            "invalid_tool_requests": [],
        }
        envelopes = [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_004",
            "call_id": "call_004",
            "provider_item_id": "fc_004",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "file_read",
            "arguments": {"path": "README.md", "reason": "inspect"},
            "arguments_json": "{\"path\":\"README.md\",\"reason\":\"inspect\"}",
            "tool_class": "read_tool",
            "risk": "medium",
            "parse_status": "ok",
            "requires_guide": False,
            "audit_projection": "原生工具调用：file_read",
        }]

        routed = apply_native_tool_calls_to_parsed_reaction(
            parsed, envelopes, native_mode=True)

        assert routed["protocol_tool_requests"] == []
        assert routed["general_tool_requests"] == [{
            "path": "README.md",
            "reason": "inspect",
            "tool_id": "file_read",
            "tool_class": "read_tool",
            "risk": "medium",
            "source": "provider_tool_call",
            "call_id": "call_004",
            "provider": "openai_responses",
            "response_id": "resp_004",
            "provider_item_id": "fc_004",
            "index": 0,
        }]
        assert routed["exit_signal"] == "waiting_tool"

    def test_native_mode_empty_envelopes_retire_text_tool_requests(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        parsed = {
            "assistant_reply": "",
            "exit_signal": "waiting_tool",
            "protocol_tool_requests": [{"tool_id": "relation_read"}],
            "general_tool_requests": [{"tool_id": "file_read", "path": "README.md"}],
            "invalid_tool_requests": [],
        }

        routed = apply_native_tool_calls_to_parsed_reaction(
            parsed, [], native_mode=True)

        assert routed["protocol_tool_requests"] == []
        assert routed["general_tool_requests"] == []
        assert [item["reason"] for item in routed["invalid_tool_requests"]] == [
            "native_tool_call_required",
            "native_tool_call_required",
        ]

    def test_native_invalid_envelopes_route_to_invalid_tool_requests(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_005",
            "call_id": "call_005",
            "provider_item_id": "fc_005",
            "index": 0,
            "tool_id": "context_assemble",
            "tool_class": "read_tool",
            "execution_route": "substrate",
            "risk": "high",
            "parse_status": "unsupported_execution_route",
            "arguments": {},
        }], native_mode=True)

        assert routed["invalid_tool_requests"] == [{
            "tool_id": "context_assemble",
            "tool_class": "read_tool",
            "risk": "high",
            "reason": "unsupported_execution_route",
            "source": "provider_tool_call",
            "call_id": "call_005",
            "provider": "openai_responses",
            "response_id": "resp_005",
            "provider_item_id": "fc_005",
            "index": 0,
        }]

    def test_native_invalid_envelope_does_not_fall_back_to_text_tool_request(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        parsed = {
            "general_tool_requests": [{"tool_id": "file_read", "path": "README.md"}],
            "protocol_tool_requests": [],
            "invalid_tool_requests": [],
        }

        routed = apply_native_tool_calls_to_parsed_reaction(parsed, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_invalid_only",
            "call_id": "call_invalid_only",
            "provider_item_id": "fc_invalid_only",
            "index": 0,
            "tool_id": "not_a_tool",
            "tool_class": "",
            "risk": "",
            "parse_status": "unknown_tool_id",
            "arguments": {},
        }], native_mode=True)

        assert routed["general_tool_requests"] == []
        assert [item["reason"] for item in routed["invalid_tool_requests"]] == [
            "native_tool_call_required",
            "unknown_tool_id",
        ]

    def test_native_arguments_missing_required_rejected_before_routing(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_missing_arg",
            "call_id": "call_missing_arg",
            "provider_item_id": "fc_missing_arg",
            "index": 0,
            "tool_id": "file_read",
            "tool_class": "read_tool",
            "risk": "medium",
            "parse_status": "ok",
            "arguments": {"reason": "missing path"},
        }], native_mode=True)

        assert routed["general_tool_requests"] == []
        assert routed["protocol_tool_requests"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_missing_required"
        assert invalid["field"] == "path"
        assert invalid["expected"] == "required"
        assert invalid["actual"] == "missing"

    def test_native_arguments_unknown_field_rejected_before_routing(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_unknown_arg",
            "call_id": "call_unknown_arg",
            "provider_item_id": "fc_unknown_arg",
            "index": 0,
            "tool_id": "file_read",
            "tool_class": "read_tool",
            "risk": "medium",
            "parse_status": "ok",
            "arguments": {"path": "README.md", "command": "dir"},
        }], native_mode=True)

        assert routed["general_tool_requests"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_unknown_field"
        assert invalid["field"] == "command"

    def test_spec781_retired_focus_mount_area_is_rejected_before_submission(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_bad_enum",
            "call_id": "call_bad_enum",
            "provider_item_id": "fc_bad_enum",
            "index": 0,
            "tool_id": "mount_cancel",
            "tool_class": "sync_tool",
            "execution_route": "internal_processor",
            "risk": "high",
            "parse_status": "ok",
            "arguments": {"mount_area": "focus", "item_id": "PRJ-000001"},
        }], native_mode=True)

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == []
        assert routed["mount_cancel_requests"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_invalid_enum"
        assert invalid["field"] == "mount_area"
        assert invalid["expected"] == ["resident_list", "instant_list"]
        assert invalid["actual"] == "focus"

    def test_native_arguments_invalid_type_rejected_before_protocol_submission(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_bad_type",
            "call_id": "call_bad_type",
            "provider_item_id": "fc_bad_type",
            "index": 0,
            "tool_id": "memory_write",
            "tool_class": "sync_tool",
            "risk": "high",
            "parse_status": "ok",
            "arguments": {
                "title": "Bad type",
                "weight": "5",
                "subject": "Codex",
                "body": "body",
                "candidate_keywords": ["native"],
            },
        }], native_mode=True, active_protocol_tool_guides=["memory_write"])

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == []
        assert routed["memory_write_declarations"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_invalid_type"
        assert invalid["field"] == "weight"
        assert invalid["expected"] == "integer"
        assert invalid["actual"] == "string"

    def test_native_arguments_array_items_rejected_before_protocol_submission(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_bad_array_item",
            "call_id": "call_bad_array_item",
            "provider_item_id": "fc_bad_array_item",
            "index": 0,
            "tool_id": "memory_write",
            "tool_class": "sync_tool",
            "risk": "high",
            "parse_status": "ok",
            "arguments": {
                "title": "Bad keyword",
                "weight": 5,
                "subject": "Codex",
                "body": "body",
                "candidate_keywords": ["native", 123],
            },
        }], native_mode=True, active_protocol_tool_guides=["memory_write"])

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == []
        assert routed["memory_write_declarations"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_invalid_type"
        assert invalid["field"] == "candidate_keywords"
        assert invalid["expected"] == "array<string>"
        assert invalid["actual"] == "array<integer,string>"

    def test_native_memory_keywords_string_keeps_value_preview_for_feedback_only(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        submitted_keywords = "结构法权,复合法权,共格元共格,国家消亡,结构动力学"
        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_bad_keywords",
            "call_id": "call_bad_keywords",
            "provider_item_id": "fc_bad_keywords",
            "index": 0,
            "tool_id": "memory_write",
            "tool_class": "sync_tool",
            "risk": "high",
            "parse_status": "ok",
            "arguments": {
                "title": "关键词格式",
                "weight": 4,
                "subject": "Codex",
                "body": "记录关键词格式错误。",
                "candidate_keywords": submitted_keywords,
            },
        }], native_mode=True, active_protocol_tool_guides=["memory_write"])

        assert routed["protocol_tool_submissions"] == []
        assert routed["memory_write_declarations"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_invalid_type"
        assert invalid["field"] == "candidate_keywords"
        assert invalid["expected"] == "array<string>"
        assert invalid["actual"] == "string"
        assert invalid["actual_value_preview"] == submitted_keywords

    def test_native_arguments_integer_float_rejected_before_general_routing(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_float_integer",
            "call_id": "call_float_integer",
            "provider_item_id": "fc_float_integer",
            "index": 0,
            "tool_id": "file_read",
            "tool_class": "read_tool",
            "risk": "medium",
            "parse_status": "ok",
            "arguments": {"path": "README.md", "line_start": 12.3},
        }], native_mode=True)

        assert routed["general_tool_requests"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_invalid_type"
        assert invalid["field"] == "line_start"
        assert invalid["expected"] == "integer"
        assert invalid["actual"] == "number"

    def test_native_arguments_missing_schema_rejected_before_routing(self, monkeypatch):
        import logic.native_tool_calls as native_tool_calls

        monkeypatch.delitem(native_tool_calls.TOOL_ARGUMENT_SCHEMAS, "file_read")

        routed = native_tool_calls.apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_missing_schema",
            "call_id": "call_missing_schema",
            "provider_item_id": "fc_missing_schema",
            "index": 0,
            "tool_id": "file_read",
            "tool_class": "read_tool",
            "risk": "medium",
            "parse_status": "ok",
            "arguments": {"path": "README.md"},
        }], native_mode=True)

        assert routed["general_tool_requests"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_schema_missing"
        assert invalid["field"] == "tool_id"
        assert invalid["expected"] == "tool_argument_schema"
        assert invalid["actual"] == "file_read"

    def test_native_protocol_guide_request_is_retired_invalid_request(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_guide",
            "call_id": "call_guide",
            "provider_item_id": "fc_guide",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "protocol_tool_guide_request",
            "arguments": {
                "tool_id": "memory_link_update",
                "reason": "需要补挂记忆容器",
                "target_hint": "MEM-1 -> DC-1",
            },
            "arguments_json": "{}",
            "tool_class": "guide_request",
            "risk": "medium",
            "parse_status": "ok",
        }], native_mode=True, active_protocol_tool_guides=["relation_card_write"])

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == []
        assert routed["native_protocol_tool_submissions"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["tool_id"] == "protocol_tool_guide_request"
        assert invalid["reason"] == "protocol_tool_guide_request_retired"
        assert invalid["call_id"] == "call_guide"
        assert "exit_signal" not in routed

    def test_native_memory_write_without_active_guide_becomes_submission_and_declaration(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_write",
            "call_id": "call_write",
            "provider_item_id": "fc_write",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "memory_write",
            "arguments": {
                "title": "Spec205",
                "weight": 4,
                "subject": "Codex",
                "body": "Native write body",
                "candidate_keywords": ["Spec205", "native"],
            },
            "arguments_json": "{}",
            "tool_class": "sync_tool",
            "risk": "high",
            "parse_status": "ok",
        }], native_mode=True)

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == ["memory_write"]
        assert routed["native_protocol_tool_submissions"] == ["memory_write"]
        assert routed["memory_write_declarations"] == [{
            "title": "Spec205",
            "weight": 4,
            "subject": "Codex",
            "body": "Native write body",
            "candidate_keywords": ["Spec205", "native"],
            "call_id": "call_write",
            "provider": "openai_responses",
            "response_id": "resp_write",
            "provider_item_id": "fc_write",
            "index": 0,
        }]
        assert routed["invalid_tool_requests"] == []
        assert routed["exit_signal"] == "waiting_tool"

    def test_native_other_protocol_write_without_active_guide_becomes_submission(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_link",
            "call_id": "call_link",
            "provider_item_id": "fc_link",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "memory_link_update",
            "arguments": {
                "mem_id": "MEM-1",
                "operation": "remove",
                "container_refs": ["DC-1"],
            },
            "arguments_json": "{}",
            "tool_class": "sync_tool",
            "risk": "high",
            "parse_status": "ok",
        }], native_mode=True)

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == ["memory_link_update"]
        assert routed["native_protocol_tool_submissions"] == ["memory_link_update"]
        assert routed["memory_link_update_declarations"] == [{
            "mem_id": "MEM-1",
            "operation": "remove",
            "container_refs": ["DC-1"],
            "call_id": "call_link",
            "provider": "openai_responses",
            "response_id": "resp_link",
            "provider_item_id": "fc_link",
            "index": 0,
        }]
        assert routed["invalid_tool_requests"] == []

    def test_native_memory_write_with_active_guide_becomes_submission_and_declaration(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": "resp_write",
            "call_id": "call_write",
            "provider_item_id": "fc_write",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "memory_write",
            "arguments": {
                "title": "Spec135",
                "weight": 4,
                "subject": "Codex",
                "body": "Native write body",
                "candidate_keywords": ["Spec135", "native"],
            },
            "arguments_json": "{}",
            "tool_class": "sync_tool",
            "risk": "high",
            "parse_status": "ok",
        }], native_mode=True, active_protocol_tool_guides=["memory_write"])

        assert routed["protocol_tool_requests"] == []
        assert routed["protocol_tool_submissions"] == ["memory_write"]
        assert routed["native_protocol_tool_submissions"] == ["memory_write"]
        assert routed["memory_write_declarations"] == [{
            "title": "Spec135",
            "weight": 4,
            "subject": "Codex",
            "body": "Native write body",
            "candidate_keywords": ["Spec135", "native"],
            "call_id": "call_write",
            "provider": "openai_responses",
            "response_id": "resp_write",
            "provider_item_id": "fc_write",
            "index": 0,
        }]
        assert routed["exit_signal"] == "waiting_tool"

    def test_native_relation_card_write_rejects_axis_fields_before_processor(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_relation",
            "call_id": "call_relation",
            "provider_item_id": "fc_relation",
            "index": 0,
            "tool_id": "relation_card_write",
            "tool_class": "sync_tool",
            "risk": "high",
            "parse_status": "ok",
            "arguments": {
                "card_id": "REL-Codex",
                "name": "Codex",
                "action": "update",
                "summary": "Native relation update",
                "trust": 100,
            },
        }], native_mode=True, active_protocol_tool_guides=["relation_card_write"])

        assert routed["protocol_tool_submissions"] == []
        assert routed["relation_card_declarations"] == []
        invalid = routed["invalid_tool_requests"][0]
        assert invalid["reason"] == "native_argument_unknown_field"
        assert invalid["field"] == "trust"

    def test_native_memory_container_create_envelope_becomes_submission_and_declaration(self):
        from logic.native_tool_calls import apply_native_tool_calls_to_parsed_reaction

        routed = apply_native_tool_calls_to_parsed_reaction({}, [{
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "response_id": "resp_focus",
            "call_id": "call_focus",
            "provider_item_id": "fc_focus",
            "index": 0,
            "tool_id": "memory_container_create",
            "tool_class": "sync_tool",
            "execution_route": "internal_processor",
            "risk": "high",
            "parse_status": "ok",
            "arguments": {
                "mem_id": "MEM-243NATIVE",
                "container_type": "PRJ",
                "title": "Native focus",
                "target_file": "plan.md",
                "container_body": "native focus body",
                "current_overview": "{container_id}：native focus",
                "reason": "native focus",
            },
        }], native_mode=True, active_protocol_tool_guides=["memory_container_create"])

        assert routed["protocol_tool_submissions"] == ["memory_container_create"]
        assert routed["native_protocol_tool_submissions"] == ["memory_container_create"]
        assert routed["memory_container_create_declarations"] == [{
            "mem_id": "MEM-243NATIVE",
            "container_type": "PRJ",
            "title": "Native focus",
            "target_file": "plan.md",
            "container_body": "native focus body",
            "current_overview": "{container_id}：native focus",
            "reason": "native focus",
            "call_id": "call_focus",
            "provider": "openai_responses",
            "response_id": "resp_focus",
            "provider_item_id": "fc_focus",
            "index": 0,
        }]
        assert routed["exit_signal"] == "waiting_tool"

class TestNativeToolCallExecutorAndAudit:
    @staticmethod
    def _popup_content(messages):
        for message in reversed(messages):
            content = str(message.get("content") or "")
            if "<!-- POPUP" in content:
                return content
        return ""

    @staticmethod
    def _assert_popup_hides_native_machine_fields(popup):
        for forbidden in (
            "kind:",
            "tier:",
            "tool_id:",
            "call_id:",
            "reason:",
            "field:",
            "expected:",
            "actual:",
            "next_action:",
        ):
            assert forbidden not in popup

    @staticmethod
    def _assert_no_native_replay_payload(payload):
        raw = json.dumps(payload, ensure_ascii=False)
        for forbidden in (
            "native_tool_call_envelopes",
            "native_tool_outputs",
            "function_call_output",
            "tool_call_id",
            "provider_native_tool_result",
        ):
            assert forbidden not in raw

    @staticmethod
    def _assert_no_native_replay_messages(messages):
        raw = json.dumps(messages, ensure_ascii=False)
        for forbidden in (
            "native_tool_call_envelopes",
            "native_tool_outputs",
            "function_call_output",
            "tool_call_id",
            "provider_native_tool_result",
        ):
            assert forbidden not in raw

    @staticmethod
    def _native_protocol_guide_request(tool_id, call_id="call_guide"):
        return {
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": f"resp_{call_id}",
            "call_id": call_id,
            "provider_item_id": f"fc_{call_id}",
            "index": 0,
            "raw_type": "function_call",
            "tool_id": "protocol_tool_guide_request",
            "arguments": {
                "tool_id": tool_id,
                "reason": "request protocol tool guide",
            },
            "arguments_json": "{}",
            "tool_class": "guide_request",
            "risk": "medium",
            "parse_status": "ok",
        }

    def test_api_executor_returns_envelopes_meta_and_reaction_tools(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/responses",
                            "model": "unit-responses",
                        },
                    },
                }

        class FakeConnectivity:
            def log_latency(self, endpoint, status, message=""):
                pass

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=FakeConnectivity())

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {
                "id": "resp_002",
                "output": [{
                    "id": "fc_002",
                    "type": "function_call",
                    "call_id": "call_002",
                    "name": "file_read",
                    "arguments": "{\"path\":\"README.md\"}",
                }],
                "usage": {"input_tokens": 5, "output_tokens": 2},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("reaction", "system", [{"role": "user", "content": "read"}])

        assert "tools" in sent["payload"]
        assert any(tool["name"] == "file_read" for tool in sent["payload"]["tools"])
        assert result["response"] == ""
        assert result["provider_response_meta"] == {
            "provider": "openai_responses",
            "response_id": "resp_002",
            "finish_reason": "",
            "raw_tool_call_count": 1,
        }
        assert result["tool_call_envelopes"][0]["call_id"] == "call_002"
        assert result["tool_call_envelopes"][0]["tool_id"] == "file_read"
        assert result["model"] == "unit-responses"
        assert result["endpoint"] == "primary"

    def test_api_executor_injects_step_terminal_tools_per_step(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/responses",
                            "model": "unit-responses",
                        },
                    },
                }

        sent_payloads = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent_payloads.append(payload)
            return {"output_text": "ok"}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "system", [{"role": "user", "content": "setup"}])
        ex.call("cleanup", "system", [{"role": "user", "content": "cleanup"}])
        ex.call("reaction", "system", [{"role": "user", "content": "reaction"}])

        assert [item["name"] for item in sent_payloads[0]["tools"]] == ["setup_finalize"]
        assert "tool_choice" not in sent_payloads[0]
        assert "parallel_tool_calls" not in sent_payloads[0]
        assert [item["name"] for item in sent_payloads[1]["tools"]] == ["cleanup_finalize"]
        assert "tool_choice" not in sent_payloads[1]
        assert "parallel_tool_calls" not in sent_payloads[1]
        reaction_names = {item["name"] for item in sent_payloads[2]["tools"]}
        assert "reaction_finalize" in reaction_names
        assert "file_read" in reaction_names
        assert "tool_choice" not in sent_payloads[2]

    def test_spec568_retired_guide_keeps_reaction_tool_surface(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://example.invalid/v1/responses",
                            "model": "unit-responses",
                        },
                    },
                }

        sent_payloads = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent_payloads.append(payload)
            return {"output_text": "ok"}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("reaction", "system", [{"role": "user", "content": "loop"}])
        ex.call(
            "reaction",
            "system",
            [{"role": "user", "content": "retired guide"}],
            active_protocol_tool_guides=["legacy-retired-guide"],
        )

        normal_names = {item["name"] for item in sent_payloads[0]["tools"]}
        assert "reaction_finalize" in normal_names
        assert "file_read" in normal_names
        assert "tool_choice" not in sent_payloads[0]

        retired_guide_names = {item["name"] for item in sent_payloads[1]["tools"]}
        assert "reaction_finalize" in retired_guide_names
        assert "file_read" in retired_guide_names
        assert "tool_choice" not in sent_payloads[1]
        assert "parallel_tool_calls" not in sent_payloads[1]

    def test_api_executor_merges_endpoint_extra_body_for_chat_tools(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.deepseek.com/v1/chat/completions",
                            "model": "deepseek-v4-pro",
                            "tool_call_provider": "openai_chat",
                            "extra_body": {
                                "thinking": {"type": "disabled"},
                                "reasoning_effort": "high",
                            },
                        },
                    },
                }

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {
                "choices": [{
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call_setup",
                            "type": "function",
                            "function": {
                                "name": "setup_finalize",
                                "arguments": "{}",
                            },
                        }],
                    },
                }],
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "system", [{"role": "user", "content": "setup"}])

        assert sent["payload"]["thinking"] == {"type": "disabled"}
        assert sent["payload"]["reasoning_effort"] == "high"
        assert "tool_choice" not in sent["payload"]
        assert "parallel_tool_calls" not in sent["payload"]

    def test_openai_chat_terminal_empty_output_retries_without_tool_choice(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "agnes-2.0-flash",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                    "handshake": {"retry": 1},
                }

        sent_payloads = []
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())
        ex._sleep = lambda _seconds: None

        def fake_send(url, api_key, payload):
            sent_payloads.append(payload)
            if len(sent_payloads) == 1:
                return {
                    "choices": [{
                        "message": {"content": ""},
                        "finish_reason": "stop",
                    }],
                }
            return {
                "choices": [{
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call_setup",
                            "type": "function",
                            "function": {
                                "name": "setup_finalize",
                                "arguments": "{\"security_verdict\":\"pass\"}",
                            },
                        }],
                    },
                    "finish_reason": "tool_calls",
                }],
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("setup", "system", [{"role": "user", "content": "setup"}])
        second = ex.call(
            "setup",
            "system",
            [{"role": "user", "content": "setup again"}],
        )

        assert len(sent_payloads) == 3
        for payload in sent_payloads:
            assert [item["function"]["name"] for item in payload["tools"]] == [
                "setup_finalize"
            ]
            assert "tool_choice" not in payload
            assert "parallel_tool_calls" not in payload
        assert result["tool_call_envelopes"][0]["tool_id"] == "setup_finalize"
        assert second["tool_call_envelopes"][0]["tool_id"] == "setup_finalize"
        assert result["request_contract_audit"]["tool_mode"] == "required"
        assert second["request_contract_audit"]["tool_mode"] == "required"

    def test_terminal_step_uses_required_mode_without_provider_tool_choice(
            self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "glm-5.1",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                }

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {
                "choices": [{
                    "message": {
                        "content": "ok",
                    },
                }],
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "system", [{"role": "user", "content": "setup"}])

        assert [item["function"]["name"] for item in sent["payload"]["tools"]] == [
            "setup_finalize"
        ]
        assert "tool_choice" not in sent["payload"]
        assert "parallel_tool_calls" not in sent["payload"]

    def test_api_executor_appends_host_query_for_all_system_chat_messages(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/chat/completions",
                            "model": "claude-sonnet-4-6",
                            "tool_call_provider": "openai_chat",
                        },
                    },
                }

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {
                "choices": [{
                    "message": {
                        "content": "",
                        "tool_calls": [{
                            "id": "call_setup",
                            "type": "function",
                            "function": {
                                "name": "setup_finalize",
                                "arguments": "{\"security_verdict\":\"pass\"}",
                            },
                        }],
                    },
                    "finish_reason": "tool_calls",
                }],
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "system", [
            {"role": "system", "content": "permanent"},
            {"role": "system", "content": "popup"},
        ])

        assert [item["role"] for item in sent["payload"]["messages"]] == [
            "system",
            "system",
            "system",
            "user",
        ]
        assert sent["payload"]["messages"][2]["content"] == "popup"
        assert sent["payload"]["messages"][-1]["content"] == (
            "【Host query】请根据以上上下文生成当前阶段所需的自然语言输出。"
        )

    def test_api_executor_builds_anthropic_messages_payload(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/messages",
                            "model": "claude-opus-4-6",
                            "provider": "anthropic_messages",
                            "output_token_limit": 32768,
                        },
                    },
                }

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent["url"] = url
            sent["payload"] = payload
            return {
                "id": "msg_001",
                "stop_reason": "tool_use",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_001",
                    "name": "file_read",
                    "input": {"path": "README.md"},
                }],
                "usage": {"input_tokens": 5, "output_tokens": 2},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("reaction", "system", [{"role": "user", "content": "read"}])

        assert sent["url"] == "https://api.example/v1/messages"
        assert sent["payload"]["system"][0]["text"] == "system"
        assert sent["payload"]["system"][0]["cache_control"] == {"type": "ephemeral"}
        assert sent["payload"]["messages"] == [{"role": "user", "content": "read"}]
        assert sent["payload"]["max_tokens"] == 32768
        assert "temperature" not in sent["payload"]
        file_read = next(tool for tool in sent["payload"]["tools"] if tool["name"] == "file_read")
        assert "input_schema" in file_read
        assert "parameters" not in file_read
        assert result["provider_response_meta"] == {
            "provider": "anthropic_messages",
            "response_id": "msg_001",
            "finish_reason": "tool_use",
            "raw_tool_call_count": 1,
        }
        assert result["tool_call_envelopes"][0]["call_id"] == "toolu_001"
        assert result["tool_call_envelopes"][0]["tool_id"] == "file_read"
        assert result["tool_call_envelopes"][0]["arguments"] == {"path": "README.md"}

    def test_api_executor_omits_anthropic_tool_choice_for_required_mode(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/messages",
                            "model": "claude-sonnet-4-6",
                            "provider": "anthropic_messages",
                        },
                    },
                }

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {
                "id": "msg_setup",
                "stop_reason": "tool_use",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_setup",
                    "name": "setup_finalize",
                    "input": {"security_verdict": "pass"},
                }],
                "usage": {"input_tokens": 5, "output_tokens": 2},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "system", [{"role": "system", "content": "popup"}])

        assert "tool_choice" not in sent["payload"]
        assert "parallel_tool_calls" not in sent["payload"]
        assert [item["name"] for item in sent["payload"]["tools"]] == ["setup_finalize"]
        assert "input_schema" in sent["payload"]["tools"][0]

    def test_api_executor_keeps_late_system_messages_as_anthropic_user_tail(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://api.example/v1/messages",
                            "model": "claude-sonnet-4-6",
                            "provider": "anthropic_messages",
                        },
                    },
                }

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {
                "id": "msg_setup",
                "stop_reason": "tool_use",
                "content": [{
                    "type": "tool_use",
                    "id": "toolu_setup",
                    "name": "setup_finalize",
                    "input": {"security_verdict": "pass"},
                }],
                "usage": {"input_tokens": 5, "output_tokens": 2},
            }

        monkeypatch.setattr(ex, "_send_request", fake_send)

        ex.call("setup", "outer-system", [
            {"role": "system", "content": "permanent"},
            {"role": "user", "content": "request"},
            {"role": "assistant", "content": "previous answer"},
            {"role": "system", "content": "popup guide"},
        ])

        assert [block["text"] for block in sent["payload"]["system"]] == ["outer-system", "permanent"]
        assert sent["payload"]["messages"] == [
            {"role": "user", "content": "request"},
            {"role": "assistant", "content": "previous answer"},
            {"role": "user", "content": "popup guide"},
        ]

    def test_api_executor_uses_explicit_provider_config(self, monkeypatch):
        from engines.executor import APIExecutor

        class FakeConfig(ConfigStoreStub):
            def load(self, name):
                assert name == "api"
                return {
                    "endpoints": {
                        "primary": {
                            "url": "https://relay.invalid/chatish",
                            "model": "unit",
                            "tool_call_provider": "openai_responses",
                        },
                    },
                }

        sent = {}
        ex = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

        def fake_send(url, api_key, payload):
            sent["payload"] = payload
            return {"id": "resp_explicit", "output_text": "ok"}

        monkeypatch.setattr(ex, "_send_request", fake_send)

        result = ex.call("reaction", "system", [{"role": "user", "content": "ping"}])

        assert "input" in sent["payload"]
        assert "messages" not in sent["payload"]
        assert result["provider_response_meta"]["provider"] == "openai_responses"

    def test_spec283_responses_payload_does_not_replay_native_tool_result_items(self):
        from engines.executor import APIExecutor

        ex = APIExecutor()
        payload = ex._build_payload(
            "https://example.invalid/v1/responses",
            "unit-responses",
            "",
            [
                {"role": "user", "content": "before"},
                {
                    "role": "tool",
                    "content": "native result",
                    "native_tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_payload",
                        "call_id": "call_payload",
                        "provider_item_id": "fc_payload",
                        "index": 0,
                        "raw_type": "function_call",
                        "tool_id": "file_read",
                        "arguments_json": "{\"path\":\"example.txt\"}",
                    }],
                    "native_tool_outputs": [{
                        "tool_id": "file_read",
                        "tool_class": "read_tool",
                        "status": "ok",
                        "result_kind": "general_tool_result",
                        "call_id": "call_payload",
                        "content": "native read ok",
                    }],
                },
            ],
            provider="openai_responses",
        )

        assert isinstance(payload["input"], str)
        assert "previous_response_id" not in payload
        assert "user: before" in payload["input"]
        assert "tool: native result" not in payload["input"]
        assert "native result" not in payload["input"]
        assert "system: 【Runtime 工具结果占位】" in payload["input"]
        self._assert_no_native_replay_payload(payload)

    def test_spec283_responses_payload_flattens_post_tool_guide_without_replay(self):
        from engines.executor import APIExecutor

        ex = APIExecutor()
        payload = ex._build_payload(
            "https://example.invalid/v1/responses",
            "unit-responses",
            "",
            [
                {"role": "user", "content": "before"},
                {
                    "role": "tool",
                    "content": "native result",
                    "native_tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "resp_payload_tail",
                        "call_id": "call_payload_tail",
                        "provider_item_id": "fc_payload_tail",
                        "index": 0,
                        "raw_type": "function_call",
                        "tool_id": "file_read",
                        "arguments_json": "{\"path\":\"example.txt\"}",
                    }],
                    "native_tool_outputs": [{
                        "tool_id": "file_read",
                        "status": "ok",
                        "result_kind": "general_tool_result",
                        "call_id": "call_payload_tail",
                        "content": "native read ok",
                    }],
                },
                {"role": "system", "content": "reaction guide"},
            ],
            provider="openai_responses",
        )

        assert "previous_response_id" not in payload
        assert isinstance(payload["input"], str)
        assert "user: before" in payload["input"]
        assert "tool: native result" not in payload["input"]
        assert "native result" not in payload["input"]
        assert "system: 【Runtime 工具结果占位】" in payload["input"]
        assert "system: reaction guide" in payload["input"]
        self._assert_no_native_replay_payload(payload)

    def test_spec283_responses_payload_normalizes_tool_roles_without_native_replay(self):
        from engines.executor import APIExecutor

        ex = APIExecutor()
        payload = ex._build_payload(
            "https://example.invalid/v1/responses",
            "unit-responses",
            "",
            [
                {
                    "role": "tool",
                    "content": "[general_tool_result]\nstatus=rejected",
                },
                {
                    "role": "tool",
                    "content": "native result",
                    "native_tool_call_envelopes": [{
                        "provider": "openai_responses",
                        "response_id": "",
                        "call_id": "call_payload_role",
                        "provider_item_id": "fc_payload_role",
                        "index": 0,
                        "raw_type": "function_call",
                        "tool_id": "file_read",
                        "arguments_json": "{\"path\":\"example.txt\"}",
                    }],
                    "native_tool_outputs": [{
                        "tool_id": "file_read",
                        "status": "ok",
                        "result_kind": "general_tool_result",
                        "call_id": "call_payload_role",
                        "content": "native read ok",
                    }],
                },
            ],
            provider="openai_responses",
        )

        assert isinstance(payload["input"], str)
        assert "tool: [general_tool_result]\nstatus=rejected" not in payload["input"]
        assert "tool: native result" not in payload["input"]
        assert "[general_tool_result]" not in payload["input"]
        assert "native result" not in payload["input"]
        assert "system: 【Runtime 工具结果占位】" in payload["input"]
        self._assert_no_native_replay_payload(payload)

    def test_spec283_chat_payload_does_not_replay_native_tool_result_messages(self):
        from engines.executor import APIExecutor

        ex = APIExecutor()
        payload = ex._build_payload(
            "https://example.invalid/v1/chat/completions",
            "unit-chat",
            "system",
            [
                {"role": "user", "content": "before"},
                {
                    "role": "tool",
                    "content": "native result",
                    "native_tool_call_envelopes": [{
                        "provider": "openai_chat",
                        "response_id": "chat_payload",
                        "call_id": "chat_call_payload",
                        "provider_item_id": "chat_call_payload",
                        "index": 0,
                        "raw_type": "function",
                        "tool_id": "web_search",
                        "arguments_json": "{\"query\":\"UPSP\"}",
                    }],
                    "native_tool_outputs": [{
                        "tool_id": "web_search",
                        "tool_class": "read_tool",
                        "status": "ok",
                        "result_kind": "general_tool_result",
                        "call_id": "chat_call_payload",
                        "content": "native search ok",
                    }],
                },
            ],
            provider="openai_chat",
        )

        assert payload["messages"][0] == {"role": "system", "content": "system"}
        assert payload["messages"][1] == {"role": "user", "content": "before"}
        assert payload["messages"][2] == {
            "role": "system",
            "content": "【Runtime 工具结果占位】工具结果已由 Runtime 结构化回执处理；此处不作为用户输入。",
        }
        self._assert_no_native_replay_payload(payload)

    def test_spec283_chat_payload_strips_injected_native_fields_from_visible_messages(self):
        from engines.executor import APIExecutor

        ex = APIExecutor()
        payload = ex._build_payload(
            "https://example.invalid/v1/chat/completions",
            "unit-chat",
            "",
            [
                {
                    "role": "assistant",
                    "content": "visible assistant text",
                    "tool_call_id": "call_leaked",
                    "function_call_output": {"status": "ok"},
                    "native_tool_outputs": [{"call_id": "call_leaked"}],
                    "provider_native_tool_result": {"call_id": "call_leaked"},
                },
                {
                    "role": "user",
                    "content": "visible user text",
                    "tool_call_id": "call_user_leaked",
                    "function_call_output": "raw replay",
                },
            ],
            provider="openai_chat",
        )

        assert payload["messages"] == [
            {"role": "assistant", "content": "visible assistant text"},
            {"role": "user", "content": "visible user text"},
        ]
        self._assert_no_native_replay_payload(payload)

    def test_chat_payload_normalizes_historical_tool_role_without_tool_call_id(self):
        from engines.executor import APIExecutor

        ex = APIExecutor()
        payload = ex._build_payload(
            "https://example.invalid/v1/chat/completions",
            "unit-chat",
            "",
            [{
                "role": "tool",
                "content": "[general_tool_result]\nstatus=ok",
                "kind": "tool_result",
                "round": 318,
            }],
            provider="openai_chat",
        )

        assert payload["messages"] == [
            {
                "role": "system",
                "content": "【Runtime 工具结果占位】工具结果已由 Runtime 结构化回执处理；此处不作为用户输入。",
            },
            {
                "role": "user",
                "content": "【Host query】请根据以上上下文生成当前阶段所需的自然语言输出。",
            },
        ]

    def test_round_audit_records_native_tool_call_projection(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        store = RoundSnapshotStore(
            context_root=str(tmp_path / "context"),
            static_projection_enabled=False,
        )
        store.start_round(7, round_type="interactive")
        store.record_llm_output(7, "reaction", 1, {
            "response": "",
            "model": "unit",
            "endpoint": "primary",
            "provider_response_meta": {
                "provider": "openai_responses",
                "response_id": "resp_003",
                "finish_reason": "",
                "raw_tool_call_count": 1,
            },
            "tool_call_envelopes": [{
                "schema_version": "tool_call_envelope.v1",
                "source": "provider_tool_call",
                "provider": "openai_responses",
                "endpoint": "primary",
                "response_id": "resp_003",
                "call_id": "call_003",
                "provider_item_id": "fc_003",
                "index": 0,
                "raw_type": "function_call",
                "tool_id": "file_read",
                "arguments": {"path": "README.md"},
                "arguments_json": "{\"path\":\"README.md\"}",
                "tool_class": "read_tool",
                "risk": "medium",
                "parse_status": "ok",
                "requires_guide": False,
                "audit_projection": "原生工具调用：file_read",
            }],
        })

        event = store.read_events(7)[1]
        assert event["event_type"] == "llm_output_raw"
        assert event["payload"]["provider_response_meta"]["response_id"] == "resp_003"
        assert event["payload"]["tool_call_envelopes"][0]["call_id"] == "call_003"

    def test_round_audit_records_request_contract_without_prompt_payload(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        store = RoundSnapshotStore(
            context_root=str(tmp_path / "context"),
            static_projection_enabled=False,
        )
        store.start_round(9, round_type="interactive")
        store.record_llm_output(9, "setup", 1, {
            "response": "",
            "request_contract_audit": {
                "step": "setup",
                "provider": "openai_responses",
                "model": "unit-responses",
                "tool_names": ["setup_finalize"],
                    "terminal_tool": "setup_finalize",
                "tool_mode": "required",
                "tools_transmitted": True,
                "standard_tools_enabled": False,
                "messages": [{"role": "user", "content": "secret prompt"}],
                "api_key": "sk-secret",
            },
        })

        payload = store.read_events(9)[1]["payload"]
        assert payload["request_contract_audit"] == {
            "step": "setup",
            "provider": "openai_responses",
            "model": "unit-responses",
            "tool_names": ["setup_finalize"],
            "terminal_tool": "setup_finalize",
            "tool_mode": "required",
            "tools_transmitted": True,
            "standard_tools_enabled": False,
        }

    def test_round_audit_preserves_prompt_cache_lane_without_prompt_payload(
            self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        store = RoundSnapshotStore(
            context_root=str(tmp_path / "context"),
            static_projection_enabled=False,
        )
        store.start_round(10, round_type="interactive")
        store.record_llm_output(10, "final_reply", 1, {
            "response": "ok",
            "request_contract_audit": {
                "step": "final_reply",
                "provider": "openai_responses",
                "model": "unit-model",
                "tool_names": [],
                    "terminal_tool": None,
                "tool_mode": None,
                "tools_transmitted": False,
                "standard_tools_enabled": False,
                "prompt_cache_lane": "reaction_final_reply_text",
                "prompt_cache_key": "unit-upsp:reaction_final_reply_text",
                "prompt_cache_key_applied": True,
                "prompt_cache_retention": "24h",
                "messages": [{"role": "user", "content": "secret prompt"}],
                "api_key": "sk-secret",
            },
        })

        payload = store.read_events(10)[1]["payload"]
        assert payload["request_contract_audit"] == {
            "step": "final_reply",
            "provider": "openai_responses",
            "model": "unit-model",
            "tool_names": [],
            "terminal_tool": None,
            "tool_mode": None,
            "tools_transmitted": False,
            "standard_tools_enabled": False,
            "prompt_cache_lane": "reaction_final_reply_text",
            "prompt_cache_key": "unit-upsp:reaction_final_reply_text",
            "prompt_cache_key_applied": True,
            "prompt_cache_retention": "24h",
        }

    def test_round_audit_records_prompt_cache_telemetry_from_raw_usage(
            self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        store = RoundSnapshotStore(
            context_root=str(tmp_path / "context"),
            static_projection_enabled=False,
        )
        store.start_round(11, round_type="interactive")
        store.record_llm_output(11, "reaction", 2, {
            "response": "ok",
            "raw_usage": {
                "prompt_tokens": 4096,
                "completion_tokens": 128,
                "prompt_tokens_details": {
                    "cached_tokens": 3072,
                    "audio_tokens": 0,
                },
            },
        })

        payload = store.read_events(11)[1]["payload"]

        assert payload["raw_usage"]["prompt_tokens_details"]["cached_tokens"] == 3072
        assert payload["prompt_cache_telemetry"] == {
            "schema_version": "prompt_cache_telemetry.v2",
            "source": "openai.prompt_tokens_details.cached_tokens",
            "cache_read_source": "openai.prompt_tokens_details.cached_tokens",
            "cache_write_source": "",
            "prompt_tokens": 4096,
            "cached_tokens": 3072,
            "cache_creation_tokens": 0,
            "cache_read_tokens": 3072,
            "cache_write_tokens": 0,
            "cache_write_status": "not_reported",
            "unclassified_prompt_tokens": 1024,
            "cache_hit_ratio": 0.75,
            "cache_write_ratio": 0.0,
            "prompt_cache_mode": "",
            "prompt_cache_lane": "",
            "prompt_cache_key_fingerprint": "",
            "breakpoint_strategy": "",
            "breakpoint_targets": [],
            "prefix_fingerprint": "",
            "lately_epoch": "",
        }

    def test_round_audit_redacts_sensitive_tool_call_arguments(self, tmp_path):
        from data.round_snapshot_store import RoundSnapshotStore

        store = RoundSnapshotStore(
            context_root=str(tmp_path / "context"),
            static_projection_enabled=False,
        )
        store.start_round(8, round_type="interactive")
        store.record_llm_output(8, "reaction", 1, {
            "response": "",
            "tool_call_envelopes": [{
                "tool_id": "web_fetch",
                "arguments": {
                    "url": "https://example.com",
                    "api_key": "sk-secret-value",
                    "nested": {"token": "secret-token"},
                    "command": "echo sk-live-secret-value",
                },
                "arguments_json": "{\"api_key\":\"sk-secret-value\"}",
            }],
        })

        payload = store.read_events(8)[1]["payload"]
        envelope = payload["tool_call_envelopes"][0]
        assert envelope["arguments"]["url"] == "https://example.com"
        assert envelope["arguments"]["api_key"] == "[redacted]"
        assert envelope["arguments"]["nested"]["token"] == "[redacted]"
        assert "sk-live-secret-value" not in envelope["arguments"]["command"]
        assert envelope["arguments_json"] == "[redacted]"

    def test_reaction_loop_consumes_native_general_tool_and_ignores_text_request(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class NativeToolExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "native result observed",
                        "tool_call_envelopes": [],
                    }
                if len(self.calls) == 1:
                    return {
                        "response": """| field | value | note |
|------|----|------|
| exit_signal | done | text should not drive tool execution |
| tool_request | shell_command | command=del x; purpose=bad |
""",
                        "tool_call_envelopes": [{
                            "schema_version": "tool_call_envelope.v1",
                            "source": "provider_tool_call",
                            "provider": "openai_responses",
                            "endpoint": "primary",
                            "response_id": "resp_006",
                            "call_id": "call_006",
                            "provider_item_id": "fc_006",
                            "index": 0,
                            "raw_type": "function_call",
                            "tool_id": "file_read",
                            "arguments": {"path": "example.txt", "reason": "native"},
                            "arguments_json": "{\"path\":\"example.txt\",\"reason\":\"native\"}",
                            "tool_class": "read_tool",
                            "risk": "medium",
                            "parse_status": "ok",
                            "requires_guide": False,
                            "audit_projection": "原生工具调用：file_read",
                        }],
                    }
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        def fake_execute(request):
            return {
                "tool_id": "file_read",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_read_handler",
                "permission_scope": "workspace_read_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "content": "native read ok",
                "protocol_tool_receipt": False,
            }

        rt.executor = NativeToolExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [item["tool_id"] for item in result["_general_tool_requests"]] == ["file_read"]
        assert result["_general_tool_requests"][0]["call_id"] == "call_006"
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert result["_general_tool_results"][0]["call_id"] == "call_006"
        assert result["_general_tool_results"][0]["path"] == "example.txt"
        assert result["_protocol_tool_requests"] == []
        assert result["response"] == "本轮已完成。"

    def test_spec283_reaction_loop_records_general_tool_fact_without_native_replay(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class NativeReplayExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec497 retired active final_reply calls")
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [{
                            "schema_version": "tool_call_envelope.v1",
                            "source": "provider_tool_call",
                            "provider": "openai_responses",
                            "endpoint": "primary",
                            "response_id": "resp_replay",
                            "call_id": "call_replay",
                            "provider_item_id": "fc_replay",
                            "index": 0,
                            "raw_type": "function_call",
                            "tool_id": "file_read",
                            "arguments": {"path": "example.txt", "reason": "native"},
                            "arguments_json": "{\"path\":\"example.txt\",\"reason\":\"native\"}",
                            "tool_class": "read_tool",
                            "risk": "medium",
                            "parse_status": "ok",
                            "requires_guide": False,
                        }],
                    }
                return {"response": "native replay observed"}

        def fake_execute(request):
            return {
                "tool_id": "file_read",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "content": "native read ok",
            }

        rt.executor = NativeReplayExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        native_messages = [
            message for message in rt.executor.calls[1]
            if message.get("native_tool_call_envelopes")
        ]
        assert result["_general_tool_results"][0]["call_id"] == "call_replay"
        assert native_messages == []
        assert "_native_tool_result_projections" not in result
        second_call_messages = rt.executor.calls[1]
        assert sum(
            1
            for message in second_call_messages
            if "native read ok" in str(message.get("content") or "")
        ) == 1
        assert not any(
            message.get("kind") == "tool_fact"
            and "native read ok" in str(message.get("content") or "")
            for message in second_call_messages
        )
        assert any(
            message.get("kind") == "material"
            and "native read ok" in str(message.get("content") or "")
            for message in second_call_messages
        )

    def test_spec561_plain_text_after_tool_finishes_without_closeout_only(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        final_text = "我已经读完材料，可以继续下一步。"

        class ToolThenPlainTextExecutor:
            def __init__(self):
                self.calls = []
                self.guides = []
                self.reaction_calls = 0

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append((_logical_step(step, active_protocol_tool_guides), list(messages)))
                self.guides.append(list(active_protocol_tool_guides or []))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec561 must not call final_reply")
                self.reaction_calls += 1
                assert "__closeout_only__" not in self.guides[-1]
                if self.reaction_calls == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [RuntimeTestMixin()._native_tool_envelope(
                            "file_read",
                            {"path": "book.md", "reason": "read before closeout"},
                            call_id="call_spec252_read",
                        )],
                    }
                return {
                    "response": final_text,
                    "tool_call_envelopes": [],
                }

        def fake_execute(request):
            return {
                "tool_id": "file_read",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "content": "line 1\nline 2",
                "read_mode": "full",
            }

        rt.executor = ToolThenPlainTextExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert [step for step, _ in rt.executor.calls] == [
            "reaction",
            "reaction",
        ]
        assert result["response"] == final_text
        assert result["_exit_signal"] == "done"
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert len(result["_general_tool_results"]) == 1
        assert result["_invalid_tool_requests"] == []
        progress_receipts = [
            receipt for receipt in result["_reaction_loop_guard_receipts"]
            if receipt.get("status") == "reaction_progress_only"
        ]
        assert progress_receipts == []
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert [
            envelope.get("channel")
            for envelope in result["_message_envelopes"]
        ].count("final_reply.text") == 1
        assert result["_settlement_ledgers"][-1]["closeout_decision"] == "finish"
        assert not result["_closeout_relay_receipts"]

    def test_spec283_reaction_loop_retries_native_invalid_tool_call_with_popup_only(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_invalid_replay",
                    "call_id": "call_invalid_replay",
                    "provider_item_id": "fc_invalid_replay",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "not_a_tool",
                    "arguments": {},
                    "arguments_json": "{}",
                    "tool_class": "",
                    "risk": "",
                    "parse_status": "unknown_tool_id",
                }],
            },
            {"response": "invalid replay observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_invalid_tool_requests"][0]["call_id"] == "call_invalid_replay"
        assert result["_invalid_tool_requests"][0]["reason"] == "unknown_tool_id"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_spec283_reaction_loop_retries_native_argument_validation_with_popup_only(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_arg_replay",
                    "call_id": "call_arg_replay",
                    "provider_item_id": "fc_arg_replay",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "file_read",
                    "arguments": {"reason": "missing path"},
                    "arguments_json": "{\"reason\":\"missing path\"}",
                    "tool_class": "read_tool",
                    "risk": "medium",
                    "parse_status": "ok",
                }],
            },
            {"response": "invalid argument observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        popup = self._popup_content(rt.executor.calls[1])
        assert result["_general_tool_requests"] == []
        assert result["_invalid_tool_requests"][0]["call_id"] == "call_arg_replay"
        assert result["_invalid_tool_requests"][0]["reason"] == (
            "native_argument_missing_required")
        assert result["_invalid_tool_requests"][0]["field"] == "path"
        assert result["_invalid_tool_requests"][0]["expected"] == "required"
        assert result["_invalid_tool_requests"][0]["actual"] == "missing"
        self._assert_no_native_replay_messages(rt.executor.calls[1])
        assert "## WARNING｜警告" in popup
        assert "### 原生工具调用警告" in popup
        self._assert_popup_hides_native_machine_fields(popup)
        assert "必须填写该字段" in popup

    def test_memory_write_keywords_string_feedback_shows_chinese_list_example(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        submitted_keywords = "结构法权,复合法权,共格元共格,国家消亡,结构动力学"

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_bad_keywords_feedback",
                    "call_id": "call_bad_keywords_feedback",
                    "provider_item_id": "fc_bad_keywords_feedback",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "memory_write",
                    "arguments": {
                        "title": "关键词格式",
                        "weight": 4,
                        "subject": "Codex",
                        "body": "记录关键词格式错误。",
                        "candidate_keywords": submitted_keywords,
                    },
                    "arguments_json": json.dumps({
                        "title": "关键词格式",
                        "weight": 4,
                        "subject": "Codex",
                        "body": "记录关键词格式错误。",
                        "candidate_keywords": submitted_keywords,
                    }, ensure_ascii=False, sort_keys=True),
                    "tool_class": "sync_tool",
                    "risk": "high",
                    "parse_status": "ok",
                }],
            },
            {"response": "keyword feedback observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        popup = self._popup_content(rt.executor.calls[1])
        invalid = result["_invalid_tool_requests"][0]
        assert invalid["actual_value_preview"] == submitted_keywords
        assert "## WARNING｜警告" in popup
        assert "### 原生工具调用警告" in popup
        self._assert_popup_hides_native_machine_fields(popup)
        assert "`candidate_keywords` 必须填写为关键词列表" in popup
        assert "字符串数组" in popup
        assert "不能写成逗号或顿号分隔的单个字符串" in popup
        assert (
            'candidate_keywords=["结构法权", "复合法权", "共格元共格", '
            '"国家消亡", "结构动力学"]'
        ) in popup
        assert "改成期望类型" not in popup
        assert "主体状态" not in popup

    def test_spec283_reaction_loop_keeps_mixed_native_results_as_facts_only(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        def fake_execute(request):
            return {
                "tool_id": "file_read",
                "tool_class": "read_tool",
                "status": "ok",
                "source": "general_tool_call",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "content": "native read ok",
            }

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_mixed",
                    "call_id": "call_ok",
                    "provider_item_id": "fc_ok",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "file_read",
                    "arguments": {"path": "example.txt", "reason": "native"},
                    "tool_class": "read_tool",
                    "risk": "medium",
                    "parse_status": "ok",
                }, {
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_mixed",
                    "call_id": "call_bad",
                    "provider_item_id": "fc_bad",
                    "index": 1,
                    "raw_type": "function_call",
                    "tool_id": "not_a_tool",
                    "arguments": {},
                    "tool_class": "",
                    "risk": "",
                    "parse_status": "unknown_tool_id",
                }],
            },
            {"response": "mixed native outputs observed"},
        )
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_read guide" if tool_id == "file_read" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_general_tool_results"][0]["call_id"] == "call_ok"
        assert result["_invalid_tool_requests"][0]["call_id"] == "call_bad"
        assert result["_general_tool_results"][0]["status"] == "ok"
        assert result["_general_tool_results"][0]["tool_id"] == "file_read"
        assert result["_invalid_tool_requests"][0]["reason"] == "unknown_tool_id"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_spec283_reaction_loop_has_no_native_invalid_projection_helper(self):
        from engines.reaction_loop import ReactionLoopRunner

        assert not hasattr(ReactionLoopRunner, "_native_invalid_tool_outputs")

    def test_spec253_final_reply_phase_exports_no_native_tools(self):
        from engines.executor import APIExecutor

        executor = APIExecutor()

        assert executor._native_tools_for_step(
            "final_reply",
            "openai_responses",
            active_protocol_tool_guides=["memory_write"],
        ) == []
        audit = executor._request_contract_audit(
            "final_reply",
            "openai_responses",
            "unit-test-model",
            [],
            {},
            None,
        )
        assert audit["tool_names"] == []
        assert audit["standard_tools_enabled"] is False

    def test_spec497_reaction_finalize_form_projects_same_response_text(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class FormThenFinalReplyExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append({
                    "step": _logical_step(step, active_protocol_tool_guides),
                    "messages": list(messages),
                    "active_protocol_tool_guides": active_protocol_tool_guides,
                })
                if len(self.calls) == 1:
                    assert step == "reaction"
                    return {
                        "response": "这是表单通过后的同响应自然语言回复。",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("Spec497 retired active final_reply calls")

        rt.executor = FormThenFinalReplyExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["response"] == "这是表单通过后的同响应自然语言回复。"
        assert [call["step"] for call in rt.executor.calls] == [
            "reaction",
        ]
        assert result["_reaction_finalize_validated"] is True
        assert result["_final_reply_done"] is True
        assert result["_final_response_source"] == "reaction.natural_final_reply"

    def test_spec254_corrected_bad_closeout_finalize_keeps_audit_ok(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        helper = RuntimeTestMixin()
        rt = helper._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class BadThenGoodCloseoutExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(_logical_step(step, active_protocol_tool_guides))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec497 retired active final_reply calls")
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "reaction_finalize",
                            {},
                            call_id="call_bad_closeout",
                            tool_family="substrate_tool",
                            tool_class="sync_tool",
                            risk="high",
                            parse_status="invalid_json",
                        )],
                    }
                return {
                    "response": "本轮已完成。",
                    "tool_call_envelopes": [],
                }

        rt.executor = BadThenGoodCloseoutExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["response"] == "本轮已完成。"
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert result["_tool_transaction_audit"]["issues"] == []
        assert result["_tool_transaction_audit"]["counts"]["corrected_invalid_requests"] == 1
        assert result["_tool_transaction_audit"]["corrected_invalid_requests"][0][
            "call_id"
        ] == "call_bad_closeout"

    def test_spec497_active_final_reply_tool_call_path_is_retired(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        helper = RuntimeTestMixin()

        class ToolCallingFinalReplyExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(_logical_step(step, active_protocol_tool_guides))
                if step == "reaction":
                    return {
                        "response": "本轮已完成。",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("Spec497 retired active final_reply calls")

        rt.executor = ToolCallingFinalReplyExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["response"] == "本轮已完成。"
        assert result["_exit_signal"] == "done"
        assert result["_final_reply_done"] is True
        assert not result["_invalid_tool_requests"]
        assert rt.executor.calls == ["reaction"]

    def test_spec561_reaction_loop_final_response_uses_natural_text(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class NativeFinalizeExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec497 retired active final_reply calls")
                if len(self.calls) == 1:
                    return {
                        "response": "native final response",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("natural final reply should close reaction")

        rt.executor = NativeFinalizeExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["response"] == "native final response"
        assert len(rt.executor.calls) == 1

    def test_spec561_reaction_loop_single_natural_reply_closes_directly(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class NativeSingleFinalizeExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec497 retired active final_reply calls")
                if len(self.calls) > 1:
                    raise AssertionError("single natural reply must close reaction")
                return {
                    "response": "native direct final response",
                    "tool_call_envelopes": [],
                }

        rt.executor = NativeSingleFinalizeExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["response"] == "native direct final response"
        assert "_reaction_internal_handoff" not in result
        assert len(rt.executor.calls) == 1

    def test_spec561_natural_reply_without_model_settlements_closes(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class MinimalFinalizeExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec497 retired active final_reply calls")
                return {
                    "response": "native final after reason",
                    "tool_call_envelopes": [],
                }

        rt.executor = MinimalFinalizeExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["response"] == "native final after reason"
        assert len(rt.executor.calls) == 1
        assert result["_tool_transaction_audit"]["corrected_invalid_requests"] == []

    def test_spec202_memory_route_pending_blocks_native_finalize_until_resolution(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        RuntimeTestMixin()._patch_memory_immediate_stores(monkeypatch, runtime=rt)

        class MemoryRoutePendingExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "native final after route skip",
                        "tool_call_envelopes": [],
                    }
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [{
                            "schema_version": "tool_call_envelope.v1",
                            "source": "provider_tool_call",
                            "provider": "openai_responses",
                            "endpoint": "primary",
                            "response_id": "resp_spec202_route_write",
                            "call_id": "call_spec202_route_write",
                            "provider_item_id": "fc_spec202_route_write",
                            "index": 0,
                            "raw_type": "function_call",
                            "tool_id": "memory_write",
                            "arguments": {
                                "title": "Spec202 route",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "Route pending body",
                                "candidate_keywords": ["Spec202", "route"],
                            },
                            "tool_class": "sync_tool",
                            "risk": "high",
                            "parse_status": "ok",
                        }],
                    }
                if len(self.calls) == 2:
                    return {
                        "response": "native final after route skip",
                        "tool_call_envelopes": [],
                    }
                raise AssertionError("skippable memory route pending should close naturally")

        rt.executor = MemoryRoutePendingExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=RuntimeTestMixin()._confirmed_meta(),
        )
        assert len(rt.executor.calls) > 1, result
        second_call_text = "\n".join(
            message.get("content", "") for message in rt.executor.calls[1])

        assert result["response"] == "native final after route skip"
        assert "记忆条目" in second_call_text
        assert "MEM-131000AA" in second_call_text
        assert result["_settlement_ledgers"][0]["pending_resolution_result"] == "open"
        assert len(rt.executor.calls) == 2

    def test_spec781_reaction_loop_popups_retired_focus_mount_area_feedback(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_enum_feedback",
                    "call_id": "call_enum_feedback",
                    "provider_item_id": "fc_enum_feedback",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "mount_cancel",
                    "arguments": {
                        "mount_area": "focus",
                        "item_id": "PRJ-000001",
                    },
                    "arguments_json": "{\"mount_area\":\"focus\",\"item_id\":\"PRJ-000001\"}",
                    "tool_class": "sync_tool",
                    "execution_route": "internal_processor",
                    "risk": "high",
                    "parse_status": "ok",
                }],
            },
            {"response": "enum feedback observed"},
        )

        rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        popup = self._popup_content(rt.executor.calls[1])
        assert "## WARNING｜警告" in popup
        assert "### 原生工具调用警告" in popup
        self._assert_popup_hides_native_machine_fields(popup)
        assert "只能使用允许枚举值" in popup

    def test_spec756_reaction_loop_executes_shell_without_keyword_warning(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_shell_feedback",
                    "call_id": "call_shell_feedback",
                    "provider_item_id": "fc_shell_feedback",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "shell_command",
                    "arguments": {
                        "command": "echo shell ok",
                        "purpose": "exercise restored shell",
                        "cwd": ".",
                    },
                    "arguments_json": "{\"command\":\"echo shell ok\"}",
                    "tool_class": "action_tool",
                    "execution_route": "host_dispatch",
                    "risk": "high",
                    "parse_status": "ok",
                }],
            },
            {"response": "capability feedback observed"},
        )

        rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        popup = self._popup_content(rt.executor.calls[1])
        assert "### 原生工具调用警告" not in popup
        replay = json.dumps(rt.executor.calls[1], ensure_ascii=False)
        assert "shell_command" in replay
        assert "shell ok" in replay

    def test_spec149_reaction_loop_replays_native_file_edit_capability_rejection(
            self, tmp_path, monkeypatch):
        from paths import PERSONA_DIR
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_file_edit_feedback",
                    "call_id": "call_file_edit_feedback",
                    "provider_item_id": "fc_file_edit_feedback",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "file_edit",
                    "arguments": {
                        "path": str(Path(PERSONA_DIR) / "STM" / "memory" / "live.md"),
                        "patch": "--- a/live.md\n+++ b/live.md\n@@ -1,1 +1,1 @@\n-a\n+b\n",
                        "purpose": "try live persona edit",
                    },
                    "tool_class": "action_tool",
                    "execution_route": "host_dispatch",
                    "risk": "high",
                    "parse_status": "ok",
                }],
            },
            {"response": "file edit rejection observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        rejected = result["_general_tool_results"][0]
        assert rejected["tool_id"] == "file_edit"
        assert rejected["status"] == "rejected"
        assert rejected["reason"] == "capability_denied"
        assert rejected["call_id"] == "call_file_edit_feedback"
        popup = self._popup_content(rt.executor.calls[1])
        assert "### 原生工具调用警告" in popup
        self._assert_popup_hides_native_machine_fields(popup)
        assert str(PERSONA_DIR) not in popup
        assert "--- a/live.md" not in popup

    def test_spec149_reaction_loop_replays_native_subagent_write_scope_rejection(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "schema_version": "tool_call_envelope.v1",
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "endpoint": "primary",
                    "response_id": "resp_subagent_feedback",
                    "call_id": "call_subagent_feedback",
                    "provider_item_id": "fc_subagent_feedback",
                    "index": 0,
                    "raw_type": "function_call",
                    "tool_id": "subagent_dispatch",
                    "arguments": {
                        "task_goal": "edit docs",
                        "allowed_paths": ["OS/tests"],
                        "expected_artifacts": "diff",
                        "task_mode": "code_change",
                    },
                    "tool_class": "action_tool",
                    "execution_route": "host_dispatch",
                    "risk": "high",
                    "parse_status": "ok",
                }],
            },
            {"response": "subagent rejection observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        rejected = result["_general_tool_results"][0]
        assert rejected["tool_id"] == "subagent_dispatch"
        assert rejected["status"] == "rejected"
        assert rejected["reason"] == "write_scope_missing"
        assert rejected["call_id"] == "call_subagent_feedback"
        popup = self._popup_content(rt.executor.calls[1])
        assert "### 原生工具调用警告" in popup
        self._assert_popup_hides_native_machine_fields(popup)
        assert "edit docs" not in popup

    def test_reaction_loop_applies_native_memory_write_with_trace(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        helper = RuntimeTestMixin()
        rt = helper._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        memory_store, _memory_index, _container_store = helper._patch_memory_immediate_stores(
            monkeypatch,
            runtime=rt,
        )

        class NativeMemoryWriteExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec497 retired active final_reply calls")
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [{
                            "source": "provider_tool_call",
                            "provider": "openai_responses",
                            "response_id": "resp_mem",
                            "call_id": "call_mem",
                            "provider_item_id": "fc_mem",
                            "index": 0,
                            "tool_id": "memory_write",
                            "tool_class": "sync_tool",
                            "risk": "high",
                            "parse_status": "ok",
                            "arguments": {
                                "title": "Spec135",
                                "weight": 4,
                                "subject": "Codex",
                                "body": "Native memory body",
                                "candidate_keywords": ["Spec135", "native"],
                            },
                        }],
                    }
                return {
                    "response": "native memory receipt observed",
                    "tool_call_envelopes": [],
                }

        rt.executor = NativeMemoryWriteExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=helper._confirmed_meta(),
        )

        assert result["_protocol_tool_submissions"] == ["memory_write"]
        assert result["_memory_write_declarations"][0]["call_id"] == "call_mem"
        assert result["_memory_write_receipts"][0]["status"] == "applied"
        assert result["_memory_write_receipts"][0]["call_id"] == "call_mem"
        assert not any(
            receipt.get("tool_id") == "memory_write"
            and receipt.get("status") == "guide_loaded"
            for receipt in result["_protocol_tool_receipts"]
        )
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert memory_store.entries[0][0] == "MEM-131000AA"
        assert memory_store.ltm["MEM-131000AA"]["meta"]["tags"] == [
            "Spec135", "native"]
        self._assert_no_native_replay_messages(rt.executor.calls[1])
        assert result["response"] == "native memory receipt observed"

    def test_reaction_loop_applies_native_relay_closeout_with_trace(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        rt.sm.set_flag("continue_requested", False)

        class NativeRelayFinalizeExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    raise AssertionError("Spec497 retired active final_reply calls")
                return {
                    "response": "",
                    "tool_call_envelopes": [{
                        "source": "provider_tool_call",
                        "provider": "openai_responses",
                        "response_id": "resp_relay",
                        "call_id": "call_relay",
                        "provider_item_id": "fc_relay",
                        "index": 0,
                        "tool_id": "reaction_finalize",
                        "arguments": {
                            "handoff_text": (
                                "下一轮起手读取读书上下文，"
                                "下一轮从当前页继续。原因：native relay"
                            ),
                        },
                        "tool_class": "sync_tool",
                        "parse_status": "ok",
                        "index": 0,
                    }],
                }

        rt.executor = NativeRelayFinalizeExecutor()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        flags = rt.sm.get_flags()

        assert result["_protocol_tool_submissions"] == []
        assert result["_closeout_relay_receipts"][0]["status"] == "continue_requested_set"
        assert result["_closeout_relay_receipts"][0]["call_id"] == "call_relay"
        assert result["_closeout_relay_receipts"][0]["provider"] == "openai_responses"
        assert result["_tool_transaction_audit"]["status"] == "ok"
        assert flags["continue_requested"] is True
        assert result["response"] == ""
        assert "_heartbeat_settlement_receipts" not in result

    def test_spec466_reaction_loop_rejects_retired_native_fault_record(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        class CapturingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                    "boundary": kwargs["boundary"],
                }

            def load_cache_compaction_debt(self):
                return {}

        class CapturingAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        class NativeFaultExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if _logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "native fault observed",
                        "tool_call_envelopes": [],
                    }
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [{
                            "source": "provider_tool_call",
                            "provider": "openai_responses",
                            "response_id": "resp_fault",
                            "call_id": "call_fault",
                            "provider_item_id": "fc_fault",
                            "index": 0,
                            "tool_id": "fault_record",
                            "tool_class": "sync_tool",
                            "risk": "medium",
                            "parse_status": "ok",
                            "arguments": {
                                "fault_type": "tool_failure",
                                "severity": "error",
                                "step": "reaction",
                                "source": "web_search",
                                "detail": "外部工具超时",
                                "action": "fallback",
                                "related_tool_id": "web_search",
                            },
                        }],
                    }
                return {
                    "response": "",
                    "tool_call_envelopes": [{
                        "tool_id": "reaction_finalize",
                        "arguments": {
                            "closeout_decision": "finish",
                        },
                        "tool_class": "sync_tool",
                        "parse_status": "ok",
                        "index": 0,
                    }],
                }

        rt.executor = NativeFaultExecutor()
        rt.ctx_store = CapturingContext()
        rt.alert_store = CapturingAlerts()

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_protocol_tool_submissions"] == []
        assert result["_fault_record_receipts"] == []
        assert result["_invalid_tool_requests"][0]["tool_id"] == "fault_record"
        assert result["_invalid_tool_requests"][0]["reason"] == (
            "native_protocol_write_not_enabled"
        )
        assert result["_tool_transaction_audit"]["status"] == "issues_found"
        assert not any(
            item.get("reason") == "missing_processor_receipt"
            for item in result["_tool_transaction_audit"].get("issues", [])
        )
        assert rt.alert_store.entries == []
        assert not any(entry[3] == "fault_note" for entry in rt.ctx_store.entries)
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_reaction_loop_rejects_deferred_native_memory_privacy_mark(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        helper = RuntimeTestMixin()
        rt = helper._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_privacy",
                    "call_id": "call_privacy",
                    "provider_item_id": "fc_privacy",
                    "index": 0,
                    "tool_id": "memory_privacy_mark",
                    "tool_class": "sync_tool",
                    "risk": "high",
                    "parse_status": "ok",
                    "arguments": {
                        "mem_id": "MEM-NOT-FOUND",
                        "privacy_subject": "Codex",
                        "body_action": "move_private",
                        "reason": "native privacy",
                    },
                }],
            },
            {"response": "native privacy observed"},
        )

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=helper._confirmed_meta(),
        )

        assert result["_memory_privacy_receipts"] == []
        assert result["_invalid_tool_requests"][0]["call_id"] == "call_privacy"
        assert result["_invalid_tool_requests"][0]["reason"] == "feature_deferred"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_reaction_loop_replays_retired_native_memory_annotation_as_invalid_output(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_annotation",
                    "call_id": "call_annotation",
                    "provider_item_id": "fc_annotation",
                    "index": 0,
                    "tool_id": "memory_annotation_update",
                    "parse_status": "unknown_tool_id",
                    "arguments": {
                        "mem_id": "MEM-NOT-FOUND",
                        "annotation_kind": "correction",
                        "annotation": "native note",
                        "container_refs": ["DC-1"],
                        "reason": "native annotation",
                    },
                }],
            },
            {"response": "native annotation observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_invalid_tool_requests"][0]["call_id"] == "call_annotation"
        assert result["_invalid_tool_requests"][0]["tool_id"] == "memory_annotation_update"
        assert result["_invalid_tool_requests"][0]["reason"] == "unknown_tool_id"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_spec148_reaction_loop_replays_native_index_view_receipt_as_provider_output(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(
            assembler,
            "build_index_view",
            lambda **kwargs: {
                "tool_id": "index_view",
                "tool_class": "read_tool",
                "status": "accepted",
                "source": "protocol_tool_request",
                "scope": kwargs.get("scope", ""),
                "content": "native index content",
                "protocol_tool_receipt": True,
            },
        )

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_index_read",
                    "call_id": "call_index_read",
                    "provider_item_id": "fc_index_read",
                    "index": 0,
                    "tool_id": "index_view",
                    "tool_class": "read_tool",
                    "risk": "low",
                    "parse_status": "ok",
                    "arguments": {
                        "scope": "ltm_heat",
                        "limit": 2,
                        "reason": "native index read",
                    },
                }],
            },
            {"response": "native index read observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_index_view_receipts"][0]["call_id"] == "call_index_read"
        assert result["_index_view_receipts"][0]["tool_id"] == "index_view"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_spec756_reaction_loop_replays_memory_search_as_material_and_fact(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(
            assembler,
            "build_memory_search",
            lambda **kwargs: {
                "tool_id": "memory_search",
                "tool_class": "read_tool",
                "status": "accepted",
                "source": "protocol_tool_request",
                "query_terms": kwargs["query_terms"],
                "offset": 0,
                "limit": 8,
                "total_matches": 1,
                "content": "MEM-00112233 定位片段：figurines were bought",
                "locator_only": True,
                "protocol_tool_receipt": True,
            },
        )
        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_memory_search",
                    "call_id": "call_memory_search",
                    "provider_item_id": "fc_memory_search",
                    "index": 0,
                    "tool_id": "memory_search",
                    "tool_class": "read_tool",
                    "risk": "low",
                    "parse_status": "ok",
                    "arguments": {"query_terms": ["figurines"]},
                }],
            },
            {"response": "memory search observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        receipt = result["_memory_search_receipts"][0]
        assert receipt["call_id"] == "call_memory_search"
        assert receipt["locator_only"] is True
        replay = json.dumps(rt.executor.calls[1], ensure_ascii=False)
        assert "figurines were bought" in replay
        assert "片段不是事实证据" in replay
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_spec148_reaction_loop_replays_native_relation_read_receipt_as_provider_output(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_relation_read",
                    "call_id": "call_relation_read",
                    "provider_item_id": "fc_relation_read",
                    "index": 0,
                    "tool_id": "relation_read",
                    "tool_class": "read_tool",
                    "risk": "low",
                    "parse_status": "ok",
                    "arguments": {
                        "subject": "REL-NOT-FOUND",
                        "summary": "temporary",
                        "body": "none",
                        "reason": "native relation read",
                    },
                }],
            },
            {"response": "native relation read observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_relation_read_receipts"][0]["call_id"] == "call_relation_read"
        assert result["_relation_read_receipts"][0]["tool_id"] == "relation_read"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_reaction_loop_replays_native_memory_content_read_receipt_as_provider_output(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_mem_read",
                    "call_id": "call_mem_read",
                    "provider_item_id": "fc_mem_read",
                    "index": 0,
                    "tool_id": "memory_content_read",
                    "tool_class": "read_tool",
                    "risk": "medium",
                    "parse_status": "ok",
                        "arguments": {
                            "mem_id": "MEM-NOT-FOUND",
                            "reason": "native read",
                        },
                }],
            },
            {"response": "native memory read observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_memory_content_read_receipts"][0]["call_id"] == "call_mem_read"
        assert result["_memory_content_read_receipts"][0]["tool_id"] == "memory_content_read"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_reaction_loop_replays_native_container_read_receipt_as_provider_output(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_container_read",
                    "call_id": "call_container_read",
                    "provider_item_id": "fc_container_read",
                    "index": 0,
                    "tool_id": "container_read",
                    "tool_class": "read_tool",
                    "risk": "medium",
                    "parse_status": "ok",
                        "arguments": {
                            "container_id": "PRJ-NOT-FOUND",
                            "target_file": "notes.md",
                            "reason": "native container read",
                        },
                }],
            },
            {"response": "native container read observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_container_read_receipts"][0]["call_id"] == "call_container_read"
        assert result["_container_read_receipts"][0]["tool_id"] == "container_read"
        self._assert_no_native_replay_messages(rt.executor.calls[1])
        popup = self._popup_content(rt.executor.calls[1])
        assert popup.count("### 原生工具调用警告") == 1
        self._assert_popup_hides_native_machine_fields(popup)
        assert "读取失败事实后再决定" in popup

    def test_reaction_loop_does_not_popup_native_protocol_accepted_receipt(
            self, tmp_path, monkeypatch):
        from data import container_store as cs
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        store = cs.ContainerStore()
        project = store.create_container("PRJ", "Spec 141", target_file="notes.md")
        store.append_container_content(
            project["container_id"],
            "notes.md",
            "验收正文",
            "accepted receipt must not create warning.",
        )

        class NativeContainerReadAcceptedExecutor:
            def __init__(self, container_id):
                self.container_id = container_id
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [{
                            "source": "provider_tool_call",
                            "provider": "openai_responses",
                            "response_id": "resp_container_read_ok",
                            "call_id": "call_container_read_ok",
                            "provider_item_id": "fc_container_read_ok",
                            "index": 0,
                            "tool_id": "container_read",
                            "tool_class": "read_tool",
                            "risk": "medium",
                            "parse_status": "ok",
                                "arguments": {
                                    "container_id": self.container_id,
                                    "target_file": "notes.md",
                                    "reason": "native accepted read",
                                },
                        }],
                    }
                return {"response": "native accepted receipt observed"}

        rt.executor = NativeContainerReadAcceptedExecutor(project["container_id"])

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_container_read_receipts"][0]["status"] == "accepted"
        assert result["_container_read_receipts"][0]["call_id"] == "call_container_read_ok"
        assert result["_container_read_receipts"][0]["tool_id"] == "container_read"
        self._assert_no_native_replay_messages(rt.executor.calls[1])
        popup = self._popup_content(rt.executor.calls[1])
        assert "### 原生工具调用警告" not in popup
        assert "next_action:" not in popup

    def test_reaction_loop_popups_native_protocol_failure(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_container_read_legacy",
                    "call_id": "call_container_read_legacy",
                    "provider_item_id": "fc_container_read_legacy",
                    "index": 0,
                    "tool_id": "container_read",
                    "tool_class": "read_tool",
                    "risk": "medium",
                    "parse_status": "ok",
                    "arguments": {
                        "container_id": "PRJ-NOT-FOUND",
                        "target_file": "notes.md",
                        "reason": "native container read",
                    },
                }],
            },
            {"response": "native feedback observed", "tool_call_envelopes": []},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_container_read_receipts"][0]["reason"] == "container_not_found"
        popup = self._popup_content(rt.executor.calls[1])
        assert popup.count("### 原生工具调用警告") == 1
        self._assert_popup_hides_native_machine_fields(popup)

    def test_reaction_loop_rejects_deferred_native_privacy_declassify(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_declassify",
                    "call_id": "call_declassify",
                    "provider_item_id": "fc_declassify",
                    "index": 0,
                    "tool_id": "memory_privacy_declassify",
                    "tool_class": "sync_tool",
                    "risk": "high",
                    "parse_status": "ok",
                    "arguments": {
                        "mem_id": "MEM-NOT-FOUND",
                        "mode": "redact",
                        "reason": "native declassify",
                    },
                }],
            },
            {"response": "native declassify observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_memory_privacy_declassify_receipts"] == []
        assert result["_invalid_tool_requests"][0]["call_id"] == "call_declassify"
        assert result["_invalid_tool_requests"][0]["reason"] == "feature_deferred"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_reaction_loop_rejects_retired_native_memory_recall_tool(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_recall",
                    "call_id": "call_recall",
                    "provider_item_id": "fc_recall",
                    "index": 0,
                    "tool_id": "memory_recall_complete",
                    "tool_class": "sync_tool",
                    "risk": "medium",
                    "parse_status": "ok",
                    "arguments": {
                        "mem_id": "MEM-NOT-FOUND",
                        "completed_body": "completed body",
                        "reason": "native recall",
                    },
                }],
            },
            {"response": "native recall observed"},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_invalid_tool_requests"][0]["call_id"] == "call_recall"
        assert result["_invalid_tool_requests"][0]["tool_id"] == (
            "memory_recall_complete"
        )
        assert result["_invalid_tool_requests"][0]["reason"] == (
            "unsupported_execution_route"
        )
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_reaction_loop_replays_native_relation_card_receipt_as_provider_output(
            self, tmp_path, monkeypatch):
        from tests.runtime_test_helpers import RuntimeTestMixin

        helper = RuntimeTestMixin()
        rt = helper._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_relation",
                    "call_id": "call_relation",
                    "provider_item_id": "fc_relation",
                    "index": 0,
                    "tool_id": "relation_card_write",
                    "tool_class": "sync_tool",
                    "risk": "high",
                    "parse_status": "ok",
                    "arguments": {
                        "name": "Codex,Other",
                        "note": "native relation",
                        "reason": "native relation write",
                    },
                }],
            },
            {"response": "native relation observed"},
        )

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta=helper._confirmed_meta(),
        )

        assert result["_relation_card_receipts"][0]["call_id"] == "call_relation"
        assert result["_relation_card_receipts"][0]["status"] == "multiple_relation_targets"
        assert result["_relation_card_receipts"][0]["tool_id"] == "relation_card_write"
        self._assert_no_native_replay_messages(rt.executor.calls[1])

    def test_reaction_loop_replays_native_memory_container_create_receipt_as_provider_output(
            self, tmp_path, monkeypatch):
        from data import container_store as cs
        from tests.runtime_test_helpers import RuntimeTestMixin

        rt = RuntimeTestMixin()._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(
            cs,
            "CONTAINER_REGISTRY_JSON",
            str(tmp_path / "container_registry.json"),
        )
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        class DummyMemoryStore:
            def __init__(self):
                self.ltm = {}
                self.stm = {}

            def list_entries(self):
                return ["MEM-243NATIVE"]

            def get_meta(self, mem_id):
                return {"id": mem_id, "access": "public", "subject": "Codex"}

            def read_body_by_id(self, mem_id):
                return {
                    "body": "内容\nNative source",
                    "meta": self.get_meta(mem_id),
                }

            def update_linked_containers(
                    self, mem_id, operation, refs, current_overview=None):
                return {
                    "id": mem_id,
                    "title": "Native source",
                    "linked_containers": list(refs),
                    "current_overview": current_overview,
                }

            def snapshot_ltm_files(self):
                return dict(self.ltm)

            def snapshot_stm_files(self):
                return dict(self.stm)

            def restore_ltm_files(self, snapshot):
                self.ltm = dict(snapshot)

            def restore_stm_files(self, snapshot):
                self.stm = dict(snapshot)

        rt.memory_store = DummyMemoryStore()

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [{
                    "source": "provider_tool_call",
                    "provider": "openai_responses",
                    "response_id": "resp_focus",
                    "call_id": "call_focus",
                    "provider_item_id": "fc_focus",
                    "index": 0,
                    "tool_id": "memory_container_create",
                    "tool_class": "sync_tool",
                    "execution_route": "internal_processor",
                    "risk": "high",
                    "parse_status": "ok",
                    "arguments": {
                        "mem_id": "MEM-243NATIVE",
                        "container_type": "PRJ",
                        "title": "Native focus",
                        "target_file": "plan.md",
                        "container_body": "native focus body",
                        "current_overview": "{container_id}：native focus",
                        "reason": "native focus",
                    },
                }],
            },
            {"response": "native focus observed", "tool_call_envelopes": []},
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])

        assert result["_protocol_tool_submissions"] == ["memory_container_create"]
        assert result["_memory_container_create_receipts"][0]["status"] == "applied", (
            result["_memory_container_create_receipts"][0]
        )
        assert result["_memory_container_create_receipts"][0]["call_id"] == "call_focus"
        assert result["_general_tool_results"] == []
        assert "_native_tool_result_projections" not in result
        assert not any(
            message.get("native_tool_call_envelopes")
            or message.get("native_tool_outputs")
            for message in rt.executor.calls[1]
        )

    # ── Spec649: 五容器自觉运用 ──

    def test_spec649_container_reference_tools_describe_five_container_types(self):
        """引用式容器工具应包含 DC/EC/PRJ/SKL/FUT 五种容器的触发指引。"""
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            active_protocol_tool_guides=[
                "memory_container_create",
                "memory_container_write",
            ],
        )
        by_name = {item["name"]: item for item in tools}
        assert "linked_containers" not in (
            by_name["memory_write"]["parameters"]["properties"]
        )
        create_schema = by_name["memory_container_create"]["parameters"]
        write_schema = by_name["memory_container_write"]["parameters"]
        combined = json.dumps(
            {
                "create": create_schema,
                "write": write_schema,
            },
            ensure_ascii=False,
        )

        assert create_schema["properties"]["container_type"]["enum"] == [
            "DC",
            "EC",
            "PRJ",
            "SKL",
            "FUT",
        ]
        assert "SKL=card.md" in combined
        assert "本 Frame 起点" in combined
        assert "不是复制 MEM" in combined
        assert "memory_container_create" in by_name
        assert "memory_container_write" in by_name

    def test_spec334_read_tool_schemas_separate_general_bounded_from_content_full(self):
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            active_protocol_tool_guides=[
                "memory_content_read",
                "container_read",
                "relation_read",
            ],
        )
        by_name = {item["name"]: item for item in tools}

        file_read_props = by_name["file_read"]["parameters"]["properties"]
        assert "max_chars" not in file_read_props
        assert set(file_read_props) == {"path", "line_start", "encoding", "reason"}
        assert {"cursor", "line_end", "char_start", "char_end"}.isdisjoint(file_read_props)
        assert "bounded" in by_name["file_read"]["description"]
        assert "next_line_start" in by_name["file_read"]["description"]
        assert "cursor" not in by_name["file_read"]["description"]

        web_fetch_props = by_name["web_fetch"]["parameters"]["properties"]
        assert "max_chars" not in web_fetch_props
        assert set(web_fetch_props) == {
            "url", "char_start", "find_text", "source_content_sha256", "reason",
        }
        assert {"backend", "provider", "strategy", "retry"}.isdisjoint(web_fetch_props)
        assert "配置" in by_name["web_fetch"]["description"]
        assert "同时复制" in by_name["web_fetch"]["description"]

        web_search_props = by_name["web_search"]["parameters"]["properties"]
        assert "max_results" not in web_search_props
        assert set(web_search_props) == {"query", "reason"}
        assert {"backend", "provider", "strategy", "retry"}.isdisjoint(web_search_props)
        assert "配置" in by_name["web_search"]["description"]

        for tool_id in ("memory_content_read", "container_read", "relation_read"):
            props = by_name[tool_id]["parameters"]["properties"]
            assert "max_chars" not in props
            assert {"line_start", "line_end", "char_start", "char_end"} <= set(props)

    def test_spec229_memory_settlement_requires_write_first(self):
        """读书收获场景优先 memory_write，结算必须走结构化账本。"""
        from logic.native_tool_calls import export_provider_tool_schemas

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        finalize_props = by_name["reaction_finalize"]["parameters"]["properties"]
        finalize_desc = by_name["reaction_finalize"]["description"]

        assert set(finalize_props) == {"handoff_text"}
        assert "closeout_decision" not in finalize_props
        assert "memory_status" not in finalize_props
        assert "真实回执" in finalize_desc
        assert "settlement_ledger" in finalize_desc

    def test_spec229_pending_reason_requests_container_audit(self):
        """pending 决策不再让模型填字段，仍由 tracker 提示容器决策。"""
        from logic.native_tool_calls import export_provider_tool_schemas
        from logic.reaction_obligations import ReactionObligationTracker

        tools = export_provider_tool_schemas(
            provider="openai_responses",
            include_protocol_writes=True,
            include_step_terminal_tools=["reaction_finalize"],
        )
        by_name = {item["name"]: item for item in tools}
        finalize_props = by_name["reaction_finalize"]["parameters"]["properties"]
        tracker = ReactionObligationTracker()
        tracker._observe_memory_write({
            "tool_id": "memory_write",
            "status": "applied",
            "mem_id": "MEM-TEST",
            "title": "测试",
            "weight": 2,
            "subject": "Codex",
            "reason": "",
        })
        prompt = tracker.render_prompt()

        assert "pending_reason" not in finalize_props
        assert "DC 辩证链" in prompt
        assert "EC 事件链" in prompt
        assert "PRJ 项目" in prompt
        assert "FUT" in prompt
        assert "reaction_finalize 收束" not in prompt

    def test_spec229_obligation_prompts_use_dialectical_chain(self):
        """memory_route_pending 提示语应使用 DDS 标准术语：辩证链。"""
        from logic.reaction_obligations import ReactionObligationTracker

        tracker = ReactionObligationTracker()
        tracker._observe_memory_write({
            "tool_id": "memory_write",
            "status": "applied",
            "mem_id": "MEM-TEST",
            "title": "测试",
            "weight": 2,
            "subject": "Codex",
            "reason": "",
        })
        prompt = tracker.render_prompt()

        assert "辩证链" in prompt
        assert "思辨链" not in prompt
