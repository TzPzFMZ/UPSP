import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub


class NoopConnectivity:
    def log_latency(self, endpoint, status, message=""):
        pass


def _schema_name(item):
    name = str(item.get("name") or "").strip()
    if name:
        return name
    function = item.get("function")
    if isinstance(function, dict):
        return str(function.get("name") or "").strip()
    return ""


def test_spec568_call_channels_physically_retire_closeout_lane():
    from logic.runtime_channels import (
        CALL_CHANNELS,
        REACTION_FINAL_REPLY_TEXT_GUIDE,
        channel_for_step,
    )

    assert set(CALL_CHANNELS) == {
        "setup",
        "reaction.loop",
        "final_reply",
        "cleanup",
    }
    assert "reaction.closeout" not in CALL_CHANNELS
    assert channel_for_step("setup").name == "setup"
    assert channel_for_step("reaction", reaction_loop_phase="loop").name == (
        "reaction.loop"
    )
    assert channel_for_step("reaction", reaction_loop_phase="closeout").name == (
        "reaction.loop"
    )
    assert channel_for_step("reaction", reaction_loop_phase="final_reply").name == (
        "final_reply"
    )
    assert channel_for_step(
        "reaction",
        active_protocol_tool_guides=[REACTION_FINAL_REPLY_TEXT_GUIDE],
    ).name == "final_reply"
    assert CALL_CHANNELS["final_reply"].step == "reaction"
    assert channel_for_step("cleanup").name == "cleanup"


def test_spec380_native_tool_headers_follow_call_channel_table(monkeypatch):
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
    executor = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

    def fake_send(url, api_key, payload):
        sent_payloads.append(payload)
        return {"output_text": "ok"}

    monkeypatch.setattr(executor, "_send_request", fake_send)

    executor.call("setup", "system", [{"role": "user", "content": "setup"}])
    executor.call("reaction", "system", [{"role": "user", "content": "loop"}])
    executor.call(
        "reaction",
        "system",
        [{"role": "user", "content": "old retired guide"}],
    )
    executor.call("final_reply", "system", [{"role": "user", "content": "final"}])
    executor.call("cleanup", "system", [{"role": "user", "content": "cleanup"}])

    assert [_schema_name(item) for item in sent_payloads[0]["tools"]] == [
        "setup_finalize"
    ]
    assert "tool_choice" not in sent_payloads[0]
    assert "parallel_tool_calls" not in sent_payloads[0]

    reaction_names = {_schema_name(item) for item in sent_payloads[1]["tools"]}
    assert "reaction_finalize" in reaction_names
    assert "file_read" in reaction_names
    assert "reaction_progress_emit" not in reaction_names
    assert "tool_choice" not in sent_payloads[1]

    old_guide_names = {_schema_name(item) for item in sent_payloads[2]["tools"]}
    assert "reaction_finalize" in old_guide_names
    assert "file_read" in old_guide_names
    assert "tool_choice" not in sent_payloads[2]
    assert "parallel_tool_calls" not in sent_payloads[2]

    assert sent_payloads[3].get("tools", []) == []
    assert "tool_choice" not in sent_payloads[3]

    assert [_schema_name(item) for item in sent_payloads[4]["tools"]] == [
        "cleanup_finalize"
    ]
    assert "tool_choice" not in sent_payloads[4]
    assert "parallel_tool_calls" not in sent_payloads[4]


def test_spec383_openai_chat_final_reply_payload_has_user_query_anchor(monkeypatch):
    from engines.executor import APIExecutor

    class FakeConfig(ConfigStoreStub):
        def load(self, name):
            assert name == "api"
            return {
                "endpoints": {
                    "primary": {
                        "url": "https://example.invalid/v1/chat/completions",
                        "model": "unit-chat",
                        "provider": "openai_chat",
                    },
                },
            }

    sent_payloads = []
    executor = APIExecutor(FakeConfig(), connectivity_store=NoopConnectivity())

    def fake_send(url, api_key, payload):
        sent_payloads.append(payload)
        return {"choices": [{"message": {"content": "ok"}}]}

    monkeypatch.setattr(executor, "_send_request", fake_send)

    executor.call(
        "final_reply",
        "system",
        [
            {"role": "system", "content": "最终回复指南"},
            {"role": "assistant", "content": "[progress] 已完成节律结算。"},
        ],
    )

    assert any(
        message.get("role") == "user"
        for message in sent_payloads[0]["messages"]
    )


def test_spec381_reaction_loop_text_becomes_progress_envelope():
    from engines.reaction_iteration import parse_reaction_iteration_result

    parsed = parse_reaction_iteration_result(
        {
            "response": "我先把下一段读完，再一起收束。",
            "tool_call_envelopes": [],
        },
        active_protocol_tool_guides=[],
    )

    assert parsed.parsed_reaction["invalid_tool_requests"] == []
    assert parsed.message_envelopes[0]["channel"] == "assistant_text"
    assert parsed.message_envelopes[0]["text"] == "我先把下一段读完，再一起收束。"
    assert parsed.message_envelopes[0]["block_kind"] == "dialogue_progress"
    assert parsed.message_envelopes[0]["final_reply_material"] is False


def test_spec568_retired_guide_text_remains_assistant_text_envelope():
    from engines.reaction_iteration import parse_reaction_iteration_result

    parsed = parse_reaction_iteration_result(
        {
            "response": "我已经完成了，可以结束。",
            "tool_call_envelopes": [],
        },
        active_protocol_tool_guides=["legacy-retired-guide"],
    )

    assert parsed.message_envelopes[0]["channel"] == "assistant_text"
    assert parsed.message_envelopes[0]["text"] == "我已经完成了，可以结束。"
    assert parsed.parsed_reaction["invalid_tool_requests"] == []


def test_spec381_message_channel_definitions_separate_memory_and_tool_facts():
    from logic.runtime_channels import build_message_envelope

    envelope = build_message_envelope(
        "assistant_text",
        text="我先查一下材料。",
        phase="loop",
        round_num=7,
        iteration=2,
    )

    assert envelope["visibility"] == "user_visible"
    assert envelope["block_kind"] == "dialogue_progress"
    assert envelope["context_policy"] == "short_term_dialogue"
    assert envelope["tool_fact_material"] is False
    assert envelope["long_term_memory"] == "memory_tool_only"
