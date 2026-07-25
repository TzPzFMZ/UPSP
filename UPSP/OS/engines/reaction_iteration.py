"""Reaction step iteration projection helpers.

本模块只处理单次反应迭代的输入投影和 LLM 输出解析，不写 cache、
state、round audit 或 persona live 数据。ReactionLoopRunner 仍负责循环
顺序、工具落账和终止条件。
"""
from __future__ import annotations

import re
from dataclasses import dataclass

from logic.mem_id import validate_mem_id
from logic.native_tool_calls import (
    apply_native_tool_calls_to_parsed_reaction,
    terminal_finalize_from_envelopes,
)
from logic.runtime_channels import build_message_envelope
from logic.single_round_probe_policy import (
    reject_reaction_execution_activity,
    single_round_probe_enabled,
)


_ASSISTANT_TEXT_TOOL_PAYLOAD_RE = re.compile(
    r"(?ism)"
    r"<\s*[|｜]\s*DSML\s*[|｜]\s*tool_calls"
    r"|<\s*/?\s*tool_call\b"
    r"|\"tool_calls\"\s*:"
    r"|\"function\"\s*:\s*\{[^{}]*\"arguments\"\s*:"
    r"|^\s*(?:exit_signal|assistant_reply|assistant_progress|"
    r"reaction_loop_done|to_next_reaction_iter|to_next_setup|"
    r"to_next_reaction|tool_request|protocol_tool_submission|"
    r"protocol_tool_request|general_tool_request|relation_card_declaration|"
    r"memory_write_declaration|memory_annotation_declaration|"
    r"memory_recall_complete|memory_link_update|memory_privacy_mark|"
    r"memory_privacy_declassify|fault_record|退出信号|对外回复|轮内回复|"
    r"反应循环结束|工具唤醒入口|工具请求|工具唤醒)\s*[:：]"
    r"|^\s*\|[^\r\n]*(?:exit_signal|assistant_reply|assistant_progress|"
    r"reaction_loop_done|tool_request|protocol_tool_submission|"
    r"protocol_tool_request|general_tool_request|relation_card_declaration|"
    r"memory_write_declaration|fault_record)[^\r\n]*\|\s*$"
)


def assistant_text_has_tool_payload(text):
    """Native 模式只识别疑似文本工具载荷，不尝试解析或执行。"""
    return bool(_ASSISTANT_TEXT_TOOL_PAYLOAD_RE.search(str(text or "")))


@dataclass(frozen=True)
class ReactionIterationParse:
    """一次 reaction LLM 返回被投影后的结构化结果。"""

    response_text: str
    native_tool_call_envelopes: list
    parsed_reaction: dict
    native_mode: bool
    native_terminal_finalize_only: bool
    message_envelopes: list


def collect_mount_preselection(mount_ids, existing_stm_memory_ids):
    """从 setup mount 请求中投影挂载记忆与预选证据。"""
    mounted_mem_ids = []
    preselection_evidence = []
    seen_preselection = set()
    skip_preselection_items = {"", "-", "—", "无", "确认", "放行", "pass", "PASS"}
    existing_stm_memory_ids = set(existing_stm_memory_ids or [])
    for mr in mount_ids or []:
        if not isinstance(mr, dict):
            continue
        mount_type = str(mr.get("type") or "").strip()
        raw_ids = str(mr.get("ids") or "")
        if mr.get("type") == "memory":
            for mid in raw_ids.split(","):
                mid = mid.strip()
                if (
                    mid.startswith("MEM-")
                    and validate_mem_id(mid)
                    and mid in existing_stm_memory_ids
                ):
                    mounted_mem_ids.append(mid)
        for item_id in [
            item.strip()
            for item in re.split(r"[,，、;；\s]+", raw_ids)
            if item.strip()
        ]:
            if item_id in skip_preselection_items:
                continue
            key = (mount_type, item_id)
            if key in seen_preselection:
                continue
            seen_preselection.add(key)
            preselection_evidence.append({
                "item_id": item_id,
                "item_type": mount_type,
                "origin": "setup_preselection",
                "selection_trigger": str(mr.get("source") or "setup_mount"),
                "surface": item_id,
                "reaction_adoption_signals": [],
                "evidence_refs": [f"setup_mount:{item_id}"],
                "privacy_scope": "runtime_visible",
            })
    return mounted_mem_ids, preselection_evidence


def _invalid_request_from_envelope(envelope, reason):
    item = {}
    for key in (
            "tool_id",
            "tool_family",
            "tool_class",
            "risk",
            "source",
            "call_id",
            "provider",
            "response_id",
            "provider_item_id",
            "index"):
        value = envelope.get(key) if isinstance(envelope, dict) else None
        if value not in (None, ""):
            item[key] = value
    item["reason"] = reason
    item.setdefault("status", "rejected")
    return item


def _empty_reaction_output_invalid():
    return {
        "tool_id": "reaction",
        "tool_family": "message_channel",
        "tool_class": "runtime_guard",
        "status": "rejected",
        "source": "empty_provider_response",
        "call_id": "empty_reaction_output",
        "reason": "reaction_empty_output",
    }


def parse_reaction_iteration_result(iter_result, active_protocol_tool_guides):
    """按当前 native/tool-calling 契约解析一次 reaction LLM 结果。"""
    iter_result = iter_result or {}
    response_text = iter_result.get("response", "")
    native_tool_call_envelopes = iter_result.get("tool_call_envelopes", [])
    native_mode = True
    native_terminal_finalize_only = False
    active_guides = {
        str(item or "").strip()
        for item in (active_protocol_tool_guides or [])
    }
    clean_response_text = str(response_text or "").strip()
    message_envelopes = []
    probe_invalids = []
    if single_round_probe_enabled() and native_tool_call_envelopes:
        probe_invalids = [
            _invalid_request_from_envelope(
                envelope,
                "single_round_probe_tool_forbidden",
            )
            for envelope in native_tool_call_envelopes
        ]
        native_tool_call_envelopes = []

    if native_mode:
        terminal_reaction, ordinary_envelopes, terminal_invalids = (
            terminal_finalize_from_envelopes(
                native_tool_call_envelopes,
                "reaction",
            )
        )
        terminal_invalids.extend(probe_invalids)
        if assistant_text_has_tool_payload(clean_response_text):
            terminal_invalids.append({
                "tool_id": "assistant_text",
                "tool_family": "message_channel",
                "tool_class": "runtime_guard",
                "status": "rejected",
                "source": "assistant_text",
                "call_id": "assistant_text_tool_payload",
                "reason": "assistant_text_tool_payload",
            })
        terminal_decision = ""
        if isinstance(terminal_reaction, dict):
            terminal_decision = str(
                terminal_reaction.get("closeout_decision")
                or (terminal_reaction.get("closeout_form") or {}).get(
                    "closeout_decision")
                or ""
            ).strip()
        natural_final_reply_candidate = bool(
            clean_response_text
            and not native_tool_call_envelopes
            and not terminal_invalids
        )
        terminal_text_candidate = (
            terminal_decision in {"finish", "blocked"}
            or natural_final_reply_candidate
        )
        if clean_response_text:
            envelope = build_message_envelope(
                "assistant_text",
                text=clean_response_text,
                phase="loop",
            )
            if terminal_text_candidate:
                envelope["terminal_text_candidate"] = True
                envelope["terminal_decision"] = terminal_decision or "finish"
            message_envelopes.append(envelope)
        native_terminal_finalize_only = (
            terminal_reaction is not None
            and not ordinary_envelopes
            and not terminal_invalids
        )
        if ordinary_envelopes:
            parsed_reaction = apply_native_tool_calls_to_parsed_reaction(
                {},
                ordinary_envelopes,
                native_mode=True,
                active_protocol_tool_guides=active_protocol_tool_guides,
            )
            if terminal_reaction is not None:
                parsed_reaction["mixed_reaction_finalize"] = terminal_reaction
            parsed_reaction.setdefault("invalid_tool_requests", []).extend(
                terminal_invalids)
        elif terminal_reaction is not None:
            parsed_reaction = terminal_reaction
            parsed_reaction.setdefault("invalid_tool_requests", []).extend(
                terminal_invalids)
        else:
            parsed_reaction = apply_native_tool_calls_to_parsed_reaction(
                {},
                [],
                native_mode=True,
                active_protocol_tool_guides=active_protocol_tool_guides,
            )
            parsed_reaction.setdefault("invalid_tool_requests", []).extend(
                terminal_invalids)
            if (
                    not native_tool_call_envelopes
                    and not terminal_invalids
                    and not clean_response_text):
                parsed_reaction.setdefault("invalid_tool_requests", []).append(
                    _empty_reaction_output_invalid())
            if clean_response_text:
                if natural_final_reply_candidate:
                    parsed_reaction["natural_final_reply_candidate"] = clean_response_text
                    parsed_reaction["closeout_form"] = {
                        "closeout_decision": "finish",
                        "handoff_text": "",
                    }
                    parsed_reaction["reaction_loop"] = {
                        "reaction_loop_done": True
                    }
                    parsed_reaction["exit_signal"] = "done"
                else:
                    parsed_reaction["reaction_loop"] = {
                        "reaction_loop_done": False
                    }
                    parsed_reaction["exit_signal"] = "continue_reaction"
    parsed_reaction = reject_reaction_execution_activity(parsed_reaction)
    if (
            native_mode
            and clean_response_text
            and not terminal_text_candidate
            and not assistant_text_has_tool_payload(clean_response_text)):
        existing_progress = str(
            parsed_reaction.get("assistant_progress") or ""
        ).strip()
        parsed_reaction["assistant_progress"] = (
            f"{existing_progress}\n{clean_response_text}"
            if existing_progress and existing_progress != clean_response_text
            else clean_response_text
        )
        parsed_reaction.setdefault("reaction_loop", {"reaction_loop_done": False})
        if not (
                parsed_reaction.get("general_tool_requests")
                or parsed_reaction.get("protocol_tool_requests")
                or parsed_reaction.get("native_protocol_tool_submissions")
                or parsed_reaction.get("invalid_tool_requests")):
            parsed_reaction["exit_signal"] = "continue_reaction"
    parsed_reaction["message_envelopes"] = message_envelopes

    return ReactionIterationParse(
        response_text=response_text,
        native_tool_call_envelopes=native_tool_call_envelopes,
        parsed_reaction=parsed_reaction,
        native_mode=native_mode,
        native_terminal_finalize_only=native_terminal_finalize_only,
        message_envelopes=message_envelopes,
    )
