"""Project round JSONL events into the live ten-layer viewer state."""
from __future__ import annotations

import json
import os
import re
from copy import deepcopy


SCHEMA_VERSION = "round_live_state.v2"

CONTEXT_PANES = (
    ("00_call_header", "00_call_header"),
    ("01_tool_header", "01_tool_header"),
    ("02_generation_config", "02_generation_config"),
    ("10_permanent", "10_permanent"),
    ("20_periodic", "20_periodic"),
    ("30_lately", "30_lately"),
    ("40_high_freq", "40_high_freq"),
    ("50_now", "50_now"),
    ("60_statusbar", "60_statusbar"),
    ("99_popup", "99_popup"),
)

LEGACY_CONTEXT_PANES = (
    ("permanent", "permanent"),
    ("periodic", "periodic"),
    ("lately", "lately"),
    ("high_freq", "high_freq"),
    ("now", "now"),
    ("statusbar", "statusbar"),
    ("popup", "popup"),
)

LEGACY_LAYER_TO_KEY = {
    "permanent": "10_permanent",
    "periodic": "20_periodic",
    "lately": "30_lately",
    "high_freq": "40_high_freq",
    "now": "50_now",
    "statusbar": "60_statusbar",
    "popup": "99_popup",
}

OPEN_CARD_TYPES = {
    "user",
    "assistant-progress",
    "assistant-final",
    "assistant-streaming",
}
COLLAPSIBLE_CARD_TYPES = {
    "context-update",
    "runtime-parse",
    "tool-call",
    "tool-result",
    "settlement",
    "warning-error",
}
DEFAULT_EXPANDED_CARD_TYPES = {"warning-error"}
STREAM_EVENT_TYPES = {
    "llm_stream_first_chunk",
    "llm_stream_delta",
    "llm_stream_done",
    "llm_stream_error",
}

LIFECYCLE_STATES = {
    "round_started": "running",
    "round_close_requested": "close_requested",
    "cleanup_obligation_created": "cleanup_pending",
    "cleanup_obligation_settled": "cleanup_pending",
    "cleanup_obligation_failed": "unsettled",
    "round_settled": "settled",
    "round_closed": "closed",
    "round_unsettled": "unsettled",
}

CALL_CHANNEL_LABELS = {
    "setup": "起手",
    "reaction.loop": "反应循环",
    "final_reply": "最终回复",
    "cleanup": "善后",
}

LAYER_MARKERS = (
    ("permanent", re.compile(r"(?m)^\s*(?:<!--\s*永固层\s*-->|PERMANENT[｜|:])", re.I)),
    ("periodic", re.compile(r"(?m)^\s*(?:<!--\s*定期层\s*-->|PERIODIC[｜|:])", re.I)),
    ("lately", re.compile(r"(?m)^\s*(?:<!--\s*(?:最近缓存|lately).*?-->|LATELY[｜|:])", re.I)),
    ("high_freq", re.compile(r"(?m)^\s*(?:<!--\s*高频层\s*-->|HIGH_FREQ[｜|:])", re.I)),
    ("now", re.compile(r"(?m)^\s*(?:<!--\s*(?:当前缓存|now).*?-->|NOW[｜|:])", re.I)),
    ("statusbar", re.compile(r"(?m)^\s*(?:<!--\s*STATUSBAR.*?-->|STATUSBAR[｜|:])", re.I)),
    ("popup", re.compile(r"(?m)^\s*(?:<!--\s*POPUP.*?-->|POPUP[｜|:])", re.I)),
)

CORPUS_BLOCK_HEADING_RE = re.compile(r"(?m)^\s*(?P<title>【[^】\n]{1,120}】)\s*")
CORPUS_BLOCK_TONES = (
    "blue",
    "green",
    "amber",
    "purple",
    "rose",
    "slate",
)

DSML_STREAM_MARKER_RE = re.compile(
    r"(?is)<[|｜]DSML[|｜]|<\s*/?\s*tool_call\b|<\s*[|｜]/?tool"
)


def build_live_state(events, live_context_root=None, use_live_layers=False):
    """Build a complete live viewer state from round JSONL events."""
    ordered = _ordered_events(events)
    latest_round = _latest_round(ordered)
    frames = _build_call_frames(
        ordered,
        live_context_root=live_context_root,
        use_live_layers=use_live_layers,
    )
    frame_ids = {frame["frame_id"] for frame in frames}
    conversation = []
    stream_states = {}
    user_sources = _canonical_user_sources(ordered)
    for event in ordered:
        frame_id = _frame_id_for_event(event, frame_ids)
        source = user_sources.get(int(event.get("round") or 0))
        if source and int(event.get("event_index") or 0) == source[0]:
            conversation.extend(_user_cards_from_messages(
                event,
                source[1],
                frame_id=frame_id,
            ))
        if event.get("event_type") in STREAM_EVENT_TYPES:
            _accumulate_stream_card(stream_states, conversation, event, frame_id=frame_id)
            continue
        conversation.extend(_cards_for_event(event, frame_id=frame_id))
    conversation = _dedupe_conversation_cards(conversation)
    conversation = _drop_superseded_streaming_cards(conversation)
    lifecycle = _round_lifecycle(ordered)
    _annotate_streaming_cards(conversation, lifecycle)

    latest_frame = frames[-1] if frames else None
    panes = latest_frame["context_panes"] if latest_frame else _default_context_panes()

    return {
        "schema_version": SCHEMA_VERSION,
        "round": latest_round,
        "last_event_index": _last_event_index(ordered),
        "latest_frame_id": latest_frame["frame_id"] if latest_frame else None,
        "call_frames": frames,
        "context_panes": panes,
        "conversation": conversation,
        "statusbar_projection": _statusbar_projection(ordered),
        "round_lifecycle": lifecycle,
        "rendering": {
            "default_mode": "markdown",
            "raw_available": True,
            "markdown_engine": "markdown-it@14.2.0",
            "html_enabled": False,
            "images_enabled": False,
        },
        "input_box": {
            "label": "用户输入",
            "editable": True,
            "send_live_provider": False,
            "placeholder": "第一版仅保留草稿，不发送 live provider。",
        },
        "manifest": deepcopy(latest_frame.get("manifest") or {}) if latest_frame else {},
    }


def _statusbar_projection(events):
    """Pass through the latest structured STATUSBAR without parsing Markdown."""
    for event in reversed(events):
        if event.get("event_type") != "step_input_snapshot":
            continue
        snapshot = (event.get("payload") or {}).get("layers_snapshot")
        if not isinstance(snapshot, dict):
            continue
        for layer in reversed(snapshot.get("layers") or []):
            if not isinstance(layer, dict) or layer.get("layer_key") != "60_statusbar":
                continue
            projection = layer.get("projection")
            if (
                isinstance(projection, dict)
                and projection.get("schema") == "statusbar_snapshot.v1"
            ):
                return deepcopy(projection)
    return None


def _round_lifecycle(events):
    state = "running" if events else None
    settlement_status = ""
    event_indexes = {}
    fatal_reasons = []
    degraded_reasons = []
    for event in events:
        event_type = str(event.get("event_type") or "")
        payload = event.get("payload") or {}
        for key, target in (
            ("fatal_reasons", fatal_reasons),
            ("degraded_reasons", degraded_reasons),
        ):
            for reason in payload.get(key) or []:
                text = str(reason)
                if text and text not in target:
                    target.append(text)
        if event_type not in LIFECYCLE_STATES:
            continue
        state = LIFECYCLE_STATES[event_type]
        event_indexes[event_type] = int(event.get("event_index") or 0)
        if event_type in {"round_settled", "round_unsettled"}:
            settlement_status = str(payload.get("status") or state)
    return {
        "state": state,
        "settlement_status": settlement_status,
        "event_indexes": event_indexes,
        "fatal_reasons": fatal_reasons,
        "degraded_reasons": degraded_reasons,
    }


def _dedupe_conversation_cards(cards):
    """Remove display-only duplicates while preserving the round audit source."""
    confirmed_natural_finals = {
        _normalized_display_text(card.get("content_raw") or card.get("content_md") or "")
        for card in cards or []
        if card.get("type") == "assistant-final"
        and card.get("event_type") == "round_closed"
        and card.get("_final_response_source") == "reaction.natural_final_reply"
    }
    for card in cards or []:
        text_key = _normalized_display_text(
            card.get("content_raw") or card.get("content_md") or ""
        )
        if card.get("_natural_final_reply_candidate") and text_key in confirmed_natural_finals:
            card["type"] = "assistant-final"
            card["title"] = "AI 最终回复"
            card["summary"] = f"AI 最终回复 · #{card.get('event_index') or 0}"
        card.pop("_natural_final_reply_candidate", None)
        card.pop("_final_response_source", None)
    skipped = set()
    seen_user_cards = set()
    parsed_assistant_text = set()
    best_final_by_text = {}
    final_priority = {
        "llm_output_parsed": 0,
        "llm_output_raw": 1,
        "round_closed": 2,
    }
    for index, card in enumerate(cards or []):
        card_type = card.get("type")
        text_key = _normalized_display_text(
            card.get("content_raw") or card.get("content_md") or ""
        )
        if (
            card_type in {"assistant-progress", "assistant-final"}
            and card.get("event_type") == "llm_output_parsed"
        ):
            key = _assistant_text_key(card)
            if key:
                parsed_assistant_text.add(key)
        if card_type != "assistant-final" or not text_key:
            continue
        rank = final_priority.get(card.get("event_type"), 9)
        current = best_final_by_text.get(text_key)
        if current is None or rank < current[0]:
            best_final_by_text[text_key] = (rank, index)
    for index, card in enumerate(cards or []):
        card_type = card.get("type")
        if card_type == "user":
            key = str(card.get("card_id") or "").strip() or _normalized_display_text(
                card.get("content_raw") or card.get("content_md") or "")
            if key and key in seen_user_cards:
                skipped.add(index)
                continue
            if key:
                seen_user_cards.add(key)
        if card_type not in {"assistant-progress", "assistant-final"}:
            continue
        if card_type == "assistant-final":
            text_key = _normalized_display_text(card.get("content_raw") or card.get("content_md") or "")
            best = best_final_by_text.get(text_key)
            if best and index != best[1]:
                skipped.add(index)
                continue
        if (
            card_type == "assistant-progress"
            and card.get("event_type") == "llm_output_raw"
            and _assistant_text_key(card) in parsed_assistant_text
        ):
            skipped.add(index)
    return [card for index, card in enumerate(cards or []) if index not in skipped]


def _drop_superseded_streaming_cards(cards):
    parsed_frames = set()
    streams_by_frame = {}
    for card in cards or []:
        if (
            card.get("event_type") == "llm_output_parsed"
            and card.get("type") in {"assistant-progress", "assistant-final"}
        ):
            parsed_frames.add(card.get("frame_id") or "")
        if card.get("type") == "assistant-streaming":
            frame_key = (
                card.get("frame_id") or "",
                card.get("phase") or "",
                str(card.get("iteration") or ""),
            )
            streams_by_frame.setdefault(frame_key, []).append(card)
    latest_stream_by_frame = {}
    for frame_key, stream_cards in streams_by_frame.items():
        candidate = stream_cards[-1]
        if (
            candidate.get("stream_state") == "interrupted"
            and not str(candidate.get("content_raw") or "")
        ):
            candidate = next(
                (
                    previous
                    for previous in reversed(stream_cards[:-1])
                    if str(previous.get("content_raw") or "")
                ),
                candidate,
            )
        latest_stream_by_frame[frame_key] = candidate
    kept = []
    for card in cards or []:
        if card.get("type") != "assistant-streaming":
            kept.append(card)
            continue
        frame_key = (
            card.get("frame_id") or "",
            card.get("phase") or "",
            str(card.get("iteration") or ""),
        )
        if card.get("frame_id") in parsed_frames:
            continue
        if latest_stream_by_frame.get(frame_key) is not card:
            continue
        kept.append(card)
    return kept


def _annotate_streaming_cards(cards, lifecycle):
    stopped = "user_stopped" in (lifecycle.get("degraded_reasons") or [])
    for card in cards or []:
        if card.get("type") != "assistant-streaming":
            continue
        if stopped and card.get("stream_state") not in {"completed", "interrupted"}:
            card["stream_state"] = "stopped"
            card["summary"] = "AI 输出已停止"


def _accumulate_stream_card(stream_states, conversation, event, *, frame_id=""):
    key = _stream_card_key(event, frame_id)
    state = stream_states.get(key)
    if state is None:
        card = _card(
            event,
            "assistant-streaming",
            "AI 正在输出",
            "正在等待模型输出...",
            "",
            frame_id,
        )
        card["card_id"] = f"stream:{key}"
        card["summary"] = _stream_card_summary(event, event.get("payload") or {})
        card["collapsible"] = False
        card["default_collapsed"] = False
        state = {
            "card": card,
            "content_parts": [],
            "reasoning_parts": [],
            "first_chunk_latency_ms": None,
            "event_ids": [],
        }
        stream_states[key] = state
        conversation.append(card)

    payload = event.get("payload") or {}
    card = state["card"]
    card["stream_id"] = str(payload.get("stream_id") or "")
    card["protocol"] = str(payload.get("protocol") or "")
    card["attempt_status"] = str(payload.get("attempt_status") or "")
    card["stream_state"] = {
        "llm_stream_done": "completed",
        "llm_stream_error": "interrupted",
    }.get(str(event.get("event_type") or ""), "active")
    event_id = str(event.get("event_id") or "").strip()
    if event_id and event_id not in state["event_ids"]:
        state["event_ids"].append(event_id)
    state["card"]["stream_event_ids"] = list(state["event_ids"])
    state["card"]["provenance"] = {
        "source_event_id": state["event_ids"][0] if state["event_ids"] else "",
        "source_event_ids": list(state["event_ids"]),
        "event_type": event.get("event_type") or "",
        "frame_id": frame_id,
        "renderer": "round_live_viewer._accumulate_stream_card",
    }
    if payload.get("first_chunk_latency_ms") is not None:
        state["first_chunk_latency_ms"] = payload.get("first_chunk_latency_ms")
    if payload.get("content_delta"):
        state["content_parts"].append(str(payload.get("content_delta")))
    if payload.get("reasoning_delta"):
        state["reasoning_parts"].append(str(payload.get("reasoning_delta")))

    raw_content = "".join(state["content_parts"])
    content = _visible_stream_content(raw_content)
    reasoning = "".join(state["reasoning_parts"])
    card["event_index"] = int(event.get("event_index") or card.get("event_index") or 0)
    card["event_type"] = event.get("event_type") or ""
    card["phase"] = event.get("phase") or card.get("phase") or ""
    card["iteration"] = event.get("iteration")
    card["recorded_at"] = event.get("recorded_at") or card.get("recorded_at") or ""
    display_payload = dict(payload)
    if (
        display_payload.get("first_chunk_latency_ms") is None
        and state.get("first_chunk_latency_ms") is not None
    ):
        display_payload["first_chunk_latency_ms"] = state.get("first_chunk_latency_ms")
    if _stream_has_hidden_tool_content(raw_content):
        display_payload["hidden_tool_stream"] = True
        display_payload["hidden_tool_stream_chars"] = len(raw_content) - len(content)
    card["summary"] = _stream_card_summary(event, display_payload)
    card["content_md"] = _streaming_card_md(
        content,
        reasoning,
        event.get("event_type") or "",
        display_payload,
    )
    card["content"] = card["content_md"]
    card["content_raw"] = content


def _stream_card_key(event, frame_id):
    payload = event.get("payload") or {}
    stream_id = str(payload.get("stream_id") or "").strip()
    if stream_id:
        return f"{frame_id or ''}|stream:{stream_id}"
    return "|".join([
        str(frame_id or ""),
        str(event.get("phase") or ""),
        str(event.get("iteration") or ""),
    ])


def _stream_card_summary(event, payload):
    label = {
        "llm_stream_first_chunk": "AI 正在输出",
        "llm_stream_delta": "AI 正在输出",
        "llm_stream_done": "AI 输出完成",
        "llm_stream_error": "AI 流式错误",
    }.get(event.get("event_type") or "", "AI 流式输出")
    bits = [label]
    elapsed = _format_ms(payload.get("elapsed_ms"))
    if elapsed:
        bits.append(elapsed)
    if payload.get("tool_call_count"):
        bits.append(f"工具调用 {payload.get('tool_call_count')}")
    return " · ".join(str(bit) for bit in bits if bit not in (None, ""))


def _streaming_card_md(content, reasoning, event_type, payload):
    content = str(content or "")
    hidden_tool_stream = bool(payload.get("hidden_tool_stream"))
    if content.strip():
        body = content
        if hidden_tool_stream:
            body += "\n\n（工具调用参数流已折叠，不在滚动对话窗口展开。）"
    elif hidden_tool_stream:
        body = "正在生成工具调用参数（已折叠显示）。"
    elif int(payload.get("tool_argument_chars") or 0) > 0 or int(payload.get("tool_call_count") or 0) > 0:
        names = payload.get("tool_names") or []
        suffix = "：" + "、".join(f"`{name}`" for name in names) if names else ""
        body = f"正在生成工具调用{suffix}。"
    elif event_type == "llm_stream_error":
        body = "流式输出中断。"
    else:
        body = "正在等待模型输出..."
    status = _stream_status_text(payload, event_type, reasoning_chars=len(str(reasoning or "")))
    if status:
        return f"{body}\n\n_流式状态：{status}_"
    return body


def _stream_status_text(payload, event_type, *, reasoning_chars=0):
    parts = []
    first_chunk = _format_ms(payload.get("first_chunk_latency_ms"))
    elapsed = _format_ms(payload.get("elapsed_ms"))
    if first_chunk:
        parts.append(f"首字 {first_chunk}")
    if elapsed:
        parts.append(f"持续 {elapsed}")
    if reasoning_chars:
        parts.append(f"推理 {reasoning_chars} 字")
    elif payload.get("reasoning_chars"):
        parts.append(f"推理 {payload.get('reasoning_chars')} 字")
    if payload.get("tool_argument_chars"):
        parts.append(f"工具参数 {payload.get('tool_argument_chars')} 字")
    if payload.get("tool_call_count"):
        parts.append(f"工具调用 {payload.get('tool_call_count')}")
    if payload.get("hidden_tool_stream"):
        hidden_chars = int(payload.get("hidden_tool_stream_chars") or 0)
        suffix = f" {hidden_chars} 字" if hidden_chars > 0 else ""
        parts.append(f"工具参数已折叠{suffix}")
    if payload.get("finish_reason"):
        parts.append(f"完成 {payload.get('finish_reason')}")
    if event_type == "llm_stream_error" and payload.get("reason"):
        parts.append(f"错误 {payload.get('reason')}")
    return "；".join(str(part) for part in parts if part)


def _visible_stream_content(content):
    text = str(content or "")
    match = DSML_STREAM_MARKER_RE.search(text)
    if not match:
        return text
    return text[:match.start()].rstrip()


def _stream_has_hidden_tool_content(content):
    return DSML_STREAM_MARKER_RE.search(str(content or "")) is not None


def _assistant_text_key(card):
    text_key = _normalized_display_text(card.get("content_raw") or card.get("content_md") or "")
    if not text_key:
        return ()
    return (
        card.get("phase") or "",
        str(card.get("iteration") or ""),
        text_key,
    )


def _normalized_display_text(value):
    return re.sub(r"\s+", " ", str(value or "").strip())


def events_after(events, after=0, include_state=True, live_context_root=None,
                 use_live_layers=False):
    """Return incremental events and the state after applying all events."""
    after_index = int(after or 0)
    ordered = _ordered_events(events)
    return {
        "schema_version": "round_live_events.v1",
        "after": after_index,
        "last_event_index": _last_event_index(ordered),
        "events": [
            event for event in ordered
            if int(event.get("event_index") or 0) > after_index
        ],
        "state": build_live_state(
            ordered,
            live_context_root=live_context_root,
            use_live_layers=use_live_layers,
        ) if include_state else None,
    }


def _build_call_frames(events, live_context_root=None, use_live_layers=False):
    frames = []
    by_key = {}
    for event in events:
        key = _call_key(event)
        if not key:
            continue
        frame = by_key.get(key)
        if frame is None:
            frame = _new_frame(event)
            by_key[key] = frame
            frames.append(frame)
        _apply_event_to_frame(frame, event)
    if frames and use_live_layers and live_context_root:
        _apply_live_layers_to_frame(frames[-1], live_context_root)
    return [_finalize_frame(frame) for frame in frames]


def _new_frame(event):
    round_num = event.get("round")
    phase = str(event.get("phase") or (event.get("payload") or {}).get("phase") or "").strip()
    iteration = int(event.get("iteration") or (event.get("payload") or {}).get("iteration") or 1)
    call_channel = _call_channel_for_event(event)
    return {
        "frame_id": str(event.get("frame_id") or "").strip()
        or _frame_id(round_num, phase, iteration),
        "round": round_num,
        "phase": phase,
        "iteration": iteration,
        "call_channel": call_channel,
        "label": _frame_label(round_num, call_channel, iteration),
        "event_start_index": int(event.get("event_index") or 0),
        "event_end_index": int(event.get("event_index") or 0),
        "manifest": {},
        "_layers": _empty_layers(),
        "layer_source": "",
        "historical": False,
    }


def _apply_event_to_frame(frame, event):
    event_index = int(event.get("event_index") or 0)
    frame["event_start_index"] = min(frame["event_start_index"], event_index)
    frame["event_end_index"] = max(frame["event_end_index"], event_index)
    event_type = event.get("event_type")
    payload = event.get("payload") or {}
    call_channel = _call_channel_for_event(event)
    if call_channel:
        frame["call_channel"] = call_channel
        frame["label"] = _frame_label(frame.get("round"), call_channel, frame.get("iteration"))
    if event_type == "step_input_snapshot":
        frame["manifest"] = deepcopy(payload.get("manifest") or {})
        snapshot_layers = _layers_from_snapshot(payload.get("layers_snapshot"))
        if snapshot_layers is not None:
            frame["_layers"] = snapshot_layers
            frame["layer_source"] = "layers_snapshot"
            frame["historical"] = False
        else:
            frame["_layers"] = _legacy_layers_from_messages(
                payload.get("messages") or [],
                manifest=frame["manifest"],
            )
            frame["layer_source"] = "legacy_messages_fallback"
            frame["historical"] = True
    if event_type in {"llm_call_started", "llm_output_raw"}:
        tool_md, tool_raw = _format_tool_header_pair(event)
        if tool_md or tool_raw:
            frame["_tool_header_fallback"] = (tool_md, tool_raw)


def _finalize_frame(frame):
    layers = frame.pop("_layers", _empty_layers())
    tool_header_fallback = frame.pop("_tool_header_fallback", None)
    if (
            not layers.get("01_tool_header")
            and isinstance(tool_header_fallback, tuple)
            and tool_header_fallback):
        layers["01_tool_header"] = tool_header_fallback[0]
    frame["context_panes"] = [
        _pane_from_layer_value(pane_id, title, layers.get(pane_id, ""))
        for pane_id, title in CONTEXT_PANES
    ]
    return frame


def _default_context_panes():
    return [_pane(pane_id, title, "") for pane_id, title in CONTEXT_PANES]


def _ordered_events(events):
    return sorted(
        [event for event in events or [] if isinstance(event, dict)],
        key=lambda item: int(item.get("event_index") or 0),
    )


def _latest_round(events):
    for event in reversed(events):
        if event.get("round") is not None:
            return event.get("round")
    return None


def _last_event_index(events):
    if not events:
        return 0
    return max(int(event.get("event_index") or 0) for event in events)


def _call_key(event):
    explicit = str(event.get("frame_id") or "").strip()
    if explicit:
        return explicit
    phase = str(event.get("phase") or "").strip()
    if not phase:
        return ""
    return _frame_id(event.get("round"), phase, int(event.get("iteration") or 1))


def _frame_id(round_num, phase, iteration):
    return f"R{int(round_num or 0):06d}:{phase}:{int(iteration or 1)}"


def _frame_id_for_event(event, known_frame_ids):
    key = str(event.get("frame_id") or "").strip() or _call_key(event)
    return key if key in known_frame_ids else ""


def _call_channel_for_event(event):
    payload = event.get("payload") or {}
    explicit = str(payload.get("call_channel") or "").strip()
    if explicit:
        return explicit
    envelope = _provider_request_envelope_for_event(event)
    call = envelope.get("call") if isinstance(envelope.get("call"), dict) else {}
    explicit = str(call.get("channel") or "").strip()
    if explicit:
        return explicit
    audit = _request_contract_audit_for_event(event)
    if isinstance(audit, dict):
        return _infer_call_channel(audit, event)
    phase = str(event.get("phase") or "").strip()
    if phase == "reaction":
        return "reaction.loop"
    return phase


def _frame_label(round_num, call_channel, iteration):
    label = CALL_CHANNEL_LABELS.get(str(call_channel or ""), str(call_channel or "调用"))
    return f"R{int(round_num or 0)} {label} {int(iteration or 1)}"


def _empty_layers():
    return {pane_id: "" for pane_id, _title in CONTEXT_PANES}


def _layers_from_snapshot(snapshot):
    if not isinstance(snapshot, dict):
        return None
    layers = snapshot.get("layers")
    if not isinstance(layers, list):
        return None
    result = _empty_layers()
    found = False
    for layer in layers:
        if not isinstance(layer, dict):
            continue
        layer_key = str(layer.get("layer_key") or "").strip()
        if layer_key not in result:
            continue
        result[layer_key] = _format_layer_payload_for_pane(layer, layer_key)
        found = True
    return result if found else None


def _legacy_layers_from_messages(messages, manifest=None):
    legacy = _extract_context_layers(messages, manifest=manifest)
    result = _empty_layers()
    for legacy_id, layer_key in LEGACY_LAYER_TO_KEY.items():
        result[layer_key] = legacy.get(legacy_id, "")
    return result


def _apply_live_layers_to_frame(frame, live_context_root):
    context_step = _context_step_for_phase(frame.get("phase"))
    try:
        from data.audit_store import AuditStore
        store = AuditStore(
            setup_dir=os.path.join(live_context_root, "setup"),
            reaction_dir=os.path.join(live_context_root, "reaction"),
            cleanup_dir=os.path.join(live_context_root, "cleanup"),
        )
        layers = store.read_context_layers(context_step)
    except Exception:
        return
    live_layers = _layers_from_snapshot({
        "schema": "context_layers_snapshot.v1",
        "source": f"context/{context_step}/layers",
        "layers": layers,
    })
    if live_layers is None:
        return
    frame["_layers"] = live_layers
    frame["layer_source"] = "live_layers"
    frame["historical"] = False


def _context_step_for_phase(phase):
    normalized = str(phase or "").strip()
    if normalized == "final_reply":
        return "reaction"
    return normalized


def _format_layer_content(content):
    if isinstance(content, str):
        return content
    return json.dumps(content, ensure_ascii=False, sort_keys=True, indent=2)


def _format_layer_payload_for_pane(layer, pane_id=""):
    if not isinstance(layer, dict):
        return ""
    content_markdown = layer.get("content_markdown")
    content = layer.get("content")
    if isinstance(content, list):
        content_md = (
            content_markdown
            if isinstance(content_markdown, str)
            else _format_layer_content(content)
        )
        return {
            "content_md": content_md,
            "content_raw": _format_layer_content(content),
            "content_blocks": _content_blocks_from_layer_entries(pane_id, content),
        }
    if isinstance(content_markdown, str):
        return content_markdown
    return _format_layer_content(content)


def _pane_from_layer_value(pane_id, title, layer_value):
    if isinstance(layer_value, dict):
        return _pane(
            pane_id,
            title,
            layer_value.get("content_md", ""),
            layer_value.get("content_raw"),
            content_blocks=layer_value.get("content_blocks"),
        )
    return _pane(pane_id, title, layer_value)


def _pane(pane_id, title, content_md, content_raw=None, content_blocks=None):
    content_md = str(content_md or "").strip()
    content_raw = str(content_raw if content_raw is not None else content_md).strip()
    if content_blocks is None:
        content_blocks = _content_blocks_for_pane(
            pane_id, title, content_md, content_raw)
    return {
        "id": pane_id,
        "title": title,
        "content": content_md,
        "content_md": content_md,
        "content_raw": content_raw,
        "content_blocks": content_blocks if isinstance(content_blocks, list) else [],
        "chars": len(content_md),
        "raw_chars": len(content_raw),
    }


def _content_blocks_from_layer_entries(pane_id, entries):
    blocks = []
    for entry in entries or []:
        if not isinstance(entry, dict):
            continue
        content = str(entry.get("content") or "").strip()
        if not content:
            continue
        index = len(blocks) + 1
        title, body = _corpus_entry_title_and_body(entry, content)
        blocks.append({
            "block_id": f"{pane_id}:B{index:02d}",
            "index": index,
            "title": title,
            "content_md": body,
            "content_raw": content,
            "chars": len(body),
            "raw_chars": len(content),
            "tone": CORPUS_BLOCK_TONES[(index - 1) % len(CORPUS_BLOCK_TONES)],
            "source_block_id": _entry_source_block_id(entry),
            "provenance": _entry_provenance(entry),
        })
    return blocks


def _entry_source_block_id(entry):
    if not isinstance(entry, dict):
        return ""
    ref = entry.get("ref") if isinstance(entry.get("ref"), dict) else {}
    return str(
        entry.get("source_block_id")
        or entry.get("id")
        or ref.get("source_block_id")
        or ""
    )


def _entry_provenance(entry):
    if not isinstance(entry, dict):
        return {}
    loc = entry.get("loc") if isinstance(entry.get("loc"), dict) else {}
    ref = entry.get("ref") if isinstance(entry.get("ref"), dict) else {}
    return {
        "source_block_id": _entry_source_block_id(entry),
        "kind": str(entry.get("kind") or ""),
        "round": entry.get("round") or loc.get("round"),
        "step": entry.get("step") or loc.get("step"),
        "iter": entry.get("iter") if entry.get("iter") is not None else loc.get("iter"),
        "timestamp": entry.get("timestamp") or loc.get("time") or "",
        "raw_log_key": entry.get("raw_log_key") or ref.get("raw_log_key") or "",
        "renderer": "round_live_viewer._content_blocks_from_layer_entries",
    }


def _corpus_entry_title_and_body(entry, content):
    match = CORPUS_BLOCK_HEADING_RE.match(content)
    if match:
        return (
            _normalize_corpus_block_title(match.group("title")),
            content[match.end():].strip(),
        )
    kind = str((entry or {}).get("kind") or "").strip()
    title_by_kind = {
        "runtime_call_request": "Runtime 调用占位",
        "interaction": "本轮交互",
        "setup_fact": "本轮起手事实",
        "tool_fact": "本轮工具事实",
        "material": "本轮资料",
        "dialogue_progress": "轮中进展记录",
        "assistant_reply": "最终回复记录",
        "reasoning_context": "本轮推理上下文",
    }
    return title_by_kind.get(kind, "内容"), content


def _content_blocks_for_pane(pane_id, title, content_md, content_raw):
    md_parts = _split_corpus_block_parts(content_md, default_title=title)
    raw_parts = _split_corpus_block_parts(content_raw, default_title=title)
    if not md_parts:
        return []
    if len(raw_parts) != len(md_parts):
        raw_parts = md_parts
    blocks = []
    for index, md_part in enumerate(md_parts, start=1):
        raw_part = raw_parts[index - 1]
        block_title = md_part.get("title") or raw_part.get("title") or title
        content_md_part = str(md_part.get("body") or "").strip()
        content_raw_part = str(raw_part.get("body") or content_md_part).strip()
        blocks.append({
            "block_id": f"{pane_id}:B{index:02d}",
            "index": index,
            "title": block_title,
            "content_md": content_md_part,
            "content_raw": content_raw_part,
            "chars": len(content_md_part),
            "raw_chars": len(content_raw_part),
            "tone": CORPUS_BLOCK_TONES[(index - 1) % len(CORPUS_BLOCK_TONES)],
        })
    return blocks


def _split_corpus_block_parts(content, default_title="内容"):
    text = str(content or "").strip()
    if not text:
        return []
    matches = list(CORPUS_BLOCK_HEADING_RE.finditer(text))
    if not matches:
        return []
    parts = []
    if matches[0].start() > 0:
        prefix = text[:matches[0].start()].strip()
        if prefix:
            parts.append({"title": str(default_title or "内容"), "body": prefix})
    for index, match in enumerate(matches):
        next_start = matches[index + 1].start() if index + 1 < len(matches) else len(text)
        body = text[match.end():next_start].strip()
        parts.append({
            "title": _normalize_corpus_block_title(match.group("title")),
            "body": body,
        })
    return parts


def _normalize_corpus_block_title(title):
    normalized = str(title or "").strip()
    if normalized.startswith("【") and normalized.endswith("】"):
        normalized = normalized[1:-1].strip()
    return normalized or "内容"


def _extract_context_layers(messages, manifest=None):
    layers = {pane_id: "" for pane_id, _title in LEGACY_CONTEXT_PANES}
    messages = [message for message in (messages or []) if isinstance(message, dict)]
    marker_indexes = _layer_marker_indexes(messages)
    for index, message in enumerate(messages):
        if not isinstance(message, dict):
            continue
        content = str(message.get("content") or "")
        segments = _split_layer_segments(content)
        if not segments:
            inferred = _infer_unmarked_layer(index, content, marker_indexes, manifest)
            if inferred:
                segments = [(inferred, content)]
        for layer_id, segment in segments:
            if layer_id in layers:
                layers[layer_id] = _append_text(layers[layer_id], segment)
    if not layers.get("now") and _manifest_layer_chars(manifest, "now") > 0:
        layers["now"] = _recover_unmarked_now_layer(messages)
    return layers


def _layer_marker_indexes(messages):
    indexes = {pane_id: [] for pane_id, _title in LEGACY_CONTEXT_PANES}
    for index, message in enumerate(messages or []):
        content = str((message or {}).get("content") or "")
        for layer_id, pattern in LAYER_MARKERS:
            if pattern.search(content):
                indexes.setdefault(layer_id, []).append(index)
    return indexes


def _first_marker_index(marker_indexes, layer_id):
    values = marker_indexes.get(layer_id) or []
    return min(values) if values else None


def _last_marker_before(marker_indexes, layer_ids, index):
    values = [
        value
        for layer_id in layer_ids
        for value in (marker_indexes.get(layer_id) or [])
        if value < index
    ]
    return max(values) if values else None


def _infer_unmarked_layer(index, content, marker_indexes, manifest):
    text = str(content or "").strip()
    if not text:
        return ""
    high_freq_index = _first_marker_index(marker_indexes, "high_freq")
    statusbar_index = _first_marker_index(marker_indexes, "statusbar")
    popup_index = _first_marker_index(marker_indexes, "popup")
    terminal_indexes = [
        value for value in (statusbar_index, popup_index)
        if value is not None
    ]
    terminal_index = min(terminal_indexes) if terminal_indexes else None
    if high_freq_index is not None:
        if (
            index < high_freq_index
            and _manifest_layer_chars(manifest, "lately") > 0
            and _last_marker_before(marker_indexes, ("permanent", "periodic"), index) is not None
        ):
            return "lately"
        if (
            index > high_freq_index
            and (terminal_index is None or index < terminal_index)
            and _manifest_layer_chars(manifest, "now") > 0
        ):
            return "now"
    guessed = _guess_layer_id(text)
    if guessed:
        return guessed
    if _looks_like_lately_text(text) and _manifest_layer_chars(manifest, "lately") > 0:
        return "lately"
    if _looks_like_now_text(text) and _manifest_layer_chars(manifest, "now") > 0:
        return "now"
    return ""


def _looks_like_lately_text(text):
    head = str(text or "")[:160]
    return any(marker in head for marker in (
        "【历史工具事实摘要",
        "【历史交互",
        "【历史回复",
        "【第 ",
    ))


def _looks_like_now_text(text):
    head = str(text or "")[:160]
    return any(marker in head for marker in (
        "【本轮工具事实】",
        "【本轮资料】",
        "【本轮交互】",
        "【轮中进展记录】",
        "【最终回复记录】",
        "【Runtime 调用占位】",
        "**user**:",
        "**assistant**:",
    ))


def _manifest_layer_chars(manifest, layer_id):
    if not isinstance(manifest, dict):
        return 0
    layers = manifest.get("layers")
    if not isinstance(layers, dict):
        return 0
    layer = layers.get(layer_id)
    if not isinstance(layer, dict):
        return 0
    try:
        return int(layer.get("chars") or 0)
    except (TypeError, ValueError):
        return 0


def _recover_unmarked_now_layer(messages):
    recovered = []
    for message in messages or []:
        if not isinstance(message, dict):
            continue
        content = str(message.get("content") or "")
        segment = _extract_unmarked_now_segment(content)
        if segment:
            recovered.append(segment)
    return "\n\n".join(recovered).strip()


def _extract_unmarked_now_segment(content):
    text = str(content or "")
    if not text:
        return ""
    start_match = re.search(
        r"(?m)(?:^---\s*\n\s*)?"
        r"(?=(?:【历史工具事实摘要|【第\s*\d+\s*轮已闭合】|"
        r"\*\*user\*\*:|【本轮交互】|\*\*assistant\*\*:|"
        r"【轮中进展记录】|【最终回复记录】|【Runtime 调用占位】))",
        text,
    )
    if not start_match:
        return ""
    start = start_match.start()
    end = len(text)
    for pattern in (
        re.compile(r"(?m)^\s*(?:<!--\s*STATUSBAR.*?-->|STATUSBAR[｜|:])", re.I),
        re.compile(r"(?m)^\s*(?:<!--\s*POPUP.*?-->|POPUP[｜|:])", re.I),
    ):
        marker = pattern.search(text, start)
        if marker:
            end = min(end, marker.start())
    return text[start:end].strip()


def _split_layer_segments(content):
    matches = []
    for layer_id, pattern in LAYER_MARKERS:
        for match in pattern.finditer(content):
            matches.append((match.start(), match.end(), layer_id))
    matches.sort(key=lambda item: item[0])
    if not matches:
        layer_id = _guess_layer_id(content)
        return [(layer_id, content)] if layer_id else []

    segments = []
    for index, (start, _end, layer_id) in enumerate(matches):
        next_start = matches[index + 1][0] if index + 1 < len(matches) else len(content)
        segment = content[start:next_start].strip()
        if segment:
            segments.append((layer_id, segment))
    return segments


def _guess_layer_id(content):
    first_line = ""
    for line in str(content or "").splitlines():
        stripped = line.strip()
        if stripped:
            first_line = stripped
            break
    head = first_line[:120]
    candidates = (
        ("permanent", ("PERMANENT", "永固层", "RULES")),
        ("periodic", ("PERIODIC", "定期层")),
        ("lately", ("LATELY", "最近缓存")),
        ("high_freq", ("HIGH_FREQ", "高频层")),
        ("now", ("NOW", "当前缓存")),
        ("statusbar", ("STATUSBAR", "状态栏")),
        ("popup", ("POPUP", "弹窗层", "GUIDE｜指南")),
    )
    for layer_id, needles in candidates:
        if any(_line_starts_with_layer_header(head, needle) for needle in needles):
            return layer_id
    return ""


def _line_starts_with_layer_header(line, marker):
    line = str(line or "").strip()
    marker = str(marker or "").strip()
    if not line or not marker:
        return False
    if marker == "GUIDE｜指南":
        return bool(re.match(r"^(?:#+\s*)?GUIDE[｜|:：]指南(?:[｜|:：]|$)", line, re.I))
    if marker == "POPUP":
        return bool(re.match(r"^(?:#+\s*)?POPUP(?:[｜|:：]|$)", line, re.I))
    return bool(
        re.match(
            rf"^(?:#+\s*)?{re.escape(marker)}(?:[｜|:：\s]|$)",
            line,
            re.I,
        )
    )


def _append_text(existing, text):
    text = str(text or "").strip()
    if not text:
        return existing
    if not existing:
        return text
    return f"{existing}\n\n{text}"


def _provider_request_envelope_for_event(event):
    payload = event.get("payload") or {}
    envelope = payload.get("provider_request_envelope")
    return envelope if isinstance(envelope, dict) else {}


def _request_contract_audit_for_event(event):
    envelope = _provider_request_envelope_for_event(event)
    audit = envelope.get("request_contract_audit")
    if isinstance(audit, dict):
        return audit
    payload = event.get("payload") or {}
    audit = payload.get("request_contract_audit")
    return audit if isinstance(audit, dict) else {}


def _format_tool_header_pair(event):
    audit = _request_contract_audit_for_event(event)
    if not isinstance(audit, dict):
        return "", ""
    envelope = _provider_request_envelope_for_event(event)
    raw = _format_tool_header_raw(event, audit, envelope=envelope)
    md = _format_tool_header_md(event, audit, envelope=envelope)
    return md, raw


def _format_tool_header_raw(event, audit, envelope=None):
    envelope = envelope if isinstance(envelope, dict) else {}
    call = envelope.get("call") if isinstance(envelope.get("call"), dict) else {}
    tool_names = audit.get("tool_names") or []
    cache_plan = audit.get("prompt_cache_plan")
    cache_plan = cache_plan if isinstance(cache_plan, dict) else {}
    lines = [
        f"event: {event.get('event_type')}",
        f"step: {call.get('step') or audit.get('step') or event.get('phase') or ''}",
        f"phase: {call.get('phase') or (event.get('payload') or {}).get('phase') or event.get('phase') or ''}",
        f"iteration: {call.get('iteration') or event.get('iteration') or (event.get('payload') or {}).get('iteration') or ''}",
        f"call_channel: {call.get('channel') or (event.get('payload') or {}).get('call_channel') or _infer_call_channel(audit, event)}",
        f"provider: {audit.get('provider') or ''}",
        f"model: {audit.get('model') or ''}",
        f"request_body_sha256: {envelope.get('request_body_sha256') or ''}",
        "tools: " + (", ".join(str(name) for name in tool_names) if tool_names else "(none)"),
        f"terminal_tool: {audit.get('terminal_tool') or ''}",
        f"tool_mode: {audit.get('tool_mode') or ''}",
        f"tools_transmitted: {str(bool(audit.get('tools_transmitted'))).lower()}",
        (
            "prompt_cache: "
            f"profile={audit.get('prompt_cache_profile') or ''}; "
            f"mode={audit.get('prompt_cache_mode') or ''}; "
            f"lane={audit.get('prompt_cache_lane') or ''}; "
            f"key={audit.get('prompt_cache_key') or ''}; "
            f"applied={str(bool(audit.get('prompt_cache_key_applied'))).lower()}; "
            f"retention={audit.get('prompt_cache_retention') or ''}; "
            "breakpoint_targets="
            f"{','.join(cache_plan.get('targets') or [])}"
        ),
    ]
    return "\n".join(lines).strip()


def _format_tool_header_md(event, audit, envelope=None):
    envelope = envelope if isinstance(envelope, dict) else {}
    call = envelope.get("call") if isinstance(envelope.get("call"), dict) else {}
    tool_names = audit.get("tool_names") or []
    cache_plan = audit.get("prompt_cache_plan")
    cache_plan = cache_plan if isinstance(cache_plan, dict) else {}
    rows = [
        ("事件", event.get("event_type")),
        ("步骤", call.get("step") or audit.get("step") or event.get("phase")),
        ("阶段", call.get("phase") or (event.get("payload") or {}).get("phase") or event.get("phase")),
        ("迭代", call.get("iteration") or event.get("iteration") or (event.get("payload") or {}).get("iteration")),
        ("调用通道", call.get("channel") or (event.get("payload") or {}).get("call_channel") or _infer_call_channel(audit, event)),
        ("提供商", audit.get("provider")),
        ("模型", audit.get("model")),
        ("终端工具", audit.get("terminal_tool")),
        ("工具模式", audit.get("tool_mode")),
        ("工具头已下发", audit.get("tools_transmitted")),
        ("提示缓存通道", audit.get("prompt_cache_lane")),
        ("提示缓存 profile", audit.get("prompt_cache_profile")),
        ("提示缓存模式", audit.get("prompt_cache_mode")),
        ("提示缓存键", audit.get("prompt_cache_key")),
        ("提示缓存已应用", audit.get("prompt_cache_key_applied")),
        ("提示缓存保留", audit.get("prompt_cache_retention")),
        (
            "提示缓存断点层",
            ", ".join(cache_plan.get("targets") or []),
        ),
    ]
    if envelope.get("request_body_sha256"):
        rows.insert(7, ("request_body_sha256", envelope.get("request_body_sha256")))
    lines = [
        "## 实际调用工具头",
        "",
        *_markdown_table(rows),
        "",
        "### 实际挂载工具",
    ]
    if tool_names:
        lines.extend(f"- `{name}`" for name in tool_names)
    else:
        lines.append("- （无）")
    return "\n".join(lines).strip()


def _infer_call_channel(audit, event=None):
    step = str(audit.get("step") or (event or {}).get("phase") or "").strip()
    if step != "reaction":
        return step
    return "reaction.loop"


def _provider_error_hint(error):
    raw = str(error or "")
    lowered = raw.lower()
    statuses = []
    for match in re.finditer(
        r"\bhttp\s+([45]\d{2})\b|\[([45]\d{2})\]\s*:|"
        r"[\"']?(?:status_code|status|code)[\"']?\s*[:=]\s*[\"']?([45]\d{2})\b",
        raw,
        re.IGNORECASE,
    ):
        status = int(next(group for group in match.groups() if group))
        if status not in statuses:
            statuses.append(status)

    target_match = re.search(
        r"\beconnrefused(?:\s*[:=-])?(?:\s+connect)?\s+"
        r"((?:\[[0-9a-f:]+\]|[a-z0-9._-]+):\d{1,5})\b",
        raw,
        re.IGNORECASE,
    )

    def has(*markers):
        return any(marker in lowered for marker in markers)

    if has("provider_call_cancelled", "request cancelled", "request canceled", "operation cancelled", "operation canceled"):
        kind = "cancelled"
    elif has("econnrefused", "connection refused"):
        kind = "connection_refused"
    elif has("getaddrinfo failed", "name or service not known", "temporary failure in name resolution", "nodename nor servname", "enotfound"):
        kind = "dns_error"
    elif has("certificate_verify_failed", "ssl: certificate", "tls handshake", "sslerror", "certificate verify failed"):
        kind = "tls_error"
    elif has("provider_request_timeout", "provider_stream_first_chunk_timeout", "provider_stream_idle_timeout", "etimedout", "timed out", "request timeout", "first chunk timeout", "stream idle timeout"):
        kind = "timeout"
    elif has("provider_stream_interrupted", "econnreset", "connection reset", "broken pipe", "remote end closed", "socket hang up"):
        kind = "connection_interrupted"
    elif has("invalid_api_key", "authentication_error", "incorrect api key", "unauthorized"):
        kind = "authentication"
    elif has("permission_denied", "forbidden"):
        kind = "permission_denied"
    elif has("model_not_found", "model_not_available", "model is not available", "no available channel", "model profile is not configured"):
        kind = "model_unavailable"
    elif has("insufficient_quota", "rate_limit", "rate limit", "quota exceeded", "quota exhausted", "insufficient balance", "balance insufficient", "insufficient credits", "credits exhausted"):
        kind = "rate_limit_or_quota"
    elif has("context_length_exceeded", "maximum context length", "context window", "too many tokens", "request too large"):
        kind = "context_too_long"
    elif has("provider_response_invalid_json", "invalid sse", "malformed sse", "sse parse", "json decode error", "failed to decode json", "provider returned an empty response", "provider_native_tool_empty_output", "provider_stream_incomplete_tool_call"):
        kind = "invalid_response"
    else:
        kind = "unknown"
        for status in reversed(statuses):
            if status == 408:
                kind = "timeout"
            elif status == 401:
                kind = "authentication"
            elif status == 403:
                kind = "permission_denied"
            elif status == 404:
                kind = "endpoint_not_found"
            elif status in {402, 429}:
                kind = "rate_limit_or_quota"
            elif status == 413:
                kind = "context_too_long"
            elif status in {400, 422}:
                kind = "request_rejected"
            elif status in {502, 503, 504}:
                kind = "upstream_unavailable"
            elif 500 <= status <= 599:
                kind = "service_error"
            elif 400 <= status <= 499:
                kind = "request_rejected"
            if kind != "unknown":
                break

    hint = {"kind": kind, "http_statuses": statuses}
    if target_match:
        hint["target"] = target_match.group(1)
    return hint


def _cards_for_event(event, frame_id=""):
    event_type = event.get("event_type")
    payload = event.get("payload") or {}
    if event_type in {
            "general_tool_approval_requested",
            "general_tool_approval_resolved",
    }:
        decision = str(payload.get("decision") or "").strip()
        title = "工具审批"
        summary = str(payload.get("summary") or payload.get("tool_id") or "").strip()
        card = _card(
            event,
            "tool-approval",
            title,
            summary,
            summary,
            frame_id or str(payload.get("frame_id") or ""),
        )
        for key in (
                "approval_id", "tool_id", "tool_signature", "decision",
                "requested_at", "resolved_at"):
            if payload.get(key) not in (None, ""):
                card[key] = payload.get(key)
        return [card]
    if event_type == "round_started":
        return [_card(event, "settlement", "轮次开始", _round_started_md(payload), _json_pretty(payload), frame_id)]
    if event_type == "step_input_snapshot":
        return [_card(
            event,
            "context-update",
            "上下文更新",
            _context_update_summary_md(event, payload),
            _context_update_summary_raw(event, payload),
            frame_id,
        )]
    if event_type == "llm_call_started":
        md, raw = _format_tool_header_pair(event)
        return [_card(event, "runtime-parse", "LLM 调用开始", md, raw, frame_id)]
    if event_type in {
        "llm_stream_first_chunk",
        "llm_stream_delta",
        "llm_stream_done",
        "llm_stream_error",
    }:
        title = {
            "llm_stream_first_chunk": "LLM 流式首字",
            "llm_stream_delta": "LLM 流式进展",
            "llm_stream_done": "LLM 流式完成",
            "llm_stream_error": "LLM 流式错误",
        }.get(event_type, "LLM 流式事件")
        severity = "warning" if event_type == "llm_stream_error" else "info"
        return [_card(
            event,
            "warning-error" if event_type == "llm_stream_error" else "runtime-parse",
            title,
            _stream_progress_md(event_type, payload),
            _json_pretty(payload),
            frame_id,
            severity=severity,
        )]
    if event_type == "llm_output_raw":
        return _raw_output_cards(event, payload, frame_id)
    if event_type == "llm_output_parsed":
        return _parsed_output_cards(event, payload, frame_id)
    if event_type == "step_settlement":
        return _settlement_cards(event, payload, frame_id)
    if event_type == "llm_error":
        raw_error = str(payload.get("error") or _json_pretty(payload))
        card = _card(
            event,
            "warning-error",
            "LLM 调用错误",
            raw_error,
            _json_pretty(payload),
            frame_id,
            severity="error",
        )
        card["provider_error_hint"] = _provider_error_hint(raw_error)
        return [card]
    if event_type == "runtime_audit":
        card_type = "warning-error" if payload.get("issues") or payload.get("status") == "issues" else "runtime-parse"
        return [_card(event, card_type, "Runtime 审计", _runtime_audit_md(payload), _json_pretty(payload), frame_id)]
    if event_type == "round_closed":
        return _round_closed_cards(event, payload, frame_id)
    return [_card(event, "runtime-parse", event_type or "事件", _event_payload_md(event_type, payload), _json_pretty(payload), frame_id)]


def _card(event, card_type, title, content_md, content_raw=None, frame_id="", severity="info"):
    content_md = str(content_md or "").strip()
    content_raw = str(content_raw if content_raw is not None else content_md).strip()
    event_index = int(event.get("event_index") or 0)
    event_id = str(event.get("event_id") or "").strip()
    collapsible = (
        card_type in COLLAPSIBLE_CARD_TYPES
        and card_type not in OPEN_CARD_TYPES
    )
    default_collapsed = bool(
        collapsible and card_type not in DEFAULT_EXPANDED_CARD_TYPES
    )
    return {
        "card_id": f"{event_index}:{card_type}:{title}",
        "event_id": event_id,
        "source_event_id": event_id,
        "type": card_type,
        "title": title,
        "summary": _card_summary(
            event,
            card_type=card_type,
            title=title,
            event_index=event_index,
            severity=severity,
        ),
        "collapsible": collapsible,
        "default_collapsed": default_collapsed,
        "content": content_md,
        "content_md": content_md,
        "content_raw": content_raw,
        "event_index": event_index,
        "event_type": event.get("event_type") or "",
        "phase": event.get("phase") or "",
        "iteration": event.get("iteration"),
        "frame_id": frame_id,
        "recorded_at": event.get("recorded_at") or "",
        "stream": "conversation",
        "severity": severity,
        "provenance": {
            "source_event_id": event_id,
            "event_index": event_index,
            "event_type": event.get("event_type") or "",
            "frame_id": frame_id,
            "renderer": "round_live_viewer._cards_for_event",
        },
    }


def _card_summary(event, card_type, title, event_index, severity):
    bits = [str(title or card_type or "事件"), f"#{event_index}"]
    phase = str(event.get("phase") or "").strip()
    if phase:
        bits.append(phase)
    iteration = event.get("iteration")
    if iteration not in (None, ""):
        bits.append(f"迭代 {iteration}")
    if severity and severity != "info":
        bits.append(str(severity))
    return " · ".join(bits)


def _round_started_md(payload):
    return "\n".join([
        "## 轮次开始",
        "",
        *_markdown_table([
            ("轮型", payload.get("round_type")),
            ("输入快照", "已记录" if payload.get("input_snapshot") is not None else "无"),
        ]),
    ]).strip()


def _context_update_summary_md(event, payload):
    messages = payload.get("messages") or []
    manifest = payload.get("manifest") or {}
    layers = manifest.get("layers") if isinstance(manifest, dict) else None
    rows = [
        ("阶段", event.get("phase")),
        ("迭代", event.get("iteration")),
        ("消息数", len(messages)),
    ]
    if isinstance(layers, dict):
        for pane_id, _title in LEGACY_CONTEXT_PANES:
            layer = layers.get(pane_id)
            chars = layer.get("chars") if isinstance(layer, dict) else None
            if chars is not None:
                rows.append((f"{pane_id} 字符数", chars))
    else:
        rows.append(("层来源", "messages"))
    return "\n".join(["## 上下文更新", "", *_markdown_table(rows)]).strip()


def _context_update_summary_raw(event, payload):
    messages = payload.get("messages") or []
    manifest = payload.get("manifest") or {}
    layers = manifest.get("layers") if isinstance(manifest, dict) else None
    layer_bits = []
    if isinstance(layers, dict):
        for pane_id, _title in LEGACY_CONTEXT_PANES:
            chars = (layers.get(pane_id) or {}).get("chars") if isinstance(layers.get(pane_id), dict) else None
            if chars is not None:
                layer_bits.append(f"{pane_id}={chars}")
    return "\n".join([
        f"phase={event.get('phase') or ''}",
        f"iteration={event.get('iteration') or ''}",
        f"messages={len(messages)}",
        "layers=" + (", ".join(layer_bits) if layer_bits else "(from messages)"),
    ])


def _canonical_user_sources(events):
    """Return one user-message source event per Round.

    New ledgers use the immutable ``round_started`` trigger.  Old ledgers may
    fall back once to their earliest input snapshot.
    """
    sources = {}
    fallback = {}
    for event in events or []:
        round_num = int(event.get("round") or 0)
        event_type = event.get("event_type")
        payload = event.get("payload") or {}
        if event_type == "round_started":
            snapshot = payload.get("input_snapshot") or {}
            trigger = snapshot.get("trigger") or {}
            messages = trigger.get("messages")
            if isinstance(messages, list):
                sources[round_num] = (
                    int(event.get("event_index") or 0), messages)
        elif event_type == "step_input_snapshot" and round_num not in fallback:
            messages = payload.get("messages")
            if isinstance(messages, list):
                fallback[round_num] = (
                    int(event.get("event_index") or 0), messages)
    for round_num, source in fallback.items():
        sources.setdefault(round_num, source)
    return sources


def _user_cards_from_messages(event, messages, frame_id=""):
    cards = []
    round_num = int(event.get("round") or 0)
    message_index = 0
    for message in messages or []:
        if isinstance(message, dict):
            if str(message.get("role") or "").strip().lower() != "user":
                continue
            content = str(message.get("content") or "")
        else:
            content = str(message or "")
        if not content:
            continue
        message_index += 1
        card = _card(event, "user", "用户输入", content, content, frame_id)
        card["card_id"] = f"R{round_num:06d}:user:{message_index}"
        card["message_index"] = message_index
        cards.append(card)
    return cards


def _raw_output_cards(event, payload, frame_id):
    cards = []
    response = str(payload.get("response") or "").strip()
    if response:
        phase = str(event.get("phase") or "").strip()
        card_type = "assistant-final" if phase == "final_reply" else "assistant-progress"
        title = "AI 最终回复" if card_type == "assistant-final" else "AI 原始文本"
        cards.append(_card(event, card_type, title, response, response, frame_id))
    for envelope in payload.get("tool_call_envelopes") or []:
        if not isinstance(envelope, dict):
            continue
        tool_id = envelope.get("tool_id") or envelope.get("name") or "tool_call"
        cards.append(_card(
            event,
            "tool-call",
            f"工具调用｜{tool_id}",
            _tool_call_md(envelope),
            _json_pretty(envelope),
            frame_id,
        ))
    if not cards and _request_contract_audit_for_event(event):
        cards.append(_card(
            event,
            "runtime-parse",
            "LLM 原始输出",
            "无文本或工具调用；已记录调用契约。",
            _json_pretty(payload),
            frame_id,
        ))
    return cards


def _stream_progress_md(event_type, payload):
    rows = [
        ("协议", payload.get("protocol")),
        ("首字耗时", _format_ms(payload.get("first_chunk_latency_ms"))),
        ("当前持续", _format_ms(payload.get("elapsed_ms"))),
        ("文本字符数", payload.get("content_chars")),
        ("推理字符数", payload.get("reasoning_chars")),
        ("工具参数字符数", payload.get("tool_argument_chars")),
        ("工具调用数", payload.get("tool_call_count")),
        ("finish_reason", payload.get("finish_reason")),
        ("reason", payload.get("reason")),
    ]
    return "\n".join([
        f"## {event_type}",
        "",
        *_markdown_table(rows),
    ]).strip()


def _format_ms(value):
    try:
        ms = int(value)
    except (TypeError, ValueError):
        return None
    if ms < 1000:
        return f"{ms}ms"
    return f"{ms / 1000:.1f}s"


def _tool_call_md(envelope):
    tool_id = envelope.get("tool_id") or envelope.get("name") or "tool_call"
    arguments = envelope.get("arguments")
    if arguments is None and envelope.get("arguments_json") is not None:
        arguments = envelope.get("arguments_json")
    lines = [
        "## 工具调用",
        "",
        *_markdown_table([
            ("工具 ID", tool_id),
            ("Provider 调用 ID", envelope.get("call_id")),
            ("状态", envelope.get("status")),
        ]),
        "",
        "### 参数",
        _json_fence(arguments),
    ]
    return "\n".join(lines).strip()


def _parsed_output_cards(event, payload, frame_id):
    cards = []
    rows = []
    labels = (
        ("parse_status", "解析状态"),
        ("response_route", "回复路线"),
        ("text_channel", "文本通道"),
        ("final_response_source", "最终回复来源"),
    )
    for key, label in labels:
        if payload.get(key) is not None:
            rows.append((label, payload.get(key)))
    envelopes = payload.get("message_envelopes") or []
    if envelopes:
        rows.append(("消息信封数", len(envelopes)))
    if rows:
        cards.append(_card(
            event,
            "runtime-parse",
            "Runtime 解析",
            "\n".join(["## Runtime 解析", "", *_markdown_table(rows)]),
            _json_pretty(payload),
            frame_id,
        ))
    for envelope in envelopes:
        if not isinstance(envelope, dict):
            continue
        channel = str(envelope.get("channel") or "")
        text = envelope.get("text") or envelope.get("redacted_marker") or ""
        if channel == "assistant_text":
            phase = str(envelope.get("phase") or event.get("phase") or "").strip()
            natural_final_reply = (
                envelope.get("terminal_text_candidate") is True
                and str(envelope.get("terminal_decision") or "").strip() == "finish"
                and str(payload.get("natural_final_reply_candidate") or "").strip()
                == str(text).strip()
            )
            if phase in {"closeout", "final_reply"}:
                card = _card(event, "assistant-final", "AI 最终回复", text, text, frame_id)
            else:
                card = _card(event, "assistant-progress", "AI 轮中进展", text, text, frame_id)
            if natural_final_reply:
                card["_natural_final_reply_candidate"] = True
            cards.append(card)
        elif channel == "reaction.progress":
            cards.append(_card(event, "assistant-progress", "AI 轮中进展", text, text, frame_id))
        elif channel == "final_reply.text":
            cards.append(_card(event, "assistant-final", "AI 最终回复", text, text, frame_id))
        elif "illegal" in channel or "empty" in channel or "invalid" in channel:
            cards.append(_card(
                event,
                "warning-error",
                f"非法/失败消息｜{channel}",
                text or "非法文本事件；原文不回灌。",
                _json_pretty(envelope),
                frame_id,
                severity="warning",
            ))
        else:
            cards.append(_card(
                event,
                "runtime-parse",
                f"消息信封｜{channel}",
                _message_envelope_md(envelope),
                _json_pretty(envelope),
                frame_id,
            ))
    if payload.get("invalid_text_response"):
        cards.append(_card(
            event,
            "warning-error",
            "非法文本摘要",
            _event_payload_md("invalid_text_response", payload.get("invalid_text_response")),
            _json_pretty(payload.get("invalid_text_response")),
            frame_id,
            severity="warning",
        ))
    return cards


def _message_envelope_md(envelope):
    rows = [
        ("通道", envelope.get("channel")),
        ("类型", envelope.get("type")),
        ("摘要", envelope.get("summary")),
    ]
    text = envelope.get("text") or envelope.get("redacted_marker")
    lines = ["## 消息信封", "", *_markdown_table(rows)]
    if text:
        lines.extend(["", "### 正文", str(text)])
    return "\n".join(lines).strip()


def _settlement_cards(event, payload, frame_id):
    cards = []
    for result in _iter_tool_results(payload):
        tool_id = result.get("tool_id") or result.get("tool") or result.get("name") or "tool_result"
        cards.append(_card(
            event,
            "tool-result",
            f"工具结果｜{tool_id}",
            _tool_result_md(result),
            _json_pretty(result),
            frame_id,
        ))
    cards.append(_card(
        event,
        "settlement",
        "落账/回执",
        _settlement_md(payload),
        _json_pretty(payload),
        frame_id,
    ))
    return cards


def _tool_result_md(result):
    tool_id = result.get("tool_id") or result.get("tool") or result.get("name") or "tool_result"
    rows = [
        ("工具 ID", tool_id),
        ("状态", result.get("status")),
        ("摘要", result.get("summary")),
        ("错误", result.get("error")),
    ]
    return "\n".join([
        "## 工具结果",
        "",
        *_markdown_table(rows),
        "",
        "### 原始结果",
        _json_fence(result),
    ]).strip()


def _settlement_md(payload):
    rows = [
        ("工具结果数", len(list(_iter_tool_results(payload)))),
        ("回执", "已记录" if payload else "无"),
    ]
    return "\n".join([
        "## 落账/回执",
        "",
        *_markdown_table(rows),
        "",
        "### 详情",
        _json_fence(payload),
    ]).strip()


def _iter_tool_results(payload):
    keys = (
        "general_tool_results",
        "protocol_tool_receipts",
        "native_tool_result_projections",
        "tool_results",
        "receipts",
    )
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            for item in value:
                if isinstance(item, dict):
                    yield item
        elif isinstance(value, dict):
            yield value


def _runtime_audit_md(payload):
    rows = [
        ("状态", payload.get("status")),
        ("问题数", len(payload.get("issues") or [])),
    ]
    lines = ["## Runtime 审计", "", *_markdown_table(rows)]
    issues = payload.get("issues") or []
    if issues:
        lines.extend(["", "### 问题"])
        lines.extend(f"- {issue}" for issue in issues)
    return "\n".join(lines).strip()


def _round_closed_cards(event, payload, frame_id):
    cards = [_card(event, "settlement", "轮次关闭", _round_closed_md(payload), _json_pretty(payload), frame_id)]
    final_response = str(payload.get("final_response") or "").strip()
    status = str(payload.get("status") or "").strip().lower()
    if final_response:
        final_card = _card(event, "assistant-final", "AI 最终回复", final_response, final_response, frame_id)
        final_card["_final_response_source"] = str(payload.get("final_response_source") or "").strip()
        cards.append(final_card)
    if status not in {"closed", "ok", "success"} or not final_response:
        cards.append(_card(
            event,
            "warning-error",
            "轮次事故标记",
            f"## 轮次事故标记\n\n- 状态：`{status or '(empty)'}`\n- 最终回复为空：`{str(not bool(final_response)).lower()}`",
            f"status={status or '(empty)'}; final_response_empty={str(not bool(final_response)).lower()}",
            frame_id,
            severity="error",
        ))
    return cards


def _round_closed_md(payload):
    return "\n".join([
        "## 轮次关闭",
        "",
        *_markdown_table([
            ("状态", payload.get("status")),
            ("最终回复为空", not bool(str(payload.get("final_response") or "").strip())),
        ]),
    ]).strip()


def _event_payload_md(event_type, payload):
    return "\n".join([
        f"## {event_type or '事件'}",
        "",
        _json_fence(payload),
    ]).strip()


def _json_compact(value):
    if value is None:
        return "(none)"
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"), default=str)


def _json_pretty(value):
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str)


def _json_fence(value):
    text = _json_pretty(value)
    if not text:
        text = "null"
    return "```json\n" + text + "\n```"


def _markdown_table(rows):
    lines = ["| 字段 | 值 |", "| --- | --- |"]
    for key, value in rows:
        if value is None or value == "":
            continue
        lines.append(f"| {_md_cell(key)} | {_md_cell(_json_compact(value))} |")
    if len(lines) == 2:
        lines.append("| （无） | （无） |")
    return lines


def _md_cell(value):
    return str(value).replace("|", "\\|").replace("\n", "<br>")
