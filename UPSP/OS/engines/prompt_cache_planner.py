"""Automatic protocol-aware prompt-cache planning."""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any


AUTOMATIC_PROFILE = "automatic_tiered"
PROMPT_CACHE_PROFILES = {AUTOMATIC_PROFILE}
EXPLICIT_PROFILES = {AUTOMATIC_PROFILE}
DEFAULT_PROMPT_SCHEMA_VERSION = "context-v43"
DEFAULT_PROMOTED_MIN_CHARS = 4096


def is_gpt56_model(model_name: Any) -> bool:
    return bool(re.match(r"^gpt-(?:5\.6|[6-9])(?:$|[-.])", str(model_name or "").lower()))


def _clean_component(value: Any, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip())
    return cleaned.strip("-") or fallback


def profile_settings(raw_config: Any, *, provider: str, model_name: str,
                     lane: str, persona_id: str = "") -> dict[str, Any]:
    """Return the single hidden cache policy used by every endpoint."""
    config = raw_config if isinstance(raw_config, dict) else {}
    # Legacy knobs are intentionally ignored; cache policy is no longer configurable.
    if provider not in {"openai_chat", "openai_responses", "anthropic_messages"}:
        raise ValueError(f"prompt_cache_provider_unsupported:{provider}")
    schema = _clean_component(config.get("prompt_schema_version"), DEFAULT_PROMPT_SCHEMA_VERSION)
    wire_explicit = provider == "anthropic_messages" or is_gpt56_model(model_name)
    return {
        "profile": AUTOMATIC_PROFILE,
        "lane": lane,
        "key": ":".join(("upsp", "v2", _clean_component(persona_id, "persona"),
                         _clean_component(provider, "provider"),
                         _clean_component(model_name, "model"),
                         _clean_component(lane, "default"), schema)),
        "applied": True,
        "mode": "explicit" if wire_explicit else "implicit",
        "strategy": "tiered",
        "prompt_schema_version": schema,
        "ttl": "30m" if wire_explicit and provider.startswith("openai_") else "",
    }


def _canonical_sha(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                     separators=(",", ":")).encode("utf-8")).hexdigest()


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return str(content or "")
    return "".join(str(block.get("text") or "") for block in content
                   if isinstance(block, dict) and block.get("type") in {"text", "input_text"})


def _decorate_breakpoint(message: dict[str, Any], provider: str) -> dict[str, Any]:
    decorated = copy.deepcopy(message)
    content = decorated.get("content")
    block_type = "input_text" if provider == "openai_responses" else "text"
    marker_key = "cache_control" if provider == "anthropic_messages" else "prompt_cache_breakpoint"
    marker = {"type": "ephemeral"} if marker_key == "cache_control" else {"mode": "explicit"}
    if isinstance(content, str):
        decorated["content"] = [{"type": block_type, "text": content, marker_key: marker}]
        return decorated
    blocks = copy.deepcopy(content) if isinstance(content, list) else []
    for block in reversed(blocks):
        if isinstance(block, dict) and block.get("type") in {"text", "input_text"}:
            block[marker_key] = marker
            decorated["content"] = blocks
            return decorated
    raise ValueError("prompt_cache_breakpoint_content_unsupported")


def apply_explicit_breakpoints(payload: dict[str, Any], *, profile: str,
                               message_layers: list[str], layer_contents: dict[str, Any],
                               provider: str = "openai_chat",
                               promoted_min_chars: int = DEFAULT_PROMOTED_MIN_CHARS
                               ) -> tuple[dict[str, Any], dict[str, Any]]:
    """Decorate B0/B1 on canonical messages and return the wire audit plan."""
    messages = payload.get("messages")
    if not isinstance(messages, list) or len(messages) != len(message_layers):
        raise ValueError("prompt_cache_message_layer_alignment_invalid")
    base = [i for i, layer in enumerate(message_layers) if layer == "10_permanent"]
    if not base:
        raise ValueError("prompt_cache_permanent_boundary_missing")
    base_index = base[-1]
    targets = [("10_permanent", base_index)]
    promoted_chars = 0
    for candidate in ("30_lately", "20_periodic"):
        indexes = [i for i, layer in enumerate(message_layers) if layer == candidate]
        if not indexes:
            continue
        index = indexes[-1]
        if index > base_index:
            promoted_chars = sum(len(_content_text(messages[i].get("content")))
                                 for i in range(base_index + 1, index + 1))
            if promoted_chars >= max(1, int(promoted_min_chars)):
                targets.append((candidate, index))
        break
    decorated = copy.deepcopy(messages)
    details = []
    for layer, index in targets:
        decorated[index] = _decorate_breakpoint(decorated[index], provider)
        prefix = {"model": payload.get("model"),
                  "messages": messages[:index + 1],
                  "tools": payload.get("tools") or []}
        details.append({"layer": layer, "message_index": index,
                        "prefix_chars": sum(len(_content_text(x.get("content"))) for x in messages[:index + 1]),
                        "prefix_fingerprint": _canonical_sha(prefix)})
    result = dict(payload)
    result["messages"] = decorated
    plan = {"schema": "prompt_cache_plan.v2", "profile": AUTOMATIC_PROFILE,
            "mode": "explicit", "wire_format": provider, "strategy": "tiered",
            "targets": [x["layer"] for x in details], "target_details": details,
            "prefix_fingerprint": details[-1]["prefix_fingerprint"],
            "lately_epoch": _canonical_sha(layer_contents.get("30_lately")) if layer_contents.get("30_lately") else "",
            "promoted_increment_chars": promoted_chars,
            "promoted_min_chars": max(1, int(promoted_min_chars))}
    return result, plan


def model_visible_contract(payload: dict[str, Any]) -> dict[str, Any]:
    messages = []
    for message in payload.get("messages") or payload.get("input") or []:
        if isinstance(message, dict):
            item = copy.deepcopy(message)
            item["content"] = _content_text(item.get("content"))
            messages.append(item)
    return {"model": payload.get("model"), "messages": messages,
            "tools": copy.deepcopy(payload.get("tools") or [])}
