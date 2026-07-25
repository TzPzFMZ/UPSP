"""GPT-5.6 explicit prompt-cache configuration and payload decoration."""
from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any


PROMPT_CACHE_PROFILES = {
    "off",
    "key_only",
    "gpt56_explicit_permanent",
    "gpt56_explicit_tiered",
}
EXPLICIT_PROFILES = {
    "gpt56_explicit_permanent",
    "gpt56_explicit_tiered",
}
DEFAULT_PROMPT_SCHEMA_VERSION = "context-v43"
DEFAULT_PROMOTED_MIN_CHARS = 4096


def is_gpt56_model(model_name: Any) -> bool:
    return bool(re.match(r"^gpt-5\.6(?:$|[-.])", str(model_name or "").lower()))


def _clean_component(value: Any, fallback: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "-", str(value or "").strip())
    return cleaned.strip("-") or fallback


def profile_settings(
    raw_config: Any,
    *,
    provider: str,
    model_name: str,
    lane: str,
) -> dict[str, Any] | None:
    """Resolve new profile settings; return ``None`` for legacy configs."""
    if not isinstance(raw_config, dict) or "profile" not in raw_config:
        return None
    profile = str(raw_config.get("profile") or "").strip().lower()
    if profile not in PROMPT_CACHE_PROFILES:
        raise ValueError(f"prompt_cache_profile_unknown:{profile or '<empty>'}")
    allowed_fields = {
        "enabled",
        "profile",
        "prompt_schema_version",
        "ttl",
        "retention",
    }
    unknown_fields = sorted(set(raw_config) - allowed_fields)
    if unknown_fields:
        raise ValueError(
            "prompt_cache_profile_fields_unsupported:"
            + ",".join(unknown_fields)
        )
    if raw_config.get("enabled") is False or profile == "off":
        return {}
    if "retention" in raw_config:
        raise ValueError("prompt_cache_retention_conflicts_with_profile")
    if provider not in {"openai_chat", "openai_responses"}:
        raise ValueError(f"prompt_cache_profile_provider_unsupported:{provider}")

    model_lc = str(model_name or "").strip().lower()
    if profile in EXPLICIT_PROFILES:
        if provider != "openai_chat":
            raise ValueError("prompt_cache_breakpoint_requires_openai_chat")
        if not is_gpt56_model(model_lc):
            raise ValueError(
                f"prompt_cache_breakpoint_model_unsupported:{model_name or '<empty>'}"
            )

    ttl = str(raw_config.get("ttl") or "30m").strip().lower()
    if profile in EXPLICIT_PROFILES and ttl != "30m":
        raise ValueError(f"prompt_cache_ttl_unsupported:{ttl}")
    if profile not in EXPLICIT_PROFILES and "ttl" in raw_config:
        raise ValueError("prompt_cache_ttl_requires_explicit_profile")
    schema_version = _clean_component(
        raw_config.get("prompt_schema_version"),
        DEFAULT_PROMPT_SCHEMA_VERSION,
    )
    key = ":".join((
        "upsp",
        "v1",
        _clean_component(provider, "provider"),
        _clean_component(model_name, "model"),
        _clean_component(lane, "default"),
        schema_version,
    ))
    settings = {
        "profile": profile,
        "lane": lane,
        "key": key,
        "applied": True,
        "mode": "explicit" if profile in EXPLICIT_PROFILES else "implicit",
        "strategy": (
            "tiered"
            if profile == "gpt56_explicit_tiered"
            else "permanent"
            if profile == "gpt56_explicit_permanent"
            else "key_only"
        ),
        "prompt_schema_version": schema_version,
    }
    if profile in EXPLICIT_PROFILES:
        settings["ttl"] = ttl
    return settings


def _canonical_sha(value: Any) -> str:
    encoded = json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _content_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if not isinstance(content, list):
        return str(content or "")
    parts: list[str] = []
    for block in content:
        if isinstance(block, str):
            parts.append(block)
        elif isinstance(block, dict) and block.get("type") == "text":
            parts.append(str(block.get("text") or ""))
    return "".join(parts)


def _decorate_breakpoint(message: dict[str, Any]) -> dict[str, Any]:
    decorated = copy.deepcopy(message)
    content = decorated.get("content")
    marker = {"mode": "explicit"}
    if isinstance(content, str):
        decorated["content"] = [{
            "type": "text",
            "text": content,
            "prompt_cache_breakpoint": marker,
        }]
        return decorated
    if isinstance(content, list):
        blocks = copy.deepcopy(content)
        for index in range(len(blocks) - 1, -1, -1):
            block = blocks[index]
            if isinstance(block, dict) and block.get("type") == "text":
                block["prompt_cache_breakpoint"] = marker
                decorated["content"] = blocks
                return decorated
    raise ValueError("prompt_cache_breakpoint_content_unsupported")


def _prefix_fingerprint(
    payload: dict[str, Any],
    messages: list[dict[str, Any]],
    target_index: int,
) -> str:
    return _canonical_sha({
        "model": payload.get("model"),
        "tools": payload.get("tools") or [],
        "messages": messages[: target_index + 1],
    })


def apply_explicit_breakpoints(
    payload: dict[str, Any],
    *,
    profile: str,
    message_layers: list[str],
    layer_contents: dict[str, Any],
    promoted_min_chars: int = DEFAULT_PROMOTED_MIN_CHARS,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Decorate stable Chat message boundaries and return an audit-only plan."""
    if profile not in EXPLICIT_PROFILES:
        return payload, {}
    messages = payload.get("messages")
    if not isinstance(messages, list) or len(messages) != len(message_layers):
        raise ValueError("prompt_cache_message_layer_alignment_invalid")
    base_indexes = [
        index for index, layer in enumerate(message_layers)
        if layer == "10_permanent"
    ]
    if not base_indexes:
        raise ValueError("prompt_cache_permanent_boundary_missing")
    base_index = base_indexes[-1]
    target_indexes = [("10_permanent", base_index)]

    promoted_layer = ""
    promoted_index = -1
    promoted_increment_chars = 0
    if profile == "gpt56_explicit_tiered":
        for candidate in ("30_lately", "20_periodic"):
            indexes = [
                index for index, layer in enumerate(message_layers)
                if layer == candidate
            ]
            if indexes:
                promoted_layer = candidate
                promoted_index = indexes[-1]
                break
        if promoted_index > base_index:
            promoted_increment_chars = sum(
                len(_content_text(messages[index].get("content")))
                for index in range(base_index + 1, promoted_index + 1)
            )
            if promoted_increment_chars >= max(1, int(promoted_min_chars)):
                target_indexes.append((promoted_layer, promoted_index))

    decorated_messages = copy.deepcopy(messages)
    target_details = []
    for layer_key, index in target_indexes:
        decorated_messages[index] = _decorate_breakpoint(decorated_messages[index])
        target_details.append({
            "layer": layer_key,
            "message_index": index,
            "prefix_chars": sum(
                len(_content_text(message.get("content")))
                for message in messages[: index + 1]
            ),
            "prefix_fingerprint": _prefix_fingerprint(payload, messages, index),
        })
    decorated = dict(payload)
    decorated["messages"] = decorated_messages
    latest = target_details[-1]
    lately_content = layer_contents.get("30_lately")
    lately_epoch = _canonical_sha(lately_content) if lately_content else ""
    plan = {
        "schema": "prompt_cache_plan.v1",
        "profile": profile,
        "mode": "explicit",
        "strategy": (
            "tiered" if profile == "gpt56_explicit_tiered" else "permanent"
        ),
        "targets": [item["layer"] for item in target_details],
        "target_details": target_details,
        "prefix_fingerprint": latest["prefix_fingerprint"],
        "lately_epoch": lately_epoch,
        "promoted_increment_chars": promoted_increment_chars,
        "promoted_min_chars": max(1, int(promoted_min_chars)),
    }
    return decorated, plan


def model_visible_contract(payload: dict[str, Any]) -> dict[str, Any]:
    """Return cache-metadata-free model-visible text/tool contract for tests."""
    messages = []
    for message in payload.get("messages") or []:
        if not isinstance(message, dict):
            continue
        normalized = copy.deepcopy(message)
        normalized["content"] = _content_text(normalized.get("content"))
        messages.append(normalized)
    return {
        "model": payload.get("model"),
        "messages": messages,
        "tools": copy.deepcopy(payload.get("tools") or []),
    }
