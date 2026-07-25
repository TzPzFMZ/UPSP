"""Provider-neutral prompt-cache usage accounting.

The extractor preserves raw provider distinctions. In particular, a missing
cache-write field is not the same fact as an explicitly reported zero.
"""
from __future__ import annotations

import hashlib
from typing import Any


def usage_int(*values: Any) -> int:
    for value in values:
        try:
            if value in (None, ""):
                continue
            return int(value)
        except (TypeError, ValueError):
            continue
    return 0


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _key_fingerprint(value: Any) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def _audit_fields(audit: Any) -> dict[str, Any]:
    audit = _mapping(audit)
    plan = _mapping(audit.get("prompt_cache_plan"))
    targets = plan.get("targets", audit.get("prompt_cache_breakpoint_targets"))
    if not isinstance(targets, list):
        targets = []
    return {
        "prompt_cache_mode": str(
            plan.get("mode") or audit.get("prompt_cache_mode") or ""
        ),
        "prompt_cache_lane": str(audit.get("prompt_cache_lane") or ""),
        "prompt_cache_key_fingerprint": _key_fingerprint(
            audit.get("prompt_cache_key")
        ),
        "breakpoint_strategy": str(
            plan.get("strategy")
            or audit.get("prompt_cache_breakpoint_strategy")
            or ""
        ),
        "breakpoint_targets": [str(item) for item in targets if str(item)],
        "prefix_fingerprint": str(
            plan.get("prefix_fingerprint")
            or audit.get("prompt_cache_prefix_fingerprint")
            or ""
        ),
        "lately_epoch": str(
            plan.get("lately_epoch")
            or audit.get("prompt_cache_lately_epoch")
            or ""
        ),
    }


def _cache_read_usage(
    raw_usage: dict[str, Any],
    prompt_details: dict[str, Any],
    input_details: dict[str, Any],
    usage_meta: dict[str, Any],
) -> tuple[int, str]:
    if "cached_tokens" in prompt_details or "cached_tokens" in input_details:
        return usage_int(
            prompt_details.get("cached_tokens"),
            input_details.get("cached_tokens"),
        ), "openai.prompt_tokens_details.cached_tokens"
    if "cache_read_input_tokens" in raw_usage or "cache_read" in input_details:
        return usage_int(
            raw_usage.get("cache_read_input_tokens"),
            input_details.get("cache_read"),
        ), "anthropic.cache_read_input_tokens"
    if (
        "cached_content_token_count" in raw_usage
        or "cached_content_token_count" in usage_meta
    ):
        return usage_int(
            raw_usage.get("cached_content_token_count"),
            usage_meta.get("cached_content_token_count"),
        ), "gemini.cached_content_token_count"
    return 0, ""


def _cache_write_usage(
    raw_usage: dict[str, Any],
    prompt_details: dict[str, Any],
    input_details: dict[str, Any],
) -> tuple[int, str, int]:
    anthropic_write = usage_int(
        raw_usage.get("cache_creation_input_tokens"),
        input_details.get("cache_creation"),
    )
    if (
        "cache_write_tokens" in prompt_details
        or "cache_write_tokens" in input_details
    ):
        return usage_int(
            prompt_details.get("cache_write_tokens"),
            input_details.get("cache_write_tokens"),
        ), "openai.prompt_tokens_details.cache_write_tokens", anthropic_write
    if (
        "cache_creation_input_tokens" in raw_usage
        or "cache_creation" in input_details
    ):
        return (
            anthropic_write,
            "anthropic.cache_creation_input_tokens",
            anthropic_write,
        )
    return 0, "", anthropic_write


def extract_prompt_cache_telemetry(
    raw_usage: Any,
    request_contract_audit: Any = None,
) -> dict[str, Any]:
    """Normalize provider usage into ``prompt_cache_telemetry.v2``."""
    if not isinstance(raw_usage, dict):
        return {}

    prompt_details = _mapping(raw_usage.get("prompt_tokens_details"))
    input_details = _mapping(raw_usage.get("input_tokens_details"))
    usage_meta = _mapping(raw_usage.get("usage_metadata"))

    prompt_tokens = usage_int(
        raw_usage.get("prompt_tokens"),
        raw_usage.get("input_tokens"),
        raw_usage.get("inputTokenCount"),
        usage_meta.get("prompt_token_count"),
        usage_meta.get("total_token_count"),
    )

    cache_read_tokens, cache_read_source = _cache_read_usage(
        raw_usage,
        prompt_details,
        input_details,
        usage_meta,
    )
    cache_write_tokens, cache_write_source, anthropic_write = _cache_write_usage(
        raw_usage,
        prompt_details,
        input_details,
    )

    if cache_write_source:
        cache_write_status = (
            "reported_nonzero" if cache_write_tokens > 0 else "reported_zero"
        )
    else:
        cache_write_status = "not_reported"

    source = cache_read_source or cache_write_source
    if not source and prompt_tokens:
        source = "usage.prompt_tokens"
    if not source:
        return {}

    unclassified = max(
        prompt_tokens - cache_read_tokens - cache_write_tokens,
        0,
    )
    telemetry = {
        "schema_version": "prompt_cache_telemetry.v2",
        "source": source,
        "cache_read_source": cache_read_source,
        "cache_write_source": cache_write_source,
        "prompt_tokens": prompt_tokens,
        "cached_tokens": cache_read_tokens,
        "cache_creation_tokens": anthropic_write,
        "cache_read_tokens": cache_read_tokens,
        "cache_write_tokens": cache_write_tokens,
        "cache_write_status": cache_write_status,
        "unclassified_prompt_tokens": unclassified,
        "cache_hit_ratio": round(
            cache_read_tokens / prompt_tokens,
            6,
        ) if prompt_tokens > 0 else 0.0,
        "cache_write_ratio": round(
            cache_write_tokens / prompt_tokens,
            6,
        ) if prompt_tokens > 0 else 0.0,
    }
    telemetry.update(_audit_fields(request_contract_audit))
    return telemetry
