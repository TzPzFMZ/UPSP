import pytest

from data.prompt_cache_telemetry import (
    extract_prompt_cache_telemetry,
    total_input_tokens,
)


@pytest.mark.parametrize(("usage", "expected"), [
    ({"prompt_tokens": 120}, 120),
    ({"input_tokens": 121}, 121),
    ({"input_tokens": 100, "cache_creation_input_tokens": 20}, 120),
    ({"input_tokens": 100, "cache_read_input_tokens": 30}, 130),
    ({"input_tokens": 100, "cache_creation_input_tokens": 20,
      "cache_read_input_tokens": 30}, 150),
    ({"prompt_tokens": 150, "input_tokens": 100,
      "cache_creation_input_tokens": 20, "cache_read_input_tokens": 30}, 150),
    ({"prompt_cache_hit_tokens": 70, "prompt_cache_miss_tokens": 50}, 120),
    ({"prompt_cache_hit_tokens": 70}, None),
    ({"prompt_cache_miss_tokens": 50}, None),
    ({"prompt_tokens": -1, "input_tokens": 120}, None),
    ({"input_tokens": True}, None),
    ({"input_tokens": "120"}, None),
    (None, None),
])
def test_total_input_tokens_normalizes_provider_usage(usage, expected):
    assert total_input_tokens(usage) == expected


def test_openai_chat_reports_cache_read_and_nonzero_write():
    telemetry = extract_prompt_cache_telemetry(
        {
            "prompt_tokens": 4096,
            "prompt_tokens_details": {
                "cached_tokens": 1024,
                "cache_write_tokens": 2048,
            },
        },
        {
            "prompt_cache_lane": "reaction_loop_tools",
            "prompt_cache_key": "upsp:test:reaction",
            "prompt_cache_plan": {
                "mode": "explicit",
                "strategy": "tiered",
                "targets": ["10_permanent", "30_lately"],
                "prefix_fingerprint": "prefix-1",
                "lately_epoch": "lately-1",
            },
        },
    )

    assert telemetry["schema_version"] == "prompt_cache_telemetry.v2"
    assert telemetry["cache_read_tokens"] == 1024
    assert telemetry["cache_write_tokens"] == 2048
    assert telemetry["cache_write_status"] == "reported_nonzero"
    assert telemetry["unclassified_prompt_tokens"] == 1024
    assert telemetry["prompt_cache_mode"] == "explicit"
    assert telemetry["prompt_cache_lane"] == "reaction_loop_tools"
    assert telemetry["prompt_cache_key_fingerprint"]
    assert telemetry["breakpoint_targets"] == ["10_permanent", "30_lately"]
    assert telemetry["lately_epoch"] == "lately-1"


def test_openai_chat_distinguishes_explicit_zero_write_from_missing_field():
    explicit_zero = extract_prompt_cache_telemetry({
        "prompt_tokens": 2048,
        "prompt_tokens_details": {
            "cached_tokens": 1024,
            "cache_write_tokens": 0,
        },
    })
    missing = extract_prompt_cache_telemetry({
        "prompt_tokens": 2048,
        "prompt_tokens_details": {"cached_tokens": 1024},
    })

    assert explicit_zero["cache_write_status"] == "reported_zero"
    assert explicit_zero["cache_write_source"].endswith("cache_write_tokens")
    assert missing["cache_write_status"] == "not_reported"
    assert missing["cache_write_source"] == ""


def test_openai_responses_reads_input_token_details():
    telemetry = extract_prompt_cache_telemetry({
        "input_tokens": 3000,
        "input_tokens_details": {
            "cached_tokens": 1500,
            "cache_write_tokens": 500,
        },
    })

    assert telemetry["cache_read_tokens"] == 1500
    assert telemetry["cache_write_tokens"] == 500
    assert telemetry["unclassified_prompt_tokens"] == 1000
    assert telemetry["cache_write_status"] == "reported_nonzero"


def test_anthropic_creation_maps_to_write_without_losing_legacy_field():
    telemetry = extract_prompt_cache_telemetry({
        "input_tokens": 2500,
        "cache_read_input_tokens": 1200,
        "cache_creation_input_tokens": 800,
    })

    assert telemetry["cache_read_source"] == "anthropic.cache_read_input_tokens"
    assert telemetry["cache_write_source"] == "anthropic.cache_creation_input_tokens"
    assert telemetry["cache_creation_tokens"] == 800
    assert telemetry["cache_write_tokens"] == 800
    assert telemetry["cache_write_status"] == "reported_nonzero"


def test_gemini_read_keeps_write_status_not_reported():
    telemetry = extract_prompt_cache_telemetry({
        "usage_metadata": {
            "prompt_token_count": 1200,
            "cached_content_token_count": 600,
        },
    })

    assert telemetry["cache_read_source"] == "gemini.cached_content_token_count"
    assert telemetry["cache_read_tokens"] == 600
    assert telemetry["cache_write_status"] == "not_reported"
