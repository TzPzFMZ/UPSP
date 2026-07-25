from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from native_tool_test_helpers import _load_prompt_prefix_cache_analyzer


def _write_round_jsonl(tmp_path, round_num, events):
    round_dir = tmp_path / "round"
    round_dir.mkdir(exist_ok=True)
    path = round_dir / f"round_{round_num}.jsonl"
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for index, event in enumerate(events, start=1):
            record = {
                "schema_version": "round_audit.v1",
                "round": round_num,
                "event_index": index,
                "event_id": f"R{round_num:06d}-{index:06d}",
                "recorded_at": "2026-06-17T10:00:00+08:00",
            }
            record.update(event)
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return path


def test_tail_change_reports_common_prefix_against_previous_call(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    first_payload = {
        "system": "base",
        "messages": [
            {"role": "system", "content": "stable"},
            {"role": "user", "content": "read section A"},
        ],
    }
    second_payload = {
        "system": "base",
        "messages": [
            {"role": "system", "content": "stable"},
            {"role": "user", "content": "read section B"},
        ],
    }
    round_file = _write_round_jsonl(
        tmp_path,
        7,
        [
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 1,
                "payload": first_payload,
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 2,
                "payload": second_payload,
            },
        ],
    )

    report = analyzer.analyze_round_files([round_file])
    second = report["calls"][1]
    expected_prefix = analyzer.common_prefix_len(
        analyzer.canonical_payload(first_payload),
        analyzer.canonical_payload(second_payload),
    )

    assert second["previous_round_name"] == "round_7.jsonl"
    assert second["common_prefix_chars"] == expected_prefix
    assert second["prefix_ratio"] == expected_prefix / second["total_chars"]
    assert second["changed_suffix_chars"] == second["total_chars"] - expected_prefix
    assert second["same_phase_common_prefix_chars"] == expected_prefix
    assert second["same_phase_prefix_ratio"] == second["prefix_ratio"]


def test_calls_are_ordered_by_round_number_and_event_index(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    round_dir = tmp_path / "round"
    round_dir.mkdir()
    later = round_dir / "round_11.jsonl"
    earlier = round_dir / "round_10.jsonl"
    for path, round_num, phase, iteration, content in [
        (later, 11, "setup", 1, "round 11"),
        (earlier, 10, "cleanup", 3, "round 10 cleanup"),
        (earlier, 10, "reaction", 1, "round 10 reaction"),
    ]:
        mode = "a" if path.exists() else "w"
        with path.open(mode, encoding="utf-8", newline="\n") as f:
            event_index = sum(1 for _ in path.open(encoding="utf-8")) + 1 if mode == "a" else 1
            record = {
                "schema_version": "round_audit.v1",
                "round": round_num,
                "event_index": event_index,
                "event_type": "step_input_snapshot",
                "phase": phase,
                "iteration": iteration,
                "payload": {"messages": [{"role": "user", "content": content}]},
            }
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    report = analyzer.analyze_round_files([later, earlier])

    assert [
        (call["round_num"], call["phase"], call["iteration"])
        for call in report["calls"]
    ] == [
        (10, "cleanup", 3),
        (10, "reaction", 1),
        (11, "setup", 1),
    ]


def test_provider_request_envelope_is_prompt_cache_truth_source(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    preview_body = {
        "model": "preview-model",
        "messages": [{"role": "user", "content": "preview only"}],
    }
    actual_body = {
        "model": "gemini-3-flash",
        "messages": [{"role": "user", "content": "actual provider body"}],
        "temperature": 0.7,
        "tools": [{"type": "function", "function": {"name": "file_read"}}],
    }
    round_file = _write_round_jsonl(
        tmp_path,
        13,
        [
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "messages": [
                        {"role": "user", "content": "old context snapshot"}
                    ]
                },
            },
            {
                "event_type": "llm_call_started",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "provider_request_envelope": {
                        "schema": "provider_request.v1",
                        "call": {
                            "step": "reaction",
                            "channel": "reaction.loop",
                            "phase": "loop",
                        },
                        "request_body": preview_body,
                        "request_body_sha256": "sha-preview",
                    }
                },
            },
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "provider_request_envelope": {
                        "schema": "provider_request.v1",
                        "call": {
                            "step": "reaction",
                            "channel": "reaction.loop",
                            "phase": "loop",
                        },
                        "request_body": actual_body,
                        "request_body_sha256": "sha-actual",
                    }
                },
            },
        ],
    )

    report = analyzer.analyze_round_files([round_file])
    call = report["calls"][0]

    assert report["summary"]["call_count"] == 1
    assert call["source_event_type"] == "llm_output_raw"
    assert call["call_channel"] == "reaction.loop"
    assert call["request_body_sha256"] == "sha-actual"
    assert call["message_count"] == 1
    assert call["total_chars"] == len(
        json.dumps(
            actual_body,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
    )
    assert report["issues"] == [
        "provider_request_envelope_mismatch:round_13.jsonl:reaction:1:reaction.loop"
    ]


def test_analyzer_pairs_fallback_output_with_matching_started_envelope(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    primary_body = {
        "model": "primary-model",
        "messages": [{"role": "user", "content": "same context"}],
    }
    fallback_body = {
        "model": "fallback-model",
        "messages": [{"role": "user", "content": "same context"}],
        "seed": 425,
    }
    round_file = _write_round_jsonl(
        tmp_path,
        425,
        [
            {
                "event_type": "llm_call_started",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "provider_request_envelope": {
                        "schema": "provider_request.v1",
                        "call": {
                            "step": "reaction",
                            "channel": "reaction.loop",
                            "phase": "loop",
                            "attempt": 1,
                        },
                        "request_body": primary_body,
                        "request_body_sha256": "sha-primary",
                    }
                },
            },
            {
                "event_type": "llm_error",
                "phase": "reaction",
                "iteration": 1,
                "payload": {"error_type": "RuntimeError"},
            },
            {
                "event_type": "llm_call_started",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "provider_request_envelope": {
                        "schema": "provider_request.v1",
                        "call": {
                            "step": "reaction",
                            "channel": "reaction.loop",
                            "phase": "loop",
                            "attempt": 2,
                        },
                        "request_body": fallback_body,
                        "request_body_sha256": "sha-fallback",
                    }
                },
            },
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "provider_request_envelope": {
                        "schema": "provider_request.v1",
                        "call": {
                            "step": "reaction",
                            "channel": "reaction.loop",
                            "phase": "loop",
                            "attempt": 2,
                        },
                        "request_body": fallback_body,
                        "request_body_sha256": "sha-fallback",
                    }
                },
            },
        ],
    )

    report = analyzer.analyze_round_files([round_file])

    assert report["issues"] == []
    assert {
        call["request_body_sha256"]
        for call in report["calls"]
    } == {"sha-primary", "sha-fallback"}
    fallback_call = next(
        call for call in report["calls"]
        if call["request_body_sha256"] == "sha-fallback"
    )
    assert fallback_call["source_event_type"] == "llm_output_raw"


def test_missing_step_snapshots_returns_empty_summary(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    round_file = _write_round_jsonl(
        tmp_path,
        8,
        [
            {"event_type": "round_started", "payload": {"round_type": "interactive"}},
            {"event_type": "round_closed", "payload": {"status": "closed"}},
        ],
    )

    report = analyzer.analyze_round_files([round_file])

    assert report["summary"]["call_count"] == 0
    assert report["summary"]["weighted_prefix_ratio"] == 0.0
    assert report["calls"] == []
    assert report["issues"] == ["no_step_input_snapshots"]


def test_summary_path_selects_round_names_from_dogfood_results(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    round_file = _write_round_jsonl(
        tmp_path,
        9,
        [
            {
                "event_type": "step_input_snapshot",
                "phase": "setup",
                "iteration": 1,
                "payload": {"messages": [{"role": "user", "content": "selected"}]},
            },
        ],
    )
    _write_round_jsonl(
        tmp_path,
        10,
        [
            {
                "event_type": "step_input_snapshot",
                "phase": "setup",
                "iteration": 1,
                "payload": {"messages": [{"role": "user", "content": "ignored"}]},
            },
        ],
    )
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {"results": [{"round_name": round_file.name}, {"round_name": ""}]},
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    selected = analyzer.select_round_files(
        round_dir=round_file.parent,
        summary_path=summary_path,
    )
    report = analyzer.analyze_round_files(selected)

    assert [path.name for path in selected] == ["round_9.jsonl"]
    assert report["summary"]["call_count"] == 1
    assert report["calls"][0]["round_name"] == "round_9.jsonl"


def test_update_summary_writes_prompt_prefix_cache_headline(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    round_file = _write_round_jsonl(
        tmp_path,
        12,
        [
            {
                "event_type": "step_input_snapshot",
                "phase": "setup",
                "iteration": 1,
                "payload": {"messages": [{"role": "user", "content": "first"}]},
            },
            {
                "event_type": "step_input_snapshot",
                "phase": "reaction",
                "iteration": 1,
                "payload": {"messages": [{"role": "user", "content": "first plus"}]},
            },
        ],
    )
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps({"schema_version": "dogfood-test.v1", "results": []}),
        encoding="utf-8",
    )
    report = analyzer.analyze_round_files([round_file])

    analyzer.update_dogfood_summary(
        summary_path,
        report,
        artifacts_dir=tmp_path / "prefix-cache",
    )
    updated = json.loads(summary_path.read_text(encoding="utf-8"))

    assert updated["schema_version"] == "dogfood-test.v1"
    assert updated["prompt_prefix_cache"]["call_count"] == 2
    assert updated["prompt_prefix_cache"]["compared_call_count"] == 1
    assert updated["prompt_prefix_cache"]["artifacts"] == {
        "calls_jsonl": "prefix-cache/prefix_cache_calls.jsonl",
        "summary_json": "prefix-cache/prefix_cache_summary.json",
        "summary_md": "prefix-cache/prefix_cache_summary.md",
    }


def test_provider_cache_summary_reads_dogfood_summary_provider_calls(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    summary_path = tmp_path / "summary.json"
    summary_path.write_text(
        json.dumps(
            {
                "schema_version": "visible-dogfood.v1",
                "results": [
                    {
                        "round_name": "round_601.jsonl",
                        "provider_calls": [
                            {
                                "phase": "setup",
                                "call_channel": "setup",
                                "provider": "openai_chat",
                                "model": "gpt-5.4",
                                "request_body_sha256": "sha-setup",
                                "usage": {
                                    "prompt_tokens": 2048,
                                    "prompt_tokens_details": {"cached_tokens": 0},
                                },
                            },
                            {
                                "phase": "reaction",
                                "call_channel": "reaction.loop",
                                "provider": "openai_chat",
                                "model": "gpt-5.4",
                                "request_body_sha256": "sha-reaction",
                                "usage": {
                                    "prompt_tokens": 4096,
                                    "prompt_tokens_details": {"cached_tokens": 3072},
                                },
                            },
                        ],
                    }
                ],
            },
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )

    report = analyzer.analyze_provider_cache_summary(summary_path)

    assert report["summary"]["call_count"] == 2
    assert report["summary"]["prompt_tokens"] == 6144
    assert report["summary"]["cached_tokens"] == 3072
    assert report["summary"]["cache_write_tokens"] == 0
    assert report["summary"]["cache_write_status_counts"] == {"not_reported": 2}
    assert report["summary"]["cache_accounting_complete"] is False
    assert report["summary"]["cache_hit_ratio"] == 0.5
    assert report["summary"]["zero_cache_call_count"] == 1
    assert report["summary"]["nonzero_cache_call_count"] == 1
    assert report["summary"]["by_call_channel"]["reaction.loop"] == {
        "call_count": 1,
        "prompt_tokens": 4096,
        "cached_tokens": 3072,
        "cache_read_tokens": 3072,
        "cache_write_tokens": 0,
        "unclassified_prompt_tokens": 1024,
        "cache_hit_ratio": 0.75,
        "cache_write_ratio": 0.0,
        "zero_cache_call_count": 0,
        "nonzero_cache_call_count": 1,
        "cache_write_status_counts": {"not_reported": 1},
        "cache_accounting_complete": False,
    }


def test_provider_cache_summary_reads_historical_v1_telemetry(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    summary_path = tmp_path / "historical-v1.json"
    summary_path.write_text(
        json.dumps({
            "results": [{
                "round_name": "round_435.jsonl",
                "provider_calls": [{
                    "phase": "reaction",
                    "call_channel": "reaction.loop",
                    "prompt_cache_telemetry": {
                        "schema_version": "prompt_cache_telemetry.v1",
                        "prompt_tokens": 4096,
                        "cached_tokens": 3072,
                        "cache_read_tokens": 3072,
                        "cache_creation_tokens": 0,
                        "cache_hit_ratio": 0.75,
                    },
                }],
            }],
        }),
        encoding="utf-8",
    )

    report = analyzer.analyze_provider_cache_summary(summary_path)

    assert report["summary"]["cache_read_tokens"] == 3072
    assert report["summary"]["cache_write_tokens"] == 0
    assert report["summary"]["unclassified_prompt_tokens"] == 1024
    assert report["summary"]["cache_write_status_counts"] == {"not_reported": 1}
    assert report["summary"]["cache_accounting_complete"] is False


def test_same_lane_layer_churn_and_prompt_cache_key_hygiene(tmp_path):
    analyzer = _load_prompt_prefix_cache_analyzer()
    first_body = {
        "model": "gpt-5.4",
        "messages": [{"role": "user", "content": "stable request A"}],
    }
    second_body = {
        "model": "gpt-5.4-mini",
        "messages": [{"role": "user", "content": "stable request B"}],
    }
    round_file = _write_round_jsonl(
        tmp_path,
        602,
        [
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 1,
                "payload": {
                    "model": "gpt-5.4",
                    "request_contract_audit": {
                        "provider": "openai_chat",
                        "model": "gpt-5.4",
                        "prompt_cache_lane": "reaction_loop_tools",
                        "prompt_cache_key": "upsp:reaction_loop_tools",
                        "prompt_cache_key_applied": True,
                    },
                    "provider_request_envelope": {
                        "schema": "provider_request.v1",
                        "provider": {
                            "provider": "openai_chat",
                            "model": "gpt-5.4",
                        },
                        "call": {"channel": "reaction.loop"},
                        "request_body": first_body,
                        "request_body_sha256": "sha-a",
                    },
                    "layers_snapshot": {
                        "schema": "context_layers_snapshot.v1",
                        "layers": [
                            {"layer_key": "10_permanent", "sha256": "stable"},
                            {"layer_key": "40_high_freq", "sha256": "hf-a"},
                            {"layer_key": "50_now", "sha256": "now-a"},
                        ],
                    },
                },
            },
            {
                "event_type": "llm_output_raw",
                "phase": "reaction",
                "iteration": 2,
                "payload": {
                    "model": "gpt-5.4-mini",
                    "request_contract_audit": {
                        "provider": "openai_chat",
                        "model": "gpt-5.4-mini",
                        "prompt_cache_lane": "reaction_loop_tools",
                        "prompt_cache_key": "upsp:reaction_loop_tools",
                        "prompt_cache_key_applied": True,
                    },
                    "provider_request_envelope": {
                        "schema": "provider_request.v1",
                        "provider": {
                            "provider": "openai_chat",
                            "model": "gpt-5.4-mini",
                        },
                        "call": {"channel": "reaction.loop"},
                        "request_body": second_body,
                        "request_body_sha256": "sha-b",
                    },
                    "layers_snapshot": {
                        "schema": "context_layers_snapshot.v1",
                        "layers": [
                            {"layer_key": "10_permanent", "sha256": "stable"},
                            {"layer_key": "40_high_freq", "sha256": "hf-b"},
                            {"layer_key": "50_now", "sha256": "now-b"},
                        ],
                    },
                },
            },
        ],
    )

    report = analyzer.analyze_round_files([round_file])
    second = report["calls"][1]

    assert second["same_lane_previous_round_name"] == "round_602.jsonl"
    assert second["same_lane_first_changed_layer"] == "40_high_freq"
    assert second["same_lane_changed_layers"] == ["40_high_freq", "50_now"]
    assert report["summary"]["layer_churn"]["by_layer"]["40_high_freq"] == {
        "compared_count": 1,
        "changed_count": 1,
    }
    assert report["summary"]["by_prompt_cache_key"]["upsp:reaction_loop_tools"][
        "call_count"
    ] == 2
    assert "prompt_cache_key_crosses_model:upsp:reaction_loop_tools" in report["issues"]
