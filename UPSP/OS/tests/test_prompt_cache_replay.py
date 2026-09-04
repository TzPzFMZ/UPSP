from __future__ import annotations

import json
from pathlib import Path
from UPSP.OS.tests.native_tool_test_helpers import _load_module_from_path


def _load_replay_module():
    path = Path(__file__).resolve().parents[3] / "tools" / (
        "replay_prompt_cache_breakpoints.py"
    )
    return _load_module_from_path('spec616_cache_replay', path)


def _event(index: int, lately: str, now: str) -> dict:
    contents = {
        "10_permanent": "P" * 5000,
        "20_periodic": "D" * 5000,
        "30_lately": lately,
        "40_high_freq": "high",
        "50_now": [{"role": "user", "content": now}],
        "60_statusbar": "status-" + now,
        "99_popup": "popup-" + now,
    }
    messages = [
        {"role": "system", "content": contents["10_permanent"]},
        {"role": "system", "content": contents["20_periodic"]},
    ]
    if lately:
        messages.append({"role": "system", "content": lately})
    messages.extend([
        {"role": "system", "content": contents["40_high_freq"]},
        {"role": "user", "content": now},
        {"role": "system", "content": contents["60_statusbar"]},
        {"role": "system", "content": contents["99_popup"]},
    ])
    layers = []
    for order, (key, content) in enumerate(contents.items()):
        layers.append({
            "layer_key": key,
            "content": content,
            "sha256": f"{key}:{hash(json.dumps(content, sort_keys=True))}",
            "order": order,
        })
    return {
        "event_type": "step_input_snapshot",
        "event_index": index,
        "iteration": index,
        "phase": "reaction",
        "payload": {
            "layers_snapshot": {"layers": layers},
            "provider_request_envelope": {
                "request_body": {
                    "model": "gpt-5.6-sol",
                    "messages": messages,
                    "tools": [{"type": "function", "function": {"name": "x"}}],
                }
            },
        },
    }


def test_replay_tracks_deterministic_b1_epochs_and_tail_churn(tmp_path):
    replay = _load_replay_module()
    path = tmp_path / "round_1.jsonl"
    events = [
        _event(1, "L1" * 2500, "n1"),
        _event(2, "L1" * 2500, "n2"),
        _event(3, "L2" * 2500, "n3"),
        _event(4, "L2" * 2500, "n4"),
    ]
    path.write_text(
        "".join(json.dumps(event, ensure_ascii=False) + "\n" for event in events),
        encoding="utf-8",
    )

    first = replay.replay_round(path)
    second = replay.replay_round(path)

    assert first == second
    assert first["reaction_calls"] == 4
    assert first["fixed_only"] == {
        "unique_prefixes": 1,
        "estimated_writes": 1,
        "estimated_reads": 3,
    }
    assert first["tiered_b1"] == {
        "unique_epochs": 2,
        "estimated_writes": 2,
        "estimated_reads": 2,
        "calls_without_b1": 0,
        "b0_fallbacks": 1,
    }
    assert first["lately_changes"] == 1
    assert first["implicit_latest_breakpoint_potential_writes"] == 4
    assert [run["consecutive_calls"] for run in first["tiered_context_epoch_runs"]] == [
        2,
        2,
    ]
    assert first["calls"][1]["tiered"]["consecutive_reuse_ordinal"] == 2


def test_replay_uses_periodic_when_lately_is_empty(tmp_path):
    replay = _load_replay_module()
    path = tmp_path / "round_2.jsonl"
    path.write_text(json.dumps(_event(1, "", "n1")) + "\n", encoding="utf-8")

    result = replay.replay_round(path)

    assert result["calls"][0]["tiered"]["targets"] == [
        "10_permanent",
        "20_periodic",
    ]
    assert result["tiered_b1"]["estimated_writes"] == 1
