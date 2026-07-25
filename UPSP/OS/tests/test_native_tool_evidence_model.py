from __future__ import annotations

import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from native_tool_test_helpers import (
    _load_native_round_evidence_summary,
    _load_native_tool_calling_acceptance,
    _load_native_tool_calling_gate,
    _load_native_tool_evidence_model,
)


def test_spec309_evidence_model_sorts_recent_rounds_by_round_number(tmp_path):
    model = _load_native_tool_evidence_model()
    round_dir = tmp_path / "round"
    round_dir.mkdir()
    for round_num in (9, 11, 10):
        path = round_dir / f"round_{round_num}.jsonl"
        path.write_text("{}\n", encoding="utf-8")

    recent = model.find_recent_rounds(round_dir, 2)

    assert [path.name for path in recent] == ["round_10.jsonl", "round_11.jsonl"]
    assert model.round_allow_keys(round_dir / "round_11.jsonl") >= {
        "11",
        "round_11",
        "round_11.jsonl",
    }


def test_spec309_evidence_model_redacts_sensitive_summary_values():
    model = _load_native_tool_evidence_model()

    assert model.safe_text("arguments_json sk-secret-value") == "[redacted]"
    assert model.safe_list(["ok", "token=secret"]) == ["ok", "[redacted]"]
    assert model.safe_dict({"path": "OS/tests/x.py", "api_key": "sk-secret"}) == {
        "path": "OS/tests/x.py",
        "[redacted]": "[redacted]",
    }
    assert model.safe_count_dict({"ok": "2", "bad": "not-int"}) == {"ok": 2}
    assert model.parse_json_object(json.dumps({"ok": True})) == {"ok": True}


def test_spec309_native_gate_and_summary_use_shared_evidence_model_source():
    gate = _load_native_tool_calling_gate()
    summary = _load_native_round_evidence_summary()
    acceptance = _load_native_tool_calling_acceptance()

    assert "native_tool_evidence_model" in gate.__dict__
    assert "native_tool_evidence_model" in summary.__dict__
    assert "native_tool_evidence_model" in acceptance.__dict__
