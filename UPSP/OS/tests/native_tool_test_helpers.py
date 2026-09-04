import importlib.util
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def _load_module_from_path(module_name, module_path, *, register=False):
    spec = importlib.util.spec_from_file_location(module_name, os.fspath(module_path))
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load {module_name} from {module_path}")
    module = importlib.util.module_from_spec(spec)
    if register:
        sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


def _load_tool_script(module_name, file_name):
    script_path = os.path.abspath(os.path.join(
        os.path.dirname(os.path.abspath(__file__)),
        "..",
        "..",
        "..",
        "tools",
        file_name,
    ))
    return _load_module_from_path(module_name, script_path)


def _load_native_round_inspector():
    return _load_tool_script("inspect_native_tool_round", "inspect_native_tool_round.py")


def _load_native_tool_calling_gate():
    return _load_tool_script("check_native_tool_calling_gate", "check_native_tool_calling_gate.py")


def _load_native_tool_evidence_model():
    return _load_tool_script("native_tool_evidence_model", "native_tool_evidence_model.py")


def _load_dogfood_runner_support():
    return _load_tool_script("dogfood_runner_support", "dogfood_runner_support.py")


def _load_prompt_prefix_cache_analyzer():
    return _load_tool_script(
        "analyze_prompt_prefix_cache",
        "analyze_prompt_prefix_cache.py",
    )


def _load_native_round_evidence_summary():
    return _load_tool_script("summarize_native_round_evidence", "summarize_native_round_evidence.py")


def _load_high_risk_general_tool_gate():
    return _load_tool_script("check_high_risk_general_tool_gate", "check_high_risk_general_tool_gate.py")


def _load_native_tool_calling_acceptance():
    return _load_tool_script("check_native_tool_calling_acceptance", "check_native_tool_calling_acceptance.py")


def _load_dogfood_visible_report():
    return _load_tool_script("dogfood_visible_report", "dogfood_visible_report.py")


def _write_round_jsonl(tmp_path, events, round_num=143):
    round_dir = tmp_path / "round"
    round_dir.mkdir()
    path = round_dir / f"round_{round_num}.jsonl"
    with path.open("w", encoding="utf-8", newline="\n") as f:
        for index, event in enumerate(events, start=1):
            record = {
                "schema_version": "round_audit.v1",
                "round": round_num,
                "event_index": index,
                "event_id": f"R{round_num:06d}-{index:06d}",
                "recorded_at": "2026-06-01T10:00:00+08:00",
            }
            record.update(event)
            f.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    return path


class _FakeCompletedProcess:
    def __init__(self, returncode=0, stdout="", stderr=""):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr
