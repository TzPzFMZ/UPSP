"""Read-only helpers for the OS round audit viewer."""
import json
import os
import re
from datetime import datetime, timezone

from data.round_audit_codec import read_round_audit_file


def _coerce_round_num(round_num):
    if isinstance(round_num, int):
        if round_num < 0:
            raise ValueError("round_num must be non-negative")
        return round_num
    value = str(round_num).strip()
    if not re.fullmatch(r"\d+", value):
        raise ValueError("round_num must be digits only")
    return int(value)


def list_rounds(round_dir):
    if not os.path.isdir(round_dir):
        return []
    rounds = []
    for name in os.listdir(round_dir):
        match = re.fullmatch(r"round_(\d+)\.jsonl", name)
        if not match:
            continue
        path = os.path.join(round_dir, name)
        try:
            stat = os.stat(path)
        except OSError:
            continue
        rounds.append({
            "round": int(match.group(1)),
            "file": name,
            "size": stat.st_size,
            "mtime": datetime.fromtimestamp(
                stat.st_mtime, timezone.utc
            ).astimezone().isoformat(timespec="seconds"),
        })
    rounds.sort(key=lambda item: item["round"])
    return rounds


def _round_path(round_dir, round_num):
    round_num = _coerce_round_num(round_num)
    filename = f"round_{round_num}.jsonl"
    path = os.path.abspath(os.path.join(round_dir, filename))
    root = os.path.abspath(round_dir)
    if os.path.commonpath([root, path]) != root:
        raise ValueError("round path escapes round_dir")
    return path


def latest_event_index(round_dir, round_num):
    path = _round_path(round_dir, round_num)
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    last_line = ""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                last_line = line
    if not last_line:
        return 0
    event = json.loads(last_line)
    return int(event.get("event_index") or 0)


def load_round_events(round_dir, round_num):
    path = _round_path(round_dir, round_num)
    if not os.path.isfile(path):
        raise FileNotFoundError(path)
    return read_round_audit_file(path)


def _event_text(event):
    payload = event.get("payload") or {}
    event_type = event.get("event_type")
    if event_type == "step_input_snapshot":
        snapshot = payload.get("layers_snapshot") or {}
        layers = snapshot.get("layers") if isinstance(snapshot, dict) else []
        if isinstance(layers, list):
            content = "\n\n".join(
                str(layer.get("content") or "")
                for layer in layers
                if isinstance(layer, dict)
            )
            if content:
                return content
        return "\n\n".join(
            str(message.get("content") or "")
            for message in payload.get("messages") or []
            if isinstance(message, dict)
        )
    if event_type == "llm_output_raw":
        return str(payload.get("response") or "")
    return json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)


def build_step_timeline(events):
    steps = []
    index = {}
    for event in events:
        phase = event.get("phase")
        if not phase:
            continue
        iteration = int(event.get("iteration") or 1)
        key = (phase, iteration)
        if key not in index:
            label = phase if phase != "reaction" else f"reaction_{iteration}"
            index[key] = {
                "label": label,
                "phase": phase,
                "iteration": iteration,
                "events": [],
                "text": "",
            }
            steps.append(index[key])
        step = index[key]
        step["events"].append(event)
        text = _event_text(event).strip()
        if text:
            step["text"] = (step["text"] + "\n\n" + text).strip()
        event_type = event.get("event_type")
        if event_type == "step_input_snapshot":
            step["input_snapshot"] = event.get("payload") or {}
        elif event_type == "llm_output_raw":
            step["llm_output_raw"] = event.get("payload") or {}
        elif event_type == "llm_output_parsed":
            step["llm_output_parsed"] = event.get("payload") or {}
        elif event_type == "step_settlement":
            step["step_settlement"] = event.get("payload") or {}
    return steps
