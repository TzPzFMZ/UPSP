import json
import sys
from pathlib import Path

import pytest


OS_DIR = Path(__file__).resolve().parents[1]
REPO_ROOT = OS_DIR.parents[1]
sys.path.insert(0, str(OS_DIR))


def _config_store(tmp_path, monkeypatch):
    from data import config_store as module

    path = tmp_path / "system.json"
    monkeypatch.setitem(
        module._CONFIG_MAP,
        "system",
        (str(path), module._CONFIG_MAP["system"][1]),
    )
    return module, module.ConfigStore(), path


def test_spec763_migrates_legacy_round_base_to_explicit_stages(tmp_path, monkeypatch):
    module, store, path = _config_store(tmp_path, monkeypatch)
    legacy = module.default_system_config()
    legacy["_version"] = "Base-0.48.16"
    legacy["round"] = {"time_limit": 900}
    path.write_text(json.dumps(legacy), encoding="utf-8")

    migrated, changed = store.migrate_system_audit_policy()

    assert changed is True
    assert migrated["_version"] == "Base-0.48.17"
    assert migrated["round"] == {
        "reminder_seconds": 900,
        "warning_seconds": 1800,
        "auto_relay_seconds": 2700,
    }
    assert store.get_round_time_milestones() == (900, 1800, 2700)
    assert store.migrate_system_audit_policy()[1] is False


def test_spec763_migrates_largest_legacy_round_value(tmp_path, monkeypatch):
    module, store, path = _config_store(tmp_path, monkeypatch)
    legacy = module.default_system_config()
    legacy["_version"] = "Base-0.48.16"
    legacy["round"] = {"time_limit": 86400}
    path.write_text(json.dumps(legacy), encoding="utf-8")

    migrated, _changed = store.migrate_system_audit_policy()

    assert migrated["round"] == {
        "reminder_seconds": 86400,
        "warning_seconds": 172800,
        "auto_relay_seconds": 259200,
    }


@pytest.mark.parametrize("round_config", [
    {
        "time_limit": 600,
        "reminder_seconds": 600,
        "warning_seconds": 1200,
        "auto_relay_seconds": 1800,
    },
    {"reminder_seconds": 600, "warning_seconds": 600, "auto_relay_seconds": 1800},
    {"reminder_seconds": 600, "warning_seconds": 1200},
])
def test_spec763_rejects_mixed_missing_or_unordered_round_shape_without_overwrite(
        tmp_path, monkeypatch, round_config):
    module, store, path = _config_store(tmp_path, monkeypatch)
    invalid = module.default_system_config()
    invalid["_version"] = "Base-0.48.16"
    invalid["round"] = round_config
    original = json.dumps(invalid, ensure_ascii=False, separators=(",", ":"))
    path.write_text(original, encoding="utf-8")

    with pytest.raises(Exception):
        store.migrate_system_audit_policy()

    assert path.read_text(encoding="utf-8") == original


def test_spec763_explicit_milestones_drive_feedback_and_auto_relay():
    from engines.reaction_loop import ReactionLoopRunner
    from engines.reaction_terminal_state import build_runtime_auto_continue_closeout

    milestones = (700, 1300, 1900)
    assert not ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=699, time_limit_seconds=milestones)
    assert "【时间提醒】" in ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=700, time_limit_seconds=milestones)
    assert "【时间警告】" in ReactionLoopRunner._reaction_time_feedback(
        elapsed_seconds=1300, time_limit_seconds=milestones)

    class StateManager:
        def __init__(self):
            self.flags = {}

        def set_flag(self, key, value):
            self.flags[key] = value

    state = StateManager()
    relay, ledger, guard = build_runtime_auto_continue_closeout(
        state, elapsed_seconds=1901, time_limit_seconds=milestones)
    assert "1900秒" in relay["handoff_text"]
    assert ledger["reminder_seconds"] == 700
    assert ledger["warning_seconds"] == 1300
    assert guard["auto_relay_seconds"] == 1900
    assert state.flags["continue_requested"] is True


def test_spec763_calendar_day_guide_aggregates_real_backend_path(
        tmp_path, monkeypatch):
    from data.workbench import WorkbenchStore
    from data.memory_compression_store import MemoryCompressionManager
    from logic.guide_submit import apply_guide_submit

    class ChronicleStore:
        def __init__(self):
            self.writes = []

        def write_focused_entry(self, focus, content):
            self.writes.append((dict(focus), content))
            return str(tmp_path / "D-active-calendar.md")

    class StateStore:
        def __init__(self):
            self.flags = {}

        def set_flag(self, key, value):
            self.flags[key] = value

    monkeypatch.setattr(
        MemoryCompressionManager,
        "prepare_daily_cycle",
        lambda self, **kwargs: {
            "status": "applied",
            "date": kwargs["local_date"],
            "pending": 1,
        },
    )
    monkeypatch.setattr(MemoryCompressionManager, "has_active_cycle", lambda self: True)

    guide_id = "rhythm:calendar_day:R000650"
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": guide_id,
        "kind": "calendar_rhythm_guide",
        "guide_slot": "rhythm",
        "items": [{
            "item_id": "calendar_day_due",
            "options": [{
                "option_id": "write_chronicle",
                "required_fields": ["content", "reason"],
                "allowed_fields": ["content", "reason"],
            }],
        }],
    }, active=True)
    chronicle = ChronicleStore()
    state = StateStore()

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": guide_id,
            "submissions": [{
                "item_id": "calendar_day_due",
                "option_id": "write_chronicle",
                "fields": {"content": "R650 日志。", "reason": "daily rhythm"},
            }],
        },
        evidence_context={
            "round_num": 650,
            "chronicle_store": chronicle,
            "chronicle_focus": {
                "layer": "daily",
                "round_num": 650,
                "round_type": "rhythm",
                "source_refs": ["round:650"],
            },
            "state_store": state,
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["completed_flags"] == ["calendar_day_due"]
    assert receipt["reopened_flags"] == ["memory_compression_due"]
    assert [item["tool_id"] for item in receipt["backend_receipts"]] == [
        "chronicle_write", "memory_compression_daily_maintenance",
    ]
    assert chronicle.writes[0][1] == "R650 日志。"
    assert state.flags["memory_compression_due"] is True

    from engines.reaction_loop import ReactionLoopRunner

    completed = {"memory_compression_due"}
    completed.update(ReactionLoopRunner._guide_completed_flags_from_receipts(
        [receipt],
        completed_flags=completed,
    ))
    completed.difference_update(
        ReactionLoopRunner._guide_reopened_flags_from_receipts([receipt]))
    assert "calendar_day_due" in completed
    assert "memory_compression_due" not in completed


def test_spec763_rhythm_backend_result_rejects_without_applied_receipt():
    from logic.guide_submit import _backend_result

    result = _backend_result(
        "calendar_day_due",
        [{"tool_id": "chronicle_write", "status": "rejected"}],
        applied_tools={"chronicle_write"},
    )

    assert result["status"] == "rejected"
    assert result["reason"] == "rhythm_guide_backend_not_applied"
    assert "completed_flags" not in result


def test_spec763_calendar_day_maintenance_exception_is_not_swallowed(
        tmp_path, monkeypatch):
    from data.memory_compression_store import MemoryCompressionManager
    from logic.guide_submit import _apply_rhythm_guide_submission

    class ChronicleStore:
        def write_focused_entry(self, focus, content):
            return str(tmp_path / "D-active-calendar.md")

    def fail_maintenance(self, **kwargs):
        raise RuntimeError("daily-maintenance-failed")

    monkeypatch.setattr(
        MemoryCompressionManager,
        "prepare_daily_cycle",
        fail_maintenance,
    )

    with pytest.raises(RuntimeError, match="daily-maintenance-failed"):
        _apply_rhythm_guide_submission(
            {"kind": "calendar_rhythm_guide"},
            {
                "item_id": "calendar_day_due",
                "option_id": "write_chronicle",
                "fields": {"content": "日节律日志。", "reason": "daily rhythm"},
            },
            evidence_context={
                "round_num": 650,
                "chronicle_store": ChronicleStore(),
                "chronicle_focus": {
                    "layer": "daily",
                    "round_num": 650,
                    "round_type": "rhythm",
                    "source_refs": ["round:650"],
                },
            },
        )


def test_spec763_gui_exposes_three_time_fields_and_no_legacy_label():
    view = (REPO_ROOT / "UPSP" / "gui" / "src" / "view.ts").read_text(
        encoding="utf-8")
    server = (REPO_ROOT / "tools" / "serve_seed_gui.py").read_text(
        encoding="utf-8")
    for key in (
        "round.reminder_seconds",
        "round.warning_seconds",
        "round.auto_relay_seconds",
    ):
        assert key in view
        assert key in server
    assert 'label: "轮次时限"' not in view
    assert '"round.time_limit": _setting' not in server
