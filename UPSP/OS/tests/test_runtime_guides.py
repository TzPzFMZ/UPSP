import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def test_spec357_main_axis_guide_renders_as_single_non_empty_popup():
    from logic.rhythm_guidance import render_current_guide_popup

    popup = render_current_guide_popup({"rhythm_due": True})

    assert popup.count("GUIDE｜") == 1
    assert "主轴节律指南" in popup
    assert "[ ]" in popup
    assert "option_id=write_chronicle" in popup
    assert "切到" not in popup


def test_spec357_calendar_guide_renders_due_layers_without_followup_flow():
    from logic.rhythm_guidance import render_current_guide_popup

    popup = render_current_guide_popup({
        "calendar_day_due": True,
        "calendar_week_due": True,
        "user_message_waiting": True,
    })

    assert popup.count("GUIDE｜") == 1
    assert "日历节律指南" in popup
    assert "日志" in popup
    assert "item_id=calendar_day_due" in popup
    assert "周志" not in popup
    assert "item_id=calendar_week_due" not in popup
    assert "option_id=write_chronicle" in popup
    assert "fields.content" in popup
    assert "用户交互指南" not in popup
    assert "切到" not in popup


def test_spec357_interaction_guide_renders_when_no_higher_guide_exists():
    from logic.rhythm_guidance import render_current_guide_popup

    popup = render_current_guide_popup({"user_message_waiting": True})

    assert popup.count("GUIDE｜") == 1
    assert "用户交互指南" in popup
    assert "[ ]" in popup


def test_spec363_current_guide_can_render_completed_items():
    from logic.rhythm_guidance import render_current_guide_popup

    popup = render_current_guide_popup(
        {"calendar_day_due": True, "calendar_week_due": True},
        completed_flags={"calendar_day_due"},
    )

    assert popup.count("GUIDE｜") == 1
    assert "[x] 日志" not in popup
    assert "[ ] 周志" in popup
    assert "item_id=calendar_week_due" in popup


def test_spec366_current_guide_skips_completed_main_axis_to_calendar():
    from logic.rhythm_guidance import render_current_guide_popup

    popup = render_current_guide_popup(
        {
            "rhythm_due": True,
            "calendar_day_due": True,
            "user_message_waiting": True,
        },
        completed_flags={"rhythm_due"},
    )

    assert popup.count("GUIDE｜") == 1
    assert "日历节律指南" in popup
    assert "主轴节律指南" not in popup
    assert "用户交互指南" not in popup


def test_spec366_current_guide_skips_completed_calendar_to_interaction():
    from logic.rhythm_guidance import render_current_guide_popup

    popup = render_current_guide_popup(
        {
            "rhythm_due": True,
            "calendar_day_due": True,
            "user_message_waiting": True,
        },
        completed_flags={"rhythm_due", "calendar_day_due"},
    )

    assert popup.count("GUIDE｜") == 1
    assert "用户交互指南" in popup
    assert "日历节律指南" not in popup
    assert "主轴节律指南" not in popup


def test_current_guide_ignores_reserved_process_flag():
    from logic.rhythm_guidance import render_current_guide_popup

    popup = render_current_guide_popup(
        {
            "api_degraded": True,
            "process_down": True,
            "rhythm_due": True,
        },
        completed_flags={"api_degraded"},
    )

    assert "process_down" not in popup
    assert "进程异常处理" not in popup
    assert "API 异常处理" not in popup
    assert "主轴节律指南" in popup


def test_spec366_runtime_guide_popup_reads_transient_completed_flags():
    from assembly.context_helpers import build_current_runtime_guide_popup

    popup = build_current_runtime_guide_popup(
        "reaction",
        state={
            "base": {
                "heartbeat_flags": {
                    "rhythm_due": True,
                    "calendar_day_due": True,
                },
                "runtime": {
                    "guide_completed_flags": ["rhythm_due"],
                },
            },
        },
    )

    assert "日历节律指南" in popup
    assert "主轴节律指南" not in popup


def test_spec464_materializes_main_axis_runtime_guide_into_workbench(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))

    guide_id = materialize_current_rhythm_guide(
        store,
        {"rhythm_due": True},
        round_num=464,
    )

    assert guide_id == "rhythm:main_axis:R000464"
    assert store.get("base.active_guides.rhythm") == guide_id
    guide = store.load_active_guide()
    assert guide["kind"] == "main_axis_rhythm_guide"
    assert guide["items"][0]["item_id"] == "rhythm_due"
    assert guide["items"][0]["options"][0]["option_id"] == "write_chronicle"
