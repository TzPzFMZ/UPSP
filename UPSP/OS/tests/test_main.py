import os
import sys


sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def test_build_pending_report_is_read_only(tmp_path):
    from data.state_store import StateStore
    from main import build_pending_report

    sm = StateStore(str(tmp_path / "state.json"))
    sm.init_if_missing()
    sm.set_flag("api_degraded", True)

    report = build_pending_report(sm)

    assert report["active_flags"] == ["api_degraded"]
    assert report["round_type"] == "rhythm"
    assert sm.get_flags()["api_degraded"] is True
