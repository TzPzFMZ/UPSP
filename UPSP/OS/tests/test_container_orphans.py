"""悬空判定、Round 边界与警告替换的回归。"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent))

from logic.container_ops import check_orphan, scan_orphans, replace_orphan_popup, ORPHAN_POPUP_HEADER, ORPHAN_POPUP_FOOTER
from runtime_test_helpers import RuntimeTestMixin


START = datetime(2026, 9, 10, 12).astimezone()


@pytest.mark.parametrize("linked,status,orphan", [
    (["MEM-12345678"], "active", False), ([], "active", True),
    ([], "expired", False),
])
def test_skill_uses_current_memory_links(linked, status, orphan):
    info = {"id": "SKL-procedures-test", "status": status,
            "linked_memories": linked, "created_at": (START-timedelta(days=1)).isoformat()}
    assert check_orphan(info, START)[0] is orphan


@pytest.mark.parametrize("created,start,orphan", [
    ((START+timedelta(minutes=1)).isoformat(), START.replace(tzinfo=None), False),
    ((START-timedelta(hours=4)).replace(tzinfo=None).isoformat(), START, True),
    ((START+timedelta(minutes=1)).astimezone(timezone.utc).isoformat(), START, False),
    (START.isoformat(), START, False),
    ((START-timedelta(seconds=1)).isoformat(), START, True),
    ((START-timedelta(days=1)).isoformat(), START, True),
])
def test_new_container_grace_uses_round_start_and_local_legacy_time(created, start, orphan):
    assert check_orphan({"id": "DC-1", "entries": [], "created_at": created}, start)[0] is orphan


def test_invalid_timestamp_and_missing_round_start_are_not_clean_scans():
    with pytest.raises(ValueError, match="created_at"):
        check_orphan({"id": "DC-1", "entries": [], "created_at": "broken"}, START)
    with pytest.raises(ValueError, match="round_start"):
        scan_orphans(SimpleNamespace(list_containers=lambda **kw: []), None)


def test_orphan_block_replacement_preserves_adjacent_and_incomplete_warnings():
    orphan = f"{ORPHAN_POPUP_HEADER}\n  DC-1\n\n{ORPHAN_POPUP_FOOTER}"
    other = "- kind: structure_warning\n  message: 保留工具错误"
    assert replace_orphan_popup(other+"\r\n"+orphan+"\r\n后续提醒", None) == other+"\r\n后续提醒"
    incomplete = ORPHAN_POPUP_HEADER+"\n未知截断内容\n"+other
    assert replace_orphan_popup(incomplete, None) == incomplete


def test_popup_update_read_failure_preserves_file(tmp_path, monkeypatch):
    from data.popup_store import PopupStore
    store = PopupStore(str(tmp_path / "popup.md"))
    store.write_popup("原始警告")
    with monkeypatch.context() as scoped:
        scoped.setattr("builtins.open", lambda *a, **k: (_ for _ in ()).throw(PermissionError("read denied")))
        with pytest.raises(PermissionError):
            store.update_popup(lambda current: "新警告")
    assert store.read_popup() == "原始警告"


def test_cleanup_updates_only_orphan_warning_and_retains_it_on_failure(tmp_path, monkeypatch):
    helper = RuntimeTestMixin()
    rt = helper._make_runtime(tmp_path)
    rt.audit.start(807, "interactive")
    info = {"id": "DC-1", "entries": [], "created_at": (START-timedelta(days=1)).isoformat()}
    rt.container_store = SimpleNamespace(
        list_containers=lambda prefix: [info] if prefix == "DC-" else [])
    other = "[!!! 安全 2026-09-10]\n保留这条独立安全提示。"
    rt.assembler.popup.write_popup(other)
    terminal = {"tool_call_envelopes": [helper._native_tool_envelope(
        "cleanup_finalize", {"connection_bridges": [], "tacit_associations": []})]}

    def cleanup():
        return rt._process_cleanup_output(terminal, 807, rt.sm.load(), {}, current_round_start=START)

    for _ in range(2):
        report = cleanup()
        assert report["errors"] == []
        text = rt.assembler.popup.read_popup()
        assert other in text and text.count(ORPHAN_POPUP_HEADER) == 1
    before = rt.assembler.popup.read_popup()
    info["created_at"] = "broken"
    assert cleanup()["errors"]
    assert rt.assembler.popup.read_popup() == before
    info["created_at"] = (START-timedelta(days=1)).isoformat()
    info["entries"] = [{"mem_id": "MEM-12345678"}]
    from data import popup_store
    with monkeypatch.context() as scoped:
        scoped.setattr(popup_store, "atomic_write_text", lambda *a, **k: (_ for _ in ()).throw(OSError("write failed")))
        assert cleanup()["errors"]
        assert rt.assembler.popup.read_popup() == before
    assert cleanup()["popup"] is None
    assert rt.assembler.popup.read_popup() == other
    _, messages = rt.assembler.assemble_setup(
        rt.sm.load(), "interactive", user_messages=["检查"], interaction_meta=helper._confirmed_meta())
    assert "保留这条独立安全提示" in messages[-1]["content"]
    assert ORPHAN_POPUP_HEADER not in messages[-1]["content"]


def test_cleanup_does_not_extend_grace_to_the_whole_day(tmp_path):
    helper = RuntimeTestMixin()
    rt = helper._make_runtime(tmp_path)
    old_empty = {"id": "DC-1", "entries": [], "created_at": (START-timedelta(hours=4)).replace(tzinfo=None).isoformat()}
    new_empty = {"id": "DC-2", "entries": [], "created_at": (START+timedelta(minutes=1)).isoformat()}
    rt.container_store = SimpleNamespace(list_containers=lambda prefix: [old_empty, new_empty] if prefix == "DC-" else [])
    terminal = {"tool_call_envelopes": [helper._native_tool_envelope("cleanup_finalize", {})]}
    report = rt._process_cleanup_output(terminal, 807, rt.sm.load(), {}, current_round_start=START)
    assert report["errors"] == []
    assert [o["container_id"] for o in report["orphans"]] == ["DC-1"]
