from __future__ import annotations

import json
from pathlib import Path

import pytest

from data.round_audit_static import prune_stale_round_js, write_static_round_index
from data.round_retention import RoundRetentionError, enforce_round_retention


def _rounds(root, count, size=10):
    root.mkdir(parents=True, exist_ok=True)
    for number in range(1, count + 1):
        (root / f"round_{number}.jsonl").write_bytes(b"x" * size)


@pytest.mark.parametrize("count", [0, 1, 8])
def test_fifo8_noop_and_idempotent(tmp_path, count):
    root = tmp_path / "round"
    _rounds(root, count)
    first = enforce_round_retention(root, retention_count=8, max_mib=256)
    second = enforce_round_retention(root, retention_count=8, max_mib=256)
    assert first["after"]["count"] == count
    assert first["deleted"] == []
    assert second["deleted"] == []


def test_missing_round_directory_is_not_created(tmp_path):
    root = tmp_path / "uninitialized" / "STM" / "context" / "round"

    receipt = enforce_round_retention(root, retention_count=8, max_mib=256)

    assert receipt["status"] == "ok"
    assert receipt["before"] == {"count": 0, "bytes": 0}
    assert not root.exists()


@pytest.mark.parametrize("count,removed", [(9, 1), (59, 51)])
def test_fifo8_removes_oldest_numeric_rounds(tmp_path, count, removed):
    root = tmp_path / "round"
    _rounds(root, count)
    receipt = enforce_round_retention(root, retention_count=8, max_mib=256)
    assert [item["round"] for item in receipt["deleted"]] == list(range(1, removed + 1))
    assert sorted(int(path.stem.split("_")[1]) for path in root.glob("round_*.jsonl")) == list(range(removed + 1, count + 1))


def test_byte_limit_can_keep_fewer_than_count_but_never_deletes_latest(tmp_path):
    root = tmp_path / "round"
    _rounds(root, 4, size=600_000)
    receipt = enforce_round_retention(root, retention_count=8, max_mib=1)
    assert receipt["after"]["count"] == 1
    assert (root / "round_4.jsonl").is_file()
    assert receipt["soft_overflow"] is False

    (root / "round_4.jsonl").write_bytes(b"x" * 1_100_000)
    overflow = enforce_round_retention(root, retention_count=8, max_mib=1)
    assert overflow["after"]["count"] == 1
    assert overflow["soft_overflow"] is True


def test_only_managed_files_are_deleted_and_receipt_is_written(tmp_path):
    root = tmp_path / "round"
    _rounds(root, 9)
    unmanaged = root / "notes.jsonl"
    unmanaged.write_text("keep", encoding="utf-8")
    receipt_path = tmp_path / "receipt.json"
    receipt = enforce_round_retention(
        root,
        retention_count=8,
        max_mib=256,
        receipt_path=receipt_path,
    )
    assert unmanaged.read_text(encoding="utf-8") == "keep"
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["after"] == receipt["after"]


def test_delete_failure_is_reported_and_not_swallowed(tmp_path, monkeypatch):
    root = tmp_path / "round"
    _rounds(root, 9)
    receipt_path = tmp_path / "receipt.json"

    def fail(_path):
        raise PermissionError("locked")

    monkeypatch.setattr("data.round_retention.os.remove", fail)
    with pytest.raises(RoundRetentionError, match="round_1.jsonl"):
        enforce_round_retention(
            root,
            retention_count=8,
            max_mib=256,
            receipt_path=receipt_path,
        )
    assert json.loads(receipt_path.read_text(encoding="utf-8"))["status"] == "error"


def test_invalid_active_policy_fails_closed_with_receipt(tmp_path, monkeypatch):
    from data import config_store, round_retention
    import paths

    system = tmp_path / "system.json"
    system.write_text("{}", encoding="utf-8")
    receipt = tmp_path / "receipt.json"
    monkeypatch.setitem(
        config_store._CONFIG_MAP,
        "system",
        (str(system), config_store._CONFIG_MAP["system"][1]),
    )
    monkeypatch.setattr(paths, "STM_CTX_ROUND_DIR", str(tmp_path / "round"))
    monkeypatch.setattr(
        round_retention,
        "active_round_retention_receipt_path",
        lambda: receipt,
    )

    with pytest.raises(RoundRetentionError):
        round_retention.enforce_active_round_retention()

    result = json.loads(receipt.read_text(encoding="utf-8"))
    assert result["status"] == "error"
    assert result["stage"] == "policy_load"


def test_static_projection_prune_matches_retained_rounds(tmp_path):
    data_dir = tmp_path / "round-data"
    data_dir.mkdir()
    (data_dir / "round_1.js").write_text("old", encoding="utf-8")
    (data_dir / "round_2.js").write_text("keep", encoding="utf-8")
    (data_dir / "notes.js").write_text("unmanaged", encoding="utf-8")

    deleted = prune_stale_round_js(data_dir, {"round_2.js"})

    assert [item["file"] for item in deleted] == ["round_1.js"]
    assert not (data_dir / "round_1.js").exists()
    assert (data_dir / "round_2.js").is_file()
    assert (data_dir / "notes.js").is_file()


def test_static_index_is_rebuilt_from_retained_rounds(tmp_path):
    round_dir = tmp_path / "round"
    audit_dir = tmp_path / "audit"
    _rounds(round_dir, 2)
    data_dir = audit_dir / "round-data"
    data_dir.mkdir(parents=True)
    (data_dir / "round_1.js").write_text("one", encoding="utf-8")

    write_static_round_index(round_dir, audit_dir)

    text = (audit_dir / "round-index.js").read_text(encoding="utf-8")
    assert '"round": 1' in text
    assert '"round": 2' not in text
