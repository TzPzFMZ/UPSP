import json
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import pytest

from data.action_recovery_store import (
    ActionRecoveryEffectError,
    ActionRecoveryError,
    ActionRecoveryStore,
)
from logic.action_recovery import render_materials


def _store(tmp_path):
    return ActionRecoveryStore(tmp_path / "context" / "action_recovery_pending.json")


def _context(round_num=12, iteration=3):
    return {
        "round_num": round_num,
        "iteration": iteration,
        "frame_id": f"R{round_num:06d}:reaction:{iteration}",
    }


def _signature(value):
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()


def _prepare_file(store, target, *, before=b"before", candidate=b"after", call="write"):
    return store.prepare_file(
        tool_id="file_write",
        request_sha256=_signature(call),
        runtime_context=_context(),
        call_id=f"call-{call}",
        target_path=str(target),
        before_bytes=before,
        candidate_bytes=candidate,
    )


def test_file_action_uses_sha_journal_without_full_backup(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "output.txt"
    target.write_bytes(b"before")
    action_id = _prepare_file(store, target)
    prepared = store.load()["items"][0]
    assert prepared["phase"] == "prepared"
    assert prepared["before"]["bytes"] == 6
    assert prepared["candidate"]["bytes"] == 5
    assert not (tmp_path / "buffer" / "action_recovery").exists()
    invalid = store.load()
    invalid["items"][0]["resolved_closed_sequence"] = 1
    with pytest.raises(ActionRecoveryError, match="item_invalid"):
        store._normalize(invalid)
    invalid["items"][0]["resolved_closed_sequence"] = None
    invalid["items"][0]["phase"] = "settled"
    with pytest.raises(ActionRecoveryError, match="result_invalid"):
        store._normalize(invalid)
    store.commit_file(action_id, target, b"before", b"after")
    result = {"action_id": action_id, "status": "ok", "reason": ""}
    store.record_results([result])
    store.settle_results([result])
    assert target.read_bytes() == b"after"
    assert store.load()["items"][0]["phase"] == "settled"
    store.note_round_closed(12)
    assert store.load()["items"] == []


def test_public_file_targets_distinguish_same_basename_without_private_path(tmp_path):
    store = _store(tmp_path)
    first = tmp_path / "one" / "output.txt"
    second = tmp_path / "two" / "output.txt"
    first.parent.mkdir()
    second.parent.mkdir()
    first.write_bytes(b"before")
    second.write_bytes(b"before")
    _prepare_file(store, first, call="first")
    _prepare_file(store, second, call="second")

    targets = [item["target"] for item in store.load()["items"]]
    assert targets[0] != targets[1]
    assert all(value.startswith("output.txt#") for value in targets)
    assert str(first.parent) not in json.dumps(targets)
    assert str(second.parent) not in json.dumps(targets)


@pytest.mark.parametrize(
    ("before", "candidate", "current", "result_status", "expected"),
    [
        (b"before", b"after", b"after", "", "applied_unregistered"),
        (b"before", b"after", b"before", "", "not_applied"),
        (b"before", b"after", b"other", "", "conflict"),
        (b"before", b"after", None, "", "conflict"),
        (None, b"new", None, "", "not_applied"),
        (b"same", b"same", b"same", "", "not_applied"),
        (b"same", b"same", b"same", "failed", "not_applied"),
        (b"before", b"after", b"after", "failed", "applied_registered"),
    ],
)
def test_file_interruption_classifies_current_sha(
        tmp_path, before, candidate, current, result_status, expected):
    store = _store(tmp_path)
    target = tmp_path / "output.txt"
    if before is not None:
        target.write_bytes(before)
    action_id = _prepare_file(store, target, before=before, candidate=candidate)
    if current is None:
        target.unlink(missing_ok=True)
    else:
        target.write_bytes(current)
    if result_status:
        store.record_results([{
            "action_id": action_id, "status": result_status,
            "reason": "write_failed",
        }])
    store.classify_interrupted(12)
    assert store.pending_items()[0]["recovery_classification"] == expected
    if expected.startswith("applied_"):
        assert store.recovered_results()[0]["status"] == "ok"


def test_later_write_before_sha_proves_earlier_write(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "chain.txt"
    target.write_bytes(b"v1")
    first = _prepare_file(store, target, before=b"v1", candidate=b"v2", call="first")
    store.commit_file(first, target, b"v1", b"v2")
    second = store.prepare_file(
        tool_id="file_edit",
        request_sha256="2" * 64,
        runtime_context=_context(iteration=4),
        call_id="call-second",
        target_path=str(target),
        before_bytes=b"v2",
        candidate_bytes=b"v3",
    )
    store.commit_file(second, target, b"v2", b"v3")
    store.classify_interrupted(12)
    assert {
        item["action_id"]: item["recovery_classification"]
        for item in store.pending_items()
    } == {first: "applied_unregistered", second: "applied_unregistered"}


def test_later_write_does_not_turn_an_indistinguishable_noop_into_success(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "chain.txt"
    target.write_bytes(b"v1")
    noop = _prepare_file(store, target, before=b"v1", candidate=b"v1", call="noop")
    later = _prepare_file(store, target, before=b"v1", candidate=b"v2", call="later")
    store.commit_file(later, target, b"v1", b"v2")
    store.classify_interrupted(12)
    classifications = {item["action_id"]: item["recovery_classification"] for item in store.pending_items()}
    assert classifications == {noop: "conflict", later: "applied_unregistered"}


@pytest.mark.parametrize(
    ("phase", "status", "outcome", "reusable"),
    [
        ("prepared", "", "not_applied", False),
        ("launching", "", "outcome_unknown", True),
        ("result_recorded", "ok", "known_success", True),
        ("settled", "failed", "known_failure", False),
    ],
)
def test_opaque_crash_points_have_one_classification(
        tmp_path, monkeypatch, phase, status, outcome, reusable):
    store = _store(tmp_path)
    if phase == "prepared":
        monkeypatch.setattr(
            store, "_transition", lambda *_args, **_kwargs: (_ for _ in ()).throw(
                SystemExit("crash before launch")
            ),
        )
        with pytest.raises(SystemExit):
            store.prepare_opaque(
                tool_id="shell_command",
                request_sha256=_signature("opaque"),
                runtime_context=_context(),
                call_id="call-shell",
                target="shell",
            )
    else:
        action_id = store.prepare_opaque(
            tool_id="shell_command",
            request_sha256=_signature("opaque"),
            runtime_context=_context(),
            call_id="call-shell",
            target="shell",
        )
        if phase in {"result_recorded", "settled"}:
            result = {"action_id": action_id, "status": status, "reason": "boom"}
            store.record_results([result])
            if phase == "settled":
                store.settle_results([result])
    store.classify_interrupted(12)
    receipt = store.recovery_receipt()
    assert receipt["items"][0]["outcome"] == outcome
    assert bool(store.recovered_results()) is reusable


def test_real_process_exit_leaves_classifiable_metadata_only(tmp_path):
    source_root = str(Path(__file__).parents[1])
    script = r'''
import os
from pathlib import Path
from data.action_recovery_store import ActionRecoveryStore
root = Path(os.environ["ACTION_RECOVERY_TEST_ROOT"])
store = ActionRecoveryStore(root / "context" / "action_recovery_pending.json")
target = root / "output.txt"
target.write_bytes(b"before")
action = store.prepare_file(tool_id="file_write", request_sha256="a" * 64,
    runtime_context={"round_num": 12, "iteration": 3, "frame_id": "f"},
    call_id="write", target_path=str(target),
    before_bytes=b"before", candidate_bytes=b"after")
store.commit_file(action, target, b"before", b"after")
store.prepare_opaque(tool_id="shell_command", request_sha256="b" * 64,
    runtime_context={"round_num": 12, "iteration": 4, "frame_id": "g"},
    call_id="shell", target="shell")
os._exit(91)
'''
    env = os.environ.copy()
    env["PYTHONPATH"] = source_root + os.pathsep + env.get("PYTHONPATH", "")
    env["ACTION_RECOVERY_TEST_ROOT"] = str(tmp_path)
    result = subprocess.run(
        [sys.executable, "-c", script], cwd=source_root, env=env, check=False,
    )
    assert result.returncode == 91
    store = _store(tmp_path)
    summary = store.classify_interrupted(12)
    assert summary["applied_unregistered"] == 1
    assert summary["outcome_unknown"] == 1
    assert (tmp_path / "output.txt").read_bytes() == b"after"
    assert not (tmp_path / "buffer" / "action_recovery").exists()


def test_target_drift_fails_before_replacement(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "output.txt"
    target.write_bytes(b"before")
    action_id = _prepare_file(store, target)
    target.write_bytes(b"external")
    with pytest.raises(ActionRecoveryError, match="target_drift"):
        store.commit_file(action_id, target, b"before", b"after")
    assert target.read_bytes() == b"external"


def test_file_commit_rejects_journal_mismatch_and_non_file_target(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "output.txt"
    target.write_bytes(b"before")
    action_id = _prepare_file(store, target)
    with pytest.raises(ActionRecoveryError, match="record_mismatch"):
        store.commit_file(action_id, target, b"before", b"different")
    target.unlink()
    target.mkdir()
    assert store.classify_interrupted(12)["conflict"] == 1
    with pytest.raises(ActionRecoveryError, match="target_drift"):
        store.commit_file(action_id, target, b"before", b"after")


def test_v1_and_unknown_shapes_fail_closed_without_rewrite(tmp_path):
    path = tmp_path / "context" / "action_recovery_pending.json"
    path.parent.mkdir()
    original = '{"schema_version":"action_recovery_pending.v1","items":[]}'
    path.write_text(original, encoding="utf-8")
    with pytest.raises(ActionRecoveryError, match="schema_unknown"):
        _store(tmp_path).load()
    assert path.read_text(encoding="utf-8") == original


def test_interruption_without_actions_does_not_create_pending_journal(tmp_path):
    store = _store(tmp_path)
    assert store.classify_interrupted(12)["pending"] is False
    assert store.pending_items() == []
    assert not Path(store.path).exists()


def test_material_and_receipt_never_disclose_private_target_path(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "secret" / "output.txt"
    target.parent.mkdir()
    target.write_bytes(b"before")
    _prepare_file(store, target)
    target.write_bytes(b"after")
    store.classify_interrupted(12)
    visible = json.dumps(
        [render_materials(store.recovery_receipt(pending_only=True)),
         store.recovery_receipt()], ensure_ascii=False,
    )
    assert str(target) not in visible
    assert "output.txt" in visible
    store.mark_disclosed(13)
    assert render_materials(store.recovery_receipt(pending_only=True)) == []


def test_retention_keeps_success_eight_closed_rounds_and_unknown_forever(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "output.txt"
    target.write_bytes(b"before")
    success_id = _prepare_file(store, target)
    store.commit_file(success_id, target, b"before", b"after")
    unknown_id = store.prepare_opaque(
        tool_id="shell_command",
        request_sha256=_signature("unknown"),
        runtime_context=_context(iteration=4),
        call_id="unknown",
        target="shell",
    )
    store.classify_interrupted(12)
    store.mark_disclosed(13)
    for round_num in range(13, 20):
        store.note_round_closed(round_num)
    assert {item["action_id"] for item in store.load()["items"]} == {
        success_id, unknown_id,
    }
    store.note_round_closed(20)
    assert [item["action_id"] for item in store.load()["items"]] == [unknown_id]


def test_known_failure_and_not_applied_retire_after_disclosed_close(tmp_path):
    store = _store(tmp_path)
    target = tmp_path / "output.txt"
    target.write_bytes(b"before")
    _prepare_file(store, target)
    failed_id = store.prepare_opaque(
        tool_id="subagent_dispatch",
        request_sha256="f" * 64,
        runtime_context=_context(iteration=4),
        call_id="failed",
        target="subagent",
    )
    result = {"action_id": failed_id, "status": "failed", "reason": "nope"}
    store.record_results([result])
    store.classify_interrupted(12)
    store.mark_disclosed(13)
    store.note_round_closed(13)
    assert store.load()["items"] == []


def test_closing_later_round_does_not_retire_unsettled_earlier_action(tmp_path):
    store = _store(tmp_path)
    action_id = store.prepare_opaque(
        tool_id="shell_command", request_sha256=_signature("old"),
        runtime_context=_context(round_num=12), call_id="old", target="shell",
    )
    result = {"action_id": action_id, "status": "ok", "reason": ""}
    store.settle_results([result])
    store.note_round_closed(13)
    assert store.load()["items"][0]["action_id"] == action_id
