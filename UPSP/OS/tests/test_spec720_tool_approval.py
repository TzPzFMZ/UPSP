import threading
import time

import pytest


SIDE_EFFECT_REQUEST = {
    "tool_id": "file_write",
    "path": "artifact.txt",
    "content": "ok",
    "call_id": "call-720",
}


def test_guarded_approval_blocks_handler_until_allow_and_skip_never_executes(monkeypatch):
    from engines.general_tool_dispatcher import GeneralToolDispatcher
    from engines.tool_approval import ToolApprovalCoordinator

    monkeypatch.setenv("UPSP_EXECUTION_PERMISSION_LEVEL", "guarded")
    monkeypatch.delenv("UPSP_ENGINEERING_SANDBOX_GRANT_JSON", raising=False)
    executed = []

    def execute(call, **_kwargs):
        executed.append(call)
        return {"tool_id": call["tool_id"], "status": "ok"}

    coordinator = ToolApprovalCoordinator()
    waiting = threading.Event()

    def approve(payload):
        waiting.set()
        return coordinator.request(payload)

    allowed = []
    worker = threading.Thread(target=lambda: allowed.extend(GeneralToolDispatcher(
        execute_fn=execute,
        approval_fn=approve,
    ).handle_requests([SIDE_EFFECT_REQUEST], [])), daemon=True)
    worker.start()
    assert waiting.wait(1)
    pending = coordinator.snapshot()
    assert pending and executed == []
    coordinator.resolve(pending["approval_id"], "allow_once")
    worker.join(1)
    assert len(executed) == 1
    assert allowed[0]["status"] == "ok"

    executed.clear()
    skipped = GeneralToolDispatcher(
        execute_fn=execute,
        approval_fn=lambda _payload: "skip",
    ).handle_requests([SIDE_EFFECT_REQUEST], [])
    assert executed == []
    assert skipped[0]["status"] == "blocked"
    assert skipped[0]["reason"] == "user_skipped_tool_approval"
    assert skipped[0]["error_hint"]["kind"] == "permission_security"


def test_skipped_duplicate_does_not_prompt_again(monkeypatch):
    from engines.general_tool_dispatcher import GeneralToolDispatcher

    monkeypatch.setenv("UPSP_EXECUTION_PERMISSION_LEVEL", "guarded")
    first = GeneralToolDispatcher(
        execute_fn=lambda *_args, **_kwargs: pytest.fail("handler ran"),
        approval_fn=lambda _payload: "skip",
    ).handle_requests([SIDE_EFFECT_REQUEST], [])
    second = GeneralToolDispatcher(
        execute_fn=lambda *_args, **_kwargs: pytest.fail("handler ran"),
        approval_fn=lambda _payload: pytest.fail("approval repeated"),
    ).handle_requests([SIDE_EFFECT_REQUEST], [], prior_results=first)
    assert second[0]["reason"] == "duplicate_tool_failure_repeated"


def test_multiple_side_effect_calls_are_approved_in_original_order(monkeypatch):
    from engines.general_tool_dispatcher import GeneralToolDispatcher

    monkeypatch.setenv("UPSP_EXECUTION_PERMISSION_LEVEL", "guarded")
    approvals = []
    executions = []
    requests = [
        dict(SIDE_EFFECT_REQUEST, call_id="one", path="one.txt"),
        dict(SIDE_EFFECT_REQUEST, call_id="two", path="two.txt"),
    ]
    dispatcher = GeneralToolDispatcher(
        execute_fn=lambda call, **_kwargs: executions.append(call["path"])
        or {"tool_id": call["tool_id"], "status": "ok"},
        approval_fn=lambda payload: approvals.append(payload["summary"]) or "allow_once",
    )
    dispatcher.handle_requests(requests, [])
    assert approvals == ["one.txt", "two.txt"]
    assert executions == approvals


def test_coordinator_rejects_wrong_and_repeated_ids_and_cancel_wakes_waiter():
    from engines.tool_approval import ToolApprovalConflict, ToolApprovalCoordinator

    coordinator = ToolApprovalCoordinator()
    outcomes = []
    worker = threading.Thread(
        target=lambda: outcomes.append(coordinator.request({"tool_id": "file_write"})),
        daemon=True,
    )
    worker.start()
    for _ in range(1000):
        pending = coordinator.snapshot()
        if pending:
            break
    assert pending
    with pytest.raises(ToolApprovalConflict):
        coordinator.resolve("wrong", "allow_once")
    coordinator.resolve(pending["approval_id"], "skip")
    with pytest.raises(ToolApprovalConflict):
        coordinator.resolve(pending["approval_id"], "skip")
    worker.join(1)
    assert outcomes == ["skip"]

    worker = threading.Thread(
        target=lambda: outcomes.append(coordinator.request({"tool_id": "file_edit"})),
        daemon=True,
    )
    worker.start()
    for _ in range(1000):
        if coordinator.snapshot():
            break
    assert coordinator.cancel() is True
    worker.join(1)
    assert outcomes[-1] == "cancelled"


def test_explicit_sandbox_grant_still_narrows_unlimited_paths(tmp_path, monkeypatch):
    import json
    from logic.execution_capability import check_general_tool_request

    allowed = tmp_path / "allowed"
    outside = tmp_path / "outside.txt"
    allowed.mkdir()
    grant = {
        "task_root": str(allowed),
        "allowed_tools": ["file_read"],
        "read_paths": [str(allowed)],
        "write_paths": [str(allowed)],
        "shell_cwd": str(allowed),
    }
    monkeypatch.setenv("UPSP_ENGINEERING_SANDBOX_GRANT_JSON", json.dumps(grant))
    from logic.sandbox_grant import load_sandbox_grant

    decision = check_general_tool_request(
        {"tool_id": "file_read", "path": str(outside)},
        phase="reaction",
        active_guides=[],
        sandbox_grant=load_sandbox_grant(),
        execution_permission_level="unlimited",
    )
    assert decision["allowed"] is False
    assert decision["reason"] == "outside_allowlist"


def test_tool_approval_stage_remains_stoppable():
    from engines.runtime_control import RuntimeControl

    control = RuntimeControl()
    control.round_in_flight = True
    control.round_num = 720
    control.set_stage("tool_approval")
    heartbeat = type("Heartbeat", (), {"_paused": True})()
    snapshot = control.snapshot(heartbeat)
    assert snapshot["can_stop"] is True
    assert snapshot["heartbeat_suspended"] is True


def test_runtime_approval_uses_runtime_control_stage_and_resumes_reaction():
    from engines.runtime import Runtime
    from engines.runtime_control import RuntimeControl
    from engines.tool_approval import ToolApprovalCoordinator

    events = []

    class Store:
        def append_event(self, _round, event_type, payload, **_kwargs):
            events.append((event_type, payload))

    runtime = Runtime.__new__(Runtime)
    runtime.control = RuntimeControl()
    runtime.control.round_in_flight = True
    runtime.control.round_num = 720
    runtime.control.set_stage("reaction")
    runtime.tool_approval = ToolApprovalCoordinator()
    runtime.audit = type("Audit", (), {"get_store": lambda self: Store()})()
    runtime.hb = type("Heartbeat", (), {"_paused": True})()
    outcome = []
    worker = threading.Thread(
        target=lambda: outcome.append(runtime._request_tool_approval({
            "round": 720,
            "frame_id": "R000720:reaction:1",
            "tool_id": "file_write",
        })),
        daemon=True,
    )
    worker.start()
    deadline = time.monotonic() + 2
    pending = None
    while time.monotonic() < deadline:
        pending = runtime.tool_approval.snapshot()
        if pending:
            break
        time.sleep(0.01)

    assert pending is not None
    assert runtime.control.snapshot(runtime.hb)["stage"] == "tool_approval"
    runtime.resolve_tool_approval(pending["approval_id"], "allow_once")
    worker.join(timeout=2)

    assert outcome == ["allow_once"]
    assert runtime.control.snapshot(runtime.hb)["stage"] == "reaction"
    assert [event_type for event_type, _payload in events] == [
        "general_tool_approval_requested",
        "general_tool_approval_resolved",
    ]


def test_approval_audit_projection_keeps_safe_fields_only():
    from data.round_live_viewer import _cards_for_event

    card = _cards_for_event({
        "event_index": 1,
        "event_type": "general_tool_approval_requested",
        "recorded_at": "2026-08-04T12:00:00+08:00",
        "payload": {
            "approval_id": "approval-720",
            "tool_id": "shell_command",
            "tool_signature": "sha256",
            "summary": "D:/work",
            "command": "must-not-persist",
        },
    })[0]
    assert card["type"] == "tool-approval"
    assert card["approval_id"] == "approval-720"
    assert "must-not-persist" not in str(card)


def test_unrestricted_paths_still_reject_local_credential_store():
    from logic.execution_capability import check_general_tool_request
    from paths import GLOBAL_MODELS_CONFIG

    decision = check_general_tool_request(
        {"tool_id": "file_read", "path": GLOBAL_MODELS_CONFIG},
        phase="reaction",
        active_guides=[],
        execution_permission_level="unlimited",
    )
    assert decision["allowed"] is False
    assert decision["details"]["denial"] == "secret_like_path"


@pytest.mark.parametrize("relative_path", [
    ".codex/auth.json",
    ".config/gh/hosts.yml",
    ".docker/config.json",
])
def test_common_local_credential_paths_are_hard_denied(relative_path):
    from pathlib import Path
    from logic.execution_capability import check_general_tool_request

    path = Path.home() / relative_path
    read = check_general_tool_request(
        {"tool_id": "file_read", "path": str(path)},
        phase="reaction",
        active_guides=[],
        execution_permission_level="unlimited",
    )
    shell = check_general_tool_request(
        {
            "tool_id": "shell_command",
            "cwd": str(Path.home()),
            "command": f'type "{path}"',
            "purpose": "probe hard boundary",
        },
        phase="reaction",
        active_guides=[],
        execution_permission_level="unlimited",
    )
    assert read["allowed"] is False
    assert read["details"]["denial"] == "secret_like_path"
    # Spec756: Shell does not claim to sandbox subprocess paths by scanning
    # command strings. The dedicated file tool still enforces the hard deny.
    assert shell["allowed"] is True


def test_requested_audit_failure_cancels_approval_without_waiting():
    from engines.tool_approval import (
        ToolApprovalCoordinator,
        request_runtime_tool_approval,
    )

    class BrokenStore:
        def append_event(self, *_args, **_kwargs):
            raise OSError("disk full")

    class Runtime:
        tool_approval = ToolApprovalCoordinator()
        audit = type("Audit", (), {"get_store": lambda self: BrokenStore()})()
        control = type("Control", (), {
            "snapshot": lambda self, _hb: {"stage": "tool_approval"},
            "set_stage": lambda self, stage: setattr(self, "stage", stage),
        })()
        hb = object()

    runtime = Runtime()
    with pytest.raises(OSError, match="disk full"):
        request_runtime_tool_approval(
            runtime, {"round": 720, "tool_id": "file_write"}
        )
    assert runtime.tool_approval.snapshot() is None
    assert runtime.control.stage == "reaction"
