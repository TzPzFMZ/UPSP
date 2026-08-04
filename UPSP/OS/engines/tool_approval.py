"""One pending, process-local approval for guarded general tools."""
from __future__ import annotations

import threading
import uuid

from constants import local_now


class ToolApprovalConflict(RuntimeError):
    pass


class ToolApprovalCoordinator:
    def __init__(self):
        self._lock = threading.Lock()
        self._pending = None

    def request(self, payload, on_requested=None, on_resolved=None):
        pending = dict(payload or {})
        pending.update({
            "schema_version": "general_tool_approval.v1",
            "approval_id": uuid.uuid4().hex,
            "requested_at": local_now().isoformat(),
            "decision": None,
            "event": threading.Event(),
        })
        with self._lock:
            if self._pending is not None:
                raise ToolApprovalConflict("tool_approval_already_pending")
            self._pending = pending
        try:
            if callable(on_requested):
                on_requested(self._public(pending, include_details=False))
            pending["event"].wait()
            with self._lock:
                decision = str(pending.get("decision") or "cancelled")
                resolved_at = str(pending.get("resolved_at") or local_now().isoformat())
                if self._pending is pending:
                    self._pending = None
            resolved = self._public(pending, include_details=False)
            resolved.update({"decision": decision, "resolved_at": resolved_at})
            if callable(on_resolved):
                on_resolved(resolved)
            return decision
        except BaseException:
            with self._lock:
                if self._pending is pending:
                    self._pending = None
            raise

    def resolve(self, approval_id, decision):
        if decision not in {"allow_once", "skip"}:
            raise ValueError("invalid_tool_approval_decision")
        with self._lock:
            pending = self._pending
            if pending is None or pending.get("approval_id") != approval_id:
                raise ToolApprovalConflict("tool_approval_not_pending")
            if pending.get("decision") is not None:
                raise ToolApprovalConflict("tool_approval_already_resolved")
            pending["decision"] = decision
            pending["resolved_at"] = local_now().isoformat()
            pending["event"].set()
        return {"approval_id": approval_id, "decision": decision}

    def cancel(self):
        with self._lock:
            pending = self._pending
            if pending is None or pending.get("decision") is not None:
                return False
            pending["decision"] = "cancelled"
            pending["resolved_at"] = local_now().isoformat()
            pending["event"].set()
            return True

    def snapshot(self):
        with self._lock:
            return self._public(self._pending, include_details=True) if self._pending else None

    def attach_status(self, status):
        status["pending_tool_approval"] = self.snapshot()
        return status

    @staticmethod
    def _public(pending, include_details):
        if not pending:
            return None
        keys = (
            "schema_version", "approval_id", "round", "frame_id", "iteration",
            "tool_id", "tool_label", "tool_signature", "summary", "requested_at",
        )
        result = {key: pending.get(key) for key in keys if pending.get(key) not in (None, "")}
        if include_details:
            result["details"] = dict(pending.get("details") or {})
        return result


def request_runtime_tool_approval(runtime, payload):
    """Wait for a decision while persisting only the safe approval envelope."""
    round_num = payload.get("round")

    def record(event_type, event):
        runtime.audit.get_store().append_event(
            round_num,
            event_type,
            {
                key: event.get(key)
                for key in (
                    "approval_id", "frame_id", "iteration", "tool_id",
                    "tool_signature", "summary", "decision",
                    "requested_at", "resolved_at",
                )
                if event.get(key) not in (None, "")
            },
            phase="reaction",
        )

    runtime._set_active_stage("tool_approval")
    try:
        return runtime.tool_approval.request(
            payload,
            on_requested=lambda event: record(
                "general_tool_approval_requested", event),
            on_resolved=lambda event: record(
                "general_tool_approval_resolved", event),
        )
    finally:
        if runtime.control.snapshot(runtime.hb).get("stage") == "tool_approval":
            runtime._set_active_stage("reaction")
