"""Hard capability boundary for the explicit single-round provider probe."""
from __future__ import annotations

import os
from typing import Any, Iterable


SINGLE_ROUND_PROBE_ENV = "UPSP_SINGLE_ROUND_PROBE"

_ALLOWED_TOOLS_BY_STEP = {
    "setup": {"setup_finalize"},
    "reaction": set(),
    "cleanup": {"cleanup_finalize"},
}

_REACTION_EXECUTION_KEYS = (
    "protocol_tool_requests",
    "protocol_tool_submissions",
    "general_tool_requests",
    "relation_card_declarations",
    "memory_write_declarations",
    "memory_annotation_declarations",
    "memory_recall_completion_requests",
    "memory_link_update_declarations",
    "memory_container_create_declarations",
    "memory_container_write_declarations",
    "memory_privacy_declarations",
    "memory_privacy_declassify_declarations",
    "chronicle_write_declarations",
    "alert_mode_settle_declarations",
    "fault_record_declarations",
    "container_focus_declarations",
    "pending_cancel_requests",
)

_PROBE_ACTIVE_FLAGS = {"user_message_waiting"}
_PROBE_SUPPRESSIBLE_SCHEDULE_FLAGS = {
    "calendar_day_due",
    "calendar_week_due",
    "calendar_month_due",
    "calendar_quarter_due",
    "calendar_year_due",
    "feeling_settle_due",
    "fatigue_expired",
    "rhythm_due",
    "standby_due",
}


def single_round_probe_enabled(env: dict[str, str] | None = None) -> bool:
    source = os.environ if env is None else env
    return str(source.get(SINGLE_ROUND_PROBE_ENV) or "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


def isolate_single_round_probe_flags(
        state_store,
        flags: dict[str, Any] | None,
        *,
        enabled: bool | None = None) -> tuple[dict[str, Any], dict[str, Any]]:
    """Remove sandbox-local background work before a paid probe round is chosen."""
    snapshot = dict(flags or {})
    if enabled is None:
        enabled = single_round_probe_enabled()
    if not enabled:
        return snapshot, {"enabled": False}

    suppressed = sorted(
        name for name, value in snapshot.items()
        if value and name in _PROBE_SUPPRESSIBLE_SCHEDULE_FLAGS
    )
    for name in suppressed:
        state_store.set_flag(name, False)
        snapshot[name] = False
    active = sorted(name for name, value in snapshot.items() if value)
    receipt = {
        "enabled": True,
        "status": "prepared" if active == ["user_message_waiting"] else "rejected",
        "suppressed_flags": suppressed,
        "active_flags": active,
    }
    if active != ["user_message_waiting"]:
        receipt["reason"] = (
            "single_round_probe_user_message_flag_missing"
            if "user_message_waiting" not in active
            else "single_round_probe_background_flags_present"
        )
    return snapshot, receipt


def validate_single_round_probe_round(
        round_type: str,
        flags: dict[str, Any] | None,
        *,
        enabled: bool | None = None) -> dict[str, Any]:
    """Fail closed before setup/provider access if probe isolation was bypassed."""
    if enabled is None:
        enabled = single_round_probe_enabled()
    if not enabled:
        return {"enabled": False}
    active = sorted(name for name, value in (flags or {}).items() if value)
    if str(round_type or "") != "interactive":
        raise RuntimeError("single_round_probe_non_interactive_round")
    if active != ["user_message_waiting"]:
        raise RuntimeError("single_round_probe_background_flags_present")
    return {
        "enabled": True,
        "status": "accepted",
        "round_type": "interactive",
        "active_flags": active,
    }


def _schema_name(schema: dict[str, Any]) -> str:
    if not isinstance(schema, dict):
        return ""
    name = str(schema.get("name") or "").strip()
    if name:
        return name
    function = schema.get("function")
    if isinstance(function, dict):
        return str(function.get("name") or "").strip()
    return ""


def filter_provider_tool_schemas(
        step: str,
        schemas: Iterable[dict[str, Any]],
        *,
        enabled: bool | None = None) -> list[dict[str, Any]]:
    """Expose only setup/cleanup terminal tools while the probe is active."""
    if enabled is None:
        enabled = single_round_probe_enabled()
    schemas = list(schemas or [])
    if not enabled:
        return schemas
    allowed = _ALLOWED_TOOLS_BY_STEP.get(str(step or "").strip().lower(), set())
    return [schema for schema in schemas if _schema_name(schema) in allowed]


def reject_reaction_execution_activity(
        parsed_reaction: dict[str, Any],
        *,
        enabled: bool | None = None) -> dict[str, Any]:
    """Convert legacy/text reaction tool requests into one non-executing fact."""
    parsed = dict(parsed_reaction or {})
    if enabled is None:
        enabled = single_round_probe_enabled()
    if not enabled:
        return parsed
    rejected_fields = [
        key for key in _REACTION_EXECUTION_KEYS if parsed.get(key)
    ]
    if not rejected_fields:
        return parsed
    for key in _REACTION_EXECUTION_KEYS:
        parsed[key] = []
    parsed.setdefault("invalid_tool_requests", []).append({
        "tool_id": "single_round_probe",
        "tool_family": "runtime_tool",
        "tool_class": "runtime_guard",
        "status": "rejected",
        "source": "single_round_probe_policy",
        "reason": "single_round_probe_tool_forbidden",
        "rejected_fields": rejected_fields,
    })
    return parsed
