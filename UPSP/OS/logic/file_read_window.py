"""Deterministic Runtime-only window planning for ``file_read``."""
from __future__ import annotations

import math
from typing import Any


WINDOW_STRATEGY = "context_budget_v1"
RUNTIME_CONTEXT_KEY = "_runtime_file_read_context"
FILE_READ_BATCH_BUDGET_EXHAUSTED = "file_read_batch_budget_exhausted"
MATERIAL_BUDGET_UNKNOWN = "material_budget_unknown"
MATERIAL_CONTEXT_BUDGET_EXHAUSTED = "material_context_budget_exhausted"
LEGACY_FLOOR_CHARS = 4096
DEFAULT_MAX_CHARS = 16384
MIN_RESERVE_TOKENS = 8192
RESERVE_RATIO = 0.10
SUCCESS_STATUSES = {"ok", "success", "accepted", "applied"}


def _positive_int(value: Any) -> int:
    try:
        number = int(value)
    except (TypeError, ValueError):
        return 0
    return number if number > 0 else 0


def runtime_file_read_context(
    state_store: Any,
    context_store: Any = None,
    round_num: Any = None,
) -> dict[str, int]:
    """Read the latest provider usage coordinates without mutating state."""
    if state_store is None or not hasattr(state_store, "get"):
        context = {"current_tokens": 0, "context_window": 0}
    else:
        context = {
            "current_tokens": _positive_int(
                state_store.get("base.token_usage.current_tokens", 0)
            ),
            "context_window": _positive_int(
                state_store.get("base.token_usage.window_size", 0)
            ),
        }
    if context_store is not None:
        material_chars = getattr(
            context_store, "get_round_material_chars", None
        )
        now_budget = getattr(context_store, "get_now_budget_chars", None)
        if callable(material_chars):
            context["round_material_chars"] = max(
                _positive_int(material_chars(round_num)), 0
            )
        if callable(now_budget):
            context["now_budget_chars"] = max(_positive_int(now_budget()), 0)
    return context


def provider_material_context_issue(
    state_store: Any,
    context_store: Any,
    round_num: Any,
    next_iteration: Any,
) -> dict[str, Any]:
    """Fail closed before provider when newly retained material exhausts reserve."""
    context = runtime_file_read_context(state_store)
    current_tokens = _positive_int(context.get("current_tokens"))
    context_window = _positive_int(context.get("context_window"))
    getter = getattr(context_store, "get_round_material_chars", None)
    if not callable(getter):
        return {}
    try:
        pending_material_chars = _positive_int(
            getter(round_num, iteration=max(int(next_iteration) - 1, 0))
        )
    except (TypeError, ValueError):
        pending_material_chars = 0
    if pending_material_chars <= 0:
        return {}
    if not current_tokens or not context_window:
        return {}
    reserve_tokens = max(
        MIN_RESERVE_TOKENS,
        int(math.ceil(context_window * RESERVE_RATIO)),
    )
    safe_room_tokens = context_window - current_tokens - reserve_tokens
    if pending_material_chars < max(safe_room_tokens, 0):
        return {}
    return {
        "schema_version": "material_context_preflight.v1",
        "status": "blocked",
        "reason": MATERIAL_CONTEXT_BUDGET_EXHAUSTED,
        "round": _positive_int(round_num),
        "iteration": _positive_int(next_iteration),
        "current_tokens": current_tokens,
        "context_window_tokens": context_window,
        "reserve_tokens": reserve_tokens,
        "safe_room_tokens": max(safe_room_tokens, 0),
        "pending_material_chars": pending_material_chars,
        "provider_call_started": False,
    }


class FileReadBatchBudget:
    """Share one deterministic file-read budget across a provider tool batch."""

    def __init__(self, runtime_context: dict[str, Any] | None = None):
        self.runtime_context = (
            dict(runtime_context) if isinstance(runtime_context, dict) else {}
        )
        self.consumed_chars = 0
        self.remaining_chars: int | None = None
        self.legacy_floor_chars = LEGACY_FLOOR_CHARS
        self.exhaustion_reason = FILE_READ_BATCH_BUDGET_EXHAUSTED
        current_tokens = _positive_int(self.runtime_context.get("current_tokens"))
        context_window = _positive_int(self.runtime_context.get("context_window"))
        if not current_tokens or not context_window or current_tokens >= context_window:
            now_budget = _positive_int(self.runtime_context.get("now_budget_chars"))
            retained = _positive_int(
                self.runtime_context.get("round_material_chars")
            )
            if now_budget:
                self.remaining_chars = max(now_budget - retained, 0)
                self.exhaustion_reason = MATERIAL_BUDGET_UNKNOWN

    def exhausted_for(self, tool_id: str) -> bool:
        return (
            tool_id == "file_read"
            and self.remaining_chars is not None
            and self.remaining_chars < self.legacy_floor_chars
        )

    def context_for(self, tool_id: str) -> dict[str, Any]:
        context = dict(self.runtime_context)
        if tool_id != "file_read" or self.consumed_chars <= 0:
            return context
        current_tokens = _positive_int(context.get("current_tokens"))
        if current_tokens:
            context["current_tokens"] = current_tokens + self.consumed_chars
        return context

    def rejection_details(self) -> dict[str, Any]:
        return {
            "window_strategy": WINDOW_STRATEGY,
            "window_budget_status": (
                MATERIAL_BUDGET_UNKNOWN
                if self.exhaustion_reason == MATERIAL_BUDGET_UNKNOWN
                else "batch_floor_exhausted"
            ),
            "window_batch_consumed_before_chars": int(self.consumed_chars),
            "window_batch_remaining_after_chars": int(self.remaining_chars or 0),
        }

    def observe(self, result: Any) -> Any:
        if not isinstance(result, dict) or result.get("tool_id") != "file_read":
            return result
        if str(result.get("status") or "").strip() not in SUCCESS_STATUSES:
            return result
        floor = _positive_int(result.get("window_legacy_floor_chars"))
        if floor:
            self.legacy_floor_chars = floor
        safe_room = _integer_or_none(result.get("window_safe_room_tokens"))
        if self.remaining_chars is None:
            self.remaining_chars = max(
                safe_room if safe_room is not None else 0,
                self.legacy_floor_chars,
            )
        returned_chars = max(
            _integer_or_none(result.get("returned_chars")) or 0,
            0,
        )
        consumed_before = self.consumed_chars
        self.consumed_chars += returned_chars
        self.remaining_chars -= returned_chars
        decorated = dict(result)
        decorated["window_batch_consumed_before_chars"] = int(consumed_before)
        decorated["window_batch_remaining_after_chars"] = int(
            self.remaining_chars
        )
        return decorated


def _integer_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError, OverflowError):
        return None


def plan_file_read_window(
    configured_max_chars: Any,
    runtime_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Return a deterministic 4K-to-configured-max file-read window plan."""
    configured_max = _positive_int(configured_max_chars) or DEFAULT_MAX_CHARS
    legacy_floor = min(configured_max, LEGACY_FLOOR_CHARS)
    context = runtime_context if isinstance(runtime_context, dict) else {}
    current_tokens = _positive_int(context.get("current_tokens"))
    context_window = _positive_int(context.get("context_window"))

    reserve_tokens = 0
    safe_room_tokens = 0
    if not current_tokens or not context_window or current_tokens >= context_window:
        window_chars = legacy_floor
        budget_status = "fallback_missing_or_invalid_budget"
    else:
        reserve_tokens = max(
            MIN_RESERVE_TOKENS,
            int(math.ceil(context_window * RESERVE_RATIO)),
        )
        safe_room_tokens = context_window - current_tokens - reserve_tokens
        if safe_room_tokens <= legacy_floor:
            window_chars = legacy_floor
            budget_status = "legacy_floor_context_tight"
        elif safe_room_tokens < configured_max:
            window_chars = int(safe_room_tokens)
            budget_status = "adaptive_reduced"
        else:
            window_chars = configured_max
            budget_status = "adaptive_max"

    return {
        "window_strategy": WINDOW_STRATEGY,
        "window_chars": int(window_chars),
        "window_configured_max_chars": int(configured_max),
        "window_legacy_floor_chars": int(legacy_floor),
        "window_budget_status": budget_status,
        "window_current_tokens": int(current_tokens),
        "window_context_window_tokens": int(context_window),
        "window_reserve_tokens": int(reserve_tokens),
        "window_safe_room_tokens": int(safe_room_tokens),
    }
