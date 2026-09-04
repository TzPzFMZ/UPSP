"""Deterministic provider-call stop-loss for controlled reaction runs."""
from __future__ import annotations

import os
from typing import Any

from logic.single_round_probe_policy import single_round_probe_enabled


REACTION_PROVIDER_CALL_LIMIT_ENV = "UPSP_REACTION_PROVIDER_CALL_LIMIT"
MAX_REACTION_PROVIDER_CALL_LIMIT = 100


def configured_reaction_provider_call_limit(
        env: dict[str, str] | None = None) -> int:
    """Return the opt-in reaction call cap; zero means disabled."""
    source = os.environ if env is None else env
    raw = str(source.get(REACTION_PROVIDER_CALL_LIMIT_ENV) or "").strip()
    if not raw:
        return 0
    try:
        limit = int(raw)
    except (TypeError, ValueError) as exc:
        raise ValueError("reaction_provider_call_limit_invalid") from exc
    if limit < 1 or limit > MAX_REACTION_PROVIDER_CALL_LIMIT:
        raise ValueError("reaction_provider_call_limit_out_of_range")
    return limit


def reaction_provider_call_limit_policy(
        env: dict[str, str] | None = None) -> dict[str, Any]:
    """Resolve the stricter of the single-round probe and paid-run cap."""
    if single_round_probe_enabled(env):
        return {
            "enabled": True,
            "limit": 1,
            "reason": "single_round_probe_reaction_call_limit",
            "source": "single_round_probe",
        }
    limit = configured_reaction_provider_call_limit(env)
    if not limit:
        return {"enabled": False, "limit": 0, "source": "default_off"}
    return {
        "enabled": True,
        "limit": limit,
        "reason": "reaction_provider_call_limit_reached",
        "source": "controlled_dogfood",
    }


def reaction_provider_call_hard_stop(
        policy: dict[str, Any],
        provider_reaction_calls: int,
) -> tuple[dict[str, Any], dict[str, Any]]:
    """Build generic and legacy single-probe receipts without starting a call."""
    limit = int(policy.get("limit") or 0)
    if not policy.get("enabled") or provider_reaction_calls < limit:
        return {}, {}
    stop = {
        "status": "hard_stop",
        "reason": policy["reason"],
        "provider_reaction_calls": provider_reaction_calls,
        "limit": limit,
        "source": policy["source"],
    }
    legacy_probe_stop = (
        {key: value for key, value in stop.items() if key != "source"}
        if policy["source"] == "single_round_probe"
        else {}
    )
    return stop, legacy_probe_stop
