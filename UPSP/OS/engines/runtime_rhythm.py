"""Rhythm preflight and Chronicle focus helpers for Runtime."""

from datetime import datetime

from constants import local_now
from logic.rhythm_guide_materializer import reconcile_recovered_emergency_flags
from logic.single_round_probe_policy import validate_single_round_probe_round


def prepare_round_before_setup(runtime, round_type, state, flags):
    """Pause heartbeat, reconcile recovered alerts, and freeze the effective round."""
    runtime.hb.pause()
    try:
        flags, cleared = reconcile_recovered_emergency_flags(
            flags,
            state_store=runtime.sm,
            connectivity_store=getattr(runtime.services, "connectivity_store", None),
        )
        effective_round_type = runtime._determine_round_type(flags, state)
        if not effective_round_type:
            runtime.sm.set_phase("idle")
            runtime.hb.resume()
            return None, flags, cleared, {}, {
                "aborted": False,
                "response": "",
                "error": None,
                "_round_skipped": "no_effective_trigger",
                "_pre_setup_cleared_flags": list(cleared),
            }
        probe_guard = validate_single_round_probe_round(effective_round_type, flags)
        if probe_guard.get("enabled"):
            validate_single_round_probe_round(
                effective_round_type,
                runtime.sm.get_flags(),
            )
        runtime.sm.set_phase("presub")
        return effective_round_type, flags, cleared, probe_guard, None
    except Exception:
        try:
            runtime.sm.set_phase("idle")
        except Exception:
            pass
        try:
            runtime.hb.resume()
        except Exception:
            pass
        raise


def refresh_round_alert_recovery(runtime, context):
    """Absorb newly healthy endpoint/process evidence before guide selection."""
    flags = dict(context.flags or {})
    get_flags = getattr(runtime.sm, "get_flags", None)
    persisted = get_flags() if callable(get_flags) else {}
    if isinstance(persisted, dict):
        for name, active in persisted.items():
            if active:
                flags[name] = True
        for name in ("api_degraded",):
            if name in persisted:
                flags[name] = bool(persisted[name])
    effective, cleared = reconcile_recovered_emergency_flags(
        flags,
        state_store=runtime.sm,
        connectivity_store=getattr(runtime.services, "connectivity_store", None),
    )
    context.flags = effective
    context.state = runtime.sm.load()
    return cleared


def prepare_chronicle_focus_for_active_guide(runtime, round_type, state, round_num):
    """Create Chronicle focus only from the coordinated active rhythm guide."""
    runner = runtime.reaction_loop_runner
    if str(round_type or "").strip().lower() != "rhythm":
        try:
            runner.chronicle_focus = None
        except Exception:
            pass
        return {}
    try:
        guide_id = str(runtime.workbench.get("base.active_guides.rhythm") or "").strip()
        active_guide = runtime.workbench.load_guide(guide_id) if guide_id else {}
    except Exception:
        active_guide = {}
    if str(active_guide.get("kind") or "").strip() != "main_axis_rhythm_guide":
        runner.chronicle_focus = None
        return {}
    store = getattr(runner, "chronicle_store", None)
    if store is None:
        try:
            from data.chronicle_store import ChronicleStore

            store = ChronicleStore()
            runner.chronicle_store = store
        except Exception:
            return {}
    state = state or {}
    base = state.get("base", {}) if isinstance(state, dict) else {}
    meta = base.get("meta", {}) if isinstance(base, dict) else {}
    closed_at = local_now().isoformat()
    state_sample = chronicle_state_sample(base)
    memory_stats = chronicle_memory_stats(runtime.memory_store, round_num)
    try:
        path = store.refresh_active_rhythm(
            round_num=round_num,
            closed_at=closed_at,
            state_sample=state_sample,
            memory_stats=memory_stats,
            range_start_round=meta.get("last_rhythm_round"),
            range_start_time=meta.get("last_round_closed_at"),
        )
    except Exception:
        return {}
    focus = {
        "layer": "rhythms",
        "path": path,
        "round_num": int(round_num or 0),
        "round_type": "rhythm",
        "source_refs": [f"round:{int(round_num or 0)}"],
        "range_start_round": meta.get("last_rhythm_round"),
        "range_start_time": meta.get("last_round_closed_at"),
        "range_end_round": int(round_num or 0),
        "range_end_time": closed_at,
        "state_sample": state_sample,
        "memory_stats": memory_stats,
    }
    runner.chronicle_focus = focus
    return focus


def chronicle_state_sample(base):
    base = base or {}
    dynamic_axes = base.get("dynamic_axes") or {}
    sample = {}
    for axis in ("valence", "arousal", "focus", "mood", "humor", "safety"):
        slot = dynamic_axes.get(axis) or {}
        sample[f"dynamic_axis_{axis}"] = slot.get("value")
    core = base.get("core_speed_wheel") or {}
    workhood = base.get("workhood_index") or {}
    token_usage = base.get("token_usage") or {}
    sample.update({
        "core_speed_current": core.get("current"),
        "core_speed_max": core.get("max"),
        "workhood_index": workhood.get("value"),
        "workhood_self_reference": workhood.get("self_reference"),
        "workhood_self_reflection": workhood.get("self_reflection"),
        "workhood_autonomy": workhood.get("autonomy"),
        "context_window_usage_ratio": token_usage.get("usage_ratio"),
    })
    return sample


def chronicle_memory_stats(memory_store, round_num):
    stats = {"total": 0, "weights": {"F": 0, "S": 0, "A": 0, "P": 0}}
    try:
        entries = memory_store.load_meta()
    except Exception:
        return stats
    iterable = entries.values() if isinstance(entries, dict) else entries or []
    for item in iterable:
        if not isinstance(item, dict):
            continue
        created_round = item.get("created_round") or item.get("round_num")
        try:
            if int(created_round or 0) != int(round_num or 0):
                continue
        except (TypeError, ValueError):
            continue
        weight = str(item.get("weight") or "").strip().upper()
        if weight not in stats["weights"]:
            continue
        stats["total"] += 1
        stats["weights"][weight] += 1
    return stats
