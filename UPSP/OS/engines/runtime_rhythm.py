"""Rhythm preflight and Chronicle focus helpers for Runtime."""

from datetime import datetime

from constants import local_now
from errors import ProviderCallCancelled
from engines.round_context import RuntimeTrigger
from logic.rhythm_guide_materializer import reconcile_recovered_emergency_flags
from logic.single_round_probe_policy import validate_single_round_probe_round


def new_runtime_trigger(runtime, round_type, flags, state=None):
    """Freeze queued input and continuation-local options into one trigger."""
    runtime._trigger_seq += 1
    flags = dict(flags or {})
    messages = []
    if flags.get("user_message_waiting") and round_type in {
            "interactive", "rhythm"}:
        dequeue = getattr(runtime.hb, "dequeue_messages", None)
        if callable(dequeue):
            messages = list(dequeue() or [])
    permission_level = runtime.permission_chain.consume(messages, flags)
    final_response_max_chars = None
    final_response_length_rejections = 0
    response_contract = {}
    task_guidance_enabled = True
    if messages:
        final_response_max_chars = runtime._pending_final_response_max_chars
        runtime._pending_final_response_max_chars = None
        runtime._continuation_final_response_budget = None
        response_contract = dict(runtime._pending_response_contract or {})
        runtime._pending_response_contract = {}
        task_guidance_enabled = runtime._pending_task_guidance_enabled
        runtime._pending_task_guidance_enabled = True
    elif flags.get("continue_requested"):
        continuation = runtime._continuation_final_response_budget or {}
        value = continuation.get("max_chars")
        if isinstance(value, int) and value > 0:
            final_response_max_chars = value
            final_response_length_rejections = int(
                continuation.get("rejections") or 0)
        response_state = runtime._continuation_final_response_budget or {}
        response_contract = dict(response_state.get("response_contract") or {})
        task_guidance_enabled = response_state.get(
            "task_guidance_enabled", True) is not False
    return RuntimeTrigger(
        trigger_id=f"T{runtime._trigger_seq:08d}",
        trigger_seq=runtime._trigger_seq,
        observed_at=local_now().isoformat(),
        round_type=round_type,
        flags=flags,
        messages=tuple(messages),
        execution_permission_level=permission_level,
        final_response_max_chars=final_response_max_chars,
        final_response_length_rejections=final_response_length_rejections,
        response_contract=response_contract,
        task_guidance_enabled=task_guidance_enabled,
    )


def continuation_response_policy(runtime, context, result):
    if (
            context.final_response_max_chars is None
            and not context.response_contract
            and context.task_guidance_enabled):
        return None
    if not runtime.sm.get_flags().get("continue_requested"):
        return None
    return {
        "max_chars": context.final_response_max_chars,
        "rejections": int(result.get("_final_response_length_rejections") or 0),
        "response_contract": dict(context.response_contract),
        "task_guidance_enabled": context.task_guidance_enabled,
    }


def prepare_round_before_setup(runtime, round_type, state, flags):
    """Pause heartbeat, reconcile recovered alerts, and freeze the effective round."""
    runtime.hb.pause()
    try:
        had_api_degraded = bool((flags or {}).get("api_degraded"))
        probe_result = {
            "status": "not_needed",
            "reason": "flag_not_set",
            "attempted_profile_ids": [],
            "selected_profile_id": None,
            "elapsed_ms": 0,
            "tokens_input": 0,
            "tokens_output": 0,
        }
        flags, cleared = reconcile_recovered_emergency_flags(
            flags,
            state_store=runtime.sm,
            connectivity_store=getattr(runtime.services, "connectivity_store", None),
        )
        if had_api_degraded and not flags.get("api_degraded"):
            probe_result["reason"] = "stored_health_recovered"
        elif flags.get("api_degraded"):
            probe = getattr(runtime.services.executor, "probe_setup_route_once", None)
            if callable(probe):
                try:
                    probe_result = dict(probe() or {})
                except ProviderCallCancelled:
                    raise
                except Exception as exc:
                    probe_result = {
                        "status": "failed",
                        "reason": "probe_error",
                        "error_kind": type(exc).__name__,
                        "attempted_profile_ids": [],
                        "selected_profile_id": None,
                        "elapsed_ms": 0,
                        "tokens_input": 0,
                        "tokens_output": 0,
                    }
                if probe_result.get("status") == "recovered":
                    flags, probe_cleared = reconcile_recovered_emergency_flags(
                        flags,
                        state_store=runtime.sm,
                        connectivity_store=getattr(
                            runtime.services, "connectivity_store", None),
                    )
                    cleared = list(dict.fromkeys([*cleared, *probe_cleared]))
            else:
                probe_result = {
                    "status": "skipped",
                    "reason": "executor_probe_unavailable",
                    "attempted_profile_ids": [],
                    "selected_profile_id": None,
                    "elapsed_ms": 0,
                    "tokens_input": 0,
                    "tokens_output": 0,
                }
        probe_result["flag_cleared"] = "api_degraded" in cleared
        effective_round_type = runtime._determine_round_type(flags, state)
        if not effective_round_type:
            runtime.sm.set_phase("idle")
            runtime.hb.resume()
            return None, flags, cleared, probe_result, {}, {
                "aborted": False,
                "response": "",
                "error": None,
                "_round_skipped": "no_effective_trigger",
                "_pre_setup_cleared_flags": list(cleared),
                "_pre_setup_api_probe": dict(probe_result),
            }
        probe_guard = validate_single_round_probe_round(effective_round_type, flags)
        if probe_guard.get("enabled"):
            validate_single_round_probe_round(
                effective_round_type,
                runtime.sm.get_flags(),
            )
        runtime.sm.set_phase("presub")
        return (
            effective_round_type, flags, cleared,
            probe_result, probe_guard, None,
        )
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


def park_interaction_for_api_probe(runtime, trigger, flags):
    """Put dequeued input back before probing so cancellation cannot lose it."""
    if not (flags.get("api_degraded") and trigger.messages):
        return trigger, False
    runtime.hb.prepend_messages(trigger.messages)
    runtime._pending_final_response_max_chars = (
        trigger.final_response_max_chars)
    runtime._pending_response_contract = dict(trigger.response_contract)
    runtime._pending_task_guidance_enabled = trigger.task_guidance_enabled
    return RuntimeTrigger(
        trigger_id=trigger.trigger_id,
        trigger_seq=trigger.trigger_seq,
        observed_at=trigger.observed_at,
        round_type=trigger.round_type,
        flags=dict(trigger.flags),
        messages=(),
        execution_permission_level=trigger.execution_permission_level,
        final_response_max_chars=trigger.final_response_max_chars,
        final_response_length_rejections=(
            trigger.final_response_length_rejections),
        response_contract=dict(trigger.response_contract),
        task_guidance_enabled=trigger.task_guidance_enabled,
    ), True


def restore_interaction_after_api_probe(
        runtime, trigger, flags, probe_result, parked):
    """Consume parked input only after recovery; failed probes leave FIFO intact."""
    if not parked:
        return trigger, flags
    status = str((probe_result or {}).get("status") or "")
    probe_failed = status == "failed" or (
        status == "skipped"
        and (probe_result or {}).get("reason") != "executor_probe_unavailable"
    )
    messages = () if probe_failed else tuple(runtime.hb.dequeue_messages() or [])
    final_response_max_chars = (
        trigger.final_response_max_chars if messages else None)
    if messages:
        runtime._pending_final_response_max_chars = None
        runtime._pending_response_contract = {}
        runtime._pending_task_guidance_enabled = True
    flags = {**flags, "user_message_waiting": bool(messages)}
    return RuntimeTrigger(
        trigger_id=trigger.trigger_id,
        trigger_seq=trigger.trigger_seq,
        observed_at=trigger.observed_at,
        round_type=trigger.round_type,
        flags=dict(trigger.flags),
        messages=messages,
        execution_permission_level=trigger.execution_permission_level,
        final_response_max_chars=final_response_max_chars,
        final_response_length_rejections=(
            trigger.final_response_length_rejections if messages else 0),
        response_contract=(dict(trigger.response_contract) if messages else {}),
        task_guidance_enabled=(
            trigger.task_guidance_enabled if messages else True),
    ), flags


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
