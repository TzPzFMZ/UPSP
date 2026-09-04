"""Runtime-owned causal settlement for one serial Seed Round."""
from engines.round_context import FrameRef
from engines.organ_runtime import organ_runtime_context
from logic.interaction_meta import cache_interaction_meta
from logic.runtime_channels import closeout_final_response_source


def _is_verified_cache_closeout(receipt):
    return (
        isinstance(receipt, dict)
        and receipt.get("schema_version") == "current_cache_transition.v1"
        and receipt.get("boundary") == "round_closeout"
        and receipt.get("status") in {"applied", "noop", "recovered"}
    )


def settle_round(runtime, context, result):
    store = runtime.audit.get_store()
    audit_failures = []
    organ_failures = []
    frames = (
        result.get("_frame_settlements") or []
        if isinstance(result, dict)
        else []
    )
    caused_by = str(
        (frames[-1] if frames else {}).get("frame_id")
        or getattr(context.setup_frame, "frame_id", "")
        or context.trigger.trigger_id
    )
    obligation_id = f"R{context.round_num:06d}:cleanup"
    common = {
        "obligation_id": obligation_id,
        "trigger_id": context.trigger.trigger_id,
        "caused_by": caused_by,
    }
    causal_frame = context.setup_frame
    required_context_failure = (
        result.get("_required_context_failure") or {}
        if isinstance(result, dict)
        else {}
    )
    if frames and isinstance(frames[-1].get("frame_ref"), dict):
        causal_frame = FrameRef(**frames[-1]["frame_ref"])

    def dispatch(event_type, payload):
        try:
            runtime.organ_runtime.dispatch(
                event_type,
                context.cleanup_frame or causal_frame,
                payload,
                organ_runtime_context(runtime, context),
            )
        except Exception as exc:
            organ_failures.append(
                f"organ_dispatch:{event_type}:{type(exc).__name__}")

    def emit(event_type, payload):
        try:
            store.append_event(context.round_num, event_type, payload)
        except Exception as exc:
            audit_failures.append(
                f"round_audit:{event_type}:{type(exc).__name__}")
        dispatch(event_type, payload)

    def apply_failures(value):
        value = dict(value or {})
        value.setdefault("status", "settled")
        degraded = list(value.get("degraded_reasons") or [])
        fatal = list(value.get("fatal_reasons") or [])
        if required_context_failure:
            reason = "required_context:{}:{}:{}".format(
                required_context_failure.get("stage") or "unknown",
                required_context_failure.get("scope") or "unknown",
                required_context_failure.get("error_type") or "unknown",
            )
            if reason not in fatal:
                fatal.append(reason)
        degraded.extend(
            reason for reason in organ_failures if reason not in degraded)
        fatal.extend(reason for reason in audit_failures if reason not in fatal)
        value["degraded_reasons"] = degraded
        value["fatal_reasons"] = fatal
        if fatal:
            value["status"] = "unsettled"
        elif degraded and value["status"] == "settled":
            value["status"] = "degraded"
        return value

    emit("round_close_requested", common)
    emit("cleanup_obligation_created", common)
    try:
        outcome = runtime._run_cleanup(
            context.round_type,
            context.state,
            result,
            context.round_num,
            context.user_input_text,
            context=context,
        ) or {}
    except Exception as exc:
        outcome = {"status": "unsettled", "fatal_reasons": [str(exc)]}
    closeout_receipt = (
        result.get("_current_cache_closeout")
        if isinstance(result, dict) else None
    )
    cache_closeout_verified = _is_verified_cache_closeout(closeout_receipt)
    if not cache_closeout_verified:
        try:
            runtime.ctx_store.save_round_to_cache(
                context.round_num,
                user_input=(
                    context.user_input_text
                    if context.trigger and context.trigger.messages else ""
                ),
                response=(result.get("response", "") if isinstance(result, dict) else ""),
                **cache_interaction_meta(context.interaction_meta),
            )
            closeout_receipt = runtime.ctx_store.transition_current_cache(
                boundary="round_closeout",
                consumer_frame_id=f"R{int(context.round_num):06d}:closeout",
                expire_call_transients=True,
            )
            if isinstance(result, dict):
                result["_current_cache_closeout"] = closeout_receipt
            cache_closeout_verified = _is_verified_cache_closeout(
                closeout_receipt)
            if not cache_closeout_verified:
                raise ValueError("current_cache_closeout_receipt_invalid")
            try:
                store.append_event(
                    context.round_num,
                    "current_cache_transition",
                    closeout_receipt,
                    phase="cleanup",
                )
            except Exception as exc:
                audit_failures.append(
                    f"round_audit:current_cache_transition:{type(exc).__name__}")
        except Exception as exc:
            fatal_reasons = list(outcome.get("fatal_reasons") or [])
            fatal_reasons.append(
                f"current_cache_closeout:{type(exc).__name__}:{exc}")
            outcome["fatal_reasons"] = fatal_reasons
            outcome["status"] = "unsettled"
    if cache_closeout_verified:
        compaction_receipt = runtime._record_cache_compaction_rhythm_if_needed(
            context.round_num
        )
        if compaction_receipt.get("status") == "error":
            fatal_reasons = list(outcome.get("fatal_reasons") or [])
            fatal_reasons.append(
                "cache_compaction_rhythm:" + str(
                    compaction_receipt.get("reason") or "unknown"
                )
            )
            outcome["fatal_reasons"] = fatal_reasons
            outcome["status"] = "unsettled"
    status = str(outcome.get("status") or "settled")
    if status not in {"settled", "degraded", "unsettled"}:
        outcome = {
            "status": "unsettled",
            "fatal_reasons": [f"invalid_cleanup_status:{status}"],
        }
        status = "unsettled"
    outcome = apply_failures(outcome)
    status = outcome["status"]
    if outcome.get("frame_ref"):
        context.cleanup_frame = FrameRef(**outcome["frame_ref"])
        dispatch(
            "cleanup_frame_settled",
            outcome,
        )
    if status == "unsettled":
        payload = {**common, **outcome}
        emit("cleanup_obligation_failed", payload)
        emit("round_unsettled", payload)
        return apply_failures(outcome)
    payload = {
        **common,
        "status": status,
        "degraded_reasons": list(outcome.get("degraded_reasons") or []),
        "fatal_reasons": list(outcome.get("fatal_reasons") or []),
    }
    if outcome.get("frame_ref"):
        payload["frame_ref"] = outcome["frame_ref"]
    emit("cleanup_obligation_settled", payload)
    outcome = apply_failures(outcome)
    if outcome["status"] == "unsettled":
        emit("round_unsettled", {**common, **outcome})
        return apply_failures(outcome)
    payload["status"] = outcome["status"]
    emit("round_settled", payload)
    outcome = apply_failures(outcome)
    if outcome["status"] == "unsettled":
        emit("round_unsettled", {**common, **outcome})
        return apply_failures(outcome)
    try:
        store.close_round(
            context.round_num,
            final_response_source=closeout_final_response_source(
                result,
                user_stop=bool(result.get("_user_stop_requested")),
            ),
            final_response=result.get("response", ""),
        )
    except Exception as exc:
        outcome = {
            **outcome,
            "status": "unsettled",
            "fatal_reasons": list(outcome.get("fatal_reasons") or []) + [
                f"round_close:{exc}"
            ],
        }
        emit("round_unsettled", {**common, **outcome})
        return apply_failures(outcome)
    dispatch("round_closed", {**common, "status": "closed"})
    return apply_failures(outcome)
