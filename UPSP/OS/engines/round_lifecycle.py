"""Runtime-owned causal settlement for one serial Seed Round."""
from engines.round_context import FrameRef
from engines.organ_runtime import organ_runtime_context


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
    runtime._record_cache_compaction_rhythm_if_needed(context.round_num)
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
            final_response_source=str(
                result.get("_final_response_source")
                or (
                    "runtime.user_stop"
                    if result.get("_user_stop_requested")
                    and not result.get("response")
                    else "reaction.final_reply_text"
                )),
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
