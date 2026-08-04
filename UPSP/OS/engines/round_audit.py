"""Round audit facade shared by Runtime and step runners."""
from data.round_snapshot_store import (
    RoundSnapshotStore,
    reaction_popup_snapshot_status,
)


import inspect


class RoundAuditRecorder:
    def __init__(self, services):
        self.services = services
        self._store = None

    def reset(self):
        self._store = self._new_store()
        return self._store

    def get_store(self):
        if self._store is None:
            self._store = self._new_store()
        return self._store

    def _new_store(self):
        context_root = getattr(getattr(self.services, "assembler", None), "_context_dir", None)
        return RoundSnapshotStore(
            context_root=context_root,
            retention_count=self.services.audit_params().get("round_snapshot_retention", 8),
        )

    def start(self, round_num, round_type, input_snapshot=None):
        try:
            self.get_store().start_round(
                round_num,
                round_type=round_type,
                input_snapshot=input_snapshot,
            )
        except Exception:
            pass

    def record_parsed(self, round_num, phase, iteration, parsed):
        try:
            self.get_store().record_parsed_result(
                round_num,
                phase,
                iteration,
                parsed or {},
            )
        except Exception:
            pass

    def record_settlement(self, round_num, phase, iteration, settlement):
        try:
            self.get_store().record_step_settlement(
                round_num,
                phase,
                iteration,
                settlement or {},
            )
        except Exception:
            pass

    def call_llm(
            self,
            phase,
            system,
            messages,
            round_num,
            iteration=1,
            active_protocol_tool_guides=None):
        store = self.get_store()
        if self._executor_uses_prepared_requests():
            result = self._call_prepared_with_audit(
                store,
                phase,
                system,
                messages,
                round_num,
                iteration,
                active_protocol_tool_guides=active_protocol_tool_guides,
            )
        else:
            # Deterministic Runtime fakes and legacy adapters do not perform
            # the prepared APIExecutor provider send.  Preserve their
            # best-effort audit compatibility; the real provider path below
            # is the only path permitted to send a prepared request.
            try:
                store.record_step_input_from_files(
                    round_num,
                    phase,
                    iteration=iteration,
                    messages=messages,
                    system=system,
                )
            except Exception:
                pass
            try:
                if hasattr(self.services.executor, "preview_request_contract"):
                    preview = self.services.executor.preview_request_contract(
                        phase,
                        system,
                        messages,
                        active_protocol_tool_guides=active_protocol_tool_guides,
                    )
                    store.record_llm_call_started(round_num, phase, iteration, preview)
            except Exception:
                pass
            try:
                if self._executor_accepts("active_protocol_tool_guides"):
                    result = self.services.executor.call(
                        phase,
                        system,
                        messages,
                        active_protocol_tool_guides=active_protocol_tool_guides,
                    )
                else:
                    result = self.services.executor.call(phase, system, messages)
            except Exception as exc:
                try:
                    store.record_llm_error(round_num, phase, iteration, exc)
                except Exception:
                    pass
                raise
        try:
            store.record_llm_output(round_num, phase, iteration, result)
        except Exception:
            pass
        return result

    def _call_prepared_with_audit(
            self,
            store,
            phase,
            system,
            messages,
            round_num,
            iteration,
            active_protocol_tool_guides=None):
        executor = self.services.executor
        endpoint = None
        attempt = 1
        logical_call_id = f"R{int(round_num):06d}:{phase}:{int(iteration)}"
        tried_endpoint_fingerprints = set()
        while True:
            try:
                prepared = executor.prepare_provider_request(
                    phase,
                    system,
                    messages,
                    endpoint=endpoint,
                    active_protocol_tool_guides=active_protocol_tool_guides,
                    attempt=attempt,
                )
                prepared["logical_call_id"] = logical_call_id
                prepared["route_slot"] = attempt
                fingerprint = getattr(executor, "_endpoint_fingerprint", None)
                if callable(fingerprint):
                    tried_endpoint_fingerprints.add(
                        fingerprint(prepared.get("endpoint_config"))
                    )
            except Exception as exc:
                try:
                    store.record_llm_error(round_num, phase, iteration, exc)
                except Exception:
                    pass
                raise
            self._record_step_input_or_fail_closed(
                store,
                phase,
                system,
                messages,
                round_num,
                iteration,
            )
            try:
                store.record_llm_call_started(
                    round_num,
                    phase,
                    iteration,
                    prepared,
                )
            except Exception:
                pass
            try:
                call_once = getattr(executor, "call_prepared_once", None)
                binder = getattr(executor, "bind_stream_event_sink", None)
                previous_sink = None
                if callable(binder):
                    previous_sink = binder(
                        lambda event_type, payload: store.record_llm_stream_event(
                            round_num,
                            phase,
                            iteration,
                            event_type,
                            payload,
                        )
                    )
                try:
                    if callable(call_once):
                        return call_once(prepared)
                    return executor.call_prepared(prepared)
                finally:
                    if callable(binder):
                        binder(previous_sink)
            except Exception as exc:
                fallback_tier = self._fallback_tier_for_prepared_error(
                    executor,
                    prepared,
                    exc,
                    tried_endpoint_fingerprints,
                )
                if not fallback_tier or attempt >= 3:
                    try:
                        store.record_llm_error(round_num, phase, iteration, exc)
                    except Exception:
                        pass
                    raise
                try:
                    store.record_llm_call_failed(
                        round_num,
                        phase,
                        iteration,
                        prepared,
                        exc,
                    )
                except Exception:
                    pass
                endpoint = fallback_tier
                attempt += 1

    @staticmethod
    def _record_step_input_or_fail_closed(
            store,
            phase,
            system,
            messages,
            round_num,
            iteration):
        """Persist the request snapshot before any provider call.

        Reaction POPUP stop-loss relies on the stored ``99_popup`` layer as
        its sole model-visible native-feedback fact source.  Therefore a
        reaction call cannot continue when that record is unavailable or
        incomplete.  Setup and cleanup retain their historical best-effort
        audit behavior.
        """
        try:
            snapshot_event = store.record_step_input_from_files(
                round_num,
                phase,
                iteration=iteration,
                messages=messages,
                system=system,
            )
        except Exception as exc:
            if phase == "reaction":
                raise RuntimeError(
                    "reaction_popup_snapshot_record_failed"
                ) from exc
            return None
        if phase != "reaction":
            return snapshot_event
        status = reaction_popup_snapshot_status(snapshot_event)
        if status != "complete":
            raise RuntimeError(
                "reaction_popup_snapshot_incomplete:" + status
            )
        return snapshot_event

    @staticmethod
    def _fallback_tier_for_prepared_error(
            executor,
            prepared,
            error,
            tried_endpoint_fingerprints=None):
        is_transient = getattr(executor, "_is_transient_provider_error", None)
        if not callable(is_transient) or not is_transient(error):
            return None
        fallback = getattr(executor, "_fallback_tier", None)
        if not callable(fallback):
            return None
        tier = prepared.get("tier") or prepared.get("endpoint")
        return fallback(
            tier,
            current_endpoint=prepared.get("endpoint_config"),
            excluded_fingerprints=tried_endpoint_fingerprints,
            step=prepared.get("step"),
        )

    def _executor_accepts(self, parameter_name):
        try:
            signature = inspect.signature(self.services.executor.call)
        except (TypeError, ValueError):
            return False
        for parameter in signature.parameters.values():
            if parameter.kind == inspect.Parameter.VAR_KEYWORD:
                return True
            if parameter.name == parameter_name:
                return True
        return False

    def _executor_uses_prepared_requests(self):
        try:
            from engines.executor import APIExecutor
        except Exception:
            return False
        executor = self.services.executor
        if not isinstance(executor, APIExecutor):
            return False
        call = getattr(executor, "call", None)
        return getattr(call, "__func__", None) is APIExecutor.call
