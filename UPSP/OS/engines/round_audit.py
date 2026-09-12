"""Round audit facade shared by Runtime and step runners."""
from data.round_snapshot_store import (
    RoundSnapshotStore,
    reaction_popup_snapshot_status,
)
from data.round_retention import (
    active_round_retention_receipt_path,
    enforce_round_retention,
)


import inspect
import os
from paths import STM_CONTEXT_DIR


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
        context_root = context_root or STM_CONTEXT_DIR
        params = self.services.audit_params()

        def enforce_current_policy():
            current = self.services.audit_params()
            receipt_path = None
            if os.path.normcase(os.path.abspath(context_root)) == os.path.normcase(
                os.path.abspath(STM_CONTEXT_DIR)
            ):
                receipt_path = active_round_retention_receipt_path()
            return enforce_round_retention(
                os.path.join(context_root, "round"),
                retention_count=current.get("round_snapshot_retention", 8),
                max_mib=current.get("round_snapshot_max_mib", 256),
                receipt_path=receipt_path,
            )

        return RoundSnapshotStore(
            context_root=context_root,
            retention_count=params.get("round_snapshot_retention", 8),
            retention_max_mib=params.get("round_snapshot_max_mib", 256),
            retention_enforcer=enforce_current_policy,
        )

    def start(self, round_num, round_type, input_snapshot=None):
        return self.get_store().start_round(
            round_num,
            round_type=round_type,
            input_snapshot=input_snapshot,
        )

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
            active_protocol_tool_guides=None,
            cache_compaction_call=False):
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
                cache_compaction_call=cache_compaction_call,
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
            active_protocol_tool_guides=None,
            cache_compaction_call=False):
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
                    cache_compaction_call=cache_compaction_call,
                )
                prepared["logical_call_id"] = logical_call_id
                prepared["route_slot"] = attempt
                tried_endpoint_fingerprints.add(
                    executor._endpoint_fingerprint(prepared.get("endpoint_config")))
            except Exception as exc:
                try:
                    store.record_llm_error(round_num, phase, iteration, exc)
                except Exception:
                    pass
                raise
            self._record_step_input_or_fail_closed(
                store,
                phase,
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
                previous_sink = executor.bind_stream_event_sink(
                    lambda event_type, payload: store.record_llm_stream_event(
                        round_num, phase, iteration, event_type, payload))
                try:
                    return executor.call_prepared_once(prepared)
                finally:
                    executor.bind_stream_event_sink(previous_sink)
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
        if not executor._allows_provider_failover(error):
            return None
        tier = prepared.get("tier") or prepared.get("endpoint")
        return executor._fallback_tier(
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
        from engines.executor import APIExecutor
        return isinstance(self.services.executor, APIExecutor)
