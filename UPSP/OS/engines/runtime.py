"""Three-step round orchestrator; step-specific work lives in its runners."""
from datetime import datetime
from collections import deque
import traceback
from constants import local_now
from engines.cleanup_pipeline import CleanupPipeline
from engines.heartbeat import round_decision_from_heartbeat_flags, round_type_from_heartbeat_flags
from engines.organ_runtime import OrganRuntime, organ_runtime_context
from engines.product_committer import RuntimeProductCommitter
from engines.reaction_loop import ReactionLoopRunner
from engines.round_audit import RoundAuditRecorder
from engines.round_context import RoundContext, RuntimeTrigger
from engines.round_lifecycle import settle_round
from engines.runtime_rhythm import chronicle_state_sample, park_interaction_for_api_probe, prepare_chronicle_focus_for_active_guide, prepare_round_before_setup, refresh_round_alert_recovery, restore_interaction_after_api_probe
from engines.runtime_services import RuntimeServices
from engines.runtime_control import RuntimeControl
from engines.tool_approval import ToolApprovalCoordinator, request_runtime_tool_approval
from engines.runtime_task_guidance import materialize_work_intent_debt_if_needed, prepare_task_bootstrap_guide, record_work_intent_debt_if_needed
from engines.setup_runner import SetupRunner
from errors import ProviderCallCancelled
from logic.cache_compaction_guide import cache_compaction_due_receipt
from logic.feeling_lookup import FeelingWordTable
from logic.rhythm_guide_materializer import materialize_current_rhythm_guide
from logic.sandbox_grant import load_sandbox_grant
from logic.execution_permission import DEFAULT_LEVEL, ExecutionPermissionChain, execution_permission_audit
from logic.single_round_probe_policy import single_round_probe_enabled
from paths import ORGAN_TOPOLOGY
class Runtime:
    _SERVICE_ATTRS = {
        "sm", "heat", "cfg", "connectivity_store", "evolution_store", "hb",
        "executor", "ctx_store", "assembler", "alert_store", "workbench",
        "dream_store", "state_backup_store", "memory_store", "memory_index", "container_store",
        "relation_store", "protocol_tool_dispatcher",
        "general_tool_dispatcher", "on_round_complete",
    }

    def __init__(self, state_store=None, heartbeat=None, executor=None,
                 assembler=None, heat=None, ctx_store=None, config_store=None,
                 alert_store=None, connectivity_store=None,
                 workbench_store=None, dream_store=None, evolution_store=None,
                 state_backup_store=None, memory_store=None, memory_index=None,
                 container_store=None, relation_store=None,
                 organ_topology_path=None, organ_handlers=None,
                 organ_context_providers=None):
        self.services = RuntimeServices.create(
            state_store=state_store,
            heartbeat=heartbeat,
            executor=executor,
            assembler=assembler,
            heat=heat,
            ctx_store=ctx_store,
            config_store=config_store,
            alert_store=alert_store,
            connectivity_store=connectivity_store,
            workbench_store=workbench_store,
            dream_store=dream_store,
            evolution_store=evolution_store,
            state_backup_store=state_backup_store,
            memory_store=memory_store,
            memory_index=memory_index,
            container_store=container_store,
            relation_store=relation_store,
        )
        self.audit = RoundAuditRecorder(self.services)
        self.product_committer = RuntimeProductCommitter(self.services)
        self.organ_runtime = OrganRuntime(
            organ_topology_path or ORGAN_TOPOLOGY,
            self.product_committer,
            handlers=organ_handlers,
            context_providers=organ_context_providers,
            audit=self.audit,
        )
        self.setup_runner = SetupRunner(self.services, self.audit)
        self.reaction_loop_runner = ReactionLoopRunner(self.services, self.audit)
        self.cleanup_pipeline = CleanupPipeline(self.services, self.audit)
        for component in (
                self.setup_runner, self.reaction_loop_runner,
                self.cleanup_pipeline):
            component.organ_runtime = self.organ_runtime
        self.reaction_loop_runner.product_committer = self.product_committer
        self._trigger_queue = deque()
        self._trigger_seq = 0
        self._latest_setup_trigger_seq = 0
        self.control = RuntimeControl()
        self.tool_approval = ToolApprovalCoordinator()
        self.general_tool_dispatcher.approval_fn = self._request_tool_approval
        self.on_round_started = None
        self.on_round_finished = None
        self.cleanup_pipeline.stage_callback = self.control.set_stage
        self.permission_chain = ExecutionPermissionChain(self.executor, self.assembler, self.general_tool_dispatcher, self.setup_runner, self.reaction_loop_runner, self.cleanup_pipeline)

    def __setattr__(self, name, value):
        object.__setattr__(self, name, value)
        services = self.__dict__.get("services")
        if services is not None and name in self._SERVICE_ATTRS:
            setattr(services, name, value)
        for component_name in ("setup_runner", "reaction_loop_runner", "cleanup_pipeline"):
            component = self.__dict__.get(component_name)
            if component is not None and hasattr(component, name):
                object.__setattr__(component, name, value)

    @property
    def on_round_complete(self):
        return self.services.on_round_complete

    @on_round_complete.setter
    def on_round_complete(self, value):
        self.services.on_round_complete = value

    def __getattr__(self, name):
        for component in (
            self.__dict__.get("setup_runner"),
            self.__dict__.get("reaction_loop_runner"),
            self.__dict__.get("cleanup_pipeline"),
        ):
            if component is None:
                continue
            try:
                return object.__getattribute__(component, name)
            except AttributeError:
                continue
        services = self.__dict__.get("services")
        if services is None:
            raise AttributeError(name)
        return getattr(services, name)

    def _new_trigger(self, round_type, flags):
        self._trigger_seq += 1
        flags = flags or {}
        messages = []
        if flags.get("user_message_waiting") and round_type in {
                "interactive", "rhythm"}:
            dequeue = getattr(self.hb, "dequeue_messages", None)
            if callable(dequeue):
                messages = list(dequeue() or [])
        permission_level = self.permission_chain.consume(messages, flags)
        return RuntimeTrigger(
            trigger_id=f"T{self._trigger_seq:08d}",
            trigger_seq=self._trigger_seq,
            observed_at=local_now().isoformat(),
            round_type=round_type,
            flags=dict(flags or {}),
            messages=tuple(messages),
            execution_permission_level=permission_level,
        )

    def enqueue_trigger(self, flags, state=None):
        round_type = self._determine_round_type(flags, state)
        if round_type is None:
            return None
        trigger = self._new_trigger(round_type, flags)
        self._trigger_queue.append(trigger)
        return trigger

    def _setup_candidate_is_current(self, trigger):
        newest = max(
            [self._latest_setup_trigger_seq]
            + [item.trigger_seq for item in self._trigger_queue]
        )
        if trigger.trigger_seq < newest:
            return False
        self._latest_setup_trigger_seq = trigger.trigger_seq
        return True

    def run_forever(self):
        self.sm.load()
        self.hb.start()
        try:
            while not self.control.shutdown.is_set():
                self.sm.set_phase("idle")
                self.hb.wait_for_wakeup()
                if self.control.shutdown.is_set():
                    break
                if self.control.stop_latched:
                    self.hb.pause(); continue
                state = self.sm.load()
                flags = state.get("base", {}).get("heartbeat_flags", {})
                if self.enqueue_trigger(flags, state) is None:
                    if flags.get("feeling_settle_due"):
                        self.services.settle_idle_feelings()
                    continue
                while self._trigger_queue:
                    trigger = self._trigger_queue.popleft()
                    try:
                        self._run_one_round(
                            trigger.round_type, self.sm.load(), trigger.flags,
                            trigger=trigger)
                        if self.control.stop_latched:
                            self._trigger_queue.clear()
                            break
                    except Exception as exc:
                        traceback.print_exc()
                        self.control.handle_round_error(self, trigger, exc)
        except KeyboardInterrupt:
            pass
        finally:
            self.hb.stop()

    def request_shutdown(self):
        self.tool_approval.cancel()
        self.control.request_shutdown(self)

    def release_stop_latch(self):
        return self.control.release_stop_latch(self.executor)

    def submit_message(self, message, execution_permission_level=DEFAULT_LEVEL):
        if not self.release_stop_latch():
            return False
        self.permission_chain.queue(execution_permission_level)
        self.hb.enqueue_message(message)
        self.hb.resume()
        return True

    def request_stop(self):
        receipt = self.control.request_stop(self)
        if receipt.get("accepted"):
            self.tool_approval.cancel()
        return receipt

    def cancel_pending_input(self):
        self.permission_chain.cancel_pending()
        return self.control.cancel_pending_input(self)

    def runtime_status(self):
        return self.tool_approval.attach_status(self.control.snapshot(self.hb))

    def resolve_tool_approval(self, approval_id, decision):
        return self.tool_approval.resolve(approval_id, decision)

    def _request_tool_approval(self, payload):
        return request_runtime_tool_approval(self, payload)

    def _run_one_round(
        self, round_type, state, flags, *, probe_policy=None, trigger=None):
        trigger = trigger or self._new_trigger(round_type, flags or {})
        trigger, parked_probe_input = park_interaction_for_api_probe(self, trigger, flags or {})
        probing = bool((flags or {}).get("api_degraded"))
        if probing and not self.control.begin_pre_setup_probe():
            return {"status": "round_stopped", "response": "", "error": None}
        try:
            (
                round_type, flags, pre_setup_cleared,
                pre_setup_api_probe, probe_guard, skipped,
            ) = prepare_round_before_setup(self, round_type, state, flags)
        finally:
            if probing:
                self.control.end_pre_setup_probe()
        if skipped:
            return skipped
        trigger, flags = restore_interaction_after_api_probe(
            self, trigger, flags, pre_setup_api_probe, parked_probe_input)
        self.audit.reset()
        audit_input = {
            "flags": dict(flags or {}),
            "trigger": trigger.as_dict(),
            "pre_setup_alert_recovery": {
                "cleared_flags": list(pre_setup_cleared),
                "effective_round_type": round_type,
            },
            "pre_setup_api_probe": dict(pre_setup_api_probe or {}),
            "context_profile": str(
                getattr(self.assembler, "context_profile", "full") or "full"
            ),
            "execution_permission": execution_permission_audit(
                trigger.execution_permission_level),
        }
        if probe_guard.get("enabled"):
            audit_input["single_round_probe"] = dict(
                probe_policy or probe_guard
            )

        def establish():
            number = self.sm.increment_round()
            self.audit.start(number, round_type, audit_input)
            return number

        round_num = self.control.establish_round(round_type, establish)
        if round_num is None:
            self.cancel_pending_input()
            return {
                "status": "round_stopped",
                "response": "",
                "_user_stop_requested": True,
            }
        state = self.sm.load()
        result = {"aborted": False, "response": "", "error": None}
        interaction_meta = {
            "interaction_object": "unknown",
            "identity_status": "unknown",
            "interaction_source": "unresolved",
        }
        context = RoundContext(
            round_num=round_num,
            round_type=round_type,
            state=state,
            flags=dict(flags or {}),
            interaction_meta=interaction_meta,
            trigger=trigger,
            topology_version=self.organ_runtime.topology_version,
            execution_permission_level=trigger.execution_permission_level,
        )
        last_phase = "presub"
        self.permission_chain.apply(trigger.execution_permission_level)
        try:
            if callable(self.on_round_started):
                self.on_round_started(round_num, round_type)
            self._update_daily_if_needed(state, round_type)

            last_phase = "setup"
            setup_result = self.setup_runner.run(context)
            if setup_result is None:
                raise RuntimeError("setup_result_missing")
            context.setup_frame = setup_result.frame_ref
            context.user_input_text = setup_result.user_input_text
            context.setup_messages = list(setup_result.setup_messages or [])
            if not self._setup_candidate_is_current(trigger):
                result = {
                    "aborted": False,
                    "response": "",
                    "error": None,
                    "_stale_setup_discarded": trigger.trigger_id,
                    "_user_input_text": setup_result.user_input_text,
                }
                return result
            commit_setup = getattr(self.setup_runner, "commit", None)
            if callable(commit_setup):
                commit_setup(context, setup_result)
            result = dict(setup_result.raw_result)
            result["_user_input_text"] = setup_result.user_input_text
            interaction_meta = dict(setup_result.interaction_meta or interaction_meta)
            context.interaction_meta = interaction_meta
            self._refresh_round_alert_recovery(context)
            self.organ_runtime.dispatch(
                "setup_frame_settled",
                setup_result.frame_ref,
                {"intent": setup_result.intent},
                organ_runtime_context(self, context),
            )

            last_phase = "reaction"
            self.control.set_stage("reaction")
            if setup_result.intent.get("security_verdict") == "reject":
                reject_reason = (
                    setup_result.intent.get("reject_reason")
                    or "setup_security_reject"
                )
                result = {
                    "aborted": False,
                    "response": f"setup 起手步未完成，已跳过反应步：{reject_reason}",
                    "error": f"setup rejected before reaction: {reject_reason}",
                    "_setup_reject_reason": reject_reason,
                    "_setup_messages": setup_result.setup_messages,
                    "_user_input_text": setup_result.user_input_text,
                }
            elif round_type == "standby" and setup_result.intent.get("standby_skip_reaction"):
                result = {
                    "aborted": False,
                    "response": "",
                    "error": None,
                    "standby_skipped_reaction": True,
                    "_setup_messages": setup_result.setup_messages,
                    "_user_input_text": setup_result.user_input_text,
                }
            else:
                self._materialize_runtime_rhythm_guide(context)
                self._prepare_chronicle_focus_for_round(
                    context.round_type,
                    context.state,
                    context.round_num,
                )
                self._record_work_intent_debt_if_needed(
                    context,
                    setup_result,
                )
                self._prepare_task_bootstrap_guide(
                    setup_result.intent,
                    context=context,
                )
                self._materialize_work_intent_debt_if_needed(context)
                self.sm.set_phase("main")
                result = self._run_reaction_loop(
                    context.state,
                    context.round_type,
                    setup_result.intent.get("mount_requests", []),
                    interaction_meta=setup_result.interaction_meta,
                    context=context,
                    setup_result=setup_result,
                )
                result["_setup_messages"] = setup_result.setup_messages
                result["_user_input_text"] = setup_result.user_input_text
                interaction_meta = result.get("_interaction_meta", interaction_meta)
                context.interaction_meta = interaction_meta
                result["_interaction_meta"] = interaction_meta

        except Exception as exc:
            if isinstance(exc, ProviderCallCancelled):
                result = {
                    "aborted": True,
                    "response": "",
                    "error": None,
                    "_failed_phase": last_phase,
                    "_user_stop_requested": True,
                }
                try:
                    self.audit.get_store().append_event(
                        round_num,
                        "provider_call_cancelled",
                        {"reason": "user_stopped", "stage": last_phase},
                        phase=last_phase,
                    )
                except Exception:
                    pass
            else:
                traceback.print_exc()
                result = self.control.step_exception_result(last_phase, exc)
        finally:
            if self.control.stop_requested.is_set():
                result["_user_stop_requested"] = True
            try:
                self.sm.set_phase("post")
                self.control.set_stage("cleanup_model")
                context.state = self.sm.load()
                if isinstance(result, dict):
                    result["_interaction_meta"] = interaction_meta
                    result["_user_input_text"] = context.user_input_text
                context.interaction_meta = interaction_meta
                result["_settlement"] = settle_round(self, context, result)
            except Exception as exc:
                traceback.print_exc()
                result.update(status="runtime_failed", _settlement={"status": "unsettled", "degraded_reasons": [], "fatal_reasons": [f"cleanup_step_exception:{type(exc).__name__}"]})
                try:
                    self.sm.set("base.meta.last_error", f"cleanup step exception R{round_num}: {exc}")
                except Exception:
                    pass
            try:
                self.sm.set_phase("idle")
            except Exception:
                pass
            self.permission_chain.finish(trigger.execution_permission_level, result, self.sm)
            stopped, settlement = bool(result.get("_user_stop_requested")), str((result.get("_settlement") or {}).get("status") or "")
            latch_until_explicit = stopped or settlement in {"degraded", "unsettled"}
            try:
                try:
                    if latch_until_explicit:
                        self.hb.pause()
                    elif not single_round_probe_enabled():
                        self.hb.resume()
                except Exception:
                    pass
                if callable(self.on_round_finished):
                    try:
                        self.on_round_finished(round_num, round_type, result)
                    except Exception:
                        traceback.print_exc()
            finally:
                self.control.finish_round(latch_until_explicit)
        return result
    def _run_reaction_loop(self, *args, **kwargs):
        if args and isinstance(args[0], RoundContext):
            return self.reaction_loop_runner.run(*args, **kwargs)
        state, round_type, mount_ids = args[:3]
        context = kwargs.pop("context", None)
        setup_result = kwargs.pop("setup_result", None)
        if context is not None and setup_result is not None:
            return self.reaction_loop_runner.run(context, setup_result)
        return self.reaction_loop_runner.run(
            state,
            round_type,
            mount_ids,
            interaction_meta=kwargs.get("interaction_meta"),
        )

    def _has_active_workbench_work_or_task(self):
        try:
            if str(self.workbench.get("base.active_task") or "").strip():
                return True
            if hasattr(self.workbench, "active_guide_slots"):
                slots = self.workbench.active_guide_slots()
                if str((slots or {}).get("work") or "").strip():
                    return True
                return False
            return bool(str(self.workbench.get("base.active_guide") or "").strip())
        except Exception:
            return False

    def _prepare_task_bootstrap_guide(self, intent, context=None):
        return prepare_task_bootstrap_guide(self, intent, context)

    def _record_work_intent_debt_if_needed(self, context, setup_result):
        return record_work_intent_debt_if_needed(self, context, setup_result)

    def _materialize_work_intent_debt_if_needed(self, context):
        return materialize_work_intent_debt_if_needed(self, context)

    @staticmethod
    def _task_guidance_has_real_user_input(context):
        if context is None:
            return True
        return bool(str(getattr(context, "user_input_text", "") or "").strip())

    @staticmethod
    def _engineering_task_grant():
        try:
            grant = load_sandbox_grant()
        except Exception:
            return {}
        return grant if isinstance(grant, dict) and grant.get("task_root") else {}

    def _run_cleanup(self, *args, **kwargs):
        if args and isinstance(args[0], RoundContext):
            return self.cleanup_pipeline.run(*args, **kwargs)
        round_type, state, result, round_num = args[:4]
        context = kwargs.pop("context", None)
        if context is not None:
            return self.cleanup_pipeline.run(context, result)
        user_input_text = args[4] if len(args) > 4 else kwargs.get("user_input_text", "")
        return self.cleanup_pipeline.run(
            round_type,
            state,
            result,
            round_num,
            user_input_text=user_input_text,
        )

    def _record_cache_compaction_rhythm_if_needed(self, round_num):
        try:
            receipt = cache_compaction_due_receipt(self.ctx_store, round_num)
            if receipt.get("status") == "due":
                debt_saver = getattr(self.ctx_store, "save_cache_compaction_debt", None)
                if callable(debt_saver) and receipt.get("source") != "cache_compaction_debt":
                    debt = debt_saver(receipt, round_num)
                    if debt:
                        receipt["debt_path"] = self.ctx_store.cache_compaction_debt_path()
                self.sm.set_flag("cache_compaction_due", True)
                receipt["set_flags"] = ["cache_compaction_due"]
        except Exception as exc:
            receipt = {"status": "error", "reason": str(exc)}
        if isinstance(receipt, dict) and receipt.get("status") != "skipped":
            try:
                self.audit.get_store().append_event(
                    round_num,
                    "cache_compaction_rhythm",
                    receipt,
                    phase="cleanup",
                )
            except Exception:
                pass
        return receipt

    def _materialize_runtime_rhythm_guide(self, context):
        try:
            base = (context.state or {}).get("base", {})
            runtime = base.get("runtime", {}) if isinstance(base, dict) else {}
            completed = runtime.get("guide_completed_flags", [])
            return materialize_current_rhythm_guide(
                self.workbench,
                context.flags,
                round_num=context.round_num,
                completed_flags=completed,
                context_store=self.ctx_store,
                state_store=self.sm,
                connectivity_store=self.connectivity_store,
            )
        except Exception:
            return None

    def _refresh_round_alert_recovery(self, context):
        return refresh_round_alert_recovery(self, context)

    def _determine_round_type(self, flags, state=None):
        return round_type_from_heartbeat_flags(flags)

    def _determine_round_decision(self, flags, state=None):
        return round_decision_from_heartbeat_flags(flags)

    def _prepare_chronicle_focus_for_round(self, round_type, state, round_num):
        return prepare_chronicle_focus_for_active_guide(
            self, round_type, state, round_num
        )

    @staticmethod
    def _chronicle_state_sample(base):
        return chronicle_state_sample(base)

    def _wake_if_sleeping(self):
        """Deferred compatibility hook; Seed round startup does not call it."""
        return None

    def _update_daily_if_needed(self, state, round_type):
        try:
            meta = state.get("base", {}).get("meta", {})
            last_update = meta.get("last_update")
            if last_update:
                last_date = datetime.fromisoformat(last_update).date()
                today = datetime.now().astimezone().date()
                if today > last_date:
                    self.sm.set("base.meta.daily_round", 0)
        except Exception:
            pass
