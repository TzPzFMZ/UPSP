"""Shared dependencies for Runtime step runners."""
from dataclasses import dataclass, field
import math
import os

from assembly.context import ContextAssembler
from data.alert_store import AlertStore
from data.action_recovery_store import ActionRecoveryStore
from data.config_store import ConfigStore
from data.connectivity_store import ConnectivityStore
from data.context_store import ContextStore
from data.container_store import ContainerStore
from data.dream_store import DreamStore
from data.memory_heat import MemoryHeat
from data.memory_index import MemoryIndex
from data.memory_store import MemoryStore
from data.prompt_cache_telemetry import total_input_tokens
from data.relation_store import RelationStore
from data.resident_list_store import ResidentListStore
from data.state_backup_store import StateBackupStore
from data.state_store import StateStore
from data.workbench import WorkbenchStore
from engines.executor import APIExecutor
from engines.general_tool_dispatcher import GeneralToolDispatcher
from engines.heartbeat import (
    HEARTBEAT_QUALIFIER_FLAGS,
    HEARTBEAT_TRIGGER_GROUPS,
    HeartbeatManager,
)
from engines.protocol_tool_dispatcher import ProtocolToolDispatcher
from paths import STATE_JSON
from logic.interaction_meta import (
    begin_interaction_instance,
    set_local_default_relation,
    switch_interaction_relation,
)
from logic.memory_recall import MemoryRecallProcessor
from logic.focus_retirement_migration import migrate_focus_retirement
from logic.state_settlement import StateSettlementError, settle_due_state


@dataclass
class RuntimeServices:
    sm: StateStore
    heat: MemoryHeat
    cfg: ConfigStore
    connectivity_store: ConnectivityStore
    hb: HeartbeatManager
    executor: APIExecutor
    ctx_store: ContextStore
    assembler: ContextAssembler
    alert_store: AlertStore
    workbench: WorkbenchStore
    dream_store: DreamStore
    state_backup_store: StateBackupStore
    memory_store: MemoryStore
    memory_index: MemoryIndex
    container_store: ContainerStore
    relation_store: RelationStore
    resident_store: ResidentListStore
    action_recovery_store: ActionRecoveryStore
    protocol_tool_dispatcher: ProtocolToolDispatcher
    general_tool_dispatcher: GeneralToolDispatcher
    memory_recall: object = None
    on_round_complete: object = None
    cache_pressure_observation: dict = field(default_factory=dict)

    @classmethod
    def create(cls, state_store=None, heartbeat=None, executor=None,
               assembler=None, heat=None, ctx_store=None, config_store=None,
               alert_store=None, connectivity_store=None, workbench_store=None,
               dream_store=None,
               state_backup_store=None, memory_store=None, memory_index=None,
               container_store=None, relation_store=None, resident_store=None,
               action_recovery_store=None):
        sm = state_store or StateStore()
        heat = heat or MemoryHeat()
        cfg = config_store or ConfigStore()
        active_endpoint_ids = getattr(cfg, "get_active_model_profile_ids", None)
        setup_endpoint_ids = getattr(cfg, "get_model_profile_ids_for_phase", None)
        connectivity_store = connectivity_store or ConnectivityStore(
            active_endpoint_ids=active_endpoint_ids,
            recovery_endpoint_ids=(
                (lambda: setup_endpoint_ids("setup"))
                if callable(setup_endpoint_ids)
                else None
            ),
        )
        hb = heartbeat or HeartbeatManager(
            sm,
            config_store=cfg,
            memory_heat=heat,
            connectivity_store=connectivity_store,
        )
        ctx_store = ctx_store or ContextStore(state_store=sm, config_store=cfg)
        if getattr(ctx_store, "state_store", None) is None:
            ctx_store.state_store = sm
        assembler = assembler or ContextAssembler(
            state_store=sm,
            context_store=ctx_store,
            config_store=cfg,
        )
        if getattr(assembler, "state_store", None) is None:
            assembler.state_store = sm
        if getattr(assembler, "context_store", None) is None:
            assembler.context_store = ctx_store
        if getattr(assembler, "config_store", None) is None:
            assembler.config_store = cfg
        if getattr(getattr(assembler, "popup", None), "state_store", None) is None:
            try:
                assembler.popup.state_store = sm
            except Exception:
                pass
        executor = executor or APIExecutor(
            config_store=cfg,
            connectivity_store=connectivity_store,
            context_dir=getattr(assembler, "_context_dir", None),
        )
        if isinstance(executor, APIExecutor) and hasattr(executor, "bind_context_dir"):
            executor.bind_context_dir(getattr(assembler, "_context_dir", None))
        assembler_resident_store = getattr(assembler, "resident_store", None)
        if (
            resident_store is not None
            and assembler_resident_store is not None
            and resident_store is not assembler_resident_store
        ):
            raise ValueError("resident_store_injection_conflict")
        resolved_resident_store = (
            resident_store
            or assembler_resident_store
            or ResidentListStore(os.path.join(
                (
                    getattr(assembler, "_context_dir", None)
                    or os.path.join(
                        os.path.dirname(
                            str(getattr(sm, "path", "") or STATE_JSON)),
                        "STM",
                        "context",
                    )
                ),
                "resident_list.json",
            ))
        )
        assembler.resident_store = resolved_resident_store
        context_dir = os.path.abspath(
            getattr(assembler, "_context_dir", None)
            or os.path.join(
                os.path.dirname(str(getattr(sm, "path", "") or STATE_JSON)),
                "STM",
                "context",
            )
        )
        resolved_action_recovery_store = action_recovery_store or ActionRecoveryStore(
            os.path.join(context_dir, "action_recovery_pending.json"),
        )
        resolved_action_recovery_store.load()
        services = cls(
            sm=sm,
            heat=heat,
            cfg=cfg,
            connectivity_store=connectivity_store,
            hb=hb,
            executor=executor,
            ctx_store=ctx_store,
            assembler=assembler,
            alert_store=alert_store or AlertStore(),
            workbench=workbench_store or WorkbenchStore(),
            dream_store=dream_store or DreamStore(),
            state_backup_store=None,
            memory_store=memory_store if memory_store is not None else MemoryStore(),
            memory_index=memory_index if memory_index is not None else MemoryIndex(),
            container_store=(
                container_store if container_store is not None else ContainerStore()
            ),
            relation_store=(
                relation_store if relation_store is not None else RelationStore()
            ),
            resident_store=resolved_resident_store,
            action_recovery_store=resolved_action_recovery_store,
            protocol_tool_dispatcher=ProtocolToolDispatcher(),
            general_tool_dispatcher=GeneralToolDispatcher(
                action_recovery_store=resolved_action_recovery_store),
        )
        # Context indexes must observe the same Runtime stores.  Falling back to
        # path-global stores here can read or normalize another active persona.
        assembler.memory_heat = services.heat
        assembler.memory_store = services.memory_store
        assembler.container_store = services.container_store
        assembler.relation_store = services.relation_store
        services.state_backup_store = state_backup_store or services.default_state_backup_store()
        services.memory_recall = MemoryRecallProcessor(
            memory_store=services.memory_store,
            heat=services.heat,
            assembler=services.assembler,
        )
        return services

    def migrate_focus_retirement_on_startup(self):
        receipt = migrate_focus_retirement(
            state_store=self.sm,
            workbench=self.workbench,
            container_store=self.container_store,
            relation_store=self.relation_store,
            resident_store=self.resident_store,
            assembler=self.assembler,
        )
        return receipt

    def audit_params(self):
        return self.cfg.get_audit_params()

    def restore_cache_compaction_due_on_startup(self):
        if self.ctx_store.has_cache_compaction_debt():
            self.ctx_store.recover_cache_compaction_debt()
        # v3 is consumed by the next natural Round's Reaction loop. It is not
        # a heartbeat source and must never open a rhythm Round by itself.
        if bool((self.sm.get_flags() or {}).get("cache_compaction_due")):
            self.sm.clear_flags(["cache_compaction_due"])
        try:
            slots = self.workbench.active_guide_slots()
            guide_id = str((slots or {}).get("rhythm") or "").strip()
            guide = self.workbench.load_guide(guide_id) if guide_id else {}
            if str(guide.get("kind") or "") == "cache_compaction_rhythm_guide":
                self.workbench.clear_active_guide(guide_id)
        except (AttributeError, FileNotFoundError, ValueError):
            pass

    def reconcile_context_cache_lifecycle_on_startup(self):
        return self.ctx_store.reconcile_now_cache_lifecycle_on_startup()

    def reconcile_reasoning_progress_on_startup(self):
        return self.ctx_store.reconcile_reasoning_progress_on_startup()

    def set_local_default_relation(self, card_id):
        return set_local_default_relation(self.sm, self.relation_store, card_id)

    def begin_interaction_instance(self, card_id=None):
        return begin_interaction_instance(self.sm, self.relation_store, card_id)

    def switch_interaction_relation(self, card_id):
        return switch_interaction_relation(self.sm, self.relation_store, card_id)

    def default_state_backup_store(self):
        retention = self.audit_params().get("state_backup_retention", 8)
        state_path = os.path.abspath(getattr(self.sm, "path", "") or "")
        default_state_path = os.path.abspath(STATE_JSON)
        if state_path and state_path != default_state_path:
            return StateBackupStore(
                os.path.join(os.path.dirname(state_path), "state_backups.jsonl"),
                retention_count=retention,
            )
        return StateBackupStore(retention_count=retention)

    def settle_idle_feelings(self):
        self.hb.pause()
        succeeded = False
        try:
            receipt = settle_due_state(self.sm, self.relation_store)
            succeeded = receipt.get("status") in {"applied", "skipped"}
            return receipt
        except StateSettlementError as exc:
            try:
                self.sm._set_internal(
                    "base.meta.last_error",
                    f"本地感受结算失败: {exc}",
                )
            except Exception:
                pass
            return exc.receipt
        finally:
            self.hb.resume(run_tick=succeeded)

class EngineComponent:
    def __init__(self, services, audit=None):
        self.services = services
        self.audit = audit

    def __getattr__(self, name):
        return getattr(self.services, name)

    def _get_round_audit_store(self):
        return self.audit.get_store()

    def _round_audit_start(self, round_num, round_type, input_snapshot=None):
        self.audit.start(round_num, round_type, input_snapshot=input_snapshot)

    def _round_audit_parsed(self, round_num, phase, iteration, parsed):
        self.audit.record_parsed(round_num, phase, iteration, parsed)

    def _round_audit_settlement(self, round_num, phase, iteration, settlement):
        self.audit.record_settlement(round_num, phase, iteration, settlement)

    def _transition_current_cache(
            self, round_num, *, boundary, consumer_frame_id, phase,
            iteration=None, **kwargs):
        transition_kwargs = dict(kwargs)
        if (
                transition_kwargs.get("expire_call_transients") is True
                and boundary in {
                    "reaction_provider_return", "cleanup_provider_return",
                }):
            transition_kwargs.setdefault("transient_round", round_num)
            transition_kwargs.setdefault("transient_target_step", phase)
            transition_kwargs.setdefault(
                "transient_target_iteration", iteration)
        receipt = self.ctx_store.transition_current_cache(
            boundary=boundary,
            consumer_frame_id=consumer_frame_id,
            **transition_kwargs,
        )
        if self.audit is not None:
            self._get_round_audit_store().append_event(
                round_num,
                "current_cache_transition",
                receipt,
                phase=phase,
                iteration=iteration,
            )
        return receipt

    def _call_llm_with_round_audit(
            self,
            phase,
            system,
            messages,
            round_num,
            iteration=1,
            active_protocol_tool_guides=None,
            cache_compaction_call=False):
        try:
            return self.audit.call_llm(
                phase,
                system,
                messages,
                round_num,
                iteration=iteration,
                active_protocol_tool_guides=active_protocol_tool_guides,
                cache_compaction_call=cache_compaction_call,
            )
        except Exception as exc:
            if self._is_context_too_long_error(exc):
                self._remember_cache_pressure({
                    "kind": "context_too_long",
                    "frame_id": f"R{int(round_num):06d}:{phase}:{int(iteration)}",
                    "endpoint": str(getattr(exc, "endpoint", "") or ""),
                    "input_tokens": None,
                    "context_window": None,
                    "usage_ratio": None,
                })
            raise

    def _load_time_milestones(self):
        return self.cfg.get_round_time_milestones()

    @staticmethod
    def _runtime_usage_token_count(value):
        if isinstance(value, bool) or not isinstance(value, int):
            return None
        return value if value >= 0 else None

    @staticmethod
    def _is_context_too_long_error(exc):
        try:
            if int(getattr(exc, "status_code", 0) or 0) == 413:
                return True
        except (TypeError, ValueError):
            pass
        text = str(exc or "").lower()
        return any(marker in text for marker in (
            "context_length_exceeded",
            "maximum context length",
            "context window exceeded",
            "exceeds the context window",
            "too many tokens",
            "request too large",
        ))

    def _remember_cache_pressure(self, observation):
        if self.ctx_store.has_cache_compaction_debt():
            return
        current = self.services.cache_pressure_observation
        if current and current.get("kind") != "unknown_window_fallback":
            return
        self.services.cache_pressure_observation = dict(observation or {})

    def _update_token_usage(
            self, result, *, round_num=None, phase=None, iteration=None):
        result = result if isinstance(result, dict) else {}
        raw_usage = result.get("raw_usage")
        input_count = (
            total_input_tokens(raw_usage)
            if isinstance(raw_usage, dict) and raw_usage
            else self._runtime_usage_token_count(result.get("tokens_input"))
        )
        output_count = self._runtime_usage_token_count(
            result.get("tokens_output")
        )
        endpoint = str(result.get("endpoint") or "primary").strip() or "primary"
        envelope = result.get("provider_request_envelope")
        frozen_window_present = (
            isinstance(envelope, dict) and "context_window_tokens" in envelope
        )
        window_size = (
            envelope.get("context_window_tokens")
            if frozen_window_present else None
        )
        if not frozen_window_present:
            try:
                if self.cfg and hasattr(
                        self.cfg, "get_context_window_for_endpoint"):
                    window_size = self.cfg.get_context_window_for_endpoint(endpoint)
            except Exception:
                window_size = None
        window_size = self._runtime_usage_token_count(window_size)
        if input_count is None:
            return
        if not window_size:
            self._remember_cache_pressure({
                "kind": "unknown_window_fallback",
                "endpoint": endpoint,
                "input_tokens": input_count,
                "context_window": None,
                "usage_ratio": None,
            })
            return

        usage_ratio = input_count / window_size
        try:
            self.sm.update_token_usage(
                current_tokens=input_count,
                window_size=window_size,
                usage_ratio=usage_ratio,
                input_tokens=input_count,
                output_tokens=output_count,
            )
        except Exception:
            pass

        try:
            warning_ratio = float(
                (self.cfg.get_token_params() or {})["warning_ratio"]
            )
        except (AttributeError, KeyError, TypeError, ValueError):
            return
        if not math.isfinite(warning_ratio) or not 0 < warning_ratio <= 1:
            return
        if usage_ratio >= warning_ratio:
            observation = {
                "kind": "token_ratio",
                "endpoint": endpoint,
                "input_tokens": input_count,
                "context_window": window_size,
                "round_context_window_tokens": window_size,
                "usage_ratio": usage_ratio,
                "threshold": warning_ratio,
            }
            if round_num is not None and phase and iteration is not None:
                observation["frame_id"] = (
                    f"R{int(round_num):06d}:{phase}:{int(iteration)}"
                )
            self._remember_cache_pressure(observation)
