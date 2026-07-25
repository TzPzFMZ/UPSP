"""Shared dependencies for Runtime step runners."""
from dataclasses import dataclass
import os

from assembly.context import ContextAssembler
from data.alert_store import AlertStore
from data.config_store import ConfigStore
from data.connectivity_store import ConnectivityStore
from data.context_store import ContextStore
from data.container_store import ContainerStore
from data.dream_store import DreamStore
from data.evolution_store import EvolutionStore
from data.memory_heat import MemoryHeat
from data.memory_index import MemoryIndex
from data.memory_store import MemoryStore
from data.relation_store import RelationStore
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
from logic.state_settlement import StateSettlementError, settle_due_state


@dataclass
class RuntimeServices:
    sm: StateStore
    heat: MemoryHeat
    cfg: ConfigStore
    connectivity_store: ConnectivityStore
    evolution_store: EvolutionStore
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
    protocol_tool_dispatcher: ProtocolToolDispatcher
    general_tool_dispatcher: GeneralToolDispatcher
    on_round_complete: object = None

    @classmethod
    def create(cls, state_store=None, heartbeat=None, executor=None,
               assembler=None, heat=None, ctx_store=None, config_store=None,
               alert_store=None, connectivity_store=None, workbench_store=None,
               dream_store=None, evolution_store=None,
               state_backup_store=None, memory_store=None, memory_index=None,
               container_store=None, relation_store=None):
        sm = state_store or StateStore()
        heat = heat or MemoryHeat()
        cfg = config_store or ConfigStore()
        active_endpoint_ids = getattr(cfg, "get_active_model_profile_ids", None)
        connectivity_store = connectivity_store or ConnectivityStore(
            active_endpoint_ids=active_endpoint_ids,
        )
        evolution_store = evolution_store or EvolutionStore()
        hb = heartbeat or HeartbeatManager(
            sm,
            config_store=cfg,
            memory_heat=heat,
            connectivity_store=connectivity_store,
            evolution_store=evolution_store,
        )
        ctx_store = ctx_store or ContextStore(state_store=sm, config_store=cfg)
        if getattr(ctx_store, "state_store", None) is None:
            ctx_store.state_store = sm
        assembler = assembler or ContextAssembler(
            state_store=sm,
            context_store=ctx_store,
        )
        if getattr(assembler, "state_store", None) is None:
            assembler.state_store = sm
        if getattr(assembler, "context_store", None) is None:
            assembler.context_store = ctx_store
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
        services = cls(
            sm=sm,
            heat=heat,
            cfg=cfg,
            connectivity_store=connectivity_store,
            evolution_store=evolution_store,
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
            protocol_tool_dispatcher=ProtocolToolDispatcher(),
            general_tool_dispatcher=GeneralToolDispatcher(),
        )
        services.state_backup_store = state_backup_store or services.default_state_backup_store()
        return services

    def audit_params(self):
        return self.cfg.get_audit_params()

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

    def _call_llm_with_round_audit(
            self,
            phase,
            system,
            messages,
            round_num,
            iteration=1,
            active_protocol_tool_guides=None):
        return self.audit.call_llm(
            phase,
            system,
            messages,
            round_num,
            iteration=iteration,
            active_protocol_tool_guides=active_protocol_tool_guides,
        )

    def _load_time_limit(self):
        return self.cfg.get_round_time_limit()

    @staticmethod
    def _runtime_usage_token_count(value):
        if value is None or isinstance(value, bool):
            return None
        if isinstance(value, float) and not value.is_integer():
            return None
        try:
            count = int(value)
        except (TypeError, ValueError, OverflowError):
            return None
        return count if count >= 0 else None

    def _update_token_usage(self, result):
        result = result if isinstance(result, dict) else {}
        input_count = self._runtime_usage_token_count(
            result.get("tokens_input", 0)
        )
        output_count = self._runtime_usage_token_count(
            result.get("tokens_output", 0)
        )
        usage_valid = input_count is not None and output_count is not None
        input_tokens = input_count if input_count is not None else 0
        output_tokens = output_count if output_count is not None else 0
        if not usage_valid:
            input_tokens = 0
            output_tokens = 0
        current = input_tokens + output_tokens if usage_valid else 0
        endpoint = str(result.get("endpoint") or "primary").strip() or "primary"
        window_size = 0
        try:
            if self.cfg and hasattr(
                    self.cfg, "get_context_window_for_endpoint"):
                window_size = self.cfg.get_context_window_for_endpoint(endpoint) or 0
        except Exception:
            window_size = 0
        usage_ratio = (
            min(1.0, current / window_size)
            if current > 0 and window_size > 0
            else 0
        )
        try:
            self.sm.update_token_usage(
                current_tokens=current,
                window_size=window_size,
                usage_ratio=usage_ratio,
                input_tokens=input_tokens,
                output_tokens=output_tokens,
            )
        except Exception:
            pass
