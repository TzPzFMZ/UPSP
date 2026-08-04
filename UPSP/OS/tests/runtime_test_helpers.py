"""Shared helpers for Runtime test-suite slices."""

import json
import os
import sys
from collections import deque

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from data.config_store import ConfigStore
from schemas.config import (
    legacy_default_api_config as default_api_config,
    default_lately_config,
    default_memory_config,
    default_now_config,
    default_system_config,
)


FINAL_REPLY_TEXT_GUIDE = "__final_reply_text__"


class ConfigStoreStub(ConfigStore):
    """Complete ConfigStore contract with isolated API/system defaults."""

    @staticmethod
    def get_execution_permission_level():
        return "unlimited"

    def load(self, name):
        if name == "api":
            return default_api_config()
        if name == "system":
            return default_system_config()
        if name == "now":
            return default_now_config()
        if name == "lately":
            return default_lately_config()
        return super().load(name)


def logical_step(step, active_protocol_tool_guides=None):
    guides = {
        str(item or "").strip()
        for item in (active_protocol_tool_guides or [])
    }
    if str(step or "").strip() == "reaction" and FINAL_REPLY_TEXT_GUIDE in guides:
        return "final_reply"
    return step


class ScriptedExecutor:
    """Return a fixed response sequence while exposing Runtime call inputs."""

    def __init__(self, *responses):
        self.responses = deque(responses)
        self.calls = []
        self.steps = []
        self.guides = []

    def call(self, step, system, messages, active_protocol_tool_guides=None):
        self.calls.append(list(messages))
        self.steps.append(logical_step(step, active_protocol_tool_guides))
        self.guides.append(list(active_protocol_tool_guides or []))
        if not self.responses:
            raise AssertionError("unexpected provider call")
        response = self.responses.popleft()
        return response(self) if callable(response) else response


class RuntimeTestMixin:
    def _make_runtime(self, tmp_path):
        from engines.runtime import Runtime
        from data.state_store import StateStore
        from data.context_store import ContextStore
        from data.chronicle_store import ChronicleStore
        from data.workbench import WorkbenchStore
        from data.connectivity_store import ConnectivityStore
        from assembly.context import ContextAssembler
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        ctx_store = ContextStore(
            state_store=sm,
            cache_dir=str(tmp_path / "context_cache"),
            corpus_rhythms_dir=str(tmp_path / "corpus" / "public" / "rhythms"),
            raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
        )
        assembler = ContextAssembler(
            state_store=sm,
            context_dir=str(tmp_path / "context"),
            context_store=ctx_store,
        )
        workbench = WorkbenchStore(str(tmp_path / "workbench"))

        class InMemoryHeat:
            def __init__(self):
                from data.stm_heat_calculator import STMHeatCalculator
                self.entries = {}
                self.boosted = []
                self.decayed = False
                self.last_decay_round_num = None
                self.config = default_memory_config()["heat"]
                self.calculator = STMHeatCalculator(self.config)

            def new_entry(self, weight=2):
                from schemas.memory import default_heat_entry
                return default_heat_entry(
                    weight,
                    initial_by_weight=self.config["initial_by_weight"],
                    significant_threshold=self.config["zone_thresholds"]["significant"],
                    uncertain_threshold=self.config["zone_thresholds"]["uncertain"],
                )

            def set_entry(self, mem_id, entry):
                self.entries[mem_id] = dict(entry)

            def recall_boost(self, mem_id, round_num=None):
                self.boosted.append((mem_id, round_num))

            def tick_decay(self, round_num=None):
                self.decayed = True
                self.last_decay_round_num = round_num

            def has_pending_degrade(self):
                return False

        class InMemoryAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **entry):
                self.entries.append(entry)

        class NoopRelationStore:
            def load_registry(self):
                return {"cards": []}

            def resolve_active_subject(self, value):
                text = str(value or "").strip()
                return text if text in {"FMZ", "Codex", "TzPz"} else None

            def read_card(self, *args, **kwargs):
                return None

            def set_summary_resident(self, *args, **kwargs):
                pass

            def set_body_resident(self, *args, **kwargs):
                pass

        runtime = Runtime(
            state_store=sm,
            assembler=assembler,
            ctx_store=ctx_store,
            workbench_store=workbench,
            connectivity_store=ConnectivityStore(str(tmp_path / "connectivity.json")),
            heat=InMemoryHeat(),
            alert_store=InMemoryAlerts(),
            relation_store=NoopRelationStore(),
        )
        # Direct reaction-loop tests model the already-authorized full-tool path.
        runtime.permission_chain.apply("unlimited")
        runtime.reaction_loop_runner.chronicle_store = ChronicleStore(
            str(tmp_path / "Chronicle")
        )
        return runtime

    def _confirmed_meta(self):
        return {
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }

    def _native_tool_envelope(
            self,
            tool_id,
            arguments=None,
            *,
            call_id=None,
            tool_family="general_tool",
            tool_class="read_tool",
            risk="medium",
            parse_status="ok",
            index=0):
        call_id = call_id or f"call_{tool_id}_{index}"
        arguments = dict(arguments or {})
        return {
            "schema_version": "tool_call_envelope.v1",
            "source": "provider_tool_call",
            "provider": "openai_responses",
            "endpoint": "primary",
            "response_id": f"resp_{call_id}",
            "call_id": call_id,
            "provider_item_id": f"fc_{call_id}",
            "index": index,
            "raw_type": "function_call",
            "tool_id": tool_id,
            "arguments": arguments,
            "arguments_json": json.dumps(arguments, ensure_ascii=False),
            "tool_family": tool_family,
            "tool_class": tool_class,
            "risk": risk,
            "parse_status": parse_status,
        }

    def _native_reaction_finalize(
            self,
            *,
            call_id="call_reaction_finalize",
            **arguments):
        return self._native_tool_envelope(
            "reaction_finalize",
            arguments,
            call_id=call_id,
            tool_family="substrate_tool",
            tool_class="sync_tool",
            risk="high",
        )

    def _native_protocol_guide_request(
            self,
            tool_id,
            *,
            call_id="call_protocol_guide"):
        return self._native_tool_envelope(
            "protocol_tool_guide_request",
            {
                "tool_id": tool_id,
                "reason": "request protocol tool guide",
            },
            call_id=call_id,
            tool_family="runtime_tool",
            tool_class="guide_request",
            risk="medium",
        )

    def _patch_memory_immediate_stores(self, monkeypatch, runtime):
        import logic.memory_write as memory_write_mod

        class DummyMemoryStore:
            def __init__(self):
                self.entries = []
                self.meta = {}
                self.index_rows = []

            def write_entry(self, mem_id, title, summary, **kwargs):
                self.entries.append((mem_id, title, summary, kwargs))

            def set_meta(self, mem_id, meta):
                self.meta[mem_id] = dict(meta)

            def append_index(self, mem_id, entry_type, weight, title, **kwargs):
                self.index_rows.append((mem_id, entry_type, weight, title, kwargs))

            def update_linked_containers(
                    self, mem_id, operation, refs, current_overview=None):
                entry = dict(self.meta[mem_id])
                current = list(entry.get("linked_containers") or [])
                if operation == "set":
                    updated = list(refs)
                elif operation == "remove":
                    updated = [ref for ref in current if ref not in refs]
                else:
                    updated = list(current)
                    for ref in refs:
                        if ref not in updated:
                            updated.append(ref)
                entry["linked_containers"] = updated
                if current_overview is not None:
                    entry["current_overview"] = current_overview
                self.meta[mem_id] = entry
                return dict(entry)

        class DummyMemoryIndex:
            def __init__(self):
                self.keywords = []

            def add_stm_keywords(self, mem_id, keywords):
                self.keywords.append((mem_id, list(keywords)))

        class DummyContainerStore:
            def __init__(self):
                self.entries = []

            def append_entry(self, container_id, mem_id, title, file_name="open.md"):
                self.entries.append((container_id, mem_id, title, file_name))

        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        container_store = DummyContainerStore()

        monkeypatch.setattr(memory_write_mod, "generate_mem_id", lambda: "MEM-131000AA")
        runtime.memory_store = memory_store
        runtime.memory_index = memory_index
        runtime.container_store = container_store
        return memory_store, memory_index, container_store
