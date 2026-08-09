"""Seed schema、路径、常量与错误边界。"""

from __future__ import annotations

import os
from pathlib import Path
import sys

import pytest


sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class TestStateSchema:
    def test_defaults_match_runtime_contract(self):
        from schemas.state import FIELDS, default_state

        state = default_state()

        flags = state["base"]["heartbeat_flags"]
        assert len(flags) == 20
        assert {
            "token_usage_warning",
            "identity_timeout",
            "evolution_pending",
            "calendar_day_due",
            "calendar_week_due",
            "calendar_month_due",
            "calendar_quarter_due",
            "calendar_year_due",
        } <= flags.keys()
        assert isinstance(state["base"]["feeling_buffer"], list)
        assert "next_round" not in state["base"]["runtime"]
        assert all(writer.strip() for _, _, writer in FIELDS.values())
        assert FIELDS["base.token_usage.usage_ratio"][2].startswith(
            "engines/runtime_services.py"
        )

class TestMemorySchema:
    def test_defaults_and_field_order_match_contract(self):
        from schemas.memory import (
            INDEX_HEADER,
            META_ENTRY_FIELDS,
            default_heat_entry,
            default_keywords_json,
            default_meta_entry,
        )

        heat = default_heat_entry(weight=5)
        assert len(heat) == 10
        assert heat["heat_locked"] is False
        assert "pinned" not in heat

        expected = [
            "id",
            "type",
            "weight",
            "title",
            "dream",
            "created_at",
            "last_recalled_at",
            "created_round",
            "last_recalled_round",
            "source",
            "model",
            "subject",
            "access",
            "recalled",
            "current_overview",
            "current_overview_updated_at",
            "tags",
            "linked_containers",
            "decay_period_days",
            "decay_countdown_days",
            "media",
        ]
        meta = default_meta_entry("MEM-TEST01", "测试标题")
        assert list(META_ENTRY_FIELDS) == expected
        assert list(meta) == expected
        assert meta["recalled"] is False
        assert meta["dream"] is False
        assert meta["current_overview"] == ""
        assert meta["current_overview_updated_at"] == ""
        assert {
            "abstract",
            "locked",
            "source_rounds",
            "mode",
            "merged_from",
            "annotation",
        }.isdisjoint(meta)
        assert default_keywords_json()["index"] == {}
        assert "梦源" in INDEX_HEADER and "现状概况" in INDEX_HEADER
        assert "注释" not in INDEX_HEADER

    def test_memory_entry_template_uses_current_fields(self):
        from schemas.memory import MEMORY_ENTRY_TEMPLATE

        result = MEMORY_ENTRY_TEMPLATE.format(
            mem_id="MEM-TEST01",
            morph="A",
            weight=2,
            subject="TzPz",
            created_round_text="第1轮",
            last_recalled_round_text="第1轮",
            title="测试",
            dream_text="否",
            current_overview="已在 DC-12 中订正",
            content_line="",
            created_at="now",
            tags="标签1",
            feelings="无",
            delta_desc="无变化",
            linked_containers="DC-001",
        )
        assert "MEM-TEST01" in result
        assert "梦源：否" in result
        assert "现状概况：已在 DC-12 中订正" in result
        assert all(marker not in result for marker in ("Δ动态", "梗概", "注释："))


class TestContainerSchema:
    def test_defaults_and_registry_match_contract(self):
        from schemas.container import (
            CONTAINER_TYPES,
            default_container_meta,
            default_container_registry,
            validate_container_meta,
        )

        assert tuple(CONTAINER_TYPES) == (
            "DC",
            "EC",
            "PRJ",
            "SKL",
            "IMM",
            "CHR",
            "COR",
            "FUT",
            "ITR",
        )
        meta = default_container_meta("DC-001", "DC", "测试辩证链")
        ok, errors = validate_container_meta(meta)
        assert ok, errors
        assert meta["focus"] is False
        assert meta["status"] == "ongoing"
        assert "watched" not in meta

        registry = default_container_registry()
        assert len(registry["containers"]) == len(CONTAINER_TYPES)

    def test_status_machines_accept_only_current_values(self):
        from schemas.container import default_container_meta, validate_container_meta

        for kind, allowed, retired in [
            ("DC", ("ongoing", "suspended", "concluded"), "open"),
            (
                "EC",
                ("active", "interrupted", "restarted", "ended", "cancelled"),
                "closed",
            ),
        ]:
            meta = default_container_meta(f"{kind}-001", kind, "测试")
            for status in allowed:
                meta["status"] = status
                ok, errors = validate_container_meta(meta)
                assert ok, (kind, status, errors)
            meta["status"] = retired
            ok, _ = validate_container_meta(meta)
            assert not ok, (kind, retired)

    def test_invalid_container_shapes_fail_closed(self):
        from schemas.container import (
            default_container_registry,
            validate_container_meta,
        )

        watched = {
            "id": "DC-001",
            "type": "DC",
            "title": "测试辩证链",
            "status": "open",
            "created_at": "2026-05-19T00:00:00+08:00",
            "updated_at": "2026-05-19T00:00:00+08:00",
            "entries": 0,
            "tags": [],
            "watched": True,
        }
        unknown = {
            "id": "XX-001",
            "type": "XX",
            "title": "t",
            "status": "open",
            "created_at": "",
            "updated_at": "",
        }
        illegal_status = {**unknown, "id": "DC-001", "type": "DC", "status": "deleted"}
        for label, value in [
            ("retired watched", watched),
            ("unknown type", unknown),
            ("illegal status", illegal_status),
        ]:
            ok, errors = validate_container_meta(value)
            assert not ok, (label, errors)

class TestRelationSchema:
    def test_defaults_and_axes_match_contract(self):
        from schemas.relation import (
            RELATION_AXES,
            default_relation_card,
            default_registry_card_entry,
        )

        assert tuple(RELATION_AXES) == (
            "trust",
            "safety",
            "value",
            "investment",
            "honesty",
            "resonance",
        )
        entry = default_registry_card_entry(
            "REL-Codex", "Codex", "them", "relation/them/REL-Codex.md"
        )
        assert entry["summary_resident"] is False
        assert entry["body_resident"] is False
        assert "watched" not in entry

        card = default_relation_card("REL-TzPz", "TzPz", "human")
        assert set(card["axes"]) == set(RELATION_AXES)


class TestConfigSchema:
    def test_defaults_match_contract(self):
        from schemas.config import (
            default_interface_config,
            default_memory_config,
            default_model_routing_config,
            default_models_config,
            default_system_config,
        )

        system = default_system_config()
        memory = default_memory_config()
        assert system["autonomous_trigger"]["tacit_pending_threshold"] == 512
        assert system["autonomous_trigger"]["connection_pending_threshold"] == 512
        assert system["audit"]["round_snapshot_retention"] == 64
        assert system["audit"]["state_backup_retention"] == 8
        assert default_interface_config()["locale"] == "system"
        assert default_models_config()["connections"] == []
        assert default_models_config()["models"] == []
        assert default_models_config()["transport"]["handshake"]["retry"] == 2
        assert default_model_routing_config()["routes"]["setup"]["primary"] is None
        assert default_model_routing_config()["cross_phase_failover_enabled"] is True
        assert memory["privacy_declassify"]["manual_enabled"] is False
        assert memory["privacy_declassify"]["auto_enabled"] is False

class TestContextSchema:
    def test_context_layer_tables_match_audit_contract(self):
        from schemas.context import (
            CONTEXT_MODULES,
            STEP_AUDIT_FILES,
        )

        assert STEP_AUDIT_FILES == [
            "step.md",
            "step.json",
            "manifest.json",
            "layers/00_call_header.json",
            "layers/01_tool_header.json",
            "layers/02_generation_config.json",
            "layers/10_permanent.json",
            "layers/10_permanent.md",
            "layers/20_periodic.json",
            "layers/20_periodic.md",
            "layers/30_lately.json",
            "layers/30_lately.md",
            "layers/40_high_freq.json",
            "layers/40_high_freq.md",
            "layers/50_now.json",
            "layers/50_now.md",
            "layers/60_statusbar.json",
            "layers/60_statusbar.md",
            "layers/99_popup.json",
            "layers/99_popup.md",
        ]
        assert len(CONTEXT_MODULES) == 5


class TestPaths:
    def test_registry_paths_are_well_formed_and_persona_scoped(self):
        from paths import (
            OS_ROOT,
            PERSONA_DIR,
            PERSONA_PRESETS_DIR,
            PERSONA_TEMPLATE_DIR,
            list_all_paths,
        )

        all_paths = list_all_paths()
        assert len(all_paths) >= 80
        assert os.path.isdir(OS_ROOT)
        persona_root = Path(PERSONA_DIR).resolve()
        global_persona_roots = {
            "PERSONA_PRESETS_DIR": Path(PERSONA_PRESETS_DIR).resolve(),
            "PERSONA_TEMPLATE_DIR": Path(PERSONA_TEMPLATE_DIR).resolve(),
        }
        for name, path in all_paths.items():
            assert isinstance(path, str), name
            if name in global_persona_roots:
                assert Path(path).resolve() == global_persona_roots[name]
                assert not Path(path).resolve().is_relative_to(persona_root)
                continue
            if any(marker in name.lower() for marker in ("persona", "stm", "ltm")):
                assert Path(path).resolve().is_relative_to(persona_root), (name, path)

    def test_runtime_cache_paths_are_separate_from_config(self):
        from paths import (
            CONTEXT_LATELY_JSON,
            CONTEXT_NOW_JSON,
            CONTEXT_STATUSBAR_JSON,
            STATE_BACKUPS_JSONL,
            STM_CONTEXT_LATELY_CACHE_JSONL,
            STM_CONTEXT_NOW_CACHE_JSONL,
        )

        expected = {
            STM_CONTEXT_NOW_CACHE_JSONL: "STM/context/cache/now_cache.jsonl",
            STM_CONTEXT_LATELY_CACHE_JSONL: "STM/context/cache/lately_cache.jsonl",
            CONTEXT_NOW_JSON: "config/context/now.json",
            CONTEXT_LATELY_JSON: "config/context/lately.json",
            CONTEXT_STATUSBAR_JSON: "config/context/statusbar.json",
            STATE_BACKUPS_JSONL: "STM/buffer/state_backups.jsonl",
        }
        for path, suffix in expected.items():
            assert suffix in path.replace("\\", "/")


class TestConstants:
    def test_current_constant_tables_and_ranges(self):
        import constants
        from schemas.config import default_memory_config

        heat = default_memory_config()["heat"]
        assert heat["decay_rates"]["significant"] < 0
        assert heat["zone_thresholds"]["significant"] > heat["zone_thresholds"]["uncertain"]
        assert constants.HEARTBEAT_DEFAULT_INTERVAL > 0
        assert len(constants.ROUND_TYPES) == 5 and "interactive" in constants.ROUND_TYPES
        assert len(constants.PHASES) == 4 and "idle" in constants.PHASES
        assert 0 < constants.TOKEN_WARNING_RATIO <= 1
        assert constants.DYNAMIC_AXIS_RANGE[0] < constants.DYNAMIC_AXIS_RANGE[1]
        assert len(constants.CONTAINER_PREFIXES) == 9
        assert "WB" not in constants.CONTAINER_PREFIXES

    def test_pinned_has_no_dedicated_character_caps(self):
        import constants

        registry = getattr(constants, "CONSTANTS_REGISTRY", constants.ALL_CONSTANTS)
        for name in ("PINNED_CHAR_LIMIT", "PINNED_ENTRY_CHAR_LIMIT"):
            assert not hasattr(constants, name)
            assert name not in registry


class TestErrors:
    def test_error_classes_share_base_and_declare_domain(self):
        import errors as error_module
        from errors import UPSPError

        classes = [
            value
            for value in vars(error_module).values()
            if isinstance(value, type)
            and issubclass(value, Exception)
            and value not in {Exception, UPSPError}
            and value.__module__ == error_module.__name__
        ]
        assert classes
        for error_class in classes:
            assert issubclass(error_class, UPSPError), error_class.__name__
            assert isinstance(error_class.domain, str) and error_class.domain

    def test_error_payload_attributes_and_messages(self):
        from errors import APIBridgeError, WriteError

        assert WriteError("/tmp/test").path == "/tmp/test"
        api = APIBridgeError("primary", status_code=521)
        assert (api.endpoint, api.status_code) == ("primary", 521)

    def test_specific_errors_are_caught_by_upsp_base(self):
        from errors import MemoryError, UPSPError

        with pytest.raises(UPSPError):
            raise MemoryError("test")
