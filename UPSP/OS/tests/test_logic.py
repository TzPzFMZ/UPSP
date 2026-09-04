"""
Phase 2 业务逻辑层测试 — 纯函数，全部测试

测试原则：
  - 每个函数：正常值、边界值、异常值
  - 全部纯函数，不需要 tmp_path（逻辑层不碰磁盘）
"""
import sys
import os
import copy
from pathlib import Path
import pytest
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from logic.native_tool_calls import project_step_finalize


def _cleanup_projection(**arguments):
    return project_step_finalize("cleanup", arguments)


def _dummy_heat_entry(weight=2):
    from schemas.memory import default_heat_entry
    from schemas.config import default_memory_config
    config = default_memory_config()["heat"]
    entry = default_heat_entry(
        weight,
        initial_by_weight=config["initial_by_weight"],
        significant_threshold=config["zone_thresholds"]["significant"],
        uncertain_threshold=config["zone_thresholds"]["uncertain"],
    )
    entry["last_heat_at"] = "2026-08-14T00:00:00+08:00"
    return entry


class _TransactionalMemoryStoreFake:
    """Small in-memory stand-in for the Spec746 LTM-first write transaction."""

    def _ensure_transaction_state(self):
        if not hasattr(self, "_ltm"):
            self._ltm = {}
            self._stm_bodies = {}
            self._stm_meta = {}
            self._heat_reader = None

    def render_entry(self, mem_id, title, **kwargs):
        self._ensure_transaction_state()
        if hasattr(self, "written"):
            self.written.append(((mem_id,), dict(kwargs)))
        self._render_kwargs = dict(kwargs)
        semantic = kwargs.get("summary", "")
        return f"# {mem_id} {title}\n\n{semantic}\n"

    def snapshot_stm_files(self):
        self._ensure_transaction_state()
        return copy.deepcopy((self._stm_bodies, self._stm_meta))

    def snapshot_ltm_files(self):
        self._ensure_transaction_state()
        return copy.deepcopy(self._ltm)

    def restore_stm_files(self, snapshot):
        self._stm_bodies, self._stm_meta = copy.deepcopy(snapshot)

    def restore_ltm_files(self, snapshot):
        self._ltm = copy.deepcopy(snapshot)

    def store_ltm_entry(self, tier, mem_id, body, meta):
        self._ensure_transaction_state()
        self._ltm[mem_id] = {"tier": tier, "body": body, "meta": copy.deepcopy(meta)}

    def replace_stm_body(self, mem_id, body):
        self._ensure_transaction_state()
        self._stm_bodies[mem_id] = body

    def replace_stm_meta(self, mem_id, meta):
        self._ensure_transaction_state()
        self._stm_meta[mem_id] = copy.deepcopy(meta)
        if hasattr(self, "meta"):
            self.meta.append(((mem_id, copy.deepcopy(meta)), {}))

    def rebuild_stm_index(self):
        if hasattr(self, "index"):
            self.index.append(((), dict(getattr(self, "_render_kwargs", {}))))

    def rebuild_stm_keywords(self):
        return None

    def ltm_entry_state(self, mem_id, *, include_backup=True):
        self._ensure_transaction_state()
        return copy.deepcopy(self._ltm.get(mem_id))

    def stm_entry_state(self, mem_id):
        self._ensure_transaction_state()
        meta = self._stm_meta.get(mem_id)
        heat = _dummy_heat_entry(meta["weight"]) if meta else None
        return {
            "body": self._stm_bodies.get(mem_id),
            "meta": copy.deepcopy(meta),
            "heat": heat,
        }


# ============================================================
# mem_id 测试
# ============================================================

class TestMemID:
    def test_generate_mem_id_format(self):
        from logic.mem_id import generate_mem_id
        mid = generate_mem_id()
        assert mid.startswith("MEM-")
        assert len(mid) == 12
        # 后8位全大写十六进制
        assert all(c in "0123456789ABCDEF" for c in mid[4:])

    def test_validate_mem_id(self):
        from logic.mem_id import validate_mem_id
        assert validate_mem_id("MEM-0E6F3A7B") is True
        assert validate_mem_id("MEM-0014D001") is True
        assert validate_mem_id("MEM-00333-01") is False  # 旧格式
        assert validate_mem_id("INVALID") is False

    def test_make_meta_template_24_fields(self):
        from logic.mem_id import make_meta_template
        meta = make_meta_template("MEM-TEST01", "测试标题", weight=5)
        assert len(meta) == 24
        assert meta["stored_at"] == ""
        assert meta["id"] == "MEM-TEST01"
        assert meta["type"] == "F"
        assert meta["weight"] == 5
        assert meta["dream"] is False
        assert meta["current_overview"] == ""
        assert "model" in meta
        assert "recalled" in meta
        assert {"abstract", "locked", "source_rounds", "mode", "merged_from"}.isdisjoint(meta)

    def test_make_heat_entry_uses_memory_config(self):
        entry = _dummy_heat_entry(weight=5)
        assert entry["H"] == 80
        assert entry["compression"] is True
        entry2 = _dummy_heat_entry(weight=1)
        assert entry2["H"] == 40
        assert entry2["compression"] is False


# ============================================================
# feeling_lookup 测试
# ============================================================


class TestFeelingLookup:
    def test_lookup_interaction_word(self):
        from logic.feeling_lookup import FeelingWordTable
        ft = FeelingWordTable()
        results = ft.lookup_interaction(["好奇", "宁静"])
        assert len(results) == 2
        assert results[0]["word"] == "好奇"
        assert results[0]["layer"] == 1

    def test_lookup_interaction_bandwidth(self):
        from logic.feeling_lookup import FeelingWordTable
        ft = FeelingWordTable()
        # 超过3个只返回前3个
        results = ft.lookup_interaction(["好奇", "困惑", "兴奋", "宁静"])
        assert len(results) == 3

    def test_lookup_nonexistent_word(self):
        from logic.feeling_lookup import FeelingWordTable
        ft = FeelingWordTable()
        results = ft.lookup_interaction(["不存在的词"])
        assert results == []

    def test_lookup_relation_word(self):
        from logic.feeling_lookup import FeelingWordTable
        ft = FeelingWordTable()
        result = ft.lookup_relation("信任")
        assert result is not None
        assert result["word"] == "信任"

    def test_merge_deltas(self):
        from logic.feeling_lookup import FeelingWordTable
        ft = FeelingWordTable()
        results = ft.lookup_interaction(["好奇", "宁静"])
        merged = ft.merge_deltas(results)
        assert merged["valence"] == 2
        assert merged["focus"] == 1
        assert merged["safety"] == 2

    def test_structured_tables_are_exact_and_drive_model_guide(self):
        import hashlib
        from logic.feeling_lookup import (
            INTERACTION_PROTOCOL_TABLE,
            RELATION_PROTOCOL_TABLE,
            build_feeling_guide,
        )

        assert len(INTERACTION_PROTOCOL_TABLE) == 64
        assert len(RELATION_PROTOCOL_TABLE) == 64
        guide = build_feeling_guide()
        assert set(INTERACTION_PROTOCOL_TABLE) == {
            word
            for word in INTERACTION_PROTOCOL_TABLE
            if word in guide
        }
        assert set(RELATION_PROTOCOL_TABLE) == {
            word
            for word in RELATION_PROTOCOL_TABLE
            if word in guide
        }
        assert hashlib.sha256(guide.encode("utf-8")).hexdigest() == (
            "7c2cf7848005740805c63587de47b9c6f86d48fe9e00132c313cf3167482e2e4"
        )

    def test_lookup_shock_level(self):
        from logic.feeling_lookup import FeelingWordTable
        ft = FeelingWordTable()
        results = ft.lookup_interaction(["自我实现"])
        assert results[0]["layer"] == 4
        assert abs(results[0]["deltas"].get("arousal", 0)) >= 5


# ============================================================
# feeling_buffer 测试
# ============================================================

class TestFeelingBuffer:
    @staticmethod
    def _receipt(mem_id="MEM-01"):
        return {
            "status": "applied",
            "mem_id": mem_id,
            "interaction_feelings": ["核心判断被推翻"],
            "relationship_feelings": [
                {"subject": "REL-A", "word": "信任"},
                {"subject": "REL-B", "word": "可靠"},
            ],
        }

    def test_three_intensity_boundaries(self):
        from logic.feeling_buffer import pulse_profile
        assert [pulse_profile(value) for value in (2, 3, 4, 5, -5)] == [
            ("ordinary", 0),
            ("attention", 1),
            ("attention", 1),
            ("shock", 2),
            ("shock", 2),
        ]

    def test_receipt_effects_are_immediate_and_subject_scoped(self):
        from datetime import datetime
        from constants import TZ_SHANGHAI
        from logic.feeling_buffer import collect_receipt_effects
        result = collect_receipt_effects(
            [self._receipt()], datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI))
        assert result["dynamic"]["arousal"] == 3
        assert result["relations"]["REL-A"]["trust"] == 3
        assert result["relations"]["REL-B"]["trust"] == 2
        assert {item["domain"] for item in result["pending"]} == {
            "dynamic", "relation",
        }

    def test_duplicate_receipt_is_consumed_once(self):
        from logic.feeling_buffer import collect_receipt_effects
        receipt = self._receipt()
        result = collect_receipt_effects([receipt, dict(receipt)])
        assert result["source_memory_ids"] == ["MEM-01"]
        assert result["dynamic"]["arousal"] == 3

    def test_noninteractive_round_does_not_advance_counter(self):
        from datetime import datetime, timedelta
        from constants import TZ_SHANGHAI
        from logic.feeling_buffer import collect_receipt_effects, settle_pending
        start = datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI)
        entry = collect_receipt_effects([self._receipt()], start)["pending"][0]
        result = settle_pending([entry], False, start + timedelta(minutes=1))
        assert result["dynamic"] == {}
        assert result["remaining"][0]["interactive_rounds_elapsed"] == 0

    def test_two_interactive_rounds_trigger_pulse(self):
        from datetime import datetime, timedelta
        from constants import TZ_SHANGHAI
        from logic.feeling_buffer import collect_receipt_effects, settle_pending
        start = datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI)
        entry = collect_receipt_effects([self._receipt()], start)["pending"][0]
        first = settle_pending([entry], True, start + timedelta(minutes=1))
        second = settle_pending(
            first["remaining"], True, start + timedelta(minutes=2))
        assert second["dynamic"][entry["axis"]] == entry["delta"]

    def test_five_minutes_trigger_without_interaction(self):
        from datetime import datetime, timedelta
        from constants import TZ_SHANGHAI
        from logic.feeling_buffer import collect_receipt_effects, settle_pending
        start = datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI)
        entry = collect_receipt_effects([self._receipt()], start)["pending"][0]
        result = settle_pending([entry], False, start + timedelta(minutes=5))
        assert result["dynamic"][entry["axis"]] == entry["delta"]


# ============================================================
# gravity 测试
# ============================================================

class TestGravity:
    def test_core_component_boundaries(self):
        from logic.gravity import core_component
        assert [core_component(value) for value in (40, 39, 20, 19)] == [
            0, -1, -1, -2,
        ]
        assert [core_component(value) for value in (60, 61, 80, 81)] == [
            0, 1, 1, 2,
        ]

    def test_fmz_core_gravity_and_comfort(self):
        from logic.gravity import core_gravity
        pulls, comfort = core_gravity({
            "S": 85, "C": 70, "V": 60,
            "A": 75, "R": 55, "B": 80,
        })
        assert pulls == {
            "focus": 1.5, "safety": 0.5, "valence": 0,
            "arousal": -1, "mood": 0, "humor": 0,
        }
        assert comfort == {
            "focus": 30, "safety": 10, "valence": 0,
            "arousal": -20, "mood": 0, "humor": 0,
        }

    def test_relation_boundaries_and_multi_subject_clamp(self):
        from logic.gravity import relation_component, relation_gravity
        assert [relation_component(value) for value in (-31, -30, 29, 30)] == [
            -1, 0, 0, 1,
        ]
        pulls = relation_gravity({
            "REL-A": {"trust": 80, "safety": 80},
            "REL-B": {"trust": 80, "safety": 80},
        })
        assert pulls["focus"] == 2
        assert pulls["safety"] == 2

    def test_changed_axis_gets_gravity_other_axis_decays(self):
        from logic.gravity import apply_dynamic
        current = {axis: {"value": 0} for axis in (
            "valence", "arousal", "focus", "mood", "humor", "safety")}
        result = apply_dynamic(
            current,
            {"valence": 0, "arousal": -20, "focus": 30,
             "mood": 0, "humor": 0, "safety": 10},
            {"valence": 2},
            {"valence": 1},
            {"valence": -1},
        )
        assert result["valence"]["value"] == 2
        assert result["focus"]["value"] == 1
        assert result["arousal"]["value"] == -1


# ============================================================
# workhood 测试
# ============================================================

class TestWorkhood:
    def test_m_curve_key_points(self):
        from logic.workhood import m_curve
        assert [m_curve(value) for value in (0, 25, 50, 75, 100)] == [
            50, 100, 20, 100, 50,
        ]

    def test_fmz_migration_values(self):
        from logic.workhood import compute_workhood
        dynamic = {axis: {"value": 0} for axis in (
            "valence", "arousal", "focus", "mood", "humor", "safety")}
        result = compute_workhood({
            "S": 85, "C": 70, "V": 60,
            "A": 75, "R": 55, "B": 80,
        }, dynamic)
        assert result == {
            "value": 57.2,
            "self_reference": 66.1,
            "self_reflection": 40.0,
            "autonomy": 70.6,
        }

    def test_speed_wheel_boundaries(self):
        from logic.workhood import speed_wheel_max
        assert [speed_wheel_max(value) for value in (
            0, 19.9, 20, 39.9, 40, 59.9, 60, 79.9, 80, 100,
        )] == [64, 64, 128, 128, 256, 256, 384, 384, 512, 512]


# ============================================================
# decay 测试
# ============================================================

class TestDecay:
    def test_tick_decay_reduces_heat(self):
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        heat = {"MEM-01": {"H": 50, "zone": "未定", "heat_locked": False, "degrade": False}}
        updates = dc.tick_decay(heat)
        assert "MEM-01" in updates
        assert updates["MEM-01"]["H"] < 50

    def test_heat_locked_entry_fixed_at_80(self):
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        heat = {"MEM-01": {
            "H": 10, "zone": "衰减", "AH_high": 0, "AH_low": 2,
            "degrade": False, "stored": False, "compression": False,
            "heat_locked": True,
        }}
        updates = dc.tick_decay(heat)
        assert updates["MEM-01"]["H"] == 80
        assert updates["MEM-01"]["zone"] == "显著"
        assert updates["MEM-01"]["AH_low"] == 0
        assert "degrade" not in updates["MEM-01"]

    def test_degrade_at_ah_low_3(self):
        """AH_low≥3 触发遗忘（衰减区累计3轮），不是H=0触发"""
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        heat = {"MEM-01": {"H": 1, "zone": "衰减", "heat_locked": False,
                           "degrade": False, "AH_low": 2, "stored": False}}
        updates = dc.tick_decay(heat)
        assert updates["MEM-01"]["H"] == 0
        assert updates["MEM-01"]["AH_low"] == 3
        assert updates["MEM-01"]["degrade"] is True

    def test_stored_entry_degrades_for_stm_duplicate_delete(self):
        """已入库条目 AH_low≥3 也要置 degrade，后续遗忘分流才能删 STM 副本"""
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        heat = {"MEM-01": {"H": 1, "zone": "衰减", "heat_locked": False,
                           "degrade": False, "AH_low": 2, "stored": True}}
        updates = dc.tick_decay(heat)
        assert updates["MEM-01"]["AH_low"] == 3
        assert updates["MEM-01"]["degrade"] is True

    def test_ah_high_increments_in_significant(self):
        """zone=显著时 AH_high+1（H≥75才能衰减后仍≥70）"""
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        heat = {"MEM-01": {"H": 75, "zone": "显著", "heat_locked": False,
                           "AH_high": 1}}
        updates = dc.tick_decay(heat)
        assert updates["MEM-01"]["AH_high"] == 2


class TestRelationContextBoundaries:
    def test_relation_context_accepts_explicit_max_slots(self):
        from logic.relation_context import RelationContextManager

        cards = [
            {"id": "REL-A", "name": "Alice", "category": "ours", "summary_resident": True},
            {"id": "REL-B", "name": "Bob", "category": "ours"},
        ]
        manager = RelationContextManager(max_slots=1)

        result = manager.determine_context_roles("Alice 和 Bob 都在", registry_cards=cards)

        assert manager.get_max_slots() == 1
        assert len(result["active"]) == 1
        assert result["active"][0]["id"] == "REL-A"

    def test_relation_context_without_registry_cards_does_not_autoload_store(self):
        from logic.relation_context import RelationContextManager

        manager = RelationContextManager(max_slots=3)

        assert manager.extract_interaction_objects("Alice", registry_cards=None) == []
        result = manager.determine_context_roles("Alice", registry_cards=None)
        assert result["active"] == []

class TestSkillSettlements:
    def test_process_cleanup_rejects_connection_without_current_memory_receipt(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection(connection_bridges=[{
            "word_a": "记忆",
            "entry_a": "MEM-A",
            "word_b": "主体",
            "entry_b": "MEM-B",
        }])
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": DummyContext(),
        }

        report = cp.process_cleanup(parsed, {}, 45, {"_memory_write_receipts": []}, data_modules)

        assert report["_connection_bridges"] == []
        assert any("联系集光锥校验" in warning for warning in report["warnings"])

    def test_process_cleanup_accepts_connection_anchored_by_content_read_receipt(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def append_to_cache(self, *args, **kwargs):
                pass

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection(connection_bridges=[{
            "word_a": "读取",
            "entry_a": "MEM-READ",
            "word_b": "历史",
            "entry_b": "MEM-HIST",
        }])
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": DummyContext(),
        }

        report = cp.process_cleanup(
            parsed,
            {},
            64,
            {"_memory_content_read_receipts": [{
                "tool_id": "memory_content_read",
                "status": "accepted",
                "mem_id": "MEM-READ",
            }]},
            data_modules,
        )

        assert report["_connection_bridges"] == [{
            "word_a": "读取",
            "entry_a": "MEM-READ",
            "word_b": "历史",
            "entry_b": "MEM-HIST",
        }]

    def test_process_cleanup_validates_tacit_after_connection_graph_and_prework(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def append_to_cache(self, *args, **kwargs):
                pass

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection(
            connection_bridges=[{
                "word_a": "预选",
                "entry_a": "MEM-SETUP",
                "word_b": "新增",
                "entry_b": "MEM-NEW",
            }],
            tacit_associations=[
                {
                    "item_id": "MEM-SETUP", "item_type": "memory",
                    "action": "kept", "note": "联系图命中",
                    "evidence_refs": ["connection:MEM-SETUP"], "drop_reason": "",
                },
                {
                    "item_id": "MEM-MISS", "item_type": "memory",
                    "action": "kept", "note": "LLM误判沿用",
                    "evidence_refs": [], "drop_reason": "",
                },
                {
                    "item_id": "MEM-DROP", "item_type": "memory",
                    "action": "dropped", "note": "未命中",
                    "evidence_refs": [], "drop_reason": "no_valid_connection_hit",
                },
                {
                    "item_id": "MEM-NEW", "item_type": "memory",
                    "action": "added", "note": "前置读取新增",
                    "evidence_refs": ["memory_content_read:MEM-NEW"], "drop_reason": "",
                },
                {
                    "item_id": "MEM-LATE", "item_type": "memory",
                    "action": "added", "note": "只在最终回复出现",
                    "evidence_refs": ["final_reply:MEM-LATE"], "drop_reason": "",
                },
            ],
        )
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": DummyContext(),
        }

        report = cp.process_cleanup(
            parsed,
            {},
            64,
            {
                "_mounted_memories": ["MEM-SETUP", "MEM-MISS", "MEM-DROP"],
                "_memory_write_receipts": [{
                    "status": "applied",
                    "mem_id": "MEM-NEW",
                    "keywords": ["新增"],
                }],
                "_memory_content_read_receipts": [{
                    "tool_id": "memory_content_read",
                    "status": "accepted",
                    "mem_id": "MEM-NEW",
                }],
            },
            data_modules,
        )

        assert report["_connection_bridges"] == [{
            "word_a": "预选",
            "entry_a": "MEM-SETUP",
            "word_b": "新增",
            "entry_b": "MEM-NEW",
        }]
        assert report["_tacit_associations"] == [
            {
                "item_id": "MEM-SETUP",
                "item_type": "memory",
                "action": "kept",
                "note": "联系图命中",
                "evidence_refs": ["connection:MEM-SETUP"],
                "drop_reason": "",
            },
            {
                "item_id": "MEM-DROP",
                "item_type": "memory",
                "action": "dropped",
                "note": "未命中",
                "evidence_refs": [],
                "drop_reason": "no_valid_connection_hit",
            },
            {
                "item_id": "MEM-NEW",
                "item_type": "memory",
                "action": "added",
                "note": "前置读取新增",
                "evidence_refs": ["memory_content_read:MEM-NEW"],
                "drop_reason": "",
            },
        ]
        assert any("默契集kept无承接证据" in warning for warning in report["warnings"])
        assert any("默契集added缺少前置痕迹" in warning for warning in report["warnings"])

    def test_reaction_builds_association_counts_from_applied_memory_receipts(self):
        from engines.reaction_metabolism import build_association_counts_from_receipts

        counts = build_association_counts_from_receipts([{
                "status": "applied",
                "mem_id": "MEM-NEW",
                "subject": "FMZ",
                "keywords": ["默契", "联系"],
                "interaction_feelings": ["专注"],
                "relationship_feelings": [
                    {"subject": "TzPz", "word": "可靠"},
                    {"subject": "Codex", "word": "坦率"},
                ],
            }])

        assert counts == {
            "assoc_kw_kw": [("默契", "联系")],
            "assoc_kw_ifeel": [("默契", "专注"), ("联系", "专注")],
            "assoc_kw_rfeel": [
                ("默契", "可靠"), ("默契", "坦率"),
                ("联系", "可靠"), ("联系", "坦率"),
            ],
            "assoc_ifeel_rfeel": [("专注", "可靠"), ("专注", "坦率")],
            "assoc_object_rfeel": [("TzPz", "可靠"), ("Codex", "坦率")],
        }

    def test_association_counts_keep_historical_relation_feeling_receipts_readable(self):
        from engines.reaction_metabolism import build_association_counts_from_receipts

        counts = build_association_counts_from_receipts([{
            "status": "applied",
            "subject": "TzPz",
            "keywords": ["历史"],
            "relation_feelings": ["可靠"],
        }])

        assert counts["assoc_object_rfeel"] == [("TzPz", "可靠")]

class TestSTMHeatCalculator:

    def test_recall_boost(self):
        """recall_boost 只改 H 和 zone，不改 AH_high（由 tick_decay 统一结算）"""
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        new_h, new_ah, zone = dc.recall_boost(50, 2)
        assert new_h > 50
        assert new_ah == 2  # AH_high 不变
        assert zone == "未定"

    def test_recall_boost_enters_significant(self):
        """即使进入显著区，AH_high 仍由 tick_decay 结算"""
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        new_h, new_ah, zone = dc.recall_boost(65, 2)
        assert new_h == 75
        assert new_ah == 2  # AH_high 不变
        assert zone == "显著"

    def test_check_upgrade(self):
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        heat = {"MEM-01": {"AH_high": 6}}
        meta = {}
        candidates = dc.check_upgrade(heat, meta)
        assert "MEM-01" in candidates

    def test_process_forgetting(self):
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        heat = {
            "MEM-01": {"degrade": True, "compression": False},
            "MEM-02": {"degrade": True, "compression": True},
            "MEM-03": {"degrade": True, "compression": True},
        }
        meta = {
            "MEM-01": {"stored_at": ""},
            "MEM-02": {"stored_at": "2026-08-14T00:00:00+08:00"},
            "MEM-03": {"stored_at": ""},
        }
        to_delete, to_abstract, need_compress = dc.process_forgetting(heat, meta)
        assert "MEM-02" in to_delete       # stored → 删STM副本
        assert "MEM-01" in to_abstract     # 无compression → 直接搬Abstract
        assert "MEM-03" in need_compress   # compression → 需LLM压缩

    def test_weight_to_type(self):
        from data.stm_heat_calculator import STMHeatCalculator
        dc = STMHeatCalculator()
        assert dc.weight_to_type(5) == "F"
        assert dc.weight_to_type(3) == "S"
        assert dc.weight_to_type(1) == "A"


# ============================================================
# 关系引力与动态结算测试
# ============================================================

# ============================================================
# Spec576 fixed memory reminder tests
# ============================================================

class TestStaticMemoryReminder:
    def test_spec576_static_memory_reminder_renders_reminder_fragment(self):
        from assembly.context_helpers import build_static_memory_reminder_popup

        card = build_static_memory_reminder_popup("reaction")

        assert "kind: memory_settlement_reminder" in card
        assert "tier: reminder" in card
        assert "主体更新" in card
        assert "material/最近缓存承载" in card
        assert "不是私有笔记或记忆替代" in card
        assert "稳定变化和可复用判断" in card
        assert "用户/任务禁止长期记忆，则不写" in card
        assert "workaround" not in card
        assert "memory_write" in card
        assert "只有 `MEM-*` 回执才算写入成功" in card
        assert "memory_container_create / memory_container_write" not in card
        assert "reaction_branch_card" not in card
        assert "记忆沉淀近位提醒" not in card

    def test_spec576_static_memory_reminder_only_reaction_step(self):
        from assembly.context_helpers import build_static_memory_reminder_popup

        assert build_static_memory_reminder_popup("setup") == ""
        assert build_static_memory_reminder_popup("cleanup") == ""

    def test_spec582_schema_describes_subject_update_memory_reminder(self):
        upsp_root = Path(__file__).resolve().parents[2]
        schema = (upsp_root / "initialization" / "persona_template/docs/protocol/base/schema.md").read_text(encoding="utf-8")

        assert "`memory_settlement_reminder`" in schema
        assert "主体更新" in schema
        assert "主动考虑 `memory_write`" in schema
        assert "不根据本轮证据动态生成行为树" in schema
        old_evidence_boundary_only = "只提示" + "记忆写入和容器承接的证据边界"
        assert old_evidence_boundary_only not in schema


# ============================================================
# provider-native reaction 合同测试
# ============================================================

class TestReactionParser:
    def test_protocol_tool_definitions_use_three_public_tool_classes(self):
        from logic.protocol_tools import TOOL_DEFINITIONS

        classes = {meta.get("tool_class") for meta in TOOL_DEFINITIONS.values()}
        assert classes == {"read_tool", "sync_tool", "action_tool"}
        assert all("focus_policy" not in meta for meta in TOOL_DEFINITIONS.values())
        assert TOOL_DEFINITIONS["state_settle"]["handler"] == "state_settlement"

    def test_protocol_tool_class_helpers_match_registered_tools(self):
        from logic.protocol_tools import (
            is_action_tool,
            is_read_tool,
            is_sync_tool,
            tool_class_for,
        )

        assert tool_class_for("container_focus") == ""
        assert is_action_tool("file_write")
        assert tool_class_for("memory_write") == "sync_tool"
        assert is_sync_tool("memory_write")
        assert is_sync_tool("relation_card_write")
        assert is_read_tool("relation_read")
        assert is_read_tool("index_view")

    def test_execution_routes_and_metadata_helpers_match_registered_tools(self):
        from logic.protocol_tools import (
            GENERAL_TOOL_IDS,
            PROTOCOL_TOOL_IDS,
            TOOL_DEFINITIONS,
            general_tool_backend_for,
            tool_metadata_for,
        )

        assert {meta.get("execution_route") for meta in TOOL_DEFINITIONS.values()} == {
            "host_dispatch", "internal_processor", "substrate",
        }
        memory_meta = tool_metadata_for("memory_write_declaration")
        assert memory_meta["execution_route"] == "internal_processor"
        assert memory_meta["tool_class"] == "sync_tool"
        assert memory_meta["domain"] == "memory"
        assert memory_meta["risk"] == "high"
        assert memory_meta["handler"] == "memory_write_processor"
        assert memory_meta["result_kind"] == "protocol_tool_receipt"
        assert tool_metadata_for("setup_security_gate")["handler"] == "setup_runner"
        assert tool_metadata_for("relation_read")["execution_route"] == "internal_processor"
        assert tool_metadata_for("index_view")["execution_route"] == "internal_processor"
        assert tool_metadata_for("made_up_table") == {}
        assert PROTOCOL_TOOL_IDS
        assert all(
            TOOL_DEFINITIONS[tool_id]["execution_route"] == "internal_processor"
            for tool_id in PROTOCOL_TOOL_IDS
        )
        assert "file_read" in GENERAL_TOOL_IDS
        file_read_meta = tool_metadata_for("file_read")
        assert file_read_meta["execution_route"] == "host_dispatch"
        assert file_read_meta["tool_class"] == "read_tool"
        assert file_read_meta["backend_type"] == "python"
        assert file_read_meta["handler"] == "file_read_handler"
        assert file_read_meta["permission_scope"] == "workspace_read_allowlist"
        assert file_read_meta["result_kind"] == "general_tool_result"
        for tool_id, handler in (
                ("web_fetch", "web_fetch_handler"),
                ("web_search", "web_search_handler")):
            meta = tool_metadata_for(tool_id)
            assert tool_id in GENERAL_TOOL_IDS
            assert meta["execution_route"] == "host_dispatch"
            assert meta["tool_class"] == "read_tool"
            assert meta["domain"] == "web"
            assert meta["backend_type"] == "python"
            assert meta["handler"] == handler
            assert meta["permission_scope"] == "public_web_read"
            assert meta["result_kind"] == "general_tool_result"
            assert meta["status"] == "enabled"
        file_edit_meta = tool_metadata_for("file_edit")
        assert file_edit_meta["execution_route"] == "host_dispatch"
        assert file_edit_meta["tool_class"] == "action_tool"
        assert file_edit_meta["domain"] == "filesystem"
        assert file_edit_meta["backend_type"] == "python"
        assert file_edit_meta["handler"] == "file_edit_handler"
        assert file_edit_meta["permission_scope"] == "workspace_patch_allowlist"
        assert file_edit_meta["result_kind"] == "general_tool_result"
        assert file_edit_meta["status"] == "enabled"
        file_write_meta = tool_metadata_for("file_write")
        assert file_write_meta["execution_route"] == "host_dispatch"
        assert file_write_meta["tool_class"] == "action_tool"
        assert file_write_meta["domain"] == "filesystem"
        assert file_write_meta["backend_type"] == "python"
        assert file_write_meta["handler"] == "file_write_handler"
        assert file_write_meta["permission_scope"] == "workspace_patch_allowlist"
        assert file_write_meta["result_kind"] == "general_tool_result"
        assert file_write_meta["status"] == "enabled"
        shell_meta = tool_metadata_for("shell_command")
        assert shell_meta["execution_route"] == "host_dispatch"
        assert shell_meta["tool_class"] == "action_tool"
        assert shell_meta["domain"] == "shell"
        assert shell_meta["backend_type"] == "python"
        assert shell_meta["handler"] == "shell_command_handler"
        assert shell_meta["permission_scope"] == "workspace_shell_allowlist"
        assert shell_meta["result_kind"] == "general_tool_result"
        assert shell_meta["status"] == "enabled"
        subagent_meta = tool_metadata_for("subagent_dispatch")
        assert subagent_meta["execution_route"] == "host_dispatch"
        assert subagent_meta["tool_class"] == "action_tool"
        assert subagent_meta["domain"] == "agent"
        assert subagent_meta["backend_type"] == "python"
        assert subagent_meta["handler"] == "subagent_dispatch_handler"
        assert subagent_meta["permission_scope"] == "subagent_task_scope"
        assert subagent_meta["result_kind"] == "general_tool_result"
        assert subagent_meta["status"] == "enabled"

        for tool_id in (
                "file_read",
                "file_edit",
                "file_write",
                "web_fetch",
                "web_search",
                "shell_command",
                "subagent_dispatch"):
            meta = tool_metadata_for(tool_id)
            backend = general_tool_backend_for(tool_id)
            assert meta["active_backend"] == backend["id"]
            assert meta["backend_candidates"]
            assert backend["id"] in {
                candidate["id"] for candidate in meta["backend_candidates"]
            }
            assert backend["backend_type"] == meta["backend_type"]
            assert backend["handler"] == meta["handler"]
            assert backend["permission_scope"] == meta["permission_scope"]
        assert general_tool_backend_for("memory_write") == {}

    def test_tool_document_has_no_activity_catalog(self):
        repo_root = Path(__file__).resolve().parents[3]
        tools_md = (
            repo_root
            / "UPSP"
            / "initialization" / "persona_template"
            / "docs"
            / "protocol"
            / "base"
            / "tools.md"
        )
        text = tools_md.read_text(encoding="utf-8")

        assert "PROTOCOL_TOOL_INDEX_START" not in text
        assert "GENERAL_TOOL_INDEX_START" not in text
        assert "当前 Frame 的 `01_tool_header` 是唯一活动工具清单" in text

    def test_spec089_relation_read_replaces_relation_card_read(self):
        from logic import protocol_tools

        assert protocol_tools.tool_metadata_for("relation_read")["execution_route"] == "internal_processor"
        assert protocol_tools.tool_class_for("relation_read") == "read_tool"
        assert protocol_tools.tool_metadata_for("relation_card_read") == {}
        assert protocol_tools.tool_metadata_for("relation_content_read") == {}

    def test_spec053_state_update_protocol_tool_is_retired(self):
        from logic import protocol_tools

        assert protocol_tools.tool_metadata_for("state_update") == {}
        assert protocol_tools.normalize_tool_id("state_update_table") == "state_update_table"

    def test_spec089_relation_read_processor_auto_mounts_summary_for_body(self):
        from logic.relation_read import apply_relation_read_requests

        class DummyRelationStore:
            def load_registry(self):
                return {"cards": [{
                    "id": "REL-Codex",
                    "name": "Codex",
                    "category": "them",
                    "status": "active",
                }]}

            def read_card(self, card_id, category=None):
                return {
                    "id": card_id,
                    "name": "Codex",
                    "category": category or "them",
                    "notes": [{"content": "正在做关系读取清缝"}],
                }

        receipts, mounts, unmounts = apply_relation_read_requests(
            [{
                "tool_id": "relation_read",
                "subject": "Codex",
                "summary": "none",
                "body": "temporary",
            }],
            {"relation_store": DummyRelationStore()},
        )

        assert receipts[0]["status"] == "accepted"
        assert unmounts == []
        assert receipts[0]["summary_mode"] == "temporary"
        assert receipts[0]["body_mode"] == "temporary"
        assert {
            "type": "relation_summary",
            "ids": "REL-Codex",
            "mode": "temporary",
            "subject": "Codex",
        } in mounts
        relation_mount = next(item for item in mounts if item["type"] == "relation")
        assert relation_mount["ids"] == "REL-Codex"
        assert relation_mount["subject"] == "Codex"
        assert relation_mount["mode"] == "temporary"
        assert relation_mount["source"] == "relation_read"
        assert relation_mount["read_mode"] == "full"

    def test_spec781_relation_read_none_clears_summary_and_resident_body(
            self, tmp_path):
        from data.resident_list_store import ResidentListStore
        from logic.relation_read import apply_relation_read_requests

        resident = ResidentListStore(str(tmp_path / "resident_list.json"))
        resident.reconcile()
        resident.add({"item_type": "relation", "item_id": "REL-Codex"})

        class DummyRelationStore:
            def __init__(self):
                self.summary_flags = []
                self.registry = {"cards": [{
                    "id": "REL-Codex",
                    "name": "Codex",
                    "category": "them",
                    "status": "active",
                    "summary_resident": True,
                }]}

            def load_registry(self):
                return copy.deepcopy(self.registry)

            def save_registry(self, value):
                self.registry = copy.deepcopy(value)

            def read_card(self, card_id, category=None):
                return {"id": card_id, "name": "Codex", "notes": []}

            def set_summary_resident(self, card_id, enabled=True):
                self.summary_flags.append((card_id, enabled))
                self.registry["cards"][0]["summary_resident"] = bool(enabled)

        class DummyAssembler:
            resident_store = resident

        store = DummyRelationStore()
        receipts, mounts, unmounts = apply_relation_read_requests(
            [{
                "tool_id": "relation_read",
                "subject": "Codex",
                "summary": "none",
                "body": "none",
            }],
            {
                "relation_store": store,
                "assembler": DummyAssembler(),
                "resident_store": resident,
            },
        )

        assert receipts[0]["status"] == "accepted"
        assert mounts == []
        assert store.summary_flags == [("REL-Codex", False)]
        assert unmounts == [
            {"item_type": "relation", "item_id": "REL-Codex"},
            {"item_type": "relation_summary", "item_id": "REL-Codex"},
        ]
        assert receipts[0]["resident_revision"] == resident.load()["revision"]
        assert not resident.contains(
            item_type="relation", item_id="REL-Codex")
        assert not store.load_registry()["cards"][0]["summary_resident"]

    @pytest.mark.parametrize(
        ("request_payload", "initial_body_resident", "initial_summary_resident"),
        [
            (
                {"summary": "none", "body": "resident"},
                False,
                False,
            ),
            (
                {"summary": "none", "body": "none"},
                True,
                True,
            ),
        ],
    )
    def test_spec781_relation_resident_summary_failure_rolls_back_both_stores(
            self,
            tmp_path,
            request_payload,
            initial_body_resident,
            initial_summary_resident):
        from data.resident_list_store import ResidentListStore
        from logic.relation_read import apply_relation_read_requests

        resident = ResidentListStore(str(tmp_path / "resident_list.json"))
        resident.reconcile()
        if initial_body_resident:
            resident.add({"item_type": "relation", "item_id": "REL-Codex"})

        class DummyAssembler:
            resident_store = resident

            @staticmethod
            def _load_relation_content(_card_id):
                return "Codex\n关系事务正文"

            @staticmethod
            def preflight_resident_add(item, *, content_overrides=None):
                preview = resident.preview_add(item)
                return {
                    "document": preview["document"],
                    "changed": preview["changed"],
                    "chars": len(next(iter(
                        (content_overrides or {}).values()), "")),
                    "expected_revision": resident.load()["revision"],
                }

        class FailingSummaryStore:
            def __init__(self):
                self.registry = {"cards": [{
                    "id": "REL-Codex",
                    "name": "Codex",
                    "category": "them",
                    "status": "active",
                    "summary_resident": initial_summary_resident,
                }]}

            def load_registry(self):
                return copy.deepcopy(self.registry)

            def save_registry(self, value):
                self.registry = copy.deepcopy(value)

            def read_card(self, card_id, category=None):
                return {
                    "id": card_id,
                    "name": "Codex",
                    "category": category or "them",
                    "notes": [{"content": "关系事务正文"}],
                }

            @staticmethod
            def set_summary_resident(_card_id, _enabled=True):
                raise RuntimeError("injected_summary_write_failure")

        relation = FailingSummaryStore()
        before_resident = resident.snapshot_bytes()
        before_registry = relation.load_registry()
        payload = {
            "tool_id": "relation_read",
            "subject": "Codex",
            **request_payload,
        }

        receipts, mounts, unmounts = apply_relation_read_requests(
            [payload],
            {
                "relation_store": relation,
                "assembler": DummyAssembler(),
                "resident_store": resident,
            },
        )

        assert receipts[0]["status"] == "rejected"
        assert receipts[0]["reason"].startswith(
            "relation_resident_transaction_failed:")
        assert mounts == []
        assert unmounts == []
        assert resident.snapshot_bytes() == before_resident
        assert relation.load_registry() == before_registry

    def test_spec078_container_read_rejects_missing_container(self, tmp_path, monkeypatch):
        from data import container_store as cs
        from logic.container_read import apply_container_read_requests

        new_dirs = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
        monkeypatch.setattr(cs, "PREFIX_TO_DIR", new_dirs)
        monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
        monkeypatch.setattr(cs, "LTM_INDEX_MD", str(tmp_path / "index.md"), raising=False)

        receipts, mounts = apply_container_read_requests(
            [{
                "tool_id": "container_read",
                "container_id": "PRJ-20990101-99",
                "target_file": "notes.md",
            }],
            {"container_store": cs.ContainerStore()},
        )

        assert mounts == []
        assert receipts == [{
            "tool_id": "container_read",
            "status": "rejected",
            "source": "protocol_tool_request",
            "container_id": "PRJ-20990101-99",
            "target_file": "notes.md",
            "content": "",
            "read_mode": "",
            "range_requested": None,
            "range_applied": None,
            "total_lines": 0,
            "total_chars": 0,
            "resident_persisted": False,
            "resident_revision": None,
            "protocol_tool_receipt": True,
            "reason": "container_not_found",
        }]

class TestMemoryWriteProtocol:
    class SubjectRelationStore:
        SUBJECTS = {
            "FMZ": "FMZ",
            "Codex": "Codex",
            "TzPz": "TzPz",
            "伙伴": "TzPz",
        }

        def resolve_active_subject(self, value):
            return self.SUBJECTS.get(str(value or "").strip())

    def test_feelings_normalize_per_item_without_rejecting_memory_body(self):
        from logic.memory_write import _normalize_feelings

        interaction, relationships, rejected = _normalize_feelings({
            "interaction_feelings": ["专注", "不存在", "宁静", "好奇", "兴奋"],
            "relationship_feelings": [
                {"subject": "伙伴", "word": "可靠"},
                {"subject": "TzPz", "word": "默契"},
                {"subject": "TzPz", "word": "珍惜"},
                {"subject": "Codex", "word": "坦率"},
                {"subject": "未登记", "word": "可靠"},
                {"subject": "Codex", "word": "不存在"},
                {"subject": "Codex"},
                "可靠",
            ],
        }, self.SubjectRelationStore())

        assert interaction == ["专注", "宁静", "好奇"]
        assert relationships == [
            {"subject": "TzPz", "word": "可靠"},
            {"subject": "TzPz", "word": "默契"},
            {"subject": "Codex", "word": "坦率"},
        ]
        assert [item["reason"] for item in rejected] == [
            "feeling_not_in_table",
            "interaction_limit_exceeded",
            "per_subject_limit_exceeded",
            "subject_not_in_relation_domain",
            "feeling_not_in_table",
            "invalid_shape",
            "invalid_shape",
        ]

    def test_new_memory_call_rejects_legacy_relation_feeling_strings(self):
        from logic.memory_write import _normalize_feelings

        interaction, relationships, rejected = _normalize_feelings(
            {"relation_feelings": ["可靠"]},
            self.SubjectRelationStore(),
        )

        assert interaction == []
        assert relationships == []
        assert rejected == [{
            "domain": "relationship",
            "index": 0,
            "reason": "legacy_field_not_accepted",
        }]

    def test_spec219_memory_write_allows_multiple_declarations_in_order(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def __init__(self):
                self.written = []
                self.meta = []
                self.index = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def set_meta(self, *args, **kwargs):
                self.meta.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.index.append((args, kwargs))

        class DummyMemoryIndex:
            def __init__(self):
                self.stm_keywords = []

            def add_stm_keywords(self, *args, **kwargs):
                self.stm_keywords.append((args, kwargs))

        class DummyMemoryHeat:
            def __init__(self):
                self.entries = []

            def new_entry(self, weight=2):
                return _dummy_heat_entry(weight)

            def set_entry(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        ids = iter([
            "MEM-041000AA",
            "MEM-041000AB",
            "MEM-041000AC",
            "MEM-041000AD",
        ])
        monkeypatch.setattr(mw, "generate_mem_id", lambda: next(ids))
        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        memory_heat = DummyMemoryHeat()

        subjects = ["FMZ", "Codex", "TzPz", "伙伴"]
        declarations = [{
            "title": f"多条写入{i}",
            "weight": i,
            "subject": subjects[i - 1],
            "body": f"我确认第 {i} 条非噪音材料可以按权重写入。",
            "candidate_keywords": [f"条目{i}", "多条写入"],
        } for i in range(1, 5)]

        state = {"presence": {"confirmed_subjects": ["Codex"]}}
        receipts = mw.apply_memory_write_declarations(
            declarations,
            state,
            41,
            {
                "memory_store": memory_store,
                "memory_index": memory_index,
                "memory_heat": memory_heat,
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert [receipt["status"] for receipt in receipts] == ["applied"] * 4
        assert [receipt["mem_id"] for receipt in receipts] == [
            "MEM-041000AA",
            "MEM-041000AB",
            "MEM-041000AC",
            "MEM-041000AD",
        ]
        assert [receipt["subject"] for receipt in receipts] == [
            "FMZ", "Codex", "TzPz", "TzPz",
        ]
        assert state == {"presence": {"confirmed_subjects": ["Codex"]}}
        assert [receipt["title"] for receipt in receipts] == [
            "多条写入1",
            "多条写入2",
            "多条写入3",
            "多条写入4",
        ]
        assert len(memory_store.written) == 4
        assert len(memory_store.index) == 4
        assert len(memory_heat.entries) == 4

    def test_spec219_memory_write_mixed_batch_keeps_valid_declarations(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def __init__(self):
                self.written = []
                self.meta = []
                self.index = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def set_meta(self, *args, **kwargs):
                self.meta.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.index.append((args, kwargs))

        class DummyMemoryIndex:
            def __init__(self):
                self.stm_keywords = []

            def add_stm_keywords(self, *args, **kwargs):
                self.stm_keywords.append((args, kwargs))

        class DummyMemoryHeat:
            def __init__(self):
                self.entries = []

            def new_entry(self, weight=2):
                return _dummy_heat_entry(weight)

            def set_entry(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        ids = iter(["MEM-041100AA", "MEM-041100AB"])
        monkeypatch.setattr(mw, "generate_mem_id", lambda: next(ids))
        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        memory_heat = DummyMemoryHeat()

        receipts = mw.apply_memory_write_declarations(
            [
                {
                    "title": "有效一",
                    "weight": 3,
                    "subject": "TzPz",
                    "body": "我确认第一条有效材料写入。",
                    "candidate_keywords": ["有效", "多条"],
                },
                {
                    "title": "缺关键词",
                    "weight": 3,
                    "subject": "TzPz",
                    "body": "这一条应只影响自身。",
                    "candidate_keywords": [],
                },
                {
                    "title": "权重零",
                    "weight": 0,
                    "subject": "TzPz",
                    "body": "这一条是噪音。",
                    "candidate_keywords": ["噪音"],
                },
                {
                    "title": "有效二",
                    "weight": 2,
                    "subject": "TzPz",
                    "body": "我确认第二条有效材料继续写入。",
                    "candidate_keywords": ["继续", "多条"],
                },
            ],
            {"presence": {"confirmed_subjects": ["TzPz"]}},
            41,
            {
                "memory_store": memory_store,
                "memory_index": memory_index,
                "memory_heat": memory_heat,
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert [receipt["status"] for receipt in receipts] == [
            "applied",
            "error",
            "skipped",
            "applied",
        ]
        assert [receipt["reason"] for receipt in receipts] == [
            "",
            "missing_keywords",
            "weight_zero",
            "",
        ]
        assert [receipt["mem_id"] for receipt in receipts] == [
            "MEM-041100AA",
            None,
            None,
            "MEM-041100AB",
        ]
        assert len(memory_store.written) == 2
        assert len(memory_store.index) == 2
        assert len(memory_heat.entries) == 2

    def test_apply_memory_write_declaration_rejects_missing_keywords(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def __init__(self):
                self.written = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

        class DummyMemoryIndex:
            def __init__(self):
                self.stm_keywords = []

            def add_stm_keywords(self, *args, **kwargs):
                self.stm_keywords.append((args, kwargs))

        class DummyMemoryHeat:
            def __init__(self):
                self.entries = []

            def new_entry(self, weight=2):
                return _dummy_heat_entry(weight)

            def set_entry(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        monkeypatch.setattr(mw, "generate_mem_id", lambda: "MEM-041000AA")
        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        memory_heat = DummyMemoryHeat()

        receipts = mw.apply_memory_write_declarations(
            [{
                "title": "善后瘦身",
                "weight": 4,
                "subject": "Codex",
                "body": "我确认善后步不再承担记忆写入本体职责。",
                "candidate_keywords": [],
            }],
            {"base": {"activity_mode": "理论"}},
            41,
            {
                "memory_store": memory_store,
                "memory_index": memory_index,
                "memory_heat": memory_heat,
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert receipts == [{
            "tool_id": "memory_write",
            "status": "error",
            "source": "memory_write_declaration",
            "mem_id": None,
            "title": "善后瘦身",
            "weight": 4,
            "subject": "Codex",
            "keywords": [],
            "reason": "missing_keywords",
        }]
        assert memory_store.written == []
        assert memory_index.stm_keywords == []
        assert memory_heat.entries == []

    def test_apply_memory_write_declaration_limits_keywords_by_memory_type(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def __init__(self):
                self.written = []
                self.meta = []
                self.index = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def set_meta(self, *args, **kwargs):
                self.meta.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.index.append((args, kwargs))

        class DummyMemoryIndex:
            def __init__(self):
                self.stm_keywords = []

            def add_stm_keywords(self, *args, **kwargs):
                self.stm_keywords.append((args, kwargs))

        class DummyMemoryHeat:
            def __init__(self):
                self.entries = []

            def new_entry(self, weight=2):
                return _dummy_heat_entry(weight)

            def set_entry(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        monkeypatch.setattr(mw, "generate_mem_id", lambda: "MEM-041000AA")
        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        memory_heat = DummyMemoryHeat()
        candidates = ["一", "二", "三", "四", "五", "六", "七", "八", "九"]

        receipts = mw.apply_memory_write_declarations(
            [{
                "title": "关键词上限",
                "weight": 2,
                "subject": "Codex",
                "body": "短期记忆创建时已经带 A 型。",
                "candidate_keywords": candidates,
            }],
            {"base": {"activity_mode": "理论"}},
            41,
            {
                "memory_store": memory_store,
                "memory_index": memory_index,
                "memory_heat": memory_heat,
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert receipts[0]["status"] == "applied"
        assert receipts[0]["keywords"] == ["一", "二", "三", "四"]
        assert memory_store.written[0][1]["tags"] == ["一", "二", "三", "四"]
        assert "annotation" not in memory_store.written[0][1]
        assert memory_store.written[0][1]["dream"] is False
        assert memory_store.written[0][1]["current_overview"] == ""
        assert "abstract" not in memory_store.written[0][1]
        assert memory_store.meta[0][0][1]["tags"] == ["一", "二", "三", "四"]
        assert len(memory_store.meta[0][0][1]) == 24
        assert memory_store.meta[0][0][1]["dream"] is False
        assert memory_store.meta[0][0][1]["current_overview"] == ""
        retired = {"abstract", "locked", "source_rounds", "mode", "merged_from"}
        assert retired.isdisjoint(memory_store.meta[0][0][1])
        assert memory_store.ltm_entry_state("MEM-041000AA")["meta"]["tags"] == [
            "一", "二", "三", "四"]

    def test_spec224_memory_write_body_too_long_stops_all_side_writes(
            self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def __init__(self):
                self.meta = []
                self.index = []

            def write_entry(self, *args, **kwargs):
                raise ValueError("memory_body_too_long:max=128;actual=129")

            def set_meta(self, *args, **kwargs):
                self.meta.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.index.append((args, kwargs))

        class DummyMemoryIndex:
            def __init__(self):
                self.stm_keywords = []

            def add_stm_keywords(self, *args, **kwargs):
                self.stm_keywords.append((args, kwargs))

        class DummyMemoryHeat:
            def __init__(self):
                self.entries = []

            def new_entry(self, weight=2):
                return _dummy_heat_entry(weight)

            def set_entry(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        monkeypatch.setattr(mw, "generate_mem_id", lambda: "MEM-224000AA")
        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        memory_heat = DummyMemoryHeat()

        receipts = mw.apply_memory_write_declarations(
            [{
                "title": "超限记忆",
                "weight": 2,
                "subject": "Codex",
                "body": "甲" * 129,
                "candidate_keywords": ["超限", "记忆"],
            }],
            {"presence": {"confirmed_subjects": ["Codex"]}},
            224,
            {
                "memory_store": memory_store,
                "memory_index": memory_index,
                "memory_heat": memory_heat,
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert receipts[0]["status"] == "error"
        assert receipts[0]["reason"] == "memory_body_too_long:max=128;actual=129"
        assert receipts[0]["max_chars"] == 128
        assert receipts[0]["actual_chars"] == 129
        assert receipts[0]["over_by"] == 1
        assert receipts[0]["target_chars"] == 120
        assert receipts[0]["reduce_by"] == 9
        assert receipts[0]["next_action"] == "use_memory_write_rewrite_guide"
        assert "weight_options" not in receipts[0]
        assert "actual=129, max=128" in receipts[0]["retry_instruction"]
        assert "即时重写指南" in receipts[0]["retry_instruction"]
        assert "不要直接重试 memory_write" in receipts[0]["retry_instruction"]
        assert memory_store.meta == []
        assert memory_store.index == []
        assert memory_index.stm_keywords == []
        assert memory_heat.entries == []

    def test_spec759_body_too_long_requests_immediate_rewrite_guide(self):
        from logic import memory_write as mw

        receipt = mw._receipt(
            "error",
            {"title": "长读书梗概", "weight": 2, "subject": "Codex"},
            reason="memory_body_too_long:max=128;actual=176",
        )

        assert "recommended_weight" not in receipt
        assert "actual=176, max=128" in receipt["retry_instruction"]
        assert "即时重写指南" in receipt["retry_instruction"]
        assert "不要直接重试 memory_write" in receipt["retry_instruction"]
        assert "优先升级" not in receipt["retry_instruction"]

    def test_apply_memory_write_declaration_writes_entry_and_receipt(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def __init__(self):
                self.written = []
                self.meta = []
                self.index = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def set_meta(self, *args, **kwargs):
                self.meta.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.index.append((args, kwargs))

        class DummyMemoryIndex:
            def __init__(self):
                self.stm_keywords = []

            def add_stm_keywords(self, *args, **kwargs):
                self.stm_keywords.append((args, kwargs))

        class DummyMemoryHeat:
            def __init__(self):
                self.entries = []

            def new_entry(self, weight=2):
                return _dummy_heat_entry(weight)

            def set_entry(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        class DummyContainerStore:
            def get_container_info(self, cid):
                return {"id": cid}

        monkeypatch.setattr(mw, "generate_mem_id", lambda: "MEM-041000AA")
        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        memory_heat = DummyMemoryHeat()

        receipts = mw.apply_memory_write_declarations(
            [{
                "title": "善后瘦身",
                "weight": 4,
                "subject": "Codex",
                "body": "我确认善后步不再承担记忆写入本体职责。",
                "candidate_keywords": ["善后", "记忆写入", "关键词边界"],
                "linked_containers": ["DC-R41-001"],
                "interaction_feelings": ["专注", "宁静"],
                "relationship_feelings": [{"subject": "伙伴", "word": "可靠"}],
            }],
            {"base": {"activity_mode": "理论"}},
            41,
            {
                "memory_store": memory_store,
                "memory_index": memory_index,
                "memory_heat": memory_heat,
                "container_store": DummyContainerStore(),
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert receipts == [{
            "tool_id": "memory_write",
            "status": "applied",
            "source": "memory_write_declaration",
            "mem_id": "MEM-041000AA",
            "title": "善后瘦身",
            "weight": 4,
            "subject": "Codex",
            "keywords": ["善后", "记忆写入", "关键词边界"],
            "interaction_feelings": ["专注", "宁静"],
            "relationship_feelings": [{"subject": "TzPz", "word": "可靠"}],
            "reason": "",
            "ltm_layer": "LTM/Summary",
            "stm_present": True,
            "stored_at": "",
            "admission_status": "pending",
        }]
        assert memory_store.written[0][0][0] == "MEM-041000AA"
        assert memory_store.written[0][1]["tags"] == ["善后", "记忆写入", "关键词边界"]
        assert memory_store.written[0][1]["feelings"] == ["专注", "宁静", "TzPz:可靠"]
        assert "annotation" not in memory_store.written[0][1]
        assert memory_store.written[0][1]["dream"] is False
        assert memory_store.written[0][1]["current_overview"] == ""
        assert "abstract" not in memory_store.written[0][1]
        assert memory_store.meta[0][0][0] == "MEM-041000AA"
        assert "annotation" not in memory_store.index[0][1]
        assert memory_store.index[0][1]["dream"] is False
        assert memory_store.index[0][1]["current_overview"] == ""
        assert memory_store.ltm_entry_state("MEM-041000AA")["meta"]["tags"] == [
            "善后", "记忆写入", "关键词边界"]
        assert memory_heat.entries[0][0][0] == "MEM-041000AA"

    def test_memory_write_rejects_when_presence_unconfirmed(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def write_entry(self, *args, **kwargs):
                raise AssertionError("unconfirmed identity must not write")

        receipts = mw.apply_memory_write_declarations(
            [{
                "title": "identity gate",
                "weight": 3,
                "subject": "unknown",
                "body": "body",
                "candidate_keywords": ["identity"],
            }],
            {"presence": {"confirmed_subjects": []}},
            41,
            {
                "memory_store": DummyMemoryStore(),
                "memory_index": object(),
                "memory_heat": object(),
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert receipts[0]["status"] == "error"
        assert receipts[0]["reason"] == "identity_unresolved"
        assert receipts[0]["submitted_subject"] == "unknown"
        assert receipts[0]["confirmed_subject"] == ""
        assert receipts[0]["confirmed_subjects"] == []

    def test_memory_write_fills_unknown_subject_from_confirmed_presence(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def __init__(self):
                self.written = []
                self.meta = []
                self.index = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def set_meta(self, *args, **kwargs):
                self.meta.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.index.append((args, kwargs))

        class DummyMemoryIndex:
            def add_stm_keywords(self, *args, **kwargs):
                pass

        class DummyMemoryHeat:
            def new_entry(self, weight=2):
                return _dummy_heat_entry(weight)

            def set_entry(self, *args, **kwargs):
                pass

        monkeypatch.setattr(mw, "generate_mem_id", lambda: "MEM-041000AB")
        memory_store = DummyMemoryStore()
        receipts = mw.apply_memory_write_declarations(
            [{
                "title": "identity gate",
                "weight": 3,
                "subject": "unknown",
                "body": "body",
                "candidate_keywords": ["identity"],
            }],
            {"presence": {"confirmed_subjects": ["TzPz"]}},
            41,
            {
                "memory_store": memory_store,
                "memory_index": DummyMemoryIndex(),
                "memory_heat": DummyMemoryHeat(),
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert receipts[0]["status"] == "applied"
        assert receipts[0]["subject"] == "TzPz"
        assert memory_store.written[0][1]["subject"] == "TzPz"

    def test_memory_write_rejects_subject_outside_relation_domain(self, monkeypatch):
        from logic import memory_write as mw

        class DummyMemoryStore(_TransactionalMemoryStoreFake):
            def write_entry(self, *args, **kwargs):
                raise AssertionError("unknown relation subject must not write")

        receipts = mw.apply_memory_write_declarations(
            [{
                "title": "identity gate",
                "weight": 3,
                "subject": "Other",
                "body": "body",
                "candidate_keywords": ["identity"],
            }],
            {"presence": {"confirmed_subjects": ["TzPz"]}},
            41,
            {
                "memory_store": DummyMemoryStore(),
                "memory_index": object(),
                "memory_heat": object(),
                "relation_store": self.SubjectRelationStore(),
            },
        )

        assert receipts[0]["status"] == "error"
        assert receipts[0]["reason"] == "subject_not_in_relation_domain"
        assert receipts[0]["submitted_subject"] == "Other"
        assert receipts[0]["interaction_object"] == "TzPz"
        assert receipts[0]["confirmed_subject"] == "TzPz"
        assert receipts[0]["confirmed_subjects"] == ["TzPz"]
        assert receipts[0]["resolved_subject"] == ""


class TestMemoryAnnotationProtocol:
    def test_memory_annotation_tool_is_retired_from_llm_facing_gate(self):
        from logic.protocol_tools import (
            tool_metadata_for,
            normalize_tool_id,
        )

        assert normalize_tool_id("memory_annotation_declaration") == "memory_annotation_declaration"
        assert tool_metadata_for("memory_annotation_declaration") == {}


class TestMemoryRecallCompletionProtocol:
    def test_memory_recall_tool_and_legacy_alias_are_retired(self):
        from logic.protocol_tools import (
            tool_metadata_for,
            normalize_tool_id,
        )

        assert normalize_tool_id("memory_recall_completion_request") == (
            "memory_recall_completion_request"
        )
        assert tool_metadata_for("memory_recall_completion_request") == {}

    def test_reconsolidation_tracker_uses_creation_coordinate_not_container_notes(self):
        from logic.memory_reconsolidation import MemoryReconsolidationTracker

        tracker = MemoryReconsolidationTracker(49)
        item = tracker.register({
            "source": "ltm",
            "ltm": {"tier": "Abstract"},
            "body": "**梗概**：旧事实",
            "meta": {
                "id": "MEM-041000AA",
                "title": "旧标题",
                "weight": 4,
                "stored_at": "2026-08-01T00:00:00+08:00",
                "access": "public",
                "tags": ["旧事实"],
                "created_instance_id": "meta",
                "created_round": 41,
            },
        })

        assert item["created_instance_id"] == "meta"
        assert item["created_round"] == 41
        assert "linked_containers" not in item

    def test_reconsolidation_guide_rejects_missing_results(self):
        from logic.memory_reconsolidation import (
            MemoryReconsolidationTracker,
            apply_memory_reconsolidation_guide,
        )

        tracker = MemoryReconsolidationTracker(49)
        receipt = apply_memory_reconsolidation_guide(
            {
                "guide_id": tracker.guide_id,
                "item_id": "memory_reconsolidation_due",
                "option_id": "submit_memory_reconsolidations",
                "fields": {},
            },
            {"memory_reconsolidation_tracker": tracker},
        )

        assert receipt["status"] == "rejected"
        assert receipt["reason"] == "memory_reconsolidation_context_unavailable"

    def test_reconsolidation_title_marker_is_idempotent(self):
        from logic.memory_reconsolidation import MemoryReconsolidationProcessor

        assert MemoryReconsolidationProcessor._completed_title({
            "title": "旧标题"
        }) == "旧标题[回忆重整]"
        assert MemoryReconsolidationProcessor._completed_title({
            "title": "旧标题[回忆重整]"
        }) == "旧标题[回忆重整]"
        assert MemoryReconsolidationProcessor._completed_title({
            "title": "旧标题[召回补全内容]"
        }) == "旧标题[召回补全内容]"
