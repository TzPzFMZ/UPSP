import json
import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


class TestRuntimeMemoryLifecycle(RuntimeTestMixin):
    @pytest.fixture(autouse=True)
    def _isolated_ltm(self, tmp_path, monkeypatch):
        import paths
        from data import memory_store

        root = tmp_path / "isolated-ltm"
        configs = {
            "FULL": ("full.md", "F"),
            "SUMMARY": ("summary.md", "S"),
            "ABSTRACT": ("abstract.md", "A"),
            "PINNED": ("pinned.md", "P"),
            "BACKUP": ("backup.md", "B"),
        }
        for tier, (body_name, _code) in configs.items():
            directory = root / tier.title()
            directory.mkdir(parents=True)
            body_attr = f"LTM_{tier}_{tier}_MD" if tier != "FULL" else "LTM_FULL_FULL_MD"
            monkeypatch.setattr(paths, f"LTM_{tier}_DIR", str(directory))
            monkeypatch.setattr(paths, body_attr, str(directory / body_name))
            monkeypatch.setattr(paths, f"LTM_{tier}_META_JSON", str(directory / "meta.json"))
            monkeypatch.setattr(paths, f"LTM_{tier}_INDEX_MD", str(directory / "index.md"), raising=False)
        keywords = root / "keywords.json"
        monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(keywords))
        monkeypatch.setattr(
            paths, "MEMORY_COMPRESSION_PENDING_JSON",
            str(root / "memory_compression_pending.json"),
        )
        monkeypatch.setattr(
            memory_store, "LTM_MEMORY_LINKS_JSON", str(tmp_path / "memory_links.json"))

    def _patch_ltm_tier(
            self,
            monkeypatch,
            paths,
            tier,
            tier_dir,
            body_path,
            meta_path,
            index_path):
        prefix = f"LTM_{tier.upper()}"
        body_attr = {
            "FULL": "FULL_MD",
            "SUMMARY": "SUMMARY_MD",
            "ABSTRACT": "ABSTRACT_MD",
            "BACKUP": "BACKUP_MD",
        }[tier.upper()]
        monkeypatch.setattr(paths, f"{prefix}_DIR", str(tier_dir))
        monkeypatch.setattr(paths, f"{prefix}_{body_attr}", str(body_path))
        monkeypatch.setattr(paths, f"{prefix}_META_JSON", str(meta_path))
        monkeypatch.setattr(paths, f"{prefix}_INDEX_MD", str(index_path), raising=False)

    def test_cleanup_forgetting_skips_private_entries_and_keeps_public_behavior(
            self, monkeypatch):
        from data.memory_store import MemoryStore
        from engines.cleanup_pipeline import CleanupPipeline
        from types import SimpleNamespace

        private_id = "MEM-PRIVATE1"
        public_id = "MEM-PUBLIC01"

        class Heat:
            @staticmethod
            def load_heat():
                return {"entries": {
                    private_id: {
                        "degrade": True,
                        "stored": False,
                        "compression": True,
                    },
                    public_id: {
                        "degrade": True,
                        "compression": False,
                    },
                }}

        monkeypatch.setattr(
            MemoryStore,
            "active_ltm_meta_by_id",
            lambda _self: {
                private_id: {
                    "id": private_id,
                    "access": "private",
                    "stored_at": "",
                },
                public_id: {
                    "id": public_id,
                    "access": "public",
                    "stored_at": "2026-08-14T00:00:00+08:00",
                },
            },
        )
        monkeypatch.setattr(
            MemoryStore, "verify_ltm_entry", lambda _self, _mem_id: "Full")
        cleanup = CleanupPipeline(SimpleNamespace(
            heat=Heat(),
            memory_store=MemoryStore(),
        ))
        candidates, _to_abstract, _need_compress = cleanup._forgetting_candidates()
        assert candidates == [public_id]
        assert private_id not in candidates

    def test_spec735_private_upgrade_never_enters_public_ltm(self):
        from engines.cleanup_pipeline import CleanupPipeline
        from types import SimpleNamespace

        class Heat:
            @staticmethod
            def check_upgrade():
                return ["MEM-PRIVATE1"]

        class Store:
            @staticmethod
            def read_stm_meta_by_id(_mem_id):
                return {"id": "MEM-PRIVATE1", "access": "private"}

        result = {}
        cleanup = CleanupPipeline(SimpleNamespace(
            heat=Heat(),
            memory_store=Store(),
        ))

        with pytest.raises(RuntimeError, match="memory_lifecycle_failed"):
            cleanup._process_memory_lifecycle(10, settlement_result=result)

        assert result["_memory_lifecycle_receipts"] == [{
            "event": "memory_lifecycle_failed",
            "status": "failed",
            "mem_id": "MEM-PRIVATE1",
            "tier": "Full",
            "reason": "ValueError:private_memory_deferred",
        }]

    def test_spec746_normal_upgrade_only_fills_stored_at_and_retains_stm(self):
        from engines.cleanup_pipeline import CleanupPipeline
        from types import SimpleNamespace

        calls = []

        class Heat:
            @staticmethod
            def check_upgrade():
                return ["MEM-74300008"]

        class Store:
            @staticmethod
            def read_stm_meta_by_id(_mem_id):
                return {
                    "id": "MEM-74300008",
                    "access": "public",
                    "title": "升格后双驻留",
                    "weight": 5,
                }

            @staticmethod
            def ltm_entry_state(_mem_id, include_backup=False):
                assert include_backup is False
                return {"tier": "Full", "meta": {"weight": 5}}

            @staticmethod
            def admit_ltm_entry(mem_id):
                calls.append(("admit", mem_id))
                return {"stored_at": "2026-08-14T00:00:00+08:00"}

        cleanup = CleanupPipeline(SimpleNamespace(
            heat=Heat(),
            memory_store=Store(),
        ))
        receipts = cleanup._process_memory_lifecycle(10)

        assert calls == [("admit", "MEM-74300008")]
        assert receipts[0]["tier"] == "Full"
        assert receipts[0]["stored_at"] == "2026-08-14T00:00:00+08:00"
        assert receipts[0]["stm_retained"] is True

    def test_ltm_degradation_candidates_use_countdown_not_weight(self, tmp_path, monkeypatch):
        import json
        import paths

        from engines.ltm_degradation import LTMDegradationManager

        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        summary_dir.mkdir(parents=True)
        summary_md = summary_dir / "summary.md"
        summary_index = summary_dir / "index.md"
        summary_meta = summary_dir / "meta.json"
        low_not_due = "MEM-ABC00001"
        due = "MEM-ABC00002"
        summary_md.write_text(
            f"## {low_not_due}  [S]  权重1\n低权重但未到期。\n\n"
            f"## {due}  [S]  权重5\n高权重但已到期。\n",
            encoding="utf-8",
        )
        summary_index.write_text("", encoding="utf-8")
        summary_meta.write_text(json.dumps({
            low_not_due: {
                "id": low_not_due,
                "title": "低权重未到期",
                "weight": 1,
                "type": "S",
                "decay_period_days": 30,
                "decay_countdown_days": 3,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
            due: {
                "id": due,
                "title": "高权重已到期",
                "weight": 5,
                "type": "S",
                "decay_period_days": 30,
                "decay_countdown_days": 0,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
        }, ensure_ascii=False), encoding="utf-8")

        self._patch_ltm_tier(
            monkeypatch, paths, "Summary", summary_dir,
            summary_md, summary_meta, summary_index)

        candidates = LTMDegradationManager().due_entries("Summary")

        assert [entry[0] for entry in candidates] == [due]

    def test_ltm_degradation_daily_countdown_decrements_positive_only(self, tmp_path, monkeypatch):
        import json
        import paths

        from engines.ltm_degradation import LTMDegradationManager

        full_dir = tmp_path / "LTM" / "Memory" / "Full"
        full_dir.mkdir(parents=True)
        full_md = full_dir / "full.md"
        full_index = full_dir / "index.md"
        full_meta = full_dir / "meta.json"
        full_md.write_text("", encoding="utf-8")
        full_index.write_text("", encoding="utf-8")
        full_meta.write_text(json.dumps({
            "MEM-ABC10001": {
                "id": "MEM-ABC10001",
                "title": "还有两天",
                "weight": 5,
                "type": "F",
                "decay_period_days": 30,
                "decay_countdown_days": 2,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
            "MEM-ABC10002": {
                "id": "MEM-ABC10002",
                "title": "已经到期",
                "weight": 5,
                "type": "F",
                "decay_period_days": 30,
                "decay_countdown_days": 0,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
        }, ensure_ascii=False), encoding="utf-8")

        self._patch_ltm_tier(
            monkeypatch, paths, "Full", full_dir,
            full_md, full_meta, full_index)

        assert LTMDegradationManager().decrement_daily_countdowns() is True

        meta = json.loads(full_meta.read_text(encoding="utf-8"))
        assert meta["MEM-ABC10001"]["decay_countdown_days"] == 1
        assert meta["MEM-ABC10002"]["decay_countdown_days"] == 0

    def test_ltm_degradation_missing_compression_keeps_due_source(self, tmp_path, monkeypatch):
        import json
        import paths

        from engines.ltm_degradation import LTMDegradationManager

        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        summary_dir.mkdir(parents=True)
        abstract_dir.mkdir(parents=True)
        summary_md = summary_dir / "summary.md"
        summary_index = summary_dir / "index.md"
        summary_meta = summary_dir / "meta.json"
        abstract_md = abstract_dir / "abstract.md"
        abstract_index = abstract_dir / "index.md"
        abstract_meta = abstract_dir / "meta.json"
        mem_id = "MEM-ABC20001"
        summary_md.write_text(f"## {mem_id}  [S]  权重3\n到期但没有 LLM 压缩结果。\n", encoding="utf-8")
        summary_index.write_text(f"| {mem_id} | [S] | 3 | 到期 | Codex | 00001 | null |\n", encoding="utf-8")
        summary_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id,
                "title": "到期",
                "weight": 3,
                "type": "S",
                "decay_period_days": 30,
                "decay_countdown_days": 0,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
        }, ensure_ascii=False), encoding="utf-8")
        abstract_md.write_text("", encoding="utf-8")
        abstract_index.write_text("", encoding="utf-8")
        abstract_meta.write_text("{}", encoding="utf-8")

        self._patch_ltm_tier(
            monkeypatch, paths, "Summary", summary_dir,
            summary_md, summary_meta, summary_index)
        self._patch_ltm_tier(
            monkeypatch, paths, "Abstract", abstract_dir,
            abstract_md, abstract_meta, abstract_index)

        candidates = LTMDegradationManager().due_entries("Summary")

        assert [entry[0] for entry in candidates] == [mem_id]
        assert mem_id in summary_md.read_text(encoding="utf-8")
        assert json.loads(summary_meta.read_text(encoding="utf-8"))[mem_id]["decay_countdown_days"] == 0
        assert mem_id not in abstract_md.read_text(encoding="utf-8")

    def test_strip_memory_heading_removes_existing_heading(self):
        from engines.cleanup_helpers import (
            extract_memory_field,
            extract_memory_free_text,
            strip_memory_heading,
        )

        text = "## MEM-0ABCDEF0  [F]\n**标题**：测试\n我确认这是一条正文。"

        assert strip_memory_heading(text) == "**标题**：测试\n我确认这是一条正文。"
        assert strip_memory_heading("我确认这是一条正文。") == "我确认这是一条正文。"
        assert extract_memory_field("感受词：谨慎乐观", "感受词") == "谨慎乐观"
        assert extract_memory_free_text(strip_memory_heading(text)) == "我确认这是一条正文。"

    def test_process_forgetting_removes_stored_stm_copy_completely(self, tmp_path, monkeypatch):
        """已入库遗忘分支必须删除 STM 正文、meta、index、keywords、heat 的整套副本"""
        import json
        from data import memory_heat as mh
        from data import memory_store as ms
        from data import memory_index as mi
        import paths

        memory_md = tmp_path / "memory.md"
        meta_json = tmp_path / "meta.json"
        index_md = tmp_path / "index.md"
        keywords_json = tmp_path / "keywords.json"
        heat_json = tmp_path / "heat.json"
        memory_md.write_text(
            "<!-- STM 记忆条目正文 -->\n\n"
            "## MEM-DEADBEEF\n待删除正文\n\n"
            "## MEM-CAFEBABE\n保留正文\n",
            encoding="utf-8",
        )
        admitted_at = "2026-08-01T00:00:00+08:00"
        meta_json.write_text(json.dumps({
            "MEM-DEADBEEF": {
                "id": "MEM-DEADBEEF", "title": "删", "weight": 5,
                "type": "F", "access": "public", "tags": ["测试"],
                "created_at": admitted_at, "stored_at": admitted_at,
                "decay_period_days": 30, "decay_countdown_days": 30,
            },
            "MEM-CAFEBABE": {"id": "MEM-CAFEBABE", "title": "留"},
        }, ensure_ascii=False), encoding="utf-8")
        index_md.write_text(
            "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 入库轮 |\n"
            "|------|------|------|------|---------|--------|\n"
            "| MEM-DEADBEEF | [F] | 5 | 删 | — | 00001 |\n"
            "| MEM-CAFEBABE | [A] | 1 | 留 | — | 00001 |\n",
            encoding="utf-8",
        )
        keywords_json.write_text(json.dumps({
            "_comment": "倒排索引（关键词→条目ID）",
            "index": {"测试": ["MEM-DEADBEEF", "MEM-CAFEBABE"]},
        }, ensure_ascii=False), encoding="utf-8")
        heat_json.write_text(json.dumps({
            "entries": {
                "MEM-DEADBEEF": {"degrade": True, "compression": True},
                "MEM-CAFEBABE": {"degrade": False, "compression": False},
            }
        }, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(ms, "MEMORY_MD", str(memory_md))
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        monkeypatch.setattr(ms, "INDEX_MD", str(index_md))
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_json))
        monkeypatch.setattr(paths, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(paths, "HEAT_JSON", str(heat_json))

        rt = self._make_runtime(tmp_path)
        rt.heat = mh.MemoryHeat()
        rt.memory_store = ms.MemoryStore()
        rt.memory_index = mi.MemoryIndex()
        rt.memory_store.store_ltm_entry(
            "Full",
            "MEM-DEADBEEF",
            "已验证入库正文",
            {
                "id": "MEM-DEADBEEF",
                "type": "F",
                "weight": 5,
                "title": "已验证入库",
                "access": "public",
                "tags": ["测试"],
                "created_at": admitted_at,
                "stored_at": admitted_at,
                "decay_period_days": 30,
                "decay_countdown_days": 30,
            },
        )
        from data.memory_compression_store import MemoryCompressionManager
        MemoryCompressionManager(memory_store=rt.memory_store).settle_stm_forgetting(
            "MEM-DEADBEEF", round_num=9)

        assert "MEM-DEADBEEF" not in ms.MemoryStore().list_entries()
        assert "MEM-DEADBEEF" not in memory_md.read_text(encoding="utf-8")
        assert "MEM-DEADBEEF" not in json.loads(meta_json.read_text(encoding="utf-8"))
        assert "MEM-DEADBEEF" not in index_md.read_text(encoding="utf-8")
        assert "MEM-DEADBEEF" not in json.loads(keywords_json.read_text(encoding="utf-8"))["index"]["测试"]
        assert "MEM-DEADBEEF" not in json.loads(heat_json.read_text(encoding="utf-8"))["entries"]
        assert "MEM-CAFEBABE" in ms.MemoryStore().list_entries()

    def test_pending_stm_forgetting_target_keeps_tier_and_weight_aligned(self):
        from data.memory_store import memory_stm_forgetting_target

        assert memory_stm_forgetting_target(5) == ("Summary", 4)
        assert memory_stm_forgetting_target(4) == ("Abstract", 2)
        assert memory_stm_forgetting_target(3) == ("Abstract", 2)
        assert memory_stm_forgetting_target(2) == ("Abstract", 2)
        assert memory_stm_forgetting_target(1) == ("Abstract", 1)
        with pytest.raises(ValueError, match="invalid_memory_weight"):
            memory_stm_forgetting_target(True)

    def test_process_forgetting_archives_unstored_f_to_summary(self, tmp_path, monkeypatch):
        """未入库 F 级 STM 遗忘时走 F→S，不应直接落到 Abstract"""
        import json
        from data import memory_heat as mh
        from data import memory_store as ms
        from data import memory_index as mi
        import paths

        memory_md = tmp_path / "memory.md"
        meta_json = tmp_path / "meta.json"
        index_md = tmp_path / "index.md"
        keywords_json = tmp_path / "keywords.json"
        heat_json = tmp_path / "heat.json"
        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        summary_dir.mkdir(parents=True)
        abstract_dir.mkdir(parents=True)
        summary_md = summary_dir / "summary.md"
        summary_index = summary_dir / "index.md"
        summary_meta = summary_dir / "meta.json"
        abstract_md = abstract_dir / "abstract.md"
        abstract_index = abstract_dir / "index.md"
        abstract_meta = abstract_dir / "meta.json"
        summary_meta.write_text("{}", encoding="utf-8")
        abstract_meta.write_text("{}", encoding="utf-8")

        f_id = "MEM-F0000001"
        s_id = "MEM-50000001"
        memory_md.write_text(
            "<!-- STM 记忆条目正文 -->\n\n"
            f"## {f_id}\nF 原文\n\n"
            f"## {s_id}\nS 原文\n",
            encoding="utf-8",
        )
        created_at = "2026-08-01T00:00:00+08:00"
        meta_json.write_text(json.dumps({
            f_id: {
                "id": f_id, "title": "F条目", "weight": 5, "type": "F",
                "access": "public", "tags": ["测试"], "created_at": created_at,
                "stored_at": "", "decay_period_days": 30,
                "decay_countdown_days": 30,
            },
            s_id: {
                "id": s_id, "title": "S条目", "weight": 3, "type": "S",
                "access": "public", "tags": ["测试"], "created_at": created_at,
                "stored_at": "", "decay_period_days": 30,
                "decay_countdown_days": 30,
            },
        }, ensure_ascii=False), encoding="utf-8")
        index_md.write_text(
            "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 入库轮 |\n"
            "|------|------|------|------|---------|--------|\n"
            f"| {f_id} | [F] | 5 | F条目 | — | 00001 |\n"
            f"| {s_id} | [S] | 3 | S条目 | — | 00001 |\n",
            encoding="utf-8",
        )
        keywords_json.write_text(json.dumps({
            "_comment": "倒排索引（关键词→条目ID）",
            "index": {"测试": [f_id, s_id]},
        }, ensure_ascii=False), encoding="utf-8")
        heat_json.write_text(json.dumps({
            "entries": {
                f_id: {"degrade": True, "compression": True},
                s_id: {"degrade": True, "compression": True},
            }
        }, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(ms, "MEMORY_MD", str(memory_md))
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        monkeypatch.setattr(ms, "INDEX_MD", str(index_md))
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_json))
        monkeypatch.setattr(paths, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(paths, "HEAT_JSON", str(heat_json))
        monkeypatch.setattr(paths, "LTM_SUMMARY_DIR", str(summary_dir))
        monkeypatch.setattr(paths, "LTM_SUMMARY_SUMMARY_MD", str(summary_md))
        monkeypatch.setattr(paths, "LTM_SUMMARY_INDEX_MD", str(summary_index))
        monkeypatch.setattr(paths, "LTM_SUMMARY_META_JSON", str(summary_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_DIR", str(abstract_dir))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_ABSTRACT_MD", str(abstract_md))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_INDEX_MD", str(abstract_index))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(abstract_meta))

        rt = self._make_runtime(tmp_path)
        rt.heat = mh.MemoryHeat()
        rt.memory_store = ms.MemoryStore()
        rt.memory_index = mi.MemoryIndex()
        stm_meta = json.loads(meta_json.read_text(encoding="utf-8"))
        rt.memory_store.store_ltm_entry(
            "Full", f_id, f"## {f_id}\n**内容**：F 原文", stm_meta[f_id])
        rt.memory_store.store_ltm_entry(
            "Summary", s_id, f"## {s_id}\n**摘要**：S 原文", stm_meta[s_id])
        from data.memory_compression_store import MemoryCompressionManager
        manager = MemoryCompressionManager(memory_store=rt.memory_store)
        manager.settle_stm_forgetting(f_id, round_num=8)
        manager.settle_stm_forgetting(s_id, round_num=8)

        assert f_id not in memory_md.read_text(encoding="utf-8")
        assert s_id not in memory_md.read_text(encoding="utf-8")
        assert f_id not in json.loads(meta_json.read_text(encoding="utf-8"))
        assert s_id not in json.loads(meta_json.read_text(encoding="utf-8"))
        assert f_id not in json.loads(heat_json.read_text(encoding="utf-8"))["entries"]
        assert s_id not in json.loads(heat_json.read_text(encoding="utf-8"))["entries"]
        assert s_id in summary_md.read_text(encoding="utf-8")
        assert f_id not in summary_md.read_text(encoding="utf-8")
        assert not abstract_md.exists()

        manager.prepare_daily_cycle(local_date="2026-08-17", round_num=9)
        manager.apply_batch([
            {"mem_id": f_id, "semantic_content": "F 压缩为 Summary。",
             "retained_keywords": ["测试"]},
            {"mem_id": s_id, "semantic_content": "S 压缩为 Abstract。",
             "retained_keywords": ["测试"]},
        ], round_num=9)

        assert f_id in summary_md.read_text(encoding="utf-8")
        assert "F 压缩为 Summary。" in summary_md.read_text(encoding="utf-8")
        assert f_id in summary_index.read_text(encoding="utf-8")
        assert f_id not in abstract_md.read_text(encoding="utf-8")
        assert s_id in abstract_md.read_text(encoding="utf-8")
        assert "S 压缩为 Abstract。" in abstract_md.read_text(encoding="utf-8")
        assert s_id in abstract_index.read_text(encoding="utf-8")
        summary_entry = json.loads(summary_meta.read_text(encoding="utf-8"))[f_id]
        abstract_entry = json.loads(abstract_meta.read_text(encoding="utf-8"))[s_id]
        assert summary_entry["type"] == "S"
        assert summary_entry["weight"] == 4
        assert abstract_entry["type"] == "A"
        assert abstract_entry["weight"] == 2
        assert f_id not in memory_md.read_text(encoding="utf-8")
        assert s_id not in memory_md.read_text(encoding="utf-8")

    def test_process_forgetting_uses_ltm_truth_when_stm_semantic_differs(self, tmp_path, monkeypatch):
        """Spec746 后 LTM 是语义真源，待压缩项不得采信不同的 STM 副本。"""
        import json
        from data import memory_heat as mh
        from data import memory_store as ms
        from data import memory_index as mi
        import paths

        memory_md = tmp_path / "memory.md"
        meta_json = tmp_path / "meta.json"
        index_md = tmp_path / "index.md"
        keywords_json = tmp_path / "keywords.json"
        heat_json = tmp_path / "heat.json"
        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        summary_dir.mkdir(parents=True)
        abstract_dir.mkdir(parents=True)
        summary_md = summary_dir / "summary.md"
        summary_index = summary_dir / "index.md"
        summary_meta = summary_dir / "meta.json"
        abstract_md = abstract_dir / "abstract.md"
        abstract_index = abstract_dir / "index.md"
        abstract_meta = abstract_dir / "meta.json"

        mem_id = "MEM-60000001"
        memory_md.write_text(f"## {mem_id}\nSTM 副本\n", encoding="utf-8")
        meta_json.write_text(json.dumps({
            mem_id: {"id": mem_id, "title": "STM副本", "weight": 3, "type": "S"},
        }, ensure_ascii=False), encoding="utf-8")
        index_md.write_text(f"| 编号 | 类型 | 权重 | 标题 | 交互对象 | 入库轮 |\n|------|------|------|------|---------|--------|\n| {mem_id} | [S] | 3 | STM副本 | — | 00001 |\n", encoding="utf-8")
        keywords_json.write_text(json.dumps({"_comment": "kw", "index": {"副本": [mem_id]}}, ensure_ascii=False), encoding="utf-8")
        heat_json.write_text(json.dumps({"entries": {
            mem_id: {"degrade": True, "stored": False, "compression": True},
        }}, ensure_ascii=False), encoding="utf-8")
        summary_md.write_text(f"## {mem_id}  [S]  权重3\n已归档 Summary\n", encoding="utf-8")
        summary_index.write_text(f"| 编号 | 类型 | 权重 | 标题 | 交互对象 | 最后调用轮 | 锁定 |\n|------|------|------|------|---------|-----------|------|\n| {mem_id} | [S] | 3 | 已归档 | — | 00001 | 否 |\n", encoding="utf-8")
        summary_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id, "title": "已归档", "weight": 3,
                "type": "S", "tags": ["归档"],
            },
        }, ensure_ascii=False), encoding="utf-8")
        abstract_md.write_text("", encoding="utf-8")
        abstract_index.write_text("", encoding="utf-8")
        abstract_meta.write_text("{}", encoding="utf-8")

        monkeypatch.setattr(ms, "MEMORY_MD", str(memory_md))
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        monkeypatch.setattr(ms, "INDEX_MD", str(index_md))
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_json))
        monkeypatch.setattr(paths, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(paths, "HEAT_JSON", str(heat_json))
        monkeypatch.setattr(paths, "LTM_SUMMARY_DIR", str(summary_dir))
        monkeypatch.setattr(paths, "LTM_SUMMARY_SUMMARY_MD", str(summary_md))
        monkeypatch.setattr(paths, "LTM_SUMMARY_INDEX_MD", str(summary_index))
        monkeypatch.setattr(paths, "LTM_SUMMARY_META_JSON", str(summary_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_DIR", str(abstract_dir))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_ABSTRACT_MD", str(abstract_md))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_INDEX_MD", str(abstract_index))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(abstract_meta))

        rt = self._make_runtime(tmp_path)
        rt.heat = mh.MemoryHeat()
        rt.memory_store = ms.MemoryStore()
        rt.memory_index = mi.MemoryIndex()
        from data.memory_compression_store import MemoryCompressionManager
        MemoryCompressionManager(memory_store=rt.memory_store).settle_stm_forgetting(
            mem_id, round_num=9)

        assert mem_id not in memory_md.read_text(encoding="utf-8")
        assert mem_id not in json.loads(meta_json.read_text(encoding="utf-8"))
        assert "压缩不应写入" not in abstract_md.read_text(encoding="utf-8")
        assert mem_id in summary_md.read_text(encoding="utf-8")

    def test_ltm_degradation_removes_source_body_index_and_meta(self, tmp_path, monkeypatch):
        import json
        import paths

        rt = self._make_runtime(tmp_path)
        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        summary_dir.mkdir(parents=True)
        abstract_dir.mkdir(parents=True)
        summary_md = summary_dir / "summary.md"
        summary_index = summary_dir / "index.md"
        summary_meta = summary_dir / "meta.json"
        abstract_md = abstract_dir / "abstract.md"
        abstract_index = abstract_dir / "index.md"
        abstract_meta = abstract_dir / "meta.json"

        summary_md.write_text(
            "## MEM-ABCDEF12  [S]  权重5\n旧摘要\n\n## MEM-00000001  [S]  权重3\n保留\n",
            encoding="utf-8",
        )
        summary_index.write_text(
            "| ID | 形态 | 权重 | 标题 | 交互对象 | 最后调用轮 | pinned |\n"
            "|----|------|------|------|----------|------------|--------|\n"
            "| MEM-ABCDEF12 | [S] | 5 | 待降格 | Codex | 00009 | 否 |\n"
            "| MEM-00000001 | [S] | 3 | 保留 | Codex | 00009 | 否 |\n",
            encoding="utf-8",
        )
        summary_meta.write_text(json.dumps({
            "MEM-ABCDEF12": {
                "id": "MEM-ABCDEF12",
                "title": "待降格",
                "weight": 5,
                "tags": ["测试"],
                "decay_period_days": 30,
                "decay_countdown_days": 0,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
            "MEM-00000001": {
                "id": "MEM-00000001",
                "title": "保留",
                "weight": 3,
                "decay_period_days": 30,
                "decay_countdown_days": 3,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
        }, ensure_ascii=False), encoding="utf-8")
        abstract_md.write_text("", encoding="utf-8")
        abstract_index.write_text(
            "| ID | 形态 | 权重 | 标题 | 交互对象 | 最后调用轮 | pinned |\n"
            "|----|------|------|------|----------|------------|--------|\n",
            encoding="utf-8",
        )
        abstract_meta.write_text("{}", encoding="utf-8")

        monkeypatch.setattr(paths, "LTM_SUMMARY_DIR", str(summary_dir))
        monkeypatch.setattr(paths, "LTM_SUMMARY_SUMMARY_MD", str(summary_md))
        monkeypatch.setattr(paths, "LTM_SUMMARY_META_JSON", str(summary_meta))
        monkeypatch.setattr(paths, "LTM_SUMMARY_INDEX_MD", str(summary_index))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_DIR", str(abstract_dir))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_ABSTRACT_MD", str(abstract_md))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(abstract_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_INDEX_MD", str(abstract_index))

        from data.memory_compression_store import MemoryCompressionManager
        manager = MemoryCompressionManager(memory_store=rt.memory_store)
        manager.prepare_daily_cycle(local_date="2026-08-17", round_num=10)
        manager.apply_batch([{
            "mem_id": "MEM-ABCDEF12",
            "semantic_content": "降格后梗概",
            "retained_keywords": ["测试"],
        }], round_num=10)

        assert "MEM-ABCDEF12" not in summary_md.read_text(encoding="utf-8")
        assert "MEM-ABCDEF12" not in summary_index.read_text(encoding="utf-8")
        assert "MEM-ABCDEF12" not in json.loads(summary_meta.read_text(encoding="utf-8"))
        assert "MEM-ABCDEF12" in abstract_md.read_text(encoding="utf-8")
        assert "MEM-ABCDEF12" in abstract_index.read_text(encoding="utf-8")
        abstract_entry = json.loads(abstract_meta.read_text(encoding="utf-8"))["MEM-ABCDEF12"]
        assert abstract_entry["weight"] == 5
        assert abstract_entry["decay_period_days"] == 30
        assert abstract_entry["decay_countdown_days"] == 30

    def test_spec216_ltm_degradation_moves_public_abstract_to_backup(self, tmp_path, monkeypatch):
        import json
        import paths

        rt = self._make_runtime(tmp_path)
        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        backup_dir = tmp_path / "LTM" / "Memory" / "Backup"
        abstract_dir.mkdir(parents=True)
        backup_dir.mkdir(parents=True)
        abstract_md = abstract_dir / "abstract.md"
        abstract_index = abstract_dir / "index.md"
        abstract_meta = abstract_dir / "meta.json"
        backup_md = backup_dir / "backup.md"
        backup_index = backup_dir / "index.md"
        backup_meta = backup_dir / "meta.json"
        ltm_keywords = tmp_path / "ltm_keywords.json"

        mem_id = "MEM-ABCD0001"
        abstract_md.write_text(
            f"## {mem_id}  [A]  权重1\n今天拿了一个普通快递。\n",
            encoding="utf-8",
        )
        abstract_index.write_text(
            "| ID | 形态 | 权重 | 标题 | 交互对象 | 最后调用轮 | pinned |\n"
            "|----|------|------|------|----------|------------|--------|\n"
            f"| {mem_id} | [A] | 1 | 日常快递 | TzPz | 00009 | 否 |\n",
            encoding="utf-8",
        )
        abstract_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id,
                "title": "日常快递",
                "weight": 1,
                "type": "A",
                "subject": "TzPz",
                "tags": ["日常"],
                "access": "public",
                "decay_period_days": 30,
                "decay_countdown_days": 0,
                "stored_at": "2026-08-01T00:00:00+08:00",
            },
        }, ensure_ascii=False), encoding="utf-8")
        backup_md.write_text("", encoding="utf-8")
        backup_index.write_text("<!-- Backup 记忆索引 -->\n", encoding="utf-8")
        backup_meta.write_text("{}", encoding="utf-8")
        ltm_keywords.write_text(json.dumps({
            "_comment": "LTM 倒排索引",
            "index": {"日常": [f"{mem_id}[A]"]},
        }, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(paths, "LTM_ABSTRACT_DIR", str(abstract_dir))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_ABSTRACT_MD", str(abstract_md))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(abstract_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_INDEX_MD", str(abstract_index))
        monkeypatch.setattr(paths, "LTM_BACKUP_DIR", str(backup_dir))
        monkeypatch.setattr(paths, "LTM_BACKUP_BACKUP_MD", str(backup_md))
        monkeypatch.setattr(paths, "LTM_BACKUP_INDEX_MD", str(backup_index), raising=False)
        monkeypatch.setattr(paths, "LTM_BACKUP_META_JSON", str(backup_meta))
        monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(ltm_keywords))

        from data.memory_compression_store import MemoryCompressionManager
        MemoryCompressionManager(memory_store=rt.memory_store).prepare_daily_cycle(
            local_date="2026-08-17", round_num=12)

        assert mem_id not in abstract_md.read_text(encoding="utf-8")
        assert mem_id not in abstract_index.read_text(encoding="utf-8")
        assert mem_id not in json.loads(abstract_meta.read_text(encoding="utf-8"))
        assert mem_id in backup_md.read_text(encoding="utf-8")
        assert mem_id in backup_index.read_text(encoding="utf-8")
        backup_entry = json.loads(backup_meta.read_text(encoding="utf-8"))[mem_id]
        assert backup_entry["type"] == "A"
        assert backup_entry["weight"] == 1
        assert backup_entry["title"] == "日常快递"
        assert backup_entry["decay_period_days"] == 0
        assert backup_entry["decay_countdown_days"] == 0
        assert "日常" not in json.loads(
            ltm_keywords.read_text(encoding="utf-8"))["index"]
