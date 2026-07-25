import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


class TestRuntimeMemoryLifecycle(RuntimeTestMixin):
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
                        "stored": True,
                        "compression": False,
                    },
                }}

        monkeypatch.setattr(
            MemoryStore,
            "get_meta",
            lambda _self, mem_id: {
                "id": mem_id,
                "access": "private" if mem_id == private_id else "public",
            },
        )
        cleanup = CleanupPipeline(SimpleNamespace(
            heat=Heat(),
            memory_store=MemoryStore(),
        ))
        removed = []
        cleanup._remove_stm_copy = lambda mem_id, _store=None: removed.append(mem_id)

        assert cleanup._build_forgetting_context() == ""
        cleanup._process_forgetting_result({"response": ""}, 1)

        assert removed == [public_id]

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
            },
            due: {
                "id": due,
                "title": "高权重已到期",
                "weight": 5,
                "type": "S",
                "decay_period_days": 30,
                "decay_countdown_days": 0,
            },
        }, ensure_ascii=False), encoding="utf-8")

        self._patch_ltm_tier(
            monkeypatch, paths, "Summary", summary_dir,
            summary_md, summary_meta, summary_index)

        context = LTMDegradationManager().build_compression_context()

        assert due in context
        assert low_not_due not in context

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
            },
            "MEM-ABC10002": {
                "id": "MEM-ABC10002",
                "title": "已经到期",
                "weight": 5,
                "type": "F",
                "decay_period_days": 30,
                "decay_countdown_days": 0,
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

        LTMDegradationManager().apply_compression_results([], round_num=13)

        assert mem_id in summary_md.read_text(encoding="utf-8")
        assert json.loads(summary_meta.read_text(encoding="utf-8"))[mem_id]["decay_countdown_days"] == 0
        assert mem_id not in abstract_md.read_text(encoding="utf-8")

    def test_strip_memory_heading_removes_existing_heading(self):
        from engines.cleanup_helpers import strip_memory_heading

        text = "## MEM-0ABCDEF0  [F]\n**标题**：测试\n我确认这是一条正文。"

        assert strip_memory_heading(text) == "**标题**：测试\n我确认这是一条正文。"
        assert strip_memory_heading("我确认这是一条正文。") == "我确认这是一条正文。"

    def test_memory_lifecycle_ltm_write_does_not_duplicate_heading(self, tmp_path, monkeypatch):
        """Spec 024 验收 10：LTM 写端不会重复写入 ## MEM 标题"""

        abstract_dir = tmp_path / "LTM" / "Memory" / "Abstract"
        abstract_dir.mkdir(parents=True, exist_ok=True)
        abstract_md = abstract_dir / "abstract.md"
        abstract_meta = abstract_dir / "meta.json"
        abstract_index = abstract_dir / "index.md"
        abstract_meta.write_text("{}", encoding="utf-8")

        full_dir = tmp_path / "LTM" / "Memory" / "Full"
        full_dir.mkdir(parents=True, exist_ok=True)
        full_md = full_dir / "full.md"
        full_meta = full_dir / "meta.json"
        full_index = full_dir / "index.md"
        full_meta.write_text("{}", encoding="utf-8")

        import paths
        monkeypatch.setattr(paths, "LTM_ABSTRACT_DIR", str(abstract_dir))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_ABSTRACT_MD", str(abstract_md))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(abstract_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_INDEX_MD", str(abstract_index))
        monkeypatch.setattr(paths, "LTM_FULL_DIR", str(full_dir))
        monkeypatch.setattr(paths, "LTM_FULL_FULL_MD", str(full_md))
        monkeypatch.setattr(paths, "LTM_FULL_META_JSON", str(full_meta))
        monkeypatch.setattr(paths, "LTM_FULL_INDEX_MD", str(full_index))

        rt = self._make_runtime(tmp_path)

        # 模拟 MemoryStore
        class _FakeMS:
            def load_meta(self):
                return {}
        fake_ms = _FakeMS()

        # 输入文本已含 ## MEM-... 标题
        text_with_heading = "## MEM-0ABCDEF0  [F]  权重5\n**交互对象**：TzPz\n**标题**：测试\n**梗概**：正文内容。"
        rt._archive_to_abstract("MEM-0ABCDEF0", text_with_heading, 100, fake_ms)

        content = abstract_md.read_text(encoding="utf-8")
        headings = [line for line in content.splitlines()
                    if line.strip().startswith("## MEM-")]
        assert len(headings) == 1, f"Abstract 层出现 {len(headings)} 个标题，预期 1 个：\n{content}"

        # 验证 LTM Full 升格路径
        text_with_heading2 = "## MEM-0BBBBB01  [S]  权重3\n**交互对象**：FMA\n**标题**：Full 测试\n**梗概**：Full 正文。"
        # 这里只验证 _archive_to_abstract 不重复写 heading
        rt._archive_to_abstract("MEM-0BBBBB01", text_with_heading2, 200, fake_ms)
        content2 = abstract_md.read_text(encoding="utf-8")
        headings2 = [line for line in content2.splitlines()
                     if line.strip().startswith("## MEM-0BBBBB01")]
        assert len(headings2) == 1, f"Abstract 层 MEM-0BBBBB01 出现 {len(headings2)} 个标题：\n{content2}"

    def test_process_forgetting_removes_stored_stm_copy_completely(self, tmp_path, monkeypatch):
        """已入库遗忘分支必须删除 STM 正文、meta、index、keywords、heat 的整套副本"""
        import json
        from data import memory_heat as mh
        from data import memory_store as ms
        from data import memory_index as mi

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
        meta_json.write_text(json.dumps({
            "MEM-DEADBEEF": {"id": "MEM-DEADBEEF", "title": "删"},
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
                "MEM-DEADBEEF": {"degrade": True, "stored": True, "compression": True},
                "MEM-CAFEBABE": {"degrade": False, "stored": True, "compression": False},
            }
        }, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(ms, "MEMORY_MD", str(memory_md))
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        monkeypatch.setattr(ms, "INDEX_MD", str(index_md))
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_json))

        rt = self._make_runtime(tmp_path)
        rt.heat = mh.MemoryHeat()
        rt.memory_store = ms.MemoryStore()
        rt.memory_index = mi.MemoryIndex()
        rt._process_forgetting_result({"response": ""}, 9)

        assert "MEM-DEADBEEF" not in ms.MemoryStore().list_entries()
        assert "MEM-DEADBEEF" not in memory_md.read_text(encoding="utf-8")
        assert "MEM-DEADBEEF" not in json.loads(meta_json.read_text(encoding="utf-8"))
        assert "MEM-DEADBEEF" not in index_md.read_text(encoding="utf-8")
        assert "MEM-DEADBEEF" not in json.loads(keywords_json.read_text(encoding="utf-8"))["index"]["测试"]
        assert "MEM-DEADBEEF" not in json.loads(heat_json.read_text(encoding="utf-8"))["entries"]
        assert "MEM-CAFEBABE" in ms.MemoryStore().list_entries()

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
        meta_json.write_text(json.dumps({
            f_id: {"id": f_id, "title": "F条目", "weight": 5, "type": "F"},
            s_id: {"id": s_id, "title": "S条目", "weight": 3, "type": "S"},
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
                f_id: {"degrade": True, "stored": False, "compression": True},
                s_id: {"degrade": True, "stored": False, "compression": True},
            }
        }, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(ms, "MEMORY_MD", str(memory_md))
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        monkeypatch.setattr(ms, "INDEX_MD", str(index_md))
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_json))
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
        rt._process_forgetting_result({
            "response": (
                f"<!-- FORGET:{f_id} -->\nF 压缩为 Summary。\n<!-- /FORGET -->\n"
                f"<!-- FORGET:{s_id} -->\nS 压缩为 Abstract。\n<!-- /FORGET -->"
            )
        }, 9)

        assert f_id in summary_md.read_text(encoding="utf-8")
        assert f_id in summary_index.read_text(encoding="utf-8")
        assert f_id not in abstract_md.read_text(encoding="utf-8")
        assert s_id in abstract_md.read_text(encoding="utf-8")
        assert s_id in abstract_index.read_text(encoding="utf-8")
        summary_entry = json.loads(summary_meta.read_text(encoding="utf-8"))[f_id]
        abstract_entry = json.loads(abstract_meta.read_text(encoding="utf-8"))[s_id]
        assert summary_entry["type"] == "S"
        assert summary_entry["weight"] == 5
        assert abstract_entry["type"] == "A"
        assert abstract_entry["weight"] == 3
        assert f_id not in memory_md.read_text(encoding="utf-8")
        assert s_id not in memory_md.read_text(encoding="utf-8")

    def test_process_forgetting_deletes_stm_when_same_id_already_in_ltm(self, tmp_path, monkeypatch):
        """LTM 任意层同编号已归档时，STM 遗忘只删 STM 副本，不再写新层。"""
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
            mem_id: {"id": mem_id, "title": "已归档", "weight": 3, "type": "S"},
        }, ensure_ascii=False), encoding="utf-8")
        abstract_md.write_text("", encoding="utf-8")
        abstract_index.write_text("", encoding="utf-8")
        abstract_meta.write_text("{}", encoding="utf-8")

        monkeypatch.setattr(ms, "MEMORY_MD", str(memory_md))
        monkeypatch.setattr(ms, "META_JSON", str(meta_json))
        monkeypatch.setattr(ms, "INDEX_MD", str(index_md))
        monkeypatch.setattr(mi, "KEYWORDS_JSON", str(keywords_json))
        monkeypatch.setattr(mh, "HEAT_JSON", str(heat_json))
        monkeypatch.setattr(paths, "LTM_SUMMARY_META_JSON", str(summary_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(abstract_meta))

        rt = self._make_runtime(tmp_path)
        rt.heat = mh.MemoryHeat()
        rt.memory_store = ms.MemoryStore()
        rt.memory_index = mi.MemoryIndex()
        rt._process_forgetting_result({
            "response": f"<!-- FORGET:{mem_id} -->\n压缩不应写入\n<!-- /FORGET -->"
        }, 9)

        assert mem_id not in memory_md.read_text(encoding="utf-8")
        assert mem_id not in json.loads(meta_json.read_text(encoding="utf-8"))
        assert "压缩不应写入" not in abstract_md.read_text(encoding="utf-8")
        assert mem_id in summary_md.read_text(encoding="utf-8")

    def test_ltm_degradation_removes_source_body_index_and_meta(self, tmp_path, monkeypatch):
        import json
        import paths
        import data.memory_index as memory_index_mod

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
                "decay_period_days": 30,
                "decay_countdown_days": 0,
            },
            "MEM-00000001": {
                "id": "MEM-00000001",
                "title": "保留",
                "weight": 3,
                "decay_period_days": 30,
                "decay_countdown_days": 3,
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
        monkeypatch.setattr(memory_index_mod, "LTM_KEYWORDS_JSON", str(tmp_path / "ltm_keywords.json"))

        rt._apply_ltm_degradation([("MEM-ABCDEF12", "降格后梗概")], round_num=10)

        assert "MEM-ABCDEF12" not in summary_md.read_text(encoding="utf-8")
        assert "MEM-ABCDEF12" not in summary_index.read_text(encoding="utf-8")
        assert "MEM-ABCDEF12" not in json.loads(summary_meta.read_text(encoding="utf-8"))
        assert "MEM-ABCDEF12" in abstract_md.read_text(encoding="utf-8")
        assert "MEM-ABCDEF12" in abstract_index.read_text(encoding="utf-8")
        abstract_entry = json.loads(abstract_meta.read_text(encoding="utf-8"))["MEM-ABCDEF12"]
        assert abstract_entry["decay_period_days"] == 30
        assert abstract_entry["decay_countdown_days"] == 30

    def test_spec216_ltm_degradation_moves_public_abstract_to_backup(self, tmp_path, monkeypatch):
        import json
        import paths
        import data.memory_index as memory_index_mod

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
        monkeypatch.setattr(memory_index_mod, "LTM_KEYWORDS_JSON", str(ltm_keywords))

        rt._prepare_ltm_degradation_for_day(round_num=12)

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
        assert f"{mem_id}[B]" in json.loads(ltm_keywords.read_text(encoding="utf-8"))["index"]["日常"]
