"""
Spec 024 persona 记忆完整性聚焦测试

测试原则：
  - 使用 tmp_path 隔离，不污染 persona/
  - 对 live persona 文件仅做只读审计验证
  - 验证审计器能检测已知异常模式
  - 验证迁移器能修复已知异常模式
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import json


# ============================================================
# 辅助工具
# ============================================================

def _make_stm_tree(stm_dir, *, blank_lines=False, missing_meta=False,
                   old_style_heading=False, nested_heading=False,
                   heat_empty=False):
    """在 tmp_path 下构建模拟 STM/memory 文件树"""
    stm_dir.mkdir(parents=True, exist_ok=True)

    mem_id = "MEM-0AAAA001"
    mem_id2 = "MEM-0AAAA002"

    # memory.md
    if old_style_heading:
        heading1 = "## MEM-00001-01"
        heading2 = "## MEM-00002-02"
    elif nested_heading:
        heading1 = f"## {mem_id}  [F]  权重5\n## {mem_id}"
        heading2 = f"## {mem_id2}  [A]  权重2"
    else:
        heading1 = f"## {mem_id}  [F]  权重5"
        heading2 = f"## {mem_id2}  [A]  权重2"

    body1 = "**交互对象**：TzPz\n**标题**：测试记忆一\n**梗概**：这是一个测试记忆。"
    body2 = "**交互对象**：FMA\n**标题**：测试记忆二\n**梗概**：这是第二个测试记忆。"

    memory_md = f"<!-- STM 记忆条目正文 -->\n\n{heading1}\n{body1}\n\n{heading2}\n{body2}\n"
    (stm_dir / "memory.md").write_text(memory_md, encoding="utf-8")

    # meta.json
    meta = {
        "_comment": "测试 meta",
        mem_id: {"id": mem_id, "type": "F", "weight": 5, "title": "测试记忆一",
                 "subject": "TzPz", "created_round": 100, "last_recalled_round": 100},
        mem_id2: {"id": mem_id2, "type": "A", "weight": 2, "title": "测试记忆二",
                  "subject": "FMA", "created_round": 101, "last_recalled_round": 101},
    }
    if missing_meta:
        del meta[mem_id2]
    (stm_dir / "meta.json").write_text(
        json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

    # index.md
    if blank_lines:
        index_lines = [
            "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 入库轮 |",
            "|------|------|------|------|---------|--------|",
            f"| {mem_id} | [F] | 5 | 测试记忆一 | TzPz | 00100 |",
            "",
            f"| {mem_id2} | [A] | 2 | 测试记忆二 | FMA | 00101 |",
        ]
    else:
        index_lines = [
            "<!-- STM 索引行 -->",
            "",
            "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 入库轮 |",
            "|------|------|------|------|---------|--------|",
            f"| {mem_id} | [F] | 5 | 测试记忆一 | TzPz | 00100 |",
            f"| {mem_id2} | [A] | 2 | 测试记忆二 | FMA | 00101 |",
        ]
    (stm_dir / "index.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")

    # keywords.json
    keywords = {"_comment": "测试 keywords", "index": {"测试": [mem_id, mem_id2]}}
    (stm_dir / "keywords.json").write_text(
        json.dumps(keywords, ensure_ascii=False, indent=2), encoding="utf-8")

    # heat.json
    if heat_empty:
        heat = {"_comment": "STM 热度值", "entries": {}}
    else:
        heat = {"_comment": "STM 热度值", "entries": {
            mem_id: {"H": 50, "zone": "未定", "AH_high": 0, "AH_low": 0,
                     "compression": True, "stored": False, "heat_locked": False},
            mem_id2: {"H": 30, "zone": "衰减", "AH_high": 0, "AH_low": 2,
                      "compression": False, "stored": False, "heat_locked": False},
        }}
    (stm_dir / "heat.json").write_text(
        json.dumps(heat, ensure_ascii=False, indent=2), encoding="utf-8")


def _make_ltm_tree(ltm_dir, *, mismatched_meta_id=False, duplicate_across_layers=False):
    """在 tmp_path 下构建模拟 LTM/Memory 文件树"""
    ltm_dir.mkdir(parents=True, exist_ok=True)

    for layer, md_name in [("Full", "full.md"), ("Summary", "summary.md"),
                            ("Abstract", "abstract.md")]:
        layer_dir = ltm_dir / layer
        layer_dir.mkdir(parents=True, exist_ok=True)

        if layer == "Full":
            mem_id = "MEM-0BBBB001"
            body = "**交互对象**：TzPz\n**标题**：Full 层记忆\n**梗概**：一条 Full 层记忆。"
            meta_key = mem_id
        elif layer == "Summary":
            mem_id = "MEM-0CCCC001"
            body = "**交互对象**：FMA\n**标题**：Summary 层记忆\n**梗概**：一条 Summary 层记忆。"
            meta_key = mem_id
        else:
            mem_id = "MEM-0DDDD001"
            body = "**交互对象**：—\n**标题**：Abstract 层记忆\n**梗概**：一条 Abstract 层记忆。"
            meta_key = mem_id

        # 如果测试重复 ID 跨层
        if duplicate_across_layers and layer == "Summary":
            mem_id = "MEM-0BBBB001"  # 与 Full 相同
            body = "**交互对象**：FMA\n**标题**：重复 ID 记忆\n**梗概**：与 Full 层共享 ID。"

        md_content = f"<!-- LTM {layer} 记忆正文 -->\n\n## {mem_id}  [{layer[0]}]  权重3\n{body}\n"
        (layer_dir / md_name).write_text(md_content, encoding="utf-8")

        # meta.json
        inner_id = "MEM-DEADBEEF" if mismatched_meta_id else mem_id
        meta = {
            "_comment": f"LTM {layer} 元数据",
            meta_key: {"id": inner_id, "type": layer[0], "weight": 3,
                       "title": f"{layer} 层记忆",
                       "subject": "TzPz" if layer == "Full" else "FMA",
                       "created_round": 200, "last_recalled_round": 200},
        }
        (layer_dir / "meta.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        # index.md
        index_lines = [
            f"<!-- {layer} 记忆索引 -->",
            "",
            "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 最后调用轮 | 锁定 |",
            "|------|------|------|------|---------|-----------|------|",
            f"| {mem_id} | [{layer[0]}] | 3 | {layer} 层记忆 | TzPz | 00200 | 否 |",
        ]
        (layer_dir / "index.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")

    # keywords.json
    kw = {"_comment": "LTM keywords", "index": {}}
    (ltm_dir / "keywords.json").write_text(
        json.dumps(kw, ensure_ascii=False, indent=2), encoding="utf-8")


# ============================================================
# 审计测试（合成数据）
# ============================================================

class TestAuditSTM:
    def test_detects_blank_lines_in_index_table(self, tmp_path):
        """审计器检测到 index.md 表格空行"""
        stm_dir = tmp_path / "STM" / "memory"
        _make_stm_tree(stm_dir, blank_lines=True)

        from scripts.audit_persona_memory import blank_lines_inside_table
        blanks = blank_lines_inside_table(stm_dir / "index.md")
        assert len(blanks) > 0

    def test_no_blank_lines_in_clean_index(self, tmp_path):
        """干净索引表无空行"""
        stm_dir = tmp_path / "STM" / "memory"
        _make_stm_tree(stm_dir, blank_lines=False)

        from scripts.audit_persona_memory import blank_lines_inside_table
        blanks = blank_lines_inside_table(stm_dir / "index.md")
        assert len(blanks) == 0

    def test_detects_meta_not_body(self, tmp_path):
        """审计器检测 meta 有但 body 无的条目"""
        stm_dir = tmp_path / "STM" / "memory"
        _make_stm_tree(stm_dir, missing_meta=False)
        # 添加一个仅存在于 meta 的条目
        meta_path = stm_dir / "meta.json"
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        meta["MEM-ORPHAN01"] = {"id": "MEM-ORPHAN01", "type": "A", "weight": 1,
                                 "title": "孤儿条目", "subject": None,
                                 "created_round": 99, "last_recalled_round": 99}
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2), encoding="utf-8")

        from scripts.audit_persona_memory import audit
        report = audit(tmp_path)
        assert "MEM-ORPHAN01" in report["stm"]["meta_not_body"]

    def test_detects_empty_heat(self, tmp_path):
        """审计器检测 heat 为空"""
        stm_dir = tmp_path / "STM" / "memory"
        _make_stm_tree(stm_dir, heat_empty=True)

        from scripts.audit_persona_memory import audit
        report = audit(tmp_path)
        assert report["stm"]["heat_count"] == 0
        assert len(report["stm"]["body_not_heat"]) == 2

    def test_detects_non_mem_prefix_in_index(self, tmp_path):
        """审计器识别非 MEM- 前缀的 ID"""
        stm_dir = tmp_path / "STM" / "memory"
        _make_stm_tree(stm_dir)
        # 写入一个旧式 ID
        index_lines = (stm_dir / "index.md").read_text(encoding="utf-8").splitlines()
        index_lines.append("| MEM-00001-01 | [A] | 2 | 旧式ID | — | 00001 |")
        (stm_dir / "index.md").write_text("\n".join(index_lines) + "\n", encoding="utf-8")

        from scripts.audit_persona_memory import index_ids
        ids = index_ids(stm_dir / "index.md")
        # 旧式 ID 会被 canonical_id 转换
        assert "MEM-00001001" in ids


class TestAuditLTM:
    def test_detects_meta_id_mismatch(self, tmp_path):
        """审计器检测 LTM meta key 与内部 id 不一致"""
        ltm_dir = tmp_path / "LTM" / "Memory"
        _make_ltm_tree(ltm_dir, mismatched_meta_id=True)

        from scripts.audit_persona_memory import meta_ids
        ids, mismatches = meta_ids(ltm_dir / "Full" / "meta.json")
        assert len(mismatches) > 0

    def test_no_mismatch_in_clean_meta(self, tmp_path):
        """干净 LTM meta 无 key/id 不一致"""
        ltm_dir = tmp_path / "LTM" / "Memory"
        _make_ltm_tree(ltm_dir, mismatched_meta_id=False)

        from scripts.audit_persona_memory import meta_ids
        ids, mismatches = meta_ids(ltm_dir / "Full" / "meta.json")
        assert len(mismatches) == 0

    def test_detects_multi_layer_duplicates(self, tmp_path):
        """审计器检测同一 ID 跨多层"""
        ltm_dir = tmp_path / "LTM" / "Memory"
        _make_ltm_tree(ltm_dir, duplicate_across_layers=True)

        from scripts.audit_persona_memory import audit
        report = audit(tmp_path)
        assert len(report["ltm_multi_layer_duplicates"]) >= 1

    def test_no_duplicates_in_clean_ltm(self, tmp_path):
        """干净 LTM 无跨层重复"""
        ltm_dir = tmp_path / "LTM" / "Memory"
        _make_ltm_tree(ltm_dir, duplicate_across_layers=False)

        from scripts.audit_persona_memory import audit
        report = audit(tmp_path)
        clean_dupes = {k: v for k, v in report["ltm_multi_layer_duplicates"].items()
                       if len(set(v) - {"Pinned", "Backup"}) > 1}
        assert len(clean_dupes) == 0

    def test_detects_bad_ltm_keyword_keys_and_dangling_refs(self, tmp_path):
        """审计器检测 LTM 全局倒排索引里的操作语句键与悬空引用"""
        ltm_dir = tmp_path / "LTM" / "Memory"
        _make_ltm_tree(ltm_dir)
        (ltm_dir / "keywords.json").write_text(
            json.dumps({
                "_comment": "LTM keywords",
                "index": {
                    "从本轮语料提取关键词：`identity_timeout`。写入倒排索引。": [
                        "MEM-0CCCC001[S]",
                    ],
                    "正常关键词": [
                        "MEM-DEADBEEF[S]",
                    ],
                },
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        from scripts.audit_persona_memory import audit
        report = audit(tmp_path)
        assert report["ltm_keywords"]["bad_keyword_keys"]
        assert "MEM-0CCCC001" in report["ltm_keywords"]["referenced_ids"]
        assert "MEM-DEADBEEF" in report["ltm_keywords"]["dangling_refs"]


class TestAuditBackups:
    def test_detects_bak_files_in_live_tree(self, tmp_path):
        """审计器检测 live persona 树中的 .bak 残留"""
        # 在非 Backup 位置创建 .bak
        stm_dir = tmp_path / "STM" / "memory"
        stm_dir.mkdir(parents=True, exist_ok=True)
        (stm_dir / "meta.json.bak").write_text("{}", encoding="utf-8")

        from scripts.audit_persona_memory import scan_backups
        residuals = scan_backups(tmp_path)
        assert any("meta.json.bak" in r for r in residuals)

    def test_does_not_flag_design_backup_dir(self, tmp_path):
        """审计器不标记 LTM/Memory/Backup 设计目录"""
        backup_dir = tmp_path / "LTM" / "Memory" / "Backup"
        backup_dir.mkdir(parents=True, exist_ok=True)
        (backup_dir / "index.md").write_text("<!-- Backup 冷备 -->\n", encoding="utf-8")

        from scripts.audit_persona_memory import scan_backups
        residuals = scan_backups(tmp_path)
        assert len(residuals) == 0


class TestSpec193MemoryOverviewAndDream:
    def test_audit_reports_retired_meta_fields_and_missing_spec193_surfaces(self, tmp_path):
        """Spec 193 审计：退役字段、缺梦源/现状概况与旧梗概要显式暴露。"""
        stm_dir = tmp_path / "STM" / "memory"
        stm_dir.mkdir(parents=True, exist_ok=True)
        mem_id = "MEM-0AAAA001"
        (stm_dir / "memory.md").write_text(
            f"<!-- STM 记忆条目正文 -->\n\n"
            f"## {mem_id}  [F]  权重5\n"
            "**交互对象**：TzPz\n"
            "**标题**：旧字段测试\n"
            "**梗概**（≤128字）：用于审计退役字段。\n",
            encoding="utf-8",
        )
        (stm_dir / "meta.json").write_text(
            json.dumps({
                mem_id: {
                    "id": mem_id,
                    "type": "F",
                    "weight": 5,
                    "title": "旧字段测试",
                    "abstract": "旧摘要",
                    "locked": False,
                    "source_rounds": [1],
                    "mode": "theory",
                    "merged_from": [],
                },
            }, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (stm_dir / "index.md").write_text(
            "| 编号 | 类型 | 权重 | 标题 | 交互对象 | 入库轮 |\n"
            "|------|------|------|------|---------|--------|\n"
            f"| {mem_id} | [F] | 5 | 旧字段测试 | TzPz | 00001 |\n",
            encoding="utf-8",
        )
        (stm_dir / "keywords.json").write_text(
            json.dumps({"_comment": "kw", "index": {}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        (stm_dir / "heat.json").write_text(
            json.dumps({"_comment": "heat", "entries": {mem_id: {}}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

        from scripts.audit_persona_memory import audit

        report = audit(tmp_path)

        assert report["stm"]["retired_meta_fields"][mem_id] == [
            "abstract",
            "locked",
            "merged_from",
            "mode",
            "source_rounds",
        ]
        assert report["stm"]["meta_field_shape_mismatches"][mem_id] == [
            "id",
            "type",
            "weight",
            "title",
            "abstract",
            "locked",
            "source_rounds",
            "mode",
            "merged_from",
        ]
        assert report["stm"]["missing_body_dream"] == [mem_id]
        assert report["stm"]["missing_body_current_overview"] == [mem_id]
        assert report["stm"]["body_gist_residuals"] == [mem_id]
        assert report["stm"]["missing_index_dream"] == [mem_id]
        assert report["stm"]["missing_index_current_overview"] == [mem_id]

    def test_audit_treats_legacy_ltm_locked_column_as_missing_spec193_columns(self, tmp_path):
        """Spec 193 审计：旧 LTM 锁定列不能被当作梦源或现状概况列。"""
        ltm_dir = tmp_path / "LTM" / "Memory"
        _make_ltm_tree(ltm_dir)

        from scripts.audit_persona_memory import audit

        report = audit(tmp_path)

        assert report["ltm"]["Full"]["missing_index_dream"] == ["MEM-0BBBB001"]
        assert report["ltm"]["Full"]["missing_index_current_overview"] == ["MEM-0BBBB001"]




# ============================================================
# live persona 只读检查
# ============================================================

class TestLivePersonaIntegrity:
    """对真实 persona 文件的只读验证——Spec 024 验收标准"""

    @staticmethod
    def _persona_root():
        from pathlib import Path
        return Path(os.path.dirname(os.path.abspath(__file__)), "..", "persona").resolve()

    def test_no_backup_residuals_in_live_tree(self):
        """验收 1：live persona 树下无 .bak 或 _backup_* 残留"""
        from scripts.audit_persona_memory import scan_backups
        residuals = scan_backups(self._persona_root())
        assert residuals == [], f"发现备份残留：{residuals}"

    def test_stm_index_no_blank_lines(self):
        """验收 5：STM index.md 无表内空行"""
        from scripts.audit_persona_memory import blank_lines_inside_table
        stm_index = self._persona_root() / "STM" / "memory" / "index.md"
        blanks = blank_lines_inside_table(stm_index)
        assert blanks == [], f"STM index.md 表内空行行号：{blanks}"

    def test_stm_body_meta_index_closed(self):
        """验收 5 补充：STM body/meta/index 闭合"""
        from scripts.audit_persona_memory import audit
        report = audit(self._persona_root())
        stm = report["stm"]
        assert stm["invalid_meta_ids"] == [], f"非法 meta ID：{stm['invalid_meta_ids']}"
        assert stm["meta_not_body"] == [], f"meta 无 body 条目：{stm['meta_not_body']}"
        assert stm["body_not_meta"] == [], f"body 无 meta 条目：{stm['body_not_meta']}"
        assert stm["body_not_index"] == [], f"body 无 index：{stm['body_not_index']}"
        assert stm["body_not_heat"] == [], f"body 无 heat：{stm['body_not_heat']}"
        assert stm["heat_not_body"] == [], f"heat 无 body 条目：{stm['heat_not_body']}"

    def test_stm_heat_covers_valid_body(self):
        """验收 6：STM heat 覆盖所有有效正文条目"""
        from scripts.audit_persona_memory import audit
        report = audit(self._persona_root())
        stm = report["stm"]
        assert stm["heat_count"] == stm["body_count"], \
            f"heat 条目 {stm['heat_count']} != body 条目 {stm['body_count']}"

    def test_ltm_meta_key_matches_inner_id(self):
        """验收 7：LTM Full/Summary/Abstract meta key 与内部 id 一致"""
        from scripts.audit_persona_memory import audit
        report = audit(self._persona_root())
        for layer in ("Full", "Summary", "Abstract"):
            mismatches = report["ltm"][layer]["meta_id_mismatches"]
            assert mismatches == [], f"{layer} 层 meta id 不一致：{mismatches}"

    def test_ltm_no_unexpected_multi_layer_duplicates(self):
        """验收 9：LTM 无跨层重复 ID"""
        from scripts.audit_persona_memory import audit
        report = audit(self._persona_root())
        assert report["ltm_multi_layer_duplicates"] == {}, \
            f"跨层重复 ID：{report['ltm_multi_layer_duplicates']}"

    def test_all_stm_ids_are_mem_prefixed(self):
        """验收 4 补充：所有 STM body ID 均为 MEM- 前缀"""
        from scripts.audit_persona_memory import audit
        report = audit(self._persona_root())
        assert report["stm"]["invalid_body_ids"] == [], \
            f"非法 body ID：{report['stm']['invalid_body_ids']}"

    def test_audit_report_outputs_valid_json(self):
        """验收 2：审计器输出有效 JSON"""
        from scripts.audit_persona_memory import audit
        report = audit(self._persona_root())
        assert "stm" in report
        assert "ltm" in report
        assert "backup_residuals" in report
        json_str = json.dumps(report, ensure_ascii=False)
        assert json.loads(json_str) == report
