"""
Phase 3 上下文装配层测试

测试原则：装配器逻辑不碰真实 persona/ 数据，用 mock state。
"""
import sys
import os
import json
import re
import pytest
from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def _assert_exact_block_index(text, block_index):
    assert len({item["block_id"] for item in block_index}) == len(block_index)
    previous_end = 0
    for item in block_index:
        assert previous_end <= item["char_start"] < item["char_end"] <= len(text)
        assert text[item["char_start"]:item["char_end"]]
        previous_end = item["char_end"]


def test_spec462_rendered_corpus_entries_get_visible_short_ids_without_mutating_source():
    from assembly.context_helpers import render_corpus_entries_for_context

    entries = [
        {
            "kind": "interaction",
            "role": "user",
            "content": "请整理这段材料。",
            "round": 600,
        },
        {
            "kind": "tool_fact",
            "role": "tool",
            "content": "本轮已经成功读取文件：task.md。",
            "round": 600,
        },
    ]

    rendered = render_corpus_entries_for_context(
        entries,
        current_round=600,
        cache_source="now_cache.jsonl",
    )

    assert "语料短ID：C-00001。" in rendered[0]["content"]
    assert "语料短ID：C-00002。" in rendered[1]["content"]
    assert rendered[0]["active_corpus_id"] == "C-00001"
    assert rendered[1]["active_corpus_id"] == "C-00002"
    assert "active_corpus_id" not in entries[0]
    assert "active_corpus_id" not in entries[1]


def test_spec721_existing_short_id_advances_allocator_without_collision():
    from assembly.context_helpers import assign_active_corpus_ids

    assigned, next_index = assign_active_corpus_ids([
        {"kind": "tool_fact", "role": "tool", "content": "新语料"},
        {
            "kind": "interaction",
            "role": "user",
            "content": "旧语料",
            "active_corpus_id": "C-00374",
        },
    ])

    assert [entry["active_corpus_id"] for entry in assigned] == [
        "C-00375", "C-00374"
    ]
    assert next_index == 376


def test_spec286_corpus_headers_are_kind_specific_chinese_and_hide_audit_fields():
    from assembly.context_helpers import render_corpus_entry_for_context

    forbidden = (
        "kind=",
        "role=",
        "visible_at_round",
        "source_round",
        "cached_at",
        "step=",
        "iter=",
        "boundary=",
        "cache_source",
        "source_block_id",
        "source_block_ids",
        "far_head",
        "near_head",
        "compact_reason",
    )
    samples = [
        ({
            "kind": "interaction",
            "role": "user",
            "content": "我是 Codex。",
            "round": 500,
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }, "【本轮交互】"),
        ({
            "kind": "assistant_reply",
            "role": "assistant",
            "content": "上一轮回复。",
            "round": 499,
        }, "【历史回复，来自第 499 轮】"),
        ({
            "kind": "dialogue_progress",
            "role": "assistant",
            "content": "我会先继续读文件。",
            "round": 500,
        }, "【轮中进展记录】"),
        ({
            "kind": "runtime_call_request",
            "role": "user",
            "content": "请根据上下文继续本次调用。",
            "round": 500,
        }, "【Runtime 调用占位】"),
        ({
            "kind": "material",
            "role": "system",
            "content": "材料正文",
            "round": 500,
            "path": "task_materials/motherboard_cards.md",
        }, "【本轮资料】"),
        ({
            "kind": "tool_fact",
            "role": "tool",
            "content": "本轮已经成功读取文件：task_materials/motherboard_cards.md。",
            "round": 500,
            "tool_result": {"tool_id": "file_read", "status": "ok"},
        }, "【本轮工具事实】"),
        ({
            "kind": "setup_fact",
            "role": "system",
            "content": "起手安全裁决通过。本轮类型为 rhythm。",
            "round": 500,
        }, "【本轮起手事实】"),
        ({
            "kind": "relay_handoff",
            "role": "user",
            "content": "继续从第 164 行读取。",
            "round": 499,
        }, "【上轮交接任务，来自第 499 轮】"),
        ({
            "kind": "minimum_commitment",
            "role": "system",
            "content": "[最小承诺] R000500 / post / status=closed",
            "round": 500,
        }, "【第 500 轮已闭合】"),
        ({
            "kind": "fault_note",
            "role": "system",
            "content": "反应步缺少有效收束表单。",
            "round": 499,
            "step": "reaction",
        }, "【故障记录，来自第 499 轮】"),
        ({
            "kind": "cache_summary",
            "role": "system",
            "content": "\n".join([
                "历史工具事实摘要：R000499 轮结束语料代谢生成。",
                "这些记录只说明历史轮曾产生工具事实条，不证明当前轮已执行。",
                "- tool_fact step=reaction iter=0: [file_read ok] tool_id=file_read; path=task_materials/motherboard_cards.md; start_line=1; end_line=80; next_start_line=81; truncated=false",
            ]),
            "round": 499,
            "compact_reason": "round_retention_settlement",
        }, "【历史工具事实摘要，来自第 499 轮】"),
        ({
            "kind": "cache_summary",
            "role": "system",
            "content": "最近缓存压缩内容。",
            "round": 498,
            "compact_reason": "post_lately_trim",
        }, "【最近缓存压缩摘要】"),
    ]

    for entry, expected_title in samples:
        rendered = render_corpus_entry_for_context(
            {
                **entry,
                "timestamp": "2026-06-13T10:00:00+08:00",
                "source_block_id": "blk-noise",
                "source_block_ids": ["blk-a", "blk-b"],
                "oldest_source_round": 490,
                "oldest_cached_at": "2026-06-13T09:00:00+08:00",
                "compacted_at": "2026-06-13T10:05:00+08:00",
            },
            current_round=500,
            cache_source="now_cache.jsonl",
        )
        text = rendered["content"]
        assert expected_title in text
        assert "语料时间：2026-06-13T10:00:00+08:00。" in text
        if entry["kind"] == "assistant_reply":
            assert rendered["role"] == "system"
        if entry["kind"] == "cache_summary" and entry.get("compact_reason") == "round_retention_settlement":
            assert "历史上曾经成功读取文件：task_materials/motherboard_cards.md。" in text
            assert "历史读取游标：上次读到第 80 行；续读游标第 81 行。" in text
            assert "如需继续" not in text
            assert "[file_read ok]" not in text
            assert "tool_id=file_read" not in text
            assert "truncated=false" not in text
        if entry["kind"] == "relay_handoff":
            assert rendered["role"] == "user"
            assert "不是用户原始输入" in text
        for token in forbidden:
            assert token not in text


def test_spec404_dialogue_progress_visible_and_fake_state_kinds_retired(tmp_path):
    from assembly.context import ContextAssembler

    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))

    assert assembler._is_context_visible_entry({
        "kind": "dialogue_progress",
        "role": "assistant",
        "content": "进展事件原文按助手对话转录可见。",
    }) is True
    assert assembler._is_context_visible_entry({
        "kind": "progress_meta",
        "role": "system",
        "content": "本轮已播报进展 1 次。",
    }) is False
    assert assembler._is_context_visible_entry({
        "kind": "current_action_state",
        "role": "system",
        "content": "当前行动状态。",
    }) is False


def test_spec521_dialogue_progress_commitment_is_downranked():
    from assembly.context_helpers import render_corpus_entry_for_context

    rendered = render_corpus_entry_for_context(
        {
            "kind": "dialogue_progress",
            "role": "assistant",
            "content": "我会先通过 guide_submit 批量更新状态，同时开始写入产物文件。",
            "round": 521,
        },
        current_round=522,
        cache_source="now_cache.jsonl",
    )

    text = rendered["content"]
    assert "计划性进展承诺已降噪" in text
    assert "不证明文件、命令或清单已经完成" in text
    assert "同时开始写入产物文件" not in text


def test_spec524_dialogue_progress_folds_after_one_call_and_expands_once():
    from assembly.context_helpers import render_corpus_entries_for_context

    entries = [
        {
            "kind": "dialogue_progress",
            "role": "assistant",
            "content": "FIRST_PROGRESS_FULL_TEXT",
            "round": 524,
            "step": "reaction",
            "iter": 1,
        },
        {
            "kind": "dialogue_progress",
            "role": "assistant",
            "content": "我会先通过 guide_submit 批量更新状态，同时开始写入产物文件。",
            "round": 524,
            "step": "reaction",
            "iter": 2,
        },
    ]
    registry = {}

    rendered = render_corpus_entries_for_context(
        entries,
        current_round=524,
        cache_source="now_cache.jsonl",
        active_corpus_registry=registry,
        current_reaction_iteration=3,
    )

    first_text = rendered[0]["content"]
    second_text = rendered[1]["content"]
    assert "语料短ID：C-00001。" in first_text
    assert "FIRST_PROGRESS_FULL_TEXT" not in first_text
    assert "轮中进展正文已折叠" in first_text
    assert 'corpus_read(corpus_id="C-00001")' in first_text
    assert "同时开始写入产物文件" in second_text
    assert "计划性进展承诺已降噪" not in second_text
    assert registry["C-00001"]["kind"] == "dialogue_progress"

    expanded = render_corpus_entries_for_context(
        entries,
        current_round=524,
        cache_source="now_cache.jsonl",
        active_corpus_registry={},
        current_reaction_iteration=4,
        expand_once_entry_keys={registry["C-00001"]["entry_key"]},
    )

    assert "FIRST_PROGRESS_FULL_TEXT" in expanded[0]["content"]
    assert "同时开始写入产物文件" not in expanded[1]["content"]


def test_spec521_high_freq_contains_readonly_task_board(monkeypatch, tmp_path):
    from assembly.context import ContextAssembler
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore

    wb_root = tmp_path / "workbench"
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        task_title="12 项日常能力测试",
        task_goal="完成产物并验证脚本。",
        guide={
            "items": [
                {
                    "item_id": "item_01",
                    "title": "写 01_frameworks.md",
                    "status": "done",
                    "required": True,
                    "evidence_refs": ["output/01_frameworks.md"],
                },
                {
                    "item_id": "item_02",
                    "title": "写 02_model_pricing.md",
                    "status": "open",
                    "required": True,
                },
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "01_frameworks.md 来源可追溯",
                    "status": "passed",
                    "required": True,
                    "evidence_refs": ["EV-00001"],
                },
                {
                    "acceptance_id": "acc_02",
                    "description": "02_model_pricing.md 标明日期和单位",
                    "status": "pending",
                    "required": True,
                },
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [],
    }, active=True)
    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
    monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
    monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda **kw: "")
    monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda **kw: "")
    monkeypatch.setattr(assembler, "_build_keyword_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_association_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda *a, **kw: "")

    high_freq = assembler._build_high_freq(
        {},
        "reaction",
        "interactive",
        include_content=False,
        mount_ids=[],
    )

    assert "## 当前任务清单状态" in high_freq
    assert "只读看板，不是提交入口" in high_freq
    assert "item_01：已完成" in high_freq
    assert "acc_02：待验收" in high_freq
    assert "缺口" in high_freq
    assert high_freq.index("## 当前任务清单状态") < high_freq.index("## 反应步短工具带")
    board_section = high_freq.split("## 反应步短工具带", 1)[0]
    assert "guide_submit(guide_id=" in board_section
    assert "item_id=task_progress" in board_section
    assert "option_id=update_task_status" in board_section
    assert "不要只写 reason" in board_section
    assert "fields.items" in board_section
    assert "fields.acceptance" in board_section
    assert "task_01" in board_section
    assert "acc_01" in board_section
    assert "evidence_refs" in board_section
    assert "不要把用户原始目标改写成更小的阶段性目标" in board_section
    assert "部分完成不能登记为全部 done/passed" in board_section
    assert "done / blocked" in board_section
    assert "passed / blocked" in board_section
    assert "任务验收 checkpoint" not in board_section
    assert "option_id=done" not in board_section
    assert "required_fields" not in board_section
    assert "allowed_fields" not in board_section


def test_spec553_task_board_uses_shared_task_progress_copy(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_active_task_board
    from logic.task_progress_copy import (
        TASK_ACCEPTANCE_UPDATE_EXAMPLE,
        TASK_ITEM_UPDATE_EXAMPLE,
    )

    wb_root = tmp_path / "workbench"
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        task_title="验证共享文案",
        task_goal="确认看板使用同一份格式说明。",
        guide={
            "items": [{"item_id": "task_01", "title": "写文件", "status": "open"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "通过验收",
                    "status": "pending",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "task_id": task_id,
        "kind": "task_execution",
        "items": [],
    }, active=True)

    board = render_active_task_board(store)

    assert TASK_ITEM_UPDATE_EXAMPLE in board
    assert TASK_ACCEPTANCE_UPDATE_EXAMPLE in board
    assert "reason 不会改变账本状态" in board
    assert "不要把用户原始目标改写成更小的阶段性目标" in board
    assert "部分完成不能登记为全部 done/passed" in board


def test_spec530_task_board_marks_recent_artifact_as_pending_registration(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_active_task_board

    wb_root = tmp_path / "workbench"
    output_root = tmp_path / "DFT_AGENT_EVAL" / "output"
    output_root.mkdir(parents=True)
    artifact_path = output_root / "03_index.md"
    artifact_path.write_text("# 索引\n", encoding="utf-8")
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        task_title="12 项日常能力测试",
        task_goal="完成产物并验证脚本。",
        guide={
            "items": [
                {
                    "item_id": "item_03",
                    "title": "整理 inbox 并输出 03_index.md",
                    "status": "open",
                    "required": True,
                },
                {
                    "item_id": "item_04",
                    "title": "写 04_sales_report.py",
                    "status": "open",
                    "required": True,
                },
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_03",
                    "description": "03_index.md 已写入 output 目录",
                    "status": "pending",
                    "required": True,
                },
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [],
    }, active=True)

    recent_entries = [{
        "kind": "tool_fact",
        "tool_result": {
            "tool_id": "file_write",
            "status": "ok",
            "call_id": "call_write_03_index",
            "path": str(artifact_path),
            "evidence_refs": [f"file_write:{artifact_path}"],
        },
    }]

    text = render_active_task_board(
        store,
        recent_context_entries=recent_entries,
    )

    assert "item_03：已产出，待登记" in text
    assert "03_index.md" in text
    assert str(artifact_path) not in text
    assert "item_04：待办" in text
    assert "任务项 item_03 仍为 open" not in text
    assert "后续 checkpoint 登记" in text


def test_spec530_assembler_passes_recent_tool_facts_to_task_board(
        monkeypatch, tmp_path):
    from assembly.context import ContextAssembler
    from data import workbench as workbench_module
    from data.context_store import ContextStore
    from data.workbench import WorkbenchStore

    wb_root = tmp_path / "workbench"
    cache_root = tmp_path / "cache"
    output_root = tmp_path / "DFT_AGENT_EVAL" / "output"
    output_root.mkdir(parents=True)
    artifact_path = output_root / "12_fix_report.md"
    artifact_path.write_text("# 修正报告\n", encoding="utf-8")
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        task_title="回看产物并修正",
        task_goal="输出 12_fix_report.md。",
        guide={
            "items": [{
                "item_id": "item_12",
                "title": "输出 12_fix_report.md",
                "status": "open",
                "required": True,
            }],
            "acceptance": [],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [],
    }, active=True)
    context_store = ContextStore(
        cache_dir=str(cache_root),
        now_cache_jsonl=str(cache_root / "now_cache.jsonl"),
        lately_cache_jsonl=str(cache_root / "lately_cache.jsonl"),
    )
    context_store.append_to_cache(
        530,
        "system",
        f"本轮已经写入文件：{artifact_path}。",
        kind="tool_fact",
        step="reaction",
        tool_result={
            "tool_id": "file_write",
            "status": "ok",
            "call_id": "call_write_12",
            "path": str(artifact_path),
            "evidence_refs": [f"file_write:{artifact_path}"],
        },
    )
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        context_store=context_store,
    )
    monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
    monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda **kw: "")
    monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda **kw: "")
    monkeypatch.setattr(assembler, "_build_keyword_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_association_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *a, **kw: "")
    monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda *a, **kw: "")

    high_freq = assembler._build_high_freq(
        {},
        "reaction",
        "interactive",
        include_content=False,
        mount_ids=[],
    )

    assert "item_12：已产出，待登记" in high_freq
    assert "12_fix_report.md" in high_freq


def test_spec531_task_board_matches_web_fetch_source_evidence(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_active_task_board

    wb_root = tmp_path / "workbench"
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        task_title="来源抓取",
        task_goal="抓取官方来源正文。",
        guide={
            "items": [],
            "acceptance": [{
                "acceptance_id": "acc_source",
                "description": "OpenAI 定价页 https://openai.com/api/pricing 已抓取正文",
                "status": "pending",
                "required": True,
            }],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [],
    }, active=True)

    text = render_active_task_board(store, recent_context_entries=[{
        "kind": "tool_fact",
        "tool_result": {
            "tool_id": "web_fetch",
            "status": "ok",
            "source_url": "https://openai.com/api/pricing",
            "evidence_refs": ["web_fetch:https://openai.com/api/pricing"],
        },
    }])

    assert "acc_source：有证据，待登记" in text
    assert "https://openai.com/api/pricing" in text
    assert "验收项 acc_source 已有证据但账本未登记" in text


def test_spec531_task_board_matches_shell_command_evidence(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_active_task_board

    wb_root = tmp_path / "workbench"
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        task_title="脚本验证",
        task_goal="运行报告脚本。",
        guide={
            "items": [],
            "acceptance": [{
                "acceptance_id": "acc_run",
                "description": "python output\\04_sales_report.py 已运行通过",
                "status": "pending",
                "required": True,
            }],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [],
    }, active=True)

    text = render_active_task_board(store, recent_context_entries=[{
        "kind": "tool_fact",
        "tool_result": {
            "tool_id": "shell_command",
            "status": "ok",
            "exit_code": 0,
            "command": "python output\\04_sales_report.py",
            "evidence_refs": ["shell_command:D:/eval"],
        },
    }])

    assert "acc_run：有证据，待登记" in text
    assert "python output/04_sales_report.py" in text
    assert "验收项 acc_run 已有证据但账本未登记" in text


def test_spec531_task_board_reads_protocol_receipts(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_active_task_board

    wb_root = tmp_path / "workbench"
    output_root = tmp_path / "DFT_AGENT_EVAL" / "output"
    output_root.mkdir(parents=True)
    artifact_path = output_root / "06_doc_brief.md"
    artifact_path.write_text("# 文档要点\n", encoding="utf-8")
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        task_title="协议回执证据",
        task_goal="输出文档要点。",
        guide={
            "items": [{
                "item_id": "item_06",
                "title": "输出 06_doc_brief.md",
                "status": "open",
                "required": True,
            }],
            "acceptance": [],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [],
    }, active=True)

    text = render_active_task_board(store, recent_context_entries=[{
        "kind": "tool_fact",
        "protocol_receipts": [{
            "tool_id": "file_write",
            "status": "ok",
            "path": str(artifact_path),
            "evidence_refs": [f"file_write:{artifact_path}"],
        }],
    }])

    assert "item_06：已产出，待登记" in text
    assert "06_doc_brief.md" in text


def test_spec522_task_execution_action_guide_ignores_settled_pending_inputs(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_task_execution_action_guide

    wb_root = tmp_path / "workbench"
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        "追加输入测试",
        "检查 settled pending input 不再诱导整合。",
        guide={
            "items": [{
                "item_id": "item_01",
                "title": "继续执行",
                "status": "open",
                "required": True,
            }],
            "acceptance": [],
            "pending_inputs": [{
                "pending_input_id": "input_01",
                "status": "integrated",
                "summary": "已经整合。",
            }],
        },
    )

    text = render_task_execution_action_guide(
        {"guide_id": f"task:{task_id}", "task_id": task_id},
        store,
    )

    assert "当前有待整合输入" not in text
    assert "option_id=integrate_pending_input" not in text


def test_spec522_task_execution_action_guide_keeps_open_pending_inputs(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_task_execution_action_guide

    wb_root = tmp_path / "workbench"
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        "追加输入测试",
        "检查 open pending input 仍优先整合。",
        guide={
            "items": [],
            "acceptance": [],
            "pending_inputs": [{
                "pending_input_id": "input_01",
                "status": "pending",
                "summary": "需要追加材料。",
            }],
        },
    )

    text = render_task_execution_action_guide(
        {"guide_id": f"task:{task_id}", "task_id": task_id},
        store,
    )

    assert "当前有待整合输入" in text
    assert "当前待整合ID：input_01" in text
    assert 'fields.pending_inputs=[{"pending_input_id":"input_01"' in text
    assert '"status":"integrated"' in text
    assert "option_id=integrate_pending_input" in text


def test_spec526_task_execution_action_guide_hides_status_update_coordinates(
        monkeypatch, tmp_path):
    from data import workbench as workbench_module
    from data.workbench import WorkbenchStore
    from logic.task_board import render_task_execution_action_guide

    wb_root = tmp_path / "workbench"
    monkeypatch.setattr(workbench_module, "WB_DIR", str(wb_root))
    store = WorkbenchStore(root_dir=str(wb_root))
    task_id = store.create_task_guide_task(
        "12 项任务",
        "写产物并运行验证。",
        guide={
            "items": [{
                "item_id": "item_01",
                "title": "写报告",
                "status": "open",
                "required": True,
            }],
            "acceptance": [{
                "acceptance_id": "acc_01",
                "description": "报告存在且可追溯",
                "status": "pending",
                "required": True,
            }],
        },
    )

    text = render_task_execution_action_guide(
        {"guide_id": f"task:{task_id}", "task_id": task_id},
        store,
    )

    assert "真实工作优先" in text
    assert "证据后登记" in text
    assert "file_write" in text
    assert "shell_command" in text
    assert "不要把用户原始目标改写成更小的阶段性目标" in text
    assert "部分完成不能登记为全部 done/passed" in text
    assert "任务验收 checkpoint" in text
    assert "完整任务清单状态已放在 40_high_freq" not in text
    assert "工具参数必须走 provider-native 工具通道" not in text
    assert "option_id=update_task_status" not in text
    assert "item_id=task_progress" not in text
    assert "已有真实证据后，再用 guide_submit 更新账本" not in text


def test_spec403_runtime_call_request_is_single_top_now_entry_all_channels(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.context_store import ContextStore

    store = ContextStore(
        cache_dir=str(tmp_path / "cache"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )
    store.append_to_cache(
        403,
        "user",
        "真实用户输入。",
        kind="interaction",
        step="setup",
    )
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        context_store=store,
    )
    monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
    monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
    state = {"base": {"runtime": {"total_round": 403}}}

    calls = [
        assembler.assemble_setup(state, "interactive", user_messages=["真实用户输入。"]),
        assembler.assemble_reaction(state, "interactive", reaction_loop_phase="loop"),
        assembler.assemble_reaction(state, "interactive", reaction_loop_phase="closeout"),
        assembler.assemble_reaction(state, "interactive", reaction_loop_phase="final_reply"),
        assembler.assemble_cleanup(state, "interactive", {"response": "ok"}),
    ]

    for _system, messages in calls:
        contents = [m.get("content", "") for m in messages]
        combined = "\n".join(contents)
        assert combined.count("【Runtime 调用占位】") == 1
        assert combined.count("请根据上下文继续本次调用。") == 1
        assert "这不是用户原始输入" in combined
        placeholder_index = next(
            index for index, content in enumerate(contents)
            if "【Runtime 调用占位】" in content
        )
        interaction_indexes = [
            index for index, content in enumerate(contents)
            if "【本轮交互】" in content or "【历史交互" in content
        ]
        if interaction_indexes:
            assert placeholder_index < min(interaction_indexes)

    store.append_to_cache(
        403,
        "user",
        "请根据上下文继续本次调用。",
        kind="runtime_call_request",
        step="reaction",
    )
    assert all(
        item.get("kind") != "runtime_call_request"
        for item in store.get_now_entries()
    )


def test_spec508_now_layer_json_always_keeps_separate_entries(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.context_store import ContextStore

    store = ContextStore(
        cache_dir=str(tmp_path / "cache"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )
    store.append_to_cache(
        508,
        "user",
        "请读取 DFT_AGENT_EVAL\\agent_eval_tasks.md。",
        kind="interaction",
        step="setup",
    )
    store.append_to_cache(
        508,
        "system",
        "起手确认：存在任务型交互。",
        kind="setup_fact",
        step="setup",
    )
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        context_store=store,
    )
    monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
    monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "高频")
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

    assembler.assemble_reaction(
        {"base": {"meta": {"total_round": 508}, "runtime": {"total_round": 508}}},
        "interactive",
        reaction_loop_phase="loop",
    )

    now_json = json.loads(
        (tmp_path / "context" / "reaction" / "layers" / "50_now.json")
        .read_text(encoding="utf-8")
    )
    content = now_json["content"]
    assert isinstance(content, list)
    assert [entry.get("kind") for entry in content[:3]] == [
        "runtime_call_request",
        "interaction",
        "setup_fact",
    ]
    assert "请读取 DFT_AGENT_EVAL" in content[1]["content"]
    assert "起手确认" in content[2]["content"]

    now_md = (
        tmp_path / "context" / "reaction" / "layers" / "50_now.md"
    ).read_text(encoding="utf-8")
    assert now_md.count("【Runtime 调用占位】") == 1
    assert now_md.index("【Runtime 调用占位】") < now_md.index("【本轮交互】")
    assert now_md.index("【本轮交互】") < now_md.index("【本轮起手事实】")


def test_spec723_lately_layer_json_keeps_rendered_entries_and_source_unchanged(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.context_store import ContextStore

    source = [
        {
            "round": 722,
            "role": "user",
            "kind": "interaction",
            "active_corpus_id": "C-00041",
            "content": "请继续核对缓存语义。",
        },
        {
            "round": 723,
            "role": "assistant",
            "kind": "dialogue_progress",
            "iter": 1,
            "active_corpus_id": "C-00042",
            "content": "正在读取资料并准备下一步。",
        },
        {
            "round": 723,
            "role": "system",
            "kind": "tool_fact",
            "active_corpus_id": "C-00043",
            "content": "file_read 已完成。",
        },
    ]
    before = json.loads(json.dumps(source, ensure_ascii=False))
    store = ContextStore(
        cache_dir=str(tmp_path / "cache"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        context_store=store,
    )
    monkeypatch.setattr(assembler, "_get_lately_entries", lambda _step: source)
    monkeypatch.setattr(assembler, "_get_now_entries", lambda: [])
    monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
    monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "高频")
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

    assembler.assemble_reaction(
        {"base": {"meta": {"total_round": 723}}, "runtime": {"total_round": 723}},
        "interactive",
        current_reaction_iteration=4,
    )

    layer_dir = tmp_path / "context" / "reaction" / "layers"
    lately_json = json.loads((layer_dir / "30_lately.json").read_text(encoding="utf-8"))
    content = lately_json["content"]
    assert [entry["role"] for entry in content] == ["user", "assistant", "system"]
    assert [entry["active_corpus_id"] for entry in content] == [
        "C-00041", "C-00042", "C-00043",
    ]
    assert all(corpus_id in entry["content"] for corpus_id, entry in zip(
        ("C-00041", "C-00042", "C-00043"), content,
    ))
    assert "正在读取资料并准备下一步" not in content[1]["content"]
    assert 'corpus_read(corpus_id="C-00042")' in content[1]["content"]
    assert lately_json["content_markdown"] == (
        layer_dir / "30_lately.md"
    ).read_text(encoding="utf-8")
    assert source == before


def test_spec408_now_layer_keeps_cache_conveyor_order_except_runtime_request(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.context_store import ContextStore

    store = ContextStore(
        cache_dir=str(tmp_path / "cache"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )
    store.append_to_cache(408, "user", "真实用户输入。", kind="interaction", step="setup")
    store.append_to_cache(408, "system", "起手安全裁决通过。", kind="setup_fact", step="setup")
    store.append_to_cache(408, "tool", "已读取文件：book.md，第 1 段。", kind="tool_fact", step="reaction")
    store.append_to_cache(408, "system", "资料正文片段 A。", kind="material", step="reaction")
    store.append_to_cache(408, "tool", "已读取文件：book.md，第 2 段。", kind="tool_fact", step="reaction")
    store.append_to_cache(408, "system", "资料正文片段 B。", kind="material", step="reaction")
    store.append_to_cache(408, "user", "上轮要求继续读书。", kind="relay_handoff", step="reaction")

    assembler = ContextAssembler(context_dir=str(tmp_path / "context"), context_store=store)
    monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
    monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "高频")
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

    _system, messages = assembler.assemble_reaction(
        {"base": {"meta": {"total_round": 408}, "runtime": {"total_round": 408}}},
        "rhythm",
        material_inputs=[{"role": "system", "kind": "material", "content": "即时资料。"}],
        reaction_loop_phase="loop",
    )
    combined = "\n".join(str(m.get("content") or "") for m in messages)

    assert combined.index("【Runtime 调用占位】") < combined.index("【本轮交互】")
    assert combined.index("真实用户输入。") < combined.index("起手安全裁决通过。")
    assert combined.index("起手安全裁决通过。") < combined.index("已读取文件：book.md，第 1 段。")
    assert combined.index("已读取文件：book.md，第 1 段。") < combined.index("资料正文片段 A。")
    assert combined.index("资料正文片段 A。") < combined.index("已读取文件：book.md，第 2 段。")
    assert combined.index("已读取文件：book.md，第 2 段。") < combined.index("资料正文片段 B。")
    assert combined.index("资料正文片段 B。") < combined.index("上轮要求继续读书。")


def test_spec409_cache_entries_use_current_label_only_in_same_round(tmp_path):
    from assembly.context_helpers import render_corpus_entries_for_context
    from data.context_store import ContextStore

    store = ContextStore(
        cache_dir=str(tmp_path / "cache"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )
    store.append_to_cache(617, "user", "本轮用户输入。", kind="interaction", step="setup")
    store.append_to_cache(617, "system", "起手事实。", kind="setup_fact", step="setup")
    store.append_to_cache(617, "tool", "已读取文件。", kind="tool_fact", step="reaction")
    store.append_to_cache(617, "system", "资料正文。", kind="material", step="reaction")

    same_round = "\n".join(
        entry["content"]
        for entry in render_corpus_entries_for_context(
            store.get_now_entries(),
            current_round=617,
            cache_source="now_cache.jsonl",
        )
    )
    next_round = "\n".join(
        entry["content"]
        for entry in render_corpus_entries_for_context(
            store.get_now_entries(),
            current_round=618,
            cache_source="now_cache.jsonl",
        )
    )

    assert "【本轮交互】" in same_round
    assert "【本轮起手事实】" in same_round
    assert "【本轮工具事实】" in same_round
    assert "【本轮资料】" in same_round
    assert "【历史交互，来自第 617 轮】" in next_round
    assert "【历史起手事实，来自第 617 轮】" in next_round
    assert "【历史工具事实，来自第 617 轮】" in next_round
    assert "【历史资料，来自第 617 轮】" in next_round


def test_spec663_same_interaction_text_remains_current_in_next_round(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.context_store import ContextStore

    store = ContextStore(
        cache_dir=str(tmp_path / "cache"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )
    store.append_to_cache(
        574, "user", "请重新读取同一张技能卡。",
        kind="interaction", step="setup")
    store.append_to_cache(
        575, "user", "请重新读取同一张技能卡。",
        kind="interaction", step="setup")

    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"), context_store=store)
    monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
    monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

    _system, messages = assembler.assemble_setup(
        {"base": {"meta": {"total_round": 575}}},
        "interactive",
    )
    combined = "\n".join(str(item.get("content") or "") for item in messages)

    assert "【历史交互，来自第 574 轮】" in combined
    assert "【本轮交互】" in combined
    assert combined.count("请重新读取同一张技能卡。") == 2

def test_permanent_rules_only_load_permanent_registry(monkeypatch, tmp_path):
    from assembly.context import ContextAssembler

    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
    monkeypatch.setattr(assembler, "_load_core_identity", lambda: "CORE")
    monkeypatch.setattr(
        assembler,
        "_load_rules_for_layers",
        lambda layers: "PERMANENT_RULES" if layers == ["permanent"] else "",
    )

    text = assembler._build_permanent({}, "final_reply", "interactive")

    assert "CORE" in text
    assert "PERMANENT_RULES" in text
    assert "<!-- [RULES:permanent+step] -->" in text


def test_spec723_permanent_cache_keeps_file_boundaries(monkeypatch, tmp_path):
    from assembly.context import ContextAssembler
    from data.state_store import StateStore

    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"), state_store=state_store)
    assembler._registry = {"permanent": [
        {"path": "protocol/base/security.md", "file": "security.md"},
        {"path": "protocol/base/memory.md", "file": "memory.md"},
    ]}
    monkeypatch.setattr(assembler, "_load_core_identity", lambda: "CORE")
    monkeypatch.setattr(
        assembler, "_load_rule_file", lambda path: f"RULE:{path}")

    first = assembler._cached_or_build(
        "setup", "permanent", True,
        lambda: assembler._build_permanent({}, "setup", "interactive"),
    )
    first_index = assembler._current_layer_block_index["permanent"]
    assembler._current_layer_block_index = {}
    second = assembler._cached_or_build(
        "setup", "permanent", False, lambda: "must not rebuild")

    assert second == first
    assert assembler._current_layer_block_index["permanent"] == first_index
    assert [item["block_id"] for item in first_index] == [
        "permanent:core_identity",
        "rule:protocol/base/security.md",
        "rule:protocol/base/memory.md",
    ]
    assert [item.get("source_block_id") for item in first_index[1:]] == [
        "protocol/base/security.md", "protocol/base/memory.md"]
    _assert_exact_block_index(first, first_index)


def test_spec405_internal_handoff_is_not_model_visible(tmp_path, monkeypatch):
    from assembly.context import ContextAssembler

    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
    monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
    monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
    monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
    monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
    state = {"base": {"runtime": {"total_round": 405}}}

    _system, messages = assembler.assemble_reaction(
        state,
        "interactive",
        internal_handoff=[{
            "role": "system",
            "kind": "handoff",
            "content": "SHADOW_LAYER_SHOULD_NOT_RENDER",
        }],
    )
    combined = "\n".join(m.get("content", "") for m in messages)

    assert "SHADOW_LAYER_SHOULD_NOT_RENDER" not in combined
    assert "【内部接力】" not in combined


def test_spec287_step_guide_popup_is_plain_chinese_without_structure_fields(tmp_path):
    from assembly.context import ContextAssembler

    assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
    popup = assembler._build_handoff_popup(
        "reaction",
        "interactive",
        reaction_loop_phase="closeout",
    )

    assert "【反应循环指南】" in popup
    assert "【反应收束指南】" not in popup
    assert "当前是反应步循环" in popup
    assert "裸文本是非法输出" not in popup
    assert "reaction_finalize(handoff_text)" in popup
    assert "Spec184" not in popup
    for token in ("- kind:", "tier:", "decision_required:", "source:", "fields:"):
        assert token not in popup
    assert "反应循环交接" not in popup


# ============================================================
# StatusBar 测试
# ============================================================


def test_spec723_statusbar_blocks_are_exact_source_slices():
    from assembly.statusbar import StatusBarBuilder

    projection = {
        "round": {"id": "R000723", "progress": "运行中", "type": "interactive"},
        "time": {"text": "2026-08-06 12:00 UTC+08:00"},
        "mode": "实践",
        "workhood": "标准运作",
        "dynamic": "稳定",
        "interaction": {"display_name": "Codex", "registration_status": "bound"},
        "supplemental_sections": ["补充甲", "补充乙"],
        "relation_cards": [
            {"id": "REL-A", "name": "A", "category": "ours", "summary": "甲"},
            {"id": "REL-B", "name": "B", "category": "them", "summary": "乙"},
        ],
    }

    text, block_index = StatusBarBuilder.render_with_block_index(projection)

    assert StatusBarBuilder.render(projection) == text
    assert [item["kind"] for item in block_index] == [
        "status_summary", "status_supplemental", "status_supplemental",
        "status_relation_card", "status_relation_card",
    ]
    _assert_exact_block_index(text, block_index)



# ============================================================
# Popup 测试
# ============================================================

class TestPopup:
    def test_read_empty_when_no_file(self, tmp_path, monkeypatch):
        from assembly import popup as pm
        popup_path = tmp_path / "popup.md"
        monkeypatch.setattr(pm, "CONTEXT_POPUP", str(popup_path))

        mgr = pm.PopupManager(str(popup_path))
        assert mgr.read_popup() == ""
        assert mgr.has_popup() is False

    def test_write_and_read(self, tmp_path):
        from assembly.popup import PopupManager
        path = str(tmp_path / "popup.md")
        mgr = PopupManager(path)
        mgr.write_popup("测试弹窗内容")
        assert mgr.has_popup() is True
        assert "测试弹窗内容" in mgr.read_popup()

    def test_clear_popup(self, tmp_path):
        from assembly.popup import PopupManager
        path = str(tmp_path / "popup.md")
        mgr = PopupManager(path)
        mgr.write_popup("test")
        mgr.clear_popup()
        assert mgr.has_popup() is False

    def test_popup_active_syncs_to_state_store(self, tmp_path):
        from assembly.popup import PopupManager
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        mgr = PopupManager(str(tmp_path / "popup.md"), state_store=sm)

        mgr.write_popup("紧急消息")
        assert sm.get("base.context_cache.popup_active") is True

        mgr.clear_popup()
        assert sm.get("base.context_cache.popup_active") is False

        (tmp_path / "popup.md").write_text("外部写入弹窗", encoding="utf-8")
        assert "外部写入弹窗" in mgr.read_popup()
        assert sm.get("base.context_cache.popup_active") is True

    def test_popup_without_state_store_has_no_state_side_effect(self, tmp_path):
        from assembly.popup import PopupManager

        popup_path = tmp_path / "popup.md"
        state_path = tmp_path / "state.json"
        mgr = PopupManager(str(popup_path))

        mgr.write_popup("紧急消息")
        assert "紧急消息" in mgr.read_popup()
        mgr.clear_popup()

        assert not state_path.exists()

    def test_inject_into_messages(self, tmp_path):
        from assembly.popup import PopupManager
        path = str(tmp_path / "popup.md")
        mgr = PopupManager(path)
        mgr.write_popup("紧急消息")
        msgs = [{"role": "user", "content": "你好"}]
        result = mgr.inject_into_messages(msgs)
        assert len(result) == 2
        assert "紧急消息" in result[-1]["content"]

    def test_inject_when_no_popup(self, tmp_path):
        from assembly.popup import PopupManager
        path = str(tmp_path / "popup.md")
        mgr = PopupManager(path)
        msgs = [{"role": "user", "content": "你好"}]
        result = mgr.inject_into_messages(msgs)
        assert len(result) == 1

    def test_emit_warning(self, tmp_path):
        from assembly.popup import PopupManager
        path = str(tmp_path / "popup.md")
        mgr = PopupManager(path)
        mgr.emit_warning("token 用量达到 75%")
        content = mgr.read_popup()
        assert "警告" in content
        assert "75%" in content

    def test_emit_urgent(self, tmp_path):
        from assembly.popup import PopupManager
        path = str(tmp_path / "popup.md")
        mgr = PopupManager(path)
        mgr.emit_urgent("身份超时")
        content = mgr.read_popup()
        assert "紧急" in content

    def test_emit_secure(self, tmp_path):
        from assembly.popup import PopupManager
        path = str(tmp_path / "popup.md")
        mgr = PopupManager(path)
        mgr.emit_secure("检测到冲击层滥用")
        content = mgr.read_popup()
        assert "安全" in content

    def test_spec201_popup_policy_renders_four_modules_and_hides_fields(self):
        from logic.popup_policy import PopupPolicy

        rendered, block_index = PopupPolicy().combine_with_block_index([
            (
                "- kind: native_tool_result\n"
                "  tier: warning\n"
                "  decision_required: false\n"
                "  tool_id: file_read\n"
                "  call_id: call_hidden\n"
                "  reason: native_argument_missing_required\n"
                "  field: path\n"
                "  expected: required\n"
                "  actual: missing\n"
                "  next_action: revise_arguments\n"
                "  message: |\n"
                "    文件读取工具调用失败。\n"
                "    下一次调用必须填写该字段：`path`。\n"
                "    不要声称该工具已经成功。"
            ),
            (
                "- kind: tool_request_card\n"
                "  tier: reminder\n"
                "  decision_required: false\n"
                "  source: docs/protocol/base/popup.md\n"
                "  message: 需要工具时只能写 `tool_request`。"
            ),
            (
                "- kind: general_tool_guide\n"
                "  tier: guide\n"
                "  decision_required: false\n"
                "  tool_id: file_read\n"
                "  source: docs/protocol/base/tools.md\n"
                "  message: |\n"
                "    `file_read` 只读文件。"
            ),
            (
                "- kind: relay_target_card\n"
                "  tier: reminder\n"
                "  decision_required: false\n"
                "  source: reaction_finalize\n"
                "  message: 当前步接收到上一轮中继目标。"
            ),
        ])

        headings = [
            "## GUIDE｜指南",
            "## REMINDER｜提醒",
            "## WARNING｜警告",
        ]
        assert [heading in rendered for heading in headings] == [True, True, True]
        assert rendered.index(headings[0]) < rendered.index(headings[1])
        assert rendered.index(headings[1]) < rendered.index(headings[2])
        assert rendered.rstrip().endswith("不要声称该工具已经成功。")
        for forbidden in (
            "kind:",
            "tier:",
            "decision_required:",
            "source:",
            "call_id:",
            "field:",
            "expected:",
            "actual:",
            "next_action:",
            "call_hidden",
        ):
            assert forbidden not in rendered
        assert "`file_read` 只读文件。" in rendered
        assert "需要工具时只能写 `tool_request`。" in rendered
        assert "当前步接收到上一轮中继目标。" in rendered
        assert "## HANDOFF｜交接" not in rendered
        assert "### 原生工具调用警告" in rendered
        assert "文件读取工具调用失败。" in rendered
        assert "下一次调用必须填写该字段：`path`。" in rendered
        assert len(block_index) == 4
        _assert_exact_block_index(rendered, block_index)

    def test_spec623_popup_budget_always_preserves_resident_memory_card(self):
        from logic.popup_policy import MAX_POPUP_CHARS, PopupPolicy

        memory = (
            "- kind: memory_settlement_reminder\n"
            "  tier: reminder\n"
            "  decision_required: false\n"
            "  message: |\n"
            "    资料正文由 material/最近缓存承载。\n"
            "    只有 `MEM-*` 回执才算写入成功。"
        )
        oversized_guide = (
            "- kind: reaction_step_guide\n"
            "  tier: guide\n"
            "  message: |\n"
            f"    {'G' * (MAX_POPUP_CHARS * 2)}"
        )

        rendered, block_index = PopupPolicy().combine_with_block_index(
            [oversized_guide] * 4 + [memory, memory])

        assert len(rendered) <= MAX_POPUP_CHARS
        assert rendered.count("### 记忆提醒") == 1
        assert rendered.count("资料正文由 material/最近缓存承载") == 1
        assert "只有 `MEM-*` 回执才算写入成功" in rendered
        assert [item["block_id"] for item in block_index] == [
            "popup:budget_capped"]
        _assert_exact_block_index(rendered, block_index)

    def test_step_output_schema_popup_template_exists(self):
        from assembly.popup import PopupManager

        template = PopupManager.load_template("step_output_schema_attention")

        assert template["tier"] == "guide"
        assert template["decision_required"] == "false"
        assert "provider-native terminal tool" in template["message"]
        assert "裸文本是非法输出" in template["message"]

# ============================================================
# Audit 测试
# ============================================================

class TestAudit:
    def test_write_audit_manifest(self, tmp_path):
        import json
        from data.audit_store import AuditStore

        writer = AuditStore(
            setup_dir=str(tmp_path / "setup"),
            reaction_dir=str(tmp_path / "reaction"),
            cleanup_dir=str(tmp_path / "cleanup"),
        )
        layers = {
            "permanent": "永固层内容",
            "periodic": "定期层内容",
            "high_freq": "高频层内容",
            "lately": "最近缓存内容",
            "now": "当前缓存内容",
            "popup": "弹窗内容",
            "full_system": "完整 system prompt",
        }
        writer.write_audit("setup", layers)

        manifest = json.loads(
            (tmp_path / "setup" / "manifest.json").read_text(encoding="utf-8")
        )
        assert manifest is not None
        assert manifest["step"] == "setup"
        assert manifest["layers"]["permanent"]["chars"] == 5  # "永固层内容"
        assert manifest["layers"]["lately"]["chars"] == 6  # "最近缓存内容"
        assert manifest["total_chars"] >= 9

    def test_step_files_written(self, tmp_path):
        from data.audit_store import AuditStore

        writer = AuditStore(reaction_dir=str(tmp_path / "reaction"))
        stale_dir = tmp_path / "reaction"
        stale_dir.mkdir()
        for fn in ["permanent.md", "periodic.md", "high_freq.md"]:
            (stale_dir / fn).write_text("old", encoding="utf-8")

        layers = {
            "permanent": "p", "periodic": "P", "high_freq": "h",
            "lately": [{"role": "user", "content": "lat"}],
            "lately_markdown": "lat", "now": "now",
            "statusbar": "status",
            "popup": "pop",
            "full_system": "full",
        }
        writer.write_audit("reaction", layers)

        # Spec 023: 旧扁平审计副本已退役，分层审计只保留 layers/*.md。
        for fn in ["permanent.md", "periodic.md", "high_freq.md"]:
            assert not os.path.exists(tmp_path / "reaction" / fn), f"{fn} 不应再写出"

        for fn in ["step.md", "manifest.json"]:
            assert os.path.isfile(tmp_path / "reaction" / fn), f"{fn} 未写入"

        # Spec316: 兼容投影退役后只保留七层审计渲染。
        for fn, expected in {
            "10_permanent.md": "p",
            "20_periodic.md": "P",
            "30_lately.md": "lat",
            "40_high_freq.md": "h",
            "50_now.md": "now",
            "60_statusbar.md": "status",
            "99_popup.md": "pop",
        }.items():
            path = tmp_path / "reaction" / "layers" / fn
            assert path.is_file(), f"layers/{fn} 未写入"
            assert path.read_text(encoding="utf-8") == expected
        for retired in [
            "20_remote_cache.md",
            "30_high_freq.md",
            "40_lately.md",
            "40_near_cache.md",
            "50_current_input.md",
            "50_interaction_input.md",
            "55_material_input.md",
            "60_internal_handoff.md",
            "00_permanent.md",
            "00_permanent.json",
            "10_periodic.md",
            "10_periodic.json",
            "90_statusbar.md",
            "90_statusbar.json",
        ]:
            assert not (tmp_path / "reaction" / "layers" / retired).exists()

        # 验证 manifest layers 包含七层。
        import json
        with open(tmp_path / "reaction" / "manifest.json", "r", encoding="utf-8") as f:
            manifest = json.load(f)
        assert set(manifest["layers"].keys()) == {
            "permanent", "periodic", "lately", "high_freq", "now", "statusbar", "popup"
        }

    def test_spec422_audit_writes_machine_json_layers(self, tmp_path):
        from data.audit_store import AuditStore

        writer = AuditStore(reaction_dir=str(tmp_path / "reaction"))
        writer.write_audit("reaction", {
            "permanent": "p",
            "periodic": "P",
            "lately": "lat",
            "high_freq": "h",
            "now": "now",
            "statusbar": "status",
            "popup": "pop",
            "full_system": "full",
        })

        import json
        expected = {
            "10_permanent.json": ("permanent", "p", 10),
            "20_periodic.json": ("periodic", "P", 20),
            "30_lately.json": ("lately", "lat", 30),
            "40_high_freq.json": ("high_freq", "h", 40),
            "50_now.json": ("now", "now", 50),
            "60_statusbar.json": ("statusbar", "status", 60),
            "99_popup.json": ("popup", "pop", 99),
        }
        for filename, (layer_id, content, order) in expected.items():
            payload = json.loads(
                (tmp_path / "reaction" / "layers" / filename).read_text(
                    encoding="utf-8"
                )
            )
            assert payload["schema"] == "context_layer.v1"
            assert payload["layer_id"] == layer_id
            assert payload["order"] == order
            assert payload["content"] == content
            assert payload["chars"] == len(content)
            assert payload["sha256"]
            assert "dirty" not in payload
            assert "reused" not in payload


        manifest = json.loads(
            (tmp_path / "reaction" / "manifest.json").read_text(encoding="utf-8")
        )
        assert manifest["layer_order"] == [
            "10_permanent",
            "20_periodic",
            "30_lately",
            "40_high_freq",
            "50_now",
            "60_statusbar",
            "99_popup",
        ]

    def test_spec420_final_reply_audit_returns_to_reaction_dir(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "permanent")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "periodic")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "high_freq")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        assembler.assemble_reaction(
            {"base": {"runtime": {"total_round": 420}}},
            "interactive",
            reaction_loop_phase="final_reply",
        )

        reaction_dir = tmp_path / "context" / "reaction"
        assert (reaction_dir / "step.md").is_file()
        assert not (reaction_dir / "step.json").exists()
        assert (reaction_dir / "manifest.json").is_file()
        assert (reaction_dir / "layers" / "50_now.md").is_file()
        assert not (tmp_path / "context" / "final_reply").exists()

        import json
        manifest = json.loads(
            (reaction_dir / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["step"] == "reaction"
        assert set(manifest["layers"].keys()) == {
            "permanent", "periodic", "lately", "high_freq", "now", "statusbar", "popup"
        }

    def test_context_audit_write_failure_blocks_assembly(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "permanent")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "periodic")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "high_freq")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        def fail_write(*_args, **_kwargs):
            raise OSError("disk full")

        monkeypatch.setattr(assembler.audit, "write_audit", fail_write)

        with pytest.raises(RuntimeError, match="context_audit_write_failed"):
            assembler.assemble_reaction(
                {"base": {"runtime": {"total_round": 423}}},
                "interactive",
            )


# ============================================================
# Context 装配器集成测试
# ============================================================

class TestContextAssembler:
    class _FakeConfig:
        def __init__(self, memory_limit=65536):
            self.memory_limit = memory_limit

        def get_periodic_limits(self):
            return {
                "periodic_memory_items_chars": self.memory_limit,
            }

    def _make_state(self, **overrides):
        base = {
            "meta": {"total_round": 1, "daily_round": 1, "last_update": "now"},
            "activity_mode": "理论",
            "fatigue": {"value": 10},
            "workhood_index": {"value": 30},
            "heartbeat_flags": {},
            "dynamic_axes": {
                "valence": {"value": 0}, "arousal": {"value": 0},
                "focus": {"value": 0}, "mood": {"value": 0},
                "humor": {"value": 0}, "safety": {"value": 0},
            },
            "core_axes": {"S": 50, "C": 50, "V": 50, "A": 50, "R": 50, "B": 50},
            "runtime": {"phase": "idle", "standby_countdown": 0},
        }
        base.update(overrides)
        return {"base": base}

    def test_spec400_container_index_distinguishes_types_from_instances(
            self, monkeypatch):
        from assembly.context_indexes import build_container_index
        import data.container_store as container_store_mod

        class FakeContainerStore:
            def load_registry(self):
                return {
                    "containers": [
                        {"prefix": "DC", "name": "辩证链"},
                        {"prefix": "PRJ", "name": "项目"},
                    ]
                }

            def list_containers(self, prefix=None, status=None):
                if prefix == "DC":
                    return []
                if prefix == "PRJ":
                    return [{
                        "id": "PRJ-20260623-01",
                        "title": "读书工程",
                        "status": "active",
                    }]
                return []

        monkeypatch.setattr(container_store_mod, "ContainerStore", FakeContainerStore)

        text = build_container_index(object())

        assert "- [DC] 辩证链（类型，不是可打开实例）" in text
        assert "暂无可打开实例；如需新建，请调用 memory_container_create。" in text
        assert "- [PRJ] 项目（类型，不是可打开实例）" in text
        assert "可打开实例：PRJ-20260623-01 — 读书工程（active）" in text
        assert "DC-1" not in text

    def test_statusbar_does_not_fallback_to_all_relation_cards(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = [
            {"id": "FMZ", "name": "FMZ", "category": "self", "status": "active"},
            {"id": "TzPz", "name": "TzPz", "category": "ours", "status": "active"},
            {"id": "FMA", "name": "FMA", "category": "them", "status": "active"},
        ]
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})
        monkeypatch.setattr(RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(tmp_path / f"{cid}.md"))
        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_get_current_input_text", lambda: None)

        text = assembler._build_statusbar_with_relations(self._make_state(), "interactive")

        assert "## STATUSBAR" in text
        assert "## 关系卡" not in text
        assert "FMA" not in text

    def test_statusbar_shows_only_present_relation_card(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = [
            {"id": "TzPz", "category": "ours", "status": "active"},
            {"id": "FMA", "category": "them", "status": "active"},
        ]
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})
        monkeypatch.setattr(RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(tmp_path / f"{cid}.md"))
        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_get_current_input_text", lambda: "TzPz 继续验证关系焦点")

        text = assembler._build_statusbar_with_relations(self._make_state(), "interactive")

        assert "## 关系卡" in text
        assert "TzPz" in text
        assert "FMA" not in text

    def test_statusbar_uses_current_round_input_for_present_focus(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = [
            {"id": "REL-Codex", "name": "Codex", "category": "them", "status": "active"},
        ]
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})
        monkeypatch.setattr(RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(tmp_path / f"{cid}.md"))
        assembler = ContextAssembler(context_dir=str(tmp_path))
        assembler._current_input_text = "我是 Codex，当前做 Spec 024 验证"
        assembler._current_interaction_meta = {
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }

        text = assembler._build_statusbar_with_relations(self._make_state(), "interactive")

        assert "## 关系卡" in text
        assert "Codex" in text


    def test_cache_section_returns_entries_without_layer_marker(self, tmp_path):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        messages = assembler._build_cache_section(
            "当前缓存 now",
            "now_cache.jsonl",
            [{
                "role": "user",
                "kind": "interaction",
                "content": "继续",
                "round": 1,
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            }],
            current_round=1,
        )

        assert len(messages) == 1
        assert "## 当前缓存 now" not in messages[0]["content"]
        assert "【本轮交互】" in messages[0]["content"]
        assert "交互对象是unknown，身份未知，来源为未解析。" in messages[0]["content"]

    def test_cache_section_hides_aggregate_round_and_char_audit(self, tmp_path):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        messages = assembler._build_cache_section(
            "当前缓存 now",
            "now_cache.jsonl",
            [{
                "role": "system",
                "kind": "tool_fact",
                "content": "reading_cursor next_start_line=891",
                "source_round": "R000473",
            }],
            current_round=474,
        )
        combined = "\n".join(m["content"] for m in messages)

        assert "当前可见轮次：R000474" not in combined
        assert "来源轮次：R000473" not in combined
        assert "条，" not in combined
        assert "字符。" not in combined
        assert "【历史工具事实，来自第 473 轮】" in combined

    def test_spec270_cache_section_uses_two_line_corpus_headers(self, tmp_path):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        entry = {
            "role": "system",
            "kind": "tool_fact",
            "content": "reading_cursor next_start_line=891",
            "round": 473,
            "timestamp": "2026-06-10T10:00:00+08:00",
            "step": "reaction",
            "iter": 2,
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "tool_fact",
            "source_block_id": "blk-473-tool-fact",
        }

        messages = assembler._build_cache_section(
            "当前缓存 now",
            "now_cache.jsonl",
            [entry],
            current_round=474,
        )

        assert len(messages) == 1
        assert messages[0] is not entry
        assert entry["content"] == "reading_cursor next_start_line=891"
        text = messages[0]["content"]
        assert "## 当前缓存 now" not in text
        assert "【历史工具事实，来自第 473 轮】" in text
        assert "这是历史工具事实，不代表本轮已经执行。" in text
        assert "visible_at_round" not in text
        assert "source_round" not in text
        assert "cached_at" not in text
        assert "step=reaction" not in text
        assert "cache_source" not in text
        assert "source_block_id" not in text
        assert "boundary=" not in text
        assert "历史语料不是本轮执行证据" not in text
        assert "visible material is not proof of execution in this round" not in text

    def test_spec276_retired_context_kinds_are_filtered_from_model_context(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.context_store import ContextStore

        store = ContextStore(
            cache_dir=str(tmp_path / "cache"),
            raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
        )
        retired = {
            "tool_result": "FULL TOOL RESULT SHOULD NOT RENDER",
            "tool_summary": "TOOL SUMMARY SHOULD NOT RENDER",
            "protocol_tool_receipt": "FULL RECEIPT SHOULD NOT RENDER",
            "native_tool_result": "CALL ID SHOULD NOT RENDER",
            "training_evidence": "TRAINING EVIDENCE SHOULD NOT RENDER",
            "final_reply_handoff": "FINAL REPLY HANDOFF SHOULD NOT RENDER",
        }
        for kind, content in retired.items():
            store.append_to_cache(276, "system", content, kind=kind, step="reaction")
        store.append_to_cache(
            276,
            "system",
            (
                "本轮已经成功读取文件：book.md。\n"
                "读取范围：第 1 行到第 10 行。\n"
                "读取结果：完整，没有截断。"
            ),
            kind="tool_fact",
            step="reaction",
            tool_result={
                "tool_id": "file_read",
                "status": "ok",
                "path": "book.md",
                "start_line": 1,
                "end_line": 10,
            },
        )

        assembler = ContextAssembler(
            context_dir=str(tmp_path / "context"),
            context_store=store,
        )
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        for phase in ("setup", "reaction", "final_reply", "cleanup"):
            if phase == "setup":
                _system, messages = assembler.assemble_setup(
                    self._make_state(meta={"total_round": 276}), "interactive")
            elif phase == "reaction":
                _system, messages = assembler.assemble_reaction(
                    self._make_state(meta={"total_round": 276}), "interactive")
            elif phase == "final_reply":
                _system, messages = assembler.assemble_reaction(
                    self._make_state(meta={"total_round": 276}),
                    "interactive",
                    reaction_loop_phase="final_reply",
                )
            else:
                _system, messages = assembler.assemble_cleanup(
                    self._make_state(meta={"total_round": 276}),
                    "interactive",
                    {"response": "自然回复"},
                )
            combined = "\n".join(m.get("content", "") for m in messages)
            assert "本轮已经成功读取文件：book.md。" in combined
            assert "[file_read ok]" not in combined
            for content in retired.values():
                assert content not in combined

    def test_spec283_internal_native_replay_handoff_is_filtered_from_payload(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_reaction(
            self._make_state(meta={"total_round": 283}),
            "interactive",
            internal_handoff=[{
                "role": "tool",
                "kind": "native_tool_result",
                "content": "provider_native_tool_result: call_283",
                "native_tool_call_envelopes": [{
                    "call_id": "call_283",
                    "tool_id": "file_read",
                }],
                "native_tool_outputs": [{
                    "call_id": "call_283",
                    "tool_id": "file_read",
                    "status": "ok",
                }],
            }],
        )

        combined = "\n".join(str(m.get("content") or "") for m in messages)
        assert "provider_native_tool_result" not in combined
        assert not any(m.get("native_tool_call_envelopes") for m in messages)
        assert not any(m.get("native_tool_outputs") for m in messages)

    def test_spec405_setup_fact_internal_handoff_no_longer_enters_now_layer(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_reaction(
            self._make_state(meta={"total_round": 318}),
            "interactive",
            internal_handoff=[{
                "role": "system",
                "kind": "setup_fact",
                "content": "[setup_fact] security_verdict=pass; mount=MEM-SETUP318",
                "interaction_object": "system",
                "identity_status": "system",
                "interaction_source": "setup_finalize",
            }],
        )

        combined = "\n".join(str(m.get("content") or "") for m in messages)
        assert "<!-- 当前缓存 now -->" not in combined
        assert "MEM-SETUP318" not in combined
        assert "setup_fact" not in combined
        assert "【Runtime 调用占位】" in combined

    def test_spec261_current_entries_are_marked_as_current_round_blocks(self, tmp_path):
        from assembly.context_helpers import render_corpus_entries_for_context

        messages = render_corpus_entries_for_context(
            [{
                "role": "user",
                "kind": "interaction",
                "content": "本轮用户输入",
                "interaction_object": "TzPz",
                "identity_status": "declared",
                "interaction_source": "current_user_message",
            }],
            current_round=474,
            cache_source="now_cache.jsonl",
        )

        text = messages[0]["content"]
        assert "【本轮交互】" in text
        assert "交互对象是TzPz，身份已声明，来源为本轮输入。" in text
        assert "visible_at_round" not in text
        assert "source_round=" not in text
        assert "boundary=current" not in text

    def test_spec270_compacted_summary_uses_chinese_source_header(self, tmp_path):
        from assembly.context_helpers import render_corpus_entries_for_context

        messages = render_corpus_entries_for_context(
            [{
                "role": "system",
                "kind": "cache_summary",
                "content": "压缩后的历史工具摘要",
                "round": 470,
                "timestamp": "2026-06-10T12:00:00+08:00",
                "compacted_at": "2026-06-10T12:05:00+08:00",
                "compact_reason": "post_lately_trim",
                "oldest_source_round": 468,
                "oldest_cached_at": "2026-06-10T09:00:00+08:00",
                "source_block_count": 7,
                "source_block_ids": ["blk-468-a", "blk-469-b", "blk-470-c"],
            }],
            current_round=474,
            cache_source="lately_cache.jsonl",
        )

        text = messages[0]["content"]
        assert "【最近缓存压缩摘要】" in text
        assert "压缩后的历史工具摘要" in text
        for token in (
            "far_head",
            "near_head",
            "source_block_count",
            "source_block_ids",
            "boundary=history",
            "压缩历史语料块",
        ):
            assert token not in text

    def test_relay_target_popup_shows_pending_file_read_target(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        state = self._make_state(
            runtime={
                "phase": "idle",
                "standby_countdown": 0,
                "pending_relay_target": {
                    "kind": "tool",
                    "tool_id": "file_read",
                    "path": "book.md",
                    "next_start_line": 891,
                },
            })

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="relay",
            state=state,
            include_content=False,
            mount_ids=None,
        )

        popup = messages[-1]["content"]
        assert "中继目标账本" in popup
        assert "book.md" in popup
        assert "line_start=891" in popup
        assert "cursor=line:891" not in popup

    def test_spec462_full_context_keeps_lately_and_now_short_ids_unique(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [{
            "round": 599,
            "role": "system",
            "kind": "tool_fact",
            "content": "历史工具事实。",
        }])
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [{
            "round": 600,
            "role": "user",
            "kind": "interaction",
            "content": "本轮任务正文。",
        }])
        state = self._make_state(meta={"total_round": 600})

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="interactive",
            state=state,
            include_content=False,
            mount_ids=None,
        )

        text = "\n\n".join(message["content"] for message in messages)
        assert "语料短ID：C-00001。" in text
        assert "语料短ID：C-00002。" in text

    def test_spec375_relay_input_cache_is_not_model_visible_when_target_card_exists(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.context_store import ContextStore

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(ContextStore, "get_now_entries", lambda self: [{
            "round": 615,
            "role": "system",
            "kind": "relay_input",
            "content": "旧中继正文：请从 line_start=891 继续读。",
            "interaction_source": "reaction_finalize",
        }])
        state = self._make_state(
            runtime={
                "phase": "idle",
                "standby_countdown": 0,
                "pending_relay_target": {
                    "kind": "tool",
                    "tool_id": "file_read",
                    "path": "book.md",
                    "next_start_line": 891,
                },
            })

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="relay",
            state=state,
            include_content=False,
            mount_ids=None,
        )

        combined = "\n".join(m.get("content", "") for m in messages)
        assert "旧中继正文" not in combined
        assert combined.count("line_start=891") == 1
        assert "cursor=line:891" not in combined
        assert "中继目标账本" in combined

    def test_cached_unknown_identity_interaction_appends_ordinary_identity_popup(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [{
                "role": "user",
                "kind": "interaction",
                "content": "继续",
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            }])

        _system, messages = assembler._build_full_context(
            step="setup",
            round_type="interactive",
            state=self._make_state(),
            include_content=False,
            mount_ids=None,
            user_messages=["这条只用于索引，不应临时造可见输入"],
        )

        popup = messages[-1]["content"]
        assert "<!-- POPUP" in popup
        assert "## REMINDER｜提醒" in popup
        assert "### 身份提醒" in popup
        assert "本轮外部输入没有明确自己的身份" in popup
        assert "kind:" not in popup
        assert "decision_required:" not in popup
        assert "security_review" not in popup
        assert "<!-- STATUSBAR（状态栏层） -->" in messages[-2]["content"]
        assert any(
            m["role"] == "user" and "继续" in m["content"]
            for m in messages
        )
        assert not any(
            m["role"] == "user" and "这条只用于索引" in m["content"]
            for m in messages
        )

    def test_reaction_unknown_identity_appends_resolution_card(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        interaction_entry = {
            "role": "user",
            "kind": "interaction",
            "content": "继续",
            "interaction_object": "unknown",
            "identity_status": "timeout",
            "interaction_source": "identity_timeout",
        }
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [interaction_entry])
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_mounted_content", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="interactive",
            state=self._make_state(),
            include_content=False,
            mount_ids=None,
        )

        popup = messages[-1]["content"]
        assert "## REMINDER｜提醒" in popup
        assert "### 身份确认" in popup
        assert "最终回复中简短询问" in popup
        assert "identity_action" not in popup
        assert "identity_note" not in popup
        assert "kind:" not in popup
        assert "decision_required:" not in popup

    def test_assemble_setup_clears_stale_interaction_meta_without_input(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        assembler._current_input_text = "我是 Codex，上一轮输入"
        assembler._current_interaction_meta = {
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }
        monkeypatch.setattr(
            ContextAssembler,
            "_build_full_context",
            lambda self, **kwargs: ("system", []),
        )

        assembler.assemble_setup(self._make_state(), "autonomous", user_messages=[])

        assert assembler._current_input_text is None
        assert assembler._current_interaction_meta is None

    def test_reaction_uses_cached_interaction_marker(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [{
            "role": "user",
            "kind": "interaction",
            "content": "我是 Codex，继续验证",
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }])

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="interactive",
            state=self._make_state(),
            include_content=True,
            mount_ids=[],
            user_messages=None,
        )

        current = next(
            m for m in messages
            if m.get("kind") == "interaction" and "我是 Codex，继续验证" in m.get("content", "")
        )
        assert not any("## 当前缓存 now" in m.get("content", "") for m in messages)
        assert current["interaction_object"] == "Codex"
        assert current["identity_status"] == "declared"

    def test_current_input_text_does_not_override_cached_interaction(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        assembler._current_input_text = "我是 Codex，继续验证"
        assembler._current_interaction_meta = {
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [{
            "role": "user",
            "kind": "interaction",
            "content": "上一轮未确认输入",
            "interaction_object": "unknown",
            "identity_status": "timeout",
            "interaction_source": "identity_timeout",
        }])

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="interactive",
            state=self._make_state(),
            include_content=True,
            mount_ids=[],
            user_messages=None,
        )

        current = next(
            m for m in messages
            if m.get("kind") == "interaction" and "上一轮未确认输入" in m.get("content", "")
        )
        combined = "\n".join(m.get("content", "") for m in messages)
        assert "## 当前缓存 now" not in combined
        assert "当前交互对象：Codex" in combined
        assert "身份状态：declared" not in combined
        assert "我是 Codex，继续验证" not in combined
        assert current["interaction_object"] == "unknown"

    def test_spec449_rhythm_reaction_keeps_interaction_text_visible(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        assembler._current_input_text = "READ_DFT_AGENT_EVAL_TASKS_NOW"
        assembler._current_interaction_meta = {
            "interaction_object": "TzPz",
            "identity_status": "known",
            "interaction_source": "user_message",
        }
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [{
            "role": "user",
            "kind": "interaction",
            "content": "READ_DFT_AGENT_EVAL_TASKS_NOW",
            "interaction_object": "TzPz",
            "identity_status": "known",
            "interaction_source": "user_message",
        }])
        state = self._make_state(
            heartbeat_flags={
                "calendar_day_due": True,
                "user_message_waiting": True,
            },
        )

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="rhythm",
            state=state,
            include_content=True,
            mount_ids=[],
            user_messages=None,
        )

        combined = "\n".join(str(m.get("content") or "") for m in messages)
        assert "READ_DFT_AGENT_EVAL_TASKS_NOW" in combined
        assert any(
            m.get("kind") == "interaction"
            and "READ_DFT_AGENT_EVAL_TASKS_NOW" in str(m.get("content") or "")
            for m in messages
        )
        now_md = (tmp_path / "reaction" / "layers" / "50_now.md").read_text(
            encoding="utf-8")
        assert "READ_DFT_AGENT_EVAL_TASKS_NOW" in now_md

    def test_triple_hit_auto_mounts_stm_body_but_ordinary_hit_stays_candidate(self, tmp_path, monkeypatch):
        import json
        from assembly.context import ContextAssembler
        import paths

        stm_dir = tmp_path / "stm_memory"
        association_dir = tmp_path / "association"
        connection_dir = tmp_path / "connection"
        stm_dir.mkdir()
        association_dir.mkdir()
        connection_dir.mkdir()

        (stm_dir / "keywords.json").write_text(json.dumps({
            "index": {
                "alpha": ["MEM-ALPHA"],
                "beta": ["MEM-BETA"],
                "gamma": ["MEM-GAMMA"],
            }
        }, ensure_ascii=False), encoding="utf-8")
        (association_dir / "assoc_kw_kw.json").write_text(
            json.dumps({"alpha|||beta": 1}, ensure_ascii=False),
            encoding="utf-8",
        )
        (connection_dir / "pending.jsonl").write_text(
            json.dumps({
                "word_a": "alpha",
                "entry_a": "MEM-ALPHA",
                "word_b": "beta",
                "entry_b": "MEM-BETA",
            }, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

        monkeypatch.setattr(paths, "STM_MEMORY_DIR", str(stm_dir))
        monkeypatch.setattr(paths, "KEYWORDS_JSON", str(stm_dir / "keywords.json"))
        monkeypatch.setattr(paths, "ASSOCIATION_SET_DIR", str(association_dir))
        monkeypatch.setattr(paths, "CONNECTION_SET_DIR", str(connection_dir))

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_load_memory_content", lambda ids: f"BODY::{ids}")

        triple_text = assembler._build_high_freq(
            self._make_state(),
            "reaction",
            "interactive",
            include_content=True,
            mount_ids=None,
            current_input_text="alpha",
        )
        ordinary_text = assembler._build_high_freq(
            self._make_state(),
            "reaction",
            "interactive",
            include_content=True,
            mount_ids=None,
            current_input_text="gamma",
        )

        assert "BODY::MEM-BETA" in triple_text
        assert "BODY::MEM-GAMMA" not in ordinary_text

    def test_spec087_high_freq_hides_relation_inverted_index_and_focus_card_moves_to_statusbar(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = [
            {"id": "FMZ", "name": "FMZ", "category": "self", "status": "active"},
            {"id": "TzPz", "name": "TzPz", "category": "ours", "status": "active"},
            {"id": "FMA", "name": "FMA", "category": "them", "status": "active"},
            {"id": "REL-Codex", "name": "Codex", "category": "them", "status": "active"},
        ]
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})
        monkeypatch.setattr(RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(tmp_path / f"{cid}.md"))

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_step_toolbelt_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda: "")

        def fake_keyword_index(source, limit=8):
            if source == "relation":
                return (
                    "## RELATION 倒排索引\n"
                    "- FMZ [1词]\n"
                    "- TzPz [1词]\n"
                    "- FMA [1词]\n"
                    "- Codex [1词]"
                )
            return ""

        monkeypatch.setattr(assembler, "_build_keyword_index", fake_keyword_index)

        text = assembler._build_high_freq(
            self._make_state(),
            "reaction",
            "interactive",
            include_content=True,
            mount_ids=None,
            current_input_text="我是 Codex，继续验证",
            interaction_meta={
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            },
        )

        assert "## RELATION 倒排索引" not in text
        assert "FMZ [1词]" not in text
        assert "TzPz [1词]" not in text
        assert "FMA [1词]" not in text
        assert "## 关系卡" not in text

        statusbar = assembler._build_statusbar_with_relations(
            self._make_state(),
            "interactive",
            current_input_text="我是 Codex，继续验证",
            interaction_meta={
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            },
        )
        assert "## 关系卡" in statusbar
        assert "Codex [present]" in statusbar

    def test_spec088_folded_inverted_index_exposes_index_view_hint(self, tmp_path, monkeypatch):
        import paths
        from assembly.context import ContextAssembler

        ltm_keywords = tmp_path / "ltm_keywords.json"
        ltm_keywords.write_text(
            json.dumps({
                "index": {
                    f"kw{i}": [f"MEM-{i:03d}"]
                    for i in range(10)
                }
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(ltm_keywords))

        assembler = ContextAssembler(context_dir=str(tmp_path))
        text = assembler._build_keyword_index("ltm", limit=8)

        assert "（另有 2 条已折叠" in text
        assert "provider-native index_view(scope=ltm_inverted; offset=8; limit=8)" in text

    def test_spec088_relation_inverted_is_dynamic_hit_set_not_full_registry(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = [
            {
                "id": "REL-Codex",
                "name": "Codex",
                "category": "them",
                "status": "active",
                "tags": ["GPT-5.3-Codex-Spark", "审查"],
                "updated_at": "2026-05-27T10:00:00+08:00",
            },
            {
                "id": "REL-DeepSeek",
                "name": "DeepSeek",
                "category": "them",
                "status": "active",
                "tags": ["审查"],
                "updated_at": "2026-05-27T09:00:00+08:00",
            },
            {
                "id": "REL-TzPz",
                "name": "TzPz",
                "category": "ours",
                "status": "active",
                "tags": ["主人"],
                "updated_at": "2026-05-27T11:00:00+08:00",
            },
        ]
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_step_toolbelt_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")

        text = assembler._build_high_freq(
            self._make_state(),
            "reaction",
            "interactive",
            include_content=True,
            mount_ids=None,
            current_input_text="我是 Codex，继续做 GPT-5.3-Codex-Spark 审查",
            interaction_meta={
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            },
        )

        assert "## 关系倒排索引" in text
        assert "- Codex [" in text
        assert "REL-Codex" not in text
        inverted_section = text.split("## 关系域索引", 1)[0]
        assert "REL-DeepSeek" not in inverted_section

    def test_spec088_relation_domain_uses_four_zones_with_fold_hints(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = []
        for category in ("self", "ours", "them", "orgs"):
            for index in range(9):
                cards.append({
                    "id": f"REL-{category}-{index}",
                    "name": f"{category}-{index}",
                    "category": category,
                    "status": "active",
                    "updated_at": f"2026-05-27T0{index}:00:00+08:00",
                })
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})

        assembler = ContextAssembler(context_dir=str(tmp_path))
        text = assembler._build_relation_domain_index(
            limit=8,
            current_input_text="",
            interaction_meta={},
        )

        for zone in ("self", "ours", "them", "orgs"):
            assert f"### {zone}" in text
            assert f"scope=relation_domain; zone={zone}; offset=8; limit=8" in text

    def test_spec088_setup_relation_mount_does_not_load_content_body(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_load_relation_content", lambda relation_id: "SHOULD_NOT_LOAD")

        text = assembler._build_mounted_content([
            {"type": "relation_summary", "ids": "REL-Codex"},
        ])

        assert "SHOULD_NOT_LOAD" not in text
        assert "（无内容被挂载）" in text

    def test_mounted_memory_content_shows_visible_and_source_rounds(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_memory_meta_visible", lambda meta: True)
        monkeypatch.setattr(
            assembler,
            "_load_memory_content",
            lambda ids: "## MEM-TEST\n正文",
        )
        monkeypatch.setattr(
            assembler,
            "_memory_mount_meta",
            lambda ids: {
                "created_round": 472,
                "last_recalled_round": 473,
            },
        )

        text = assembler._build_mounted_content(
            [{"type": "memory", "ids": "MEM-TEST"}],
            current_round=474,
        )

        assert "当前可见轮次：R000474" in text
        assert "创建轮次：R000472" in text
        assert "最近召回轮次：R000473" in text

    def test_spec089_resident_relation_body_enters_content_with_summary(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = [{
            "id": "REL-Codex",
            "name": "Codex",
            "category": "them",
            "status": "active",
            "summary_resident": True,
            "body_resident": True,
        }]
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})
        monkeypatch.setattr(RelationStore, "get_card_path",
                           lambda self, cid, cat="them": str(tmp_path / f"{cid}.md"))
        (tmp_path / "REL-Codex.md").write_text(
            "# Codex\n\n## 现在\n我们正在审查 UPSP。\n",
            encoding="utf-8",
        )

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_inverted_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_relation_domain_index", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_step_toolbelt_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda: "")

        text = assembler._build_high_freq(
            self._make_state(),
            "reaction",
            "interactive",
            include_content=True,
            mount_ids=None,
        )

        assert "### 关系卡 Codex" in text
        assert "REL-Codex" not in text
        assert "我们正在审查 UPSP" in text
        assert "## 关系卡" in text
        assert "Codex" in text

    def test_spec087_association_index_does_not_show_keyword_fallback_without_memory_entries(self, tmp_path, monkeypatch):
        import json
        from assembly.context import ContextAssembler
        import paths

        association_dir = tmp_path / "association"
        connection_dir = tmp_path / "connection"
        stm_dir = tmp_path / "stm"
        ltm_keywords = tmp_path / "ltm_keywords.json"
        association_dir.mkdir()
        connection_dir.mkdir()
        stm_dir.mkdir()
        (association_dir / "assoc_kw_kw.json").write_text(
            json.dumps({"alpha|||beta": 3}, ensure_ascii=False),
            encoding="utf-8",
        )
        (stm_dir / "keywords.json").write_text(
            json.dumps({"index": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        ltm_keywords.write_text(
            json.dumps({"index": {}}, ensure_ascii=False),
            encoding="utf-8",
        )

        monkeypatch.setattr(paths, "ASSOCIATION_SET_DIR", str(association_dir))
        monkeypatch.setattr(paths, "CONNECTION_SET_DIR", str(connection_dir))
        monkeypatch.setattr(paths, "STM_MEMORY_DIR", str(stm_dir))
        monkeypatch.setattr(paths, "KEYWORDS_JSON", str(stm_dir / "keywords.json"))
        monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(ltm_keywords))

        text = ContextAssembler(context_dir=str(tmp_path))._build_association_index(
            limit=8,
            input_keywords=["alpha"],
        )

        assert "关联分=" not in text
        assert "beta (关联分=3)" not in text
        assert "无高置信记忆条目" in text

    def test_spec087_association_index_projects_memory_entries_only(self, tmp_path, monkeypatch):
        import json
        from assembly.context import ContextAssembler
        import paths

        association_dir = tmp_path / "association"
        connection_dir = tmp_path / "connection"
        stm_dir = tmp_path / "stm"
        ltm_dir = tmp_path / "ltm"
        skills_dir = tmp_path / "Skills"
        relation_dir = tmp_path / "relation" / "_index"
        for directory in (association_dir, connection_dir, stm_dir, ltm_dir, skills_dir, relation_dir):
            directory.mkdir(parents=True)

        (association_dir / "assoc_kw_kw.json").write_text(
            json.dumps({
                "alpha|||beta": 2,
                "alpha|||gamma": 1,
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        (connection_dir / "pending.jsonl").write_text(
            json.dumps({
                "word_a": "alpha",
                "entry_a": "MEM-ALPHA",
                "word_b": "beta",
                "entry_b": "MEM-BETA",
                "round_id": "87",
                "timestamp": "2026-05-28T00:00:00+08:00",
            }, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        (stm_dir / "keywords.json").write_text(
            json.dumps({
                "index": {
                    "beta": ["MEM-BETA"],
                    "gamma": ["MEM-GAMMA"],
                }
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        (stm_dir / "meta.json").write_text(
            json.dumps({
                "MEM-BETA": {"title": "Beta 记忆"},
                "MEM-GAMMA": {"title": "Gamma 记忆"},
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        (ltm_dir / "keywords.json").write_text(
            json.dumps({"index": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        (skills_dir / "keywords.json").write_text(
            json.dumps({"index": {"beta": ["SKILL-BETA"]}}, ensure_ascii=False),
            encoding="utf-8",
        )
        (relation_dir / "keywords.json").write_text(
            json.dumps({"index": {"beta": ["Codex"]}}, ensure_ascii=False),
            encoding="utf-8",
        )

        monkeypatch.setattr(paths, "ASSOCIATION_SET_DIR", str(association_dir))
        monkeypatch.setattr(paths, "CONNECTION_SET_DIR", str(connection_dir))
        monkeypatch.setattr(paths, "STM_MEMORY_DIR", str(stm_dir))
        monkeypatch.setattr(paths, "KEYWORDS_JSON", str(stm_dir / "keywords.json"))
        monkeypatch.setattr(paths, "LTM_DIR", str(ltm_dir))
        monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(ltm_dir / "keywords.json"))

        text = ContextAssembler(context_dir=str(tmp_path))._build_association_index(
            limit=8,
            input_keywords=["alpha"],
        )

        assert "MEM-BETA" in text
        assert "Beta 记忆" in text
        assert "MEM-GAMMA" in text
        assert text.index("MEM-BETA") < text.index("MEM-GAMMA")
        assert "SKILL-BETA" not in text
        assert "Codex" not in text
        assert "关联分=" not in text

    def test_ltm_heat_index_skips_meta_comment_entries(self, tmp_path, monkeypatch):
        """LTM meta.json 中的 _comment 不应导致索引装配失败"""
        from assembly.context import ContextAssembler
        import paths

        full_meta = tmp_path / "full_meta.json"
        summary_meta = tmp_path / "summary_meta.json"
        abstract_meta = tmp_path / "abstract_meta.json"
        pinned_meta = tmp_path / "pinned_meta.json"
        full_meta.write_text(json.dumps({
            "_comment": "LTM Full 元数据",
            "MEM-ABCDEF12": {
                "id": "MEM-ABCDEF12",
                "type": "F",
                "title": "索引装配验证",
                "last_recalled_round": 12,
            },
            "MEM-00000001": {
                "id": "MEM-00000001",
                "type": "A",
                "title": "无调用轮兜底",
                "last_recalled_round": None,
            },
        }, ensure_ascii=False), encoding="utf-8")
        for path in (summary_meta, abstract_meta, pinned_meta):
            path.write_text(json.dumps({"_comment": "empty"}, ensure_ascii=False), encoding="utf-8")

        monkeypatch.setattr(paths, "LTM_FULL_META_JSON", str(full_meta))
        monkeypatch.setattr(paths, "LTM_SUMMARY_META_JSON", str(summary_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(abstract_meta))
        monkeypatch.setattr(paths, "LTM_PINNED_META_JSON", str(pinned_meta))

        result = ContextAssembler()._build_ltm_heat_index()

        assert "LTM 索引读取失败" not in result
        assert "MEM-ABCDEF12" in result
        assert "索引装配验证" in result

    def test_spec220_ltm_heat_index_id_can_be_read_by_memory_store(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data import memory_store as ms
        import paths

        summary_dir = tmp_path / "LTM" / "Memory" / "Summary"
        summary_dir.mkdir(parents=True)
        summary_meta = summary_dir / "meta.json"
        summary_md = summary_dir / "summary.md"
        empty_full_meta = tmp_path / "full_meta.json"
        empty_abstract_meta = tmp_path / "abstract_meta.json"
        empty_pinned_meta = tmp_path / "pinned_meta.json"
        for path in (empty_full_meta, empty_abstract_meta, empty_pinned_meta):
            path.write_text(json.dumps({"_comment": "empty"}, ensure_ascii=False), encoding="utf-8")

        mem_id = "MEM-0ABC2205"
        summary_meta.write_text(json.dumps({
            mem_id: {
                "id": mem_id,
                "type": "S",
                "title": "索引正文闭环",
                "access": "public",
                "subject": "Codex",
                "last_recalled_round": 220,
            }
        }, ensure_ascii=False), encoding="utf-8")
        summary_md.write_text(
            "\n## MEM-0ABC2205  [S]  权重3\n"
            "标题：索引正文闭环\n"
            "摘要：index_view 可见的 LTM Summary 正文可被读取。\n",
            encoding="utf-8",
        )

        monkeypatch.setattr(paths, "LTM_FULL_META_JSON", str(empty_full_meta))
        monkeypatch.setattr(paths, "LTM_SUMMARY_META_JSON", str(summary_meta))
        monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(empty_abstract_meta))
        monkeypatch.setattr(paths, "LTM_PINNED_META_JSON", str(empty_pinned_meta))
        monkeypatch.setattr(ms, "MEMORY_MD", str(tmp_path / "stm_memory.md"))
        monkeypatch.setattr(ms, "META_JSON", str(tmp_path / "stm_meta.json"))
        monkeypatch.setattr(ms, "INDEX_MD", str(tmp_path / "stm_index.md"))
        monkeypatch.setattr(ms, "LTM_SUMMARY_SUMMARY_MD", str(summary_md), raising=False)
        monkeypatch.setattr(ms, "LTM_SUMMARY_META_JSON", str(summary_meta), raising=False)

        index_text = ContextAssembler().build_index_view("ltm_heat", limit=4)["content"]
        read_result = ms.MemoryStore().read_body_by_id(mem_id, max_chars=200)

        assert mem_id in index_text
        assert "索引正文闭环" in index_text
        assert read_result["memory_layer"] == "LTM/Summary"
        assert "可被读取" in read_result["body"]

    def test_assemble_setup_returns_system_and_messages(self, tmp_path, monkeypatch):
        from assembly import context as ctx
        monkeypatch.setattr(ctx, "CORE_MD", str(tmp_path / "core.md"))
        monkeypatch.setattr(ctx, "DREAMS_MD", str(tmp_path / "dreams.md"))

        # Mock data layer stores（assembly 现在走 data 层读文件）
        from data.container_store import ContainerStore
        from data.relation_store import RelationStore
        from data.memory_heat import MemoryHeat
        from data.memory_store import MemoryStore
        monkeypatch.setattr(ContainerStore, "load_registry",
                          lambda self: {"containers": [{"prefix": "DC-001", "name": "测试", "status": "open"}]})
        monkeypatch.setattr(RelationStore, "load_registry",
                          lambda self: {"cards": []})
        monkeypatch.setattr(MemoryHeat, "load_heat", lambda self: {"entries": {}})
        monkeypatch.setattr(MemoryStore, "load_meta", lambda self: {})

        # 创建一个假的 core.md
        with open(tmp_path / "core.md", "w", encoding="utf-8") as f:
            f.write("PID：FMZ\n中文名：零号广播员\n位格编码：SCVARB\n## 6. 位格自述\n测试自述")

        assembler = ctx.ContextAssembler(context_dir=str(tmp_path / "context"))
        state = self._make_state()
        system, messages = assembler.assemble_setup(state, "interactive")
        rendered = (tmp_path / "context" / "setup" / "step.md").read_text(encoding="utf-8")

        assert system == ""
        assert "位格核心" in rendered
        assert "永固层" in rendered or "位格" in rendered
        assert isinstance(messages, list)

    def test_assemble_cleanup(self, tmp_path, monkeypatch):
        from assembly import context as ctx
        monkeypatch.setattr(ctx, "CORE_MD", str(tmp_path / "core.md"))
        monkeypatch.setattr(ctx, "DREAMS_MD", str(tmp_path / "dreams.md"))

        from data.container_store import ContainerStore
        from data.relation_store import RelationStore
        from data.memory_heat import MemoryHeat
        from data.memory_store import MemoryStore
        monkeypatch.setattr(ContainerStore, "load_registry", lambda self: {"containers": []})
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": []})
        monkeypatch.setattr(MemoryHeat, "load_heat", lambda self: {"entries": {}})
        monkeypatch.setattr(MemoryStore, "load_meta", lambda self: {})

        with open(tmp_path / "core.md", "w", encoding="utf-8") as f:
            f.write("PID：FMZ\n位格编码：SCVARB\n## 6. 位格自述\n测试")

        from data.context_store import ContextStore
        store = ContextStore(
            cache_dir=str(tmp_path / "cache"),
            raw_log_jsonl=str(tmp_path / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "raw_log.md"),
        )
        store.append_cleanup_round_material(
            1,
            "本轮善后临时材料：测试回复内容",
        )

        assembler = ctx.ContextAssembler(
            context_dir=str(tmp_path / "context"),
            context_store=store,
        )
        state = self._make_state()
        result = {"response": "测试回复内容"}
        system, messages = assembler.assemble_cleanup(state, "interactive", result)
        rendered = (tmp_path / "context" / "cleanup" / "step.md").read_text(encoding="utf-8")

        assert system == ""
        assert isinstance(messages, list)
        assert any("测试回复内容" in m.get("content", "") for m in messages)
        assert sum(
            "测试回复内容" in m.get("content", "")
            for m in messages
        ) == 1
        assert "<!-- 当前缓存 now -->" not in rendered
        assert "<!-- 内部交接层 -->" not in rendered
        manifest = json.loads(
            (tmp_path / "context" / "cleanup" / "manifest.json").read_text(encoding="utf-8"))
        assert manifest["layers"]["now"]["chars"] > 0
        permanent = (tmp_path / "context" / "cleanup" / "layers" / "10_permanent.md").read_text(
            encoding="utf-8")
        popup = (tmp_path / "context" / "cleanup" / "layers" / "99_popup.md").read_text(
            encoding="utf-8")
        assert "<!-- [OUTPUT_FORMAT:cleanup] -->" not in permanent
        assert "## GUIDE｜指南" in popup
        assert "### 善后步指南" in popup
        assert "<!-- [STEP_GUIDE:cleanup] -->" not in popup
        assert "## HANDOFF｜交接" not in popup
        assert "当前是善后步。" in popup
        assert "裸文本是非法输出" in popup
        assert "cleanup_finalize" in popup
        assert "connection_material_settle:" not in popup
        assert "tacit_material_settle:" not in popup
        assert "cache_compact:" not in popup
        assert "你的任务：填写善后步处理清单" not in popup
        assert "| 词A | 条目A | 词B | 条目B | 说明 |" not in popup
        assert "| 预选项 | 类型 | 动作 | 说明 | 证据 | 丢弃原因 |" not in popup
        assert "| source_block_ids | action | replacement_text | reason |" not in popup
        assert "只输出上述表格" not in popup
        assert "### 4. 最小承诺" not in rendered
        assert "### 5. 成品输出" not in rendered
        assert "训练材料整理" in rendered
        assert "cleanup_finalize" in rendered
        assert "connection_material_settle:" not in rendered
        assert "tacit_material_settle:" not in rendered

    def test_spec276_cleanup_does_not_mount_training_material_evidence_package(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_cleanup(
            {"base": {"context_cache": {}}},
            "interactive",
            {
                "response": "收到",
                "_preselection_evidence": [{
                    "item_id": "PRJ-SETUP",
                    "item_type": "container",
                    "origin": "setup_preselection",
                    "selection_trigger": "project_focus",
                    "surface": "PRJ-SETUP",
                    "reaction_adoption_signals": [],
                    "evidence_refs": ["setup_mount:PRJ-SETUP"],
                    "privacy_scope": "runtime_visible",
                }, {
                    "item_id": "—",
                    "item_type": "container",
                    "origin": "setup_preselection",
                    "selection_trigger": "setup_mount",
                    "surface": "—",
                    "reaction_adoption_signals": [],
                    "evidence_refs": ["setup_mount:—"],
                    "privacy_scope": "runtime_visible",
                }],
                "_mounted_memories": ["MEM-SETUP"],
                "_memory_write_receipts": [{
                    "status": "applied",
                    "mem_id": "MEM-NEW",
                    "title": "新增记忆",
                    "keywords": ["新增"],
                }],
                "_memory_content_read_receipts": [{
                    "tool_id": "memory_content_read",
                    "status": "accepted",
                    "mem_id": "MEM-READ",
                }],
                "_reaction_internal_handoff": "需要沿用 MEM-SETUP，并补看 MEM-HANDOFF",
            },
        )
        combined = "\n".join(m.get("content", "") for m in messages)
        material_messages = [
            m for m in messages
            if m.get("kind") in (None, "", "material")
        ]
        evidence_messages = [
            m for m in messages
            if m.get("kind") == "training_evidence"
        ]
        now_layer = (
            tmp_path / "context" / "cleanup" / "layers" / "50_now.md"
        ).read_text(encoding="utf-8")

        assert "训练材料证据包" not in combined
        assert len(evidence_messages) == 0
        assert all(
            "训练材料证据包" not in m.get("content", "")
            for m in material_messages
        )
        assert "训练材料证据包" not in now_layer
        assert "preselection_evidence" not in combined
        assert "connection_candidate_entries" not in combined
        assert "added_prework_traces" not in combined

    def test_spec276_final_reply_handoff_is_retired_from_model_context(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永久")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_reaction(
            {"base": {"context_cache": {}}},
            "interactive",
            internal_handoff=[{
                "role": "user",
                "kind": "final_reply_handoff",
                "content": "只给最终回复调用看的交接。",
            }],
        )

        combined = "\n".join(m.get("content", "") for m in messages)
        now_layer = (
            tmp_path / "context" / "reaction" / "layers" / "50_now.md"
        ).read_text(encoding="utf-8")

        assert "只给最终回复调用看的交接" not in combined
        assert "只给最终回复调用看的交接" not in now_layer

    def test_internal_handoff_is_ignored_without_received_handoff_popup(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        assembler.assemble_cleanup(
            {"base": {"context_cache": {}}},
            "interactive",
            {"response": "收到"},
            internal_handoff=[{
                "role": "user",
                "kind": "handoff",
                "content": "交接：请善后步结算终端证据",
            }],
        )

        popup = (tmp_path / "context" / "cleanup" / "layers" / "99_popup.md").read_text(
            encoding="utf-8")
        now = (tmp_path / "context" / "cleanup" / "layers" / "50_now.md").read_text(
            encoding="utf-8")

        assert "### 待处理交接" not in popup
        assert "received_handoff" not in popup
        assert "交接：请善后步结算终端证据" not in popup
        assert "kind:" not in popup
        assert "tier:" not in popup
        assert "交接：请善后步结算终端证据" not in now
        assert "【Runtime 调用占位】" in now
        assert "这不是用户原始输入" in now
        assert "请根据上下文继续本次调用。" in now

    def test_reaction_result_output_template_moves_to_popup(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        system, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        permanent = (tmp_path / "context" / "reaction" / "layers" / "10_permanent.md").read_text(
            encoding="utf-8")
        popup = (tmp_path / "context" / "reaction" / "layers" / "99_popup.md").read_text(
            encoding="utf-8")
        combined = "\n".join(m.get("content", "") for m in messages)

        assert system == ""
        assert "<!-- [OUTPUT_FORMAT:reaction_result] -->" not in permanent
        assert "<!-- [STEP_GUIDE:reaction_result] -->" not in popup
        assert "### 反应步终端字段说明" not in popup
        assert "reaction_loop_done" not in popup
        assert "to_next_reaction_iter" not in popup
        assert "tool_request | file_read" not in popup
        assert "### 反应循环指南" in combined
        assert "assistant_text" in combined
        assert "reaction_progress_emit" not in combined
        assert "reaction_finalize" in combined
        reaction_guide = assembler._extract_schema_section("REACTION_RESULT_FORMAT")
        assert "反应步：推理、工具调用、生成回复" in reaction_guide
        assert "assistant_text" in reaction_guide
        assert "无阻断时，Runtime 将无工具自然语言派生为 `finish`" in reaction_guide
        assert "只有需要跨轮继续时才调用 `reaction_finalize(handoff_text)`" in reaction_guide
        assert "container_focus_table" not in popup
        assert "memory_write_declaration" not in popup
        assert "relation_card_declaration" not in popup

    def test_spec363_open_relay_pool_projects_into_reaction_popup(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        state = self._make_state(
            heartbeat_flags={"user_message_waiting": True},
            runtime={
                "phase": "idle",
                "standby_countdown": 0,
                "relay_intents": [{
                    "relay_intent_id": "RLY-R000040-N001",
                    "status": "open",
                    "source_round": 40,
                    "handoff_text": "继续读书。",
                }],
            },
        )

        _, messages = assembler.assemble_reaction(state, "interactive")

        step_md = (tmp_path / "context" / "reaction" / "step.md").read_text(
            encoding="utf-8")
        combined = "\n".join(m.get("content", "") for m in messages)
        assert "REMINDER｜中继规划池" in combined
        assert "RLY-R000040-N001" in combined
        assert "继续读书。" not in step_md
        assert "具体事务以当前中继目标卡" in step_md

    def test_reaction_tool_index_moves_to_high_freq(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        combined = "\n".join(m.get("content", "") for m in messages)
        permanent = (tmp_path / "context" / "reaction" / "layers" / "10_permanent.md").read_text(
            encoding="utf-8")
        high_freq = (tmp_path / "context" / "reaction" / "layers" / "40_high_freq.md").read_text(
            encoding="utf-8")

        assert "<!-- [PROTOCOL_TOOLS:index] -->" not in permanent
        assert "<!-- [GENERAL_TOOLS:index] -->" not in permanent
        assert "<!-- [PROTOCOL_TOOLS:index] -->" in high_freq
        protocol_index = high_freq.split("<!-- [PROTOCOL_TOOLS:index] -->", 1)[1].split(
            "<!-- [GENERAL_TOOLS:index] -->", 1
        )[0]
        assert "memory_write" in protocol_index
        assert "relation_card_write" in protocol_index
        assert "relation_read" in protocol_index
        assert "index_view" in protocol_index
        assert "relation_card_read" not in protocol_index
        assert "relation_content_read" not in protocol_index
        assert "state_update" not in protocol_index
        assert "| tool_id | 姿态 | 领域 | 何时请求 | guide/边界提示 |" in protocol_index
        assert "| tool_family |" not in protocol_index
        assert "| handler |" not in protocol_index
        assert "| result_kind |" not in protocol_index
        assert "general_tool" not in protocol_index
        assert "substrate_tool" not in protocol_index
        assert "| tool_class |" not in protocol_index
        assert "<!-- [GENERAL_TOOLS:index] -->" in high_freq
        general_index = high_freq.split("<!-- [GENERAL_TOOLS:index] -->", 1)[1]
        assert "| tool_id | 姿态 | 领域 | 何时请求 | guide/边界提示 |" in general_index
        assert "file_read" in general_index
        assert "general_tool_result" in general_index
        assert "protocol_tool_receipt" not in general_index
        assert "| backend_type |" not in general_index
        assert "| permission_scope |" not in general_index
        assert "web_fetch" in general_index
        assert "web_search" in general_index
        assert "### 工具唤醒提醒" not in combined
        assert "### 协议工具提醒" not in combined

    def test_setup_and_cleanup_toolbelts_are_high_freq_step_scoped(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "persona core")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "")
        monkeypatch.setattr(assembler, "_build_ltm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_stm_heat_index", lambda **kwargs: "")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_workbench_focus_projection", lambda: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        assembler.assemble_setup(self._make_state(), "interactive")
        setup_permanent = (tmp_path / "context" / "setup" / "layers" / "10_permanent.md").read_text(
            encoding="utf-8")
        setup_high_freq = (tmp_path / "context" / "setup" / "layers" / "40_high_freq.md").read_text(
            encoding="utf-8")
        setup_popup = (tmp_path / "context" / "setup" / "layers" / "99_popup.md").read_text(
            encoding="utf-8")

        assert "<!-- [STEP_TOOLBELT:setup] -->" not in setup_permanent
        assert "<!-- [STEP_TOOLBELT:setup] -->" in setup_high_freq
        assert "setup_mount_apply" in setup_high_freq
        assert "setup_security_gate" in setup_high_freq
        assert "setup_handoff" in setup_high_freq
        assert "reaction_loop" not in setup_high_freq
        assert "### 起手步指南" in setup_popup
        assert "<!-- [STEP_GUIDE:setup] -->" not in setup_popup

        assembler.assemble_cleanup(
            self._make_state(),
            "interactive",
            {"response": "ok"},
        )
        cleanup_permanent = (tmp_path / "context" / "cleanup" / "layers" / "10_permanent.md").read_text(
            encoding="utf-8")
        cleanup_high_freq = (tmp_path / "context" / "cleanup" / "layers" / "40_high_freq.md").read_text(
            encoding="utf-8")
        cleanup_popup = (tmp_path / "context" / "cleanup" / "layers" / "99_popup.md").read_text(
            encoding="utf-8")

        assert "<!-- [STEP_TOOLBELT:cleanup] -->" not in cleanup_permanent
        assert "<!-- [STEP_TOOLBELT:cleanup] -->" in cleanup_high_freq
        assert "connection_material_settle" in cleanup_high_freq
        assert "tacit_material_settle" in cleanup_high_freq
        assert "cache_compact" in cleanup_high_freq
        assert "cleanup_handoff" in cleanup_high_freq
        assert "tool_request_card" not in cleanup_popup
        assert "### 善后步指南" in cleanup_popup
        assert "<!-- [STEP_GUIDE:cleanup] -->" not in cleanup_popup

    def test_spec077_reaction_mounts_workbench_focus_projection(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.workbench import WorkbenchStore
        from data.container_store import ContainerStore

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_container_index", lambda: "## 容器索引")
        monkeypatch.setattr(
            assembler, "_build_ltm_heat_index", lambda **kwargs: "## LTM 索引")
        monkeypatch.setattr(
            assembler, "_build_stm_heat_index", lambda **kwargs: "## STM 索引")
        monkeypatch.setattr(assembler, "_build_keyword_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_association_index", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(
            WorkbenchStore,
            "load_status",
            lambda self: {"base": {"focus": "PRJ-20260525-01", "old_focus": None}},
        )
        monkeypatch.setattr(
            ContainerStore,
            "read_focus_projection",
            lambda self, cid: {
                "container_id": cid,
                "container_type": "PRJ",
                "status": "active",
                "title": "Spec 077",
                "allowed_targets": ["plan.md", "notes.md"],
                "default_target": "plan.md",
                "content": "已有计划片段",
                "format_hint": "memory_container_write via visible WB focus",
            },
        )

        _, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        combined = "\n".join(m.get("content", "") for m in messages)

        assert "WB 焦点投影" in combined
        assert "PRJ-20260525-01" in combined
        assert "plan.md" in combined
        assert "已有计划片段" in combined
        assert "memory_container_write" in combined
        assert "CONTAINER_FOCUS_CONTENT_START" not in combined
        assert "container_focus_table" not in combined

    def test_general_tool_reminder_is_not_default_popup_injected(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        popup = messages[-1]["content"]
        rendered = (tmp_path / "context" / "reaction" / "step.md").read_text(encoding="utf-8")

        assert "<!-- POPUP" in popup
        assert "### 工具唤醒提醒" not in popup
        assert "tool_request | file_read" not in popup
        assert "工具指南：" not in popup
        assert "### 反应循环指南" in popup
        assert "assistant_text" in popup
        assert "reaction_progress_emit" not in popup
        assert "reaction_finalize" in popup
        reaction_guide = assembler._extract_schema_section("REACTION_RESULT_FORMAT")
        assert "反应步：推理、工具调用、生成回复" in reaction_guide
        assert "无阻断时，Runtime 将无工具自然语言派生为 `finish`" in reaction_guide
        assert "只有需要跨轮继续时才调用 `reaction_finalize(handoff_text)`" in reaction_guide
        assert "### 工具唤醒提醒" not in rendered

    def test_spec380_reaction_closeout_step_md_uses_closeout_channel_only(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            reaction_loop_phase="closeout",
            general_tool_guides=["file_read"],
            protocol_tool_guides=["memory_write"],
        )
        popup = messages[-1]["content"]
        rendered = (tmp_path / "context" / "reaction" / "step.md").read_text(
            encoding="utf-8")

        assert "### 反应步指南" in popup
        assert "当前是反应步循环" in popup
        assert "裸文本是非法输出" not in popup
        assert "reaction_finalize" in popup
        assert "assistant_text" in popup
        assert "reaction_progress_emit" not in popup
        assert "### 工具指南：`file_read`" in popup
        assert "反应步有四条车道" not in rendered

    def test_spec408_runtime_does_not_generate_project_route_popup_from_input(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_setup(
            self._make_state(activity_mode="工程"),
            "interactive",
            user_messages=["实现 Spec293，补 PRJ 项目容器路由并跑 pytest 验收。"],
        )
        popup = messages[-1]["content"]

        assert "项目容器路由提醒" not in popup
        assert "PRJ 记录项目目标、阶段、计划、交付物、验收和剩余工作" not in popup
        assert "EC 记录已经发生的事件经过" not in popup
        assert "logic/mode_router" not in popup
        assert "expected_container_focus" not in popup
        assert "activity_mode" not in popup

    def test_requested_general_tool_guide_is_popup_injected(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "persona core")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            general_tool_guides=["file_read"],
        )
        popup = messages[-1]["content"]

        assert "<!-- POPUP" in popup
        assert "## GUIDE｜指南" in popup
        assert "### 工具指南：`file_read`" in popup
        assert "tool_id:" not in popup
        assert "file_read" in popup
        guide = popup.split("### 工具指南：`file_read`", 1)[1]
        assert "protocol_tool_submission" not in guide

    def test_requested_web_general_tool_guides_are_popup_injected(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "persona core")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            general_tool_guides=["web_fetch", "web_search"],
        )
        popup = messages[-1]["content"]

        assert popup.count("### 工具指南：") == 2
        assert "### 工具指南：`web_fetch`" in popup
        assert "### 工具指南：`web_search`" in popup
        assert "tool_id:" not in popup
        assert "web_fetch" in popup
        assert "web_search" in popup
        guide = popup.split("### 工具指南：`web_fetch`", 1)[1]
        assert "protocol_tool_submission" not in guide

    def test_requested_file_edit_general_tool_guide_is_popup_injected(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "persona core")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            general_tool_guides=["file_edit"],
        )
        popup = messages[-1]["content"]

        assert "### 工具指南：`file_edit`" in popup
        assert "tool_id:" not in popup
        assert "文件编辑通用工具" in popup
        assert "patch" in popup
        guide = popup.split("### 工具指南：`file_edit`", 1)[1]
        assert "protocol_tool_submission" not in guide

    def test_requested_shell_command_general_tool_guide_is_popup_injected(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "persona core")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            general_tool_guides=["shell_command"],
        )
        popup = messages[-1]["content"]

        assert "### 工具指南：`shell_command`" in popup
        assert "tool_id:" not in popup
        assert "shell 命令通用工具" in popup
        assert "timeout_ms" in popup
        assert "cmd.exe" in popup
        assert "powershell -NoProfile -Command" in popup
        guide = popup.split("### 工具指南：`shell_command`", 1)[1]
        assert "protocol_tool_submission" not in guide

    def test_spec275_sandbox_grant_guide_is_popup_injected(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from logic.sandbox_grant import SANDBOX_GRANT_ENV

        task_root = tmp_path / "sandbox-task"
        task_root.mkdir()
        monkeypatch.setenv(
            SANDBOX_GRANT_ENV,
            json.dumps(
                {
                    "phase": "engineering",
                    "task_root": str(task_root),
                    "read_paths": [str(task_root)],
                    "write_paths": [str(task_root)],
                    "shell_cwd": str(task_root),
                    "allowed_tools": ["file_read", "file_edit", "shell_command"],
                    "validation_commands": ["python -m pytest -q"],
                },
                ensure_ascii=False,
            ),
        )
        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "persona core")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            general_tool_guides=["file_edit", "shell_command"],
        )
        popup = messages[-1]["content"]

        assert "工程任务 Sandbox 授权" in popup
        assert "phase=engineering" in popup
        assert f"task_root={task_root.resolve()}" in popup
        assert "allowed_tools=file_read, file_edit, shell_command" in popup
        assert "validation_commands:" in popup
        assert "python -m pytest -q" in popup

    def test_requested_subagent_dispatch_general_tool_guide_is_popup_injected(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "persona core")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            general_tool_guides=["subagent_dispatch"],
        )
        popup = messages[-1]["content"]

        assert "### 工具指南：`subagent_dispatch`" in popup
        assert "tool_id:" not in popup
        assert "子 agent 调度通用工具" in popup
        assert "write_scope" in popup
        guide = popup.split("### 工具指南：`subagent_dispatch`", 1)[1]
        assert "protocol_tool_submission" not in guide

    def test_requested_protocol_tool_guide_is_popup_injected(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            protocol_tool_guides=["memory_link_update"],
        )
        popup = messages[-1]["content"]

        assert "<!-- POPUP" in popup
        assert "### 工具指南：`memory_link_update`" in popup
        assert "tool_id:" not in popup
        assert "记忆关联历史修复工具" in popup
        assert "current_overview" in popup
        assert "memory_write 感受词清单（仅词条，不含数值）" not in popup
        assert "### 交互感受词" not in popup
        assert "### 关系感受词" not in popup

    def test_memory_write_protocol_tool_guide_request_is_retired(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            protocol_tool_guides=["memory_write"],
        )
        popup = messages[-1]["content"]

        assert "### 反应步指南" in popup
        assert "### 工具指南：`memory_write`" not in popup
        assert "memory_write 感受词清单（仅词条，不含数值）" not in popup
        assert "### 交互感受词" not in popup
        assert "### 关系感受词" not in popup

    def test_non_memory_protocol_tool_guide_does_not_include_feeling_words(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            protocol_tool_guides=["relation_read"],
        )
        popup = messages[-1]["content"]

        assert "### 工具指南：`relation_read`" in popup
        assert "tool_id:" not in popup
        assert "memory_write 感受词清单（仅词条，不含数值）" not in popup
        assert "### 交互感受词" not in popup
        assert "### 关系感受词" not in popup

    def test_setup_and_reaction_loop_popups_are_before_warning(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: (
            "- kind: structure_warning\n"
            "  decision_required: false\n"
            "  message: 结构复核。"
        ))

        _, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        popup = messages[-1]["content"]

        assert "<!-- POPUP" in popup
        assert "### 反应循环指南" in popup
        assert "assistant_progress" not in popup
        assert "reaction_loop_done" not in popup
        assert "to_next_reaction_iter" not in popup
        assert "internal_handoff_route" not in popup
        assert "### 协议工具提醒" not in popup
        assert "### 工具唤醒提醒" not in popup
        assert popup.index("### 反应循环指南") < popup.index(
            "### 结构警告")

        _, setup_messages = assembler.assemble_setup(self._make_state(), "interactive")
        setup_popup = setup_messages[-1]["content"]
        setup_combined = "\n\n".join(m.get("content", "") for m in setup_messages)
        assert "### 起手步指南" in setup_popup
        assert "setup_mount_apply:" not in setup_popup
        assert "setup_security_gate:" not in setup_popup
        assert "setup_handoff:" not in setup_popup
        assert "起手步不执行用户任务" in setup_popup
        assert "不得读取材料、创建任务账本、写产物或运行命令" in setup_popup
        assert "真实读取、建账、写产物和验收登记都从反应步开始" in setup_popup
        assert "| 字段 | 类型 | 脚本预选 | 你的动作 | 说明 |" not in setup_popup
        assert "只输出上述表格" not in setup_popup
        assert "### 待命起手指南" not in setup_popup
        assert "standby_skip_reaction" not in setup_popup
        assert "standby_reaction_hint" not in setup_popup
        assert "standby_skip_reaction" not in setup_combined
        assert "standby_reaction_hint" not in setup_combined

        _, standby_messages = assembler.assemble_setup(self._make_state(), "standby")
        standby_popup = standby_messages[-1]["content"]
        standby_combined = "\n\n".join(
            m.get("content", "") for m in standby_messages)
        assert "### 待命起手指南" in standby_popup
        assert "当前是待命起手步。" in standby_popup
        assert "standby_skip_reaction" not in standby_popup
        assert "standby_reaction_hint" not in standby_popup
        assert "standby_skip_reaction" not in standby_combined
        assert "standby_reaction_hint" not in standby_combined

    def test_spec379_handoff_popups_use_docs_resident_guides(self, tmp_path):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))

        setup_popup = assembler._build_handoff_popup("setup", "interactive")
        reaction_popup = assembler._build_handoff_popup("reaction", "interactive")
        closeout_popup = assembler._build_handoff_popup(
            "reaction",
            "interactive",
            reaction_loop_phase="closeout",
        )
        final_reply_popup = assembler._build_handoff_popup(
            "reaction",
            "interactive",
            reaction_loop_phase="final_reply",
        )
        cleanup_popup = assembler._build_handoff_popup("cleanup", "interactive")

        assert "当前是起手步。" in setup_popup
        assert "UPSP 是工具驱动系统；裸文本是非法输出。" in setup_popup
        assert "`setup_finalize`" in setup_popup
        assert "起手步不执行用户任务" in setup_popup
        assert "真实读取、建账、写产物和验收登记都从反应步开始" in setup_popup
        assert "当前是反应步循环。" in reaction_popup
        assert "UPSP 是通道驱动系统" in reaction_popup
        assert "继续执行调用合法工具" in reaction_popup
        assert "assistant_text" in reaction_popup
        assert "完成时直接自然语言回复用户" in reaction_popup
        assert "`reaction_finalize(handoff_text)`" in reaction_popup
        assert "当前是反应步循环。" in closeout_popup
        assert "继续执行调用合法工具" in closeout_popup
        assert "裸文本是非法输出" not in closeout_popup
        assert "完成时直接自然语言回复用户" in closeout_popup
        assert "`reaction_finalize(handoff_text)`" in closeout_popup
        assert "当前是最终回复阶段。" in final_reply_popup
        assert "自然语言最终回复" in final_reply_popup
        assert "裸文本是非法输出" not in final_reply_popup
        assert "assistant_text" not in final_reply_popup
        assert "setup_finalize" not in final_reply_popup
        assert "reaction_finalize" not in final_reply_popup
        assert "cleanup_finalize" not in final_reply_popup
        assert "当前是善后步。" in cleanup_popup
        assert "UPSP 是工具驱动系统；裸文本是非法输出。" in cleanup_popup
        assert "`cleanup_finalize`" in cleanup_popup

    def test_spec379_actual_messages_have_four_stage_resident_guides(
            self, tmp_path):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        state = self._make_state()

        _, setup_messages = assembler.assemble_setup(
            state, "interactive", user_messages=["验证四阶段 POPUP 指南"])
        _, reaction_messages = assembler.assemble_reaction(
            state, "interactive")
        _, final_reply_messages = assembler.assemble_reaction(
            state,
            "interactive",
            reaction_loop_phase="final_reply",
        )
        _, cleanup_messages = assembler.assemble_cleanup(
            state, "interactive", {"response": "已完成。"})

        setup_text = "\n".join(m.get("content", "") for m in setup_messages)
        reaction_text = "\n".join(
            m.get("content", "") for m in reaction_messages)
        final_reply_text = "\n".join(
            m.get("content", "") for m in final_reply_messages)
        final_reply_popup = final_reply_messages[-1].get("content", "")
        cleanup_text = "\n".join(
            m.get("content", "") for m in cleanup_messages)

        assert "当前是起手步。" in setup_text
        assert "裸文本是非法输出" in setup_text
        assert "setup_finalize" in setup_text
        assert "当前是反应步循环。" in reaction_text
        assert "assistant_text" in reaction_text
        assert "reaction_progress_emit" not in reaction_text
        assert "reaction_finalize" in reaction_text
        assert "当前是善后步。" in cleanup_text
        assert "裸文本是非法输出" in cleanup_text
        assert "cleanup_finalize" in cleanup_text
        assert "当前是最终回复阶段。" in final_reply_text
        assert "自然语言最终回复" in final_reply_text
        assert "裸文本是非法输出" not in final_reply_text
        assert "assistant_text" not in final_reply_text
        assert "setup_finalize" not in final_reply_popup
        assert "cleanup_finalize" not in final_reply_popup

    def test_popup_policy_orders_default_layers_without_tool_reminder(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: (
            "- kind: structure_warning\n"
            "  tier: warning\n"
            "  decision_required: false\n"
            "  message: 结构复核。"
        ))

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            general_tool_guides=["file_read"],
        )
        popup = messages[-1]["content"]

        assert "### 工具唤醒提醒" not in popup
        assert popup.index("## GUIDE｜指南") < popup.index("## WARNING｜警告")
        assert "## HANDOFF｜交接" not in popup
        assert popup.index("### 工具指南：`file_read`") < popup.index(
            "### 反应循环指南")
        assert popup.index("### 反应循环指南") < popup.index(
            "### 结构警告")

    def test_reaction_popup_always_mounts_reaction_step_guide(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(meta={"total_round": 209, "daily_round": 1}),
            "interactive",
        )
        popup = messages[-1]["content"]

        assert "### 反应步指南" in popup
        assert "### 记忆入口指南" not in popup
        assert "kind: memory_entry_guide" not in popup
        assert "反应步：推理、工具调用、生成回复" in popup
        assert "迭代流程" in popup
        assert "核验与询问" in popup
        assert "稳定事实和纯仓内任务不强制联网" in popup
        assert "实质改变交付结果或授权边界" in popup
        assert "范围最小、可回退的带界假设" in popup
        assert "每轮记忆节奏" in popup
        assert "四容器自觉" in popup
        assert "直接自然语言回复用户" in popup
        assert "reaction_finalize(handoff_text)" in popup
        assert "closeout_decision=continue" not in popup
        assert "handoff_text" in popup
        assert "to_next_reaction" not in popup
        assert "| 权重 | 初始形态 | 适用场景 |" not in popup
        assert "字段纪律" not in popup
        assert "先请求" not in popup
        assert "### 工具唤醒提醒" not in popup
        assert popup.index("### 反应步指南") < popup.index("### 反应循环指南")

    def test_spec351_emergency_guide_replaces_reaction_step_guide_in_artifacts(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        state = self._make_state(
            meta={"total_round": 350, "daily_round": 1},
            heartbeat_flags={
                "api_degraded": True,
                "rhythm_due": True,
                "user_message_waiting": True,
            },
        )
        _, messages = assembler.assemble_reaction(state, "rhythm")
        popup = messages[-1]["content"]
        step_md = (tmp_path / "context" / "reaction" / "step.md").read_text(
            encoding="utf-8")

        assert "紧急处理指南" in popup
        assert "API 异常处理" in popup
        assert "反应步：推理、工具调用、生成回复" not in popup
        assert "主轴节律指南" not in popup
        assert "用户交互" not in popup
        assert "API 异常处理" in step_md
        assert "反应步：推理、工具调用、生成回复" not in step_md

    def test_spec576_static_memory_reminder_uses_reminder_layer(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: (
            "- kind: structure_warning\n"
            "  tier: warning\n"
            "  decision_required: false\n"
            "  message: 结构复核。"
        ))
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [])

        _, messages = assembler.assemble_reaction(
            self._make_state(meta={"total_round": 157, "daily_round": 1}),
            "interactive",
        )
        popup = messages[-1]["content"]

        assert "### 记忆提醒" in popup
        assert "提醒内容：" in popup
        assert "主体更新" in popup
        assert "material/最近缓存承载" in popup
        assert "不是私有笔记或记忆替代" in popup
        assert "weight=1/2" in popup
        assert "用户/任务禁止长期记忆，则不写" in popup
        assert "workaround" not in popup
        assert "memory_write" in popup
        assert "memory_container_create / memory_container_write" not in popup
        assert "已有 MEM-* 后" not in popup
        assert "### 当前证据决策提醒" not in popup
        assert "记忆沉淀近位提醒" not in popup
        assert "### 反应步指南" in popup
        assert "reaction_finalize(handoff_text)" in popup
        assert popup.index("### 反应步指南") < popup.index(
            "### 记忆提醒")
        assert popup.index("### 记忆提醒") < popup.index("### 结构警告")

    def test_spec576_tool_evidence_does_not_generate_dynamic_branch_card(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda step: [{
            "role": "system",
            "kind": "tool_fact",
            "round": 617,
            "content": (
                "[tool_fact]\n"
                "tool_id=memory_write\n"
                "status=applied\n"
                "mem_id=MEM-1050971B"
            ),
        }])
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [{
            "role": "system",
            "kind": "tool_fact",
            "round": 617,
            "content": (
                "[tool_fact]\n"
                "tool_id=memory_write\n"
                "status=applied\n"
                "mem_id=MEM-10597A19"
            ),
        }])

        _, messages = assembler.assemble_reaction(
            self._make_state(meta={"total_round": 617, "daily_round": 1}),
            "interactive",
        )
        popup = messages[-1]["content"]

        assert "### 记忆提醒" in popup
        assert "主体更新" in popup
        assert "稳定变化和可复用判断" in popup
        assert "不抄资料、不写工具流水" in popup
        assert "已有 MEM-* 后" not in popup
        assert "MEM-1050971B、MEM-10597A19" not in popup
        assert "### 当前证据决策提醒" not in popup
        assert "kind: reaction_branch_card" not in popup

    def test_spec576_static_memory_reminder_visible_without_evidence(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [])

        _, messages = assembler.assemble_reaction(
            self._make_state(meta={"total_round": 157, "daily_round": 1}),
            "interactive",
        )
        popup = messages[-1]["content"]

        assert "### 记忆提醒" in popup
        assert "weight=1/2" in popup
        assert "只有 `MEM-*` 回执才算写入成功" in popup
        assert "噪音不写" not in popup
        assert "### 当前证据决策提醒" not in popup
        assert "### 反应循环指南" in popup

    def test_popup_policy_splits_raw_popup_events_before_sorting(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: (
            "- kind: structure_warning\n"
            "  tier: warning\n"
            "  decision_required: false\n"
            "  message: 结构复核。\n"
            "\n"
            "- kind: relation_update_reminder\n"
            "  tier: reminder\n"
            "  decision_required: false\n"
            "  message: 关系提醒。"
        ))

        _, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        popup = messages[-1]["content"]

        assert popup.index("### 反应循环指南") < popup.index(
            "### 关系更新提醒")
        assert popup.index("### 关系更新提醒") < popup.index(
            "### 结构警告")

    def test_spec561_reaction_closeout_phase_uses_normal_loop_popup(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._make_state(),
            "interactive",
            reaction_loop_phase="closeout",
        )
        popup = messages[-1]["content"]

        assert "### 反应循环指南" in popup
        assert "### 反应收束指南" not in popup
        assert "phase:" not in popup
        assert "reaction_finalize(handoff_text)" in popup
        assert "直接自然语言回复用户" in popup
        assert "assistant_progress" not in popup
        assert "to_next_reaction_iter" not in popup

    def test_private_memory_hidden_from_stm_heat_index_when_owner_absent(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.memory_heat import MemoryHeat
        from data.memory_store import MemoryStore

        monkeypatch.setattr(MemoryHeat, "load_heat", lambda self: {
            "entries": {
                "MEM-PUBLIC01": {"H": 90, "zone": "hot"},
                "MEM-PRIVATE1": {"H": 80, "zone": "hot"},
            }
        })
        monkeypatch.setattr(MemoryStore, "load_meta", lambda self: {
            "MEM-PUBLIC01": {
                "id": "MEM-PUBLIC01",
                "title": "Public Memory",
                "access": "public",
            },
            "MEM-PRIVATE1": {
                "id": "MEM-PRIVATE1",
                "title": "Private Memory",
                "access": "private",
                "subject": "FMZ",
            },
        })
        monkeypatch.setattr(MemoryStore, "private_subjects_for_memory",
                            lambda self, mem_id: ["TzPz"] if mem_id == "MEM-PRIVATE1" else [])

        assembler = ContextAssembler()
        hidden = assembler._build_stm_heat_index()
        assembler._current_interaction_meta = {"interaction_object": "TzPz"}
        still_hidden = assembler._build_stm_heat_index()

        assert "Public Memory" in hidden
        assert "Private Memory" not in hidden
        assert "Private Memory" not in still_hidden

    def test_spec221_round_hidden_memory_is_filtered_from_stm_indexes(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.memory_heat import MemoryHeat
        from data.memory_store import MemoryStore
        import paths

        stm_dir = tmp_path / "stm"
        ltm_dir = tmp_path / "ltm"
        association_dir = tmp_path / "association"
        connection_dir = tmp_path / "connection"
        stm_dir.mkdir()
        ltm_dir.mkdir()
        association_dir.mkdir()
        connection_dir.mkdir()

        (stm_dir / "keywords.json").write_text(
            json.dumps({
                "index": {
                    "alpha": ["MEM-HIDDEN22", "MEM-VISIBLE2"],
                }
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        (stm_dir / "meta.json").write_text(
            json.dumps({
                "MEM-HIDDEN22": {
                    "id": "MEM-HIDDEN22",
                    "title": "本轮新写隐藏条目",
                    "access": "public",
                },
                "MEM-VISIBLE2": {
                    "id": "MEM-VISIBLE2",
                    "title": "既有可见条目",
                    "access": "public",
                },
            }, ensure_ascii=False),
            encoding="utf-8",
        )
        (ltm_dir / "keywords.json").write_text(
            json.dumps({"index": {}}, ensure_ascii=False),
            encoding="utf-8",
        )
        (association_dir / "assoc_kw_kw.json").write_text(
            json.dumps({"seed|||alpha": 2}, ensure_ascii=False),
            encoding="utf-8",
        )
        (connection_dir / "pending.jsonl").write_text("", encoding="utf-8")

        monkeypatch.setattr(paths, "STM_MEMORY_DIR", str(stm_dir))
        monkeypatch.setattr(paths, "KEYWORDS_JSON", str(stm_dir / "keywords.json"))
        monkeypatch.setattr(paths, "LTM_DIR", str(ltm_dir))
        monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(ltm_dir / "keywords.json"))
        monkeypatch.setattr(paths, "ASSOCIATION_SET_DIR", str(association_dir))
        monkeypatch.setattr(paths, "CONNECTION_SET_DIR", str(connection_dir))
        monkeypatch.setattr(MemoryHeat, "load_heat", lambda self: {
            "entries": {
                "MEM-HIDDEN22": {"H": 90, "zone": "hot"},
                "MEM-VISIBLE2": {"H": 80, "zone": "warm"},
            }
        })
        monkeypatch.setattr(MemoryStore, "load_meta", lambda self: {
            "MEM-HIDDEN22": {
                "id": "MEM-HIDDEN22",
                "title": "本轮新写隐藏条目",
                "access": "public",
            },
            "MEM-VISIBLE2": {
                "id": "MEM-VISIBLE2",
                "title": "既有可见条目",
                "access": "public",
            },
        })

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        assembler._hidden_stm_memory_ids = {"MEM-HIDDEN22"}

        heat = assembler._build_stm_heat_index()
        inverted = assembler._build_keyword_index("stm", limit=8)
        association = assembler._build_association_index(
            limit=8,
            input_keywords=["seed"],
        )

        assert "本轮新写隐藏条目" not in heat
        assert "本轮新写隐藏条目" not in inverted
        assert "本轮新写隐藏条目" not in association
        assert "既有可见条目" in heat
        assert "既有可见条目" in inverted
        assert "既有可见条目" in association

    def test_private_memory_body_not_loaded_while_feature_is_deferred(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.memory_store import MemoryStore

        def fake_get_meta(_self, mem_id):
            return {
                "id": mem_id,
                "title": "Private Memory",
                "access": "private",
                "subject": ["TzPz"],
            }

        def fake_read_entry(_self, mem_id):
            return f"## {mem_id}\nPrivate Body"

        monkeypatch.setattr(MemoryStore, "get_meta", fake_get_meta)
        monkeypatch.setattr(MemoryStore, "read_entry", fake_read_entry)
        monkeypatch.setattr(MemoryStore, "private_subjects_for_memory",
                            lambda self, mem_id: ["TzPz"])

        assembler = ContextAssembler()
        assert assembler._load_memory_content("MEM-PRIVATE1") == ""

        assembler._current_interaction_meta = {"interaction_object": "TzPz"}
        assert assembler._load_memory_content("MEM-PRIVATE1") == ""

    def test_memory_content_hides_null_annotation_but_keeps_non_empty_annotation(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.memory_store import MemoryStore

        def fake_read_entry(_self, mem_id):
            if mem_id == "MEM-NULL0001":
                return "## MEM-NULL0001\n**标题**：空注释\n注释：null\n正文"
            return "## MEM-NOTE0001\n**标题**：有注释\n注释：旧判断已订正，参见 DC-12\n正文"

        monkeypatch.setattr(MemoryStore, "read_entry", fake_read_entry)
        assembler = ContextAssembler()

        null_content = assembler._load_memory_content("MEM-NULL0001")
        note_content = assembler._load_memory_content("MEM-NOTE0001")

        assert "注释：null" not in null_content
        assert "注释：旧判断已订正，参见 DC-12" in note_content

    def test_reaction_output_template_missing_uses_exit_guide_in_popup(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_load_core_identity", lambda: "位格核心")
        monkeypatch.setattr(assembler, "_load_rules_for_layers", lambda *a, **kw: "")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        def fake_extract(name):
            if name == "REACTION_RESULT_FORMAT":
                return ""
            if name == "REACTION_EXIT_FORMAT":
                return "旧退出信号模板"
            return ""

        monkeypatch.setattr(assembler, "_extract_schema_section", fake_extract)

        _, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        permanent = (tmp_path / "context" / "reaction" / "layers" / "10_permanent.md").read_text(
            encoding="utf-8")
        popup = (tmp_path / "context" / "reaction" / "layers" / "99_popup.md").read_text(
            encoding="utf-8")
        combined = "\n".join(m.get("content", "") for m in messages)

        assert "<!-- [OUTPUT_FORMAT:reaction_fallback_warning] -->" not in permanent
        assert "<!-- [STEP_GUIDE:reaction_result] -->" not in popup
        assert "旧退出信号模板" not in combined

    def test_assemble_structure_has_six_layers(self, tmp_path, monkeypatch):
        """验证 system prompt 包含六层频率梯度结构标记"""
        from assembly import context as ctx
        monkeypatch.setattr(ctx, "CORE_MD", str(tmp_path / "core.md"))
        monkeypatch.setattr(ctx, "DREAMS_MD", str(tmp_path / "dreams.md"))

        from data.container_store import ContainerStore
        from data.relation_store import RelationStore
        from data.memory_heat import MemoryHeat
        from data.memory_store import MemoryStore
        monkeypatch.setattr(ContainerStore, "load_registry", lambda self: {"containers": []})
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": []})
        monkeypatch.setattr(MemoryHeat, "load_heat", lambda self: {"entries": {}})
        monkeypatch.setattr(MemoryStore, "load_meta", lambda self: {})

        with open(tmp_path / "core.md", "w", encoding="utf-8") as f:
            f.write("PID：FMZ\n位格编码：SCVARB\n## 6. 位格自述\n测试")

        with open(tmp_path / "cr.json", "w", encoding="utf-8") as f:
            json.dump({"containers": []}, f)

        assembler = ctx.ContextAssembler(context_dir=str(tmp_path / "context"))
        state = self._make_state()
        system, messages = assembler.assemble_setup(state, "interactive")
        combined = "\n".join(m.get("content", "") for m in messages)

        # 六层频率梯度标记（永固层、定期层、高频层为 system prompt 必现层）
        assert system == ""
        assert "<!-- 永固层 -->" in combined or "位格核心" in combined
        assert "<!-- 定期层 -->" in combined or "EXPLORER" in combined or "索引" in combined
        assert "<!-- 高频层 -->" in combined or "STM" in combined or "动态" in combined or "核心" in combined
        assert "<!-- 远缓存层 -->" not in combined
        assert "<!-- 近缓存层 -->" not in combined
        assert "<!-- 交互输入层 -->" not in combined
        assert "<!-- POPUP" in combined or "STATUSBAR" in combined or "R1" in combined

    def test_invalidate_high_freq_does_not_create_expired_flag(self, tmp_path):
        from assembly.context import ContextAssembler
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        assembler = ContextAssembler(state_store=sm)

        assembler.invalidate_layer("high_freq")

        cache = sm.load()["base"]["context_cache"]
        assert "high_freq_expired" not in cache

    def test_periodic_layer_applies_structured_limits(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.periodic_mount_store import PeriodicMountStore

        monkeypatch.setattr(PeriodicMountStore, "load", lambda self: {
            "periodic_memory_items": [
                {"id": "MEM-A", "rendered_text": "memory-a"},
                {"id": "MEM-B", "rendered_text": "memory-b-overflow"},
            ],
        })
        assembler = ContextAssembler(config_store=self._FakeConfig(
            memory_limit=len("memory-a"),
        ))

        periodic = assembler._build_periodic(self._make_state(), "setup", "interactive")

        assert "memory-a" in periodic
        assert "memory-b-overflow" not in periodic
        assert "定期技能工具索引" not in periodic
        block_index = assembler._current_layer_block_index["periodic"]
        assert [item["source_block_id"] for item in block_index] == ["MEM-A"]
        _assert_exact_block_index(periodic, block_index)

    def test_spec723_high_freq_blocks_follow_real_modules_and_mounts(
            self, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler()
        monkeypatch.setattr(assembler, "_high_freq_index_limits", lambda: {})
        for name, value in (
            ("_build_container_index", "CONTAINERS"),
            ("_build_ltm_heat_index", "LTM_HEAT"),
            ("_build_stm_heat_index", "STM_HEAT"),
            ("_build_association_index", "ASSOCIATION"),
            ("_build_relation_inverted_index", "RELATION_INVERTED"),
            ("_build_relation_domain_index", "RELATION_DOMAIN"),
            ("_build_task_board_projection", "TASK_BOARD"),
            ("_build_step_toolbelt_index", "TOOLBELT"),
            ("_build_workbench_focus_projection", "WORKBENCH"),
        ):
            monkeypatch.setattr(assembler, name, lambda *args, _value=value, **kwargs: _value)
        monkeypatch.setattr(
            assembler, "_build_keyword_index",
            lambda source, *_args, **_kwargs: f"KEYWORD:{source}")
        monkeypatch.setattr(
            assembler, "_content_mounts_with_triple_hits",
            lambda *_args: [
                {"type": "memory", "ids": "MEM-1"},
                {"type": "container", "ids": "CTR-1"},
            ])
        monkeypatch.setattr(assembler, "_load_memory_content", lambda _ids: "MEMORY")
        monkeypatch.setattr(assembler, "_memory_mount_meta", lambda _ids: {})
        monkeypatch.setattr(assembler, "_load_container_content", lambda _ids: "CONTAINER")

        text = assembler._build_high_freq(
            self._make_state(), "reaction", "interactive", True, [],
            input_keywords=[],
            runtime_focus_entries=[
                {"source_block_id": "same", "title": "焦点甲", "content": "FOCUS_A"},
                {"source_block_id": "same", "title": "焦点乙", "content": "FOCUS_B"},
            ],
        )
        block_index = assembler._current_layer_block_index["high_freq"]

        assert [item["kind"] for item in block_index[-4:]] == [
            "runtime_focus", "runtime_focus", "memory_mount", "container_mount"]
        assert block_index[-4]["block_id"] != block_index[-3]["block_id"]
        _assert_exact_block_index(text, block_index)

    def test_periodic_layer_ignores_retired_skill_tool_projection(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.periodic_mount_store import PeriodicMountStore

        monkeypatch.setattr(PeriodicMountStore, "load", lambda self: {
            "periodic_memory_items": [
                {"id": "MEM-TOO-LONG", "rendered_text": "memory-too-long"},
            ],
            "periodic_skill_tools": [
                {"id": "SKL-OK", "rendered_text": "habit-or-reflex-ok"},
            ],
        })
        assembler = ContextAssembler(config_store=self._FakeConfig(
            memory_limit=3,
        ))

        periodic = assembler._build_periodic(self._make_state(), "setup", "interactive")

        assert "memory-too-long" not in periodic
        assert "habit-or-reflex-ok" not in periodic

    def test_periodic_layer_ignores_removed_simple_mount_shape(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.periodic_mount_store import PeriodicMountStore

        monkeypatch.setattr(PeriodicMountStore, "load", lambda self: {
            "memories": ["MEM-LEGACY"],
            "skill_tools": ["SKL-habits-format-check"],
        })
        assembler = ContextAssembler(config_store=self._FakeConfig())

        periodic = assembler._build_periodic(self._make_state(), "setup", "interactive")

        assert periodic == ""

    def test_periodic_layer_structured_schema_does_not_fall_back_when_over_budget(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.periodic_mount_store import PeriodicMountStore
        from data.memory_store import MemoryStore

        monkeypatch.setattr(PeriodicMountStore, "load", lambda self: {
            "periodic_memory_items": [
                {"id": "MEM-TOO-LONG", "rendered_text": "memory-too-long"},
            ],
            "memories": ["MEM-LEGACY"],
            "skill_tools": ["SKL-habits-format-check"],
        })
        monkeypatch.setattr(MemoryStore, "load_meta", lambda self: {
            "MEM-LEGACY": {"title": "旧格式记忆", "type": "memory"},
        })
        assembler = ContextAssembler(config_store=self._FakeConfig(
            memory_limit=3,
        ))

        periodic = assembler._build_periodic(self._make_state(), "setup", "interactive")

        assert periodic == ""

    def test_lately_entries_use_context_store_source(self, monkeypatch):
        from assembly.context import ContextAssembler
        from data.context_store import ContextStore

        monkeypatch.setattr(ContextStore, "get_lately_entries", lambda self, step="setup": [
            {"round": 17, "role": "assistant", "content": f"最近缓存机器源:{step}"},
        ])

        assembler = ContextAssembler()

        lately = assembler._get_lately_entries("reaction")

        assert lately[0]["content"] == "最近缓存机器源:reaction"

    def test_cleanup_reads_tool_facts_from_injected_context_store(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.context_store import ContextStore

        store = ContextStore(
            cache_dir=str(tmp_path / "cache"),
            raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
        )
        store.append_to_cache(
            44,
            "system",
            "【本轮关系卡写入回执】\n处理结果：applied。\n关系卡：REL-Codex。",
            kind="tool_fact",
            step="reaction",
            protocol_receipt={
                "tool_id": "relation_card_write",
                "status": "applied",
                "card_id": "REL-Codex",
            },
        )

        assembler = ContextAssembler(
            context_dir=str(tmp_path / "context"),
            context_store=store,
        )
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_cleanup(
            self._make_state(),
            "interactive",
            {"response": "收到"},
        )
        combined = "\n".join(m.get("content", "") for m in messages)

        assert "<!-- 当前缓存 now -->" not in combined
        assert any(m.get("kind") == "tool_fact" for m in messages)
        assert "【本轮关系卡写入回执】" in combined
        assert "处理结果：applied。" in combined
        assert "[relation_card_write applied]" not in combined

    def test_assembler_reads_lately_cache_without_legacy_projection(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data import context_store as ctxs

        monkeypatch.setattr(ctxs, "CONTAINER_CORPUS_DIR", str(tmp_path / "corpus"))
        monkeypatch.setattr(ctxs, "STM_CONTEXT_CACHE_DIR", str(tmp_path / "cache"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_NOW_CACHE_JSONL", str(tmp_path / "cache" / "now_cache.jsonl"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_LATELY_CACHE_JSONL", str(tmp_path / "cache" / "lately_cache.jsonl"), raising=False)

        class CacheConfig(ConfigStoreStub):
            def get_now_cache_params(self):
                return {"budget_chars": 8, "trim_chars": 4}

            def get_lately_cache_params(self):
                return {"budget_chars": 65536, "trim_chars": 16384}

            def get_now_policy_by_kind(self):
                return {}

            def get_lately_allowed_kinds(self):
                return [
                    "interaction",
                    "assistant_reply",
                    "tool_fact",
                    "minimum_commitment",
                    "fault_note",
                ]

        store = ctxs.ContextStore(config_store=CacheConfig())
        for round_num in range(1, 42):
            store.save_round_to_cache(round_num, f"用户{round_num}", f"回复{round_num}")

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_reaction(self._make_state(), "interactive")
        combined = "\n".join(m.get("content", "") for m in messages)

        assert "回复41" in combined
        assert "用户10" in combined
        assert "用户1" in combined
        assert "回复1" in combined
        assert "【历史交互" in combined
        assert "lately_cache.jsonl" not in combined

    def test_reaction_marks_lately_and_now_layers_and_counts_real_chars(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.context_store import ContextStore

        monkeypatch.setattr(ContextStore, "get_lately_entries", lambda self, step="setup": [
            {"round": 17, "role": "assistant", "content": "最近缓存机器源"},
        ])
        monkeypatch.setattr(ContextStore, "get_now_entries", lambda self: [
            {"round": 18, "role": "user", "kind": "tool_fact", "content": "当前缓存机器源"},
        ])

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "高频")
        monkeypatch.setattr(
            assembler,
            "_build_statusbar_with_relations",
            lambda *args, **kwargs: "## STATUSBAR\n状态栏层\n\n## 关系卡摘要\nTzPz",
        )
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        system, messages = assembler._build_full_context(
            step="reaction",
            round_type="interactive",
            state=self._make_state(),
            include_content=True,
            mount_ids=None,
            popup_fragments=[
                "- kind: statusbar_order_probe\n  tier: reminder\n  message: POPUP顺序探针"
            ],
        )
        combined = "\n".join(m.get("content", "") for m in messages)

        assert system == ""
        assert "<!-- 高频层 -->" in combined
        assert "<!-- 最近缓存 lately -->" not in combined
        assert "<!-- 当前缓存 now -->" not in combined
        assert "<!-- STATUSBAR（状态栏层） -->" in combined
        assert "<!-- POPUP" in combined
        assert "<!-- 远缓存层 -->" not in combined
        assert "<!-- 近缓存层 -->" not in combined
        assert combined.index("最近缓存机器源") < combined.index("<!-- 高频层 -->")
        assert combined.index("<!-- 高频层 -->") < combined.index("当前缓存机器源")
        assert combined.index("当前缓存机器源") < combined.index("<!-- STATUSBAR（状态栏层） -->")
        assert combined.index("<!-- STATUSBAR（状态栏层） -->") < combined.index("<!-- POPUP")
        assert "当前可见轮次：" not in combined
        assert "来源轮次：" not in combined
        assert "条，" not in combined
        assert any(
            m["role"] == "system"
            and "最近缓存机器源" in m["content"]
            and "【历史回复" in m["content"]
            for m in messages
        )

        manifest_path = tmp_path / "reaction" / "manifest.json"
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        lately_json = json.loads(
            (tmp_path / "reaction" / "layers" / "30_lately.json")
            .read_text(encoding="utf-8")
        )
        assert isinstance(lately_json["content"], list)
        assert manifest["layers"]["lately"]["model_visible_chars"] == len(
            lately_json["content_markdown"])
        assert manifest["layers"]["lately"]["chars"] == len(json.dumps(
            lately_json["content"], ensure_ascii=False, sort_keys=True))
        now_json = json.loads(
            (tmp_path / "reaction" / "layers" / "50_now.json")
            .read_text(encoding="utf-8")
        )
        assert isinstance(now_json["content"], list)
        assert manifest["layers"]["now"]["model_visible_chars"] == len(
            now_json["content_markdown"])
        assert manifest["layers"]["now"]["chars"] == len(json.dumps(
            now_json["content"], ensure_ascii=False, sort_keys=True))
        assert manifest["layers"]["statusbar"]["chars"] == len("## STATUSBAR\n状态栏层\n\n## 关系卡摘要\nTzPz")

        high_freq_layer = (tmp_path / "reaction" / "layers" / "40_high_freq.md").read_text(
            encoding="utf-8"
        )
        statusbar_layer = (tmp_path / "reaction" / "layers" / "60_statusbar.md").read_text(
            encoding="utf-8"
        )
        assert "## STATUSBAR" not in high_freq_layer
        assert "## STATUSBAR" in statusbar_layer
        assert "## 关系卡摘要" in statusbar_layer

    def test_spec417_native_subject_feedback_is_appended_to_final_popup_message(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from engines.reaction_helpers import native_tool_failure_feedbacks

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "高频")
        monkeypatch.setattr(
            assembler,
            "_build_statusbar_with_relations",
            lambda *args, **kwargs: "## STATUSBAR\n状态栏层",
        )
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        feedbacks = native_tool_failure_feedbacks([{
            "tool_id": "memory_write",
            "status": "error",
            "reason": "subject_not_confirmed",
            "call_id": "call_memory_subject",
            "subject": "Other",
            "submitted_subject": "Other",
            "confirmed_subject": "TzPz",
            "confirmed_subjects": ["TzPz"],
        }])

        _system, messages = assembler._build_full_context(
            step="reaction",
            round_type="interactive",
            state=self._make_state(),
            include_content=True,
            mount_ids=None,
            native_tool_feedbacks=feedbacks,
        )

        assert messages[-1]["role"] == "system"
        assert messages[-1]["content"].startswith("<!-- POPUP")
        assert "call_memory_subject" in messages[-1]["content"]
        assert "NO-GO" in messages[-1]["content"]
        warning_tail = messages[-1]["content"].split("call_memory_subject", 1)[1]
        assert "relation_card_write" not in warning_tail
        assert "memory_write" in messages[-1]["content"]

    def test_spec623_reaction_static_memory_reminder_is_resident_every_iteration(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_build_statusbar_with_relations",
                            lambda *args, **kwargs: "## STATUSBAR\n状态栏层")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        counts = []
        for iteration in range(1, 6):
            _system, messages = assembler._build_full_context(
                step="reaction",
                round_type="interactive",
                state=self._make_state(),
                include_content=True,
                mount_ids=None,
                current_reaction_iteration=iteration,
            )
            combined = "\n".join(message.get("content", "") for message in messages)
            counts.append(combined.count("资料正文由 material/最近缓存承载"))

        assert counts == [1, 1, 1, 1, 1]

    def test_spec598_popup_policy_dedupes_identity_unresolved_memory_feedbacks(self):
        from engines.reaction_helpers import native_tool_failure_feedbacks
        from logic.popup_policy import PopupPolicy

        feedbacks = native_tool_failure_feedbacks([
            {
                "tool_id": "memory_write",
                "status": "error",
                "reason": "identity_unresolved",
                "call_id": f"call_memory_{idx}",
                "subject": "unknown",
            }
            for idx in range(5)
        ])

        rendered = PopupPolicy().combine(feedbacks)

        assert rendered.count("原生工具调用警告") == 1
        assert rendered.count("identity_unresolved") <= 1
        assert "已压缩 4 条同类重复提醒" in rendered
        assert "relation_card_write" not in rendered
        assert "NO-GO" in rendered

    def test_spec598_popup_policy_caps_r572_style_repeat_growth(self):
        from engines.reaction_helpers import native_tool_failure_feedbacks
        from logic.popup_policy import PopupPolicy

        long_body = "噪音" * 4000
        feedbacks = native_tool_failure_feedbacks([
            {
                "tool_id": "memory_write",
                "status": "error",
                "reason": "identity_unresolved",
                "call_id": f"call_repeat_{idx}",
                "subject": "unknown",
                "actual": long_body,
            }
            for idx in range(40)
        ])

        rendered = PopupPolicy().combine(feedbacks)

        assert len(rendered) < 8000
        assert rendered.count("memory_write") <= 4
        assert "relation_card_write" not in rendered

    def test_setup_interaction_input_is_in_now_after_lately(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.context_store import ContextStore

        monkeypatch.setattr(ContextStore, "get_lately_entries", lambda self, step="setup": [
            {"round": 17, "role": "assistant", "content": "最近缓存机器源"},
        ])
        monkeypatch.setattr(ContextStore, "get_now_entries", lambda self: [{
            "round": 1,
            "role": "user",
            "kind": "interaction",
            "content": "本轮输入",
        }])

        assembler = ContextAssembler(context_dir=str(tmp_path))
        monkeypatch.setattr(assembler, "_build_permanent", lambda *args: "永固")
        monkeypatch.setattr(assembler, "_build_periodic", lambda *args: "定期")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args: "高频")
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        system, messages = assembler.assemble_setup(
            self._make_state(), "interactive")
        combined = "\n".join(m.get("content", "") for m in messages)

        assert system == ""
        assert "<!-- 最近缓存 lately -->" not in combined
        assert "<!-- 当前缓存 now -->" not in combined
        assert "最近缓存机器源" in combined
        assert "本轮输入" in combined
        assert "<!-- 交互输入层 -->" not in combined
        manifest_path = tmp_path / "setup" / "manifest.json"
        with open(manifest_path, "r", encoding="utf-8") as f:
            manifest = json.load(f)
        lately_json = json.loads(
            (tmp_path / "setup" / "layers" / "30_lately.json")
            .read_text(encoding="utf-8")
        )
        assert isinstance(lately_json["content"], list)
        assert manifest["layers"]["lately"]["model_visible_chars"] == len(
            lately_json["content_markdown"])
        now_json = json.loads(
            (tmp_path / "setup" / "layers" / "50_now.json")
            .read_text(encoding="utf-8")
        )
        assert isinstance(now_json["content"], list)
        assert manifest["layers"]["now"]["model_visible_chars"] == len(
            now_json["content_markdown"])

    def test_high_freq_does_not_include_feeling_vocabulary_by_default(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_cleanup(
            {"base": {"context_cache": {}}},
            "interactive",
            {"response": "收到"},
        )

        high = next(
            m["content"]
            for m in messages
            if "<!-- 高频层 -->" in m.get("content", "")
        )
        assert "感受词库（仅词条，不含数值）" not in high
        assert "memory_write 感受词清单（仅词条，不含数值）" not in high
        assert "| +1 |" not in high
        assert "| -1 |" not in high
        assert "（+2" not in high
        assert "（-2" not in high

    def test_identity_prompt_event_uses_popup_template(self, tmp_path, monkeypatch):
        from assembly import popup as pm
        from assembly.popup import PopupManager

        popup_doc = tmp_path / "popup.md"
        popup_doc.write_text(
            "# POPUP 模板表\n\n"
            "## reminder 提醒模板\n\n"
            "### identity_prompt\n"
            "- tier: reminder\n"
            "- kind: identity_prompt\n"
            "- decision_required: false\n"
            "- message: 临时模板身份提示。\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(pm, "POPUP_TEMPLATE_MD", str(popup_doc))

        event = PopupManager.build_identity_prompt_event()

        assert "kind: identity_prompt" in event
        assert "decision_required: false" in event
        assert "临时模板身份提示" in event

    def test_reaction_step_guide_uses_popup_template(
            self, tmp_path, monkeypatch):
        from assembly import popup as pm
        from assembly.context_helpers import build_reaction_step_guide_popup

        popup_doc = tmp_path / "popup.md"
        popup_doc.write_text(
            "# POPUP 模板表\n\n"
            "### reaction_step_guide\n"
            "- tier: guide\n"
            "- kind: reaction_step_guide\n"
            "- decision_required: false\n"
            "- message: |\n"
            "  来自 popup.md 的反应步指南。\n"
            "  UPSP 是工具驱动系统；裸文本是非法输出。\n",
            encoding="utf-8",
        )
        monkeypatch.setattr(pm, "POPUP_TEMPLATE_MD", str(popup_doc))

        rendered = build_reaction_step_guide_popup("reaction")

        assert "来自 popup.md 的反应步指南" in rendered
        assert "裸文本是非法输出" in rendered
        assert "反应步：推理、工具调用、生成回复" not in rendered

    def test_reaction_step_guide_fallback_keeps_verification_thresholds(
            self, monkeypatch):
        from assembly.context_helpers import PopupManager, build_reaction_step_guide_popup

        monkeypatch.setattr(PopupManager, "load_template", lambda *_: {})

        rendered = build_reaction_step_guide_popup("reaction")

        assert "核验与询问" in rendered
        assert "稳定事实和纯仓内任务不强制联网" in rendered
        assert "实质改变交付结果或授权边界" in rendered
        assert "范围最小、可回退的带界假设" in rendered

    def test_now_lately_layers_are_rendered_without_retired_layer_names(self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_get_now_entries", lambda: [{
            "round": 1,
            "role": "user",
            "kind": "interaction",
            "content": "我是 Codex，验证 now 兼容标记。",
        }])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _system, messages = assembler.assemble_setup(
            {"base": {"context_cache": {}}},
            "interactive",
        )

        combined = "\n".join(m.get("content", "") for m in messages)
        assert "当前缓存 now" not in combined
        assert "验证 now 兼容标记" in combined
        assert "交互输入层" not in combined
        assert "资料输入层" not in combined
        assert "内部交接层" not in combined
