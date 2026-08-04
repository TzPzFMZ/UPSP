import json
import os
import sys
from pathlib import Path

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def _spec487_expected_evidence_handle(result):
    import hashlib

    parts = []
    for key in (
            "tool_id",
            "call_id",
            "tool_call_id",
            "id",
            "path",
            "file_path",
            "target_path",
            "root",
            "cwd",
            "url",
            "source_url",
            "query",
            "command"):
        value = str(result.get(key) or "").strip()
        if value:
            parts.append(f"{key}={value}")
    digest = hashlib.sha1("\n".join(parts).encode("utf-8")).hexdigest()
    return f"EV-{digest[:10].upper()}"


def _successful_tool_evidence_context():
    return {
        "prior_general_tool_results": [
            {
                "tool_id": "file_write",
                "status": "ok",
                "call_id": "call_write_report",
                "path": r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL\output\report.md",
            },
            {
                "tool_id": "shell_command",
                "status": "ok",
                "call_id": "call_run_report",
                "cwd": r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL",
                "command": "python output/report.py",
            },
        ],
        "current_general_tool_requests": [],
    }


def test_spec593_task_bootstrap_visible_schema_contains_minimal_ledger_shape(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_guide_feedback import render_active_guide_feedback
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    message = render_active_guide_feedback(guide, workbench=store)

    assert "source_requirements=[{requirement_id, source_ref, summary}]" in message
    assert "items=[{item_id, title, requirement_refs:[...]}]" in message
    assert "acceptance=[{acceptance_id, description, item_refs:[...]}]" in message
    assert "source_ref 必须来自已读来源" in message


class _StateStoreStub:
    def __init__(self):
        self.cleared_flags = []
        self.saved = None

    def clear_flags(self, flags):
        self.cleared_flags.extend(list(flags or []))

    def load(self):
        return {"base": {}}

    def save(self, data):
        self.saved = data


class _AlertStoreStub:
    def __init__(self):
        self.alerts = []

    def append_alert(self, **kwargs):
        self.alerts.append(dict(kwargs))


def test_spec592_setup_schema_restores_task_guidance_gate():
    from logic.native_tool_calls import export_provider_tool_schemas

    schemas = export_provider_tool_schemas(include_step_terminal_tools=["setup_finalize"])
    setup = next(item for item in schemas if item["name"] == "setup_finalize")
    props = setup["parameters"]["properties"]

    assert props["task_guidance_required"]["type"] == "boolean"
    assert set(props["task_guidance_route"]["enum"]) == {
        "none",
        "new_work",
        "current_work",
    }
    assert props["task_guidance_reason"]["type"] == "string"
    assert "task_guidance_required" in setup["description"]
    assert "task_source_refs" not in props
    assert "task_mode" not in props


def test_spec434_setup_popup_names_complex_task_guidance_trigger():
    root = Path(__file__).resolve().parents[2]
    popup = (root / "initialization" / "persona_template" / "docs" / "protocol" / "base" / "popup.md").read_text(
        encoding="utf-8"
    )

    assert "task_guidance_required=true" in popup
    assert "读取资料、运行命令、写文件、给出证据路径、完成验收" in popup
    assert "起手步不执行用户任务" in popup


def test_spec510_resident_task_and_memory_prompts_are_strongly_anchored():
    from logic.native_tool_calls import export_provider_tool_schemas
    from logic.reaction_resident_guide import reaction_loop_resident_feedback

    resident = reaction_loop_resident_feedback()
    assert "- kind: resident_task_guidance_guide" in resident
    assert "需要任务清单时" in resident
    assert "当前工作需要任务清单、但还没有 active task guide" not in resident
    assert "先建清单，再执行" not in resident
    assert "option_id=request_task_guidance" in resident
    assert "分批整理" not in resident
    assert "约 4K tokens" not in resident
    assert "DSML、JSON 工具调用" not in resident
    assert "如果当前工作需要任务清单" not in resident

    schemas = export_provider_tool_schemas(include_protocol_writes=True)
    memory_schema = next(item for item in schemas if item["name"] == "memory_write")
    description = memory_schema["description"]
    assert "任务未闭合仍可写阶段性发现" in description
    assert "不得写成已完成结论" in description


def test_spec434_guide_submit_schema_is_single_answer_sheet():
    from logic.native_tool_calls import export_provider_tool_schemas

    schemas = export_provider_tool_schemas(
        include_protocol_writes=True,
        include_step_terminal_tools=["reaction_finalize"],
        active_protocol_tool_guides={"guide_submit"},
    )
    guide_tool = next(item for item in schemas if item["name"] == "guide_submit")
    params = guide_tool["parameters"]
    props = params["properties"]

    assert params["additionalProperties"] is False
    assert params["required"] == ["guide_id"]
    assert props["guide_id"]["type"] == "string"
    assert props["submissions"]["type"] == "array"
    assert props["item_id"]["type"] == "string"
    assert props["option_id"]["type"] == "string"
    assert props["fields"]["type"] == "object"
    submission_props = props["submissions"]["items"]["properties"]
    assert set(submission_props) == {
        "item_id",
        "option_id",
        "fields",
        "evidence_refs",
        "reason",
    }


def test_spec434_workbench_can_store_active_guide_and_append_ledger(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = {
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "items": [
            {
                "item_id": "build_initial_task_guide",
                "mandatory": True,
                "options": [
                    {
                        "option_id": "submit_initial_guide",
                        "required_fields": ["task_title", "items", "acceptance"],
                        "allowed_fields": ["task_title", "items", "acceptance", "risk_notes"],
                    }
                ],
            }
        ],
    }

    store.save_guide(guide, active=True)
    assert store.get("base.active_guide") == "task_bootstrap"
    assert store.load_active_guide()["guide_id"] == "task_bootstrap"

    store.append_guide_ledger("task_bootstrap", {
        "event": "guide_submission_accepted",
        "item_id": "build_initial_task_guide",
    })

    ledger_path = tmp_path / "workbench" / "guides" / "task_bootstrap" / "ledger.jsonl"
    entries = [json.loads(line) for line in ledger_path.read_text(encoding="utf-8").splitlines()]
    assert entries[0]["event"] == "guide_submission_accepted"


def test_spec464_rhythm_guide_submit_writes_chronicle(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

    class ChronicleStub:
        def __init__(self):
            self.entries = []

        def write_focused_entry(self, focus, content):
            self.entries.append((dict(focus), content))
            return str(tmp_path / "chronicle.md")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide_id = materialize_current_rhythm_guide(
        store,
        {"rhythm_due": True},
        round_num=464,
    )
    chronicle = ChronicleStub()

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": guide_id,
            "submissions": [{
                "item_id": "rhythm_due",
                "option_id": "write_chronicle",
                "fields": {
                    "content": "本轮主轴节律完成一次自检。",
                },
            }],
        },
        evidence_context={
            "round_num": 464,
            "round_type": "rhythm",
            "chronicle_store": chronicle,
            "chronicle_focus": {
                "layer": "rhythms",
                "round_num": 464,
                "round_type": "rhythm",
                "source_refs": ["round:464"],
            },
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["completed_flags"] == ["rhythm_due"]
    assert receipt["backend_receipts"][0]["tool_id"] == "chronicle_write"
    assert chronicle.entries[0][1] == "本轮主轴节律完成一次自检。"
    assert store.get("base.active_guides.rhythm") is None


def test_spec472_context_pressure_guide_submit_settles_alert_flag(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide_id = materialize_current_rhythm_guide(
        store,
        {"context_pressure": True},
        round_num=472,
    )
    state_store = _StateStoreStub()
    alert_store = _AlertStoreStub()

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": guide_id,
            "submissions": [{
                "item_id": "context_pressure",
                "option_id": "settle_alert",
                "fields": {
                    "status": "recovered",
                    "summary": "context pressure handled",
                },
            }],
        },
        evidence_context={
            "round_num": 472,
            "state_store": state_store,
            "alert_store": alert_store,
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["completed_flags"] == ["context_pressure"]
    assert state_store.cleared_flags == ["context_pressure"]
    assert store.get("base.active_guides.rhythm") is None


def test_reserved_process_flag_is_not_materialized_as_guide_item(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide_id = materialize_current_rhythm_guide(
        store,
        {
            "api_degraded": True,
            "process_down": True,
        },
        round_num=472,
    )

    first = apply_guide_submit(
        store,
        {
            "guide_id": guide_id,
            "submissions": [{
                "item_id": "api_degraded",
                "option_id": "settle_alert",
                "fields": {
                    "status": "recovered",
                    "summary": "api restored",
                },
            }],
        },
        evidence_context={
            "round_num": 472,
            "state_store": _StateStoreStub(),
            "alert_store": _AlertStoreStub(),
        },
    )

    assert first["status"] == "applied"
    assert first["completed_flags"] == ["api_degraded"]
    assert store.current_active_guide_id() is None
    guide_dir = guide_id.replace(":", "__colon__")
    guide_path = (
        tmp_path
        / "workbench"
        / "guides"
        / guide_dir
        / "guide.json"
    )
    guide_doc = json.loads(guide_path.read_text(encoding="utf-8"))
    assert guide_doc["status"] == "completed"
    assert [item["item_id"] for item in guide_doc["items"]] == ["api_degraded"]
    ledger_text = (
        tmp_path
        / "workbench"
        / "guides"
        / guide_dir
        / "ledger.jsonl"
    ).read_text(encoding="utf-8")
    assert "guide_completed" in ledger_text


def test_spec464_cache_compaction_guide_submit_executes_cache_compact(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    class ContextStub:
        def __init__(self):
            self.rewrite_decisions = []

        def get_lately_compact_ratio(self):
            return 0.618

        def build_lately_compression_candidates(self, max_blocks=None):
            return [{
                "id": "R000001-user-0000",
                "chars": 100,
                "text": "旧缓存正文",
            }]

        def rewrite_lately_blocks(self, decisions, current_round=None):
            self.rewrite_decisions.append((decisions, current_round))
            return {
                "kept": 0,
                "replaced": 1,
                "dropped": 0,
                "skipped": 0,
                "summaries": 1,
                "source_blocks": 1,
                "before_chars": 100,
                "after_chars": 31,
            }

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = {
        "guide_id": "cache_compaction:R000464",
        "kind": "cache_compaction_rhythm_guide",
        "guide_slot": "rhythm",
        "items": [{
            "item_id": "compress_lately_cache",
            "options": [{
                "option_id": "submit_cache_compaction_shard",
                "required_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
                "allowed_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
            }],
        }],
    }
    store.save_guide(guide, active=True)
    context = ContextStub()

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "cache_compaction:R000464",
            "submissions": [{
                "item_id": "compress_lately_cache",
                "option_id": "submit_cache_compaction_shard",
                "fields": {
                    "shard_id": "shard_01",
                    "source_block_ids": ["R000001-user-0000"],
                    "summary": "压缩后的缓存摘要",
                    "input_chars": 100,
                    "output_chars": 31,
                },
            }],
        },
        evidence_context={
            "round_num": 464,
            "context_store": context,
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["backend_receipts"][0]["tool_id"] == "cache_compact"
    assert context.rewrite_decisions[0][0][0]["action"] == "replace"
    assert context.rewrite_decisions[0][0][0]["summary"] == "压缩后的缓存摘要"
    assert store.get("base.active_guides.rhythm") is None


def test_spec468_cache_compaction_keeps_guide_until_all_shards_done(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    class ContextStub:
        def __init__(self):
            self.rewrite_decisions = []

        def get_lately_compact_ratio(self):
            return 0.618

        def rewrite_lately_blocks(self, decisions, current_round=None):
            self.rewrite_decisions.append((decisions, current_round))
            return {
                "kept": 0,
                "replaced": 1,
                "dropped": 0,
                "skipped": 0,
                "summaries": 1,
                "source_blocks": len(decisions[0]["source_block_ids"]),
                "before_chars": decisions[0]["input_chars"],
                "after_chars": decisions[0]["output_chars"],
            }

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = {
        "guide_id": "cache_compaction:R000468",
        "kind": "cache_compaction_rhythm_guide",
        "guide_slot": "rhythm",
        "compaction_plan": {
            "shards": [
                {"shard_id": "shard_01", "source_block_ids": ["R1"], "input_chars": 100},
                {"shard_id": "shard_02", "source_block_ids": ["R2"], "input_chars": 80},
            ],
        },
        "items": [{
            "item_id": "compress_lately_cache",
            "options": [{
                "option_id": "submit_cache_compaction_shard",
                "required_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
                "allowed_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
            }],
        }],
    }
    store.save_guide(guide, active=True)
    context = ContextStub()

    first = apply_guide_submit(
        store,
        {
            "guide_id": "cache_compaction:R000468",
            "submissions": [{
                "item_id": "compress_lately_cache",
                "option_id": "submit_cache_compaction_shard",
                "fields": {
                    "shard_id": "shard_01",
                    "source_block_ids": ["R1"],
                    "summary": "第一片摘要",
                    "input_chars": 100,
                    "output_chars": 31,
                },
            }],
        },
        evidence_context={"round_num": 468, "context_store": context},
    )

    assert first["status"] == "applied"
    assert first["completed_flags"] == []
    assert first["cache_compaction"]["completed_shards"] == ["shard_01"]
    assert first["cache_compaction"]["remaining_shards"] == ["shard_02"]
    assert store.get("base.active_guides.rhythm") == "cache_compaction:R000468"
    assert store.load_guide("cache_compaction:R000468")["completed_shards"] == ["shard_01"]

    second = apply_guide_submit(
        store,
        {
            "guide_id": "cache_compaction:R000468",
            "submissions": [{
                "item_id": "compress_lately_cache",
                "option_id": "submit_cache_compaction_shard",
                "fields": {
                    "shard_id": "shard_02",
                    "source_block_ids": ["R2"],
                    "summary": "第二片摘要",
                    "input_chars": 80,
                    "output_chars": 25,
                },
            }],
        },
        evidence_context={"round_num": 468, "context_store": context},
    )

    assert second["status"] == "applied"
    assert second["completed_flags"] == [
        "compress_lately_cache",
        "cache_compaction_due",
    ]
    assert second["cache_compaction"]["completed_shards"] == ["shard_01", "shard_02"]
    assert second["cache_compaction"]["remaining_shards"] == []
    assert store.get("base.active_guides.rhythm") is None


def test_spec469_cache_compaction_finishes_when_measured_target_met(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    class ContextStub:
        def __init__(self):
            self.blocks = [
                {"id": "R1", "chars": 100, "text": "A" * 100},
                {"id": "R2", "chars": 80, "text": "B" * 80},
            ]

        def get_lately_compact_ratio(self):
            return 0.618

        def build_lately_compression_candidates(self, max_blocks=None):
            return list(self.blocks)

        def rewrite_lately_blocks(self, decisions, current_round=None):
            decision = decisions[0]
            source_ids = set(decision["source_block_ids"])
            replacement = {
                "id": "cache-summary-R1",
                "chars": decision["output_chars"],
                "text": decision["summary"],
            }
            self.blocks = [
                replacement if block["id"] in source_ids else block
                for block in self.blocks
            ]
            return {
                "kept": 1,
                "replaced": 1,
                "dropped": 0,
                "skipped": 0,
                "summaries": 1,
                "source_blocks": len(source_ids),
                "before_chars": decision["input_chars"],
                "after_chars": decision["output_chars"],
            }

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = {
        "guide_id": "cache_compaction:R000469",
        "kind": "cache_compaction_rhythm_guide",
        "guide_slot": "rhythm",
        "compaction_plan": {
            "before_chars": 180,
            "target_chars": 111,
            "shards": [
                {"shard_id": "shard_01", "source_block_ids": ["R1"], "input_chars": 100},
                {"shard_id": "shard_02", "source_block_ids": ["R2"], "input_chars": 80},
            ],
        },
        "items": [{
            "item_id": "compress_lately_cache",
            "options": [{
                "option_id": "submit_cache_compaction_shard",
                "required_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
                "allowed_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
            }],
        }],
    }
    store.save_guide(guide, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "cache_compaction:R000469",
            "submissions": [{
                "item_id": "compress_lately_cache",
                "option_id": "submit_cache_compaction_shard",
                "fields": {
                    "shard_id": "shard_01",
                    "source_block_ids": ["R1"],
                    "summary": "第一片摘要",
                    "input_chars": 100,
                    "output_chars": 31,
                },
            }],
        },
        evidence_context={"round_num": 469, "context_store": ContextStub()},
    )

    assert receipt["status"] == "applied"
    assert receipt["completed_flags"] == [
        "compress_lately_cache",
        "cache_compaction_due",
    ]
    assert receipt["cache_compaction"]["target_met"] is True
    assert receipt["cache_compaction"]["current_chars"] == 111
    assert receipt["cache_compaction"]["target_chars"] == 111
    assert receipt["cache_compaction"]["skipped_shards"] == ["shard_02"]
    assert store.get("base.active_guides.rhythm") is None


def test_spec474_cache_compaction_shard_updates_and_clears_persisted_debt(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    class ContextStub:
        def __init__(self):
            self.blocks = [
                {"id": "R1", "chars": 100, "text": "A" * 100},
                {"id": "R2", "chars": 80, "text": "B" * 80},
            ]
            self.debt = {
                "schema_version": "cache_compaction_debt.v1",
                "status": "open",
                "completed_shards": [],
                "compaction_plan": {
                    "before_chars": 180,
                    "target_chars": 111,
                    "shards": [
                        {"shard_id": "shard_01", "source_block_ids": ["R1"]},
                        {"shard_id": "shard_02", "source_block_ids": ["R2"]},
                    ],
                },
            }

        def get_lately_compact_ratio(self):
            return 0.618

        def build_lately_compression_candidates(self, max_blocks=None):
            return list(self.blocks)

        def rewrite_lately_blocks(self, decisions, current_round=None):
            decision = decisions[0]
            source_ids = set(decision["source_block_ids"])
            replacement = {
                "id": "cache-summary-R1",
                "chars": decision["output_chars"],
                "text": decision["summary"],
            }
            self.blocks = [
                replacement if block["id"] in source_ids else block
                for block in self.blocks
            ]
            return {
                "kept": 1,
                "replaced": 1,
                "dropped": 0,
                "skipped": 0,
                "summaries": 1,
                "source_blocks": len(source_ids),
                "before_chars": decision["input_chars"],
                "after_chars": decision["output_chars"],
            }

        def update_cache_compaction_debt(self, **fields):
            self.debt.update(fields)
            return dict(self.debt)

        def clear_cache_compaction_debt(self):
            self.debt = None

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = {
        "guide_id": "cache_compaction:R000474",
        "kind": "cache_compaction_rhythm_guide",
        "guide_slot": "rhythm",
        "compaction_plan": {
            "before_chars": 180,
            "target_chars": 111,
            "shards": [
                {"shard_id": "shard_01", "source_block_ids": ["R1"], "input_chars": 100},
                {"shard_id": "shard_02", "source_block_ids": ["R2"], "input_chars": 80},
            ],
        },
        "items": [{
            "item_id": "compress_lately_cache",
            "options": [{
                "option_id": "submit_cache_compaction_shard",
                "required_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
                "allowed_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
            }],
        }],
    }
    store.save_guide(guide, active=True)
    context = ContextStub()

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "cache_compaction:R000474",
            "submissions": [{
                "item_id": "compress_lately_cache",
                "option_id": "submit_cache_compaction_shard",
                "fields": {
                    "shard_id": "shard_01",
                    "source_block_ids": ["R1"],
                    "summary": "第一片摘要",
                    "input_chars": 100,
                    "output_chars": 31,
                },
            }],
        },
        evidence_context={"round_num": 474, "context_store": context},
    )

    assert receipt["status"] == "applied"
    assert receipt["cache_compaction"]["target_met"] is True
    assert context.debt is None


def test_spec475_cache_compaction_rejects_unknown_plan_shard(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    class ContextStub:
        def __init__(self):
            self.rewrite_decisions = []

        def get_lately_compact_ratio(self):
            return 0.618

        def build_lately_compression_candidates(self, max_blocks=None):
            return [{"id": "R1", "chars": 100}, {"id": "R2", "chars": 80}]

        def rewrite_lately_blocks(self, decisions, current_round=None):
            self.rewrite_decisions.append((decisions, current_round))
            return {
                "kept": 1,
                "replaced": 1,
                "dropped": 0,
                "skipped": 0,
                "summaries": 1,
                "source_blocks": 1,
                "before_chars": 100,
                "after_chars": 31,
            }

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "cache_compaction:R000475",
        "kind": "cache_compaction_rhythm_guide",
        "guide_slot": "rhythm",
        "compaction_plan": {
            "target_chars": 111,
            "shards": [
                {"shard_id": "shard_01", "source_block_ids": ["R1"], "input_chars": 100},
                {"shard_id": "shard_02", "source_block_ids": ["R2"], "input_chars": 80},
            ],
        },
        "items": [{
            "item_id": "compress_lately_cache",
            "options": [{
                "option_id": "submit_cache_compaction_shard",
                "required_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
                "allowed_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
            }],
        }],
    }, active=True)
    context = ContextStub()

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "cache_compaction:R000475",
            "submissions": [{
                "item_id": "compress_lately_cache",
                "option_id": "submit_cache_compaction_shard",
                "fields": {
                    "shard_id": "Spec474 cache block 15",
                    "source_block_ids": ["Spec474 cache block 15"],
                    "summary": "wrong visible title",
                    "input_chars": 100,
                    "output_chars": 31,
                },
            }],
        },
        evidence_context={"round_num": 475, "context_store": context},
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "cache_compaction_shard_not_in_plan"
    assert context.rewrite_decisions == []


def test_spec475_cache_compaction_feedback_lists_actionable_shards(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "cache_compaction:R000475",
        "kind": "cache_compaction_rhythm_guide",
        "guide_slot": "rhythm",
        "completed_shards": ["shard_01"],
        "compaction_plan": {
            "before_chars": 180,
            "target_chars": 111,
            "shards": [
                {
                    "shard_id": "shard_01",
                    "source_block_ids": ["R1"],
                    "input_chars": 100,
                    "target_chars": 31,
                },
                {
                    "shard_id": "shard_02",
                    "source_block_ids": ["R2"],
                    "input_chars": 80,
                    "target_chars": 25,
                },
            ],
        },
        "items": [{
            "item_id": "compress_lately_cache",
            "options": [{
                "option_id": "submit_cache_compaction_shard",
                "required_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
                "allowed_fields": [
                    "shard_id",
                    "source_block_ids",
                    "summary",
                    "input_chars",
                    "output_chars",
                ],
            }],
        }],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))

    feedback = runner._active_guide_feedback()

    assert "缓存压缩计划：" in feedback
    assert "目标字符数 111" in feedback
    assert "已完成分片：shard_01" in feedback
    assert "shard_02" in feedback
    assert "来源块 R2" in feedback
    assert "输入 80 字" in feedback
    assert "shard_id=shard_01" not in feedback


def test_spec461_workbench_slots_prioritize_rhythm_over_work(tmp_path):
    from data.workbench import WorkbenchStore

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    work_guide = {
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "items": [],
    }
    rhythm_guide = {
        "guide_id": "calendar:20260628",
        "kind": "calendar_rhythm_guide",
        "items": [],
    }

    store.save_guide(work_guide, active=True)
    store.save_guide(rhythm_guide, active=True)

    assert store.get("base.active_guides.work") == "task_bootstrap"
    assert store.get("base.active_guides.rhythm") == "calendar:20260628"
    assert store.get("base.active_guide") == "calendar:20260628"
    assert store.current_active_guide_id() == "calendar:20260628"
    assert store.load_active_guide()["kind"] == "calendar_rhythm_guide"

    store.clear_active_guide("calendar:20260628")

    assert store.get("base.active_guides.rhythm") is None
    assert store.get("base.active_guides.work") == "task_bootstrap"
    assert store.get("base.active_guide") == "task_bootstrap"
    assert store.current_active_guide_id() == "task_bootstrap"


def test_spec461_guide_submit_rejects_waiting_work_guide_while_rhythm_active(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "items": [
            {
                "item_id": "build_initial_task_guide",
                "options": [
                    {
                        "option_id": "submit_initial_guide",
                        "required_fields": ["task_title"],
                        "allowed_fields": ["task_title"],
                    }
                ],
            }
        ],
    }, active=True)
    store.save_guide({
        "guide_id": "calendar:20260628",
        "kind": "calendar_rhythm_guide",
        "items": [
            {
                "item_id": "calendar_day_due",
                "options": [
                    {
                        "option_id": "settle_calendar_item",
                        "required_fields": ["settlement"],
                        "allowed_fields": ["settlement"],
                    }
                ],
            }
        ],
    }, active=True)

    waiting_work_receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {"task_title": "整理任务"},
                }
            ],
        },
    )

    assert waiting_work_receipt["status"] == "rejected"
    assert waiting_work_receipt["reason"] == "guide_not_active"
    assert waiting_work_receipt["details"]["active_guide"] == "calendar:20260628"

    rhythm_receipt = apply_guide_submit(
        store,
        {
            "guide_id": "calendar:20260628",
            "submissions": [
                {
                    "item_id": "calendar_day_due",
                    "option_id": "settle_calendar_item",
                    "fields": {"settlement": "no_op"},
                }
            ],
        },
    )

    assert rhythm_receipt["status"] == "accepted"
    assert rhythm_receipt["guide_id"] == "calendar:20260628"


def test_spec461_reaction_feedback_mentions_waiting_work_without_schema(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "items": [
            {
                "item_id": "build_initial_task_guide",
                "options": [
                    {
                        "option_id": "submit_initial_guide",
                        "required_fields": ["task_title"],
                        "allowed_fields": ["task_title"],
                    }
                ],
            }
        ],
    }, active=True)
    store.save_guide({
        "guide_id": "calendar:20260628",
        "kind": "calendar_rhythm_guide",
        "items": [
            {
                "item_id": "calendar_day_due",
                "options": [
                    {
                        "option_id": "settle_calendar_item",
                        "required_fields": ["settlement"],
                        "allowed_fields": ["settlement"],
                    }
                ],
            }
        ],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))

    feedback = runner._active_guide_feedback()

    assert "当前指南：节律指南" in feedback
    assert "调用坐标：guide_id=calendar:20260628" in feedback
    assert "option_id=settle_calendar_item" in feedback
    waiting = "等待中的任务指南：先完成当前节律指南，完成后任务指南会重新显示。"
    assert waiting in feedback
    assert feedback.index(waiting) < feedback.index("当前指南：节律指南")
    assert "option_id=submit_initial_guide" not in feedback
    assert "Active guide:" not in feedback
    assert "required_fields=" not in feedback


def test_spec434_guide_submit_rejects_hidden_or_unknown_guide(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "active",
        "kind": "task_bootstrap",
        "items": [],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "hidden",
            "submissions": [],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "guide_not_active"


def test_spec434_guide_submit_rejects_undeclared_fields(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "items": [
            {
                "item_id": "build_initial_task_guide",
                "options": [
                    {
                        "option_id": "submit_initial_guide",
                        "required_fields": ["task_title"],
                        "allowed_fields": ["task_title"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "整理项目",
                        "surprise": "not allowed",
                    },
                }
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "undeclared_guide_fields"
    assert receipt["details"]["fields"] == ["surprise"]


def test_spec434_task_bootstrap_submit_initial_guide_materializes_active_task(tmp_path):
    from data.workbench import WorkbenchStore
    from data.state_store import StateStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide
    from logic.work_intent_debt import create_work_intent_debt

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    create_work_intent_debt(
        state_store,
        round_num=510,
        reason="agent_eval 任务必须先建清单",
        source="engineering_task_phase",
        source_refs=["round:510:interaction"],
        task_phase="agent_eval",
        task_root=str(tmp_path / "DFT_AGENT_EVAL"),
    )
    create_task_bootstrap_guide(store, reason="用户派发多交付物工程任务")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "整理周报",
                        "task_goal": "生成周报并验证数据",
                        "items": [
                            {"item_id": "item_01", "title": "读取数据", "required": True}
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "kind": "artifact_exists",
                                "description": "输出 report.md",
                                "required": True,
                                "target": "output/report.md",
                            }
                        ],
                        "risk_notes": ["来源不足时不能 finish"],
                    },
                    "evidence_refs": ["SRC-user-001"],
                }
            ],
        },
        evidence_context={"state_store": state_store},
    )

    assert receipt["status"] == "accepted"
    assert state_store.get("base.runtime.work_intent_debt") == {}
    task_id = store.get("base.active_task")
    assert task_id
    guide = store.load_task_guide(task_id)
    assert guide["task_title"] == "整理周报"
    assert guide["items"][0]["item_id"] == "item_01"
    assert store.load_active_guide()["guide_id"] == f"task:{task_id}"
    bootstrap = store.load_guide("task_bootstrap")
    assert bootstrap["status"] == "completed"


def test_spec547_task_bootstrap_not_a_task_clears_debt_and_active_guide(tmp_path):
    from data.workbench import WorkbenchStore
    from data.state_store import StateStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide
    from logic.work_intent_debt import create_work_intent_debt

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    create_work_intent_debt(
        state_store,
        round_num=547,
        reason="误判为多步骤任务",
        source="setup_finalize",
        source_refs=["round:547:interaction"],
        task_phase="agent_eval",
        task_root=str(tmp_path / "DFT_AGENT_EVAL"),
    )
    create_task_bootstrap_guide(store, reason="误判为任务")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "item_id": "build_initial_task_guide",
            "option_id": "not_a_task",
            "fields": {
                "reason": "这只是用户澄清，不需要建立任务账本。",
            },
        },
        evidence_context={"state_store": state_store},
    )

    assert receipt["status"] == "accepted"
    assert receipt["action"] == "task_bootstrap_dismissed"
    assert state_store.get("base.runtime.work_intent_debt") == {}
    assert store.current_active_guide_id() is None
    assert store.get("base.active_guide") in (None, "")
    assert store.get("base.active_guides.work") in (None, "")
    guide = store.load_guide("task_bootstrap")
    assert guide["status"] == "dismissed"
    ledger_path = tmp_path / "workbench" / "guides" / "task_bootstrap" / "ledger.jsonl"
    assert "task_bootstrap_dismissed" in ledger_path.read_text(encoding="utf-8")


def test_spec547_task_bootstrap_not_a_task_still_requires_reason(tmp_path):
    from data.workbench import WorkbenchStore
    from data.state_store import StateStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide
    from logic.work_intent_debt import create_work_intent_debt

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    create_work_intent_debt(
        state_store,
        round_num=547,
        reason="误判为多步骤任务",
        source="setup_finalize",
        source_refs=["round:547:interaction"],
        task_phase="agent_eval",
        task_root=str(tmp_path / "DFT_AGENT_EVAL"),
    )
    create_task_bootstrap_guide(store, reason="误判为任务")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "item_id": "build_initial_task_guide",
            "option_id": "not_a_task",
        },
        evidence_context={"state_store": state_store},
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "missing_guide_fields"
    assert receipt["details"]["fields"] == ["reason"]
    assert state_store.get("base.runtime.work_intent_debt", {}).get("source") == "setup_finalize"
    assert store.current_active_guide_id() == "task_bootstrap"
    assert store.load_guide("task_bootstrap").get("status") in (None, "active")


def test_spec485_guide_submit_accepts_flat_single_submission(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="multi-deliverable task")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "item_id": "build_initial_task_guide",
            "option_id": "submit_initial_guide",
            "task_title": "Daily agent eval",
            "task_goal": "Produce requested output files.",
            "items": [
                {"item_id": "item_01", "title": "Read task source"},
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "Task source is read and outputs are listed.",
                    "item_refs": ["item_01"],
                },
            ],
            "risk_notes": "Stay inside the task output directory.",
        },
    )

    assert receipt["status"] == "accepted"
    accepted = receipt["accepted_submissions"][0]
    assert accepted["item_id"] == "build_initial_task_guide"
    assert accepted["option_id"] == "submit_initial_guide"
    assert accepted["fields"]["task_title"] == "Daily agent eval"
    task_id = store.get("base.active_task")
    assert task_id
    guide = store.load_task_guide(task_id)
    assert guide["task_title"] == "Daily agent eval"
    assert guide["items"][0]["item_id"] == "item_01"


def test_spec486_flat_guide_submit_ignores_native_envelope_metadata(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="daily eval task")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "item_id": "build_initial_task_guide",
            "option_id": "submit_initial_guide",
            "task_title": "Daily agent eval",
            "task_goal": "Produce requested output files.",
            "items": [
                {"item_id": "item_01", "title": "Read task source"},
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "Task source is read and outputs are listed.",
                    "item_refs": ["item_01"],
                },
            ],
            "call_id": "call_123",
            "index": 0,
            "provider": "openai_chat",
            "provider_item_id": "toolu_123",
            "response_id": "resp_123",
        },
    )

    assert receipt["status"] == "accepted"
    fields = receipt["accepted_submissions"][0]["fields"]
    assert fields["task_title"] == "Daily agent eval"
    assert "call_id" not in fields
    assert "index" not in fields
    assert "provider" not in fields
    assert "provider_item_id" not in fields
    assert "response_id" not in fields


def test_spec482_task_execution_guide_exposes_direct_checklist_items(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户派发多交付物工程任务")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "整理周报",
                        "items": [
                            {"item_id": "item_01", "title": "读取数据"}
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "输出 report.md",
                                "item_refs": ["item_01"],
                            }
                        ],
                    },
                }
            ],
        },
    )

    assert receipt["status"] == "accepted"
    active = store.load_active_guide()
    active_item_ids = [item["item_id"] for item in active["items"]]
    assert "task_progress" in active_item_ids
    assert "item_01" in active_item_ids
    assert "acc_01" in active_item_ids
    item_options = {
        item["item_id"]: [option["option_id"] for option in item.get("options", [])]
        for item in active["items"]
    }
    assert "done" in item_options["item_01"]
    assert item_options["acc_01"] == ["passed", "blocked"]


def test_spec482_direct_task_item_done_updates_task_item(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理周报",
            "items": [{"item_id": "item_01", "title": "读取数据"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "输出 report.md",
                    "item_refs": ["item_01"],
                    "required": True,
                }
            ],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "item_01",
                    "option_id": "done",
                    "fields": {"evidence_refs": ["output/report.md"]},
                    "reason": "报告已写出。",
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_update"]["updated_items"] == ["item_01"]
    assert receipt["task_acceptance"]["allowed"] is False
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["status"] == "done"
    assert guide["items"][0]["evidence_refs"] == ["output/report.md"]
    assert guide["acceptance"][0]["status"] == "pending"
    assert store.get("base.active_guide") == f"task:{task_id}"


def test_spec513_direct_task_item_done_refreshes_active_projection(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理周报",
            "items": [{"item_id": "item_01", "title": "读取数据"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "输出 report.md",
                    "item_refs": ["item_01"],
                    "required": True,
                }
            ],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "item_01",
                    "option_id": "done",
                    "fields": {"evidence_refs": ["output/report.md"]},
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    active = store.load_guide(f"task:{task_id}")
    active_item = next(
        item for item in active["items"] if item["item_id"] == "item_01"
    )
    assert active_item["status"] == "done"
    assert active_item["evidence_refs"] == ["output/report.md"]


def test_spec513_direct_acceptance_done_refreshes_active_projection(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理周报",
            "items": [{"item_id": "item_01", "title": "读取数据"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "输出 report.md",
                    "item_refs": ["item_01"],
                    "required": True,
                }
            ],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "acc_01",
                    "option_id": "passed",
                    "fields": {"evidence_refs": ["output/report.md"]},
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is False
    active = store.load_guide(f"task:{task_id}")
    active_acceptance = next(
        item for item in active["items"] if item["item_id"] == "acc_01"
    )
    assert active_acceptance["status"] == "passed"
    assert active_acceptance["evidence_refs"] == ["output/report.md"]
    assert active["acceptance"][0]["status"] == "passed"
    assert active["acceptance"][0]["evidence_refs"] == ["output/report.md"]


def test_spec482_direct_acceptance_done_can_complete_task(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理周报",
            "items": [
                {
                    "item_id": "item_01",
                    "title": "读取数据",
                    "status": "done",
                    "evidence_refs": ["output/report.md"],
                }
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "description": "输出 report.md",
                    "item_refs": ["item_01"],
                    "required": True,
                }
            ],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "acc_01",
                    "option_id": "done",
                    "fields": {"evidence_refs": ["output/report.md"]},
                    "reason": "验收项已满足。",
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_update"]["updated_acceptance"] == ["acc_01"]
    assert receipt["task_acceptance"]["allowed"] is True
    assert receipt["task_completion"]["status"] == "completed"
    assert receipt["next_action"] == "natural_final_reply"
    assert store.get("base.active_task") is None
    assert store.get("base.active_guide") is None


def test_spec482_direct_done_requires_evidence_refs(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理周报",
            "items": [{"item_id": "item_01", "title": "读取数据"}],
            "acceptance": [{"acceptance_id": "acc_01", "description": "输出"}],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {"item_id": "item_01", "option_id": "done", "fields": {}}
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_required"
    assert receipt["details"] == {"missing_evidence_refs": ["items:item_01"]}


def test_spec541_direct_task_item_completed_alias_normalizes_to_done(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理报告",
            "items": [{"item_id": "item_01", "title": "写出报告"}],
            "acceptance": [{"acceptance_id": "acc_01", "description": "报告存在"}],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "item_01",
                    "option_id": "completed",
                    "fields": {"evidence_refs": ["output/report.md"]},
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["status"] == "done"
    active = store.load_guide(f"task:{task_id}")
    active_item = next(item for item in active["items"] if item["item_id"] == "item_01")
    assert active_item["status"] == "done"


def test_spec541_direct_acceptance_completed_alias_normalizes_to_passed(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理报告",
            "items": [
                {
                    "item_id": "item_01",
                    "title": "写出报告",
                    "status": "done",
                    "evidence_refs": ["output/report.md"],
                }
            ],
            "acceptance": [{"acceptance_id": "acc_01", "description": "报告存在"}],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "acc_01",
                    "option_id": "completed",
                    "fields": {"evidence_refs": ["output/report.md"]},
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["acceptance"][0]["status"] == "passed"


def test_spec541_direct_completed_alias_still_requires_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "整理报告",
            "items": [{"item_id": "item_01", "title": "写出报告"}],
            "acceptance": [{"acceptance_id": "acc_01", "description": "报告存在"}],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {"item_id": "item_01", "option_id": "completed", "fields": {}}
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_required"


def test_spec439_task_bootstrap_rejects_file_source_without_prior_read_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务文件后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "读取任务文件并执行",
                        "items": ["读取任务文件", "完成任务"],
                        "acceptance": ["输出产物"],
                        "source_refs": [source_path],
                    },
                }
            ],
        },
        evidence_context={"prior_general_tool_results": []},
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_source_not_read"
    assert receipt["details"]["missing_source_refs"] == [source_path]
    assert "file_read" in receipt["details"]["message"]
    assert "web_fetch" in receipt["details"]["message"]
    assert "file_read" in receipt["details"]["repair_hint"]
    assert "web_fetch" in receipt["details"]["repair_hint"]
    assert "need_more_sources" not in receipt["details"]["message"]
    assert "blocked_by_missing_access" not in receipt["details"]["message"]
    assert store.get("base.active_guide") == "task_bootstrap"
    assert store.get("base.active_task") in (None, "")


def test_spec439_task_bootstrap_rejects_same_iteration_source_tools(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务文件后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "读取任务文件并执行",
                        "items": ["读取任务文件", "完成任务"],
                        "acceptance": ["输出产物"],
                        "source_refs": [source_path],
                    },
                }
            ],
        },
        evidence_context={
            "current_general_tool_requests": [
                {"tool_id": "file_read", "path": source_path}
            ],
            "prior_general_tool_results": [],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_wait_for_tool_results"
    assert receipt["details"]["current_tool_ids"] == ["file_read"]
    assert store.get("base.active_guide") == "task_bootstrap"


def test_spec593_task_bootstrap_waits_even_with_workbench_source_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "SealGate-01"
    task_root.mkdir()
    source_path = task_root / "00_BRIEF.md"
    source_path.write_text("任务说明", encoding="utf-8")
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.append_source_read_evidence({
        "tool_id": "file_read",
        "status": "ok",
        "path": str(source_path),
        "round": 12,
        "iteration": 3,
        "task_root": str(task_root),
    })
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "SealGate",
                        "task_goal": "完成封包试炼",
                        "source_refs": ["00_BRIEF.md"],
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": "00_BRIEF.md",
                                "summary": "完成说明中的任务",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "执行任务",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "有交付产物",
                                "item_refs": ["item_01"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [],
            "current_general_tool_requests": [
                {"tool_id": "file_read", "path": str(source_path)}
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_wait_for_tool_results"


def test_spec439_task_bootstrap_accepts_file_source_after_prior_read_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务文件后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "读取任务文件并执行",
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": source_path,
                                "summary": "读取任务文件并执行其中的明确任务",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "完成任务文件要求",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "输出产物",
                                "item_refs": ["item_01"],
                            }
                        ],
                        "source_refs": [source_path],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_read",
                    "status": "ok",
                    "path": source_path,
                    "start_line": 1,
                    "end_line": 160,
                }
            ]
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    assert task_id
    guide = store.load_task_guide(task_id)
    assert guide["source_refs"] == [source_path]
    assert guide["source_requirements"][0]["requirement_id"] == "req_01"
    assert guide["items"][0]["requirement_refs"] == ["req_01"]
    assert guide["acceptance"][0]["item_refs"] == ["item_01"]


def test_spec593_task_bootstrap_accepts_workbench_source_evidence_across_rounds(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "SealGate-01"
    task_root.mkdir()
    source_path = task_root / "00_BRIEF.md"
    source_path.write_text("任务说明", encoding="utf-8")
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.append_source_read_evidence({
        "tool_id": "file_read",
        "status": "ok",
        "path": str(source_path),
        "round": 11,
        "iteration": 2,
        "task_root": str(task_root),
    })
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "SealGate",
                        "task_goal": "完成封包试炼",
                        "source_refs": ["00_BRIEF.md"],
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": "00_BRIEF.md",
                                "summary": "完成说明中的任务",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "执行任务",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "有交付产物",
                                "item_refs": ["item_01"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [],
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert guide["source_requirements"][0]["source_ref"] == "00_BRIEF.md"


def test_spec593_workbench_source_evidence_is_scoped_by_task_root(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    old_root = tmp_path / "OldPack"
    new_root = tmp_path / "NewPack"
    old_root.mkdir()
    new_root.mkdir()
    old_source = old_root / "00_BRIEF.md"
    new_source = new_root / "00_BRIEF.md"
    old_source.write_text("旧任务说明", encoding="utf-8")
    new_source.write_text("新任务说明", encoding="utf-8")
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.append_source_read_evidence({
        "tool_id": "file_read",
        "status": "ok",
        "path": str(old_source),
        "round": 8,
        "iteration": 1,
        "task_root": str(old_root),
    })
    create_task_bootstrap_guide(store, reason="用户要求读取新任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "新任务",
                        "task_goal": "完成新任务包",
                        "source_refs": ["00_BRIEF.md"],
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": "00_BRIEF.md",
                                "summary": "完成新说明中的任务",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "执行新任务",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "有新任务交付产物",
                                "item_refs": ["item_01"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(new_root),
            "prior_general_tool_results": [],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_source_not_read"
    assert receipt["details"]["missing_source_refs"] == ["00_BRIEF.md"]


def test_spec514_bootstrap_defaults_single_source_and_one_to_one_refs(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务文件后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "读取任务文件并执行",
                        "source_refs": [source_path],
                        "source_requirements": [
                            {"req_id": "req_01", "summary": "完成第一项任务"},
                            {"req_id": "req_02", "summary": "完成第二项任务"},
                        ],
                        "items": [
                            {"item_id": "item_01", "summary": "处理第一项"},
                            {"item_id": "item_02", "summary": "处理第二项"},
                        ],
                        "acceptance": [
                            {"acc_id": "acc_01", "summary": "第一项产物存在"},
                            {"acc_id": "acc_02", "summary": "第二项产物存在"},
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_read",
                    "status": "ok",
                    "path": source_path,
                    "start_line": 1,
                    "end_line": 160,
                }
            ]
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert [item["source_ref"] for item in guide["source_requirements"]] == [
        source_path,
        source_path,
    ]
    assert guide["items"][0]["requirement_refs"] == ["req_01"]
    assert guide["items"][1]["requirement_refs"] == ["req_02"]
    assert guide["acceptance"][0]["item_refs"] == ["item_01"]
    assert guide["acceptance"][1]["item_refs"] == ["item_02"]


def test_spec453_task_bootstrap_accepts_source_requirement_anchors_after_prior_read(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务文件后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "读取任务文件并逐条执行",
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": source_path + "#01",
                                "summary": "完成任务文件第 01 项要求",
                            },
                            {
                                "requirement_id": "req_02",
                                "source_ref": "agent_eval_tasks.md#02",
                                "summary": "完成任务文件第 02 项要求",
                            },
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "完成第 01 项",
                                "requirement_refs": ["req_01"],
                            },
                            {
                                "item_id": "item_02",
                                "title": "完成第 02 项",
                                "requirement_refs": ["req_02"],
                            },
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "第 01 项有产物",
                                "item_refs": ["item_01"],
                            },
                            {
                                "acceptance_id": "acc_02",
                                "description": "第 02 项有产物",
                                "item_refs": ["item_02"],
                            },
                        ],
                        "source_refs": [source_path],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_read",
                    "status": "ok",
                    "path": source_path,
                    "start_line": 1,
                    "end_line": 160,
                }
            ]
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert [item["requirement_id"] for item in guide["source_requirements"]] == [
        "req_01",
        "req_02",
    ]


def test_spec511_task_bootstrap_adapter_accepts_aliases_and_glob_sources(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "DFT_AGENT_EVAL"
    inbox = task_root / "inbox"
    inbox.mkdir(parents=True)
    task_file = task_root / "agent_eval_tasks.md"
    note_a = inbox / "meeting_notes.md"
    note_b = inbox / "todo_dump.md"
    task_file.write_text("1. 查找框架\n2. 整理 inbox\n", encoding="utf-8")
    note_a.write_text("会议材料", encoding="utf-8")
    note_b.write_text("待办材料", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "完成任务文件与 inbox 中的任务",
                        "source_refs": [
                            str(task_file),
                            "DFT_AGENT_EVAL/inbox/*.md",
                        ],
                        "source_requirements": [
                            {
                                "req_id": "req_01",
                                "source_ref": "agent_eval_tasks.md#task-01",
                                "title": "完成任务文件中的框架检索任务",
                            },
                            {
                                "id": "req_02",
                                "source_ref": "meeting_notes.md",
                                "description": "整理已经读取的 inbox 材料",
                            },
                            {
                                "requirement_id": "req_03",
                                "source_ref": "todo_dump.md",
                                "summary": "整理已经读取的待办材料",
                            },
                        ],
                        "items": [
                            {
                                "id": "task_01",
                                "summary": "输出框架对比结果",
                                "requirement_refs": ["req_01"],
                            },
                            {
                                "item_id": "task_02",
                                "description": "整理 inbox 索引和待办",
                                "requirement_refs": ["req_02", "req_03"],
                            },
                        ],
                        "acceptance": [
                            {
                                "acc_id": "acc_01",
                                "title": "生成 output/01_frameworks.md",
                                "item_refs": ["task_01"],
                            },
                            {
                                "id": "acc_02",
                                "summary": "生成 output/03_index.md",
                                "item_refs": ["task_02"],
                            },
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": str(task_file)},
                {"tool_id": "file_read", "status": "ok", "path": str(note_a)},
                {"tool_id": "file_read", "status": "ok", "path": str(note_b)},
            ],
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert [item["requirement_id"] for item in guide["source_requirements"]] == [
        "req_01",
        "req_02",
        "req_03",
    ]
    assert guide["source_requirements"][0]["summary"] == "完成任务文件中的框架检索任务"
    assert guide["items"][0]["item_id"] == "task_01"
    assert guide["items"][0]["title"] == "输出框架对比结果"
    assert guide["acceptance"][0]["acceptance_id"] == "acc_01"
    assert guide["acceptance"][1]["description"] == "生成 output/03_index.md"


def test_spec532_task_bootstrap_accepts_full_material_source_refs_with_task_only_requirements(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "DFT_AGENT_EVAL"
    inbox = task_root / "inbox"
    data_dir = task_root / "data"
    debug_dir = task_root / "debug"
    inbox.mkdir(parents=True)
    data_dir.mkdir()
    debug_dir.mkdir()
    task_file = task_root / "agent_eval_tasks.md"
    inbox_note = inbox / "meeting_notes.md"
    sales_csv = data_dir / "sales.csv"
    buggy_script = debug_dir / "task_sort_buggy.py"
    task_file.write_text("1. 查找框架\n2. 修复脚本\n", encoding="utf-8")
    inbox_note.write_text("辅助会议材料", encoding="utf-8")
    sales_csv.write_text("region,amount\n华东,100\n", encoding="utf-8")
    buggy_script.write_text("def sort_tasks(x):\n    return x\n", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "完成任务文件列出的 12 项任务",
                        "source_refs": [
                            str(task_file),
                            str(inbox_note),
                            str(sales_csv),
                            str(buggy_script),
                        ],
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": "agent_eval_tasks.md#01",
                                "summary": "完成任务文件第 01 项框架检索任务",
                            },
                            {
                                "requirement_id": "req_02",
                                "source_ref": "agent_eval_tasks.md#05",
                                "summary": "完成任务文件第 05 项脚本修复任务",
                            },
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "输出框架检索结果",
                                "requirement_refs": ["req_01"],
                            },
                            {
                                "item_id": "item_02",
                                "title": "修复预置 buggy 脚本",
                                "requirement_refs": ["req_02"],
                            },
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "生成 output/01_frameworks.md",
                                "item_refs": ["item_01"],
                            },
                            {
                                "acceptance_id": "acc_02",
                                "description": "生成 output/05_task_sort_fixed.py 并运行验证",
                                "item_refs": ["item_02"],
                            },
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": str(task_file)},
                {"tool_id": "file_read", "status": "ok", "path": str(inbox_note)},
                {"tool_id": "file_read", "status": "ok", "path": str(sales_csv)},
                {"tool_id": "file_read", "status": "ok", "path": str(buggy_script)},
            ],
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert guide["source_refs"] == [
        str(task_file),
        str(inbox_note),
        str(sales_csv),
        str(buggy_script),
    ]
    assert [item["source_ref"] for item in guide["source_requirements"]] == [
        "agent_eval_tasks.md#01",
        "agent_eval_tasks.md#05",
    ]
    assert guide["items"][1]["requirement_refs"] == ["req_02"]


def test_spec544_task_bootstrap_strips_source_ref_annotations(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "DFT_AGENT_EVAL"
    inbox = task_root / "inbox"
    inbox.mkdir(parents=True)
    task_file = task_root / "agent_eval_tasks.md"
    note_a = inbox / "error_record.md"
    note_b = inbox / "meeting_notes.md"
    task_file.write_text("1. framework\n", encoding="utf-8")
    note_a.write_text("note a\n", encoding="utf-8")
    note_b.write_text("note b\n", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="agent eval")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "Agent eval",
                        "source_refs": [
                            "DFT_AGENT_EVAL/agent_eval_tasks.md（任务清单，160 行）",
                            "DFT_AGENT_EVAL/inbox/*.md（6 个候选文件）",
                        ],
                        "source_requirements": [
                            {
                                "req_id": "req_01",
                                "source_ref": "agent_eval_tasks.md#01（任务 01）",
                                "summary": "Read the framework task.",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "Write framework report",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acc_id": "acc_01",
                                "item_ref": "item_01",
                                "summary": "output/01_frameworks.md exists",
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": str(task_file)},
                {"tool_id": "file_read", "status": "ok", "path": str(note_a)},
                {"tool_id": "file_read", "status": "ok", "path": str(note_b)},
            ],
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert guide["source_refs"] == [
        "DFT_AGENT_EVAL/agent_eval_tasks.md",
        "DFT_AGENT_EVAL/inbox/*.md",
    ]
    assert guide["source_requirements"][0]["source_ref"] == "agent_eval_tasks.md#01"


def test_spec545_task_bootstrap_preserves_url_parentheses(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    url = "https://en.wikipedia.org/wiki/Foo_(bar)"
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="url source")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "网页来源任务",
                        "source_refs": [url],
                        "source_requirements": [
                            {
                                "req_id": "req_01",
                                "source_ref": url,
                                "summary": "使用已抓取的网页来源。",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "整理网页要点",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acc_id": "acc_01",
                                "item_ref": "item_01",
                                "summary": "输出包含该网页的要点。",
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {"tool_id": "web_fetch", "status": "ok", "url": url}
            ],
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert guide["source_refs"] == [url]
    assert guide["source_requirements"][0]["source_ref"] == url


def test_spec545_source_ref_annotation_boundary():
    from logic.guide_submit import _strip_source_ref_annotation

    assert (
        _strip_source_ref_annotation("DFT_AGENT_EVAL/agent_eval_tasks.md（任务清单，160 行）")
        == "DFT_AGENT_EVAL/agent_eval_tasks.md"
    )
    assert (
        _strip_source_ref_annotation("DFT_AGENT_EVAL/inbox/*.md（6 个候选文件）")
        == "DFT_AGENT_EVAL/inbox/*.md"
    )
    assert (
        _strip_source_ref_annotation("agent_eval_tasks.md#01（任务 01）")
        == "agent_eval_tasks.md#01"
    )
    assert (
        _strip_source_ref_annotation("DFT_AGENT_EVAL/agent_eval_tasks.md (task list)")
        == "DFT_AGENT_EVAL/agent_eval_tasks.md"
    )
    assert (
        _strip_source_ref_annotation("https://en.wikipedia.org/wiki/Foo_(bar)")
        == "https://en.wikipedia.org/wiki/Foo_(bar)"
    )
    assert (
        _strip_source_ref_annotation("https://example.com/path(1)")
        == "https://example.com/path(1)"
    )


def test_spec544_task_bootstrap_missing_fields_explains_complete_initial_ledger(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="agent eval")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {"source_refs": ["DFT_AGENT_EVAL/agent_eval_tasks.md"]},
                }
            ],
        },
        evidence_context={},
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "missing_guide_fields"
    assert "one complete submission" in receipt["details"]["message"]
    assert "do not submit only source_refs" in receipt["details"]["message"]


def test_spec535_task_bootstrap_defaults_missing_requirement_sources_to_task_file(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "DFT_AGENT_EVAL"
    inbox = task_root / "inbox"
    data_dir = task_root / "data"
    debug_dir = task_root / "debug"
    inbox.mkdir(parents=True)
    data_dir.mkdir()
    debug_dir.mkdir()
    task_file = task_root / "agent_eval_tasks.md"
    inbox_note = inbox / "meeting_notes.md"
    sales_csv = data_dir / "sales.csv"
    buggy_script = debug_dir / "task_sort_buggy.py"
    task_file.write_text("1. 查找框架\n2. 修复脚本\n", encoding="utf-8")
    inbox_note.write_text("辅助会议材料", encoding="utf-8")
    sales_csv.write_text("region,amount\n华东,100\n", encoding="utf-8")
    buggy_script.write_text("def sort_tasks(x):\n    return x\n", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "完成任务文件列出的任务",
                        "source_refs": [
                            str(task_file),
                            str(inbox_note),
                            str(sales_csv),
                            str(buggy_script),
                        ],
                        "source_requirements": [
                            {"req_id": "req_01", "summary": "完成任务 01 框架检索"},
                            {"req_id": "req_02", "summary": "完成任务 05 脚本修复"},
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "输出框架检索结果",
                                "requirement_refs": ["req_01"],
                            },
                            {
                                "item_id": "item_02",
                                "title": "修复预置 buggy 脚本",
                                "requirement_refs": ["req_02"],
                            },
                        ],
                        "acceptance": [
                            {
                                "acc_id": "acc_01",
                                "description": "生成 output/01_frameworks.md",
                                "item_refs": ["item_01"],
                            },
                            {
                                "acc_id": "acc_02",
                                "description": "生成 output/05_task_sort_fixed.py 并运行验证",
                                "item_refs": ["item_02"],
                            },
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": str(task_file)},
                {"tool_id": "file_read", "status": "ok", "path": str(inbox_note)},
                {"tool_id": "file_read", "status": "ok", "path": str(sales_csv)},
                {"tool_id": "file_read", "status": "ok", "path": str(buggy_script)},
            ],
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert [item["source_ref"] for item in guide["source_requirements"]] == [
        str(task_file),
        str(task_file),
    ]


def test_spec535_task_bootstrap_defaults_relative_task_file_source_ref(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "DFT_AGENT_EVAL"
    task_root.mkdir()
    task_file = task_root / "agent_eval_tasks.md"
    task_file.write_text("1. 输出报告\n", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "完成任务文件列出的任务",
                        "source_refs": ["agent_eval_tasks.md", "notes.md"],
                        "source_requirements": [
                            {"req_id": "req_01", "summary": "完成任务 01"}
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "输出报告",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acc_id": "acc_01",
                                "description": "生成 output/report.md",
                                "item_refs": ["item_01"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": str(task_file)},
                {"tool_id": "file_read", "status": "ok", "path": str(task_root / "notes.md")},
            ],
        },
    )

    assert receipt["status"] == "accepted"
    task_id = store.get("base.active_task")
    guide = store.load_task_guide(task_id)
    assert guide["source_requirements"][0]["source_ref"] == "agent_eval_tasks.md"


def test_spec532_task_bootstrap_still_rejects_unknown_requirement_source_ref(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "DFT_AGENT_EVAL"
    task_root.mkdir()
    task_file = task_root / "agent_eval_tasks.md"
    task_file.write_text("1. 查找框架\n", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "完成任务文件要求",
                        "source_refs": [str(task_file)],
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": "unread_notes.md#01",
                                "summary": "完成未读材料里的任务",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "输出结果",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "生成结果文件",
                                "item_refs": ["item_01"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": str(task_file)}
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_source_requirement_ref_unknown"
    assert receipt["details"]["unknown_source_refs"] == ["unread_notes.md#01"]


def test_spec511_task_bootstrap_rejects_uncovered_glob_source(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    task_root = tmp_path / "DFT_AGENT_EVAL"
    inbox = task_root / "inbox"
    inbox.mkdir(parents=True)
    read_note = inbox / "meeting_notes.md"
    unread_note = inbox / "todo_dump.md"
    read_note.write_text("已读材料", encoding="utf-8")
    unread_note.write_text("未读材料", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "整理 inbox",
                        "source_refs": ["DFT_AGENT_EVAL/inbox/*.md"],
                        "source_requirements": [
                            {
                                "req_id": "req_01",
                                "source_ref": "DFT_AGENT_EVAL/inbox/*.md",
                                "summary": "整理 inbox 材料",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "task_01",
                                "title": "整理 inbox",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acc_id": "acc_01",
                                "description": "输出 inbox 索引",
                                "item_refs": ["task_01"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "task_root": str(task_root),
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": str(read_note)}
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_source_not_read"
    assert receipt["details"]["missing_source_refs"] == ["DFT_AGENT_EVAL/inbox/*.md"]
    assert str(unread_note) in receipt["details"]["unread_glob_matches"]


def test_spec511_task_bootstrap_invalid_requirement_feedback_is_actionable(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取任务包后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "读取任务文件后执行",
                        "source_refs": [source_path],
                        "source_requirements": [
                            {
                                "req_id": "req_01",
                                "source_ref": source_path,
                            }
                        ],
                        "items": [
                            {
                                "item_id": "task_01",
                                "title": "完成任务",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acc_id": "acc_01",
                                "description": "输出产物",
                                "item_refs": ["task_01"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {"tool_id": "file_read", "status": "ok", "path": source_path}
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_invalid_source_requirements"
    assert "source_requirements=[{requirement_id, source_ref, summary}]" in receipt["details"]["repair_hint"]
    assert receipt["details"]["invalid_requirements"] == [
        {
            "requirement_id": "req_01",
            "index": 1,
            "missing_fields": ["summary"],
            "repair_hint": "为该来源需求补充中文自然语言 summary/title/description。",
        }
    ]


def test_spec452_task_bootstrap_rejects_file_source_without_source_requirements(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取来源材料后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "读取任务文件并执行",
                        "items": ["完成本地任务", "完成检索任务"],
                        "acceptance": ["输出产物"],
                        "source_refs": [source_path],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_read",
                    "status": "ok",
                    "path": source_path,
                    "content": "任务材料正文可以是任意格式，Runtime 不解析。",
                }
            ]
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_source_requirements_required"
    assert "source_requirements=[{requirement_id, source_ref, summary}]" in receipt["details"]["repair_hint"]
    assert store.get("base.active_guide") == "task_bootstrap"
    assert store.get("base.active_task") in (None, "")


def test_spec452_task_bootstrap_rejects_uncovered_source_requirements(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取来源材料后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\tasks.png"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "图片任务整理",
                        "task_goal": "按图片中的要求产出结果",
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": source_path,
                                "summary": "完成图片中的第一组要求",
                            },
                            {
                                "requirement_id": "req_02",
                                "source_ref": source_path,
                                "summary": "完成图片中的第二组要求",
                            },
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "完成第一组要求",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "第一组要求有产物",
                                "item_refs": ["item_01"],
                            }
                        ],
                        "source_refs": [source_path],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_read",
                    "status": "ok",
                    "path": source_path,
                    "content": "[binary/image observation rendered for the model]",
                }
            ]
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_source_requirement_coverage_missing"
    assert receipt["details"]["requirement_ids"] == ["req_02"]
    assert store.get("base.active_guide") == "task_bootstrap"


def test_spec452_task_bootstrap_rejects_items_without_acceptance_coverage(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户要求读取来源材料后执行")

    source_path = r"D:\AI_WORKSPACE\base\example\agent_eval_tasks.md"
    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "source_requirements": [
                            {
                                "requirement_id": "req_01",
                                "source_ref": source_path,
                                "summary": "完成来源材料要求",
                            }
                        ],
                        "items": [
                            {
                                "item_id": "item_01",
                                "title": "执行来源材料要求",
                                "requirement_refs": ["req_01"],
                            },
                            {
                                "item_id": "item_02",
                                "title": "复核来源材料要求",
                                "requirement_refs": ["req_01"],
                            }
                        ],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "description": "验收描述存在但无法确定覆盖哪个任务项",
                            }
                        ],
                        "source_refs": [source_path],
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_read",
                    "status": "ok",
                    "path": source_path,
                    "content": "来源材料正文",
                }
            ]
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "bootstrap_acceptance_refs_required"
    assert receipt["details"]["acceptance"] == ["acc_01"]


def test_spec439_task_bootstrap_does_not_treat_runtime_refs_as_source_files(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(
        store,
        reason="合轮交互债务延后显影",
        source_refs=[
            "round:591",
            "interaction_debt",
            r"task_root:D:\AI_WORKSPACE\base\example\DFT_AGENT_EVAL",
        ],
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "日常能力测试",
                        "task_goal": "根据已知用户请求执行任务",
                        "items": ["确认任务目标", "完成任务"],
                        "acceptance": ["输出产物"],
                        "source_refs": ["round:591", "interaction_debt"],
                    },
                }
            ],
        },
        evidence_context={"prior_general_tool_results": []},
    )

    assert receipt["status"] == "accepted"
    assert store.get("base.active_task")


def test_spec434_bootstrap_string_checklists_become_required_records(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="用户派发多步骤工程任务")

    apply_guide_submit(
        store,
        {
            "guide_id": "task_bootstrap",
            "submissions": [
                {
                    "item_id": "build_initial_task_guide",
                    "option_id": "submit_initial_guide",
                    "fields": {
                        "task_title": "验证实现",
                        "items": ["读取相关文件", "运行本地测试"],
                        "acceptance": ["报告文件存在", "最终回复给出证据路径"],
                    },
                }
            ],
        },
    )

    guide = store.load_task_guide(store.get("base.active_task"))

    assert guide["items"] == [
        {
            "item_id": "item_01",
            "title": "读取相关文件",
            "required": True,
            "status": "open",
        },
        {
            "item_id": "item_02",
            "title": "运行本地测试",
            "required": True,
            "status": "open",
        },
    ]
    assert guide["acceptance"] == [
        {
            "acceptance_id": "acc_01",
            "description": "报告文件存在",
            "required": True,
            "status": "pending",
        },
        {
            "acceptance_id": "acc_02",
            "description": "最终回复给出证据路径",
            "required": True,
            "status": "pending",
        },
    ]


def test_spec434_task_acceptance_blocks_finish_until_required_items_done(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="整理周报",
        task_goal="生成周报并验证数据",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "kind": "artifact_exists",
                    "required": True,
                    "target": "output/report.md",
                    "status": "pending",
                }
            ],
        },
    )
    store.set("base.active_task", task_id)

    result = validate_task_closeout(store, {"closeout_decision": "finish"})

    assert result["allowed"] is False
    assert result["reason"] == "task_acceptance_blocked"
    assert "item_01" in result["blockers"]
    assert "acc_01" in result["blockers"]


def test_spec613_task_acceptance_marks_evidenced_blocked_ledger_terminal(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="SealGate NO-GO",
        task_goal="完成可行项并如实登记不可继续项",
        guide={
            "items": [
                {
                    "item_id": "item_output",
                    "required": True,
                    "status": "done",
                    "evidence_refs": ["EV-output"],
                },
                {
                    "item_id": "item_memory",
                    "required": True,
                    "status": "blocked",
                    "evidence_refs": ["EV-identity-unresolved"],
                },
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_output",
                    "required": True,
                    "status": "passed",
                    "evidence_refs": ["EV-output"],
                },
                {
                    "acceptance_id": "acc_memory",
                    "required": True,
                    "status": "blocked",
                    "evidence_refs": ["EV-identity-unresolved"],
                },
            ],
        },
    )
    store.set("base.active_task", task_id)

    result = validate_task_closeout(store, {"closeout_decision": "finish"})

    assert result["allowed"] is False
    assert result["reason"] == "task_acceptance_blocked"
    assert result["terminal_blocked"] is True
    assert result["blockers"] == ["item_memory", "acc_memory"]


def test_spec613_blocked_without_evidence_is_not_terminal(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Naked blocked claim",
        task_goal="不得把无证据 blocked 当成终态",
        guide={
            "items": [
                {"item_id": "item_01", "required": True, "status": "blocked"}
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "required": True,
                    "status": "blocked",
                }
            ],
        },
    )
    store.set("base.active_task", task_id)

    result = validate_task_closeout(store, {"closeout_decision": "finish"})

    assert result["allowed"] is False
    assert result["terminal_blocked"] is False


def test_spec436_task_bootstrap_blocks_finish_until_guide_submitted(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_acceptance import validate_task_closeout
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    create_task_bootstrap_guide(store, reason="合轮交互债务延后显影")

    result = validate_task_closeout(store, {"closeout_decision": "finish"})

    assert result["allowed"] is False
    assert result["reason"] == "task_bootstrap_pending"
    assert result["blockers"] == ["task_bootstrap"]


def test_spec434_task_acceptance_allows_finish_after_required_done(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="整理周报",
        task_goal="生成周报并验证数据",
        guide={
            "items": [
                {
                    "item_id": "item_01",
                    "required": True,
                    "status": "done",
                    "evidence_refs": ["call_write_report"],
                }
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "kind": "artifact_exists",
                    "required": True,
                    "target": "output/report.md",
                    "status": "passed",
                    "evidence_refs": ["call_write_report"],
                }
            ],
        },
    )
    store.set("base.active_task", task_id)

    result = validate_task_closeout(store, {"closeout_decision": "finish"})

    assert result["allowed"] is True
    assert result["reason"] == "task_acceptance_passed"


def test_spec434_task_execution_guide_submit_updates_items_and_acceptance(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="整理周报",
        task_goal="生成周报并验证数据",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "kind": "artifact_exists",
                    "required": True,
                    "target": "output/report.md",
                    "status": "pending",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": ["items", "acceptance"],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "evidence_refs": ["call_write_report"],
                    "fields": {
                        "items": [{"item_id": "item_01", "status": "done"}],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "status": "passed",
                                "evidence_refs": ["call_write_report"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_completion"]["status"] == "completed"
    assert store.get("base.active_task") is None
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["status"] == "done"
    assert guide["acceptance"][0]["status"] == "passed"
    assert validate_task_closeout(store, {"closeout_decision": "finish"})["allowed"] is True
    ledger_path = (
        tmp_path
        / "workbench"
        / "output"
        / task_id
        / "acceptance_ledger.jsonl"
    )
    ledger_text = ledger_path.read_text(encoding="utf-8")
    assert "acceptance_updated" in ledger_text
    assert "acc_01" in ledger_text


def test_spec523_task_progress_accepts_id_keyed_records_with_evidence_alias(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="整理报告",
        task_goal="写出报告并验收",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "required": True,
                    "status": "pending",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": ["items", "acceptance"],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence": ["call_write_report"],
                            }
                        },
                        "acceptance": {
                            "acc_01": {
                                "status": "passed",
                                "evidence": ["call_write_report"],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["evidence_refs"] == ["call_write_report"]
    assert guide["acceptance"][0]["evidence_refs"] == ["call_write_report"]


def test_spec551_task_progress_rejects_empty_update_without_changing_ledger(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="12 项任务狗粮",
        task_goal="完成产物并登记账本。",
        guide={
            "items": [{"item_id": "task_01", "required": True, "status": "open"}],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "required": True,
                    "status": "pending",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "reason": "任务 01 已完成，验收也通过。",
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_status_update_empty"
    assert "fields.items" in receipt["details"]["hint"]
    assert "fields.acceptance" in receipt["details"]["hint"]
    assert "不要只写 reason" in receipt["details"]["hint"]
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["status"] == "open"
    assert guide["acceptance"][0]["status"] == "pending"
    ledger_path = (
        tmp_path
        / "workbench"
        / "process"
        / task_id
        / "acceptance_ledger.jsonl"
    )
    ledger_text = ledger_path.read_text(encoding="utf-8")
    assert "acceptance_updated" not in ledger_text


def test_spec434_task_progress_rejects_unknown_checklist_ids(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="验证实现",
        task_goal="生成报告",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": [{"item_id": "invented_item", "status": "done"}],
                        "acceptance": [
                            {"acceptance_id": "invented_acc", "status": "passed"}
                        ],
                    },
                }
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "unknown_task_guide_records"
    assert receipt["details"] == {
        "items": ["invented_item"],
        "acceptance": ["invented_acc"],
    }
    guide = store.load_task_guide(task_id)
    assert guide["items"] == [
        {"item_id": "item_01", "required": True, "status": "open"}
    ]
    assert guide["acceptance"] == [
        {"acceptance_id": "acc_01", "required": True, "status": "pending"}
    ]
    assert store.get("base.active_guide") == f"task:{task_id}"


def test_spec456_task_progress_accepts_acceptance_item_id_alias(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Web eval",
        task_goal="Verify web tool behavior",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": ["call_write_report"],
                            }
                        },
                        "acceptance": [
                            {
                                "item_id": "acc_01",
                                "status": "done",
                                "evidence_refs": ["call_write_report"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is True
    guide = store.load_task_guide(task_id)
    assert guide["acceptance"][0]["acceptance_id"] == "acc_01"
    assert guide["acceptance"][0]["status"] == "done"
    assert validate_task_closeout(store, {"closeout_decision": "finish"})["allowed"] is True


def test_spec456_task_progress_rejects_acceptance_update_without_id(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Web eval",
        task_goal="Verify web tool behavior",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "acceptance": [
                            {
                                "status": "done",
                                "evidence_refs": ["call_write_report"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "missing_task_guide_record_ids"
    assert receipt["details"] == {"items": [], "acceptance": ["acceptance[1]"]}
    guide = store.load_task_guide(task_id)
    assert guide["acceptance"][0]["status"] == "pending"


def test_spec448_task_progress_rejects_completion_without_evidence_refs(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce files and verify them",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {"item_01": "done"},
                        "acceptance": {"acc_01": "passed"},
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_required"
    assert receipt["details"]["missing_evidence_refs"] == [
        "items:item_01",
        "acceptance:acc_01",
    ]
    assert store.get("base.active_guide") == f"task:{task_id}"


def test_spec448_task_progress_rejects_unmatched_evidence_refs(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce files and verify them",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "evidence_refs": ["call_missing"],
                    "fields": {
                        "items": {"item_01": "done"},
                        "acceptance": {"acc_01": "passed"},
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_not_found"
    assert receipt["details"]["unknown_evidence_refs"] == ["call_missing"]
    assert "call_write_report" in receipt["details"]["known_evidence_refs"]


def test_spec597_task_progress_rejection_includes_usable_evidence_map(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    evidence_context = _successful_tool_evidence_context()
    file_result = evidence_context["prior_general_tool_results"][0]
    shell_result = evidence_context["prior_general_tool_results"][1]
    file_ev = _spec487_expected_evidence_handle(file_result)
    shell_ev = _spec487_expected_evidence_handle(shell_result)

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="SealGate eval",
        task_goal="Produce files and verify them",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": ["EV-SELFWRITTEN"],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context=evidence_context,
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_not_found"
    assert receipt["details"]["unknown_evidence_refs"] == ["EV-SELFWRITTEN"]
    usable = receipt["details"]["known_evidence_items"]
    assert any(
        item["ref"] == file_ev
        and item["tool_id"] == "file_write"
        and "output\\report.md" in item["summary"]
        for item in usable
    )
    assert any(
        item["ref"] == shell_ev
        and item["tool_id"] == "shell_command"
        and "python output/report.py" in item["summary"]
        for item in usable
    )
    hint = receipt["details"]["hint"]
    assert "报告正文里自写的 EV-*" in hint
    assert "不要全盘搜索 EV 字符串" in hint


def test_spec462_task_progress_accepts_active_corpus_short_id_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Use visible corpus evidence",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "evidence_refs": ["C-00001"],
                    "fields": {
                        "items": {"item_01": "done"},
                        "acceptance": {"acc_01": "passed"},
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [],
            "current_general_tool_requests": [],
            "active_corpus_ids": ["C-00001"],
        },
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["evidence_refs"] == ["C-00001"]
    assert guide["acceptance"][0]["evidence_refs"] == ["C-00001"]


def test_spec462_task_progress_rejects_unknown_active_corpus_short_id(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Use visible corpus evidence",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "evidence_refs": ["C-99999"],
                    "fields": {
                        "items": {"item_01": "done"},
                        "acceptance": {"acc_01": "passed"},
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [],
            "current_general_tool_requests": [],
            "active_corpus_ids": ["C-00001"],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_not_found"
    assert receipt["details"]["unknown_evidence_refs"] == ["C-99999"]


def test_spec450_task_progress_accepts_artifact_path_evidence_aliases(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    root = r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL"
    report_path = root + r"\output\report.md"
    script_path = root + r"\output\script.py"
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce files and verify them",
        guide={
            "items": [
                {"item_id": "item_01", "required": True, "status": "open"},
                {"item_id": "item_02", "required": True, "status": "open"},
            ],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [f"file:{report_path}"],
                            },
                            "item_02": {
                                "status": "done",
                                "evidence_refs": [f"file_read:{script_path}"],
                            },
                        },
                        "acceptance": {
                            "acc_01": {
                                "status": "passed",
                                "evidence_refs": ["output/report.md"],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_write",
                    "status": "ok",
                    "call_id": "call_write_report",
                    "path": report_path,
                },
                {
                    "tool_id": "file_write",
                    "status": "ok",
                    "call_id": "call_write_script",
                    "path": script_path,
                },
                {
                    "tool_id": "shell_command",
                    "status": "ok",
                    "call_id": "call_run_report",
                    "cwd": root,
                    "command": "python output/script.py",
                },
            ],
            "current_general_tool_requests": [],
        },
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is True
    assert receipt["next_action"] == "natural_final_reply"
    assert validate_task_closeout(store, {"closeout_decision": "finish"})["allowed"] is True


def test_spec450_task_progress_rejects_unknown_artifact_path_alias(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    known_path = r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL\output\report.md"
    missing_path = r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL\output\missing.md"
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce files and verify them",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [f"file:{missing_path}"],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_write",
                    "status": "ok",
                    "call_id": "call_write_report",
                    "path": known_path,
                }
            ],
            "current_general_tool_requests": [],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_not_found"
    assert receipt["details"]["unknown_evidence_refs"] == [f"file:{missing_path}"]


def test_spec454_task_progress_accepts_existing_file_uri_under_artifact_root(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    task_root = tmp_path / "DFT_AGENT_EVAL"
    output_root = task_root / "output"
    output_root.mkdir(parents=True)
    report_path = output_root / "04_sales_report.md"
    report_path.write_text("# Sales report\n", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce script-generated artifact",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [f"file:{report_path}"],
                            }
                        },
                        "acceptance": {
                            "acc_01": {
                                "status": "passed",
                                "evidence_refs": [f"file:{report_path}"],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "shell_command",
                    "status": "ok",
                    "call_id": "call_run_report",
                    "cwd": str(task_root),
                    "command": "python output/04_sales_report.py",
                },
            ],
            "current_general_tool_requests": [],
            "task_root": str(task_root),
            "artifact_roots": [str(output_root)],
        },
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is True
    assert receipt["next_action"] == "natural_final_reply"
    assert validate_task_closeout(store, {"closeout_decision": "finish"})["allowed"] is True


def test_spec454_task_progress_rejects_existing_file_uri_outside_artifact_root(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    task_root = tmp_path / "DFT_AGENT_EVAL"
    output_root = task_root / "output"
    outside_root = tmp_path / "outside"
    output_root.mkdir(parents=True)
    outside_root.mkdir()
    outside_path = outside_root / "report.md"
    outside_path.write_text("# Outside\n", encoding="utf-8")

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce only authorized artifacts",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [f"file:{outside_path}"],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [],
            "current_general_tool_requests": [],
            "task_root": str(task_root),
            "artifact_roots": [str(output_root)],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_not_found"
    assert receipt["details"]["unknown_evidence_refs"] == [f"file:{outside_path}"]


def test_spec451_task_progress_accepts_shell_command_evidence_alias(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    script_path = r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL\output\script.py"
    command = "python output/script.py"
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce files and verify command output",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [script_path, command],
                            }
                        },
                        "acceptance": {
                            "acc_01": {
                                "status": "passed",
                                "evidence_refs": [
                                    "call:python output/script.py",
                                    "call_id:python output/script.py",
                                ],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_write",
                    "status": "ok",
                    "call_id": "call_write_script",
                    "path": script_path,
                },
                {
                    "tool_id": "shell_command",
                    "status": "ok",
                    "call_id": "call_run_script",
                    "cwd": r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL",
                    "command": command,
                },
            ],
            "current_general_tool_requests": [],
        },
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["status"] == "done"
    assert guide["acceptance"][0]["status"] == "passed"


def test_spec451_task_progress_rejects_unknown_shell_command_alias(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    command = "python output/script.py"
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Verify command output",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [f"call:{command}"],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "shell_command",
                    "status": "ok",
                    "call_id": "call_run_other",
                    "cwd": r"D:\AI_WORKSPACE\base\dftest91-agent-gemini\DFT_AGENT_EVAL",
                    "command": "python output/other.py",
                },
            ],
            "current_general_tool_requests": [],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_not_found"
    assert receipt["details"]["unknown_evidence_refs"] == [f"call:{command}"]


def test_spec487_task_progress_accepts_shell_compound_subcommand_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    command = (
        "dir /b output && echo ---RUN04--- && python output\\04_sales_report.py "
        "&& echo ---RUN05--- && python output\\05_task_sort_fixed.py"
    )
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Verify compound command output",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [
                                    "call:python output\\04_sales_report.py",
                                ],
                            }
                        },
                        "acceptance": {
                            "acc_01": {
                                "status": "passed",
                                "evidence_refs": [
                                    "command:python output\\05_task_sort_fixed.py",
                                ],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "shell_command",
                    "status": "ok",
                    "call_id": "call_verify_all",
                    "cwd": r"D:\AI_WORKSPACE\base\dftest-aipabox-gpt55-12tasks\DFT_AGENT_EVAL",
                    "command": command,
                    "exit_code": 0,
                },
            ],
            "current_general_tool_requests": [],
        },
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["status"] == "done"
    assert guide["acceptance"][0]["status"] == "passed"


def test_spec487_task_progress_accepts_general_tool_evidence_handle(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    result = {
        "tool_id": "file_write",
        "status": "ok",
        "call_id": "call_write_report",
        "path": r"D:\AI_WORKSPACE\base\dftest\DFT_AGENT_EVAL\output\report.md",
    }
    handle = _spec487_expected_evidence_handle(result)
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Write report",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [handle],
                            }
                        },
                        "acceptance": {
                            "acc_01": {
                                "status": "passed",
                                "evidence_refs": [handle],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [result],
            "current_general_tool_requests": [],
        },
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["items"][0]["evidence_refs"] == [handle]
    assert guide["acceptance"][0]["evidence_refs"] == [handle]


def test_spec487_shell_complex_control_flow_does_not_create_subcommand_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    command = "python output\\04_sales_report.py || python output\\fallback.py"
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Verify command output",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {
                            "item_01": {
                                "status": "done",
                                "evidence_refs": [
                                    "call:python output\\04_sales_report.py",
                                ],
                            }
                        },
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "shell_command",
                    "status": "ok",
                    "call_id": "call_complex_flow",
                    "cwd": r"D:\AI_WORKSPACE\base\dftest\DFT_AGENT_EVAL",
                    "command": command,
                    "exit_code": 0,
                },
            ],
            "current_general_tool_requests": [],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "task_completion_evidence_not_found"
    assert receipt["details"]["unknown_evidence_refs"] == [
        "call:python output\\04_sales_report.py"
    ]


def test_spec448_task_closeout_blocks_done_records_without_evidence_refs(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Daily eval",
        task_goal="Produce files and verify them",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "done"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "passed"}
            ],
        },
    )
    store.set("base.active_task", task_id)

    result = validate_task_closeout(store, {"closeout_decision": "finish"})

    assert result["allowed"] is False
    assert result["reason"] == "task_acceptance_blocked"
    assert result["blockers"] == [
        "item_01:evidence_refs",
        "acc_01:evidence_refs",
    ]


def test_spec434_task_progress_completion_retires_active_guide_for_finalize(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="验证实现",
        task_goal="生成报告",
        guide={
            "items": [{"item_id": "item_01", "required": True, "status": "open"}],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"}
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "evidence_refs": ["call_write_report"],
                    "fields": {
                        "items": [{"item_id": "item_01", "status": "done"}],
                        "acceptance": [
                            {
                                "acceptance_id": "acc_01",
                                "status": "passed",
                                "evidence_refs": ["call_write_report"],
                            }
                        ],
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is True
    assert receipt["next_action"] == "natural_final_reply"
    assert receipt["task_completion"]["status"] == "completed"
    assert store.get("base.active_guide") is None
    assert store.get("base.active_task") is None
    assert not (tmp_path / "workbench" / "process" / task_id).exists()
    assert (tmp_path / "workbench" / "output" / task_id).exists()
    assert not (
        tmp_path
        / "workbench"
        / "guides"
        / f"task__colon__{task_id}"
    ).exists()
    assert validate_task_closeout(store, {"closeout_decision": "finish"})["allowed"] is True


def test_spec447_task_progress_completed_status_aliases_finish_task(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="日常能力测试",
        task_goal="完成输出并验收",
        guide={
            "items": [
                {"item_id": "item_01", "required": True, "status": "open"},
                {"item_id": "item_02", "required": True, "status": "open"},
            ],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"},
                {"acceptance_id": "acc_02", "required": True, "status": "pending"},
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "evidence_refs": ["call_write_report"],
                    "fields": {
                        "items": {
                            "item_01": "completed",
                            "item_02": "completed",
                        },
                        "acceptance": {
                            "acc_01": "completed",
                            "acc_02": "completed",
                        },
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is True
    assert receipt["next_action"] == "natural_final_reply"
    assert receipt["task_completion"]["status"] == "completed"
    assert store.get("base.active_guide") is None
    assert store.get("base.active_task") is None
    assert validate_task_closeout(store, {"closeout_decision": "finish"})["allowed"] is True
    guide = store.load_task_guide(task_id)
    assert [item["status"] for item in guide["items"]] == ["done", "done"]
    assert [item["status"] for item in guide["acceptance"]] == ["done", "done"]


def test_spec479_task_execution_guide_no_longer_exposes_settle_option(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "日常能力测试",
            "items": ["生成报告"],
            "acceptance": ["报告存在"],
        },
    )

    guide = store.load_guide(f"task:{task_id}")
    options = [
        option.get("option_id")
        for item in guide.get("items") or []
        for option in item.get("options") or []
    ]

    assert "update_task_status" in options
    assert "integrate_pending_input" in options
    assert "settle_task_completed" not in options


@pytest.mark.parametrize(
    ("option_id", "fields", "required_fields", "allowed_fields"),
    [
        (
            "need_more_sources",
            {"missing_sources": ["task.md"]},
            ["missing_sources"],
            ["missing_sources", "reason"],
        ),
        (
            "blocked_by_missing_access",
            {"reason": "无法读取用户给出的路径"},
            ["reason"],
            ["reason", "missing_access"],
        ),
    ],
)
def test_retired_task_bootstrap_options_fail_closed(
        tmp_path, option_id, fields, required_fields, allowed_fields):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "items": [{
            "item_id": "build_initial_task_guide",
            "options": [{
                "option_id": option_id,
                "required_fields": required_fields,
                "allowed_fields": allowed_fields,
            }],
        }],
    }, active=True)

    receipt = apply_guide_submit(store, {
        "guide_id": "task_bootstrap",
        "submissions": [{
            "item_id": "build_initial_task_guide",
            "option_id": option_id,
            "fields": fields,
        }],
    })

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "unsupported_task_bootstrap_submission"
    assert receipt["details"]["option_id"] == option_id
    ledger = (
        tmp_path / "workbench" / "guides" / "task_bootstrap" / "ledger.jsonl"
    ).read_text(encoding="utf-8")
    assert "guide_submission_rejected" in ledger
    assert "guide_submission_accepted" not in ledger


def test_spec447_task_progress_rejects_freeform_notes(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import materialize_initial_task_guide
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "日常能力测试",
            "items": ["生成报告"],
            "acceptance": ["报告存在"],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": {"item_01": "done"},
                        "acceptance": {"acc_01": "done"},
                        "notes": "这类自由备注不应进入 task progress。",
                    },
                }
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "undeclared_guide_fields"
    assert receipt["details"] == {"fields": ["notes"]}
    assert store.get("base.active_guide") == f"task:{task_id}"


def test_spec465_task_pending_input_blocks_finish_until_integrated(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="持续任务",
        task_goal="已有任务进行中",
        guide={
            "items": [
                {
                    "item_id": "item_01",
                    "required": True,
                    "status": "done",
                    "evidence_refs": ["call_write_report"],
                }
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "required": True,
                    "status": "passed",
                    "evidence_refs": ["call_write_report"],
                }
            ],
            "pending_inputs": [
                {
                    "pending_input_id": "input_01",
                    "status": "pending",
                    "source_refs": ["round:12:interaction"],
                    "summary": "用户追加了一个需要整合的新要求",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "integrate_pending_input",
                        "required_fields": ["pending_inputs"],
                        "allowed_fields": ["pending_inputs", "items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    blocked = validate_task_closeout(store, {"closeout_decision": "finish"})
    assert blocked["allowed"] is False
    assert blocked["blockers"] == ["input_01:pending_input"]

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "integrate_pending_input",
                    "fields": {
                        "pending_inputs": [
                            {
                                "pending_input_id": "input_01",
                                "status": "integrated",
                                "decision": "纳入当前任务验收",
                            }
                        ]
                    },
                }
            ],
        },
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["pending_inputs"][0]["status"] == "integrated"
    assert validate_task_closeout(store, {"closeout_decision": "finish"})[
        "allowed"
    ] is True


def test_spec488_pending_input_blocks_task_update_with_actionable_receipt(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_helpers import format_protocol_tool_fact
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="持续任务",
        task_goal="已有任务进行中",
        guide={
            "items": [
                {
                    "item_id": "item_01",
                    "required": True,
                    "status": "open",
                }
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "required": True,
                    "status": "pending",
                }
            ],
            "pending_inputs": [
                {
                    "pending_input_id": "input_01",
                    "status": "pending",
                    "source_refs": ["round:573:interaction"],
                    "summary": "用户追加了一个需要整合的新要求",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    },
                    {
                        "option_id": "integrate_pending_input",
                        "required_fields": ["pending_inputs"],
                        "allowed_fields": ["pending_inputs", "items", "acceptance"],
                    },
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "fields": {
                        "items": [
                            {
                                "item_id": "item_01",
                                "status": "done",
                                "evidence_refs": ["EV-1234567890"],
                            }
                        ]
                    },
                }
            ],
        },
        evidence_context={
            "prior_general_tool_results": [
                {
                    "tool_id": "file_write",
                    "status": "success",
                    "evidence_handle": "EV-1234567890",
                }
            ]
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "pending_interaction_first"
    assert receipt["next_option_id"] == "integrate_pending_input"
    assert receipt["pending_input_ids"] == ["input_01"]
    fact = format_protocol_tool_fact(receipt)
    assert "pending_interaction_first" in fact
    assert "input_01" in fact
    assert "integrate_pending_input" in fact


def test_spec470_pending_input_integration_updates_existing_status_with_source_evidence(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_acceptance import validate_task_closeout

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Spec470 guide scenario",
        task_goal="Settle rhythm, then integrate waiting work input.",
        guide={
            "items": [
                {
                    "item_id": "item_01",
                    "required": True,
                    "status": "open",
                }
            ],
            "acceptance": [
                {
                    "acceptance_id": "acc_01",
                    "required": True,
                    "status": "pending",
                }
            ],
            "pending_inputs": [
                {
                    "pending_input_id": "input_01",
                    "status": "pending",
                    "source_refs": ["scenario:Spec470:pending-input"],
                    "summary": "Waiting guide-system scenario input.",
                },
                {
                    "pending_input_id": "input_02",
                    "status": "pending",
                    "source_refs": ["round:591:interaction"],
                    "summary": "Waiting user interaction input.",
                }
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "integrate_pending_input",
                        "required_fields": ["pending_inputs"],
                        "allowed_fields": ["pending_inputs", "items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "integrate_pending_input",
                    "fields": {
                        "pending_inputs": ["input_01", "input_02"],
                        "items": {"item_01": "done"},
                        "acceptance": {"acc_01": "passed"},
                    },
                    "evidence_refs": ["MEM-0C60F7C7", "DC-1"],
                }
            ],
        },
        evidence_context={},
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is True
    guide = store.load_task_guide(task_id)
    assert guide["pending_inputs"][0]["status"] == "integrated"
    assert guide["pending_inputs"][1]["status"] == "integrated"
    assert guide["items"][0]["status"] == "done"
    assert guide["items"][0]["evidence_refs"] == [
        "pending_input:input_01",
        "scenario:Spec470:pending-input",
        "pending_input:input_02",
        "round:591:interaction",
    ]
    assert guide["acceptance"][0]["status"] == "passed"
    assert guide["acceptance"][0]["evidence_refs"] == [
        "pending_input:input_01",
        "scenario:Spec470:pending-input",
        "pending_input:input_02",
        "round:591:interaction",
    ]
    assert validate_task_closeout(store, {"closeout_decision": "finish"})[
        "allowed"
    ] is True


def test_spec542_pending_input_integration_accepts_flat_alias_fields(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Spec542 pending input",
        task_goal="Integrate waiting interaction before status registration.",
        guide={
            "items": [],
            "acceptance": [],
            "pending_inputs": [{
                "pending_input_id": "input_01",
                "status": "pending",
                "source_refs": ["round:573:interaction"],
                "summary": "重复入口消息。",
            }],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [{
            "item_id": "task_progress",
            "options": [{
                "option_id": "integrate_pending_input",
                "required_fields": ["pending_inputs"],
                "allowed_fields": ["pending_inputs", "items", "acceptance"],
            }],
        }],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [{
                "item_id": "task_progress",
                "option_id": "integrate_pending_input",
                "fields": {
                    "input_id": "input_01",
                    "status": "integrated",
                    "summary": "第572轮产物已纳入当前任务。",
                },
            }],
        },
        evidence_context={},
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["pending_inputs"][0]["status"] == "integrated"
    assert guide["pending_inputs"][0]["summary"] == "第572轮产物已纳入当前任务。"


def test_spec542_pending_input_integration_accepts_prefixed_alias_fields(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="Spec542 prefixed pending input",
        task_goal="Integrate waiting interaction before status registration.",
        guide={
            "items": [],
            "acceptance": [],
            "pending_inputs": [{
                "pending_input_id": "input_01",
                "status": "pending",
                "source_refs": ["round:573:interaction"],
                "summary": "重复入口消息。",
            }],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [{
            "item_id": "task_progress",
            "options": [{
                "option_id": "integrate_pending_input",
                "required_fields": ["pending_inputs"],
                "allowed_fields": ["pending_inputs", "items", "acceptance"],
            }],
        }],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [{
                "item_id": "task_progress",
                "option_id": "integrate_pending_input",
                "fields": {
                    "input_01_status": "integrated",
                    "input_01_summary": "第572轮产物已纳入当前任务。",
                },
            }],
        },
        evidence_context={},
    )

    assert receipt["status"] == "accepted"
    guide = store.load_task_guide(task_id)
    assert guide["pending_inputs"][0]["status"] == "integrated"
    assert guide["pending_inputs"][0]["summary"] == "第572轮产物已纳入当前任务。"


def test_spec465_task_execution_guide_exposes_pending_input_choice(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "已有工程任务",
            "items": ["完成实现"],
            "acceptance": ["测试通过"],
        },
    )

    guide = store.load_guide(f"task:{task_id}")
    options = guide["items"][0]["options"]
    pending_option = next(
        item for item in options
        if item.get("option_id") == "integrate_pending_input"
    )

    assert pending_option["required_fields"] == ["pending_inputs"]
    assert "notes" not in pending_option["allowed_fields"]
    assert {"pending_inputs", "items", "acceptance"} <= set(
        pending_option["allowed_fields"]
    )


def test_spec438_task_progress_accepts_status_map_shorthand_and_done_acceptance(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task(
        task_title="日常能力测试",
        task_goal="完成输出并验收",
        guide={
            "items": [
                {"item_id": "item_01", "required": True, "status": "open"},
                {"item_id": "item_02", "required": True, "status": "open"},
            ],
            "acceptance": [
                {"acceptance_id": "acc_01", "required": True, "status": "pending"},
                {"acceptance_id": "acc_02", "required": True, "status": "pending"},
            ],
        },
    )
    store.save_guide({
        "guide_id": f"task:{task_id}",
        "kind": "task_execution",
        "task_id": task_id,
        "items": [
            {
                "item_id": "task_progress",
                "options": [
                    {
                        "option_id": "update_task_status",
                        "required_fields": [],
                        "allowed_fields": ["items", "acceptance"],
                    }
                ],
            }
        ],
    }, active=True)

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": f"task:{task_id}",
            "submissions": [
                {
                    "item_id": "task_progress",
                    "option_id": "update_task_status",
                    "evidence_refs": ["call_write_report"],
                    "fields": {
                        "items": {
                            "item_01": "done",
                            "item_02": "done",
                        },
                        "acceptance": {
                            "acc_01": "done",
                            "acc_02": "done",
                        },
                    },
                }
            ],
        },
        evidence_context=_successful_tool_evidence_context(),
    )

    assert receipt["status"] == "accepted"
    assert receipt["task_acceptance"]["allowed"] is True
    assert receipt["next_action"] == "natural_final_reply"
    assert store.get("base.active_guide") is None
    guide = store.load_task_guide(task_id)
    assert [item["status"] for item in guide["items"]] == ["done", "done"]
    assert [item["status"] for item in guide["acceptance"]] == ["done", "done"]


def test_spec592_runtime_exposes_setup_bootstrap_helper(tmp_path):
    from engines.runtime import Runtime

    runtime = Runtime()

    assert hasattr(runtime, "_prepare_task_bootstrap_guide")


def test_spec571_task_bootstrap_no_longer_exposes_escape_options(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.task_guide import create_task_bootstrap_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide = create_task_bootstrap_guide(store, reason="用户要求读取任务文件后执行")

    option_ids = [
        option.get("option_id")
        for item in guide["items"]
        for option in item.get("options") or []
    ]

    assert option_ids == ["submit_initial_guide", "not_a_task"]
    assert "need_more_sources" not in option_ids
    assert "blocked_by_missing_access" not in option_ids


def test_spec434_reaction_runner_exposes_active_guide_submit_popup(tmp_path):
    from data.workbench import WorkbenchStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide({
        "guide_id": "task_bootstrap",
        "kind": "task_bootstrap",
        "items": [
            {
                "item_id": "build_initial_task_guide",
                "options": [
                    {
                        "option_id": "submit_initial_guide",
                        "required_fields": ["task_title"],
                        "allowed_fields": ["task_title"],
                    }
                ],
            }
        ],
    }, active=True)
    runner = ReactionLoopRunner(RuntimeServices.create(workbench_store=store))

    assert runner._active_guide_protocol_tools() == ["guide_submit"]
    feedback = runner._active_guide_feedback()
    assert "guide_submit" in feedback
    assert "task_bootstrap" in feedback
    assert "submit_initial_guide" in feedback


def test_spec719_declared_root_is_satisfied_by_read_child_only(tmp_path):
    from logic.guide_submit import _source_ref_satisfied

    root = tmp_path / "project"
    child = root / "docs" / "spec.md"
    sibling = tmp_path / "other" / "spec.md"
    child.parent.mkdir(parents=True)
    sibling.parent.mkdir(parents=True)
    child.write_text("read", encoding="utf-8")
    sibling.write_text("not read", encoding="utf-8")
    prior = {str(child.resolve()).lower()}
    context = {"task_root": str(root)}

    assert _source_ref_satisfied(str(root), prior, context)
    assert _source_ref_satisfied(str(child), prior, context)
    assert not _source_ref_satisfied(str(sibling), prior, context)


def test_spec476_reaction_resident_task_guidance_creates_bootstrap(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "reaction_loop_guide",
            "submissions": [
                {
                    "item_id": "task_guidance_entry",
                    "option_id": "request_task_guidance",
                    "fields": {},
                }
            ],
        },
        evidence_context={"round_num": 476},
    )

    assert receipt["status"] == "applied"
    assert receipt["action"] == "created_task_bootstrap"
    assert store.get("base.active_guides.work") == "task_bootstrap"
    assert store.get("base.active_guide") == "task_bootstrap"
    guide = store.load_guide("task_bootstrap")
    assert guide["kind"] == "task_bootstrap"
    assert guide["source_refs"] == ["round:476:reaction"]


def test_spec476_reaction_resident_task_guidance_rejects_fields(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "reaction_loop_guide",
            "submissions": [
                {
                    "item_id": "task_guidance_entry",
                    "option_id": "request_task_guidance",
                    "fields": {"notes": "should not be accepted"},
                }
            ],
        },
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "undeclared_guide_fields"
    assert store.get("base.active_guide") is None


def test_spec476_reaction_resident_task_guidance_appends_pending_input(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "已有任务",
            "task_goal": "继续处理",
            "items": [{"item_id": "item_01", "title": "处理输入"}],
            "acceptance": [{"acceptance_id": "acc_01", "description": "完成"}],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "reaction_loop_guide",
            "submissions": [
                {
                    "item_id": "task_guidance_entry",
                    "option_id": "request_task_guidance",
                    "fields": {},
                }
            ],
        },
        evidence_context={"round_num": 477},
    )

    assert receipt["status"] == "applied"
    assert receipt["action"] == "registered_pending_input"
    assert receipt["task_id"] == task_id
    task_guide = store.load_task_guide(task_id)
    assert task_guide["pending_inputs"][0]["status"] == "pending"
    assert task_guide["pending_inputs"][0]["source_refs"] == ["round:477:reaction"]
    assert task_guide["pending_inputs"][0]["task_guidance_route"] == "current_work"


def test_spec488_reaction_resident_task_guidance_relay_does_not_append_pending_input(
        tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.task_guide import materialize_initial_task_guide

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    task_id = materialize_initial_task_guide(
        store,
        {
            "task_title": "已有任务",
            "task_goal": "继续处理",
            "items": [{"item_id": "item_01", "title": "处理输入"}],
            "acceptance": [{"acceptance_id": "acc_01", "description": "完成"}],
        },
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "reaction_loop_guide",
            "submissions": [
                {
                    "item_id": "task_guidance_entry",
                    "option_id": "request_task_guidance",
                    "fields": {},
                }
            ],
        },
        evidence_context={"round_num": 488, "round_type": "relay"},
    )

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "relay_task_guidance_not_pending_input"
    assert receipt["task_id"] == task_id
    task_guide = store.load_task_guide(task_id)
    assert task_guide.get("pending_inputs") in (None, [])


def test_spec578_reaction_runner_exposes_resident_task_guidance_option_by_default(tmp_path):
    from data.workbench import WorkbenchStore
    from data.state_store import StateStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices
    from logic.native_tool_calls import export_provider_tool_schemas

    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    runner = ReactionLoopRunner(RuntimeServices.create(
        state_store=state_store,
        workbench_store=store,
    ))
    tools = export_provider_tool_schemas(
        include_protocol_writes=True,
        active_protocol_tool_guides=[],
    )

    assert any(tool.get("name") == "guide_submit" for tool in tools)
    assert runner._active_guide_protocol_tools() == []
    feedback = runner._reaction_resident_guide_feedback()
    assert "reaction_loop_guide" in feedback
    assert "item_id=task_guidance_entry" in feedback
    assert "option_id=request_task_guidance" in feedback
    assert "需要任务清单" in feedback
    assert "需要任务清单时" in feedback
    assert "Resident guide:" not in feedback
    assert "required_fields" not in feedback
    assert "分批整理" not in feedback


def test_spec512_reaction_resident_task_guidance_hidden_when_work_guide_active(tmp_path):
    from data.workbench import WorkbenchStore
    from data.state_store import StateStore
    from engines.reaction_loop import ReactionLoopRunner
    from engines.runtime_services import RuntimeServices

    state_store = StateStore(str(tmp_path / "state.json"))
    state_store.init_if_missing()
    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide(
        {
            "guide_id": "task_bootstrap",
            "kind": "task_bootstrap",
            "items": [
                {
                    "item_id": "build_initial_task_guide",
                    "options": [{"option_id": "submit_initial_guide"}],
                }
            ],
        },
        active=True,
    )
    runner = ReactionLoopRunner(RuntimeServices.create(
        state_store=state_store,
        workbench_store=store,
    ))

    assert store.get("base.active_guides.work") == "task_bootstrap"
    assert runner._reaction_resident_guide_feedback() == ""


def test_spec512_repeated_resident_task_guidance_points_to_existing_bootstrap(tmp_path):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit

    store = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    store.save_guide(
        {
            "guide_id": "task_bootstrap",
            "kind": "task_bootstrap",
            "items": [
                {
                    "item_id": "build_initial_task_guide",
                    "options": [{"option_id": "submit_initial_guide"}],
                }
            ],
        },
        active=True,
    )

    receipt = apply_guide_submit(
        store,
        {
            "guide_id": "reaction_loop_guide",
            "submissions": [
                {
                    "item_id": "task_guidance_entry",
                    "option_id": "request_task_guidance",
                    "fields": {},
                }
            ],
        },
    )

    assert receipt["status"] == "blocked"
    assert receipt["reason"] == "task_guidance_already_active"
    assert receipt["work_guide"] == "task_bootstrap"
    assert receipt["next_action"] == "submit_existing_task_bootstrap"
    assert "task_bootstrap" in receipt["message"]
    assert "submit_initial_guide" in receipt["message"]
