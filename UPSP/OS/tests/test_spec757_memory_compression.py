import json

import pytest

from UPSP.OS.tests.test_spec744_ltm_first_recall import (
    _add_ltm,
    _meta,
    _processor,
    recall_layout,
)


@pytest.fixture
def compression_layout(recall_layout, tmp_path, monkeypatch):
    import paths

    ledger = tmp_path / "LTM" / "Memory" / "memory_compression_pending.json"
    monkeypatch.setattr(paths, "MEMORY_COMPRESSION_PENDING_JSON", str(ledger))
    recall_layout["ledger"] = ledger
    return recall_layout


def _add_stm(env, mem_id, body, meta, *, degrade=True):
    stm_body = body if body.lstrip().startswith(f"## {mem_id}") else f"## {mem_id}\n{body}"
    env["store"].replace_stm_body(mem_id, stm_body)
    env["store"].replace_stm_meta(mem_id, meta, canonical_sync=True)
    heat = env["heat"].new_entry(weight=int(meta["weight"]))
    heat["degrade"] = degrade
    heat["compression"] = degrade
    env["heat"].set_entry(mem_id, heat)
    env["store"].rebuild_stm_index()
    env["store"].rebuild_stm_keywords()


def test_unadmitted_full_forgetting_queues_before_stm_delete_and_applies(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700001"
    meta = _meta(mem_id, weight=5, tags=["主体", "地点", "泛词"])
    meta["stored_at"] = ""
    body = "**标题**：长期事实\n**内容**：主体在地点形成了一条需要保留的长期事实。"
    env["store"].store_ltm_entry("Full", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(
        memory_store=env["store"], instance_id="branch-a",
        now_fn=lambda: "2026-08-17T10:00:00+08:00")

    settled = manager.settle_stm_forgetting(mem_id, round_num=7)

    assert settled["outcome"] == "compression_queued"
    assert env["store"].stm_entry_state(mem_id)["body"] is None
    pending = json.loads(env["ledger"].read_text(encoding="utf-8"))
    assert pending["entries"][0]["target_tier"] == "Summary"
    assert pending["entries"][0]["target_weight"] == 4
    assert env["store"].ltm_entry_state(mem_id)["meta"]["stored_at"] == ""

    manager.prepare_daily_cycle(
        local_date="2026-08-17", round_num=8,
        chronicle_receipt_hash="chronicle-sha")
    receipt = manager.apply_batch([{
        "mem_id": mem_id,
        "semantic_content": "主体在地点形成了需要保留的长期事实。",
        "retained_keywords": ["地点", "主体"],
    }], round_num=8)

    current = env["store"].ltm_entry_state(mem_id)
    assert receipt["schema_version"] == "memory_compression_batch_receipt.v1"
    assert current["tier"] == "Summary"
    assert current["meta"]["weight"] == 4
    assert current["meta"]["tags"] == ["主体", "地点"]
    assert current["meta"]["stored_at"] == "2026-08-17T10:00:00+08:00"
    assert json.loads(env["ledger"].read_text(encoding="utf-8"))["entries"] == []


def test_unadmitted_abstract_forgetting_is_immediate_without_batch(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700002"
    meta = _meta(mem_id, weight=2, tags=["轻量事实"])
    meta["stored_at"] = ""
    body = "**标题**：轻量事实\n**梗概**：今天收到一个普通快递。"
    env["store"].store_ltm_entry("Abstract", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(
        memory_store=env["store"],
        now_fn=lambda: "2026-08-17T11:00:00+08:00")

    settled = manager.settle_stm_forgetting(mem_id, round_num=9)

    assert settled["outcome"] == "unadmitted_abstract_admitted"
    assert env["store"].stm_entry_state(mem_id)["body"] is None
    assert env["store"].ltm_entry_state(mem_id)["meta"]["stored_at"]
    assert not env["ledger"].exists()


def test_admitted_pinned_stm_forgetting_removes_only_branch_copy(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700016"
    meta = _meta(mem_id, weight=3, tags=["钉选"])
    body = "**标题**：钉选记忆\n**摘要**：定期层真源仍须保留。"
    env["store"].store_ltm_entry("Pinned", mem_id, body, meta)
    pinned = env["store"].ltm_entry_state(mem_id)
    _add_stm(env, mem_id, pinned["body"], pinned["meta"])

    settled = MemoryCompressionManager(
        memory_store=env["store"]).settle_stm_forgetting(
            mem_id, round_num=9)

    assert settled["outcome"] == "admitted_stm_removed"
    assert env["store"].ltm_entry_state(mem_id)["tier"] == "Pinned"
    assert env["store"].stm_entry_state(mem_id)["body"] is None
    assert not env["ledger"].exists()


def test_daily_degradation_queues_full_and_keeps_weight(compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700003"
    _add_ltm(env, "Full", mem_id, weight=5, semantic="到期的完整长期正文。")
    state = env["store"].ltm_entry_state(mem_id)
    meta = dict(state["meta"])
    meta["decay_countdown_days"] = 1
    env["store"].replace_ltm_entry("Full", mem_id, state["body"], meta)
    manager = MemoryCompressionManager(
        memory_store=env["store"],
        now_fn=lambda: "2026-08-17T12:00:00+08:00")

    prepared = manager.prepare_daily_cycle(
        local_date="2026-08-17", round_num=10,
        chronicle_receipt_hash="chronicle-sha")
    batch = manager.current_batch()

    assert prepared["pending"] == 1
    assert batch["stage"] == "ltm"
    assert batch["items"][0]["target_tier"] == "Summary"
    manager.apply_batch([{
        "mem_id": mem_id,
        "semantic_content": "到期的长期正文摘要。",
        "retained_keywords": ["LTM标签"],
    }], round_num=10)
    current = env["store"].ltm_entry_state(mem_id)
    assert current["tier"] == "Summary"
    assert current["meta"]["weight"] == 5
    assert current["meta"]["decay_countdown_days"] == current["meta"]["decay_period_days"]


def test_batch_rejects_unknown_keyword_without_partial_write(compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700004"
    meta = _meta(mem_id, weight=3, tags=["主体", "事件"])
    meta["stored_at"] = ""
    body = "**标题**：事件\n**摘要**：主体完成了事件。"
    env["store"].store_ltm_entry("Summary", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=11)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=12)
    before = env["store"].ltm_entry_state(mem_id)

    with pytest.raises(ValueError, match="memory_compression_keyword_unknown"):
        manager.apply_batch([{
            "mem_id": mem_id,
            "semantic_content": "主体完成了事件。",
            "retained_keywords": ["新关键词"],
        }], round_num=12)

    assert env["store"].ltm_entry_state(mem_id) == before
    assert json.loads(env["ledger"].read_text(encoding="utf-8"))["entries"][0]["phase"] == "pending"


def test_batch_material_uses_user_visible_label_not_internal_track(
        compression_layout):
    from assembly.context import ContextAssembler
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700005"
    meta = _meta(mem_id, weight=3, tags=["主体"])
    meta["stored_at"] = ""
    body = "**标题**：材料\n**摘要**：需要压缩的完整材料。"
    env["store"].store_ltm_entry("Summary", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=13)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=14)

    material_entry = manager.render_current_batch_material()
    material = material_entry["content"]

    assert ContextAssembler._is_allowed_material_input_entry(material_entry)
    assert material_entry["source"] == "memory_compression_rhythm"
    assert material.startswith("记忆语义压缩材料｜批次 MCB-")
    assert "完整语义正文" in material
    assert "C轨" not in material


def test_memory_compression_guide_submit_applies_current_frozen_batch(
        compression_layout, tmp_path):
    from data.memory_compression_store import MemoryCompressionManager
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.rhythm_guide_materializer import materialize_current_rhythm_guide
    from logic.rhythm_guidance import current_guide

    class StateStub:
        def __init__(self):
            self.flags = {"memory_compression_due": True}

        def set_flag(self, name, value):
            self.flags[name] = bool(value)

    env = compression_layout
    mem_id = "MEM-75700006"
    meta = _meta(mem_id, weight=3, tags=["主体", "事件"])
    meta["stored_at"] = ""
    body = "**标题**：节律提交\n**摘要**：主体完成了需要压缩的事件。"
    env["store"].store_ltm_entry("Summary", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=15)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=16)
    workbench = WorkbenchStore(root_dir=str(tmp_path / "workbench"))
    guide_id = materialize_current_rhythm_guide(
        workbench,
        {"calendar_day_due": True, "memory_compression_due": True},
        round_num=16,
    )
    state = StateStub()

    receipt = apply_guide_submit(
        workbench,
        {
            "guide_id": guide_id,
            "submissions": [{
                "item_id": "memory_compression_due",
                "option_id": "submit_memory_compressions",
                "fields": {
                    "results": [{
                        "mem_id": mem_id,
                        "semantic_content": "主体完成了事件。",
                        "retained_keywords": ["主体"],
                    }],
                },
            }],
        },
        evidence_context={
            "round_num": 16,
            "round_type": "rhythm",
            "memory_store": env["store"],
            "state_store": state,
        },
    )

    assert receipt["status"] == "applied"
    assert workbench.load_guide(guide_id)["kind"] == "memory_compression_rhythm_guide"
    resumed = current_guide(
        {"calendar_day_due": True, "memory_compression_due": True},
        completed_flags={"memory_compression_due"},
    )
    assert resumed["kind"] == "calendar_rhythm_guide"
    assert resumed["items"][0]["flag"] == "calendar_day_due"
    assert receipt["completed_flags"] == ["memory_compression_due"]
    assert receipt["backend_receipts"][0]["schema_version"] == (
        "memory_compression_batch_receipt.v1")
    assert state.flags["memory_compression_due"] is False
    assert workbench.get("base.active_guides.rhythm") is None


def test_real_recall_cancels_pending_compression_and_admits_original_truth(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700007"
    meta = _meta(mem_id, weight=5, tags=["主体", "原始事实"])
    meta["stored_at"] = ""
    body = "**标题**：召回覆盖\n**内容**：召回前仍保留完整原始事实。"
    env["store"].store_ltm_entry("Full", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=17)

    receipt = _processor(env).recall(
        mem_id, round_num=18, boosted_ids=set())

    assert receipt["memory_compression_override"]["status"] == "applied"
    assert env["store"].ltm_entry_state(mem_id)["tier"] == "Full"
    assert env["store"].ltm_entry_state(mem_id)["meta"]["stored_at"]
    assert "完整原始事实" in env["store"].stm_entry_state(mem_id)["body"]
    assert json.loads(env["ledger"].read_text(encoding="utf-8"))["entries"] == []


def test_non_recall_inspection_does_not_cancel_pending_daily_degradation(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager
    from logic.memory_reconsolidation import reconsolidation_candidate

    env = compression_layout
    mem_id = "MEM-75700015"
    _add_ltm(env, "Full", mem_id, weight=5, semantic="仍在Full层的到期正文。")
    ltm = env["store"].ltm_entry_state(mem_id)
    meta = dict(ltm["meta"])
    meta["decay_countdown_days"] = 1
    env["store"].replace_ltm_entry("Full", mem_id, ltm["body"], meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=18)
    before = json.loads(env["ledger"].read_text(encoding="utf-8"))

    state = _processor(env).inspect(mem_id)
    assert reconsolidation_candidate(state) is None

    assert json.loads(env["ledger"].read_text(encoding="utf-8")) == before
    assert env["store"].ltm_entry_state(mem_id)["meta"][
        "decay_countdown_days"] == 0


@pytest.mark.parametrize("failure_stage", ["after_ltm_or_ledger", "after_stm_remove"])
def test_cleanup_settlement_fault_rolls_back_ltm_stm_and_ledger(
        compression_layout, failure_stage):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700008"
    meta = _meta(mem_id, weight=5, tags=["回滚"])
    meta["stored_at"] = ""
    body = "**标题**：结算回滚\n**内容**：任何阶段失败都必须保留完整状态。"
    env["store"].store_ltm_entry("Full", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    before_ltm = env["store"].ltm_entry_state(mem_id)
    before_stm = env["store"].stm_entry_state(mem_id)
    manager = MemoryCompressionManager(
        memory_store=env["store"],
        fault_hook=lambda stage: (_ for _ in ()).throw(OSError(stage))
        if stage == failure_stage else None,
    )

    with pytest.raises(OSError, match=failure_stage):
        manager.settle_stm_forgetting(mem_id, round_num=19)

    assert env["store"].ltm_entry_state(mem_id) == before_ltm
    assert env["store"].stm_entry_state(mem_id) == before_stm
    assert not env["ledger"].exists()


@pytest.mark.parametrize(
    "failure_stage",
    [
        "after_prepare", "after_applying",
        "after_apply:MEM-75700009", "after_ledger_finish",
    ],
)
def test_batch_apply_fault_restores_source_and_pending_phase(
        compression_layout, failure_stage):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700009"
    meta = _meta(mem_id, weight=5, tags=["主体", "回滚"])
    meta["stored_at"] = ""
    body = "**标题**：批次回滚\n**内容**：批次失败不能留下半搬层结果。"
    env["store"].store_ltm_entry("Full", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=20)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=21)
    before_ltm = env["store"].ltm_entry_state(mem_id)
    before_ledger = json.loads(env["ledger"].read_text(encoding="utf-8"))
    manager.fault_hook = (
        lambda stage: (_ for _ in ()).throw(OSError(stage))
        if stage == failure_stage else None
    )

    with pytest.raises(OSError, match=failure_stage):
        manager.apply_batch([{
            "mem_id": mem_id,
            "semantic_content": "批次失败不能留下半结果。",
            "retained_keywords": ["主体"],
        }], round_num=21)

    assert env["store"].ltm_entry_state(mem_id) == before_ltm
    assert env["store"].stm_entry_state(mem_id)["body"] is None
    assert json.loads(env["ledger"].read_text(encoding="utf-8")) == before_ledger


def test_ready_resets_prepared_source_tail_to_pending(compression_layout):
    from data.memory_compression_store import (
        MemoryCompressionManager,
        MemoryCompressionStore,
        _sha_json,
        _sha_text,
    )

    env = compression_layout
    mem_id = "MEM-7570000A"
    meta = _meta(mem_id, weight=3, tags=["主体"])
    meta["stored_at"] = ""
    body = "**标题**：恢复源尾巴\n**摘要**：源层仍未发生写入。"
    env["store"].store_ltm_entry("Summary", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=22)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=23)
    ledger_store = MemoryCompressionStore()
    doc = ledger_store.load()
    entry = doc["entries"][0]
    target_meta = dict(meta)
    target_meta.update({
        "type": "A", "weight": 2, "tags": ["主体"],
        "stored_at": "2026-08-17T13:00:00+08:00",
        "decay_countdown_days": target_meta["decay_period_days"],
    })
    entry.update({
        "phase": "prepared",
        "target_body": "恢复后的梗概。",
        "target_tags": ["主体"],
        "target_meta": target_meta,
        "target_body_sha256": _sha_text("恢复后的梗概。"),
        "target_tags_sha256": _sha_json(["主体"]),
        "target_meta_sha256": _sha_json(target_meta),
    })
    ledger_store.save(doc)

    receipt = manager.reconcile_ready()
    repaired = ledger_store.load()["entries"][0]

    assert receipt["status"] == "repaired"
    assert repaired["phase"] == "pending"
    assert "target_body" not in repaired


def test_ready_finishes_verified_target_tail(compression_layout):
    from data.memory_compression_store import (
        MemoryCompressionManager,
        MemoryCompressionStore,
        _sha_json,
        _sha_text,
    )
    from data.memory_store import replace_memory_semantic_payload

    env = compression_layout
    mem_id = "MEM-7570000B"
    meta = _meta(mem_id, weight=3, tags=["主体", "旧词"])
    meta["stored_at"] = ""
    body = "**标题**：恢复目标尾巴\n**摘要**：源层尚未压缩的正文。"
    env["store"].store_ltm_entry("Summary", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(
        memory_store=env["store"],
        now_fn=lambda: "2026-08-17T14:00:00+08:00")
    manager.settle_stm_forgetting(mem_id, round_num=24)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=25)
    ledger_store = MemoryCompressionStore()
    doc = ledger_store.load()
    entry = doc["entries"][0]
    target_semantic = "已完成但账本尚未收尾的梗概。"
    target_tags = ["主体"]
    entry.update({
        "phase": "applying",
        "target_body": target_semantic,
        "target_tags": target_tags,
        "target_body_sha256": _sha_text(target_semantic),
        "target_tags_sha256": _sha_json(target_tags),
    })
    source = env["store"].ltm_entry_state(mem_id)
    target_meta = dict(source["meta"])
    target_meta["weight"] = 2
    target_meta["type"] = "A"
    target_meta["tags"] = target_tags
    target_meta["stored_at"] = "2026-08-17T14:00:00+08:00"
    target_meta["decay_countdown_days"] = target_meta["decay_period_days"]
    entry.update({
        "target_meta": target_meta,
        "target_meta_sha256": _sha_json(target_meta),
    })
    ledger_store.save(doc)
    target_body = replace_memory_semantic_payload(
        source["body"], target_meta["title"], target_semantic, 2,
        tier="Abstract")
    env["store"].store_ltm_entry(
        "Abstract", mem_id, target_body, target_meta,
        source_tier="Summary", admission_weight_drop=True)

    receipt = manager.reconcile_ready()

    assert receipt["status"] == "repaired"
    assert receipt["completed"] == [mem_id]
    assert ledger_store.load()["entries"] == []
    assert env["store"].ltm_entry_state(mem_id)["tier"] == "Abstract"


@pytest.mark.parametrize(
    ("mem_id", "source_tier", "weight", "limit", "target_tier"),
    [
        ("MEM-7570000C", "Full", 5, 512, "Summary"),
        ("MEM-7570000D", "Summary", 3, 128, "Abstract"),
    ],
)
def test_body_limit_accepts_exact_boundary(
        compression_layout, mem_id, source_tier, weight, limit, target_tier):
    from data.memory_compression_store import MemoryCompressionManager
    from data.memory_store import extract_memory_semantic

    env = compression_layout
    meta = _meta(mem_id, weight=weight, tags=["边界"])
    meta["stored_at"] = ""
    field = "内容" if source_tier == "Full" else "摘要"
    body = f"**标题**：边界\n**{field}**：原始正文。"
    env["store"].store_ltm_entry(source_tier, mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=26)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=27)

    manager.apply_batch([{
        "mem_id": mem_id,
        "semantic_content": "界" * limit,
        "retained_keywords": ["边界"],
    }], round_num=27)

    current = env["store"].ltm_entry_state(mem_id)
    assert current["tier"] == target_tier
    assert len(extract_memory_semantic(current["body"])) == limit


def test_body_limit_rejects_one_character_over_without_partial_write(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-7570000E"
    meta = _meta(mem_id, weight=3, tags=["边界"])
    meta["stored_at"] = ""
    body = "**标题**：越界\n**摘要**：不能截断正文。"
    env["store"].store_ltm_entry("Summary", mem_id, body, meta)
    _add_stm(env, mem_id, body, meta)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=28)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=29)
    before = env["store"].ltm_entry_state(mem_id)

    with pytest.raises(ValueError, match="memory_compression_body_limit_invalid"):
        manager.apply_batch([{
            "mem_id": mem_id,
            "semantic_content": "界" * 129,
            "retained_keywords": ["边界"],
        }], round_num=29)

    assert env["store"].ltm_entry_state(mem_id) == before


def test_batch_requires_all_and_only_frozen_ids(compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    manager = MemoryCompressionManager(memory_store=env["store"])
    for suffix in ("0F", "10"):
        mem_id = f"MEM-757000{suffix}"
        meta = _meta(mem_id, weight=3, tags=["覆盖"])
        meta["stored_at"] = ""
        body = f"**标题**：覆盖{suffix}\n**摘要**：必须整批提交。"
        env["store"].store_ltm_entry("Summary", mem_id, body, meta)
        _add_stm(env, mem_id, body, meta)
        manager.settle_stm_forgetting(mem_id, round_num=30)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=31)

    with pytest.raises(ValueError, match="memory_compression_batch_coverage_invalid"):
        manager.apply_batch([{
            "mem_id": "MEM-7570000F",
            "semantic_content": "只提交了一项。",
            "retained_keywords": ["覆盖"],
        }], round_num=31)

    assert all(
        entry["phase"] == "pending"
        for entry in json.loads(env["ledger"].read_text(encoding="utf-8"))["entries"])


def test_active_cycle_blocks_next_date_and_excludes_late_queue(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    manager = MemoryCompressionManager(memory_store=env["store"])
    for mem_id in ("MEM-75700011", "MEM-75700012"):
        meta = _meta(mem_id, weight=3, tags=["批次"])
        meta["stored_at"] = ""
        body = f"**标题**：{mem_id}\n**摘要**：等待压缩。"
        env["store"].store_ltm_entry("Summary", mem_id, body, meta)
        _add_stm(env, mem_id, body, meta)
    manager.settle_stm_forgetting("MEM-75700011", round_num=32)
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=33)
    manager.settle_stm_forgetting("MEM-75700012", round_num=33)

    assert [item["mem_id"] for item in manager.current_batch()["items"]] == [
        "MEM-75700011"]
    with pytest.raises(ValueError, match="memory_compression_cycle_active"):
        manager.prepare_daily_cycle(local_date="2026-08-18", round_num=34)


def test_daily_compression_syncs_existing_stm_but_preserves_heat(
        compression_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = compression_layout
    mem_id = "MEM-75700013"
    _add_ltm(env, "Full", mem_id, weight=5, semantic="需要同步到STM的完整正文。")
    ltm = env["store"].ltm_entry_state(mem_id)
    meta = dict(ltm["meta"])
    meta["decay_countdown_days"] = 1
    env["store"].replace_ltm_entry("Full", mem_id, ltm["body"], meta)
    _add_stm(env, mem_id, ltm["body"], meta, degrade=False)
    heat_before = env["store"].stm_entry_state(mem_id)["heat"]
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.prepare_daily_cycle(local_date="2026-08-17", round_num=35)

    manager.apply_batch([{
        "mem_id": mem_id,
        "semantic_content": "同步后的摘要。",
        "retained_keywords": ["LTM标签"],
    }], round_num=35)

    stm = env["store"].stm_entry_state(mem_id)
    assert "同步后的摘要" in stm["body"]
    assert stm["meta"]["tags"] == ["LTM标签"]
    assert stm["heat"] == heat_before


def test_ledger_rejects_target_hash_drift(compression_layout):
    from data.memory_compression_store import MemoryCompressionStore

    env = compression_layout
    env["ledger"].parent.mkdir(parents=True, exist_ok=True)
    doc = {
        "schema_version": "memory_compression_pending.v1",
        "revision": 1,
        "updated_at": "2026-08-17T10:00:00+08:00",
        "next_sequence": 2,
        "last_daily_settled_date": "2026-08-17",
        "entries": [{
            "mem_id": "MEM-75700014", "sequence": 1,
            "queued_at": "2026-08-17T10:00:00+08:00",
            "reason": "ltm_daily_degradation", "source_instance_id": "meta",
            "source_round": 1, "source_tier": "Summary",
            "target_tier": "Abstract", "target_weight": 3,
            "body_limit": 128, "keyword_limit": 4,
            "body_sha256": "a" * 64, "meta_sha256": "b" * 64,
            "tags_sha256": "c" * 64, "phase": "prepared",
            "target_body": "正文", "target_tags": ["词"],
            "target_meta": {"type": "A", "weight": 3, "tags": ["词"]},
            "target_body_sha256": "0" * 64,
            "target_tags_sha256": "0" * 64,
            "target_meta_sha256": "0" * 64,
        }],
        "active_cycle": None,
    }
    env["ledger"].write_text(json.dumps(doc, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="memory_compression_ledger_invalid"):
        MemoryCompressionStore().load()
