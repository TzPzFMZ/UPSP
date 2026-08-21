import json

import pytest


@pytest.fixture
def recall_layout(tmp_path, monkeypatch):
    import paths
    from data import memory_heat, memory_index, memory_store
    from data.memory_heat import MemoryHeat
    from data.memory_index import MemoryIndex
    from data.memory_store import MemoryStore

    ltm_root = tmp_path / "LTM" / "Memory"
    tiers = {
        "Full": "full.md",
        "Summary": "summary.md",
        "Abstract": "abstract.md",
        "Pinned": "pinned.md",
        "Backup": "backup.md",
    }
    ltm = {}
    for tier, body_name in tiers.items():
        directory = ltm_root / tier
        directory.mkdir(parents=True)
        prefix = tier.upper()
        body_attr = f"LTM_{prefix}_{prefix}_MD"
        if tier == "Full":
            body_attr = "LTM_FULL_FULL_MD"
        body = directory / body_name
        meta = directory / "meta.json"
        index = directory / "index.md"
        meta.write_text("{}", encoding="utf-8")
        monkeypatch.setattr(paths, body_attr, str(body))
        monkeypatch.setattr(paths, f"LTM_{prefix}_META_JSON", str(meta))
        monkeypatch.setattr(paths, f"LTM_{prefix}_INDEX_MD", str(index), raising=False)
        monkeypatch.setattr(memory_store, body_attr, str(body), raising=False)
        monkeypatch.setattr(memory_store, f"LTM_{prefix}_META_JSON", str(meta), raising=False)
        ltm[tier] = {"body": body, "meta": meta, "index": index}

    ltm_keywords = ltm_root / "keywords.json"
    compression_ledger = ltm_root / "memory_compression_pending.json"
    overlay = tmp_path / "instance" / "LTM" / "memory_links.json"
    monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(ltm_keywords))
    monkeypatch.setattr(
        paths, "MEMORY_COMPRESSION_PENDING_JSON", str(compression_ledger))
    monkeypatch.setattr(memory_store, "LTM_MEMORY_LINKS_JSON", str(overlay))
    monkeypatch.setattr(memory_store, "LTM_META_PATHS", {
        str(ltm[tier]["meta"].resolve())
        for tier in ("Full", "Summary", "Abstract", "Pinned")
    })

    stm = tmp_path / "instance" / "STM" / "memory"
    stm.mkdir(parents=True)
    stm_paths = {
        "body": stm / "memory.md",
        "meta": stm / "meta.json",
        "index": stm / "index.md",
        "keywords": stm / "keywords.json",
        "heat": stm / "heat.json",
    }
    stm_paths["body"].write_text("", encoding="utf-8")
    stm_paths["meta"].write_text("{}", encoding="utf-8")
    stm_paths["index"].write_text("", encoding="utf-8")
    stm_paths["keywords"].write_text('{"index": {}}', encoding="utf-8")
    stm_paths["heat"].write_text('{"entries": {}}', encoding="utf-8")
    for module in (paths, memory_store):
        monkeypatch.setattr(module, "MEMORY_MD", str(stm_paths["body"]), raising=False)
        monkeypatch.setattr(module, "META_JSON", str(stm_paths["meta"]), raising=False)
        monkeypatch.setattr(module, "INDEX_MD", str(stm_paths["index"]), raising=False)
    monkeypatch.setattr(paths, "KEYWORDS_JSON", str(stm_paths["keywords"]))
    monkeypatch.setattr(paths, "HEAT_JSON", str(stm_paths["heat"]))
    monkeypatch.setattr(memory_index, "KEYWORDS_JSON", str(stm_paths["keywords"]))
    monkeypatch.setattr(memory_heat, "HEAT_JSON", str(stm_paths["heat"]))
    monkeypatch.setattr(paths, "ACTIVE_INSTANCE_ID", "branch-a")
    monkeypatch.setattr(memory_store, "ACTIVE_INSTANCE_ID", "branch-a")

    return {
        "ltm": ltm,
        "ltm_keywords": ltm_keywords,
        "compression_ledger": compression_ledger,
        "overlay": overlay,
        "stm": stm_paths,
        "store": MemoryStore(),
        "index": MemoryIndex(),
        "heat": MemoryHeat(),
    }


def _meta(mem_id, *, weight=4, title="LTM 标题", tags=None):
    return {
        "id": mem_id,
        "type": "F" if weight >= 5 else "S" if weight >= 3 else "A",
        "weight": weight,
        "title": title,
        "subject": "TzPz",
        "access": "public",
        "tags": list(tags or ["LTM标签"]),
        "created_at": "2026-08-13T07:00:00+08:00",
        "stored_at": "2026-08-13T07:00:00+08:00",
        "created_round": 1,
        "created_instance_id": "meta",
        "last_recalled_round": 2,
        "last_recalled_instance_id": "meta",
        "last_recalled_at": "2026-08-13T08:00:00+08:00",
        "linked_containers": [],
        "current_overview": "",
        "current_overview_updated_at": "",
        "recalled": False,
        "decay_period_days": 30,
        "decay_countdown_days": 30,
    }


def _add_ltm(env, tier, mem_id, *, weight=4, semantic="LTM 唯一真源正文", title="LTM 标题"):
    meta = _meta(mem_id, weight=weight, title=title)
    label = "内容" if weight >= 5 else "摘要" if weight >= 3 else "梗概"
    env["store"].store_ltm_entry(
        tier,
        mem_id,
        f"**标题**：{title}\n**{label}**：{semantic}\n标签：LTM标签",
        meta,
    )
    return meta


def _processor(env, **kwargs):
    from logic.memory_recall import MemoryRecallProcessor

    return MemoryRecallProcessor(
        memory_store=env["store"],
        heat=env["heat"],
        now_fn=lambda: "2026-08-13T12:00:00+08:00",
        instance_id="branch-a",
        **kwargs,
    )


def _reconsolidate(
        env, mem_id, semantic_content, *, round_num, boosted_ids,
        completed_title=None, assembler=None, fault_hook=None):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        MemoryReconsolidationTracker,
    )

    tracker = MemoryReconsolidationTracker(round_num)
    recall_receipt = _processor(env).recall(
        mem_id,
        round_num=round_num,
        boosted_ids=boosted_ids,
        reconsolidation_tracker=tracker,
    )
    item = tracker.get(mem_id)
    if item is None:
        raise RuntimeError("memory_reconsolidation_not_required")
    receipt = MemoryReconsolidationProcessor(
        memory_store=env["store"],
        assembler=assembler,
        fault_hook=fault_hook,
    ).apply(item, semantic_content, ["重整关键词"])
    receipt.update({
        "completed_body_chars": receipt["body_chars"],
        "completed_body_limit": receipt["body_limit"],
        "heat_boost_applied": recall_receipt["heat_boost_applied"],
        "heat_boost_deduplicated": recall_receipt["heat_boost_deduplicated"],
    })
    return receipt


def _set_ltm_meta(env, mem_id, **updates):
    state = env["store"].ltm_entry_state(mem_id, include_backup=False)
    meta = dict(state["meta"])
    meta.update(updates)
    env["store"].replace_ltm_entry(
        state["tier"], mem_id, state["body"], meta)


def test_spec749_recall_preserves_regex_replacement_escapes(recall_layout):
    env = recall_layout
    mem_id = "MEM-74900001"
    semantic = (
        r"Python位于C:\Users\TzPz\AppData\Local\Programs\Python；"
        r"原样文本包含\1与\g<1>。"
    )
    _add_ltm(env, "Abstract", mem_id, weight=2, semantic=semantic)

    first = _processor(env).recall(
        mem_id, round_num=49, boosted_ids=set())
    second = _processor(env).recall(
        mem_id, round_num=50, boosted_ids=set())

    assert first["status"] == "applied"
    assert second["status"] == "applied"
    assert semantic in env["store"].ltm_entry_state(mem_id)["body"]
    assert semantic in env["store"].stm_entry_state(mem_id)["body"]


@pytest.mark.parametrize(
    ("tier", "weight"),
    [("Full", 5), ("Summary", 4), ("Abstract", 2)],
)
def test_spec746_real_ltm_recall_resets_current_tier_decay_period(
        recall_layout, tier, weight):
    env = recall_layout
    mem_id = f"MEM-7461{weight:04X}"
    _add_ltm(env, tier, mem_id, weight=weight)
    _set_ltm_meta(env, mem_id, decay_countdown_days=0)

    receipt = _processor(env).recall(
        mem_id, round_num=44, boosted_ids=set())

    assert receipt["ltm_decay_reset_applied"] is True
    assert receipt["ltm_decay_countdown_before"] == 0
    assert receipt["ltm_decay_countdown_after"] == 30
    assert env["store"].ltm_entry_state(mem_id)["meta"][
        "decay_countdown_days"] == 30
    assert env["store"].stm_entry_state(mem_id)["meta"][
        "decay_countdown_days"] == 30


@pytest.mark.parametrize(
    ("tier", "stored_at"),
    [("Summary", ""), ("Pinned", "2026-08-13T07:00:00+08:00")],
)
def test_spec746_pending_or_pinned_recall_does_not_fake_ltm_decay_reset(
        recall_layout, tier, stored_at):
    env = recall_layout
    mem_id = "MEM-74610009"
    _add_ltm(env, tier, mem_id, weight=4)
    _set_ltm_meta(
        env, mem_id, stored_at=stored_at, decay_countdown_days=7)

    receipt = _processor(env).recall(
        mem_id, round_num=44, boosted_ids=set())

    assert receipt["ltm_decay_reset_applied"] is False
    assert receipt["ltm_decay_countdown_before"] == 7
    assert receipt["ltm_decay_countdown_after"] == 7
    assert env["store"].ltm_entry_state(mem_id)["meta"][
        "decay_countdown_days"] == 7


def test_spec746_normal_recall_failure_rolls_back_ltm_decay_and_stm(
        recall_layout):
    env = recall_layout
    mem_id = "MEM-7461000A"
    _add_ltm(env, "Summary", mem_id, weight=4)
    _set_ltm_meta(env, mem_id, decay_countdown_days=0)
    before_ltm = env["store"].snapshot_ltm_files()
    before_stm = env["store"].snapshot_stm_files()

    def fault(stage):
        if stage == "after_ltm_write":
            raise OSError("injected")

    with pytest.raises(OSError, match="injected"):
        _processor(env, fault_hook=fault).recall(
            mem_id, round_num=44, boosted_ids=set())

    assert env["store"].snapshot_ltm_files() == before_ltm
    assert env["store"].snapshot_stm_files() == before_stm


def test_spec746_admitted_recall_rejects_invalid_decay_period_without_mutation(
        recall_layout):
    from logic.memory_recall import MemoryRecallError

    env = recall_layout
    mem_id = "MEM-7461000B"
    _add_ltm(env, "Summary", mem_id, weight=4)
    _set_ltm_meta(
        env, mem_id, decay_period_days=0, decay_countdown_days=0)
    before_ltm = env["store"].snapshot_ltm_files()
    before_stm = env["store"].snapshot_stm_files()

    with pytest.raises(MemoryRecallError, match="invalid_decay_period_days"):
        _processor(env).recall(mem_id, round_num=44, boosted_ids=set())

    assert env["store"].snapshot_ltm_files() == before_ltm
    assert env["store"].snapshot_stm_files() == before_stm


def test_spec758_unadmitted_tier_mismatch_does_not_create_reconsolidation(
        recall_layout):
    from logic.memory_reconsolidation import MemoryReconsolidationTracker

    env = recall_layout
    mem_id = "MEM-7461000C"
    _add_ltm(env, "Summary", mem_id, weight=5)
    _set_ltm_meta(env, mem_id, stored_at="")
    tracker = MemoryReconsolidationTracker(44)
    receipt = _processor(env).recall(
        mem_id, round_num=44, boosted_ids=set(),
        reconsolidation_tracker=tracker,
    )

    assert receipt["reconsolidation_required"] is False
    assert tracker.pending_ids() == []


def test_spec744_ltm_first_recall_repairs_dual_residence_and_preserves_local_state(
        recall_layout):
    env = recall_layout
    mem_id = "MEM-74400001"
    _add_ltm(env, "Summary", mem_id, weight=4)
    env["stm"]["body"].write_text(
        f"## {mem_id} [A] 权重2\n**标题**：陈旧 STM 标题\n**正文**：陈旧 STM 正文\n",
        encoding="utf-8",
    )
    stale_meta = _meta(mem_id, weight=2, title="陈旧 STM 标题", tags=["陈旧标签"])
    stale_meta.update(linked_containers=["PRJ-LOCAL"], current_overview="本地挂接")
    env["stm"]["meta"].write_text(
        json.dumps({mem_id: stale_meta}, ensure_ascii=False), encoding="utf-8")
    env["stm"]["heat"].write_text(json.dumps({"entries": {mem_id: {
        "H": 60,
        "zone": "未定",
        "AH_high": 7,
        "AH_low": 3,
        "last_heat_at": "before",
        "last_high_at": None,
        "degrade": True,
        "compression": True,
        "heat_locked": False,
    }}}, ensure_ascii=False), encoding="utf-8")

    assert "LTM 唯一真源正文" in env["store"].read_entry(mem_id)
    ledger = set()
    receipt = _processor(env).recall(mem_id, round_num=44, boosted_ids=ledger)

    assert receipt["source_memory_layer"] == "LTM/Summary"
    assert receipt["stm_created"] is False
    assert receipt["heat_boost_applied"] is True
    assert ledger == {mem_id}
    stm = env["store"].stm_entry_state(mem_id)
    assert "LTM 唯一真源正文" in stm["body"]
    assert "陈旧 STM 正文" not in stm["body"]
    assert stm["meta"]["title"] == "LTM 标题"
    assert stm["meta"]["tags"] == ["LTM标签"]
    assert stm["meta"]["linked_containers"] == ["PRJ-LOCAL"]
    assert stm["meta"]["current_overview"] == "本地挂接"
    assert stm["heat"]["H"] == 70
    assert stm["heat"]["AH_high"] == 7
    assert stm["heat"]["AH_low"] == 0
    assert stm["heat"]["degrade"] is False
    assert "stored" not in stm["heat"]
    rows = [row for row in env["store"].list_public_entries() if row["id"] == mem_id]
    assert len(rows) == 1
    assert rows[0]["title"] == "LTM 标题"
    assert rows[0]["memory_layers"] == ["STM", "LTM/Summary"]


def test_spec744_partial_content_read_rehydrates_full_ltm_body_and_dedupes_heat(
        recall_layout):
    from logic.memory_content_read import apply_memory_content_read_requests

    env = recall_layout
    mem_id = "MEM-74400002"
    _add_ltm(
        env, "Abstract", mem_id, weight=2,
        semantic="前半段-后半段-必须完整进入 STM",
    )
    ledger = set()
    data_modules = {
        "memory_store": env["store"],
        "memory_recall": _processor(env),
    }
    receipts, mounts, _ = apply_memory_content_read_requests(
        [{"mem_id": mem_id, "mount_mode": "temporary", "char_start": 1, "char_end": 20}],
        {"presence": {"confirmed_subjects": []}},
        data_modules,
        round_num=51,
        memory_heat_boosted_ids=ledger,
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["read_mode"] == "partial"
    assert receipts[0]["source_memory_layer"] == "LTM/Abstract"
    assert receipts[0]["stm_present"] is True
    assert receipts[0]["heat_boost_applied"] is True
    assert receipts[0]["ltm_decay_reset_applied"] is True
    assert receipts[0]["ltm_decay_countdown_after"] == 30
    assert receipts[0]["meta"]["decay_countdown_days"] == 30
    assert receipts[0]["meta"]["last_recalled_at"] == (
        "2026-08-13T12:00:00+08:00")
    assert "必须完整进入 STM" in env["store"].stm_entry_state(mem_id)["body"]
    assert mounts[0]["content"] == receipts[0]["body"]
    first_h = env["store"].stm_entry_state(mem_id)["heat"]["H"]

    again = _processor(env).recall(mem_id, round_num=51, boosted_ids=ledger)
    assert again["heat_boost_applied"] is False
    assert again["heat_boost_deduplicated"] is True
    assert again["ltm_decay_reset_applied"] is True
    assert again["ltm_decay_countdown_after"] == 30
    assert env["store"].stm_entry_state(mem_id)["heat"]["H"] == first_h

    next_round = _processor(env).recall(mem_id, round_num=52, boosted_ids=set())
    assert next_round["heat_boost_applied"] is True
    assert env["store"].stm_entry_state(mem_id)["heat"]["H"] == first_h + 10


@pytest.mark.parametrize(
    ("tier", "weight", "expected_h"),
    [
        ("Abstract", 1, 50),
        ("Abstract", 2, 60),
        ("Summary", 3, 70),
        ("Summary", 4, 80),
        ("Pinned", 5, 90),
    ],
)
def test_spec744_ltm_only_recall_initializes_heat_from_true_weight(
        recall_layout, tier, weight, expected_h):
    env = recall_layout
    mem_id = f"MEM-7442000{weight}"
    _add_ltm(env, tier, mem_id, weight=weight)

    receipt = _processor(env).recall(
        mem_id, round_num=54, boosted_ids=set())

    heat = env["store"].stm_entry_state(mem_id)["heat"]
    assert receipt["stm_created"] is True
    assert heat["H"] == expected_h
    assert "stored" not in heat
    assert heat["degrade"] is False
    assert heat["AH_low"] == 0
    assert heat["heat_locked"] is False


def test_spec744_mount_none_has_zero_recall_side_effects(recall_layout):
    from logic.memory_content_read import apply_memory_content_read_requests

    env = recall_layout
    mem_id = "MEM-74400003"
    _add_ltm(env, "Pinned", mem_id, weight=5)
    before = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()
    receipts, mounts, unmounts = apply_memory_content_read_requests(
        [{"mem_id": mem_id, "mount_mode": "none"}],
        {"presence": {"confirmed_subjects": []}},
        {"memory_store": env["store"], "memory_recall": _processor(env)},
        round_num=52,
        memory_heat_boosted_ids=set(),
    )
    assert receipts[0]["status"] == "accepted"
    assert mounts == []
    assert unmounts == [mem_id]
    assert env["store"].snapshot_stm_files() == before
    assert env["store"].snapshot_ltm_files() == before_ltm


def test_spec744_invalid_read_range_has_zero_recall_side_effects(recall_layout):
    from logic.memory_content_read import apply_memory_content_read_requests

    env = recall_layout
    mem_id = "MEM-7440000B"
    _add_ltm(env, "Summary", mem_id, weight=4)
    before_stm = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()
    ledger = set()

    receipts, mounts, unmounts = apply_memory_content_read_requests(
        [{
            "mem_id": mem_id,
            "mount_mode": "temporary",
            "line_start": 1,
            "line_end": 1,
            "char_start": 1,
            "char_end": 1,
        }],
        {"presence": {"confirmed_subjects": []}},
        {"memory_store": env["store"], "memory_recall": _processor(env)},
        round_num=53,
        memory_heat_boosted_ids=ledger,
    )

    assert receipts[0]["status"] == "rejected"
    assert receipts[0]["reason"] == "range_mode_conflict"
    assert mounts == []
    assert unmounts == []
    assert env["store"].snapshot_stm_files() == before_stm
    assert env["store"].snapshot_ltm_files() == before_ltm
    assert ledger == set()


@pytest.mark.parametrize(
    ("tier", "target", "weight", "limit"),
    [("Summary", "Full", 5, 2048),
     ("Abstract", "Summary", 4, 512),
     ("Abstract", "Summary", 3, 512)],
)
def test_spec744_completion_replaces_one_semantic_field_at_exact_weight_limit(
        recall_layout, tier, target, weight, limit):
    env = recall_layout
    mem_id = f"MEM-74410{weight:03X}"
    _add_ltm(env, tier, mem_id, weight=weight, semantic="旧语义正文")
    ledger = set()

    receipt = _reconsolidate(env,
        mem_id,
        "新" * limit,
        round_num=61,
        boosted_ids=ledger,
        completed_title="LTM 标题[召回补全内容]",
    )

    assert receipt["completed_body_chars"] == limit
    assert receipt["completed_body_limit"] == limit
    assert receipt["source_memory_layer"] == f"LTM/{tier}"
    assert receipt["heat_boost_applied"] is True
    canonical = env["store"].read_entry(mem_id)
    assert canonical.count("回忆重整") == 1
    assert "旧语义正文" not in canonical
    assert "新" * limit in canonical
    assert sum(canonical.count(f"**{label}**") for label in ("内容", "摘要", "正文", "梗概")) == 1
    assert env["store"].ltm_entry_state(mem_id)["tier"] == target
    assert env["store"].ltm_entry_state(mem_id)["meta"]["recalled"] is True
    assert env["store"].stm_entry_state(mem_id)["body"] == canonical


def test_spec744_completion_drops_formatted_lines_from_old_semantic_payload(
        recall_layout):
    env = recall_layout
    mem_id = "MEM-7440C001"
    _add_ltm(
        env,
        "Abstract",
        mem_id,
        weight=4,
        semantic="旧第一段\n**旧结论**：这也属于旧正文",
    )

    _reconsolidate(env,
        mem_id,
        "全新正文",
        round_num=61,
        boosted_ids=set(),
        completed_title="LTM 标题[召回补全内容]",
    )

    canonical = env["store"].read_entry(mem_id)
    assert "全新正文" in canonical
    assert "旧第一段" not in canonical
    assert "旧结论" not in canonical
    assert canonical.count("**摘要**") == 1


def test_spec758_reconsolidation_over_limit_does_not_undo_trigger_recall(
        recall_layout):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationError,
        MemoryReconsolidationProcessor,
        MemoryReconsolidationTracker,
    )

    env = recall_layout
    mem_id = "MEM-74400004"
    _add_ltm(env, "Abstract", mem_id, weight=3)
    ledger = set()
    tracker = MemoryReconsolidationTracker(62)
    _processor(env).recall(
        mem_id, round_num=62, boosted_ids=ledger,
        reconsolidation_tracker=tracker,
    )
    before_stm = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()

    with pytest.raises(MemoryReconsolidationError, match="max=512;actual=513"):
        MemoryReconsolidationProcessor(memory_store=env["store"]).apply(
            tracker.get(mem_id), "超" * 513, ["超限"]
        )

    assert env["store"].snapshot_stm_files() == before_stm
    assert env["store"].snapshot_ltm_files() == before_ltm
    assert ledger == {mem_id}


@pytest.mark.parametrize(
    "failed_stage",
    [
        "before_write",
        "after_ltm_move",
        "after_stm_body",
        "after_stm_meta",
        "after_stm_projections",
        "after_verify",
    ],
)
def test_spec744_completion_failure_rolls_back_ltm_stm_indexes_and_heat(
        recall_layout, failed_stage):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        MemoryReconsolidationTracker,
    )

    env = recall_layout
    mem_id = "MEM-74400005"
    _add_ltm(env, "Summary", mem_id, weight=5)
    ledger = set()
    tracker = MemoryReconsolidationTracker(63)
    _processor(env).recall(
        mem_id, round_num=63, boosted_ids=ledger,
        reconsolidation_tracker=tracker,
    )
    before_stm = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()

    def fault(stage):
        if stage == failed_stage:
            raise OSError("injected")

    with pytest.raises(OSError, match="injected"):
        MemoryReconsolidationProcessor(
            memory_store=env["store"], fault_hook=fault
        ).apply(tracker.get(mem_id), "合法新正文", ["关键词"])

    assert env["store"].snapshot_stm_files() == before_stm
    assert env["store"].snapshot_ltm_files() == before_ltm
    assert ledger == {mem_id}


def test_spec758_existing_dual_reconsolidation_failure_rolls_back_every_file(
        recall_layout):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        MemoryReconsolidationTracker,
    )

    env = recall_layout
    mem_id = "MEM-7440000E"
    _add_ltm(env, "Abstract", mem_id, weight=4)
    tracker = MemoryReconsolidationTracker(69)
    _processor(env).recall(
        mem_id, round_num=69, boosted_ids=set(),
        reconsolidation_tracker=tracker,
    )
    before_stm = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()
    ledger = set()

    def fault(stage):
        if stage == "after_stm_meta":
            raise OSError("overlay injected")

    with pytest.raises(OSError, match="overlay injected"):
        MemoryReconsolidationProcessor(
            memory_store=env["store"], fault_hook=fault
        ).apply(tracker.get(mem_id), "new body", ["keyword"])

    assert env["store"].snapshot_stm_files() == before_stm
    assert env["store"].snapshot_ltm_files() == before_ltm
    assert ledger == set()


def test_spec758_pinned_recall_does_not_create_guide_or_touch_periodic_cache(
        recall_layout):
    from logic.memory_reconsolidation import MemoryReconsolidationTracker

    env = recall_layout
    mem_id = "MEM-7440000F"
    _add_ltm(env, "Pinned", mem_id, weight=5, semantic="old pinned body")

    class StateStore:
        def __init__(self):
            self.expired = False

        def load(self):
            return {"base": {"context_cache": {
                "periodic_expired": self.expired,
            }}}

        def _set_internal(self, _path, value):
            self.expired = bool(value)

    class Assembler:
        def __init__(self):
            self._layer_cache = {("setup", "periodic"): "old"}
            self._layer_block_cache = {
                ("setup", "periodic"): [{"old": True}],
            }
            self.state_store = StateStore()

        def invalidate_layer(self, layer, strict=False):
            assert layer == "periodic" and strict is True
            self._layer_cache.clear()
            self._layer_block_cache.clear()
            self.state_store.expired = True

    assembler = Assembler()
    before_stm = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()

    tracker = MemoryReconsolidationTracker(71)
    receipt = _processor(env).recall(
        mem_id, round_num=71, boosted_ids=set(),
        reconsolidation_tracker=tracker,
    )

    assert receipt["reconsolidation_required"] is False
    assert tracker.pending_ids() == []
    assert env["store"].snapshot_stm_files() != before_stm
    assert env["store"].snapshot_ltm_files() != before_ltm
    assert assembler._layer_cache == {("setup", "periodic"): "old"}
    assert assembler._layer_block_cache == {
        ("setup", "periodic"): [{"old": True}],
    }
    assert assembler.state_store.expired is False


def test_spec744_ready_repairs_stale_live_dual_but_keeps_heat_and_overlay(recall_layout):
    env = recall_layout
    mem_id = "MEM-74400006"
    _add_ltm(env, "Abstract", mem_id, weight=2)
    env["stm"]["body"].write_text(
        f"## {mem_id}\n**正文**：休眠分身陈旧正文\n", encoding="utf-8")
    stale = _meta(mem_id, weight=5, title="休眠分身陈旧标题", tags=["旧标签"])
    stale.update(linked_containers=["PRJ-DORMANT"], current_overview="休眠挂接")
    env["stm"]["meta"].write_text(
        json.dumps({mem_id: stale}, ensure_ascii=False), encoding="utf-8")
    heat = {
        "H": 33, "zone": "衰减", "AH_high": 9, "AH_low": 2,
        "last_heat_at": "unchanged", "last_high_at": None,
        "degrade": False, "compression": False, "stored": True,
        "heat_locked": False,
    }
    env["stm"]["heat"].write_text(
        json.dumps({"entries": {mem_id: heat}}, ensure_ascii=False),
        encoding="utf-8",
    )

    env["store"].reconcile_ltm_projections()

    stm = env["store"].stm_entry_state(mem_id)
    assert "LTM 唯一真源正文" in stm["body"]
    assert stm["meta"]["title"] == "LTM 标题"
    assert stm["meta"]["tags"] == ["LTM标签"]
    assert stm["meta"]["linked_containers"] == ["PRJ-DORMANT"]
    assert stm["heat"] == {key: value for key, value in heat.items() if key != "stored"}


def test_spec744_ready_repairs_dual_stm_index_and_keywords(recall_layout):
    env = recall_layout
    mem_id = "MEM-7440000C"
    _add_ltm(env, "Summary", mem_id, weight=4, title="Canonical title")
    _processor(env).recall(mem_id, round_num=66, boosted_ids=set())
    env["stm"]["index"].write_text("stale index\n", encoding="utf-8")
    env["stm"]["keywords"].write_text(
        '{"index":{"stale":["MEM-DEADBEEF"]}}', encoding="utf-8")

    receipt = env["store"].reconcile_ltm_projections()

    assert receipt["status"] == "repaired"
    index_text = env["stm"]["index"].read_text(encoding="utf-8")
    keywords = json.loads(env["stm"]["keywords"].read_text(encoding="utf-8"))
    assert "stale index" not in index_text
    assert mem_id in index_text
    assert "Canonical title" in index_text
    assert keywords["index"]["LTM标签"] == [mem_id]


def test_spec757_ready_moves_pending_degrade_to_compression_ledger(recall_layout):
    env = recall_layout
    mem_id = "MEM-7440D006"
    _add_ltm(env, "Summary", mem_id, weight=4)
    state = env["store"].ltm_entry_state(mem_id, include_backup=False)
    pending_meta = dict(state["meta"])
    pending_meta["stored_at"] = ""
    env["store"].replace_ltm_entry(
        "Summary", mem_id, state["body"], pending_meta)
    _processor(env).recall(mem_id, round_num=70, boosted_ids=set())
    heat_doc = env["heat"].load_heat()
    heat_doc["entries"][mem_id]["degrade"] = True
    heat_doc["entries"][mem_id]["AH_low"] = 3
    env["heat"].save_heat(heat_doc)

    env["store"].reconcile_ltm_projections()

    repaired = env["store"].stm_entry_state(mem_id)
    assert repaired["body"] is None
    assert repaired["meta"] is None
    assert repaired["heat"] is None
    canonical = env["store"].ltm_entry_state(mem_id)
    assert canonical["tier"] == "Summary"
    assert canonical["meta"]["stored_at"] == ""
    ledger = json.loads(
        env["compression_ledger"].read_text(encoding="utf-8"))
    assert ledger["entries"][0]["mem_id"] == mem_id
    assert ledger["entries"][0]["target_tier"] == "Abstract"


@pytest.mark.parametrize(
    ("variant", "reason"),
    [
        ("missing_heat", "stm_residence_incomplete"),
        ("orphan_heat", "stm_residence_incomplete"),
        ("stored_without_ltm", "stm_without_ltm_canonical_truth"),
        ("backup_collision", "stm_backup_conflict"),
    ],
)
def test_spec744_ready_rejects_unprovable_stm_only_state(
        recall_layout, variant, reason):
    env = recall_layout
    mem_id = {
        "missing_heat": "MEM-7440D001",
        "orphan_heat": "MEM-7440D002",
        "stored_without_ltm": "MEM-7440D003",
        "backup_collision": "MEM-7440D004",
    }[variant]
    meta = _meta(mem_id, weight=2, title="STM-only")
    heat = env["heat"].new_entry(weight=2)

    if variant != "orphan_heat":
        env["stm"]["body"].write_text(
            f"## {mem_id}\n**正文**：STM-only 正文\n", encoding="utf-8")
        env["stm"]["meta"].write_text(
            json.dumps({mem_id: meta}, ensure_ascii=False), encoding="utf-8")
    if variant != "missing_heat":
        if variant == "stored_without_ltm":
            heat["stored"] = True
        env["stm"]["heat"].write_text(
            json.dumps({"entries": {mem_id: heat}}, ensure_ascii=False),
            encoding="utf-8",
        )
    if variant == "backup_collision":
        env["store"].store_ltm_entry(
            "Backup", mem_id, f"## {mem_id}\nBackup 正文", meta)

    before_stm = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()
    with pytest.raises(ValueError, match=reason):
        env["store"].reconcile_ltm_projections()
    assert env["store"].snapshot_stm_files() == before_stm
    assert env["store"].snapshot_ltm_files() == before_ltm


def test_spec744_ready_accepts_complete_unstored_stm_only_state(recall_layout):
    env = recall_layout
    mem_id = "MEM-7440D005"
    meta = _meta(mem_id, weight=2, title="完整 STM-only")
    meta.pop("stored_at")
    env["stm"]["body"].write_text(
        f"## {mem_id}\n**正文**：完整 STM-only 正文\n", encoding="utf-8")
    env["stm"]["meta"].write_text(
        json.dumps({mem_id: meta}, ensure_ascii=False), encoding="utf-8")
    legacy_heat = env["heat"].new_entry(weight=2)
    legacy_heat["stored"] = False
    env["heat"].set_entry(mem_id, legacy_heat)

    receipt = env["store"].reconcile_ltm_projections()

    assert receipt["status"] in {"ok", "repaired"}
    assert env["store"].ltm_entry_state(mem_id)["meta"]["stored_at"] == ""
    assert "stored" not in env["store"].stm_entry_state(mem_id)["heat"]


def test_spec746_legacy_migration_preserves_overlay_and_normalizes_abstract(
        recall_layout):
    env = recall_layout
    tail_id = "MEM-7440D007"
    pending_id = "MEM-7440D008"
    _add_ltm(env, "Full", tail_id, weight=5)

    tail_meta = _meta(tail_id, weight=5, title="中断残尾")
    tail_meta.update(
        linked_containers=["PRJ-TAIL"],
        current_overview="必须保留的挂接",
    )
    pending_meta = _meta(pending_id, weight=2, title="旧梗概")
    pending_meta.pop("stored_at")
    pending_meta["created_instance_id"] = "branch-a"
    env["stm"]["body"].write_text(
        f"## {pending_id}\n**标题**：旧梗概\n**正文**：应规范为梗概\n",
        encoding="utf-8",
    )
    env["stm"]["meta"].write_text(
        json.dumps({
            tail_id: tail_meta,
            pending_id: pending_meta,
        }, ensure_ascii=False),
        encoding="utf-8",
    )
    tail_heat = env["heat"].new_entry(weight=5)
    tail_heat["stored"] = True
    pending_heat = env["heat"].new_entry(weight=2)
    pending_heat["stored"] = False
    env["stm"]["heat"].write_text(
        json.dumps({"entries": {
            tail_id: tail_heat,
            pending_id: pending_heat,
        }}, ensure_ascii=False),
        encoding="utf-8",
    )

    env["store"].reconcile_ltm_projections()

    overlay = json.loads(env["overlay"].read_text(encoding="utf-8"))
    assert overlay["entries"][tail_id]["linked_containers"] == ["PRJ-TAIL"]
    assert overlay["entries"][tail_id]["current_overview"] == "必须保留的挂接"
    canonical = env["store"].ltm_entry_state(pending_id, include_backup=False)
    assert canonical["tier"] == "Abstract"
    assert "**梗概**" in canonical["body"]
    assert "**正文**" not in canonical["body"]


def test_spec744_completion_updates_only_active_branch_then_ready_repairs_dormant(
        recall_layout, monkeypatch, tmp_path):
    import paths
    from data import memory_store as memory_store_module
    from data.memory_store import MemoryStore

    env = recall_layout
    mem_id = "MEM-7440000D"
    _add_ltm(env, "Abstract", mem_id, weight=4, semantic="shared old body")
    _processor(env).recall(mem_id, round_num=67, boosted_ids=set())

    dormant_root = tmp_path / "branch-b" / "STM" / "memory"
    dormant_root.mkdir(parents=True)
    dormant = {
        "body": dormant_root / "memory.md",
        "meta": dormant_root / "meta.json",
        "index": dormant_root / "index.md",
        "keywords": dormant_root / "keywords.json",
        "heat": dormant_root / "heat.json",
    }
    for name, path in dormant.items():
        path.write_bytes(env["stm"][name].read_bytes())
    dormant_before = {name: path.read_bytes() for name, path in dormant.items()}

    _reconsolidate(env,
        mem_id,
        "shared completed body",
        round_num=68,
        boosted_ids=set(),
        completed_title="LTM 标题[召回补全内容]",
    )

    assert "shared completed body" in env["store"].read_stm_entry(mem_id)
    assert {name: path.read_bytes() for name, path in dormant.items()} == dormant_before

    for module in (paths, memory_store_module):
        monkeypatch.setattr(module, "MEMORY_MD", str(dormant["body"]), raising=False)
        monkeypatch.setattr(module, "META_JSON", str(dormant["meta"]), raising=False)
        monkeypatch.setattr(module, "INDEX_MD", str(dormant["index"]), raising=False)
    monkeypatch.setattr(paths, "KEYWORDS_JSON", str(dormant["keywords"]))
    monkeypatch.setattr(paths, "HEAT_JSON", str(dormant["heat"]))
    monkeypatch.setattr(
        memory_store_module,
        "LTM_MEMORY_LINKS_JSON",
        str(tmp_path / "branch-b" / "LTM" / "memory_links.json"),
    )
    heat_before = dormant["heat"].read_bytes()

    receipt = MemoryStore().reconcile_ltm_projections()

    assert receipt["status"] == "repaired"
    repaired = MemoryStore().stm_entry_state(mem_id)
    assert "shared completed body" in repaired["body"]
    assert repaired["meta"]["title"] == "LTM 标题[回忆重整]"
    assert dormant["heat"].read_bytes() == heat_before


def test_spec744_corrupt_ltm_never_falls_back_to_readable_stm(recall_layout):
    env = recall_layout
    mem_id = "MEM-74400007"
    env["ltm"]["Summary"]["meta"].write_text(
        json.dumps({mem_id: _meta(mem_id)}, ensure_ascii=False), encoding="utf-8")
    env["stm"]["body"].write_text(
        f"## {mem_id}\n**正文**：不允许泄露的 STM 旧副本\n", encoding="utf-8")
    env["stm"]["meta"].write_text(
        json.dumps({mem_id: _meta(mem_id)}, ensure_ascii=False), encoding="utf-8")
    env["stm"]["heat"].write_text(
        json.dumps({"entries": {mem_id: {
            "H": 50, "zone": "未定", "AH_high": 0, "AH_low": 0,
            "last_heat_at": "before", "last_high_at": None,
            "degrade": False, "compression": True, "stored": True,
            "heat_locked": False,
        }}}, ensure_ascii=False), encoding="utf-8")

    with pytest.raises(ValueError, match="ltm_body_meta_conflict"):
        env["store"].read_entry(mem_id)


def test_spec744_backup_and_missing_memory_never_create_stm(recall_layout):
    from errors import EntryNotFoundError
    from logic.memory_recall import MemoryRecallError

    env = recall_layout
    backup_id = "MEM-74400010"
    meta = _meta(backup_id, weight=2)
    env["store"].store_ltm_entry(
        "Backup", backup_id, f"## {backup_id}\nbackup body", meta)
    before = env["store"].snapshot_stm_files()

    with pytest.raises(MemoryRecallError, match="backup_memory_not_active"):
        _processor(env).recall(backup_id, round_num=72, boosted_ids=set())
    with pytest.raises(EntryNotFoundError):
        _processor(env).recall(
            "MEM-7440FFFF", round_num=72, boosted_ids=set())

    assert env["store"].snapshot_stm_files() == before


def test_spec744_private_ltm_read_stays_hidden_and_side_effect_free(recall_layout):
    from logic.memory_content_read import apply_memory_content_read_requests

    env = recall_layout
    mem_id = "MEM-74400011"
    private_meta = _meta(mem_id, weight=4)
    private_meta["access"] = "private"
    env["ltm"]["Summary"]["meta"].write_text(
        json.dumps({mem_id: private_meta}, ensure_ascii=False), encoding="utf-8")
    (env["ltm"]["Summary"]["body"].parent / "TzPz.private.md").write_text(
        f"## {mem_id}\nprivate body\n", encoding="utf-8")
    before = env["store"].snapshot_stm_files()

    receipts, mounts, unmounts = apply_memory_content_read_requests(
        [{"mem_id": mem_id, "mount_mode": "temporary"}],
        {"presence": {"confirmed_subjects": ["TzPz"]}},
        {"memory_store": env["store"], "memory_recall": _processor(env)},
        round_num=73,
        memory_heat_boosted_ids=set(),
    )

    assert receipts[0]["status"] == "private_memory_not_visible"
    assert receipts[0]["body"] == ""
    assert mounts == []
    assert unmounts == []
    assert env["store"].snapshot_stm_files() == before


def test_spec746_pending_dual_residence_recalls_from_ltm_truth(recall_layout):
    env = recall_layout
    mem_id = "MEM-74400012"
    _add_ltm(env, "Full", mem_id, weight=5)
    env["store"].replace_stm_body(
        mem_id, f"## {mem_id}\n**正文**：uncommitted STM")
    env["store"].replace_stm_meta(mem_id, _meta(mem_id, weight=5))
    env["heat"].set_entry(mem_id, env["heat"].new_entry(weight=5))
    before = env["store"].snapshot_stm_files()

    receipt = _processor(env).recall(mem_id, round_num=74, boosted_ids=set())
    assert receipt["source_memory_layer"] == "LTM/Full"
    assert env["store"].stm_entry_state(mem_id)["body"] == env["store"].read_entry(mem_id)
    assert env["store"].snapshot_stm_files() != before


def test_spec746_stm_only_completion_is_rejected(recall_layout):
    from logic.memory_recall import MemoryRecallError
    env = recall_layout
    mem_id = "MEM-74400008"
    meta = _meta(mem_id, weight=2, title="STM 标题", tags=["STM标签"])
    env["store"].write_entry(
        mem_id, "STM 标题", summary="旧 STM 正文", weight=2,
        tags=["STM标签"], subject="TzPz", round_num=1,
    )
    env["store"].set_meta(mem_id, meta)
    env["heat"].set_entry(mem_id, env["heat"].new_entry(weight=2))

    with pytest.raises(MemoryRecallError, match="stm_without_ltm_canonical_truth"):
        _reconsolidate(env,
            mem_id, "仅修改 STM 的新正文", round_num=64, boosted_ids=set(),
            completed_title="STM 标题[召回补全内容]",
        )
    assert env["store"].ltm_entry_state(mem_id) is None


def test_spec744_ltm_comment_survives_query_and_completion(recall_layout):
    env = recall_layout
    mem_id = "MEM-7440E001"
    comment = "LTM Summary 合法说明头"
    env["ltm"]["Summary"]["meta"].write_text(
        json.dumps({"_comment": comment}, ensure_ascii=False), encoding="utf-8")
    _add_ltm(env, "Abstract", mem_id, weight=4)

    listed = env["store"].list_public_ltm_entries()
    assert [entry["id"] for entry in listed] == [mem_id]
    assert json.loads(
        env["ltm"]["Summary"]["meta"].read_text(encoding="utf-8")
    )["_comment"] == comment

    _reconsolidate(env,
        mem_id,
        "补全后的正文",
        round_num=75,
        boosted_ids=set(),
        completed_title="LTM 标题[召回补全内容]",
    )

    meta_doc = json.loads(
        env["ltm"]["Summary"]["meta"].read_text(encoding="utf-8"))
    assert meta_doc["_comment"] == comment
    assert meta_doc[mem_id]["recalled"] is True


def test_spec744_ltm_comments_survive_cross_tier_move(recall_layout):
    env = recall_layout
    mem_id = "MEM-7440E002"
    source_comment = "Summary 说明头"
    target_comment = "Abstract 说明头"
    env["ltm"]["Summary"]["meta"].write_text(
        json.dumps({"_comment": source_comment}, ensure_ascii=False),
        encoding="utf-8",
    )
    env["ltm"]["Abstract"]["meta"].write_text(
        json.dumps({"_comment": target_comment}, ensure_ascii=False),
        encoding="utf-8",
    )
    _add_ltm(env, "Summary", mem_id, weight=4)
    state = env["store"].ltm_entry_state(mem_id)

    env["store"].store_ltm_entry(
        "Abstract",
        mem_id,
        state["body"],
        dict(state["meta"], type="A"),
        source_tier="Summary",
    )

    source_doc = json.loads(
        env["ltm"]["Summary"]["meta"].read_text(encoding="utf-8"))
    target_doc = json.loads(
        env["ltm"]["Abstract"]["meta"].read_text(encoding="utf-8"))
    assert source_doc == {"_comment": source_comment}
    assert target_doc["_comment"] == target_comment
    assert mem_id in target_doc


def test_spec758_missing_recall_processor_fails_closed_for_read(
        recall_layout):
    from logic.memory_content_read import apply_memory_content_read_requests

    env = recall_layout
    mem_id = "MEM-7440E003"
    _add_ltm(env, "Summary", mem_id, weight=4)
    before_stm = env["store"].snapshot_stm_files()
    before_ltm = env["store"].snapshot_ltm_files()

    read_receipts, mounts, unmounts = apply_memory_content_read_requests(
        [{"mem_id": mem_id, "mount_mode": "temporary"}],
        {"presence": {"confirmed_subjects": []}},
        {"memory_store": env["store"]},
        round_num=76,
        memory_heat_boosted_ids=set(),
    )
    assert read_receipts[0]["status"] == "error"
    assert read_receipts[0]["reason"] == "memory_recall_processor_unavailable"
    assert read_receipts[0]["body"] == ""
    assert mounts == []
    assert unmounts == []
    assert env["store"].snapshot_stm_files() == before_stm
    assert env["store"].snapshot_ltm_files() == before_ltm


def test_spec758_pinned_candidate_does_not_mutate_periodic_cache(
        recall_layout):
    from logic.memory_reconsolidation import (
        MemoryReconsolidationTracker,
    )
    env = recall_layout
    mem_id = "MEM-74400009"
    _add_ltm(env, "Pinned", mem_id, weight=5)

    class StateStore:
        def __init__(self):
            self.expired = False

        def load(self):
            return {"base": {"context_cache": {"periodic_expired": self.expired}}}

        def _set_internal(self, _path, value):
            self.expired = bool(value)

    class Assembler:
        def __init__(self):
            self._layer_cache = {("setup", "periodic"): "old"}
            self._layer_block_cache = {("setup", "periodic"): [{"old": True}]}
            self.state_store = StateStore()

        def invalidate_layer(self, layer, strict=False):
            assert layer == "periodic" and strict is True
            self._layer_cache.clear()
            self._layer_block_cache.clear()
            self.state_store.expired = True

    assembler = Assembler()
    tracker = MemoryReconsolidationTracker(65)
    receipt = _processor(env).recall(
        mem_id, round_num=65, boosted_ids=set(),
        reconsolidation_tracker=tracker,
    )

    assert receipt["reconsolidation_required"] is False
    assert tracker.pending_ids() == []

    assert assembler._layer_cache == {("setup", "periodic"): "old"}
    assert assembler._layer_block_cache == {
        ("setup", "periodic"): [{"old": True}],
    }
    assert assembler.state_store.expired is False


def test_spec744_setup_recall_failure_blocks_before_first_reaction_provider(tmp_path):
    from tests.runtime_test_helpers import RuntimeTestMixin

    rt = RuntimeTestMixin()._make_runtime(tmp_path)

    class Store:
        def list_entries(self):
            return []

        def read_meta_by_id(self, mem_id):
            return {"id": mem_id, "_memory_layer": "LTM/Summary"}

    store = Store()
    rt.memory_store = store

    class Processor:
        memory_store = store
        heat = rt.heat

        def recall(self, *_args, **_kwargs):
            raise ValueError("injected_rehydration_failure")

    rt.services.memory_recall = Processor()

    class Executor:
        calls = 0

        def call(self, *_args, **_kwargs):
            self.calls += 1
            raise AssertionError("reaction provider must not be called")

    executor = Executor()
    rt.executor = executor
    result = rt._run_reaction_loop(rt.sm.load(), "interactive", [{
        "type": "memory",
        "ids": "MEM-7440000A",
        "source": "setup_mount",
    }])

    assert result["aborted"] is True
    assert result["_required_context_failure"]["stage"] == "recall"
    assert executor.calls == 0


def test_spec744_setup_missing_recall_processor_fails_closed(tmp_path):
    from errors import RequiredContextError
    from tests.runtime_test_helpers import RuntimeTestMixin

    rt = RuntimeTestMixin()._make_runtime(tmp_path)
    rt.memory_recall = None

    with pytest.raises(RequiredContextError) as exc_info:
        rt._boost_mounted_memory_once(
            "MEM-7440E004",
            77,
            set(),
            "LTM/Summary",
        )
    assert str(exc_info.value.cause) == "memory_recall_processor_unavailable"
