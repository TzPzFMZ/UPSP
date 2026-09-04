import json
from pathlib import Path
from types import SimpleNamespace

import pytest


@pytest.fixture
def ltm_layout(tmp_path, monkeypatch):
    import paths
    from data import memory_heat, memory_index, memory_store

    root = tmp_path / "LTM" / "Memory"
    names = {
        "FULL": "full.md",
        "SUMMARY": "summary.md",
        "ABSTRACT": "abstract.md",
        "PINNED": "pinned.md",
        "BACKUP": "backup.md",
    }
    layout = {"root": root}
    for tier, body_name in names.items():
        directory = root / tier.title()
        directory.mkdir(parents=True)
        body_attr = f"LTM_{tier}_{tier}_MD" if tier != "FULL" else "LTM_FULL_FULL_MD"
        monkeypatch.setattr(paths, f"LTM_{tier}_DIR", str(directory))
        monkeypatch.setattr(paths, body_attr, str(directory / body_name))
        monkeypatch.setattr(paths, f"LTM_{tier}_META_JSON", str(directory / "meta.json"))
        monkeypatch.setattr(paths, f"LTM_{tier}_INDEX_MD", str(directory / "index.md"), raising=False)
        monkeypatch.setattr(memory_store, body_attr, str(directory / body_name), raising=False)
        monkeypatch.setattr(
            memory_store, f"LTM_{tier}_META_JSON", str(directory / "meta.json"), raising=False)
        layout[tier.title()] = {
            "body": directory / body_name,
            "meta": directory / "meta.json",
            "index": directory / "index.md",
        }
    keywords = root / "keywords.json"
    monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(keywords))
    layout["links"] = tmp_path / "memory_links.json"
    monkeypatch.setattr(
        memory_store, "LTM_MEMORY_LINKS_JSON", str(layout["links"]))
    monkeypatch.setattr(memory_store, "LTM_META_PATHS", {
        str(layout[tier]["meta"].resolve())
        for tier in ("Full", "Summary", "Abstract", "Pinned")
    })
    stm = tmp_path / "STM" / "memory"
    stm.mkdir(parents=True)
    layout["STM"] = {
        "body": stm / "memory.md",
        "meta": stm / "meta.json",
        "index": stm / "index.md",
        "keywords": stm / "keywords.json",
        "heat": stm / "heat.json",
    }
    monkeypatch.setattr(memory_store, "MEMORY_MD", str(layout["STM"]["body"]))
    monkeypatch.setattr(memory_store, "META_JSON", str(layout["STM"]["meta"]))
    monkeypatch.setattr(memory_store, "INDEX_MD", str(layout["STM"]["index"]))
    monkeypatch.setattr(paths, "KEYWORDS_JSON", str(layout["STM"]["keywords"]))
    monkeypatch.setattr(paths, "HEAT_JSON", str(layout["STM"]["heat"]))
    monkeypatch.setattr(memory_index, "KEYWORDS_JSON", str(layout["STM"]["keywords"]))
    monkeypatch.setattr(memory_heat, "HEAT_JSON", str(layout["STM"]["heat"]))
    layout["keywords"] = keywords
    return layout


def _meta(mem_id, *, title="记忆", tags=None, tier="A", **extra):
    entry = {
        "id": mem_id,
        "type": tier,
        "weight": 2,
        "title": title,
        "subject": "TzPz",
        "access": "public",
        "tags": list(tags or []),
        "created_at": "2026-08-01T00:00:00+08:00",
        "created_round": 1,
        "last_recalled_round": 2,
    }
    entry.update(extra)
    return entry


def _write_ltm_entry(layout, tier, mem_id, *, body="正文", meta=None):
    layout[tier]["body"].write_text(
        f"## {mem_id}\n{body}\n", encoding="utf-8")
    layout[tier]["meta"].write_text(
        json.dumps({mem_id: meta or _meta(mem_id)}, ensure_ascii=False),
        encoding="utf-8",
    )


def test_spec735_reconciles_36_active_memories_and_is_idempotent(ltm_layout):
    from data.memory_store import MemoryStore

    entries = {}
    bodies = []
    for index in range(36):
        mem_id = f"MEM-{index:08X}"
        entries[mem_id] = _meta(mem_id, title=f"条目{index}", tags=[f"标签{index}"])
        bodies.append(f"## {mem_id}  [A]  权重2\n**梗概**：正文{index}")
    ltm_layout["Abstract"]["body"].write_text(
        "\n\n".join(bodies) + "\n", encoding="utf-8")
    ltm_layout["Abstract"]["meta"].write_text(
        json.dumps(entries, ensure_ascii=False), encoding="utf-8")
    ltm_layout["Abstract"]["index"].write_text("错误索引\n", encoding="utf-8")

    store = MemoryStore()
    first = store.reconcile_ltm_projections()
    keywords = json.loads(ltm_layout["keywords"].read_text(encoding="utf-8"))

    assert first["status"] == "repaired"
    assert first["active_entries"] == 36
    assert len(keywords["index"]) == 36
    assert keywords["index"]["标签7"] == ["MEM-00000007[A]"]
    assert ltm_layout["Abstract"]["index"].read_text(
        encoding="utf-8").count("| MEM-") == 36

    before = {
        path: Path(path).stat().st_mtime_ns
        for path in first["updated"]
    }
    second = store.reconcile_ltm_projections()
    assert second == {"status": "ok", "updated": [], "active_entries": 36}
    assert {path: Path(path).stat().st_mtime_ns for path in before} == before


def test_spec735_reconcile_indexes_every_existing_meta_tag(ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-AAAA0001"
    tags = [f"标签{index}" for index in range(6)]
    ltm_layout["Abstract"]["body"].write_text(
        f"## {mem_id}  [A]  权重2\n正文\n", encoding="utf-8")
    ltm_layout["Abstract"]["meta"].write_text(
        json.dumps({mem_id: _meta(mem_id, tags=tags)}, ensure_ascii=False),
        encoding="utf-8",
    )

    MemoryStore().reconcile_ltm_projections()

    index = json.loads(ltm_layout["keywords"].read_text(encoding="utf-8"))["index"]
    assert set(index) == set(tags)


def test_spec735_backup_is_removed_from_active_keywords(ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-BAA00001"
    ltm_layout["Backup"]["body"].write_text(
        f"## {mem_id}  [A]  权重1\n旧事实\n", encoding="utf-8")
    ltm_layout["Backup"]["meta"].write_text(
        json.dumps({mem_id: _meta(mem_id, tags=["旧词"])}), encoding="utf-8")

    MemoryStore().reconcile_ltm_projections()

    assert json.loads(
        ltm_layout["keywords"].read_text(encoding="utf-8"))["index"] == {}
    assert mem_id in ltm_layout["Backup"]["index"].read_text(encoding="utf-8")


def test_spec735_reconcile_rejects_active_tier_conflict(ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-D0A00001"
    for tier in ("Full", "Abstract"):
        ltm_layout[tier]["body"].write_text(
            f"## {mem_id}\n同一正文\n", encoding="utf-8")
        ltm_layout[tier]["meta"].write_text(
            json.dumps({mem_id: _meta(mem_id)}), encoding="utf-8")

    with pytest.raises(ValueError, match="ltm_active_tier_conflict"):
        MemoryStore().reconcile_ltm_projections()


@pytest.mark.parametrize("partial_side", ["body", "meta"])
def test_spec735_reconcile_preserves_unproven_partial_ltm_and_fails_closed(
        ltm_layout, partial_side):
    from data.memory_store import MemoryStore

    mem_id = "MEM-A1100001"
    ltm_layout["STM"]["body"].write_text(
        f"## {mem_id}\nSTM 原始正文\n", encoding="utf-8")
    ltm_layout["STM"]["meta"].write_text(
        json.dumps({mem_id: _meta(mem_id)}, ensure_ascii=False), encoding="utf-8")
    if partial_side == "body":
        ltm_layout["Full"]["body"].write_text(
            f"## {mem_id}\n未完成目标正文\n", encoding="utf-8")
    else:
        ltm_layout["Full"]["meta"].write_text(
            json.dumps({mem_id: _meta(mem_id)}, ensure_ascii=False), encoding="utf-8")

    before = {
        name: path.read_bytes() if path.exists() else None
        for name, path in {
            "stm_body": ltm_layout["STM"]["body"],
            "stm_meta": ltm_layout["STM"]["meta"],
            "ltm_body": ltm_layout["Full"]["body"],
            "ltm_meta": ltm_layout["Full"]["meta"],
        }.items()
    }

    with pytest.raises(ValueError, match="ltm_body_meta_conflict"):
        MemoryStore().reconcile_ltm_projections()

    assert before == {
        name: path.read_bytes() if path.exists() else None
        for name, path in {
            "stm_body": ltm_layout["STM"]["body"],
            "stm_meta": ltm_layout["STM"]["meta"],
            "ltm_body": ltm_layout["Full"]["body"],
            "ltm_meta": ltm_layout["Full"]["meta"],
        }.items()
    }


def test_spec743_reconcile_preserves_verified_live_stm_ltm_dual_residence(ltm_layout):
    from data.memory_store import MemoryStore, shared_memory_meta_entry

    mem_id = "MEM-A1100008"
    source = _meta(
        mem_id,
        tier="F",
        tags=["升格"],
        linked_containers=["PRJ-ONE"],
        current_overview="最新挂接",
    )
    ltm_layout["STM"]["body"].write_text(
        f"## {mem_id}\n已升格正文\n", encoding="utf-8")
    ltm_layout["STM"]["meta"].write_text(
        json.dumps({mem_id: source}, ensure_ascii=False), encoding="utf-8")
    ltm_layout["STM"]["index"].write_text(
        f"| {mem_id} | [F] | 5 | 已升格 |\n", encoding="utf-8")
    ltm_layout["STM"]["keywords"].write_text(
        json.dumps({"index": {"升格": [mem_id]}}, ensure_ascii=False),
        encoding="utf-8",
    )
    ltm_layout["STM"]["heat"].write_text(
        json.dumps({"entries": {mem_id: {"stored": True}}}), encoding="utf-8")
    _write_ltm_entry(
        ltm_layout, "Full", mem_id, body="已升格正文",
        meta=shared_memory_meta_entry(source),
    )

    receipt = MemoryStore().reconcile_ltm_projections()

    assert receipt["status"] == "repaired"
    for name in ("body", "index", "meta", "keywords", "heat"):
        assert mem_id in ltm_layout["STM"][name].read_text(encoding="utf-8")
    assert mem_id in ltm_layout["Full"]["body"].read_text(encoding="utf-8")


def test_spec735_reconcile_finishes_stored_compressed_stm_cleanup(ltm_layout):
    from data.memory_store import MemoryStore, shared_memory_meta_entry

    mem_id = "MEM-A1100009"
    source = _meta(mem_id, tier="S")
    ltm_layout["STM"]["body"].write_text(
        f"## {mem_id}\n很长的 STM 原始正文\n", encoding="utf-8")
    ltm_layout["STM"]["meta"].write_text(
        json.dumps({mem_id: source}, ensure_ascii=False), encoding="utf-8")
    ltm_layout["STM"]["heat"].write_text(
        json.dumps({"entries": {mem_id: {
            "stored": True,
            "degrade": True,
        }}}), encoding="utf-8")
    _write_ltm_entry(
        ltm_layout, "Abstract", mem_id, body="已验证的压缩摘要",
        meta=shared_memory_meta_entry(dict(
            source, type="A", title="压缩后新标题", subject="压缩后新对象")),
    )

    receipt = MemoryStore().reconcile_ltm_projections()

    assert receipt["status"] == "repaired"
    assert mem_id not in ltm_layout["STM"]["body"].read_text(encoding="utf-8")
    assert mem_id in ltm_layout["Abstract"]["body"].read_text(encoding="utf-8")


def test_spec735_reconcile_cleans_stored_stm_tail_after_body_meta_are_gone(
        ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-A1100010"
    _write_ltm_entry(
        ltm_layout, "Full", mem_id, body="已完整入库",
        meta=_meta(mem_id, tier="F"),
    )
    ltm_layout["STM"]["index"].write_text(
        f"| {mem_id} | [F] | 5 | 尾巴 | — | 00001 |\n", encoding="utf-8")
    ltm_layout["STM"]["keywords"].write_text(
        json.dumps({"index": {"尾巴": [mem_id]}}, ensure_ascii=False),
        encoding="utf-8",
    )
    ltm_layout["STM"]["heat"].write_text(
        json.dumps({"entries": {mem_id: {"stored": True}}}), encoding="utf-8")

    MemoryStore().reconcile_ltm_projections()

    assert mem_id not in ltm_layout["STM"]["index"].read_text(encoding="utf-8")
    assert mem_id not in ltm_layout["STM"]["keywords"].read_text(encoding="utf-8")
    assert mem_id not in ltm_layout["STM"]["heat"].read_text(encoding="utf-8")


def test_spec735_reconcile_preserves_unmarked_cross_tier_copy(ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-A1100002"
    source_meta = _meta(
        mem_id, tier="F", decay_countdown_days=0, created_at="2026-01-01T00:00:00+08:00")
    destination_meta = dict(source_meta, type="S")
    _write_ltm_entry(
        ltm_layout, "Full", mem_id, body="尚未完成删除的源正文", meta=source_meta)
    _write_ltm_entry(
        ltm_layout, "Summary", mem_id, body="已写入但未提交的目标摘要", meta=destination_meta)

    with pytest.raises(ValueError, match="ltm_active_tier_conflict"):
        MemoryStore().reconcile_ltm_projections()

    assert mem_id in ltm_layout["Full"]["body"].read_text(encoding="utf-8")
    assert mem_id in ltm_layout["Summary"]["body"].read_text(encoding="utf-8")
    assert mem_id in json.loads(
        ltm_layout["Summary"]["meta"].read_text(encoding="utf-8"))


def test_spec735_reconcile_preserves_partial_source_next_to_complete_backup(
        ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-A1100003"
    source_meta = _meta(
        mem_id, decay_countdown_days=0, created_at="2026-01-01T00:00:00+08:00")
    ltm_layout["Abstract"]["meta"].write_text(
        json.dumps({mem_id: source_meta}, ensure_ascii=False), encoding="utf-8")
    _write_ltm_entry(
        ltm_layout, "Backup", mem_id, body="已完整落入备份层", meta=source_meta)

    with pytest.raises(ValueError, match="ltm_body_meta_conflict"):
        MemoryStore().reconcile_ltm_projections()

    assert mem_id in json.loads(
        ltm_layout["Abstract"]["meta"].read_text(encoding="utf-8"))
    assert mem_id in ltm_layout["Backup"]["body"].read_text(encoding="utf-8")
    assert mem_id in json.loads(
        ltm_layout["Backup"]["meta"].read_text(encoding="utf-8"))


@pytest.mark.parametrize("variant", ["not_due", "identity_mismatch"])
def test_spec735_reconcile_does_not_guess_unproven_cross_tier_duplicate(
        ltm_layout, variant):
    from data.memory_store import MemoryStore

    mem_id = "MEM-A1100004"
    source_meta = _meta(mem_id, tier="F", decay_countdown_days=1)
    destination_meta = dict(source_meta, type="S")
    if variant == "identity_mismatch":
        source_meta["decay_countdown_days"] = 0
        destination_meta["title"] = "另一条记忆"
    _write_ltm_entry(ltm_layout, "Full", mem_id, meta=source_meta)
    _write_ltm_entry(ltm_layout, "Summary", mem_id, meta=destination_meta)

    with pytest.raises(ValueError, match="ltm_active_tier_conflict"):
        MemoryStore().reconcile_ltm_projections()


@pytest.mark.parametrize("failed_part", ["body", "meta", "tier_index", "root_index"])
def test_spec735_ltm_write_retries_each_partial_failure(
        ltm_layout, monkeypatch, failed_part):
    from data import memory_store as module

    mem_id = "MEM-FAB00001"
    target = {
        "body": str(ltm_layout["Full"]["body"]),
        "meta": str(ltm_layout["Full"]["meta"]),
        "tier_index": str(ltm_layout["Full"]["index"]),
        "root_index": str(ltm_layout["keywords"]),
    }[failed_part]
    real_text = module.atomic_write_text
    real_json = module.atomic_write_json

    def text_write(path, value, **kwargs):
        if str(path) == target:
            raise OSError(f"injected:{failed_part}")
        return real_text(path, value, **kwargs)

    def json_write(path, value, **kwargs):
        if str(path) == target:
            raise OSError(f"injected:{failed_part}")
        return real_json(path, value, **kwargs)

    monkeypatch.setattr(module, "atomic_write_text", text_write)
    monkeypatch.setattr(module, "atomic_write_json", json_write)
    store = module.MemoryStore()
    with pytest.raises(OSError, match="injected"):
        store.store_ltm_entry(
            "Full", mem_id, "唯一正文", _meta(mem_id, tags=["立即索引"], tier="F"))

    monkeypatch.setattr(module, "atomic_write_text", real_text)
    monkeypatch.setattr(module, "atomic_write_json", real_json)
    store.store_ltm_entry(
        "Full", mem_id, "唯一正文", _meta(mem_id, tags=["立即索引"], tier="F"))

    assert store.verify_ltm_entry(mem_id, tier="Full") == "Full"
    keywords = json.loads(ltm_layout["keywords"].read_text(encoding="utf-8"))
    assert keywords["index"]["立即索引"] == [f"{mem_id}[F]"]


def test_spec735_ltm_meta_conflict_is_zero_mutation(ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-FAB00004"
    target_meta = _meta(mem_id, title="已有冲突", tier="F")
    ltm_layout["Full"]["body"].write_text("", encoding="utf-8")
    ltm_layout["Full"]["meta"].write_text(
        json.dumps({mem_id: target_meta}, ensure_ascii=False), encoding="utf-8")
    before = {
        name: ltm_layout["Full"][name].read_bytes()
        for name in ("body", "meta")
    }

    with pytest.raises(ValueError, match="ltm_meta_conflict"):
        MemoryStore().store_ltm_entry(
            "Full", mem_id, "不应写入的正文",
            _meta(mem_id, title="另一份元数据", tier="F"),
        )

    assert before == {
        name: ltm_layout["Full"][name].read_bytes()
        for name in ("body", "meta")
    }


@pytest.mark.parametrize(
    "failed_part", ["tier_index", "root_index", "overlay", "verify", "source_body"])
def test_spec735_cross_tier_write_keeps_rich_source_until_target_is_verified(
        ltm_layout, monkeypatch, failed_part):
    from data import memory_store as module

    mem_id = "MEM-FAB00003"
    source_meta = _meta(mem_id, tier="F", tags=["富源"])
    _write_ltm_entry(
        ltm_layout, "Full", mem_id, body="不可提前删除的完整富源", meta=source_meta)
    source_before = {
        name: ltm_layout["Full"][name].read_bytes()
        for name in ("body", "meta")
    }
    target = {
        "tier_index": str(ltm_layout["Summary"]["index"]),
        "root_index": str(ltm_layout["keywords"]),
        "source_body": str(ltm_layout["Full"]["body"]),
    }.get(failed_part)
    real_text = module.atomic_write_text
    real_json = module.atomic_write_json

    def text_write(path, value, **kwargs):
        if target and str(path) == target:
            raise OSError(f"injected:{failed_part}")
        return real_text(path, value, **kwargs)

    def json_write(path, value, **kwargs):
        if target and str(path) == target:
            raise OSError(f"injected:{failed_part}")
        return real_json(path, value, **kwargs)

    monkeypatch.setattr(module, "atomic_write_text", text_write)
    monkeypatch.setattr(module, "atomic_write_json", json_write)
    def fail(*_args, **_kwargs):
        raise OSError(f"injected:{failed_part}")
    if failed_part == "overlay":
        monkeypatch.setattr(module, "write_memory_overlay_entry", fail)
    if failed_part == "verify":
        monkeypatch.setattr(module.MemoryStore, "_verify_ltm_tier_entry", fail)

    with pytest.raises(OSError, match="injected"):
        module.MemoryStore().store_ltm_entry(
            "Summary",
            mem_id,
            "目标压缩摘要",
            dict(source_meta, type="S"),
            source_tier="Full",
        )

    assert source_before == {
        name: ltm_layout["Full"][name].read_bytes()
        for name in ("body", "meta")
    }


def test_spec756_memory_search_ignores_same_id_stm_transition_copy(ltm_layout):
    from assembly.context_indexes import build_ltm_memory_search

    mem_id = "MEM-A1100005"
    ltm_layout["STM"]["body"].write_text(
        f"## {mem_id}\nSTM 过渡副本没有目标事实\n", encoding="utf-8")
    ltm_layout["STM"]["meta"].write_text(
        json.dumps({mem_id: _meta(mem_id, title="STM 副本")}, ensure_ascii=False),
        encoding="utf-8",
    )
    _write_ltm_entry(
        ltm_layout,
        "Abstract",
        mem_id,
        body="**标题**：长期记忆\n**正文**（≤128字）：唯一检索词是月球露营",
        meta=_meta(mem_id, title="长期记忆"),
    )

    result = build_ltm_memory_search(["月球露营"])
    content = result["content"]

    assert mem_id in content
    assert "[LTM/Abstract]" in content
    assert "创建坐标：meta/R000001" in content
    assert "没有命中" not in content


def test_spec756_memory_search_excludes_dynamic_overlay_fields(
        ltm_layout):
    from assembly.context_indexes import build_ltm_memory_search

    mem_id = "MEM-A1100006"
    body = "\n".join((
        "**交互对象**：TzPz",
        "**入库**：第1轮",
        "**最后调用**：第2轮",
        "**标题**：动态投影检查",
        "现状概况：旧挂接备注",
        "**正文**（≤128字）：静态事实",
        "入库时间：2025-01-01T00:00:00+08:00",
        "关联容器：OLD-CONTAINER",
    ))
    meta = _meta(
        mem_id,
        title="动态投影检查",
        created_at="2025-01-01T00:00:00+08:00",
        last_recalled_at="2026-08-11T10:20:30+08:00",
        last_recalled_round=19,
        current_overview="新挂接备注",
        current_overview_updated_at="2026-08-11T10:00:00+08:00",
        linked_containers=["NEW-CONTAINER"],
    )
    _write_ltm_entry(ltm_layout, "Abstract", mem_id, body=body, meta=meta)

    stale = build_ltm_memory_search(["旧挂接备注", "OLD-CONTAINER"])
    current = build_ltm_memory_search(["新挂接备注", "NEW-CONTAINER"])

    assert mem_id not in stale["content"]
    assert "没有命中" in stale["content"]
    assert mem_id not in current["content"]
    assert "没有命中" in current["content"]


def test_spec756_memory_search_is_locator_only_and_byte_read_only(ltm_layout):
    from assembly.context_indexes import build_ltm_memory_search

    mem_id = "MEM-A1100008"
    _write_ltm_entry(
        ltm_layout,
        "Abstract",
        mem_id,
        body=(
            "**标题**：Unicode 检索\n"
            "**梗概**（≤128字）：前缀内容用于制造窗口，目标是ＡＢＣ露营日期2026年8月17日，"
            "后缀内容继续超过三十二个字符。"
        ),
        meta=_meta(
            mem_id,
            title="Unicode 检索",
            tags=["露营"],
            created_instance_id="meta",
            created_round=17,
        ),
    )
    for path in ltm_layout["STM"].values():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("{}" if path.suffix == ".json" else "", encoding="utf-8")
    before = {
        path: path.read_bytes()
        for path in ltm_layout["root"].parent.parent.rglob("*")
        if path.is_file()
    }

    result = build_ltm_memory_search([" abc ", "露营"], offset=0, limit=1)
    after = {
        path: path.read_bytes()
        for path in ltm_layout["root"].parent.parent.rglob("*")
        if path.is_file()
    }

    assert result["total_matches"] == 1
    assert result["has_more"] is False
    candidate = result["candidates"][0]
    assert candidate["mem_id"] == mem_id
    assert candidate["created_instance_id"] == "meta"
    assert candidate["created_round"] == 17
    assert set(candidate["matched_terms"]) == {"abc", "露营"}
    assert len(candidate["snippet"]) <= 32
    assert "ＡＢＣ" in candidate["snippet"]
    assert before == after


def test_spec756_memory_search_fails_closed_on_active_tier_conflict(ltm_layout):
    from assembly.context_indexes import build_ltm_memory_search

    mem_id = "MEM-A1100009"
    for tier in ("Full", "Abstract"):
        _write_ltm_entry(
            ltm_layout,
            tier,
            mem_id,
            body="**标题**：冲突\n**正文**：冲突检索词",
            meta=_meta(mem_id, title="冲突", tags=["冲突检索词"]),
        )

    with pytest.raises(ValueError, match="ltm_active_tier_conflict"):
        build_ltm_memory_search(["冲突检索词"])


def test_spec735_mark_recalled_rebuilds_eight_column_ltm_tier_index(ltm_layout):
    from data.memory_store import MemoryStore

    mem_id = "MEM-A1100007"
    _write_ltm_entry(
        ltm_layout,
        "Abstract",
        mem_id,
        meta=_meta(mem_id, created_round=1, last_recalled_round=2),
    )
    store = MemoryStore()
    store.reconcile_ltm_projections()

    store.mark_recalled(
        mem_id, round_num=19, recalled_at="2026-08-11T10:20:30+08:00")

    meta = json.loads(
        ltm_layout["Abstract"]["meta"].read_text(encoding="utf-8"))[mem_id]
    index_text = ltm_layout["Abstract"]["index"].read_text(encoding="utf-8")
    header = next(line for line in index_text.splitlines() if line.startswith("| 编号"))
    row = next(line for line in index_text.splitlines() if line.startswith(f"| {mem_id}"))
    assert meta["last_recalled_round"] == 19
    assert meta["last_recalled_at"] == "2026-08-11T10:20:30+08:00"
    assert len([cell for cell in header.strip("|").split("|")]) == 8
    assert len([cell for cell in row.strip("|").split("|")]) == 8
    assert "第1轮 / 第19轮" in row


@pytest.mark.parametrize(
    "failed_action", ["index", "keywords", "meta", "heat", "body"])
def test_spec735_stm_delete_failure_restores_every_source_projection(
        tmp_path, monkeypatch, failed_action):
    import paths
    from data import memory_heat, memory_index, memory_store
    from schemas.memory import default_heat_entry

    mem_id = "MEM-FAB00002"
    root = tmp_path / "stm"
    root.mkdir()
    files = {
        "body": root / "memory.md",
        "meta": root / "meta.json",
        "index": root / "index.md",
        "keywords": root / "keywords.json",
        "heat": root / "heat.json",
    }
    monkeypatch.setattr(memory_store, "MEMORY_MD", str(files["body"]))
    monkeypatch.setattr(memory_store, "META_JSON", str(files["meta"]))
    monkeypatch.setattr(memory_store, "INDEX_MD", str(files["index"]))
    monkeypatch.setattr(paths, "KEYWORDS_JSON", str(files["keywords"]))
    monkeypatch.setattr(paths, "HEAT_JSON", str(files["heat"]))
    monkeypatch.setattr(memory_index, "KEYWORDS_JSON", str(files["keywords"]))
    monkeypatch.setattr(memory_heat, "HEAT_JSON", str(files["heat"]))
    files["body"].write_text(f"## {mem_id}\n原始正文\n", encoding="utf-8")
    files["meta"].write_text(
        json.dumps({mem_id: _meta(mem_id)}, ensure_ascii=False), encoding="utf-8")
    files["index"].write_text(
        f"| {mem_id} | [A] | 2 | 原始索引 |\n", encoding="utf-8")
    files["keywords"].write_text(
        json.dumps({"index": {"原始关键词": [mem_id]}}, ensure_ascii=False),
        encoding="utf-8",
    )
    heat_entry = default_heat_entry()
    heat_entry["stored"] = True
    files["heat"].write_text(
        json.dumps({"entries": {mem_id: heat_entry}}, ensure_ascii=False),
        encoding="utf-8",
    )
    initial = {name: path.read_bytes() for name, path in files.items()}

    targets = {
        "body": (memory_store.MemoryStore, "remove_stm_body"),
        "index": (memory_store.MemoryStore, "remove_index"),
        "keywords": (memory_index.MemoryIndex, "remove_stm_entry"),
        "meta": (memory_store.MemoryStore, "delete_meta"),
        "heat": (memory_heat.MemoryHeat, "remove_entry"),
    }
    owner, method_name = targets[failed_action]
    original = getattr(owner, method_name)

    def mutate_then_fail(self, *args, **kwargs):
        original(self, *args, **kwargs)
        raise OSError(f"injected:{failed_action}")

    monkeypatch.setattr(owner, method_name, mutate_then_fail)
    with pytest.raises(OSError, match="injected"):
        memory_store.MemoryStore().remove_stm_copy(mem_id)
    assert {name: path.read_bytes() for name, path in files.items()} == initial


def test_spec735_degree_only_gates_second_hop(tmp_path, monkeypatch):
    import paths
    from assembly.context_indexes import build_association_index

    association = tmp_path / "association"
    connection = tmp_path / "connection"
    memory = tmp_path / "memory"
    association.mkdir()
    connection.mkdir()
    memory.mkdir()
    stm_keywords = memory / "keywords.json"
    ltm_keywords = tmp_path / "ltm-keywords.json"
    ltm_meta = tmp_path / "ltm-meta.json"
    empty_meta = tmp_path / "empty-meta.json"
    empty_meta.write_text("{}", encoding="utf-8")
    stm_keywords.write_text(json.dumps({"index": {}}), encoding="utf-8")
    ltm_keywords.write_text(json.dumps({
        "index": {
            "alpha": ["MEM-00000001[A]"],
            "beta": ["MEM-00000002[A]"],
            "backup": ["MEM-00000003[B]"],
            "dangling": ["MEM-00000004[A]"],
        }
    }), encoding="utf-8")
    ltm_meta.write_text(json.dumps({
        "MEM-00000001": _meta("MEM-00000001", title="直接"),
        "MEM-00000002": _meta("MEM-00000002", title="二跳"),
    }), encoding="utf-8")
    monkeypatch.setattr(paths, "ASSOCIATION_SET_DIR", str(association))
    monkeypatch.setattr(paths, "CONNECTION_SET_DIR", str(connection))
    monkeypatch.setattr(paths, "KEYWORDS_JSON", str(stm_keywords))
    monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(ltm_keywords))
    monkeypatch.setattr(paths, "STM_MEMORY_DIR", str(memory))
    monkeypatch.setattr(paths, "LTM_FULL_META_JSON", str(empty_meta))
    monkeypatch.setattr(paths, "LTM_SUMMARY_META_JSON", str(empty_meta))
    monkeypatch.setattr(paths, "LTM_ABSTRACT_META_JSON", str(ltm_meta))
    monkeypatch.setattr(paths, "LTM_PINNED_META_JSON", str(empty_meta))
    assembler = SimpleNamespace(_memory_meta_visible=lambda _meta: True)

    def write_pairs(count):
        pairs = {"alpha|||beta": 1}
        pairs.update({f"alpha|||partner-{index}": 1 for index in range(count - 1)})
        (association / "assoc_kw_kw.json").write_text(
            json.dumps(pairs), encoding="utf-8")

    (association / "assoc_kw_kw.json").write_text("{}", encoding="utf-8")
    degree_zero = build_association_index(assembler, input_keywords=["alpha"])
    assert "MEM-00000001" in degree_zero
    assert "MEM-00000002" not in degree_zero

    write_pairs(15)
    below = build_association_index(assembler, input_keywords=["alpha"])
    assert "MEM-00000001" in below
    assert "MEM-00000002" not in below

    write_pairs(16)
    (connection / "pending.jsonl").write_text("\n".join((
        json.dumps({
            "word_a": "alpha",
            "word_b": "beta",
            "entry_a": "MEM-00000002",
            "entry_b": "MEM-00000001",
        }),
        json.dumps({
            "word_a": "alpha",
            "word_b": "orphan",
            "entry_a": "MEM-00000002",
            "entry_b": "DC-NOT-A-MEMORY",
        }),
        "",
    )), encoding="utf-8")
    graduated = build_association_index(assembler, input_keywords=["alpha"])
    assert "MEM-00000001" in graduated
    assert "MEM-00000002" in graduated
    assert graduated.count("- MEM-00000001 ") == 1
    assert "DC-NOT-A-MEMORY" not in graduated
    unavailable = build_association_index(
        assembler, input_keywords=["backup", "dangling"])
    assert "MEM-00000003" not in unavailable
    assert "MEM-00000004" not in unavailable
