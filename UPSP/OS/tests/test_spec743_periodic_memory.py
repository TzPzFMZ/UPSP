import json
from pathlib import Path

import pytest


class _Config:
    def __init__(self, limit=65536):
        self.limit = limit

    def get_periodic_limits(self):
        return {"periodic_memory_items_chars": self.limit}


class _Heat:
    def __init__(self, path):
        self.path = Path(path)

    def load_heat(self):
        return json.loads(self.path.read_text(encoding="utf-8"))


@pytest.fixture
def periodic_layout(tmp_path, monkeypatch):
    import paths
    from assembly.context import ContextAssembler
    from data import (
        memory_heat,
        memory_index,
        memory_store,
        periodic_mount_store,
        periodic_pin_owner_store,
    )
    from data.memory_store import MemoryStore
    from data.periodic_mount_store import PeriodicMountStore
    from data.periodic_pin_owner_store import PeriodicPinOwnerStore
    from data.resident_list_store import ResidentListStore
    from data.state_store import StateStore

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
        body_attr = f"LTM_{tier.upper()}_{tier.upper()}_MD"
        if tier == "Full":
            body_attr = "LTM_FULL_FULL_MD"
        body = directory / body_name
        meta = directory / "meta.json"
        index = directory / "index.md"
        meta.write_text("{}", encoding="utf-8")
        monkeypatch.setattr(paths, body_attr, str(body))
        monkeypatch.setattr(paths, f"LTM_{tier.upper()}_META_JSON", str(meta))
        monkeypatch.setattr(
            paths, f"LTM_{tier.upper()}_INDEX_MD", str(index), raising=False)
        monkeypatch.setattr(memory_store, body_attr, str(body), raising=False)
        monkeypatch.setattr(
            memory_store, f"LTM_{tier.upper()}_META_JSON", str(meta), raising=False)
        ltm[tier] = {"body": body, "meta": meta, "index": index}

    keywords = ltm_root / "keywords.json"
    compression_ledger = ltm_root / "memory_compression_pending.json"
    links = tmp_path / "local" / "LTM" / "memory_links.json"
    monkeypatch.setattr(paths, "LTM_KEYWORDS_JSON", str(keywords))
    monkeypatch.setattr(
        paths, "MEMORY_COMPRESSION_PENDING_JSON", str(compression_ledger))
    monkeypatch.setattr(memory_store, "LTM_MEMORY_LINKS_JSON", str(links))
    monkeypatch.setattr(memory_store, "LTM_META_PATHS", {
        str(ltm[tier]["meta"].resolve())
        for tier in ("Full", "Summary", "Abstract", "Pinned")
    })

    stm = tmp_path / "local" / "STM" / "memory"
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

    mount_path = tmp_path / "local" / "STM" / "context" / "periodic_mounts.json"
    owner_path = ltm_root / "periodic_pin_owners.json"
    monkeypatch.setattr(
        periodic_mount_store, "DEFAULT_PERIODIC_MOUNTS_PATH", str(mount_path))
    monkeypatch.setattr(
        periodic_pin_owner_store, "PERIODIC_PIN_OWNERS_JSON", str(owner_path))
    monkeypatch.setattr(paths, "ACTIVE_INSTANCE_ID", "meta")
    state_store = StateStore(str(tmp_path / "local" / "state.json"))
    state_store.init_if_missing()
    resident_store = ResidentListStore(str(
        tmp_path / "local" / "STM" / "context" / "resident_list.json"
    ))
    resident_store.reconcile()
    assembler = ContextAssembler(
        state_store=state_store,
        config_store=_Config(),
        resident_store=resident_store,
    )
    env = {
        "tmp": tmp_path,
        "ltm": ltm,
        "stm": stm_paths,
        "store": MemoryStore(),
        "heat": _Heat(stm_paths["heat"]),
        "assembler": assembler,
        "resident_store": resident_store,
        "mount_path": mount_path,
        "owner_path": owner_path,
        "mount_store": PeriodicMountStore(str(mount_path), now_fn=lambda: "2026-08-13T10:00:00+08:00"),
        "owner_store": PeriodicPinOwnerStore(str(owner_path), now_fn=lambda: "2026-08-13T10:00:00+08:00"),
    }
    return env


def _meta(mem_id, *, weight=5, access="public"):
    return {
        "id": mem_id,
        "type": "F",
        "weight": weight,
        "title": f"标题 {mem_id}",
        "subject": "TzPz",
        "access": access,
        "tags": ["定期测试"],
        "created_at": "2026-08-13T08:00:00+08:00",
        "stored_at": "2026-08-13T08:00:00+08:00",
        "created_round": 3,
        "created_instance_id": "meta",
        "last_recalled_round": 7,
        "last_recalled_instance_id": "meta",
        "last_recalled_at": "2026-08-13T09:00:00+08:00",
        "linked_containers": ["PRJ-TEST"],
        "current_overview": "易变挂接备注",
        "current_overview_updated_at": "",
        "decay_period_days": 30,
        "decay_countdown_days": 30,
    }


def _add_stm(env, mem_id="MEM-74300001", *, weight=5, access="public"):
    meta = _meta(mem_id, weight=weight, access=access)
    meta["stored_at"] = ""
    body = (
        f"## {mem_id}  [F]  权重{weight}\n"
        f"**标题**：{meta['title']}\n"
        "**正文**：这是真实公共记忆正文。\n"
    )
    env["stm"]["body"].write_text(body, encoding="utf-8")
    env["stm"]["meta"].write_text(
        json.dumps({mem_id: meta}, ensure_ascii=False), encoding="utf-8")
    env["stm"]["index"].write_text(
        f"| {mem_id} | [F] | {weight} | 标题 |\n", encoding="utf-8")
    env["stm"]["keywords"].write_text(
        json.dumps({"index": {"定期测试": [mem_id]}}, ensure_ascii=False),
        encoding="utf-8",
    )
    heat = {
        "H": 88,
        "zone": "显著区",
        "AH_high": 6,
        "AH_low": 2,
        "degrade": False,
        "last_heat_at": "2026-08-13T09:30:00+08:00",
        "last_recalled_round": 7,
        "last_recalled_at": "2026-08-13T09:00:00+08:00",
    }
    env["stm"]["heat"].write_text(
        json.dumps({"entries": {mem_id: heat}}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    if access == "public" and env["store"].ltm_entry_state(mem_id) is None:
        tier = "Full" if weight == 5 else "Summary" if weight >= 3 else "Abstract"
        env["store"].store_ltm_entry(tier, mem_id, body, meta)
    return body, meta, heat


def _add_ltm(env, tier, mem_id, *, weight=5):
    meta = _meta(mem_id, weight=weight)
    label = "内容" if weight == 5 else "摘要" if weight >= 3 else "梗概"
    env["store"].store_ltm_entry(
        tier, mem_id, f"**标题**：{meta['title']}\n**{label}**：{tier} 真实正文。", meta)


def _processor(env, *, instance_id="meta", mount_store=None, limit=65536, fault=None):
    from logic.periodic_memory_mount import PeriodicMemoryMountProcessor

    return PeriodicMemoryMountProcessor(
        memory_store=env["store"],
        heat=env["heat"],
        assembler=env["assembler"],
        config_store=_Config(limit),
        mount_store=mount_store or env["mount_store"],
        owner_store=env["owner_store"],
        instance_id=instance_id,
        now_fn=lambda: "2026-08-13T10:00:00+08:00",
        fault_hook=fault,
    )


def test_spec746_mounts_pending_dual_residence_without_recall(
        periodic_layout):
    env = periodic_layout
    mem_id = "MEM-74300001"
    _body, _meta_value, heat_before = _add_stm(env, mem_id)

    receipt = _processor(env).apply("mount", mem_id)

    assert receipt["status"] == "applied"
    assert receipt["tool_id"] == "periodic_memory_mount"
    assert receipt["recall_applied"] is False
    assert receipt["provider_called"] is False
    assert receipt["after"]["memory_layers"] == ["STM", "LTM/Pinned"]
    assert receipt["after"]["periodic_pin_owned"] is True
    heat_after = env["heat"].load_heat()["entries"][mem_id]
    assert {key: heat_after[key] for key in heat_before} == heat_before
    assert env["store"].ltm_entry_state(mem_id)["meta"]["stored_at"]
    row = next(item for item in env["store"].list_public_entries() if item["id"] == mem_id)
    assert row["memory_layers"] == ["STM", "LTM/Pinned"]
    assert row["stm_present"] is True
    assert row["ltm_layer"] == "LTM/Pinned"


def test_spec757_periodic_mount_cancels_pending_compression_without_recall(
        periodic_layout):
    from data.memory_compression_store import MemoryCompressionManager

    env = periodic_layout
    mem_id = "MEM-75710001"
    _add_stm(env, mem_id, weight=5)
    manager = MemoryCompressionManager(memory_store=env["store"])
    manager.settle_stm_forgetting(mem_id, round_num=75)
    assert env["store"].stm_entry_state(mem_id)["body"] is None
    assert json.loads(
        Path(env["ltm"]["Full"]["body"]).parent.parent.joinpath(
            "memory_compression_pending.json").read_text(encoding="utf-8")
    )["entries"][0]["mem_id"] == mem_id

    receipt = _processor(env).apply("mount", mem_id)

    assert receipt["status"] == "applied"
    assert receipt["recall_applied"] is False
    assert receipt["after"]["ltm_layer"] == "LTM/Pinned"
    assert receipt["after"]["stm_present"] is False
    assert env["store"].ltm_entry_state(mem_id)["meta"]["stored_at"]
    ledger = Path(env["ltm"]["Full"]["body"]).parent.parent / (
        "memory_compression_pending.json")
    assert json.loads(ledger.read_text(encoding="utf-8"))["entries"] == []


@pytest.mark.parametrize(
    ("weight", "expected"),
    [(5, "Full"), (4, "Summary"), (3, "Summary"), (2, "Abstract"), (1, "Abstract")],
)
def test_spec743_last_periodic_owner_returns_to_weight_tier(
        periodic_layout, weight, expected):
    env = periodic_layout
    mem_id = f"MEM-74301{weight:03X}"
    _add_ltm(env, expected, mem_id, weight=weight)

    _processor(env).apply("mount", mem_id)
    receipt = _processor(env).apply("unmount", mem_id)

    assert receipt["after"]["ltm_layer"] == f"LTM/{expected}"
    assert env["store"].verify_ltm_entry(mem_id) == expected


def test_spec743_preexisting_pinned_survives_unmount(periodic_layout):
    env = periodic_layout
    mem_id = "MEM-74300002"
    _add_ltm(env, "Pinned", mem_id, weight=2)

    mounted = _processor(env).apply("mount", mem_id)
    unmounted = _processor(env).apply("unmount", mem_id)

    assert mounted["after"]["periodic_pin_owned"] is False
    assert unmounted["after"]["ltm_layer"] == "LTM/Pinned"
    assert env["store"].verify_ltm_entry(mem_id) == "Pinned"


def test_spec743_duplicate_mount_and_unmount_are_noop(periodic_layout):
    env = periodic_layout
    mem_id = "MEM-74300009"
    _add_stm(env, mem_id)
    processor = _processor(env)
    processor.apply("mount", mem_id)
    mounted_files = {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    }

    assert processor.apply("mount", mem_id)["status"] == "noop"
    assert {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    } == mounted_files
    processor.apply("unmount", mem_id)
    assert processor.apply("unmount", mem_id)["status"] == "noop"


def test_spec743_multi_instance_keeps_pinned_until_last_owner(periodic_layout):
    from data.periodic_mount_store import PeriodicMountStore

    env = periodic_layout
    mem_id = "MEM-74300003"
    _add_ltm(env, "Full", mem_id, weight=5)
    store_a = PeriodicMountStore(str(env["tmp"] / "a-periodic.json"))
    store_b = PeriodicMountStore(str(env["tmp"] / "b-periodic.json"))
    proc_a = _processor(env, instance_id="A", mount_store=store_a)
    proc_b = _processor(env, instance_id="B", mount_store=store_b)

    proc_a.apply("mount", mem_id)
    proc_b.apply("mount", mem_id)
    proc_a.apply("unmount", mem_id)
    assert env["store"].verify_ltm_entry(mem_id) == "Pinned"
    proc_b.apply("unmount", mem_id)
    assert env["store"].verify_ltm_entry(mem_id) == "Full"


def test_spec746_misaligned_memory_waits_for_completion_and_can_be_cancelled(
        periodic_layout):
    env = periodic_layout
    mem_id = "MEM-74600001"
    _add_ltm(env, "Summary", mem_id, weight=5)
    before = env["store"].snapshot_ltm_files()

    queued = _processor(env).apply("mount", mem_id)

    assert queued["mount_status"] == "awaiting_completion"
    assert queued["outcome"] == "pending"
    assert env["store"].ltm_entry_state(mem_id)["tier"] == "Summary"
    assert env["store"].snapshot_ltm_files() == before
    pending = env["mount_store"].load()["pending_memory_items"]
    assert [item["id"] for item in pending] == [mem_id]

    cancelled = _processor(env).apply("unmount", mem_id)
    assert cancelled["outcome"] == "pending_cancelled"
    assert env["mount_store"].load()["pending_memory_items"] == []


def test_spec746_completion_then_periodic_mount_preserves_stored_at(
        periodic_layout):
    from data.memory_heat import MemoryHeat
    from logic.memory_recall import MemoryRecallProcessor
    from logic.memory_reconsolidation import (
        MemoryReconsolidationProcessor,
        MemoryReconsolidationTracker,
    )

    env = periodic_layout
    mem_id = "MEM-74600002"
    _add_ltm(env, "Summary", mem_id, weight=5)
    original_stored_at = env["store"].ltm_entry_state(mem_id)["meta"]["stored_at"]
    periodic = _processor(env)
    assert periodic.apply("mount", mem_id)["outcome"] == "pending"

    recall = MemoryRecallProcessor(
        memory_store=env["store"], heat=MemoryHeat(),
        instance_id="meta",
    )
    tracker = MemoryReconsolidationTracker(746)
    recall.recall(
        mem_id, round_num=746, boosted_ids=set(),
        reconsolidation_tracker=tracker, periodic_requested=True,
    )
    completed = MemoryReconsolidationProcessor(
        memory_store=env["store"], assembler=env["assembler"]
    ).apply(
        tracker.get(mem_id), "恢复后的完整正文", ["恢复正文"]
    )
    assert completed["target_memory_layer"] == "LTM/Full"
    mounted = periodic.apply("mount", mem_id)
    final = env["store"].ltm_entry_state(mem_id)
    assert mounted["outcome"] == "mounted"
    assert final["tier"] == "Pinned"
    assert final["meta"]["stored_at"] == original_stored_at


@pytest.mark.parametrize(
    ("weight", "tier"),
    [(1, "Abstract"), (2, "Abstract"), (3, "Summary"),
     (4, "Summary"), (5, "Full")],
)
def test_spec746_memory_write_creates_pending_ltm_and_nine_field_heat(
        periodic_layout, monkeypatch, weight, tier):
    from data.memory_heat import MemoryHeat
    from logic import memory_write

    env = periodic_layout
    mem_id = f"MEM-74610{weight:03X}"
    monkeypatch.setattr(memory_write, "generate_mem_id", lambda: mem_id)

    class Relations:
        @staticmethod
        def resolve_active_subject(value):
            return value

    receipts = memory_write.apply_memory_write_declarations(
        [{
            "title": f"权重{weight}",
            "weight": weight,
            "subject": "TzPz",
            "body": "创建时即进入唯一 LTM 真源。",
            "candidate_keywords": ["唯一真源"],
        }],
        {"presence": {"confirmed_subjects": ["TzPz"]}},
        746,
        {
            "memory_store": env["store"],
            "memory_index": object(),
            "memory_heat": MemoryHeat(),
            "relation_store": Relations(),
        },
    )

    assert receipts[0]["status"] == "applied", receipts[0].get("reason")
    assert receipts[0]["ltm_layer"] == f"LTM/{tier}"
    assert receipts[0]["admission_status"] == "pending"
    ltm = env["store"].ltm_entry_state(mem_id)
    stm = env["store"].stm_entry_state(mem_id)
    assert ltm["tier"] == tier
    assert ltm["meta"]["stored_at"] == ""
    assert stm["body"] == ltm["body"]
    assert len(stm["heat"]) == 9
    assert "stored" not in stm["heat"]


def test_spec743_budget_rejection_has_no_partial_pinned_write(periodic_layout):
    from logic.periodic_memory_mount import PeriodicMemoryMountError

    env = periodic_layout
    mem_id = "MEM-74300004"
    _add_stm(env, mem_id)

    with pytest.raises(PeriodicMemoryMountError, match="periodic_memory_budget_exceeded"):
        _processor(env, limit=1).apply("mount", mem_id)

    assert env["store"].ltm_entry_state(mem_id)["tier"] == "Full"
    assert "stored" not in env["heat"].load_heat()["entries"][mem_id]
    assert not env["mount_path"].exists()
    assert not env["owner_path"].exists()


def test_spec743_budget_accepts_exact_limit_and_rejects_one_character_over(
        periodic_layout):
    from logic.periodic_memory_mount import PeriodicMemoryMountError

    env = periodic_layout
    mem_id = "MEM-7430000A"
    _add_stm(env, mem_id)
    normal = _processor(env)
    used = normal.apply("mount", mem_id)["periodic_chars_after"]
    normal.apply("unmount", mem_id)

    exact = _processor(env, limit=used)
    assert exact.apply("mount", mem_id)["periodic_chars_after"] == used
    exact.apply("unmount", mem_id)
    with pytest.raises(PeriodicMemoryMountError, match="budget_exceeded"):
        _processor(env, limit=used - 1).apply("mount", mem_id)


def test_spec743_configured_limit_can_exceed_default(periodic_layout):
    env = periodic_layout
    mem_id = "MEM-74300011"
    meta = _meta(mem_id)
    env["store"].store_ltm_entry(
        "Full", mem_id,
        f"**标题**：{meta['title']}\n**正文**：{'长' * 70_000}",
        meta,
    )

    receipt = _processor(env, limit=100_000).apply("mount", mem_id)

    assert receipt["periodic_chars_limit"] == 100_000
    assert receipt["periodic_chars_after"] > 65_536


def test_spec743_over_limit_state_keeps_unmount_as_recovery_path(
        periodic_layout):
    env = periodic_layout
    first = "MEM-7430000E"
    second = "MEM-7430000F"
    unmounted = "MEM-74300010"
    for mem_id in (first, second, unmounted):
        _add_ltm(env, "Full", mem_id)

    generous = _processor(env, limit=1_000_000)
    generous.apply("mount", first)
    generous.apply("mount", second)

    constrained = _processor(env, limit=1)
    noop = constrained.apply("unmount", unmounted)
    first_receipt = constrained.apply("unmount", first)

    assert noop["status"] == "noop"
    assert noop["periodic_chars_before"] > noop["periodic_chars_limit"]
    assert first_receipt["status"] == "applied"
    assert first_receipt["periodic_chars_after"] > first_receipt["periodic_chars_limit"]
    assert env["store"].verify_ltm_entry(first) == "Full"
    assert constrained.apply("unmount", second)["periodic_chars_after"] == 0


@pytest.mark.parametrize("case", ["missing", "private", "backup"])
def test_spec743_rejects_non_public_active_memory_sources(periodic_layout, case):
    from logic.periodic_memory_mount import PeriodicMemoryMountError

    env = periodic_layout
    mem_id = "MEM-7430000B"
    if case == "private":
        _add_stm(env, mem_id, access="private")
    elif case == "backup":
        _add_ltm(env, "Backup", mem_id)

    expected = {
        "missing": "periodic_memory_not_found",
        "private": "private_memory_deferred",
        "backup": "backup_memory_not_mountable",
    }[case]
    for action in ("mount", "unmount"):
        with pytest.raises(PeriodicMemoryMountError, match=expected):
            _processor(env).apply(action, mem_id)


@pytest.mark.parametrize(
    "stage",
    ["after_admission", "after_pinned", "after_owners", "after_mounts", "after_invalidate", "after_verify"],
)
def test_spec743_each_transaction_stage_rolls_back(periodic_layout, stage):
    env = periodic_layout
    mem_id = "MEM-74300005"
    _add_stm(env, mem_id)
    env["assembler"]._layer_cache[("setup", "periodic")] = "OLD"
    env["assembler"]._layer_block_cache[("setup", "periodic")] = [{"old": True}]
    before = {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    }

    def fail(current):
        if current == stage:
            raise RuntimeError(f"fault:{stage}")

    with pytest.raises(RuntimeError, match=f"fault:{stage}"):
        _processor(env, fault=fail).apply("mount", mem_id)

    after = {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    }
    assert after == before
    assert env["assembler"]._layer_cache[("setup", "periodic")] == "OLD"


def test_spec781_periodic_admission_preflights_resident_projection(
        periodic_layout, monkeypatch):
    env = periodic_layout
    mem_id = "MEM-74300015"
    _add_stm(env, mem_id)
    before = {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    }

    def reject_resident_update(*_args, **_kwargs):
        raise ValueError("resident_list_char_limit_exceeded:max=65536;actual=65537")

    monkeypatch.setattr(
        env["assembler"],
        "preflight_resident_source_update",
        reject_resident_update,
    )

    with pytest.raises(ValueError, match="resident_list_char_limit_exceeded"):
        _processor(env).apply("mount", mem_id)

    after = {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    }
    assert after == before


@pytest.mark.parametrize(
    "stage",
    ["after_mounts", "after_owners", "after_pinned", "after_invalidate", "after_verify"],
)
def test_spec743_each_unmount_stage_rolls_back(periodic_layout, stage):
    env = periodic_layout
    mem_id = "MEM-7430000D"
    _add_ltm(env, "Full", mem_id)
    _processor(env).apply("mount", mem_id)
    env["assembler"]._layer_cache[("setup", "periodic")] = "MOUNTED"
    env["assembler"]._layer_block_cache[("setup", "periodic")] = [{"mounted": True}]
    before = {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    }

    def fail(current):
        if current == stage:
            raise RuntimeError(f"fault:{stage}")

    with pytest.raises(RuntimeError, match=f"fault:{stage}"):
        _processor(env, fault=fail).apply("unmount", mem_id)

    after = {
        path.relative_to(env["tmp"]): path.read_bytes()
        for path in env["tmp"].rglob("*") if path.is_file()
    }
    assert after == before
    assert env["assembler"]._layer_cache[("setup", "periodic")] == "MOUNTED"


def test_spec743_periodic_assembly_reads_live_pinned_truth_and_unmount_invalidates(
        periodic_layout):
    env = periodic_layout
    mem_id = "MEM-74300006"
    _add_ltm(env, "Full", mem_id)
    processor = _processor(env)
    processor.apply("mount", mem_id)

    for step in ("setup", "reaction", "cleanup"):
        mounted = env["assembler"]._build_periodic({}, step, "interactive")
        assert mem_id in mounted
        assert "Full 真实正文" in mounted
        assert "最近调用时间" not in mounted
        assert "易变挂接备注" not in mounted

    pinned = env["ltm"]["Pinned"]["body"]
    pinned.write_text(
        pinned.read_text(encoding="utf-8").replace(
            "Full 真实正文", "正文静态更新已同步"),
        encoding="utf-8",
    )
    pinned_meta = json.loads(env["ltm"]["Pinned"]["meta"].read_text(
        encoding="utf-8"))
    pinned_meta[mem_id]["title"] = "静态标题更新已同步"
    env["ltm"]["Pinned"]["meta"].write_text(
        json.dumps(pinned_meta, ensure_ascii=False), encoding="utf-8")
    env["store"].reconcile_ltm_projections()
    env["assembler"].invalidate_layer("periodic")
    refreshed = env["assembler"]._build_periodic(
        {}, "reaction", "interactive")
    assert "正文静态更新已同步" in refreshed
    assert "静态标题更新已同步" in refreshed
    assert f"标题 {mem_id}" not in refreshed

    processor.apply("unmount", mem_id)
    assert env["assembler"]._build_periodic({}, "setup", "interactive") == ""


@pytest.mark.parametrize("case", ["corrupt", "missing", "not_pinned", "owner_missing"])
def test_spec743_required_periodic_truth_failure_is_not_silenced(
        periodic_layout, case):
    from errors import RequiredContextError

    env = periodic_layout
    mem_id = "MEM-7430000C"
    env["mount_path"].parent.mkdir(parents=True, exist_ok=True)
    if case == "corrupt":
        env["mount_path"].write_text("{broken", encoding="utf-8")
    else:
        if case == "not_pinned":
            _add_ltm(env, "Full", mem_id)
        elif case == "owner_missing":
            _add_ltm(env, "Pinned", mem_id)
        env["mount_path"].write_text(json.dumps({
            "schema_version": "periodic_mounts.v2",
            "updated_at": "2026-08-13T10:00:00+08:00",
            "instance_id": "meta",
            "periodic_memory_items": [{
                "id": mem_id,
                "source": "user_manual",
                "mounted_at": "2026-08-13T10:00:00+08:00",
            }],
        }), encoding="utf-8")
        if case != "owner_missing":
            env["owner_path"].write_text(json.dumps({
                "schema_version": "periodic_pin_owners.v1",
                "updated_at": "2026-08-13T10:00:00+08:00",
                "entries": {
                    mem_id: {
                        "pin_source": "periodic",
                        "owners": ["meta"],
                        "created_at": "2026-08-13T10:00:00+08:00",
                    }
                },
            }), encoding="utf-8")

    with pytest.raises(RequiredContextError, match="periodic_memory_mounts"):
        env["assembler"]._build_periodic({}, "setup", "interactive")


def test_spec743_nonempty_legacy_mounts_are_read_only(periodic_layout):
    from logic.periodic_memory_mount import PeriodicMemoryMountError

    env = periodic_layout
    mem_id = "MEM-74300007"
    _add_stm(env, mem_id)
    env["mount_path"].parent.mkdir(parents=True, exist_ok=True)
    env["mount_path"].write_text(json.dumps({
        "periodic_memory_items": [{"id": mem_id, "rendered_text": "旧投影"}],
    }), encoding="utf-8")

    with pytest.raises(PeriodicMemoryMountError, match="legacy_read_only"):
        _processor(env).apply("mount", mem_id)

    assert json.loads(env["mount_path"].read_text(
        encoding="utf-8"))["periodic_memory_items"][0]["rendered_text"] == "旧投影"
