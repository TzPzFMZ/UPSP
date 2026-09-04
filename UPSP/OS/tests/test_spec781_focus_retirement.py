import json
import os
import sys
from copy import deepcopy
from pathlib import Path

import pytest


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


def test_active_tool_registry_uses_hidden_routes_not_legacy_families():
    from logic import protocol_tools

    TOOL_DEFINITIONS = protocol_tools.TOOL_DEFINITIONS
    assert TOOL_DEFINITIONS
    assert all("tool_family" not in meta for meta in TOOL_DEFINITIONS.values())
    assert {
        meta["execution_route"] for meta in TOOL_DEFINITIONS.values()
    } <= {"internal_processor", "host_dispatch", "substrate"}
    assert {
        meta["tool_class"] for meta in TOOL_DEFINITIONS.values()
    } <= {"read_tool", "sync_tool", "action_tool"}
    assert not hasattr(protocol_tools, "legacy_receipt_family")


def test_tool_posture_is_attached_only_from_registry_boundary():
    from logic.protocol_tools import attach_registered_tool_metadata

    receipts = [
        {"tool_id": "memory_content_read", "status": "accepted"},
        {"tool_id": "runtime_guard", "tool_class": "runtime_guard"},
    ]
    attach_registered_tool_metadata(receipts)
    assert receipts[0]["tool_class"] == "read_tool"
    assert receipts[0]["execution_route"] == "internal_processor"
    assert "tool_class" not in receipts[1]
    assert "execution_route" not in receipts[1]

    source_root = Path(TESTS_DIR).parent
    allowed = {
        source_root / "logic" / "protocol_tools.py",
        source_root / "engines" / "general_tool_dispatcher.py",
        source_root / "engines" / "protocol_tool_dispatcher.py",
    }
    literals = (
        '"tool_class": "read_tool"',
        '"tool_class": "sync_tool"',
        '"tool_class": "action_tool"',
        '"tool_class": "runtime_guard"',
        '"tool_class": "provider_recovery"',
        '"tool_class": "guide_request"',
    )
    offenders = []
    for path in source_root.rglob("*.py"):
        if "tests" in path.parts or path in allowed:
            continue
        text = path.read_text(encoding="utf-8")
        if any(literal in text for literal in literals):
            offenders.append(str(path.relative_to(source_root)))
    assert offenders == []


def test_resident_list_v1_is_stable_idempotent_and_multi_target(tmp_path):
    from data.resident_list_store import ResidentListStore

    path = tmp_path / "resident_list.json"
    store = ResidentListStore(str(path))

    assert store.reconcile()["status"] == "applied"
    first = store.add({"item_type": "memory", "item_id": "MEM-00000001"})
    duplicate = store.add({"item_type": "memory", "item_id": "MEM-00000001"})
    second = store.add({
        "item_type": "container",
        "item_id": "PRJ-20260827-01",
        "target_file": "plan.md",
    })
    third = store.add({
        "item_type": "container",
        "item_id": "PRJ-20260827-01",
        "target_file": "notes.md",
    })

    document = store.load()
    assert [item["item_id"] for item in document["items"]] == [
        "MEM-00000001", "PRJ-20260827-01", "PRJ-20260827-01",
    ]
    assert first["status"] == "applied"
    assert duplicate["status"] == "noop"
    assert second["revision"] == 2
    assert third["revision"] == 3

    removed = store.remove_matching(
        item_type="container", item_id="PRJ-20260827-01")
    assert removed["status"] == "applied"
    assert len(removed["removed_items"]) == 2


def test_resident_list_upgrades_known_legacy_shapes_in_array_order(tmp_path):
    from data.resident_list_store import ResidentListError, ResidentListStore

    path = tmp_path / "resident_list.json"
    path.write_text('{"items": []}\n', encoding="utf-8")
    store = ResidentListStore(str(path))
    assert store.reconcile()["status"] == "applied"
    assert store.load()["schema_version"] == "resident_list.v1"

    path.write_text(json.dumps({
        "schema_version": "resident_list.v1",
        "revision": 2,
        "next_sequence": 8,
        "items": [
            {
                "sequence": 3,
                "item_type": "memory",
                "item_id": "MEM-00000001",
            },
            {
                "sequence": 7,
                "item_type": "container",
                "item_id": "PRJ-20260828-01",
                "target_file": "plan.md",
            },
        ],
    }, ensure_ascii=False), encoding="utf-8")
    assert store.reconcile()["status"] == "applied"
    assert store.load()["items"] == [
        {"item_type": "memory", "item_id": "MEM-00000001"},
        {
            "item_type": "container",
            "item_id": "PRJ-20260828-01",
            "target_file": "plan.md",
        },
    ]

    # Generic replacement must normalize the known v1 shape, never erase it.
    legacy_document = {
        "schema_version": "resident_list.v1",
        "revision": 3,
        "next_sequence": 9,
        "items": [{
            "sequence": 8,
            "item_type": "relation",
            "item_id": "REL-TzPz",
        }],
    }
    replaced = store.replace(legacy_document)
    assert replaced["items"] == [
        {"item_type": "relation", "item_id": "REL-TzPz"}
    ]

    legacy = {"items": [{"type": "memory", "id": "MEM-00000001"}]}
    raw = json.dumps(legacy, ensure_ascii=False, indent=2) + "\n"
    path.write_text(raw, encoding="utf-8")
    with pytest.raises(ResidentListError, match="shape_unknown"):
        store.reconcile()
    assert path.read_text(encoding="utf-8") == raw


def test_resident_list_rejects_noncanonical_container_target(tmp_path):
    from data.resident_list_store import ResidentListError, ResidentListStore

    store = ResidentListStore(str(tmp_path / "resident_list.json"))
    store.reconcile()
    with pytest.raises(ResidentListError, match="resident_container_target_invalid"):
        store.add({
            "item_type": "container",
            "item_id": "PRJ-20260827-01",
            "target_file": "../plan.md",
        })


def test_resident_list_commits_only_the_exact_preflight_candidate(tmp_path):
    from data.resident_list_store import ResidentListError, ResidentListStore

    store = ResidentListStore(str(tmp_path / "resident_list.json"))
    store.reconcile()
    item = {"item_type": "memory", "item_id": "MEM-00000001"}
    candidate, changed = store.preview_add(item)
    assert changed is True

    tampered = deepcopy(candidate)
    tampered["items"].append(
        {"item_type": "relation", "item_id": "REL-unrelated"}
    )
    tampered["revision"] += 1
    with pytest.raises(ResidentListError, match="resident_candidate_mismatch"):
        store.add(item, candidate=tampered, expected_revision=0)
    assert store.load()["items"] == []

    receipt = store.add(item, candidate=candidate, expected_revision=0)
    assert receipt["status"] == "applied"


def test_resident_list_write_failure_restores_previous_bytes(tmp_path, monkeypatch):
    from data.resident_list_store import ResidentListStore

    path = tmp_path / "resident_list.json"
    store = ResidentListStore(str(path))
    store.reconcile()
    store.add({"item_type": "relation", "item_id": "REL-Codex"})
    before = path.read_bytes()
    original = store._write_verified
    calls = {"count": 0}

    def fail_once(document):
        calls["count"] += 1
        if calls["count"] == 1:
            path.write_text('{"broken": true}', encoding="utf-8")
            raise RuntimeError("injected_resident_write_failure")
        return original(document)

    monkeypatch.setattr(store, "_write_verified", fail_once)
    with pytest.raises(RuntimeError, match="injected_resident_write_failure"):
        store.add({"item_type": "memory", "item_id": "MEM-00000002"})

    assert path.read_bytes() == before
    assert [item["item_id"] for item in store.load()["items"]] == ["REL-Codex"]


def test_combined_resident_source_preflight_uses_one_projection(tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.resident_list_store import ResidentListStore

    store = ResidentListStore(str(tmp_path / "resident_list.json"))
    store.reconcile()
    store.add({"item_type": "memory", "item_id": "MEM-00000003"})
    store.add({
        "item_type": "container",
        "item_id": "DC-1",
        "target_file": "open.md",
    })
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"), resident_store=store)
    monkeypatch.setattr(assembler, "_load_memory_content", lambda _item: "old-memory")
    monkeypatch.setattr(
        assembler, "_load_container_content", lambda _item, _target=None: "old-container")
    monkeypatch.setattr(
        assembler,
        "_build_mounted_content",
        lambda mounts, current_round=None: "|".join(
            str(item.get("content") or "") for item in mounts),
    )

    receipt = assembler.preflight_resident_source_updates({
        ("memory", "MEM-00000003", ""): "new-memory",
        ("container", "DC-1", "open.md"): "new-container",
    }, required_keys={("container", "DC-1", "open.md")})

    assert receipt["resident"] is True
    assert receipt["chars"] == len("new-memory|new-container")
    assert receipt["resident_keys"] == [
        ("container", "DC-1", "open.md"),
        ("memory", "MEM-00000003", ""),
    ]


@pytest.mark.parametrize("target_file", [
    "objectives.md",
    "plans.md",
    "predictions.md",
])
def test_future_container_resident_reads_instance_target_on_next_frame(
        tmp_path, monkeypatch, target_file):
    from assembly.context import ContextAssembler
    from data import container_store as container_store_module
    from data.resident_list_store import ResidentListStore

    roots = {
        prefix: str(tmp_path / prefix)
        for prefix in container_store_module.PREFIX_TO_DIR
    }
    monkeypatch.setattr(container_store_module, "PREFIX_TO_DIR", roots)
    monkeypatch.setattr(
        container_store_module,
        "CONTAINER_REGISTRY_JSON",
        str(tmp_path / "container_registry.json"),
    )
    monkeypatch.setattr(
        container_store_module,
        "LTM_INDEX_MD",
        str(tmp_path / "index.md"),
        raising=False,
    )

    container_store = container_store_module.ContainerStore()
    created = container_store.create_container(
        "FUT", "未来事项核验", target_file=target_file)
    container_store.append_container_content(
        created["container_id"],
        target_file,
        "未来事项核验",
        "2027-01-01 后核验公告价格是否生效。",
    )
    resident_store = ResidentListStore(str(tmp_path / "resident_list.json"))
    resident_store.reconcile()
    resident_store.add({
        "item_type": "container",
        "item_id": created["container_id"],
        "target_file": target_file,
    })
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        resident_store=resident_store,
    )

    mounts = assembler.resident_mount_requests()

    assert len(mounts) == 1
    assert mounts[0]["ids"] == created["container_id"]
    assert mounts[0]["target_file"] == target_file
    assert "2027-01-01 后核验公告价格是否生效" in mounts[0]["content"]
    assert not (
        tmp_path / "FUT" / created["container_id"] / target_file
    ).exists()


def test_future_resident_reference_rejects_mismatched_category_file(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data import container_store as container_store_module
    from data.resident_list_store import ResidentListStore
    from errors import RequiredContextError

    roots = {
        prefix: str(tmp_path / prefix)
        for prefix in container_store_module.PREFIX_TO_DIR
    }
    monkeypatch.setattr(container_store_module, "PREFIX_TO_DIR", roots)
    monkeypatch.setattr(
        container_store_module,
        "CONTAINER_REGISTRY_JSON",
        str(tmp_path / "container_registry.json"),
    )
    monkeypatch.setattr(
        container_store_module,
        "LTM_INDEX_MD",
        str(tmp_path / "index.md"),
        raising=False,
    )

    container_store = container_store_module.ContainerStore()
    created = container_store.create_container(
        "FUT", "未来预测", target_file="predictions.md")
    resident_store = ResidentListStore(str(tmp_path / "resident_list.json"))
    resident_store.reconcile()
    resident_store.add({
        "item_type": "container",
        "item_id": created["container_id"],
        "target_file": "objectives.md",
    })
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"),
        resident_store=resident_store,
    )

    with pytest.raises(RequiredContextError) as error:
        assembler.resident_mount_requests()
    assert error.value.stage == "read"
    assert error.value.scope == "resident_list"


def test_resident_relation_body_limit_is_global_across_rounds(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.resident_list_store import ResidentListStore

    store = ResidentListStore(str(tmp_path / "resident_list.json"))
    store.reconcile()
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"), resident_store=store)
    monkeypatch.setattr(
        assembler,
        "_load_relation_content",
        lambda card_id: f"relation body {card_id}",
    )
    for index in range(1, 4):
        item = {"item_type": "relation", "item_id": f"REL-{index}"}
        candidate = assembler.preflight_resident_add(item)
        store.add(
            item,
            candidate=candidate["document"],
            expected_revision=candidate["expected_revision"],
        )

    with pytest.raises(ValueError, match="relation_body_limit_exceeded"):
        assembler.preflight_resident_add({
            "item_type": "relation",
            "item_id": "REL-4",
        })

    store.add({"item_type": "relation", "item_id": "REL-4"})
    with pytest.raises(ValueError, match="relation_body_limit_exceeded"):
        assembler.resident_mount_requests()


def test_resident_relation_body_never_exposes_substrate_axes(
        tmp_path, monkeypatch):
    from assembly.context_mounts import load_relation_content
    from data import relation_store as relation_store_module

    relation_root = tmp_path / "relation"
    monkeypatch.setattr(
        relation_store_module, "RELATION_DIR", str(relation_root))
    monkeypatch.setattr(
        relation_store_module,
        "RELATION_REGISTRY_JSON",
        str(relation_root / "relation_registry.json"),
    )
    store = relation_store_module.RelationStore()
    store.create_card("REL-A", "阿廖沙", "ours")

    projected = load_relation_content("REL-A")
    assert projected == "阿廖沙"
    assert "关系六轴" not in projected
    assert "信任" not in projected

    store.add_note("REL-A", "共同完成了第一次验证。")
    projected = load_relation_content("REL-A")
    assert projected.startswith("阿廖沙\n")
    assert projected.endswith("共同完成了第一次验证。")
    assert "关系六轴" not in projected


def test_resident_projection_exact_boundary_matches_reaction_render(
        tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from constants import RESIDENT_LIST_CHAR_LIMIT
    from data.resident_list_store import ResidentListStore

    store = ResidentListStore(str(tmp_path / "resident_list.json"))
    store.reconcile()
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"), resident_store=store)
    item = {"item_type": "relation", "item_id": "REL-Boundary"}
    key = ("relation", "REL-Boundary", "")
    monkeypatch.setattr(
        assembler, "_load_relation_content", lambda _item: "placeholder")

    one_char = assembler.preflight_resident_add(
        item, content_overrides={key: "x"})["chars"]
    overhead = one_char - 1
    # Keep the first probe below the limit because the rendered total_chars
    # header itself grows from one to five digits for a large body.
    probe_body = "x" * (RESIDENT_LIST_CHAR_LIMIT - overhead - 16)
    probe_chars = assembler.preflight_resident_add(
        item, content_overrides={key: probe_body})["chars"]
    exact_body = probe_body + "x" * (
        RESIDENT_LIST_CHAR_LIMIT - probe_chars)
    exact = assembler.preflight_resident_add(
        item, content_overrides={key: exact_body})
    assert exact["chars"] == RESIDENT_LIST_CHAR_LIMIT

    store.add(
        item,
        candidate=exact["document"],
        expected_revision=exact["expected_revision"],
    )
    mounts = assembler._resident_mount_requests(
        content_overrides={key: exact_body})
    assert len(assembler._build_mounted_content(
        mounts, current_round=781)) == RESIDENT_LIST_CHAR_LIMIT
    assert "当前可见轮次" not in assembler._build_mounted_content(
        mounts, current_round=781)

    with pytest.raises(ValueError, match="resident_list_char_limit_exceeded"):
        assembler.preflight_resident_source_update(
            item, exact_body + "x", require_resident=True)


def test_resident_memory_projection_is_not_applied_twice(tmp_path, monkeypatch):
    from assembly.context import ContextAssembler
    from data.resident_list_store import ResidentListStore

    store = ResidentListStore(str(tmp_path / "resident_list.json"))
    store.reconcile()
    store.add({"item_type": "memory", "item_id": "MEM-00000004"})
    assembler = ContextAssembler(
        context_dir=str(tmp_path / "context"), resident_store=store)
    projected = "**创建轮次**：meta/R000004\n**内容**：唯一正文"
    monkeypatch.setattr(
        assembler, "_load_memory_content", lambda _item: projected)
    monkeypatch.setattr(
        assembler, "_memory_mount_meta", lambda _item: {
            "created_round": 4,
            "created_instance_id": "meta",
        })

    rendered = assembler._build_mounted_content(
        assembler._resident_mount_requests(), current_round=781)
    assert rendered.count("**创建轮次**") == 1
    assert rendered.count("**内容**：唯一正文") == 1


class _StateStore:
    def __init__(self):
        self.value = {
            "base": {
                "focus": "DC-1",
                "old_focus": "EC-1",
                "dynamic_axes": {"focus": {"value": 7}},
            }
        }

    def load(self):
        return deepcopy(self.value)

    def save(self, value):
        self.value = deepcopy(value)


class _Workbench:
    def __init__(self):
        self.value = {"base": {"focus": "DC-1", "old_focus": "EC-1"}}

    def load_status(self):
        return deepcopy(self.value)

    def save_status(self, value):
        self.value = deepcopy(value)


class _RelationStore:
    def __init__(self):
        self.value = {"cards": [{
            "id": "REL-Codex",
            "summary_resident": True,
            "body_resident": True,
        }, {
            "id": "REL-TzPz",
            "summary_resident": False,
            "body_resident": False,
        }]}

    def load_registry(self):
        return deepcopy(self.value)

    def save_registry(self, value):
        self.value = deepcopy(value)


class _ContainerStore:
    def __init__(self, *, fail=False):
        self.value = {"focus": True, "semantic": {"focus": "keep"}}
        self.fail = fail

    def snapshot_mutation_files(self):
        return deepcopy(self.value)

    def has_retired_focus_fields(self, *, lightweight=False):
        return "focus" in self.value

    def restore_mutation_files(self, value):
        self.value = deepcopy(value)

    def retire_focus_fields(self, snapshot=None):
        self.value.pop("focus", None)
        if self.fail:
            raise RuntimeError("injected_container_migration_failure")
        return {"changed_paths": ["meta.json"]}


class _MigrationAssembler:
    @staticmethod
    def resident_projection_chars(document):
        return 100 * len(document.get("items") or [])


def _migration_fixture(tmp_path, *, fail_container=False):
    from data.resident_list_store import ResidentListStore

    resident = ResidentListStore(str(tmp_path / "resident_list.json"))
    resident.reconcile()
    return (
        _StateStore(),
        _Workbench(),
        _RelationStore(),
        _ContainerStore(fail=fail_container),
        resident,
    )


def test_focus_retirement_migrates_known_fields_and_preserves_dynamic_axis(tmp_path):
    from logic.focus_retirement_migration import migrate_focus_retirement

    state, workbench, relations, containers, resident = _migration_fixture(tmp_path)
    receipt = migrate_focus_retirement(
        state_store=state,
        workbench=workbench,
        container_store=containers,
        relation_store=relations,
        resident_store=resident,
        assembler=_MigrationAssembler(),
    )

    assert receipt["status"] == "applied"
    assert set(state.value["base"]) == {"dynamic_axes"}
    assert state.value["base"]["dynamic_axes"]["focus"]["value"] == 7
    assert workbench.value["base"] == {}
    assert relations.value["cards"][0]["summary_resident"] is True
    assert all("body_resident" not in item for item in relations.value["cards"])
    assert resident.load()["items"] == [{
        "item_type": "relation",
        "item_id": "REL-Codex",
    }]
    assert containers.value["semantic"]["focus"] == "keep"


def test_focus_retirement_clean_startup_is_metadata_only(tmp_path, monkeypatch):
    from logic.focus_retirement_migration import migrate_focus_retirement

    state, workbench, relations, containers, resident = _migration_fixture(tmp_path)
    state.value = {"base": {"dynamic_axes": {"focus": {"value": 7}}}}
    workbench.value = {"base": {}}
    relations.value = {"cards": []}
    containers.value = {}
    probes = []
    monkeypatch.setattr(
        containers, "has_retired_focus_fields",
        lambda *, lightweight=False: probes.append(lightweight) or False,
    )
    monkeypatch.setattr(
        containers, "snapshot_mutation_files",
        lambda: pytest.fail("clean startup must not snapshot containers"),
    )
    monkeypatch.setattr(
        _MigrationAssembler, "resident_projection_chars",
        lambda _self, _document: pytest.fail(
            "clean startup must not render resident bodies"),
    )
    receipt = migrate_focus_retirement(
        state_store=state,
        workbench=workbench,
        container_store=containers,
        relation_store=relations,
        resident_store=resident,
        assembler=_MigrationAssembler(),
    )

    assert receipt["status"] == "noop"
    assert receipt["resident_chars"] is None
    assert probes == [True]


def test_focus_retirement_rolls_back_every_store_on_failure(tmp_path):
    from errors import RequiredContextError
    from logic.focus_retirement_migration import migrate_focus_retirement

    state, workbench, relations, containers, resident = _migration_fixture(
        tmp_path, fail_container=True)
    before = (
        deepcopy(state.value),
        deepcopy(workbench.value),
        deepcopy(relations.value),
        deepcopy(containers.value),
        resident.snapshot_bytes(),
    )

    with pytest.raises(RequiredContextError):
        migrate_focus_retirement(
            state_store=state,
            workbench=workbench,
            container_store=containers,
            relation_store=relations,
            resident_store=resident,
            assembler=_MigrationAssembler(),
        )

    assert state.value == before[0]
    assert workbench.value == before[1]
    assert relations.value == before[2]
    assert containers.value == before[3]
    assert resident.snapshot_bytes() == before[4]


def test_focus_retirement_rolls_back_legacy_resident_upgrade_on_failure(tmp_path):
    from data.resident_list_store import ResidentListStore
    from errors import RequiredContextError
    from logic.focus_retirement_migration import migrate_focus_retirement

    resident_path = tmp_path / "resident_list.json"
    resident_path.write_text('{"items": []}\n', encoding="utf-8")
    resident = ResidentListStore(str(resident_path))
    before = resident.snapshot_bytes()

    with pytest.raises(RequiredContextError):
        migrate_focus_retirement(
            state_store=_StateStore(),
            workbench=_Workbench(),
            container_store=_ContainerStore(fail=True),
            relation_store=_RelationStore(),
            resident_store=resident,
            assembler=_MigrationAssembler(),
        )

    assert resident.snapshot_bytes() == before


def test_container_focus_retirement_reuses_mutation_snapshot(
        tmp_path, monkeypatch):
    from data import container_store as cs

    roots = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
    monkeypatch.setattr(cs, "PREFIX_TO_DIR", roots)
    monkeypatch.setattr(
        cs, "CONTAINER_REGISTRY_JSON", str(tmp_path / "container_registry.json"))
    index_path = tmp_path / "index.md"
    monkeypatch.setattr(cs, "LTM_INDEX_MD", str(index_path))
    index_path.write_text("# stale focus projection\n", encoding="utf-8")
    skills_index = tmp_path / "SKL" / "index.md"
    skills_index.parent.mkdir(parents=True, exist_ok=True)
    skills_index.write_text("# stale [focus] projection\n", encoding="utf-8")
    body_path = tmp_path / "PRJ" / "PRJ-1" / "plan.md"
    body_path.parent.mkdir(parents=True, exist_ok=True)
    body_path.write_text("semantic body", encoding="utf-8")
    meta_path = body_path.with_name("meta.json")
    meta_path.write_text('{"focus": true}\n', encoding="utf-8")

    store = cs.ContainerStore()
    assert store.has_retired_focus_fields() is True
    snapshot = store.snapshot_mutation_files()
    assert str(body_path) in snapshot["files"]
    first = store.retire_focus_fields(snapshot=snapshot)
    assert "focus" not in meta_path.read_text(encoding="utf-8")
    assert str(index_path) in first["changed_paths"]
    assert str(skills_index) in first["changed_paths"]
    assert index_path.read_text(encoding="utf-8") == (
        "# LTM 工作容器总索引\n\n（暂无工作容器实例）\n")
    assert skills_index.read_text(encoding="utf-8") == (
        "# 技能索引\n\n（暂无技能容器）\n")

    second = store.retire_focus_fields(snapshot=store.snapshot_mutation_files())
    assert second["changed_paths"] == []


def test_focus_retirement_probe_detects_registry_or_derived_index_tail(
        tmp_path, monkeypatch):
    from data import container_store as cs

    roots = {prefix: str(tmp_path / prefix) for prefix in cs.PREFIX_TO_DIR}
    monkeypatch.setattr(cs, "PREFIX_TO_DIR", roots)
    registry_path = tmp_path / "container_registry.json"
    monkeypatch.setattr(cs, "CONTAINER_REGISTRY_JSON", str(registry_path))
    index_path = tmp_path / "index.md"
    monkeypatch.setattr(cs, "LTM_INDEX_MD", str(index_path))
    store = cs.ContainerStore()

    registry_path.write_text(
        '{"containers":[{"id":"DC-1","focus":false}]}',
        encoding="utf-8",
    )
    assert store.has_retired_focus_fields() is True

    registry_path.write_text('{"containers":[]}', encoding="utf-8")
    index_path.write_text("- DC-1 title (open) [focus] — path\n", encoding="utf-8")
    assert store.has_retired_focus_fields() is True


def test_container_transaction_restore_attempts_every_store():
    from logic.memory_container_tools import _restore_transaction

    calls = []

    class Resident:
        def restore_bytes(self, _snapshot):
            calls.append("resident")
            raise OSError("resident failed")

    class Memory:
        def restore_stm_files(self, _snapshot):
            calls.append("stm")

        def restore_ltm_files(self, _snapshot):
            calls.append("ltm")
            raise RuntimeError("ltm failed")

    class Containers:
        def restore_mutation_files(self, _snapshot):
            calls.append("container")

    with pytest.raises(RuntimeError, match=(
            "resident:OSError,ltm:RuntimeError")):
        _restore_transaction(
            container_store=Containers(),
            container_snapshot={},
            memory_store=Memory(),
            ltm_snapshot={},
            stm_snapshot={},
            resident_store=Resident(),
            resident_snapshot=None,
        )

    assert calls == ["resident", "stm", "ltm", "container"]


def test_relation_context_config_migration_surfaces_rollback_failure(
        tmp_path, monkeypatch):
    from data import config_store as cfs
    from errors import WriteError

    path = tmp_path / "relation.json"
    current = cfs.default_relation_config()
    legacy = deepcopy(current)
    legacy["_version"] = "Base-0.10.0"
    legacy["relation_focus"] = legacy.pop("relation_context")
    path.write_text(
        json.dumps(legacy, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    monkeypatch.setitem(
        cfs._CONFIG_MAP,
        "relation",
        (str(path), cfs.default_relation_config),
    )
    store = cfs.ConfigStore()
    original_read = store._read_json_object
    calls = {"count": 0}

    def mismatched_read(read_path):
        calls["count"] += 1
        if calls["count"] == 1:
            return original_read(read_path)
        return {"readback": "mismatch"}

    monkeypatch.setattr(store, "_read_json_object", mismatched_read)

    def rollback_failed(*_args, **_kwargs):
        raise OSError("injected rollback failure")

    monkeypatch.setattr(cfs, "atomic_write_text", rollback_failed)
    with pytest.raises(
            WriteError, match="relation_migration_rollback_failed:OSError"):
        store.migrate_relation_context_policy()


def test_relation_context_config_migration_restores_on_readback_error(
        tmp_path, monkeypatch):
    from data import config_store as cfs
    from errors import ReadError

    path = tmp_path / "relation.json"
    current = cfs.default_relation_config()
    legacy = deepcopy(current)
    legacy["_version"] = "Base-0.10.0"
    legacy["relation_focus"] = legacy.pop("relation_context")
    original = json.dumps(legacy, ensure_ascii=False, indent=2) + "\n"
    path.write_text(original, encoding="utf-8")
    monkeypatch.setitem(
        cfs._CONFIG_MAP,
        "relation",
        (str(path), cfs.default_relation_config),
    )
    store = cfs.ConfigStore()
    original_read = store._read_json_object
    calls = {"count": 0}

    def fail_readback(read_path):
        calls["count"] += 1
        if calls["count"] == 1:
            return original_read(read_path)
        raise ReadError(read_path, message="injected_readback_failure")

    monkeypatch.setattr(store, "_read_json_object", fail_readback)
    with pytest.raises(ReadError, match="injected_readback_failure"):
        store.migrate_relation_context_policy()

    assert path.read_text(encoding="utf-8") == original


def test_gui_manual_does_not_reintroduce_retired_focus_contract():
    repo_root = Path(TESTS_DIR).parents[2]
    manual_root = repo_root / "UPSP" / "gui" / "manual"
    combined = "\n".join(
        (manual_root / name).read_text(encoding="utf-8")
        for name in ("audit-tools.md", "content-window.md")
    )

    assert "focus tool" not in combined
    assert "工作台焦点" not in combined
    assert "容器焦点" not in combined
    assert "read tool" in combined
    assert "sync tool" in combined
    assert "action tool" in combined
    assert "resident_list" in combined
    assert "instant_list" in combined
