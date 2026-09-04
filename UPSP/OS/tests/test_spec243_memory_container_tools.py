import os
import sys
from copy import deepcopy


TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


class DummyMemoryStore:
    def __init__(self):
        self.entries = []
        self.meta = {}
        self.private_owners = {}
        self.index_rows = []
        self.link_calls = []
        self.ltm = {}
        self.stm_bodies = {}
        self.heat_entries = {}

    def write_entry(self, mem_id, title, summary, **kwargs):
        self.entries.append((mem_id, title, summary, kwargs))

    def set_meta(self, mem_id, meta):
        self.meta[mem_id] = dict(meta)

    def append_index(self, mem_id, entry_type, weight, title, **kwargs):
        self.index_rows.append((mem_id, entry_type, weight, title, kwargs))

    def get_meta(self, mem_id):
        return dict(self.meta[mem_id])

    def render_entry(self, mem_id, title, summary="", **kwargs):
        self.entries.append((mem_id, title, summary, kwargs))
        return f"## {mem_id} {title}\n**标题**：{title}\n**正文**：{summary}"

    def snapshot_stm_files(self):
        return deepcopy((self.stm_bodies, self.meta, self.heat_entries))

    def snapshot_ltm_files(self):
        return deepcopy(self.ltm)

    def restore_stm_files(self, snapshot):
        self.stm_bodies, self.meta, self.heat_entries = deepcopy(snapshot)

    def restore_ltm_files(self, snapshot):
        self.ltm = deepcopy(snapshot)

    def store_ltm_entry(self, tier, mem_id, body, meta):
        self.ltm[mem_id] = {
            "tier": tier, "body": body, "meta": deepcopy(meta),
        }

    def replace_stm_body(self, mem_id, body):
        self.stm_bodies[mem_id] = body

    def replace_stm_meta(self, mem_id, meta):
        self.meta[mem_id] = deepcopy(meta)

    def rebuild_stm_index(self):
        return None

    def rebuild_stm_keywords(self):
        return None

    def ltm_entry_state(self, mem_id, *, include_backup=True):
        return deepcopy(self.ltm.get(mem_id))

    def stm_entry_state(self, mem_id):
        return {
            "body": self.stm_bodies.get(mem_id),
            "meta": deepcopy(self.meta.get(mem_id)),
            "heat": deepcopy(self.heat_entries.get(mem_id)),
        }

    def private_subjects_for_memory(self, mem_id):
        return list(self.private_owners.get(mem_id, []))

    def update_linked_containers(
            self, mem_id, operation, refs, current_overview=None):
        self.link_calls.append((mem_id, operation, list(refs), current_overview))
        entry = dict(self.meta.get(mem_id) or {"id": mem_id, "title": mem_id})
        current = list(entry.get("linked_containers") or [])
        if operation == "set":
            linked = list(refs)
        elif operation == "remove":
            linked = [ref for ref in current if ref not in refs]
        else:
            linked = current + [ref for ref in refs if ref not in current]
        entry["linked_containers"] = linked
        if current_overview is not None:
            entry["current_overview"] = current_overview
        self.meta[mem_id] = entry
        return dict(entry)

    def read_body_by_id(self, mem_id):
        meta = dict(self.meta[mem_id])
        return {
            "mem_id": mem_id,
            "memory_layer": "LTM/Summary",
            "meta": meta,
            "body": (
                f"## {mem_id} {meta.get('title') or mem_id}\n"
                f"**正文**：{meta.get('title') or mem_id}"
            ),
        }


class DummyMemoryIndex:
    def __init__(self):
        self.keywords = []

    def add_stm_keywords(self, mem_id, keywords):
        self.keywords.append((mem_id, list(keywords)))


class DummyHeat:
    def __init__(self, memory_store=None):
        self.entries = []
        self.memory_store = memory_store

    def set_entry(self, mem_id, entry):
        self.entries.append((mem_id, dict(entry)))
        if self.memory_store is not None:
            self.memory_store.heat_entries[mem_id] = dict(entry)

    @staticmethod
    def new_entry(weight=2):
        from schemas.memory import default_heat_entry
        return default_heat_entry(weight=weight)


class DummyRelationStore:
    def resolve_active_subject(self, value):
        return {
            "FMZ": "FMZ",
            "Codex": "Codex",
            "TzPz": "TzPz",
            "伙伴": "TzPz",
        }.get(str(value))


class DummyContainerStore:
    def __init__(self):
        self.append_entry_calls = []
        self.create_calls = []
        self.write_calls = []
        self.existing = {"DC-OLD", "DC-NEW", "PRJ-NEW"}
        self.bodies = {}

    def append_entry(self, container_id, title, content, file_name="open.md"):
        self.append_entry_calls.append((container_id, title, content, file_name))

    def create_container(
            self, container_type, title, target_file=None,
            anchor_refs=None, round_num=0, **kwargs):
        self.create_calls.append(
            (container_type, title, target_file, list(anchor_refs or []), round_num))
        container_id = "PRJ-NEW" if str(container_type).upper() == "PRJ" else "DC-NEW"
        self.existing.add(container_id)
        return {
            "status": "applied",
            "container_id": container_id,
            "container_type": str(container_type or "").upper(),
            "title": title,
            "target_file": target_file,
            "path": f"/fake/{container_id}",
            "link_required": False,
        }

    def append_container_content(
            self, container_id, target_file, title, content, **kwargs):
        self.write_calls.append((container_id, target_file, title, content))
        key = (container_id, target_file)
        self.bodies[key] = self.bodies.get(key, "") + str(content)
        return {
            "path": f"/fake/{container_id}/{target_file}",
            "chars_written": len(str(content).strip()),
        }

    def read_container_content(self, container_id, target_file=None):
        return {
            "container_id": container_id,
            "target_file": target_file,
            "content": self.bodies.get((container_id, target_file), ""),
        }

    def snapshot_mutation_files(self):
        return deepcopy((self.existing, self.bodies, self.create_calls, self.write_calls))

    def restore_mutation_files(self, snapshot):
        self.existing, self.bodies, self.create_calls, self.write_calls = deepcopy(snapshot)

    def container_exists(self, container_id):
        return container_id in self.existing

    def resolve_container_type(self, container_id):
        return str(container_id or "").split("-", 1)[0].upper()


class DummyResidentStore:
    def __init__(self):
        self.items = []
        self.revision = 0

    def snapshot_bytes(self):
        return deepcopy((self.items, self.revision))

    def restore_bytes(self, snapshot):
        self.items, self.revision = deepcopy(snapshot)

    def contains(self, *, item_type, item_id, target_file=None):
        return any(
            item == (item_type, item_id, target_file or "")
            for item in self.items
        )

    def add(self, item, *, candidate=None, expected_revision=None):
        item_type = item["item_type"]
        item_id = item["item_id"]
        target_file = item.get("target_file")
        key = (item_type, item_id, target_file or "")
        if key not in self.items:
            self.items.append(key)
            self.revision += 1
        return {"status": "applied", "revision": self.revision}

    def load(self):
        return {"revision": self.revision}


class DummyAssembler:
    def __init__(self):
        self.resident_store = DummyResidentStore()
        self.persist_overrides = []
        self.preflight_overrides = []

    def preflight_resident_add(self, item, content_overrides=None):
        self.persist_overrides.append(dict(content_overrides or {}))
        return {
            "document": {"items": [item]},
            "changed": True,
            "chars": sum(len(str(value or "")) for value in
                         (content_overrides or {}).values()),
            "expected_revision": self.resident_store.revision,
        }

    def preflight_resident_source_update(self, item, body, require_resident=True):
        assert self.resident_store.contains(
            item_type=item["item_type"],
            item_id=item["item_id"],
            target_file=item.get("target_file"),
        ) is require_resident
        return {"chars": len(body)}

    def preflight_resident_source_updates(
            self, content_overrides, *, required_keys=None):
        self.preflight_overrides.append(dict(content_overrides or {}))
        required_keys = set(required_keys or ())
        for item_type, item_id, target_file in required_keys:
            assert self.resident_store.contains(
                item_type=item_type,
                item_id=item_id,
                target_file=target_file,
            )
        return {
            "chars": sum(len(str(value or "")) for value in content_overrides.values())
        }


def test_spec243_memory_write_no_longer_directly_links_or_writes_container_stub(
        monkeypatch):
    import logic.memory_write as memory_write_mod

    memory_store = DummyMemoryStore()
    memory_heat = DummyHeat(memory_store)
    container_store = DummyContainerStore()
    monkeypatch.setattr(memory_write_mod, "generate_mem_id", lambda: "MEM-243WRITE")

    receipts = memory_write_mod.apply_memory_write_declarations(
        [{
            "title": "引用源",
            "body": "本轮形成独立记忆，容器挂接留给后续焦点工具。",
            "weight": 3,
            "subject": "TzPz",
            "candidate_keywords": ["引用式挂接"],
            "linked_containers": ["DC-OLD"],
        }],
        state={},
        round_num=243,
        data_modules={
            "memory_store": memory_store,
            "memory_index": DummyMemoryIndex(),
            "memory_heat": memory_heat,
            "container_store": container_store,
            "relation_store": DummyRelationStore(),
        },
    )

    assert receipts[0]["status"] == "applied"
    assert memory_store.entries[0][3]["linked_containers"] == []
    assert memory_store.meta["MEM-243WRITE"]["linked_containers"] == []
    assert container_store.append_entry_calls == []


def test_spec243_memory_link_update_add_set_are_retired_but_remove_still_applies():
    from logic.memory_link_update import apply_memory_link_update_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-243LINK"] = {
        "id": "MEM-243LINK",
        "title": "旧挂接",
        "linked_containers": ["DC-OLD"],
    }

    receipts = apply_memory_link_update_declarations(
        [{
            "mem_id": "MEM-243LINK",
            "operation": "add",
            "container_refs": ["DC-NEW"],
            "current_overview": "DC-NEW：引用式补写。",
            "reason": "旧 add 路径",
        }, {
            "mem_id": "MEM-243LINK",
            "operation": "set",
            "container_refs": ["DC-NEW"],
            "current_overview": "DC-NEW：引用式重设。",
            "reason": "旧 set 路径",
        }, {
            "mem_id": "MEM-243LINK",
            "operation": "remove",
            "container_refs": ["DC-OLD"],
            "reason": "历史修复移除",
        }],
        data_modules={"memory_store": memory_store},
    )

    assert receipts[0]["status"] == "error"
    assert receipts[0]["reason"] == "invalid_operation"
    assert receipts[1]["status"] == "error"
    assert receipts[1]["reason"] == "invalid_operation"
    assert receipts[2]["status"] == "applied"
    assert memory_store.link_calls == [("MEM-243LINK", "remove", ["DC-OLD"], None)]


def test_private_memory_container_and_link_side_paths_require_owner_presence():
    from logic.memory_container_tools import (
        apply_memory_container_create_declarations,
        apply_memory_container_write_declarations,
    )
    from logic.memory_link_update import apply_memory_link_update_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-PRIVATE"] = {
        "id": "MEM-PRIVATE",
        "title": "私密主体",
        "access": "private",
        "subject": "FMZ",
        "linked_containers": ["DC-OLD"],
    }
    memory_store.private_owners["MEM-PRIVATE"] = ["TzPz"]
    relation_store = DummyRelationStore()
    container_store = DummyContainerStore()
    assembler = DummyAssembler()
    modules = {
        "memory_store": memory_store,
        "container_store": container_store,
        "assembler": assembler,
        "resident_store": assembler.resident_store,
        "visible_container_targets": {("DC-OLD", "open.md")},
        "relation_store": relation_store,
    }
    hidden_state = {"presence": {"confirmed_subjects": ["Codex"]}}
    create = apply_memory_container_create_declarations([{
        "mem_id": "MEM-PRIVATE",
        "container_type": "PRJ",
        "title": "私密挂接",
        "target_file": "plan.md",
        "container_body": "不应写入",
        "current_overview": "私密挂接",
        "reason": "test",
    }], modules, state=hidden_state)
    write = apply_memory_container_write_declarations([{
        "mem_id": "MEM-PRIVATE",
        "container_id": "DC-OLD",
        "title": "私密挂接",
        "target_file": "open.md",
        "container_body": "不应写入",
        "current_overview": "DC-OLD：私密挂接",
        "reason": "test",
    }], modules, state=hidden_state)
    unlink = apply_memory_link_update_declarations([{
        "mem_id": "MEM-PRIVATE",
        "operation": "remove",
        "container_refs": ["DC-OLD"],
        "reason": "test",
    }], modules, state=hidden_state)

    assert [create[0]["reason"], write[0]["reason"], unlink[0]["reason"]] == [
        "private_memory_not_visible",
        "private_memory_not_visible",
        "private_memory_not_visible",
    ]
    assert container_store.create_calls == []
    assert container_store.write_calls == []
    assert memory_store.link_calls == []

    still_hidden = apply_memory_link_update_declarations([{
        "mem_id": "MEM-PRIVATE",
        "operation": "remove",
        "container_refs": ["DC-OLD"],
        "reason": "test",
    }], modules, state={"presence": {"confirmed_subjects": ["伙伴"]}})
    assert still_hidden[0]["status"] == "private_memory_not_visible"
    assert memory_store.link_calls == []


def test_spec781_container_focus_has_no_active_processor_or_metadata():
    from logic import protocol_tools

    assert "container_focus" not in protocol_tools.TOOL_DEFINITIONS
    assert protocol_tools.tool_metadata_for("container_focus") == {}


def test_spec781_memory_container_create_writes_body_links_mem_and_resident():
    from logic.memory_container_tools import apply_memory_container_create_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-243CREATE"] = {
        "id": "MEM-243CREATE",
        "title": "引用源",
        "linked_containers": [],
    }
    container_store = DummyContainerStore()
    assembler = DummyAssembler()

    receipts = apply_memory_container_create_declarations(
        [{
            "mem_id": "MEM-243CREATE",
            "container_type": "PRJ",
            "title": "引用式项目",
            "target_file": "plan.md",
            "container_body": "这是一段基于 MEM 的项目首段正文。",
            "current_overview": "{container_id}：作为项目首段被引用组织。",
            "reason": "形成项目容器",
        }],
        {
            "memory_store": memory_store,
            "container_store": container_store,
            "assembler": assembler,
            "resident_store": assembler.resident_store,
        },
        round_num=243,
    )

    receipt = receipts[0]
    assert receipt["status"] == "applied"
    assert receipt["tool_id"] == "memory_container_create"
    assert "tool_class" not in receipt
    assert receipt["mem_id"] == "MEM-243CREATE"
    assert receipt["container_id"] == "PRJ-NEW"
    assert "previous_focus" not in receipt
    assert receipt["container_body_written"] is True
    assert receipt["memory_link_applied"] is True
    assert receipt["visibility_verified"] is False
    assert receipt["resident_persisted"] is True
    assert set(assembler.persist_overrides[-1]) == {
        ("container", "PRJ-NEW", "plan.md"),
        ("memory", "MEM-243CREATE", ""),
    }
    assert container_store.create_calls[0][3] == ["MEM-243CREATE"]
    assert container_store.write_calls == [(
        "PRJ-NEW",
        "plan.md",
        "引用式项目",
        "这是一段基于 MEM 的项目首段正文。",
    )]
    assert memory_store.link_calls == [(
        "MEM-243CREATE",
        "add",
        ["PRJ-NEW"],
        "PRJ-NEW：作为项目首段被引用组织。",
    )]
    assert assembler.resident_store.contains(
        item_type="container", item_id="PRJ-NEW", target_file="plan.md")


def test_spec250_memory_container_create_rejection_names_allowed_target_files():
    from logic.memory_container_tools import apply_memory_container_create_declarations

    class TargetCheckingContainerStore(DummyContainerStore):
        def create_container(
                self, container_type, title, target_file=None,
                anchor_refs=None, round_num=0, **kwargs):
            if str(container_type).upper() == "DC" and target_file != "open.md":
                raise ValueError("invalid_target_file")
            return super().create_container(
                container_type,
                title,
                target_file=target_file,
                anchor_refs=anchor_refs,
                round_num=round_num,
                **kwargs,
            )

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-250CREATE"] = {
        "id": "MEM-250CREATE",
        "title": "引用源",
        "linked_containers": [],
    }

    assembler = DummyAssembler()
    receipts = apply_memory_container_create_declarations(
        [{
            "mem_id": "MEM-250CREATE",
            "container_type": "DC",
            "title": "引用式辩证链",
            "target_file": "notes.md",
            "container_body": "这是一段基于 MEM 的辩证链首段正文。",
            "current_overview": "{container_id}：作为辩证链首段被引用组织。",
            "reason": "形成辩证链容器",
        }],
        {
            "memory_store": memory_store,
            "container_store": TargetCheckingContainerStore(),
            "assembler": assembler,
            "resident_store": assembler.resident_store,
        },
        round_num=250,
    )

    assert receipts[0]["status"] == "rejected"
    assert receipts[0]["reason"] == "invalid_target_file"
    assert receipts[0]["allowed_target_files"] == ["open.md"]
    assert receipts[0]["container_type"] == "DC"
    assert receipts[0]["target_file"] == "notes.md"


def test_spec781_memory_container_write_requires_frame_visible_target_then_links():
    from logic.memory_container_tools import apply_memory_container_write_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-243WRITE"] = {
        "id": "MEM-243WRITE",
        "title": "引用源",
        "linked_containers": [],
    }
    container_store = DummyContainerStore()
    assembler = DummyAssembler()
    declaration = {
        "mem_id": "MEM-243WRITE",
        "container_id": "DC-NEW",
        "target_file": "open.md",
        "title": "引用式续写",
        "container_body": "这是看到焦点投影后的续写正文。",
        "current_overview": "DC-NEW：已作为续写段落引用组织。",
        "reason": "已有焦点容器续写",
    }

    rejected = apply_memory_container_write_declarations(
        [declaration],
        {
            "memory_store": memory_store,
            "container_store": container_store,
            "assembler": assembler,
            "resident_store": assembler.resident_store,
            "visible_container_targets": {("DC-OLD", "open.md")},
        },
        round_num=243,
    )
    assert rejected[0]["status"] == "rejected"
    assert rejected[0]["reason"] == "container_not_visible_at_frame_start"
    assert container_store.write_calls == []

    assembler.resident_store.add({
        "item_type": "container",
        "item_id": "DC-NEW",
        "target_file": "open.md",
    })
    applied = apply_memory_container_write_declarations(
        [declaration],
        {
            "memory_store": memory_store,
            "container_store": container_store,
            "assembler": assembler,
            "resident_store": assembler.resident_store,
            "visible_container_targets": {("DC-NEW", "open.md")},
        },
        round_num=244,
    )

    receipt = applied[0]
    assert receipt["status"] == "applied"
    assert receipt["tool_id"] == "memory_container_write"
    assert receipt["container_body_written"] is True
    assert receipt["memory_link_applied"] is True
    assert receipt["visibility_verified"] is True
    assert receipt["resident_persisted"] is True
    assert set(assembler.preflight_overrides[-1]) == {
        ("container", "DC-NEW", "open.md"),
        ("memory", "MEM-243WRITE", ""),
    }
    assert container_store.write_calls == [(
        "DC-NEW",
        "open.md",
        "引用式续写",
        "这是看到焦点投影后的续写正文。",
    )]
    assert memory_store.link_calls == [(
        "MEM-243WRITE",
        "add",
        ["DC-NEW"],
        "DC-NEW：已作为续写段落引用组织。",
    )]


def test_spec781_memory_container_write_allows_visible_nonresident_target():
    from logic.memory_container_tools import apply_memory_container_write_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-243INSTANT"] = {
        "id": "MEM-243INSTANT",
        "title": "即时引用源",
        "linked_containers": [],
    }
    container_store = DummyContainerStore()
    assembler = DummyAssembler()

    receipts = apply_memory_container_write_declarations(
        [{
            "mem_id": "MEM-243INSTANT",
            "container_id": "DC-NEW",
            "target_file": "open.md",
            "title": "即时可见续写",
            "container_body": "目标由本轮 CONTENT 提供，但不在跨轮清单中。",
            "current_overview": "DC-NEW：即时可见续写。",
            "reason": "验证写权来自 Frame 可见性而非常驻状态。",
        }],
        {
            "memory_store": memory_store,
            "container_store": container_store,
            "assembler": assembler,
            "resident_store": assembler.resident_store,
            "visible_container_targets": {("DC-NEW", "open.md")},
        },
        round_num=245,
    )

    assert receipts[0]["status"] == "applied"
    assert receipts[0]["visibility_verified"] is True
    assert receipts[0]["resident_persisted"] is False
    assert assembler.resident_store.items == []


def test_spec243_obligations_close_memory_route_only_after_container_reference_write():
    from logic.reaction_obligations import ReactionObligationTracker

    tracker = ReactionObligationTracker()
    tracker.observe_receipts([{
        "tool_id": "memory_write",
        "status": "applied",
        "mem_id": "MEM-243OBL",
    }])
    assert tracker.pending_types() == ["memory_route_pending"]

    tracker.observe_receipts([{
        "tool_id": "container_focus",
        "status": "applied",
        "action": "open",
        "container_type": "DC",
        "container_id": "DC-NEW",
    }])
    assert tracker.pending_types() == ["memory_route_pending"]

    tracker.observe_receipts([{
        "tool_id": "memory_container_write",
        "status": "applied",
        "mem_id": "MEM-243OBL",
        "container_type": "DC",
        "container_id": "DC-NEW",
    }])
    assert tracker.pending_types() == ["future_jump_pending"]
