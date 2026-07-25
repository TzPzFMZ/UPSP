import os
import sys


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

    def write_entry(self, mem_id, title, summary, **kwargs):
        self.entries.append((mem_id, title, summary, kwargs))

    def set_meta(self, mem_id, meta):
        self.meta[mem_id] = dict(meta)

    def append_index(self, mem_id, entry_type, weight, title, **kwargs):
        self.index_rows.append((mem_id, entry_type, weight, title, kwargs))

    def get_meta(self, mem_id):
        return dict(self.meta[mem_id])

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


class DummyMemoryIndex:
    def __init__(self):
        self.keywords = []

    def add_stm_keywords(self, mem_id, keywords):
        self.keywords.append((mem_id, list(keywords)))


class DummyHeat:
    def __init__(self):
        self.entries = []

    def set_entry(self, mem_id, entry):
        self.entries.append((mem_id, dict(entry)))


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
        self.focus_calls = []
        self.existing = {"DC-OLD", "DC-NEW", "PRJ-NEW"}

    def append_entry(self, container_id, title, content, file_name="open.md"):
        self.append_entry_calls.append((container_id, title, content, file_name))

    def create_focus_container(
            self, container_type, title, target_file=None,
            anchor_refs=None, round_num=0):
        self.create_calls.append(
            (container_type, title, target_file, list(anchor_refs or []), round_num))
        container_id = "PRJ-NEW" if str(container_type).upper() == "PRJ" else "DC-NEW"
        return {
            "container_id": container_id,
            "container_type": str(container_type or "").upper(),
            "title": title,
            "target_file": target_file,
            "path": f"/fake/{container_id}",
            "link_required": False,
        }

    def append_focus_content(self, container_id, target_file, title, content):
        self.write_calls.append((container_id, target_file, title, content))
        return {
            "path": f"/fake/{container_id}/{target_file}",
            "chars_written": len(str(content).strip()),
        }

    def set_container_focus(self, container_id, focus):
        self.focus_calls.append((container_id, bool(focus)))

    def container_exists(self, container_id):
        return container_id in self.existing

    def resolve_container_type(self, container_id):
        return str(container_id or "").split("-", 1)[0].upper()


class StaleFocusContainerStore(DummyContainerStore):
    def __init__(self):
        super().__init__()
        self.existing = {"DC-NEW", "PRJ-NEW"}

    def set_container_focus(self, container_id, focus):
        if container_id not in self.existing:
            from errors import ContainerNotFoundError
            raise ContainerNotFoundError(f"container missing: {container_id}")
        super().set_container_focus(container_id, focus)


class DummyWorkbenchStore:
    def __init__(self, focus=None):
        self.focus = focus
        self.old_focus = None
        self.mount_calls = []

    def get(self, dotpath, default=None):
        if dotpath == "base.focus":
            return self.focus
        if dotpath == "base.old_focus":
            return self.old_focus
        return default

    def set(self, dotpath, value):
        if dotpath == "base.focus":
            self.focus = value
        elif dotpath == "base.old_focus":
            self.old_focus = value

    def mount_focus(self, container_id):
        if self.focus and self.focus != container_id:
            self.old_focus = self.focus
        self.focus = container_id
        self.mount_calls.append(container_id)

    def unmount_focus(self, container_id=None):
        if self.focus and (container_id is None or self.focus == container_id):
            self.old_focus = self.focus
            self.focus = None

    def restore_focus(self):
        if self.old_focus:
            self.focus = self.old_focus
            self.old_focus = None
        return self.focus


def test_spec243_memory_write_no_longer_directly_links_or_writes_container_stub(
        monkeypatch):
    import logic.memory_write as memory_write_mod

    memory_store = DummyMemoryStore()
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
            "memory_heat": DummyHeat(),
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

    assert receipts[0]["status"] == "rejected"
    assert receipts[0]["reason"] == "retired_use_memory_container_create_or_write"
    assert receipts[1]["status"] == "rejected"
    assert receipts[1]["reason"] == "retired_use_memory_container_create_or_write"
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
    workbench_store = DummyWorkbenchStore(focus="DC-OLD")
    modules = {
        "memory_store": memory_store,
        "container_store": container_store,
        "workbench_store": workbench_store,
        "visible_focus_id": "DC-OLD",
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


def test_spec243_container_focus_create_write_are_retired_focus_hygiene_only():
    from logic.container_focus import apply_container_focus_declarations

    container_store = DummyContainerStore()
    workbench_store = DummyWorkbenchStore(focus="DC-OLD")

    receipts = apply_container_focus_declarations(
        [{
            "action": "create",
            "container_type": "PRJ",
            "title": "旧创建",
            "target_file": "plan.md",
            "content": "旧正文",
            "reason": "旧 create 路径",
        }, {
            "action": "write",
            "container_id": "DC-OLD",
            "target_file": "open.md",
            "content": "旧追加",
            "reason": "旧 write 路径",
        }],
        {
            "container_store": container_store,
            "workbench_store": workbench_store,
        },
        round_num=243,
    )

    assert receipts[0]["status"] == "rejected"
    assert receipts[0]["reason"] == "retired_container_focus_action"
    assert receipts[1]["status"] == "rejected"
    assert receipts[1]["reason"] == "retired_container_focus_action"
    assert container_store.create_calls == []
    assert container_store.write_calls == []
    assert workbench_store.focus == "DC-OLD"


def test_spec243_memory_container_create_writes_body_links_mem_and_replaces_focus():
    from logic.memory_container_tools import apply_memory_container_create_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-243CREATE"] = {
        "id": "MEM-243CREATE",
        "title": "引用源",
        "linked_containers": [],
    }
    container_store = DummyContainerStore()
    workbench_store = DummyWorkbenchStore(focus="DC-OLD")

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
            "workbench_store": workbench_store,
        },
        round_num=243,
    )

    receipt = receipts[0]
    assert receipt["status"] == "applied"
    assert receipt["tool_id"] == "memory_container_create"
    assert receipt["tool_class"] == "focus_tool"
    assert receipt["mem_id"] == "MEM-243CREATE"
    assert receipt["container_id"] == "PRJ-NEW"
    assert receipt["previous_focus"] == "DC-OLD"
    assert receipt["container_body_written"] is True
    assert receipt["memory_link_applied"] is True
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
    assert container_store.focus_calls == [("DC-OLD", False), ("PRJ-NEW", True)]
    assert workbench_store.focus == "PRJ-NEW"


def test_spec323_container_focus_open_clears_stale_current_focus_without_crashing():
    from logic.container_focus import apply_container_focus_declarations

    container_store = StaleFocusContainerStore()
    workbench_store = DummyWorkbenchStore(focus="FUT-predictions-12")

    receipts = apply_container_focus_declarations(
        [{
            "action": "open",
            "container_id": "DC-NEW",
            "reason": "打开存在的新焦点",
        }],
        {
            "container_store": container_store,
            "workbench_store": workbench_store,
        },
        round_num=323,
    )

    receipt = receipts[0]
    assert receipt["status"] == "applied"
    assert receipt["container_id"] == "DC-NEW"
    assert receipt["previous_focus"] == "FUT-predictions-12"
    assert receipt["stale_previous_focus"] == "FUT-predictions-12"
    assert container_store.focus_calls == [("DC-NEW", True)]
    assert workbench_store.focus == "DC-NEW"
    assert workbench_store.old_focus is None


def test_spec323_container_focus_close_clears_stale_current_focus():
    from logic.container_focus import apply_container_focus_declarations

    container_store = StaleFocusContainerStore()
    workbench_store = DummyWorkbenchStore(focus="FUT-predictions-12")

    receipts = apply_container_focus_declarations(
        [{"action": "close"}],
        {
            "container_store": container_store,
            "workbench_store": workbench_store,
        },
        round_num=323,
    )

    receipt = receipts[0]
    assert receipt["status"] == "applied"
    assert receipt["container_id"] == "FUT-predictions-12"
    assert receipt["stale_focus_cleared"] == "FUT-predictions-12"
    assert workbench_store.focus is None
    assert workbench_store.old_focus is None


def test_spec323_container_focus_restore_rejects_stale_old_focus():
    from logic.container_focus import apply_container_focus_declarations

    container_store = StaleFocusContainerStore()
    workbench_store = DummyWorkbenchStore(focus="DC-NEW")
    workbench_store.old_focus = "FUT-predictions-12"

    receipts = apply_container_focus_declarations(
        [{"action": "restore"}],
        {
            "container_store": container_store,
            "workbench_store": workbench_store,
        },
        round_num=323,
    )

    receipt = receipts[0]
    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "container_not_found"
    assert workbench_store.focus == "DC-NEW"
    assert workbench_store.old_focus is None


def test_spec400_container_focus_open_missing_instance_names_create_recovery():
    from logic.container_focus import apply_container_focus_declarations

    container_store = DummyContainerStore()
    workbench_store = DummyWorkbenchStore()

    receipts = apply_container_focus_declarations(
        [{
            "action": "open",
            "container_id": "DC-1",
            "reason": "误把示例当实例",
        }],
        {
            "container_store": container_store,
            "workbench_store": workbench_store,
        },
        round_num=400,
    )

    receipt = receipts[0]
    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "container_not_found"
    assert receipt["recovery_tool"] == "memory_container_create"
    assert "不存在" in receipt["message"]
    assert "不要继续 open 这个 ID" in receipt["message"]
    assert "memory_container_create" in receipt["message"]
    assert workbench_store.focus is None
    assert container_store.focus_calls == []


def test_spec323_memory_container_create_clears_stale_current_focus():
    from logic.memory_container_tools import apply_memory_container_create_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-323CREATE"] = {
        "id": "MEM-323CREATE",
        "title": "引用源",
        "linked_containers": [],
    }
    container_store = StaleFocusContainerStore()
    workbench_store = DummyWorkbenchStore(focus="FUT-predictions-12")

    receipts = apply_memory_container_create_declarations(
        [{
            "mem_id": "MEM-323CREATE",
            "container_type": "PRJ",
            "title": "悬空焦点后的项目",
            "target_file": "plan.md",
            "container_body": "基于 MEM 创建的新项目正文。",
            "current_overview": "{container_id}：用于验证悬空旧焦点降级。",
            "reason": "旧焦点已清理但工作台仍残留",
        }],
        {
            "memory_store": memory_store,
            "container_store": container_store,
            "workbench_store": workbench_store,
        },
        round_num=323,
    )

    receipt = receipts[0]
    assert receipt["status"] == "applied"
    assert receipt["container_id"] == "PRJ-NEW"
    assert receipt["previous_focus"] == "FUT-predictions-12"
    assert receipt["stale_previous_focus"] == "FUT-predictions-12"
    assert container_store.focus_calls == [("PRJ-NEW", True)]
    assert workbench_store.focus == "PRJ-NEW"
    assert workbench_store.old_focus is None


def test_spec250_memory_container_create_rejection_names_allowed_target_files():
    from logic.memory_container_tools import apply_memory_container_create_declarations

    class TargetCheckingContainerStore(DummyContainerStore):
        def create_focus_container(
                self, container_type, title, target_file=None,
                anchor_refs=None, round_num=0):
            if str(container_type).upper() == "DC" and target_file != "open.md":
                raise ValueError("invalid_target_file")
            return super().create_focus_container(
                container_type,
                title,
                target_file=target_file,
                anchor_refs=anchor_refs,
                round_num=round_num,
            )

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-250CREATE"] = {
        "id": "MEM-250CREATE",
        "title": "引用源",
        "linked_containers": [],
    }

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
            "workbench_store": DummyWorkbenchStore(),
        },
        round_num=250,
    )

    assert receipts[0]["status"] == "rejected"
    assert receipts[0]["reason"] == "invalid_target_file"
    assert receipts[0]["allowed_target_files"] == ["open.md"]
    assert receipts[0]["container_type"] == "DC"
    assert receipts[0]["target_file"] == "notes.md"


def test_spec243_memory_container_write_requires_entry_visible_focus_then_links():
    from logic.memory_container_tools import apply_memory_container_write_declarations

    memory_store = DummyMemoryStore()
    memory_store.meta["MEM-243WRITE"] = {
        "id": "MEM-243WRITE",
        "title": "引用源",
        "linked_containers": [],
    }
    container_store = DummyContainerStore()
    workbench_store = DummyWorkbenchStore(focus="DC-NEW")
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
            "workbench_store": workbench_store,
            "visible_focus_id": "DC-OLD",
        },
        round_num=243,
    )
    assert rejected[0]["status"] == "rejected"
    assert rejected[0]["reason"] == "focus_not_visible_at_iteration_start"
    assert container_store.write_calls == []

    applied = apply_memory_container_write_declarations(
        [declaration],
        {
            "memory_store": memory_store,
            "container_store": container_store,
            "workbench_store": workbench_store,
            "visible_focus_id": "DC-NEW",
        },
        round_num=244,
    )

    receipt = applied[0]
    assert receipt["status"] == "applied"
    assert receipt["tool_id"] == "memory_container_write"
    assert receipt["container_body_written"] is True
    assert receipt["memory_link_applied"] is True
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
