import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


class DummyWorkbench:
    def __init__(self, focus="PRJ-305"):
        self.focus = focus
        self.unmounted = []

    def get(self, dotpath, default=None):
        if dotpath == "base.focus":
            return self.focus
        return default

    def unmount_focus(self, container_id=None):
        if self.focus and (container_id is None or container_id == self.focus):
            self.unmounted.append(self.focus)
            self.focus = None


class DummyContainerStore:
    def __init__(self):
        self.focus_flags = {}

    def set_container_focus(self, container_id, focus):
        self.focus_flags[container_id] = bool(focus)

    def resolve_container_type(self, container_id):
        return str(container_id).split("-", 1)[0]


class DummyRelationStore:
    def __init__(self):
        self.summary = {}
        self.body = {}

    def set_summary_resident(self, card_id, enabled=True):
        self.summary[card_id] = bool(enabled)

    def set_body_resident(self, card_id, enabled=True):
        self.body[card_id] = bool(enabled)


def test_spec305_mount_cancel_clears_focus_without_deleting_container():
    from logic.mount_cancel import apply_mount_cancel_requests

    workbench = DummyWorkbench(focus="PRJ-305")
    container_store = DummyContainerStore()

    receipts, mount_ids = apply_mount_cancel_requests(
        [{
            "tool_id": "mount_cancel",
            "mount_area": "focus",
            "item_type": "container",
            "item_id": "PRJ-305",
            "reason": "no longer editing",
        }],
        modules={
            "workbench_store": workbench,
            "container_store": container_store,
        },
        mount_ids=[{"type": "memory", "ids": "MEM-KEEP", "mode": "temporary"}],
    )

    assert receipts[0]["status"] == "applied"
    assert receipts[0]["mount_area"] == "focus"
    assert receipts[0]["item_id"] == "PRJ-305"
    assert receipts[0]["removed"] is True
    assert receipts[0]["protocol_tool_receipt"] is True
    assert workbench.focus is None
    assert workbench.unmounted == ["PRJ-305"]
    assert container_store.focus_flags == {"PRJ-305": False}
    assert mount_ids == [{"type": "memory", "ids": "MEM-KEEP", "mode": "temporary"}]


def test_spec305_mount_cancel_removes_instant_memory_mount_only():
    from logic.mount_cancel import apply_mount_cancel_requests

    receipts, mount_ids = apply_mount_cancel_requests(
        [{
            "tool_id": "mount_cancel",
            "mount_area": "instant_list",
            "item_type": "memory",
            "item_id": "MEM-DROP",
        }],
        modules={},
        mount_ids=[
            {"type": "memory", "ids": "MEM-DROP, MEM-KEEP", "mode": "temporary"},
            {"type": "relation", "ids": "REL-KEEP", "mode": "temporary"},
        ],
    )

    assert receipts[0]["status"] == "applied"
    assert receipts[0]["removed"] is True
    assert mount_ids == [
        {"type": "memory", "ids": "MEM-KEEP", "mode": "temporary"},
        {"type": "relation", "ids": "REL-KEEP", "mode": "temporary"},
    ]


def test_spec305_mount_cancel_clears_resident_relation_flags():
    from logic.mount_cancel import apply_mount_cancel_requests

    relation_store = DummyRelationStore()

    receipts, mount_ids = apply_mount_cancel_requests(
        [
            {
                "tool_id": "mount_cancel",
                "mount_area": "resident_list",
                "item_type": "relation",
                "item_id": "REL-305",
            },
            {
                "tool_id": "mount_cancel",
                "mount_area": "resident_list",
                "item_type": "relation_summary",
                "item_id": "REL-305",
            },
        ],
        modules={"relation_store": relation_store},
        mount_ids=[],
    )

    assert [receipt["status"] for receipt in receipts] == ["applied", "applied"]
    assert relation_store.body == {"REL-305": False}
    assert relation_store.summary == {"REL-305": False}
    assert mount_ids == []


def test_spec305_mount_cancel_reports_not_found_without_side_effects():
    from logic.mount_cancel import apply_mount_cancel_requests

    receipts, mount_ids = apply_mount_cancel_requests(
        [{
            "tool_id": "mount_cancel",
            "mount_area": "instant_list",
            "item_type": "memory",
            "item_id": "MEM-MISSING",
        }],
        modules={},
        mount_ids=[{"type": "memory", "ids": "MEM-KEEP", "mode": "temporary"}],
    )

    assert receipts[0]["status"] == "not_found"
    assert receipts[0]["removed"] is False
    assert mount_ids == [{"type": "memory", "ids": "MEM-KEEP", "mode": "temporary"}]
