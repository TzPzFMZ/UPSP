import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


class DummyResidentStore:
    def __init__(self):
        self.items = []

    def load(self):
        return {"items": list(self.items)}

    def remove_matching(self, *, item_type, item_id, target_file=""):
        removed = []
        kept = []
        for item in self.items:
            matches = (
                item.get("item_type") == item_type
                and item.get("item_id") == item_id
                and (not target_file or item.get("target_file", "") == target_file)
            )
            (removed if matches else kept).append(item)
        self.items = kept
        return {
            "removed": bool(removed),
            "removed_items": removed,
            "revision": 2,
        }


class DummyAssembler:
    def __init__(self, items):
        self.store = DummyResidentStore()
        self.store.items = list(items)
        self.resident_store = self.store


def test_spec781_mount_cancel_rejects_retired_focus_area():
    from logic.mount_cancel import apply_mount_cancel_requests

    receipts, mount_ids = apply_mount_cancel_requests(
        [{
            "tool_id": "mount_cancel",
            "mount_area": "focus",
            "item_type": "container",
            "item_id": "PRJ-305",
            "reason": "no longer editing",
        }],
        modules={},
        mount_ids=[{"type": "memory", "ids": "MEM-KEEP", "mode": "temporary"}],
    )

    assert receipts[0]["status"] == "rejected"
    assert receipts[0]["mount_area"] == "focus"
    assert receipts[0]["reason"] == "invalid_mount_area"
    assert receipts[0]["removed"] is False
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


def test_spec781_mount_cancel_removes_resident_relation_reference():
    from logic.mount_cancel import apply_mount_cancel_requests

    assembler = DummyAssembler([
        {"item_type": "relation", "item_id": "REL-305"},
    ])

    receipts, mount_ids = apply_mount_cancel_requests(
        [{
            "tool_id": "mount_cancel",
            "mount_area": "resident_list",
            "item_type": "relation",
            "item_id": "REL-305",
        }],
        modules={"resident_store": assembler.store},
        mount_ids=[{"type": "relation", "ids": "REL-305", "mode": "resident"}],
    )

    assert receipts[0]["status"] == "applied"
    assert receipts[0]["resident_revision"] == 2
    assert assembler.store.items == []
    assert mount_ids == []


def test_spec781_mount_cancel_container_target_keeps_other_resident_file():
    from logic.mount_cancel import apply_mount_cancel_requests

    assembler = DummyAssembler([
        {
            "item_type": "container",
            "item_id": "PRJ-781",
            "target_file": "plan.md",
        },
        {
            "item_type": "container",
            "item_id": "PRJ-781",
            "target_file": "notes.md",
        },
    ])
    mounts = [
        {
            "type": "container",
            "ids": "PRJ-781",
            "target_file": "plan.md",
            "mode": "resident",
        },
        {
            "type": "container",
            "ids": "PRJ-781",
            "target_file": "notes.md",
            "mode": "resident",
        },
    ]

    receipts, remaining = apply_mount_cancel_requests(
        [{
            "tool_id": "mount_cancel",
            "mount_area": "resident_list",
            "item_type": "container",
            "item_id": "PRJ-781",
            "target_file": "plan.md",
        }],
        modules={"resident_store": assembler.store},
        mount_ids=mounts,
    )

    assert receipts[0]["status"] == "applied"
    assert [item["target_file"] for item in assembler.store.items] == [
        "notes.md"
    ]
    assert [item["target_file"] for item in remaining] == ["notes.md"]


def test_spec781_mount_cancel_auto_fails_closed_when_resident_truth_is_unreadable():
    from logic.mount_cancel import apply_mount_cancel_requests

    class UnreadableStore:
        def load(self):
            raise ValueError("corrupt resident truth")

    class UnreadableAssembler:
        resident_store = UnreadableStore()

    original = []
    receipts, remaining = apply_mount_cancel_requests(
        [{
            "tool_id": "mount_cancel",
            "mount_area": "resident_list",
            "item_type": "auto",
            "item_id": "PRJ-781",
        }],
        modules={"resident_store": UnreadableStore()},
        mount_ids=original,
    )

    assert receipts[0]["status"] == "error"
    assert receipts[0]["reason"] == "resident_list_read_failed"
    assert remaining == original


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
