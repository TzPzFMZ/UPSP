import os
import sys

import pytest

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


class _DummyResidentStore:
    def __init__(self):
        self.revision = 0
        self.mounted = False

    def snapshot_bytes(self):
        return None

    def restore_bytes(self, _payload):
        return None

    def add(self, _item, *, candidate=None, expected_revision=None):
        self.revision += 1
        self.mounted = True
        return {"status": "applied", "revision": self.revision}

    def remove_matching(self, **_kwargs):
        self.revision += 1
        self.mounted = False
        return {"status": "applied", "removed": True, "revision": self.revision}

    def contains(self, **_kwargs):
        return self.mounted


class _DummyResidentAssembler:
    def __init__(self):
        self.resident_store = _DummyResidentStore()

    def preflight_resident_add(self, _item, *, content_overrides=None):
        return {
            "document": {},
            "changed": True,
            "chars": len(next(iter((content_overrides or {}).values()), "")),
            "expected_revision": self.resident_store.revision,
        }

    @staticmethod
    def resident_projection_chars(document=None, content_overrides=None):
        return len(next(iter((content_overrides or {}).values()), ""))


def test_mounted_content_uses_full_mount_payload_and_labels_full_read():
    from assembly.context_mounts import build_mounted_content

    full_body = "正文行一\n" + ("完整正文-" * 120) + "\n正文行三"

    class DummyAssembler:
        def _load_memory_content(self, ids):
            raise AssertionError("mount payload should be used before legacy loader")

        def _load_container_content(self, ids):
            raise AssertionError("mount payload should be used before legacy loader")

        def _load_relation_content(self, ids):
            raise AssertionError("mount payload should be used before legacy loader")

        def _load_skill_content(self, ids):
            raise AssertionError("mount payload should be used before legacy loader")

        def _memory_mount_meta(self, ids):
            return {}

    rendered = build_mounted_content(DummyAssembler(), [{
        "type": "container",
        "ids": "DC-329",
        "mode": "resident",
        "source": "container_read",
        "target_file": "open.md",
        "read_mode": "full",
        "content": full_body,
        "total_lines": 3,
        "total_chars": len(full_body),
    }])

    assert "### 容器 DC-329 / open.md" in rendered
    assert "来源工具：container_read" in rendered
    assert "读取模式：full" in rendered
    assert f"总字符数：{len(full_body)}" in rendered
    assert full_body in rendered


def test_merge_mount_requests_upgrades_existing_mount_with_read_payload():
    from assembly.context_mounts import build_mounted_content
    from engines.reaction_helpers import merge_mount_requests

    full_body = "后续读取正文\n" + ("必须进窗" * 40)

    class DummyAssembler:
        def _load_memory_content(self, ids):
            raise AssertionError("merged read payload should avoid legacy loader")

        def _load_container_content(self, ids):
            raise AssertionError("merged read payload should avoid legacy loader")

        def _load_relation_content(self, ids):
            raise AssertionError("merged read payload should avoid legacy loader")

        def _load_skill_content(self, ids):
            raise AssertionError("merged read payload should avoid legacy loader")

        def _memory_mount_meta(self, ids):
            return {}

    merged = merge_mount_requests(
        [{"type": "container", "ids": "DC-333", "mode": "resident"}],
        [{
            "type": "container",
            "ids": "DC-333",
            "mode": "resident",
            "source": "container_read",
            "target_file": "open.md",
            "content": full_body,
            "read_mode": "full",
            "total_lines": 2,
            "total_chars": len(full_body),
        }],
    )

    assert len(merged) == 1
    assert merged[0]["content"] == full_body
    rendered = build_mounted_content(DummyAssembler(), merged)
    assert "来源工具：container_read" in rendered
    assert full_body in rendered


def test_mounted_content_renders_empty_successful_read_metadata():
    from assembly.context_mounts import build_mounted_content

    class DummyAssembler:
        def _load_memory_content(self, ids):
            raise AssertionError("empty read payload is still a payload")

        def _load_container_content(self, ids):
            raise AssertionError("empty read payload is still a payload")

        def _load_relation_content(self, ids):
            raise AssertionError("empty read payload is still a payload")

        def _load_skill_content(self, ids):
            raise AssertionError("empty read payload is still a payload")

        def _memory_mount_meta(self, ids):
            return {}

    rendered = build_mounted_content(DummyAssembler(), [{
        "type": "container",
        "ids": "DC-EMPTY",
        "mode": "resident",
        "source": "container_read",
        "target_file": "open.md",
        "content": "",
        "read_mode": "full",
        "total_lines": 0,
        "total_chars": 0,
    }])

    assert "### 容器 DC-EMPTY / open.md" in rendered
    assert "来源工具：container_read" in rendered
    assert "读取模式：full" in rendered
    assert "总字符数：0" in rendered


def test_memory_content_read_resident_reads_full_body_without_max_chars():
    from logic.memory_content_read import apply_memory_content_read_requests

    class DummyMemoryStore:
        def read_meta_by_id(self, mem_id):
            return {"id": mem_id, "title": "Full memory", "access": "public"}

        def read_body_by_id(self, mem_id):
            body = "记忆全文\n" + ("不要截断" * 300)
            return {
                "body": body,
                "meta": {"id": mem_id, "title": "Full memory"},
                "memory_layer": "STM",
                "total_lines": len(body.splitlines()),
                "total_chars": len(body),
                "read_mode": "full",
            }

    class DummyMemoryRecall:
        @staticmethod
        def recall(_mem_id, **_kwargs):
            commit = _kwargs.get("transaction_commit")
            if callable(commit):
                commit()
            return {
                "source_memory_layer": "STM",
                "stm_present": True,
                "heat_boost_applied": False,
                "heat_boost_deduplicated": False,
            }

    store = DummyMemoryStore()
    assembler = _DummyResidentAssembler()
    receipts, mounts, unmounts = apply_memory_content_read_requests(
        [{
            "tool_id": "memory_content_read",
            "mem_id": "MEM-FULLREAD",
            "mount_mode": "resident",
        }],
        {"presence": {"confirmed_subjects": ["Codex"]}},
        {
            "memory_store": store,
            "memory_recall": DummyMemoryRecall(),
            "assembler": assembler,
            "resident_store": assembler.resident_store,
        },
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["read_mode"] == "full"
    assert receipts[0]["total_chars"] == len(receipts[0]["body"])
    assert "不要截断" * 20 in receipts[0]["body"]
    assert len(mounts) == 1
    assert mounts[0]["type"] == "memory"
    assert mounts[0]["ids"] == "MEM-FULLREAD"
    assert mounts[0]["mode"] == "resident"
    assert mounts[0]["source"] == "memory_content_read"
    assert mounts[0]["content_projected"] is True
    assert receipts[0]["body"] in mounts[0]["content"]
    assert mounts[0]["read_mode"] == "full"
    assert unmounts == []


def test_relation_read_body_uses_full_notes_without_six_note_or_char_cutoff():
    from logic.relation_read import apply_relation_read_requests

    notes = [
        {"content": f"关系正文第 {index} 条 " + ("内容" * 60)}
        for index in range(1, 9)
    ]

    class DummyRelationStore:
        def load_registry(self):
            return {"cards": [{
                "id": "REL-FULL",
                "name": "Codex",
                "category": "ours",
                "status": "active",
            }]}

        def read_card(self, card_id, category=None):
            return {
                "id": card_id,
                "name": "Codex",
                "category": category or "ours",
                "notes": notes,
            }

    assembler = _DummyResidentAssembler()
    receipts, mounts, unmounts = apply_relation_read_requests(
        [{
            "tool_id": "relation_read",
            "card_id": "REL-FULL",
            "body": "resident",
            "summary": "none",
        }],
        {
            "relation_store": DummyRelationStore(),
            "assembler": assembler,
            "resident_store": assembler.resident_store,
        },
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["read_mode"] == "full"
    assert "关系正文第 1 条" in receipts[0]["body"]
    assert "关系正文第 8 条" in receipts[0]["body"]
    assert receipts[0]["total_chars"] == len(receipts[0]["body"])
    body_mount = next(item for item in mounts if item["type"] == "relation")
    assert body_mount["mode"] == "resident"
    assert "关系正文第 1 条" in body_mount["content"]
    assert "关系正文第 8 条" in body_mount["content"]
    assert unmounts == []


def test_memory_content_read_rechecks_resident_capacity_after_recall():
    from constants import RESIDENT_LIST_CHAR_LIMIT
    from logic.memory_content_read import apply_memory_content_read_requests

    class MemoryStore:
        def read_meta_by_id(self, mem_id):
            return {"id": mem_id, "title": "Recall growth", "access": "public"}

        def read_body_by_id(self, mem_id, **_range):
            body = "## MEM-RECALL\n**内容**：正文"
            return {
                "body": body,
                "meta": {"id": mem_id, "title": "Recall growth"},
                "memory_layer": "Full",
                "total_lines": 2,
                "total_chars": len(body),
                "read_mode": "full",
            }

    class ResidentStore(_DummyResidentStore):
        def __init__(self):
            super().__init__()
            self.add_calls = 0

        def add(self, *args, **kwargs):
            self.add_calls += 1
            return super().add(*args, **kwargs)

    resident = ResidentStore()

    class Assembler:
        def preflight_resident_add(self, _item, *, content_overrides=None):
            return {
                "document": {"items": []},
                "changed": True,
                "chars": RESIDENT_LIST_CHAR_LIMIT,
                "expected_revision": resident.revision,
            }

        @staticmethod
        def resident_projection_chars(document=None, content_overrides=None):
            return RESIDENT_LIST_CHAR_LIMIT + 1

    class Recall:
        @staticmethod
        def recall(_mem_id, **kwargs):
            kwargs["transaction_commit"]()
            raise AssertionError("capacity failure must abort recall")

    receipts, mounts, unmounts = apply_memory_content_read_requests(
        [{
            "tool_id": "memory_content_read",
            "mem_id": "MEM-RECALL",
            "mount_mode": "resident",
        }],
        {"presence": {"confirmed_subjects": ["Codex"]}},
        {
            "memory_store": MemoryStore(),
            "memory_recall": Recall(),
            "assembler": Assembler(),
            "resident_store": resident,
        },
    )

    assert receipts[0]["status"] == "error"
    assert receipts[0]["reason"].startswith("resident_list_char_limit_exceeded:")
    assert resident.add_calls == 0
    assert mounts == []
    assert unmounts == []


def test_relation_read_summary_keeps_summary_semantics_not_notes_body():
    from logic.relation_read import apply_relation_read_requests

    class DummyRelationStore:
        def load_registry(self):
            return {"cards": [{
                "id": "REL-SUMMARY",
                "name": "Codex",
                "category": "ours",
                "status": "active",
                "summary": "稳定摘要",
            }]}

        def read_card(self, card_id, category=None):
            return {
                "id": card_id,
                "name": "Codex",
                "category": category or "ours",
                "summary": "稳定摘要",
                "notes": [{"content": "这是一段关系正文 notes，不应该进入 summary。"}],
            }

    assembler = _DummyResidentAssembler()
    receipts, mounts, unmounts = apply_relation_read_requests(
        [{
            "tool_id": "relation_read",
            "card_id": "REL-SUMMARY",
            "summary": "temporary",
            "body": "none",
        }],
        {
            "relation_store": DummyRelationStore(),
            "assembler": assembler,
            "resident_store": assembler.resident_store,
        },
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["summary"] == "稳定摘要"
    assert "不应该进入 summary" not in receipts[0]["summary"]
    assert mounts == [{
        "type": "relation_summary",
        "ids": "REL-SUMMARY",
        "mode": "temporary",
        "subject": "Codex",
    }]
    assert unmounts == [{
        "item_type": "relation",
        "item_id": "REL-SUMMARY",
    }]


def test_container_read_returns_resident_mount_with_full_content_and_metadata():
    from logic.container_read import apply_container_read_requests

    body = "第一行\n第二行\n" + ("完整容器正文" * 180)

    class DummyContainerStore:
        def read_container_content(self, container_id, target_file=None, **kwargs):
            assert "max_chars" not in kwargs
            assert kwargs == {}
            return {
                "container_id": container_id,
                "container_type": "DC",
                "status": "active",
                "title": "Full container",
                "target_file": target_file or "open.md",
                "path": "D:/fake/DC-329/open.md",
                "content": body,
                "chars": len(body),
                "read_mode": "full",
                "total_lines": len(body.splitlines()),
                "total_chars": len(body),
            }

    assembler = _DummyResidentAssembler()
    receipts, mounts = apply_container_read_requests(
        [{
            "tool_id": "container_read",
            "container_id": "DC-329",
            "target_file": "open.md",
        }],
        {
            "container_store": DummyContainerStore(),
            "assembler": assembler,
            "resident_store": assembler.resident_store,
        },
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["read_mode"] == "full"
    assert receipts[0]["content"] == body
    assert mounts == [{
        "type": "container",
        "ids": "DC-329",
        "mode": "resident",
        "source": "container_read",
        "target_file": "open.md",
        "path": "D:/fake/DC-329/open.md",
        "content": body,
        "read_mode": "full",
        "total_lines": len(body.splitlines()),
        "total_chars": len(body),
    }]


def test_container_read_explicit_line_range_marks_partial_and_records_range():
    from logic.container_read import apply_container_read_requests

    body = "\n".join(f"第 {index} 行" for index in range(1, 8))

    class DummyContainerStore:
        def read_container_content(self, container_id, target_file=None, **kwargs):
            from utils.content_ranges import apply_explicit_range

            result = apply_explicit_range(body, kwargs)
            result.update({
                "container_id": container_id,
                "container_type": "DC",
                "status": "active",
                "title": "Range container",
                "target_file": target_file or "open.md",
                "path": "D:/fake/DC-331/open.md",
                "chars": len(result["content"]),
            })
            return result

    assembler = _DummyResidentAssembler()
    receipts, mounts = apply_container_read_requests(
        [{
            "tool_id": "container_read",
            "container_id": "DC-331",
            "target_file": "open.md",
            "line_start": 2,
            "line_end": 4,
        }],
        {
            "container_store": DummyContainerStore(),
            "assembler": assembler,
            "resident_store": assembler.resident_store,
        },
    )

    assert receipts[0]["status"] == "accepted"
    assert receipts[0]["read_mode"] == "partial"
    assert receipts[0]["range_requested"] == {
        "type": "line",
        "line_start": 2,
        "line_end": 4,
    }
    assert receipts[0]["range_applied"] == {
        "type": "line",
        "line_start": 2,
        "line_end": 4,
    }
    assert receipts[0]["content"] == "第 2 行\n第 3 行\n第 4 行"
    assert mounts[0]["read_mode"] == "full"
    assert mounts[0]["content"] == body
    assert "range_applied" not in mounts[0]


def test_explicit_line_and_char_ranges_conflict_is_rejected():
    from logic.container_read import apply_container_read_requests

    class DummyContainerStore:
        def read_container_content(self, container_id, target_file=None, **kwargs):
            from utils.content_ranges import apply_explicit_range

            return apply_explicit_range("abc", kwargs)

    receipts, mounts = apply_container_read_requests(
        [{
            "tool_id": "container_read",
            "container_id": "DC-331",
            "target_file": "open.md",
            "line_start": 1,
            "line_end": 1,
            "char_start": 1,
            "char_end": 2,
        }],
        {"container_store": DummyContainerStore()},
    )

    assert receipts[0]["status"] == "rejected"
    assert receipts[0]["reason"] == "range_mode_conflict"
    assert mounts == []


def test_explicit_line_and_char_range_presence_conflict_precedes_pair_validation():
    from utils.content_ranges import apply_explicit_range

    with pytest.raises(ValueError, match="range_mode_conflict"):
        apply_explicit_range("abc", {"line_start": 1, "char_start": 1})


def test_spec399_empty_range_placeholders_are_treated_as_absent():
    from utils.content_ranges import apply_explicit_range

    result = apply_explicit_range(
        "第一行\n第二行\n第三行",
        {
            "line_start": "None",
            "line_end": 0,
            "char_start": "null",
            "char_end": "undefined",
        },
    )

    assert result["read_mode"] == "full"
    assert result["range_requested"] is None
    assert result["content"] == "第一行\n第二行\n第三行"


@pytest.mark.parametrize("range_request", [
    {"line_end": 2},
    {"char_end": 2},
])
def test_spec400_extra_end_range_fields_are_ignored_for_file_read(tmp_path, range_request):
    from logic.general_tools import _execute_file_read

    path = tmp_path / "range.md"
    path.write_text("第一行\n第二行\n第三行", encoding="utf-8")
    result = _execute_file_read(
        {"path": str(path), **range_request},
        allowed_roots=(tmp_path,),
    )

    assert result["status"] == "ok"
    assert result["range_requested"] is None
    assert result["requested_end_line"] is None
    assert result["content"] == "第一行\n第二行\n第三行"


@pytest.mark.parametrize("range_request", [
    {"line_start": 2},
    {"line_end": 2},
    {"char_start": 2},
    {"char_end": 2},
])
def test_explicit_range_requires_start_and_end_pairs_for_protocol_reads(range_request):
    from logic.container_read import apply_container_read_requests
    from logic.memory_content_read import apply_memory_content_read_requests
    from logic.relation_read import apply_relation_read_requests

    class DummyContainerStore:
        def read_container_content(self, container_id, target_file=None, **kwargs):
            from utils.content_ranges import apply_explicit_range

            return apply_explicit_range("第一行\n第二行\n第三行", kwargs)

    container_receipts, container_mounts = apply_container_read_requests(
        [{"tool_id": "container_read", "container_id": "DC-333", **range_request}],
        {"container_store": DummyContainerStore()},
    )
    assert container_receipts[0]["status"] == "rejected"
    assert container_receipts[0]["reason"] == "range_pair_required"
    assert container_mounts == []

    class DummyMemoryStore:
        def read_meta_by_id(self, mem_id):
            return {"id": mem_id, "title": "Range memory", "access": "public"}

        def read_body_by_id(self, mem_id, **kwargs):
            from utils.content_ranges import apply_explicit_range

            result = apply_explicit_range("第一行\n第二行\n第三行", kwargs)
            return {
                "body": result["content"],
                "meta": {"id": mem_id, "title": "Range memory"},
                "memory_layer": "STM",
                "read_mode": result["read_mode"],
                "range_requested": result["range_requested"],
                "range_applied": result["range_applied"],
                "total_lines": result["total_lines"],
                "total_chars": result["total_chars"],
            }

    class DummyMemoryRecall:
        @staticmethod
        def recall(*_args, **_kwargs):
            raise AssertionError("invalid range must fail before recall")

    memory_receipts, memory_mounts, memory_unmounts = apply_memory_content_read_requests(
        [{"tool_id": "memory_content_read", "mem_id": "MEM-333", **range_request}],
        {"presence": {"confirmed_subjects": ["Codex"]}},
        {
            "memory_store": DummyMemoryStore(),
            "memory_recall": DummyMemoryRecall(),
        },
    )
    assert memory_receipts[0]["status"] == "rejected"
    assert memory_receipts[0]["reason"] == "range_pair_required"
    assert memory_mounts == []
    assert memory_unmounts == []

    class DummyRelationStore:
        def load_registry(self):
            return {"cards": [{
                "id": "REL-333",
                "name": "Codex",
                "category": "ours",
                "status": "active",
            }]}

        def read_card(self, card_id, category=None):
            return {
                "id": card_id,
                "name": "Codex",
                "category": category or "ours",
                "notes": [{"content": "第一行\n第二行\n第三行"}],
            }

    relation_receipts, relation_mounts, relation_unmounts = apply_relation_read_requests(
        [{
            "tool_id": "relation_read",
            "card_id": "REL-333",
            "body": "resident",
            "summary": "none",
            **range_request,
        }],
        {"relation_store": DummyRelationStore()},
    )
    assert relation_receipts[0]["status"] == "rejected"
    assert relation_receipts[0]["reason"] == "range_pair_required"
    assert relation_mounts == []
    assert relation_unmounts == []
