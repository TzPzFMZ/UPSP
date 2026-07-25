import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from logic.native_tool_calls import project_step_finalize


def _cleanup_projection(**arguments):
    return project_step_finalize("cleanup", arguments)


# ============================================================
# cleanup_processor 测试
# ============================================================

class TestCleanupProcessor:
    def test_process_cleanup_ignores_retired_fault_table(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

        class DummyAlerts:
            def __init__(self):
                self.entries = []

            def append_alert(self, **kwargs):
                self.entries.append(kwargs)

        ctx = DummyContext()
        alert_store = DummyAlerts()
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))

        parsed = _cleanup_projection()
        parsed["faults"] = "反应步 API timeout"
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": ctx,
            "alert_store": alert_store,
        }

        report = cp.process_cleanup(
            parsed,
            {},
            12,
            {"_interaction_meta": {
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            }},
            data_modules,
        )

        assert report["errors"] == []
        assert [entry[3] for entry in ctx.entries] == ["minimum_commitment"]
        assert alert_store.entries == []

    def test_process_cleanup_strips_non_cache_interaction_meta_fields(self, monkeypatch):
        from logic import cleanup_processor as cp

        class StrictContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(
                    self,
                    round_num,
                    role,
                    text,
                    *,
                    kind,
                    step="round",
                    iter=0,
                    interaction_object="unknown",
                    identity_status="unknown",
                    interaction_source="unresolved"):
                self.entries.append({
                    "round_num": round_num,
                    "role": role,
                    "text": text,
                    "kind": kind,
                    "step": step,
                    "iter": iter,
                    "interaction_object": interaction_object,
                    "identity_status": identity_status,
                    "interaction_source": interaction_source,
                })

        ctx = StrictContext()
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": ctx,
        }

        report = cp.process_cleanup(
            _cleanup_projection(),
            {},
            328,
            {"_interaction_meta": {
                "interaction_object": "TzPz",
                "identity_status": "known",
                "interaction_source": "conversation_context",
                "basis": "setup_finalize evidence",
            }},
            data_modules,
        )

        assert report["errors"] == []
        assert ctx.entries[0]["interaction_object"] == "TzPz"
        assert ctx.entries[0]["identity_status"] == "known"
        assert ctx.entries[0]["interaction_source"] == "conversation_context"

    def test_process_cleanup_ignores_no_exception_fault_text(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, *args, **kwargs):
                self.entries.append((args, kwargs))

        ctx = DummyContext()
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed["faults"] = "无异常"
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": ctx,
        }

        report = cp.process_cleanup(parsed, {}, 12, {}, data_modules)

        assert report["errors"] == []
        assert ctx.entries[0][1]["kind"] == "minimum_commitment"

    def test_process_cleanup_writes_script_boundary_minimum_commitment(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

        ctx = DummyContext()
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed["minimum_commitment"] = {
            "round_id": "R12",
            "phase": "post",
            "payload": "继续验证 lately 压缩链路",
        }
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": ctx,
        }

        report = cp.process_cleanup(parsed, {}, 12, {}, data_modules)

        assert report["errors"] == []
        assert ctx.entries == [
            (
                12,
                "system",
                "[最小承诺] R000012 / post / status=closed",
                "minimum_commitment",
                {"step": "cleanup"},
            )
        ]

    def test_process_cleanup_backfills_missing_minimum_commitment(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

        ctx = DummyContext()
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": ctx,
        }

        report = cp.process_cleanup(parsed, {}, 45, {}, data_modules)

        assert report["errors"] == []
        assert ctx.entries == [(
            45,
            "system",
            "[最小承诺] R000045 / post / status=closed",
            "minimum_commitment",
            {"step": "cleanup"},
        )]

    def test_process_cleanup_ignores_hallucinated_minimum_commitment_payload(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

        ctx = DummyContext()
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed["minimum_commitment"] = {
            "round_id": "R000002",
            "phase": "post",
            "payload": "Spec 041 验证交互完成",
        }
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": ctx,
        }

        report = cp.process_cleanup(parsed, {}, 213, {}, data_modules)

        assert report["errors"] == []
        assert ctx.entries[0][2] == "[最小承诺] R000213 / post / status=closed"

    def test_process_cleanup_does_not_create_memory_for_no_archive_statement(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryStore:
            def __init__(self):
                self.written = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def set_meta(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.written.append((args, kwargs))

        class DummyMemoryIndex:
            def add_stm_keywords(self, *args, **kwargs):
                pass

            def add_ltm_keywords(self, *args, **kwargs):
                pass

        class DummyMemoryHeat:
            def set_entry(self, *args, **kwargs):
                raise AssertionError("no archive should not write heat")

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed.update({
            "archive_title": "记忆条目：无（纯技术验证，无认知变化）。事件链：无推进。",
            "archive_weight": 3,
            "keywords": ["Spec 024"],
        })
        memory_store = DummyMemoryStore()
        data_modules = {
            "state_store": object(),
            "memory_store": memory_store,
            "memory_index": DummyMemoryIndex(),
            "memory_heat": DummyMemoryHeat(),
            "container_store": object(),
            "context_store": object(),
        }

        report = cp.process_cleanup(parsed, {}, 114, {}, data_modules)

        assert report["memory_ids"] == []
        assert memory_store.written == []

    def test_process_cleanup_does_not_create_memory_for_bare_archive_scaffold(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryStore:
            def __init__(self):
                self.written = []

            def write_entry(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def set_meta(self, *args, **kwargs):
                self.written.append((args, kwargs))

            def append_index(self, *args, **kwargs):
                self.written.append((args, kwargs))

        class DummyMemoryIndex:
            def add_stm_keywords(self, *args, **kwargs):
                raise AssertionError("bare scaffold should not write keywords")

            def add_ltm_keywords(self, *args, **kwargs):
                raise AssertionError("bare scaffold should not write keywords")

        class DummyMemoryHeat:
            def set_entry(self, *args, **kwargs):
                raise AssertionError("bare scaffold should not write heat")

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed.update({
            "archive_title": "记忆条目",
            "archive_body": "记忆条目",
            "archive_weight": 5,
            "archive_subject": "Codex",
            "keywords": ["D阶段"],
        })
        memory_store = DummyMemoryStore()
        data_modules = {
            "state_store": object(),
            "memory_store": memory_store,
            "memory_index": DummyMemoryIndex(),
            "memory_heat": DummyMemoryHeat(),
            "container_store": object(),
            "context_store": object(),
        }

        report = cp.process_cleanup(parsed, {}, 130, {}, data_modules)

        assert report["memory_ids"] == []
        assert memory_store.written == []

    def test_process_cleanup_does_not_archive_no_new_memory_commitment(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryStore:
            def write_entry(self, *args, **kwargs):
                raise AssertionError("no-new-memory commitment should not create memory")

            def set_meta(self, *args, **kwargs):
                raise AssertionError("no-new-memory commitment should not write meta")

            def append_index(self, *args, **kwargs):
                raise AssertionError("no-new-memory commitment should not write index")

        class DummyMemoryIndex:
            def add_stm_keywords(self, *args, **kwargs):
                raise AssertionError("no-new-memory commitment should not write keywords")

            def add_ltm_keywords(self, *args, **kwargs):
                raise AssertionError("no-new-memory commitment should not write keywords")

        class DummyMemoryHeat:
            def set_entry(self, *args, **kwargs):
                raise AssertionError("no-new-memory commitment should not write heat")

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed.update({
            "archive_title": "不新建记忆条目；最小承诺记录：工程回归验证完成",
            "archive_body": "不新建记忆条目；最小承诺记录：工程回归验证完成",
            "archive_weight": 3,
            "archive_subject": "Codex",
            "keywords": ["engineering_regression"],
        })
        data_modules = {
            "state_store": object(),
            "memory_store": DummyMemoryStore(),
            "memory_index": DummyMemoryIndex(),
            "memory_heat": DummyMemoryHeat(),
            "container_store": object(),
            "context_store": object(),
        }

        report = cp.process_cleanup(parsed, {}, 131, {}, data_modules)

        assert report["memory_ids"] == []

    def test_process_cleanup_does_not_archive_minimum_commitment_title(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryStore:
            def write_entry(self, *args, **kwargs):
                raise AssertionError("minimum commitment should not create memory")

            def set_meta(self, *args, **kwargs):
                raise AssertionError("minimum commitment should not write meta")

            def append_index(self, *args, **kwargs):
                raise AssertionError("minimum commitment should not write index")

        class DummyMemoryIndex:
            def add_stm_keywords(self, *args, **kwargs):
                raise AssertionError("minimum commitment should not write keywords")

            def add_relation_keywords(self, *args, **kwargs):
                raise AssertionError("minimum commitment should not update relation index")

        class DummyMemoryHeat:
            def set_entry(self, *args, **kwargs):
                raise AssertionError("minimum commitment should not write heat")

        class DummyRelationStore:
            def find_card(self, subject):
                return {"id": "REL-Codex", "name": subject}

            def add_note(self, *args, **kwargs):
                raise AssertionError("minimum commitment should not write relation note")

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed.update({
            "archive_title": "最小承诺：D阶段长跑确认收到，无异常。",
            "archive_body": "最小承诺：D阶段长跑确认收到，无异常。",
            "archive_weight": 5,
            "archive_subject": "Codex",
            "keywords": ["D阶段长跑"],
        })
        data_modules = {
            "state_store": object(),
            "memory_store": DummyMemoryStore(),
            "memory_index": DummyMemoryIndex(),
            "memory_heat": DummyMemoryHeat(),
            "container_store": object(),
            "context_store": object(),
            "relation_store": DummyRelationStore(),
        }

        report = cp.process_cleanup(parsed, {}, 132, {}, data_modules)

        assert report["errors"] == []
        assert report["memory_ids"] == []

    def test_process_cleanup_does_not_archive_relation_note_scaffold(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryStore:
            def write_entry(self, *args, **kwargs):
                raise AssertionError("relation note scaffold should not create memory")

        class DummyRelationStore:
            def find_card(self, subject):
                return {"id": "REL-Codex", "name": subject}

            def add_note(self, *args, **kwargs):
                raise AssertionError("relation note scaffold should not write relation note")

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed.update({
            "archive_title": "关系笔记",
            "archive_body": "关系笔记",
            "archive_weight": 3,
            "archive_subject": "Codex",
        })
        data_modules = {
            "state_store": object(),
            "memory_store": DummyMemoryStore(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": object(),
            "relation_store": DummyRelationStore(),
        }

        report = cp.process_cleanup(parsed, {}, 133, {}, data_modules)

        assert report["errors"] == []
        assert report["memory_ids"] == []

    def test_process_cleanup_does_not_archive_no_progress_statement(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryStore:
            def write_entry(self, *args, **kwargs):
                raise AssertionError("no-progress statement should not create memory")

        class DummyRelationStore:
            def find_card(self, subject):
                return {"id": "REL-Codex", "name": subject}

            def add_note(self, *args, **kwargs):
                raise AssertionError("no-progress statement should not write relation note")

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        parsed.update({
            "archive_title": "无推进",
            "archive_body": "无推进",
            "archive_weight": 5,
            "archive_subject": "Codex",
            "keywords": ["stability_verification"],
        })
        data_modules = {
            "state_store": object(),
            "memory_store": DummyMemoryStore(),
            "memory_index": object(),
            "memory_heat": object(),
            "container_store": object(),
            "context_store": object(),
            "relation_store": DummyRelationStore(),
        }

        report = cp.process_cleanup(parsed, {}, 134, {}, data_modules)

        assert report["errors"] == []
        assert report["memory_ids"] == []

    def test_process_cleanup_ignores_retired_archive_fields(self, monkeypatch):
        from logic import cleanup_processor as cp
        from logic import mem_id

        class DummyMemoryStore:
            def __init__(self):
                self.written = None
                self.meta = None
                self.index = None

            def write_entry(self, mem_id, title, **kwargs):
                self.written = {
                    "mem_id": mem_id,
                    "title": title,
                    **kwargs,
                }

            def set_meta(self, mem_id, meta):
                self.meta = (mem_id, meta)

            def append_index(self, *args, **kwargs):
                self.index = (args, kwargs)

        class DummyMemoryIndex:
            def __init__(self):
                self.relation_calls = []
                self.stm_calls = []
                self.ltm_calls = []

            def add_stm_keywords(self, *args, **kwargs):
                self.stm_calls.append((args, kwargs))

            def add_ltm_keywords(self, *args, **kwargs):
                self.ltm_calls.append((args, kwargs))

            def add_relation_keywords(self, *args, **kwargs):
                self.relation_calls.append((args, kwargs))

        class DummyMemoryHeat:
            def __init__(self):
                self.entry = None

            def set_entry(self, mem_id, entry):
                self.entry = (mem_id, entry)

        class DummyContainerStore:
            pass

        class DummyContext:
            pass

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        monkeypatch.setattr(mem_id, "generate_mem_id", lambda: "MEM-0ABCDEF0")

        parsed = _cleanup_projection()
        parsed.update({
            "keywords": ["记忆规则"],
            "archive_title": "规则落地",
            "archive_weight": 4,
            "archive_subject": "TzPz",
            "archive_body": "我确认记忆写入规则需要落到正文表单，只记录结论与变化。",
        })
        memory_store = DummyMemoryStore()
        memory_index = DummyMemoryIndex()
        data_modules = {
            "state_store": object(),
            "memory_store": memory_store,
            "memory_index": memory_index,
            "memory_heat": DummyMemoryHeat(),
            "container_store": DummyContainerStore(),
            "context_store": DummyContext(),
        }

        report = cp.process_cleanup(
            parsed,
            {"base": {"activity_mode": "工程"}},
            24,
            {"response": "## 一、反应步讲稿\n这不是记忆正文。"},
            data_modules,
        )

        assert report["errors"] == []
        assert memory_store.written is None
        assert memory_store.meta is None
        assert memory_store.index is None
        assert memory_index.stm_calls == []
        assert memory_index.ltm_calls == []

    def test_process_cleanup_ignores_legacy_relation_note_from_archive(self, monkeypatch):
        from logic import cleanup_processor as cp
        from logic import mem_id

        class DummyMemoryStore:
            def write_entry(self, *args, **kwargs):
                pass

            def set_meta(self, *args, **kwargs):
                pass

            def append_index(self, *args, **kwargs):
                pass

        class DummyMemoryIndex:
            def __init__(self):
                self.relation_calls = []

            def add_stm_keywords(self, *args, **kwargs):
                pass

            def add_ltm_keywords(self, *args, **kwargs):
                pass

            def add_relation_keywords(self, *args, **kwargs):
                self.relation_calls.append((args, kwargs))

        class DummyRelationStore:
            def __init__(self):
                self.created = []
                self.notes = []

            def find_card(self, subject):
                return {"id": f"REL-{subject}", "name": subject, "category": "them"}

            def create_card(self, card_id, name, category):
                self.created.append((card_id, name, category))
                return {"id": card_id, "name": name, "category": category}

            def add_note(self, card_id, content):
                self.notes.append((card_id, content))

        class DummyMemoryHeat:
            def set_entry(self, *args, **kwargs):
                pass

        class DummyContext:
            pass

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        monkeypatch.setattr(mem_id, "generate_mem_id", lambda: "MEM-0ABCDEF1")

        parsed = _cleanup_projection()
        parsed.update({
            "archive_title": "关系落地",
            "archive_weight": 4,
            "archive_subject": "Claude",
            "archive_body": "我确认这次直接交互需要形成关系卡与关系日记。",
            "_legacy_relation": [{"subject": "Claude", "word": "信任"}],
        })
        memory_index = DummyMemoryIndex()
        relation_store = DummyRelationStore()
        data_modules = {
            "state_store": object(),
            "memory_store": DummyMemoryStore(),
            "memory_index": memory_index,
            "memory_heat": DummyMemoryHeat(),
            "container_store": object(),
            "context_store": DummyContext(),
            "relation_store": relation_store,
        }

        report = cp.process_cleanup(parsed, {"base": {}}, 25, {}, data_modules)

        assert report["errors"] == []
        assert relation_store.created == []
        assert relation_store.notes == []
        assert memory_index.relation_calls == []

    def test_process_cleanup_does_not_create_card_from_interaction_meta_only(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryIndex:
            def __init__(self):
                self.relation_calls = []

            def add_relation_keywords(self, *args, **kwargs):
                self.relation_calls.append((args, kwargs))

        class DummyRelationStore:
            def __init__(self):
                self.created = []
                self.notes = []

            def find_card(self, subject):
                return None

            def create_card(self, card_id, name, category):
                self.created.append((card_id, name, category))
                return {"id": card_id, "name": name, "category": category}

            def add_note(self, card_id, content):
                self.notes.append((card_id, content))

        class DummyContext:
            pass

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        memory_index = DummyMemoryIndex()
        relation_store = DummyRelationStore()
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": memory_index,
            "memory_heat": object(),
            "container_store": object(),
            "context_store": DummyContext(),
            "relation_store": relation_store,
        }

        report = cp.process_cleanup(
            parsed,
            {"base": {}},
            117,
            {"_interaction_meta": {
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            }},
            data_modules,
        )

        assert report["errors"] == []
        assert relation_store.created == []
        assert relation_store.notes == []
        assert memory_index.relation_calls == []
        assert "_relation_cards" not in report

    def test_process_cleanup_does_not_create_relation_card_from_reaction_declaration(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryIndex:
            def __init__(self):
                self.relation_calls = []

            def add_relation_keywords(self, *args, **kwargs):
                self.relation_calls.append((args, kwargs))

        class DummyRelationStore:
            def __init__(self):
                self.created = []

            def find_card(self, subject):
                return None

            def create_card(self, card_id, name, category):
                self.created.append((card_id, name, category))
                return {"id": card_id, "name": name, "category": category}

        class DummyContext:
            pass

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        memory_index = DummyMemoryIndex()
        relation_store = DummyRelationStore()
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": memory_index,
            "memory_heat": object(),
            "container_store": object(),
            "context_store": DummyContext(),
            "relation_store": relation_store,
        }

        report = cp.process_cleanup(
            parsed,
            {"base": {}},
            118,
            {
                "response": "| 新建关系卡 | them | Codex | 已确认直接交互 |",
                "_interaction_meta": {
                    "interaction_object": "Codex",
                    "identity_status": "declared",
                    "interaction_source": "self_declaration",
                },
            },
            data_modules,
        )

        assert report["errors"] == []
        assert relation_store.created == []
        assert memory_index.relation_calls == []
        assert "_relation_cards" not in report

    def test_process_cleanup_does_not_apply_relation_mounting_from_relation_declaration(self, monkeypatch):
        from logic import cleanup_processor as cp

        class DummyMemoryIndex:
            def __init__(self):
                self.relation_calls = []

            def add_relation_keywords(self, *args, **kwargs):
                self.relation_calls.append((args, kwargs))

        class DummyRelationStore:
            def __init__(self):
                self.created = []

            def find_card(self, subject):
                return None

            def create_card(self, card_id, name, category):
                self.created.append((card_id, name, category))
                return {"id": card_id, "name": name, "category": category}

        class DummyContext:
            pass

        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))
        parsed = _cleanup_projection()
        memory_index = DummyMemoryIndex()
        relation_store = DummyRelationStore()
        data_modules = {
            "state_store": object(),
            "memory_store": object(),
            "memory_index": memory_index,
            "memory_heat": object(),
            "container_store": object(),
            "context_store": DummyContext(),
            "relation_store": relation_store,
        }

        report = cp.process_cleanup(
            parsed,
            {"base": {}},
            218,
            {
                "_relation_card_declarations": [{
                    "name": "Codex",
                    "category": "them",
                }],
                "_interaction_meta": {
                    "interaction_object": "Codex",
                    "identity_status": "declared",
                    "interaction_source": "self_declaration",
                },
            },
            data_modules,
        )

        assert report["errors"] == []
        assert relation_store.created == []
        assert "_relation_cards" not in report

    def test_process_cleanup_ignores_reaction_relation_card_declarations(self, tmp_path, monkeypatch):
        from data import relation_store as rs
        from logic import cleanup_processor as cp

        class DummyMemoryIndex:
            def add_relation_keywords(self, *args, **kwargs):
                pass

        class DummyContext:
            pass

        rel_dir = tmp_path / "relation"
        monkeypatch.setattr(rs, "RELATION_DIR", str(rel_dir))
        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "relation_registry.json"))
        monkeypatch.setattr(
            rs.RelationStore,
            "get_card_path",
            lambda self, cid, cat="ours": str(rel_dir / cat / f"{cid}.md"),
        )
        monkeypatch.setattr(cp, "scan_orphans", lambda cs, round_start: ([], None))

        relation_store = rs.RelationStore()
        report = cp.process_cleanup(
            _cleanup_projection(),
            {"base": {}},
            221,
            {
                "_relation_card_declarations": [{
                    "name": "Codex",
                    "category": "them",
                }],
                "_interaction_meta": {
                    "interaction_object": "Codex",
                    "identity_status": "declared",
                    "interaction_source": "self_declaration",
                },
            },
            {
                "state_store": object(),
                "memory_store": object(),
                "memory_index": DummyMemoryIndex(),
                "memory_heat": object(),
                "container_store": object(),
                "context_store": DummyContext(),
                "relation_store": relation_store,
            },
        )

        registry = relation_store.load_registry()
        assert report["errors"] == []
        assert registry["cards"] == []
        assert "_relation_cards" not in report
