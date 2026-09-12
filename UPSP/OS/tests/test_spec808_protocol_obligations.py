"""Spec808：真实隔离存储的写后登记、恢复及原始异常合同。"""
from types import SimpleNamespace

import pytest

from errors import ReadError, UncertainEffectError


def test_guide_backend_receipt_survives_ledger_failure(tmp_path, monkeypatch):
    from data.workbench import WorkbenchStore
    from logic.guide_submit import apply_guide_submit
    from logic.rhythm_guide_materializer import materialize_current_rhythm_guide

    store = WorkbenchStore(str(tmp_path / "workbench"))
    guide_id = materialize_current_rhythm_guide(store, {"rhythm_due": True}, round_num=808)
    writes = []
    def commit(scope, content):
        target = tmp_path / "chronicle.md"
        target.write_text(content, encoding="utf-8")
        writes.append(target)
        return str(target)
    def fail_ledger(*args):
        raise OSError("guide ledger offline")
    monkeypatch.setattr(store, "append_guide_ledger", fail_ledger)
    with pytest.raises(UncertainEffectError) as caught:
        apply_guide_submit(store, {"guide_id": guide_id, "item_id": "rhythm_due",
            "option_id": "write_chronicle", "fields": {"content": "已写入"}}, {
                "round_num": 808, "chronicle_store": SimpleNamespace(commit_write_scope=commit),
                "chronicle_write_scope": {"schema_version": "chronicle_write_scope.v1",
                    "layer": "rhythms", "round_num": 808, "round_type": "rhythm", "source_refs": ["round:808"]}})
    assert len(writes) == 1 and writes[0].read_text(encoding="utf-8") == "已写入"
    assert caught.value.affected_ids == [guide_id]
    assert str(caught.value.cause) == "guide ledger offline"
    assert caught.value.partial_receipts[0]["backend_receipts"][0]["status"] == "applied"


def test_workbench_move_failure_keeps_destination_and_task_id(tmp_path, monkeypatch):
    from data.workbench import WorkbenchStore
    store = WorkbenchStore(str(tmp_path / "workbench"))
    task_id = store.create_task_guide_task("隔离任务", "保留目录")
    def fail(*args):
        raise OSError("after rename")
    monkeypatch.setattr(store, "_atomic_write_json", fail)
    with pytest.raises(UncertainEffectError) as caught:
        store.move_to_output(task_id, "已完成")
    assert caught.value.affected_ids == [task_id]
    assert str(caught.value.cause) == "after rename"
    assert (tmp_path / "workbench" / "output" / task_id / "manifest.json").exists()
    assert not (tmp_path / "workbench" / "process" / task_id).exists()


def test_workbench_creation_failure_keeps_partial_files_and_allocated_id(tmp_path, monkeypatch):
    from data.workbench import WorkbenchStore
    store = WorkbenchStore(str(tmp_path / "workbench"))
    def fail(*args):
        raise OSError("payload write failed")
    monkeypatch.setattr(store, "_atomic_write_text", fail)
    with pytest.raises(UncertainEffectError) as caught:
        store.create_task_guide_task("隔离任务", "保留现场")
    task_id = caught.value.affected_ids[0]
    assert (tmp_path / "workbench" / "process" / task_id / "manifest.json").exists()


@pytest.mark.parametrize("error", [ReadError("broken meta"), TypeError("implementation bug")])
def test_memory_read_failure_is_not_absence_and_keeps_prior_unmount(error):
    from logic.memory_content_read import apply_memory_content_read_requests
    def fail(*args):
        raise error
    with pytest.raises(type(error)) as caught:
        apply_memory_content_read_requests([
            {"mem_id": "MEM-80800001", "mount_mode": "none"},
            {"mem_id": "MEM-80800002"}], {}, {
                "memory_store": SimpleNamespace(read_meta_by_id=fail),
                "resident_store": SimpleNamespace(remove_matching=lambda **kw: {"revision": 2})})
    assert caught.value is error
    assert len(error.partial_receipts) == 1
    assert error.partial_receipts[0]["reason"] == "unmounted"


def test_file_restore_attempts_all_files_and_keeps_each_failure(tmp_path, monkeypatch):
    from data import memory_store
    a, b = tmp_path / "a", tmp_path / "b"
    a.write_text("changed")
    b.write_text("changed")
    write = memory_store.atomic_write_text
    def fail_first(path, text):
        if path == str(a):
            raise OSError("a locked")
        return write(path, text)
    monkeypatch.setattr(memory_store, "atomic_write_text", fail_first)
    with pytest.raises(ExceptionGroup) as caught:
        memory_store.MemoryStore.restore_ltm_files({str(a): "old a", str(b): "old b"})
    assert b.read_text() == "old b"
    assert str(caught.value.exceptions[0]) == "a locked"


def test_classification_write_failure_retains_original_pending_ledger(tmp_path, monkeypatch):
    from data.action_recovery_store import ActionRecoveryStore
    store = ActionRecoveryStore(tmp_path / "ledger.json")
    store.prepare_opaque(tool_id="shell_command", request_sha256="a" * 64,
        runtime_context={"round_num": 808, "iteration": 1}, call_id="unknown", target="shell")
    before = (tmp_path / "ledger.json").read_bytes()
    def fail(document):
        raise OSError("classification not persisted")
    monkeypatch.setattr(store, "_save", fail)
    with pytest.raises(OSError, match="classification not persisted"):
        store.classify_pending()
    assert (tmp_path / "ledger.json").read_bytes() == before
    assert store.has_unsettled(808)


def test_explicit_index_failure_is_failed_and_unknown_error_propagates():
    from engines.reaction_protocol_tool_execution import apply_index_view_requests
    def failed(**kwargs):
        raise ReadError("index")
    assembler = SimpleNamespace(build_index_view=failed, _derive_input_keywords=lambda *args: [])
    args = (assembler, [{"scope": "ltm_heat"}], {}, "interactive", [], {})
    assert apply_index_view_requests(*args)[0]["status"] == "failed"
    def bug(**kwargs):
        raise TypeError("index implementation")
    assembler.build_index_view = bug
    with pytest.raises(TypeError, match="index implementation"):
        apply_index_view_requests(*args)


def test_corrupt_ltm_meta_is_not_an_empty_index(tmp_path, monkeypatch):
    import paths
    from assembly.context_indexes import build_ltm_heat_index
    meta = tmp_path / "broken.json"
    meta.write_text("{broken", encoding="utf-8")
    monkeypatch.setattr(paths, "LTM_FULL_META_JSON", str(meta))
    with pytest.raises(ReadError):
        build_ltm_heat_index(SimpleNamespace(_memory_meta_visible=lambda item: True))


def test_association_corruption_is_not_overwritten(tmp_path):
    from data.training_material_store import update_count_file
    path = tmp_path / "counts.json"
    path.write_bytes(b'{broken')
    with pytest.raises(ReadError):
        update_count_file(str(path), [("a", "b")])
    assert path.read_bytes() == b'{broken'


def test_association_restore_continues_after_one_file_fails(tmp_path, monkeypatch):
    from engines import reaction_metabolism as module
    first, second = tmp_path / "first", tmp_path / "second"
    first.write_bytes(b'changed'); second.write_bytes(b'changed')
    write = module.durable_write_bytes
    def fail_one(path, data):
        if path == str(first):
            raise OSError("first locked")
        write(path, data)
    monkeypatch.setattr(module, "durable_write_bytes", fail_one)
    with pytest.raises(ExceptionGroup):
        module.ReactionMetabolism._restore_association_snapshot({str(first): b'old1', str(second): b'old2'})
    assert second.read_bytes() == b'old2'


def test_policy_migration_preserves_original_and_rollback_failure(tmp_path, monkeypatch):
    from data import config_store as module
    path = str(tmp_path / "policy.json")
    original = TypeError("readback implementation fault")
    rollback = OSError("restore unavailable")
    store = module.ConfigStore()
    monkeypatch.setattr(store, "_read_json_object", lambda _: (_ for _ in ()).throw(original))
    def write(target, data):
        if data == {"old": True}:
            raise rollback
    monkeypatch.setattr(module, "atomic_write_json", write)
    with pytest.raises(UncertainEffectError) as caught:
        store._write_policy_migration(path, {"old": True}, {"new": True})
    assert caught.value.cause is original
    assert caught.value.rollback_causes == [rollback]
    assert caught.value.affected_ids == [path]


@pytest.mark.parametrize("rollback_fails", [False, True])
def test_branch_activation_write_then_failure_keeps_recovery_basis(tmp_path, monkeypatch, rollback_fails):
    from UPSP.OS.tests.test_instance_service import _ready_layout
    from initialization import instance_service as module
    from initialization.windows_data import ACTIVE_INSTANCE_FILENAME
    import json
    layout, _ = _ready_layout(tmp_path)
    service = module.InstanceService(layout)
    write = service._write_active
    calls = []
    original = OSError("lost activation acknowledgement")
    def fail_after_write(pid, instance_id):
        calls.append(instance_id)
        if len(calls) == 2 and rollback_fails:
            raise OSError("active pointer restore blocked")
        write(pid, instance_id)
        if len(calls) == 1:
            raise original
    monkeypatch.setattr(service, "_write_active", fail_after_write)
    with pytest.raises(UncertainEffectError if rollback_fails else OSError) as caught:
        service.create_branch(mode="new", label="isolated")
    target = layout.pid_root / calls[0]
    if rollback_fails:
        assert target.is_dir()
        assert caught.value.cause is original
        assert str(target) in caught.value.affected_ids
    else:
        assert caught.value is original
        assert not target.exists()
        active = json.loads((layout.data_root / ACTIVE_INSTANCE_FILENAME).read_text(encoding="utf-8"))
        assert active["instance_id"] == layout.instance_id


def test_memory_search_prefix_retains_native_call_id_on_unknown_failure():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
    calls = []
    def search(**kwargs):
        calls.append(kwargs)
        if len(calls) == 2:
            raise TypeError("unknown search fault")
        return {"tool_id": "memory_search", "status": "accepted", "candidates": []}
    dispatcher = ReactionToolSettlementDispatcher(SimpleNamespace(assembler=SimpleNamespace(build_memory_search=search)))
    with pytest.raises(TypeError, match="unknown search fault") as caught:
        dispatcher.handle_memory_search(iter_memory_search_requests=[
            {"query_terms": [name], "call_id": name} for name in ('first', 'second', 'third')],
            accumulated_messages=[], iter_native_tool_call_envelopes=[],
            all_memory_search_receipts=[], all_protocol_tool_receipts=[])
    assert len(calls) == 2
    assert [row['call_id'] for row in caught.value.partial_receipts] == ['first']
