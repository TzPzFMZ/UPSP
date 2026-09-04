import hashlib
import json
from pathlib import Path

import pytest

import data.context_store as context_store_module
from data.context_store import ContextStore


def _write_jsonl(path, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "".join(json.dumps(row, ensure_ascii=False) + "\n" for row in rows),
        encoding="utf-8",
    )


def _block(text="仅供推理的文字", *, block_id="R000007-assistant-0001"):
    raw_key = hashlib.sha256(b"proof-key").hexdigest()
    return {
        "id": block_id,
        "role": "assistant",
        "kind": "dialogue_progress",
        "text": text,
        "loc": {"round": 7, "step": "reaction", "iter": 2, "time": "t"},
        "policy": {"now": True, "lately": True},
        "ref": {
            "active_corpus_id": "C-00019",
            "source_block_id": "R000007-assistant-source",
            "raw_log_key": raw_key,
        },
    }


def _audit(text="仅供推理的文字", *, content_chars=0, progress=None):
    frame = "R000007:reaction:2"
    stream = "stream-1"
    progress = text if progress is None else progress
    payloads = [
        ("llm_stream_delta", {
            "stream_id": stream,
            "reasoning_delta": text,
            "content_chars": content_chars,
        }),
        ("llm_stream_done", {
            "stream_id": stream,
            "attempt_status": "completed",
            "content_chars": content_chars,
            "reasoning_chars": len(text),
        }),
        ("llm_output_raw", {"response": text}),
        ("llm_output_parsed", {
            "assistant_progress": progress,
            "assistant_reply": "",
        }),
    ]
    return [
        {
            "schema_version": "round_audit.v2",
            "event_id": f"R000007-{index:06d}",
            "event_index": index,
            "event_type": event_type,
            "frame_id": frame,
            "iteration": 2,
            "phase": "reaction",
            "round": 7,
            "payload": payload,
        }
        for index, (event_type, payload) in enumerate(payloads, 1)
    ]


def _store(tmp_path):
    cache = tmp_path / "cache"
    raw_dir = tmp_path / "buffer"
    return ContextStore(
        cache_dir=str(cache),
        now_cache_jsonl=str(cache / "now_cache.jsonl"),
        lately_cache_jsonl=str(cache / "lately_cache.jsonl"),
        raw_log_jsonl=str(raw_dir / "raw_log.jsonl"),
        raw_log_md=str(raw_dir / "raw_log.md"),
    )


def _fixture(tmp_path, *, audit_rows=None, with_debt=True):
    store = _store(tmp_path)
    block = _block()
    _write_jsonl(Path(store._now_cache_jsonl()), [block])
    _write_jsonl(Path(store._lately_cache_jsonl()), [block])
    _write_jsonl(Path(store._raw_log_jsonl()), [block])
    Path(store._raw_log_md()).write_text(
        store._render_raw_log_md([block]), encoding="utf-8"
    )
    rounds = tmp_path / "round"
    _write_jsonl(
        rounds / "round_7.jsonl",
        _audit() if audit_rows is None else audit_rows,
    )
    Path(store.active_corpus_meta_path()).parent.mkdir(parents=True, exist_ok=True)
    Path(store.active_corpus_meta_path()).write_text(
        json.dumps({
            "schema_version": "active_corpus_meta.v1",
            "next_short_id": 20,
            "interaction_round_count": 3,
        }), encoding="utf-8"
    )
    if with_debt:
        Path(store.cache_compaction_debt_path()).write_text(
            json.dumps({
                "schema_version": "cache_compaction_debt.v3",
                "status": "open",
            }), encoding="utf-8"
        )
    return store, rounds, block


def test_ready_repair_removes_only_proven_echo_and_discards_open_debt(tmp_path):
    store, rounds, block = _fixture(tmp_path)

    receipt = store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert receipt["schema_version"] == "reasoning_progress_repair.v1"
    assert receipt["status"] == "repaired"
    assert receipt["proven_count"] == 1
    assert receipt["removed_block_ids"] == [block["id"]]
    assert receipt["removed_active_corpus_ids"] == ["C-00019"]
    assert receipt["compaction_debt_discarded"] is True
    assert receipt["checkpoint"] == "complete"
    assert Path(store.reasoning_progress_repair_checkpoint_path()).is_file()
    assert store._current_now_blocks() == []
    assert store._current_lately_blocks() == []
    assert store._current_raw_log_blocks() == []
    assert not Path(store.cache_compaction_debt_path()).exists()
    meta = json.loads(Path(store.active_corpus_meta_path()).read_text(encoding="utf-8"))
    assert meta["next_short_id"] == 20

    second = store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))
    assert second["status"] == "noop"
    assert second["candidate_count"] == 0
    assert second["proven_count"] == 0


def test_ready_repair_checkpoint_skips_all_cache_and_audit_reads(
        tmp_path, monkeypatch):
    store, rounds, _ = _fixture(tmp_path, with_debt=False)
    store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    def unexpected(*_args, **_kwargs):
        raise AssertionError("completed repair must not rescan historical truth")

    monkeypatch.setattr(store, "_current_now_blocks", unexpected)
    monkeypatch.setattr(store, "_current_lately_blocks", unexpected)
    monkeypatch.setattr(store, "_current_raw_log_blocks", unexpected)
    monkeypatch.setattr(
        store, "_read_reasoning_progress_audit_events", unexpected)

    receipt = store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert receipt["status"] == "noop"
    assert receipt["checkpoint"] == "complete"


def test_ready_repair_rejects_corrupt_checkpoint(tmp_path):
    store, rounds, _ = _fixture(tmp_path, with_debt=False)
    checkpoint = Path(store.reasoning_progress_repair_checkpoint_path())
    checkpoint.parent.mkdir(parents=True, exist_ok=True)
    checkpoint.write_text("{}", encoding="utf-8")

    with pytest.raises(Exception, match="reasoning_progress_repair_checkpoint_invalid"):
        store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))


@pytest.mark.parametrize("audit_rows", [
    [],
    _audit(content_chars=1),
    _audit(progress="真实的另一段进展"),
])
def test_ready_repair_keeps_candidate_when_proof_is_incomplete(tmp_path, audit_rows):
    store, rounds, block = _fixture(tmp_path, audit_rows=audit_rows, with_debt=False)

    receipt = store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert receipt["status"] == "noop"
    assert receipt["proven_count"] == 0
    assert store._current_lately_blocks()[0]["text"] == block["text"]


def test_ready_repair_keeps_candidate_when_historical_audit_is_broken(tmp_path):
    store, rounds, block = _fixture(tmp_path, with_debt=False)
    (rounds / "round_7.jsonl").write_text("{broken", encoding="utf-8")

    receipt = store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert receipt["status"] == "noop"
    assert receipt["unproven_count"] == 1
    assert store._current_lately_blocks()[0]["text"] == block["text"]


def test_ready_repair_fails_closed_on_raw_key_conflict(tmp_path):
    store, rounds, block = _fixture(tmp_path, with_debt=False)
    conflict = _block("另一段正文", block_id="R000007-assistant-0002")
    conflict["ref"]["raw_log_key"] = block["ref"]["raw_log_key"]
    _write_jsonl(Path(store._raw_log_jsonl()), [block, conflict])

    with pytest.raises(Exception, match="reasoning_progress_raw_key_conflict"):
        store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))


@pytest.mark.parametrize("failed_path_name", [
    "now_cache.jsonl",
    "lately_cache.jsonl",
    "raw_log.jsonl",
])
def test_ready_repair_rolls_back_jsonl_write_failures(
        tmp_path, monkeypatch, failed_path_name):
    store, rounds, _ = _fixture(tmp_path)
    tracked = [
        Path(store._now_cache_jsonl()),
        Path(store._lately_cache_jsonl()),
        Path(store._raw_log_jsonl()),
        Path(store._raw_log_md()),
        Path(store.cache_compaction_debt_path()),
    ]
    before = {path: path.read_bytes() for path in tracked}
    original = store._write_jsonl_atomic

    def fail(path, rows):
        if Path(path).name == failed_path_name:
            raise RuntimeError("injected-jsonl-failure")
        return original(path, rows)

    monkeypatch.setattr(store, "_write_jsonl_atomic", fail)
    with pytest.raises(RuntimeError, match="injected-jsonl-failure"):
        store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert {path: path.read_bytes() for path in tracked} == before
    assert not Path(store.reasoning_progress_repair_checkpoint_path()).exists()


def test_ready_repair_rolls_back_debt_clear_failure(tmp_path, monkeypatch):
    store, rounds, _ = _fixture(tmp_path)
    tracked = [
        Path(store._now_cache_jsonl()),
        Path(store._lately_cache_jsonl()),
        Path(store._raw_log_jsonl()),
        Path(store._raw_log_md()),
        Path(store.cache_compaction_debt_path()),
    ]
    before = {path: path.read_bytes() for path in tracked}

    def fail():
        raise RuntimeError("injected-debt-failure")

    monkeypatch.setattr(store, "clear_cache_compaction_debt", fail)
    with pytest.raises(RuntimeError, match="injected-debt-failure"):
        store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert {path: path.read_bytes() for path in tracked} == before
    assert not Path(store.reasoning_progress_repair_checkpoint_path()).exists()


def test_ready_repair_rolls_back_checkpoint_write_failure(tmp_path, monkeypatch):
    store, rounds, _ = _fixture(tmp_path)
    tracked = [
        Path(store._now_cache_jsonl()),
        Path(store._lately_cache_jsonl()),
        Path(store._raw_log_jsonl()),
        Path(store._raw_log_md()),
        Path(store.cache_compaction_debt_path()),
    ]
    before = {path: path.read_bytes() for path in tracked}
    original = store._write_json_atomic

    def fail(path, value):
        if Path(path) == Path(store.reasoning_progress_repair_checkpoint_path()):
            raise RuntimeError("injected-checkpoint-failure")
        return original(path, value)

    monkeypatch.setattr(store, "_write_json_atomic", fail)
    with pytest.raises(RuntimeError, match="injected-checkpoint-failure"):
        store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert {path: path.read_bytes() for path in tracked} == before
    assert not Path(store.reasoning_progress_repair_checkpoint_path()).exists()


def test_ready_repair_rolls_back_raw_markdown_write_failure(tmp_path, monkeypatch):
    store, rounds, _ = _fixture(tmp_path)
    tracked = [
        Path(store._now_cache_jsonl()),
        Path(store._lately_cache_jsonl()),
        Path(store._raw_log_jsonl()),
        Path(store._raw_log_md()),
        Path(store.cache_compaction_debt_path()),
    ]
    before = {path: path.read_bytes() for path in tracked}
    original = context_store_module.atomic_write_text
    failed = False

    def fail_once(path, *args, **kwargs):
        nonlocal failed
        if Path(path) == Path(store._raw_log_md()) and not failed:
            failed = True
            raise RuntimeError("injected-raw-md-failure")
        return original(path, *args, **kwargs)

    monkeypatch.setattr(context_store_module, "atomic_write_text", fail_once)
    with pytest.raises(RuntimeError, match="injected-raw-md-failure"):
        store.reconcile_reasoning_progress_on_startup(round_dir=str(rounds))

    assert {path: path.read_bytes() for path in tracked} == before
    assert not Path(store.reasoning_progress_repair_checkpoint_path()).exists()
