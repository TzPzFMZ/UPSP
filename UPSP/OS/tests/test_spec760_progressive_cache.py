import json

import hashlib
import json

import pytest

from assembly.context_helpers import render_corpus_entry_for_context
from data.context_store import ContextStore
from data.config_store import ConfigStore
from errors import ReadError, WriteError
from logic.progressive_cache_compaction import (
    current_batch,
    render_materials,
)
from logic.guide_submit import apply_guide_submit
from logic.reaction_obligations import ReactionObligationTracker
from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub


class Spec760Config(ConfigStoreStub):
    def get_lately_cache_params(self):
        return {
            "pressure_ratio": 0.9,
            "protected_interaction_count": 1,
            "semantic_summary_ratio": 0.125,
            "cycle_target_ratio": 0.25,
            "batch_source_chars": 1024,
        }


def _store(tmp_path):
    return ContextStore(
        config_store=Spec760Config(),
        cache_dir=str(tmp_path / "cache"),
        now_cache_jsonl=str(tmp_path / "cache" / "now_cache.jsonl"),
        lately_cache_jsonl=str(tmp_path / "cache" / "lately_cache.jsonl"),
        raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
    )


def _block(block_id, role, kind, text, interaction_index):
    return {
        "id": block_id,
        "role": role,
        "kind": kind,
        "text": text,
        "loc": {"round": interaction_index, "step": "reaction", "iter": 1,
                "time": "2026-08-18T00:00:00+08:00"},
        "policy": {"now": False, "lately": True},
        "ref": {"interaction_round_index": interaction_index},
    }


def test_spec760_freezes_lately_and_preserves_protected_user_until_apply(tmp_path):
    store = _store(tmp_path)
    blocks = [
        _block("u1", "user", "interaction", "第一问原文", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 400, 1),
        _block("u2", "user", "interaction", "第二问原文", 2),
        _block("a2", "assistant", "assistant_reply", "乙" * 400, 2),
    ]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)
    before = open(store._lately_cache_jsonl(), "rb").read()
    prepared = store.prepare_lately_pressure_compaction(
        760,
        {"kind": "token_ratio", "input_tokens": 900,
         "context_window": 1000, "round_context_window_tokens": 1000},
    )
    assert prepared["status"] == "prepared"
    assert open(store._lately_cache_jsonl(), "rb").read() == before
    debt = prepared["debt"]
    assert debt["schema_version"] == "cache_compaction_debt.v3"
    assert debt["groups"][-1]["protected"] is True
    assert "第二问原文" not in "\n".join(
        item["content"] for item in render_materials(debt))

    while store.load_cache_compaction_debt():
        debt = store.load_cache_compaction_debt()
        results = [{
            "shard_id": shard["shard_id"],
            "action": "replace",
            "semantic_content": "摘要",
        } for shard in current_batch(debt)]
        receipt = store.stage_progressive_cache_compaction(
            results, current_round=761)
        assert receipt["status"] == "applied"

    after = store._current_lately_blocks()
    assert any(item["role"] == "user" and item["text"] == "第二问原文"
               for item in after)
    assert any(item["kind"] == "interaction_summary" for item in after)
    repeated = store.prepare_lately_pressure_compaction(
        762,
        {"kind": "token_ratio", "input_tokens": 900,
         "context_window": 1000, "round_context_window_tokens": 1000},
    )
    assert repeated["reason"] == "cache_compaction_terminal_fingerprint_unchanged"


def test_spec760_partial_batch_remains_open_and_unknown_id_is_structural_reject(tmp_path):
    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 1400, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        760, {"kind": "token_ratio", "context_window": 1000}
    )["debt"]
    first = current_batch(debt)[0]
    rejected = store.stage_progressive_cache_compaction([{
        "shard_id": "CCS-unknown", "action": "replace",
        "semantic_content": "摘要",
    }])
    assert rejected["status"] == "rejected"
    staged = store.stage_progressive_cache_compaction([{
        "shard_id": first["shard_id"], "action": "replace",
        "semantic_content": "摘要",
    }])
    assert staged["status"] == "applied"
    assert store.load_cache_compaction_debt()


def test_spec765_next_cycle_keeps_compacted_interaction_as_an_interaction_anchor(
        tmp_path):
    store = _store(tmp_path)
    existing_summary = _block(
        "cache-compaction-R000760-old:interaction:1",
        "system",
        "interaction_summary",
        "第一轮历史交互摘要",
        1,
    )
    existing_summary["ref"].update({
        "compact_reason": "progressive_lately_pressure",
        "source_group_id": "interaction:1",
        "source_round_start": 1,
        "source_round_end": 1,
    })
    blocks = [
        existing_summary,
        _block("u2", "user", "interaction", "第二问原文", 2),
        _block("a2", "assistant", "assistant_reply", "乙" * 400, 2),
        _block("u3", "user", "interaction", "第三问原文", 3),
        _block("a3", "assistant", "assistant_reply", "丙" * 400, 3),
    ]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)

    debt = store.prepare_lately_pressure_compaction(
        765, {"kind": "token_ratio", "context_window": 1000}
    )["debt"]

    assert [item["group_id"] for item in debt["groups"]] == [
        "interaction:1", "interaction:2", "interaction:3"]
    assert debt["groups"][0]["user_block_id"] == ""
    assert debt["groups"][0]["protected"] is False
    assert debt["groups"][-1]["protected"] is True

    while store.load_cache_compaction_debt():
        current = store.load_cache_compaction_debt()
        receipt = store.stage_progressive_cache_compaction(
            [{
                "shard_id": shard["shard_id"],
                "action": "replace",
                "semantic_content": "继续压缩后的历史交互摘要",
            } for shard in current_batch(current)],
            current_round=766,
            current_reaction_iteration=2,
        )
        assert receipt["status"] == "applied"

    after = store._current_lately_blocks()
    assert not any(item["kind"] == "cache_summary" for item in after)
    assert all(
        item["kind"] in {"interaction", "interaction_summary"}
        for item in after
    )


def test_spec765_legacy_interaction_summary_without_index_never_becomes_cache_summary(
        tmp_path):
    store = _store(tmp_path)
    legacy = _block(
        "legacy-interaction-summary", "system", "interaction_summary",
        "缺少交互序号的旧历史交互摘要", 0,
    )
    legacy["ref"].pop("interaction_round_index", None)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        legacy,
        _block("prefix", "system", "setup_fact", "历史背景" * 200, 0),
    ])
    debt = store.prepare_lately_pressure_compaction(
        765, {"kind": "token_ratio", "context_window": 1000}
    )["debt"]

    receipt = store.stage_progressive_cache_compaction(
        [{
            "shard_id": shard["shard_id"],
            "action": "replace",
            "semantic_content": "旧历史交互与背景的重整摘要",
        } for shard in current_batch(debt)],
        current_round=766,
        current_reaction_iteration=3,
    )

    assert receipt["rewrite_applied"] is True
    summaries = [
        item for item in store._current_lately_blocks()
        if item["kind"].endswith("summary")
    ]
    assert summaries
    assert all(item["kind"] == "interaction_summary" for item in summaries)


def test_spec765_v3_projects_tool_receipts_and_keeps_material_as_semantic_source(
        tmp_path):
    store = _store(tmp_path)
    tool = _block(
        "t1", "system", "tool_fact",
        "重复渲染的长工具回执不应进入投影" * 20, 1,
    )
    tool["ref"]["tool_result"] = {
        "tool_id": "file_read",
        "call_id": "call-765",
        "status": "ok",
        "path": "docs/source.md",
        "chars": 321,
        "sha256": "a" * 64,
        "credential": "must-not-leak",
    }
    material_text = "原始资料正文必须作为语义压缩来源"
    blocks = [
        _block("u1", "user", "interaction", "问题", 1),
        tool,
        _block("m1", "system", "material", material_text, 1),
    ]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)

    debt = store.prepare_lately_pressure_compaction(
        765, {"kind": "token_ratio", "context_window": 1000})["debt"]
    projection = "\n".join(
        shard["projected_content"] for shard in debt["shards"]
    )

    assert '"tool_id": "file_read"' in projection
    assert '"call_id": "call-765"' in projection
    assert "重复渲染的长工具回执" not in projection
    assert "must-not-leak" not in projection
    assert material_text in projection


def test_spec760_guide_submit_preempts_other_guides_and_blocks_closeout(tmp_path):
    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 400, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        760, {"kind": "token_ratio", "context_window": 1000}
    )["debt"]

    class Workbench:
        def current_active_guide_id(self):
            return "some-task-guide"

    rejected = apply_guide_submit(
        Workbench(), {"guide_id": "some-task-guide"},
        evidence_context={"context_store": store, "round_num": 761},
    )
    assert rejected["status"] == "rejected"
    assert rejected["reason"] == "cache_compaction_pending"
    tracker = ReactionObligationTracker(context_store=store)
    closeout = tracker.validate_closeout_form({
        "closeout_decision": "finish",
        "memory_status": "not_applicable",
        "read_status": "not_applicable",
        "pending_status": "none",
    })
    assert closeout["blocked"] is True
    assert tracker.pending_types() == ["cache_compaction_pending"]


def test_spec765_guide_submit_records_context_store_transaction_and_iteration(
        tmp_path):
    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 400, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]

    class Workbench:
        pass

    receipt = apply_guide_submit(
        Workbench(),
        {
            "guide_id": f"cache_compaction:{debt['compaction_id']}",
            "item_id": "cache_compaction_due",
            "option_id": "submit_cache_compaction_batch",
            "fields": {
                "results": [{
                    "shard_id": shard["shard_id"],
                    "action": "replace",
                    "semantic_content": "旧问答摘要",
                } for shard in current_batch(debt)],
            },
        },
        evidence_context={
            "context_store": store,
            "round_num": 765,
            "current_reaction_iteration": 7,
        },
    )

    assert receipt["status"] == "applied"
    assert receipt["cache_compaction"]["rewrite_applied"] is True
    assert receipt["backend_receipts"] == [{
        "schema_version": "cache_compaction_batch_receipt.v3",
        "operation_id": "progressive_cache_compaction",
        **receipt["cache_compaction"],
    }]
    assert all(item["loc"]["iter"] == 7 for item in store._current_lately_blocks()
               if item["kind"] in {"cache_summary", "interaction_summary"})


def test_spec760_round_window_uses_smallest_primary_and_rejects_unknown():
    store = object.__new__(ConfigStore)
    api = {
        "step_routes": {
            "setup": ["s", "s-backup"],
            "reaction": ["r"],
            "cleanup": ["c"],
        },
        "endpoints": {
            "s": {"context_window": 200000},
            "s-backup": {"context_window": 32000},
            "r": {"context_window": 128000},
            "c": {"context_window": 160000},
        },
    }
    store.load = lambda name: api
    assert store.get_round_context_window_tokens() == 128000
    api["endpoints"]["c"]["context_window"] = 0
    try:
        store.get_round_context_window_tokens()
    except ValueError as exc:
        assert str(exc) == "primary_context_window_unknown:cleanup"
    else:
        raise AssertionError("unknown primary window must fail closed")


def test_spec760_keep_never_becomes_fifo_deletion_candidate(tmp_path):
    store = _store(tmp_path)
    blocks = [
        _block("u1", "user", "interaction", "必须保留的原问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 900, 1),
    ]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)
    debt = store.prepare_lately_pressure_compaction(
        760, {"kind": "token_ratio", "context_window": 1000})["debt"]
    while store.load_cache_compaction_debt():
        receipt = store.stage_progressive_cache_compaction([{
            "shard_id": shard["shard_id"],
            "action": "keep",
            "semantic_content": "",
            "reason": "事实不可无损缩短",
        } for shard in current_batch(store.load_cache_compaction_debt())])
        assert receipt["status"] == "applied"
    after = store._current_lately_blocks()
    assert [(item["id"], item["kind"], item["text"]) for item in after] == [
        (item["id"], item["kind"], item["text"]) for item in blocks]


def test_spec760_applies_after_oldest_groups_reach_target_and_keeps_newer_raw(tmp_path):
    store = _store(tmp_path)
    blocks = [
        _block("u1", "user", "interaction", "一", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 500, 1),
        _block("u2", "user", "interaction", "二", 2),
        _block("a2", "assistant", "assistant_reply", "乙" * 500, 2),
        _block("u3", "user", "interaction", "三", 3),
        _block("a3", "assistant", "assistant_reply", "丙" * 100, 3),
    ]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)
    debt = store.prepare_lately_pressure_compaction(
        760, {"kind": "token_ratio", "context_window": 1000})["debt"]
    batch = current_batch(debt)
    receipt = store.stage_progressive_cache_compaction([{
        "shard_id": shard["shard_id"],
        "action": "replace",
        "semantic_content": "摘",
    } for shard in batch[:2]])
    assert receipt["rewrite_applied"] is True
    assert store.load_cache_compaction_debt() == {}
    after = store._current_lately_blocks()
    assert any(item["id"] == "a3" and item["text"] == "丙" * 100 for item in after)
    assert not any(item["id"] in {"a1", "a2"} for item in after)


def test_spec760_v1_is_read_only_until_next_pressure_replans(tmp_path):
    store = _store(tmp_path)
    legacy = {
        "schema_version": "cache_compaction_debt.v1",
        "status": "open",
        "completed_shards": ["shard_01"],
    }
    store._write_json_atomic(store.cache_compaction_debt_path(), legacy)
    assert store.recover_cache_compaction_debt() == legacy
    assert not hasattr(store, "stage_cache_compaction_shard")


def test_spec765_summary_kinds_coordinates_sources_and_rendering_are_truthful(tmp_path):
    store = _store(tmp_path)
    prefix = _block("p1", "system", "setup_fact", "前情" * 80, 0)
    first_user = _block("u1", "user", "interaction", "第一问原文", 1)
    first_reply = _block("a1", "assistant", "assistant_reply", "甲" * 240, 1)
    second_user = _block("u2", "user", "interaction", "第二问原文", 2)
    second_reply = _block("a2", "assistant", "assistant_reply", "乙" * 240, 2)
    for block, round_num in zip(
            (prefix, first_user, first_reply, second_user, second_reply),
            (3, 5, 6, 9, 10)):
        block["loc"]["round"] = round_num
    blocks = [prefix, first_user, first_reply, second_user, second_reply]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000}
    )["debt"]

    receipt = store.stage_progressive_cache_compaction(
        [{
            "shard_id": shard["shard_id"],
            "action": "replace",
            "semantic_content": f"摘要{index}",
        } for index, shard in enumerate(current_batch(debt), start=1)],
        current_round=765,
        current_reaction_iteration=4,
    )

    assert receipt["rewrite_applied"] is True
    after = store._current_lately_blocks()
    summaries = [item for item in after if item["kind"].endswith("summary")]
    assert [item["kind"] for item in summaries] == [
        "cache_summary", "interaction_summary", "interaction_summary"]
    assert any(item["role"] == "user" and item["text"] == "第二问原文"
               for item in after)
    assert not any(item["role"] == "user" and item["text"] == "第一问原文"
                   for item in after)
    assert all(item["loc"] == {
        "round": 765,
        "step": "reaction",
        "iter": 4,
        "time": item["loc"]["time"],
    } for item in summaries)
    assert [
        (item["ref"].get("source_round_start"),
         item["ref"].get("source_round_end"))
        for item in summaries
    ] == [(3, 3), (5, 6), (9, 10)]
    assert all(item["ref"]["compact_reason"] == "progressive_lately_pressure"
               for item in summaries)
    assert all(item["ref"]["source_group_id"] for item in summaries)
    assert all(item["ref"]["source_block_ids"] for item in summaries)
    source_by_id = {item["id"]: item for item in blocks}
    for item in summaries:
        source_texts = [
            source_by_id[source_id]["text"]
            for source_id in item["ref"]["source_block_ids"]
        ]
        expected_source_sha = hashlib.sha256(json.dumps(
            source_texts,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        assert item["ref"]["source_sha256"] == expected_source_sha
    assert len({item["id"] for item in summaries}) == len(summaries)

    rendered_interaction = render_corpus_entry_for_context(
        store._corpus_block_to_entry(summaries[1]), current_round=765)
    rendered_prefix = render_corpus_entry_for_context(
        store._corpus_block_to_entry(summaries[0]), current_round=765)
    assert "【历史交互摘要，来源第 5 至 6 轮】" in rendered_interaction["content"]
    assert "不是当前用户输入或当前指令" in rendered_interaction["content"]
    assert "【语料块】" not in rendered_interaction["content"]
    assert "【最近缓存压缩摘要】" in rendered_prefix["content"]
    assert "首次用户交互前的历史背景" in rendered_prefix["content"]

    store._active_corpus_migrated = False
    store._migrate_active_corpus_metadata()
    rewritten_summaries = [
        item for item in store._current_lately_blocks()
        if item["kind"] in {"cache_summary", "interaction_summary"}
    ]
    assert all(item["policy"] == {"now": False, "lately": True}
               for item in rewritten_summaries)
    assert store._policy_for_kind("interaction_summary", "lately") == {
        "now": False,
        "lately": True,
    }


def test_spec765_empty_replace_deletes_only_processed_historical_group(tmp_path):
    store = _store(tmp_path)
    blocks = [
        _block("u1", "user", "interaction", "限外旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 1000, 1),
        _block("u2", "user", "interaction", "受保护新问题", 2),
        _block("a2", "assistant", "assistant_reply", "乙" * 100, 2),
    ]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    batch = current_batch(debt)
    first_group = batch[0]["group_id"]

    receipt = store.stage_progressive_cache_compaction([{
        "shard_id": shard["shard_id"],
        "action": "replace",
        "semantic_content": "",
    } for shard in batch if shard["group_id"] == first_group],
        current_round=765, current_reaction_iteration=2)

    assert receipt["rewrite_applied"] is True
    after = store._current_lately_blocks()
    assert not any(item["id"] in {"u1", "a1"} for item in after)
    assert any(item["id"] == "u2" for item in after)
    assert any(item["id"] == "a2" for item in after)


@pytest.mark.parametrize("failure_stage", (
    "applying_debt", "meta", "lately", "terminal_debt",
))
def test_spec765_v3_apply_failure_restores_every_source_file(
        tmp_path, monkeypatch, failure_stage):
    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 500, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    paths = {
        "debt": store.cache_compaction_debt_path(),
        "meta": store.active_corpus_meta_path(),
        "lately": store._lately_cache_jsonl(),
    }
    before = {
        name: open(path, "rb").read() if __import__("os").path.isfile(path) else None
        for name, path in paths.items()
    }
    failed = {"value": False}
    original_json = store._write_json_atomic
    original_jsonl = store._write_jsonl_atomic

    def write_json(path, value):
        should_fail = (
            failure_stage == "meta" and path == paths["meta"]
            or failure_stage == "applying_debt" and path == paths["debt"]
            and value.get("phase") == "applying"
            or failure_stage == "terminal_debt" and path == paths["debt"]
            and value.get("status") == "closed"
        )
        if should_fail and not failed["value"]:
            failed["value"] = True
            raise OSError(failure_stage)
        return original_json(path, value)

    def write_jsonl(path, values):
        if failure_stage == "lately" and path == paths["lately"] and not failed["value"]:
            failed["value"] = True
            raise OSError(failure_stage)
        return original_jsonl(path, values)

    monkeypatch.setattr(store, "_write_json_atomic", write_json)
    monkeypatch.setattr(store, "_write_jsonl_atomic", write_jsonl)
    with pytest.raises(OSError, match=failure_stage):
        store.stage_progressive_cache_compaction(
            [{
                "shard_id": shard["shard_id"],
                "action": "replace",
                "semantic_content": "摘要",
            } for shard in current_batch(debt)],
            current_round=765,
            current_reaction_iteration=3,
        )

    assert failed["value"] is True
    for name, path in paths.items():
        current = open(path, "rb").read() if __import__("os").path.isfile(path) else None
        assert current == before[name]


def test_spec765_v3_rejects_incomplete_source_map_without_rewriting_lately(
        tmp_path):
    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 500, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    debt["groups"][0]["source_block_ids"].remove("a1")
    store._write_json_atomic(store.cache_compaction_debt_path(), debt)
    lately_before = open(store._lately_cache_jsonl(), "rb").read()

    with pytest.raises(ReadError, match="cache_compaction_source_map_invalid"):
        store.recover_cache_compaction_debt()

    assert open(store._lately_cache_jsonl(), "rb").read() == lately_before
    assert store.load_cache_compaction_record()["status"] == "open"


@pytest.mark.parametrize("silent_stage", (
    "applying_debt", "meta", "lately", "terminal_debt",
))
def test_spec765_v3_apply_readback_rejects_silent_write_loss(
        tmp_path, monkeypatch, silent_stage):
    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 500, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    paths = {
        "debt": store.cache_compaction_debt_path(),
        "meta": store.active_corpus_meta_path(),
        "lately": store._lately_cache_jsonl(),
    }
    before = {
        name: open(path, "rb").read() if __import__("os").path.isfile(path) else None
        for name, path in paths.items()
    }
    skipped = {"value": False}
    original_json = store._write_json_atomic
    original_jsonl = store._write_jsonl_atomic

    def write_json(path, value):
        should_skip = (
            silent_stage == "meta" and path == paths["meta"]
            or silent_stage == "applying_debt" and path == paths["debt"]
            and value.get("phase") == "applying"
            or silent_stage == "terminal_debt" and path == paths["debt"]
            and value.get("status") == "closed"
        )
        if should_skip and not skipped["value"]:
            skipped["value"] = True
            return None
        return original_json(path, value)

    def write_jsonl(path, values):
        if silent_stage == "lately" and path == paths["lately"] and not skipped["value"]:
            skipped["value"] = True
            return None
        return original_jsonl(path, values)

    monkeypatch.setattr(store, "_write_json_atomic", write_json)
    monkeypatch.setattr(store, "_write_jsonl_atomic", write_jsonl)
    with pytest.raises(WriteError, match="verification_failed"):
        store.stage_progressive_cache_compaction(
            [{
                "shard_id": shard["shard_id"],
                "action": "replace",
                "semantic_content": "摘要",
            } for shard in current_batch(debt)],
            current_round=765,
            current_reaction_iteration=3,
        )

    assert skipped["value"] is True
    for name, path in paths.items():
        current = open(path, "rb").read() if __import__("os").path.isfile(path) else None
        assert current == before[name]


def test_spec765_open_v3_preserves_tail_appended_after_freeze(tmp_path):
    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 900, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    tail = _block("tail", "assistant", "assistant_reply", "债务建立后的新尾部", 1)
    store._write_jsonl_atomic(
        store._lately_cache_jsonl(), store._current_lately_blocks() + [tail])
    tail_line_before = open(store._lately_cache_jsonl(), "rb").read().splitlines()[-1]

    receipt = store.stage_progressive_cache_compaction(
        [{
            "shard_id": shard["shard_id"],
            "action": "replace",
            "semantic_content": "旧问答摘要",
        } for shard in current_batch(debt)],
        current_round=765,
        current_reaction_iteration=5,
    )

    assert receipt["rewrite_applied"] is True
    after = store._read_jsonl_strict(
        store._lately_cache_jsonl(), label="lately_cache")
    assert after[-1] == tail
    assert open(store._lately_cache_jsonl(), "rb").read().splitlines()[-1] == tail_line_before


def test_spec765_hard_crash_after_lately_write_recovers_terminal_fingerprint(
        tmp_path, monkeypatch):
    from data.progressive_cache_compaction import source_fingerprint

    class HardCrash(BaseException):
        pass

    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 900, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    original_write = store._write_json_atomic

    def crash_before_terminal_record(path, value):
        if (
                path == store.cache_compaction_debt_path()
                and value.get("status") == "closed"):
            raise HardCrash()
        return original_write(path, value)

    monkeypatch.setattr(store, "_write_json_atomic", crash_before_terminal_record)
    with pytest.raises(HardCrash):
        store.stage_progressive_cache_compaction(
            [{
                "shard_id": shard["shard_id"],
                "action": "replace",
                "semantic_content": "旧问答摘要",
            } for shard in current_batch(debt)],
            current_round=765,
            current_reaction_iteration=6,
        )
    assert store.load_cache_compaction_record()["phase"] == "applying"

    monkeypatch.setattr(store, "_write_json_atomic", original_write)
    assert store.recover_cache_compaction_debt() == {}
    terminal = store.load_cache_compaction_record()
    current = store._current_lately_blocks()
    assert terminal["status"] == "closed"
    assert terminal["source_fingerprint"] == source_fingerprint(
        current,
        terminal["policy"],
        terminal["logical_window_tokens"],
    )


def test_spec765_recovery_readback_rejects_silent_meta_write_loss(
        tmp_path, monkeypatch):
    class HardCrash(BaseException):
        pass

    store = _store(tmp_path)
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 900, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    original_write = store._write_json_atomic

    def crash_before_terminal_record(path, value):
        if (
                path == store.cache_compaction_debt_path()
                and value.get("status") == "closed"):
            raise HardCrash()
        return original_write(path, value)

    monkeypatch.setattr(store, "_write_json_atomic", crash_before_terminal_record)
    with pytest.raises(HardCrash):
        store.stage_progressive_cache_compaction(
            [{
                "shard_id": shard["shard_id"],
                "action": "replace",
                "semantic_content": "旧问答摘要",
            } for shard in current_batch(debt)],
            current_round=765,
            current_reaction_iteration=6,
        )
    assert store.load_cache_compaction_record()["phase"] == "applying"

    monkeypatch.setattr(store, "_write_json_atomic", original_write)
    store._write_json_atomic(store.active_corpus_meta_path(), {
        "schema_version": store.ACTIVE_CORPUS_META_SCHEMA,
        "next_short_id": 1,
        "interaction_round_count": 0,
    })

    def skip_meta_write(path, value):
        if path == store.active_corpus_meta_path():
            return None
        return original_write(path, value)

    monkeypatch.setattr(store, "_write_json_atomic", skip_meta_write)
    with pytest.raises(
            WriteError,
            match="cache_compaction_recovery_meta_verification_failed"):
        store.recover_cache_compaction_debt()
    assert store.load_cache_compaction_record()["phase"] == "applying"

    monkeypatch.setattr(store, "_write_json_atomic", original_write)
    assert store.recover_cache_compaction_debt() == {}


def test_spec765_unicode_shards_and_exact_summary_limit(tmp_path):
    store = _store(tmp_path)
    source = "甲乙丙丁🙂" * 300
    store._write_jsonl_atomic(store._lately_cache_jsonl(), [
        _block("u1", "user", "interaction", "问题", 1),
        _block("a1", "assistant", "assistant_reply", source, 1),
    ])
    debt = store.prepare_lately_pressure_compaction(
        764, {"kind": "token_ratio", "context_window": 1000})["debt"]
    shards = debt["shards"]
    assert len(shards) >= 2
    source_spans = [
        span for shard in shards for span in shard["source_spans"]
        if span["block_id"] == "a1"
    ]
    assert source_spans[0]["start"] == 0
    assert source_spans[-1]["end"] == len(source)
    assert all(left["end"] == right["start"]
               for left, right in zip(source_spans, source_spans[1:]))
    assert "".join(source[item["start"]:item["end"]]
                   for item in source_spans) == source
    assert all("\ufffd" not in shard["projected_content"] for shard in shards)
    first = current_batch(debt)[0]
    rejected = store.stage_progressive_cache_compaction([{
        "shard_id": first["shard_id"],
        "action": "replace",
        "semantic_content": "摘" * (first["target_chars"] + 1),
    }])
    assert rejected["status"] == "rejected"
    assert rejected["rejected_results"][0]["reason"] == "semantic_content_too_long"
    accepted = store.stage_progressive_cache_compaction([{
        "shard_id": first["shard_id"],
        "action": "replace",
        "semantic_content": "摘" * first["target_chars"],
    }])
    assert accepted["status"] == "applied"
    assert store.load_cache_compaction_debt()


def test_spec765_known_v2_is_verified_then_replanned_as_v3(tmp_path):
    store = _store(tmp_path)
    blocks = [
        _block("u1", "user", "interaction", "旧问题", 1),
        _block("a1", "assistant", "assistant_reply", "甲" * 500, 1),
    ]
    store._write_jsonl_atomic(store._lately_cache_jsonl(), blocks)
    normalized_blocks = store._current_lately_blocks()
    store._write_json_atomic(store.cache_compaction_debt_path(), {
        "schema_version": "cache_compaction_debt.v2",
        "status": "open",
        "phase": "staging",
        "fifo": {
            "after_block_count": len(normalized_blocks),
            "after_sha256": store._lately_blocks_sha256(normalized_blocks),
        },
        "staged_summaries": [{"legacy": True}],
    })

    prepared = store.prepare_lately_pressure_compaction(
        765, {"kind": "token_ratio", "context_window": 1000})

    assert prepared["status"] == "prepared"
    debt = prepared["debt"]
    assert debt["schema_version"] == "cache_compaction_debt.v3"
    assert debt["legacy_migration"] == {
        "schema_version": "cache_compaction_debt.v2",
        "phase": "staging",
        "discarded_staged_summaries": 1,
    }


def test_spec765_unknown_compaction_schema_fails_closed(tmp_path):
    from errors import ReadError

    store = _store(tmp_path)
    store._write_json_atomic(store.cache_compaction_debt_path(), {
        "schema_version": "cache_compaction_debt.v999",
        "status": "open",
    })

    with pytest.raises(ReadError, match="unsupported_cache_compaction_debt"):
        store.load_cache_compaction_debt()
