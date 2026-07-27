import json
from pathlib import Path

import pytest


def test_persona_config_missing_fails_closed(tmp_path, monkeypatch):
    from data import config_store
    from errors import ReadError

    missing = tmp_path / "system.json"
    monkeypatch.setitem(
        config_store._CONFIG_MAP,
        "system",
        (str(missing), config_store.default_system_config),
    )
    with pytest.raises(ReadError):
        config_store.ConfigStore().load("system")


def test_connectivity_fifo_limit_must_be_positive():
    from data.config_store import ConfigStore
    from schemas.config import default_system_config

    config = default_system_config()
    config["connectivity"]["max_latency_records"] = 0
    with pytest.raises(ValueError, match="must be positive"):
        ConfigStore._validate("system", config)


def test_memory_heat_uses_one_config_for_creation_decay_and_upgrade():
    from data.stm_heat_calculator import STMHeatCalculator
    from schemas.config import default_memory_config
    from schemas.memory import default_heat_entry

    config = default_memory_config()["heat"]
    assert [
        default_heat_entry(
            weight=weight,
            initial_by_weight=config["initial_by_weight"],
            significant_threshold=config["zone_thresholds"]["significant"],
            uncertain_threshold=config["zone_thresholds"]["uncertain"],
        )["H"]
        for weight in range(1, 6)
    ] == [40, 50, 60, 70, 80]
    calculator = STMHeatCalculator(config)
    update = calculator.tick_decay({
        "MEM-00000001": {
            "H": 80,
            "zone": "显著",
            "AH_high": 2,
            "AH_low": 0,
            "stored": False,
        }
    })
    assert update["MEM-00000001"]["AH_high"] == 3
    assert calculator.check_upgrade(
        {"MEM-00000001": {"AH_high": config["upgrade_high_rounds"], "stored": False}},
        {"MEM-00000001": {"ltm_status": "未归档"}},
    ) == ["MEM-00000001"]


def test_state_store_rejects_missing_bad_values_and_reserved_flags(tmp_path):
    from data.state_store import StateStore
    from errors import ReadError
    from schemas.state import default_state

    path = tmp_path / "state.json"
    store = StateStore(str(path))
    with pytest.raises(ReadError):
        store.load()
    state = default_state()
    state["base"]["core_axes"]["S"] = 101
    path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ReadError):
        store.load()
    state = default_state()
    state["base"]["heartbeat_flags"]["process_down"] = True
    path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    with pytest.raises(ReadError):
        StateStore(str(path)).load()


def _record(key, text):
    return {
        "id": key,
        "role": "user",
        "kind": "interaction",
        "text": text,
        "loc": {"round": 1, "step": "reaction", "iter": 1},
        "policy": {"now": True, "lately": True},
        "ref": {"raw_log_key": key},
    }


def test_corpus_merges_jsonl_and_derives_markdown(tmp_path):
    from data.chronicle_store import CorpusStore

    store = CorpusStore()
    store.corpus_dir = str(tmp_path / "Corpus")
    rhythms = Path(store.corpus_dir) / "public" / "rhythms"
    rhythms.mkdir(parents=True)
    record = _record("K1", "同一事实")
    for index in (1, 2):
        (rhythms / f"{index}.jsonl").write_text(
            json.dumps(record, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    target = Path(store.merge_layer("rhythms", "daily"))
    rows = [json.loads(line) for line in target.read_text(encoding="utf-8").splitlines()]
    assert rows == [record]
    assert target.with_suffix(".md").is_file()
    assert not list(rhythms.glob("*"))


def test_corpus_ignores_derived_source_block_id_when_content_matches(tmp_path):
    from data.chronicle_store import CorpusStore

    store = CorpusStore()
    store.corpus_dir = str(tmp_path / "Corpus")
    rhythms = Path(store.corpus_dir) / "public" / "rhythms"
    rhythms.mkdir(parents=True)
    for index in (1, 2):
        record = _record("K1", "同一事实")
        record["ref"]["source_block_id"] = f"R1-system-{index:04d}"
        (rhythms / f"{index}.jsonl").write_text(
            json.dumps(record, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )

    target = Path(store.merge_layer("rhythms", "daily"))

    assert len(target.read_text(encoding="utf-8").splitlines()) == 1


def test_corpus_conflicting_key_fails_without_deleting_sources(tmp_path):
    from data.chronicle_store import CorpusStore

    store = CorpusStore()
    store.corpus_dir = str(tmp_path / "Corpus")
    rhythms = Path(store.corpus_dir) / "public" / "rhythms"
    rhythms.mkdir(parents=True)
    for index, text in enumerate(("甲", "乙"), 1):
        (rhythms / f"{index}.jsonl").write_text(
            json.dumps(_record("K1", text), ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
    with pytest.raises(ValueError, match="raw_log_key conflict"):
        store.merge_layer("rhythms", "daily")
    assert len(list(rhythms.glob("*.jsonl"))) == 2


def test_corpus_conflicting_metadata_fails_closed():
    from data.chronicle_store import dedupe_corpus_records

    first = _record("K1", "同一正文")
    second = json.loads(json.dumps(first, ensure_ascii=False))
    second["policy"]["lately"] = False
    with pytest.raises(ValueError, match="raw_log_key conflict"):
        dedupe_corpus_records([first, second])


def test_attic_moves_only_expired_yearly_pairs_by_stable_filename(
        tmp_path, monkeypatch):
    from datetime import datetime, timezone
    from data import chronicle_store as chronicle_module

    monkeypatch.setattr(
        chronicle_module,
        "local_now",
        lambda: datetime(2026, 7, 27, tzinfo=timezone.utc),
    )
    store = chronicle_module.CorpusStore()
    store.corpus_dir = str(tmp_path / "Corpus")
    yearly = Path(store.corpus_dir) / "public" / "yearly"
    rhythms = Path(store.corpus_dir) / "public" / "rhythms"
    yearly.mkdir(parents=True)
    rhythms.mkdir(parents=True)
    expired = yearly / "merged_20220101_000000_000000.jsonl"
    recent = yearly / "merged_20250101_000000_000000.jsonl"
    low_layer = rhythms / "merged_20200101_000000_000000.jsonl"
    store._write_pair(str(expired), [_record("K1", "旧年事实")])
    store._write_pair(str(recent), [_record("K2", "近年事实")])
    store._write_pair(str(low_layer), [_record("K3", "低层仍由保留期管理")])

    moved = store.move_to_attic()

    target = (
        Path(store.corpus_dir)
        / "Attic"
        / "2022"
        / "attic-2022.jsonl"
    )
    assert moved == [str(target)]
    assert target.is_file()
    assert target.with_suffix(".md").is_file()
    assert not expired.exists()
    assert not expired.with_suffix(".md").exists()
    assert recent.exists()
    assert low_layer.exists()


def test_raw_log_archives_one_rhythm_pair_then_clears_buffer(tmp_path):
    from data.context_store import ContextStore

    raw_jsonl = tmp_path / "STM" / "buffer" / "raw_log.jsonl"
    raw_md = raw_jsonl.with_suffix(".md")
    rhythms = tmp_path / "LTM" / "Corpus" / "public" / "rhythms"
    store = ContextStore(
        raw_log_jsonl=str(raw_jsonl),
        raw_log_md=str(raw_md),
        corpus_rhythms_dir=str(rhythms),
    )
    first = _record("K1", "第一段")
    last = _record("K2", "第二段")
    last["loc"]["round"] = 3

    store._mirror_lately_blocks_to_raw_log([first, last])
    assert len(raw_jsonl.read_text(encoding="utf-8").splitlines()) == 2
    assert not rhythms.exists()

    archived = Path(store.archive_raw_log())

    assert archived.name.startswith("rhythm_")
    assert archived.name.endswith("_R000001-R000003.jsonl")
    assert archived.with_suffix(".md").is_file()
    assert raw_jsonl.read_text(encoding="utf-8") == ""
    assert raw_md.read_text(encoding="utf-8") == "<!-- 原始语料备份 -->\n"


def test_local_time_helpers_are_offset_aware():
    from datetime import datetime
    from constants import local_fromtimestamp, local_now

    now = local_now()
    assert now.tzinfo is not None
    assert now.utcoffset() is not None
    stamp = 1_767_225_600
    assert local_fromtimestamp(stamp).utcoffset() == (
        datetime.fromtimestamp(stamp).astimezone().utcoffset()
    )


def test_reserved_flags_do_not_enter_model_visible_contracts():
    root = Path(__file__).resolve().parents[2] / "initialization" / "persona_template"
    content = "\n".join(
        path.read_text(encoding="utf-8")
        for folder in ("rules", "docs")
        for path in (root / folder).rglob("*.md")
    )
    for field in ("fatigue_expired", "identity_timeout", "process_down"):
        assert field not in content


def test_persona_skeleton_uses_raw_log_and_corpus_rhythms():
    from initialization.persona_initializer import (
        REQUIRED_TEMPLATE_DIRS,
        REQUIRED_TEMPLATE_FILES,
    )

    root = Path(__file__).resolve().parents[2] / "initialization" / "persona_template"
    assert "LTM/Corpus/public/rhythms" in REQUIRED_TEMPLATE_DIRS
    assert "LTM/Corpus/raw_logs" not in REQUIRED_TEMPLATE_DIRS
    assert "STM/buffer/raw_log.jsonl" in REQUIRED_TEMPLATE_FILES
    assert "STM/buffer/raw_log.md" in REQUIRED_TEMPLATE_FILES
    assert (root / "LTM/Corpus/public/rhythms").is_dir()
    assert not (root / "LTM/Corpus/raw_logs").exists()
