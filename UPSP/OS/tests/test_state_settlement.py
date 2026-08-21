from copy import deepcopy
from datetime import datetime, timedelta

import pytest

from constants import RELATION_AXIS_KEYS, TZ_SHANGHAI
from data.round_snapshot_store import RoundSnapshotStore
from data.state_store import StateStore
from logic.state_settlement import (
    StateSettlementError,
    settle_due_state,
    settle_state,
)


class FakeRelationStore:
    def __init__(self, *subjects, fail_once=None):
        self.cards = {
            subject: {
                "id": subject,
                "axes": {axis: 0 for axis in RELATION_AXIS_KEYS},
                "last_state_settlement_id": None,
            }
            for subject in subjects
        }
        self.fail_once = fail_once
        self.calls = []

    def resolve_active_subject(self, value):
        return value if value in self.cards else None

    def read_card(self, subject):
        card = self.cards.get(subject)
        return deepcopy(card) if card else None

    def apply_state_settlement(self, subject, axes, settlement_id,
                               observed_at=None):
        card = self.cards[subject]
        before = dict(card["axes"])
        self.calls.append(subject)
        if card["last_state_settlement_id"] == settlement_id:
            return {
                "status": "already_applied",
                "card_id": subject,
                "settlement_id": settlement_id,
                "before": before,
                "after": before,
            }
        if self.fail_once == subject:
            self.fail_once = None
            raise OSError("simulated relation write failure")
        card["axes"] = dict(axes)
        card["last_state_settlement_id"] = settlement_id
        return {
            "status": "applied",
            "card_id": subject,
            "settlement_id": settlement_id,
            "before": before,
            "after": dict(axes),
        }


def _stores(tmp_path, round_num=1, relations=None):
    state = StateStore(str(tmp_path / "state.json"))
    state.init_if_missing()
    rounds = RoundSnapshotStore(
        str(tmp_path / "context"),
        static_projection_enabled=False,
    )
    rounds.start_round(round_num, "interactive")
    return state, relations or FakeRelationStore(), rounds


def _receipt(mem_id="MEM-01", interaction=None, relationships=None):
    return {
        "status": "applied",
        "mem_id": mem_id,
        "interaction_feelings": list(interaction or []),
        "relationship_feelings": list(relationships or []),
    }


@pytest.mark.parametrize(
    "round_type", ["interactive", "rhythm", "relay", "autonomous", "standby"])
def test_every_round_type_settles_without_memory(tmp_path, round_type):
    state, relations, rounds = _stores(tmp_path)

    receipt = settle_state(
        state, relations, rounds, 1, round_type,
        user_input_text="外部输入" if round_type == "interactive" else "",
    )

    assert receipt["status"] == "applied"
    base = state.load()["base"]
    assert base["dynamic_axes"]["focus"]["value"] == 0
    assert base["dynamic_axes"]["safety"]["value"] == 0
    assert base["dynamic_axes"]["arousal"]["value"] == 0
    assert base["workhood_index"]["value"] > 0
    assert base["meta"]["last_state_settlement_id"] == "SS-R000001"


def test_memory_round_closes_dynamic_relation_buffer_and_audit(tmp_path):
    relations = FakeRelationStore("REL-A")
    state, relations, rounds = _stores(tmp_path, relations=relations)
    receipt = _receipt(
        interaction=["核心判断被推翻"],
        relationships=[{"subject": "REL-A", "word": "信任"}],
    )

    result = settle_state(
        state, relations, rounds, 1, "interactive",
        memory_write_receipts=[receipt],
        user_input_text="这是真实外部输入",
        observed_at=datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI),
    )

    base = state.load()["base"]
    assert result["source_memory_ids"] == ["MEM-01"]
    assert base["dynamic_axes"]["arousal"]["value"] == 3
    assert base["feeling_buffer"]
    assert relations.cards["REL-A"]["axes"]["trust"] == 3
    event_types = [event["event_type"] for event in rounds.read_events(1)]
    assert event_types.count("state_settle_plan") == 1
    assert event_types.count("state_settle_receipt") == 1


def test_duplicate_receipt_and_cleanup_do_not_accumulate(tmp_path):
    state, relations, rounds = _stores(tmp_path)
    receipt = _receipt(interaction=["专注"])

    first = settle_state(
        state, relations, rounds, 1, "interactive",
        memory_write_receipts=[receipt, deepcopy(receipt)],
        user_input_text="输入",
    )
    after_first = state.load()
    second = settle_state(
        state, relations, rounds, 1, "interactive",
        memory_write_receipts=[receipt],
        user_input_text="输入",
    )

    assert first == second
    assert state.load() == after_first
    assert after_first["base"]["dynamic_axes"]["focus"]["value"] == 2
    events = rounds.read_events(1)
    assert sum(e["event_type"] == "state_settle_plan" for e in events) == 1
    assert sum(e["event_type"] == "state_settle_receipt" for e in events) == 1


def test_noninteractive_round_does_not_advance_buffer_but_time_does(tmp_path):
    start = datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI)
    state, relations, rounds = _stores(tmp_path)
    settle_state(
        state, relations, rounds, 1, "interactive",
        memory_write_receipts=[_receipt(interaction=["核心判断被推翻"])],
        user_input_text="输入",
        observed_at=start,
    )

    rounds.start_round(2, "relay")
    settle_state(
        state, relations, rounds, 2, "relay",
        observed_at=start + timedelta(minutes=1),
    )
    assert state.load()["base"]["feeling_buffer"][0][
        "interactive_rounds_elapsed"] == 0

    rounds.start_round(3, "standby")
    settle_state(
        state, relations, rounds, 3, "standby",
        observed_at=start + timedelta(minutes=5),
    )
    base = state.load()["base"]
    assert base["dynamic_axes"]["arousal"]["value"] == 5
    assert base["feeling_buffer"] == []


def test_only_real_interactive_input_advances_round_trigger(tmp_path):
    start = datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI)
    state, relations, rounds = _stores(tmp_path)
    settle_state(
        state, relations, rounds, 1, "interactive",
        memory_write_receipts=[_receipt(interaction=["核心判断被推翻"])],
        user_input_text="输入",
        observed_at=start,
    )
    for round_num, round_type, text in (
        (2, "interactive", ""),
        (3, "rhythm", "内部轮"),
        (4, "interactive", "第二次真实输入"),
    ):
        rounds.start_round(round_num, round_type)
        settle_state(
            state, relations, rounds, round_num, round_type,
            user_input_text=text,
            observed_at=start + timedelta(minutes=round_num - 1),
        )
    assert state.load()["base"]["feeling_buffer"][0][
        "interactive_rounds_elapsed"] == 1


def test_rhythm_with_explicit_external_trigger_advances_buffer(tmp_path):
    start = datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI)
    state, relations, rounds = _stores(tmp_path)
    settle_state(
        state, relations, rounds, 1, "interactive",
        memory_write_receipts=[_receipt(interaction=["核心判断被推翻"])],
        user_input_text="输入",
        observed_at=start,
    )
    rounds.start_round(2, "rhythm")
    settle_state(
        state, relations, rounds, 2, "rhythm",
        user_input_text="被节律优先级合并的外部输入",
        external_interaction=True,
        observed_at=start + timedelta(minutes=1),
    )
    assert state.load()["base"]["feeling_buffer"][0][
        "interactive_rounds_elapsed"] == 1


def test_invalid_buffer_fails_closed_without_data_loss(tmp_path):
    state, relations, rounds = _stores(tmp_path)
    current = state.load()
    current["base"]["feeling_buffer"] = [{"legacy": True}]
    state.save(current)
    rounds.start_round(1, "relay")

    with pytest.raises(StateSettlementError):
        settle_state(
            state, relations, rounds, 1, "relay",
            observed_at=datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI),
        )

    assert state.load()["base"]["feeling_buffer"] == [{"legacy": True}]


def test_corrupt_round_audit_fails_closed_and_writes_error_receipt(tmp_path):
    state, relations, rounds = _stores(tmp_path)
    round_path = rounds._round_path(1)
    with open(round_path, "a", encoding="utf-8") as handle:
        handle.write("{broken-json\n")

    with pytest.raises(StateSettlementError) as caught:
        settle_state(
            state, relations, rounds, 1, "relay",
            observed_at=datetime(2026, 7, 18, tzinfo=TZ_SHANGHAI),
        )

    assert "JSONDecodeError" in caught.value.receipt["reason"]
    assert state.load()["base"]["meta"]["last_state_settlement_id"] is None
    assert '"status":"error"' in open(
        round_path, "r", encoding="utf-8").read()


def test_partial_relation_failure_reuses_absolute_plan(tmp_path):
    relations = FakeRelationStore("REL-A", "REL-B", fail_once="REL-B")
    state, relations, rounds = _stores(tmp_path, relations=relations)
    receipt = _receipt(relationships=[
        {"subject": "REL-A", "word": "可靠"},
        {"subject": "REL-B", "word": "可靠"},
    ])

    with pytest.raises(StateSettlementError):
        settle_state(
            state, relations, rounds, 1, "interactive",
            memory_write_receipts=[receipt], user_input_text="输入")
    assert relations.cards["REL-A"]["axes"]["trust"] == 2
    assert relations.cards["REL-B"]["axes"]["trust"] == 0
    assert state.load()["base"]["meta"]["last_state_settlement_id"] is None

    result = settle_state(
        state, relations, rounds, 1, "interactive",
        memory_write_receipts=[receipt], user_input_text="输入")
    assert result["status"] == "applied"
    assert relations.cards["REL-A"]["axes"]["trust"] == 2
    assert relations.cards["REL-B"]["axes"]["trust"] == 2
    assert relations.calls.count("REL-A") == 2
    assert state.load()["base"]["meta"]["last_state_settlement_id"] == (
        "SS-R000001")
    events = rounds.read_events(1)
    assert sum(e["event_type"] == "state_settle_plan" for e in events) == 1


def test_stale_partial_plan_cannot_overwrite_newer_settlement(tmp_path):
    relations = FakeRelationStore("REL-A", "REL-B", fail_once="REL-B")
    state, relations, rounds = _stores(tmp_path, relations=relations)
    receipt = _receipt(relationships=[
        {"subject": "REL-A", "word": "可靠"},
        {"subject": "REL-B", "word": "可靠"},
    ])

    with pytest.raises(StateSettlementError):
        settle_state(
            state, relations, rounds, 1, "interactive",
            memory_write_receipts=[receipt], user_input_text="输入")

    current = state.load()
    current["base"]["meta"]["total_round"] = 2
    state.save(current)
    rounds.start_round(2, "relay")
    settle_state(state, relations, rounds, 2, "relay")
    state_after_round_2 = state.load()
    relations_after_round_2 = deepcopy(relations.cards)

    with pytest.raises(StateSettlementError) as caught:
        settle_state(
            state, relations, rounds, 1, "interactive",
            memory_write_receipts=[receipt], user_input_text="输入")

    assert "stale_state_settlement_replay" in caught.value.receipt["reason"]
    assert state.load() == state_after_round_2
    assert relations.cards == relations_after_round_2


def test_completed_old_settlement_remains_idempotent_after_newer_round(tmp_path):
    state, relations, rounds = _stores(tmp_path)
    first = settle_state(state, relations, rounds, 1, "relay")
    current = state.load()
    current["base"]["meta"]["total_round"] = 2
    state.save(current)
    rounds.start_round(2, "relay")
    settle_state(state, relations, rounds, 2, "relay")
    state_after_round_2 = state.load()

    assert settle_state(state, relations, rounds, 1, "relay") == first
    assert state.load() == state_after_round_2


def test_due_feeling_settles_locally_without_incrementing_round(tmp_path):
    start = datetime(2026, 7, 24, 20, 0, tzinfo=TZ_SHANGHAI)
    state, relations, rounds = _stores(tmp_path)
    first = settle_state(
        state,
        relations,
        rounds,
        1,
        "interactive",
        memory_write_receipts=[_receipt(interaction=["核心判断被推翻"])],
        user_input_text="输入",
        observed_at=start,
    )
    state.set_flag("feeling_settle_due", True)
    total_before = state.get("base.meta.total_round")

    receipt = settle_due_state(
        state,
        relations,
        journal_path=tmp_path / "state_settlement_journal.json",
        observed_at=start + timedelta(minutes=5),
    )

    base = state.load()["base"]
    assert receipt["status"] == "applied"
    assert receipt["schema_version"] == "state_settle_local_receipt.v1"
    assert base["meta"]["total_round"] == total_before
    assert base["heartbeat_flags"]["feeling_settle_due"] is False
    assert base["meta"]["last_state_settlement_id"].startswith("SS-T")
    assert settle_state(state, relations, rounds, 1, "interactive") == first


def test_stale_due_flag_clears_without_state_drift(tmp_path):
    state, relations, _rounds = _stores(tmp_path)
    before = state.load()["base"]["dynamic_axes"]
    current = state.load()
    current["base"]["heartbeat_flags"]["feeling_settle_due"] = True
    current["base"]["meta"]["next_settle_at"] = (
        datetime(2026, 7, 24, 19, 59, tzinfo=TZ_SHANGHAI).isoformat()
    )
    state.save(current)

    receipt = settle_due_state(
        state,
        relations,
        journal_path=tmp_path / "state_settlement_journal.json",
        observed_at=datetime(2026, 7, 24, 20, 0, tzinfo=TZ_SHANGHAI),
    )

    assert receipt["status"] == "skipped"
    assert receipt["reason"] == "no_due_feeling_buffer"
    assert state.load()["base"]["dynamic_axes"] == before
    assert state.get("base.heartbeat_flags.feeling_settle_due") is False
    assert state.get("base.meta.next_settle_at") is None


def test_local_settlement_reuses_plan_after_partial_relation_failure(tmp_path):
    now = datetime(2026, 7, 24, 20, 0, tzinfo=TZ_SHANGHAI)
    relations = FakeRelationStore("REL-A", "REL-B", fail_once="REL-B")
    state, relations, _rounds = _stores(tmp_path, relations=relations)
    current = state.load()
    current["base"]["heartbeat_flags"]["feeling_settle_due"] = True
    current["base"]["feeling_buffer"] = [
        {
            "buffer_id": "FB-A",
            "memory_id": "MEM-A",
            "domain": "relation",
            "subject": "REL-A",
            "axis": "trust",
            "delta": 2,
            "remaining_settlements": 1,
            "next_settle_at": now.isoformat(),
            "settle_rounds": 2,
            "interactive_rounds_elapsed": 0,
        },
        {
            "buffer_id": "FB-B",
            "memory_id": "MEM-B",
            "domain": "relation",
            "subject": "REL-B",
            "axis": "trust",
            "delta": 2,
            "remaining_settlements": 1,
            "next_settle_at": now.isoformat(),
            "settle_rounds": 2,
            "interactive_rounds_elapsed": 0,
        },
    ]
    current["base"]["meta"]["next_settle_at"] = now.isoformat()
    state.save(current)
    journal = tmp_path / "state_settlement_journal.json"

    with pytest.raises(StateSettlementError):
        settle_due_state(
            state, relations, journal_path=journal, observed_at=now)
    assert relations.cards["REL-A"]["axes"]["trust"] == 2
    assert relations.cards["REL-B"]["axes"]["trust"] == 0

    receipt = settle_due_state(
        state, relations, journal_path=journal, observed_at=now)

    assert receipt["status"] == "applied"
    assert relations.cards["REL-A"]["axes"]["trust"] == 2
    assert relations.cards["REL-B"]["axes"]["trust"] == 2
    assert relations.calls.count("REL-A") == 2
    assert state.get("base.heartbeat_flags.feeling_settle_due") is False


def test_round_recovers_partial_local_settlement_before_new_plan(tmp_path):
    now = datetime(2026, 7, 24, 20, 0, tzinfo=TZ_SHANGHAI)
    relations = FakeRelationStore("REL-A", "REL-B", fail_once="REL-B")
    state, relations, rounds = _stores(tmp_path, relations=relations)
    current = state.load()
    current["base"]["heartbeat_flags"]["feeling_settle_due"] = True
    current["base"]["feeling_buffer"] = [
        {
            "buffer_id": f"FB-{subject}",
            "memory_id": f"MEM-{subject}",
            "domain": "relation",
            "subject": subject,
            "axis": "trust",
            "delta": 2,
            "remaining_settlements": 1,
            "next_settle_at": now.isoformat(),
            "settle_rounds": 2,
            "interactive_rounds_elapsed": 0,
        }
        for subject in ("REL-A", "REL-B")
    ]
    current["base"]["meta"]["next_settle_at"] = now.isoformat()
    state.save(current)

    with pytest.raises(StateSettlementError):
        settle_due_state(state, relations, observed_at=now)
    assert relations.cards["REL-A"]["axes"]["trust"] == 2
    assert relations.cards["REL-B"]["axes"]["trust"] == 0

    settle_state(
        state,
        relations,
        rounds,
        1,
        "interactive",
        user_input_text="继续",
        observed_at=now,
        external_interaction=True,
    )

    assert relations.cards["REL-A"]["axes"]["trust"] == 2
    assert relations.cards["REL-B"]["axes"]["trust"] == 2
    assert state.get("base.meta.last_state_settlement_id") == "SS-R000001"


def test_invalid_utf8_local_journal_fails_closed(tmp_path):
    state, relations, _rounds = _stores(tmp_path)
    journal = tmp_path / "state_settlement_journal.json"
    journal.write_bytes(b"\xff")

    with pytest.raises(StateSettlementError) as caught:
        settle_due_state(state, relations, journal_path=journal)

    assert "UnicodeDecodeError" in caught.value.receipt["reason"]
