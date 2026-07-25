import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))


class MemoryStateStore:
    def __init__(self):
        self.state = {
            "base": {
                "runtime": {},
                "heartbeat_flags": {"continue_requested": False},
            }
        }

    def get(self, path, default=None):
        node = self.state
        for part in path.split("."):
            if not isinstance(node, dict) or part not in node:
                return default
            node = node[part]
        return node

    def set(self, path, value):
        node = self.state
        parts = path.split(".")
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value

    def load(self):
        return self.state


def test_spec359_multiple_relay_intents_do_not_overwrite_each_other():
    from logic.relay_intent_pool import create_relay_intent, open_relay_intents

    sm = MemoryStateStore()

    first = create_relay_intent(
        sm,
        source_round=12,
        handoff_text="继续整理书中概念。",
        reaction_finalize_id="rf-1",
    )
    second = create_relay_intent(
        sm,
        source_round=13,
        handoff_text="补充上一轮未完成的索引检查。",
        reaction_finalize_id="rf-2",
    )

    assert first["relay_intent_id"] != second["relay_intent_id"]
    assert [item["handoff_text"] for item in open_relay_intents(sm.load())] == [
        "继续整理书中概念。",
        "补充上一轮未完成的索引检查。",
    ]


def test_spec360_relay_intent_settle_updates_only_target_intent():
    from logic.relay_intent_pool import create_relay_intent, settle_relay_intent, open_relay_intents

    sm = MemoryStateStore()
    first = create_relay_intent(sm, source_round=20, handoff_text="任务甲", reaction_finalize_id="rf-a")
    create_relay_intent(sm, source_round=21, handoff_text="任务乙", reaction_finalize_id="rf-b")

    receipt = settle_relay_intent(
        sm,
        {
            "relay_intent_id": first["relay_intent_id"],
            "status": "completed",
            "note": "已完成任务甲。",
        },
        round_num=22,
    )

    assert receipt["status"] == "applied"
    assert [item["handoff_text"] for item in open_relay_intents(sm.load())] == ["任务乙"]


def test_spec490_relay_intent_settle_accepts_blocked_status():
    from logic.relay_intent_pool import create_relay_intent, settle_relay_intent, open_relay_intents

    sm = MemoryStateStore()
    intent = create_relay_intent(sm, source_round=490, handoff_text="失败中继")

    receipt = settle_relay_intent(
        sm,
        {
            "relay_intent_id": intent["relay_intent_id"],
            "status": "blocked",
            "note": "Runtime 死路，不能继续。",
        },
        round_num=491,
    )

    assert receipt["status"] == "applied"
    assert receipt["final_status"] == "blocked"
    assert open_relay_intents(sm.load()) == []


def test_spec490_settle_open_relay_intents_consumes_all_open_items():
    from logic.relay_intent_pool import (
        create_relay_intent,
        open_relay_intents,
        settle_open_relay_intents,
    )

    sm = MemoryStateStore()
    create_relay_intent(sm, source_round=490, handoff_text="任务甲")
    create_relay_intent(sm, source_round=491, handoff_text="任务乙")

    receipt = settle_open_relay_intents(
        sm,
        status="completed",
        round_num=492,
        note="relay finish",
        source="unit_test",
    )

    assert receipt["status"] == "applied"
    assert receipt["final_status"] == "completed"
    assert len(receipt["settled_relay_intent_ids"]) == 2
    assert open_relay_intents(sm.load()) == []


def test_spec363_relay_intent_id_uses_persistent_sequence_not_pool_length():
    from logic.relay_intent_pool import create_relay_intent

    sm = MemoryStateStore()
    first = create_relay_intent(sm, source_round=30, handoff_text="任务甲")
    sm.set("base.runtime.relay_intents", [])
    second = create_relay_intent(sm, source_round=30, handoff_text="任务乙")

    assert first["relay_intent_id"] != second["relay_intent_id"]
    assert second["relay_intent_id"].endswith("-N002")


def test_spec363_open_relay_intents_render_as_visible_context_material():
    from logic.relay_intent_pool import create_relay_intent, render_open_relay_intents_for_context

    sm = MemoryStateStore()
    create_relay_intent(sm, source_round=40, handoff_text="继续读书。")

    text = render_open_relay_intents_for_context(sm.load())

    assert "REMINDER｜中继规划池" in text
    assert "RLY-R000040-N001" in text
    assert "继续读书。" not in text
    assert "隐藏 payload" in text
