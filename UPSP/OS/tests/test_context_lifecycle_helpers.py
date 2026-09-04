import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


def test_persistent_lanes_are_disjoint_and_material_is_no_raw_lane():
    from data.context_store import ContextStore

    lanes = ContextStore()._persistent_lanes()
    assert set(lanes["now_lately_raw"]).isdisjoint(lanes["now_lately_no_raw"])
    assert lanes["now_lately_no_raw"] == ["material"]
    assert "reasoning_context" not in set().union(*map(set, lanes.values()))


def test_legacy_round_pinned_material_normalizes_to_regular_persistent_material():
    from data.context_store import ContextStore

    entry = ContextStore._normalize_entry({
        "round": 12,
        "role": "system",
        "kind": "material",
        "content": "legacy material",
        "round_retention": "round_pinned",
    })

    assert "round_retention" not in entry
    assert ContextStore._is_call_transient(entry) is False


def test_legacy_now_only_reasoning_normalizes_to_one_target_reaction_call():
    from data.context_store import ContextStore

    entry = ContextStore._normalize_entry({
        "round": 12,
        "role": "assistant",
        "kind": "reasoning_context",
        "content": "reasoning",
        "iter": 3,
        "native_replay": {"provider": "openai_chat"},
    })

    assert entry["call_transient"] is True
    assert entry["transient_scope"] == "reasoning_replay"
    assert entry["transient_target_step"] == "reaction"
    assert entry["transient_target_iteration"] == 5


def test_persistent_append_rejects_unknown_keyword_arguments():
    from data.context_store import ContextStore

    with pytest.raises(TypeError, match="unexpected keyword argument 'lately'"):
        ContextStore().append_to_cache(
            12, "system", "ordinary material", kind="material", lately=False,
        )


def test_call_transient_requires_scope_and_explicit_target_step():
    from data.context_store import ContextStore

    with pytest.raises(ValueError, match="scope 与目标步骤"):
        ContextStore().append_call_transient(
            12, "system", "cleanup material", transient_scope="cleanup_round",
        )
