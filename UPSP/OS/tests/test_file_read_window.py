import hashlib

import pytest


def test_file_read_window_uses_configured_max_with_ample_budget():
    from logic.file_read_window import plan_file_read_window

    plan = plan_file_read_window(
        16384,
        {"current_tokens": 61683, "context_window": 1_000_000},
    )

    assert plan["window_chars"] == 16384
    assert plan["window_budget_status"] == "adaptive_max"
    assert plan["window_reserve_tokens"] == 100000


def test_file_read_window_reduces_between_floor_and_max():
    from logic.file_read_window import plan_file_read_window

    plan = plan_file_read_window(
        16384,
        {"current_tokens": 80000, "context_window": 100000},
    )

    assert plan["window_chars"] == 10000
    assert plan["window_budget_status"] == "adaptive_reduced"


def test_file_read_window_keeps_legacy_floor_under_pressure_or_missing_budget():
    from logic.file_read_window import plan_file_read_window

    tight = plan_file_read_window(
        16384,
        {"current_tokens": 88000, "context_window": 100000},
    )
    missing = plan_file_read_window(16384, {})

    assert tight["window_chars"] == 4096
    assert tight["window_budget_status"] == "legacy_floor_context_tight"
    assert tight["window_safe_room_tokens"] == 2000
    assert missing["window_chars"] == 4096
    assert missing["window_budget_status"] == "fallback_missing_or_invalid_budget"


def test_file_read_window_honors_custom_cap_below_legacy_floor():
    from logic.file_read_window import plan_file_read_window

    plan = plan_file_read_window(
        2048,
        {"current_tokens": 1000, "context_window": 100000},
    )

    assert plan["window_chars"] == 2048
    assert plan["window_legacy_floor_chars"] == 2048


def test_file_read_window_is_deterministic():
    from logic.file_read_window import plan_file_read_window

    context = {"current_tokens": 12345, "context_window": 200000}

    assert plan_file_read_window(16384, context) == plan_file_read_window(16384, context)


def test_runtime_file_read_context_reads_latest_provider_usage():
    from logic.file_read_window import runtime_file_read_context

    class Store:
        values = {
            "base.token_usage.current_tokens": 54321,
            "base.token_usage.window_size": 128000,
        }

        def get(self, key, default=None):
            return self.values.get(key, default)

    assert runtime_file_read_context(Store()) == {
        "current_tokens": 54321,
        "context_window": 128000,
    }


def test_invalid_provider_usage_clears_stale_budget_to_legacy_floor():
    from types import SimpleNamespace

    from engines.runtime_services import EngineComponent
    from logic.file_read_window import (
        plan_file_read_window,
        runtime_file_read_context,
    )

    class Store:
        values = {}

        def update_token_usage(self, **values):
            self.values.update({
                "base.token_usage.current_tokens": values["current_tokens"],
                "base.token_usage.window_size": values["window_size"],
                "base.token_usage.last_round_input": values["input_tokens"],
                "base.token_usage.last_round_output": values["output_tokens"],
            })

        def get(self, key, default=None):
            return self.values.get(key, default)

    class Config:
        @staticmethod
        def get_context_window_for_endpoint(_endpoint):
            return 100000

    store = Store()
    component = EngineComponent(SimpleNamespace(sm=store, cfg=Config()))
    component._update_token_usage({
        "tokens_input": 1000,
        "tokens_output": 100,
        "endpoint": "primary",
    })
    component._update_token_usage({
        "tokens_input": None,
        "tokens_output": 50,
        "endpoint": "primary",
    })

    context = runtime_file_read_context(store)
    plan = plan_file_read_window(16384, context)

    assert context == {"current_tokens": 0, "context_window": 100000}
    assert store.get("base.token_usage.last_round_input") == 0
    assert store.get("base.token_usage.last_round_output") == 0
    assert plan["window_chars"] == 4096
    assert plan["window_budget_status"] == "fallback_missing_or_invalid_budget"


def test_file_read_handler_applies_private_runtime_budget(tmp_path, monkeypatch):
    from logic import general_tools
    from logic.file_read_window import RUNTIME_CONTEXT_KEY

    target = tmp_path / "book.md"
    target.write_text(
        "".join(f"line-{index:04d} " + "x" * 90 + "\n" for index in range(300)),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        general_tools,
        "_general_tool_window_params",
        lambda: {
            "file_read_window_chars": 16384,
            "web_fetch_window_chars": 4096,
            "web_search_window_results": 5,
        },
    )

    result = general_tools.execute_general_tool_call(
        {
            "tool_id": "file_read",
            "path": str(target),
            RUNTIME_CONTEXT_KEY: {
                "current_tokens": 61683,
                "context_window": 1_000_000,
            },
        },
        allowed_roots=[tmp_path],
    )

    assert result["status"] == "ok"
    assert result["window_chars"] == 16384
    assert result["window_strategy"] == "context_budget_v1"
    assert result["window_budget_status"] == "adaptive_max"
    assert result["returned_chars"] <= result["window_chars"]
    assert result["has_more"] is True


def test_direct_file_read_without_runtime_budget_keeps_4k(tmp_path, monkeypatch):
    from logic import general_tools

    target = tmp_path / "book.md"
    target.write_text("".join("x" * 90 + "\n" for _ in range(200)), encoding="utf-8")
    monkeypatch.setattr(
        general_tools,
        "_general_tool_window_params",
        lambda: {
            "file_read_window_chars": 16384,
            "web_fetch_window_chars": 4096,
            "web_search_window_results": 5,
        },
    )

    result = general_tools.execute_general_tool_call(
        {"tool_id": "file_read", "path": str(target)},
        allowed_roots=[tmp_path],
    )

    assert result["window_chars"] == 4096
    assert result["window_budget_status"] == "fallback_missing_or_invalid_budget"


def test_complete_overlong_line_remains_existing_exception(tmp_path, monkeypatch):
    from logic import general_tools
    from logic.file_read_window import RUNTIME_CONTEXT_KEY

    target = tmp_path / "one-line.txt"
    content = "x" * 20000
    target.write_text(content, encoding="utf-8")
    monkeypatch.setattr(
        general_tools,
        "_general_tool_window_params",
        lambda: {
            "file_read_window_chars": 16384,
            "web_fetch_window_chars": 4096,
            "web_search_window_results": 5,
        },
    )

    result = general_tools.execute_general_tool_call(
        {
            "tool_id": "file_read",
            "path": str(target),
            RUNTIME_CONTEXT_KEY: {
                "current_tokens": 1000,
                "context_window": 100000,
            },
        },
        allowed_roots=[tmp_path],
    )

    assert result["content"] == content
    assert result["returned_chars"] > result["window_chars"]
    assert result["line_overlong"] is True
    assert result["has_more"] is False


def test_budget_diagnostics_do_not_enter_model_fact_or_material(tmp_path, monkeypatch):
    from logic import general_tools
    from logic.file_read_window import RUNTIME_CONTEXT_KEY

    target = tmp_path / "notes.md"
    target.write_text("alpha\nbeta\n", encoding="utf-8")
    monkeypatch.setattr(
        general_tools,
        "_general_tool_window_params",
        lambda: {
            "file_read_window_chars": 16384,
            "web_fetch_window_chars": 4096,
            "web_search_window_results": 5,
        },
    )
    result = general_tools.execute_general_tool_call(
        {
            "tool_id": "file_read",
            "path": str(target),
            RUNTIME_CONTEXT_KEY: {
                "current_tokens": 1000,
                "context_window": 100000,
            },
        },
        allowed_roots=[tmp_path],
    )

    fact = general_tools.format_general_tool_fact(result)
    material = general_tools.format_general_tool_material_entry(result)

    assert "window_strategy" not in fact
    assert "window_current_tokens" not in fact
    assert material["content"] == "alpha\nbeta\n"
    assert not any(key.startswith("window_") for key in material)


def test_budget_diagnostics_are_removed_before_now_lately_storage():
    from schemas.context import context_safe_read_tool_result

    receipt = {
        "tool_id": "file_read",
        "status": "ok",
        "window_chars": 16384,
        "window_strategy": "context_budget_v1",
        "window_budget_status": "adaptive_max",
        "window_current_tokens": 61683,
        "window_context_window_tokens": 1_000_000,
        "window_reserve_tokens": 100000,
        "window_safe_room_tokens": 838317,
        "window_batch_consumed_before_chars": 0,
        "window_batch_remaining_after_chars": 821933,
        "content": "fixture",
    }

    stored = context_safe_read_tool_result(receipt)

    assert stored["window_chars"] == 16384
    assert "content" not in stored
    assert stored["material_body_sha256"] == hashlib.sha256(
        b"fixture"
    ).hexdigest()
    assert stored["material_body_chars"] == len("fixture")
    assert not any(
        key in stored
        for key in (
            "window_strategy",
            "window_budget_status",
            "window_current_tokens",
            "window_context_window_tokens",
            "window_reserve_tokens",
            "window_safe_room_tokens",
            "window_batch_consumed_before_chars",
            "window_batch_remaining_after_chars",
        )
    )
    assert receipt["window_strategy"] == "context_budget_v1"


def test_all_read_tool_bodies_are_removed_before_cache_storage():
    from schemas.context import context_safe_read_tool_result

    samples = [
        (
            {"tool_id": "file_read", "status": "ok", "content": "正文"},
            "content",
            "正文",
        ),
        (
            {
                "tool_id": "file_search",
                "status": "ok",
                "matches": [{"name": "a.md", "path": "a.md", "is_file": True}],
            },
            "matches",
            "候选路径：\n1. [file] a.md - a.md",
        ),
        (
            {
                "tool_id": "web_fetch",
                "status": "ok",
                "content": "网页正文",
                "content_quality": "degraded",
                "content_quality_reason": "fixture",
            },
            "content",
            (
                "【网页正文质量提示】content_quality=degraded; reason=fixture。"
                "本材料不能作为可靠正文证据。\n\n网页正文"
            ),
        ),
        (
            {
                "tool_id": "web_search",
                "status": "ok",
                "results": [{"title": "X", "url": "https://x"}],
            },
            "results",
            (
                "候选来源（搜索结果不是正文）：\n1. X\n"
                "   url=https://x\n   kind=search_candidate"
            ),
        ),
    ]
    for receipt, body_key, expected_material in samples:
        stored = context_safe_read_tool_result(receipt)
        assert body_key not in stored
        assert stored["material_body_sha256"] == hashlib.sha256(
            expected_material.encode("utf-8")
        ).hexdigest()
        assert stored["material_body_chars"] == len(expected_material)


def test_missing_usage_stops_at_now_soft_watermark_without_deleting_material():
    from engines.general_tool_dispatcher import GeneralToolDispatcher

    dispatcher = GeneralToolDispatcher(execute_fn=lambda call, **kwargs: {
        "tool_id": "file_read",
        "tool_family": "general_tool",
        "tool_class": "read_tool",
        "status": "ok",
        "content": "x" * 4096,
        "returned_chars": 4096,
        "window_legacy_floor_chars": 4096,
    })
    result = dispatcher.handle_requests(
        [{"tool_id": "file_read", "path": "next.md"}],
        [],
        runtime_context={
            "current_tokens": 0,
            "context_window": 0,
            "round_material_chars": 63000,
            "now_budget_chars": 65536,
        },
    )[0]

    assert result["status"] == "rejected"
    assert result["reason"] == "material_budget_unknown"
    assert result["window_budget_status"] == "material_budget_unknown"


def test_provider_preflight_blocks_only_newly_retained_material():
    from logic.file_read_window import provider_material_context_issue

    class State:
        def get(self, key, default=None):
            return {
                "base.token_usage.current_tokens": 85000,
                "base.token_usage.window_size": 100000,
            }.get(key, default)

    class Context:
        def get_round_material_chars(self, round_num, iteration=None):
            assert round_num == 7
            return 8000 if iteration == 2 else 0

    issue = provider_material_context_issue(State(), Context(), 7, 3)
    assert issue["reason"] == "material_context_budget_exhausted"
    assert issue["provider_call_started"] is False
    assert provider_material_context_issue(State(), Context(), 7, 2) == {}


@pytest.mark.parametrize("current_tokens", [100000, 100001])
def test_provider_preflight_fails_closed_when_context_is_full(current_tokens):
    from logic.file_read_window import provider_material_context_issue

    class State:
        def get(self, key, default=None):
            return {
                "base.token_usage.current_tokens": current_tokens,
                "base.token_usage.window_size": 100000,
            }.get(key, default)

    class Context:
        def get_round_material_chars(self, round_num, iteration=None):
            return 4096 if iteration == 1 else 0

    issue = provider_material_context_issue(State(), Context(), 623, 2)

    assert issue["reason"] == "material_context_budget_exhausted"
    assert issue["safe_room_tokens"] == 0
    assert issue["pending_material_chars"] == 4096
    assert issue["provider_call_started"] is False


def test_dispatcher_injects_budget_after_signature_calculation():
    from engines.general_tool_dispatcher import GeneralToolDispatcher
    from logic.file_read_window import RUNTIME_CONTEXT_KEY

    captured = {}

    def fake_execute(call, **_kwargs):
        captured.update(call)
        return {
            "tool_id": "file_read",
            "tool_family": "general_tool",
            "tool_class": "read_tool",
            "status": "ok",
            "content": "fixture",
        }

    result = GeneralToolDispatcher(execute_fn=fake_execute).handle_requests(
        [{"tool_id": "file_read", "path": "fixture.md"}],
        [],
        runtime_context={"current_tokens": 1000, "context_window": 100000},
    )[0]

    assert captured[RUNTIME_CONTEXT_KEY] == {
        "current_tokens": 1000,
        "context_window": 100000,
    }
    assert RUNTIME_CONTEXT_KEY not in result["tool_signature_payload"]["arguments"]
    assert RUNTIME_CONTEXT_KEY not in result["duplicate_guard_payload"]["arguments"]


def test_dispatcher_shares_and_decrements_file_read_batch_budget():
    from engines.general_tool_dispatcher import GeneralToolDispatcher
    from logic.file_read_window import RUNTIME_CONTEXT_KEY, plan_file_read_window

    planned_windows = []

    def fake_execute(call, **_kwargs):
        plan = plan_file_read_window(16384, call[RUNTIME_CONTEXT_KEY])
        planned_windows.append(plan["window_chars"])
        return {
            "tool_id": "file_read",
            "tool_family": "general_tool",
            "tool_class": "read_tool",
            "status": "ok",
            "content": "x" * 4000,
            "returned_chars": 4000,
            **plan,
        }

    results = GeneralToolDispatcher(execute_fn=fake_execute).handle_requests(
        [
            {"tool_id": "file_read", "path": "one.md"},
            {"tool_id": "file_read", "path": "two.md"},
            {"tool_id": "file_read", "path": "three.md"},
        ],
        [],
        runtime_context={"current_tokens": 80000, "context_window": 100000},
    )

    assert planned_windows == [10000, 6000]
    assert [item["status"] for item in results] == ["ok", "ok", "rejected"]
    assert results[0]["window_batch_remaining_after_chars"] == 6000
    assert results[1]["window_batch_remaining_after_chars"] == 2000
    assert results[2]["reason"] == "file_read_batch_budget_exhausted"
    assert all(
        RUNTIME_CONTEXT_KEY not in item["tool_signature_payload"]["arguments"]
        for item in results
    )


def test_tool_settlement_passes_state_budget_to_dispatcher():
    from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher

    class Store:
        def get(self, key, default=None):
            return {
                "base.token_usage.current_tokens": 55555,
                "base.token_usage.window_size": 200000,
            }.get(key, default)

    class Dispatcher:
        context = None

        def handle_requests(
            self,
            _requests,
            _guides,
            prior_results=None,
            runtime_context=None,
        ):
            self.context = runtime_context
            return []

    class Runner:
        sm = Store()
        general_tool_dispatcher = Dispatcher()

        @staticmethod
        def _native_tool_failure_feedbacks(_results):
            return []

        @staticmethod
        def _write_general_tool_results(*_args, **_kwargs):
            return None

    runner = Runner()
    settlement = ReactionToolSettlementDispatcher(runner)
    settlement.handle_general_tool_results(
        iter_general_tool_requests=[],
        active_general_tool_guides=[],
        accumulated_messages=[],
        iter_native_tool_call_envelopes=[],
        all_general_tool_results=[],
        iter_native_feedbacks=[],
        round_num=1,
        iteration=1,
        interaction_meta={},
    )

    assert runner.general_tool_dispatcher.context == {
        "current_tokens": 55555,
        "context_window": 200000,
        "round_num": 1,
        "iteration": 1,
        "frame_id": "R000001:reaction:1",
        "execution_permission_level": "guarded",
    }
