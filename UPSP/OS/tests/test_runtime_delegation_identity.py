import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, ScriptedExecutor, logical_step


class TestRuntimeDelegationIdentity(RuntimeTestMixin):
    class AnchorRelationStore:
        cards = [
            {"id": "REL-USER", "name": "用户", "aliases": ["本人"],
             "category": "ours", "status": "active"},
            {"id": "REL-GUEST", "name": "访客", "aliases": ["Guest"],
             "category": "them", "status": "active"},
            {"id": "REL-OLD", "name": "旧对象", "aliases": [],
             "category": "them", "status": "archived"},
        ]

        def load_registry(self):
            return {"cards": list(self.cards)}

        def resolve_active_subject(self, value):
            text = str(value or "").strip().lower()
            matches = []
            for card in self.cards:
                if card["status"] != "active":
                    continue
                names = [card["id"], card["name"], *card.get("aliases", [])]
                if text in {str(item).lower() for item in names}:
                    matches.append(card["id"])
            return matches[0] if len(matches) == 1 else None

    def test_runtime_relation_anchor_interfaces_keep_default_and_instance_separate(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.relation_store = self.AnchorRelationStore()

        default_receipt = rt.set_local_default_relation("本人")
        assert default_receipt == {
            "schema": "interaction_anchor_receipt.v1",
            "action": "set_local_default_relation",
            "status": "applied",
            "relation_id": "REL-USER",
            "display_name": "用户",
            "reason": "",
        }
        assert rt.sm.get("base.identity.local_default_relation_id") == "REL-USER"
        assert rt.sm.get("base.identity.current_relation_id") == "REL-USER"

        switched = rt.switch_interaction_relation("Guest")
        assert switched["relation_id"] == "REL-GUEST"
        assert rt.sm.get("base.identity.current_relation_id") == "REL-GUEST"
        assert rt.sm.get("base.identity.local_default_relation_id") == "REL-USER"

        reset = rt.begin_interaction_instance()
        assert reset["relation_id"] == "REL-USER"
        assert rt.sm.get("base.identity.current_relation_id") == "REL-USER"
        rejected = rt.switch_interaction_relation("REL-OLD")
        assert rejected["status"] == "rejected"
        assert rt.sm.get("base.identity.current_relation_id") == "REL-USER"

    def test_state_anchor_survives_without_recent_cache(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.relation_store = self.AnchorRelationStore()
        rt.switch_interaction_relation("访客")

        meta = rt._resolve_interaction_meta(state=rt.sm.load())

        assert meta["interaction_object_id"] == "REL-GUEST"
        assert meta["interaction_object"] == "访客"
        assert meta["identity_status"] == "known"
        assert meta["interaction_source"] == "instance_selection"

    def test_setup_commit_preserves_local_default_anchor_source(self, tmp_path):
        from engines.round_context import RoundContext, SetupResult

        rt = self._make_runtime(tmp_path)
        rt.relation_store = self.AnchorRelationStore()
        rt.set_local_default_relation("本人")
        rt.setup_runner._write_setup_facts = lambda *args, **kwargs: None

        meta = rt._resolve_interaction_meta(state=rt.sm.load())
        rt.setup_runner.commit(
            RoundContext(1, "interactive", rt.sm.load(), {}),
            SetupResult(raw_result={}, intent={}, interaction_meta=meta),
        )

        assert rt.sm.get("base.identity.current_relation_id") == "REL-USER"
        assert rt.sm.get("base.identity.current_source") == "local_default"

    def test_reaction_loop_wrapper_delegates_to_runner(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class StubRunner:
            def __init__(self):
                self.calls = []

            def run(self, state, round_type, mount_ids, **kwargs):
                self.calls.append((state, round_type, mount_ids, kwargs))
                return {"response": "delegated"}

        runner = StubRunner()
        rt.reaction_loop_runner = runner

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            ["MEM-1"],
            interaction_meta={"interaction_object": "Codex"},
        )

        assert result == {"response": "delegated"}
        assert runner.calls[0][1] == "interactive"
        assert runner.calls[0][2] == ["MEM-1"]
        assert runner.calls[0][3] == {
            "interaction_meta": {"interaction_object": "Codex"}
        }

    def test_cleanup_wrapper_delegates_to_pipeline(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class StubPipeline:
            def __init__(self):
                self.calls = []

            def run(self, round_type, state, result, round_num, **kwargs):
                self.calls.append((round_type, state, result, round_num, kwargs))
                return {"cleanup": "delegated"}

        pipeline = StubPipeline()
        rt.cleanup_pipeline = pipeline

        result = rt._run_cleanup(
            "interactive",
            rt.sm.load(),
            {"response": "reaction"},
            9,
            user_input_text="hello",
        )

        assert result == {"cleanup": "delegated"}
        assert pipeline.calls[0][0] == "interactive"
        assert pipeline.calls[0][3] == 9
        assert pipeline.calls[0][4]["user_input_text"] == "hello"

    def test_protocol_tool_dispatcher_builds_submission_receipts(self):
        from engines.protocol_tool_dispatcher import ProtocolToolDispatcher

        dispatcher = ProtocolToolDispatcher()

        receipts = dispatcher.build_submission_receipts(
            ["memory_write_declaration"],
            ["relation_card_declaration"],
        )

        assert receipts[0]["tool_id"] == "memory_write"
        assert receipts[0]["status"] == "submission_received"
        assert receipts[0]["tool_class"] == "sync_tool"
        assert receipts[0]["execution_route"] == "internal_processor"
        assert receipts[1]["tool_id"] == "relation_card_write"
        assert receipts[1]["status"] == "invalid_tool_request"
        assert receipts[1]["tool_class"] == "sync_tool"
        assert receipts[1]["execution_route"] == "internal_processor"
        assert receipts[1]["reason"] == "retired_text_protocol_submission"

    def test_reaction_loop_rejects_unresolved_memory_preselection_before_provider(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_mounted_content", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(rt, "_existing_stm_memory_ids", lambda: set())

        rt.executor = ScriptedExecutor(
            {"response": "完成 [DONE]"},
        )

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [{
                "type": "memory",
                "ids": "MEM-00162001",
                "source": "ltm_heat_index",
            }],
        )

        assert result["aborted"] is True
        assert result["_required_context_failure"]["stage"] == "recall"
        assert rt.executor.calls == []

    def test_runtime_uses_state_anchor_as_identity_truth(self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.relation_store = self.AnchorRelationStore()
        rt.switch_interaction_relation("访客")

        meta = rt._resolve_interaction_meta(state=rt.sm.load())

        assert meta == {
            "interaction_object_id": "REL-GUEST",
            "interaction_object": "访客",
            "identity_status": "known",
            "interaction_source": "instance_selection",
        }

    def test_setup_reuses_recent_identity_without_timeout_flag(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class RecentContext:
            def get_now_entries(self):
                return [
                    {
                        "kind": "assistant_reply",
                        "interaction_object": "TzPz",
                        "identity_status": "known",
                        "interaction_source": "recent_context_and_handoff",
                    },
                ]

            def get_lately_entries(self, step="setup"):
                return []

        rt.ctx_store = RecentContext()

        meta = rt._resolve_interaction_meta(state=rt.sm.load())

        assert meta == {
            "interaction_object": "TzPz",
            "identity_status": "known",
            "interaction_source": "recent_context_and_handoff",
        }

    def test_setup_runner_retries_once_when_native_finalize_missing(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        calls = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = lambda *args, **kwargs: None
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append((iteration, list(messages)))
            if iteration == 1:
                return {
                    "response": "我已经读取了 2228-2375 行，接下来会继续。",
                    "tool_call_envelopes": [],
                }
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "setup_finalize",
                    "arguments": {"security_verdict": "pass"},
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=207,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)

        assert [iteration for iteration, _messages in calls] == [1, 2]
        assert setup_result.intent["security_verdict"] == "pass"
        retry_content = calls[1][1][-1]["content"]
        retry_text = "\n".join(
            str(message.get("content") or "")
            for message in calls[1][1]
            if isinstance(message, dict)
        )
        assert "setup_finalize" in retry_content
        assert "只进 audit" in retry_content
        assert "不作为事实" in retry_content
        assert "不要声称已读、已执行" in retry_content
        assert "我已经读取了 2228-2375 行" not in retry_text

    def test_spec274_setup_runner_allows_second_retry_to_recover(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        calls = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = lambda *args, **kwargs: None
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append(iteration)
            if iteration < 3:
                return {"response": "still text", "tool_call_envelopes": []}
            return {
                "response": "",
                "tool_call_envelopes": [self._native_tool_envelope(
                    "setup_finalize",
                    {"security_verdict": "pass"},
                    tool_family="substrate_tool",
                    tool_class="sync_tool",
                )],
            }

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=208,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)

        assert calls == [1, 2, 3]
        assert setup_result.intent["security_verdict"] == "pass"

    def test_spec725_setup_retry_applies_pending_permission_before_next_frame(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        rt.audit.start(725, "interactive", {})
        assert rt.control.establish_round("interactive", lambda: 725) == 725
        rt.permission_chain.apply("guarded")
        runner = rt.setup_runner
        assembled_permissions = []

        def assemble_setup(
                state, round_type, user_msgs, internal_handoff=None,
                interaction_meta=None):
            del state, round_type, user_msgs, internal_handoff, interaction_meta
            level = runner.assembler.execution_permission_level
            assembled_permissions.append(level)
            return "setup system", [{"role": "user", "content": level}]

        runner.assembler.assemble_setup = assemble_setup
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = lambda *args, **kwargs: None
        runner._update_token_usage = lambda result, **kwargs: None
        calls = []

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            del phase, system, round_num, kwargs
            calls.append((iteration, list(messages)))
            if iteration == 1:
                rt.permission_updates.request("unlimited")
                return {"response": "retry", "tool_call_envelopes": []}
            return {
                "response": "",
                "tool_call_envelopes": [self._native_tool_envelope(
                    "setup_finalize",
                    {"security_verdict": "pass"},
                    tool_family="substrate_tool",
                    tool_class="sync_tool",
                )],
            }

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=725,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={},
            execution_permission_level="guarded",
        )

        result = runner.run(context)

        assert result.intent["security_verdict"] == "pass"
        assert assembled_permissions == ["guarded", "unlimited"]
        assert calls[1][1][0]["content"] == "unlimited"
        assert "setup_finalize" in calls[1][1][-1]["content"]
        assert context.execution_permission_level == "unlimited"
        assert rt.permission_chain.current == "unlimited"

    def test_spec274_setup_runner_marks_missing_finalize_after_two_retries(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        calls = []
        settlements = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = (
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement))
        )
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append(iteration)
            return {"response": "still text", "tool_call_envelopes": []}

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=274,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)

        assert calls == [1, 2, 3]
        assert setup_result.intent["security_verdict"] == "reject"
        assert setup_result.intent["reject_reason"] == (
            "setup_finalize_missing_after_retry"
        )
        assert settlements[-1][1] == 3
        assert settlements[-1][2]["retry_exhausted"] == (
            "setup_finalize_missing_after_retry"
        )
        assert settlements[-1][2]["frame_ref"]["frame_id"] == (
            "R000274:setup:3")
        assert settlements[-1][2]["frame_ref"]["caused_by"] == (
            "R000274:setup:2")

    def test_spec258_relay_setup_hands_execution_prefix_to_reaction(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = lambda *args, **kwargs: None
        runner._update_token_usage = lambda result, **kwargs: None
        runner._call_llm_with_round_audit = lambda *args, **kwargs: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "setup_finalize",
                {
                    "security_verdict": "pass",
                },
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        }
        context = RoundContext(
            round_num=258,
            round_type="relay",
            state=rt.sm.load(),
            flags={"continue_requested": True},
            interaction_meta=self._confirmed_meta(),
        )

        setup_result = runner.run(context)

        handoff = setup_result.setup_facts
        assert len(handoff) == 1
        assert handoff[0]["kind"] == "setup_fact"
        assert handoff[0]["handoff_target"] == "reaction"
        assert handoff[0]["interaction_source"] == "setup_finalize"
        assert "本轮类型：中继轮" in handoff[0]["content"]
        assert "round_type_confirm=relay" not in handoff[0]["content"]
        assert "note" not in handoff[0]["content"]

    def test_spec411_relay_setup_projects_previous_handoff_to_cache(
            self, tmp_path):
        from engines.round_context import RoundContext
        from logic.relay_intent_pool import create_relay_intent

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        create_relay_intent(
            rt.sm,
            source_round=617,
            handoff_text="下一轮继续从第 2284 行读取《共格主体论》。",
            reaction_finalize_id="call_finalize_617",
        )
        state = rt.sm.load()

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = lambda *args, **kwargs: None
        runner._update_token_usage = lambda result, **kwargs: None
        runner._call_llm_with_round_audit = lambda *args, **kwargs: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "setup_finalize",
                {
                    "security_verdict": "pass",
                },
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        }
        context = RoundContext(
            round_num=618,
            round_type="relay",
            state=state,
            flags={"continue_requested": True},
            interaction_meta=self._confirmed_meta(),
        )

        runner.run(context)

        relay_handoffs = [
            entry for entry in rt.ctx_store.get_now_entries()
            if entry.get("kind") == "relay_handoff"
        ]
        assert len(relay_handoffs) == 1
        assert relay_handoffs[0]["content"] == "下一轮继续从第 2284 行读取《共格主体论》。"
        assert relay_handoffs[0]["round"] == 618
        assert relay_handoffs[0]["interaction_source"] == "relay_intent"

        from assembly.context_helpers import render_corpus_entry_for_context

        target_round = render_corpus_entry_for_context(
            relay_handoffs[0], current_round=618)
        later_round = render_corpus_entry_for_context(
            relay_handoffs[0], current_round=619)
        assert "【上轮交接任务】" in target_round["content"]
        assert "【历史交接任务，来自第 618 轮】" in later_round["content"]

        stored_intent = rt.sm.get("base.runtime.relay_intents", [])[0]
        assert stored_intent["source_round"] == 617
        assert stored_intent["handoff_projected_round"] == 618

    def test_spec273_relay_setup_inherits_recent_confirmed_identity_without_user_input(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        rt.ctx_store.append_to_cache(
            272,
            "assistant",
            "上一轮自然回复。",
            kind="assistant_reply",
            interaction_object="Codex",
            identity_status="known",
            interaction_source="context_continuity",
        )

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = lambda *args, **kwargs: None
        runner._update_token_usage = lambda result, **kwargs: None
        runner._call_llm_with_round_audit = lambda *args, **kwargs: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "setup_finalize",
                {
                    "security_verdict": "pass",
                },
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        }
        context = RoundContext(
            round_num=273,
            round_type="relay",
            state=rt.sm.load(),
            flags={"continue_requested": True},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)
        processor_state = rt.reaction_loop_runner._build_protocol_processor_state(
            setup_result.interaction_meta
        )

        assert setup_result.interaction_meta == {
            "interaction_object": "Codex",
            "identity_status": "known",
            "interaction_source": "context_continuity",
        }
        assert processor_state["presence"]["confirmed_subjects"] == ["Codex"]

    def test_spec598_relay_setup_ignores_retired_timeout_and_reuses_identity(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        rt.ctx_store.append_to_cache(
            272,
            "assistant",
            "上一轮自然回复。",
            kind="assistant_reply",
            interaction_object="Codex",
            identity_status="known",
            interaction_source="context_continuity",
        )

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = lambda *args, **kwargs: None
        runner._update_token_usage = lambda result, **kwargs: None
        runner._call_llm_with_round_audit = lambda *args, **kwargs: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "setup_finalize",
                {
                    "security_verdict": "pass",
                },
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        }
        context = RoundContext(
            round_num=274,
            round_type="relay",
            state=rt.sm.load(),
            flags={"continue_requested": True, "identity_timeout": True},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)
        processor_state = rt.reaction_loop_runner._build_protocol_processor_state(
            setup_result.interaction_meta
        )

        assert setup_result.interaction_meta == {
            "interaction_object": "Codex",
            "identity_status": "known",
            "interaction_source": "context_continuity",
        }
        assert processor_state["presence"]["confirmed_subjects"] == ["Codex"]

    def test_setup_runner_audits_file_read_violation_before_retry(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        settlements = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = (
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement))
        )
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            if iteration == 1:
                return {
                    "response": "",
                    "tool_call_envelopes": [{
                        "tool_id": "file_read",
                        "arguments": {"path": "README.md"},
                        "parse_status": "ok",
                        "index": 0,
                    }],
                }
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "setup_finalize",
                    "arguments": {"security_verdict": "pass"},
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=209,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)

        assert setup_result.intent["security_verdict"] == "pass"
        first_settlement = settlements[0][2]
        assert first_settlement["setup_terminal_violation"] is True
        assert first_settlement["violation_reason"] == "step_non_reaction_tool_not_allowed"
        assert first_settlement["retry_requested"] == "setup_finalize_missing_or_invalid"

    def test_spec548_setup_retry_escalates_reminder_then_warning(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        calls = []
        settlements = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = (
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement))
        )
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append((iteration, list(messages)))
            if iteration == 1:
                return {
                    "response": "",
                    "tool_call_envelopes": [{
                        "tool_id": "file_read",
                        "arguments": {"path": "README.md"},
                        "parse_status": "ok",
                        "index": 0,
                    }],
                }
            if iteration == 2:
                return {
                    "response": "",
                    "tool_call_envelopes": [{
                        "tool_id": "guide_submit",
                        "arguments": {"guide_id": "task_bootstrap"},
                        "parse_status": "ok",
                        "index": 0,
                    }],
                }
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "setup_finalize",
                    "arguments": {"security_verdict": "pass"},
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=548,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)

        assert setup_result.intent["security_verdict"] == "pass"
        first_retry = calls[1][1][-1]["content"]
        second_retry = calls[2][1][-1]["content"]
        assert "REMINDER｜提醒" in first_retry
        assert "file_read" in first_retry
        assert "step_non_reaction_tool_not_allowed" in first_retry
        assert "只能调用 setup_finalize" in first_retry
        assert "WARNING｜警告" not in first_retry
        assert "WARNING｜警告" in second_retry
        assert "第二次无效 setup 输出" in second_retry
        assert "guide_submit" in second_retry
        assert "setup_finalize_missing_after_retry" in second_retry
        assert settlements[0][2]["retry_attempt"] == 1
        assert settlements[0][2]["retry_severity"] == "reminder"
        assert settlements[0][2]["invalid_tool_ids"] == ["file_read"]
        assert settlements[0][2]["invalid_reasons"] == [
            "step_non_reaction_tool_not_allowed"
        ]
        assert settlements[1][2]["retry_attempt"] == 2
        assert settlements[1][2]["retry_severity"] == "warning"
        assert settlements[1][2]["invalid_tool_ids"] == ["guide_submit"]

    def test_setup_runner_retries_when_native_envelope_key_is_missing(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        calls = []
        settlements = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = (
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement))
        )
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append(iteration)
            if iteration == 1:
                return {
                    "response": (
                        "setup_mount_apply:\n"
                        "mount_memory: MEM-OLD-FALLBACK\n"
                        "setup_handoff:\n"
                        "note: 旧文本不应生效"
                    ),
                }
            return {
                "response": "",
                "tool_call_envelopes": [self._native_tool_envelope(
                    "setup_finalize",
                    {"security_verdict": "pass"},
                    tool_family="substrate_tool",
                    tool_class="sync_tool",
                    risk="high",
                )],
            }

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=318,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)

        assert calls == [1, 2]
        assert setup_result.intent["security_verdict"] == "pass"
        assert setup_result.intent["mount_requests"] == []
        assert "MEM-OLD-FALLBACK" not in "\n".join(
            entry.get("content", "") for entry in setup_result.setup_facts
        )
        assert settlements[0][2]["retry_requested"] == "setup_finalize_missing_or_invalid"

    def test_setup_runner_retries_when_file_read_is_mixed_with_setup_finalize(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        calls = []
        settlements = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = (
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement))
        )
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            calls.append(iteration)
            if iteration == 1:
                return {
                    "response": "",
                    "tool_call_envelopes": [
                        {
                            "tool_id": "setup_finalize",
                            "arguments": {"security_verdict": "pass"},
                            "parse_status": "ok",
                            "index": 0,
                        },
                        {
                            "tool_id": "file_read",
                            "arguments": {"path": "README.md"},
                            "parse_status": "ok",
                            "index": 1,
                        },
                    ],
                }
            return {
                "response": "",
                "tool_call_envelopes": [{
                    "tool_id": "setup_finalize",
                    "arguments": {"security_verdict": "pass"},
                    "parse_status": "ok",
                    "index": 0,
                }],
            }

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=211,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        setup_result = runner.run(context)

        assert calls == [1, 2]
        assert setup_result.intent["security_verdict"] == "pass"
        assert settlements[0][2]["setup_terminal_violation"] is True
        assert settlements[0][2]["violation_reason"] == "step_non_reaction_tool_not_allowed"

    def test_setup_runner_audits_retry_interruption_before_reraising(self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        runner = rt.setup_runner
        settlements = []

        runner.assembler.assemble_setup = (
            lambda state, round_type, user_msgs, internal_handoff=None,
            interaction_meta=None: (
                "setup system",
                [{"role": "user", "content": "setup"}],
            )
        )
        runner._round_audit_parsed = lambda *args, **kwargs: None
        runner._round_audit_settlement = (
            lambda round_num, phase, iteration, settlement:
            settlements.append((phase, iteration, settlement))
        )
        runner._update_token_usage = lambda result, **kwargs: None

        def fake_call(phase, system, messages, round_num, iteration=1, **kwargs):
            if iteration == 1:
                return {"response": "plain text", "tool_call_envelopes": []}
            raise RuntimeError("retry lost")

        runner._call_llm_with_round_audit = fake_call
        context = RoundContext(
            round_num=210,
            round_type="interactive",
            state=rt.sm.load(),
            flags={},
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        try:
            runner.run(context)
        except RuntimeError as exc:
            assert str(exc) == "retry lost"
        else:
            raise AssertionError("expected retry interruption")

        assert settlements[-1][1] == 2
        assert settlements[-1][2]["setup_retry_interrupted"] is True
        assert settlements[-1][2]["retry_requested"] == "setup_finalize_missing_or_invalid"
        assert settlements[-1][2]["error"] == "retry lost"

    def test_setup_reuses_recent_confirmed_interaction_meta_before_timeout(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class RecentContext:
            def get_now_entries(self):
                return [
                    {
                        "kind": "interaction",
                        "interaction_object": "TzPz",
                        "identity_status": "known",
                        "interaction_source": "recent_context_and_handoff",
                    },
                    {
                        "kind": "assistant_reply",
                        "interaction_object": "TzPz",
                        "identity_status": "known",
                        "interaction_source": "recent_context_and_handoff",
                    },
                ]

            def get_lately_entries(self, step="setup"):
                return []

        rt.ctx_store = RecentContext()

        meta = rt._resolve_interaction_meta(state=rt.sm.load())

        assert meta == {
            "interaction_object": "TzPz",
            "identity_status": "known",
            "interaction_source": "recent_context_and_handoff",
        }

    def test_setup_recent_identity_prefers_now_over_stale_lately(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class MixedContext:
            def get_now_entries(self):
                return [
                    {
                        "kind": "assistant_reply",
                        "interaction_object": "TzPz",
                        "identity_status": "known",
                        "interaction_source": "recent_context_and_handoff",
                    },
                ]

            def get_lately_entries(self, step="setup"):
                return [
                    {
                        "kind": "interaction",
                        "interaction_object": "Codex",
                        "identity_status": "declared",
                        "interaction_source": "self_declaration",
                    },
                ]

        rt.ctx_store = MixedContext()

        meta = rt._resolve_interaction_meta(state=rt.sm.load())

        assert meta["interaction_object"] == "TzPz"
        assert meta["identity_status"] == "known"

    def test_setup_recent_identity_skips_stale_self_declaration(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        class MixedContext:
            def get_now_entries(self):
                return [
                    {
                        "kind": "assistant_reply",
                        "interaction_object": "TzPz",
                        "identity_status": "known",
                        "interaction_source": "recent_context_and_handoff",
                    },
                    {
                        "kind": "assistant_reply",
                        "interaction_object": "Codex",
                        "identity_status": "declared",
                        "interaction_source": "self_declaration",
                    },
                ]

            def get_lately_entries(self, step="setup"):
                return []

        rt.ctx_store = MixedContext()

        meta = rt._resolve_interaction_meta(state=rt.sm.load())

        assert meta["interaction_object"] == "TzPz"
        assert meta["identity_status"] == "known"

    def test_setup_identity_confirmation_commits_after_candidate_selection(self, tmp_path):
        from engines.round_context import RoundContext, RuntimeTrigger, SetupResult

        rt = self._make_runtime(tmp_path)
        rt.relation_store = self.AnchorRelationStore()

        meta = rt._apply_setup_interaction_meta(
            {
                "interaction_object": "unknown",
                "identity_status": "timeout",
                "interaction_source": "identity_timeout",
            },
            {
                "interaction_meta": {
                    "interaction_object": "Guest",
                    "identity_status": "known",
                    "interaction_source": "context_continuity",
                    "basis": "recent reading task continuity",
                }
            },
        )

        assert meta == {
            "interaction_object_id": "REL-GUEST",
            "interaction_object": "访客",
            "identity_status": "known",
            "interaction_source": "context_continuity",
            "basis": "recent reading task continuity",
        }
        assert rt.sm.get("base.identity.confirmed") is False

        context = RoundContext(
            1, "interactive", rt.sm.load(), {"user_message_waiting": True},
            trigger=RuntimeTrigger(
                "T00000001", 1, "2026-07-16T00:00:00+08:00",
                "interactive", {"user_message_waiting": True}, ("hello",),
            ),
        )
        rt.setup_runner.commit(context, SetupResult(
            raw_result={},
            intent={},
            interaction_meta=meta,
        ))

        assert rt.sm.get("base.identity.confirmed") is True
        assert rt.sm.get("base.identity.current_relation_id") == "REL-GUEST"

    def test_setup_declared_identity_overrides_current_anchor_after_validation(
            self, tmp_path):
        from engines.round_context import RoundContext, SetupResult

        rt = self._make_runtime(tmp_path)
        rt.relation_store = self.AnchorRelationStore()
        rt.switch_interaction_relation("访客")

        meta = rt._apply_setup_interaction_meta(
            rt._resolve_interaction_meta(state=rt.sm.load()),
            {"interaction_meta": {
                "interaction_object": "本人",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            }},
        )
        assert meta == {
            "interaction_object_id": "REL-USER",
            "interaction_object": "用户",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }

        rt.setup_runner.commit(
            RoundContext(2, "interactive", rt.sm.load(), {}),
            SetupResult(raw_result={}, intent={}, interaction_meta=meta),
        )
        assert rt.sm.get("base.identity.current_relation_id") == "REL-USER"

    def test_setup_identity_judgment_does_not_downgrade_declared_identity(self, tmp_path):
        rt = self._make_runtime(tmp_path)

        meta = rt._apply_setup_interaction_meta(
            {
                "interaction_object": "Codex",
                "identity_status": "declared",
                "interaction_source": "self_declaration",
            },
            {
                "interaction_meta": {
                    "interaction_object": "unknown",
                    "identity_status": "timeout",
                    "interaction_source": "identity_timeout",
                }
            },
        )

        assert meta == {
            "interaction_object": "Codex",
            "identity_status": "declared",
            "interaction_source": "self_declaration",
        }

    def test_reaction_identity_gate_detects_blocked_tool_activity(self):
        from engines.reaction_helpers import (
            reaction_identity_has_blocked_activity,
            reaction_identity_requires_resolution,
        )

        assert reaction_identity_has_blocked_activity({
            "general_tool_requests": [{"tool_id": "file_read"}],
        }) is True
        assert reaction_identity_has_blocked_activity({
            "identity_resolution": {"action": "ask_user", "question": "Who are you?"},
        }) is False
        assert reaction_identity_requires_resolution({
            "interaction_object": "张三",
            "identity_status": "unregistered",
            "interaction_source": "self_declaration",
        }) is False
        assert reaction_identity_requires_resolution({
            "interaction_object": "unknown",
            "identity_status": "timeout",
            "interaction_source": "identity_timeout",
        }) is True

    def test_reaction_loop_blocks_high_impact_actions_until_identity_resolved(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_mounted_content", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class IdentityBlockedExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                self.calls.append(list(messages))
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "I need to confirm who is speaking before I act. Who are you?",
                        "tool_call_envelopes": [],
                    }
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_read",
                            {
                                "path": "library/theory/book.md",
                                "reason": "continue reading",
                            },
                            call_id="call_identity_file",
                        )],
                    }
                return {
                    "response": "I need to confirm who is speaking before I act. Who are you?",
                    "tool_call_envelopes": [],
                }

        rt.executor = IdentityBlockedExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        assert result["response"] == "I need to confirm who is speaking before I act. Who are you?"
        assert result["_general_tool_requests"] == []
        assert result["_invalid_tool_requests"][0]["reason"] == "identity_unresolved"
        second_call_text = "\n".join(
            message.get("content", "") for message in rt.executor.calls[1])
        assert "identity_unresolved" in second_call_text
        assert "_native_tool_result_projections" not in result
        assert "native_tool_outputs" not in second_call_text
        assert "native_tool_call_envelopes" not in second_call_text

    def test_reaction_loop_ignores_retired_finalize_identity_resolution(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_mounted_content", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RetiredIdentityFinalizeExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages, active_protocol_tool_guides=None):
                if logical_step(step, active_protocol_tool_guides) == "final_reply":
                    return {
                        "response": "Confirmed.",
                        "tool_call_envelopes": [],
                    }
                self.calls.append(list(messages))
                if len(self.calls) == 1:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_reaction_finalize(
                            call_id="call_identity_confirm",
                            identity_resolution={
                                "action": "confirm",
                                "interaction_object": "TzPz",
                                "identity_status": "known",
                                "basis": "context_continuity",
                            },
                        )],
                }
                return {
                    "response": "Confirmed.",
                    "tool_call_envelopes": [],
                }

        rt.executor = RetiredIdentityFinalizeExecutor()

        result = rt._run_reaction_loop(
            rt.sm.load(),
            "interactive",
            [],
            interaction_meta={
                "interaction_object": "unknown",
                "identity_status": "unknown",
                "interaction_source": "unresolved",
            },
        )

        assert result["response"] == "Confirmed."
        assert result["_identity_resolutions"] == []
        assert result["_interaction_meta"]["interaction_object"] == "unknown"
        assert assembler._current_interaction_meta == {
            "interaction_object": "unknown",
            "identity_status": "unknown",
            "interaction_source": "unresolved",
        }
        assert rt.sm.get("base.identity.confirmed") is False
