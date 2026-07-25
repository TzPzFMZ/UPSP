import os
import sys
import json

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


class TestRuntimeRoundFlow(RuntimeTestMixin):
    def test_spec383_coalesced_rhythm_dequeues_waiting_user_message(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        dequeued = []

        def fake_dequeue():
            dequeued.append(True)
            return ["请读取并内化这本书。path=D:\\AI_WORKSPACE\\base\\book\\共格主体论_V5_6.1.md"]

        monkeypatch.setattr(rt.hb, "dequeue_messages", fake_dequeue)

        trigger = rt._new_trigger("rhythm", {
            "calendar_day_due": True,
            "user_message_waiting": True,
        })

        assert dequeued == [True]
        assert "请读取并内化这本书" in trigger.messages[0]

    def test_api_timeout_in_reaction_enters_cleanup(self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult
        from errors import APITimeoutError

        rt = self._make_runtime(tmp_path)
        cleanup_calls = []
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={"security_verdict": "pass", "mount_requests": []},
            interaction_meta=self._confirmed_meta(),
            user_input_text="读书",
            setup_messages=[],
            internal_handoff=[],
        )

        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)
        monkeypatch.setattr(
            rt,
            "_run_reaction_loop",
            lambda *args, **kwargs: (_ for _ in ()).throw(
                APITimeoutError("primary", "API timeout after 300 seconds")
            ),
        )
        monkeypatch.setattr(
            rt,
            "_run_cleanup",
            lambda *args, **kwargs: cleanup_calls.append((args, kwargs)),
        )
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("interactive", rt.sm.load(), {"user_message_waiting": True})

        assert cleanup_calls
        cleanup_args = cleanup_calls[0][0]
        assert cleanup_args[0] == "interactive"
        assert cleanup_args[2]["aborted"] is True
        assert cleanup_args[2]["_failed_phase"] == "reaction"
        assert "API timeout after 300 seconds" in cleanup_args[2]["error"]

    def test_run_one_round_writes_interaction_input_into_now_layer(self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        monkeypatch.setattr(rt.hb, "dequeue_messages", lambda: [
            "我是 Codex，验证起手步当前缓存 now。"
        ])
        monkeypatch.setattr(rt.executor, "call", lambda *args, **kwargs: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "setup_finalize",
                {"security_verdict": "pass", "mount_requests": []},
                tool_family="substrate_tool",
                tool_class="sync_tool",
                risk="high",
            )],
        })
        monkeypatch.setattr(rt, "_run_reaction_loop", lambda *args, **kwargs: {
            "response": "反应步略过",
        })
        monkeypatch.setattr(rt, "_run_cleanup", lambda *args, **kwargs: None)
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round(
            "interactive", rt.sm.load(), {"user_message_waiting": True})

        step_md = (tmp_path / "context" / "setup" / "step.md").read_text(
            encoding="utf-8")
        assert "<!-- 当前缓存 now -->" not in step_md
        assert "验证起手步当前缓存 now" in step_md
        assert "<!-- 交互输入层 -->" not in step_md
        assert "<!-- 当前输入层 -->" not in step_md

    def test_spec449_coalesced_rhythm_reaction_reads_interaction_from_now(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        rt.setup_runner._write_interaction_input(
            591,
            "请读取 DFT_AGENT_EVAL\\agent_eval_tasks.md 并完成任务。",
            self._confirmed_meta(),
        )
        state = rt.sm.load()
        state["base"]["meta"]["total_round"] = 591
        state["base"]["heartbeat_flags"] = {
            "calendar_day_due": True,
            "user_message_waiting": True,
        }

        _system, messages = assembler.assemble_reaction(state, "rhythm")

        combined = "\n".join(str(m.get("content") or "") for m in messages)
        assert "请读取 DFT_AGENT_EVAL\\agent_eval_tasks.md 并完成任务。" in combined
        now_md = (tmp_path / "context" / "reaction" / "layers" / "50_now.md").read_text(
            encoding="utf-8")
        assert "请读取 DFT_AGENT_EVAL\\agent_eval_tasks.md 并完成任务。" in now_md

    def test_spec409_active_round_uses_fresh_state_after_increment(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult

        rt = self._make_runtime(tmp_path)
        rt.sm.set("base.meta.total_round", 616)
        captured = {"setup_round": None, "cleanup_round": None}

        def fake_setup(context):
            captured["setup_round"] = context.state["base"]["meta"]["total_round"]
            return SetupResult(
                raw_result={"response": ""},
                intent={"security_verdict": "pass", "mount_requests": []},
                interaction_meta=self._confirmed_meta(),
                user_input_text="请读取并内化这本书。",
                setup_messages=[],
                internal_handoff=[],
            )

        def fake_cleanup(round_type, state, result, round_num, *args, **kwargs):
            captured["cleanup_round"] = state["base"]["meta"]["total_round"]
            assert round_num == 617

        monkeypatch.setattr(rt.setup_runner, "run", fake_setup)
        monkeypatch.setattr(rt, "_run_reaction_loop", lambda *args, **kwargs: {
            "response": "反应步完成",
            "_interaction_meta": self._confirmed_meta(),
        })
        monkeypatch.setattr(rt, "_run_cleanup", fake_cleanup)
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("interactive", rt.sm.load(), {
            "user_message_waiting": True,
        })

        assert captured == {"setup_round": 617, "cleanup_round": 617}

    def test_spec383_rhythm_interaction_flag_clear_requires_real_user_input(self):
        from engines.cleanup_pipeline import CleanupPipeline

        assert not CleanupPipeline._interaction_consumed_by_result({
            "_reaction_finalize_validated": True,
            "_final_reply_done": True,
            "_user_input_text": "",
        })
        assert CleanupPipeline._interaction_consumed_by_result({
            "_reaction_finalize_validated": True,
            "_final_reply_done": False,
            "_user_input_text": "请读取并内化这本书。",
        })

    def test_spec592_setup_task_guidance_stages_task_guide(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult

        rt = self._make_runtime(tmp_path)
        task_root = tmp_path / "DFT_AGENT_EVAL"
        output_root = task_root / "output"
        output_root.mkdir(parents=True)
        monkeypatch.setenv(
            "UPSP_ENGINEERING_SANDBOX_GRANT_JSON",
            json.dumps({
                "phase": "agent_daily_eval",
                "task_root": str(task_root),
                "read_paths": [str(task_root)],
                "write_paths": [str(output_root)],
                "shell_cwd": str(task_root),
                "allowed_tools": ["file_read", "file_write", "shell_command"],
            }),
        )
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={
                "security_verdict": "pass",
                "mount_requests": [],
                "task_guidance_required": True,
                "task_guidance_reason": "read task file and complete outputs",
            },
            interaction_meta=self._confirmed_meta(),
            user_input_text="请读取 DFT_AGENT_EVAL/agent_eval_tasks.md 并完成任务。",
            setup_messages=[],
            internal_handoff=[],
        )
        cleanup_calls = []

        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)

        def fake_reaction(*args, **kwargs):
            rhythm_id = rt.workbench.get("base.active_guides.rhythm")
            assert rhythm_id
            assert rt.workbench.get("base.active_guide") == rhythm_id
            assert rt.workbench.get("base.active_guides.work") == "task_bootstrap"
            assert rt.workbench.load_guide(rhythm_id)["kind"] == "calendar_rhythm_guide"
            assert rt.workbench.load_guide("task_bootstrap")["kind"] == "task_bootstrap"
            return {
                "response": "只完成了一个节律合轮内的局部动作。",
                "_reaction_finalize_validated": True,
                "_final_reply_done": True,
                "_interaction_meta": self._confirmed_meta(),
            }

        monkeypatch.setattr(rt, "_run_reaction_loop", fake_reaction)
        monkeypatch.setattr(
            rt,
            "_run_cleanup",
            lambda *args, **kwargs: cleanup_calls.append((args, kwargs)),
        )
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("rhythm", rt.sm.load(), {
            "calendar_day_due": True,
            "user_message_waiting": True,
        })

        rhythm_id = rt.workbench.get("base.active_guides.rhythm")
        assert rhythm_id
        assert rt.workbench.get("base.active_guide") == rhythm_id
        assert rt.workbench.get("base.active_guides.work") == "task_bootstrap"
        debt = rt.sm.get("base.runtime.work_intent_debt")
        assert debt["status"] == "open"
        assert debt["source"] == "setup_finalize"
        assert debt["reason"] == "read task file and complete outputs"
        assert rt.sm.get("base.heartbeat_flags.continue_requested") is not True
        assert not rt.sm.get("base.runtime.relay_intents")
        assert cleanup_calls
        cleanup_result = cleanup_calls[0][0][2]
        assert "_interaction_debt_receipt" not in cleanup_result

    def test_spec592_agent_eval_task_phase_auto_creates_debt_and_bootstrap(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult

        rt = self._make_runtime(tmp_path)
        task_root = tmp_path / "DFT_AGENT_EVAL"
        output_root = task_root / "output"
        output_root.mkdir(parents=True)
        monkeypatch.setenv(
            "UPSP_ENGINEERING_SANDBOX_GRANT_JSON",
            json.dumps({
                "phase": "agent_eval",
                "task_root": str(task_root),
                "read_paths": [str(task_root)],
                "write_paths": [str(output_root)],
                "shell_cwd": str(task_root),
                "allowed_tools": ["file_read", "file_write", "shell_command"],
            }),
        )
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={
                "security_verdict": "pass",
                "mount_requests": [],
                "task_guidance_required": False,
            },
            interaction_meta=self._confirmed_meta(),
            user_input_text="请读取 agent_eval_tasks.md 并完成 12 项任务。",
            setup_messages=[],
            internal_handoff=[],
        )

        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)

        def fake_reaction(*args, **kwargs):
            debt = rt.sm.get("base.runtime.work_intent_debt")
            assert debt["status"] == "open"
            assert debt["source"] == "engineering_task_phase"
            assert debt["task_phase"] == "agent_eval"
            assert rt.workbench.get("base.active_guides.work") == "task_bootstrap"
            assert rt.workbench.get("base.active_guide") == "task_bootstrap"
            return {
                "response": "先自然处理用户输入。",
                "_reaction_finalize_validated": True,
                "_final_reply_done": True,
                "_interaction_meta": self._confirmed_meta(),
            }

        monkeypatch.setattr(rt, "_run_reaction_loop", fake_reaction)
        monkeypatch.setattr(rt, "_run_cleanup", lambda *args, **kwargs: None)
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("interactive", rt.sm.load(), {
            "user_message_waiting": True,
        })

    def test_spec592_open_work_intent_debt_blocks_finish(self, tmp_path):
        from logic.work_intent_debt import create_work_intent_debt

        rt = self._make_runtime(tmp_path)
        create_work_intent_debt(
            rt.sm,
            round_num=510,
            reason="agent_eval 任务必须先建清单",
            source="engineering_task_phase",
            source_refs=["round:510:interaction"],
            task_phase="agent_eval",
            task_root=str(tmp_path / "DFT_AGENT_EVAL"),
        )

        result = rt.reaction_loop_runner._task_closeout_acceptance({
            "closeout_decision": "finish",
        })

        assert result["allowed"] is False
        assert result["reason"] == "task_bootstrap_required"
        assert result["blockers"] == ["work_intent_debt"]

    def test_spec592_debt_feedback_points_to_task_bootstrap(self):
        from engines.reaction_loop import ReactionLoopRunner

        feedback = ReactionLoopRunner._task_acceptance_feedback({
            "allowed": False,
            "reason": "task_bootstrap_required",
            "blockers": ["work_intent_debt"],
        })

        assert "work_intent_debt" in feedback
        assert "task_bootstrap" in feedback
        assert "先建立任务指南清单" in feedback

    def test_spec592_cleanup_treats_open_work_intent_debt_as_task_blocker(self):
        from engines.cleanup_pipeline import CleanupPipeline

        state = {
            "base": {
                "active_guides": {},
                "runtime": {
                    "work_intent_debt": {
                        "status": "open",
                        "source": "legacy_test",
                    }
                },
            }
        }

        assert CleanupPipeline._cleanup_has_unclosed_task_blocker(state) is True

    def test_spec579_runtime_deferred_interaction_debt_path_removed(self):
        from engines.runtime import Runtime

        assert not hasattr(Runtime, "_should_defer_interaction_debt")
        assert not hasattr(Runtime, "_record_deferred_interaction_debt")

    def test_spec592_simple_setup_false_does_not_create_task_debt(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult

        rt = self._make_runtime(tmp_path)
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={
                "security_verdict": "pass",
                "mount_requests": [],
                "task_guidance_required": False,
                "task_guidance_route": "none",
                "task_guidance_reason": "",
            },
            interaction_meta=self._confirmed_meta(),
            user_input_text="现在状态如何？",
            setup_messages=[],
            internal_handoff=[],
        )

        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)

        def fake_reaction(*args, **kwargs):
            assert not rt.sm.get("base.runtime.work_intent_debt")
            assert not rt.workbench.get("base.active_guides.work")
            assert rt.workbench.get("base.active_guide") in (None, "")
            return {
                "response": "当前空闲。",
                "_reaction_finalize_validated": True,
                "_final_reply_done": True,
                "_interaction_meta": self._confirmed_meta(),
            }

        monkeypatch.setattr(rt, "_run_reaction_loop", fake_reaction)
        monkeypatch.setattr(rt, "_call_llm_with_round_audit", lambda *a, **kw: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "cleanup_finalize",
                {},
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        })
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("interactive", rt.sm.load(), {
            "user_message_waiting": True,
        })

        assert not rt.sm.get("base.runtime.work_intent_debt")
        assert not rt.workbench.get("base.active_guides.work")

    def test_spec448_coalesced_rhythm_does_not_default_to_relay_with_active_task(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult

        rt = self._make_runtime(tmp_path)
        task_root = tmp_path / "DFT_AGENT_EVAL"
        output_root = task_root / "output"
        output_root.mkdir(parents=True)
        monkeypatch.setenv(
            "UPSP_ENGINEERING_SANDBOX_GRANT_JSON",
            json.dumps({
                "phase": "agent_daily_eval",
                "task_root": str(task_root),
                "read_paths": [str(task_root)],
                "write_paths": [str(output_root)],
                "shell_cwd": str(task_root),
                "allowed_tools": ["file_read", "file_write", "shell_command"],
            }),
        )
        rt.workbench.init_if_missing()
        rt.workbench.set("base.active_task", "T-20260627-01")
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={
                "security_verdict": "pass",
                "mount_requests": [],
                "task_guidance_required": True,
                "task_guidance_reason": "需要读取任务文件并完成多项产物验收",
            },
            interaction_meta=self._confirmed_meta(),
            user_input_text="请读取 DFT_AGENT_EVAL/agent_eval_tasks.md 并完成任务。",
            setup_messages=[],
            internal_handoff=[],
        )

        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)
        monkeypatch.setattr(rt, "_run_reaction_loop", lambda *args, **kwargs: {
            "response": "节律事项已完成。",
            "_reaction_finalize_validated": True,
            "_final_reply_done": True,
            "_interaction_meta": self._confirmed_meta(),
        })
        monkeypatch.setattr(rt, "_call_llm_with_round_audit", lambda *a, **kw: {
            "response": "",
            "tool_call_envelopes": [self._native_tool_envelope(
                "cleanup_finalize",
                {},
                tool_family="substrate_tool",
                tool_class="sync_tool",
            )],
        })
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("rhythm", rt.sm.load(), {
            "calendar_day_due": True,
            "user_message_waiting": True,
        })

        flags = rt.sm.get("base.heartbeat_flags")
        assert flags["user_message_waiting"] is False
        assert flags.get("continue_requested") is not True
        assert not rt.sm.get("base.runtime.relay_intents")

    def test_spec592_setup_records_existing_task_pending_input(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult
        from logic.task_guide import materialize_initial_task_guide

        rt = self._make_runtime(tmp_path)
        task_id = materialize_initial_task_guide(
            rt.workbench,
            {
                "task_title": "已有长任务",
                "items": ["完成现有任务"],
                "acceptance": ["输出已验证"],
            },
        )
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={
                "security_verdict": "pass",
                "mount_requests": [],
                "task_guidance_required": True,
                "task_guidance_route": "current_work",
                "task_guidance_reason": "用户追加了需要整合的新要求",
            },
            interaction_meta=self._confirmed_meta(),
            user_input_text="顺手再把输出报告改成中文摘要。",
            setup_messages=[],
            internal_handoff=[],
        )

        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)

        def fake_reaction(*args, **kwargs):
            guide = rt.workbench.load_task_guide(task_id)
            pending = guide.get("pending_inputs") or []
            assert len(pending) == 1
            assert pending[0]["summary"] == "用户追加了需要整合的新要求"
            assert pending[0]["task_guidance_route"] == "current_work"
            return {
                "response": "继续处理已有任务。",
                "_reaction_finalize_validated": True,
                "_final_reply_done": True,
                "_interaction_meta": self._confirmed_meta(),
            }

        cleanup_calls = []
        monkeypatch.setattr(rt, "_run_reaction_loop", fake_reaction)
        monkeypatch.setattr(
            rt,
            "_run_cleanup",
            lambda *args, **kwargs: cleanup_calls.append((args, kwargs)),
        )
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("interactive", rt.sm.load(), {
            "user_message_waiting": True,
        })

        assert rt.workbench.get("base.active_task") == task_id
        assert rt.workbench.get("base.active_guide") == f"task:{task_id}"
        assert rt.sm.get("base.heartbeat_flags.continue_requested") is not True
        assert not rt.sm.get("base.runtime.relay_intents")
        assert cleanup_calls

    def test_spec488_relay_task_guidance_does_not_create_pending_input(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult
        from logic.task_guide import materialize_initial_task_guide

        rt = self._make_runtime(tmp_path)
        task_id = materialize_initial_task_guide(
            rt.workbench,
            {
                "task_title": "已有 12 项任务",
                "items": ["继续完成 12 项任务"],
                "acceptance": ["输出文件齐全且验证通过"],
            },
        )
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={
                "security_verdict": "pass",
                "mount_requests": [],
                "task_guidance_required": True,
                "task_guidance_route": "current_work",
                "task_guidance_reason": "继续执行上轮 12 项任务。",
            },
            interaction_meta=self._confirmed_meta(),
            user_input_text="",
            setup_messages=[],
            internal_handoff=[],
        )

        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)

        def fake_reaction(*args, **kwargs):
            guide = rt.workbench.load_task_guide(task_id)
            assert guide.get("pending_inputs") in (None, [])
            return {
                "response": "继续处理已有任务。",
                "_reaction_finalize_validated": True,
                "_final_reply_done": True,
                "_interaction_meta": self._confirmed_meta(),
            }

        cleanup_calls = []
        monkeypatch.setattr(rt, "_run_reaction_loop", fake_reaction)
        monkeypatch.setattr(
            rt,
            "_run_cleanup",
            lambda *args, **kwargs: cleanup_calls.append((args, kwargs)),
        )
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("relay", rt.sm.load(), {"continue_requested": True})

        guide = rt.workbench.load_task_guide(task_id)
        assert guide.get("pending_inputs") in (None, [])
        assert cleanup_calls

    def test_spec471_lately_trim_sets_cache_compaction_due_without_active_guide(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult

        rt = self._make_runtime(tmp_path)
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={"security_verdict": "pass", "mount_requests": []},
            interaction_meta=self._confirmed_meta(),
            user_input_text="继续。",
            setup_messages=[],
            internal_handoff=[],
        )
        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)
        monkeypatch.setattr(rt, "_run_reaction_loop", lambda *args, **kwargs: {
            "response": "反应步完成",
            "_interaction_meta": self._confirmed_meta(),
        })
        monkeypatch.setattr(rt, "_run_cleanup", lambda *args, **kwargs: None)
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)
        monkeypatch.setattr(rt.ctx_store, "get_last_cache_stats", lambda: {
            "lately_trimmed": True,
            "cache_compaction_required": True,
            "lately_deleted_blocks": 2,
            "lately_deleted_chars": 9000,
            "lately_surviving_chars": 12000,
            "lately_compact_ratio": 0.618,
            "lately_compact_shard_chars": 8192,
            "lately_compact_shard_ratio": 0.314,
        })
        monkeypatch.setattr(rt.ctx_store, "get_lately_compaction_params", lambda: {
            "compact_ratio": 0.618,
            "compact_shard_chars": 8192,
            "compact_shard_ratio": 0.314,
        }, raising=False)
        monkeypatch.setattr(
            rt.ctx_store,
            "build_lately_compression_candidates",
            lambda max_blocks=None: [
                {"id": "R000001-user-0000", "chars": 4000, "text": "A" * 4000},
                {"id": "R000002-assistant-0000", "chars": 4000, "text": "B" * 4000},
                {"id": "R000003-tool-0000", "chars": 4000, "text": "C" * 4000},
            ],
        )

        rt._run_one_round("interactive", rt.sm.load(), {"user_message_waiting": True})

        assert rt.sm.get("base.heartbeat_flags.cache_compaction_due") is True
        assert rt.workbench.get("base.active_guides.rhythm") is None
        assert rt.workbench.get("base.active_guide") is None

    def test_spec471_cache_compaction_due_materializes_in_next_rhythm_agenda(
            self, tmp_path, monkeypatch):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        monkeypatch.setattr(rt.ctx_store, "get_last_cache_stats", lambda: {
            "lately_trimmed": True,
            "cache_compaction_required": True,
            "lately_deleted_blocks": 2,
            "lately_deleted_chars": 9000,
            "lately_surviving_chars": 12000,
            "lately_compact_ratio": 0.618,
            "lately_compact_shard_chars": 8192,
            "lately_compact_shard_ratio": 0.314,
        })
        monkeypatch.setattr(rt.ctx_store, "get_lately_compaction_params", lambda: {
            "compact_ratio": 0.618,
            "compact_shard_chars": 8192,
            "compact_shard_ratio": 0.314,
        }, raising=False)
        monkeypatch.setattr(
            rt.ctx_store,
            "build_lately_compression_candidates",
            lambda max_blocks=None: [
                {"id": "R000001-user-0000", "chars": 4000, "text": "A" * 4000},
                {"id": "R000002-assistant-0000", "chars": 4000, "text": "B" * 4000},
                {"id": "R000003-tool-0000", "chars": 4000, "text": "C" * 4000},
            ],
        )
        rt.sm.set_flag("cache_compaction_due", True)
        state = rt.sm.load()
        context = RoundContext(
            round_num=2,
            round_type="rhythm",
            state=state,
            flags=state["base"]["heartbeat_flags"],
        )

        guide_id = rt._materialize_runtime_rhythm_guide(context)

        assert guide_id == "cache_compaction:R000002"
        assert rt.workbench.get("base.active_guides.rhythm") == "cache_compaction:R000002"
        assert rt.workbench.get("base.active_guide") == "cache_compaction:R000002"
        guide = rt.workbench.load_active_guide()
        assert guide["kind"] == "cache_compaction_rhythm_guide"
        assert guide["compaction_plan"]["before_chars"] == 12000
        assert guide["compaction_plan"]["shards"][0]["target_chars"] == int(8000 * 0.314)

    def test_spec474_fresh_runtime_materializes_cache_compaction_from_persisted_debt(
            self, tmp_path, monkeypatch):
        from engines.round_context import RoundContext

        first = self._make_runtime(tmp_path)
        monkeypatch.setattr(first.ctx_store, "get_last_cache_stats", lambda: {
            "lately_trimmed": True,
            "cache_compaction_required": True,
            "lately_deleted_blocks": 2,
            "lately_deleted_chars": 9000,
            "lately_surviving_chars": 12000,
            "lately_compact_ratio": 0.618,
            "lately_compact_shard_chars": 8192,
            "lately_compact_shard_ratio": 0.314,
        })
        monkeypatch.setattr(first.ctx_store, "get_lately_compaction_params", lambda: {
            "compact_ratio": 0.618,
            "compact_shard_chars": 8192,
            "compact_shard_ratio": 0.314,
        }, raising=False)
        monkeypatch.setattr(
            first.ctx_store,
            "build_lately_compression_candidates",
            lambda max_blocks=None: [
                {"id": "R000001-user-0000", "chars": 4000, "text": "A" * 4000},
                {"id": "R000002-assistant-0000", "chars": 4000, "text": "B" * 4000},
                {"id": "R000003-tool-0000", "chars": 4000, "text": "C" * 4000},
            ],
        )

        first._record_cache_compaction_rhythm_if_needed(474)
        assert first.sm.get("base.heartbeat_flags.cache_compaction_due") is True
        assert os.path.isfile(first.ctx_store.cache_compaction_debt_path())

        fresh = self._make_runtime(tmp_path)
        state = fresh.sm.load()
        context = RoundContext(
            round_num=475,
            round_type="rhythm",
            state=state,
            flags=state["base"]["heartbeat_flags"],
        )

        guide_id = fresh._materialize_runtime_rhythm_guide(context)

        assert guide_id == "cache_compaction:R000475"
        guide = fresh.workbench.load_active_guide()
        assert guide["kind"] == "cache_compaction_rhythm_guide"
        assert guide["compaction_plan"]["before_chars"] == 12000
        assert guide["source_refs"] == ["cache_compaction_debt"]

    def test_spec474_stale_cache_compaction_due_flag_is_cleared(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("cache_compaction_due", True)
        state = rt.sm.load()
        context = RoundContext(
            round_num=475,
            round_type="rhythm",
            state=state,
            flags=state["base"]["heartbeat_flags"],
        )

        guide_id = rt._materialize_runtime_rhythm_guide(context)

        assert guide_id is None
        assert rt.sm.get("base.heartbeat_flags.cache_compaction_due") is False
        assert rt.workbench.get("base.active_guides.rhythm") is None

    def test_spec474_cleanup_does_not_rewrite_existing_compaction_debt(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        rt.ctx_store.save_cache_compaction_debt({
            "status": "due",
            "source": "last_cache_stats",
            "cache_stats": {
                "lately_trimmed": True,
                "cache_compaction_required": True,
            },
            "candidate_ids": ["R1", "R2"],
            "plan": {
                "before_chars": 180,
                "target_chars": 111,
                "shards": [
                    {"shard_id": "shard_01", "source_block_ids": ["R1"]},
                    {"shard_id": "shard_02", "source_block_ids": ["R2"]},
                ],
            },
        }, 474)
        rt.ctx_store.update_cache_compaction_debt(completed_shards=["shard_01"])

        receipt = rt._record_cache_compaction_rhythm_if_needed(475)
        debt = rt.ctx_store.load_cache_compaction_debt()

        assert receipt["status"] == "due"
        assert receipt["source"] == "cache_compaction_debt"
        assert debt["created_round"] == 474
        assert debt["completed_shards"] == ["shard_01"]

    def test_spec472_reaction_iteration_materializes_next_rhythm_guide_after_settlement(
            self, tmp_path):
        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()
        state["base"]["heartbeat_flags"] = {
            "calendar_day_due": True,
            "calendar_week_due": True,
        }
        state["base"]["runtime"] = {
            "guide_completed_flags": ["calendar_day_due"],
        }

        guide_id = rt.reaction_loop_runner._materialize_next_runtime_rhythm_guide_if_needed(
            state,
            "rhythm",
            472,
            {"calendar_day_due"},
        )

        assert guide_id == "rhythm:calendar_week:R000472"
        assert rt.workbench.get("base.active_guides.rhythm") == guide_id
        item = rt.workbench.load_active_guide()["items"][0]
        assert item["item_id"] == "calendar_week_due"
        assert item["status"] == "open"
        assert item["options"] == [{
            "option_id": "write_chronicle",
            "required_fields": ["content"],
            "allowed_fields": ["content", "reason"],
        }]

    def test_spec473_materialize_clears_stale_api_emergency_before_guide(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set_flag("context_pressure", True)
        state = rt.sm.load()
        context = RoundContext(
            round_num=473,
            round_type="rhythm",
            state=state,
            flags=state["base"]["heartbeat_flags"],
        )

        guide_id = rt._materialize_runtime_rhythm_guide(context)

        assert rt.sm.get("base.heartbeat_flags.api_degraded") is False
        assert guide_id == "rhythm:context_pressure:R000473"
        assert rt.workbench.load_active_guide()["kind"] == "context_pressure_rhythm_guide"

    def test_spec473_materialize_keeps_active_api_emergency_when_still_degraded(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "timeout", "still down")
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set_flag("context_pressure", True)
        state = rt.sm.load()
        context = RoundContext(
            round_num=473,
            round_type="rhythm",
            state=state,
            flags=state["base"]["heartbeat_flags"],
        )

        guide_id = rt._materialize_runtime_rhythm_guide(context)

        assert rt.sm.get("base.heartbeat_flags.api_degraded") is True
        assert guide_id == "rhythm:emergency:R000473"
        assert rt.workbench.load_active_guide()["kind"] == "emergency_handling_guide"

    def test_spec473_materialize_clears_stale_process_down_and_continues_agenda(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        rt.sm.set_flag("process_down", True)
        rt.sm.set_flag("context_pressure", True)
        state = rt.sm.load()
        context = RoundContext(
            round_num=473,
            round_type="rhythm",
            state=state,
            flags=state["base"]["heartbeat_flags"],
        )

        guide_id = rt._materialize_runtime_rhythm_guide(context)

        assert rt.sm.get("base.heartbeat_flags.process_down") is False
        assert guide_id == "rhythm:context_pressure:R000473"
        assert rt.workbench.load_active_guide()["kind"] == "context_pressure_rhythm_guide"

    def test_spec473_materialize_clears_all_stale_emergency_then_next_guide(
            self, tmp_path):
        from engines.round_context import RoundContext

        rt = self._make_runtime(tmp_path)
        rt.connectivity_store.log_latency("primary", "ok", "recovered")
        rt.sm.set_flag("api_degraded", True)
        rt.sm.set_flag("process_down", True)
        rt.sm.set_flag("context_pressure", True)
        state = rt.sm.load()
        context = RoundContext(
            round_num=473,
            round_type="rhythm",
            state=state,
            flags=state["base"]["heartbeat_flags"],
        )

        guide_id = rt._materialize_runtime_rhythm_guide(context)

        assert rt.sm.get("base.heartbeat_flags.api_degraded") is False
        assert rt.sm.get("base.heartbeat_flags.process_down") is False
        assert guide_id == "rhythm:context_pressure:R000473"

    def test_spec464_runtime_materializes_current_rhythm_guide(
            self, tmp_path, monkeypatch):
        from engines.round_context import SetupResult

        rt = self._make_runtime(tmp_path)
        setup_result = SetupResult(
            raw_result={"response": ""},
            intent={"security_verdict": "pass", "mount_requests": []},
            interaction_meta=self._confirmed_meta(),
            user_input_text="",
            setup_messages=[],
            internal_handoff=[],
        )
        monkeypatch.setattr(rt.setup_runner, "run", lambda context: setup_result)

        def fake_reaction(*args, **kwargs):
            guide = rt.workbench.load_active_guide()
            assert rt.workbench.get("base.active_guides.rhythm") == "rhythm:main_axis:R000001"
            assert guide["kind"] == "main_axis_rhythm_guide"
            return {
                "response": "节律 guide 已物化。",
                "_interaction_meta": self._confirmed_meta(),
            }

        monkeypatch.setattr(rt, "_run_reaction_loop", fake_reaction)
        monkeypatch.setattr(rt, "_run_cleanup", lambda *args, **kwargs: None)
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("rhythm", rt.sm.load(), {"rhythm_due": True})

    def test_standby_setup_skip_bypasses_reaction_but_runs_cleanup(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        calls = []

        monkeypatch.setattr(rt.executor, "call",
                            lambda step, system, messages: {
                                "response": "",
                                "tool_call_envelopes": [self._native_tool_envelope(
                                    "setup_finalize",
                                    {
                                        "security_verdict": "pass",
                                        "standby_skip_reaction": True,
                                    },
                                    tool_family="substrate_tool",
                                    tool_class="sync_tool",
                                    risk="high",
                                )],
                            })
        monkeypatch.setattr(rt, "_run_reaction_loop",
                            lambda *args, **kwargs: calls.append("reaction") or {
                                "response": "不应进入反应步",
                            })
        monkeypatch.setattr(rt, "_run_cleanup",
                            lambda *args, **kwargs: calls.append(
                                ("cleanup", args[0], args[2])))
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("standby", rt.sm.load(), {"standby_due": True})

        assert "reaction" not in calls
        assert calls and calls[0][0] == "cleanup"
        assert calls[0][1] == "standby"
        assert "_standby_reaction_hint" not in calls[0][2]

    def test_non_standby_ignores_standby_skip_field(
            self, tmp_path, monkeypatch):
        rt = self._make_runtime(tmp_path)
        calls = []

        monkeypatch.setattr(rt.executor, "call",
                            lambda step, system, messages: {
                                "response": "",
                                "tool_call_envelopes": [self._native_tool_envelope(
                                    "setup_finalize",
                                    {
                                        "security_verdict": "pass",
                                        "standby_skip_reaction": True,
                                    },
                                    tool_family="substrate_tool",
                                    tool_class="sync_tool",
                                    risk="high",
                                )],
                            })
        monkeypatch.setattr(rt.hb, "dequeue_messages", lambda: [
            "普通交互轮不消费待命字段。"
        ])
        monkeypatch.setattr(rt, "_run_reaction_loop",
                            lambda *args, **kwargs: calls.append("reaction") or {
                                "response": "进入反应步",
                            })
        monkeypatch.setattr(rt, "_run_cleanup",
                            lambda *args, **kwargs: calls.append("cleanup"))
        monkeypatch.setattr(rt.hb, "pause", lambda: None)
        monkeypatch.setattr(rt.hb, "resume", lambda: None)

        rt._run_one_round("interactive", rt.sm.load(), {
            "user_message_waiting": True,
        })

        assert "reaction" in calls
        assert "cleanup" in calls
