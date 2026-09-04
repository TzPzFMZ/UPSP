import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin


class TestRuntimeCleanupArtifacts(RuntimeTestMixin):
    def test_cleanup_round_snapshot_uses_assembler_context_dir(self, tmp_path, monkeypatch):
        from engines.runtime import Runtime
        from data.context_store import ContextStore
        from data.state_store import StateStore
        from assembly.context import ContextAssembler
        import json

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        context_dir = tmp_path / "context"
        for step in ["setup", "reaction", "cleanup"]:
            step_dir = context_dir / step
            step_dir.mkdir(parents=True)
            (step_dir / "step.json").write_text(
                json.dumps([{"role": "system", "content": step}], ensure_ascii=False),
                encoding="utf-8",
            )

        ctx_store = ContextStore(
            state_store=sm,
            cache_dir=str(tmp_path / "context_cache"),
            raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
        )
        assembler = ContextAssembler(
            state_store=sm,
            context_dir=str(context_dir),
            context_store=ctx_store,
        )
        rt = Runtime(state_store=sm, assembler=assembler, ctx_store=ctx_store)

        pipeline = rt.cleanup_pipeline
        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt.executor, "call", lambda *a, **kw: {"response": ""})
        monkeypatch.setattr(pipeline, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(pipeline, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", lambda *a, **kw: ("sys", []))

        rt._run_cleanup("interactive", sm.load(), {"response": "reaction"}, 12)

        round_path = context_dir / "round" / "round_12.jsonl"
        assert round_path.is_file()
        events = [
            json.loads(line)
            for line in round_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        step_events = [
            event for event in events
            if event["event_type"] == "step_input_snapshot"
        ]
        assert [event["phase"] for event in step_events[:3]] == [
            "setup",
            "reaction",
            "cleanup",
        ]
        cleanup_events = [
            event for event in step_events
            if event["phase"] == "cleanup"
        ]
        assert [event["iteration"] for event in cleanup_events] == [1, 2, 3]
        assert all("messages" not in event["payload"] for event in step_events[:3])
        assert [event["payload"]["error"] for event in step_events[:3]] == [
            "legacy_step_json_rejected",
            "legacy_step_json_rejected",
            "legacy_step_json_rejected",
        ]

    def test_setup_exception_does_not_backfill_stale_reaction_snapshot(
            self, tmp_path, monkeypatch):
        from engines.runtime import Runtime
        from data.context_store import ContextStore
        from data.state_store import StateStore
        from assembly.context import ContextAssembler
        import json

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        context_dir = tmp_path / "context"
        for step, content in (
                ("setup", "current setup"),
                ("reaction", "stale reaction"),
                ("cleanup", "current cleanup")):
            step_dir = context_dir / step
            step_dir.mkdir(parents=True)
            (step_dir / "step.json").write_text(
                json.dumps([{"role": "system", "content": content}],
                           ensure_ascii=False),
                encoding="utf-8",
            )
            (step_dir / "manifest.json").write_text(
                json.dumps({"step": step}, ensure_ascii=False),
                encoding="utf-8",
            )

        ctx_store = ContextStore(
            state_store=sm,
            cache_dir=str(tmp_path / "context_cache"),
            raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
        )
        assembler = ContextAssembler(
            state_store=sm,
            context_dir=str(context_dir),
            context_store=ctx_store,
        )
        rt = Runtime(state_store=sm, assembler=assembler, ctx_store=ctx_store)

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.executor, "call", lambda *a, **kw: {"response": ""})
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        monkeypatch.setattr(
            rt.assembler,
            "assemble_cleanup",
            lambda *a, **kw: ("sys", [{"role": "system", "content": "cleanup llm"}]),
        )

        rt._run_cleanup("interactive", sm.load(), {
            "aborted": True,
            "response": "",
            "error": "setup step exception: provider_native_tool_empty_output",
            "_failed_phase": "setup",
        }, 13)

        round_path = context_dir / "round" / "round_13.jsonl"
        events = [
            json.loads(line)
            for line in round_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        step_events = [
            event for event in events
            if event["event_type"] == "step_input_snapshot"
        ]
        assert step_events[0]["phase"] == "setup"
        assert not any(event["phase"] == "reaction" for event in step_events)
        cleanup_events = [
            event for event in step_events
            if event["phase"] == "cleanup"
        ]
        assert [event["iteration"] for event in cleanup_events] == [1, 2, 3]
        assert all(
            "stale reaction" not in json.dumps(event, ensure_ascii=False)
            for event in step_events
        )

    def test_spec324_reaction_exception_writes_runtime_audit(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)
        context_dir = tmp_path / "context"
        for step in ["setup", "reaction", "cleanup"]:
            step_dir = context_dir / step
            step_dir.mkdir(parents=True)
            (step_dir / "step.json").write_text(
                json.dumps([{"role": "system", "content": step}], ensure_ascii=False),
                encoding="utf-8",
            )
            (step_dir / "manifest.json").write_text(
                json.dumps({"step": step}, ensure_ascii=False),
                encoding="utf-8",
            )

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.executor, "call", lambda *a, **kw: {"response": ""})
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        monkeypatch.setattr(
            rt.assembler,
            "assemble_cleanup",
            lambda *a, **kw: ("sys", [{"role": "system", "content": "cleanup"}]),
        )

        rt._run_cleanup("relay", rt.sm.load(), {
            "aborted": True,
            "response": "",
            "error": "reaction step exception: HTTP 402: Insufficient Balance",
            "_failed_phase": "reaction",
        }, 14)

        round_path = context_dir / "round" / "round_14.jsonl"
        events = [
            json.loads(line)
            for line in round_path.read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        runtime_events = [
            event for event in events
            if event["event_type"] == "runtime_audit"
        ]
        assert len(runtime_events) == 1
        runtime_payload = runtime_events[0]["payload"]
        assert runtime_payload["status"] == "issues"
        assert runtime_payload["issues"] == ["runtime_exception:reaction"]
        assert runtime_payload["runtime_exception"]["failed_phase"] == "reaction"
        assert "Insufficient Balance" in runtime_payload["runtime_exception"]["error"]

    def test_cleanup_round_snapshot_prunes_fifo_and_appends_state_backup(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)
        context_dir = tmp_path / "context"
        round_dir = context_dir / "round"
        round_dir.mkdir(parents=True)
        for round_num in (10, 11):
            (round_dir / f"round_{round_num}.jsonl").write_text(
                json.dumps({"event_type": "round_closed", "round": round_num},
                           ensure_ascii=False) + "\n",
                encoding="utf-8",
            )
        (round_dir / "round_9.json").write_text("{}", encoding="utf-8")
        for step in ["setup", "reaction", "cleanup"]:
            step_dir = context_dir / step
            step_dir.mkdir(parents=True, exist_ok=True)
            (step_dir / "step.json").write_text("[]", encoding="utf-8")

        class StubConfig:
            def get_audit_params(self):
                return {"round_snapshot_retention": 2, "state_backup_retention": 8}

        rt.cfg = StubConfig()
        pipeline = rt.cleanup_pipeline
        monkeypatch.setattr(rt.heat, "tick_decay", lambda round_num=None: None)
        monkeypatch.setattr(rt.executor, "call", lambda *a, **kw: {"response": ""})
        monkeypatch.setattr(pipeline, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(pipeline, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        monkeypatch.setattr(rt.assembler, "assemble_cleanup", lambda *a, **kw: ("sys", []))

        captured = []

        class StubStateBackups:
            def append_backup(self, round_num, state, reason="cleanup"):
                captured.append((round_num, reason, state["base"]["meta"]["total_round"]))

        rt.state_backup_store = StubStateBackups()

        rt._run_cleanup("interactive", rt.sm.load(), {"response": "reaction"}, 12)

        assert sorted(p.name for p in round_dir.glob("round_*.jsonl")) == [
            "round_11.jsonl",
            "round_12.jsonl",
        ]
        assert (round_dir / "round_9.json").is_file()
        assert captured == [(12, "cleanup", 0)]

    def test_cleanup_internal_tasks_are_written_as_transient_material(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)
        state = rt.sm.load()
        state["base"]["meta"]["total_round"] = 26
        state["base"]["heartbeat_flags"]["calendar_day_due"] = True
        rt.sm.save(state)

        captured = {}

        def fake_call(step, system, messages):
            captured["messages"] = list(messages)
            return {
                "response": "",
                "tool_call_envelopes": [self._native_tool_envelope(
                    "cleanup_finalize",
                    {},
                    tool_family="substrate_tool",
                    tool_class="sync_tool",
                )],
            }

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.executor, "call", fake_call)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)

        rt._run_cleanup("rhythm", rt.sm.load(), {
            "response": "反应步最终回复",
            "_reaction_internal_handoff": "反应步结构化交接哨兵",
        }, 26, "本轮用户输入")

        assert not any("STM遗忘交接哨兵" in m.get("content", "") for m in captured["messages"])
        assert not any("LTM降格交接哨兵" in m.get("content", "") for m in captured["messages"])
        assert not any("反应步结构化交接哨兵" in m.get("content", "") for m in captured["messages"])
        step_messages = captured["messages"]
        assert not any("STM遗忘交接哨兵" in m.get("content", "") for m in step_messages)
        assert not any("LTM降格交接哨兵" in m.get("content", "") for m in step_messages)
        assert not any("反应步结构化交接哨兵" in m.get("content", "") for m in step_messages)
        now_text = "\n".join(m.get("content", "") for m in step_messages)
        assert "【本轮资料】" in now_text
        assert "善后内部任务：STM 遗忘压缩" not in now_text
        assert "善后内部任务：LTM 降格处理" not in now_text

    def test_cleanup_prepares_pressure_only_after_final_cache_save(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)
        events = []
        transitions = []

        class CandidateContext:
            def get_last_cache_stats(self):
                return {"lately_trimmed": True}

            def get_lately_compact_ratio(self):
                return 0.618

            def save_round_to_cache(self, *args, **kwargs):
                events.append("cache_saved")

            def transition_current_cache(self, **kwargs):
                events.append("cache_drained")
                transitions.append(dict(kwargs))
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "applied",
                }

            def prepare_lately_pressure_compaction(self, round_num, observation):
                events.append("pressure_prepared")
                assert round_num == 28
                assert observation["kind"] == "token_ratio"
                return {
                    "status": "prepared",
                    "reason": "cache_compaction_due",
                }

        rt.ctx_store = CandidateContext()
        rt.services.cache_pressure_observation = {
            "kind": "token_ratio",
            "input_tokens": 900,
            "context_window": 1000,
            "usage_ratio": 0.9,
        }
        captured = {}
        result = {"response": "反应步最终回复"}

        def fake_call(step, system, messages):
            captured["messages"] = list(messages)
            return {"response": ""}

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.executor, "call", fake_call)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)

        rt._run_cleanup("interactive", rt.sm.load(), result, 28, "本轮用户输入")

        assert events == [
            "cache_drained",
            "cache_drained",
            "cache_drained",
            "cache_saved",
            "cache_drained",
            "pressure_prepared",
        ]
        assert [item["boundary"] for item in transitions] == [
            "cleanup_provider_return",
            "cleanup_provider_return",
            "cleanup_provider_return",
            "round_closeout",
        ]
        assert all(item["expire_call_transients"] is True
                   for item in transitions)
        assert result["_lately_compression_pending"]["status"] == "prepared"
        assert rt.services.cache_pressure_observation == {}
        pressure_events = [
            event for event in rt.audit.get_store().read_events(28)
            if event.get("event_type") == "lately_pressure_compaction"
        ]
        assert len(pressure_events) == 1
        assert pressure_events[0]["payload"]["status"] == "prepared"
        assert pressure_events[0]["payload"]["pressure_observation"][
            "usage_ratio"
        ] == 0.9
        assert not any("最近缓存压缩提醒" in m.get("content", "") for m in captured["messages"])
        assert not any("<!-- 最近缓存 lately -->" in m.get("content", "") for m in captured["messages"])
        assert not any("cleanup_finalize.lately_compression" in m.get("content", "") for m in captured["messages"])
        assert not any("候选 1" in m.get("content", "") for m in captured["messages"])
        assert not any("工具输出压缩候选哨兵" in m.get("content", "") for m in captured["messages"])
        assert not any("source_block_id" in m.get("content", "") for m in captured["messages"])
        assert not any("compact_ratio=" in m.get("content", "") for m in captured["messages"])
        assert not any("target_chars=" in m.get("content", "") for m in captured["messages"])
        step_messages = captured["messages"]
        assert not any("最近缓存压缩提醒" in m.get("content", "") for m in step_messages)
        assert not any("候选 1" in m.get("content", "") for m in step_messages)
        assert not any("工具输出压缩候选哨兵" in m.get("content", "") for m in step_messages)
        assert not any("source_block_id" in m.get("content", "") for m in step_messages)

    def test_cleanup_skips_lately_compaction_when_ratio_is_one(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)

        class CandidateContext:
            def get_last_cache_stats(self):
                return {"lately_trimmed": True}

            def get_lately_compact_ratio(self):
                return 1.0

            def save_round_to_cache(self, *args, **kwargs):
                pass

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                }

        rt.ctx_store = CandidateContext()
        captured = {}

        def fake_call(step, system, messages):
            captured["messages"] = list(messages)
            return {"response": ""}

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.executor, "call", fake_call)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)

        rt._run_cleanup("interactive", rt.sm.load(), {"response": "反应步最终回复"}, 30, "本轮用户输入")

        assert not any("压缩比例：1.0" in m.get("content", "") for m in captured["messages"])
        assert not any("目标字符数：" in m.get("content", "") for m in captured["messages"])
        assert not any("compact_ratio=1.0" in m.get("content", "") for m in captured["messages"])
        assert not any("target_chars=" in m.get("content", "") for m in captured["messages"])
        step_messages = captured["messages"]
        assert not any("压缩比例：1.0" in m.get("content", "") for m in step_messages)
        assert not any("目标字符数：" in m.get("content", "") for m in step_messages)
        assert not any("compact_ratio=1.0" in m.get("content", "") for m in step_messages)
        assert not any("target_chars=" in m.get("content", "") for m in step_messages)

    def test_cleanup_skips_lately_compression_candidates_without_trim_event(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)

        class CandidateContext:
            def get_last_cache_stats(self):
                return {"lately_trimmed": False}

            def save_round_to_cache(self, *args, **kwargs):
                pass

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                }

        rt.ctx_store = CandidateContext()
        captured = {}

        def fake_call(step, system, messages):
            captured["messages"] = list(messages)
            return {"response": ""}

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.executor, "call", fake_call)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)

        rt._run_cleanup("interactive", rt.sm.load(), {"response": "反应步最终回复"}, 29, "本轮用户输入")

        assert not any("工具输出压缩候选哨兵" in m.get("content", "") for m in captured["messages"])
        step_messages = captured["messages"]
        assert not any("工具输出压缩候选哨兵" in m.get("content", "") for m in step_messages)

    def test_cleanup_no_longer_mounts_retired_tool_call_ledger(self, tmp_path, monkeypatch):
        import json

        rt = self._make_runtime(tmp_path)
        assert not hasattr(rt, "tool_call_ledger")

        captured = {}

        def fake_call(step, system, messages):
            captured["messages"] = list(messages)
            return {"response": ""}

        monkeypatch.setattr(rt.heat, "tick_decay", lambda: None)
        monkeypatch.setattr(rt.executor, "call", fake_call)
        monkeypatch.setattr(rt, "_process_cleanup_output", lambda *a, **kw: None)
        monkeypatch.setattr(rt, "_process_rest_cycle", lambda *a, **kw: None)
        monkeypatch.setattr(rt.ctx_store, "save_round_to_cache", lambda *a, **kw: None)
        rt._run_cleanup("interactive", rt.sm.load(), {"response": "反应步最终回复"}, 27, "本轮用户输入")

        assert not any("工具调用台账" in m.get("content", "") for m in captured["messages"])
        step_messages = captured["messages"]
        assert not any("工具调用台账" in m.get("content", "") for m in step_messages)
        assert not (tmp_path / "tool_call_ledger.jsonl").exists()
