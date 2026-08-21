import os
import sys

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, TESTS_DIR)
sys.path.insert(0, os.path.join(TESTS_DIR, ".."))

from runtime_test_helpers import RuntimeTestMixin, ScriptedExecutor


class TestRuntimeReactionGeneralToolsWrite(RuntimeTestMixin):
    @staticmethod
    def _enable_legacy_shell_for_historical_pipeline_test(monkeypatch):
        from logic.protocol_tools import TOOL_DEFINITIONS

        monkeypatch.setitem(
            TOOL_DEFINITIONS["shell_command"], "status", "enabled"
        )

    def test_spec428_duplicate_general_tool_fact_has_actionable_short_fields(self):
        from logic.general_tools import format_general_tool_fact

        fact = format_general_tool_fact({
            "tool_id": "shell_command",
            "status": "rejected",
            "reason": "duplicate_tool_result_satisfied",
            "tool_signature_payload": {
                "tool_id": "shell_command",
                "arguments": {
                    "cwd": ".",
                    "command": "python -V",
                },
            },
            "duplicate_of_call_id": "call_shell_first",
            "previous_status": "ok",
        })

        assert "duplicate_tool_result_satisfied" in fact
        assert "重复对象：call_shell_first。" in fact
        assert "上次状态：ok。" in fact
        assert "参数摘要：tool_id=shell_command; cwd=.; command=python -V。" in fact
        assert "下一步：直接使用已有工具事实，或改参数推进下一步。" in fact
        assert "工作目录：未记录" not in fact

    def test_spec071_file_edit_general_tool_uses_independent_result_channel(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        patch = "--- a/notes.md\n+++ b/notes.md\n@@ -1,1 +1,1 @@\n-old\n+new"

        class RecordingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                    "boundary": kwargs["boundary"],
                }

            def load_cache_compaction_debt(self):
                return {}

        def fake_execute(request):
            return {
                "tool_id": "file_edit",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_edit_handler",
                "permission_scope": "workspace_patch_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "change_summary": "applied patch",
                "lines_added": 1,
                "lines_removed": 1,
                "protocol_tool_receipt": False,
            }

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "file_edit",
                    {
                        "path": "notes.md",
                        "purpose": "apply patch",
                        "patch": patch,
                    },
                    call_id="call_file_edit",
                    tool_class="focus_tool",
                    risk="high",
                )],
            },
            {"response": "file edit observed", "tool_call_envelopes": []},
        )
        rt.ctx_store = RecordingContext()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "file_edit guide" if tool_id == "file_edit" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "本轮已经尝试编辑文件：notes.md。" in second_call_text
        assert "applied patch" in second_call_text
        assert [r["status"] for r in result["_general_tool_results"]] == ["ok"]
        assert result["_general_tool_results"][0]["call_id"] == "call_file_edit"
        assert result["_protocol_tool_receipts"] == []
        assert result["_tool_transaction_audit"]["status"] in {"ok", "issues_found"}
        assert all(entry[3] != "protocol_tool_receipt" for entry in rt.ctx_store.entries)
        assert any(entry[3] == "tool_fact" for entry in rt.ctx_store.entries)

    def test_spec072_shell_command_general_tool_uses_independent_result_channel(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        self._enable_legacy_shell_for_historical_pipeline_test(monkeypatch)

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RecordingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                    "boundary": kwargs["boundary"],
                }

            def load_cache_compaction_debt(self):
                return {}

        def fake_execute(request):
            return {
                "tool_id": "shell_command",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "shell_command_handler",
                "permission_scope": "workspace_shell_allowlist",
                "result_kind": "general_tool_result",
                "cwd": request.get("cwd"),
                "command": request.get("command"),
                "stdout": "Python 3.x",
                "stderr": "",
                "exit_code": 0,
                "protocol_tool_receipt": False,
            }

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "shell_command",
                    {
                        "cwd": ".",
                        "command": "python -V",
                        "purpose": "check version",
                        "timeout_ms": 3000,
                    },
                    call_id="call_shell",
                    tool_class="focus_tool",
                    risk="medium",
                )],
            },
            {"response": "shell observed", "tool_call_envelopes": []},
        )
        rt.ctx_store = RecordingContext()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: "shell guide" if tool_id == "shell_command" else "",
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "本轮 shell 命令执行成功。" in second_call_text
        assert "标准输出约 1 行" in second_call_text
        assert [r["status"] for r in result["_general_tool_results"]] == ["ok"]
        assert result["_general_tool_results"][0]["call_id"] == "call_shell"
        assert result["_protocol_tool_receipts"] == []
        assert all(entry[3] != "protocol_tool_receipt" for entry in rt.ctx_store.entries)
        assert any(entry[3] == "tool_fact" for entry in rt.ctx_store.entries)

    def test_spec428_shell_command_duplicate_guard_counts_by_iteration(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        self._enable_legacy_shell_for_historical_pipeline_test(monkeypatch)

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RepeatingShellExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append((step, list(messages)))
                if step == "final_reply":
                    return {"response": "shell loop guarded", "tool_call_envelopes": []}
                if len([item for item in self.calls if item[0] == "reaction"]) <= 4:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "shell_command",
                            {
                                "cwd": ".",
                                "command": "dir /b sandbox\\round_25",
                                "purpose": "inspect files",
                                "timeout_ms": 3000,
                            },
                            call_id=f"call_shell_repeat_{len(self.calls)}",
                            tool_class="focus_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "已有 shell 结果，停止重复读取。", "tool_call_envelopes": []}

        executed = []

        def fake_execute(request):
            executed.append(dict(request))
            return {
                "tool_id": "shell_command",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "shell_command_handler",
                "permission_scope": "workspace_shell_allowlist",
                "result_kind": "general_tool_result",
                "cwd": request.get("cwd"),
                "command": request.get("command"),
                "stdout": "slugger.py\ntests\\test_slugger.py\n",
                "stderr": "",
                "exit_code": 0,
                "protocol_tool_receipt": False,
            }

        rt.executor = RepeatingShellExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        reaction_texts = [
            "\n".join(m.get("content", "") for m in messages)
            for step, messages in rt.executor.calls
            if step == "reaction"
        ]

        assert len(executed) == 1
        assert [
            item.get("reason")
            for item in result["_general_tool_results"]
            if item.get("status") == "rejected"
        ] == [
            "duplicate_tool_result_satisfied",
            "duplicate_tool_result_satisfied",
            "duplicate_tool_result_satisfied",
        ]
        assert "工具重复提醒" in reaction_texts[2]
        assert "closeout_only" not in reaction_texts[2]
        assert "工具重复警告" in reaction_texts[3]
        assert "下一迭代仍重复同一工具签名" in reaction_texts[3]
        assert "closeout_only" not in reaction_texts[4]
        assert "停止" in reaction_texts[4] or "reaction_finalize" in reaction_texts[4]
        assert any(
            item.get("status") == "general_tool_duplicate_stop_or_finalize"
            for item in result["_reaction_loop_guard_receipts"]
        )

    def test_spec429_file_edit_patch_mismatch_variants_advance_duplicate_guard(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self
        path = "workspace_note.md"

        patches = [
            "--- workspace_note.md\n+++ workspace_note.md\n@@\n-status: TODO\n+status: IN_PROGRESS",
            "--- D:/AI_WORKSPACE/base/workspace_note.md\n"
            "+++ D:/AI_WORKSPACE/base/workspace_note.md\n"
            "@@\n-status: TODO\n+status: IN_PROGRESS",
            "--- a/workspace_note.md\n+++ b/workspace_note.md\n@@\n-status: TODO\n+status: IN_PROGRESS\n+log: started",
            "@@\n-status: TODO\n+status: IN_PROGRESS\n+log: retry",
        ]

        class RepeatingFileEditExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append((step, list(messages)))
                if step == "final_reply":
                    return {"response": "file edit loop guarded", "tool_call_envelopes": []}
                reaction_count = len([
                    item for item in self.calls if item[0] == "reaction"
                ])
                if reaction_count <= 5:
                    patch = patches[(reaction_count - 1) % len(patches)]
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "file_edit",
                            {
                                "path": path,
                                "purpose": f"mark task progress attempt {reaction_count}",
                                "patch": patch,
                            },
                            call_id=f"call_file_edit_retry_{reaction_count}",
                            tool_class="focus_tool",
                            risk="high",
                        )],
                    }
                return {"response": "停止重复修改同一文件。", "tool_call_envelopes": []}

        executed = []

        def fake_execute(request):
            executed.append(dict(request))
            return {
                "tool_id": "file_edit",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "rejected",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "file_edit_handler",
                "permission_scope": "workspace_patch_allowlist",
                "result_kind": "general_tool_result",
                "path": request.get("path"),
                "reason": "patch_context_mismatch",
                "detail": "target context not found",
                "protocol_tool_receipt": False,
            }

        rt.executor = RepeatingFileEditExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        reaction_texts = [
            "\n".join(m.get("content", "") for m in messages)
            for step, messages in rt.executor.calls
            if step == "reaction"
        ]

        assert len(executed) == 3
        assert {
            item.get("duplicate_guard_key")
            for item in result["_general_tool_results"]
            if item.get("tool_id") == "file_edit"
            and item.get("reason") == "patch_context_mismatch"
        } == {
            result["_general_tool_results"][0]["duplicate_guard_key"],
        }
        assert "工具重复提醒" in reaction_texts[2]
        assert "重复对象：call_file_edit_retry_1" in reaction_texts[2]
        assert "上次原因：patch_context_mismatch" in reaction_texts[2]
        assert "工具重复警告" in reaction_texts[3]
        assert "上次失败或被拒绝" in reaction_texts[3]
        assert "closeout_only" not in reaction_texts[3]
        assert "停止" in reaction_texts[3] or "reaction_finalize" in reaction_texts[3]
        assert any(
            item.get("status") == "general_tool_duplicate_stop_or_finalize"
            for item in result["_reaction_loop_guard_receipts"]
        )

    def test_spec428_same_iteration_duplicate_signature_counts_once(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        self._enable_legacy_shell_for_historical_pipeline_test(monkeypatch)

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class BatchDuplicateShellExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append((step, list(messages)))
                if step == "final_reply":
                    return {"response": "batch duplicate guarded", "tool_call_envelopes": []}
                reaction_count = len([item for item in self.calls if item[0] == "reaction"])
                if reaction_count == 1:
                    envelopes = [helper._native_tool_envelope(
                        "shell_command",
                        {
                            "cwd": ".",
                            "command": "dir /b sandbox\\round_25",
                            "purpose": "inspect files",
                            "timeout_ms": 3000,
                        },
                        call_id="call_shell_first",
                        tool_class="focus_tool",
                        risk="medium",
                    )]
                    return {"response": "", "tool_call_envelopes": envelopes}
                if reaction_count == 2:
                    envelopes = [
                        helper._native_tool_envelope(
                            "shell_command",
                            {
                                "cwd": ".",
                                "command": "dir /b sandbox\\round_25",
                                "purpose": "inspect files",
                                "timeout_ms": 3000,
                            },
                            call_id=f"call_shell_dup_{index}",
                            tool_class="focus_tool",
                            risk="medium",
                        )
                        for index in (1, 2)
                    ]
                    return {"response": "", "tool_call_envelopes": envelopes}
                return {"response": "同迭代重复已提醒，正常收束。", "tool_call_envelopes": []}

        executed = []

        def fake_execute(request):
            executed.append(dict(request))
            return {
                "tool_id": "shell_command",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "shell_command_handler",
                "permission_scope": "workspace_shell_allowlist",
                "result_kind": "general_tool_result",
                "cwd": request.get("cwd"),
                "command": request.get("command"),
                "stdout": "slugger.py\n",
                "stderr": "",
                "exit_code": 0,
                "protocol_tool_receipt": False,
            }

        rt.executor = BatchDuplicateShellExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        reaction_texts = [
            "\n".join(m.get("content", "") for m in messages)
            for step, messages in rt.executor.calls
            if step == "reaction"
        ]

        assert len(executed) == 1
        assert len([
            item for item in result["_general_tool_results"]
            if item.get("status") == "rejected"
        ]) == 2
        assert "工具重复提醒" in reaction_texts[2]
        assert "本迭代同一参数重复提交 2 次" in reaction_texts[2]
        assert "closeout_only" not in "\n".join(reaction_texts)
        assert not any(
            item.get("status") == "general_tool_duplicate_closeout"
            for item in result["_reaction_loop_guard_receipts"]
        )

    def test_spec428_effective_new_general_tool_progress_resets_duplicate_streak(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        self._enable_legacy_shell_for_historical_pipeline_test(monkeypatch)

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        def shell(command, call_id):
            return helper._native_tool_envelope(
                "shell_command",
                {
                    "cwd": ".",
                    "command": command,
                    "purpose": "inspect files",
                    "timeout_ms": 3000,
                },
                call_id=call_id,
                tool_class="focus_tool",
                risk="medium",
            )

        class ProgressThenDuplicateShellExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append((step, list(messages)))
                if step == "final_reply":
                    return {"response": "progress resets duplicate streak", "tool_call_envelopes": []}
                reaction_count = len([item for item in self.calls if item[0] == "reaction"])
                if reaction_count == 1:
                    return {"response": "", "tool_call_envelopes": [shell("dir /b sandbox\\round_25", "call_shell_a")]}
                if reaction_count == 2:
                    return {"response": "", "tool_call_envelopes": [
                        shell("dir /b sandbox\\round_25", "call_shell_a_dup"),
                        shell("dir /b sandbox\\round_26", "call_shell_b"),
                    ]}
                if reaction_count == 3:
                    return {"response": "", "tool_call_envelopes": [shell("dir /b sandbox\\round_25", "call_shell_a_dup_again")]}
                return {"response": "新进展已打断重复阶梯。", "tool_call_envelopes": []}

        executed = []

        def fake_execute(request):
            executed.append(dict(request))
            return {
                "tool_id": "shell_command",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "shell_command_handler",
                "permission_scope": "workspace_shell_allowlist",
                "result_kind": "general_tool_result",
                "cwd": request.get("cwd"),
                "command": request.get("command"),
                "stdout": "ok\n",
                "stderr": "",
                "exit_code": 0,
                "protocol_tool_receipt": False,
            }

        rt.executor = ProgressThenDuplicateShellExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        reaction_texts = [
            "\n".join(m.get("content", "") for m in messages)
            for step, messages in rt.executor.calls
            if step == "reaction"
        ]

        assert [item["command"] for item in executed] == [
            "dir /b sandbox\\round_25",
            "dir /b sandbox\\round_26",
        ]
        assert "工具重复提醒" in reaction_texts[2]
        assert "工具重复提醒" in reaction_texts[3]
        assert "工具重复警告" not in "\n".join(reaction_texts)
        assert "closeout_only" not in "\n".join(reaction_texts)
        assert not any(
            item.get("status") == "general_tool_duplicate_closeout"
            for item in result["_reaction_loop_guard_receipts"]
        )

    def test_spec428_multi_signature_duplicates_without_main_chain_do_not_advance(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        self._enable_legacy_shell_for_historical_pipeline_test(monkeypatch)

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        def shell(command, call_id):
            return helper._native_tool_envelope(
                "shell_command",
                {
                    "cwd": ".",
                    "command": command,
                    "purpose": "inspect files",
                    "timeout_ms": 3000,
                },
                call_id=call_id,
                tool_class="focus_tool",
                risk="medium",
            )

        class MultiSignatureDuplicateExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append((step, list(messages)))
                if step == "final_reply":
                    return {"response": "multi signature duplicate guarded", "tool_call_envelopes": []}
                reaction_count = len([item for item in self.calls if item[0] == "reaction"])
                if reaction_count == 1:
                    return {"response": "", "tool_call_envelopes": [shell("dir /b sandbox\\round_25", "call_shell_a")]}
                if reaction_count == 2:
                    return {"response": "", "tool_call_envelopes": [shell("dir /b sandbox\\round_26", "call_shell_b")]}
                if reaction_count == 3:
                    return {"response": "", "tool_call_envelopes": [
                        shell("dir /b sandbox\\round_25", "call_shell_a_dup"),
                        shell("dir /b sandbox\\round_26", "call_shell_b_dup"),
                    ]}
                return {"response": "多签名重复已提醒，正常收束。", "tool_call_envelopes": []}

        executed = []

        def fake_execute(request):
            executed.append(dict(request))
            return {
                "tool_id": "shell_command",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "shell_command_handler",
                "permission_scope": "workspace_shell_allowlist",
                "result_kind": "general_tool_result",
                "cwd": request.get("cwd"),
                "command": request.get("command"),
                "stdout": "ok\n",
                "stderr": "",
                "exit_code": 0,
                "protocol_tool_receipt": False,
            }

        rt.executor = MultiSignatureDuplicateExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        reaction_texts = [
            "\n".join(m.get("content", "") for m in messages)
            for step, messages in rt.executor.calls
            if step == "reaction"
        ]

        assert [item["command"] for item in executed] == [
            "dir /b sandbox\\round_25",
            "dir /b sandbox\\round_26",
        ]
        assert "工具重复提醒" in reaction_texts[3]
        assert "本迭代共有 2 个不同工具签名重复" in reaction_texts[3]
        assert "主连续签名" not in reaction_texts[3]
        assert "批量复读提醒" in reaction_texts[3]
        assert "工具重复警告" not in "\n".join(reaction_texts)
        assert "closeout_only" not in "\n".join(reaction_texts)
        assert not any(
            item.get("status") == "general_tool_duplicate_closeout"
            for item in result["_reaction_loop_guard_receipts"]
        )

    def test_spec428_failed_duplicate_uses_failure_specific_popup(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        self._enable_legacy_shell_for_historical_pipeline_test(monkeypatch)

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class FailedDuplicateShellExecutor:
            def __init__(self):
                self.calls = []

            def call(self, step, system, messages):
                self.calls.append((step, list(messages)))
                if step == "final_reply":
                    return {"response": "failed duplicate guarded", "tool_call_envelopes": []}
                reaction_count = len([item for item in self.calls if item[0] == "reaction"])
                if reaction_count <= 2:
                    return {
                        "response": "",
                        "tool_call_envelopes": [helper._native_tool_envelope(
                            "shell_command",
                            {
                                "cwd": ".",
                                "command": "python missing_script.py",
                                "purpose": "run missing script",
                                "timeout_ms": 3000,
                            },
                            call_id=f"call_shell_fail_{reaction_count}",
                            tool_class="focus_tool",
                            risk="medium",
                        )],
                    }
                return {"response": "失败重复已提醒，正常收束。", "tool_call_envelopes": []}

        executed = []

        def fake_execute(request):
            executed.append(dict(request))
            return {
                "tool_id": "shell_command",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "failed",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "shell_command_handler",
                "permission_scope": "workspace_shell_allowlist",
                "result_kind": "general_tool_result",
                "cwd": request.get("cwd"),
                "command": request.get("command"),
                "stdout": "",
                "stderr": "module not found\n",
                "exit_code": 1,
                "protocol_tool_receipt": False,
            }

        rt.executor = FailedDuplicateShellExecutor()
        rt.general_tool_dispatcher = GeneralToolDispatcher(execute_fn=fake_execute)

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        reaction_texts = [
            "\n".join(m.get("content", "") for m in messages)
            for step, messages in rt.executor.calls
            if step == "reaction"
        ]

        assert len(executed) == 1
        rejected = [
            item for item in result["_general_tool_results"]
            if item.get("status") == "rejected"
        ]
        assert rejected[0]["reason"] == "duplicate_tool_failure_repeated"
        assert "duplicate_tool_failure_repeated" in reaction_texts[2]
        assert "这个参数组合上次失败或被拒绝" in reaction_texts[2]
        assert "请修正参数、换工具，或停止这条路径。" in reaction_texts[2]

    def test_spec073_subagent_dispatch_general_tool_uses_independent_result_channel(
            self, tmp_path, monkeypatch):
        from engines.general_tool_dispatcher import GeneralToolDispatcher

        rt = self._make_runtime(tmp_path)
        assembler = rt.assembler
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")
        helper = self

        class RecordingContext:
            def __init__(self):
                self.entries = []

            def append_to_cache(self, round_num, role, text, *, kind, **kwargs):
                self.entries.append((round_num, role, text, kind, kwargs))

            def transition_current_cache(self, **kwargs):
                return {
                    "schema_version": "current_cache_transition.v1",
                    "status": "noop",
                    "boundary": kwargs["boundary"],
                }

            def load_cache_compaction_debt(self):
                return {}

        def fake_execute(request):
            return {
                "tool_id": "subagent_dispatch",
                "tool_family": "general_tool",
                "tool_class": "focus_tool",
                "status": "ok",
                "source": "general_tool_call",
                "backend_type": "python",
                "handler": "subagent_dispatch_handler",
                "permission_scope": "subagent_task_scope",
                "result_kind": "general_tool_result",
                "task_goal": request.get("task_goal"),
                "backend_session_id": "agent-rt-001",
                "conclusion": "agent report",
                "modified_files": [],
                "test_evidence": ["pytest ok"],
                "risks": [],
                "unfinished": [],
                "protocol_tool_receipt": False,
            }

        rt.executor = ScriptedExecutor(
            {
                "response": "",
                "tool_call_envelopes": [helper._native_tool_envelope(
                    "subagent_dispatch",
                    {
                        "task_goal": "review docs",
                        "allowed_paths": ["UPSP"],
                        "expected_artifacts": "structured report",
                        "validation_commands": "python -m pytest OS/tests/test_logic.py -q",
                        "task_mode": "read_only",
                        "reason": "parallel review",
                    },
                    call_id="call_subagent",
                    tool_class="focus_tool",
                    risk="medium",
                )],
            },
            {"response": "subagent observed", "tool_call_envelopes": []},
        )
        rt.ctx_store = RecordingContext()
        rt.general_tool_dispatcher = GeneralToolDispatcher(
            load_guide_fn=lambda tool_id: (
                "subagent guide" if tool_id == "subagent_dispatch" else ""
            ),
            execute_fn=fake_execute,
        )

        result = rt._run_reaction_loop(rt.sm.load(), "interactive", [])
        second_call_text = "\n".join(
            m.get("content", "") for m in rt.executor.calls[1])

        assert "本轮已经执行子 agent 调度。" in second_call_text
        assert "agent report" in second_call_text
        assert [r["status"] for r in result["_general_tool_results"]] == ["ok"]
        assert result["_general_tool_results"][0]["call_id"] == "call_subagent"
        assert result["_protocol_tool_receipts"] == []
        assert all(entry[3] != "protocol_tool_receipt" for entry in rt.ctx_store.entries)
        assert any(entry[3] == "tool_fact" for entry in rt.ctx_store.entries)
