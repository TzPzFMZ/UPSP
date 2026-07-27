"""
Spec 026 / DDS v0.11.0 输入来源分层、工具台账与资料文件暂存测试。
"""
import json
import os
import sys

import pytest
from UPSP.OS.tests.runtime_test_helpers import ConfigStoreStub

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))


class TestInputLayerAssembly:
    def _state(self):
        return {
            "base": {
                "context_cache": {},
                "heartbeat_flags": {},
                "runtime": {"phase": "idle", "standby_countdown": 0},
            }
        }

    def test_three_source_entries_are_merged_into_now_layer(self, tmp_path, monkeypatch):
        from assembly import context as ctx

        monkeypatch.setattr(ctx, "CORE_MD", str(tmp_path / "core.md"))
        (tmp_path / "core.md").write_text("PID：FMZ\n位格编码：SCVARB", encoding="utf-8")

        assembler = ctx.ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        system, messages = assembler.assemble_setup(
            self._state(),
            "interactive",
            user_messages=["我是 TzPz，继续做输入分层验证。"],
            material_inputs=[{
                "role": "user",
                "kind": "runtime_retry_notice",
                "content": "资料输入哨兵：Runtime 短提醒。",
                "interaction_source": "runtime_retry_notice",
            }],
            internal_handoff=[{
                "role": "user",
                "content": "内部交接哨兵：起手步交给反应步。",
            }],
        )

        combined = "\n".join(m.get("content", "") for m in messages)
        assert system == ""
        assert "<!-- 当前缓存 now -->" not in combined
        assert "<!-- 交互输入层 -->" not in combined
        assert "<!-- 资料输入层 -->" not in combined
        assert "<!-- 内部交接层 -->" not in combined
        assert any("资料输入哨兵" in m.get("content", "") for m in messages)
        assert not any("内部交接哨兵" in m.get("content", "") for m in messages)

        contents = [m.get("content", "") for m in messages]
        assert not any("<!-- 当前缓存 now -->" in c for c in contents)
        assert not any("<!-- 交互输入层 -->" in c for c in contents)
        assert not any("<!-- 资料输入层 -->" in c for c in contents)
        assert not any("<!-- 内部交接层 -->" in c for c in contents)
        assert not any("<!-- 当前输入层 -->" in c for c in contents)

        manifest = json.loads(
            (tmp_path / "context" / "setup" / "manifest.json").read_text(encoding="utf-8")
        )
        assert manifest["layers"]["now"]["chars"] > 0
        assert "interaction_input" not in manifest["layers"]
        assert "material_input" not in manifest["layers"]
        assert "internal_handoff" not in manifest["layers"]

    def test_spec406_material_inputs_reject_unbounded_material(self, tmp_path, monkeypatch):
        from assembly import context as ctx

        monkeypatch.setattr(ctx, "CORE_MD", str(tmp_path / "core.md"))
        (tmp_path / "core.md").write_text("PID：FMZ\n位格编码：SCVARB", encoding="utf-8")

        assembler = ctx.ContextAssembler(context_dir=str(tmp_path / "context"))
        monkeypatch.setattr(assembler, "_cached_or_build", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_build_high_freq", lambda *args, **kwargs: "")
        monkeypatch.setattr(assembler, "_get_lately_entries", lambda *args, **kwargs: [])
        monkeypatch.setattr(assembler.popup, "read_popup", lambda: "")

        _, messages = assembler.assemble_reaction(
            self._state(),
            "interactive",
            material_inputs=[{
                "role": "system",
                "kind": "material",
                "content": "UNBOUNDED_RUNTIME_MATERIAL" * 200,
                "interaction_source": "arbitrary_material_inputs",
            }],
        )

        combined = "\n".join(m.get("content", "") for m in messages)
        assert "UNBOUNDED_RUNTIME_MATERIAL" not in combined


class TestToolFeedbackCorpusBlocks:
    def test_spec407_historical_proof_blocks_promote_to_lately_and_raw_log(self, tmp_path, monkeypatch):
        from data import context_store as ctxs

        monkeypatch.setattr(ctxs, "CONTAINER_CORPUS_DIR", str(tmp_path / "corpus"))
        monkeypatch.setattr(ctxs, "STM_CONTEXT_CACHE_DIR", str(tmp_path / "cache"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_NOW_CACHE_JSONL", str(tmp_path / "cache" / "now_cache.jsonl"), raising=False)
        monkeypatch.setattr(ctxs, "STM_CONTEXT_LATELY_CACHE_JSONL", str(tmp_path / "cache" / "lately_cache.jsonl"), raising=False)

        class CacheConfig(ConfigStoreStub):
            def get_now_cache_params(self):
                return {"budget_chars": 80, "trim_chars": 40}

            def get_lately_cache_params(self):
                return {"budget_chars": 1024, "trim_chars": 128}

            def get_now_policy_by_kind(self):
                return {}

            def get_lately_allowed_kinds(self):
                return [
                    "interaction",
                    "assistant_reply",
                    "tool_fact",
                    "setup_fact",
                    "relay_handoff",
                    "minimum_commitment",
                    "fault_note",
                    "cache_summary",
                ]

        store = ctxs.ContextStore(
            config_store=CacheConfig(),
            raw_log_jsonl=str(tmp_path / "buffer" / "raw_log.jsonl"),
            raw_log_md=str(tmp_path / "buffer" / "raw_log.md"),
            corpus_rhythms_dir=str(tmp_path / "corpus" / "public" / "rhythms"),
        )
        store.append_to_cache(18, "user", "请继续读书。", kind="interaction")
        store.append_to_cache(18, "system", "起手安全裁决通过。", kind="setup_fact")
        store.append_to_cache(18, "tool", "已读取文件：book.md。", kind="tool_fact")
        store.append_to_cache(18, "user", "继续从第 164 行读取。", kind="relay_handoff")
        store.append_to_cache(18, "system", "原始资料正文" * 20, kind="material")

        now_blocks = [
            json.loads(line)
            for line in (tmp_path / "cache" / "now_cache.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        lately_blocks = [
            json.loads(line)
            for line in (tmp_path / "cache" / "lately_cache.jsonl").read_text(encoding="utf-8").splitlines()
            if line.strip()
        ]
        raw_blocks = [
            json.loads(line)
            for line in (tmp_path / "buffer" / "raw_log.jsonl")
            .read_text(encoding="utf-8")
            .splitlines()
            if line.strip()
        ]

        now_kinds = {block["kind"] for block in now_blocks}
        lately_kinds = {block["kind"] for block in lately_blocks}
        raw_kinds = {block["kind"] for block in raw_blocks}

        assert "material" in lately_kinds
        assert "material" not in raw_kinds
        assert {"interaction", "setup_fact", "tool_fact", "relay_handoff"} <= lately_kinds
        assert {"interaction", "setup_fact", "tool_fact", "relay_handoff"} <= raw_kinds
        assert not (tmp_path / "corpus" / "public" / "rhythms").exists()

        assert not (tmp_path / "tool_call_ledger.jsonl").exists()


class TestFilesStore:
    def test_os_files_layout_has_no_global_index(self, tmp_path):
        from data.files_store import FilesStore

        store = FilesStore(os_root=str(tmp_path / "OS"))
        layout = store.ensure_layout()
        for key in ("raw", "media_raw", "clips", "archive"):
            assert os.path.isdir(layout[key])

        files_root = tmp_path / "OS" / "files"
        assert not (files_root / "files_index.json").exists()
        assert not (files_root / "_index.json").exists()

        raw_path = store.save_raw_text("原始资料", "note.md")
        media_path = store.save_raw_bytes(b"image", "image.png", media=True)
        clip_path = store.save_clip_text("剪贴资料", "clip.md")
        archive_path = store.save_archive_text("留档资料", "archive.md")

        assert os.path.isfile(raw_path)
        assert os.path.isfile(media_path)
        assert os.path.isfile(clip_path)
        assert os.path.isfile(archive_path)
