import json
import os
import re
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

class TestStatusBar:
    @staticmethod
    def _state():
        return {
            "base": {
                "meta": {"total_round": 1},
                "heartbeat_flags": {},
                "workhood_index": {"value": 50},
                "dynamic_axes": {},
            }
        }

    def test_build_full(self):
        from assembly.statusbar import StatusBarBuilder
        sb = StatusBarBuilder()
        state = {
            "base": {
                "meta": {"total_round": 42, "daily_round": 5},
                "activity_mode": "理论",
                "fatigue": {"value": 25},
                "workhood_index": {"value": 55},
                "heartbeat_flags": {
                    "rhythm_due": True,
                    "user_message_waiting": False,
                    "identity_timeout": True,
                    "fatigue_expired": True,
                },
                "dynamic_axes": {
                    "valence": {"value": 30}, "arousal": {"value": 10},
                    "focus": {"value": -5}, "mood": {"value": 0},
                    "humor": {"value": 20}, "safety": {"value": 40},
                },
            }
        }
        result = sb.build_full(state, "interactive")
        assert "当前轮：R000042" in result
        assert "已稳定运行多轮" in result
        assert "疲劳" not in result
        assert "轻微疲劳" not in result
        assert "标准运作" in result
        assert "rhythm_due" in result
        assert "identity_timeout" not in result
        assert "fatigue_expired" not in result
        assert re.search(r"当前时间：\d{4}-\d{2}-\d{2} \d{2}:\d{2} UTC[+-]\d{2}:\d{2}", result)
        assert not re.search(r"\d{2}:\d{2}:\d{2}", result)

    def test_setup_uses_runtime_identity_without_parsing_input(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler(context_dir=str(tmp_path))
        anchor = {
            "interaction_object_id": "REL-GUEST",
            "interaction_object": "访客",
            "identity_status": "known",
            "interaction_source": "instance_selection",
        }
        monkeypatch.setattr(
            ContextAssembler,
            "_build_full_context",
            lambda self, **kwargs: ("system", []),
        )

        assembler.assemble_setup(
            self._state(),
            "interactive",
            user_messages=["请改写：\n我是 用户。"],
            interaction_meta=anchor,
        )

        assert assembler._current_interaction_meta == anchor

    def test_projection_and_markdown_share_interaction_source(self):
        from assembly.statusbar import StatusBarBuilder
        sb = StatusBarBuilder()
        projection = sb.build_projection({"base": {}}, "reaction")
        projection["interaction"].update({
            "relation_id": "REL-1",
            "display_name": "TzPz",
            "registration_status": "registered",
            "identity_source": "instance_selection",
            "summary": "继续推进",
        })

        rendered = sb.render(projection)

        assert projection["schema"] == "statusbar_snapshot.v1"
        assert "当前交互对象：TzPz" in rendered
        assert projection["interaction"]["relation_id"] == "REL-1"

    def test_spec739_response_anchor_is_a_statusbar_block(self):
        from assembly.statusbar import StatusBarBuilder

        builder = StatusBarBuilder()
        projection = builder.build_projection(
            self._state(), "reaction", response_anchor="使用英文；只回答结论。")
        rendered = builder.render(projection)

        assert projection["response_anchor"] == "使用英文；只回答结论。"
        assert "回答锚点：使用英文；只回答结论。" in rendered
        assert "回答锚点" not in builder.build_full(self._state(), "reaction")

    def test_current_relation_is_first_and_outside_focus_limit(
            self, tmp_path, monkeypatch):
        from assembly.context import ContextAssembler
        from data.relation_store import RelationStore

        cards = [
            {"id": "REL-OTHER", "name": "其他", "category": "them",
             "status": "active"},
            {"id": "REL-CURRENT", "name": "当前对象", "category": "ours",
             "status": "active"},
        ]
        monkeypatch.setattr(RelationStore, "load_registry", lambda self: {"cards": cards})
        monkeypatch.setattr(RelationStore, "get_card_path",
                            lambda self, cid, cat="ours": str(tmp_path / f"{cid}.md"))
        monkeypatch.setattr(RelationStore, "resolve_active_subject",
                            lambda self, value: "REL-CURRENT" if value in {
                                "REL-CURRENT", "当前对象"} else None)
        assembler = ContextAssembler(context_dir=str(tmp_path))
        assembler._current_interaction_meta = {
            "interaction_object_id": "REL-CURRENT",
            "interaction_object": "当前对象",
            "identity_status": "known",
            "interaction_source": "instance_selection",
        }
        monkeypatch.setattr(assembler, "_relation_focus_max_slots", lambda: 1)

        text = assembler._build_statusbar_with_relations(
            self._state(), "interactive", current_input_text="讨论其他")
        projection = assembler._last_statusbar_projection

        assert "当前交互对象：当前对象" in text
        assert [card["id"] for card in projection["relation_cards"]] == [
            "REL-CURRENT", "REL-OTHER"]

    def test_unregistered_reminder_is_reaction_only_and_deduplicated(self):
        from assembly.context import ContextAssembler

        assembler = ContextAssembler()
        meta = {
            "interaction_object": "张三",
            "identity_status": "unregistered",
            "interaction_source": "self_declaration",
        }
        reminder = assembler._build_relation_registration_popup("reaction", meta)
        combined = assembler.popup_policy.combine([reminder, reminder])

        assert combined.count("当前交互对象为陌生关系") == 1
        assert assembler._build_relation_registration_popup("setup", meta) == ""
        assert assembler._build_relation_registration_popup("reaction", {
            "interaction_object": "TzPz",
            "identity_status": "known",
            "interaction_source": "instance_selection",
        }) == ""

    def test_statusbar_audit_json_keeps_markdown_and_projection(self, tmp_path):
        from data.audit_store import AuditStore

        store = AuditStore(
            setup_dir=str(tmp_path / "setup"),
            reaction_dir=str(tmp_path / "reaction"),
            cleanup_dir=str(tmp_path / "cleanup"),
        )
        store.write_audit("reaction", {
            "statusbar": "status text",
            "statusbar_projection": {
                "schema": "statusbar_snapshot.v1",
                "interaction": {"relation_id": "REL-1"},
            },
        })

        payload = json.loads(
            (tmp_path / "reaction" / "layers" / "60_statusbar.json").read_text(
                encoding="utf-8"))
        assert payload["content"] == "status text"
        assert payload["projection"]["schema"] == "statusbar_snapshot.v1"
        assert payload["projection"]["interaction"]["relation_id"] == "REL-1"

    def test_spec458_statusbar_budget_limit_is_retired(self):
        from pathlib import Path

        root = Path(__file__).resolve().parents[3]
        config_path = (
            root
            / "UPSP"
            / "initialization"
            / "os_template"
            / "config"
            / "context"
            / "statusbar.json"
        )
        config = json.loads(config_path.read_text(encoding="utf-8"))

        assert "budget_chars" not in config
        assert config.get("trim_policy") == "none"

    def test_workhood_to_desc(self):
        from assembly.statusbar import StatusBarBuilder
        assert "高度自主" in StatusBarBuilder.workhood_to_desc(85)
        assert "标准运作" in StatusBarBuilder.workhood_to_desc(55)
        assert "休眠" in StatusBarBuilder.workhood_to_desc(5)

    def test_dynamic_axes_to_text(self):
        from assembly.statusbar import StatusBarBuilder
        sb = StatusBarBuilder()
        axes = {
            "valence": {"value": 30}, "arousal": {"value": -10},
            "focus": {"value": 50}, "mood": {"value": 0},
            "humor": {"value": 70}, "safety": {"value": -30},
        }
        text = sb.dynamic_axes_to_text(axes)
        assert "|" in text  # 六轴用 | 分隔
        assert len(text.split("|")) >= 6

    def test_dynamic_axes_to_text_all_neutral(self):
        from assembly.statusbar import StatusBarBuilder
        sb = StatusBarBuilder()
        axes = {k: {"value": 0} for k in
                ("valence", "arousal", "focus", "mood", "humor", "safety")}
        text = sb.dynamic_axes_to_text(axes)
        assert isinstance(text, str)
        assert len(text) > 0
