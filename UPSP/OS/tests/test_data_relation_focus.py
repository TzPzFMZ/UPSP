import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

class TestRelationFocusManager:
    """关系焦点管理器测试"""

    def test_extract_known_names(self):
        """P1-2: 从输入文本中提取已知交互对象名"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()
        cards = [
            {"id": "REL-TzPz", "name": "TzPz", "category": "ours", "status": "active"},
            {"id": "REL-DeepSeek", "name": "DeepSeek", "category": "ai", "status": "active"},
            {"id": "REL-Archive", "name": "Archive", "category": "ours", "status": "archived"},
        ]
        result = rfm.extract_interaction_objects("我在跟TzPz讨论问题", cards)
        assert len(result) == 1
        assert result[0]["name"] == "TzPz"
        assert result[0]["match_type"] == "present"

    def test_extract_registry_id_when_legacy_registry_has_no_name(self):
        """旧注册表没有 name 字段时，id 仍可作为对象名匹配。"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()
        cards = [
            {"id": "TzPz", "category": "ours"},
            {"id": "FMA", "category": "them"},
        ]
        result = rfm.extract_interaction_objects("TzPz 发来了一条交互输入", cards)
        assert len(result) == 1
        assert result[0]["id"] == "TzPz"
        assert result[0]["name"] == "TzPz"

    def test_extract_empty_input(self):
        """空输入返回空列表"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()
        assert rfm.extract_interaction_objects("") == []
        assert rfm.extract_interaction_objects(None) == []

    def test_three_states(self):
        """P1-3: 在场/回想/议论三态判定"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()
        cards = [
            {"id": "REL-A", "name": "Alice", "category": "ours", "status": "active", "summary_resident": True},
            {"id": "REL-B", "name": "Bob", "category": "them", "status": "active", "summary_resident": False},
        ]
        result = rfm.determine_focus_states("我和Bob聊天", cards)
        # Alice 是 recall（summary_resident=true），Bob 是 present（名字匹配）
        assert any(o["match_type"] == "recall" for o in result["recall"])
        assert any(o["name"] == "Alice" for o in result["recall"])
        assert any(o["match_type"] == "present" for o in result["present"])
        assert any(o["name"] == "Bob" for o in result["present"])

    def test_present_focus_can_use_interaction_object_metadata(self):
        """当前输入层已解析的交互对象应直接触发在场焦点。"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()
        cards = [
            {"id": "REL-Codex", "name": "Codex", "category": "them", "status": "active"},
        ]

        result = rfm.determine_focus_states(
            input_text="继续验证",
            registry_cards=cards,
            interaction_object="Codex",
        )

        assert result["present"][0]["id"] == "REL-Codex"
        assert result["present"][0]["source"] == "interaction_object"

    def test_mentioned_card_is_discussion_when_direct_object_is_declared(self):
        """已有直接交互对象时，文本中提到的其他关系卡不是在场对象。"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()
        cards = [
            {"id": "TzPz", "name": "TzPz", "category": "ours", "status": "active"},
        ]

        result = rfm.determine_focus_states(
            input_text="我是 Codex，当前受 TzPz 授权做验证。",
            registry_cards=cards,
            interaction_object="Codex",
        )

        assert result["present"] == []
        assert result["discussion"][0]["id"] == "TzPz"

    def test_unknown_interaction_object_does_not_create_present_focus(self):
        """unknown 不得伪装成关系卡焦点。"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()

        result = rfm.determine_focus_states(
            input_text="继续",
            registry_cards=[],
            interaction_object="unknown",
        )

        assert result["present"] == []
        assert result["active"] == []

    def test_max_slots_limit(self):
        """P1-6: max_slots 限制焦点数量"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager(max_slots=2)
        cards = [
            {"id": "REL-A", "name": "A", "category": "ours", "status": "active", "summary_resident": True},
            {"id": "REL-B", "name": "B", "category": "them", "status": "active", "summary_resident": True},
            {"id": "REL-C", "name": "C", "category": "them", "status": "active", "summary_resident": True},
        ]
        result = rfm.determine_focus_states("", cards)
        # 3个recall但max_slots=2，active只取前2
        assert len(result["active"]) == 2

    def test_clear_round_focus(self):
        """善后步清除当轮临时焦点"""
        from logic.relation_focus import RelationFocusManager
        rfm = RelationFocusManager()
        rfm._present_focus = [{"id": "X", "name": "test"}]
        rfm._discussion_focus = [{"id": "Y", "name": "disc"}]
        rfm.clear_round_focus()
        assert rfm._present_focus == []
        assert rfm._discussion_focus == []

    def test_relation_resident_mount_flags(self, tmp_path, monkeypatch):
        """Spec 089: summary/body resident 标记由 relation_read 使用。"""
        from data import relation_store as rs
        rel_dir = tmp_path / "relation_resident"
        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "reg_w.json"))
        monkeypatch.setattr(rs, "RELATION_DIR", str(rel_dir))
        monkeypatch.setattr(rs.RelationStore, "get_card_path",
                           lambda self, cid, cat="ours": str(rel_dir / cat / f"{cid}.md"))

        store = rs.RelationStore()
        store.create_card("REL-W", "WatchTest")
        reg = store.set_summary_resident("REL-W", True)
        card = [c for c in reg["cards"] if c["id"] == "REL-W"][0]
        assert card["summary_resident"] is True
        assert card["body_resident"] is False

        reg2 = store.set_body_resident("REL-W", True)
        card2 = [c for c in reg2["cards"] if c["id"] == "REL-W"][0]
        assert card2["summary_resident"] is True
        assert card2["body_resident"] is True
        assert "watched" not in card2

    def test_save_relation_registry_rejects_retired_watched_field(self, tmp_path, monkeypatch):
        from data import relation_store as rs

        monkeypatch.setattr(rs, "RELATION_REGISTRY_JSON", str(tmp_path / "reg_legacy.json"))
        old_registry = {
            "_comment": "关系卡注册表",
            "cards": [{
                "id": "REL-OLD",
                "name": "Old",
                "category": "ours",
                "path": "relation/ours/REL-OLD.md",
                "status": "active",
                "watched": True,
            }],
        }

        store = rs.RelationStore()
        with pytest.raises(rs.WriteError):
            store.save_registry(old_registry)

    def test_get_relation_params(self, tmp_path, monkeypatch):
        """P1-6: get_relation_params 读取 max_slots"""
        from data import config_store as cfs
        # 只替换relation配置路径
        monkeypatch.setitem(cfs._CONFIG_MAP, "relation",
                           (str(tmp_path / "relation.json"), cfs._CONFIG_MAP["relation"][1]))
        store = cfs.ConfigStore()
        params = store.get_relation_params()
        assert "max_slots" in params
        assert params["max_slots"] == 3  # 默认值
