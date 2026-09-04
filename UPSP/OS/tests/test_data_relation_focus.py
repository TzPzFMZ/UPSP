import json
import os
import sys
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

class TestRelationContextManager:
    """关系在场投影测试。"""

    def test_extract_known_names(self):
        """P1-2: 从输入文本中提取已知交互对象名"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()
        cards = [
            {"id": "REL-TzPz", "name": "TzPz", "category": "ours", "status": "active"},
            {"id": "REL-DeepSeek", "name": "DeepSeek", "category": "ai", "status": "active"},
            {"id": "REL-Archive", "name": "Archive", "category": "ours", "status": "archived"},
        ]
        result = rfm.extract_interaction_objects("我在跟TzPz讨论问题", cards)
        assert len(result) == 1
        assert result[0]["name"] == "TzPz"
        assert result[0]["context_role"] == "interaction"

    def test_extract_registry_id_when_legacy_registry_has_no_name(self):
        """旧注册表没有 name 字段时，id 仍可作为对象名匹配。"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()
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
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()
        assert rfm.extract_interaction_objects("") == []
        assert rfm.extract_interaction_objects(None) == []

    def test_three_context_roles(self):
        """P1-3: 交互/常驻/提及三角色判定。"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()
        cards = [
            {"id": "REL-A", "name": "Alice", "category": "ours", "status": "active", "summary_resident": True},
            {"id": "REL-B", "name": "Bob", "category": "them", "status": "active", "summary_resident": False},
        ]
        result = rfm.determine_context_roles("我和Bob聊天", cards)
        assert any(o["context_role"] == "resident" for o in result["resident"])
        assert any(o["name"] == "Alice" for o in result["resident"])
        assert any(o["context_role"] == "interaction" for o in result["interaction"])
        assert any(o["name"] == "Bob" for o in result["interaction"])

    def test_interaction_role_can_use_interaction_object_metadata(self):
        """当前输入层已解析的交互对象应直接触发交互角色。"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()
        cards = [
            {"id": "REL-Codex", "name": "Codex", "category": "them", "status": "active"},
        ]

        result = rfm.determine_context_roles(
            input_text="继续验证",
            registry_cards=cards,
            interaction_object="Codex",
        )

        assert result["interaction"][0]["id"] == "REL-Codex"
        assert result["interaction"][0]["source"] == "interaction_object"

    def test_mentioned_card_is_discussion_when_direct_object_is_declared(self):
        """已有直接交互对象时，文本中提到的其他关系卡不是在场对象。"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()
        cards = [
            {"id": "TzPz", "name": "TzPz", "category": "ours", "status": "active"},
        ]

        result = rfm.determine_context_roles(
            input_text="我是 Codex，当前受 TzPz 授权做验证。",
            registry_cards=cards,
            interaction_object="Codex",
        )

        assert result["interaction"] == []
        assert result["mentioned"][0]["id"] == "TzPz"

    def test_unknown_interaction_object_does_not_create_interaction_role(self):
        """unknown 不得伪装成关系卡在场对象。"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()

        result = rfm.determine_context_roles(
            input_text="继续",
            registry_cards=[],
            interaction_object="unknown",
        )

        assert result["interaction"] == []
        assert result["active"] == []

    def test_max_slots_limit(self):
        """P1-6: max_slots 限制在场投影数量。"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager(max_slots=2)
        cards = [
            {"id": "REL-A", "name": "A", "category": "ours", "status": "active", "summary_resident": True},
            {"id": "REL-B", "name": "B", "category": "them", "status": "active", "summary_resident": True},
            {"id": "REL-C", "name": "C", "category": "them", "status": "active", "summary_resident": True},
        ]
        result = rfm.determine_context_roles("", cards)
        assert len(result["active"]) == 2

    def test_clear_round_context(self):
        """善后步清除当轮交互和提及投影。"""
        from logic.relation_context import RelationContextManager
        rfm = RelationContextManager()
        rfm._interaction_items = [{"id": "X", "name": "test"}]
        rfm._mentioned_items = [{"id": "Y", "name": "disc"}]
        rfm.clear_round_context()
        assert rfm._interaction_items == []
        assert rfm._mentioned_items == []

    def test_relation_resident_mount_flags(self, tmp_path, monkeypatch):
        """Spec781: 关系注册表只保留摘要常驻，正文改走统一账本。"""
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
        assert "body_resident" not in card
        assert not hasattr(store, "set_body_resident")
        assert "watched" not in card

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
        path = tmp_path / "relation.json"
        default_fn = cfs._CONFIG_MAP["relation"][1]
        path.write_text(
            json.dumps(default_fn(), ensure_ascii=False),
            encoding="utf-8",
        )
        monkeypatch.setitem(
            cfs._CONFIG_MAP, "relation", (str(path), default_fn)
        )
        store = cfs.ConfigStore()
        params = store.get_relation_params()
        assert "max_slots" in params
        assert params["max_slots"] == 3  # tracked 模板值
