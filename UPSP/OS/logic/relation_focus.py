"""
关系焦点管理 — 在场/常驻摘要/议论三态判定 + 交互对象提取
DDS §9 关系焦点

三态定义：
  在场(present): 输入语料中直接出现的交互对象 → 当轮临时，善后步自动收
  回想(recall):  关系卡 summary_resident=true → 跨轮常驻摘要
  议论(discussion): 输入语料中提及的第三方（有卡但不在场）→ 当轮临时

关系焦点名匹配(P1-2):
  从输入语料中匹配关系卡注册表中的已知名称+别名
  匹配策略：精确匹配卡名 → 标签匹配 → 别名匹配

建卡边界:
  关系卡只能由反应步显式间接声明并经脚本校验后创建；
  语料层只匹配焦点，不解析身份、不凭元数据自动建卡。

max_slots(P1-6):
  从 config/relation.json 读取 relation_focus.max_slots，默认 3
"""
from schemas.relation import relation_card_label, relation_public_name

class RelationFocusManager:
    """关系焦点管理器"""

    def __init__(self, max_slots=3):
        try:
            self._max_slots = max(1, int(max_slots or 3))
        except (TypeError, ValueError):
            self._max_slots = 3
        self._present_focus = []  # 当轮在场焦点（临时）
        self._discussion_focus = []  # 当轮议论焦点（临时）

    # ==============================================================
    # P1-6: max_slots 配置读取
    # ==============================================================

    def get_max_slots(self):
        """返回调用方显式传入的 relation_focus.max_slots。"""
        return self._max_slots

    # ==============================================================
    # P1-2: 交互对象名提取
    # ==============================================================

    def extract_interaction_objects(self, input_text, registry_cards=None):
        """从输入语料中提取交互对象名。

        策略：
        1. 精确匹配：注册表中的卡名出现在输入中
        2. 标签匹配：输入文本包含关系卡标签
        3. 返回匹配到的关系卡信息列表

        Args:
            input_text: 用户输入文本
            registry_cards: 注册表中的卡列表（None时自动加载）

        Returns:
            list[dict]: 匹配到的关系卡，每项含 id/name/category/match_type
        """
        if not input_text or not input_text.strip():
            return []

        if registry_cards is None:
            registry_cards = []

        if not registry_cards:
            return []

        matched = []
        text_lower = input_text.lower()

        for card in registry_cards:
            if card.get("status") == "archived":
                continue

            name = card.get("name") or card.get("id", "")
            card_id = card.get("id", "")
            category = card.get("category", "ours")

            # 策略1: 精确名称匹配
            if name and name in input_text:
                matched.append({
                    "id": card_id,
                    "name": name,
                    "category": category,
                    "match_type": "present",
                    "source": "name_exact",
                })
                continue

            # 策略2: 名称小写匹配（容错大小写）
            if name and name.lower() in text_lower:
                matched.append({
                    "id": card_id,
                    "name": name,
                    "category": category,
                    "match_type": "present",
                    "source": "name_case_insensitive",
                })
                continue

            # 策略3: 标签/别名匹配 → 议论（非直接交互）
            tags = list(card.get("tags", []) or []) + list(card.get("aliases", []) or [])
            if tags:
                for tag in tags:
                    if tag and tag in input_text:
                        matched.append({
                            "id": card_id,
                            "name": name,
                            "category": category,
                            "match_type": "discussion",
                            "source": f"tag:{tag}",
                        })
                        break

        return matched

    # ==============================================================
    # P1-3: 在场/回想/议论三态判定
    # ==============================================================

    def determine_focus_states(self, input_text=None, registry_cards=None,
                               interaction_object=None):
        """三态判定：在场 + 回想 + 议论 → 合并后的焦点列表。

        Args:
            input_text: 当轮输入语料（可为None，如自主轮）
            registry_cards: 注册表卡列表（None时自动加载）

        Returns:
            dict: {
                "present": [...],    # 在场焦点（当轮临时）
                "recall": [...],     # 回想焦点（跨轮常驻，summary_resident=true）
                "discussion": [...], # 议论焦点（当轮临时）
                "active": [...],     # 合并后激活焦点（去重+max_slots截断）
            }
        """
        if registry_cards is None:
            registry_cards = []

        present = []
        recall = []
        discussion = []

        # 语料层已解析的交互对象优先触发“在场”；unknown 不触发。
        direct_object = relation_public_name(interaction_object)
        has_direct_object = direct_object and direct_object not in ("unknown", "无", "—", "-")
        if direct_object and direct_object not in ("unknown", "无", "—", "-"):
            card = self._find_card_for_object(direct_object, registry_cards)
            if card:
                card_id = card.get("id", "")
                name = relation_card_label(card) or card_id
                present.append({
                    "id": card_id,
                    "name": name,
                    "category": card.get("category", "ours"),
                    "match_type": "present",
                    "source": "interaction_object",
                })

        # 在场 + 议论：从输入语料提取
        if input_text:
            extracted = self.extract_interaction_objects(input_text, registry_cards)
            for obj in extracted:
                present_ids = {p.get("id") for p in present}
                discussion_ids = {d.get("id") for d in discussion}
                if (obj["match_type"] == "present" and
                        obj.get("id") not in present_ids and
                        not has_direct_object):
                    present.append(obj)
                elif (obj["match_type"] == "present" and
                      obj.get("id") not in present_ids and
                      obj.get("id") not in discussion_ids and
                      has_direct_object):
                    discussion.append({
                        **obj,
                        "match_type": "discussion",
                        "source": "mentioned_with_direct_object",
                    })
                elif obj["match_type"] == "discussion":
                    discussion.append(obj)

        # 回想：扫描 summary_resident=true 的关系卡
        for card in registry_cards:
            if card.get("status") == "archived":
                continue
            if card.get("summary_resident"):
                card_id = card.get("id", "")
                name = relation_card_label(card) or card_id
                recall.append({
                    "id": card_id,
                    "name": name,
                    "category": card.get("category", "ours"),
                    "match_type": "recall",
                    "source": "summary_resident=true",
                })

        # 合并去重（优先级: recall > present > discussion）
        max_slots = self.get_max_slots()
        seen_ids = set()
        active = []

        # 回想优先（跨轮常驻，最高优先级）
        for obj in recall:
            if obj["id"] not in seen_ids:
                seen_ids.add(obj["id"])
                active.append(obj)

        # 在场次之
        for obj in present:
            if obj["id"] not in seen_ids:
                seen_ids.add(obj["id"])
                active.append(obj)

        # 议论最低
        for obj in discussion:
            if obj["id"] not in seen_ids:
                seen_ids.add(obj["id"])
                active.append(obj)

        # max_slots 截断
        active = active[:max_slots]

        # 缓存当轮状态
        self._present_focus = present
        self._discussion_focus = discussion

        return {
            "present": present,
            "recall": recall,
            "discussion": discussion,
            "active": active,
        }

    @staticmethod
    def _find_card_for_object(name_or_id, registry_cards):
        needle = str(name_or_id or "").strip()
        if not needle:
            return None
        for card in registry_cards or []:
            if card.get("status") == "archived":
                continue
            card_id = card.get("id", "")
            card_name = card.get("name") or card_id
            candidates = {card_id, card_name}
            if card_name:
                candidates.add(f"REL-{card_name}")
            if needle in candidates:
                return card
        return None

    def clear_round_focus(self):
        """善后步调用：清除当轮临时焦点（在场+议论）。
        常驻摘要不受影响，由 relation_read 的 resident 请求切换。"""
        self._present_focus = []
        self._discussion_focus = []
