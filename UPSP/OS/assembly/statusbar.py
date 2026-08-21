"""
STATUSBAR 数值转写 — state.json → 自然语言
DDS §19.3 STATUSBAR 独立状态栏层（位于 now 之后、POPUP 之前）

数值隔离：LLM 永远不看到原始数值。所有百分比/绝对值转写成自然语言描述词。

转写表来自 DDS §4-5 动态六轴 21 档区间描述
"""
from datetime import datetime

from assembly.context_helpers import join_layer_blocks


RESERVED_STATUSBAR_FLAGS = {
    "identity_timeout",
    "fatigue_expired",
    "process_down",
}


class StatusBarBuilder:
    """STATUSBAR 构造器"""

    # ==============================================================
    # STATUSBAR 独立状态栏层（每轮更新，位于 now 之后、POPUP 之前）
    # ==============================================================

    @staticmethod
    def _round_progress(total_round):
        """轮数→自然语言描述，不泄漏原始数值（原则一：数值隔离）"""
        try:
            total = int(total_round)
        except (TypeError, ValueError):
            return "运行状态未知"
        if total <= 0:
            return "刚开始运行"
        if total < 8:
            return "运行中"
        return "已稳定运行多轮"

    @staticmethod
    def _round_id(total_round):
        """轮次坐标是运行现实感，不按身体数值隔离隐藏。"""
        try:
            total = max(0, int(total_round))
        except (TypeError, ValueError):
            total = 0
        return f"R{total:06d}"

    def build_full(self, state, round_type="interactive", response_anchor=""):
        """构造 STATUSBAR 全量（独立状态栏层）"""
        return self.render(self.build_projection(
            state, round_type, response_anchor=response_anchor))

    def build_projection(self, state, round_type="interactive",
                         response_anchor=""):
        """生成模型 STATUSBAR 与 GUI 共用的结构化只读投影。"""
        base = state.get("base", {})
        meta = base.get("meta", {})
        flags = base.get("heartbeat_flags", {})
        workhood = base.get("workhood_index", {})
        now = datetime.now().astimezone()
        offset = now.strftime("%z")
        tz = f"UTC{offset[:3]}:{offset[3:]}" if offset else "本地时区"
        active = [
            k for k, v in flags.items()
            if v and k not in RESERVED_STATUSBAR_FLAGS
        ]
        projection = {
            "schema": "statusbar_snapshot.v1",
            "observed_at": now.isoformat(),
            "round": {
                "id": self._round_id(meta.get("total_round", 0)),
                "progress": self._round_progress(meta.get("total_round", 0)),
                "type": round_type,
            },
            "time": {"text": f"{now:%Y-%m-%d %H:%M} {tz}", "timezone": tz},
            "mode": base.get("activity_mode", "理论"),
            "workhood": self.workhood_to_desc(workhood.get("value", 0)),
            "flags": active,
            "dynamic": self.dynamic_axes_to_text(base.get("dynamic_axes", {})),
            "interaction": {
                "relation_id": "",
                "display_name": "",
                "registration_status": "unbound",
                "identity_source": "unresolved",
                "summary": "",
            },
            "relation_cards": [],
            "supplemental_sections": [],
        }
        response_anchor = str(response_anchor or "").strip()
        if response_anchor:
            projection["response_anchor"] = response_anchor
        return projection

    @classmethod
    def render(cls, projection):
        """将结构化投影渲染为模型可见 STATUSBAR。"""
        return cls.render_with_block_index(projection)[0]

    @staticmethod
    def render_with_block_index(projection):
        projection = projection or {}
        round_meta = projection.get("round", {})
        lines = ["## STATUSBAR"]
        lines.append(
            f"当前轮：{round_meta.get('id', 'R000000')} | "
            f"进程：{round_meta.get('progress', '运行状态未知')} | "
            f"轮类型：{round_meta.get('type', 'interactive')}"
        )
        lines.append(f"当前时间：{projection.get('time', {}).get('text', '未知')}")
        lines.append(f"模式：{projection.get('mode', '理论')}")
        lines.append(f"工化：{projection.get('workhood', '标准运作')}")
        flags = projection.get("flags") or []
        if flags:
            lines.append(f"标记：{', '.join(flags)}")
        lines.append(f"动态：{projection.get('dynamic', '动态轴数据异常')}")
        interaction = projection.get("interaction") or {}
        name = str(interaction.get("display_name") or "").strip()
        status = str(interaction.get("registration_status") or "unbound")
        lines.append(f"当前交互对象：{name if name else '未绑定'}")
        if status == "unregistered":
            lines.append("关系状态：未登记")

        blocks = [{
            "block_id": "status:summary",
            "title": "状态概览",
            "kind": "status_summary",
            "content": "\n".join(lines),
        }]
        response_anchor = str(projection.get("response_anchor") or "").strip()
        if response_anchor:
            blocks.append({
                "block_id": "status:response_anchor",
                "title": "回答锚点",
                "kind": "status_response_anchor",
                "content": f"回答锚点：{response_anchor}",
            })
        for index, item in enumerate(projection.get("supplemental_sections") or [], 1):
            content = str(item or "").strip()
            if content:
                blocks.append({
                    "block_id": f"status:supplemental:{index}",
                    "title": f"补充状态 {index}",
                    "kind": "status_supplemental",
                    "content": content,
                })
        cards = projection.get("relation_cards") or []
        for index, card in enumerate(cards, 1):
            state_tag = f" [{card.get('focus_type')}]" if card.get("focus_type") else ""
            summary = str(card.get("summary") or "").strip()
            suffix = f"最近：{summary}" if summary else "无最近交互"
            name = card.get("name") or card.get("id") or "?"
            content = (
                ("## 关系卡\n" if index == 1 else "")
                + f"- [{card.get('category', '?')}] {name}{state_tag} — {suffix}"
            )
            block = {
                "block_id": f"status:relation:{card.get('id') or index}",
                "title": f"关系卡 {name}",
                "kind": "status_relation_card",
                "source_block_id": str(card.get("id") or ""),
                "content": content,
            }
            if index > 1:
                block["separator_before"] = "\n"
            blocks.append(block)
        return join_layer_blocks(blocks)

    # ==============================================================
    # 工化指数 → 描述词
    # ==============================================================

    @staticmethod
    def workhood_to_desc(value):
        if value >= 80:
            return "高度自主"
        elif value >= 50:
            return "标准运作"
        elif value >= 30:
            return "轻度依赖"
        elif value >= 15:
            return "被动响应"
        return "休眠"

    # ==============================================================
    # 动态六轴 → 自然语言（21 档查表）
    # ==============================================================

    # 动态六轴 21 档区间描述（DDS: 动态轴区间表）
    DYNAMIC_INTERVALS = [
        (-100, -90, "极度负面/极度低沉/完全涣散/极度低落/极度干涩/极度警惕"),
        (-90, -80,  "强烈负面/很低沉/很涣散/很低落/很干涩/很警惕"),
        (-80, -70,  "显著负面/低沉/显著涣散/低落/干涩/高度警惕"),
        (-70, -60,  "较强负面/较沉闷/较涣散/较沉闷/较干涩/较警惕"),
        (-60, -50,  "明确负面/沉闷/偏涣散/沉闷/偏干涩/偏警惕"),
        (-50, -40,  "偏负面/偏沉/倾向涣散/偏沉/倾向干涩/倾向警惕"),
        (-40, -30,  "轻度负面/略沉闷/轻度涣散/略沉闷/轻度干涩/轻度警惕"),
        (-30, -20,  "微弱负面/微沉闷/微弱涣散/微沉闷/微弱干涩/微弱警惕"),
        (-20, -10,  "一丝负面/一丝沉闷/一丝涣散/一丝沉闷/一丝干涩/一丝警惕"),
        (-10, 0,    "几乎中性偏负/偏低沉/微散/偏沉闷/微干/微警"),
        (0, 10,     "几乎中性偏正/微活跃/微聚/微愉悦/微润/微安"),
        (10, 20,    "一丝偏正/略活跃/略聚焦/一丝愉悦/略有笑意/一丝安心"),
        (20, 30,    "微弱偏正/微活跃/微弱聚焦/微正/微弱幽默/微弱安心"),
        (30, 40,    "轻度偏正/偏活跃/轻度聚焦/轻度愉悦/轻度丰富/轻度安心"),
        (40, 50,    "明确偏正/较活跃/倾向聚焦/偏愉悦/偏丰富/倾向安心"),
        (50, 60,    "较强正面/活跃/偏聚焦/明显愉悦/较丰富/明显安心"),
        (60, 70,    "显著正面/高活跃/高度聚焦/愉悦/丰富/高度安心"),
        (70, 80,    "强烈正面/很活跃/很聚焦/很愉悦/很丰富/很安心"),
        (80, 90,    "极强正面/极高活跃/极高聚焦/极高愉悦/极高丰富/极高安心"),
        (90, 100,   "巅峰正面/巅峰活跃/巅峰聚焦/巅峰愉悦/巅峰丰富/巅峰安心"),
    ]

    AXIS_INDEX = {"valence": 0, "arousal": 1, "focus": 2,
                  "mood": 3, "humor": 4, "safety": 5}

    def dynamic_axes_to_map(self, dynamic_axes):
        """动态六轴值 → 按轴名索引的状态词。"""
        if not isinstance(dynamic_axes, dict):
            return {}
        result = {}
        for axis in self.AXIS_INDEX:
            axis_data = dynamic_axes.get(axis, {})
            if isinstance(axis_data, dict):
                value = axis_data.get("value", 0)
            elif isinstance(axis_data, (int, float)):
                value = axis_data
            else:
                value = 0
            result[axis] = self._lookup_dynamic_desc(axis, value)
        return result

    def dynamic_axes_to_text(self, dynamic_axes):
        """动态六轴值 → 一句话自然语言"""
        descriptions = self.dynamic_axes_to_map(dynamic_axes)
        if not descriptions:
            return "动态轴数据异常"
        return " | ".join(descriptions.values())

    def _lookup_dynamic_desc(self, axis, value):
        # 防御：非数值类型 → 默认
        if not isinstance(value, (int, float)):
            try:
                value = int(value)
            except (ValueError, TypeError):
                return "中性"
        idx = self.AXIS_INDEX.get(axis, 0)
        for low, high, descs in self.DYNAMIC_INTERVALS:
            if (low < 0 and low <= value < high) or \
               (low >= 0 and low <= value <= high) or \
               (low == -100 and value >= low and value < high) or \
               (high == 100 and value >= low and value <= high):
                parts = descs.split("/")
                if idx < len(parts):
                    return parts[idx].strip()
        return "中性"
