"""
记忆热度管理 — heat.json 读写 + 衰减逻辑
DDS §8 STM 热度 + §9 LTM 降格

STM 三区：
  显著区：H ≥ 70，衰减速率 -5/轮
  未定区：40 ≤ H < 70，衰减速率 -10/轮
  衰减区：H < 40，衰减速率 -15/轮

升格条件：AH_high ≥ 5 → LTM Full
"""
import json
import os
from datetime import datetime

from data.atomic_write import atomic_write_json
from paths import HEAT_JSON
from schemas.memory import default_heat_entry, default_heat_json
from errors import ReadError
from constants import local_now
from data.config_store import ConfigStore
from data.stm_heat_calculator import STMHeatCalculator


class MemoryHeat:
    """热度值读写管理"""

    def __init__(self, heat_config=None):
        self.config = heat_config or ConfigStore().load("memory")["heat"]
        self.calculator = STMHeatCalculator(self.config)

    # ==============================================================
    # 读写
    # ==============================================================

    def load_heat(self):
        """读取 heat.json 全量"""
        if not os.path.isfile(HEAT_JSON):
            return default_heat_json()
        try:
            with open(HEAT_JSON, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(HEAT_JSON, cause=e)
        normalized, changed = self._normalize_heat_schema(data)
        if changed:
            self.save_heat(normalized)
        return normalized

    def save_heat(self, data):
        """写入 heat.json（原子）"""
        atomic_write_json(HEAT_JSON, data)

    def get_entry(self, mem_id):
        """获取单条热度"""
        heat = self.load_heat()
        entries = heat.get("entries", {})
        if mem_id not in entries:
            # 不存在就创建一个默认条目
            entries[mem_id] = self.new_entry()
            self.save_heat(heat)
        return entries[mem_id]

    def set_entry(self, mem_id, entry):
        """写入单条热度"""
        heat = self.load_heat()
        heat["entries"][mem_id] = entry
        self.save_heat(heat)

    def _normalize_heat_schema(self, data):
        """兼容旧 heat JSON pinned 字段，并应用 STM heat lock 不变量。"""
        changed = False
        entries = data.setdefault("entries", {})
        for info in entries.values():
            if "pinned" in info:
                info["heat_locked"] = bool(info.pop("pinned")) or bool(info.get("heat_locked"))
                changed = True
            if "heat_locked" not in info:
                info["heat_locked"] = False
                changed = True
            if info.get("heat_locked"):
                changed = self._apply_heat_lock(info) or changed
        return data, changed

    def _apply_heat_lock(self, info):
        changed = False
        fixed = {
            "H": self.config["locked_value"],
            "zone": "显著",
            "AH_low": 0,
            "degrade": False,
        }
        for key, value in fixed.items():
            if info.get(key) != value:
                info[key] = value
                changed = True
        return changed

    # ==============================================================
    # 热度衰减（每轮善后步后调用）
    # ==============================================================

    def tick_decay(self, round_num=None):
        """对所有 STM 条目执行一次热度衰减。返回是否有变化。

        round_num 存在时按轮幂等结算：同一轮重复调用不重复衰减；
        本轮刚创建或刚召回的条目不计作冷落。
        """
        heat = self.load_heat()
        if round_num is not None:
            normalized_round = self._normalize_round_num(round_num)
            if heat.get("_last_decay_round") == normalized_round:
                return False
            touched_this_round = self._memory_ids_touched_in_round(normalized_round)
        else:
            normalized_round = None
            touched_this_round = set()
        entries = heat.get("entries", {})
        changed = False

        decay_entries = {
            mem_id: dict(info)
            for mem_id, info in entries.items()
            if mem_id not in touched_this_round
        }
        updates = self.calculator.tick_decay(decay_entries)
        significant = self.config["zone_thresholds"]["significant"]
        for mem_id in touched_this_round:
            info = entries.get(mem_id)
            if (
                isinstance(info, dict)
                and info.get("H", 0) >= significant
            ):
                updates[mem_id] = {
                    "AH_high": info.get("AH_high", 0) + 1,
                }
        for mem_id, fields in updates.items():
            info = entries.get(mem_id)
            if not isinstance(info, dict):
                continue
            for key, value in fields.items():
                if info.get(key) != value:
                    info[key] = value
                    changed = True

        if normalized_round is not None:
            heat["_last_decay_round"] = normalized_round
            changed = True

        if changed:
            self.save_heat(heat)

        return changed

    @staticmethod
    def _normalize_round_num(round_num):
        try:
            return int(round_num)
        except (TypeError, ValueError):
            return round_num

    def _memory_ids_touched_in_round(self, round_num):
        touched = set()
        try:
            from data.memory_store import MemoryStore
            meta = MemoryStore().load_meta()
        except Exception:
            return touched

        for mem_id, entry in meta.items():
            if not isinstance(mem_id, str) or not mem_id.startswith("MEM-"):
                continue
            if not isinstance(entry, dict):
                continue
            created_round = self._normalize_round_num(entry.get("created_round"))
            recalled_round = self._normalize_round_num(entry.get("last_recalled_round"))
            if created_round == round_num or recalled_round == round_num:
                touched.add(mem_id)
        return touched

    # ==============================================================
    # 热度增加（被回忆时调用）
    # ==============================================================

    def recall_boost(self, mem_id, boost=None, round_num=None):
        """记忆被回忆时加热。不存在则自动创建。
        round_num 可选，传入则同步更新 meta.json 的 last_recalled_round。"""
        heat = self.load_heat()
        entries = heat.get("entries", {})
        now = local_now().isoformat()

        if mem_id not in entries:
            entries[mem_id] = self.new_entry()
            # 修正 compression 字段：auto-created条目可能权重被default_heat_entry误判
            try:
                from data.memory_store import MemoryStore
                weight = MemoryStore().load_meta().get(mem_id, {}).get("weight", 1)
                entries[mem_id]["compression"] = (weight >= 3)
            except Exception:
                pass

        info = entries[mem_id]
        new_h, ah_high, new_zone = self.calculator.recall_boost(
            info.get("H", 0),
            info.get("AH_high", 0),
            boost=boost,
        )
        info["H"] = new_h
        info["AH_high"] = ah_high
        info["zone"] = new_zone
        info["last_heat_at"] = now
        if new_zone == "显著":
            info["last_high_at"] = now

        # 被调用 = 打断冷落连续计数（AH_low 归零重新累积）
        info["AH_low"] = 0

        self.save_heat(heat)

        # 同步更新 meta.json 的召回时间（P0-2 修复）
        try:
            from data.memory_store import MemoryStore
            ms = MemoryStore()
            meta = ms.load_meta()
            if mem_id in meta:
                meta[mem_id]["last_recalled_at"] = now
                if round_num is not None:
                    meta[mem_id]["last_recalled_round"] = round_num
            ms.save_meta(meta)
        except Exception:
            pass

    # ==============================================================
    # 升格检查（STM → LTM Full）
    # ==============================================================

    def check_upgrade(self):
        """返回 AH_high ≥ 阈值 的条目列表"""
        heat = self.load_heat()
        entries = heat.get("entries", {})
        try:
            from data.memory_store import MemoryStore
            meta = MemoryStore().load_meta()
        except Exception:
            meta = {}
        return self.calculator.check_upgrade(entries, meta)

    def new_entry(self, weight=2):
        return default_heat_entry(
            weight=weight,
            initial_by_weight=self.config["initial_by_weight"],
            significant_threshold=self.config["zone_thresholds"]["significant"],
            uncertain_threshold=self.config["zone_thresholds"]["uncertain"],
        )

    def mark_stored(self, mem_id):
        """标记条目已存入 LTM"""
        heat = self.load_heat()
        if mem_id in heat.get("entries", {}):
            heat["entries"][mem_id]["stored"] = True
            self.save_heat(heat)

    # ==============================================================
    # STM heat lock（与 LTM Pinned 无关）
    # ==============================================================


    # ==============================================================
    # 遗忘分流查询
    # ==============================================================


    def remove_entry(self, mem_id):
        """从 heat.json 中删除条目"""
        heat = self.load_heat()
        if mem_id in heat.get("entries", {}):
            del heat["entries"][mem_id]
            self.save_heat(heat)

    def has_pending_degrade(self):
        """检查是否有需主动唤醒处理的未入库降格条目（心跳用）"""
        heat = self.load_heat()
        for info in heat.get("entries", {}).values():
            if info.get("degrade") and not info.get("stored"):
                return True
        return False
