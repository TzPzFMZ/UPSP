"""
记忆热度管理 — heat.json 读写 + 衰减逻辑
DDS §8 STM 热度 + §9 LTM 降格

STM 三区：
  显著区：H ≥ 70，衰减速率 -5/轮
  未定区：40 ≤ H < 70，衰减速率 -10/轮
  衰减区：H < 40，衰减速率 -15/轮

升格条件：AH_high ≥ 5 → 填写 LTM meta.stored_at
"""
import json
import os

from data.atomic_write import atomic_write_json
from paths import HEAT_JSON
from schemas.memory import default_heat_entry, default_heat_json
from errors import ReadError
from data.config_store import ConfigStore
from data.memory_store import MemoryStore
from data.stm_heat_calculator import STMHeatCalculator


class MemoryHeat:
    """热度值读写管理"""

    def __init__(self, heat_config=None, *, memory_store=None):
        self.config = heat_config or ConfigStore().load("memory")["heat"]
        self.calculator = STMHeatCalculator(self.config)
        self.memory_store = memory_store if memory_store is not None else MemoryStore()

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
    # 热度衰减（合法 Reaction 语义终态调用）
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
        meta = self.memory_store.load_meta()

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
    # 正式入库检查
    # ==============================================================

    def check_upgrade(self):
        """返回 AH_high 达标且 LTM 尚未正式入库的条目列表。"""
        heat = self.load_heat()
        entries = heat.get("entries", {})
        meta = self.memory_store.active_ltm_meta_by_id()
        return self.calculator.check_upgrade(entries, meta)

    def new_entry(self, weight=2):
        return default_heat_entry(
            weight=weight,
            initial_by_weight=self.config["initial_by_weight"],
            significant_threshold=self.config["zone_thresholds"]["significant"],
            uncertain_threshold=self.config["zone_thresholds"]["uncertain"],
        )

    # ==============================================================
    # STM heat lock（与 LTM Pinned 无关）
    # ==============================================================


    def remove_entry(self, mem_id):
        """从 heat.json 中删除条目"""
        heat = self.load_heat()
        if mem_id in heat.get("entries", {}):
            del heat["entries"][mem_id]
            self.save_heat(heat)
