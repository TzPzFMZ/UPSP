"""
state.json 唯一读写入口
DDS §3 state.json

写端追踪：
  engines/runtime.py → base.runtime.phase
  engines/heartbeat.py → base.heartbeat_flags (20项)
  logic/ → 算完值后通过 update_xxx 写入对应字段

约束：
  - 只有本模块直接操作 state.json 文件
  - 原子写入: tmp + os.replace，加重试
  - 线程锁防心跳和主线程并发写
"""
import json
import os
import time
import threading
import tempfile
from copy import deepcopy

from paths import STATE_JSON
from schemas.state import FIELDS, default_state
from errors import WriteError, ReadError
from constants import local_now

INTERACTION_ANCHOR_SOURCES = frozenset({
    "unbound", "local_default", "instance_selection",
    "self_declaration", "relation_card_created",
})

# 全局文件锁
_LOCK = threading.Lock()


class StateStore:
    """state.json 读写管理器"""

    def __init__(self, path=None):
        self.path = path or STATE_JSON
        self._cache = None       # TD-001: 内存缓存，避免每轮反复读盘
        self._cache_dirty = False

    # ==============================================================
    # 读取
    # ==============================================================

    def load(self):
        """严格读取完整 state.json；缺失或坏值均不得由 Runtime 回填。"""
        if self._cache is not None:
            return deepcopy(self._cache)
        if not os.path.isfile(self.path):
            raise ReadError(self.path, message=f"状态真源不存在: {self.path}")
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            self._validate(loaded)
            self._cache = deepcopy(loaded)
            return deepcopy(self._cache)
        except (json.JSONDecodeError, OSError, ValueError) as e:
            raise ReadError(self.path, cause=e)

    def read_snapshot(self):
        """严格只读当前 state.json；不补缺、不缓存、不触发写回。"""
        if not os.path.isfile(self.path):
            raise ReadError(self.path, message=f"状态真源不存在: {self.path}")
        try:
            with open(self.path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            self._validate(loaded)
        except (json.JSONDecodeError, OSError, ValueError) as e:
            raise ReadError(self.path, cause=e)
        return deepcopy(loaded)

    @staticmethod
    def _validate(data):
        if not isinstance(data, dict):
            raise ValueError("state must be an object")
        for dotpath, (type_spec, _, _) in FIELDS.items():
            cur = data
            for key in dotpath.split("."):
                if not isinstance(cur, dict) or key not in cur:
                    raise ValueError(f"missing state field: {dotpath}")
                cur = cur[key]
            allowed = type_spec.split("|")
            valid = (
                ("None" in allowed and cur is None)
                or ("bool" in allowed and isinstance(cur, bool))
                or (
                    "int" in allowed
                    and isinstance(cur, int)
                    and not isinstance(cur, bool)
                )
                or (
                    "float" in allowed
                    and isinstance(cur, (int, float))
                    and not isinstance(cur, bool)
                )
                or ("str" in allowed and isinstance(cur, str))
                or ("dict" in allowed and isinstance(cur, dict))
                or ("list" in allowed and isinstance(cur, list))
            )
            if not valid:
                raise ValueError(f"invalid state field: {dotpath}")
        base = data["base"]
        for value in base["core_axes"].values():
            if not 0 <= value <= 100:
                raise ValueError("core axis out of range")
        for slot in base["dynamic_axes"].values():
            if not -100 <= slot["value"] <= 100:
                raise ValueError("dynamic axis out of range")
        for value in base["comfort_zone"].values():
            if not -40 <= value <= 40:
                raise ValueError("comfort value out of range")
        flags = base["heartbeat_flags"]
        for name in ("fatigue_expired", "identity_timeout", "process_down"):
            if flags[name] is not False:
                raise ValueError(f"reserved Seed flag must be false: {name}")


    def get(self, dotpath, default=None):
        """按点号路径读单个字段，如 'base.meta.total_round'"""
        data = self.load()
        keys = dotpath.split(".")
        cur = data
        for k in keys:
            if isinstance(cur, dict) and k in cur:
                cur = cur[k]
            else:
                return default
        return cur

    # ==============================================================
    # 写入（原子操作）
    # ==============================================================

    def save(self, data):
        """全量写入 state.json（原子 + 线程安全 + 重试）。同步更新内存缓存"""
        self._validate(data)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        with _LOCK:
            fd, tmp = tempfile.mkstemp(suffix=".tmp", dir=os.path.dirname(self.path))
            try:
                with os.fdopen(fd, "w", encoding="utf-8") as f:
                    json.dump(data, f, ensure_ascii=False, indent=2)
                # Windows 上 os.replace 可能被杀软短暂锁定，加重试
                for attempt in range(5):
                    try:
                        os.replace(tmp, self.path)
                        self._cache = deepcopy(data)
                        return
                    except PermissionError:
                        if attempt < 4:
                            time.sleep(0.05 * (attempt + 1))
                        else:
                            raise WriteError(self.path,
                                message=f"os.replace 失败（重试{attempt+1}次）")
            except Exception:
                if os.path.isfile(tmp):
                    try:
                        os.remove(tmp)
                    except OSError:
                        pass
                raise

    def set(self, dotpath, value):
        """按点号路径写入单个字段（白名单校验）"""
        if dotpath not in FIELDS:
            raise ValueError(f"未知 state 字段: {dotpath}")
        self._set_internal(dotpath, value)

    def _set_internal(self, dotpath, value):
        """内部写入 escape hatch：仅 engines/ 层必要时使用，不走白名单校验"""
        data = self.load()
        keys = dotpath.split(".")
        cur = data
        for k in keys[:-1]:
            if k not in cur or not isinstance(cur[k], dict):
                cur[k] = {}
            cur = cur[k]
        cur[keys[-1]] = value
        self.save(data)

    def update_many(self, updates):
        """批量更新多个字段（一次读写，字段白名单校验）"""
        for dotpath in updates:
            if dotpath not in FIELDS:
                raise ValueError(f"未知 state 字段: {dotpath}")
        data = self.load()
        for dotpath, value in updates.items():
            keys = dotpath.split(".")
            cur = data
            for k in keys[:-1]:
                if k not in cur or not isinstance(cur[k], dict):
                    cur[k] = {}
                cur = cur[k]
            cur[keys[-1]] = value
        self.save(data)

    # ==============================================================
    # 便捷方法
    # ==============================================================

    def get_phase(self):
        return self.get("base.runtime.phase", "idle")

    def set_phase(self, phase):
        if phase not in ("idle", "presub", "main", "post"):
            raise ValueError(f"非法 phase: {phase}（合法值：idle/presub/main/post）")
        self.set("base.runtime.phase", phase)

    def get_flags(self):
        return self.get("base.heartbeat_flags", {})

    def set_flag(self, name, value):
        self.set(f"base.heartbeat_flags.{name}", bool(value))

    def clear_flags(self, names):
        updates = {f"base.heartbeat_flags.{n}": False for n in names}
        self.update_many(updates)

    def get_total_round(self):
        return self.get("base.meta.total_round", 0)

    def increment_round(self):
        from datetime import datetime
        now = local_now().isoformat()
        data = self.load()
        meta = data["base"]["meta"]
        meta["total_round"] = meta.get("total_round", 0) + 1
        meta["daily_round"] = meta.get("daily_round", 0) + 1
        meta["last_update"] = now
        self.save(data)
        return meta["total_round"]

    def init_if_missing(self):
        """显式测试辅助；正式活动 state 只能由 PersonaInitializer 创建。"""
        if not os.path.isfile(self.path):
            if os.path.abspath(self.path) == os.path.abspath(STATE_JSON):
                raise WriteError(
                    self.path,
                    message="persona_initialization_required",
                )
            from datetime import datetime
            state = default_state()
            state["base"]["meta"]["last_update"] = \
                local_now().isoformat()
            self.save(state)
            return True
        return False

    def update_token_usage(self, current_tokens, window_size, usage_ratio,
                           input_tokens, output_tokens):
        """更新 token 用量（engines/executor 调用）"""
        self.update_many({
            "base.token_usage.current_tokens": current_tokens,
            "base.token_usage.window_size": window_size,
            "base.token_usage.usage_ratio": usage_ratio,
            "base.token_usage.last_round_input": input_tokens,
            "base.token_usage.last_round_output": output_tokens,
        })

    def confirm_identity(self):
        """标记身份已确认"""
        from datetime import datetime
        now = local_now().isoformat()
        self.update_many({
            "base.identity.confirmed": True,
            "base.identity.confirmed_at": now,
        })

    def set_interaction_anchor(self, relation_id=None, declared_name=None,
                               source="unbound"):
        """原子更新当前实例关系锚点；调用方负责关系卡校验。"""
        source = str(source or "unbound")
        if source not in INTERACTION_ANCHOR_SOURCES:
            raise ValueError("invalid_interaction_anchor_source")
        self.update_many({
            "base.identity.current_relation_id": relation_id or None,
            "base.identity.current_declared_name": declared_name or None,
            "base.identity.current_source": source,
        })
