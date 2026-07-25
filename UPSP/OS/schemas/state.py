"""
state.json 完整 Schema + 默认值
DDS §3 state.json + §23.5 heartbeat_flags

维护者：
  engines/runtime.py — 独占写 base.runtime.phase
  engines/heartbeat.py — 独占写 base.heartbeat_flags
  logic/ — 算完值后通过 data/state_store.py 写入对应字段
  data/state_store.py — 唯一的读写入口

写端追踪（数据管线断裂防治）：
  字段 → 谁在什么时候写入
"""
from datetime import datetime, timezone, timedelta

TZ = timezone(timedelta(hours=8))

# ============================================================
# 字段清单（名称 + 类型 + DDS出处 + 写端）
# ============================================================

FIELDS = {
    # --- meta（DDS §3.1）---
    "base.meta.total_round":          ("int",    "§3.1", "engines/runtime.py 每轮+1"),
    "base.meta.daily_round":          ("int",    "§3.1", "engines/runtime.py 每轮+1"),
    "base.meta.last_rhythm_round":    ("int",    "§3.1", "engines/runtime.py 节律轮后更新"),
    "base.meta.last_heartbeat_at":    ("str|None","§3.1","engines/heartbeat.py 每tick更新"),
    "base.meta.last_standby_round":   ("int",    "§3.1", "engines/runtime.py 待命轮后更新"),
    "base.meta.last_round_closed_at": ("str|None","§3.1","engines/cleanup_pipeline.py 每轮闭合后更新"),
    "base.meta.last_external_input_at":("str|None","§3.1","engines/runtime.py 收到外部输入时"),
    "base.meta.last_update":          ("str|None","§3.1", "engines/runtime.py 每轮更新"),
    "base.meta.version":              ("str",    "§3.1", "初始化时设置，后续不变"),
    "base.meta.last_calendar_check_at": ("str|None","§3.1","engines/heartbeat.py 日历节律检查"),
    "base.meta.next_settle_at":       ("str|None","§6",  "logic/feeling_buffer.py 写入下次结算时间"),
    "base.meta.last_state_settlement_id": ("str|None", "§6", "logic/state_settlement.py 每轮成功结算"),
    "base.meta.shelve_timer_at":      ("str|None","§3.1","engines/runtime.py 搁置时写入"),
    "base.meta.last_error":           ("str|None","§3.1","engines/runtime.py 异常时写入"),

    # --- core_axes（DDS §2）---
    "base.core_axes.S": ("int", "§2", "初始化；核心演化另立项"),
    "base.core_axes.C": ("int", "§2", "初始化；核心演化另立项"),
    "base.core_axes.V": ("int", "§2", "初始化；核心演化另立项"),
    "base.core_axes.A": ("int", "§2", "初始化；核心演化另立项"),
    "base.core_axes.R": ("int", "§2", "初始化；核心演化另立项"),
    "base.core_axes.B": ("int", "§2", "初始化；核心演化另立项"),

    # --- dynamic_axes（DDS §4）---
    "base.dynamic_axes.valence.value":  ("int|float", "§4", "logic/state_settlement.py 每轮结算"),
    "base.dynamic_axes.arousal.value":  ("int|float", "§4", "logic/state_settlement.py 每轮结算"),
    "base.dynamic_axes.focus.value":    ("int|float", "§4", "logic/state_settlement.py 每轮结算"),
    "base.dynamic_axes.mood.value":     ("int|float", "§4", "logic/state_settlement.py 每轮结算"),
    "base.dynamic_axes.humor.value":    ("int|float", "§4", "logic/state_settlement.py 每轮结算"),
    "base.dynamic_axes.safety.value":   ("int|float", "§4", "logic/state_settlement.py 每轮结算"),

    # --- comfort_zone（DDS §7.3）---
    "base.comfort_zone.valence":  ("int", "§7.3", "logic/comfort_zone.py 每轮善后步"),
    "base.comfort_zone.arousal":  ("int", "§7.3", "logic/comfort_zone.py 每轮善后步"),
    "base.comfort_zone.focus":    ("int", "§7.3", "logic/comfort_zone.py 每轮善后步"),
    "base.comfort_zone.mood":     ("int", "§7.3", "logic/comfort_zone.py 每轮善后步"),
    "base.comfort_zone.humor":    ("int", "§7.3", "logic/comfort_zone.py 每轮善后步"),
    "base.comfort_zone.safety":   ("int", "§7.3", "logic/comfort_zone.py 每轮善后步"),

    # --- core_speed_wheel（DDS §8.5）---
    "base.core_speed_wheel.current": ("int", "§8.5", "logic/workhood.py 工化指数变化时"),
    "base.core_speed_wheel.max":     ("int", "§8.5", "logic/workhood.py 工化指数变化时"),

    # --- workhood_index（DDS §8）---
    "base.workhood_index.value":            ("float", "§8", "logic/workhood.py 每轮善后步"),
    "base.workhood_index.self_reference":   ("float", "§8", "logic/workhood.py 每轮善后步"),
    "base.workhood_index.self_reflection":  ("float", "§8", "logic/workhood.py 每轮善后步"),
    "base.workhood_index.autonomy":         ("float", "§8", "logic/workhood.py 每轮善后步"),

    # --- activity_mode（DDS §3）---
    "base.activity_mode": ("str", "§3", "engines/runtime.py 轮类型决定"),

    # --- fatigue（DDS §3）---
    "base.fatigue.value":      ("float", "§3", "logic/ 疲劳计算"),
    "base.fatigue.awake_since":("str|None","§3","engines/runtime.py 启动时"),

    # --- token_usage（DDS §3）---
    "base.token_usage.current_tokens":   ("int",   "§3", "engines/executor.py 每次API调用后"),
    "base.token_usage.window_size":      ("int",   "§3", "config/system.json 读取"),
    "base.token_usage.usage_ratio":      ("float", "§3", "engines/runtime_services.py 每次API调用后"),
    "base.token_usage.last_round_input": ("int",   "§3", "engines/executor.py 每轮记录"),
    "base.token_usage.last_round_output":("int",   "§3", "engines/executor.py 每轮记录"),

    # --- identity（DDS §23）---
    "base.identity.confirmed":      ("bool",     "§23", "engines/runtime.py 首轮确认后"),
    "base.identity.confirmed_at":   ("str|None", "§23", "engines/runtime.py 首轮确认后"),
    "base.identity.timeout_seconds":("int",      "§23", "config/system.json 读取，默认3600"),
    "base.identity.local_default_relation_id": ("str|None", "§23", "Runtime/GUI 本地默认关系锚点"),
    "base.identity.current_relation_id": ("str|None", "§23", "Runtime 当前实例关系锚点"),
    "base.identity.current_declared_name": ("str|None", "§23", "setup 已声明未登记对象"),
    "base.identity.current_source": ("str", "§23", "Runtime 关系锚点来源"),

    # --- sleep_state（DDS §3）---
    "base.sleep_state.level":      ("str",     "§3", "engines/runtime.py 休眠/唤醒时"),
    "base.sleep_state.entered_at": ("str|None","§3", "engines/runtime.py 休眠/唤醒时"),

    # --- runtime（DDS §3）---
    "base.runtime.phase":            ("str",      "§3", "engines/runtime.py 独占写"),
    "base.runtime.standby_countdown": ("int",     "§3", "engines/runtime.py 每待命轮-1"),
    "base.runtime.pending_relay_target": ("dict", "§23", "engines/reaction_loop.py 中继目标账本"),
    "base.runtime.relay_intents": ("list", "§23", "logic/relay_intent_pool.py 中继规划池"),
    "base.runtime.relay_intent_seq": ("int", "§23", "logic/relay_intent_pool.py 中继意图稳定序列"),
    "base.runtime.work_intent_debt": ("dict", "§34", "logic/work_intent_debt.py legacy 任务入口债务状态；只读/清理"),

    # --- focus（DDS §25 WB焦点）---
    "base.focus":         ("str|None", "§25", "data/container_store.py mount_focus"),
    "base.old_focus":     ("str|None", "§25", "data/container_store.py unmount_focus"),

    # --- heartbeat_flags（DDS §23.5）---
    "base.heartbeat_flags.fatigue_expired":       ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.feeling_settle_due":    ("bool", "§23.5", "heartbeat 置位，Runtime 本地结算或 Round cleanup 消费"),
    "base.heartbeat_flags.api_degraded":          ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.stm_degrade_pending":   ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.process_down":          ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.user_message_waiting":  ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.rhythm_due":            ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.standby_due":           ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.continue_requested":    ("bool", "§23.5", "engines/heartbeat.py tick"),
    "base.heartbeat_flags.shelve_timer_expired":  ("bool", "§23.5", "engines/heartbeat.py tick"),
    # V2 新增（上次遗漏的两项）
    "base.heartbeat_flags.token_usage_warning":   ("bool", "§3",   "engines/heartbeat.py tick"),
    "base.heartbeat_flags.context_pressure":      ("bool", "§3",   "logic/Runtime 维护节律置位"),
    "base.heartbeat_flags.cache_compaction_due":  ("bool", "§21",  "engines/runtime.py lately 水位删除后置位"),
    "base.heartbeat_flags.identity_timeout":      ("bool", "§23",  "engines/heartbeat.py tick"),
    # V3 新增
    "base.heartbeat_flags.calendar_day_due":      ("bool", "§3",   "engines/heartbeat.py 日历日检测"),
    "base.heartbeat_flags.calendar_week_due":     ("bool", "§3",   "engines/heartbeat.py 日历周检测"),
    "base.heartbeat_flags.calendar_month_due":    ("bool", "§3",   "engines/heartbeat.py 日历月检测"),
    "base.heartbeat_flags.calendar_quarter_due":  ("bool", "§3",   "engines/heartbeat.py 日历季检测"),
    "base.heartbeat_flags.calendar_year_due":     ("bool", "§3",   "engines/heartbeat.py 日历年检测"),
    # V6 新增
    "base.heartbeat_flags.evolution_pending":     ("bool", "§23.5", "engines/heartbeat.py 训练材料阈值检测"),
    "base.alert_deferrals":                       ("dict", "§23.5", "logic/alert_mode_settle.py 紧急处理搁置账本"),

    # --- feeling_buffer（DDS §6）--- V2: 并入 state.json，不再独立文件
    "base.feeling_buffer": ("list", "§6", "logic/feeling_buffer.py 善后步写入"),

    # --- context_cache（DDS §21）--- 上下文装配过期标记
    "base.context_cache.permanent_expired":    ("bool", "§21.1", "assembly/context.py 重建时置false, invalidate时置true"),
    "base.context_cache.periodic_expired":     ("bool", "§21.1", "assembly/context.py 重建时置false, invalidate时置true"),
    "base.context_cache.popup_active":         ("bool", "§21.1", "POPUP 注意力事件存在时置true"),
}


# 善后步可写字段白名单（反应步声明，善后步落地）
CLEANUP_WRITABLE_FIELDS = frozenset({
    # dynamic_axes
    "base.dynamic_axes.valence.value",
    "base.dynamic_axes.arousal.value",
    "base.dynamic_axes.focus.value",
    "base.dynamic_axes.mood.value",
    "base.dynamic_axes.humor.value",
    "base.dynamic_axes.safety.value",
    # comfort_zone
    "base.comfort_zone.valence", "base.comfort_zone.arousal",
    "base.comfort_zone.focus", "base.comfort_zone.mood",
    "base.comfort_zone.humor", "base.comfort_zone.safety",
    # workhood
    "base.workhood_index.value", "base.workhood_index.self_reference",
    "base.workhood_index.self_reflection", "base.workhood_index.autonomy",
    "base.core_speed_wheel.current", "base.core_speed_wheel.max",
    # fatigue
    "base.fatigue.value", "base.fatigue.awake_since",
    # token_usage
    "base.token_usage.current_tokens", "base.token_usage.window_size",
    "base.token_usage.usage_ratio", "base.token_usage.last_round_input",
    "base.token_usage.last_round_output",
    # feeling_buffer
    "base.feeling_buffer",
    # activity_mode
    "base.activity_mode",
    # meta (只读维护)
    "base.meta.last_update",
    "base.meta.last_round_closed_at",
    "base.meta.last_state_settlement_id",
})

# ============================================================
# 默认值模板
# ============================================================

def default_state():
    """返回全新的 state.json 默认模板"""
    now = datetime.now(TZ).isoformat()
    return {
        "base": {
            "meta": {
                "total_round": 0,
                "daily_round": 0,
                "last_calendar_check_at": None,
                "last_rhythm_round": 0,
                "last_heartbeat_at": None,
                "last_standby_round": 0,
                "last_round_closed_at": None,
                "last_external_input_at": None,
                "last_update": now,
                "version": "official-base-v2",
                "next_settle_at": None,
                "last_state_settlement_id": None,
                "shelve_timer_at": None,
            },
            "core_axes": {
                "S": 85, "C": 70, "V": 60,
                "A": 75, "R": 55, "B": 80,
            },
            "dynamic_axes": {
                "valence":  {"value": 0},
                "arousal":  {"value": 0},
                "focus":    {"value": 0},
                "mood":     {"value": 0},
                "humor":    {"value": 0},
                "safety":   {"value": 0},
            },
            "comfort_zone": {
                "valence": 0, "arousal": -20, "focus": 30,
                "mood": 0, "humor": 0, "safety": 10,
            },
            "core_speed_wheel": {
                "current": 0,
                "max": 256,
            },
            "workhood_index": {
                "value": 57.2,
                "self_reference": 66.1,
                "self_reflection": 40.0,
                "autonomy": 70.6,
            },
            "activity_mode": "理论",
            "fatigue": {
                "value": 0.0,
                "awake_since": now,
            },
            "token_usage": {
                "current_tokens": 0,
                "window_size": 200000,
                "usage_ratio": 0.0,
                "last_round_input": 0,
                "last_round_output": 0,
            },
            "identity": {
                "confirmed": False,
                "confirmed_at": None,
                "timeout_seconds": 3600,
                "local_default_relation_id": None,
                "current_relation_id": None,
                "current_declared_name": None,
                "current_source": "unbound",
            },
            "sleep_state": {
                "level": "awake",
                "entered_at": None,
            },
            "focus": None,        # WB 当前焦点容器 ID
            "old_focus": None,    # WB 上一焦点容器 ID
            "runtime": {
                "phase": "idle",
                "standby_countdown": 0,
                "pending_relay_target": {},
                "relay_intents": [],
                "relay_intent_seq": 0,
                "work_intent_debt": {},
            },
            "heartbeat_flags": {
                "fatigue_expired": False,
                "feeling_settle_due": False,
                "api_degraded": False,
                "stm_degrade_pending": False,
                "process_down": False,
                "user_message_waiting": False,
                "rhythm_due": False,
                "standby_due": False,
                "continue_requested": False,
                "shelve_timer_expired": False,
                "token_usage_warning": False,     # V2 新增
                "context_pressure": False,         # Spec471: 上下文压力维护节律
                "cache_compaction_due": False,     # Spec471: 最近缓存压缩维护节律
                "identity_timeout": False,         # V2 新增
                "calendar_day_due": False,         # V5 新增
                "calendar_week_due": False,        # V5 新增
                "calendar_month_due": False,       # V5 新增
                "calendar_quarter_due": False,     # V5 新增
                "calendar_year_due": False,        # V5 新增
                "evolution_pending": False,        # V6 新增
            },
            "alert_deferrals": {},
            "feeling_buffer": [],  # V2: 并入 state.json
            # 上下文装配缓存（DDS §21.1）
            # 高频层、lately 与 now 无过期标记（每轮/每步重算）
            "context_cache": {
                "permanent_expired": True,
                "periodic_expired": True,
                "popup_active": False,
            },
        },
        "plus": {},
        "pro": {},
        "dlc": {},
        "mod": {},
    }
