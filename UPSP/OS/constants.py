"""
UPSP Base V2 — 全局常量（无依赖，可被任何模块 import）

约束：
  - 不 import 其他业务模块（paths/schemas/data/logic/engines 都不碰）
  - 只放数值常量和枚举，不放路径
"""
from datetime import timezone, timedelta

# ============================================================
# 时区
# ============================================================

TZ_SHANGHAI = timezone(timedelta(hours=8))

# ============================================================
# 心跳（DDS §23.5）
# ============================================================

HEARTBEAT_DEFAULT_INTERVAL = 5         # 秒
FATIGUE_EXPIRED_HOURS = 20             # 连续清醒超时

# ============================================================
# 记忆衰减（DDS §8-9）
# ============================================================

DECAY_RATES = {
    "显著": -5,
    "未定": -10,
    "衰减": -15,
}

ZONE_THRESHOLDS = {
    "显著": 70,
    "未定": 40,
}

# 升格条件
UPGRADE_AH_HIGH_MIN = 5               # AH_high ≥ 5 → LTM Full

# 热度
INITIAL_HEAT = 50
MAX_HEAT = 100
HEAT_LOCKED_VALUE = 80
RECALL_BOOST = 10

# LTM 降格 token 上限
LTM_LIMITS = {
    "Full": 2048,
    "Summary": 512,
    "Abstract": 128,
}

# 权重→形态映射（DDS §9.2）
WEIGHT_TO_TYPE = {5: "F", 4: "S", 3: "S", 2: "A", 1: "A", 0: "A"}

# ============================================================
# 节律（DDS §3）
# ============================================================

RHYTHM_INTERVAL_ROUNDS = 32           # 节律轮间隔
STANDBY_IDLE_MINUTES = 30             # 待命轮触发

# ============================================================
# 字符上限（DDS §4.10, §9, §20）
# ============================================================

RESIDENT_LIST_CHAR_LIMIT = 65536   # 常驻清单字符上限
REFERENCE_WINDOW_CHAR_LIMIT = 65536 # 参考窗口(高频层CONTENT)字符上限

# ============================================================
# 上下文缓存（DDS §19 — Spec 037 六层频率梯度）
# ============================================================

LATELY_SETUP_ROUNDS = 8            # setup 装配最近缓存轮数
LATELY_REACTION_ROUNDS = 32        # reaction 装配最近缓存轮数
LATELY_CLEANUP_ROUNDS = 8          # cleanup 装配最近缓存轮数
LATELY_HOT_WINDOW_ROUNDS = 40      # lately 履带热窗口总轮数
LATELY_TRIM_ROUNDS = 8             # 达限后删除最旧轮数

# ============================================================
# Token 预警（DDS §3）
# ============================================================

TOKEN_WARNING_RATIO = 0.7             # ≥0.7 预警
TOKEN_URGENT_RATIO = 0.85             # ≥0.85 紧急节律点
TOKEN_WINDOW_SIZE = 200000            # 默认窗口

# ============================================================
# 身份确认（DDS §23）
# ============================================================

IDENTITY_TIMEOUT_SECONDS = 3600       # 1 小时

# ============================================================
# 休眠（DDS §3）
# ============================================================

SLEEP_LEVELS = ("awake", "light", "moderate", "deep")
SLEEP_LIGHT_AFTER_MIN = 15
SLEEP_MODERATE_AFTER_MIN = 45
SLEEP_DEEP_AFTER_MIN = 120

# ============================================================
# 上下文工程（DDS §19）
# ============================================================

STM_INDEX_DISPLAY_LIMIT = 64          # 热度索引显示上限
DREAMS_DISPLAY_LIMIT = 16             # 梦境显示上限

CONTEXT_MODULES = ("STATUSBAR", "EXPLORER", "CONTENT", "RULES", "POPUP")

# ============================================================
# 感受词（DDS §4-5）
# ============================================================

# 感知带宽
INTERACTION_FEELINGS_MAX_PER_ROUND = 3    # 交互感受每轮最多3个
RELATION_FEELINGS_MAX_PER_OBJECT = 2      # 关系感受每对象最多2个

# ============================================================
# 引力场（DDS §7）
# ============================================================

COMFORT_ZONE_MIN = -40
COMFORT_ZONE_MAX = 40

# ============================================================
# 六轴（DDS §4-5）
# ============================================================

CORE_AXIS_LABELS = (
    ("S", "结构/体验"),
    ("C", "收敛/发散"),
    ("V", "证据/幻想"),
    ("A", "分析/直觉"),
    ("R", "批判/协作"),
    ("B", "抽象/具体"),
)

DYNAMIC_AXIS_KEYS = ("valence", "arousal", "focus", "mood", "humor", "safety")
DYNAMIC_AXIS_RANGE = (-100, 100)
CORE_AXIS_RANGE = (0, 100)

RELATION_AXIS_KEYS = ("trust", "safety", "value", "investment", "honesty", "resonance")
RELATION_AXIS_RANGE = (-100, 100)

# ============================================================
# 轮类型枚举（DDS §3）
# ============================================================

ROUND_TYPES = ("interactive", "rhythm", "relay", "autonomous", "standby")

# ============================================================
# 反应步时限（DDS §23.2 时间上限替代最大迭代次数）
# ============================================================

REACTION_TIME_LIMIT = 600  # 默认600秒；1x提醒、2x警告、3x自动continue中继
STANDBY_COUNTDOWN_INITIAL = 10  # 待命倒计时初始值，每待命轮-1，归零触发深睡

# ============================================================
# 反应步退出信号（DDS §23.2）
# ============================================================

REACTION_EXIT_SIGNALS = ("done", "time_limit", "interrupted")
REACTION_CONTINUE_SIGNALS = ("continue_reaction", "waiting_tool")

# ============================================================
# Phase 枚举（DDS §22-23）
# ============================================================

PHASES = ("idle", "presub", "main", "post")

# ============================================================
# 容器类型（DDS §13-18）
# ============================================================

CONTAINER_PREFIXES = ("DC", "EC", "PRJ", "SKL", "IMM", "CHR", "COR", "FUT", "ITR")

# ============================================================
# 速查：DDS 常量总表（DDS §33）
# ============================================================

# 方便一个地方 view 所有"魔法数字"
ALL_CONSTANTS = {
    # 心跳
    "HEARTBEAT_INTERVAL": HEARTBEAT_DEFAULT_INTERVAL,
    "FATIGUE_EXPIRED_HOURS": FATIGUE_EXPIRED_HOURS,
    # 衰减
    "DECAY_RATES": DECAY_RATES,
    "ZONE_THRESHOLDS": ZONE_THRESHOLDS,
    "UPGRADE_AH_HIGH_MIN": UPGRADE_AH_HIGH_MIN,
    "INITIAL_HEAT": INITIAL_HEAT,
    "MAX_HEAT": MAX_HEAT,
    "HEAT_LOCKED_VALUE": HEAT_LOCKED_VALUE,
    "RECALL_BOOST": RECALL_BOOST,
    # LTM
    "LTM_LIMITS": LTM_LIMITS,
    # 节律
    "RHYTHM_INTERVAL_ROUNDS": RHYTHM_INTERVAL_ROUNDS,
    "STANDBY_IDLE_MINUTES": STANDBY_IDLE_MINUTES,
    "RESIDENT_LIST_CHAR_LIMIT": RESIDENT_LIST_CHAR_LIMIT,
    "REFERENCE_WINDOW_CHAR_LIMIT": REFERENCE_WINDOW_CHAR_LIMIT,
    "LATELY_SETUP_ROUNDS": LATELY_SETUP_ROUNDS,
    "LATELY_REACTION_ROUNDS": LATELY_REACTION_ROUNDS,
    "LATELY_CLEANUP_ROUNDS": LATELY_CLEANUP_ROUNDS,
    "LATELY_HOT_WINDOW_ROUNDS": LATELY_HOT_WINDOW_ROUNDS,
    "LATELY_TRIM_ROUNDS": LATELY_TRIM_ROUNDS,
    # Token
    "TOKEN_WARNING_RATIO": TOKEN_WARNING_RATIO,
    "TOKEN_URGENT_RATIO": TOKEN_URGENT_RATIO,
    "TOKEN_WINDOW_SIZE": TOKEN_WINDOW_SIZE,
    # 身份
    "IDENTITY_TIMEOUT_SECONDS": IDENTITY_TIMEOUT_SECONDS,
    # 上下文
    "STM_INDEX_DISPLAY_LIMIT": STM_INDEX_DISPLAY_LIMIT,
    "DREAMS_DISPLAY_LIMIT": DREAMS_DISPLAY_LIMIT,
    # 感受
    "INTERACTION_FEELINGS_MAX_PER_ROUND": INTERACTION_FEELINGS_MAX_PER_ROUND,
    "RELATION_FEELINGS_MAX_PER_OBJECT": RELATION_FEELINGS_MAX_PER_OBJECT,
    # 引力
    "COMFORT_ZONE_MIN": COMFORT_ZONE_MIN,
    "COMFORT_ZONE_MAX": COMFORT_ZONE_MAX,
}
