"""
UPSP Base V2 — 全局常量（无依赖，可被任何模块 import）

约束：
  - 不 import 其他业务模块（paths/schemas/data/logic/engines 都不碰）
  - 只放数值常量和枚举，不放路径
"""
from datetime import datetime

# ============================================================
# 时区
# ============================================================

def local_now():
    """返回带当前 Windows 本地偏移的时间，不缓存夏令时偏移。"""
    return datetime.now().astimezone()


def local_fromtimestamp(timestamp):
    """按时间戳发生时的 Windows 本地偏移返回时间。"""
    return datetime.fromtimestamp(timestamp).astimezone()


# 仅保留给历史外部 import；生产时间戳必须调用 local_now/local_fromtimestamp。
TZ_SHANGHAI = local_now().tzinfo

# ============================================================
# 心跳（DDS §23.5）
# ============================================================

HEARTBEAT_DEFAULT_INTERVAL = 5         # 秒
MAX_HEAT = 100                         # 协议固定范围 0..100

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
TOKEN_WINDOW_SIZE = 200000            # 默认窗口

# ============================================================
# 身份确认（DDS §23）
# ============================================================


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
    "MAX_HEAT": MAX_HEAT,
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
    "TOKEN_WINDOW_SIZE": TOKEN_WINDOW_SIZE,
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
