"""
UPSP Base V2 — 统一错误类型

约束：
  - 不 import 其他业务模块
  - 每个错误类型带 domain 标记，方便日志定位
  - 所有异常继承 UPSPError 基类
"""


class UPSPError(Exception):
    """UPSP 所有异常的基类"""
    domain = "base"


# ============================================================
# 数据访问层错误
# ============================================================

class DataError(UPSPError):
    """数据读写失败"""
    domain = "data"

class WriteError(DataError):
    """原子写入失败"""
    def __init__(self, path, message=None, cause=None):
        super().__init__(message or f"写入失败: {path}")
        self.path = path
        self.cause = cause


class ReadError(DataError):
    """读取失败/解析失败"""
    def __init__(self, path, message=None, cause=None):
        super().__init__(message or f"读取失败: {path}")
        self.path = path
        self.cause = cause

# ============================================================
# 记忆错误
# ============================================================

class MemoryError(UPSPError):
    """记忆操作错误"""
    domain = "memory"


class EntryNotFoundError(MemoryError):
    """记忆条目不存在"""
    def __init__(self, mem_id, message=None):
        super().__init__(message or f"记忆条目不存在: {mem_id}")
        self.mem_id = mem_id

# ============================================================
# 容器错误
# ============================================================

class ContainerError(UPSPError):
    """容器操作错误"""
    domain = "container"


class ContainerNotFoundError(ContainerError):
    """容器不存在"""

# ============================================================
# 编排层错误
# ============================================================

class EngineError(UPSPError):
    """编排引擎错误"""
    domain = "engine"


class RequiredContextError(EngineError):
    """必需上下文读取或投影失败；只暴露稳定范围与异常类型。"""
    domain = "context"

    def __init__(self, stage, scope, cause):
        self.stage = str(stage or "unknown").strip() or "unknown"
        self.scope = str(scope or "unknown").strip() or "unknown"
        self.cause = cause
        self.error_type = type(cause).__name__
        super().__init__(
            f"required_context_{self.stage}_failed:"
            f"{self.scope}:{self.error_type}"
        )

    def as_dict(self):
        return {
            "receipt_type": "required_context_failure.v1",
            "status": "failed",
            "stage": self.stage,
            "scope": self.scope,
            "reason": str(self),
            "error_type": self.error_type,
        }


class ExecutorError(EngineError):
    """API 执行器异常"""


class APIBridgeError(ExecutorError):
    """API 桥接错误（熔断/握手/超时）"""
    def __init__(self, endpoint, message=None, status_code=None, *, transient=False,
                 allow_fallback=False, affects_connectivity=True):
        super().__init__(message or f"API 错误: {endpoint}")
        self.endpoint = endpoint
        self.status_code = status_code
        self.transient = bool(transient)
        self.allow_fallback = bool(allow_fallback)
        self.affects_connectivity = bool(affects_connectivity)


class APITimeoutError(APIBridgeError):
    """API 请求或响应读取超时；可能已经触达 provider。"""
    def __init__(self, endpoint, message=None, timeout_seconds=None):
        text = message or f"API timeout after {timeout_seconds} seconds: {endpoint}"
        super().__init__(endpoint, text)
        self.timeout_seconds = timeout_seconds
        self.timeout_after_send = True


class ProviderCallCancelled(ExecutorError):
    """用户主动终止当前 provider 等待；不得计入连接失败或重试。"""

    def __init__(self, message="provider_call_cancelled"):
        super().__init__(message)
        self.user_requested = True
