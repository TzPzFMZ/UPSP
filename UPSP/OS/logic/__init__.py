"""
UPSP Base V2 - 业务逻辑层 (Layer 4)

负责语义规则、领域计算、候选选择与协议解析等处理。目标边界是不
拥有 persona 写端；当前仍存在少量待拆 IO 候选，按后续 spec 收口。
约束：不 import engines/assembly；data 依赖必须显式登记并逐步迁移。
"""
