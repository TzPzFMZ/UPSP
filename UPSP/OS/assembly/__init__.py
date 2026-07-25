"""
UPSP Base V2 - 上下文装配层 (Layer 6)

当前模块：
  context.py   - 组装 API messages，并同步渲染 step.md / layers
  statusbar.py - state.json 转自然语言描述（数值隔离）
  popup.py     - POPUP 弹窗、指南、提醒与可见工具块
  audit.py     - 审计痕迹写入 STM/context/{step}/

约束：
  - 可 import schemas/paths/constants/data/ 和 logic/
  - 不 import engines/
  - 不承担 phase、心跳事实源或状态结算职责
"""
