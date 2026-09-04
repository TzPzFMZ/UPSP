"""
UPSP Base V2 — 数据格式定义层 (Layer 1)

每个模块输出：
  DEFAULT_XXX  — 默认模板（初始化和占位文件生成用）
  validate_xxx(data) — 校验函数（测试用）
  FIELDS — 字段清单（名称+类型+DDS出处）

约束：
  - 绝不 import data/ logic/ engines/ assembly/
  - 可以被任何上层模块 import
"""
