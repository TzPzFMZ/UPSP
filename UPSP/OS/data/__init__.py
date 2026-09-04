"""
UPSP Base V2 - 数据访问层 (Layer 3)

负责 persona、config、context、files 等持久文件的读写入口、格式迁移
和原子落盘。模块可以对应单文件或同一领域的一组文件，但不拥有语义
决策。
约束：
  - load_xxx() 和 save_xxx() 必须配对出现
  - 原子写入（tmp + replace）
  - 文件不存在时返回 schema 的 default
  - 不 import logic/engines/assembly
"""
