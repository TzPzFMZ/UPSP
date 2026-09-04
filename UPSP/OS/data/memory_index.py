"""
倒排索引 — keywords.json 读写 + 查询
DDS §26 倒排索引

四库：
  STM 倒排 → STM/memory/inverted/stm_index.json
  LTM 倒排 → STM/memory/inverted/ltm_index.json
  Skills 倒排 → STM/memory/inverted/skills_index.json
  关系域倒排 → relation/_index/keywords.json

Base 版：拼接 keywords.json 作为简易倒排（keyword → [mem_id, ...]）
Plus 版：迁移到 chromadb 向量库
"""
import json
import os

from data.atomic_write import atomic_write_json
from paths import KEYWORDS_JSON, RELATION_KEYWORDS_JSON
from schemas.memory import default_keywords_json
from errors import ReadError


class MemoryIndex:
    """倒排索引管理（Base 版：keywords.json 简易实现）"""

    def __init__(self, relation_keywords_json=None):
        self.relation_keywords_json = relation_keywords_json or RELATION_KEYWORDS_JSON

    # ==============================================================
    # 读写
    # ==============================================================

    def load_index(self):
        """读取 STM 倒排索引"""
        if not os.path.isfile(KEYWORDS_JSON):
            return default_keywords_json()
        try:
            with open(KEYWORDS_JSON, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(KEYWORDS_JSON, cause=e)

    def save_index(self, data):
        """写入 STM 倒排索引（原子）"""
        atomic_write_json(KEYWORDS_JSON, data)

    # ==============================================================
    # STM 倒排操作
    # ==============================================================

    def remove_stm_entry(self, mem_id):
        """从倒排索引中移除 STM 条目"""
        data = self.load_index()
        index = data.get("index", {})
        for kw in list(index.keys()):
            if mem_id in index[kw]:
                index[kw].remove(mem_id)
                if not index[kw]:
                    del index[kw]
        self.save_index(data)

    # ==============================================================
    # 关系域倒排操作
    # ==============================================================

    def load_relation_index(self):
        """读取关系域倒排索引"""
        path = self.relation_keywords_json
        if not os.path.isfile(path):
            return {"_comment": "关系域倒排索引", "index": {}}
        try:
            with open(path, "r", encoding="utf-8") as f:
                return json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(path, cause=e)

    def save_relation_index(self, data):
        """写入关系域倒排索引（原子）"""
        atomic_write_json(self.relation_keywords_json, data)

    def add_relation_keywords(self, subject_name, keywords):
        """为关系主体添加关键词映射（值=主体名，非 MEM-ID）"""
        data = self.load_relation_index()
        index = data.setdefault("index", {})
        for kw in keywords:
            kw = kw.strip()
            if not kw:
                continue
            if kw not in index:
                index[kw] = []
            if subject_name not in index[kw]:
                index[kw].append(subject_name)
        self.save_relation_index(data)
