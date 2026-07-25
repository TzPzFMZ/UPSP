"""
梦境素材存储 — STM/memory/dreams.md
DDS §29

data 层独占 dreams.md 文件 I/O。
"""
import os
from datetime import datetime

from constants import TZ_SHANGHAI
from errors import WriteError
from paths import DREAMS_MD


class DreamStore:
    """梦境素材追加写入"""

    def __init__(self, path=None):
        self.path = path or DREAMS_MD

    def append_dream(self, content, round_num=None):
        text = str(content or "").strip()
        if not text:
            return False
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        now = datetime.now(TZ_SHANGHAI).isoformat()
        title = f"## R{round_num} {now}" if round_num is not None else f"## {now}"
        entry = f"\n{title}\n{text}\n"
        try:
            with open(self.path, "a", encoding="utf-8") as f:
                f.write(entry)
            return True
        except OSError as e:
            raise WriteError(self.path, cause=e)
