"""
POPUP 存储 — 读写 popup.md
DDS §24 插话机制

data 层独占文件 I/O。assembly 层通过本模块读写 POPUP 数据。
"""
import os
from threading import RLock

from data.atomic_write import atomic_write_text
from paths import CONTEXT_POPUP
from schemas.context import default_popup_content

# ponytail: 单 Runtime 进程内共用锁；支持多个写进程时再改文件锁。
_POPUP_LOCK = RLock()

class PopupStore:
    """POPUP 文件存储（文件 I/O 唯一入口）"""

    def __init__(self, popup_path=None):
        self.popup_path = popup_path or CONTEXT_POPUP

    def read_popup(self):
        """读 popup.md 当前内容"""
        if not os.path.isfile(self.popup_path):
            return default_popup_content()
        try:
            with open(self.popup_path, "r", encoding="utf-8") as f:
                content = f.read().strip()
            return content if content else ""
        except OSError:
            return ""

    def write_popup(self, content):
        """写入 POPUP 内容（原子）"""
        with _POPUP_LOCK:
            atomic_write_text(self.popup_path, content.strip() + "\n")

    def update_popup(self, transform):
        """锁内读取、更新和原子写；读取失败不得当空文件覆盖。"""
        with _POPUP_LOCK:
            try:
                with open(self.popup_path, encoding="utf-8") as handle:
                    current = handle.read().strip()
            except FileNotFoundError:
                current = ""
            updated = transform(current).strip()
            if updated != current:
                self.write_popup(updated)
            return updated
