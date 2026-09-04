"""
POPUP 存储 — 读写 popup.md
DDS §24 插话机制

data 层独占文件 I/O。assembly 层通过本模块读写 POPUP 数据。
"""
import os

from data.atomic_write import atomic_write_text
from paths import CONTEXT_POPUP
from schemas.context import default_popup_content


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
        atomic_write_text(self.popup_path, content.strip() + "\n")
