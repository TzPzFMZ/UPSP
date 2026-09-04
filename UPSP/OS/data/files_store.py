"""
OS/files 资料暂存区。

DDS v0.11.0: raw/media_raw 跟资料输入 FIFO，clips/archive 由月度节律整理。
"""
import os
from pathlib import Path

from paths import (
    FILES_DIR,
)


class FilesStore:
    """管理 OS/files 四类资料暂存目录，不建立全局索引。"""

    def __init__(self, os_root=None):
        if os_root is None:
            self.files_dir = Path(FILES_DIR)
        else:
            self.files_dir = Path(os_root) / "files"
        self.raw_dir = self.files_dir / "raw"
        self.media_raw_dir = self.files_dir / "media_raw"
        self.clips_dir = self.files_dir / "clips"
        self.archive_dir = self.files_dir / "archive"

    def ensure_layout(self):
        for path in (self.raw_dir, self.media_raw_dir, self.clips_dir, self.archive_dir):
            path.mkdir(parents=True, exist_ok=True)
        return {
            "raw": str(self.raw_dir),
            "media_raw": str(self.media_raw_dir),
            "clips": str(self.clips_dir),
            "archive": str(self.archive_dir),
        }

    def save_raw_text(self, text, filename):
        return self._write_text(self.raw_dir, filename, text)

    def save_raw_bytes(self, data, filename, media=False):
        return self._write_bytes(self.media_raw_dir if media else self.raw_dir, filename, data)

    def save_clip_text(self, text, filename):
        return self._write_text(self.clips_dir, filename, text)

    def save_archive_text(self, text, filename):
        return self._write_text(self.archive_dir, filename, text)


    def _write_text(self, root, filename, text):
        target = self._safe_target(root, filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_text(text or "", encoding="utf-8")
        os.replace(tmp, target)
        return str(target)

    def _write_bytes(self, root, filename, data):
        target = self._safe_target(root, filename)
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_bytes(data or b"")
        os.replace(tmp, target)
        return str(target)

    def _safe_target(self, root, filename):
        self.ensure_layout()
        name = Path(filename).name
        if not name:
            raise ValueError("filename 不能为空")
        target = (Path(root) / name).resolve()
        root_resolved = Path(root).resolve()
        if not target.is_relative_to(root_resolved):
            raise ValueError("目标路径越界")
        return target
