"""
意外事件日志读写 — STM/health/base/alerts.md

DDS §38.6 L3 心跳急救与 alerts.md Base 版条目格式。
"""
import os
from datetime import datetime

from paths import ALERTS_MD
from errors import ReadError, WriteError
from constants import local_now


class AlertStore:
    """alerts.md 追加写入管理。"""

    def read(self):
        """读取 alerts.md 全文；不存在时返回空字符串。"""
        if not os.path.isfile(ALERTS_MD):
            return ""
        try:
            with open(ALERTS_MD, "r", encoding="utf-8") as f:
                return f.read()
        except OSError as e:
            raise ReadError(ALERTS_MD, cause=e)

    def append_alert(self, round_num, step, event_type, detail, action):
        """按 DDS Markdown 列表行格式追加一条警报。"""
        os.makedirs(os.path.dirname(ALERTS_MD), exist_ok=True)
        now = local_now().isoformat()
        line = (
            f"- `{now}` | round={self._format_round(round_num)} "
            f"| step={self._one_line(step)} "
            f"| type={self._one_line(event_type)} "
            f"| detail={self._one_line(detail)} "
            f"| action={self._one_line(action)}"
        )

        existing = self.read()
        if not existing.strip():
            existing = "<!-- 意外事件日志 -->\n"
        content = existing.rstrip() + "\n\n" + line + "\n"

        tmp = ALERTS_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp, ALERTS_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(ALERTS_MD, cause=e)

    def clear(self):
        """清空 alerts.md，保留文件头。"""
        os.makedirs(os.path.dirname(ALERTS_MD), exist_ok=True)
        tmp = ALERTS_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write("<!-- 意外事件日志 -->\n")
            os.replace(tmp, ALERTS_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(ALERTS_MD, cause=e)

    @staticmethod
    def _format_round(round_num):
        try:
            return f"{int(round_num):05d}"
        except (TypeError, ValueError):
            return str(round_num)

    @staticmethod
    def _one_line(value, limit=500):
        text = str(value or "")
        text = text.replace("\r", " ").replace("\n", " ")
        text = text.replace("|", "/")
        return " ".join(text.split())[:limit]
