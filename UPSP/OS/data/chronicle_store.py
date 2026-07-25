"""
编年史读写 + 保留清理
DDS §28 编年史 + §31 语料库

编年史内部结构（CHR- 无注册表，纯目录）：
  Chronicle/rhythms/  节志（512字/篇，近5日）
  Chronicle/daily/    日志（节志×128字合并，近15天）
  Chronicle/weekly/   周志（日志×0.3压缩，近10周）
  Chronicle/monthly/  月志（周志×0.3压缩，近10月）
  Chronicle/quarterly/ 季志（月志×0.3压缩，近10季）
  Chronicle/yearly/   年志（不删）

语料库内部结构（COR- 无注册表，纯目录）：
  Corpus/public/rounds/    轮备份（近5日）
  Corpus/public/daily/     日合并（近10日）
  Corpus/public/weekly/    周合并（近5周）
  Corpus/public/monthly/   月合并（近5月）
  Corpus/public/quarterly/ 季合并（近5季）
  Corpus/public/yearly/    年合并（不删）
  Corpus/Attic/            阁楼（3年+冷备）

所有清理由日历节律轮脚本执行，不调 LLM。
"""
import os, shutil
from datetime import datetime, timedelta
from paths import LTM_DIR
from constants import TZ_SHANGHAI


# ============================================================
# 保留期限配置
# ============================================================

CHRONICLE_RETENTION = {
    "rhythms":    timedelta(days=5),
    "daily":      timedelta(days=15),
    "weekly":     timedelta(weeks=10),
    "monthly":    timedelta(days=300),    # ≈10月
    "quarterly":  timedelta(days=900),    # ≈10季
    "yearly":     None,                    # 不删
}

CORPUS_RETENTION = {
    "rounds":     timedelta(days=5),
    "daily":      timedelta(days=10),
    "weekly":     timedelta(weeks=5),
    "monthly":    timedelta(days=150),    # ≈5月
    "quarterly":  timedelta(days=450),    # ≈5季
    "yearly":     None,
}

ATTIC_AGE = timedelta(days=1095)  # 3年


class ChronicleStore:
    """编年史读写管理（CHR- 无注册表，纯目录操作）"""

    def __init__(self, chronicle_dir=None):
        self.chronicle_dir = chronicle_dir or os.path.join(LTM_DIR, "Chronicle")

    # ==============================================================
    # 写入
    # ==============================================================

    def write_entry(self, layer, content):
        """写入一条编年史条目。layer: rhythms/daily/weekly/monthly/quarterly/yearly"""
        layer_dir = os.path.join(self.chronicle_dir, layer)
        os.makedirs(layer_dir, exist_ok=True)

        now = datetime.now(TZ_SHANGHAI)
        if layer == "rhythms":
            filename = f"R-{now.strftime('%Y%m%d')}-{now.strftime('%H%M%S')}.md"
        elif layer == "daily":
            filename = f"D-{now.strftime('%Y%m%d')}.md"
        elif layer == "weekly":
            iso = now.isocalendar()
            filename = f"W-{now.year}-{iso[1]:02d}.md"
        elif layer == "monthly":
            filename = f"M-{now.strftime('%Y%m')}.md"
        elif layer == "quarterly":
            q = (now.month - 1) // 3 + 1
            filename = f"Q-{now.year}-{q}.md"
        elif layer == "yearly":
            filename = f"Y-{now.year}.md"
        else:
            filename = f"{layer}-{now.strftime('%Y%m%d%H%M%S')}.md"

        filepath = os.path.join(layer_dir, filename)
        # 追加模式（日志/周志等同周期可能多次写入）
        existing = ""
        if os.path.isfile(filepath):
            with open(filepath, "r", encoding="utf-8") as f:
                existing = f.read()
        with open(filepath, "w", encoding="utf-8") as f:
            if existing:
                f.write(existing.rstrip() + "\n\n")
            f.write(content)

        return filepath

    def active_rhythm_path(self):
        layer_dir = os.path.join(self.chronicle_dir, "rhythms")
        os.makedirs(layer_dir, exist_ok=True)
        return os.path.join(layer_dir, "R-active-main-axis.md")

    def active_calendar_path(self, layer):
        """返回当前日历节律层的活动写入框路径。"""
        layer = str(layer or "").strip()
        layer_dir = os.path.join(self.chronicle_dir, layer)
        os.makedirs(layer_dir, exist_ok=True)
        prefixes = {
            "daily": "D",
            "weekly": "W",
            "monthly": "M",
            "quarterly": "Q",
            "yearly": "Y",
        }
        prefix = prefixes.get(layer, layer or "calendar")
        return os.path.join(layer_dir, f"{prefix}-active-calendar.md")

    def refresh_active_rhythm(
            self,
            *,
            round_num,
            closed_at,
            state_sample=None,
            memory_stats=None,
            range_start_round=None,
            range_start_time=None):
        """刷新当前活动主轴节律文件；该文件自身承载范围与统计。"""
        path = self.active_rhythm_path()
        memory_stats = memory_stats or {}
        weights = memory_stats.get("weights") or {}
        state_sample = state_sample or {}
        lines = [
            "# 活动主轴节律文件",
            "",
            f"range_start_round: {'' if range_start_round is None else range_start_round}",
            f"range_start_time: {'' if range_start_time is None else range_start_time}",
            f"range_end_round: {round_num}",
            f"range_end_time: {closed_at}",
            "",
            "## 新增记忆统计",
            f"新增记忆总数: {int(memory_stats.get('total', 0) or 0)}",
            "权重分布:",
        ]
        for weight in ("F", "S", "A", "P"):
            lines.append(f"- {weight}: {int(weights.get(weight, 0) or 0)}")
        lines.extend([
            "",
            "## 状态样本",
        ])
        for key in sorted(state_sample):
            lines.append(f"- {key}: {state_sample[key]}")
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines).rstrip() + "\n")
        return path

    def refresh_active_calendar(
            self,
            *,
            layer,
            title,
            round_num,
            closed_at,
            source_layer=None,
            max_source_files=5):
        """刷新当前活动日历节律写入框，供模型按 Runtime 焦点写正文。"""
        layer = str(layer or "").strip()
        title = str(title or layer or "日历节律").strip()
        path = self.active_calendar_path(layer)
        source_layer = str(source_layer or "").strip()
        source_paths = self.list_layer(source_layer, limit=max_source_files) if source_layer else []
        source_parts = []
        for source_path in source_paths:
            try:
                with open(source_path, "r", encoding="utf-8") as f:
                    body = f.read().strip()
            except OSError:
                body = ""
            if not body:
                continue
            source_parts.append(
                f"### {os.path.basename(source_path)}\n{body[:4000]}"
            )
        lines = [
            "# 活动日历节律文件",
            "",
            f"layer: {layer}",
            f"title: {title}",
            f"range_end_round: {round_num}",
            f"range_end_time: {closed_at}",
            f"source_layer: {source_layer}",
            "",
            "## 上游材料片段",
        ]
        if source_parts:
            lines.append("\n\n".join(source_parts))
        else:
            lines.append("(当前没有可见上游材料；请基于本轮可见事实写入。)")
        lines.extend([
            "",
            "## 本次写入正文",
        ])
        with open(path, "w", encoding="utf-8") as f:
            f.write("\n".join(lines).rstrip() + "\n")
        return path

    def write_focused_entry(self, focus, content):
        """写入 Runtime 当前挂载的编年史焦点。"""
        focus = focus or {}
        path = str(focus.get("path") or "").strip()
        layer = str(focus.get("layer") or "rhythms").strip()
        if path:
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "a", encoding="utf-8") as f:
                if os.path.getsize(path) > 0:
                    f.write("\n")
                f.write(str(content or "").strip())
                f.write("\n")
            return path
        return self.write_entry(layer, content)

    # ==============================================================
    # 保留清理
    # ==============================================================

    def cleanup_expired(self):
        """清理过期编年史条目（日历节律轮脚本执行）"""
        now = datetime.now(TZ_SHANGHAI)
        cleaned = []

        for layer, retention in CHRONICLE_RETENTION.items():
            if retention is None:
                continue  # 年志不删
            layer_dir = os.path.join(self.chronicle_dir, layer)
            if not os.path.isdir(layer_dir):
                continue
            cutoff = now - retention
            for fname in os.listdir(layer_dir):
                fpath = os.path.join(layer_dir, fname)
                if not os.path.isfile(fpath):
                    continue
                mtime = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_SHANGHAI)
                if mtime < cutoff:
                    try:
                        os.remove(fpath)
                        cleaned.append(fpath)
                    except OSError:
                        pass

        return cleaned

    # ==============================================================
    # 读取
    # ==============================================================

    def list_layer(self, layer, limit=10):
        """列出某层最近的文件"""
        layer_dir = os.path.join(self.chronicle_dir, layer)
        if not os.path.isdir(layer_dir):
            return []
        files = [os.path.join(layer_dir, f) for f in os.listdir(layer_dir)
                 if os.path.isfile(os.path.join(layer_dir, f))]
        files.sort(key=lambda f: os.path.getmtime(f), reverse=True)
        return files[:limit]


class CorpusStore:
    """语料库读写管理（COR- 无注册表，纯目录操作）"""

    def __init__(self):
        self.corpus_dir = os.path.join(LTM_DIR, "Corpus")

    # ==============================================================
    # 写入
    # ==============================================================

    def archive_raw_log(self, raw_log_path):
        """将 raw_log.md 归档到 COR/public/rounds/"""
        if not os.path.isfile(raw_log_path):
            return None
        with open(raw_log_path, "r", encoding="utf-8") as f:
            content = f.read()
        if not content.strip() or content.strip() == "<!-- 原始语料备份 -->":
            return None

        rounds_dir = os.path.join(self.corpus_dir, "public", "rounds")
        os.makedirs(rounds_dir, exist_ok=True)
        now = datetime.now(TZ_SHANGHAI)
        fname = f"rounds_{now.strftime('%Y%m%d_%H%M%S')}.md"
        fpath = os.path.join(rounds_dir, fname)
        with open(fpath, "w", encoding="utf-8") as f:
            f.write(content)

        # 清空源文件
        with open(raw_log_path, "w", encoding="utf-8") as f:
            f.write("<!-- 原始语料备份 -->\n")
        return fpath

    def merge_layer(self, source_layer, target_layer):
        """将 source 层内容合并到 target 层（同级合并，非压缩）"""
        source_dir = os.path.join(self.corpus_dir, "public", source_layer)
        target_dir = os.path.join(self.corpus_dir, "public", target_layer)
        if not os.path.isdir(source_dir):
            return

        os.makedirs(target_dir, exist_ok=True)
        now = datetime.now(TZ_SHANGHAI)
        merged_content = []

        for fname in sorted(os.listdir(source_dir)):
            fpath = os.path.join(source_dir, fname)
            if not os.path.isfile(fpath):
                continue
            with open(fpath, "r", encoding="utf-8") as f:
                merged_content.append(f.read())

        if merged_content:
            tname = f"merged_{now.strftime('%Y%m%d_%H%M%S')}.md"
            tpath = os.path.join(target_dir, tname)
            with open(tpath, "w", encoding="utf-8") as f:
                f.write("\n\n---\n\n".join(merged_content))
            # 合并后删除源文件，防止下次重复合并
            for fname in sorted(os.listdir(source_dir)):
                fpath = os.path.join(source_dir, fname)
                if os.path.isfile(fpath):
                    try:
                        os.remove(fpath)
                    except OSError:
                        pass

    # ==============================================================
    # 保留清理
    # ==============================================================

    def cleanup_expired(self):
        """清理过期语料（日历节律轮脚本执行）"""
        now = datetime.now(TZ_SHANGHAI)
        cleaned = []

        for layer, retention in CORPUS_RETENTION.items():
            if retention is None:
                continue
            layer_dir = os.path.join(self.corpus_dir, "public", layer)
            if not os.path.isdir(layer_dir):
                continue
            cutoff = now - retention
            for fname in os.listdir(layer_dir):
                fpath = os.path.join(layer_dir, fname)
                if not os.path.isfile(fpath):
                    continue
                mtime = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_SHANGHAI)
                if mtime < cutoff:
                    try:
                        os.remove(fpath)
                        cleaned.append(fpath)
                    except OSError:
                        pass

        return cleaned

    def move_to_attic(self):
        """将 3 年以上的语料搬到 Attic"""
        now = datetime.now(TZ_SHANGHAI)
        cutoff = now - ATTIC_AGE
        attic_dir = os.path.join(self.corpus_dir, "Attic")
        os.makedirs(attic_dir, exist_ok=True)
        moved = []

        for layer_dir_name in ["rounds", "daily", "weekly", "monthly", "quarterly"]:
            layer_dir = os.path.join(self.corpus_dir, "public", layer_dir_name)
            if not os.path.isdir(layer_dir):
                continue
            for fname in os.listdir(layer_dir):
                fpath = os.path.join(layer_dir, fname)
                if not os.path.isfile(fpath):
                    continue
                mtime = datetime.fromtimestamp(os.path.getmtime(fpath), TZ_SHANGHAI)
                if mtime < cutoff:
                    try:
                        dest = os.path.join(attic_dir, f"{layer_dir_name}_{fname}")
                        shutil.move(fpath, dest)
                        moved.append(dest)
                    except OSError:
                        pass

        return moved
