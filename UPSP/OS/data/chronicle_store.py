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
  Corpus/public/rhythms/   节级原始语料（近5日）
  Corpus/public/daily/     日合并（近10日）
  Corpus/public/weekly/    周合并（近5周）
  Corpus/public/monthly/   月合并（近5月）
  Corpus/public/quarterly/ 季合并（近5季）
  Corpus/public/yearly/    年合并（不删）
  Corpus/Attic/            阁楼（3年+冷备）

所有清理由日历节律轮脚本执行，不调 LLM。
"""
import json
import os
from datetime import datetime, timedelta
from paths import LTM_DIR
from constants import local_now, local_fromtimestamp
from errors import ReadError


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
    "rhythms":    timedelta(days=5),
    "daily":      timedelta(days=10),
    "weekly":     timedelta(weeks=5),
    "monthly":    timedelta(days=150),    # ≈5月
    "quarterly":  timedelta(days=450),    # ≈5季
    "yearly":     None,
}

def dedupe_corpus_records(records):
    by_key = {}
    ordered = []
    for record in records:
        ref = record.get("ref") if isinstance(record.get("ref"), dict) else {}
        key = str(ref.get("raw_log_key") or "").strip()
        if not key:
            raise ValueError("Corpus record missing ref.raw_log_key")
        loc = record.get("loc") if isinstance(record.get("loc"), dict) else {}
        comparable_ref = dict(ref)
        comparable_ref.pop("raw_log_key", None)
        comparable_ref.pop("source_block_id", None)
        comparable = {
            "role": record.get("role"),
            "kind": record.get("kind"),
            "text": record.get("text"),
            "loc": {
                "round": loc.get("round"),
                "step": loc.get("step"),
                "iter": loc.get("iter"),
            },
            "policy": record.get("policy"),
            "ref": comparable_ref,
        }
        canonical = json.dumps(
            comparable,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        previous = by_key.get(key)
        if previous is not None:
            if previous != canonical:
                raise ValueError(f"Corpus raw_log_key conflict: {key}")
            continue
        by_key[key] = canonical
        ordered.append(record)
    return ordered


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

        now = local_now()
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
        now = local_now()
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
                mtime = local_fromtimestamp(os.path.getmtime(fpath))
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

    def merge_layer(self, source_layer, target_layer):
        """按 raw_log_key 合并 JSONL，并由同批记录派生 Markdown。"""
        source_dir = os.path.join(self.corpus_dir, "public", source_layer)
        target_dir = os.path.join(self.corpus_dir, "public", target_layer)
        if not os.path.isdir(source_dir):
            return None

        os.makedirs(target_dir, exist_ok=True)
        now = local_now()
        sources = [
            os.path.join(source_dir, name)
            for name in sorted(os.listdir(source_dir))
            if name.endswith(".jsonl")
        ]
        records = dedupe_corpus_records(
            record
            for path in sources
            for record in self._read_jsonl(path)
        )
        if not records:
            return None
        target = os.path.join(
            target_dir,
            f"merged_{now.strftime('%Y%m%d_%H%M%S_%f')}.jsonl",
        )
        self._write_pair(target, records)
        for path in sources:
            for item in (path, os.path.splitext(path)[0] + ".md"):
                if os.path.isfile(item):
                    os.remove(item)
        return target

    @staticmethod
    def _read_jsonl(path):
        records = []
        try:
            with open(path, "r", encoding="utf-8") as handle:
                for line_number, line in enumerate(handle, 1):
                    if not line.strip():
                        continue
                    value = json.loads(line)
                    if not isinstance(value, dict):
                        raise ValueError(f"{path}:{line_number} must be an object")
                    records.append(value)
        except (OSError, json.JSONDecodeError) as exc:
            raise ReadError(path, cause=exc)
        return records

    @staticmethod
    def _dedupe_records(records):
        return dedupe_corpus_records(records)

    @staticmethod
    def _render_md(records):
        parts = ["<!-- Corpus Markdown view; JSONL is the machine truth. -->"]
        for record in records:
            loc = record.get("loc") if isinstance(record.get("loc"), dict) else {}
            parts.extend([
                "",
                f"## R{loc.get('round', '?')} / {loc.get('step', '?')} / {loc.get('iter', 0)}",
                "",
                f"**{record.get('role', 'system')} / {record.get('kind', 'unknown')}**",
                "",
                str(record.get("text") or ""),
            ])
        return "\n".join(parts).rstrip() + "\n"

    @classmethod
    def _write_pair(cls, jsonl_path, records):
        from data.atomic_write import atomic_write_jsonl, atomic_write_text
        atomic_write_jsonl(jsonl_path, records)
        atomic_write_text(
            os.path.splitext(jsonl_path)[0] + ".md",
            cls._render_md(records),
        )

    # ==============================================================
    # 保留清理
    # ==============================================================

    def cleanup_expired(self):
        """清理过期语料（日历节律轮脚本执行）"""
        now = local_now()
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
                mtime = local_fromtimestamp(os.path.getmtime(fpath))
                if mtime < cutoff:
                    try:
                        os.remove(fpath)
                        cleaned.append(fpath)
                    except OSError:
                        pass

        return cleaned

    def move_to_attic(self):
        """将满 3 年的 yearly 机器真源成对迁入 Attic。"""
        now = local_now()
        try:
            cutoff = now.replace(year=now.year - 3)
        except ValueError:
            cutoff = now.replace(year=now.year - 3, day=28)
        attic_dir = os.path.join(self.corpus_dir, "Attic")
        moved = []
        yearly_dir = os.path.join(self.corpus_dir, "public", "yearly")
        if not os.path.isdir(yearly_dir):
            return moved

        eligible = {}
        for name in sorted(os.listdir(yearly_dir)):
            if not name.endswith(".jsonl"):
                continue
            if not name.startswith("merged_") or len(name) < len("merged_YYYYMMDD"):
                raise ValueError(f"unrecognized yearly corpus filename: {name}")
            stamp = name[len("merged_"):len("merged_") + 8]
            try:
                created = datetime.strptime(stamp, "%Y%m%d").replace(
                    tzinfo=now.tzinfo)
            except ValueError as exc:
                raise ValueError(
                    f"unrecognized yearly corpus filename: {name}") from exc
            if created > cutoff:
                continue
            eligible.setdefault(created.year, []).append(
                os.path.join(yearly_dir, name))

        for year, sources in sorted(eligible.items()):
            year_dir = os.path.join(attic_dir, str(year))
            target = os.path.join(year_dir, f"attic-{year}.jsonl")
            records = []
            if os.path.isfile(target):
                records.extend(self._read_jsonl(target))
            for source in sources:
                records.extend(self._read_jsonl(source))
            merged = dedupe_corpus_records(records)
            self._write_pair(target, merged)
            if dedupe_corpus_records(self._read_jsonl(target)) != merged:
                raise ValueError(f"Attic verification failed: {target}")
            md_target = os.path.splitext(target)[0] + ".md"
            with open(md_target, "r", encoding="utf-8") as handle:
                if handle.read() != self._render_md(merged):
                    raise ValueError(f"Attic Markdown verification failed: {md_target}")
            for source in sources:
                for item in (source, os.path.splitext(source)[0] + ".md"):
                    if os.path.isfile(item):
                        os.remove(item)
            moved.append(target)
        return moved
