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
import hashlib
import json
import os
from datetime import datetime, timedelta
from paths import LTM_DIR
from constants import local_now, local_fromtimestamp
from errors import ReadError
from data.atomic_write import atomic_write_text


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

    @staticmethod
    def _sha256_text(text):
        return hashlib.sha256(str(text or "").encode("utf-8")).hexdigest()

    @staticmethod
    def _read_text(path):
        if not os.path.isfile(path):
            return ""
        try:
            with open(path, "r", encoding="utf-8") as handle:
                return handle.read()
        except OSError as exc:
            raise ReadError(path, cause=exc) from exc

    def _target_path(self, layer, closed_at, round_num):
        layer_dir = os.path.join(self.chronicle_dir, layer)
        try:
            now = datetime.fromisoformat(str(closed_at or ""))
        except (TypeError, ValueError):
            raise ValueError("chronicle_closed_at_invalid")
        if layer == "rhythms":
            filename = (
                f"R-{now.strftime('%Y%m%d')}-{now.strftime('%H%M%S')}"
                f"-R{int(round_num or 0):06d}.md"
            )
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
        path = os.path.abspath(os.path.join(layer_dir, filename))
        root = os.path.abspath(self.chronicle_dir)
        if os.path.commonpath([root, path]) != root:
            raise ValueError("chronicle_target_outside_root")
        return path

    def build_rhythm_write_scope(
            self,
            *,
            round_num,
            closed_at,
            state_sample=None,
            memory_stats=None,
            range_start_round=None,
            range_start_time=None,
            guide_id=""):
        """Freeze a main-axis Chronicle write without touching the target."""
        memory_stats = memory_stats or {}
        state_sample = state_sample or {}
        path = self._target_path("rhythms", closed_at, round_num)
        existing = self._read_text(path)
        scope = {
            "schema_version": "chronicle_write_scope.v1",
            "layer": "rhythms",
            "title": "主轴节律记录",
            "target_path": path,
            "target_before_sha256": self._sha256_text(existing),
            "round_num": int(round_num or 0),
            "round_type": "rhythm",
            "source_refs": [f"round:{int(round_num or 0)}"],
            "range_start_round": range_start_round,
            "range_start_time": range_start_time,
            "range_end_round": int(round_num or 0),
            "range_end_time": str(closed_at or ""),
            "state_sample": dict(state_sample),
            "memory_stats": dict(memory_stats),
            "sources": [],
            "guide_id": str(guide_id or "").strip(),
        }
        scope["scope_id"] = self._scope_id(scope)
        return scope

    def build_calendar_write_scope(
            self,
            *,
            layer,
            title,
            round_num,
            closed_at,
            calendar_flag="",
            source_layer=None,
            max_source_files=5,
            guide_id=""):
        """Freeze one calendar Chronicle write without pre-writing a file."""
        layer = str(layer or "").strip()
        title = str(title or layer or "日历节律").strip()
        source_layer = str(source_layer or "").strip()
        source_paths = self.list_layer(source_layer, limit=max_source_files) if source_layer else []
        sources = []
        for source_path in source_paths:
            body = self._read_text(source_path)
            if not body:
                continue
            sources.append({
                "name": os.path.basename(source_path),
                "path": os.path.abspath(source_path),
                "sha256": self._sha256_text(body),
                "content": body[:4000],
            })
        path = self._target_path(layer, closed_at, round_num)
        existing = self._read_text(path)
        flag = str(calendar_flag or "").strip()
        scope = {
            "schema_version": "chronicle_write_scope.v1",
            "layer": layer,
            "title": title,
            "target_path": path,
            "target_before_sha256": self._sha256_text(existing),
            "round_num": int(round_num or 0),
            "round_type": "rhythm",
            "calendar_flag": flag,
            "source_layer": source_layer,
            "source_refs": [
                f"calendar:{flag}",
                f"round:{int(round_num or 0)}",
            ],
            "range_end_round": int(round_num or 0),
            "range_end_time": str(closed_at or ""),
            "state_sample": {},
            "memory_stats": {},
            "sources": sources,
            "guide_id": str(guide_id or "").strip(),
        }
        scope["scope_id"] = self._scope_id(scope)
        return scope

    @staticmethod
    def _scope_id(scope):
        """Identify the due item, not one transient attempt to settle it."""
        calendar_flag = str(scope.get("calendar_flag") or "").strip()
        guide_id = str(scope.get("guide_id") or "").strip()
        if guide_id:
            stable = {
                "kind": "calendar" if calendar_flag else "rhythm",
                "guide_id": guide_id,
                "calendar_flag": calendar_flag,
                "layer": scope.get("layer"),
            }
        elif calendar_flag:
            stable = {
                "kind": "calendar",
                "layer": scope.get("layer"),
                "calendar_flag": calendar_flag,
                "target_name": os.path.basename(
                    str(scope.get("target_path") or "")
                ),
            }
        else:
            stable = {
                "kind": "rhythm",
                "layer": scope.get("layer"),
                "range_start_round": scope.get("range_start_round"),
                "range_start_time": scope.get("range_start_time"),
            }
        digest = hashlib.sha256(json.dumps(
            stable,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            default=str,
        ).encode("utf-8")).hexdigest()
        return f"CWS-{digest[:16].upper()}"

    @staticmethod
    def _path_within(root, path):
        root = os.path.normcase(os.path.realpath(os.path.abspath(root)))
        candidate = os.path.normcase(os.path.realpath(os.path.abspath(path)))
        try:
            return os.path.commonpath([root, candidate]) == root
        except ValueError:
            return False

    def _require_write_scope(self, scope):
        if not isinstance(scope, dict) or scope.get(
                "schema_version") != "chronicle_write_scope.v1":
            raise ValueError("invalid_chronicle_write_scope")
        for field in (
            "scope_id",
            "layer",
            "title",
            "target_path",
            "target_before_sha256",
        ):
            if not str(scope.get(field) or "").strip():
                raise ValueError(f"chronicle_write_scope_missing:{field}")
        layer = str(scope.get("layer") or "").strip()
        if layer not in CHRONICLE_RETENTION:
            raise ValueError("chronicle_write_scope_layer_invalid")
        target_path = str(scope.get("target_path") or "").strip()
        layer_root = os.path.join(self.chronicle_dir, layer)
        if (
            not self._path_within(layer_root, target_path)
            or os.path.normcase(os.path.realpath(os.path.dirname(target_path)))
            != os.path.normcase(os.path.realpath(layer_root))
        ):
            raise ValueError("chronicle_write_scope_target_outside_layer")
        digest = str(scope.get("target_before_sha256") or "").strip().lower()
        if len(digest) != 64 or any(char not in "0123456789abcdef" for char in digest):
            raise ValueError("chronicle_write_scope_target_sha_invalid")
        round_num = scope.get("round_num")
        if (
            isinstance(round_num, bool)
            or not isinstance(round_num, int)
            or round_num < 0
        ):
            raise ValueError("chronicle_write_scope_round_invalid")
        sources = scope.get("sources")
        if not isinstance(sources, list):
            raise ValueError("chronicle_write_scope_sources_invalid")
        source_layer = str(scope.get("source_layer") or "").strip()
        if source_layer and source_layer not in CHRONICLE_RETENTION:
            raise ValueError("chronicle_write_scope_source_layer_invalid")
        if sources and not source_layer:
            raise ValueError("chronicle_write_scope_source_layer_missing")
        source_root = (
            os.path.join(self.chronicle_dir, source_layer)
            if source_layer else self.chronicle_dir
        )
        for source in sources:
            if not isinstance(source, dict):
                raise ValueError("chronicle_write_scope_source_invalid")
            if not str(source.get("path") or "").strip():
                raise ValueError("chronicle_write_scope_source_path_missing")
            source_path = str(source.get("path") or "").strip()
            if (
                not self._path_within(source_root, source_path)
                or os.path.normcase(os.path.realpath(os.path.dirname(source_path)))
                != os.path.normcase(os.path.realpath(source_root))
            ):
                raise ValueError("chronicle_write_scope_source_outside_layer")
            source_sha = str(source.get("sha256") or "").strip().lower()
            if len(source_sha) != 64 or any(
                    char not in "0123456789abcdef" for char in source_sha):
                raise ValueError("chronicle_write_scope_source_sha_invalid")
        return scope

    def render_write_scope_material(self, scope):
        scope = self._require_write_scope(scope)
        stats = scope.get("memory_stats") or {}
        weights = stats.get("weights") or {}
        sample = scope.get("state_sample") or {}
        lines = [
            "【本轮资料】编年史写入材料",
            f"层级：{scope.get('layer')}",
            f"标题：{scope.get('title') or ''}",
            "来源轮次范围："
            f"{scope.get('range_start_round', '')} → {scope.get('range_end_round', '')}",
        ]
        if stats:
            lines.extend([
                f"新增记忆总数：{int(stats.get('total', 0) or 0)}",
                "权重分布：" + "，".join(
                    f"{weight}={int(weights.get(weight, 0) or 0)}"
                    for weight in ("F", "S", "A", "P")
                ),
            ])
        if sample:
            lines.append("状态样本：")
            lines.extend(
                f"- {key}: {sample[key]}" for key in sorted(sample)
            )
        sources = scope.get("sources") or []
        lines.append("既有上游正文片段：")
        if sources:
            for source in sources:
                lines.extend([
                    f"### {source.get('name') or '上游记录'}",
                    str(source.get("content") or "").strip(),
                ])
        else:
            lines.append("（当前没有既有上游正文；请只使用本轮可见事实。）")
        lines.append("请按当前节律指南提交自然语言正文；层级、路径与统计由 Runtime 结算。")
        return {
            "role": "user",
            "kind": "material",
            "interaction_source": "chronicle_write",
            "source_block_id": f"chronicle:{scope.get('scope_id')}",
            "title": "编年史写入材料",
            "content": "\n".join(lines).rstrip(),
        }

    def commit_write_scope(self, scope, content):
        """Atomically append the model body after revalidating the frozen scope."""
        scope = self._require_write_scope(scope)
        body = str(content or "").strip()
        if not body:
            raise ValueError("empty_chronicle_content")
        path = str(scope.get("target_path") or "").strip()
        if not path:
            raise ValueError("chronicle_target_missing")
        root = os.path.abspath(self.chronicle_dir)
        target = os.path.abspath(path)
        if os.path.commonpath([root, target]) != root:
            raise ValueError("chronicle_target_outside_root")
        existing = self._read_text(target)
        scope_id = str(scope.get("scope_id") or "").strip()
        if not scope_id:
            raise ValueError("chronicle_scope_id_missing")
        start = f"<!-- chronicle_write_scope:{scope_id} -->"
        end = f"<!-- /chronicle_write_scope:{scope_id} -->"
        if start in body or end in body:
            raise ValueError("chronicle_scope_marker_in_content")
        committed = self._find_committed_scope(
            layer=str(scope.get("layer") or "").strip(),
            start=start,
            end=end,
        )
        if committed is not None:
            committed_path, committed_body = committed
            if committed_body == body:
                return committed_path
            raise ValueError("chronicle_scope_conflict")
        for source in scope.get("sources") or []:
            source_path = str(source.get("path") or "").strip()
            if not source_path or self._sha256_text(
                    self._read_text(source_path)) != str(
                        source.get("sha256") or ""):
                raise ValueError("chronicle_source_drift")
        entry = self._render_scope_entry(scope, body, start, end)
        if self._sha256_text(existing) != str(
                scope.get("target_before_sha256") or ""):
            raise ValueError("chronicle_target_drift")
        updated = (
            existing.rstrip() + "\n\n" + entry
            if existing.strip() else entry
        ).rstrip() + "\n"
        existed_before = os.path.isfile(target)
        try:
            atomic_write_text(target, updated)
            if self._read_text(target) != updated:
                raise ValueError("chronicle_write_readback_mismatch")
        except Exception:
            try:
                if existed_before:
                    atomic_write_text(target, existing)
                elif os.path.isfile(target):
                    os.remove(target)
            except Exception as rollback_exc:
                raise RuntimeError(
                    "chronicle_write_rollback_failed"
                ) from rollback_exc
            raise
        return target

    def _find_committed_scope(self, *, layer, start, end):
        """Find one prior commit for a stable due item across retry Rounds."""
        layer_dir = os.path.abspath(os.path.join(self.chronicle_dir, layer))
        root = os.path.abspath(self.chronicle_dir)
        if os.path.commonpath([root, layer_dir]) != root:
            raise ValueError("chronicle_target_outside_root")
        if not os.path.isdir(layer_dir):
            return None
        matches = []
        for name in sorted(os.listdir(layer_dir)):
            candidate = os.path.abspath(os.path.join(layer_dir, name))
            if os.path.commonpath([layer_dir, candidate]) != layer_dir:
                raise ValueError("chronicle_target_outside_root")
            if not os.path.isfile(candidate):
                continue
            text = self._read_text(candidate)
            lines = text.splitlines()
            start_positions = [
                index for index, line in enumerate(lines) if line == start
            ]
            end_positions = [
                index for index, line in enumerate(lines) if line == end
            ]
            if not start_positions and not end_positions:
                continue
            if len(start_positions) != 1 or len(end_positions) != 1:
                raise ValueError("chronicle_scope_conflict")
            start_index = start_positions[0]
            end_index = end_positions[0]
            if end_index <= start_index:
                raise ValueError("chronicle_scope_conflict")
            section = lines[start_index + 1:end_index]
            body_headers = [
                index for index, line in enumerate(section) if line == "## 正文"
            ]
            if len(body_headers) != 1:
                raise ValueError("chronicle_scope_conflict")
            body = "\n".join(section[body_headers[0] + 1:]).strip()
            matches.append((candidate, body))
        if len(matches) > 1:
            raise ValueError("chronicle_scope_conflict")
        return matches[0] if matches else None

    @staticmethod
    def _render_scope_entry(scope, body, start, end):
        stats = scope.get("memory_stats") or {}
        weights = stats.get("weights") or {}
        lines = [
            start,
            f"# {scope.get('title') or scope.get('layer') or '编年史'}",
            "",
            f"layer: {scope.get('layer')}",
            f"range_start_round: {scope.get('range_start_round', '')}",
            f"range_start_time: {scope.get('range_start_time', '')}",
            f"range_end_round: {scope.get('range_end_round', '')}",
            f"range_end_time: {scope.get('range_end_time', '')}",
        ]
        if stats:
            lines.extend([
                "",
                "## 新增记忆统计",
                f"新增记忆总数: {int(stats.get('total', 0) or 0)}",
                "权重分布:",
            ])
            lines.extend(
                f"- {weight}: {int(weights.get(weight, 0) or 0)}"
                for weight in ("F", "S", "A", "P")
            )
        sample = scope.get("state_sample") or {}
        if sample:
            lines.extend(["", "## 状态样本"])
            lines.extend(f"- {key}: {sample[key]}" for key in sorted(sample))
        sources = scope.get("sources") or []
        if sources:
            lines.extend(["", "## 上游材料"])
            lines.extend(f"- {item.get('name')}" for item in sources)
        lines.extend(["", "## 正文", body, end])
        return "\n".join(str(line) for line in lines).rstrip()

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
