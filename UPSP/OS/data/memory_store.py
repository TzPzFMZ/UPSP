"""
记忆条目正文读写 — memory.md / meta.json / index.md
DDS §9 记忆体系

职责：
  - 记忆条目 CRUD（memory.md 追加式写入）
  - 元数据管理（meta.json）
  - 索引行维护（index.md）

注意：heat.json 在 memory_heat.py，倒排索引在 memory_index.py
"""
import json
import os
import re
import threading

from data.atomic_write import atomic_write_json, atomic_write_text
import paths as runtime_paths
from utils.content_ranges import apply_explicit_range, range_kwargs_from_request
from paths import (
    ACTIVE_INSTANCE_ID, LTM_MEMORY_LINKS_JSON,
    MEMORY_MD, META_JSON, INDEX_MD,
    LTM_FULL_FULL_MD, LTM_FULL_META_JSON,
    LTM_SUMMARY_SUMMARY_MD, LTM_SUMMARY_META_JSON,
    LTM_ABSTRACT_ABSTRACT_MD, LTM_ABSTRACT_META_JSON,
    LTM_PINNED_PINNED_MD, LTM_PINNED_META_JSON,
)
from schemas.memory import (
    default_meta_entry, default_meta_json,
    MEMORY_ENTRY_TEMPLATE, INDEX_HEADER, INDEX_SEPARATOR, META_ENTRY_FIELDS,
)
from errors import EntryNotFoundError, WriteError, ReadError
from constants import local_now, normalize_iso_timestamp


MEMORY_OVERLAY_SCHEMA = "upsp_memory_links.v1"
MEMORY_OVERLAY_FIELDS = (
    "linked_containers", "current_overview", "current_overview_updated_at",
)
MEMORY_MUTATION_LOCK = threading.RLock()


def memory_target_tier(weight):
    if isinstance(weight, bool):
        raise ValueError("invalid_memory_weight")
    try:
        value = int(weight)
    except (TypeError, ValueError) as exc:
        raise ValueError("invalid_memory_weight") from exc
    if value < 1 or value > 5:
        raise ValueError("invalid_memory_weight")
    return "Full" if value == 5 else "Summary" if value >= 3 else "Abstract"


def memory_stm_forgetting_target(weight):
    """Return the admitted tier/weight after pending STM forgetting."""
    memory_target_tier(weight)
    value = int(weight)
    if value == 5:
        return "Summary", 4
    if value >= 3:
        return "Abstract", 2
    return "Abstract", value


def memory_is_admitted(meta):
    return bool(str((meta or {}).get("stored_at") or "").strip())


LTM_META_PATHS = {
    os.path.abspath(path) for path in (
        LTM_FULL_META_JSON, LTM_SUMMARY_META_JSON,
        LTM_ABSTRACT_META_JSON, LTM_PINNED_META_JSON,
    )
}
_IMPORTED_LTM_PATHS = {
    name: globals()[name]
    for name in (
        "LTM_FULL_FULL_MD", "LTM_FULL_META_JSON",
        "LTM_SUMMARY_SUMMARY_MD", "LTM_SUMMARY_META_JSON",
        "LTM_ABSTRACT_ABSTRACT_MD", "LTM_ABSTRACT_META_JSON",
        "LTM_PINNED_PINNED_MD", "LTM_PINNED_META_JSON",
    )
}


def _live_path(name):
    """Honor either a module-bound or central-path test/runtime rebind."""
    module_value = globals().get(name)
    runtime_value = getattr(runtime_paths, name)
    initial = _IMPORTED_LTM_PATHS.get(name, module_value)
    module_changed = module_value != initial
    runtime_changed = runtime_value != initial
    if module_changed and not runtime_changed:
        return module_value
    if runtime_changed and not module_changed:
        return runtime_value
    if module_changed and runtime_changed and module_value != runtime_value:
        raise ValueError(f"memory_path_rebind_conflict:{name}")
    return runtime_value


def _read_memory_overlay():
    if not os.path.isfile(LTM_MEMORY_LINKS_JSON):
        return {"schema_version": MEMORY_OVERLAY_SCHEMA, "entries": {}}
    try:
        with open(LTM_MEMORY_LINKS_JSON, "r", encoding="utf-8") as handle:
            value = json.load(handle)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ReadError(LTM_MEMORY_LINKS_JSON, cause=exc)
    if (
        not isinstance(value, dict)
        or value.get("schema_version") != MEMORY_OVERLAY_SCHEMA
        or not isinstance(value.get("entries"), dict)
    ):
        raise ReadError(LTM_MEMORY_LINKS_JSON)
    return value


def write_memory_overlay_entry(mem_id, source):
    overlay = _read_memory_overlay()
    current = overlay["entries"].get(mem_id)
    current = current if isinstance(current, dict) else {}
    entry = {
        key: source.get(
            key, current.get(key, [] if key == "linked_containers" else "")
        )
        for key in MEMORY_OVERLAY_FIELDS
    }
    if current == entry:
        return entry
    overlay["entries"][mem_id] = entry
    atomic_write_json(LTM_MEMORY_LINKS_JSON, overlay)
    return entry


def shared_memory_meta_entry(source):
    return {
        key: value for key, value in source.items()
        if key not in MEMORY_OVERLAY_FIELDS
    }


def shared_memory_meta_document(source):
    return {
        mem_id: shared_memory_meta_entry(entry) if isinstance(entry, dict) else entry
        for mem_id, entry in source.items()
    }


def _overview_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if not text:
        return ""
    if len(text) > 128:
        raise ValueError("current_overview_too_long")
    return text


def _dream_text(value):
    return "是" if bool(value) else "否"


def _normalise_meta_entry(value):
    entry = dict(value) if isinstance(value, dict) else {}
    entry.setdefault("current_overview_updated_at", "")
    entry.setdefault("created_instance_id", "meta")
    entry.setdefault("last_recalled_instance_id", "meta")
    entry.setdefault("stored_at", "")
    return entry


def _round_ref(value, instance_id):
    if not isinstance(value, int) or isinstance(value, bool):
        return "未记录"
    return f"{str(instance_id or 'meta')}/R{value:06d}"


def project_memory_body(body, meta):
    """Replace stale mutable body fields with the latest metadata truth."""
    entry = _normalise_meta_entry(meta)
    facts = [
        f"**创建轮次**：{_round_ref(entry.get('created_round'), entry.get('created_instance_id'))}",
        f"**创建时间**：{normalize_iso_timestamp(entry.get('created_at')) or '未记录'}",
        f"**入库时间**：{normalize_iso_timestamp(entry.get('stored_at')) or '未入库'}",
        f"**最近调用轮次**：{_round_ref(entry.get('last_recalled_round'), entry.get('last_recalled_instance_id'))}",
        f"**最近调用时间**：{normalize_iso_timestamp(entry.get('last_recalled_at')) or '未记录'}",
        f"**挂接备注**：{str(entry.get('current_overview') or '').strip() or '无'}",
        "**挂接备注更新时间**："
        f"{normalize_iso_timestamp(entry.get('current_overview_updated_at')) or '未记录'}",
        "**关联容器**：" + ", ".join(
            str(item).strip() for item in entry.get("linked_containers") or []
            if str(item).strip()
        ) or "**关联容器**：无",
    ]
    header_prefixes = (
        "**入库**：", "**最后调用**：", "现状概况：", "创建时间：", "入库时间：", "关联容器：",
        "**入库轮次**：", "**创建轮次**：", "**创建时间**：", "**入库时间**：", "**最近调用轮次**：",
        "**最近调用时间**：", "**挂接备注**：", "**挂接备注更新时间**：",
        "**关联容器**：", "注释：", "**注释**：",
    )
    lines = str(body or "").splitlines()
    heading = next((
        index for index, line in enumerate(lines)
        if line.lstrip().startswith("## MEM-")
    ), None)
    if heading is None:
        return "\n".join(facts + lines).strip()

    content_prefixes = ("**内容**", "**摘要**", "**正文**", "**梗概**")
    content_start = next((
        index for index in range(heading + 1, len(lines))
        if lines[index].strip().startswith(content_prefixes)
    ), len(lines))
    drop = {
        index for index in range(heading + 1, content_start)
        if lines[index].strip().startswith(header_prefixes)
    }
    if content_start < len(lines):
        for prefix in ("创建时间：", "入库时间：", "关联容器："):
            match = next((
                index for index in range(len(lines) - 1, content_start, -1)
                if lines[index].strip().startswith(prefix)
            ), None)
            if match is not None:
                drop.add(match)
    lines = [line for index, line in enumerate(lines) if index not in drop]
    heading = next(
        index for index, line in enumerate(lines)
        if line.lstrip().startswith("## MEM-")
    )
    lines[heading + 1:heading + 1] = facts
    return "\n".join(lines).strip()


def project_periodic_memory_body(body, meta):
    """Render the stable semantic projection used by ``20_periodic``.

    Periodic mounts deliberately omit call coordinates, local attachment notes,
    and container links. The live Pinned body and static title remain the truth,
    so a body/title edit is visible the next time the layer is assembled without
    turning routine recall activity into cache churn.
    """
    projected = project_memory_body(body, meta)
    volatile_prefixes = (
        "**入库轮次**：", "**创建轮次**：", "**创建时间**：", "**入库时间**：", "**最近调用轮次**：",
        "**最近调用时间**：", "**挂接备注**：", "**挂接备注更新时间**：",
        "**关联容器**：",
    )
    lines = [
        line for line in projected.splitlines()
        if not line.strip().startswith(volatile_prefixes)
    ]
    title = str((meta or {}).get("title") or "").strip()
    lines = [
        line for line in lines
        if not line.strip().startswith("**标题**")
    ]
    heading = next((
        index for index, line in enumerate(lines)
        if line.lstrip().startswith("## MEM-")
    ), None)
    if title:
        if heading is None:
            lines.insert(0, f"**标题**：{title}")
        else:
            lines.insert(heading + 1, f"**标题**：{title}")
    return "\n".join(lines).strip()


def _body_limit_for_weight(weight):
    if weight >= 5:
        return 2048
    if weight >= 3:
        return 512
    return 128


_SEMANTIC_FIELD_RE = re.compile(
    r"^(?:\*\*)?(内容|摘要|正文|梗概)(?:\*\*)?[^：:]*[：:]"
)
_TITLE_FIELD_RE = re.compile(r"^(?:\*\*)?标题(?:\*\*)?[：:]")
_STRUCTURAL_FIELD_RE = re.compile(
    r"^(?:\*\*)?(?:"
    r"交互对象|入库|最后调用|梦源|现状概况|创建时间|入库时间|标签|感受词|"
    r"关联容器|注释|入库轮次|创建轮次|最近调用轮次|最近调用时间|挂接备注|"
    r"挂接备注更新时间|权重|访问|公开性"
    r")(?:\*\*)?[：:]"
)


def extract_memory_semantic(text):
    """Return the semantic payload from a structured or legacy memory body."""
    body = str(text or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    for line in body.splitlines():
        stripped = line.strip()
        if _SEMANTIC_FIELD_RE.match(stripped):
            semantic = re.split(r"[：:]", stripped, maxsplit=1)[1].strip()
            if semantic:
                return semantic
    return "\n".join(
        line for line in body.splitlines()
        if not line.lstrip().startswith("## MEM-")
        and not _TITLE_FIELD_RE.match(line.strip())
        and not _STRUCTURAL_FIELD_RE.match(line.strip())
    ).strip()


def replace_memory_semantic_payload(body, title, semantic_body, weight, *, tier=None):
    """Return one normalized block with exactly one semantic field.

    Existing structured provenance is retained, while every old semantic
    field and its free-text continuation is removed.  This avoids the former
    ``梗概 + 召回补全内容`` double body.
    """
    text = str(body or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    semantic = str(semantic_body or "").replace(
        "\r\n", "\n").replace("\r", "\n").strip()
    clean_title = str(title or "").strip()
    if not text or not semantic or not clean_title:
        raise ValueError("missing_title_or_body")

    lines = text.splitlines()
    heading = next((line for line in lines if line.lstrip().startswith("## MEM-")), "")
    if not heading:
        raise ValueError("memory_heading_missing")

    semantic_label = ""
    structural = []
    in_semantic = False
    title_insert_at = None
    for line in lines[lines.index(heading) + 1:]:
        stripped = line.strip()
        match = _SEMANTIC_FIELD_RE.match(stripped)
        if match:
            semantic_label = semantic_label or match.group(1)
            in_semantic = True
            continue
        if _TITLE_FIELD_RE.match(stripped):
            if title_insert_at is None:
                title_insert_at = len(structural)
            in_semantic = False
            continue
        if _STRUCTURAL_FIELD_RE.match(stripped):
            in_semantic = False
            structural.append(line)
            continue
        if in_semantic or stripped:
            # Unlabelled text belongs to the old semantic payload.  Blank
            # separators are reconstructed below.
            continue

    if tier is not None:
        semantic_label = {
            "Full": "内容", "Summary": "摘要", "Abstract": "梗概",
        }.get(tier, "")
        if not semantic_label:
            raise ValueError("invalid_semantic_tier")
    elif not semantic_label:
        semantic_label = "内容" if int(weight or 0) >= 5 else (
            "摘要" if int(weight or 0) >= 3 else "梗概"
        )
    limit = {"Full": 2048, "Summary": 512, "Abstract": 128}.get(
        tier, _body_limit_for_weight(int(weight or 0)))
    if len(semantic) > limit:
        raise ValueError(
            f"memory_body_too_long:max={limit};actual={len(semantic)}")
    title_line = f"**标题**：{clean_title}"
    semantic_line = f"**{semantic_label}**（≤{limit}字）：{semantic}"
    insert_at = title_insert_at if title_insert_at is not None else 0
    structural.insert(insert_at, title_line)
    structural.insert(insert_at + 1, semantic_line)
    return "\n".join([heading, *structural]).strip()


class MemoryStore:
    """记忆条目正文 + 元数据 + 索引 的读写管理"""

    def __init__(self):
        pass

    def _active_read_layers(self):
        """STM + LTM active layers visible to memory_content_read."""
        return [
            ("STM", META_JSON, MEMORY_MD),
            ("LTM/Full", LTM_FULL_META_JSON, LTM_FULL_FULL_MD),
            ("LTM/Summary", LTM_SUMMARY_META_JSON, LTM_SUMMARY_SUMMARY_MD),
            ("LTM/Abstract", LTM_ABSTRACT_META_JSON, LTM_ABSTRACT_ABSTRACT_MD),
            ("LTM/Pinned", LTM_PINNED_META_JSON, LTM_PINNED_PINNED_MD),
        ]

    def _active_ltm_read_layers(self):
        """Active public LTM tiers in canonical read priority."""
        return self._active_read_layers()[1:]

    @staticmethod
    def _ltm_tier_paths(tier):
        names = {
            "Full": ("LTM_FULL_FULL_MD", "LTM_FULL_META_JSON", "LTM_FULL_INDEX_MD", "F", 8),
            "Summary": ("LTM_SUMMARY_SUMMARY_MD", "LTM_SUMMARY_META_JSON", "LTM_SUMMARY_INDEX_MD", "S", 6),
            "Abstract": ("LTM_ABSTRACT_ABSTRACT_MD", "LTM_ABSTRACT_META_JSON", "LTM_ABSTRACT_INDEX_MD", "A", 4),
            "Pinned": ("LTM_PINNED_PINNED_MD", "LTM_PINNED_META_JSON", "LTM_PINNED_INDEX_MD", "P", 8),
            "Backup": ("LTM_BACKUP_BACKUP_MD", "LTM_BACKUP_META_JSON", "LTM_BACKUP_INDEX_MD", "B", 0),
        }
        if tier not in names:
            raise ValueError("invalid_ltm_tier")
        body, meta, index, code, tag_limit = names[tier]
        return {
            "body": _live_path(body) if body in _IMPORTED_LTM_PATHS else getattr(runtime_paths, body),
            "meta": _live_path(meta) if meta in _IMPORTED_LTM_PATHS else getattr(runtime_paths, meta),
            "index": getattr(runtime_paths, index),
            "code": code,
            "tag_limit": tag_limit,
        }

    @staticmethod
    def _ltm_blocks(path):
        if not os.path.isfile(path):
            return {}, ""
        try:
            with open(path, "r", encoding="utf-8") as handle:
                text = handle.read()
        except OSError as exc:
            raise ReadError(path, cause=exc)
        matches = list(re.finditer(
            r"(?ms)^##\s+(MEM-[0-9A-F]{8})\b.*?(?=^##\s+MEM-[0-9A-F]{8}\b|\Z)",
            text,
        ))
        blocks = {}
        for match in matches:
            mem_id = match.group(1)
            if mem_id in blocks:
                raise ValueError(f"ltm_body_duplicate:{mem_id}")
            blocks[mem_id] = match.group(0).strip()
        return blocks, text

    @staticmethod
    def _ltm_meta_parts(path):
        if not os.path.isfile(path):
            return {}, {}
        try:
            with open(path, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ReadError(path, cause=exc)
        if not isinstance(data, dict):
            raise ValueError(f"ltm_meta_invalid:{path}")
        headers = {}
        entries = {}
        for mem_id, entry in data.items():
            if mem_id == "_comment":
                if not isinstance(entry, str):
                    raise ValueError(f"ltm_meta_invalid:{path}")
                headers[mem_id] = entry
                continue
            if not re.fullmatch(r"MEM-[0-9A-F]{8}", str(mem_id)) or not isinstance(entry, dict):
                raise ValueError(f"ltm_meta_invalid:{path}")
            if entry.get("id") not in (None, mem_id):
                raise ValueError(f"ltm_meta_id_mismatch:{mem_id}")
            entries[mem_id] = entry
        return headers, entries

    @classmethod
    def _ltm_meta(cls, path):
        return cls._ltm_meta_parts(path)[1]

    @staticmethod
    def _compose_ltm_meta_document(headers, entries):
        document = dict(headers)
        document.update(shared_memory_meta_document(entries))
        return document

    @staticmethod
    def _index_cell(value):
        return str(value or "—").replace("|", "\\|").replace("\r", " ").replace("\n", " ")

    @classmethod
    def _render_ltm_index(cls, tier, entries):
        code = cls._ltm_tier_paths(tier)["code"]
        lines = [
            f"<!-- {tier} 记忆索引 -->",
            "| 编号 | 类型 | 权重 | 标题 | 梦源 | 交互对象 | 创建轮/最后调用轮 | 现状概况 |",
            "|------|------|------|------|------|---------|-------------------|----------|",
        ]
        for mem_id, entry in sorted(entries.items()):
            created_round = entry.get("created_round")
            last_round = entry.get("last_recalled_round")
            created_text = (
                f"第{created_round}轮" if isinstance(created_round, int) else "—")
            last_text = f"第{last_round}轮" if isinstance(last_round, int) else "—"
            lines.append(
                f"| {mem_id} | [{code}] | {int(entry.get('weight', 1) or 1)} | "
                f"{cls._index_cell(entry.get('title') or mem_id)} | "
                f"{'是' if entry.get('dream') else '否'} | "
                f"{cls._index_cell(entry.get('subject'))} | "
                f"{created_text} / {last_text} | "
                f"{cls._index_cell(entry.get('current_overview'))} |"
            )
        return "\n".join(lines) + "\n"

    def _stm_source_state(self, mem_id):
        meta = self._read_json_file(META_JSON, merge_overlay=False)
        entry = meta.get(mem_id) if isinstance(meta.get(mem_id), dict) else None
        try:
            body = self._read_entry_from_file(MEMORY_MD, mem_id)
        except EntryNotFoundError:
            body = None
        heat_entry = None
        if os.path.isfile(runtime_paths.HEAT_JSON):
            try:
                with open(runtime_paths.HEAT_JSON, "r", encoding="utf-8") as handle:
                    heat_doc = json.load(handle)
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise ReadError(runtime_paths.HEAT_JSON, cause=exc)
            if not isinstance(heat_doc, dict) or not isinstance(
                    heat_doc.get("entries", {}), dict):
                raise ReadError(runtime_paths.HEAT_JSON)
            heat_entry = heat_doc.get("entries", {}).get(mem_id)
        return {
            "body": body,
            "meta": entry,
            "heat": dict(heat_entry) if isinstance(heat_entry, dict) else None,
            "degrade": bool(
                isinstance(heat_entry, dict) and heat_entry.get("degrade") is True
            ),
        }

    def _validate_stm_residences(self, truth):
        """Reject unprovable current-branch STM state before READY repairs."""
        body_ids = set(self._ltm_blocks(MEMORY_MD)[0])
        raw_meta = self._read_json_file(META_JSON, merge_overlay=False)
        meta_entries = {}
        for mem_id, entry in raw_meta.items():
            if mem_id == "_comment":
                continue
            if not re.fullmatch(r"MEM-[0-9A-F]{8}", str(mem_id)) or not isinstance(
                    entry, dict):
                raise ValueError(f"stm_meta_invalid:{mem_id}")
            if entry.get("id") not in (None, mem_id):
                raise ValueError(f"stm_meta_id_mismatch:{mem_id}")
            meta_entries[mem_id] = entry

        heat_entries = {}
        if os.path.isfile(runtime_paths.HEAT_JSON):
            try:
                with open(runtime_paths.HEAT_JSON, "r", encoding="utf-8") as handle:
                    heat_doc = json.load(handle)
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise ReadError(runtime_paths.HEAT_JSON, cause=exc)
            if not isinstance(heat_doc, dict) or not isinstance(
                    heat_doc.get("entries", {}), dict):
                raise ReadError(runtime_paths.HEAT_JSON)
            for mem_id, entry in heat_doc.get("entries", {}).items():
                if not re.fullmatch(r"MEM-[0-9A-F]{8}", str(mem_id)) or not isinstance(
                        entry, dict):
                    raise ValueError(f"stm_heat_invalid:{mem_id}")
                heat_entries[mem_id] = entry

        active_ltm_ids = {
            mem_id
            for tier in ("Full", "Summary", "Abstract", "Pinned")
            for mem_id in truth[tier]["meta"]
        }
        backup_ids = set(truth["Backup"]["meta"])
        for mem_id in sorted(body_ids | set(meta_entries) | set(heat_entries)):
            meta = meta_entries.get(mem_id)
            if mem_id in backup_ids:
                raise ValueError(f"stm_backup_conflict:{mem_id}")
            if isinstance(meta, dict) and str(
                    meta.get("access") or "public").strip().lower() == "private":
                if mem_id in active_ltm_ids:
                    raise ValueError(f"stm_ltm_access_conflict:{mem_id}")
                if mem_id in body_ids:
                    raise ValueError(f"private_public_body_conflict:{mem_id}")
                continue

            present = (
                mem_id in body_ids,
                mem_id in meta_entries,
                mem_id in heat_entries,
            )
            if mem_id in active_ltm_ids:
                if all(present) or not any(present):
                    continue
                raise ValueError(f"stm_ltm_residence_incomplete:{mem_id}")
            if not all(present):
                raise ValueError(f"stm_residence_incomplete:{mem_id}")
            raise ValueError(f"stm_without_ltm_canonical_truth:{mem_id}")

    def stm_entry_state(self, mem_id):
        """Return exact STM residence state without falling through to LTM."""
        return self._stm_source_state(mem_id)

    @staticmethod
    def _transition_meta_matches(source, destination, *, strict=True):
        if not isinstance(source, dict) or not isinstance(destination, dict):
            return True
        keys = [
            "id", "weight", "access", "created_at", "created_round",
            "created_instance_id", "stored_at",
        ]
        if strict:
            keys.extend(("title", "subject"))
        for key in keys:
            left = source.get(key)
            right = destination.get(key)
            if left not in (None, "") and right not in (None, "") and left != right:
                return False
        return True

    def _repair_interrupted_ltm_writes(self):
        """Repair valid dual residence from verified canonical LTM truth."""
        truth = self._ltm_truth()
        self._validate_stm_residences(truth)
        stm_blocks = self._ltm_blocks(MEMORY_MD)[0]
        stm_meta = self._read_json_file(META_JSON, merge_overlay=False)
        heat_doc = {"entries": {}}
        if os.path.isfile(runtime_paths.HEAT_JSON):
            try:
                with open(runtime_paths.HEAT_JSON, "r", encoding="utf-8") as handle:
                    heat_doc = json.load(handle)
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise ReadError(runtime_paths.HEAT_JSON, cause=exc)
        heat_entries = heat_doc.get("entries") if isinstance(heat_doc, dict) else None
        if not isinstance(heat_entries, dict):
            raise ReadError(runtime_paths.HEAT_JSON)
        overlay_doc = _read_memory_overlay()
        overlay_entries = overlay_doc["entries"]
        overlay_changed = False
        updated = []
        has_live_dual = False
        for tier in ("Full", "Summary", "Abstract", "Pinned"):
            item = truth[tier]
            for mem_id in item["meta"]:
                raw_heat = heat_entries.get(mem_id)
                raw_meta = stm_meta.get(mem_id)
                stm = {
                    "body": stm_blocks.get(mem_id),
                    "meta": raw_meta if isinstance(raw_meta, dict) else None,
                    "heat": dict(raw_heat) if isinstance(raw_heat, dict) else None,
                    "degrade": bool(
                        isinstance(raw_heat, dict)
                        and raw_heat.get("degrade") is True
                    ),
                }
                if stm["body"] is None and stm["meta"] is None:
                    if stm.get("heat") is not None:
                        updated.extend(self.remove_stm_copy(mem_id))
                    continue
                complete_stm = (
                    stm["body"] is not None
                    and stm["meta"] is not None
                    and stm.get("heat") is not None
                )
                if not complete_stm:
                    raise ValueError(f"stm_ltm_residence_incomplete:{mem_id}")
                if str(
                        stm["meta"].get("access") or "public"
                ).strip().lower() != "public":
                    raise ValueError(f"stm_ltm_access_conflict:{mem_id}")
                desired_overlay = {
                    key: stm["meta"].get(
                        key, [] if key == "linked_containers" else "")
                    for key in MEMORY_OVERLAY_FIELDS
                }
                if overlay_entries.get(mem_id) != desired_overlay:
                    overlay_entries[mem_id] = desired_overlay
                    overlay_changed = True
                if stm["degrade"] and memory_is_admitted(item["meta"][mem_id]):
                    updated.extend(self.remove_stm_copy(mem_id))
                    continue

                has_live_dual = True
                desired_meta = dict(item["meta"][mem_id])
                desired_meta.update({
                    key: desired_overlay.get(
                        key, [] if key == "linked_containers" else ""
                    )
                    for key in MEMORY_OVERLAY_FIELDS
                })
                desired_meta = _normalise_meta_entry(desired_meta)
                desired_body = item["blocks"][mem_id]
                if stm["body"] != desired_body:
                    self.replace_stm_body(mem_id, desired_body)
                    updated.append(MEMORY_MD)
                if _normalise_meta_entry(stm["meta"]) != desired_meta:
                    self.replace_stm_meta(
                        mem_id, desired_meta, canonical_sync=True)
                    updated.append(META_JSON)

        if overlay_changed:
            atomic_write_json(LTM_MEMORY_LINKS_JSON, overlay_doc)
            updated.append(LTM_MEMORY_LINKS_JSON)

        if has_live_dual:
            stm_meta = self._read_json_file(META_JSON, merge_overlay=False)
            expected_index = self._render_stm_index(stm_meta)
            current_index = ""
            if os.path.isfile(INDEX_MD):
                try:
                    with open(INDEX_MD, "r", encoding="utf-8") as handle:
                        current_index = handle.read()
                except OSError as exc:
                    raise ReadError(INDEX_MD, cause=exc)
            if current_index != expected_index:
                atomic_write_text(INDEX_MD, expected_index)
                updated.append(INDEX_MD)

            expected_keywords = self._stm_keyword_document(stm_meta)
            current_keywords = None
            if os.path.isfile(runtime_paths.KEYWORDS_JSON):
                try:
                    with open(
                            runtime_paths.KEYWORDS_JSON, "r",
                            encoding="utf-8") as handle:
                        current_keywords = json.load(handle)
                except (OSError, UnicodeError, json.JSONDecodeError):
                    current_keywords = None
            if current_keywords != expected_keywords:
                atomic_write_json(runtime_paths.KEYWORDS_JSON, expected_keywords)
                updated.append(runtime_paths.KEYWORDS_JSON)
        return list(dict.fromkeys(updated))

    def _migrate_admission_lifecycle(self):
        """Migrate known Spec745 memory state in one LTM+STM rollback boundary."""
        ltm_snapshot = self.snapshot_ltm_files()
        stm_snapshot = self.snapshot_stm_files()
        updated = []
        try:
            from data.memory_compression_store import MemoryCompressionStore

            compression_pending_ids = {
                entry["mem_id"]
                for entry in MemoryCompressionStore().load()["entries"]
            }
            # Shared LTM existed before stored_at; every such legacy entry was
            # formally admitted under the old stored=true contract.
            for tier in ("Full", "Summary", "Abstract", "Pinned", "Backup"):
                paths = self._ltm_tier_paths(tier)
                headers, entries = self._ltm_meta_parts(paths["meta"])
                changed = False
                for mem_id, raw in entries.items():
                    unknown = set(raw) - set(META_ENTRY_FIELDS) - set(
                        MEMORY_OVERLAY_FIELDS)
                    if unknown:
                        raise ValueError(
                            f"unknown_memory_meta_fields:{mem_id}:"
                            f"{','.join(sorted(unknown))}")
                    created_at = str(raw.get("created_at") or "").strip()
                    if not normalize_iso_timestamp(created_at):
                        raise ValueError(f"invalid_memory_created_at:{mem_id}")
                    if "stored_at" not in raw:
                        raw["stored_at"] = created_at
                        changed = True
                    else:
                        stored_at = str(raw.get("stored_at") or "").strip()
                        if stored_at and not normalize_iso_timestamp(stored_at):
                            raise ValueError(f"invalid_memory_stored_at:{mem_id}")
                if changed:
                    atomic_write_json(
                        paths["meta"],
                        self._compose_ltm_meta_document(headers, entries),
                    )
                    updated.append(paths["meta"])

            truth = self._ltm_truth()
            ltm_by_id = {
                mem_id: (tier, item["blocks"][mem_id], dict(meta))
                for tier, item in truth.items()
                for mem_id, meta in item["meta"].items()
            }
            raw_meta = self._read_json_file(META_JSON, merge_overlay=False)
            stm_meta = {
                mem_id: dict(value) for mem_id, value in raw_meta.items()
                if isinstance(value, dict) and re.fullmatch(
                    r"MEM-[0-9A-F]{8}", str(mem_id))
            }
            stm_blocks = self._ltm_blocks(MEMORY_MD)[0]
            body_ids = set(stm_blocks)
            heat_doc = {"entries": {}}
            if os.path.isfile(runtime_paths.HEAT_JSON):
                try:
                    with open(runtime_paths.HEAT_JSON, "r", encoding="utf-8") as handle:
                        heat_doc = json.load(handle)
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise ReadError(runtime_paths.HEAT_JSON, cause=exc)
            heat_entries = heat_doc.get("entries") if isinstance(heat_doc, dict) else None
            if not isinstance(heat_entries, dict):
                raise ReadError(runtime_paths.HEAT_JSON)
            ltm_creates = {tier: [] for tier in ("Full", "Summary", "Abstract")}
            overlay_doc = _read_memory_overlay()
            overlay_changed = False

            for mem_id in sorted(body_ids | set(stm_meta) | set(heat_entries)):
                parts = (
                    mem_id in body_ids, mem_id in stm_meta,
                    isinstance(heat_entries.get(mem_id), dict),
                )
                if not all(parts):
                    legacy_heat = heat_entries.get(mem_id)
                    ltm = ltm_by_id.get(mem_id)
                    if (
                        ltm is not None
                        and isinstance(legacy_heat, dict)
                        and legacy_heat.get("stored") is True
                    ):
                        if mem_id in stm_meta:
                            source = stm_meta[mem_id]
                            current = overlay_doc["entries"].get(mem_id)
                            current = current if isinstance(current, dict) else {}
                            overlay_doc["entries"][mem_id] = {
                                key: source.get(
                                    key,
                                    current.get(
                                        key,
                                        [] if key == "linked_containers" else "",
                                    ),
                                )
                                for key in MEMORY_OVERLAY_FIELDS
                            }
                            overlay_changed = True
                        updated.extend(self.remove_stm_copy(mem_id))
                        stm_meta.pop(mem_id, None)
                        heat_entries.pop(mem_id, None)
                        continue
                    raise ValueError(f"stm_residence_incomplete:{mem_id}")
                meta = stm_meta[mem_id]
                unknown = set(meta) - set(META_ENTRY_FIELDS)
                if unknown:
                    raise ValueError(
                        f"unknown_memory_meta_fields:{mem_id}:"
                        f"{','.join(sorted(unknown))}")
                created_at = str(meta.get("created_at") or "").strip()
                if not normalize_iso_timestamp(created_at):
                    raise ValueError(f"invalid_memory_created_at:{mem_id}")
                legacy_stored = heat_entries[mem_id].get("stored", None)
                if legacy_stored is not None and not isinstance(legacy_stored, bool):
                    raise ValueError(f"invalid_legacy_stored:{mem_id}")
                ltm = ltm_by_id.get(mem_id)
                if ltm is None:
                    if legacy_stored is not False:
                        raise ValueError(f"stm_without_ltm_canonical_truth:{mem_id}")
                    meta["stored_at"] = ""
                    body = stm_blocks[mem_id]
                    tier = memory_target_tier(meta.get("weight"))
                    shared_meta = shared_memory_meta_entry(meta)
                    ltm_creates[tier].append((mem_id, body, shared_meta))
                    overlay_doc["entries"][mem_id] = {
                        key: meta.get(
                            key, [] if key == "linked_containers" else "")
                        for key in MEMORY_OVERLAY_FIELDS
                    }
                    overlay_changed = True
                    ltm = (tier, body, shared_meta)
                    ltm_by_id[mem_id] = ltm
                else:
                    if legacy_stored is False:
                        raise ValueError(f"stm_ltm_transition_uncommitted:{mem_id}")
                    meta["stored_at"] = str(ltm[2].get("stored_at") or "")
                if "stored" in heat_entries[mem_id]:
                    del heat_entries[mem_id]["stored"]
                    updated.append(runtime_paths.HEAT_JSON)

            for tier, creates in ltm_creates.items():
                if not creates:
                    continue
                paths = self._ltm_tier_paths(tier)
                blocks, existing_text = self._ltm_blocks(paths["body"])
                headers, entries = self._ltm_meta_parts(paths["meta"])
                additions = []
                for mem_id, body, meta in creates:
                    if mem_id in blocks or mem_id in entries:
                        raise ValueError(f"ltm_migration_target_conflict:{mem_id}")
                    semantic = extract_memory_semantic(body)
                    if not semantic:
                        raise ValueError(f"memory_semantic_content_missing:{mem_id}")
                    normalized_body = replace_memory_semantic_payload(
                        str(body), meta.get("title") or mem_id,
                        semantic, int(meta["weight"]), tier=tier,
                    )
                    clean_body = re.sub(
                        r"(?s)^##\s+MEM-[0-9A-F]{8}\b[^\n]*\n?", "",
                        normalized_body, count=1,
                    ).strip()
                    if not clean_body:
                        raise ValueError(f"empty_ltm_body:{mem_id}")
                    code = self._ltm_tier_paths(tier)["code"]
                    meta = dict(meta)
                    meta["type"] = code
                    entries[mem_id] = meta
                    additions.append(
                        f"## {mem_id}  [{code}]  权重{int(meta['weight'])}\n"
                        f"{clean_body}"
                    )
                combined = "\n\n".join(
                    part for part in (existing_text.strip(), *additions) if part)
                atomic_write_text(paths["body"], combined + ("\n" if combined else ""))
                atomic_write_json(
                    paths["meta"], self._compose_ltm_meta_document(headers, entries))
                updated.extend((paths["body"], paths["meta"]))
            if overlay_changed:
                atomic_write_json(LTM_MEMORY_LINKS_JSON, overlay_doc)
                updated.append(LTM_MEMORY_LINKS_JSON)

            # A blank-admission LTM created by this same branch without its
            # STM mate is the only provable create-tail; other branches remain
            # legitimately LTM-only until recalled.
            from data.memory_heat import MemoryHeat
            for mem_id, (tier, body, meta) in sorted(ltm_by_id.items()):
                if (
                    tier == "Backup"
                    or memory_is_admitted(meta)
                    or mem_id in stm_meta
                    or mem_id in compression_pending_ids
                    or str(meta.get("created_instance_id") or "")
                    != str(runtime_paths.ACTIVE_INSTANCE_ID or "meta")
                ):
                    continue
                branch_meta = _normalise_meta_entry(meta)
                overlay = _read_memory_overlay()["entries"].get(mem_id, {})
                branch_meta.update({
                    key: overlay.get(key, [] if key == "linked_containers" else "")
                    for key in MEMORY_OVERLAY_FIELDS
                })
                self.replace_stm_body(mem_id, body)
                self.replace_stm_meta(mem_id, branch_meta)
                heat_entries[mem_id] = MemoryHeat().new_entry(
                    weight=int(meta.get("weight")))
                stm_meta[mem_id] = branch_meta
                updated.extend((MEMORY_MD, META_JSON, runtime_paths.HEAT_JSON))

            if stm_meta:
                document = {
                    key: value for key, value in raw_meta.items()
                    if key == "_comment"
                }
                document.update(stm_meta)
                atomic_write_json(META_JSON, document)
            atomic_write_json(runtime_paths.HEAT_JSON, heat_doc)
            self._validate_stm_residences(self._ltm_truth())
        except Exception as exc:
            try:
                self.restore_stm_files(stm_snapshot)
                self.restore_ltm_files(ltm_snapshot)
            except Exception as restore_exc:
                raise RuntimeError(
                    "memory_admission_migration_rollback_failed:"
                    f"{type(restore_exc).__name__}"
                ) from exc
            raise
        return list(dict.fromkeys(updated))

    def _ltm_truth(self, ignore_source=None):
        truth = {}
        locations = {}
        for tier in ("Full", "Summary", "Abstract", "Pinned", "Backup"):
            paths = self._ltm_tier_paths(tier)
            blocks, _text = self._ltm_blocks(paths["body"])
            meta = self._ltm_meta(paths["meta"])
            public = {
                mem_id: entry for mem_id, entry in meta.items()
                if str(entry.get("access") or "public").strip().lower() == "public"
            }
            private_ids = set(meta) - set(public)
            if private_ids & set(blocks):
                raise ValueError(f"private_ltm_in_public_body:{sorted(private_ids & set(blocks))[0]}")
            if set(blocks) != set(public):
                missing_body = sorted(set(public) - set(blocks))
                missing_meta = sorted(set(blocks) - set(public))
                detail = (missing_body or missing_meta or [tier])[0]
                raise ValueError(f"ltm_body_meta_conflict:{tier}:{detail}")
            if ignore_source and ignore_source[0] == tier:
                ignored_id = ignore_source[1]
                blocks = {key: value for key, value in blocks.items() if key != ignored_id}
                public = {key: value for key, value in public.items() if key != ignored_id}
            for mem_id in public:
                if mem_id in locations:
                    reason = (
                        "ltm_tier_conflict" if "Backup" in {locations[mem_id], tier}
                        else "ltm_active_tier_conflict"
                    )
                    raise ValueError(
                        f"{reason}:{mem_id}:{locations[mem_id]}:{tier}")
                locations[mem_id] = tier
            truth[tier] = {"paths": paths, "blocks": blocks, "meta": public}
        return truth

    def reconcile_ltm_projections(self, *, repair=True, ignore_source=None):
        """Rebuild derived LTM indexes from canonical public body/meta truth."""
        recovered = []
        if repair:
            with MEMORY_MUTATION_LOCK:
                from data.memory_compression_store import MemoryCompressionManager

                compression_recovery = MemoryCompressionManager(
                    memory_store=self).reconcile_ready()
                if compression_recovery.get("status") == "repaired":
                    recovered.append(
                        runtime_paths.MEMORY_COMPRESSION_PENDING_JSON)
                recovered.extend(self._migrate_admission_lifecycle())
                degraded_recovery = MemoryCompressionManager(
                    memory_store=self).settle_ready_degraded_stm()
                if degraded_recovery.get("status") == "repaired":
                    recovered.extend((
                        runtime_paths.MEMORY_COMPRESSION_PENDING_JSON,
                        MEMORY_MD,
                        META_JSON,
                        runtime_paths.HEAT_JSON,
                    ))
                recovered.extend(self._repair_interrupted_ltm_writes())
        truth = self._ltm_truth(ignore_source=ignore_source)
        updated = list(recovered)
        keyword_index = {}
        for tier, item in truth.items():
            index_text = self._render_ltm_index(tier, item["meta"])
            current = ""
            if os.path.isfile(item["paths"]["index"]):
                try:
                    with open(item["paths"]["index"], "r", encoding="utf-8") as handle:
                        current = handle.read()
                except OSError as exc:
                    raise ReadError(item["paths"]["index"], cause=exc)
            if current != index_text:
                atomic_write_text(item["paths"]["index"], index_text)
                updated.append(item["paths"]["index"])
            if tier == "Backup":
                continue
            for mem_id, entry in item["meta"].items():
                tags = []
                for value in entry.get("tags") or []:
                    tag = str(value or "").strip()
                    if tag and tag not in tags:
                        tags.append(tag)
                for tag in tags:
                    keyword_index.setdefault(tag, []).append(
                        f"{mem_id}[{item['paths']['code']}]"
                    )
        keyword_doc = {
            "_comment": "LTM 倒排索引（活跃公共记忆标签→层级条目）",
            "index": {
                keyword: sorted(values)
                for keyword, values in sorted(keyword_index.items())
            },
        }
        keyword_path = runtime_paths.LTM_KEYWORDS_JSON
        current_keywords = None
        if os.path.isfile(keyword_path):
            try:
                with open(keyword_path, "r", encoding="utf-8") as handle:
                    current_keywords = json.load(handle)
            except (OSError, UnicodeError, json.JSONDecodeError):
                current_keywords = None
        if current_keywords != keyword_doc:
            atomic_write_json(keyword_path, keyword_doc)
            updated.append(keyword_path)
        for tier, item in truth.items():
            expected = self._render_ltm_index(tier, item["meta"])
            try:
                with open(item["paths"]["index"], "r", encoding="utf-8") as handle:
                    actual = handle.read()
            except OSError as exc:
                raise ReadError(item["paths"]["index"], cause=exc)
            if actual != expected:
                raise ValueError(f"ltm_index_unverified:{tier}")
        try:
            with open(keyword_path, "r", encoding="utf-8") as handle:
                actual_keywords = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ReadError(keyword_path, cause=exc)
        if actual_keywords != keyword_doc:
            raise ValueError("ltm_keywords_unverified")
        return {
            "status": "repaired" if updated else "ok",
            "updated": updated,
            "active_entries": sum(
                len(truth[tier]["meta"])
                for tier in ("Full", "Summary", "Abstract", "Pinned")
            ),
        }

    def store_ltm_entry(
            self, tier, mem_id, body, meta, *, source_tier=None,
            admission_weight_drop=False):
        """Idempotently persist one LTM entry and its two derived indexes."""
        paths = self._ltm_tier_paths(tier)
        clean_body = re.sub(
            r"(?s)^##\s+MEM-[0-9A-F]{8}\b[^\n]*\n?", "", str(body or "").strip(), count=1
        ).strip()
        if not clean_body:
            raise ValueError("empty_ltm_body")
        entry = shared_memory_meta_entry(dict(meta or {}))
        entry["id"] = mem_id
        entry["type"] = paths["code"] if tier != "Backup" else str(entry.get("type") or "A")
        memory_target_tier(entry.get("weight"))
        tags = []
        for value in entry.get("tags") or []:
            tag = str(value or "").strip()
            if tag and tag not in tags:
                tags.append(tag)
        if paths["tag_limit"]:
            entry["tags"] = tags[:paths["tag_limit"]]

        blocks, existing_text = self._ltm_blocks(paths["body"])
        meta_headers, meta_doc = self._ltm_meta_parts(paths["meta"])
        if mem_id in meta_doc and meta_doc[mem_id] != entry:
            raise ValueError(f"ltm_meta_conflict:{mem_id}")
        transition = bool(source_tier and source_tier != tier)
        if transition:
            source_paths = self._ltm_tier_paths(source_tier)
            source_blocks, _source_text = self._ltm_blocks(source_paths["body"])
            source_meta = self._ltm_meta(source_paths["meta"])
            source_body_exists = mem_id in source_blocks
            source_meta_exists = mem_id in source_meta
            if source_body_exists != source_meta_exists:
                raise ValueError(
                    f"ltm_source_incomplete:{source_tier}:{mem_id}")
            if not source_body_exists:
                target_body_exists = mem_id in blocks
                target_meta_matches = meta_doc.get(mem_id) == entry
                if not (target_body_exists and target_meta_matches):
                    raise ValueError(
                        f"ltm_source_missing:{source_tier}:{mem_id}")
                transition = False
            else:
                source_entry = source_meta[mem_id]
                source_weight = int(source_entry.get("weight"))
                target_weight = int(entry.get("weight"))
                if source_weight != target_weight:
                    expected_tier, expected_weight = memory_stm_forgetting_target(
                        source_weight)
                    valid_admission_drop = (
                        admission_weight_drop
                        and not memory_is_admitted(source_entry)
                        and memory_is_admitted(entry)
                        and tier == expected_tier
                        and target_weight == expected_weight
                    )
                    if not valid_admission_drop:
                        raise ValueError(f"memory_weight_immutable:{mem_id}")
        if mem_id in blocks:
            old_body = re.sub(
                r"(?s)^##\s+MEM-[0-9A-F]{8}\b[^\n]*\n?", "", blocks[mem_id], count=1
            ).strip()
            if old_body != clean_body:
                raise ValueError(f"ltm_body_conflict:{mem_id}")

        mutation_paths = [
            paths["body"], paths["meta"], runtime_paths.LTM_KEYWORDS_JSON,
            LTM_MEMORY_LINKS_JSON,
        ]
        mutation_paths.extend(
            self._ltm_tier_paths(name)["index"]
            for name in ("Full", "Summary", "Abstract", "Pinned", "Backup")
        )
        snapshots = {}
        for path in dict.fromkeys(mutation_paths):
            if not os.path.isfile(path):
                snapshots[path] = None
                continue
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    snapshots[path] = handle.read()
            except OSError as exc:
                raise ReadError(path, cause=exc)

        try:
            if mem_id not in blocks:
                heading = f"## {mem_id}  [{paths['code']}]  权重{int(entry.get('weight', 1) or 1)}"
                combined = existing_text.rstrip()
                combined = (
                    f"{combined}\n\n{heading}\n{clean_body}\n"
                    if combined else f"{heading}\n{clean_body}\n"
                )
                atomic_write_text(paths["body"], combined)

            if mem_id not in meta_doc:
                meta_doc[mem_id] = entry
                atomic_write_json(
                    paths["meta"],
                    self._compose_ltm_meta_document(meta_headers, meta_doc),
                )

            write_memory_overlay_entry(mem_id, meta)
            receipt = self.reconcile_ltm_projections(
                repair=False,
                ignore_source=(source_tier, mem_id) if transition else None,
            )
            self._verify_ltm_tier_entry(
                mem_id, tier=tier, body=clean_body, meta=entry)
            if transition:
                self._remove_ltm_source(source_tier, mem_id)
            else:
                self.verify_ltm_entry(
                    mem_id, tier=tier, body=clean_body, meta=entry)
            return receipt
        except Exception as exc:
            try:
                for path, text in snapshots.items():
                    if text is None:
                        if os.path.isfile(path):
                            os.remove(path)
                    else:
                        current = None
                        if os.path.isfile(path):
                            with open(path, "r", encoding="utf-8") as handle:
                                current = handle.read()
                        if current != text:
                            atomic_write_text(path, text)
            except Exception as restore_exc:
                raise RuntimeError(
                    f"ltm_target_restore_failed:{tier}:{mem_id}:{restore_exc}"
                ) from exc
            raise

    def _remove_ltm_source(self, tier, mem_id):
        paths = self._ltm_tier_paths(tier)
        updated = []
        meta_headers, meta_doc = self._ltm_meta_parts(paths["meta"])
        blocks, text = self._ltm_blocks(paths["body"])
        try:
            with open(paths["meta"], "r", encoding="utf-8") as handle:
                original_meta_text = handle.read()
        except OSError as exc:
            raise ReadError(paths["meta"], cause=exc)
        meta_changed = False
        try:
            if mem_id in meta_doc:
                del meta_doc[mem_id]
                atomic_write_json(
                    paths["meta"],
                    self._compose_ltm_meta_document(meta_headers, meta_doc),
                )
                meta_changed = True
                updated.append(paths["meta"])
            if mem_id in blocks:
                pattern = re.compile(
                    rf"(?ms)^##\s+{re.escape(mem_id)}\b.*?(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
                )
                text = pattern.sub("", text).strip()
                atomic_write_text(paths["body"], text + ("\n" if text else ""))
                updated.append(paths["body"])
        except Exception as exc:
            try:
                if meta_changed:
                    atomic_write_text(paths["meta"], original_meta_text)
            except Exception as restore_exc:
                raise RuntimeError(
                    f"ltm_source_restore_failed:{tier}:{mem_id}:{restore_exc}"
                ) from exc
            raise
        return updated

    def _verify_ltm_tier_entry(self, mem_id, *, tier, body, meta):
        paths = self._ltm_tier_paths(tier)
        blocks, _text = self._ltm_blocks(paths["body"])
        entries = self._ltm_meta(paths["meta"])
        if mem_id not in blocks or entries.get(mem_id) != meta:
            raise ValueError(f"ltm_entry_unverified:{mem_id}")
        actual = re.sub(
            r"(?s)^##\s+MEM-[0-9A-F]{8}\b[^\n]*\n?", "",
            blocks[mem_id], count=1,
        ).strip()
        if actual != str(body).strip():
            raise ValueError(f"ltm_body_unverified:{mem_id}")
        expected_index = self._render_ltm_index(tier, entries)
        try:
            with open(paths["index"], "r", encoding="utf-8") as handle:
                actual_index = handle.read()
        except OSError as exc:
            raise ReadError(paths["index"], cause=exc)
        if actual_index != expected_index:
            raise ValueError(f"ltm_index_unverified:{tier}")

    def verify_ltm_entry(self, mem_id, *, tier=None, body=None, meta=None):
        truth = self._ltm_truth()
        matches = [name for name, item in truth.items() if mem_id in item["meta"]]
        if tier is not None:
            matches = [name for name in matches if name == tier]
        if len(matches) != 1:
            raise ValueError(f"ltm_entry_unverified:{mem_id}")
        item = truth[matches[0]]
        if body is not None:
            actual = re.sub(
                r"(?s)^##\s+MEM-[0-9A-F]{8}\b[^\n]*\n?", "", item["blocks"][mem_id], count=1
            ).strip()
            if actual != str(body).strip():
                raise ValueError(f"ltm_body_unverified:{mem_id}")
        if meta is not None and item["meta"][mem_id] != meta:
            raise ValueError(f"ltm_meta_unverified:{mem_id}")
        return matches[0]

    def ltm_entry_state(self, mem_id, *, include_backup=True):
        """Return one verified LTM body/meta/tier tuple, or ``None``."""
        truth = self._ltm_truth()
        matches = [
            name for name, item in truth.items()
            if mem_id in item["meta"] and (include_backup or name != "Backup")
        ]
        if len(matches) > 1:
            raise ValueError(f"ltm_active_tier_conflict:{mem_id}")
        if not matches:
            return None
        tier = matches[0]
        return {
            "tier": tier,
            "memory_layer": f"LTM/{tier}",
            "body": truth[tier]["blocks"][mem_id],
            "meta": dict(truth[tier]["meta"][mem_id]),
        }

    def active_ltm_meta_by_id(self):
        """Return canonical metadata for every active LTM entry."""
        truth = self._ltm_truth()
        return {
            mem_id: dict(meta)
            for tier in ("Full", "Summary", "Abstract", "Pinned")
            for mem_id, meta in truth[tier]["meta"].items()
        }

    def admit_ltm_entry(self, mem_id, *, stored_at=None):
        """Fill formal admission time once and reset the full decay period."""
        state = self.ltm_entry_state(mem_id, include_backup=False)
        if state is None:
            raise EntryNotFoundError(mem_id)
        meta = dict(state["meta"])
        existing = str(meta.get("stored_at") or "").strip()
        if existing:
            return {
                "status": "noop", "mem_id": mem_id,
                "memory_layer": state["memory_layer"],
                "stored_at": existing, "admission_status": "stored",
            }
        value = stored_at or local_now().isoformat()
        if not normalize_iso_timestamp(value):
            raise ValueError("invalid_stored_at")
        period = meta.get("decay_period_days")
        if not isinstance(period, int) or isinstance(period, bool) or period <= 0:
            raise ValueError("invalid_decay_period_days")
        meta["stored_at"] = value
        meta["decay_countdown_days"] = period
        self.replace_ltm_entry(state["tier"], mem_id, state["body"], meta)
        return {
            "status": "applied", "mem_id": mem_id,
            "memory_layer": state["memory_layer"],
            "stored_at": value, "admission_status": "stored",
        }

    @staticmethod
    def snapshot_ltm_files():
        """Snapshot every canonical/derived LTM file touched by a tier move."""
        paths = []
        for tier in ("Full", "Summary", "Abstract", "Pinned", "Backup"):
            tier_paths = MemoryStore._ltm_tier_paths(tier)
            paths.extend(
                tier_paths[key] for key in ("body", "meta", "index"))
        paths.extend((runtime_paths.LTM_KEYWORDS_JSON, LTM_MEMORY_LINKS_JSON))
        snapshot = {}
        for path in dict.fromkeys(
                value for value in paths if isinstance(value, str)):
            if not os.path.isfile(path):
                snapshot[path] = None
                continue
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    snapshot[path] = handle.read()
            except OSError as exc:
                raise ReadError(path, cause=exc)
        return snapshot

    @staticmethod
    def restore_ltm_files(snapshot):
        for path, text in (snapshot or {}).items():
            if text is None:
                if os.path.isfile(path):
                    os.remove(path)
            else:
                atomic_write_text(path, text)

    def remove_stm_body(self, mem_id):
        """Remove only the STM body copy; never resolve through shared LTM."""
        try:
            self._remove_entry_from_file(MEMORY_MD, mem_id)
        except EntryNotFoundError:
            return

    @staticmethod
    def snapshot_stm_files():
        snapshot = {}
        for name, path in (
                ("body", MEMORY_MD), ("meta", META_JSON), ("index", INDEX_MD),
                ("keywords", runtime_paths.KEYWORDS_JSON),
                ("heat", runtime_paths.HEAT_JSON)):
            if not os.path.isfile(path):
                snapshot[name] = None
                continue
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    snapshot[name] = handle.read()
            except OSError as exc:
                raise ReadError(path, cause=exc)
        return snapshot

    @staticmethod
    def restore_stm_files(snapshot):
        for name, path in (
                ("body", MEMORY_MD), ("meta", META_JSON), ("index", INDEX_MD),
                ("keywords", runtime_paths.KEYWORDS_JSON),
                ("heat", runtime_paths.HEAT_JSON)):
            text = snapshot.get(name)
            if text is None:
                if os.path.isfile(path):
                    os.remove(path)
                continue
            atomic_write_text(path, text)

    @staticmethod
    def _replace_or_append_public_block(path, mem_id, body):
        block = str(body or "").strip()
        if not re.match(rf"^##\s+{re.escape(mem_id)}\b", block):
            raise ValueError("memory_heading_mismatch")
        existing = ""
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    existing = handle.read()
            except OSError as exc:
                raise ReadError(path, cause=exc)
        pattern = re.compile(
            rf"(?ms)^##\s+{re.escape(mem_id)}\b.*?(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
        )
        if pattern.search(existing):
            updated = pattern.sub(lambda _match: block + "\n", existing, count=1)
        else:
            prefix = existing.rstrip()
            updated = (
                f"{prefix}\n\n{block}\n" if prefix else f"{block}\n"
            )
        atomic_write_text(path, updated.rstrip() + "\n")

    def replace_stm_body(self, mem_id, body):
        """Upsert exactly one complete STM body block."""
        self._replace_or_append_public_block(MEMORY_MD, mem_id, body)

    def replace_stm_meta(self, mem_id, entry, *, canonical_sync=False):
        """Replace the current branch STM metadata without LTM fallback."""
        meta = self._read_json_file(META_JSON, merge_overlay=False)
        current = meta.get(mem_id)
        if (
            not canonical_sync
            and isinstance(current, dict)
            and int(current.get("weight")) != int((entry or {}).get("weight"))
        ):
            raise ValueError(f"memory_weight_immutable:{mem_id}")
        meta[mem_id] = dict(entry or {})
        meta[mem_id]["id"] = mem_id
        atomic_write_json(META_JSON, meta)

    @classmethod
    def _render_stm_index(cls, entries):
        lines = [
            "<!-- STM 索引行 -->",
            "",
            INDEX_HEADER,
            INDEX_SEPARATOR,
        ]
        for mem_id, entry in sorted(entries.items()):
            if not isinstance(entry, dict) or not re.fullmatch(
                    r"MEM-[0-9A-F]{8}", str(mem_id)):
                continue
            created = entry.get("created_round")
            recalled = entry.get("last_recalled_round")
            coordinates = "{} / {}".format(
                f"{int(created):05d}" if isinstance(created, int) else "—",
                f"{int(recalled):05d}" if isinstance(recalled, int) else "—",
            )
            lines.append(
                f"| {mem_id} | [{cls._index_cell(entry.get('type') or 'A')}] | "
                f"{int(entry.get('weight', 1) or 0)} | "
                f"{cls._index_cell(entry.get('title') or mem_id)} | "
                f"{'是' if entry.get('dream') else '否'} | "
                f"{cls._index_cell(entry.get('subject'))} | "
                f"{coordinates} | "
                f"{cls._index_cell(entry.get('current_overview'))} |"
            )
        return "\n".join(lines) + "\n"

    def rebuild_stm_index(self):
        meta = self._read_json_file(META_JSON, merge_overlay=False)
        atomic_write_text(INDEX_MD, self._render_stm_index(meta))

    @staticmethod
    def _stm_keyword_document(meta):
        index = {}
        for mem_id, entry in meta.items():
            if not isinstance(entry, dict) or not re.fullmatch(
                    r"MEM-[0-9A-F]{8}", str(mem_id)):
                continue
            for value in entry.get("tags") or []:
                keyword = str(value or "").strip()
                if keyword:
                    index.setdefault(keyword, []).append(mem_id)
        return {
            "_comment": "STM 倒排索引（关键词→条目ID列表）",
            "index": {
                key: sorted(dict.fromkeys(value))
                for key, value in sorted(index.items())
            },
        }

    def rebuild_stm_keywords(self):
        meta = self._read_json_file(META_JSON, merge_overlay=False)
        atomic_write_json(
            runtime_paths.KEYWORDS_JSON, self._stm_keyword_document(meta))

    def replace_ltm_entry(self, tier, mem_id, body, meta):
        """Replace one entry in its existing LTM tier and verify projections."""
        paths = self._ltm_tier_paths(tier)
        truth = self._ltm_truth()
        if mem_id not in truth.get(tier, {}).get("meta", {}):
            raise EntryNotFoundError(mem_id)
        clean_body = re.sub(
            r"(?s)^##\s+MEM-[0-9A-F]{8}\b[^\n]*\n?", "",
            str(body or "").strip(), count=1,
        ).strip()
        if not clean_body:
            raise ValueError("empty_ltm_body")
        entry = shared_memory_meta_entry(dict(meta or {}))
        entry["id"] = mem_id
        entry["type"] = paths["code"]
        current_meta = truth[tier]["meta"][mem_id]
        if int(current_meta.get("weight")) != int(entry.get("weight")):
            raise ValueError(f"memory_weight_immutable:{mem_id}")
        snapshot = self.snapshot_ltm_files()
        try:
            blocks, existing = self._ltm_blocks(paths["body"])
            if mem_id not in blocks:
                raise EntryNotFoundError(mem_id)
            heading = (
                f"## {mem_id}  [{paths['code']}]  "
                f"权重{int(entry.get('weight', 1) or 0)}"
            )
            pattern = re.compile(
                rf"(?ms)^##\s+{re.escape(mem_id)}\b.*?"
                rf"(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
            )
            replacement = f"{heading}\n{clean_body}\n"
            updated, count = pattern.subn(
                lambda _match: replacement, existing, count=1)
            if count != 1:
                raise EntryNotFoundError(mem_id)
            atomic_write_text(paths["body"], updated.rstrip() + "\n")
            meta_headers, meta_doc = self._ltm_meta_parts(paths["meta"])
            meta_doc[mem_id] = entry
            atomic_write_json(
                paths["meta"],
                self._compose_ltm_meta_document(meta_headers, meta_doc),
            )
            self.reconcile_ltm_projections(repair=False)
            self._verify_ltm_tier_entry(
                mem_id, tier=tier, body=clean_body, meta=entry)
        except Exception as exc:
            try:
                self.restore_ltm_files(snapshot)
            except Exception as restore_exc:
                raise RuntimeError(
                    f"ltm_replace_restore_failed:{tier}:{mem_id}:{restore_exc}"
                ) from exc
            raise
        return {
            "status": "applied",
            "mem_id": mem_id,
            "memory_layer": f"LTM/{tier}",
        }

    def remove_stm_copy(self, mem_id):
        """Remove one promoted STM copy, restoring all five files on failure."""
        from data.memory_heat import MemoryHeat
        from data.memory_index import MemoryIndex

        snapshot = self.snapshot_stm_files()
        try:
            # The LTM target is already verified. Removing the body first
            # leaves meta/heat as restart evidence for READY rollback repair.
            self.remove_stm_body(mem_id)
            self.remove_index(mem_id)
            MemoryIndex().remove_stm_entry(mem_id)
            self.delete_meta(mem_id)
            MemoryHeat().remove_entry(mem_id)
        except Exception as exc:
            try:
                self.restore_stm_files(snapshot)
            except Exception as restore_exc:
                raise RuntimeError(
                    f"stm_source_restore_failed:{mem_id}:{restore_exc}"
                ) from exc
            raise
        return [
            path for name, path in (
                ("body", MEMORY_MD), ("meta", META_JSON), ("index", INDEX_MD),
                ("keywords", runtime_paths.KEYWORDS_JSON),
                ("heat", runtime_paths.HEAT_JSON),
            ) if snapshot.get(name) is not None
        ]

    def _read_json_file(self, path, *, merge_overlay=True):
        if not os.path.isfile(path):
            return {}
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            raise ReadError(path, cause=e)
        if not isinstance(data, dict):
            return {}
        result = {
            key: _normalise_meta_entry(value) if isinstance(value, dict) else value
            for key, value in data.items()
        }
        if merge_overlay and os.path.abspath(path) in LTM_META_PATHS:
            overlay = _read_memory_overlay()["entries"]
            for mem_id, value in result.items():
                if isinstance(value, dict) and isinstance(overlay.get(mem_id), dict):
                    value.update({
                        key: overlay[mem_id].get(
                            key, [] if key == "linked_containers" else ""
                        )
                        for key in MEMORY_OVERLAY_FIELDS
                    })
        return result

    def _read_entry_from_file(self, path, mem_id):
        if not os.path.isfile(path):
            raise EntryNotFoundError(mem_id, f"{os.path.basename(path)} 不存在")
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(path, cause=e)

        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        marker = f"\n## MEM-{clean_id}"
        start = content.find(marker)
        if start == -1:
            if content.startswith(f"## MEM-{clean_id}"):
                start = 0
            else:
                raise EntryNotFoundError(mem_id)
        else:
            start += 1

        end = content.find("\n## ", start + 1)
        if end == -1:
            end = len(content)
        return content[start:end].strip()

    def _private_entry_paths(self, mem_id, directory=None):
        """Return private files that actually contain mem_id."""
        if directory is None:
            directories = list(dict.fromkeys(
                os.path.dirname(body_path)
                for _layer, _meta_path, body_path in self._active_read_layers()
            ))
        else:
            directories = [directory]
        matches = []
        # ponytail: linear file scan is enough for Seed; add a runtime cache only
        # if profiling shows private-file lookup is material.
        for root in directories:
            if not os.path.isdir(root):
                continue
            for name in sorted(os.listdir(root)):
                if not name.endswith(".private.md"):
                    continue
                path = os.path.join(root, name)
                try:
                    self._read_entry_from_file(path, mem_id)
                except Exception:
                    continue
                matches.append(path)
        return matches

    def private_subjects_for_memory(self, mem_id):
        """Derive privacy owners from the private files containing mem_id."""
        suffix = ".private.md"
        return list(dict.fromkeys(
            os.path.basename(path)[:-len(suffix)]
            for path in self._private_entry_paths(mem_id)
        ))

    def _private_entry_path(self, mem_id, directory=None):
        matches = self._private_entry_paths(mem_id, directory=directory)
        if not matches:
            return None
        if len(matches) != 1:
            raise ValueError("ambiguous_private_memory_owner")
        return matches[0]

    def _resolve_read_target(self, mem_id):
        # Resolve private metadata first only so the visibility gate can deny
        # it without touching or exposing a body. Private production remains
        # deferred and is not a fallback for public canonical reads.
        private_targets = []
        for layer, meta_path, body_path in self._active_ltm_read_layers():
            meta = self._read_json_file(meta_path, merge_overlay=False)
            entry = meta.get(mem_id)
            if not isinstance(entry, dict):
                continue
            if str(entry.get("access") or "public").strip().lower() == "private":
                private_targets.append((layer, body_path, dict(entry)))
        if len(private_targets) > 1:
            raise ValueError(f"ltm_active_tier_conflict:{mem_id}")
        if private_targets:
            layer, body_path, entry = private_targets[0]
            private_path = self._private_entry_path(
                mem_id, directory=os.path.dirname(body_path))
            if private_path is None:
                raise EntryNotFoundError(mem_id, "private memory body not found")
            return layer, _normalise_meta_entry(entry), private_path

        # Validate the whole LTM body/meta/tier truth before considering STM.
        # A damaged canonical tier must never be hidden by a readable local
        # copy of the same ID.
        ltm = self.ltm_entry_state(mem_id, include_backup=True)
        if ltm is not None:
            if ltm["tier"] == "Backup":
                raise ValueError(f"backup_memory_not_active:{mem_id}")
            stm = self._stm_source_state(mem_id)
            stm_parts = (
                stm.get("body") is not None,
                stm.get("meta") is not None,
                stm.get("heat") is not None,
            )
            if any(stm_parts):
                if not all(stm_parts):
                    raise ValueError(f"stm_ltm_residence_incomplete:{mem_id}")
                if str(
                        stm["meta"].get("access") or "public"
                ).strip().lower() != "public":
                    raise ValueError(f"stm_ltm_access_conflict:{mem_id}")
            paths = self._ltm_tier_paths(ltm["tier"])
            meta = dict(ltm["meta"])
            overlay = _read_memory_overlay()["entries"].get(mem_id)
            if isinstance(overlay, dict):
                meta.update({
                    key: overlay.get(
                        key, [] if key == "linked_containers" else ""
                    )
                    for key in MEMORY_OVERLAY_FIELDS
                })
            return ltm["memory_layer"], _normalise_meta_entry(meta), paths["body"]

        # Private LTM is deferred but remains discoverable for the privacy
        # gate; it is never allowed to fall through to a public STM copy.
        meta = self._read_json_file(META_JSON, merge_overlay=False)
        entry = meta.get(mem_id)
        try:
            self._read_entry_from_file(MEMORY_MD, mem_id)
            body_present = True
        except EntryNotFoundError:
            body_present = False
        if isinstance(entry, dict):
            if str(entry.get("access") or "public").strip().lower() == "private":
                private_path = self._private_entry_path(
                    mem_id, directory=os.path.dirname(MEMORY_MD))
                if private_path is None:
                    raise EntryNotFoundError(mem_id, "private memory body not found")
                if body_present:
                    raise ValueError(f"private_public_body_conflict:{mem_id}")
                return "STM", _normalise_meta_entry(entry), private_path
            if not body_present:
                raise ValueError(f"stm_body_meta_conflict:{mem_id}")
            return "STM", _normalise_meta_entry(entry), MEMORY_MD
        if body_present:
            raise ValueError(f"stm_body_meta_conflict:{mem_id}")
        raise EntryNotFoundError(mem_id)

    def read_meta_by_id(self, mem_id):
        """Return metadata for read tools across STM and active LTM layers."""
        layer, meta, _body_path = self._resolve_read_target(mem_id)
        meta["_memory_layer"] = layer
        return meta

    def mark_recalled(self, mem_id, round_num=None, recalled_at=None):
        """Update recall coordinates in the active metadata layer."""
        for layer, meta_path, _body_path in self._active_read_layers():
            meta = self._read_json_file(meta_path, merge_overlay=False)
            if not isinstance(meta.get(mem_id), dict):
                continue
            entry = dict(meta[mem_id])
            entry["last_recalled_at"] = recalled_at or local_now().isoformat()
            if round_num is not None:
                entry["last_recalled_round"] = round_num
            entry["last_recalled_instance_id"] = ACTIVE_INSTANCE_ID
            meta[mem_id] = entry
            if os.path.abspath(meta_path) in LTM_META_PATHS:
                meta = shared_memory_meta_document(meta)
            atomic_write_json(meta_path, meta)
            if layer != "STM":
                self.reconcile_ltm_projections(repair=False)
            return entry
        raise EntryNotFoundError(mem_id)

    # ==============================================================
    # memory.md 读写
    # ==============================================================

    def read_entry(self, mem_id):
        """Read canonical body, preferring verified active LTM over STM."""
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        return self._read_entry_from_file(body_path, mem_id)

    def read_stm_entry(self, mem_id):
        """Read only the current branch STM body for lifecycle code."""
        return self._read_entry_from_file(MEMORY_MD, mem_id)

    def read_stm_meta_by_id(self, mem_id):
        """Read only registered current branch STM metadata."""
        meta = self._read_json_file(META_JSON, merge_overlay=False)
        if not isinstance(meta.get(mem_id), dict):
            raise EntryNotFoundError(mem_id)
        return dict(meta[mem_id])

    def render_entry(self, mem_id, title, summary="", weight=2,
                     tags=None, linked_containers=None,
                     feelings=None, delta_desc="", subject=None,
                     round_num=None, last_recalled_round=None,
                     dream=False, current_overview="", created_at=None,
                     stored_at=""):
        """Render one complete STM-compatible memory block without writing."""
        now = created_at or local_now().isoformat()
        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        # morph 不带括号，模板自己包
        morph = {5: "F", 4: "S", 3: "S", 2: "A", 1: "A"}.get(weight, "A")
        max_len = _body_limit_for_weight(weight)
        source_text = summary.strip() if summary else ""
        if source_text and len(source_text) > max_len:
            raise ValueError(
                f"memory_body_too_long:max={max_len};actual={len(source_text)}")
        content = source_text
        overview = _overview_text(current_overview)

        if weight >= 5:
            content_line = f"**内容**（≤2048字）：{content}" if content else ""
        elif weight >= 3:
            content_line = f"**摘要**（≤512字）：{content}" if content else ""
        else:
            content_line = f"**梗概**（≤128字）：{content}" if content else ""

        def _round_text(value):
            if value is None:
                return "未知"
            return f"第{value}轮"

        last_round = last_recalled_round if last_recalled_round is not None else round_num
        return MEMORY_ENTRY_TEMPLATE.format(
            mem_id=clean_id,
            morph=morph,
            weight=weight,
            subject=subject or "—",
            created_round_text=_round_text(round_num),
            last_recalled_round_text=_round_text(last_round),
            title=title,
            dream_text=_dream_text(dream),
            current_overview=overview,
            content_line=content_line,
            created_at=now,
            stored_at_text=stored_at or "未入库",
            tags=", ".join(tags) if tags else "",
            feelings=", ".join(feelings) if feelings else "无",
            delta_desc=delta_desc if delta_desc else "",
            linked_containers=", ".join(linked_containers) if linked_containers else "",
        )

    def list_entries(self):
        """列出 STM 公开与私密正文中实际存在的活动条目 ID。"""
        result = []
        body_paths = [MEMORY_MD]
        root = os.path.dirname(MEMORY_MD)
        if os.path.isdir(root):
            body_paths.extend(
                os.path.join(root, name)
                for name in sorted(os.listdir(root))
                if name.endswith(".private.md")
            )
        for body_path in body_paths:
            if not os.path.isfile(body_path):
                continue
            try:
                with open(body_path, "r", encoding="utf-8") as f:
                    result.extend(re.findall(
                        r"^## (MEM-[0-9A-F]{8})", f.read(), re.MULTILINE))
            except OSError:
                continue
        return list(dict.fromkeys(result))

    def list_public_entries(self):
        """List one public row per ID with canonical LTM static fields."""
        truth = self._ltm_truth()
        overlay = _read_memory_overlay()["entries"]
        by_id = {}
        for tier in ("Full", "Summary", "Abstract", "Pinned"):
            for mem_id, raw in truth[tier]["meta"].items():
                entry = _normalise_meta_entry(raw)
                if isinstance(overlay.get(mem_id), dict):
                    entry.update({
                        key: overlay[mem_id].get(
                            key, [] if key == "linked_containers" else ""
                        )
                        for key in MEMORY_OVERLAY_FIELDS
                    })
                entry.update({
                    "id": mem_id,
                    "stm_present": False,
                    "ltm_layer": f"LTM/{tier}",
                    "memory_layers": [f"LTM/{tier}"],
                })
                by_id[mem_id] = entry

        stm_meta = self._read_json_file(META_JSON, merge_overlay=False)
        stm_body_ids = set()
        if os.path.isfile(MEMORY_MD):
            try:
                with open(MEMORY_MD, "r", encoding="utf-8") as handle:
                    stm_body_ids = set(re.findall(
                        r"^## (MEM-[0-9A-F]{8})", handle.read(), re.MULTILINE))
            except OSError as exc:
                raise ReadError(MEMORY_MD, cause=exc)
        all_stm_meta_ids = {
            mem_id for mem_id, raw in stm_meta.items()
            if isinstance(raw, dict) and re.fullmatch(r"MEM-[0-9A-F]{8}", str(mem_id))
        }
        private_stm_ids = {
            mem_id for mem_id in all_stm_meta_ids
            if str(stm_meta[mem_id].get("access") or "public").strip().lower()
            == "private"
        }
        # Private STM entries share the legacy body file but stay invisible to
        # the public projection.
        stm_body_ids -= private_stm_ids
        stm_meta_ids = all_stm_meta_ids - private_stm_ids
        if stm_body_ids != stm_meta_ids:
            detail = sorted(stm_body_ids ^ stm_meta_ids)[0]
            raise ValueError(f"stm_body_meta_conflict:{detail}")
        for mem_id in sorted(stm_meta_ids):
            raw = dict(stm_meta[mem_id])
            if str(raw.get("access") or "public").strip().lower() != "public":
                by_id.pop(mem_id, None)
                continue
            if mem_id in by_id:
                stm = self._stm_source_state(mem_id)
                if stm.get("heat") is None:
                    raise ValueError(f"stm_ltm_residence_incomplete:{mem_id}")
                for key in MEMORY_OVERLAY_FIELDS:
                    if key in raw:
                        by_id[mem_id][key] = raw[key]
                by_id[mem_id]["stm_present"] = True
                by_id[mem_id]["memory_layers"] = [
                    "STM", by_id[mem_id]["ltm_layer"]
                ]
                continue
            raw.update({
                "id": mem_id,
                "stm_present": True,
                "ltm_layer": "",
                "memory_layers": ["STM"],
            })
            by_id[mem_id] = raw

        entries = []
        for mem_id, entry in by_id.items():
            entry["id"] = mem_id
            entry.setdefault("stm_present", False)
            entry.setdefault("ltm_layer", "")
            entry["memory_layer"] = (
                "STM" if entry["stm_present"] else entry["ltm_layer"]
            )
            entry["admission_status"] = (
                "stored" if memory_is_admitted(entry) else "pending"
            )
            entries.append(entry)

        return sorted(
            entries,
            key=lambda item: (
                str(item.get("created_at") or ""),
                str(item.get("id") or ""),
            ),
            reverse=True,
        )

    def write_entry(self, mem_id, title, summary="", weight=2,
                    tags=None, linked_containers=None,
                    feelings=None, delta_desc="", subject=None,
                    round_num=None, last_recalled_round=None,
                    dream=False, current_overview=""):
        """追加一条新记忆条目到 memory.md（原子写）。"""
        entry_text = self.render_entry(
            mem_id, title, summary=summary, weight=weight, tags=tags,
            linked_containers=linked_containers, feelings=feelings,
            delta_desc=delta_desc, subject=subject, round_num=round_num,
            last_recalled_round=last_recalled_round, dream=dream,
            current_overview=current_overview,
        )
        os.makedirs(os.path.dirname(MEMORY_MD), exist_ok=True)
        if os.path.isfile(MEMORY_MD):
            with open(MEMORY_MD, "r", encoding="utf-8") as handle:
                existing = handle.read()
        else:
            existing = "<!-- STM 记忆条目正文 -->\n"
        atomic_write_text(
            MEMORY_MD,
            existing.rstrip() + "\n\n" + entry_text + "\n",
        )

    def list_public_ltm_entries(self):
        """Return active public LTM truth without STM transition shadowing."""
        entries = []
        seen = set()
        overlay = _read_memory_overlay()["entries"]
        for layer, meta_path, body_path in self._active_read_layers()[1:]:
            meta = self._ltm_meta(meta_path)
            for mem_id, raw in meta.items():
                if mem_id in seen:
                    raise ValueError(f"ltm_active_tier_conflict:{mem_id}")
                seen.add(mem_id)
                if str(raw.get("access") or "public").strip().lower() != "public":
                    continue
                try:
                    body = self._read_entry_from_file(body_path, mem_id)
                except EntryNotFoundError as exc:
                    raise ValueError(
                        f"ltm_body_meta_conflict:{layer}:{mem_id}"
                    ) from exc
                entry = _normalise_meta_entry(raw)
                if isinstance(overlay.get(mem_id), dict):
                    entry.update({
                        key: overlay[mem_id].get(
                            key, [] if key == "linked_containers" else ""
                        )
                        for key in MEMORY_OVERLAY_FIELDS
                    })
                entry["id"] = mem_id
                entry["memory_layer"] = layer
                entry["body"] = body
                entries.append(entry)
        return sorted(entries, key=lambda item: str(item.get("id") or ""))


    def remove_entry(self, mem_id):
        """从当前公开/私密正文文件中物理移除指定条目块。"""
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        self._remove_entry_from_file(body_path, mem_id)

    def _remove_entry_from_file(self, body_path, mem_id):
        if not os.path.isfile(body_path):
            raise EntryNotFoundError(mem_id)
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(body_path, cause=e)

        import re
        pattern = re.compile(
            rf"(?ms)^##\s+{re.escape(mem_id)}\b.*?(?=^##\s+MEM-[0-9A-F]{{8}}\b|\Z)"
        )
        new_content, count = pattern.subn("", content)
        if count == 0:
            raise EntryNotFoundError(mem_id)

        if body_path.endswith(".private.md") and not re.search(
                r"(?m)^##\s+MEM-[0-9A-F]{8}\b", new_content):
            os.remove(body_path)
            return

        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content.rstrip() + "\n")
            os.replace(tmp, body_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=e)

    # ==============================================================
    # meta.json 读写
    # ==============================================================

    def load_meta(self):
        """读取 meta.json 全量"""
        if not os.path.isfile(META_JSON):
            return default_meta_json()
        return self._read_json_file(META_JSON)

    def save_meta(self, meta):
        """写入 meta.json（原子）"""
        atomic_write_json(META_JSON, meta)

    def get_meta(self, mem_id):
        """获取单条记忆的元数据"""
        meta = self.load_meta()
        if mem_id not in meta:
            return default_meta_entry(mem_id)
        return meta[mem_id]

    def set_meta(self, mem_id, entry):
        """写入单条记忆元数据"""
        meta = self.load_meta()
        if mem_id in meta:
            if int(meta[mem_id].get("weight")) != int(entry.get("weight")):
                raise ValueError(f"memory_weight_immutable:{mem_id}")
            meta[mem_id] = entry
            self.save_meta(meta)
            return
        for _layer, meta_path, _body_path in self._active_read_layers()[1:]:
            ltm_meta = self._read_json_file(meta_path, merge_overlay=False)
            if mem_id not in ltm_meta:
                continue
            if int(ltm_meta[mem_id].get("weight")) != int(entry.get("weight")):
                raise ValueError(f"memory_weight_immutable:{mem_id}")
            write_memory_overlay_entry(mem_id, entry)
            ltm_meta[mem_id] = shared_memory_meta_entry(entry)
            atomic_write_json(meta_path, shared_memory_meta_document(ltm_meta))
            self.reconcile_ltm_projections(repair=False)
            return
        meta[mem_id] = entry
        self.save_meta(meta)


    def delete_meta(self, mem_id):
        """删除单条记忆元数据"""
        meta = self.load_meta()
        if mem_id in meta:
            del meta[mem_id]
            self.save_meta(meta)

    # ==============================================================
    # index.md 读写
    # ==============================================================

    def append_index(self, mem_id, entry_type, weight, title,
                     subject="", round_num=0, dream=False,
                     current_overview=""):
        """追加一条索引行（原子写）"""
        os.makedirs(os.path.dirname(INDEX_MD), exist_ok=True)
        default_index = f"<!-- STM 索引行 -->\n\n{INDEX_HEADER}\n{INDEX_SEPARATOR}\n"
        overview = _overview_text(current_overview)

        line = (f"| {mem_id} | [{entry_type}] | {weight} "
                f"| {title} | {_dream_text(dream)} | {subject or '—'} "
                f"| {round_num:05d} | {overview} |\n")

        tmp = INDEX_MD + ".tmp"
        try:
            if os.path.isfile(INDEX_MD):
                with open(INDEX_MD, "r", encoding="utf-8") as f:
                    existing = f.read()
            else:
                existing = default_index
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(existing.rstrip() + "\n" + line)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)

    def read_index(self):
        """读取 index.md 全部行"""
        if not os.path.isfile(INDEX_MD):
            return []
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError:
            return []
        # 过滤表头，只返回数据行
        return [l.strip() for l in lines
                if l.startswith("| MEM-") and "|---" not in l]


    def update_entry_title_and_body(self, mem_id, title, body):
        """同步更新 memory.md 正文标题/正文行与 index.md 标题列。"""
        clean_title = str(title or "").strip()
        clean_body = str(body or "").strip()
        if not clean_title or not clean_body:
            raise ValueError("missing_title_or_body")
        layer, meta, body_path = self._resolve_read_target(mem_id)
        source = self._read_entry_from_file(body_path, mem_id)
        tier = layer.split("/", 1)[1] if layer.startswith("LTM/") else None
        if tier not in {"Full", "Summary", "Abstract"}:
            tier = None
        normalized = replace_memory_semantic_payload(
            source,
            clean_title,
            clean_body,
            meta.get("weight"),
            tier=tier,
        )
        try:
            with open(body_path, "r", encoding="utf-8") as handle:
                content = handle.read()
        except OSError as exc:
            raise ReadError(body_path, cause=exc)
        start, end = self._entry_bounds(content, mem_id)
        suffix = content[end:].lstrip("\r\n")
        separator = "\n\n" if suffix else "\n"
        updated = content[:start] + normalized.rstrip() + separator + suffix
        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as handle:
                handle.write(updated)
            os.replace(tmp, body_path)
        except OSError as exc:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=exc)
        self._update_index_title(mem_id, clean_title)

    def read_body_by_id(self, mem_id, **range_request):
        """Return one memory entry body and metadata for protocol read tools."""
        layer, meta, body_path = self._resolve_read_target(mem_id)
        body = self._read_entry_from_file(body_path, mem_id)
        ranged = apply_explicit_range(body, range_kwargs_from_request(range_request))
        return {
            "mem_id": mem_id,
            "memory_layer": layer,
            "meta": meta,
            "body": ranged["content"],
            "read_mode": ranged["read_mode"],
            "range_requested": ranged["range_requested"],
            "range_applied": ranged["range_applied"],
            "total_lines": ranged["total_lines"],
            "total_chars": ranged["total_chars"],
        }

    def update_linked_containers(self, mem_id, operation, container_refs,
                                 current_overview=None):
        """Update local link overlay while shared LTM provenance stays global."""
        refs = []
        for ref in container_refs or []:
            text = str(ref or "").strip()
            if text and text not in refs:
                refs.append(text)
        op = str(operation or "add").strip().lower()
        if op not in {"add", "remove", "set"}:
            raise ValueError("invalid_operation")

        target = None
        ltm = self.ltm_entry_state(mem_id, include_backup=True)
        if ltm is not None and ltm.get("tier") != "Backup":
            layer, entry, _body_path = self._resolve_read_target(mem_id)
            target = (layer, self._ltm_tier_paths(ltm["tier"])["meta"], {
                mem_id: entry,
            })
        else:
            candidate = self._read_json_file(META_JSON, merge_overlay=False)
            if isinstance(candidate.get(mem_id), dict):
                target = ("STM", META_JSON, candidate)
        if target is None:
            raise EntryNotFoundError(mem_id)
        layer, meta_path, meta = target
        entry = dict(meta[mem_id])
        current = []
        for ref in entry.get("linked_containers") or []:
            text = str(ref or "").strip()
            if text and text not in current:
                current.append(text)

        if op == "set":
            updated_refs = refs
        elif op == "add":
            updated_refs = current + [ref for ref in refs if ref not in current]
        else:
            updated_refs = [ref for ref in current if ref not in refs]

        entry["linked_containers"] = updated_refs
        if current_overview is not None:
            overview = _overview_text(current_overview)
            if overview != str(entry.get("current_overview") or "").strip():
                entry["current_overview_updated_at"] = local_now().isoformat()
            entry["current_overview"] = overview
        entry = _normalise_meta_entry(entry)
        if layer == "STM":
            meta[mem_id] = entry
            self.save_meta(meta)
            self._update_memory_linked_containers(mem_id, updated_refs)
            if current_overview is not None:
                self._update_memory_current_overview(mem_id, entry["current_overview"])
                self._update_index_current_overview(mem_id, entry["current_overview"])
        else:
            write_memory_overlay_entry(mem_id, entry)
            stm = self._stm_source_state(mem_id)
            if stm.get("body") is not None and stm.get("meta") is not None:
                local = dict(stm["meta"])
                for key in MEMORY_OVERLAY_FIELDS:
                    local[key] = entry.get(
                        key, [] if key == "linked_containers" else "")
                self.replace_stm_meta(mem_id, local)
                self.rebuild_stm_index()
        return entry

    def mark_private(self, mem_id, privacy_subject, body_action="move_private"):
        """Move one public memory into its relation-owned private file."""
        subject = str(privacy_subject or "").strip()
        if not subject:
            raise ValueError("missing_privacy_subject")
        action = str(body_action or "move_private").strip().lower()
        if action != "move_private":
            raise ValueError("invalid_body_action")

        meta = self.load_meta()
        if mem_id not in meta or not isinstance(meta.get(mem_id), dict):
            raise EntryNotFoundError(mem_id)
        entry = dict(meta[mem_id])
        original_entry = dict(entry)
        private_path = self._private_memory_path(subject)
        owners = self.private_subjects_for_memory(mem_id)
        if owners and owners != [subject]:
            raise ValueError("privacy_subject_conflict")

        if owners == [subject]:
            try:
                self._read_entry_from_file(MEMORY_MD, mem_id)
            except Exception:
                pass
            else:
                self._remove_entry_from_file(MEMORY_MD, mem_id)
            entry["access"] = "private"
            meta[mem_id] = entry
            self.save_meta(meta)
            result = dict(entry)
            result["private_path"] = private_path
            return result

        try:
            body = self._read_entry_from_file(MEMORY_MD, mem_id)
        except Exception as exc:
            raise EntryNotFoundError(mem_id, "public memory body not found") from exc
        if not body.strip():
            raise ValueError("empty_memory_body")

        self._append_entry_to_file(
            private_path,
            body,
            header=f"<!-- private memory: {subject} -->",
        )
        entry["access"] = "private"
        meta[mem_id] = entry
        try:
            self.save_meta(meta)
            self._remove_entry_from_file(MEMORY_MD, mem_id)
        except Exception:
            try:
                self._remove_entry_from_file(private_path, mem_id)
            except Exception:
                pass
            try:
                meta[mem_id] = original_entry
                self.save_meta(meta)
            except Exception:
                pass
            raise

        result = dict(entry)
        result["private_path"] = private_path
        return result

    def _append_entry_to_file(self, body_path, body, header=""):
        """Atomically append one complete memory block to body_path."""
        existing = ""
        if os.path.isfile(body_path):
            try:
                with open(body_path, "r", encoding="utf-8") as f:
                    existing = f.read()
            except OSError as exc:
                raise ReadError(body_path, cause=exc)
        os.makedirs(os.path.dirname(body_path), exist_ok=True)
        content = existing.rstrip()
        if not content:
            content = header.strip()
        content = content + "\n\n" + str(body or "").strip() + "\n"
        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(content)
            os.replace(tmp, body_path)
        except OSError as exc:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=exc)

    def declassify_private_memory(self, mem_id, mode, redacted_body="", reason=""):
        """Apply privacy declassification modes to a private memory entry."""
        action = str(mode or "").strip().lower()
        if action not in {"declassify", "redact", "delete", "keep_private"}:
            raise ValueError("invalid_declassify_mode")
        meta = self.load_meta()
        if mem_id not in meta or not isinstance(meta.get(mem_id), dict):
            raise EntryNotFoundError(mem_id)
        entry = dict(meta[mem_id])
        private_path = self._private_entry_path(mem_id)
        if private_path is None:
            raise EntryNotFoundError(mem_id, "private memory body not found")

        if action == "keep_private":
            entry["access"] = "private"
            meta[mem_id] = entry
            self.save_meta(meta)
            result = dict(entry)
            result["mode"] = action
            return result

        if action == "delete":
            self._remove_entry_from_file(private_path, mem_id)
            self.delete_meta(mem_id)
            self.remove_index(mem_id)
            return {"mem_id": mem_id, "mode": action, "deleted": True}

        if action == "redact":
            clean_body = str(redacted_body or "").strip()
            if not clean_body:
                raise ValueError("missing_redacted_body")
            self.update_entry_title_and_body(
                mem_id,
                entry.get("title") or mem_id,
                clean_body,
            )

        body = self._read_entry_from_file(private_path, mem_id)
        try:
            self._remove_entry_from_file(MEMORY_MD, mem_id)
        except Exception:
            pass
        self._append_entry_to_file(
            MEMORY_MD,
            body,
            header="<!-- STM 记忆条目正文 -->",
        )
        self._remove_entry_from_file(private_path, mem_id)
        entry["access"] = "public"
        meta[mem_id] = entry
        self.save_meta(meta)

        result = dict(entry)
        result["mode"] = action
        result["reason"] = str(reason or "").strip()
        result["private_path"] = private_path
        return result

    @staticmethod
    def _private_memory_path(subject):
        safe_subject = re.sub(r'[<>:"/\\\\|?*]+', "_", str(subject or "private").strip())
        safe_subject = safe_subject or "private"
        return os.path.join(os.path.dirname(MEMORY_MD), f"{safe_subject}.private.md")

    def _entry_bounds(self, content, mem_id):
        clean_id = mem_id[4:] if mem_id.startswith("MEM-") else mem_id
        patterns = (f"## MEM-{clean_id}", f"## {mem_id}")
        start = -1
        for pattern in patterns:
            match = re.search(rf"(?m)^{re.escape(pattern)}\b", content)
            if match:
                start = match.start()
                break
        if start == -1:
            raise EntryNotFoundError(mem_id)
        end_match = re.search(r"(?m)^##\s+MEM-[0-9A-FA-Z-]+\b", content[start + 1:])
        end = start + 1 + end_match.start() if end_match else len(content)
        return start, end

    def _update_memory_current_overview(self, mem_id, overview):
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(body_path, cause=e)

        start, end = self._entry_bounds(content, mem_id)
        block = content[start:end]
        replacement = f"现状概况：{overview}"
        if re.search(r"(?m)^现状概况：", block):
            block = re.sub(
                r"(?m)^现状概况：.*$",
                lambda _match: replacement,
                block,
                count=1,
            )
        elif re.search(r"(?m)^梦源：", block):
            block = re.sub(
                r"(?m)^(梦源：.*)$",
                lambda match: f"{match.group(1)}\n{replacement}",
                block,
                count=1,
            )
        else:
            block = re.sub(
                r"(?m)^(\*\*标题\*\*：.*)$",
                lambda match: f"{match.group(1)}\n{replacement}",
                block,
                count=1,
            )

        new_content = content[:start] + block + content[end:]
        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content)
            os.replace(tmp, body_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=e)

    def _update_index_current_overview(self, mem_id, overview):
        if not os.path.isfile(INDEX_MD):
            return
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            raise ReadError(INDEX_MD, cause=e)

        changed = False
        new_lines = []
        for line in lines:
            if line.lstrip().startswith(f"| {mem_id} |"):
                cells = [part.strip() for part in line.strip().strip("|").split("|")]
                if len(cells) >= 8:
                    cells[7] = overview
                else:
                    while len(cells) < 7:
                        cells.append("")
                    cells.append(overview)
                line = "| " + " | ".join(cells) + " |\n"
                changed = True
            new_lines.append(line)
        if not changed:
            return

        tmp = INDEX_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)

    def _update_index_title(self, mem_id, title):
        if not os.path.isfile(INDEX_MD):
            return
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            raise ReadError(INDEX_MD, cause=e)

        changed = False
        new_lines = []
        for line in lines:
            if line.lstrip().startswith(f"| {mem_id} |"):
                cells = [part.strip() for part in line.strip().strip("|").split("|")]
                if len(cells) >= 4:
                    cells[3] = title
                    line = "| " + " | ".join(cells) + " |\n"
                    changed = True
            new_lines.append(line)
        if not changed:
            return

        tmp = INDEX_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)

    def _update_memory_linked_containers(self, mem_id, linked_containers):
        _layer, _meta, body_path = self._resolve_read_target(mem_id)
        try:
            with open(body_path, "r", encoding="utf-8") as f:
                content = f.read()
        except OSError as e:
            raise ReadError(body_path, cause=e)

        start, end = self._entry_bounds(content, mem_id)
        block = content[start:end]
        replacement = f"关联容器：{', '.join(linked_containers)}"
        if re.search(r"(?m)^关联容器：.*$", block):
            block = re.sub(
                r"(?m)^关联容器：.*$",
                lambda _match: replacement,
                block,
                count=1,
            )
        else:
            block = block.rstrip() + "\n" + replacement + "\n"
        new_content = content[:start] + block + content[end:]

        tmp = body_path + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.write(new_content)
            os.replace(tmp, body_path)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(body_path, cause=e)

    def remove_index(self, mem_id):
        """从 index.md 中移除指定条目行"""
        if not os.path.isfile(INDEX_MD):
            return
        try:
            with open(INDEX_MD, "r", encoding="utf-8") as f:
                lines = f.readlines()
        except OSError as e:
            raise ReadError(INDEX_MD, cause=e)

        new_lines = [
            line for line in lines
            if not line.lstrip().startswith(f"| {mem_id} |")
        ]
        if new_lines == lines:
            return

        tmp = INDEX_MD + ".tmp"
        try:
            with open(tmp, "w", encoding="utf-8") as f:
                f.writelines(new_lines)
            os.replace(tmp, INDEX_MD)
        except OSError as e:
            if os.path.isfile(tmp):
                os.remove(tmp)
            raise WriteError(INDEX_MD, cause=e)
