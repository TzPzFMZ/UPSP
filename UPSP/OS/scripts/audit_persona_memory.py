"""persona 记忆完整性审计器。

只读扫描 STM/memory 与 LTM/Memory，输出 JSON 或 Markdown 摘要。
"""
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path


VALID_MEM_ID = re.compile(r"^MEM-[0-9A-F]{8}$")
OLD_MEM_ID = re.compile(r"^MEM-(\d{5})-(\d{2})$")
BARE_MEM_ID = re.compile(r"^[0-9A-F]{8}$")
HEADING_RE = re.compile(r"(?m)^##\s+((?:MEM-[0-9A-F]{8})|(?:MEM-\d{5}-\d{2})|(?:[0-9A-F]{8}))\b")
MEM_REF_RE = re.compile(r"(MEM-[0-9A-F]{8}|MEM-\d{5}-\d{2}|(?<![0-9A-F])(?:[0-9A-F]{8})(?![0-9A-F]))")
BAD_KEY_PATTERNS = (
    "从本轮语料提取关键词",
    "写入倒排索引",
    "新建记忆条目",
    "权重",
    "Δ动态",
    "`",
)
RETIRED_META_FIELDS = ("abstract", "locked", "merged_from", "mode", "source_rounds")
ANNOTATION_RE = re.compile(r"(?m)^[^\S\r\n]*注释[:：][^\S\r\n]*(.*)$")
GIST_RE = re.compile(r"(?m)^\s*\*\*梗概\*\*")
DREAM_RE = re.compile(r"(?m)^[^\S\r\n]*梦源[:：][^\S\r\n]*(.*)$")
OVERVIEW_RE = re.compile(r"(?m)^[^\S\r\n]*现状概况[:：][^\S\r\n]*(.*)$")
EXPECTED_META_FIELDS = (
    "id", "type", "weight", "title", "dream", "created_at",
    "last_recalled_at", "created_round", "last_recalled_round",
    "source", "model", "subject", "access", "recalled",
    "current_overview", "current_overview_updated_at", "tags", "linked_containers",
    "decay_period_days", "decay_countdown_days", "media",
)


def canonical_id(value: str) -> str:
    value = str(value or "").strip()
    if VALID_MEM_ID.match(value):
        return value
    if BARE_MEM_ID.match(value):
        return f"MEM-{value}"
    old = OLD_MEM_ID.match(value)
    if old:
        round_num = int(old.group(1))
        seq_num = int(old.group(2))
        return f"MEM-{round_num:05X}{seq_num:03X}"
    return value


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"_error": str(exc)}


def markdown_ids(path: Path) -> list[str]:
    if not path.exists():
        return []
    text = path.read_text(encoding="utf-8")
    return [canonical_id(match.group(1)) for match in HEADING_RE.finditer(text)]


def index_ids(path: Path) -> list[str]:
    if not path.exists():
        return []
    ids = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not cells or cells[0] in {"编号", "条目ID"} or set(cells[0]) <= {"-"}:
            continue
        ids.append(canonical_id(cells[0]))
    return ids


def keyword_ids(path: Path) -> list[str]:
    data = load_json(path)
    seen = set()

    def walk(obj):
        if isinstance(obj, str):
            for match in MEM_REF_RE.finditer(obj):
                cid = canonical_id(match.group(1))
                if VALID_MEM_ID.match(cid):
                    seen.add(cid)
        elif isinstance(obj, list):
            for item in obj:
                walk(item)
        elif isinstance(obj, dict):
            for item in obj.values():
                walk(item)

    walk(data)
    return sorted(seen)


def keyword_report(path: Path, known_ids: set[str] | None = None) -> dict:
    data = load_json(path)
    if "_error" in data:
        return {
            "keyword_count": 0,
            "referenced_ids": [],
            "bad_keyword_keys": [data["_error"]],
            "dangling_refs": [],
        }

    index = data.get("index", {}) if isinstance(data, dict) else {}
    if not isinstance(index, dict):
        return {
            "keyword_count": 0,
            "referenced_ids": [],
            "bad_keyword_keys": ["index is not an object"],
            "dangling_refs": [],
        }

    bad_keys = []
    refs = set()
    for key, value in index.items():
        key_text = str(key).strip()
        if not key_text or any(pattern in key_text for pattern in BAD_KEY_PATTERNS):
            bad_keys.append(key_text)

        values = value if isinstance(value, list) else [value]
        for item in values:
            if isinstance(item, str):
                for match in MEM_REF_RE.finditer(item):
                    cid = canonical_id(match.group(1))
                    if VALID_MEM_ID.match(cid):
                        refs.add(cid)

    dangling = sorted(refs - known_ids) if known_ids is not None else []
    return {
        "keyword_count": len(index),
        "referenced_ids": sorted(refs),
        "bad_keyword_keys": sorted(set(bad_keys)),
        "dangling_refs": dangling,
    }


def blank_lines_inside_table(path: Path) -> list[int]:
    if not path.exists():
        return []
    lines = path.read_text(encoding="utf-8").splitlines()
    blanks = []
    in_table = False
    for idx, line in enumerate(lines, start=1):
        stripped = line.strip()
        if stripped.startswith("|"):
            in_table = True
            continue
        if in_table and stripped == "":
            next_table = any(l.strip().startswith("|") for l in lines[idx:])
            if next_table:
                blanks.append(idx)
            else:
                in_table = False
        elif stripped and not stripped.startswith("|"):
            in_table = False
    return blanks


def meta_ids(path: Path) -> tuple[list[str], list[str]]:
    data = load_json(path)
    if "_error" in data:
        return [], [data["_error"]]
    ids = []
    mismatches = []
    for key, value in data.items():
        if key.startswith("_"):
            continue
        cid = canonical_id(key)
        ids.append(cid)
        inner = canonical_id(value.get("id", "")) if isinstance(value, dict) else ""
        if inner and inner != cid:
            mismatches.append(f"{key}: inner id {value.get('id')} != key")
    return ids, mismatches


def retired_meta_fields(path: Path) -> dict[str, list[str]]:
    data = load_json(path)
    if "_error" in data:
        return {}
    result = {}
    for key, value in data.items():
        if str(key).startswith("_") or not isinstance(value, dict):
            continue
        cid = canonical_id(key)
        fields = sorted(field for field in RETIRED_META_FIELDS if field in value)
        if fields:
            result[cid] = fields
    return result


def meta_field_shape_report(path: Path) -> dict[str, list[str]]:
    data = load_json(path)
    if "_error" in data:
        return {"_error": [data["_error"]]}
    result = {}
    expected = list(EXPECTED_META_FIELDS)
    for key, value in data.items():
        if str(key).startswith("_") or not isinstance(value, dict):
            continue
        keys = list(value.keys())
        if "current_overview_updated_at" not in keys and "current_overview" in keys:
            keys.insert(keys.index("current_overview") + 1, "current_overview_updated_at")
        if keys != expected:
            result[canonical_id(key)] = list(value.keys())
    return result


def body_surface_report(path: Path) -> dict:
    if not path.exists():
        return {
            "missing_dream": [],
            "missing_overview": [],
            "overview_too_long": {},
            "residual_annotations": {},
            "residual_gist": [],
        }
    text = path.read_text(encoding="utf-8")
    matches = list(HEADING_RE.finditer(text))
    missing_dream = []
    missing_overview = []
    overview_too_long = {}
    residual_annotations = {}
    residual_gist = []
    for idx, match in enumerate(matches):
        end = matches[idx + 1].start() if idx + 1 < len(matches) else len(text)
        mem_id = canonical_id(match.group(1))
        body = text[match.end():end]
        if not DREAM_RE.search(body):
            missing_dream.append(mem_id)
        overview = OVERVIEW_RE.search(body)
        if not overview:
            missing_overview.append(mem_id)
        else:
            value = overview.group(1).strip()
            if len(value) > 128:
                overview_too_long[mem_id] = value
        annotation = ANNOTATION_RE.search(body)
        if annotation:
            residual_annotations[mem_id] = annotation.group(1).strip()
        if GIST_RE.search(body):
            residual_gist.append(mem_id)
    return {
        "missing_dream": sorted(missing_dream),
        "missing_overview": sorted(missing_overview),
        "overview_too_long": overview_too_long,
        "residual_annotations": residual_annotations,
        "residual_gist": sorted(residual_gist),
    }


def index_surface_report(path: Path) -> dict:
    if not path.exists():
        return {"missing_dream": [], "missing_overview": [], "residual_annotation_column": False}
    missing_dream = []
    missing_overview = []
    has_dream = False
    has_overview = False
    residual_annotation_column = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if not line.startswith("|"):
            continue
        cells = [cell.strip() for cell in line.strip().strip("|").split("|")]
        if not cells:
            continue
        if cells[0] in {"编号", "条目ID"}:
            has_dream = "梦源" in cells
            has_overview = "现状概况" in cells
            residual_annotation_column = "注释" in cells
            continue
        if set(cells[0]) <= {"-"}:
            continue
        mem_id = canonical_id(cells[0])
        if not VALID_MEM_ID.match(mem_id):
            continue
        if not has_dream or len(cells) < 5:
            missing_dream.append(mem_id)
        if not has_overview or len(cells) < 8:
            missing_overview.append(mem_id)
    return {
        "missing_dream": sorted(missing_dream),
        "missing_overview": sorted(missing_overview),
        "residual_annotation_column": residual_annotation_column,
    }


def scan_backups(root: Path) -> list[str]:
    residuals = []
    for path in root.rglob("*"):
        rel = path.relative_to(root).as_posix()
        if "LTM/Memory/Backup" in rel:
            continue
        name = path.name.lower()
        if name.endswith(".bak") or name.startswith("_backup_") or name == "backup":
            residuals.append(rel)
    return residuals


def audit(root: Path) -> dict:
    stm = root / "STM" / "memory"
    ltm = root / "LTM" / "Memory"
    body = set(markdown_ids(stm / "memory.md"))
    meta, meta_mismatches = meta_ids(stm / "meta.json")
    index = set(index_ids(stm / "index.md"))
    keywords = set(keyword_ids(stm / "keywords.json"))
    heat_data = load_json(stm / "heat.json")
    heat = set(canonical_id(k) for k in heat_data.get("entries", {}).keys())
    stm_keyword_report = keyword_report(stm / "keywords.json", body | set(meta))
    stm_body_surface = body_surface_report(stm / "memory.md")
    stm_index_surface = index_surface_report(stm / "index.md")

    ltm_report = {}
    layer_files = {
        "Full": ("full.md", "index.md", "meta.json"),
        "Summary": ("summary.md", "index.md", "meta.json"),
        "Abstract": ("abstract.md", "index.md", "meta.json"),
        "Pinned": ("pinned.md", "index.md", "meta.json"),
        "Backup": ("backup.md", "index.md", "meta.json"),
    }
    ltm_locations = {}
    for layer, files in layer_files.items():
        layer_dir = ltm / layer
        body_ids = markdown_ids(layer_dir / files[0])
        meta_layer, mismatches = meta_ids(layer_dir / files[2])
        idx_ids = index_ids(layer_dir / files[1])
        body_surface = body_surface_report(layer_dir / files[0])
        index_surface = index_surface_report(layer_dir / files[1])
        for mid in set(body_ids) | set(meta_layer):
            ltm_locations.setdefault(mid, []).append(layer)
        ltm_report[layer] = {
            "body_count": len(body_ids),
            "body_unique_count": len(set(body_ids)),
            "meta_count": len(meta_layer),
            "index_count": len(idx_ids),
            "duplicate_body_ids": sorted({mid for mid in body_ids if body_ids.count(mid) > 1}),
            "meta_id_mismatches": mismatches,
            "body_not_meta": sorted(set(body_ids) - set(meta_layer)),
            "meta_not_body": sorted(set(meta_layer) - set(body_ids)),
            "body_not_index": sorted(set(body_ids) - set(idx_ids)),
            "index_not_body": sorted(set(idx_ids) - set(body_ids)),
            "retired_meta_fields": retired_meta_fields(layer_dir / files[2]),
            "meta_field_shape_mismatches": meta_field_shape_report(layer_dir / files[2]),
            "missing_body_dream": body_surface["missing_dream"],
            "missing_body_current_overview": body_surface["missing_overview"],
            "body_current_overview_too_long": body_surface["overview_too_long"],
            "body_annotation_residuals": body_surface["residual_annotations"],
            "body_gist_residuals": body_surface["residual_gist"],
            "missing_index_dream": index_surface["missing_dream"],
            "missing_index_current_overview": index_surface["missing_overview"],
            "index_annotation_column_residual": index_surface["residual_annotation_column"],
        }

    duplicates = {
        mid: layers
        for mid, layers in ltm_locations.items()
        if len(set(layers) - {"Pinned", "Backup"}) > 1
    }
    ltm_keyword_report = keyword_report(ltm / "keywords.json", set(ltm_locations.keys()))

    return {
        "root": str(root),
        "stm": {
            "body_count": len(body),
            "meta_count": len(meta),
            "index_count": len(index),
            "keywords_count": len(keywords),
            "heat_count": len(heat),
            "blank_lines_inside_index_table": blank_lines_inside_table(stm / "index.md"),
            "invalid_meta_ids": sorted(mid for mid in meta if not VALID_MEM_ID.match(mid)),
            "invalid_body_ids": sorted(mid for mid in body if not VALID_MEM_ID.match(mid)),
            "meta_id_mismatches": meta_mismatches,
            "body_not_meta": sorted(body - set(meta)),
            "meta_not_body": sorted(set(meta) - body),
            "body_not_index": sorted(body - index),
            "index_not_body": sorted(index - body),
            "body_not_heat": sorted(body - heat),
            "heat_not_body": sorted(heat - body),
            "bad_keyword_keys": stm_keyword_report["bad_keyword_keys"],
            "keyword_dangling_refs": stm_keyword_report["dangling_refs"],
            "retired_meta_fields": retired_meta_fields(stm / "meta.json"),
            "meta_field_shape_mismatches": meta_field_shape_report(stm / "meta.json"),
            "missing_body_dream": stm_body_surface["missing_dream"],
            "missing_body_current_overview": stm_body_surface["missing_overview"],
            "body_current_overview_too_long": stm_body_surface["overview_too_long"],
            "body_annotation_residuals": stm_body_surface["residual_annotations"],
            "body_gist_residuals": stm_body_surface["residual_gist"],
            "missing_index_dream": stm_index_surface["missing_dream"],
            "missing_index_current_overview": stm_index_surface["missing_overview"],
            "index_annotation_column_residual": stm_index_surface["residual_annotation_column"],
        },
        "ltm": ltm_report,
        "ltm_keywords": ltm_keyword_report,
        "ltm_multi_layer_duplicates": duplicates,
        "backup_residuals": scan_backups(root),
    }


def render_markdown(report: dict) -> str:
    lines = ["# persona 记忆完整性审计", ""]
    stm = report["stm"]
    lines.append("## STM")
    for key in ("body_count", "meta_count", "index_count", "keywords_count", "heat_count"):
        lines.append(f"- {key}: {stm[key]}")
    for key in ("blank_lines_inside_index_table", "invalid_meta_ids", "invalid_body_ids",
                "body_not_meta", "meta_not_body", "body_not_heat",
                "missing_body_dream", "missing_body_current_overview",
                "missing_index_dream", "missing_index_current_overview",
                "body_gist_residuals"):
        values = stm[key]
        lines.append(f"- {key}: {len(values)}")
        if values:
            lines.append(f"  - {', '.join(map(str, values[:20]))}")
    lines.append(f"- retired_meta_fields: {len(stm.get('retired_meta_fields', {}))}")
    lines.append(f"- meta_field_shape_mismatches: {len(stm.get('meta_field_shape_mismatches', {}))}")
    lines.append(f"- body_annotation_residuals: {len(stm.get('body_annotation_residuals', {}))}")
    lines.append(f"- index_annotation_column_residual: {stm.get('index_annotation_column_residual', False)}")
    lines.append("")
    lines.append("## LTM")
    for layer, data in report["ltm"].items():
        lines.append(
            f"- {layer}: body={data['body_count']} unique={data['body_unique_count']} "
            f"meta={data['meta_count']} index={data['index_count']} "
            f"duplicate_body={len(data['duplicate_body_ids'])}"
        )
        for key in ("body_not_index", "index_not_body"):
            values = data.get(key, [])
            lines.append(f"  - {key}: {len(values)}")
            if values:
                lines.append(f"    - {', '.join(map(str, values[:20]))}")
        lines.append(f"  - retired_meta_fields: {len(data.get('retired_meta_fields', {}))}")
        lines.append(f"  - meta_field_shape_mismatches: {len(data.get('meta_field_shape_mismatches', {}))}")
        lines.append(f"  - missing_body_current_overview: {len(data.get('missing_body_current_overview', []))}")
        lines.append(f"  - missing_index_current_overview: {len(data.get('missing_index_current_overview', []))}")
        lines.append(f"  - body_annotation_residuals: {len(data.get('body_annotation_residuals', {}))}")
        lines.append(f"  - body_gist_residuals: {len(data.get('body_gist_residuals', []))}")
        lines.append(f"  - index_annotation_column_residual: {data.get('index_annotation_column_residual', False)}")
    lines.append("")
    ltm_keywords = report.get("ltm_keywords", {})
    lines.append("## LTM Keywords")
    lines.append(f"- keyword_count: {ltm_keywords.get('keyword_count', 0)}")
    for key in ("bad_keyword_keys", "dangling_refs"):
        values = ltm_keywords.get(key, [])
        lines.append(f"- {key}: {len(values)}")
        if values:
            lines.append(f"  - {', '.join(map(str, values[:20]))}")
    lines.append("")
    lines.append(f"## 备份残留: {len(report['backup_residuals'])}")
    for item in report["backup_residuals"]:
        lines.append(f"- {item}")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", default="OS/persona")
    parser.add_argument("--format", choices=["json", "md"], default="json")
    args = parser.parse_args()

    report = audit(Path(args.root).resolve())
    if args.format == "json":
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        print(render_markdown(report), end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
