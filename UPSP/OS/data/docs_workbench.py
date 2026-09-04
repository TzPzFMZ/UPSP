"""Data helpers for the docs visual workbench."""
from __future__ import annotations

import difflib
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from paths import DOCS_DIR


SPEC_DIRNAME = "178-docs信息图工作台与草案diff底座"
CATEGORIES = ("inject", "lookup", "popup", "persona")
POPUP_TIERS = ("guide", "reminder", "warning")
SUCCESS_CONSUMER_ORDER = (
    "STATUSBAR",
    "CONTENT",
    "LOOKUP",
    "POPUP:guide",
    "POPUP:reminder",
    "POPUP:warning",
    "runtime_growth",
)
DEPRECATED_FEELING_USAGE = "投影到高频层感受词库"
GUIDE_SOURCE_PATHS = {
    "protocol/base/tools.md",
    "protocol/base/popup.md",
}
TEMPLATE_SOURCE_PATHS = {
    "protocol/base/schema.md",
    "protocol/base/popup.md",
}


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_docs_root(repo_root: str | Path | None = None) -> Path:
    if repo_root is None:
        return Path(DOCS_DIR)
    root = Path(repo_root)
    if root.resolve() == default_repo_root().resolve():
        return Path(DOCS_DIR)
    return root / "UPSP" / "OS" / "persona" / "docs"


def default_spec_dir(repo_root: str | Path | None = None) -> Path:
    root = Path(repo_root) if repo_root is not None else default_repo_root()
    return root / ".speckit" / "specs" / SPEC_DIRNAME


def default_draft_root(repo_root: str | Path | None = None) -> Path:
    return default_spec_dir(repo_root) / "drafts" / "docs"


def default_diff_log_path(repo_root: str | Path | None = None) -> Path:
    return default_spec_dir(repo_root) / "diff_log.jsonl"


def normalize_doc_path(raw_path: str | Path) -> str:
    raw_value = str(raw_path or "").replace("\\", "/").strip()
    if raw_value.startswith("/"):
        raise ValueError("doc path must be relative")
    value = raw_value.lstrip("/")
    if not value:
        raise ValueError("doc path is empty")
    pure = PurePosixPath(value)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError("doc path escapes docs root")
    if ":" in value:
        raise ValueError("doc path must be relative")
    return pure.as_posix()


def _safe_join(root: Path, rel_path: str) -> Path:
    root = root.resolve()
    target = (root / normalize_doc_path(rel_path)).resolve()
    if os.path.commonpath([str(root), str(target)]) != str(root):
        raise ValueError("path escapes root")
    return target


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return ""


def load_registry(repo_root: str | Path | None = None) -> dict[str, Any]:
    path = default_docs_root(repo_root) / "docs_registry.json"
    try:
        with path.open("r", encoding="utf-8") as f:
            registry = json.load(f)
    except (OSError, json.JSONDecodeError, UnicodeError):
        registry = {}
    for category in ("inject", "lookup", "persona"):
        registry.setdefault(category, [])
    popup = registry.get("popup")
    if not isinstance(popup, dict):
        popup = {}
    for tier in POPUP_TIERS:
        popup.setdefault(tier, [])
    registry["popup"] = popup
    return registry


def _doc_roles(rel_path: str, category: str, entry: dict[str, Any]) -> list[str]:
    roles = [category]
    tier = str(entry.get("tier") or "")
    usage = str(entry.get("usage") or "")
    if category == "popup":
        roles.append("popup_source")
        if tier:
            roles.append(f"popup_{tier}")
    if category == "popup" or rel_path in GUIDE_SOURCE_PATHS or "POPUP guide" in usage:
        roles.append("guide_source")
    if rel_path in TEMPLATE_SOURCE_PATHS or "模板" in usage or "schema" in usage:
        roles.append("template_source")
    return list(dict.fromkeys(role for role in roles if role))


def _iter_registry_entries(registry: dict[str, Any]):
    for category in ("inject", "lookup", "persona"):
        for order, entry in enumerate(registry.get(category, []) or []):
            yield category, "", order, entry
    popup = registry.get("popup") or {}
    if isinstance(popup, dict):
        for tier in POPUP_TIERS:
            for order, entry in enumerate(popup.get(tier, []) or []):
                yield "popup", tier, order, entry


def _registered_entries(registry: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for category, tier, order, entry in _iter_registry_entries(registry):
        if not isinstance(entry, dict):
            continue
        raw_path = entry.get("path")
        if not raw_path:
            continue
        try:
            rel_path = normalize_doc_path(raw_path)
        except ValueError:
            continue
        item = dict(entry)
        if category == "popup":
            item.setdefault("tier", tier)
        item.update({
            "category": category,
            "order": order,
            "rel_path": rel_path,
            "registered": True,
            "roles": _doc_roles(rel_path, category, item),
        })
        entries.append(item)
    return entries


def _consumer_for_item(item: dict[str, Any]) -> str:
    category = item.get("category")
    if category == "inject":
        return str(item.get("target") or "INJECT")
    if category == "lookup":
        return "LOOKUP"
    if category == "popup":
        return f"POPUP:{item.get('tier') or 'unknown'}"
    if category == "persona":
        return str(item.get("consume") or "PERSONA")
    return "UNREGISTERED"


def _mount_from_entry(entry: dict[str, Any]) -> dict[str, Any]:
    return {
        "category": entry.get("category", ""),
        "order": entry.get("order", 0),
        "tier": entry.get("tier", ""),
        "source_mode": entry.get("source_mode", ""),
        "target": entry.get("target", ""),
        "trigger": entry.get("trigger", ""),
        "usage": entry.get("usage", ""),
        "consume": entry.get("consume", ""),
        "description": entry.get("description", ""),
        "roles": entry.get("roles") or [],
        "consumer": _consumer_for_item(entry),
    }


def list_doc_files(repo_root: str | Path | None = None) -> list[dict[str, Any]]:
    docs_root = default_docs_root(repo_root)
    draft_root = default_draft_root(repo_root)
    registry = load_registry(repo_root)
    entries = _registered_entries(registry)
    by_path: dict[str, dict[str, Any]] = {}
    for entry in entries:
        rel_path = entry["rel_path"]
        mount = _mount_from_entry(entry)
        if rel_path not in by_path:
            item = dict(entry)
            item["mounts"] = [mount]
            by_path[rel_path] = item
        else:
            by_path[rel_path].setdefault("mounts", []).append(mount)

    if docs_root.is_dir():
        for path in sorted(docs_root.rglob("*.md")):
            rel_path = path.relative_to(docs_root).as_posix()
            if rel_path not in by_path:
                by_path[rel_path] = {
                    "file": path.name,
                    "path": rel_path,
                    "category": "unregistered",
                    "order": 9999,
                    "rel_path": rel_path,
                    "registered": False,
                    "target": "",
                    "trigger": "",
                    "usage": "",
                    "consume": "",
                    "description": "真实存在但未进入 docs_registry.json",
                    "roles": ["unregistered"],
                    "mounts": [],
                }

    category_rank = {category: idx for idx, category in enumerate(CATEGORIES)}
    category_rank["unregistered"] = len(CATEGORIES)
    files = []
    for rel_path, entry in by_path.items():
        production_path = _safe_join(docs_root, rel_path)
        draft_path = _safe_join(draft_root, rel_path)
        production_text = _read_text(production_path)
        draft_exists = draft_path.is_file()
        draft_text = _read_text(draft_path) if draft_exists else ""
        production_hash = _sha256_text(production_text) if production_text else ""
        draft_hash = _sha256_text(draft_text) if draft_exists else ""
        files.append({
            "rel_path": rel_path,
            "file": entry.get("file") or Path(rel_path).name,
            "category": entry.get("category", "unregistered"),
            "target": entry.get("target", ""),
            "trigger": entry.get("trigger", ""),
            "usage": entry.get("usage", ""),
            "consume": entry.get("consume", ""),
            "description": entry.get("description", ""),
            "tier": entry.get("tier", ""),
            "source_mode": entry.get("source_mode", ""),
            "consumer": _consumer_for_item(entry),
            "registered": bool(entry.get("registered")),
            "roles": entry.get("roles") or [],
            "order": int(entry.get("order") or 0),
            "mounts": entry.get("mounts") or [],
            "draft_exists": draft_exists,
            "draft_changed": bool(draft_exists and draft_hash != production_hash),
            "production_exists": production_path.is_file(),
            "production_hash": production_hash,
            "draft_hash": draft_hash,
        })
    files.sort(key=lambda item: (
        category_rank.get(item["category"], 999),
        item["order"],
        item["rel_path"],
    ))
    return files


def build_issues(repo_root: str | Path | None = None) -> list[dict[str, Any]]:
    issues: list[dict[str, Any]] = []
    for item in list_doc_files(repo_root):
        mounts = item.get("mounts") or []
        usages = [str(mount.get("usage") or "") for mount in mounts] or [str(item.get("usage") or "")]
        rel_path = str(item.get("rel_path") or "")
        if any(DEPRECATED_FEELING_USAGE in usage for usage in usages):
            issues.append({
                "id": "docs_registry_feeling_projection_deprecated",
                "severity": "red",
                "message": "docs_registry.json 仍写感受词投影到高频层；当前应由 memory_write 参数说明近位注入词清单。",
                "file": rel_path,
                "source": "Spec158 docs registry 红项",
            })
        has_popup_mount = any(mount.get("category") == "popup" for mount in mounts)
        lookup_source_mount = any(
            mount.get("category") == "lookup"
            and set(mount.get("roles") or []).intersection({"guide_source", "template_source"})
            for mount in mounts
        )
        if lookup_source_mount and not has_popup_mount:
            issues.append({
                "id": "docs_registry_lookup_mixes_guide_source",
                "severity": "yellow",
                "message": "该 docs 文件仍以 lookup 登记，但实际承担 guide/template 源职责。",
                "file": rel_path,
                "source": "Spec158 guide_source/template_source 裁决项",
            })
    return issues


def build_graph(repo_root: str | Path | None = None) -> dict[str, Any]:
    files = list_doc_files(repo_root)
    graph_items = []
    for item in files:
        mounts = item.get("mounts") or []
        if not mounts:
            graph_items.append(item)
            continue
        for mount in mounts:
            graph_item = dict(item)
            graph_item.update(mount)
            graph_items.append(graph_item)
    consumers = sorted(
        {_consumer_for_item(item) for item in graph_items},
        key=lambda value: (
            SUCCESS_CONSUMER_ORDER.index(value)
            if value in SUCCESS_CONSUMER_ORDER else len(SUCCESS_CONSUMER_ORDER),
            value,
        ),
    )
    nodes = [
        {"id": f"consumer:{consumer}", "type": "consumer", "label": consumer}
        for consumer in consumers
    ]
    for item in files:
        nodes.append({
            "id": f"file:{item['rel_path']}",
            "type": "doc",
            "label": item["file"],
            "rel_path": item["rel_path"],
            "category": item["category"],
            "roles": item.get("roles") or [],
            "mounts": item.get("mounts") or [],
            "draft_exists": item["draft_exists"],
        })
    edges = []
    for idx, item in enumerate(graph_items):
        consumer = _consumer_for_item(item)
        edges.append({
            "id": f"{idx}:{item['rel_path']}->{consumer}",
            "source": f"file:{item['rel_path']}",
            "target": f"consumer:{consumer}",
            "category": item["category"],
            "consumer": consumer,
            "tier": item.get("tier", ""),
            "source_mode": item.get("source_mode", ""),
            "reason": item.get("trigger") or item.get("usage") or item.get("consume") or item.get("description") or "",
            "roles": item.get("roles") or [],
        })
    return {
        "nodes": nodes,
        "edges": edges,
        "issues": build_issues(repo_root),
    }


def build_workbench_index(repo_root: str | Path | None = None) -> dict[str, Any]:
    registry = load_registry(repo_root)
    files = list_doc_files(repo_root)
    groups: dict[str, list[dict[str, Any]]] = {category: [] for category in CATEGORIES}
    groups["unregistered"] = []
    for item in files:
        mounts = item.get("mounts") or []
        if not mounts:
            groups.setdefault(item["category"], []).append(item)
            continue
        for mount in mounts:
            grouped = dict(item)
            grouped.update({
                "category": mount.get("category", item["category"]),
                "tier": mount.get("tier", item.get("tier", "")),
                "source_mode": mount.get("source_mode", item.get("source_mode", "")),
                "target": mount.get("target", item.get("target", "")),
                "trigger": mount.get("trigger", item.get("trigger", "")),
                "usage": mount.get("usage", item.get("usage", "")),
                "consume": mount.get("consume", item.get("consume", "")),
                "description": mount.get("description", item.get("description", "")),
                "roles": mount.get("roles", item.get("roles", [])),
                "consumer": mount.get("consumer", item.get("consumer", "")),
                "order": mount.get("order", item.get("order", 0)),
            })
            groups.setdefault(grouped["category"], []).append(grouped)
    return {
        "schema_version": "docs_workbench.v1",
        "registry_version": registry.get("_version", ""),
        "docs_root": str(default_docs_root(repo_root)),
        "draft_root": str(default_draft_root(repo_root)),
        "diff_log": str(default_diff_log_path(repo_root)),
        "categories": list(CATEGORIES),
        "files": files,
        "groups": groups,
        "graph": build_graph(repo_root),
        "issues": build_issues(repo_root),
    }


def make_unified_diff(before: str, after: str, fromfile: str, tofile: str) -> str:
    if before == after:
        return ""
    return "".join(difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=fromfile,
        tofile=tofile,
    ))


def load_doc_document(rel_path: str, repo_root: str | Path | None = None) -> dict[str, Any]:
    rel_path = normalize_doc_path(rel_path)
    docs_root = default_docs_root(repo_root)
    draft_root = default_draft_root(repo_root)
    production_path = _safe_join(docs_root, rel_path)
    draft_path = _safe_join(draft_root, rel_path)
    production = _read_text(production_path)
    draft = _read_text(draft_path) if draft_path.is_file() else None
    current = draft if draft is not None else production
    source = "draft" if draft is not None else "production"
    current_diff = make_unified_diff(
        production,
        current,
        fromfile=f"production/{rel_path}",
        tofile=f"{source}/{rel_path}",
    )
    return {
        "rel_path": rel_path,
        "source": source,
        "content": current,
        "original_content": production,
        "production_content": production,
        "draft_content": draft,
        "production_hash": _sha256_text(production),
        "current_hash": _sha256_text(current),
        "draft_hash": _sha256_text(draft) if draft is not None else "",
        "draft_exists": draft is not None,
        "current_diff": current_diff,
    }


def save_doc_draft(rel_path: str, content: str, note: str = "",
                   repo_root: str | Path | None = None) -> dict[str, Any]:
    rel_path = normalize_doc_path(rel_path)
    document = load_doc_document(rel_path, repo_root)
    before = document["content"]
    draft_root = default_draft_root(repo_root)
    draft_path = _safe_join(draft_root, rel_path)
    draft_path.parent.mkdir(parents=True, exist_ok=True)
    draft_path.write_text(content, encoding="utf-8")

    diff = make_unified_diff(
        before,
        content,
        fromfile=f"{document['source']}/{rel_path}",
        tofile=f"draft/{rel_path}",
    )
    record = {
        "timestamp": datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds"),
        "rel_path": rel_path,
        "note": str(note or ""),
        "old_hash": _sha256_text(before),
        "new_hash": _sha256_text(content),
        "old_source": document["source"],
        "draft_path": str(draft_path),
        "diff": diff,
    }
    log_path = default_diff_log_path(repo_root)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

    updated = load_doc_document(rel_path, repo_root)
    updated["diff_record"] = record
    return updated


def read_diff_log(repo_root: str | Path | None = None, limit: int = 100) -> list[dict[str, Any]]:
    path = default_diff_log_path(repo_root)
    if not path.is_file():
        return []
    records = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    if limit > 0:
        records = records[-limit:]
    return records
