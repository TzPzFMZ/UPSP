"""Data helpers for the rules visual workbench."""
from __future__ import annotations

import difflib
import hashlib
import json
import os
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from typing import Any

from paths import RULES_DIR


SPEC_DIRNAME = "159-rules信息图工作台与草案diff底座"
CATEGORIES = (
    "permanent",
    "passive_read",
    "step_level",
    "periodic",
    "on_demand",
)
STEP_ORDER = ("setup", "reaction", "cleanup")
STEP_SCENES = {
    "setup": ("containers", "memory"),
    "reaction": ("workbench", "containers"),
    "cleanup": ("memory", "relation"),
}
CONDITIONAL_STEP_SCENES = {
    "setup": ("relation",),
}
SCENE_KEYWORDS = {
    "memory": ("记忆", "memory"),
    "relation": ("交互对象", "关系"),
    "containers": ("容器", "container"),
    "workbench": ("WB", "焦点", "workbench"),
    "rhythm": ("节律", "中继"),
    "debug": ("调试", "初始化"),
}


def default_repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def default_rules_root(repo_root: str | Path | None = None) -> Path:
    if repo_root is None:
        return Path(RULES_DIR)
    root = Path(repo_root)
    if root.resolve() == default_repo_root().resolve():
        return Path(RULES_DIR)
    return root / "UPSP" / "OS" / "persona" / "rules"


def default_spec_dir(repo_root: str | Path | None = None) -> Path:
    root = Path(repo_root) if repo_root is not None else default_repo_root()
    return root / ".speckit" / "specs" / SPEC_DIRNAME


def default_draft_root(repo_root: str | Path | None = None) -> Path:
    return default_spec_dir(repo_root) / "drafts" / "rules"


def default_diff_log_path(repo_root: str | Path | None = None) -> Path:
    return default_spec_dir(repo_root) / "diff_log.jsonl"


def normalize_rule_path(raw_path: str | Path) -> str:
    value = str(raw_path or "").replace("\\", "/").strip().lstrip("/")
    if not value:
        raise ValueError("rule path is empty")
    pure = PurePosixPath(value)
    if pure.is_absolute() or any(part in {"", ".", ".."} for part in pure.parts):
        raise ValueError("rule path escapes rules root")
    if ":" in value:
        raise ValueError("rule path must be relative")
    return pure.as_posix()


def _safe_join(root: Path, rel_path: str) -> Path:
    root = root.resolve()
    target = (root / normalize_rule_path(rel_path)).resolve()
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
    path = default_rules_root(repo_root) / "rules_registry.json"
    try:
        with path.open("r", encoding="utf-8") as f:
            registry = json.load(f)
    except (OSError, json.JSONDecodeError, UnicodeError):
        registry = {}
    for category in CATEGORIES:
        registry.setdefault(category, [])
    return registry


def _registered_entries(registry: dict[str, Any]) -> list[dict[str, Any]]:
    entries: list[dict[str, Any]] = []
    for category in CATEGORIES:
        for order, entry in enumerate(registry.get(category, []) or []):
            if not isinstance(entry, dict):
                continue
            raw_path = entry.get("path")
            if not raw_path:
                continue
            try:
                rel_path = normalize_rule_path(raw_path)
            except ValueError:
                continue
            item = dict(entry)
            item.update({
                "category": category,
                "order": order,
                "rel_path": rel_path,
                "registered": True,
            })
            entries.append(item)
    return entries


def list_rule_files(repo_root: str | Path | None = None) -> list[dict[str, Any]]:
    rules_root = default_rules_root(repo_root)
    draft_root = default_draft_root(repo_root)
    registry = load_registry(repo_root)
    entries = _registered_entries(registry)
    by_path: dict[str, dict[str, Any]] = {}
    for entry in entries:
        rel_path = entry["rel_path"]
        mount = {
            "category": entry.get("category", ""),
            "order": entry.get("order", 0),
            "layer": entry.get("layer", ""),
            "scope": entry.get("scope", ""),
            "step": entry.get("step", ""),
            "trigger": entry.get("trigger", ""),
            "load": entry.get("load", ""),
            "sections": entry.get("sections", ""),
            "description": entry.get("description", ""),
        }
        if rel_path not in by_path:
            item = dict(entry)
            item["mounts"] = [mount]
            by_path[rel_path] = item
        else:
            by_path[rel_path].setdefault("mounts", []).append(mount)

    if rules_root.is_dir():
        for path in sorted(rules_root.rglob("*.md")):
            rel_path = path.relative_to(rules_root).as_posix()
            if rel_path not in by_path:
                by_path[rel_path] = {
                    "file": path.name,
                    "path": rel_path,
                    "category": "unregistered",
                    "order": 9999,
                    "rel_path": rel_path,
                    "registered": False,
                    "layer": "未登记",
                    "description": "真实存在但未进入 rules_registry.json",
                    "mounts": [],
                }

    files = []
    category_rank = {category: idx for idx, category in enumerate(CATEGORIES)}
    category_rank["unregistered"] = len(CATEGORIES)
    for rel_path, entry in by_path.items():
        production_path = _safe_join(rules_root, rel_path)
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
            "layer": entry.get("layer", ""),
            "scope": entry.get("scope", ""),
            "step": entry.get("step", ""),
            "trigger": entry.get("trigger", ""),
            "load": entry.get("load", ""),
            "sections": entry.get("sections", ""),
            "description": entry.get("description", ""),
            "registered": bool(entry.get("registered")),
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


def _scene_matches(trigger_text: str, scene: str) -> bool:
    keywords = SCENE_KEYWORDS.get(scene, (scene,))
    return any(keyword in trigger_text for keyword in keywords)


def _edge(edge_id: str, source: str, target: str, step: str, category: str,
          order: int, reason: str, conditional: bool = False) -> dict[str, Any]:
    return {
        "id": edge_id,
        "source": source,
        "target": target,
        "step": step,
        "category": category,
        "order": order,
        "reason": reason,
        "conditional": conditional,
    }


def build_step_loads(repo_root: str | Path | None = None) -> dict[str, list[dict[str, Any]]]:
    files = list_rule_files(repo_root)
    by_path = {item["rel_path"]: item for item in files}
    registry_entries = _registered_entries(load_registry(repo_root))
    loads = {step: [] for step in STEP_ORDER}

    def add(step: str, rel_path: str, category: str, order: int,
            reason: str, conditional: bool = False) -> None:
        item = by_path.get(rel_path)
        if not item:
            return
        loads[step].append({
            "rel_path": rel_path,
            "file": item["file"],
            "category": category,
            "order": order,
            "reason": reason,
            "conditional": conditional,
        })

    for entry in registry_entries:
        rel_path = entry["rel_path"]
        category = entry["category"]
        order = int(entry.get("order") or 0)
        if category == "permanent":
            for step in STEP_ORDER:
                add(step, rel_path, category, order, "固定层常驻")
        elif category == "step_level":
            step = str(entry.get("step") or "")
            if step in loads:
                add(step, rel_path, category, order, "步级规则")
        elif category == "periodic":
            trigger = str(entry.get("trigger") or "")
            for step, scenes in STEP_SCENES.items():
                if any(_scene_matches(trigger, scene) for scene in scenes):
                    add(step, rel_path, category, order, "默认场景触发")
            for step, scenes in CONDITIONAL_STEP_SCENES.items():
                if any(_scene_matches(trigger, scene) for scene in scenes):
                    add(step, rel_path, category, order, "条件场景触发", conditional=True)

    for step in STEP_ORDER:
        loads[step].sort(key=lambda item: (
            CATEGORIES.index(item["category"]) if item["category"] in CATEGORIES else 999,
            item["order"],
            item["rel_path"],
        ))
    return loads


def build_graph(repo_root: str | Path | None = None) -> dict[str, Any]:
    files = list_rule_files(repo_root)
    loads = build_step_loads(repo_root)
    nodes = [
        {"id": f"step:{step}", "type": "step", "label": step}
        for step in STEP_ORDER
    ]
    for item in files:
        nodes.append({
            "id": f"file:{item['rel_path']}",
            "type": "rule",
            "label": item["file"],
            "rel_path": item["rel_path"],
            "category": item["category"],
            "draft_exists": item["draft_exists"],
        })
    edges = []
    for step, items in loads.items():
        for idx, item in enumerate(items):
            edges.append(_edge(
                f"{step}:{idx}:{item['rel_path']}",
                f"step:{step}",
                f"file:{item['rel_path']}",
                step,
                item["category"],
                idx,
                item["reason"],
                item.get("conditional", False),
            ))
    issues = []
    reaction_items = list(loads.get("reaction", []))
    reaction_paths = {item["rel_path"] for item in reaction_items}
    if "protocol/base/memory.md" not in reaction_paths:
        issues.append({
            "id": "reaction_missing_memory_contract",
            "severity": "red",
            "message": "当前 reaction 不加载 memory.md；记忆写入已迁入反应步，但记忆行为契约不可见。",
            "source": "历史 Spec158 场景装配模型（当前 Runtime 已退役）",
            "file": "protocol/base/memory.md",
            "step": "reaction",
        })
    relation_scene_items = [
        item for item in reaction_items
        if item["rel_path"] == "protocol/base/relation.md"
        and item.get("category") == "periodic"
    ]
    if relation_scene_items:
        issues.append({
            "id": "reaction_relation_keyword_overlap",
            "severity": "red",
            "message": "relation.md 仍由场景层触发进入 reaction；当前应作为固定层关系契约，不再按“关系焦点”场景驱动。",
            "source": "rules_registry.json + rules fixed-layer migration",
            "file": "protocol/base/relation.md",
            "step": "reaction",
        })
    return {
        "nodes": nodes,
        "edges": edges,
        "step_loads": loads,
        "issues": issues,
    }


def build_workbench_index(repo_root: str | Path | None = None) -> dict[str, Any]:
    registry = load_registry(repo_root)
    files = list_rule_files(repo_root)
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
                "layer": mount.get("layer", item.get("layer", "")),
                "scope": mount.get("scope", item.get("scope", "")),
                "step": mount.get("step", item.get("step", "")),
                "trigger": mount.get("trigger", item.get("trigger", "")),
                "load": mount.get("load", item.get("load", "")),
                "sections": mount.get("sections", item.get("sections", "")),
                "description": mount.get("description", item.get("description", "")),
                "order": mount.get("order", item.get("order", 0)),
            })
            groups.setdefault(grouped["category"], []).append(grouped)
    return {
        "schema_version": "rules_workbench.v1",
        "registry_version": registry.get("_version", ""),
        "rules_root": str(default_rules_root(repo_root)),
        "draft_root": str(default_draft_root(repo_root)),
        "diff_log": str(default_diff_log_path(repo_root)),
        "categories": list(CATEGORIES),
        "files": files,
        "groups": groups,
        "graph": build_graph(repo_root),
    }


def load_rule_document(rel_path: str, repo_root: str | Path | None = None) -> dict[str, Any]:
    rel_path = normalize_rule_path(rel_path)
    rules_root = default_rules_root(repo_root)
    draft_root = default_draft_root(repo_root)
    production_path = _safe_join(rules_root, rel_path)
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


def make_unified_diff(before: str, after: str, fromfile: str, tofile: str) -> str:
    if before == after:
        return ""
    return "".join(difflib.unified_diff(
        before.splitlines(keepends=True),
        after.splitlines(keepends=True),
        fromfile=fromfile,
        tofile=tofile,
    ))


def save_rule_draft(rel_path: str, content: str, note: str = "",
                    repo_root: str | Path | None = None) -> dict[str, Any]:
    rel_path = normalize_rule_path(rel_path)
    document = load_rule_document(rel_path, repo_root)
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

    updated = load_rule_document(rel_path, repo_root)
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
