import json
from pathlib import Path


def _write_doc_fixture(repo_root: Path) -> None:
    docs_root = repo_root / "UPSP" / "OS" / "persona" / "docs"
    base = docs_root / "protocol" / "base"
    persona = docs_root / "persona"
    base.mkdir(parents=True)
    persona.mkdir(parents=True)
    registry = {
        "_version": "DDS v0.19.1",
        "inject": [
            {
                "file": "dynamic.md",
                "path": "protocol/base/dynamic.md",
                "target": "STATUSBAR",
                "trigger": "every_round",
                "description": "动态六轴定义",
            },
        ],
        "lookup": [
            {
                "file": "interaction.md",
                "path": "protocol/base/interaction.md",
                "usage": "交互感受词→动态六轴Δ值（脚本查表）；词条由 memory_write 参数说明近位注入",
                "description": "交互感受词表",
            },
            {
                "file": "tools.md",
                "path": "protocol/base/tools.md",
                "usage": "工具注册表短索引与查表参考",
                "description": "工具注册表",
            },
            {
                "file": "schema.md",
                "path": "protocol/base/schema.md",
                "usage": "JSON schema校验 + 三步输出模板",
                "description": "schema",
            },
        ],
        "popup": {
            "guide": [
                {
                    "file": "tools.md",
                    "path": "protocol/base/tools.md",
                    "tier": "guide",
                    "usage": "协议/通用工具完整 guide 源",
                    "source_mode": "template",
                    "description": "工具 guide 源",
                },
                {
                    "file": "schema.md",
                    "path": "protocol/base/schema.md",
                    "tier": "guide",
                    "usage": "三步输出 guide 源",
                    "source_mode": "template",
                    "description": "本步输出 guide 源",
                },
            ],
            "reminder": [
                {
                    "file": "popup.md",
                    "path": "protocol/base/popup.md",
                    "tier": "reminder",
                    "usage": "POPUP reminder 模板源",
                    "source_mode": "template",
                    "description": "提醒模板",
                },
            ],
            "warning": [
                {
                    "file": "popup.md",
                    "path": "protocol/base/popup.md",
                    "tier": "warning",
                    "usage": "POPUP warning 模板源",
                    "source_mode": "template",
                    "description": "警告模板",
                },
            ],
        },
        "persona": [
            {
                "file": "glossary.md",
                "path": "persona/glossary.md",
                "consume": "runtime_growth",
                "description": "位格术语表",
            },
        ],
    }
    (docs_root / "docs_registry.json").write_text(
        json.dumps(registry, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    for rel in [
        "protocol/base/dynamic.md",
        "protocol/base/interaction.md",
        "protocol/base/tools.md",
        "protocol/base/schema.md",
        "protocol/base/popup.md",
        "persona/glossary.md",
        "protocol/base/unregistered.md",
    ]:
        (docs_root / rel).write_text(f"# {rel}\n\nbody\n", encoding="utf-8")


def test_docs_workbench_lists_current_docs_files():
    from data.docs_workbench import default_docs_root, list_doc_files, load_registry

    docs_root = default_docs_root()
    registry = load_registry()
    registered = {
        entry.get("path")
        for category in ("inject", "lookup", "persona")
        for entry in registry.get(category, [])
        if entry.get("path")
    }
    for entries in registry.get("popup", {}).values():
        registered.update(
            entry.get("path")
            for entry in entries
            if entry.get("path")
        )
    expected = sorted(
        {
            path.relative_to(docs_root).as_posix()
            for path in docs_root.rglob("*.md")
        }
        | registered
    )
    actual = sorted(item["rel_path"] for item in list_doc_files())

    assert expected
    assert actual == expected


def test_docs_registry_has_current_counts():
    from data.docs_workbench import load_registry

    registry = load_registry()

    assert registry.get("_version") == "DDS v0.19.1"
    assert len(registry.get("inject", [])) == 4
    assert len(registry.get("lookup", [])) == 15
    assert set(registry.get("popup", {})) == {"guide", "reminder", "warning"}
    assert len(registry.get("persona", [])) == 4


def test_docs_workbench_no_longer_flags_resolved_docs_registry_items():
    from data.docs_workbench import build_workbench_index

    index = build_workbench_index()
    issues = index["issues"]
    issue_ids = {issue["id"] for issue in issues}

    assert "docs_registry_feeling_projection_deprecated" not in issue_ids
    assert "docs_registry_lookup_mixes_guide_source" not in issue_ids


def test_docs_workbench_groups_popup_tiers():
    from data.docs_workbench import build_workbench_index

    index = build_workbench_index()
    popup_items = index["groups"]["popup"]
    by_tier = {}
    for item in popup_items:
        by_tier.setdefault(item.get("tier"), set()).add(item["rel_path"])

    assert "protocol/base/tools.md" in by_tier["guide"]
    assert "protocol/base/schema.md" in by_tier["guide"]
    assert "protocol/base/popup.md" in by_tier["reminder"]
    assert "protocol/base/popup.md" in by_tier["warning"]
    assert "handoff" not in by_tier


def test_docs_workbench_builds_consumption_graph_from_registry(tmp_path):
    from data.docs_workbench import build_graph

    _write_doc_fixture(tmp_path)
    graph = build_graph(tmp_path)
    edges = {(edge["source"], edge["target"]) for edge in graph["edges"]}
    issue_ids = {issue["id"] for issue in graph["issues"]}

    assert ("file:protocol/base/dynamic.md", "consumer:STATUSBAR") in edges
    assert ("file:protocol/base/interaction.md", "consumer:LOOKUP") in edges
    assert ("file:protocol/base/tools.md", "consumer:POPUP:guide") in edges
    assert ("file:protocol/base/schema.md", "consumer:POPUP:guide") in edges
    assert not any(target == "consumer:POPUP:handoff" for _, target in edges)
    assert ("file:persona/glossary.md", "consumer:runtime_growth") in edges
    assert "docs_registry_feeling_projection_deprecated" not in issue_ids
    assert "docs_registry_lookup_mixes_guide_source" not in issue_ids


def test_docs_workbench_save_draft_does_not_modify_production_and_logs_diff(tmp_path):
    from data.docs_workbench import (
        default_diff_log_path,
        default_draft_root,
        load_doc_document,
        save_doc_draft,
    )

    _write_doc_fixture(tmp_path)
    rel_path = "protocol/base/tools.md"
    docs_root = tmp_path / "UPSP" / "OS" / "persona" / "docs"
    production_path = docs_root / rel_path
    before = production_path.read_text(encoding="utf-8")

    result = save_doc_draft(
        rel_path,
        "# tools draft\n\nnew body\n",
        "test note",
        tmp_path,
    )

    assert production_path.read_text(encoding="utf-8") == before
    assert (default_draft_root(tmp_path) / rel_path).read_text(encoding="utf-8") == "# tools draft\n\nnew body\n"
    assert result["source"] == "draft"
    assert result["draft_exists"] is True
    assert result["current_diff"]

    log_lines = default_diff_log_path(tmp_path).read_text(encoding="utf-8").splitlines()
    assert len(log_lines) == 1
    record = json.loads(log_lines[0])
    assert record["rel_path"] == rel_path
    assert record["note"] == "test note"
    assert "new body" in record["diff"]

    loaded = load_doc_document(rel_path, tmp_path)
    assert loaded["source"] == "draft"
    assert loaded["content"] == "# tools draft\n\nnew body\n"


def test_docs_workbench_rejects_path_traversal(tmp_path):
    from data.docs_workbench import load_doc_document

    _write_doc_fixture(tmp_path)

    for bad_path in ("../secret.md", "protocol/../secret.md", "/abs.md", "C:/x.md"):
        try:
            load_doc_document(bad_path, tmp_path)
        except ValueError:
            pass
        else:
            raise AssertionError(f"path should be rejected: {bad_path}")


def test_docs_workbench_html_supports_markdown_tables():
    html_path = Path(__file__).resolve().parents[1] / "audit" / "docs_workbench.html"
    html = html_path.read_text(encoding="utf-8")

    assert "function renderTable(startIndex)" in html
    assert "<table><thead><tr>" in html
    assert ".markdown table" in html
    assert ".markdown th, .markdown td" in html
    assert ".markdown blockquote" in html
    assert "const ordered =" in html
