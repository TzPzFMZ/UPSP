import json
import os


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False), encoding="utf-8")


def test_ltm_provenance_is_shared_while_container_notes_are_branch_local(
    tmp_path, monkeypatch
):
    from data import memory_store as ms

    ltm_root = tmp_path / "meta" / "persona" / "LTM" / "Memory"
    shared_meta = ltm_root / "Full" / "meta.json"
    shared_body = ltm_root / "Full" / "full.md"
    overlay_a = tmp_path / "meta" / "persona" / "LTM" / "memory_links.json"
    overlay_b = tmp_path / "branch" / "persona" / "LTM" / "memory_links.json"
    mem_id = "MEM-5A4ED001"
    _write_json(shared_meta, {
        mem_id: {
            "id": mem_id,
            "access": "public",
            "title": "共享来源",
            "weight": 5,
            "created_round": 1,
            "created_instance_id": "meta",
            "last_recalled_round": 1,
            "last_recalled_instance_id": "meta",
        }
    })
    shared_body.write_text(
        f"## {mem_id}  [F]  权重5\n**标题**：共享来源\n**内容**：共享正文\n",
        encoding="utf-8",
    )
    tier_files = {
        "FULL": (shared_meta, shared_body),
        "SUMMARY": (ltm_root / "Summary" / "meta.json", ltm_root / "Summary" / "summary.md"),
        "ABSTRACT": (ltm_root / "Abstract" / "meta.json", ltm_root / "Abstract" / "abstract.md"),
        "PINNED": (ltm_root / "Pinned" / "meta.json", ltm_root / "Pinned" / "pinned.md"),
        "BACKUP": (ltm_root / "Backup" / "meta.json", ltm_root / "Backup" / "backup.md"),
    }
    for tier, (meta_path, body_path) in tier_files.items():
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        if tier != "FULL":
            _write_json(meta_path, {})
            body_path.write_text("", encoding="utf-8")
        monkeypatch.setattr(ms, f"LTM_{tier}_META_JSON", str(meta_path), raising=False)
        body_name = tier.lower() if tier != "SUMMARY" else "summary"
        body_attr = f"LTM_{tier}_{body_name.upper()}_MD"
        monkeypatch.setattr(ms, body_attr, str(body_path), raising=False)
        monkeypatch.setattr(ms.runtime_paths, f"LTM_{tier}_META_JSON", str(meta_path))
        monkeypatch.setattr(ms.runtime_paths, body_attr, str(body_path))
        index_path = meta_path.with_name("index.md")
        monkeypatch.setattr(ms.runtime_paths, f"LTM_{tier}_INDEX_MD", str(index_path))
    monkeypatch.setattr(
        ms,
        "LTM_META_PATHS",
        {
            os.path.abspath(meta_path)
            for tier, (meta_path, _body_path) in tier_files.items()
            if tier != "BACKUP"
        },
    )
    monkeypatch.setattr(ms, "LTM_MEMORY_LINKS_JSON", str(overlay_a))
    monkeypatch.setattr(ms, "ACTIVE_INSTANCE_ID", "I20260810-120000-AAAA")

    store = ms.MemoryStore()
    store.update_linked_containers(
        mem_id, "set", ["PRJ-A"], current_overview="branch A"
    )
    store.mark_recalled(mem_id, round_num=8)
    shared = json.loads(shared_meta.read_text(encoding="utf-8"))[mem_id]
    assert shared["last_recalled_round"] == 8
    assert shared["last_recalled_instance_id"] == "I20260810-120000-AAAA"
    assert "linked_containers" not in shared
    assert "current_overview" not in shared

    monkeypatch.setattr(ms, "LTM_MEMORY_LINKS_JSON", str(overlay_b))
    monkeypatch.setattr(ms, "ACTIVE_INSTANCE_ID", "I20260810-120100-BBBB")
    store.update_linked_containers(
        mem_id, "set", ["PRJ-B"], current_overview="branch B"
    )

    entry_a = json.loads(overlay_a.read_text(encoding="utf-8"))["entries"][mem_id]
    entry_b = json.loads(overlay_b.read_text(encoding="utf-8"))["entries"][mem_id]
    assert entry_a["linked_containers"] == ["PRJ-A"]
    assert entry_a["current_overview"] == "branch A"
    assert entry_b["linked_containers"] == ["PRJ-B"]
    assert entry_b["current_overview"] == "branch B"


def test_shared_update_without_dynamic_fields_preserves_branch_overlay(
    tmp_path, monkeypatch
):
    from data import memory_store as ms

    overlay = tmp_path / "memory_links.json"
    monkeypatch.setattr(ms, "LTM_MEMORY_LINKS_JSON", str(overlay))
    ms.write_memory_overlay_entry("MEM-SHARED1", {
        "linked_containers": ["PRJ-A"],
        "current_overview": "branch note",
        "current_overview_updated_at": "2026-08-10T12:00:00+08:00",
    })

    value = ms.write_memory_overlay_entry("MEM-SHARED1", {
        "title": "shared update only",
    })

    assert value == {
        "linked_containers": ["PRJ-A"],
        "current_overview": "branch note",
        "current_overview_updated_at": "2026-08-10T12:00:00+08:00",
    }
