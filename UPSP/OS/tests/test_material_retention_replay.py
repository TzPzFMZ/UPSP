import hashlib
import importlib.util
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "tools" / "replay_material_retention.py"
SPEC = importlib.util.spec_from_file_location("replay_material_retention", MODULE_PATH)
MODULE = importlib.util.module_from_spec(SPEC)
assert SPEC and SPEC.loader
SPEC.loader.exec_module(MODULE)


def test_spec623_material_retention_replay_is_exact_and_deterministic(tmp_path):
    markers = [
        "0037", "0128", "0219", "0311", "0312", "0470",
        "0706", "0930", "1176", "1177", "1178", "SG01-EOF-7F3A",
    ]
    lines = [
        " ".join(markers) + "\n",
        "第二窗口\n",
        "第三窗口\n",
        "第四窗口\n",
        "第五窗口\n",
        "第六窗口\n",
        "第七窗口\n",
    ]
    text = "".join(lines)
    source = tmp_path / "manual.md"
    source.write_text(text, encoding="utf-8", newline="\n")
    source_sha = hashlib.sha256(text.encode("utf-8")).hexdigest()
    verification = tmp_path / "live_verification.json"
    verification.write_text(
        json.dumps({
            "source": {"normalized_sha256": source_sha},
            "live": {"line_ranges": [[index, index] for index in range(1, 8)]},
        }, ensure_ascii=False),
        encoding="utf-8",
    )

    first = MODULE.build_replay(source, verification)
    second = MODULE.build_replay(source, verification)

    assert first == second
    assert first["outcome"] == "GO_LOCAL"
    assert first["checks"] == {
        "seven_windows_retained": True,
        "every_prefix_remained_visible": True,
        "reactions_10_to_14_see_all_windows": True,
        "source_sha_matches_before_settlement": True,
        "source_sha_matches_after_settlement": True,
        "all_required_markers_directly_visible": True,
        "dialogue_progress_not_required": True,
        "corpus_read_not_required": True,
        "material_absent_from_raw_log": True,
        "hidden_read_body_copies_absent": True,
    }
    assert first["body_accounting"]["accepted_material_blocks"] == 7
    assert first["body_accounting"]["accepted_material_chars"] == len(text)
    assert first["settlement"]["visible_material_blocks"] == 7
    assert first["settlement"]["raw_log_material_blocks"] == 0


def test_spec623_memory_card_order_and_hash_ignore_tool_feedback():
    from assembly.context_helpers import build_static_memory_reminder_popup
    from logic.popup_policy import PopupPolicy

    resident_first = build_static_memory_reminder_popup("reaction", 1)
    resident_late = build_static_memory_reminder_popup("reaction", 9)
    warning_a = (
        "- kind: structure_warning\n"
        "  tier: warning\n"
        "  decision_required: false\n"
        "  message: 工具结果 A 需要复核。"
    )
    warning_b = warning_a.replace("结果 A", "结果 B")

    popup_a = PopupPolicy().combine([warning_a, resident_first])
    popup_b = PopupPolicy().combine([resident_late, warning_b])

    def memory_card(text):
        start = text.index("### 记忆提醒")
        end = text.find("\n\n## WARNING｜警告", start)
        return text[start:] if end < 0 else text[start:end]

    card_a = memory_card(popup_a)
    card_b = memory_card(popup_b)
    assert resident_first == resident_late
    assert card_a == card_b
    assert hashlib.sha256(card_a.encode("utf-8")).hexdigest() == hashlib.sha256(
        card_b.encode("utf-8")
    ).hexdigest()
    assert popup_a.index("### 记忆提醒") < popup_a.index("### 结构警告")
    assert popup_b.index("### 记忆提醒") < popup_b.index("### 结构警告")
    assert build_static_memory_reminder_popup("setup", 1) == ""
    assert build_static_memory_reminder_popup("cleanup", 1) == ""
