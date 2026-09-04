import importlib.util
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
MODULE_PATH = ROOT / "tools" / "replay_file_read_window.py"
SPEC = importlib.util.spec_from_file_location("replay_file_read_window", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


def _manual_fixture(lines: int = 1183) -> str:
    return "".join(
        f"line {index:04d}: evidence boundary and deterministic replay.\n"
        for index in range(1, lines + 1)
    )


def test_replay_preserves_exact_text_and_continuous_lines():
    text = _manual_fixture()

    replay = MODULE.replay_text(text, 16384)

    assert replay["exact_text_match"] is True
    assert replay["line_continuity"] is True
    assert replay["within_window_or_overlong_line"] is True
    assert replay["returned_chars_total"] == len(text)
    assert replay["windows"][-1]["has_more"] is False


def test_build_replay_compares_legacy_and_adaptive_shapes(tmp_path):
    target = tmp_path / "long_manual.md"
    text = _manual_fixture() + "SEALGATE_EOF_TOKEN\n"
    target.write_text(text, encoding="utf-8", newline="")

    payload = MODULE.build_replay(
        target,
        configured_max_chars=16384,
        current_tokens=61683,
        context_window=1_000_000,
    )

    assert payload["analysis_boundary"]["provider_called"] is False
    assert payload["source"]["path"] == str(target.resolve())
    assert payload["source"]["eof_marker_present"] is True
    assert payload["adaptive_plan"]["window_chars"] == 16384
    assert payload["baseline"]["window_count"] > payload["adaptive"]["window_count"]
    assert payload["comparison"]["source_sha_matches_baseline"] is True
    assert payload["comparison"]["source_sha_matches_adaptive"] is True
    assert payload["comparison"]["now_body_chars_not_increased"] is True


def test_replay_retains_complete_overlong_line_exception():
    text = "x" * 20000

    replay = MODULE.replay_text(text, 16384)

    assert replay["window_count"] == 1
    assert replay["windows"][0]["line_overlong"] is True
    assert replay["windows"][0]["returned_chars"] > 16384
    assert replay["within_window_or_overlong_line"] is True


def test_build_replay_handles_empty_file_without_division_by_zero(tmp_path):
    target = tmp_path / "empty.md"
    target.write_text("", encoding="utf-8")

    payload = MODULE.build_replay(target)

    assert payload["baseline"]["window_count"] == 0
    assert payload["adaptive"]["window_count"] == 0
    assert payload["baseline"]["exact_text_match"] is True
    assert payload["adaptive"]["exact_text_match"] is True
    assert payload["comparison"]["window_count_reduction_ratio"] == 0.0
