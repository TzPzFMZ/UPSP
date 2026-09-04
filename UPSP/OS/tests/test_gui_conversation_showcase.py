import re
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
GUI = ROOT / "UPSP" / "gui"
SOURCE = GUI / "src" / "showcase" / "conversation-components.ts"
HTML = GUI / "showcase" / "conversation-components.html"
CSS = GUI / "showcase" / "conversation-components.css"
BUILD = GUI / "scripts" / "build.mjs"
SERVER = ROOT / "tools" / "serve_seed_gui.py"
DESKTOP_BUILD = ROOT / "tools" / "build_windows_desktop.ps1"


def _text(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_spec770_showcase_has_four_groups_and_exactly_three_stable_variants():
    source = _text(SOURCE)
    expected = {
        "thinking": (
            "thinking_inline_disclosure",
            "thinking_auto_card",
            "thinking_phase_timeline",
        ),
        "tools": (
            "tools_compact_records",
            "tools_execution_cards",
            "tools_execution_timeline",
        ),
        "activity": (
            "activity_pulse_dots",
            "activity_breath_timer",
            "activity_stage_rail",
        ),
        "streaming": (
            "streaming_direct_delta",
            "streaming_smoothed_phrases",
            "streaming_block_commit",
        ),
    }
    for component, variant_ids in expected.items():
        assert f'{component}: [' in source
        for variant_id in variant_ids:
            assert source.count(f'id: "{variant_id}"') == 1
    assert 'const COMPONENTS: ComponentKey[] = ["thinking", "tools", "activity", "streaming"]' in source


def test_spec770_showcase_has_all_scenarios_and_terminal_states():
    source = _text(SOURCE)
    for scenario_id in (
        "multi_tool",
        "first_byte_wait",
        "no_reasoning",
        "tool_retry",
        "user_stop",
        "long_markdown",
    ):
        assert source.count(f'id: "{scenario_id}"') == 1
    for state in (
        "idle",
        "connecting",
        "thinking",
        "progressing",
        "tool_running",
        "tool_approval",
        "answering",
        "completed",
        "stopped",
    ):
        assert f'"{state}"' in source
    assert 'type ToolState = "running" | "approval" | "succeeded" | "failed" | "stopped"' in source
    assert "if (elapsedMs <= 0 || event.atMs > elapsedMs) break;" in source
    assert 'const sourceComplete = ["completed", "stopped"].includes(snapshot.stage)' in source


def test_spec770_showcase_is_explicitly_mock_only_and_has_no_network_client():
    html = _text(HTML)
    source = _text(SOURCE)
    css = _text(CSS)
    assert "设计演示 · 模拟数据 · 未连接 Runtime" in html
    for forbidden in (
        "fetch(",
        "XMLHttpRequest",
        "new WebSocket",
        "new EventSource",
        "/api/",
    ):
        assert forbidden not in source
    assert "round_live_state" not in source
    assert "provider SDK" not in source
    assert "../styles.css" not in html
    assert "@import" not in css
    assert "http://" not in html + css
    assert "https://" not in html + css


def test_spec770_showcase_selection_contract_and_no_default_choice():
    source = _text(SOURCE)
    html = _text(HTML)
    assert 'const STORAGE_KEY = "upsp.conversationShowcase.v1"' in source
    assert 'const SELECTION_SCHEMA = "upsp_conversation_showcase_selection.v1"' in source
    assert "let selections: SelectionState = {};" in source
    assert "四组均选择后" in html
    assert "复制选择摘要" in html
    assert "导出 JSON" in html


def test_spec770_showcase_keeps_ordered_reasoning_progress_tools_and_reply_nodes():
    source = _text(SOURCE)
    assert 'createElement("section", `thinking-unit' in source
    assert 'type: "progress" as const' in source
    assert 'segment.kind === "progress" ? "轮中进展" : "最终回复"' in source
    assert 'details.dataset.callId = tool.callId' in source
    assert 'card.dataset.callId = tool.callId' in source
    assert 'createElement("section", "reply-object stream-output' in source
    assert "snapshot.timeline.forEach((item) =>" in source
    assert 'node.dataset.roundEventKey = `${item.kind}:${item.id}`' in source
    assert "function renderRoundStream" in source
    assert "focusedSurfaces[component].forEach" in source
    assert "renderRoundStream(surface, snapshot" in source
    assert "focusedMounts" not in source
    assert "focusedReplyObjects" not in source
    assert "renderCompanionReply" not in source
    assert "function renderTools" not in source
    assert 'label: `思考片段 ${reasoningOrder.length}`' in source
    assert 'segment: "判断"' not in source
    assert 'reasoning: reasoningSegments.map' not in source
    assert ".chat-tool-group" not in source
    assert "innerHTML" not in source


def test_spec770_showcase_all_cards_share_one_round_event_renderer_without_rebuilding_visible_nodes():
    source = _text(SOURCE)
    html = _text(HTML)
    assert 'const timeline = createElement("div", "round-event-stream")' in source
    assert 'timeline.setAttribute("aria-label", "本轮可观察事件时间线")' in source
    assert 'let node = [...timeline.querySelectorAll<HTMLElement>("[data-round-event-key]")]' in source
    assert "timeline.append(node)" in source
    shared_renderer = source[source.index("function renderRoundStream"):source.index("function renderCombination")]
    assert "replaceChildren" not in shared_renderer
    assert "root.className =" not in source
    assert 'root.classList.add("thinking-mount", "thinking-list")' in source
    assert "reasoning、轮中进展、工具调用与最终回复" in html


def test_spec770_thinking_variants_remain_visibly_distinct_after_reasoning_settles():
    source = _text(SOURCE)
    css = _text(CSS)
    assert 'title: "极简折叠行"' in source
    assert 'title: "自动收束预览卡"' in source
    assert 'title: "全事件时间线"' in source
    assert 'preview ? "两行预览"' in source
    assert 'timeline.classList.toggle("is-timeline", useTimeline)' in source
    assert ".thinking-content.is-preview" in css
    assert ".round-event-stream.is-timeline" in css


def test_spec770_showcase_accessibility_and_reduced_motion_contract():
    html = _text(HTML)
    source = _text(SOURCE)
    css = _text(CSS)
    assert 'role="tablist"' in html
    assert html.count('role="tabpanel"') == 5
    assert 'aria-expanded' in source
    assert 'aria-pressed' in source
    assert 'prefers-reduced-motion: reduce' in css
    assert "ArrowLeft" in source and "ArrowRight" in source
    assert "Home" in source and "End" in source


def test_spec770_showcase_styles_are_self_contained():
    css = _text(CSS)
    used = set(re.findall(r"var\((--[A-Za-z0-9-]+)", css))
    defined = set(re.findall(r"(?m)^\s*(--[A-Za-z0-9-]+)\s*:", css))
    assert used <= defined


def test_spec770_showcase_builds_but_is_not_served_or_packaged():
    build = _text(BUILD)
    server = _text(SERVER)
    desktop = _text(DESKTOP_BUILD)
    assert '"showcase", "conversation-components.js"' in build
    assert '"src", "showcase", "conversation-components.ts"' in build
    assert "/showcase/" not in server
    assert "conversation-components" not in desktop
    assert "showcase" not in desktop
