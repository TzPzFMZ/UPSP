from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
TEMPLATE = ROOT / "UPSP" / "initialization" / "persona_template"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_spec756_tool_counts_and_permission_tiers() -> None:
    from logic.native_tool_calls import export_provider_tool_schemas

    limited = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
        include_step_terminal_tools=["reaction_finalize"],
        execution_permission_level="limited",
    )
    guarded = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
        include_step_terminal_tools=["reaction_finalize"],
        execution_permission_level="guarded",
    )
    unlimited = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
        include_step_terminal_tools=["reaction_finalize"],
        execution_permission_level="unlimited",
    )
    limited_names = {item["name"] for item in limited}
    guarded_names = {item["name"] for item in guarded}
    unlimited_names = {item["name"] for item in unlimited}

    assert len(limited) == 20
    assert len(guarded) == len(unlimited) == 24
    assert "memory_search" in limited_names
    assert "shell_command" not in limited_names
    assert "shell_command" in guarded_names == unlimited_names


def test_spec756_memory_search_material_and_fact_are_split() -> None:
    from engines.reaction_helpers import (
        format_protocol_tool_fact,
        format_protocol_tool_material_entry,
    )

    receipt = {
        "tool_id": "memory_search",
        "status": "accepted",
        "query_terms": ["figurines"],
        "offset": 0,
        "limit": 8,
        "total_matches": 1,
        "next_offset": None,
        "content": "MEM-00112233 定位片段：figurines were bought",
    }

    material = format_protocol_tool_material_entry(receipt)
    fact = format_protocol_tool_fact(receipt)

    assert material["kind"] == "material"
    assert material["content"] == receipt["content"]
    assert "figurines were bought" not in fact
    assert "figurines" not in fact
    assert "候选总数：1" in fact
    assert "片段不是事实证据" in fact
    assert "memory_content_read" in fact


def test_spec756_permanent_precision_and_container_contracts() -> None:
    memory = _read(TEMPLATE / "rules" / "protocol" / "base" / "memory.md")
    containers = _read(TEMPLATE / "rules" / "protocol" / "base" / "containers.md")
    obligations = _read(ROOT / "UPSP" / "OS" / "logic" / "reaction_obligations.py")

    assert "请求的精度维度尚未满足" in memory
    assert "近似候选提前回答" in memory
    assert "精确日期、原话、轻量事实或任一多跳要素" in memory
    assert "取得对应证据或明确报告覆盖边界" in memory
    assert "DC 与 EC 必须分别独立检查" in containers
    assert "已经建立 PRJ/FUT/EC 就省略 DC" in containers
    assert "已经建立 PRJ/FUT/DC 就省略 EC" in containers
    assert "合法 PRJ/FUT 共存不算过度链接" in containers
    assert "分别检查 DC、EC、PRJ、FUT" in obligations
    assert "不值得本轮挂接" not in obligations
    assert "可以处理，也可以直接自然语言回复用户" not in obligations
