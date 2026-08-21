import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]
RULES_ROOT = ROOT / "UPSP" / "initialization" / "persona_template" / "rules"
DOCS_ROOT = ROOT / "UPSP" / "initialization" / "persona_template" / "docs"
TEMPLATE_ROOT = ROOT / "UPSP" / "initialization" / "persona_template"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_spec753_permanent_rules_are_strong_but_bounded() -> None:
    registry = json.loads(_read(RULES_ROOT / "rules_registry.json"))
    memory = _read(RULES_ROOT / "protocol" / "base" / "memory.md")
    containers = _read(RULES_ROOT / "protocol" / "base" / "containers.md")
    setup = _read(RULES_ROOT / "protocol" / "base" / "setup.md")

    assert [len(registry[name]) for name in ("permanent", "passive_read", "on_demand")] == [8, 8, 4]
    permanent = {item["path"]: item for item in registry["permanent"]}
    assert permanent["protocol/base/memory.md"]["load"] == "full"
    assert permanent["protocol/base/containers.md"]["load"] == "full"
    assert registry["step_level"] == []
    assert registry["periodic"] == []

    for phrase in (
        "主体 × 独立耐久事实",
        "稳定主体与可区分的事件、对象、变化或结论",
        "用户直接陈述、工具或原文核验结果与模型推断",
        "一次性指令、临时格式要求和当前任务步骤",
        "主体及别名、对象、动作、时间、地点、结果与约束",
        "问题要素—`MEM-*`—来源证据",
        "应先用 `container_read` 读取容器关系骨架",
    ):
        assert phrase in memory
    assert "已有记忆覆盖" not in memory
    assert "已有类似条目时优先复用" not in _read(ROOT / "UPSP_Base_DDS.md")
    assert "人物姓名、别名与关系主体" in memory
    assert "汇总其 `created_instance_id` 作为候选来源分身" in memory

    for phrase in (
        "工作容器不是可有可无的装饰",
        "模型必须主动创建、复用或续写对应容器",
        "DC：形成以后仍可复用的多步推演",
        "EC：同一事件出现有序状态变化",
        "PRJ：出现需要跨轮延续的目标",
        "FUT：出现需要未来核验的预测",
        "单一孤立事实、一次性草稿推理、临时任务步骤和无证据关系不得建链",
        "同一种语义职责内只维护一条主链",
        "不同职责可以由 DC/EC/PRJ/FUT 等不同类型同时承接",
        "同一 `MEM-*` 也可作为真实桥接节点",
        "不要求 Runtime 因未建链而硬阻 finalize",
        "也不产生跨轮路由债务",
    ):
        assert phrase in containers

    for phrase in (
        "多步骤或多来源材料",
        "PRJ 本身代表跨轮任务，因此必为 true",
        "单轮直接闭合的 `memory_write` 或 DC/EC/FUT",
        "不得据此豁免整个任务",
    ):
        assert phrase in setup


def test_spec753_tool_headers_sync_semantics_without_wire_changes() -> None:
    from logic.native_tool_calls import export_provider_tool_schemas

    tools = export_provider_tool_schemas(
        provider="openai_responses",
        include_protocol_writes=True,
        include_step_terminal_tools=["reaction_finalize"],
        execution_permission_level="unlimited",
    )
    assert len(tools) == 25
    by_name = {item["name"]: item for item in tools}
    assert {"memory_search", "shell_command"} <= set(by_name)

    memory = by_name["memory_write"]["parameters"]
    assert set(memory["properties"]) == {
        "title",
        "weight",
        "subject",
        "body",
        "candidate_keywords",
        "interaction_feelings",
        "relationship_feelings",
        "reason",
    }
    assert memory["required"] == ["title", "weight", "subject", "body", "candidate_keywords"]
    assert "稳定主体+可区分" in memory["properties"]["title"]["description"]
    assert "工具/原文核验与模型推断" in memory["properties"]["body"]["description"]
    assert "稳定实体及别名" in memory["properties"]["candidate_keywords"]["description"]
    assert "泛词占位" in memory["properties"]["candidate_keywords"]["description"]

    create = by_name["memory_container_create"]
    write = by_name["memory_container_write"]
    assert set(create["parameters"]["properties"]) == {
        "mem_id",
        "container_type",
        "title",
        "skill_category",
        "skill_name",
        "target_file",
        "container_body",
        "current_overview",
        "reason",
    }
    assert set(write["parameters"]["properties"]) == {
        "mem_id",
        "container_id",
        "target_file",
        "title",
        "container_body",
        "current_overview",
        "reason",
    }
    type_help = create["parameters"]["properties"]["container_type"]["description"]
    assert "DC=可复用推演/判断修正" in type_help
    assert "孤立事实、一次性草稿和临时步骤不建容器" in type_help
    assert "同一主题或 MEM 可按不同职责分别进入不同类型" in type_help
    assert "仅在永久合同的持久关系条件成立" in create["description"]
    assert "同类型同职责容器" in create["description"]
    assert "不同持久职责可以由不同类型容器共同承接" in create["description"]
    assert "不因同批记忆或标题相似机械追加" in write["description"]


def test_spec753_keeps_memory_route_pending_soft_and_nonpersistent() -> None:
    source = _read(ROOT / "UPSP" / "OS" / "logic" / "reaction_obligations.py")
    assert "MEMORY_ROUTE_SOFT_PROMPT_LIMIT = 3" in source
    assert "分别检查 DC、EC、PRJ、FUT" in source
    assert "确实均不满足永固触发条件" in source
    assert "deferred/open" in source
    assert "future_anchor_pending" in source
    assert "memory_route_debt" not in source


def test_spec753_dds_and_passive_docs_match_the_permanent_contract() -> None:
    dds = _read(ROOT / "UPSP_Base_DDS.md")
    containers_doc = _read(DOCS_ROOT / "protocol" / "base" / "containers.md")
    popup_doc = _read(DOCS_ROOT / "protocol" / "base" / "popup.md")
    schema_doc = _read(DOCS_ROOT / "protocol" / "base" / "schema.md")
    shapes_doc = _read(DOCS_ROOT / "protocol" / "base" / "shapes.md")
    index_template = _read(TEMPLATE_ROOT / "STM" / "memory" / "index.md")

    assert "\n**版本**：DDS v" in dds
    assert "## 25.12 永固主动建链纪律" in dds
    assert "模型语义扩词与多候选聚合纪律（Spec740/753）" in dds
    assert "宿主仍只做确定性字面扫描" in dds
    assert "不新增 finalize 硬阻或跨轮路由债务" in dds
    assert "任务债务边界（Spec753/754 复审）" in dds
    assert "不同职责可由 DC/EC/PRJ/FUT 等不同类型并存" in dds
    assert "### 主动建链判定" in containers_doc
    assert "同一种语义职责复用同类型主链" in containers_doc
    assert "单轮直接闭合的 `memory_write` 或 DC/EC/FUT" in popup_doc
    assert "用户请求本身要求多步骤/多来源研究" in schema_doc
    assert "内部工具步骤不形成任务债务" in schema_doc
    assert "创建轮/最后调用轮" in shapes_doc
    assert "创建轮/最后调用轮" in index_template
    assert "Base层24字段" in shapes_doc
    assert "入库轮/最后调用轮" not in shapes_doc
    assert "入库轮/最后调用轮" not in index_template
    assert "Base层21字段" not in shapes_doc
