"""Provider-native tool calling helpers."""
import json

from logic.protocol_tools import TOOL_DEFINITIONS, normalize_tool_id, tool_metadata_for
from logic.execution_permission import (
    DEFAULT_LEVEL as DEFAULT_EXECUTION_PERMISSION_LEVEL,
    normalize_execution_permission_level,
    tool_allowed_by_execution_permission,
)
from logic.runtime_channels import STEP_TERMINAL_TOOLS, STEP_TERMINAL_TOOL_IDS
from logic.write_pending_settlement import CANCEL_REASON_CODES


ENVELOPE_SCHEMA_VERSION = "tool_call_envelope.v1"
ENVELOPE_SOURCE = "provider_tool_call"
PROVIDER_OPENAI_RESPONSES = "openai_responses"
PROVIDER_OPENAI_CHAT = "openai_chat"
PROVIDER_ANTHROPIC_MESSAGES = "anthropic_messages"

ANTHROPIC_FLAT_REACTION_FINALIZE_FIELDS = {
}

SUPPORTED_NATIVE_PROTOCOL_WRITE_TOOLS = {
    "container_focus",
    "guide_submit",
    "memory_container_create",
    "memory_container_write",
    "memory_link_update",
    "mount_cancel",
    "pending_cancel",
    "relay_intent_settle",
    "memory_recall_complete",
    "memory_write",
    "relation_card_write",
}
RETIRED_NATIVE_PROTOCOL_GUIDE_REQUEST_TOOL = "protocol_tool_guide_request"

NATIVE_PROTOCOL_DECLARATION_FIELDS = {
    "container_focus": "container_focus_declarations",
    "guide_submit": "guide_submit_requests",
    "memory_container_create": "memory_container_create_declarations",
    "memory_container_write": "memory_container_write_declarations",
    "memory_link_update": "memory_link_update_declarations",
    "mount_cancel": "mount_cancel_requests",
    "pending_cancel": "pending_cancel_requests",
    "relay_intent_settle": "relay_intent_settle_requests",
    "memory_privacy_declassify": "memory_privacy_declassify_declarations",
    "memory_privacy_mark": "memory_privacy_declarations",
    "memory_recall_complete": "memory_recall_completion_requests",
    "memory_write": "memory_write_declarations",
    "relation_card_write": "relation_card_declarations",
}

NATIVE_DECLARATION_TRACE_KEYS = (
    "call_id",
    "provider",
    "response_id",
    "provider_item_id",
    "index",
)

COMMON_ARGUMENT_PROPERTIES = {
    "path": {"type": "string"},
    "root": {"type": "string"},
    "pattern": {"type": "string"},
    "reason": {"type": "string"},
    "line_start": {"type": "integer"},
    "line_end": {"type": "integer"},
    "char_start": {"type": "integer"},
    "char_end": {"type": "integer"},
    "encoding": {"type": "string"},
    "url": {"type": "string"},
    "query": {"type": "string"},
    "max_results": {"type": "integer"},
    "recursive": {"type": "boolean"},
    "command": {"type": "string"},
    "cwd": {"type": "string"},
    "timeout_ms": {"type": "integer"},
    "purpose": {"type": "string"},
    "risk_level": {"type": "string"},
    "patch": {"type": "string"},
    "task_goal": {"type": "string"},
    "allowed_paths": {"type": "array", "items": {"type": "string"}},
    "expected_artifacts": {"type": "string"},
    "validation_commands": {"type": "string"},
    "task_mode": {"type": "string"},
    "write_scope": {"type": "array", "items": {"type": "string"}},
    "mem_id": {"type": "string"},
    "subject": {"type": "string"},
    "summary": {"type": "string"},
    "body": {"type": "string"},
    "scope": {"type": "string"},
    "zone": {"type": "string"},
    "offset": {"type": "integer"},
    "limit": {"type": "integer"},
    "container_id": {"type": "string"},
    "target_file": {"type": "string"},
}


def _with_description(schema, description=None):
    if description:
        schema = dict(schema)
        schema["description"] = description
    return schema


def _string(description=None):
    return _with_description({"type": "string"}, description)


def _integer(description=None):
    return _with_description({"type": "integer"}, description)


def _boolean(description=None):
    return _with_description({"type": "boolean"}, description)


def _string_array(description=None):
    return _with_description({"type": "array", "items": {"type": "string"}}, description)


def _object_array(properties=None, required=()):
    item = {
        "type": "object",
        "properties": properties or {},
        "required": list(required),
        "additionalProperties": False,
    }
    return {"type": "array", "items": item}


def _object(properties=None, required=()):
    return {
        "type": "object",
        "properties": properties or {},
        "required": list(required),
        "additionalProperties": False,
    }


def _freeform_object(description=None):
    return _with_description(
        {"type": "object", "additionalProperties": True},
        description,
    )


def _enum(values, description=None):
    return _with_description({"type": "string", "enum": list(values)}, description)


def _closed_parameters(properties, required=()):
    return {
        "type": "object",
        "properties": properties,
        "required": list(required),
        "additionalProperties": False,
    }


def _protocol_write_parameters(properties, required=()):
    merged = dict(properties or {})
    merged["resolves_pending_id"] = _string(
        "仅补写失败写入时填 pending_id；成功才结清，失败不另建提醒；首次写入留空。"
    )
    return _closed_parameters(merged, required=required)


CONTAINER_TARGET_FILE_GUIDE = (
    "target_file 按容器类型选择：DC/EC=open.md；"
    "PRJ=plan.md/notes.md；"
    "SKL=card.md；"
    "FUT=objectives.md/plans.md/predictions.md。"
)


def _container_target_file_enum(description_prefix=""):
    description = CONTAINER_TARGET_FILE_GUIDE
    if description_prefix:
        description = f"{description_prefix}；{description}"
    return _enum((
        "open.md",
        "plan.md",
        "notes.md",
        "card.md",
        "objectives.md",
        "plans.md",
        "predictions.md",
    ), description)


def _read_range_properties(default_text="不填任何范围则读全文。"):
    return {
        "line_start": _integer(f"行范围起点；须与 line_end 同填；{default_text}"),
        "line_end": _integer("行范围终点；须与 line_start 同填；不得与字符范围并用。"),
        "char_start": _integer(f"字符范围起点；须与 char_end 同填；{default_text}"),
        "char_end": _integer("字符范围终点；须与 char_start 同填；不得与行范围并用。"),
    }


def _file_read_range_properties():
    return {
        "line_start": _integer(
            "从第几行开始读取；缺省为第 1 行。续读时复制上次回执的 next_line_start 到 line_start。"
        ),
    }


MEMORY_WRITE_WEIGHT_TABLE = (
    "权重表：权重按沉淀价值判断，不按材料来源判断；"
    "1=轻量但非噪音：弱线索、轻量交互、临时背景；"
    "2=一般有效记录：普通任务结果、背景事实、日常事件、有效但不突出的判断；"
    "3=有效沉淀：形成可复用理解、判断、方法、路线感，或完成有价值协作推进；"
    "4=高价值输出、完整方案、深度分析、重要关系变化或可复用工程方法；"
    "5=主体连续性、重大承诺、理论框架或不可丢失的根级事实；"
    "0=连接测试、空泛重复、无来源猜测、无实质短句、纯工具失败噪声，不写。"
)

MEMORY_WRITE_PARAMETER_DESCRIPTIONS = {
    "title": "<=16字；写具体标题，不写“新建记忆条目”。",
    "weight": (
        "整数1-5；权重0不应调用；按沉淀价值而非来源评级，正文长不自动升权。"
        + MEMORY_WRITE_WEIGHT_TABLE
    ),
    "subject": (
        "记忆涉及的活动关系主体，不等同于当前交互对象；可填当前位格自身、当前对象或已登记但缺席对象的稳定 ID/name/alias，处理器保存 Registry 规范 ID。"
        "关系域无卡、歧义或 archived 会拒绝；unknown 仅回退到已有关系卡的当前对象。"
    ),
    "body": (
        "第一人称只写结论与变化；按权重限长："
        "1/2=[A]≤128字，3/4=[S]≤512字，5=[F]≤2048字。"
        "超长先压缩，不写对话/工具流水、数值字段或Δ动态。"
    ),
    "candidate_keywords": (
        "字符串数组且至少1项；每个关键词单列，禁止用分隔符拼成一个字符串。"
        "按初始形态裁剪：F≤8、S≤6、A≤4；脚本不从标题/正文补词。"
    ),
    "interaction_feelings": "交互感受词；只从 schema description 中列出的词条选，最多3个；无则传空数组。",
    "relationship_feelings": (
        "关系感受对象数组；每项填写活动关系卡 subject（稳定 ID/name/alias）和清单内 word，"
        "处理器保存规范 ID；每个对象最多2个，无则传空数组。"
    ),
    "reason": (
        "本次为什么不是权重0噪音、应形成记忆条目；"
        "简短说明证据、主体更新依据、权重或变化依据。"
    ),
}


def _memory_write_parameter_schema():
    return _protocol_write_parameters({
        "title": _string(MEMORY_WRITE_PARAMETER_DESCRIPTIONS["title"]),
        "weight": _integer(MEMORY_WRITE_PARAMETER_DESCRIPTIONS["weight"]),
        "subject": _string(MEMORY_WRITE_PARAMETER_DESCRIPTIONS["subject"]),
        "body": _string(MEMORY_WRITE_PARAMETER_DESCRIPTIONS["body"]),
        "candidate_keywords": _string_array(
            MEMORY_WRITE_PARAMETER_DESCRIPTIONS["candidate_keywords"]
        ),
        "interaction_feelings": _string_array(
            MEMORY_WRITE_PARAMETER_DESCRIPTIONS["interaction_feelings"]
        ),
        "relationship_feelings": _with_description(_object_array({
            "subject": _string("关系感受所属的活动关系卡稳定 ID/name/alias。"),
            "word": _string("只从 memory_write 说明中的关系感受词清单选择。"),
        }, required=("subject", "word")),
            MEMORY_WRITE_PARAMETER_DESCRIPTIONS["relationship_feelings"]
        ),
        "reason": _string(MEMORY_WRITE_PARAMETER_DESCRIPTIONS["reason"]),
    }, required=("title", "weight", "subject", "body", "candidate_keywords"))


TOOL_ARGUMENT_SCHEMAS = {
    "setup_finalize": _closed_parameters({
        "security_verdict": _enum(("pass", "reject")),
        "reject_reason": _string(),
        "mount_requests": _object_array({
            "type": _enum(("memory", "container", "relation_summary", "relation", "skill")),
            "ids": _string(),
            "source": _string(),
        }),
        "rules_selection": _string(),
        "round_type_confirm": _string(),
        "standby_skip_reaction": _boolean(),
        "suggested_mode": _string(),
        "task_guidance_required": _boolean(
            "Set true for nontrivial user tasks that need model-authored decomposition and acceptance before execution: multi-step repo/file/research/debug/test/report/reading work, or any request with materials to read, deliverables, commands, source coverage, output files, validation, memory/report/internalization, or evidence paths. This judgment is still required during rhythm/heartbeat coalesced rounds; true records interaction debt and does not override rhythm priority. Set false only for trivial direct answers, casual chat, a single simple command/status check, or runtime-triggered rhythm/heartbeat work without a user task."
        ),
        "task_guidance_route": _enum(
            ("none", "new_work", "current_work"),
            "Minimal work routing for task_guidance_required: none means no work guide debt, new_work means create a new task bootstrap when no active task blocks it, current_work means integrate this interaction into the current active task pending_inputs."
        ),
        "task_guidance_reason": _string(
            "Short reason for task_guidance_required; when true name the user task deliverable/material and validation need, when false leave empty."
        ),
        "interaction_object": _string("本轮已确认的交互对象；无法确认时留空或填 unknown，由反应步继续确认。"),
        "identity_status": _enum(
            ("known", "declared", "unknown", "timeout"),
            "本轮身份入口状态；known/declared 表示可作为内部 interaction_meta 入口确认。"
        ),
        "interaction_source": _string("身份入口确认来源，例如 context_continuity / self_declaration。"),
        "interaction_basis": _string("身份入口确认依据；写短句，不写长推理。"),
    }, required=("security_verdict",)),
    "reaction_finalize": _closed_parameters({
        "handoff_text": _string(
            "必填；只在需要跨轮继续时调用本工具，写一段自然语言交接，说明下一轮应接着做什么。"
        ),
    }, required=("handoff_text",)),
    "guide_submit": _protocol_write_parameters({
        "guide_id": _string("按当前清单显示的 guide_id 提交；具体填法看清单顶部说明。"),
        "item_id": _string(
            "单条提交快捷方式：当前清单显示的 item_id。"
        ),
        "option_id": _string(
            "单条提交快捷方式：当前清单显示的 option_id。"
        ),
        "fields": _freeform_object(
            "单条提交快捷方式：按当前清单说明填写；多余顶层键会并入 fields 后校验。"
        ),
        "evidence_refs": _string_array("可选证据引用；具体要求看当前清单顶部说明。"),
        "reason": _string("可选简短原因。"),
        "submissions": _object_array({
            "item_id": _string("当前清单显示的 item_id。"),
            "option_id": _string("当前清单显示的 option_id。"),
            "fields": _freeform_object("按当前清单说明填写。"),
            "evidence_refs": _string_array("可选证据引用。"),
            "reason": _string("可选简短原因。"),
        }, required=("item_id", "option_id")),
    }, required=("guide_id",)),
    "cleanup_finalize": _closed_parameters({
        "connection_bridges": _object_array({
            "word_a": _string(),
            "entry_a": _string(),
            "word_b": _string(),
            "entry_b": _string(),
            "note": _string(),
        }),
        "tacit_associations": _object_array({
            "item_id": _string(),
            "item_type": _string(),
            "action": _enum(("kept", "dropped", "added")),
            "note": _string(),
            "evidence_refs": _string_array(),
            "drop_reason": _string(),
        }),
        "lately_compression": _object({
            "action": _enum(("keep", "replace", "drop")),
            "replacement_text": _string(),
            "reason": _string(),
        }),
    }),
    "file_read": _closed_parameters({
        "path": _string(),
        **_file_read_range_properties(),
        "encoding": _string(),
        "reason": _string(),
    }, required=("path",)),
    "file_search": _closed_parameters({
        "root": _string("要搜索的目录；必须在 workspace read allowlist 内。"),
        "pattern": _string("文件名 glob 模式，例如 `共格主体论*` 或 `*.md`；不要传路径穿越。"),
        "recursive": _boolean("是否递归搜索子目录；默认 false，只有模型显式声明时才递归。"),
        "max_results": _integer("结果窗口上限；程序会 clamp 到安全范围。"),
        "reason": _string(),
    }, required=("root", "pattern")),
    "file_edit": _closed_parameters({
        "path": _string(),
        "patch": _string(),
        "purpose": _string(),
        "reason": _string(),
    }, required=("path", "patch", "purpose")),
    "file_write": _closed_parameters({
        "path": _string(),
        "content": _string(),
        "purpose": _string(),
        "encoding": _string(),
        "risk_level": _string(),
        "reason": _string(),
    }, required=("path", "content", "purpose")),
    "web_fetch": _closed_parameters({
        "url": _string(),
        "char_start": _integer("bounded 网页正文续读游标；工具事实给出 next_char_start 时填写。窗口大小由系统配置决定。"),
        "reason": _string(),
    }, required=("url",)),
    "web_search": _closed_parameters({
        "query": _string(),
        "reason": _string(),
    }, required=("query",)),
    "shell_command": _closed_parameters({
        "command": _string(
            "要执行的 Windows shell 命令；默认不是 Bash，不能使用 python - <<'PY' 这类 POSIX here-doc。"
            "多行 Python 优先用 file_write 写临时 .py 后执行，或使用 PowerShell here-string 管道。"
        ),
        "purpose": _string("为什么需要执行该命令；必须说明具体验证、诊断或生成目的。"),
        "cwd": _string("命令工作目录；必须在当前工作区或任务级 shell_cwd 授权根内。"),
        "timeout_ms": _integer(),
        "risk_level": _string(),
        "reason": _string(),
    }, required=("command", "purpose")),
    "subagent_dispatch": _closed_parameters({
        "task_goal": _string(),
        "allowed_paths": _string_array(),
        "expected_artifacts": _string(),
        "validation_commands": _string(),
        "task_mode": _enum(("read_only", "code_change", "verification")),
        "write_scope": _string_array(),
        "reason": _string(),
    }, required=("task_goal", "allowed_paths", "expected_artifacts")),
    "index_view": _closed_parameters({
        "scope": _enum((
            "ltm_heat",
            "stm_heat",
            "skills_inverted",
            "ltm_inverted",
            "stm_inverted",
            "association",
            "relation_inverted",
            "relation_domain",
        ), "只选择上方枚举中的索引窗口；不提供容器注册表视图。"),
        "zone": _enum(("self", "ours", "them", "orgs")),
        "offset": _integer(),
        "limit": _integer(),
        "reason": _string(),
    }, required=("scope",)),
    "corpus_read": _closed_parameters({
        "corpus_id": _string("当前上下文里可见的轮中进展语料短ID，例如 C-00001。"),
    }, required=("corpus_id",)),
    "relation_read": _closed_parameters({
        "card_id": _string(),
        "subject": _string(),
        "summary": _enum(("none", "temporary", "resident")),
        "body": _enum(("none", "temporary", "resident")),
        **_read_range_properties(),
        "reason": _string(),
    }),
    "memory_content_read": _closed_parameters({
        "mem_id": _string(),
        "mount_mode": _enum(("temporary", "resident", "none")),
        **_read_range_properties(),
        "reason": _string(),
    }, required=("mem_id",)),
    "container_read": _closed_parameters({
        "container_id": _string(
            "必须填写容器索引中真实列出的具体容器编号；"
            "EC、DC、PRJ、SKL、FUT 只是容器类型，不能当成容器编号。"
        ),
        "target_file": _enum((
            "open.md",
            "plan.md",
            "notes.md",
            "card.md",
            "objectives.md",
            "plans.md",
            "predictions.md",
        )),
        **_read_range_properties(),
        "reason": _string(),
    }, required=("container_id",)),
    "mount_cancel": _closed_parameters({
        "mount_area": _enum((
            "focus",
            "resident_list",
            "instant_list",
        ), "只取消内容窗口三路之一；不会删除源正文或通用工具结果。"),
        "item_type": _enum((
            "auto",
            "memory",
            "container",
            "relation",
            "relation_summary",
        )),
        "item_id": _string(
            "要取消的稳定对象 ID；mount_area=focus 时可留空，表示当前 WB focus。"
        ),
        "reason": _string("为什么取消该挂载。"),
    }, required=("mount_area",)),
    "pending_cancel": _closed_parameters({
        "pending_id": _string("要取消的失败写入 pending 编号；它也是 POPUP 中的失败写入提醒 ID。"),
        "reason_code": _enum(CANCEL_REASON_CODES, "为什么取消这次失败写入意图。"),
        "note": _string("一句自然语言说明，给下一迭代的自己看；不要复述写入正文。取消不是补写。"),
    }, required=("pending_id", "reason_code")),
    "relay_intent_settle": _closed_parameters({
        "relay_intent_id": _string("要结算的中继意图 ID。"),
        "status": _enum((
            "completed",
            "merged",
            "question",
            "deferred",
        ), "完成、合题、反问或搁置。"),
        "note": _string("一句中文说明，只写本次中继意图如何处理。"),
    }, required=("relay_intent_id", "status")),
    "memory_write": _memory_write_parameter_schema(),
    "memory_link_update": _protocol_write_parameters({
        "mem_id": _string(),
        "operation": _enum(("add", "remove", "set")),
        "container_refs": _string_array(),
        "current_overview": _string(),
        "reason": _string(
            "历史修复工具；正常挂接路径不再使用 add/set，应改用 memory_container_create 或 memory_container_write。remove 保留用于移除错误旧挂接。"
        ),
    }, required=("mem_id", "operation", "container_refs")),
    "memory_container_create": _protocol_write_parameters({
        "mem_id": _string("真实 MEM-*；不接受 PENDING。"),
        "container_type": _enum((
            "DC",
            "EC",
            "PRJ",
            "SKL",
            "FUT",
        ), "容器类型会决定 target_file 合法值；DC/EC=open.md，PRJ=plan.md/notes.md，SKL=card.md，FUT=objectives.md/plans.md/predictions.md。"),
        "title": _string("新容器标题。"),
        "skill_category": _enum((
            "procedures",
            "patterns",
        ), "仅 container_type=SKL 时填写；Seed 只创建源技能 procedures/patterns。"),
        "skill_name": _string(
            "仅 container_type=SKL 时填写；小写字母、数字与单连字符，最长64字符；用于 SKL-{category}-{skill_name}。"
        ),
        "target_file": _container_target_file_enum("新容器正文落点"),
        "container_body": _string(
            "新容器首段正文；不是复制 MEM，而是基于 MEM 引用源组织出的连续正文。"
        ),
        "current_overview": _string(
            "MEM 当前在容器中的位置概况，<=128字；可用 {container_id} 占位，Runtime 创建后替换。"
        ),
        "reason": _string("为什么需要新建容器并把该 MEM 作为引用源。"),
    }, required=(
        "mem_id",
        "container_type",
        "title",
        "target_file",
        "container_body",
        "current_overview",
        "reason",
    )),
    "memory_container_write": _protocol_write_parameters({
        "mem_id": _string("真实 MEM-*；不接受 PENDING。"),
        "container_id": _string("本迭代入口已可见的 WB focus 容器。"),
        "target_file": _container_target_file_enum("按当前 focus 容器类型选择"),
        "title": _string("本段容器正文标题。"),
        "container_body": _string(
            "写入当前 focus 的连续正文；必须基于已可见 focus 投影和本轮 MEM 引用源组织。"
        ),
        "current_overview": _string("MEM 当前在该容器中的位置概况，<=128字，需含 container_id。"),
        "reason": _string("为什么把该 MEM 挂接写入当前 focus 容器。"),
    }, required=(
        "mem_id",
        "container_id",
        "target_file",
        "title",
        "container_body",
        "current_overview",
        "reason",
    )),
    "memory_recall_complete": _protocol_write_parameters({
        "mem_id": _string(),
        "completed_body": _string(),
        "reason": _string(),
    }, required=("mem_id", "completed_body")),
    "memory_privacy_mark": _protocol_write_parameters({
        "mem_id": _string(),
        "privacy_subject": _string(
            "当前确认在场的活动关系对象；Runtime 规范化后把完整条目迁入 {规范ID}.private.md，不改 memory subject。"
        ),
        "basis": _enum(("user_explicit", "context_confidential", "retroactive")),
        "body_action": _enum(
            ("move_private",),
            "当前仅支持 move_private；成功后公共正文副本消失。",
        ),
        "reason": _string(),
    }, required=("mem_id", "privacy_subject", "body_action")),
    "memory_privacy_declassify": _protocol_write_parameters({
        "mem_id": _string(),
        "mode": _enum(("declassify", "redact", "delete", "keep_private")),
        "redacted_body": _string(),
        "reason": _string(),
    }, required=("mem_id", "mode")),
    "relation_card_write": _protocol_write_parameters({
        "name": _string("关系对象名称；更新已有关系卡前，必须已经在上一轮工具回执后看见对应 relation_read(body) CONTENT。"),
        "subject": _string("关系对象名称的兼容字段；与 name 同义。"),
        "card_id": _string("已有关系卡 ID；更新已有卡前，必须已经看见该卡正文 CONTENT。"),
        "category": _enum(("self", "ours", "them", "orgs")),
        "action": _enum(("create", "update", "append_note"), "create 仅用于目标关系卡不存在；目标已存在时必须先 relation_read(body)，下一次模型调用再 update/append_note。"),
        "note": _string("自然语言关系内容；不得填写轴数值、状态数值或脚本派生字段。"),
        "summary": _string("可选短摘要；不得替代正文读取纪律。"),
        "reason": _string("为什么本次关系卡写入有必要。"),
    }),
    "chronicle_write": _protocol_write_parameters({
        "content": _string("只填写当前编年史焦点的正文；层级、轮次、范围和状态统计由 Runtime 预填。"),
        "reason": _string("为什么本次正文可以收束当前编年史焦点。"),
    }, required=("content", "reason")),
    "alert_mode_settle": _protocol_write_parameters({
        "alert_type": _enum((
            "api_degraded",
            "token_usage_warning",
            "context_pressure",
            "standby_due",
        )),
        "status": _enum(("recovered", "deferred", "needs_human")),
        "summary": _string(),
        "clear_flags": _string_array(),
        "fault_refs": _string_array(),
        "next_attention": _string(),
        "reason": _string(),
    }, required=(
        "alert_type",
        "status",
        "summary",
        "clear_flags",
        "fault_refs",
        "next_attention",
        "reason",
    )),
    "fault_record": _protocol_write_parameters({
        "fault_type": _enum((
            "tool_failure",
            "parse_failure",
            "external_dependency",
            "api_degraded",
            "data_format",
            "runtime_exception",
        )),
        "severity": _enum(("info", "warning", "error", "critical")),
        "step": _enum(("setup", "reaction", "cleanup", "heartbeat", "runtime")),
        "source": _string(),
        "detail": _string(),
        "action": _enum((
            "ignored",
            "retried",
            "fallback",
            "emergency_save",
            "needs_review",
        )),
        "related_tool_id": _string(),
    }, required=("fault_type", "severity", "step", "source", "detail")),
    "container_focus": _protocol_write_parameters({
        "action": _enum(("open", "close", "restore")),
        "container_id": _string(
            "open 或 close 时必须填写容器索引中真实列出的具体容器编号；"
            "EC、DC、PRJ、SKL、FUT 只是容器类型，不能当成容器编号；restore 可留空。"
        ),
        "reason": _string("为什么要调整 WB focus。"),
    }, required=("action",)),
}


def provider_for_url(url, endpoint_config=None):
    endpoint_config = endpoint_config or {}
    explicit = str(
        endpoint_config.get("tool_call_provider")
        or endpoint_config.get("provider")
        or endpoint_config.get("api_format")
        or ""
    ).strip()
    if explicit in {PROVIDER_OPENAI_RESPONSES, "responses", "openai-responses"}:
        return PROVIDER_OPENAI_RESPONSES
    if explicit in {PROVIDER_OPENAI_CHAT, "chat", "openai-chat"}:
        return PROVIDER_OPENAI_CHAT
    if explicit in {
        PROVIDER_ANTHROPIC_MESSAGES,
        "anthropic",
        "messages",
        "anthropic-messages",
    }:
        return PROVIDER_ANTHROPIC_MESSAGES

    text = str(url or "").split("?", 1)[0].rstrip("/")
    if text.endswith("/responses"):
        return PROVIDER_OPENAI_RESPONSES
    if text.endswith("/messages"):
        return PROVIDER_ANTHROPIC_MESSAGES
    return PROVIDER_OPENAI_CHAT


def extract_tool_call_envelopes(response_data, provider, endpoint):
    response_data = response_data or {}
    if provider == PROVIDER_OPENAI_RESPONSES:
        raw_calls = _responses_tool_calls(response_data)
        replay_items = _responses_replay_items(response_data)
    elif provider == PROVIDER_OPENAI_CHAT:
        raw_calls = _chat_tool_calls(response_data)
        replay_items = []
    elif provider == PROVIDER_ANTHROPIC_MESSAGES:
        raw_calls = _anthropic_tool_calls(response_data)
        replay_items = []
    else:
        raw_calls = []
        replay_items = []
    return [
        _build_envelope(
            response_data,
            provider,
            endpoint,
            raw_call,
            index,
            replay_items=replay_items,
        )
        for index, raw_call in enumerate(raw_calls)
    ]


def build_provider_response_meta(response_data, provider, envelopes):
    response_data = response_data or {}
    finish_reason = str(response_data.get("finish_reason") or "")
    choices = response_data.get("choices") or []
    if not finish_reason and choices:
        finish_reason = str((choices[0] or {}).get("finish_reason") or "")
    if not finish_reason:
        finish_reason = str(response_data.get("stop_reason") or "")
    if not finish_reason:
        finish_reason = str(response_data.get("status") or "")
    return {
        "provider": str(provider or ""),
        "response_id": str(response_data.get("id") or ""),
        "finish_reason": finish_reason,
        "raw_tool_call_count": len(envelopes or []),
    }


def export_provider_tool_schemas(provider=PROVIDER_OPENAI_RESPONSES,
                                 include_protocol_writes=False,
                                 include_step_terminal_tools=None,
                                 include_standard_tools=True,
                                 active_protocol_tool_guides=None,
                                 execution_permission_level=None):
    permission_level = normalize_execution_permission_level(
        execution_permission_level,
        default=DEFAULT_EXECUTION_PERMISSION_LEVEL,
    )
    step_terminal_tools = {
        normalize_tool_id(tool_id)
        for tool_id in include_step_terminal_tools or []
        if normalize_tool_id(tool_id)
    }
    schemas = []
    for tool_id in sorted(TOOL_DEFINITIONS):
        meta = tool_metadata_for(tool_id)
        if not _is_exportable_tool(
                tool_id,
                meta,
                include_protocol_writes,
                step_terminal_tools,
                include_standard_tools,
                active_protocol_tool_guides,
                execution_permission_level=permission_level):
            continue
        schemas.append(_provider_tool_schema_item(provider, _tool_schema(tool_id, meta)))
    return schemas


def apply_native_tool_calls_to_parsed_reaction(
        parsed_reaction,
        envelopes,
        native_mode=False,
        active_protocol_tool_guides=None):
    if not native_mode and not envelopes:
        return parsed_reaction or {}
    parsed = dict(parsed_reaction or {})
    if native_mode:
        parsed["assistant_reply"] = ""
        parsed["assistant_progress"] = ""
        parsed["tool_summaries"] = []
        parsed["reaction_loop"] = {}
    invalids = list(parsed.get("invalid_tool_requests", []) or [])
    invalids.extend(_text_requests_retired_by_native_mode(
        parsed.get("protocol_tool_requests", []),
        parsed.get("general_tool_requests", []),
    ))
    parsed["protocol_tool_requests"] = []
    parsed["general_tool_requests"] = []
    parsed["protocol_tool_submissions"] = []
    parsed["invalid_protocol_tool_submissions"] = []
    parsed["invalid_tool_requests"] = invalids
    parsed["native_protocol_tool_submissions"] = []
    for field in NATIVE_PROTOCOL_DECLARATION_FIELDS.values():
        parsed[field] = []
    valid_request_seen = False
    focus_tool_seen = None

    for envelope in sorted(envelopes or [], key=lambda item: item.get("index", 0)):
        if not isinstance(envelope, dict):
            continue
        envelope = _normalize_native_envelope_arguments(envelope)
        if envelope.get("parse_status") != "ok":
            parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(envelope))
            continue
        if normalize_tool_id(envelope.get("tool_id", "")) == RETIRED_NATIVE_PROTOCOL_GUIDE_REQUEST_TOOL:
            parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
                envelope,
                reason="protocol_tool_guide_request_retired",
            ))
            continue
        request = _request_from_envelope(envelope)
        family = envelope.get("tool_family") or request.get("tool_family")
        if family == "general_tool":
            validation_error = _native_argument_validation_error(envelope)
            if validation_error:
                parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
                    envelope,
                    reason=validation_error.get("reason"),
                    details=validation_error,
                ))
                continue
            parsed["general_tool_requests"].append(request)
            valid_request_seen = True
            continue
        if family == "protocol_tool":
            if tool_metadata_for(envelope.get("tool_id", "")).get("status") == "disabled":
                parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
                    envelope,
                    reason="feature_deferred",
                ))
                continue
            if _is_native_protocol_write(envelope):
                tool_id = normalize_tool_id(envelope.get("tool_id", ""))
                validation_error = _native_argument_validation_error(envelope)
                if validation_error:
                    parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
                        envelope,
                        reason=validation_error.get("reason"),
                        details=validation_error,
                    ))
                    continue
                if envelope.get("tool_class") == "focus_tool":
                    if focus_tool_seen:
                        parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
                            envelope,
                            reason="focus_tool_iteration_conflict",
                            details={"accepted_focus_tool": focus_tool_seen},
                        ))
                        continue
                    focus_tool_seen = tool_id
                _append_native_protocol_submission(parsed, envelope)
                valid_request_seen = True
                continue
            if envelope.get("tool_class") in {"sync_tool", "focus_tool"}:
                parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
                    envelope,
                    reason="native_protocol_write_not_enabled",
                ))
                continue
            validation_error = _native_argument_validation_error(envelope)
            if validation_error:
                parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
                    envelope,
                    reason=validation_error.get("reason"),
                    details=validation_error,
                ))
                continue
            parsed["protocol_tool_requests"].append(request)
            valid_request_seen = True
            continue
        parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
            envelope,
            reason="unsupported_tool_family",
        ))

    if valid_request_seen:
        parsed["exit_signal"] = "waiting_tool"
    elif parsed.get("exit_signal") in {"waiting_tool", "continue_reaction"}:
        parsed["exit_signal"] = "done"
    return parsed


def _normalize_native_envelope_arguments(envelope):
    tool_id = normalize_tool_id((envelope or {}).get("tool_id", ""))
    if tool_id != "file_read":
        return envelope
    arguments = envelope.get("arguments")
    if not isinstance(arguments, dict):
        return envelope
    cleaned = dict(arguments)
    for field in ("cursor", "line_end", "char_start", "char_end", "max_chars"):
        cleaned.pop(field, None)
    if cleaned == arguments:
        return envelope
    normalized = dict(envelope)
    normalized["arguments"] = cleaned
    normalized["arguments_json"] = json.dumps(
        cleaned, ensure_ascii=False, sort_keys=True)
    return normalized


def split_step_terminal_envelopes(envelopes, step):
    step = str(step or "").strip().lower()
    terminal_tool_id = STEP_TERMINAL_TOOLS.get(step)
    ordinary = []
    terminals = []
    invalids = []
    for envelope in envelopes or []:
        if not isinstance(envelope, dict):
            continue
        tool_id = normalize_tool_id(envelope.get("tool_id", ""))
        if tool_id == terminal_tool_id:
            terminals.append(envelope)
        elif tool_id in STEP_TERMINAL_TOOL_IDS:
            invalids.append(_invalid_request_from_envelope(
                envelope,
                reason="step_terminal_wrong_step",
            ))
        elif step != "reaction":
            invalids.append(_invalid_request_from_envelope(
                envelope,
                reason="step_non_reaction_tool_not_allowed",
            ))
        else:
            ordinary.append(envelope)
    return ordinary, terminals, invalids


def project_step_finalize(step, arguments):
    step = str(step or "").strip().lower()
    arguments = arguments if isinstance(arguments, dict) else {}
    if step == "setup":
        return _project_setup_finalize(arguments)
    if step == "reaction":
        return _project_reaction_finalize(arguments)
    if step == "cleanup":
        return _project_cleanup_finalize(arguments)
    return {}


def terminal_finalize_from_envelopes(envelopes, step):
    ordinary, terminals, invalids = split_step_terminal_envelopes(envelopes, step)
    if not terminals:
        return None, ordinary, invalids
    terminal = sorted(terminals, key=lambda item: item.get("index", 0))[-1]
    terminal = _canonicalize_provider_terminal_arguments(terminal)
    validation_error = None
    if terminal.get("parse_status") != "ok":
        validation_error = {"reason": terminal.get("parse_status") or "invalid_tool_call"}
    else:
        validation_error = _native_argument_validation_error(terminal)
    if validation_error:
        invalids.append(_invalid_request_from_envelope(
            terminal,
            reason=validation_error.get("reason"),
            details=validation_error,
        ))
        return None, ordinary, invalids
    projected = project_step_finalize(step, terminal.get("arguments") or {})
    if isinstance(projected, dict):
        trace = {
            key: terminal.get(key)
            for key in NATIVE_DECLARATION_TRACE_KEYS
            if terminal.get(key) not in (None, "")
        }
        if trace:
            projected[f"{step}_finalize_trace"] = trace
    return projected, ordinary, invalids


def _canonicalize_provider_terminal_arguments(envelope):
    if not isinstance(envelope, dict):
        return envelope
    tool_id = normalize_tool_id(envelope.get("tool_id", ""))
    if tool_id != "reaction_finalize":
        return envelope
    return envelope


def _project_setup_finalize(arguments):
    intent = {
        "mount_requests": [],
        "rules_selection": None,
        "round_type_confirm": None,
        "security_verdict": "pass",
        "reject_reason": None,
        "suggested_mode": None,
        "task_guidance_required": False,
        "task_guidance_route": "none",
        "task_guidance_reason": None,
        "interaction_meta": None,
        "standby_skip_reaction": False,
    }
    verdict = str(arguments.get("security_verdict") or "pass").strip().lower()
    if verdict in {"pass", "reject"}:
        intent["security_verdict"] = verdict
    for item in arguments.get("mount_requests") or []:
        if not isinstance(item, dict):
            continue
        mount_type = str(item.get("type") or "").strip()
        ids = str(item.get("ids") or "").strip()
        if mount_type and ids:
            intent["mount_requests"].append({"type": mount_type, "ids": ids})
    for field in ("rules_selection", "round_type_confirm", "reject_reason",
                  "suggested_mode"):
        value = _clean_optional_text(arguments.get(field))
        if value:
            intent[field] = value
    if isinstance(arguments.get("standby_skip_reaction"), bool):
        intent["standby_skip_reaction"] = arguments.get("standby_skip_reaction")
    if isinstance(arguments.get("task_guidance_required"), bool):
        intent["task_guidance_required"] = arguments.get("task_guidance_required")
    route = _clean_optional_text(arguments.get("task_guidance_route"))
    if route in {"none", "new_work", "current_work"}:
        intent["task_guidance_route"] = route
    task_guidance_reason = _clean_optional_text(
        arguments.get("task_guidance_reason"))
    if task_guidance_reason:
        intent["task_guidance_reason"] = task_guidance_reason
    interaction_meta = _project_setup_interaction_meta(arguments)
    if interaction_meta:
        intent["interaction_meta"] = interaction_meta
    return intent


def _project_setup_interaction_meta(arguments):
    if not isinstance(arguments, dict):
        return None
    interaction_object = _clean_optional_text(arguments.get("interaction_object"))
    identity_status = _clean_optional_text(arguments.get("identity_status"))
    if (
        not interaction_object
        or interaction_object.lower() == "unknown"
        or identity_status not in {"known", "declared", "unknown", "timeout"}
    ):
        return None
    projected = {
        "interaction_object": interaction_object,
        "identity_status": identity_status,
    }
    interaction_source = _clean_optional_text(arguments.get("interaction_source"))
    if interaction_source:
        projected["interaction_source"] = interaction_source
    basis = _clean_optional_text(arguments.get("interaction_basis"))
    if basis:
        projected["basis"] = basis
    return projected


def _project_reaction_finalize(arguments):
    retired_fields = (
        "closeout_decision",
        "final_closeout",
        "relay_closeout",
        "obligation_resolutions",
        "memory_settlement",
        "read_settlement",
        "assistant_reply",
        "memory_no_write_reason",
        "memory_status",
        "memory_reason",
        "read_status",
        "read_reason",
        "pending_status",
        "pending_reason",
        "identity_action",
        "identity_object",
        "identity_status",
        "identity_note",
        "to_next_setup",
        "to_next_reaction",
        "relay_reason",
        "closeout_note",
    )
    finalize_errors = []
    for field in retired_fields:
        if field in arguments:
            finalize_errors.append(f"reaction_finalize.retired_field:{field}")

    handoff_text = _clean_optional_text(arguments.get("handoff_text"))

    if not handoff_text:
        finalize_errors.append("reaction_finalize.handoff_text_required")
    closeout_form = {
        "closeout_decision": "continue",
        "handoff_text": handoff_text,
    }
    parsed = {
        "closeout_form": closeout_form,
        "protocol_tool_requests": [],
        "general_tool_requests": [],
        "protocol_tool_submissions": [],
        "invalid_protocol_tool_submissions": [],
        "invalid_tool_requests": [],
        "native_protocol_tool_submissions": [],
        "reaction_loop": {"reaction_loop_done": True},
        "exit_signal": "done",
    }
    if finalize_errors:
        parsed["reaction_finalize_errors"] = finalize_errors
    for field in NATIVE_PROTOCOL_DECLARATION_FIELDS.values():
        parsed[field] = []
    return parsed


def _project_cleanup_finalize(arguments):
    return {
        "keywords": [],
        "tacit_associations": [
            dict(item) for item in arguments.get("tacit_associations") or []
            if isinstance(item, dict)
        ],
        "connection_bridges": [
            dict(item) for item in arguments.get("connection_bridges") or []
            if isinstance(item, dict)
        ],
        "lately_compression": (
            dict(arguments.get("lately_compression"))
            if isinstance(arguments.get("lately_compression"), dict)
            else {}
        ),
        "state_updates": [],
        "archive_title": "",
        "archive_subject": None,
        "archive_weight": None,
        "archive_body": "",
        "archive_type": "memory",
        "archive_linked": [],
        "faults": "",
        "minimum_commitment": None,
        "output_text": "",
        "has_output": False,
        "raw": "",
    }


def _clean_optional_text(value):
    text = str(value or "").strip()
    if text in {"无", "none", "None", "NONE", "-", "—"}:
        return ""
    return text


def _clean_status_text(value):
    return str(value or "").strip()

def _responses_tool_calls(response_data):
    result = []
    for item in response_data.get("output", []) or []:
        if not isinstance(item, dict) or item.get("type") != "function_call":
            continue
        result.append({
            "provider_item_id": item.get("id") or "",
            "call_id": item.get("call_id") or item.get("id") or "",
            "raw_type": item.get("type") or "function_call",
            "name": item.get("name") or "",
            "arguments": item.get("arguments", "{}"),
        })
    return result


def _responses_replay_items(response_data):
    output = response_data.get("output", []) or []
    if not any(
        isinstance(item, dict) and item.get("type") == "reasoning"
        for item in output
    ):
        return []
    replay_items = []
    for item in output:
        if not isinstance(item, dict):
            continue
        if item.get("type") not in {"reasoning", "function_call"}:
            continue
        replay_items.append(json.loads(json.dumps(item, ensure_ascii=False)))
    return replay_items


def _chat_tool_calls(response_data):
    result = []
    for choice in response_data.get("choices", []) or []:
        message = (choice or {}).get("message") or {}
        for item in message.get("tool_calls", []) or []:
            if not isinstance(item, dict):
                continue
            function = item.get("function") or {}
            call = {
                "provider_item_id": item.get("id") or "",
                "call_id": item.get("id") or "",
                "raw_type": item.get("type") or "function",
                "name": function.get("name") or "",
                "arguments": function.get("arguments", "{}"),
                "native_tool_call": dict(item),
            }
            if "content" in message:
                call["message_content"] = message.get("content")
            if "reasoning_content" in message:
                call["reasoning_content"] = message.get("reasoning_content")
            result.append(call)
    return result


def _anthropic_tool_calls(response_data):
    result = []
    for item in response_data.get("content", []) or []:
        if not isinstance(item, dict) or item.get("type") != "tool_use":
            continue
        result.append({
            "provider_item_id": item.get("id") or "",
            "call_id": item.get("id") or "",
            "raw_type": item.get("type") or "tool_use",
            "name": item.get("name") or "",
            "arguments": item.get("input") or {},
        })
    return result


def _build_envelope(
        response_data,
        provider,
        endpoint,
        raw_call,
        index,
        replay_items=None):
    raw_name = raw_call.get("name") or ""
    tool_id = normalize_tool_id(raw_name)
    arguments, arguments_json, argument_status = _parse_arguments(
        raw_call.get("arguments", "{}"))
    meta = tool_metadata_for(tool_id) or _native_only_tool_metadata(tool_id)
    parse_status = _parse_status(tool_id, meta, argument_status)
    response_id = str((response_data or {}).get("id") or "")
    call_id = str(raw_call.get("call_id") or "")
    if not call_id:
        call_id = f"{provider}:{response_id or 'response'}:{index}:{tool_id or 'unknown'}"
    raw_provider_item_id = str(raw_call.get("provider_item_id") or "")
    provider_item_id = raw_provider_item_id or call_id
    tool_family = meta.get("tool_family", "")
    tool_class = meta.get("tool_class", "")
    envelope = {
        "schema_version": ENVELOPE_SCHEMA_VERSION,
        "source": ENVELOPE_SOURCE,
        "provider": str(provider or ""),
        "endpoint": str(endpoint or ""),
        "response_id": response_id,
        "call_id": call_id,
        "provider_item_id": provider_item_id,
        "index": int(index),
        "raw_type": str(raw_call.get("raw_type") or ""),
        "tool_id": tool_id,
        "arguments": arguments,
        "arguments_json": arguments_json,
        "tool_family": tool_family,
        "tool_class": tool_class,
        "risk": meta.get("risk", ""),
        "parse_status": parse_status,
        "requires_guide": False,
        "audit_projection": f"原生工具调用：{tool_id or raw_name}",
    }
    if not raw_provider_item_id:
        envelope["provider_item_id_is_synthetic"] = True
    if "message_content" in raw_call:
        envelope["message_content"] = raw_call.get("message_content")
    if "reasoning_content" in raw_call:
        envelope["reasoning_content"] = raw_call.get("reasoning_content")
    if isinstance(raw_call.get("native_tool_call"), dict):
        envelope["native_tool_call"] = dict(raw_call.get("native_tool_call"))
    return envelope


def _request_from_envelope(envelope):
    request = dict(envelope.get("arguments") or {})
    for key in (
            "tool_id",
            "tool_family",
            "tool_class",
            "risk",
            "source",
            "call_id",
            "provider",
            "response_id",
            "provider_item_id",
            "index"):
        value = envelope.get(key)
        if value not in (None, ""):
            request[key] = value
    return request


def _is_native_protocol_write(envelope):
    return (
        envelope.get("tool_family") == "protocol_tool"
        and envelope.get("tool_class") in {"sync_tool", "focus_tool"}
        and envelope.get("tool_id") in SUPPORTED_NATIVE_PROTOCOL_WRITE_TOOLS
    )


def _native_argument_validation_error(envelope):
    tool_id = normalize_tool_id(envelope.get("tool_id", ""))
    schema = TOOL_ARGUMENT_SCHEMAS.get(tool_id)
    if not schema:
        return {
            "reason": "native_argument_schema_missing",
            "field": "tool_id",
            "expected": "tool_argument_schema",
            "actual": tool_id or "missing",
        }
    arguments = envelope.get("arguments")
    if not isinstance(arguments, dict):
        return {
            "reason": "native_argument_invalid_type",
            "field": "arguments",
            "expected": "object",
            "actual": _json_type_name(arguments),
        }
    properties = schema.get("properties") or {}
    if tool_id == "reaction_finalize" and "closeout_decision" in arguments:
        return {
            "reason": "reaction_finalize_retired_field",
            "field": "closeout_decision",
            "expected": sorted(properties),
            "actual": "closeout_decision",
        }
    for field in schema.get("required") or []:
        if field not in arguments:
            return {
                "reason": "native_argument_missing_required",
                "field": field,
                "expected": "required",
                "actual": "missing",
            }
    if schema.get("additionalProperties") is False:
        for field in arguments:
            if field not in properties:
                if _is_flat_guide_submit_argument(tool_id, arguments):
                    continue
                return {
                    "reason": "native_argument_unknown_field",
                    "field": field,
                    "expected": sorted(properties),
                    "actual": field,
                }
    for field, value in arguments.items():
        property_schema = properties.get(field)
        if not property_schema:
            continue
        expected_type = property_schema.get("type")
        if expected_type and not _matches_json_type(value, expected_type, property_schema):
            detail = {
                "reason": "native_argument_invalid_type",
                "field": field,
                "expected": _expected_type_label(expected_type, property_schema),
                "actual": _actual_type_label(value, property_schema),
            }
            preview = _argument_value_preview_for_feedback(tool_id, field, value)
            if preview:
                detail["actual_value_preview"] = preview
            return detail
        enum_values = property_schema.get("enum")
        if enum_values and value not in enum_values:
            return {
                "reason": "native_argument_invalid_enum",
                "field": field,
                "expected": list(enum_values),
                "actual": value,
            }
    return None


def _is_flat_guide_submit_argument(tool_id, arguments):
    if tool_id != "guide_submit" or not isinstance(arguments, dict):
        return False
    if isinstance(arguments.get("submissions"), list):
        return False
    return bool(
        str(arguments.get("item_id") or "").strip()
        and str(arguments.get("option_id") or "").strip()
    )


def _matches_json_type(value, expected_type, property_schema):
    if expected_type == "string":
        return isinstance(value, str)
    if expected_type == "integer":
        return isinstance(value, int) and not isinstance(value, bool)
    if expected_type == "boolean":
        return isinstance(value, bool)
    if expected_type == "object":
        return isinstance(value, dict)
    if expected_type == "array":
        if not isinstance(value, list):
            return False
        item_schema = property_schema.get("items") or {}
        item_type = item_schema.get("type")
        if item_type == "string":
            return all(isinstance(item, str) for item in value)
        if item_type == "object":
            return all(isinstance(item, dict) for item in value)
        return True
    return True


def _expected_type_label(expected_type, property_schema):
    if expected_type == "array":
        item_type = (property_schema.get("items") or {}).get("type")
        if item_type:
            return "array<{}>".format(item_type)
    return expected_type


def _actual_type_label(value, property_schema):
    if isinstance(value, list):
        item_type = (property_schema.get("items") or {}).get("type")
        if item_type:
            item_types = sorted({_json_type_name(item) for item in value})
            return "array<{}>".format(",".join(item_types))
    return _json_type_name(value)


def _json_type_name(value):
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, str):
        return "string"
    if isinstance(value, int) and not isinstance(value, bool):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return type(value).__name__


def _argument_value_preview_for_feedback(tool_id, field, value):
    if tool_id == "memory_write" and field == "candidate_keywords" and isinstance(value, str):
        text = " ".join(value.replace("|", "/").split())
        if len(text) > 160:
            return text[:157] + "..."
        return text
    return ""


def _append_native_protocol_submission(parsed, envelope):
    tool_id = normalize_tool_id(envelope.get("tool_id", ""))
    field = NATIVE_PROTOCOL_DECLARATION_FIELDS.get(tool_id)
    if not field:
        parsed["invalid_tool_requests"].append(_invalid_request_from_envelope(
            envelope,
            reason="native_protocol_write_not_enabled",
        ))
        return
    parsed.setdefault("protocol_tool_submissions", []).append(tool_id)
    parsed.setdefault("native_protocol_tool_submissions", []).append(tool_id)
    parsed.setdefault(field, []).append(_declaration_from_envelope(envelope))


def _declaration_from_envelope(envelope):
    declaration = dict(envelope.get("arguments") or {})
    for key in NATIVE_DECLARATION_TRACE_KEYS:
        value = envelope.get(key)
        if value not in (None, ""):
            declaration[key] = value
    return declaration


def _invalid_request_from_envelope(envelope, reason=None, details=None):
    item = {}
    for key in (
            "tool_id",
            "tool_family",
            "tool_class",
            "risk",
            "source",
            "call_id",
            "provider",
            "response_id",
            "provider_item_id",
            "index"):
        value = envelope.get(key)
        if value not in (None, ""):
            item[key] = value
    item["reason"] = reason or envelope.get("parse_status") or "invalid_tool_call"
    for key in ("field", "expected", "actual", "actual_value_preview"):
        value = (details or {}).get(key)
        if value not in (None, "", []):
            item[key] = value
    return item


def _text_requests_retired_by_native_mode(protocol_requests, general_requests):
    invalids = []
    for request in list(protocol_requests or []) + list(general_requests or []):
        if isinstance(request, dict):
            tool_id = normalize_tool_id(request.get("tool_id", ""))
        else:
            tool_id = normalize_tool_id(request)
            request = {}
        meta = tool_metadata_for(tool_id)
        invalids.append({
            "tool_id": tool_id,
            "tool_family": meta.get("tool_family", request.get("tool_family", "")),
            "tool_class": meta.get("tool_class", request.get("tool_class", "")),
            "risk": meta.get("risk", request.get("risk", "")),
            "reason": "native_tool_call_required",
            "source": "text_tool_request",
        })
    return invalids


def _parse_arguments(raw_arguments):
    if isinstance(raw_arguments, dict):
        return dict(raw_arguments), json.dumps(
            raw_arguments, ensure_ascii=False, sort_keys=True), "ok"
    arguments_json = str(raw_arguments or "{}")
    try:
        parsed = json.loads(arguments_json)
    except json.JSONDecodeError:
        return {}, arguments_json, "invalid_json"
    if not isinstance(parsed, dict):
        return {}, arguments_json, "schema_invalid"
    if any(not isinstance(key, str) or not key.isascii() for key in parsed):
        return parsed, arguments_json, "schema_invalid"
    return parsed, arguments_json, "ok"


def _parse_status(tool_id, meta, argument_status):
    if argument_status != "ok":
        return argument_status
    if normalize_tool_id(tool_id) == RETIRED_NATIVE_PROTOCOL_GUIDE_REQUEST_TOOL:
        return "ok"
    if not tool_id or not meta:
        return "unknown_tool_id"
    if meta.get("tool_family") == "substrate_tool":
        return "unsupported_tool_family"
    return "ok"


def _native_only_tool_metadata(tool_id):
    if normalize_tool_id(tool_id) != RETIRED_NATIVE_PROTOCOL_GUIDE_REQUEST_TOOL:
        return {}
    return {
        "tool_family": "runtime_tool",
        "tool_class": "guide_request",
        "domain": "protocol",
        "risk": "medium",
        "status": "enabled",
    }


def _is_exportable_tool(
        tool_id,
        meta,
        include_protocol_writes,
        step_terminal_tools=None,
        include_standard_tools=True,
        active_protocol_tool_guides=None,
        execution_permission_level=None):
    step_terminal_tools = step_terminal_tools or set()
    if (
        meta
        and meta.get("native_only")
        and (meta.get("step_terminal") or meta.get("step_runtime"))
    ):
        return tool_id in step_terminal_tools
    if not include_standard_tools:
        return False
    if not meta or meta.get("tool_family") == "substrate_tool":
        return False
    if meta.get("tool_family") == "general_tool":
        return (
            meta.get("status") == "enabled"
            and tool_allowed_by_execution_permission(
                tool_id,
                normalize_execution_permission_level(
                    execution_permission_level,
                    default=DEFAULT_EXECUTION_PERMISSION_LEVEL,
                ),
            )
        )
    if meta.get("tool_family") == "protocol_tool":
        if meta.get("status") == "disabled":
            return False
        if meta.get("tool_class") == "read_tool":
            return True
        return bool(
            include_protocol_writes
            and tool_id in SUPPORTED_NATIVE_PROTOCOL_WRITE_TOOLS
        )
    return False


def _provider_tool_schema_item(provider, schema):
    if provider == PROVIDER_OPENAI_CHAT:
        return {"type": "function", "function": schema}
    if provider == PROVIDER_ANTHROPIC_MESSAGES:
        return {
            "name": schema.get("name", ""),
            "description": schema.get("description", ""),
            "input_schema": schema.get("parameters", _closed_parameters({})),
        }
    item = {"type": "function"}
    item.update(schema)
    return item


def _memory_write_feeling_words_description():
    try:
        from logic.feeling_lookup import build_feeling_guide
        return build_feeling_guide()
    except Exception:
        return ""


def _memory_write_tool_description():
    parts = [
        (
            "UPSP memory_write：创建不可覆写的正式记忆条目。"
            "非噪音主体更新（偏好、边界、关系、判断、方法、环境约束等）应主动考虑写入，"
            "无需等待用户要求；日常事实和轻量变化可用低权重。"
            "权重0不调用，每轮建议不超过3条。字段细则见参数；不直接挂接容器。"
            "只有下一迭代 applied 回执及 MEM-* 才证明写入；"
            "任务未闭合仍可写阶段性发现，但不得写成已完成结论。"
            "不得传 dream；感受词仅从下列清单选择，无则空数组。"
        ),
    ]
    feeling_words = _memory_write_feeling_words_description()
    if feeling_words:
        parts.append(feeling_words)
    else:
        parts.append("感受词清单：[]")
    return "\n\n".join(parts)


REACTION_TOOL_DESCRIPTIONS = {
    "container_focus": "焦点工具：打开、关闭或恢复 WB 当前容器；同一迭代只保持一个焦点。",
    "container_read": "只读工具：按真实 container_id 读取容器正文，可选目标文件与行/字符范围；不改变 WB focus。",
    "corpus_read": "只读工具：按当前可见 corpus_id 读取轮中进展语料；不写入或挂载。",
    "file_edit": (
        "高风险文件编辑；仅放行档下发。用 unified diff patch 修改已有 tracked/allowlist 文本文件，"
        "必填 path/patch/purpose；新建或整文件覆盖用 file_write。"
        "越权、位格真源、Git/密钥、未跟踪目标或无效 patch 由 Runtime 拒绝；仅 status=ok 证明生效。"
    ),
    "guide_submit": "同步工具：按当前 guide_id/条目坐标提交清单状态或证据；只以处理器回执为准。",
    "index_view": "只读工具：查看指定记忆、技能、关联或关系索引窗口；不提供容器注册表。",
    "memory_container_create": (
        "焦点工具：以真实 MEM-* 为引用源新建 DC/EC/PRJ/SKL/FUT 容器、写首段正文并替换 WB focus；"
        "SKL 只开放 procedures/patterns 源技能；"
        "成功回执才证明创建。"
    ),
    "memory_container_write": (
        "焦点工具：把真实 MEM-* 挂接写入当前已可见 WB focus 容器并更新概况；"
        "无 focus 或失败回执不得声称写入。"
    ),
    "memory_content_read": "只读工具：读取真实 MEM-* 正文；mount_mode 决定临时/常驻挂载或不挂载，不改正文和索引。",
    "memory_link_update": (
        "同步历史修复工具：仅 remove 用于移除错误旧挂接；"
        "正常新挂接改用 memory_container_create/write。"
    ),
    "memory_privacy_declassify": (
        "同步工具：对真实 MEM-* 执行 declassify/redact/delete/keep_private；"
        "高风险结果可能 needs_review。"
    ),
    "memory_privacy_mark": (
        "同步工具：把真实 MEM-* 迁入当前确认对象的 {规范ID}.private.md；"
        "文件有首条条目时才创建，不改 memory subject，只以处理器回执为准。"
    ),
    "memory_recall_complete": "同步工具：基于已读证据补全真实 MEM-* 正文；不是普通读取，成功回执才生效。",
    "mount_cancel": (
        "同步工具：仅取消 focus/resident_list/instant_list 挂载；"
        "不删除源记忆、容器、关系或通用工具结果。"
    ),
    "pending_cancel": "同步工具：取消一次失败写入意图；不补写、不证明原写入成功。",
    "relation_card_write": (
        "同步工具：创建或更新关系卡；已有卡必须先 relation_read(body) 后再 update/append_note，"
        "禁止填写轴数值。"
    ),
    "relation_read": "只读工具：按 card_id/subject 读取关系摘要或正文，可选行/字符范围；不修改关系卡。",
    "relay_intent_settle": (
        "同步工具：把既有 relay_intent_id 结算为 completed/merged/question/deferred；不创建新中继。"
    ),
    "subagent_dispatch": (
        "高风险子 agent 派发；仅放行档下发。必填 task_goal/allowed_paths/expected_artifacts，"
        "只派发边界清晰且可独立执行的子任务；code_change 必填 write_scope 且不得超出 allowed_paths。"
        "缺范围、越权或 backend_unavailable 由 Runtime 拒绝；返回结果前不得声称完成。"
    ),
}


def _tool_schema(tool_id, meta):
    if tool_id == "memory_write":
        description = _memory_write_tool_description()
    elif tool_id == "setup_finalize":
        description = (
            "UPSP 起手步终端工具；起手步的放行、驳回、挂载请求、身份入口和轮型确认"
            "只能通过 provider-native setup_finalize 生效。"
            "起手步不读取材料、不建任务账本、不执行用户任务；"
            "需要后续任务处理时，只在本工具中声明 task_guidance_required 和 task_guidance_route。"
            "多步骤、工程、资料、检索、调试、测试、报告、读书/长文内化、带产物、验收或证据路径的请求，"
            "task_guidance_required=true；普通闲聊、单个状态查询、单条简单命令、"
            "一次有界只读查询/核验且没有多步骤产物或验收债务、纯 Runtime 节律事项才是 false。"
            "裸文本、旧表格和自然语言判断不生效，不会被解析成起手 intent。"
            "若缺少该工具，Runtime 会把裸文本隔离为 audit 观察，不作为事实或执行证据。"
        )
    elif tool_id == "reaction_finalize":
        description = (
            "反应步跨轮中继工具；仅需下一轮继续时调用，必填 handoff_text。"
            "普通完成直接自然语言回复，模型不选择 finish/blocked。"
            "可与最后一批普通工具同一 response 提交，Runtime 先结算普通工具再结算中继，"
            "并以真实回执、读取游标和 pending tracker 生成 settlement_ledger。"
            "旧收束字段由 closed schema 拒绝。"
        )
    elif tool_id == "cleanup_finalize":
        description = (
            "UPSP 善后步终端工具；善后步只整理训练材料、默契材料和最近缓存压缩。"
            "善后裸文本只进 audit，不作为训练材料、缓存决策或落账输入。"
            "善后不能创建中继或置位 heartbeat flag；需要继续长任务时必须由反应步"
            " reaction_finalize 提交 handoff_text。"
        )
    elif tool_id == "file_read":
        description = (
            "读取允许路径内文件，返回配置驱动的 bounded 完整行窗口及 has_more/next_line_start；"
            "长文续读只复制 next_line_start 到 line_start。必填 path，只可选 line_start。"
        )
    elif tool_id == "file_search":
        description = (
            "UPSP 通用文件搜索工具；在允许目录内按文件名 glob 搜索候选路径。"
            "只返回路径候选，不读取正文；默认不递归，只有 recursive=true 时搜索子目录。"
        )
    elif tool_id == "file_write":
        description = (
            "UPSP 通用文件写入工具；仅在执行权限为放行档时下发。"
            "可在当前工作区内创建或覆盖普通文件；必须填写 path、content 与 purpose。"
            "位格真源、Git 内部数据、密钥类路径和危险目标仍由 Runtime 硬拒绝。"
        )
    elif tool_id == "web_fetch":
        description = (
            "UPSP 通用网页读取工具；读取公开网页正文的配置驱动 bounded 字符窗口。"
            "窗口大小由系统配置决定；工具事实会标注 returned_chars、window_chars、has_more 与 next_char_start。"
        )
    elif tool_id == "web_search":
        description = (
            "UPSP 通用网页搜索工具；返回配置驱动的 bounded 候选来源结果窗口。"
            "搜索结果不是网页正文；需要正文时继续调用 web_fetch。"
        )
    elif tool_id == "shell_command":
        description = (
            "放行档 Windows shell 工具。禁止 POSIX here-doc；"
            "多行 Python 写临时 .py 或用 PowerShell here-string。"
            "危险命令、越权 cwd、后台服务、凭据读取和网络写由 Runtime 拒绝。"
        )
    elif tool_id in REACTION_TOOL_DESCRIPTIONS:
        description = REACTION_TOOL_DESCRIPTIONS[tool_id]
    else:
        description = (
            f"UPSP工具 {tool_id}；"
            f"family={meta.get('tool_family', '')}；"
            f"class={meta.get('tool_class', '')}；"
            f"domain={meta.get('domain', '')}；"
            f"risk={meta.get('risk', '')}。"
        )
    return {
        "name": tool_id,
        "description": description,
        "parameters": TOOL_ARGUMENT_SCHEMAS.get(tool_id, _closed_parameters({})),
    }
