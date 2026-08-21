"""
上下文工程 Schema
DDS §19 上下文工程 + §21 过期标记 + §24 插话机制

文件:
  STM/context/cache/now_cache.jsonl    — 当前缓存语料块主源
  STM/context/cache/lately_cache.jsonl — 最近缓存语料块主源
  STM/buffer/raw_log.jsonl              — lately 接纳后的原始语料缓冲
  LTM/Corpus/public/rhythms/*.jsonl     — 原始节语料机器真源
  LTM/Corpus/public/rhythms/*.md        — 同批人类可读派生件
  STM/context/{step}/            — 审计痕迹（high_freq/step/manifest 等）
  config/context/                — 装配规则 JSON
    permanent.json / periodic.json / lately.json / high_freq.json / now.json / statusbar.json / popup.json
"""
import hashlib

from utils.read_tool_material import read_tool_material_content

READ_TOOL_PRIVATE_DIAGNOSTIC_FIELDS = frozenset({
    "window_strategy",
    "window_configured_max_chars",
    "window_legacy_floor_chars",
    "window_budget_status",
    "window_current_tokens",
    "window_context_window_tokens",
    "window_reserve_tokens",
    "window_safe_room_tokens",
    "window_batch_consumed_before_chars",
    "window_batch_remaining_after_chars",
    "host_path",
    "host_root",
})
READ_TOOL_BODY_FIELDS = {
    "file_read": frozenset({"content"}),
    "file_glob": frozenset({"matches"}),
    "file_grep": frozenset({"matches"}),
    "web_fetch": frozenset({"content", "raw_html", "html", "body"}),
    "web_search": frozenset({"content", "results"}),
}


def context_safe_read_tool_result(result):
    """Strip read bodies and Runtime-only coordinates before cache storage."""
    if not isinstance(result, dict):
        return result
    tool_id = str(result.get("tool_id") or "").strip()
    body_fields = READ_TOOL_BODY_FIELDS.get(tool_id, frozenset())
    if not body_fields and tool_id != "file_read":
        return result
    safe = {
        key: value
        for key, value in result.items()
        if (
            key not in READ_TOOL_PRIVATE_DIAGNOSTIC_FIELDS
            and key not in body_fields
        )
    }
    material_body = read_tool_material_content(result)
    if material_body:
        safe["material_body_sha256"] = hashlib.sha256(
            material_body.encode("utf-8")
        ).hexdigest()
        safe["material_body_chars"] = len(material_body)
    return safe

# ============================================================
# corpus_block JSONL（DDS §19 / Spec 035 / Spec 038）
# ============================================================

CORPUS_BLOCK_FIELDS = {
    "id":     ("str",  "语料块唯一ID"),
    "role":   ("str",  "user/assistant/system"),
    "kind":   ("str",  "interaction/assistant_reply/dialogue_progress/material/tool_fact/setup_fact/relay_handoff/minimum_commitment/fault_note/cache_summary"),
    "text":   ("str",  "正文文本"),
    "loc":    ("dict", "round/step/iter/time"),
    "policy": ("dict", "now/lately"),
    "ref":    ("dict", "追溯信息"),
}

CORPUS_BLOCK_TOP_LEVEL_KEYS = set(CORPUS_BLOCK_FIELDS)
CORPUS_BLOCK_KINDS = {
    "interaction",
    "assistant_reply",
    "dialogue_progress",
    "material",
    "tool_fact",
    "setup_fact",
    "relay_handoff",
    "minimum_commitment",
    "fault_note",
    "cache_summary",
}


# ============================================================
# STM/context/{step}/ 审计痕迹（DDS §19.3）
# ============================================================

# 每个 step 目录下的文件:
#   step.json     — provider_request.v1 脱敏请求信封，request_body 为唯一发送体
#   step.md       — 派生审计文本，不作为机器源反向解析
#   layers/*.json — 分层机器真源
#   layers/*.md   — 分层派生审计渲染
#   manifest.json — 时间戳+各层字符统计+本步装配压力事实+本次 dirty/reused 审计状态

STEP_AUDIT_FILES = [
    "step.md",
    "step.json",
    "manifest.json",
    "layers/00_call_header.json",
    "layers/01_tool_header.json",
    "layers/02_generation_config.json",
    "layers/10_permanent.json",
    "layers/10_permanent.md",
    "layers/20_periodic.json",
    "layers/20_periodic.md",
    "layers/30_lately.json",
    "layers/30_lately.md",
    "layers/40_high_freq.json",
    "layers/40_high_freq.md",
    "layers/50_now.json",
    "layers/50_now.md",
    "layers/60_statusbar.json",
    "layers/60_statusbar.md",
    "layers/99_popup.json",
    "layers/99_popup.md",
]

STEP_AUDIT_MANIFEST_FIELDS = {
    "step":        ("str", "setup/reaction/cleanup"),
    "assembled_at":("str", "ISO时间戳"),
    "layers":      ("dict","{permanent: {chars, sha256, dirty, reused}, ...}"),
    "total_chars": ("int", "总字符数"),
}


# 五模块内容分类标签（DDS §19.1 横向分类，与七层频率梯度正交）
CONTEXT_MODULES = ["STATUSBAR", "EXPLORER", "CONTENT", "RULES", "POPUP"]


# ============================================================
# 校验
# ============================================================

def validate_audit_manifest(manifest):
    """校验审计 manifest.json"""
    errors = []
    for field in STEP_AUDIT_MANIFEST_FIELDS:
        if field not in manifest:
            errors.append(f"manifest 缺字段: {field}")
    if manifest.get("step") not in ("setup", "reaction", "cleanup"):
        errors.append(f"manifest.step 非法值: {manifest.get('step')}")
    return (len(errors) == 0, errors)


def default_popup_content():
    """默认空白 POPUP"""
    return ""
