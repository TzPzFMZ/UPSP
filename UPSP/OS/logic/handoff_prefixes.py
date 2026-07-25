"""Runtime handoff near-field prefixes."""

RELAY_REACTION_EXECUTION_PREFIX = (
    "本步不是确认上轮，也不是向用户复述计划；本步是中继执行反应步。"
    "请先执行交接里的第一动作；若交接要求续读，先调用 file_read；"
    "若现实阻塞，直接自然语言说明阻塞事实；"
    "若需要跨轮继续，调用 reaction_finalize(handoff_text)，可与最后一批工具同次提交。"
    "普通自然语言只有在 Runtime 门禁允许时才会结束本步。"
)

SETUP_TO_REACTION_RELAY_PREFIX = (
    "起手步只完成放行、挂载和入口确认，不代表本轮任务已执行。"
    "反应步必须接手执行下面便签；不要把旧 CONTENT、WB 焦点或缓存"
    "当成本轮新执行结果。"
)

RELAY_NEXT_SETUP_PREFIX = (
    "下一轮起手步只做放行、挂载和入口确认；"
    "请把下面中继任务转成反应步可执行入口，"
    "不要把它改写成等待用户的计划。"
)

def ensure_handoff_prefix(text, prefix):
    """Prefix handoff text once, keeping the original free note intact."""
    text = str(text or "").strip()
    prefix = str(prefix or "").strip()
    if not prefix:
        return text
    if not text:
        return prefix
    if prefix in text:
        return text
    return f"{prefix}\n{text}"


def prefix_reaction_loop_handoff(target, text):
    """Add target-specific near-field framing to reaction loop handoffs."""
    target = str(target or "").strip()
    if target == "next_reaction":
        return ensure_handoff_prefix(text, RELAY_REACTION_EXECUTION_PREFIX)
    if target == "next_setup":
        return ensure_handoff_prefix(text, RELAY_NEXT_SETUP_PREFIX)
    return str(text or "").strip()
