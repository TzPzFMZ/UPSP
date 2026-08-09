"""
Periodic context layer helpers.

The assembler keeps the public instance methods as compatibility wrappers; this
module owns the structured periodic mount projection and budget trimming rules.
"""

from assembly.context_helpers import join_layer_blocks


def build_periodic(assembler, state, step, round_type):
    """Build the DDS §19.5 periodic memory projection."""
    return build_periodic_with_block_index(assembler, state, step, round_type)[0]


def build_periodic_with_block_index(assembler, state, step, round_type):
    try:
        from data.periodic_mount_store import PeriodicMountStore

        mounts = PeriodicMountStore().load()
        limits = periodic_limits(getattr(assembler, "config_store", None))
        structured = render_structured_periodic_with_block_index(
            mounts,
            limits["periodic_memory_items_chars"],
        )
        if structured[0] is not None:
            return structured
    except Exception:
        pass
    return "", []


def periodic_limits(config_store=None):
    try:
        cfg = config_store
        if cfg is None:
            from data.config_store import ConfigStore

            cfg = ConfigStore()
        limits = cfg.get_periodic_limits()
    except Exception:
        limits = {}
    if not isinstance(limits, dict):
        limits = {}
    return {
        "periodic_memory_items_chars": limits.get("periodic_memory_items_chars", 65536),
    }


def render_structured_periodic(mounts, memory_limit):
    return render_structured_periodic_with_block_index(mounts, memory_limit)[0]


def render_structured_periodic_with_block_index(mounts, memory_limit):
    if "periodic_memory_items" not in mounts:
        return None, []
    memory_items = mounts.get("periodic_memory_items", [])
    selected = select_periodic_items(memory_items, memory_limit)
    blocks = []
    for index, (item, text) in enumerate(selected, 1):
        ident = item.get("id") or item.get("name") or item.get("title") if isinstance(item, dict) else ""
        title = item.get("title") or item.get("name") or ident if isinstance(item, dict) else ""
        blocks.append({
            "block_id": f"periodic:{index:02d}:{ident or index}",
            "title": str(title or ident or f"定期记忆 {index}"),
            "kind": "periodic_memory",
            "source_block_id": str(ident or ""),
            "content": ("## 定期记忆投影\n" if index == 1 else "") + text,
        })
    return join_layer_blocks(blocks)


def select_periodic_items(items, limit):
    selected = []
    used = 0
    for item in items or []:
        text = periodic_item_text(item)
        if not text:
            continue
        size = len(text)
        if size > limit or used + size > limit:
            continue
        selected.append((item, text))
        used += size
    return selected


def select_periodic_texts(items, limit):
    return [text for _item, text in select_periodic_items(items, limit)]


def periodic_item_text(item):
    if isinstance(item, str):
        return item.strip()
    if not isinstance(item, dict):
        return ""
    text = str(item.get("rendered_text", "")).strip()
    if text:
        return text
    ident = item.get("id") or item.get("name") or item.get("title")
    title = item.get("title") or item.get("name") or ""
    if ident and title:
        return f"- {ident} {title}"
    if ident:
        return f"- {ident}"
    return ""
