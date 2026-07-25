"""
Periodic context layer helpers.

The assembler keeps the public instance methods as compatibility wrappers; this
module owns the structured periodic mount projection and budget trimming rules.
"""


def build_periodic(assembler, state, step, round_type):
    """Build the DDS §19.5 periodic memory projection."""
    try:
        from data.periodic_mount_store import PeriodicMountStore

        mounts = PeriodicMountStore().load()
        limits = periodic_limits(getattr(assembler, "config_store", None))
        structured = render_structured_periodic(
            mounts,
            limits["periodic_memory_items_chars"],
        )
        if structured is not None:
            return structured
    except Exception:
        pass
    return ""


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
    if "periodic_memory_items" not in mounts:
        return None
    memory_items = mounts.get("periodic_memory_items", [])

    parts = []
    memory_texts = select_periodic_texts(memory_items, memory_limit)
    if memory_texts:
        parts.append("## 定期记忆投影\n" + "\n\n".join(memory_texts))

    return "\n\n".join(parts)


def select_periodic_texts(items, limit):
    selected = []
    used = 0
    for item in items or []:
        text = periodic_item_text(item)
        if not text:
            continue
        size = len(text)
        if size > limit or used + size > limit:
            continue
        selected.append(text)
        used += size
    return selected


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
