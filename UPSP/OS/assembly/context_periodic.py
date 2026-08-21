"""Periodic context assembly from verified Pinned memory truth."""

from assembly.context_helpers import join_layer_blocks
from errors import RequiredContextError


def build_periodic(assembler, state, step, round_type):
    """Build the DDS §19.5 periodic memory projection."""
    return build_periodic_with_block_index(assembler, state, step, round_type)[0]


def build_periodic_with_block_index(assembler, state, step, round_type):
    try:
        from data.memory_store import MemoryStore, project_periodic_memory_body
        from data.periodic_mount_store import (
            PERIODIC_MOUNTS_SCHEMA,
            PeriodicMountStore,
        )
        from data.periodic_pin_owner_store import PeriodicPinOwnerStore
        from paths import ACTIVE_INSTANCE_ID

        mounts = PeriodicMountStore().load()
        limits = periodic_limits(getattr(assembler, "config_store", None))
        if mounts.get("schema_version") == PERIODIC_MOUNTS_SCHEMA:
            owner_entries = PeriodicPinOwnerStore().load().get("entries", {})
            by_id = {
                item["id"]: item
                for item in MemoryStore().list_public_ltm_entries()
            }
            live_items = []
            used = 0
            for mounted in mounts.get("periodic_memory_items", []):
                mem_id = mounted["id"]
                owner = owner_entries.get(mem_id)
                if (
                    not isinstance(owner, dict)
                    or ACTIVE_INSTANCE_ID not in owner.get("owners", [])
                ):
                    raise ValueError(f"periodic_memory_owner_missing:{mem_id}")
                source = by_id.get(mem_id)
                if source is None:
                    raise ValueError(f"periodic_memory_missing:{mem_id}")
                if source.get("memory_layer") != "LTM/Pinned":
                    raise ValueError(f"periodic_memory_not_pinned:{mem_id}")
                text = project_periodic_memory_body(
                    source.get("body", ""), source)
                used += len(text)
                if used > limits["periodic_memory_items_chars"]:
                    raise ValueError("periodic_memory_budget_exceeded")
                live_items.append({
                    "id": mem_id,
                    "title": source.get("title") or mem_id,
                    "rendered_text": text,
                })
            mounts = {"periodic_memory_items": live_items}
        structured = render_structured_periodic_with_block_index(
            mounts,
            limits["periodic_memory_items_chars"],
        )
        if structured[0] is not None:
            return structured
    except RequiredContextError:
        raise
    except Exception as exc:
        raise RequiredContextError("read", "periodic_memory_mounts", exc) from exc
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
