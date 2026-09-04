"""Read the current persona's public identity from its durable sources."""

import json
import os
import re

from paths import CORE_MD


def _core_value(label, path=CORE_MD):
    if not os.path.isfile(path):
        return ""
    try:
        with open(path, "r", encoding="utf-8") as handle:
            content = handle.read()
    except OSError:
        return ""
    match = re.search(rf"(?m)^{re.escape(label)}：\s*(.*)$", content)
    return match.group(1).strip() if match else ""


def public_identity(core_path=CORE_MD):
    values = {
        "pid": _core_value("PID", core_path),
        "name_zh": _core_value("中文名", core_path),
        "name_en": _core_value("英文名", core_path),
        "abbreviation": _core_value("缩写", core_path),
    }
    values["display_name"] = (
        values["name_zh"] or values["name_en"] or values["abbreviation"] or values["pid"]
    )
    return values


def self_relation_id(registry_path=None):
    from paths import PERSONA_DIR

    path = registry_path or os.path.join(PERSONA_DIR, "relation", "relation_registry.json")
    try:
        with open(path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return ""
    cards = data.get("cards") if isinstance(data, dict) else None
    if not isinstance(cards, list):
        return ""
    matches = [
        str(item.get("id") or "").strip()
        for item in cards
        if isinstance(item, dict)
        and item.get("category") == "self"
        and item.get("status", "active") == "active"
    ]
    return matches[0] if len(matches) == 1 else ""
