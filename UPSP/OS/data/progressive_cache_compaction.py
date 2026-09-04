"""Spec760 interaction-anchored progressive cache data model."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy


SCHEMA_VERSION = "cache_compaction_debt.v3"
GUIDE_ITEM_ID = "cache_compaction_due"
GUIDE_OPTION_ID = "submit_cache_compaction_batch"
MAX_BATCH_SHARDS = 32
COMPACTION_OUTPUT_TOKENS = 65536


def text_sha256(value):
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def blocks_sha256(blocks):
    payload = json.dumps(
        list(blocks or []), ensure_ascii=False, sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def source_fingerprint(blocks, policy, logical_window):
    payload = {
        "blocks_sha256": blocks_sha256(blocks),
        "policy": dict(policy or {}),
        "logical_window": int(logical_window or 0),
    }
    return text_sha256(json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ))


def _ref(block):
    value = block.get("ref") if isinstance(block, dict) else None
    return value if isinstance(value, dict) else {}


def _interaction_index(block):
    try:
        return int(_ref(block).get("interaction_round_index") or 0)
    except (TypeError, ValueError):
        return 0


def group_lately_blocks(blocks, protected_count):
    """Derive stable interaction groups from ordered user anchors."""
    groups = []
    current = {
        "group_id": "pre-user",
        "interaction_round_index": 0,
        "user_block_id": "",
        "blocks": [],
    }
    seen_indexes = set()
    raw_user_indexes = set()
    last_index = 0
    for position, raw in enumerate(blocks or []):
        block = deepcopy(raw)
        index = _interaction_index(block)
        is_user = block.get("role") == "user" and block.get("kind") == "interaction"
        is_compacted_anchor = (
            block.get("kind") == "interaction_summary"
            and index > 0
            and index != last_index
        )
        if is_user or is_compacted_anchor:
            if index <= 0 or index <= last_index or index in seen_indexes:
                raise ValueError("cache_compaction_interaction_index_conflict")
            if current["blocks"]:
                groups.append(current)
            current = {
                "group_id": f"interaction:{index}",
                "interaction_round_index": index,
                "user_block_id": str(block.get("id") or "") if is_user else "",
                "blocks": [],
            }
            seen_indexes.add(index)
            if is_user:
                raw_user_indexes.add(index)
            last_index = index
        elif index not in {0, last_index}:
            raise ValueError("cache_compaction_interaction_index_conflict")
        block["_source_position"] = position
        current["blocks"].append(block)
    if current["blocks"]:
        groups.append(current)

    protected_indexes = set(
        sorted(raw_user_indexes)[-max(0, int(protected_count or 0)):]
    )
    result = []
    for group in groups:
        copied = deepcopy(group)
        copied["protected"] = (
            copied["interaction_round_index"] in protected_indexes
            and copied["interaction_round_index"] > 0
        )
        copied["source_block_ids"] = [
            str(item.get("id") or "") for item in copied["blocks"]
        ]
        copied["source_chars"] = sum(
            len(str(item.get("text") or "")) for item in copied["blocks"]
        )
        result.append(copied)
    return result


def _safe_projection(block):
    """Project only structured facts whose equality is locally provable."""
    text = str(block.get("text") or "")
    kind = str(block.get("kind") or "")
    ref = _ref(block)
    if kind != "tool_fact":
        return text, "raw"
    receipt = ref.get("tool_result") or ref.get("protocol_receipt")
    if not isinstance(receipt, dict):
        return text, "raw"
    stable = {}
    for key in (
            "tool_id", "call_id", "status", "reason", "mem_id", "container_id",
            "path", "url", "query", "count", "chars", "sha256",
            "coverage_complete", "has_more", "stop_reason", "applied"):
        value = receipt.get(key)
        if value not in (None, "", [], {}):
            stable[key] = value
    if not stable:
        return text, "raw"
    return json.dumps(stable, ensure_ascii=False, sort_keys=True), "structured"


def _split_text_ranges(text, limit):
    text = str(text or "")
    if len(text) <= limit:
        return [(0, len(text))]
    ranges = []
    start = 0
    while start < len(text):
        hard_end = min(len(text), start + limit)
        end = hard_end
        if hard_end < len(text):
            floor = start + max(1, limit // 2)
            for marker in ("\n\n", "\n"):
                found = text.rfind(marker, floor, hard_end)
                if found >= floor:
                    end = found + len(marker)
                    break
        if end <= start:
            end = hard_end
        ranges.append((start, end))
        start = end
    return ranges


def plan_debt(blocks, policy, *, logical_window, round_num, observation, now_iso):
    groups = group_lately_blocks(
        blocks, policy.get("protected_interaction_count", 16)
    )
    batch_limit = int(policy["batch_source_chars"])
    ratio = float(policy["semantic_summary_ratio"])
    shards = []
    public_groups = []
    for group_index, group in enumerate(groups, start=1):
        compressible = []
        context_only_chars = 0
        for block in group["blocks"]:
            if str(block.get("kind") or "") == "interaction_summary":
                # It remains visible in normal 30_lately as context, but is
                # not duplicated into the one-shot C-track source material.
                context_only_chars += len(str(block.get("text") or ""))
                continue
            if group["protected"] and str(block.get("id") or "") == group["user_block_id"]:
                continue
            projection, projection_kind = _safe_projection(block)
            raw_text = str(block.get("text") or "")
            if not raw_text:
                continue
            if len(projection) <= batch_limit:
                pieces = [(0, len(raw_text), projection)]
            else:
                pieces = [
                    (start, end, raw_text[start:end])
                    for start, end in _split_text_ranges(raw_text, batch_limit)
                ]
                projection_kind = "raw_split"
            for start, end, body in pieces:
                compressible.append({
                    "block_id": str(block.get("id") or ""),
                    "start": start,
                    "end": end,
                    "source_chars": end - start,
                    "body_sha256": text_sha256(raw_text),
                    "projected_content": body,
                    "projection_kind": projection_kind,
                    "projection_sha256": text_sha256(body),
                })

        group_shards = []
        current = []
        current_projection_chars = 0

        def flush():
            nonlocal current, current_projection_chars
            if not current:
                return
            shard_id = f"CCS-{len(shards) + 1:05d}"
            projection = "\n\n".join(item["projected_content"] for item in current)
            source_chars = sum(item["source_chars"] for item in current)
            item = {
                "shard_id": shard_id,
                "group_id": group["group_id"],
                "interaction_round_index": group["interaction_round_index"],
                "source_spans": [
                    {key: value for key, value in span.items()
                     if key != "projected_content"}
                    for span in current
                ],
                "source_block_ids": list(dict.fromkeys(
                    span["block_id"] for span in current
                )),
                "source_chars": source_chars,
                "projected_chars": len(projection),
                "projected_content": projection,
                "projection_sha256": text_sha256(projection),
                "target_chars": int(source_chars * ratio),
            }
            shards.append(item)
            group_shards.append(shard_id)
            current = []
            current_projection_chars = 0

        for piece in compressible:
            chars = len(piece["projected_content"])
            if current and current_projection_chars + chars > batch_limit:
                flush()
            current.append(piece)
            current_projection_chars += chars
            if current_projection_chars >= batch_limit:
                flush()
        flush()
        if group_shards and context_only_chars:
            first = next(
                item for item in shards
                if item.get("shard_id") == group_shards[0]
            )
            first["source_chars"] += context_only_chars
            first["context_only_chars"] = context_only_chars
            first["target_chars"] = int(first["source_chars"] * ratio)
        public_groups.append({
            "group_id": group["group_id"],
            "interaction_round_index": group["interaction_round_index"],
            "user_block_id": group["user_block_id"],
            "user_chars": next((
                len(str(item.get("text") or ""))
                for item in group["blocks"]
                if str(item.get("id") or "") == group["user_block_id"]
            ), 0),
            "protected": group["protected"],
            "source_block_ids": group["source_block_ids"],
            "source_chars": group["source_chars"],
            "shard_ids": group_shards,
        })

    total_chars = sum(len(str(block.get("text") or "")) for block in blocks)
    seed = f"{round_num}|{blocks_sha256(blocks)}|{now_iso}"
    compaction_id = (
        f"cache-compaction-R{int(round_num or 0):06d}-"
        f"{hashlib.sha256(seed.encode('utf-8')).hexdigest()[:16]}"
    )
    return {
        "schema_version": SCHEMA_VERSION,
        "status": "open",
        "phase": "staging",
        "revision": 1,
        "compaction_id": compaction_id,
        "created_round": int(round_num or 0),
        "created_at": now_iso,
        "updated_at": now_iso,
        "source_fingerprint": source_fingerprint(blocks, policy, logical_window),
        "pressure_observation": deepcopy(observation or {}),
        "logical_window_tokens": int(logical_window or 0),
        "policy": deepcopy(policy),
        "frozen_prefix": {
            "block_count": len(blocks),
            "chars": total_chars,
            "sha256": blocks_sha256(blocks),
        },
        "target_chars": int(total_chars * float(policy["cycle_target_ratio"])),
        "groups": public_groups,
        "shards": shards,
        "results": [],
        "deleted_group_ids": [],
    }


def pending_shards(debt):
    completed = {
        str(item.get("shard_id") or "")
        for item in debt.get("results") or [] if isinstance(item, dict)
    }
    return [
        deepcopy(item) for item in debt.get("shards") or []
        if isinstance(item, dict)
        and str(item.get("shard_id") or "") not in completed
    ]


def current_batch(debt):
    policy = debt.get("policy") if isinstance(debt.get("policy"), dict) else {}
    char_limit = min(
        int(policy.get("batch_source_chars") or 65536),
        int(debt.get("active_batch_source_chars") or 65536),
    )
    selected = []
    chars = 0
    for shard in pending_shards(debt):
        projected = int(shard.get("projected_chars") or 0)
        if selected and (len(selected) >= MAX_BATCH_SHARDS or chars + projected > char_limit):
            break
        selected.append(shard)
        chars += projected
        if len(selected) >= MAX_BATCH_SHARDS or chars >= char_limit:
            break
    return selected


def render_guide(debt, discipline):
    batch = current_batch(debt)
    if not batch:
        return ""
    lines = [
        "## GUIDE｜最近缓存压缩指南",
        str(discipline or "").strip(),
        "",
        "调用坐标：",
        f"- guide_id=cache_compaction:{debt['compaction_id']}",
        f"- item_id={GUIDE_ITEM_ID}",
        f"- option_id={GUIDE_OPTION_ID}",
        "",
        "本批分片：" + "、".join(item["shard_id"] for item in batch),
        "遗漏分片表示尚未处理；replace 可提交空正文，keep 必须说明原因。",
        (
            '精确形状：fields={"results":[{"shard_id":"'
            f'{batch[0]["shard_id"]}","action":"replace",'
            '"semantic_content":"压缩后的语义正文","reason":""}]}。'
        ),
        (
            '选择 keep 时仍使用同一四个字段，令 semantic_content=""，'
            "并填写非空 reason。"
        ),
    ]
    return "\n".join(line for line in lines if line is not None)


def render_materials(debt):
    materials = []
    batch_id = f"CCB-{int(debt.get('revision') or 1):05d}"
    for shard in current_batch(debt):
        spans = ", ".join(
            f"{item['block_id']}[{item['start']}:{item['end']}]"
            for item in shard.get("source_spans") or []
        )
        materials.append({
            "role": "system",
            "kind": "material",
            "source": "cache_compaction",
            "source_block_id": shard["shard_id"],
            "content": "\n".join([
                f"最近缓存压缩材料｜批次 {batch_id}",
                f"shard_id: {shard['shard_id']}",
                f"interaction: {shard['group_id']}",
                f"source_ranges: {spans}",
                f"source_chars: {shard['source_chars']}",
                f"summary_limit: {shard['target_chars']}",
                f"projection_sha256: {shard['projection_sha256']}",
                "投影正文：",
                shard["projected_content"],
            ]),
        })
    return materials
