"""Round-local memory reconsolidation guide and deterministic settlement."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import re
import unicodedata

from data.memory_store import (
    MEMORY_MUTATION_LOCK,
    MEMORY_OVERLAY_FIELDS,
    _body_limit_for_weight,
    _normalise_meta_entry,
    _read_memory_overlay,
    memory_target_tier,
    replace_memory_semantic_payload,
)


GUIDE_ID_PREFIX = "memory_reconsolidation"
GUIDE_ITEM_ID = "memory_reconsolidation_due"
GUIDE_OPTION_ID = "submit_memory_reconsolidations"
NEW_TITLE_MARKER = "[回忆重整]"
LEGACY_TITLE_MARKER = "[召回补全内容]"
TIER_RANK = {"Abstract": 1, "Summary": 2, "Full": 3}
KEYWORD_LIMITS = {"Abstract": 4, "Summary": 6, "Full": 8}


class MemoryReconsolidationError(RuntimeError):
    """Stable fail-closed reconsolidation error."""


def _weight(value):
    if isinstance(value, bool):
        raise MemoryReconsolidationError("invalid_memory_weight")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise MemoryReconsolidationError("invalid_memory_weight") from exc
    if number < 0 or number > 5:
        raise MemoryReconsolidationError("invalid_memory_weight")
    return number


def _norm_keyword(value):
    return " ".join(
        unicodedata.normalize("NFKC", str(value or "")).split()
    ).casefold()


def _semantic_fingerprint(*, body, meta, tier):
    payload = {
        "body": str(body or ""),
        "stored_at": str((meta or {}).get("stored_at") or ""),
        "tags": list((meta or {}).get("tags") or []),
        "tier": str(tier or ""),
        "title": str((meta or {}).get("title") or ""),
        "weight": _weight((meta or {}).get("weight")),
    }
    encoded = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def reconsolidation_candidate(state):
    """Return the frozen guide item for a legitimate admitted tier mismatch."""
    if not isinstance(state, dict) or state.get("source") != "ltm":
        return None
    ltm = state.get("ltm") or {}
    tier = str(ltm.get("tier") or "").strip()
    if tier in {"Pinned", "Backup"}:
        return None
    if tier not in TIER_RANK:
        raise MemoryReconsolidationError("memory_reconsolidation_layer_invalid")
    meta = dict(state.get("meta") or {})
    if str(meta.get("access") or "public").strip().lower() != "public":
        return None
    if not str(meta.get("stored_at") or "").strip():
        return None
    weight = _weight(meta.get("weight"))
    target_tier = memory_target_tier(weight)
    if tier == target_tier:
        return None
    if TIER_RANK[tier] > TIER_RANK[target_tier]:
        raise MemoryReconsolidationError(
            "memory_reconsolidation_layer_conflict"
        )
    mem_id = str(meta.get("id") or "").strip()
    if not re.fullmatch(r"MEM-[0-9A-F]{8}", mem_id):
        raise MemoryReconsolidationError("invalid_mem_id")
    tags = meta.get("tags")
    if not isinstance(tags, list):
        raise MemoryReconsolidationError("memory_keywords_invalid")
    return {
        "mem_id": mem_id,
        "title": str(meta.get("title") or mem_id).strip() or mem_id,
        "source_tier": tier,
        "target_tier": target_tier,
        "weight": weight,
        "body_limit": _body_limit_for_weight(weight),
        "keyword_limit": KEYWORD_LIMITS[target_tier],
        "current_keywords": list(tags),
        "created_instance_id": str(meta.get("created_instance_id") or ""),
        "created_round": meta.get("created_round"),
        "semantic_fingerprint": _semantic_fingerprint(
            body=state.get("body"), meta=meta, tier=tier
        ),
        "periodic_requested": False,
    }


class MemoryReconsolidationTracker:
    """One non-persistent reconsolidation obligation set for a Round."""

    def __init__(self, round_num):
        self.round_num = int(round_num)
        self._pending = {}

    @property
    def guide_id(self):
        return f"{GUIDE_ID_PREFIX}:R{self.round_num:06d}"

    def register(self, state, *, periodic_requested=False):
        item = reconsolidation_candidate(state)
        if item is None:
            return None
        mem_id = item["mem_id"]
        existing = self._pending.get(mem_id)
        if existing is not None:
            if existing["semantic_fingerprint"] != item["semantic_fingerprint"]:
                raise MemoryReconsolidationError(
                    "memory_reconsolidation_fingerprint_conflict"
                )
            if periodic_requested:
                existing["periodic_requested"] = True
            return deepcopy(existing)
        item["periodic_requested"] = bool(periodic_requested)
        self._pending[mem_id] = item
        return deepcopy(item)

    def pending_items(self):
        return [deepcopy(item) for item in self._pending.values()]

    def pending_ids(self):
        return list(self._pending)

    def get(self, mem_id):
        item = self._pending.get(str(mem_id or "").strip())
        return deepcopy(item) if item is not None else None

    def complete(self, mem_id):
        return self._pending.pop(str(mem_id or "").strip(), None)

    def has_pending(self):
        return bool(self._pending)

    def render_guide(self, discipline):
        discipline = str(discipline or "").strip()
        if not discipline:
            raise MemoryReconsolidationError(
                "memory_reconsolidation_guide_template_missing"
            )
        if not self._pending:
            return ""
        lines = [
            "## GUIDE｜回忆重整指南",
            discipline,
            "",
            "调用坐标：",
            f"- guide_id={self.guide_id}",
            f"- item_id={GUIDE_ITEM_ID}",
            f"- option_id={GUIDE_OPTION_ID}",
            "",
            "当前必须逐条重整：",
        ]
        for item in self._pending.values():
            created_round = item.get("created_round")
            created = (
                f"{item.get('created_instance_id') or '—'} / "
                f"{created_round if created_round is not None else '—'}"
            )
            lines.append(
                "- {mem_id}｜{title}｜LTM/{source_tier}→LTM/{target_tier}｜"
                "正文≤{body_limit}字｜关键词1–{keyword_limit}个｜创建坐标 {created}".format(
                    created=created, **item
                )
            )
        lines.extend([
            "",
            "使用 guide_submit 的 fields.results 覆盖全部当前 ID；"
            "每项只填写 mem_id、semantic_content、final_keywords。",
        ])
        return "\n".join(lines)

    def audit_state(self):
        return {
            "guide_id": self.guide_id,
            "pending_items": self.pending_items(),
        }


class MemoryReconsolidationProcessor:
    """Replace one canonical semantic body without creating a second recall."""

    def __init__(self, *, memory_store, assembler=None, fault_hook=None):
        self.memory_store = memory_store
        self.assembler = assembler
        self.fault_hook = fault_hook

    def apply(self, frozen_item, semantic_content, final_keywords):
        item = dict(frozen_item or {})
        mem_id = str(item.get("mem_id") or "").strip()
        if not re.fullmatch(r"MEM-[0-9A-F]{8}", mem_id):
            raise MemoryReconsolidationError("invalid_mem_id")
        semantic = self._validate_semantic(
            semantic_content, int(item.get("body_limit") or 0)
        )
        keywords = self._validate_keywords(
            final_keywords, int(item.get("keyword_limit") or 0)
        )

        with MEMORY_MUTATION_LOCK:
            pre_ltm = self.memory_store.snapshot_ltm_files()
            pre_stm = self.memory_store.snapshot_stm_files()
            cache_snapshot = self._snapshot_periodic_cache()
            try:
                state = self._state(mem_id)
                current = reconsolidation_candidate(state)
                if current is None:
                    raise MemoryReconsolidationError(
                        "memory_reconsolidation_not_required"
                    )
                if current["semantic_fingerprint"] != str(
                    item.get("semantic_fingerprint") or ""
                ):
                    raise MemoryReconsolidationError(
                        "memory_reconsolidation_source_changed"
                    )
                if not state["stm_present"]:
                    raise MemoryReconsolidationError(
                        "memory_reconsolidation_stm_missing"
                    )

                source_tier = current["source_tier"]
                target_tier = current["target_tier"]
                source_meta = dict(state["meta"])
                title = self._completed_title(source_meta)
                updated_shared = dict(source_meta)
                updated_shared["title"] = title
                updated_shared["tags"] = keywords
                updated_shared["recalled"] = True
                period = updated_shared.get("decay_period_days")
                if (
                    not isinstance(period, int)
                    or isinstance(period, bool)
                    or period <= 0
                ):
                    raise MemoryReconsolidationError(
                        "invalid_decay_period_days"
                    )
                updated_shared["decay_countdown_days"] = period
                target_body = replace_memory_semantic_payload(
                    state["body"],
                    title,
                    semantic,
                    current["weight"],
                    tier=target_tier,
                )

                self._fault("before_write")
                self.memory_store.store_ltm_entry(
                    target_tier,
                    mem_id,
                    target_body,
                    updated_shared,
                    source_tier=source_tier,
                )
                self._fault("after_ltm_move")
                canonical = self.memory_store.ltm_entry_state(
                    mem_id, include_backup=False
                )
                if canonical is None:
                    raise MemoryReconsolidationError(
                        "memory_reconsolidation_ltm_missing"
                    )

                overlay = _read_memory_overlay()["entries"].get(mem_id, {})
                branch_meta = dict(canonical["meta"])
                branch_meta.update({
                    key: overlay.get(
                        key, [] if key == "linked_containers" else ""
                    )
                    for key in MEMORY_OVERLAY_FIELDS
                })
                branch_meta = _normalise_meta_entry(branch_meta)
                original_heat = deepcopy(state["stm"]["heat"])
                self.memory_store.replace_stm_body(mem_id, canonical["body"])
                self._fault("after_stm_body")
                self.memory_store.replace_stm_meta(
                    mem_id, branch_meta, canonical_sync=True
                )
                self._fault("after_stm_meta")
                self.memory_store.rebuild_stm_index()
                self.memory_store.rebuild_stm_keywords()
                self._fault("after_stm_projections")
                self._verify(
                    mem_id,
                    target_tier=target_tier,
                    body=canonical["body"],
                    meta=branch_meta,
                    heat=original_heat,
                )
                self._fault("after_verify")
                if self.assembler is not None:
                    self.assembler.invalidate_layer("periodic", strict=True)
                self._fault("after_cache_invalidation")
            except Exception as exc:
                try:
                    self.memory_store.restore_ltm_files(pre_ltm)
                    self.memory_store.restore_stm_files(pre_stm)
                    self._restore_periodic_cache(cache_snapshot)
                except Exception as restore_exc:
                    raise RuntimeError(
                        "memory_reconsolidation_rollback_failed:"
                        f"{type(restore_exc).__name__}"
                    ) from exc
                raise

        return {
            "schema_version": "memory_reconsolidation_receipt.v1",
            "status": "applied",
            "mem_id": mem_id,
            "source_memory_layer": f"LTM/{source_tier}",
            "target_memory_layer": f"LTM/{target_tier}",
            "title": title,
            "body_chars": len(semantic),
            "body_limit": current["body_limit"],
            "keywords": list(keywords),
            "keyword_limit": current["keyword_limit"],
            "stm_present": True,
            "heat_changed": False,
            "recall_coordinates_changed": False,
            "stored_at": str(canonical["meta"].get("stored_at") or ""),
            "weight": current["weight"],
            "ltm_decay_countdown_after": canonical["meta"].get(
                "decay_countdown_days"
            ),
            "periodic_cache_invalidated": self.assembler is not None,
        }

    def _state(self, mem_id):
        ltm = self.memory_store.ltm_entry_state(mem_id, include_backup=True)
        if ltm is None:
            raise MemoryReconsolidationError("memory_not_found")
        stm = self.memory_store.stm_entry_state(mem_id)
        parts = (
            stm.get("body") is not None,
            stm.get("meta") is not None,
            stm.get("heat") is not None,
        )
        if any(parts) and not all(parts):
            raise MemoryReconsolidationError("stm_ltm_residence_incomplete")
        meta = dict(ltm.get("meta") or {})
        if str(meta.get("access") or "public").strip().lower() != "public":
            raise MemoryReconsolidationError("private_memory_deferred")
        return {
            "source": "ltm",
            "ltm": ltm,
            "stm": stm,
            "stm_present": all(parts),
            "body": ltm.get("body"),
            "meta": meta,
        }

    @staticmethod
    def _validate_semantic(value, limit):
        semantic = str(value or "").replace(
            "\r\n", "\n"
        ).replace("\r", "\n").strip()
        if not semantic:
            raise MemoryReconsolidationError(
                "memory_reconsolidation_body_missing"
            )
        if len(semantic) > limit:
            raise MemoryReconsolidationError(
                f"memory_reconsolidation_body_too_long:max={limit};"
                f"actual={len(semantic)}"
            )
        if re.search(
            r"<!--|^\s*#|^\s*(?:MEM-|标题[：:]|内容[：:]|摘要[：:]|梗概[：:])",
            semantic,
            re.MULTILINE,
        ):
            raise MemoryReconsolidationError(
                "memory_reconsolidation_body_shape_invalid"
            )
        return semantic

    @staticmethod
    def _validate_keywords(values, limit):
        if not isinstance(values, list) or not 1 <= len(values) <= limit:
            raise MemoryReconsolidationError(
                "memory_reconsolidation_keywords_limit_invalid"
            )
        result = []
        seen = set()
        for value in values:
            keyword = str(value or "").strip()
            normalized = _norm_keyword(keyword)
            if not normalized:
                raise MemoryReconsolidationError(
                    "memory_reconsolidation_keyword_invalid"
                )
            if normalized in seen:
                raise MemoryReconsolidationError(
                    "memory_reconsolidation_keyword_duplicate"
                )
            seen.add(normalized)
            result.append(keyword)
        return result

    @staticmethod
    def _completed_title(meta):
        title = str((meta or {}).get("title") or (meta or {}).get("id") or "").strip()
        if not title:
            raise MemoryReconsolidationError("memory_title_missing")
        if NEW_TITLE_MARKER in title or LEGACY_TITLE_MARKER in title:
            return title
        return f"{title}{NEW_TITLE_MARKER}"

    def _verify(self, mem_id, *, target_tier, body, meta, heat):
        ltm = self.memory_store.ltm_entry_state(mem_id, include_backup=False)
        if (
            ltm is None
            or ltm.get("tier") != target_tier
            or ltm.get("body") != body
            or list((ltm.get("meta") or {}).get("tags") or [])
            != list(meta.get("tags") or [])
        ):
            raise MemoryReconsolidationError(
                "memory_reconsolidation_ltm_unverified"
            )
        stm = self.memory_store.stm_entry_state(mem_id)
        if (
            stm.get("body") != body
            or _normalise_meta_entry(stm.get("meta") or {}) != meta
            or stm.get("heat") != heat
        ):
            raise MemoryReconsolidationError(
                "memory_reconsolidation_stm_unverified"
            )

    def _snapshot_periodic_cache(self):
        if self.assembler is None:
            return None
        return {
            "cache": {
                key: value
                for key, value in self.assembler._layer_cache.items()
                if key[1] == "periodic"
            },
            "blocks": {
                key: deepcopy(value)
                for key, value in self.assembler._layer_block_cache.items()
                if key[1] == "periodic"
            },
            "expired": self._periodic_expired(),
        }

    def _restore_periodic_cache(self, snapshot):
        if self.assembler is None or snapshot is None:
            return
        for key in list(self.assembler._layer_cache):
            if key[1] == "periodic":
                del self.assembler._layer_cache[key]
        for key in list(self.assembler._layer_block_cache):
            if key[1] == "periodic":
                del self.assembler._layer_block_cache[key]
        self.assembler._layer_cache.update(snapshot["cache"])
        self.assembler._layer_block_cache.update(snapshot["blocks"])
        if snapshot["expired"] is not None:
            self.assembler.state_store._set_internal(
                "base.context_cache.periodic_expired", snapshot["expired"]
            )

    def _periodic_expired(self):
        state_store = getattr(self.assembler, "state_store", None)
        if state_store is None:
            return None
        state = state_store.load()
        return bool(
            ((state.get("base") or {}).get("context_cache") or {}).get(
                "periodic_expired", True
            )
        )

    def _fault(self, stage):
        if callable(self.fault_hook):
            self.fault_hook(stage)


def apply_memory_reconsolidation_guide(arguments, evidence_context):
    """Apply the current Round-local guide with per-memory independence."""
    arguments = arguments if isinstance(arguments, dict) else {}
    context = dict(evidence_context or {})
    tracker = context.get("memory_reconsolidation_tracker")
    processor = context.get("memory_reconsolidation_processor")
    periodic = context.get("periodic_mount_processor")
    if not isinstance(tracker, MemoryReconsolidationTracker) or processor is None:
        return {
            "status": "rejected",
            "reason": "memory_reconsolidation_context_unavailable",
            "backend_receipts": [],
        }
    if str(arguments.get("guide_id") or "").strip() != tracker.guide_id:
        return {
            "status": "rejected",
            "reason": "memory_reconsolidation_guide_not_active",
            "backend_receipts": [],
        }
    if str(arguments.get("item_id") or "").strip() != GUIDE_ITEM_ID:
        return {
            "status": "rejected",
            "reason": "memory_reconsolidation_item_invalid",
            "backend_receipts": [],
        }
    if str(arguments.get("option_id") or "").strip() != GUIDE_OPTION_ID:
        return {
            "status": "rejected",
            "reason": "memory_reconsolidation_option_invalid",
            "backend_receipts": [],
        }
    fields = arguments.get("fields")
    if not isinstance(fields, dict) or set(fields) != {"results"}:
        return {
            "status": "rejected",
            "reason": "memory_reconsolidation_fields_invalid",
            "backend_receipts": [],
        }
    results = fields.get("results") if isinstance(fields, dict) else None
    if not isinstance(results, list):
        return {
            "status": "rejected",
            "reason": "memory_reconsolidation_results_invalid",
            "backend_receipts": [],
        }

    pending_ids = tracker.pending_ids()
    submitted = {}
    invalid_entries = []
    for index, result in enumerate(results):
        if not isinstance(result, dict):
            invalid_entries.append({
                "status": "rejected",
                "reason": "memory_reconsolidation_result_invalid",
                "index": index,
            })
            continue
        if set(result) != {"mem_id", "semantic_content", "final_keywords"}:
            invalid_entries.append({
                "schema_version": "memory_reconsolidation_receipt.v1",
                "status": "rejected",
                "mem_id": str(result.get("mem_id") or "").strip(),
                "reason": "memory_reconsolidation_result_fields_invalid",
                "index": index,
            })
            continue
        mem_id = str(result.get("mem_id") or "").strip()
        submitted.setdefault(mem_id, []).append(result)

    backend_receipts = list(invalid_entries)
    completed_ids = []
    for mem_id in pending_ids:
        matches = submitted.get(mem_id) or []
        if len(matches) != 1:
            backend_receipts.append({
                "schema_version": "memory_reconsolidation_receipt.v1",
                "status": "rejected",
                "mem_id": mem_id,
                "reason": (
                    "memory_reconsolidation_result_missing"
                    if not matches
                    else "memory_reconsolidation_result_duplicate"
                ),
            })
            continue
        item = tracker.get(mem_id)
        result = matches[0]
        try:
            receipt = processor.apply(
                item,
                result.get("semantic_content"),
                result.get("final_keywords"),
            )
        except Exception as exc:
            backend_receipts.append({
                "schema_version": "memory_reconsolidation_receipt.v1",
                "status": "rejected",
                "mem_id": mem_id,
                "reason": str(exc),
            })
            continue

        tracker.complete(mem_id)
        completed_ids.append(mem_id)
        if item.get("periodic_requested"):
            if periodic is None:
                receipt["periodic_mount_outcome"] = "mount_blocked"
                receipt["periodic_mount_reason"] = (
                    "periodic_mount_processor_unavailable"
                )
            else:
                try:
                    mount_receipt = periodic.apply("mount", mem_id)
                    receipt["periodic_mount_outcome"] = mount_receipt.get(
                        "outcome", "mounted"
                    )
                    receipt["periodic_mount_receipt"] = mount_receipt
                except Exception as mount_exc:
                    reason = str(mount_exc)
                    try:
                        periodic.mark_pending_blocked(mem_id, reason)
                    except Exception as ledger_exc:
                        reason = (
                            f"{reason};pending_ledger_update_failed:"
                            f"{type(ledger_exc).__name__}:{ledger_exc}"
                        )
                    receipt["periodic_mount_outcome"] = "mount_blocked"
                    receipt["periodic_mount_reason"] = reason
        backend_receipts.append(receipt)

    for mem_id, matches in submitted.items():
        if mem_id in pending_ids:
            continue
        for _match in matches:
            backend_receipts.append({
                "schema_version": "memory_reconsolidation_receipt.v1",
                "status": "rejected",
                "mem_id": mem_id,
                "reason": "memory_reconsolidation_result_unknown",
            })

    remaining_ids = tracker.pending_ids()
    return {
        "status": "applied" if completed_ids else "rejected",
        "reason": "" if completed_ids else "memory_reconsolidation_no_item_applied",
        "backend_receipts": backend_receipts,
        "completed_ids": completed_ids,
        "remaining_ids": remaining_ids,
    }
