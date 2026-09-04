"""PID-shared source of truth for deferred memory semantic compression."""

import hashlib
import json
import os
import re
import unicodedata
from copy import deepcopy
from datetime import date

import paths
from constants import local_now
from data.atomic_write import atomic_write_json, atomic_write_text
from data.memory_store import (
    MEMORY_OVERLAY_FIELDS,
    MEMORY_MUTATION_LOCK,
    MemoryStore,
    _normalise_meta_entry,
    _read_memory_overlay,
    extract_memory_semantic,
    memory_is_admitted,
    memory_stm_forgetting_target,
    project_memory_body,
    replace_memory_semantic_payload,
)


SCHEMA = "memory_compression_pending.v1"
RECEIPT_SCHEMA = "memory_compression_batch_receipt.v1"
MEM_ID_RE = re.compile(r"^MEM-[0-9A-F]{8}$")
BATCH_MAX_ITEMS = 32
BATCH_MAX_SOURCE_CHARS = 65536
TARGET_LIMITS = {
    "Summary": {"body_chars": 512, "keywords": 6},
    "Abstract": {"body_chars": 128, "keywords": 4},
}
DOCUMENT_FIELDS = {
    "schema_version", "revision", "updated_at", "next_sequence",
    "last_daily_settled_date", "entries", "active_cycle",
}
ENTRY_BASE_FIELDS = {
    "mem_id", "sequence", "queued_at", "reason", "source_instance_id",
    "source_round", "source_tier", "target_tier", "target_weight",
    "body_limit", "keyword_limit", "body_sha256", "meta_sha256",
    "tags_sha256", "phase",
}
ENTRY_TARGET_FIELDS = {
    "target_body", "target_tags", "target_meta", "target_body_sha256",
    "target_tags_sha256", "target_meta_sha256",
}
CYCLE_FIELDS = {
    "cycle_id", "local_date", "round_num", "chronicle_receipt_hash",
    "stage", "frozen_mem_ids", "cursor",
}


def _now_text(now_fn=local_now):
    value = now_fn()
    return value.isoformat() if hasattr(value, "isoformat") else str(value)


def _sha_text(value):
    return hashlib.sha256(str(value or "").encode("utf-8")).hexdigest()


def _sha_json(value):
    return _sha_text(json.dumps(value, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":")))


def _norm_keyword(value):
    return " ".join(unicodedata.normalize("NFKC", str(value or "")).split()).casefold()


def empty_document():
    return {
        "schema_version": SCHEMA,
        "revision": 0,
        "updated_at": "",
        "next_sequence": 1,
        "last_daily_settled_date": "",
        "entries": [],
        "active_cycle": None,
    }


class MemoryCompressionStore:
    """Strict append-order ledger; unknown shapes never get overwritten."""

    def __init__(self, path=None, now_fn=local_now):
        self.path = path or paths.MEMORY_COMPRESSION_PENDING_JSON
        self.now_fn = now_fn

    def load(self):
        if not os.path.isfile(self.path):
            return empty_document()
        try:
            with open(self.path, "r", encoding="utf-8") as handle:
                value = json.load(handle)
        except (OSError, UnicodeError, json.JSONDecodeError) as exc:
            raise ValueError("memory_compression_ledger_invalid") from exc
        self._validate(value)
        return value

    def save(self, value):
        value = deepcopy(value)
        self._validate(value)
        value["revision"] = int(value.get("revision", 0)) + 1
        value["updated_at"] = _now_text(self.now_fn)
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        atomic_write_json(self.path, value)
        verified = self.load()
        if verified != value:
            raise ValueError("memory_compression_ledger_unverified")
        return verified

    def snapshot(self):
        if not os.path.isfile(self.path):
            return None
        with open(self.path, "r", encoding="utf-8") as handle:
            return handle.read()

    def restore(self, text):
        if text is None:
            if os.path.isfile(self.path):
                os.remove(self.path)
            return
        atomic_write_text(self.path, text)

    @staticmethod
    def _validate(value):
        if not isinstance(value, dict) or value.get("schema_version") != SCHEMA:
            raise ValueError("memory_compression_ledger_invalid")
        if set(value) != DOCUMENT_FIELDS:
            raise ValueError("memory_compression_ledger_invalid")
        if not isinstance(value["revision"], int) or value["revision"] < 0:
            raise ValueError("memory_compression_ledger_invalid")
        if not isinstance(value["next_sequence"], int) or value["next_sequence"] < 1:
            raise ValueError("memory_compression_ledger_invalid")
        if not isinstance(value["updated_at"], str) or not isinstance(
                value["last_daily_settled_date"], str):
            raise ValueError("memory_compression_ledger_invalid")
        if value["last_daily_settled_date"]:
            try:
                if date.fromisoformat(value["last_daily_settled_date"]).isoformat() != (
                        value["last_daily_settled_date"]):
                    raise ValueError
            except ValueError as exc:
                raise ValueError("memory_compression_ledger_invalid") from exc
        if not isinstance(value["entries"], list):
            raise ValueError("memory_compression_ledger_invalid")
        seen = set()
        for entry in value["entries"]:
            if not isinstance(entry, dict) or not MEM_ID_RE.fullmatch(str(entry.get("mem_id") or "")):
                raise ValueError("memory_compression_ledger_invalid")
            if entry["mem_id"] in seen:
                raise ValueError("memory_compression_ledger_duplicate")
            seen.add(entry["mem_id"])
            if entry.get("phase") not in {"pending", "prepared", "applying"}:
                raise ValueError("memory_compression_ledger_invalid")
            expected_fields = set(ENTRY_BASE_FIELDS)
            if entry.get("phase") != "pending":
                expected_fields.update(ENTRY_TARGET_FIELDS)
            if set(entry) != expected_fields:
                raise ValueError("memory_compression_ledger_invalid")
            if entry.get("reason") not in {
                    "stm_unadmitted_forgetting", "ltm_daily_degradation"}:
                raise ValueError("memory_compression_ledger_invalid")
            if entry.get("source_tier") not in {"Full", "Summary"}:
                raise ValueError("memory_compression_ledger_invalid")
            if entry.get("target_tier") not in TARGET_LIMITS:
                raise ValueError("memory_compression_ledger_invalid")
            if entry["target_tier"] != (
                    "Summary" if entry["source_tier"] == "Full" else "Abstract"):
                raise ValueError("memory_compression_ledger_invalid")
            limits = TARGET_LIMITS[entry["target_tier"]]
            if (entry.get("body_limit") != limits["body_chars"]
                    or entry.get("keyword_limit") != limits["keywords"]):
                raise ValueError("memory_compression_ledger_invalid")
            integers = (
                "sequence", "source_round", "target_weight", "body_limit",
                "keyword_limit",
            )
            if any(
                    not isinstance(entry.get(key), int)
                    or isinstance(entry.get(key), bool)
                    or entry[key] < (1 if key != "source_round" else 0)
                    for key in integers):
                raise ValueError("memory_compression_ledger_invalid")
            strings = (
                "queued_at", "source_instance_id", "body_sha256",
                "meta_sha256", "tags_sha256",
            )
            if any(not isinstance(entry.get(key), str) or not entry[key] for key in strings):
                raise ValueError("memory_compression_ledger_invalid")
            if any(
                    not re.fullmatch(r"[0-9a-f]{64}", entry[key])
                    for key in ("body_sha256", "meta_sha256", "tags_sha256")):
                raise ValueError("memory_compression_ledger_invalid")
            if entry.get("phase") != "pending":
                if not isinstance(entry.get("target_body"), str) or not entry["target_body"]:
                    raise ValueError("memory_compression_ledger_invalid")
                if not isinstance(entry.get("target_tags"), list):
                    raise ValueError("memory_compression_ledger_invalid")
                if (not 1 <= len(entry["target_tags"]) <= entry["keyword_limit"]
                        or any(
                            not isinstance(tag, str) or not _norm_keyword(tag)
                            for tag in entry["target_tags"])
                        or len({_norm_keyword(tag) for tag in entry["target_tags"]})
                        != len(entry["target_tags"])):
                    raise ValueError("memory_compression_ledger_invalid")
                if not isinstance(entry.get("target_meta"), dict):
                    raise ValueError("memory_compression_ledger_invalid")
                if any(
                        not isinstance(entry.get(key), str) or not entry[key]
                        for key in (
                            "target_body_sha256", "target_tags_sha256",
                            "target_meta_sha256",
                        )):
                    raise ValueError("memory_compression_ledger_invalid")
                if (_sha_text(entry["target_body"]) != entry["target_body_sha256"]
                        or _sha_json(entry["target_tags"]) != entry["target_tags_sha256"]
                        or _sha_json(entry["target_meta"]) != entry["target_meta_sha256"]
                        or entry["target_meta"].get("tags") != entry["target_tags"]
                        or entry["target_meta"].get("weight") != entry["target_weight"]
                        or entry["target_meta"].get("type") != (
                            "S" if entry["target_tier"] == "Summary" else "A")):
                    raise ValueError("memory_compression_ledger_invalid")
        cycle = value["active_cycle"]
        if cycle is not None:
            if not isinstance(cycle, dict) or set(cycle) != CYCLE_FIELDS:
                raise ValueError("memory_compression_ledger_invalid")
            if cycle.get("stage") not in {"stm", "ltm"}:
                raise ValueError("memory_compression_ledger_invalid")
            if not isinstance(cycle.get("frozen_mem_ids"), list):
                raise ValueError("memory_compression_ledger_invalid")
            if len(cycle["frozen_mem_ids"]) != len(set(cycle["frozen_mem_ids"])):
                raise ValueError("memory_compression_ledger_invalid")
            if any(mem_id not in seen for mem_id in cycle["frozen_mem_ids"]):
                raise ValueError("memory_compression_ledger_invalid")
            if (not cycle["frozen_mem_ids"]
                    or cycle.get("local_date") != value["last_daily_settled_date"]
                    or cycle.get("cycle_id") != f"MCD-{cycle.get('local_date')}"):
                raise ValueError("memory_compression_ledger_invalid")
            if any(
                    not isinstance(cycle.get(key), int)
                    or isinstance(cycle.get(key), bool)
                    or cycle[key] < 0
                    for key in ("round_num", "cursor")):
                raise ValueError("memory_compression_ledger_invalid")
            if any(
                    not isinstance(cycle.get(key), str) or not cycle[key]
                    for key in ("cycle_id", "local_date")):
                raise ValueError("memory_compression_ledger_invalid")
            if not isinstance(cycle.get("chronicle_receipt_hash"), str):
                raise ValueError("memory_compression_ledger_invalid")


class MemoryCompressionManager:
    """Queue, render and atomically apply rhythm memory compression batches."""

    def __init__(self, memory_store=None, ledger_store=None, now_fn=local_now,
                 instance_id=None, fault_hook=None, assembler=None):
        self.memory_store = memory_store or MemoryStore()
        self.ledger = ledger_store or MemoryCompressionStore(now_fn=now_fn)
        self.now_fn = now_fn
        self.instance_id = str(instance_id or paths.ACTIVE_INSTANCE_ID or "meta")
        self.fault_hook = fault_hook
        self.assembler = assembler

    def queue_stm_forgetting(self, mem_id, *, round_num=None):
        """Persist the pending item before deleting the current STM copy."""
        with MEMORY_MUTATION_LOCK:
            ltm = self._canonical(mem_id)
            meta = dict(ltm["meta"])
            if memory_is_admitted(meta):
                raise ValueError("memory_compression_not_pending_admission")
            target_tier, target_weight = memory_stm_forgetting_target(meta.get("weight"))
            if target_tier not in TARGET_LIMITS:
                raise ValueError("memory_compression_target_invalid")
            return self._queue(
                mem_id, reason="stm_unadmitted_forgetting",
                source_tier=ltm["tier"], target_tier=target_tier,
                target_weight=target_weight, round_num=round_num,
            )

    def settle_stm_forgetting(self, mem_id, *, round_num=None):
        """Cleanup mechanical settlement with ledger-before-delete ordering."""
        with MEMORY_MUTATION_LOCK:
            ltm = self.memory_store.ltm_entry_state(
                mem_id, include_backup=False)
            if ltm is None:
                raise ValueError(f"ltm_canonical_truth_missing:{mem_id}")
            meta = dict(ltm["meta"])
            stm = self.memory_store.stm_entry_state(mem_id)
            if not all(stm.get(key) is not None for key in ("body", "meta", "heat")):
                raise ValueError("stm_residence_incomplete")
            ltm_snapshot = self.memory_store.snapshot_ltm_files()
            stm_snapshot = self.memory_store.snapshot_stm_files()
            ledger_snapshot = self.ledger.snapshot()
            try:
                if memory_is_admitted(meta):
                    self.memory_store.verify_ltm_entry(mem_id)
                    outcome = "admitted_stm_removed"
                elif int(meta.get("weight")) <= 2:
                    if ltm["tier"] == "Pinned":
                        raise ValueError("unadmitted_pinned_memory_invalid")
                    self.memory_store.admit_ltm_entry(mem_id)
                    outcome = "unadmitted_abstract_admitted"
                else:
                    self.queue_stm_forgetting(mem_id, round_num=round_num)
                    outcome = "compression_queued"
                self._fault("after_ltm_or_ledger")
                self.memory_store.remove_stm_copy(mem_id)
                self._fault("after_stm_remove")
                if any(self.memory_store.stm_entry_state(mem_id).get(key) is not None
                       for key in ("body", "meta", "heat")):
                    raise ValueError("stm_remove_unverified")
                return {"status": "applied", "mem_id": mem_id, "outcome": outcome}
            except Exception as exc:
                self.memory_store.restore_ltm_files(ltm_snapshot)
                self.memory_store.restore_stm_files(stm_snapshot)
                self.ledger.restore(ledger_snapshot)
                raise

    def prepare_daily_cycle(self, *, local_date, round_num=None,
                            chronicle_receipt_hash=""):
        """Once per PID/date: decrement LTM and freeze the day's queue."""
        from data.ltm_degradation_store import LTMDegradationManager

        date_text = str(local_date or "").strip()
        try:
            date.fromisoformat(date_text)
        except (TypeError, ValueError):
            raise ValueError("memory_compression_daily_date_missing")
        with MEMORY_MUTATION_LOCK:
            doc = self.ledger.load()
            if doc["last_daily_settled_date"] == date_text:
                return {"status": "noop", "date": date_text,
                        "pending": len(doc["entries"])}
            if doc.get("active_cycle"):
                raise ValueError("memory_compression_cycle_active")
            ltm_snapshot = self.memory_store.snapshot_ltm_files()
            ledger_snapshot = self.ledger.snapshot()
            try:
                degradation = LTMDegradationManager(memory_store=self.memory_store)
                degradation.decrement_daily_countdowns()
                resident_item_present = getattr(
                    self.assembler, "resident_item_present", None)
                if callable(resident_item_present):
                    for mem_id, _meta, _body in degradation.due_entries(
                            "Abstract"):
                        if resident_item_present(
                                item_type="memory", item_id=mem_id):
                            raise ValueError(
                                f"resident_memory_backup_conflict:{mem_id}")
                degradation.apply_due_abstract_backups(round_num)
                for tier in ("Full", "Summary"):
                    for mem_id, _meta, _body in degradation.due_entries(tier):
                        self._queue(
                            mem_id, reason="ltm_daily_degradation",
                            source_tier=tier,
                            target_tier=("Summary" if tier == "Full" else "Abstract"),
                            target_weight=int(_meta.get("weight")),
                            round_num=round_num,
                        )
                doc = self.ledger.load()
                frozen = [entry["mem_id"] for entry in doc["entries"]]
                doc["last_daily_settled_date"] = date_text
                doc["active_cycle"] = ({
                    "cycle_id": f"MCD-{date_text}",
                    "local_date": date_text,
                    "round_num": int(round_num or 0),
                    "chronicle_receipt_hash": str(chronicle_receipt_hash or ""),
                    "stage": "stm" if any(e["reason"] == "stm_unadmitted_forgetting" for e in doc["entries"]) else "ltm",
                    "frozen_mem_ids": frozen,
                    "cursor": 0,
                } if frozen else None)
                saved = self.ledger.save(doc)
                return {"status": "applied", "date": date_text,
                        "pending": len(saved["entries"]),
                        "active_cycle": deepcopy(saved["active_cycle"])}
            except Exception:
                self.memory_store.restore_ltm_files(ltm_snapshot)
                self.ledger.restore(ledger_snapshot)
                raise

    def current_batch(self):
        doc = self.ledger.load()
        cycle = doc.get("active_cycle") or {}
        frozen = set(cycle.get("frozen_mem_ids") or [])
        pending = [e for e in doc["entries"] if e["mem_id"] in frozen]
        pending.sort(key=lambda e: (0 if e["reason"] == "stm_unadmitted_forgetting" else 1,
                                   int(e["sequence"])))
        batch = []
        total = 0
        stage = None
        for entry in pending:
            item_stage = "stm" if entry["reason"] == "stm_unadmitted_forgetting" else "ltm"
            if stage is None:
                stage = item_stage
            if item_stage != stage:
                break
            source = self._verified_source(entry)
            chars = len(source["semantic"])
            if batch and (len(batch) >= BATCH_MAX_ITEMS or total + chars > BATCH_MAX_SOURCE_CHARS):
                break
            if not batch and chars > BATCH_MAX_SOURCE_CHARS:
                raise ValueError("memory_compression_source_too_large")
            batch.append({**entry, **source})
            total += chars
        if not batch:
            return None
        return {
            "batch_id": f"MCB-{cycle.get('local_date') or 'pending'}-{int(batch[0]['sequence']):06d}",
            "cycle_id": cycle.get("cycle_id"),
            "stage": stage,
            "ledger_revision": doc["revision"],
            "source_chars": total,
            "items": batch,
        }

    def render_current_batch_material(self):
        self._verify_visible_contract()
        batch = self.current_batch()
        if not batch:
            return None
        lines = [f"记忆语义压缩材料｜批次 {batch['batch_id']}", ""]
        for item in batch["items"]:
            lines.extend([
                f"## {item['mem_id']}｜{item['title']}",
                f"来源层：{item['source_tier']}；目标层：{item['target_tier']}",
                f"正文上限：{item['body_limit']}字；关键词上限：{item['keyword_limit']}个",
                "当前关键词：" + "、".join(item["tags"]),
                "完整语义正文：",
                item["semantic"],
                "",
            ])
        return {
            "role": "system", "kind": "material",
            "source": "memory_compression_rhythm",
            "source_block_id": batch["batch_id"],
            "content": "\n".join(lines).rstrip(),
        }

    @staticmethod
    def _verify_visible_contract():
        try:
            with open(paths.DOCS_POPUP_TEMPLATE, "r", encoding="utf-8") as handle:
                text = handle.read()
        except (OSError, UnicodeError) as exc:
            raise ValueError("memory_compression_guide_template_missing") from exc
        required = (
            "记忆语义压缩节律指南",
            "submit_memory_compressions",
            "retained_keywords",
            "Summary 最多 512 字",
            "Abstract 最多 128 字",
        )
        if any(marker not in text for marker in required):
            raise ValueError("memory_compression_guide_template_invalid")

    def apply_batch(self, results, *, expected_batch_id="", round_num=None):
        with MEMORY_MUTATION_LOCK:
            batch = self.current_batch()
            if not batch:
                raise ValueError("memory_compression_batch_missing")
            if expected_batch_id and expected_batch_id != batch["batch_id"]:
                raise ValueError("memory_compression_batch_mismatch")
            normalized = self._validate_results(batch, results)
            ltm_snapshot = self.memory_store.snapshot_ltm_files()
            stm_snapshot = self.memory_store.snapshot_stm_files()
            ledger_snapshot = self.ledger.snapshot()
            before = {item["mem_id"]: item for item in batch["items"]}
            try:
                doc = self.ledger.load()
                by_id = {entry["mem_id"]: entry for entry in doc["entries"]}
                for mem_id, result in normalized.items():
                    entry = by_id[mem_id]
                    target_meta = self._build_target_meta(before[mem_id], result)
                    entry["phase"] = "prepared"
                    entry["target_body"] = result["semantic_content"]
                    entry["target_tags"] = result["retained_keywords"]
                    entry["target_meta"] = target_meta
                    entry["target_body_sha256"] = _sha_text(result["semantic_content"])
                    entry["target_tags_sha256"] = _sha_json(result["retained_keywords"])
                    entry["target_meta_sha256"] = _sha_json(target_meta)
                self.ledger.save(doc)
                self._fault("after_prepare")
                doc = self.ledger.load()
                for entry in doc["entries"]:
                    if entry["mem_id"] in normalized:
                        entry["phase"] = "applying"
                self.ledger.save(doc)
                self._fault("after_applying")
                applying_by_id = {
                    entry["mem_id"]: entry for entry in doc["entries"]
                    if entry["mem_id"] in normalized
                }
                receipts = []
                for mem_id, result in normalized.items():
                    source = before[mem_id]
                    current = self._verified_source(source)
                    applying_entry = applying_by_id[mem_id]
                    meta = dict(applying_entry["target_meta"])
                    if _sha_json(meta) != applying_entry["target_meta_sha256"]:
                        raise ValueError("memory_compression_target_meta_drift")
                    target_body = replace_memory_semantic_payload(
                        current["body"], meta.get("title") or mem_id,
                        result["semantic_content"], int(meta["weight"]),
                        tier=source["target_tier"],
                    )
                    if self.assembler is not None:
                        prospective_meta = dict(meta)
                        overlay = _read_memory_overlay()["entries"].get(
                            mem_id, {})
                        prospective_meta.update({
                            key: overlay.get(
                                key,
                                [] if key == "linked_containers" else "",
                            )
                            for key in MEMORY_OVERLAY_FIELDS
                        })
                        prospective_meta = _normalise_meta_entry(
                            prospective_meta)
                        self.assembler.preflight_resident_source_update(
                            {"item_type": "memory", "item_id": mem_id},
                            project_memory_body(target_body, prospective_meta),
                        )
                    self.memory_store.store_ltm_entry(
                        source["target_tier"], mem_id, target_body, meta,
                        source_tier=source["source_tier"],
                        admission_weight_drop=(source["reason"] == "stm_unadmitted_forgetting"),
                    )
                    self._sync_existing_stm(mem_id)
                    verified_tier = self.memory_store.verify_ltm_entry(mem_id)
                    if verified_tier != source["target_tier"]:
                        raise ValueError("memory_compression_target_unverified")
                    self._fault(f"after_apply:{mem_id}")
                    receipts.append({
                        "mem_id": mem_id,
                        "from_tier": source["source_tier"],
                        "to_tier": source["target_tier"],
                        "body_chars_before": len(current["semantic"]),
                        "body_chars_after": len(result["semantic_content"]),
                        "keywords_before": list(source["tags"]),
                        "keywords_after": list(result["retained_keywords"]),
                        "source_body_sha256": source["body_sha256"],
                        "source_meta_sha256": source["meta_sha256"],
                        "target_body_sha256": _sha_text(
                            result["semantic_content"]),
                        "target_tags_sha256": _sha_json(
                            result["retained_keywords"]),
                        "target_meta_sha256": _sha_json(meta),
                        "admission_action": (
                            "stored_at_filled"
                            if source["reason"] == "stm_unadmitted_forgetting"
                            else "stored_at_preserved"
                        ),
                        "ltm_index_verified": True,
                        "stm_projection_verified": True,
                    })
                doc = self.ledger.load()
                completed = set(normalized)
                doc["entries"] = [e for e in doc["entries"] if e["mem_id"] not in completed]
                cycle = doc.get("active_cycle") or {}
                remaining_frozen = [mid for mid in cycle.get("frozen_mem_ids") or [] if mid not in completed]
                if remaining_frozen:
                    cycle["frozen_mem_ids"] = remaining_frozen
                    cycle["cursor"] = int(cycle.get("cursor", 0)) + len(completed)
                    cycle["stage"] = ("stm" if any(
                        e["mem_id"] in remaining_frozen and e["reason"] == "stm_unadmitted_forgetting"
                        for e in doc["entries"]) else "ltm")
                    doc["active_cycle"] = cycle
                else:
                    doc["active_cycle"] = None
                saved = self.ledger.save(doc)
                self._fault("after_ledger_finish")
                return {
                    "schema_version": RECEIPT_SCHEMA,
                    "status": "applied", "batch_id": batch["batch_id"],
                    "cycle_id": batch["cycle_id"], "stage": batch["stage"],
                    "ledger_revision_before": batch["ledger_revision"],
                    "ledger_revision_after": saved["revision"],
                    "items": receipts,
                    "rollback": {"performed": False, "status": "not_needed"},
                    "remaining": len(saved["entries"]),
                }
            except Exception as exc:
                self.memory_store.restore_ltm_files(ltm_snapshot)
                self.memory_store.restore_stm_files(stm_snapshot)
                self.ledger.restore(ledger_snapshot)
                raise

    def cancel_for_recall(self, mem_id):
        """Real recall overrides a pending degradation before recall writes."""
        with MEMORY_MUTATION_LOCK:
            doc = self.ledger.load()
            entry = next((e for e in doc["entries"] if e["mem_id"] == mem_id), None)
            if entry is None:
                return {"status": "noop", "mem_id": mem_id}
            ltm_snapshot = self.memory_store.snapshot_ltm_files()
            ledger_snapshot = self.ledger.snapshot()
            try:
                ltm = self._canonical(mem_id)
                meta = dict(ltm["meta"])
                if entry["reason"] == "stm_unadmitted_forgetting" and not memory_is_admitted(meta):
                    self.memory_store.admit_ltm_entry(mem_id)
                else:
                    period = meta.get("decay_period_days")
                    if not isinstance(period, int) or period <= 0:
                        raise ValueError("invalid_decay_period_days")
                    meta["decay_countdown_days"] = period
                    self.memory_store.replace_ltm_entry(ltm["tier"], mem_id, ltm["body"], meta)
                doc["entries"] = [e for e in doc["entries"] if e["mem_id"] != mem_id]
                cycle = doc.get("active_cycle") or {}
                if mem_id in (cycle.get("frozen_mem_ids") or []):
                    cycle["frozen_mem_ids"] = [mid for mid in cycle["frozen_mem_ids"] if mid != mem_id]
                    doc["active_cycle"] = cycle if cycle["frozen_mem_ids"] else None
                self.ledger.save(doc)
                return {"status": "applied", "mem_id": mem_id,
                        "reason": "real_recall_override"}
            except Exception:
                self.memory_store.restore_ltm_files(ltm_snapshot)
                self.ledger.restore(ledger_snapshot)
                raise

    def cancel_for_pin(self, mem_id):
        return self.cancel_for_recall(mem_id)

    def has_active_cycle(self):
        return bool((self.ledger.load().get("active_cycle") or {}).get("frozen_mem_ids"))

    def reconcile_ready(self):
        """Repair provable prepared/applying tails before projection repair."""
        with MEMORY_MUTATION_LOCK:
            doc = self.ledger.load()
            changed = False
            completed = set()
            for entry in doc["entries"]:
                if entry.get("phase") == "pending":
                    self._verified_source(entry)
                    continue
                ltm = self.memory_store.ltm_entry_state(
                    entry["mem_id"], include_backup=False)
                source_matches = False
                if ltm is not None and ltm["tier"] == entry["source_tier"]:
                    semantic = extract_memory_semantic(ltm["body"])
                    source_matches = (
                        _sha_text(semantic) == entry["body_sha256"]
                        and _sha_json(ltm["meta"]) == entry["meta_sha256"]
                        and _sha_json(ltm["meta"].get("tags") or [])
                        == entry["tags_sha256"]
                    )
                target_matches = False
                if ltm is not None and ltm["tier"] == entry["target_tier"]:
                    semantic = extract_memory_semantic(ltm["body"])
                    target_matches = (
                        _sha_text(semantic) == entry.get("target_body_sha256")
                        and _sha_json(ltm["meta"].get("tags") or [])
                        == entry.get("target_tags_sha256")
                        and _sha_json(ltm["meta"])
                        == entry.get("target_meta_sha256")
                    )
                if source_matches:
                    for key in (
                        "target_body", "target_tags", "target_meta",
                        "target_body_sha256", "target_tags_sha256",
                        "target_meta_sha256",
                    ):
                        entry.pop(key, None)
                    entry["phase"] = "pending"
                    changed = True
                elif target_matches:
                    completed.add(entry["mem_id"])
                    changed = True
                else:
                    raise ValueError(
                        f"memory_compression_recovery_conflict:{entry['mem_id']}")
            if completed:
                doc["entries"] = [
                    entry for entry in doc["entries"]
                    if entry["mem_id"] not in completed
                ]
                cycle = doc.get("active_cycle") or {}
                if cycle:
                    cycle["frozen_mem_ids"] = [
                        mem_id for mem_id in cycle.get("frozen_mem_ids") or []
                        if mem_id not in completed
                    ]
                    doc["active_cycle"] = (
                        cycle if cycle["frozen_mem_ids"] else None)
            if changed:
                self.ledger.save(doc)
            return {
                "status": "repaired" if changed else "ok",
                "completed": sorted(completed),
                "pending": len(doc["entries"]),
            }

    def settle_ready_degraded_stm(self):
        """Migrate provable legacy degrade tails into the new settlement path."""
        with MEMORY_MUTATION_LOCK:
            ltm_snapshot = self.memory_store.snapshot_ltm_files()
            stm_snapshot = self.memory_store.snapshot_stm_files()
            ledger_snapshot = self.ledger.snapshot()
            try:
                heat_path = paths.HEAT_JSON
                if not os.path.isfile(heat_path):
                    return {"status": "noop", "settled": []}
                try:
                    with open(heat_path, "r", encoding="utf-8") as handle:
                        heat_doc = json.load(handle)
                except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                    raise ValueError("memory_heat_invalid") from exc
                entries = heat_doc.get("entries") if isinstance(heat_doc, dict) else None
                if not isinstance(entries, dict):
                    raise ValueError("memory_heat_invalid")
                candidates = sorted(
                    mem_id for mem_id, heat in entries.items()
                    if isinstance(heat, dict) and heat.get("degrade") is True
                )
                settled = []
                for mem_id in candidates:
                    result = self.settle_stm_forgetting(mem_id, round_num=0)
                    settled.append({
                        "mem_id": mem_id,
                        "outcome": result["outcome"],
                    })
                return {
                    "status": "repaired" if settled else "noop",
                    "settled": settled,
                }
            except Exception:
                self.memory_store.restore_ltm_files(ltm_snapshot)
                self.memory_store.restore_stm_files(stm_snapshot)
                self.ledger.restore(ledger_snapshot)
                raise

    def _queue(self, mem_id, *, reason, source_tier, target_tier,
               target_weight, round_num):
        ltm = self._canonical(mem_id)
        if ltm["tier"] != source_tier:
            raise ValueError("memory_compression_source_tier_drift")
        semantic = extract_memory_semantic(ltm["body"])
        if not semantic:
            raise ValueError("memory_semantic_content_missing")
        meta = dict(ltm["meta"])
        tags = meta.get("tags")
        if (not isinstance(tags, list) or not tags
                or any(not isinstance(tag, str) or not _norm_keyword(tag) for tag in tags)
                or len({_norm_keyword(tag) for tag in tags}) != len(tags)):
            raise ValueError("memory_source_keywords_invalid")
        fingerprint = {
            "body_sha256": _sha_text(semantic),
            "meta_sha256": _sha_json(meta),
            "tags_sha256": _sha_json(tags),
        }
        doc = self.ledger.load()
        existing = next((e for e in doc["entries"] if e["mem_id"] == mem_id), None)
        if existing is not None:
            if all(existing.get(key) == value for key, value in fingerprint.items()) and (
                    existing.get("source_tier") == source_tier
                    and existing.get("target_tier") == target_tier
                    and int(existing.get("target_weight")) == int(target_weight)):
                return {"status": "noop", "entry": deepcopy(existing)}
            raise ValueError("memory_compression_pending_conflict")
        limits = TARGET_LIMITS[target_tier]
        entry = {
            "mem_id": mem_id,
            "sequence": int(doc["next_sequence"]),
            "queued_at": _now_text(self.now_fn),
            "reason": reason,
            "source_instance_id": self.instance_id,
            "source_round": int(round_num or 0),
            "source_tier": source_tier,
            "target_tier": target_tier,
            "target_weight": int(target_weight),
            "body_limit": limits["body_chars"],
            "keyword_limit": limits["keywords"],
            **fingerprint,
            "phase": "pending",
        }
        doc["next_sequence"] += 1
        doc["entries"].append(entry)
        saved = self.ledger.save(doc)
        return {"status": "applied", "entry": deepcopy(saved["entries"][-1])}

    def _verified_source(self, entry):
        ltm = self._canonical(entry["mem_id"])
        semantic = extract_memory_semantic(ltm["body"])
        meta = dict(ltm["meta"])
        if (ltm["tier"] != entry["source_tier"]
                or _sha_text(semantic) != entry["body_sha256"]
                or _sha_json(meta) != entry["meta_sha256"]
                or _sha_json(meta.get("tags") or []) != entry["tags_sha256"]):
            raise ValueError("memory_compression_source_drift")
        return {
            "body": ltm["body"], "semantic": semantic, "meta": meta,
            "title": str(meta.get("title") or entry["mem_id"]),
            "tags": list(meta.get("tags") or []),
        }

    def _canonical(self, mem_id):
        ltm = self.memory_store.ltm_entry_state(mem_id, include_backup=False)
        if ltm is None:
            raise ValueError(f"ltm_canonical_truth_missing:{mem_id}")
        if ltm["tier"] == "Pinned":
            raise ValueError("pinned_memory_not_compressible")
        return ltm

    def _build_target_meta(self, source, result):
        current = self._verified_source(source)
        meta = dict(current["meta"])
        meta["tags"] = list(result["retained_keywords"])
        meta["weight"] = int(source["target_weight"])
        meta["type"] = "S" if source["target_tier"] == "Summary" else "A"
        if source["reason"] == "stm_unadmitted_forgetting":
            meta["stored_at"] = _now_text(self.now_fn)
        period = meta.get("decay_period_days")
        if not isinstance(period, int) or isinstance(period, bool) or period <= 0:
            raise ValueError("invalid_decay_period_days")
        meta["decay_countdown_days"] = period
        return meta

    def _validate_results(self, batch, results):
        if not isinstance(results, list):
            raise ValueError("memory_compression_results_invalid")
        expected = [item["mem_id"] for item in batch["items"]]
        if len(results) != len(expected):
            raise ValueError("memory_compression_batch_coverage_invalid")
        normalized = {}
        sources = {item["mem_id"]: item for item in batch["items"]}
        for result in results:
            if not isinstance(result, dict):
                raise ValueError("memory_compression_result_invalid")
            mem_id = str(result.get("mem_id") or "").strip()
            if mem_id in normalized or mem_id not in sources:
                raise ValueError("memory_compression_batch_coverage_invalid")
            semantic = str(result.get("semantic_content") or "").replace("\r\n", "\n").replace("\r", "\n").strip()
            if not semantic or len(semantic) > int(sources[mem_id]["body_limit"]):
                raise ValueError("memory_compression_body_limit_invalid")
            if re.search(r"<!--|^\s*#|^\s*(?:MEM-|标题[：:])", semantic, re.MULTILINE):
                raise ValueError("memory_compression_body_shape_invalid")
            selected = result.get("retained_keywords")
            if not isinstance(selected, list):
                raise ValueError("memory_compression_keywords_invalid")
            limit = int(sources[mem_id]["keyword_limit"])
            if not 1 <= len(selected) <= limit:
                raise ValueError("memory_compression_keywords_limit_invalid")
            source_tags = list(sources[mem_id]["tags"])
            lookup = {}
            for tag in source_tags:
                key = _norm_keyword(tag)
                if not key or key in lookup:
                    raise ValueError("memory_source_keywords_invalid")
                lookup[key] = tag
            selected_keys = [_norm_keyword(tag) for tag in selected]
            if any(not key or key not in lookup for key in selected_keys):
                raise ValueError("memory_compression_keyword_unknown")
            if len(set(selected_keys)) != len(selected_keys):
                raise ValueError("memory_compression_keyword_duplicate")
            chosen = set(selected_keys)
            retained = [tag for tag in source_tags if _norm_keyword(tag) in chosen]
            normalized[mem_id] = {
                "semantic_content": semantic,
                "retained_keywords": retained,
            }
        if list(normalized) != expected:
            # Coverage is set based, but application order remains frozen batch order.
            if set(normalized) != set(expected):
                raise ValueError("memory_compression_batch_coverage_invalid")
            normalized = {mem_id: normalized[mem_id] for mem_id in expected}
        return normalized

    def _sync_existing_stm(self, mem_id):
        stm = self.memory_store.stm_entry_state(mem_id)
        parts = [stm.get(key) is not None for key in ("body", "meta", "heat")]
        if not any(parts):
            return
        if not all(parts):
            raise ValueError("stm_residence_incomplete")
        ltm = self.memory_store.ltm_entry_state(mem_id, include_backup=False)
        branch_meta = dict(ltm["meta"])
        for key in ("linked_containers", "current_overview", "current_overview_updated_at"):
            if key in (stm.get("meta") or {}):
                branch_meta[key] = stm["meta"][key]
        self.memory_store.replace_stm_body(mem_id, ltm["body"])
        self.memory_store.replace_stm_meta(mem_id, branch_meta, canonical_sync=True)
        self.memory_store.rebuild_stm_index()
        self.memory_store.rebuild_stm_keywords()

    def _fault(self, stage):
        if callable(self.fault_hook):
            self.fault_hook(stage)
