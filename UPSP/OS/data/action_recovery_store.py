"""Minimal durable action intent and crash classification for one instance."""

from __future__ import annotations

from copy import deepcopy
import hashlib
import json
import os
from pathlib import Path
import re
import threading

from data.atomic_write import atomic_write_json, durable_write_bytes
from errors import ReadError, WriteError
from paths import ACTION_RECOVERY_PENDING_JSON


SCHEMA_VERSION = "action_recovery_pending.v2"
FILE_TOOLS = frozenset({"file_write", "file_edit"})
OPAQUE_TOOLS = frozenset({"shell_command", "subagent_dispatch"})
RECOVERED_RESULT_RETENTION_ROUNDS = 8
_ACTION_ID = re.compile(r"^ACT-R\d{6,}-F\d{6,}-A\d{3,}$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")
_PHASES = {"prepared", "launching", "result_recorded", "settled"}
_CLASSIFICATIONS = {
    "", "applied_unregistered", "applied_registered", "not_applied",
    "known_result", "conflict", "outcome_unknown"}
_SUCCESS_STATUSES = {
    "ok", "success", "accepted", "applied", "completed", "guide_loaded"}
_HEAD_FIELDS = {
    "schema_version", "next_sequence", "closed_sequence", "last_closed_round", "items"}
_ITEM_FIELDS = {
    "action_id", "round", "frame_id", "iteration", "call_id", "tool_id",
    "request_sha256", "target", "target_path", "phase", "before",
    "candidate", "result", "recovery_classification", "disclosed_round",
    "resolved_closed_sequence"}


class ActionRecoveryError(RuntimeError):
    def __init__(self, message, *, action_id=""):
        super().__init__(message)
        self.action_id = str(action_id or "")


class ActionRecoveryEffectError(ActionRecoveryError):
    """The target changed but durable result settlement did not finish."""

    round_failure_flag = "_action_recovery_failed_after_effect"


def _sha(payload):
    return hashlib.sha256(bytes(payload)).hexdigest()


def _valid_int(value, minimum=0):
    return not isinstance(value, bool) and isinstance(value, int) and value >= minimum


def _success(item):
    result = item.get("result") or {}
    return str(result.get("status") or "").strip().lower() in _SUCCESS_STATUSES


def _outcome(item):
    classification = item["recovery_classification"]
    if classification.startswith("applied_"):
        return "applied"
    if classification == "known_result":
        return "known_success" if _success(item) else "known_failure"
    return classification


class ActionRecoveryStore:
    def __init__(self, path=None):
        self.path = str(path or ACTION_RECOVERY_PENDING_JSON)
        self._lock = threading.RLock()

    def load(self):
        with self._lock:
            if not os.path.isfile(self.path):
                return {"schema_version": SCHEMA_VERSION, "next_sequence": 1,
                        "closed_sequence": 0, "last_closed_round": None, "items": []}
            try:
                with open(self.path, "r", encoding="utf-8-sig") as handle:
                    raw = json.load(handle)
            except (OSError, UnicodeError, json.JSONDecodeError) as exc:
                raise ReadError(self.path, cause=exc) from exc
            return self._normalize(raw)

    @classmethod
    def _normalize(cls, raw):
        if not isinstance(raw, dict) or raw.get("schema_version") != SCHEMA_VERSION:
            raise ActionRecoveryError("action_recovery_schema_unknown")
        if set(raw) != _HEAD_FIELDS:
            raise ActionRecoveryError("action_recovery_shape_unknown")
        if (not _valid_int(raw.get("next_sequence"), 1)
                or not _valid_int(raw.get("closed_sequence"))
                or not isinstance(raw.get("items"), list)):
            raise ActionRecoveryError("action_recovery_header_invalid")
        last_closed_round = raw.get("last_closed_round")
        if last_closed_round is not None and not _valid_int(last_closed_round):
            raise ActionRecoveryError("action_recovery_header_invalid")

        seen = set()
        for source in raw["items"]:
            if not isinstance(source, dict) or set(source) != _ITEM_FIELDS:
                raise ActionRecoveryError("action_recovery_item_invalid")
            item = source
            action_id = str(item.get("action_id") or "")
            if not _ACTION_ID.fullmatch(action_id) or action_id in seen:
                raise ActionRecoveryError("action_recovery_action_id_invalid")
            strings = ("frame_id", "call_id", "target", "target_path")
            if (item.get("tool_id") not in FILE_TOOLS | OPAQUE_TOOLS
                    or not _valid_int(item.get("round"))
                    or not _valid_int(item.get("iteration"))
                    or not all(isinstance(item.get(key), str) for key in strings)
                    or not item.get("target")
                    or not _SHA256.fullmatch(str(item.get("request_sha256") or ""))
                    or item.get("phase") not in _PHASES
                    or item.get("recovery_classification") not in _CLASSIFICATIONS):
                raise ActionRecoveryError("action_recovery_item_invalid")
            for key in ("disclosed_round", "resolved_closed_sequence"):
                value = item.get(key)
                if value is not None and (not _valid_int(value) or
                        key == "resolved_closed_sequence" and value > raw["closed_sequence"]):
                    raise ActionRecoveryError("action_recovery_item_invalid")
            result = item.get("result")
            if result is not None and (
                    not isinstance(result, dict)
                    or set(result) != {"status", "reason"}
                    or not all(isinstance(value, str) for value in result.values())):
                raise ActionRecoveryError("action_recovery_result_invalid")
            if bool(result) != (item["phase"] in {"result_recorded", "settled"}):
                raise ActionRecoveryError("action_recovery_result_invalid")
            if (bool(item["recovery_classification"])
                    != (item["resolved_closed_sequence"] is not None)
                    or item["disclosed_round"] is not None
                    and not item["recovery_classification"]):
                raise ActionRecoveryError("action_recovery_item_invalid")
            cls._validate_payload(item)
            seen.add(action_id)
        return deepcopy(raw)

    @staticmethod
    def _validate_payload(item):
        before = item.get("before")
        candidate = item.get("candidate")
        if item["tool_id"] in OPAQUE_TOOLS:
            if before is not None or candidate is not None or item.get("target_path"):
                raise ActionRecoveryError("action_recovery_opaque_state_invalid")
            return
        if (not str(item.get("target_path") or "")
                or not isinstance(before, dict)
                or set(before) != {"exists", "sha256", "bytes"}
                or not isinstance(candidate, dict)
                or set(candidate) != {"sha256", "bytes"}
                or not isinstance(before.get("exists"), bool)
                or not _valid_int(before.get("bytes"))
                or bool(before["exists"]) != bool(
                    _SHA256.fullmatch(str(before.get("sha256") or "")))
                or not _valid_int(candidate.get("bytes"))
                or not _SHA256.fullmatch(str(candidate.get("sha256") or ""))):
            raise ActionRecoveryError("action_recovery_file_state_invalid")

    def _save(self, document):
        normalized = self._normalize(document)
        atomic_write_json(self.path, normalized, durable=True)
        if self.load() != normalized:
            raise WriteError(self.path, message="action_recovery_readback_mismatch")

    @staticmethod
    def _find(document, action_id):
        return next((
            item for item in document["items"]
            if item["action_id"] == str(action_id or "")), None)

    def _append(
        self, *, tool_id, request_sha256, runtime_context, call_id, target="",
        target_path="", before=None, candidate=None,
    ):
        with self._lock:
            document = self.load()
            context = runtime_context if isinstance(runtime_context, dict) else {}
            round_num = int(context.get("round_num") or 0)
            iteration = int(context.get("iteration") or 0)
            frame_id = str(context.get("frame_id") or "")
            existing = next((item for item in document["items"]
                             if item["frame_id"] == frame_id
                             and item["call_id"] == str(call_id or "")
                             and item["request_sha256"] == str(request_sha256 or "")), None)
            if existing:
                raise ActionRecoveryError(
                    "action_recovery_call_already_prepared",
                    action_id=existing["action_id"])
            sequence = int(document["next_sequence"])
            action_id = f"ACT-R{round_num:06d}-F{iteration:06d}-A{sequence:03d}"
            document["next_sequence"] = sequence + 1
            document["items"].append({
                "action_id": action_id, "round": round_num,
                "frame_id": frame_id, "iteration": iteration,
                "call_id": str(call_id or ""), "tool_id": tool_id,
                "request_sha256": str(request_sha256 or ""),
                "target": str(target or ""), "target_path": str(target_path or ""),
                "phase": "prepared", "before": deepcopy(before),
                "candidate": deepcopy(candidate), "result": None,
                "recovery_classification": "", "disclosed_round": None,
                "resolved_closed_sequence": None})
            try:
                self._save(document)
            except Exception as exc:
                raise ActionRecoveryError(
                    "action_recovery_prepare_failed", action_id=action_id) from exc
            return action_id

    def _transition(self, action_ids, phase, *, results=None):
        ids = [str(value or "") for value in action_ids if str(value or "")]
        if not ids:
            return False
        by_id = {
            str((item or {}).get("action_id") or ""): item
            for item in results or []
            if isinstance(item, dict)
        }
        with self._lock:
            document = self.load()
            for action_id in ids:
                item = self._find(document, action_id)
                if item is None:
                    raise ActionRecoveryError("action_recovery_action_missing")
                item["phase"] = phase
                result = by_id.get(action_id)
                if result is not None:
                    item["result"] = {
                        "status": str(result.get("status") or ""),
                        "reason": str(result.get("reason") or ""),
                    }
            self._save(document)
            return True

    def prepare_file(
        self, *, tool_id, request_sha256, runtime_context, call_id, target_path,
        before_bytes, candidate_bytes,
    ):
        if tool_id not in FILE_TOOLS:
            raise ActionRecoveryError("action_recovery_file_tool_invalid")
        return self._append(
            tool_id=tool_id, request_sha256=request_sha256,
            runtime_context=runtime_context, call_id=call_id,
            target=(f"{Path(target_path).name}#" + hashlib.sha256(
                os.path.normcase(str(Path(target_path).resolve())).encode("utf-8")
            ).hexdigest()[:12]), target_path=target_path,
            before={
                "exists": before_bytes is not None,
                "sha256": _sha(before_bytes) if before_bytes is not None else "",
                "bytes": len(before_bytes) if before_bytes is not None else 0},
            candidate={"sha256": _sha(candidate_bytes),
                       "bytes": len(candidate_bytes)})

    def commit_file(self, action_id, target_path, before_bytes, candidate_bytes):
        with self._lock:
            document = self.load()
            item = self._find(document, action_id)
            if item is None or item["tool_id"] not in FILE_TOOLS:
                raise ActionRecoveryError("action_recovery_action_missing")
            before = {"exists": before_bytes is not None,
                      "sha256": _sha(before_bytes) if before_bytes is not None else "",
                      "bytes": len(before_bytes) if before_bytes is not None else 0}
            candidate = {"sha256": _sha(candidate_bytes),
                         "bytes": len(candidate_bytes)}
            if (item["target_path"] != str(target_path)
                    or item["before"] != before or item["candidate"] != candidate):
                raise ActionRecoveryError("action_recovery_record_mismatch")
            target = Path(target_path)
            if target.exists() and not target.is_file():
                raise ActionRecoveryError("action_recovery_target_drift")
            current = target.read_bytes() if target.is_file() else None
            if current != before_bytes:
                raise ActionRecoveryError("action_recovery_target_drift")
            durable_write_bytes(target, candidate_bytes)
            try:
                written = target.read_bytes() if target.is_file() else None
            except OSError as exc:
                raise ActionRecoveryEffectError(
                    "action_recovery_target_readback_failed",
                    action_id=action_id) from exc
            if written != candidate_bytes:
                raise ActionRecoveryEffectError(
                    "action_recovery_target_readback_mismatch", action_id=action_id)

    def prepare_opaque(
        self, *, tool_id, request_sha256, runtime_context, call_id, target="",
    ):
        if tool_id not in OPAQUE_TOOLS:
            raise ActionRecoveryError("action_recovery_opaque_tool_invalid")
        action_id = self._append(
            tool_id=tool_id, request_sha256=request_sha256,
            runtime_context=runtime_context, call_id=call_id, target=target)
        try:
            self._transition([action_id], "launching")
        except Exception as exc:
            if isinstance(exc, ActionRecoveryError):
                exc.action_id = exc.action_id or action_id
                raise
            raise ActionRecoveryError(
                "action_recovery_launch_journal_failed", action_id=action_id) from exc
        return action_id

    def record_results(self, results):
        return self._transition(
            [str((item or {}).get("action_id") or "") for item in results or []],
            "result_recorded", results=results)

    def settle_results(self, results):
        return self._transition(
            [str((item or {}).get("action_id") or "") for item in results or []],
            "settled", results=results)

    @staticmethod
    def _classify_file(item, current, later_before_sha):
        before = item["before"]
        candidate = item["candidate"]
        current_sha = _sha(current) if current is not None else ""
        before_matches = (bool(before["exists"]) == (current is not None)
                          and before["sha256"] == current_sha)
        candidate_matches = current is not None and candidate["sha256"] == current_sha
        same_content = bool(before["exists"]) and candidate["sha256"] == before["sha256"]
        later_proves_applied = not same_content and later_before_sha == candidate["sha256"]
        no_op = same_content and before_matches
        if later_proves_applied or (candidate_matches and not no_op):
            return (
                "applied_registered"
                if item["phase"] in {"result_recorded", "settled"}
                else "applied_unregistered"
            )
        if item["result"] is not None and not _success(item):
            return "not_applied" if before_matches else "conflict"
        if candidate_matches and item["phase"] != "prepared":
            return "applied_registered"
        if before_matches:
            return "not_applied"
        return "conflict"

    def classify_interrupted(self, round_num):
        with self._lock:
            document = self.load()
            items = [item for item in document["items"]
                     if item["round"] == int(round_num)]
            if not items:
                summary = self.summary(document)
                summary["round"] = int(round_num)
                return summary
            later_before_by_path = {}
            later_before = {}
            for item in reversed(items):
                if item["tool_id"] in FILE_TOOLS:
                    later_before[item["action_id"]] = later_before_by_path.get(
                        item["target_path"], "")
                    later_before_by_path[item["target_path"]] = item["before"]["sha256"]
            for item in items:
                if item["recovery_classification"]:
                    continue
                if item["tool_id"] in FILE_TOOLS:
                    try:
                        path = Path(item["target_path"])
                        if path.exists() and not path.is_file():
                            classification = "conflict"
                        else:
                            current = path.read_bytes() if path.is_file() else None
                            classification = self._classify_file(
                                item, current,
                                later_before.get(item["action_id"], ""))
                    except OSError:
                        classification = "outcome_unknown"
                elif item["phase"] in {"result_recorded", "settled"}:
                    classification = "known_result"
                elif item["phase"] == "prepared":
                    classification = "not_applied"
                else:
                    classification = "outcome_unknown"
                item["recovery_classification"] = classification
                item["resolved_closed_sequence"] = document["closed_sequence"]
            self._save(document)
            return self.summary(document)

    def pending_items(self, document=None):
        document = document or self.load()
        return [deepcopy(item) for item in document["items"]
                if item["recovery_classification"]
                and item["disclosed_round"] is None]

    def summary(self, document=None):
        document = document or self.load()
        items = self.pending_items(document)
        counts = {key: 0 for key in (
            "applied_unregistered", "applied_registered", "not_applied",
            "known_result", "conflict", "outcome_unknown")}
        for item in items:
            counts[item["recovery_classification"]] += 1
        return {"pending": bool(items),
                "round": min((item["round"] for item in items), default=None),
                **counts}

    def recovery_receipt(self, document=None, *, pending_only=False):
        document = document or self.load()
        items = [item for item in document["items"]
                 if item["recovery_classification"]
                 and (not pending_only or item["disclosed_round"] is None)]
        if not items:
            return None
        completion = []
        blockers = []
        public_items = []
        for item in items:
            ref = f"action:{item['action_id']}"
            outcome = _outcome(item)
            if outcome in {"applied", "known_success"}:
                completion.append(ref)
            else:
                blockers.append(ref)
            result = item.get("result") or {}
            public_items.append({
                "action_id": item["action_id"], "tool_id": item["tool_id"],
                "target": item.get("target") or "", "outcome": outcome,
                "result_status": result.get("status") or "",
                "result_reason": result.get("reason") or "",
                "evidence_ref": ref})
        return {
            "schema_version": "action_recovery_receipt.v2",
            "tool_id": "runtime_action_recovery", "status": "applied",
            "items": public_items, "completion_evidence_refs": completion,
            "blocker_evidence_refs": blockers,
            "evidence_refs": completion, "source_refs": completion + blockers}

    def mark_disclosed(self, round_num):
        with self._lock:
            document = self.load()
            changed = False
            for item in document["items"]:
                if (item["recovery_classification"]
                        and item["disclosed_round"] is None):
                    item["disclosed_round"] = int(round_num)
                    changed = True
            if changed:
                self._save(document)
            return changed

    def recovered_results(self, document=None):
        document = document or self.load()
        results = []
        for item in document["items"]:
            classification = item["recovery_classification"]
            if not classification:
                continue
            blocked = classification in {"conflict", "outcome_unknown"}
            reusable = _outcome(item) in {"applied", "known_success"}
            if not blocked and not reusable:
                continue
            result = item.get("result") or {}
            results.append({
                "tool_id": item["tool_id"],
                "tool_signature": item["request_sha256"],
                "call_id": item["call_id"],
                "status": "blocked" if blocked else "ok",
                "reason": (f"interrupted_action_{classification}"
                           if blocked else result.get("reason") or ""),
                "dispatch_stage": "handler", "action_id": item["action_id"],
                "evidence_refs": [f"action:{item['action_id']}"],
                "recovered": True})
        return results

    def note_round_closed(self, round_num):
        with self._lock:
            if not os.path.isfile(self.path):
                return False
            document = self.load()
            changed = False
            for item in list(document["items"]):
                if (item["round"] == int(round_num)
                        and item["phase"] == "settled"
                        and not item["recovery_classification"]):
                    document["items"].remove(item)
                    changed = True
            recovered = [item for item in document["items"] if item["recovery_classification"]]
            retained_ids = {item["action_id"] for item in recovered
                            if _outcome(item) in {"applied", "known_success"}}
            previous = document["last_closed_round"]
            if retained_ids and (previous is None or int(round_num) > previous):
                document["closed_sequence"] += 1
                document["last_closed_round"] = int(round_num)
                changed = True
            current_closed = document["closed_sequence"]
            for item in list(recovered):
                if item["recovery_classification"] in {"conflict", "outcome_unknown"}:
                    continue
                if (item["disclosed_round"] is not None
                        and item["action_id"] not in retained_ids):
                    document["items"].remove(item)
                    changed = True
                    continue
                resolved = item["resolved_closed_sequence"]
                if (item["disclosed_round"] is not None and resolved is not None
                        and current_closed >=
                        resolved + RECOVERED_RESULT_RETENTION_ROUNDS):
                    document["items"].remove(item)
                    changed = True
            if changed:
                self._save(document)
            return changed
