"""Strict file-level retention for active Round JSONL audit streams."""
from __future__ import annotations

from datetime import datetime, timezone
import os
from pathlib import Path
import re

from data.atomic_write import atomic_write_json


RECEIPT_SCHEMA = "round_retention_receipt.v1"
ROUND_FILE_RE = re.compile(r"^round_(\d+)\.jsonl$")
DEFAULT_RETENTION = 8
MAX_RETENTION = 64
DEFAULT_MAX_MIB = 256


class RoundRetentionError(RuntimeError):
    pass


def _now():
    return datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")


def _policy_value(value, *, minimum, maximum, name):
    if isinstance(value, bool) or not isinstance(value, int):
        raise RoundRetentionError(f"{name}_invalid")
    if not minimum <= value <= maximum:
        raise RoundRetentionError(f"{name}_invalid")
    return value


def _write_receipt(path, receipt):
    if path is not None:
        atomic_write_json(str(path), receipt)


def active_round_retention_receipt_path():
    from paths import ACTIVE_PID, UPSP_LOCAL_STATE_ROOT

    return (
        Path(UPSP_LOCAL_STATE_ROOT)
        / "cache"
        / "audit"
        / ACTIVE_PID
        / "round_retention_receipt.json"
    )


def enforce_round_retention(
    round_dir,
    *,
    retention_count=DEFAULT_RETENTION,
    max_mib=DEFAULT_MAX_MIB,
    receipt_path=None,
):
    """Delete oldest managed files until both count and byte limits hold."""
    retention = _policy_value(
        retention_count,
        minimum=1,
        maximum=MAX_RETENTION,
        name="round_snapshot_retention",
    )
    maximum_mib = _policy_value(
        max_mib,
        minimum=1,
        maximum=4096,
        name="round_snapshot_max_mib",
    )
    maximum_bytes = maximum_mib * 1024 * 1024
    requested_root = Path(round_dir)
    if requested_root.is_symlink():
        raise RoundRetentionError("round_snapshot_directory_unsafe")
    root = requested_root.resolve()
    receipt_target = Path(receipt_path) if receipt_path is not None else None
    receipt = {
        "schema_version": RECEIPT_SCHEMA,
        "checked_at": _now(),
        "round_dir": str(root),
        "policy": {
            "round_snapshot_retention": retention,
            "round_snapshot_max_mib": maximum_mib,
        },
        "before": {"count": 0, "bytes": 0},
        "after": {"count": 0, "bytes": 0},
        "deleted": [],
        "freed_bytes": 0,
        "soft_overflow": False,
        "status": "ok",
    }
    try:
        if not root.exists():
            _write_receipt(receipt_target, receipt)
            return receipt
        if not root.is_dir() or root.is_symlink():
            raise RoundRetentionError("round_snapshot_directory_unsafe")
        snapshots = []
        for candidate in root.iterdir():
            match = ROUND_FILE_RE.fullmatch(candidate.name)
            if not match:
                continue
            resolved = candidate.resolve()
            if (
                candidate.is_symlink()
                or resolved.parent != root
                or not resolved.is_file()
            ):
                raise RoundRetentionError(
                    f"round_snapshot_path_unsafe:{candidate.name}"
                )
            stat = resolved.stat()
            snapshots.append({
                "round": int(match.group(1)),
                "path": resolved,
                "file": candidate.name,
                "size": int(stat.st_size),
            })
        snapshots.sort(key=lambda item: item["round"])
        receipt["before"] = {
            "count": len(snapshots),
            "bytes": sum(item["size"] for item in snapshots),
        }
        remove_count = max(0, len(snapshots) - retention)
        selected = snapshots[:remove_count]
        remaining = snapshots[remove_count:]
        remaining_bytes = sum(item["size"] for item in remaining)
        while len(remaining) > 1 and remaining_bytes > maximum_bytes:
            item = remaining.pop(0)
            selected.append(item)
            remaining_bytes -= item["size"]
        for item in selected:
            try:
                os.remove(item["path"])
            except OSError as exc:
                raise RoundRetentionError(
                    f"round_snapshot_delete_failed:{item['file']}:{exc}"
                ) from exc
            receipt["deleted"].append({
                "round": item["round"],
                "file": item["file"],
                "bytes": item["size"],
            })
            receipt["freed_bytes"] += item["size"]
        after = remaining
        receipt["after"] = {
            "count": len(after),
            "bytes": sum(item["size"] for item in after),
        }
        receipt["soft_overflow"] = bool(
            len(after) == 1 and receipt["after"]["bytes"] > maximum_bytes
        )
    except Exception as exc:
        receipt["status"] = "error"
        receipt["error"] = str(exc)
        try:
            _write_receipt(receipt_target, receipt)
        except Exception as receipt_exc:
            raise RoundRetentionError(
                f"round_retention_receipt_write_failed:{receipt_exc}"
            ) from exc
        if isinstance(exc, RoundRetentionError):
            raise
        raise RoundRetentionError(str(exc)) from exc
    _write_receipt(receipt_target, receipt)
    return receipt


def _retained_static_names(round_dir):
    root = Path(round_dir).resolve()
    if not root.is_dir() or root.is_symlink():
        return set()
    names = set()
    for path in root.iterdir():
        match = ROUND_FILE_RE.fullmatch(path.name)
        if match and path.is_file() and not path.is_symlink():
            names.add(f"round_{int(match.group(1))}.js")
    return names


def enforce_active_round_retention():
    """Load the active persona policy and write its host-side receipt."""
    from data.config_store import ConfigStore
    from paths import ACTIVE_PID, STM_CTX_ROUND_DIR, UPSP_LOCAL_STATE_ROOT

    receipt_path = active_round_retention_receipt_path()
    try:
        config, migrated = ConfigStore().migrate_system_audit_policy()
        audit = config["audit"]
    except Exception as exc:
        _write_receipt(receipt_path, {
            "schema_version": RECEIPT_SCHEMA,
            "checked_at": _now(),
            "round_dir": str(Path(STM_CTX_ROUND_DIR).resolve()),
            "status": "error",
            "stage": "policy_load",
            "error": str(exc),
        })
        raise RoundRetentionError(str(exc)) from exc
    receipt = enforce_round_retention(
        STM_CTX_ROUND_DIR,
        retention_count=audit["round_snapshot_retention"],
        max_mib=audit["round_snapshot_max_mib"],
        receipt_path=receipt_path,
    )
    from data.round_audit_static import (
        prune_stale_round_js,
        write_static_round_index,
    )

    static_dir = (
        Path(UPSP_LOCAL_STATE_ROOT) / "cache" / "audit" / ACTIVE_PID / "round-data"
    )
    try:
        static_deleted = prune_stale_round_js(
            static_dir,
            _retained_static_names(STM_CTX_ROUND_DIR),
        )
        write_static_round_index(STM_CTX_ROUND_DIR, static_dir.parent)
    except Exception as exc:
        receipt["status"] = "error"
        receipt["error"] = str(exc)
        _write_receipt(receipt_path, receipt)
        raise RoundRetentionError(str(exc)) from exc
    receipt["static_projection"] = {
        "deleted": static_deleted,
        "freed_bytes": sum(item["bytes"] for item in static_deleted),
    }
    receipt["policy_migrated"] = migrated
    _write_receipt(receipt_path, receipt)
    return receipt
