#!/usr/bin/env python3
"""UPSP repo-local CLI for diagnostics, GUI adapters, and maintenance."""
from __future__ import annotations

import argparse
import contextlib
from datetime import datetime
import importlib.util
import json
import os
from pathlib import Path
import re
import sys
from typing import Any
from urllib.parse import urlsplit
import urllib.error
import urllib.request


ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
UPSP_ROOT = ROOT / "UPSP"
PROGRAM_OS_ROOT = UPSP_ROOT / "OS"
EXECUTION_PERMISSION_ENV = "UPSP_EXECUTION_PERMISSION_LEVEL"
API_CONFIG_OVERRIDE_ENV = "UPSP_API_CONFIG_OVERRIDE_JSON"

for path in (UPSP_ROOT, PROGRAM_OS_ROOT, TOOLS_DIR):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

ACTIVE_OS_ROOT: Path
CONFIG_DIR: Path
GLOBAL_CONFIG_DIR: Path
MODELS_CONFIG: Path
MODEL_ROUTING_CONFIG: Path
PERSONA_DIR: Path
STATE_JSON: Path
ROUND_DIR: Path
_ACTIVE_PATHS_CONFIGURED = False


def _configure_active_paths() -> None:
    global _ACTIVE_PATHS_CONFIGURED
    if _ACTIVE_PATHS_CONFIGURED:
        return
    from initialization.windows_data import ensure_active_instance

    ensure_active_instance(UPSP_ROOT)
    from paths import (
        CONFIG_DIR as config_dir,
        CONFIG_MODEL_ROUTING,
        GLOBAL_CONFIG_DIR as global_config_dir,
        GLOBAL_MODELS_CONFIG,
        OS_ROOT as active_os_root,
        resolve_persona_dir,
    )

    global ACTIVE_OS_ROOT, CONFIG_DIR, GLOBAL_CONFIG_DIR
    global MODELS_CONFIG, MODEL_ROUTING_CONFIG, PERSONA_DIR, STATE_JSON, ROUND_DIR
    ACTIVE_OS_ROOT = Path(active_os_root)
    CONFIG_DIR = Path(config_dir)
    GLOBAL_CONFIG_DIR = Path(global_config_dir)
    MODELS_CONFIG = Path(GLOBAL_MODELS_CONFIG)
    MODEL_ROUTING_CONFIG = Path(CONFIG_MODEL_ROUTING)
    PERSONA_DIR = Path(resolve_persona_dir())
    STATE_JSON = PERSONA_DIR / "state.json"
    ROUND_DIR = PERSONA_DIR / "STM" / "context" / "round"
    _ACTIVE_PATHS_CONFIGURED = True


class CliError(Exception):
    def __init__(self, code: str, message: str, hint: str = ""):
        super().__init__(message)
        self.code = code
        self.message = message
        self.hint = hint


def _success(command: str, data: dict[str, Any], warnings: list[str] | None = None):
    return {
        "ok": True,
        "command": command,
        "data": data,
        "warnings": list(warnings or []),
    }


def _failure(command: str, error: CliError):
    return {
        "ok": False,
        "command": command,
        "error": {
            "code": error.code,
            "message": error.message,
            "hint": error.hint,
        },
    }


def _emit(payload: dict[str, Any], json_mode: bool) -> None:
    if json_mode:
        print(json.dumps(payload, ensure_ascii=False, sort_keys=True))
        return
    if payload.get("ok"):
        print(json.dumps(payload.get("data", {}), ensure_ascii=False, indent=2))
    else:
        error = payload.get("error", {})
        print(
            f"{error.get('code', 'error')}: {error.get('message', '')}",
            file=sys.stderr,
        )
        if error.get("hint"):
            print(error["hint"], file=sys.stderr)


@contextlib.contextmanager
def _temporary_permission_level(args):
    level = str(getattr(args, "permission_level", "") or "").strip()
    if not level:
        yield
        return
    previous = os.environ.get(EXECUTION_PERMISSION_ENV)
    os.environ[EXECUTION_PERMISSION_ENV] = level
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(EXECUTION_PERMISSION_ENV, None)
        else:
            os.environ[EXECUTION_PERMISSION_ENV] = previous


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    if spec is None or spec.loader is None:
        raise CliError(
            "module_load_failed",
            f"无法加载模块：{path}",
            "确认仓库结构完整，并从 UPSP 主仓根运行 CLI。",
        )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_os_main():
    return _load_module("upsp_os_main", PROGRAM_OS_ROOT / "main.py")


def _load_round_inspector():
    return _load_module(
        "inspect_native_tool_round",
        TOOLS_DIR / "inspect_native_tool_round.py",
    )


def _load_round_context_acceptance():
    return _load_module(
        "round_context_acceptance",
        TOOLS_DIR / "round_context_acceptance.py",
    )


def _read_json(path: Path, default: Any) -> Any:
    if not path.is_file():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _api_config_for_summary() -> tuple[dict[str, Any], bool, str]:
    override = os.environ.get(API_CONFIG_OVERRIDE_ENV, "").strip()
    if override:
        try:
            loaded = json.loads(override)
        except json.JSONDecodeError as exc:
            return {}, True, f"json_decode:{exc.msg}"
        if not isinstance(loaded, dict):
            return {}, True, "not_object"
        return loaded, True, ""
    try:
        from data.config_store import ConfigStore

        loaded = ConfigStore(use_api_environment=False).load("api")
    except Exception as exc:
        return {}, False, f"config_read:{type(exc).__name__}"
    return loaded if isinstance(loaded, dict) else {}, False, ""


def _read_round_events(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    try:
        from data.round_audit_codec import read_round_audit_file
        return read_round_audit_file(path)
    except Exception:
        return []


def _latest_round_events(round_dir: Path | None = None) -> list[dict[str, Any]]:
    round_dir = ROUND_DIR if round_dir is None else round_dir
    if not round_dir.is_dir():
        return []
    candidates: list[tuple[int, Path]] = []
    for path in round_dir.glob("round_*.jsonl"):
        match = re.match(r"^round_(\d+)\.jsonl$", path.name)
        if not match:
            continue
        try:
            round_num = int(match.group(1))
        except ValueError:
            continue
        candidates.append((round_num, path))
    if not candidates:
        return []
    _round_num, latest_path = sorted(candidates)[-1]
    return _read_round_events(latest_path)


def _terminal_state_from_round_events(events: list[dict[str, Any]]) -> dict[str, Any]:
    closed_payload: dict[str, Any] = {}
    for event in events or []:
        if not isinstance(event, dict):
            continue
        if event.get("event_type") == "round_closed":
            payload = event.get("payload") or {}
            if isinstance(payload, dict):
                closed_payload = payload
    if not closed_payload:
        return {}
    state: dict[str, Any] = {
        "status": str(closed_payload.get("status") or "").strip(),
    }
    reason = ""
    blockers: list[str] = []
    classification = ""
    latest_closeout_decision = ""
    auto_blocked_reason = ""
    auto_blocked_blockers: list[str] = []
    for event in events or []:
        if not isinstance(event, dict):
            continue
        if event.get("event_type") != "step_settlement":
            continue
        if str(event.get("phase") or "").strip() != "reaction":
            continue
        payload = event.get("payload") or {}
        if not isinstance(payload, dict):
            continue
        for ledger in payload.get("settlement_ledgers") or []:
            if not isinstance(ledger, dict):
                continue
            decision = str(ledger.get("closeout_decision") or "").strip()
            if decision:
                latest_closeout_decision = decision
            if decision == "blocked":
                reason = str(
                    ledger.get("blocked_reason") or reason or "blocked"
                ).strip()
                blockers = [
                    str(item)
                    for item in ledger.get("blockers") or []
                    if str(item or "").strip()
                ]
        for key in (
            "protocol_tool_receipts",
            "native_tool_feedbacks",
            "reaction_loop_guard_receipts",
        ):
            for receipt in payload.get(key) or []:
                if not isinstance(receipt, dict):
                    continue
                status = str(receipt.get("status") or "").strip()
                receipt_reason = str(receipt.get("reason") or "").strip()
                if "auto_blocked" in status:
                    auto_blocked_reason = receipt_reason or auto_blocked_reason or status
                    auto_blocked_blockers = auto_blocked_blockers or [
                        str(item)
                        for item in receipt.get("blockers") or []
                        if str(item or "").strip()
                    ]
    final_source = str(closed_payload.get("final_response_source") or "").strip()
    if final_source == "reaction.runtime_auto_blocked_final_reply":
        classification = "runtime_blocked_closed"
        reason = reason or auto_blocked_reason or "runtime_auto_blocked"
        blockers = blockers or auto_blocked_blockers
    elif latest_closeout_decision == "blocked":
        classification = "runtime_blocked_closed"
        reason = reason or auto_blocked_reason or "blocked"
        blockers = blockers or auto_blocked_blockers
    elif auto_blocked_reason and latest_closeout_decision not in {"finish", "continue"}:
        classification = "runtime_blocked_closed"
        reason = auto_blocked_reason
        blockers = auto_blocked_blockers
    if classification:
        state["classification"] = classification
    if reason:
        state["reason"] = reason
    if blockers:
        state["blockers"] = blockers
    return state


def _latest_terminal_state() -> dict[str, Any]:
    return _terminal_state_from_round_events(_latest_round_events())


def _configured_api_key_env_names() -> list[str]:
    cfg, _override_env, _override_error = _api_config_for_summary()
    endpoints = cfg.get("endpoints", {}) if isinstance(cfg, dict) else {}
    names: list[str] = []
    if isinstance(endpoints, dict):
        for endpoint in endpoints.values():
            if not isinstance(endpoint, dict):
                continue
            name = str(endpoint.get("api_key_env") or "").strip()
            if name and name not in names:
                names.append(name)
    return names or ["DEEPSEEK_API_KEY"]


def _redacted_url(value: str) -> str:
    if not value:
        return ""
    try:
        parsed = urlsplit(value)
    except ValueError:
        return "configured"
    if not parsed.scheme or not parsed.netloc:
        return "configured"
    return f"{parsed.scheme}://{parsed.netloc}{parsed.path}"


def _api_key_source(endpoint: dict[str, Any]) -> str:
    env_name = str(endpoint.get("api_key_env") or "").strip()
    inline_key = str(endpoint.get("api_key") or "").strip()
    if env_name and os.environ.get(env_name):
        return "env"
    if inline_key:
        return "config"
    if env_name:
        return "missing"
    return "missing"


def _api_summary() -> dict[str, Any]:
    cfg, override_env, override_error = _api_config_for_summary()
    endpoints = cfg.get("endpoints", {}) if isinstance(cfg, dict) else {}
    endpoint_summary: dict[str, Any] = {}
    if isinstance(endpoints, dict):
        for tier, endpoint in endpoints.items():
            if not isinstance(endpoint, dict):
                continue
            endpoint_id = str(endpoint.get("profile_id") or tier)
            if endpoint_id in endpoint_summary:
                continue
            endpoint_summary[endpoint_id] = {
                "url": _redacted_url(str(endpoint.get("url") or "")),
                "url_configured": bool(str(endpoint.get("url") or "").strip()),
                "provider": str(endpoint.get("provider") or ""),
                "model": str(endpoint.get("model") or ""),
                "context_window": endpoint.get("context_window", 0),
                "api_key_env": str(endpoint.get("api_key_env") or ""),
                "api_key_source": _api_key_source(endpoint),
                "prompt_cache_profile": (
                    str((endpoint.get("prompt_cache") or {}).get("profile") or "legacy")
                    if isinstance(endpoint.get("prompt_cache"), dict)
                    and endpoint.get("prompt_cache")
                    else "off"
                ),
            }
    summary = {
        "path": str(MODELS_CONFIG),
        "routing_path": str(MODEL_ROUTING_CONFIG),
        "exists": MODELS_CONFIG.is_file() and MODEL_ROUTING_CONFIG.is_file(),
        "override_env": override_env,
        "step_tiers": cfg.get("step_tiers", {}) if isinstance(cfg, dict) else {},
        "step_routes": cfg.get("step_routes", {}) if isinstance(cfg, dict) else {},
        "endpoints": endpoint_summary,
    }
    if override_error:
        summary["override_error"] = override_error
    return summary


def _runtime_import_status() -> dict[str, Any]:
    try:
        _load_os_main()
    except Exception as exc:
        return {
            "ok": False,
            "error_type": type(exc).__name__,
            "message": str(exc),
        }
    return {"ok": True, "module": str(PROGRAM_OS_ROOT / "main.py")}


def _tool_status() -> dict[str, Any]:
    names = [
        "inspect_native_tool_round.py",
        "check_encoding.py",
        "audit_upsp_consistency.py",
        "check_current_anchor.py",
    ]
    return {
        name: {
            "path": str(TOOLS_DIR / name),
            "exists": (TOOLS_DIR / name).is_file(),
        }
        for name in names
    }


def command_doctor(_args) -> tuple[dict[str, Any], list[str]]:
    data = {
        "repo_root": {"path": str(ROOT), "exists": ROOT.is_dir()},
        "upsp_root": {"path": str(UPSP_ROOT), "exists": UPSP_ROOT.is_dir()},
        "program_os_root": {
            "path": str(PROGRAM_OS_ROOT),
            "exists": PROGRAM_OS_ROOT.is_dir(),
        },
        "os_root": {
            "path": str(ACTIVE_OS_ROOT),
            "exists": ACTIVE_OS_ROOT.is_dir(),
        },
        "python": {
            "executable": sys.executable,
            "version": sys.version.split()[0],
        },
        "runtime_import": _runtime_import_status(),
        "api_config": _api_summary(),
        "state": {"path": str(STATE_JSON), "exists": STATE_JSON.is_file()},
        "round_dir": {"path": str(ROUND_DIR), "exists": ROUND_DIR.is_dir()},
        "tools": _tool_status(),
    }
    warnings: list[str] = []
    if not data["runtime_import"].get("ok"):
        warnings.append("runtime_import_failed")
    if not STATE_JSON.is_file():
        warnings.append("state_json_missing")
    if not MODELS_CONFIG.is_file() or not MODEL_ROUTING_CONFIG.is_file():
        warnings.append("api_config_missing")
    return data, warnings


def command_init(_args) -> tuple[dict[str, Any], list[str]]:
    state_existed = STATE_JSON.is_file()
    config_before: dict[str, bool] = {}
    try:
        from data.config_store import _CONFIG_MAP
        config_before = {
            name: Path(path).is_file()
            for name, (path, _default_fn) in _CONFIG_MAP.items()
        }
    except Exception:
        config_before = {}

    os_main = _load_os_main()
    with contextlib.redirect_stdout(sys.stderr):
        os_main.init_environment()

    created_configs: list[str] = []
    if config_before:
        try:
            from data.config_store import _CONFIG_MAP
            created_configs = [
                name for name, (path, _default_fn) in _CONFIG_MAP.items()
                if not config_before.get(name) and Path(path).is_file()
            ]
        except Exception:
            created_configs = []

    return {
        "initialized": True,
        "state_created": (not state_existed and STATE_JSON.is_file()),
        "created_configs": created_configs,
        "state": {"path": str(STATE_JSON), "exists": STATE_JSON.is_file()},
        "api_config": _api_summary(),
    }, []


def _round_type_from_flags(flags: dict[str, Any]) -> str | None:
    try:
        from engines.heartbeat import round_type_from_heartbeat_flags
        return round_type_from_heartbeat_flags(flags)
    except Exception:
        return None


def _round_decision_from_flags(flags: dict[str, Any]) -> dict[str, Any]:
    try:
        from engines.heartbeat import round_decision_from_heartbeat_flags
        return round_decision_from_heartbeat_flags(flags)
    except Exception:
        return {
            "round_type": _round_type_from_flags(flags),
            "guide_queue": [],
            "coalesced": False,
            "deferred_items": [],
        }


def _workbench_pending_status() -> dict[str, Any]:
    try:
        from data.workbench import WorkbenchStore
        from logic.task_acceptance import SETTLED_PENDING_INPUT_STATUSES
        from logic.work_intent_debt import current_work_intent_debt
    except Exception:
        return {}
    try:
        store = WorkbenchStore()
        slots = (
            store.active_guide_slots()
            if hasattr(store, "active_guide_slots") else {}
        )
        active_task = str(store.get("base.active_task") or "").strip()
    except Exception:
        return {}
    open_guides: list[dict[str, Any]] = []
    inconsistencies: list[dict[str, Any]] = []
    guides_dir = Path(getattr(store, "guides_dir", ""))
    if guides_dir.is_dir():
        for guide_path in guides_dir.glob("*/guide.json"):
            try:
                guide = _read_json(guide_path, {})
            except Exception:
                continue
            if not isinstance(guide, dict):
                continue
            guide_id = str(guide.get("guide_id") or guide_path.parent.name).strip()
            status = str(guide.get("status") or "open").strip().lower()
            if status not in {"completed", "superseded"}:
                open_guides.append({
                    "guide_id": guide_id,
                    "kind": str(guide.get("kind") or "").strip(),
                    "status": status,
                })
            item_statuses = [
                str((item or {}).get("status") or "").strip().lower()
                for item in guide.get("items") or []
                if isinstance(item, dict)
            ]
            if item_statuses and all(status == "completed" for status in item_statuses):
                if status not in {"completed", "superseded"}:
                    inconsistencies.append({
                        "guide_id": guide_id,
                        "reason": "items_completed_but_guide_open",
                    })
    open_pending_inputs: list[str] = []
    if active_task:
        try:
            task_guide = store.load_task_guide(active_task)
            for item in task_guide.get("pending_inputs") or []:
                if not isinstance(item, dict):
                    continue
                status = str(item.get("status") or "pending").strip().lower()
                if status in SETTLED_PENDING_INPUT_STATUSES:
                    continue
                pending_id = str(
                    item.get("pending_input_id") or item.get("id") or ""
                ).strip()
                if pending_id:
                    open_pending_inputs.append(pending_id)
        except Exception:
            pass
    debt = current_work_intent_debt(_read_json(STATE_JSON, {}))
    return {
        "active_guides": slots,
        "active_task": active_task,
        "work_intent_debt": debt,
        "open_guides": open_guides,
        "pending_guides": open_guides,
        "open_pending_inputs": open_pending_inputs,
        "guide_status_inconsistencies": inconsistencies,
    }


def command_status(_args) -> tuple[dict[str, Any], list[str]]:
    state = _read_json(STATE_JSON, {})
    base = state.get("base", {}) if isinstance(state, dict) else {}
    meta = base.get("meta", {}) if isinstance(base, dict) else {}
    flags = base.get("heartbeat_flags", {}) if isinstance(base, dict) else {}
    if not isinstance(flags, dict):
        flags = {}
    active_flags = [name for name, value in flags.items() if value]
    decision = _round_decision_from_flags(flags)
    wb_pending = _workbench_pending_status()
    data = {
        "state_exists": STATE_JSON.is_file(),
        "total_round": meta.get("total_round", 0) if isinstance(meta, dict) else 0,
        "active_flags": active_flags,
        "round_type": decision.get("round_type"),
        "guide_queue": decision.get("guide_queue") or [],
        "coalesced": bool(decision.get("coalesced")),
        "deferred_items": decision.get("deferred_items") or [],
        "phase": (
            base.get("runtime", {}).get("phase", "idle")
            if isinstance(base.get("runtime"), dict)
            else "idle"
        ),
        "api_config": _api_summary(),
        "terminal_state": _latest_terminal_state(),
    }
    data.update(wb_pending)
    return data, []


def command_send(args) -> tuple[dict[str, Any], list[str]]:
    if not args.live:
        raise CliError(
            "live_required",
            "send 会触发真实 Runtime/provider 调用，必须显式传入 --live。",
            "GUI 或脚本预检请使用 doctor/status/rounds；真实发送时再加 --live。",
        )
    message = _resolve_send_message(args)
    if not message.strip():
        raise CliError(
            "message_required",
            "send 需要非空 --message 或 --message-file。",
            "传入要发送给 UPSP Runtime 的用户消息。",
        )
    final_response_max_chars = getattr(args, "final_response_max_chars", None)
    if final_response_max_chars is not None and final_response_max_chars <= 0:
        raise CliError(
            "invalid_final_response_max_chars",
            "--final-response-max-chars 必须是正整数。",
        )
    kwargs = {"message": message}
    if final_response_max_chars is not None:
        kwargs["final_response_max_chars"] = final_response_max_chars
    result = _run_resident_command(
        "send",
        str(getattr(args, "permission_level", "limited") or "limited"),
        **kwargs,
    )
    return result, []


def _resolve_send_message(args) -> str:
    message_file = getattr(args, "message_file", None)
    if message_file:
        path = Path(message_file).expanduser()
        if not path.is_file():
            raise CliError(
                "message_file_not_found",
                f"未找到 --message-file：{path}",
                "请传入 UTF-8 文本文件路径，或改用 --message。",
            )
        return path.read_text(encoding="utf-8-sig")
    return str(getattr(args, "message", "") or "")


def command_relay(args) -> tuple[dict[str, Any], list[str]]:
    if not args.live:
        raise CliError(
            "live_required",
            "relay 会触发真实 Runtime/provider 调用，必须显式传入 --live。",
            "仅在 continue_requested 已置位、需要消费自然中继轮时使用。",
        )
    result = _run_resident_command(
        "relay",
        str(getattr(args, "permission_level", "limited") or "limited"),
    )
    return result, []


def command_tick(args) -> tuple[dict[str, Any], list[str]]:
    if not args.live:
        raise CliError(
            "live_required",
            "tick 会触发真实 Runtime/provider 调用，必须显式传入 --live。",
            "仅在需要让 Runtime 自然推进一个已 pending 轮时使用。",
        )
    result = _run_resident_command(
        "tick",
        str(getattr(args, "permission_level", "limited") or "limited"),
    )
    return result, []


def _run_resident_command(kind, permission_level, *, message=None,
                          final_response_max_chars=None):
    from engines.resident_runtime import (
        ResidentRuntimeService,
        RuntimeAlreadyRunning,
        RuntimeServiceError,
    )

    service = ResidentRuntimeService()
    try:
        try:
            with contextlib.redirect_stdout(sys.stderr):
                service.start()
        except RuntimeAlreadyRunning as exc:
            kwargs = {"message": message}
            if kind == "send" and final_response_max_chars is not None:
                kwargs["final_response_max_chars"] = final_response_max_chars
            return _call_resident_host(
                exc.host, kind, permission_level, **kwargs)
        with contextlib.redirect_stdout(sys.stderr):
            if kind == "send":
                kwargs = {}
                if final_response_max_chars is not None:
                    kwargs["final_response_max_chars"] = final_response_max_chars
                return service.submit_message(message, permission_level, **kwargs)
            return service.submit_pending(kind, permission_level)
    except RuntimeServiceError as exc:
        raise CliError(str(exc), str(exc)) from exc
    finally:
        service.close()


def _call_resident_host(host, kind, permission_level, *, message=None,
                        final_response_max_chars=None):
    address = str((host or {}).get("address") or "")
    port = (host or {}).get("port")
    if address != "127.0.0.1" or not isinstance(port, int) or port <= 0:
        raise CliError(
            "runtime_already_running",
            "当前位格已有常驻 Runtime，但监督状态没有可用宿主地址。",
        )
    path = {
        "send": "/api/runtime/send",
        "relay": "/api/runtime/relay",
        "tick": "/api/runtime/tick",
    }[kind]
    payload = {
        "permission_level": permission_level,
        "unlimited_confirmed": permission_level == "unlimited",
    }
    if kind == "send":
        payload["message"] = str(message or "")
    if kind == "send" and final_response_max_chars is not None:
        payload["final_response_max_chars"] = final_response_max_chars
    origin = f"http://{address}:{port}"
    request = urllib.request.Request(
        origin + path,
        data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
        method="POST",
        headers={
            "Content-Type": "application/json",
            "Origin": origin,
        },
    )
    try:
        with urllib.request.urlopen(request) as response:
            body = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        try:
            failure = json.loads(exc.read().decode("utf-8"))
        except (UnicodeError, json.JSONDecodeError):
            failure = {}
        code = str(failure.get("error") or f"runtime_host_http_{exc.code}")
        raise CliError(code, code, str(failure.get("detail") or "")) from exc
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CliError("runtime_host_unavailable", str(exc)) from exc
    data = body.get("data") if isinstance(body, dict) else None
    if not isinstance(data, dict):
        raise CliError("runtime_host_invalid_response", "常驻 Runtime 返回格式无效。")
    return data


def _round_num_from_path(path: Path) -> int | None:
    match = re.match(r"round_(\d+)\.jsonl$", path.name)
    if not match:
        return None
    return int(match.group(1))


def _round_files() -> list[tuple[int, Path]]:
    if not ROUND_DIR.is_dir():
        return []
    items: list[tuple[int, Path]] = []
    for path in ROUND_DIR.glob("round_*.jsonl"):
        round_num = _round_num_from_path(path)
        if round_num is None:
            continue
        items.append((round_num, path))
    items.sort(key=lambda item: item[0], reverse=True)
    return items


def _round_item(path: Path, round_num: int) -> dict[str, Any]:
    stat = path.stat()
    return {
        "round_num": round_num,
        "path": str(path),
        "mtime": stat.st_mtime,
        "mtime_iso": datetime.fromtimestamp(stat.st_mtime).isoformat(),
        "bytes": stat.st_size,
    }


def command_rounds_list(args) -> tuple[dict[str, Any], list[str]]:
    limit = max(0, int(args.limit))
    rounds = [_round_item(path, num) for num, path in _round_files()[:limit]]
    return {
        "round_dir": str(ROUND_DIR),
        "rounds": rounds,
    }, []


def _resolve_round(round_ref: str) -> Path:
    value = str(round_ref or "").strip().lower()
    rounds = _round_files()
    if value == "latest":
        if not rounds:
            raise CliError(
                "round_not_found",
                f"未找到 round_*.jsonl：{ROUND_DIR}",
                "先运行已有 Runtime 轮次，或检查 persona/STM/context/round 目录。",
            )
        return rounds[0][1]
    try:
        round_num = int(value)
    except ValueError as exc:
        raise CliError(
            "invalid_round",
            "--round 只接受 latest 或轮号整数。",
            "例如：rounds inspect --round latest",
        ) from exc
    path = ROUND_DIR / f"round_{round_num}.jsonl"
    if not path.is_file():
        raise CliError(
            "round_not_found",
            f"未找到 round 文件：{path}",
            "先用 rounds list 查看可用轮次。",
        )
    return path


def command_rounds_inspect(args) -> tuple[dict[str, Any], list[str]]:
    path = _resolve_round(args.round)
    inspector = _load_round_inspector()
    return inspector.inspect_round_file(path), []


def command_acceptance_run(args) -> tuple[dict[str, Any], list[str]]:
    acceptance = _load_round_context_acceptance()
    data = acceptance.cli_run(args)
    failed = list((data.get("summary") or {}).get("failed_checks") or [])
    if args.strict and failed:
        raise CliError(
            "acceptance_failed",
            f"整轮上下文验收失败：{', '.join(failed)}",
            f"查看报告：{data.get('report_path')}",
        )
    return data, []


def command_acceptance_export(args) -> tuple[dict[str, Any], list[str]]:
    acceptance = _load_round_context_acceptance()
    return acceptance.cli_export(args), []


def command_acceptance_import_spark_report(args) -> tuple[dict[str, Any], list[str]]:
    acceptance = _load_round_context_acceptance()
    return acceptance.cli_import_spark_report(args), []


def command_acceptance_require_spark(args) -> tuple[dict[str, Any], list[str]]:
    acceptance = _load_round_context_acceptance()
    data = acceptance.cli_require_spark(args)
    if not data.get("ok"):
        codes = [
            str(issue.get("code") or "unknown")
            for issue in data.get("issues") or []
        ]
        raise CliError(
            "spark_observation_failed",
            "Spark 真实观察门禁失败：" + ", ".join(codes),
            f"查看报告：{data.get('report_path')}",
        )
    return data, []


def _locate_item(relative_path: str) -> dict[str, Any]:
    path = ROOT / relative_path
    return {
        "path": relative_path.replace("\\", "/"),
        "exists": path.exists(),
    }


LOCATE_TOPICS: dict[str, dict[str, list[str]]] = {
    "payload": {
        "specs": [
            ".speckit/specs/422-payload-truth-source",
            ".speckit/specs/423-payload-truth-review-closeout",
            ".speckit/specs/424-payload-layer-lazy-update",
            ".speckit/specs/425-payload-truth-review-fixes",
            ".speckit/specs/426-payload-from-layers-truth",
            ".speckit/specs/427-round-live-layers-panes",
        ],
        "docs": [
            "UPSP_Base_DDS.md",
            "AGENTS.md",
            "CODEX_MEMORY.md",
        ],
        "tools": [
            "tools/upsp_cli.py",
            "tools/upsp_visible_dogfood.py",
        ],
        "tests": [
            "UPSP/OS/tests/test_engines.py",
            "UPSP/OS/tests/test_round_live_viewer.py",
        ],
    },
    "dogfood": {
        "specs": [
            ".speckit/specs/415-dogfood-evidence-external-archive",
            ".speckit/specs/430-execution-permission-level",
            ".speckit/specs/431-round-wall-timeout-dogfood-discipline",
            ".speckit/specs/432-codex-efficiency-entry-and-evidence-index",
            ".speckit/specs/433-engineering-task-execution-grant",
        ],
        "docs": [
            "docs/codex/current_entry_index.md",
            "CODEX_AGENT_SESSION.md",
        ],
        "tools": [
            "tools/upsp_visible_dogfood.py",
            "tools/upsp_dogfood_monitor.py",
            "tools/prepare_dogfood_state.py",
            "tools/upsp_dogfood_admission.py",
            "tools/upsp_evidence_index.py",
        ],
        "tests": [
            "UPSP/OS/tests/test_visible_dogfood_entrypoint.py",
            "UPSP/OS/tests/test_prepare_dogfood_state.py",
            "UPSP/OS/tests/test_codex_efficiency_tools.py",
        ],
    },
    "permission": {
        "specs": [
            ".speckit/specs/430-execution-permission-level",
            ".speckit/specs/433-engineering-task-execution-grant",
        ],
        "docs": [
            "UPSP_Base_DDS.md",
            "CODEX_CONTEXT_CHECKPOINT.md",
        ],
        "tools": [
            "tools/upsp_cli.py",
            "tools/upsp_visible_dogfood.py",
        ],
        "tests": [
            "UPSP/OS/tests/test_runtime_reaction_general_tools_write.py",
            "UPSP/OS/tests/test_upsp_cli.py",
        ],
    },
    "watchdog": {
        "specs": [
            ".speckit/specs/431-round-wall-timeout-dogfood-discipline",
        ],
        "docs": [
            "UPSP_Base_DDS.md",
            "AGENTS.md",
        ],
        "tools": [
            "tools/upsp_visible_dogfood.py",
            "UPSP/OS/data/config_store.py",
            "UPSP/initialization/os_template/config/system.json",
        ],
        "tests": [
            "UPSP/OS/tests/test_visible_dogfood_entrypoint.py",
            "UPSP/OS/tests/test_data.py",
        ],
    },
}


def command_locate(args) -> tuple[dict[str, Any], list[str]]:
    topic = str(args.topic or "").strip()
    config = LOCATE_TOPICS.get(topic)
    if config is None:
        raise CliError(
            "unknown_topic",
            f"未知 locate topic：{topic}",
            "可用 topic：payload, dogfood, permission, watchdog。",
        )
    data: dict[str, Any] = {
        "topic": topic,
        "current_worktree": str(ROOT),
        "specs": [_locate_item(path) for path in config.get("specs", [])],
        "docs": [_locate_item(path) for path in config.get("docs", [])],
        "tools": [_locate_item(path) for path in config.get("tools", [])],
        "tests": [_locate_item(path) for path in config.get("tests", [])],
        "warnings": [],
    }
    missing = [
        item["path"]
        for group in ("specs", "docs", "tools", "tests")
        for item in data[group]
        if not item["exists"]
    ]
    if missing:
        data["warnings"].append({"code": "missing_paths", "paths": missing})
    return data, []


def _command_name(args) -> str:
    if args.command == "rounds":
        return f"rounds {args.rounds_command}"
    if args.command == "acceptance":
        return f"acceptance {args.acceptance_command}"
    return str(args.command or "")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="UPSP repo-local CLI for GUI adapters and maintenance."
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Emit a stable JSON envelope to stdout.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("doctor", help="Read-only local diagnostics.")
    subparsers.add_parser("init", help="Initialize missing local runtime files.")
    subparsers.add_parser("status", help="Read current UPSP local status.")
    locate = subparsers.add_parser("locate", help="Locate current UPSP work topics.")
    locate.add_argument(
        "--topic",
        required=True,
        choices=sorted(LOCATE_TOPICS),
        help="Current read-only topic index to return.",
    )

    send = subparsers.add_parser("send", help="Send one live message.")
    message_group = send.add_mutually_exclusive_group(required=True)
    message_group.add_argument("--message")
    message_group.add_argument("--message-file", type=Path)
    send.add_argument(
        "--live",
        action="store_true",
        help="Required acknowledgement for real Runtime/provider calls.",
    )
    send.add_argument("--permission-level", choices=("limited", "guarded", "unlimited"))
    send.add_argument("--final-response-max-chars", type=int)

    relay = subparsers.add_parser("relay", help="Run one live pending relay round.")
    relay.add_argument(
        "--live",
        action="store_true",
        help="Required acknowledgement for real Runtime/provider calls.",
    )
    relay.add_argument("--permission-level", choices=("limited", "guarded", "unlimited"))
    tick = subparsers.add_parser("tick", help="Run one live pending natural round.")
    tick.add_argument(
        "--live",
        action="store_true",
        help="Required acknowledgement for real Runtime/provider calls.",
    )
    tick.add_argument("--permission-level", choices=("limited", "guarded", "unlimited"))

    rounds = subparsers.add_parser("rounds", help="Inspect round evidence.")
    rounds_sub = rounds.add_subparsers(dest="rounds_command", required=True)
    rounds_list = rounds_sub.add_parser("list", help="List recent round files.")
    rounds_list.add_argument("--limit", type=int, default=10)
    rounds_inspect = rounds_sub.add_parser("inspect", help="Inspect one round.")
    rounds_inspect.add_argument("--round", default="latest")

    acceptance = subparsers.add_parser(
        "acceptance",
        help="Run local round-context acceptance without provider calls.",
    )
    acceptance_sub = acceptance.add_subparsers(
        dest="acceptance_command",
        required=True,
    )
    acceptance_run = acceptance_sub.add_parser(
        "run",
        help="Run a deterministic fake-model round-context acceptance scenario.",
    )
    acceptance_run.add_argument("--scenario", default="coalesced_calendar_book")
    acceptance_run.add_argument("--mode", default="fake")
    acceptance_run.add_argument("--output-dir")
    acceptance_run.add_argument("--book-path")
    acceptance_run.add_argument("--envelope-path")
    acceptance_run.add_argument("--strict", action="store_true")

    acceptance_export = acceptance_sub.add_parser(
        "export",
        help="Export a Spark/subagent observation bundle from a fake run.",
    )
    acceptance_export.add_argument("--scenario", default="coalesced_calendar_book")
    acceptance_export.add_argument("--output-dir")
    acceptance_export.add_argument("--book-path")

    acceptance_import = acceptance_sub.add_parser(
        "import-spark-report",
        help="Attach a Spark observation report to an acceptance report.",
    )
    acceptance_import.add_argument("--report-path", required=True)
    acceptance_import.add_argument("--spark-report", required=True)

    acceptance_require_spark = acceptance_sub.add_parser(
        "require-spark",
        help="Require a validated Spark observation in an acceptance report.",
    )
    acceptance_require_spark.add_argument("--report-path", required=True)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    _configure_active_paths()
    command = _command_name(args)
    try:
        if args.command == "doctor":
            data, warnings = command_doctor(args)
        elif args.command == "init":
            data, warnings = command_init(args)
        elif args.command == "status":
            data, warnings = command_status(args)
        elif args.command == "locate":
            data, warnings = command_locate(args)
        elif args.command == "send":
            data, warnings = command_send(args)
        elif args.command == "relay":
            data, warnings = command_relay(args)
        elif args.command == "tick":
            data, warnings = command_tick(args)
        elif args.command == "rounds" and args.rounds_command == "list":
            data, warnings = command_rounds_list(args)
        elif args.command == "rounds" and args.rounds_command == "inspect":
            data, warnings = command_rounds_inspect(args)
        elif args.command == "acceptance" and args.acceptance_command == "run":
            data, warnings = command_acceptance_run(args)
        elif args.command == "acceptance" and args.acceptance_command == "export":
            data, warnings = command_acceptance_export(args)
        elif (
                args.command == "acceptance"
                and args.acceptance_command == "import-spark-report"):
            data, warnings = command_acceptance_import_spark_report(args)
        elif (
                args.command == "acceptance"
                and args.acceptance_command == "require-spark"):
            data, warnings = command_acceptance_require_spark(args)
        else:
            raise CliError("unknown_command", f"未知命令：{command}", "运行 --help 查看命令面。")
        payload = _success(command, data, warnings)
        _emit(payload, args.json)
        return 0
    except CliError as exc:
        _emit(_failure(command, exc), args.json)
        return 2
    except Exception as exc:
        if str(exc) == "persona_initialization_required":
            error = CliError(
                "persona_initialization_required",
                "当前尚未完成位格初始化。",
                "请先在 Seed GUI 中完成模型测试与位格创建。",
            )
            _emit(_failure(command, error), args.json)
            return 2
        error = CliError(
            "unexpected_error",
            str(exc),
            "查看 stderr 或运行 doctor 获取本地环境诊断。",
        )
        _emit(_failure(command, error), args.json)
        if not args.json:
            raise
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
