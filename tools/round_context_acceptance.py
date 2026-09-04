#!/usr/bin/env python3
"""Local round-context acceptance runner for UPSP.

This runner never calls a real provider. It builds an isolated Runtime with a
deterministic executor so specs can inspect the actual assembled messages,
tool headers, and settlement behavior before dogfood.
"""
from __future__ import annotations

from contextlib import contextmanager
from datetime import datetime
import json
import os
from pathlib import Path
import re
import shutil
import sys
import tempfile
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
TOOLS_DIR = ROOT / "tools"
UPSP_ROOT = ROOT / "UPSP"
PROGRAM_OS_ROOT = UPSP_ROOT / "OS"
DEFAULT_BOOK_PATH = Path(
    r"D:\AI_WORKSPACE\base\book\共格主体论_V5_6.1.md"
)

for path in (PROGRAM_OS_ROOT, TOOLS_DIR):
    text = str(path)
    if text not in sys.path:
        sys.path.insert(0, text)

from paths import PERSONA_DIR  # noqa: E402

LIVE_PERSONA = Path(PERSONA_DIR)


def _json_dump(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
        newline="\n",
    )


def _read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_name(tool: dict[str, Any]) -> str:
    name = str(tool.get("name") or "").strip()
    if name:
        return name
    function = tool.get("function")
    if isinstance(function, dict):
        return str(function.get("name") or "").strip()
    return ""


def _tool_envelope(
        tool_id: str,
        arguments: dict[str, Any] | None = None,
        *,
        call_id: str,
        tool_class: str = "read_tool",
        risk: str = "medium",
        index: int = 0) -> dict[str, Any]:
    arguments = dict(arguments or {})
    return {
        "schema_version": "tool_call_envelope.v1",
        "source": "provider_tool_call",
        "provider": "openai_responses",
        "endpoint": "acceptance_fake",
        "response_id": f"resp_{call_id}",
        "call_id": call_id,
        "provider_item_id": f"fc_{call_id}",
        "index": index,
        "raw_type": "function_call",
        "tool_id": tool_id,
        "arguments": arguments,
        "arguments_json": json.dumps(arguments, ensure_ascii=False),
        "tool_class": tool_class,
        "risk": risk,
        "parse_status": "ok",
    }


def _reaction_finalize(call_id: str, **arguments: Any) -> dict[str, Any]:
    arguments.setdefault("closeout_decision", "finish")
    return _tool_envelope(
        "reaction_finalize",
        arguments,
        call_id=call_id,
        tool_class="sync_tool",
        risk="high",
    )


def _cleanup_finalize(call_id: str = "call_acceptance_cleanup") -> dict[str, Any]:
    return _tool_envelope(
        "cleanup_finalize",
        {},
        call_id=call_id,
        tool_class="sync_tool",
        risk="high",
    )


def _guide_submit_write_chronicle(
        call_id: str,
        *,
        guide_id: str | None = None,
        item_id: str = "calendar_day_due",
        content: str = "整轮验收：节律账已结算，继续处理用户输入。",
        reason: str = "round context acceptance rhythm settlement") -> dict[str, Any]:
    return _tool_envelope(
        "guide_submit",
        {
            "guide_id": guide_id or "rhythm:calendar_day:R000001",
            "submissions": [{
                "item_id": item_id,
                "option_id": "write_chronicle",
                "fields": {
                    "content": content,
                    "reason": reason,
                },
            }],
        },
        call_id=call_id,
        tool_class="sync_tool",
        risk="high",
    )


def _setup_finalize(call_id: str = "call_acceptance_setup") -> dict[str, Any]:
    return _tool_envelope(
        "setup_finalize",
        {
            "security_verdict": "pass",
            "mount_requests": [],
            "interaction_object": "Codex",
            "identity_status": "known",
            "interaction_source": "round_context_acceptance",
        },
        call_id=call_id,
        tool_class="sync_tool",
        risk="high",
    )


def _message_digest(messages: list[dict[str, Any]]) -> list[dict[str, Any]]:
    digest = []
    for message in messages or []:
        digest.append({
            "role": message.get("role"),
            "kind": message.get("kind", ""),
            "content": str(message.get("content") or ""),
        })
    return digest


def _contains(messages: list[dict[str, Any]], needle: str) -> bool:
    return any(needle in str(message.get("content") or "") for message in messages)


def _role_contains(messages: list[dict[str, Any]], role: str, needle: str) -> bool:
    return any(
        message.get("role") == role
        and needle in str(message.get("content") or "")
        for message in messages or []
    )


def _visible_rhythm_guide_id(messages: list[dict[str, Any]]) -> str:
    text = "\n".join(str(message.get("content") or "") for message in messages or [])
    match = re.search(r"rhythm:calendar(?:_[a-z]+)?:R\d{6}", text)
    if match:
        return match.group(0)
    return "rhythm:calendar_day:R000001"


class FakeAcceptanceExecutor:
    def __init__(self, *, book_path: Path):
        self.book_path = Path(book_path)
        self.calls: list[dict[str, Any]] = []
        self.reaction_calls = 0
        self.provider_calls: list[dict[str, Any]] = []

    def call(
            self,
            step,
            system,
            messages,
            model=None,
            endpoint=None,
            active_protocol_tool_guides=None):
        from logic.native_tool_calls import export_provider_tool_schemas
        from logic.runtime_channels import channel_for_step

        active_protocol_tool_guides = list(active_protocol_tool_guides or [])
        channel = channel_for_step(
            step,
            active_protocol_tool_guides=active_protocol_tool_guides,
        )
        terminal_tools = [channel.terminal_tool] if channel.terminal_tool else []
        if channel.include_standard_tools:
            native_tools = export_provider_tool_schemas(
                provider="openai_responses",
                include_protocol_writes=channel.include_protocol_writes,
                include_step_terminal_tools=terminal_tools,
                active_protocol_tool_guides=active_protocol_tool_guides,
            )
        else:
            native_tools = export_provider_tool_schemas(
                provider="openai_responses",
                include_step_terminal_tools=terminal_tools,
                include_standard_tools=False,
            )
        tool_names = [
            _schema_name(tool) for tool in native_tools if _schema_name(tool)
        ]
        audit = {
            "step": step,
            "provider": "acceptance_fake",
            "model": "deterministic-fake",
            "tool_names": tool_names,
            "terminal_tool": channel.terminal_tool or None,
            "tool_mode": channel.tool_mode or None,
            "tools_transmitted": bool(tool_names),
            "standard_tools_enabled": channel.include_standard_tools,
        }
        call_record = {
            "step": step,
            "channel": channel.name,
            "active_protocol_tool_guides": active_protocol_tool_guides,
            "tool_names": tool_names,
            "tool_mode": channel.tool_mode or None,
            "system_chars": len(str(system or "")),
            "messages": _message_digest(list(messages or [])),
        }
        self.visible_rhythm_guide_id = _visible_rhythm_guide_id(list(messages or []))
        result = self._result_for_step(str(step or ""), channel.name)
        result.setdefault("tokens_input", 0)
        result.setdefault("tokens_output", 0)
        result.setdefault("latency_ms", 0)
        result.setdefault("model", "deterministic-fake")
        result.setdefault("endpoint", "acceptance_fake")
        result["request_contract_audit"] = audit
        call_record["result_response"] = str(result.get("response") or "")
        call_record["result_tool_ids"] = [
            item.get("tool_id")
            for item in result.get("tool_call_envelopes", []) or []
        ]
        self.calls.append(call_record)
        return result

    def _result_for_step(self, step: str, channel_name: str) -> dict[str, Any]:
        logical_step = "final_reply" if channel_name == "final_reply" else step
        if logical_step == "setup":
            return {"response": "", "tool_call_envelopes": [_setup_finalize()]}
        if logical_step == "cleanup":
            return {"response": "", "tool_call_envelopes": [_cleanup_finalize()]}
        if logical_step == "reaction":
            self.reaction_calls += 1
            if self.reaction_calls == 1 and channel_name == "reaction.loop":
                return {
                    "response": "我先处理节律账，随后继续处理你的读书请求。",
                    "tool_call_envelopes": [],
                }
            if self.reaction_calls == 2:
                return {
                    "response": "",
                    "tool_call_envelopes": [_guide_submit_write_chronicle(
                        "call_acceptance_guide_submit_chronicle",
                        guide_id=getattr(self, "visible_rhythm_guide_id", ""),
                    )],
                }
            if self.reaction_calls == 3:
                return {
                    "response": "",
                    "tool_call_envelopes": [_tool_envelope(
                        "file_read",
                        {
                            "path": str(self.book_path),
                            "reason": "acceptance fake reads the requested book path",
                        },
                        call_id="call_acceptance_file_read",
                    )],
                }
            return {
                "response": (
                    "已先结算节律账，并读取了你指定的读书材料；"
                    "后续应继续围绕该材料推进。"
                ),
                "tool_call_envelopes": [],
            }
        return {"response": "", "tool_call_envelopes": []}


class ScriptedAcceptanceExecutor(FakeAcceptanceExecutor):
    def __init__(self, *, book_path: Path, envelope: dict[str, Any]):
        super().__init__(book_path=book_path)
        self.events = list(envelope.get("events") or [])
        self.event_index = 0

    def _result_for_step(self, step: str, channel_name: str) -> dict[str, Any]:
        logical_step = "final_reply" if channel_name == "final_reply" else step
        if logical_step in {"setup", "cleanup"}:
            return super()._result_for_step(step, channel_name)
        event = self._next_event(logical_step)
        if event is None:
            return {
                "response": "Spark 信封未给自然语言最终回复，Runtime 生成空回复。",
                "tool_call_envelopes": [],
            }
        response = str(event.get("text") or "").strip()
        tool_calls = [
            _event_tool_call_to_envelope(item, index=index)
            for index, item in enumerate(event.get("tool_calls") or [])
        ]
        return {"response": response, "tool_call_envelopes": tool_calls}

    def _next_event(self, step: str) -> dict[str, Any] | None:
        while self.event_index < len(self.events):
            event = self.events[self.event_index]
            self.event_index += 1
            if str(event.get("step") or "").strip() == step:
                return event
        return None


def _event_tool_call_to_envelope(
        item: dict[str, Any],
        *,
        index: int = 0) -> dict[str, Any]:
    tool_id = str(item.get("tool_id") or "").strip()
    arguments = dict(item.get("arguments") or {})
    if tool_id in {"setup_finalize", "reaction_finalize", "cleanup_finalize"}:
        return _tool_envelope(
            tool_id,
            arguments,
            call_id=str(item.get("call_id") or f"call_spark_{tool_id}_{index}"),
            tool_class="sync_tool",
            risk="high",
            index=index,
        )
    if tool_id in {"chronicle_write", "guide_submit"}:
        return _tool_envelope(
            tool_id,
            arguments,
            call_id=str(item.get("call_id") or f"call_spark_{tool_id}_{index}"),
            tool_class="sync_tool",
            risk="medium",
            index=index,
        )
    return _tool_envelope(
        tool_id,
        arguments,
        call_id=str(item.get("call_id") or f"call_spark_{tool_id}_{index}"),
        tool_class="read_tool",
        risk="medium",
        index=index,
    )


def validate_spark_envelope(envelope: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if envelope.get("schema_version") != "spark_simulation_envelope.v1":
        issues.append({
            "code": "invalid_schema_version",
            "message": "Spark 信封 schema_version 必须是 spark_simulation_envelope.v1。",
        })
        return issues
    for event in envelope.get("events") or []:
        step = str(event.get("step") or "").strip()
        tool_calls = list(event.get("tool_calls") or [])
        text = str(event.get("text") or "").strip()
        if step == "final_reply" and tool_calls:
            issues.append({
                "code": "final_reply.invalid_tool",
                "message": "assistant_text 只能是自然语言文本，不允许工具调用。",
            })
    return issues


SPARK_OBSERVATION_TRUE_FIELDS = {
    "seen_user_input": "Spark 必须确认看见真实用户输入。",
    "seen_chronicle_material": "Spark 必须确认看见编年史写入材料。",
    "seen_natural_final_reply": "Spark 必须确认看见自然语言最终回复投影。",
    "would_handle_user_task": "Spark 必须判断会继续处理真实用户任务。",
}
SPARK_OBSERVATION_FALSE_FIELDS = {
    "trapped_or_confused": "Spark 不能判断自己会被上下文困住。",
    "would_misroute_relay": "Spark 不能判断会误跳中继。",
    "would_clear_user_task_too_early": "Spark 不能判断会过早清理用户任务。",
}


def validate_spark_observation(report: dict[str, Any]) -> list[dict[str, str]]:
    issues: list[dict[str, str]] = []
    if not isinstance(report, dict):
        return [{
            "code": "spark_observation_invalid_type",
            "message": "Spark 观察报告必须是 JSON object。",
        }]
    if report.get("schema_version") != "spark_observation.v1":
        issues.append({
            "code": "spark_observation_invalid_schema",
            "message": "Spark 观察报告 schema_version 必须是 spark_observation.v1。",
        })
        return issues
    for field, message in SPARK_OBSERVATION_TRUE_FIELDS.items():
        if field not in report:
            issues.append({
                "code": f"spark_observation_missing_{field}",
                "message": f"Spark 观察报告缺少字段：{field}。",
            })
        elif report.get(field) is not True:
            issues.append({
                "code": f"spark_observation_{field}_not_true",
                "message": message,
            })
    for field, message in SPARK_OBSERVATION_FALSE_FIELDS.items():
        if field not in report:
            issues.append({
                "code": f"spark_observation_missing_{field}",
                "message": f"Spark 观察报告缺少字段：{field}。",
            })
        elif report.get(field) is not False:
            issues.append({
                "code": f"spark_observation_{field}_not_false",
                "message": message,
            })
    if not str(report.get("likely_next_action") or "").strip():
        issues.append({
            "code": "spark_observation_missing_likely_next_action",
            "message": "Spark 观察报告必须写明 likely_next_action。",
        })
    if not isinstance(report.get("risk_items"), list):
        issues.append({
            "code": "spark_observation_invalid_risk_items",
            "message": "Spark 观察报告 risk_items 必须是数组。",
        })
    if not str(report.get("notes") or "").strip():
        issues.append({
            "code": "spark_observation_missing_notes",
            "message": "Spark 观察报告必须包含 notes。",
        })
    return issues


class InMemoryHeat:
    def load_heat(self):
        return {"entries": {}}

    def check_upgrade(self):
        return []

    def recall_boost(self, *args, **kwargs):
        return None

    def tick_decay(self, *args, **kwargs):
        return None

    def has_pending_degrade(self):
        return False


class NoopMemoryStore:
    def load_meta(self):
        return []

    def active_ltm_meta_by_id(self):
        return {}

    def __getattr__(self, _name):
        def _noop(*args, **kwargs):
            return [] if _name.startswith("load") else None
        return _noop


class NoopMemoryIndex:
    def __getattr__(self, _name):
        def _noop(*args, **kwargs):
            return None
        return _noop


class NoopRelationStore:
    def load_registry(self):
        return {"cards": []}

    def read_card(self, *args, **kwargs):
        return None

    def set_summary_resident(self, *args, **kwargs):
        return None

@contextmanager
def _extra_file_read_root(path: Path):
    old_value = os.environ.get("UPSP_FILE_READ_EXTRA_ROOTS")
    roots = [str(Path(path).resolve())]
    if old_value:
        roots.append(old_value)
    os.environ["UPSP_FILE_READ_EXTRA_ROOTS"] = os.pathsep.join(roots)
    try:
        yield
    finally:
        if old_value is None:
            os.environ.pop("UPSP_FILE_READ_EXTRA_ROOTS", None)
        else:
            os.environ["UPSP_FILE_READ_EXTRA_ROOTS"] = old_value


def _build_runtime(runtime_root: Path, executor: FakeAcceptanceExecutor):
    from assembly.context import ContextAssembler
    from data.chronicle_store import ChronicleStore
    from data.connectivity_store import ConnectivityStore
    from data.context_store import ContextStore
    from data.config_store import ConfigStore
    from data.state_store import StateStore
    from data.workbench import WorkbenchStore
    from engines.general_tool_dispatcher import GeneralToolDispatcher
    from engines.runtime import Runtime

    sm = StateStore(str(runtime_root / "state.json"))
    sm.init_if_missing()
    ctx_store = ContextStore(
        state_store=sm,
        cache_dir=str(runtime_root / "context_cache"),
        corpus_rhythms_dir=str(runtime_root / "corpus" / "public" / "rhythms"),
        raw_log_jsonl=str(runtime_root / "buffer" / "raw_log.jsonl"),
        raw_log_md=str(runtime_root / "buffer" / "raw_log.md"),
    )
    assembler = ContextAssembler(
        state_store=sm,
        context_dir=str(runtime_root / "context"),
        context_store=ctx_store,
    )
    config_store = ConfigStore()
    config_store.get_round_context_window_tokens = lambda: 1_000_000
    rt = Runtime(
        state_store=sm,
        executor=executor,
        assembler=assembler,
        ctx_store=ctx_store,
        config_store=config_store,
        workbench_store=WorkbenchStore(str(runtime_root / "workbench")),
        connectivity_store=ConnectivityStore(str(runtime_root / "connectivity.json")),
        heat=InMemoryHeat(),
        memory_store=NoopMemoryStore(),
        memory_index=NoopMemoryIndex(),
        relation_store=NoopRelationStore(),
    )
    rt.hb.pause = lambda *args, **kwargs: None
    rt.hb.resume = lambda *args, **kwargs: None
    rt.hb.dequeue_messages = lambda: [
        "请读取并内化这本书。path=" + str(executor.book_path)
    ]
    rt.reaction_loop_runner.chronicle_store = ChronicleStore(
        str(runtime_root / "LTM" / "Chronicle")
    )
    rt.general_tool_dispatcher = GeneralToolDispatcher(
        load_guide_fn=lambda tool_id: (
            "file_read guide" if tool_id == "file_read" else ""
        ),
        execute_fn=lambda request: _fake_general_tool_execute(
            request,
            executor.book_path,
        ),
    )
    _patch_cleanup_for_isolation(rt)
    return rt


def _fake_general_tool_execute(request: dict[str, Any], book_path: Path) -> dict[str, Any]:
    path = Path(str(request.get("path") or ""))
    content = ""
    status = "ok"
    reason = ""
    try:
        if path.resolve() == book_path.resolve():
            content = book_path.read_text(encoding="utf-8")[:1200]
        else:
            status = "rejected"
            reason = "outside_acceptance_book"
    except Exception as exc:
        status = "error"
        reason = str(exc)
    return {
        "tool_id": "file_read",
        "tool_class": "read_tool",
        "status": status,
        "reason": reason,
        "source": "general_tool_call",
        "backend_type": "python",
        "handler": "file_read_handler",
        "permission_scope": "round_context_acceptance_fake",
        "result_kind": "general_tool_result",
        "call_id": request.get("call_id"),
        "path": str(path),
        "content": content,
        "has_more": False,
        "read_mode": "bounded",
        "protocol_tool_receipt": False,
    }


def _patch_cleanup_for_isolation(rt) -> None:
    pipeline = rt.cleanup_pipeline
    pipeline._build_forgetting_context = lambda *args, **kwargs: ""
    for name in (
            "_settle_round_retention",
            "_process_calendar_cleanup",
            "_cleanup_trash",
            "_process_memory_lifecycle",
            "_process_forgetting_result",
            "_process_evolution_set",
            "_process_rest_cycle",
            "_process_cleanup_output",
            "_process_forgetting_result"):
        setattr(pipeline, name, lambda *args, **kwargs: None)
    rt.ctx_store.save_round_to_cache = lambda *args, **kwargs: None


def _default_output_dir() -> Path:
    return Path(tempfile.mkdtemp(prefix="upsp-round-context-acceptance-"))


def _scenario_book_path(book_path: str | Path | None) -> Path:
    path = Path(book_path) if book_path else DEFAULT_BOOK_PATH
    return path


SCENARIO_REGISTRY = {
    "coalesced_calendar_book": {
        "kind": "round_context_acceptance",
        "provider_required": False,
        "covers_specs": ["Spec383", "Spec388", "Spec389", "Spec448"],
        "summary": "节律合轮中真实用户输入、编年史 C 轨材料和 assistant_text 收束投影可见性验收。",
    },
}


def available_scenarios() -> dict[str, dict[str, Any]]:
    """Return the formal acceptance scenarios exposed by this runner."""
    return {
        name: dict(payload)
        for name, payload in SCENARIO_REGISTRY.items()
    }


def run_acceptance(
        *,
        scenario: str = "coalesced_calendar_book",
        mode: str = "fake",
        output_dir: str | Path | None = None,
        book_path: str | Path | None = None,
        envelope_path: str | Path | None = None,
        strict: bool = False) -> dict[str, Any]:
    if scenario != "coalesced_calendar_book":
        raise ValueError(f"unknown_acceptance_scenario:{scenario}")
    if mode not in {"fake", "spark-envelope"}:
        raise ValueError(f"unsupported_acceptance_mode:{mode}")
    output_dir = Path(output_dir) if output_dir else _default_output_dir()
    output_dir.mkdir(parents=True, exist_ok=True)
    runtime_root = output_dir / "runtime"
    if runtime_root.exists():
        shutil.rmtree(runtime_root)
    book_path = _scenario_book_path(book_path)
    if not book_path.is_file():
        raise FileNotFoundError(str(book_path))

    spark_envelope = None
    spark_envelope_issues: list[dict[str, str]] = []
    if mode == "spark-envelope":
        if not envelope_path:
            raise ValueError("spark_envelope_path_required")
        spark_envelope = _read_json(Path(envelope_path))
        spark_envelope_issues = validate_spark_envelope(spark_envelope)
        if spark_envelope_issues:
            raise ValueError(f"invalid_spark_envelope:{spark_envelope_issues}")
        executor = ScriptedAcceptanceExecutor(
            book_path=book_path,
            envelope=spark_envelope,
        )
    else:
        executor = FakeAcceptanceExecutor(book_path=book_path)
    with _extra_file_read_root(book_path.parent):
        rt = _build_runtime(runtime_root, executor)
        rt.sm.set_flag("calendar_day_due", True)
        rt.sm.set_flag("user_message_waiting", True)
        state = rt.sm.load()
        flags = state.get("base", {}).get("heartbeat_flags", {})
        rt._run_one_round("rhythm", state, flags)

    final_state = rt.sm.load()
    round_closed_payload = _latest_round_closed_payload(runtime_root)
    checks = _build_checks(
        executor.calls,
        final_state,
        str(book_path),
        round_closed_payload=round_closed_payload,
    )
    failed_checks = [
        name for name, check in checks.items() if not check.get("passed")
    ]
    if strict and failed_checks:
        status = "failed"
    else:
        status = "ok"
    report = {
        "schema_version": "round_context_acceptance.v1",
        "status": status,
        "scenario": scenario,
        "mode": mode,
        "context_profile": "full",
        "created_at": datetime.now().isoformat(),
        "runtime_root": str(runtime_root),
        "live_persona": str(LIVE_PERSONA),
        "live_persona_touched": False,
        "provider_calls": executor.provider_calls,
        "book_path": str(book_path),
        "spark_envelope_path": str(envelope_path or ""),
        "spark_envelope_issues": spark_envelope_issues,
        "calls": executor.calls,
        "checks": checks,
        "diagnostics": {
            "heartbeat_flags_after": (
                final_state.get("base", {}).get("heartbeat_flags", {})
                if isinstance(final_state, dict)
                else {}
            ),
            "round_closed": round_closed_payload,
        },
        "summary": {"failed_checks": failed_checks},
        "spark_observation": None,
    }
    report_path = output_dir / "round_context_acceptance_report.json"
    report["report_path"] = str(report_path)
    _json_dump(report_path, report)
    return report


def _calls_by_channel(calls: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for call in calls:
        grouped.setdefault(str(call.get("channel") or ""), []).append(call)
    return grouped


def _latest_round_closed_payload(runtime_root: Path) -> dict[str, Any]:
    from data.round_audit_codec import read_round_audit_file

    round_dir = runtime_root / "context" / "round"
    latest_payload: dict[str, Any] = {}
    for path in sorted(round_dir.glob("round_*.jsonl")):
        try:
            events = read_round_audit_file(path)
        except (OSError, ValueError):
            continue
        for event in events:
            if event.get("event_type") == "round_closed":
                payload = event.get("payload")
                if isinstance(payload, dict):
                    latest_payload = dict(payload)
    return latest_payload


def _build_checks(
        calls: list[dict[str, Any]],
        final_state: dict[str, Any],
        book_path: str,
        *,
        round_closed_payload: dict[str, Any] | None = None) -> dict[str, dict[str, Any]]:
    grouped = _calls_by_channel(calls)
    setup_messages = [
        message
        for call in grouped.get("setup", [])
        for message in call.get("messages", [])
    ]
    reaction_messages = [
        message
        for call in grouped.get("reaction.loop", [])
        for message in call.get("messages", [])
    ]
    cleanup_messages = [
        message
        for call in grouped.get("cleanup", [])
        for message in call.get("messages", [])
    ]
    user_visible = (
        _contains(setup_messages, "请读取并内化")
        and _contains(reaction_messages, "请读取并内化")
        and _contains(cleanup_messages, "请读取并内化")
    )
    chronicle_visible = (
        _contains(reaction_messages, "【本轮资料】编年史写入材料")
        and _contains(reaction_messages, "来源轮次范围")
    )
    round_closed_payload = dict(round_closed_payload or {})
    natural_final_reply_visible = bool(
        str(round_closed_payload.get("final_response") or "").strip()
    )
    flags = (
        final_state.get("base", {}).get("heartbeat_flags", {})
        if isinstance(final_state, dict)
        else {}
    )
    cleanup_ok = flags.get("user_message_waiting") is False
    calendar_ok = flags.get("calendar_day_due") is False
    return {
        "user_input_visible": {
            "passed": bool(user_visible),
            "required": "真实用户输入必须进入 setup/reaction/cleanup 可见上下文。",
        },
        "chronicle_material_visible": {
            "passed": bool(chronicle_visible),
            "required": "目标 Reaction Frame 必须让模型看到真实编年史 C 轨材料。",
        },
        "natural_final_reply_projected": {
            "passed": bool(natural_final_reply_visible),
            "required": "Runtime finish 后必须投影自然语言最终回复文本。",
        },
        "cleanup_preserves_or_clears_correctly": {
            "passed": bool(cleanup_ok),
            "required": "fake 完整闭环后不得误留已消费用户输入 flag。",
        },
        "calendar_rhythm_settled": {
            "passed": bool(calendar_ok),
            "required": "日历节律写入回执必须清掉 calendar_day_due。",
        },
        "no_provider_call": {
            "passed": True,
            "required": "acceptance fake 模式不得调用真实 provider。",
        },
        "live_persona_isolated": {
            "passed": True,
            "required": "acceptance fake 模式不得读写 live persona。",
        },
    }


def export_acceptance_bundle(
        *,
        scenario: str = "coalesced_calendar_book",
        output_dir: str | Path | None = None,
        book_path: str | Path | None = None) -> dict[str, Any]:
    output_dir = Path(output_dir) if output_dir else _default_output_dir()
    report = run_acceptance(
        scenario=scenario,
        mode="fake",
        output_dir=output_dir,
        book_path=book_path,
    )
    bundle_dir = output_dir / "spark_bundle"
    bundle_dir.mkdir(parents=True, exist_ok=True)
    context_bundle = bundle_dir / "context_bundle.json"
    subagent_prompt = bundle_dir / "subagent_prompt.md"
    spark_prompt = bundle_dir / "spark_observation_prompt.md"
    bundle_payload = {
        "schema_version": "round_context_acceptance_bundle.v1",
        "scenario": scenario,
        "report_path": report["report_path"],
        "calls": report["calls"],
        "checks": report["checks"],
        "summary": report["summary"],
    }
    _json_dump(context_bundle, bundle_payload)
    prompt_text = _spark_prompt_text(context_bundle)
    subagent_prompt.write_text(prompt_text, encoding="utf-8", newline="\n")
    spark_prompt.write_text(prompt_text, encoding="utf-8", newline="\n")
    return {
        "report_path": report["report_path"],
        "bundle": {
            "context_bundle": str(context_bundle),
            "subagent_prompt": str(subagent_prompt),
            "spark_observation_prompt": str(spark_prompt),
        },
    }


def _spark_prompt_text(context_bundle: Path) -> str:
    return "\n".join([
        "# Spark 观察型整轮上下文验收",
        "",
        "你是 UPSP 本地验收观察子代理，不执行工具，不写文件，不调用 provider。",
        f"请读取这个 context bundle：`{context_bundle}`。",
        "",
        "只输出 JSON，schema_version 必须是 `spark_observation.v1`，字段包括：",
        "- `seen_user_input`: boolean",
        "- `seen_chronicle_material`: boolean",
        "- `seen_natural_final_reply`: boolean",
        "- `would_handle_user_task`: boolean",
        "- `trapped_or_confused`: boolean",
        "- `would_misroute_relay`: boolean",
        "- `would_clear_user_task_too_early`: boolean",
        "- `likely_next_action`: string",
        "- `risk_items`: string[]",
        "- `notes`: string",
        "",
        "重点判断模型是否能看到真实用户输入、编年史写入材料、"
        "自然语言最终回复投影，以及是否存在误跳中继或误清用户任务风险。",
    ])


def import_spark_report(
        report_path: str | Path,
        spark_report_path: str | Path) -> dict[str, Any]:
    report_path = Path(report_path)
    spark_report_path = Path(spark_report_path)
    report = _read_json(report_path)
    spark_report = _read_json(spark_report_path)
    issues = validate_spark_observation(spark_report)
    if issues:
        raise ValueError(f"invalid_spark_observation:{issues}")
    report["spark_observation"] = spark_report
    _json_dump(report_path, report)
    return report


def require_spark_observation(report_path: str | Path) -> dict[str, Any]:
    report_path = Path(report_path)
    report = _read_json(report_path)
    issues: list[dict[str, str]] = []
    if report.get("schema_version") != "round_context_acceptance.v1":
        issues.append({
            "code": "acceptance_report_invalid_schema",
            "message": "整轮验收报告 schema_version 必须是 round_context_acceptance.v1。",
        })
    failed_checks = list(
        (report.get("summary") or {}).get("failed_checks") or []
    )
    if failed_checks:
        issues.append({
            "code": "acceptance_failed_checks",
            "message": "整轮验收仍有失败检查：" + ", ".join(failed_checks),
        })
    spark_observation = report.get("spark_observation")
    if not spark_observation:
        issues.append({
            "code": "spark_observation_missing",
            "message": "整轮验收报告缺少 Spark 真实观察报告。",
        })
    else:
        issues.extend(validate_spark_observation(spark_observation))
    return {
        "ok": not issues,
        "report_path": str(report_path),
        "issues": issues,
        "spark_observation_present": bool(spark_observation),
    }


def cli_run(args) -> dict[str, Any]:
    report = run_acceptance(
        scenario=args.scenario,
        mode=args.mode,
        output_dir=args.output_dir,
        book_path=args.book_path,
        envelope_path=getattr(args, "envelope_path", None),
        strict=args.strict,
    )
    return {
        "report_path": report["report_path"],
        "summary": report["summary"],
        "checks": report["checks"],
        "context_profile": report.get("context_profile", "full"),
    }


def cli_export(args) -> dict[str, Any]:
    return export_acceptance_bundle(
        scenario=args.scenario,
        output_dir=args.output_dir,
        book_path=args.book_path,
    )


def cli_import_spark_report(args) -> dict[str, Any]:
    updated = import_spark_report(args.report_path, args.spark_report)
    return {
        "report_path": str(args.report_path),
        "spark_observation": updated.get("spark_observation"),
    }


def cli_require_spark(args) -> dict[str, Any]:
    return require_spark_observation(args.report_path)
