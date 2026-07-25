"""General tool dispatch helpers for reaction-loop external actions."""
import hashlib
import json
import re

from logic.execution_capability import check_general_tool_request
from logic.execution_permission import load_execution_permission_level
from logic.file_read_window import (
    FILE_READ_BATCH_BUDGET_EXHAUSTED,
    RUNTIME_CONTEXT_KEY,
    FileReadBatchBudget,
)
from logic.general_tools import (
    execute_general_tool_call,
    format_general_tool_fact,
    format_general_tool_material_entry,
    format_general_tool_result,
    web_backend_ids_for_tool,
)
from logic.sandbox_grant import (
    load_sandbox_grant,
    normalize_sandbox_tool_path_alias,
    sandbox_roots_for_tool,
)
from logic.protocol_tools import (
    general_tool_backend_for,
    normalize_tool_id,
    tool_metadata_for,
)

DUPLICATE_TOOL_RESULT_SATISFIED = "duplicate_tool_result_satisfied"
DUPLICATE_TOOL_FAILURE_REPEATED = "duplicate_tool_failure_repeated"
WEB_BACKEND_EXHAUSTED_DUPLICATE = "web_backend_exhausted_duplicate"
DUPLICATE_GENERAL_TOOL_REASONS = {
    DUPLICATE_TOOL_RESULT_SATISFIED,
    DUPLICATE_TOOL_FAILURE_REPEATED,
    WEB_BACKEND_EXHAUSTED_DUPLICATE,
}

SUCCESS_STATUSES = {"ok", "success", "accepted", "applied", "guide_loaded"}

SIGNATURE_IGNORED_FIELDS = {
    "call_id",
    "provider",
    "response_id",
    "provider_item_id",
    "index",
    "purpose",
    "reason",
    "risk",
    "risk_level",
    "source",
    "tool_family",
    "tool_class",
    "active_backend",
    "backend_type",
    "handler",
    "permission_scope",
    "result_kind",
    "protocol_tool_receipt",
    "arguments_json",
}

SIGNATURE_FIELDS_BY_TOOL = {
    "file_read": ("path", "line_start", "encoding"),
    "file_search": ("root", "pattern", "recursive", "max_results"),
    "file_edit": ("path", "patch"),
    "file_write": ("path", "content"),
    "web_fetch": ("url", "char_start"),
    "web_search": ("query", "max_results"),
    "shell_command": ("cwd", "command"),
    "subagent_dispatch": (
        "task_goal",
        "task_mode",
        "allowed_paths",
        "write_scope",
        "expected_artifacts",
        "validation_commands",
    ),
}


class GeneralToolDispatcher:
    def __init__(self, load_guide_fn=None, execute_fn=None):
        self.load_guide_fn = load_guide_fn
        self.execute_fn = execute_fn or execute_general_tool_call

    @staticmethod
    def _base_result(tool_id, status, source="general_tool_request", reason=""):
        meta = tool_metadata_for(tool_id)
        result = {
            "tool_id": tool_id,
            "tool_family": meta.get("tool_family", ""),
            "tool_class": meta.get("tool_class", ""),
            "status": status,
            "source": source,
            "active_backend": meta.get("active_backend", ""),
            "backend_type": meta.get("backend_type", ""),
            "handler": meta.get("handler", ""),
            "permission_scope": meta.get("permission_scope", ""),
            "result_kind": "general_tool_result",
            "protocol_tool_receipt": False,
        }
        if reason:
            result["reason"] = reason
        return result

    @staticmethod
    def _trace_fields(request):
        return {
            key: request.get(key)
            for key in (
                "call_id",
                "provider",
                "response_id",
                "provider_item_id",
                "index",
            )
            if isinstance(request, dict) and request.get(key) not in (None, "")
        }

    @classmethod
    def _with_trace(cls, result, request):
        traced = dict(result or {})
        for key, value in cls._trace_fields(request).items():
            traced.setdefault(key, value)
        return traced

    def _execute_call(self, call, sandbox_grant=None):
        if sandbox_grant:
            roots = sandbox_roots_for_tool(sandbox_grant, call.get("tool_id"))
            try:
                return self.execute_fn(call, allowed_roots=roots)
            except TypeError:
                return self.execute_fn(call)
        return self.execute_fn(call)

    @staticmethod
    def _backend_readiness_issue(tool_id):
        backend = general_tool_backend_for(tool_id)
        if not backend or not backend.get("backend_type"):
            return "backend_missing"
        if not backend.get("handler") or backend.get("handler") == "reserved":
            return "handler_missing"
        if not backend.get("permission_scope"):
            return "permission_scope_missing"
        return ""

    @staticmethod
    def _canonical_scalar(value):
        text = str(value or "").strip()
        return text.replace("\\", "/")

    @classmethod
    def _canonical_value(cls, value):
        if isinstance(value, dict):
            return {
                str(key): cls._canonical_value(value[key])
                for key in sorted(value)
                if value[key] not in (None, "", [])
            }
        if isinstance(value, (list, tuple)):
            return [cls._canonical_value(item) for item in value]
        if isinstance(value, set):
            return sorted(cls._canonical_value(item) for item in value)
        if isinstance(value, str):
            return cls._canonical_scalar(value)
        return value

    @staticmethod
    def _canonical_shell_command(command):
        text = str(command or "").strip()
        text = re.sub(r"\s+", " ", text)
        text = re.sub(r"\s+2\s*>\s*&\s*1\s*$", "", text, flags=re.IGNORECASE)
        return text

    @classmethod
    def _signature_payload(cls, tool_id, request):
        fields = SIGNATURE_FIELDS_BY_TOOL.get(tool_id)
        if fields:
            values = {}
            for key in fields:
                value = request.get(key)
                if value in (None, "", []):
                    continue
                if tool_id == "shell_command" and key == "command":
                    values[key] = cls._canonical_shell_command(value)
                else:
                    values[key] = cls._canonical_value(value)
            return {"tool_id": tool_id, "arguments": values}
        values = {}
        for key, value in sorted((request or {}).items()):
            if key in SIGNATURE_IGNORED_FIELDS or value in (None, "", []):
                continue
            values[key] = cls._canonical_value(value)
        return {"tool_id": tool_id, "arguments": values}

    @classmethod
    def _request_signature(cls, tool_id, request):
        payload = cls._signature_payload(tool_id, request)
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @classmethod
    def _guard_signature_payload(cls, tool_id, request, result=None):
        result = result or {}
        status = str(result.get("status") or "").strip()
        reason = str(result.get("reason") or "").strip()
        if (
                tool_id == "file_edit"
                and status
                and status not in SUCCESS_STATUSES
                and reason):
            path = cls._canonical_value(
                (request or {}).get("path") or result.get("path")
            )
            if path:
                return {
                    "tool_id": tool_id,
                    "guard": "failure_by_path_reason",
                    "arguments": {
                        "path": path,
                        "failure_reason": cls._canonical_value(reason),
                    },
                }
        return cls._signature_payload(tool_id, request or {})

    @classmethod
    def _guard_signature(cls, tool_id, request, result=None):
        payload = cls._guard_signature_payload(tool_id, request, result=result)
        encoded = json.dumps(
            payload,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        )
        return hashlib.sha256(encoded.encode("utf-8")).hexdigest()

    @classmethod
    def _decorate_signatures(cls, result, tool_id, request, signature):
        decorated = dict(result or {})
        decorated.setdefault("tool_signature", signature)
        decorated.setdefault(
            "tool_signature_payload",
            cls._signature_payload(tool_id, request or {}),
        )
        guard_payload = cls._guard_signature_payload(
            tool_id, request or {}, result=decorated
        )
        decorated["duplicate_guard_key"] = cls._guard_signature(
            tool_id, request or {}, result=decorated
        )
        decorated["duplicate_guard_payload"] = guard_payload
        return decorated

    @staticmethod
    def _find_prior_duplicate(signature, prior_results):
        for item in prior_results or []:
            if not isinstance(item, dict):
                continue
            if item.get("tool_signature") == signature:
                return item
        return None

    @staticmethod
    def _find_prior_duplicates(signature, prior_results):
        return [
            item for item in (prior_results or [])
            if isinstance(item, dict) and item.get("tool_signature") == signature
        ]

    @staticmethod
    def _web_attempted_backend_ids(prior_items):
        attempted = set()
        for item in prior_items or []:
            for attempt in item.get("backend_attempts") or []:
                if isinstance(attempt, dict):
                    backend_id = str(attempt.get("backend_id") or "").strip()
                    if backend_id:
                        attempted.add(backend_id)
        return attempted

    @classmethod
    def _web_duplicate_retry_skip_ids(cls, tool_id, signature, prior_results):
        backend_ids = tuple(web_backend_ids_for_tool(tool_id))
        if not backend_ids:
            return []
        prior_items = cls._find_prior_duplicates(signature, prior_results)
        if not prior_items:
            return []
        if any(str(item.get("status") or "").strip() in SUCCESS_STATUSES for item in prior_items):
            return []
        attempted = cls._web_attempted_backend_ids(prior_items)
        if not attempted:
            return []
        untried = [backend_id for backend_id in backend_ids if backend_id not in attempted]
        return sorted(attempted) if untried else []

    @classmethod
    def _duplicate_result(cls, tool_id, prior, signature, request=None):
        previous_status = str((prior or {}).get("status") or "").strip()
        previous_reason = str((prior or {}).get("reason") or "").strip()
        if previous_status in SUCCESS_STATUSES:
            reason = DUPLICATE_TOOL_RESULT_SATISFIED
        elif (
                tool_id in {"web_fetch", "web_search"}
                and previous_reason == "web_backend_exhausted"):
            reason = WEB_BACKEND_EXHAUSTED_DUPLICATE
        else:
            reason = DUPLICATE_TOOL_FAILURE_REPEATED
        result = cls._base_result(tool_id, "rejected", reason=reason)
        result["tool_signature"] = signature
        result["tool_signature_payload"] = cls._signature_payload(
            tool_id, request or {}
        )
        result["duplicate_of_call_id"] = (prior or {}).get("call_id", "")
        result["previous_status"] = previous_status
        if previous_reason:
            result["previous_reason"] = previous_reason
        if (prior or {}).get("duplicate_guard_key"):
            result["duplicate_guard_key"] = prior.get("duplicate_guard_key")
            result["duplicate_guard_payload"] = prior.get("duplicate_guard_payload") or {}
        return result

    @staticmethod
    def _with_path_alias_info(result, alias_info):
        if not alias_info:
            return result
        enriched = dict(result or {})
        for key, value in alias_info.items():
            if value not in (None, ""):
                enriched.setdefault(key, value)
        return enriched

    @staticmethod
    def _request_with_runtime_context(request, tool_id, runtime_context):
        call = dict(request)
        call["tool_id"] = tool_id
        if tool_id == "file_read" and isinstance(runtime_context, dict):
            call[RUNTIME_CONTEXT_KEY] = dict(runtime_context)
        return call

    def _execute_allowed_request(
        self,
        request,
        tool_id,
        signature,
        sandbox_grant,
        path_alias_info,
        file_read_batch,
    ):
        if file_read_batch.exhausted_for(tool_id):
            result = self._base_result(
                tool_id,
                "rejected",
                reason=file_read_batch.exhaustion_reason,
            )
            result.update(file_read_batch.rejection_details())
        else:
            call = self._request_with_runtime_context(
                request,
                tool_id,
                file_read_batch.context_for(tool_id),
            )
            result = self._execute_call(call, sandbox_grant=sandbox_grant)
        result = self._with_trace(result, request)
        result = self._with_path_alias_info(result, path_alias_info)
        result = self._decorate_signatures(result, tool_id, request, signature)
        return file_read_batch.observe(result)

    def handle_requests(self, requests, active_guides, prior_results=None, runtime_context=None):
        results = []
        known_results = list(prior_results or [])
        file_read_batch = FileReadBatchBudget(runtime_context)
        sandbox_grant = load_sandbox_grant()
        execution_permission_level = load_execution_permission_level()
        for request in requests or []:
            if isinstance(request, dict):
                raw_tool_id = request.get("tool_id", "")
            else:
                raw_tool_id = request
                request = {"tool_id": raw_tool_id}
            tool_id = normalize_tool_id(raw_tool_id)
            meta = tool_metadata_for(tool_id)
            if not meta or meta.get("tool_family") != "general_tool":
                results.append(self._with_trace(
                    self._base_result(
                        tool_id,
                        "rejected",
                        reason="unknown_general_tool",
                    ),
                    request,
                ))
                continue
            if meta.get("status") != "enabled":
                results.append(self._with_trace(
                    self._base_result(
                        tool_id,
                        "rejected",
                        reason="general_tool_not_enabled",
                    ),
                    request,
                ))
                continue
            backend_issue = self._backend_readiness_issue(tool_id)
            if backend_issue:
                results.append(self._with_trace(
                    self._base_result(
                        tool_id,
                        "rejected",
                        reason=backend_issue,
                    ),
                    request,
                ))
                continue
            request, path_alias_info = normalize_sandbox_tool_path_alias(
                sandbox_grant,
                tool_id,
                request,
            )
            signature = self._request_signature(tool_id, request)
            duplicate = self._find_prior_duplicate(signature, known_results)
            if duplicate:
                web_skip_backend_ids = self._web_duplicate_retry_skip_ids(
                    tool_id,
                    signature,
                    known_results,
                )
                if web_skip_backend_ids:
                    request = dict(request)
                    request["_web_skip_backend_ids"] = web_skip_backend_ids
                else:
                    duplicate_for_result = duplicate
                    if web_backend_ids_for_tool(tool_id):
                        prior_duplicates = self._find_prior_duplicates(
                            signature,
                            known_results,
                        )
                        duplicate_for_result = (
                            prior_duplicates[-1] if prior_duplicates else duplicate
                        )
                    result = self._with_trace(
                        self._duplicate_result(
                            tool_id,
                            duplicate_for_result,
                            signature,
                            request=request,
                        ),
                        request,
                    )
                    result = self._with_path_alias_info(result, path_alias_info)
                    results.append(result)
                    known_results.append(result)
                    continue
            decision = check_general_tool_request(
                request,
                phase="reaction",
                active_guides=active_guides,
                sandbox_grant=sandbox_grant,
                execution_permission_level=execution_permission_level,
            )
            if not decision.get("allowed"):
                result = self._base_result(
                    tool_id,
                    "rejected",
                    reason=decision.get("reason") or "capability_denied",
                )
                result["capability_gate"] = decision
                for key, value in (decision.get("details") or {}).items():
                    if key not in result and value not in (None, ""):
                        result[key] = value
                result["tool_signature"] = signature
                result = self._decorate_signatures(
                    self._with_trace(result, request),
                    tool_id,
                    request,
                    signature,
                )
                result = self._with_path_alias_info(result, path_alias_info)
                results.append(result)
                known_results.append(results[-1])
                continue
            result = self._execute_allowed_request(
                request,
                tool_id,
                signature,
                sandbox_grant,
                path_alias_info,
                file_read_batch,
            )
            results.append(result)
            known_results.append(result)
        return results

    @staticmethod
    def format_result(result):
        return format_general_tool_result(result)

    @staticmethod
    def format_fact(result):
        return format_general_tool_fact(result)

    @staticmethod
    def format_material_entry(result):
        return format_general_tool_material_entry(result)
