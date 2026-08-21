"""协议工具注册表与反应步提交校验。"""
import os
import re

from paths import DOCS_PROTOCOL_TOOLS


def _python_backend(handler, permission_scope, status="enabled"):
    return [{
        "id": "python",
        "backend_type": "python",
        "handler": handler,
        "permission_scope": permission_scope,
        "status": status,
    }]


# 与 docs/protocol/base/tools.md 的短索引同步；新增工具时必须同时补齐这里。
TOOL_DEFINITIONS = {
    "context_assemble": {
        "tool_family": "substrate_tool",
        "tool_class": "read_tool",
        "domain": "context",
        "risk": "high",
        "handler": "context_assembler",
        "result_kind": "step_messages",
    },
    "file_read": {
        "tool_family": "general_tool",
        "tool_class": "read_tool",
        "domain": "filesystem",
        "risk": "medium",
        "active_backend": "python",
        "backend_candidates": _python_backend(
            "file_read_handler",
            "workspace_read_allowlist",
        ),
        "backend_type": "python",
        "handler": "file_read_handler",
        "permission_scope": "workspace_read_allowlist",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "file_glob": {
        "tool_family": "general_tool",
        "tool_class": "read_tool",
        "domain": "filesystem",
        "risk": "medium",
        "active_backend": "python",
        "backend_candidates": _python_backend(
            "file_glob_handler",
            "workspace_read_allowlist",
        ),
        "backend_type": "python",
        "handler": "file_glob_handler",
        "permission_scope": "workspace_read_allowlist",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "file_grep": {
        "tool_family": "general_tool",
        "tool_class": "read_tool",
        "domain": "filesystem",
        "risk": "medium",
        "active_backend": "python",
        "backend_candidates": _python_backend(
            "file_grep_handler",
            "workspace_read_allowlist",
        ),
        "backend_type": "python",
        "handler": "file_grep_handler",
        "permission_scope": "workspace_read_allowlist",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "file_edit": {
        "tool_family": "general_tool",
        "tool_class": "focus_tool",
        "domain": "filesystem",
        "risk": "high",
        "active_backend": "python",
        "backend_candidates": _python_backend(
            "file_edit_handler",
            "workspace_patch_allowlist",
        ),
        "backend_type": "python",
        "handler": "file_edit_handler",
        "permission_scope": "workspace_patch_allowlist",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "file_write": {
        "tool_family": "general_tool",
        "tool_class": "focus_tool",
        "domain": "filesystem",
        "risk": "high",
        "active_backend": "python",
        "backend_candidates": _python_backend(
            "file_write_handler",
            "workspace_patch_allowlist",
        ),
        "backend_type": "python",
        "handler": "file_write_handler",
        "permission_scope": "workspace_patch_allowlist",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "web_fetch": {
        "tool_family": "general_tool",
        "tool_class": "read_tool",
        "domain": "web",
        "risk": "medium",
        "active_backend": "direct_fetch",
        "backend_candidates": [
            {
                "id": "direct_fetch",
                "backend_type": "python",
                "handler": "web_fetch_handler",
                "permission_scope": "public_web_read",
                "status": "enabled",
            },
            {
                "id": "jina_reader",
                "backend_type": "python",
                "handler": "web_fetch_handler",
                "permission_scope": "public_web_read",
                "status": "enabled",
            },
        ],
        "backend_type": "python",
        "handler": "web_fetch_handler",
        "permission_scope": "public_web_read",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "web_search": {
        "tool_family": "general_tool",
        "tool_class": "read_tool",
        "domain": "web",
        "risk": "medium",
        "active_backend": "ddgs",
        "backend_candidates": [
            {
                "id": "ddgs",
                "backend_type": "python",
                "handler": "web_search_handler",
                "permission_scope": "public_web_read",
                "status": "enabled",
            },
            {
                "id": "html_fallback",
                "backend_type": "python",
                "handler": "web_search_handler",
                "permission_scope": "public_web_read",
                "status": "enabled",
            },
        ],
        "backend_type": "python",
        "handler": "web_search_handler",
        "permission_scope": "public_web_read",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "shell_command": {
        "tool_family": "general_tool",
        "tool_class": "focus_tool",
        "domain": "shell",
        "risk": "high",
        "active_backend": "python",
        "backend_candidates": _python_backend(
            "shell_command_handler",
            "workspace_shell_allowlist",
        ),
        "backend_type": "python",
        "handler": "shell_command_handler",
        "permission_scope": "workspace_shell_allowlist",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "subagent_dispatch": {
        "tool_family": "general_tool",
        "tool_class": "focus_tool",
        "domain": "agent",
        "risk": "high",
        "active_backend": "python",
        "backend_candidates": _python_backend(
            "subagent_dispatch_handler",
            "subagent_task_scope",
        ),
        "backend_type": "python",
        "handler": "subagent_dispatch_handler",
        "permission_scope": "subagent_task_scope",
        "result_kind": "general_tool_result",
        "status": "enabled",
    },
    "memory_write": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "memory",
        "risk": "high",
        "handler": "memory_write_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "relation_card_write": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "relation",
        "risk": "high",
        "handler": "relation_card_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "memory_content_read": {
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "domain": "memory",
        "risk": "medium",
        "handler": "memory_content_read_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "mount_cancel": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "context_mount",
        "risk": "medium",
        "handler": "mount_cancel_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "relay_intent_settle": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "relay",
        "risk": "medium",
        "handler": "relay_intent_pool",
        "result_kind": "protocol_tool_receipt",
    },
    "memory_link_update": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "memory",
        "risk": "high",
        "handler": "memory_link_update_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "memory_container_create": {
        "tool_family": "protocol_tool",
        "tool_class": "focus_tool",
        "domain": "memory",
        "risk": "high",
        "handler": "memory_container_create_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "memory_container_write": {
        "tool_family": "protocol_tool",
        "tool_class": "focus_tool",
        "domain": "memory",
        "risk": "high",
        "handler": "memory_container_write_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "memory_privacy_mark": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "memory",
        "risk": "high",
        "status": "disabled",
        "handler": "memory_privacy_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "memory_privacy_declassify": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "memory",
        "risk": "high",
        "status": "disabled",
        "handler": "memory_privacy_declassify_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "chronicle_write": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "chronicle",
        "risk": "medium",
        "handler": "chronicle_write_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "alert_mode_settle": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "immune",
        "risk": "medium",
        "handler": "alert_mode_settle_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "fault_record": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "immune",
        "risk": "medium",
        "handler": "fault_record_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "container_focus": {
        "tool_family": "protocol_tool",
        "tool_class": "focus_tool",
        "domain": "workbench",
        "risk": "high",
        "handler": "workbench_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "container_read": {
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "domain": "workbench",
        "risk": "medium",
        "handler": "container_read_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "corpus_read": {
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "domain": "context",
        "risk": "low",
        "handler": "corpus_read_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "index_view": {
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "domain": "context",
        "risk": "low",
        "handler": "index_view_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "memory_search": {
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "domain": "memory",
        "risk": "low",
        "handler": "memory_search_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "relation_read": {
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "domain": "relation",
        "risk": "low",
        "handler": "relation_read_processor",
        "result_kind": "protocol_tool_receipt",
    },
    "setup_mount_apply": {
        "tool_family": "substrate_tool",
        "tool_class": "read_tool",
        "domain": "context",
        "risk": "high",
        "handler": "context_assembler",
        "result_kind": "setup_mount_package",
    },
    "setup_security_gate": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "security",
        "risk": "high",
        "handler": "setup_runner",
        "result_kind": "control_flow",
    },
    "setup_handoff": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "setup",
        "risk": "medium",
        "handler": "context_assembler",
        "result_kind": "now_handoff",
    },
    "standby_setup_handoff": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "setup",
        "risk": "medium",
        "handler": "context_assembler",
        "result_kind": "control_flow",
    },
    "setup_finalize": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "runtime",
        "risk": "high",
        "handler": "step_terminal_finalize",
        "result_kind": "protocol_tool_receipt",
        "status": "enabled",
        "native_only": True,
        "step_terminal": "setup",
    },
    "reaction_loop": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "reaction",
        "risk": "high",
        "handler": "runtime_loop_guard",
        "result_kind": "now_handoff",
    },
    "reaction_finalize": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "runtime",
        "risk": "high",
        "handler": "step_terminal_finalize",
        "result_kind": "protocol_tool_receipt",
        "status": "enabled",
        "native_only": True,
        "step_terminal": "reaction",
    },
    "guide_submit": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "workbench",
        "risk": "high",
        "handler": "guide_submit_processor",
        "result_kind": "protocol_tool_receipt",
        "status": "enabled",
        "native_only": True,
    },
    "tool_transaction_audit": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "audit",
        "risk": "high",
        "handler": "tool_transaction_audit",
        "result_kind": "round_snapshot_runtime",
    },
    "cleanup_handoff": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "cleanup",
        "risk": "medium",
        "handler": "cleanup_finalizer",
        "result_kind": "now_handoff",
    },
    "cleanup_finalize": {
        "tool_family": "protocol_tool",
        "tool_class": "sync_tool",
        "domain": "runtime",
        "risk": "high",
        "handler": "step_terminal_finalize",
        "result_kind": "protocol_tool_receipt",
        "status": "enabled",
        "native_only": True,
        "step_terminal": "cleanup",
    },
    "heartbeat_tick": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "heartbeat",
        "risk": "high",
        "handler": "heartbeat_manager",
        "result_kind": "heartbeat_flags",
    },
    "connection_material_settle": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "training",
        "risk": "high",
        "handler": "cleanup_processor",
        "result_kind": "raw_connection",
    },
    "tacit_material_settle": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "training",
        "risk": "high",
        "handler": "cleanup_processor",
        "result_kind": "raw_tacit",
    },
    "association_count_update": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "training",
        "risk": "high",
        "handler": "training_material_store",
        "result_kind": "raw_association_counts",
    },
    "heartbeat_restart": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "heartbeat",
        "risk": "high",
        "handler": "runtime_finalizer",
        "result_kind": "runtime_state",
    },
    "registry_reload": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "registry",
        "risk": "medium",
        "handler": "registry_loader",
        "result_kind": "runtime_state",
    },
    "migration_guard": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "migration",
        "risk": "high",
        "handler": "migration_guard",
        "result_kind": "migration_report",
    },
    "state_settle": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "state",
        "risk": "high",
        "handler": "state_settlement",
        "result_kind": "runtime_state",
    },
    "state_coordinate": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "state",
        "risk": "high",
        "handler": "runtime_state_coordinator",
        "result_kind": "runtime_state",
    },
    "state_reconcile": {
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "domain": "state",
        "risk": "high",
        "handler": "state_store",
        "result_kind": "runtime_state",
    },
}

SUBMISSION_ALIASES = {
    "memory_write_declaration": "memory_write",
    "relation_card_declaration": "relation_card_write",
    "memory_link_update_declaration": "memory_link_update",
    "memory_container_create_declaration": "memory_container_create",
    "memory_container_write_declaration": "memory_container_write",
    "memory_privacy_declaration": "memory_privacy_mark",
    "memory_privacy_declassify_declaration": "memory_privacy_declassify",
    "fault_record_table": "fault_record",
}

TOOL_IDS = set(TOOL_DEFINITIONS)
PROTOCOL_TOOL_IDS = {
    tool_id
    for tool_id, meta in TOOL_DEFINITIONS.items()
    if meta.get("tool_family") == "protocol_tool"
}
GENERAL_TOOL_IDS = {
    tool_id
    for tool_id, meta in TOOL_DEFINITIONS.items()
    if meta.get("tool_family") == "general_tool"
}


def tool_class_for(tool_id):
    """返回协议工具分类。未知工具返回空字符串。"""
    normalized = normalize_tool_id(tool_id)
    return TOOL_DEFINITIONS.get(normalized, {}).get("tool_class", "")

def _metadata_with_backend_projection(metadata):
    data = dict(metadata or {})
    if data.get("tool_family") != "general_tool":
        return data

    active_backend = data.get("active_backend") or data.get("backend_type", "")
    candidates = [dict(item) for item in data.get("backend_candidates", [])]
    if not candidates and active_backend:
        candidates = [{
            "id": active_backend,
            "backend_type": data.get("backend_type", active_backend),
            "handler": data.get("handler", ""),
            "permission_scope": data.get("permission_scope", ""),
            "status": data.get("status", "enabled"),
        }]

    data["active_backend"] = active_backend
    data["backend_candidates"] = candidates
    return data


def tool_metadata_for(tool_id):
    """返回协议工具元数据副本。未知工具返回空 dict。"""
    normalized = normalize_tool_id(tool_id)
    metadata = TOOL_DEFINITIONS.get(normalized)
    return _metadata_with_backend_projection(metadata) if metadata else {}


def general_tool_backend_for(tool_id):
    """返回通用工具当前 active backend 元数据；非通用工具或缺后端返回空 dict。"""
    metadata = tool_metadata_for(tool_id)
    if metadata.get("tool_family") != "general_tool":
        return {}
    active_backend = metadata.get("active_backend") or metadata.get("backend_type", "")
    for candidate in metadata.get("backend_candidates") or []:
        if candidate.get("id") == active_backend or candidate.get("backend_type") == active_backend:
            return dict(candidate)
    if metadata.get("backend_type"):
        return {
            "id": active_backend,
            "backend_type": metadata.get("backend_type", ""),
            "handler": metadata.get("handler", ""),
            "permission_scope": metadata.get("permission_scope", ""),
            "status": metadata.get("status", "enabled"),
        }
    return {}


def is_focus_tool(tool_id):
    return tool_class_for(tool_id) == "focus_tool"


def is_sync_tool(tool_id):
    return tool_class_for(tool_id) == "sync_tool"


def is_read_tool(tool_id):
    return tool_class_for(tool_id) == "read_tool"


def _read_tools_doc():
    if not os.path.isfile(DOCS_PROTOCOL_TOOLS):
        return ""
    try:
        with open(DOCS_PROTOCOL_TOOLS, "r", encoding="utf-8") as f:
            return f.read()
    except (OSError, UnicodeError):
        return ""


def _extract_between(content, start_marker, end_marker):
    start = content.find(start_marker)
    if start == -1:
        return ""
    end = content.find(end_marker, start + len(start_marker))
    if end == -1:
        return ""
    return content[start + len(start_marker):end].strip()


def load_protocol_tool_index():
    """读取常驻短索引，供永固层/高频层注入。"""
    content = _read_tools_doc()
    return _extract_between(
        content,
        "<!-- PROTOCOL_TOOL_INDEX_START -->",
        "<!-- PROTOCOL_TOOL_INDEX_END -->",
    )


def load_general_tool_index():
    """Read the general tool short index for reaction context injection."""
    content = _read_tools_doc()
    return _extract_between(
        content,
        "<!-- GENERAL_TOOL_INDEX_START -->",
        "<!-- GENERAL_TOOL_INDEX_END -->",
    )


def normalize_tool_id(value):
    """把提交表别名归一到协议工具 ID。未知值返回清理后的原值。"""
    text = str(value or "").strip().strip("`")
    if not text:
        return ""
    return SUBMISSION_ALIASES.get(text, text)
