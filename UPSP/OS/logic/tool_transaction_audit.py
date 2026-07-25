"""工具事务验账纯逻辑。"""

from logic.protocol_tools import (
    normalize_tool_id,
    tool_metadata_for,
)


TERMINAL_PROCESSOR_STATUSES = {
    "accepted",
    "applied",
    "degraded",
    "rejected",
    "needs_review",
    "rejected_state_or_axis_write",
    "processor_error",
    "error",
    "multiple_relation_card_declarations",
    "multiple_relation_targets",
    "relation_body_not_visible",
    "relation_card_exists",
    "not_found",
}

def _issue(code, tool_id, source="", severity="warning", detail=""):
    return {
        "code": code,
        "tool_id": tool_id,
        "source": source,
        "severity": severity,
        "detail": detail,
    }


def _receipt_tool_id(receipt):
    return normalize_tool_id((receipt or {}).get("tool_id", ""))


def _requested_tool_id(request):
    if isinstance(request, dict):
        return normalize_tool_id(request.get("tool_id", ""))
    return normalize_tool_id(request)


def _invalid_request_key(item):
    if isinstance(item, dict):
        call_id = str(
            item.get("call_id")
            or item.get("provider_item_id")
            or ""
        ).strip()
        if call_id:
            return ("call", call_id)
        return (
            "shape",
            normalize_tool_id(item.get("tool_id", "")),
            str(item.get("source") or "tool_request").strip(),
            str(item.get("reason") or item.get("detail") or "").strip(),
        )
    return ("shape", normalize_tool_id(item), "tool_request", "")


def _status_by_tool(receipts):
    by_tool = {}
    for receipt in receipts or []:
        tool_id = _receipt_tool_id(receipt)
        if not tool_id:
            continue
        by_tool.setdefault(tool_id, []).append(receipt)
    return by_tool


def audit_tool_transactions(
        requests=None,
        submissions=None,
        receipts=None,
        active_guides=None,
        invalid_submissions=None,
        invalid_requests=None,
        corrected_invalid_requests=None):
    """校验协议工具 native submission / processor / receipt 是否闭合。

    本函数只做事后审计，不读写文件，不改变任何工具执行结果。
    """
    requests = requests or []
    submissions = submissions or []
    receipts = receipts or []
    active_guides = {normalize_tool_id(item) for item in (active_guides or [])}
    invalid_submissions = list(invalid_submissions or [])
    invalid_requests = list(invalid_requests or [])
    corrected_invalid_requests = list(corrected_invalid_requests or [])
    corrected_invalid_keys = {
        _invalid_request_key(item)
        for item in corrected_invalid_requests
    }
    issues = []
    by_tool = _status_by_tool(receipts)

    for item in invalid_requests:
        if isinstance(item, dict):
            tool_id = normalize_tool_id(item.get("tool_id", ""))
            detail = str(item.get("reason") or "").strip()
            source = str(item.get("source") or "tool_request").strip()
        else:
            tool_id = normalize_tool_id(item)
            detail = ""
            source = "tool_request"
        if not tool_id:
            continue
        if _invalid_request_key(item) in corrected_invalid_keys:
            continue
        issues.append(_issue(
            "invalid_request_rejected",
            tool_id,
            source=source,
            detail=detail,
        ))

    invalid_seen = set()
    for item in invalid_submissions:
        source = str(item or "").strip()
        if not source or source in invalid_seen:
            continue
        invalid_seen.add(source)
        issues.append(_issue(
            "invalid_submission_rejected",
            normalize_tool_id(source),
            source=source,
        ))

    for receipt in receipts:
        tool_id = _receipt_tool_id(receipt)
        if not tool_id:
            continue
        status = receipt.get("status", "")
        source = receipt.get("source", "")
        meta = tool_metadata_for(tool_id)
        expected_family = meta.get("tool_family", "")
        expected_class = meta.get("tool_class", "")
        receipt_family = receipt.get("tool_family", "")
        receipt_class = receipt.get("tool_class", "")
        if (
            (receipt_family and expected_family and receipt_family != expected_family)
            or (receipt_class and expected_class and receipt_class != expected_class)
        ):
            issues.append(_issue(
                "metadata_mismatch",
                tool_id,
                source=source,
                severity="error",
                detail=(
                    f"expected={expected_family}/{expected_class}; "
                    f"actual={receipt_family}/{receipt_class}"
                ),
            ))
        if status == "rejected_missing_guide" and source not in invalid_seen:
            invalid_seen.add(source)
            issues.append(_issue(
                "invalid_submission_rejected",
                tool_id,
                source=source,
            ))
        if (
            status not in {"rejected_missing_guide", "guide_missing"}
            and expected_family
            and expected_family != "protocol_tool"
        ):
            issues.append(_issue(
                "non_protocol_protocol_receipt",
                tool_id,
                source=source,
                severity="error",
            ))

    for request in requests:
        tool_id = _requested_tool_id(request)
        if not tool_id:
            continue
        meta = tool_metadata_for(tool_id)
        if meta and meta.get("tool_family") != "protocol_tool":
            issues.append(_issue(
                "non_protocol_request_ignored",
                tool_id,
                source="protocol_tool_request",
            ))
            continue
        if not meta:
            issues.append(_issue(
                "unknown_request_ignored",
                tool_id,
                source="protocol_tool_request",
            ))
            continue
        request_receipts = by_tool.get(tool_id, [])
        if not any(receipt.get("status") for receipt in request_receipts):
            issues.append(_issue(
                "missing_request_receipt",
                tool_id,
                source="protocol_tool_request",
                severity="error",
            ))

    for submission in submissions:
        source = str(submission or "").strip()
        tool_id = normalize_tool_id(source)
        if not tool_id:
            continue
        meta = tool_metadata_for(tool_id)
        if meta.get("tool_family") != "protocol_tool":
            issues.append(_issue(
                "non_protocol_submission_accepted",
                tool_id,
                source=source,
                severity="error",
            ))
            continue
        if meta.get("tool_class") == "read_tool":
            issues.append(_issue(
                "read_tool_submission_accepted",
                tool_id,
                source=source,
                severity="error",
            ))
        tool_receipts = by_tool.get(tool_id, [])
        if not any(
            receipt.get("status") == "submission_received"
            for receipt in tool_receipts
        ):
            issues.append(_issue(
                "missing_submission_receipt",
                tool_id,
                source=source,
                severity="error",
            ))
        if not any(
            receipt.get("status") in TERMINAL_PROCESSOR_STATUSES
            for receipt in tool_receipts
        ):
            issues.append(_issue(
                "missing_processor_receipt",
                tool_id,
                source=source,
                severity="error",
            ))

    return {
        "tool_id": "tool_transaction_audit",
        "tool_family": "substrate_tool",
        "tool_class": "sync_tool",
        "status": "ok" if not issues else "issues_found",
        "counts": {
            "requests": len(requests),
            "submissions": len(submissions),
            "invalid_requests": len(invalid_requests),
            "corrected_invalid_requests": len(corrected_invalid_requests),
            "invalid_submissions": len(invalid_submissions),
            "receipts": len(receipts),
            "active_guides": len(active_guides),
            "issues": len(issues),
        },
        "issues": issues,
        "corrected_invalid_requests": corrected_invalid_requests,
    }
