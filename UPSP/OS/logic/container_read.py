"""container_read 协议只读工具处理器。"""

from data.container_store import ContainerStore
from utils.content_ranges import range_kwargs_from_request


def _clean(value):
    return str(value or "").strip().strip("`")


def _base_receipt(request):
    return {
        "tool_id": "container_read",
        "tool_family": "protocol_tool",
        "tool_class": "read_tool",
        "status": "",
        "source": "protocol_tool_request",
        "container_id": _clean(request.get("container_id")) if isinstance(request, dict) else "",
        "target_file": _clean(request.get("target_file")) if isinstance(request, dict) else "",
        "content": "",
        "read_mode": "",
        "range_requested": None,
        "range_applied": None,
        "total_lines": 0,
        "total_chars": 0,
        "protocol_tool_receipt": True,
    }


def _reject(receipt, reason):
    receipt["status"] = "rejected"
    receipt["reason"] = reason
    return receipt

def apply_container_read_requests(requests, modules=None):
    modules = modules or {}
    container_store = modules.get("container_store") or ContainerStore()
    receipts = []
    mounts = []
    for request in requests or []:
        if not isinstance(request, dict):
            receipts.append(_reject(_base_receipt({}), "invalid_request"))
            continue
        receipt = _base_receipt(request)
        container_id = receipt["container_id"]
        if not container_id:
            receipts.append(_reject(receipt, "missing_container_id"))
            continue
        try:
            result = container_store.read_container_content(
                container_id,
                target_file=receipt["target_file"] or None,
                **range_kwargs_from_request(request),
            )
        except ValueError as exc:
            receipts.append(_reject(receipt, str(exc)))
            continue
        except Exception:
            receipts.append(_reject(receipt, "container_not_found"))
            continue
        receipt.update({
            "status": "accepted",
            "container_id": result.get("container_id", container_id),
            "container_type": result.get("container_type", ""),
            "target_file": result.get("target_file", ""),
            "path": result.get("path", ""),
            "title": result.get("title", ""),
            "content": result.get("content", ""),
            "chars": result.get("chars", 0),
            "read_mode": result.get("read_mode") or "full",
            "range_requested": result.get("range_requested"),
            "range_applied": result.get("range_applied"),
            "total_lines": result.get("total_lines", 0),
            "total_chars": result.get("total_chars", 0),
        })
        reason = _clean(request.get("reason"))
        if reason:
            receipt["reason"] = reason
        mount = {
            "type": "container",
            "ids": receipt["container_id"],
            "mode": "resident",
            "source": "container_read",
            "target_file": receipt.get("target_file", ""),
            "path": receipt.get("path", ""),
            "content": receipt.get("content", ""),
            "read_mode": receipt.get("read_mode") or "full",
            "range_requested": receipt.get("range_requested"),
            "range_applied": receipt.get("range_applied"),
            "total_lines": receipt.get("total_lines", 0),
            "total_chars": receipt.get("total_chars", 0),
        }
        mounts.append({
            key: value for key, value in mount.items() if value is not None
        })
        receipts.append(receipt)
    return receipts, mounts
