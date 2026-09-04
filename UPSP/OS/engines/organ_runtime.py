"""Frozen organ topology and synchronous per-Frame dispatcher."""

import hashlib
import json
from collections import deque
from dataclasses import dataclass
from graphlib import CycleError, TopologicalSorter
from pathlib import Path
from types import MappingProxyType

from engines.product_committer import ORGAN_PRODUCT_TOOLS
from engines.reaction_helpers import record_pending_memory_ids


AXES = frozenset({"setup", "reaction", "cleanup"})
SUBSCRIPTIONS = frozenset({
    "setup_frame_settled",
    "reaction_frame_settled",
    "cleanup_frame_settled",
    "round_close_requested",
    "cleanup_obligation_created",
    "cleanup_obligation_settled",
    "cleanup_obligation_failed",
    "round_settled",
    "round_unsettled",
    "round_closed",
    "product_committed",
})
ROLE_FIELDS = frozenset({
    "id", "version", "enabled", "axes", "subscriptions", "requires",
    "provides", "context_mode", "context_provider", "handler",
    "product_tools",
})
PRODUCT_SUCCESS = frozenset({"accepted", "applied", "ok", "success", "settled"})


class OrganTopologyError(ValueError):
    pass


def _freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze(item) for item in value)
    return value


@dataclass(frozen=True)
class OrganInvocation:
    role_id: str
    event_type: str
    frame_ref: object
    topology_version: str
    context_mode: str
    context: object
    signals: tuple = ()


@dataclass(frozen=True)
class OrganResult:
    signals: tuple = ()
    products: tuple = ()


def organ_runtime_context(runtime, context):
    runner = runtime.reaction_loop_runner
    return {
        "round_num": context.round_num,
        "interaction_meta": context.interaction_meta,
        "pending_memory_ids": {},
        "visible_container_targets": tuple(
            runtime.assembler.visible_container_targets()),
        "chronicle_store": getattr(runner, "chronicle_store", None),
        "chronicle_write_scope": getattr(
            runner, "chronicle_write_scope", None),
        "memory_heat_boosted_ids": context.memory_heat_boosted_ids,
    }


class OrganRuntime:
    def __init__(
            self, manifest_path, product_committer, *, handlers=None,
            context_providers=None, audit=None):
        raw = Path(manifest_path).read_bytes()
        self.topology_version = hashlib.sha256(raw).hexdigest()
        self.product_committer = product_committer
        self.handlers = MappingProxyType(dict(handlers or {}))
        self.context_providers = MappingProxyType(dict(context_providers or {}))
        self.audit = audit
        self.roles, self.order, self.capability_roles = self._validate(
            json.loads(raw.decode("utf-8")))
        self._signals = {axis: deque() for axis in AXES}
        self._frame_signals = {}

    def _validate(self, manifest):
        if not isinstance(manifest, dict) or manifest.get(
                "schema_version") != "upsp_organ_topology.v1":
            raise OrganTopologyError("invalid_topology_schema")
        rows = manifest.get("roles")
        if not isinstance(rows, list):
            raise OrganTopologyError("roles_must_be_list")
        roles = {}
        capability_roles = {}
        for row in rows:
            if not isinstance(row, dict) or set(row) != ROLE_FIELDS:
                raise OrganTopologyError("invalid_role_fields")
            role_id = str(row.get("id") or "").strip()
            if not role_id or role_id in roles:
                raise OrganTopologyError("duplicate_or_empty_role_id")
            if not str(row.get("version") or "").strip() or not isinstance(
                    row.get("enabled"), bool):
                raise OrganTopologyError(f"invalid_role_identity:{role_id}")
            role = dict(row)
            for key in ("axes", "subscriptions", "requires", "provides",
                        "product_tools"):
                values = row.get(key)
                if (
                    not isinstance(values, list)
                    or any(not isinstance(value, str) or not value.strip()
                           for value in values)
                    or len(values) != len(set(values))
                ):
                    raise OrganTopologyError(f"invalid_role_lists:{role_id}")
                role[key] = [value.strip() for value in values]
            if row.get("context_mode") not in {"assembled", "cumulative"}:
                raise OrganTopologyError(f"invalid_context_mode:{role_id}")
            handler_id = row.get("handler")
            provider_id = row.get("context_provider")
            if not isinstance(handler_id, str) or not handler_id.strip():
                raise OrganTopologyError(f"unknown_handler:{role_id}")
            if not isinstance(provider_id, str) or not provider_id.strip():
                raise OrganTopologyError(f"unknown_context_provider:{role_id}")
            role["id"] = role_id
            role["version"] = str(row["version"]).strip()
            role["handler"] = handler_id.strip()
            role["context_provider"] = provider_id.strip()
            if not set(role["axes"]).issubset(AXES):
                raise OrganTopologyError(f"invalid_axis:{role_id}")
            subscriptions = set(role["subscriptions"])
            if not subscriptions.issubset(SUBSCRIPTIONS):
                raise OrganTopologyError(f"invalid_subscription:{role_id}")
            if not set(role["product_tools"]).issubset(ORGAN_PRODUCT_TOOLS):
                raise OrganTopologyError(f"invalid_product_tool:{role_id}")
            if not callable(self.handlers.get(role["handler"])):
                raise OrganTopologyError(f"unknown_handler:{role_id}")
            if not callable(self.context_providers.get(role["context_provider"])):
                raise OrganTopologyError(f"unknown_context_provider:{role_id}")
            roles[role_id] = role
            for capability in role["provides"]:
                if capability in capability_roles:
                    raise OrganTopologyError(f"duplicate_capability:{capability}")
                capability_roles[capability] = role_id
        graph = {}
        for role_id, role in roles.items():
            dependencies = set()
            for capability in role["requires"]:
                provider = capability_roles.get(capability)
                if not provider:
                    raise OrganTopologyError(f"missing_capability:{capability}")
                if role.get("enabled") and not roles[provider].get("enabled"):
                    raise OrganTopologyError(f"disabled_capability:{capability}")
                dependencies.add(provider)
            graph[role_id] = dependencies
        try:
            order = tuple(TopologicalSorter(graph).static_order())
        except CycleError as exc:
            raise OrganTopologyError("topology_cycle") from exc
        return (
            MappingProxyType({key: _freeze(value) for key, value in roles.items()}),
            order,
            MappingProxyType(capability_roles),
        )

    def begin_frame_materials(self, frame_ref):
        frame = frame_ref.as_dict() if hasattr(frame_ref, "as_dict") else dict(frame_ref or {})
        axis = frame.get("axis")
        if axis not in AXES:
            return ()
        signals = tuple(self._signals[axis])
        self._signals[axis].clear()
        self._frame_signals[axis] = signals
        return tuple({
            "role": "user",
            "kind": "organ_signal",
            "content": (
                "[organ_signal "
                f"source_role={signal.get('source_role', '')} "
                f"caused_by={signal.get('caused_by', '')}]\n"
                f"{signal.get('body', '')}"
            ),
            "source_role": signal.get("source_role", ""),
            "caused_by": signal.get("caused_by", ""),
            "interaction_object": "runtime",
            "identity_status": "system",
            "interaction_source": "organ_signal",
        } for signal in signals if signal.get("body"))

    def dispatch(self, event_type, frame_ref=None, payload=None, runtime_context=None):
        if event_type not in SUBSCRIPTIONS:
            raise OrganTopologyError(f"unsupported_event:{event_type}")
        if not self.roles:
            return {"records": [], "receipts": []}
        frame = frame_ref.as_dict() if hasattr(frame_ref, "as_dict") else dict(frame_ref or {})
        axis = frame.get("axis")
        context = dict(runtime_context or {})
        frame_signals = self._frame_signals.pop(axis, ())
        events = deque([(event_type, payload or {})])
        ran, failed, completed_caps = set(), set(), set()
        records, receipts = [], []
        product_sequence = 0
        while events:
            current_event, current_payload = events.popleft()
            active = self._activation(current_event, axis)
            for role_id in self.order:
                if role_id not in active or role_id in ran:
                    continue
                role = self.roles[role_id]
                missing = set(role["requires"]) - completed_caps
                if missing:
                    ran.add(role_id)
                    record = {
                        "kind": "organ_skipped",
                        "role_id": role_id,
                        "reason": "dependency_unavailable",
                        "missing": sorted(missing),
                    }
                    records.append(record)
                    self._audit(frame, record)
                    continue
                ran.add(role_id)
                try:
                    provider_input = _freeze({
                        "role": dict(role),
                        "event_type": current_event,
                        "payload": current_payload,
                        "frame_ref": frame,
                        "signals": frame_signals,
                    })
                    organ_context = self.context_providers[role["context_provider"]](
                        provider_input)
                    invocation = OrganInvocation(
                        role_id=role_id,
                        event_type=current_event,
                        frame_ref=_freeze(frame),
                        topology_version=self.topology_version,
                        context_mode=role["context_mode"],
                        context=_freeze(organ_context),
                        signals=tuple(_freeze(item) for item in frame_signals),
                    )
                    result = self.handlers[role["handler"]](invocation)
                    result = self._result(result)
                    signal_rows = [self._signal(role_id, frame, item) for item in result.signals]
                    product_rows = [dict(item) for item in result.products]
                    product_failed = False
                    for product in product_rows:
                        product_sequence += 1
                        try:
                            product_receipts = self.product_committer.commit_product(
                                product,
                                frame_ref=frame,
                                role_id=role_id,
                                sequence=product_sequence,
                                allowed_tools=role["product_tools"],
                                round_num=context.get(
                                    "round_num", frame.get("round_num", 0)),
                                interaction_meta=context.get("interaction_meta"),
                                pending_memory_ids=context.get("pending_memory_ids"),
                                visible_container_targets=context.get(
                                    "visible_container_targets", ()),
                                visible_relation_body_ids=context.get(
                                    "visible_relation_body_ids", ()),
                                chronicle_store=context.get("chronicle_store"),
                                chronicle_write_scope=context.get(
                                    "chronicle_write_scope"),
                                memory_heat_boosted_ids=context.get(
                                    "memory_heat_boosted_ids"),
                            )
                        except Exception as exc:
                            product_receipts = [{
                                "tool_id": product.get("tool_id", ""),
                                "status": "rejected",
                                "reason": f"product_commit_failed:{type(exc).__name__}",
                                "product_id": (
                                    f"{frame.get('frame_id', '')}:product:"
                                    f"{product_sequence}"
                                ),
                                "role_id": role_id,
                                "frame_id": frame.get("frame_id", ""),
                                "trigger_id": frame.get("trigger_id", ""),
                                "caused_by": frame.get("frame_id", ""),
                            }]
                        receipts.extend(product_receipts)
                        if product.get("tool_id") == "memory_write":
                            record_pending_memory_ids(
                                context.setdefault("pending_memory_ids", {}),
                                product_receipts,
                            )
                        for receipt in product_receipts:
                            record = self._product_record(role_id, receipt)
                            records.append(record)
                            self._audit(frame, record)
                            if receipt.get("status") in PRODUCT_SUCCESS:
                                events.append(("product_committed", record))
                            else:
                                product_failed = True
                    if product_failed:
                        failed.add(role_id)
                        record = {
                            "kind": "organ_failed",
                            "role_id": role_id,
                            "error": "product_commit_rejected",
                        }
                        records.append(record)
                        self._audit(frame, record)
                        continue
                    for signal, audit_row in signal_rows:
                        self._signals[signal["target_axis"]].append(signal)
                        records.append(audit_row)
                        self._audit(frame, audit_row)
                    completed_caps.update(role["provides"])
                except Exception as exc:
                    failed.add(role_id)
                    record = {
                        "kind": "organ_failed",
                        "role_id": role_id,
                        "error": f"organ_handler_failed:{type(exc).__name__}",
                    }
                    records.append(record)
                    self._audit(frame, record)
        return {"records": records, "receipts": receipts, "failed_roles": sorted(failed)}

    def _activation(self, event_type, axis):
        active = {
            role_id for role_id, role in self.roles.items()
            if role["enabled"]
            and event_type in role["subscriptions"]
            and (axis not in AXES or axis in role["axes"])
        }
        pending = list(active)
        while pending:
            role = self.roles[pending.pop()]
            for capability in role["requires"]:
                provider = self.capability_roles[capability]
                provider_role = self.roles[provider]
                if axis in AXES and axis not in provider_role["axes"]:
                    continue
                if provider not in active:
                    active.add(provider)
                    pending.append(provider)
        return active

    @staticmethod
    def _result(value):
        if isinstance(value, OrganResult):
            return value
        if not isinstance(value, dict):
            raise TypeError("organ_result_must_be_mapping")
        signals = value.get("signals") or ()
        products = value.get("products") or ()
        if not isinstance(signals, (list, tuple)) or not isinstance(products, (list, tuple)):
            raise TypeError("organ_outputs_must_be_lists")
        return OrganResult(tuple(signals), tuple(products))

    @staticmethod
    def _signal(role_id, frame, value):
        if not isinstance(value, dict):
            raise TypeError("organ_signal_must_be_mapping")
        signal_type = str(value.get("type") or "").strip()
        target_axis = str(value.get("target_axis") or "").strip()
        body = str(value.get("body") or "")
        if not signal_type or target_axis not in AXES:
            raise ValueError("invalid_organ_signal")
        if value.get("layer") in {"permanent", "system"}:
            raise ValueError("organ_signal_cannot_modify_system_layer")
        encoded = body.encode("utf-8")
        signal = {
            "type": signal_type,
            "target_axis": target_axis,
            "body": body,
            "source_role": role_id,
            "frame_id": frame.get("frame_id", ""),
            "caused_by": frame.get("frame_id", ""),
        }
        audit = {
            "kind": "organ_signal",
            "type": signal_type,
            "source_role": role_id,
            "target_axis": target_axis,
            "size_bytes": len(encoded),
            "sha256": hashlib.sha256(encoded).hexdigest(),
            "frame_id": frame.get("frame_id", ""),
            "caused_by": frame.get("frame_id", ""),
        }
        return signal, audit

    @staticmethod
    def _product_record(role_id, receipt):
        return {
            "kind": "product_committed",
            "role_id": role_id,
            "tool_id": receipt.get("tool_id", ""),
            "status": receipt.get("status", ""),
            "product_id": receipt.get("product_id", ""),
            "frame_id": receipt.get("frame_id", ""),
            "trigger_id": receipt.get("trigger_id", ""),
            "caused_by": receipt.get("caused_by", ""),
        }

    def _audit(self, frame, payload):
        if self.audit is not None and frame.get("round_num"):
            self.audit.get_store().append_event(
                frame["round_num"], "organ_runtime", payload)
