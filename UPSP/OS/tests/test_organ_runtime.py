import hashlib
import json
from types import MappingProxyType, SimpleNamespace

import pytest

from engines.organ_runtime import (
    OrganResult,
    OrganRuntime,
    OrganTopologyError,
)
from engines.product_committer import RuntimeProductCommitter
from engines.reaction_tool_settlement import ReactionToolSettlementDispatcher
from engines.round_context import FrameRef


def _role(role_id, **overrides):
    role = {
        "id": role_id,
        "version": "1.0.0",
        "enabled": True,
        "axes": ["setup", "reaction", "cleanup"],
        "subscriptions": ["setup_frame_settled"],
        "requires": [],
        "provides": [],
        "context_mode": "assembled",
        "context_provider": "context",
        "handler": role_id,
        "product_tools": [],
    }
    role.update(overrides)
    return role


def _manifest(tmp_path, roles):
    path = tmp_path / "topology.json"
    path.write_text(json.dumps({
        "schema_version": "upsp_organ_topology.v1",
        "roles": roles,
    }, ensure_ascii=False), encoding="utf-8")
    return path


class FakeCommitter:
    def __init__(self):
        self.calls = []

    def commit_product(self, product, **kwargs):
        self.calls.append((dict(product), dict(kwargs)))
        frame = kwargs["frame_ref"]
        return [{
            "tool_id": product.get("tool_id"),
            "status": "applied",
            "product_id": f"{frame['frame_id']}:product:{kwargs['sequence']}",
            "frame_id": frame["frame_id"],
            "trigger_id": frame.get("trigger_id", ""),
            "caused_by": frame["frame_id"],
        }]


def _runtime(tmp_path, roles, handlers, providers=None, committer=None):
    return OrganRuntime(
        _manifest(tmp_path, roles),
        committer or FakeCommitter(),
        handlers=handlers,
        context_providers=providers or {"context": lambda event: {}},
    )


def test_empty_topology_is_frozen_and_has_no_runtime_output(tmp_path):
    path = _manifest(tmp_path, [])
    raw = path.read_bytes()
    runtime = OrganRuntime(path, FakeCommitter())
    version = hashlib.sha256(raw).hexdigest()

    path.write_text("{}", encoding="utf-8")

    assert runtime.topology_version == version
    assert not runtime.roles
    with pytest.raises(TypeError):
        runtime.handlers["late"] = lambda call: {}
    assert runtime.dispatch("setup_frame_settled") == {
        "records": [], "receipts": []}


@pytest.mark.parametrize("roles,handlers,providers,reason", [
    (
        [_role("a", requires=["b"], provides=["a"]),
         _role("b", requires=["a"], provides=["b"])],
        {"a": lambda call: {}, "b": lambda call: {}},
        {"context": lambda event: {}},
        "topology_cycle",
    ),
    (
        [_role("a", requires=["missing"])],
        {"a": lambda call: {}},
        {"context": lambda event: {}},
        "missing_capability",
    ),
    (
        [_role("a", provides=["same"]), _role("b", provides=["same"])],
        {"a": lambda call: {}, "b": lambda call: {}},
        {"context": lambda event: {}},
        "duplicate_capability",
    ),
    (
        [_role("a", handler="missing")],
        {},
        {"context": lambda event: {}},
        "unknown_handler",
    ),
    (
        [_role("a", context_provider="missing")],
        {"a": lambda call: {}},
        {},
        "unknown_context_provider",
    ),
    (
        [_role("a", subscriptions=["heartbeat"])],
        {"a": lambda call: {}},
        {"context": lambda event: {}},
        "invalid_subscription",
    ),
])
def test_invalid_topologies_fail_closed(
        tmp_path, roles, handlers, providers, reason):
    with pytest.raises(OrganTopologyError, match=reason):
        OrganRuntime(
            _manifest(tmp_path, roles),
            FakeCommitter(),
            handlers=handlers,
            context_providers=providers,
        )


def test_dependency_order_and_failure_isolation(tmp_path):
    calls = []

    def fail(_invocation):
        calls.append("source")
        raise RuntimeError("boom")

    roles = [
        _role("source", subscriptions=[], provides=["source_ready"]),
        _role("dependent", requires=["source_ready"]),
        _role("unrelated"),
    ]
    runtime = _runtime(tmp_path, roles, {
        "source": fail,
        "dependent": lambda call: calls.append("dependent") or {},
        "unrelated": lambda call: calls.append("unrelated") or {},
    })

    result = runtime.dispatch(
        "setup_frame_settled", FrameRef.for_axis(1, "setup", 1))

    assert calls == ["source", "unrelated"]
    assert result["failed_roles"] == ["source"]
    assert "boom" not in json.dumps(result, ensure_ascii=False)
    assert any(row.get("role_id") == "dependent"
               and row.get("reason") == "dependency_unavailable"
               for row in result["records"])


def test_dag_dependency_is_activated_before_subscriber(tmp_path):
    calls = []
    roles = [
        _role("source", subscriptions=[], provides=["ready"]),
        _role("consumer", requires=["ready"]),
    ]
    runtime = _runtime(tmp_path, roles, {
        "source": lambda call: calls.append("source") or {},
        "consumer": lambda call: calls.append("consumer") or {},
    })

    runtime.dispatch("setup_frame_settled", FrameRef.for_axis(1, "setup", 1))

    assert calls == ["source", "consumer"]


def test_rejected_product_blocks_signals_and_dependent_capability(tmp_path):
    calls = []
    committer = SimpleNamespace(commit_product=lambda *args, **kwargs: [{
        "tool_id": "memory_write", "status": "rejected", "reason": "bad",
    }])
    roles = [
        _role("source", provides=["ready"], product_tools=["memory_write"]),
        _role("consumer", requires=["ready"]),
    ]
    runtime = _runtime(tmp_path, roles, {
        "source": lambda call: OrganResult(
            signals=({"type": "x", "target_axis": "reaction", "body": "no"},),
            products=({"tool_id": "memory_write", "arguments": {}},),
        ),
        "consumer": lambda call: calls.append("consumer") or {},
    }, committer=committer)

    result = runtime.dispatch(
        "setup_frame_settled", FrameRef.for_axis(1, "setup", 1))

    assert result["failed_roles"] == ["source"]
    assert calls == []
    assert runtime.begin_frame_materials(
        FrameRef.for_axis(1, "reaction", 1)) == ()


def test_signals_are_visible_only_at_next_target_frame_and_body_is_not_audited(
        tmp_path):
    secret = "只给下一反应帧的正文"
    seen = []
    runtime = _runtime(tmp_path, [
        _role("producer"),
        _role("consumer", subscriptions=["reaction_frame_settled"]),
    ], {
        "producer": lambda call: OrganResult(signals=({
            "type": "memory_material",
            "target_axis": "reaction",
            "body": secret,
        },)),
        "consumer": lambda call: seen.extend(call.signals) or {},
    })
    setup = FrameRef.for_axis(1, "setup", 1, trigger_id="T1")
    reaction = FrameRef.for_axis(1, "reaction", 1, trigger_id="T1")

    result = runtime.dispatch("setup_frame_settled", setup)

    assert secret not in json.dumps(result["records"], ensure_ascii=False)
    materials = runtime.begin_frame_materials(reaction)
    assert materials[0]["role"] == "user"
    assert materials[0]["kind"] == "organ_signal"
    assert materials[0]["content"].endswith(secret)
    assert materials[0]["source_role"] == "producer"
    assert materials[0]["caused_by"] == setup.frame_id
    runtime.dispatch("reaction_frame_settled", reaction)
    assert seen[0]["body"] == secret
    assert runtime.begin_frame_materials(reaction) == ()


def test_context_providers_and_handler_context_are_read_only(tmp_path):
    seen = []

    def provider(event):
        assert isinstance(event, MappingProxyType)
        return {"value": event["role"]["context_mode"]}

    def handler(invocation):
        assert isinstance(invocation.context, MappingProxyType)
        with pytest.raises(TypeError):
            invocation.context["value"] = "changed"
        seen.append((invocation.role_id, invocation.context_mode,
                     invocation.context["value"]))
        return {}

    roles = [
        _role("assembled", context_mode="assembled"),
        _role("cumulative", context_mode="cumulative"),
    ]
    runtime = _runtime(
        tmp_path, roles,
        {"assembled": handler, "cumulative": handler},
        {"context": provider},
    )

    runtime.dispatch("setup_frame_settled", FrameRef.for_axis(1, "setup", 1))

    assert seen == [
        ("assembled", "assembled", "assembled"),
        ("cumulative", "cumulative", "cumulative"),
    ]


@pytest.mark.parametrize("tool_id", [
    "file_write",
    "reaction_finalize",
    "memory_privacy_mark",
    "memory_privacy_declassify",
])
def test_product_committer_rejects_general_control_and_disabled_tools(tool_id):
    committer = RuntimeProductCommitter(SimpleNamespace())
    frame = FrameRef.for_axis(1, "reaction", 1, trigger_id="T1")

    receipt = committer.commit_product(
        {"tool_id": tool_id, "arguments": {}},
        frame_ref=frame,
        role_id="test",
        sequence=1,
        allowed_tools=[tool_id],
        round_num=1,
    )[0]

    assert receipt["status"] == "rejected"
    assert receipt["reason"] == "organ_product_tool_not_allowed"
    assert receipt["product_id"] == "R000001:reaction:1:product:1"
    assert receipt["trigger_id"] == "T1"


class _State:
    @staticmethod
    def load():
        return {"base": {}}


class _Relation:
    @staticmethod
    def resolve_active_subject(value):
        return value if value == "FMZ" else None


class _FileMemoryStore:
    def __init__(self, root):
        self.root = root
        self.entries = {}
        self.meta = {}
        self.index = []

    def _save(self, name, value):
        (self.root / name).write_text(
            json.dumps(value, ensure_ascii=False, sort_keys=True, default=str),
            encoding="utf-8")

    def write_entry(self, mem_id, title, summary, **kwargs):
        self.entries[mem_id] = {"title": title, "body": summary, **kwargs}
        self._save("memory.json", self.entries)

    def set_meta(self, mem_id, meta):
        self.meta[mem_id] = dict(meta)
        self._save("meta.json", self.meta)

    def append_index(self, mem_id, entry_type, weight, title, **kwargs):
        self.index.append([mem_id, entry_type, weight, title, kwargs])
        self._save("index.json", self.index)


class _FileMap:
    def __init__(self, root, name):
        self.path = root / name
        self.rows = {}

    def _set(self, key, value):
        self.rows[key] = value
        self.path.write_text(json.dumps(
            self.rows, ensure_ascii=False, sort_keys=True, default=str),
            encoding="utf-8")

    def set_entry(self, key, value):
        self._set(key, value)

    def add_stm_keywords(self, key, value):
        self._set(key, value)


class _FileHeat(_FileMap):
    @staticmethod
    def new_entry(weight=2):
        from schemas.memory import default_heat_entry
        value = default_heat_entry(weight=weight)
        value["last_heat_at"] = "2026-07-16T00:00:00+08:00"
        return value


def _memory_services(root):
    return SimpleNamespace(
        sm=_State(),
        memory_store=_FileMemoryStore(root),
        memory_index=_FileMap(root, "keywords.json"),
        heat=_FileHeat(root, "heat.json"),
        container_store=SimpleNamespace(),
        relation_store=_Relation(),
    )


def _tree(root):
    return {path.name: path.read_bytes() for path in root.iterdir() if path.is_file()}


def test_fake_memory_organ_reuses_reaction_committer_receipt_and_file_result(
        tmp_path, monkeypatch):
    from logic import memory_write

    direct_root = tmp_path / "direct"
    organ_root = tmp_path / "organ"
    direct_root.mkdir()
    organ_root.mkdir()
    monkeypatch.setattr(memory_write, "generate_mem_id", lambda: "MEM-TEST659")
    make_meta = memory_write.make_meta_template

    def fixed_meta(*args, **kwargs):
        value = make_meta(*args, **kwargs)
        value["created_at"] = value["last_recalled_at"] = "2026-07-16T00:00:00+08:00"
        return value

    monkeypatch.setattr(memory_write, "make_meta_template", fixed_meta)
    declaration = {
        "title": "器官共用提交",
        "weight": 4,
        "subject": "FMZ",
        "body": "相同 processor 应产生相同记忆正文与元数据。",
        "candidate_keywords": ["器官", "提交"],
    }
    meta = {
        "interaction_object": "FMZ",
        "identity_status": "known",
        "interaction_source": "test",
    }
    reaction_committer = RuntimeProductCommitter(_memory_services(direct_root))
    direct = ReactionToolSettlementDispatcher(SimpleNamespace(
        product_committer=reaction_committer,
    ))._commit(
        "memory_write", [declaration], round_num=659,
        interaction_meta=meta)[0]
    committer = RuntimeProductCommitter(_memory_services(organ_root))
    role = _role(
        "memory_organ",
        product_tools=["memory_write"],
    )
    runtime = _runtime(
        tmp_path, [role],
        {"memory_organ": lambda call: OrganResult(products=({
            "tool_id": "memory_write",
            "arguments": declaration,
        },))},
        committer=committer,
    )
    frame = FrameRef.for_axis(659, "setup", 1, trigger_id="T659")

    organ = runtime.dispatch(
        "setup_frame_settled", frame,
        runtime_context={"round_num": 659, "interaction_meta": meta},
    )["receipts"][0]
    normalized = {key: value for key, value in organ.items() if key not in {
        "product_id", "frame_id", "trigger_id", "role_id", "caused_by"}}

    assert normalized == direct
    assert _tree(organ_root) == _tree(direct_root)
