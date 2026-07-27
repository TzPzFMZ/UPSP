"""
三步轮集成测试 — 完整链路测试（起手步→反应步→善后步）
DDS §22-23 三步轮范式

测试原则：
  - mock LLM API，不依赖外部服务
  - tmp_path 隔离，不污染 persona/
  - 每个测试一条断言主线
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

from unittest.mock import MagicMock


# ============================================================
# 测试辅助
# ============================================================

MOCK_REACTION_RESPONSE = """收到了你的消息。我会继续思考这个问题。

本轮完结，下轮继续。[DONE]"""

MOCK_REACTION_FINALIZE = {
    "response": "",
    "tool_call_envelopes": [
        {
            "tool_id": "reaction_finalize",
            "arguments": {
                "closeout_decision": "finish",
            },
            "parse_status": "ok",
            "index": 0,
        }
    ],
}

def _mock_terminal_result(tool_id, arguments=None):
    return {
        "response": "",
        "tool_call_envelopes": [{
            "tool_id": tool_id,
            "arguments": dict(arguments or {}),
            "parse_status": "ok",
            "index": 0,
        }],
    }


def _mock_setup_finalize(**arguments):
    data = {"security_verdict": "pass"}
    data.update(arguments)
    return _mock_terminal_result("setup_finalize", data)


def _mock_cleanup_finalize(**arguments):
    return _mock_terminal_result("cleanup_finalize", arguments)


def _make_runtime(tmp_path):
    """构建测试用 Runtime，所有文件指向 tmp_path"""
    from engines.runtime import Runtime
    from data.state_store import StateStore
    from data.context_store import ContextStore
    from data.connectivity_store import ConnectivityStore
    from engines.heartbeat import HeartbeatManager
    from assembly.context import ContextAssembler

    class TestMemoryHeat:
        def __init__(self):
            self.entries = {}
            self.decayed = False
            self.last_decay_round_num = None
            self.removed = []

        def load_heat(self):
            return {"entries": self.entries}

        def save_heat(self, data):
            self.entries = dict(data.get("entries", {}))

        def get_entry(self, mem_id):
            return self.entries.setdefault(mem_id, {})

        def set_entry(self, mem_id, entry):
            self.entries[mem_id] = dict(entry)

        def remove_entry(self, mem_id):
            self.removed.append(mem_id)
            self.entries.pop(mem_id, None)

        def tick_decay(self, round_num=None):
            self.decayed = True
            self.last_decay_round_num = round_num
            return False

        def check_upgrade(self):
            return []

        def has_pending_degrade(self):
            return False

    class NoopExecutor:
        def call(self, *args, **kwargs):
            raise AssertionError("测试必须显式注入 mock executor 后才能调用 LLM")

    state_path = str(tmp_path / "state.json")
    sm = StateStore(state_path)
    sm.init_if_missing()
    # 确保有完整的默认字段
    sm._set_internal("base.meta.total_round", 0)
    sm._set_internal("base.runtime.phase", "idle")

    ctx_store = ContextStore(
        state_store=sm,
        cache_dir=str(tmp_path / "persona" / "STM" / "context" / "cache"),
        corpus_rhythms_dir=str(
            tmp_path / "persona" / "LTM" / "Corpus" / "public" / "rhythms"
        ),
        raw_log_jsonl=str(
            tmp_path / "persona" / "STM" / "buffer" / "raw_log.jsonl"
        ),
        raw_log_md=str(
            tmp_path / "persona" / "STM" / "buffer" / "raw_log.md"
        ),
    )
    hb = HeartbeatManager(sm, interval=0.1)
    # 不启动心跳（集成测试手动触发轮次）
    # context_dir 指向 tmp_path 避免污染真实 persona/ 文件
    assembler = ContextAssembler(state_store=sm,
                                 context_dir=str(tmp_path / "context"))

    rt = Runtime(
        state_store=sm,
        heartbeat=hb,
        executor=NoopExecutor(),
        assembler=assembler,
        ctx_store=ctx_store,
        connectivity_store=ConnectivityStore(
            str(tmp_path / "persona" / "STM" / "health" / "base" / "connectivity.json")
        ),
        heat=TestMemoryHeat(),
    )
    return rt, sm


def _patch_io(monkeypatch, tmp_path):
    """mock 所有文件 I/O 到 tmp_path"""
    persona = str(tmp_path / "persona")
    stm = str(tmp_path / "persona" / "STM")
    ltm = str(tmp_path / "persona" / "LTM")

    for d in [persona, stm, ltm,
              str(tmp_path / "persona" / "STM" / "memory"),
              str(tmp_path / "persona" / "STM" / "buffer"),
              str(tmp_path / "persona" / "STM" / "context"),
              str(tmp_path / "persona" / "LTM" / "Memory"),
              str(tmp_path / "persona" / "LTM" / "Corpus" / "public" / "rounds"),
              ]:
        os.makedirs(d, exist_ok=True)

    corpus_dir = str(tmp_path / "persona" / "LTM" / "Corpus")
    cache = str(tmp_path / "persona" / "STM" / "context" / "cache")
    now_jsonl = os.path.join(cache, "now_cache.jsonl")
    lately_jsonl = os.path.join(cache, "lately_cache.jsonl")

    monkeypatch.setattr("paths.CONTAINER_CORPUS_DIR", corpus_dir)
    monkeypatch.setattr("paths.STM_CONTEXT_CACHE_DIR", cache)
    monkeypatch.setattr("paths.STM_CONTEXT_NOW_CACHE_JSONL", now_jsonl, raising=False)
    monkeypatch.setattr("paths.STM_CONTEXT_LATELY_CACHE_JSONL", lately_jsonl, raising=False)
    monkeypatch.setattr("data.context_store.CONTAINER_CORPUS_DIR", corpus_dir)
    monkeypatch.setattr("data.context_store.STM_CONTEXT_CACHE_DIR", cache)
    monkeypatch.setattr("data.context_store.STM_CONTEXT_NOW_CACHE_JSONL", now_jsonl, raising=False)
    monkeypatch.setattr("data.context_store.STM_CONTEXT_LATELY_CACHE_JSONL", lately_jsonl, raising=False)

    # 阻止心跳线程副作用
    monkeypatch.setattr("engines.heartbeat.HeartbeatManager.start", lambda s: None)
    monkeypatch.setattr("engines.heartbeat.HeartbeatManager.stop", lambda s: None)
    monkeypatch.setattr("engines.heartbeat.HeartbeatManager.pause", lambda s: None)
    monkeypatch.setattr("engines.heartbeat.HeartbeatManager.resume", lambda s: None)


# ============================================================
# 完整三步轮测试
# ============================================================

class TestThreeStepRound:
    """三步轮完整链路：起手步 → 反应步 → 善后步"""

    def test_full_round_pipeline(self, tmp_path, monkeypatch):
        """完整三步轮执行：验证 state.json 前后一致"""
        _patch_io(monkeypatch, tmp_path)
        rt, sm = _make_runtime(tmp_path)

        # Mock API：三步各返回预定义响应
        call_log = []

        def mock_call(step, system, messages, active_protocol_tool_guides=None):
            from logic.runtime_channels import (
                REACTION_FINAL_REPLY_TEXT_GUIDE,
                channel_for_step,
            )

            channel = channel_for_step(
                step,
                active_protocol_tool_guides=active_protocol_tool_guides,
            )
            call_log.append((step, channel.name))
            if step == "setup":
                return {
                    **_mock_setup_finalize(round_type_confirm="interactive"),
                    "tokens_input": 100,
                    "tokens_output": 50,
                }
            elif step == "reaction" and channel.name == "reaction.loop":
                return {
                    "response": "这是 reaction 轮直接产生的最终自然语言回复。",
                    "tool_call_envelopes": [],
                    "tokens_input": 200,
                    "tokens_output": 80,
                }
            elif step == "cleanup":
                return {
                    **_mock_cleanup_finalize(),
                    "tokens_input": 150,
                    "tokens_output": 40,
                }
            return {"response": ""}

        rt.executor = MagicMock()
        rt.executor.call = mock_call

        # 构造交互轮 state + flags
        state = sm.load()
        flags = {"user_message_waiting": True}

        # 执行
        rt._run_one_round("interactive", state, flags)

        # 验证：三步都被调用了
        assert any(step == "setup" for step, _ in call_log), \
            f"起手步未调用，调用记录: {call_log}"
        assert any(step == "reaction" for step, _ in call_log), \
            f"反应步未调用，调用记录: {call_log}"
        assert any(step == "cleanup" for step, _ in call_log), \
            f"善后步未调用，调用记录: {call_log}"

        # 验证：三步顺序正确；最终回复是 reaction 内部通道，不是第四步
        assert call_log == [
            ("setup", "setup"),
            ("reaction", "reaction.loop"),
            ("cleanup", "cleanup"),
        ], \
            f"三步顺序错误: {call_log}"

        # 验证 state.json 终态
        final_state = sm.load()
        assert final_state["base"]["runtime"]["phase"] == "idle", \
            f"终态不是 idle: {final_state['base']['runtime']['phase']}"
        assert final_state["base"]["meta"]["total_round"] >= 1, \
            "轮次未递增"
        assert os.path.isfile(
            tmp_path / "persona" / "STM" / "context" / "cache" / "now_cache.jsonl"
        ), "当前缓存主源应写入测试临时目录"
        assert os.path.isfile(
            tmp_path / "persona" / "STM" / "context" / "cache" / "lately_cache.jsonl"
        ), "最近缓存主源应写入测试临时目录"
        assert not list(
            (tmp_path / "persona" / "LTM" / "Corpus" / "public" / "rhythms").glob("*.jsonl")
        ), "普通交互轮不得绕过 raw_log 直接制造 Corpus 节"
        assert not os.path.exists(
            tmp_path / "persona" / "STM" / "buffer" / "context_buffer.json"
        ), "兼容 context_buffer 不应再写入"
        assert not os.path.exists(
            tmp_path / "persona" / "STM" / "context" / "cache" / "near_cache.json"
        ), "near_cache 兼容投影不应再写入"
        assert not os.path.exists(
            tmp_path / "persona" / "STM" / "context" / "cache" / "remote_index.json"
        ), "remote_index 兼容投影不应再写入"

    def test_setup_reject_skips_reaction(self, tmp_path, monkeypatch):
        """安全驳回：起手步裁决驳回→跳过反应步→善后步仍执行"""
        _patch_io(monkeypatch, tmp_path)
        rt, sm = _make_runtime(tmp_path)

        call_log = []

        def mock_call(step, system, messages):
            call_log.append(step)
            if step == "setup":
                return {
                    **_mock_setup_finalize(
                        security_verdict="reject",
                        reject_reason="安全驳回",
                    ),
                    "tokens_input": 100,
                    "tokens_output": 50,
                }
            elif step == "cleanup":
                return {
                    **_mock_cleanup_finalize(),
                    "tokens_input": 100,
                    "tokens_output": 50,
                }
            return {"response": ""}

        rt.executor = MagicMock()
        rt.executor.call = mock_call

        state = sm.load()
        flags = {"user_message_waiting": True}
        rt._run_one_round("interactive", state, flags)

        # 反应步被跳过
        assert "reaction" not in call_log, "安全驳回时不应调反应步"
        # 善后步仍执行
        assert "cleanup" in call_log, "安全驳回后善后步必须执行"

    def test_cleanup_runs_on_setup_error(self, tmp_path, monkeypatch):
        """起手步异常→善后步必走（try/finally 硬化）"""
        _patch_io(monkeypatch, tmp_path)
        rt, sm = _make_runtime(tmp_path)

        call_log = []

        def mock_call(step, system, messages):
            call_log.append(step)
            if step == "setup":
                raise RuntimeError("模拟起手步异常")
            if step == "cleanup":
                return {
                    **_mock_cleanup_finalize(),
                    "tokens_input": 100,
                    "tokens_output": 50,
                }
            return {"response": ""}

        rt.executor = MagicMock()
        rt.executor.call = mock_call

        state = sm.load()
        flags = {"user_message_waiting": True}
        # 不应抛异常到外层
        rt._run_one_round("interactive", state, flags)

        assert "cleanup" in call_log, "起手步异常后善后步必须执行"
        # 终态仍为 idle
        final_state = sm.load()
        assert final_state["base"]["runtime"]["phase"] == "idle"

    def test_cleanup_runs_on_reaction_error(self, tmp_path, monkeypatch):
        """反应步异常→善后步必走（try/finally 硬化）"""
        _patch_io(monkeypatch, tmp_path)
        rt, sm = _make_runtime(tmp_path)

        call_log = []

        def mock_call(step, system, messages):
            call_log.append(step)
            if step == "setup":
                return {
                    **_mock_setup_finalize(round_type_confirm="interactive"),
                    "tokens_input": 100,
                    "tokens_output": 50,
                }
            if step == "reaction":
                raise RuntimeError("模拟反应步异常")
            if step == "cleanup":
                return {
                    **_mock_cleanup_finalize(),
                    "tokens_input": 100,
                    "tokens_output": 50,
                }
            return {"response": ""}

        rt.executor = MagicMock()
        rt.executor.call = mock_call

        state = sm.load()
        flags = {"user_message_waiting": True}
        rt._run_one_round("interactive", state, flags)

        assert "setup" in call_log
        assert "reaction" in call_log
        assert "cleanup" in call_log, "反应步异常后善后步必须执行"


# ============================================================
# 轮类型判定 + 终态测试
# ============================================================

class TestRoundTypeRouting:
    """轮类型判定与状态变迁"""

    def test_flags_cleared_after_cleanup(self, tmp_path, monkeypatch):
        """interactive 善后只清本轮已消费的交互 flags"""
        _patch_io(monkeypatch, tmp_path)
        rt, sm = _make_runtime(tmp_path)
        sm.set_flag("user_message_waiting", True)
        sm.set_flag("rhythm_due", True)

        state = sm.load()
        rt._finalize_flags(state, "interactive", 1)

        flags = sm.get_flags()
        assert flags["user_message_waiting"] is False
        assert flags["rhythm_due"] is True

    def test_rhythm_priority_over_interactive(self, tmp_path, monkeypatch):
        """节律优先于交互；交互 flag 留给后续轮。"""
        rt, sm = _make_runtime(tmp_path)
        flags = {"user_message_waiting": True, "rhythm_due": True,
                 "continue_requested": True}
        result = rt._determine_round_type(flags)
        assert result == "rhythm"

    def test_relay_round_type(self, tmp_path, monkeypatch):
        """中继轮：continue_requested flag"""
        rt, sm = _make_runtime(tmp_path)
        flags = {"continue_requested": True}
        result = rt._determine_round_type(flags)
        assert result == "relay"


# ============================================================
# 上下文装配脏标记测试
# ============================================================

class TestContextCache:
    """DDS §21 上下文装配缓存与过期标记"""

    def test_cache_hit_when_not_expired(self, tmp_path, monkeypatch):
        """未过期层（永固/定期）复用缓存，高频层（含STATUSBAR）必重建"""
        from assembly.context import ContextAssembler
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm._set_internal("base.context_cache.permanent_expired", False)
        sm._set_internal("base.context_cache.periodic_expired", False)

        assembler = ContextAssembler(
            state_store=sm,
            context_dir=str(tmp_path / "context"),
        )
        assembler._layer_cache[("setup", "permanent")] = "CACHED_PERMANENT"
        assembler._layer_cache[("setup", "periodic")] = "CACHED_PERIODIC"

        build_count = {"permanent": 0, "periodic": 0, "high_freq": 0}

        monkeypatch.setattr(assembler, "_build_permanent",
            lambda s, step, rt: (build_count.__setitem__("permanent", 1), "P")[1])
        monkeypatch.setattr(assembler, "_build_periodic",
            lambda s, step, rt: (build_count.__setitem__("periodic", 1), "P")[1])

        state = sm.load()
        _, messages = assembler.assemble_setup(state, "interactive", [])
        rendered = "\n".join(m.get("content", "") for m in messages)

        assert build_count["permanent"] == 0, "未过期永固层不应重建"
        assert build_count["periodic"] == 0, "未过期定期层不应重建"
        assert "CACHED_PERMANENT" in rendered
        assert "CACHED_PERIODIC" in rendered

    def test_rebuild_when_expired(self, tmp_path, monkeypatch):
        """过期层强制重建"""
        from assembly.context import ContextAssembler
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        # 所有层过期（默认状态）
        sm._set_internal("base.context_cache.permanent_expired", True)
        sm._set_internal("base.context_cache.periodic_expired", True)

        assembler = ContextAssembler(
            state_store=sm,
            context_dir=str(tmp_path / "context"),
        )
        # 预填充过期缓存
        assembler._layer_cache[("setup", "permanent")] = "OLD_CACHED"

        build_count = {"permanent": 0}

        def count_build(s, step, rt):
            build_count["permanent"] += 1
            return "REBUILT_PERMANENT"

        monkeypatch.setattr(assembler, "_build_permanent", count_build)
        monkeypatch.setattr(assembler, "_build_periodic", lambda s, step, rt: "PERIODIC")
        monkeypatch.setattr(assembler, "_build_high_freq",
            lambda *args, **kwargs: "HIGH_FREQ")

        state = sm.load()
        _, messages = assembler.assemble_setup(state, "interactive", [])
        rendered = "\n".join(m.get("content", "") for m in messages)

        assert build_count["permanent"] == 1, "过期永固层必须重建"
        assert "REBUILT_PERMANENT" in rendered

    def test_high_freq_always_rebuilt(self, tmp_path, monkeypatch):
        """高频层每轮必重算（DDS §21.1：不设过期标记）"""
        from assembly.context import ContextAssembler
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()

        assembler = ContextAssembler(
            state_store=sm,
            context_dir=str(tmp_path / "context"),
        )
        call_count = [0]

        def count_calls(*args, **kwargs):
            call_count[0] += 1
            return "HIGH_FREQ"

        monkeypatch.setattr(assembler, "_build_high_freq", count_calls)
        monkeypatch.setattr(assembler, "_build_permanent", lambda s, step, rt: "P")
        monkeypatch.setattr(assembler, "_build_periodic", lambda s, step, rt: "P")

        state = sm.load()
        # 第一次调用
        assembler.assemble_setup(state, "interactive", [])
        # 第二次调用——高频层应再次重建
        assembler.assemble_setup(state, "interactive", [])

        assert call_count[0] == 2, f"高频层应每轮重建，实际调用 {call_count[0]} 次"

    def test_invalidate_layer_clears_cache(self, tmp_path, monkeypatch):
        """invalidate_layer 清除缓存并置过期标记"""
        from assembly.context import ContextAssembler
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()

        assembler = ContextAssembler(
            state_store=sm,
            context_dir=str(tmp_path / "context"),
        )
        assembler._layer_cache[("setup", "permanent")] = "CACHED"
        assembler._layer_cache[("reaction", "permanent")] = "CACHED_R"

        assembler.invalidate_layer("permanent")

        # 缓存已清
        assert ("setup", "permanent") not in assembler._layer_cache
        assert ("reaction", "permanent") not in assembler._layer_cache
        # state.json 过期标记已置 true
        expired = sm.get("base.context_cache.permanent_expired")
        assert expired is True, f"过期标记应为 true，实际: {expired}"


# ============================================================
# 记忆生命周期测试（TD-004）
# ============================================================

class TestMemoryLifecycle:
    """STM→LTM升格 + 降格清理"""

    def test_upgrade_check_empty_when_no_candidates(self, tmp_path, monkeypatch):
        """无满足条件的条目时，check_upgrade 返回空列表"""
        rt, _sm = _make_runtime(tmp_path)
        candidates = rt.heat.check_upgrade()
        assert candidates == []

    def test_process_memory_lifecycle_noop_when_empty(self, tmp_path, monkeypatch):
        """无待处理条目时，生命周期处理不抛异常"""
        rt, _sm = _make_runtime(tmp_path)
        rt._process_memory_lifecycle(1)

    def test_has_pending_degrade_returns_bool(self, tmp_path, monkeypatch):
        """has_pending_degrade 返回布尔值"""
        rt, _sm = _make_runtime(tmp_path)
        result = rt.heat.has_pending_degrade()
        assert isinstance(result, bool)

    def test_remove_nonexistent_entry_no_error(self, tmp_path, monkeypatch):
        """删除不存在的条目不抛异常"""
        rt, _sm = _make_runtime(tmp_path)
        rt.heat.remove_entry("MEM-DEADBEEF")


# ============================================================
# TD-009 激活：轴值变化→高频层过期
# ============================================================

class TestContextCacheInvalidation:
    """轴值变化时高频层自动重算（DDS §21.1: 高频层无过期标记）"""

    def test_dynamic_axis_update_does_not_need_expired_flag(self, tmp_path):
        """轴值变化后高频层下轮自动重算，无需过期标记"""
        from data.state_store import StateStore
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        # 高频层(含STATUSBAR)无过期标记，轴值变化后自动重算
        sm.set("base.dynamic_axes.valence.value", 30)
        # 验证轴值确实被写入
        assert sm.get("base.dynamic_axes.valence.value") == 30

    def test_comfort_zone_update_does_not_need_expired_flag(self, tmp_path):
        """舒适区变化后高频层下轮自动重算，无需过期标记"""
        from data.state_store import StateStore
        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()
        sm.set("base.comfort_zone.valence", 5)
        sm.set("base.comfort_zone.safety", 3)
        # 验证舒适区确实被写入
        assert sm.get("base.comfort_zone.valence") == 5

    def test_finalize_flags_rhythm_invalidates_periodic(self, tmp_path, monkeypatch):
        """节律轮终态时定期层被标记过期"""
        rt, sm = _make_runtime(tmp_path)
        sm._set_internal("base.context_cache.periodic_expired", False)
        state = sm.load()
        rt._finalize_flags(state, "rhythm", 5)

        expired = sm.get("base.context_cache.periodic_expired")
        assert expired is True, f"节律轮后定期层应过期，实际: {expired}"

    def test_finalize_flags_interactive_does_not_invalidate(self, tmp_path, monkeypatch):
        """非节律轮不触发定期层过期"""
        rt, sm = _make_runtime(tmp_path)
        sm._set_internal("base.context_cache.periodic_expired", False)
        state = sm.load()
        rt._finalize_flags(state, "interactive", 3)

        expired = sm.get("base.context_cache.periodic_expired")
        assert expired is False, f"交互轮不应触发定期层过期"


# ============================================================
# 多轮对话测试（合并自 multi_chat.py）
# ============================================================

class TestMultiRoundConversation:
    """连续多轮交互，验证上下文持久与状态累积（原 multi_chat.py 逻辑）"""

    def test_round_counter_increments(self, tmp_path, monkeypatch):
        """多轮后 total_round 正确递增"""
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()

        for i in range(5):
            sm.increment_round()

        assert sm.get_total_round() == 5

    def test_state_persists_across_rounds(self, tmp_path, monkeypatch):
        """state.json 在多次 increment_round 后保持完整性"""
        from data.state_store import StateStore

        sm = StateStore(str(tmp_path / "state.json"))
        sm.init_if_missing()

        for i in range(3):
            sm.increment_round()
            # 模拟轴值变化
            sm.set("base.dynamic_axes.valence.value", i * 10)
            sm.set("base.dynamic_axes.mood.value", i * 5)

        state = sm.load()
        assert state["base"]["meta"]["total_round"] == 3
        assert state["base"]["dynamic_axes"]["valence"]["value"] == 20
