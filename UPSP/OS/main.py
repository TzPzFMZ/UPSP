"""
UPSP Base V2 — 主入口
DDS §22-23 三步轮范式

启动流程：
  1. 初始化 config/；persona/ 必须先由显式用户初始化流程原子创建
  2. 组装五层架构：data → logic → assembly → engines
  3. 进入主循环：心跳 → 五类轮路由 → 三步执行

用法：
  python OS/main.py                 # 从 UPSP 根进入主循环（交互式）
  python OS/main.py --once          # 从 UPSP 根跑一轮后退出（测试用）
  python OS/main.py --message "..." # 从 UPSP 根发送一条消息并等待回复
"""
import sys
import os
import json

# 修复 Windows GBK 终端编码问题（stdout + stderr）
for stream in (sys.stdout, sys.stderr):
    try:
        if stream.encoding != 'utf-8':
            stream.reconfigure(encoding='utf-8', errors='replace')
    except Exception:
        pass
# 确保子进程也走 UTF-8
os.environ.setdefault('PYTHONIOENCODING', 'utf-8')

# 确保 OS/ 在 sys.path
_OS_ROOT = os.path.dirname(os.path.abspath(__file__))
_UPSP_ROOT = os.path.dirname(_OS_ROOT)
sys.path.insert(0, _OS_ROOT)
sys.path.insert(0, _UPSP_ROOT)

from initialization.windows_data import ensure_active_instance

ensure_active_instance(_UPSP_ROOT)

from data.state_store import StateStore
from data.config_store import ConfigStore
from data.files_store import FilesStore
from engines.heartbeat import (
    HeartbeatManager,
    round_decision_from_heartbeat_flags,
    round_type_from_heartbeat_flags,
)
from engines.executor import APIExecutor
from engines.runtime import Runtime
from assembly.context import ContextAssembler
from logic.context_profile import normalize_context_profile
from logic.single_round_probe_policy import (
    isolate_single_round_probe_flags,
    single_round_probe_enabled,
)
from initialization.persona_initializer import PersonaInitializer
from data.persona_identity import public_identity
from paths import (
    ACTIVE_PID,
    PERSONA_DIR,
    PERSONA_PRESETS_DIR,
    PERSONA_TEMPLATE_DIR,
    SHARED_PERSONA_DIR,
)


def init_environment():
    """初始化运行环境：确保所有必需文件和目录存在"""
    print("[UPSP] 初始化运行环境...")

    cfg = ConfigStore()

    # 初始化所有配置文件
    created = cfg.init_all()
    for name in created:
        print(f"  config/{name}.json 已创建")

    sm = StateStore()
    sm.migrate_memory_compression_flags()

    status = PersonaInitializer(
        PERSONA_DIR,
        PERSONA_TEMPLATE_DIR,
        PERSONA_PRESETS_DIR,
        pid=ACTIVE_PID,
        shared_persona_dir=SHARED_PERSONA_DIR,
    ).status()
    if not status["ready"]:
        raise RuntimeError("persona_initialization_required")

    # 仅在完整位格已经存在后确保运行时派生目录。
    import os as _os
    from paths import (
        STM_MEMORY_DIR, STM_BUFFER_DIR, STM_HEALTH_DIR, STM_CONTEXT_DIR,
        LTM_DIR, LTM_MEMORY_DIR,
        TRASH_DIR, WB_DIR, CONFIG_DIR,
    )
    for d in [STM_MEMORY_DIR, STM_BUFFER_DIR, STM_HEALTH_DIR,
              STM_CONTEXT_DIR, LTM_DIR, LTM_MEMORY_DIR,
              TRASH_DIR, WB_DIR, CONFIG_DIR]:
        _os.makedirs(d, exist_ok=True)
    FilesStore().ensure_layout()

    return sm, cfg


def build_runtime(sm, cfg, *, context_profile="full"):
    """组装 Runtime 实例"""
    context_profile = normalize_context_profile(context_profile)
    executor = APIExecutor(config_store=cfg)
    assembler = ContextAssembler(
        state_store=sm,
        config_store=cfg,
        context_profile=context_profile,
    )
    heartbeat = HeartbeatManager(state_store=sm, config_store=cfg)

    runtime = Runtime(
        state_store=sm,
        heartbeat=heartbeat,
        executor=executor,
        assembler=assembler,
        config_store=cfg,
    )
    return runtime


def build_pending_report(sm):
    """只读当前 heartbeat flags，供 CLI 准入检查使用。"""
    state = sm.load()
    base = state.get("base", {})
    meta = base.get("meta", {})
    flags = base.get("heartbeat_flags", {})
    active_flags = [name for name, value in flags.items() if value]
    decision = round_decision_from_heartbeat_flags(flags)
    return {
        "total_round": meta.get("total_round", 0),
        "active_flags": active_flags,
        "round_type": decision.get("round_type") or round_type_from_heartbeat_flags(flags),
        "guide_queue": decision.get("guide_queue") or [],
        "coalesced": bool(decision.get("coalesced")),
        "deferred_items": decision.get("deferred_items") or [],
        "phase": base.get("runtime", {}).get("phase", "idle"),
    }


def _runtime_round_decision(rt, flags):
    try:
        return rt._determine_round_decision(flags)
    except AttributeError:
        return round_decision_from_heartbeat_flags(flags)


def _stamp_round_decision(result, decision):
    result["round_type"] = decision.get("round_type")
    result["guide_queue"] = decision.get("guide_queue") or []
    result["coalesced"] = bool(decision.get("coalesced"))
    result["deferred_items"] = decision.get("deferred_items") or []


def run_pending(sm):
    """打印当前待处理状态，不启动 heartbeat，不调 API。"""
    print(json.dumps(build_pending_report(sm), ensure_ascii=False, indent=2))


def run_once(sm, cfg):
    """跑一轮后退出（测试用）"""
    print("[UPSP] 单轮模式")
    rt = build_runtime(sm, cfg)

    # 手动触发一轮交互
    rt.hb.start()

    state = rt.sm.load()
    flags = rt.sm.get_flags()

    # 手动置位用户消息标记
    rt.hb.enqueue_message("UPSP 启动自检消息")

    # 等待心跳检测
    print("  等待心跳...")
    rt.hb.wait_for_wakeup(timeout=3)

    round_type = rt._determine_round_type(rt.sm.get_flags())
    if round_type:
        print(f"  轮类型: {round_type}")
        state = rt.sm.load()
        flags = rt.sm.get_flags()
        rt._run_one_round(round_type, state, flags)
        print("  一轮完成")
    else:
        print("  无待处理的 flag")

    rt.hb.stop()


def run_interactive(sm, cfg):
    """交互模式：stdin 监听 + 主循环"""
    import threading
    print("[UPSP] 进入交互模式（输入消息回车发送，Ctrl+C 退出）")
    print("[UPSP] ───────────────────────────────")
    rt = build_runtime(sm, cfg)
    rt.hb.start()

    # stdout 锁，防打印错乱
    print_lock = threading.Lock()

    # 每轮结束后打印回复
    def on_round_complete(round_num, response_text, is_interactive):
        if is_interactive and response_text:
            with print_lock:
                label = public_identity().get("abbreviation") or "UPSP"
                print(f"\n[{label}] {response_text[:800]}")
                print("\n[UPSP] ───────────────────────────────")

    rt.on_round_complete = on_round_complete

    # stdin 监听线程
    def stdin_reader():
        try:
            for line in sys.stdin:
                line = line.strip()
                if line:
                    rt.hb.enqueue_message(line)
        except (EOFError, OSError):
            pass

    stdin_thread = threading.Thread(
        target=stdin_reader, name="stdin", daemon=True)
    stdin_thread.start()

    try:
        rt.run_forever()
    except KeyboardInterrupt:
        print("\n[UPSP] 收到退出信号")
    finally:
        rt.hb.stop()
        print("[UPSP] 心跳已停止")


def send_message_once(
        sm, cfg, message, wait_timeout=3, *, context_profile="full"):
    """Send one message through Runtime and return a structured summary."""
    context_profile = normalize_context_profile(context_profile)
    rt = build_runtime(sm, cfg, context_profile=context_profile)
    result = {
        "status": "pending",
        "context_profile": context_profile,
        "round_type": None,
        "round_num": None,
        "round_file": None,
        "final_response": "",
        "is_interactive": False,
        "active_flags": [],
        "guide_queue": [],
        "coalesced": False,
        "deferred_items": [],
    }

    def on_round_complete(round_num, response_text, is_interactive):
        result["round_num"] = round_num
        result["final_response"] = str(response_text or "")
        result["is_interactive"] = bool(is_interactive)
        try:
            from paths import STM_CTX_ROUND_DIR
            result["round_file"] = os.path.join(
                STM_CTX_ROUND_DIR, f"round_{round_num}.jsonl"
            )
        except Exception:
            result["round_file"] = f"round_{round_num}.jsonl"

    rt.on_round_complete = on_round_complete
    probe_enabled = single_round_probe_enabled()

    try:
        rt.hb.enqueue_message(message)
        rt.hb.wait_for_wakeup(timeout=wait_timeout)

        flags = rt.sm.get_flags()
        flags, probe_policy = isolate_single_round_probe_flags(rt.sm, flags)
        if probe_policy.get("enabled"):
            result["probe_policy"] = probe_policy
        if probe_policy.get("status") == "rejected":
            result["status"] = "probe_rejected"
            result["active_flags"] = list(probe_policy.get("active_flags") or [])
            return result
        result["active_flags"] = [
            name for name, value in (flags or {}).items() if value
        ]
        decision = _runtime_round_decision(rt, flags)
        _stamp_round_decision(result, decision)
        round_type = result["round_type"]
        if round_type:
            state = rt.sm.load()
            if probe_policy.get("enabled"):
                rt._run_one_round(
                    round_type,
                    state,
                    flags,
                    probe_policy=probe_policy,
                )
            else:
                rt._run_one_round(round_type, state, flags)
            result["status"] = "round_completed"
        else:
            result["status"] = "no_round"
        return result
    finally:
        rt.hb.stop()


def run_pending_once(
        sm, cfg, *, required_round_type=None, context_profile="full"):
    """Run one already-pending heartbeat round without enqueuing a user message."""
    context_profile = normalize_context_profile(context_profile)
    rt = build_runtime(sm, cfg, context_profile=context_profile)
    result = {
        "status": "pending",
        "context_profile": context_profile,
        "round_type": None,
        "round_num": None,
        "round_file": None,
        "final_response": "",
        "is_interactive": False,
        "active_flags": [],
        "guide_queue": [],
        "coalesced": False,
        "deferred_items": [],
    }

    def on_round_complete(round_num, response_text, is_interactive):
        result["round_num"] = round_num
        result["final_response"] = str(response_text or "")
        result["is_interactive"] = bool(is_interactive)
        try:
            from paths import STM_CTX_ROUND_DIR
            result["round_file"] = os.path.join(
                STM_CTX_ROUND_DIR, f"round_{round_num}.jsonl"
            )
        except Exception:
            result["round_file"] = f"round_{round_num}.jsonl"

    rt.on_round_complete = on_round_complete

    try:
        flags = rt.sm.get_flags()
        result["active_flags"] = [
            name for name, value in (flags or {}).items() if value
        ]
        decision = _runtime_round_decision(rt, flags)
        _stamp_round_decision(result, decision)
        round_type = result["round_type"]
        if not round_type:
            result["status"] = "no_round"
            return result
        if required_round_type and round_type != required_round_type:
            result["status"] = "wrong_round_type"
            return result
        state = rt.sm.load()
        rt._run_one_round(round_type, state, flags)
        result["status"] = "round_completed"
        return result
    finally:
        rt.hb.stop()


def run_message(sm, cfg, message):
    """发送一条消息并等待回复"""
    print(f"[UPSP] 发送: {message}")
    send_message_once(sm, cfg, message)
    print("[UPSP] 完成")


# ============================================================
# 入口
# ============================================================

if __name__ == "__main__":
    sm, cfg = init_environment()

    args = sys.argv[1:]

    if "--once" in args:
        run_once(sm, cfg)
    elif "--pending" in args:
        run_pending(sm)
    elif "--message" in args:
        idx = args.index("--message")
        msg = args[idx + 1] if idx + 1 < len(args) else "自检消息"
        run_message(sm, cfg, msg)
    elif "--interactive" in args or "-i" in args:
        run_interactive(sm, cfg)
    else:
        print("[UPSP] 用法: python OS/main.py [选项]")
        print("  --interactive / -i   交互模式（终端对话）")
        print("  --message '...'      发送一条消息")
        print("  --once               单轮自检")
        print("  --pending            read heartbeat flags")
        state = sm.load()
        print(f"\n  当前状态: R{state['base']['meta']['total_round']} | v{state['base']['meta']['version']}")
        print("  模型配置: 检查 LocalAppData\\UPSP\\config\\models.json 与当前位格 OS\\config\\model_routing.json")
