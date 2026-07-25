"""Task-guidance debt helpers extracted from the Runtime orchestrator."""

from logic.task_guide import append_task_pending_input, create_task_bootstrap_guide
from logic.work_intent_debt import create_work_intent_debt, current_work_intent_debt


def prepare_task_bootstrap_guide(runtime, intent, context=None):
    if not isinstance(intent, dict):
        return None
    if intent.get("task_guidance_required") is not True:
        return None
    if not runtime._task_guidance_has_real_user_input(context):
        return None
    try:
        active_task = str(runtime.workbench.get("base.active_task") or "").strip()
    except Exception:
        active_task = ""
    route = str(intent.get("task_guidance_route") or "").strip()
    if route not in {"none", "new_work", "current_work"}:
        route = "current_work" if active_task else "new_work"
    if route == "none":
        route = "current_work" if active_task else "new_work"
    round_num = int(getattr(context, "round_num", 0) or 0)
    if active_task:
        return append_task_pending_input(
            runtime.workbench,
            active_task,
            source_refs=[f"round:{round_num}:interaction"],
            summary=str(intent.get("task_guidance_reason") or "").strip(),
            input_kind="interaction",
            round_num=round_num,
            task_guidance_route=route,
        )
    if runtime._has_active_workbench_work_or_task():
        return None
    return create_task_bootstrap_guide(
        runtime.workbench,
        reason=str(intent.get("task_guidance_reason") or "").strip(),
        source_refs=[f"round:{round_num}:interaction"],
    )


def record_work_intent_debt_if_needed(runtime, context, setup_result):
    if not runtime._task_guidance_has_real_user_input(context):
        return None
    if runtime._has_active_workbench_work_or_task():
        return None
    intent = getattr(setup_result, "intent", None)
    intent = intent if isinstance(intent, dict) else {}
    grant = runtime._engineering_task_grant()
    task_phase = str(grant.get("phase") or "").strip() if grant else ""
    task_root = str(grant.get("task_root") or "").strip() if grant else ""
    intent_required = intent.get("task_guidance_required") is True
    phase_required = task_phase == "agent_eval"
    if not (intent_required or phase_required):
        return None
    reason = str(intent.get("task_guidance_reason") or "").strip()
    if not reason:
        reason = (
            "agent_eval 任务必须先建立任务指南清单"
            if phase_required else
            "用户请求需要先建立任务指南清单"
        )
    round_num = int(getattr(context, "round_num", 0) or 0)
    source_refs = [f"round:{round_num}:interaction"]
    if task_root:
        source_refs.append(f"task_root:{task_root}")
    return create_work_intent_debt(
        runtime.sm,
        round_num=round_num,
        reason=reason,
        source=(
            "engineering_task_phase"
            if phase_required and not intent_required else
            "setup_finalize"
        ),
        source_refs=source_refs,
        task_phase=task_phase,
        task_root=task_root,
    )


def materialize_work_intent_debt_if_needed(runtime, context):
    debt = current_work_intent_debt(runtime.sm)
    if not debt:
        return None
    if runtime._has_active_workbench_work_or_task():
        return None
    guide = create_task_bootstrap_guide(
        runtime.workbench,
        reason=debt.get("reason") or "当前工作需要先建立任务指南清单",
        source_refs=debt.get("source_refs") or [],
    )
    try:
        runtime.audit.get_store().append_event(
            getattr(context, "round_num", 0),
            "work_intent_debt_materialized",
            {
                "tool_id": "runtime_work_intent_debt",
                "tool_family": "substrate_tool",
                "tool_class": "sync_tool",
                "status": "task_bootstrap_created",
                "source": debt.get("source") or "runtime",
                "guide_id": guide.get("guide_id"),
                "source_refs": debt.get("source_refs") or [],
            },
            phase="reaction",
        )
    except Exception:
        pass
    return guide
