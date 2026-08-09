"""Setup step runner."""
from engines.round_context import FrameRef, SetupResult
from engines.runtime_services import EngineComponent
from engines.heartbeat import HEARTBEAT_QUALIFIER_FLAGS, HEARTBEAT_TRIGGER_GROUPS
from logic.native_tool_calls import terminal_finalize_from_envelopes
from logic.handoff_prefixes import (
    RELAY_REACTION_EXECUTION_PREFIX,
    ensure_handoff_prefix,
)
from logic.interaction_meta import (
    active_relation_card,
    cache_interaction_meta,
    interaction_meta_for_card,
    interaction_meta_from_anchor,
)
from logic.relay_intent_pool import (
    mark_relay_handoff_projected,
    open_relay_intents,
)


class SetupRunner(EngineComponent):
    def run(self, context):
        self._clear_stale_call_transients(context.round_num)
        user_msgs = list(context.trigger.messages) if context.trigger else []
        user_input_text = "\n".join(user_msgs) if user_msgs else ""
        if (
                context.flags.get("user_message_waiting")
                and context.round_type in {"interactive", "rhythm"}
                and not user_msgs):
            self._round_audit_settlement(
                context.round_num,
                "setup",
                0,
                {
                    "warning": "user_message_waiting_queue_empty",
                    "round_type": context.round_type,
                    "preserve_flag": "user_message_waiting",
                },
            )
        interaction_meta = self._resolve_interaction_meta(state=context.state)
        if str(interaction_meta.get("identity_status") or "") in {
                "", "unknown", "timeout"}:
            context_meta = dict(context.interaction_meta or {})
            if str(context_meta.get("identity_status") or "") not in {
                    "", "unknown", "timeout"}:
                interaction_meta = context_meta
        if (
                context.round_type == "relay"
                and str(interaction_meta.get("identity_status") or "") in {
                    "", "unknown", "timeout"}
        ):
            inherited_meta = self._relay_inherited_interaction_meta(context.flags)
            if inherited_meta:
                interaction_meta = inherited_meta

        heartbeat_handoff = self._heartbeat_handoff_entries(
            context.round_type,
            context.flags,
        )
        self._write_interaction_input(
            context.round_num,
            user_input_text,
            interaction_meta,
        )
        self._write_heartbeat_handoff(
            context.round_num,
            context.round_type,
            context.flags,
        )
        self._write_relay_handoff_inputs(
            context.round_num,
            context.round_type,
            context.state,
        )
        trigger_id = context.trigger.trigger_id if context.trigger else ""
        def setup_frame_ref(sequence):
            return FrameRef.for_axis(
                context.round_num, "setup", sequence,
                trigger_id=trigger_id,
                caused_by=(
                    trigger_id if sequence == 1
                    else f"R{context.round_num:06d}:setup:{sequence - 1}"
                ),
                topology_version=context.topology_version, role_id="setup")

        first_frame_ref = setup_frame_ref(1)
        organ_runtime = getattr(self, "organ_runtime", None)
        material_inputs = (
            organ_runtime.begin_frame_materials(first_frame_ref)
            if organ_runtime is not None else ()
        )
        assemble_kwargs = {
            "internal_handoff": heartbeat_handoff,
            "interaction_meta": interaction_meta,
        }
        if material_inputs:
            assemble_kwargs["material_inputs"] = material_inputs

        def apply_permission_boundary(iteration):
            callback = getattr(self, "permission_boundary_callback", None)
            if not callable(callback):
                return None
            applied = callback(context.round_num, "setup", iteration)
            if isinstance(applied, dict):
                current = applied.get("current") or {}
                level = str(current.get("permission_level") or "").strip()
                if level:
                    context.execution_permission_level = level
            return applied

        apply_permission_boundary(1)
        system, messages = self.assembler.assemble_setup(
            context.state,
            context.round_type,
            user_msgs,
            **assemble_kwargs,
        )
        result = self._call_llm_with_round_audit(
            "setup",
            system,
            messages,
            context.round_num,
            iteration=1,
        )
        result["_setup_messages"] = messages
        result["_interaction_meta"] = interaction_meta
        intent = self._parse_setup_intent(result, context.round_type)
        iteration = 1
        retry_feedbacks = []
        while self._needs_setup_finalize_retry(intent) and iteration < 3:
            self._round_audit_parsed(context.round_num, "setup", iteration, intent)
            retry_settlement = self._setup_retry_settlement(
                intent,
                interaction_meta,
                retry_attempt=iteration,
            )
            retry_settlement["frame_ref"] = setup_frame_ref(iteration).as_dict()
            self._round_audit_settlement(
                context.round_num,
                "setup",
                iteration,
                retry_settlement,
            )
            messages = self._setup_finalize_retry_messages(
                messages,
                intent,
                retry_attempt=iteration,
            )
            retry_feedbacks.append(messages[-1])
            try:
                next_iteration = iteration + 1
                if apply_permission_boundary(next_iteration):
                    system, messages = self.assembler.assemble_setup(
                        context.state,
                        context.round_type,
                        user_msgs,
                        **assemble_kwargs,
                    )
                    messages = list(messages) + list(retry_feedbacks)
                result = self._call_llm_with_round_audit(
                    "setup",
                    system,
                    messages,
                    context.round_num,
                    iteration=next_iteration,
                )
            except Exception as exc:
                interrupted = self._setup_retry_interrupted_settlement(
                    intent,
                    interaction_meta,
                    exc,
                    retry_attempt=iteration,
                )
                interrupted["frame_ref"] = setup_frame_ref(
                    iteration + 1).as_dict()
                self._round_audit_settlement(
                    context.round_num,
                    "setup",
                    iteration + 1,
                    interrupted,
                )
                raise
            result["_setup_messages"] = messages
            result["_interaction_meta"] = interaction_meta
            intent = self._parse_setup_intent(result, context.round_type)
            iteration = next_iteration
        if self._needs_setup_finalize_retry(intent):
            intent = self._setup_retry_exhausted_intent(intent)
        self._round_audit_parsed(context.round_num, "setup", iteration, intent)
        interaction_meta = self._apply_setup_interaction_meta(interaction_meta, intent)
        intent["interaction_meta"] = interaction_meta
        result["_interaction_meta"] = interaction_meta
        frame_ref = setup_frame_ref(iteration)
        final_settlement = {
            "intent": intent,
            "interaction_meta": interaction_meta,
            "frame_ref": frame_ref.as_dict(),
        }
        if (
                isinstance(intent, dict)
                and intent.get("reject_reason") == "setup_finalize_missing_after_retry"):
            final_settlement["retry_exhausted"] = "setup_finalize_missing_after_retry"
        self._round_audit_settlement(
            context.round_num,
            "setup",
            iteration,
            final_settlement,
        )
        self._update_token_usage(result)
        setup_facts = self._setup_fact_from_intent(context.round_type, intent)
        return SetupResult(
            raw_result=result,
            intent=intent,
            interaction_meta=interaction_meta,
            user_input_text=user_input_text,
            setup_messages=messages,
            internal_handoff=setup_facts,
            frame_ref=frame_ref,
        )

    def commit(self, context, setup_result):
        self._write_setup_facts(
            context.round_num,
            setup_result.internal_handoff,
        )
        if getattr(self, "assembler", None) is not None:
            self.assembler._current_interaction_meta = dict(
                setup_result.interaction_meta or {})
        if str((setup_result.interaction_meta or {}).get(
                "identity_status") or "") in {"declared", "known"}:
            try:
                self.sm.confirm_identity()
            except Exception:
                pass
        meta = setup_result.interaction_meta or {}
        source = str(meta.get("interaction_source") or "").strip()
        card_id = str(meta.get("interaction_object_id") or "").strip()
        if card_id:
            anchor_source = (
                source
                if source in {
                    "local_default",
                    "instance_selection",
                    "self_declaration",
                    "relation_card_created",
                }
                else "instance_selection"
            )
            self.sm.set_interaction_anchor(
                relation_id=card_id,
                source=anchor_source,
            )
        elif (
                source == "self_declaration"
                and str(meta.get("identity_status") or "") == "unregistered"
        ):
            self.sm.set_interaction_anchor(
                declared_name=meta.get("interaction_object"),
                source="self_declaration")
        return setup_result

    def _parse_setup_intent(self, result, round_type):
            envelopes = result.get("tool_call_envelopes", [])
            if "tool_call_envelopes" not in result:
                result["_step_terminal_invalids"] = []
                return self._missing_setup_finalize_intent([])
            intent, _ordinary_envelopes, terminal_invalids = terminal_finalize_from_envelopes(
                envelopes,
                "setup",
            )
            result["_step_terminal_invalids"] = terminal_invalids
            if terminal_invalids:
                intent = self._missing_setup_finalize_intent(terminal_invalids)
            elif intent is None:
                intent = self._missing_setup_finalize_intent(terminal_invalids)
            return intent

    @staticmethod
    def _needs_setup_finalize_retry(intent):
            if not isinstance(intent, dict):
                return False
            return intent.get("reject_reason") == "setup_finalize_missing_or_invalid"

    @staticmethod
    def _setup_retry_settlement(intent, interaction_meta, retry_attempt=1):
            retry_attempt = SetupRunner._setup_retry_attempt(retry_attempt)
            settlement = {
                "intent": intent,
                "interaction_meta": interaction_meta,
                "retry_requested": "setup_finalize_missing_or_invalid",
                "retry_attempt": retry_attempt,
                "retry_severity": SetupRunner._setup_retry_severity(retry_attempt),
                "invalid_tool_ids": SetupRunner._setup_invalid_tool_ids(intent),
                "invalid_reasons": SetupRunner._setup_invalid_reasons(intent),
            }
            violation_reason = SetupRunner._setup_terminal_violation_reason(intent)
            if violation_reason:
                settlement["setup_terminal_violation"] = True
                settlement["violation_reason"] = violation_reason
            return settlement

    @staticmethod
    def _setup_retry_interrupted_settlement(
            intent,
            interaction_meta,
            exc,
            retry_attempt=1):
            settlement = SetupRunner._setup_retry_settlement(
                intent,
                interaction_meta,
                retry_attempt=retry_attempt,
            )
            settlement["setup_retry_interrupted"] = True
            settlement["error"] = str(exc)
            return settlement

    @staticmethod
    def _setup_terminal_violation_reason(intent):
            if not isinstance(intent, dict):
                return ""
            for invalid in intent.get("terminal_invalids", []) or []:
                if not isinstance(invalid, dict):
                    continue
                reason = str(invalid.get("reason") or "").strip()
                if reason:
                    return reason
            return ""

    @staticmethod
    def _setup_retry_attempt(value):
            try:
                attempt = int(value)
            except (TypeError, ValueError):
                attempt = 1
            return max(1, attempt)

    @staticmethod
    def _setup_retry_severity(retry_attempt):
            return "warning" if SetupRunner._setup_retry_attempt(retry_attempt) >= 2 else "reminder"

    @staticmethod
    def _setup_invalid_tool_ids(intent):
            tool_ids = []
            if isinstance(intent, dict):
                for invalid in intent.get("terminal_invalids", []) or []:
                    if not isinstance(invalid, dict):
                        continue
                    tool_id = str(invalid.get("tool_id") or "").strip()
                    if tool_id and tool_id not in tool_ids:
                        tool_ids.append(tool_id)
            return tool_ids

    @staticmethod
    def _setup_invalid_reasons(intent):
            reasons = []
            if isinstance(intent, dict):
                for invalid in intent.get("terminal_invalids", []) or []:
                    if not isinstance(invalid, dict):
                        continue
                    reason = str(invalid.get("reason") or "").strip()
                    if reason and reason not in reasons:
                        reasons.append(reason)
            return reasons

    @staticmethod
    def _setup_invalid_summary(intent):
            tool_ids = SetupRunner._setup_invalid_tool_ids(intent)
            reasons = SetupRunner._setup_invalid_reasons(intent)
            parts = []
            if tool_ids:
                parts.append("非法工具：" + "、".join(tool_ids))
            else:
                parts.append("缺少 provider-native setup_finalize")
            if reasons:
                parts.append("原因：" + "、".join(reasons))
            return "；".join(parts)

    @staticmethod
    def _setup_finalize_retry_messages(messages, intent=None, retry_attempt=1):
            retry_attempt = SetupRunner._setup_retry_attempt(retry_attempt)
            severity = SetupRunner._setup_retry_severity(retry_attempt)
            title = "WARNING｜警告" if severity == "warning" else "REMINDER｜提醒"
            invalid_summary = SetupRunner._setup_invalid_summary(intent)
            warning_line = ""
            if severity == "warning":
                warning_line = (
                    "这是第二次无效 setup 输出；下一次仍不用合法 setup_finalize，"
                    "Runtime 将以 setup_finalize_missing_after_retry 关闭本轮并跳过反应步。"
                )
            retry = {
                "role": "user",
                "content": (
                    f"## {title}\n"
                    f"setup 第{retry_attempt}次输出无效：{invalid_summary}。\n"
                    "起手步不执行用户任务；不得读取材料、创建任务账本、写产物或运行命令。\n"
                    "唯一合法动作：只能调用 setup_finalize 完成起手步终端确认；"
                    "需要读取材料、建任务清单或执行用户任务时，结束 setup 后交给反应步处理。\n"
                    "上一轮 setup 输出缺少 provider-native setup_finalize 或混入了非 setup 工具。"
                    "上一轮裸文本只进 audit，不作为事实、交接或执行证据；"
                    "不要用普通文本替代，不要声称已读、已执行。"
                    + (f"\n{warning_line}" if warning_line else "")
                ),
            }
            return list(messages or []) + [retry]

    @staticmethod
    def _missing_setup_finalize_intent(terminal_invalids=None):
            return {
                "mount_requests": [],
                "rules_selection": None,
                "round_type_confirm": None,
                "security_verdict": "reject",
                "reject_reason": "setup_finalize_missing_or_invalid",
                "suggested_mode": None,
                "interaction_meta": None,
                "standby_skip_reaction": False,
                "terminal_invalids": list(terminal_invalids or []),
            }

    @staticmethod
    def _setup_retry_exhausted_intent(intent):
            updated = dict(intent or {})
            updated["security_verdict"] = "reject"
            updated["reject_reason"] = "setup_finalize_missing_after_retry"
            updated["retry_exhausted"] = "setup_finalize_missing_after_retry"
            return updated

    def _setup_fact_from_intent(self, round_type, intent):
            if not isinstance(intent, dict):
                return []
            if intent.get("security_verdict") == "reject":
                return []
            verdict = intent.get("security_verdict") or "pass"
            verdict_text = "通过" if verdict == "pass" else str(verdict)
            lines = [
                f"起手安全裁决：{verdict_text}。",
                f"本轮类型：{self._round_type_label(round_type)}。",
            ]
            round_confirm = str(intent.get("round_type_confirm") or "").strip()
            if round_confirm:
                lines.append(f"起手确认轮型：{self._round_type_label(round_confirm)}。")
            if isinstance(intent.get("standby_skip_reaction"), bool):
                lines.append(
                    "待命跳过反应步：" +
                    ("是。" if intent.get("standby_skip_reaction") else "否。")
                )
            mount_lines = self._setup_fact_mount_lines(intent.get("mount_requests") or [])
            if mount_lines:
                lines.extend(mount_lines)
            interaction_meta = intent.get("interaction_meta")
            if isinstance(interaction_meta, dict):
                interaction_object = str(
                    interaction_meta.get("interaction_object") or "unknown").strip()
                identity_status = str(
                    interaction_meta.get("identity_status") or "unknown").strip()
                interaction_source = str(
                    interaction_meta.get("interaction_source") or "unresolved").strip()
                lines.append(
                    "交互对象是"
                    f"{interaction_object}，身份{self._identity_status_label(identity_status)}，"
                    f"来源为{self._interaction_source_label(interaction_source)}。"
                )
            return [{
                "role": "system",
                "kind": "setup_fact",
                "content": "\n".join(lines),
                "interaction_object": "system",
                "identity_status": "system",
                "interaction_source": "setup_finalize",
                "handoff_target": "reaction",
            }]

    @staticmethod
    def _setup_fact_mount_lines(mount_requests):
            lines = []
            for item in mount_requests:
                if not isinstance(item, dict):
                    continue
                mount_type = str(item.get("type") or "").strip()
                ids = str(item.get("ids") or "").strip()
                if mount_type and ids:
                    lines.append(f"起手挂载请求：{mount_type}:{ids}。")
            return lines

    @staticmethod
    def _round_type_label(value):
            text = str(value or "").strip()
            return {
                "interactive": "交互轮",
                "rhythm": "节律轮",
                "relay": "中继轮",
                "autonomous": "自主轮",
                "standby": "待命轮",
            }.get(text, text or "未知轮型")

    @staticmethod
    def _identity_status_label(value):
            text = str(value or "").strip()
            return {
                "declared": "已声明",
                "confirmed": "已确认",
                "known": "已确认",
                "unknown": "未知",
                "system": "系统",
            }.get(text, text or "未知")

    @staticmethod
    def _interaction_source_label(value):
            text = str(value or "").strip()
            return {
                "self_declaration": "对象自述",
                "current_user_message": "本轮输入",
                "setup_finalize": "起手收束",
                "heartbeat": "心跳",
                "system": "系统",
                "unresolved": "未解析",
            }.get(text, text or "未解析")

    def _heartbeat_handoff_entries(self, round_type, flags):
            entry = self._build_heartbeat_handoff_entry(round_type, flags)
            return [entry] if entry else []

    def _build_heartbeat_handoff_entry(self, round_type, flags):
            flags = flags or {}
            group_by_round_type = {
                "interactive": "interaction",
                "rhythm": "rhythm",
                "relay": "relay",
                "autonomous": "autonomous",
                "standby": "standby",
            }
            group = group_by_round_type.get(round_type)
            trigger_flags = [
                flag for flag in HEARTBEAT_TRIGGER_GROUPS.get(group, ())
                if flags.get(flag)
            ]
            qualifier_flags = [
                flag for flag in HEARTBEAT_QUALIFIER_FLAGS
                if flags.get(flag)
            ]
            if not trigger_flags and not qualifier_flags:
                return None
            trigger_text = "、".join(trigger_flags) if trigger_flags else "无"
            parts = [
                f"本轮类型为 {self._round_type_label(round_type)}",
                f"触发 flag 为 {trigger_text}",
            ]
            if qualifier_flags:
                parts.append(f"提示 flag 为 {'、'.join(qualifier_flags)}")
            coalesced_flags = []
            if group != "interaction" and flags.get("user_message_waiting"):
                coalesced_flags.append("user_message_waiting")
            if group != "relay" and flags.get("continue_requested"):
                coalesced_flags.append("continue_requested")
            if coalesced_flags:
                parts.append(
                    "合轮待处理 flag 为 " + "、".join(coalesced_flags)
                )
            content = "心跳触发本轮：" + "；".join(parts) + "。"
            if round_type == "relay" and flags.get("continue_requested"):
                content = ensure_handoff_prefix(
                    content,
                    RELAY_REACTION_EXECUTION_PREFIX,
                )
            return {
                "role": "system",
                "kind": "setup_fact",
                "content": content,
                "interaction_object": "system",
                "identity_status": "system",
                "interaction_source": "heartbeat",
            }

    def _write_heartbeat_handoff(self, round_num, round_type, flags):
            entry = self._build_heartbeat_handoff_entry(round_type, flags)
            if not entry:
                return
            try:
                self.ctx_store.append_to_cache(
                    round_num,
                    entry["role"],
                    entry["content"],
                    kind=entry.get("kind") or "setup_fact",
                    step="setup",
                    interaction_object=entry["interaction_object"],
                    identity_status=entry["identity_status"],
                    interaction_source=entry["interaction_source"],
                )
            except Exception:
                try:
                    self.sm.set("base.meta.last_error", "心跳交接写入失败")
                except Exception:
                    pass

    def _write_relay_handoff_inputs(self, round_num, round_type, state):
            if str(round_type or "").strip().lower() != "relay":
                return
            try:
                intents = open_relay_intents(state)
            except Exception:
                intents = []
            for intent in intents:
                if not isinstance(intent, dict):
                    continue
                if intent.get("handoff_projected_round"):
                    continue
                content = str(intent.get("handoff_text") or "").strip()
                if not content:
                    continue
                try:
                    source_round = int(intent.get("source_round") or round_num or 0)
                except (TypeError, ValueError):
                    source_round = int(round_num or 0)
                try:
                    self.ctx_store.append_to_cache(
                        source_round,
                        "user",
                        content,
                        kind="relay_handoff",
                        step="setup",
                        interaction_object="runtime",
                        identity_status="system",
                        interaction_source="relay_intent",
                    )
                    mark_relay_handoff_projected(
                        self.sm,
                        intent.get("relay_intent_id"),
                        round_num=round_num,
                    )
                except Exception:
                    try:
                        self.sm.set("base.meta.last_error", "relay_handoff 写入失败")
                    except Exception:
                        pass

    def _write_interaction_input(self, round_num, user_input_text, interaction_meta):
            content = str(user_input_text or "").strip()
            if not content:
                return
            try:
                self.ctx_store.append_to_cache(
                    round_num,
                    "user",
                    content,
                    kind="interaction",
                    step="setup",
                    **cache_interaction_meta(interaction_meta),
                )
            except Exception:
                try:
                    self.sm.set("base.meta.last_error", "本轮交互写入失败")
                except Exception:
                    pass

    def _write_setup_facts(self, round_num, entries):
            for entry in entries or []:
                if not isinstance(entry, dict):
                    continue
                content = str(entry.get("content") or "").strip()
                if not content:
                    continue
                try:
                    self.ctx_store.append_to_cache(
                        round_num,
                        entry.get("role") or "system",
                        content,
                        kind="setup_fact",
                        step="setup",
                        interaction_object=entry.get("interaction_object", "system"),
                        identity_status=entry.get("identity_status", "system"),
                        interaction_source=entry.get("interaction_source", "setup_finalize"),
                    )
                except Exception:
                    try:
                        self.sm.set("base.meta.last_error", "setup_fact 写入失败")
                    except Exception:
                        pass

    def _clear_stale_call_transients(self, round_num):
        clearer = getattr(self.ctx_store, "clear_stale_call_transients", None)
        if not callable(clearer):
            return None
        try:
            return clearer(round_num)
        except Exception:
            return None

    def _resolve_interaction_meta(self, state=None):
            """只读取 Runtime 真源；自然语言身份判断属于 setup LLM。"""
            try:
                from data.relation_store import RelationStore
                relation_store = getattr(self.services, "relation_store", None) or RelationStore()
                anchored = interaction_meta_from_anchor(state, relation_store)
                if anchored:
                    return anchored
                recent_meta = self._recent_confirmed_interaction_meta()
                if recent_meta:
                    return recent_meta
                return {
                    "interaction_object": "unknown",
                    "identity_status": "unknown",
                    "interaction_source": "unresolved",
                }
            except Exception:
                return {
                    "interaction_object": "unknown",
                    "identity_status": "unknown",
                    "interaction_source": "unresolved",
                }

    def _relay_inherited_interaction_meta(self, flags=None):
            recent_meta = self._recent_confirmed_interaction_meta()
            if not recent_meta:
                return None
            return recent_meta

    def _recent_confirmed_interaction_meta(self):
            entry_groups = []
            try:
                getter = getattr(self.ctx_store, "get_now_entries", None)
                if callable(getter):
                    entry_groups.append(getter() or [])
            except Exception:
                pass
            try:
                getter = getattr(self.ctx_store, "get_lately_entries", None)
                if callable(getter):
                    entry_groups.append(getter("setup") or [])
            except Exception:
                pass
            for entries in entry_groups:
                for entry in reversed(entries):
                    if not isinstance(entry, dict):
                        continue
                    kind = str(entry.get("kind") or "").strip()
                    if kind not in {
                        "interaction",
                        "assistant_reply",
                        "minimum_commitment",
                    }:
                        continue
                    interaction_object = str(
                        entry.get("interaction_object") or "").strip()
                    identity_status = str(
                        entry.get("identity_status") or "").strip()
                    interaction_source = str(
                        entry.get("interaction_source") or "").strip()
                    if (
                        interaction_object
                        and interaction_object.lower() != "unknown"
                        and identity_status == "known"
                        and interaction_source not in {
                            "system",
                            "heartbeat",
                            "no_external_input",
                            "self_declaration",
                        }
                    ):
                        return {
                            "interaction_object": interaction_object,
                            "identity_status": identity_status,
                            "interaction_source": interaction_source or "recent_context",
                        }
            return None

    def _apply_setup_interaction_meta(self, current_meta, intent):
            current = dict(current_meta or {})
            incoming = (intent or {}).get("interaction_meta")
            if not isinstance(incoming, dict):
                return current

            incoming_object = str(incoming.get("interaction_object") or "").strip()
            incoming_status = str(incoming.get("identity_status") or "").strip()
            if (
                not incoming_object
                or incoming_object.lower() == "unknown"
                or incoming_status not in {"known", "declared"}
            ):
                return current

            try:
                from data.relation_store import RelationStore
                relation_store = (
                    getattr(self.services, "relation_store", None)
                    or RelationStore()
                )
                card = active_relation_card(relation_store, incoming_object)
            except Exception:
                card = None

            if incoming_status == "declared":
                if card:
                    updated = interaction_meta_for_card(
                        card, "self_declaration")
                    updated["identity_status"] = "declared"
                else:
                    updated = {
                        "interaction_object": incoming_object,
                        "identity_status": "unregistered",
                        "interaction_source": "self_declaration",
                    }
                basis = str(incoming.get("basis") or "").strip()
                if basis:
                    updated["basis"] = basis
                return updated

            current_status = str(current.get("identity_status") or "").strip()
            current_object = str(current.get("interaction_object") or "").strip()
            current_unknown = (
                not current_object
                or current_object.lower() == "unknown"
                or current_status in {"", "unknown", "timeout", "unregistered"}
            )
            if not current_unknown or not card:
                return current

            updated = interaction_meta_for_card(
                card,
                str(incoming.get("interaction_source") or "setup_finalize").strip(),
            )
            basis = str(incoming.get("basis") or "").strip()
            if basis:
                updated["basis"] = basis
            return updated
