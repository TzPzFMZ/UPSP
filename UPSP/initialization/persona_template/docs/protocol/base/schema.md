# 结构字典

> 职能：本文件只描述结构字段和账本形状，不承载操作指南长文。工具纪律归 native schema，POPUP 文案归 `popup.md`。

## POPUP envelope

| 字段 | 类型 | 说明 |
|---|---|---|
| `kind` | string | POPUP 片段类型，如 `reaction_step_guide`、`memory_settlement_reminder`、`native_tool_result`。旧 `received_handoff` 已退役，不再渲染。 |
| `tier` | enum | `guide` / `reminder` / `warning`。 |
| `decision_required` | bool | 是否要求本步显式裁决。 |
| `message` | string | 给当前 LLM 的可见提示正文。 |
| `source` | string | 可选，片段来源。 |

可见 POPUP 渲染顺序固定为“指南 -> 提醒 -> 警告”。机器字段如 `call_id`、`field`、`expected`、`actual`、`provider_item_id` 保留在 Runtime fragment、native projection 和 round audit 中，不作为可见正文逐项展示。

## reaction GUIDE / REMINDER 层

| kind | 说明 |
|---|---|
| `reaction_step_guide` | reaction 常驻主卡，正文模板来自 `popup.md`。 |
| active tool hint | 当前轮需要展示的短工具提示，不是 guide 门禁。 |
| `memory_settlement_reminder` | 每次 reaction provider 请求恰好一张的固定精简提醒卡；提示识别主体更新并主动考虑 `memory_write`，同时说明 material/最近缓存承载正文、`dialogue_progress` 不是私有笔记、`MEM-*` 回执才算真实写入；不根据本轮证据动态生成行为树。 |

## CONTENT / WB focus

| 结构 | 说明 |
|---|---|
| `CONTENT` | 当前迭代可阅读和改写参考材料面。read tool 结果、记忆正文、关系正文、容器正文可投到这里。 |
| `WB focus` | 工作台焦点投影，展示当前焦点容器的元数据、可写目标和正文片段。 |
| `kind=setup_fact` | 起手步或心跳触发说明的自然语言短事实，不等于 POPUP，不承载自由文本暗层；与起手包留在 now，首个成功 Reaction 后进入 lately/Corpus。 |
| `runtime_call_request` | 每次 provider 调用固定占位，文本为“请根据上下文继续本次调用。”；只出现在实际调用上下文，不写 cache。 |
| `relay_handoff` | `reaction_finalize.handoff_text` 形成的跨轮交接语料，role=user，但标题声明不是用户原始输入。 |
| `relay_intents[]` | Runtime 中继意图池，承载 `reaction_finalize.handoff_text` 调度 payload；模型可见层同时有 `relay_handoff` 和目标卡/意图指针。 |

已有内容改写必须先通过 CONTENT / WB focus 看见材料。POPUP 不承载待改正文。

## provider-native tool envelope

provider-native tool call 是当前 reaction 的 LLM-facing 工具入口。

| 字段 | 类型 | 说明 |
|---|---|---|
| `tool_id` | string | provider-native 工具名。 |
| `arguments` | object | 工具参数，按 native schema 校验。 |
| `call_id` | string | provider 调用 ID。 |
| `provider` | string | provider 标识。 |
| `response_id` | string | provider response ID。 |
| `provider_item_id` | string | provider item ID。 |
| `index` | int | 同一 response 内顺序。 |
| `tool_family` | enum | `protocol_tool` / `general_tool` / `substrate_tool`。 |
| `tool_class` | enum | `focus_tool` / `sync_tool` / `read_tool`。 |
| `parse_status` | enum | `ok` / `invalid_json` / `schema_invalid` / `unknown_tool_id` / `unsupported_tool_family`。 |

## protocol receipt

| 字段 | 类型 | 说明 |
|---|---|---|
| `tool_id` | string | 被处理的协议工具。 |
| `tool_family` | enum | 通常为 `protocol_tool`。 |
| `tool_class` | enum | `focus_tool` / `sync_tool` / `read_tool`。 |
| `status` | enum | `accepted` / `applied` / `rejected` / `needs_review` / `processor_error` / `invalid_tool_request`。 |
| `source` | string | `provider_tool_call`、processor 或 audit 来源。 |
| `detail` | string | 可选处理摘要。 |
| `reason` | string | 可选拒绝或失败原因。 |

## native tool result warning

`native_tool_result` 是警告层的纠错 POPUP 片段；可见正文只提示失败事实、失败原因和纠偏动作，结构字段保留 `next_action` 给 Runtime、审计和测试判读。

| 字段 | 类型 | 说明 |
|---|---|---|
| `tool_id` | string | 失败或被拒绝的工具。 |
| `call_id` | string | provider call ID。 |
| `reason` | string | 稳定原因码。 |
| `field` | string | 可选，出错字段。 |
| `expected` | any | 可选，期望值。 |
| `actual` | any | 可选，实际值。 |
| `next_action` | enum | `revise_arguments` / `remove_unknown_field` / `respect_capability_gate` / `stop_or_retry_with_valid_tool` / `inspect_failure`。 |

## reaction_finalize

| 字段 | 必填 | 说明 |
|---|---|---|
| `handoff_text` | 是 | 一段自然语言交接；只在需要跨轮继续时调用本工具。 |

完成时不要调用 `reaction_finalize`，直接自然语言回复用户；Runtime 在账本无阻断时派生 `finish`。`blocked` 只由 Runtime 蓝屏类事故派生。记忆、读取、pending 和身份结算由 Runtime 根据真实回执、读取游标、pending tracker 与当前 interaction meta 生成 `settlement_ledger`；模型不再填写对应状态字段。

<!-- SETUP_FORMAT_START -->
起手步：审阅索引、选择挂载、安全裁决、身份入口确认、standby 判断和任务债务入口判断。起手步只能通过 provider-native setup_finalize 收束；裸文本、旧表格和自然语言判断只进 audit，不作为事实或执行证据。setup_finalize 结构字段：mount_requests[{type,ids}], security_verdict, reject_reason, suggested_mode, standby_skip_reaction, task_guidance_required, task_guidance_route, task_guidance_reason, interaction_object, identity_status, interaction_source, interaction_basis。`suggested_mode` 当前仅进入 Setup intent/audit，不切换模式、不注入规则、不投影 Reaction、不积累默契。起手步只声明任务债务，不读取材料、不创建任务账本、不写产物、不运行命令；用户请求本身要求多步骤/多来源研究、跨轮、执行命令、独立产物、验收或证据链交付时判 true，PRJ 因跨轮也为 true；直接回答即使使用 `memory_search`、`index_view`、`memory_content_read` 或有界只读查证也判 false，内部工具步骤不形成任务债务。无需独立产物或验收债务的单轮 `memory_write` 或 DC/EC/FUT 沉淀也判 false，但不能豁免它所属的更大任务。Runtime 在反应步把真实债务显影为 task_bootstrap 或登记 active task pending input。Runtime 会把结构化 setup_finalize 投影为自然语言 kind=setup_fact 交给反应步；setup_fact 不承载模型自由文本，与起手输入一并留在 now，首个成功 Reaction 返回后进入 lately/Corpus。
<!-- SETUP_FORMAT_END -->

<!-- STANDBY_SETUP_FORMAT_START -->
待命起手：可见 POPUP 只显示待命起手指南；standby setup 的机器结构字段仍由 provider-native setup_finalize 承载。待命轮仍以 provider-native setup_finalize 收束。
<!-- STANDBY_SETUP_FORMAT_END -->

<!-- CLEANUP_FORMAT_START -->
善后步：落账、归档、收尾。connection_material_settle: 联系材料整理结构；tacit_material_settle: 默契材料整理结构。善后本轮材料作为 C 轨 `transient_scope=cleanup_round`、`transient_target_step=cleanup` 临时语料块进入上下文，目标 cleanup 调用完成后清除，不进入 lately/Corpus。成功调用完整输入 Token 达三步共同逻辑窗口 90% 时 Runtime 记录压力；善后最终缓存落账后只冻结 `cache_compaction_debt.v3`，不改写 lately、不置 heartbeat。下一自然轮 Setup 照常，Reaction 以即时 `guide_submit(submit_cache_compaction_batch)` 分片暂存，随后由 v3 `ContextStore` 原子事务写回。cleanup_finalize 结构字段仍为 connection_materials, tacit_materials, lately_compression；善后裸文本只进 audit。
<!-- CLEANUP_FORMAT_END -->

<!-- REACTION_RESULT_FORMAT_START -->
反应步：推理、工具调用、生成回复。reaction loop 可继续调用合法工具，也可直接输出自然语言；有未闭合工具、任务账本、pending、节律或写入债务时，Runtime 将其记为 `assistant_text` / `kind=dialogue_progress` 对话进展语料并继续反应循环；无阻断时，Runtime 将无工具自然语言派生为 `finish` 并投影为用户最终回复。只有需要跨轮继续时才调用 `reaction_finalize(handoff_text)`；`blocked` 只由 Runtime 蓝屏类事故派生。
<!-- REACTION_RESULT_FORMAT_END -->

## general tool structure names

当前活动通用工具结构名：`file_read`、`file_glob`、`file_grep`、`file_edit`、`file_write`、`web_fetch`、`web_search`、`shell_command`、`subagent_dispatch`。`shell_command` 在 limited 不导出、guarded 逐次审批、unlimited 直接执行；sandbox grant 只约束初始 cwd，不构成进程级文件系统沙箱。内部账本结构包括 `general_tool_request`、`general_tool_call`、`general_tool_result`。通用工具 backend 字段包括 `backend_candidates`、`active_backend`、`backend_type`、`handler`、`permission_scope`。`file_read` 续读使用 `next_line_start` / `line_start`；除 `path/line_start/encoding/reason` 外的范围或窗口字段都不是当前 provider-native `file_read` 正向字段。

## audit structure names

Runtime 审计结构名：`runtime.tool_transaction_audit` 与 `round_{N}.jsonl`。pytest、schema、编码、一致性及 persona 验收属于宿主开发流程，直接保存真实命令输出、Spec verification receipt 与 Runtime 证据，不包装成 reaction 或 substrate tool。

### corpus_block

| 字段 | 类型 | 说明 |
|---|---|---|
| `id` | string | 语料块 ID。 |
| `role` | string | message 角色。 |
| `kind` | string | 语料块类型。 |
| `text` | string | 语料正文。 |
| `loc` | object | 来源定位。 |
| `policy` | object | 注入和留存策略。 |
| `ref` | object | 来源引用。 |

## retired text roots

旧 markdown 表格、冒号行、自由文本工具声明和旧容器正文块不再列为当前结构字段；Runtime 只把它们当作 retired / invalid 事实写审计，不加载指南、不授权工具、不执行 processor。
