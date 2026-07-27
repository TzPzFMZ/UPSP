# 协议工作流说明

> 消费方式：脚本与 LLM 共同参考。脚本用它确认协议级固定操作流边界，LLM 用它理解三步轮中的流程插槽。
> 注入模块：默认不整篇注入；按需抽取到 RULES 或 POPUP guide/reminder。
> 触发：协议工作流、流程插槽、技能挂载或三步输出边界发生争议时。

---

## 一、三层边界

UPSP 的固定运行不是普通 skill，也不是任意 OS 脚本拼接。当前分三层：

| 层级 | 中文名 | 英文字段 | 说明 |
|------|--------|----------|------|
| 协议骨架 | 三步轮本体 | protocol skeleton | 起手步→反应步→善后步、心跳事实源、血脑屏障、provider-native 工具执行账本 |
| 协议级固定操作流 | 协议工作流 | protocol_workflow | 骨架内的固定流程、输出契约、工具调用投影、POPUP guide/reminder 装配 |
| 位格可挂载能力 | 程序能力/认知范式 | procedures / patterns | 可被流程插槽调用或调制，但不改写协议硬点 |

三步轮本体不是技能，不进入 `LTM/Skills` 生命周期。协议工作流由 DDS 定义、由基座执行、由 config/registry 决定启用项，可以开放流程插槽让位格能力参与。

---

## 二、协议工作流不是技能容器

协议工作流不参与熟练度、遗忘、固化、回源复盘。它是 UPSP 基座的新陈代谢流程，类似呼吸节律本身。

`LTM/Skills/procedures` 中的程序能力可作为未来协议工作流插槽的候选材料，例如“立场一致性检查”“关系姿态复判”“D 阶段验收流程”；当前 Seed 只提供技能卡创建、挂接、读取与续写，不自动绑定插槽。

`LTM/Skills/patterns` 中的认知范式可以记录期望的思考或表达方法；自动调制 guide/reminder 属于 Arbor 目标。任何未来调制都不得修改硬 schema，不得跳过 provider-native 工具 schema、processor/guard/receipt/audit。

---

## 三、工具族与工作流

工具族决定工具碰触边界：

| tool_family | 中文名 | 典型位置 |
|-------------|--------|----------|
| protocol_tool | 协议工具 | 协议工作流内，操作 UPSP 内环境 |
| general_tool | 通用工具 | 接触外部文件、网页、shell、连接器、子 agent |
| substrate_tool | 基座工具 | runtime、heartbeat、context assembler、迁移脚本、测试门禁 |

工具姿态决定注意力与焦点：

| tool_class | 中文名 | 工作流含义 |
|------------|--------|------------|
| focus_tool | 焦点工具 | 单步单焦点，表示注意力独占或高副作用操作位；`container_focus` 改 WB focus；`memory_container_create/write` 写容器正文并更新 MEM 引用关系；旧 `memory_link_update add/set` 不再是正常挂接路径 |
| sync_tool | 同步工具 | 不占焦点，结构化声明，脚本串行校验落盘 |
| read_tool | 只读工具 | 不写目标边界，只做资料语料或只读装配 |

LLM 只声明 `tool_id`。工具族、工具姿态、风险、handler 和 result_kind 由注册表查出。不要要求 LLM 先声明工具族再声明子工具。

---

## 四、provider-native 工具调用四层职责

Spec134 之后，反应步工具入口可以由 provider-native tool calling 承载，但职责不下沉给模型：

1. provider schema：只限制导出的工具名、必填字段、枚举和参数形状，不能保证业务事实一定正确。
2. Runtime native validation/result projection：在路由前拒绝缺字段、未知字段、枚举越界、未导出工具或 provider-native trace 缺口，并把失败投影为 POPUP 原生工具调用警告。
3. `processor/handler/guard/receipt/audit`：仍是真实执行账本。通用工具先过 `ExecutionCapabilityGate` 再进 handler，协议工具走 native projection → processor/guard/receipt，协议链仍由 `tool_transaction_audit` 验账。
4. POPUP/永固层：只解释失败事实、纠错动作和安全边界。`native_tool_result` 是警告层 feedback，不是 workflow slot、不是工具入口、不是 processor receipt，也不进入 now/lately/Corpus。

`native_tool_result.next_action` 的稳定枚举为 `revise_arguments`、`remove_unknown_field`、`respect_capability_gate`、`stop_or_retry_with_valid_tool`、`inspect_failure`。若 warning 中出现 `actual/expected` 或 `arguments_json` 相关片段，命令、URL、路径、参数、正文、密钥、关系轴数值、`state.json` 数值和 live `persona/` 状态只可脱敏/截断，不得完整回显。

---

## 五、通用工具外部行动总线

`general_tool` 是 UPSP 的外部行动权工具族，不等于 MCP。MCP、connector、plugin、adapter 与 Python handler 只是执行后端。Spec 069 开通 `file_read`，Spec 070 开通 `web_fetch` / `web_search` 两个 `public_web_read` 只读 web 工具，Spec 071 开通 `file_edit` 受控 `workspace_patch_allowlist` 写入工具，Spec 072 开通 `shell_command` 低风险 `workspace_shell_allowlist` 命令工具，Spec 073 开通 `subagent_dispatch` 子 agent `subagent_task_scope` 调度工具，Spec339 开通 `file_search` 只读文件名搜索工具；后续按后端接线分批推进。

通用工具独立链路为：

1. provider-native tool call：反应步 LLM-facing 工具入口；Runtime 按注册表把通用工具投影为内部 `general_tool_request`。
2. `ExecutionCapabilityGate`：backend ready 后、handler 前做动作级能力裁决；拒绝时不调用 handler，直接返回 `general_tool_result status=rejected`。
3. `general_tool_call`：门禁放行后，Runtime 根据注册表 `backend_candidates / active_backend` 取出当前 `backend_type / handler / permission_scope`，形成内部执行对象。
4. `general_tool_result`：脚本返回结构化结果，供下一次反应迭代读取；执行事实写成 `kind=tool_fact`，只读正文/候选内容写成 `kind=material` 或既有 CONTENT 挂载。

`general_tool_result` 不叫 `protocol_tool_receipt`，也不进入 `tool_transaction_audit`。`backend_type=python/adapter/mcp/connector/plugin` 只表示执行后端，不改变工具族身份；MCP 后端执行的 `file_read` 仍是 `general_tool`。Spec132 门禁拒绝码固定为 `capability_denied`、`dangerous_shell_command`、`outside_allowlist`、`private_network_denied`、`write_scope_missing`。`file_read` 默认只读工作区 allowlist，拒绝 persona live 私密数据和密钥类路径；续读复制工具回执 `next_line_start` 到 `line_start`，不传其他范围或窗口字段。`file_search` 只搜索 allowlist 内候选文件名，不读取正文，默认不递归；`file_read` / `file_search` / `web_fetch` / `web_search` 的正文或候选资料不拼入 `tool_fact`。`file_edit` 默认只对仓库 tracked 文本文件或显式 allowlist 应用 unified diff，门禁先拒绝 live persona、密钥、越权路径、无 patch 与自然语言写入；`web_fetch` / `web_search` 默认只读公开 `http/https` 网页，门禁先拒绝本机/私网、登录交互、下载型资源和外部账号操作；`shell_command` 默认只在 allowlist cwd 执行低风险命令，门禁先拒绝删除、移动、重置、后台服务、网络写、远端脚本管道和凭据读取；Windows 下不支持 Bash/POSIX here-doc，多行 Python 应用 `file_write` 写临时 `.py` 后执行，或使用 PowerShell here-string 管道；`subagent_dispatch` 默认只调度边界清晰的子 agent 任务，写入型任务必须声明 `write_scope`，无真实后端时返回 `backend_unavailable`。

---

## 六、工具事务验账边界

`tool_transaction_audit` 是 `substrate_tool / sync_tool / audit / high`。它由 Runtime 在协议工具 processor 全部完成后执行，检查 native projection / processor / receipt 是否闭合；输出写入 `round_{N}.jsonl` 的 `runtime_audit` 事件。

开发与发布验收由宿主直接执行 pytest、schema、编码和一致性审计，并保存命令输出、Spec verification receipt 与已有 Runtime／persona 证据。当前不把这些外部检查包装成 Runtime 工具，也不生成独立 `validation_audit.jsonl`。

它不是 protocol_tool，不进入反应步 guide，不接受 `protocol_tool_submission`，不产生 `protocol_tool_receipt`。正常审计不写 now/lately/Corpus，避免把基座审计噪声塞进语义缓存。本阶段只做事后验账和留痕，不做事中拦截、回滚、熔断或自动故障记账。

---

## 七、缓存压缩边界

善后步的“最近缓存压缩”只在本轮发生 `lately` 字符履带删除后置位维护节律。删后幸存段仍作为原语料块留在“最近缓存 lately”层；脚本只在 POPUP 提醒 Runtime 将置位 `cache_compaction_due`，不要求 LLM 在 cleanup_finalize 中给出 keep、drop 或 replace 动作。压缩不设 kind 白名单，不默认排除当前轮或 `minimum_commitment`。

真正改写 `lately_cache.jsonl` 的执行器是 `cache_compact`，属于 `substrate_tool / sync_tool / context / high`。真实 source ids 留在 Runtime pending metadata；下一轮 rhythm agenda 物化 `cache_compaction_rhythm_guide` 后，模型用 `guide_submit(submit_cache_compaction_shard)` 提交分片摘要，Runtime 后台调用 `cache_compact`。它默认不作为反应步常规声明工具暴露，不注入普通 reaction guide，不产生独立前台工具入口。raw_log 与 Corpus 节归档保留 A 轨原文，压缩摘要以 `kind=cache_summary` 留在 lately。

---

## 八、立场一致性检查

立场一致性检查是可挂载在起手步的程序能力，而不是新的上下文层。

它可以参考仍在缓存中的交互语料、关系卡、记忆条目、DC/EC 线索；缓存没有就不补造 `recent_interaction_trace`。它的输出只能是模式建议、POPUP reminder、挂载建议或候选协议工具请求，不能直接写 persona 真源。
