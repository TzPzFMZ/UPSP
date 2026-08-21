# 流程插槽注册表

> 消费方式：脚本查表 + LLM参考。登记协议级固定操作流中允许挂载程序能力、认知范式调制、提醒模板、脚本事件或基座动作的位置。
> 注入模块：默认不整篇注入；命中具体插槽时抽取短说明或 POPUP reminder。
> 触发：三步装配、节律轮固化、技能挂载、流程插槽自检。

---

## 一、定义

流程插槽（workflow_slot）是协议工作流中允许挂载能力或提醒的固定位置。工程注释上它等价于 hook point / 钩子触发位置；面向 LLM 的主术语统一用“流程插槽”。

插槽不是写盘权限。插槽产物必须落在既有边界中：挂载声明、mode suggestion、POPUP reminder、协议工具请求、固定输出表、脚本事件或 C 轨单次调用临时语料。

---

## 二、等级

| 等级 | 中文名 | 能否自定义 | 说明 |
|------|--------|------------|------|
| L0 | 协议硬点 | 不能 | 三步轮顺序、心跳事实源、血脑屏障、provider-native 工具执行账本 |
| L1 | 协议保留插槽 | 位置固定，可配置挂载内容 | 反应步身份确认、安全裁决、模式建议、工具请求/提交、善后压缩 |
| L2 | 位格自定义插槽 | 可新增或调整，必须登记 | 立场一致性检查、关系姿态复判、教学策略检查、语气风格自检 |
| L3 | 临时工作流实例 | 当前任务短期挂载 | 项目专用检查清单、读书任务规则、一次性资料处理规范 |

L3 若被反复采用，可整理为 `procedures/subtype=workflow`；未整理前不作为稳定技能存在。

---

## 三、插槽清单

| step | slot_id | 等级 | 触发时机 | 允许挂载内容 | 输出边界 |
|------|---------|------|----------|--------------|----------|
| reaction | identity_resolution | L1 | 交互对象为 unknown/timeout 且有高影响动作前 | 身份确认卡、询问模板 | `identity_resolution`、identity question |
| reaction | relation_registration | L1 | 当前对象已明确自报但关系域未登记 | 同名新建关系卡提醒 | `relation_registration_reminder`、关系卡回执 |
| setup | security_review | L1 | 安全粗筛命中后 | 安全二值裁决 | 放行/驳回建议，不改轮类型 |
| setup | mode_suggestion | L1 | 装配反应步前 | 模式建议程序 | 模式建议、POPUP reminder |
| setup | mount_selection | L1 | 装配反应步前 | 记忆/关系/容器/技能挂载选择 | 挂载声明 |
| setup | setup_finalize | L1 | 装配反应步前 | 起手安全裁决、挂载请求、轮型确认、身份入口 | provider-native `setup_finalize` |
| setup | setup_fact_projection | L1 | 有效 setup_finalize 后 | Runtime 自然语言事实投影 | `kind=setup_fact`，与起手包留在 now，首个成功 Reaction 后进入 lately/Corpus |
| setup | stance_consistency_check | L2 | 对象已知、话题切换或前提冲突疑似出现时 | 程序能力、认知范式调制 | mode suggestion、POPUP reminder、候选工具请求 |
| reaction | provider_native_tool_call | L1 | 每次反应步迭代内 | LLM-facing 工具入口；直接调用当前已导出的 provider-native tool schema | Runtime 按注册表分流到 protocol/general/substrate 内部链路 |
| reaction | protocol_tool_request | internal | provider-native 协议工具投影后 | 内部路由字段；LLM 直接写旧文本字段会被 Runtime 记为 retired / invalid，不执行并回灌纠错 | processor / receipt |
| reaction | general_tool_request | internal | provider-native 通用工具投影后 | 内部路由字段；LLM 直接写旧文本字段会被 Runtime 记为 retired / invalid，不执行并回灌纠错；已开通 `file_read` / `file_glob` / `file_grep` / `file_edit` / `file_write` / `web_fetch` / `web_search` / `shell_command` / `subagent_dispatch` | `general_tool_call` |
| reaction | focus_tool_execution | L1 | provider-native focus tool 调用时 | 焦点工具正文编辑或 WB focus 切换 | 单焦点提交 |
| reaction | sync_tool_submission | L1 | provider-native sync tool 调用时 | 同步工具结构化提交 | 多工具串行落盘 |
| reaction | read_tool_mount | L1 | 需要只读资料或内环境只读内容时 | provider-native read tool | 资料语料、只读装配、回执 |
| runtime | tool_transaction_audit | L0/L1 | 协议工具 processor 完成后、round JSONL 收尾前 | 工具事务验账基座审计线 | `round_{N}.jsonl:runtime_audit` |
| reaction | style_or_pattern_modulation | L2 | 生成回复或填写 guide 前 | 认知范式、程序能力、reminder | 提醒风格、指南风格，不改硬 schema |
| reaction | contradiction_handling | L2 | 检测到前提冲突后 | 程序能力 | 澄清策略、批判/质疑模式提醒 |
| cleanup | training_material_settlement | L1 | 善后训练材料线 | 联系先行默契落账规则、联想五表计数规则 | `connection_material_settle` → `tacit_material_settle` → `association_count_update` |
| reaction | lately_compaction | L1 | 善后已按共同窗口压力冻结 v3 债务，且当前为下一自然轮 Reaction | 最近缓存即时压缩指南 | 分片结果暂存；达标后一次原子替换 |
| cleanup | minimum_commitment | L0 | 善后脚本固定边界标记 | 最小承诺规则 | kind=minimum_commitment |
| cleanup | settlement_review | L1/L2 | 善后收束时 | 默契/联系处理规则 | 善后两线清单 |
| heartbeat | trigger_classification | L0/L1 | heartbeat tick | 心跳事实源分类 | 轮类型触发事实 |
| rhythm | persona_custom_review | L2 | 节律轮 | 位格自定义复盘程序 | 复盘提醒、候选工具请求 |

---

## 四、绑定真源

流程插槽定义真源在本文件与 DDS。当前 Seed 没有技能自动绑定、habit/reflex 投影或定期技能缓存；若 Arbor 恢复技能器官选路，必须另立可版本化拓扑与启用回执。

---

## 五、硬约束

- L2/L3 不能改写 L0 硬点。
- 协议工具不能绕开 provider-native schema、processor/guard/receipt/audit。旧 guide + submission 闸门已退役，不再作为开通条件。
- LLM-facing 工具入口只能是当前已导出的 provider-native tool call；`protocol_tool_request` / `general_tool_request` 只作为 Runtime 内部路由字段保留，旧文本字段出现时按 retired / invalid 审计。
- `native_tool_result` 是 provider-native 工具失败 POPUP 警告层 feedback，不是 workflow slot、不是工具入口、不是 `protocol_tool_submission`、不是 processor receipt、不是 `tool_id` 或请求字段，也不进入 now/lately/Corpus；它的结构字段留给 Runtime、step.json 和 audit 判读，可见 POPUP 只说明失败事实、失败原因和纠偏动作。
- `native_tool_result` 中的 `actual/expected`、`arguments_json`、命令、参数、正文、密钥、关系轴数值、`state.json` 数值和 live `persona/` 状态只可脱敏/截断，不得完整回显。
- 通用工具不能伪装成协议提交；`file_read`、`file_glob`、`file_grep`、`file_edit`、`file_write`、`web_fetch`、`web_search`、`shell_command`、`subagent_dispatch` 经 provider-native 调用投影到内部 `general_tool_request`，并通过 `general_tool_result` 独立闭环。
- `backend_type` / `active_backend` 只描述通用工具的执行后端，不改变 workflow slot 或 `tool_family`。
- 固定输出表、脚本事件、基座工具动作和 C 轨单次调用临时语料走各自边界，不能伪装成协议工具提交。
- 插槽不能新增第二套心跳事实源。
- 立场一致性检查不新增近期交互摘要层；缓存里有就参考，没有就依靠关系卡、记忆与链容器。
- 认知范式可以调制提醒风格，但不能改变表格字段、字段含义或脚本校验规则。
