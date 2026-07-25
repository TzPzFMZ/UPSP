# 工具短索引与边界表

> 职能：本文件只保存工具索引、分类边界和执行纪律。工具参数、字段约束、权重表、回执纪律写在 provider-native tool description / parameter description 中。Runtime 不再从本文抽取完整 guide。

## 总则

- provider-native 工具直接导出、直接调用；不再先请求工具 guide。
- `protocol_tool_guide_request`、旧 `tool_request`、旧 `protocol_tool_submission`、旧 markdown 表格动作行都退役出正常路径。
- 旧文本字段出现时只进入 retired / invalid 审计，不加载指南、不授权工具、不执行 processor。
- `native_tool_result` 只是 Runtime warning 投影，不是可调用工具。
- `reaction_finalize` 只负责跨轮中继交接；完成时直接自然语言回复用户，由 Runtime 派生 `finish`。

## 工具分类

| family | 含义 | 当前入口 |
|---|---|---|
| `protocol_tool` | 操作 UPSP 内环境，走 processor / guard / receipt / audit | provider-native tool call |
| `general_tool` | 接触外部世界或宿主环境，先过 capability gate | provider-native tool call |
| `substrate_tool` | 维护 Runtime 基座自身，通常只在内部使用 | Runtime 内部 |

高频层本步短工具带只展示当前可用工具索引与边界，不展示完整 guide。setup/cleanup 的 substrate 工具只作为本步固定工作流 guide；reaction 只看 provider-native tool schema 和短提示。

注册表元数据真源在 `logic/protocol_tools.py`：`tool_family`、`tool_class`、`backend_type`、`permission_scope`、`handler`、`result_kind` 不在短索引表头展开。中文名仍可查：文件写入通用工具、文件编辑通用工具、shell 命令通用工具、子 agent 调度通用工具。

## 姿态纪律

| tool_class | 纪律 |
|---|---|
| `focus_tool` | 一个 reaction iteration 最多接受一个。用于 WB 焦点切换或焦点绑定写入；多个 focus tool 同迭代时只处理第一个，其余写 invalid audit。 |
| `sync_tool` | 同一 reaction iteration 可以提交多个不同 sync tool；Runtime 串行校验、处理、写 receipt。 |
| `read_tool` | 不占焦点，不写目标边界；结果进入 receipt、NOW 或 CONTENT。 |

## 当前 provider-native 协议工具

<!-- PROTOCOL_TOOL_INDEX_START -->
| tool_id | 姿态 | 领域 | 何时请求 | guide/边界提示 |
|---|---|---|---|---|
| memory_write | sync_tool | memory | 创建独立 MEM 条目 | 直接看 native schema；不请求完整 guide |
| memory_container_create | focus_tool | memory/container | 挂接创建 | 写首段正文并替换 WB focus |
| memory_container_write | focus_tool | memory/container | 挂接写入 | 入口已可见 WB focus 后才能写 |
| container_focus | focus_tool | workbench | open/close/restore 焦点卫生 | 不承载正文 create/write |
| mount_cancel | sync_tool | context_mount | 取消 focus/resident_list/instant_list 挂载 | 不删除源正文，不改变通用工具结果 |
| pending_cancel | sync_tool | write_settlement | 取消 `memory_write` 反复失败后的写入意图 | 只对 `memory_write` 第三次仍失败后显式开放的 open pending 生效；已补写或不存在的提醒返回“未发现此提醒或已结清” |
| guide_submit | sync_tool | workbench | 按当前 active guide 选择选项并提交字段 | 只使用 Runtime 当前显示的 `guide_id/item_id/option_id/fields`；任务 progress 只能更新已声明 `item_XX/acc_XX` 的结构状态，不接受 `notes`；全部 required 记录通过 `update_task_status` 完成且证据通过后，Runtime 自动撤下任务清单；之后完成就自然语言回复，跨轮继续才 `reaction_finalize(handoff_text)`，不要把 `guide_submit` 当成普通任务第一动作 |
| setup_finalize | sync_tool | runtime | 起手步终端收束 | 只在 setup channel 作为 provider-native 终端工具暴露；`native_only + step_terminal` |
| reaction_finalize | sync_tool | runtime | 跨轮中继交接 | 只在需要跨轮继续时单独调用；只接受 `handoff_text`；完成时直接自然语言回复用户 |
| cleanup_finalize | sync_tool | runtime | 善后步终端收束 | 只在 cleanup channel 作为 provider-native 终端工具暴露；`native_only + step_terminal` |
| relay_intent_settle | sync_tool | relay | 结算中继意图 | 只更新 `relay_intent_id` 对应意图为完成、合题、反问或搁置 |
| memory_link_update | sync_tool | memory | 历史修复或移除旧挂接 | 正常挂接正文不走薄索引 |
| memory_recall_complete | sync_tool | memory | 补全召回义务 | processor receipt 为准 |
| relation_card_write | sync_tool | relation | 创建或更新关系卡 | 更新已有卡前必须先让对应 `relation_read(body)` 正文在 CONTENT 中可见；不写轴数值 |
| fault_record | sync_tool | fault | 记录故障事实 | processor receipt 为准 |
| chronicle_write | sync_tool | chronicle | 写编年材料 | 只消费 Runtime 当前挂上的编年史焦点；模型只填正文 |
| alert_mode_settle | sync_tool | alert | 结算 alert 事件 | status 只允许 `recovered`、`deferred`、`needs_human` |
| index_view | read_tool | index | 查看索引窗口 | 不占焦点；不提供容器注册表视图 |
| corpus_read | read_tool | context | 原位展开折叠的轮中进展语料块 | 只接受当前上下文可见的 `dialogue_progress` 短 ID；下一次 provider 调用展开一次 |
| relation_read | read_tool | relation | 读取关系材料 | 可投到 CONTENT |

`memory_privacy_mark` 与 `memory_privacy_declassify` 当前为 `disabled/deferred`，不属于 reaction 可调用工具；历史调用只返回 `feature_deferred`。
| memory_content_read | read_tool | memory | 读取记忆正文 | 可投到 CONTENT |
| container_read | read_tool | container | 读取容器材料 | 必须使用具体容器编号；EC、DC、PRJ、SKL、FUT 只是容器类型 |
<!-- PROTOCOL_TOOL_INDEX_END -->

## 当前 provider-native 通用工具

<!-- GENERAL_TOOL_INDEX_START -->
| tool_id | 姿态 | 领域 | 何时请求 | guide/边界提示 |
|---|---|---|---|---|
| file_read | read_tool | filesystem | 读取允许范围内文件 | 续读复制回执 `next_line_start` 到 `line_start`；不要传其他范围或窗口字段 |
| file_search | read_tool | filesystem | 搜索允许范围内候选文件名 | 只返回候选路径，不读取正文；默认不递归 |
| file_edit | focus_tool | filesystem | 修改允许范围内文件 | 必须满足写入能力门 |
| file_write | focus_tool | filesystem | 创建或覆盖允许范围内普通文件 | 仅放行档下发；必须填写 `path/content/purpose` |
| web_fetch | read_tool | web | 抓取网页内容 | 必须满足网络能力门 |
| web_search | read_tool | web | 搜索网页资料 | 必须满足网络能力门 |
| shell_command | focus_tool | shell | 执行命令 | 必须满足命令能力门和风险约束；Windows 下按 cmd.exe 语义执行，不支持 Bash/POSIX here-doc；多行 Python 用 `file_write` 临时脚本或 PowerShell here-string |
| subagent_dispatch | focus_tool | subagent | 派发子任务 | 必须声明目标和边界 |
| general_tool_result | internal | result | 通用工具结果结构 | Runtime 内部结构；模型不调用；只读结果拆为 `tool_fact` + `material` |
<!-- GENERAL_TOOL_INDEX_END -->

## substrate 工具索引

| tool_id | 说明 | family | class | domain | risk | LLM-facing |
|---|---|---|---|---|---|---|
| context_assemble | 上下文装配基座能力 | substrate_tool | read_tool | context | high | 否 |
| setup_mount_apply | 起手挂载应用基座工具 | substrate_tool | read_tool | context | high | 否 |
| setup_security_gate | 起手安全裁决基座工具 | substrate_tool | sync_tool | security | high | 否 |
| setup_handoff | 起手交接基座工具 | substrate_tool | sync_tool | setup | medium | 否 |
| standby_setup_handoff | 待命起手交接基座工具 | substrate_tool | sync_tool | setup | medium | 否 |
| reaction_loop | 反应循环指南基座车道 | substrate_tool | sync_tool | reaction | high | 否 |
| tool_transaction_audit | 工具事务验账基座审计工具 | substrate_tool | sync_tool | audit | high | 否 |
| cleanup_handoff | 善后交接基座工具 | substrate_tool | sync_tool | cleanup | medium | 否 |
| cache_compact | 最近缓存压缩工具 | substrate_tool | sync_tool | context | high | 否 |
| heartbeat_tick | 心跳检测基座能力 | substrate_tool | sync_tool | heartbeat | high | 否 |
| connection_material_settle | 联系材料整理工具 | substrate_tool | sync_tool | training | high | 否 |
| tacit_material_settle | 默契材料整理工具 | substrate_tool | sync_tool | training | high | 否 |
| association_count_update | 联想计数更新工具 | substrate_tool | sync_tool | training | high | 否 |
| heartbeat_restart | 心跳恢复基座能力 | substrate_tool | sync_tool | heartbeat | high | 否 |
| registry_reload | 注册表重载工具 | substrate_tool | sync_tool | registry | medium | 否 |
| migration_guard | 迁移守门工具 | substrate_tool | sync_tool | migration | high | 否 |
| state_settle | 状态结算基座工具 | substrate_tool | sync_tool | state | high | 否 |
| state_coordinate | 状态协调基座工具 | substrate_tool | sync_tool | state | high | 否 |
| state_reconcile | 状态对账基座工具 | substrate_tool | sync_tool | state | high | 否 |

## CONTENT / 焦点写入

- 已有内容改写必须先看见目标材料。
- 已有关系卡更新必须先看见对应 `relation_read(body)` 正文；同一响应里先读再写不算已看见。
- 真实正文材料放在 CONTENT 或 WB focus 投影中，不放在 POPUP。
- 只读工具的执行事实只写状态、来源、范围、游标、数量和失败原因；正文、网页内容、搜索候选和索引展开内容写 `kind=material` 或 CONTENT，不拼进工具事实。
- `container_focus.open` 只让下一迭代看见目标 focus。
- 取消挂载必须使用 `mount_cancel`；它只取消 `focus`、`resident_list` 或 `instant_list` 的挂载项，不删除源正文。
- `memory_container_write` 只能在看到入口 WB focus 投影的迭代写入。
- 同迭代刚 open 的容器不能立刻写。
