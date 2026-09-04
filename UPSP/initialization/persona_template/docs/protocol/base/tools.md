# 工具边界

> 职能：本文件只保存工具分类边界和执行纪律。当前 Frame 的 `01_tool_header` 是唯一活动工具清单；工具参数、字段约束和回执纪律以 provider-native schema 为准。

## 总则

- provider-native 工具直接导出、直接调用；不再先请求工具 guide。
- `protocol_tool_guide_request`、旧 `tool_request`、旧 `protocol_tool_submission`、旧 markdown 表格动作行都退役出正常路径。
- 旧文本字段出现时只进入 retired / invalid 审计，不加载指南、不授权工具、不执行 processor。
- `native_tool_result` 只是 Runtime warning 投影，不是可调用工具。
- `reaction_finalize` 只负责跨轮中继交接；完成时直接自然语言回复用户，由 Runtime 派生 `finish`。

## 三种工具姿态

| tool_class | 纪律 |
|---|---|
| `read_tool` | 读取证据或正文；可以触发该读工具既定的确定性生命周期，但不接受模型正文写入。 |
| `sync_tool` | 修改位格内环境；同一 Frame 可提交多个合法同步工具，Runtime 分别串行校验、事务写入和出具 receipt。 |
| `action_tool` | 操作宿主或外部环境；必须经过当前权限、能力门与必要审批。 |

行动工具在执行前写入私有动作账本。进程中断后，Runtime 只按账本与当前文件状态分类；不重放旧 Frame、不恢复旧正文、不自动重跑 Shell 或子 agent。有活动任务时恢复事实进入既有待整合输入；没有任务时在下一次成功 Reaction 作为一次性资料出现。冲突或结果不确定的同签名动作不可重试，其他工具仍按当前权限正常使用。

Runtime 用隐藏的 `execution_route` 选择内部 processor、宿主派发或基座执行。该字段不进入模型工具头，也不是模型需要选择的分类。

## 当前工具真源

- 只使用本 Frame `01_tool_header` 实际导出的工具；未导出的 ID 不可调用。
- POPUP 只补充当前任务、纠错、即时指南或节律事务的近位纪律，不复制完整工具表。
- `memory_privacy_mark`、`memory_privacy_declassify` 等 deferred 能力不会出现在活动工具头；历史调用只读展示。
- Runtime 后台 processor 名称和内部结果结构不是模型工具。

## CONTENT 与写入可见性

- 已有内容改写必须先看见目标材料。
- 已有关系卡更新必须先看见对应 `relation_read(body)` 正文；同一响应里先读再写不算已看见。
- 真实正文材料放在 CONTENT，不放在 POPUP。
- 只读工具的执行事实只写状态、来源、范围、游标、数量和失败原因；正文、网页内容、搜索候选和索引展开内容写 `kind=material` 或 CONTENT，不拼进工具事实。
- 取消挂载必须使用 `mount_cancel`；它只取消 `resident_list` 或 `instant_list` 的挂载项，不删除源正文。
- `memory_container_write` 只能写本 Frame 起点已经装配到 CONTENT 的具体容器目标文件。
- 同 Frame 刚执行的 `container_read` 或 `memory_container_create` 尚未进入本次输入，不能立即取得写权；下一 Frame 才可写。
- `container_read` 成功即把目标文件加入常驻清单；容器写权只按 Frame 起点的 CONTENT 可见性判断。
