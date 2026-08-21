# 热度公式与记忆生命周期

> 消费方式：Runtime 读取配置并管理当前分身 STM `heat.json`；LTM 没有热度。
> 触发：召回时加热，每轮 cleanup 后衰减与结算。

## 一、STM 热度

`heat.json` 每条记录固定 9 个字段：`H`、`zone`、`AH_high`、`AH_low`、`last_heat_at`、`last_high_at`、`degrade`、`compression`、`heat_locked`。正式入库状态不属于 heat，只能读取公共 LTM `meta.stored_at`。

| 分区 | 条件 | 衰减/轮 |
|---|---:|---:|
| 显著 | `H >= 70` | -5 |
| 未定 | `40 <= H < 70` | -10 |
| 衰减 | `H < 40` | -15 |

正文实际进入 CONTENT 时 H+10；普通索引命中不加热。setup、`memory_content_read`、本轮新写自动挂载与器官产品共用 `RoundContext.memory_heat_boosted_ids`，同一 ID 同轮最多加热一次。回忆重整使用触发召回已经形成的 STM 与调用坐标，不二次加热、不二次记召回。局部读取只裁剪本次返回，不裁剪 STM 副本。

新条目初始 H：W5=80、W4=70、W3=60、W2=50、W1=40。`weight=0` 不创建记忆。

`heat_locked=true` 时固定 H=80、zone=显著、AH_low=0、degrade=false；它与 LTM Pinned 无关。

## 二、LTM 唯一真源与正式入库

公共记忆创建时，Runtime 按初始 weight 同时写入 LTM 与当前分身 STM：W5→Full、W3/4→Summary、W1/2→Abstract。LTM 从创建起就是正文、标题、tags 与共享 meta 的唯一真源；STM 只承担当前分身热度、衰减、临时驻留和挂接 overlay。weight 在正式入库后不可变；唯一例外是未入库 STM 遗忘时与物理降层同步降权。

`created_at/created_round` 表示创建；`stored_at=""` 表示尚未正式入库。只有以下事件可首次填写 `stored_at`，填写后永久不变：

- `AH_high` 达到阈值的自然升格；
- 未入库 STM 的遗忘压缩或直接归档；
- 用户人工钉选到 Pinned。

自然升格只填写 `stored_at` 并重置完整 LTM 衰减周期，不改正文、层级、weight 或 STM 热度历史，也不删除 STM。

## 三、召回与双驻留

公共 LTM 召回时，Runtime 先把完整 LTM 正文和共享 meta 投影到当前分身 STM，再按同轮去重规则加热。新 heat 按真实 weight 初始化，随后 H+10、degrade=false、AH_low=0；已有双驻留保留本地 overlay、AH_high、热度锁和其他热度历史。已正式入库且位于 Full/Summary/Abstract 的条目每次真实召回还会把当前层 `decay_countdown_days` 重置为 `decay_period_days`；同轮热度去重不取消这项幂等续期。未入库条目与 Pinned 不伪造 LTM 日衰减续期。

GUI 查看详情和人工定期挂载都不是模型召回，不创建 STM、不加热、不改调用坐标。

## 四、遗忘分流

`degrade=false` 时始终保留 STM。`degrade=true` 时：

- LTM `stored_at` 非空：核验 LTM 后删除当前分身 STM 五件套；
- `stored_at` 为空且 W5：Cleanup 先把 Full→Summary/W4 事项写入共享压缩账本，核验后删除 STM；日节律按冻结材料完成语义压缩与关键词裁剪，成功后填写 `stored_at` 并重置周期；
- `stored_at` 为空且 W3/4：Cleanup 先把 Summary→Abstract/W2 事项写入共享压缩账本，核验后删除 STM；日节律完成压缩后才填写 `stored_at` 并重置周期；
- `stored_at` 为空且 W1/2：保持既有 LTM Abstract 正文，填写 `stored_at`，重置周期，再删除 STM。

账本落盘失败时不得删除 STM；账本成功后，LTM 原文、原 tags、原层级和空 `stored_at` 保持到日节律 apply。日节律中 STM 阶段先于 LTM 阶段，任一批次失败都不得越过到周志。模型只能从每条现有 tags 中选择保留项，不能在压缩阶段创造关键词；处理器按原 tags 顺序落盘。事务成功后层级与新 weight 对齐，不触发回忆重整。LTM 日衰减跳过 `stored_at=""` 的条目；已入库 LTM 日衰减只降层、不降权，由它造成的合法错位在真实召回时进入回忆重整指南。

待压缩项若在 apply 前被 setup 或 `memory_content_read` 真实召回，Runtime 取消待办：未入库项按当前形态正式入库，已入库项续满周期，然后正常重建 STM 与加热。GUI、索引、raw inspection、`mount_mode=none` 不触发取消。人工定期钉选也覆盖待办，但不算召回或加热。

## 五、人工定期挂载

人工挂载只作用于公共 LTM 真源，不进入 `resident_list/instant_list`。形态已与当前 weight 对齐时，Runtime 首次填写空的 `stored_at`、将条目置于 Pinned 并进入定期层。

已入库 LTM 日衰减造成层级低于 weight 目标层时，GUI 只登记等待重整请求；后续自然进入 Reaction 的 Round 才把真源挂入 CONTENT，并以前台即时指南强制完成回忆重整。STM 未入库遗忘后的条目已同步降权并对齐，可直接挂载。重整成功后 Runtime 自动尝试钉选；容量或写盘失败时保留重整结果，把请求记为 `mount_blocked`，等待用户取消或调整容量后重试。

Pinned 不参与普通 LTM 日降格。最后一个已核验的定期所有者取消后，定期挂载造成的 Pinned 按当前 weight 回到 Full/Summary/Abstract；旧账本无法证明钉选前形态时保守留在 Pinned。

## 六、强制溢出

STM `memory.md` 超过 65536 字符时仍按既有保险丝处理：优先低权低热，保护 H>=70，单次最多 32 条，并把无法收敛的结果写入现有健康告警与 Round audit。该保险丝不得改写 LTM `stored_at` 语义。
