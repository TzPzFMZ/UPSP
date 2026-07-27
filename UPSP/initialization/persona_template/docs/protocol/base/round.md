# 五类轮参数表·协议层

> 消费方式：脚本查表——读 heartbeat_flags + state → 查本文件 → 判定轮类型+装配参数
> 注入模块：不直接注入（脚本内部消费）
> 触发：每轮起手步

---

## 一、五类轮编排表

| 轮类型 | Tier | 起手步触发源 | 反应步形态 | 善后步输出 |
|--------|------|------------|-----------|-----------|
| 节律轮 | 1 | rhythm/calendar/API/token/context/cache 类心跳 flag | 写节志+复核真实 connectivity 证据+警戒结算 | 节志落盘+alerts归档进IMM |
| 交互轮 | 1 | 用户消息到达 | 1-N次装配+生成，超时存档续轮 | 有回复·联系集+默契集+联想集更新+最小承诺 |
| 中继轮 | 2 | `continue_requested` heartbeat flag | 中继续传：总结进度 | 无对外回复 |
| 自主轮 | 3 | STM/evolution 类心跳 flag | 1-N次执行，超时存档续轮 | 可能无回复·进化集整理（阈值触发） |
| 待命轮 | 4 | standby/shelve 类心跳 flag | 检查既有 connectivity／breaker 证据 | 健康状态归档 |

差异仅在两端（起手步触发源 + 善后步输出形态），中间反应步完全同构。

---

## 二、优先级分层

```
Tier 1：交互 = 节律（可合轮；合轮内按紧急最小处理、主轴节律、日历节律、交互推进）
Tier 2：中继
Tier 3：自主
Tier 4：待命
```

### 判定流程

```
起手步读取 state + heartbeat_flags
  │
  ├─ rhythm/calendar/API/token/context类?  ──→ 节律轮；若 user_message_waiting 同时存在则合轮
  ├─ user_message_waiting?                ──→ 交互轮
  ├─ continue_requested?                  ──→ 中继轮
  ├─ STM/evolution类?                      ──→ 自主轮
  ├─ standby/shelve类?                     ──→ 待命轮
  └─ 以上都无                              ──→ 休眠（idle）
```

### 冲突矩阵

| 情况 | 处理 |
|------|------|
| 交互 + 节律 | 合轮：当前指南顺序为紧急最小处理 → 主轴节律 → 日历节律 → 交互，善后步按真实结构回执逐项结算 |
| 节律 + 中继 | 节律先跑；`continue_requested` 保留给后续中继轮 |
| 高层 + 低层（非待命） | 高层先，低层 flag 保留，下一轮处理 |
| 任何轮 + 待命 | 待命倒计时重置，当轮不处理待命 |

---

## 三、heartbeat_flags（17 个活动字段）

### 基础活动字段

| Flag | 检查条件 | 含义 |
|------|---------|------|
| feeling_settle_due | next_settle时间戳过期 | 感受缓冲超时 |
| api_degraded | 当前有效模型链 endpoint 最新状态为 `error/timeout`，或 circuit_breaker=open | API降级；同 endpoint 最新 `ok` 抵消旧错误，未参与当前路由的模型不阻塞恢复 |
| stm_degrade_pending | heat.json有降格=true且入库=false | STM降格待处理 |

### v0.7追加5个

| Flag | 检查条件 | 含义 |
|------|---------|------|
| user_message_waiting | 入口管线有未处理交互语料块 | 用户消息等待 |
| rhythm_due | total_round - last_rhythm_round ≥ 32 | 节律点到期 |
| standby_due | 距上一轮结束≥30分钟 | 待命到期 |
| continue_requested | 反应步超时、协议工具或脚本事件明确置位 | 中继续传请求 |
| shelve_timer_expired | 搁置任务计时器到期 | 搁置任务超时 |

### v0.10.0及后续追加9个

| Flag | 检查条件 | 含义 |
|------|---------|------|
| token_usage_warning | token 用量比例 ≥ 0.7 | token预警 |
| context_pressure | lately 归零后仍持续超窗 | Runtime／装配器置位的上下文维护义务 |
| cache_compaction_due | lately 本轮发生删除 | Runtime 置位的缓存压缩义务 |
| calendar_day_due | 跨日 | 日节律触发 |
| calendar_week_due | 跨周 | 周节律触发 |
| calendar_month_due | 跨月 | 月节律触发 |
| calendar_quarter_due | 跨季 | 季节律触发 |
| calendar_year_due | 跨年 | 年节律触发 |
| evolution_pending | Raw/Tacit 或 Raw/Connection pending 行数达阈值 | 进化集整理待触发 |

心跳硬约束：不计轮数 / 不调API / 不注入LLM / 不判断业务 / 不回滚 / 轮内暂停。

### 五类触发归口

| 触发类 | flags |
|--------|-------|
| interaction | user_message_waiting |
| rhythm | rhythm_due / calendar_day_due / calendar_week_due / calendar_month_due / calendar_quarter_due / calendar_year_due / api_degraded / token_usage_warning / context_pressure / cache_compaction_due |
| relay | continue_requested |
| autonomous | stm_degrade_pending / evolution_pending |
| standby | standby_due / shelve_timer_expired |

`feeling_settle_due` 是本地维护旗标，不创建自主轮。空闲时由常驻 Runtime 直接完成数值结算；若已有真实轮触发，则由该轮善后合并结算。

当前交互对象复判来自真实 `unknown` 或未确认 subject 事实；当前自主轮只由有效 STM/evolution flags 触发；进程崩溃只由 Runtime supervisor 恢复。

---

## 四、步内分结构参数

### 起手步三段

| # | 子阶段 | 执行者 | 强制度 |
|---|--------|--------|--------|
| ① | 脚本预装配 | 脚本 | 硬 |
| ② | LLM挂载决策 | LLM | 中 |
| ③ | 脚本拆解 | 脚本 | 硬 |

### 反应步五阶段

| # | 子阶段 | 执行者 | 强制度 |
|---|--------|--------|--------|
| ① | 脚本按指令装配 | 脚本 | 硬 |
| ② | 进入 | 脚本 | 硬 |
| ③ | Agent Loop (0~N) | LLM | 中 |
| ④ | 退出（A类体面/B类蓝屏） | 脚本 | 硬 |
| ⑤ | 交出结果 | LLM→脚本 | — |

### 善后步四阶段

| # | 子阶段 | 执行者 | 强制度 |
|---|--------|--------|--------|
| ① | 接收输入 | 脚本 | 硬 |
| ② | LLM结构化填表 | LLM | 中 |
| ③ | 脚本原子写 | 脚本 | 硬 |
| ④ | 心跳收尾 | 脚本 | 硬 |

### 三步工具通道

| 步 | 焦点工具 | 同步工具 | 只读装配 | 总特征 |
|----|----------|----------|----------|--------|
| 起手步 | ✗ | ✗ | ✓ | 纯读 |
| 反应步 | ✓（WB焦点，每迭代最多1） | ✓（N个，批量声明） | ✓ | 生成+工具调度 |
| 善后步 | ✗ | ✓（两线 substrate 工具输入） | ✓ | 收束+归档 |

---

## 五、节律点3项职责

| # | 职责 | 说明 |
|---|------|------|
| 1 | 写节志 | 本节总结落盘 |
| 2 | 连接状态复核 | 读取当前有效模型链真实调用留下的 connectivity／breaker 证据；不自动发送付费 probe |
| 3 | alerts归档进IMM | STM/health/base/alerts.md → LTM/Immune/alerts.md |

---

## 六、故障梯级（L1-L5）

| 级别 | 情况 | 处理者 | 处理方式 | Base版 |
|------|------|--------|----------|--------|
| L1 | 反应步API断连 | 脚本 | 当前模型最多三次暂态尝试，再按本阶段有效模型链降级；不同指纹模型总数最多三个 | ✅ |
| L2 | L1未恢复 | 善后步 | B类蓝屏退出→善后步归档残局 | ✅ |
| L3 | 善后步API也挂 | 心跳急救 | 脚本级最小状态保存（不需LLM） | ✅ |
| L4 | 全部API挂了 | 脚本极端措施 | 联系紧急联系人/唤醒冷备模型 | ⚠️ 接口位 |
| L5 | 断电 | 硬件 | UPS/紧急电源 | ❌ 非软件范畴 |

---

## 七、善后步flag清零语义

善后步④在恢复心跳检测前执行：

1. 清零本轮已消费的flag（未处理的低层flag保留）
2. 重置待命倒计时（无条件，每一轮都做）

三条机制联锁：轮执行期间暂停心跳检测 → 善后步选择性清零+待命归零 → 心跳检测面对干净快照恢复。

### 插话规则

非交互轮运行期间用户消息到达 → 写入缓冲区 → 当前步走完 → 善后步正常结算 → 心跳检测恢复后首个检查点置位 user_message_waiting → 下一轮按判定流程处理。被中断的轮不丢弃，其flag在善后步中被保留。

### 内部交接与心跳触发

`runtime.next_round` 已退役。轮类型只由 heartbeat flags 判定，任何待续、节律、待命或自主唤醒都必须先形成对应事实源或明确置位 heartbeat flag。

心跳触发后的脚本说明写成 `kind=setup_fact`，供本轮起手/反应链路阅读，并可在 now 水位后进入 lately/Corpus。反应步 `reaction_finalize.handoff_text` 触发的跨轮继续正文登记到 `state.base.runtime.relay_intents[]` 供调度追踪，同时写成 `kind=relay_handoff` / `role=user` 语料块；标题必须声明“上轮交接任务”，不得伪装成用户原始输入。运行期任务条、GUIDE、POPUP 和焦点投影不是 pending，也不是缓存履带；如果某段说明需要长期保留，必须由反应步协议工具写成记忆条目、故障记录或合适的工作容器内容，不能把最小承诺当长期语义载体。

```json
{
  "relay_intent_id": "RLY-R000615-N001",
  "status": "open",
  "handoff_text": "上轮反应步达到时间上限，请从上一段真实工具结果之后继续。"
}
```

---

## 八、轮相关经验常数

| 常数 | 值 | 类别 |
|------|-----|------|
| 反应事务基准窗口 | 600秒（1x/2x/3x 时间阶梯） | 容灾 |
| 节律点周期 | 32主轴轮 | 校准+容灾 |
| 待命轮间隔 | 上一轮结束后30分钟 | 容灾 |
| 心跳间隔 | 5秒 | 容灾 |
| 疲劳倒计时 | 退役兼容；live 不使用 | 兼容 |
| 熔断阈值 | 连续3次失败 | 容灾 |
| 熔断锁定 | 15分钟 | 容灾 |
| 身份确认超时 | 退役兼容；live 不使用 | 兼容 |
| 对话历史履带上限 | 32轮 | 配额 |
| 三级反抗定盘阈值 | L2=连续5轮/L3=1小时窗口 | 刚性（不进config） |
