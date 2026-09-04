# WB 参数表·协议层

> 消费方式：脚本查表——WB 三区流转、任务指南与面单解析时查本文件取参数
> 注入模块：不直接注入（脚本内部消费 + LLM 参考）
> 触发：任务生命周期事件

---

## 一、基本参数

WB 是 STM 层调度台，全局唯一实例 WB-main。我对 WB 只读，工作台本身只由 Runtime 处理。

`status.json` 为五层架构（base/plus/pro/dlc/mod）。base 层包含 `instance_id`、`active_task`、`step_count`、`last_checkpoint`、`pending_interrupt` 和 `settlement`。它不保存容器选择或正文副本。

`persona/state.json` 是身体体征表；`workbench/status.json` 是任务调度台仪表盘。两者都不复制容器、记忆或关系正文。

`pending_interrupt` 表示非交互任务执行中需要在迭代边界处理的插话或打断记录。它不直接制造新轮；是否进入交互轮仍由入口管线和 `user_message_waiting` 决定。

---

## 二、三区流转与物流

WB 物流通道：input（待处理原料）→ process（正在加工）→ output（处理完成待配送）。完整任务在目录内保存清单、验收账、证据和产物；轻量任务可不建立跨轮目录。

任务 ID 格式为 `T-YYYYMMDD-序号`。面单的 `source` 表示任务来源，不表示正文已经读取，也不构成完成证据。

---

## 三、常驻正文与工作台

跨轮正文统一由 `STM/context/resident_list.json` 保存引用。它可以指向记忆条目、关系卡正文或一个容器的具体目标文件，但不复制正文；每个 Reaction Frame 都从对应真源重新读取。

`container_read` 成功后把目标文件加入常驻清单。范围参数只裁剪本次回执，下一 Frame 装配该目标文件的完整当前正文。`memory_container_create` 创建并写入首段后自动登记常驻引用；`memory_container_write` 只允许写本 Frame 起点已经装配到 CONTENT 的具体目标文件。

`instant_list` 是当前 Round 的内存挂载投影，不存在活动持久 `instant_list.json` 真源。取消正文挂载只使用 `mount_cancel(resident_list|instant_list, ...)`。

---

## 四、任务计划与证据

复杂自然语言工程任务可由 Runtime 创建 WB task guide：`task_bootstrap` 把用户目标、计划来源坐标、粗粒度任务拆分与验收标准物化为 `process/<task_id>/task_guide.json` 与 `acceptance_ledger.jsonl`。路径、URL 或文件名可以先作为 `source_refs` 计划指针，但不表示已经成功读取，也不能作为完成、阻塞或验收条件；`source_requirements` 可选。

工具结果真正改变来源、任务拆分、验收或风险时，`guide_submit(task_progress/revise_task_plan)` 可原子替换一个或多个完整目标片段，并在 ledger 中保存 unified diff。已完成/已通过记录不可删除、换 ID 或语义改写，状态和证据也不能经修订入口提交。

进度只走 `guide_submit(task_progress/update_task_status)`。`done/passed` 只接受成功证据；`blocked` 必须同时写非空 reason 和 Runtime 已登记的 evidence ref。全部 required 记录闭合后 Runtime 自动撤下任务清单；完成则自然语言回复，需要跨轮继续才单独调用 `reaction_finalize(handoff_text)`。
