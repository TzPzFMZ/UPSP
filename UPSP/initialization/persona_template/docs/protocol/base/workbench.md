# WB 参数表·协议层

> 消费方式：脚本查表——WB 三区流转/焦点切换/面单解析时查本文件取参数
> 注入模块：不直接注入（脚本内部消费+LLM参考）
> 触发：任务生命周期事件、焦点变更

---

## 一、基本参数

WB 是 STM 层唯一调度台，全局唯一实例 WB-main，非容器不进 container_registry。我对 WB 只读——WB 本身只由脚本操作。

status.json 为五层架构（base/plus/pro/dlc/mod）。base 层含 instance_id、focus（当前焦点容器 ID，0 或 1 个）、old_focus（节律点清空前的焦点，用于恢复提示）、active_task（当前活跃任务 ID）、step_count、last_checkpoint、pending_interrupt 和 settlement（结算状态，含 pending 布尔、level 整数、reason 文本）。

与 persona/state.json 的区别：state.json 是身体体征表（跟着位格走，回答身体状况如何），status.json 是调度台仪表盘（跟着工作台走，回答正在干什么）。

`pending_interrupt` 是 WB 插话事实源：表示非交互任务执行中出现需要在迭代边界处理的插话或打断记录。它不是外部用户消息 pending，不直接制造新的轮类型；是否进入交互轮仍由入口管线和 `user_message_waiting` heartbeat flag 决定。

---

## 二、三区流转与物流

WB 物流通道：input（收货区，待处理原料，每个任务一个文件夹含 manifest.json 和 payload）→ process（装配区，正在加工，含中间结果）→ output（发货区，处理完等配送，面单含配送目标）。

任务 ID 格式为 T-YYYYMMDD-序号，文件夹命名与任务 ID 同步。物流面单 manifest.json 含 task_id、title、weight、priority、dispatch（auto/manual）、keywords、target（配送目标，容器 ID 或外部路径）、source（declared/rhythm/heat/alert/immune）、created_at、status（input→process→output）、progress。这里的 `source` 是任务来源，不是焦点来源；焦点来源只按 declared/heat/task/alert 四类表达。

---

## 三、焦点机制

焦点来源四种：declared（协议工具声明打开容器，可被抢占）、heat（STM 条目 H≥70 自动浮出，可被抢占）、task（正在执行的任务绑定容器，稳定）、alert（安全事件等级≥3，抢占一切）。`container_focus` 已开通为唯一公开容器焦点协议工具，旧自然语言声明打开/新建容器不再切换焦点。焦点当节持续跨轮保持。用户提到新话题时我判断是否切换，安全弹窗临时抢占处理后回原焦点，已开通焦点协议工具打开或关闭容器时焦点切换或清空，节律点清空焦点但 old_focus 保留。

`container_read` 只读已有容器内容，不改变 `base.focus/old_focus`，不写文件。`container_focus` 的动作集收口为 `open/close/restore`，只处理焦点卫生。新建容器并写首段正文走 `memory_container_create`；已有焦点容器正文续写走 `memory_container_write`，且必须在下一迭代看到 WB 焦点投影后执行。提交工具的同一 response 不要同时调用 `reaction_finalize`，下一迭代再收束。WB status 的 `base.focus/old_focus` 与目标容器 `focus=true/false` 保持单焦点一致。

WB 是 CONTENT 的常驻底层——类比操作系统桌面，焦点容器是打开的窗口。CONTENT 加载什么正文由 focus 决定。WB 常驻底层和 memory.md 显著区始终加载于 CONTENT 底部，焦点内容置其上方。EXPLORER 中焦点所在容器格口保持二级展开。节律点后 STATUSBAR 提示原焦点可恢复。

---

## 四、关注与任务

焦点（focus）、常驻清单（resident_list）和即时清单（instant_list）必须分清。焦点是当前正在读写的容器，0 或 1 个，焦点工具权限，存在 WB status.json 和容器 registry 中。常驻清单是反应步声明跨轮保留的只读正文，只包含记忆条目、工作容器和关系卡正文。即时清单承接起手步挂载、本轮新写入、三重命中和临时材料；被移入常驻清单后不得重复留在即时清单。

任务分轻量和完整物流两种模式。轻量模式临时挂载用完卸载，无三区流转无 task 文件夹无跨步断点，面单目标为外部路径。完整物流走三区流转加 task 文件夹加跨步断点，面单目标为容器 ID，成品写回工作容器。三种终态：暂停保留缓存和断点，中止全删，结束成品写回。

Spec434/479/551/560/729 后，复杂自然语言工程任务可由 Runtime 创建 WB task guide：`task_bootstrap` 先收集任务拆解与验收标准，随后物化为 `process/<task_id>/task_guide.json` 与 `acceptance_ledger.jsonl`。初始字符串清单会规范化为稳定 `item_XX` / `acc_XX` 记录；后续 `guide_submit(task_progress/update_task_status)` 只能更新这些已声明 ID 的结构状态，只接受 `items` 与 `acceptance` 字段，不接受 `notes` 这类 Runtime 不消费的自由备注；若 payload 没有任何结构化 `items` 或 `acceptance` 更新，必须拒绝为 `task_status_update_empty`，不得 accepted no-op。`done/passed` 只接受成功证据；`blocked` 必须同时写非空 reason 和 Runtime 已登记的 evidence ref，失败调用使用 `call:<call_id>` 而不生成 `EV-*`。所有 required items / acceptance 有证据完成后 Runtime 自动撤下 `base.active_guide`；若全部 required 记录均已成功或合法阻塞且至少一项阻塞，Runtime 直接进入 blocked 善后，保留未完成任务供后续恢复，不接受继续 relay。随后反应循环按事务账自然判定：完成就直接自然语言回复用户；需要跨轮继续才单独调用 `reaction_finalize(handoff_text)`；账未闭合时最终回复候选会被 checkpoint 拦截，应继续做真实工作和账本登记。
