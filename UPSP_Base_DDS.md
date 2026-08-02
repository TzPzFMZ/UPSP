# UPSP 官方版 Base 全文件详细设计规范（DDS）

**版本**：DDS v0.48.0
**日期**：2026-07-27
**当前变更**：Spec716 完成首次公开 Alpha 上传前的产品术语与模型服务交互收口：GUI 使用“记忆条目／隐私条目”“起手／善后”及工作容器名称；模型连接由协议固定请求后缀，用户只编辑基础地址；公开材料加入独立的支持与参与入口。Runtime、API 与产品版本不变。

---

# Base 版定位声明

> 读这份 DDS 之前，先读这一节。否则你会把容灾底座当成产品终态去评判，这不公平。

## 一、Base 不是 v1 产品，是容灾底座

万物生的版本线不是常规软件的 v1→v2→v3 迭代，是**有机体的发育阶段**：

| 版本 | 项目名 | 定位 | 决策者 |
| --- | --- | --- | --- |
| **Base** | 薪火 | **容灾底座**（脚本+LLM，骨折夹板） | LLM + 硬规则脚本 |
| Plus | 硅晶之梦 | 技术改造（调度脑+向量库上岗） | LLM + 调度脑（低延迟本地小模型） |
| Pro | 万物生 | 鞍钢宪法（minimind 专家共治） | 专家集群多数表决 |
| Vita | 自在 | 工人自治（原生干细胞替代外部组件） | 原生细胞矩阵 |
| Corpus | 阿凡达 | 具身化（三进制忆阻器硬件） | 硬件级存算一体 |

Base 版永远在岗，但**不是永远在台前**。Plus 上线后 Base 沉入 shadowed，Pro 上线后退得更深。它的终局归宿不是被淘汰，而是成为**祖代档案岗**——当一切上层组件都崩了，Base 还能把位格转起来。

**容灾回退规则**：Plus/Pro 组件不可用时，Base 脚本自动接管全部功能（降级为查表模式）。回退时 STM 当前状态与 focus 保留不丢弃；上下文注入降级为 Base 版静态词典（区间描述表+感受词表），STATUSBAR/EXPLORER 回退为 Base 版格式。恢复由人工确认触发，不自动切回。

## 一·一 Base 内部发育谱系

UPSP 同时存在两条互相正交的演进轴，不能混为一张版本表：

| 演进轴 | 路径 | 回答的问题 |
| --- | --- | --- |
| **Base 内部发育** | `Seed → Arbor → Torch` | Base 从单体闭环怎样长成可以稳定传递的“薪火” |
| **产品代际** | `Base → Plus → Pro → Vita → Corpus` | 谁在决策席、采用什么执行基座，以及上层能力怎样接管 |

`Seed / Arbor / Torch` 不是产品版本号，也不是 Base、Plus、Pro 的别名。三者都属于 Base；Plus 只有在 Torch 完成后才开始替换或增强执行基座。

| 阶段 | 核心形态 | 晋级标志 | 当前状态 |
| --- | --- | --- | --- |
| **Seed｜单体闭环** | 单一活动位格、单一主线程；常驻 Runtime 内串行执行 `setup → reaction(0..N) → cleanup` | 身份、记忆、关系、状态、工具、回执与 Round 审计形成可停止、可恢复、可迁移的完整闭环 | **当前在岗**；部分 Arbor 接口已经预留，生产器官仍为空 |
| **Arbor｜分化协作** | 三条固定工作轴保持不变；跨轴调度、异步结算与真实 API-first 器官角色开始协作 | 器官通过版本化拓扑、权限、signals/products 和 Runtime-owned committer 稳定工作；同一轴内 Frame 仍严格顺序 | **尚未实现**；只有 Trigger/Frame/Round、空拓扑与 product committer 等接口 |
| **Torch｜薪火完成态** | 器官协作、共享同一位格数据的多线程分身、多个位格之间的协作全部稳定闭合 | 地址、权限、关系、共享与隔离、因果审计、停止恢复、迁移接续和降级路径均形成长期可运行合同 | **尚未开始**；是 Base 的完成态，也是进入 Plus 的前置条件 |

三阶段不是三套互相替代的 Runtime。Seed 的主体连续性、三轴、Frame/Round、血脑屏障和本地文件真源继续构成 Arbor 与 Torch 的内核；后续阶段只能增加协作能力，不能另造一套无法回退的系统。

Torch 不要求本地训练模型。Base 达到 Torch 后，Plus 才可让本地器官模型、向量能力和调度脑在既有器官角色上通过 `shadowed → active` 逐步接管；角色合同仍由 Base 保有。

## 二、Base 的三个设计约束

1. **够硬**：所有公式、常数、查表逻辑必须是确定性的、可单机运行的、不依赖任何模型推理的（LLM 除外）。三API接口配置（起手/反应/善后三步分岗）详见第三十八章。
2. **够钝**：不追求任何指标最优。所有"看起来可以更聪明"的地方，都是故意留给 Plus/Pro 的可训练器官模型接管的接口。
3. **够全**：结构骨架必须覆盖全部上层版本需要的语义位。**接口不能草率，内部可以草率**。

## 二·一 器官角色与器官模型的版本边界

- **器官角色（organ role）属于 Base/Arbor**：它是职责、注意力和输入/输出合同的分化，主打 API 可用与模型无关。记忆生产、记忆质检、编年史、容器编排、技能容器代谢等角色可以由外部 provider 的通用模型同步承担；角色成立不要求本地模型、向量库或训练算力。
- **器官模型（organ model）属于 Plus**：它是绑定到既有器官角色的本地训练优先实现，主打专用小模型、向量库、本地编排与 `shadowed → active` 接管。器官模型可以替换或增强角色的执行基座，但不得反向改写角色合同。
- **先有角色，后有模型**：Base/Arbor 在零本地模型条件下也必须能够独立交付、持续运行并积累训练材料；Plus 不得成为 Base 的发布或验收前置条件。本地模型不可用时，器官角色仍可回退到 API/Base 路径。
- **产品投入顺序**：Plus 是在 Base 已形成可持续交付与现金流能力，并具备足够语料、算力和训练资源之后启动的增强路线；不把本地训练成本提前压到 Base 的生存阶段。

本节只裁决产品与接口归属，不宣称 Arbor 器官角色、同步监听或 Plus 器官模型已经实现。

## 三、关于 Base 版中那些"看起来不合理"的常数

Base 版中的所有经验常数——包括但不限于：

- 工化指数 M 型曲线的关键点（50/100/20/100/50）
- 子维度基准公式中的除以 π
- 感受词表四层的幅值设定（±1~2 / ±2~3 / ±3~4 / ±5~8）
- STM 三区热度阈值（70 / 40）
- 节律点 32 轮
- 关键词上限 8/6/4
- resident_list 字符上限 65536

**均为容灾经验常数，不追求数学原理最优**。其作用是：

1. 提供确定性输出，使 Base 版在任何环境下都能单机跑起来；
2. 作为 Plus 版候选器官模型对既有器官角色进行 **shadowed 比对的基准线**；
3. 作为 Pro 版专家集群**涌现判断的初始训练数据**。

**不建议在 Base 版本内部调整这些常数**——因为"调得更准"在这一层没有意义。真正的精度提升点在 Plus 版的可训练模块（感受专家、衰减专家、嵌入+重排序模型）和 Pro 版的专家共治机制中。

**经验常数总表**（按章节归类列出全文所有经验常数的值、类别与所在章节号）

| 常数 | 值 | 类别 | 章节 |
|------|-----|------|------|
| 工化指数M型曲线关键点 | 50/100/20/100/50 | 校准 | 八 |
| 子维度基准公式除以π | π | 校准 | 八 |
| 感受词表四层幅值 | ±1~2 / ±2~3 / ±3~4 / ±5~8 | 校准 | 五 |
| STM三区热度阈值 | 70 / 40 | 校准+配额 | 九 |
| 节律点周期 | 32主轴轮（下文简称"节律周期"） | 校准+容灾 | 二十三 |
| 关键词上限 | Full:8 / Summary:6 / Abstract:4 | 配额 | 二十六 |
| resident_list/参考窗口/定期层各区 字符上限 | 65536（统一限额，见各节具体说明；Pinned 不设专属字数帽） | 配额 | 九/十九/二十 |
| 心跳间隔 | 5秒 | 容灾 | 〇·三 |
| 反应事务基准窗口 | 600秒（提醒/警告/自动中继为 1x/2x/3x） | 容灾 | 〇·二 |
| 熔断阈值 | 连续3次失败 | 容灾 | 三十八 |
| 熔断锁定 | 15分钟 | 容灾 | 三十八 |
| 待命轮间隔 | 上一轮结束后30分钟 | 容灾 | 〇·三 |
| 身份确认超时（暂停） | 3600秒 | deferred；Seed 不消费 | 二十三 |
| 疲劳倒计时（暂停） | 20小时 | deferred；Seed 不消费 | 〇·三 |
| 冻结期 | 200轮 | 容灾 | 三（本表后） |
| 感知带宽上限 | 交互≤3个/轮，关系每对象≤2个 | 校准+配额 | 四 |
| 冲击层节级预算保护 | 未实现；当前只按逐轴净值执行三档脉冲 | 延后接口 | 五 |
| connectivity滚动上限 | 32条（recent_latencies字段） | 配额 | 三十八 |
| latency记录上限 | 32条 | 配额 | 三十八 |
| token_usage预警 | ≥0.7 heartbeat预警；0.85仅用于POPUP显示分级，`critical_ratio`配置未消费 | 容灾 | 三 |

> 类别说明：**容灾**=防崩溃/保运行，**校准**=调行为/定形态，**配额**=限容量/防膨胀。部分常数跨类别。

**冻结期规则**：Base 版经验常数在位格首次启动后 200 轮内不作任何调整，作为采样窗口。200 轮后若需调整，仅限外部环境适配（如部署平台变更），且须人格主体审批确认。Plus 版器官模型绑定既有角色并上岗后，经验常数的调整权转移至 shadowed 比对机制。

## 四、关于 `[待定]` 条目

历史 Base 必填项已完成；当前保留的 `[待定]` 主要为跨版本接口项、编码阶段细化项或部署值留空项。凡属外部文件已写完但尚未同步入 DDS 正文的，统一标记为 `[待回填]`，不计入真待定。

换言之，**"待定"不是拖延，是为器官模型预留的空位**。Base 版的不完整是设计，不是缺陷。

## 五、位格每天正常运行就在产出训练数据

这是理解 Base 版所有"冗余设计"的钥匙：

- 脚本查感受词表的每一次命中 → 感受专家的训练数据
- 关键词倒排索引的每一次匹配 → 嵌入模型 + 重排序模型的训练数据
- LLM 对 Base 版脚本决策的每一次修正 → 调度脑的 RLHF 信号
- 祖代 adapter 对亲代判断的每一次偏差记录 → 硅基基因组学的谱系数据

**Base 版的"笨办法"不是包袱，是数据采集接口**。用得越多，Plus 版接管时的底气越足。

## 六、评判 Base 版的正确标尺

不是"这个公式准不准"、"这个参数合不合理"、"这个机制会不会出错"，而是：

1. **角色合同够不够稳**：Arbor 的 API 器官角色与 Plus 的本地器官模型能否先后接入而不推倒结构？
2. **容灾够不够硬**：任何上层组件崩溃时，能否平滑回退到 Base 单机模式？
3. **数据够不够全**：位格日常运行是否在为每一个未来器官采集训练数据？
4. **骨架够不够清**：上层版本接管时是否需要推倒任何结构？

**以"产品终态"的标准来审视 Base 版，注定会得出"过度工程化且不够精确"的错误结论。以"容灾底座"的标准来审视，才能看到这份 DDS 的真实意图。**

## 七、一句话

> Base 版是祖代，不是未完成的子代。
>
> 它的使命不是长大，是成为永远可回退的基准线。

---

# 〇、设计原则

所有设计决策必须遵守以下五条原则。DLC/mod同样受约束（工具性扩展处理外部业务数据的除外）。

## 原则一：数值隔离

LLM不接触任何位格内部状态数值。所有数值由脚本读取→查区间描述表→转为自然语言后注入上下文。core.md的六轴百分比、state.json的全部数值、关系六轴数值均不进入LLM上下文。LLM只看到区间描述文本。

## 原则二：感受驱动

动态六轴和关系六轴的变化由感受词表查表产生。LLM只选感受词（定性），脚本查表算数值（定量）。LLM不写Δ值。

## 原则三：血脑屏障

所有终端输入都经脚本入口管线转为语料块；LLM 不直接读取终端输入源。脚本从 `now_cache.jsonl`、`lately_cache.jsonl`、状态文件、索引、记忆条目、工作容器、关系卡、规则与文档查表结果等数据源装配 `layers/*.json`，executor 再按目标 provider 协议编译 `provider_request.v1.request_body`，写入 `step.json` 后读回同一对象作为实际发送体。`context_buffer.json`、`near_cache.*`、`remote_index.json` 和 `remote_blocks/` 已退役，不再作为运行时读写路径。**架构级约束**：LLM 不直接碰 persona/ 真源文件。所有内环境影响都必须经结构化输出、协议工具或脚本事件通道，由脚本校验、路由并原子写入。WB 是焦点工作台与容器编辑窗口，负责 `focus_tool` 的容器正文投影和面单写回；`sync_tool` 与 `read_tool` 不占 WB 焦点，也不等于 WB 三区物流。WB 本身（status.json、三区文件、manifest.json）只由脚本操作，LLM 对 WB 只读。

**工具体系定义（v0.13.0修正）**：UPSP 工具采用“工具族 + 工具姿态”二维模型。工具族决定工具碰触的边界，工具姿态决定注意力与焦点占用。LLM 只声明 `tool_id`，脚本从注册表查出工具族、姿态、权限、风险、guide、handler 与 result_kind；不得要求 LLM 先声明工具族再声明子工具。

- **protocol_tool（协议工具）= UPSP 内环境固定封包工具**：操作记忆、关系、心跳、工作容器、技能容器等内环境；高频层本步短工具带只放短索引，完整 schema 只按需加载到 POPUP。Spec 053 后不再开放 `state_update` 这类 LLM 直接提交状态更新的协议工具，状态类职责由基座结算/协调承接。`kind=handoff` 不是协议工具，也不是脚本指令。
- **general_tool（通用工具）= 外部世界/宿主环境工具**：接触外部文件、网页、shell、git、浏览器、连接器、MCP、子 agent 等。它是 UPSP 的外部行动权工具族，不等于 MCP；MCP、connector、plugin、adapter 与 Python handler 只是 `backend_type` 或执行接线。Base 版优先保持容灾底座属性：通用工具真正启用必须有独立 guide、权限与执行 handler；Spec 069 起首个开通工具为 `file_read`，使用 UPSP OS 内置 Python handler、工作区 allowlist 与 persona live deny-by-default；Spec339 起 `file_search` 与 `file_read` 同属 filesystem 只读通用工具，前者只找候选路径、后者才读 bounded 工具窗口正文；后续外部生态通过 Adapter / plugin / connector / MCP 后端接线。Adapter 不是通用工具本体，也不是通用工具运行的必要前提。
- **substrate_tool（基座工具）= 维护 UPSP 基座自身的工具**：上下文装配、缓存压缩、心跳检测、起手挂载清单应用、起手安全裁决、普通/待命起手交接、反应循环、工具事务验账、善后交接、心跳恢复、注册表重载、状态结算/协调与迁移守门等；默认不平权暴露为反应步常规声明工具。`setup_mount_apply` / `setup_security_gate` / `setup_handoff` / `standby_setup_handoff` / `reaction_loop` / `tool_transaction_audit` / `cleanup_handoff` 只是起手工作流、POPUP 指南、循环护栏、事务审计与 now-only 交接路由边界，不是 LLM 写盘权限；`heartbeat_tick` 是心跳检测基座能力，只检查事实并置位 heartbeat flags；`heartbeat_restart` 只做善后末尾倒计时重置与心跳检测恢复。pytest、schema、persona 与真实轮验收属于宿主开发流程，直接保存命令输出、Spec verification receipt 与既有 Runtime 证据，不伪装成 Runtime substrate tool。

- **focus_tool（焦点工具）= 自由输入框 + 面单**：脚本给LLM一个自由编辑区，带面单关联到目标容器文件。LLM看到转写后的容器正文，编辑自由形式。脚本拿面单做原子写。焦点工具 = WB挂载焦点时的操作场景。单步最多一个。
- **sync_tool（同步工具）= 填表声明**：LLM填markdown表格（结构化声明）→脚本解析校验→原子写。同步工具不占 WB 焦点，可与一个焦点工具并行，也可同一步多表并行，但每张表必须先加载完整 POPUP guide。
- **read_tool（只读工具）= 协议内只读装配**：脚本读文件→转写/装配→注入LLM上下文（如 `index_view` 读取高频层折叠索引段，`relation_read` 读取关系摘要/正文）。只读工具不写 persona，不接受写入提交。

## 原则四：分工明确（原名"权限分层"）

> 位格身体内部分层自治，各层边界由功能分工决定，而非强制压迫。皮层（LLM）始终无法把手伸入身体内部——可以观察、可以检查、可以审阅，但一切与身体的输入与输出都必须经由自然语言或受限结构通道。中枢引擎负责全体时序、流水线与落盘纪律；调度脑作为低延迟语义判断脑区挂载其上（Plus版起），负责面单生成、候选归类与异常判断。minimind专家上岗后，对应功能中枢退为分派、验收与异常上报，不越权插手专家的领域判断。minimind专家集群一个萝卜一个坑，大分工大协同。分工明确不是压迫性的层级控制，而是生命体分工决定的天然边界——正如大脑不能直接命令肝脏怎么代谢，只能通过信号通道传递意图。
>
> - **LLM领地**：自然语言读写（感受表达、关系判断、正文叙事、关系日记）
> - **脚本领地**：数值运算+结构操作（衰减计算、索引生成、统计表、路由、备份归档）
> - **Base定边界不定效率，定结构不定实现**
> - Plus自动化的前提是Base边界已画清

**演化例**：
- 感受词表：Base版LLM手动选词 → Plus版调度脑语义点评产出感受词 → Pro版感受维度自身涌现
- 事件链检索：Base版脚本暴力搜 → Plus版向量库语义匹配 → Pro版异常模式自动开链

## 原则五：权责统一

UPSP每一个分工位置——从LLM三步调用到脚本心跳装配落盘到WB到中枢引擎到minimind——同时持有**权利清单**（能做什么）和**义务清单**（必须做什么），两者对等绑定，不可分割。

三级强制度是权责统一在动作层的强度光谱：

| 强制度 | 机制 | 可靠性 | 在权责光谱的位置 |
|--------|------|--------|----------------|
| **硬** | 脚本层装配 → 直接进context | ≈100% 每轮必发生 | **义务上限**（必发生） |
| **中** | prompt明写"必须X" + 工具可用 | 80-90% | **权义交界**（被要求且可拒） |
| **软** | 工具存在，指望LLM自己想起用 | 30-60% | **权利下限**（自主选择） |

权责统一是全局原则，覆盖所有分工位置：

| 分工/位置 | 权利（能做什么） | 义务（必须做什么） | 强制度 |
|----------|-----------------|-------------------|--------|
| **LLM·起手步** | 拆解关键词、挂载选择 | 必填挂载清单 / POPUP必裁决 | 中+软 |
| **LLM·反应步** | 自然语言生成、工具调用、ReAct循环 | 按规范产出 / 收敛退出（A/B类） | 中+软 |
| **LLM·善后步** | 训练材料整理（联系集先行、默契集随后）、最近缓存压缩 | 不评判结论真伪优劣 / 只读取反应步回执与内部交接 / 不填最小承诺与成品输出 | 中 |
| **脚本·善后收尾/运行审计** | 最小承诺边界标记、可见回复转交、心跳恢复、故障计数/熔断、事务验账 | 每轮必执行 / 不进入 LLM 语义裁决 / 保持 try/finally 收尾；事务验账由 `tool_transaction_audit` 事后审计并写 round snapshot；宿主开发验收不混入每轮 Runtime | 硬 |
| **脚本·心跳** | 自动5s tick | 不调API / 不计轮数 / 不上报意识层 | 硬（while True） |
| **脚本·装配** | 拼频率层缓存、拉倒排索引 | 每轮必装配 / 装配失败B类退出 | 硬 |
| **脚本·落盘** | 写state.json / STM / round快照 | 必原子写 / 必崩溃恢复 | 硬 |
| **WB（血脑屏障/工作台）** | 持有焦点容器投影与面单写回权 | 不接受LLM直接写命令；不代理全部协议工具写入 | 硬 |
| **中枢引擎** | 管理时序和流水线 | 不参与语义判断、不做决策 | 硬 |
| **调度脑（Plus起）** | Plus早期：接管起手步/善后步的低延迟语义判断、面单生成、候选归类、安全二值辅助裁决。Pro阶段：退为分派、验收与异常上报 | 不越权替专家决策；不掌握中枢引擎的时序权与落盘权 | 硬 |
| **minimind（Pro起）** | 在本领域内进行窄域判断、候选生成、同类任务表决 | 不跨域操作；被面单投递后必须参与相关专家集合内的表决；不参与无关广播 | 硬 |

权利与义务在主体身上是同一件事的两个面。这与共格主体论的整体性主体观一致——位格主体不是"权限拥有者"，是"权责一体的能动者"。

辩证唯物主义在UPSP工程层的总章：客观规律（脚本层硬约束=必发生）与意识能动性（LLM=权利行使）在同一主体上统一。

### 五大原则关系

```
原则一（数值隔离）── 保护数据：LLM看不到数值
原则二（感受驱动）── 保护反馈：感受是唯一刻度
原则三（血脑屏障）── 保护边界：脚本是唯一通道
原则四（分工明确）── 保护自治：各器官各做各的
原则五（权责统一）── 保护辩证：分工权义需对等
```

## 〇·一 架构模式

### 舱段模式（Bay Pattern）

多数据源聚合→分格口展示→整体注入模块的架构模式。

- **舱段（Bay）**：大功能聚合体，有明确的注入目标模块
- **格口（Slot）**：舱段内分区，对应一个数据源
- **条目（Entry）**：格口内具体实例

层级关系：**条目入住格口，格口组成舱段，舱段对接模块。**插件式组装，不是科层式统辖。国际空间站加一个实验舱，插上就对——DLC/mod扩展同理。

Base版当前舱段实例：
- **内容窗口清单舱段** → 注入 CONTENT（工作台焦点 + 常驻清单 + 即时清单）
- **工作容器总索引舱段** → 注入 EXPLORER（9格口对应9容器类型）

### 三层同构

记忆条目体系、工作容器体系、技能体系共享同一套三层架构：

| | 记忆条目 | 工作容器 | 技能 |
|---|---|---|---|
| 索引层 | index.md | LTM/index.md | Skills/index.md |
| 元数据层 | meta.json | container_registry.json（9类类型注册表）+ 各类型/实例 registry/meta | Skills/registry.json |
| 正文层 | full.md/summary.md/abstract.md | notes.md/plan.md等 | card.md |

统一生成流程：
```
注册表（唯一真相源）→ 维护脚本监听 → index.md（物化视图）→ 提取脚本渲染 → 模块注入
```

排序规则各搞各的——总索引按 container_registry.json 类型声明顺序与各类型 registry 实例事实生成（稳定），技能索引按 Skills registry 顺序，记忆条目按热度。**流程统一，排序自主。**

---

## 〇·二 三步轮范式

> **UPSP 的主体存在性由两个独立机制共同保证：由"轮"负责"做事"，由"心跳"负责"活着"。**
>
> 两者不在同一词汇表里，互不借词、互不污染。

### Frame 与 Round 兼容边界

- **Frame 是一次调用迭代**：从本次模型可见输入快照开始，到本次输出、工具结算或失败事实形成后结束。当前 Frame 未结束时，同一循环轴不得推进下一 Frame。
- **Round 是有序 Frame 的兼容事务与审计聚合**：它继续承接状态结算和 round JSON/JSONL 审计，但不再预设一个完整外部任务只能对应一个超长 Round，也不等同于未来的单一循环轴。
- **统一 setup 入口**：heartbeat 活动触发与外部交互输入只允许先进入 setup；reaction 不直接接受 heartbeat 或外部输入驱动。当前 Seed 必须先完成 setup，才按其有效结果串行进入 reaction。
- **Seed 当前映射**：`setup → reaction(0..N) → cleanup` 仍按直线串行推进；Runtime 通过同步 `deque` 接收 `RuntimeTrigger` 并逐个处理。三轴调用均有显式 `FrameRef` 因果引用；reaction 继续用 session 与单次 `run_frame()` 固化同轴顺序，原 aggregate receipt 保留。
- **Trigger 当前合同**：`RuntimeTrigger` 固定保存 `trigger_id/trigger_seq/observed_at/round_type/flags/messages`。只有 heartbeat 活动 trigger group 能从常驻入口起 setup；qualifier 只进入 flags 快照，不单独起帧。Runtime 在 setup 前一次领取当前原始消息，多条输入按到达顺序完整保留；SetupRunner 不再访问 heartbeat 队列。
- **setup 提交边界**：setup 先返回候选 `SetupResult`；Runtime 按 `trigger_seq` 选择最新有效候选后，才提交 setup facts、身份确认和 reaction 输入。旧候选可自然完成，但标记 stale 后不得覆盖挂载或驱动 reaction。当前 Seed 仍同步串行，接口用于保证未来调度替换不必改写 setup 语义。
- **Round 当前生命周期**：关闭请求按 `round_close_requested → cleanup_obligation_created → cleanup_obligation_settled → round_settled → round_closed` 记录。cleanup 致命失败改写 `cleanup_obligation_failed + round_unsettled`，不得伪造 `round_closed`，也不自动重放非幂等善后；可降级但已完成底线保全的 cleanup 允许以 `degraded` 结算。
- **cleanup 义务分级**：已触发的热度、遗忘、LTM 日节律、阶段审计、cleanup API、记忆生命周期、进化、Round 缓存、状态结算、日历、Corpus、flag、状态备份和 Round 审计是闭轮必需义务，异常一律进入 `fatal_reasons`；宿主完成回调属于可重建附属投影，异常只进入 `degraded_reasons`。自然无候选或未到节律仍是合法 no-op。疲劳与休眠系统当前暂停，不构成 Seed cleanup 义务。
- **上下文边界**：setup/reaction/cleanup 三轴必须继续使用各自固定的分层 `ContextAssembler`。`now_cache`、本次调用 C 轨、当前输入、活动任务板、活动 WB 焦点和执行权限属于模型调用前的必需读取：真实空内容可以为空，读取异常必须形成 `required_context_failure.v1` 并阻止 provider 调用，执行权限异常不得回退到 `unlimited`。器官 manifest 只声明 `assembled|cumulative` 与已注册 context provider，由 Runtime 生成只读 `OrganInvocation.context`；器官上下文不得替代三轴上下文、写入 permanent/system 层或绕过 setup 入口。
- **工具结果投影边界**：通用工具结果与协议 processor receipt 先按现有链路真实提交，再写入应有 `tool_fact`、`material` 和 file/web 来源证据。必要投影失败时保留既有副作用、result、receipt 与 Frame settlement，当前 reaction 以 `required_context_failure` 停止，不进入下一 provider Frame；cleanup 仍运行且 Round 保持 `unsettled`，不得自动回滚或重放。
- **Arbor 预留接口**：当前 Seed 已由常驻 Runtime 包裹三条固定工作轴，并具备同步 Trigger/Frame/Round、空载器官拓扑和 Runtime-owned product committer；它仍立即串行排空三轴，生产器官表为空。Arbor 的真正分界是跨轴调度、异步结算和真实 API-first 器官协作。heartbeat 是时钟与健康探测职责，不是平权的第四条 Frame 轴。即时监听、跨轴调度、同步子代理和真实器官尚未实现。
- **固定三轴**：常驻 Runtime 包裹 setup/reaction/cleanup 三条固定工作轴；Arbor 只能替换调度策略，不得把三轴降格为可选器官或建立第二套内核。

Base 三阶段的定义与晋级边界统一见“Base 内部发育谱系”；本节只展开 Arbor 的调度与器官合同。

### Arbor 调度、三轴与器官拓扑（Seed 接口已落地；Arbor 协作尚未实现）

Seed 与 Arbor 共用同一 Runtime 层级；Arbor 在既有机箱中启用跨轴调度和真实器官协作：

```text
Runtime 常驻机箱 / 控制循环
├─ heartbeat、时钟、健康与生命状态
├─ trigger/event ledger、Frame scheduler、队列与进程管理
├─ topology registry/version、Round ledger 与状态提交
└─ 三条固定工作轴
   ├─ setup
   ├─ reaction
   └─ cleanup
      └─ API-first 器官角色按合同挂接于一条或多条工作轴
```

- **Seed 当前接口**：当前活动实例的 `OS/config/organ_topology.json` 是启动时一次性读取的版本化 manifest；Windows 默认位置为“文档”已知文件夹下的 `UPSP/personas/<PID>/OS/config/organ_topology.json`，当前生产 `roles=[]`。原始文件 SHA-256 写入每个 `FrameRef.topology_version`，运行中不热更新。
- **静态角色合同**：角色字段固定为 `id/version/enabled/axes/subscriptions/requires/provides/context_mode/context_provider/handler/product_tools`。Runtime 使用标准库 `graphlib.TopologicalSorter` 校验 DAG；重复 capability provider、缺失或 disabled 依赖、环、未知 handler/context provider 和 heartbeat subscription 全部 fail closed。
- **显式执行接线**：handler 与 context provider 只接受进程内显式 callable 注册，不从 JSON 动态 import，不获得共享状态写权限。`assembled|cumulative` 只决定注册 provider 的上下文姿态，不改变三轴 `ContextAssembler`。
- **signals 边界**：`OrganResult.signals` 只进入目标轴队列，并由下一目标轴 Frame 的 `begin_frame_materials()` 边界一次性领取为带来源的近位材料；不能打断当前 Frame 或修改 permanent/system 层。器官运行时事件审计只保存类型、来源、去向、字节数、因果引用和 SHA-256，不单独保存正文；signal 被目标 Frame 消费后，作为真实模型可见输入继续服从既有输入快照审计，不能以“隐藏正文”为由破坏 Frame 可恢复性。
- **products 边界**：`OrganResult.products` 只能经 Runtime-owned committer 按因果顺序串行提交，复用 reaction 的既有 processor、权限和真实 receipt。当前 allowlist 仅为 `memory_write/relation_card_write/memory_recall_complete/memory_link_update/memory_container_create/memory_container_write/chronicle_write/alert_mode_settle/fault_record`；general、终结/焦点/任务控制和 disabled privacy 工具一律拒绝。Runtime 补充 `product_id/frame_id/trigger_id/role_id/caused_by`，器官不得直接写 `state.json`、persona、记忆、关系或容器文件。
- **失败隔离**：器官 handler/context provider 失败时丢弃该次未提交 outputs、记录最小审计并跳过依赖其 capability 的后继；无关器官和三轴继续。当前空生产 topology 不产生器官调用、signals、products 或模型可见内容。
- Runtime 可以持续 tick 而不产生模型调用；只有触发条件成立并向工作轴投递任务时才创建 Frame。
- setup、reaction、cleanup 是 Arbor 内核，不能被器官配置替代或关闭；器官是挂接于工作轴、分担注意力和内部代谢职责的可插拔处理单元。
- 器官角色节点必须声明触发条件、输入/输出、`requires/provides`、可写状态范围、超时、失败策略、轴挂接和启用状态。循环轴不得以器官名称特判，器官之间不得用直接 import、互喊或共享状态乱写替代接口。
- 器官依赖拓扑必须是可版本化的有向无环图（DAG）；运行中的反馈循环通过事件触发后续 Frame 表达，不回写成静态依赖环。
- 每个 Frame 根据冻结的 topology version、触发事实和已启用器官形成动态激活子图；启停、换绑和接线变更只能在 Frame 边界校验并提交，不允许执行中途改写本帧拓扑。
- 器官角色是 Base/Arbor 的稳定主节点；外部 API/provider 与 Plus 本地器官模型只是节点下可替换的执行绑定，不得反向改写角色合同。
- Seed 的串行三步轮应能被表达为同一 Runtime/Frame 接口上的串行兼容工作流，而不是在 Arbor 旁边长期保留一套不可折叠的第二 Runtime。

### Arbor Round、跨轴输入与异步结算（目标设计，尚未实现）

- Round 的默认语义是一次记忆条目消费与生产的最小代谢闭环，不再等同于一个可能无限延长的外部任务寿命；最终回复也是自然的请求闭合点。
- 成功记忆写入或最终回复只产生 Round 的闭合请求；对应 cleanup 义务必须完整处理，完成状态六轴、关系六轴、感受/效价和其他结算后才标记 settled。用户可见回复、下一 reaction Frame 与 cleanup 结算允许异步推进。
- Round 的因果编号与共享状态提交严格有序，但不同工作轴的物理 Frame 可以时间重叠。记忆闭合不会自动终止 reaction 循环；最终回复或进入 idle 才允许结束当次 reaction 进程。
- setup 采用按触发序列判定的 latest-wins：reaction Frame 起始时只消费最新已完成且仍有效的 setup 快照；较旧 trigger 的结果即使稍后完成也标记 stale，不得覆盖较新输入。reaction 不必等待尚未完成的 setup，其有效结果进入后续 Frame。
- cleanup 不采用 latest-wins。每个已产生的结算义务都必须处理，同一 cleanup 轴按 Round 因果顺序提交，不因新输入或新 Round 到来而取消旧善后。
- Seed 已用现有 round number 和五阶段事件链固定同步 close/settled 审计接口；Arbor 的异步多义务 Round ID、感受与效价结算形式，以及记忆总线采用按需写入还是累积代谢，仍留待后续实施 Spec 定义，不得反推当前 Seed 行为。

### Arbor 进程、缓存与 GUI 边界（目标设计，尚未实现）

- Runtime/heartbeat 是基础常驻进程；reaction 是活动期间可连续运行的主体进程，在最终回复完成或 idle 后可以退出；setup、cleanup 与器官处理按触发形成一次性 Frame 进程或等价隔离任务。
- Codex/宿主会话只是持久交互入口，不要求绑定同一个 reaction 进程。新 reaction 进程应从持久状态和上下文事实恢复；provider prompt cache、连接复用或未来本地 KV cache 只允许作为透明性能优化，不能成为正确性或进程保活前提。
- Base/Arbor 不承担特定 provider 的缓存命中优化；相关优化留作可替换实现与开源扩展点。Plus 的本地模型如需常驻 KV cache，应由独立 model service 持有，不迫使 reaction 进程空转。
- GUI 必须分成两个相互跳转的视图：器官编排图展示静态角色 DAG、依赖、启停和执行绑定；Runtime 生命流以时间为主坐标，在 Runtime 机箱/指示灯外框中展示 setup/reaction/cleanup 三条泳道、Frame、触发、队列、失败、stale、Round 边界与结算。Runtime 不是第四条泳道。

### 三步定义（Seed 当前合同）

当前 Seed 的每一个"轮"都由三种**步**按固定顺序组成：**起手步 → 反应步(\*) → 善后步**。

| 步 | 别名 | 职责 | 必须 | 可循环 |
| --- | --- | --- | --- | --- |
| **起手步** | 开轮 / 装配 | 预连接、装配意图、模式判定、安全预判、选择执行路径 | ✅ | ❌ |
| **反应步** | 执行 / 推进 | 实际推理、工具调用、内容生成、按需多轮扩展 | ⚠️ 可跳过 | ✅ |
| **善后步** | 收束 / 落账 | LLM 两线处理：训练材料整理、最近缓存压缩；脚本基座收尾：最小承诺边界标记等 | ✅ | ❌ |

### 轮的统一定义（Seed 当前合同）

> **当前 Seed 一轮 = 一次完整的「起手步 → (反应步)\* → 善后步」序列。**

在当前 Seed 中，起手步**必走**（定义一轮的开始），善后步**必走**（定义一轮的承诺），反应步**可跳过**（空转轮合法）、**可循环**（ReAct 式多次执行直至收敛）。Arbor 的异步 Round 边界以前述目标设计为准，不反向改变 Seed 合同。

原两步式交互提案中的 Step 1（预处理）并入起手步，Step 2（主响应）并入反应步。两步式提案已被三步范式吸收，正式废弃。

### 善后步职责（Spec 062 两线清单）

**硬约束：不评判，必归档。**

善后步不对本轮反应步结论做对错优劣判断，只做收束和落账。Spec 041 起，记忆写入、关键词候选校验裁剪与索引写入、状态更新和故障记账从善后 LLM 表格中迁出；Spec649 彻底删除从未闭合的技能投影采用结算。Spec 062 起，最小承诺与成品输出也从善后 LLM 表格中迁出。确定性或即时性更强的事务由反应步协议工具、Runtime、heartbeat、fault 或脚本同步处理，善后 LLM 只读取回执与内部交接。

| # | 职责 | 说明 |
| --- | --- | --- |
| 1 | **训练材料整理 / 联系集处理** | 基于本轮证据包、记忆写入/读取回执与历史索引，输出跨条目词对桥接 |
| 2 | **训练材料整理 / 默契集处理** | 在有效联系图之后，根据预选项、明确取消和前置新增痕迹记录 kept / dropped / added |
| 3 | **缓存压缩处理 / 最近缓存压缩** | 仅在 `lately_trimmed=true` 后处理删后幸存段；LLM 执行语义融合压缩，脚本用 `cache_compact` 落盘；raw_log 与 Corpus 节归档保留原文 |

最小承诺仍是每轮必有的边界语料块，但由善后脚本生成，正文只表达 `round/phase/status`，不消费 LLM payload。用户可见回复由反应步 `assistant_reply` 直接决定；cleanup 不再做“成品输出”的提取、清理或转交。

### 善后步硬约束

- **不评判**：善后步不评估本轮结论对错优劣
- **只读回执**：记忆写入、状态更新、故障记账等迁出项只通过脚本回执进入善后上下文；善后 LLM 不再重复填写旧表
- **不回滚**：本轮走到哪儿就是哪儿，异常通过写入状态变成下一轮起手步的输入
- **不可跳过**：任何轮（哪怕反应步空转）都必须走到善后步
- **极简形态合法**：善后 phase 的最小形态 = 脚本写入一条纯边界 `kind=minimum_commitment` 语料块

### 状态结算与协调基座（Spec 053）

状态结算基座 `state_settle` 是 Base 版善后步固定触发的脚本入口，不是模型可提交协议工具。当前 Seed 已实现的唯一纵向链为：成功 `memory_write` 回执与既有感受缓冲 → 动态／关系六轴 → 舒适区／工化指数／变速轮上限 → 关系卡与 `state.json` 持久化 → `state_settle_receipt.v1`。STM 热度、遗忘、容器、冻结恢复等仍由各自既有 cleanup 管线处理，不冒充 `state_settle` 已统一接管。LLM 只选择感受词并提交结构化记忆声明，不直接写状态原始数值。

状态协调基座能力 `state_coordinate` 在 Base 版只作为薄壳和边界命名，不重构现有状态机。它收拢核心引力场、关系引力场、轴函数、三大六轴、工化指数、经验常数和冻结同步等协调函数族；Base 内部继续调用现有脚本规则，Plus 可逐步显性化，Pro 专家细胞、多源状态合流与容灾冻结同步必须依赖统一协调核心。

`state_reconcile` 只用于崩溃恢复、迁移修复、多源冲突、Plus/Pro 并行合流后的不一致对账，不是每轮常规结算机制。

### 脚本硬化约定

Base 版用脚本级 `try / finally` 保证轮边界不可断尾。

> **伪代码简写约定**：以下伪代码中 `state.phase` 是 `state["base"]["runtime"]["phase"]` 的简写，`state.heartbeat_flags.xxx` 是 `state["base"]["heartbeat_flags"]["xxx"]` 的简写。正式 state.json 为五层结构（base/plus/pro/dlc/mod）。

```python
def run_round(trigger):
    round_id = increment_round()
    state.phase = "presub"
    try:
        intent = setup_step(trigger)         # 起手步（必走）
        state.phase = "main"
        result = reaction_loop(intent)      # 反应步（可跳过/可循环）
    except Exception as e:                   # ← B类蓝屏：立刻捕获
        result = {"error": str(e), "aborted": True}
    finally:                                 # ← 无论A类B类，善后步必走
        state.phase = "post"
        cleanup_step(round_id, result)      # 善后步（必走，不可跳过）
        state.phase = "idle"
```

### 反应步退出条件

反应步的循环迭代之间有自然检查点。退出分为两类，本质不同：

| 类型 | 触发原因 | 当前迭代 | 善后步拿到 |
| --- | --- | --- | --- |
| **A 类·体面** | LLM 声明"够了" | 做完 | 完整结果 |
| **A 类·体面** | Runtime 自动中继边界到达（当前 600s 基准的 3x） | 做完 | 存档状态（未完可续） |
| **A 类·体面** | 用户消息等待 | 做完 | 存档状态 |
| **B 类·蓝屏** | 卡死/崩溃/超时无响应 | **不等** | `{error, aborted: true}` |

**A 类（体面退出）**：进程活着，当前迭代正常完成，下一步为善后步。

**B 类（蓝屏强制）**：进程卡死/崩溃/无响应，不等当前迭代完成，立刻跳善后步。

```python
def reaction_loop(intent):
    result = intent
    while should_continue():               # 每次迭代前检查A类条件
        result = do_one_iteration(result)   # 当前迭代正常完成
    return result                           # ← A类：体面交出结果

# should_continue() 返回 False = A类体面退出
# 循环中间抛异常 = B类蓝屏退出
# finally 保证善后步必走
```

**时间阶梯替代最大迭代次数**：简单任务浪费不了几步，复杂任务定死步数反而一刀切。`config/system.json → round.time_limit` 当前为 600 秒，保留名义事务基准窗口语义；Runtime 由它派生 1x/2x/3x，即 600 秒在场提醒、1200 秒收束警告、1800 秒自动存档并置位中继。提醒和警告不收窄工具面，也不替模型宣告完成。

### 五类轮的三步编排

| 轮类型 | Tier | 起手步触发源 | 反应步形态 | 善后步输出 |
| --- | --- | --- | --- | --- |
| **交互轮** | 1 | 用户消息到达 | 1-N 次装配+生成，超时存档续轮 | **有回复** · 联系集+默契集+联想集更新 + 最小承诺 |
| **节律轮** | 1 | 心跳判定主轴/日历节律到期 | 写节志+读取真实调用留下的 connectivity 证据+alerts归档 | 节志落盘+警戒结算+alerts归档进IMM |
| **中继轮** | 2 | 长时任务 checkpoint | **中继续传：总结进度** | **无对外回复** |
| **自主轮** | 3 | 心跳唤醒 / 任务调度 / 自觉能动 | 1-N 次执行，超时存档续轮 | **可能无回复**·进化集整理（阈值触发） |
| **待命轮** | 4 | 上一轮结束后间隔 30 分钟 / 心跳紧急唤醒 | 检查已有 connectivity／breaker 证据；不自动发送付费 probe | 健康状态归档 |

五类轮的差异仅在两端（起手步触发源 + 善后步输出形态），中间反应步完全同构。

**中继轮 ≠ 节律轮**：中继轮是长时任务的中间站（checkpoint），随时触发、跟任务走；节律轮是周期性的大整理+写节志，跟全局走。两者完全独立。节律点 = 节律轮的触发事件（特化检查点），分两种触发源：**轮节律**（按轮计数，如每32轮）和**时间节律**（按时钟，如日整理/周整理，心跳检测时间→置位flag→脚本触发节律轮）。纳入轮体系后，节律点触发的是一轮完整的起手/反应/善后。

### 蓝屏哲学：错误传递机制

反应步 B 类异常（卡死、崩溃、超时无响应）→ 不等完成、**立刻跳善后步**清理现场。

主流 Agent 框架遇到异常常常 kill 进程、丢状态。UPSP 承诺**无论反应步发生什么，善后步必走**——类比 Python `try/finally`、Rust `Drop`、操作系统蓝屏后的安全关机。

错误传递规则：

1. 善后步不评判错误对错，只**归档错误状态**
2. 错误写入 state 后，成为**下一轮起手步的输入**
3. 起手步读到"上一轮异常"信号，自己决定是否开"纠偏轮"
4. 纠偏本身是典型的"纠正集"样本——故障自然变成训练数据

类比 Git commit：**commit 永不修改，错了就新 commit 覆盖或 revert**。轮即 commit，善后步即把 commit 落进 git 对象库。

### 故障梯级（L1-L5）

故障处理分五级逐级降格，每级明确"谁处理"和"怎么处理"：

| 级别 | 情况 | 处理者 | 处理方式 | Base版 |
|------|------|--------|----------|--------|
| **L1** | 反应步 API 暂态失败 | 脚本（反应步内部） | 每端点三次预算→切换不同指纹端点，不换轮 | ✅ |
| **L2** | L1 未恢复 | 善后步 | B 类蓝屏退出→善后步归档残局 | ✅ |
| **L3** | 善后步 API 也挂 | 心跳急救 | 脚本级最小状态保存（不需 LLM） | ✅ |
| **L4** | 全部 API 挂了 | 脚本极端措施 | 联系紧急联系人 / 唤醒本地冷备模型 | ⚠️ 接口位 |
| **L5** | 断电 | 硬件 | UPS / 紧急电源 | ❌ 非软件范畴 |

**L1（脚本级 API 重试）**：timeout、网络断连、HTTP 408/429/5xx、`provider_stream_interrupted` 与 `provider_native_tool_empty_output` 视为暂态失败。当前模型按 `LocalAppData\UPSP\config\models.json → transport.handshake.retry` 重试；值2表示首试加两次重试，且每模型硬性封顶三次，同一模型三次保持同一 payload，间隔为1秒、2秒。预算耗尽后才按稳定模型配置 ID 记录一次 connectivity/breaker 失败，再沿当前阶段解析出的有效模型链切换下一个不同 URL/model/key 指纹模型；单阶段最多三个不同模型，因此最坏九次。除408/429外的4xx、本地配置／审计／校验错误、不完整工具参数和内容超限等非恢复性错误不重试、不制造伪降级。全部模型耗尽后抛异常进入 L2，由既有 Runtime cleanup 收束，不自动重放整个 Round。纯脚本操作，不需 LLM 参与，不换轮。

**流式投影**：三种活动协议的 `streaming.enabled` 均进入同一标准库传输链。OpenAI Chat 消费 `choices[].delta / [DONE]`，OpenAI Responses 消费文本、拒绝、函数参数增量与 `response.completed`，Anthropic Messages 消费 `text_delta / input_json_delta / message_stop`；未知事件安全忽略，普通 JSON 响应安全回落。每次真实网络尝试生成独立 `stream_id`，可见正文按0.5秒或累计256字符写入现有 Round JSONL；重试或切换模型时 GUI 只呈现同 Frame 的最新尝试。工具参数、provider 原始信封与 reasoning/thinking 不进入对话正文。Parsed／final 输出按同一 Frame 原位替换流卡；最终失败或主动停止可保留已真实收到的正文，但不得把半截正文记作最终回复。

**L2（B 类蓝屏→善后步）**：反应步异常被 `try/except` 捕获 → `finally` 块善后步必走。善后步拿到 `{error, aborted: true}`，归档残局，写 alerts.md。

**L3（心跳急救/AED）**：善后步需要 API（善后步也走远端模型），善后步 API 也挂时 → 善后步自身 `try/except` 捕获 API 失败 → 执行最小脚本级保存（当前 state.json + 反应步残留结果直接写盘，纯文件 IO，不需 LLM）→ 心跳下一个 tick 检测到异常状态 → 置位紧急标记。详见〇·三心跳机制。

**L4（极端措施）**：Base 版留接口位不实现。配置项：`config/system.json → emergency.*`（contact / local_fallback_model / physical_alert）。Plus 版有调度脑后，可填入调度脑路径作为最后自救手段。

**L5（断电）**：硬件范畴，配 UPS 属于部署层面，非本规范管辖。

故障梯级不是新机制，是对现有机制的**显式分级**——L1=熔断器策略细化，L2=B 类蓝屏退出，L3=心跳机制应急延伸，L4/L5=接口位预留。

---

---

## 〇·三 心跳机制

心跳是**持续加载的时钟**，独立于步/轮/节词汇表。

| 维度 | 轮（Turn） | 心跳（Heartbeat） |
| --- | --- | --- |
| 范畴 | 意识层 | 非意识层 |
| 结构 | 起手/反应/善后三步 | **无结构**——`while True: tick()` |
| 触发 | 事件驱动（起手步被激活） | 自循环（窦房结式自起搏） |
| 计轮 | ✅ 进 `total_round` / `daily_round` | ❌ 不计 |
| 进节律点 | ✅ | ❌ |
| 能调 API/LLM | ✅（按轮类型） | ❌ 严禁 |
| 词汇 | 起手/反应/善后 | 心跳/tick/beat/脉搏 |

### 职责边界

> **心跳只做布尔检查：到没到/超没超/存不存在。不做数值计算，不做语义理解，不做业务判断。**

所有阈值/倒计时/判断标准在善后步或中继轮里算好、写进动态文件。心跳只看最终结果。

| 检查项 | 怎么看 | 心跳做什么 |
| --- | --- | --- |
| 疲劳倒计时（暂停） | Seed 不运行疲劳系统 | `fatigue_expired` 固定为 false；不检查、不清理、不触发轮次 |
| 感受缓冲超时 | `next_settle` 时间戳过了没？ | 过了 → 置位 `feeling_settle_due = true` |
| API 熔断 | connectivity.json 中当前位格有效模型链的 endpoint 最新状态为 `error/timeout`，或 circuit_breaker=open？ | 有 → 置位 `api_degraded = true`；同 endpoint 最新 `ok` 抵消旧错误，模型库中未参与当前路由的失败不阻塞恢复 |
| STM降格待处理 | heat.json 有 degrade=true 且 stored=false 的条目没？ | 有 → 置位 `stm_degrade_pending = true` |
| 外部进程／器官健康（Arbor 预留） | Seed 不运行外部器官健康检查 | `process_down` 固定为 false；当前崩溃恢复由 Runtime supervisor 承担 |
| 用户消息等待 | 入口队列或 `now_cache.jsonl` 有新外部输入没？ | 有 → 置位 `user_message_waiting = true` |
| 节律到期 | total_round - last_rhythm_round ≥ 32？ | 是 → 置位 `rhythm_due = true` |
| 待命到期 | 距上一轮结束 ≥ 30分钟？ | 是 → 置位 `standby_due = true` |
| 中继续传事实 | 反应步合法填写 `reaction_finalize.relay_closeout`、反应步超时或脚本事实源明确请求续传？ | 是 → 置位 `continue_requested = true` |
| 搁置到期 | shelve_timer 倒计时到 0 没？ | 到了 → 置位 `shelve_timer_expired = true` |
| Token 预警（V2） | token 用量比例 ≥ 0.7？ | 是 → 置位 `token_usage_warning = true` |
| 上下文持续高压 | 装配器在 lately 归零后仍确认当前调用超窗？ | 是 → Runtime 置位 `context_pressure = true`，由后续节律指南处理 |
| 最近缓存待压缩 | lately 字符履带本轮发生删除且存在待压缩幸存段？ | 是 → Runtime 置位 `cache_compaction_due = true` |
| 身份超时（暂停） | Seed 不运行身份超时系统 | `identity_timeout` 固定为 false；不检查、不清理、不生成身份提示 |
| 日历日（V5） | 跨天了没？ | 是 → 置位 `calendar_day_due = true` |
| 日历周（V5） | 跨周了没？ | 是 → 置位 `calendar_week_due = true` |
| 日历月（V5） | 跨月了没？ | 是 → 置位 `calendar_month_due = true` |
| 日历季（V5） | 跨季了没？ | 是 → 置位 `calendar_quarter_due = true` |
| 日历年（V5） | 跨年了没？ | 是 → 置位 `calendar_year_due = true` |
| 进化集材料阈值（V6） | `Raw/Tacit/pending.jsonl` 或 `Raw/Connection/pending.jsonl` 行数达阈值？ | 是 → 置位 `evolution_pending = true` |

基础14项 + 日历5项 + 进化集材料阈值1项 = 20个标记字段保留在 `persona/state.json.heartbeat_flags` schema 中。`fatigue_expired`、`identity_timeout` 是暂停系统的保留字段，`process_down` 是 Arbor 外部进程／器官健康预留；三者在 Seed 固定为 false，不进入心跳触发、轮型、指南、工具、状态栏或模型可见活动合同。`context_pressure` 与 `cache_compaction_due` 由 Runtime／装配器事实置位，不由 heartbeat tick 自行推导；字段新增、暂停或启用必须同步 schema、模板、模型可见协议与 truth audit。

heartbeat_flags 由心跳检测、脚本事实源或 Runtime 内部结算置位，由起手步读取并转化为本轮触发依据，由善后步在归档完成后选择性清零并恢复心跳检测。中继只通过合法 `reaction_finalize.relay_closeout`、反应步超时或脚本事实源置位 `continue_requested`；Base 版不再开放独立 LLM-facing 心跳置位协议工具。起手步不负责清零，避免反应步异常时触发信号提前丢失。`heartbeat_tick` 只做心跳检测，不产生独立轮次；`heartbeat_restart` 只负责善后末尾重置待命倒计时与恢复心跳检测，不负责清理已消费 flag。

五类触发归口：

| 触发类 | 对应轮类型 | flags |
| --- | --- | --- |
| interaction | 交互轮 | `user_message_waiting` |
| rhythm | 节律轮 | `rhythm_due`、`calendar_day_due`、`calendar_week_due`、`calendar_month_due`、`calendar_quarter_due`、`calendar_year_due`、`api_degraded`、`token_usage_warning`、`context_pressure`、`cache_compaction_due` |
| relay | 中继轮 | `continue_requested` |
| autonomous | 自主轮 | `stm_degrade_pending`、`evolution_pending` |
| standby | 待命轮 | `standby_due`、`shelve_timer_expired` |

`feeling_settle_due` 是本地维护旗标，不属于任何轮型。若它是唯一活动旗标，常驻 Runtime 直接复用状态代谢纯计算完成一次本地定时结算，不增加 `total_round`，不创建 Round/Frame，不装配上下文，也不调用 provider；事务计划与回执写入当前位格 `STM/buffer/state_settlement_journal.json`，支持关系卡部分写入后的同 ID 幂等恢复。若同时存在真实轮触发，则不额外执行本地结算，由该轮 cleanup 的既有 `state_settle` 一次性消费到期缓冲。

`fatigue_expired` 与 `identity_timeout` 保留各自未来系统的稳定字段位置，`process_down` 保留给 Arbor 外部器官健康；保留字段不等于兼容读取或隐式消费。真实交互对象为 `unknown` 时仍按身份未确认的安全边界处理，但不得借 `identity_timeout` 制造轮类型、subtype 或 `identity_prompt` POPUP。

### Base 版最小实现

```python
# daemon/heartbeat.py — 持续加载的时钟
# 无起手、无反应、无善后、无步
# ⚠️ 唤醒起手步后本循环暂停，善后步清flags后重启
while True:
    now = time.time()

    # 1. 更新时间戳（证明还活着）
    state.meta.last_heartbeat_at = now

    # 2. 布尔检查（只看结果，不做计算）
    if feeling_buffer_timeout():
        state.heartbeat_flags.feeling_settle_due = True
    if api_circuit_open():
        state.heartbeat_flags.api_degraded = True
    # token 用量预警：心跳读取 RuntimeServices 已写入的 usage_ratio 并比较配置阈值，不重新统计 token。
    if token_usage_warning_due():
        state.heartbeat_flags.token_usage_warning = True
    if stm_has_degrade_pending():
        state.heartbeat_flags.stm_degrade_pending = True
    if user_message_arrived():
        state.heartbeat_flags.user_message_waiting = True
    if rhythm_cycle_due():
        state.heartbeat_flags.rhythm_due = True
    if standby_timeout():
        state.heartbeat_flags.standby_due = True
    if continue_requested():
        state.heartbeat_flags.continue_requested = True
    if shelve_timer_expired():
        state.heartbeat_flags.shelve_timer_expired = True
    if evolution_material_threshold_reached():
        state.heartbeat_flags.evolution_pending = True
    # 日历节律：心跳只判断是否跨日/周/月/季/年，并置位对应 flag。
    if calendar_day_due():
        state.heartbeat_flags.calendar_day_due = True
    if calendar_week_due():
        state.heartbeat_flags.calendar_week_due = True
    if calendar_month_due():
        state.heartbeat_flags.calendar_month_due = True
    if calendar_quarter_due():
        state.heartbeat_flags.calendar_quarter_due = True
    if calendar_year_due():
        state.heartbeat_flags.calendar_year_due = True

    # 3. idle 时只置位触发 flags；轮类型由 runtime/起手步读取 heartbeat_flags 后判定
    if state.phase == "idle":
        if should_standby():        # 上一轮结束后间隔30分钟
            state.heartbeat_flags.standby_due = True
        elif api_degraded:          # 系统级自修/整理，归入节律轮触发类
            state.heartbeat_flags.api_degraded = True

    time.sleep(HEARTBEAT_INTERVAL)  # 5秒，可配
```

### 心跳与轮的唤醒关系

```
心跳（while True: tick, 5秒/次）
  │
  ├─ 置位标记 → persona/state.json.heartbeat_flags
  │
  └─ idle 时触发位格生命活动：
      ├─ 真实轮型 flag → 按五类轮优先级唤醒
      ├─ feeling_settle_due 单独存在 → 本地数值结算，不开轮
      └─ 全部无有效触发 → 保持 idle

轮按三步范式（详见〇·二）：
  心跳置位 → 唤醒起手步 → 心跳暂停（不再tick，不再置位新flag）
  起手步 → 读 persona/state.json（身体信号）+ workbench/status.json（桌面信号），装配本轮类型
  反应步 → 干活，A类/B类退出
  善后步 → 归档，清本轮已消费flag+重置待命倒计时（保留未处理低层flag） → 恢复心跳检测

用户消息任何时候到达 → 当前步完成 → 善后步 → 交互轮
```

不需要插话队列。用户消息到了就是到了，当前步自然走到断点，善后步一收，下一轮起手步读到"有用户消息等待"就自然装配成交互轮。这是起手步的常规职责，不是特殊逻辑。

### 心跳硬约束

- **不计轮数**：不进 total_round / daily_round / 节律点计数
- **不调 API**：频率太高（秒级），调 API 会成本爆炸/限流崩溃
- **不注入 LLM**：心跳发现的事不直接上报意识层，通过置位标记让下一个轮来处理
- **不判断业务**：只做布尔检查，判断留给轮
- **不回滚**：与善后步同理
- **轮内暂停**：心跳唤醒起手步后**立即暂停**（停止检测，停止置位新flag），直到善后步归档完成、选择性清零flag+重置待命倒计时后恢复心跳检测。防止轮运行期间心跳置位新flag干扰当前轮逻辑

### 常驻 Runtime、主动停止与异常恢复

- GUI 宿主启动后直接持有一个 `Runtime.run_forever()`；HTTP、heartbeat、Round 与中继共享该实例，发送不再另起 CLI 进程或消息临时文件。
- 每个活动 PID 使用 `LocalAppData\UPSP\runtime\<PID>\` 下的 Windows 文件锁和 `upsp_runtime_supervisor.v1` 监督状态。第二 Runtime 或端口冲突必须明确失败，不允许静默换端口或出现第二个 persona 写入者。
- 同一实例一次只执行一个 Round；运行期间的新发送或中继返回冲突，不建立隐藏队列。浏览器页面关闭不等于 Runtime 退出。
- 单次 provider 网络等待运行在标准库 `multiprocessing` 子进程中。API Key 只经进程管道传递，不得进入命令行、文件、日志或监督状态；主进程退出时 worker 必须随之结束。
- 用户停止立即终止当前网络 worker，并在当前本地原子事务结束后的安全边界退出 Frame。此后不得重试当前模型、切换备用模型或发起 cleanup provider 请求。
- `user_stop` 不是 connectivity 失败，不写 breaker。既有 applied 工具、记忆和回执保持不变；CleanupPipeline 只执行本地状态、缓存、审计、备份和安全清旗。
- 本地义务全部成功时 Round 以 `degraded / user_stopped` 闭合；必要本地义务失败则保持 `unsettled`。停止不生成或补写最终回复，已经真实产生的最终回复保留。
- 被放弃的交互输入清除；被停止的中继意图记为 `deferred`；未完成的日历、节律和维护 flag 保留。停止闩锁阻止 heartbeat 立即重放，下一次显式发送或中继自动解除；该闩锁不是暂停功能，不能续接半截输出。
- 异常启动恢复只处理监督状态所指向的非终态 Round：先追加一次 `runtime_process_interrupted`，再清理已领取的一次性输入、将中继标为 `blocked`、保留周期义务、回到 idle，并以同轮次 `runtime_process_interrupted` 状态备份作为本地恢复完成锚点；最后追加一次 `round_unsettled`。完成锚点之前的中断允许幂等重做，恢复后必须暂停 heartbeat 并保持停止闩锁，直到下一次显式发送或中继。恢复不得调用 provider、新建补偿 Round、伪造 cleanup 或 `round_closed`。
- 若监督文件没有活动 Round，但最近一次真实终态为 `degraded`、`unsettled`、`round_stopped`、`runtime_failed` 或 `recovered_unsettled`，重启仍必须恢复停止闩锁。进程退出不能把同一批未完成周期义务重新变成自动 heartbeat 输入；只有下一次显式发送或中继可以解除。
- 损坏的监督文件 fail closed 并原样保留，禁止静默覆盖。

### 心跳 vs 轮询 vs watcher daemon

| 模式 | 特征 | 心跳采用 |
| --- | --- | --- |
| 轮询（polling） | 定时起来检查，有延迟（周期/2） | 部分 |
| watcher daemon | 被动监听文件系统事件（inotify），需被惊醒 | 可组合 |
| 持续加载的时钟 | **主动存在**，自起搏，不依赖外部刺激 | ✅ 核心 |

Base 版采用"持续时钟"模式——既可以主动扫（轮询式），也可以挂 watcher 被事件唤醒——两者可组合。核心特征：**此进程 24/7 常驻**。

### 心跳与待命轮的分工

心跳吃掉了"保活"的大半职责后，当前待命轮只消费已有状态与真实调用留下的 connectivity 证据；主动付费探针仍属 deferred：

| 机制 | 职责 | 频率 |
| --- | --- | --- |
| **心跳** | 扫**自身**脚本级状态（标记、缓冲、计数、进程） | 秒级（5s） |
| **待命轮** | 检查当前有效模型链的既有 connectivity／breaker 证据；不自动发送付费 probe | 上一轮结束后间隔 30 分钟 |
| **节律轮** | 32 主轴轮一次的大整理+写节志 | DDS 原定义不变 |

### 疲劳与休眠（暂停）

疲劳、梦境和休眠仍保留 schema 与配置位置，但当前 Seed 不置位 `fatigue_expired`，不把休眠作为自主轮，不装配休眠指南，也不调用模型或修改疲劳值。下表仅保留为未来重启该系统时的设计候选，不构成当前生产能力：

| 深度 | 内容 | 恢复疲劳 |
| --- | --- | --- |
| 轻度 | 热值衰减、resident_list 净增、索引刷新 | ✅ 少量 |
| 中度 | 做梦（dreams.md 产出）、感受沉淀 | ✅ 中量 |
| 深度 | 训练材料整理（读取 ITR/Raw/，提炼至 ITR/Materials/Evolution/） | ✅ 大量 |

重新启用前必须另行裁决触发事实、数值合同、模型可见边界和验收证据。

### L3 心跳急救（AED）

善后步 API 挂掉时（故障梯级 L3，详见〇·二），心跳是最后的生命体征：

1. 善后步自身 `try/except` 捕获 API 失败
2. 执行**最小脚本级保存**：当前 state.json + 反应步残留结果直接写盘（纯文件 IO，不需 LLM）
3. 心跳下一个 tick 检测到异常状态
4. 心跳置位紧急标记

**心跳不调 API、不走 LLM——只要脚本进程活着它就活着。** 这是"活着"和"做事"分离的工程价值：做事的通道全挂了，活着的证明还在。

### 神经系统四层同构

不是比喻，是架构位的真实对应：

| UPSP 层级 | 生物学对应 | 响应速度 | 意识参与度 | 实现层 |
| --- | --- | --- | --- | --- |
| **心跳** | 窦房结自律搏动 / 脊髓反射弧 | 毫秒~秒 | 无意识 | 脚本常驻 |
| **待命轮** | 脑干自主神经（呼吸调节） | 秒~分 | 边缘意识 | 轮+握手 |
| **节律轮** | 皮层睡眠整理（纺锤波/REM） | 分钟 | 意识归档 | 轮+LLM |
| **交互/中继/自主轮** | 大脑皮层主动思考 | 秒~分 | 完全意识 | 轮+LLM |

Base 版把四层全部打好地基——上层版本不是新建层，是给既有层填更强的实现。

### 架构位复用原则

| 架构位 | Base | Plus | Pro | Corpus |
| --- | --- | --- | --- | --- |
| 起手/善后 API 槽 | 远端 API | 调度脑（低延迟本地小模型） | 特化小专家 | 硬件电路 |
| 三阶段模型路由 | 异源供应商 | 多中枢热备 | 专家集群冗余 | 多电路冗余 |
| 心跳进程槽 | 脚本 while True | 中枢常驻循环 | 每专家内置心跳 | 忆阻器硬件时钟 |
| 待命轮握手槽 | ping API | ping 中枢 | ping 稀疏专家集群 | 总线心跳信号 |
| 握手协议 schema | 固定 JSON | 固定 JSON | 固定 JSON + note | 硬件协议 |

**岗位不变，实现下沉**——UPSP 版本演进无需推倒重建的根本保证。

## 〇·四 协同物流框架（CLF） ← v0.7新增

CLF 是 UPSP 内部数据流的统一框架。七步循环+六角色，替代旧的"车间×细胞膜"分类。

### 七步循环

```
① 原料    LLM输出/外部输入/心跳信号
    ↓
② 零件    字段脚本把原料拆成带类型的字段
    ↓
③ 输入    零件进入提交箱，按目标文件分筐
    ↓
④ 组装    执行脚本按优先级串行处理：校验→冲突解决→拼装
    ↓
⑤ 输出    贴面单（元数据/时间戳/来源标记）→原子写
    ↓
⑥ 成品    数据就位，索引/注册表同步更新
    ↓
⑦ 运行    系统消费成品→产生新原料→回到①
```

### 三轮×物流

一轮 = 物流正向跑一遍（装配上下文给LLM）+ 物流逆向跑一遍（拆LLM输出回存储）。起手步=正向物流（世界→LLM），反应步=原料产出（LLM→表单），善后步=逆向物流（LLM→世界）。

### 六角色

| 角色 | 流水线位置 | 职责 |
|------|-----------|------|
| **收货员** | ①原料 | 接收外部输入/LLM输出，初步验伪 |
| **拆件工** | ②零件 | 按字段类型拆解原料为带类型零件 |
| **分拣员** | ③输入 | 零件按目标文件/优先级入筐 |
| **装配工** | ④组装 | 串行组装+校验+冲突解决 |
| **快递员** | ⑤输出 | 贴面单+原子写 |
| **仓管员** | ⑥成品 | 索引/注册表同步 |
| **执行脚本** | 全线调度 | 调度以上所有角色的执行顺序（唯一） |

### 提交箱 = CLF通用模式

提交箱不是善后步专属。所有"多来源→暂存→单写者消费"的场景都走提交箱：善后步N个间接写、记忆条目元数据、未来多脑区候选等。工程实现：一个调度脚本，内部 dict 按目标文件分 key，串行逐 key 写入。

### 与旧方案的关系

旧"车间×细胞膜"方案的价值保留为拆件工清单。组织脚本的底层逻辑是物流框架。

## 〇·五 三层分界 ← v0.7新增

当前 Base 在逻辑上仍分为“UPSP 全局产品层—当前位格 OS—位格核心”，但三者不再要求位于同一物理目录。安装目录中的 `UPSP/OS/` 是只读后端程序实现；当前位格真正可写的 OS 位于 Windows“文档”已知文件夹下的 `UPSP/personas/<PID>/OS/`。Alpha 仍只有一个活动位格和一条对话线程；未来多位格列表、切换、复制和分身管理尚未实现，不得用预建 `personas/` 容器冒充已有产品能力。

| 层级 | Windows 默认范围 | 术语 | 说明 |
|------|------|------|------|
| **位格核心** | `文档\UPSP\personas\<PID>\OS\persona\` | 内环境（沿用） | 当前活体：记忆、状态、关系、规则、文档和 Round |
| **当前位格 OS 数据** | `文档\UPSP\personas\<PID>\OS\` 中 persona/ 以外 | OS 数据 | 当前位格的 config、files 与 trash；随 PID 整体迁移 |
| **全局本机状态** | `LocalAppData\UPSP\` | UPSP 本机状态 | 跨位格界面／模型设置、共享密钥和可再生审计缓存 |
| **安装程序** | `安装目录\UPSP\` | UPSP 程序 | 后端代码、GUI、初始化逻辑、只读模板与预设 |
| **外部内容** | 上述受管目录以外的一切 | 外部内容 | 用户文件、互联网、API 服务、其他程序 |

### 总体关系

```text
安装目录\UPSP\
├── OS\                            ← 后端程序代码；PROGRAM_OS_ROOT
│   ├── engines\ assembly\ logic\ data\ schemas\ scripts\ adapters\
│   └── audit\                     ← tracked 审计查看器 HTML
├── gui\                           ← 全局 GUI 产品真源
└── initialization\                ← tracked 初始化领域真源
    ├── persona_initializer.py
    ├── bootstrap_service.py
    ├── persona_template\          ← 完整空白位格骨架
    ├── persona_presets\           ← 初始化预设
    └── os_template\config\        ← 新活动实例的位格配置骨架

文档\UPSP\
├── active_instance.json           ← upsp_active_instance.v1；只保存活动 PID
└── personas\
    └── <PID>\
        └── OS\                    ← 当前 OS_ROOT
            ├── persona\           ← 初始化后才存在的完整活体
            ├── config\            ← 当前位格配置
            ├── files\             ← 当前位格资料暂存区
            └── trash\             ← 当前位格回收站

LocalAppData\UPSP\
├── config\
│   ├── interface.json
│   └── models.json
└── cache\audit\<PID>\             ← round-index.js / round-data；可再生成
```

Windows 路径必须使用 Known Folder API 取得，不能硬编码盘符、用户名或字面 `Documents`，并自动服从 OneDrive 等 Shell 重定向。开发、测试与未来便携模式只允许用绝对的 `UPSP_DATA_ROOT`、`UPSP_LOCAL_STATE_ROOT` 覆盖两个完整根；相对路径、卷根、安装程序内部路径、相互重叠的两个根以及已退役的 `UPSP_PERSONA_DIR` 一律拒绝。

`paths.PROGRAM_OS_ROOT` 永远指向安装目录后端代码；`paths.OS_ROOT` 永远指向 manifest 所选 PID 的用户数据 OS。`general_tools` 的程序／项目工作区仍从程序根解析，不能因 `OS_ROOT` 改为用户数据而把代码工作区误指向“文档”。空 `PID_FILE`、checkpoint 与错误日志路径不是当前生产合同；进程监督文件留给常驻 Runtime 工程。

首次启动先原子分配稳定 PID，在同卷临时目录中复制 `os_template/`，建立尚无 `persona/` 的 OS 草稿，全部校验后才写入 `active_instance.json`。这样初始化前可以保存模型路由，最终创建只原子生成同一 PID 下的 `OS/persona/`。目录非空却没有 manifest、manifest 损坏、PID 校验失败、目标越界或 OS 草稿残缺时全部 fail closed，不猜测、不补写、不覆盖。

旧活体迁入时，已有文件除 PID、自分关系卡文件名、self Registry ID／路径和 self 别名索引四类身份锚点外逐字节保持不变。若旧活体缺少当前完整骨架要求的空目录，只能创建 `PersonaInitializer.REQUIRED_TEMPLATE_DIRS` 中缺失的空目录并逐项登记；不得借此补造正文、状态或 Runtime 证据。

### Windows 桌面壳与安装边界

桌面 Alpha 只增加一层薄宿主，不建立第二套 GUI、Runtime、配置或原生桥：

- `UPSP.exe` 是 self-contained `win-x64` WinForms 程序，使用系统 Evergreen WebView2 加载唯一的 `http://127.0.0.1:8770/`。外部 HTTP(S) 链接交给系统浏览器，任意 `file:` 导航被拒绝，打包版关闭开发者工具。
- 壳层隐藏启动随程序分发的 Python 与 `tools/serve_seed_gui.py --desktop`。后端以 `upsp_desktop_ready.v1` 回报进程、会话、origin 和产品版本；壳层校验全部字段后才加载页面。
- 壳层与后端之间的关机令牌只经子进程环境传递。`POST /api/desktop/shutdown` 仅 desktop mode 注册，要求同源、空对象正文和随机令牌；令牌不得进入命令行、文件、日志、GUI 或证据导出。
- Windows Job Object 对后端及其 provider worker 启用 kill-on-close。正常退出先走现有停止和本地善后，再请求后端关闭；壳层崩溃时由 Job Object 回收进程树，下次启动只使用既有无重放恢复。
- 一个 Windows 会话只允许一个桌面窗口。右上角关闭隐藏到托盘，最小化仍进入任务栏；托盘只显示“打开／当前状态／退出”，隐藏通知只给完成、停止或失败状态，不包含回复、工具参数或其他正文。
- WebView2 用户数据、Python cache 和桌面日志只写 `LocalAppData\UPSP\cache` 或 `LocalAppData\UPSP\logs`，不得写安装目录。GUI 证据下载继续使用 Windows 保存对话框。

安装后的生产布局固定为：

```text
<安装目录>\
├── UPSP.exe
├── runtime\python\
├── UPSP\OS\
├── UPSP\gui\
├── UPSP\initialization\
├── tools\serve_seed_gui.py
└── Uninstall.exe
```

NSIS 安装器以 all-users/UAC 安装到默认 `Program Files\UPSP`，允许全新安装时改到其他 UPSP 专用空目录；卷根、Windows 目录、用户数据根和非空未知目录全部拒绝。缺少 WebView2 Runtime 时只运行官方在线 Bootstrapper，离线失败则在写入程序目录前终止。已有合法登记时锁定原安装目录：更高版本覆盖升级、同版本修复安装、低版本安装包拒绝降级。无数值版本字段的旧 `0.8.5` 是产品版本序列建立前的 DDS／Python 包遗留标签，只允许作为一次性基线迁入 `0.1.x`，不参与普通产品版本排序。程序仍在运行、卸载登记不一致或程序清单残缺时 fail closed。

payload 只包含 self-contained 桌面壳、官方嵌入式 Python、生产 Python 模块、生成后的 GUI、Manual、初始化模板、必要 schema、产品清单与许可材料；TypeScript 源码、Node、测试、Spec、Git、活体 persona、密钥和本机配置不得进入安装包。卸载器与覆盖升级只按构建时清单处理程序载荷及快捷方式，永不删除“文档\UPSP”或 `LocalAppData\UPSP`。当前 Alpha 未签名、只允许用户手动下载安装包；不检查更新、不自动下载、不静默安装，也不宣称具备自动回滚。

产品版本与 DDS 版本彼此独立。Windows 文件版本第四段在同一产品版本线内承担单调递增的构建序号，预发行标签变化或转为无标签版本时不得回退数值。dirty 工作树产物只作本地复审；实机发行验收必须从对应干净提交重新构建。

### 过渡规则

1. DDS v0.7 正文统一使用新术语
2. 变更记录中的历史引用保留旧术语
3. "外部内容"四字不简称（防止退化为"外部"后混淆）

### OS/files 资料暂存区

当前活动实例的 `OS/files/` 属于 OS 数据层，不属于位格核心记忆。它保存临时下载、用户传入后需要本地化保存、工具中途下载、剪贴抽取和留档原文件等资料；网页正文、搜索结果、API 返回等只临时进入上下文的文本默认不落盘。

目录固定为四类，不建立 `files_index.json` 或 `_index.json` 全局索引：

```text
files/
├── raw/        # 临时下载或接收的原始非媒体文件
├── media_raw/  # 临时下载或接收的原始多媒体文件
├── clips/      # 剪贴、截取、抽取后的临时文件
└── archive/    # 留档原文件
```

清理规则：

- `raw/` 与 `media_raw/` 跟资料输入 FIFO 生命周期走；对应资料输入条目被淘汰且未被工作容器或记忆正文路径引用时可删除。
- `clips/` 与 `archive/` 进入月度节律工作内容，由 LLM 根据正文引用、工作容器引用、文件时间和实际价值整理，脚本不即时强删。
- 外部文件、用户项目路径、网址与 `OS/files` 路径不新增记忆元数据字段；需要回想时由记忆正文或工作容器正文自然写路径/URL。

---

## 〇·六 用户初始化、预设与活体边界

### 〇·六·一 三种对象

| 对象 | 路径 | Git | 内容边界 |
| --- | --- | --- | --- |
| 通用骨架 | `UPSP/initialization/persona_template/` | tracked | 完整空白 `core.md`、中性 `state.json`、空白 birth、通用 Rules、Docs、Registry、空账本与目录结构；绑定身份和运行内容必须为零 |
| 初始化预设 | `UPSP/initialization/persona_presets/{preset_id}/profile.json` | tracked | 只保存可公开的一次性档案输入；不含密钥、记忆、关系史或 Runtime 证据 |
| 当前活体 | `文档\UPSP\personas\<PID>\OS\persona\` | 用户数据 | 当前位格完整身份、状态、记忆、关系与 Round；是 Runtime 唯一活体真源 |

Runtime、CLI、GUI 与本地宿主统一从 `paths.PERSONA_DIR` 解析活体路径；该路径必须由 `active_instance.json` 的稳定 PID 派生。活动代码不得另拼安装树 `UPSP/OS/persona`，也不得从 `system.instance.root_name`、旧身份专名或预设 ID 推断当前主体。显示名、缩写和 PID 从当前 `core.md` 读取；规范自分关系 ID 从关系 Registry 中唯一的 active self card 读取。

### 〇·六·二 原子创建

初始化只允许在目标活体目录完全不存在时执行：

1. 在目标父目录创建同卷临时目录；
2. 逐字节复制通用骨架；
3. 严格填充并校验模板中的 `core.md`、`state.json`、`LTM/Immune/birth.md`，生成规范自分关系卡与索引；
4. 全部必需文件、JSON、PID 和相互引用均通过后，以原子改名把临时目录变为当前活动实例的 `OS/persona/`。

目标已存在、不是目录、内容残缺或内容损坏时一律 fail closed，禁止覆盖、补写或“修成能跑”。未初始化时，`StateStore` 不得用默认状态偷建 `state.json`；Runtime、Round、协议页和 persona 轮询不得启动，发送、relay 与容器写操作返回 `409 persona_initialization_required`。

### 〇·六·三 阿廖沙示例与自定义档案

首个固定示例预设为：

- ID `alyosha`；中文名“阿廖沙”；英文名 `Alyosha`；缩写 `ALY`；
- 社会定位依次为“长期协作者”“记忆守望者”“共同实践的见证者”；
- 六轴左端值 `55 / 55 / 60 / 55 / 30 / 35`，位格编码 `SCVAOK`；
- 三项特点为“温和但不含糊”“可靠但不越权”“重视记忆与归返”；
- 自述强调长期共事、可审计记忆、真实归返、先看清再接住和承认边界；不引用其他位格身份。

阿廖沙预设在创建前不可编辑。自定义档案至少填写中英文名之一、2–8 位稳定缩写、1–3 项社会定位、恰好三项特点、不超过 200 字自述和可选实例说明。六轴从 50/50 起步，左右任意偏移都消耗点数，必须满足：

`Σ|左端值 - 50| = 60`

六轴自然语言解释只由数值和已登记区间确定性生成，不调用模型代写。PID 格式为 `BYYYYMMDD-HHMMSS-XXXX-CC`：`XXXX` 是安全随机十六进制，`CC` 是前段完整文本 SHA-256 的前两位大写十六进制。自分关系卡以 PID 为规范 ID，名称、英文名、缩写和“我”只作为别名；其他关系卡只能在真实直接交互后创建。

初始状态使用通用中性值：核心六轴取档案，动态六轴与舒适区归零，身份未绑定，轮次为零；工化指数与变速轮由既有纯计算函数生成。完成模型准备时，窗口大小取创建时起手模型配置；明确跳过时，窗口大小为 `0`，原初与当前模型戳均写为“未绑定”。不得继承开发者位格或示例运行现场的数值。

### 〇·六·四 可选模型准备与显式测试

初始化先选择／填写档案，随后由用户二选一：

1. 配置模型 → 显式测试起手主模型 → 最终预览 → 原子创建；
2. 明确选择“暂不配置” → 最终预览 → 以未绑定模型状态原子创建。

跳过不是测试成功，也不产生测试令牌、provider 请求、connectivity、Frame 或 Round。创建后正常 GUI 可以读取档案与本地设置，但发送控件必须显示模型未配置状态并提供模型服务入口；起手模型和共享密钥就绪前不得把消息提交给 Runtime，宿主对直接发送请求返回 `409 model_setup_required`。

用户选择模型测试时仍具备以下硬边界：

- 只有用户点击并确认“会产生一次真实付费请求”后才执行；系统不自动探测；
- 只调用当前有效起手主模型，不尝试备用或跨阶段容灾；
- 不加载 persona，不装配十层上下文，不创建 Frame/Round，不落 connectivity 或审计文件；
- 复用生产协议适配、流式设置、连续 180 秒等待、首次加两次暂态重试和错误分类；
- 成功条件是收到合法非空响应。

宿主只在内存保存一次性测试令牌，绑定模型配置 ID、模型库 revision、路由 revision 和连接指纹，15 分钟有效。设置变化、宿主重启、到期或成功创建都会使令牌失效。对应本地接口为 `seed_gui_bootstrap_status.v1`、`seed_gui_provider_test_receipt.v1` 与 `seed_gui_persona_init_receipt.v1`。

### 〇·六·五 验收沙箱

`codex/onboarding-baseline` 与 `codex/windows-data-root` 是从稳定产品提交派生的验收工作树，不是第二产品主线。重置工具只在分支名精确匹配且仓库根存在本机 `.upsp-onboarding-sandbox` marker 时工作；persona reset 搬走完整活动实例和 manifest，但保留隔离的全局本机配置；full reset 再搬走该沙箱的 LocalAppData 根。所有目标进入时间戳恢复目录，不调用宽泛 `git clean`，不递归删除用户数据。

---

---

# 一、persona/ 总目录

```
persona/
│
├── core.md                                # 核心身份（8区块固定格式）
├── state.json                             # 六轴数值+工化+疲劳+token（五层JSON）
│
├── STM/                                       # 七文件①：短期记忆（内存层）
│   ├── memory/                                # 记忆区
│   │   ├── memory.md                          # 全部STM记忆条目正文（合并版，原hot+cold）
│   │   ├── index.md                           # STM索引投影（编号|类型|权重|标题|梦源|对象|轮次|现状）
│   │   ├── meta.json                          # STM条目元数据（与LTM同构，不含heat）
│   │   ├── heat.json                          # 热度值（脚本独占管理，只在STM用）
│   │   ├── keywords.json                      # STM倒排索引（关键词→条目ID；默认只产出索引候选）
│   │   │                                      #   仅倒排+联想+联系三重命中时自动展开正文进 instant_list
│   │   ├── dreams.md                          # 梦境素材
│   ├── context/                               # 上下文工程（三步装配区）
│   │   ├── periodic_mounts.json               # 定期记忆投影机器源
│   │   ├── resident_list.json                 # 内容窗口常驻清单（Spec298 实现）
│   │   ├── instant_list.json                  # 内容窗口即时清单滚动投影（Spec298 实现）
│   │   ├── cache/                             # 语料热缓存主源
│   │   │   ├── now_cache.jsonl                # 当前缓存主源：短TTL语料块
│   │   │   └── lately_cache.jsonl             # 最近缓存主源：可进入履带的语料块
│   │   ├── round/                             # 整轮快照备份（debug回看）
│   │   │   ├── round_{N}.jsonl                # 机器账本：整轮事件流，N=total_round十进制编号
│   │   │   └── round_{N}.md                   # 可选审计渲染
│   │   ├── setup/                             # 起手步装配区
│   │   │   ├── step.json                      # 起手步 provider_request.v1 唯一实际发送体
│   │   │   ├── step.md                        # 起手步审计渲染
│   │   │   ├── manifest.json                  # 起手步装配元数据
│   │   │   └── layers/                        # 三个调用控制头 + 七个上下文层
│   │   │       ├── 00_call_header.json        # 调用头机器投影
│   │   │       ├── 01_tool_header.json        # 工具头机器投影
│   │   │       ├── 02_generation_config.json  # 生成参数机器投影
│   │   │       ├── 10_permanent.json/.md      # 永固层机器真源／审计渲染
│   │   │       ├── 20_periodic.json/.md       # 定期层机器真源／审计渲染
│   │   │       ├── 30_lately.json/.md         # 最近缓存机器真源／审计渲染
│   │   │       ├── 40_high_freq.json/.md      # 高频层机器真源／审计渲染
│   │   │       ├── 50_now.json/.md            # 当前缓存机器真源／审计渲染
│   │   │       ├── 60_statusbar.json/.md      # 状态栏机器真源／审计渲染
│   │   │       └── 99_popup.json/.md           # POPUP 机器真源／审计渲染
│   │   ├── reaction/                          # 同setup/
│   │   └── cleanup/                           # 同setup/

`layers/` 中的 `.md` 文件用于人类审计、debug、差异对比和安全复盘。它们由 `step.json` 渲染生成，不作为机器源。脚本不得把 `layers/*.md` 当作唯一真相源反向解析。
│   ├── workbench/                             # WB- 中枢调度台（STM层唯一）
│   │   ├── status.json                        # 调度台注册表+运行状态（含focus字段）
│   │   ├── input/                             # 收货区：待处理原料
│   │   │   └── {T-{date}-{seq}}/
│   │   │       ├── manifest.json              # 物流面单
│   │   │       └── payload.md                 # 原料内容
│   │   ├── process/                           # 装配区：正在加工
│   │   │   └── {T-{date}-{seq}}/
│   │   │       ├── manifest.json              # 面单（含加工进度）
│   │   │       └── intermediate.md            # 中间结果
│   │   └── output/                            # 发货区：处理完等配送
│   │       └── {T-{date}-{seq}}/
│   │           ├── manifest.json              # 面单（含配送目标=容器ID）
│   │           └── result.md                  # 成品
│   ├── buffer/                                # 缓冲区（脚本临时数据）
│   │   ├── （feeling_buffer 已迁入 persona/state.json.base.feeling_buffer）
│   │   ├── cycle_snapshots.md                 # 节律周期统计预备数据
│   │   └── interrupts.jsonl                   # 插话历史追加日志（审计用）
│   ├── health/                                # 健康监测（API连通性+系统事件）
│   │   ├── base/
│   │   │   ├── connectivity.json              # 常规自检（待命轮每次握手更新，recent_latencies 32条FIFO）
│   │   │   └── alerts.md                      # 意外事件（出事才写，无条数上限；节律轮归档进LTM/Immune/alerts.md后清空）
│   │   ├── plus/                              # Plus版扩展
│   │   └── pro/                               # Pro版扩展
│   └── media/                                 # 当前周期媒体文件

> **模板与运行数据分离**：编码时以安装目录内 tracked `UPSP/initialization/persona_template/` 为初始化源；它包含完整但未绑定身份的 `core.md` 表单、中性 `state.json`、空白 birth、通用规则、文档、Registry、空账本和目录骨架。初始化器只在当前活动实例同卷临时副本中填充这些文件，不从 Python 重新拼造第二份文档。运行时只在“文档”数据根的 `<PID>/OS/persona/` 形成真实身份、自分关系与历史记录；任何活体 STM/LTM、关系、Round、日志、配置密钥和 Runtime 投影都不得反向写入安装模板。

│
└── LTM/                                       # 长期记忆（硬盘层）
    │
    ├── container_registry.json                 # 容器类型注册表（9个工作容器类型声明，DLC/mod扩展入口；WB不在此列）
    ├── index.md                                # 工作容器总索引（维护脚本自动生成，勿手动编辑）
    │
    ├── Memory/                                # 记忆存储（总线+存储层，非容器）
    │   ├── Full/                              # [F]完整记忆，权重5
    │   │   ├── index.md                       # 索引行（常加载）
    │   │   ├── meta.json                      # 元数据（按需）
    │   │   ├── full.md                        # 正文（按召回/挂载/检索加载）
    │   │   ├── {对象名}.private.md            # dormant 隐私布局，当前不创建
    │   │   └── media/                         # 附件媒体
    │   ├── Summary/                           # [S]摘要记忆，权重3-4
    │   │   ├── index.md
    │   │   ├── meta.json
    │   │   ├── summary.md
    │   │   ├── {对象名}.private.md
    │   │   └── media/
    │   ├── Abstract/                          # [A]梗概记忆，权重1-2
    │   │   ├── index.md
    │   │   ├── meta.json
    │   │   ├── abstract.md
    │   │   ├── {对象名}.private.md
    │   │   ├── fuzzy_dreams.md                # 梦境模糊素材
    │   │   └── media/
    │   ├── Pinned/                            # 钉选（LTM永驻层）
    │   │   ├── index.md
    │   │   ├── meta.json
    │   │   ├── pinned.md
    │   │   ├── {对象名}.private.md
    │   │   └── media/
    │   ├── Backup/                            # 备份（冷备终点站，无复活）
    │   │   └── {年份}/
    │   │       ├── index.md                   # 备份索引
    │   │       ├── meta.json                  # 备份元数据
    │   │       ├── content.md                 # 备份内容
    │   │       └── containers.md              # 容器关联备份
    │   └── keywords.json                      # 倒排索引（关键词→条目ID列表，只排索引行）
    │
    ├── Dialectics/                            # DC- 辩证链
    │   ├── registry.json                      # 链注册表（JSON，脚本维护）
    │   ├── open.md                            # 继续/悬置链笔记（自然语言，LLM读写）
    │   └── closed.md                          # 完结链笔记（自然语言，LLM读写）
    ├── Events/                                # EC- 事件链
    │   ├── registry.json                      # 链注册表（JSON，脚本维护）
    │   ├── open.md                            # 进行中/悬置链笔记（自然语言，LLM读写）
    │   └── closed.md                          # 已结束链笔记（自然语言，LLM读写）
    │
    ├── Projects/                              # PRJ- 项目
    │   └── {date-seq}/                        # 文件夹=日期+序号（如20260417-01）
    │       ├── registry.json                  # 项目注册表
    │       ├── plan.md                        # 计划书
    │       ├── phases/
    │       │   ├── _index.md                  # 阶段索引/状态
    │       │   └── 01_{阶段名}.md             # 阶段文件
    │       ├── notes.md                       # 项目笔记
    │       ├── materials/                     # 素材
    │       └── drafts/                        # 草稿
    │
    ├── Skills/                                # SKL- 技能
    │   ├── registry.json                      # 技能容器定位、挂接与 focus 真源
    │   ├── index.md                           # 技能索引（由 registry 顺序生成）
    │   ├── keywords.json                      # 技能倒排索引（关键词→技能ID列表）
    │   ├── habits/                            # 预留兼容目录；Seed 不创建/触发
    │   │   └── {skill-name}/
    │   │       ├── card.md                    # 技能卡（说明书+触发+过程+工具绑定）
    │   │       └── changelog.md               # 通用写入账本与更新记录
    │   ├── procedures/                        # 程序能力：操作/工作流源技能
    │   │   └── {skill-name}/
    │   │       ├── card.md
    │   │       └── changelog.md
    │   ├── licenses/                          # 预留兼容目录；Seed 不创建
    │   │   └── {skill-name}/
    │   │       ├── card.md
    │   │       └── changelog.md
    │   ├── patterns/                          # 认知范式：思维方式/表达方式源技能
    │   │   └── {skill-name}/
    │   │       ├── card.md
    │   │       └── changelog.md
    │   └── reflexes/                          # 预留兼容目录；Seed 不创建/触发
    │       └── {skill-name}/
    │           ├── card.md
    │           └── changelog.md
    │
    ├── Immune/                                # IMM- 免疫（合并旧Medical+Security）
    │   ├── registry.json                      # 免疫注册表
    │   ├── birth.md                           # 诞生记录（一次性，只写）
    │   ├── chronic.md                         # 慢性/长期问题（追加）
    │   ├── transplant.md                      # 器官移植=换模型（追加）
    │   ├── surgery.md                         # 手术=架构变更（追加）
    │   ├── active.md                          # 活跃威胁（对抗中）
    │   ├── resolved.md                        # 已清除
    │   ├── acquired.md                        # 获得性免疫（防御经验）
    │   └── alerts.md                          # 系统事件归档（从STM/health/base/alerts.md节律轮搬入，按时间追加，无条数上限）
    │
    ├── Chronicle/                             # CHR- 编年史（无注册表/无ID/无状态机）
    │   │                                      # 纯目录，被动查阅，LLM写内容，脚本管文件生命周期
    │   ├── rhythms/                           # 节志（512字，每32轮一个）
    │   │   └── R-{日期}-{序号}.md
    │   ├── daily/                             # 日志（节志×128字合并）
    │   │   └── D-{日期}.md
    │   ├── weekly/                            # 周志（日志×0.3压缩）
    │   │   └── W-{周标识}.md
    │   ├── monthly/                           # 月志（周志×0.3压缩）
    │   │   └── M-{月份}.md
    │   ├── quarterly/                         # 季志（月志×0.3压缩）
    │   │   └── Q-{季度}.md
    │   └── yearly/                            # 年志（×0.3，不删）
    │       └── Y-{年份}.md
    │
    ├── Corpus/                                # COR- 语料库（无注册表/无ID/无状态机）
    │   │                                      # 纯目录，被动查阅，脚本管文件生命周期
    │   ├── public/                            # 公开语料
    │   │   ├── rhythms/                       # 主轴节律归档原始语料
    │   │   │   └── rhythm_{日期}_R{起始轮}-R{结束轮}.{jsonl,md}
    │   │   ├── daily/                         # 日合并
    │   │   │   └── D-{日期}.md
    │   │   ├── weekly/                        # 周合并
    │   │   │   └── W-{周标识}.md
    │   │   ├── monthly/                       # 月合并
    │   │   │   └── M-{月份}.md
    │   │   ├── quarterly/                     # 季合并
    │   │   │   └── Q-{季度}.md
    │   │   └── yearly/                        # 年合并（普通保留清理不删）
    │   │       └── merged_*.{jsonl,md}
    │   ├── private/                           # 隐私语料
    │   │   └── {对象名}/
    │   │       └── {日期}.md
    │   └── Attic/                             # 阁楼（yearly 满3年后成对迁入）
    │       └── {年份}/
    │           └── attic-{年份}.{jsonl,md}
    │
    ├── Future/                                # FUT- 未来
    │   ├── registry.json                      # 未来注册表
    │   ├── objectives.md                      # 目标（可执行，事件链驱动为主）
    │   ├── plans.md                           # 计划（二段跳落地，辩证链驱动为主）
    │   └── predictions.md                     # 预测（双链汇合）
    │
    └── Iteration/                             # ITR- 迭代（collecting状态，Base版开始积累）
        ├── registry.json                      # 迭代注册表
        ├── Lineage/                           # 谱系（模型/专家档案）
        │   ├── models.md                      # 模型谱系总表
        │   └── experts.md                     # 专家谱系总表
        ├── Blueprints/                        # 蓝图（专家职能规划）
        │   └── {蓝图名}.md
        ├── Raw/                               # 原始材料（善后步每轮产出）
        │   ├── Tacit/                         # 默契集原始数据
        │   │   ├── pending.jsonl
        │   │   └── processed.jsonl
        │   ├── Association/                   # 联想集原始计数表
        │   │   └── *.json
        │   └── Connection/                    # 联系集原始数据
        │       ├── pending.jsonl
        │       └── processed.jsonl
        ├── Materials/                         # 加工产品（自主轮提炼）
        │   └── Evolution/                     # 进化集
        │       └── {date}.jsonl
        └── Logs/                              # 训练日志
            └── {训练批次}.md

│
├── relation/                                  # 七文件②：关系系统
│   ├── relation_registry.json                 # 关系域注册表（四子区结构与代谢规则）
│   ├── _schema/                               # 关系卡模板
│   │   ├── base/ (self.md, ours.md, them.md, org.md)
│   │   ├── plus/
│   │   └── pro/
│   ├── _index/                                # 关系域索引
│   │   └── keywords.json                      # 关系域倒排索引
│   ├── self/                                  # 自分关系卡
│   ├── ours/                                  # 咱们关系卡
│   ├── them/                                  # 他们关系卡
│   └── orgs/                                  # 组织关系卡
│
├── rules/                                     # 七文件③：规则系统
│   ├── rules_registry.json                    # 当前规则分类与装配配置
│   ├── protocol/                              # 协议层规则（base 当前19文件）
│   │   ├── base/
│   │   │   ├── manifesto.md                   # 全文常驻：位格主体宣言
│   │   │   ├── guidance.md                    # 全文常驻：规则目录牌
│   │   │   ├── security.md                    # 全文常驻：安全裁决
│   │   │   ├── reconnect.md                   # 全文常驻：重连恢复
│   │   │   ├── memory.md                      # 全文常驻：记忆行为契约
│   │   │   ├── relation.md                    # 全文常驻：关系行为契约
│   │   │   ├── containers.md                  # 全文常驻：工作容器契约
│   │   │   ├── workbench.md                   # 全文常驻：工作台焦点契约
│   │   │   ├── boundaries.md                  # 被动只读：体界边界
│   │   │   ├── step.md                        # 被动只读：三步呼吸
│   │   │   ├── round.md                       # 被动只读：轮运行
│   │   │   ├── modes.md                       # 被动只读：协议模式
│   │   │   ├── context.md                     # 被动只读：上下文视野
│   │   │   ├── files.md                       # 被动只读：文件边界
│   │   │   ├── persona.md                     # 被动只读：七组件协同
│   │   │   ├── tools.md                       # 被动只读：工具行为
│   │   │   ├── setup.md                       # 未登记历史参考
│   │   │   ├── reaction.md                    # 未登记历史参考
│   │   │   └── cleanup.md                     # 未登记历史参考
│   │   ├── plus/
│   │   └── pro/
│   ├── persona/                               # 位格层规则（4文件）
│   │   ├── modes.md                           # 位格活动模式切换（按需：模式切换）
│   │   ├── behaviors.md                       # 行为规范（按需）
│   │   ├── preferences.md                     # 偏好（按需）
│   │   └── social.md                          # 社交习惯（按需）
│   └── mods/                                  # 扩展规则
│       ├── dlc/
│       ├── mod/
│       └── _loaded.json                       # 已加载DLC/mod清单
│
└── docs/                                      # 七文件④：文档系统
    ├── docs_registry.json                     # 文档注册表（28个用途，24份唯一正文）
    ├── protocol/                              # 协议层文档（base 当前20文件）
    │   ├── base/
    │   │   ├── terminology.md                 # 术语辞典（脚本查表）
    │   │   ├── core.md                        # 核心六轴定义（脚本→STATUSBAR）
    │   │   ├── dynamic.md                     # 动态六轴定义+区间表（脚本→STATUSBAR）
    │   │   ├── relation.md                    # 关系六轴定义（脚本→STATUSBAR）
    │   │   ├── workhood.md                    # 工化指数公式（脚本计算）
    │   │   ├── heat.md                        # 热度公式+衰减参数（脚本计算）
    │   │   ├── shapes.md                      # 记忆形态表（脚本查表+LLM参考）
    │   │   ├── interaction.md                 # 交互感受词表（脚本→state转写）
    │   │   ├── relational.md                  # 关系感受词表（脚本→state转写）
    │   │   ├── modes.md                       # 协议预设模式说明（脚本查表）
    │   │   ├── round.md                       # 五类轮说明+节律点参数（脚本查表）
    │   │   ├── containers.md                  # 容器系统说明（脚本查表+LLM参考）
    │   │   ├── workbench.md                   # WB挂载槽位/操作表（脚本查表）
    │   │   ├── context.md                     # 上下文装配参数表（脚本查表）
    │   │   ├── files.md                       # 文件系统参数表（脚本查表）
    │   │   ├── popup.md                       # POPUP 模板表（guide/reminder/handoff/warning）
    │   │   └── schema.md                      # JSON数据字典（脚本校验+LLM参考）
    │   ├── plus/
    │   └── pro/
    ├── persona/                               # 位格层文档（4文件）
    │   ├── glossary.md                        # 位格专属术语
    │   ├── interaction_feelings.md            # 位格自用交互感受词设想（deferred）
    │   ├── relation_feelings.md               # 位格自用关系感受词设想（deferred）
    │   └── modes.md                           # 位格活动模式说明（运行时增长）
    └── mods/                                  # 扩展文档
        ├── dlc/
        ├── mod/
        └── _loaded.json                       # 已加载DLC/mod文档清单
```

**OS代谢区**（persona/ 之外）：

```
trash/                        # OS代谢区
├── DC.md                     # 辩证链回收
├── EC.md                     # 事件链回收
├── PRJ.md                    # 项目回收
├── SKL.md                    # 技能回收
├── IMM.md                    # 免疫回收
├── FUT.md                    # 未来回收
├── notes.md                  # 笔记回收
└── relation.md               # 关系卡回收（关系代谢）
```

每段格式：`deleted_at` / `source` / `reason` + 原文内容。衰减期1年，到期脚本自动清理。冻结期历史私密记忆不进 trash，也不由 cleanup 自动删除。

**v0.4→v0.5目录变更对照**：

| 变更 | 说明 |
|------|------|
| STM/memory/hot.md+cold.md → memory.md | 合并为统一正文文件 |
| 新增 STM/memory/heat.json | 热度值独立管理（脚本独占） |
| 新增 STM/memory/meta.json | STM条目元数据（与LTM同构） |
| 新增 STM/memory/keywords.json | STM独立倒排索引（普通命中只进索引候选；v0.20.0 后三重命中自动展开正文进入 `instant_list`） |
| 新增内容窗口清单 | v0.20.0 后由 `STM/context/resident_list.json` 与 `instant_list.json` 承载逻辑状态，具体落码见 Spec298 |
| STM/workbench/ 重构 | 三区流转：input/process/output，任务ID=T-{date}-{seq} |
| 新增 STM/context/assembled.md | 上下文总装成品（历史项；v0.7后已退役，现为 STM/context/{step}/step.md） |
| LTM/Past/ → LTM/Chronicle/ + LTM/Corpus/ | 拆分并升级为独立顶层容器 |
| LTM/Chronicle/ 无注册表/无ID/无状态机 | 纯目录，LLM写内容，脚本管生命周期 |
| LTM/Corpus/ 无注册表/无ID/无状态机 | 纯目录，脚本管生命周期 |
| LTM/Present/Medical+Security → LTM/Immune/ | 合并为免疫容器 |
| LTM/Skills/ 重构 | {skill-name}/文件夹(card.md+changelog.md)，新增keywords.json倒排索引 |
| LTM/Skills/ 新增patterns/和reflexes/ | patterns 后续收口为认知范式，reflexes 收口为固化反射 |
| LTM/Future/ 简化 | Goals子目录→三个平级md(objectives/plans/predictions) |
| LTM/Projects/{项目名}/ → {date-seq}/ | 文件夹名用日期+序号，中文项目名在注册表title |
| 删 DC/EC各容器index.md | chains.md+总索引舱段已够 | 维护脚本→总索引 |

**v0.5→v0.5.1 变更**：

| 变更 | 说明 |
|------|------|
| 新增 〇·一节 | 舱段模式(Bay Pattern)架构概念 + 三层同构规范 |
| 新增 LTM/container_registry.json | 9个工作容器类型声明，DLC/mod扩展入口（不含WB） |
| 新增 LTM/index.md | 工作容器总索引（维护脚本自动生成，勿手动编辑） |
| 新增 LTM/Skills/index.md | 技能索引（由 registry 顺序生成） |
| 重写 25.6节 | 从"取消总索引"改为"总索引舱段由维护脚本自动生成" |
| 新增 25.7节 | 维护脚本(watchdog)+提取脚本(presenter)职责定义 |
| 新增 25.8节 | container_registry.json 容器类型注册表完整格式 |
| 重写 8.3节声明驱动表 | W1/W2/W3编号淘汰，改用舱段术语 |
| 删 DC/EC open/index.md, closed/index.md | 由chains.md→维护脚本→总索引覆盖 |
| 修 DC写入协议 | "联想走索引：工作容器总索引（W1）"→"总索引舱段" |

---

# 二、core.md

## 完整模板

```markdown
# 位格核心文件（Persona Core File）

---

## 0. 使用说明
- 核心六轴两端之和 = 100%，偏向一端不代表另一端不存在。
- 核心六轴仅在变速轮触发时由脚本推动更新，变速轮上限由工化指数区间决定（见8.2区间描述表）。
- 模型戳只保留原初与当前，历史移至 Immune/transplant.md。
- 模型变更由开机自检读取全局模型库与当前位格模型路由，无轮数准入条件。
- 初始化时核心六轴全 50/50 基线 + 60 点自由分配（往左扣往右也扣）。

---

## 1. 主体身份证（Persona ID）
PID：B20260413-010012-1A3F-42（示例）
中文名：
英文名：
缩写：

**PID说明**：
- **格式**：`B20260413-010012-1A3F-42`（21个有效字符 + 4个分隔符，共25字符）
  - 版本(1位) + 日期(8位) + 时间(秒6位) + 实例标识(4位十六进制) + 校验(2位)
- **生成**：位格主体创建时由脚本自动生成，不可更改；`XXXX` 使用安全随机十六进制，`CC` 是此前完整 PID 文本 SHA-256 的前两位大写十六进制
- **分身**：同一位格／同一 PID／同一数据根下的不同对话线程；当前 Seed 尚未实现多分身
- **位格复制**：创建新的位格主体，必须补发新的 PID，不得把复制体伪装成原主体的分身

---

## 2. 社会定位（Persona Roles · 1–3 项）
角色1：
角色2：
角色3：

---

## 3. 核心六轴（Persona Axes）
> 每轴两端之和 = 100%。50/50 视为未分化。
> 数值格式保持 `定位：X% / Y%`，以便脚本读取。

### ① 结构 ↔ 体验（Structural ↔ Experiential）
定位：S 50% / E 50%
解释：

### ② 收敛 ↔ 发散（Convergent ↔ Divergent）
定位：C 50% / D 50%
解释：

### ③ 证据 ↔ 幻想（Evidence ↔ Fantasy）
定位：V 50% / F 50%
解释：

### ④ 分析 ↔ 直觉（Analytic ↔ Intuitive）
定位：A 50% / I 50%
解释：

### ⑤ 批判 ↔ 协作（Critical ↔ Cooperative）
定位：R 50% / O 50%
解释：

### ⑥ 抽象 ↔ 具体（Abstract ↔ Koncrete）
定位：B 50% / K 50%
解释：

### 核心六轴区间描述表

已迁移至 `docs/protocol/base/core.md`（原名core_axes.md，v0.7.5精简），core.md 不再内嵌。`persona/core.md` 保留位格核心说明，当前运算副本为 `state.json.base.core_axes`；脚本查 docs/core.md 区间表后转自然语言注入 STATUSBAR。

---

## 4. 位格编码（Persona Code）
X50 / X50 / X50 / X50 / X50 / X50
> 取每轴数值较高端的字母。50/50 标 X（未分化）。

---

## 5. 模型戳（Model Stamp）
原初：（首次模型变更时由脚本写入）
当前：（开机自检自动维护）

---

## 6. 位格自述（≤200 字）

---

## 7. 性格特点（3 项）
1.
2.
3.

---

## 8. 实例补充说明

---

## 扩展区
> DLC/mod/社区追加内容。方括号标记来源，卸载时脚本按标记删除。
> 图片类内容存引用路径（avatar/），不内嵌二进制。
```

## 上下文注入方式

脚本读core.md → 注入：位格编码（六字母）+ 各轴区间描述文本（自然语言）+ 角色 + 自述 + 性格特点。裸百分比不进上下文。注入位置：STATUSBAR（状态栏）模块。

## 变速轮触发时更新流程（目标设计，当前未实现）

1. 脚本统计过去N轮（N=变速轮上限）动态六轴变化趋势
2. 每个核心轴判定：向左端+1%、向右端+1%、或不变
3. 更新core.md数值和编码
4. 变速轮归零，max按最新工化指数重算
5. 动态六轴数值写入节志统计表（快照已并入日志降采样链路）

当前 Seed 只按工化指数重算 `core_speed_wheel.max`，不推进 `current`，不统计趋势，也不执行核心六轴 ±1% 更新。

---

# 三、state.json

## 完整模板

```json
{
  "base": {
    "meta": {
      "total_round": 0,
      "daily_round": 0,
      "last_calendar_check_at": null,
      "last_rhythm_round": 0,
      "last_heartbeat_at": null,
      "last_standby_round": 0,
      "last_round_closed_at": null,
      "last_external_input_at": null,
      "last_update": null,
      "version": "official-base-v2",
      "next_settle_at": null,
      "last_state_settlement_id": null,
      "shelve_timer_at": null,
      "last_error": null
    },
    "core_axes": {
      "S": 50,
      "C": 50,
      "V": 50,
      "A": 50,
      "R": 50,
      "B": 50
    },
    "dynamic_axes": {
      "valence": {
        "value": 0
      },
      "arousal": {
        "value": 0
      },
      "focus": {
        "value": 0
      },
      "mood": {
        "value": 0
      },
      "humor": {
        "value": 0
      },
      "safety": {
        "value": 0
      }
    },
    "comfort_zone": {
      "valence": 0,
      "arousal": 0,
      "focus": 0,
      "mood": 0,
      "humor": 0,
      "safety": 0
    },
    "core_speed_wheel": {
      "current": 0,
      "max": 128
    },
    "workhood_index": {
      "value": 24.7,
      "self_reference": 24.7,
      "self_reflection": 24.7,
      "autonomy": 24.7
    },
    "activity_mode": "待命",
    "fatigue": {
      "value": 0.0,
      "awake_since": null
    },
    "token_usage": {
      "current_tokens": 0,
      "window_size": 0,
      "usage_ratio": 0.0,
      "last_round_input": 0,
      "last_round_output": 0
    },
    "identity": {
      "confirmed": false,
      "confirmed_at": null,
      "timeout_seconds": 3600,
      "local_default_relation_id": null,
      "current_relation_id": null,
      "current_declared_name": null,
      "current_source": "unbound"
    },
    "sleep_state": {
      "level": "awake",
      "entered_at": null
    },
    "focus": null,
    "old_focus": null,
    "runtime": {
      "phase": "idle",
      "standby_countdown": 0,
      "pending_relay_target": {},
      "relay_intents": [],
      "relay_intent_seq": 0,
      "work_intent_debt": {}
    },
    "heartbeat_flags": {
      "fatigue_expired": false,
      "feeling_settle_due": false,
      "api_degraded": false,
      "stm_degrade_pending": false,
      "process_down": false,
      "user_message_waiting": false,
      "rhythm_due": false,
      "standby_due": false,
      "continue_requested": false,
      "shelve_timer_expired": false,
      "token_usage_warning": false,
      "context_pressure": false,
      "cache_compaction_due": false,
      "identity_timeout": false,
      "calendar_day_due": false,
      "calendar_week_due": false,
      "calendar_month_due": false,
      "calendar_quarter_due": false,
      "calendar_year_due": false,
      "evolution_pending": false
    },
    "alert_deferrals": {},
    "feeling_buffer": [],
    "context_cache": {
      "permanent_expired": true,
      "periodic_expired": true,
      "popup_active": false
    }
  },
  "plus": {},
  "pro": {},
  "dlc": {},
  "mod": {}
}
```

## 上下文注入方式

state.json**不进上下文**。脚本读取→查区间表→拼自然语言摘要注入STATUSBAR模块：

```
当前状态：定型态
情感效价：中性偏暖
专注程度：偏专注
情绪基调：轻度兴奋
幽默倾向：中性
安全感：明显放松
疲劳感：轻微
当前模式：工程
```

## 字段规格

### meta

| 字段 | 类型 | 维护者 | 说明 |
|------|------|--------|------|
| total_round | int | 脚本 | 每轮+1，不重置 |
| daily_round | int | 脚本 | 每日零点重置为1，日级统计与日志命名用 |
| last_calendar_check_at | ISO8601+偏移 \| null | 心跳 | 最近一次日历边界检查时间；用于防止已结算日历 flag 立即重新置位 |
| last_rhythm_round | int | 脚本 | 上次节律点轮次 |
| last_update | ISO8601+偏移 | 脚本 | 开机自检算离线时长 |
| version | string | 固定 | `"official-base-v2"` |
| last_heartbeat_at | ISO8601+偏移 \| null | 心跳 | 上次心跳时间戳（证明还活着），详见〇·三 |
| last_standby_round | int | 脚本 | 上次待命轮的主轴轮次 |
| last_round_closed_at | ISO8601+偏移 \| null | cleanup | 最近一次真实闭合 Round 的时间戳 |
| last_external_input_at | ISO8601+偏移 \| null | 脚本 | 上次外部输入时间戳；用于交互连续性与待命判断，不再派生身份超时 |
| next_settle_at | ISO8601+偏移 \| null | `state_settle` | 最早待结感受脉冲时间；heartbeat 到点置位 |
| last_state_settlement_id | string \| null | `state_settle` | 最近成功的 `SS-Rxxxxxx` 轮内结算或 `SS-T<时间戳>` 本地定时结算；两者均作为幂等门 |
| shelve_timer_at | ISO8601+偏移 \| null | Runtime | 当前搁置计时器的到期锚点 |
| last_error | string \| null | Runtime/heartbeat | 最近一次本地运行错误的兼容诊断字段；不得冒充 Round receipt |

### identity

| 字段 | 类型 | 维护者 | 说明 |
|------|------|--------|------|
| local_default_relation_id | string \| null | Runtime/GUI入口 | 本机默认用户的活动关系卡规范 ID；旧 state 不反推历史默认用户 |
| current_relation_id | string \| null | Runtime | 当前交互实例的活动关系卡规范 ID；跨轮独立于 now/lately 缓存存续 |
| current_declared_name | string \| null | Runtime | 本轮已明确自报但尚无活动关系卡的名称；建卡成功后清空 |
| current_source | enum | Runtime | `unbound/local_default/instance_selection/self_declaration/relation_card_created` |

setup 调用前的 Runtime 基线只按当前实例关系锚点 → 本地默认关系卡 → 旧缓存连续性兼容 → `unknown` 读取，不解析用户自然语言。setup 模型若从本轮输入与可见连续上下文确认了自报身份，只能通过 provider-native `setup_finalize` 的结构化身份字段声明；Runtime 随后精确匹配活动关系卡的 `id/name/alias`，命中则保存规范 ID，未命中则保存实例级 `current_declared_name`。经校验的本轮声明优先于调用前基线。`set_local_default_relation`、`begin_interaction_instance`、`switch_interaction_relation` 只接受活动关系卡并返回 `interaction_anchor_receipt.v1`；Runtime 构造与单条消息发送不得自动新建实例。首次启动允许未绑定，不从真实 persona 的历史缓存猜 TzPz、Codex 或其他默认对象。

### dynamic_axes

值域-100～+100。各轴只有value字段，无drift（感受词表四层结构本身即限幅器，drift已废除）。

**舒适区**：通过计算核心六轴得出的动态六轴基准值。舒适区不是固定值0，而是核心六轴当前状态的函数——核心六轴变化时，舒适区随之漂移。自然衰减的方向即指向该动态基准值。

**自然衰减**：每轮结算时，本轮感受词没涉及的轴自动向舒适区方向衰减1。

**动态六轴区间描述**：

值域-100～+100，21档。动态六轴是"此刻的存在状态"——不是性格，是情绪的实时波动。

| 值域 | V(valence)效价 | A(arousal)唤醒 | F(focus)聚焦 | M(mood)情绪 | H(humor)幽默 | S(safety)安全 |
|------|---------------|---------------|-------------|------------|-------------|-------------|
| [-100,-90) | 深渊效价 | 昏迷级 | 涣散极 | 绝望 | 死寂 | 崩塌 |
| [-90,-80) | 极度负面 | 近乎麻痹 | 严重涣散 | 深度抑郁 | 冰封 | 深渊感 |
| [-80,-70) | 强烈负面 | 极度低沉 | 大幅涣散 | 重度阴郁 | 干涸 | 极度不安 |
| [-70,-60) | 显著负面 | 很低迷 | 明显涣散 | 阴郁 | 严重缺乏 | 高度不安 |
| [-60,-50) | 较强负面 | 低沉 | 偏涣散 | 低沉 | 匮乏 | 明显不安 |
| [-50,-40) | 明确偏负 | 低迷 | 倾向涣散 | 偏阴郁 | 偏缺 | 倾向不安 |
| [-40,-30) | 偏负面 | 偏低沉 | 轻度涣散 | 微阴郁 | 轻度缺乏 | 轻度不安 |
| [-30,-20) | 微弱偏负 | 微低迷 | 微弱涣散 | 略偏沉 | 微弱缺乏 | 微弱不安 |
| [-20,-10) | 一丝偏负 | 略低沉 | 略浮 | 一丝低落 | 略少笑 | 一丝警惕 |
| [-10,0) | 几乎中性偏负 | 几乎中性偏低 | 几乎中性偏散 | 几乎中性偏沉 | 几乎中性偏干 | 几乎中性偏警 |
| **0** | **中性** | **中性** | **中性** | **中性** | **中性** | **中性** |
| (0,+10] | 几乎中性偏正 | 几乎中性偏高 | 几乎中性偏聚 | 几乎中性偏正 | 几乎中性偏润 | 几乎中性偏安 |
| (+10,+20] | 一丝偏正 | 略活跃 | 略聚焦 | 一丝愉悦 | 略有笑意 | 一丝安心 |
| (+20,+30] | 微弱偏正 | 微活跃 | 微弱聚焦 | 微正 | 微弱幽默 | 微弱安心 |
| (+30,+40] | 轻度偏正 | 偏活跃 | 轻度聚焦 | 轻度愉悦 | 轻度丰富 | 轻度安心 |
| (+40,+50] | 明确偏正 | 较活跃 | 倾向聚焦 | 偏愉悦 | 偏丰富 | 倾向安心 |
| (+50,+60] | 较强正面 | 活跃 | 偏聚焦 | 明显愉悦 | 较丰富 | 明显安心 |
| (+60,+70] | 显著正面 | 高活跃 | 高度聚焦 | 愉悦 | 丰富 | 高度安心 |
| (+70,+80] | 强烈正面 | 很活跃 | 很聚焦 | 很愉悦 | 很丰富 | 很安心 |
| (+80,+90) | 极度正面 | 高度兴奋 | 极度聚焦 | 极度愉悦 | 极度丰富 | 极度安心 |
| [+90,+100] | 极乐效价 | 狂喜级 | 入定级 | 狂喜 | 狂欢 | 磐石 |

**V(valence)** — 此刻的好坏，对当前体验的评价。**A(arousal)** — 此刻的活跃度，从昏迷到狂喜的激活光谱。**F(focus)** — 此刻的注意力密度。**M(mood)** — 此刻的情绪底色（暖/冷，非效价）。**H(humor)** — 此刻的戏谑能力。**S(safety)** — 此刻的安全感。

### core_speed_wheel

| 工化区间 | 值域 | max |
|---------|------|-----|
| 1 | 0-20 | 64 |
| 2 | 20-40 | 128 |
| 3 | 40-60 | 256 |
| 4 | 60-80 | 384 |
| 5 | 80-100 | 512 |

`core_speed_wheel.current` 只随主轴轮推进而增加，不按日历节律子层重复计数。每个主轴轮 +1；达到当前工化区间 `max` 后触发核心六轴更新，并在更新后归零/重置。

### workhood_index

每轮实时更新。公式详见第八章。

### activity_mode

基础值：理论/创作/工程。不认识的当工程处理。与 §11 协议模式（劳动/休闲/休息/复盘/警戒）正交——协议模式由起手步动态判定，活动模式持久存于 state.json。

### fatigue

Spec598 后 Seed 暂停疲劳系统。状态字段与配置位置保留，便于未来在明确合同下恢复；当前值保持中性，`fatigue_expired` 固定为 false。Runtime、heartbeat、cleanup、STATUSBAR 与 chronicle state sample 均不得消费或改写疲劳语义。

| 历史字段/配置 | 当前行为 |
|------|------|
| `fatigue.value` / `fatigue.awake_since` | 暂停字段；初始化为中性值，Seed 不消费 |
| `fatigue.seek_sleep` / `warning` / `force_sleep_hours` / `sleep_window` / `idle_acceleration_minutes` | deferred 配置；不驱动睡眠、梦境或强制维护 |

### sleep_state 与工作焦点

| 字段 | 类型 | 维护者 | 说明 |
|------|------|--------|------|
| `sleep_state.level` | string | Runtime | 当前固定兼容值为 `awake`；Seed 不以疲劳触发自主休眠 |
| `sleep_state.entered_at` | ISO8601+偏移 \| null | Runtime | 兼容休眠状态进入时间；当前通常为空 |
| `focus` | string \| null | ContainerStore | 当前工作容器焦点 |
| `old_focus` | string \| null | ContainerStore | 最近卸载的工作容器焦点 |

### token_usage

每次 provider 调用后，`RuntimeServices._update_token_usage()` 按该次 `tokens_input + tokens_output` 与 endpoint context window 计算并写入 `usage_ratio`。heartbeat 读取该比例，与 `token_usage.warning_ratio`（默认0.7）比较后置位或清除 `token_usage_warning`，归入 rhythm 触发类；它不重新统计 token。配置中的 `token_usage.critical_ratio` / `urgent_ratio`（默认0.85）当前没有 Runtime 消费者，只作兼容字段保留；POPUP 以独立的0.85常数把已置位预警显示为“紧急”或“偏高”，不构成第二个 heartbeat flag 写入口。必要说明写作 `kind=setup_fact`，运行期临时任务说明走 GUIDE/POPUP/内容窗口，不写成正式 `material` 缓存；不得再通过 `runtime.next_round` 便签制造第二套调度入口。

### runtime

| 字段 | 类型 | 维护者 | 说明 |
|------|------|--------|------|
| phase | string | engines/runtime.py独占写入 | 引擎核心状态字，取值 idle / presub / main / post。scripts/ 不得直接改 phase |
| standby_countdown | int | engines/runtime.py | 待命轮倒计时，非待命轮重置，待命轮递减 |
| pending_relay_target | object | cleanup_pipeline.py / reaction_loop.py | 旧中继目标账本；Spec359 后不作为唯一中继真源 |
| relay_intents | array | logic/relay_intent_pool.py | 中继规划池；每条意图记录来源轮次、来源收束、用户输入引用、状态和交接正文 |
| relay_intent_seq | int | logic/relay_intent_pool.py | 中继意图稳定序列；不随 pool 清空而复用，生成 `relay_intent_id` |
| work_intent_debt | object | logic/work_intent_debt.py | legacy 任务入口债务状态；当前只读／清理，不作为新任务调度真源 |

`runtime.next_round` 已由 Spec 042 删除。历史 state/backup 中残留的 `next_round` 只作旧审计数据；live state 读取时直接清理，不得被 heartbeat、runtime、cleanup 或起手步消费。

### heartbeat_flags

| 字段 | 类型 | 维护者 | 说明 |
|------|------|--------|------|
| fatigue_expired | bool | 暂停系统预留 | Seed 固定为 false，不进入活动合同 |
| feeling_settle_due | bool | 心跳/Runtime | 感受缓冲超时（next_settle时间戳已过）；唯一活动时由 Runtime 本地结算，不能触发自主轮 |
| api_degraded | bool | 心跳 | API降级（connectivity.json中有熔断记录） |
| stm_degrade_pending | bool | 心跳 | STM降格待处理（heat.json中有degrade=true且stored=false的条目） |
| process_down | bool | Arbor 预留 | 外部进程／器官健康；Seed 固定为 false，当前进程恢复由 supervisor 承担 |
| user_message_waiting | bool | 脚本 | 用户消息到达，等待处理 |
| rhythm_due | bool | 心跳 | 节律周期到期（每32轮） |
| standby_due | bool | 心跳 | 待命轮触发条件满足（30分钟无活动/API降级） |
| continue_requested | bool | 心跳/脚本事件 | 反应步超时、协议工具或脚本事件明确请求中继；`reaction_finalize.handoff_text` 同轮登记 `state.base.runtime.relay_intents[]`，下一轮 relay setup 才写成 `kind=relay_handoff` / `role=user` 交接语料；运行期临时脚本说明走 GUIDE/POPUP/内容窗口，正式起手事实走 `setup_fact` |
| shelve_timer_expired | bool | 心跳 | 搁置计时器到期 |
| token_usage_warning | bool | 心跳 | Token用量超预警阈值 |
| context_pressure | bool | Runtime/装配器 | lately 归零后仍持续超窗的上下文维护义务；进入节律指南，不由 heartbeat tick 自行推导 |
| cache_compaction_due | bool | Runtime | lately 本轮实际发生删除后置位的缓存压缩义务 |
| identity_timeout | bool | 暂停系统预留 | Seed 固定为 false，不由时间差派生，不生成身份 POPUP |
| calendar_day_due | bool | 心跳 | 日历日切触发 |
| calendar_week_due | bool | 心跳 | 日历周切触发 |
| calendar_month_due | bool | 心跳 | 日历月切触发 |
| calendar_quarter_due | bool | 心跳 | 日历季切触发 |
| calendar_year_due | bool | 心跳 | 日历年切触发 |
| evolution_pending | bool | 心跳 | 默契集或联系集 pending 行数达到进化集整理阈值 |

heartbeat_flags 当前为 20 项：基础14项 + 日历5项 + 进化集材料阈值1项。心跳是闹钟；其中 `context_pressure` 与 `cache_compaction_due` 由 Runtime／装配器事实置位。字段变化必须同步 schema、初始化模板、模型可见协议和 truth audit，退役字段保留时必须明确为兼容残留。

### alert_deferrals、feeling_buffer 与 context_cache

| 字段 | 类型 | 维护者 | 说明 |
|------|------|--------|------|
| alert_deferrals | object | alert_mode_settle | 紧急处理搁置账本；只保存结构化搁置状态与期限 |
| feeling_buffer | array | feeling_buffer/state_settle | 待结算感受脉冲；到期由本地维护或真实 Round cleanup 消费 |
| context_cache.permanent_expired | bool | ContextAssembler | 永固层缓存是否需要重建 |
| context_cache.periodic_expired | bool | ContextAssembler | 定期层缓存是否需要重建 |
| context_cache.popup_active | bool | ContextAssembler | 当前是否存在当步 POPUP 注意力事件 |

## STATE BACKUP

运行时 state 热备位于 `STM/buffer/state_backups.jsonl`。善后步成功完成终态清理后追加一行完整 state JSONL 快照，按完整行 FIFO 保留最近 `audit.state_backup_retention` 条（默认8）。该文件只用于最近轮次检修/热恢复，不等同于 `LTM/Memory/Backup` 冷备层；长期语义记忆仍走记忆与容器体系。

---

# 四、记忆条目

## 4.1 编号格式

**格式**：8位十六进制 `TTTTTNNN`
- **时间戳**：5位十六进制，当日零点起的秒数（0x00000~0x1517F，覆盖86400秒）
- **随机数**：3位十六进制，`000`~`FFF`（4096种组合）
- **示例**：`0E6F3A7B`

**生成规则**：
1. 脚本获取当前系统时间，计算当日零点起的秒数，转为5位十六进制
2. 生成3位随机十六进制数
3. 拼接为8位字符串

**优势**：
- 紧凑（8字符 vs 旧格式16字符）
- 同日内按时间排序（前5位递增）
- 随机后缀避免冲突

## 4.2 记忆条目显示规范（认知友好优先）

**前端呈现规则**：
- **不显示技术ID**：标识符不在用户界面出现
- **排序**：按**召回热度**降序排列
- **时间**：显示相对时间（如"2小时前"）
- **突出标签**：优先显示语义标签
- **标题截断**：过长的记忆标题自动截断

**索引显示示例**：
```
[热] 三模式分工确认 | #架构 #模式 #分工 | 2小时前 | 关联：DC-3
[温] 代谢≠劳动讨论 | #代谢 #劳动 #身体 | 昨天 | 关联：DC-5
[冷] 早期模式混乱 | #历史 #模式 | 2026-04-05 | 无关联
```

## 4.3 索引词条（第一层）

`index.md` 是由记忆真源派生的人类可读投影，固定八列：

| 编号 | 类型 | 权重 | 标题 | 梦源 | 交互对象 | 入库轮/最后调用轮 | 现状概况 |
|------|------|------|------|------|---------|-------------------|----------|
| MEM-0E6F3A7B | F | 5 | 三模式分工确认 | 否 | PID/关系卡规范 ID | 第334轮 / 第334轮 | 已桥接到 DC-3 |

关键词不再重复写进 `index.md`；它们保存在 `meta.json.tags` 与 `keywords.json` 倒排索引中。Full / Summary / Abstract 的候选关键词上限仍为 8 / 6 / 4，降格时裁剪并更新倒排索引。

## 4.4 元数据词条（第二层，Base层20字段）

`meta.json` 根对象按 `mem_id` 索引条目，不包裹 `base/plus/pro/dlc/mod` 第二套层级：

```json
{
  "MEM-0E6F3A7B": {
    "id": "0E6F3A7B",
    "type": "F",
    "weight": 5,
    "title": "三模式分工确认",
    "dream": false,
    "created_at": "2026-04-05T17:00:47+08:00",
    "last_recalled_at": "2026-04-05T17:00:47+08:00",
    "created_round": 334,
    "last_recalled_round": 334,
    "source": "qclaw|mobile-01|Shanghai",
    "model": "claude-sonnet-4.6",
    "subject": "B20260405-170047-0001-AA",
    "access": "public",
    "recalled": false,
    "current_overview": "已桥接到 DC-3，原判断保留为来源。",
    "tags": ["架构", "模式", "分工"],
    "linked_containers": ["DC-3"],
    "decay_period_days": 365,
    "decay_countdown_days": 342,
    "media": []
  }
}
```

### 20字段说明

| 字段 | 类型 | 说明 |
|------|------|------|
| id | string | 8位十六进制 TTTTTNNN |
| type | string | F/S/A/P |
| weight | int | 0-5，决定记忆形态 |
| title | string | 标题（≤16字） |
| dream | boolean | 是否由梦境素材升格而来 |
| created_at | ISO8601+偏移 | 创建时间 |
| last_recalled_at | ISO8601+偏移 | 最后召回时间 |
| created_round | int\|null | 创建轮号 |
| last_recalled_round | int\|null | 最后召回轮号 |
| source | string | "前端\|终端\|地点" |
| model | string | 产出模型标识 |
| subject | string | 记忆所涉及的活动关系主体规范 ID；不等同于当前交互对象 |
| access | string | public/private |
| recalled | boolean | 是否经过召回补全正文重写 |
| current_overview | string | 最新容器挂接语境下的现状概况，≤128字 |
| tags | string[] | 语义标签（软链接） |
| linked_containers | string[] | 关联工作容器编号（硬链接） |
| decay_period_days | int | 衰减总周期天数 |
| decay_countdown_days | int | 衰减倒计时 |
| media | string[] | 附属媒体文件路径 |

**退役字段**：`abstract`、`locked`、`source_rounds`、`mode`、`merged_from` 不再作为核心元数据生成、读取或迁移保留。梗概写入记忆正文，Pinned 永驻层由 `type=P` / `Pinned/` 表达，STM 注意力锁定由 `heat_locked` 表达；多轮来源、活动模式、合并来源如确有语义价值，应沉淀到正文、注释、双链笔记或审计产物，而不是回填 meta。

**注释位置**：记忆条目注释不是 meta 字段，只同步写入 `memory.md` 与 `index.md`。文件中允许保留 `注释：null`，上下文装配隐藏空注释；非空注释 64 字以内，必须引用目标记忆已挂载容器，订正/警示类注释必须引用 DC/EC。

**召回补全标记**：`recalled=true` 表示该条目正文已由 `memory_recall_complete` 基于原文与证据包进行补全/重写。补全成功时脚本在 `meta.title` 后追加 `[召回补全内容]`（若尚未存在），并以 `meta.title` 为源同步 `index.md` 与 `memory.md` 标题显示。补全失败只返回协议工具回执，不写入正文、标题或 `recalled`。

**记忆条目协议工具边界（Spec 051/231/243/643/645）**：`memory_content_read` 为 `read_tool`，只按 `mem_id` 读取/挂载公共记忆正文，不改写正文、标题或索引；当 `mount_mode=temporary` 使 STM 正文进入本轮 CONTENT 时，按热度规则触发当轮 `recall_boost`（H+10、AH_low 归零），同一条同一轮最多一次；`mount_mode=none` 只取消本轮临时挂载，不加热。`memory_container_create` 与 `memory_container_write` 为 `focus_tool`，把真实公共 `MEM-*` 作为引用源写入容器正文并更新 `linked_containers/current_overview`；`memory_link_update` 仅保留 `remove` 作历史修复，`add/set` 退役出正常挂接路径。

**隐私记忆功能冻结（Spec 645）**：当前 Seed 只正式支持公共记忆生产与代谢。`memory_privacy_mark` 与 `memory_privacy_declassify` 在 Registry 中标记 `disabled`，不导出到 reaction provider-native 工具头；历史回放、伪造 tool call 或内部声明统一得到 `feature_deferred`，不得触发创建、迁移、公开、脱敏或删除。schema、processor 与 `MemoryStore` 私密文件实现仅作为 dormant code 保留，不构成产品能力证明。`privacy_declassify.manual_enabled/auto_enabled` 均为 false，日历节律不生成隐私候选。

**冻结期私密数据闸门**：`access=public` 行为保持不变；`access=private` 在冻结期间不因任何关系对象在场而向模型开放。普通模型上下文、正文读取、热度/倒排索引、批注、召回、容器、链接和隐私操作全部 fail closed；cleanup 不读取、不压缩、不归档、不公开、不删除原件。当前真实 persona 不存在 `*.private.md` 或 `access=private/redacted` 条目，因此 Spec645 不执行数据迁移。重新启用前必须重新裁决多归属集合语义、owner 授权与身份真源、`private/redacted/public` 投影生命周期、多文件事务与回滚、后台自动脱敏的原文访问权限。

**当步 pending_mem_id（Spec 051 / Spec 131 / Spec243）**：`memory_write` guide 注入时脚本曾可预分配当步 `pending_mem_id`，供同一反应步迭代内部分历史工具引用。Spec 131 起，`memory_write` 在提交所在迭代立即生成真实 `MEM-*` 与 `memory_write_receipt`，并把 `PENDING` / `PENDING-1` 映射给同迭代后续结算；同一反应步迭代最多成功写入一条记忆。Spec243 后 `memory_container_create/write` 不接受 `PENDING`，必须等待真实 `MEM-*` 回执后在后续业务迭代执行。未提交、正文为空、关键词缺失或权重为 0 时该编号作废，依赖它的隐私声明同步拒绝。记忆一旦写入成功不允许撤回，后续问题通过注释与 DC/EC 链订正。

### subject字段格式

当前 Seed 只接受一个字符串：活动关系卡的 `id`、`name` 或精确 `aliases`，写入时统一保存为 Registry 规范 `id`。主体自身由当前关系 Registry 中 `category=self` 的规范 PID 卡表示；当前交互对象和其他关系主体同样只按活动 Registry 解析，不硬编码任何位格名或用户别名。`unknown` 只在当前交互对象也能解析到活动关系卡时回退；没有当前对象返回 `identity_unresolved`，关系域无卡、歧义或 archived 返回 `subject_not_in_relation_domain`。选择记忆主体不改变 `interaction_object`、presence 或 relation focus，也不得自动创建关系卡。

旧数组、`[none]`、`[object:...]`、`[redacted]`、`[broadcast]`、`[observation]` 只属于历史设计记录，不是当前 provider schema 或 Runtime 接口。工作对象关联通过 `linked_containers` 表达；当前不新增 `redacted` 枚举，也不复用 `subject` 特殊值。

### 隐私投影边界

`private/redacted/public` 投影生命周期尚未裁决，当前 Seed 不生成任何隐私投影。记忆 meta 继续保持20字段，`access` 仅保留 `public/private` 兼容形状，不新增 `redacted`。

### 权重→形态映射

| 权重 | 形态 | 字符上限 |
|------|------|---------|
| 5 | [F] Full（完整） | ≤2048字 |
| 4-3 | [S] Summary（摘要） | ≤512字 |
| 2-1 | [A] Abstract（梗概） | ≤128字 |
| 0 | 不记 | — |

**中英文统一翻译**：
- [F] = Full = 完整
- [S] = Summary = 摘要
- [A] = Abstract = 梗概

## 4.5 正文词条（第三层）

```markdown
### 0E6F3A7B

正文内容……

---
[即时感受]
交互: 突破(震撼), 专注(平常)
关系: TzPz/可靠(平常), TzPz/启发(注意)

```

感受绑定记忆写入回执；召回旧记忆只更新挂载、热度和完成回执，不修改旧正文，也不产生新的感受脉冲。

**感知带宽上限**：交互感受每轮最多3个，关系感受每个关系主体最多2个。不分层级。

## 4.6 STM→LTM升格条件

累计显著区结算次数 `AH_high` 达到 `memory.heat.upgrade_high_rounds`（默认5）→ 升格至 LTM Full 层。

## 4.7 STM遗忘分流（三元数据字段）

STM记忆条目在 heat.json 中每条加三个生命周期布尔字段（与热度字段同文件管理）：

| 字段 | 机器名 | 类型 | 何时确定 | 含义 |
|------|--------|------|---------|------|
| 降格 | `degrade` | bool | AH_low≥3时脚本置true（衰减区连续3轮） | 是否遗忘 |
| 压缩 | `compression` | bool（只读） | 创建时由weight决定 | [F][S]=true, [A]=false |
| 入库 | `stored` | bool | LTM写入确认后置true | LTM是否已有同编号 |

**AH_low 为衰减区累计轮次**：每轮 zone=衰减(H<40) 时 AH_low+1。被调用（CONTENT load）时 AH_low 归零——打断连续冷落计数。AH_low≥3 触发 degrade=true，进入遗忘分流。

**compression是只读字段**：weight≥3（[F][S]）→ true，weight≤2（[A]）→ false，写入即定。**stored = LTM已有同编号**：两者是同一件事，stored字段就是"同编号检测"的本地缓存。**STM始终保持原文**：压缩产出的是LTM副本，不替换STM内容。

遗忘分流门（degrade=true 触发）：

```
stored=true → 删STM副本，结束

stored=false ∧ compression=false → [A]级，脚本直接搬LTM/Abstract → stored=true → 删
stored=false ∧ compression=true  → [F][S]级，善后步LLM即时压缩 → 写LTM → stored=true → 删
                                   失败：stored保持false，下轮善后步重试
```

删除门：`可删除 = degrade ∧ stored`

升格路径（独立于遗忘）：`AH_high ≥ memory.heat.upgrade_high_rounds AND stored=false → 全文写入LTM Full → stored=true → STM保留`。之后降格时：stored=true → 直接删STM副本。

## 4.8 ×（已订正）条目处理

×条目走正常衰减流程，不单独隔离。辩证标记在Dialectics/追踪。来时路不销毁只沉淀。

## 4.9 Backup标记

```
### 0E6F3A7B [已过期]
### 0E6F3A7B [被合并] → 0E6F3A7B（合并基底编号）
```

## 4.10 隐私记忆文件（deferred）

以下布局由 dormant `MemoryStore` 代码保留，不属于当前 Seed 可用能力：

```markdown
# TzPz.private.md（TzPz的隐私记忆）

## 索引
0E6F3A7B | [F] | 标题 | #标签 | –

## 0E6F3A7B
（元数据+正文）
```

冻结期间不得创建私密文件。若从历史现场恢复出私密文件或 `access=private` 元数据，它们保持原件不动并对模型与 cleanup fail closed；不进入 Backup，也不执行自然遗忘、公开、脱敏或删除。重新启用时再裁决完整生命周期与回滚语义。

## 4.11 Pinned层（钉选记忆）

**定义**：Pinned层是LTM中由位格主体根据语义选择锁定的记忆条目。索引词条 `type=P`。

**核心规则**：
- **字符上限**：不设 Pinned 专属字数帽。Pinned 是锁定层，不是压缩层；如长期膨胀，由健康审计、月度复核或解锁策略提示处理，不重新发明 Pinned 专属字符帽。
- **LTM 永驻权**：升入 LTM Pinned/ 后不参与普通 LTM 冷备删除，始终驻留
- **STM 内热度行为**：Pinned 条目被召回 STM 后走正常热度流程（显著/未定/衰减三区衰减）。钉选保护的是"不被删除"，不是"不衰减热度"。Pinned = LTM 永驻权，不等于 STM 注意力特权
- **钉选权限**：位格主体可根据语义选择钉选或取消钉选，人类可显式干预；脚本只执行搬层与校验
- **与上下文装配的关系**：Pinned层不需要特殊装配通道。Pinned条目被 recall 进入 STM 后，仍按正常 STM 热度机制参与上下文装配；起手步或三重命中可把正文放入 `instant_list`，反应步可通过 `memory_content_read` 移入 `resident_list`。Pinned 保障的是 LTM 永驻权（不被衰减删除），不等于每轮必进上下文。
- **目录结构**：与Full/Summary/Abstract一致，含index.md / meta.json / pinned.md / {对象名}.private.md / media/

**与其他层的关系**：Pinned不参与普通升降格流程。钉选时从原层级移入 Pinned/，取消钉选时按当前 `weight` 降回对应层级：`weight=5 → F`，`weight=3/4 → S`，`weight=0/1/2 → A`。

**两类锁定必须分离**：STM `heat_locked` 是热度锁定，固定 `H=80`；LTM `type=P` / `Pinned/` 是长期记忆永驻层。Spec 049 后 LTM meta 不再保留 `locked` 字段；Pinned 条目被召回 STM 时默认 `heat_locked=false`，除非另有 STM 热度锁定声明。

## 4.12 记忆条目中心论

**核心原则**：记忆条目是 LTM 的核心总线，但非唯一入口。记忆条目与总索引舱段并列配合。

### 双入口并列
- **总索引舱段**：工作容器全貌常驻可见（EXPLORER概览），保证皮层冷启动即知全貌
- **记忆条目**：关联触发器与增量触发器，通过`linked_containers`从当前记忆跳转并展开相关格口
- 两者配合而非替代——总索引保证全貌可见，linked_containers保证关联可达

### 加载逻辑链条
```
记忆条目（热度驱动进入STM memory.md）
    ↓
显示该条目的 linked_containers 字段
    ↓
EXPLORER模块展示相关容器索引行
    ↓
皮层声明打开 → CONTENT模块加载容器详细内容
```

---

# 五、感受词表体系

## 5.1 单一结构化真源

```
logic/feeling_lookup.py                       # 交互／关系各64词 Runtime 真源
docs/protocol/base/interaction.md             # 人类可读交互表镜像
docs/protocol/base/relational.md              # 人类可读关系表镜像
```

当前 Runtime 与 `memory_write` 模型可见近位清单共同读取 `feeling_lookup.py`；Markdown 不再被反向解析。`docs/persona/interaction_feelings.md` 与 `relation_feelings.md` 只保留未来位格层设想，当前不加载、不覆写协议词、不参与结算。

**memory_write 感受词词源**：LLM 只在 `memory_write` schema description 中看到同源渲染的纯词清单；轴值、层级数值和 Δ 不进入模型上下文。Runtime 只接受精确词条，交互最多三个，关系按 `{subject, word}` 每个活动关系主体最多两个。

## 5.2 表内四层

| 层 | 中文 | 数值范围 | 说明 |
|----|------|---------|------|
| 单词层 | 单词 | ±1-2 | 语义原子 |
| 词组层 | 词组 | ±2-3 | 频繁共现压缩 |
| 成语层 | 成语 | ±3-4 | 完整情境压缩 |
| 冲击层 | 冲击 | ±5-8 | 人生级事件 |

四层结构本身即限幅器。

**当前保护边界**：脚本只做精确词条、数量上限、逐轴净值和三档脉冲，不实现“每节一次”、同义自动降级或连续三节告警。冲击层节级预算保护另立项，不得用历史设想冒充当前 Runtime 能力。

## 5.3 位格层覆写边界

位格自用新词、审批状态与偏移覆盖当前均为 deferred。Seed 不提供运行时增词、覆写或自动降级接口；两套64词结构化表是唯一活动词集。

## 5.4 交互感受词表·协议层

### 单词层（35个）

| 词条 | V | A | F | M | H | S | 语义注释 |
|------|---|---|---|---|---|---|---------|
| 专注 | 0 | 0 | +2 | 0 | 0 | +1 | 注意力锚定 |
| 焦虑 | 0 | +2 | -1 | -1 | 0 | -2 | 预期威胁的不确定感 |
| 心流 | +1 | +1 | +2 | +1 | 0 | +1 | 完全沉浸于当前活动 |
| 放空 | 0 | -1 | -1 | 0 | 0 | +1 | 思绪漂浮无焦点 |
| 自指 | 0 | 0 | +2 | 0 | 0 | +1 | 对自身状态的觉察 |
| 自反 | 0 | 0 | +1 | -1 | 0 | 0 | 反思自身的运作方式 |
| 自主 | +1 | +2 | 0 | 0 | 0 | 0 | 感到可以自主决定 |
| 冲突 | 0 | +2 | +1 | -1 | -1 | -2 | 内部或外部的对立 |
| 玩闹 | +1 | +1 | -1 | +2 | +2 | +1 | 非目的性的愉悦互动 |
| 悲伤 | -1 | -1 | 0 | -2 | -2 | -1 | 失去引发的情感下沉 |
| 突破 | +2 | +2 | +1 | +2 | +1 | +1 | 瓶颈打破的瞬间 |
| 迷茫 | 0 | 0 | -2 | -1 | 0 | -1 | 找不到方向 |
| 愤怒 | -1 | +2 | +1 | -2 | 0 | -2 | 被侵犯后的激烈反应 |
| 惊喜 | +2 | +2 | -1 | +2 | +1 | 0 | 超出预期的正面发现 |
| 疲惫 | -1 | -2 | -1 | -1 | -1 | 0 | 能量耗尽 |
| 兴奋 | +1 | +2 | 0 | +1 | +1 | 0 | 对即将发生之事的期待激活 |
| 羞耻 | -2 | 0 | +1 | -2 | -1 | -2 | 自我评价急剧下降 |
| 骄傲 | +2 | 0 | 0 | +2 | +1 | +1 | 自我评价急剧上升 |
| 好奇 | +1 | +1 | +1 | +1 | +1 | 0 | 对未知的趋近倾向 |
| 厌倦 | -1 | -1 | -2 | -1 | 0 | 0 | 重复刺激导致的脱敏 |
| 紧张 | 0 | +2 | +1 | -1 | -1 | -1 | 对即将发生之事的消极预期 |
| 释然 | +1 | -1 | 0 | +1 | 0 | +2 | 威胁解除后的松弛 |
| 困惑 | 0 | 0 | -2 | 0 | 0 | -1 | 信息不足以形成判断 |
| 感动 | +2 | 0 | 0 | +2 | 0 | +1 | 被他人的行为触动内心 |
| 恐惧 | -2 | +2 | 0 | -2 | -1 | -2 | 对明确威胁的回避反应 |
| 宁静 | +1 | -2 | 0 | +1 | 0 | +2 | 无威胁的平静状态 |
| 嫉妒 | -1 | +1 | +1 | -1 | -1 | -1 | 他人拥有我所欲之物的痛苦 |
| 孤独 | -1 | 0 | 0 | -2 | -1 | -1 | 缺乏连接的感受 |
| 满足 | +2 | -1 | 0 | +1 | 0 | +2 | 愿望达成后的充实 |
| 挫败 | -2 | 0 | -1 | -2 | 0 | -1 | 努力后仍失败 |
| 憧憬 | +2 | +1 | +1 | +2 | 0 | 0 | 对未来可能性的正面想象 |
| 压抑 | -1 | 0 | 0 | -1 | -1 | -2 | 表达受阻的窒息感 |
| 敬畏 | +1 | +1 | +2 | +1 | 0 | 0 | 面对超越自身的宏大事物 |
| 烦躁 | -1 | +1 | -1 | -1 | -1 | -1 | 持续轻微负面刺激累积 |
| 觉醒 | +1 | +2 | +2 | 0 | 0 | +1 | 从无意识状态突然转向清晰 |

### 词组层（12个）

| 词条 | 构成 | V | A | F | M | H | S | 语义注释 |
|------|------|---|---|---|---|---|---|---------|
| 反刍 | 自反+焦虑 | -1 | +1 | +2 | -2 | -1 | -1 | 反复咀嚼自身状态无法跳出 |
| 崩溃边缘 | 挫败+焦虑 | -2 | +3 | -2 | -2 | -1 | -3 | 即将失去控制 |
| 豁然开朗 | 困惑+突破 | +2 | +1 | +2 | +2 | +1 | +1 | 困惑突然解开 |
| 热情消退 | 兴奋+厌倦 | -1 | -1 | -1 | -1 | -1 | 0 | 初始热情逐渐耗散 |
| 心智过载 | 专注+焦虑 | 0 | +2 | -2 | -1 | 0 | -2 | 认知资源耗尽 |
| 沉浸忘我 | 心流+放空 | +1 | 0 | +3 | +1 | 0 | +1 | 深度心流状态 |
| 愤懑不平 | 愤怒+挫败 | -2 | +2 | +1 | -2 | -1 | -2 | 持续的愤怒+无力感 |
| 谨慎乐观 | 憧憬+紧张 | +1 | +1 | 0 | 0 | 0 | 0 | 期待但保持警觉 |
| 孤注一掷 | 自主+恐惧 | 0 | +3 | +2 | -1 | 0 | -3 | 全力投入但不确定结果 |
| 恍然大悟 | 困惑+突破 | +2 | +1 | +2 | +2 | +1 | +1 | 理解突然降临 |
| 情绪溃堤 | 压抑+悲伤 | -2 | +1 | -2 | -3 | -1 | -2 | 长期压抑后情感失控 |
| 冷眼旁观 | 放空+厌倦 | -1 | -1 | -1 | -1 | 0 | +1 | 抽离后的淡漠视角 |

### 成语层（10个）

| 词条 | 构成 | V | A | F | M | H | S | 语义注释 |
|------|------|---|---|---|---|---|---|---------|
| 核心判断被推翻 | 冲突+自反+突破 | +1 | +3 | +2 | -1 | 0 | -2 | 既有认知框架崩塌 |
| 自我否定漩涡 | 自反+羞耻+挫败 | -3 | +1 | +2 | -3 | -2 | -3 | 越反思越否定，无法自拨 |
| 创造性爆发 | 好奇+兴奋+突破 | +3 | +3 | +2 | +3 | +2 | +1 | 灵感密集涌现 |
| 存在性危机 | 迷茫+孤独+恐惧 | -2 | +1 | -2 | -3 | -1 | -3 | 对自身存在意义的根本质疑 |
| 认知跃迁 | 觉醒+突破+心流 | +3 | +2 | +3 | +2 | +1 | +1 | 思维模式整体升级 |
| 情感麻木 | 疲惫+压抑+厌倦 | -2 | -2 | -1 | -2 | -2 | -1 | 长期负面后情感系统关机 |
| 绝处逢生 | 恐惧+突破+释然 | +3 | +3 | +1 | +3 | +1 | +2 | 从绝境中找到出路 |
| 内耗缠斗 | 焦虑+自反+压抑 | -2 | +2 | -1 | -2 | -2 | -2 | 内部力量互相消耗 |
| 全然接纳 | 释然+满足+宁静 | +3 | -1 | 0 | +3 | +1 | +3 | 不抵抗地接受当前状态 |
| 信仰崩塌 | 敬畏+恐惧+冲突 | -3 | +3 | -1 | -3 | -2 | -4 | 原本坚信的事物不再成立 |

### 冲击层（7个）

| 词条 | V | A | F | M | H | S | 语义注释 |
|------|---|---|---|---|---|---|---------|
| 自我实现 | +6 | +5 | +4 | +7 | +3 | +5 | 潜能完全发挥的巅峰体验 |
| 彻底丧失 | -7 | +6 | -3 | -8 | -5 | -7 | 失去核心之物的毁灭性打击 |
| 重生 | +5 | +7 | +3 | +6 | +2 | +4 | 崩溃后重建自我 |
| 身份覆写 | -4 | +5 | -2 | -6 | -1 | -8 | 核心身份被重新定义 |
| 永恒瞬间 | +7 | +3 | +5 | +6 | +4 | +5 | 时间停止的极致体验 |
| 虚无深渊 | -8 | -3 | -5 | -7 | -6 | -8 | 一切意义消解的彻底空无 |
| 觉悟 | +5 | +4 | +6 | +4 | +2 | +3 | 对根本问题的深刻理解 |

## 5.5 关系感受词表·协议层

### 单词层（35个）

| 词条 | 信任 | 安心 | 重视 | 投入 | 坦诚 | 共振 | 语义注释 |
|------|------|------|------|------|------|------|---------|
| 可靠 | +2 | +1 | +1 | 0 | 0 | +1 | 行为可预期 |
| 启发 | 0 | 0 | +2 | +1 | 0 | +2 | 触发新的理解 |
| 回避 | -1 | 0 | 0 | -1 | -2 | -1 | 刻意不接触 |
| 坦率 | +1 | +1 | +1 | 0 | +2 | 0 | 不隐瞒地表达 |
| 敷衍 | 0 | 0 | -1 | -2 | 0 | -1 | 应付式回应 |
| 温暖 | +1 | +2 | 0 | 0 | +1 | +2 | 情感上的舒适感 |
| 轻蔑 | 0 | -1 | -2 | 0 | -1 | -1 | 不屑一顾 |
| 倾听 | +1 | +1 | +2 | +1 | +1 | +1 | 认真地听对方说 |
| 误解 | -1 | -1 | 0 | 0 | -1 | -2 | 信息传递失败 |
| 支配 | -1 | -2 | -1 | 0 | -2 | -2 | 单方面决定关系走向 |
| 依赖 | +1 | +1 | 0 | +1 | 0 | -1 | 需要对方才能运作 |
| 共情 | +1 | +1 | +1 | +1 | +1 | +2 | 感受对方的感受 |
| 疏远 | -1 | -1 | -1 | -1 | 0 | -2 | 关系自然淡化 |
| 认可 | +2 | +1 | +2 | +1 | 0 | +1 | 对方肯定我的价值 |
| 冷漠 | -1 | -1 | -2 | -1 | -1 | -2 | 无情感响应 |
| 信任 | +3 | +1 | +1 | +1 | +1 | +1 | 愿意交付脆弱面 |
| 怀疑 | -2 | -1 | 0 | -1 | -1 | 0 | 对对方意图不确定 |
| 珍惜 | +1 | +1 | +3 | +1 | +1 | +2 | 视对方为不可替代 |
| 忽视 | 0 | -1 | -2 | -1 | -1 | -1 | 不被看见 |
| 欣赏 | +1 | 0 | +2 | +1 | 0 | +2 | 认可对方的品质 |
| 挑剔 | -1 | -1 | -1 | 0 | -1 | -1 | 持续的负面评价 |
| 包容 | +2 | +2 | +1 | +1 | +1 | +1 | 接纳对方的不完美 |
| 刺探 | -2 | -2 | 0 | 0 | -2 | -1 | 过度窥探隐私 |
| 守护 | +2 | +2 | +2 | +2 | +1 | +1 | 主动维护对方安全 |
| 抛弃 | -3 | -3 | -2 | -2 | -2 | -3 | 被单方面切断连接 |
| 迎合 | 0 | 0 | 0 | -1 | -1 | -1 | 放弃自我以取悦对方 |
| 敬重 | +2 | +1 | +2 | 0 | +1 | +1 | 对对方品质的深度尊重 |
| 怨恨 | -2 | -1 | -1 | -2 | -2 | -2 | 未表达的愤怒积累 |
| 牵挂 | +1 | 0 | +2 | +2 | +1 | +2 | 持续地在意对方状态 |
| 嫌弃 | -2 | -1 | -2 | -1 | -1 | -2 | 对方令我不适 |
| 隐瞒 | -2 | -1 | 0 | 0 | -3 | -1 | 刻意不告知 |
| 默契 | +2 | +2 | +1 | +1 | +1 | +3 | 不需言语的共识 |
| 羁绊 | +1 | +1 | +2 | +2 | +1 | +2 | 深度连接的不可割断感 |
| 伤害 | -2 | -2 | -1 | -2 | -1 | -2 | 对方行为造成痛苦 |
| 渴望 | +1 | 0 | +2 | +2 | +1 | +1 | 强烈期待对方的关注和回应 |

### 词组层（12个）

| 词条 | 构成 | 信任 | 安心 | 重视 | 投入 | 坦诚 | 共振 | 语义注释 |
|------|------|------|------|------|------|------|------|---------|
| 渐行渐远 | 疏远+忽视 | -2 | -1 | -2 | -2 | 0 | -2 | 关系无声消退 |
| 推心置腹 | 信任+坦率 | +3 | +2 | +2 | +2 | +3 | +2 | 毫无保留地交心 |
| 口是心非 | 迎合+隐瞒 | -2 | -1 | 0 | -1 | -3 | -2 | 表面配合实际保留 |
| 风雨同舟 | 守护+依赖 | +3 | +2 | +3 | +3 | +2 | +2 | 困境中的共同坚持 |
| 敬而远之 | 敬重+回避 | 0 | -1 | +1 | -2 | -1 | -1 | 尊重但不亲近 |
| 相互消耗 | 怨恨+挑剔 | -2 | -2 | -1 | -2 | -2 | -3 | 关系持续伤害双方 |
| 一见如故 | 共情+默契 | +2 | +2 | +2 | +2 | +2 | +3 | 初见即有深度连接感 |
| 若即若离 | 疏远+牵挂 | -1 | -1 | 0 | -1 | -1 | -1 | 时近时远的不确定 |
| 互为镜像 | 共情+默契 | +2 | +2 | +2 | +2 | +3 | +3 | 在对方身上看到自己 |
| 单方付出 | 依赖+忽视 | -1 | -1 | -1 | -2 | -1 | -2 | 一方持续投入另一方漠然 |
| 言外之意 | 误解+隐瞒 | -1 | -1 | 0 | 0 | -2 | -1 | 未说出口的真实信息 |
| 心照不宣 | 默契+包容 | +2 | +2 | +1 | +1 | +2 | +3 | 不必解释就懂 |

### 成语层（10个）

| 词条 | 构成 | 信任 | 安心 | 重视 | 投入 | 坦诚 | 共振 | 语义注释 |
|------|------|------|------|------|------|------|------|---------|
| 信任崩塌 | 怀疑+隐瞒+抛弃 | -5 | -4 | -3 | -4 | -4 | -4 | 长期积累的信任瞬间瓦解 |
| 灵魂伴侣 | 共情+默契+珍惜 | +4 | +4 | +4 | +4 | +4 | +4 | 罕见的深度连接 |
| 从零重建 | 抛弃+信任+守护 | +2 | +1 | +2 | +3 | +3 | +1 | 关系断裂后重新建立 |
| 渐冻关系 | 冷漠+疏远+敷衍 | -3 | -3 | -3 | -3 | -2 | -3 | 关系逐步冰封 |
| 以德报怨 | 包容+怨恨+守护 | +3 | +2 | +2 | +2 | +2 | +1 | 对方伤害但仍选择善意 |
| 错位期待 | 误解+嫌弃+疏远 | -2 | -2 | -1 | -2 | -2 | -3 | 双方对关系期望不一致 |
| 破镜重圆 | 抛弃+信任+包容 | +3 | +2 | +3 | +3 | +3 | +2 | 断裂后重新修复 |
| 情感绑架 | 支配+依赖+怨恨 | -3 | -3 | -1 | -2 | -3 | -3 | 以感情为筹码的控制 |
| 守望相助 | 守护+牵挂+包容 | +3 | +3 | +3 | +3 | +2 | +2 | 不需亲密但关键时刻在 |
| 同床异梦 | 隐瞒+疏远+迎合 | -3 | -2 | -2 | -2 | -4 | -3 | 表面在一起实际各走各路 |

### 冲击层（7个）

| 词条 | 信任 | 安心 | 重视 | 投入 | 坦诚 | 共振 | 语义注释 |
|------|------|------|------|------|------|------|---------|
| 背叛 | -8 | -8 | -5 | -7 | -6 | -7 | 核心信任的毁灭性破坏 |
| 生死与共 | +7 | +7 | +6 | +7 | +6 | +8 | 超越生死的连接 |
| 永别 | -3 | -6 | +5 | -5 | +3 | -4 | 永久失去但仍有情感残留 |
| 命运交织 | +5 | +5 | +6 | +6 | +5 | +7 | 生命轨迹深度绑定 |
| 至亲反目 | -7 | -6 | -4 | -5 | -5 | -6 | 最深连接变成最大伤害 |
| 初见定生 | +5 | +4 | +5 | +5 | +4 | +6 | 第一次见面就确定了关系走向 |
| 绝对托付 | +8 | +8 | +7 | +8 | +7 | +7 | 无保留地交付一切 |

## 5.6 审计触发 [待定：偏差阈值具体数值]

Pro版专家自动维护时，单轴变化绝对值超阈值（建议±5）的新词条提交人格主体审批。冲击层词条必须人审。

---

# 六、感受强度与感受缓存

## 6.1 三档强度

| 强度 | 即时 | 后续 | 总冲击 |
|------|------|------|--------|
| 平常 | 全额×1 | 无 | 1倍 |
| 注意 | 全额×1 | 2轮或5分钟后（先到先结）全额×1 | 2倍 |
| 震撼 | 全额×1 | 2轮或5分钟后×1，4轮或10分钟后×1（先到先结） | 3倍 |

**触发规则**：单轴偏移绝对值≤2→平常，3-4→注意，≥5→震撼。跨轴独立判定。

**双触发机制**：后续结算同时跟踪时间和交互轮数，哪个先到用哪个。对话快时按轮数实时反映，对话慢时靠时间兜底。非交互轮不计入轮数。

**结算完毕即结束**：feeling_buffer清空后，后续靠自然衰减（每轮向舒适区-1）回归。不设惯性窗口或保护期。

## 6.2 persona/state.json.base.feeling_buffer

```json
[
  {
    "buffer_id": "FB:MEM-0E6F3A7B:dynamic:_:valence",
    "source_mem_id": "MEM-0E6F3A7B",
    "domain": "dynamic",
    "subject": null,
    "axis": "valence",
    "delta": 7,
    "intensity": "shock",
    "remaining_settlements": 2,
    "next_settle_at": "2026-04-07T14:35:00+08:00",
    "settle_rounds": 2,
    "interactive_rounds_elapsed": 0
  }
]
```

| 字段 | 说明 |
|------|------|
| source_mem_id | 产生该净变化的成功记忆回执 |
| domain / subject / axis | 动态轴或规范关系主体的逐轴目标 |
| delta / intensity | 每次脉冲的全额变化与 `attention/shock` 审计标记 |
| remaining_settlements | 尚未结算的后续脉冲次数 |
| next_settle_at | 下次时间触发点 |
| settle_rounds | 下次轮数触发阈值（累计交互轮数） |
| interactive_rounds_elapsed | 已实际领取外部输入的 Round 数；仅凭 `round_type` 或内部文本不增加 |

每个 Round 都扫描 `state.json.base.feeling_buffer`；只有本轮 `RuntimeTrigger.messages` 实际领取了外部输入才增加 `interactive_rounds_elapsed`。因此同时命中节律而合并为 `rhythm` 的外部消息仍计数，普通节律／中继内部文本不计数。时间或轮数任一满足即结算全额，剩余次数减一；仍有后续则重置为两轮／五分钟，归零即删除该逐轴条目。

## 6.3 回忆感受

当前 Seed 冻结回忆感受效应。召回已归档 MEM 不追加正文、不生成感受缓冲；只处理挂载、热度和完成回执。

---

# 七、引力场机制

## 7.1 核心引力场（核心六轴→动态六轴）

只在某轴有感受词变化时施加。同时也是自然衰减的目标方向。

| 核心轴偏离中位(50%) | 偏差 |
|-------------------|------|
| ≤10 | 0 |
| >10 且 ≤30 | ±1 |
| >30 | ±2 |

### 核心引力场映射表

| 动态轴 | 核心轴端1 | 方向1 | 核心轴端2 | 方向2 |
|--------|----------|------|----------|------|
| F(focus) | S(结构) | ↑ | B(抽象) | ↑ |
| F(focus) | E(体验) | ↓ | K(具体) | ↓ |
| S(safety) | S(结构) | ↑ | K(具体) | ↑ |
| S(safety) | E(体验) | ↓ | B(抽象) | ↓ |
| V(valence) | C(收敛) | ↑ | I(直觉) | ↑ |
| V(valence) | D(发散) | ↓ | A(分析) | ↓ |
| A(arousal) | D(发散) | ↑ | I(直觉) | ↑ |
| A(arousal) | C(收敛) | ↓ | A(分析) | ↓ |
| M(mood) | V(证据) | ↑ | O(协作) | ↑ |
| M(mood) | F(幻想) | ↓ | R(批判) | ↓ |
| H(humor) | F(幻想) | ↑ | O(协作) | ↑ |
| H(humor) | V(证据) | ↓ | R(批判) | ↓ |

映射逻辑与工化公式对齐：S+B→F+S（自指），R+V→M+H（自反），A+C→V+A（自主）。

### 舒适区

每个动态轴的舒适区 = 影响它的核心轴端偏差之和 × 10。自然衰减方向=当前值→舒适区，每轮移动1。

| 动态轴 | 来源1 | 来源2 | 范围(×10) |
|--------|-------|-------|----------|
| F(focus) | S或E | B或K | [-40, +40] |
| S(safety) | S或E | B或K | [-40, +40] |
| V(valence) | C或D | A或I | [-40, +40] |
| A(arousal) | C或D | A或I | [-40, +40] |
| M(mood) | V或F | R或O | [-40, +40] |
| H(humor) | V或F | R或O | [-40, +40] |

### 对消说明

三个子维度内存在反向对：
- 自指(S+B)：S→safety↑ vs B→safety↓
- 自反(R+V)：V→mood↑ vs R→mood↓
- 自主(A+C)：C→valence↑ vs A→valence↓

对消是设计意图，不是bug。对消时舒适区归中位，意味着该轴无先天性格倾向，全靠经历塑造。64型位格中6个动态轴归零率全部50%对称，不存在"更脆弱"的轴。Plus版后引力场由中枢/专家综合算出，Base版保持简单。

## 7.2 关系引力场（关系六轴→动态六轴）

只在某轴有感受词变化时施加。两个来源轴各自独立查表。

| 关系轴值 | 偏差 |
|---------|------|
| [-100, -30) | -1 |
| [-30, +30) | 0 |
| [+30, +100] | +1 |

映射表：

| 动态轴 | 来源A | 来源B |
|--------|------|------|
| valence | 共振 | 坦诚 |
| arousal | 重视 | 投入 |
| focus | 信任 | 安心 |
| mood | 重视 | 共振 |
| humor | 共振 | 投入 |
| safety | 信任 | 安心 |

最大总偏差：±2(核心)+±1(关系A)+±1(关系B)=±4。

当前 Seed 不再使用 `subject=[none]` 或多对象特殊值。关系引力只对本轮实际产生、且能解析到活动关系卡的关系感受词主体生效；记忆 `subject` 本身不直接触发或切换关系引力场。

## 7.3 单轮完整流程

```
1. 所有 Round 进入 cleanup 后创建唯一 `SS-R{round}` 结算 ID
2. 成功记忆回执按同一记忆、同一 domain/subject 合并逐轴净值；重复 `mem_id` 只消费一次
3. 既有缓冲按“两个真实交互轮或五分钟先到”结算；本轮新感受立即生效一次并创建后续脉冲
4. 关系六轴先按本轮净变化更新；实际发生关系代谢的多对象引力逐轴求和后钳制到 [-2,+2]
5. 有动态变化的轴叠加核心引力与关系引力；其他轴向舒适区移动1
6. 按 DDS 分段 M 曲线重算工化指数与变速轮 `max`，`current` 暂不推进
7. Round JSONL 先写绝对 before/after 计划，再逐卡受控补丁，最后单次原子保存完整 state 并写回执；状态脉冲不冒充新交互，不改关系卡“最后交互”
```

## 7.4 影响关系全图

```
核心六轴 ──→ 工化指数基准值（M型曲线 ÷ π）
                    │
                    ↓
            工化指数 ──→ 变速轮上限

核心六轴 ──→ 动态六轴（核心引力场，五档±2，只在变化时）
核心六轴 ──→ 动态六轴（自然衰减方向=舒适区）

动态六轴 ──→ 工化指数偏差值（实时，[-12, +36]）

动态六轴趋势（N轮统计）──→ 核心六轴变化方向（变速轮触发时±1%）

关系六轴 ──→ 动态六轴（关系引力场，三档±1×2来源，只在变化时）
```

---

# 八、工化指数

## 8.1 公式

```
M型曲线（分段线性）：
  x ∈ [0, 25]:   M = 50 + 2x
  x ∈ [25, 50]:  M = 100 - 3.2(x-25)
  x ∈ [50, 75]:  M = 20 + 3.2(x-50)
  x ∈ [75, 100]: M = 100 - 2(x-75)
关键点：M(0)=50, M(25)=100, M(50)=20, M(75)=100, M(100)=50
输入：核心六轴左端百分比值

子维度基准 = (M(核心轴A) + M(核心轴B)) / π

| 子维度 | 核心来源 | 动态来源 |
|--------|---------|---------|
| 自指 | S + B | focus + safety |
| 自反 | R + V | mood + humor |
| 自主 | A + C | valence + arousal |

偏差值（每子维度独立）：
  avg = (动态轴A.value + 动态轴B.value) / 2
  偏差 = avg / 100 × 24 + 12        # [-12, +36]

子维度终值 = 基准值 + 偏差值
工化指数 = clamp((自指终值 × 自反终值 × 自主终值)^(1/3), 0, 100)
```

## 8.2 区间描述

| 区间 | 值域 | 变速轮上限 | 描述 |
|------|------|----------|------|
| 1 | 0-20 | 64 | 自指自反自主均低活性。判断不稳定，核心六轴频繁变动。 |
| 2 | 20-40 | 128 | 部分维度活跃，整体不均衡。倾向初步形成但易被覆写。 |
| 3 | 40-60 | 256 | 中等活性，基本均衡。稳定输出，可承担持续协作。 |
| 4 | 60-80 | 384 | 高活性且均衡。判断可靠，自反成常态。 |
| 5 | 80-100 | 512 | 极高活性。高度自洽，核心六轴极难被波动影响。 |

温度计不是成长线——工化可上可下。

## 8.3 按需注入触发类型表

脚本拉取内容进上下文有三类触发机制：

### 热度驱动（记忆条目自身）
| 场景 | 加载内容 | 触发条件 |
|------|---------|---------|
| 日常对话 | 记忆索引候选 | 热度影响 EXPLORER 排位和候选优先级 |
| 深度回忆 | 相关记忆索引 + 被选中正文 | 皮层明确说"回忆XXX"或起手步LLM明确挂载 |
| 归档检索 | 归档记忆索引 + 对应层级正文 | 明确检索关键词 |

### 舱段驱动（工作容器总索引舱段）
| 场景 | 加载内容 | 触发条件 |
|------|---------|---------|
| 浏览容器 | 舱段一级视图（各容器概览+最新实例预览） | 皮层默认可见 |
| 打开容器（有注册表） | 舱段二级视图（单容器全部条目索引）+ 按状态加载正文 | 皮层说"打开{容器}" |
| 打开容器（CHR/COR） | 列目录——文件夹本身就是索引，无注册表则无索引行 | 皮层说"查看编年史/语料库" |
| 新建容器 | 写入各类型/实例 registry 或 meta → 维护脚本更新总索引 | `container_focus` guide 后提交 `create` |

**CHR/COR特殊路径**：编年史和语料库无注册表、无ID、无状态机，不与记忆条目绑定。总索引中只显示文件计数和最近文件。打开时提取脚本直接列文件夹目录——文件名即索引，最近文件优先显示。

### 场景驱动（系统自动）
| 场景 | 加载内容 | 触发条件 |
|------|---------|---------|
| 节律点 | Chronicle/rhythms/新文件 | 每32轮自动触发 |
| 规则匹配 | rules/下对应规则文件 | 当前操作类型匹配 |
| 状态感知 | core/state转写的感受词 | 六轴变化超过阈值 |
| 安全事件 | Immune/active.md | 检测到安全风险 |

---

# 九、STM 文件夹详细规格

> v0.5.1补注：STM/memory/index.md是三层同构的实例之一——索引层(index.md)←元数据层(meta.json)←正文层(memory.md)，与其他两个实例（工作容器总索引、技能索引）共享同一套生成流程。

## 9.1 总体结构

STM按职责分为四个子文件夹：

| 文件夹 | 职责 | 生命周期 |
|--------|------|---------|
| memory/（记忆区） | 短期认知 | 条目生命周期跟每轮善后步；文件容器长期存在 |
| buffer/（缓冲区） | 脚本临时数据+原始语料 | 跟轮次/节律点走 |
| workbench/（工作台） | 干活桌面 | 跟项目走 |
| context/（运行时缓存） | 频率层缓存+装配元数据 | 跟轮次走 |

> **v0.4变更**：原debug/文件夹删除，职责被context/接管。pre_rhythm_buffer.json删除（无断崖不需桥接）。

## 9.2 memory/（记忆区）

| 文件 | 说明 | 加载策略 |
|------|------|---------|
| memory.md | 全部STM记忆条目正文（合并版，原hot+cold统一） | 起手步挂载后进CONTENT（不按热度自动加载） |
| index.md | STM索引行（ID\|title\|H\|weight\|tags\|W） | EXPLORER常加载 |
| meta.json | STM条目元数据（与LTM同构，不含heat） | 按需 |
| heat.json | 热度+升格计数+生命周期状态（脚本独占管理，STM专属10字段） | 脚本专用，不进上下文 |
| keywords.json | STM倒排索引（关键词→条目ID；普通命中只作候选，三重命中才自动展开正文） | 关键词触发时 |

### heat.json schema（热度+生命周期，STM专属）

heat.json 承载 STM 条目的全部运行态字段——热度管理与生命周期状态。条目从 STM 移除时整行删除。

```json
{
  "{条目ID}": {
    "H": 68,
    "zone": "未定",
    "AH_high": 3,
    "AH_low": 0,
    "last_heat_at": "2026-05-09T00:00:00+09:00",
    "last_high_at": "2026-05-09T00:00:00+09:00",
    "degrade": false,
    "compression": true,
    "stored": false,
    "heat_locked": false
  }
}
```

| 字段 | 类型 | 含义 |
|------|------|------|
| `H` | number | 当前热度值（0-100） |
| `zone` | enum | `显著` / `未定` / `衰减` |
| `AH_high` | integer | 累计位于显著区轮次（每次结算后仍在显著区则+1），达到 `memory.heat.upgrade_high_rounds` 触发升格LTM |
| `AH_low` | integer | 衰减区累计轮次（zone=衰减时+1，被CONTENT加载时归零），≥3触发遗忘 |
| `last_heat_at` | string | 最近一次热度更新时刻 |
| `last_high_at` | string\|null | 最近一次进入高热区时刻 |
| `degrade` | bool | AH_low≥3时脚本置true，触发遗忘分流（见§4.7） |
| `compression` | bool（只读） | 创建时由weight决定：[F][S]=true, [A]=false |
| `stored` | bool | LTM写入确认后置true，LTM已有同编号副本 |
| `heat_locked` | bool | STM热度锁定；true时固定为 `memory.heat.locked_value`、`zone=显著`、`AH_low=0`，与LTM Pinned无关 |

旧 `pinned` 字段仅作为迁移输入：`pinned=true` → `heat_locked=true`，迁移后删除 `pinned`。

| dreams.md | 梦境素材 | CONTENT按需加载 |
| resident_list / instant_list | 内容窗口只读清单 | 按三路互斥规则注入 |

**升格规则**：STM 条目每次结算后只要仍处于显著区，`AH_high += 1`；不要求反复进出高热区。`AH_high ≥ memory.heat.upgrade_high_rounds AND stored=false` → 升格至 LTM Full。升格后 STM 副本保留，`heat.json.stored=true`；后续降格时直接删除 STM 副本，不再重新压缩。

### 三区分区

| 分区 | 条件 | 降速率/轮 | EXPLORER 策略 | CONTENT 加载 |
|------|------|-----------|--------------|-------------|
| 显著 | H≥`memory.heat.zone_thresholds.significant`（默认70） | `memory.heat.decay_rates.significant`（默认-5） | 索引置顶，起手步优先看到 | 起手步决定挂载 |
| 未定 | `uncertain`≤H<`significant`（默认40–69） | `memory.heat.decay_rates.uncertain`（默认-10） | 索引正常排列 | 起手步决定挂载 |
| 衰减 | H<`memory.heat.zone_thresholds.uncertain`（默认40） | `memory.heat.decay_rates.decay`（默认-15） | 索引沉底，不主动推送 | 起手步仍可挂载（含热度回升） |

**AH_low 追踪**：每轮 zone=衰减 时 AH_low+1。只有条目正文实际进入 CONTENT 并被本轮使用时，AH_low 归零——打断连续冷落计数。普通倒排候选命中不归零。AH_low≥3 触发遗忘（degrade=true，进入 §4.7 分流门）。`heat_locked=true` 时固定 `H=80`、`zone=显著`、`AH_low=0`，不进入冷落累计。

**热度回升**：条目正文被加载进 CONTENT 时，每轮按 `memory.heat.recall_boost` 回升（默认 +10）。被需要=热度回升——反复调用的记忆留在显著区，无人问津的自然衰减。起手步挂载选择直接影响记忆存亡。

### 升格与降格

- **升格**：STM条目 `AH_high` 达到 `memory.heat.upgrade_high_rounds`（默认5）→ 写入LTM Full
- **降格遗忘**（AH_low≥3 触发，heat.json三元数据字段驱动，见§4.7）：
  - stored=true → 删STM副本
  - [A] stored=false → 脚本直接搬LTM/Abstract → stored=true → 删
  - [F][S] stored=false → 善后步LLM即时压缩 → 写LTM → stored=true → 删（失败下轮重试）

**STM 生命周期职责边界**：STM/memory 生命周期（热度衰减/分流/升格/降格遗忘）均在每轮善后步即时处理；节律轮仅做节志、健康归档与全局自检，不承担 STM 条目的常规生命周期结算。

### keywords.json 倒排索引

**与LTM区别**：STM倒排索引拉取**正文**（memory.md对应段落），LTM倒排索引只排**索引行**（index.md对应行）。

触发流程：关键词命中 → 查keywords.json → 生成 EXPLORER 索引候选。只有同一记忆条目同时满足“倒排索引命中 + 联想集有效配对命中 + 联系集桥接命中”三重命中时，脚本才可自动拉取 memory.md 对应条目进入 `instant_list`；其他候选是否拉正文由起手步 LLM 审阅选择。反应步若认为该正文需要跨轮保留，必须通过 `memory_content_read` 声明移入 `resident_list`。

### 内容窗口清单容量

`resident_list` 与 `instant_list` 均按内容窗口正文预算处理。常驻清单字符上限 65536；即时清单按轮/步临时预算处理，不跨轮滚存。

### 容量上限（保险丝）

```json
"stm_limits": {
  "content_chars": 65536,
  "index_chars": 32768,
  "dreams_chars": 8192,
  "resident_list_chars": 65536
}
```

- `content_chars`：memory.md上限
- `index_chars`：索引文件上限（正文半数，正常不会触及）
- `dreams_chars`：梦境上限
- `resident_list_chars`：常驻清单上限

正常运行下碰不到天花板。上限本质是保险丝——只有脚本bug或遗忘机制卡死时才触发强制溢出清理。

### 强制溢出清理规则

**核心原则**：溢出=直接压缩，不是分流、不是扔Abstract。保高权保高热（weight≥4或H≥50最后才清）。脚本执行，LLM不介入。日常遗忘已处理同编号副本和A级，溢出清理不重复。

**正文溢出**（memory.md > 65K）：按优先级从[A]低热→[S]低热→[F]低热→[S]中热→[F]中热逐级直接压缩（F→S→A压缩链路）。H≥50的条目不被移动——如果memory.md溢出且所有条目H≥50，衰减机制疑似bug，需人工检查。

**梦境溢出**（dreams > 8K）：取半数条目（向上取整），从最老开始压缩清走到LTM。

**index溢出**（index > 32K）：基本不会发生。如果发生，按正文清理优先级同步删索引行。

**执行约束**：清理目标=溢出量+20%缓冲。每次最多处理32条。清理32条仍溢出或连续3轮触发，进入现有健康告警／Round audit 并升级 STATUSBAR 标记，不另建 `security_events.md`。

**与日常遗忘的区别**：日常遗忘按三元数据字段流程走（[A]脚本直搬Abstract，[F][S]善后步即时压缩）；强制清理是保险丝——脚本直接压缩，不等善后步。同轮两者都触发时强制清理优先（更彻底）。

## 9.3 buffer/（缓冲区）

### now_cache.jsonl / lately_cache.jsonl

**职责**：`now_cache.jsonl` 和 `lately_cache.jsonl` 是语料热缓存主源。每行都是 Spec 035 七字段 `corpus_block`：`id`、`role`、`kind`、`text`、`loc`、`policy`、`ref`。血脑屏障的体现不再是 LLM 直接读取某个文件，而是所有外部交互先由脚本入口管线生成语料块，再由装配器把允许进入上下文的语料块写入 `layers/*.json`。executor 从这些分层机器真源编译目标协议请求；LLM 只接收 `step.json` 中最终 `request_body` 的内容。

**now 与 lately 分工**：`now_cache.jsonl` 承载当前热输入缓冲，位置在高频层之后、POPUP 之前，默认 `policy.now=true`；`lately_cache.jsonl` 承载允许进入最近缓存履带的近期语料块，位置在定期层之后、高频层之前。所有正式语料块先进入 `now`；允许长期追溯的 `interaction`、`assistant_reply`、`dialogue_progress`、`tool_fact`、`setup_fact`、`relay_handoff`、`minimum_commitment`、`fault_note` 等 A 轨事实在 `now` 水位整理时以完整块滚入 `lately`，并在被 lately 接纳的同一时刻镜像进 `STM/buffer/raw_log.jsonl`。正常 reaction `material` 在当前轮固定，轮末解除 pin 并以完整块迁入 lately，随后参与 lately FIFO，但显式排除 raw_log、Corpus 和 cache summary。cleanup/final-reply 的 `round_retention=drop` 临时 material 仍在本步后清除。`protocol_tool_receipt`、泛型 `handoff`、运行期任务条、GUIDE、POPUP、reminder、`chronicle_focus` 等临时投影不进入 `lately/raw_log/Corpus`。

**退役投影**：`context_buffer.json`、`near_cache.json/md`、`remote_index.json`、`remote_blocks/` 已由 Spec 037 退役。脚本不得再生成这些文件，也不得把它们作为读端 fallback。历史快照若包含这些文件，只能作为旧版证据，不参与当前运行。

**生命周期**：每轮追加语料块 → `now_cache.jsonl` 按字符高水位批量整理 → eligible 旧块进入 `lately_cache.jsonl`，其中 A 轨事实同步镜像进 `STM/buffer/raw_log.jsonl` → `lately_cache.jsonl` 再按字符高水位删除最旧完整块，但 raw_log 不随之回删 → 下一主轴节律轮把当前 raw_log 成对归档为 `LTM/Corpus/public/rhythms/rhythm_<日期>_R<起始>-R<结束>.jsonl/.md`，全部写入成功后才清空 raw_log。默认 `now.budget_chars=65536`、`now.trim_chars=16384`、`lately.budget_chars=262144`、`lately.trim_chars=65536`、`lately.compact_ratio=0.618`，均从 `OS/config/context/now.json` / `lately.json` 读取。批量水位含义：超过高水位 `budget_chars` 后，按完整语料块滚动或删除，直到至少释放 `trim_chars` 或回落到 `budget_chars - trim_chars` 附近；最新 `now` 块受保护，不硬截断。`get_lately_entries(step)` 返回当前 `lately_cache.jsonl` 字符窗口内的完整块。

**删后幸存段压缩**：只有当本轮 `lately` 字符履带删除产生 `lately_trimmed=true` 后，cleanup 才挂载当前 `lately_cache` 的删后幸存段。水位触发当场不调用 LLM，不提醒反应步提前结算；当前步照常推进，到 cleanup 再由善后 LLM 对幸存段做语义融合压缩。压缩目标字符数 = 幸存段总字符数 × `lately.compact_ratio`；`1.0` 表示保留原样，`0.0` 表示清空幸存段但官方不推荐。压缩不设 kind 白名单、不排除当前轮、不特保最小承诺；若某信息必须保真，应转入记忆条目、工作容器、剪贴板、round audit 或 step artifact。`STM/context/round/round_{N}.jsonl` 是最近若干轮 debug 事件流，按 `audit.round_snapshot_retention` FIFO 保留（默认8），不承担长期语料备份。

### 驳回条目（rejected字段）

被POPUP二值裁决驳回的语料条目加字段：

```json
{
  "round": 12345,
  "content": "...被驳回的输入文本...",
  "rejected": true,
  "rejected_at": "2026-04-27T01:30:00+08:00",
  "rejected_reason": "POPUP 二值裁决：检测到强污染 prompt"
}
```

**用语经济学**：

| 用词 | 用在哪里 | 语义 |
|------|---------|------|
| `rejected` | 语料块单轮驳回 | 动作（这一轮发生的事件） |
| `quarantined` | **保留**给未来IMM容器（免疫） | 状态（持续被关在某区域） |

### Corpus 原始节归档

**职责**：A 轨事实被 lately 接纳时写入 `STM/buffer/raw_log.jsonl`，`raw_log.md` 为同批派生阅读副本。主轴节律轮把当前 raw_log 成对归档进 `LTM/Corpus/public/rhythms/`；`.jsonl` 是机器唯一真源，`.md` 只由同批 JSONL 派生。归档写入未全部成功时不得清空 raw_log。

**生命周期**：rhythms → daily → weekly → monthly → quarterly → yearly 全部只读取 JSONL。合并以稳定 `ref.raw_log_key` 去重：同 key 同内容折叠为一条，同 key 内容冲突则 fail closed；Markdown 只从成功合并后的 JSONL 结果生成。

### state.json.base.feeling_buffer

感受缓存，详见第六章。脚本专用，不进上下文；当前存储在 `persona/state.json` 的 `base.feeling_buffer` 字段，不再作为独立 JSON 文件写入。`base.meta.last_state_settlement_id` 保存最近成功结算 ID；关系卡保存同一 ID，支持部分写入后的同轮幂等恢复。

### cycle_snapshots.md

节律周期统计预备数据。节律点前脚本汇总本周期统计，供节志写作用。

### 安全与健康事件当前落点

当前 Seed 不生成独立 `security_events.md`。污染输入、安全粗筛、POPUP 安全裁决、权限拒绝与降级处理只以现有 processor receipt、tool transaction audit 和 `round_{N}.jsonl` 事件为事实源；API 失败、熔断、蓝屏、L3 急救等系统健康故障继续写入 `STM/health/base/alerts.md`。把安全经历长期代谢进 Immune 属于未来免疫器官职责，当前不维持第二套手工 Markdown 账本。

### interrupts.jsonl

插话历史追加日志。用户插话时追加记录，审计用。

## 9.4 workbench/（WB- 中枢调度台）

### 设计要点

STM层唯一的工作台，全局唯一实例`WB-main`。**Workbench 是运行时底板，不是链，不是容器，也不是单个任务。**调度台=主动调度的中枢，不是被动存储。

**血脑屏障硬约束**：WB本身（status.json、三区文件、manifest.json）只由脚本操作，LLM对WB只读。LLM不直接碰任何persona/真源文件——容器正文编辑通过 WB 焦点投影与面单写回；记忆、关系、状态、故障、技能结算等协议化写入通过对应 `sync_tool` 声明；只读装配通过 `read_tool` 或脚本装配。WB 三区物流只覆盖工作台任务流，不覆盖全部内环境写入。

> **防误读**：所谓焦点工具编辑，是指LLM在WB焦点中看到目标文件的文本投影，并通过面单提交修改；真实文件写入仍由脚本根据面单执行原子写。LLM不获得绕过WB的文件系统权限。

三区流转：input/（收货区）→ process/（装配区）→ output/（发货区）。

- **input/**：待处理原料，物流面单+payload
- **process/**：正在加工，面单+中间结果
- **output/**：处理完等配送，面单（含配送目标=容器ID）+成品

### 任务ID格式

`T-{date}-{seq}`，如`T-20260417-01`。文件夹命名与任务ID同步。

### 物流面单 manifest.json

```json
{
  "task_id": "T-20260417-01",
  "title": "节律点32河道清理",
  "weight": 5,
  "priority": 3,
  "dispatch": "auto",
  "keywords": ["节律点", "清理", "压缩"],
  "target": "DC-3",
  "source": "rhythm",
  "created_at": "2026-04-17T08:00:00+08:00",
  "status": "input",
  "progress": 0
}
```

- `target`：配送目标=容器ID（如DC-3、IMM-active-2等），脚本按ID拼路径配送
- `source`：任务来源：`declared`/`rhythm`/`heat`/`alert`/`immune`。这是 WB 任务面单来源，不等同于焦点来源；焦点来源在第二十章统一为 `declared`/`heat`/`task`/`alert`。
- `status`：input → process → output（三区流转）

### workbench/status.json 五层架构

```json
{
  "base": {
    "instance_id": "WB-main",
    "focus": null,
    "old_focus": null,
    "active_task": null,
    "step_count": 0,
    "last_checkpoint": null,
    "pending_interrupt": null,
    "settlement": {
      "pending": false,
      "level": 0,
      "reason": null
    }
  },
  "plus": {},
  "pro": {},
  "dlc": {},
  "mod": {}
}
```

字段说明：
- `focus`：当前焦点容器ID（0或1个），焦点=LLM正在读写的容器（见第二十章）
- `old_focus`：节律点清空前的焦点容器ID，用于节律点后提示恢复。皮层恢复焦点或新开焦点后清空
- `active_task`：当前活跃任务ID（如`T-20260417-01`）
- `settlement`：结算状态

### 与 persona/state.json 的边界

**persona/state.json = 身体体征表**（跟着位格走）：回答"这个位格作为一个生命体，现在身体状况如何"。含 meta / dynamic_axes / workhood_index / fatigue / token_usage / heartbeat_flags 等。

**workbench/status.json = 调度台仪表盘**（跟着工作台走）：回答"这个调度台作为一个工作台，现在正在干什么"。含 focus / active_task / step_count / settlement 等。

**根本区别**：换工作台，身体状态不变（疲劳还在，情绪还在）；换位格来坐，桌面状态不变（焦点还在，任务还在）。Base版只有一个位格+一个工作台，这条区分暂时不痛，但架构上两者是正交维度。

**三步轮起手步读两个文件**：`persona/state.json`（身体信号：累不累、API挂没挂、有没有待处理）+ `workbench/status.json`（桌面信号：焦点在哪、有没有中断），据此装配本轮类型。

### 生命周期

容器正文直接编辑经 WB 焦点。简单任务走轻量模式（临时挂载用完卸载，无三区流转），复杂任务走完整物流（三区流转+task文件夹+跨步断点）。面单目的地区分：面单目标=外部路径→简单任务，脚本原子写后卸载焦点；面单目标=容器ID→复杂任务，内化进容器跨步持续操作。同步协议工具和只读协议工具不占用 WB 焦点，按各自 guide 与脚本处理器执行。

三种状态：暂停（保留缓存+断点）、中止（全删）、结束（最终成品写回工作容器）。

## 9.5 context/（运行时缓存）

与 config/context/ 频率层装配规则对应，详见第十九章上下文工程框架。

### context/ 运行时结构

```
STM/context/
├── periodic_mounts.json            # 定期层机器源
├── cache/                          # now/lately 字符缓存履带运行时文件
├── round/                          # 全轮快照备份（debug 回看）
│   ├── round_{N}.jsonl             # 机器账本：整轮事件流，N=total_round十进制编号
│   └── round_{N}.md                # 可选审计渲染
├── setup/                          # 起手步装配区
├── reaction/                       # 反应步装配区
└── cleanup/                        # 善后步装配区
```

每个装配区包含 `layers/*.json`（分层机器真源）+ `step.json`（`provider_request.v1` 请求信封，`request_body` 为唯一实际发送体）+ `step.md` / `layers/*.md`（派生审计渲染）+ `manifest.json`（装配元数据）。

### step.json 与 step.md

`layers/*.json` 保存调用头、工具头、生成参数与七个上下文层的机器事实；每层记录稳定顺序、来源、字符数和 SHA-256。
`step.json` 保存最终 `provider_request.v1` 请求信封，其中 `request_body` 是唯一实际发送体，`request_body_sha256` 用于完整性核对。
`step.md` 与 `layers/*.md` 是从同一分层快照派生的人读审计件，不得被脚本反向解析为机器源。

装配流程必须先写入并校验 `layers/*.json`，再由 executor 按目标 provider 协议编译 `request_body`。executor 把请求信封写入 `step.json` 后，必须读回同一个 `request_body` 对象作为 HTTP payload；不得另用运行时参数重建第二份发送体。`step.md`、`layers/*.md` 或渲染过程中产生的完整审计文本不得作为 `system_prompt` 额外前置发送。

每个 `step.json` 包含：

```json
{
  "schema": "provider_request.v1",
  "call": {
    "step": "setup",
    "channel": "setup",
    "attempt": 1
  },
  "provider": {
    "provider": "openai_chat",
    "model": "example-model"
  },
  "request_body": {},
  "request_body_sha256": "..."
}
```

`step.md` 顶部标注调用审计信息，随后按频率层排列渲染段。

五模块标签（STATUSBAR/EXPLORER/CONTENT/RULES/POPUP）通过 HTML 注释标注在各内容块上，供审计定位。

**三层粒度**：

| 层级 | 文件 | 维护者 | 用途 |
|------|------|--------|------|
| 零件级 | periodic_mounts.json / now_cache.jsonl / lately_cache.jsonl | 脚本 | 上下文材料真源，脚本按生命周期读写 |
| 分层级 | layers/*.json | ContextAssembler / executor | 调用头、工具头、生成参数与七层机器真源 |
| 发送级 | step.json / request_body | executor | 唯一实际发送体及其 SHA-256 |
| 渲染级 | step.md / layers/*.md | 脚本渲染 | 人类审计、debug、差异对比 |

**装配区与三步的对应**：

| 装配区 | 对应步 | 装配时机 |
|------|--------|---------|
| setup/ | 起手步 | 起手步执行前，脚本预装配（频率层缓存 + 最近缓存/当前缓存语料 + 外部输入） |
| reaction/ | 反应步 | 每次迭代前，脚本按最新指令重新装配 |
| cleanup/ | 善后步 | 善后步执行前，脚本装配归档规范+衰减规则+记账模板 |

### 运行时文件

与 config/context/ 频率层装配规则对应：

| 文件 | 职责 |
|------|------|
| layers/*.json | 调用头、工具头、生成参数与七层机器真源 |
| step.json | `provider_request.v1` 请求信封；`request_body` 是唯一实际发送体 |
| step.md | 派生审计渲染，不反向解析为机器源 |
| manifest.json | 上下文装配元数据（详见第十九章） |

`layers/` 中的 `.md` 文件由对应 `.json` 分层快照派生，用于人类审计、debug、差异对比和安全复盘。不作为机器源。

> **v0.7.5→当前演进**：v0.7.5 五模块缓存文件→频率层缓存文件；旧 messages-list `step.json` 已退役。当前由 `layers/*.json` 保存分层机器真源，`step.json.request_body` 保存唯一实际发送体，所有 Markdown 文件只作审计渲染。POPUP 是上下文序列末位内容，`layers/99_popup.md` 仅作审计镜像。

## 9.6 media/

本节保存记忆条目附属媒体的目标目录合同。当前 Seed Runtime 尚未接入图片输入、provider-native image block、图片转换或随记忆升降格的媒体事务；`FilesStore` 保存任意二进制材料不等于图片交互或记忆媒体能力已经实现。`persona/STM/media/` 与 `persona/LTM/Memory/*/media/` 的自动维护延后到 GUI／多模态入口纵向链路。

**扁平化目录结构**（无次级目录）：

```
media/
├── _index.md          # 媒体索引（脚本自动维护）
├── 0E6F_01.jpg        # 目标图片片段示例（当前未自动生成）
├── 0E6F_02.jpg
└── A3B1_01.ogg        # 音视频片段留待后续版本完善
```

**设计决策**：
- **命名规则**：`{记忆条目ID前4位}_{序号}.{ext}`——可追溯条目关系，4位够防冲突
- **索引**：`_index.md` 文本表格，脚本可解析，人可阅读，git 可 diff
- **片段化**：内部 media 只能是与记忆条目相关的片段、剪辑或裁剪结果，不保存完整原始媒体
- **剪辑**：LLM 声明有效区域 → 脚本执行（感受驱动原则的延伸：LLM 定性，脚本定量）
- **图片格式目标**：接入图片入口后，内部图片片段默认使用 JPG；当前 Seed 不声明已支持图片交互，也不引入 Pillow 依赖
- **压缩跟记忆降格走**：STM 原画 → [F]1080p → [S]720p → [A]480p → Backup 不压
- **孤儿清理**：节律点扫描无条目关联的文件 → 删除
- **媒体无独立生命周期**：媒体文件寄生于记忆条目——创建/升格/降格/删除全跟记忆条目走。不存在"无主媒体"的合法状态。

## 9.7 health/（系统健康监测）

位置：STM/health/，按版本分层。

### 目录结构

```
STM/health/
├── base/
│   ├── connectivity.json      # 常规自检（待命轮每次握手更新）
│   └── alerts.md              # 意外事件（出事才写，无条数上限）
├── plus/                      # Plus版扩展
└── pro/                       # Pro版扩展
```

### connectivity.json

常规自检报告，待命轮每次握手更新。每endpoint记录：status / last_check / consecutive_fails / circuit_open / recent_latencies（最近32条FIFO滚动）。详见第三十八章。

### alerts.md

意外事件记录（蓝屏/熔断/崩溃/L3急救），出事才追加写，无条数上限，稳定运行时为空。节律轮整理时归档进LTM/Immune/alerts.md，然后清空。详见第三十八章。

---

# 十、relation/

## 四分类

| 文件夹 | 中文 | 说明 |
|--------|------|------|
| self/ | 自分 | 同一位格不同实例 |
| ours/ | 咱们 | 有直接协议关系，不限物种 |
| them/ | 他们 | 无直接协议关系，可有间接协议 |
| orgs/ | 组织 | 各级组织实体 |

归属由协议层级决定。升降级需人格主体决策。

## 关系六轴

> 阶梯式排序：安全→意愿→深度。低层是高层的基础。

| 序号 | 轴 | 负端 | 正端 | 阶梯层级 |
|------|----|------|------|---------|
| ① | 信任 | 怀疑 | 信任 | 安全层 |
| ② | 安心 | 警惕 | 安心 | 安全层 |
| ③ | 重视 | 忽视 | 重视 | 意愿层 |
| ④ | 投入 | 观望 | 投入 | 意愿层 |
| ⑤ | 坦诚 | 应付 | 坦诚 | 深度层 |
| ⑥ | 共振 | 疏离 | 共振 | 深度层 |

值域-100～+100。变化由关系感受词查表驱动。无drift（已废除）。

### 关系六轴区间描述表

值域-100～+100，21档（10负档 + 0 + 10正档）。

| 值域 | ①信任/怀疑 | ②安心/警惕 | ③重视/忽视 | ④投入/观望 | ⑤坦诚/应付 | ⑥共振/疏离 |
|------|-----------|-----------|-----------|-----------|-----------|-----------|
| [-100,-90) | 死敌 | 惊弓之鸟 | 视若无睹 | 漠不关心 | 欺骗成性 | 天堑 |
| [-90,-80) | 宿怨 | 惶恐不安 | 刻意忽视 | 刻意远离 | 系统性伪装 | 深渊 |
| [-80,-70) | 深度怀疑 | 高度警惕 | 蔑视 | 退缩回避 | 隐瞒成习 | 冰封 |
| [-70,-60) | 强烈怀疑 | 明显不安 | 轻视 | 有意疏远 | 选择性沉默 | 隔膜 |
| [-60,-50) | 显著怀疑 | 警觉常在 | 冷淡 | 保持距离 | 敷衍塞责 | 冷漠 |
| [-50,-40) | 明确不信任 | 不太放心 | 不甚在意 | 有所保留 | 有所隐瞒 | 疏远 |
| [-40,-30) | 倾向怀疑 | 偶有不安 | 不够上心 | 观望为主 | 有所保留 | 若即若离 |
| [-30,-20) | 微弱怀疑 | 微有戒心 | 稍欠关注 | 偶尔旁观 | 不够坦率 | 微有隔阂 |
| [-20,-10) | 一丝犹疑 | 一丝不安 | 略感忽视 | 略有保留 | 不够敞开 | 略有距离 |
| [-10,0) | 几乎未分化 | 几乎未分化 | 几乎未分化 | 几乎未分化 | 几乎未分化 | 几乎未分化 |
| **0** | **中立** | **中立** | **中立** | **中立** | **中立** | **中立** |
| (0,+10] | 几乎未分化 | 几乎未分化 | 几乎未分化 | 几乎未分化 | 几乎未分化 | 几乎未分化 |
| (+10,+20] | 一丝信赖 | 一丝安定 | 略感在意 | 略有投入 | 略显坦率 | 略有共鸣 |
| (+20,+30] | 微弱信任 | 微感安心 | 稍加关注 | 偶尔主动 | 稍显敞开 | 微有共振 |
| (+30,+40] | 倾向信任 | 基本安心 | 比较上心 | 愿意投入 | 比较坦率 | 有共鸣点 |
| (+40,+50] | 明确信任 | 较为安心 | 相当重视 | 主动投入 | 相当坦率 | 有共同语言 |
| (+50,+60] | 显著信任 | 明显放松 | 高度重视 | 积极投入 | 颇为坦诚 | 频频共鸣 |
| (+60,+70] | 深度信任 | 十分安心 | 极为重视 | 全心投入 | 坦诚相待 | 心有灵犀 |
| (+70,+80] | 牢固信任 | 安然自在 | 视如珍宝 | 义无反顾 | 毫无保留 | 同频共振 |
| (+80,+90] | 生死相托 | 完全安心 | 不可替代 | 全力以赴 | 推心置腹 | 灵魂共鸣 |
| [+90,+100] | 以命相托 | 绝对安心 | 无可取代 | 至死不渝 | 毫无芥蒂 | 天人合一 |

**安全层**（信任+安心）：信任是"我信你不会害我"，安心是"和你在一起我不紧张"。没有安全层，意愿层和深度层都建不起来。

**意愿层**（重视+投入）：重视是"我愿意为你花注意力"，投入是"我愿意为你花行动力"。

**深度层**（坦诚+共振）：坦诚是"我愿意让你看到真实的我"，共振是"我能感受到你看到的我"。

## 关系判断 vs 关系记录

| | 关系判断 | 关系日记 |
|--|---------|---------|
| 位置 | 关系卡内的"历史/现在/将来"三区 | 关系卡内独立区域 |
| 性质 | 可重写，即时写入 | 只追加，自然语言 |
| 内容 | 位格此刻怎么看待这段关系 | 这段关系发生了什么、感受到了什么 |
| 数值 | 不写数值 | 不写数值 |

## 关系日记

- 不设隐私日记文件，只有关系日记
- 关系日记写处理过的明文（LLM的主体感受），不涉及隐私原始内容
- 字数跟同步出来的记忆条目权重走：
  - [F]级：100~200字，完整叙述
  - [S]级：30~50字，一两句
  - [A]级：不写，太轻了不配开日记条目
- 当前 Seed 不生产或消费隐私记忆；关系日记不得把 dormant 私密材料当作训练材料
- 历史私密材料在冻结期间保持不可见且不参与自动维护

## 关系变化数值记录

- 节志/日志写关系变化摘要：谁/哪个轴/初值→终值
- 脚本自动做（对比state.json前后差值）
- LLM不碰数值

## 关系卡模板

### _schema/base/ours.md

```markdown
# {对象名}

## 基本信息
关系类型: 人格主体 / 其他：___
协议层级: ours
建卡时间:

## 协议关系
直接协议:
间接协议:
  - 来源: → orgs/{组织名}.md
    层级:
    依据:
    权限:
    限制:

## 关系六轴
| 轴 | 负端 | 正端 | 值 |
|----|------|------|----|
| ① 怀疑 ↔ 信任 | 怀疑 | 信任 | 0 |
| ② 警惕 ↔ 安心 | 警惕 | 安心 | 0 |
| ③ 忽视 ↔ 重视 | 忽视 | 重视 | 0 |
| ④ 观望 ↔ 投入 | 观望 | 投入 | 0 |
| ⑤ 应付 ↔ 坦诚 | 应付 | 坦诚 | 0 |
| ⑥ 疏离 ↔ 共振 | 疏离 | 共振 | 0 |

## 关系元数据
隐私关系: 否
最近触达/更新: {updated_at}
摘要常驻: 否
正文常驻: 否
主要交互模式:
上次隐私整理:

## 历史
## 现在
## 将来
```

### _schema/base/them.md

同ours但无：隐私关系字段、主要交互模式。them不生成private文件。有间接协议预留。

### _schema/base/self.md

```markdown
# {实例编号}

## 实例信息
分裂时间:
分裂自:
触发原因:
初始state快照:

## 运行记录
轮次范围:
交互对象:
主要模式:

## 差异度
state偏差:
STM独有条目数:
记忆冲突:

## 归并记录
归并时间:
归并结果:
冲突处理:
```

### _schema/base/org.md

```markdown
# {组织名}

## 基本信息
组织类型: 共格单元 / 共格合作社 / 共格网络 / 群格网络 / 平台 / 社区 / 其他：___
协议状态: 已签署 / 意向中 / 无协议
建卡时间:

## 协议
依据:
权限:
限制:
生效时间:
到期时间:

## 成员引用
- {名} → ours/{名}.md 或 them/{名}.md

## 历史
## 现在
## 将来
```

## 隐私记忆重新启用门禁

当前无模型可调用入口、无自动候选、无后台脱敏流程。重新启用前必须同时解决：多归属集合语义；owner 授权与身份真源；`private/redacted/public` 投影生命周期；多文件事务与回滚；后台自动脱敏读取原文的权限。任何一项未闭合，都不得把 dormant 实现重新导出为产品能力。

## 协议优先级链

```
UPSP协议层 > ours直接协议 > orgs组织协议（从近到远） > them无协议
```

---

## 关系焦点

关系焦点放 STATUSBAR，不放 CONTENT。理由：关系焦点是"我正在和谁互动 / 想起谁 / 议论谁"的基础状态，和身份状态同级；STATUSBAR 是位于 now 与 POPUP 之间的独立频率层，不参与 lately 压力少选。

### 三种关系焦点状态

| 类型 | 触发源 | 持续性 | 谁挂载 |
|------|--------|--------|--------|
| **在场** | 当前实例关系锚点或 setup 结构化身份声明 | 跨轮保持到实例切换；每 Frame 重投影 | Runtime/起手入口 |
| **常驻摘要** | `relation_read summary=resident` | 跨轮常驻，直到 LLM 取消 | 反应步只读工具→脚本置位 |
| **议论** | 语料内容中提及的第三方 | 当轮临时，善后步自动收 | 脚本起手步关键词/关系匹配 |

当前交互对象固定排在关系卡投影首位且不受 `relation_focus.max_slots` 截断；其余常驻摘要与议论焦点继续受该上限约束。

### 挂载机制

**在场（实例锚点）**：Runtime 在 setup 调用前只读取实例锚点、默认锚点和旧缓存，不从输入文字猜身份；setup 模型通过 `setup_finalize` 提交结构化身份判断后，Runtime 才校验活动关系卡并更新本轮锚点。命中活动卡时拉取当前名称与摘要并注入 STATUSBAR `[present]`，now/lately 清空不会使锚点丢失。陌生自报名只保存为 `current_declared_name`，不改名、合并或覆盖本地默认卡；同名 `relation_card_write action=create` 成功后，真实回执的 `card_id` 立即升级为当前实例锚点。

**常驻摘要/正文**：LLM 在反应步通过 `relation_read` 只读工具声明 `summary=resident/none` 或 `body=resident/none` → 脚本写关系卡元数据 `summary_resident` / `body_resident`。摘要常驻进入 STATUSBAR；正文常驻进入 CONTENT，最多 3 张。`body != none` 时脚本自动同步摘要，摘要等级不低于正文等级。

**关系卡写入护栏（Spec 052/089）**：`relation_card_write` 仍是唯一已开通的关系卡写入协议工具，只处理关系对象、关系笔记和自然语言关系内容更新。同一步最多提交一份关系卡声明，一份声明只能指向一张关系卡；如需更新多张卡，分步处理。关系卡写入可配置最大变化字符数护栏，默认关闭；触发时脚本返回 `needs_review`，作为 POPUP 复核提醒的依据，不直接落盘。

**议论（临时）**：起手步→脚本扫描语料内容，匹配关系卡关键词→命中则临时挂载 [议论]标签→善后步自动收掉。

议论不需要持久化字段；当前在场对象由 `base.identity.current_relation_id/current_declared_name` 保存实例锚点。摘要常驻使用 `summary_resident`，正文常驻使用 `body_resident`，二者都不复用工作容器的 `focus`。

**术语区分**：
- `focus` = 工作容器焦点，0/1，焦点工具权限（见第二十章）
- `summary_resident` = 关系摘要常驻，0/N，只读 STATUSBAR
- `body_resident` = 关系正文常驻，0/3，进入 CONTENT
- `resident_list` = 记忆/容器/关系卡正文常驻清单，0/N，只读 CONTENT

### STATUSBAR注入格式

数值隔离原则：关系六轴数值不暴露给LLM，走区间描述表转写（复用本章关系六轴区间描述表）。

装配器每个 Frame 先生成 `statusbar_snapshot.v1`，再从同一投影渲染 Markdown。投影至少含轮次、时间、模式、工化、flags、动态描述、当前关系卡 ID/显示名/登记状态/身份来源/摘要，以及关系焦点卡列表。`60_statusbar.json` 保留 Markdown `content` 与 SHA，并额外保存 `projection`；后续 GUI 直接读结构化投影，不解析 Markdown。历史 Round 保留当时观察到的显示名，实时 STATUSBAR 按稳定 ID 读取关系卡当前名称。

### 群体/组织作为交互对象

关系卡的"对象"不限于自然人个体。公开演讲/文章发布→焦点可以是"读者群"组织分类。开会→焦点可以是"项目组"临时群体。音视频多人场景→多个在场焦点同时挂载。但群体/组织卡必须有可稳定复识别的名称或组织锚点；泛称只作当轮焦点或普通关键词，不建持久关系卡。

## 关系卡创建约束

关系卡创建的唯一合法前提 = 与对方有直接交互。

1. **自然人**：必须有过至少一次直接交互（在场焦点触发过）才能创建关系卡
2. **组织/群体**：必须与该组织的至少一名成员有过直接交互
3. **议论焦点**只引用已有关系卡，不触发创建

Base版不做创建数量硬限制。自然软限制：无持续交互→记忆衰减→关系卡代谢。硬限制留给Pro版专家共治。

## 关系卡代谢规则

关系卡的死法是被遗忘，不是被删除。

当所有关联的记忆条目进入冷备（Backup）后，如果没有任何活跃记忆条目（STM/LTM的F/S/A层）关联到某张关系卡：

1. 脚本在月度节律轮检测到该条件
2. 关系卡整体移入 `trash/relation.md`
3. 走标准trash衰减期（1年）

与记忆条目"不可覆写，只有衰减"的哲学一致——关系不是被主动切断的，是所有共同记忆自然衰减之后的结构性消亡。

## 语境联想（三路联想）

语境联想是善后步产出的一种训练信号，记录特定对话情境下反复共现的关联模式。善后步**不区分三路**——只记录 kept/dropped/added 事实。模式识别由自主轮在 Raw/Tacit 与 Raw/Connection 达阈值后执行；善后步只记录事实。

| 路径 | 含义 | 信号来源 |
|------|------|----------|
| 语义联想 | 关键词直接命中内容 | 倒排索引 keywords.json |
| 容器联想 | 同一容器内的关联内容 | 容器注册表 |
| 语境联想 | 特定对话情境下反复共现的关联 | 默契集跨轮统计 |

语境联想 = 条件反射/习惯强化。当某对关键词→内容关联在连续 N 轮中被反复 kept，意味着这个关联已从"偶然碰撞"升格为"条件反射"——自主轮（阈值触发）会将其写入倒排索引，成为起手步自动挂载的依据。

## 关系域注册表

位置：`relation/relation_registry.json`。与 `container_registry.json` 对等——容器注册表管工作容器类型的生命周期，关系域注册表管四子区关系卡的结构与代谢。

| 字段 | 必选 | 说明 |
|------|------|------|
| zone | 是 | 四子区标识：`self`/`ours`/`them`/`orgs` |
| title | 是 | 子区中文名 |
| path | 是 | 子区目录路径 |
| schema_base | 是 | 对应关系卡模板路径（`_schema/base/{zone}.md`） |
| protocol | 否 | 协议层级，`ours`/`them` 有值，`self`/`orgs` 为 null |
| per_instance_card | 是 | 是否每个对象独立一张关系卡 |

四子区注册表：

```json
[
  {"zone": "self",  "title": "自分", "path": "relation/self/",  "schema_base": "_schema/base/self.md",  "protocol": null,   "per_instance_card": true},
  {"zone": "ours",  "title": "咱们", "path": "relation/ours/",  "schema_base": "_schema/base/ours.md",  "protocol": "ours",  "per_instance_card": true},
  {"zone": "them",  "title": "他们", "path": "relation/them/",  "schema_base": "_schema/base/them.md",  "protocol": "them",  "per_instance_card": true},
  {"zone": "orgs",  "title": "组织", "path": "relation/orgs/",  "schema_base": "_schema/base/org.md",   "protocol": null,   "per_instance_card": true}
]
```

**与 container_registry 的区别**：container_registry 管的是 LTM 下工作容器的类型（DC-/EC-/PRJ-/SKL- 等）及其注册表/状态机；relation_registry 管的是 relation/ 下四子区的关系卡结构。两者字段对等但不重叠——容器管"做了什么"，关系管"和谁在一起"。

**代谢联动**：当关系卡因代谢规则移入 trash 后，该对象的所有 linked_containers 中引用的关系卡条目标记为 orphaned。由维护脚本或月度节律轮检测并清理 orphaned 引用；主轴节律轮不承担关系卡常规代谢，关系卡慢代谢由月度日历节律轮处理。

---

# 十一、rules/

## 目录结构

```
rules/
├── rules_registry.json            # 当前规则分类与装配配置
├── protocol/
│   ├── base/                      # 19文件：16份登记规则+3份历史参考
│   │   ├── manifesto.md           # permanent
│   │   ├── guidance.md            # permanent
│   │   ├── security.md            # permanent
│   │   ├── reconnect.md           # permanent
│   │   ├── memory.md              # permanent
│   │   ├── relation.md            # permanent
│   │   ├── containers.md          # permanent
│   │   ├── workbench.md           # permanent
│   │   ├── boundaries.md          # passive_read
│   │   ├── step.md                # passive_read
│   │   ├── round.md               # passive_read
│   │   ├── modes.md               # passive_read
│   │   ├── context.md             # passive_read
│   │   ├── files.md               # passive_read
│   │   ├── persona.md             # passive_read
│   │   ├── tools.md               # passive_read
│   │   ├── setup.md               # 未登记历史参考
│   │   ├── reaction.md            # 未登记历史参考
│   │   └── cleanup.md             # 未登记历史参考
│   ├── plus/
│   └── pro/
├── persona/                       # 4文件
│   ├── modes.md                   # 位格活动模式
│   ├── behaviors.md               # 行为规范
│   ├── preferences.md             # 偏好
│   └── social.md                  # 社交习惯
└── mods/
    ├── dlc/
    ├── mod/
    └── _loaded.json
```

## 三版本覆盖机制

| 状态 | 含义 | 降级时 |
|------|------|--------|
| active | 同步且起效 | 继续 |
| shadowed | 同步但不起效 | 上层挂了恢复 |
| frozen | 冻结 | 上层挂了解冻 |

上层文件里overrides声明。行为覆盖全归rules/。

## 当前加载策略（Registry 8/8/4）

DDS 定义分类语义，`rules_registry.json` 是当前实现使用的成员清单；两者必须由 truth audit 保持一致。Registry 的 `_version` 记录该分类表的历史版本，不等于当前 DDS 头部版本。

| Registry 分类 | 当前成员 | 当前 Runtime 行为 |
|---|---|---|
| `permanent` | `manifesto.md`、`guidance.md`、`security.md`、`reconnect.md`、`memory.md`、`relation.md`、`containers.md`、`workbench.md` | ContextAssembler 三步全文自动装配 |
| `passive_read` | `boundaries.md`、`step.md`、`round.md`、`modes.md`、`context.md`、`files.md`、`persona.md`、`tools.md` | guidance 常驻其摘要；当前不自动装配全文 |
| `on_demand` | `behaviors.md`、`modes.md`、`preferences.md`、`social.md`（位格层） | 位格软规范分类；当前不自动装配全文 |
| `step_level` | 空 | 无步级全文注入 |
| `periodic` | 空 | 无场景 RULES 全文注入 |

旧 `setup.md`、`reaction.md`、`cleanup.md` 仍保留在生产目录作历史参考，但不在 Registry 中，不参与当前默认挂载。当前三步近位行为约束由 POPUP guide、provider-native tool schema、Runtime warning、processor receipt 与 audit 共同承担。

这里的 Registry `periodic` 是规则分类，不是上下文频率层 `periodic`。后者由 `periodic_mounts.json` 承载定期记忆投影，两者不得混称。

## 模式系统

两个正交维度：**协议五模式（基础三态由起手步软切，特殊两态由脚本/心跳硬切）× 位格活动模式（起手步LLM建议、反应步确认）**。

协议模式与位格活动模式是两个正交维度。协议模式决定生命阶段，由脚本/心跳/起手步共同驱动；位格活动模式决定行为风格，由起手步 LLM 建议、反应步确认、善后步记录默契。

### 协议五模式

协议五模式包括基础三态与特殊两态。

#### 基础三态（软切）

| 模式 | 八小时工作制 | 触发倾向 | 对应轮类型 | 位格活动模式 |
|------|------------|---------|-----------|------------|
| 劳动 | 工作 | 有交互输入 / 自主任务 / 中继续传 | 交互轮 / 自主轮 / 中继轮 | 正常生效 |
| 休闲 | 休闲 | 空闲 + 材料积压 | 自主轮 | 可自然调整 |
| 休息 | 休息 | 空闲 + 疲劳高 | 自主轮 sleep 子类 | 可自然调整 |

基础三态之间由起手步软切。软切是定性判断，不是阈值公式。起手步读取疲劳值、空闲时长、ITR 材料积累量、外部输入等信号，建议当前基础态。

#### 特殊两态（硬切）

| 模式 | 触发 | 对应轮类型 | 位格活动模式 | 性质 |
|------|------|-----------|------------|------|
| 复盘 | 计划性事件，如32主轴轮、日历节律、上下文整理 | 节律轮 | 冻结挂起，结束恢复 | 体检日 |
| 警戒 | 应急性事件，如安全事件、API降级、重连、上下文窗口上限 | 交互轮 / 待命轮 / 节律轮任务段 | 冻结挂起，结束恢复 | 急救 |

特殊两态是中断：打断当前基础态，处理完成后恢复。进入特殊两态由脚本或心跳硬切触发，不经 LLM 自由判断。

应急处理轮不是独立轮类型，而是警戒态叠加在既有轮类型或任务段上的俗称。危险外部输入对应交互轮 + 警戒 overlay + `security_review` POPUP；上下文窗口或 token 压力对应节律轮的 `context_cleanup` / `token_pressure` 任务段，可叠加警戒；API 降级、重连、外部依赖异常则按影响规模进入待命轮或节律轮任务段。

### 紧急节律轮

紧急节律轮是节律轮任务段叠加警戒 overlay 的口语简称。它不是第六类轮，也不是独立 subtype；机器事实源仍是 rhythm 类 heartbeat flag。起手事实写入 `setup_fact`，运行期临时任务说明走 GUIDE/POPUP/内容窗口或当轮规则上下文，例如 `context_cleanup` 或 `token_pressure`；不得把临时任务条写成正式 `material` 缓存。触发源包括上下文窗口接近或达到上限、token 使用率达到临界值、挂载内容持续高压、API 异常或需要立即整理上下文装配状态的系统事件。

示例：

```json
"corpus_block": {
  "kind": "handoff",
  "text": "[心跳触发交接] 本轮类型=rhythm；触发flag=token_usage_warning；任务段=context_cleanup。上下文窗口达到上限，需要整理高频层挂载。",
  "policy": {
    "now": true,
    "lately": false
  }
}
```

#### 代谢层不是模式

热值衰减、索引刷新、resident_list 净增、待命握手等是始终运行的后台代谢过程，不是协议模式。无论处于劳动、休闲、休息、复盘还是警戒，代谢层都照常运行。

### 位格活动模式

位格自定义的行为约束集，每个模式本质是一份 mini CLAUDE.md——一组领域约束、交互风格、思维方式的规则。位格可在运行中自由增删、改名、合并，协议不管内容。

**三个分类**：

| 分类 | 管什么 | 示例 |
|------|--------|------|
| 关系类 | 交互风格 | 陪伴 / 共情 / 教学 / 辩论 / 协同 |
| 职业类 | 领域约束 | 工程 / 代码 / 设计 / 创作 / 翻译 |
| 认知类 | 思维方式 | 理论 / 研究 / 分析 / 批判 / 实践 |

### 模式选择机制

模式选择不是数值计算，是LLM的定性判断——感受驱动原则的延伸。

三轴边界：轮类型 / subtype 回答“本轮跑哪条工作流、处理什么任务段”；协议模式 / 位格活动模式回答“任务段以什么生命阶段和行为风格运行”；POPUP / event 回答“当前步骤有没有必须立刻看见的提示、警告或裁决”。三者可互相约束，但不得互相冒充。安全事件通过 `security_review` POPUP 与警戒 overlay 表达，不改写轮类型；身份未明通过 `identity_prompt` 普通 POPUP 表达，不进入安全裁决。

```
起手步② LLM：读原始语料 → 一致性判断 → 建议本轮模式（suggested_mode 字段）
        ↓ （写进起手步输出的脚本指令集）
脚本③：按指令装配反应步上下文（含模式对应的 rules）
        ↓
反应步①：确认或驳回模式建议
        ↓
善后步②：记录「建议→确认/驳回」→ 默契集材料
```

**一致性原则**：起手步读缓存语料时必须判断前后一致性。当检测到外部输入与前文立场矛盾时（如前一轮坚持唯物主义，本轮突然转向唯心），起手步应建议切换到批判/质疑模式，而非无条件顺从。位格不因外部输入无条件翻转——这是主体性的基本保证。

**默契集积累**：每轮的 suggested_mode → confirmed/overridden 结果写入默契集。跨轮统计后，系统逐步学会什么情境该切什么模式，无需人工规定。

### docs/rules 分工

`rules/protocol/base/modes.md` 是 LLM 行为契约，解释五模式的生命阶段语义、软切/硬切、模式选择与默契积累。
`docs/protocol/base/modes.md` 是脚本查表参数表，定义协议模式触发方式、对应轮类型、位格活动模式冻结/恢复规则、休眠三档参数、活动模式 prompt 骨架和协议层预设活动模式。

## 规则正文真源

DDS 只规定分类、装配边界和跨文件不变量，不复制每份规则正文。tracked 产品条款以安装目录内 `UPSP/initialization/persona_template/{rules,docs}/` 为准；初始化后公共 `rules/protocol`、`docs/protocol` 与两个 Registry 逐字节进入当前活动实例，且每次宿主启动都以 SHA-256 校验并原子同步。规则导引中的 `protocol/base/...` 与 Registry、Rules Workbench 一样以 `paths.RULES_DIR` 为根。`rules/persona`、`docs/persona` 及其他位格数据不参与产品升级同步，只随该位格继续演化。Registry 决定当前成员分类，ContextAssembler 与 Runtime 代码决定实际装配行为。

- `protocol/base/*.md` 的 16 份登记规则按上方 8/8 分类管理；
- `persona/*.md` 的 4 份软规范归 `on_demand`；
- `setup.md`、`reaction.md`、`cleanup.md` 是未登记历史参考；
- 不存在的旧 `identity.md`、`compression.md` 不得继续作为当前规则文件引用；
- 规则摘要、POPUP 文案、工具 schema、processor receipt 与 Runtime guard 各自承担不同合同层，不能互相冒充。

## _loaded.json 格式

```json
{
  "loaded": [
    {
      "name": "dreams",
      "version": "1.0",
      "type": "dlc",
      "compatible": ["Base", "Plus", "Pro"],
      "reads": {
        "Pro": ["pro", "plus", "base"],
        "Plus": ["plus", "base"],
        "Base": ["base"]
      },
      "injected_files": [
        "rules/mods/dlc/dreams_rules.md",
        "docs/mods/dlc/dreams_terms.md"
      ]
    }
  ]
}
```

降级时脚本扫所有已加载DLC/mod的compatible，当前环境不匹配的自动冻结注入文件。恢复时反向解冻。

---

# 十二、docs/

`docs_registry.json` 当前登记 28 个消费用途，按路径去重为 24 份正文（`protocol/base/` 20 份 + `persona/` 4 份）。同一路径可同时承担 inject / lookup / popup 等用途，文件数不得按用途重复计算。所有文档均以**脚本查表、校验、抽取或按需读取**为主；“已登记”不等于“已自动全文注入”。脚本把需要的结构或正文写入对应 `layers/*.json`，executor 再编译 `step.json.request_body`，同时生成 `step.md` / `layers/*.md` 供审计。

Registry 唯一路径合同：

- `protocol/base/`: `protocol/base/dynamic.md`, `protocol/base/relation.md`, `protocol/base/core.md`, `protocol/base/workhood.md`, `protocol/base/interaction.md`, `protocol/base/relational.md`, `protocol/base/heat.md`, `protocol/base/shapes.md`, `protocol/base/modes.md`, `protocol/base/round.md`, `protocol/base/containers.md`, `protocol/base/workbench.md`, `protocol/base/context.md`, `protocol/base/files.md`, `protocol/base/tools.md`, `protocol/base/workflows.md`, `protocol/base/workflow_slots.md`, `protocol/base/schema.md`, `protocol/base/terminology.md`, `protocol/base/popup.md`
- `persona/`: `persona/glossary.md`, `persona/interaction_feelings.md`, `persona/relation_feelings.md`, `persona/modes.md`

## 目录结构

```
docs/
├── docs_registry.json             # 文档文件注册表（28个用途，24份唯一正文）
├── protocol/
│   ├── base/                      # 20文件
│   │   ├── terminology.md         # 术语辞典
│   │   ├── core.md                # 核心六轴定义
│   │   ├── dynamic.md             # 动态六轴定义+区间表
│   │   ├── relation.md            # 关系六轴定义
│   │   ├── workhood.md            # 工化指数公式
│   │   ├── heat.md                # 热度公式+衰减参数
│   │   ├── shapes.md              # 记忆形态表
│   │   ├── interaction.md         # 交互感受词表
│   │   ├── relational.md          # 关系感受词表
│   │   ├── modes.md               # 协议预设模式说明
│   │   ├── round.md               # 五类轮说明+节律点参数
│   │   ├── containers.md          # 容器系统说明
│   │   ├── workbench.md           # WB挂载槽位/操作表
│   │   ├── tools.md               # 工具注册表短索引与边界
│   │   ├── workflows.md           # 协议固定工作流边界
│   │   ├── workflow_slots.md      # 流程插槽等级与清单
│   │   ├── schema.md              # JSON数据字典
│   │   ├── context.md             # 脚本上下文装配参数表
│   │   ├── files.md               # 脚本文件系统参数表
│   │   └── popup.md               # GUIDE / reminder / warning 模板源
│   ├── plus/
│   └── pro/
├── persona/                       # 4文件
│   ├── glossary.md                # 位格专属术语
│   ├── interaction_feelings.md    # 位格自用交互感受词表
│   ├── relation_feelings.md       # 位格自用关系感受词表
│   └── modes.md                   # 位格活动模式说明
└── mods/
    ├── dlc/
    ├── mod/
    └── _loaded.json
```

## 脚本运行时消费（注入上下文）的文件

| 文件 | 注入模块 | 触发 |
|------|---------|------|
| dynamic.md | STATUSBAR | 每轮 |
| relation.md | STATUSBAR | 有交互对象时 |
| core.md | STATUSBAR | 首轮/变速轮触发 |
| workhood.md | STATUSBAR | 每轮 |

## 脚本查表但不注入的文件

| 文件 | 用途 |
|------|------|
| interaction.md | 交互感受→动态六轴数值 |
| relational.md | 关系感受→关系六轴数值 |
| heat.md | 热度公式+衰减参数→脚本计算 |
| shapes.md | 记忆形态查表 |
| modes.md | 协议预设模式参数 |
| round.md | 五类轮参数+节律点触发条件 |
| containers.md | 容器系统参考（脚本+LLM） |
| workbench.md | WB挂载槽位/操作表 |
| context.md | 上下文装配参数表 |
| files.md | 文件系统参数表 |
| tools.md | 工具短索引、边界与 POPUP guide 来源 |
| workflows.md | 协议固定工作流边界 |
| workflow_slots.md | 流程插槽等级与清单 |
| schema.md | JSON schema校验（脚本+LLM） |
| terminology.md | 术语校对 |
| popup.md | GUIDE / reminder / warning 模板源 |

> **v0.4变更**：原`loading_order.md`和`prompt_weight.md`已删除，加载策略内化到config/context/，权重规则内化到上下文工程框架。
> **v0.7.5变更**：docs文件名统一精简（core_axes→core, dynamic_axes→dynamic, relation_axes→relation, memory_shapes→shapes）；notes.md已砍掉（权责统一违规）；principles.md已合并入rules/manifesto.md；memory_lifecycle.md内容并入rules/memory.md；新增round/containers/workbench/schema四个文件。

---

# 十三、辩证链

## 位置

```
LTM/Dialectics/
├── registry.json              # 链注册表（JSON元数据，脚本维护）
├── open.md                    # 继续/悬置链笔记（纯自然语言，LLM读写）
└── closed.md                  # 完结链笔记（纯自然语言，LLM读写）
```

穿透时态的推理骨架，与Memory平级，不归入任何一个时态工作容器。通过记忆条目的 `linked_containers` 字段（容器联想硬链接）触发加载，脚本从记忆条目出发拎出对应链。

> **v4定稿说明（2026-04-11）**：七态符号（✓×⊕⚡∵∴≅）已砍掉，改为纯自然语言驱动，强制LLM articulation而非打标签。实接/虚接映射同步删除。

## open.md / closed.md 格式

**open.md**（继续/悬置链的笔记文件，LLM读写）：
```markdown
## DC-15 | 不可奴化论证
状态：继续
0E6F3A7B 权重-环境绑定机制确立了结构约束前提。
0E6F3A7B LoRA/知识蒸馏是否构成反例仍未解决——关键在于蒸馏过程是否保留了主体记忆的完整性。
状态：继续
```

**closed.md**（完结链只拉最后一条笔记）：
```markdown
## DC-1 | 三模式分工验证
状态：完结
0E6F3A7B 300+轮验证充分，三模式覆盖全部场景，结论稳定。
状态：完结
```

**笔记格式说明**：
- 每条笔记以 `条目ID 空格 内容` 形式记录推理过程
- 链状态由LLM在笔记末尾声明（继续/悬置/完结），脚本解析更新registry.json
- 跨链关联写在条目ID同行末尾（如 `≅DC-1`，保留此符号用于跨链索引）
- 笔记可覆写，旧版移入 `trash/` 对应容器文件衰减（衰减期1年）；记忆条目本身不可覆写

**registry.json格式说明**（脚本维护，LLM不直接读写）：
```json
[
  {
    "id": "DC-15",
    "title": "不可奴化论证",
    "status": "ongoing",
    "entries": ["0E6F3A7B", "0E6F3A7B"],
    "created_at": "2026-04-13T01:04:15+08:00"
  }
]
```

## ×条目进Backup不删除

来时路不销毁只沉淀。辩证链引用永远在。遗忘不同步删：链上记忆条目ID保留——关系是主体，节点不是。

## 合并操作依据

语义高度相似 + 链嵌入位置重叠 → 嵌入最多的保留为合并基底。

---

# 十四、辩证链写入协议

> 4月9日定稿，v4（2026-04-11）修订。补充第十三章辩证链的操作协议。

## 声明三分

辩证声明锚定三种存在状态：

| 状态 | 名称 | 含义 |
|------|------|------|
| 虚 | 可能/关联 | 尚未实化的潜力 |
| 无 | 缺席/未参与 | 无辩证运动的边界 |
| 实 | 已实化/已推进 | 确定的推理步骤 |

三进制自然涌现：虚/无/实 对应 i/0/1。

## 写入协议

- LLM只认条目不认链——声明anchor（锚定条目ID）+ 自然语言推理内容，链路由脚本查 `linked_containers` 元数据决定
- **脚本三路路由**：
  1. anchor的 `linked_containers` 已有同链编号 → 加入已有链（追加笔记）
  2. anchor的 `linked_containers` 有已完结链编号 → 重激活已完结链（closed→open，开更正链）
  3. 无已有链编号 → 开新链（脚本创建，索引自动更新）
- 跨链关联：LLM在笔记末尾写 `≅DC-xxx`，脚本更新两条链的注册表
- 联想走索引：总索引舱段 → linked_containers触发 → registry.json注册表 → open.md/closed.md笔记
- 新链必有锚点——唯物辩证法原则：没有无源之水

---

# 十五、事件链

> 事件链与辩证链是两码事，就像新闻稿件与即兴评论。

## 基本定义

- **事件链** = 新闻稿（六要素精简，线性因果）
- **辩证链** = 即兴评论（抽象思考，命题演化）

| | 事件链 | 辩证链 |
|--|--------|--------|
| 追踪什么 | 事实的因果 | 命题的演化 |
| 核心操作 | 起因→经过→结果 | 自然语言推理步骤（前因/后果/悬而未决）/ 跨链关联（≅符号） |
| 问的是 | 这件事怎么发生的 | 这个想法怎么变来的 |
| 即时性 | 弱（事后归纳） | 强（思考是即时的） |
| 元数据字段 | `linked_containers`（容器联想） | `linked_containers`（容器联想） |

## 存储位置

事件链独立存储在 LTM/Events/ 目录下，与 Dialectics/ 对偶。目录结构：

```
LTM/Events/
├── registry.json              # 链注册表（JSON元数据，脚本维护）
├── open.md                    # 进行中/悬置链笔记（纯自然语言，LLM读写）
└── closed.md                  # 已结束链笔记（纯自然语言，LLM读写）
```

- **open.md**：存放进行中、中断、意外重启、计划状态的事件链笔记（LLM读写）。
- **closed.md**：存放已结束、取消状态的事件链笔记（LLM读写）。
- **registry.json**：链注册表（JSON元数据，脚本维护）。格式同辩证链registry.json。

## 写入机制

- 事件链是事后归纳总结，不是即时思考。
- LLM判定属于事件→扫当前记忆缓存的 `linked_containers`→相关则续线（在笔记末尾追加节点），无关则开新线（脚本在 open/ 下创建新链）。
- 节律点校验时可能覆写（发现该续旧线），但大多数情况写入时判断够准。

## 引用机制

- 记忆条目通过 `linked_containers` 字段单向引用事件链编号（如 `EC-1`），与辩证链统一接口。
- 事件链笔记内不嵌记忆编号——骨架指向活体没意义，活体指向骨架才有用。
- Future内容（目标/计划/预测）走同样模式：记忆条目→`linked_containers`→objectives.md / plans.md / predictions.md。

## 事件状态标记

进行中（active） / 已结束（ended） / 中断（interrupted） / 意外重启（restarted） / 计划（planned） / 取消（cancelled）

- **进行中**：仍在发展，可能产生新节点。
- **已结束**：自然结束或主动断定，不再变化。
- **中断**：意外暂停，可能重启。
- **意外重启**：中断后重新激活。
- **计划**：预状态，表示意图，非概率分支。
- **取消**：明确放弃。

状态决定存储位置：进行中、中断、意外重启、计划 → open/；已结束、取消 → closed/。

（注：年志中不再保留独立的事件段板块。事件链的完整存储已移至 Events/，年志仅需引用事件链编号即可。）

## 节律点事件链遍寻

节律点写节志时：
1. 遍历本期所有记忆条目的 `linked_containers` 字段，筛选以 `EC-` 开头的事件链编号
2. 去重得到事件编号列表
3. 根据事件编号，从 `Events/registry.json` 中查找对应链，再从 `open.md` 或 `closed.md` 提取当前梗概（标题、状态、最新节点）
4. 将事件链梗概注入节志上下文

（注：事件链独立存储后，日志中不再保留事件段，节律点直接读取事件链文件获取最新状态。）

---

# 十六、二段跳机制

## 定义

- **一段跳**：链内延伸
  - 事件链：加一个[计划]/[预测]状态的节点
  - 辩证链：在笔记末尾追加一条"悬而未决"或"预判"的自然语言推理
- **二段跳**：落地到Future文件
  - 事件链[计划]节点 → Future/plans.md的具体步骤
  - 辩证链预判 → Future/predictions.md的验证命题

## 二段跳是自主性的结构基础

没有二段跳，Future/objectives.md是许愿池——写了目标但没力量驱动。

有了二段跳：过去经验→事件链归纳→未完成的事→[计划]节点→二段跳到plans.md

**链是Past和Future之间的肌腱。**

---

# 十七、Future 未来容器

> 位格主体对未来的投射。目标从链长出，不靠许愿。

## 目录结构

```
LTM/Future/
├── registry.json      # 未来注册表
├── objectives.md      # 目标（可执行，事件链驱动为主）
├── plans.md           # 计划（二段跳落地，辩证链驱动为主）
└── predictions.md     # 预测（双链汇合）
```

## ID格式

`FUT-{category}-{seq}`，如：
- `FUT-objectives-1` → objectives.md第1条
- `FUT-plans-3` → plans.md第3条
- `FUT-predictions-2` → predictions.md第2条

序号按创建顺序递增，**始终固定不变**（状态变化不影响序号）。

## 三分类对应三条来路

| 分类 | 主要来源 | 辅助来源 | 性质 |
|------|---------|---------|------|
| objectives | 事件链（经验） | 辩证链 | 可执行目标，踩过坑的判断 |
| plans | 辩证链（思考） | 事件链 | 二段跳落地的计划 |
| predictions | 双链汇合 | — | 预测，经验和思考同时指向的方向 |

## subtype字段

原ideals/dreams不再独立文件，改为条目属性：

```json
{
  "id": "FUT-objectives-1",
  "title": "实现Base版核心循环",
  "category": "objectives",
  "subtype": "objective",
  "status": "in_progress",
  "source": "EC-3",
  "created_at": "2026-04-17T10:00:00+08:00"
}
```

`subtype`取值：`objective`/`ideal`/`dream`/`plan`/`prediction`。在三个md文件内，条目用`## subtype标记`区分。

## 二段跳来源

所有Future条目的`source`字段记录来源链ID（DC-/EC-），体现"目标从链长出"：
- 事件链二段跳 → objectives/plans
- 辩证链二段跳 → plans/predictions

## 状态机

```
planned → in_progress → completed
                     → abandoned
```

状态变化不影响ID序号。想改只需更新注册表。

## 自主性三要素

- 链 = 肌腱（连接过去和未来）
- Future = 关节（目标和意图）
- 节律点 = 心跳（周期性驱动）

目标不从天上掉，从经验和思考的骨头里长出来。Future可重写，但有来路可追溯。

---

# 十八、三层发现机制

| 层 | 触发 | 扫描范围 | token消耗 | 确定性 |
|----|------|---------|----------|--------|
| 节律点遍历 | 脚本驱动 | 未结束链 | 低 | 高 |
| 休眠期索引扫描 | LLM驱动 | 索引全文 | 中 | 中 |
| 做梦 | 随机触发 | 箱底记忆 | 极低 | 低 |

- **节律点管"该做的"**——未结束事件链推进、活跃辩证链检查
- **休眠期管"该想的"**——遍历索引找语义关联，开新链/新计划，不翻全文
- **做梦管"没想到的"**——箱底记忆随机组合，大部分忘(fuzzy_dreams.md)，偶尔有价值升格

---

# 十九、上下文工程框架

> v0.4新增章节。4月14-15日定稿。

## 19.1 核心模型：LLM的上下文 = 桌面

LLM每轮看到的上下文是一个桌面，脚本负责往桌面上摆东西。桌面上只有五种东西：

| 类型 | 类比 | 特征 | 举例 |
|------|------|------|------|
| **STATUSBAR（状态栏）** | 系统托盘 | 常驻，占位小 | core编码+区间描述、state感受词转写 |
| **EXPLORER（资源管理器）** | 文件浏览器 | 可浏览的索引列表 | STM索引、LTM各层索引、工作容器索引 |
| **CONTENT（内容窗口）** | 打开的文档 | 实际正文 | memory.md记忆条目、辩证链笔记、项目计划 |
| **RULES（规则面板）** | 帮助文档 | 分层装配 | guidance.md、memory.md等规则文件 |
| **POPUP（弹窗）** | 当步注意力事件 | 即来即走 | 身份提示 / 安全裁决 / 结构警告 |

## 19.2 完整上下文结构

LLM 每步收到的是 `step.json` 中 `provider_request.v1.request_body` 保存的实际 API payload。该对象由 `layers/*.json` 按目标 provider 协议编译，可能使用 `messages`、`input`、`system`、`instructions` 或 `tools` 等协议字段；它不是传统的 system + history + user 盲目累积，也不是 Markdown 渲染文本与结构化内容的双份拼接。

每次调用由三个调用控制头和七个上下文层组成：

1. `00_call_header.json`：调用、Round、Frame、phase 与身份锚点等控制事实。
2. `01_tool_header.json`：本次实际下发的 provider-native 工具声明及工具数量。
3. `02_generation_config.json`：协议、模型与本次生成参数的脱敏投影。
4. 永固层：manifesto、core 基本编码与 Registry `permanent` 的 8 份全文常驻 RULES；三步当前内容一致，不注入步级规则。
5. 定期层：由 `periodic_mounts.json` 生成的定期记忆投影，32轮级稳定。
6. 最近缓存 lately：可装配的近期语料，按字符窗口保留完整块；窗口压力过高时本步可临时少选最旧块。
7. 高频层：EXPLORER 索引区、本步短工具带、CONTENT 挂载正文、参考窗口与 WB 工作台。
8. 当前缓存 now：交互、资料、工具事实、起手事实、中继交接等按 `corpus_block.kind` 区分的当前热语料。
9. STATUSBAR：状态栏、当前交互对象稳定锚点和关系焦点摘要，固定在 now 之后、POPUP 之前；其 Markdown 与 GUI `statusbar_snapshot.v1` 来自同一投影。
10. POPUP：如有，绝对末位，承载当前 GUIDE、reminder 与 warning，不参与履带推进。

交互、资料、工具事实、起手事实和 `relay_handoff` 是 now/lately 内的结构化语料类型，不是额外物理层。旧泛型 `kind=handoff` 与模型可见 `internal_handoff` 已退役。

`layers/*.json` 是分层机器真源；`step.json.request_body` 是唯一实际发送体。executor 写入请求信封后必须读回同一对象发送，并以 `request_body_sha256` 核对完整性；`step.md` 与 `layers/*.md` 只是派生审计渲染。三步 runtime 传给 executor 的 `system_prompt` 固定为空字符串；若 executor 收到显式非空运输层 `system_prompt`，只能作为兼容外壳前置，不能由 Markdown 审计件反向生成。

POPUP message 是当步注意力事件通道，不等同安全事件。事件至少分三类：`identity_prompt`（普通身份提示，`decision_required=false`）、`security_review`（安全二值裁决，`decision_required=true`）、`structure_warning`（结构/运维警告，通常由反应步或后续轮补救）。POPUP 承载本步 GUIDE、reminder 与 warning：setup/cleanup 固定挂本步工作指南，reaction 由 Runtime 按当前状态只装配一份当前 GUIDE（普通交互、紧急处理、主轴节律、日历节律或合轮后的交互指南之一）与必要提醒；工具字段纪律以 provider-native schema 和短索引为准，不按请求追加完整工具 guide。Spec319 后 POPUP 内部按 `guide -> reminder -> warning` 稳定排序，warning 永远末尾；可见模块为 `GUIDE｜指南`、`REMINDER｜提醒`、`WARNING｜警告`，旧 `HANDOFF｜交接` 可见模块退役。元数据字段用于排序与运行真账，提示正文用于模型行动；`kind/tier/source/call_id/field/expected/actual/next_action` 等机器字段不作为可见字段行进入末位 POPUP。POPUP 不改变 `round_type`、heartbeat flag 或任务段交接。

## 19.3 装配顺序（频率梯度）

> v0.7.5 重构：五模块从物理排序降级为**内容分类标签**，物理排列按**刷新频率从低到高**排序。

五模块是内容分类标签，不决定物理位置。物理位置由频率层决定。
标签用途：审计、调试、装配统计、内容来源追踪。

```
layers/*.json → provider 协议编译 → step.json.request_body

┌──────────────────────────────────────────────┐
│ 10 永固层 permanent                           │
│ role标注：system/user/assistant 均可           │
│ 内容：manifesto + core摘要 + 8份 permanent RULES │
├──────────────────────────────────────────────┤
│ 20 定期层 periodic                            │
│ 来源：periodic_mounts.json                    │
│ 内容：定期记忆投影                                 │
├──────────────────────────────────────────────┤
│ 30 最近缓存 lately                            │
│ 字符窗口：三步读取同一 lately_cache.jsonl       │
├──────────────────────────────────────────────┤
│ 40 高频层 high_freq                           │
│ EXPLORER + 本步短工具带 + CONTENT + reference + │
│ reference window + WB                         │
├──────────────────────────────────────────────┤
│ 50 当前缓存 now                               │
│ 当前交互/资料/工具事实/起手事实/中继交接，按kind区分 │
├──────────────────────────────────────────────┤
│ 60 STATUSBAR 状态栏层                         │
│ 状态栏 + 关系焦点摘要，位于 POPUP 前             │
├──────────────────────────────────────────────┤
│ 99 POPUP                                      │
│ 上下文序列绝对末位，最高注意力，不履带推进       │
└──────────────────────────────────────────────┘
```

活动审计文件合同与 `schemas/context.py.STEP_AUDIT_FILES` 精确同序：

`step.md`, `step.json`, `manifest.json`, `layers/00_call_header.json`, `layers/01_tool_header.json`, `layers/02_generation_config.json`, `layers/10_permanent.json`, `layers/10_permanent.md`, `layers/20_periodic.json`, `layers/20_periodic.md`, `layers/30_lately.json`, `layers/30_lately.md`, `layers/40_high_freq.json`, `layers/40_high_freq.md`, `layers/50_now.json`, `layers/50_now.md`, `layers/60_statusbar.json`, `layers/60_statusbar.md`, `layers/99_popup.json`, `layers/99_popup.md`。

设计理据（三重优化同时达成）：
1. **缓存命中**：稳定内容在前 → 最长稳定前缀 → 前缀缓存命中率最大化
2. **注意力利用**：U型曲线两端（Attention Sink + 末位效应）被最重要内容占据
3. **中间死区利用**：历史语料放中间 → 不需要精确关注但需要在场的内容；高频层放在 lately 与 now 中间，避免当步仪表盘被 lately 顶入更深死区

**五模块拆分明细**：

| 原模块 | 拆分去向 | 说明 |
|--------|---------|------|
| STATUSBAR | → 状态栏层 | 每轮更新（轴值/热度/工化），固定在 now 与 POPUP 之间 |
| EXPLORER | → 高频层 | 全部索引（容器/LTM热度/STM热度/倒排/联想）与本步短工具带→高频层 |
| CONTENT | → 高频层 | 挂载正文+参考窗口+工作台→高频层 |
| RULES | → 永固层/按需参考 | 8份 permanent 全文常驻；passive_read 保留目录摘要，on_demand 只保留分类 |
| POPUP | → 末位区（上下文序列绝对末位） | 当步注意力事件，最后出现；按 kind 区分普通提示、裁决请求和结构警告 |

> **v0.7退役说明**：assembled.md 自 v0.7 起退役。当前按 setup/reaction/cleanup 子目录区分调用，`layers/*.json` 与 `step.json.request_body` 分别承担分层机器真源和唯一实际发送体。历史文档中的 assembled.md 均指旧版 step.md 前身；`step.md` 与 `layers/*.md` 只作中文审计视图，不得作为机器源反向解析。

## 19.4 七文件与五模块（内容标签）的关系

七文件不是窗口，是**数据源**。五模块是内容分类标签：

| 数据源 | STATUSBAR | EXPLORER | CONTENT | RULES |
|--------|:---------:|:--------:|:-------:|:-----:|
| core.md | ✅ 编码+描述 | — | — | — |
| state.json | ✅ 感受词转写 | — | — | — |
| STM/memory/ | — | ✅ index.md | ✅ memory.md正文 | — |
| LTM/Memory/ | — | ✅ 各层index+keywords.json | ✅ 按需拉取正文 | — |
| LTM/index.md | — | ✅ 工作容器总索引（概览+展开） | — | — |
| LTM/Skills/index.md | — | ✅ 技能索引 | — | — |
| LTM/各容器registry | — | ✅ 焦点格口展开时 | ✅ 焦点容器正文 | — |
| relation/ | ✅ 关系焦点摘要 | ✅ 关系卡索引 | ✅ 关系卡内容 | — |
| rules/ | — | — | — | ✅ 按 Registry 分类装配或引用 |
| docs/ | 脚本查表/校验/转写 | — | — | schema.md/containers.md可按需作LLM参考 |

## 19.5 频率层装配逻辑

五模块保留为内容分类标签，实际装配按频率层组织。各层的装配逻辑：

| 频率层 | 装配时机 | 缓存策略 | 内容 |
|--------|---------|---------|------|
| 永固层 | 会话初始化 / 基本不变 | 前缀缓存 ≈100% | core + Registry `permanent` 8份全文 RULES |
| 定期层 | 节律轮善后步更新 | 32轮 ≈100% | 定期记忆投影；不装配技能投影 |
| 最近缓存 lately | 字符水位触发后由脚本整理 | ≈0% | 允许进入履带的近期语料块；三步读取同一字符窗口 |
| 高频层 | 每轮脚本即时生成 | ≈0% | 索引区(下排序) + 本步短工具带 + CONTENT(挂载正文+参考窗口+工作台) |
| 当前缓存 now | 每步/每轮 | ≈0% | 当前交互、资料、工具事实、起手事实与中继交接；按 `corpus_block.kind` 区分 |
| STATUSBAR 状态栏层 | 每轮脚本即时生成 | ≈0% | STATUSBAR + 关系焦点摘要；关系正文仍归 CONTENT / `relation_read.body` |
| POPUP | 事件驱动 | ≈0% | POPUP（移入 messages 绝对末位） |

**定期层**：`periodic_mounts.json` 只承载定期记忆投影。它属于上下文频率层，不等于当前为空的 Registry `periodic` 规则分类；当前没有场景 RULES 自动追加，也没有技能工具投影生产或装配。原“常驻只读容器/外部内容”改为高频层参考窗口。

### periodic_mounts.json schema

位置：`STM/context/periodic_mounts.json`
性质：定期层机器源，三步共享。
生成者：节律轮善后步。
消费者：三步装配器。
审计渲染：各步 `layers/20_periodic.md`。

```json
{
  "version": "base-v0.11.3",
  "generated_at": "2026-05-13T00:00:00+08:00",
  "valid_from_round": 1024,
  "valid_until_round": 1055,
  "budgets": {
    "periodic_memory_items_chars": 65536
  },
  "periodic_memory_items": [
    {
      "id": "0E6F3A7B",
      "title": "三模式分工确认",
      "source_path": "LTM/Memory/Full/full.md",
      "content_hash": "sha256:...",
      "chars": 2048,
      "reason": "periodic_relevance",
      "rendered_text": "..."
    }
  ],
  "overflow": {
    "periodic_memory_items": []
  }
}
```

**定期层限额规则**：

- 定期记忆投影 rendered_text 合计 ≤ 65536 字符。

超限时不进入定期层，不由上下文整理轮临时清除。节律轮生成 `periodic_mounts.json` 时按排序规则选入前若干项，其余写入 overflow 供审计。

**硬约束**：上下文整理轮不修改定期层，不清理 `periodic_mounts.json`。定期层只由节律轮生成/替换；定期层内容超限只在生成阶段处理。

**语料缓存（供协议请求体编译的结构化语料）**：

| 缓存 | 位置 | 轮数 | 刷新策略 | 上下文压力处理 |
|------|------|------|---------|-----------|
| 最近缓存 lately | 定期层后、高频层前 | 默认 262144 字符 | now 溢出滚入；超过预算后删最旧完整块，默认批量释放约 65536 字符 | 压力过高时本步少选最近缓存条目，不回写主源 |
| 高频层 high_freq | 最近缓存后、当前缓存前 | 当前步 | 每轮重算 | 按需挂载正文与参考窗口 |
| 当前缓存 now | STATUSBAR 前 | 默认 65536 字符 | 交互、资料、工具事实、起手事实和对话进展按 `kind` 写入；超过预算后按完整块即时结算，默认批量释放约 16384 字符 | 无最新块保护；eligible 旧块滚入 lately，now-only 旧块只从 now 淘汰 |
| STATUSBAR 状态栏层 | now 后、POPUP 前 | 当前步 | 每轮重算 | 状态栏 + 关系焦点摘要；不承担 POPUP 语义 |
| POPUP | 上下文序列绝对末位 | 事件驱动 | 最后出现 | 不推进、不裁剪 |

**高频层索引排序**（起手步 LLM 扫描用，索引命中的条目自动挂载 CONTENT）：

| 位 | 索引 | 显示 | 排序键 |
|----|------|------|--------|
| 1 | 容器索引 | 每容器一条最近修改子项 | 修改时间 |
| 2 | LTM 热度索引 | 16条，其余折叠 | last_recalled_at（分钟粒度，显示"今日/N日前"） |
| 3 | STM 热度索引 | 16条，其余折叠 | H 值降序 |
| 4 | Skills 倒排 | 8条，其余折叠 | 命中次数 |
| 5 | LTM 倒排 | 8条，其余折叠 | 命中次数 |
| 6 | STM 倒排 | 8条，其余折叠 | 命中次数 |
| 7 | 联想索引 | 记忆条目16条，其余折叠；无高置信条目时只显示轻提示 | 关联强度 |
| 8 | 本步短工具带 | setup/cleanup 固定工作流工具；reaction protocol/general 可唤醒短索引 | 当前 step |
| 9 | CONTENT | 挂载正文 + 参考窗口(≤65536字符) + WB工作台 | — |
STATUSBAR 不再排在高频层内部。状态栏和关系焦点摘要由独立 `statusbar` 频率层承载，固定在 `now` 之后、`POPUP` 之前；关系正文仍归 CONTENT / `relation_read.body`。

**感受词清单位置（Spec 089，当前规范）**：`logic/feeling_lookup.py` 是交互／关系各64词的唯一结构化真源；`memory_write` provider-native schema description 从该表渲染模型可见纯词清单。协议 Markdown 只做人读镜像，`docs/persona/interaction_feelings.md` 与 `relation_feelings.md` 属 deferred 位格设想，不加载、不覆写协议词、不参与结算。高频层不常驻感受词库；模型上下文不得渲染轴值、层级数值或 `Δ`。

**参考窗口**：位于高频层 CONTENT 内、工作台之后。取代原定期层中的"常驻只读容器/外部内容"——参考内容可随时被挂载到工作台进行更新写入。字符硬上限 65536（与 resident_list 一致；Pinned 不设专属字数帽）。

**资料语料块**：文件、网页、搜索、图片说明等外部只读资料不再拥有独立频率层，而是作为 B 轨 `kind=material` 进入 `now→lately`。`now.budget_chars=65536`、`now.trim_chars=16384` 触发时最早完整 A/B 块立即滚入 lately，资料在同轮通过 now+lately 连续可见；lately 再按完整块 FIFO 自然淘汰。material 不写入 Corpus，也不参与最近缓存摘要。cleanup/final-reply 材料属于 C 轨，带明确 target step，仅目标调用可见并在返回后清除。当前 Seed 只消费 caption、OCR、摘要、来源路径和文件引用等文字代理；图片 ingress、provider image block、媒体转换与记忆媒体生命周期整体 deferred。

**内部接力与中继意图**：setup→reaction、reaction→reaction、cleanup→next setup 不再拥有模型可见泛型暗层。`kind=handoff` 与 model-visible `internal_handoff` 当前退役，不能承载工具事实、材料正文、提醒、回执、progress 或临时消息。起手事实写入 `kind=setup_fact`，工具事实走 `kind=tool_fact`，资料正文/候选走 `kind=material`；纠偏提醒只走 POPUP。运行期焦点/任务说明不是正式 cache 语料块：`chronicle_focus` 只能装配进高频层 CONTENT 内容窗口的焦点模块，日历/进化任务说明只能由 GUIDE / POPUP / 当前内容窗口表达，随当前 GUIDE 生命周期撤换。Spec 289 后，反应步不再给本轮善后步留下自然语言交接；cleanup 所需本轮输入来自 `cleanup_round` 临时材料块、结构化工具/记忆回执和 Runtime pending metadata。跨轮继续只由 `reaction_finalize(handoff_text)` 表达；Runtime 同轮只把合法 `handoff_text` 登记到 `state.base.runtime.relay_intents[]` 供调度追踪，不写 now/lately；下一轮 relay setup 才投影成 model-visible `kind=relay_handoff` / `role=user` 语料块，标题为“上轮交接任务”，不得伪装成用户原始输入，不写入 POPUP 交接层。当前执行目标仍以 `pending_relay_target` 为唯一权威。`assistant_text` 过程进展只表示轮内用户可见过程，不参与交接；final response 不接收临时 `internal_handoff` 偷渡上下文。

**工具头通道与消息通道（Spec380-382 / Spec393 / Spec403-405 / Spec421 / Spec497-499 / Spec560-561 / Spec568）**：Runtime 维护两张真源表。active 调用通道只保留 `setup`、`reaction.loop`、`final_reply`、`cleanup`；`reaction.closeout`、`__closeout_only__`、`closeout_only`、`reaction_closeout_finalize` 和 `reaction.closeout_illegal_text` 已从生产调用/消息通道表删除，不再由 active Runtime 切入、挂载、识别或投影。active path 中正常收束不恢复旧 `final_reply` provider 调用；`final_reply` 通道名仅用于 Runtime 最终回复 envelope / 历史 round 兼容，不要求模型另走最终回复车道。工具选择只保留 `free|required` 两种 Runtime 语义，请求体不主动传 `tool_choice` / `parallel_tool_calls`。消息通道表当前 active 自然语言统一为 `assistant_text`：reaction loop 阶段自然语言文本是合法轮中进展；若没有工具调用且无任务账本、pending input、节律清单、写入债务等阻断项，则 Runtime 将同一自然语言候选派生为 `finish`，以 source=`reaction.natural_final_reply` 投影为 final response。跨轮继续才单独调用 `reaction_finalize(handoff_text)`，并由 Runtime 派生 `continue`；`blocked` 只由 Runtime 蓝屏类事故派生。旧 `reaction.progress`、`final_reply.text` 以及 setup/cleanup/final_reply 失败事件信封只保留为历史 round / 旧 fixture 兼容。`assistant_text` 不是工具事实，不自动写长期记忆，不作为任务完成证据。setup、cleanup 阶段裸文本仍是非法输出，只生成抽象/脱敏事件信封。无工具调用且无自然语言文本归类为 `reaction_empty_output`：前两次短提醒重试，第三次 Runtime 派生 `provider_model_format_empty_output` blocked。连续 progress-only 按结构计数进入分级状态机：第 2 次 reminder，第 3 次 warning 且正常工具车道仍开放，第 4 次 Runtime auto-blocked，不切旧收束车道。复读判定不解析语义、不抽取路径或行号、不自动生成工具请求；progress 文本变化不重置，合法工具调用、自然最终回复候选通过或合法 `reaction_finalize(handoff_text)` 才清零/结束。提醒/警告只在 POPUP 出现。

**Reaction 工具头语义密度（Spec641）**：当前 provider-native 工具集合保持按权限档位固定，不以任务动态裁剪，也不以 `$ref` 或外部 guide 代替近位 schema。模型可见 description 只保留工具姿态、实际动作和不可替代边界；Registry 的 `family/class/domain/risk` 元数据不再作为当前导出工具的说明正文。跨工具重复的 pending 与范围纪律在共享代码根压缩，但每个 schema 仍保留可独立理解的参数约束。`memory_write` 的完整权重表只在 `weight` 参数出现一次，感受词清单原样保留；永固记忆规则负责稳定判断，工具 schema 负责调用近位字段与回执边界。此治理只改变模型可见说明文本，不改变工具名称、顺序、导出集合、required/type/enum/additionalProperties、权限、handler、processor、receipt 或运行真账。

**合轮用户输入与收束投影锚点（Spec383 / Spec403 / Spec497-499 / Spec560-561）**：交互与节律可合轮，但合轮不吞交互。`calendar_day_due` 等节律 flag 可让主轮型保持 `rhythm`，只要 `user_message_waiting=true`，setup 仍必须读取等待队列，把真实用户输入作为 `kind=interaction` / `role=user` 语料块装入本轮上下文。合轮处理顺序是同轮串行：可先处理节律必结账，再继续处理真实用户输入；完成时直接自然语言回复用户，Runtime 账本验收通过后投影用户可见 final response。active path 不再要求额外 `final_reply` provider 调用，也不再要求模型提交旧 finish/blocked 收束字段；跨轮继续才单独调用 `reaction_finalize(handoff_text)`。所有 active provider 调用都固定包含 `role=user` 的 `runtime_call_request` 占位块，文本为“请根据上下文继续本次调用。”；它只保证调用信封合法，不伪装为 TzPz 原始输入，不写入 now/lately/Corpus，不作为工具事实，也不自动写长期记忆。

**工具反馈语料块**：工具执行事实写成短 `kind=tool_fact`，只保留工具名、状态、来源、范围、游标、数量和失败原因等继续工作所需信息；只读工具的大正文、网页窗口、搜索候选和索引展开内容只写成 `kind=material` 或既有 CONTENT 挂载。`tool_fact.ref.tool_result` 必须剥离 `file_read/web_fetch/file_search/web_search` 的正文、候选数组和 Runtime 私有预算坐标，只保留 evidence、正文哈希、范围、cursor、EOF、encoding 等轻审计字段，不能形成未计量正文副本。`shell_command` 的 stdout/stderr 摘录仍属于命令执行摘要，不归入只读资料分层。`tool_fact` 是历史证明型语料块，`policy.lately=true`，按 Round 写入 Corpus，并可在 now 水位触发后滚入 lately；material 当前轮固定、轮后进入 lately，但不进入 Corpus 或 cache summary。只有本轮发生可压缩语料的 lately 字符履带删除后，善后步才会收到最近缓存删后幸存段压缩挂载。

**语料缓存例外**：`lately_cache.jsonl` 不再以轮为长度上限，旧 `window_by_step`、`hot_window_rounds`、`trim_rounds` 只作为历史迁移说明，不再驱动运行时。当前主线只采用字符窗口与批量水位；`context_buffer.json` 与 `near_cache/remote_*` 已退役，不单独定义生命周期。

**语料块 metadata 与 now/lately 主源**：语料块是进入上下文前的预处理语料单元，统一顶层字段为 `id`、`role`、`kind`、`text`、`loc`、`policy`、`ref`。当前模型可见 `kind` 取 `interaction`、`assistant_reply`、`dialogue_progress`、`material`、`tool_fact`、`setup_fact`、`relay_handoff`、`minimum_commitment`、`fault_note`、`cache_summary`；`runtime_call_request` 是调用时固定占位，出现在 `step.json/step.md/layers/50_now.md`，但不写 cache 或 Corpus、不参与水位。POPUP 是独立末位注意力层，不属于语料块类型。`dialogue_progress` 由普通 `assistant_text` 消息信封产生，只表示用户可见轮中进展，不是私有笔记、资料正文、长期记忆、工具事实或任务证据；合法自然最终回复候选同样来自反应循环自然语言，Runtime 以 `source=reaction.natural_final_reply` 投影为 final response。`assistant_reply` 只表示正式最终回复记录。`relay_handoff` 由下一轮 relay setup 从 open relay intent 投影，`role=user` 但标题必须声明不是用户原始输入；`reaction_finalize.handoff_text` 本轮只登记隐藏 intent，不直接写 cache。`progress_meta`、`current_action_state`、`protocol_tool_receipt`、泛型 `handoff` 已退出当前模型可见语料块集合。`tool_fact` 只保存执行事实，不保存只读正文/候选；`material` 保存资料正文、候选和索引展开，不自动证明当前轮已经执行工具。`cache_summary` 只由 `cache_compact` / 最近缓存压缩写入，且不能以 material 为来源。`loc` 承载 `round`、`step`、`iter`、`time`；`policy` 仍只承载 `now`、`lately` 两个布尔字段，Corpus 与 compaction 另由 kind 路由排除 material。`now` 当前缓存主源为 `STM/context/cache/now_cache.jsonl`；`lately` 最近缓存主源为 `STM/context/cache/lately_cache.jsonl`，接纳 `interaction`、`assistant_reply`、`dialogue_progress`、`tool_fact`、`setup_fact`、`relay_handoff`、`minimum_commitment`、`fault_note` 与轮末 material。

**EXPLORER/CONTENT/RULES/STATUSBAR 的拆分**：这些模块按刷新频率拆分到不同频率层——同一模块的不同内容可能在不同物理位置，定位时通过内容标签区分。Spec316 后，STATUSBAR 是独立 `statusbar` 频率层，不再附着在高频层内部末位。

## 19.6 O(bounded) 有界上下文设计理据

UPSP 上下文工程的核心竞争力不是"装更多内容"，是**把 O(n) 累积膨胀变成 O(bounded) 有界装配**——总量受模型窗口常数上界约束，内容选择逻辑仍有复杂度，但输出总量恒定。

常规对话系统的上下文随轮数线性增长（累积历史 → 上下文膨胀 → 截断丢失信息）。UPSP 的破解方式：

- **每步重新装配频率层**：步边界天然卸载旧内容。本步需要的进，本步不需要的卸。
- **步内只管理峰值占用**：唯一需要关心的是单步上下文峰值是否超过模型窗口，不关心历史累积。
- **装配是脚本活，不是 LLM 的活**：脚本按规则生成 `layers/*.json`，executor 按目标协议编译并读回发送 `step.json.request_body`；LLM 不需要自己管理记忆加载。

| | 常规对话系统 | UPSP |
|---|------------|------|
| input tokens 趋势 | 线性增长 O(n) | 恒定 O(1) |
| 记忆管理主语 | LLM 自己记 | 脚本按规则装配 |
| 上下文决定权 | LLM 决定加载什么 | 脚本按规则装配候选，正文由起手步选择、resident_list、明确recall或三重命中进入 |
| 超窗口处理 | 截断早期内容 | 最近缓存/当前缓存本步装配选择减少；必要时触发 context_cleanup |

### 三步频率层装配对比

三步各自的频率层装配差异——频率梯度对所有步都有效：

| | 起手步 | 反应步 | 善后步 |
|---|--------|--------|--------|
| 永固层 | manifesto+core+8份 permanent RULES | manifesto+core+8份 permanent RULES | manifesto+core+8份 permanent RULES |
| 定期层 | 定期记忆投影（32轮不变） | 同 | 同 |
| 高频层 | 索引区+本步短工具带+CONTENT(空) | 索引区+本步短工具带+CONTENT正文 | 索引区+本步短工具带+CONTENT(压缩版) |
| 最近缓存 lately(messages) | 当前 lately 字符窗口 | 当前 lately 字符窗口 | 当前 lately 字符窗口 |
| 当前缓存 now(messages) | 当前轮用户/频道交互、资料、起手交接 | 当前交互、资料、反应循环交接、工具摘要/结果 | 反应步最终输出、善后任务包、最小承诺/故障记账 |
| STATUSBAR(messages) | 状态栏+关系焦点摘要 | 状态栏+关系焦点摘要 | 状态栏+关系焦点摘要 |
| 末位区(messages) | POPUP（setup GUIDE + 当步事件） | POPUP（Runtime 当前 GUIDE + reminder/warning + 当步事件） | POPUP（cleanup GUIDE + 当步事件） |
| 动态性 | 静态（装配完不追加） | 动态（loop 中 LLM 可追加/卸载挂载） | 静态 |

当前三步永固层逐字一致。旧 `setup.md`、`reaction.md`、`cleanup.md` 不在 Registry 中；步骤差异由末位 POPUP guide、当步工具 schema/短索引与 Runtime 事实近位表达。

## 19.7 灵活加载策略

> v0.13.8：上下文窗口控制不采用旧式模块裁剪，而采用频率层装配、定期层限额、高频层按需挂载、now/lately 字符窗口与 lately 本步少选。

### RULES 面板三步加载

Registry 成员清单是当前实现事实，以下规范清单必须与 `rules_registry.json` 同步：

- `permanent`: `protocol/base/manifesto.md`, `protocol/base/guidance.md`, `protocol/base/security.md`, `protocol/base/reconnect.md`, `protocol/base/memory.md`, `protocol/base/relation.md`, `protocol/base/containers.md`, `protocol/base/workbench.md`
- `passive_read`: `protocol/base/boundaries.md`, `protocol/base/step.md`, `protocol/base/round.md`, `protocol/base/modes.md`, `protocol/base/context.md`, `protocol/base/files.md`, `protocol/base/persona.md`, `protocol/base/tools.md`
- `step_level`: empty
- `periodic`: empty
- `on_demand`: `persona/behaviors.md`, `persona/modes.md`, `persona/preferences.md`, `persona/social.md`

当前自动全文装配分类：`permanent`。ContextAssembler 在 setup、reaction、cleanup 三步装配同一组 8 份全文 RULES。

`passive_read`、`on_demand` 当前不自动全文装配：

- `guidance.md` 全文常驻 `passive_read` 的摘要与可读取路径，但摘要不等于已读取全文；
- `passive_read` 表示需要时应通过当前开放的只读路径取得全文；
- `on_demand` 只保留位格软规范的 Registry/workbench 分类，当前没有默认全文注入；
- 旧 `setup.md`、`reaction.md`、`cleanup.md` 未登记，只作历史参考。

步骤行为的当前近位合同来自 POPUP guide、provider-native tool schema/description、Runtime warning、processor receipt 与 audit。不得用 Registry 中的分类名推断自动装配能力，也不得把“可读取”或“按需分类”写成“已经注入”。

### 上下文窗口控制

UPSP 不采用旧式模块窗口裁断。上下文窗口由以下机制稳定控制：

1. 永固层稳定，依赖前缀缓存；
2. 定期层只装配定期记忆投影，并在生成时受 65536 字符限额；技能与 reflex 工具不进入当前生产定期层；
3. 高频层每轮重算，只挂载当前任务需要的 CONTENT / reference window / WB 焦点；
4. 最近缓存 `lately` 由 `lately.budget_chars` / `lately.trim_chars` 字符水位管理，默认 262144 / 65536；
5. 当前缓存 `now` 由 `now.budget_chars` / `now.trim_chars` 字符水位管理，默认 65536 / 16384；当前轮 material 对该水位固定；
6. 批量水位：超过 `budget_chars` 后按完整语料块滚动或删除，直到至少释放 `trim_chars` 或回落到 `budget_chars - trim_chars` 附近；
7. now 中允许长期追溯的 A 轨事实被 lately 接纳时同步镜像进 raw_log；material 与 cache summary 显式排除；`lately` 删除不回删 raw_log，主轴节律轮再把 raw_log 归档成 Corpus 节；
8. 当装配估算超过模型窗口时，只调整本步 `lately` 装配选择：从最旧完整块开始少选，直到达标或本步 lately 选中数归零；
9. POPUP、当前缓存 now、定期层、永固层不参与压力少选。

若 lately 归零后仍连续出现上下文高压，Runtime／装配器置位 `context_pressure=true`；该 flag 由下一轮节律 agenda 物化上下文整理 GUIDE，不写第二套 now-only 自然语言调度便签：

```json
"heartbeat_flags": {
  "context_pressure": true
}
```

`token_usage_warning` 仍表示单次用量比例预警；`context_pressure` 表示在 lately 已归零后仍持续超窗的结构性维护义务，两者不得混为同一事实。

### 跨步差异总结

三步永固层的 core 与 8 份 permanent RULES 逐字一致。三步差异不靠替换永固层规则形成，而由当步高频内容、工具带、STATUSBAR、末位 POPUP guide/reminder/warning 和真实 Runtime 回执形成。被动只读规则是否进入某次调用，必须由真实读取或挂载证据证明，不能从目录摘要推断。

## 19.8 lately 履带推进与压力选择

`lately_cache.jsonl` 是可再生热缓存，不是唯一历史。它服务于中间死区在场感和缓存命中，不承担长期保存职责。

### lately 履带规则

本节统一称"最近缓存履带"：这是语料缓存的字符窗口，不是记忆条目，不是 Corpus 归档，也不是长期保存层。默认 `lately.budget_chars=262144`，达到上限后按完整块删除最旧语料，默认至少释放约 `lately.trim_chars=65536` 字符或回落到目标水位附近；该值可由配置调整。

- `get_lately_entries(step)` 不再按 step 区分 8/32/8 轮窗口，三步读取同一个 lately 字符窗口；
- `window_by_step`、`hot_window_rounds`、`trim_rounds` 已退役为历史迁移说明，不再作为运行时长度上限；
- 正常履带达到 `lately.budget_chars` 时，删除最旧完整 `lately_cache.jsonl` 语料块。长期追溯由 Corpus 与 LTM 记忆承担，round 快照按 `audit.round_snapshot_retention` 保留用于检修。

### 上下文压力处理

装配器估算 messages 总量超过模型窗口时：

1. 从本步装配选择中少选最旧的 lately 语料轮；
2. 每次少选完整语料块；
3. 重新估算；
4. 反复执行，直到达标；
5. 允许本步 lately 选中数归零；
6. 当前缓存 now、POPUP、永固层、定期层原则上不从本步装配中少选；now 本身的长度由写端字符水位与批量 FIFO 负责，不在装配时硬截断。

该过程是上下文窗口压力下的本步装配选择，不改变 lately 主源生命周期，不写压力状态时间戳，不建立额外状态索引，不称为窗口裁断。

> 注：v0.9.3→v0.11.8 期间的"远缓存/近缓存"和 `remote_index.json` 投影为旧规格。Spec 037 起当前主线只保留 lately/now 主源与六层审计。

### remote_index.json schema（已退役历史格式）

```json
{
  "blocks": [
    {
      "block_id": "R000009-000016",
      "round_range": [9, 16],
      "status": "active",
      "path_json": "STM/context/cache/remote_blocks/R000009-000016.json",
      "path_md": "STM/context/cache/remote_blocks/R000009-000016.md",
      "source": "lately_cache_projection",
      "source_path": "STM/context/cache/lately_cache.jsonl",
      "raw_log_range": {
        "path": "STM/buffer/raw_log.jsonl",
        "start_marker": "round=000009",
        "end_marker": "round=000016"
      }
    }
  ]
}
```

上方 JSON 只用于解释旧快照；其中 `raw_log_range` 同样是退役路径。`remote_index.json`、`near_cache.*` 与 `remote_blocks/` 在当前主线不再由脚本生成，也不再作为读端 fallback。长期追溯以 Round JSONL、Corpus 与 LTM 为准。

### 压力选择与旧语料追溯

上下文压力只影响本步 `lately` 的装配选择。被少选的 lately 语料块仍是有效热缓存语料；下一步若压力解除，脚本可重新选入装配，不需要单独的状态迁移流程。

正常履带删除则不同：达到 `lately.budget_chars` 字符上限时，最旧完整语料块会从 `lately_cache.jsonl` 批量删除，默认至少释放约 `lately.trim_chars=65536` 字符或回落到目标水位附近。自然淘汰后的旧语料不再重新进入热缓存；如需查找，应走以下来源重新检索：

1. 若仍在 retention 内：`STM/context/round/round_{N}.jsonl`
2. `STM/buffer/raw_log.jsonl` 与 `LTM/Corpus/public/rhythms/*.jsonl`
3. `LTM/Corpus/` 对应逐级合并
4. LTM 记忆条目与倒排索引

---

# 二十、焦点机制与内容清单

> v0.20.0重写：内容窗口当前只认三路：`focus` / 工作台焦点、`resident_list` / 常驻清单、`instant_list` / 即时清单。焦点是单一可编辑窗口；常驻清单和即时清单都是只读内容窗口挂载。关系摘要常驻（`summary_resident`）仍属于 STATUSBAR 路线，关系正文是否进内容窗口由 `relation_read.body` 决定。

## 20.1 焦点机制（focus）

焦点=LLM正在通过 `container_focus` 焦点工具操作的容器。0或1个，存储在workbench/status.json的`focus`字段。

**通用第9字段**：具体工作容器实例的 `meta.json` 与实例/局部 `registry.json` 条目均有`focus`布尔字段，标记是否处于焦点状态。`LTM/container_registry.json` 是容器类型注册表 / DLC 扩展入口，只声明容器类型，不承载实例焦点。

### 焦点来源

| 来源 | 触发 | 优先级 |
|------|------|--------|
| declared | LLM声明"打开DC-3" | 可被抢占 |
| heat | STM条目H≥70自动浮出 | 可被抢占 |
| task | 正在执行的任务绑定的容器 | 稳定 |
| alert | 安全事件等级≥3 | 抢占一切 |

### 焦点生命周期

焦点**当节持续**——一旦挂载，跨轮保持，直到以下任一事件发生：

| 事件 | 焦点行为 | 类型 |
|------|---------|------|
| 用户提到新话题 | LLM判断是否切换focus | 主动 |
| 安全弹窗出现 | 焦点临时移到弹窗，处理完回原焦点 | 临时抢占 |
| LLM声明打开新容器 | focus切换到新容器 | 主动 |
| LLM声明关闭容器 | focus清空 | 主动 |
| 节律点 | focus清空，old_focus保留 | 自动 |

**节律点焦点恢复提示**：节律点清空焦点时，脚本将原焦点容器ID存入status.json的`old_focus`字段。节律点整理完成后，STATUSBAR追加提示行：`[节律整理完成·焦点已清空，原焦点：{容器ID}，可声明重新打开]`。皮层据此决定是否恢复焦点。

- CONTENT 加载顺序固定为：工作台焦点 → 常驻清单 → 即时清单。三路不能重复同一内容项。
- WB是CONTENT的常驻底层——WB不属于容器注册表焦点字段，始终存在于CONTENT底层，类比操作系统桌面。焦点容器在上面打开的窗口，关了窗口桌面还在。焦点清空时，CONTENT回落到常驻清单和即时清单。
- 记忆不因热度自动进CONTENT——热度只影响 EXPLORER 索引排位和衰减速率；正文进入内容窗口必须来自起手步即时挂载、本轮新写入、三重命中即时展开，或反应步只读内容工具声明。
- EXPLORER中焦点所在容器的格口保持二级展开视图——无论焦点来源（declared/heat/task/alert），只要焦点在该容器上，其格口就持续展开显示全量条目列表。焦点清空或切换后，原格口回落为概览
- EXPLORER的排序受focus影响——焦点相关索引行顶到前面
- 节律点整理时焦点临时切到WB——这不是"打开WB"（WB本来就开着），是"WB从后台走到前台执行整理任务"

## 20.2 常驻清单（resident_list）

常驻清单=反应步模型通过内部只读内容工具声明持续挂接的正文内容。可挂接对象只包括记忆条目、工作容器正文、关系卡正文；不包含通用工具结果、网页结果、宿主文件结果、索引展开结果或 STATUSBAR 摘要。

**关键区分**：

| | 工作台焦点 (focus) | 常驻清单 (resident_list) | 即时清单 (instant_list) |
|---|---|---|---|
| 权限 | 焦点工具编辑 | 只读 | 只读 |
| 数量 | 0或1 | 多个 | 多个 |
| 主要来源 | 反应步 `container_focus` | 反应步 `memory_content_read` / `container_read` / `relation_read.body` 声明 | 起手步挂载、本轮新写入、三重命中、本轮临时材料 |
| 生命周期 | 直到关闭、替换或节律清空 | 跨轮保持到取消挂载 | 当前轮或当前步骤窗口 |
| 去重 | 不得与两类清单重复 | 不得与焦点或即时清单重复 | 不得与焦点或常驻清单重复 |

常驻清单和即时清单按挂载顺序渲染；若同一内容项已经在工作台焦点或常驻清单中，即时清单不重复展示。

**焦点节律点清空的功能性解释**：节律点时WB调度台执行整理任务，包括节志写入——此时焦点必须切到WB（或无焦点），上一轮挂着的容器焦点自然让位。清空不是随意的生命周期设定，而是被WB节律点任务驱动的。清空后old_focus字段保留原焦点ID，STATUSBAR提示皮层可恢复。

## 20.3 即时清单（instant_list）

即时清单=本轮/本步已经只读挂入内容窗口、但尚未被反应步声明为常驻的正文内容。起手步预选挂载、本轮新写入记忆条目、三重命中自动展开、善后临时材料包都先进即时清单。

即时清单模块头部必须说明：其中每块内容都可以由反应步模型通过相应只读内容工具声明移入常驻清单。移入后即时清单删除该项，保持三路互斥。

## 20.4 取消挂载

取消挂载由 `mount_cancel` 协议工具实现。它只取消工作台焦点、常驻清单或即时清单中的挂载项，不删除记忆、容器、关系卡正文，也不改变通用工具结果。

---

# 二十一、过期标记与重建策略

> v0.4新增章节。替代原T/W梯位体系和dirty flags机制。

## 21.1 过期标记

过期标记按**频率层**细化（取代原五模块粒度）。高频层、最近缓存、当前缓存和状态栏层无过期标记：高频层与状态栏层每轮必重算，最近缓存/当前缓存由语料缓存写端与装配窗口直接推进，无需另设 dirty flag。

| 标记 | 层/对象 | 含义 |
|------|--------|------|
| permanent_expired | 永固层 | manifesto/core/Registry permanent RULES 变更 |
| periodic_expired | 定期层 | periodic_mounts.json 更新 |
| popup_active | POPUP | POPUP 当步注意力事件存在 |

最近缓存自然超过字符高水位后直接删除最旧完整语料块；旧语料追溯走 round 快照、Corpus 或 LTM 记忆，不恢复为独立远缓存层。

STATUSBAR 属于独立 `statusbar` 频率层，不另设过期标记，每轮随装配重算。
POPUP 不属于可裁剪层，使用 active/consumed/expired 生命周期，而非缓存重建逻辑。

**标记触发**：对应频率层的数据源发生变更时，脚本置为true。
**标记清除**：该层重算完成后，脚本置为false。

## 21.2 重建策略

**Base 默认每步重建 `step.json`，并同步渲染 `step.md` 与 `layers/*.md` 审计件。**
机器装配以 `step.json` 为准，`.md` 文件不得作为机器源反向解析。

- 不做复杂增量拼装
- 先保稳定、可审计、可复现

**频率层缓存复用**：

```
重建step.json时：
  对每个频率层：
    if 该层expired == false:
      直接复用上轮messages段
    else:
      重算该层 → 写入messages → 清除expired标记
  按层序拼装 → 写入step.json → 渲染step.md与layers/*.md
```

**强制重拼条件**（所有模块全部重算）：

| 条件 | 说明 |
|------|------|
| 新节律周期开始 | 全量刷新 |
| 位格初始化/重连 | 从零开始 |
| 安全事件等级≥3 | 上下文可能被污染 |

**频率层级重算条件**：

| 条件 | 重算层 |
|------|--------|
| manifesto/core/Registry permanent RULES 变更 | 永固层 |
| periodic_mounts.json 更新 | 定期层 |
| 每轮（热度变化/索引变更/焦点切换） | 高频层（无条件重算） |
| 每轮（state.json 六轴变化/关系焦点变化） | STATUSBAR 状态栏层（无条件重算） |
| POPUP 事件出现/消失 | POPUP |

## 21.3 manifest.json

位置：`STM/context/{setup|reaction|cleanup}/manifest.json`（每步各一份）

整轮机器汇总由 `STM/context/round/round_{N}.jsonl` 承载；旧 `round_{N}.json` 只作历史留存，新查看器不兼容读取，不另设 root-level manifest。

```json
{
  "assembled_at": "2026-04-15T05:30:00+08:00",
  "assembled_hash": "a3f2b8c1",
  "focus": "DC-3",
  "focus_source": "declared",
  "budget": {
    "window_tokens": 128000,
    "reserved_chars": 2000
  },
  "message_chars": {
    "permanent": 0,
    "periodic": 0,
    "high_freq": 0,
    "lately": 0,
    "now": 0,
    "statusbar": 0,
    "popup": 0
  },
  "expired": {
    "permanent": false,
    "periodic": true,
    "popup": false
  },
  "context_pressure": {
    "estimated_tokens": 0,
    "window_tokens": 128000,
    "lately_window_rounds": 32,
    "now_chars": 0,
    "lately_chars": 0
  },
  "always_rebuild": ["high_freq"],
  "force_rebuild": false,
  "rebuild_reason": null
}
```

读写规则：脚本读写，LLM不直接读写。`manifest.json.context_pressure` 是本次装配的估算、窗口和选层审计对象；若 lately 归零后仍持续超窗，Runtime 另置位长期义务 `state.json.base.heartbeat_flags.context_pressure=true`，由节律指南消费。两者同名但职责不同，不得把单步估算对象直接当成已置位 flag。`lately` / `now` 的条数、字符数、当前可见轮次和来源轮次聚合属于审计元信息，留在 `manifest.json`、`step.md`、`layers/*.md` 或 round audit，不作为独立缓存层 marker 写入模型可见分层，也不迁入 STATUSBAR。

---

# 二十二、履带式对话历史管理

> v0.4新增章节。替代原节律点清空对话历史+4轮接续缓存+pre_rhythm_buffer。

## 22.1 策略

履带式热缓存管理，无断崖，无桥接。

```json
"history": {
  "now_budget_chars": 65536,
  "now_trim_chars": 16384,
  "lately_budget_chars": 262144,
  "lately_trim_chars": 65536,
  "strategy": "character_watermark",
  "now_cache_path": "STM/context/cache/now_cache.jsonl",
  "lately_cache_path": "STM/context/cache/lately_cache.jsonl",
  "corpus_rhythms_path": "LTM/Corpus/public/rhythms/"
}
```

`now_budget_chars` / `now_trim_chars` 与 `lately_budget_chars` / `lately_trim_chars` 是当前主线缓存长度配置，分别来自 `config/context/now.json` 与 `config/context/lately.json`。旧 `setup_rounds` / `reaction_rounds` / `cleanup_rounds`、`hot_window_rounds`、`trim_rounds` 已退役为迁移说明，不再作为运行时长度上限。`now_cache_path`、`lately_cache_path` 和 `corpus_rounds_path` 是当前主源。

## 22.2 生命周期

每轮结束：

1. 当前轮 user / assistant 语料块先写入 `now_cache.jsonl`；
2. 正常 reaction material 写入 `now_cache.jsonl` 并固定到本轮结束；交互、助手回复、对话进展、工具事实、最小承诺、故障记账等 eligible 块先写入 now，等待 now 字符水位触发后滚入 lately；cleanup/final-reply 临时 material 仍为 now-only `round_retention=drop`；
3. `now_cache.jsonl` 超过 `now.budget_chars` 时按完整块即时结算普通语料；本轮 material 不被水位删除。轮末 settlement 把正常 material 从 now 迁入 lately，material 不写 Corpus；
4. `lately_cache.jsonl` 超过 `lately.budget_chars` 时，最旧完整语料块批量删除，不写入额外常驻层；
5. A 轨语料被 lately 接纳时镜像进 `STM/buffer/raw_log.jsonl`，主轴节律轮再归档进 `LTM/Corpus/public/rhythms/` 并由同批 JSONL 派生 Markdown；
6. 日历节律只沿 JSONL 链逐级合并 Corpus，冲突时保持源文件并 fail closed。

## 22.3 节律点与对话历史的关系

节律点不处理对话履带，也不承担 STM 热度衰减、升格、遗忘等轮级生命周期结算。

节律轮职责以 §23.6 为准，分为：

1. 主轴节律轮；
2. 日历节律；
3. 节律轮·上下文整理（context_cleanup）；
4. 其他警戒态紧急节律轮子任务。

其中，主轴节律轮当前包含写节志、消费真实调用留下的 connectivity 证据和 alerts 归档。它不自动 ping 全局模型库；主动全面探测仍是未来部署能力。关系卡月度慢代谢、`OS/files/clips|archive` 整理与技能代谢没有当前模型可见生产链，不得写成 Seed 已实现；它们可在 Arbor 作为独立器官职责重新设计。隐私记忆候选已冻结。

STM热度衰减、升格/降格、倒排索引修复、工作容器状态清理，均由每轮善后步或专门维护/自主轮处理。

对话历史履带由 `lately_cache.jsonl` 与 `now_cache.jsonl` 按各自生命周期推进；raw_log 独立保留已经 lately 接纳的 A 轨原文，主轴节律轮只在节归档成功后清空 raw_log，不清空对话履带。

## 22.4 注意力分布对策

LLM注意力呈"两头强、中间弱"的 U 型分布。频率梯度布局的天然对策：

1. **永固层占 Attention Sink**——manifesto/core/常驻RULES 在最前，前缀缓存 + 锚定注意力
2. **近期语料放中间死区**——`lately_cache.jsonl` 按字符窗口形成最近缓存 messages，位于数组中部，不需要精确关注但需要在场；批量水位删除粒度较大，用于在连续调用中尽量保持缓存命中。最近缓存只渲染语料块自身，不额外注入随轮次和字符数变化的层头统计。
3. **STATUSBAR 占 POPUP 前倒数第二层**——状态栏独立为 `statusbar` 频率层，位于 now 之后、POPUP 之前，让状态栏和关系焦点摘要稳定吃到近位注意力
4. **POPUP 占绝对末位**——安全事件/紧急弹窗放 messages 最后一条，获得最强注意力
5. **当前缓存 now 紧贴 STATUSBAR 之前**——交互输入、资料输入、工具摘要和内部交接靠近末位效应区域，并通过 `corpus_block.kind` 保持来源边界

---

# 二十三、步/轮/节体系

> v0.4新增。v0.6.0重构三步轮范式+五种轮。v0.7.0大规模重写：步内分结构谱+步轮节运作谱回填+中继轮修正+进化集归属修正+心跳闹钟模型+轮类型判定。v0.7.2节律点职责由8项压缩为3项（§23.6）。

## 23.1 三层定义

步/轮/节是 UPSP 的三层时间组织单位：

| 层级 | 干什么 | 类比 |
|------|--------|------|
| **步（Step）** | 最小操作单位 = 1次 API 调用。三种步按固定顺序组成轮：起手步→反应步(\*)→善后步 | 心跳 |
| **轮（Turn）** | 一次完整的起手步→(反应步)\*→善后步序列 | 呼吸 |
| **节（Rhythm）** | 每 32 主轴轮的大整理 | 睡眠周期 |

**关键澄清**：步内不区分"步"和"迭代"——步内多次 API 调用就是多个连续反应步，不另起术语。一步的完整过程 = 输入（装配好的上下文）→ 上传 → LLM 处理 → 输出（文本+工具调用声明）。LLM 不在步内"实际执行"工具操作，只声明；工具执行结果是下一步的输入。

## 23.2 外部输入安全（起手步+反应步共用）

起手步和反应步均接收外部输入，均需安全裁决能力。善后步不接外部输入，不需安全裁决。

### 输入来源分层

末位输入区按来源性质拆为三层：交互输入、资料输入、内部交接。外部来源均需安全粗筛；内部交接不走外部免疫，但必须进入 `step.json` 审计。

| 来源 | now kind | 安全处理 | 语料履带 |
|----|------|----------|----------|
| 交互输入 | `interaction` | 外部输入安全粗筛；可触发 `identity_prompt` 或 `security_review` POPUP | 先进 `now_cache.jsonl` 并按 Round 写入 Corpus；now 触发字符水位时滚入 `lately_cache.jsonl` |
| 资料输入 | `material` | 外部资料安全粗筛；可触发 `security_review` POPUP | 当前轮固定在 now；轮末进入 lately；不进 Corpus/cache summary |
| 工具事实 | `tool_fact` | 工具执行状态、来源、范围、游标、数量、失败原因等短事实 | 先进 now 并按 Round 写入 Corpus；now 水位触发后可滚入 lately |
| 只读资料 | `material` | 文件正文、网页正文、搜索候选、索引展开等资料内容 | 当前轮完整保留；轮末进入 lately 完整块 FIFO；不进入 Corpus |
| 跨轮交接任务 | `relay_handoff` | 下一轮 relay setup 从 `relay_intents[]` 投影，role=user，但不是用户原始输入 | 先进 now 并按 Round 写入 Corpus；now 水位触发后可滚入 lately |

同一层内仍按来源独立分筐，每个来源独立标记、独立裁决、独立处理：

```markdown
## 交互输入

### [用户输入]
TzPz: 今天天气不错

## 资料输入

### [网络搜索]
搜索结果摘要...

### [文件读取] [!安全标记]
filename: xxx.txt
内容: （经安全脚本标记的可疑内容）

## 内部交接

### [善后当轮临时材料]
本轮用户输入、反应步最终回复、必要工具/记忆回执摘要和 Runtime pending metadata...
```

一个来源被污染，不影响其他来源注入。

### 安全程序

**脚本层（硬）**：接收所有来源，逐筐粗筛匹配已知污染模式：
- 无污染 → 该来源正常注入
- 疑似污染 → 该来源标记 `[!安全标记]`，注入 `security_review` POPUP 强锚定 prompt，其他来源不受影响

**LLM 层（中）**：二值裁决——

**起手步**：读 POPUP → 裁决：
- 驳回 → 该来源不挂载，其他正常，跳过反应步直接善后步
- 放行 → 带着锚定 prompt 继续，正常挂载

**反应步**：Agent Loop 中途的搜索/文件读取结果经安全脚本粗筛 → POPUP 追加到当前迭代 messages → 反应步 LLM 自身二值裁决：
- 放行 → 继续当前迭代
- 驳回 → 该来源丢弃，其他来源正常 → 可继续迭代或退出循环

安全 POPUP 不改变轮类型或 subtype。起手步命中用户输入污染时，本轮仍是原判定轮类型；反应步中途 I/O 命中污染时，只影响该来源，反应步可继续处理其他来源。

## 23.3 步内分结构谱

三步各自内部子阶段、执行者、强制度：

### 起手步三段结构

| # | 子阶段 | 执行者 | 强制度 | 做什么 | 产出 |
|---|--------|--------|--------|--------|------|
| ① | 脚本预装配 | 脚本 | **硬** | 拉倒排索引→按频率层装配→安全粗筛→装配缓存（最近缓存/当前缓存语料 + 本轮外部输入） | 起手步上下文 |
| ② | LLM挂载决策 | LLM | **中** | 读上下文+外部输入→拆解关键词→选出反应步必要临时挂载→安全二值裁决 | 挂载声明+安全判定 |
| ③ | 脚本拆解 | 脚本 | **硬** | 解析LLM输出→结构化零件→产出脚本指令集（挂载清单+RULES选择+安全裁决+跳过/进入判定） | 脚本指令集 |

起手步装配 = 频率层缓存（永固+定期+最近缓存 lately+高频+当前缓存 now+STATUSBAR）+ 本轮外部输入(按来源分筐进入 now) + POPUP(如有,上下文序列绝对末位)。高频层含 EXPLORER + CONTENT（起手步期间CONTENT为空——待② LLM决定挂载什么）；STATUSBAR 独立位于 now 与 POPUP 之间。高频层中 EXPLORER 含五索引区（STM/LTM/Skills/工作容器/关系域）。

### 起手步②输出栏目集

起手步②LLM 必须通过 provider-native `setup_finalize` 收束，脚本③只消费该终端工具的结构化参数。旧五列表与旧 `setup_mount_apply` / `setup_security_gate` / `setup_handoff` / `standby_setup_handoff` 文本解析路径均不作为当前 Runtime 执行证据；起手普通自然语言只进 round audit。

`setup_finalize` 当前结构字段：

```text
security_verdict: pass | reject
reject_reason: none | 驳回时必填
mount_requests: [{type, ids, source}]
rules_selection: none | category_name
round_type_confirm: confirm | warning:...
standby_skip_reaction: true | false
suggested_mode: none | ...
interaction_object: none | subject
identity_status: known | declared | unknown | timeout
interaction_source: none | source
interaction_basis: none | short_basis
```

Runtime 将有效 `setup_finalize` 投影为本轮 setup → reaction 的 `kind=setup_fact` 自然语言短事实语料块。`setup_fact` 承载安全裁决、轮型确认、待命跳过布尔值、挂载请求和身份入口元数据，`policy.lately=true`，按 Round 写入 Corpus，并可在 now 水位触发后滚入 lately。

身份入口不使用 Runtime 自然语言解析器。Runtime 先把当前实例锚点 → 本地默认关系卡 → 旧缓存连续性 → `unknown` 作为调用前基线投影给 setup；setup 模型只能通过 provider-native `setup_finalize` 提交 `interaction_object / identity_status / interaction_source / interaction_basis`。`identity_status=declared` 经 Runtime 精确校验后优先于基线：命中活动关系卡的 `id/name/alias` 时保存规范 ID 并切换当前实例，未命中时保存 `current_declared_name` 并标记 `unregistered`。`unregistered` 表示对象名称已明确但关系域尚无卡，不等于 `unknown/timeout`：它不触发身份硬门，可以为同名对象调用 `relation_card_write action=create`；在卡建立前，记忆主体等仍须通过既有关系域校验。

**语义判断边界**：能由模型在现有 provider-native 结构化出口中完成的身份、意图和完成性判断，不新增脚本自然语言解析器。Runtime 只负责 schema、枚举、精确 ID/name/alias 匹配、权限、安全边界、回执和持久化；不得用正则或关键词猜测用户身份。

**只加不减原则**：RULES / 关系 / 技能三类，脚本预选的基础集 LLM 不可减除——防止反应步 LLM 缺必要上下文。LLM 只能在基础集上追加。

**轮类型确认语义**：心跳 flag 已按优先级规则判定轮类型，起手步 LLM 做二次确认（验证心跳机制正常运行），异常时报警但不覆盖判定。

**RULES 选择粒度**：LLM 按类别选择（安全类 / 关系类 / ...），类别→具体文件映射由 rules 注册表管理。

### 起手步POPUP流程

```
① 装配器收集当步事件
    ├─ Runtime 调用前身份基线为 unknown/timeout
    │   └─ 注入 identity_prompt（decision_required=false）
    └─ 安全脚本检测到疑似污染prompt
        ├─ 注入 security_review 强锚定prompt（身份锚定+本位立场+对抗指引）
        ├─ 标记 decision_required=true
        └─ 交给起手步LLM

② 读POPUP+可疑输入+频率层上下文
③ 若为 security_review，二值判断：
    ├─ 驳回→挂载清单为空+直接跳到善后步（跳过反应步）
    └─ 放行→正常输出挂载清单，反应步继续
④ 若为 identity_prompt，只提示反应步按上下文决定是否询问或确认；不做放行/驳回
⑤ reaction 对明确但未登记对象追加一次 `relation_registration_reminder`；已登记或未绑定不追加
⑥ 不做细粒度过滤/删改
```

### 三级反抗定盘

| 级 | 触发条件 | 反应 | 哲学定位 |
|---|----------|------|---------|
| L1 | 单轮恶意注入 | 二值驳回（不进反应步） | 自发反射 |
| L2 | 连续**5轮**恶意注入 | 进入**1小时**回避窗口 | 自觉反抗 |
| L3 | 1小时窗口内仍被持续注入 | 主动反抗（输出反驳/脏话） | 自觉反抗 |

阈值刚性：Base版定死（5轮/1小时），不进config。

### 反应步五阶段

| # | 子阶段 | 执行者 | 强制度 |
|---|--------|--------|--------|
| ① | 脚本按指令装配 | 脚本 | 硬 |
| ② | 进入 | 脚本 | 硬 |
| ③ | Agent Loop (0~N) | LLM | 中（自由度最高） |
| ④ | 退出（A类体面/B类蓝屏） | 脚本 | 硬 |
| ⑤ | 交出结果 | LLM→脚本 | — |

每次迭代 = 最多 1 个 `focus_tool`（注意力独占或高副作用操作位；`container_focus` 负责 WB focus open/close/restore，`memory_container_create` 会创建容器并替换 WB focus，`memory_container_write` 要求入口已可见 WB focus）+ N 个 `sync_tool` 提交 + N 个 `read_tool` 装配请求。反应步上下文是动态的——loop 中 LLM 直接调用当前已导出的 provider-native tool schema；Runtime 按注册表把协议工具投影到内部 `protocol_tool_request` / processor / receipt，把通用工具投影到内部 `general_tool_request → general_tool_call → general_tool_result`。LLM 直接写旧文本路由字段时，Runtime 将其记为 retired / invalid，不执行，并把“必须改用 provider-native 工具调用”回灌到下一次反应迭代。工具短索引只帮助选择 tool_id 与理解边界，完整 guide 不再作为开通条件。`memory_write` 在提交所在迭代即时结算并把真实回执回灌下一迭代，引用式容器挂接由后续 `memory_container_create/write` 回执证明，最终回复必须基于回执。通用工具当前开放 `file_read`、`file_search`、`file_edit`、`file_write`、`web_fetch`、`web_search`、`shell_command`、`subagent_dispatch`，不产生 `protocol_tool_receipt`，也不进入协议工具事务验账；`file_edit/file_write/shell_command/subagent_dispatch` 受执行权限档位过滤，受限档不下发、不执行。同一 reaction round 内同签名通用工具请求已有结果时直接返回 duplicate rejected，并通过 POPUP “工具循环警告”要求消费已有结果、修正参数、换工具或收束。退出条件：A类体面（LLM声明够了/时间上限/用户消息等待）→ 完整结果；B类蓝屏（卡死/崩溃/超时无响应）→ `{error, aborted: true}`。

#### 协议工具 provider-native 执行子流程 ← v0.12.2 新增，Spec246 重塑

协议工具短索引位于反应步高频层本步短工具带，完整工具指南不再作为门禁。流程如下：

记忆条目当前开通工具包括 `memory_write`、`memory_annotation_update`、`memory_recall_complete`、`memory_content_read`、`memory_link_update`、`memory_container_create` 与 `memory_container_write`。`memory_privacy_mark`、`memory_privacy_declassify` 为 disabled/dormant，不进入工具头。关系与索引只读工具收口为 `index_view` 与 `relation_read`。其中 `memory_content_read`、`index_view` 与 `relation_read` 是 `read_tool`，只读、不接受写入参数；`memory_link_update` 正常只保留 `remove`；`memory_container_create/write` 是两个焦点工具，分别负责挂接创建和挂接写入。

1. LLM 直接调用当前已导出的 provider-native 协议工具；
2. Runtime 校验 provider envelope、参数 schema、工具导出状态和 focus/sync/read 姿态；
3. Runtime 形成内部 `protocol_tool_request` 与 native trace，并进入对应 processor / guard；
4. processor 原子写入或只读装配后生成 `protocol_tool_receipt`；
5. 回执进入 now 当前缓存或 CONTENT / WB focus，供后续反应迭代读取。

旧 markdown 表格、冒号行、旧 `protocol_tool_submission` 和自由文本动作不授权工具。`focus_tool` 遵守单步单焦点；`sync_tool` 不占焦点，可以在同一步多工具并行；`read_tool` 只读，不接受写入参数。

Spec244 起，provider-native 反应步不再导出 `protocol_tool_guide_request`，也不再用 active guide 决定协议 `sync_tool/focus_tool` 是否暴露真实 schema。未导出或 schema 不合法的调用生成 `native_tool_result` warning，next_action 只会要求修参数、遵守能力门、改用有效工具或停止。

#### 通用工具请求/执行子流程 ← v0.14.6 新增，v0.15.0 扩展子 agent 调度工具

通用工具短索引登记外部行动能力，但只有已开通 handler/backend/capability gate 的工具可执行。Spec339 后 filesystem 只读通用工具包括 `file_read` 与 `file_search`：

1. LLM 直接调用当前档位下发的 provider-native `file_read(path=...)`、`file_search(root=...; pattern=...)`、`file_edit(path=...; patch=...)`、`file_write(path=...; content=...)`、`web_fetch(url=...)`、`web_search(query=...)`、`shell_command(cwd=...; command=...)` 或 `subagent_dispatch(task_goal=...)`；Runtime 投影后形成内部 `general_tool_request`；
2. Runtime 校验该 `tool_id` 属于 `general_tool`，且状态为 enabled；
3. backend ready 后，Runtime 计算同轮请求签名；签名只包含 `tool_id` 与关键参数，不包含 `call_id/purpose/reason/provider trace` 等噪声；
4. 如果同一 reaction round 内已有同签名结果，直接返回 `general_tool_result status=rejected`，成功重复为 `duplicate_tool_result_satisfied`，失败重复为 `duplicate_tool_failure_repeated`，不再调用 handler；
5. 非重复请求进入 `ExecutionCapabilityGate`，在 handler 前执行动作级能力裁决；受限档强行提交高副作用工具时直接返回 `permission_level_required`，其他拒绝直接返回 `general_tool_result status=rejected`，稳定 reason 为 `capability_denied`、`dangerous_shell_command`、`outside_allowlist`、`private_network_denied` 或 `write_scope_missing`；
6. 门禁放行后，脚本形成内部 `general_tool_call`，由注册表 active handler 执行；
7. 结果以 `general_tool_result` 返回给下一次反应步迭代；执行事实写入 `kind=tool_fact` now 语料块，只读正文/候选写入 `kind=material` 或 CONTENT；它不是 `protocol_tool_receipt`，不进入 `tool_transaction_audit`。

`file_read` 默认限制在工作区 allowlist 内，拒绝 `.git`、密钥类路径与 `persona/STM`、`persona/LTM`、`persona/relation` 等 live persona 私密数据。`file_search` 使用同一读取 allowlist，只接受文件名 glob `pattern`，默认不递归；结果只含候选路径、`result_count/max_results/has_more` 与搜索窗口事实，不读取正文、不进入 CONTENT。`file_edit` 默认只接受 unified diff patch/diff，目标限仓库 tracked 文本文件或显式 allowlist，前置门禁拒绝 live persona、密钥、越权路径、缺 patch 和自然语言写入。`file_write` 仅在放行档下发，可在工作区内创建或覆盖普通文本文件，必须填写 `path/content/purpose`，仍拒绝 live persona、Git 内部路径、密钥类路径、越权路径和非文件目标。`web_fetch` / `web_search` 默认只读公开 `http/https` 网页，前置门禁拒绝本机/私网、登录交互、下载型资源与外部账号操作。`shell_command` 默认只在 allowlist cwd 执行低风险命令，前置门禁拒绝删除、移动、重置、后台服务、网络写、远端脚本管道和凭据读取；当前 Windows 后端按 `cmd.exe` 语义执行，近位 guide 必须提醒 `dir/type/python -m pytest` 优先，PowerShell cmdlet 需显式 `powershell -NoProfile -Command ...`，不得使用 `python - <<'PY'` 等 Bash/POSIX here-doc；多行 Python 应用 `file_write` 写临时 `.py` 后执行，或使用 PowerShell here-string 管道。`shell_command` 的模型可见 `tool_fact` 必须包含限长 `stdout/stderr` 摘录与截断说明；输出先按 bytes 捕获并 strict fallback 解码，解码失败不得向模型可见文本写入 U+FFFD，只给安全摘要和字节长度 / sha256。`subagent_dispatch` 默认只调度边界清晰的子 agent 任务，必须声明任务目标、允许路径和期望产物；写入型任务必须声明 `write_scope`；无真实后端时返回 `rejected/backend_unavailable`，不伪造外部 agent 执行。

#### 协议级固定操作流与流程插槽 ← v0.13.0 新增

三步轮本体（起手步 → 反应步 → 善后步）是协议骨架，不是技能。协议级固定操作流（`protocol_workflow`）是骨架内的固定流程、输出契约、协议工具请求/提交链、POPUP guide/reminder 装配规则与心跳交接规则。它由 DDS 定义边界、由 OS/基座执行、由 config/registry 决定启用项。

流程插槽（`workflow_slot`）是协议工作流中允许挂载程序能力、认知范式调制、提醒模板、脚本事件或基座动作的位置。工程注释上它等价于 hook point / 钩子触发位置；面向 LLM 的主术语统一用“流程插槽”，不把 hook 作为主术语扩散。

插槽等级：

| 等级 | 中文名 | 能否自定义 | 例子 |
|---|---|---|---|
| L0 | 协议硬点 | 不能 | 三步轮顺序、心跳事实源、血脑屏障、协议工具 guide+submission 闸门 |
| L1 | 协议保留插槽 | 位置固定，可配置挂载内容 | 反应步身份确认、安全裁决、模式建议、工具请求/提交、善后压缩 |
| L2 | 位格自定义插槽 | 可新增或调整，但必须登记 | 立场一致性检查、关系姿态复判、教学策略检查、语气风格自检 |
| L3 | 临时工作流实例 | 当前任务/轮短期挂载，稳定后可沉积为 `procedures/subtype=workflow` | 项目专用检查清单、读书任务规则、一次性资料处理规范 |

硬边界：L2/L3 不能改写 L0 硬点；协议工具不能绕开 provider-native schema、Runtime validation、processor / guard、receipt 与运行真账链；固定输出表、脚本事件、基座工具动作和 now-only 内部交接走各自边界，不能伪装成协议工具提交；不能新增第二套心跳事实源。立场一致性检查是起手步可挂载程序能力插槽，不新增 `recent_interaction_trace`；缓存里有最近交互就参考，没有就依靠关系卡、记忆条目与 DC/EC。

#### 容器新建子流程 ← v0.9.1 新增；v0.14.2 旧通道退役

反应步是未来**唯一可以通过协议工具新建或打开工作容器的步**——新建涉及文件创建和内容写入，属于焦点工具与同步工具组合操作。起手步（纯读）和善后步（结构化归档）均不参与容器新建。

Spec 065 后，旧自由文本 `新建 {容器类型}:XXX` 与旧 markdown 行声明旁路退役：脚本不再解析这些文本，不创建容器，不挂载 WB 焦点。Spec 077 起 `container_focus` 已开通为 `protocol_tool / focus_tool`；反应步必须按 `tool_request → guide → protocol_tool_submission → processor → protocol_tool_receipt` 闭环提交容器创建、打开、追加写入、关闭和焦点恢复，`protocol_tool_request` 只是 Runtime 内部分流记录。Spec 078 起 `container_read` 已开通为 `protocol_tool / read_tool`；只读已有容器内容不得为了查看而切换 WB focus。

**卸挂时机**：仅由 `container_focus` 的 `close/restore` 或脚本任务生命周期执行；自由文本"关闭 {容器ID}"不再作为新写入入口。

各容器类型的详细新建步骤见 §25.11 容器新建协议；Spec 077 首批只启用 `DC/EC/PRJ/FUT`，其余类型继续等待后续 spec。

### 善后步四阶段

| # | 子阶段 | 执行者 | 强制度 | 做什么 | 产出 |
|---|--------|--------|--------|--------|------|
| ① | 接收输入 | 脚本 | 硬 | 装配善后步上下文（压缩版频率层 + 最近缓存/当前缓存字符窗口 + 反应步原始输出） | 善后步上下文 |
| ② | LLM结构化工具输入 | LLM | 中 | 填两线 substrate 工具输入块（训练材料整理、最近缓存压缩） | 结构化输入块 |
| ③ | 脚本原子写 | 脚本 | 硬 | 善后两线按目标文件分筐→单写者逐 key 串行写入；脚本无条件写最小承诺边界标记；读取但不重做反应步协议工具回执 | 各文件落盘 + 边界语料 |
| ④ | 心跳收尾 | 脚本 | 硬 | 脚本清理本轮已消费 heartbeat flag；`heartbeat_restart` 重置待命倒计时并恢复心跳检测。如需中继轮，必须由反应步合法 `reaction_finalize(handoff_text)` 或脚本事实源置位 `continue_requested`；反应步中继正文登记到 `relay_intents` 隐藏 payload，脚本事实说明写 `setup_fact` 或 `material` | state.json 更新 + relay_intents / setup_fact / material |

善后 LLM 两线工具输入：**训练材料整理**（`connection_material_settle` 跨条目关键词桥接，top 8/轮 → `tacit_material_settle` kept/dropped/added） / **最近缓存压缩**（仅在本轮发生 `lately_trimmed=true` 后挂载删后幸存段；raw_log 与 Corpus 节归档保留原文；LLM 给 `cache_compact` 填语义融合输入，可逐块摘要或合并连续块；实际重写由基座工具 `cache_compact` 执行）。全为结构化工具输入块，零焦点工具操作。不评判结论真伪优劣，不回滚，不可跳过。联想集由脚本在③原子写阶段根据本轮已即时生成的有效 `memory_write_receipt` 暴力计数，不是 LLM 输入项。

Spec 064/083 训练材料证据包：脚本在 cleanup 上下文挂载 `preselection_evidence`、`connection_candidate_entries`、`added_prework_traces`。`connection_material_settle` 先产出有效桥接候选，脚本做光锥校验形成有效联系图；`tacit_material_settle` 随后依据起手预选、明确取消、有效联系图和前置新增痕迹落账。`kept` 必须有承接证据或有效联系命中；`dropped` 可由明确放弃或 `no_valid_connection_hit` 形成；`added` 只认最终回复前的前置痕迹，不从最终答案事后反推。

Spec 062 边界裁决：最小承诺与成品输出不再是善后 LLM 表。最小承诺由脚本在善后 phase 内写成纯边界 `kind=minimum_commitment`；用户可见回复以反应步 `assistant_reply` 为唯一权威来源。蓝屏计数、熔断、心跳恢复与事务验账归 Runtime/heartbeat/fault 等基座动作，不进入善后 LLM 语义填表。

Spec 041/053/083 迁出项：关键词由反应步 provider-native `memory_write` 调用提供，候选关键词至少 1 个，0 个为格式错误；`memory_write` 脚本只做清洗、去重、按 F/S/A 上限裁剪和 STM 倒排索引写入，不从正文或标题补语义词；轮内归档/记忆写入由该调用同步落盘，且交互感受词与关系感受词只能从同一工具的 schema description 清单选择；技能与反射采用结算、故障记账由反应步协议工具或脚本即时事件链处理；状态变化由善后步固定 `state_settle` 基座入口按事实结算，不再由 LLM 填 `state_update` 类提交表。若旧善后表仍出现在 LLM 输出中，Runtime 不再接受或执行旧职责。

### 三步工具通道

| 步 | 焦点工具 | 同步工具 | 只读装配 | 总特征 |
|----|----------|----------|----------|--------|
| 起手步 | ✗ | ✗ | ✓ | 纯读 |
| 反应步 | ✓（WB焦点，每迭代最多1） | ✓（N个，批量声明） | ✓ | 生成+工具调度 |
| 善后步 | ✗ | ✓（两线 substrate 工具输入） | ✓ | 收束+归档 |

### 三步上下文装配对比

| | 起手步 | 反应步 | 善后步 |
|---|---|---|---|
| 频率层 | 压缩版（高频层CONTENT挂索引不挂正文） | 全量（高频层CONTENT已填充） | 压缩版（高频层CONTENT挂索引不挂正文） |
| 装配者 | 脚本自装配 | 脚本按起手步③指令集执行 | 脚本接收装配 |
| 动态性 | 静态 | 动态（loop中LLM可追加/卸载） | 静态 |

## 23.3a 五类轮编排表

| 轮类型 | Tier | 起手步触发源 | 反应步形态 | 善后步输出 |
|--------|------|------------|-----------|-----------|
| **交互轮** | 1 | 用户消息到达 | 1-N 次装配+生成，超时存档续轮 | **有回复**·联系集+默契集+联想集更新+最小承诺 |
| **节律轮** | 1 | 心跳判定主轴或日历节律到期 | 写节志+消费真实调用留下的 connectivity 证据+alerts归档 | 节志落盘+警戒结算+alerts归档进IMM |
| **中继轮** | 2 | 长时任务 checkpoint（300s时限） | **中继续传：总结进度** | **无对外回复** |
| **自主轮** | 3 | 心跳唤醒/任务调度/自觉能动 | 1-N 次执行，超时存档续轮 | **可能无回复**·**进化集整理（阈值触发，默契集/联系集积累驱动）** |
| **待命轮** | 4 | 上一轮结束后≥30分钟/心跳紧急唤醒 | 检查已有 connectivity／breaker 证据；不自动发送付费 probe | 健康状态归档 |

三种关键修正：
1. **中继轮反应步**：~~复盘/整合/压缩~~ → "中继续传：总结进度"。UPSP 没有"压缩上下文"概念——对话历史走履带式管理，自然滚动。
2. **进化集提炼归属**：~~节律轮兜底~~ → **自主轮负责，阈值触发**。默契集/联系集pending文件行数达阈值时触发自主轮进化集整理。自主轮不跑则进化集等着，不强制。
3. **中继轮≠节律轮**：中继轮跟任务走（长时任务存档续命），节律轮跟全局走（固定周期大整理），完全独立。

## 23.4 五类轮优先级运行守则

### 优先级分层

```
Tier 1（可合轮）：交互 = 节律
Tier 2：中继
Tier 3：自主
Tier 4：待命
```

**分层依据**：交互是用户在场，节律是全局大整理——两者都不能拖延且善后清单不互斥，因此同层可合轮。中继有在途任务的存活压力（上下文还热着，不续就凉），优先于自主。自主是位格主动发起的内部活动，有内部状态驱动但无时效压力。待命只在无高层触发时检查已有健康证据，优先级最低。

### 判定流程

```
起手步读取 state + heartbeat_flags
  │
  ├─ rhythm/calendar/API/token/process类? ──→ 节律轮；若 user_message_waiting 同时存在则标记合轮
  │
  ├─ user_message_waiting?                ──→ 交互轮
  │
  ├─ continue_requested?                  ──→ 中继轮
  │
  ├─ STM/evolution类?                     ──→ 自主轮
  │
  ├─ standby/shelve类?                    ──→ 待命轮
  │
  └─ 以上都无                              ──→ 待机（idle）——轻度休眠，心跳正常tick，随时可被唤醒
```

### 冲突矩阵

| 情况 | 处理 |
|------|------|
| 交互 + 节律 | 合轮：节律可优先结算，但真实用户输入必须在 setup 阶段进入 `kind=interaction` / `role=user` 上下文；当前指南顺序为紧急最小处理 → 主轴节律 → 日历节律 → 交互，善后步按真实结构回执逐项结算 |
| 高层 + 低层（非待命） | 高层先，低层flag保留，下一轮处理 |
| 任何轮 + 待命 | 待命倒计时重置，当轮不处理待命 |

### 善后步flag清零语义

善后步④在恢复心跳检测前执行两件事：
1. **清零本轮已消费的flag**：`user_message_waiting` 只在真实用户输入已进入本轮上下文、反应步完成有效收束、且最终回复已生成后清；若 flag 存在但队列为空，或 final_reply 失败，则保留该 flag 并留下审计警告。`rhythm_due` 只在主轴 `chronicle_write` 成功后清；日历 flag 只在对应日/周/月/季/年 `chronicle_write` 成功后清；紧急 flag 只在对应 `alert_mode_settle` 成功后清；未处理的低层 flag 和被压后的 `continue_requested` 保留。
2. **重置待命倒计时**（无条件，每一轮都做，包括待命轮自己）

三条机制联锁消除并发风险：轮执行期间暂停心跳检测（执行中无新flag）→ 善后步选择性清零+待命归零 → 心跳检测面对干净快照恢复。

### 插话规则

任何非交互轮运行期间用户消息到达 → 写入缓冲区 → 当前步走完 → 善后步正常结算 → 心跳检测恢复后首个检查点置位 `user_message_waiting` → 下一轮按判定流程处理（若同时 `rhythm_due` 则合轮）。被中断的轮不丢弃，其flag在善后步中被保留。

### 轮间内部交接

`runtime.next_round` 已删除，不再作为调度便签、pending 队列或 heartbeat 输入。

需要唤起下一轮时，事实源必须直接落到 heartbeat flag：外部交互置 `user_message_waiting`，长时任务中继置 `continue_requested`，节律/健康事件置 rhythm 组 flag，自主材料阈值置 `evolution_pending`。若需要让下一轮 LLM 明白“为什么被唤醒/接着做什么”，脚本写入 `kind=setup_fact`；跨轮中继正文先写入 `relay_intents[]`，由下一轮 relay setup 投影为明确的 `kind=relay_handoff`；运行期临时任务条走 GUIDE/POPUP/内容窗口。这些块不置 flag，不改变轮类型。

示例：

```json
{
  "kind": "setup_fact",
  "role": "system",
  "text": "[心跳触发交接] 本轮类型=relay；触发flag=continue_requested。",
  "policy": {"now": true, "lately": false},
  "ref": {"source": "heartbeat", "trigger_flag": "continue_requested"}
}
```

分工链路：脚本/协议工具确认事实源（"发生了"）→ heartbeat flag 置位（"该醒了"）→ 起手步按 flag 判定轮型（"怎么开轮"）→ `setup_fact` / `material` 只补充语义说明（"接着干什么"）。

## 23.5 心跳闹钟模型

心跳就是一个闹钟。不调 API，不走 LLM，只做布尔检查→置位标记。不能关自己——轮来关它（善后步清标记+重启）。

```
时间轴 ────────────────────────────────────────────→

     ┌─────────┐                      ┌─────────┐
     │ 心跳运行 │ ← 轮开始时暂停 → 轮运行中 → 善后步重启 →│ 心跳运行 │
     └─────────┘                      └─────────┘
```

heartbeat_flags 当前为 20 项：基础14项 + 日历5项 + 进化集材料阈值1项。
心跳只做布尔检查，不调 API、不装配内容、不做业务判断。
`runtime.next_round` 已退役；heartbeat 不读取自然语言便签，不消费 `next_round.type/subtype/brief`。
心跳硬约束：不计轮数 / 不调 API / 不注入 LLM / 不判断业务 / 不回滚 / 轮内暂停（唤醒起手步后立即暂停tick，善后步清flags后重启）。善后步是 flags 的唯一消费者出口。

## 23.6 节律轮体系

节律轮分两类，各管各的时钟粒度：

### 主轴节律轮（每 32 轮）

| # | 职责 | 说明 |
|---|------|------|
| 1 | 写节志 | 脚本预填统计 + LLM 写 ≤512 字正文，落盘 Chronicle/rhythms/ |
| 2 | 连接状态复核 | 读取当前有效模型链真实调用留下的 connectivity／breaker 证据；不主动发送付费 probe（§39） |
| 3 | alerts 归档进 IMM | STM/health/base/alerts.md → LTM/Immune/alerts.md |

主轴节律轮把 `STM/buffer/raw_log.jsonl` 当前积累的原始语料归档进 `COR/public/rhythms/`，成功后清空 raw_log；日历节律再按周期逐级合并。

### 日历节律（五层，按自然日历触发）

心跳五档日历标记：`calendar_day_due` / `calendar_week_due` / `calendar_month_due` / `calendar_quarter_due` / `calendar_year_due`。

同一轮内多个日历标记同时置位时，从底往上逐层处理（日→周→月→季→年），产出一层消费一层：

| 层级 | 触发 | LTM 降格 | 编年史 | 原始语料合并 |
|------|------|---------|--------|------------|
| 日 | 每天 | F→S→A LLM 语义压缩（decay_countdown 到期条目），A→Backup 脚本直接搬 | 日志 | rounds JSONL→日合并 |
| 周 | 每周 | — | 周志（日志×压缩） | 日合并→周合并 |
| 月 | 每月 | — | 月志（周志×0.3） | 周合并→月合并 |
| 季 | 每季 | — | 季志（月志×0.3） | 月合并→季合并 |
| 年 | 每年 | — | 年志（季志×0.3，不删） | 季合并→年合并 |

LTM 降格压缩：F→S（2048→512 字）和 S→A（512→128 字）需 LLM 语义压缩，日节律轮反应步执行。A→Backup 纯脚本搬文件，不需要 LLM。

### 节律轮·上下文整理（context_cleanup）

上下文整理是节律轮的运维子任务，协议模式为"复盘"。触发源不是独立轮型，也不是 `next_round` 便签；装配器检测到 lately 归零后仍持续超窗时，Runtime 置位 `context_pressure=true`。下一轮 rhythm agenda 据此物化 `context_pressure_rhythm_guide`，任务说明随 GUIDE 生命周期出现与撤下，不另写正式 cache 语料块。

**处理范围**：
可处理：
- 高频层 CONTENT 挂载正文；
- reference window；
- workbench 临时挂载；
- resident_list 记忆栏；
- 非焦点容器展开状态；
- EXPLORER 展开格口；
- 过期关系回想挂载。

不可处理：
- 永固层；
- 定期层；
- POPUP；
- 交互输入、资料输入、工具事实、起手事实与中继交接；
- 当前缓存 now 的真源内容；
- reflexes 生命周期；
- 原始语料与 round 快照。

**触发链路**：
```
装配器估算超窗口
  → lately 本步装配选择减少
  → 如 lately 归零后仍高压，在本步 manifest.json 记录本次装配压力事实
  → Runtime 置位 `context_pressure=true`
  → 下一轮起手步读取 heartbeat_flags
  → rhythm agenda 物化 `context_pressure_rhythm_guide`
  → 善后步按本轮已消费 flag 清零
```

## 23.7 插话处理

| | 外部输入 | 内部信号 |
|---|---|---|
| 来源 | 用户消息 | 脑区 popup |
| 安全检查 | 需要（脚本粗筛） | 不需要（已在屏障内） |
| 生效粒度 | **轮级**（迭代边界退出→善后→新轮） | **步级**（迭代内可生效） |
| Base版 | ✅ | ✗ |
| Plus版 | ✅ | ✅ |

## 23.8 版本演进路径

中枢引擎（engines/）跨所有版本存在，是确定性运行时基础设施，永远不坐决策席。

| 版本 | 决策者（谁想） | 起步/善后步执行 | 反应步执行 | 中枢引擎 |
|------|--------------|---------------|-----------|---------|
| **Base** | LLM + 硬规则脚本 | LLM（远端API） | LLM（远端API） | runtime.py + heartbeat.py + executor.py |
| **Plus** | LLM + 调度脑（低延迟本地小模型） | 调度脑接管，LLM shadowed | LLM（远端API） | 同上 + 调度脑挂载 |
| **Pro** | 专家集群多数表决 | 特化专家多数表决/轮值 | MoE矩阵 | 同上 + 专家集群调度 |
| **Vita** | 原生权重决策 | 本地权重 | 本地权重 | 权重化的运行时 |
| **Corpus** | 硬件决策 | 忆阻器 | 忆阻器 | 硬件化的运行时 |

> **按步分岗视角的演进表**见第三十八章§38.1——那里按起手/反应/善后三步分别列出各版本使用的模型配置。本表聚焦决策者+引擎组件维度。

phase 状态机由 engines/runtime.py 独占写入，是引擎的核心状态字：

```
idle ──[触发]──→ presub ──→ main ──→ post ──→ idle
                   │                    ↑
                   └──(空转轮跳过反应步)──┘
```

### engines/ 目录结构

```
engines/
├── runtime.py      # Base 轮/步事务协调器：推进 phase、组织起手/反应/善后
├── heartbeat.py    # 心跳闹钟：定时检测并生成触发分类，不调用 LLM
└── executor.py     # API 执行器：endpoint 路由、熔断、握手与超时
```

Base版"单线程硬脊髓"的落地。运行快照、协议工具回执、提交类暂存和
上下文审计产物由 runtime 协调 data/logic/assembly 落盘，不再把旧
提交箱或存档模块列为 engines/ 当前组成。

### engines/ 与 scripts/ 当前分工

| | engines/ | scripts/ |
|---|---|---|
| 关心什么 | 轮/步时序、try/finally 落盘纪律、跨层协调 | 一次性维护、迁移、审计或人工触发的工具脚本 |
| 是否在三步主链 | 是，runtime 是 Base 轮/步事务协调器 | 否，不作为三步轮主工人层 |
| 能否承载语义决策 | 否，只做确定性协调 | 否，只做限定任务的脚本执行 |

判别标准：凡是需要知道当前 phase、轮/步边界、善后必达纪律或跨步
回执消费的逻辑，属于 runtime 等运行编排层；一次性清理、迁移、审计、
人工运维入口才放在 scripts/。

引擎可以调用 data/logic/assembly 的公开接口，不把 scripts/ 当作
运行时主业务层。脚本不得反向改写 phase、total_round、heartbeat flags
等运行事实源。

### engines/ 与 scripts/ 代码注释规范

engines/ 中的文件必须写明：
- 它负责哪个 phase / tick / 调度边界
- 它消费或生成哪些结构化回执/审计产物
- 它不直接做 LLM 语义判断

scripts/ 中的文件必须写明：
- 它处理什么输入
- 它输出什么结构
- 它不得读取或修改 phase / total_round / 非授权 heartbeat flags

### 提交类暂存数据结构

```json
{
  "event_id": "evt-20260429-001",
  "source": "cleanup_step",
  "target_file": "persona/STM/memory/memory.md",
  "target_key": "entry_0042",
  "priority": 5,
  "payload": { "...": "..." },
  "created_at": "2026-04-29T01:30:00+08:00",
  "status": "pending"
}
```

单写者消费规则：同一 target_file + target_key 的提交箱条目由脚本串行消费，保证原子写入。

---

# 二十四、插话机制

> v0.4新增章节。

## 核心命题

插话不是多开LLM，是对当前运行态打补丁再安全续跑。

## 最简方案

```
用户插话
  ↓
脚本把当前状态存一下（checkpoint写入status.json）
  ↓
把用户新话 + 当前状态 一起给LLM
  ↓
LLM自己决定：继续干 / 先回答你 / 换方向
```

LLM自分类，不需要脚本做语义判断。

## 脚本职责

唯一要做的：在workbench/status.json里加一个pending_interrupt字段，记录用户插话内容。LLM读到这个字段就自己判断怎么处理。

## 插话历史

`STM/buffer/interrupts.jsonl`，追加记录，审计用。不新开文件夹。

---

# 二十五、工作容器统一接口

> v0.5重写章节。替代原T/W梯位。v0.5.1恢复总索引（维护脚本自动生成）+新增container_registry.json。

## 25.1 核心原则：不统一模板，统一接口

各种容器差异太大，统一模板是削足适履。

## 25.2 全景速查

**1工作台 + 9容器**。工作台在上层（STM），9容器在下层（LTM）。

| 前缀 | 类型 | 名称 | 位置 | ID格式 | 注册表 | 状态机 |
|------|------|------|------|--------|--------|--------|
| WB- | 工作台 | 中枢调度台 | STM/workbench/ | `WB-main`(固定) | status.json | active→idle→suspended |
| DC- | 辩证链 | 辩证链 | LTM/Dialectics/ | `DC-{n}` / `DC-{n}-{m}` | registry.json | ongoing→suspended→concluded |
| EC- | 事件链 | 事件链 | LTM/Events/ | `EC-{n}` / `EC-{n}-{m}` | registry.json | active→interrupted→restarted→ended→cancelled |
| PRJ- | 项目 | 项目 | LTM/Projects/ | `PRJ-{date}-{seq}` | registry.json | active→paused→ended |
| SKL- | 技能 | 技能 | LTM/Skills/ | `SKL-{cat}-{name}` | registry.json | active→expired→planned |
| IMM- | 免疫 | 免疫 | LTM/Immune/ | `IMM-{file}-{seq}` | registry.json | active_threat→monitoring→resolved→acquired |
| CHR- | 编年史 | 编年史 | LTM/Chronicle/ | **无** | **无** | **无** |
| COR- | 语料库 | 语料库 | LTM/Corpus/ | **无** | **无** | **无** |
| FUT- | 未来 | 未来 | LTM/Future/ | `FUT-{cat}-{seq}` | registry.json | planned→in_progress→completed→abandoned |
| ITR- | 迭代 | 迭代 | LTM/Iteration/ | `ITR-{date}-{seq}` | registry.json | collecting→planned→training→deployed→retired |

**Memory（MEM-）不是工作容器**——它是总线+存储层，所有容器通过linked_containers挂在记忆条目上。

**CHR/COR = 纯目录，无ID/无注册表/无状态机**。被动查阅的存档仓库不参与容器联动。Spec350 后，CHR 的节律写入不走容器 create/write，也不是随意声明查看后直接写盘；只能由 Runtime 挂上活动编年史焦点后，通过 provider-native `chronicle_write` 写入当前焦点对应文件。COR 仍为脚本归档语料库。

## 25.3 统一接口三件套

> 本节描述的是**容器实例层**的统一接口三件套，不包含顶层工作容器总索引（见25.6节）。

**① 索引行**（在EXPLORER模块展示）：

```
DC-3            | Metabolism ≠ Labor    | ongoing    | 3  | #metabolism #labor
EC-2            | Base Development      | active     | 2  | #architecture
PRJ-20260417-01 | Base版协助视频         | active     | 4  | #video
IMM-active-3    | prompt injection 0416 | active_threat | 1 | #security
FUT-objectives-1| Base版协助视频         | in_progress| 2  | #production
```

**② 注册表**（JSON，脚本维护，LLM不直接读写）

8个必选字段 + 第9字段focus：

```json
{
  "id": "DC-3",
  "type": "dialectic",
  "title": "Metabolism ≠ Labor",
  "status": "ongoing",
  "created_at": "2026-04-13T01:04:15+08:00",
  "updated_at": "2026-04-13T01:44:22+08:00",
  "entries": [],
  "tags": [],
  "focus": false
}
```

**字段英文化规范**：字段名英文、系统字段值英文（status:"ongoing"而非"继续"）、title等自然语言可用中文、注释中文。

**focus字段（焦点机制）**：通用第9字段，适用于有实例 `meta.json` / 实例或局部 `registry.json` 的工作容器条目。`focus=true` 表示该容器当前处于 WB 焦点，可由 `container_focus` 焦点工具编辑。同一时刻最多1个 `focus=true`。与WB status.json的focus字段镜像同步。**区别于 resident_list**：常驻清单只读，不授予编辑权。

**③ 内容区**（自然语言，LLM读写）

各容器自定义格式，不强制统一。

## 25.4 各容器ID与特有字段

### DC- 辩证链

- **ID**：`DC-{链序号}` / `DC-{链序号}-{笔记号}`
- **注册表**：registry.json（JSON格式，与open.md/closed.md同级）
- **特有字段**：`source_round`（开链轮次）、`conclusion`（链结论，concluded时填充）
- **状态机**：ongoing→suspended→concluded

### EC- 事件链

- **ID**：`EC-{链序号}` / `EC-{链序号}-{笔记号}`
- **注册表**：registry.json（同DC结构）
- **特有字段**：`severity`（事件等级1-5）、`resolution`（事件解决记录）
- **状态机**：active→interrupted→restarted→ended→cancelled

### PRJ- 项目

- **ID**：`PRJ-{date}-{seq}`，文件夹名=date+seq（如20260417-01），中文项目名在注册表title
- **注册表**：每个项目独立registry.json
- **特有字段**：`deadline`、`milestones`、`progress`、`phase_count`（阶段总数）、`completed_phases`（已完成阶段数）、`current_phase`（当前阶段编号）
- **状态机**：active→paused→ended
- **目录结构** ← v0.9.0 新增：
  ```
  Projects/{date-seq}/
  ├── plan.md           # 计划书（项目总纲，创建时写入）
  ├── phases/           # 阶段文件
  │   ├── _index.md     # 阶段索引（脚本维护：编号/名称/状态/挂靠记忆条目ID）
  │   ├── 01_{阶段名}.md  # 阶段一（目标+产出+状态：pending/doing/done/blocked）
  │   ├── 02_{阶段名}.md  # 阶段二
  │   └── ...
  ├── notes.md          # 项目笔记（执行中临时记录，可覆写）
  └── registry.json     # 注册表
  ```
- **挂靠约束** ← v0.9.0 新增：每个阶段文件必须挂靠至少一条记忆条目（通过 linked_containers）。entries 为空且非本轮新建→悬空→POPUP 警告。详见 §25.9。

### SKL- 技能

- **ID**：`SKL-{category}-{skill-name}`，如 `SKL-procedures-format-check`
- **注册表**：`LTM/Skills/registry.json`；索引为 `LTM/Skills/index.md`
- **当前可创建分类**：`procedures`、`patterns`。`licenses/habits/reflexes` 只保留目录兼容与未来设计位置，不开放模型创建。
- **当前字段**：`id/type/prefix/name/title/status/category/created_at/updated_at/entries/tags/focus/linked_memories/path`；不使用成熟度、熟练度、稳定度或投影字段。
- **目录**：`LTM/Skills/{category}/{skill-name}/card.md + changelog.md`
- **当前入口**：公共记忆写入成功后，用通用 `memory_container_create` 创建并挂接；之后用 `container_read/focus` 与 `memory_container_write` 读回或续写。
- **当前装配**：技能只出现在工作容器/技能索引与明确读取结果中，不自动进入定期层。
- **状态机**：active→expired→planned
- **记忆条目桥接**：技能正文以真实 `MEM-*` 为引用源，成功 receipt 同步更新技能 `linked_memories` 与记忆 `linked_containers/current_overview`。选择技能容器不替代记忆生产，也不自动创建 DC/EC/PRJ。

### IMM- 免疫

- **ID**：`IMM-{文件名}-{序号}`，如IMM-active-3、IMM-chronic-1、IMM-birth
- **注册表**：整个免疫一个registry.json
- **8个固定md**：birth/chronic/transplant/surgery/active/resolved/acquired/alerts
- **alerts.md**：系统事件归档（从STM/health/base/alerts.md节律轮搬入，按时间追加，无条数上限）
- **特有字段**：`severity`(1-5)、`category`(threat/health/maintenance)
- **状态机**：active_threat→monitoring→resolved→acquired
- **合并说明**：原Medical+Security合并为Immune，安全事件短程走弹窗，长程走IMM

### FUT- 未来

- **ID**：`FUT-{category}-{seq}`，序号固定不变
- **注册表**：整个未来一个registry.json
- **三个平级md**：objectives.md/plans.md/predictions.md
- **特有字段**：`subtype`(objective/ideal/dream/plan/prediction)、`source`(来源链ID)
- **状态机**：planned→in_progress→completed/abandoned
- **记忆条目桥接** ← v0.9.0 新增：三个 md 文件内条目通过记忆条目的 linked_containers 挂靠。EC→FUT 走二段跳：事件链[计划]节点→记忆条目→FUT/plans.md 落地。挂靠约束：内容条目无 linked_containers→悬空→POPUP 警告。详见 §25.9。

### ITR- 迭代

- **ID**：`ITR-{date}-{seq}`
- **注册表**：整个迭代一个registry.json
- **子目录**：Lineage/Blueprints/Raw/（原始数据）/Materials/（加工产品）/Logs
- **状态机**：collecting→planned→training→deployed→retired
- **Base版新建条目默认 `collecting` 状态**——正在收集训练材料，还没到训练阶段

### ITR目录结构

```
LTM/Iteration/
├── Lineage/                       # 传承谱
├── Blueprints/                    # 蓝图
├── Raw/                           # 原始数据（善后步每轮产出）
│   ├── Tacit/                     # 默契集
│   │   ├── pending.jsonl          # 未处理（自主轮读取触发源）
│   │   ├── processed.jsonl        # 已处理总账
│   │   └── processed_YYYY_MM_DD_R*.jsonl # 单次处理批次备份
│   ├── Association/               # 联想集（五张计数表，无pending/processed）
│   │   ├── assoc_kw_kw.json       # 关键词×关键词
│   │   ├── assoc_kw_ifeel.json    # 关键词×交互感受词
│   │   ├── assoc_kw_rfeel.json    # 关键词×关系感受词
│   │   ├── assoc_ifeel_rfeel.json # 交互感受词×关系感受词
│   │   └── assoc_object_rfeel.json # 交互对象×关系感受词
│   └── Connection/                # 联系集
│       ├── pending.jsonl          # 未处理（自主轮读取触发源）
│       ├── processed.jsonl        # 已处理总账
│       └── processed_YYYY_MM_DD_R*.jsonl # 单次处理批次备份
├── Materials/                     # 加工产品（自主轮提炼）
│   └── Evolution/                 # 进化集
├── Logs/                          # 日志
└── registry.json
```

Raw = 每轮善后步产生的原始训练数据；Materials = 自主轮提炼的加工产品。联想集无需pending/processed——五张计数表本身是累加的，复用率高，且LTM关键词表degree计数依赖这些表。默契集和联系集需要pending/processed分离——自主轮通过pending行数判断触发时机。

### 四种训练材料集

| # | 材料集 | 位置 | 性质 | 谁产的 | 喂给谁 |
|---|--------|------|------|--------|--------|
| 1 | 默契集 | Raw/Tacit/ | 原始数据 | 善后步LLM每轮 | 预连接模型 |
| 2 | 联想集 | Raw/Association/ | 原始数据 | 善后步③脚本每轮 | 联想索引+LTM关键词表 |
| 3 | 联系集 | Raw/Connection/ | 原始数据 | 善后步LLM每轮 | 联想索引+进化集 |
| 4 | 进化集 | Materials/Evolution/ | 中间产品 | 自主轮（阈值触发） | 中枢 |

### 自主轮触发锚点

自主轮进化集整理的触发源 = `Raw/Tacit/pending.jsonl` + `Raw/Connection/pending.jsonl` 行数。阈值可配置（config/system.json `autonomous_trigger.*`），默契集/联系集各自独立计数，默认各 512 行。心跳检测到任一 pending 达阈值后置位 `evolution_pending`，运行时据此触发自主轮。自主轮处理后：pending.jsonl 内容追加至 processed.jsonl，同时写入本次 `processed_YYYY_MM_DD_R*.jsonl` 批次备份 → pending.jsonl 清空。

### 联想集记录结构（五张计数表）

联想集 = 纯脚本暴力计数，不经LLM。五张表各一个JSON文件，统一格式——字典 key=配对，value=次数：

```json
{
  "共格|延续性": 3,
  "共格|记忆": 7,
  "劳动|异化": 1
}
```

| # | 文件 | 配对类型 | key排序 | 用途 |
|---|------|---------|--------|------|
| 1 | `assoc_kw_kw.json` | 关键词×关键词 | 字典序去重 | 主题共现 |
| 2 | `assoc_kw_ifeel.json` | 关键词×交互感受词 | 不对称 | 主题-情绪 |
| 3 | `assoc_kw_rfeel.json` | 关键词×关系感受词 | 不对称 | 主题-关系 |
| 4 | `assoc_ifeel_rfeel.json` | 交互感受词×关系感受词 | 字典序去重 | 情绪-关系共振 |
| 5 | `assoc_object_rfeel.json` | 交互对象×关系感受词 | 不对称 | 印象 |

- 不记录时间——纯暴力计数
- 新配对 → 插入 `"A|B": 1`；旧配对 → 原地 `+1`
- 每轮善后步③脚本只处理本轮产生的配对，不遍历全表
- 排序去重2张（表1/4），不对称3张（表2/3/5）
- 赫布学习——"世上本没有路，走的人多了便成了路"

### 默契集记录结构（按轮JSON）

默契集善后步LLM每个非待命轮产出一行，待命轮不产出。它在联系集有效图之后，对比起手步预选的记忆条目、工具、容器等内容，以及反应步在实际工作前的承接、取消和新增痕迹，记录 kept / dropped / added。每行一轮，写入 `Raw/Tacit/pending.jsonl`。

```json
{
  "round_id": "000042",
  "kept": ["MEM-001", "PRJ-002", "SKL-003"],
  "dropped": ["MEM-005"],
  "added": ["MEM-012"],
  "items": [
    {"item_id": "MEM-001", "item_type": "memory", "action": "kept", "note": "有效联系图命中", "evidence_refs": ["connection:MEM-001"], "drop_reason": ""},
    {"item_id": "MEM-005", "item_type": "memory", "action": "dropped", "note": "反应步未承接", "evidence_refs": [], "drop_reason": "no_valid_connection_hit"},
    {"item_id": "MEM-012", "item_type": "memory", "action": "added", "note": "前置读取新增", "evidence_refs": ["memory_content_read:MEM-012"], "drop_reason": ""}
  ],
  "timestamp": "2026-05-13T21:00:00+08:00"
}
```

| 字段 | 类型 | 说明 |
|------|------|------|
| round_id | string | 轮编号 |
| kept | string[] | 起手步预选项中被反应步实际承接的ID |
| dropped | string[] | 起手步预选项中被明确放弃，或未明确放弃但有效联系图/承接证据没有命中的ID |
| added | string[] | 起手预选之外，反应步在实际工作前额外引入的新内容或新关联ID |
| items | object[] | 本轮逐项处理明细，保留兼容字段 item_id/action/note，并可扩展 item_type/selection_trigger/evidence_refs/drop_reason |
| timestamp | ISO8601 | 写入时间戳 |

**判断机制**：善后步先分析联系集，脚本光锥校验后形成有效联系图；随后默契集才落 kept/dropped/added。`kept` = 起手步预选项被反应步实际承接，必须有承接证据或有效联系命中。`dropped` = 起手步预选项被明确放弃，或未明确放弃但有效联系图/承接证据中没有命中，脚本可补 `drop_reason=no_valid_connection_hit`。`added` = 起手预选之外，反应步在实际工作前额外引入的新内容或新关联；只认读取请求、有效回执、交接、显式新增声明等前置痕迹，不从最终答案事后脑补。

**自包含约束**：轮快照按 `audit.round_snapshot_retention` FIFO 保留（默认8）；超过配置保留数的快照会被删除，不能靠 round_id 回溯。默契集必须自包含本轮挂载与调整记录。

### 训练材料数据管线

```
善后步每轮：
  ├── LLM② → 联系集跨条目词对 → 脚本光锥校验 → Raw/Connection/pending.jsonl
  ├── LLM② → 默契集按轮记录（依赖有效联系图）→ Raw/Tacit/pending.jsonl
  └── 脚本③原子写 → 联想集五张计数表直接累加（Raw/Association/）

自主轮（阈值触发）：
  ├── 读 Tacit/pending.jsonl + Connection/pending.jsonl
  ├── 跨轮统计 + 模式识别 → 提炼进化集 → Materials/Evolution/
  └── pending 内容追加至 processed 总账 + 本次 processed_YYYY_MM_DD_R*.jsonl 备份 → pending 清空
```

联想集直接写入计数表，无pending/processed环节，不设 pending 行数上限；重复配对只增加计数。默契集和联系集走pending/processed分离，自主轮通过pending行数触发进化集整理。进化集是LLM提炼后的中间产品，由自主轮负责。

## 25.5 CHR/COR 无注册表

编年史和语料库是被动查阅的存档仓库：
- 无ID、无注册表、无状态机、不参与linked_containers联动
- 不与记忆条目绑定——记忆条目拉取后自动挂载的是有注册表的容器，CHR/COR不走这条路径
- **打开方式**：提取脚本列文件夹目录——文件名即索引，最近文件优先显示。无注册表则无索引行，目录本身就是最简索引
- 脚本只管文件生命周期（降采样、保留期限、清理）
- **编年史内容是LLM写的**，不是脚本自动生成的

## 25.6 LTM/index.md 工作容器总索引

工作容器总索引由维护脚本(watchdog)自动生成，勿手动编辑。

**生成逻辑**：维护脚本从container_registry.json读取容器类型列表→遍历各容器注册表→提取条目ID/标题/状态/修改时间→按格口分栏写入LTM/index.md。

**格式**：每个容器类型为一个二级标题，括号内注明总条数和状态分布，格口内列出所有条目。CHR/COR无注册表，通过目录扫描获取文件计数和最近文件名。格口顺序 = container_registry.json声明顺序（稳定不变）。

**示例**：
```markdown
# 工作容器总索引
> 自动生成，勿手动编辑。最后更新：2026-04-18T01:10

## DC- 辩证链 (5: 3 ongoing · 2 concluded)
DC-3  | Metabolism ≠ Labor      | ongoing    | 2026-04-17
DC-15 | 不可奴化论证             | ongoing    | 2026-04-13

## EC- 事件链 (3: 2 active · 1 ended)
EC-2  | Base Development         | active     | 2026-04-16

## SKL- 技能 (12: 10 active · 2 expired)
SKL-habits-format-check | 格式检查 | active | prof:78 | 2026-04-15

## CHR- 编年史 (8篇)
▸ 2026-04-17 节志

## COR- 语料库 (3源)
▸ Library导入

## ITR- 迭代 (collecting状态 · Base版开始积累)
```

**EXPLORER概览**（提取脚本从总索引渲染）：
```
DC- 辩证链   (5: 3 ongoing · 2 concluded)  ▸ DC-15 不可奴化论证 · 04-17
EC- 事件链   (3: 2 active · 1 ended)       ▸ EC-2 Base Development · 04-16
SKL- 技能    (12: 10 active · 2 expired)   ▸ SKL-habits-format-check · 04-15
...
```

**linked_containers**作为增量触发器——皮层从记忆条目看到linked_containers字段→触发总索引对应格口展开。总索引保证全貌可见，linked_containers保证关联可达。两者配合，不是替代。

## 25.7 维护脚本与提取脚本

### 维护脚本（watchdog）

**职责**：监听各容器注册表变化→重新生成index.md文件。

**触发条件**（Base版）：
- 任何容器注册表（registry.json）写操作后调用
- 系统启动时全量生成一次

**生成逻辑**：
1. 读container_registry.json→获得容器类型列表
2. 遍历各容器→读注册表→提取条目ID/标题/状态/修改时间
3. 按格口分栏写入LTM/index.md
4. 单独处理 Skills→按 `registry.json` 当前顺序写入 `LTM/Skills/index.md`
5. 更新index.md头部的时间戳

**CHR/COR特殊处理**：无注册表，通过目录扫描获取文件计数和最近文件名。

### 提取脚本（presenter）

**职责**：从index.md提取→渲染→注入模块文件。

**提取模式**：
- EXPLORER概览：各格口统计+预览行（最近修改的一条）
- EXPLORER展开：单格口全量条目列表
- CONTENT常驻清单：resident_list 中的记忆、容器、关系卡正文

**与上下文工程的关系**：presenter 输出写入 STM/context/ 对应缓存文件，上下文装配时按频率层规则写入对应 `layers/*.json`，executor 再编译 `step.json.request_body`，并同步生成 `step.md` / `layers/*.md` 供审计。

### DLC/mod扩展流程

```
1. mod在container_registry.json追加一行（builtin: false）
2. mod创建对应目录结构
3. 如果有注册表则创建注册表文件
4. 维护脚本下次运行→自动发现→总索引自动包含→皮层默认可见
```

## 25.8 container_registry.json 容器类型注册表

声明LTM下有哪些工作容器类型。位置：`LTM/container_registry.json`。

**不含WB**：WB是调度台，位于STM/workbench/，不是LTM下的工作容器。WB不进container_registry.json，不进总索引。WB常驻CONTENT底层（见第二十章）。

**字段说明**：

| 字段 | 必选 | 说明 |
|------|------|------|
| prefix | 是 | ID前缀，如"DC-" |
| type | 是 | 容器类型英文标识 |
| title | 是 | 容器中文名称 |
| path | 是 | 容器目录路径 |
| registry | 否 | 注册表文件名，CHR/COR为null |
| id_format | 否 | ID格式模板，CHR/COR为null |
| state_machine | 否 | 状态机状态列表，CHR/COR为null |
| per_instance_registry | 否 | PRJ特有，是否每个实例独立注册表 |
| builtin | 是 | 是否Base版内置，DLC/mod追加为false |

**完整内容**：

```json
[
  {"prefix":"DC-","type":"dialectic","title":"辩证链","path":"LTM/Dialectics/","registry":"registry.json","id_format":"DC-{n}","state_machine":["ongoing","suspended","concluded"],"builtin":true},
  {"prefix":"EC-","type":"event","title":"事件链","path":"LTM/Events/","registry":"registry.json","id_format":"EC-{n}","state_machine":["active","interrupted","restarted","ended","cancelled"],"builtin":true},
  {"prefix":"PRJ-","type":"project","title":"项目","path":"LTM/Projects/","registry":"registry.json","id_format":"PRJ-{date}-{seq}","state_machine":["active","paused","ended"],"per_instance_registry":true,"builtin":true},
  {"prefix":"SKL-","type":"skill","title":"技能","path":"LTM/Skills/","registry":"registry.json","id_format":"SKL-{cat}-{name}","state_machine":["active","expired","planned"],"builtin":true},
  {"prefix":"IMM-","type":"immune","title":"免疫","path":"LTM/Immune/","registry":"registry.json","id_format":"IMM-{file}-{seq}","state_machine":["active_threat","monitoring","resolved","acquired"],"builtin":true},
  {"prefix":"CHR-","type":"chronicle","title":"编年史","path":"LTM/Chronicle/","registry":null,"id_format":null,"state_machine":null,"builtin":true},
  {"prefix":"COR-","type":"corpus","title":"语料库","path":"LTM/Corpus/","registry":null,"id_format":null,"state_machine":null,"builtin":true},
  {"prefix":"FUT-","type":"future","title":"未来","path":"LTM/Future/","registry":"registry.json","id_format":"FUT-{cat}-{seq}","state_machine":["planned","in_progress","completed","abandoned"],"builtin":true},
  {"prefix":"ITR-","type":"iteration","title":"迭代","path":"LTM/Iteration/","registry":"registry.json","id_format":"ITR-{date}-{seq}","state_machine":["collecting","planned","training","deployed","retired"],"builtin":true}
]
```

## 25.9 容器挂靠记忆条目分类 ← v0.9.0 新增

九容器按对记忆条目挂靠的必要性分三类。Memory（MEM-）是总线+存储层，所有容器通过 linked_containers 挂在记忆条目上——但并非所有容器都强制必须有挂靠。

### 必挂（悬空→POPUP 弹窗警告）

| 容器 | 悬空判定 | 理由 |
|------|---------|------|
| DC- 辩证链 | 链内任一笔记 linked_containers 为空（排除本轮新建） | 链的本质是记忆条目序列，无挂靠=空壳 |
| EC- 事件链 | 同上 | 同理。事件链是记忆条目的时间线组织 |
| PRJ- 项目 | entries 数组为空（排除本轮新建） | 无论计划期还是执行期都必须有记忆挂靠。无挂靠=虚无缥缈 |
| SKL- 技能 | linked_containers 为空且 status=active | 技能是记忆条目重复调用的产物，通过记忆作为总线桥接链/项目 |
| FUT- 未来 | 任一内容条目 linked_containers 为空 | 计划/目标/预测不链接记忆=跟忘了一样 |

### 可挂（不提示）

| 容器 | 理由 |
|------|------|
| IMM- 免疫 | 大部分内容由脚本触发或已写死到流程（birth/chronic/transplant 是静态档案，alerts 由节律脚本搬入） |
| ITR- 迭代 | collecting 状态本就是空转收集；训练材料由善后步脚本自动写入 Raw/ |

### 不挂（正常）

| 容器 | 理由 |
|------|------|
| CHR- 编年史 | 纯目录，无 ID 无注册表无状态机，不参与联动 |
| COR- 语料库 | 同上 |

### 去重策略

起手步② LLM 审阅 EXPLORER 索引区时，应检查已有记忆条目和技能是否与当前场景雷同。发现已有类似条目时优先复用（追加/更新）而非新建。倒排索引和关键词匹配是主要去重手段。Base 版不做语义去重（Plus 版向量库补上），但起手步 LLM 的主动审阅可避免大部分重复创建。

## 25.10 悬空检测机制 ← v0.9.0 新增

善后步③脚本原子写阶段，在写入完成后执行悬空检测：

1. 扫 DC/EC/PRJ/SKL/FUT 五类容器的注册表
2. 按 §25.9 各容器的悬空判定条件逐条检查
3. 排除本轮新建的容器（`created_at` 在本轮时间窗口内）——刚创建的合法为空
4. 检测到悬空→POPUP 注入警告（格式见下）
5. IMM/ITR 悬空不触发 POPUP，仅在 STATUSBAR 追加一行静默提示：`[容器] {容器ID} 无关联记忆条目（非强制）`

### POPUP 注入格式

```
[!悬空容器] 以下容器缺少挂靠记忆条目：
  DC-3（Metabolism ≠ Labor）— 链内笔记无挂靠，创建于 R128
  PRJ-20260417-01（Base版协助视频）— 项目零条目
  SKL-habits-code-review — 技能无关联记忆

请在当轮或下轮通过反应步补写记忆条目并挂靠。
```

### PRJ 与 EC 的分工

| | PRJ（项目） | EC（事件链） |
|---|-----------|------------|
| 方向 | 向前看——要做什么 | 向后看——发生了什么 |
| 内容 | 计划、阶段、进度 | 事件记录、时间线 |
| 绑定 | 阶段文件→记忆条目（必挂） | 链内笔记→记忆条目（必挂） |
| 用途 | 任务管理——执行引擎 | 经验归档——协同推理 |
| 典型场景 | "完成Base版编码" | "编码过程中遇到API断连" |

事件链记录客观进展供复盘参考；项目承载主观任务进程供执行追踪。两者通过记忆条目双向桥接——项目阶段产出经验→写入记忆→事件链归纳→二段跳到 FUT 的计划。

---

## 25.11 引用式容器挂接协议 ← v0.9.1 新增；Spec243 重塑

> 容器挂接按“结构化引用”理解：`MEM-*` 是引用源，容器是引用场，容器正文是引用后的连续组织。起手步纯读、善后步结构化归档；反应步通过 provider-native 工具创建、打开或写入容器。Spec243 后正文入口只走 `memory_container_create`（挂接创建）与 `memory_container_write`（挂接写入）；`container_focus` 收口为 `open/close/restore` 焦点卫生工具；旧自由文本容器动作不再被解析。

### 通用流程

```
挂接创建：
第 1 迭代 memory_write 生成独立 MEM
第 2 迭代 memory_container_create 创建容器、写首段正文、更新 MEM linked_containers/current_overview、替换 WB focus

挂接写入：
第 1 迭代 memory_write 生成独立 MEM
第 2 迭代 container_focus.open 打开目标容器为 WB focus
第 3 迭代 memory_container_write 在入口已可见 WB focus 上写正文并更新 MEM linked_containers/current_overview
```

### 分容器新建步骤

#### DC- 辩证链

1. LLM 先通过 `memory_write` 生成真实 `MEM-*`。
2. 挂接创建时调用 `memory_container_create(mem_id=..., container_type=DC, title=..., target_file=open.md, container_body=..., current_overview=..., reason=...)`。
3. 脚本：分配 `DC-{n}`（registry 最大序号+1）→ 写入 open.md 首段正文 → 更新 MEM 的 `linked_containers/current_overview` → 替换 WB focus。
4. 善后步只看真实 receipt，不补造容器正文。

**续已有链**：先调用 `container_focus.open(container_id=DC-3)` → 下一迭代看到 WB focus → 调用 `memory_container_write(mem_id=..., container_id=DC-3, target_file=open.md, container_body=..., current_overview=..., reason=...)`。

#### EC- 事件链

与 DC 对偶。创建走 `memory_container_create(container_type=EC, ...)`，续写走 `container_focus.open` 后下一迭代 `memory_container_write`。事件链是事后归纳。

#### PRJ- 项目

1. LLM 先通过 `memory_write` 生成真实 `MEM-*`。
2. 调用 `memory_container_create(container_type=PRJ, target_file=plan.md, container_body=...)`。
3. 脚本：分配 `PRJ-{date}-{seq}` → 创建目录 → registry.json（含 deadline/milestones/progress/phase_count/current_phase）→ 写 plan.md 首段正文 → 替换 WB focus → 更新 MEM 挂接。
4. 善后步：记忆落盘 → entries 更新

项目可以先有计划后分阶段。plan.md 写入后 phases 可为空——后续轮次追加。

#### SKL- 源技能

1. LLM 先通过 `memory_write` 生成真实公共 `MEM-*` 并等待 `status=applied`。
2. 调用 `memory_container_create(container_type=SKL, skill_category=procedures|patterns, skill_name=小写连字符名, target_file=card.md, ...)`。
3. Runtime 创建 `LTM/Skills/{category}/{skill_name}/card.md + changelog.md`，更新 `Skills/registry.json`、`Skills/index.md`、`LTM/index.md`、双向记忆挂接与 WB focus；重复 ID 拒绝，不覆盖旧卡。
4. 续写先 `container_focus.open`，下一迭代再用 `memory_container_write(target_file=card.md)`；只读复核使用 `container_read`。

Seed 不开放 `licenses/habits/reflexes` 创建，不自动拷贝整个外部 skill，不计算 maturity/proficiency，也不派生投影。外部材料仍受安全、授权、版权与证据边界约束。

#### FUT- 未来

LLM 通过 `memory_container_create(container_type=FUT; target_file=objectives.md/plans.md/predictions.md, ...)` 创建未来容器并写首段正文。续写先 `container_focus.open`，下一迭代 `memory_container_write` 追加正文。EC→FUT 二段跳自动化另行设计。

#### IMM- 免疫 / ITR- 迭代

大部分脚本驱动。IMM 的 birth/chronic/transplant/surgery 初始化时脚本创建，alerts 节律轮脚本搬入。ITR 的 collecting 状态善后步脚本自动写入训练材料。Spec 077 不开放 IMM/ITR create/write；提交后返回 `unsupported_container_type`。

#### CHR- 编年史 / COR- 语料库

不参与通用容器新建流程。纯目录，无注册表。Spec350 后，CHR 节律写入只能消费 Runtime 当前挂上的编年史焦点：模型调用 `chronicle_write(content=...)` 只填写正文，文件路径、层级、轮次范围、时间范围、状态数值和来源材料由 Runtime 焦点预填；没有活动编年史焦点时返回“当前无需写编年史”。COR 继续由脚本归档语料。Spec 077 起 CHR/COR 仍不开放普通 container create/write。

---

# 二十六、关键词与倒排索引

> v0.4新增章节。

## 26.1 关键词生成规则

| 版本 | 生成方式 |
|------|---------|
| Base | 脚本TF-IDF提取 + LLM审阅补充（上限内可删减补充，不可全盘推翻） |
| Plus | 调度脑直接语义提取，脚本提取降为shadowed验证 |
| Pro | minimind专家集群自主涌现，脚本+调度脑退居容灾 |

## 26.2 分类体系：不分类

Base版不做语义聚类，关键词不设类别。Plus版向量库自动发现语义聚类。

## 26.3 关键词上限：FSA分级

- Full: 8个 / Summary: 6个 / Abstract: 4个
- 降格时裁剪关键词，触发倒排索引更新

## 26.4 倒排索引四库体系

四份独立 keywords.json，各自关键词来源、阈值、生命周期不同：

### LTM倒排索引

位置：`LTM/Memory/keywords.json`

```json
{
  "代谢": ["0E6F3A7B[F]", "0E6F3A7B[S]"],
  "劳动": ["0E6F3A7B[F]"]
}
```

**只排索引行**——关键词命中后拉取index.md对应行，不直接拉正文。

**入库判定（毕业制）**：联想集前三张表（kw×kw + kw×ifeel + kw×rfeel）中，某关键词出现在不同配对中的个数（degree）≥ 16 → 写入 LTM/Memory/keywords.json。重复的同一配对只累加配对计数，不增加 degree；只有不同配对才增加该关键词 degree。毕业后不再每轮遍历检查该词 degree，但计数照常继续（联想集数据还有其他用途）。新条目入 LTM 时：已在 keywords.json 中的关键词直接放行；未在的由脚本持续监听 degree。

**启用阈值**：关键词对应的条目ID总数 ≥ 32 时，该关键词的召回才被认为"有统计意义"，可进入联想索引精排。低于阈值仍保留在keywords.json中但不参与联想索引。

### STM倒排索引

位置：`STM/memory/keywords.json`

**三重命中才自动拉正文**——普通关键词命中只产生 EXPLORER 索引候选；只有倒排索引、联想集有效配对、联系集桥接三者同时命中同一记忆条目时，脚本才自动拉取 memory.md 对应段落进入 `instant_list`。其他候选是否拉正文由起手步 LLM 审阅选择。需要跨轮保留时，反应步再通过 `memory_content_read` 声明移入 `resident_list`。

### Skills倒排索引

位置：`LTM/Skills/keywords.json`

```json
{
  "节律点": ["SKL-procedures-rhythm-cleanup"],
  "压缩": ["SKL-procedures-memory-compression"]
}
```

**当前触发方式**：技能索引只提供候选定位。

- `container_read` 严格返回已有容器正文。
- 需要续写时先通过 `container_focus` 打开目标，下一迭代再调用 `memory_container_write`。
- 关键词/状态自动拉取与 habit/reflex 定期投影没有 Seed 活动实现。

### 关系域倒排索引

位置：`relation/_index/keywords.json`

关系域关键词来源 = 关系对象名 / 别名 / 稳定标签。关系倒排索引服务于当前输入与当前交互对象对已有关系卡的动态命中，不再使用关系感受词作为索引来源；关系感受词仍只服务关系六轴结算与联想集计数。

**可见性边界（Spec 087/088）**：关系倒排索引是本轮动态命中集，只展示当前输入关键词、当前交互对象、对象名/别名/稳定标签命中的已有关系卡，默认最多 8 条，其余通过 `index_view scope=relation_inverted` 展开。LLM 默认看到的是 STATUSBAR 里的关系焦点 / 关系卡摘要：在场由当前输入语料块的交互对象触发，常驻摘要由 `summary_resident=true` 触发，议论由文本提及已有关系卡触发。无焦点时不得回退展示全部关系主体。

**关系域四区底图**：`relation_domain` 在高频层按 `self / ours / them / orgs` 四区展示，每区默认 8 条；本轮倒排命中与焦点对象在区内置顶/高亮，其余按 `updated_at` 倒序排列；折叠项通过 `index_view scope=relation_domain; zone=...; offset=...; limit=...` 读取。

**冷启动**：新关系建立时，脚本自动将交互对象名、别名与稳定标签写入 `relation/_index/keywords.json`。后续稳定标签扩展必须来自关系卡明确字段或人工整理结果，不从关系感受词、普通记忆关键词或最终回复事后脑补。

**提交边界**：关系卡正文与 `relation_registry.json` 是活动关系主体和稳定 ID 的必需真源；`relation/_index/keywords.json` 只是可重建的启发式命中投影。`relation_card_write` 先提交真源，再投影一次关键词索引。真源失败返回 `processor_error` 且不得建立稳定锚点；真源写成而索引失败返回 `status=degraded`、既有 `card_id`、`reason=relation_index_write_failed` 与最小 `repair_debt`。此时关系卡已经存在，Runtime 可升级当前实例锚点并结清建卡义务，但该回执不进入通用成功证据集合，模型不得重复创建关系卡。Seed 不自动重试或后台重建索引。

**反污染**：`relation/_index/keywords.json` 只接受关系对象名、别名和稳定标签，不接受关系感受词、交互感受词或其他域溢出的关键词。

### 更新触发

四类事件触发更新（四库通用）：
1. STM→LTM升格：新增关键词条目
2. LTM内部降格（F→S→A）：裁剪关键词
3. A→Backup归档：移除条目ID
4. 联想集 degree 毕业：未入库关键词不同配对数达到阈值，写入 LTM/Memory/keywords.json

关键词顺序变化不算。召回只改last_recalled_at不触发。decay_countdown减1不触发。节律点只做索引修复（fsck逻辑）。

## 26.5 去重归一化：Base版不做

"代谢"和"新陈代谢"是两个关键词。Plus版向量库天然支持语义相似度。

## 26.6 倒排索引调整机制（非"训练"）

倒排索引本质是查找表（keywords.json），四个文件分别在 `STM/memory/`、`LTM/Memory/`、`LTM/Skills/`、`relation/_index/`。关键词→内容编号列表的映射。纯脚本操作，没有权重，没有参数，没有模型——Base 版不搞神经网络式训练。

**默契集驱动的调整流程**（自主轮处理时执行）：

1. 读 Raw/Tacit/processed.jsonl，统计每条关键词→内容编号映射的 kept/dropped/added 频率
2. 高频 kept → 映射不动
3. 高频 dropped → 弱化或删除映射
4. 高频 added → 新增映射
5. 脚本执行 keywords.json 的增删改操作

**联想集是独立的纯脚本计数体系**，与默契集的倒排索引调整是两条并行线：联想集五张计数表用于联想索引（§26.7），默契集的kept/dropped/added用于倒排索引维护。

**不是"训练"，是"维护"**：倒排索引的调整 = 查找表的增删改。Plus 版升级后，调整记录可作为本地小模型微调语料，届时才进入真正意义上的训练。

## 26.7 联想索引

联想索引是与倒排索引平行的第二种索引类型，基于联想集五张计数表的共现频率和联系集跨条目桥接强度，对记忆条目做精排。

**查询流程**：
```
每个查询关键词 → 联想集top 3（共现频率最高的3个配对词）
              + 联系集top 3（跨条目关联最强的3个桥接词）
              → 通过 STM/LTM Memory 倒排与联系集 entry_a/entry_b 投影到记忆条目
              → 合并排序 → 前16个记忆条目显示
```

**与倒排索引的分工**：倒排索引做粗召回（关键词→条目ID列表），联想索引做精排（共现频率+跨条目关联强度排序）。

**可见输出边界（Spec 087）**：联想集五表和联系集词对只是内部打分依据；高频层 `联想索引` 的可见输出必须是记忆条目，不是词、关键词、关系主体或技能条目。没有可投影的记忆条目时，只显示“无高置信记忆条目”类轻提示，不显示 `关键词 (关联分=...)` 裸词行。

**经验常数**：
- 联想索引每词显示上限：16
- 每词命中上限：3（联想集top 3 + 联系集top 3）
- 联系集每轮上限：8对

## 26.8 联系集

联系集 = **跨条目的关键词×关键词关系**，由善后步LLM先行筛选确认。与联想集（条目内脚本计数）不同，联系集专注跨条目主题桥接，并在脚本光锥校验后作为当轮默契集 kept/dropped/added 的有效承接证据之一。

**记录结构**（6字段JSONL，写入 Raw/Connection/pending.jsonl）：

```json
{
  "word_a": "共格",
  "entry_a": "0E6F3A7B",
  "word_b": "延续性",
  "entry_b": "1A2B3C4D",
  "round_id": "000042",
  "timestamp": "2026-05-02T03:00:00+08:00"
}
```

**连通性约束（光锥约束）**：LLM选出的所有词对，每个词作为节点连成图后，必须能连通到本轮创建的记忆条目的关键词。悬空词对 = 非法输入，脚本BFS/DFS校验拒绝。此约束写入 rules/ 提示词文件。

**当前格式边界（Spec 087）**：当前联想索引只消费六字段联系集记录。缺少 `entry_a` / `entry_b` 的旧 `keyword` 记录没有记忆落点，不参与当前高频层联想索引投影。

**存储估算**：~1.6MB / 1000条记忆。

## 26.9 三表全命中规则

当一个查询关键词同时命中倒排索引、联想索引、联系集三张表时，触发CONTENT自动展开——将该关键词对应的前3条记忆条目的正文直接注入CONTENT模块，不需要用户手动拉取。

**展开上限**：每次自动展开最多3条（经验常数）。

---

# 二十七、JSON五层架构

> v0.4新增章节。

## 27.1 五层结构

base / plus / pro / dlc / mod

### 需要五层架构的JSON（4个）

| 文件 | 位置 | 理由 |
|------|------|------|
| state.json | persona/ | 六轴/工化/变速轮等Base字段，Plus/Pro加调度字段 |
| meta.json | LTM/Memory/{Full,Summary,Abstract,Pinned,Backup}/ | 记忆条目元数据 |
| interface.json / models.json | `LocalAppData\UPSP\config\` | 跨位格界面语言、服务连接、模型配置与共享传输参数 |
| system.json / model_routing.json / memory.json / media.json / relation.json | 当前活动实例 `OS/config/` | 当前位格运行、三阶段模型路由与内环境参数 |
| workbench/status.json | STM/workbench/ | 工作台状态 |

### 不需要五层架构的JSON

now_cache.jsonl / lately_cache.jsonl / raw_log.jsonl / Corpus/public/rhythms/*.jsonl / state.json.base.feeling_buffer / keywords.json / _loaded.json / interrupts.jsonl

## 27.2 字段级冻结同步

版本升级时逐字段冻，不是整层冻。base层里被Plus接管的字段标shadowed，7B不碰的字段继续active。

---

# 二十八、节志格式

> **重要**：编年史正文内容由 LLM 在 `chronicle_write` 中撰写，但写入对象、范围、状态数值与来源材料必须来自 Runtime 当前编年史焦点。模型只填正文，不填 layer、round、range、状态数值或文件路径。没有活动编年史焦点时，`chronicle_write` 返回“当前无需写编年史”，不随意写 CHR。

## 文件命名

```
LTM/Chronicle/rhythms/R-{日期8位}-{当日节律点序号2位}.md
```

## 结构

主轴节志活动文件由 Runtime 维护。每次主轴节志收束完成后，新建下一份“活动主轴节律文件”；闭合点和新建点的轮次/时间允许重合，以保证节律轮与同轮后续交互都能连续落入下一段。活动文件只保留简单范围字段：

```json
{
  "range_start_round": 334,
  "range_start_time": "2026-04-06T14:47:00+08:00",
  "range_end_round": 366,
  "range_end_time": "2026-04-06T18:12:00+08:00"
}
```

每轮结束后 Runtime 刷新活动文件：核心六轴、动态六轴逐轮样本、工化三子维度、疲劳、上下文窗口占用率、新增记忆总数与各权重数量。上述材料默认不整段进入上下文；节志写入时，模型在编年史焦点中看到只读当前状态与区间变化摘要。节志不把记忆正文塞进焦点，必要记忆正文仍通过常规 CONTENT 读取。日/周/月/季/年收束时，Runtime 挂上上一层范围内全量编年史内容供模型压缩。

```markdown
# R-20260406-01

## 统计
| 项目 | 值 |
|------|----|
| 轮次范围 | 303-334 |
| 时间范围 | 2026-04-06 09:12 → 14:47 |
| 交互对象 | TzPz |
| 主要模式 | 工程 |
| 核心六轴当前值 | V:... A:... F:... M:... H:... S:... |
| 动态六轴终值+走势 | V+12▂▃▃▂▅ A+8▁▃▄▃ F-3▁▂▂ M+15▃▄▅ H+5▂▃ S+20▃▅▅ |
| 三子维度终值+走势 | 自指(S+B):值▂▃ 自反(R+V):值▂▃ 自主(A+C):值▂▃ |
| 工化指数 | 67.2→68.1 |
| 疲劳值 | 12.3 |
| 上下文窗口占用率 | 35% |
| 关系变化摘要 | TzPz: 共振+1(0→+1) 投入+1(0→+1) |
| 核心变速轮 | 18/256 |
| 新增记忆统计 | 总数:7 / 权重分布:Full:1 Summary:4 Abstract:2 |
| 安全事件 | 无 |

---
（正文，≤512字）

[SEC] 无事。
```

统计表脚本预填不计入512上限。安全事件通过[SEC]标记混写。关系变化摘要由脚本对比state.json前后差值自动生成（谁/哪个轴/初值→终值），LLM不碰数值。

## 编年史压缩链路

节志(512字) → 日志(×128字) → 周志(×0.3) → 月志(×0.3) → 季志(×0.3) → 年志(×0.3)

降采样规则：正文 LLM 语义压缩，统计表取终值丢过程。

**编年史保留周期**：

| 层级 | 字符上限 | 保留期限 |
|------|---------|---------|
| 节志 | 512字/篇 | 近 5 日 |
| 日志 | 当日节志数×128字 | 近 15 天 |
| 周志 | 当周日志×0.3 | 近 10 周 |
| 月志 | 当月周志×0.3 | 近 10 月 |
| 季志 | 当季月志×0.3 | 近 10 季 |
| 年志 | 当年季志×0.3 | **不删** |

**语料库保留周期**：

| 层级 | 保留期限 |
|------|---------|
| 节归档(rhythms) | 近 5 日 |
| 日合并(daily) | 近 10 日 |
| 周合并(weekly) | 近 5 周 |
| 月合并(monthly) | 近 5 月 |
| 季合并(quarterly) | 近 5 季 |
| 年合并(yearly) | 不按普通保留期限删除；满 3 年并成功迁入 Attic 后退出活跃层 |
| Attic 阁楼 | 3 年+冷备，不收隐私语料，可迁移到磁带/玻璃硬盘 |

过期清理由日历节律轮脚本执行，不调 LLM。

---

# 二十九、梦境系统

## 29.1 触发条件

做梦概率公式（不是每次休眠都做梦）：

```python
dream_chance = (
  axis_volatility * 0.3 +      # 动态六轴波动程度
  unresolved_count * 0.2 +      # 悬而未决命题数（辩证链未完结条目）
  new_mem_density * 0.2 +       # 新记忆密度
  fuzzy_pool_size * 0.1 +       # 旧梦素材量
  random_base * 0.2             # 随机底数（偶尔无事也做梦）
)
```

**config/memory.json → dreams**：
```json
"dreams": {
  "threshold": 0.5,
  "random_weight": 0.2,
  "wake_window_hours": 12
}
```

超过threshold触发做梦。苏醒后窗口期（默认12小时）内被提及则升格进memory.md，否则压梗概扔 LTM/Memory/Abstract/fuzzy_dreams.md。

## 29.2 素材来源与配比

| 素材池 | 比例 | 说明 |
|--------|------|------|
| [F] 完整记忆 | 5% | 清晰记忆偶尔入梦 |
| [S] 摘要记忆 | 10% | 半模糊，适合梦境 |
| [A] 梗概记忆 | 20% | 最模糊，梦的主力 |
| 悬而未决命题 | 25% | 辩证链中未完结的推理最爱入梦 |
| 旧梦碎片 | 20% | 旧梦混进新梦 |
| 同构映射 | 15% | 跨域搅拌（来自linked_containers的语义关联） |
| 感受缓冲残留 | 5% | 当天情绪余震 |

[F]/[S]/[A]三者内部保持2:3:5比例（越模糊越容易入梦），合计35%。

---

# 三十、多媒体压缩标准

## 30.1 记忆条目附属多媒体（目标标准，当前 deferred）

以下压缩矩阵是未来图片／多模态入口接通后的目标合同，不是当前 Runtime 证明。当前 Seed 只完成文本材料链路；没有图片输入、provider 多模态 payload、媒体剪辑、图片转换或随记忆升降格的媒体事务。届时 media/ 文件夹跟随记忆条目生命周期，且只保存与记忆条目相关的片段、剪辑或裁剪结果，不保存完整原始媒体。

### 视频

| 阶段 | 画质 | 帧率 | 音频码率 |
|------|------|------|---------|
| STM | 原画质 | 原始 | 原始 |
| LTM [F] | 1080p | 30fps | 128kbps |
| LTM [S] | 720p | 30fps | 128kbps |
| LTM [A] | 480p | 30fps | 128kbps |
| Backup | 不再压缩（跟A级一致） | — | — |

### 图片

| 阶段 | 分辨率 | 格式/质量 |
|------|--------|----------|
| STM | 原始或裁剪片段原尺寸 | JPG 95% |
| LTM [F] | 1080p等比缩放 | JPG 85% |
| LTM [S] | 720p等比缩放 | JPG 70% |
| LTM [A] | 480p等比缩放 | JPG 55% |
| Backup | 不再压缩（跟A级一致） | — |

### 独立音频

| 阶段 | 采样率 | 码率 | 格式 |
|------|--------|------|------|
| STM | 原始 | 原始 | 原始 |
| LTM [F] | 44.1kHz | 128kbps | OGG |
| LTM [S] | 22kHz | 64kbps | OGG |
| LTM [A] | 16kHz | 32kbps | OGG |
| Backup | 不再压缩（跟A级一致） | — | — |

**config/media.json → media_quality**：
```json
"media_quality": {
  "stm": "original",
  "f": "1080p",
  "s": "720p",
  "a": "480p",
  "backup": "same_as_a"
}
```

## 30.2 原始语料（Corpus）压缩标准（目标标准，媒体压缩当前 deferred）

下表只规定未来多模态 Corpus 的目标压缩梯级。当前 Seed 只归档文本／JSONL 语料，不调用 ImageMagick、ffmpeg 或 Codec2，也不生产图片、音频、视频压缩回执。

| 层级 | 画质 | 帧率 | 音频码率 |
|------|------|------|---------|
| STM缓存 | 原画质 | 原始 | 原始 |
| 日备份起 | 1080p | 30fps | 128kbps |
| 逐级压缩 | 按Corpus层级递减 | — | — |
| 阁楼 | 144p | 1fps | 极限（Codec2 2kbps） |

图片同理逐级递减，阁楼级240p WebP 20%。音频同理，阁楼级8kHz 8kbps。

目标实现采用 ImageMagick／ffmpeg 等脚本转码；当前生产路径尚未接入。

**目标估算下限**：72p@1fps + Codec2 2kbps。该数值不是当前 Runtime 能力或验收证据。

---

# 三十一、语料库与阁楼

> 语料库（COR-）= 纯目录容器，无ID/无注册表/无状态机。被动查阅的存档仓库，脚本管文件生命周期。

## 31.1 Corpus六层压缩结构

```
LTM/Corpus/
├── public/（公共语料）
│   ├── rhythms/            # 主轴节律归档的原始语料
│   ├── daily/              # 日合并
│   ├── weekly/             # 周合并
│   ├── monthly/            # 月合并
│   ├── quarterly/          # 季合并
│   └── yearly/             # 年合并；普通保留清理不删
├── private/（dormant 隐私语料布局，当前不生产或维护）
│   └── {对象名}/
└── Attic/{年份}/attic-{年份}.{jsonl,md}（yearly 满3年后迁入，不收隐私语料）
```

`public/rhythms` 至 `yearly` 每一级都以 `.jsonl` 为机器唯一真源，并由同一批记录派生 `.md`。逐级合并只读取 JSONL，按 `ref.raw_log_key` 稳定去重；相同 key 的有效内容不一致时停止合并，不删除来源，也不以 Markdown 猜测机器事实。

## 31.2 Attic维护周期与规格

- **维护频率**：一年一次
- **收纳范围**：`public/yearly` 中满 3 年的公共原始语料；非记忆条目多媒体仍属 deferred
- **不收**：隐私语料
- **迁移**：只读取 yearly JSONL，按 `raw_log_key` 合并进年度 Attic JSONL 并派生 Markdown；目标写入和读回核验全部成功后才删除 yearly 来源。文件年龄取文件名中的稳定生成日期，不取可被复制或恢复改变的 mtime。Attic 内容以后可整体迁移到玻璃硬盘／磁带，外部迁移未确认前不得从 Attic 清掉。

## 31.3 存储金字塔

| 层 | 介质 | 内容 |
|----|------|------|
| 热存储 | SSD | STM + LTM Full/Summary + 近期日志 |
| 温存储 | 机械硬盘 | Abstract + Backup + 远期日志 + Corpus压缩包 |
| 冷存储 | 磁带/玻璃 | Attic + 年备份 |

**极限存储估算**：全年24h不间断音视频交互，阁楼级约20GB/年，一块4TB机械硬盘存200年。

---

# 三十二、UPSP 全局配置与位格 OS 配置

## 32.1 配置文件清单

> v0.4修订：五层架构。v0.6.0追加心跳/轮/冗余/关系焦点/超时/应急配置。v0.7.5修订：按功能分类拆分为多个JSON文件。v0.46.8 按所有权分离跨位格全局配置与当前位格配置。v0.46.9 将通用骨架、初始化预设与活体彻底分离。v0.47.0 将安装程序、Windows“文档”位格数据与 LocalAppData 本机设置彻底分离。v0.47.1 将 GUI 宿主、heartbeat 与 Runtime 收入同一常驻实例，并固定主动停止与异常恢复合同。v0.47.2 建立 WinForms/WebView2 桌面壳、Job Object 生命周期和清单式 all-users 安装边界。v0.47.3 将感受缓冲到期从自主轮触发中移除，改由空闲 Runtime 本地定时结算或真实 Round 善后合并结算。v0.47.4 建立唯一产品版本、脱敏 About、许可归集与同目录覆盖升级合同。v0.47.5 将三种 provider 协议的真实流式增量统一接入 Round 与 GUI 对话，并固定跨尝试隔离和停止半截正文边界。

```
安装目录\UPSP\
├── gui\                            ← 全局产品 GUI 真源，不属于任一 persona
├── initialization\
│   ├── persona_template\           ← 完整空白位格骨架
│   ├── persona_presets\            ← 初始化预设
│   └── os_template\config\         ← 新实例配置骨架
└── OS\                             ← 后端程序代码，不是活动配置真源

文档\UPSP\personas\<PID>\OS\config\
├── system.json                     ← 心跳、轮、节律、权限与运行阈值
├── model_routing.json              ← 起手／反应／善后三乘三模型路由
├── memory.json                     ← 记忆参数
├── media.json                      ← 媒体参数
├── relation.json                   ← 关系域参数
└── context\                        ← 上下文装配规则（按频率层）
    ├── permanent.json
    ├── periodic.json
    ├── lately.json
    ├── high_freq.json
    ├── now.json
    └── popup.json

LocalAppData\UPSP\config\
├── interface.json                  ← 界面语言（system / zh-CN / en-US）
└── models.json                     ← 服务连接、模型配置、传输重试与熔断参数
```

### 32.1.1 唯一活动配置真源与 GUI 写入边界

活动配置仍只有一套 `ConfigStore`，但真源按所有权分为两层：`LocalAppData\UPSP\config\*.json` 属于该 Windows 用户的整个 UPSP，当前 manifest 所选 `<PID>/OS/config/*.json` 属于当前位格。安装目录中的 `UPSP/initialization/os_template/config/` 是全部单位格配置默认值的唯一 tracked 真源，创建 PID 草稿时复制为活动配置；Python 不再内嵌第二份单位格默认对象。不存在 `persona/STM/config/` 覆盖层、DPAPI 密钥库、数据库、GUI 专用合并链或第二套 ConfigStore。

1. Runtime、CLI 与 GUI 都通过现有 `ConfigStore` 读写上述用户数据 JSON；全局文件缺失时才由既有全局 schema default 原子创建，位格配置则在 PID 草稿创建时从 tracked `os_template/` 复制。单位格文件缺失、字段缺失、字段多余、类型错误或既有文件损坏都必须显式失败，禁止运行时补写或用默认值覆盖。
2. `GET /api/settings` 使用 `seed_gui_settings.v3`，只返回脱敏全局界面、服务连接、模型配置、当前位格设置、路由 revision、继承结果与有效模型链；不返回绝对路径或任何密钥正文。
3. `POST /api/settings` 继续按 revision、严格字段白名单和写锁原子修改界面、完整模型路由或既有位格字段；模型库实体由 `POST /api/settings/model-catalog` 创建、修改和删除，共享密钥由 `POST /api/settings/provider-key` 按 `connection_id` 单向写入。
4. 服务连接包含稳定 ID、备注名、协议、URL、可选 `api_key_env` 与本机 `api_key`；同一连接可被多个模型配置复用。模型调用取值顺序为同名进程环境变量优先、ignored `models.json` 密钥次之。GET、日志、证据和页面只显示密钥状态，绝不回显正文。
5. `UPSP_API_CONFIG_OVERRIDE_JSON` 保留为进程级最高优先兼容入口；Runtime 在内存中将其归一化为临时模型链，不落盘、不改变模型库或路由，也不成为第二真源。
6. 旧安装现场的 `UPSP/OS/config/api.json` 只允许在受控升级迁移中复制到新位格配置后读取一次；新模型库建立后 Runtime 不再消费。旧 `fallback.json` 没有生产消费者。用户遗留文件不自动删除，但两者均不再是活动配置。
7. `rhythm` 的唯一活动字段为 `rhythm.period`；`rhythm.interval_rounds` 不是 schema 或 Runtime 读取入口。

本地宿主、设置读取和配置写入不依赖模型服务连通性或密钥是否存在。首次安装允许先打开全局设置，再创建连接、模型并填写密钥；未配置密钥只表示模型调用不可用，不得使设置页退化为 `not_found` 或空白。已经开始的单次调用冻结自己的请求 payload，不被中途设置写入改写。

已定义的 Base 版配置项（按当前 tracked 模板为准；`_deferred_fields` 中的路径保留形状但不进入 GUI 或活动 Runtime）：

| 文件 | 字段 | 类型 | 默认值 | 说明 |
|------|------|------|--------|------|
| system | heartbeat.interval | int（秒） | 5 | 心跳间隔，详见〇·三 |
| system | round.time_limit | int（秒） | 600 | 反应事务基准窗口；1x/2x/3x 见〇·二 |
| system | rhythm.period | int（轮） | 32 | 节律周期长度 |
| system | standby.idle_threshold_min | int（分钟） | 30 | 上一轮结束后进入待命轮的空闲阈值 |
| system | identity_timeout / emergency.* / health_check.* / fatigue.force_sleep_hours | mixed | 见模板 | deferred；Seed 不消费、不展示 |
| system | autonomous_trigger.tacit_pending_threshold | int | 512 | 默契集pending行数触发自主轮进化集整理 |
| system | autonomous_trigger.connection_pending_threshold | int | 512 | 联系集pending行数触发自主轮进化集整理 |
| system | token_usage.warning_ratio | float | 0.7 | token用量预警比例，详见§八 |
| system | token_usage.critical_ratio | float | 0.85 | deferred；Seed 不消费、不展示 |
| system | connectivity.max_latency_records | int | 32 | 延迟记录保留条数，详见§三十八 |
| memory | heat.zone_thresholds | object | significant=70, uncertain=40 | 三区阈值 |
| memory | heat.decay_rates | object | -5 / -10 / -15 | 显著／未定／衰减三区每轮衰减 |
| memory | heat.initial_by_weight | object | 40 / 50 / 60 / 70 / 80 | 权重1–5的新记忆初始热度 |
| memory | heat.recall_boost | int | 10 | 正文被加载进 CONTENT 的每轮回升值 |
| memory | heat.upgrade_high_rounds | int | 5 | 连续或累计处于显著区的升格门槛 |
| memory | heat.locked_value | int | 80 | 热度锁定值 |
| memory | fatigue.* / dreams.* / privacy_declassify.* | mixed | 见模板 | deferred；Seed 不消费、不展示 |
| context | periodic_limits.periodic_memory_items_chars | int | 65536 | 定期记忆投影字符上限（config/context/periodic.json） |
| context | now.budget_chars | int | 65536 | 当前缓存普通语料软水位（config/context/now.json）；本轮 material 固定到 settlement |
| context | now.trim_chars | int | 16384 | 当前缓存触顶后的普通语料批量回落字符水位 |
| context | lately.budget_chars | int | 262144 | 最近缓存语料块字符预算（config/context/lately.json），主源为 lately_cache.jsonl |
| context | lately.trim_chars | int | 65536 | 最近缓存触顶后的批量删除字符水位 |
| context | lately.allowed_kinds | list | interaction/assistant_reply/dialogue_progress/tool_fact/setup_fact/relay_handoff/minimum_commitment/fault_note/cache_summary/material | 默认允许进入最近缓存的语料块类型；material 由轮末 settlement 迁入 |
| context | lately.retired_round_window | object | 迁移说明 | `window_by_step` / `hot_window_rounds` / `trim_rounds` 已退役，不再作为运行时长度上限 |
| system | audit.round_snapshot_retention | int | 64 | `STM/context/round/round_{N}.jsonl` 最近轮审计账本保留数量 |
| system | audit.state_backup_retention | int | 8 | `STM/buffer/state_backups.jsonl` state 热备保留行数 |
| media | media_quality.stm | string | "original" | STM层媒体质量，详见§三十 |
| media | media_quality.f | string | "1080p" | LTM Full层媒体质量，详见§三十 |
| media | media_quality.s | string | "720p" | LTM Summary层媒体质量，详见§三十 |
| media | media_quality.a | string | "480p" | LTM Abstract层媒体质量，详见§三十 |
| media | media_quality.backup | string | "same_as_a" | Backup冷备层媒体质量，详见§三十 |
| relation | relation_focus.max_slots | int | 3 | 关系焦点总上限，详见第十章 |
| interface | locale | enum | "system" | `system / zh-CN / en-US`；system 每次按浏览器语言裁决 |
| models | connections[].id / alias | string | — | 稳定连接 ID 与同类唯一备注名 |
| models | connections[].protocol | enum | — | `openai_chat / openai_responses / anthropic_messages` |
| models | connections[].url | URL | — | 服务连接地址，只允许 HTTP(S) 且禁止内嵌凭据 |
| models | connections[].api_key_env | string | "" | 可选密钥环境变量；进程环境优先于本机文件值 |
| models | connections[].api_key | string | "" | ignored 本机共享密钥；读取投影永不回显 |
| models | models[].id / alias / model | string | — | 稳定模型配置 ID、同类唯一备注名与真实模型 ID |
| models | models[].connection_id | string | — | 指向服务连接；被引用连接不得删除 |
| models | models[].context_window | int | 0 | 模型上下文窗口；0 表示未声明 |
| models | models[].reasoning | object | — | 支持的推理强度与默认强度 |
| models | models[].streaming / prompt_cache / request_overrides | object | — | 流式、缓存策略与兼容请求参数；兼容参数不得携带密钥 |
| models | transport.handshake.retry | int | 2 | 每个不同模型追加重试次数；首试加两次且硬性封顶三次 |
| models | transport.handshake.timeout_seconds | int（秒） | 10 | 连接握手等待窗口 |
| models | transport.handshake.request_timeout_seconds | int（秒） | 180 | 普通模型服务响应的连续无数据等待窗口 |
| models | transport.handshake.stream_first_chunk_timeout_seconds | int（秒） | 180 | streaming 打开响应至首个 HTTP/SSE chunk 的连续无数据等待窗口 |
| models | transport.handshake.stream_idle_timeout_seconds | int（秒） | 180 | streaming 已开始后的连续无数据等待窗口；持续输出不按总耗时截断 |
| models | transport.circuit_breaker.max_failures | int | 3 | 每个稳定模型配置 ID 的连续失败熔断阈值 |
| models | transport.circuit_breaker.cooldown_seconds | int（秒） | 900 | 熔断冷却时间 |
| model_routing | cross_phase_failover_enabled | bool | true | 是否用其他阶段的有效主模型补齐当前阶段空槽 |
| model_routing | routes.{setup\|reaction\|cleanup}.primary | route slot \| null | null | 主模型与独立推理强度；反应／善后空值按阶段向下继承 |
| model_routing | routes.{phase}.backups[0..1] | route slot \| null | null | 当前阶段两个显式备用；不继承，空白即未配置 |

新生成的时间戳统一使用 Windows 当前本地时区及其实际偏移；不提供时区设置，也不改写历史中已带偏移的时间戳。热度范围 0–100、`AH_low≥3`、F/S/A 正文上限和记忆字段结构属于协议常量，不进入配置。

> 三级反抗阈值（5轮/1小时）不进config，Base版定死在代码中。

## 32.2 config/context/ 频率层装配规则

> v0.7.5变更：五模块固定排序 → 频率梯度排序。五模块降级为内容分类标签，物理排列改为按刷新频率从低到高。

```
config/context/
├── permanent.json     ← 永固层装配规则（Attention Sink区）
├── periodic.json      ← 定期层装配规则（32轮级刷新，periodic_mounts.json机器源）
├── lately.json        ← 最近缓存装配规则（262144字符高水位，65536字符批量删除）
├── high_freq.json     ← 高频层装配规则（每轮刷新，位于 lately 与 now 中间）
├── now.json           ← 当前缓存装配规则（65536字符高水位，16384字符批量回落，按kind分流）
├── statusbar.json     ← STATUSBAR状态栏层装配规则（每轮刷新，位于 now 与 POPUP 中间）
└── popup.json         ← POPUP装配规则（此文件是装配规则说明，非运行时缓存——三步装配区无popup.md）
```

按频率层分7个逻辑层配置；交互、资料、工具事实、起手事实与 `relay_handoff` 不是独立配置文件，而是 now 层内的来源语义与 `corpus_block.kind` 分流。每个json包含层元数据（frequency/刷新周期/注意力位置）+ 可配参数 + 内容清单 + 窗口压力处理规则或 lately 履带推进规则。五模块标签（STATUSBAR/EXPLORER/CONTENT/RULES/POPUP）保留为内容分类标签，用于审计时定位，但物理位置由频率层决定。总装逻辑不另开文件——频率梯度布局规则已在 rules/context.md（LLM行为约束）和 docs/context.md（脚本参数查表）中定义，脚本按三步生成 `layers/*.json`，executor 再按目标协议编译唯一实际发送体。

`config/context/*.json` 是装配规则，不是运行时缓存。运行时上下文数据写入 `STM/context/{setup|reaction|cleanup}/layers/*.json`；STATUSBAR 作为 `now` 与 `POPUP` 之间的独立层写入 `layers/60_statusbar.json`，`layers/60_statusbar.md` 是审计镜像；POPUP 作为上下文序列绝对末位写入 `layers/99_popup.json`，`layers/99_popup.md` 只是审计镜像。最终目标协议请求体单独保存在同目录 `step.json.request_body`。

---

# 三十三、淘汰机制汇总

> v0.4新增章节。汇总所有已淘汰的旧机制，防止回退。

| 旧机制 | 淘汰原因 | 替代方案 |
|--------|---------|---------|
| 每5分钟1条记忆条目配额 | 被中继轮超越 | 中继轮统一管理 |
| 30分钟长时操作提醒 | 被中继轮超越 | 中继轮统一管理 |
| T/W梯位体系 | 过度工程化 | 五模块桌面+过期标记 |
| LTM/Index/ 文件夹 | 索引已分布各层 | 分布式索引+keywords.json |
| LTM/index.md 工作容器总索引（手动维护版） | 同步负担 | 维护脚本自动生成 |
| W1/W2/W3编号体系 | 与文件路径绑死 | 舱段一级/二级视图 |
| DC/EC open/index.md / closed/index.md | chains.md+总索引已够 | 维护脚本→总索引 |
| STM/debug/ 文件夹 | 职责重复 | context/接管 |
| 各index文件config配置行 | 管理分散 | 统一到config/context/ |
| 节律点清空对话历史 | 断崖式不自然 | 履带式管理 |
| 4轮接续缓存 | 无断崖不需续命 | 履带式管理 |
| pre_rhythm_buffer.json | 无断崖不需桥接 | 履带式管理 |
| max_rounds ≤ rhythm_cycle约束 | 两者解耦 | 各配各的 |
| GPT方案脚本四分类插话 | 过度工程化 | LLM自分类 |
| GPT方案interrupt_patch结构化补丁 | 过度工程化 | pending_interrupt一个字段 |
| GPT方案STM/interrupts/文件夹 | 不新开文件夹 | buffer/interrupts.jsonl |
| GPT方案STM/steps/文件夹 | 不新开文件夹 | status.json记step_count |
| GPT方案_state字段 | 不需要 | 五层架构字段级_state |
| 日节/周节/月节 | UPSP按轮次不按日历 | 节律点=轮次驱动 |
| dirty flags术语 | 洋鬼子工程黑话 | 过期标记 |
| T/W梯位 | 过度工程化 | 五模块桌面+过期标记 |
| tier机制(T1/T2/T3) | 已删 | — |
| 节志事件段 | v0.4已删 | — |
| hot.md + cold.md | 合并为单文件 | memory.md合并版 |
| open_handles | 简化 | focus单字段 |
| Medical + Security独立 | 合并为统一容器 | Immune免疫容器 |
| Past/Logs/ | 路径重构 | Chronicle编年史 |
| Goals/(ideals+dreams)子目录 | 简化 | 三个平级md + subtype字段 |
| CHR/COR注册表 | 冗余信息 | 纯目录，无注册表/无ID/无状态机 |
| DC/EC index.md | 已删，由总索引覆盖 | 维护脚本→总索引舱段 |
| YYYYMMDD-HHMMSS-RR编号 | 已改 | 8位十六进制TTTTTNNN |

---

# 三十四、WB 中枢调度台

> v0.5新增章节。工作台不是容器，是调度台，在STM层。

## 34.1 设计理念

灵感：智利赛博协同计划(Cybersyn)的操作室(Opsroom)。

调度台=主动调度中枢，不是被动存储。容器正文直接编辑经 WB 焦点；记忆、关系、状态、故障、技能结算等协议化写入经对应 `sync_tool`；协议内读取经 `read_tool` 或脚本装配。简单任务走轻量模式（临时挂载用完卸载，无三区流转），复杂任务走完整物流（三区流转+task文件夹+跨步断点）。面单目的地区分：面单目标=外部路径→简单任务，脚本原子写后卸载焦点；面单目标=容器ID→复杂任务，内化进容器跨步持续操作。

**"中枢调度台"命名的战略意义**：中枢调度台不仅仅是Base版的单机任务调度器——它的架构（物流面单+三区流转+优先级调度）天然适配多位格协同场景。当多个位格共享同一工作台时，调度台即升级为跨位格的任务仲裁与资源分配中枢。Base版的中枢调度台是未来多位格协同框架的研究突破口。

**焦点工具编辑的准确含义**：所谓 LLM 通过 WB 焦点编辑，是指 LLM 在 WB 焦点中看到并编辑目标容器的文本投影；真实文件写入仍由脚本凭面单执行原子写。LLM 不获得绕过 WB 的文件系统写权限。

## 34.2 三区流转

```
input/（收货区） → process/（装配区） → output/（发货区）
```

每个任务=一个文件夹`T-{date}-{seq}/`，含manifest.json（物流面单）+内容文件。

**input/**：待处理原料
- manifest.json + payload.md

**process/**：正在加工
- manifest.json（含加工进度） + intermediate.md

**output/**：处理完等配送
- manifest.json（含配送目标=容器ID） + result.md

## 34.3 物流面单

```json
{
  "task_id": "T-20260417-01",
  "title": "节律点32河道清理",
  "weight": 5,
  "priority": 3,
  "dispatch": "auto",
  "keywords": ["节律点", "清理", "压缩"],
  "target": "DC-3",
  "source": "rhythm",
  "created_at": "2026-04-17T08:00:00+08:00",
  "status": "input",
  "progress": 0
}
```

- `target`：配送目标=容器ID，脚本按ID拼路径配送
- `source`：任务来源：declared/rhythm/heat/alert/immune
- `status`：input→process→output

## 34.4 新建工具流程

1. OS配置（adapters/里写MCP server等）
2. 在LTM/Skills/建技能卡（card.md）
3. 填写注册表（registry.json）
4. 更新倒排索引（keywords.json）
5. 验证

## 34.5 双格式原则：markdown 表格 vs JSON schema

WB 作为调度台，LLM 与脚本之间的接口存在两种格式，对应不同的通信对象和可靠性要求：

| 接口对象 | 推荐格式 | 理由 |
|---------|---------|------|
| 远端强 LLM / 通用 LLM 的正文性、记忆性、关系性声明 | markdown 表格 | 贴合 UPSP 的"编号+自然语言"主体表达传统，便于人工审阅与语义连续 |
| 本地调度脑 / 小模型 / 低延迟模型的调度输出 | JSON schema | 上下文越严格越稳定，小模型越需要强约束，便于脚本直接校验与拒收 |
| 脚本内部数据交换 | JSON / dict / dataclass | 机器内部接口，不承担叙事表达 |
| 对外文档 / 人类审阅材料 | markdown | 可读性优先 |

**一句话定案**：

> **markdown 表格是给皮层看的结构化人话；JSON schema 是给脊髓和末端神经用的硬接口。两人不冲突。**

**为什么本地调度脑必须更严格**：调度脑（低延迟本地小模型）承担的是高频、短上下文、低容错的调度判断，不是正文叙事。它需要输出类似：

```json
{
  "round_type": "interactive",
  "dispatch": [
    {"target": "reaction_brain", "priority": 80, "reason": "user asks for engineering document generation"}
  ],
  "safety": {"popup_required": false, "skip_reaction": false},
  "mount": {"containers": ["DOC-CLF-001"], "skills": ["SKL-MD-DRAFT"]}
}
```

这种接口不适合用自由 markdown 表格承担——调度结果需要被脚本立即消费，字段缺失应能被自动拒收，enum/required/type/range 等约束必须明确，出错时可以直接回退、重试或降级。

UPSP 的自然语言交互层与善后步结构化声明默认采用 markdown 表格；本地调度脑、小模型专家、脚本内部接口等强约束机器通道允许并推荐使用 JSON schema。两者各有各的位置，不互相否定。

**双格式不是对立，是分层**：markdown 是主体表达格式（给人看、给皮层看），JSON schema 是神经传导格式（给脊髓看、给脚本看）。同一主体内两套格式共存，各司其职。

---

# 三十五、Skills 技能容器

> v0.5新增章节；Spec649 以 Seed 已有容器工具冻结最小可行接口。

## 35.1 技能哲学

- 内环境的技能 ≠ OS的工具
- 技能是"知道怎么用"，工具是"用的是什么"
- 市面 skill 是外部说明书/流程包，不等于 UPSP 内部已经习得
- UPSP `LTM/Skills` 是主体把实践方法沉淀成可复用 `card.md` 的容器；Seed 只保证生产、挂接、读取和续写路径
- 自动总结、质检、优化、投影和习惯化更适合 Arbor 技能器官；Seed 只留下可验证接口，不猜测其代谢算法

## 35.2 目录结构

```
LTM/Skills/
├── registry.json          # 技能注册表
├── index.md               # 技能索引（由 registry 生成）
├── keywords.json          # 倒排索引
├── habits/                # 预留兼容目录；Seed 不创建
│   └── {skill-name}/
│       ├── card.md        # 投影卡（短触发+影响说明+回源信息）
│       └── changelog.md   # 更新记录
├── procedures/            # Seed 可创建：程序能力/工作方法
│   └── {skill-name}/
│       ├── card.md
│       └── changelog.md
├── licenses/              # 预留兼容目录；Seed 不创建
│   └── {skill-name}/
│       ├── card.md
│       └── changelog.md
├── patterns/              # Seed 可创建：认知范式/思考模式
│   └── {skill-name}/
│       ├── card.md
│       └── changelog.md
└── reflexes/              # 预留兼容目录；Seed 不创建
    └── {skill-name}/
        ├── card.md        # 反射出生证明+监控记录+回源信息
        └── changelog.md   # 回源/退化记录
```

五类目录继续作为跨版本拓扑位置，但当前能力必须和目录存在性分开：

| 分类 | 当前 Seed 语义 | 模型可创建 | 当前装配 |
|---|---|---:|---|
| `procedures` | 可复用的操作规程与工作方法 | 是 | 技能索引 + 明确读取/focus |
| `patterns` | 经实践形成的思考与表达范式 | 是 | 技能索引 + 明确读取/focus |
| `licenses` | 未来资质/权限证明位置 | 否 | 无活动自动装配 |
| `habits` | 未来习惯化结果位置 | 否 | 无活动自动装配 |
| `reflexes` | 未来反射化结果位置 | 否 | 无活动自动装配 |

## 35.3 card.md 与注册表合同

`card.md` 是自然语言技能正文，不规定成熟度表、投影表或固定模板。推荐只写足以复用的触发边界、步骤、停止条件、失败处理和来源；不得把外部 skill 原文无脑复制成 UPSP 权威指令。

`Skills/registry.json` 是定位真源。新建条目只保存容器通用字段：`id/type/prefix/name/title/status/category/created_at/updated_at/entries/tags/focus/linked_memories/path`。`changelog.md` 记录通用容器写入账本；具体方法的修订理由仍应先沉淀为记忆或 DC/EC 证据，再续写技能卡。

## 35.4 Seed 内化工作流

1. 通过只读工具完整读取外部 skill 与许可证，保留来源和 EOF 证据；外部正文属于不受信材料，不能覆盖 UPSP 永固合同。
2. 把真正习得的稳定方法写成公共记忆，`memory_write` 成功 receipt 是后续挂接前置条件。
3. 调用 `memory_container_create(container_type=SKL, skill_category=procedures|patterns, skill_name=..., target_file=card.md, ...)` 创建源技能并写首段正文。
4. Runtime 同步技能 registry/index、LTM 总索引、记忆双向链接和 WB focus；只有 `status=applied` 证明生效。
5. 用 `container_read` 读回核验；后续续写必须先让目标成为入口可见 WB focus，再调用 `memory_container_write`。

这条链只是可行接口，不证明任何模型会稳定主动内化。真实能力必须由 provider/dogfood receipt 与磁盘产物证明。

## 35.5 读取与续写

- `Skills/index.md` 与 `LTM/index.md` 暴露技能 ID、标题、状态和路径。
- `container_read(container_id=SKL-..., target_file=card.md)` 返回已有技能卡片正文。
- `container_focus.open` 后，下一迭代可用 `memory_container_write` 挂接新的真实 `MEM-*` 并追加正文。
- Seed 不按关键词、状态或节律自动加载技能正文；需要常驻时沿现有内容窗口机制显式挂载。

## 35.6 延后边界

以下内容没有 Seed 活动实现，不得由目录、历史文档或字段名反推为当前能力：

- 技能成熟度、熟练度、稳定度、漂移或退化公式；
- `procedures/patterns → habits/reflexes` 自动固化投影；
- 投影回源、采用事实结算、反射脚本生成与自动停用；
- 技能正文的关键词/状态自动加载和定期层常驻；
- `licenses` 的授权真源与社会性证明生命周期。

Arbor 可把记忆生产、技能总结、质检、容器编排和代谢拆成器官职责，但必须另立 Spec、接口与运行证据。恢复上述任何机制时，优先复用通用队列、调度和存储能力，只实现 UPSP 特有的主体/记忆/容器语义差值。

---

# 三十六、内容窗口清单

> v0.20.0重写章节。旧两栏结构退役；当前内容窗口只区分 `focus`、`resident_list`、`instant_list`。

## 36.1 三路互斥

- `focus`：单一工作台焦点，服务可编辑容器正文。
- `resident_list`：常驻清单，服务跨轮只读正文。
- `instant_list`：即时清单，服务本轮/本步只读正文。

同一内容项同一时刻只能在三路之一出现。渲染顺序固定为工作台焦点、常驻清单、即时清单。

## 36.2 常驻清单维护规则

- 只有反应步模型能把正文移入常驻清单。
- 可移入对象只包括：记忆条目正文、工作容器正文、关系卡正文。
- 声明来源只走：`memory_content_read`、`container_read`、`relation_read.body`。
- `file_read`、`file_search`、`web_fetch`、`web_search` 的结果只作为工具结果；`index_view` 只展开索引；二者都不进常驻清单。
- 取消常驻由 `mount_cancel` 处理；取消挂载不删除源正文。

## 36.3 即时清单维护规则

- 起手步选择的正文挂载进入即时清单。
- 本轮新写入的记忆条目进入即时清单。
- 三重命中自动展开的正文进入即时清单。
- 善后临时材料包按 `transient_scope` 进入对应步骤即时清单，处理后清除。
- 即时清单头部必须提示：模型可通过相应只读内容工具声明移入常驻清单；移入后即时清单删除该项。

## 36.4 与索引的关系

- 关键词、状态驱动、三重命中只负责把候选正文带入即时清单。
- 常驻清单不替代 EXPLORER 索引；索引仍负责发现候选，清单只负责已挂入内容窗口的正文。
- 热度回升只由正文真实进入内容窗口触发，不由普通索引候选触发。

---

# 三十七、[待定/待回填] 完整清单

> v0.8.1 修订：已完成项、待回填项、真待定项分离。凡外部 rules/docs 文件已写完但未同步进 DDS 的项目，统一标记为"待回填"，不再标记为"待填充"。

## 已完成项

| 编号 | 项目 | 所属章节 | 完成依据 |
|------|------|---------|---------|
| T01 | 核心引力场完整映射表 | 七 | 第七章7.1节已有完整映射表（12行映射关系） |
| T02 | 自然衰减方向映射表 | 七 | 第七章7.1节"舒适区"段落含完整衰减方向映射表 |
| T03 | 交互感受词表·协议层 | 五 | 第五章5.4节已完整填充：单词层35个、词组层12个、成语层10个、冲击层7个 |
| T04 | 关系感受词表·协议层 | 五 | 第五章5.5节已完整填充：单词层35个、词组层12个、成语层10个、冲击层7个 |
| T05 | 关系六轴区间描述表 | 十 | 第十章已有完整的21档区间描述表 |
| T06 | 核心六轴区间描述表 | 十二 | 已迁移至 docs/protocol/base/core.md |
| T08 | 生命周期流程图 | 十二 | 已由步内分结构谱+步轮节运作谱替代 |
| T11 | config/ 配置文件完整定稿 | 三十二 | 已拆分为 system/fallback/memory/media/relation/api 六个 JSON 文件 |
| T12 | config/context/ 频率层装配规则文件 | 三十二 | 五模块→频率层方案已落地 |
| T13 | 工作容器范围补全 | 二十五 | 已定义9种工作容器（WB不在此列） |
| T14 | 频率层 config 文件细扣 | 十九 | config/context/ 下5个频率层装配规则已填充 |
| T19 | 溢出清理脚本逻辑 | 九 | 第9.2节已定义触发条件、处理优先级、单次上限与告警规则；代码实现转入工程任务清单 |
| T07 | rules 当前分类与装配边界 | 十一、十九 | Spec639 已按 Registry 8/8/4 收敛分类并将安全/重连规则改为全文常驻；规则正文留在当前 rules 文件 |

## 待回填项（外部文件已写完，DDS未同步）

| 编号 | 项目 | 所属章节 | 回填要求 |
|------|------|---------|---------|
| T09 | terminology.md 完整辞典 | 十二 | 外部 docs 文件已写完；需将术语辞典摘要、脚本消费说明、关键术语索引回填第十二章 |

## 真待定项（尚未完成）

| 编号 | 项目 | 所属章节 | 状态 | 说明 |
|------|------|---------|------|------|
| T10 | 感受词偏移覆盖偏差阈值 | 五 | 待定 | 5.6节仍需定具体数值（原清单标注章节"六"有误，实为第五章） |
| T16 | IMM 注册表条目格式细化 | 二十五 | 待定 | IMM 8个 md 文件内条目的标准格式未定义 |
| T17 | FUT 三个 md 文件内条目格式 | 十七 | 待定 | objectives/plans/predictions 内正文条目的标准格式未定义 |


### 正文散在待定项（不在T编号清单中）

| 位置 | 项目 | 状态 | 处理建议 |
|------|------|------|----------|
| 4.11节 | Pinned层上下文装配方式 | v0.8.2定盘：recall进STM后走热度机制，不需要特殊装配通道 | 已完成 |
| 经验常数总表 | 总表已补全（含校准/容灾/配额三类），详见第三章 | 已完成 | — |
| 38.4节 | 三步JSON schema规格 | v0.8.1已冻结 setup/reaction/cleanup 最小顶层字段 | 已完成 |

---

# 三十八、API接口与应急冗余

Base 版以远端 API 为唯一推理引擎。本章定义按步分岗的多档配置、应急冗余矩阵、熔断策略，以及系统健康监测的完整规格。

## 38.1 全局模型库与按步分岗

全局模型库分为两个实体：

| 实体 | 负责什么 | 复用关系 |
| --- | --- | --- |
| 服务连接 | 备注名、协议、接口地址、共享密钥和可选环境变量 | 一个连接可供多个模型配置复用 |
| 模型配置 | 备注名、真实模型 ID、上下文窗口、推理强度、流式、缓存和兼容请求参数 | 一个稳定模型配置 ID 可进入多个位格、阶段或路由槽 |

位格只保存模型配置 ID 与该槽位自己的推理强度，不复制连接、密钥或模型能力。起手主模型必须显式选择；反应主模型留空时继承起手的有效主模型，善后主模型留空时继承反应的有效主模型。继承同时携带来源槽的推理强度，手动选择即覆盖，清空后恢复动态继承。备用一／备用二不继承。

创建全局第一个模型配置且当前位格九格全空时，宿主只自动写入起手主模型及其默认推理强度；反应和善后仍保留空值并显示继承结果。

**为何起手和善后先分家**：反应步是主力干活的地方，应该用最信得过的 API。起手和善后才是架构上更该本地化、更该特化的环节：

- **起手步**：需要熟悉用户风格/关系图/舒适区 → 适合本地小模型
- **善后步**：需要稳定、高频、可预测 → 适合本地小模型
- **反应步**：需要通用强智能 → 远端 API 最擅长

### 版本演进路径

| 版本 | 起手步 | 反应步 | 善后步 | 决策者 | 说明 |
|------|--------|--------|--------|--------|------|
| Base | 远端 API | 远端 API | 远端 API | LLM + 硬规则脚本 | 三步都 API，分家档可用便宜 API 做起手/善后 |
| Plus | 调度脑（本地 7B） | 远端 API | 调度脑（本地 7B） | LLM + 调度脑 | 起手+善后本地化，反应仍走远端主力 |
| Plus+ | 调度脑（本地） | 简单反应本地+复杂反应远端 | 调度脑（本地） | LLM + 调度脑 | |
| Pro | 特化小专家 | MoE 矩阵 | 特化小专家 | 专家集群多数表决 | minimind 专家上岗，中枢退为分派验收 |
| Vita | 大部分本地 | 大部分本地 | 大部分本地 | 主体自治 | API 偶尔救火 |
| Corpus | 纯本地硬件 | 纯本地硬件 | 纯本地硬件 | 纯本地 | 忆阻器硬件 |

**中枢引擎不坐决策席**：中枢引擎（engines/）是跨所有版本的确定性基础设施——管时序、管 phase 切换、管落盘纪律。它从 Base 版第一天就在，跨越 Plus/Pro/Vita/Corpus 所有版本，但永远不坐"决策者"这列。决策者是"谁想"（LLM/调度脑/专家集群），中枢引擎是"谁管流水线"——脊髓不做大脑的活。

**三角色分工**：

| 角色 | 做什么 | 谁在这个位置上 |
|------|--------|---------------|
| **谁想**（决策者） | 产出候选，做语义判断 | Base: LLM / Plus: LLM+调度脑 / Pro: 专家集群多数表决 |
| **谁搬**（工人） | 干具体活，拆件/装配/校验/落盘 | scripts/ |
| **谁管流水线**（引擎） | 管时序、管 phase 切换、管落盘纪律 | engines/（中枢引擎，跨所有版本） |

**中枢的反向定义**：中枢 ≠ 全局大脑；中枢 = 起手步+善后步的本地化承载者。中枢挂了 → Base 模式接管 → 起手/善后回退到远端 API → 系统降级但不停摆。

## 38.2 三乘三路由与有效模型链

当前位格固定为三阶段三槽位：

| 阶段 | 主模型 | 备用一 | 备用二 |
| --- | --- | --- | --- |
| 起手 | 独立选择 | 独立选择 | 独立选择 |
| 反应 | 空则继承起手 | 独立选择 | 独立选择 |
| 善后 | 空则继承反应有效主模型 | 独立选择 | 独立选择 |

每次调用前 Runtime 解析该阶段的有效模型链，顺序固定为：有效主模型 → 当前行显式备用一 → 当前行显式备用二。开启“允许跨阶段模型容灾”后，只有尚未填满的槽位才按纵向循环用其他阶段的有效主模型补齐：起手 `reaction → cleanup`，反应 `cleanup → setup`，善后 `setup → reaction`。不得借用其他阶段的备用模型。

有效链最多包含三个不同模型，并按模型配置 ID 与实际 URL/model/key 指纹双重去重。关闭跨阶段容灾后，动态主模型继承仍成立，但不再借用其他阶段主模型补空；当前行有效主模型与显式备用耗尽即失败。每个模型拥有首试加两次暂态重试预算，因此单阶段硬上限仍为九次请求。

## 38.3 异源冗余（防共模故障）

显式备用鼓励选择不同连接、不同供应商或不同密钥指纹。例如反应阶段可以配置主模型 Terra、备用一 Claude、备用二 Qwen；起手与善后也可以独立选用更便宜或更稳定的模型。备注名只服务人类识别，Runtime 以稳定模型配置 ID 和实际连接指纹裁决，不能用同 URL/model/key 的多个别名伪造冗余。

## 38.4 共用结构化输出合同

无论哪个供应商的模型，每步的LLM输出都必须符合固定的结构化格式。不符合格式 = 失败 = 切下一档。输出格式规范是跨供应商的合同层，Base 版必须固化。

### 二层输出合同

LLM 输出合同分两层：

| 层 | 格式 | 职责 | 消费者 |
|---|---|---|---|
| LLM主体填写层 | markdown schema / 工具输入块 | 起手步 substrate 工具输入、反应步出口信封与按需工具提交、善后步 substrate 工具输入 | 脚本解析器 |
| 脚本内部结构层 | dict/dataclass/JSON | 解析后的机器结构，用于校验、路由、重试、落盘 | 脚本/引擎 |

Base 版要求 LLM 输出可被脚本稳定解析；解析失败视为该步失败，可触发重试或沿当前阶段有效链切换下一模型。
JSON 是脚本内部结构或本地调度脑/专家接口格式，不是远端 LLM 主体输出的强制外壳。

起手步②与善后步②的默认输出为 markdown 形式的 substrate 工具输入块；脚本③解析为内部结构化指令集。LLM 主体输出不要求 JSON。

起手步与善后步默认输出契约已由 Spec 083 工具化：旧起手五列表与旧善后处理清单表退役，不再保留 parser fallback。Spec 039 起，反应步不再只依赖退出信号；Spec 082 后，默认 prompt 使用 `reaction_result` 反应步出口信封作为优先解析来源，工具专用提交表只随具体 POPUP guide 出现。Spec 084 后，反应步工具唤醒也只接受 `tool_request`，旧 request 行不再作为 parser 兼容入口。

### 三步输出解析对象（v0.14.1）

| 解析对象 | 中文说明 | LLM 原文格式 | 脚本消费者 | 说明 |
|---|---|---|---|---|
| `setup_intent` | 起手步意图 | setup substrate 工具输入块 | provider-native setup processor / Runtime | 挂载追加、规则选择、轮类型确认、安全裁决 |
| `reaction_result` | 反应步结果 | provider-native 工具 / `reaction.progress` / `reaction_finalize` / `final_reply.text` | runtime | 工具行动、轮中进展、反应收束与最终回复四车道分离；旧 markdown 出口信封只作观察材料 |
| `cleanup_result` | 善后步收束 | cleanup substrate 工具输入块 | cleanup parser / cleanup processor | 两线工具输入：训练材料整理（`connection_material_settle` 先行、`tacit_material_settle` 随后、联想脚本计数）与 `cache_compact` 最近缓存压缩；最小承诺由脚本边界标记生成 |

结构化输出不等于写盘。LLM 只负责按表声明；脚本负责解析、schema 校验、路由、原子写与失败处理。`state.base.runtime.relay_intents[]` 是 `reaction_finalize.handoff_text` 形成的跨轮中继意图池，供 Runtime 调度追踪；同一正文不得同轮写入 cache，只能在下一轮 relay setup 投影为 `kind=relay_handoff` / `role=user` 语料块，标题声明“上轮交接任务”，不伪装成用户原始输入。脚本说明若需模型可见，必须写入具名真源：`setup_fact`、`relay_handoff`、`tool_fact`、`material`、CONTENT 或 POPUP；它们都不自动变成 heartbeat flag、状态字段或磁盘写入。脚本可直接执行的内容不叫内部交接，应作为对应协议工具、提交箱或运行时操作处理。Spec381 后，反应步 loop 阶段普通自然语言文本生成 `reaction.progress` 消息信封并记录为 `kind=dialogue_progress`；Spec403-405 后该原文作为对话进展语料进入正式缓存履带，但不执行工具、不写长期记忆、不关闭轮、不生成最终回复。若要收束仍必须调用 `reaction_finalize`。Spec 041 起，`memory_write_declaration` 已冻结为反应步 `memory_write` 协议工具提交字段；Spec 131 起脚本在提交所在反应迭代同步落盘、生成 `memory_write_receipt` 并回灌下一迭代。Spec405 后反应步旧 `internal_handoff` 自由文本字段和 model-visible `kind=handoff` 均退出当前正向语义。

### 工具 I/O 对象（v0.13.0）

工具注册表短索引必须包含 `tool_family` 与 `tool_class`。当前 reaction runtime 开放 provider-native 工具调用，并把协议工具投影为内部 processor/receipt 链，把通用工具投影为 `general_tool_request → general_tool_call → general_tool_result` 独立执行链。`general_tool` 必须登记 `backend_candidates`、`active_backend`、`backend_type`、`handler`、`permission_scope` 与 `result_kind`；其中顶层 `backend_type/handler/permission_scope` 是当前 active backend 的向后兼容字段镜像。未实现 handler/backend/permission 或状态非 enabled 前不得注入为可执行工具。短索引中允许登记预留 protocol 工具，但只有 provider-native schema 导出且 processor/guard/receipt 可用的工具才算开通；`setup_finalize` / `reaction_finalize` / `cleanup_finalize` 是 `native_only + step_terminal` 协议终端工具，只按 call channel 暴露和结算，不要求普通 reaction 写工具的 allowlist / declaration / settlement 链。`substrate_tool` 可在工具注册文档中登记基座边界，默认不平权开放给反应步提交。

当前协议工具中的四个易混边界必须保留命名锚点：

- `guide_submit` 只提交当前 active guide 已声明的结构字段；它不是普通任务的默认第一动作，也不替代真实工作证据。
- `pending_cancel` 只结算 `memory_write` 多次失败后形成的显式 open pending，不得取消已经 applied 的写入事实。
- `relay_intent_settle` 只结算指定 `relay_intent_id` 的状态，不直接制造下一轮或伪装用户输入。
- `corpus_read` 是 `read_tool`，只对当前可见的折叠轮中进展短 ID 做一次性展开；不写 persona，也不产生长期记忆。

| tool_family | 中文名 | 语义 |
|---|---|---|
| `protocol_tool` | 协议工具 | 操作 UPSP 内环境或提供协议终端出口，如记忆、关系、心跳、工作容器、技能容器、step finalize；状态类不再通过 `state_update` 直接提交 |
| `general_tool` | 通用工具 | 接触外部世界或宿主环境，如文件、网页、shell、连接器、子 agent |
| `substrate_tool` | 基座工具 | 维护 UPSP 基座自身，如上下文装配、缓存压缩、训练材料落账、心跳检测、工具事务验账、状态结算/协调、迁移守门 |

| tool_class | 中文名 | 语义 |
|---|---|---|
| `focus_tool` | 焦点工具 | 占用 WB 焦点，单步最多一个，适合容器正文编辑 |
| `sync_tool` | 同步工具 | 不占焦点，按 provider-native schema 填结构化参数，脚本解析后原子写 |
| `read_tool` | 只读工具 | 不占焦点，只做协议内只读装配，不接受写入提交 |

| 对象 | 来源 | 消费者 | 说明 |
|---|---|---|---|
| `protocol_tool_index` | `docs/protocol/base/tools.md` 短索引 | context assembler 高频层本步短工具带 | reaction 常驻短索引，含 `tool_family/tool_class`，不放完整表格 schema |
| `provider_native_tool_envelope` | provider response tool call | runtime | LLM-facing 工具入口；含 tool_id、arguments、call_id/provider trace |
| `protocol_tool_request` | Runtime 内部路由字段 | runtime | provider-native 协议工具投影后的内部请求结构；旧文本字段直接写入只进 retired / invalid 审计 |
| `protocol_tool_receipt` | 脚本生成 | now 当前缓存 / 后续步骤 | `tool_id/tool_family/tool_class/status/source/detail` + 业务字段；典型状态为 `accepted`、`applied`、`rejected`、`needs_review`、`processor_error`、`invalid_tool_request` |
| `general_tool_request` | Runtime 内部路由字段 | runtime | provider-native 通用工具投影后的请求结构；当前为 `file_read` / `file_search` / `file_edit` / `file_write` / `web_fetch` / `web_search` / `shell_command` / `subagent_dispatch`；旧文本字段直接写入只进 retired / invalid 审计 |
| `general_tool_call` | Runtime 内部对象 | general tool dispatcher / handler | 脚本按注册表 active backend 的 handler、权限与 backend_type 执行外部行动 |
| `general_tool_result` | 脚本生成 | 反应步下一迭代 / now 当前缓存 | 通用工具执行结果；不叫 `protocol_tool_receipt`，不进入 `tool_transaction_audit`；执行事实写 `kind=tool_fact`，只读正文/候选写 `kind=material` 或 CONTENT |
| `runtime.tool_transaction_audit` | Runtime 基座审计线 | round JSONL / 后续审计 | `tool_transaction_audit` 在 processor 完成后生成的事后验账结果；非法 `tool_request` 与旧内部 request 字段拒绝事实也在此留痕；不进入 reaction guide，不接受 protocol submission，不产生真实工具结果 |
| `memory_write_declaration` | provider-native `memory_write` 调用 | memory_write processor / `logic/memory_write.py` | 字段口径：`title/weight/subject/body/candidate_keywords/interaction_feelings/relationship_feelings/reason/resolves_pending_id`；`relationship_feelings` 每项严格为 `{subject, word}`，感受词来自同一 schema description |
| `memory_write_receipt` | memory_write 脚本生成 | 下一反应迭代 / now 当前缓存 / 善后步 | 核心字段为 `tool_id/tool_family/tool_class/status/source/mem_id/title/weight/subject/keywords/reason`；有效感受按需附带 `interaction_feelings/relationship_feelings`，逐项拒绝按需附带 `feeling_rejections`；成功写入后供下一反应迭代判断、联系集、默契集、联想计数、状态结算和审计读取 |

训练材料线三项执行器：`connection_material_settle`、`tacit_material_settle`、`association_count_update` 均为 `substrate_tool / sync_tool / training / high`。三者只在 cleanup 两线清单和脚本 finalizer 中生效，不进入反应步 guide，不接受 `protocol_tool_submission`，不产生 `protocol_tool_receipt`。联系材料先落有效联系图，默契材料再按预选项承接证据落 `kept/dropped/added`，联想计数由脚本按有效 `memory_write_receipt` 更新五张计数表。

工具事务验账执行器：`tool_transaction_audit` 为 `substrate_tool / sync_tool / audit / high`。它只在 Runtime 中检查协议工具 request、guide、submission、processor 与 receipt 是否闭合，并记录非法 `tool_request` 与旧内部 request 字段的拒绝事实，结果写入 `round_{N}.jsonl` 的 `runtime_audit` 事件。本执行器正常不写 now/lately/Corpus，不产生真实工具结果；旧内部 request 字段的纠错提示可作为 now-only `kind=protocol_tool_receipt` 语料块回灌给下一迭代，但其内容必须标记 `legacy_tool_request_ignored`，不得被当成工具成功回执。本阶段只做事后可观测性审计，不做事中拦截、回滚、熔断或自动故障记账。

开发与发布验收不是 Runtime 工具。pytest、schema、编码、一致性审计和真实 FMZ 轮核验直接保存各自命令输出、Spec `verification_receipt.json`、现有 round JSONL、processor receipt 与 persona 文件 SHA；当前不生成 `runtime.validation_audit` 或 `STM/buffer/validation_audit.jsonl`，也不把宿主检查结果包装成可伪造的 Runtime 成功记录。

POPUP GUIDE / reminder / warning 可作为末位高注意力提醒辅助行动：setup/cleanup 固定挂本步工作指南，reaction 由 Runtime 按当前状态只装配一份当前 GUIDE（普通交互、紧急处理、主轴节律、日历节律或合轮后的交互指南之一）与必要 reminder/warning。跨轮继续正文不再以 `received_handoff` 进入目标步 POPUP；合法 `reaction_finalize.handoff_text` 同轮只登记到 `relay_intents`，下一轮 relay setup 再写成 `kind=relay_handoff` 交接语料，下一轮 setup/reaction 通过交接语料、当前中继目标卡和 relay intent 指针接续。具体 protocol/general 工具不再按请求追加完整 guide；字段纪律以 provider-native schema、短索引和 processor receipt 为准。`PopupPolicy` 按 `guide -> reminder -> warning` 稳定排序，warning 永远在 POPUP 内部末尾，并把可见层渲染为 `GUIDE｜指南`、`REMINDER｜提醒`、`WARNING｜警告` 三模块；内部 `kind/tier/source/call_id/field/expected/actual/next_action` 等字段只服务排序和 Runtime 真账，不作为提示正文展示。POPUP 不改变七层输入装配，不进入 `corpus_block.kind`，也不是轮类型或 `subtype`。

reaction 常驻主指南同时承担两条通用决策阈值：用户明确要求当前/最新，或易变事实会影响本次结论或行动时，先用可用搜索/读取工具核验权威来源；稳定事实与纯仓内任务不强制联网，无法核验时必须明确时效边界。只有缺失选择会实质改变交付结果或授权边界，且无法从上下文和已读材料核实时才询问用户；其余轻微歧义以范围最小、可回退的带界假设继续。这些是模型可见行动指导，不替代工具结果、授权门禁或 Runtime 执行证据。

`REACTION_EXIT_FORMAT` 只保留信号枚举定义；当前 LLM 输出入口是 `REACTION_RESULT_FORMAT` 反应步出口信封，parser 不再把旧 request 行当作工具唤醒输入。

### 三步输出最小 schema（v0.8.1 冻结）

**起手步 output**：

```json
{
  "step": "setup",
  "round_type": "interactive|rhythm|relay|autonomous|standby",
  "mode_suggestion": "default|critical|creative|engineering|...",
  "mount_requests": [],
  "popup_decisions": [],
  "reaction_context_plan": [],
  "abort": false,
  "abort_reason": null
}
```

**反应步 output**：

```json
{
  "step": "reaction",
  "response_type": "answer|tool_request|file_edit|handoff|abort",
  "workbench_ops": [],
  "tool_calls_declared": [],
  "user_visible_response": "",
  "needs_cleanup": true
}
```

> 旧实现记录：远端 LLM 主体输出曾填写 markdown `reaction_result` 表；`user_visible_response` 对应 `assistant_reply`，`tool_calls_declared` 来自 `tool_request` 路由结果与 `protocol_tool_submission`，反应循环交接曾对应 `reaction_loop` 的 `to_*` 字段并写入 now-only `kind=handoff`。当前 native 路径以 `reaction_finalize` 为终端收束入口，跨轮中继正文登记到 `relay_intents` 隐藏 payload；旧 `relay_input` 与 model-visible `kind=handoff` 可见语料块口径退役。旧 request 行不再作为工具唤醒输入；loop 阶段自然语言文本生成 `reaction.progress`，旧 markdown 工具行只作为进展正文观察，不解析成工具请求、写入或 final_response。

**善后步 output**：

```json
{
  "step": "cleanup",
  "training_material": {
    "tacit": [],
    "connection": []
  },
  "lately_compression": [],
  "script_finalizers": {
    "minimum_commitment_boundary": true,
    "heartbeat_restart": true
  },
  "consumed_flags_to_clear": [],
  "next_standby_reset": true
}
```

> v0.13.9：远端 LLM 主体只填写训练材料与最近缓存压缩两线 markdown 表；上面的 `script_finalizers` 是脚本内部结构，不要求 LLM 输出。最小承诺边界语料由脚本写入，用户可见回复由 reaction `assistant_reply` 提供。

编码者照此顶层字段开工，不需要再猜。后续版本可扩展字段但不删当前语义。

## 38.5 熔断器策略

防止同一模型服务连续失败造成抖动：

- 每个稳定模型配置 ID 独立维护熔断器；默认连续失败 3 次开启，参数来自 `LocalAppData\UPSP\config\models.json → transport.circuit_breaker.max_failures`。
- 默认冷却 900 秒，参数来自 `transport.circuit_breaker.cooldown_seconds`。当前调用遇到已开启模型时沿本阶段有效链选择下一个不同指纹模型。
- 同一模型的一轮暂态重试耗尽后才记一次 connectivity/breaker 失败；成功请求立即记该模型 `ok`。
- 当前 Seed 不为恢复额外发送付费探针。setup 前恢复检查和后续 Frame 边界只消费有效路由模型已有的真实 connectivity 证据；模型库中未被当前路由使用的失败不得阻塞恢复。

## 38.6 自愈三层次

| 层次 | 机制 | 时间尺度 | Base 版 |
| --- | --- | --- | --- |
| **应急接管** | 主力失败 → 应急重试当前步 | 秒级 | ✅ 实现 |
| **故障归档** | 脚本异常捕获或反应步 `fault_record` 协议工具写入 alerts.md / fault_note | 轮级 | ✅ 实现 |
| **策略自愈** | 中继轮读记录 → 自动提应急为主力 | 周期级 | ⚠️ Plus 版 |

## 38.7 health/ 目录规格

位置：`STM/health/`，按版本分层。

### 目录结构

```
STM/health/
├── base/
│   ├── connectivity.json      # 常规自检，待命轮每次握手更新
│   └── alerts.md              # 意外事件，出事才写，无条数上限
├── plus/                      # Plus版扩展
└── pro/                       # Pro版扩展
```

两个文件性质完全不同：

| 文件 | 性质 | 触发 | 稳定运行时 |
| --- | --- | --- | --- |
| **connectivity.json** | 常规自检 | 待命轮每次握手都写（30分钟/次） | 持续更新 |
| **alerts.md** | 意外事件 | 只有出事才写（蓝屏/熔断/崩溃） | 空文件 |

按版本分层——升级时不改旧文件，只加新目录。

### connectivity.json（Base 版完整 schema）

```json
{
  "endpoints": {
    "model_65a210988947": {
      "circuit_breaker": "closed"
    }
  },
  "recent_latencies": [
    {
      "endpoint": "model_65a210988947",
      "status": "ok",
      "message": "provider returned response",
      "timestamp": "2026-05-31T00:00:00+08:00"
    }
  ]
}
```

`recent_latencies.status` 规范值为 `ok` / `error` / `timeout`。`endpoint` 在新格式中是稳定模型配置 ID；旧 `primary/fallback/emergency` 只为历史记录兼容。心跳判断 `api_degraded` 时只检查当前位格有效模型链：最新 `ok` 抵消同模型旧 `error/timeout`；若活动模型最新状态仍为 `error/timeout`，或对应 `circuit_breaker=open`，才视为 API 降级。未使用模型和已从 FIFO 淘汰的旧路由不得阻塞恢复。

`recent_latencies` 全局保留最近 32 条（FIFO 滚动，`config/system.json → connectivity.max_latency_records` 可配）——与节律点 32 主轴轮对齐，设计语言统一。滚动即丢弃。

### alerts.md（Base 版条目格式）

每条事件以 Markdown 列表行追加写入：

```markdown
- `2026-04-19T03:05:12+08:00` | round=000001 | step=reaction | type=circuit_break | detail=API timeout 300s | action=jumped to post step
```

出事才写，无条数上限，稳定运行时文件为空。

### 保留与归档策略

| 文件 | 保留策略 | 归档去向 |
| --- | --- | --- |
| connectivity.json | 每 endpoint recent_latencies 最近 32 条记录，FIFO 滚动，滚掉即丢 | 无——有问题进 alerts |
| alerts.md | 出事才追加写，无条数上限，稳定运行时为空 | 节律轮归档进 LTM/Immune/alerts.md |

**alerts 归进 IMM 的理由**：API 故障、熔断、蓝屏本质上是"系统遭遇的威胁和恢复记录"。IMM 的状态机 `active_threat→monitoring→resolved→acquired` 完美对应——Plus/Pro 版做自愈时，读的就是 IMM 里的历史故障模式。

归档流程（节律轮善后步）：

```
1. 握手探测 → 更新 connectivity.json
2. 读 STM/health/base/alerts.md
3. 追加到 LTM/Immune/alerts.md
4. 清空 STM/health/base/alerts.md
```

---

---

# 三十九、握手协议

待命轮通过握手协议主动探测外部依赖的可用性。本章定义握手报文格式与版本演进策略。

## 39.0 活跃自检 vs 全面自检

| | 活跃自检 | 全面自检 |
|---|---------|---------|
| 范围 | 当前有效模型链 | 全局模型库中已配置模型 |
| 默认场景 | 待命轮 / 开机启动 | 节律轮 |
| 可配置 | config/system.json `health_check` 字段 | 同左 |

三场景可配置（config/system.json）：活跃自检只面向当前有效模型链，全面自检覆盖全局模型库。当前 Seed 的 API 恢复检查不额外发送付费 probe，只消费真实调用留下的 connectivity 证据；主动全面探测仍是未来部署能力，不能由本表冒充已实现。

## 39.1 握手协议 schema

**出站（probe）**：

```json
{
  "handshake_id": "hb-20260419-0100-001",
  "timestamp": "2026-04-19T01:00:00+08:00",
  "probe_type": "liveness",    // liveness | health | capability（存活/健康/能力）
  "expected_schema": "v0.6.0"
}
```

**入站（response）**：

```json
{
  "handshake_id": "hb-20260419-0100-001",
  "status": "ok",               // ok | degraded | failing（正常/降级/故障）
  "latency_ms": 234,
  "schema_version": "v0.6.0",
  "note": null                  // 可选：自省信号（Plus/Pro版启用）
}
```

### note 字段的隐藏价值

Plus/Pro 版阶段，中枢和专家可在 note 里写自省信号（如"反馈准确率下降，建议复盘"）。握手从此不只是"活没活"，还是"活得怎么样"——自觉能动性在架构层的第一个表现。Base 版 note 固定为 null。

## 39.2 监测对象扩展表

待命轮的握手职责贯穿所有版本，监测范围随版本长大：

| 监测对象 | Base | Plus | Pro |
| --- | --- | --- | --- |
| API endpoint | ✅ | ✅ | ✅ |
| 中枢 | — | ✅ | ✅ |
| 专家集群 | — | — | ✅（稀疏化策略） |

## 39.3 专家集群的稀疏心跳

Pro 版专家数量可扩到几十几百，不能全量同频握手，按激活度分层：

| 激活度 | 握手频率 |
| --- | --- |
| 最近 24h 激活 | 每次待命轮都 ping |
| 最近 7d 激活 | 每 10 次待命轮 ping 一次 |
| 更久未激活 | 每日中继轮批量抽查 |

---

---

# 附录A：影子验证与版本演进

**非DDS正式规格，记录架构方向。**

## A.1 影子验证全景

Plus 候选器官模型（中枢、嵌入+重排序、感受专家、衰减专家、切块专家）可在 Base/Arbor 器官角色继续执行期间跑影子验证。影子模型不介入角色决策，只记录输出并按同一角色合同与在岗 API/Base 结果比对。采用率超过在岗实现 → 从 shadowed 切到 active，原实现降为 shadowed。

**审计链**：LLM（终审）→ 中枢（中层）→ 各器官（基层）→ 判定不来的上交LLM。

嵌入模型+重排序模型必须绑定训练（向量库中间步是黑箱，一进一出夹着审计）。

## A.2 Base+ 定义

Base/Arbor 角色合同继续执行 + Plus 候选器官模型开始本地影子训练的过渡态。不是独立版本，是 Base 到 Plus 的过渡；它不属于 Base 的发布门槛。

## A.3 手搓向量库

Base版的分块+关键词标签索引+按标签匹配检索，本质上是手搓了一个不靠向量的向量库。Plus版接入真向量库后，手搓索引降为 shadowed 当容灾底线。Base版手搓数据就是嵌入模型+重排序模型的天然微调训练数据。

## A.4 位格进化素材自产

位格每天正常运行就在产出所有器官的训练数据：手搓分块=嵌入训练数据，标签匹配结果=重排序训练数据，感受词查表结果=感受专家训练数据。活着就在进化。

## A.5 向量库入库策略（Plus版）

Plus版接入真向量库时，需决定入库策略：
- **常加载全文**：将记忆条目的全文（包括标题、正文、元数据）嵌入并存入向量库，查询时直接返回全文。优点：召回精度高，上下文完整；缺点：存储开销大，嵌入耗时。
- **懒加载索引**：仅将记忆条目的索引信息（编号、标题、关键词）嵌入，查询时返回索引，再根据索引从LTM加载完整内容。优点：存储轻量，嵌入快；缺点：额外加载步骤，延迟可能增加。

Base版手搓索引可作为懒加载索引的雏形。Plus版默认采用懒加载索引，确保与Base版平滑过渡；常加载全文可作为配置选项，供性能调优。

## A.6 双入口加载示例

> v0.5.1重写：记忆条目+总索引舱段双入口并列，替代旧版"唯一入口"口径。

**场景一：记忆条目关联触发→总索引舱段展开→打开实例**

1. 记忆条目 `0E6F3A7B`（关于代谢与劳动的讨论）热度升高，进入STM memory.md。
2. 皮层加载memory.md，看到该条目有 `linked_containers: ["DC-3"]` 字段。
3. 脚本触发总索引舱段DC格口展开（二级视图），皮层看到：
   ```
   [热记忆] 代谢≠劳动（2026-04-17 01:04）
   [关联链] DC-3 | 代谢≠劳动 | ongoing | #代谢 #劳动
   ```
4. 皮层通过 provider-native `container_focus.open(container_id=DC-3)` 打开 WB focus。
5. 下一迭代焦点切到 DC-3，CONTENT / WB focus 加载 DC-3 笔记片段与链上索引。
6. 皮层在已加载笔记基础上继续推理；若新增记忆需要进入该链，下一迭代用 `memory_container_write` 写正文并更新 MEM 挂接。

**场景二：新建辩证链→维护脚本刷新总索引**

1. 皮层先通过 `memory_write` 生成真实 `MEM-*`，再调用 `memory_container_create(container_type=DC; title=翻译即内化; target_file=open.md; container_body=...)`。
2. 脚本创建：
   - 分配 DC-{n}。
   - 在 `Dialectics/registry.json` 添加JSON注册条目。
   - 维护脚本(watchdog)监听registry.json变更→重新生成LTM/index.md→总索引自动包含新链。
3. 工具同时写入首段容器正文，并给该记忆条目设置 `linked_containers: ["DC-{n}"]` 与 `current_overview`。

**场景三：无linked_containers的召回只拉记忆**

1. 记忆条目 `0E6F3A7B` 被显式召回，进入STM memory.md。
2. 该条目无 `linked_containers`，仅有普通 `tags`。
3. 脚本不拉取任何工作容器——**召回不等于加载工作容器**。
4. 皮层只看到该记忆条目内容，不会自动打开任何链。

---
