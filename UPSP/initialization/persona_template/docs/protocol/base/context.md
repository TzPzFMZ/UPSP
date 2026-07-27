# 上下文装配参数表·协议层

> 消费方式：脚本查表——编译 `provider_request.v1` 请求信封时查本文件取频率层排列、token预算、窗口控制参数、过期标记、缓存参数
> 注入模块：不直接注入（脚本内部消费）
> 触发：每步装配

---

## 一、频率层注入位置

`step.json.request_body` 中的 provider messages/input 正文按七层频率梯度（DDS §19.3）从低到高组织，同时优化缓存命中（稳定内容在前）、注意力利用（U型两端放重要内容）和死区利用（近期历史语料放中间）。顺序：永固层（manifesto + core编码 + Registry `permanent` 的8份全文 RULES）→ 定期层（定期记忆投影）→ 最近缓存 lately → 高频层（倒排索引 + STM全量索引 + 本步短工具带 + CONTENT挂载正文 + 参考窗口）→ 当前缓存 now → STATUSBAR 状态栏层 → 弹窗层/POPUP。

messages 末位区域：当前缓存 now（交互、资料、工具、内部交接按 `corpus_block.kind` 区分）→ STATUSBAR 状态栏层（状态栏 + 当前交互对象稳定锚点 + 关系焦点摘要）→ 弹窗层/POPUP（绝对末位）。STATUSBAR 先生成 `statusbar_snapshot.v1` 结构化投影，再由同一投影渲染模型可见 Markdown；`60_statusbar.json.projection` 供后续 GUI 直接读取，不反向解析 Markdown。

`now` 当前缓存主源为 `STM/context/cache/now_cache.jsonl`，位置在高频层之后、STATUSBAR 之前；`lately` 最近缓存主源为 `STM/context/cache/lately_cache.jsonl`，位于定期层之后、高频层之前，只接纳 `policy.lately=true` 的语料块。`context_buffer.json`、`near_cache.*`、`remote_index.json`、`remote_blocks/` 已退役，不再作为运行时读写或 fallback 路径。

当前 Registry 的 `step_level=[]`、`periodic=[]`。setup、reaction、cleanup 三步装配同一组8份 permanent RULES；步骤差异由当步工具 schema／短索引、Runtime 事实与末位 POPUP guide/reminder/warning 表达。旧 `setup.md`、`reaction.md`、`cleanup.md` 不在 Registry 中，不参与当前生产装配。

---

## 二、模块 token 预算

五模块按内容标签计预算，与物理位置无关。脚本裁剪时按标签定位内容块。

| 内容标签 | 字符上限 | token 估算 | 说明 |
|---------|---------|-----------|------|
| STATUSBAR | 300字 | 150t | 状态栏极短，硬上限 |
| EXPLORER | 1500字 | 750t | 索引行列表，可截断 |
| CONTENT | 4000字 | 2000t | 正文区，波动最大 |
| RULES | 1500字 | 750t | 按需加载，可裁规则 |
| POPUP | 500字 | 250t | 有当步注意力事件时才占位 |
| 分隔符/标题 | 200字 | 100t | 频率层标题+分段标记；`lately` / `now` 不注入独立统计层头 |
| **系统注入合计** | **~8000字** | **~4000t** | 常态上限 |

窗口降级：128K+ 和 64K 窗口宽裕（8000字/4000t，32轮），32K 精简 CONTENT 和 EXPLORER（6000字/3000t，16轮），16K 大幅精简（4000字/2000t，8轮），8K 以下不推荐运行 UPSP。

`layers/*.json` 是稳定机器层真源，只保存 `schema/layer_key/layer_id/order/source/chars/sha256/content`。同一稳定 payload 再次装配或编译时脚本跳过重写；变化、缺失、损坏或 schema 不符时才重写。`dirty/reused` 只进入 `manifest.json.layers` 与 `step.json.layers_manifest.layers[]`，表示本次装配或本次 provider 编译的写入状态；它们不进入 `layers/*.json`，也不进入 `step.json.request_body`。

---

## 三、窗口控制

当前窗口控制不走旧式模块裁剪顺序。脚本按频率层装配、定期层限额、lately 字符窗口、高频层按需、now 字符窗口与 STATUSBAR 硬上限控制上下文；POPUP 仍位于 provider messages 绝对末位，STATUSBAR 紧贴 POPUP 之前。LLM API payload 的上下文正文只来自 `step.json.request_body`；`step.md` 与 `layers/*.md` 只是由 `layers/*.json` / `step.json` 派生的审计渲染，不得作为 `system_prompt` 再次发送。`STM/context/round/round_{N}.jsonl` 只记录轮审计事件流，不进入上下文装配、语料缓存或 Corpus。LocalAppData 审计缓存中的 `round-index.js` 与 `round-data/round_{N}.js` 只是由 JSONL 生成的静态查看投影，同样不进入上下文装配。

过期标记以频率层为单位：永固层、定期层可置过期，高频层、lately、now、statusbar 每轮/每步由脚本直接重算或推进，POPUP 通过 `popup_active` 表示当前是否有当步注意力事件。新节律周期、位格初始化或重连、安全事件等级≥3 时强制全量重拼。

高频层中的 EXPLORER 索引默认只展示各索引的当前上限；折叠项必须带 `index_view` 可请求参数，例如 `scope=ltm_inverted; offset=8; limit=8`。`index_view` 是 `protocol_tool / read_tool / context`，只读取高频层折叠索引段，不写 persona，不改 WB focus，不挂正文，不改 STATUSBAR；执行状态进入 A 轨 `kind=tool_fact`，索引展开内容进入 B 轨 `kind=material`，供后续反应迭代消费。当前接入 `ltm_heat`、`stm_heat`、`skills_inverted`、`ltm_inverted`、`stm_inverted`、`association`、`relation_inverted`、`relation_domain`。反应步本轮新写入并临时挂载的记忆条目只在当前轮装配投影中从 STM 索引隐藏；正文、meta、真实索引、倒排关键词和热度仍即时落盘，取消本轮挂载或跨轮重装配后恢复正常索引显示。

关系倒排索引是本轮动态命中集，只根据当前输入、当前交互对象、对象名/别名/稳定标签命中已有关系卡；不再使用关系感受词来源。关系域索引是四区底图，按 `self / ours / them / orgs` 展示，每区默认 8 条，命中对象或焦点对象置顶/高亮，其余按 `updated_at` 排。高频层不再展示 `感受词库（仅词条，不含数值）`；感受词清单默认在 provider-native `memory_write` schema description 中，必要时只通过短提示提醒模型查看 native schema。高频层还挂载本步短工具带：setup/cleanup 只列本步固定 substrate 工作流工具，reaction 列 protocol/general provider-native 工具短索引。

POPUP 事件按指南、提醒、警告三层渲染。指南只讲当前车道规则，提醒只提示证据、行为边界或中继目标卡，警告只说明失败、拒绝、非法结构或必须纠偏的动作。结构字段、来源字段、裁决布尔值和内部类型名留在 `step.json`、Runtime fragment 与 round audit，不直接进入可见 POPUP 正文。`native_tool_result` 是 provider-native 工具失败/拒绝/无效请求的警告卡，只说明失败事实、原因和下一步纠偏动作；它不是工具入口、processor receipt 或 now/lately/Corpus 语料块。`received_handoff`、`上环交接`、`待处理交接` 与 `HANDOFF｜交接` 可见模块已退役。reaction 默认挂反应循环指南与必要提醒，`reaction_loop` 不再渲染成交接层标题。装配器按“指南 -> 提醒 -> 警告”稳定排序，警告永远在 POPUP 内部末尾。触发逻辑仍由脚本负责。

---

## 四、三步装配差异

三步在频率层和内容标签两个维度上有明确差异。起手步 CONTENT 为空（预连接不需要正文），反应步 CONTENT 已填充（操作正文），善后步 CONTENT 只挂索引不挂正文（归档只需索引）。EXPLORER 在起手步和善后步全展开，反应步按焦点收窄。三步 RULES 全文相同；步骤差异来自工具头、Runtime 事实和 POPUP。三步 POPUP 均可承载本步 guide，其中善后步只提示当前真实善后义务，并在 lately 水位删除后提醒最近缓存压缩动作。

---

## 五、缓存通道参数

语料层主源走 `now_cache.jsonl` 与 `lately_cache.jsonl` 两段字符窗口。A 轨语料按 `Corpus + now→lately`、B 轨正式 material 按 `now→lately` 滚动；默认 `now.budget_chars=65536`、`now.trim_chars=16384`，触顶后最早完整 A/B 块立即滚入 `lately_cache.jsonl`。`runtime_call_request` 固定占位每次调用可见，但不写 cache、不参与水位。C 轨只服务带明确目标步骤的单次调用，调用完成即清除。只读工具的执行事实写 A 轨 `kind=tool_fact`，读到的正文、候选列表和索引展开内容只写 B 轨 `kind=material` 或既有 CONTENT 挂载，`tool_fact.ref.tool_result` 不得隐藏正文副本。当前读写声明仍以本轮真实工具结果、processor receipt 或 round audit 为准。

模型可见语料头按语料类型单独渲染，不再使用通用结构字段头，也不为 `lately` / `now` 额外注入独立层头概述。交互、助手回复、资料、工具事实、上轮交接任务、最小承诺、故障记录和缓存摘要分别用中文说明当前轮/历史轮/压缩来源、证据效力和最低必要说明；条数、字符数、当前可见轮次和来源轮次聚合等审计字段留在 `manifest.json`、`step.md`、`layers/*.md` 和 round audit，不迁入 STATUSBAR。轮结束工具事实不再降解显示为“历史工具事实摘要”，最近缓存水位触发的语义压缩显示为“最近缓存压缩摘要”。工具事实正文使用自然语言描述，例如“已读取文件……读取范围……继续读取请调用 file_read(path=..., line_start=N)”。最小承诺只渲染为一行中文边界。

`lately_cache.jsonl` 默认 `budget_chars=262144`、`trim_chars=65536`、`compact_ratio=0.618`，触顶后删除最旧完整块；三步读取同一 lately 字符窗口，不再按起手/反应/善后取 8/32/8 轮。只有本轮发生 `lately_trimmed=true` 时，cleanup 才在 POPUP 提醒最近缓存压缩；删后幸存段仍在 lately 层，真实 source ids 留在 Runtime pending metadata，压缩结果写回缓存摘要语料块。eligible 历史证明型 A 轨语料被 lately 接纳时同步镜像进 `STM/buffer/raw_log.jsonl`，不随 lately FIFO 回删；主轴节律轮再把当前 raw_log 成对归档到 `LTM/Corpus/public/rhythms/`，同名 `.md` 只是派生阅读副本。material、压缩摘要和轮结束历史工具摘要不写入 raw_log 或 Corpus。交互语料块在结构化引用中携带交互对象、身份状态和来源；反应步遇到身份未知时追加身份确认提醒，高影响动作必须先确认或询问，不允许靠 STATUSBAR 全量关系 fallback 猜对象。quarantine_buffer.json 为 FIFO 32 条滚动删除，存安全粗筛标记的可疑输入。

资料输入、图像说明、文件正文、网页正文、搜索候选和索引展开等外部/只读资料作为 B 轨 `kind=material` 进入 now，水位触发即滚入 `lately_cache.jsonl`，同轮在 now+lately 中连续可见并按 lately 完整块 FIFO 自然淘汰；material 不写入 Corpus，也不参加 `cache_compact` 摘要。善后本轮材料包同为 `kind=material`，但走 C 轨，必须带 `transient_scope=cleanup_round` 与 `transient_target_step=cleanup`，cleanup 调用完成后清除，不压成历史摘要。工具执行状态、范围、游标、失败原因等短事实写成 A 轨 `kind=tool_fact`，按 Round 写入 Corpus，并允许水位后进入 lately；只读正文和候选内容不得拼进 `tool_fact` 或藏在嵌套 ref。当前 Seed 只处理文字代理、摘要、caption、OCR 或路径引用；图片 ingress、provider image block、媒体转换与记忆媒体生命周期整体 deferred。`reaction_finalize.handoff_text` 产生的跨轮继续正文同轮只登记到 `state.base.runtime.relay_intents[]`，不写 cache；下一轮 relay setup 才投影成 A 轨 `kind=relay_handoff` / `role=user` 语料块，标题必须声明“上轮交接任务，不是用户原始输入”。`kind=handoff` 与模型可见 `internal_handoff` 当前退役。心跳触发事实如需进入正式调用，使用具名 `kind=setup_fact`；节律任务本身由 GUIDE / POPUP / 当前内容窗口表达并随 GUIDE 生命周期撤换。反应步不再给本轮善后步留下自然语言交接，善后输入来自 cleanup C 轨临时材料、结构化回执和 Runtime pending metadata。

最小承诺与故障记账也是语料块：善后脚本把纯边界标记写成 `kind=minimum_commitment`；故障由脚本异常捕获或反应步 `fault_record` 协议工具写成 `kind=fault_note`，同时保留 `alerts.md` 告警索引。最近缓存压缩没有 kind 白名单、当前轮保护或最小承诺特保；若内容必须不可压缩，应进入记忆条目、工作容器、剪贴板或审计快照，而不是要求缓存保真。

---

## 六、定期层注册

内容窗口位于高频层 CONTENT，由三路组成：`focus` / 工作台焦点、`resident_list` / 常驻清单、`instant_list` / 即时清单。常驻清单只读挂接记忆条目正文、工作容器正文和关系卡正文；即时清单承接起手步挂载、本轮新写入、三重命中和临时材料。取消三路挂载使用 `mount_cancel`，只移除挂载项，不删除源正文。`periodic_mounts.json` 是定期记忆投影文件，不迁入三路内容窗口状态；旧“五类挂载”口径已经退役。当前没有场景 RULES 自动追加，只读容器与外部内容归高频层参考窗口，不再塞进定期层机器源。

与 `container_registry.json` 的区分：前者只管定期记忆投影，后者管 9 容器元数据（持续有效）。技能不进入当前定期层；`LTM/Skills/registry.json` 只是真实技能容器的定位、挂接与 focus 真源。

## 七、终端收束与裸文本边界

上下文层可以保存和展示交互、资料、工具执行事实、内部交接和历史摘要，但这些语料块不是当轮执行事实本身。setup / cleanup 终端结果必须来自 `setup_finalize` / `cleanup_finalize` 的 provider-native envelope；反应步完成时直接自然语言回复用户，只有跨轮继续才调用 `reaction_finalize(handoff_text)`。普通正文、历史缓存、旧助手声明、markdown 表格、冒号行和未来 reasoning stream 只能作为观察、警告、来源证据扫描或历史摘要材料。

当轮工具事实以 provider-native `tool_call_envelopes`、`general_tool_result`、`protocol_tool_receipt`、processor receipt 和 round audit 为准；用户可见最终回复以无阻断自然语言候选或 Runtime 确定性兜底为准。缓存中的历史工具摘要只说明“历史上曾有工具事件被摘要”，不证明当前轮已经执行。
