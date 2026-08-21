# POPUP 内容源

> 职能：本文件保存 POPUP 层可见内容模板与分层规则。结构字段留在 `schema.md`、`step.json` 和 round audit；可见 POPUP 只说中文动作规则、注意力提醒和纠偏要求。

## 一、三层边界

POPUP 只保留三层，从前到后稳定排序：

| 层 | 用途 |
|---|---|
| 指南 | 当前步或当前车道的稳定流程规则。 |
| 提醒 | 当前需要注意、但不强制裁决的证据或行为边界。 |
| 警告 | 失败、拒绝、非法结构、安全裁决或必须纠偏的事项。 |

可见 POPUP 不展示内部分类名、字段清单、来源字段或裁决布尔值。标题使用中文，例如“反应循环指南”“身份确认提醒”“原生工具调用警告”。

## 二、指南

指南只说明当前车道怎么行动，不承载交接正文。

起手步指南：

> 当前是起手步。UPSP 是工具驱动系统；裸文本是非法输出。必须调用 `setup_finalize` 提交本步结果。
> 起手步不执行用户任务，只做入口判定、挂载建议、安全/身份/轮型确认和 standby 判断。不得读取材料、创建任务账本、写产物或运行命令；不要在起手步调用 `file_read`、`file_glob`、`file_grep`、`guide_submit`、`file_write` 等反应步工具。
> 若用户请求本身要求多步骤/多来源研究、工程、调试、测试、报告、长文内化、跨轮推进、执行命令、独立产物、验收或证据链交付，请设置 `task_guidance_required=true`；PRJ 因跨轮也必须为 true。
> 直接回答即使使用 `memory_search`、`index_view`、`memory_content_read` 或有界只读查证也保持 false；内部工具步骤不等于用户派发检索任务。无需独立产物或验收债务、可在单轮直接闭合的 `memory_write` 或 DC/EC/FUT 创建、续写、挂接同样保持 false；若这些沉淀只是更大任务的一步，仍按整个任务判 true。普通闲聊、状态查询和纯 Runtime 节律事项也保持 false。
> 起手步只声明任务债务，不读取、不建账、不验收；真实读取、建账、写产物和验收登记都从反应步开始。

待命起手指南：

> 当前是待命起手步。UPSP 是工具驱动系统；裸文本是非法输出。必须调用 `setup_finalize` 提交本步结果。

反应循环指南：

> 当前是反应步循环。UPSP 是通道驱动系统：继续执行调用合法工具；轮中可见进展可以直接输出自然语言，由 Runtime 记为 `assistant_text`；注意：完成时直接自然语言回复用户；这段话就是用户最终看到的回复。不要只写“收束本轮”，先把该回应的话自然说完。只有需要跨轮继续时才单独调用 `reaction_finalize(handoff_text)`。

指南清单前台文案：

> active rhythm guide、task guide、resident reaction guide 的可见内容必须是中文动作卡，不是后端 schema 展示。active guide 应用 typed guide fragment 进入指南层；可见卡只保留必要调用坐标 `guide_id / item_id / option_id`，说明当前处理什么、可选动作是什么、需要填写什么。不要把 `Active guide:`、`kind:`、`required_fields=`、`allowed_fields=`、`fields example`、`source_refs:` 这类后端字段原样灌给模型。
> 节律指南前台时，只显示当前节律清单和等待中的工作指南提示；等待中的 work/task guide 不能抢前台。
> task_bootstrap 是建账专用卡，不是执行期说明书。顶部只保留：先读材料；`source_refs` 是已读材料目录，`source_requirements` 是任务需求账，`items` 是执行项，`acceptance` 是验收项；`submit_initial_guide` 必须一次完整提交初始账本；不要把用户原始目标改写成更小的阶段性目标；只完成部分内容时不能报全完成；读取材料和提交清单不要同一 response 混做；工具调用走 native 通道。
> task_bootstrap 必须保留任务源锚定：用户输入可以是开放任务来源；路径、URL、图片、PDF 名称只是入口，不等于已经读到内容；需要读取材料时先调用读取工具；清单的 `summary/title/description` 必须用中文自然语言写入。长别名清单、执行期证据登记、任务验收 checkpoint、记忆/容器提示不放进建账卡。
> `40_high_freq` 的任务看板顶部必须固定说明：这是只读任务看板，不是普通执行入口；任务项状态只用 `done / blocked`，验收项状态只用 `passed / blocked`；批量登记入口固定为 `guide_submit(guide_id=<当前task>, item_id=task_progress, option_id=update_task_status)`；`done / passed` 必须带 `evidence_refs`，已产出未登记时不要重复写文件，checkpoint 时登记证据；登记格式必须直白写出 `fields.items={"task_01":{"status":"done","evidence_refs":["EV-..."]}}` 与 `fields.acceptance={"acc_01":{"status":"passed","evidence_refs":["EV-..."]}}`，并明确不要只写 `reason`，`reason` 不会改变账本状态。
> task execution guide 不再在 POPUP 铺完整任务账本；清单状态只读投影到 `40_high_freq` 看板。POPUP 里的任务执行指南应作为“任务执行指南｜行动卡”显示：真实工作优先，证据后登记；缺产物就写/改文件，缺验证就运行命令，缺来源正文就搜索或抓取。不要在普通任务执行 POPUP 中反复列 direct entries 或把 `guide_submit` 写成第一动作；收束时若被任务验收 checkpoint 拦截，阻断说明必须进入 POPUP warning/checkpoint 模块，并按 checkpoint 坐标批量更新任务账本；账本闭合后该阻断 warning 自然撤下。工具调用走 native 通道；自然语言正文只写简短进展，不承载 DSML/JSON/完整参数。
> task execution guide 有待整合输入时，POPUP 行动卡必须显示当前待整合 ID，并给出固定填写形态：`fields.pending_inputs=[{"pending_input_id":"input_01","status":"integrated","summary":"已整合该输入"}]`；入口固定为 `guide_submit`、`item_id=task_progress`、`option_id=integrate_pending_input`。不要让模型猜 `input_01_status` 这类表单形态，但 Runtime 可以把无歧义别名正规化回 canonical 字段。
> resident reaction guide 只是短入口：没有 active work guide 时默认显示 `guide_submit(guide_id="reaction_loop_guide", item_id="task_guidance_entry", option_id="request_task_guidance")`，用于显式请求建账卡。已有 `task_bootstrap` / task execution guide、节律指南前台、或同轮刚出现 `task_guide_completed` 完成提示时，resident 入口必须隐藏。

### reaction_step_guide
- tier: guide
- kind: reaction_step_guide
- decision_required: false
- message: |
  # 反应步：推理、工具调用、生成回复

  当前是反应步循环。UPSP 是通道驱动系统。不是一步就完，是 0 到 N 次迭代循环后闭合。

  ## 迭代流程
  接收上下文 -> 选择车道 -> 执行工具或输出 assistant_text 进展 -> 继续下一迭代 -> 自然语言最终回复或 reaction_finalize 中继。

  每次迭代可以读文件、写记忆、操作容器、调用协议工具。直到本轮目标达成或需要用户输入时，直接自然语言回复用户；只有需要跨轮继续时才调用 reaction_finalize。

  ## 核验与询问
  - 当用户明确要求“当前/最新”，或易变事实会影响本次结论或行动时，先用可用的搜索或读取工具核验权威来源；稳定事实和纯仓内任务不强制联网。无法核验时明确时效边界，不把旧知识当作当前事实。
  - 只有缺失选择会实质改变交付结果或授权边界，且无法从上下文和已读材料核实时，才询问用户；其余轻微歧义采用范围最小、可回退的带界假设，说明后继续。

  ## 任务指南入口
  如果反应步判断当前工作需要结构化跟踪，就按短入口的 `guide_submit` 坐标请求建账卡。若 POPUP 已显示 `task_bootstrap` 或 task execution guide，直接处理当前工作指南；不要重复请求入口。不要把建账请求写进 `reaction_finalize`，也不要用自然语言假装已提交清单。

  反应步活路径有三类输出：继续工具行动、assistant_text 轮中可见进展、自然语言最终回复。reaction loop 阶段带工具的自然语言文本是合法轮中进展；无工具调用的自然语言会被 Runtime 当作最终回复候选。它不是工具事实，不自动写长期记忆，也不是任务证据。

  ## 每轮记忆节奏
  记忆沉淀看 POPUP 提醒层的“记忆提醒”；工具字段、权重和回执纪律以 provider-native schema、processor 回执为准。

  ## 工具三轴
  - 只读工具：file_read、file_glob、file_grep、memory_content_read、container_read、relation_read。
  - 同步工具：memory_write、memory_link_update、relation_card_write。
  - 焦点工具：container_focus；同一反应迭代谨慎保持单焦点。

  ## 四容器自觉
  - DC 辩证链：理解推进/判断修正，新 MEM 订正旧 MEM。
  - EC 事件链：事件经过/打断/恢复，向后看。
  - PRJ 项目：多步任务/专项整理，向前看。
  - FUT 未来：预测性判断，二段跳（预测 -> 验证）。

  ## 退出
  - 过程性用户可见进展直接输出自然语言；Runtime 会记为 assistant_text，它不是终点。
  - 如果只是想继续执行，直接调用合法工具；不要把自然语言说明当作工具结果。
  - 注意：完成时直接自然语言回复用户；这段话就是用户最终看到的回复。不要只写“收束本轮”，先把该回应的话自然说完。
  - 需要下一轮继续时，调用 reaction_finalize(handoff_text)，写清下一轮继续做什么；可与最后一批工具同次提交，Runtime 会最后结算并置位 continue_requested。
  - blocked 不是模型出口；只有 Runtime 蓝屏类事故才派生 blocked。
  - 不允许无声明悬挂；完成就自然回复，跨轮继续才 handoff。

历史反应收束指南（已退役）：

> 当前是历史反应收束纠偏阶段。普通完成不走这里；如果确需跨轮继续，只能单独调用 `reaction_finalize(handoff_text)`。不得调用读取、写入、自然语言进展或其他工具。

最终回复指南：

> 当前是最终回复阶段。这里生成用户可见的自然语言最终回复；不调用工具，不提交表单，不再执行新动作。语气保持平直。

善后步指南：

> 当前是善后步。UPSP 是工具驱动系统；裸文本是非法输出。必须调用 `cleanup_finalize` 提交善后结果。

回忆重整指南：

> 当前真实召回了已经正式入库、但因长期未调用而日衰减到低于原权重目标层的记忆。回忆重整是本轮必须完成的即时事务；它暂时覆盖其他指南前台。不得取消、跳过、延后、降权、最终回复或中继。可以继续使用只读检索工具核验证据，但其他指南提交会被拒绝。
>
> 必须调用 `guide_submit`，使用当前卡片给出的 `guide_id`、`item_id=memory_reconsolidation_due`、`option_id=submit_memory_reconsolidations`，并在 `fields.results` 中覆盖全部当前待办 ID。每项只提交 `mem_id`、纯语义正文 `semantic_content` 和最终关键词 `final_keywords`。
>
> 有充分正文或原始证据时，只恢复证据能够确认的事实与细节；当前证据不足时，保留仍能确认的主体与事件，并明确时间久远、哪些细节已经模糊。不得凭空补写，也不得只写一句空泛的“记不清”。需要精确日期、原话、轻量事实或多跳关系时，可按 `created_instance_id + created_round` 追溯创建分身原始语料，再提交重整结果。
>
> 重整正文会恢复到记忆 immutable weight 对应层，但字数上限只是边界，不是扩写目标；无需为接近上限而补齐、重复或编造。Full 最多 2048 字、Summary 最多 512 字、Abstract 最多 128 字。关键词必须由正文或已核验证据支持，规范化后不得重复；Full 1–8 个、Summary 1–6 个、Abstract 1–4 个。允许恢复压缩时丢失但证据支持的关键词。每条由处理器独立验收；看清回执里的 `completed_ids` 与 `remaining_ids`，未通过项须留在当前指南中纠正。

记忆写入重写指南：

> 当前 Round 有 `memory_write.body` 超出其冻结权重上限。原调用没有写入任何记忆；Runtime 已冻结合法的标题、权重、主体、关键词、感受与来源坐标，并把原正文作为本轮资料显示。该即时事务完成前不得最终回复、中继或提交其他指南，也不得直接重试 `memory_write`。
>
> 必须调用 `guide_submit`，使用当前卡片给出的 `guide_id`、`item_id=memory_write_rewrite_due`、`option_id=submit_memory_write_rewrites`，并在 `fields.results` 中覆盖全部当前 `rewrite_id`。每项恰好一次：要写入时填 `action=rewrite` 和不超过原上限的纯语义正文；确实不应写时填 `action=not_written` 且正文为空。不得借重写修改已冻结字段，不得合并多条候选。
>
> 字数上限只是容量边界，不是目标篇幅；无需为了接近上限而扩写、补齐或重复。保留原正文中的耐久事实、主体、时间、范围、否定与不确定性，删除重复、对话噪声和过程流水；禁止截断或补造。看清回执中的 `completed_ids` 与 `remaining_ids`，只纠正仍待处理项。

最近缓存压缩指南：

> 最近缓存已经越过当前三步模型共同可用窗口的压力线。本轮起手步已经正常完成；从当前 Reaction Frame 开始，必须先处理这项即时压缩，完成前不得提交其他指南、最终回复或中继。只处理卡片列出的当前分片；原始 `lately` 在整次压缩达标前保持不变。
>
> 调用 `guide_submit`，使用卡片给出的 `guide_id`、`item_id=cache_compaction_due`、`option_id=submit_cache_compaction_batch`。`fields.results` 可覆盖当前批次中的一项或多项：每项填写 `shard_id`；需要压缩时填 `action=replace` 与不超过该分片 `summary_limit` 的 `semantic_content`，允许空正文表示删除；确实不应改写时填 `action=keep`、正文留空并给出非空 `reason`。遗漏表示尚未处理，重复或未知 ID 会被拒绝。
>
> 按交互段和材料中的原位置指针理解上下文关系。保留决定、事实、时间、约束、否定、未决点、工具结论与因果联系，删除重复回执、机械字段、过程噪声和已经失效的中间状态；不得发明原文没有的事实。最近受保护交互的用户输入原文由 Runtime 单独保留，不要在摘要里机械复写。每片上限是硬边界而不是扩写目标；若保护原文本身使全局目标不可达，Runtime 会以受保护下限闭合并提示用户调整保护数量。

记忆语义压缩节律指南：

> 该指南只在日志已经成功写入且 Runtime 挂出冻结的“记忆语义压缩材料”批次时出现。必须调用 `guide_submit`，使用当前 `guide_id`、`item_id=memory_compression_due`、`option_id=submit_memory_compressions`，并在 `fields.results` 中覆盖全部且仅当前批次 ID。每项只提交 `mem_id`、`semantic_content` 与 `retained_keywords`。
>
> 每条记忆必须独立压缩，不能合并、拆分或遗漏。只保留该条源正文已经存在的事实，不引入其他记忆、推断或新结论；保留主体、对象、事件、时间、地点、因果、结果、限制、否定、条件、范围、不确定性和轻量事实，删除重复、对话噪声、修辞和过程描述。`semantic_content` 只能是纯语义正文，不带标题、ID、层级标签、Markdown 标题或 HTML 控制注释。Summary 最多 512 字，Abstract 最多 128 字；精确边界允许，禁止截断。
>
> `retained_keywords` 只能从该条材料列出的当前关键词选择，不得创造新词。Summary 保留 1–6 个，Abstract 保留 1–4 个。优先保留压缩后仍有检索价值的主体与别名、独特对象或事件、地点、时间锚、结果和关键约束，不得只留“记忆、用户、事情”等泛词。返回顺序不表达权重；Runtime 会按原 tags 顺序和原拼写落盘。任一条为空、超限、重复、含未知词或正文非法时，整批拒绝；按回执纠正同一批，不得越过到周志。

## 三、提醒

提醒只提示注意力，不替模型做裁决，也不证明动作已经成功。

记忆提醒：

> 每次 reaction provider 请求都显示同一张精简卡，不读取工具结果或记忆状态决定是否出现。
> 本轮若出现会影响以后判断、行动、协作、关系或自我理解的真实非噪音主体更新，请主动考虑 `memory_write`，不要等待用户要求。资料正文由 material/最近缓存承载；`dialogue_progress` 只用于用户可见进展，不是私有笔记或记忆替代。只沉淀稳定变化和可复用判断，不抄资料、不写工具流水；轻量变化可使用 `weight=1/2`。若没有主体更新，或用户/任务禁止长期记忆，则不写。只有 `MEM-*` 回执才算写入成功。

身份提醒：

> 本轮外部输入没有明确自己的身份；先根据上下文判断是否需要确认或询问。

身份确认提醒：

> 身份未知或超时时，先基于本轮上下文自然确认对象；无法确认时，先不做高影响动作，并在最终回复中简短询问。

### relation_registration_reminder
- tier: reminder
- kind: relation_registration_reminder
- decision_required: false
- message: |
  当前交互对象为陌生关系；如需沉淀关系或记忆，请优先创建新的关系卡。

工具族提醒：

> 区分协议工具、通用工具和基座工具；不要把外部动作和内部记账混成一个工具。

关系与容器提醒：

> 出现关系判断、关系材料或可复用内容时，先确认对象和正文证据，再决定是否写入关系、记忆或容器。

## 四、中继正文不在 POPUP

POPUP 不再承载交接文本模块。终端工具要求和步骤规则属于指南或警告；跨轮继续正文属于 `runtime.relay_intents` 隐藏 payload，不属于 POPUP，也不属于 model-visible 当前缓存正文。

`reaction_finalize.handoff_text` 是跨轮中继自然语言出口。Runtime 将合法中继正文登记到 `state.base.runtime.relay_intents[]`，生成 `relay_intent_id` 并置位 `continue_requested`；模型可见层只展示当前中继目标卡和中继意图指针，当前轮善后步不读取这段自由文本。

旧 `received_handoff`、`上环交接`、`待处理交接` 和 `HANDOFF｜交接` 可见层均已退役。善后步输入来自 cleanup 临时材料、结构化回执和 Runtime pending metadata，不接收反应步自由文本交接。

## 五、警告

警告在 POPUP 末尾，必须先看失败事实，再决定纠偏。

时间警告：

> 反应事务超过 20 分钟时，时间警告必须使用明确收束语气：务必立即进入收束优先，停止扩张新任务，只保留收束所必需的执行、验证和证据补齐；完成就直接自然语言回复用户，确需跨轮继续才单独调用 `reaction_finalize(handoff_text)`。不要在可见警告里解释“工具面不会收窄”这类 Runtime 实现细节。

原生工具调用警告：

> 上一次 provider-native 工具调用失败、被拒绝或无效。不要声称成功；先看失败原因和下一步纠偏动作，再重新提交合法工具调用或停止重试。
> `arguments_json`、`actual`、`expected` 中的命令、参数、正文、密钥和活体状态只能脱敏或截断展示，不得完整回显。

工具通道卫生警告：

> assistant_text 中出现疑似工具载荷、DSML 或 JSON 工具调用时，只提醒纠偏：工具参数必须走 provider-native 工具通道；不要把工具调用载荷写进自然语言正文。Runtime 不执行正文里的伪工具调用，不从正文恢复参数，也不放宽 native tool JSON 校验。

内部事件名 `native_tool_result` 对应这张警告卡；可见标题使用“原生工具调用警告”，不展示内部事件名、字段清单或调用参数。

结构警告：

> 当前输出或工具参数结构非法。setup / cleanup 阶段裸文本是非法输出；reaction loop 阶段自然语言会被记为 `assistant_text`，无阻断时可作为最终回复候选。需要继续执行就调用合法工具；完成就直接自然语言回复用户；需要跨轮继续才单独调用 `reaction_finalize(handoff_text)`。

安全警告：

> 当前材料触发安全裁决。只对该来源做放行或驳回，不把普通身份提醒、工具失败或缓存摘要误判为安全事件。

工具失败、非法字段、未知工具、权限拒绝、能力门禁拒绝和 provider trace 缺口都只能作为纠错卡存在；它们不是工具入口、不是成功回执，也不是 now/lately/Corpus 语料块。
