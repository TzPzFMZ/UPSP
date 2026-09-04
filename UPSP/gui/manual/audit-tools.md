---
id: audit-tools
title: 协议中心
page: audit
summary: 动态账本、规则与文档共用一个只读协议入口；任何展示都不能绕过 Runtime 真账与 registry 白名单。
sourceRefs: notes/06_UPSP协议工具总览与候选清单.md; OS/persona/docs/protocol/base/tools.md
---

# 协议中心

UPSP 的 GUI 不能把图形按钮变成绕过协议闸门的特权入口。

重要边界：

- provider-native schema 约束工具名和参数形状。
- processor / guard / receipt / audit 才是物质动作链。
- read tool 请求读取和装配材料；工具明示的确定性召回生命周期仍由 Runtime 执行。
- sync tool 修改位格内环境，必须经 processor 验收并留下回执。
- action tool 操作宿主或外部环境，继续受权限与审批约束。

模型只看到上述三种工具姿态；`execution_route` 是 Runtime 内部路由字段，不赋予模型额外权限。当前 GUI 已读取真实 Round JSONL 的 Frame、tool-result、receipt 与结算投影；Runtime send/relay 仍必须经过 localhost 宿主与 CLI，界面不直接写 live persona 真源。

主对话按 Round 的真实首现顺序排列模型明确返回的思考片段、轮中进展、每个工具调用、最终回复与善后事件。每个工具调用都是时间线上的独立节点，结果与审批在原节点更新；参数和结果展开查看。步骤结算与 receipt 仍留在协议中心的“动态账本”；界面不展示 provider 原始信封，也不从普通正文猜测 reasoning。

协议中心只有三个标签：

- **动态账本**：打开标签时才按需读取所选 retention Round 的轻量事件目录，点击单条事件后才读取其正文。轮次选择器先显示最新轮，再按轮号倒序显示历史轮；用户选择旧轮后，轮询不会抢回最新轮，只有该轮被 retention 淘汰时才回落。账本按 `event_index` 升序展示，不在首屏携带完整工具结果、provider 请求体或上下文正文。事件详情中的合法 `json` 围栏可切换为字段表格和递归详情；折叠的原始 JSON 仍可复制。隐藏推理不会伪装成普通事件正文。
- **规则**：只列出 `rules_registry.json` 当前登记的 20 项规则，完整保留 `permanent / passive_read / step_level / periodic / on_demand` 分类与空分类。Registry `_version` 只标作历史版本，不冒充当前 DDS。
- **文档**：把 `docs_registry.json` 的 28 条用途登记按路径合并为 24 份正文；同一路径的 `inject / lookup / popup / persona` 用途作为标签共同展示。

Rules 与 Docs 的条目 ID 只从 registry 白名单生成，宿主不接受任意路径、目录枚举或未登记 Markdown。两类正文继续使用统一富 Markdown 管线；它们自己的 JSON 围栏保持普通代码块，只有 Runtime 动态账本启用 JSON 表格。

“导出当前证据”只在用户点击时按需读取当前选择轮并序列化为 `seed_gui_evidence_export.v1` JSON。它不会触发 provider、不会补造缺失 receipt，也不会把静态设计页变成 Runtime 证据。

Manual、Runtime 事件、Rule 和 Doc 复用同一个详情弹窗、焦点陷阱、Escape 关闭和关闭后焦点恢复。正文读取失败时只显示明确错误与只读重试，不回落为伪正文。

宿主断开时可以手动“重新连接”。该动作只重读状态，不会重发 send、relay 或任何写请求。

当前位格的进展／最终回复、工具结构化正文与所有 GUI Manual 共享同一套富 Markdown 渲染器；用户输入始终按字面文本显示。原始 HTML 会被拒绝，远程图片默认只显示域名与替代文本，必须由用户在当前文档内主动放行，刷新后重新锁定。
