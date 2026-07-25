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
- read tool 只读取和装配材料。
- sync tool 可声明结构化写入，但必须有回执。
- focus tool 占用工作台焦点，同一时刻必须克制。

当前 GUI 已读取真实 Round JSONL 的 Frame、tool-result、receipt 与结算投影；容器焦点变更仍必须经过既有 processor 和 receipt，Runtime send/relay 仍必须经过 localhost 宿主与 CLI，界面不直接写 live persona 真源。

主对话只把工具调用与工具结果按自然语言边界合并成一条两级原生折叠轨迹；第一次展开是中文摘要，第二次才显示投影提供的结构化代码。步骤结算与 receipt 留在协议中心的“动态账本”，不再占据主体对话；界面不展示 provider 原始信封或隐藏推理内容，也不从自然语言猜测缺失证据。

协议中心只有三个标签：

- **动态账本**：从浏览器已经持有的 retention Round 投影列出事件目录。轮次选择器先显示最新轮，再按轮号倒序显示历史轮；用户选择旧轮后，轮询不会抢回最新轮，只有该轮被 retention 淘汰时才回落。账本不重新排序卡片：viewer 按 `event_index` 升序读取 Round JSONL 并投影结构化卡片，同一事件产生的多张卡保持投影生成顺序。事件详情只消费 `content_md`，其中合法 `json` 围栏可切换为字段表格和递归详情；折叠的原始 JSON 仍可复制。`content_raw`、provider 信封与隐藏推理不进入弹窗。
- **规则**：只列出 `rules_registry.json` 当前登记的 20 项规则，完整保留 `permanent / passive_read / step_level / periodic / on_demand` 分类与空分类。Registry `_version` 只标作历史版本，不冒充当前 DDS。
- **文档**：把 `docs_registry.json` 的 28 条用途登记按路径合并为 24 份正文；同一路径的 `inject / lookup / popup / persona` 用途作为标签共同展示。

Rules 与 Docs 的条目 ID 只从 registry 白名单生成，宿主不接受任意路径、目录枚举或未登记 Markdown。两类正文继续使用统一富 Markdown 管线；它们自己的 JSON 围栏保持普通代码块，只有 Runtime 动态账本启用 JSON 表格。

“导出当前证据”只把当前选择轮的既有结构化投影序列化为 `seed_gui_evidence_export.v1` JSON。它不会请求额外文件、不会触发 provider、不会补造缺失 receipt，也不会把静态设计页变成 Runtime 证据。

Manual、Runtime 事件、Rule 和 Doc 复用同一个详情弹窗、焦点陷阱、Escape 关闭和关闭后焦点恢复。正文读取失败时只显示明确错误与只读重试，不回落为伪正文。

宿主断开时可以手动“重新连接”。该动作只重读状态，不会重发 send、relay 或焦点写请求。

当前位格的进展／最终回复、工具结构化正文与所有 GUI Manual 共享同一套富 Markdown 渲染器；用户输入始终按字面文本显示。原始 HTML 会被拒绝，远程图片默认只显示域名与替代文本，必须由用户在当前文档内主动放行，刷新后重新锁定。
