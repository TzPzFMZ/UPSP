---
id: context-layers
title: 上下文十层
page: context
summary: 上下文审阅按真实调用头与频率层显示十层投影；Markdown 只负责阅读，不反向生成真账。
sourceRefs: UPSP_Base_DDS.md §19.2; OS/data/round_live_viewer.py; OS/data/audit_store.py
---

# 上下文十层

上下文审阅固定显示当前 Frame 的十层投影：

- `00_call_header`：本次调用与 endpoint 元数据。
- `01_tool_header`：本次调用可见的工具合同。
- `02_generation_config`：本次生成参数。
- `10_permanent`：主体契约和长期规则。
- `20_periodic`：周期性投影。
- `30_lately`：近期连续性材料。
- `40_high_freq`：索引、工具短带与 CONTENT。
- `50_now`：本轮交互、资料和工具事实。
- `60_statusbar`：独立状态栏层。
- `99_popup`：绝对末位的 GUIDE、reminder 与 warning。

分层机器真源是 `layers/*.json`，唯一实际发送体是 `step.json.request_body`，Round JSONL 保留调用与结算事件投影。页面可在当前 FIFO 保留的 Round 间倒序切换，并进一步选择该轮的 setup、每次 reaction 与 cleanup Frame。调用头与生成参数在“内容详情”中把合法 JSON 渲染为可递归展开的字段表格，并保留折叠的原始 JSON；工具头改用中文总览仪表显示权限、模式、终端工具、工具数量与传输状态，下面逐项列出工具，点击“查看详情”后在统一弹窗中显示真实 `description` 与参数 schema。其余层优先消费 `content_md`。渲染只改善阅读，不解析或改写 Runtime 事实。

页面只保留“分层导览”和“内容详情”：导览将调用头、工具头与生成参数显示为三联载具仪表，其余七层保持轻量摘要列表，并以原生折叠保留 Frame/manifest 装配详情。点击任一层后进入内容详情，使用同一安全渲染管线显示真实正文。
