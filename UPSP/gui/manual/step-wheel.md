---
id: step-wheel
title: 三步轮
page: run
summary: 三步轮把每轮运行拆成起手、反应和善后三个阶段，避免模型裸文本直接变成物质动作。
sourceRefs: UPSP_Base_DDS.md §38; OS/persona/rules/protocol/base/reaction.md
---

# 三步轮

UPSP Base 的一轮工作被拆成三个阶段：

- `setup` 起手步：读上下文、做挂载建议、确认轮型和安全入口。
- `reaction` 反应步：生成回复、调用工具、操作工作容器、提交 provider-native 终端表单。
- `cleanup` 善后步：整理训练材料、压缩最近缓存、结算必要回执。

GUI 首屏需要让用户知道当前处在哪一步、为什么停在这里、是否有待处理的回执或阻塞。

三步轮不是装饰仪表。它应该回答：

1. 当前阶段是什么。
2. 这一步看见了哪些上下文。
3. 这一步调用了哪些工具。
4. 这一步产生了哪些回执。
5. 后续是否需要进入善后或等待单写者。
