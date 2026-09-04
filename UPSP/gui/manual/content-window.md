---
id: content-window
title: 内容窗口与单次材料
page: context
summary: 正文挂载只区分跨轮 resident_list 与当轮 instant_list；定向处理材料另以单次 C 轨出现。
sourceRefs: UPSP_Base_DDS.md §20; Spec781; OS/persona/rules/protocol/base/workbench.md
---

# 内容窗口与单次材料

当前正文挂载只区分两种生命周期：

- `resident_list`：跨轮常驻引用。每个 Reaction Frame 从记忆条目、工作容器或关系卡真源重读正文，持续到显式取消。
- `instant_list`：当前 Round 的内存挂载。本轮结束后自然消失，不存在活动持久文件。

编年史、记忆压缩等定向事务的参考内容使用单次可见 C 轨材料；它们不写入两份挂载清单，也不进入最近缓存或语料库。定期层仍是独立层，不等同于正文常驻清单。

容器写权不来自某个全局焦点，而来自该 Frame 起点已经装配的容器目标文件。GUI 上的含义是：用户应该能看见材料来自哪种生命周期、是否只是本轮临时可见，以及对应工具回执是否真的完成了写入。
