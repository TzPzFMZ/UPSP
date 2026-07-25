---
id: content-window
title: 内容窗口三通道
page: context
summary: 当前内容窗口只区分 focus、resident_list 和 instant_list；定期层不是内容窗口常驻清单。
sourceRefs: UPSP_Base_DDS.md §19; Spec297; OS/persona/rules/protocol/base/workbench.md
---

# 内容窗口三通道

当前内容窗口只区分三条路：

- `focus`：工作台焦点。最多一个，用于当前可编辑容器正文。
- `resident_list`：常驻清单。跨轮只读正文，持续到取消。
- `instant_list`：即时清单。本轮或本步临时挂载材料。

这三条路都属于高频层 CONTENT 的可见材料，不等同于定期层。

GUI 上的含义是：用户应该能看见当前页面为什么有这些材料、它们来自哪条通道、是否只是本轮临时可见、是否具备写入权限。
