---
id: memory-bus
title: Memory Bus
page: mem
summary: MEM is the bus and storage layer for memory entries; work containers mount entries through linked_containers.
sourceRefs: UPSP_Base_DDS.md; notes/20_UPSP_GUI分身实例与容器工作台备忘录_20260604.md
---

# Memory Bus

MEM is not another folder. It is the bus and storage layer for memory entries.

One entry may be referenced by several work containers. Its significance is better described by its mounts, recent context assembly, receipt and audit trail, and whether it is producing new work or long-term deposits than by a self-counted recall number.

The memory page exposes entries, mounts, lifecycle, context visibility, and audit pointers. Its main surface stays a lightweight index; selecting a memory entry opens the shared detail dialog with complete metadata. Structured fields use a two-column table, long summaries wrap, and remaining content uses Markdown. Runtime truth remains read-only.
