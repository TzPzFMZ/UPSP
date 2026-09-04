---
id: content-window
title: Three Content Channels
page: context
summary: The content window distinguishes focus, resident_list, and instant_list; the periodic layer is not a resident content list.
sourceRefs: UPSP_Base_DDS.md §19; Spec297; OS/persona/rules/protocol/base/workbench.md
---

# Three Content Channels

The current content window has three paths:

- `focus`: the single editable workbench focus.
- `resident_list`: cross-round read-only content retained until removed.
- `instant_list`: material mounted temporarily for the current round or step.

All three are visible material in the high-frequency CONTENT layer; they are not the periodic layer. The GUI should show why material is present, which channel supplied it, how long it remains visible, and whether it is writable.
