---
id: context-layers
title: Ten Context Layers
page: context
summary: Context Review shows ten real call-header and frequency-layer projections; Markdown improves reading without generating ledger facts.
sourceRefs: UPSP_Base_DDS.md §19.2; OS/data/round_live_viewer.py; OS/data/audit_store.py
---

# Ten Context Layers

Context Review shows ten projections for the selected Frame:

- `00_call_header`: call and endpoint metadata.
- `01_tool_header`: tool contracts visible to this call.
- `02_generation_config`: generation settings.
- `10_permanent`: persona contract and long-term rules.
- `20_periodic`: periodic projections.
- `30_lately`: recent continuity material.
- `40_high_freq`: indexes, compact tool context, and CONTENT.
- `50_now`: current interaction, materials, and tool facts.
- `60_statusbar`: independent status-bar layer.
- `99_popup`: the final GUIDE, reminder, and warning layer.

Layered machine truth lives in `layers/*.json`; the sole request body actually sent is `step.json.request_body`, while Round JSONL retains call and settlement event projections. The initial view loads only the Frame catalog. Ten-layer content and the manifest are fetched only after the user selects a current or historical Frame, and the frontend retains only that open detail. A detail failure stays local to Context Review and can be retried. Valid call-header JSON is rendered as recursive field tables with the original JSON retained. Tool headers show a compact summary and an individual detail dialog for each real tool description and parameter schema. Other layers prefer `content_md`.

Rendering only improves reading. It never parses Markdown back into Runtime facts or rewrites source content.
