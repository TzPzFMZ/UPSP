---
id: audit-tools
title: Protocol Center
page: audit
summary: The runtime ledger, rules, and documents share one read-only protocol entry point backed by real Runtime data and registry allowlists.
sourceRefs: notes/06_UPSP协议工具总览与候选清单.md; OS/persona/docs/protocol/base/tools.md
---

# Protocol Center

The GUI must never turn a visual control into a privileged path around UPSP protocol gates.

- Provider-native schemas constrain tool names and arguments.
- Processors, guards, receipts, and audits form the material action chain.
- Read tools only retrieve and assemble material.
- Sync tools may request structured writes, but every write requires a receipt.
- A focus tool occupies the workbench focus and must remain exclusive.

The GUI reads real Round JSONL projections for Frames, tool results, receipts, and settlement. Container focus changes still pass through the existing processor and receipt path. Runtime send and relay still pass through the localhost host and CLI; the interface never writes live persona sources directly.

The conversation groups tool calls and results between natural-language messages into a compact two-level disclosure. Settlement and receipt details remain in the Runtime Ledger. Provider envelopes, hidden reasoning, and inferred evidence are never displayed.

The Protocol Center has three tabs:

- **Runtime Ledger** lists structured events from the retained FIFO rounds in original projection order. Event details consume only `content_md`; valid `json` fences can be viewed as recursive field tables while the original JSON remains copyable.
- **Rules** lists the 20 entries registered by `rules_registry.json`, preserving its categories and empty groups. Registry `_version` is historical metadata, not the current DDS version.
- **Documents** merges the 28 registrations in `docs_registry.json` into 24 unique source documents and displays their registered uses as tags.

Rule and document IDs come only from registry allowlists. Their Markdown uses the shared safe renderer; only Runtime Ledger JSON receives the table view. Evidence export serializes the selected existing projection as `seed_gui_evidence_export.v1` and never triggers a provider or invents missing receipts.

Manuals, Runtime events, rules, and documents share the same detail dialog, focus trap, Escape handling, and focus restoration. A failed read shows an explicit error and read-only retry, never fake fallback content.
