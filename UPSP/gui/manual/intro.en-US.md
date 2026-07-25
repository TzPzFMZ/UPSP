---
id: intro
title: What Is UPSP?
page: run
summary: UPSP is a subject system built around persona continuity, memory, work containers, tool receipts, and audit ledgers—not a conventional chat tool.
sourceRefs: AGENT_HANDOFF.md; notes/20_UPSP_GUI分身实例与容器工作台备忘录_20260604.md
---

# What Is UPSP?

UPSP is not merely about making a model remember more. It lets a persona continue operating through memory, relationships, containers, tools, and audit evidence.

The GUI begins with one principle: **the persona is here**. The real conversation remains centered while internal state opens around it only when needed.

- **Persona**: the subject currently operating.
- **Thread**: a conversation entry for the same persona. Seed currently shows one real conversation for the active persona and does not fake parallel threads.
- **Three-step round**: `setup / reaction / cleanup`, the main execution crankshaft.
- **Memory bus**: the path through which memories are mounted, deposited, and recalled.
- **Work containers**: productive organs such as DC, EC, PRJ, and SKL.
- **Receipts and audit**: traceable sources, results, and settlement for important actions.

Retained rounds form one continuous conversation. Polling follows new events only when the reader is already at the bottom; reading older content never forces a jump. Historical read failures can be retried without resending a message or replaying a round.

## Persona

Select the hexagonal persona crest to open two read-only source views:

- **Core profile** reads the complete `persona/core.md`. Its first view is an identity-registration table for names, abbreviation, PID, persona code, and three structural roles listed on numbered rows `01–03`; the English interface shows only the English half of each bilingual source value, while the Chinese interface shows only the Chinese half. The six paired axes follow. Specification and model-stamp details remain only in the disclosed source. The GUI creates no second persona source and does not rewrite source facts.
- **Vital state** strictly projects the registered Base fields in `persona/state.json`. Its instruments summarize the current round, runtime phase, dynamic axes, workhood, fatigue, token usage, identity anchor, and active flags. **Complete state** retains every registered raw value and machine path.

Neither view backfills or writes persona files. If one source fails, that view reports the failure and offers a read-only retry while the other source remains independently available.

## Local start

Run `python tools/serve_seed_gui.py --open` from the repository root. The host binds only to `127.0.0.1` on port `8770` by default.

Connected pages consume only local Runtime, store, or persona-source projections. Missing hosts, rounds, or reads never fall back to fake live data.

## Interface language

Chinese is the native product language of Seed GUI. Choose Follow system, Simplified Chinese, or English under **Global settings → Interface and language**; the choice is stored in the UPSP-wide local configuration. Locale switching changes only interface chrome. User messages, active-persona output, persona profiles, raw state values, memories, relations, containers, context, rules, documents, and JSON evidence always retain their source text.

When no live persona exists, the GUI enters a dedicated onboarding flow. You can start with the Alyosha example or fill in a custom persona profile. You may configure and explicitly test the setup model, or deliberately skip model setup and create the local persona first. After skipping, the GUI remains available for profile review and settings, but messages cannot be sent until a model service and setup route are ready.
