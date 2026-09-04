---
id: base-serial
title: Base Serial Model Routing
page: settings
summary: Seed remains frame-serial while each phase may use a different model and fallback chain.
sourceRefs: LocalAppData/UPSP/config/models.json; Documents/UPSP/personas/<PID>/OS/config/model_routing.json; AGENTS.md
---

# Base Serial Model Routing

Seed still runs `setup → reaction → cleanup` serially, and one Frame calls only one model from its effective chain. This does not mean the UPSP installation can configure only one model.

## Two settings layers

- Global settings → Model service manages connections, shared keys, and reusable model profiles. Multiple profiles may share one connection and key.
- Persona settings → Model routing selects the models used by setup, reaction, and cleanup for the current persona.
- The global model truth lives in Windows `LocalAppData\UPSP\config\models.json`; the current persona route lives under the Documents known folder at `UPSP\personas\<PID>\OS\config\model_routing.json`. Runtime, CLI, and GUI share the existing `ConfigStore`, without a persona-local override layer or second secret store.

## Three-by-three routing

Each phase has a primary model and two explicit backups. A blank reaction primary inherits setup; a blank cleanup primary inherits the effective reaction primary. Backup slots never inherit. The UI shows explicit selections, inheritance sources, and the final fallback order.

When cross-phase failover is enabled, explicit backups remain first and only effective primaries from other phases fill empty slots. Duplicate model IDs and identical URL/model/key fingerprints are removed. Each model receives at most three requests and each phase resolves at most three distinct models, for a hard ceiling of nine requests.

An environment key overrides the local-file value. Settings reads, UI, logs, and evidence expose only whether a key exists and never return the key body. Global settings remain available without a key or a reachable model service.

The local desktop entry point is `python tools/serve_seed_gui.py --open`. It is a stdlib localhost host and browser entry point, not a background installer; closing its terminal stops the service.

The right side shows only real Round and Frames projections. The model catalog and persona routes stay in settings instead of creating extra work units in the runtime overview.
