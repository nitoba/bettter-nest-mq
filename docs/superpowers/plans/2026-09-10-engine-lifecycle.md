# Engine Lifecycle Implementation Plan

> For agentic workers: use the executing-plans workflow and verify each task before claiming completion.

**Goal:** Deliver the real private runtime and named storage lifecycle for the approved Nest facade.

**Architecture:** Inert adapter descriptors build named JobStore layers inside one application-context runtime. A root-owned host performs validation, acquisition, readiness and rollback; a public Promise-only service exposes sanitized state. PostgreSQL is the first optional production integration.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6, better-effect 0.14.0, better-effect-mq 0.1.2, PostgreSQL adapter 0.1.3, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-10-engine-lifecycle-design.md

## Global constraints

- No public Effect/Result/Layer/Runtime types, no implicit in-memory fallback.
- One runtime per configured application context with one root registration; borrowed pools remain caller-owned.
- Validate migrations at startup without applying them.
- Work on feat/engine-lifecycle and retain read-only CI after development helpers are removed.
- Generate bun.lock with Bun and run TypeScript 6/7 plus packed consumer tests.

## Task 1 — Regression baseline and dependency inspection

Files: tests/unit/engine-public-api.test.ts and design documents.

- [x] Verify the existing 83 tests and observe the new readiness API regression fail against M1.
- [x] Inspect the actual published engine, adapter, runtime disposal and named-store signatures before wiring them.

## Task 2 — Connection descriptors and host

Files: src/connections, src/engine, module configuration and registry.

- [x] Test inert descriptors, missing connections, unsupported capabilities, contract-only mode, startup atomicity, concurrent start/close and closed-state rejection.
- [x] Implement opaque connection declarations and validated copied configuration.
- [x] Implement one private runtime, explicit registry initialization, named store warmup, readiness snapshots, cleanup aggregation and ownership-aware disposal.
- [x] Test that one session resolves distinct named stores and separate application sessions never share stores.
- [x] Exercise close after an actual scoped acquisition has started, then verify ordered rollback when that acquisition finishes.

## Task 3 — PostgreSQL slice

Files: src/integrations/postgres.ts, tests/postgres, integration workflow.

- [x] Test explicit migration setup and startup validation against a real PostgreSQL service.
- [x] Verify borrowed pools remain usable, owned pools close and failed startup releases acquired resources.
- [x] Verify namespace isolation and actual durable records after recreating the application context.
- [x] Add the optional PostgreSQL factory and deploy helpers using the upstream adapter rather than reimplementing queue SQL.
- [x] Verify inert declarations and root imports without optional database dependencies installed.
- [x] Document and test that the named connection token is part of the upstream persisted namespace.
- [x] Observe a packed Node consumer crash after terminating an idle pg client, then fix owned-pool error handling and pass that fault test under Node and Bun.

## Task 4 — Package verification and delivery

Files: package.json, bun.lock, build configuration, packed consumers and docs.

- [x] Pin engine compatibility, preserve optional drivers and check every declaration chunk for leaked engine imports.
- [x] Run the complete quality gate and real database tests; fix observed failures without weakening lint or compiler settings.
- [x] Update actual/pending status and usage documentation without advertising producer/worker/flow/outbox features.
- [x] Remove the temporary branch-only generation workflow; retain read-only Node 22/24 and PostgreSQL CI jobs.

The read-only CI run 34543433177 passed for a002bc9bdba8b3e11ecb112ee425f8e8cb6ea843: 106 tests, TypeScript 6.0.3 and 7.0.2, exact upstream tooling integrity, formatting, type-aware lint, build, publint and actual packed consumers. Its separate PostgreSQL job passed persistence, ownership, rollback and packed public integration with deliberate idle-client termination. The final workflow-removal/documentation commit must pass that same retained matrix before merge.

## Scope carried into M3

The real runtime and named stores are implemented. Compiling complete typed job schemas/policies into executable producer/worker bindings is the next milestone, not a hidden stub in M2. Public enqueue, handler execution, retry execution, flows, schedules and transactional outbox are still absent. No npm release or repository setting changes are part of this delivery.
