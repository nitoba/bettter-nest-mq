# Engine Lifecycle Implementation Plan

> For agentic workers: use the executing-plans workflow and verify each task before claiming completion.

**Goal:** Deliver the real private runtime and named storage lifecycle for the approved Nest facade.

**Architecture:** Inert adapter descriptors build named JobStore layers inside one application-context runtime. A root-owned host performs validation, acquisition, readiness and rollback; a public Promise-only service exposes sanitized state. PostgreSQL is the first optional production integration.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6, better-effect 0.14.0, better-effect-mq 0.1.2, PostgreSQL adapter 0.1.3, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-10-engine-lifecycle-design.md

## Global constraints

- No public Effect/Result/Layer/Runtime types, no implicit in-memory fallback.
- One runtime per application context; borrowed pools remain caller-owned.
- Validate migrations at startup without applying them.
- Work on feat/engine-lifecycle and retain read-only CI after development helpers are removed.
- Generate bun.lock with Bun and run TypeScript 6/7 plus packed consumer tests.

## Task 1 — Regression baseline and dependency inspection

Files: tests/unit/engine-public-api.test.ts and design documents.

- [ ] Verify the existing 83 tests and observe the new readiness API regression fail against M1.
- [ ] Inspect the actual published engine, adapter, runtime disposal and named-store signatures before wiring them.

## Task 2 — Connection descriptors and host

Files: src/connections, src/engine, module configuration and registry.

- [ ] Add tests for inert descriptors, missing connections, unsupported capabilities, contract-only mode, startup atomicity, concurrent start/close and closed-state rejection.
- [ ] Implement opaque connection declarations and validated copied configuration.
- [ ] Implement one private runtime, explicit registry initialization, named store warmup, readiness snapshots, cleanup aggregation and ownership-aware disposal.
- [ ] Add tests proving the same real runtime serves multiple named stores and separate Nest contexts do not share runtime state.

## Task 3 — PostgreSQL slice

Files: src/integrations/postgres.ts, tests/postgres, integration workflow.

- [ ] Test explicit migration setup and startup validation against a real PostgreSQL service.
- [ ] Test borrowed pools remain usable, owned pools close, failed startup releases acquisitions, named namespaces stay isolated and durable jobs survive context shutdown/recreation.
- [ ] Add the optional PostgreSQL factory and deploy helpers using the upstream adapter rather than reimplementing queue SQL.
- [ ] Verify no database connection is created by declaring decorators or importing the root.

## Task 4 — Package verification and delivery

Files: package.json, bun.lock, build configuration, packed consumers and docs.

- [ ] Pin engine compatibility, preserve optional drivers and verify declarations do not expose internal engine types.
- [ ] Run the complete quality gate and real database tests; fix observed failures with regression tests.
- [ ] Update implemented/pending status and usage documentation without advertising producer/worker/flow/outbox features.
- [ ] Remove branch-only generation helpers, verify final read-only CI, and deliver the new PR with exact results.
