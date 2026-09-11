# Transactional Outbox Implementation Plan

> For agentic workers: use executing-plans and verify each delivered boundary before integration.

**Goal:** Atomically persist PostgreSQL domain writes and prepared jobs, then publish committed outbox records through the existing engine.

**Architecture:** An opt-in outbox store shares each existing connection pool/runtime. An injected MqOutboxService exposes safe reads; postgresOutbox supplies native transaction callbacks. Publisher layers reuse the upstream outbox protocol and existing named destination stores.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6, pinned internal better-effect/MQ/PostgreSQL/outbox packages, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-11-transactional-outbox-design.md

## Global constraints

No additional runtime, implicit in-memory fallback, required engine peers, changed raw connection token, automatic migrations or npm publication. Work on feat/transactional-outbox, preserve native application query parsing, and integrate only after final read-only CI succeeds.

## Task 1 — Regression and protocol inspection

Files: tests/unit/outbox-public-api.test.ts, branch-only inspection workflow, plan/spec.

- [ ] Run `bun test` against main plus the export regression; confirm missing MqOutboxService while the 191-test baseline remains valid.
- [ ] Inspect installed OutboxStore/Publisher/Record signatures and current engine/JSON lifecycle code; record the source and destination namespace boundaries.

## Task 2 — Opt-in stores and service

Files: src/outbox/types.ts, options.ts, service.ts; src/engine/outbox.ts and connection/session/module integration.

- [ ] Add invalid publisher option, disabled-source and record-validation tests before implementation.
- [ ] Implement `postgres({outbox:true})` resources and named outbox tokens sharing native pools. Warm/probe stores before publication; add one upstream publisher layer to the existing runtime.
- [ ] Expose `MqOutboxService.get/list/counts/publisher` with safe snapshots, no leases or engine types. Preserve independent `execution.outboxPublisher` control.

## Task 3 — Transactions

Files: src/integrations/postgres-outbox.ts, postgres-outbox-transaction.ts; tests/postgres/outbox.ts and tests/types/outbox.types.ts.

- [ ] Specify atomic commit/rollback, native query parsers, multi-append and escaped-handle rejection with real PostgreSQL assertions.
- [ ] Implement tracked transaction operations using one actual PoolClient, separate native/adapter query views, poison-on-failure, rollback and exactly-once client release.
- [ ] Validate PreparedJob and stable id/routing, compare full duplicate content after locked append, and preserve callback causes without automatic retry.

## Task 4 — Durable publication qualification

Files: tests/package/consumer/outbox.ts, tsconfig.outbox.json, scripts/test-package.ts.

- [ ] Install the actual tarball with only Nest and chosen pg/Zod peers. Execute committed publication, no visibility before commit, domain rollback, separate producer/publisher/worker contexts and persistent outcomes.
- [ ] Verify replay after enqueue-before-ack and source/target routing with deterministic job IDs, no second handler execution for the same queued record.
- [ ] Exercise scalar/null codec payloads, key conflicts and custom native JSON parsers. Rerun existing real PostgreSQL/distributed/JSON suites.

## Task 5 — Documentation and delivery

Files: docs/outbox.md, README.md, roadmap/architecture/dependencies/AGENTS and CHANGELOG.

- [ ] Document actual Service/factory syntax, publisher-role separation, transaction restrictions, business-callback idempotency and pending ORM bridges.
- [ ] Run `bun run check` and retained PostgreSQL CI; remove branch-only generation workflow and reverify the final head before merging.
