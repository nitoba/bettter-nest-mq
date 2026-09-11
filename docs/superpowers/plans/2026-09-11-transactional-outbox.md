# Transactional Outbox Implementation Plan

> For agentic workers: use executing-plans and verify each delivered boundary before integration.

**Goal:** Atomically persist PostgreSQL domain writes and prepared jobs, then publish committed outbox records through the existing engine.

**Architecture:** An opt-in outbox store shares each existing connection pool/runtime. An injected MqOutboxService exposes safe reads; postgresOutbox supplies native transaction callbacks. Publisher layers reuse the upstream outbox protocol and existing named destination stores.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6, pinned internal better-effect/MQ/PostgreSQL/outbox packages, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-11-transactional-outbox-design.md

## Global constraints

No additional runtime, implicit in-memory fallback, required engine peers, changed raw connection token, automatic migrations or npm publication. Work on feat/transactional-outbox, preserve native application query parsing, and integrate only after final read-only CI succeeds.

## Task 1 — Regression and protocol inspection

- [x] Observe the missing public outbox API regression fail while the existing 191 tests pass.
- [x] Inspect the published OutboxStore/Publisher/Record and current engine/JSON lifecycle contracts.
- [x] Preserve distinct stable job and outbox storage namespaces without changing raw connection tokens.

## Task 2 — Opt-in stores and service

- [x] Add publisher-option, disabled-source and record-validation tests before implementation.
- [x] Implement postgres({ outbox: true }) resources sharing native pools and the application runtime.
- [x] Warm/probe source stores before activation and reuse the upstream lazy publisher layer.
- [x] Expose safe MqOutboxService get/list/counts/publisher views and independent outboxPublisher execution control.

## Task 3 — Native PostgreSQL transactions

- [x] Use one actual PoolClient for domain queries and outbox appends, preserving native parsers for business queries and private JSON parsing for adapter SQL.
- [x] Implement multi-entry and dynamic-append transactions, rollback, tracked-operation draining and caught-failure poisoning.
- [x] Expose only query/append on the callback facade and reject use after the callback closes.
- [x] Preserve callback errors without automatically replaying business code after query, connection or commit failure.
- [x] Validate prepared destinations/payloads/keys and compare full duplicate request, destination and publication budget after locked append.

## Task 4 — Durable publication qualification

- [x] Test actual installed tarballs with only Nest and chosen native pg/Zod peers, without consumer-declared internal engine packages.
- [x] Verify domain rollback, uncommitted invisibility, native custom parsers, multi-record/generated-ID transactions and late-handle rejection.
- [x] Verify separate producer/publisher/worker contexts, competing publishers, scalar/null payloads and persisted results.
- [x] Reproduce an abandoned source record after destination enqueue but before acknowledgement; recover it with one stable destination job and one execution attempt.
- [x] Verify active transaction disconnection rolls back without rerunning the callback, and shutdown drains an already-admitted transaction before releasing storage.
- [x] Verify missing target routes consume the independent publication budget, then persist a classified terminal failure without publishing elsewhere.
- [x] Keep existing real PostgreSQL JSON, connection, clock-fencing and independent-process distributed-control suites enabled.

## Task 5 — Additional regression and public packaging

- [x] Observe and correct the callback facade exposing implementation fields; keep only query and append publicly accessible.
- [x] Separate public transaction types from native resource modules so no engine imports appear in any public declaration chunk.
- [x] Add an observed regression for identical queue/name/version handlers across different connections in one Worker. Reject the unsupported supervisor configuration before acquisition and test separate Worker Services as the supported alternative.
- [x] Update outbox, dependency, connection, execution, architecture and contributor documentation with implemented and remaining boundaries.
- [x] Remove the branch-only generation workflow and all temporary patch scripts from the final changeset.

## Observed verification

Development run **34600522538** completed successfully after formatting the branch to **108f705ced168f5fda3582f4d11237f3bd8551af**. It ran the focused PostgreSQL transaction/publisher scenarios and the complete quality gate: **210 passing tests**, zero failures, TypeScript **6.0.3 and 7.0.2**, the exact **20 upstream tooling hashes**, formatting, type-aware lint with zero warnings/errors, ESM/declaration build, publint and installed tarball consumers.

Both compiler consumers executed under Node and Bun against PostgreSQL. The installed outbox fixtures passed rollback, visibility, parser, duplicate, competing-publisher, replay, scalar/null, disconnect, shutdown and missing-route cases. Existing distributed-control and 18-value JSON-fidelity matrices also passed. No engine or adapter package was added to consumer manifests, and dependency versions were not changed.

The final cleanup/documentation commit must pass the retained read-only CI matrix before merge. Prior development success does not substitute for verification of the exact final head. Main-branch verification follows integration separately.

## Deliberate scope boundaries

This is the first native PostgreSQL outbox slice. Arbitrary external transaction handles, ORM bridges, failed-record reset administration, other adapters, flows and schedules remain pending. Managed query SQL is trusted application code and must not issue manual transaction control. Business callbacks are not automatically retried or deduplicated. Publication remains at-least-once, and proposed IDs based on logical source aliases need globally unique outbox IDs or explicit scoped job IDs when independent source deployments converge on one target.

No npm release, automatic migration, production deployment or repository settings change is part of this delivery.
