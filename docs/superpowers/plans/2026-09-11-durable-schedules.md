# Durable Schedules Implementation Plan

> **For agentic workers:** Use superpowers:executing-plans and verify each task before reporting completion.

**Goal:** Deliver Nest-decorated persistent schedules using the existing scheduler and PostgreSQL store.

**Architecture:** Queue job-property metadata is compiled and schema-validated before startup. An opt-in schedule store shares the existing native pool and raw namespace; a coordinator manages definition validation/deployment and upstream scheduler lifecycle within the same runtime.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6.0.0, pinned better-effect 0.14.0 / better-effect-mq 0.1.2 / PostgreSQL adapter 0.1.3, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-11-durable-schedules-design.md

## Global constraints

- Work on feat/durable-schedules; integrate only after exact-head CI passes.
- Internal engine packages remain normal dependencies, not consumer-installed peers. No new runtime/pool per feature or process-global acquired resources.
- No automatic migrations, silent memory fallback, timer-per-replica enqueue or replacement occurrence algorithm.
- Preserve raw connection identity and JSON/null corrections. Scheduled work never silently loses a required dispatch key.
- Use a coordinated reconcile writer; ordinary replicas validate and omissions are not removed. No cross-store rollback promise.
- Retain TS6/7, Bun, installed Node/Bun consumers, real PostgreSQL and exact 20-file tooling provenance.

## Task 1 — Baseline and published contract inspection

- [x] Observe the Schedule export regression fail against the original 210-test baseline.
- [x] Inspect installed schedule, store, registry and lifecycle contracts and the real PostgreSQL namespace/tick implementation.

## Task 2 — Metadata and compilation

- [x] Add repeatable metadata, identity/inheritance checks, immutable JSON inputs and schema/codec compilation before resource writes.
- [x] Validate cron/interval/timezone, unknown fields, accessors, timer bounds and the pinned catch-up maximum.
- [x] Reject missing job metadata, conflicting addresses, inherited relative job delay and unsupported per-key/dispatch-key schedules explicitly.
- [x] Preserve TypeScript input inference on dynamic upsert and negative type tests under both compilers.

## Task 3 — Resource and lifecycle integration

- [x] Add opt-in PostgreSQL schedule resources using the same native pool, private JSON view and raw stable JobStore namespace.
- [x] Add raw/operation schedule aliases and a lazy upstream scheduler inside the existing runtime.
- [x] Warm and validate stores/definitions before clients and consumers activate; keep scheduler role independent of workers/publisher.
- [x] Test missing opt-in, preflight failure without writes, drift without overwriting and draining an admitted tick before store release.

## Task 4 — Administration and distributed qualification

- [x] Expose typed upsert, contract-scoped get/list/pause/resume/remove, validate/reconcile reports and local status/sweep through facade Promises.
- [x] Verify preservation of pauses, unchanged revisions/cursors and omitted dynamic definitions.
- [x] Run two independent Node scheduler processes against the installed package and PostgreSQL, proving occurrence fencing and later worker processing.
- [x] Cover scalar/null/object/array JSON, Date codecs, IANA timezone, run-once/catch-up/skip, overlap, pause/resume and source ownership.
- [x] Retain the existing real PostgreSQL JSON, controls, transaction/outbox and pool-failure qualification matrix.

## Task 5 — Review and final integration gate

- [x] Inspect the implementation and diff for resource ownership, namespace stability, lifecycle ordering, unsafe options and public type/dependency leakage.
- [x] Run the complete quality gate after hardening: run 34607821597 passed on 3c4ed5874c963120aba94fd11ad7ab3663f159e2 with 242 tests, zero failures, TypeScript 6.0.3/7.0.2, Oxfmt, type-aware Oxlint, build, publint and installed Node/Bun PostgreSQL consumers.
- [x] Update supported/pending status in the guides and document the exact pinned protocol limitations.
- [x] Retire temporary source/doc patch helpers and remove the branch-only write-enabled workflow. Retained CI remains read-only.

The exact cleanup commit must pass the retained Node 22/24 and PostgreSQL CI matrix before merge. Its result and final merge identity are recorded in PR #8; previous green commits are not a substitute for that integration gate.

## Explicit limitations and carry-over

The pinned schedule protocol has no dispatchKey, so derived-key/per-key destinations are rejected rather than silently weakening grouping. Use an unkeyed coordinator that publishes keyed work. Catch-up is capped at 256 per tick, not globally across replicas. Skip advances every observed due slot without emitting; it has no lateness threshold. Declaration writers and live changes of incompatible queue policies require coordinated deployment.

Schedules are persistent, but emitted jobs still have at-least-once delivery. Pausing/removing a schedule does not cancel already-emitted work. No cross-store atomic configuration rollout or arbitrary persisted function is promised. Dynamic admin get/check/mutate sequences assume coordinated writers, not tenant authorization.

Flows, external ORM transaction bridges, other adapters, custom retry providers and MQ enhancer/event integration remain separate work. No dependency version update, required consumer peer, new persisted job envelope, npm release or production deployment is included.
