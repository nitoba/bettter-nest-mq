# Producers and Workers Implementation Plan

> For agentic workers: use executing-plans and verify each deliverable before claiming completion.

**Goal:** Execute real typed jobs through Nest Services and the existing queue engine.

**Architecture:** Compile inert contracts and Nest handlers into upstream Job/Worker descriptors. Bind public Promise clients to one private runtime and keep ownership/lease logic in the existing supervisor.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6, pinned better-effect 0.14.0 / better-effect-mq 0.1.2, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-10-producers-workers-design.md

## Constraints

No simulated queue operations, second runtime, implicit memory fallback, public engine types, weakened tooling, automatic migrations or npm release. Work on feat/producers-workers and merge only after verification.

## Tasks

- [ ] Observe `execution-public-api.test.ts` fail against M2 while the existing tests remain green. Inspect the installed Job/Worker/Codec/Retry/Runtime signatures before implementation.
- [ ] Add producer tests for validated enqueue/decoded enqueue, idempotency, prepare without publication, batch validation, poll/attempts and identity-scoped mutations. Implement facade DTOs/errors, runtime bindings and contract compilation.
- [ ] Add decorated worker tests with actual Nest DI, explicit argument mapping, known failures versus defects, retry/timeout, local concurrency and producer-only mode. Compile Worker layers into the existing runtime; do not create an independent supervisor.
- [ ] Add fresh-context scoped-provider and invalid-registration tests. Reject duplicate processors, unregistered contract references, accessors and unsupported HTTP enhancer metadata before acquisition.
- [ ] Test bounded/aborted waiting without cancelling durable jobs, detached clients, worker quiescence and rollback after activation failure. Add safe local worker status/awaitIdle.
- [ ] Add real PostgreSQL end-to-end and packed consumer tests: successful result, retry ledger, producer-only persistence across recreated contexts, known failures and cancellation.
- [ ] Run `bun run check` plus PostgreSQL integration, preserving TypeScript 6/7, Node 22/24, Bun and the exact tooling baseline. Fix observed failures with regressions.
- [ ] Document implemented APIs and explicit limitations; remove temporary branch-only generation tooling and verify retained read-only CI before merge.
