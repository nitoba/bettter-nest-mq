# Producers and Workers Implementation Plan

> For agentic workers: use executing-plans and verify each deliverable before claiming completion.

**Goal:** Execute real typed jobs through Nest Services and the existing queue engine.

**Architecture:** Compile inert contracts and Nest handlers into upstream Job/Worker descriptors. Bind public Promise clients to one private runtime and keep ownership/lease logic in the existing supervisor.

**Tech Stack:** Bun 1.4.2, Nest 12, TypeScript >=6, pinned better-effect 0.14.0 / better-effect-mq 0.1.2, unchanged Oxlint/Oxfmt.

**Spec:** docs/superpowers/specs/2026-09-10-producers-workers-design.md

## Constraints

No simulated queue operations, second runtime, implicit memory fallback, public engine types, weakened tooling, automatic migrations or npm release. Work on feat/producers-workers and integrate only after verification.

## Implemented tasks

- [x] Observe the new execution export regression fail against M2 while the original 106 tests pass. Inspect the installed Job/Worker/Codec/Retry/Runtime signatures.
- [x] Test and implement validated enqueue/decoded enqueue, idempotency, prepare without publication, batch validation, poll/attempts and identity-scoped mutations.
- [x] Test real decorated workers with Nest DI, explicit argument mapping, known failures versus defects, retry/timeout, local concurrency and producer-only mode. Compile lazy Worker layers into the existing runtime.
- [x] Test fresh-context scoped dependencies and invalid registrations. Reject duplicate processors, unregistered references, accessors and unsupported HTTP enhancer metadata before acquisition.
- [x] Test wait timeout/abort without cancelling durable jobs, detached clients, active cancellation and shutdown draining. Add safe local worker snapshots and awaitIdle.
- [x] Observe and fix inherited processor loss and ambiguous reuse of one descriptor for multiple identities. Keep parameter metadata attached to the implementation actually invoked.
- [x] Add real PostgreSQL end-to-end packed consumers: producer exit, later worker startup, successful decoded result, retries/ledger, known failure, pending cancellation and persisted outcomes after both contexts close.
- [x] Run the complete gate with the original 20 tooling hashes, both TypeScript compilers, Node 22/24 and actual packed Node/Bun consumers. Fix lint without weakening rules.
- [x] Update execution, contract, connection, architecture and contributor documentation with accurate implemented/pending boundaries.

## Observed verification

Read-only CI run **34546429931** passed at **a3968386bf8293824a8b18d9eaec3c03e3be0960**: 132 tests, TypeScript 6.0.3 and 7.0.2, Node 22/24 quality gates and the real PostgreSQL integration/package job. The initial regression, lazy Effect-program type mismatch, factory-provider discovery, default defect policy, active cancellation, inherited processors and ambiguous descriptor cases were corrected against observed failures rather than by changing expected behavior silently.

The retained package tests install a tarball outside the workspace with optional peers first absent, then present. PostgreSQL execution runs under both Node and Bun with both TypeScript compilers. The exact upstream lint/format configuration is unchanged.

## Final integration gate

The temporary branch-only formatter workflow must be removed after documentation formatting. The resulting final commit must pass the same retained read-only CI matrix before merge; a previous green commit is not a substitute. No npm release or repository setting change is part of this milestone.

## Explicit carry-over

This delivers M3 core execution, not full parity. Remaining M3.1 work includes distributed controls, named custom retry providers, an explicit MQ enhancer pipeline and durable-event waits. Broader crash/lease-loss and adversarial worker-activation rollback tests remain release-hardening work; current tests establish store acquisition rollback, normal worker activation, invalid registrations before acquisition, active cancellation and admitted-handler draining. Flows, schedules, other adapters and transactional outbox remain separate roadmap items.
