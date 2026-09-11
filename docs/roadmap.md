# Implementation roadmap

## Current delivery

The foundation, typed producers/workers, PostgreSQL lifecycle and JSON fidelity, distributed controls, native PostgreSQL transactional outbox, persistent schedules and **durable flows** are implemented. The library still lacks full better-effect-mq feature parity and remains unreleased at version 0.0.0.

Current guides: README.md, docs/dependencies.md, docs/contracts.md, docs/connections.md, docs/execution.md, docs/controls.md, docs/outbox.md, docs/schedules.md and docs/flows.md. Remaining APIs below are not exported as placeholders.

## M0 — Foundation — implemented

Bun-generated lockfile/scripts, strict TypeScript with independent 6.x and primary compiler checks, unchanged upstream Oxlint/Oxfmt/plugin with 20-file integrity verification, ESM/declarations, publint, actual tarball consumers and read-only CI.

## M1 — Contracts — implemented

QueueService/JobDefinition, Queue/Job/Retry/JobTimeout metadata, immutable policies, versioned identities, Standard Schema input/output/failure types and explicit generic/Zod codecs. Nest discovery validates complete singleton/static queue registries and supports ordinary module re-exports without starting consumers merely by importing a contract.

## M2 — Lifecycle and PostgreSQL fidelity — implemented

One private application runtime, named stores, capability/protocol checks, safe probes, owned/borrowed resource boundaries and rollback. PostgreSQL delegates to the upstream adapter, validates schema without automatic migration and exposes explicit deployment helpers. Raw connection identities remain stable.

Issue #5 is corrected through the private adapter JSON boundary, preserving scalar/JSON-looking strings, null versus SQL NULL, native application parsers and pool ownership. Actual PostgreSQL and installed consumers cover regular/controlled jobs, retries, domain failures, batches and post-restart reads. Native outbox, schedules and flows reuse that boundary without changing stored job envelopes. See docs/postgres-json.md.

## M3 — Core execution — implemented

Enqueue, decoded enqueue, batches, prepare, poll, result waiting, execute, attempts, cancel, retry and promote use the actual engine. Schema validation applies at persistence boundaries; caller timeout/abort does not cancel durable work. Preparation is data, not an implicit transaction.

Worker/Process/JobData/JobContext use actual Nest class providers and fresh attempt contexts for scoped dependencies. The supervisor owns retries, timeouts, heartbeat, leases, local concurrency and cooperative cancellation. Defects do not retry by default. Producer-only mode, inheritance, identity validation and shutdown draining have regression coverage. Packed PostgreSQL consumers verify work/results across recreated producer/worker contexts.

## M3.1a — Distributed controls — implemented

QueueControls declares storage-backed global/per-key concurrency and fixed-window admissions. Dispatch keys derive from decoded payloads; required keys and conflicting overrides are checked before writes. Default replica startup validates policies read-only; coordinated deployment reconciles with group checks, unchanged revisions and omission safety. Writes across connections are not a distributed transaction.

An operation-store view routes claims, settlement, release, cancellation and stalled recovery to the upstream controlled protocol without changing raw tokens or opening another pool/runtime. Independent Node processes against PostgreSQL qualify shared limits, rate-window identities, heartbeat clock races and permit release. The internal shared memory fixture is test-only, not a production distributed store.

## M3.1b — Further execution integration — pending

Add named/versioned custom retry providers through DI without serializing functions, an explicitly specified MQ enhancer pipeline and durable-event waits with cursor/recovery semantics. Extend crash/lease-loss and mid-flight policy-change qualification. Current incompatible HTTP enhancer metadata is rejected rather than ignored.

## M4 — Additional adapters and resource bundles — pending

Add MySQL, Redis/Valkey, MongoDB and Node SQLite wrappers with optional drivers and tested topology/transaction semantics. Reuse upstream conformance/failure tests. PostgreSQL currently shares its pool among JobStore, schedules, native outbox and flows; equivalent resources for other adapters remain pending.

## M5a — Persistent schedules — implemented

Repeatable Schedule decorators declare cron or intervals, IANA timezones, JSON schema input, job-policy overrides, misfire and overlap. MqSchedulesService offers typed upsert and contract-scoped reads, list, pause/resume/remove, deployment reports and local scheduler status/sweep.

PostgreSQL schedule resources are opt-in and share the corrected JSON view, stable raw namespace and existing runtime/pool. Normal startup validates declarations; one coordinated reconcile writer deploys them. Unchanged reconciliation retains revision, operator pauses and cursor state, and omissions are never removed automatically. Scheduler enablement is independent of workers and outbox publisher. Failed startup cleans up resources; admitted ticks drain before store release.

Actual installed-package tests run two independent scheduler processes against PostgreSQL, proving one emitted job for a retained occurrence, JSON/date fidelity, timezone interpretation, bounded catch-up, skip/overlap semantics, pause/resume and worker processing after the schedulers exit. Tests compile with TypeScript 6/7 and run under Node/Bun.

Explicit limits: the pinned schedule record has no dispatchKey, so keyed scheduled jobs/per-key-limited destinations reject rather than lose their grouping. Use an unkeyed coordinator to publish keyed work. Catch-up is capped at 256 per tick, not across replicas; skip drops every observed due slot without a lateness grace threshold. See docs/schedules.md before selecting policies or changing live configuration.

## M5b — Durable flows — implemented

Flow/FanOut/Collect now bind Nest providers to the persisted parent/children model. Stable child keys, bounded fan-out, typed paged collection, explicit continue/fail policies and cooperative cancellation use the upstream FlowStore rather than process-local Promise coordination.

The PostgreSQL flow resource is opt-in through `postgres({ flows: true })` and shares the existing runtime, native pool, stable JobStore namespace and private JSON parser boundary. FanOut persists the manifest and relinquishes the original parent lease; Collect runs only after a fresh claim. Waiting parents do not hold ordinary execution capacity.

The facade keeps the native v1/v2 boundary explicit: v1 JobStore inspection is not widened to expose `waiting-children`. FlowStore/v2 supplies suspended-parent state for administration/recovery. JSON `null`, scalar strings and explicit codecs retain the same persistence rules as ordinary jobs.

Qualification uses released `better-effect-mq@0.1.3` and `better-effect-mq-postgres@0.1.4`. Installed-package PostgreSQL scenarios cover manifest persistence followed by SIGKILL, two independent replacement workers, stable child IDs, scalar/null/Date codecs, bounded pages, typed failure continuation, empty and nested flows, fail-fast and cascade cancellation. See docs/flows.md for exact guarantees and limits.

## M6a — Native PostgreSQL transactional outbox — implemented

MqOutboxService exposes safe read models; postgresOutbox manages domain query/append on one real transaction client. It supports multiple prepared jobs, draining started operations, poisoned transactions after caught failures and completed-handle rejection. Native parsers remain unchanged.

The upstream publisher forwards committed records using stable routing and independent publication retries. Worker/publisher roles are separate. Duplicate validation includes destination and dispatch key; abandoned enqueue-before-ack entries replay idempotently. No automatic callback replay, external transaction handle, exactly-once effect or cross-database transaction is promised.

## M6b — Outbox bridges and administration — pending

Add separately verified TypeORM/Prisma/Kysely transaction bridges, other adapters, failed-record reset/retry and richer recovery administration. Preserve real transaction identity, uncertain-commit semantics and borrowed ownership. Application outbox remains distinct from flow coordination.

## M7 — Operational and release qualification — pending

Extend diagnostics to authenticated opt-in administration, durable cursors, observability and production examples. Qualify remaining failure windows and every optional entry point using installed tarballs. Define upgrade compatibility before publishing a production version. Never promise exactly-once external effects or cross-database atomicity.

## Dependencies and independent work

```text
M0 → M1 → M2 → M3 → M3.1a
                  ├──→ M3.1b custom retry / MQ enhancers / durable events
                  ├──→ M4 additional adapters and resources
                  ├──→ M5a schedules + M5b durable flows implemented
                  └──→ M6a native outbox implemented; M6b ORM bridges pending
       remaining integrations → M7 full parity and release qualification
```

Independent adapters/integrations can proceed once their shared lifecycle contracts are stable. Lease, settlement and transaction changes require coordinated protocol tests rather than overlapping independent edits.
