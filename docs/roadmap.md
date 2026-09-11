# Implementation roadmap

## Current delivery

M0 foundation, M1 contracts, M2 engine/PostgreSQL lifecycle, M3 core producer/worker execution, **M3.1a distributed queue controls** and the **M6a native PostgreSQL outbox** are implemented. The library still does not provide full better-effect-mq feature parity and remains unreleased at version 0.0.0.

Current guides: README.md, docs/contracts.md, docs/connections.md, docs/execution.md, docs/controls.md and docs/outbox.md. Remaining APIs below are not exported as placeholders.

## PostgreSQL JSON fidelity — corrected

Issue #5 now has public and installed-package regressions for scalar strings, JSON-looking strings, numbers, booleans, null, arrays and objects. The adapter-only parser boundary preserves SQL NULL distinction, native application parsing and pool ownership without changing persisted envelopes. Ordinary/controlled queues, retries, typed failures, batches, preparation and post-restart reads are covered. See docs/postgres-json.md; the subsequent native outbox integration reuses this corrected JSON boundary; flows and schedules remain pending.

## M0 — Foundation — implemented

Bun-generated lockfile/scripts, strict TypeScript with independent 6.x and primary compiler checks, unchanged upstream Oxlint/Oxfmt/plugin with 20-file integrity verification, ESM/declarations, publint, real tarball consumers and read-only CI.

## M1 — Contracts — implemented

QueueService/JobDefinition, Queue/Job/Retry/JobTimeout metadata, immutable policies, versioned identities, Standard Schema input/output/failure types and explicit generic/Zod codecs. Nest discovery validates complete singleton/static queue registries and supports ordinary module re-exports without starting consumers merely by importing a contract.

## M2 — Lifecycle — implemented

One private application runtime, named stores, capabilities/protocol checks, safe probes, owned/borrowed resource boundaries and rollback. PostgreSQL delegates to the upstream adapter, validates schema without automatic migration, exposes explicit deployment helpers, and handles owned idle-client errors. Durable raw connection identities remain stable.

## M3 — Core execution — implemented

Public enqueue/enqueueDecoded/enqueueMany, prepare, poll, awaitResult, execute, attempts, cancel, retry and promote use the actual queue engine. Schemas validate every persistence boundary; wait timeout/abort does not cancel durable work. Preparation is data, not an implicit transaction.

Worker/Process/JobData/JobContext use real Nest class providers and fresh attempt contexts for scoped dependencies. The existing supervisor owns retries, timeout, heartbeat, leases, local concurrency and cancellation. Defects are not retried by default. Producer-only mode, inherited metadata and shutdown draining have regression coverage. Packed PostgreSQL consumers verify work and results across recreated producer/worker contexts.

## M3.1a — Distributed controls — implemented

QueueControls declares storage-backed global/per-key concurrency and fixed-window admission limits. Jobs derive dispatch keys from decoded payloads; publication/prepare/batches validate required keys and reject conflicting overrides before engine execution.

Replicas validate existing enabled policy read-only by default. Explicit coordinated deployment reconciles using group ownership checks, preserves unchanged revisions and ignores omissions rather than disabling policies. All adapter capabilities are checked before writes; writes across connections are not a distributed transaction.

A private operation-store view routes the existing worker protocol to controlled claims, settlement, release, cancellation and recovery. It retains raw persistence tokens and introduces neither another pool/runtime nor a local substitute for distributed limits. Actual packed tests run independent Node worker processes against PostgreSQL, auditing global/per-key active jobs, persisted rate-window admissions, permit release and revision persistence. Shared memory is only an explicit reference fixture; production code does not promote its conservative capabilities.

## M3.1b — Further execution integration — pending

Add named/versioned custom retry providers with DI and no serialized executable functions. Specify a true MQ execution-context pipeline before supporting Nest guards/pipes/interceptors/filters; current unsupported HTTP metadata is rejected. Add durable-event wakeups with precise cursor/recovery semantics. Extend crash/lease-loss and mid-flight policy revision qualification without weakening fencing or cancellation guarantees.

## M4 — Additional adapters and shared resource bundles — pending

Add MySQL, Redis/Valkey, MongoDB and Node SQLite wrappers with optional native drivers and real topology/transaction semantics. Reuse conformance/failure tests. Add required flow/schedule/event/outbox stores to connection bundles without redundant pools. Existing PostgreSQL JobStore execution and controls are available; native PostgreSQL outbox resources now share the same connection pool; corresponding resources for other adapters remain pending.

## M5 — Durable flows and schedules — pending

Bind Flow/FanOut/Collect to the existing persisted parent/children model: stable manifests, bounded fan-out/depth, paginated collection and explicit failure policies. Waiting parents must not occupy worker slots. Do not replace durable coordination with Promise.all.

Implement persistent cron/interval/timezone/misfire/overlap and safe reconciliation during rolling deployments. Test competing schedulers and restart windows. Dynamic work belongs in a coordinator job, not a serialized function or timer per replica.

## M6a — Native PostgreSQL transactional outbox — implemented

Opt-in outbox source stores share existing PostgreSQL pools and the application runtime. MqOutboxService exposes safe read models; postgresOutbox provides typed managed transactions with domain query/append on one actual client. Callbacks can append several prepared jobs, including data derived from generated domain IDs. Already-started work is drained, caught failures prevent commit, and completed handles reject further use. Native parser behavior is preserved.

The existing outbox publisher forwards committed rows using stable routes and independent publication retries. Worker and publisher enablement are separate. Full duplicate checks include destination and dispatch key, and abandoned enqueue-before-ack records are replayed idempotently. Callback replay, arbitrary external transaction handles, exactly-once effects and cross-database atomicity are not promised. See outbox.md.

## M6b — Outbox bridges and administration — pending

Add separately verified TypeORM/Prisma/Kysely or other native transaction bridges, additional adapter support, failed-record retry/reset APIs and richer operational recovery controls. Preserve real transaction identity, borrowed ownership and uncertain-commit semantics. Application outbox remains distinct from internal flow coordination.

## M7 — Operational and release qualification — pending

Extend connection/worker/control diagnostics to authenticated opt-in job/flow/schedule/outbox administration, durable cursors, observability and production examples. Qualify additional distributed failure windows and every optional entry point with installed tarballs. Define version/upgrade guarantees before publishing a production release. Never promise exactly-once external effects or cross-database atomicity.

## Dependencies and independent work

```text
M0 → M1 → M2 → M3 → M3.1a
                  ├──→ M3.1b custom retry / MQ enhancers / durable events
                  ├──→ M4 adapter and resource bundles
                  ├──→ M5 flows / schedules after their stores exist
                  └──→ M6a native outbox implemented; M6b ORM bridges remain
     M3.1b + M4 + M5 + M6 → M7 full parity and release qualification
```

Adapters and independent execution extensions may proceed in parallel after their shared boundaries are stable. Flows and schedules can be split once their store/lifecycle requirements are agreed. Lease, settlement and transaction changes require coordinated protocol tests rather than overlapping independent edits.
