# Implementation roadmap

## Current delivery

M0 foundations, M1 typed contracts/Nest registration and the M2 engine/storage lifecycle are implemented. PostgreSQL is the first production adapter slice. The public library still lacks producer and worker execution APIs; it is not yet a feature-complete queue library.

README.md, docs/contracts.md and docs/connections.md describe the exported surface. Planned APIs below are not exported as stubs.

## M0 — Foundation — implemented

Bun-generated lockfile, unchanged upstream Oxlint/Oxfmt/plugin with integrity checks, TypeScript 6 compatibility alongside the primary compiler, Nest dynamic modules, ESM/declarations, actual packed-consumer tests and read-only CI.

## M1 — Typed contracts — implemented

Inert QueueService/JobDefinition properties, Queue/Job/Retry/JobTimeout decorators, durable versioned identities, immutable policy resolution, Standard Schema input/output/failure inference, explicit codecs and optional Zod integration. Real Nest discovery validates contracts atomically, supports aliases and standard module re-exports, and isolates application contexts.

## M2 — Engine and connection lifecycle — implemented

One private better-effect runtime per configured application context, named JobStores, protocol/capability validation, sanitized readiness/probes and ownership-aware shutdown. Acquisition failures roll back locally instead of relying on Nest hooks after failed bootstrap. Concurrent start/close and cleanup failures have regression coverage.

PostgreSQL uses the existing adapter, validates schema without auto-migration, preserves borrowed pools, manages owned pools including idle-client errors, and offers explicit deployment migration/validation helpers. Real-server tests prove durable persistence across contexts and the connection-name component of storage identity. Packed optional integrations run under Node and Bun.

The engine versions are pinned for compatibility. The runtime exists, but compiling complete M1 job schemas/policies into executable engine Job/Worker bindings belongs to M3; that functionality is not claimed by the lifecycle delivery.

## M3 — Publication and workers — next

Compile typed descriptors/codecs and resolved policies into actual engine jobs. Implement enqueue, bulk enqueue, lookup, result waiting, prepare and administrative primitives. Bind Worker/Process decorators to real Nest providers and create an explicit attempt execution context.

Preserve input-versus-decoded values, stable IDs, failure classification, retry/timeout/cancellation, heartbeat, leases, stalled recovery and local concurrency. A waiting timeout must not implicitly cancel a durable job. Separate producer-only, worker, scheduler and outbox-publisher roles. Request-scoped handler dependencies need an attempt-local DI context; contracts stay singleton/static.

Enforce global/per-key concurrency and rate limits in storage, not local semaphores. Lifecycle-managed store access must remain inside the private runtime's admission/drain boundary.

## M4 — Other adapters and transaction bridges

The PostgreSQL JobStore lifecycle slice is available. Add MySQL, Redis/Valkey, MongoDB and Node SQLite wrappers, keeping native drivers optional. Reuse current adapter contracts and resource ownership; run real-driver conformance/failure tests. Equal method names do not erase differences in transaction, topology or bulk-operation guarantees. Extend PostgreSQL flow/event/schedule/outbox resources when their features are implemented.

## M5 — Durable flows and schedules

Bind decorated fan-out/collect Services to the existing persisted flow model: stable child manifests, bounded children/depth, paginated results, failure/cancellation semantics and cross-connection capability checks. Parents waiting for children must not occupy worker slots.

Add persistent schedules with cron/interval, time zones, misfire/overlap handling, administration and rolling-deployment-safe reconciliation. Test concurrent replicas and restart windows. No in-memory Promise.all substitute or timer-per-replica scheduling.

## M6 — Transactional outbox

Implement prepare/transaction/append/publish APIs and routing using actual domain transaction resources. Add adapter-specific contexts and separately tested ORM bridges. Preserve domain rollback, publish-after-commit, replay after enqueue-before-ack, conflicting ID detection, independent publication/job retries and borrowed ownership. The upstream outbox dependency used by the PostgreSQL package is not an implemented Nest outbox facade.

## M7 — Operations and hardening

Add worker registry, job/flow/schedule/outbox administration, durable event cursors, observability and operational examples. HTTP management must be opt-in/authenticated. Run crash/restart, concurrency, shutdown and rolling-deployment suites before production release. Do not promise exactly-once external effects or cross-database atomicity.

## Dependencies and parallel work

```text
M0 → M1 → M2 → M3
           └──→ M4 adapter wrappers
                M3 + required adapters → M5 flows/schedules
                M3 + transaction bridges → M6 outbox
                M3 + M4 + M5 + M6 → M7
```

Adapters can proceed independently once their resource/capability contract is stable. Flows and schedules can proceed in parallel after worker primitives exist. Do not split lease/settlement/outbox protocol changes across independent tasks before agreeing and testing their common contracts.
