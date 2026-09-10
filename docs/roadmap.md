# Implementation roadmap

## Current delivery

M0 foundations and M1 typed contracts/Nest registration are implemented. The library is not yet an operational queue. README.md and docs/contracts.md describe the actual exported API; subsequent milestones below are implementation targets, not available methods.

## M0 — Project foundation — implemented

Bun-generated lockfile; exact upstream Oxlint/Oxfmt/custom plugin with integrity checks; strict TypeScript and a 6.x compatibility check; Nest dynamic module configuration; ESM/declaration build; genuine packed-consumer tests; CI and contribution documentation.

## M1 — Typed contracts and schemas — implemented

Inert QueueService job properties; Queue/Job/Retry/JobTimeout decorators; versioned identities; immutable policy precedence; schema-derived input/output/failure types; Standard Schema validation; explicit codecs and optional Zod subpath; distinct failure classes. MqModule.forFeature and MqRegistry discover actual Nest providers and atomically validate contracts, aliases, identities and scope. Registration has no storage or worker side effects.

Compile-time tests reject invalid types and nonexistent enqueue APIs. Runtime tests cover encoding fidelity, transformation inverses, metadata inheritance, configuration validation and application-context isolation. The package boundary is checked with actual external consumers, including a Zod-free root import.

## M2 — Engine bridge and connection lifecycle — next

Inspect and pin compatible better-effect, better-effect-mq and outbox versions at implementation time. Add one private runtime host per Nest application context, named stores, capability validation, ownership, controlled migration validation and ordered shutdown. Compile M1 identities/policies into the actual engine contracts without leaking engine types publicly.

Test failed acquisition rollback, borrowed versus owned resources, repeated close and separate application contexts. A failed Nest initialization can cause close() to rethrow before destruction hooks, so acquisition failures must clean themselves up inside the host. Do not open resources in decorators or QueueService constructors.

## M3 — Publication and workers

Implement enqueue/bulk/query/wait/prepare and administration primitives against the engine. Bind Process/Worker decorators to real Nest providers and build an explicit execution context. Cover async/await results, domain failures versus defects, retry/timeout/cancellation, heartbeat, lease loss, stalled recovery, local concurrency and request-scoped handler dependencies. Separate producer, worker, scheduler and outbox-publisher roles.

A waiting-client timeout must not implicitly cancel a persistent job. Distributed controls must be enforced by the backing storage rather than local semaphores.

## M4 — Optional storage integrations

Implement PostgreSQL, MySQL, Redis/Valkey, MongoDB and Node SQLite wrappers against current adapter contracts. Reuse native resources and keep drivers optional. Run store conformance suites and real-database tests. Validate supported topologies, transaction capabilities, bulk semantics and distributed controls. Uniform method names do not make different storage guarantees identical.

## M5 — Flows and schedules

Bind decorated fan-out/collect Services to durable flows. Preserve stable child manifests, bounded child counts/depth, cross-connection checks, paginated results and child failure/cancellation semantics. Add persisted schedules, cron/interval, time zones, misfire/overlap policies and safe reconciliation during rolling deployments. Test competing replicas and restart windows. Do not substitute an in-memory Promise.all or a timer per replica for persisted coordination.

## M6 — Transactional outbox

Implement explicit prepare/transaction/append/publish APIs and stable routing. Add adapter-specific transaction contexts and separately tested ORM bridges. Cover domain rollback, publication only after commit, crash after enqueue before acknowledgement, identical/conflicting IDs, independent retry policies and resource ownership. The actual domain write and outbox append must share the real transaction resource; do not infer atomicity from an arbitrary transaction-named argument.

## M7 — Operations and hardening

Add worker registry, job/flow/schedule/outbox administration, durable event cursors, observability and operational examples. Administrative HTTP endpoints must be opt-in and authenticated if added. Validate optional drivers and package subpaths outside the workspace. Run crash/restart, concurrency, shutdown and rolling-deployment suites before production release. No exactly-once external-side-effect promise.

## Dependencies and parallel work

```text
M0 → M1 → M2 → M3
           ├──→ M4 (driver wrappers; integration with M3)
           └──→ M6 transaction bridges (after prepare/routing contracts settle)
                M3 + M4 → M5
                M3 + M4 → M6 publisher integration
                M3 + M4 + M5 + M6 → M7
```

Adapter wrappers can proceed independently once connection/capability contracts are stable. Flows and schedules can proceed in parallel after worker/lifecycle primitives exist. Do not split lease, settlement or outbox transaction semantics across independent tasks before agreeing and testing their shared contracts.
