# Implementation roadmap

The project bootstrap is a foundation, not a feature-complete queue integration. All milestones below require working implementations and tests before APIs are exported.

## M0 — Project foundation

Bun lockfile and scripts; the exact upstream Oxlint/Oxfmt/custom plugin; strict TypeScript with a 6.x compatibility check; Nest dynamic module configuration; ESM build and declarations; unit, Nest integration and packed-consumer tests; CI and contribution documentation.

## M1 — Typed contracts and schemas

Implement QueueService, inert job descriptors, queue/job/retry/timeout decorators, stable identity and versioning, Standard Schema input/output inference, explicit codecs, known failure contracts and configuration precedence. Add compile-time invalid-contract cases and transformation/encoding regression tests. No producer API may silently accept an unregistered contract.

## M2 — Engine bridge and connection lifecycle

Pin compatible better-effect, better-effect-mq and outbox versions at implementation time. Add one private runtime host per Nest application context, named store registration, capability validation, ownership, controlled migration validation and ordered shutdown. Test failed acquisition rollback, repeated close, borrowed/owned resources and isolation between application contexts.

## M3 — Publication and workers

Compile contracts to engine jobs, implement enqueue/bulk/query/wait/prepare and administration primitives. Discover actual Nest providers, validate registrations, bind @Process methods and implement the execution context. Cover async/await return values, failure normalization, retry/timeout/cancellation, heartbeat, lease loss, stalled recovery, local concurrency and request-scoped providers. Separate producer, worker, scheduler and publisher roles.

## M4 — Optional storage integrations

Implement PostgreSQL, MySQL, Redis/Valkey, MongoDB and Node SQLite wrappers against the current adapter contracts. Reuse native connection resources; keep driver dependencies optional. Run adapter conformance suites and real-database tests. Validate topologies, transaction capabilities, bulk semantics and distributed controls without changing promises across adapters.

## M5 — Flows and schedules

Bind decorated fan-out/collect Services to durable flows. Cover stable manifests, bounded child counts/depth, cross-connection checks, paginated results and child failure/cancellation semantics. Separately implement persisted schedule declaration/reconciliation, cron/interval, time zones, misfire/overlap and dynamic administration. Test competing replicas and restart windows, not only successful in-memory execution.

## M6 — Transactional outbox

Implement explicit prepare/transaction/append/publish APIs and stable routing. Add adapter-specific transaction contexts and separately tested ORM bridges. Cover domain rollback, publication after commit, crash after enqueue before acknowledgement, identical/conflicting repeated IDs, independent retry policies and borrowed resource ownership. Never emulate a PostgreSQL transaction with a Redis outbox.

## M7 — Operations and hardening

Add worker registry, job/flow/schedule/outbox administration, event-cursor APIs, observability, authenticated opt-in HTTP integration if required, and operational examples. Validate package subpaths and optional drivers outside the workspace. Run crash/restart, concurrency, shutdown and rolling-deployment regression suites before a production release.

## Dependencies and parallel work

```text
M0 → M1 → M2 → M3
           ├──→ M4 (driver wrappers; integration with M3)
           └──→ M6 transaction bridges (after prepare/routing contracts settle)
                M3 + M4 → M5
                M3 + M4 → M6 publisher integration
                M3 + M4 + M5 + M6 → M7
```

M1 schema/codec tests can be developed independently from decorator metadata once the descriptor contract is fixed. M4 adapters can proceed in parallel after the connection/capability contract is stable. Flows and schedules can proceed in parallel after worker/lifecycle primitives are ready. Do not split lease, settlement or outbox transaction semantics across independent agents before their shared contracts are agreed.
