# M3.1a — Storage-backed distributed controls

The user authorized merging and continuing the previously approved architecture. PR #3 is already merged at 2482d5e413e4aa6dd92fa2a0e700a2b8fe610450 and there are no open PRs. This milestone delivers QueueControls declarations, safe policy deployment and typed per-job dispatch keys using the existing controlled store protocol, not a process-local semaphore or a new claim algorithm.

## Public surface

`@QueueControls({ globalConcurrency, perKeyConcurrency, rateLimit: { max, durationMs } })` decorates a QueueService. Limits are positive safe integers; declarations are copied/frozen and concrete queue subclasses must declare their own controls to avoid unexpectedly inheriting an operational policy. Job options gain a typed `dispatchKey(decodedPayload)` callback. The publication boundary validates the key, forbids conflicting caller overrides of derived keys, and requires a key for new jobs in per-key-limited queues. Batch validation and prepare follow the same rules before publication.

`MqModule.forRoot({ controls: { mode: 'validate' | 'reconcile', group } })` selects startup behavior. Default mode is validate and group is `nestjs/queue-controls`. Reconcile is explicit deployment authority; it must be run by one coordinated deployment process, not conflicting application versions. Runtime replicas validate declarations against persisted policy without rewriting it. Unchanged reconcile preserves revision/rate-window state and never disables omitted queues. Existing queues with no declaration are unaffected; removing a decorator never deletes persisted policy.

`MqQueueControlsService` exposes read-only persisted policy lookup by registered QueueService class and an explicit reconcile operation restricted to applications configured in reconcile mode. The service returns facade-owned Promise results/snapshots with revisions and policy fields, not engine objects. No management HTTP endpoint is installed.

## Lifecycle and safety

Validate all adapter capabilities and controlled-store methods for every declared queue before writing any policy. Before producer binding or Worker activation, reconcile when explicitly requested or compare persisted declarations in validate mode. Missing/different policies fail startup without opening consumers. A failed policy pass rolls back runtime resources; persisted successful policy writes are not rolled back across stores and must be documented as deployment effects, not a distributed transaction.

Policy drift during startup must never silently relax a stronger persisted limit. Group ownership conflicts fail even in reconcile mode. Omitted definitions are never disabled. The upstream adapter remains responsible for atomic claims, permits, fairness, rate windows, lease fencing and release/settlement. Per-key jobs already persisted without a key retain the engine's reserved bucket behavior; new facade publications require keys to avoid ambiguous grouping.

Configured workers use the same durable connection name/schema/namespace across processes. Synchronization applies before workers start and does not create another runtime/pool. Uncontrolled queues do not require controlled capabilities.

## Verification

Observe a failing public-export regression against the 132-test baseline. Add contract/metadata tests, typed dispatch-key inference tests, pure startup validation/ownership tests and runtime tests with real upstream stores. Qualify actual distributed enforcement using separate Node worker processes and a shared PostgreSQL 16 database: combined global concurrency, per-key serialization with other keys progressing, fixed-window admission rate, persistence across restart, no revision churn, explicit drift rejection, capability rejection, cancellation permit release, and normal unrelated queue behavior.

External tarball consumers compile with TypeScript 6 and 7 and run Node/Bun. No engine types leak, no optional driver imports enter the root, and all 20 upstream tooling files remain unchanged. No npm release, automatic migration, environment deployment or new infrastructure credentials are requested. Flows, schedules, outbox, custom retry providers and MQ enhancer/event integration remain separate milestones.
