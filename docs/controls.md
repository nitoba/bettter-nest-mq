# Distributed queue controls

QueueControls adds storage-backed global concurrency, per-dispatch-key concurrency and fixed-window admission rates to the existing Nest producers/workers. The limits belong to a durable queue, not to one Worker instance. The facade dispatches to the upstream controlled-store operations; it does not approximate distribution with in-process counters or reimplement the claim algorithm.

PostgreSQL is the qualified production adapter. The separate-process package tests share one PostgreSQL database while running independent worker PIDs. A shared in-memory reference fixture is used only for protocol tests and is not a supported distributed production store.

## Declare limits and a typed dispatch key

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, Queue, QueueControls, QueueService } from 'better-nest-mq'

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
@QueueControls({
  globalConcurrency: 10,
  perKeyConcurrency: 2,
  rateLimit: { max: 100, durationMs: 1_000 }
})
export class ReportsQueue extends QueueService {
  @Job({ name: 'generate', version: 1 })
  readonly generate = this.job({
    payload: z.object({ tenantId: z.uuid(), values: z.array(z.number()) }),
    result: z.object({ total: z.number() }),
    dispatchKey: (payload) => payload.tenantId
  })
}
```

This permits at most ten active jobs in this queue across all participating workers, at most two for a given tenant key across all job names/versions in the queue, and at most one hundred admissions in the current one-second fixed window. Normal Worker and Process concurrency still limit each local worker/handler. Effective admission must satisfy both local and storage-backed limits.

Controls are scoped to the configured connection and queue. Producers and workers must use the same database, schema, configured namespace and connection name. The raw `nestjs/<connection>` token remains unchanged by this feature; the internal operation-view token does not create another database namespace or pool. Different connection names remain different durable addresses.

Every supplied numeric limit must be a positive safe integer, and a declaration must contain at least one limit. Options are copied/frozen. A concrete queue subclass must declare its own controls: operational limits are not accidentally inherited when the subclass declares a new queue identity. Omission never means that a persisted policy should be deleted.

## Deployment is separate from replica startup

Normal module registration defaults to:

```ts
controls: {
  mode: 'validate',
  group: 'nestjs/queue-controls'
}
```

Validation is read-only. Each declared policy must already exist, be enabled, belong to the same group and match the declared limits. A missing or different policy fails initialization before producers bind or workers start. Adapter capabilities and the real controlled-operation set must also be present. A capability bit alone is not accepted as an implementation.

Apply policies deliberately through a dedicated deployment application:

```ts
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { MqModule, MqQueueControlsService } from 'better-nest-mq'
import { postgres } from 'better-nest-mq/postgres'
import { ReportsQueue } from './reports.queue.js'

@Module({
  imports: [
    MqModule.forRootAsync({
      useFactory: () => {
        const connectionString = process.env.DATABASE_URL
        if (!connectionString) throw new Error('DATABASE_URL is required')
        return {
          connections: {
            primary: postgres({ connectionString, schema: 'mq', namespace: 'reports-app' })
          },
          execution: { workers: false },
          controls: { mode: 'reconcile', group: 'reports-deployment' }
        }
      }
    }),
    MqModule.forFeature([ReportsQueue])
  ]
})
class PolicyDeploymentModule {}

const app = await NestFactory.createApplicationContext(PolicyDeploymentModule)
try {
  // Reconciliation already happened during startup. Reapplying identical policy is a no-op.
  const controls = app.get(MqQueueControlsService)
  console.log(await controls.get(ReportsQueue))
} finally {
  await app.close()
}
```

Run schema migrations explicitly before this deployment, using the existing migration helper. Policy reconciliation does not create database tables. Configure subsequent producer/worker applications with `controls: { group: 'reports-deployment' }`, preserving the same connection identity. They inherit validate mode and cannot call the mutating service successfully.

**Use one coordinated policy writer per deployment.** Reconcile is not a distributed deployment election, a compare-and-swap rollout protocol or protection against two authorized writers racing with different desired configurations. Do not run conflicting old/new application versions in reconcile mode. Group ownership is checked before reconciliation, but groups are not an authentication system or an atomic cross-process lock.

All adapter/ownership checks finish before the first policy write. Reconciliation is then applied per connection through the existing adapter. A database failure after an earlier connection succeeded can leave a partial policy deployment; those writes are not rolled back as a cross-database transaction. Inspect state and rerun the intended deployment. Runtime resources still roll back on startup failure.

Unchanged policies preserve their revision and rate-window state. Changed policies follow the upstream revision/window transition semantics. Omitted queues are ignored rather than disabled. Removing a decorator or omitting a queue from a deployment does not remove its existing limits. This facade currently provides no automatic policy deletion, disable or group transfer API.

## Dispatch keys and publication

A job's dispatchKey callback receives the decoded payload type, including transformed codec values such as Date. It is separate from idempotencyKey: dispatchKey groups concurrent work, while idempotencyKey deduplicates publication. Several jobs can share one dispatch key while retaining independent IDs.

New facade publications to a queue declaring perKeyConcurrency require a key. The contract may derive it, or a job without a derivation can supply `enqueue(payload, { dispatchKey })`. A caller cannot override a derived key with a different value. This prevents an accidental override from bypassing the contract's grouping rule; it is not tenant authorization, which belongs to the application.

Keys must be non-empty, have no surrounding whitespace or NUL, fit the supported 512-character protocol limit, and not equal the reserved `__none__` bucket. Invalid keys and missing required keys fail before engine execution/publication, preserving facade validation errors. Additional upstream encoding constraints still apply.

The same derivation and validation apply to enqueueDecoded, enqueueMany and prepare. Batch payload/key validation completes before any batch write. Preparation contains the resolved dispatchKey but does not publish or automatically join an outbox transaction.

Previously persisted jobs without a key, or jobs written by another producer that bypasses this facade, retain the upstream reserved-bucket semantics. Enabling per-key limits is not a migration that populates keys on old jobs. Audit/drain or migrate old work deliberately before relying on business-specific grouping.

## Reading and reconciling policies

Inject MqQueueControlsService from a module that imports the root service exports. `get(QueueClass)` returns the persisted policy for a registered queue identity, or undefined when none exists. Snapshots contain connection, queue, group, enabled, revision, limit fields and creation/update timestamps. They contain no pool, engine token, Result or Runtime.

`reconcile()` reapplies this application's declared policies and returns per-connection reports with created, updated, unchanged and record snapshots. It is rejected in validate mode. The service is explicit application functionality, not an automatically exposed HTTP admin endpoint. Apply your own authorization if exposing it through a protected administration surface.

Failures use QueueControlsException with a phase such as capability, missing, drift, ownership, mode or operation. Policy errors stop startup and release acquired runtime resources. They do not silently fall back to uncontrolled claims. Ordinary queues without declarations do not require distributed capabilities, while already-persisted enabled policies on a supported adapter are still honored by the operation dispatcher.

## Claims, leases and timing

Every controlled claim carries the persisted policy revision. The dispatcher uses the upstream controlled claim, settlement, release, cancellation and stalled-recovery operations so permit accounting remains consistent with lease ownership. Revision conflicts are errors/retryable protocol signals, never permission to fall back to plain claims. A declared policy disappearing or becoming disabled makes claims fail closed rather than run without limits.

Active cancellation remains cooperative: acceptance records a cancellation request, and the owning supervisor performs fenced settlement and releases its permit. The library does not steal leases or undo side effects. Leases limit concurrent owned attempts, not arbitrary external work that continues after a non-cooperative handler loses ownership. Keep external effects idempotent and honor cancellation where supported.

Rate limits count admissions, including new retry attempts, not successful completions or publications. They use the upstream persistent fixed-window algorithm, not a sliding-window promise or smooth pacing. Bursts near a window boundary may occur; combine a concurrency limit where simultaneous work must also be bounded. Synchronize clocks on participating hosts: this bridge does not introduce a new server-clock protocol or claim to compensate for arbitrary clock skew.

## Verification

The public package fixture creates two independent Node worker processes with different PIDs, each processing its own job names on the same PostgreSQL queues. The parent runs under Node and Bun, with TypeScript 6 and 7 compilation. Test-only audit triggers record actual claim transactions and persisted rate-window identities; they neither implement limits nor add a substitute lock.

Scenarios cover combined global capacity, per-key serialization while another key progresses, permit reuse after active cancellation, shared fixed-window admissions, zero permits after settlement and policy revision persistence after all contexts close. Existing producer/worker restart, codecs, retries, optional dependency isolation and PostgreSQL ownership tests remain enabled.

Unit/real-Nest tests cover immutable metadata, decoded dispatch keys, missing-key batches, capability/ownership/drift rejection, read-only mode, revision stability and omission safety. The shared reference memory fixture is explicitly test-only; unmodified memory capabilities are rejected for distributed declarations by production code.

## Remaining work

This delivers the distributed-controls portion of M3.1, not full feature parity or a production deployment. Named custom retry providers, MQ-specific enhancers, durable-event waits, additional adapters, flows and ORM transaction bridges remain separate milestones. Native PostgreSQL outbox transactions are documented in outbox.md. No npm package publication or environment provisioning is part of this change.

## Heartbeat clock race and dependency ownership

Controlled mutations can be rejected when a heartbeat commits a newer updatedAt after the supervisor samples now. The bridge refreshes only the upstream explicit stale-clock rejection, at most three times. It retains the job ID, original lease token and handler result, rechecks current wall time and lease validity in the store, and preserves a retry delay when moving its scheduled time. It never replays the handler or retries ambiguous writes/network failures. Deterministic memory and PostgreSQL tests verify completion, duplicate acknowledgments, active cancellation and expired/replaced lease fencing. Engine/adapter packages are now normal internal dependencies; consumers install only their chosen pg/Zod integrations as documented in dependencies.md.

## Persistent schedule integration

Persistent Schedule declarations and MqSchedulesService are now implemented; see schedules.md for the supported API and exact recurrence semantics. Schedule stores opt in with postgres({ schedules: true }), sharing the existing pool, private JSON view and stable namespace. Definition deployment/validation and scheduler execution are independent from workers and the outbox publisher.

This does not add flows or external ORM transactions. The pinned schedule protocol cannot carry dispatch keys, so keyed/per-key-limited schedules reject explicitly. Normal startup preserves operator pauses and validates deployed definitions; explicit reconciliation is a coordinated administrative operation, not a cross-store transaction.
