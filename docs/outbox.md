# PostgreSQL transactional outbox

This integration makes a domain write and an outbox append part of one actual PostgreSQL transaction. A managed publisher later forwards committed records to the configured job stores. Nest application code uses an injected Service, typed transaction methods and Promises, not engine tokens or Effect programs.

This first slice supports library-managed PostgreSQL transactions and get/list/count diagnostics. It is not an ORM transaction bridge, a cross-database transaction, a workflow engine or an exactly-once external-effects guarantee. The outbox engine and adapter packages remain normal internal dependencies; consumers need only their Nest environment and chosen native `pg` driver/types.

## Configure the source and execution roles

Enable outbox storage on a PostgreSQL connection explicitly:

```ts
import { Module } from '@nestjs/common'
import { MqModule } from 'better-nest-mq'
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
            primary: postgres({
              connectionString,
              schema: 'mq',
              namespace: 'reports-app',
              outbox: true
            })
          },
          execution: {
            workers: false,
            outboxPublisher: false
          }
        }
      }
    }),
    MqModule.forFeature([ReportsQueue])
  ]
})
export class ApiMessagingModule {}
```

The example is a domain/API process: it can publish normal jobs and append outbox records but starts neither workers nor an outbox publisher. A separate publisher application uses the same source name/schema/namespace and sets `execution.outboxPublisher: true`, keeping workers false when it should only deliver records. Worker applications can enable workers and disable the publisher. Both flags default to true, but a publisher is started only when an outbox source is explicitly enabled and workers require registered Worker providers.

The outbox shares its connection's native pool and the existing private application runtime. It does not create an extra pool or runtime. A borrowed `postgres({ pool, ..., outbox: true })` pool stays caller-owned. The outbox has a stable source-specific storage namespace derived from the configured namespace and connection name; existing job storage addresses do not change. Use the same configuration across process restarts.

Run the existing explicit PostgreSQL migration helper before startup. The shipped upstream migration set already contains the outbox table; enabling the feature never silently creates or upgrades tables. Source storage is probed before publisher activation.

## Inject the Service and use a transaction

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, MqOutboxService, Queue, QueueService, type InputOf } from 'better-nest-mq'
import { postgresOutbox } from 'better-nest-mq/postgres'

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
export class ReportsQueue extends QueueService {
  @Job({ name: 'generate', version: 1 })
  readonly generate = this.job({
    payload: z.object({ requestId: z.uuid(), tenantId: z.uuid() }),
    result: z.object({ fileKey: z.string() })
  })
}

@Injectable()
export class ReportRequestsService {
  constructor(
    private readonly reports: ReportsQueue,
    private readonly outboxes: MqOutboxService
  ) {}

  async create(input: InputOf<ReportsQueue['generate']>) {
    const jobId = `report:${input.requestId}`
    const prepared = await this.reports.generate.prepare(input, { jobId })
    const outbox = postgresOutbox(this.outboxes, 'primary')

    return outbox.transaction(
      { id: `report-request:${input.requestId}`, job: prepared, attempts: 10 },
      async (tx) => {
        // report_requests belongs to the application's domain schema and migrations.
        await tx.query(
          `INSERT INTO report_requests (id, tenant_id, status)
           VALUES ($1, $2, $3)`,
          [input.requestId, input.tenantId, 'pending']
        )
        return { requestId: input.requestId, jobId }
      }
    )
  }
}
```

`postgresOutbox(service, source)` is an inert typed factory around the injected Service. It does not acquire a resource. The default source is `default`; supply `primary` explicitly when that is the configured name. The source must be enabled for outbox storage. Prepared jobs can target a different configured connection: atomicity covers the domain row and source outbox row, while destination publication happens afterward.

For the overload above, the entry is validated before opening the native transaction. After the callback succeeds, the prepared outbox entry is appended using that same transaction client, then the transaction commits. Callback failure or append failure causes rollback. The returned value is the callback's value, delivered only after successful commit.

## Multiple records and generated domain IDs

`transaction(entries, callback)` accepts a readonly array of entries. Every prepared entry is checked before the transaction; all appends then share the callback transaction. Another overload supports appending explicitly after a generated domain ID becomes available:

```ts
const outbox = postgresOutbox(outboxes, 'primary')
const result = await outbox.transaction(async (tx) => {
  const inserted = await tx.query<{ id: string }>(
    'INSERT INTO report_requests (tenant_id, status) VALUES ($1, $2) RETURNING id',
    [tenantId, 'pending']
  )
  const requestId = inserted.rows[0]?.id
  if (!requestId) throw new Error('The database did not return the request id')

  const prepared = await reports.generate.prepare({ requestId, tenantId })
  const appended = await tx.append({
    id: `report-request:${requestId}`,
    job: prepared
  })
  return { requestId, proposedJobId: appended.record.request.id }
})
```

The returned transaction handle offers only typed `query` and `append`. It is valid only during the callback. Late query/append calls reject instead of using a released client. Already-started operations are tracked and drained before the client is released; callers should still await each operation deliberately. A query or append failure poisons the transaction even when the callback catches that rejection, so a caught SQL error cannot accidentally produce a successful commit.

Business queries use the native pool/client type parsers, including custom JSON parsers. Outbox SQL uses the library's private JSON parser view on the same client. Neither the pool's configuration nor global parsers are replaced. Row type arguments are TypeScript annotations, not runtime validation of arbitrary SQL results.

## Transaction restrictions

SQL is trusted application code. Do not issue `BEGIN`, `COMMIT`, `ROLLBACK`, transaction-ending procedure calls or other manual transaction control through `tx.query`; the library owns that boundary and does not sandbox or parse SQL. Do not acquire another pool client for a write that must be atomic with this outbox. An ordinary repository call using its own connection is outside this transaction.

This slice intentionally does not accept an arbitrary caller `PoolClient`, TypeORM EntityManager, Prisma transaction or Kysely executor. Those bridges need separate resource-identity and rollback tests. There is no `appendIn(unknownTransaction)` API pretending that a matching variable name proves atomicity.

The business callback is not automatically retried, including on a failed/uncertain commit response. A lost commit acknowledgement can leave the caller uncertain whether PostgreSQL committed; use stable domain identifiers and reconcile state before retrying. Transaction callbacks are not safe places for external non-transactional side effects that assume a future commit.

## IDs, duplicates and schemas

Each entry needs a stable outbox `id` and a prepared job. Prepared data is checked again against a destination job registered in the application, including identity/version, payload encoding and dispatch-key requirements. Preparation itself does not publish. Non-JSON decoded values must already be represented by the explicit job codec. The publisher handles encoded data, not a serialized schema function.

When the prepared request has no explicit job ID, the integration derives a stable proposed ID from source, outbox ID and destination. It remains identical during publisher retries. Explicit IDs are preserved. Job idempotency keys still apply: the destination may return an existing job for the same key, so a proposed ID is not a new-job guarantee. The publisher's `published` state means that destination enqueue was accepted, not that the handler finished or that every entry produced a distinct job.

Repeating the identical entry is a duplicate append, not a second record. Reusing its outbox ID for another prepared request, destination or publication-attempt budget fails and rolls back the transaction. The facade compares destination and dispatch key too; it does not rely only on the upstream request digest. Replay the original prepared data for intentional retries, since preparing again can change time-sensitive request fields.

Duplicate append does **not** deduplicate the domain callback: the callback still runs. Use unique constraints, appropriate conflict handling or an application idempotency record for domain writes. Replaying a duplicate does not reschedule or reset an existing outbox record. `runAtMs` controls initial publication eligibility, not a hidden update operation.

## Publisher policy and recovery

Configure the publisher separately from job retry policies:

```ts
outbox: {
  concurrency: 2,
  leaseDurationMs: 30_000,
  heartbeatIntervalMs: 10_000,
  pollIntervalMs: 100,
  retryBaseDelayMs: 1_000,
  retryMaxDelayMs: 60_000
}
```

An entry's `attempts` defaults to ten publication attempts and is independent of `prepared.request.attemptsMax`, which controls job execution. Timers are positive safe integers within the supported timer range, heartbeat is shorter than the lease, and maximum retry delay cannot be below its base delay. The current publisher exposes the upstream bounded exponential publication retry behavior rather than a second policy engine.

Only committed records are visible to publishers. Competing publishers coordinate claims through persisted leases. A crash after destination enqueue but before source acknowledgement can cause another publication attempt; it reuses the stable prepared request and destination idempotency behavior. This remains at-least-once delivery. Handlers and external effects must be idempotent where repeated execution would be harmful.

Routing uses configured destination connection names. A publisher missing a target route cannot deliver that record and records the upstream classified failure; it does not guess another database or fall back to memory. Keep all required routes present during rolling deployments. A failed record is visible through diagnostics; this first slice does not export an automatic reset/retry-failed administration method.

Shutdown stops admission and quiesces the publisher, releases its claims as appropriate, then releases stores and owned pools. It does not wait for every pending or future-dated outbox row to be published. Callback/driver cancellation remains cooperative; arbitrary non-cooperative application code is not forcibly interrupted. A completed transaction can leave a pending record safely for the next publisher process.

## Inspection and errors

Inject `MqOutboxService` and call `get(source, id)`, `list(source, { state, target, limit })`, `counts(source)` or `publisher()`. Record snapshots include state, route, encoded prepared request, publication attempts and classified failure, without active lease tokens. Publisher snapshots are local to the application; no publisher returns undefined. No automatic HTTP endpoints are installed.

`MqOutboxException` identifies configuration, preparation, availability, read, append, conflict and transaction misuse. Business callback errors retain their identity when cleanup succeeds. SQL/driver errors and error causes are trusted diagnostics and should not be serialized indiscriminately to untrusted HTTP users. Cleanup failures aggregate with the original failure instead of silently hiding it.

## Qualification and remaining features

The installed-package fixture covers domain rollback, uncommitted invisibility, native custom parsers, completed-handle rejection, conflicting destinations/keys, caught SQL failures, multiple/dynamic appends, scalar JSON/null payloads, competing publishers, independent process roles, replay of an abandoned post-enqueue record and persisted job outcomes after recreated application contexts. It compiles with TypeScript 6/7 and runs with Node and Bun against PostgreSQL. Existing dependency isolation, JSON fidelity and distributed-control tests remain enabled.

The native PostgreSQL transaction boundary is the first outbox implementation. ORM transaction bridges, additional drivers, richer administrative recovery controls, flows and persistent schedules remain separate work. No npm publication, production environment provisioning or automatic migration is included.

## Identity scope and operational qualifications

A missing destination route is classified as retryable by the pinned upstream publisher. It can recover when a later deployment supplies that route; without recovery, the record becomes failed after exhausting its publication attempt budget. The final failure can retain retryable=true even though no budget remains. This differs from an invalid prepared request, which is not made valid by retrying. The installed-package test verifies the missing-route budget independently of job execution attempts.

Automatic proposed job IDs use the logical source name, outbox ID and logical destination name. Distinct source databases/namespaces with the same logical aliases are not globally distinguished by that derivation. Use globally unique outbox IDs, such as UUIDs, or explicit globally scoped job IDs when multiple independent source deployments converge on one destination. Duplicate append within one source does not deduplicate business callbacks across sources.

The pinned Worker supervisor requires queue/name/version uniqueness inside each Worker Service even across connections. When source and destination expose the same queue/job/version, register separate Worker Services for them. This is checked before resources open; the integration does not silently split a worker and change its local concurrency budget.
