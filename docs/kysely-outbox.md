# Kysely in a managed PostgreSQL outbox transaction

This optional integration gives an outbox callback a typed Kysely query builder on the **same native PostgreSQL client** used for its outbox appends. Kysely does not open another pool or start a second transaction. The existing native outbox implementation owns BEGIN, COMMIT, ROLLBACK and client release.

## Installation and registration

Use the selected application's Kysely (qualified with 0.29.5) and PostgreSQL driver alongside the library tarball. Kysely is an optional peer behind `better-nest-mq/kysely`; importing the package root or PostgreSQL integration alone does not require Kysely. Internal better-effect/MQ/adapter packages remain normal dependencies installed by the library.

Enable `postgres({ pool, schema: 'mq', namespace: 'application', outbox: true })` in MqModule and register the queue as usual. Run explicit schema migrations before starting. No new connection option or automatic migration is introduced by the Kysely factory.

## Use inside a Nest Service

```ts
import { Injectable } from '@nestjs/common'
import { MqOutboxService } from 'better-nest-mq'
import { kyselyOutbox } from 'better-nest-mq/kysely'
import type { Generated } from 'kysely'
import { ReportsQueue } from './reports.queue.js'

interface Database {
  report_requests: {
    id: Generated<string>
    tenant_id: string
    status: string
  }
}

@Injectable()
export class ReportsService {
  constructor(
    private readonly outboxes: MqOutboxService,
    private readonly reports: ReportsQueue
  ) {}

  create(tenantId: string) {
    return kyselyOutbox<Database>(this.outboxes, 'primary').transaction(async (tx) => {
      const request = await tx.db
        .insertInto('report_requests')
        .values({ tenant_id: tenantId, status: 'pending' })
        .returning('id')
        .executeTakeFirstOrThrow()

      const job = await this.reports.generate.prepare({ requestId: request.id, tenantId })
      await tx.append({ id: `report:${request.id}`, job })
      return request
    })
  }
}
```

The domain table/default ID generation and the ReportsQueue schema belong to the application. The callback result is returned only after the native transaction commits. `transaction(entry, callback)` and `transaction(entries, callback)` also support prevalidated entries appended after the callback. Use `tx.append` when generated domain IDs are needed first.

The factory is inert. A Kysely instance created for a callback is only a compiler/executor view over the already checked-out native client, not a new database runtime or pool. Existing application repositories must receive/use this scoped `tx.db`; repositories that use an unrelated Kysely instance execute outside this transaction.

## Ownership and failure rules

`tx.db` exposes queries, not transaction ownership. Nested transactions, streaming and manual driver destruction are rejected. Derived builders/instances share the same guarded driver, so obtaining a clone does not acquire an independent connection or permission to commit. SQL is trusted application code: never issue manual BEGIN/COMMIT/ROLLBACK, transaction-ending procedures or non-transactional side effects through raw SQL. This is not a SQL sandbox.

Database/driver and append failures prevent commit even when the callback catches the rejection. Operations already admitted by the driver are drained before completion. Await your Kysely operations: detached compilation, plugins and application promises that have not reached the driver are not automatically owned by the transaction. Compiler/plugin/business errors caught entirely by application code do not themselves poison native SQL; propagate such errors when they should force rollback.

An escaped `tx`, builder, derived database or append function cannot start new SQL after the callback finishes. This also applies after a failed callback. Do not cache the scoped builder in a singleton Service. The library does not retry the callback, including after a lost commit acknowledgement. Existing outbox duplicate/idempotency, publication retry and at-least-once rules remain unchanged.

Business results keep the native pg parser behavior, including custom parsers. Adapter JSON uses its separate private view on the same client. Kysely result types are SQL typing, not runtime schema validation of database rows. Affected-row counts follow Kysely's bigint result conventions. The driver does not promise server-side query cancellation; admitted SQL still drains before release.

## Deliberate boundary

This is a **library-managed transaction bridge**, not an adapter for an arbitrary already-open `Kysely.Transaction`, Prisma transaction or TypeORM EntityManager. It does not assert that a transaction variable belongs to the configured source. External transaction enrollment, savepoints, streaming and other databases need separate ownership/rollback designs and are not silently simulated.

## Verification

The source suite uses the real Kysely compiler/driver interfaces and tests parameterized SQL, affected-row counts, failed and escaped operations, derived instances, forbidden ownership operations and draining. The installed PostgreSQL consumer records the physical backend PID and transaction ID for both domain insert and outbox append, checks pre-commit invisibility, preserves custom parsers, verifies rollback after caught errors, publishes scalar/null jobs after recreating the application, and confirms the borrowed pool remains usable. The package-consumer matrix compiles with TypeScript 6/7 and executes under Node/Bun.

Kysely documentation consulted: its public Driver/DatabaseConnection interfaces and PostgreSQL dialect components. No private Kysely executor or driver internals are used. The library version remains 0.0.0 until a separately authorized publication.
