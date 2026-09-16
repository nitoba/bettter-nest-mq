# SQLite transactional outbox

SQLite can persist domain writes and prepared MQ publications in the same native transaction while keeping the public Nest API independent of `bun:sqlite`, `node:sqlite`, Effect and Result.

This integration is available from the host-specific entry point used by the application:

```ts
import { sqlite, sqliteOutbox } from 'better-nest-mq/sqlite/node'
// or better-nest-mq/sqlite/bun
```

Enable the durable outbox source on the named connection:

```ts
MqModule.forRoot({
  connections: {
    primary: sqlite({
      path: './data/jobs.db',
      namespace: 'reports',
      outbox: true
    })
  },
  execution: {
    workers: false,
    outboxPublisher: false
  }
})
```

Run `migrateSqlite()` explicitly before application startup. Enabling the outbox never runs migrations automatically and does not create a second database handle or runtime.

## Managed transaction

Prepare at least one job before entering a SQLite outbox transaction:

```ts
const prepared = await reports.generate.prepare(
  { reportId },
  { jobId: `report:${reportId}` }
)

const result = await sqliteOutbox(outboxes, 'primary').transaction(
  { id: `report:${reportId}`, job: prepared },
  (tx) => {
    tx.run(
      'INSERT INTO report_requests(id,status) VALUES (?,?)',
      [reportId, 'pending']
    )

    return tx.get<{ id: string }>(
      'SELECT id FROM report_requests WHERE id=?',
      [reportId]
    )
  }
)
```

The callback exposes only `run`, `get`, `all` and `append`. It does not expose the native Database object. Bind parameters are limited to SQLite scalar/blob values accepted by both qualified host drivers.

At least one predeclared outbox entry is required. That requirement lets the released SQLite adapter own `BEGIN IMMEDIATE`, commit, rollback and per-handle transaction serialization through its existing `SqliteOutboxTransactions.transaction` helper. The Nest facade does not implement another transaction coordinator.

Additional prepared jobs may be appended inside the callback:

```ts
const second = await reports.generate.prepare({ reportId: anotherId })

await sqliteOutbox(outboxes, 'primary').transaction(
  { id: firstId, job: first },
  async (tx) => {
    tx.run('INSERT INTO report_requests(id,status) VALUES (?,?)', [firstId, 'pending'])
    await tx.append({ id: anotherId, job: second })
  }
)
```

Already-started `append()` operations are drained before the callback can commit even if application code forgot to await them. New operations through an escaped transaction handle reject after callback completion.

## Failure and rollback rules

A SQL or outbox append failure poisons the managed transaction. Catching that error inside application code does not turn the transaction into a successful commit. Domain rows and outbox rows are rolled back together.

Manual `BEGIN`, `COMMIT`, `ROLLBACK`, `END`, `SAVEPOINT` and `RELEASE` statements are rejected. The callback cannot take ownership of the adapter-managed transaction.

The callback itself is never retried automatically. An ambiguous host/database failure is surfaced to the caller rather than replaying business SQL. The usual outbox publisher remains a separate at-least-once stage after commit.

## Publisher and worker roles

A process that performs business transactions can disable both workers and the publisher. A later application context may open the same file/namespace with `outbox: true` and enable only `outboxPublisher`. Workers may run separately and do not need the outbox resource merely to process already-published jobs.

The package qualification uses this exact lifecycle: producer commits and exits, an independent publisher process forwards the persisted records, then an independent worker process executes the jobs. Node/Bun same-host and cross-host combinations share the file and namespace.

Publication success means the durable job was accepted by its destination JobStore, not that its handler already completed. External handler effects remain at-least-once.

## Ownership and limits

The outbox store borrows the SQLite database already acquired for the named connection. File-backed connections remain owned by the MQ application lifecycle; caller-supplied native handles remain borrowed and are not closed by the library. The outbox store is disposed before an owned database is released.

`MqOutboxService.retryFailed()` remains PostgreSQL-only in the current facade. SQLite failed-publication administration, external transaction enrollment, ORM bridges and bulk/pruning administration are separate roadmap items.

This capability does not provide cross-file, cross-database or network-distributed atomicity. Keep the business write and its outbox source on the same SQLite database. SQLite remains a synchronous local embedded backend; concurrent processes rely on SQLite locking/WAL behavior rather than a network broker.

No npm publication is part of this development milestone. The repository package remains unreleased while the remaining roadmap is completed.
