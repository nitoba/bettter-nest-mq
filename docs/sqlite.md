# SQLite job storage and event waits (Node and Bun)

SQLite provides the ordinary JobStore and opt-in event-assisted result waits through the existing Nest producers/workers. It reuses the released better-effect-mq-sqlite adapter as a normal internal dependency. Application code does not install or configure that adapter, Effect or Result.

## Native entry points

Use sqlite, migrateSqlite and validateSqlite from `better-nest-mq/sqlite/node` for Node's DatabaseSync, or from `better-nest-mq/sqlite/bun` for Bun's Database. Select the entry point for the actual execution host. The package root loads neither host driver. Bun is still the package manager; the application chooses its execution host.

```ts
import { Module } from '@nestjs/common'
import { MqModule } from 'better-nest-mq'
import { sqlite } from 'better-nest-mq/sqlite/node'

@Module({
  imports: [
    MqModule.forRoot({
      connections: {
        primary: sqlite({ path: './data/jobs.db', namespace: 'reports', events: true })
      }
    })
  ]
})
export class MessagingModule {}
```

Register your Queue Services with forFeature and Worker providers normally. Their decorators, Promise operations, schemas/codecs and DI do not change. The directory must exist and be writable; the library does not provision directories. Omit events or set events:false when this application does not need an event reader.

## Explicit migrations and ownership

Run migrations separately before starting the application:

```ts
import { migrateSqlite, validateSqlite } from 'better-nest-mq/sqlite/node'

await migrateSqlite({ path: './data/jobs.db' })
await validateSqlite({ path: './data/jobs.db' })
```

The migrator returns a frozen component/version/applied report. Repeating it with the current layout applies nothing. It is the upstream complete migration set: the presence of tables for optional components does not enable unimplemented Nest resource bundles. Startup validates the schema and never invokes the migrator. Native writer activation bookkeeping may be initialized lazily by the upstream job writer; enabling an event reader alone neither creates that optional activation table nor promotes writers to required mode.

A file path creates a fresh owned native handle at application acquisition, never at decorator/factory time. It uses WAL and defaults to foreign_keys and busy_timeout configuration, and closes only after worker and store cleanup. Acquisition failure closes an owned handle as well. Relative paths resolve against the working directory at declaration; use the same absolute file, connection name and namespace across processes. Symlink/hardlink aliases are not universally detected.

To borrow a handle, create and explicitly migrate a native DatabaseSync/Database, then pass sqlite({ database }). It is neither frozen nor closed. Its pragmas are preserved unless configurePragmas:true is explicitly supplied. busyTimeoutMs requires pragma configuration; pollIntervalMs controls native job wake polling and the opted-in event reader's polling for commits made by other local processes. Defaults are 5000 ms and 1000 ms respectively; values are bounded to safe native timer/integer ranges.

Owned :memory: paths and URI filenames are rejected. An explicitly created/migrated borrowed in-memory database is supported for ephemeral workloads/tests, but it is not durable across restarts and is never an automatic fallback. Do not use the borrowed MQ handle inside an unrelated long-lived application transaction; native queue operations own their SQL transactions.

## Event-assisted result waits

Enable events:true in the application waiting for a result. The existing job API then accepts the same strategy as PostgreSQL:

```ts
const id = await reports.summarize.enqueue({ values: [10, 20, 30] })
const result = await reports.summarize.awaitResult(id, {
  strategy: 'events',
  pollFallbackMs: 5_000,
  timeoutMs: 30_000
})
```

This snippet uses the ReportsQueue contract from README.md. `execute(input, { wait: { strategy: 'events', pollFallbackMs: 5000, timeoutMs: 30000 } })` combines normal publication and waiting; it never invokes a local handler directly. Explicit codecs, scalar strings and JSON null retain their ordinary job semantics. See [event waits](event-waits.md) for the shared options and error types.

The reader borrows the already-acquired native database and joins the same private application runtime. Its token derives from the raw named JobStore token; the in-runtime operation alias does not create another durable namespace. Producer, worker and reader processes must use the same file, connection name and namespace. A worker may keep events:false: that flag disables only its reader, not the pinned adapter's native event writes when the event layout is present.

Event records are wake hints, never a substitute for authoritative job results. The engine rereads persisted job state and decodes the registered result/failure schema. A result completed before registration remains readable. Lost hints, runtime reader SQL failures and cursor expiry retain the existing bounded fallback behavior; a missing reader or failed startup probe rejects instead of claiming readiness. No global or per-wait checkpoint is acknowledged by this API.

Caller timeout and abort terminate only that wait; they do not cancel durable work. Explicit job cancellation remains a separate operation. Shutdown aborts/drains admitted waits and disposes native event waiters before releasing the owned database. Borrowed handles remain usable after closure. There is no forced interruption of synchronous SQLite SQL or external handler side effects.

The opt-in adds no retention configuration, log pruning, required-writer activation, replay subscription or event administration API. A read-only reader startup preserves existing history and activation state. It does not guarantee that other writers or administrators retain events indefinitely. Do not interpret an ephemeral result-wait cursor as a durable subscription checkpoint.

Native SQLite event waits poll the event log using pollIntervalMs and retain job polling through pollFallbackMs. This is not a zero-polling, push-only, immediate-notification or reduced-query-load guarantee. Concurrent waits consume queries and synchronous host time; measure the selected intervals for the workload.

## Host and TypeScript compatibility

The installed-package tests execute actual SQLite files under Node and Bun with TypeScript 6.0.3 and 7.0.2. They start new worker processes, including Node producers/readers with Bun workers and Bun producers/readers with Node workers, against the same persisted file and logical namespace. This checks interoperability between the tested native hosts, not simultaneous multi-host network access.

Node's earliest declared 22.12 floor needs --experimental-sqlite. The native Node test commands pass that flag; the compatibility matrix uses the maintained Node 22 and 24 lines rather than claiming every historical patch release was executed.

Node/root declaration consumers use @types/node and are compiled before any Bun typings are installed. The Bun SQLite fixture installs @types/bun and checks the unmodified `bun-types/sqlite.d.ts` with skipLibCheck still false. Its configuration deliberately does not load unrelated Bun global augmentations:

```json
{
  "compilerOptions": { "types": ["node"] },
  "include": ["src/**/*.ts", "node_modules/bun-types/sqlite.d.ts"]
}
```

This is the targeted strict declaration boundary used for SQLite qualification, not a replacement declaration file. Loading the full pinned Bun 1.4.1 ambient types together with Node 22.20.2 typings exposed unrelated TextEncoder/TLS declaration conflicts in the external fixture. No declarations were edited or fabricated, and no compiler checks were disabled to pass this integration. Applications using the full Bun global surface must select mutually compatible host typings; this increment does not claim to repair those upstream ambient-type conflicts.

## Guarantees and limits

Persistence uses the upstream named-store namespace, derived from the stable nestjs/<connection-name> token. Changing that name or namespace selects a different logical queue even in the same file. No new supervisor, lease protocol or persistence envelope is introduced. Handler effects remain at-least-once and cancellation remains cooperative.

SQLite is a synchronous, local embedded backend. Busy waits and large queries block the host thread. This does not provide a network broker, multi-host cluster database, or universal throughput guarantee. Keep files on supported local storage and qualify your operating limits before production use.

SQLite flow, schedule, outbox and ORM transaction resources remain unimplemented in the Nest facade; requesting their unknown configuration options fails. Schedules have a concrete upstream prerequisite: [better-effect#390](https://github.com/nitoba/better-effect/issues/390). The released 0.1.2 adapter reparses decoded scalar strings while cloning schedule records. Pausing a schedule whose payload is the string "null" can rewrite that column to JSON null. This was reproduced directly against the installed adapter, independently of this event-reader implementation. Do not bypass the missing schedule facade by treating extension tables as a qualified API. A corrected release needs separate persistence, occurrence and package qualification; existing corrupted payloads cannot be repaired without an authoritative original value.

Existing PostgreSQL features remain intact. Additional SQLite bundles, durable subscriptions and broader cross-process distributed-control qualification remain roadmap work. Ordinary job recovery after a killed worker does not prove every controlled-claim or flow failure window.

## Verification

Source tests use actual native SQLite databases with real Nest contexts and engine workers. They cover inert configuration, explicit migrations, no automatic startup migration, borrowed pragma preservation, owned cleanup after failed acquisition, unsupported capabilities, duplicate lexical paths and draining an admitted handler before closing the database. Event tests trace native SELECTs, compare physical job/event namespaces, exercise execute and completed-result reads, inject event-query/probe failures, check caller abort/timeout and shutdown, and verify that reader initialization preserves the native catalog, actual history and writer activation.

Installed consumers cover JSON-looking strings, scalar/null/array/object values, Date codecs, decoded enqueue, batches, idempotency, retry attempt history, pending cancellation, delayed promotion, prepare without publication, wait timeout without job cancellation, namespace isolation and persisted results after producer/worker exit. The event fixture adds concurrently running reader/worker processes and SIGKILL after a real active claim, followed by a replacement worker and persisted stalled/completed attempt history. Successful event cases use a caller timeout shorter than polling fallback so fallback alone cannot satisfy them. Both native hosts, both compilers and all four reader/worker host directions run alongside the complete pre-existing PostgreSQL package matrix. No npm publication or production deployment accompanies this repository delivery.
