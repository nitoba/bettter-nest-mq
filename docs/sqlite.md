# SQLite job storage (Node and Bun)

The first SQLite increment provides the ordinary JobStore and existing Nest producers/workers with an embedded local database. It reuses the released better-effect-mq-sqlite adapter as a normal internal dependency. Application code does not install or configure that adapter, Effect or Result.

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
        primary: sqlite({ path: './data/jobs.db', namespace: 'reports' })
      }
    })
  ]
})
export class MessagingModule {}
```

Register your Queue Services with forFeature and Worker providers normally. Their decorators, Promise operations, schemas/codecs and DI do not change. The directory must exist and be writable; the library does not provision directories.

## Explicit migrations and ownership

Run migrations separately before starting the application:

```ts
import { migrateSqlite, validateSqlite } from 'better-nest-mq/sqlite/node'

await migrateSqlite({ path: './data/jobs.db' })
await validateSqlite({ path: './data/jobs.db' })
```

The migrator returns a frozen component/version/applied report. Repeating it with the current layout applies nothing. It is the upstream complete migration set: the presence of tables for optional components does not enable unimplemented Nest resource bundles. Startup always validates and never creates or upgrades the schema.

A file path creates a fresh owned native handle at application acquisition, never at decorator/factory time. It uses WAL and defaults to foreign_keys and busy_timeout configuration, and closes only after worker and store cleanup. Acquisition failure closes an owned handle as well. Relative paths resolve against the working directory at declaration; use the same absolute file, connection name and namespace across processes. Symlink/hardlink aliases are not universally detected.

To borrow a handle, create and explicitly migrate a native DatabaseSync/Database, then pass sqlite({ database }). It is neither frozen nor closed. Its pragmas are preserved unless configurePragmas:true is explicitly supplied. busyTimeoutMs requires pragma configuration; pollIntervalMs controls the adapter's fallback for local cross-process commits. Defaults are 5000 ms and 1000 ms respectively; values are bounded to safe native timer/integer ranges.

Owned :memory: paths and URI filenames are rejected. An explicitly created/migrated borrowed in-memory database is supported for ephemeral workloads/tests, but it is not durable across restarts and is never an automatic fallback. Do not use the borrowed MQ handle inside an unrelated long-lived application transaction; native queue operations own their SQL transactions.

## Host and TypeScript compatibility

The installed-package tests execute actual SQLite files under Node and Bun with TypeScript 6.0.3 and 7.0.2. They start new worker processes, including Node producers with Bun workers and Bun producers with Node workers, against the same persisted file and logical namespace. This checks interoperability between the tested native hosts, not simultaneous multi-host network access.

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

This increment does not expose SQLite flow, schedule, outbox, event-reader or ORM transaction resources. Requests to enable those unknown options fail. Existing PostgreSQL features remain intact. Additional resource bundles and multi-process distributed-control qualification remain roadmap work rather than inferred feature parity. Passing ordinary queue tests is not evidence that every upstream optional component has a qualified Nest integration.

## Verification

Source tests use actual native SQLite databases with real Nest contexts and engine workers. They cover inert configuration, explicit migrations, no automatic startup migration, borrowed pragma preservation, owned cleanup after failed acquisition, unsupported capabilities, duplicate lexical paths and draining an admitted handler before closing the database.

Installed consumers cover JSON-looking strings, scalar/null/array/object values, Date codecs, decoded enqueue, batches, idempotency, retry attempt history, pending cancellation, delayed promotion, prepare without publication, wait timeout without job cancellation, namespace isolation and persisted results after producer/worker exit. Both host/compiler combinations and the complete pre-existing PostgreSQL package matrix are retained in CI. No npm publication or production deployment accompanies this repository delivery.
