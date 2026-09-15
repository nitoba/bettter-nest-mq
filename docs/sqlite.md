# SQLite job storage (Node and Bun)

The first SQLite increment provides the ordinary JobStore and existing Nest producers/workers with an embedded local database. It reuses the released better-effect-mq-sqlite adapter as a normal internal dependency. Application code does not install or configure that adapter, Effect or Result.

## Native entry points

Use sqlite, migrateSqlite and validateSqlite from `better-nest-mq/sqlite/node` for Node's DatabaseSync, or from `better-nest-mq/sqlite/bun` for Bun's Database. Do not import the Bun entry point in Node or vice versa. The package root loads neither host driver. Bun is still the package manager; the application chooses its execution host.

```ts
import { Module } from '@nestjs/common'
import { MqModule } from 'better-nest-mq'
import { sqlite } from 'better-nest-mq/sqlite/node'

@Module({
  imports: [MqModule.forRoot({
    connections: { primary: sqlite({ path: './data/jobs.db', namespace: 'reports' }) }
  })]
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

## Guarantees and limits

Persistence uses the upstream named-store namespace, derived from the stable nestjs/<connection-name> token. Changing that name or namespace selects a different logical queue even in the same file. No new supervisor, lease protocol or persistence envelope is introduced. Handler effects remain at-least-once and cancellation remains cooperative.

SQLite is a synchronous, local embedded backend. Busy waits and large queries block the host thread. This does not provide a network broker, multi-host cluster database, or universal throughput guarantee. Keep files on supported local storage and qualify your operating limits before production use.

This increment does not expose SQLite flow, schedule, outbox, event-reader or ORM transaction resources. Requests to enable those unknown options fail. Existing PostgreSQL features remain intact. Additional resource bundles and multi-process distributed-control qualification remain roadmap work rather than inferred feature parity.

Node's earliest supported 22.12 release requires --experimental-sqlite; tests use the flag explicitly. Native typings come from @types/node for Node and @types/bun for Bun. The Node/root entry points do not require Bun ambient types. No npm publication or production deployment accompanies this repository delivery.
