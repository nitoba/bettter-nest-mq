# better-nest-mq

NestJS-native contracts and lifecycle integration for the better-effect-mq engine.

**Status: M2 — private engine host, named stores and PostgreSQL integration. Version 0.0.0; not yet a complete producer/worker library. No npm release has been published.**

The repository is `nitoba/bettter-nest-mq` (three `t` characters); the package name is `better-nest-mq`.

## Available now

Typed Queue Services and Job definitions, Queue/Job/Retry/JobTimeout decorators, Standard Schema validation and explicit codecs, optional Zod integration, Nest feature-module registration, and a validated registry are implemented.

Configuring connections now starts the real engine: one private runtime per application context, named JobStores, protocol/capability checks, live readiness probes, rollback after failed acquisition and ownership-aware shutdown. PostgreSQL is the first optional production integration, with explicit migration and validation helpers.

**Not implemented yet:** public enqueue/query/wait methods, Worker/Process decorators and handler execution, retry execution, flows, persistent schedules, transactional outbox, other database wrappers and operational job administration. Retry and timeout decorators remain validated contracts until the producer/worker bridge is implemented. No fake enqueue or worker methods are exported.

See [contracts](docs/contracts.md), [connections and PostgreSQL](docs/connections.md), [architecture](docs/architecture.md) and [roadmap](docs/roadmap.md).

## Queue contracts

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, JobTimeout, Queue, QueueService, Retry } from 'better-nest-mq'

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
export class ReportsQueue extends QueueService {
  @Job({ name: 'generate', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'fixed', delayMs: 1_000 } })
  @JobTimeout(60_000)
  readonly generate = this.job({
    payload: z.object({ requestId: z.uuid() }),
    result: z.object({ fileKey: z.string() }),
    failure: z.object({ code: z.string(), retryable: z.boolean() }),
    idempotencyKey: (payload) => payload.requestId,
    retryable: (failure) => failure.retryable
  })
}
```

`InputOf`, `PayloadOf`, `ResultOf` and `FailureOf` preserve schema inference. Payload/result/failure validation and JSON encoding are usable independently of storage. `defineCodec` supplies an explicit inverse for any Standard Schema validator; `zodCodec` from `better-nest-mq/zod` uses Zod 4.1+ nested codecs. The root does not require Zod.

Every concrete queue declares its own identity. Class/property renaming does not change it; connection, queue name, job name and version do. Queue constructors and decorators never perform I/O.

## Connect PostgreSQL

The example below uses the implemented local/package API, not an already published npm release. After consuming a local tarball, PostgreSQL users need the optional integration dependencies:

```sh
bun add pg@^8.16.3 better-effect-mq-postgres@0.1.3 better-effect-mq-outbox@0.1.3
bun add -d @types/pg
```

The outbox package is required by the upstream PostgreSQL adapter's package graph; this does not enable the future Nest outbox API.

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
              namespace: 'my-application'
            })
          },
          shutdown: { gracePeriodMs: 30_000, abortAfterGracePeriod: true }
        }
      }
    }),
    MqModule.forFeature([ReportsQueue])
  ]
})
export class ApplicationModule {}
```

**Run explicit migrations before starting this application.** Startup validates the existing schema by default and fails if it is missing; it never creates or upgrades database tables automatically. The connection factory itself remains inert until the Nest application initializes.

To reuse a pool, call `postgres({ pool, schema, namespace })` inside `forRootAsync` and inject the application's pool provider. Borrowed pools are never closed by this library. Owned pools are created lazily, handle idle-client disconnections and close after adapter resources are released.

Connection names are durable addresses: the upstream named-store protocol includes the connection token in its PostgreSQL namespace. Keep `primary`, schema and namespace stable between deployments; renaming the connection does not reopen the old jobs. Use the same names in producer and worker applications when those roles become available.

## Explicit deployment migrations

```ts
import { Pool } from 'pg'
import { migratePostgres } from 'better-nest-mq/postgres'

const connectionString = process.env.DATABASE_URL
if (!connectionString) throw new Error('DATABASE_URL is required')
const pool = new Pool({ connectionString })
try {
  const report = await migratePostgres({ pool, schema: 'mq' })
  console.log(report)
} finally {
  await pool.end()
}
```

`validatePostgres({ pool, schema })` verifies the layout without applying migrations. Both helpers borrow their pool, preserve failure causes and return plain Promise results with facade-owned types.

## Connection readiness

Inject `MqConnectionsService` into a Service:

```ts
@Injectable()
export class MessagingHealthService {
  constructor(private readonly connections: MqConnectionsService) {}

  check() {
    return this.connections.probe('primary')
  }
}
```

Import `Injectable` from `@nestjs/common` and `MqConnectionsService` from `better-nest-mq`. `connections()` returns a frozen diagnostic snapshot; `probe(name)` queries the actual store. The snapshot contains adapter/protocol versions, capabilities and ownership, not credentials or raw pools. `state` describes lifecycle, not an automatically refreshed database-health guarantee. No HTTP health endpoint or timer is installed.

Omitting `connections` keeps the original contract-only mode: the registry works, the engine reports `disabled`, and no runtime/store is acquired. A configured connection map requires every registered queue to reference a declared connection.

## Development and verification

Use Bun **1.4.2**, Node **22.12+**, and the committed lockfile. TypeScript's public floor is **>=6.0.0**; source and packed consumers are checked with TypeScript 6 and the primary TypeScript 7 compiler.

```sh
git clone https://github.com/nitoba/bettter-nest-mq.git
cd bettter-nest-mq
bun install --frozen-lockfile
bun run hooks:install
bun run check
```

`check` runs tooling integrity, both typechecks, unit/real-Nest tests, Oxfmt, type-aware Oxlint, the ESM/declaration build, publint and tarball consumers outside the workspace. The consumers first run without Zod/pg/adapter packages installed, then validate optional integration subpaths using Node and Bun.

```sh
MQ_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/dedicated_test_db' bun run test:postgres
```

Use a dedicated test database. Integration tests create/drop random schemas, inspect connections and deliberately terminate tagged test clients. The read-only CI has Node 22/24 quality jobs and a real PostgreSQL 16 job that also runs the packed public integration under Node and Bun.

## Tooling provenance

The `.oxlintrc.json`, `.oxfmtrc.json` and complete anti-slop plugin retain the exact better-effect baseline at `42c28fb0af7882eb048ee5d4ab1c1db81142c9dd`. `bun run check:tooling` verifies all 20 file hashes. Bun, tsdown, Oxlint/oxlint-tsgolint, Oxfmt, Lefthook and publint remain the toolchain; no ESLint or Prettier is added.

## License

MIT. Vendored tooling retains its upstream license and provenance.
