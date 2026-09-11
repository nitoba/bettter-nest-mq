# better-nest-mq

NestJS-native producers and decorated workers backed by the better-effect-mq engine.

**Status: M3 core execution and M3.1a distributed controls are implemented. Version 0.0.0, unreleased on npm.** PostgreSQL jobs can now be published, processed, retried, cancelled and queried through the Nest facade. Flows, schedules, transactional outbox and additional integrations remain on the roadmap.

Repository: `nitoba/bettter-nest-mq` (three `t` characters). Package name: `better-nest-mq`.

## What works

Queue Services declare typed jobs using Standard Schema or optional Zod codecs. Worker Services implement methods decorated with `@Process`, using normal Nest dependency injection and Promise-returning methods. One private runtime per configured application owns the existing engine's named stores, Clock and worker supervisors. Application code does not import Effect, Result, Layer or Runtime.

Producers support enqueue, decoded enqueue, batches, preparation without publication, polling, result waiting, publish-and-wait execution, attempt history, promotion, retry and cancellation. Workers support known failures, configurable retries, execution timeout, local worker/handler concurrency, cooperative cancellation and attempt-local scoped dependencies. PostgreSQL resource ownership, explicit migrations and live connection probes remain available.

See [distributed controls](docs/controls.md), [execution](docs/execution.md), [contracts and codecs](docs/contracts.md), [connections](docs/connections.md), [architecture](docs/architecture.md) and [remaining roadmap](docs/roadmap.md).

## Distributed controls

Queues can now declare limits shared by workers across processes:

```ts
@QueueControls({
  globalConcurrency: 10,
  perKeyConcurrency: 2,
  rateLimit: { max: 100, durationMs: 1_000 }
})
```

Import QueueControls from better-nest-mq and apply it to the same QueueService as @Queue. Jobs derive a typed dispatch key through `this.job({ payload, result, dispatchKey: (value) => value.tenantId })`. Per-key declarations require a key for every new publication; callers cannot override a derived key with a different value.

Ordinary applications default to read-only policy validation. Apply policies in one coordinated deployment process using `controls: { mode: 'reconcile', group: 'your-deployment' }`; replicas use validate mode and the same group. Missing/different policies fail startup before consumers start. Reconciliation never disables omitted queues and unchanged policies retain their revision. This is not a cross-database deployment transaction or a leader-election protocol.

Global/per-key permits and fixed-window admissions use the existing controlled-store protocol. Local Worker/Process concurrency remains a separate bound. See the complete syntax, deployment example, guarantees and limitations in [docs/controls.md](docs/controls.md).

## Declare a queue and a worker

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import {
  Job,
  JobData,
  Queue,
  QueueService,
  Process,
  Worker,
  type PayloadOf,
  type ResultOf
} from 'better-nest-mq'

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
export class ReportsQueue extends QueueService {
  @Job({ name: 'summarize', version: 1 })
  readonly summarize = this.job({
    payload: z.object({ values: z.array(z.number()) }),
    result: z.object({ count: z.int(), total: z.number() })
  })
}

@Injectable()
@Worker({ name: 'reports-worker', concurrency: 8 })
export class ReportsWorker {
  @Process(ReportsQueue, 'summarize', { concurrency: 4 })
  async summarize(
    @JobData() payload: PayloadOf<ReportsQueue['summarize']>
  ): Promise<ResultOf<ReportsQueue['summarize']>> {
    return {
      count: payload.values.length,
      total: payload.values.reduce((sum, value) => sum + value, 0)
    }
  }
}
```

Use the consuming application's normal legacy TypeScript decorators and emitted metadata. Business Services can be constructor-injected into the worker. `@JobContext()` supplies attempt information and an AbortSignal without exposing a lease token. Queue constructors and decorators perform no I/O.

Register the connection and providers:

```ts
import { Module } from '@nestjs/common'
import { MqModule } from 'better-nest-mq'
import { postgres } from 'better-nest-mq/postgres'
import { ReportsQueue, ReportsWorker } from './reports.js'

@Module({
  imports: [
    MqModule.forRootAsync({
      useFactory: () => {
        const connectionString = process.env.DATABASE_URL
        if (!connectionString) throw new Error('DATABASE_URL is required')
        return {
          connections: {
            primary: postgres({ connectionString, schema: 'mq', namespace: 'reports-app' })
          }
        }
      }
    }),
    MqModule.forFeature([ReportsQueue])
  ],
  providers: [ReportsWorker]
})
export class ApplicationModule {}
```

After initialization, an injected queue can publish and await persisted work:

```ts
const id = await reports.summarize.enqueue({ values: [10, 20, 30] })
const result = await reports.summarize.awaitResult(id, { timeoutMs: 30_000 })
// { count: 3, total: 60 }
```

The examples describe the actual local/tarball API, not a published npm release. Run the explicit migration below before starting the application. A queue alone never starts a consumer. An API-only deployment sets `execution: { workers: false }` and imports only shared queue modules. Producer and worker applications must use the same connection name, schema, namespace and job identity/version.

## PostgreSQL setup

Consumers of the optional PostgreSQL subpath install its peers alongside the local package tarball:

```sh
bun add pg@^8.16.3 better-effect-mq-postgres@0.1.3 better-effect-mq-outbox@0.1.3
bun add -d @types/pg
```

The engine and its PostgreSQL/outbox adapters are normal internal dependencies, installed automatically with this library. Nest consumers do not install better-effect, better-result or any better-effect-mq package manually. Only the chosen native driver/schema library is application-facing. The root remains usable without loading pg or Zod, and the internal outbox dependency does not enable the pending Nest transactional-outbox API. See [dependency ownership](docs/dependencies.md).

Execute migrations deliberately in a deployment script:

```ts
import { Pool } from 'pg'
import { migratePostgres } from 'better-nest-mq/postgres'

const connectionString = process.env.DATABASE_URL
if (!connectionString) throw new Error('DATABASE_URL is required')
const pool = new Pool({ connectionString })
try {
  await migratePostgres({ pool, schema: 'mq' })
} finally {
  await pool.end()
}
```

Startup validates the existing schema by default and never applies migrations automatically. `postgres({ pool, schema, namespace })` borrows an application-owned pool without closing it. A connectionString creates an owned pool during startup and closes it after worker/store resources. Keep named connections stable: the upstream token is part of the durable PostgreSQL namespace.

## Operational semantics

`@Retry` policies are now executed, not merely stored as metadata. Fixed, linear and exponential backoffs use the upstream supervisor. Typed failures must satisfy the declared failure schema; unexpected exceptions remain defects and are not retried unless `retryDefects: true` is explicitly set on the worker. An execution timeout must be positive.

Waiting timeout/abort only ends the caller's wait. It does not cancel persisted work. `execute` means enqueue and wait, never direct local invocation. Cancelling active work requests cooperative cancellation and leaves the engine to perform fenced settlement; it cannot undo external side effects.

Input and decoded types remain distinct through `InputOf`, `PayloadOf`, `ResultOf` and `FailureOf`. Non-JSON values use explicit inverse codecs, including the optional `better-nest-mq/zod` integration. Invalid outputs cannot be persisted as successful results.

`MqConnectionsService` exposes safe connection snapshots/live probes. `MqWorkersService` exposes local state and awaitIdle; idle does not mean every delayed job in the database has completed. Shutdown detaches producers, stops admission and drains/cooperatively aborts workers before releasing stores and owned pools.

Current boundaries: polling result waits only; class-based Worker providers; explicit JobData/JobContext parameters; no HTTP enhancer execution. Method/class HTTP guards, pipes, interceptors and filters are rejected instead of silently ignored. Global HTTP enhancers do not apply. Named custom retry providers, durable events, other adapters, flows, schedules and transactional outbox are still pending.

## Development and tests

Use Bun **1.4.2**, Node **22.12+**, the committed lockfile, and the strict TypeScript configuration. Public TypeScript floor: **>=6.0.0**; source and actual tarball consumers are checked with TypeScript 6 and 7.

```sh
git clone https://github.com/nitoba/bettter-nest-mq.git
cd bettter-nest-mq
bun install --frozen-lockfile
bun run hooks:install
bun run check
```

The complete gate checks tooling hashes, both compilers, real Nest/engine tests, formatting, type-aware lint, ESM/declarations, publint and external package consumers. CI covers Node 22/24 and a real PostgreSQL 16 service. The PostgreSQL job additionally runs packed producers/workers under Node and Bun with both compiler versions, including producer shutdown, separate consumer startup and durable result verification.

```sh
MQ_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/dedicated_test_db' bun run test:postgres
MQ_TEST_DATABASE_URL='postgresql://user:password@localhost:5432/dedicated_test_db' bun run test:package
```

Build before running test:package individually. Use a dedicated test database: tests create/drop random schemas and terminate tagged idle clients to exercise recovery. Never use production credentials for these tests.

The 20 original Oxlint/Oxfmt/custom-plugin files retain their exact better-effect baseline at `42c28fb0af7882eb048ee5d4ab1c1db81142c9dd`, verified by `bun run check:tooling`. Bun, tsdown, Oxlint/oxlint-tsgolint, Oxfmt, Lefthook and publint remain the tooling; no ESLint/Prettier was introduced.

## License

MIT. Vendored tooling retains the original license and provenance.
