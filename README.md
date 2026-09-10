# better-nest-mq

Typed NestJS contracts and module integration for the better-effect-mq ecosystem.

**Status: M1 — typed contracts and Nest registration, version 0.0.0. The queue engine is not connected yet.**

The repository is `nitoba/bettter-nest-mq` (three `t` characters); the package name is `better-nest-mq`. No npm release has been published.

## Available now

Declare injectable Queue Services with typed job properties, `@Queue`, `@Job`, `@Retry` and `@JobTimeout`. Register them with `MqModule.forFeature()` and inspect the validated, immutable snapshot through `MqRegistry`. Payload, result and domain-failure contracts support Standard Schema validators and explicit bidirectional codecs. Zod integration is optional, through `better-nest-mq/zod`.

`MqModule.forRoot()` and `forRootAsync()` support normal Nest factories, imports, injection and opt-in global registration. The registry discovers actual Nest singleton providers, rejects conflicting identities before publishing its snapshot, and is isolated between application contexts.

**Not implemented yet:** enqueue, workers, database connections/adapters, retry execution, flows, persistent schedules, transactional outbox and operational administration/events. Retry and shutdown settings are validated declarations only; no background processing starts. Connection names currently identify contracts, not opened or verified database resources. No simulated producer or worker APIs are exported.

See [the contract guide](docs/contracts.md), [the architecture](docs/architecture.md) and [the roadmap](docs/roadmap.md).

## Declare a Queue Service

```ts
import { Injectable, Module } from '@nestjs/common'
import { z } from 'zod'
import {
  Job,
  JobTimeout,
  MqModule,
  Queue,
  QueueService,
  Retry,
  type InputOf,
  type PayloadOf,
  type ResultOf
} from 'better-nest-mq'

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
export class ReportsQueue extends QueueService {
  @Job({ name: 'generate', version: 1 })
  @Retry({
    attempts: 5,
    backoff: {
      type: 'exponential',
      initialDelayMs: 1_000,
      factor: 2,
      maxDelayMs: 60_000,
      jitter: 0.2
    }
  })
  @JobTimeout(120_000)
  readonly generate = this.job({
    payload: z.object({ requestId: z.uuid() }),
    result: z.object({ fileKey: z.string().min(1) }),
    failure: z.object({ code: z.string(), retryable: z.boolean() }),
    idempotencyKey: (payload) => payload.requestId,
    retryable: (failure) => failure.retryable
  })
}

export type GenerateInput = InputOf<ReportsQueue['generate']>
export type GeneratePayload = PayloadOf<ReportsQueue['generate']>
export type GenerateResult = ResultOf<ReportsQueue['generate']>

@Module({
  imports: [
    MqModule.forRoot({ defaults: { priority: 0 } }),
    MqModule.forFeature([ReportsQueue])
  ]
})
export class ApplicationModule {}
```

The example is the implemented local/package API, not an instruction to install a released npm version. Application code can inject `ReportsQueue` using normal Nest constructor injection. Importing or constructing it performs no I/O.

The durable identity is the connection, queue name, job name and positive integer version. Renaming a class or property does not change that identity. Definitions are inert; a producer-only process does not need worker providers.

## Validate and encode contracts

Each job currently exposes `parsePayload`, `encodePayload`, `decodePayload`, equivalent result operations, `encodeFailure` and `decodeFailure`, plus the declared idempotency/retry predicates. These are contract operations, not database writes.

```ts
const payload = await reports.generate.parsePayload(input)
const json = await reports.generate.encodePayload(payload)
const restored = await reports.generate.decodePayload(json)
```

Input and decoded values are different types when a schema transforms data. `encodePayload()` takes the decoded type and returns a JSON string. A plain schema can encode an unchanged JSON value; a non-JSON domain value or non-idempotent transformation requires an explicit inverse.

```ts
import { z } from 'zod'
import { decodeSchema, encodeSchema } from 'better-nest-mq'
import { zodCodec } from 'better-nest-mq/zod'

const event = zodCodec(
  z.object({
    occurredAt: z.codec(z.iso.datetime(), z.date(), {
      decode: (value) => new Date(value),
      encode: (value) => value.toISOString()
    })
  })
)

const original = { occurredAt: new Date('2026-09-10T12:00:00.000Z') }
const json = await encodeSchema(event, original)
const restored = await decodeSchema(event, json)
```

`restored.occurredAt` is a `Date`. JSON fidelity and decode/encode round trips are checked; lossy conversions and one-way transforms are not silently accepted. `defineCodec(schema, encoder)` provides the same explicit inverse for other Standard Schema implementations, including asynchronous encoders. Validators/encoders must be deterministic and side-effect-free because boundary checks may call them more than once.

The root entry point neither imports nor requires Zod. `@standard-schema/spec` supplies the public type contract; `zod` is an optional peer for the Zod subpath, with codecs requiring Zod 4.1 or newer.

## Modules and registry

`forRootAsync()` supports `useFactory`, `useClass`, `useExisting`, `imports` and `inject`. Class/existing factories implement `MqOptionsFactory.createMqOptions()`. Global registration remains opt-in through `isGlobal: true`.

After application initialization, `MqRegistry.queues()` and `.jobs()` expose read-only definitions; `.get(identityKey)` returns a registered job or `undefined`. Discovery validates the complete set before exposing it. Queue contracts require singleton providers and static dependency trees; request/transient-scoped execution belongs to the future worker integration.

Library, module, queue, job and property-decorator settings resolve in that order. A later retry policy replaces the earlier policy as a unit. No function or provider instance is persisted as retry configuration. The current registry also rejects duplicate queue identities across distinct providers; aliases pointing at the same instance are deduplicated.

## Development and verification

Use Bun **1.4.2** and Node **22.12+**. CI covers Node 22/24, the development TypeScript 7 compiler and a separate TypeScript 6.x compatibility installation. The public TypeScript peer floor is `>=6.0.0`.

```sh
git clone https://github.com/nitoba/bettter-nest-mq.git
cd bettter-nest-mq
bun install --frozen-lockfile
bun run hooks:install
bun run check
```

Individual commands include `typecheck`, `typecheck:minimum`, `test`, `test:coverage`, `lint`, `format`, `format:check`, `build`, `publint` and `test:package`.

The complete check covers source/types, real Nest contexts, schema and metadata regressions, formatting, type-aware lint, ESM/declaration build, package exports and actual tarball consumption outside the workspace. External consumers first run without Zod installed, then exercise the optional Zod subpath, using both TypeScript compilers and both Node and Bun execution.

## Tooling provenance

The `.oxlintrc.json`, `.oxfmtrc.json` and entire `tools/oxlint` plugin are copied unchanged from `nitoba/better-effect` at `42c28fb0af7882eb048ee5d4ab1c1db81142c9dd`. `bun run check:tooling` checks all 20 Git blob hashes. See [tooling provenance](docs/tooling.md) before changing this baseline.

Bun manages dependencies/tests, tsdown builds ESM and declarations, Oxlint/oxlint-tsgolint apply the original anti-slop rules, Oxfmt formats authored files, Lefthook runs local hooks and publint validates exports. No ESLint or Prettier configuration is introduced. Vendored tooling is excluded from rewriting, not weakened.

## Architectural boundary

The planned engine bridge will reuse better-effect-mq rather than reimplement its lease, retry and transaction protocols. Effect/Result/Layer/Runtime stay private. Database drivers remain optional. Module registration must never implicitly start an HTTP server, open database connections or apply production migrations. Durable delivery will remain at-least-once with explicit idempotency and outbox semantics.

## License

MIT. Vendored tooling retains its upstream license and provenance.
