# Typed contracts and Nest registration

## Implemented boundary

M1 provides contract declaration, validation, JSON encoding/decoding, policy resolution and Nest registration. It does not publish jobs, execute retries, run workers or connect to storage. A JSON string returned by an encoder has not been saved anywhere.

The package root is independent of Zod. Standard Schema implementations are accepted through their public `~standard` interface, including asynchronous validators. The optional `better-nest-mq/zod` subpath uses Zod's reverse-validation/encoding API for nested codecs.

## Queue and job declaration

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, JobTimeout, Queue, QueueService, Retry } from 'better-nest-mq'

@Injectable()
@Queue({
  name: 'reports',
  connection: 'primary',
  defaults: { priority: 5 }
})
export class ReportsQueue extends QueueService {
  @Job({ name: 'generate', version: 1, defaults: { delayMs: 100 } })
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

Decorators use Nest's legacy TypeScript decorator mode. Enable `experimentalDecorators` and `emitDecoratorMetadata` in the consuming application. Constructor injection remains ordinary Nest injection; the packaged consumer tests verify it without explicit `@Inject` annotations in application Services.

Every job descriptor must have `@Job`. Every concrete QueueService must declare its own `@Queue`, including subclasses. Names must be non-empty without surrounding whitespace; versions are positive safe integers. Two properties can expose different versions of the same job. Duplicate decorators, empty queues, undecorated descriptors and accessors in place of job fields are rejected. Discovery reads property descriptors and never evaluates getters.

Inherited job declarations may be specialized with a subclass retry or timeout decorator. Metadata is copied on write; changing a subclass does not alter its parent. Supplied policy objects are copied, while caller-owned schema instances are not deep-frozen or cloned.

## Identity

A job identity contains `connection`, `queue`, `name` and `version`. Its key is the JSON tuple of those values, avoiding delimiter collisions. Class and property names are not persisted identity components.

```ts
const definition = getQueueDefinition(reports)
const job = definition.jobs[0]
// job.identity.key = '["primary","reports","generate",1]'
```

Connection defaults to `default`. In M1 it is a namespace only: there is no connection factory, adapter-capability validation or database acquisition yet. Do not mistake a validated queue definition for a connected producer.

## Configuration precedence

Resolved policy precedence is library defaults, module defaults, queue defaults, job defaults and property decorators, in that order. A later retry value replaces the complete earlier retry policy rather than combining incompatible backoff variants.

| Setting | Library default | Validation |
| --- | --- | --- |
| `retry.attempts` | 1 | Positive safe integer |
| `retry.backoff` | Unset | Validated discriminated policy |
| `timeoutMs` | Unset | Non-negative safe integer |
| `priority` | 0 | Non-negative safe integer |
| `delayMs` | 0 | Non-negative safe integer |

`fixed` backoff has `delayMs`; `linear` has `initialDelayMs` and `incrementMs`; `exponential` has `initialDelayMs` and a finite `factor >= 1`. Built-in policies accept optional `maxDelayMs` and finite `jitter` in `[0, 1]`. A `custom` declaration identifies a named policy and positive integer version; execution/provider resolution is not implemented in M1. These settings are not yet translated into engine retries, so runtime semantics such as zero timeout will be defined and tested in the engine bridge.

`resolveJobPolicy(...layers)` performs the same validation and immutable resolution without creating a Nest application. An explicit zero is not replaced by a nonzero default. Currently there is no per-enqueue override because publication itself is not implemented.

## Inputs, decoded values and JSON

The public utility types preserve each boundary:

```ts
import type { InputOf, PayloadOf, ResultOf, FailureOf } from 'better-nest-mq'

type Input = InputOf<ReportsQueue['generate']>
type Payload = PayloadOf<ReportsQueue['generate']>
type Result = ResultOf<ReportsQueue['generate']>
type Failure = FailureOf<ReportsQueue['generate']>
```

`InputOf` is the payload schema input. `PayloadOf`, `ResultOf` and `FailureOf` are decoded schema outputs. Without a failure schema, `FailureOf` is `never`. A retryability predicate without a failure schema is rejected.

| Operation | Input | Output |
| --- | --- | --- |
| `job.parsePayload(input)` | Payload schema input | Decoded payload |
| `job.encodePayload(value)` | Decoded payload | JSON string |
| `job.decodePayload(text)` | JSON string | Revalidated decoded payload |
| `job.parseResult(input)` | Result schema input | Decoded result |
| `job.encodeResult(value)` | Decoded result | JSON string |
| `job.decodeResult(text)` | JSON string | Revalidated decoded result |
| `job.encodeFailure(value)` | Decoded declared failure | JSON string |
| `job.decodeFailure(text)` | JSON string | Revalidated declared failure |

The standalone `validateSchema`, `encodeSchema` and `decodeSchema` functions offer the same behavior for any supported contract. Validation accepts untrusted values at runtime even when the higher-level job method has a typed argument.

Encoding verifies that JSON does not lose or change the representation. Non-finite numbers, negative zero, undefined values, cycles, BigInt and a plain Date require a different explicit representation or are rejected. A codec must also decode back to a deeply equal domain value. It cannot silently truncate or alter data merely because its encoded output passes validation.

A plain schema is an identity encoder only for an unchanged JSON-compatible decoded value. A one-way transformation has no implied inverse. Use an explicit encoder for transformations such as string-to-Date or an incrementing numerical transform.

```ts
import { z } from 'zod'
import { defineCodec, decodeSchema, encodeSchema } from 'better-nest-mq'

const plusOne = z.number().transform((input) => input + 1)
const reversible = defineCodec(plusOne, (decoded) => decoded - 1)
const restored = await decodeSchema(reversible, await encodeSchema(reversible, 4))
// restored === 4; the JSON wire value is 3.
```

`defineCodec` is validator-agnostic and allows asynchronous encoders. For Zod codecs, including nested objects, use `zodCodec` rather than manually reproducing the reverse schema traversal. Validators and encoders must be deterministic and side-effect-free; checks may validate more than once. Schemas are application code, not a sandbox for untrusted executable validators.

## Failure categories

`SchemaValidationException` contains validator issues; `SchemaEncodingException` identifies corrupt JSON, lossy serialization and failed round trips; `SchemaDefectException` preserves a throwing validator/encoder as its cause and records the failing phase. Invalid declarations use `ContractDefinitionException`.

`JobFailureException<T>` is an ordinary Error with typed failure content and optional cause. Construction does not itself validate against a job schema: validation happens through the job's failure contract before persistence. TypeScript does not gain checked exceptions from this class. Worker-side normalization of domain failures versus unexpected defects remains an engine-integration task.

`getIdempotencyKey(decodedPayload)` evaluates the declared key factory and rejects empty/whitespace-padded keys. `canRetry(decodedFailure)` evaluates the declared predicate, defaulting to false. These helpers do not deduplicate persisted jobs or perform retries by themselves.

## Nest registration and lifecycle

```ts
@Module({
  imports: [MqModule.forFeature([ReportsQueue])],
  exports: [MqModule]
})
export class ReportsMessagingModule {}

@Module({
  imports: [
    MqModule.forRoot({ defaults: { priority: 1 } }),
    ReportsMessagingModule
  ]
})
export class ApplicationModule {}
```

Use one root registration per Nest application context. `forFeature` registers and exports the supplied QueueService classes, deduplicating repeated classes in that call. Register queue providers in one shared feature module rather than independently instantiating the same durable queue in several modules. Ordinary providers and `useExisting` aliases are also discovered; aliases of the same instance are deduplicated.

`MqRegistry` is root-owned and uses `DiscoveryService` over registered Nest providers. Queues must have singleton scope and static dependency trees. Distinct provider instances sharing a queue identity are rejected; jobs cannot become a partial valid snapshot when a later declaration is invalid. Empty registry state before bootstrap is expected. `queues()`, `jobs()` and `get(identityKey)` read the validated snapshot, which is cleared on normal module destruction.

A failed Nest initialization is also rethrown by Nest's `close()`. M1 acquires no resources, and its snapshot is committed only after validation succeeds. The future engine host must clean up partial acquisition within its own failure path rather than relying exclusively on shutdown hooks after a bootstrap error.

## Testing and next milestone

Tests cover schema validation/encoding failures, metadata inheritance and immutability, policy precedence, job versioning, real Nest discovery, alias handling, scope rejection and context isolation. Compile-time regressions cover wrong payload/result/failure types and the intentional absence of producer methods. Packed consumers verify real imports/declarations and optional Zod isolation, not just workspace source aliases.

The next milestone connects these descriptors to a private engine host with named stores, ownership, capability validation and controlled lifecycle. Publication, workers, flows, schedules and outbox must be implemented against those real resources, not simulated on top of this registry.
