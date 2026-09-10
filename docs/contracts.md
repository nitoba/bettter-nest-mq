# Typed contracts and Nest registration

## Boundary

These M1 contracts remain available with the M2 runtime integration. Declaration, validation and JSON encoding do not themselves publish jobs. Configuring connections enables the real storage lifecycle described in docs/connections.md, but public enqueue/worker methods are still not implemented.

The root is independent of Zod. Standard Schema validators may be synchronous or asynchronous. The optional better-nest-mq/zod subpath uses Zod's supported reverse-validation/encoding API, including nested codecs.

## Queue and job declarations

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, JobTimeout, Queue, QueueService, Retry } from 'better-nest-mq'

@Injectable()
@Queue({ name: 'reports', connection: 'primary', defaults: { priority: 5 } })
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

Enable experimentalDecorators and emitDecoratorMetadata in the consuming Nest application. Ordinary constructor injection works; packed consumers verify it without requiring explicit Inject on their application Services.

Every descriptor needs Job metadata. Every concrete QueueService, including subclasses, declares its own Queue identity. Names must be non-empty without surrounding whitespace; versions are positive safe integers. Two properties may expose different versions of the same job. Duplicate decorators, empty queues, missing metadata and accessors instead of initialized job fields are rejected without invoking getters.

Inherited job metadata can be specialized with a subclass retry/timeout decorator. Copy-on-write metadata prevents changes to the base class. Supplied policies are copied/frozen; caller-owned schemas are not cloned or deep-frozen.

## Identity and policies

Identity is the tuple of connection, queue, job name and version. Its key is JSON.stringify of that tuple, avoiding delimiter collisions. Class/property names are excluded. Connection defaults to `default`. With configured M2 connections, each queue must reference an existing name; with no connection map, contract-only registration remains available.

Connection names now participate in actual storage addressing through the upstream named-store protocol. Renaming a connection must not be treated as a harmless code refactor; see docs/connections.md.

Policies resolve in order: library, module, queue, job, property decorators. A later retry policy replaces the complete previous one rather than mixing backoff variants. Explicit zero values are preserved.

| Setting | Default | Declaration validation |
| --- | --- | --- |
| retry.attempts | 1 | Positive safe integer |
| retry.backoff | Unset | Supported discriminated policy |
| timeoutMs | Unset | Non-negative safe integer |
| priority | 0 | Non-negative safe integer |
| delayMs | 0 | Non-negative safe integer |

Fixed backoff has delayMs; linear has initialDelayMs/incrementMs; exponential has initialDelayMs and finite factor >=1. Built-ins accept optional maxDelayMs and finite jitter in [0,1]. Custom declarations identify a policy name/version. `resolveJobPolicy(...layers)` exposes the same immutable resolution independently of Nest.

These job policies are not yet executed: M3 will bind them to the engine and specify/test runtime semantics such as a zero timeout. M2 shutdown grace settings, by contrast, already govern runtime disposal. No per-enqueue override exists until publication is implemented.

## Types and JSON boundaries

```ts
import type { InputOf, PayloadOf, ResultOf, FailureOf } from 'better-nest-mq'

type Input = InputOf<ReportsQueue['generate']>
type Payload = PayloadOf<ReportsQueue['generate']>
type Result = ResultOf<ReportsQueue['generate']>
type Failure = FailureOf<ReportsQueue['generate']>
```

InputOf is the payload schema input; PayloadOf/ResultOf/FailureOf are decoded outputs. Without a failure schema, FailureOf is never. A retryable predicate without a declared failure schema is rejected.

| Method | Input | Promise output |
| --- | --- | --- |
| parsePayload | Payload schema input | Decoded payload |
| encodePayload | Decoded payload | JSON string |
| decodePayload | JSON string | Revalidated decoded payload |
| parseResult | Result schema input | Decoded result |
| encodeResult | Decoded result | JSON string |
| decodeResult | JSON string | Revalidated result |
| encodeFailure | Decoded declared failure | JSON string |
| decodeFailure | JSON string | Revalidated declared failure |

Standalone validateSchema, encodeSchema and decodeSchema implement the same boundaries. Runtime validation does not trust an input merely because TypeScript typed it.

Encoding checks JSON fidelity: undefined, non-finite numbers, negative zero, cycles, BigInt and plain Date values cannot silently disappear/change. An explicit codec must decode back to a deeply equal domain value. Plain schemas act as identity encoders only when the decoded value remains unchanged and JSON-compatible. One-way transformations have no assumed inverse.

```ts
import { z } from 'zod'
import { defineCodec, decodeSchema, encodeSchema } from 'better-nest-mq'

const plusOne = z.number().transform((input) => input + 1)
const reversible = defineCodec(plusOne, (decoded) => decoded - 1)
const restored = await decodeSchema(reversible, await encodeSchema(reversible, 4))
// restored === 4; the JSON wire value is 3.
```

For Zod codec objects use zodCodec from better-nest-mq/zod rather than reproducing reverse traversal. defineCodec supports other vendors and asynchronous encoders. Validators/encoders must be deterministic and side-effect-free because boundary checks can invoke them repeatedly; executable schemas are trusted application code, not sandboxed user input.

## Failure categories and predicates

SchemaValidationException contains validator issues; SchemaEncodingException identifies corrupt/lossy JSON or failed round trips; SchemaDefectException preserves validator/encoder exceptions and phase. ContractDefinitionException represents invalid declarations.

JobFailureException<T> is an ordinary Error containing typed failure data and optional cause. Its constructor cannot validate against a particular job; validation happens through that job's failure contract. TypeScript does not gain checked exceptions. Worker-side domain-failure/defect normalization remains M3 work.

getIdempotencyKey(decodedPayload) runs the declared key factory and rejects empty/padded keys. canRetry(decodedFailure) runs the declared predicate and defaults to false. Neither performs persistent deduplication or retry execution by itself.

## Nest registration

```ts
@Module({
  imports: [MqModule.forFeature([ReportsQueue])],
  exports: [MqModule]
})
export class ReportsMessagingModule {}
```

Use one root registration per application context and one shared feature module per queue. forFeature registers/exports its classes, deduplicating repeated classes in that invocation. Ordinary providers and useExisting aliases are discovered; aliases to the same instance are deduplicated. Distinct instances claiming the same durable queue identity are rejected.

MqRegistry uses DiscoveryService over actual providers. Contracts require singleton scope/static dependency trees. queues()/jobs()/get(identityKey) expose a frozen complete snapshot after successful validation. Invalid later definitions never leave a partially populated registry. The engine invokes registry.initialize() idempotently before acquiring stores, so concurrent Nest lifecycle hooks do not race discovery.

Normal destruction clears the registry. A failed Nest initialization may be rethrown by close() before shutdown hooks; the M2 host therefore rolls back acquired resources in its own startup failure path. Feature-only applications can inspect contracts without a root and without storage.

## Tests and next step

Regression tests cover metadata inheritance, versions, policies, codec fidelity, vendor failures, Nest aliases/scopes/context isolation, public module re-exports and wrong TypeScript input/output/failure types. Actual tarballs verify public imports and optional integration isolation outside the workspace. The next milestone binds these contracts to real producer/worker behavior using the current named store host.
