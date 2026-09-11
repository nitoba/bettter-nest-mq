# Typed contracts, schemas and registration

## Current boundary

The M1 contract layer is now connected to the M3 producers and workers. Declaration, parsing and encoding remain usable without storage; enqueue and execution require a ready configured application. See execution.md for public job operations and connections.md for PostgreSQL/lifecycle setup.

The root uses Standard Schema interfaces and does not require Zod. Validators may be asynchronous. The optional better-nest-mq/zod entry point uses Zod's supported reverse encoding, including nested codecs.

## Queue definitions

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

Enable experimentalDecorators and emitDecoratorMetadata in the consuming Nest application. Constructor injection is ordinary Nest DI. Queue constructors and decorators remain inert.

Every descriptor needs Job metadata and every concrete QueueService, including subclasses, declares its own Queue identity. Names are non-empty without surrounding whitespace; versions are positive safe integers. Two fields may expose different versions of one job. Duplicate decorators, empty queues, missing metadata and accessors instead of initialized job fields are rejected without invoking getters. One descriptor instance cannot be bound to multiple durable identities.

Inherited job metadata can be specialized without modifying its base class. Policies are copied/frozen; caller-owned schemas are not deep-frozen or cloned. Worker metadata inheritance has its own rules described in execution.md.

## Identity and policies

A job identity contains connection, queue, name and version; its key is a JSON tuple to avoid delimiter collisions. Class and property names are excluded. Connection defaults to `default` and, in configured applications, must refer to a declared connection. Omitting the entire connection map retains contract-only registration.

Connection names now contribute to the actual PostgreSQL storage address through the stable named-store token. Keep the same name/schema/namespace between deployments and producers/workers; renaming a connection does not reopen its old jobs.

Policy precedence is library, module, queue, job, property decorators, then permitted enqueue overrides. A later retry policy replaces the complete earlier one, rather than combining incompatible backoff variants. Defaults are one total attempt, no explicit execution timeout, priority zero and delay zero.

Fixed backoff has delayMs; linear has initialDelayMs/incrementMs; exponential has initialDelayMs and factor >=1. Built-ins support optional maxDelayMs and jitter in [0,1]. These policies now execute through the upstream supervisor. Custom named/versioned retry providers are still metadata-only and are rejected when compiling executable jobs. Zero timeout is likewise representable as metadata but rejected for execution; it is not silently treated as unlimited.

resolveJobPolicy exposes immutable metadata resolution separately. The worker's retryDefects default is false, independent of the upstream default. Typed retry predicates receive validated decoded failure values. Runtime attempt budgets and persisted overrides are described in execution.md.

## Types and JSON boundaries

```ts
import type { InputOf, PayloadOf, ResultOf, FailureOf } from 'better-nest-mq'

type Input = InputOf<ReportsQueue['generate']>
type Payload = PayloadOf<ReportsQueue['generate']>
type Result = ResultOf<ReportsQueue['generate']>
type Failure = FailureOf<ReportsQueue['generate']>
```

InputOf is the payload schema input. PayloadOf, ResultOf and FailureOf are decoded outputs. FailureOf is never when no failure schema exists. A retryable predicate without a failure schema is invalid.

| Contract method | Input | Promise output |
| --- | --- | --- |
| parsePayload | Payload schema input | Decoded payload |
| encodePayload | Decoded payload | JSON string |
| decodePayload | JSON string | Revalidated decoded payload |
| parseResult | Result schema input | Decoded result |
| encodeResult | Decoded result | JSON string |
| decodeResult | JSON string | Revalidated result |
| encodeFailure | Decoded declared failure | JSON string |
| decodeFailure | JSON string | Revalidated declared failure |

Standalone validateSchema, encodeSchema and decodeSchema expose the same boundaries. TypeScript annotations do not replace runtime checks. Enqueue parses an input and encodes its decoded value; enqueueDecoded encodes the supplied decoded type directly. Workers revalidate persisted payloads and results/failures are checked before storage and on reads.

JSON fidelity checks reject data that would disappear or change: undefined, non-finite numbers, negative zero, cycles, BigInt and plain Date values require another representation or an explicit codec. A codec must decode back to a deeply equal value. Plain schemas are identity encoders only when the decoded value remains unchanged and JSON-compatible; a one-way transform has no implied inverse.

```ts
import { z } from 'zod'
import { defineCodec, decodeSchema, encodeSchema } from 'better-nest-mq'

const plusOne = z.number().transform((input) => input + 1)
const reversible = defineCodec(plusOne, (decoded) => decoded - 1)
const restored = await decodeSchema(reversible, await encodeSchema(reversible, 4))
// restored is 4, while its JSON wire representation is 3.
```

For Zod codec objects, use zodCodec from better-nest-mq/zod instead of manually traversing nested schemas. defineCodec accepts other vendors and asynchronous encoders. Validators/encoders are trusted application code and must be deterministic/side-effect-free, since persistence-boundary checks may invoke them repeatedly.

## Failures and predicates

SchemaValidationException contains validator issues. SchemaEncodingException identifies corrupt/lossy JSON or failed round trips. SchemaDefectException retains a throwing validator/encoder and phase. ContractDefinitionException represents invalid declarations. The engine bridge translates schema problems into its persisted encode/decode categories without leaking engine exception types publicly.

JobFailureException<T> carries a typed failure and optional cause. Its constructor has no implicit job schema; the worker bridge validates the declared failure contract before returning a typed failure to the supervisor. Unexpected exceptions remain defects. Invalid failure data cannot masquerade as a valid domain failure. Promise-returning TypeScript methods do not acquire checked exceptions.

getIdempotencyKey and canRetry remain independently callable predicates; they do not write data themselves. Bound enqueue now passes idempotency to the actual engine, and the worker consults retryability while applying the persisted attempt policy.

## Nest registration and lifetime

```ts
@Module({
  imports: [MqModule.forFeature([ReportsQueue])],
  exports: [MqModule]
})
export class ReportsMessagingModule {}
```

Use one root registration per application context and a shared feature module per queue. forFeature registers/exports classes and deduplicates repeated entries. Ordinary providers and aliases are discovered; aliases to the same instance are deduplicated, while distinct instances claiming one durable queue are rejected.

MqRegistry uses actual DiscoveryService providers and builds its complete immutable snapshot before publishing it. Contracts require singleton scope/static dependencies. The host invokes initialize idempotently before acquisition and executable compilation. Missing or duplicate worker references fail before stores are opened. Request-scoped worker dependencies are separate from singleton contracts.

Contract-only apps and manually constructed queues can parse/encode, but producer methods reject until bound to a ready application. Normal shutdown detaches producer bindings and clears registry state. Failed activation detaches clients and rolls back resources locally, rather than relying on Nest destruction hooks after failed bootstrap.

## Verification

Regression coverage includes schema transformations/fidelity, vendor errors, policies, identity/versioning, inherited metadata, Nest scopes/aliases/module exports and compile-time invalid types. Packed consumers use the actual installed package with optional peers absent or present. Public execution and persistence tests are detailed in execution.md; flows, schedules and outbox remain separate planned features.
