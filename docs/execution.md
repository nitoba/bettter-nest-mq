# Typed producers and Nest workers

The M3 execution integration delegates durable jobs, retries, heartbeats, leases and settlement to better-effect-mq. Application code uses Nest providers and Promises; the engine types and runtime remain private. PostgreSQL is the currently supported production connection factory.

## A complete queue and worker

```ts
import { Injectable, Module } from '@nestjs/common'
import { z } from 'zod'
import {
  Job,
  JobContext,
  JobData,
  JobFailureException,
  JobTimeout,
  MqModule,
  Process,
  Queue,
  QueueService,
  Retry,
  Worker,
  type FailureOf,
  type JobExecutionContext,
  type PayloadOf,
  type ResultOf
} from 'better-nest-mq'
import { postgres } from 'better-nest-mq/postgres'

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
export class ReportsQueue extends QueueService {
  @Job({ name: 'generate', version: 1 })
  @Retry({
    attempts: 3,
    backoff: { type: 'exponential', initialDelayMs: 1_000, factor: 2 }
  })
  @JobTimeout(60_000)
  readonly generate = this.job({
    payload: z.object({ requestId: z.uuid(), values: z.array(z.number()) }),
    result: z.object({ count: z.int(), total: z.number() }),
    failure: z.object({ code: z.literal('empty-report'), retryable: z.boolean() }),
    idempotencyKey: (payload) => payload.requestId,
    retryable: (failure) => failure.retryable
  })
}

@Injectable()
export class ReportCalculator {
  calculate(values: readonly number[]) {
    return { count: values.length, total: values.reduce((sum, value) => sum + value, 0) }
  }
}

@Injectable()
@Worker({ name: 'reports-worker', concurrency: 8 })
export class ReportsWorker {
  constructor(private readonly calculator: ReportCalculator) {}

  @Process(ReportsQueue, 'generate', { concurrency: 4 })
  async generate(
    @JobData() payload: PayloadOf<ReportsQueue['generate']>,
    @JobContext() context: JobExecutionContext
  ): Promise<ResultOf<ReportsQueue['generate']>> {
    context.signal.throwIfAborted()
    if (payload.values.length === 0) {
      throw new JobFailureException<FailureOf<ReportsQueue['generate']>>({
        code: 'empty-report',
        retryable: false
      })
    }
    return this.calculator.calculate(payload.values)
  }
}

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
  providers: [ReportsWorker, ReportCalculator]
})
export class ApplicationModule {}
```

Run explicit PostgreSQL migrations before initializing this module; see connections.md. The example calculates a real result without placeholder I/O. An application doing external work should pass the context's signal and an idempotency key to clients that support them. Nest legacy decorators and emitted constructor metadata must be enabled in the consuming TypeScript configuration.

## Publishing and waiting

Any Service importing the queue's feature module can inject ReportsQueue normally. After application initialization:

```ts
const id = await reports.generate.enqueue({
  requestId: crypto.randomUUID(),
  values: [10, 20, 30]
})

const result = await reports.generate.awaitResult(id, {
  timeoutMs: 30_000,
  pollIntervalMs: 100
})
// result: { count: 3, total: 60 }
```

A job descriptor is inert until the owning application binds it to a ready connection. A manually constructed, contract-only or closed queue cannot publish and rejects with MqJobException. Calls from provider constructors are too early. Contracts may still be validated/encoded independently of a connection.

| Method                          | Implemented behavior                                                                   |
| ------------------------------- | -------------------------------------------------------------------------------------- |
| enqueue(input, options?)        | Validate the schema input, encode it and publish through the engine                    |
| enqueueDecoded(value, options?) | Accept the decoded output type and run its explicit inverse encoding                   |
| enqueueMany(items)              | Validate all payloads, then call the engine's batch publication API                    |
| prepare(input, options?)        | Produce encoded, serializable request data with connection routing, without publishing |
| poll(id)                        | Read a job of this exact queue/name/version and decode its result/failure              |
| awaitResult(id, options?)       | Poll for a decoded terminal result with an optional signal and wait timeout            |
| execute(input, options?)        | Enqueue and wait; never invoke the handler locally                                     |
| attempts(id)                    | Read the persisted attempt history and decoded failure/result fields                   |
| cancel(id)                      | Cancel pending work or request cooperative cancellation of active work                 |
| retry(id, schedule?)            | Administratively requeue a failed/cancelled job with a fresh attempt budget            |
| promote(id)                     | Make a delayed job immediately eligible                                                |

Batch items have `{ payload, options? }`; methods return IDs or typed Promise results, not engine Results. Full payload validation precedes any batch write. The adapter determines the actual batch transaction guarantee; a uniform API does not make all future adapters atomic.

`execute` separates publication and waiting options:

```ts
const result = await reports.generate.execute(input, {
  enqueue: { priority: 10, delayMs: 1_000 },
  wait: { timeoutMs: 30_000, pollIntervalMs: 100 }
})
```

A caller timeout or AbortSignal stops waiting only. It does not cancel the persisted job. In particular, a pre-aborted wait passed to execute does not undo its preceding enqueue. Use explicit cancel when changing the durable job is intended. Waits currently support polling only; durable-event wakeups are not exposed in this milestone. Avoid aggressive polling across large numbers of clients.

## Schemas and serialization

InputOf describes the schema's wire input; PayloadOf/ResultOf/FailureOf describe decoded outputs. Explicit Zod or generic Standard Schema codecs preserve values such as Date without double-applying transforms. The worker receives the decoded payload; the database receives validated JSON. A plain schema cannot invent the inverse of a one-way transform. See contracts.md.

Payloads are checked before publication and again when read by the worker. Handler results and known domain failures are checked before persistence and revalidated on client reads. Invalid handler output is an encoding failure, never a successful job. Invalid domain-failure content does not masquerade as a valid typed failure. Validators/encoders must be deterministic and side-effect-free because these boundaries can invoke them more than once.

`poll` deliberately exposes payload as portable JSON, while result and typed failure data are decoded. Snapshots contain identifiers, state, metadata, timestamps and attempt counts, but not a lease token. Attempt views expose outcomes such as completed/retried/failed/cancelled, retry timing and decoded failures. These diagnostic views do not grant ownership of a running attempt.

## Retries and errors

Module, queue, job, decorator and enqueue settings resolve into the published job policy. Enqueue accepts `retry: { attempts, backoff? }`, replacing the entire earlier retry policy rather than partially combining variants. `attempts` is the total handler-attempt budget, including the first execution. Fixed, linear and exponential backoffs, jitter and caps are translated to the upstream persisted representation.

A JobFailureException contains a declared domain failure. Its payload is validated, then the job's retryable predicate determines eligibility. A terminal domain failure rejects awaitResult with JobFailureException containing the decoded data. Unexpected exceptions stay defects and use `retryDefects: false` by default; explicit true opts them into the configured attempt budget. It does not turn defects into domain failures.

Execution timeout, decode/encode failure and cancellation retain distinct persisted categories. MqJobException wraps operation or untyped terminal failures; JobCancelledException identifies a cancelled result. JobWaitTimeoutException and JobWaitAbortedException concern the caller's wait, not execution. Causes are trusted diagnostics and may contain infrastructure details; do not serialize them indiscriminately in HTTP responses.

Executable timeoutMs must be positive. A zero timeout and named custom retry providers are rejected during executable contract compilation, before store acquisition. Contract-only metadata can still represent those declarations for inspection. Custom provider policy execution is a remaining extension, not a silent fallback.

Idempotency keys and explicit job IDs are passed to the engine. Duplicate publication can return the same ID; this does not make external side effects exactly-once. Design handlers to tolerate redelivery. Queue/name/version guards prevent another job definition from accessing a guessed ID. These guards are contract scoping, not tenant authorization; applications remain responsible for tenant access controls.

## Worker discovery and dependency injection

Workers are class providers registered in Nest modules. Discovery validates all processors before opening storage: queue/property references, duplicate processors, worker names, method descriptors and argument metadata. Parameters must explicitly use JobData or JobContext. Getters are not evaluated as processor methods. Processors inherited from a base class remain available when subclasses add processors; an overridden implementation needs its own parameter annotations so a changed parameter order is not inferred incorrectly.

A single JobDefinition instance cannot be reused to claim multiple durable identities. Provider aliases of the same registered Service do not create duplicate processors. Queue contracts stay singleton/static; request/transient-scoped worker dependencies are resolved using a fresh Nest ContextId for each attempt. No HTTP Request is fabricated, and this does not promise a new destructor protocol for request-scoped Nest providers.

The current pipeline is an MQ invocation, not the HTTP transport. Method/class UseGuards, UsePipes, UseInterceptors and UseFilters metadata is rejected instead of silently ignored. Application-wide HTTP enhancers do not apply to MQ handlers. A future MQ-specific enhancer pipeline must specify its own execution context and tests before claiming compatibility. Business validation can run in a Service or the shared schema contract today.

Use explicit class providers for workers. Factory/value-provider workers and arbitrary parameter decorator adapters are not supported. TypeScript does not automatically compare a reflected method's parameter annotation with its job; use PayloadOf/ResultOf and rely on the mandatory runtime schema boundary as well.

## Concurrency, cancellation and shutdown

Worker concurrency limits the local supervisor. Process concurrency limits that particular handler within the worker. These are not cluster-wide controls. Global/per-key concurrency and rate-limit declarations are implemented through storage-backed QueueControls; see controls.md for explicit policy deployment and validation. Runtime custom retry providers remain a separate extension. Local limits are never presented as distributed coordination.

JobContext includes jobId, queue/name/version, connection, attempt budget, delivery, workerId, metadata and the cooperative signal. The engine owns the lease and heartbeat. Active cancellation first validates the job identity and records a cancellation request; the supervisor observes it and performs a fenced terminal settlement. cancel() returning means the request was accepted, not necessarily that an active handler has stopped. A job finishing concurrently may make cancellation fail explicitly. A non-cooperative side effect is not undone.

All worker layers share the existing application runtime with stores and Clock. Clients are bound before lazy workers activate. Shutdown detaches clients, closes admission and lets the supervisor drain/abort according to the configured grace policy before stores and owned pools are released. Partially failed activation rolls back resources and detaches bindings. There is no per-job runtime and no automatic process signal handler.

MqWorkersService exposes local snapshots and awaitIdle({ timeoutMs?, signal? }). Idle means no currently active local attempts; it does not assert that every delayed/waiting job in the database is finished. MqConnectionsService remains available for live connectivity probes.

## Producer-only deployments and verification

Set `execution: { workers: false }` and import only shared queue modules for API/producer deployments. Workers default to enabled only for registered Worker providers; importing a queue alone never starts a consumer. Keep the same connection name, schema, namespace and contract version across producer/worker processes. The named connection token participates in the PostgreSQL storage address.

The packed consumer tests install an actual tarball outside the repository with TypeScript 6/7 and execute under Node/Bun. The PostgreSQL job verifies producer shutdown, worker startup in a new context, idempotency, decoded codecs, retries, terminal failures and persisted results after both contexts close. Unit/integration tests additionally cover concurrency, active cancellation, scope isolation, identity guards, metadata edge cases and draining before store release.

The repository remains version 0.0.0 and is not published to npm. Flows, persistent schedules, transactional outbox/ORM transaction bridges, other storage wrappers and durable event subscriptions remain separate milestones. Distributed QueueControls are available as documented in controls.md. Native PostgreSQL outbox transactions and managed publication are available as documented in outbox.md; publisher and worker enablement are independent.
