# Event-assisted result waits

The job facade can wait using the existing engine's durable event-log strategy. An event is a wake-up hint: success, failure and cancellation are always confirmed by rereading the persisted job and decoding its declared schemas. The library does not substitute an event payload for a result or create another subscription supervisor.

## Opt in on the waiting application's connection

```ts
import { MqModule } from 'better-nest-mq'
import { postgres } from 'better-nest-mq/postgres'

MqModule.forRootAsync({
  useFactory: () => {
    const connectionString = process.env.DATABASE_URL
    if (!connectionString) throw new Error('DATABASE_URL is required')
    return {
      connections: {
        primary: postgres({
          connectionString,
          schema: 'mq',
          namespace: 'reports-app',
          events: true
        })
      },
      execution: { workers: false }
    }
  }
})
```

Register your queue through `MqModule.forFeature` as usual. The reader uses the same named connection, native pool and private application runtime as the queue. With an application-owned pool, use `postgres({ pool, schema, namespace, events: true })`; the library does not close that pool. Events may coexist with flows, schedules and transactional outbox on the same connection.

Run explicit PostgreSQL migrations before startup. Enabling a reader never invokes the migrator or promotes event persistence to required mode. The native writer can establish optional event persistence as part of its existing behavior. `events: false` means this app has no event reader; it is not an instruction to disable event writes from this or other apps.

Producer/worker processes must use identical database/schema/namespace/connection names to share jobs and events. Internally the reader is constructed against the stable raw `nestjs/<name>` token, then aliased to the operation token within the runtime. That alias does not hash events into a second physical namespace. A worker that only executes jobs does not need `events: true`; the application selecting event waits does.

## Await a result

For an injected queue with the `generate` job:

```ts
const id = await reports.generate.enqueue(input)
const result = await reports.generate.awaitResult(id, {
  strategy: 'events',
  pollFallbackMs: 5_000,
  timeoutMs: 30_000,
  signal: abortController.signal
})
```

Publication and waiting can still be combined:

```ts
const result = await reports.generate.execute(input, {
  wait: {
    strategy: 'events',
    pollFallbackMs: 5_000,
    timeoutMs: 30_000
  }
})
```

`execute` enqueues first and then waits; it never invokes a handler directly. A caller-aborted wait does not undo the enqueue. Input/result/failure types and explicit codecs are the same as the other job APIs. No EventStore, Effect, Result, Runtime or engine token is imported by application code.

Polling remains the default and needs no reader:

```ts
await reports.generate.awaitResult(id, {
  pollIntervalMs: 100,
  timeoutMs: 30_000
})
```

`pollIntervalMs` belongs to polling; `pollFallbackMs` belongs to the events strategy. Their combinations are checked by TypeScript and runtime validation. Both intervals must be positive safe integers and fit the supported JavaScript timer range. `timeoutMs` may be zero but also must fit that range. Event fallback defaults to 5000 ms; default polling remains 100 ms. An unknown strategy or incompatible option fails before the wait begins.

## Races, fallback and cancellation

The facade delegates the race-safe event wait sequence to the existing engine. It establishes a cursor, checks the job, reads terminal events and checks the job again before waiting. A job already completed before registration is returned without waiting for another event. Result lookup remains scoped to queue/name/version; an event for a different job contract cannot provide its result.

A missed wake-up, expired cursor or runtime reader failure does not make the event log the only source of truth. The engine uses bounded polling fallback and cursor recovery to inspect the authoritative job. The per-wait cursor is internal and ephemeral: this method does not expose a durable subscriber checkpoint or guarantee delivery of every intermediate event.

Selecting events with no explicitly enabled ready reader rejects with MqJobException, including when the job has already completed. This catches configuration mistakes rather than pretending the requested strategy is configured. An event-reader acquisition/probe failure rejects startup and rolls back acquired resources. Runtime failures after successful initialization are the separate fallback case.

Caller timeout produces JobWaitTimeoutException; caller abort produces JobWaitAbortedException. They stop only that wait. Explicit job cancellation remains the separate `cancel` operation, and terminal cancellation rejects with JobCancelledException. Known domain failures remain JobFailureException with schema-validated decoded data.

Normal application shutdown detaches clients and aborts/drains admitted work according to the existing runtime policy. Cooperative event waits stop before stores and owned resources are released. This is not forced cancellation of arbitrary SQL or non-cooperative application code.

## Operational limits

This is event-log-assisted waiting, not a promise of zero polling, lower database load or immediate push delivery. The pinned PostgreSQL event reader itself polls its log internally, and the result strategy retains periodic job fallback. Use measurements with your job volume and latency requirements when choosing between strategies. A large number of concurrent waits still consumes queries and pool capacity; the facade does not add a process-wide multiplexed subscription broker.

No retention policy, required-writer activation, event administration API, SSE endpoint or replay subscription is installed. Event-log retention/cursor limitations remain those of the underlying adapter. Ordinary job history, durable events and process-local diagnostics are different facilities. Do not use result waiting as a substitute for an application outbox or resumable business event consumer.

## Verification and dependencies

The source tests use real Nest contexts and native memory stores. They verify entry into the matching event reader, completion before registration, timeout/abort without durable cancellation, missing-reader rejection, lost wake-up fallback, runtime reader errors, startup probe rollback and application shutdown.

The installed-package fixture uses real PostgreSQL and distinct producer/worker contexts, confirming JSON-looking strings, JSON null, Date codecs, typed failures, cancellation, timeout, persisted event/job namespace matching, unchanged optional activation and reads after restarting the reader. It runs under Node/Bun with both supported TypeScript compilers alongside the existing flow/outbox/schedule/control matrix.

No new dependencies or required application peers are introduced. Internal engine and adapter packages stay ordinary dependencies owned by better-nest-mq; the application installs only the Nest/schema/native-driver dependencies it actually uses. Version remains 0.0.0 until an explicitly authorized release.
