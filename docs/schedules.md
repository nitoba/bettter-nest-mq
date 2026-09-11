# Persistent schedules

This integration uses the existing better-effect-mq scheduler and PostgreSQL schedule store. Queue metadata declares recurring work; persisted schedule records retain cadence, pause state, revision and next occurrence. Competing schedulers use the store's atomic occurrence/revision fencing instead of independently calling enqueue from in-memory cron callbacks.

## Declare a recurring job

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import { Job, Queue, QueueService, Schedule } from 'better-nest-mq'

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
export class ReportsQueue extends QueueService {
  @Job({ name: 'daily', version: 1 })
  @Schedule({
    key: 'daily-summary',
    cron: '0 9 * * *',
    timeZone: 'America/Fortaleza',
    payload: { scope: 'all' },
    misfire: { strategy: 'run-once' },
    overlap: 'skip'
  })
  readonly daily = this.job({
    payload: z.object({ scope: z.literal('all') }),
    result: z.object({ processed: z.int().nonnegative() })
  })
}
```

Processing remains an ordinary Worker Service with `@Process(ReportsQueue, 'daily')`; the Schedule decorator is not the handler. Several Schedule decorators can refer to the same job. Every schedule has a stable group/key address within its connection; changing a class/property name does not change that address.

Use exactly one of `cron` (five fields, minute resolution) or `everyMs` (positive safe integer). The timezone defaults to UTC and affects cron wall-clock interpretation. Intervals are elapsed milliseconds, not local calendar days. Static payloads are JSON schema inputs, copied on declaration and decoded/encoded through the job's codec before persistence. Dynamic JavaScript functions are not serialized. Schedule a coordinator job when each execution needs to query fresh business data.

## Enable storage and deploy definitions

The PostgreSQL connection opts into the schedule store:

```ts
postgres({ pool, schema: 'mq', namespace: 'reports-app', schedules: true })
```

It uses the same native pool, JSON parser compatibility layer and durable raw connection namespace as the JobStore. It does not allocate another pool or runtime. `outbox: true` may be enabled on that same connection. Owned/borrowed pool rules remain unchanged. Existing upstream migration files contain the schedule layout; run the explicit migration helper as part of deployment, never assume module initialization will create tables.

One coordinated deployment context registers definitions using:

```ts
MqModule.forRoot({
  connections: {
    primary: postgres({ pool, schema: 'mq', namespace: 'reports-app', schedules: true })
  },
  schedules: { mode: 'reconcile', group: 'reports-deployment' },
  execution: { workers: false, scheduler: false, outboxPublisher: false }
})
```

Import `MqModule`, `postgres` and the queue's `MqModule.forFeature([ReportsQueue])` in the deployment application. Startup compiles and validates all declared payloads before acquiring resources, then checks persisted identity/capabilities before applying definitions. Successful writes on one connection are not rolled back if a later connection fails: this is not a cross-database deployment transaction.

Normal replicas use `schedules: { group: 'reports-deployment' }`, which defaults to **validate** mode. Missing or changed declarations reject startup rather than overwrite deployed definitions. Upsert and reconcile operations also require explicit reconcile mode. All processes must agree on connection name, schema, namespace and group; a connection rename is a change of durable address, not merely DI configuration.

Reapplying unchanged definitions preserves revision and next occurrence. Operator pauses and runtime cursor fields are preserved. Omitted schedules are not removed or paused automatically. Use a coordinated writer for definition changes; this is not a leader election or a compare-and-swap deployment protocol. A property override with its own schedule declarations replaces that property's inherited list; otherwise inherited job schedules retain their declarations under the concrete queue identity.

## Separate execution roles

`execution.scheduler` controls the scheduler independently of `workers` and `outboxPublisher`. It defaults to true but starts nothing without an opted-in schedule store. A scheduler-only process can enqueue durable jobs for a worker process that starts later. Producer-only and deployment processes can disable it explicitly.

Scheduler configuration also includes `sweepIntervalMs` (default 1000), `batchSize` (100), `maxStoreRetries` (3) and `retryDelayMs` (25). Timer delays must fit JavaScript's timer range; a schedule's persisted everyMs is not itself a timer and may represent longer periods. These are store-operation retries, separate from the emitted job's execution attempt policy.

The scheduler serves the module's default group (`nestjs/schedules` when omitted) and any explicitly declared groups in that connection. A dynamic schedule cannot introduce an unserved group. Multiple replicas may sweep those groups concurrently; only the accepted fenced tick advances the occurrence and emits its jobs. This prevents duplicate publication of the same retained occurrence, not at-least-once redelivery of the resulting job to handlers.

## Missed occurrences and overlap

The pinned upstream protocol has precise semantics:

| Policy | Behavior on a due record |
| --- | --- |
| `run-once` (default) | Emit one job for the pending occurrence and advance beyond the current sweep time. |
| `catch-up` | Emit up to maxOccurrences overdue slots during this tick. Remaining backlog may be handled by later ticks, including another replica. |
| `skip` | Emit no job for due slots; advance to a future slot. |

Catch-up is bounded to 256 occurrences per tick, the pinned store limit. It is not a global catch-up budget across multiple sweeps or replicas. **The current upstream skip policy has no lateness grace threshold:** every observed due slot is discarded. Do not choose it expecting ordinary periodic jobs to run while only sufficiently old ones are ignored. Use run-once for ordinary recurring work. This facade preserves the protocol rather than silently inventing a new timing algorithm.

`overlap: 'skip'` prevents a new occurrence while the previous occurrence tracked by that schedule is unfinished; `allow` permits overlap. This is scoped to that schedule, not a substitute for global queue concurrency. Pausing prevents future schedule ticks and does not cancel a job already enqueued. Resuming keeps the pending cursor, so the chosen misfire policy governs overdue work.

Changing an emitted job's retry policy follows module/queue/job/decorator defaults plus schedule overrides. Scheduled jobs cannot inherit a relative enqueue delay: the cadence determines occurrence time. Absolute timing/idempotency fields are owned by the schedule protocol, not caller overrides. Removing/recreating a schedule should be treated as deliberate administration; it is not a rewind guarantee for arbitrary already-emitted jobs.

## Administration through Nest Services

Inject MqSchedulesService normally:

```ts
const record = await schedules.get(ReportsQueue, 'daily', 'daily-summary')
const records = await schedules.list(ReportsQueue, 'daily', { paused: true, limit: 20 })

await schedules.pause(ReportsQueue, 'daily', 'daily-summary')
await schedules.resume(ReportsQueue, 'daily', 'daily-summary')
```

A reconcile-mode application may also call typed upsert:

```ts
await schedules.upsert(ReportsQueue, 'daily', {
  key: 'hourly-summary',
  everyMs: 3_600_000,
  payload: { scope: 'all' },
  overlap: 'skip'
})
```

Upsert infers the payload input from the referenced job. Runtime schema validation still occurs. Static decorator metadata cannot enforce TypeScript correspondence with its property by reflection alone, so invalid static payloads reject before startup writes. `get/list/pause/resume/remove` check the registered job identity; another job cannot access an address merely by guessing its key. These checks are not tenant authorization. No administrative HTTP endpoint is installed.

Optional group parameters select a served group. `remove` returns false for a missing address and never cancels an emitted job. Read-only deployment mode does not prohibit explicit operational pause/resume/remove; protect these methods when exposing administration. Concurrent definition reassignment/removal must be coordinated by the application.

`scheduler()` returns local state, active tick count and reported error count, or undefined when no local scheduler runs. `sweep()` requests a pass through the actual upstream scheduler and rejects when the scheduler reports a failed operation. It does not execute handlers directly. State is not continuous database-health monitoring; use connection probes and operational diagnostics appropriately.

## Codecs, dispatch keys and compatibility

The same schema encoding rules apply to schedules, emitted jobs and results, including scalar JSON, null and explicit Date codecs. The private PostgreSQL JSON parser view remains isolated from the application's native parsers. Internal Effect/MQ/adapter packages remain ordinary dependencies managed by better-nest-mq, not required peers for consumers.

**The pinned schedule record does not contain a dispatchKey field.** Therefore a scheduled job that derives a dispatch key or targets a per-key-concurrency queue is rejected, including checks against persisted controls at initialization/upsert. Schedule an unkeyed coordinator job that publishes appropriately keyed child work instead. Do not enable incompatible per-key controls live while a scheduler is running; this integration does not perform a distributed feature-policy migration. Global concurrency/rate limits still belong to the ordinary worker claim path.

Schedulers trust their persisted definitions within configured groups. Adding new external writers, deleting required schema tables or reassigning groups while replicas run requires coordinated administration; validation at startup is not an ongoing authorization layer. Clocks on participating hosts must be synchronized for meaningful wall-clock scheduling.

## Shutdown and testing

Scheduler layers share the existing application Runtime, Clock and stores. Initialization validates definitions before activating consumers; failure releases acquired resources. Shutdown quiesces scheduler loops and drains admitted ticks before closing stores and owned pools. It does not forcibly interrupt arbitrary driver calls. The consuming Nest application remains responsible for normal signal/shutdown-hook setup.

The tests use actual upstream stores and actual installed package consumers, not a mocked cron callback. PostgreSQL qualification starts two independent Node scheduler processes and checks persisted occurrence IDs/counts, JSON values, cron timezone, bounded catch-up, skip/overlap, pause/resume and later worker execution after schedulers exit. Consumer projects compile with TypeScript 6/7 and run under Node/Bun. Existing outbox, distributed controls and JSON/pool ownership regressions remain enabled.

Flows, ORM transaction bridges, other adapters, custom retry providers, MQ enhancers and durable-event subscriptions remain separate roadmap items. This delivery does not publish to npm or deploy a production application.
