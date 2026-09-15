# SQLite schedules — release-gated candidate

This branch implements the next SQLite resource bundle. It is **not qualified with the currently pinned published SQLite adapter 0.1.2**: that version can rewrite JSON-looking strings when reading or pausing schedules. The engine correction is merged in better-effect PR #391 at `788f9526f2d25e8575b2a4b18286fc466d07a693`.

Before merging this integration, publish a corrected adapter release, pin that exact version with a Bun-generated lockfile, remove the candidate-archive workflow and pass the unchanged normal CI. The candidate workflow uses actual archives built from the exact corrected source only in a disposable test checkout. A successful candidate test is not a registry release or production approval. No version override is exposed to application code.

## Configuration

Both `better-nest-mq/sqlite/node` and `better-nest-mq/sqlite/bun` accept the same boolean opt-in:

```ts
MqModule.forRoot({
  connections: {
    primary: sqlite({
      path: './data/jobs.db',
      namespace: 'reports',
      schedules: true,
      events: true
    })
  },
  schedules: { mode: 'validate' }
})
```

Import `MqModule` from `better-nest-mq` and `sqlite` from the entry matching the execution host. Create the containing directory and run `migrateSqlite({ path })` explicitly before startup. Existing migrations already contain the schedule layout; this integration adds none and never runs migration during startup. A supplied native database stays caller-owned, with its pragmas unchanged by default.

Schedules borrow the already-acquired database and use `JobScheduleStore.for(rawNamedJobStore)` through the native adapter. The operation alias exists only inside the existing application runtime. There is no additional file connection, runtime, timer-based scheduler implementation or changed durable namespace.

## Declarations and administration

The existing declaration and service APIs are unchanged. Apply a schedule to a job property on a registered QueueService:

```ts
@Job({ name: 'summarize', version: 1 })
@Schedule({
  key: 'daily-summary',
  cron: '0 9 * * *',
  timeZone: 'America/Fortaleza',
  payload: { values: [10, 20, 30] }
})
readonly summarize = this.job({
  payload: z.object({ values: z.array(z.number()) }),
  result: z.object({ total: z.number() })
})
```

Import Job/Schedule from better-nest-mq and z from zod. Register the queue with forFeature. Worker services keep Process/JobData and ordinary Nest DI. Dynamic upsert, contract-scoped get/list, pause/resume/remove, reconcile reports and explicit sweep continue through MqSchedulesService. See [persistent schedules](schedules.md) for the full API.

Run one coordinated deployment with `schedules: { mode: 'reconcile' }`, then start ordinary replicas in the default validate mode. Validation never creates missing declarations or overwrites changes. Unchanged reconciliation retains operator pauses, revision and occurrence cursor, and does not remove omitted schedules. Changing connection name or namespace selects another durable identity even in the same physical file.

`execution.scheduler` is independent from `execution.workers`. A scheduler-only process can enqueue occurrences while workers are offline. Workers started later consume those persisted jobs normally. Disabling the scheduler does not delete its definitions. Enabling events is optional; a completed scheduled job supports the same result waiting and schema codecs as an ordinary job.

## Guarantees and limits

Native SQLite atomic ticks fence competing schedulers using revision and occurrence time. Tests run two independent processes against a local file and assert one persisted job for the retained occurrence. This is not an exactly-once external-effect guarantee: worker handlers remain at-least-once and need their own idempotency.

The existing misfire policies, overlap rules, cron/timezone interpretation, per-tick catch-up bound and rejection of keyed scheduled destinations remain unchanged. This integration does not add global catch-up quotas, automatic leadership, external database transactions, SQLite flows/outbox or durable event subscriptions. SQLite is synchronous local storage; do not interpret same-file process coordination as multi-host network-database support.

The corrected native snapshot preserves payload value/type through reads, pause/resume, reconciliation and ticks, including strings such as `"null"`, actual JSON null, arrays and explicit Date codecs. Previously corrupted rows cannot be repaired without an authoritative original value. No payload envelope or additional parsing workaround is introduced here.

## Verification

New native Nest tests cover inert/strict option validation, same-namespace persistence, scalar payload fidelity, administrative revisions/pauses, read-only replica startup, contract isolation, actual scheduling/event-result reads and owned/borrowed failure cleanup. The unmodified released adapter fails the fidelity regressions; the corrected archived adapter passes them.

Installed package fixtures run Node/Node, Bun/Bun, Node/Bun and Bun/Node reader/worker directions. They start two scheduler processes before making the same occurrences due, require exactly one persisted occurrence per schedule, compare raw JSON columns, stop both schedulers, then start a separate worker and reopen a result reader. TypeScript 6/7 and both host declaration boundaries remain enabled. The complete preceding PostgreSQL, SQLite jobs/events, controls, flows, outbox and Kysely matrix must pass with the final published dependency before merge.
