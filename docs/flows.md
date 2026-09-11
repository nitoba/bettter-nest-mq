# Durable flows

Durable flows are implemented through the Nest facade and persisted by the existing better-effect-mq flow protocol. The qualified PostgreSQL combination is `better-effect-mq@0.1.3` with `better-effect-mq-postgres@0.1.4`, both managed as internal dependencies of better-nest-mq. Applications do not install or configure those packages directly.

A flow separates a parent job into two durable phases. `@FanOut` validates and persists a finite child manifest. The parent then leaves its original lease. `@Collect` runs only after the parent becomes ready again and is acquired under a fresh lease. Waiting for children does not occupy an ordinary worker execution slot, and the facade does not replace this coordination with `Promise.all` or a process-local timer.

## Declare a flow

Queue Services keep the ordinary typed job declarations:

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import {
  Collect,
  FanOut,
  Flow,
  FlowChildren,
  FlowData,
  Job,
  JobData,
  Process,
  Queue,
  QueueService,
  Worker,
  flowChildren,
  flowJob,
  type FlowResultsReader
} from 'better-nest-mq'

@Injectable()
@Queue({ name: 'calculations', connection: 'primary' })
export class CalculationsQueue extends QueueService {
  @Job({ name: 'sum', version: 1 })
  readonly sum = this.job({
    payload: z.array(z.number()).max(100),
    result: z.number()
  })

  @Job({ name: 'double', version: 1 })
  readonly double = this.job({
    payload: z.number(),
    result: z.number()
  })
}

const sum = flowJob(CalculationsQueue, 'sum')
const double = flowJob(CalculationsQueue, 'double')

@Injectable()
@Worker({ name: 'calculations', concurrency: 4 })
@Flow({
  name: 'sum-doubles-v1',
  parent: sum,
  children: [double],
  onChildFailure: 'continue',
  maxChildren: 100
})
export class CalculationsWorker {
  @FanOut()
  split(@FlowData() values: number[]) {
    return [
      flowChildren(
        double,
        values.map((payload, index) => ({ key: `item-${index}`, payload }))
      )
    ]
  }

  @Collect()
  async finish(@FlowChildren() results: FlowResultsReader) {
    const children = await results.all(double, { maxItems: 100 })
    return children.reduce(
      (total, child) => (child.outcome === 'completed' ? total + child.result : total),
      0
    )
  }

  @Process(CalculationsQueue, 'double')
  double(@JobData() value: number) {
    return value * 2
  }
}
```

Register the queue through `MqModule.forFeature([CalculationsQueue])` and the Worker as a normal Nest provider. The parent is published using the ordinary `CalculationsQueue.sum` job descriptor.

`flowJob()` creates an immutable typed reference to a registered job. `flowChildren()` creates a copied, finite child plan; it does not enqueue anything immediately. Every child receives a stable key so recovery can identify the same manifest entry after a crash or process restart.

## PostgreSQL resource

Enable the flow store explicitly on the existing connection:

```ts
postgres({
  pool,
  schema: 'mq',
  namespace: 'calculations-app',
  flows: true
})
```

The flow resource shares the same application Runtime and native PostgreSQL pool as jobs, schedules and outbox resources. It does not create another process-global runtime or pool. Migrations remain explicit and must be applied before application startup.

The named JobStore namespace remains stable. The facade supplies the flow resource with the matching durable namespace and keeps its JSON decoding view private so application-level PostgreSQL parsers are not mutated.

## Phase and lease semantics

PostgreSQL fan-out atomically persists the manifest and relinquishes the parent job lease. The original FanOut attempt then ends without encoding its child plan as the parent result, settling the parent, or trying to release the old lease a second time.

When all required child state is ready, the parent is made claimable again. Collect executes under a **new active lease and a new delivery**. The persisted fan-out token is historical state, not permission to execute Collect.

Old parent leases are fenced. Heartbeat classifies a relinquished parent lease as lost without invalidating unrelated leases in the same batch, while stale release/settlement operations fail rather than rewriting a suspended parent.

The upstream v1 `JobStore.getJob()` contract intentionally remains v1 and does not expose the `waiting-children` state. Suspended-parent inspection therefore goes through the FlowStore/v2 boundary. The Nest integration maintains an explicit validated v2 read projection for public flow administration and restart recovery instead of widening or weakening the v1 job decoder.

## Child results and codecs

FanOut receives decoded parent data through `@FlowData`, but child plans contain **input representation** for the referenced child contract. For example, a Date codec uses an ISO string in `flowChildren()` and a Date after decoding in the child handler or result reader.

Collect receives a phase-owned `FlowResultsReader`. Its outcomes are discriminated:

- `completed` carries the decoded child result;
- `failed` carries the declared typed failure when available;
- `cancelled` has no successful result.

PostgreSQL JSON `null` is distinguished from SQL `NULL`. Scalar strings that resemble JSON remain strings, and the same explicit codec/round-trip rules used by ordinary jobs apply to flow payloads and results.

## Bounded collection

`page(reference, { limit, cursor })` defaults to a bounded page and accepts only cursors issued by the active Collect reader. The cursor advances over the complete manifest before reference filtering, so a filtered page may contain no items and still return a next cursor.

`all(reference, { maxItems })` requires an explicit maximum and rejects overflow instead of silently discarding children. The current upstream page operation reads a persisted flow snapshot and slices it; this is **not SQL cursor streaming or constant-memory iteration**. Bound `maxChildren`, manifest size and result size according to the workload.

Closing the Collect phase rejects new reader operations and drains reads that were already admitted.

## Failure policies

`onChildFailure: 'continue'` allows Collect to run with completed, failed and cancelled child outcomes. `onChildFailure: 'fail'` terminates the parent according to the flow protocol and cooperatively cancels unfinished siblings instead of invoking Collect.

Failure propagation still obeys the declared schemas. A child failure is not an exception tunnel: typed failure content is validated before it becomes an outcome. External side effects remain at-least-once concerns and should be idempotent where required.

Fail-fast does not imply that JavaScript or a remote system can be forcefully rolled back. Cancellation is cooperative, and already-started external effects are not automatically reversed.

## Recovery and multiple workers

Flow coordination is durable. Qualification includes a process that persists the manifest and is then killed with `SIGKILL`, followed by two independent replacement Worker processes that recover the same parent/children from PostgreSQL. Stable child identities prevent reconstruction from creating a different logical manifest.

Child terminal reports use the FlowStore route associated with the child's JobStore. The recovery sweep is an independent safety path: it rotates over known routes, parents and pending children so a small batch size cannot indefinitely starve work later in the set.

Startup recovery of known parents is intentionally bounded in this facade. If more than the supported recovery bound is found, startup fails explicitly instead of silently truncating the set. The current upstream snapshot API is not advertised as an unbounded SQL cursor.

## Nest DI and worker restrictions

FanOut and Collect methods are actual Nest provider methods. Constructor injection and request/transient-scoped business dependencies work normally; attempt-local `JobContext` is supplied internally by the worker runtime rather than becoming a dependency the application must provide.

Unsupported combinations fail before resource acquisition where possible:

- a flow parent cannot also have an ordinary `@Process` handler in the same Worker;
- ambiguous queue/name/version handler identities are rejected;
- parent controls that conflict with flow lease ownership are rejected;
- child per-key dispatch requirements are rejected when the flow protocol cannot persist the required dispatch key;
- invalid child schemas, duplicate child keys, recursive definition cycles and configured fan-out limits fail rather than being approximated.

No dummy handler is created to satisfy the upstream worker. No executable JavaScript function is serialized into the manifest.

## Administration

`MqFlowsService` provides contract-scoped inspection and cancellation. `get(parentReference, id)` returns the materialized flow after FanOut; before a manifest exists it returns `undefined`. `cancel()` validates the registered flow/job identity and delegates to the appropriate durable job/flow cancellation path.

These identity checks are not tenant authorization. If flow administration is exposed over HTTP, authentication and authorization remain application responsibilities. The package does not install an administrative HTTP controller.

## Guarantees and non-guarantees

The qualified flow implementation provides durable manifest persistence, stable child identities, fresh-lease collection, bounded recovery, typed result collection, fail-fast/continue policies and cooperative cancellation across process restarts.

It does **not** promise exactly-once external effects, automatic saga compensation, arbitrary JavaScript replay, cross-database atomicity, unbounded manifest streaming, or forceful cancellation of an external side effect. The emitted jobs retain at-least-once delivery semantics.

The engine and PostgreSQL adapter remain internal dependencies of better-nest-mq. Applications install the Nest package plus their selected native/schema peers such as `pg` and Zod; they do not install `better-effect`, `better-result`, `better-effect-mq` or its adapters manually.

## Qualification

The released dependency combination is verified through the normal source suite and installed-package consumers using TypeScript 6 and 7, Node and Bun, and PostgreSQL 16. Coverage includes:

- process death after manifest persistence and recovery by two independent processes;
- scalar, JSON-looking string, object, array and `null` values;
- explicit Date codecs;
- paged collection and bounded `all()`;
- typed child failure with `continue`;
- empty and nested flows;
- fail-fast without Collect;
- cooperative cascading cancellation;
- ordinary jobs, distributed controls, outbox and schedules in the same regression matrix.

The isolated protocol regression intentionally checks the supported boundary: FlowStore/v2 inspects the suspended parent, while v1 JobStore heartbeat and release safely fence the relinquished lease. It does not require v1 `getJob()` to expose a v2-only state.
