# Durable flows — work in progress

**Status: implemented on `feat/durable-flows` / PR #9, not merged and not qualified for PostgreSQL production use.** The stable main branch remains the schedules delivery. The current published PostgreSQL adapter cannot compose its JobStore with the persisted `waiting-children` state written by FlowStore. This is tracked in [better-effect issue #387](https://github.com/nitoba/better-effect/issues/387).

The examples below describe the implemented candidate API, not a released feature. The reference-store tests exercise the phases, schemas and Nest lifecycle, but do not establish PostgreSQL correctness. Do not bypass the failing database gate or ship this branch as a finished flow implementation.

## Confirmed upstream blocker

The independent reproduction in `tests/postgres/flow-protocol.ts` imports only published engine/adapter packages, never Nest or this facade. It applies native migrations, uses one native pool/schema/namespace, claims a parent and successfully persists a one-child fan-out. FlowStore then reads the parent as `waiting-children`, with one pending child.

The same parent fails native JobStore `getJob`, `heartbeat` and `release` with `JobDefinitionError: unsupported job state`. The heartbeat cannot classify the old lease as lost, and release cannot return the expected ownership failure. In the installed consumer this also prevents clean completion of the intended attempt/shutdown path. Reading administration directly through FlowStore does not repair the engine's heartbeat and release operations.

Verified combination: better-effect 0.14.0, better-effect-mq 0.1.2, better-effect-mq-postgres 0.1.3, better-result 3.0.1, Bun 1.4.2 and PostgreSQL 16.15. The recorded failing run is [34616650978](https://github.com/nitoba/bettter-nest-mq/actions/runs/34616650978). These are version-specific observations, not an assertion about every adapter or future release.

To reproduce, use a dedicated test database:

```sh
bun install --frozen-lockfile
MQ_TEST_DATABASE_URL='postgresql://user:password@localhost/test_database' \
  bun tests/postgres/flow-protocol.ts
```

The script creates and removes its own random schema. A failing exit is the currently observed defect, not a successful integration result. The correction belongs at the explicit v1/v2 adapter/Worker compatibility boundary; do not coerce suspended parents into ordinary waiting jobs or silently widen the frozen v1 contract.

## Candidate Nest API

Queue Services still declare typed parent and child jobs. A Worker Service owns separate FanOut and Collect methods. No runtime, Effect program or coordination outbox is exposed to the application.

```ts
import { Injectable } from '@nestjs/common'
import { z } from 'zod'
import {
  Queue,
  QueueService,
  Job,
  Worker,
  Process,
  JobData,
  Flow,
  FanOut,
  Collect,
  FlowData,
  FlowChildren,
  flowJob,
  flowChildren,
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
  readonly double = this.job({ payload: z.number(), result: z.number() })
}

const sum = flowJob(CalculationsQueue, 'sum')
const double = flowJob(CalculationsQueue, 'double')

@Injectable()
@Worker({ name: 'calculations', concurrency: 1 })
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

Register the Queue Service with `MqModule.forFeature` and the Worker as a normal provider. The parent is published through its normal job descriptor. In this branch, `postgres({ flows: true })` enables the flow resource, but the published-adapter defect above prevents treating that opt-in as a supported PostgreSQL deployment.

`flowJob` creates an immutable typed reference. `flowChildren` creates a finite, copied JSON-input plan without publication; its payload type comes from the referenced job. Runtime discovery also verifies registration. FanOut validates every child before submitting a manifest. Stable child keys identify entries across recovery. Empty plans, input/output codec distinctions and ordinary scoped Nest injection are covered by reference-store tests.

## Result collection and policy boundaries

Collect receives a phase-owned `FlowResultsReader`. Completed, failed and cancelled outcomes are discriminated; results and available declared domain failures are decoded using the referenced contract. FlowData is decoded parent data, whereas child plans carry schema inputs. Date codecs therefore use ISO strings in plans and Date values after decoding.

`page(reference, { limit, cursor })` defaults to 100 and permits at most 1000 manifest entries. Cursors advance over the entire manifest before reference filtering, so an empty filtered page may still have a next cursor. Only cursors issued by that active Collect reader are accepted. `all(reference, { maxItems })` requires a bound and rejects overflow. Closing the phase prevents new reads and drains admitted ones.

**The upstream page implementation loads a flow snapshot and slices it.** This is not SQL cursor pagination or constant-memory streaming. Manifest size and child outputs must be bounded separately.

The candidate policies delegate to upstream `onChildFailure: 'continue' | 'fail'`. Continue exposes terminal child outcomes to Collect; fail is intended to terminate the parent and cooperatively cancel unfinished siblings without Collect. Fail-fast failure propagation may require compatible parent/child failure schemas. PostgreSQL fail-fast, cancellation and nested-flow behavior are still unqualified, because the real database scenario stops at the earlier blocker.

`MqFlowsService.get(parentReference, id)` reads the materialized flow; it returns undefined before a manifest exists. `cancel` requests cancellation using the registered flow/job identity. Identity checks are not tenant authorization, and cancellation is not a guarantee that already-started external side effects have been reversed or stopped. No HTTP administration endpoint or manifest reset/replay API is installed.

## Resource integration and explicit restrictions

The candidate integration shares the existing application Runtime and PostgreSQL pool. It supplies FlowStore with the namespace derived from the raw JobStore token, because the pinned adapters do not derive named namespaces identically. Flow queries use a separate non-owning decoded-JSON view; the ordinary JobStore uses its encoded-JSON view. Native application parsers remain untouched. The namespace and parser unit tests do not resolve the separate state-protocol failure.

Participating workers receive registered flow definitions for reporting/reconciliation. `execution.workers: false` keeps phase execution disabled. Schedule and application-outbox publisher enablement remain independent; migrations are explicit.

Unsupported combinations fail instead of being simulated: a flow-owning Worker needs at least one real Process handler in the pinned engine; a parent cannot also have an ordinary handler; ambiguous identities, recursive definition cycles, incompatible parent controls and per-key child dispatch are rejected. No fabricated handler, local substitute for persistence, second runtime or serialized executable function is introduced. Nested definitions are represented in the candidate API, but their complete database recovery remains to be verified.

## Verification and resumption

The source/reference gate at commit `89c66e8` passed both TypeScript checks, the 261-test unit/Nest suite, formatting, build, type-aware lint and publint in development run 34616650927. That run **failed** its installed PostgreSQL flow scenario. The isolated published-protocol run separately confirmed all three compatibility errors. A successful reference test is not a database guarantee.

The packed consumer scenarios already include process death after manifest creation, two replacement processes, JSON/date values, bounded collection, nesting, continue/fail and cascade cancellation. Their presence is not evidence that they all pass. Re-run the complete suite after the upstream correction is available through an explicitly agreed dependency path.

Before merge: resolve issue #387, rerun the pure protocol reproduction and full installed Node/Bun/TypeScript matrix, confirm all existing controls/outbox/schedules regressions, review the complete diff and require green read-only CI on the exact final head. Keep main unchanged until then. No npm publication, production deployment, dependency upgrade or successful PostgreSQL flow qualification is claimed by this checkpoint.
