# Versioned retry providers and MQ enhancers

These APIs extend the existing supervisor. They do not implement another retry loop, create another runtime, change the persisted job envelope or require consumer-installed engine packages. Native claims, retry timestamps, lease fencing, timeouts and settlement remain authoritative.

## Named retry policies through Nest DI

Declare a stable name/version in the job contract and register the implementation as a normal class provider:

```ts
import { Injectable, Module } from '@nestjs/common'
import { z } from 'zod'
import {
  Job,
  JobContext,
  JobData,
  JobFailureException,
  MqModule,
  Process,
  Queue,
  QueueService,
  Retry,
  RetryPolicy,
  Worker,
  type JobExecutionContext,
  type MqRetryPolicy,
  type RetryPolicyContext
} from 'better-nest-mq'

@Injectable()
class RetryTuning {
  readonly baseDelayMs = 250
}

@Injectable()
@RetryPolicy({ name: 'upstream-busy', version: 1 })
class UpstreamRetry implements MqRetryPolicy<{ retryAfterMs: number }> {
  constructor(private readonly tuning: RetryTuning) {}

  decide(failure: { retryAfterMs: number }, context: RetryPolicyContext) {
    return {
      retry: true,
      delayMs: Math.max(failure.retryAfterMs, this.tuning.baseDelayMs * context.attempt)
    }
  }
}

@Injectable()
@Queue({ name: 'reports', connection: 'primary' })
class ReportsQueue extends QueueService {
  @Job({ name: 'calculate', version: 1 })
  @Retry({
    attempts: 4,
    backoff: { type: 'custom', policy: 'upstream-busy', version: 1 }
  })
  readonly calculate = this.job({
    payload: z.number(),
    result: z.number(),
    failure: z.object({ retryAfterMs: z.number().int().nonnegative() }),
    retryable: () => true
  })
}

@Injectable()
@Worker({ name: 'reports', pollIntervalMs: 100 })
class ReportsWorker {
  @Process(ReportsQueue, 'calculate')
  calculate(@JobData() value: number, @JobContext() context: JobExecutionContext) {
    if (context.attempt === 1) throw new JobFailureException({ retryAfterMs: 500 })
    return value * 2
  }
}

@Module({
  imports: [MqModule.forFeature([ReportsQueue])],
  providers: [ReportsWorker, UpstreamRetry, RetryTuning]
})
class ReportsModule {}
```

The application's root still configures its PostgreSQL connection with `MqModule.forRoot`. The example intentionally fails its first execution to demonstrate persisted retry behavior; a real worker throws a declared failure based on the downstream operation's result.

The provider's `decide` method must be **synchronous**, with singleton/static dependencies. Request/transient policies, missing consumed policies and duplicate name/version registrations fail before storage acquisition. Producer-only applications can publish, prepare, schedule and append outbox entries without the decision provider. Decoration alone is not provider registration.

The method receives decoded failure content, not `JobFailureException`, and a frozen `{ name, version, attempt, attemptsMax }` context. Attempts include the initial execution. `false` or `{ retry: false }` stops; `true` requests an immediate retry; `{ retry: true, delayMs }` uses a nonnegative safe-integer delay. The native validator rejects malformed decisions, thrown policy errors and accidentally returned promises without a new attempt. It also observes rejected promises rather than creating an unhandled rejection.

The job must declare a failure schema and an appropriate `retryable` predicate. The facade's default predicate is false: a custom policy does not override an explicit or default refusal to retry. The native attempt budget remains an upper bound and no decision is called once it is exhausted. Custom decisions apply to **typed failures**, not infrastructure errors, defect retries or timeouts; those retain the native rules. Do not perform network I/O or business effects inside a retry decision.

### Persistence and rolling deployments

Only the name/version tuple is added as the reserved string metadata field `__better_nest_mq_retry`. Callback functions, provider instances and DI tokens are never serialized. The native store persists the decided retry timestamp and delay normally. A new application context resolves its own implementation after a restart.

Workers verify the saved reference **before** guards, filters or business methods. Missing/mismatched references fail rather than executing an old job with another policy. Changing the implementation behind an unchanged name/version cannot be detected automatically: treat both retry and job versions as immutable deployment contracts, bump them together for incompatible changes, and retain old contracts/providers while their jobs drain.

Callers cannot set the reserved metadata key or replace a custom policy through enqueue, batches, prepare, schedules or flow plans. To override only the attempt budget, repeat the same name/version in the retry option. Outbox preparation revalidates the reference. Ordinary built-in retry overrides keep their existing producer behavior.

For flow children, use the declared backoff and optionally a different attempt budget with the same policy. As tracked in [better-effect#389](https://github.com/nitoba/better-effect/issues/389), the pinned engine normalizes a child backoff into a policy object but forwards it to the producer's numeric-backoff decoder. This facade therefore inherits the declared child backoff and explicitly rejects changes to it before creating a manifest; it does not silently drop the override. Custom child policies and ordinary inherited policies remain supported.

## Explicit MQ enhancer pipeline

Use `UseMqGuards`, `UseMqPipes`, `UseMqInterceptors` and `UseMqFilters` on a Worker class or a Process/FanOut/Collect method. Register each referenced class as a provider. These decorators do not enable Nest's HTTP pipeline and do not construct an HTTP request or expose a native runtime.

```ts
import { Injectable } from '@nestjs/common'
import {
  UseMqGuards,
  UseMqInterceptors,
  type MqGuard,
  type MqExecutionContext,
  type MqInterceptor,
  type MqNext
} from 'better-nest-mq'

@Injectable()
class NonnegativeInput implements MqGuard<number> {
  canActivate(context: MqExecutionContext<number>) {
    return context.payload >= 0
  }
}

@Injectable()
class ResultBounds implements MqInterceptor<number> {
  async intercept(_context: MqExecutionContext, next: MqNext<number>) {
    const result = await next()
    return Math.min(result, 1_000)
  }
}

// Apply these to ReportsWorker or its calculate method and register both providers:
UseMqGuards(NonnegativeInput)(ReportsWorker)
UseMqInterceptors(ResultBounds)(ReportsWorker)
```

Decorator syntax such as `@UseMqGuards(NonnegativeInput)` is equivalent. Examples are separated to show the independent retry and enhancer APIs; in an application, apply metadata before Nest bootstrap.

### Order, scope and validation

Class metadata composes base-first, followed by method metadata. Within a decorator, providers execute in argument order; repeated decorators retain source order. Method metadata follows the actual implementation: overriding a method does not inherit decorators from the replaced function.

Execution order is guards, pipes, outer-to-inner interceptors, then the actual handler. Interceptors unwind in reverse order. On an exception, method filters are tried before class filters, in declared order; the first whose `supports` returns true handles it. Filters may return a replacement result or throw a declared `JobFailureException` to enter normal retry processing. A filter exception propagates instead of being offered to later filters.

`MqExecutionContext` exposes `type: 'mq'`, phase, Worker class, method name, decoded payload, the existing facade `JobExecutionContext`, and a `children` reader only during Collect. It does not expose a lease token. The context is frozen; it is not an application authorization boundary.

Workers and their enhancers resolve with the **same fresh Nest ContextId per attempt or flow phase**. Request-scoped dependencies shared between them therefore share one subtree; later attempts/phases receive another. Concrete class providers are required, with callable prototype methods. Ambiguous registrations, accessors, factory/value enhancer registrations and missing providers fail preflight; no implicit enhancer is fabricated.

Pipes receive and return decoded values. Every transformed payload goes through the existing inverse-codec/validation boundary before business code sees it, including Date codecs. The native result/failure encoders still validate final outcomes. A filter or interceptor cannot turn a schema-invalid output into a successful persisted job, bypass flow-manifest validation or bypass lease ownership checks.

A guard returning false raises `MqGuardRejectedException`; it is a failed invocation, **not** queue pausing or rescheduling. Defects remain non-retryable by default, while explicitly enabled `retryDefects` retains its existing meaning. Guards and filter predicates must return booleans, not truthy values. HTTP `UseGuards`/`UsePipes`/`UseInterceptors`/`UseFilters` metadata remains rejected; global HTTP enhancers do not run here.

### Continuation and cancellation safety

An interceptor may call `next()` at most once. Repeated calls reject, including when middleware catches that rejection, and no second handler execution occurs. After the interceptor returns or throws, an escaped continuation is closed. An already-started downstream invocation drains before the attempt/phase can settle or release its resources, even if the interceptor returned without awaiting it.

The existing attempt abort signal is checked between stages and before business invocation. Cancellation during a guard or provider resolution cannot be recovered through filters into a new handler execution. This remains cooperative cancellation: arbitrary external operations and synchronous JavaScript cannot be forcibly interrupted or undone.

## Verification scope

Source tests cover retry DI, attempt budgets, decoded failures, malformed decisions, version fencing, middleware order, shared scoped dependencies, inherited/overridden metadata, Date pipes, filters, guard rejection, cancellation, single-use/escaped continuations and draining. Installed-package PostgreSQL qualification additionally exercises producer-only publication, schedules/outbox references, retry persistence across recreated worker contexts, and custom child retries through real FanOut/Collect phases under Node/Bun and TypeScript 6/7.

Durable event subscriptions/waits, ORM transaction bridges, additional storage adapters and full operational/release qualification remain separate roadmap work. Process-local middleware is not a durable event log or an exactly-once external-effect guarantee.

## Event-assisted wait integration

Event-assisted awaitResult/execute is implemented; see docs/event-waits.md (event-waits.md from this directory) for the API and exact scope. Configure postgres({ events: true }) on the waiting application and select strategy: 'events' with pollFallbackMs. Polling remains the default. The raw event token shares the job namespace; only an in-runtime operation alias is added. No second pool/runtime, automatic migrations or required-writer activation is introduced.

Earlier references to polling-only result waits are superseded by this integration. Runtime reader errors/lost hints use the existing bounded fallback; missing explicit reader configuration still fails. Timeout and abort do not cancel durable work. Keep tests for shutdown, parser fidelity and installed consumers. Resumable subscriptions, checkpoint APIs and retention administration remain pending; this is not a promise of zero polling.
