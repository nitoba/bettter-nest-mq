import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable, Scope } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  ContractDefinitionException,
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
} from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'
import { assertRetryReference } from '../../src/engine/retry-reference.ts'
import { getQueueDefinition } from '../../src/contracts/queue-definition.ts'

const retry = { attempts: 4, backoff: { type: 'custom' as const, policy: 'remote', version: 2 } }
const wait = { timeoutMs: 3_000, pollIntervalMs: 5 }

@Queue({ name: 'retry-policy', connection: 'primary' })
class Tasks extends QueueService {
  @Job({ name: 'task', version: 1 })
  @Retry(retry)
  readonly task = this.job({
    payload: z.object({ succeedAt: z.number(), allowed: z.boolean().default(true) }),
    result: z.number(),
    failure: z.object({ code: z.literal('busy'), allowed: z.boolean() }),
    retryable: (failure) => failure.allowed
  })
}

@Injectable()
class Config {
  readonly delay = 7
  readonly decisions: RetryPolicyContext[] = []
}

@Injectable()
@RetryPolicy({ name: 'remote', version: 2 })
class RemoteRetry implements MqRetryPolicy<{ code: 'busy'; allowed: boolean }> {
  constructor(@Inject(Config) private readonly config: Config) {}
  decide(failure: { code: 'busy'; allowed: boolean }, context: RetryPolicyContext) {
    assert.equal(failure.code, 'busy')
    assert.equal(Object.isFrozen(context), true)
    this.config.decisions.push(context)
    return { retry: true, delayMs: this.config.delay * context.attempt }
  }
}

@Worker({ name: 'retry-policy-worker', pollIntervalMs: 5 })
class Consumer {
  @Process(Tasks, 'task')
  run(
    @JobData() payload: { succeedAt: number; allowed: boolean },
    @JobContext() context: JobExecutionContext
  ) {
    if (context.attempt < payload.succeedAt)
      throw new JobFailureException({ code: 'busy', allowed: payload.allowed })
    return context.attempt
  }
}

async function application() {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: fixture.connection } }),
      MqModule.forFeature([Tasks])
    ],
    providers: [Consumer, RemoteRetry, Config]
  }).compile()
  await app.init()
  return app
}

test('native custom retries resolve static DI, persist decisions and respect predicates and per-message budgets', async () => {
  const app = await application()
  try {
    const tasks = app.get(Tasks)
    const id = await tasks.task.enqueue({ succeedAt: 3, allowed: true })
    expect(await tasks.task.awaitResult(id, wait)).toBe(3)
    const attempts = await tasks.task.attempts(id)
    expect(attempts.map((item) => item.outcome)).toEqual(['retried', 'retried', 'completed'])
    expect(attempts.map((item) => item.retryDelayMs)).toEqual([7, 14, undefined])
    expect((await tasks.task.poll(id))?.metadata).toEqual({
      __better_nest_mq_retry: '["remote",2]'
    })
    expect(app.get(Config).decisions).toEqual([
      { name: 'remote', version: 2, attempt: 1, attemptsMax: 4 },
      { name: 'remote', version: 2, attempt: 2, attemptsMax: 4 }
    ])
    const terminal = await tasks.task.enqueue({ succeedAt: 3, allowed: false })
    await assert.rejects(tasks.task.awaitResult(terminal, wait), JobFailureException)
    expect((await tasks.task.attempts(terminal)).length).toBe(1)
    const limited = await tasks.task.enqueue(
      { succeedAt: 3, allowed: true },
      { retry: { ...retry, attempts: 2 } }
    )
    await assert.rejects(tasks.task.awaitResult(limited, wait), JobFailureException)
    expect((await tasks.task.attempts(limited)).length).toBe(2)
    expect(app.get(Config).decisions.at(-1)?.attemptsMax).toBe(2)
  } finally {
    await app.close()
  }
})

test('producer-only applications publish references without installing a retry provider', async () => {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: fixture.connection },
        execution: { workers: false }
      }),
      MqModule.forFeature([Tasks])
    ]
  }).compile()
  await app.init()
  try {
    const task = app.get(Tasks).task
    const prepared = await task.prepare(
      { succeedAt: 1, allowed: true },
      { metadata: { tenant: 'one' } }
    )
    expect(prepared.request.backoff).toBeUndefined()
    expect(prepared.request.metadata).toEqual({
      tenant: 'one',
      __better_nest_mq_retry: '["remote",2]'
    })
    expect(JSON.stringify(prepared)).not.toContain('decide')
    const ids = await task.enqueueMany([{ payload: { succeedAt: 1, allowed: true } }])
    expect(ids.length).toBe(1)
    await assert.rejects(
      task.enqueue(
        { succeedAt: 1, allowed: true },
        { metadata: { __better_nest_mq_retry: '["remote",1]' } }
      ),
      ContractDefinitionException
    )
    await assert.rejects(
      task.prepare(
        { succeedAt: 1, allowed: true },
        { retry: { ...retry, backoff: { ...retry.backoff, version: 1 } } }
      ),
      ContractDefinitionException
    )
    await assert.rejects(
      task.enqueue(
        { succeedAt: 1, allowed: true },
        { retry: { attempts: 2, backoff: { type: 'fixed', delayMs: 1 } } }
      ),
      ContractDefinitionException
    )
  } finally {
    await app.close()
  }
})

for (const kind of ['missing', 'scoped', 'duplicate'] as const) {
  test(`rejects ${kind} retry providers before acquiring storage`, async () => {
    @RetryPolicy({ name: 'remote', version: 2 })
    @Injectable({ scope: Scope.REQUEST })
    class ScopedPolicy {
      decide() {
        return true
      }
    }
    @RetryPolicy({ name: 'remote', version: 2 })
    class DuplicatePolicy {
      decide() {
        return true
      }
    }
    const fixture = memoryConnection()
    const policies =
      kind === 'missing'
        ? []
        : kind === 'scoped'
          ? [ScopedPolicy]
          : [RemoteRetry, DuplicatePolicy, Config]
    const app = await Test.createTestingModule({
      imports: [
        MqModule.forRoot({ connections: { primary: fixture.connection } }),
        MqModule.forFeature([Tasks])
      ],
      providers: [Consumer, ...policies]
    }).compile()
    await assert.rejects(app.init(), ContractDefinitionException)
    expect(fixture.trace.acquisitions).toBe(0)
    await assert.rejects(app.close(), ContractDefinitionException)
  })
}

test('persisted policy references fence a missing or changed job-contract version mapping', () => {
  const job = getQueueDefinition(new Tasks()).jobs[0]
  assert.ok(job)
  assert.throws(() => assertRetryReference(job, {}), ContractDefinitionException)
  assert.throws(
    () => assertRetryReference(job, { __better_nest_mq_retry: '["remote",1]' }),
    ContractDefinitionException
  )
  assert.doesNotThrow(() => assertRetryReference(job, { __better_nest_mq_retry: '["remote",2]' }))
})

for (const behavior of ['stop', 'throw', 'async', 'invalid'] as const) {
  test(`native retry decision ${behavior} fails closed without an extra attempt`, async () => {
    @RetryPolicy({ name: 'remote', version: 2 })
    class TerminalPolicy {
      decide() {
        if (behavior === 'throw') throw new Error('policy defect')
        if (behavior === 'async') return Promise.reject(new Error('async decision unsupported'))
        if (behavior === 'invalid') return { retry: true, delayMs: -1 }
        return false
      }
    }
    const fixture = memoryConnection()
    const app = await Test.createTestingModule({
      imports: [
        MqModule.forRoot({ connections: { primary: fixture.connection } }),
        MqModule.forFeature([Tasks])
      ],
      providers: [Consumer, TerminalPolicy]
    }).compile()
    await app.init()
    try {
      const task = app.get(Tasks).task
      const id = await task.enqueue({ succeedAt: 3, allowed: true })
      await assert.rejects(task.awaitResult(id, wait), JobFailureException)
      expect((await task.attempts(id)).map((entry) => entry.outcome)).toEqual(['failed'])
    } finally {
      await app.close()
    }
  })
}

test('retry providers remain application-owned rather than sharing static state', async () => {
  const first = await application()
  const second = await application()
  try {
    await first.get(Tasks).task.execute({ succeedAt: 2, allowed: true }, { wait })
    expect(first.get(Config).decisions.length).toBe(1)
    expect(second.get(Config).decisions.length).toBe(0)
  } finally {
    await Promise.all([first.close(), second.close()])
  }
})
