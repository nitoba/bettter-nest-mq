import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as delay } from 'node:timers/promises'
import { Inject, Injectable, Module, Scope } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import {
  Job,
  JobContext,
  JobData,
  JobFailureException,
  MqModule,
  MqOutboxService,
  MqOutboxException,
  MqSchedulesService,
  Process,
  Queue,
  QueueService,
  Retry,
  RetryPolicy,
  Worker,
  UseMqGuards,
  UseMqPipes,
  UseMqInterceptors,
  Flow,
  FanOut,
  Collect,
  FlowData,
  FlowChildren,
  flowJob,
  flowChildren,
  type MqExecutionContext,
  type MqNext,
  type MqRetryPolicy,
  type RetryPolicyContext,
  type JobExecutionContext,
  type FlowResultsReader
} from 'better-nest-mq'
import { postgres, migratePostgres, postgresOutbox } from 'better-nest-mq/postgres'

const Payload = z.object({ value: z.number(), succeedAt: z.int().min(1), allowed: z.boolean() })
const Output = z.object({ value: z.number(), attempt: z.int() })
type PayloadValue = z.output<typeof Payload>
@Queue({ name: 'extensions', connection: 'primary' })
class Tasks extends QueueService {
  @Job({ name: 'request', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'custom', policy: 'remote', version: 1 } })
  readonly request = this.job({
    payload: Payload,
    result: Output,
    failure: z.object({ busy: z.boolean() }),
    retryable: (failure) => failure.busy
  })
  @Job({ name: 'parent', version: 1 })
  readonly parent = this.job({ payload: Payload, result: Output })
}
const parent = flowJob(Tasks, 'parent')
const request = flowJob(Tasks, 'request')
@Injectable()
class Audit {
  readonly invocations: { id: string; attempt: number; scope: Attempt; phase: string }[] = []
  readonly decisions: RetryPolicyContext[] = []
}
@Injectable({ scope: Scope.REQUEST })
class Attempt {
  readonly stages: string[] = []
}
@Injectable()
class Gate {
  constructor(@Inject(Attempt) private readonly attempt: Attempt) {}
  canActivate(context: MqExecutionContext<PayloadValue>) {
    assert.equal(context.type, 'mq')
    this.attempt.stages.push('guard')
    return context.payload.allowed
  }
}
@Injectable()
class Increment {
  constructor(@Inject(Attempt) private readonly attempt: Attempt) {}
  transform(value: PayloadValue) {
    this.attempt.stages.push('pipe')
    return { ...value, value: value.value + 1 }
  }
}
@Injectable()
class Around {
  constructor(@Inject(Attempt) private readonly attempt: Attempt) {}
  async intercept(context: MqExecutionContext, next: MqNext) {
    this.attempt.stages.push('before')
    assert.equal(context.children !== undefined, context.phase === 'collect')
    try {
      return await next()
    } finally {
      this.attempt.stages.push('after')
    }
  }
}
@Injectable()
@RetryPolicy({ name: 'remote', version: 1 })
class RemoteRetry implements MqRetryPolicy<{ busy: boolean }> {
  constructor(@Inject(Audit) private readonly audit: Audit) {}
  decide(failure: { busy: boolean }, context: RetryPolicyContext) {
    assert.equal(failure.busy, true)
    this.audit.decisions.push(context)
    return { retry: true, delayMs: 1_500 }
  }
}
@Injectable()
@Worker({ name: 'extension-worker', concurrency: 1, pollIntervalMs: 10, flowSweepIntervalMs: 20 })
@Flow({
  name: 'extension-flow',
  parent,
  children: [request],
  onChildFailure: 'continue',
  maxChildren: 1
})
@UseMqGuards(Gate)
@UseMqPipes(Increment)
@UseMqInterceptors(Around)
class Consumer {
  constructor(
    @Inject(Attempt) private readonly attempt: Attempt,
    @Inject(Audit) private readonly audit: Audit
  ) {}
  private record(context: JobExecutionContext, phase: string): void {
    assert.deepEqual(this.attempt.stages, ['guard', 'pipe', 'before'])
    this.audit.invocations.push({
      id: context.jobId,
      attempt: context.attempt,
      scope: this.attempt,
      phase
    })
    this.attempt.stages.push('handler')
  }
  @Process(Tasks, 'request')
  run(@JobData() value: PayloadValue, @JobContext() context: JobExecutionContext) {
    this.record(context, 'process')
    assert.equal(context.metadata.__better_nest_mq_retry, '["remote",1]')
    if (context.attempt < value.succeedAt) throw new JobFailureException({ busy: true })
    return { value: value.value, attempt: context.attempt }
  }
  @FanOut()
  split(@FlowData() value: PayloadValue, @JobContext() context: JobExecutionContext) {
    this.record(context, 'fanOut')
    return [flowChildren(request, [{ key: 'one', payload: value }])]
  }
  @Collect()
  async collect(
    @FlowChildren() children: FlowResultsReader,
    @JobContext() context: JobExecutionContext
  ) {
    this.record(context, 'collect')
    const rows = await children.all(request, { maxItems: 1 })
    assert.ok(rows[0]?.outcome === 'completed')
    return rows[0].result
  }
}

async function until(predicate: () => Promise<boolean>): Promise<void> {
  const deadline = Date.now() + 8_000
  while (!(await predicate())) {
    assert.ok(Date.now() < deadline, 'Extension regression timed out')
    await delay(10)
  }
}

async function verify(connectionString: string): Promise<void> {
  const schema = `mq_extensions_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 12 })
  const connection = postgres({
    pool,
    schema,
    namespace: 'extensions',
    schedules: true,
    outbox: true,
    flows: true
  })
  const wait = { timeoutMs: 8_000, pollIntervalMs: 10 }
  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: connection },
        execution: { workers: false, scheduler: true, outboxPublisher: false }
      }),
      MqModule.forFeature([Tasks])
    ]
  })
  class Producers {}
  function workerModule(publisher: boolean) {
    @Module({
      imports: [
        MqModule.forRoot({
          connections: { primary: connection },
          execution: { scheduler: false, outboxPublisher: publisher }
        }),
        MqModule.forFeature([Tasks])
      ],
      providers: [Consumer, RemoteRetry, Audit, Attempt, Gate, Increment, Around]
    })
    class Consumers {}
    return Consumers
  }
  const options = { logger: false as const, abortOnError: false }
  try {
    await migratePostgres({ pool, schema })
    await pool.query(`CREATE TABLE "${schema}".business_requests (id text PRIMARY KEY)`)
    const producer = await NestFactory.createApplicationContext(Producers, options)
    let direct: string
    let denied: string
    let stale: string
    let scheduled: string
    const outboxId = randomUUID()
    try {
      const task = producer.get(Tasks).request
      direct = await task.enqueue({ value: 4, succeedAt: 2, allowed: true })
      denied = await task.enqueue({ value: 8, succeedAt: 1, allowed: false })
      stale = await task.enqueue({ value: 9, succeedAt: 1, allowed: true })
      await pool.query(
        `UPDATE "${schema}".better_effect_mq_jobs SET metadata=$1::jsonb WHERE id=$2`,
        [JSON.stringify({ __better_nest_mq_retry: '["remote",2]' }), stale]
      )
      const prepared = await task.prepare({ value: 10, succeedAt: 1, allowed: true })
      assert.equal(prepared.request.metadata.__better_nest_mq_retry, '["remote",1]')
      assert.equal(prepared.request.backoff, undefined)
      assert.equal(JSON.stringify(prepared).includes('decide'), false)
      const outbox = postgresOutbox(producer.get(MqOutboxService), 'primary')
      await outbox.transaction({ id: outboxId, job: prepared }, async (tx) => {
        await tx.query(`INSERT INTO "${schema}".business_requests (id) VALUES ($1)`, [outboxId])
      })
      let forgedCallback = false
      await assert.rejects(
        outbox.transaction(
          {
            id: 'forged',
            job: {
              ...prepared,
              request: { ...prepared.request, metadata: { __better_nest_mq_retry: '["remote",2]' } }
            }
          },
          async () => {
            forgedCallback = true
          }
        ),
        MqOutboxException
      )
      assert.equal(forgedCallback, false)
      const schedules = producer.get(MqSchedulesService)
      await schedules.upsert(Tasks, 'request', {
        key: 'once',
        everyMs: 60_000,
        payload: { value: 11, succeedAt: 1, allowed: true }
      })
      await pool.query(
        `UPDATE "${schema}".better_effect_mq_schedules SET next_run_at_ms=$1 WHERE schedule_key='once'`,
        [Date.now() - 1]
      )
      await schedules.sweep()
      const record = await schedules.get(Tasks, 'request', 'once')
      assert.ok(record?.lastJobId)
      scheduled = record.lastJobId
      await schedules.remove(Tasks, 'request', 'once')
    } finally {
      await producer.close()
    }
    console.log(
      'PASS producer-only custom retry references across enqueue, prepare, native outbox and schedules'
    )

    const first = await NestFactory.createApplicationContext(workerModule(false), options)
    const firstAudit = first.get(Audit)
    try {
      await until(async () =>
        (await first.get(Tasks).request.attempts(direct)).some(
          (attempt) => attempt.outcome === 'retried'
        )
      )
      assert.equal((await first.get(Tasks).request.poll(direct))?.state, 'delayed')
      assert.deepEqual(firstAudit.decisions, [
        { name: 'remote', version: 1, attempt: 1, attemptsMax: 3 }
      ])
    } finally {
      await first.close()
    }
    const second = await NestFactory.createApplicationContext(workerModule(true), options)
    try {
      const tasks = second.get(Tasks)
      assert.deepEqual(await tasks.request.awaitResult(direct, wait), { value: 5, attempt: 2 })
      assert.deepEqual(
        (await tasks.request.attempts(direct)).map((entry) => entry.retryDelayMs),
        [1_500, undefined]
      )
      assert.deepEqual(await tasks.request.awaitResult(scheduled, wait), { value: 12, attempt: 1 })
      await assert.rejects(tasks.request.awaitResult(denied, wait))
      await assert.rejects(tasks.request.awaitResult(stale, wait))
      await until(
        async () =>
          (await second.get(MqOutboxService).get('primary', outboxId))?.state === 'published'
      )
      const outbox = await second.get(MqOutboxService).get('primary', outboxId)
      assert.ok(outbox?.request.id)
      assert.deepEqual(await tasks.request.awaitResult(outbox.request.id, wait), {
        value: 11,
        attempt: 1
      })
      const rows = await pool.query<{ count: number }>(
        `SELECT count(*)::integer AS count FROM "${schema}".business_requests`
      )
      assert.equal(rows.rows[0]?.count, 1)
      console.log(
        'PASS persisted retry timing and restarted DI, guard rejection, rolling-version fencing and outbox publication'
      )

      assert.deepEqual(
        await tasks.parent.execute({ value: 20, succeedAt: 2, allowed: true }, { wait }),
        { value: 22, attempt: 2 }
      )
      const all = [...firstAudit.invocations, ...second.get(Audit).invocations]
      assert.equal(
        all.some((entry) => entry.id === denied || entry.id === stale),
        false
      )
      assert.equal(new Set(all.map((entry) => entry.scope)).size, all.length)
      assert.ok(all.some((entry) => entry.phase === 'fanOut'))
      assert.ok(all.some((entry) => entry.phase === 'collect'))
      for (const entry of all)
        assert.deepEqual(entry.scope.stages, ['guard', 'pipe', 'before', 'handler', 'after'])
      console.log(
        'PASS native PostgreSQL flows with custom child retries and one fresh shared MQ enhancer scope per phase/attempt'
      )
    } finally {
      await second.close()
    }
    assert.equal((await pool.query('SELECT 1 AS alive')).rows[0]?.alive, 1)
  } finally {
    try {
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
    } finally {
      await pool.end()
    }
  }
}

const connectionString = process.env.MQ_TEST_DATABASE_URL
if (connectionString === undefined)
  console.log('Execution extensions: live scenarios run in the separate PostgreSQL CI job')
else await verify(connectionString)
