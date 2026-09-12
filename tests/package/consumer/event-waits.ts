import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import {
  Job,
  JobCancelledException,
  JobData,
  JobFailureException,
  JobWaitAbortedException,
  JobWaitTimeoutException,
  MqJobException,
  MqModule,
  Process,
  Queue,
  QueueService,
  Worker,
  type JobWaitOptions,
  type PayloadOf
} from 'better-nest-mq'
import { migratePostgres, postgres } from 'better-nest-mq/postgres'
import { zodCodec } from 'better-nest-mq/zod'

const Json = z.union([
  z.string(),
  z.number(),
  z.boolean(),
  z.null(),
  z.array(z.string()),
  z.object({ value: z.string() })
])
const DateValue = zodCodec(
  z.codec(z.iso.datetime(), z.date(), {
    decode: (input) => new Date(input),
    encode: (date) => date.toISOString()
  })
)
@Injectable()
@Queue({ name: 'packed-event-waits', connection: 'primary' })
class EventQueue extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: Json, result: Json })
  @Job({ name: 'failure', version: 1 })
  readonly failure = this.job({
    payload: z.string(),
    result: z.string(),
    failure: z.object({ code: z.literal('expected') })
  })
  @Job({ name: 'date', version: 1 })
  readonly date = this.job({ payload: DateValue, result: DateValue })
  @Job({ name: 'another', version: 1 })
  readonly another = this.job({ payload: z.string(), result: z.string() })
}
@Injectable()
@Worker({ name: 'packed-events', concurrency: 4, pollIntervalMs: 10 })
class EventWorker {
  @Process(EventQueue, 'echo')
  echo(@JobData() payload: PayloadOf<EventQueue['echo']>) {
    return payload
  }
  @Process(EventQueue, 'failure')
  fail(@JobData() payload: string) {
    assert.equal(payload, 'fail')
    throw new JobFailureException({ code: 'expected' })
  }
  @Process(EventQueue, 'date')
  date(@JobData() payload: PayloadOf<EventQueue['date']>) {
    assert.ok(payload instanceof Date)
    return payload
  }
}

async function verifyEventWaits(connectionString: string): Promise<void> {
  const schema = `mq_events_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 10 })
  // If this reader silently falls back instead of using the correct log, the
  // pending result tests expire before their intentionally much longer fallback.
  const wait: JobWaitOptions = { strategy: 'events', pollFallbackMs: 60_000, timeoutMs: 5_000 }
  const inputs = [
    '123',
    'true',
    '{"json":"string"}',
    '',
    'ação',
    123,
    false,
    null,
    ['array'],
    { value: 'object' }
  ]
  const connection = postgres({
    pool,
    schema,
    namespace: 'events',
    events: true,
    outbox: true,
    schedules: true,
    flows: true
  })
  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: connection },
        execution: { workers: false, scheduler: false, outboxPublisher: false }
      }),
      MqModule.forFeature([EventQueue])
    ]
  })
  class ProducerModule {}
  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: postgres({ connectionString, schema, namespace: 'events' }) }
      }),
      MqModule.forFeature([EventQueue])
    ],
    providers: [EventWorker]
  })
  class WorkerModule {}
  try {
    await migratePostgres({ pool, schema })
    const producer = await NestFactory.createApplicationContext(ProducerModule, {
      logger: false,
      abortOnError: false
    })
    const ids: string[] = []
    try {
      const queue = producer.get(EventQueue)
      for (const input of inputs) ids.push(await queue.echo.enqueue(input))
      const cancellation = await queue.echo.enqueue('cancel before workers start')
      const cancelled = assert.rejects(
        queue.echo.awaitResult(cancellation, wait),
        JobCancelledException
      )
      await queue.echo.cancel(cancellation)
      await cancelled
      await assert.rejects(
        queue.echo.awaitResult(ids[0]!, {
          strategy: 'events',
          timeoutMs: 30,
          pollFallbackMs: 1000
        }),
        JobWaitTimeoutException
      )
      assert.equal((await queue.echo.poll(ids[0]!))?.state, 'waiting')
      const aborted = new AbortController()
      aborted.abort('caller left')
      await assert.rejects(
        queue.echo.awaitResult(ids[0]!, { ...wait, signal: aborted.signal }),
        JobWaitAbortedException
      )
      const pending = Promise.all(ids.map((id) => queue.echo.awaitResult(id, wait)))
      const worker = await NestFactory.createApplicationContext(WorkerModule, {
        logger: false,
        abortOnError: false
      })
      try {
        assert.deepEqual(await pending, inputs)
        await assert.rejects(worker.get(EventQueue).echo.awaitResult(ids[0]!, wait), MqJobException)
        assert.equal(await worker.get(EventQueue).echo.awaitResult(ids[0]!), '123')
        const failureId = await queue.failure.enqueue('fail')
        await assert.rejects(queue.failure.awaitResult(failureId, wait), (cause) => {
          assert.ok(cause instanceof JobFailureException)
          assert.deepEqual(cause.failure, { code: 'expected' })
          return true
        })
        const date = '2026-09-12T10:00:00.000Z'
        assert.deepEqual(await queue.date.execute(date, { wait }), new Date(date))
        await assert.rejects(queue.another.awaitResult(ids[0]!, wait), MqJobException)
        // Match event and job namespaces from actual persisted rows, never an
        // assumption that a second hashed operation token points at the same log.
        const rows = await pool.query<{ total: number }>(
          `
          SELECT count(*)::integer AS total
          FROM "${schema}".better_effect_mq_job_events AS e
          JOIN "${schema}".better_effect_mq_jobs AS j ON e.namespace=j.namespace AND e.job_id=j.id
          WHERE j.id=ANY($1::text[]) AND e.event_type='job-completed'
        `,
          [ids]
        )
        assert.equal(rows.rows[0]?.total, inputs.length)
        const activation = await pool.query<{ activation_state: string }>(
          `SELECT activation_state FROM "${schema}".better_effect_mq_job_event_activation`
        )
        assert.ok(activation.rows.length > 0)
        assert.ok(
          activation.rows.every((row) => row.activation_state === 'optional'),
          'Reader opt-in must never promote event persistence to required'
        )
        console.log(
          'PASS event-log-assisted results across separate producer/worker contexts, JSON/null/Date, terminal failures, cancellation, timeout and stable namespaces'
        )
      } finally {
        await worker.close()
      }
    } finally {
      await producer.close()
    }
    const restarted = await NestFactory.createApplicationContext(ProducerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      for (const [index, id] of ids.entries())
        assert.deepEqual(await restarted.get(EventQueue).echo.awaitResult(id, wait), inputs[index])
    } finally {
      await restarted.close()
    }
    assert.equal((await pool.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)
    console.log(
      'PASS completed results remain readable after restarting the reader and borrowed pool stays usable'
    )
  } finally {
    try {
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
    } finally {
      await pool.end()
    }
  }
}

const database = process.env.MQ_TEST_DATABASE_URL
if (database !== undefined) await verifyEventWaits(database)
else
  console.log(
    'Packed event-wait types passed; real PostgreSQL result waits run in the database CI job'
  )
