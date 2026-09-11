import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import {
  Job, JobCancelledException, JobContext, JobData, JobFailureException,
  JobWaitTimeoutException, MqModule, MqWorkersService, Process, Queue, QueueService,
  Retry, Worker, type JobExecutionContext, type PayloadOf, type ResultOf
} from 'better-nest-mq'
import { migratePostgres, postgres } from 'better-nest-mq/postgres'
import { zodCodec } from 'better-nest-mq/zod'

const timestamp = z.codec(z.iso.datetime(), z.date(), {
  decode: (text) => new Date(text), encode: (date) => date.toISOString()
})

@Injectable()
@Queue({ name: 'packed-execution', connection: 'primary' })
class PackedQueue extends QueueService {
  @Job({ name: 'transform', version: 1 })
  readonly transform = this.job({
    payload: zodCodec(z.object({ id: z.string(), timestamp })),
    result: zodCodec(z.object({ label: z.string(), timestamp })),
    idempotencyKey: (payload) => payload.id
  })

  @Job({ name: 'retry', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'exponential', initialDelayMs: 10, factor: 2 } })
  readonly retrying = this.job({
    payload: z.object({ retryable: z.boolean() }), result: z.int(),
    failure: z.object({ code: z.literal('unavailable'), retryable: z.boolean() }),
    retryable: (failure) => failure.retryable
  })
}

@Injectable()
class Labels { readonly prefix = 'from-real-nest-di' }

@Injectable()
@Worker({ name: 'packed-worker', concurrency: 2, pollIntervalMs: 10, leaseDurationMs: 2_000, heartbeatIntervalMs: 250 })
class PackedWorker {
  // Metadata emitted by each consumer TypeScript compiler must resolve ordinary Nest DI.
  constructor(private readonly labels: Labels) {}

  @Process(PackedQueue, 'transform')
  transform(@JobData() payload: PayloadOf<PackedQueue['transform']>): ResultOf<PackedQueue['transform']> {
    assert.ok(payload.timestamp instanceof Date)
    return { label: `${this.labels.prefix}:${payload.id}`, timestamp: new Date(payload.timestamp.getTime() + 1_000) }
  }

  @Process(PackedQueue, 'retrying')
  retrying(@JobData() payload: PayloadOf<PackedQueue['retrying']>, @JobContext() context: JobExecutionContext) {
    if (context.attempt < 3) throw new JobFailureException({ code: 'unavailable', retryable: payload.retryable })
    return context.attempt
  }
}

export async function verifyExecution(connectionString: string): Promise<void> {
  const schema = `mq_execution_${randomUUID().replaceAll('-', '')}`
  const admin = new Pool({ connectionString, max: 3 })
  const options = { connectionString, schema, namespace: 'packed-workers' }
  const wait = { timeoutMs: 5_000, pollIntervalMs: 10 }
  try {
    await migratePostgres({ pool: admin, schema })

    @Module({ imports: [
      MqModule.forRoot({ connections: { primary: postgres(options) }, execution: { workers: false } }),
      MqModule.forFeature([PackedQueue])
    ] })
    class ProducerModule {}

    const producer = await NestFactory.createApplicationContext(ProducerModule, { logger: false, abortOnError: false })
    let savedId: string
    try {
      const queue = producer.get(PackedQueue)
      const input = { id: 'survives-producer-exit', timestamp: '2026-09-10T12:00:00.000Z' }
      const ids = await Promise.all(Array.from({ length: 5 }, () => queue.transform.enqueue(input)))
      assert.equal(new Set(ids).size, 1)
      const first = ids[0]
      assert.ok(first)
      savedId = first
      assert.equal((await queue.transform.poll(first))?.state, 'waiting')
      assert.deepEqual(producer.get(MqWorkersService).workers(), [])
      await assert.rejects(queue.transform.awaitResult(first, { timeoutMs: 25, pollIntervalMs: 5 }), JobWaitTimeoutException)
      assert.equal((await queue.transform.poll(first))?.state, 'waiting')
      const prepared = await queue.transform.prepare({ ...input, id: 'prepared' }, { jobId: 'not-published' })
      assert.equal(prepared.connection, 'primary')
      assert.equal(await queue.transform.poll('not-published'), undefined)
      const delayed = await queue.transform.enqueue({ ...input, id: 'cancel-before-work' }, { delayMs: 60_000 })
      await queue.transform.cancel(delayed)
      await assert.rejects(queue.transform.awaitResult(delayed, wait), JobCancelledException)
    } finally { await producer.close() }

    @Module({ imports: [
      MqModule.forRoot({ connections: { primary: postgres(options) } }),
      MqModule.forFeature([PackedQueue])
    ], providers: [PackedWorker, Labels] })
    class ConsumerModule {}

    const consumer = await NestFactory.createApplicationContext(ConsumerModule, { logger: false, abortOnError: false })
    try {
      const queue = consumer.get(PackedQueue)
      assert.deepEqual(await queue.transform.awaitResult(savedId, wait), {
        label: 'from-real-nest-di:survives-producer-exit', timestamp: new Date('2026-09-10T12:00:01.000Z')
      })
      assert.equal((await queue.transform.attempts(savedId))[0]?.outcome, 'completed')
      const retryId = await queue.retrying.enqueue({ retryable: true })
      assert.equal(await queue.retrying.awaitResult(retryId, wait), 3)
      assert.deepEqual((await queue.retrying.attempts(retryId)).map((attempt) => attempt.outcome), ['retried', 'retried', 'completed'])
      const failed = await queue.retrying.enqueue({ retryable: false })
      await assert.rejects(queue.retrying.awaitResult(failed, wait), (error) => {
        assert.ok(error instanceof JobFailureException)
        assert.deepEqual(error.failure, { code: 'unavailable', retryable: false })
        return true
      })
      assert.equal((await queue.retrying.poll(failed))?.attemptsMade, 1)
      assert.deepEqual(await queue.transform.execute({ id: 'execute', timestamp: '2026-09-10T12:00:00.000Z' }, { wait }), {
        label: 'from-real-nest-di:execute', timestamp: new Date('2026-09-10T12:00:01.000Z')
      })
      await consumer.get(MqWorkersService).awaitIdle({ timeoutMs: 1_000 })
      assert.equal(consumer.get(MqWorkersService).workers()[0]?.activeCount, 0)
    } finally { await consumer.close() }

    const reader = await NestFactory.createApplicationContext(ProducerModule, { logger: false, abortOnError: false })
    try {
      assert.equal((await reader.get(PackedQueue).transform.poll(savedId))?.state, 'completed')
      assert.equal((await reader.get(PackedQueue).transform.attempts(savedId)).length, 1)
    } finally { await reader.close() }
    console.log('PASS packed real PostgreSQL execution: producer restart, workers, DI, codecs, idempotency, retries, terminal failures and durable results')
  } finally {
    try { await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`) }
    finally { await admin.end() }
  }
}

const connectionString = process.env.MQ_TEST_DATABASE_URL
if (connectionString !== undefined) await verifyExecution(connectionString)
else console.log('Packed worker declarations and types passed; live execution runs in the PostgreSQL CI job')
