import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool, types as pgTypes, type CustomTypesConfig } from 'pg'
import { z } from 'zod'
import {
  Job,
  JobData,
  MqModule,
  MqOutboxService,
  MqOutboxException,
  Process,
  Queue,
  QueueService,
  Worker,
  type JobJsonValue,
  type OutboxEntry
} from 'better-nest-mq'
import {
  migratePostgres,
  postgres,
  postgresOutbox,
  type PostgresOutboxTransaction
} from 'better-nest-mq/postgres'

class EchoJobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
}
@Injectable()
@Queue({ name: 'outbox-echo', connection: 'primary' })
class PrimaryQueue extends EchoJobs {}
@Injectable()
@Queue({ name: 'outbox-echo', connection: 'target' })
class TargetQueue extends EchoJobs {}

@Injectable()
@Worker({ name: 'outbox-consumer', concurrency: 4, pollIntervalMs: 10 })
class EchoWorker {
  @Process(PrimaryQueue, 'echo')
  primary(@JobData() value: z.output<ReturnType<typeof z.json>>) {
    return value
  }
  @Process(TargetQueue, 'echo')
  target(@JobData() value: z.output<ReturnType<typeof z.json>>) {
    return value
  }
}

function barrier() {
  let release: (() => void) | undefined
  const promise = new Promise<void>((resolve) => {
    release = resolve
  })
  return {
    promise,
    open: () => {
      assert.ok(release)
      release()
    }
  }
}
async function until(check: () => Promise<boolean>, label: string): Promise<void> {
  const deadline = Date.now() + 10_000
  while (!(await check())) {
    assert.ok(Date.now() < deadline, `Timed out: ${label}`)
    await sleep(10)
  }
}

export async function verifyOutbox(connectionString: string): Promise<void> {
  const schema = `mq_outbox_${randomUUID().replaceAll('-', '')}`
  const admin = new Pool({ connectionString, max: 5 })
  const getTypeParser: CustomTypesConfig['getTypeParser'] = (oid, format = 'text') => {
    if (format === 'binary') return pgTypes.getTypeParser(oid, 'binary')
    if (oid === 114 || oid === 3802) return (text: string) => ({ native: JSON.parse(text) })
    return pgTypes.getTypeParser(oid, 'text')
  }
  const borrowed = new Pool({ connectionString, max: 8, types: { getTypeParser } })
  const routes = {
    primary: postgres({ pool: borrowed, schema, namespace: 'source', outbox: true }),
    target: postgres({ connectionString, schema, namespace: 'destination' })
  }
  const publisherOptions = {
    concurrency: 2,
    pollIntervalMs: 10,
    leaseDurationMs: 500,
    heartbeatIntervalMs: 50,
    retryBaseDelayMs: 10,
    retryMaxDelayMs: 50
  }
  const delivered: Array<{
    outboxId: string
    jobId: string
    target: 'primary' | 'target'
    payload: JobJsonValue
  }> = []
  try {
    await migratePostgres({ pool: admin, schema })
    await admin.query(`CREATE TABLE "${schema}".business(id text PRIMARY KEY, value text NOT NULL)`)

    @Module({
      imports: [
        MqModule.forRoot({
          connections: routes,
          execution: { workers: false, outboxPublisher: false },
          outbox: publisherOptions
        }),
        MqModule.forFeature([PrimaryQueue, TargetQueue])
      ]
    })
    class ProducerModule {}
    const producer = await NestFactory.createApplicationContext(ProducerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      const service = producer.get(MqOutboxService)
      const outbox = postgresOutbox(service, 'primary')
      const primary = producer.get(PrimaryQueue)
      const target = producer.get(TargetQueue)
      assert.equal(service.publisher(), undefined)
      assert.deepEqual(await service.counts('primary'), {
        pending: 0,
        active: 0,
        published: 0,
        failed: 0,
        total: 0
      })
      await assert.rejects(
        postgresOutbox(service, 'target').transaction(async () => 1),
        MqOutboxException
      )

      const prepared = await target.echo.prepare({ committed: true }, { jobId: 'commit-job' })
      const entry: OutboxEntry = { id: 'commit-record', job: prepared, attempts: 4 }
      const appended = barrier()
      const complete = barrier()
      let escaped: PostgresOutboxTransaction | undefined
      const transaction = outbox.transaction(async (tx) => {
        escaped = tx
        assert.deepEqual(Object.keys(tx).sort(), ['append', 'query'])
        await tx.query(`INSERT INTO "${schema}".business VALUES($1,$2)`, ['commit', 'value'])
        const native = await tx.query<{ json: { native: { value: number } } }>(
          'SELECT $1::jsonb AS json',
          ['{"value":7}']
        )
        assert.deepEqual(native.rows[0]?.json, { native: { value: 7 } })
        const result = await tx.append(entry)
        assert.equal(result.duplicate, false)
        assert.equal(result.record.request.id, 'commit-job')
        appended.open()
        await complete.promise
        return 'domain-result'
      })
      // Attach a rejection observer immediately so a failing assertion cannot leak a task.
      const observed = transaction.then(
        (value) => ({ value }),
        (error) => ({ error })
      )
      try {
        await Promise.race([appended.promise, transaction])
        assert.equal(
          (await admin.query(`SELECT 1 FROM "${schema}".business WHERE id='commit'`)).rowCount,
          0
        )
        assert.equal(await service.get('primary', 'commit-record'), undefined)
        assert.equal(await target.echo.poll('commit-job'), undefined)
      } finally {
        complete.open()
      }
      const outcome = await observed
      if ('error' in outcome) throw outcome.error
      assert.equal(outcome.value, 'domain-result')
      assert.ok(escaped)
      await assert.rejects(escaped.query('SELECT 1'), MqOutboxException)
      await assert.rejects(escaped.append(entry), MqOutboxException)
      assert.equal((await service.get('primary', 'commit-record'))?.state, 'pending')
      assert.equal(await target.echo.poll('commit-job'), undefined)
      delivered.push({
        outboxId: entry.id,
        jobId: 'commit-job',
        target: 'target',
        payload: { committed: true }
      })
      console.log(
        'PASS one-client transaction, native parsers, uncommitted invisibility and closed handle rejection'
      )

      const duplicate = await outbox.transaction(async (tx) => tx.append(entry))
      assert.equal(duplicate.duplicate, true)
      const changedDestination: OutboxEntry = {
        ...entry,
        job: { ...entry.job, connection: 'primary' }
      }
      await assert.rejects(
        outbox.transaction(changedDestination, async (tx) => {
          await tx.query(`INSERT INTO "${schema}".business VALUES($1,$2)`, [
            'conflict',
            'must roll back'
          ])
        }),
        MqOutboxException
      )
      await assert.rejects(
        outbox.transaction(
          {
            ...entry,
            job: { ...entry.job, request: { ...entry.job.request, dispatchKey: 'changed' } }
          },
          async () => 1
        ),
        MqOutboxException
      )
      assert.equal(
        (await admin.query(`SELECT 1 FROM "${schema}".business WHERE id='conflict'`)).rowCount,
        0
      )

      const cause = new Error('business rejected')
      const rolledBack: OutboxEntry = {
        id: 'rollback-record',
        job: await target.echo.prepare('rollback', { jobId: 'rollback-job' })
      }
      await assert.rejects(
        outbox.transaction(async (tx) => {
          await tx.query(`INSERT INTO "${schema}".business VALUES($1,$2)`, ['rollback', 'no'])
          await tx.append(rolledBack)
          throw cause
        }),
        (error) => error === cause
      )
      assert.equal(
        (await admin.query(`SELECT 1 FROM "${schema}".business WHERE id='rollback'`)).rowCount,
        0
      )
      assert.equal(await service.get('primary', rolledBack.id), undefined)
      const swallowed: OutboxEntry = {
        id: 'swallowed-record',
        job: await target.echo.prepare('swallowed', { jobId: 'swallowed-job' })
      }
      await assert.rejects(
        outbox.transaction(swallowed, async (tx) => {
          await tx.query(`INSERT INTO "${schema}".business VALUES($1,$2)`, ['swallowed', 'no'])
          await assert.rejects(tx.query('SELECT 1/0'))
          return 'caller caught SQL error'
        })
      )
      assert.equal(await service.get('primary', swallowed.id), undefined)
      assert.equal(
        (await admin.query(`SELECT 1 FROM "${schema}".business WHERE id='swallowed'`)).rowCount,
        0
      )
      console.log(
        'PASS domain rollback, caught-operation poisoning and full route/key duplicate conflicts'
      )

      const values: Array<z.input<ReturnType<typeof z.json>>> = [
        '123',
        'null',
        '',
        'true',
        '{"nested":1}',
        null,
        false,
        0,
        [1, 'two'],
        { nested: [null, true] }
      ]
      const entries: OutboxEntry[] = []
      for (const [index, payload] of values.entries()) {
        const destination = index % 2 === 0 ? primary : target
        const jobId = `scalar-${index}`
        const record: OutboxEntry = {
          id: `scalar-record-${index}`,
          job: await destination.echo.prepare(payload, { jobId }),
          attempts: 3
        }
        entries.push(record)
        delivered.push({
          outboxId: record.id,
          jobId,
          target: index % 2 === 0 ? 'primary' : 'target',
          payload
        })
      }
      await outbox.transaction(entries, async (tx) => {
        await tx.query(`INSERT INTO "${schema}".business VALUES($1,$2)`, ['batch', 'committed'])
      })
      const dynamic = await outbox.transaction(async (tx) => {
        const row = await tx.query<{ id: string }>(
          `INSERT INTO "${schema}".business VALUES($1,$2) RETURNING id`,
          ['generated-business-id', 'committed']
        )
        const id = row.rows[0]?.id
        assert.ok(id)
        const result = await tx.append({
          id: 'generated-record',
          job: await target.echo.prepare({ id })
        })
        assert.ok(result.record.request.id)
        return result.record.request.id
      })
      delivered.push({
        outboxId: 'generated-record',
        jobId: dynamic,
        target: 'target',
        payload: { id: 'generated-business-id' }
      })

      // Reproduce the durable state after enqueue succeeded but the publisher never acknowledged.
      const replay: OutboxEntry = {
        id: 'replay-record',
        job: await target.echo.prepare({ replay: true }, { jobId: 'replay-job' }),
        attempts: 4
      }
      await outbox.transaction(replay, async () => undefined)
      await target.echo.enqueue(
        { replay: true },
        { jobId: 'replay-job', at: replay.job.request.runAt }
      )
      const now = Date.now()
      await admin.query(
        `UPDATE "${schema}".better_effect_mq_outbox SET state='active',attempts_made=1,lease_owner=$2,lease_token=$3,lease_expires_at_ms=$4,updated_at_ms=$5 WHERE id=$1`,
        [replay.id, 'abandoned-publisher', randomUUID(), now + 20, now]
      )
      await sleep(30)
      delivered.push({
        outboxId: replay.id,
        jobId: 'replay-job',
        target: 'target',
        payload: { replay: true }
      })
      const before = await service.list('primary', { limit: 100 })
      assert.equal(before.length, delivered.length)
      assert.ok(before.every((record) => !('leaseToken' in record)))
      assert.deepEqual(
        (
          await borrowed.query<{ json: { native: { value: number } } }>(
            'SELECT $1::jsonb AS json',
            ['{"value":9}']
          )
        ).rows[0]?.json,
        { native: { value: 9 } }
      )
    } finally {
      await producer.close()
    }
    assert.equal((await borrowed.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)

    const ownRoutes = {
      primary: postgres({ connectionString, schema, namespace: 'source', outbox: true }),
      target: postgres({ connectionString, schema, namespace: 'destination' })
    }
    @Module({
      imports: [
        MqModule.forRoot({
          connections: ownRoutes,
          execution: { workers: false },
          outbox: publisherOptions
        }),
        MqModule.forFeature([PrimaryQueue, TargetQueue])
      ]
    })
    class PublisherModule {}
    const publisher = await NestFactory.createApplicationContext(PublisherModule, {
      logger: false,
      abortOnError: false
    })
    const competitor = await NestFactory.createApplicationContext(PublisherModule, {
      logger: false,
      abortOnError: false
    })
    try {
      assert.equal(publisher.get(MqOutboxService).publisher()?.state, 'running')
      await until(
        async () =>
          (await publisher.get(MqOutboxService).counts('primary')).published === delivered.length,
        'committed outbox records publish'
      )
      const replay = await publisher.get(MqOutboxService).get('primary', 'replay-record')
      assert.ok(replay)
      assert.equal(replay.attemptsMade, 2)
      assert.equal(replay.request.id, 'replay-job')
      for (const record of delivered) {
        const queue =
          record.target === 'primary' ? publisher.get(PrimaryQueue) : publisher.get(TargetQueue)
        assert.equal((await queue.echo.poll(record.jobId))?.state, 'waiting')
      }
      assert.equal(
        (
          await admin.query(
            `SELECT id FROM "${schema}".better_effect_mq_jobs WHERE id='replay-job'`
          )
        ).rowCount,
        1
      )
      console.log(
        'PASS competing publishers, durable restart, source/target routes and replay after enqueue-before-ack'
      )
    } finally {
      await competitor.close()
      await publisher.close()
    }

    @Module({
      imports: [
        MqModule.forRoot({ connections: ownRoutes, execution: { outboxPublisher: false } }),
        MqModule.forFeature([PrimaryQueue, TargetQueue])
      ],
      providers: [EchoWorker]
    })
    class ConsumerModule {}
    const consumer = await NestFactory.createApplicationContext(ConsumerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      for (const record of delivered) {
        const queue =
          record.target === 'primary' ? consumer.get(PrimaryQueue) : consumer.get(TargetQueue)
        assert.deepEqual(
          await queue.echo.awaitResult(record.jobId, { timeoutMs: 10_000, pollIntervalMs: 10 }),
          record.payload
        )
        assert.equal((await queue.echo.attempts(record.jobId)).length, 1)
      }
      assert.equal(consumer.get(MqOutboxService).publisher(), undefined)
      assert.equal(
        (await consumer.get(MqOutboxService).counts('primary')).published,
        delivered.length
      )
      console.log(
        'PASS installed-package scalar JSON/null results, stable job ids and independent publisher/worker roles'
      )
    } finally {
      await consumer.close()
    }
  } finally {
    await borrowed.end()
    try {
      await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
    } finally {
      await admin.end()
    }
  }
}

const connectionString = process.env.MQ_TEST_DATABASE_URL
if (connectionString !== undefined) await verifyOutbox(connectionString)
else
  console.log(
    'Packed outbox API/types passed; transaction and publication cases run in PostgreSQL CI'
  )
