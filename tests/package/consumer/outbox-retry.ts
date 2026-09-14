import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { Injectable, Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import {
  Job, JobData, MqModule, MqOutboxException, MqOutboxService, Process, Queue,
  QueueService, Worker, type OutboxRetryExpected, type OutboxSnapshot, type PayloadOf
} from 'better-nest-mq'
import { migratePostgres, postgres, postgresOutbox } from 'better-nest-mq/postgres'

const Value = z.union([z.string(), z.null(), z.object({ text: z.string() })])
@Injectable()
@Queue({ name: 'outbox-recovery', connection: 'target' })
class RecoveryQueue extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: Value, result: Value, dispatchKey: () => 'tenant-key' })
}
@Injectable()
@Worker({ name: 'recovery-worker', concurrency: 3, pollIntervalMs: 10 })
class RecoveryWorker {
  @Process(RecoveryQueue, 'echo')
  run(@JobData() value: PayloadOf<RecoveryQueue['echo']>) { return value }
}
function expected(record: OutboxSnapshot): OutboxRetryExpected {
  return { updatedAtMs: record.updatedAtMs, attemptsMade: record.attemptsMade, attemptsMax: record.attemptsMax }
}
async function until(check: () => Promise<boolean>, label: string): Promise<void> {
  const end = Date.now() + 10_000
  while (!(await check())) { assert.ok(Date.now() < end, `Timed out: ${label}`); await sleep(10) }
}
async function record(service: MqOutboxService, id: string): Promise<OutboxSnapshot> {
  const value = await service.get('source', id)
  assert.ok(value)
  return value
}

async function verifyRecovery(connectionString: string): Promise<void> {
  const schema = `mq_retry_${randomUUID().replaceAll('-', '')}`
  const admin = new Pool({ connectionString, max: 5 })
  const shared = new Pool({ connectionString, max: 12, application_name: schema })
  const source = postgres({ pool: shared, schema, namespace: 'recovery-source', outbox: true })
  const target = postgres({ pool: shared, schema, namespace: 'recovery-target' })
  const publisher = { concurrency: 2, pollIntervalMs: 10, retryBaseDelayMs: 10, retryMaxDelayMs: 20 }
  async function start(mode: 'admin' | 'missing' | 'worker') {
    @Module({
      imports: [
        MqModule.forRoot({
          connections: mode === 'missing' ? { source } : { source, target },
          execution: { workers: mode === 'worker', outboxPublisher: mode !== 'admin' },
          outbox: publisher,
          shutdown: { gracePeriodMs: 5_000 }
        }),
        ...(mode === 'missing' ? [] : [MqModule.forFeature([RecoveryQueue])])
      ],
      providers: mode === 'worker' ? [RecoveryWorker] : []
    })
    class Application {}
    return NestFactory.createApplicationContext(Application, { logger: false, abortOnError: false })
  }
  const inputs = ['123', null, { text: 'ação' }]
  const ids = ['publication-0', 'publication-1', 'publication-2', 'corrupt', 'draining', 'delayed']
  try {
    await migratePostgres({ pool: admin, schema })
    await admin.query(`CREATE TABLE "${schema}".business(id text PRIMARY KEY)`)
    const initial = await start('admin')
    try {
      const service = initial.get(MqOutboxService)
      const queue = initial.get(RecoveryQueue)
      for (const [index, id] of ids.entries()) {
        const job = await queue.echo.prepare(inputs[index] ?? 'control', {
          jobId: `job-${id}`, retry: { attempts: 4, backoff: { type: 'fixed', delayMs: 10 } }
        })
        // Preserve an actual null rather than replacing it with the control value.
        const prepared = index === 1 ? await queue.echo.prepare(null, { jobId: `job-${id}`, retry: { attempts: 4, backoff: { type: 'fixed', delayMs: 10 } } }) : job
        await postgresOutbox(service, 'source').transaction({ id, job: prepared, attempts: 2 }, async (tx) => {
          await tx.query(`INSERT INTO "${schema}".business(id) VALUES($1)`, [id])
        })
        const pending = await record(service, id)
        await assert.rejects(service.retryFailed('source', id, { expected: expected(pending), attempts: 1 }), MqOutboxException)
      }
    } finally { await initial.close() }

    // Exhaust real publisher attempts while the destination is not deployed.
    const missing = await start('missing')
    try {
      const service = missing.get(MqOutboxService)
      await until(async () => (await service.counts('source')).failed === ids.length, 'all missing-target publications exhaust their attempts')
      for (const id of ids) {
        const failed = await record(service, id)
        assert.equal(failed.attemptsMade, 2)
        assert.equal(failed.state, 'failed')
        assert.equal(failed.failure?.kind, 'target-missing')
        await assert.rejects(service.retryFailed('source', id, { expected: expected(failed), attempts: 2 }), MqOutboxException)
      }
    } finally { await missing.close() }
    console.log('PASS real publisher exhaustion and missing destination contract rejection')

    const left = await start('admin')
    const right = await start('admin')
    const accepted: OutboxSnapshot[] = []
    let delayedRunAt = 0
    try {
      const service = left.get(MqOutboxService)
      const competing = right.get(MqOutboxService)
      const id = ids[0]
      assert.ok(id)
      const failed = await record(service, id)
      const jobsBefore = (await admin.query<{ total: number }>(`SELECT count(*)::integer AS total FROM "${schema}".better_effect_mq_jobs`)).rows[0]?.total
      assert.equal(jobsBefore, 0)
      const raced = await Promise.allSettled([
        service.retryFailed('source', id, { expected: expected(failed), attempts: 3 }),
        competing.retryFailed('source', id, { expected: expected(failed), attempts: 3 })
      ])
      const success = raced.filter((value) => value.status === 'fulfilled')
      assert.equal(success.length, 1, 'Exactly one administrator may requeue the inspected version')
      for (const value of raced) if (value.status === 'rejected') assert.ok(value.reason instanceof MqOutboxException)
      const retried = await record(service, id)
      accepted.push(retried)
      assert.equal(retried.state, 'pending')
      assert.equal(retried.attemptsMade, 2)
      assert.equal(retried.attemptsMax, 5)
      assert.equal(retried.request.attemptsMax, 4)
      assert.equal(retried.request.dispatchKey, 'tenant-key')
      assert.deepEqual(retried.request, failed.request)
      assert.deepEqual(retried.failure, failed.failure)
      assert.equal(retried.createdAtMs, failed.createdAtMs)
      assert.ok(retried.updatedAtMs > failed.updatedAtMs)
      await assert.rejects(service.retryFailed('source', id, { expected: expected(failed), attempts: 3 }), MqOutboxException)
      await assert.rejects(service.retryFailed('target', id, { expected: expected(failed), attempts: 3 }), MqOutboxException)
      await assert.rejects(service.retryFailed('source', 'absent', { expected: expected(failed), attempts: 3 }), MqOutboxException)
      for (const nextId of ids.slice(1, 3)) {
        const old = await record(service, nextId)
        await assert.rejects(service.retryFailed('source', nextId, { expected: { ...expected(old), updatedAtMs: old.updatedAtMs + 1 }, attempts: 2 }), MqOutboxException)
        assert.deepEqual(await record(service, nextId), old)
        accepted.push(await service.retryFailed('source', nextId, { expected: expected(old), attempts: 3 }))
      }
      const corrupted = await record(service, 'corrupt')
      await admin.query(`UPDATE "${schema}".better_effect_mq_outbox SET request=jsonb_set(request,'{payload}','123'::jsonb) WHERE id='corrupt'`)
      await assert.rejects(service.retryFailed('source', 'corrupt', { expected: expected(corrupted), attempts: 2 }))
      assert.equal((await record(service, 'corrupt')).state, 'failed')
      console.log('PASS concurrent administrators, optimistic guards, immutable requests, budgets and schema revalidation')

      const blocker = await admin.connect()
      try {
        await blocker.query('BEGIN')
        await blocker.query(`SELECT id FROM "${schema}".better_effect_mq_outbox WHERE id='draining' FOR UPDATE`)
        const old = await record(service, 'draining')
        const writing = service.retryFailed('source', old.id, { expected: expected(old), attempts: 1, runAtMs: Date.now() + 60_000 })
        // Observe an actual blocked UPDATE; a timeout is only a test failure bound.
        await until(async () => (await admin.query(`SELECT pid FROM pg_stat_activity WHERE application_name=$1 AND wait_event_type='Lock' AND query LIKE 'UPDATE %'`, [schema])).rowCount === 1, 'retry UPDATE blocked on a real row lock')
        let closed = false
        const closing = left.close().then(() => { closed = true })
        await new Promise<void>((resolve) => setImmediate(resolve))
        assert.equal(closed, false, 'Store shutdown must await the admitted SQL mutation')
        await blocker.query('COMMIT')
        assert.equal((await writing).state, 'pending')
        await closing
        await assert.rejects(service.retryFailed('source', old.id, { expected: expected(old), attempts: 1 }), Error)
      } finally {
        await blocker.query('ROLLBACK')
        blocker.release()
      }
      const delayed = await record(competing, 'delayed')
      delayedRunAt = Date.now() + 500
      accepted.push(await competing.retryFailed('source', 'delayed', { expected: expected(delayed), attempts: 1, runAtMs: delayedRunAt }))
      console.log('PASS retry SQL drains on shutdown and borrowed application pool remains owned by its caller')
    } finally { await left.close(); await right.close() }

    const consumer = await start('worker')
    try {
      const queue = consumer.get(RecoveryQueue)
      const service = consumer.get(MqOutboxService)
      for (const [index, item] of accepted.entries()) {
        const jobId = item.request.id
        assert.ok(jobId)
        const result = await queue.echo.awaitResult(jobId, { timeoutMs: 10_000, pollIntervalMs: 10 })
        assert.deepEqual(result, index < inputs.length ? inputs[index] : 'control')
        await until(async () => (await record(service, item.id)).state === 'published', 'recovered publication acknowledgement')
        const published = await record(service, item.id)
        assert.equal(published.attemptsMade, 3)
        assert.deepEqual(published.request, item.request)
        await assert.rejects(service.retryFailed('source', item.id, { expected: expected(published), attempts: 1 }), MqOutboxException)
      }
      const delayedJob = await queue.echo.poll('job-delayed')
      assert.ok(delayedJob?.processedAt !== undefined && delayedJob.processedAt >= delayedRunAt)
      assert.equal((await admin.query<{ total: number }>(`SELECT count(*)::integer AS total FROM "${schema}".business`)).rows[0]?.total, ids.length)
      assert.equal((await admin.query<{ total: number }>(`SELECT count(*)::integer AS total FROM "${schema}".better_effect_mq_jobs`)).rows[0]?.total, accepted.length)
      console.log('PASS post-restart publication and processing preserve job IDs/JSON/dispatch keys without rerunning business writes')
    } finally { await consumer.close() }
    assert.equal((await shared.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)
  } finally {
    try { await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`) }
    finally { await shared.end(); await admin.end() }
  }
}
const database = process.env.MQ_TEST_DATABASE_URL
if (database !== undefined) await verifyRecovery(database)
else console.log('Packed outbox retry API/types verified; live recovery executes in PostgreSQL CI')
