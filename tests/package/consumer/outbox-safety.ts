import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import { Job, MqModule, MqOutboxService, MqOutboxException, Queue, QueueService, type OutboxEntry } from 'better-nest-mq'
import { migratePostgres, postgres, postgresOutbox } from 'better-nest-mq/postgres'

@Queue({ name: 'outbox-safety', connection: 'target' })
class SafetyQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.object({ value: z.string() }), result: z.string() })
}

function barrier() {
  let release: (() => void) | undefined
  const promise = new Promise<void>((resolve) => { release = resolve })
  return { promise, open: () => { assert.ok(release); release() } }
}
async function until(check: () => Promise<boolean>, label: string): Promise<void> {
  const deadline = Date.now() + 5_000
  while (!(await check())) { assert.ok(Date.now() < deadline, `Timed out: ${label}`); await sleep(10) }
}

export async function verifyOutboxSafety(connectionString: string): Promise<void> {
  const schema = `mq_outbox_safety_${randomUUID().replaceAll('-', '')}`
  const admin = new Pool({ connectionString, max: 4 })
  const borrowed = new Pool({ connectionString, max: 6 })
  const source = postgres({ pool: borrowed, schema, namespace: 'source', outbox: true })
  const target = postgres({ connectionString, schema, namespace: 'target' })
  try {
    await migratePostgres({ pool: admin, schema })
    await admin.query(`CREATE TABLE "${schema}".business(id text PRIMARY KEY)`)
    @Module({ imports: [
      MqModule.forRoot({ connections: { source, target }, execution: { workers: false, outboxPublisher: false }, shutdown: { gracePeriodMs: 1_000 } }),
      MqModule.forFeature([SafetyQueue])
    ] })
    class Producer {}
    const app = await NestFactory.createApplicationContext(Producer, { logger: false, abortOnError: false })
    let closed = false
    try {
      const service = app.get(MqOutboxService)
      const outbox = postgresOutbox(service, 'source')
      const queue = app.get(SafetyQueue)
      const prepare = async (id: string): Promise<OutboxEntry> => ({ id, job: await queue.task.prepare({ value: id }, { jobId: id }), attempts: 3 })

      const drain = await prepare('drained')
      await outbox.transaction(async (tx) => {
        // Already-started operations are tracked even when the callback forgets to await them.
        void tx.query(`INSERT INTO "${schema}".business SELECT $1 FROM pg_sleep(0.03)`, ['drained'])
        void tx.append(drain)
      })
      assert.equal((await admin.query(`SELECT 1 FROM "${schema}".business WHERE id='drained'`)).rowCount, 1)
      assert.equal((await service.get('source', 'drained'))?.state, 'pending')

      const rejected = await prepare('caught-append-failure')
      await assert.rejects(outbox.transaction(async (tx) => {
        await tx.query(`INSERT INTO "${schema}".business VALUES ($1)`, [rejected.id])
        await assert.rejects(tx.append({ ...rejected, job: { ...rejected.job, connection: 'unregistered' } }))
      }))
      assert.equal(await service.get('source', rejected.id), undefined)
      assert.equal((await admin.query(`SELECT 1 FROM "${schema}".business WHERE id=$1`, [rejected.id])).rowCount, 0)
      console.log('PASS admitted operations drain before commit and caught append failures still roll back')

      const lost = await prepare('lost-connection')
      const entered = barrier()
      let pid: number | undefined
      let executions = 0
      const task = outbox.transaction(async (tx) => {
        executions += 1
        await tx.query(`INSERT INTO "${schema}".business VALUES ($1)`, [lost.id])
        await tx.append(lost)
        const result = await tx.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')
        pid = result.rows[0]?.pid
        entered.open()
        await tx.query('SELECT pg_sleep(10)')
      })
      const failed = assert.rejects(task)
      await entered.promise
      assert.ok(pid)
      await until(async () => (await admin.query(`SELECT 1 FROM pg_stat_activity WHERE pid=$1 AND query='SELECT pg_sleep(10)' AND state='active'`, [pid])).rowCount === 1, 'transaction query starts')
      await admin.query('SELECT pg_terminate_backend($1)', [pid])
      await failed
      assert.equal(executions, 1, 'A lost transaction must never rerun its business callback')
      assert.equal((await admin.query(`SELECT 1 FROM "${schema}".business WHERE id=$1`, [lost.id])).rowCount, 0)
      assert.equal(await service.get('source', lost.id), undefined)
      assert.equal((await borrowed.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)
      console.log('PASS active transaction disconnection rolls back, releases the failed client and never replays the callback')

      const closingEntry = await prepare('shutdown-commit')
      const started = barrier()
      const finish = barrier()
      const transaction = outbox.transaction(async (tx) => {
        await tx.query(`INSERT INTO "${schema}".business VALUES ($1)`, [closingEntry.id])
        started.open()
        await finish.promise
        await tx.append(closingEntry)
      })
      await started.promise
      const closing = app.close()
      await sleep(10)
      finish.open()
      await Promise.all([transaction, closing])
      closed = true
      assert.equal((await admin.query(`SELECT 1 FROM "${schema}".business WHERE id=$1`, [closingEntry.id])).rowCount, 1)
      assert.equal((await admin.query(`SELECT 1 FROM "${schema}".better_effect_mq_outbox WHERE id=$1 AND state='pending'`, [closingEntry.id])).rowCount, 1)
      await assert.rejects(service.counts('source'), MqOutboxException)
      console.log('PASS shutdown drains the already-admitted domain transaction before releasing its outbox resource')
    } finally { if (!closed) await app.close() }

    @Module({ imports: [MqModule.forRoot({ connections: { source }, execution: { workers: false }, outbox: { pollIntervalMs: 10, retryBaseDelayMs: 10, retryMaxDelayMs: 50 } })] })
    class IncompletePublisher {}
    const publisher = await NestFactory.createApplicationContext(IncompletePublisher, { logger: false, abortOnError: false })
    try {
      const service = publisher.get(MqOutboxService)
      await until(async () => (await service.counts('source')).failed === 2, 'missing target routes become classified terminal failures')
      for (const id of ['drained', 'shutdown-commit']) {
        const record = await service.get('source', id)
        assert.ok(record)
        assert.equal(record.failure?.kind, 'target-missing')
        assert.equal(record.attemptsMade, 1)
        assert.equal(record.attemptsMax, 3)
      }
      assert.equal((await admin.query(`SELECT 1 FROM "${schema}".better_effect_mq_jobs`)).rowCount, 0)
      console.log('PASS missing-route failures stay in the outbox without publishing to another destination')
    } finally { await publisher.close() }
  } finally {
    await borrowed.end()
    try { await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`) }
    finally { await admin.end() }
  }
}
