import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { spawn, type ChildProcess } from 'node:child_process'
import { fileURLToPath } from 'node:url'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool } from 'pg'
import { z } from 'zod'
import { JobData, MqModule, MqSchedulesService, MqScheduleException, Process, Worker, type PayloadOf, type ScheduleOptions } from 'better-nest-mq'
import { postgres, migratePostgres } from 'better-nest-mq/postgres'
import { ScheduledQueue, ValueSchema } from './schedule-contracts.js'

const messageSchema = z.union([z.object({ type: z.literal('ready'), pid: z.int() }), z.object({ type: z.enum(['done', 'error']), id: z.string(), message: z.string().optional() })])
class SchedulerProcess {
  readonly child: ChildProcess
  readonly ready: Promise<number>
  readonly exited: Promise<number | null>
  private readonly pending = new Map<string, { resolve(): void; reject(cause: Error): void }>()
  private errors = ''
  constructor(connectionString: string, schema: string) {
    this.child = spawn('node', [fileURLToPath(new URL('./schedule-child.js', import.meta.url))], {
      stdio: ['ignore', 'ignore', 'pipe', 'ipc'], env: { ...process.env, MQ_TEST_DATABASE_URL: connectionString, MQ_SCHEDULE_SCHEMA: schema }
    })
    this.child.stderr?.on('data', (chunk: Buffer) => { this.errors = `${this.errors}${chunk.toString()}`.slice(-8_000) })
    this.exited = new Promise((resolve) => this.child.once('exit', resolve))
    this.ready = new Promise((resolve, reject) => {
      this.child.once('error', reject)
      this.child.once('exit', (code) => {
        const error = new Error(`Scheduler child exited ${code}: ${this.errors}`)
        reject(error)
        for (const pending of this.pending.values()) pending.reject(error)
        this.pending.clear()
      })
      this.child.on('message', (input) => {
        const parsed = messageSchema.safeParse(input)
        if (!parsed.success) return
        const message = parsed.data
        if (message.type === 'ready') { resolve(message.pid); return }
        const pending = this.pending.get(message.id)
        this.pending.delete(message.id)
        if (message.type === 'error') pending?.reject(new Error(message.message ?? 'sweep failed'))
        else pending?.resolve()
      })
    })
  }
  async sweep(): Promise<void> {
    const id = randomUUID()
    const timer = setTimeout(() => { this.pending.get(id)?.reject(new Error('Scheduler IPC sweep timed out')); this.pending.delete(id) }, 10_000)
    try { await new Promise<void>((resolve, reject) => { this.pending.set(id, { resolve, reject }); this.child.send({ id, type: 'sweep' }) }) }
    finally { clearTimeout(timer) }
  }
  async stop(): Promise<void> {
    if (this.child.connected) this.child.send({ id: randomUUID(), type: 'stop' })
    const timer = setTimeout(() => this.child.kill('SIGKILL'), 10_000)
    try { assert.equal(await this.exited, 0, this.errors) }
    finally { clearTimeout(timer) }
  }
}

@Worker({ name: 'scheduled-results', pollIntervalMs: 10 })
class ScheduledWorker {
  @Process(ScheduledQueue, 'echo')
  echo(@JobData() value: PayloadOf<ScheduledQueue['echo']>) { return value }
  @Process(ScheduledQueue, 'date')
  date(@JobData() value: PayloadOf<ScheduledQueue['date']>) { assert.ok(value.timestamp instanceof Date); return value }
}

async function verify(connectionString: string): Promise<void> {
  const schema = `mq_schedule_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 5 })
  const children: SchedulerProcess[] = []
  const connection = postgres({ pool, schema, namespace: 'schedules', schedules: true, outbox: true })
  const wait = { timeoutMs: 5_000, pollIntervalMs: 10 }
  const completed: { id: string; value: z.output<typeof ValueSchema> }[] = []
  @Module({ imports: [MqModule.forRoot({ connections: { primary: connection }, execution: { scheduler: false, workers: false, outboxPublisher: false }, schedules: { mode: 'reconcile' } }), MqModule.forFeature([ScheduledQueue])] })
  class DeployModule {}
  @Module({ imports: [MqModule.forRoot({ connections: { primary: connection }, execution: { scheduler: false, outboxPublisher: false } }), MqModule.forFeature([ScheduledQueue])], providers: [ScheduledWorker] })
  class ConsumerModule {}
  try {
    await migratePostgres({ pool, schema })
    const app = await NestFactory.createApplicationContext(DeployModule, { logger: false, abortOnError: false })
    try {
      const schedules = app.get(MqSchedulesService)
      assert.equal(schedules.scheduler(), undefined)
      const initial = await schedules.get(ScheduledQueue, 'echo', 'regular')
      assert.ok(initial)
      await schedules.pause(ScheduledQueue, 'echo', 'regular')
      const paused = await schedules.get(ScheduledQueue, 'echo', 'regular')
      await schedules.reconcile()
      assert.deepEqual(await schedules.get(ScheduledQueue, 'echo', 'regular'), paused)
      const cron = await schedules.upsert(ScheduledQueue, 'echo', { key: 'cron', cron: '0 9 * * *', timeZone: 'America/Fortaleza', payload: 'cron' })
      assert.equal(new Intl.DateTimeFormat('en-GB', { timeZone: 'America/Fortaleza', hour: '2-digit', hourCycle: 'h23' }).format(cron.nextRunAtMs), '09')
      await schedules.pause(ScheduledQueue, 'echo', 'cron')
      const left = new SchedulerProcess(connectionString, schema)
      const right = new SchedulerProcess(connectionString, schema)
      children.push(left, right)
      const timer = setTimeout(() => { left.child.kill('SIGKILL'); right.child.kill('SIGKILL') }, 15_000)
      let pids: number[]
      try { pids = await Promise.all([left.ready, right.ready]) }
      finally { clearTimeout(timer) }
      assert.equal(new Set(pids).size, 2)
      const both = () => Promise.all(children.map((child) => child.sweep()))
      const due = async (key: string, ago = 10) => {
        const value = Date.now() - ago
        const result = await pool.query(`UPDATE "${schema}".better_effect_mq_schedules SET next_run_at_ms=$1 WHERE schedule_group=$2 AND schedule_key=$3`, [value, 'nestjs/schedules', key])
        assert.equal(result.rowCount, 1)
        return value
      }
      const count = async () => (await pool.query<{ count: number }>(`SELECT count(*)::integer AS count FROM "${schema}".better_effect_mq_jobs`)).rows[0]?.count ?? 0
      const inputs: z.output<typeof ValueSchema>[] = ['true', '123', '{"looks":"json"}', '', 'ação', 123, false, null, ['array'], { text: 'object' }]
      for (const [index, payload] of inputs.entries()) {
        const key = `json-${index}`
        await schedules.upsert(ScheduledQueue, 'echo', { key, everyMs: 60_000, payload })
        const before = await count()
        await due(key)
        await both()
        const record = await schedules.get(ScheduledQueue, 'echo', key)
        assert.ok(record?.lastJobId)
        assert.deepEqual(record.payload, payload)
        assert.equal(await count(), before + 1, 'Competing schedulers must persist one job for the same slot')
        await both()
        assert.equal(await count(), before + 1)
        completed.push({ id: record.lastJobId, value: payload })
        await schedules.remove(ScheduledQueue, 'echo', key)
      }
      console.log('PASS independent scheduler processes, scalar/null JSON, stable occurrence fencing and cron timezone')

      const beforeMisfires = await count()
      await schedules.upsert(ScheduledQueue, 'echo', { key: 'catch-up', everyMs: 60_000, payload: 'catch-up', misfire: { strategy: 'catch-up', maxOccurrences: 2 } })
      await due('catch-up', 125_000)
      await left.sweep()
      assert.equal(await count(), beforeMisfires + 2)
      await left.sweep()
      assert.equal(await count(), beforeMisfires + 3)
      await schedules.remove(ScheduledQueue, 'echo', 'catch-up')
      await schedules.upsert(ScheduledQueue, 'echo', { key: 'skip', everyMs: 60_000, payload: 'skip', misfire: { strategy: 'skip' } })
      await due('skip', 125_000)
      await both()
      assert.equal(await count(), beforeMisfires + 3)
      await schedules.remove(ScheduledQueue, 'echo', 'skip')
      await schedules.upsert(ScheduledQueue, 'echo', { key: 'overlap', everyMs: 60_000, payload: 'overlap', overlap: 'skip' })
      await due('overlap')
      await both()
      const overlap = await schedules.get(ScheduledQueue, 'echo', 'overlap')
      assert.ok(overlap?.lastJobId)
      const withOverlap = await count()
      await due('overlap')
      await both()
      assert.equal(await count(), withOverlap, 'An unfinished prior occurrence blocks overlap')
      await app.get(ScheduledQueue).echo.cancel(overlap.lastJobId)
      await due('overlap')
      await both()
      assert.equal(await count(), withOverlap + 1)
      await schedules.remove(ScheduledQueue, 'echo', 'overlap')
      console.log('PASS bounded catch-up per sweep, overdue-slot skipping and overlap prevention')

      const dateInput = { timestamp: '2026-09-11T12:00:00.000Z' }
      await schedules.upsert(ScheduledQueue, 'date', { key: 'date', everyMs: 60_000, payload: dateInput })
      await schedules.pause(ScheduledQueue, 'date', 'date')
      await due('date')
      await both()
      assert.equal((await schedules.get(ScheduledQueue, 'date', 'date'))?.lastJobId, undefined)
      await schedules.resume(ScheduledQueue, 'date', 'date')
      await both()
      const dateRecord = await schedules.get(ScheduledQueue, 'date', 'date')
      assert.ok(dateRecord?.lastJobId)
      await schedules.pause(ScheduledQueue, 'date', 'date')
      await assert.rejects(schedules.get(ScheduledQueue, 'echo', 'date'), MqScheduleException)
      await Promise.all(children.map((child) => child.stop()))
      children.length = 0
      const consumer = await NestFactory.createApplicationContext(ConsumerModule, { logger: false, abortOnError: false })
      try {
        const queue = consumer.get(ScheduledQueue)
        for (const result of completed) assert.deepEqual(await queue.echo.awaitResult(result.id, wait), result.value)
        assert.deepEqual(await queue.date.awaitResult(dateRecord.lastJobId, wait), { timestamp: new Date(dateInput.timestamp) })
        assert.equal((await consumer.get(MqSchedulesService).get(ScheduledQueue, 'echo', 'regular'))?.revision, paused?.revision)
      } finally { await consumer.close() }
      console.log('PASS pause/resume, decoded Date result, later worker processing and persisted schedule revisions')
    } finally { await app.close() }
    assert.equal((await pool.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1, 'Borrowed pool remains usable')
  } finally {
    try { await Promise.all(children.map((child) => child.stop())) }
    finally { try { await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`) } finally { await pool.end() } }
  }
}
const connectionString = process.env.MQ_TEST_DATABASE_URL
if (connectionString !== undefined) await verify(connectionString)
else console.log('Packed schedule APIs and types passed; real competing schedulers run in PostgreSQL CI')

// @ts-expect-error A schedule cannot combine relative intervals and cron.
export const invalid: ScheduleOptions = { key: 'invalid', everyMs: 10, cron: '* * * * *', payload: null }
