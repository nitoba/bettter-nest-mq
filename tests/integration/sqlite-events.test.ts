import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import assert from 'node:assert/strict'
import { Inject, Injectable } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Job,
  JobData,
  JobWaitAbortedException,
  JobWaitTimeoutException,
  MqJobException,
  MqModule,
  Process,
  Queue,
  QueueService,
  Worker
} from '../../src/index.ts'
import { migrateSqlite, sqlite } from '../../src/integrations/sqlite-bun.ts'

@Queue({ name: 'sqlite-events', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.string(), result: z.string() })
}
@Injectable()
class Gate {
  readonly release = Promise.withResolvers<void>()
}
@Worker({ name: 'sqlite-events', pollIntervalMs: 5 })
class Processor {
  constructor(@Inject(Gate) private readonly gate: Gate) {}
  @Process(Jobs, 'echo')
  async echo(@JobData() value: string) {
    await this.gate.release.promise
    return value
  }
}
async function fixture() {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  const entered = Promise.withResolvers<void>()
  const prepare = database.prepare.bind(database)
  const faults = { failReads: false, reads: 0 }
  Object.defineProperty(database, 'prepare', {
    configurable: true,
    value: (sql: string) => {
      if (sql.startsWith('SELECT cursor,recorded_at_ms')) {
        faults.reads += 1
        entered.resolve()
        if (faults.failReads) throw new Error('Injected native event SELECT failure')
      }
      return prepare(sql)
    }
  })
  return { database, entered: entered.promise, faults }
}
async function application(database: Database, events: boolean, workers: boolean) {
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: {
          primary: sqlite({ database, namespace: 'events', events, pollIntervalMs: 5 })
        },
        execution: { workers },
        shutdown: { gracePeriodMs: 20 }
      }),
      MqModule.forFeature([Jobs])
    ],
    providers: [Gate, Processor]
  }).compile()
  await app.init()
  return app
}
const wait = { strategy: 'events', pollFallbackMs: 5_000, timeoutMs: 2_000 } as const

test('SQLite waits read the real event log in the raw job namespace before fallback', async () => {
  const source = await fixture()
  const app = await application(source.database, true, true)
  try {
    const job = app.get(Jobs).echo
    const id = await job.enqueue('null')
    const waiting = job.awaitResult(id, wait)
    await Promise.race([source.entered, waiting])
    expect(source.faults.reads).toBeGreaterThan(0)
    app.get(Gate).release.resolve()
    expect(await waiting).toBe('null')
    expect(await job.awaitResult(id, wait)).toBe('null')
    const eventNamespace = source.database
      .prepare('SELECT DISTINCT namespace FROM better_effect_mq_job_events WHERE job_id = ?')
      .all(id)
    const jobNamespace = source.database
      .prepare('SELECT namespace FROM better_effect_mq_jobs WHERE id = ?')
      .all(id)
    expect(eventNamespace).toEqual(jobNamespace)
    expect(eventNamespace).toHaveLength(1)
    expect(await job.execute('true', { wait })).toBe('true')
  } finally {
    app.get(Gate).release.resolve()
    await app.close()
    source.database.close(true)
  }
})

test('events=false rejects the strategy without disabling persisted native event writes', async () => {
  const source = await fixture()
  const app = await application(source.database, false, true)
  try {
    app.get(Gate).release.resolve()
    const job = app.get(Jobs).echo
    const id = await job.enqueue('ordinary')
    expect(await job.awaitResult(id, { timeoutMs: 1000, pollIntervalMs: 5 })).toBe('ordinary')
    await assert.rejects(job.awaitResult(id, wait), MqJobException)
    expect(
      source.database
        .prepare(
          "SELECT job_id FROM better_effect_mq_job_events WHERE event_type = 'job-completed'"
        )
        .all()
    ).toHaveLength(1)
  } finally {
    app.get(Gate).release.resolve()
    await app.close()
    source.database.close(true)
  }
})

test('native event SELECT failures fall back to job reads without losing scalar results', async () => {
  const source = await fixture()
  const app = await application(source.database, true, true)
  try {
    const job = app.get(Jobs).echo
    const id = await job.enqueue('123')
    source.faults.failReads = true
    const waiting = job.awaitResult(id, { ...wait, pollFallbackMs: 20 })
    await Promise.race([source.entered, waiting])
    expect(source.faults.reads).toBeGreaterThan(0)
    app.get(Gate).release.resolve()
    expect(await waiting).toBe('123')
  } finally {
    app.get(Gate).release.resolve()
    await app.close()
    source.database.close(true)
  }
})

test('SQLite caller timeout and abort leave durable work waiting', async () => {
  const source = await fixture()
  const app = await application(source.database, true, false)
  try {
    const job = app.get(Jobs).echo
    const id = await job.enqueue('still pending')
    await assert.rejects(job.awaitResult(id, { ...wait, timeoutMs: 20 }), JobWaitTimeoutException)
    const controller = new AbortController()
    const waiting = job.awaitResult(id, { ...wait, signal: controller.signal })
    const rejected = assert.rejects(waiting, JobWaitAbortedException)
    await source.entered
    controller.abort()
    await rejected
    expect((await job.poll(id))?.state).toBe('waiting')
  } finally {
    await app.close()
    source.database.close(true)
  }
})

test('SQLite shutdown aborts event waits and preserves borrowed pragmas and ownership', async () => {
  const source = await fixture()
  source.database.exec('PRAGMA foreign_keys = OFF; PRAGMA busy_timeout = 123')
  const before = source.database.prepare('PRAGMA busy_timeout').get()
  const app = await application(source.database, true, false)
  try {
    const id = await app.get(Jobs).echo.enqueue('survives shutdown')
    const waiting = app.get(Jobs).echo.awaitResult(id, wait)
    const rejected = assert.rejects(waiting, (cause) => {
      assert.ok(cause instanceof Error)
      assert.ok(!(cause instanceof JobWaitTimeoutException))
      return true
    })
    await source.entered
    await app.close()
    await rejected
    expect(source.database.prepare('PRAGMA busy_timeout').get()).toEqual(before)
    expect(source.database.prepare('PRAGMA foreign_keys').get()).toEqual({ foreign_keys: 0 })
    expect(source.database.prepare('SELECT state FROM better_effect_mq_jobs').all()).toEqual([
      { state: 'waiting' }
    ])
  } finally {
    await app.close()
    source.database.close(true)
  }
})
