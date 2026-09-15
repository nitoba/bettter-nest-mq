import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import assert from 'node:assert/strict'
import { mkdtemp, rm, stat } from 'node:fs/promises'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import { Test } from '@nestjs/testing'
import {
  Job,
  JobData,
  JobContext,
  JobFailureException,
  MqConnectionException,
  MqConnectionsService,
  MqModule,
  Process,
  Queue,
  QueueService,
  Retry,
  Worker,
  type MqConnection,
  type JobExecutionContext
} from '../../src/index.ts'
import { z } from 'zod'
import { sqlite, migrateSqlite, validateSqlite } from '../../src/integrations/sqlite-bun.ts'
import { sqliteConnection, type SqliteHost } from '../../src/integrations/sqlite-connection.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

@Queue({ name: 'sqlite-jobs', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
  @Job({ name: 'retry', version: 1 })
  @Retry({ attempts: 2, backoff: { type: 'fixed', delayMs: 5 } })
  readonly retry = this.job({
    payload: z.string(),
    result: z.string(),
    failure: z.object({ retry: z.boolean() }),
    retryable: (value) => value.retry
  })
  @Job({ name: 'date', version: 1 })
  readonly date = this.job({
    payload: zodCodec(
      z.codec(z.iso.datetime(), z.date(), {
        decode: (value) => new Date(value),
        encode: (date) => date.toISOString()
      })
    ),
    result: z.string()
  })
}
@Worker({ name: 'sqlite', concurrency: 2, pollIntervalMs: 5 })
class Processor {
  @Process(Jobs, 'echo')
  echo(@JobData() value: z.output<ReturnType<typeof z.json>>) {
    return value
  }
  @Process(Jobs, 'retry')
  retry(@JobData() value: string, @JobContext() context: JobExecutionContext) {
    if (context.attempt === 1) throw new JobFailureException({ retry: true })
    return value
  }
  @Process(Jobs, 'date')
  date(@JobData() value: Date) {
    assert.ok(value instanceof Date)
    return value.toISOString()
  }
}
async function application(connection: MqConnection, workers = false) {
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: connection }, execution: { workers } }),
      MqModule.forFeature([Jobs])
    ],
    providers: [Processor]
  }).compile()
  return app
}

test('file declarations are inert; explicit migrations are idempotent', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'mq-sqlite-'))
  const path = join(directory, 'jobs.db')
  try {
    sqlite({ path })
    await assert.rejects(stat(path), { code: 'ENOENT' })
    const initial = await migrateSqlite({ path })
    expect(initial.applied.length).toBeGreaterThan(0)
    expect((await migrateSqlite({ path })).applied).toEqual([])
    await validateSqlite({ path })
    expect(Object.isFrozen(initial.applied)).toBe(true)
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test('borrowed native handles remain open and their pragmas unchanged by default', async () => {
  const database = new Database(':memory:')
  try {
    await migrateSqlite({ database })
    database.exec('PRAGMA foreign_keys = OFF; PRAGMA busy_timeout = 27')
    const app = await application(sqlite({ database }))
    await app.init()
    expect(app.get(MqConnectionsService).connections()[0]).toMatchObject({
      adapter: 'sqlite',
      ownership: 'borrowed'
    })
    await app.close()
    expect(database.prepare('PRAGMA busy_timeout').get()).toEqual({ timeout: 27 })
    expect(database.prepare('PRAGMA foreign_keys').get()).toEqual({ foreign_keys: 0 })
    expect(database.prepare('SELECT 1 AS ok').get()).toEqual({ ok: 1 })
  } finally {
    database.close()
  }
})

test('startup validates but never migrates an empty borrowed database', async () => {
  const database = new Database(':memory:')
  try {
    const app = await application(sqlite({ database }))
    await assert.rejects(app.init(), MqConnectionException)
    await assert.rejects(app.close())
    expect(database.prepare("SELECT name FROM sqlite_master WHERE type='table'").all()).toEqual([])
    expect(database.prepare('SELECT 1 AS ok').get()).toEqual({ ok: 1 })
  } finally {
    database.close()
  }
})

test('owned database cleanup happens exactly once after failed startup and after normal shutdown', async () => {
  let acquired = 0
  let closed = 0
  let migrate = false
  const host: SqliteHost<Database> = {
    async open() {
      acquired += 1
      const database = new Database(':memory:')
      if (migrate) await migrateSqlite({ database })
      return database
    },
    close(database) {
      closed += 1
      database.close(true)
    }
  }
  const failed = await application(sqliteConnection({ path: 'not-opened-by-this-test.db' }, host))
  await assert.rejects(failed.init(), MqConnectionException)
  expect({ acquired, closed }).toEqual({ acquired: 1, closed: 1 })
  await assert.rejects(failed.close())
  expect(closed).toBe(1)
  migrate = true
  const healthy = await application(sqliteConnection({ path: 'not-opened-by-this-test.db' }, host))
  await healthy.init()
  await healthy.close()
  await healthy.close()
  expect({ acquired, closed }).toEqual({ acquired: 2, closed: 2 })
})

test('file-backed public jobs survive producer exit and preserve JSON, retries and Date codecs', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'mq-sqlite-durable-'))
  const path = join(directory, 'jobs.db')
  const values = [
    '123',
    'true',
    'null',
    '{"value":1}',
    '',
    'ação',
    0,
    5,
    false,
    null,
    ['array'],
    { value: 'object' }
  ]
  try {
    await migrateSqlite({ path })
    const producer = await application(sqlite({ path, namespace: 'durable', pollIntervalMs: 5 }))
    await producer.init()
    const ids: string[] = []
    try {
      const jobs = producer.get(Jobs)
      for (const value of values) ids.push(await jobs.echo.enqueue(value))
      expect(await jobs.echo.enqueue('123', { idempotencyKey: 'unique' })).toBe(
        await jobs.echo.enqueue('123', { idempotencyKey: 'unique' })
      )
      const prepared = await jobs.echo.prepare('prepared only', { jobId: 'unpublished' })
      expect(prepared.connection).toBe('primary')
      expect(await jobs.echo.poll('unpublished')).toBeUndefined()
    } finally {
      await producer.close()
    }
    const worker = await application(
      sqlite({ path, namespace: 'durable', pollIntervalMs: 5 }),
      true
    )
    await worker.init()
    try {
      const jobs = worker.get(Jobs)
      expect(
        await Promise.all(
          ids.map((id) => jobs.echo.awaitResult(id, { timeoutMs: 3000, pollIntervalMs: 5 }))
        )
      ).toEqual(values)
      const retryId = await jobs.retry.enqueue('retried')
      expect(await jobs.retry.awaitResult(retryId, { timeoutMs: 3000, pollIntervalMs: 5 })).toBe(
        'retried'
      )
      expect((await jobs.retry.attempts(retryId)).map((attempt) => attempt.outcome)).toEqual([
        'retried',
        'completed'
      ])
      expect(
        await jobs.date.execute('2026-09-15T12:00:00.000Z', {
          wait: { timeoutMs: 3000, pollIntervalMs: 5 }
        })
      ).toBe('2026-09-15T12:00:00.000Z')
    } finally {
      await worker.close()
    }
    const reader = await application(sqlite({ path, namespace: 'durable' }))
    await reader.init()
    try {
      expect((await reader.get(Jobs).echo.poll(ids[0]!))?.result).toBe('123')
    } finally {
      await reader.close()
    }
    const isolated = await application(sqlite({ path, namespace: 'another' }))
    await isolated.init()
    try {
      expect(await isolated.get(Jobs).echo.poll(ids[0]!)).toBeUndefined()
    } finally {
      await isolated.close()
    }
  } finally {
    await rm(directory, { recursive: true, force: true })
  }
})

test.each([
  {},
  { path: '' },
  { path: ':memory:' },
  { path: 'file:jobs.db' },
  { path: 'jobs.db', outbox: true },
  { path: 'jobs.db', pollIntervalMs: 0 },
  { path: 'jobs.db', busyTimeoutMs: -1 },
  { path: 'jobs.db', pollIntervalMs: 2_147_483_648 },
  { path: 'jobs.db', configurePragmas: 'yes' }
])('invalid or unsupported SQLite configuration fails before acquisition: %p', (options) => {
  expect(() => sqlite(JSON.parse(JSON.stringify(options)))).toThrow(MqConnectionException)
})

test('configuration getters are rejected without invocation', () => {
  let invoked = false
  expect(() =>
    sqlite({
      get path() {
        invoked = true
        return 'jobs.db'
      }
    })
  ).toThrow(MqConnectionException)
  expect(invoked).toBe(false)
})
