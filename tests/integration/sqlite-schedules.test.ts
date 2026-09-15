import { Database } from 'bun:sqlite'
import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Job,
  JobData,
  MqConnectionException,
  MqModule,
  MqScheduleException,
  MqSchedulesService,
  Process,
  Queue,
  QueueService,
  Schedule,
  Worker,
  type MqConnection,
  type MqModuleOptions
} from '../../src/index.ts'
import { sqliteSchedulesQualified } from '../../src/integrations/sqlite-adapter-version.ts'
import { migrateSqlite, sqlite } from '../../src/integrations/sqlite-bun.ts'
import { sqliteConnection } from '../../src/integrations/sqlite-connection.ts'

@Queue({ name: 'sqlite-schedules', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  @Schedule({ key: 'declared', everyMs: 60_000, payload: 'null' })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
  @Job({ name: 'other', version: 1 })
  readonly other = this.job({ payload: z.string(), result: z.string() })
}
@Worker({ name: 'sqlite-schedule-test', pollIntervalMs: 5 })
class Processor {
  @Process(Jobs, 'echo')
  echo(@JobData() value: z.output<ReturnType<typeof z.json>>) {
    return value
  }
}
function connection(database: Database, enabled = true): MqConnection {
  return sqlite({
    ...JSON.parse(JSON.stringify({ schedules: enabled })),
    database,
    namespace: 'schedules',
    events: true
  })
}
async function application(source: MqConnection, options: Partial<MqModuleOptions> = {}) {
  return Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: source },
        execution: { workers: false, scheduler: false },
        schedules: { mode: 'reconcile', sweepIntervalMs: 60_000 },
        ...options
      }),
      MqModule.forFeature([Jobs])
    ],
    providers: [Processor]
  }).compile()
}
const values = [
  'null',
  'true',
  '123',
  '{}',
  '[]',
  '"quoted"',
  '',
  'ação',
  null,
  false,
  123,
  ['null'],
  { nested: [null, 'true'] }
]

function scheduleTest(name: string, body: () => Promise<void>): void {
  test(name, async () => {
    if (!sqliteSchedulesQualified()) {
      const operation = () => sqlite({ path: './not-opened.db', schedules: true })
      expect(operation).toThrow(MqConnectionException)
      return
    }
    await body()
  })
}

scheduleTest(
  'SQLite schedules preserve scalar payloads, namespace and revisions across administrative writes',
  async () => {
    const database = new Database(':memory:')
    await migrateSqlite({ database })
    const app = await application(connection(database))
    try {
      await app.init()
      const schedules = app.get(MqSchedulesService)
      for (const [index, payload] of values.entries()) {
        const key = `payload-${index}`
        const original = await schedules.upsert(Jobs, 'echo', { key, everyMs: 60_000, payload })
        expect(original.payload).toEqual(payload)
        const unchanged = await schedules.upsert(Jobs, 'echo', { key, everyMs: 60_000, payload })
        expect(unchanged.revision).toBe(original.revision)
        await schedules.pause(Jobs, 'echo', key)
        const paused = await schedules.get(Jobs, 'echo', key)
        assert.ok(paused)
        expect(paused).toMatchObject({ payload, paused: true, nextRunAtMs: original.nextRunAtMs })
        await schedules.resume(Jobs, 'echo', key)
        expect((await schedules.get(Jobs, 'echo', key))?.payload).toEqual(payload)
        expect(
          database
            .prepare('SELECT payload FROM better_effect_mq_schedules WHERE schedule_key=?')
            .get(key)
        ).toEqual({ payload: JSON.stringify(payload) })
      }
      await app.get(Jobs).echo.enqueue('namespace probe')
      expect(
        database.prepare('SELECT DISTINCT namespace FROM better_effect_mq_schedules').all()
      ).toEqual(database.prepare('SELECT DISTINCT namespace FROM better_effect_mq_jobs').all())
    } finally {
      await app.close()
      database.close(true)
    }
  }
)

scheduleTest(
  'SQLite replicas validate without overwriting operator pauses, cursors or omitted definitions',
  async () => {
    const database = new Database(':memory:')
    await migrateSqlite({ database })
    const app = await application(connection(database))
    let saved
    try {
      await app.init()
      const schedules = app.get(MqSchedulesService)
      await schedules.pause(Jobs, 'echo', 'declared')
      saved = await schedules.get(Jobs, 'echo', 'declared')
      await schedules.upsert(Jobs, 'other', { key: 'keep', everyMs: 60_000, payload: 'keep' })
      await schedules.reconcile()
      expect(await schedules.get(Jobs, 'echo', 'declared')).toEqual(saved)
    } finally {
      await app.close()
    }
    const replica = await application(connection(database), { schedules: { mode: 'validate' } })
    try {
      await replica.init()
      const schedules = replica.get(MqSchedulesService)
      expect(await schedules.get(Jobs, 'echo', 'declared')).toEqual(saved)
      expect((await schedules.get(Jobs, 'other', 'keep'))?.payload).toBe('keep')
      await assert.rejects(schedules.reconcile(), MqScheduleException)
      await assert.rejects(schedules.get(Jobs, 'other', 'declared'), MqScheduleException)
      expect(await schedules.remove(Jobs, 'other', 'keep')).toBe(true)
    } finally {
      await replica.close()
      database.close(true)
    }
  }
)

scheduleTest(
  'SQLite scheduler emits one durable occurrence and native event waits observe its result',
  async () => {
    const database = new Database(':memory:')
    await migrateSqlite({ database })
    const app = await application(connection(database), {
      execution: { workers: true, scheduler: true }
    })
    try {
      await app.init()
      database
        .prepare('UPDATE better_effect_mq_schedules SET next_run_at_ms=? WHERE schedule_key=?')
        .run(Date.now() - 10, 'declared')
      const schedules = app.get(MqSchedulesService)
      await schedules.sweep()
      const fired = await schedules.get(Jobs, 'echo', 'declared')
      assert.ok(fired?.lastJobId)
      expect(fired.payload).toBe('null')
      expect(
        await app.get(Jobs).echo.awaitResult(fired.lastJobId, {
          strategy: 'events',
          timeoutMs: 2000,
          pollFallbackMs: 5000
        })
      ).toBe('null')
      await schedules.sweep()
      expect(database.prepare('SELECT count(*) AS total FROM better_effect_mq_jobs').get()).toEqual({
        total: 1
      })
      expect(schedules.scheduler()).toMatchObject({ state: 'running', reportedErrors: 0 })
    } finally {
      await app.close()
      database.close(true)
    }
  }
)

scheduleTest(
  'a validating SQLite replica cannot silently deploy missing schedule declarations',
  async () => {
    const database = new Database(':memory:')
    await migrateSqlite({ database })
    const app = await application(connection(database), { schedules: { mode: 'validate' } })
    try {
      await assert.rejects(app.init(), MqScheduleException)
      expect(database.prepare('SELECT * FROM better_effect_mq_schedules').all()).toEqual([])
    } finally {
      await assert.rejects(app.close(), MqScheduleException)
      database.close(true)
    }
  }
)

for (const owned of [true, false]) {
  scheduleTest(
    `failed SQLite schedule acquisition cleans up the ${owned ? 'owned' : 'borrowed'} handle`,
    async () => {
      const database = new Database(':memory:')
      await migrateSqlite({ database })
      const prepare = database.prepare.bind(database)
      let closes = 0
      Object.defineProperty(database, 'prepare', {
        value: (sql: string) => {
          if (sql.includes('FROM better_effect_mq_schedules'))
            throw new Error('Injected schedule read failure')
          return prepare(sql)
        }
      })
      const settings = JSON.parse('{"schedules":true}')
      const source = sqliteConnection(
        { ...settings, ...(owned ? { path: './owned.db' } : { database }) },
        {
          open: async () => database,
          close: () => {
            closes += 1
            database.close(true)
          }
        }
      )
      const app = await application(source)
      await assert.rejects(app.init())
      expect(closes).toBe(owned ? 1 : 0)
      if (!owned) {
        expect(prepare('SELECT 1 AS ok').get()).toEqual({ ok: 1 })
        database.close(true)
      }
    }
  )
}
