import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import { Job, MqModule, Queue, QueueService } from '../../src/index.ts'
import type { MqConnection } from '../../src/index.ts'
import { migrateSqlite, sqlite } from '../../src/integrations/sqlite-bun.ts'
import { sqliteConnection } from '../../src/integrations/sqlite-connection.ts'

@Queue({ name: 'event-lifecycle', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.string(), result: z.string() })
}
async function moduleFor(connection: MqConnection) {
  return Test.createTestingModule({
    imports: [
      MqModule.forRoot({ connections: { primary: connection } }),
      MqModule.forFeature([Jobs])
    ]
  }).compile()
}

for (const ownership of ['owned', 'borrowed'] as const) {
  test(`event tail probe failure releases ${ownership} SQLite resources correctly`, async () => {
    const database = new Database(':memory:')
    await migrateSqlite({ database })
    const prepare = database.prepare.bind(database)
    let reads = 0
    let closes = 0
    Object.defineProperty(database, 'prepare', {
      configurable: true,
      value: (sql: string) => {
        if (sql.startsWith('SELECT next_cursor')) {
          reads += 1
          throw new Error('Injected event probe SQL failure')
        }
        return prepare(sql)
      }
    })
    const host = {
      open: async () => database,
      close: (native: Database) => {
        closes += 1
        native.close(true)
      }
    }
    const connection =
      ownership === 'owned'
        ? sqliteConnection({ path: './probe.db', events: true }, host)
        : sqliteConnection({ database, events: true }, host)
    const app = await moduleFor(connection)
    await assert.rejects(app.init())
    expect(reads).toBeGreaterThan(0)
    expect(closes).toBe(ownership === 'owned' ? 1 : 0)
    if (ownership === 'borrowed') {
      expect(database.prepare('SELECT 1 AS usable').get()).toEqual({ usable: 1 })
      database.close(true)
    }
  })
}

test('a fresh SQLite reader does not create the optional activation table or run migrations', async () => {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  const catalog = database.prepare(
    "SELECT name, sql FROM sqlite_master WHERE type = 'table' ORDER BY name"
  )
  const before = catalog.all()
  const schema = database.prepare('SELECT * FROM better_effect_mq_schema_versions').all()
  const app = await moduleFor(sqlite({ database, events: true }))
  try {
    await app.init()
    expect(catalog.all()).toEqual(before)
    expect(database.prepare('SELECT * FROM better_effect_mq_schema_versions').all()).toEqual(schema)
  } finally {
    await app.close()
    database.close(true)
  }
})

test('reopening a SQLite event reader preserves actual event history and writer activation', async () => {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  try {
    const producer = await moduleFor(sqlite({ database, events: false }))
    try {
      await producer.init()
      await producer.get(Jobs).echo.enqueue('retained event', { jobId: 'retained' })
    } finally {
      await producer.close()
    }
    const activation = database.prepare('SELECT * FROM better_effect_mq_job_event_activation').all()
    const history = database.prepare('SELECT * FROM better_effect_mq_job_events').all()
    expect(history.length).toBeGreaterThan(0)
    const app = await moduleFor(sqlite({ database, events: true }))
    try {
      await app.init()
      expect(database.prepare('SELECT * FROM better_effect_mq_job_event_activation').all()).toEqual(
        activation
      )
      expect(database.prepare('SELECT * FROM better_effect_mq_job_events').all()).toEqual(history)
      expect((await app.get(Jobs).echo.poll('retained'))?.state).toBe('waiting')
    } finally {
      await app.close()
    }
  } finally {
    database.close(true)
  }
})
