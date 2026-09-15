import { expect, test } from 'bun:test'
import { Database } from 'bun:sqlite'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { MqModule } from '../../src/index.ts'
import type { MqConnection } from '../../src/index.ts'
import { migrateSqlite, sqlite } from '../../src/integrations/sqlite-bun.ts'
import { sqliteConnection } from '../../src/integrations/sqlite-connection.ts'

async function moduleFor(connection: MqConnection) {
  return Test.createTestingModule({
    imports: [MqModule.forRoot({ connections: { primary: connection } })]
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

test('enabling only a SQLite reader does not activate writers, prune history or migrate', async () => {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  const schema = database.prepare('SELECT * FROM better_effect_mq_schema_versions').all()
  const activation = database.prepare('SELECT * FROM better_effect_mq_job_event_activation').all()
  const history = database.prepare('SELECT * FROM better_effect_mq_job_events').all()
  const app = await moduleFor(sqlite({ database, events: true }))
  try {
    await app.init()
    expect(database.prepare('SELECT * FROM better_effect_mq_schema_versions').all()).toEqual(schema)
    expect(database.prepare('SELECT * FROM better_effect_mq_job_event_activation').all()).toEqual(
      activation
    )
    expect(database.prepare('SELECT * FROM better_effect_mq_job_events').all()).toEqual(history)
  } finally {
    await app.close()
    database.close(true)
  }
})
