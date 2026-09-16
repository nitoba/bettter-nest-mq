import assert from 'node:assert/strict'
import { Database } from 'bun:sqlite'
import * as api from 'better-nest-mq/sqlite/bun'
import { verifySqlite } from './sqlite-common.js'
import { verifySqliteEvents } from './sqlite-events-common.js'
import { verifySqliteOutbox } from './sqlite-outbox-common.js'
import { verifySqliteSchedules } from './sqlite-schedules-common.js'

const mode = process.argv[2]
if (mode?.startsWith('outbox-'))
  await verifySqliteOutbox(api, 'bun', (path) => new Database(path))
else if (mode?.startsWith('schedules-'))
  await verifySqliteSchedules(api, 'bun', (path) => new Database(path))
else if (mode?.startsWith('events-')) await verifySqliteEvents(api, 'bun')
else {
  await verifySqlite(api, 'bun')
  if (mode !== 'consume') {
    await verifySqliteEvents(api, 'bun')
    await verifySqliteSchedules(api, 'bun', (path) => new Database(path))
    await verifySqliteOutbox(api, 'bun', (path) => new Database(path))
  }
}
const database = new Database(':memory:')
try {
  await api.migrateSqlite({ database })
  await api.validateSqlite({ database })
  assert.equal(api.sqlite({ database }).ownership, 'borrowed')
  assert.deepEqual(database.prepare('SELECT 1 AS ok').get(), { ok: 1 })
} finally {
  database.close(true)
}
