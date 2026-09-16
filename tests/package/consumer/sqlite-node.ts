import assert from 'node:assert/strict'
import { DatabaseSync } from 'node:sqlite'
import * as api from 'better-nest-mq/sqlite/node'
import { verifySqlite } from './sqlite-common.js'
import { verifySqliteEvents } from './sqlite-events-common.js'
import { verifySqliteOutbox } from './sqlite-outbox-common.js'
import { verifySqliteSchedules } from './sqlite-schedules-common.js'

const mode = process.argv[2]
if (mode?.startsWith('outbox-'))
  await verifySqliteOutbox(api, 'node', (path) => new DatabaseSync(path))
else if (mode?.startsWith('schedules-'))
  await verifySqliteSchedules(api, 'node', (path) => new DatabaseSync(path))
else if (mode?.startsWith('events-')) await verifySqliteEvents(api, 'node')
else {
  await verifySqlite(api, 'node')
  if (mode !== 'consume') {
    await verifySqliteEvents(api, 'node')
    await verifySqliteSchedules(api, 'node', (path) => new DatabaseSync(path))
    await verifySqliteOutbox(api, 'node', (path) => new DatabaseSync(path))
  }
}
const database = new DatabaseSync(':memory:')
try {
  await api.migrateSqlite({ database })
  await api.validateSqlite({ database })
  assert.equal(api.sqlite({ database }).ownership, 'borrowed')
  assert.equal(database.prepare('SELECT 1 AS ok').get()?.ok, 1)
} finally {
  database.close()
}
