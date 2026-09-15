import assert from 'node:assert/strict'
import { Database } from 'bun:sqlite'
import * as api from 'better-nest-mq/sqlite/bun'
import { verifySqlite } from './sqlite-common.js'
import { verifySqliteEvents } from './sqlite-events-common.js'

const mode = process.argv[2]
if (mode?.startsWith('events-')) await verifySqliteEvents(api, 'bun')
else {
  await verifySqlite(api, 'bun')
  if (mode !== 'consume') await verifySqliteEvents(api, 'bun')
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
