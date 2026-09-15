import assert from 'node:assert/strict'
import { DatabaseSync } from 'node:sqlite'
import * as api from 'better-nest-mq/sqlite/node'
import { verifySqlite } from './sqlite-common.js'
import { verifySqliteEvents } from './sqlite-events-common.js'

const mode = process.argv[2]
if (mode?.startsWith('events-')) await verifySqliteEvents(api, 'node')
else {
  await verifySqlite(api, 'node')
  if (mode !== 'consume') await verifySqliteEvents(api, 'node')
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
