import assert from 'node:assert/strict'
import { Database } from 'bun:sqlite'
import * as api from 'better-nest-mq/sqlite/bun'
import { verifySqlite } from './sqlite-common.js'

await verifySqlite(api, 'bun')
const database = new Database(':memory:')
try {
  await api.migrateSqlite({ database })
  await api.validateSqlite({ database })
  assert.equal(api.sqlite({ database }).ownership, 'borrowed')
  assert.equal(database.prepare('SELECT 1 AS ok').get()?.ok, 1)
} finally { database.close(true) }
