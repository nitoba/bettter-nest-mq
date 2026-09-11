import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { Pool } from 'pg'
import { postgresJsonPool } from '../../src/integrations/postgres-json-pool.ts'

const connectionString = process.env.MQ_TEST_DATABASE_URL
assert.ok(connectionString, 'Use an isolated PostgreSQL test database')
const tag = `json_fault_${randomUUID().replaceAll('-', '')}`
const pool = new Pool({ connectionString, max: 1, application_name: tag })
const control = new Pool({ connectionString, max: 1 })
const view = postgresJsonPool(pool)
try {
  const first = await view.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')
  await assert.rejects(view.query(`SELECT * FROM "missing_${randomUUID().replaceAll('-', '')}"`))
  const second = await view.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')
  assert.notEqual(
    second.rows[0]?.pid,
    first.rows[0]?.pid,
    'Like native pool.query, the adapter view must discard a client released after query failure'
  )

  const rejected = assert.rejects(view.query('SELECT pg_sleep(10)'))
  const deadline = Date.now() + 3_000
  let terminated = false
  while (!terminated) {
    assert.ok(Date.now() < deadline, 'The synthetic slow query must start before termination')
    const result = await control.query<{ terminated: boolean }>(
      "SELECT pg_terminate_backend(pid) AS terminated FROM pg_stat_activity WHERE application_name=$1 AND state='active' AND query='SELECT pg_sleep(10)'",
      [tag]
    )
    terminated = result.rows.some((row) => row.terminated)
    if (!terminated) await sleep(10)
  }
  await rejected
  assert.deepEqual((await view.query('SELECT $1::jsonb AS value', [JSON.stringify('123')])).rows, [
    { value: '"123"' }
  ])
  assert.deepEqual((await pool.query('SELECT $1::jsonb AS value', [JSON.stringify('123')])).rows, [
    { value: '123' }
  ])
  assert.equal(pool.waitingCount, 0)
  console.log(
    'PASS query failure discards its client; active backend termination rejects without process crash and the same native pool remains usable'
  )
} finally {
  try {
    await pool.end()
  } finally {
    await control.end()
  }
}
