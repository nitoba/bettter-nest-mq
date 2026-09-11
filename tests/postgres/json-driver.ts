import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Pool, type Notification } from 'pg'
import { postgresJsonPool } from '../../src/integrations/postgres-json-pool.ts'

const connectionString = process.env.MQ_TEST_DATABASE_URL
assert.ok(connectionString, 'MQ_TEST_DATABASE_URL must point to an isolated test database')
const pool = new Pool({ connectionString, max: 3 })
const view = postgresJsonPool(pool)
try {
  const result = await view.query('SELECT $1::json AS json_text,$1::jsonb AS jsonb_text,$2::jsonb AS json_null,NULL::jsonb AS sql_null,$3::integer AS n,$4::text AS plain', [JSON.stringify('null'), 'null', 42, '123'])
  assert.deepEqual(result.rows, [{ json_text: '"null"', jsonb_text: '"null"', json_null: 'null', sql_null: null, n: 42, plain: '123' }])
  const native = await pool.query('SELECT $1::jsonb AS value', [JSON.stringify('123')])
  assert.deepEqual(native.rows, [{ value: '123' }])

  const client = await view.connect()
  const channel = `json_probe_${randomUUID().replaceAll('-', '')}`
  let onNotification: ((notification: Notification) => void) | undefined
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    const backend = await client.query<{ pid: number }>('SELECT pg_backend_pid() AS pid')
    await client.query('BEGIN')
    await client.query('CREATE TEMP TABLE json_driver_probe(value jsonb) ON COMMIT DROP')
    await client.query('INSERT INTO json_driver_probe(value) VALUES ($1::jsonb)', [JSON.stringify('123')])
    assert.deepEqual((await client.query('SELECT value FROM json_driver_probe')).rows, [{ value: '"123"' }])
    assert.deepEqual((await client.query('SELECT pg_backend_pid() AS pid')).rows, backend.rows)
    await client.query('ROLLBACK')
    assert.deepEqual((await client.query("SELECT to_regclass('pg_temp.json_driver_probe') AS relation")).rows, [{ relation: null }])

    const notification = new Promise<string>((resolve, reject) => {
      onNotification = (message) => { if (message.channel === channel) resolve(message.payload ?? '') }
      client.on('notification', onNotification)
      timer = setTimeout(() => reject(new Error('Native LISTEN notification did not arrive')), 3_000)
    })
    await client.query(`LISTEN "${channel}"`)
    await pool.query('SELECT pg_notify($1,$2)', [channel, 'native-listener'])
    assert.equal(await notification, 'native-listener')
  } finally {
    if (timer !== undefined) clearTimeout(timer)
    if (onNotification !== undefined) client.removeListener('notification', onNotification)
    try { await client.query('ROLLBACK'); await client.query('UNLISTEN *') }
    finally { client.release() }
  }
  await assert.rejects(view.query(`SELECT * FROM "missing_${randomUUID().replaceAll('-', '')}"`))
  assert.equal(pool.waitingCount, 0)
  assert.deepEqual((await pool.query('SELECT 1 AS value')).rows, [{ value: 1 }])
  console.log('PASS adapter JSON/JSONB versus SQL NULL, native application parsing, same-client transaction rollback, listener delegation and error cleanup')
} finally { await pool.end() }
