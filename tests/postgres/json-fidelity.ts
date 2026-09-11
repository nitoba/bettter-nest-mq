import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Test } from '@nestjs/testing'
import { Pool } from 'pg'
import { z } from 'zod'
import { Job, JobData, MqModule, Process, Queue, QueueService, Worker, type JobJsonValue } from '../../src/index.ts'
import { migratePostgres, postgres } from '../../src/integrations/postgres.ts'

@Queue({ name: 'json-fidelity', connection: 'primary' })
class JsonQueue extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
}

@Worker({ name: 'json-fidelity-worker', concurrency: 4, pollIntervalMs: 5 })
class JsonWorker {
  @Process(JsonQueue, 'echo')
  run(@JobData() value: JobJsonValue) { return value }
}

const cases = [
  'retry me', '', 'null', 'true', '123', '"quoted"', '{"nested":1}', '[1,2]',
  'ação 🌱\n"quoted"\\', 123, -4.5, 0, true, false, null, [], [1, 'x', null],
  { nested: ['null', null, false, 0], text: 'retry me' }
] satisfies z.input<ReturnType<typeof z.json>>[]

export async function verifyJsonFidelity(connectionString: string): Promise<void> {
  const schema = `mq_json_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 8 })
  const admin = new Pool({ connectionString, max: 2 })
  try {
    await migratePostgres({ pool: admin, schema })
    const observed = await pool.query<{ value: string; kind: string }>('SELECT $1::jsonb AS value,jsonb_typeof($1::jsonb) AS kind', [JSON.stringify('retry me')])
    assert.deepEqual(observed.rows, [{ value: 'retry me', kind: 'string' }])
    const app = await Test.createTestingModule({ imports: [
      MqModule.forRoot({ connections: { primary: postgres({ pool, schema }) } }),
      MqModule.forFeature([JsonQueue])
    ], providers: [JsonWorker] }).compile()
    const errors: Error[] = []
    try {
      await app.init()
      const queue = app.get(JsonQueue)
      for (const value of cases) {
        let phase = 'enqueue'
        try {
          const id = await queue.echo.enqueue(value)
          phase = 'worker-result'
          assert.deepEqual(await queue.echo.awaitResult(id, { timeoutMs: 1_500, pollIntervalMs: 5 }), value)
          phase = 'poll'
          assert.deepEqual((await queue.echo.poll(id))?.payload, value)
          assert.deepEqual((await queue.echo.poll(id))?.result, value)
          console.log('PASS JSON ROUND TRIP', JSON.stringify(value))
        } catch (cause) {
          const error = new Error(`JSON value ${JSON.stringify(value)} failed during ${phase}`, { cause })
          errors.push(error)
          console.error(error)
        }
      }
    } finally { await app.close() }
    // The pool is still borrowed and application-owned after MQ closes.
    assert.deepEqual((await pool.query('SELECT $1::jsonb AS value', [JSON.stringify('retry me')])).rows, [{ value: 'retry me' }])
    if (errors.length > 0) throw new AggregateError(errors, `${errors.length} JSON persistence regressions`)
    console.log(`PASS ${cases.length} PostgreSQL public JSON round trips`)
  } finally {
    // Drain native checked-out queries before dropping tables; DDL must not race a finishing
    // database transaction after cooperative runtime shutdown. The application owns this pool.
    try { await pool.end() }
    finally {
      try { await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`) }
      finally { await admin.end() }
    }
  }
}

const connectionString = process.env.MQ_TEST_DATABASE_URL
assert.ok(connectionString, 'MQ_TEST_DATABASE_URL must point to a dedicated PostgreSQL test database')
await verifyJsonFidelity(connectionString)
