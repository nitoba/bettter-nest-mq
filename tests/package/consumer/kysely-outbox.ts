import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { setTimeout as sleep } from 'node:timers/promises'
import { Module } from '@nestjs/common'
import { NestFactory } from '@nestjs/core'
import { Pool, types as pgTypes, type CustomTypesConfig } from 'pg'
import { sql, type Generated } from 'kysely'
import { z } from 'zod'
import {
  Job,
  JobData,
  MqModule,
  MqOutboxService,
  MqOutboxException,
  Process,
  Queue,
  QueueService,
  Worker,
  type JobJsonValue
} from 'better-nest-mq'
import { migratePostgres, postgres } from 'better-nest-mq/postgres'
import { kyselyOutbox } from 'better-nest-mq/kysely'

interface Database {
  business: {
    id: string
    note: string
    payload: JobJsonValue
    txid: Generated<string>
    pid: Generated<number>
  }
}
@Queue({ name: 'kysely-business', connection: 'primary' })
class BusinessQueue extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.json(), result: z.json() })
}
@Worker({ name: 'kysely-consumer', pollIntervalMs: 10 })
class BusinessWorker {
  @Process(BusinessQueue, 'echo')
  echo(@JobData() payload: JobJsonValue) {
    return payload
  }
}
function barrier() {
  let open = () => {
    throw new Error('Barrier not initialized')
  }
  const promise = new Promise<void>((resolve) => {
    open = resolve
  })
  return { promise, open: () => open() }
}

async function verify(databaseUrl: string): Promise<void> {
  const schema = `mq_kysely_${randomUUID().replaceAll('-', '')}`
  const admin = new Pool({ connectionString: databaseUrl, max: 5 })
  const getTypeParser: CustomTypesConfig['getTypeParser'] = (oid, format = 'text') => {
    if (format === 'binary') return pgTypes.getTypeParser(oid, 'binary')
    if (oid === 114 || oid === 3802) return (text: string) => ({ native: JSON.parse(text) })
    return pgTypes.getTypeParser(oid, 'text')
  }
  const pool = new Pool({ connectionString: databaseUrl, max: 8, types: { getTypeParser } })
  const connection = postgres({ pool, schema, namespace: 'kysely', outbox: true })
  @Module({
    imports: [
      MqModule.forRoot({
        connections: { primary: connection },
        execution: { workers: false, outboxPublisher: false }
      }),
      MqModule.forFeature([BusinessQueue])
    ]
  })
  class ProducerModule {}
  @Module({
    imports: [
      MqModule.forRoot({ connections: { primary: connection }, outbox: { pollIntervalMs: 10 } }),
      MqModule.forFeature([BusinessQueue])
    ],
    providers: [BusinessWorker]
  })
  class ProcessorModule {}
  const delivered: { id: string; payload: JobJsonValue }[] = []
  try {
    await migratePostgres({ pool: admin, schema })
    await admin.query(`CREATE TABLE "${schema}".business (
      id text PRIMARY KEY, note text NOT NULL, payload jsonb,
      txid text NOT NULL DEFAULT txid_current()::text,
      pid integer NOT NULL DEFAULT pg_backend_pid()
    )`)
    // A test-only trigger records the actual transaction/session used by append.
    // It does not implement queue operations or alter production transaction handling.
    await admin.query(`CREATE TABLE "${schema}".append_trace(id text PRIMARY KEY,pid integer,txid text);
      CREATE FUNCTION "${schema}".trace_append() RETURNS trigger LANGUAGE plpgsql AS $$
      BEGIN INSERT INTO "${schema}".append_trace VALUES(NEW.id,pg_backend_pid(),txid_current()::text); RETURN NEW; END $$;
      CREATE TRIGGER trace_append AFTER INSERT ON "${schema}".better_effect_mq_outbox
      FOR EACH ROW EXECUTE FUNCTION "${schema}".trace_append();`)
    const app = await NestFactory.createApplicationContext(ProducerModule, {
      logger: false,
      abortOnError: false
    })
    try {
      const service = app.get(MqOutboxService)
      const jobs = app.get(BusinessQueue)
      const client = kyselyOutbox<Database>(service, 'primary')
      const id = randomUUID()
      const payload = { requestId: id, marker: 'business' }
      const job = await jobs.echo.prepare(payload, { jobId: id })
      const entry = { id: `outbox:${id}`, job }
      const entered = barrier()
      const release = barrier()
      const transaction = client.transaction(async (tx) => {
        const db = tx.db.withSchema(schema)
        const row = await db
          .insertInto('business')
          .values({ id, note: 'created', payload })
          .returningAll()
          .executeTakeFirstOrThrow()
        const appended = await tx.append(entry)
        assert.equal(appended.duplicate, false)
        const identity = (
          await sql<{
            pid: number
            txid: string
          }>`select pg_backend_pid() as pid, txid_current()::text as txid`.execute(db)
        ).rows[0]
        const trace = (
          await sql<{
            pid: number
            txid: string
          }>`select pid, txid from ${sql.table(`${schema}.append_trace`)} where id=${entry.id}`.execute(
            db
          )
        ).rows[0]
        assert.deepEqual(trace, identity)
        assert.equal(row.pid, identity?.pid)
        assert.equal(row.txid, identity?.txid)
        assert.deepEqual(row.payload, { native: payload }, 'Business SQL keeps application parsers')
        entered.open()
        await release.promise
        return { tx, db, row }
      })
      try {
        await Promise.race([entered.promise, transaction])
        assert.equal(
          (await admin.query(`SELECT id FROM "${schema}".business WHERE id=$1`, [id])).rowCount,
          0
        )
        assert.equal(await service.get('primary', entry.id), undefined)
      } finally {
        release.open()
      }
      const saved = await transaction
      assert.equal(
        (await admin.query(`SELECT id FROM "${schema}".business WHERE id=$1`, [id])).rowCount,
        1
      )
      assert.equal((await service.get('primary', entry.id))?.state, 'pending')
      assert.deepEqual(
        (await service.get('primary', entry.id))?.request.payload,
        payload,
        'Adapter JSON stays independent of application parsers'
      )
      await assert.rejects(saved.db.selectFrom('business').selectAll().execute(), MqOutboxException)
      await assert.rejects(saved.tx.append(entry), MqOutboxException)
      delivered.push({ id, payload })
      console.log(
        'PASS Kysely business query and outbox append share the exact native transaction/backend; uncommitted writes remain invisible'
      )

      for (const mode of ['callback', 'query', 'append', 'nested'] as const) {
        const failedId = randomUUID()
        const failedEntry = {
          id: `failed:${failedId}`,
          job: await jobs.echo.prepare({ failedId }, { jobId: failedId })
        }
        let calls = 0
        await assert.rejects(
          client.transaction(failedEntry, async (tx) => {
            calls += 1
            const db = tx.db.withSchema(schema)
            await db
              .insertInto('business')
              .values({ id: failedId, note: mode, payload: null })
              .execute()
            if (mode === 'callback') throw new Error('Intentional business failure')
            if (mode === 'query')
              await assert.rejects(
                db
                  .insertInto('business')
                  .values({ id: failedId, note: mode, payload: null })
                  .execute()
              )
            if (mode === 'append') await assert.rejects(tx.append({ ...failedEntry, id: '' }))
            if (mode === 'nested')
              await assert.rejects(
                db.transaction().execute(async () => 1),
                MqOutboxException
              )
          })
        )
        assert.equal(calls, 1, 'Business callbacks must not replay automatically')
        assert.equal(
          (await admin.query(`SELECT id FROM "${schema}".business WHERE id=$1`, [failedId]))
            .rowCount,
          0
        )
        assert.equal(await service.get('primary', failedEntry.id), undefined)
      }
      console.log(
        'PASS callback, caught SQL/append failure and rejected nested transaction all roll back domain and outbox'
      )

      // Predeclared batch entries retain the existing outbox validation/append protocol.
      const batch = await Promise.all(
        [null, '123'].map(async (value) => {
          const jobId = randomUUID()
          delivered.push({ id: jobId, payload: value })
          return { id: `batch:${jobId}`, job: await jobs.echo.prepare(value, { jobId }) }
        })
      )
      const updated = await client.transaction(batch, async ({ db }) => {
        const result = await db
          .withSchema(schema)
          .updateTable('business')
          .set({ note: 'updated' })
          .where('id', '=', id)
          .executeTakeFirst()
        assert.equal(result.numUpdatedRows, 1n)
        return result.numUpdatedRows
      })
      assert.equal(updated, 1n)
      await assert.rejects(
        kyselyOutbox<Database>(service, 'missing').transaction(() => 1),
        MqOutboxException
      )
      const duplicate = await client.transaction(async (tx) => (await tx.append(entry)).duplicate)
      assert.equal(duplicate, true)
    } finally {
      await app.close()
    }

    const processor = await NestFactory.createApplicationContext(ProcessorModule, {
      logger: false,
      abortOnError: false
    })
    try {
      for (const item of delivered)
        assert.deepEqual(
          await processor
            .get(BusinessQueue)
            .echo.awaitResult(item.id, { timeoutMs: 10_000, pollIntervalMs: 10 }),
          item.payload
        )
      const deadline = Date.now() + 10_000
      while (
        (await processor.get(MqOutboxService).counts('primary')).published !== delivered.length
      ) {
        assert.ok(Date.now() < deadline)
        await sleep(10)
      }
    } finally {
      await processor.close()
    }
    assert.equal((await pool.query<{ value: number }>('SELECT 1 AS value')).rows[0]?.value, 1)
    console.log(
      'PASS Kysely batch append and scalar/null jobs publish/process after producer restart; borrowed pool remains open'
    )
  } finally {
    try {
      await pool.end()
    } finally {
      try {
        await admin.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      } finally {
        await admin.end()
      }
    }
  }
}

const database = process.env.MQ_TEST_DATABASE_URL
if (database !== undefined) await verify(database)
else
  console.log('Packed Kysely types passed; transaction identity and rollback run in PostgreSQL CI')
