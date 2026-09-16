import { Database } from 'bun:sqlite'
import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Injectable } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import {
  Job,
  MqModule,
  MqOutboxException,
  MqOutboxService,
  Queue,
  QueueService
} from '../../src/index.ts'
import { migrateSqlite, sqlite, sqliteOutbox } from '../../src/integrations/sqlite-bun.ts'
import type { SqliteOutboxTransaction } from '../../src/integrations/sqlite-outbox.types.ts'

@Injectable()
@Queue({ name: 'sqlite-outbox', connection: 'primary' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.string(), result: z.string() })
}

async function application(database: Database, enabled = true) {
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: sqlite({ database, namespace: 'outbox', outbox: enabled }) },
        execution: { workers: false, outboxPublisher: false }
      }),
      MqModule.forFeature([Jobs])
    ]
  }).compile()
  await app.init()
  return app
}

test('SQLite domain writes and predeclared/dynamic outbox entries commit atomically', async () => {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  database.exec('CREATE TABLE domain_items (id TEXT PRIMARY KEY, value TEXT NOT NULL)')
  const app = await application(database)
  let escaped: SqliteOutboxTransaction | undefined
  try {
    const jobs = app.get(Jobs)
    const outboxes = app.get(MqOutboxService)
    const client = sqliteOutbox(outboxes, 'primary')
    const first = await jobs.echo.prepare('first')
    const second = await jobs.echo.prepare('second')
    const dynamic = await jobs.echo.prepare('dynamic')
    const value = await client.transaction(
      [
        { id: 'first', job: first },
        { id: 'second', job: second }
      ],
      async (transaction) => {
        escaped = transaction
        expect(transaction.run('INSERT INTO domain_items (id,value) VALUES (?,?)', ['a', 'one']).changes).toBe(1)
        transaction.run('INSERT INTO domain_items (id,value) VALUES (?,?)', ['b', 'two'])
        expect(
          transaction.get<{ id: string; value: string }>(
            'SELECT id,value FROM domain_items WHERE id=?',
            ['a']
          )
        ).toEqual({ id: 'a', value: 'one' })
        void transaction.append({ id: 'dynamic', job: dynamic })
        return 42
      }
    )
    expect(value).toBe(42)
    expect(database.prepare('SELECT id,value FROM domain_items ORDER BY id').all()).toEqual([
      { id: 'a', value: 'one' },
      { id: 'b', value: 'two' }
    ])
    expect(await outboxes.counts('primary')).toMatchObject({ pending: 3, total: 3 })
    assert.ok(escaped)
    expect(() => escaped.run('SELECT 1')).toThrow(MqOutboxException)
    await assert.rejects(escaped.append({ id: 'closed', job: first }), MqOutboxException)
  } finally {
    await app.close()
    database.close(true)
  }
})

test('caught SQLite SQL failures poison commit and roll back both domain and outbox writes', async () => {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  database.exec("CREATE TABLE domain_items (id TEXT PRIMARY KEY); INSERT INTO domain_items VALUES ('seed')")
  const app = await application(database)
  try {
    const jobs = app.get(Jobs)
    const outboxes = app.get(MqOutboxService)
    const prepared = await jobs.echo.prepare('rollback')
    await assert.rejects(
      sqliteOutbox(outboxes, 'primary').transaction({ id: 'rollback', job: prepared }, (transaction) => {
        transaction.run("INSERT INTO domain_items VALUES ('temporary')")
        try {
          transaction.run("INSERT INTO domain_items VALUES ('seed')")
        } catch {
          // The transaction scope must still remember the native failure.
        }
        return 'ignored'
      })
    )
    expect(database.prepare("SELECT id FROM domain_items WHERE id='temporary'").get()).toBeNull()
    expect(await outboxes.get('primary', 'rollback')).toBeUndefined()
  } finally {
    await app.close()
    database.close(true)
  }
})

test('SQLite transaction control cannot escape the managed native boundary', async () => {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  const app = await application(database)
  try {
    const jobs = app.get(Jobs)
    const outboxes = app.get(MqOutboxService)
    const prepared = await jobs.echo.prepare('guarded')
    await assert.rejects(
      sqliteOutbox(outboxes, 'primary').transaction({ id: 'guarded', job: prepared }, (transaction) => {
        try {
          transaction.run('COMMIT')
        } catch {
          // Caught misuse must still poison the managed transaction.
        }
        return undefined
      }),
      MqOutboxException
    )
    expect(await outboxes.get('primary', 'guarded')).toBeUndefined()
  } finally {
    await app.close()
    database.close(true)
  }
})

test('SQLite outbox requires opt-in and at least one prepared record', async () => {
  const database = new Database(':memory:')
  await migrateSqlite({ database })
  const disabled = await application(database, false)
  try {
    const jobs = disabled.get(Jobs)
    const service = disabled.get(MqOutboxService)
    const prepared = await jobs.echo.prepare('no source')
    await assert.rejects(
      sqliteOutbox(service, 'primary').transaction({ id: 'missing', job: prepared }, () => 'no'),
      MqOutboxException
    )
    await assert.rejects(
      sqliteOutbox(service, 'primary').transaction([], () => 'no'),
      MqOutboxException
    )
  } finally {
    await disabled.close()
    database.close(true)
  }
})
