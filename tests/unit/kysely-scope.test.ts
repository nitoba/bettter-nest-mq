import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { sql } from 'kysely'
import type { Kysely } from 'kysely'
import type { QueryResult, QueryResultRow } from 'pg'
import { MqOutboxException } from '../../src/outbox/errors.ts'
import { runKyselyOutbox } from '../../src/integrations/kysely-outbox-scope.ts'
import type { KyselyOutboxTransaction } from '../../src/integrations/kysely-outbox.types.ts'
import type {
  PostgresOutboxParameter,
  PostgresOutboxTransaction
} from '../../src/integrations/postgres-outbox.types.ts'

type Database = { items: { id: string; value: string } }

function fixture() {
  const queries: { text: string; parameters: readonly PostgresOutboxParameter[] | undefined }[] = []
  const entered = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  const failure = new Error('Native query failed')
  let block = false
  let fail = false
  const tx: PostgresOutboxTransaction = {
    async query<Row extends QueryResultRow>(
      text: string,
      parameters?: readonly PostgresOutboxParameter[]
    ): Promise<QueryResult<Row>> {
      queries.push({ text, parameters })
      entered.resolve()
      if (block) await release.promise
      if (fail) throw failure
      return {
        rows: [],
        rowCount: 3,
        command: text.startsWith('insert')
          ? 'INSERT'
          : text.startsWith('update')
            ? 'UPDATE'
            : text.startsWith('delete')
              ? 'DELETE'
              : 'SELECT',
        oid: 0,
        fields: []
      }
    },
    append() {
      return Promise.reject(failure)
    }
  }
  return {
    tx,
    queries,
    entered: entered.promise,
    release,
    failure,
    block: () => {
      block = true
    },
    fail: () => {
      fail = true
    }
  }
}

test('real Kysely compiles parameterized PostgreSQL and preserves native parameter values', async () => {
  const f = fixture()
  const date = new Date('2026-09-14T12:00:00Z')
  await runKyselyOutbox<Database, void>(f.tx, async ({ db }) => {
    await db.insertInto('items').values({ id: 'a', value: 'SQL is not interpolated' }).execute()
    await sql`select ${date}::timestamptz`.execute(db)
  })
  expect(f.queries[0]).toEqual({
    text: 'insert into "items" ("id", "value") values ($1, $2)',
    parameters: ['a', 'SQL is not interpolated']
  })
  expect(f.queries[1]?.parameters?.[0]).toBe(date)
})

test('Kysely mutation results retain affected-row bigint semantics', async () => {
  await runKyselyOutbox<Database, void>(fixture().tx, async ({ db }) => {
    expect(
      (await db.insertInto('items').values({ id: 'a', value: 'b' }).executeTakeFirst())
        .numInsertedOrUpdatedRows
    ).toBe(3n)
    expect(
      (await db.updateTable('items').set({ value: 'b' }).executeTakeFirst()).numUpdatedRows
    ).toBe(3n)
    expect((await db.deleteFrom('items').executeTakeFirst()).numDeletedRows).toBe(3n)
  })
})

test('a caught native driver failure still rejects the managed callback', async () => {
  const f = fixture()
  f.fail()
  await assert.rejects(
    runKyselyOutbox<Database, void>(f.tx, async ({ db }) => {
      await assert.rejects(
        db.selectFrom('items').selectAll().execute(),
        (cause) => cause === f.failure
      )
    }),
    (cause) => cause === f.failure
  )
})

test('escaped builders and derived database instances cannot query after callback completion', async () => {
  const f = fixture()
  const saved = await runKyselyOutbox<
    Database,
    { db: Kysely<Database>; query: ReturnType<Kysely<Database>['selectNoFrom']> }
  >(f.tx, ({ db }) => ({
    db: db.withSchema('public'),
    query: db.selectNoFrom(sql<number>`1`.as('value'))
  }))
  await assert.rejects(saved.db.selectFrom('items').selectAll().execute(), MqOutboxException)
  await assert.rejects(saved.query.execute(), MqOutboxException)
  expect(f.queries).toHaveLength(0)
})

test('driver-admitted SQL drains before callback completion even when not awaited by the callback', async () => {
  const f = fixture()
  f.block()
  let ended = false
  const scope = runKyselyOutbox<Database, void>(f.tx, async ({ db }) => {
    void db.selectFrom('items').selectAll().execute()
    await f.entered
  }).then(() => {
    ended = true
  })
  await f.entered
  await Promise.resolve()
  expect(ended).toBe(false)
  f.release.resolve()
  await scope
  expect(ended).toBe(true)
})

test.each(['transaction', 'destroy', 'stream'] as const)(
  'unsupported %s poisons the callback without issuing transaction SQL',
  async (operation) => {
    const f = fixture()
    await assert.rejects(
      runKyselyOutbox<Database, void>(f.tx, async ({ db }) => {
        const derived = db.withSchema('public')
        if (operation === 'transaction')
          await assert.rejects(
            derived.transaction().execute(async () => 1),
            MqOutboxException
          )
        else if (operation === 'destroy') await assert.rejects(derived.destroy(), MqOutboxException)
        else
          await assert.rejects(
            derived.selectFrom('items').selectAll().stream().next(),
            MqOutboxException
          )
      }),
      MqOutboxException
    )
    expect(f.queries).toHaveLength(0)
  }
)

test('escaped append also rejects and a synchronous callback error keeps its identity', async () => {
  const f = fixture()
  const callbackFailure = new Error('business failure')
  await assert.rejects(
    runKyselyOutbox<Database, void>(f.tx, () => {
      throw callbackFailure
    }),
    (cause) => cause === callbackFailure
  )
  const saved = await runKyselyOutbox<Database, KyselyOutboxTransaction<Database>>(f.tx, (tx) => tx)
  await assert.rejects(saved.append(JSON.parse('{}')), MqOutboxException)
})
