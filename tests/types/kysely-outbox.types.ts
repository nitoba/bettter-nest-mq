import type { KyselyOutboxTransaction } from '../../src/integrations/kysely-outbox.types.ts'

type Database = { requests: { id: string; count: number } }
export async function check(tx: KyselyOutboxTransaction<Database>): Promise<number> {
  const row = await tx.db
    .insertInto('requests')
    .values({ id: 'request', count: 1 })
    .returning('count')
    .executeTakeFirstOrThrow()
  // @ts-expect-error The database type rejects unknown tables.
  tx.db.selectFrom('unknown_table')
  // @ts-expect-error SQL parameters retain the column type.
  tx.db.insertInto('requests').values({ id: 'request', count: 'wrong' })
  // @ts-expect-error The caller does not own transaction lifecycle.
  tx.db.transaction()
  // @ts-expect-error The caller does not own a pool to destroy.
  tx.db.destroy()
  return row.count
}
