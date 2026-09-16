import { MqOutboxService, type PreparedJob } from '../../src/index.ts'
import {
  sqlite,
  sqliteOutbox,
  type SqliteOutboxTransaction
} from '../../src/integrations/sqlite-node.ts'

sqlite({ path: './jobs.db', outbox: true })

declare const service: MqOutboxService
declare const prepared: PreparedJob
const client = sqliteOutbox(service, 'primary')
void client.transaction({ id: 'one', job: prepared }, (transaction: SqliteOutboxTransaction) => {
  transaction.run('INSERT INTO x(value) VALUES (?)', ['value'])
  const row = transaction.get<{ value: string }>('SELECT value FROM x')
  transaction.all<{ value: string }>('SELECT value FROM x')
  void transaction.append({ id: 'two', job: prepared })
  return row?.value
})
void client.transaction({ id: 'bad', job: prepared }, (transaction) =>
  // @ts-expect-error SQLite bind parameters do not accept arbitrary JSON objects.
  transaction.run('SELECT ?', [{}])
)
