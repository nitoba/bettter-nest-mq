import type { QueryResult, QueryResultRow } from 'pg'
import type { JobJsonValue } from '../jobs/types.ts'
import type { OutboxAppendResult, OutboxEntry } from '../outbox/types.ts'

export type PostgresOutboxParameter = JobJsonValue | Date | Uint8Array
export interface PostgresOutboxTransaction {
  query<Row extends QueryResultRow = QueryResultRow>(
    text: string,
    values?: readonly PostgresOutboxParameter[]
  ): Promise<QueryResult<Row>>
  append(entry: OutboxEntry): Promise<OutboxAppendResult>
}
export type PostgresOutboxCallback<Value> = (
  transaction: PostgresOutboxTransaction
) => Value | PromiseLike<Value>
export interface PostgresOutboxClient {
  transaction<Value>(callback: PostgresOutboxCallback<Value>): Promise<Value>
  transaction<Value>(
    entries: OutboxEntry | readonly OutboxEntry[],
    callback: PostgresOutboxCallback<Value>
  ): Promise<Value>
}
