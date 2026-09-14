import type { Kysely } from 'kysely'
import type { OutboxAppendResult, OutboxEntry } from '../outbox/types.ts'

/** Query building/execution is available; transaction and resource ownership stay with MQ.
 * Derived Kysely instances share the same guarded driver and cannot acquire another client. */
export type KyselyOutboxDatabase<Database> = Omit<
  Kysely<Database>,
  'transaction' | 'startTransaction' | 'connection' | 'destroy' | typeof Symbol.asyncDispose
>

export interface KyselyOutboxTransaction<Database> {
  readonly db: KyselyOutboxDatabase<Database>
  append(entry: OutboxEntry): Promise<OutboxAppendResult>
}
export type KyselyOutboxCallback<Database, Value> = (
  transaction: KyselyOutboxTransaction<Database>
) => Value | PromiseLike<Value>

export interface KyselyOutboxClient<Database> {
  transaction<Value>(callback: KyselyOutboxCallback<Database, Value>): Promise<Value>
  transaction<Value>(
    entries: OutboxEntry | readonly OutboxEntry[],
    callback: KyselyOutboxCallback<Database, Value>
  ): Promise<Value>
}
