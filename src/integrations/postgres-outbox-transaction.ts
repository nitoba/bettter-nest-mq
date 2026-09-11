import type { Pool, PoolClient, QueryResult, QueryResultRow } from 'pg'
import type { PostgresOutboxStore } from 'better-effect-mq-postgres'
import type { OutboxRecord } from 'better-effect-mq-outbox'
import type { JobJsonValue } from '../jobs/types.ts'
import type { PrepareOutboxEntry } from '../engine/outbox-coordinator.ts'
import { assertSameOutboxContent, outboxSnapshot } from '../engine/outbox-record.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { OutboxAppendResult, OutboxEntry } from '../outbox/types.ts'
import { postgresJsonClient } from './postgres-json-pool.ts'

export type PostgresOutboxParameter = JobJsonValue | Date | Uint8Array
export interface PostgresOutboxTransaction {
  query<Row extends QueryResultRow = QueryResultRow>(text: string, values?: readonly PostgresOutboxParameter[]): Promise<QueryResult<Row>>
  append(entry: OutboxEntry): Promise<OutboxAppendResult>
}
export type PostgresOutboxCallback<Value> = (transaction: PostgresOutboxTransaction) => Value | PromiseLike<Value>

function asError<Cause>(cause: Cause): Error {
  return cause instanceof Error ? cause : new MqOutboxException('transaction', 'The transaction failed', { cause })
}

/** Every operation belongs to this callback's actual client. Caught failures still poison
 * commit, and unawaited but already-admitted operations are drained before releasing it. */
class TransactionScope implements PostgresOutboxTransaction {
  private accepting = true
  private readonly pending = new Set<Promise<void>>()
  private failure: Error | undefined
  constructor(private readonly client: PoolClient, private readonly store: PostgresOutboxStore, private readonly source: string, private readonly prepare: PrepareOutboxEntry) {}

  poison(cause: Error): void { this.failure ??= cause }

  private execute<Value>(operation: () => Promise<Value>): Promise<Value> {
    if (!this.accepting) return Promise.reject(new MqOutboxException('transaction', 'This outbox transaction callback has finished'))
    const task = Promise.resolve().then(operation)
    const completed = task.then(
      () => { this.pending.delete(completed) },
      (cause) => { this.poison(asError(cause)); this.pending.delete(completed) }
    )
    this.pending.add(completed)
    return task
  }

  query<Row extends QueryResultRow = QueryResultRow>(text: string, values?: readonly PostgresOutboxParameter[]): Promise<QueryResult<Row>> {
    return this.execute(() => values === undefined ? this.client.query<Row>(text) : this.client.query<Row>(text, [...values]))
  }

  append(entry: OutboxEntry): Promise<OutboxAppendResult> {
    return this.execute(async () => this.appendDirect(await this.prepare(entry)))
  }

  appendPrepared(record: OutboxRecord): Promise<OutboxAppendResult> {
    return this.execute(() => this.appendDirect(record))
  }

  private async appendDirect(record: OutboxRecord): Promise<OutboxAppendResult> {
    try {
      // Only adapter SQL sees JSON wire text; business queries above keep native parsers.
      const appended = await this.store.appendIn(postgresJsonClient(this.client), record)
      if (appended.duplicate) assertSameOutboxContent(appended.record, record)
      return Object.freeze({ record: outboxSnapshot(this.source, appended.record), duplicate: appended.duplicate })
    } catch (cause) {
      if (cause instanceof MqOutboxException) throw cause
      throw new MqOutboxException('append', 'The outbox record could not be appended', { cause })
    }
  }

  async finish(): Promise<void> {
    this.accepting = false
    await Promise.all([...this.pending])
    if (this.failure !== undefined) throw this.failure
  }
}

/** No retry of the business callback, including uncertain commit responses. */
export async function postgresOutboxTransaction<Value>(pool: Pool, store: PostgresOutboxStore, source: string, records: readonly OutboxRecord[], prepare: PrepareOutboxEntry, callback: PostgresOutboxCallback<Value>): Promise<Value> {
  const client = await pool.connect()
  const scope = new TransactionScope(client, store, source, prepare)
  let primary: Error | undefined
  let discard: Error | undefined
  let value: Value | undefined
  let committed = false
  let commitAttempted = false
  const onError = (error: Error): void => { discard = error; scope.poison(error) }
  client.on('error', onError)
  try {
    await client.query('BEGIN')
    try {
      value = await callback(scope)
      for (const record of records) await scope.appendPrepared(record)
    } catch (cause) { scope.poison(asError(cause)) }
    await scope.finish()
    commitAttempted = true
    await client.query('COMMIT')
    committed = true
  } catch (cause) {
    primary = asError(cause)
    if (commitAttempted) discard = primary
    // Drain previously admitted work even if BEGIN/callback failed. finish does no writes.
    try { await scope.finish() } catch (failure) { primary ??= asError(failure) }
  }
  const cleanup: Error[] = []
  if (!committed) {
    try { await client.query('ROLLBACK') }
    catch (cause) { discard = asError(cause); cleanup.push(discard) }
  }
  client.removeListener('error', onError)
  try { client.release(discard) } catch (cause) { cleanup.push(asError(cause)) }
  if (cleanup.length > 0) throw new AggregateError(primary === undefined ? cleanup : [primary, ...cleanup], 'Outbox transaction cleanup failed', { cause: primary })
  if (primary !== undefined) throw primary
  if (!committed) throw new MqOutboxException('transaction', 'The transaction did not commit')
  // SAFETY: a successful callback assigned its generic result, including a legitimate undefined.
  return value as Value
}
