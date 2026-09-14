import { Kysely, PostgresAdapter, PostgresIntrospector, PostgresQueryCompiler } from 'kysely'
import type { CompiledQuery, DatabaseConnection, Driver, QueryResult } from 'kysely'
import type { QueryResultRow } from 'pg'
import { MqOutboxException } from '../outbox/errors.ts'
import type { OutboxAppendResult, OutboxEntry } from '../outbox/types.ts'
import type { PostgresOutboxParameter, PostgresOutboxTransaction } from './postgres-outbox.types.ts'
import type { KyselyOutboxCallback, KyselyOutboxTransaction } from './kysely-outbox.types.ts'

function closed(): MqOutboxException {
  return new MqOutboxException(
    'transaction',
    'This Kysely outbox transaction callback has finished'
  )
}

/** A driver view, not a pool: every statement uses the existing managed transaction.
 * Track SQL completion independently of the caller's promise/cancellation handling. */
class ScopedDriver implements Driver, DatabaseConnection {
  private accepting = true
  private failure: Error | undefined
  private readonly pending = new Set<Promise<void>>()

  constructor(private readonly transaction: PostgresOutboxTransaction) {}

  poison<Cause>(cause: Cause): void {
    this.failure ??=
      cause instanceof Error
        ? cause
        : new MqOutboxException('transaction', 'Kysely outbox callback failed', { cause })
  }

  private unsupported(operation: string): MqOutboxException {
    const error = new MqOutboxException(
      'transaction',
      `Kysely ${operation} is not supported inside the managed outbox transaction`
    )
    if (this.accepting) this.poison(error)
    return error
  }

  private admit<Value>(operation: () => Promise<Value>): Promise<Value> {
    if (!this.accepting) return Promise.reject(closed())
    const task = Promise.resolve().then(operation)
    const drained = task.then(
      () => {
        this.pending.delete(drained)
      },
      (cause) => {
        this.poison(cause)
        this.pending.delete(drained)
      }
    )
    this.pending.add(drained)
    return task
  }

  init(): Promise<void> {
    return this.accepting ? Promise.resolve() : Promise.reject(closed())
  }
  acquireConnection(): Promise<DatabaseConnection> {
    return this.accepting ? Promise.resolve(this) : Promise.reject(closed())
  }
  releaseConnection(): Promise<void> {
    // Kysely owns no native resource here. Only postgresOutbox may release the client.
    return Promise.resolve()
  }
  beginTransaction(): Promise<void> {
    return Promise.reject(this.unsupported('nested transactions'))
  }
  commitTransaction(): Promise<void> {
    return Promise.reject(this.unsupported('manual commit'))
  }
  rollbackTransaction(): Promise<void> {
    return Promise.reject(this.unsupported('manual rollback'))
  }
  destroy(): Promise<void> {
    return Promise.reject(this.unsupported('driver destruction'))
  }
  streamQuery<Row>(): AsyncIterableIterator<QueryResult<Row>> {
    throw this.unsupported('streaming')
  }

  executeQuery<Row>(query: CompiledQuery): Promise<QueryResult<Row>> {
    return this.admit(async () => {
      // SAFETY: compiled parameters are trusted application query inputs and are
      // passed unchanged to pg, the same native parameter boundary as tx.query.
      // This is not a schema validation claim or a JSON conversion of SQL values.
      const parameters = query.parameters as readonly PostgresOutboxParameter[]
      const result = await this.transaction.query<Row & QueryResultRow>(query.sql, parameters)
      if (
        result.command === 'INSERT' ||
        result.command === 'UPDATE' ||
        result.command === 'DELETE' ||
        result.command === 'MERGE'
      ) {
        return { rows: result.rows, numAffectedRows: BigInt(result.rowCount ?? 0) }
      }
      return { rows: result.rows }
    })
  }
  append(entry: OutboxEntry): Promise<OutboxAppendResult> {
    return this.admit(() => this.transaction.append(entry))
  }
  async finish(): Promise<void> {
    this.accepting = false
    await Promise.all(this.pending)
    if (this.failure !== undefined) throw this.failure
  }
}

/** Native commit/rollback remain outside this callback in postgresOutboxTransaction. */
export async function runKyselyOutbox<Database, Value>(
  transaction: PostgresOutboxTransaction,
  callback: KyselyOutboxCallback<Database, Value>
): Promise<Value> {
  const driver = new ScopedDriver(transaction)
  const db = new Kysely<Database>({
    dialect: {
      createDriver: () => driver,
      createAdapter: () => new PostgresAdapter(),
      createQueryCompiler: () => new PostgresQueryCompiler(),
      createIntrospector: (database) => new PostgresIntrospector(database)
    }
  })
  const view: KyselyOutboxTransaction<Database> = Object.freeze({
    db,
    append: (entry: OutboxEntry) => driver.append(entry)
  })
  let value: Value | undefined
  try {
    // Kysely skips destroy() on a never-initialized driver. Initialize its public
    // connection wrapper without issuing SQL or acquiring another native client,
    // so even a pre-query lifecycle misuse reaches the guarded driver.
    await db.connection().execute(async () => undefined)
    value = await callback(view)
  } catch (cause) {
    driver.poison(cause)
  }
  await driver.finish()
  // SAFETY: finish rejects on callback failure; otherwise the callback assigned
  // its actual result, including an intentionally undefined return value.
  return value as Value
}
