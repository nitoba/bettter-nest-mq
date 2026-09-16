import { SqliteOutboxTransactions } from 'better-effect-mq-sqlite'
import type { SqliteDatabase } from 'better-effect-mq-sqlite'
import type { OutboxRecord } from 'better-effect-mq-outbox'
import { Result } from 'better-result'
import type { PrepareOutboxEntry } from '../engine/outbox-coordinator.ts'
import { assertSameOutboxContent, outboxSnapshot } from '../engine/outbox-record.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { OutboxAppendResult, OutboxEntry } from '../outbox/types.ts'
import type { NativeSqliteOutbox } from './sqlite-outbox-resource.ts'
import type {
  SqliteOutboxCallback,
  SqliteOutboxParameter,
  SqliteOutboxRunResult,
  SqliteOutboxTransaction
} from './sqlite-outbox.types.ts'

function asError<Cause>(cause: Cause): Error {
  return cause instanceof Error
    ? cause
    : new MqOutboxException('transaction', 'The SQLite outbox transaction failed', { cause })
}

function closedTransaction(): MqOutboxException {
  return new MqOutboxException('transaction', 'This SQLite outbox transaction callback has finished')
}

function assertApplicationSql(sql: string): void {
  if (/^\s*(?:BEGIN|COMMIT|ROLLBACK|END|SAVEPOINT|RELEASE)\b/iu.test(sql))
    throw new MqOutboxException(
      'configuration',
      'Manual transaction control is not supported inside a managed SQLite outbox transaction'
    )
}

function appendPrepared(
  database: SqliteDatabase,
  namespace: string,
  source: string,
  record: OutboxRecord
): OutboxAppendResult {
  const appended = SqliteOutboxTransactions.appendIn(database, record, { namespace })
  if (Result.isError(appended))
    throw new MqOutboxException('append', 'The SQLite outbox record could not be appended', {
      cause: appended.error
    })
  if (appended.value.duplicate) assertSameOutboxContent(appended.value.record, record)
  return Object.freeze({
    record: outboxSnapshot(source, appended.value.record),
    duplicate: appended.value.duplicate
  })
}

class TransactionScope implements SqliteOutboxTransaction {
  private accepting = true
  private failure: Error | undefined
  private readonly pending = new Set<Promise<void>>()

  constructor(
    private readonly database: SqliteDatabase,
    private readonly namespace: string,
    private readonly source: string,
    private readonly prepare: PrepareOutboxEntry
  ) {}

  poison(cause: Error): void {
    this.failure ??= cause
  }

  private ensureOpen(): void {
    if (!this.accepting) throw closedTransaction()
  }

  private execute<Value>(operation: () => Value): Value {
    this.ensureOpen()
    try {
      return operation()
    } catch (cause) {
      this.poison(asError(cause))
      throw cause
    }
  }

  private executeAsync<Value>(operation: () => Promise<Value>): Promise<Value> {
    try {
      this.ensureOpen()
    } catch (cause) {
      return Promise.reject(cause)
    }
    const task = Promise.resolve().then(operation)
    const completed = task.then(
      () => {
        this.pending.delete(completed)
      },
      (cause) => {
        this.poison(asError(cause))
        this.pending.delete(completed)
      }
    )
    this.pending.add(completed)
    return task
  }

  run(sql: string, parameters: readonly SqliteOutboxParameter[] = []): SqliteOutboxRunResult {
    return this.execute(() => {
      assertApplicationSql(sql)
      return this.database.prepare(sql).run(...parameters)
    })
  }

  get<Row extends object = import('./sqlite-outbox.types.ts').SqliteOutboxRow>(
    sql: string,
    parameters: readonly SqliteOutboxParameter[] = []
  ): Row | undefined {
    return this.execute(() => {
      assertApplicationSql(sql)
      const row = this.database.prepare(sql).get(...parameters)
      // SAFETY: row shape is explicitly selected by trusted application SQL; the generic mirrors
      // pg's caller-declared result row and this facade does not fabricate or transform columns.
      return (row ?? undefined) as Row | undefined
    })
  }

  all<Row extends object = import('./sqlite-outbox.types.ts').SqliteOutboxRow>(
    sql: string,
    parameters: readonly SqliteOutboxParameter[] = []
  ): readonly Row[] {
    return this.execute(() => {
      assertApplicationSql(sql)
      const rows = this.database.prepare(sql).all(...parameters)
      // SAFETY: row shapes are selected by trusted application SQL; values are returned unchanged.
      return rows as readonly Row[]
    })
  }

  append(entry: OutboxEntry): Promise<OutboxAppendResult> {
    return this.executeAsync(async () =>
      appendPrepared(this.database, this.namespace, this.source, await this.prepare(entry))
    )
  }

  async finish(): Promise<void> {
    this.accepting = false
    await Promise.all(this.pending)
    if (this.failure !== undefined) throw this.failure
  }
}

/** The adapter owns BEGIN IMMEDIATE/COMMIT/ROLLBACK and serializes transactions per native handle. */
export async function sqliteOutboxTransaction<Value>(
  resource: NativeSqliteOutbox,
  source: string,
  records: readonly OutboxRecord[],
  prepare: PrepareOutboxEntry,
  callback: SqliteOutboxCallback<Value>
): Promise<Value> {
  const last = records.at(-1)
  if (last === undefined)
    throw new MqOutboxException(
      'configuration',
      'A SQLite outbox transaction requires at least one prepared outbox entry'
    )
  const beforeLast = records.slice(0, -1)
  const transaction = await SqliteOutboxTransactions.transaction(
    resource.database,
    last,
    async (database) => {
      const scope = new TransactionScope(database, resource.namespace, source, prepare)
      let value: Value | undefined
      let primary: Error | undefined
      try {
        value = await callback(scope)
      } catch (cause) {
        primary = asError(cause)
        scope.poison(primary)
      }
      try {
        await scope.finish()
      } catch (cause) {
        primary ??= asError(cause)
      }
      if (primary !== undefined) throw primary
      for (const record of beforeLast)
        appendPrepared(database, resource.namespace, source, record)
      return Result.ok(value)
    },
    { namespace: resource.namespace }
  )
  if (Result.isError(transaction))
    throw new MqOutboxException('transaction', 'The SQLite outbox transaction rolled back', {
      cause: transaction.error
    })
  // SAFETY: a successful callback assigned its result, including a legitimate undefined.
  return transaction.value as Value
}
