import type { OutboxAppendResult, OutboxEntry } from '../outbox/types.ts'

export type SqliteOutboxParameter = string | number | bigint | Uint8Array | null
export type SqliteOutboxCell = SqliteOutboxParameter
export type SqliteOutboxRow = Readonly<Record<string, SqliteOutboxCell>>

export interface SqliteOutboxRunResult {
  readonly changes: number
  readonly lastInsertRowid?: number | bigint
}

export interface SqliteOutboxTransaction {
  run(sql: string, parameters?: readonly SqliteOutboxParameter[]): SqliteOutboxRunResult
  get<Row extends object = SqliteOutboxRow>(
    sql: string,
    parameters?: readonly SqliteOutboxParameter[]
  ): Row | undefined
  all<Row extends object = SqliteOutboxRow>(
    sql: string,
    parameters?: readonly SqliteOutboxParameter[]
  ): readonly Row[]
  append(entry: OutboxEntry): Promise<OutboxAppendResult>
}

export type SqliteOutboxCallback<Value> = (
  transaction: SqliteOutboxTransaction
) => Value | PromiseLike<Value>

export interface SqliteOutboxClient {
  transaction<Value>(
    entries: OutboxEntry | readonly OutboxEntry[],
    callback: SqliteOutboxCallback<Value>
  ): Promise<Value>
}
