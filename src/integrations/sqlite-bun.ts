import type { Database } from 'bun:sqlite'
import type { MqConnection } from '../connections/connection.ts'
import {
  sqliteConnection,
  migrateSqliteDatabase,
  validateSqliteDatabase
} from './sqlite-connection.ts'
import type { SqliteHost } from './sqlite-connection.ts'
import type { SqliteLocation, SqliteOptions, SqliteMigrationReport } from './sqlite.types.ts'

export type SqliteConnectionOptions = SqliteOptions<Database>
export type SqliteMigrationOptions = SqliteLocation<Database>
export type { SqliteMigrationReport } from './sqlite.types.ts'
const bunHost: SqliteHost<Database> = {
  async open(path) {
    const { Database: SqliteDatabase } = await import('bun:sqlite')
    return new SqliteDatabase(path, { strict: true })
  },
  close(database) {
    database.close(true)
  }
}
export function sqlite(options: SqliteConnectionOptions): MqConnection {
  return sqliteConnection(options, bunHost)
}
export function migrateSqlite(options: SqliteMigrationOptions): Promise<SqliteMigrationReport> {
  return migrateSqliteDatabase(options, bunHost)
}
export function validateSqlite(options: SqliteMigrationOptions): Promise<void> {
  return validateSqliteDatabase(options, bunHost)
}
