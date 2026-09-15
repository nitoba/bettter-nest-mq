import type { DatabaseSync } from 'node:sqlite'
import type { MqConnection } from '../connections/connection.ts'
import {
  sqliteConnection,
  migrateSqliteDatabase,
  validateSqliteDatabase
} from './sqlite-connection.ts'
import type { SqliteHost } from './sqlite-connection.ts'
import type { SqliteLocation, SqliteOptions, SqliteMigrationReport } from './sqlite.types.ts'

export type SqliteConnectionOptions = SqliteOptions<DatabaseSync>
export type SqliteMigrationOptions = SqliteLocation<DatabaseSync>
export type { SqliteMigrationReport } from './sqlite.types.ts'
const nodeHost: SqliteHost<DatabaseSync> = {
  async open(path) {
    const { DatabaseSync: Database } = await import('node:sqlite')
    return new Database(path)
  },
  close(database) {
    database.close()
  }
}
export function sqlite(options: SqliteConnectionOptions): MqConnection {
  return sqliteConnection(options, nodeHost)
}
export function migrateSqlite(options: SqliteMigrationOptions): Promise<SqliteMigrationReport> {
  return migrateSqliteDatabase(options, nodeHost)
}
export function validateSqlite(options: SqliteMigrationOptions): Promise<void> {
  return validateSqliteDatabase(options, nodeHost)
}
