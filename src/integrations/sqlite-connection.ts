import { resolve } from 'node:path'
import { SqliteJobStore, SqliteMigrator, validateDatabase, validateNamespace } from 'better-effect-mq-sqlite'
import type { SqliteDatabase } from 'better-effect-mq-sqlite'
import type { MqConnection } from '../connections/connection.ts'
import { MqConnectionException } from '../connections/errors.ts'
import { requireInteger, requireName } from '../contracts/policies.ts'
import { defineConnection } from '../engine/connection-definition.ts'
import type { SqliteLocation, SqliteMigrationReport, SqliteOptions } from './sqlite.types.ts'

/** Private host boundary; no host constructor or acquired handle enters the root module. */
export interface SqliteHost<Database extends object> {
  open(path: string): Promise<Database>
  close(database: Database): void
}

function assertOptions<Options extends object>(options: Options, allowed: readonly string[]): void {
  const prototype = Object.getPrototypeOf(options)
  if (prototype !== Object.prototype && prototype !== null) throw new Error('SQLite options must be a plain object')
  for (const key of Reflect.ownKeys(options)) {
    if (key !== String(key) || !allowed.includes(String(key))) throw new Error(`Unsupported SQLite option ${String(key)}`)
    const descriptor = Object.getOwnPropertyDescriptor(options, key)
    if (descriptor?.get !== undefined || descriptor?.set !== undefined) throw new Error('SQLite configuration accessors are not supported')
  }
}
function location<Database extends object>(options: SqliteLocation<Database>): SqliteLocation<Database> {
  if ((options.path === undefined) === (options.database === undefined)) throw new Error('Supply exactly one SQLite path or database')
  if (options.database !== undefined) {
    validateDatabase(options.database)
    return Object.freeze({ database: options.database })
  }
  const path = options.path
  requireName(path, 'sqlite.path')
  if (path.includes('\0') || path.startsWith('file:') || path === ':memory:') {
    throw new Error('Use a local file path; for an in-memory database supply an explicitly migrated native database handle')
  }
  return Object.freeze({ path: resolve(path) })
}
function integer(value: number, name: string, minimum: number): number {
  requireInteger(value, name, minimum)
  if (value > 2_147_483_647) throw new Error(`${name} exceeds the supported timer/SQLite integer range`)
  return value
}

export function sqliteConnection<Database extends object>(options: SqliteOptions<Database>, host: SqliteHost<Database>): MqConnection {
  try {
    assertOptions(options, ['database', 'path', 'namespace', 'configurePragmas', 'busyTimeoutMs', 'pollIntervalMs', 'requireCapabilities'])
    const source = location(options)
    const namespace = validateNamespace(options.namespace ?? 'default')
    const configurePragmas = options.configurePragmas ?? source.path !== undefined
    if (configurePragmas !== true && configurePragmas !== false) throw new Error('configurePragmas must be boolean')
    if (!configurePragmas && options.busyTimeoutMs !== undefined) throw new Error('busyTimeoutMs requires explicit pragma configuration on a borrowed handle')
    const busyTimeoutMs = integer(options.busyTimeoutMs ?? 5000, 'sqlite.busyTimeoutMs', 0)
    const pollIntervalMs = integer(options.pollIntervalMs ?? 1000, 'sqlite.pollIntervalMs', 1)
    const requirements = [...(options.requireCapabilities ?? [])]
    return defineConnection({
      adapter: 'sqlite', ownership: source.path === undefined ? 'borrowed' : 'owned',
      boundary: source.database ?? source.path, scope: namespace, requirements
    }, async (token) => {
      const database = source.database ?? await host.open(source.path)
      let closed = false
      const release = async (): Promise<void> => {
        if (closed || source.database !== undefined) return
        closed = true
        host.close(database)
      }
      try {
        const native = validateDatabase(database)
        // File ownership grants only lifecycle/configuration ownership, never permission to migrate.
        if (source.database === undefined) native.exec('PRAGMA journal_mode = WAL')
        const layer = SqliteJobStore.layerFor(token, { database: native, namespace, configurePragmas, busyTimeoutMs, pollIntervalMs, validateSchema: true })
        return source.database === undefined ? { layer, release } : { layer }
      } catch (cause) {
        try { await release() }
        catch (cleanupCause) { throw new AggregateError([cause, cleanupCause], 'SQLite acquisition and cleanup failed', { cause }) }
        throw cause
      }
    })
  } catch (cause) {
    if (cause instanceof MqConnectionException) throw cause
    throw new MqConnectionException('<sqlite>', 'configuration', { cause })
  }
}

async function withDatabase<Database extends object, Value>(options: SqliteLocation<Database>, host: SqliteHost<Database>, operation: (database: SqliteDatabase) => Value): Promise<Value> {
  assertOptions(options, ['path', 'database'])
  const source = location(options)
  const database = source.database ?? await host.open(source.path)
  let result: Value
  try { result = operation(validateDatabase(database)) }
  catch (cause) {
    if (source.database === undefined) {
      try { host.close(database) }
      catch (cleanupCause) { throw new AggregateError([cause, cleanupCause], 'SQLite operation and cleanup failed', { cause }) }
    }
    throw cause
  }
  if (source.database === undefined) host.close(database)
  return result
}
export async function migrateSqliteDatabase<Database extends object>(options: SqliteLocation<Database>, host: SqliteHost<Database>): Promise<SqliteMigrationReport> {
  try {
    return await withDatabase(options, host, (database) => {
      const report = SqliteMigrator.migrate({ database })
      return Object.freeze({ component: report.component, version: report.version, applied: Object.freeze([...report.applied]) })
    })
  } catch (cause) { throw new MqConnectionException('<sqlite-migrations>', 'migrate', { cause }) }
}
export async function validateSqliteDatabase<Database extends object>(options: SqliteLocation<Database>, host: SqliteHost<Database>): Promise<void> {
  try { await withDatabase(options, host, (database) => { SqliteMigrator.validate(database) }) }
  catch (cause) { throw new MqConnectionException('<sqlite-migrations>', 'probe', { cause }) }
}
