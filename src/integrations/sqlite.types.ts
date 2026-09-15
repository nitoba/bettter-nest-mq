import type { MqCapability } from '../connections/connection.ts'

/** Native handles remain owned by their caller; a path is owned by the MQ application. */
export type SqliteLocation<Database extends object> =
  | { readonly path: string; readonly database?: never }
  | { readonly database: Database; readonly path?: never }

export type SqliteOptions<Database extends object> = SqliteLocation<Database> & {
  readonly namespace?: string
  /** Apply connection-local foreign_keys/busy_timeout; defaults true for owned files, false for borrowed handles. */
  readonly configurePragmas?: boolean
  readonly busyTimeoutMs?: number
  /** Native adapter wake polling for commits made by another local process. */
  readonly pollIntervalMs?: number
  readonly requireCapabilities?: readonly MqCapability[]
}

export interface SqliteMigrationReport {
  readonly component: string
  readonly version: number
  readonly applied: readonly number[]
}
