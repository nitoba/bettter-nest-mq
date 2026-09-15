import { SqliteJobEventStore } from 'better-effect-mq-sqlite'
import type { SqliteDatabase } from 'better-effect-mq-sqlite'
import { eventToken } from '../engine/event-plan.ts'

/** The native reader borrows the job database and derives its namespace from the raw token. */
export function sqliteEventLayer(
  name: string,
  database: SqliteDatabase,
  namespace: string,
  pollIntervalMs: number
) {
  return SqliteJobEventStore.layerFor(eventToken(name), {
    database,
    namespace,
    pollIntervalMs,
    configurePragmas: false,
    validateSchema: true
  })
}
