import { SqliteJobScheduleStore } from 'better-effect-mq-sqlite'
import type { SqliteDatabase } from 'better-effect-mq-sqlite'
import { scheduleToken } from '../engine/schedule-plan.ts'

/** Share the acquired job database and its raw namespace, never a new file handle. */
export function sqliteScheduleLayer(name: string, database: SqliteDatabase, namespace: string) {
  return SqliteJobScheduleStore.layerFor(scheduleToken(name), {
    database,
    namespace,
    configurePragmas: false,
    validateSchema: true
  })
}
