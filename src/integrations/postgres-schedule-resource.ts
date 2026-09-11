import type { Pool } from 'pg'
import { PostgresJobScheduleStore } from 'better-effect-mq-postgres'
import { scheduleToken } from '../engine/schedule-plan.ts'
import { postgresJsonPool } from './postgres-json-pool.ts'

/** Reuse the native pool/parser boundary and the raw stable JobStore namespace. */
export function postgresScheduleLayer(name: string, pool: Pool, schema: string, namespace: string) {
  return PostgresJobScheduleStore.layerFor(scheduleToken(name), {
    pool: postgresJsonPool(pool),
    schema,
    namespace,
    validateSchema: false
  })
}
