import type { Layer } from 'better-effect'
import type { Pool } from 'pg'
import { PostgresJobEventStore } from 'better-effect-mq-postgres'
import { eventToken } from '../engine/event-plan.ts'
import type { NamedEvents } from '../engine/event-plan.ts'
import { namedStoreToken } from '../engine/connection-definition.ts'
import type { NamedStore } from '../engine/connection-definition.ts'
import { postgresFlowJsonPool } from './postgres-json-pool.ts'

/** Event attributes are decoded JSON, unlike the pinned v1 JobStore's text codec.
 * Both views borrow the same native pool and leave application parsers untouched. */
export function postgresEventLayer(
  name: string,
  pool: Pool,
  schema: string,
  namespace: string
): Layer<NamedEvents, NamedStore> {
  return PostgresJobEventStore.layerWithFor(eventToken(name), async function* () {
    yield* namedStoreToken(name)
    return { pool: postgresFlowJsonPool(pool), schema, namespace, validateSchema: false }
  })
}
