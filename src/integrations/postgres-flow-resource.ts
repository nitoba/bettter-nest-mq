import { createHash } from 'node:crypto'
import type { Pool } from 'pg'
import { PostgresFlowStore } from 'better-effect-mq-postgres'
import { flowToken } from '../engine/flow-plan.ts'
import { namedStoreToken } from '../engine/connection-definition.ts'
import { postgresFlowJsonPool } from './postgres-json-pool.ts'

/** Pinned adapter compatibility: PostgresJobStore.layerFor hashes its raw token, but
 * PostgresFlowStore.layerFor does not. Match the existing namespace; never rename stored jobs.
 * Matches the pinned store.ts namespaceForToken; full flow qualification is blocked by
 * upstream issue #387. See docs/flows.md before using this candidate integration. */
export function postgresFlowNamespace(name: string, namespace: string): string {
  const hash = createHash('sha256')
    .update(namedStoreToken(name).serviceTag)
    .digest('hex')
    .slice(0, 48)
  return `${namespace}:store-${hash}`
}
export function postgresFlowLayer(name: string, pool: Pool, schema: string, namespace: string) {
  return PostgresFlowStore.layerFor(flowToken(name), {
    pool: postgresFlowJsonPool(pool),
    schema,
    namespace: postgresFlowNamespace(name, namespace),
    validateSchema: false
  })
}
