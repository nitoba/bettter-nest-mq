import { createHash } from 'node:crypto'
import type { Pool } from 'pg'
import { Layer } from 'better-effect'
import { PostgresFlowStore } from 'better-effect-mq-postgres'
import { flowToken } from '../engine/flow-plan.ts'
import type { NamedFlow } from '../engine/flow-plan.ts'
import { namedStoreToken } from '../engine/connection-definition.ts'
import type { NamedStore } from '../engine/connection-definition.ts'
import { postgresFlowJsonPool } from './postgres-json-pool.ts'
import { postgresFlowSnapshot } from './postgres-flow-snapshot.ts'

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
export function postgresFlowLayer(
  name: string,
  pool: Pool,
  schema: string,
  namespace: string
): Layer<NamedFlow, NamedStore> {
  const raw = namedStoreToken(name)
  const address = postgresFlowNamespace(name, namespace)
  let acquired: Awaited<ReturnType<typeof PostgresFlowStore.make>> | undefined
  return Layer.scopedGen(
    flowToken(name),
    async function* () {
      // Preserve the dependency ordering of the upstream layer: jobs initialize first,
      // and this flow resource is disposed before the borrowed/native job resource.
      yield* raw
      const store = await PostgresFlowStore.make({
        pool: postgresFlowJsonPool(pool),
        schema,
        namespace: address,
        validateSchema: false
      })
      acquired = store
      return postgresFlowSnapshot(store, pool, schema, address)
    },
    async () => {
      await acquired?.dispose()
    }
  )
}
