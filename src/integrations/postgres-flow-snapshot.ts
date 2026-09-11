import type { Pool } from 'pg'
import { JobStoreFailure, validateFlowChildRecord } from 'better-effect-mq'
import type { FlowStoreV2, JsonValue } from 'better-effect-mq'
import { Result } from 'better-result'
import { postgresFlowJsonPool } from './postgres-json-pool.ts'

/** The pinned FlowStore maps SQL/JSON null to undefined. Restore only values whose
 * presence is established by SQL, preserving the immutable terminal child identity. */
export function postgresFlowSnapshot(
  store: FlowStoreV2,
  pool: Pool,
  schema: string,
  namespace: string
): FlowStoreV2 {
  const client = postgresFlowJsonPool(pool)
  const getFlow: FlowStoreV2['getFlow'] = async (request) => {
    const result = await store.getFlow(request)
    if (Result.isError(result) || result.value === undefined) return result
    const snapshot = result.value
    const missing = snapshot.children.filter(
      (child) => child.status === 'completed' && child.result === undefined
    )
    if (missing.length === 0) return result
    try {
      const rows = await client.query<{
        child_key: string
        child_job_id: string
        result: JsonValue
      }>(
        `SELECT child_key, child_job_id, result
         FROM "${schema}"."better_effect_mq_flow_children"
         WHERE namespace=$1 AND flow_id=$2 AND child_key=ANY($3::text[])
           AND status='completed' AND result IS NOT NULL`,
        [namespace, request.flowId, missing.map((child) => child.childKey)]
      )
      const persisted = new Map(rows.rows.map((row) => [row.child_key, row]))
      const children = []
      for (const child of snapshot.children) {
        const row = persisted.get(child.childKey)
        if (row === undefined || child.status !== 'completed' || child.result !== undefined) {
          children.push(child)
          continue
        }
        if (row.child_job_id !== child.childJobId) {
          return Result.err(
            new JobStoreFailure({
              operation: 'getFlow',
              message: 'Flow child identity changed during snapshot reading',
              retryable: true
            })
          )
        }
        const checked = validateFlowChildRecord({ ...child, result: row.result })
        if (Result.isError(checked)) return checked
        children.push(checked.value)
      }
      return Result.ok(Object.freeze({ ...snapshot, children: Object.freeze(children) }))
    } catch (cause) {
      const error = new JobStoreFailure({
        operation: 'getFlow',
        message: 'Persisted flow child results could not be read',
        retryable: false
      })
      Object.defineProperty(error, 'cause', { value: cause })
      return Result.err(error)
    }
  }
  return new Proxy(store, {
    get(target, key) {
      if (key === 'getFlow') return getFlow
      const value = Reflect.get(target, key, target)
      return value instanceof Function ? value.bind(target) : value
    }
  })
}
