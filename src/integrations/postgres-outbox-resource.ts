import { createHash } from 'node:crypto'
import { Layer } from 'better-effect'
import { PostgresOutbox } from 'better-effect-mq-postgres'
import type { PostgresOutboxStore } from 'better-effect-mq-postgres'
import type { OutboxStore } from 'better-effect-mq-outbox'
import type { Pool } from 'pg'
import { outboxToken } from '../engine/outbox-plan.ts'
import type { NamedOutbox } from '../engine/outbox-plan.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import { postgresJsonPool } from './postgres-json-pool.ts'

interface NativeOutbox {
  readonly pool: Pool
  readonly store: PostgresOutboxStore
}
const resources = new WeakMap<OutboxStore, NativeOutbox>()

/** Internal source-specific resource; importing the public factory does not acquire it. */
export function postgresOutboxLayer(
  name: string,
  pool: Pool,
  schema: string,
  namespace: string
): Layer<NamedOutbox, never> {
  const sourceNamespace = `nestjs-outbox-${createHash('sha256')
    .update(JSON.stringify([namespace, name]))
    .digest('hex')}`
  return Layer.scoped(
    outboxToken(name),
    () => {
      const store = PostgresOutbox.make({
        pool: postgresJsonPool(pool),
        schema,
        namespace: sourceNamespace,
        validateSchema: false
      })
      resources.set(store, { pool, store })
      return store
    },
    async (store) => {
      const resource = resources.get(store)
      resources.delete(store)
      await resource?.store.dispose()
    }
  )
}

export function nativePostgresOutbox(store: OutboxStore): NativeOutbox {
  const resource = resources.get(store)
  if (resource === undefined)
    throw new MqOutboxException('unavailable', 'This source is not a PostgreSQL outbox')
  return resource
}
