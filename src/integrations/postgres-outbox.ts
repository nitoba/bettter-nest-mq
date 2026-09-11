import { createHash } from 'node:crypto'
import { Layer } from 'better-effect'
import { PostgresOutbox } from 'better-effect-mq-postgres'
import type { PostgresOutboxStore } from 'better-effect-mq-postgres'
import type { OutboxStore } from 'better-effect-mq-outbox'
import type { Pool } from 'pg'
import { outboxCoordinator } from '../engine/outbox-coordinator.ts'
import { outboxToken } from '../engine/outbox-plan.ts'
import type { NamedOutbox } from '../engine/outbox-plan.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { MqOutboxService } from '../outbox/service.ts'
import type { OutboxEntry } from '../outbox/types.ts'
import { postgresJsonPool } from './postgres-json-pool.ts'
import { postgresOutboxTransaction } from './postgres-outbox-transaction.ts'
import type { PostgresOutboxCallback } from './postgres-outbox-transaction.ts'

interface NativeOutbox {
  readonly pool: Pool
  readonly store: PostgresOutboxStore
}
const resources = new WeakMap<OutboxStore, NativeOutbox>()

/** An outbox namespace is distinct from job storage and stable across process recreation. */
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

export interface PostgresOutboxClient {
  transaction<Value>(callback: PostgresOutboxCallback<Value>): Promise<Value>
  transaction<Value>(
    entries: OutboxEntry | readonly OutboxEntry[],
    callback: PostgresOutboxCallback<Value>
  ): Promise<Value>
}

/** Typed factory around an injected Nest service; no native resource is acquired here. */
export function postgresOutbox(service: MqOutboxService, source = 'default'): PostgresOutboxClient {
  return Object.freeze({
    async transaction<Value>(
      input: OutboxEntry | readonly OutboxEntry[] | PostgresOutboxCallback<Value>,
      callback?: PostgresOutboxCallback<Value>
    ): Promise<Value> {
      const run = input instanceof Function ? input : callback
      if (run === undefined)
        throw new MqOutboxException('configuration', 'An outbox transaction requires a callback')
      const entries = input instanceof Function ? [] : 'id' in input ? [input] : [...input]
      return outboxCoordinator(service).withSource(source, async (store, prepare) => {
        const resource = resources.get(store)
        if (resource === undefined)
          throw new MqOutboxException('unavailable', 'This source is not a PostgreSQL outbox')
        const records = await Promise.all(entries.map(prepare))
        return postgresOutboxTransaction(
          resource.pool,
          resource.store,
          source,
          records,
          prepare,
          run
        )
      })
    }
  })
}

export type {
  PostgresOutboxTransaction,
  PostgresOutboxCallback,
  PostgresOutboxParameter
} from './postgres-outbox-transaction.ts'
