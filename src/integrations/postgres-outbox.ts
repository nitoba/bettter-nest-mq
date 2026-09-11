import { outboxCoordinator } from '../engine/outbox-coordinator.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { MqOutboxService } from '../outbox/service.ts'
import type { OutboxEntry } from '../outbox/types.ts'
import { nativePostgresOutbox } from './postgres-outbox-resource.ts'
import { postgresOutboxTransaction } from './postgres-outbox-transaction.ts'
import type { PostgresOutboxCallback, PostgresOutboxClient } from './postgres-outbox.types.ts'

/** Typed factory around an injected Nest service; no native resource is acquired here. */
export function postgresOutbox(service: MqOutboxService, source = 'default'): PostgresOutboxClient {
  return Object.freeze({
    async transaction<Value>(input: OutboxEntry | readonly OutboxEntry[] | PostgresOutboxCallback<Value>, callback?: PostgresOutboxCallback<Value>): Promise<Value> {
      const run = input instanceof Function ? input : callback
      if (run === undefined) throw new MqOutboxException('configuration', 'An outbox transaction requires a callback')
      const entries = input instanceof Function ? [] : 'id' in input ? [input] : [...input]
      return outboxCoordinator(service).withSource(source, async (store, prepare) => {
        const resource = nativePostgresOutbox(store)
        const records = await Promise.all(entries.map(prepare))
        return postgresOutboxTransaction(resource.pool, resource.store, source, records, prepare, run)
      })
    }
  })
}
