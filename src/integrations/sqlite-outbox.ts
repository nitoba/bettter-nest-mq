import { outboxCoordinator } from '../engine/outbox-coordinator.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import type { MqOutboxService } from '../outbox/service.ts'
import type { OutboxEntry } from '../outbox/types.ts'
import { nativeSqliteOutbox } from './sqlite-outbox-resource.ts'
import { sqliteOutboxTransaction } from './sqlite-outbox-transaction.ts'
import type { SqliteOutboxCallback, SqliteOutboxClient } from './sqlite-outbox.types.ts'

/** Host-neutral factory around an injected Nest service; the configured SQLite subpath owns I/O. */
export function sqliteOutbox(service: MqOutboxService, source = 'default'): SqliteOutboxClient {
  return Object.freeze({
    async transaction<Value>(
      input: OutboxEntry | readonly OutboxEntry[],
      callback: SqliteOutboxCallback<Value>
    ): Promise<Value> {
      const entries = 'id' in input ? [input] : [...input]
      if (entries.length === 0)
        throw new MqOutboxException(
          'configuration',
          'A SQLite outbox transaction requires at least one outbox entry'
        )
      return outboxCoordinator(service).withSource(source, async (store, prepare) => {
        const resource = nativeSqliteOutbox(store)
        const records = await Promise.all(entries.map(prepare))
        return sqliteOutboxTransaction(resource, source, records, prepare, callback)
      })
    }
  })
}
