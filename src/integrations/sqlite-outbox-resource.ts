import { Layer } from 'better-effect'
import { SqliteOutboxStore } from 'better-effect-mq-sqlite'
import type { SqliteDatabase, SqliteOutboxStoreContract } from 'better-effect-mq-sqlite'
import type { OutboxStore } from 'better-effect-mq-outbox'
import { outboxToken } from '../engine/outbox-plan.ts'
import type { NamedOutbox } from '../engine/outbox-plan.ts'
import { MqOutboxException } from '../outbox/errors.ts'

export interface NativeSqliteOutbox {
  readonly database: SqliteDatabase
  readonly namespace: string
}

interface DisposableSqliteOutboxStore extends SqliteOutboxStoreContract {
  dispose(): Promise<void>
}

const resources = new WeakMap<OutboxStore, NativeSqliteOutbox>()

function scopedNamespace(name: string, namespace: string): string {
  return `${namespace}:${encodeURIComponent(outboxToken(name).serviceTag)}`
}

export function sqliteOutboxLayer(
  name: string,
  database: SqliteDatabase,
  namespace: string
): Layer<NamedOutbox, never> {
  const token = outboxToken(name)
  const sourceNamespace = scopedNamespace(name, namespace)
  return Layer.scoped(
    token,
    () => {
      // SAFETY: SqliteOutboxStore.make returns its concrete implementation; dispose is intentionally
      // omitted from the public store contract but is the lifecycle method used by layerFor as well.
      const store = SqliteOutboxStore.make({
        database,
        namespace: sourceNamespace,
        configurePragmas: false,
        validateSchema: true
      }) as DisposableSqliteOutboxStore
      resources.set(store, Object.freeze({ database, namespace: sourceNamespace }))
      return store
    },
    async (store) => {
      resources.delete(store)
      await (store as DisposableSqliteOutboxStore).dispose()
    }
  )
}

export function nativeSqliteOutbox(store: OutboxStore): NativeSqliteOutbox {
  const resource = resources.get(store)
  if (resource === undefined)
    throw new MqOutboxException('unavailable', 'This source is not a SQLite outbox')
  return resource
}
