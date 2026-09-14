import { MqOutboxException } from '../outbox/errors.ts'
import type { MqOutboxService } from '../outbox/service.ts'
import type { OutboxEntry } from '../outbox/types.ts'
import { postgresOutbox } from './postgres-outbox.ts'
import type { PostgresOutboxTransaction } from './postgres-outbox.types.ts'
import { runKyselyOutbox } from './kysely-outbox-scope.ts'
import type { KyselyOutboxCallback, KyselyOutboxClient } from './kysely-outbox.types.ts'

export type { KyselyOutboxCallback, KyselyOutboxClient, KyselyOutboxDatabase, KyselyOutboxTransaction } from './kysely-outbox.types.ts'

/** Inert integration factory. Kysely never owns a separate database pool/runtime. */
export function kyselyOutbox<Database>(service: MqOutboxService, source = 'default'): KyselyOutboxClient<Database> {
  const native = postgresOutbox(service, source)
  return Object.freeze({
    async transaction<Value>(
      input: OutboxEntry | readonly OutboxEntry[] | KyselyOutboxCallback<Database, Value>,
      callback?: KyselyOutboxCallback<Database, Value>
    ): Promise<Value> {
      const run = input instanceof Function ? input : callback
      if (run === undefined) throw new MqOutboxException('configuration', 'A Kysely outbox transaction requires a callback')
      const invoke = (transaction: PostgresOutboxTransaction) => runKyselyOutbox(transaction, run)
      return input instanceof Function ? native.transaction(invoke) : native.transaction(input, invoke)
    }
  })
}
