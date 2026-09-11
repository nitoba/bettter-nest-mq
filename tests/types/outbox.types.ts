import { MqModule, type MqOutboxService, type OutboxEntry } from '../../src/index.ts'
import { postgres, postgresOutbox, type PostgresOutboxTransaction } from '../../src/integrations/postgres.ts'

MqModule.forRoot({ execution: { workers: false, outboxPublisher: true }, outbox: { concurrency: 2 } })
postgres({ connectionString: 'postgresql://localhost/db', outbox: true })
// @ts-expect-error Timer configuration must be numeric.
MqModule.forRoot({ outbox: { pollIntervalMs: '100' } })

export async function types(service: MqOutboxService, entry: OutboxEntry): Promise<void> {
  const client = postgresOutbox(service, 'primary')
  const value: number = await client.transaction(entry, async (tx) => {
    const rows = await tx.query<{ value: number }>('SELECT 1 AS value')
    return rows.rows[0]?.value ?? 0
  })
  await client.transaction(async (tx) => { await tx.append(entry) })
  await client.transaction([entry], async () => value)
}

export function restricted(tx: PostgresOutboxTransaction): void {
  // @ts-expect-error Managed transactions cannot release their native client.
  tx.release()
  // @ts-expect-error Pool/driver resources are not exposed on the transaction handle.
  tx.pool.end()
}
