import type { Pool } from 'pg'
import type { OutboxRetryWrite } from '../engine/outbox-retry.ts'
import { MqOutboxException } from '../outbox/errors.ts'
import { postgresJsonPool } from './postgres-json-pool.ts'

/** A single conditional administrative transition; native publisher leases and settlement are unchanged. */
export function postgresOutboxRetry(pool: Pool, schema: string, namespace: string): OutboxRetryWrite {
  const database = postgresJsonPool(pool)
  const table = `"${schema.replaceAll('"', '""')}"."better_effect_mq_outbox"`
  return async (previous, next) => {
    try {
      const result = await database.query<{ id: string }>(
        `UPDATE ${table}
         SET state='pending', attempts_max=$3, run_at_ms=$4, updated_at_ms=$5
         WHERE namespace=$1 AND id=$2 AND state='failed'
           AND attempts_made=$6 AND attempts_max=$7 AND updated_at_ms=$8
           AND target=$9 AND request_digest=$10 AND request=$11::jsonb
           AND created_at_ms=$12 AND run_at_ms=$13
           AND failure IS NOT DISTINCT FROM $14::jsonb
           AND published_at_ms IS NULL AND lease_owner IS NULL
           AND lease_token IS NULL AND lease_expires_at_ms IS NULL
         RETURNING id`,
        [namespace, previous.id, next.attemptsMax, next.runAtMs, next.updatedAtMs,
          previous.attemptsMade, previous.attemptsMax, previous.updatedAtMs,
          previous.target, previous.requestDigest, JSON.stringify(previous.request),
          previous.createdAtMs, previous.runAtMs, previous.failure === undefined ? null : JSON.stringify(previous.failure)]
      )
      if (result.rows.length !== 1 || result.rows[0]?.id !== previous.id) {
        throw new MqOutboxException('conflict', 'Publication changed concurrently; inspect it before retrying again')
      }
    } catch (cause) {
      if (cause instanceof MqOutboxException) throw cause
      // Never replay an uncertain administrative write automatically. A caller must inspect state.
      throw new MqOutboxException('retry', 'The outbox retry could not be confirmed', { cause })
    }
  }
}
