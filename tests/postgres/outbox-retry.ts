import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Test } from '@nestjs/testing'
import { makeOutboxWorkerId } from 'better-effect-mq-outbox'
import type { OutboxRecord } from 'better-effect-mq-outbox'
import { Result } from 'better-result'
import { Pool } from 'pg'
import { z } from 'zod'
import {
  Job,
  MqModule,
  MqOutboxException,
  MqOutboxService,
  Queue,
  QueueService,
  type OutboxSnapshot
} from '../../src/index.ts'
import { migratePostgres, postgres, postgresOutbox } from '../../src/integrations/postgres.ts'
import { outboxCoordinator } from '../../src/engine/outbox-coordinator.ts'
import { outboxRetryAdapter, retryOutboxRecord } from '../../src/engine/outbox-retry.ts'

@Queue({ name: 'retry-protocol', connection: 'primary' })
class ProtocolQueue extends QueueService {
  @Job({ name: 'job', version: 1 })
  readonly task = this.job({ payload: z.string(), result: z.string() })
}
function valueOf<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result)) throw result.error
  return result.value
}
function expected(record: OutboxSnapshot | OutboxRecord) {
  return {
    updatedAtMs: record.updatedAtMs,
    attemptsMade: record.attemptsMade,
    attemptsMax: record.attemptsMax
  }
}
export async function verifyOutboxRetryProtocol(connectionString: string): Promise<void> {
  const schema = `mq_retry_protocol_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 5 })
  const app = await Test.createTestingModule({
    imports: [
      MqModule.forRoot({
        connections: { primary: postgres({ pool, schema, outbox: true }) },
        execution: { workers: false, outboxPublisher: false }
      }),
      MqModule.forFeature([ProtocolQueue])
    ]
  }).compile()
  try {
    await migratePostgres({ pool, schema })
    await app.init()
    const service = app.get(MqOutboxService)
    const job = await app.get(ProtocolQueue).task.prepare('original', { jobId: 'unchanged' })
    await postgresOutbox(service, 'primary').transaction(
      { id: 'record', job, attempts: 1 },
      async () => undefined
    )
    await outboxCoordinator(service).withSource('primary', async (store) => {
      const owner = valueOf(makeOutboxWorkerId('test-publisher'))
      const claims = valueOf(
        await store.claim({ owner, limit: 1, nowMs: Date.now(), leaseDurationMs: 30_000 })
      )
      const active = claims[0]
      assert.ok(active)
      const snapshot = await service.get('primary', active.id)
      assert.ok(snapshot)
      await assert.rejects(
        service.retryFailed('primary', active.id, { expected: expected(snapshot), attempts: 2 }),
        MqOutboxException
      )
      const failedAt = Math.max(Date.now(), active.updatedAtMs)
      valueOf(
        await store.markFailed({
          id: active.id,
          leaseToken: active.leaseToken,
          nowMs: failedAt,
          failure: {
            kind: 'store-permanent',
            message: 'Reproducible failure',
            retryable: false,
            recordedAtMs: failedAt
          }
        })
      )
      const failed = valueOf(await store.get(active.id))
      assert.ok(failed)
      const firstExpected = expected(failed)
      const pending = await service.retryFailed('primary', active.id, {
        expected: firstExpected,
        attempts: 2
      })
      assert.equal(pending.state, 'pending')
      const oldPublished = await store.markPublished({
        id: active.id,
        leaseToken: active.leaseToken,
        nowMs: pending.updatedAtMs + 1
      })
      assert.ok(
        Result.isError(oldPublished),
        'Old lease cannot acknowledge an administratively retried publication'
      )
      const oldHeartbeat = await store.heartbeat({
        id: active.id,
        leaseToken: active.leaseToken,
        nowMs: pending.updatedAtMs + 1,
        leaseDurationMs: 30_000
      })
      assert.ok(Result.isError(oldHeartbeat))
      assert.equal(valueOf(await store.get(active.id))?.state, 'pending')
      const second = valueOf(
        await store.claim({
          owner,
          limit: 1,
          nowMs: Math.max(Date.now(), pending.runAtMs),
          leaseDurationMs: 30_000
        })
      )[0]
      assert.ok(second)
      assert.notEqual(second.leaseToken, active.leaseToken)
      assert.equal(second.attemptsMade, 2)
      const secondAt = Math.max(Date.now(), second.updatedAtMs)
      valueOf(
        await store.markFailed({
          id: second.id,
          leaseToken: second.leaseToken,
          nowMs: secondAt,
          failure: {
            kind: 'store-permanent',
            message: 'Second publication failure',
            retryable: false,
            recordedAtMs: secondAt
          }
        })
      )
      await assert.rejects(
        service.retryFailed('primary', second.id, { expected: firstExpected, attempts: 2 }),
        MqOutboxException
      )
      const current = valueOf(await store.get(active.id))
      assert.ok(current)
      const desired = retryOutboxRecord(
        current,
        { expected: expected(current), attempts: 1 },
        Date.now()
      )
      // Simulate an external writer changing immutable routing without advancing a version.
      // The admin UPDATE also compares exact persisted content, not only the caller's guard.
      await pool.query(
        `UPDATE "${schema}".better_effect_mq_outbox SET target='different-target' WHERE id=$1`,
        [current.id]
      )
      await assert.rejects(outboxRetryAdapter(store).retry(current, desired), MqOutboxException)
      const final = valueOf(await store.get(active.id))
      assert.equal(final?.state, 'failed')
      assert.equal(final?.target, 'different-target')
      assert.equal(final?.attemptsMade, 2)
    })
    console.log(
      'PASS actual PostgreSQL active/old-lease fencing, repeated recovery guards and full-content compare-and-swap'
    )
  } finally {
    try {
      await app.close()
    } finally {
      try {
        await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      } finally {
        await pool.end()
      }
    }
  }
}
