import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Pool } from 'pg'
import {
  LeaseLostError,
  Queue as EngineQueue,
  QueueControls as EngineControls,
  makeJobName,
  makeQueueName,
  makeWorkerId
} from 'better-effect-mq'
import { Result } from 'better-result'
import { z } from 'zod'
import { getQueueDefinition, Job, Queue, QueueControls, QueueService } from '../../src/index.ts'
import { migratePostgres, postgres } from '../../src/integrations/postgres.ts'
import { EngineSession } from '../../src/engine/engine-session.ts'
import { dispatchStore, requireControlledStore } from '../../src/engine/controlled-store.ts'

@Queue({ name: 'postgres-clock', connection: 'primary' })
@QueueControls({ globalConcurrency: 1 })
class ClockQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({
    payload: z.object({ value: z.string() }),
    result: z.object({ value: z.string() })
  })
}

function valueOf<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result)) throw result.error
  return result.value
}

export async function verifyPostgresControlledClock(connectionString: string): Promise<void> {
  const schema = `mq_clock_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 4 })
  const queues = [getQueueDefinition(new ClockQueue())]
  const session = new EngineSession({ primary: postgres({ pool, schema }) })
  try {
    await migratePostgres({ pool, schema })
    await session.start(queues)
    await session.withStore('primary', async (raw) => {
      const controlled = requireControlledStore(raw)
      const identity = {
        queue: valueOf(makeQueueName('postgres-clock')),
        name: valueOf(makeJobName('task')),
        version: 1
      }
      const workerId = valueOf(makeWorkerId('postgres-clock-worker'))
      const policy = valueOf(
        await controlled.reconcile(
          EngineControls.registry({
            group: 'clock-test',
            controls: [
              EngineControls.define(EngineQueue.define('postgres-clock'), { globalConcurrency: 1 })
            ]
          })
        )
      )
      const revision = policy.records[0]?.revision
      assert.ok(revision)
      const view = dispatchStore(raw, queues)
      const now = Date.now()
      const saved = valueOf(
        await raw.enqueue({
          job: identity,
          payload: { value: 'payload' },
          attemptsMax: 2,
          now,
          runAt: now
        })
      )
      const claim = {
        queue: identity.queue,
        accepted: [identity],
        workerId,
        controlsRevision: revision,
        limit: 1,
        leaseDurationMs: 60_000
      }
      const job = valueOf(await controlled.claimControlled({ ...claim, now: now + 1 })).jobs[0]
      assert.ok(job)
      valueOf(
        await raw.heartbeat({
          leases: [{ jobId: job.id, leaseToken: job.leaseToken }],
          now: now + 1_000,
          leaseDurationMs: 60_000
        })
      )
      const request = {
        jobId: job.id,
        leaseToken: job.leaseToken,
        startedAt: now + 1,
        now: now + 10,
        outcome: { type: 'complete' as const, result: { value: 'persisted' } }
      }
      assert.equal(valueOf(await view.settle(request)).status, 'applied')
      assert.equal(valueOf(await view.settle(request)).status, 'already-applied')
      const stored = valueOf(await raw.getJob({ jobId: saved.job.id }))
      assert.equal(stored?.state, 'completed')
      assert.equal(stored.deliveryCount, 1)
      assert.ok(stored.updatedAt >= now + 1_000)
      assert.equal(valueOf(await raw.getAttempts({ jobId: job.id })).length, 1)

      const later = Math.max(Date.now(), stored.updatedAt) + 1
      valueOf(
        await raw.enqueue({
          job: identity,
          payload: { value: 'cancel' },
          attemptsMax: 2,
          now: later,
          runAt: later
        })
      )
      const active = valueOf(await controlled.claimControlled({ ...claim, now: later + 1 })).jobs[0]
      assert.ok(active)
      valueOf(
        await raw.heartbeat({
          leases: [{ jobId: active.id, leaseToken: active.leaseToken }],
          now: later + 1_000,
          leaseDurationMs: 60_000
        })
      )
      valueOf(await view.cancel({ jobId: active.id, now: later + 10 }))
      const requested = valueOf(await raw.getJob({ jobId: active.id }))
      assert.equal(requested?.state, 'active')
      assert.equal(requested.leaseToken, active.leaseToken)
      assert.ok((requested.cancellationRequestedAt ?? 0) >= later + 1_000)
      const expired = await view.settle({
        jobId: active.id,
        leaseToken: active.leaseToken,
        now: later + 100_000,
        outcome: { type: 'complete', result: { value: 'late' } }
      })
      assert.ok(Result.isError(expired))
      assert.ok(expired.error instanceof LeaseLostError)
      valueOf(
        await controlled.recoverStalledControlled({
          queue: identity.queue,
          controlsRevision: revision,
          now: later + 100_000,
          maxStalledCount: 1
        })
      )
      const permits = await pool.query<{ count: number }>(
        `SELECT count(*)::integer AS count FROM "${schema}".better_effect_mq_controlled_permits`
      )
      assert.equal(permits.rows[0]?.count, 0)
    })
    console.log(
      'PASS PostgreSQL clock races: one durable completion, idempotent acknowledgment, cooperative cancellation and expired-lease fencing'
    )
  } finally {
    try {
      await session.close()
    } finally {
      try {
        await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
      } finally {
        await pool.end()
      }
    }
  }
}
