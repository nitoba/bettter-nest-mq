import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import {
  Queue as EngineQueue,
  QueueControls as EngineControls,
  makeJobName,
  makeQueueName,
  makeWorkerId,
  LeaseLostError
} from 'better-effect-mq'
import { Result } from 'better-result'
import { z } from 'zod'
import { getQueueDefinition, Job, Queue, QueueControls, QueueService } from '../../src/index.ts'
import { dispatchStore } from '../../src/engine/controlled-store.ts'
import { controlReferenceStore } from '../fixtures/control-store.ts'

@Queue({ name: 'clock-race', connection: 'primary' })
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

async function racedJob() {
  const raw = controlReferenceStore()
  const now = Date.now()
  const queue = valueOf(makeQueueName('clock-race'))
  const name = valueOf(makeJobName('task'))
  const workerId = valueOf(makeWorkerId('clock-race-worker'))
  const identity = { queue, name, version: 1 }
  const registry = EngineControls.registry({
    group: 'clock-race-test',
    controls: [EngineControls.define(EngineQueue.define('clock-race'), { globalConcurrency: 1 })]
  })
  const policy = valueOf(await raw.reconcile(registry))
  const revision = policy.records[0]?.revision
  assert.ok(revision)
  valueOf(
    await raw.enqueue({
      job: identity,
      payload: { value: 'payload' },
      now,
      runAt: now,
      attemptsMax: 3
    })
  )
  const claimed = valueOf(
    await raw.claimControlled({
      queue,
      accepted: [identity],
      workerId,
      limit: 1,
      leaseDurationMs: 60_000,
      now: now + 1,
      controlsRevision: revision
    })
  )
  const job = claimed.jobs[0]
  assert.ok(job)
  // Deterministically reproduce a heartbeat committing after a mutation sampled its clock.
  const renewed = valueOf(
    await raw.heartbeat({
      leases: [{ jobId: job.id, leaseToken: job.leaseToken }],
      leaseDurationMs: 60_000,
      now: now + 100
    })
  )
  expect(renewed.renewed).toHaveLength(1)
  return { raw, view: dispatchStore(raw, [getQueueDefinition(new ClockQueue())]), job, now }
}

test('a heartbeat that wins before settlement does not force a completed handler to run again', async () => {
  const { raw, view, job, now } = await racedJob()
  const result = valueOf(
    await view.settle({
      jobId: job.id,
      leaseToken: job.leaseToken,
      startedAt: now + 1,
      now: now + 10,
      outcome: { type: 'complete', result: { value: 'done' } }
    })
  )
  expect(result.status).toBe('applied')
  const stored = valueOf(await raw.getJob({ jobId: job.id }))
  expect(stored).toMatchObject({
    state: 'completed',
    deliveryCount: 1,
    attemptsMade: 1,
    result: { value: 'done' }
  })
  expect(stored?.updatedAt).toBeGreaterThanOrEqual(now + 100)
})

test('a heartbeat clock race does not reject an otherwise valid cooperative cancellation request', async () => {
  const { raw, view, job, now } = await racedJob()
  valueOf(await view.cancel({ jobId: job.id, now: now + 10 }))
  const stored = valueOf(await raw.getJob({ jobId: job.id }))
  expect(stored?.state).toBe('active')
  expect(stored?.leaseToken).toBe(job.leaseToken)
  expect(stored?.cancellationRequestedAt).toBeGreaterThanOrEqual(now + 100)
})

test('retry settlement preserves its declared delay when a rejected clock sample is refreshed', async () => {
  const { raw, view, job, now } = await racedJob()
  valueOf(
    await view.settle({
      jobId: job.id,
      leaseToken: job.leaseToken,
      startedAt: now + 1,
      now: now + 10,
      outcome: {
        type: 'retry',
        runAt: now + 510,
        retryDelayMs: 500,
        failure: { kind: 'defect', message: 'transient', retryable: true, recordedAt: now + 10 }
      }
    })
  )
  const stored = valueOf(await raw.getJob({ jobId: job.id }))
  assert.ok(stored)
  expect(stored.state).toBe('delayed')
  expect(stored.runAt - stored.updatedAt).toBe(500)
  expect(stored.failure?.recordedAt).toBe(now + 10)
  expect(stored.deliveryCount).toBe(1)
})

test('clock recovery never makes an expired lease eligible to settle again', async () => {
  const { view, job, now } = await racedJob()
  const result = await view.settle({
    jobId: job.id,
    leaseToken: job.leaseToken,
    now: now + 100_000,
    outcome: { type: 'complete', result: { value: 'late' } }
  })
  assert.ok(Result.isError(result))
  expect(result.error).toBeInstanceOf(LeaseLostError)
})
