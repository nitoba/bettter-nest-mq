import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import {
  JobDefinitionError, JobStoreFailure, LeaseLostError,
  Queue as EngineQueue, QueueControls as EngineControls,
  makeJobName, makeQueueName, makeWorkerId
} from 'better-effect-mq'
import { Result } from 'better-result'
import { z } from 'zod'
import { getQueueDefinition, Job, Queue, QueueControls, QueueService } from '../../src/index.ts'
import { dispatchStore } from '../../src/engine/controlled-store.ts'
import { withFreshControlledClock } from '../../src/engine/controlled-clock.ts'
import { controlReferenceStore } from '../fixtures/control-store.ts'

@Queue({ name: 'clock-safety', connection: 'primary' })
@QueueControls({ globalConcurrency: 1 })
class SafetyQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.object({ value: z.string() }), result: z.object({ value: z.string() }) })
}

function valueOf<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result)) throw result.error
  return result.value
}

async function acquiredJob(now = Date.now()) {
  const raw = controlReferenceStore()
  const identity = { queue: valueOf(makeQueueName('clock-safety')), name: valueOf(makeJobName('task')), version: 1 }
  const workerId = valueOf(makeWorkerId('clock-safety-worker'))
  const registry = EngineControls.registry({ group: 'clock-safety', controls: [EngineControls.define(EngineQueue.define('clock-safety'), { globalConcurrency: 1 })] })
  const revision = valueOf(await raw.reconcile(registry)).records[0]?.revision
  assert.ok(revision)
  valueOf(await raw.enqueue({ job: identity, payload: { value: 'payload' }, now, runAt: now, attemptsMax: 3 }))
  const claim = { queue: identity.queue, accepted: [identity], workerId, limit: 1, leaseDurationMs: 60_000, controlsRevision: revision }
  const job = valueOf(await raw.claimControlled({ ...claim, now: now + 1 })).jobs[0]
  assert.ok(job)
  valueOf(await raw.heartbeat({ leases: [{ jobId: job.id, leaseToken: job.leaseToken }], leaseDurationMs: 60_000, now: now + 100 }))
  return { raw, view: dispatchStore(raw, [getQueueDefinition(new SafetyQueue())]), job, now, claim, revision }
}

test('release refreshes a rejected clock sample and returns the same job without consuming an attempt', async () => {
  const { raw, view, job, now, claim } = await acquiredJob()
  valueOf(await view.release({ jobId: job.id, leaseToken: job.leaseToken, now: now + 10 }))
  const stored = valueOf(await raw.getJob({ jobId: job.id }))
  assert.ok(stored)
  expect(stored.state).toBe('waiting')
  expect(stored.attemptsMade).toBe(0)
  const next = valueOf(await raw.claimControlled({ ...claim, now: stored.updatedAt + 1 })).jobs[0]
  expect(next?.id).toBe(job.id)
  expect(next?.leaseToken).not.toBe(job.leaseToken)
})

test('a refreshed clock never grants an old token the replacement delivery lease', async () => {
  const { raw, view, job, now, claim, revision } = await acquiredJob()
  valueOf(await raw.releaseControlled({ jobId: job.id, leaseToken: job.leaseToken, now: now + 200, controlsRevision: revision }))
  const next = valueOf(await raw.claimControlled({ ...claim, now: now + 201 })).jobs[0]
  assert.ok(next)
  const result = await view.settle({ jobId: job.id, leaseToken: job.leaseToken, now: now + 10, outcome: { type: 'complete', result: { value: 'obsolete' } } })
  assert.ok(Result.isError(result))
  expect(result.error).toBeInstanceOf(LeaseLostError)
  const stored = valueOf(await raw.getJob({ jobId: job.id }))
  expect(stored).toMatchObject({ state: 'active', leaseToken: next.leaseToken, deliveryCount: 2, attemptsMade: 0 })
})

test('refresh uses current wall time and cannot backdate settlement into an expired lease', async () => {
  const { view, job, now } = await acquiredJob(Date.now() - 120_000)
  const result = await view.settle({ jobId: job.id, leaseToken: job.leaseToken, now: now + 10, outcome: { type: 'complete', result: { value: 'late' } } })
  assert.ok(Result.isError(result))
  expect(result.error).toBeInstanceOf(LeaseLostError)
})

test('duplicate completion acknowledgment never adds a second attempt after clock refresh', async () => {
  const { raw, view, job, now } = await acquiredJob()
  const request = { jobId: job.id, leaseToken: job.leaseToken, now: now + 10, outcome: { type: 'complete' as const, result: { value: 'done' } } }
  expect(valueOf(await view.settle(request)).status).toBe('applied')
  expect(valueOf(await view.settle(request)).status).toBe('already-applied')
  expect(valueOf(await raw.getAttempts({ jobId: job.id }))).toHaveLength(1)
})

test('refresh is bounded even if every attempted mutation loses to another heartbeat', async () => {
  const { raw, job, now, revision } = await acquiredJob(Date.now() + 1_000)
  let calls = 0
  const result = await withFreshControlledClock(raw, job.id, now + 10, async (sample) => {
    calls += 1
    valueOf(await raw.heartbeat({ leases: [{ jobId: job.id, leaseToken: job.leaseToken }], leaseDurationMs: 60_000, now: now + (calls + 1) * 100 }))
    return raw.settleControlled({ jobId: job.id, leaseToken: job.leaseToken, now: sample, controlsRevision: revision, outcome: { type: 'complete', result: { value: 'never applied' } } })
  })
  assert.ok(Result.isError(result))
  expect(result.error).toBeInstanceOf(JobDefinitionError)
  expect(calls).toBe(4)
  expect(valueOf(await raw.getJob({ jobId: job.id }))).toMatchObject({ state: 'active', deliveryCount: 1, attemptsMade: 0 })
})

test('ambiguous infrastructure errors and unrelated validation errors are never replayed', async () => {
  const { raw, job, now } = await acquiredJob()
  for (const error of [
    new JobStoreFailure({ operation: 'settleControlled', retryable: true, message: 'lost response' }),
    new JobDefinitionError({ field: 'now', message: 'must be a valid timestamp' }),
    new JobDefinitionError({ field: 'result', message: 'must not be earlier than updatedAt' })
  ]) {
    let calls = 0
    const result = await withFreshControlledClock(raw, job.id, now + 10, () => { calls += 1; return Result.err(error) })
    assert.ok(Result.isError(result))
    expect(result.error).toBe(error)
    expect(calls).toBe(1)
  }
})
