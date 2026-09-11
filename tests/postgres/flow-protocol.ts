import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Effect, Runtime } from 'better-effect'
import { Result } from 'better-result'
import {
  JobStore,
  makeFlowChildId,
  makePreparedEnqueue,
  makeQueueName,
  makeWorkerId,
  LeaseLostError
} from 'better-effect-mq'
import { PostgresJobStore, PostgresFlowStore, PostgresMigrator } from 'better-effect-mq-postgres'
import { Pool } from 'pg'

function unwrap<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result)) throw result.error
  return result.value
}

/** Verify the released flow lease boundary without importing Nest or this package's source.
 * Suspended-parent inspection belongs to FlowStore/v2. The v1 JobStore keeps its frozen
 * state union while heartbeat/release must safely fence the relinquished parent lease. */
export async function verifyPublishedFlowProtocol(connectionString: string): Promise<void> {
  const schema = `mq_protocol_${randomUUID().replaceAll('-', '')}`
  const namespace = 'published-flow-protocol'
  const pool = new Pool({ connectionString, max: 4 })
  try {
    await PostgresMigrator.run(pool, { schema })
    await using runtime = await Runtime.make(PostgresJobStore.layer({ pool, schema, namespace }))
    const store = unwrap(
      await runtime.run(() =>
        Effect.gen(async function* () {
          return Result.ok(yield* JobStore)
        })
      )
    )
    const flows = await PostgresFlowStore.make({ pool, schema, namespace })
    try {
      const queue = unwrap(makeQueueName('protocol'))
      const identity = { queue, name: 'parent', version: 1 }
      const now = Date.now()
      const saved = unwrap(
        await store.enqueue({
          job: identity,
          payload: { valid: true },
          runAt: now,
          now,
          attemptsMax: 2
        })
      )
      const claim = unwrap(
        await store.claim({
          queue,
          accepted: [identity],
          workerId: unwrap(makeWorkerId('protocol-worker')),
          limit: 1,
          leaseDurationMs: 60_000,
          now: now + 1
        })
      )
      const parent = claim.jobs[0]
      assert.ok(parent)
      const childId = unwrap(
        makeFlowChildId({ parentStoreKey: 'default', flowId: saved.job.id, childKey: 'one' })
      )
      const request = unwrap(
        makePreparedEnqueue({
          protocolVersion: 1,
          identity: { queue: 'protocol', name: 'child', version: 1 },
          id: childId,
          payload: { valid: true },
          metadata: {},
          priority: 0,
          attemptsMax: 1,
          runAt: now + 2,
          now: now + 2
        })
      )
      const fanOut = unwrap(
        await flows.fanOut({
          flowId: parent.id,
          flowName: 'protocol-flow',
          parentStoreKey: 'default',
          depth: 1,
          leaseToken: parent.leaseToken,
          failFast: false,
          children: [
            {
              childKey: 'one',
              name: 'child',
              version: 1,
              storeKey: 'default',
              childJobId: childId,
              request
            }
          ],
          now: now + 2
        })
      )
      assert.equal(fanOut.parent.state, 'waiting-children')
      assert.equal(fanOut.parent.flow.pending, 1)

      const persisted = unwrap(await flows.getFlow({ flowId: parent.id }))
      assert.equal(persisted?.parent.state, 'waiting-children')
      assert.equal(persisted.parent.flow.pending, 1)

      // The released v1 JobStore intentionally remains a v1 inspection API. It does
      // not widen JobRecord to expose waiting-children; callers use FlowStore/v2 here.
      const v1Read = await store.getJob({ jobId: parent.id })
      assert.ok(Result.isError(v1Read))

      const heartbeat = unwrap(
        await store.heartbeat({
          leases: [{ jobId: parent.id, leaseToken: parent.leaseToken }],
          leaseDurationMs: 60_000,
          now: now + 3
        })
      )
      assert.equal(heartbeat.renewed.length, 0)
      assert.equal(heartbeat.lost.length, 1)
      assert.equal(heartbeat.lost[0]?.jobId, parent.id)

      const release = await store.release({
        jobId: parent.id,
        leaseToken: parent.leaseToken,
        now: now + 3
      })
      assert.ok(Result.isError(release))
      assert.ok(LeaseLostError.is(release.error))

      const afterFencing = unwrap(await flows.getFlow({ flowId: parent.id }))
      assert.equal(afterFencing?.parent.state, 'waiting-children')
      assert.equal(afterFencing.parent.flow.pending, 1)
      console.log(
        'PASS released FlowStore suspended-parent inspection and JobStore relinquished-lease fencing'
      )
    } finally {
      await flows.dispose()
    }
  } finally {
    try {
      await pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`)
    } finally {
      await pool.end()
    }
  }
}

if (import.meta.main) {
  const connectionString = process.env.MQ_TEST_DATABASE_URL
  assert.ok(connectionString, 'Set MQ_TEST_DATABASE_URL to a dedicated test database')
  await verifyPublishedFlowProtocol(connectionString)
}
