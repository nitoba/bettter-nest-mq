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

/** Isolate the published adapter mismatch without importing Nest or this package's source.
 * Only a dedicated test database is permitted: this creates/drops its own random schema. */
export async function verifyPublishedFlowProtocol(connectionString: string): Promise<void> {
  const schema = `mq_protocol_${randomUUID().replaceAll('-', '')}`
  const namespace = 'published-flow-protocol'
  const pool = new Pool({ connectionString, max: 4 })
  const failures: Error[] = []
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
    console.log(
      'PROTOCOL: native PostgreSQL FlowStore persisted waiting-children with one pending child'
    )

    const read = await store.getJob({ jobId: parent.id })
    if (Result.isError(read))
      failures.push(
        new Error('JobStore.getJob must be able to inspect a suspended flow parent', {
          cause: read.error
        })
      )
    const heartbeat = await store.heartbeat({
      leases: [{ jobId: parent.id, leaseToken: parent.leaseToken }],
      leaseDurationMs: 60_000,
      now: now + 3
    })
    if (Result.isError(heartbeat))
      failures.push(
        new Error(
          'Heartbeat must report the relinquished lease as lost, not reject the whole batch',
          { cause: heartbeat.error }
        )
      )
    else assert.equal(heartbeat.value.lost.length, 1)
    const release = await store.release({
      jobId: parent.id,
      leaseToken: parent.leaseToken,
      now: now + 3
    })
    if (Result.isOk(release) || !(release.error instanceof LeaseLostError))
      failures.push(
        new Error('Releasing the previous fan-out lease must report LeaseLostError', {
          cause: Result.isError(release) ? release.error : release.value
        })
      )
    const persisted = unwrap(await flows.getFlow({ flowId: parent.id }))
    assert.equal(persisted?.parent.state, 'waiting-children')
    assert.equal(persisted.parent.flow.pending, 1)
    for (const failure of failures) console.error('PROTOCOL INCOMPATIBILITY', failure)
    if (failures.length > 0)
      throw new AggregateError(
        failures,
        'Published PostgreSQL JobStore/FlowStore compatibility regression'
      )
    console.log(
      'PASS published JobStore/FlowStore suspended parent reads, heartbeat and lease fencing'
    )
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
