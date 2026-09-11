import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Effect, Runtime } from 'better-effect'
import { Result } from 'better-result'
import {
  JobStore,
  makeFlowChildId,
  makePreparedEnqueue,
  makeQueueName,
  makeWorkerId
} from 'better-effect-mq'
import { PostgresJobStore, PostgresFlowStore, PostgresMigrator } from 'better-effect-mq-postgres'
import { Pool } from 'pg'

function valueOf<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result)) throw result.error
  return result.value
}

/** Reproduce terminal child observation independently of Nest and of its v2 read projection. */
export async function verifyTypedFlowFailure(connectionString: string): Promise<void> {
  const schema = `mq_child_failure_${randomUUID().replaceAll('-', '')}`
  const namespace = 'published-child-failure'
  const pool = new Pool({ connectionString, max: 4 })
  try {
    await PostgresMigrator.run(pool, { schema })
    await using runtime = await Runtime.make(PostgresJobStore.layer({ pool, schema, namespace }))
    const jobs = valueOf(
      await runtime.run(() =>
        Effect.gen(async function* () {
          return Result.ok(yield* JobStore)
        })
      )
    )
    const flows = await PostgresFlowStore.make({ pool, schema, namespace })
    try {
      const now = Date.now()
      const queue = valueOf(makeQueueName('failure-probe'))
      const identity = { queue, name: 'parent', version: 1 }
      const parent = valueOf(
        await jobs.enqueue({
          job: identity,
          payload: { probe: true },
          runAt: now,
          now,
          attemptsMax: 2
        })
      )
      const claimed = valueOf(
        await jobs.claim({
          queue,
          accepted: [identity],
          workerId: valueOf(makeWorkerId('probe')),
          limit: 1,
          leaseDurationMs: 60_000,
          now: now + 1
        })
      ).jobs[0]
      assert.ok(claimed)
      const children = ['bad', 'good'].map((key) => {
        const id = valueOf(
          makeFlowChildId({
            parentStoreKey: 'default',
            flowId: parent.job.id,
            childKey: key
          })
        )
        return {
          childKey: key,
          name: 'child',
          version: 1,
          storeKey: 'default',
          childJobId: id,
          request: valueOf(
            makePreparedEnqueue({
              protocolVersion: 1,
              identity: { queue: 'failure-probe', name: 'child', version: 1 },
              id,
              payload: { probe: true },
              metadata: {},
              priority: 0,
              attemptsMax: 1,
              runAt: now + 2,
              now: now + 2
            })
          )
        }
      })
      valueOf(
        await flows.fanOut({
          flowId: parent.job.id,
          flowName: 'failure-probe',
          parentStoreKey: 'default',
          depth: 1,
          leaseToken: claimed.leaseToken,
          failFast: false,
          children,
          now: now + 2
        })
      )
      const reconciled = await flows.reconcile({
        flowId: parent.job.id,
        now: now + 3,
        observations: [
          {
            childKey: 'bad',
            state: 'failed',
            failure: {
              kind: 'typed',
              code: 'handler-failure',
              message: 'Handler returned a typed failure',
              data: { code: 'child-failed' },
              retryable: false,
              recordedAt: now + 3
            }
          },
          { childKey: 'good', state: 'completed', result: { value: 'good' } }
        ]
      })
      if (Result.isError(reconciled)) console.error('NATIVE FLOW RECONCILE ERROR', reconciled.error)
      const reports = valueOf(reconciled).reports
      assert.equal(reports.length, 2)
      for (const report of reports) {
        const child = children.find((entry) => entry.childKey === report.childKey)
        assert.ok(child)
        const appended = await flows.appendChildReport({
          id: child.childJobId,
          flowName: 'failure-probe',
          parentStoreKey: 'default',
          report
        })
        if (Result.isError(appended)) console.error('NATIVE FLOW OUTBOX ERROR', appended.error)
        valueOf(appended)
      }
      const observed = await flows.getFlow({ flowId: parent.job.id })
      if (Result.isError(observed)) console.error('NATIVE FLOW SNAPSHOT ERROR', observed.error)
      assert.equal(valueOf(observed)?.outbox.length, 2)
      const recorded = await flows.recordChildResults({
        flowId: parent.job.id,
        reports,
        now: now + 4
      })
      if (Result.isError(recorded)) console.error('NATIVE FLOW REPORT ERROR', recorded.error)
      const value = valueOf(recorded)
      assert.equal(value.applied, 2)
      assert.equal(value.parent.state, 'waiting')
      assert.deepEqual(value.parent.flow, {
        flowName: 'failure-probe',
        failFast: false,
        pending: 0,
        completed: 1,
        failed: 1,
        cancelled: 0
      })
      console.log('PASS published PostgreSQL typed-failure outbox and child reports release the parent')
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
  assert.ok(connectionString, 'MQ_TEST_DATABASE_URL must use a dedicated test database')
  await verifyTypedFlowFailure(connectionString)
}
