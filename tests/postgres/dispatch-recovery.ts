import 'reflect-metadata'
import assert from 'node:assert/strict'
import { randomUUID } from 'node:crypto'
import { Pool } from 'pg'
import { makeJobName, makeQueueName, makeWorkerId } from 'better-effect-mq'
import { Result } from 'better-result'
import { z } from 'zod'
import { getQueueDefinition, Job, Queue, QueueService } from '../../src/index.ts'
import { postgres, migratePostgres } from '../../src/integrations/postgres.ts'
import { dispatchStore } from '../../src/engine/controlled-store.ts'
import { EngineSession } from '../../src/engine/engine-session.ts'

@Queue({ name: 'ordinary-recovery', connection: 'primary' })
class OrdinaryQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.object({ message: z.string() }), result: z.string() })
}

export async function verifyOrdinaryRecovery(connectionString: string): Promise<void> {
  const schema = `mq_recovery_${randomUUID().replaceAll('-', '')}`
  const pool = new Pool({ connectionString, max: 4 })
  const queues = [getQueueDefinition(new OrdinaryQueue())]
  const session = new EngineSession({ primary: postgres({ pool, schema }) })
  try {
    await migratePostgres({ pool, schema })
    await session.start(queues)
    await session.withStore('primary', async (raw) => {
      const queue = makeQueueName('ordinary-recovery')
      const name = makeJobName('task')
      const workerId = makeWorkerId('recovery-regression')
      if (Result.isError(queue)) throw queue.error
      if (Result.isError(name)) throw name.error
      if (Result.isError(workerId)) throw workerId.error
      const identity = { queue: queue.value, name: name.value, version: 1 }
      const now = Date.now()
      const saved = await raw.enqueue({
        job: identity,
        payload: { message: 'retry me' },
        attemptsMax: 2,
        now,
        runAt: now
      })
      if (Result.isError(saved)) throw saved.error
      const claimed = await raw.claim({
        queue: queue.value,
        accepted: [identity],
        workerId: workerId.value,
        limit: 1,
        leaseDurationMs: 20,
        now
      })
      if (Result.isError(claimed)) throw claimed.error
      assert.equal(claimed.value.jobs.length, 1)
      console.log(
        'Ordinary recovery regression: actual PostgreSQL job acquired, checking expired lease'
      )
      // Simulate an expired owner using the store's explicit clock boundary. No fake adapter
      // or local queue is substituted for PostgreSQL's actual persisted transition.
      const recovered = await dispatchStore(raw, queues).recoverStalled({
        maxStalledCount: 1,
        now: now + 30
      })
      if (Result.isError(recovered)) throw recovered.error
      assert.equal(recovered.value.recovered, 1)
      assert.equal(recovered.value.transitions[0]?.attempt?.outcome, 'stalled')
      const record = await raw.getJob({ jobId: saved.value.job.id })
      if (Result.isError(record)) throw record.error
      assert.equal(record.value?.state, 'waiting')
      assert.equal(record.value?.deliveryCount, 1)
    })
    console.log(
      'PASS ordinary PostgreSQL stalled recovery remains operational through the controls dispatcher'
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
