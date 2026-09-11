import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { z } from 'zod'
import { Job, JobCancelledException, JobContext, JobData, MqModule, MqWorkersService, Process, Queue, QueueService, Worker, type JobExecutionContext } from '../../src/index.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

@Queue({ name: 'concurrency', connection: 'primary' })
class ConcurrentQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.int(), result: z.int() })
  @Job({ name: 'cancel', version: 1 })
  readonly cancellable = this.job({ payload: z.string(), result: z.string() })
}

@Injectable()
class Barriers {
  readonly admitted = Promise.withResolvers<void>()
  readonly twoAdmitted = Promise.withResolvers<void>()
  readonly release = Promise.withResolvers<void>()
  readonly cancelStarted = Promise.withResolvers<void>()
  active = 0
  maximum = 0
  finished = 0
  cancelledSignal: AbortSignal | undefined
}

@Injectable()
@Worker({ name: 'concurrency-worker', concurrency: 4, pollIntervalMs: 5, leaseDurationMs: 300, heartbeatIntervalMs: 30 })
class ConcurrentWorker {
  constructor(@Inject(Barriers) private readonly barriers: Barriers) {}

  @Process(ConcurrentQueue, 'task', { concurrency: 2 })
  async run(@JobData() value: number) {
    this.barriers.active += 1
    this.barriers.maximum = Math.max(this.barriers.maximum, this.barriers.active)
    this.barriers.admitted.resolve()
    if (this.barriers.active === 2) this.barriers.twoAdmitted.resolve()
    try { await this.barriers.release.promise; return value }
    finally { this.barriers.active -= 1; this.barriers.finished += 1 }
  }

  @Process(ConcurrentQueue, 'cancellable')
  async cancel(@JobContext() context: JobExecutionContext) {
    this.barriers.cancelledSignal = context.signal
    this.barriers.cancelStarted.resolve()
    await new Promise<void>((resolve) => {
      if (context.signal.aborted) return resolve()
      context.signal.addEventListener('abort', () => resolve(), { once: true })
    })
    return 'aborted cooperatively'
  }
}

async function createApplication() {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [MqModule.forRoot({ connections: { primary: fixture.connection }, shutdown: { gracePeriodMs: 1_000 } }), MqModule.forFeature([ConcurrentQueue])],
    providers: [Barriers, ConcurrentWorker]
  }).compile()
  await app.init()
  return { app, fixture, barriers: app.get(Barriers), queue: app.get(ConcurrentQueue) }
}

test('per-handler concurrency is enforced below the worker-wide concurrency', async () => {
  const { app, barriers, queue } = await createApplication()
  try {
    const ids = await queue.task.enqueueMany(Array.from({ length: 6 }, (_, index) => ({ payload: index })))
    await barriers.twoAdmitted.promise
    expect(barriers.active).toBe(2)
    barriers.release.resolve()
    const results = await Promise.all(ids.map((id) => queue.task.awaitResult(id, { timeoutMs: 3_000, pollIntervalMs: 5 })))
    expect(results).toEqual([0, 1, 2, 3, 4, 5])
    await app.get(MqWorkersService).awaitIdle({ timeoutMs: 1_000 })
    expect(barriers.maximum).toBe(2)
    expect(barriers.finished).toBe(6)
  } finally { barriers.release.resolve(); await app.close() }
})

test('cancelling an active job aborts the handler signal and persists cancellation', async () => {
  const { app, barriers, queue } = await createApplication()
  try {
    const id = await queue.cancellable.enqueue('active')
    await barriers.cancelStarted.promise
    expect((await queue.cancellable.poll(id))?.state).toBe('active')
    await queue.cancellable.cancel(id)
    await assert.rejects(queue.cancellable.awaitResult(id, { timeoutMs: 3_000, pollIntervalMs: 5 }), JobCancelledException)
    expect(barriers.cancelledSignal?.aborted).toBe(true)
    expect((await queue.cancellable.attempts(id))[0]?.outcome).toBe('cancelled')
  } finally { await app.close() }
})

test('application shutdown drains an admitted handler before releasing its store', async () => {
  const { app, barriers, queue, fixture } = await createApplication()
  await queue.task.enqueue(1)
  await barriers.admitted.promise
  const closing = app.close()
  expect(fixture.trace.resourceReleases).toBe(0)
  barriers.release.resolve()
  await closing
  expect(barriers.finished).toBe(1)
  expect(fixture.trace.events).toEqual(['acquire', 'layer-release', 'resource-release'])
  expect(app.get(MqWorkersService).workers()).toEqual([])
})
