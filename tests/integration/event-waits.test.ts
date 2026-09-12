import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Inject, Injectable } from '@nestjs/common'
import { Test } from '@nestjs/testing'
import { Layer } from 'better-effect'
import { JobEventStore, MemoryJobEventStore, MemoryJobStore } from 'better-effect-mq'
import { z } from 'zod'
import { Job, JobData, JobWaitAbortedException, JobWaitTimeoutException, MqJobException, MqModule, Process, Queue, QueueService, Worker } from '../../src/index.ts'
import { defineConnection, namedStoreToken } from '../../src/engine/connection-definition.ts'

@Queue({ name: 'event-wait', connection: 'primary' })
class EventQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({ payload: z.string(), result: z.string() })
}
@Injectable()
class Gate { readonly release = Promise.withResolvers<void>() }
@Worker({ name: 'event-wait', pollIntervalMs: 5 })
class EventWorker {
  constructor(@Inject(Gate) private readonly gate: Gate) {}
  @Process(EventQueue, 'task')
  async run(@JobData() value: string) { await this.gate.release.promise; return value }
}
function fixture(enabled = true) {
  const events = MemoryJobEventStore.make({ retention: { count: 100 } })
  const jobs = MemoryJobStore.make({ eventStore: events })
  const entered = Promise.withResolvers<void>()
  const original = events.awaitEvents.bind(events)
  let waits = 0
  Object.defineProperty(events, 'awaitEvents', { value: (...args: Parameters<typeof original>) => {
    waits += 1
    entered.resolve()
    return original(...args)
  } })
  const connection = defineConnection({ adapter: 'memory', ownership: 'borrowed', boundary: jobs, scope: 'event-wait-test' }, (token) => {
    const layer = Layer.succeed(token, jobs)
    return enabled ? { layer, events: (name: string) => Layer.succeed(JobEventStore.for(namedStoreToken(name)), events) } : { layer }
  })
  return { connection, events, entered: entered.promise, waits: () => waits }
}
async function application(source: ReturnType<typeof fixture>, workers = true) {
  const app = await Test.createTestingModule({ imports: [
    MqModule.forRoot({ connections: { primary: source.connection }, execution: { workers }, shutdown: { gracePeriodMs: 30 } }),
    MqModule.forFeature([EventQueue])
  ], providers: [Gate, EventWorker] }).compile()
  await app.init()
  return app
}
// JSON construction intentionally exercises a public JavaScript caller during the red baseline.
const eventOptions = () => JSON.parse('{"strategy":"events","pollFallbackMs":1000,"timeoutMs":2000}')

test('event result waits enter the matching event store and return the persisted result', async () => {
  const source = fixture()
  const app = await application(source)
  const gate = app.get(Gate)
  try {
    const queue = app.get(EventQueue)
    const id = await queue.task.enqueue('123')
    const pending = queue.task.awaitResult(id, eventOptions())
    await Promise.race([source.entered, pending])
    expect(source.waits()).toBeGreaterThan(0)
    gate.release.resolve()
    expect(await pending).toBe('123')
    expect((await queue.task.poll(id))?.state).toBe('completed')
  } finally { gate.release.resolve(); await app.close() }
})

test('event strategy requires an explicitly enabled reader; ordinary polling remains available', async () => {
  const source = fixture(false)
  const app = await application(source)
  try {
    app.get(Gate).release.resolve()
    const job = app.get(EventQueue).task
    const id = await job.enqueue('ordinary')
    await assert.rejects(job.awaitResult(id, eventOptions()), MqJobException)
    expect(await job.awaitResult(id, { timeoutMs: 1000, pollIntervalMs: 5 })).toBe('ordinary')
  } finally { app.get(Gate).release.resolve(); await app.close() }
})

test('event wait timeout ends only the wait, not the durable job', async () => {
  const app = await application(fixture(), false)
  try {
    const job = app.get(EventQueue).task
    const id = await job.enqueue('not cancelled')
    await assert.rejects(job.awaitResult(id, { ...eventOptions(), timeoutMs: 20 }), JobWaitTimeoutException)
    expect((await job.poll(id))?.state).toBe('waiting')
  } finally { await app.close() }
})

test('event wait abort is scoped to the caller and preserves the job', async () => {
  const source = fixture()
  const app = await application(source, false)
  try {
    const job = app.get(EventQueue).task
    const id = await job.enqueue('not aborted')
    const controller = new AbortController()
    const waiting = job.awaitResult(id, { ...eventOptions(), signal: controller.signal })
    const rejected = assert.rejects(waiting, JobWaitAbortedException)
    await Promise.race([source.entered, waiting])
    controller.abort('caller left')
    await rejected
    expect((await job.poll(id))?.state).toBe('waiting')
  } finally { await app.close() }
})

test('invalid event fallback timers are rejected before starting a wait', async () => {
  const app = await application(fixture(), false)
  try {
    const job = app.get(EventQueue).task
    const id = await job.enqueue('pending')
    for (const value of [0, -1, 0.5, Number.NaN, 2_147_483_648]) {
      await assert.rejects(job.awaitResult(id, { ...eventOptions(), pollFallbackMs: value }))
    }
  } finally { await app.close() }
})
