import { describe, expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { Test } from '@nestjs/testing'
import { MqModule, MqWorkersService, MqJobException, JobFailureException, JobWaitTimeoutException, JobWaitAbortedException, JobCancelledException, SchemaValidationException } from '../../src/index.ts'
import { ExecutionLog, ExecutionQueue, ExecutionWorker } from '../fixtures/execution.ts'
import { memoryConnection } from '../fixtures/memory-connection.ts'

async function application(workers = true) {
  const fixture = memoryConnection()
  const app = await Test.createTestingModule({
    imports: [MqModule.forRoot({ connections: { primary: fixture.connection }, execution: { workers }, shutdown: { gracePeriodMs: 1_000 } }), MqModule.forFeature([ExecutionQueue])],
    providers: [ExecutionLog, ExecutionWorker]
  }).compile()
  await app.init()
  return { app, queue: app.get(ExecutionQueue), log: app.get(ExecutionLog), fixture }
}

const wait = { timeoutMs: 3_000, pollIntervalMs: 5 }

describe('real typed jobs and the upstream supervisor', () => {
  test('executes a decorated Nest service, preserves context and exposes no lease token', async () => {
    const { app, queue, log } = await application()
    try {
      const id = await queue.echo.enqueue({ value: 'hello' }, { metadata: { tenant: 'tenant-1' } })
      expect(await queue.echo.awaitResult(id, wait)).toEqual({ value: 'HELLO' })
      const snapshot = await queue.echo.poll(id)
      expect(snapshot).toMatchObject({ id, state: 'completed', attemptsMade: 1, result: { value: 'HELLO' } })
      expect(snapshot && 'leaseToken' in snapshot).toBe(false)
      expect(log.contexts[0]).toMatchObject({ jobId: id, queue: 'execution', name: 'echo', version: 1, attempt: 1, metadata: { tenant: 'tenant-1' } })
      expect(log.contexts[0]?.signal).toBeInstanceOf(AbortSignal)
      expect(log.contexts[0] && 'leaseToken' in log.contexts[0]).toBe(false)
      expect((await queue.echo.attempts(id)).map((entry) => entry.outcome)).toEqual(['completed'])
      await app.get(MqWorkersService).awaitIdle({ timeoutMs: 1_000 })
      expect(app.get(MqWorkersService).workers()).toMatchObject([{ name: 'execution-worker', state: 'running', activeCount: 0 }])
    } finally { await app.close() }
  })

  test('round-trips decoded Dates and distinguishes input from enqueueDecoded', async () => {
    const { app, queue, log } = await application()
    try {
      const timestamp = '2026-09-10T12:00:00.000Z'
      const first = await queue.codec.enqueue({ timestamp })
      const second = await queue.codec.enqueueDecoded({ timestamp: new Date(timestamp) })
      expect(await queue.codec.awaitResult(first, wait)).toEqual({ timestamp: new Date('2026-09-10T12:00:01.000Z') })
      expect(await queue.codec.awaitResult(second, wait)).toEqual({ timestamp: new Date('2026-09-10T12:00:01.000Z') })
      expect(log.decodedDates.every((value) => value instanceof Date)).toBe(true)
    } finally { await app.close() }
  })

  test('executes means publish and wait, not a local handler call', async () => {
    const { app, queue, log } = await application()
    try {
      expect(await queue.echo.execute({ value: 'execute' }, { wait })).toEqual({ value: 'EXECUTE' })
      expect(log.contexts[0]?.jobId.length).toBeGreaterThan(0)
    } finally { await app.close() }
  })

  test('retries known failures using the persisted policy and records each attempt', async () => {
    const { app, queue } = await application()
    try {
      const id = await queue.retrying.enqueue({ succeedAt: 3, retryable: true })
      expect(await queue.retrying.awaitResult(id, wait)).toEqual({ attempt: 3 })
      const attempts = await queue.retrying.attempts(id)
      expect(attempts.map((entry) => entry.outcome)).toEqual(['retried', 'retried', 'completed'])
      expect(attempts[0]?.failure).toMatchObject({ kind: 'typed', data: { code: 'unavailable', retryable: true } })
      expect(attempts[0]?.retryDelayMs).toBe(5)
      expect(attempts[1]?.retryDelayMs).toBe(10)
    } finally { await app.close() }
  })

  test('non-retryable domain failures reject with typed content and are not retried', async () => {
    const { app, queue } = await application()
    try {
      const id = await queue.retrying.enqueue({ succeedAt: 3, retryable: false })
      await assert.rejects(queue.retrying.awaitResult(id, wait), (error) => {
        assert.ok(error instanceof JobFailureException)
        expect(error.failure).toEqual({ code: 'unavailable', retryable: false })
        return true
      })
      expect((await queue.retrying.poll(id))?.attemptsMade).toBe(1)
    } finally { await app.close() }
  })

  test('unexpected exceptions remain defects and do not retry by default', async () => {
    const { app, queue, log } = await application()
    try {
      const id = await queue.defect.enqueue('broken')
      await assert.rejects(queue.defect.awaitResult(id, wait), MqJobException)
      expect((await queue.defect.poll(id))?.failure?.kind).toBe('defect')
      expect(log.defects).toEqual(['broken'])
    } finally { await app.close() }
  })

  test('invalid handler outputs fail encoding instead of completing successfully', async () => {
    const { app, queue } = await application()
    try {
      const id = await queue.invalidResult.enqueue('not a number')
      await assert.rejects(queue.invalidResult.awaitResult(id, wait), MqJobException)
      expect((await queue.invalidResult.poll(id))?.failure?.kind).toBe('encode')
    } finally { await app.close() }
  })

  test('execution timeout aborts the handler and persists a timeout outcome', async () => {
    const { app, queue, log } = await application()
    try {
      const id = await queue.timeout.enqueue('slow')
      await assert.rejects(queue.timeout.awaitResult(id, wait), MqJobException)
      expect((await queue.timeout.poll(id))?.failure?.kind).toBe('timeout')
      expect(log.contexts.find((context) => context.jobId === id)?.signal.aborted).toBe(true)
    } finally { await app.close() }
  })

  test('idempotency returns one stable ID under concurrent publication', async () => {
    const { app, queue } = await application(false)
    try {
      const ids = await Promise.all(Array.from({ length: 10 }, () => queue.unique.enqueue({ key: 'stable' })))
      expect(new Set(ids).size).toBe(1)
      expect((await queue.unique.poll(ids[0] ?? 'missing'))?.state).toBe('waiting')
    } finally { await app.close() }
  })

  test('prepare is serializable, preserves routing, and does not enqueue', async () => {
    const { app, queue } = await application(false)
    try {
      const prepared = await queue.echo.prepare({ value: 'later' }, { jobId: 'prepared-only' })
      expect(prepared.connection).toBe('primary')
      expect(prepared.request.identity).toEqual({ queue: 'execution', name: 'echo', version: 1 })
      expect(JSON.parse(JSON.stringify(prepared)).request.payload).toEqual({ value: 'later' })
      expect(await queue.echo.poll('prepared-only')).toBeUndefined()
    } finally { await app.close() }
  })

  test('batch publication validates every item before any write', async () => {
    const { app, queue } = await application(false)
    try {
      await assert.rejects(queue.echo.enqueueMany([
        { payload: { value: 'valid' }, options: { jobId: 'batch-valid' } },
        { payload: { value: '' }, options: { jobId: 'batch-invalid' } }
      ]), SchemaValidationException)
      expect(await queue.echo.poll('batch-valid')).toBeUndefined()
      const ids = await queue.echo.enqueueMany([{ payload: { value: 'one' } }, { payload: { value: 'two' } }])
      expect(ids).toHaveLength(2)
    } finally { await app.close() }
  })

  test('bounded waiting and explicit abort never cancel the stored job', async () => {
    const { app, queue } = await application(false)
    try {
      const id = await queue.echo.enqueue({ value: 'pending' })
      await assert.rejects(queue.echo.awaitResult(id, { timeoutMs: 20, pollIntervalMs: 5 }), JobWaitTimeoutException)
      expect((await queue.echo.poll(id))?.state).toBe('waiting')
      const controller = new AbortController()
      const waiting = queue.echo.awaitResult(id, { signal: controller.signal, pollIntervalMs: 5 })
      const rejected = assert.rejects(waiting, JobWaitAbortedException)
      controller.abort()
      await rejected
      expect((await queue.echo.poll(id))?.state).toBe('waiting')
      expect(app.get(MqWorkersService).workers()).toEqual([])
    } finally { await app.close() }
  })

  test('delayed jobs can be promoted, cancelled and administratively retried', async () => {
    const { app, queue } = await application(false)
    try {
      const id = await queue.echo.enqueue({ value: 'delayed' }, { delayMs: 60_000 })
      expect((await queue.echo.poll(id))?.state).toBe('delayed')
      await queue.echo.promote(id)
      expect((await queue.echo.poll(id))?.state).toBe('waiting')
      await queue.echo.cancel(id)
      expect((await queue.echo.poll(id))?.state).toBe('cancelled')
      await assert.rejects(queue.echo.awaitResult(id, wait), JobCancelledException)
      await queue.echo.retry(id)
      expect((await queue.echo.poll(id))?.state).toBe('waiting')
    } finally { await app.close() }
  })

  test('another contract cannot read or mutate a job by guessing its ID', async () => {
    const { app, queue } = await application(false)
    try {
      const id = await queue.echo.enqueue({ value: 'private-to-contract' })
      await assert.rejects(queue.unique.poll(id), MqJobException)
      await assert.rejects(queue.unique.cancel(id), MqJobException)
      expect((await queue.echo.poll(id))?.state).toBe('waiting')
    } finally { await app.close() }
  })

  test('unbound and closed contract instances reject producer operations explicitly', async () => {
    await assert.rejects(new ExecutionQueue().echo.enqueue({ value: 'unbound' }), MqJobException)
    const { app, queue, fixture } = await application()
    await app.close()
    await assert.rejects(queue.echo.enqueue({ value: 'closed' }), MqJobException)
    expect(fixture.trace.resourceReleases).toBe(1)
  })
})
