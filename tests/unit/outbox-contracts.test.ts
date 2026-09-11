import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { z } from 'zod'
import { Job, Queue, QueueService, getQueueDefinition, MqConfiguration, MqOutboxException, type PreparedJob } from '../../src/index.ts'
import { compileOutboxRecord, assertSameOutboxContent } from '../../src/engine/outbox-record.ts'

@Queue({ name: 'outbox-test', connection: 'target' })
class Jobs extends QueueService {
  @Job({ name: 'echo', version: 1 })
  readonly echo = this.job({ payload: z.string(), result: z.string() })
}
const jobs = getQueueDefinition(new Jobs()).jobs
const prepared: PreparedJob = {
  connection: 'target', request: {
    protocolVersion: 1, identity: { queue: 'outbox-test', name: 'echo', version: 1 },
    payload: '123', metadata: {}, priority: 0, runAt: 1_000, now: 1_000, attemptsMax: 1
  }
}

test('outbox ids produce stable per-source job ids without mutating the prepared request', async () => {
  const first = await compileOutboxRecord('source', { id: 'record', job: prepared }, jobs)
  const retry = await compileOutboxRecord('source', { id: 'record', job: prepared }, jobs)
  const other = await compileOutboxRecord('another-source', { id: 'record', job: prepared }, jobs)
  expect(first.request.id).toBe(retry.request.id)
  expect(first.request.id).not.toBe(other.request.id)
  expect(prepared.request.id).toBeUndefined()
  expect(first.request.payload).toBe('123')
  expect(first.attemptsMax).toBe(10)
  expect(first.request.attemptsMax).toBe(1)
})

test('explicit job ids and publication budgets remain independent of job execution policy', async () => {
  const record = await compileOutboxRecord('source', { id: 'record', job: { ...prepared, request: { ...prepared.request, id: 'explicit-job' } }, attempts: 3 }, jobs)
  expect(record.request.id).toBe('explicit-job')
  expect(record.attemptsMax).toBe(3)
  expect(record.request.attemptsMax).toBe(1)
})

test('unregistered target identities and invalid wire payloads are rejected before a transaction', async () => {
  await assert.rejects(compileOutboxRecord('source', { id: 'id', job: { ...prepared, connection: 'missing' } }, jobs), MqOutboxException)
  await assert.rejects(compileOutboxRecord('source', { id: 'id', job: { ...prepared, request: { ...prepared.request, payload: 1 } } }, jobs))
})

test('duplicate comparison includes route and dispatchKey, not just the upstream digest', async () => {
  const record = await compileOutboxRecord('source', { id: 'record', job: prepared }, jobs)
  assertSameOutboxContent(record, { ...record, updatedAtMs: record.updatedAtMs + 1 })
  expect(() => assertSameOutboxContent(record, { ...record, target: 'other' })).toThrow(MqOutboxException)
  expect(() => assertSameOutboxContent(record, { ...record, request: { ...record.request, dispatchKey: 'other' } })).toThrow(MqOutboxException)
  expect(() => assertSameOutboxContent(record, { ...record, attemptsMax: 20 })).toThrow(MqOutboxException)
})

test.each([0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY])('rejects invalid publication attempt counts %p', async (attempts) => {
  await assert.rejects(compileOutboxRecord('source', { id: 'record', job: prepared, attempts }, jobs), MqOutboxException)
})

test('publisher configuration is copied, validates timing and has independent role enablement', () => {
  const options = { concurrency: 2, leaseDurationMs: 300, heartbeatIntervalMs: 50 }
  const config = new MqConfiguration({ outbox: options, execution: { workers: false } })
  options.concurrency = 99
  expect(config.options.outbox.concurrency).toBe(2)
  expect(config.options.execution.outboxPublisher).toBe(true)
  expect(config.options.execution.workers).toBe(false)
  expect(Object.isFrozen(config.options.outbox)).toBe(true)
  expect(() => new MqConfiguration({ outbox: { leaseDurationMs: 100, heartbeatIntervalMs: 100 } })).toThrow(MqOutboxException)
  expect(() => new MqConfiguration({ outbox: { retryBaseDelayMs: 100, retryMaxDelayMs: 10 } })).toThrow(MqOutboxException)
})

test.each([0, -1, 0.5, Number.NaN, 2_147_483_648])('rejects unsafe publisher timer values %p', (pollIntervalMs) => {
  expect(() => new MqConfiguration({ outbox: { pollIntervalMs } })).toThrow(MqOutboxException)
})
