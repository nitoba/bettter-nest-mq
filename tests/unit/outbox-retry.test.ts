import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import {
  makeOutboxId,
  makeOutboxRecord,
  validateOutboxRecord,
  MemoryOutboxStore
} from 'better-effect-mq-outbox'
import { makePreparedEnqueue } from 'better-effect-mq'
import { Result } from 'better-result'
import { MqOutboxException } from '../../src/index.ts'
import { copyOutboxRetryOptions } from '../../src/outbox/retry-options.ts'
import type { OutboxRetryOptions } from '../../src/outbox/retry-options.ts'
import {
  retryOutboxRecord,
  OutboxRetryAdapter,
  bindOutboxRetry,
  closeOutboxRetry,
  outboxRetryAdapter
} from '../../src/engine/outbox-retry.ts'

function valueOf<Value, Failure>(result: Result<Value, Failure>): Value {
  if (Result.isError(result)) throw result.error
  return result.value
}
function failed() {
  const pending = valueOf(
    makeOutboxRecord({
      id: valueOf(makeOutboxId('publication')),
      target: 'target',
      attemptsMax: 2,
      nowMs: 100,
      request: valueOf(
        makePreparedEnqueue({
          protocolVersion: 1,
          identity: { queue: 'reports', name: 'generate', version: 1 },
          id: 'original-job',
          dispatchKey: 'tenant',
          payload: { text: '123' },
          metadata: {},
          priority: 0,
          attemptsMax: 4,
          runAt: 100,
          now: 100
        })
      )
    })
  )
  return valueOf(
    validateOutboxRecord({
      ...pending,
      state: 'failed',
      attemptsMade: 2,
      updatedAtMs: 200,
      failure: {
        kind: 'target-missing',
        message: 'Route was absent',
        retryable: false,
        recordedAtMs: 200
      }
    })
  )
}
function options(): OutboxRetryOptions {
  return { expected: { updatedAtMs: 200, attemptsMade: 2, attemptsMax: 2 }, attempts: 3 }
}

test('administrative retry preserves content, history and job budget while granting publication attempts', () => {
  const before = failed()
  const next = retryOutboxRecord(before, copyOutboxRetryOptions(options()), 300)
  expect(next.state).toBe('pending')
  expect(next.attemptsMade).toBe(2)
  expect(next.attemptsMax).toBe(5)
  expect(next.failure).toEqual(before.failure)
  expect(next.request).toEqual(before.request)
  expect(next.requestDigest).toBe(before.requestDigest)
  expect(next.createdAtMs).toBe(100)
  expect(next.updatedAtMs).toBe(300)
  expect(next.runAtMs).toBe(300)
  expect(next.leaseToken).toBeUndefined()
  expect(before.state).toBe('failed')
})

test('retry advances the optimistic timestamp even with a frozen or backward application clock', () => {
  const next = retryOutboxRecord(failed(), options(), 100)
  expect(next.updatedAtMs).toBe(201)
  expect(next.runAtMs).toBe(201)
})

test('publication can be delayed independently from the immutable job runAt', () => {
  const next = retryOutboxRecord(failed(), { ...options(), runAtMs: 1000 }, 300)
  expect(next.runAtMs).toBe(1000)
  expect(next.request.runAt).toBe(100)
})

test('a permanent early failure receives the requested remaining budget, not its unused previous budget', () => {
  const before = valueOf(validateOutboxRecord({ ...failed(), attemptsMax: 10, attemptsMade: 1 }))
  const next = retryOutboxRecord(
    before,
    { expected: { updatedAtMs: 200, attemptsMade: 1, attemptsMax: 10 }, attempts: 2 },
    300
  )
  expect(next.attemptsMax).toBe(3)
  expect(next.attemptsMade).toBe(1)
})

test.each(['pending', 'active', 'published'] as const)(
  'retry never resets a %s publication',
  (state) => {
    expect(() => retryOutboxRecord({ ...failed(), state }, options(), 300)).toThrow(
      MqOutboxException
    )
  }
)

test.each(['updatedAtMs', 'attemptsMade', 'attemptsMax'] as const)(
  'each expected %s value fences stale requests',
  (field) => {
    const selected = options()
    expect(() =>
      retryOutboxRecord(
        failed(),
        { ...selected, expected: { ...selected.expected, [field]: selected.expected[field] + 1 } },
        300
      )
    ).toThrow(MqOutboxException)
  }
)

test.each([0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY])(
  'invalid retry budget %p rejects before storage',
  (attempts) => {
    expect(() => copyOutboxRetryOptions({ ...options(), attempts })).toThrow(MqOutboxException)
  }
)

test('retry rejects unsafe sums and malformed failed leases instead of repairing records', () => {
  expect(() =>
    retryOutboxRecord(failed(), { ...options(), attempts: Number.MAX_SAFE_INTEGER }, 300)
  ).toThrow(MqOutboxException)
  const record = { ...failed(), updatedAtMs: Number.MAX_SAFE_INTEGER }
  expect(() =>
    retryOutboxRecord(
      record,
      { ...options(), expected: { ...options().expected, updatedAtMs: record.updatedAtMs } },
      300
    )
  ).toThrow(MqOutboxException)
  const invalid = JSON.parse(JSON.stringify({ ...failed(), leaseToken: 'not-a-failed-lease' }))
  expect(() => retryOutboxRecord(invalid, options(), 300)).toThrow(MqOutboxException)
})

test('options are copied and accessors/unknown ownership fields fail without being invoked', () => {
  const input = { expected: { updatedAtMs: 200, attemptsMade: 2, attemptsMax: 2 }, attempts: 3 }
  const copied = copyOutboxRetryOptions(input)
  input.expected.attemptsMade = 1
  input.attempts = 99
  expect(copied.expected.attemptsMade).toBe(2)
  expect(copied.attempts).toBe(3)
  expect(Object.isFrozen(copied.expected)).toBe(true)
  let invoked = false
  expect(() =>
    copyOutboxRetryOptions({
      get expected() {
        invoked = true
        return input.expected
      },
      attempts: 1
    })
  ).toThrow(MqOutboxException)
  expect(invoked).toBe(false)
  expect(() =>
    copyOutboxRetryOptions(
      JSON.parse(
        '{"attempts":1,"expected":{"updatedAtMs":1,"attemptsMade":1,"attemptsMax":1},"force":true}'
      )
    )
  ).toThrow(MqOutboxException)
})

test('adapter closure rejects new writes and drains already admitted failures without hiding them', async () => {
  const gate = Promise.withResolvers<void>()
  const failure = new Error('uncertain network response')
  let calls = 0
  const adapter = new OutboxRetryAdapter(async () => {
    calls += 1
    await gate.promise
    throw failure
  })
  const before = failed()
  const next = retryOutboxRecord(before, options(), 300)
  const writing = adapter.retry(before, next)
  const rejected = assert.rejects(writing, (cause) => cause === failure)
  let closed = false
  const closing = adapter.close().then(() => {
    closed = true
  })
  await assert.rejects(adapter.retry(before, next), MqOutboxException)
  expect(closed).toBe(false)
  gate.resolve()
  await rejected
  await closing
  expect(closed).toBe(true)
  expect(calls).toBe(1)
})

test('retry capability is scoped to acquired stores and is detached at shutdown', async () => {
  const store = MemoryOutboxStore.make()
  expect(() => outboxRetryAdapter(store)).toThrow(MqOutboxException)
  const adapter = bindOutboxRetry(store, async () => {})
  expect(outboxRetryAdapter(store)).toBe(adapter)
  expect(() => bindOutboxRetry(store, async () => {})).toThrow(MqOutboxException)
  await closeOutboxRetry(store)
  expect(() => outboxRetryAdapter(store)).toThrow(MqOutboxException)
  await assert.rejects(adapter.retry(failed(), failed()), MqOutboxException)
})
