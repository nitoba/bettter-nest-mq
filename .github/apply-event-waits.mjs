import assert from 'node:assert/strict'
import { readFileSync, writeFileSync } from 'node:fs'
const staged = new Map()
function patch(path, replacements) {
  let text = readFileSync(path, 'utf8')
  for (const [before, after] of replacements) {
    assert.equal(text.split(before).length - 1, 1, `Expected exactly one anchor: ${path}: ${before}`)
    text = text.replace(before, after)
  }
  staged.set(path, text)
}
patch('src/engine/job-client.ts', [
  ["import type { JobAwaitOptions } from 'better-effect-mq'\n", ''],
  ["import type { OperationStoreToken } from './operation-store.ts'\n", ''],
  [`    const wait: JobAwaitOptions<OperationStoreToken> =
      options.strategy === 'events'
        ? {
            strategy: 'events',
            eventStore: operationEventToken(connection),
            pollFallbackMs: options.pollFallbackMs ?? 5000,
            signal
          }
        : { signal, pollIntervalMs: options.pollIntervalMs ?? 100 }
    const result = await session.runOperation(() => job.awaitResult(id, wait))`,
   `    const result = await session.runOperation(() => options.strategy === 'events'
      ? job.awaitResult(id, { strategy: 'events', eventStore: operationEventToken(connection), pollFallbackMs: options.pollFallbackMs ?? 5000, signal })
      : job.awaitResult(id, { signal, pollIntervalMs: options.pollIntervalMs ?? 100 })
    )`]
])
patch('scripts/test-package.ts', [["      'execution-extensions'\n", "      'execution-extensions',\n      'event-waits'\n"]])
const tests = 'tests/integration/event-waits.test.ts'
let text = readFileSync(tests, 'utf8')
assert.ok(!text.includes('a missing wake hint'))
text = "import { Result } from 'better-result'\n" + text
text += `

test('a result completed before event registration is still returned without waiting for a future event', async () => {
  const source = fixture()
  const app = await application(source)
  try {
    app.get(Gate).release.resolve()
    const job = app.get(EventQueue).task
    const id = await job.enqueue('already done')
    await job.awaitResult(id, { timeoutMs: 1000, pollIntervalMs: 5 })
    expect(await job.awaitResult(id, eventOptions())).toBe('already done')
    expect(source.waits()).toBe(0)
  } finally { app.get(Gate).release.resolve(); await app.close() }
})

test('a missing wake hint falls back to the authoritative job without leaking a waiter', async () => {
  const source = fixture()
  const entered = Promise.withResolvers<void>()
  let active = 0
  Object.defineProperty(source.events, 'awaitEvents', { value: async (request: Parameters<typeof source.events.awaitEvents>[0]) => {
    assert.ok(request.signal)
    const signal = request.signal
    entered.resolve()
    active += 1
    try {
      await new Promise<void>((resolve) => {
        if (signal.aborted) resolve()
        else signal.addEventListener('abort', () => resolve(), { once: true })
      })
      return Result.ok(undefined)
    } finally { active -= 1 }
  } })
  const app = await application(source)
  try {
    const job = app.get(EventQueue).task
    const id = await job.enqueue('durable truth')
    const waiting = job.awaitResult(id, { strategy: 'events', pollFallbackMs: 20, timeoutMs: 1000 })
    await entered.promise
    app.get(Gate).release.resolve()
    expect(await waiting).toBe('durable truth')
    expect(active).toBe(0)
  } finally { app.get(Gate).release.resolve(); await app.close() }
})

test('a runtime event-reader failure degrades to bounded polling after a successful startup', async () => {
  const source = fixture()
  const entered = Promise.withResolvers<void>()
  const app = await application(source)
  Object.defineProperty(source.events, 'read', { value: async () => { entered.resolve(); throw new Error('Injected event reader failure') } })
  try {
    const job = app.get(EventQueue).task
    const id = await job.enqueue('fallback result')
    const waiting = job.awaitResult(id, { strategy: 'events', pollFallbackMs: 20, timeoutMs: 1000 })
    await entered.promise
    app.get(Gate).release.resolve()
    expect(await waiting).toBe('fallback result')
  } finally { app.get(Gate).release.resolve(); await app.close() }
})

test('application shutdown ends its admitted event wait without waiting for the caller deadline', async () => {
  const source = fixture()
  const app = await application(source, false)
  const job = app.get(EventQueue).task
  const id = await job.enqueue('survives application shutdown')
  const waiting = job.awaitResult(id, { strategy: 'events', pollFallbackMs: 1000, timeoutMs: 2000 })
  const rejected = assert.rejects(waiting, (cause) => {
    assert.ok(cause instanceof Error)
    assert.ok(!(cause instanceof JobWaitTimeoutException), 'Shutdown must not rely on the caller timeout')
    return true
  })
  await source.entered
  await app.close()
  await rejected
})

test('event-reader probe failures roll back initialization rather than activating an unusable reader', async () => {
  const source = fixture()
  Object.defineProperty(source.events, 'tailCursor', { value: async () => { throw new Error('Injected event-store probe failure') } })
  await assert.rejects(application(source, false))
})
`
staged.set(tests, text)
for (const [path, text] of staged) writeFileSync(path, text)
