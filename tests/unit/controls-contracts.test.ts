import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { z } from 'zod'
import {
  ContractDefinitionException,
  getQueueDefinition,
  Job,
  MqConfiguration,
  Queue,
  QueueControls,
  QueueService
} from '../../src/index.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

const limits = {
  globalConcurrency: 4,
  perKeyConcurrency: 1,
  rateLimit: { max: 10, durationMs: 1_000 }
}
@Queue({ name: 'controlled' })
@QueueControls(limits)
class ControlledQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({
    payload: z.object({ tenant: z.string() }),
    result: z.string(),
    dispatchKey: (payload) => payload.tenant
  })
}

test('queue control metadata is copied and deeply immutable without mutating caller configuration', () => {
  limits.rateLimit.max = 99
  const definition = getQueueDefinition(new ControlledQueue())
  expect(definition.controls).toEqual({
    globalConcurrency: 4,
    perKeyConcurrency: 1,
    rateLimit: { max: 10, durationMs: 1_000 }
  })
  expect(definition.jobs[0]?.controls).toEqual(definition.controls)
  expect(Object.isFrozen(definition.controls?.rateLimit)).toBe(true)
  expect(Object.isFrozen(limits)).toBe(false)
})

test('a concrete queue subclass does not silently inherit an operational policy', () => {
  @Queue({ name: 'another-queue' })
  class Derived extends ControlledQueue {}
  expect(getQueueDefinition(new Derived()).controls).toBeUndefined()
  expect(getQueueDefinition(new ControlledQueue()).controls?.perKeyConcurrency).toBe(1)
})

test.each([0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
  'rejects invalid positive limits %p',
  (value) => {
    expect(() => QueueControls({ globalConcurrency: value })).toThrow(ContractDefinitionException)
    expect(() => QueueControls({ perKeyConcurrency: value })).toThrow(ContractDefinitionException)
    expect(() => QueueControls({ rateLimit: { max: value, durationMs: 10 } })).toThrow(
      ContractDefinitionException
    )
    expect(() => QueueControls({ rateLimit: { max: 1, durationMs: value } })).toThrow(
      ContractDefinitionException
    )
  }
)

test('rejects an empty policy and duplicate decorators rather than guessing defaults', () => {
  expect(() => QueueControls({})).toThrow(ContractDefinitionException)
  expect(() => {
    @QueueControls({ globalConcurrency: 1 })
    @QueueControls({ globalConcurrency: 2 })
    class Duplicate extends ControlledQueue {}
    return Duplicate
  }).toThrow(ContractDefinitionException)
})

test.each(['', ' ', '__none__', ' leading', 'trailing ', 'nul\0key', 'x'.repeat(513)])(
  'rejects invalid or reserved derived dispatch keys %p',
  (key) => {
    class Invalid extends QueueService {
      readonly task = this.job({ payload: z.string(), result: z.string(), dispatchKey: () => key })
    }
    expect(() => new Invalid().task.getDispatchKey('value')).toThrow(ContractDefinitionException)
  }
)

test('the dispatch-key callback receives decoded codec output, not the input representation', async () => {
  const timestamp = z.codec(z.iso.datetime(), z.date(), {
    decode: (text) => new Date(text),
    encode: (date) => date.toISOString()
  })
  class Dates extends QueueService {
    readonly task = this.job({
      payload: zodCodec(timestamp),
      result: z.string(),
      dispatchKey: (date) => date.toISOString()
    })
  }
  const job = new Dates().task
  expect(job.getDispatchKey(await job.parsePayload('2026-09-10T12:00:00.000Z'))).toBe(
    '2026-09-10T12:00:00.000Z'
  )
})

test('normal replicas default to read-only policy validation', () => {
  expect(new MqConfiguration({}).options.controls).toEqual({
    mode: 'validate',
    group: 'nestjs/queue-controls'
  })
  expect(
    new MqConfiguration({ controls: { mode: 'reconcile', group: 'deploy' } }).options.controls
  ).toEqual({ mode: 'reconcile', group: 'deploy' })
  assert.throws(
    () => new MqConfiguration({ controls: { group: 'x'.repeat(129) } }),
    ContractDefinitionException
  )
})
