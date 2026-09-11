import { expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import { z } from 'zod'
import {
  Job,
  Queue,
  QueueControls,
  QueueService,
  Schedule,
  MqConfiguration,
  MqScheduleException,
  getQueueDefinition
} from '../../src/index.ts'
import { scheduleMetadata } from '../../src/schedules/decorator.ts'
import { compileSchedules } from '../../src/engine/schedule-compiler.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

const payload = { label: 'original' }
@Queue({ name: 'scheduled' })
class ScheduledQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  @Schedule({ key: 'daily', cron: '0 9 * * *', timeZone: 'America/Fortaleza', payload })
  @Schedule({ key: 'hourly', everyMs: 3_600_000, payload: { label: 'hourly' } })
  readonly task = this.job({ payload: z.object({ label: z.string() }), result: z.string() })
}

test('repeatable schedule declarations copy JSON input without freezing the caller', async () => {
  payload.label = 'changed'
  const queue = new ScheduledQueue()
  const definitions = await compileSchedules(getQueueDefinition(queue).jobs, { group: 'app' })
  expect(definitions).toHaveLength(2)
  expect(definitions.find((entry) => entry.schedule.key === 'daily')?.encoded).toEqual({
    label: 'original'
  })
  expect(Object.isFrozen(payload)).toBe(false)
  expect(scheduleMetadata(queue, 'task')).toHaveLength(2)
})

test.each(['', 'bad', '* * *', '* * * * * *', '99 * * * *'])(
  'rejects invalid five-field cron %p',
  (cron) => {
    expect(() => Schedule({ key: 'bad', cron, payload: null })).toThrow(MqScheduleException)
  }
)
test.each([0, -1, 0.5, Number.NaN, Number.POSITIVE_INFINITY])(
  'rejects invalid interval %p',
  (everyMs) => {
    expect(() => Schedule({ key: 'bad', everyMs, payload: null })).toThrow(MqScheduleException)
  }
)
test('rejects invalid timezone, empty keys and invalid missed-occurrence policies', () => {
  expect(() =>
    Schedule({ key: 'bad', cron: '0 0 * * *', timeZone: 'not-a-zone', payload: null })
  ).toThrow(MqScheduleException)
  expect(() => Schedule({ key: '', everyMs: 10, payload: null })).toThrow(MqScheduleException)
  expect(() =>
    Schedule({
      key: 'bad',
      everyMs: 10,
      payload: null,
      misfire: { strategy: 'catch-up', maxOccurrences: 0 }
    })
  ).toThrow(MqScheduleException)
})
test('duplicate addresses are rejected across different job properties before persistence', async () => {
  @Queue({ name: 'duplicate' })
  class Duplicate extends QueueService {
    @Job({ name: 'one', version: 1 })
    @Schedule({ key: 'same', everyMs: 1_000, payload: 'a' })
    readonly first = this.job({ payload: z.string(), result: z.string() })
    @Job({ name: 'two', version: 1 })
    @Schedule({ key: 'same', everyMs: 1_000, payload: 'b' })
    readonly second = this.job({ payload: z.string(), result: z.string() })
  }
  await assert.rejects(
    compileSchedules(getQueueDefinition(new Duplicate()).jobs),
    MqScheduleException
  )
})
test('inherited job schedules follow metadata without changing the base declaration', async () => {
  @Queue({ name: 'derived' })
  class Derived extends ScheduledQueue {}
  expect(await compileSchedules(getQueueDefinition(new Derived()).jobs)).toHaveLength(2)
  expect(scheduleMetadata(new ScheduledQueue(), 'task')).toHaveLength(2)
})
test('schedule payloads are checked through explicit codecs before encoding', async () => {
  const timestamp = z.codec(z.iso.datetime(), z.date(), {
    decode: (value) => new Date(value),
    encode: (date) => date.toISOString()
  })
  @Queue({ name: 'dates' })
  class Dates extends QueueService {
    @Job({ name: 'task', version: 1 })
    @Schedule({ key: 'date', everyMs: 1_000, payload: { timestamp: '2026-09-11T12:00:00.000Z' } })
    readonly task = this.job({ payload: zodCodec(z.object({ timestamp })), result: z.string() })
  }
  const compiled = await compileSchedules(getQueueDefinition(new Dates()).jobs)
  expect(compiled[0]?.encoded).toEqual({ timestamp: '2026-09-11T12:00:00.000Z' })
})
test('per-key queues fail clearly because the pinned schedule protocol has no dispatch-key field', async () => {
  @Queue({ name: 'keyed' })
  @QueueControls({ perKeyConcurrency: 1 })
  class Keyed extends QueueService {
    @Job({ name: 'task', version: 1 })
    @Schedule({ key: 'key', everyMs: 10, payload: 'tenant' })
    readonly task = this.job({
      payload: z.string(),
      result: z.string(),
      dispatchKey: (value) => value
    })
  }
  await assert.rejects(compileSchedules(getQueueDefinition(new Keyed()).jobs), MqScheduleException)
})
test('default schedule registration validates only and scheduler configuration is immutable', () => {
  const options = new MqConfiguration({}).options
  expect(options.schedules.mode).toBe('validate')
  expect(options.schedules.group).toBe('nestjs/schedules')
  expect(options.execution.scheduler).toBe(true)
  expect(Object.isFrozen(options.schedules)).toBe(true)
})
