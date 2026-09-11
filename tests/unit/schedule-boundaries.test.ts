import { expect, test } from 'bun:test'
import { Schedule, MqConfiguration, MqScheduleException } from '../../src/index.ts'

test.each([2_147_483_648, Number.MAX_SAFE_INTEGER])('scheduler timer values cannot overflow the JavaScript timer range: %p', (value) => {
  expect(() => new MqConfiguration({ schedules: { sweepIntervalMs: value } })).toThrow(MqScheduleException)
  expect(() => new MqConfiguration({ schedules: { retryDelayMs: value } })).toThrow(MqScheduleException)
})
test('a persistent interval is not itself a timer and can span longer periods', () => {
  expect(() => Schedule({ key: 'long-cadence', everyMs: 3_000_000_000, payload: null })).not.toThrow()
})
test('unknown schedule and misfire options are not silently discarded for JavaScript callers', () => {
  expect(() => Schedule(JSON.parse('{"key":"k","everyMs":1000,"payload":null,"everySecond":true}'))).toThrow(MqScheduleException)
  expect(() => Schedule(JSON.parse('{"key":"k","everyMs":1000,"payload":null,"misfire":{"strategy":"skip","maxOccurrences":5}}'))).toThrow(MqScheduleException)
  const configuration = JSON.parse('{"schedules":{"group":"g","mode":"validate","autoMigrate":true}}')
  expect(() => new MqConfiguration(configuration)).toThrow(MqScheduleException)
})
test('schedule option accessors are rejected before any getter is evaluated', () => {
  let invoked = false
  const options = { key: 'getter', everyMs: 1000, get payload() { invoked = true; return null } }
  expect(() => Schedule(options)).toThrow(MqScheduleException)
  expect(invoked).toBe(false)
})
test('catch-up limits respect the pinned store protocol maximum rather than failing every tick', () => {
  expect(() => Schedule({ key: 'too-many', everyMs: 10, payload: null, misfire: { strategy: 'catch-up', maxOccurrences: 257 } })).toThrow(MqScheduleException)
  expect(() => Schedule({ key: 'max', everyMs: 10, payload: null, misfire: { strategy: 'catch-up', maxOccurrences: 256 } })).not.toThrow()
})
