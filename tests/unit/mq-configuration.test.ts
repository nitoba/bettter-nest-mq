import { describe, expect, test } from 'bun:test'

import { MqConfiguration } from '../../src/index.ts'

describe('MqConfiguration', () => {
  test('normalizes the default shutdown policy', () => {
    const configuration = new MqConfiguration({})
    expect(configuration.options.shutdown).toEqual({
      gracePeriodMs: 30_000,
      abortAfterGracePeriod: true
    })
  })

  test('supports a partial shutdown override', () => {
    const configuration = new MqConfiguration({ shutdown: { gracePeriodMs: 500 } })
    expect(configuration.options.shutdown).toEqual({
      gracePeriodMs: 500,
      abortAfterGracePeriod: true
    })
  })

  test('preserves zero grace and explicit false', () => {
    const configuration = new MqConfiguration({
      shutdown: { gracePeriodMs: 0, abortAfterGracePeriod: false }
    })
    expect(configuration.options.shutdown).toEqual({
      gracePeriodMs: 0,
      abortAfterGracePeriod: false
    })
  })

  test.each([-1, 0.5, Number.NaN, Number.POSITIVE_INFINITY, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid grace period %p',
    (gracePeriodMs) => {
      expect(() => new MqConfiguration({ shutdown: { gracePeriodMs } })).toThrow(RangeError)
    }
  )

  test('freezes resolved values without freezing or retaining caller-owned objects', () => {
    const options = { shutdown: { gracePeriodMs: 100 } }
    const configuration = new MqConfiguration(options)
    options.shutdown.gracePeriodMs = 200
    expect(configuration.options.shutdown.gracePeriodMs).toBe(100)
    expect(Object.isFrozen(configuration.options)).toBe(true)
    expect(Object.isFrozen(configuration.options.shutdown)).toBe(true)
    expect(Object.isFrozen(options.shutdown)).toBe(false)
  })
})
