import { describe, expect, test } from 'bun:test'
import { z } from 'zod'

import {
  ContractDefinitionException,
  getQueueDefinition,
  Job,
  JobTimeout,
  Queue,
  QueueService,
  resolveJobPolicy,
  Retry
} from '../../src/index.ts'

@Queue({ name: 'reports', connection: 'primary', defaults: { priority: 4, timeoutMs: 5_000 } })
class Reports extends QueueService {
  @Job({ name: 'generate', version: 1, defaults: { delayMs: 10 } })
  @Retry({
    attempts: 4,
    backoff: { type: 'exponential', initialDelayMs: 100, factor: 2, maxDelayMs: 5_000, jitter: 0.2 }
  })
  @JobTimeout(1_000)
  readonly generate = this.job({ payload: z.string(), result: z.string() })
}

describe('queue metadata and identities', () => {
  test('compiles inert, immutable definitions with module/queue/job/decorator precedence', () => {
    const definition = getQueueDefinition(new Reports(), {
      priority: 1,
      timeoutMs: 9_000,
      delayMs: 20
    })
    expect(definition.name).toBe('reports')
    expect(definition.connection).toBe('primary')
    expect(definition.jobs[0]?.property).toBe('generate')
    expect(definition.jobs[0]?.identity).toEqual({
      connection: 'primary',
      queue: 'reports',
      name: 'generate',
      version: 1,
      key: JSON.stringify(['primary', 'reports', 'generate', 1])
    })
    expect(definition.jobs[0]?.policy).toMatchObject({
      priority: 4,
      timeoutMs: 1_000,
      delayMs: 10,
      retry: { attempts: 4 }
    })
    expect(Object.isFrozen(definition)).toBe(true)
    expect(Object.isFrozen(definition.jobs)).toBe(true)
    expect(Object.isFrozen(definition.jobs[0]?.policy.retry)).toBe(true)
  })

  test('uses a stable key independent of class and property names', () => {
    @Queue({ name: 'reports', connection: 'primary' })
    class Renamed extends QueueService {
      @Job({ name: 'generate', version: 1 })
      readonly renamed = this.job({ payload: z.string(), result: z.string() })
    }
    expect(getQueueDefinition(new Renamed()).jobs[0]?.identity.key).toBe(
      getQueueDefinition(new Reports()).jobs[0]?.identity.key
    )
  })

  test('allows two versions of the same job in one queue', () => {
    @Queue({ name: 'versioned' })
    class Versioned extends QueueService {
      @Job({ name: 'generate', version: 1 })
      readonly old = this.job({ payload: z.string(), result: z.string() })
      @Job({ name: 'generate', version: 2 })
      readonly current = this.job({ payload: z.string(), result: z.string() })
    }
    const definitions = getQueueDefinition(new Versioned()).jobs
    expect(definitions).toHaveLength(2)
    expect(definitions[0]?.identity.key).not.toBe(definitions[1]?.identity.key)
  })

  test('requires every concrete queue class to declare its own identity', () => {
    class Missing extends Reports {}
    expect(() => getQueueDefinition(new Missing())).toThrow(ContractDefinitionException)
  })

  test('inherits job metadata without mutating the base class', () => {
    @Queue({ name: 'derived', connection: 'secondary' })
    class Derived extends Reports {
      @Retry({ attempts: 2 })
      override readonly generate = this.job({ payload: z.string(), result: z.string() })
    }
    expect(getQueueDefinition(new Derived()).jobs[0]?.policy.retry.attempts).toBe(2)
    expect(getQueueDefinition(new Reports()).jobs[0]?.policy.retry.attempts).toBe(4)
    expect(getQueueDefinition(new Derived()).jobs[0]?.identity.connection).toBe('secondary')
  })

  test('rejects descriptors missing @Job', () => {
    @Queue({ name: 'missing' })
    class Missing extends QueueService {
      readonly task = this.job({ payload: z.string(), result: z.string() })
    }
    expect(() => getQueueDefinition(new Missing())).toThrow('Missing @Job')
  })

  test('rejects @Job on a non-job value', () => {
    @Queue({ name: 'invalid' })
    class Invalid extends QueueService {
      @Job({ name: 'task', version: 1 })
      readonly task = 'not a descriptor'
    }
    expect(() => getQueueDefinition(new Invalid())).toThrow(ContractDefinitionException)
  })

  test('rejects accessors without invoking the getter', () => {
    const instance = new Reports()
    let called = false
    Object.defineProperty(instance, 'generate', {
      get: () => {
        called = true
        throw new Error('getter ran')
      }
    })
    expect(() => getQueueDefinition(instance)).toThrow(ContractDefinitionException)
    expect(called).toBe(false)
  })

  test('rejects duplicate durable identities inside a queue', () => {
    @Queue({ name: 'duplicate' })
    class Duplicate extends QueueService {
      @Job({ name: 'task', version: 1 })
      readonly first = this.job({ payload: z.string(), result: z.string() })
      @Job({ name: 'task', version: 1 })
      readonly second = this.job({ payload: z.string(), result: z.string() })
    }
    expect(() => getQueueDefinition(new Duplicate())).toThrow('Duplicate job identity')
  })

  test('rejects duplicate decorators on one property', () => {
    expect(() => {
      class Duplicate extends QueueService {
        @Job({ name: 'a', version: 1 })
        @Job({ name: 'b', version: 1 })
        readonly task = this.job({ payload: z.string(), result: z.string() })
      }
      return new Duplicate()
    }).toThrow(ContractDefinitionException)
  })

  test('copies policy configuration instead of retaining caller-owned objects', () => {
    const retry = { attempts: 3, backoff: { type: 'fixed' as const, delayMs: 100 } }
    @Queue({ name: 'copy' })
    class Copy extends QueueService {
      @Job({ name: 'task', version: 1 })
      @Retry(retry)
      readonly task = this.job({ payload: z.string(), result: z.string() })
    }
    retry.attempts = 9
    retry.backoff.delayMs = 999
    expect(getQueueDefinition(new Copy()).jobs[0]?.policy.retry).toEqual({
      attempts: 3,
      backoff: { type: 'fixed', delayMs: 100 }
    })
    expect(Object.isFrozen(retry)).toBe(false)
  })

  test.each([0, -1, 1.5, Number.NaN, Number.MAX_SAFE_INTEGER + 1])(
    'rejects invalid job version %p',
    (version) => {
      expect(() => Job({ name: 'task', version })).toThrow(ContractDefinitionException)
    }
  )

  test.each(['', ' ', ' leading', 'trailing '])('rejects ambiguous queue names %p', (name) => {
    expect(() => Queue({ name })).toThrow(ContractDefinitionException)
  })
})

describe('policy resolution', () => {
  test('replaces a retry policy as a unit instead of mixing backoff variants', () => {
    const policy = resolveJobPolicy(
      { retry: { attempts: 5, backoff: { type: 'exponential', initialDelayMs: 100, factor: 2 } } },
      { retry: { attempts: 2, backoff: { type: 'fixed', delayMs: 10 } } }
    )
    expect(policy.retry).toEqual({ attempts: 2, backoff: { type: 'fixed', delayMs: 10 } })
  })

  test('preserves explicit zero values', () => {
    expect(
      resolveJobPolicy({ priority: 5, delayMs: 10 }, { priority: 0, delayMs: 0 })
    ).toMatchObject({ priority: 0, delayMs: 0 })
  })

  test.each([0, -1, 0.5, Number.POSITIVE_INFINITY])(
    'rejects invalid attempt counts %p',
    (attempts) => {
      expect(() => Retry({ attempts })).toThrow(ContractDefinitionException)
    }
  )

  test.each([-1, 0.5, Number.NaN])('rejects invalid timeouts %p', (timeoutMs) => {
    expect(() => JobTimeout(timeoutMs)).toThrow(ContractDefinitionException)
  })

  test.each([-0.1, 1.1, Number.NaN])('rejects invalid jitter %p', (jitter) => {
    expect(() =>
      Retry({
        attempts: 2,
        backoff: { type: 'exponential', initialDelayMs: 100, factor: 2, jitter }
      })
    ).toThrow(ContractDefinitionException)
  })

  test('supports linear and named/versioned custom policy declarations', () => {
    expect(
      resolveJobPolicy({
        retry: { attempts: 3, backoff: { type: 'linear', initialDelayMs: 10, incrementMs: 5 } }
      }).retry.attempts
    ).toBe(3)
    expect(
      resolveJobPolicy({
        retry: { attempts: 3, backoff: { type: 'custom', policy: 'provider-retry', version: 1 } }
      }).retry.backoff
    ).toEqual({ type: 'custom', policy: 'provider-retry', version: 1 })
  })
})
