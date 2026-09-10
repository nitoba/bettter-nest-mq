import { describe, expect, test } from 'bun:test'
import assert from 'node:assert/strict'
import type { StandardSchemaV1 } from '@standard-schema/spec'
import { z } from 'zod'

import {
  decodeSchema,
  defineCodec,
  encodeSchema,
  JobFailureException,
  QueueService,
  SchemaDefectException,
  SchemaEncodingException,
  SchemaValidationException,
  validateSchema
} from '../../src/index.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

const dateSchema = z.codec(z.iso.datetime(), z.date(), {
  decode: (text) => new Date(text),
  encode: (date) => date.toISOString()
})

describe('Standard Schema boundaries', () => {
  test('validates ordinary Zod payloads and preserves issue paths', async () => {
    const schema = z.object({ count: z.int().positive() })
    expect(await validateSchema(schema, { count: 2 })).toEqual({ count: 2 })
    await assert.rejects(validateSchema(schema, { count: -1 }), (error) => {
      assert.ok(error instanceof SchemaValidationException)
      expect(error.issues[0]?.path).toEqual(['count'])
      return true
    })
  })

  test('supports asynchronous Standard Schema vendors without Zod-specific assumptions', async () => {
    const schema: StandardSchemaV1<string, number> = {
      '~standard': {
        version: 1,
        vendor: 'test-vendor',
        validate: async (input) =>
          input === 'seven' ? { value: 7 } : { issues: [{ message: 'Expected seven' }] }
      }
    }
    expect(await validateSchema(schema, 'seven')).toBe(7)
    const contract = defineCodec(schema, async () => 'seven')
    expect(await encodeSchema(contract, 7)).toBe('"seven"')
    expect(await decodeSchema(contract, '"seven"')).toBe(7)
    await assert.rejects(validateSchema(schema, 'other'), SchemaValidationException)
  })

  test('keeps throwing validators distinct from validation failures', async () => {
    const cause = new Error('vendor failed')
    const schema: StandardSchemaV1<string> = {
      '~standard': {
        version: 1,
        vendor: 'throwing-vendor',
        validate: () => {
          throw cause
        }
      }
    }
    await assert.rejects(validateSchema(schema, 'value'), (error) => {
      assert.ok(error instanceof SchemaDefectException)
      expect(error.cause).toBe(cause)
      return true
    })
  })

  test('does not silently transform an already decoded value twice', async () => {
    const schema = z.number().transform((value) => value + 1)
    const decoded = await validateSchema(schema, 3)
    expect(decoded).toBe(4)
    await assert.rejects(encodeSchema(schema, decoded), SchemaEncodingException)
    const contract = defineCodec(schema, (value) => value - 1)
    expect(await decodeSchema(contract, await encodeSchema(contract, decoded))).toBe(4)
  })

  test('round-trips nested Zod codecs through JSON', async () => {
    const contract = zodCodec(z.object({ occurredAt: dateSchema }))
    const value = { occurredAt: new Date('2026-09-10T12:00:00.000Z') }
    const encoded = await encodeSchema(contract, value)
    expect(encoded).toBe('{"occurredAt":"2026-09-10T12:00:00.000Z"}')
    expect(await decodeSchema(contract, encoded)).toEqual(value)
  })

  test('accepts asynchronous codec encoders', async () => {
    const contract = defineCodec(dateSchema, async (date) => date.toISOString())
    const value = new Date('2026-09-10T12:00:00.000Z')
    expect(await decodeSchema(contract, await encodeSchema(contract, value))).toEqual(value)
  })

  test('rejects lossy codecs even when their wire value validates', async () => {
    const contract = defineCodec(z.number(), (value) => Math.floor(value))
    await assert.rejects(encodeSchema(contract, 1.5), SchemaEncodingException)
  })

  test('preserves encoder failures as causes', async () => {
    const cause = new Error('cannot encode')
    const contract = defineCodec(z.string(), () => {
      throw cause
    })
    await assert.rejects(encodeSchema(contract, 'value'), (error) => {
      assert.ok(error instanceof SchemaDefectException)
      expect(error.cause).toBe(cause)
      return true
    })
  })

  test('rejects one-way Zod transforms on the Zod encode path', async () => {
    const contract = zodCodec(z.string().transform((value) => value.length))
    await assert.rejects(encodeSchema(contract, 3), SchemaDefectException)
  })

  test('rejects corrupted JSON independently from schema-invalid JSON', async () => {
    await assert.rejects(decodeSchema(z.string(), '{'), SchemaEncodingException)
    await assert.rejects(decodeSchema(z.string(), '3'), SchemaValidationException)
  })

  test('validates persisted results again when decoding', async () => {
    await assert.rejects(
      decodeSchema(z.object({ count: z.int() }), '{"count":"x"}'),
      SchemaValidationException
    )
  })

  test('rejects Date without an explicit encoder', async () => {
    await assert.rejects(encodeSchema(z.date(), new Date()), SchemaEncodingException)
  })

  test.each([Number.NaN, Number.POSITIVE_INFINITY, undefined, 1n, -0])(
    'rejects JSON values that would disappear, throw or change: %p',
    async (value) => {
      await assert.rejects(encodeSchema(z.any(), value), SchemaEncodingException)
    }
  )

  test('rejects nested undefined and cyclic values', async () => {
    await assert.rejects(encodeSchema(z.any(), { value: undefined }), SchemaEncodingException)
    const cyclic: { child?: object } = {}
    cyclic.child = cyclic
    await assert.rejects(encodeSchema(z.any(), cyclic), SchemaEncodingException)
  })

  test('allows ordinary JSON primitives, arrays and objects', async () => {
    const schema = z.object({ name: z.string(), flags: z.array(z.boolean()), extra: z.null() })
    const value = { name: 'reports', flags: [true, false], extra: null }
    expect(await decodeSchema(schema, await encodeSchema(schema, value))).toEqual(value)
  })
})

describe('inert job definitions', () => {
  class Reports extends QueueService {
    readonly generate = this.job({
      payload: z.object({ id: z.string() }),
      result: z.object({ key: z.string() }),
      failure: z.object({ retryable: z.boolean() }),
      idempotencyKey: (payload) => payload.id,
      retryable: (failure) => failure.retryable
    })
  }

  test('validates and round-trips payload, result and known failure contracts', async () => {
    const job = new Reports().generate
    expect(await job.parsePayload({ id: 'request-1' })).toEqual({ id: 'request-1' })
    expect(await job.decodePayload(await job.encodePayload({ id: 'request-1' }))).toEqual({
      id: 'request-1'
    })
    expect(await job.decodeResult(await job.encodeResult({ key: 'file-1' }))).toEqual({
      key: 'file-1'
    })
    expect(await job.decodeFailure(await job.encodeFailure({ retryable: true }))).toEqual({
      retryable: true
    })
    expect(job.getIdempotencyKey({ id: 'request-1' })).toBe('request-1')
    expect(job.canRetry({ retryable: false })).toBe(false)
    expect(job.canRetry({ retryable: true })).toBe(true)
    expect(Object.isFrozen(job.schemas)).toBe(true)
    expect('enqueue' in job).toBe(false)
  })

  test('rejects invalid persisted failure content', async () => {
    await assert.rejects(
      new Reports().generate.decodeFailure('{"retryable":1}'),
      SchemaValidationException
    )
  })

  test('keeps known failure content and cause without claiming checked exceptions', () => {
    const cause = new Error('upstream')
    const error = new JobFailureException({ code: 'unavailable' }, { cause })
    expect(error.failure).toEqual({ code: 'unavailable' })
    expect(error.cause).toBe(cause)
    expect(error).toBeInstanceOf(Error)
  })

  test('rejects empty generated idempotency keys', () => {
    class Invalid extends QueueService {
      readonly task = this.job({
        payload: z.string(),
        result: z.string(),
        idempotencyKey: () => ''
      })
    }
    expect(() => new Invalid().task.getIdempotencyKey('x')).toThrow('idempotency')
  })
})
