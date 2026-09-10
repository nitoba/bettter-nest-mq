import { z } from 'zod'

import { QueueService, type FailureOf, type InputOf, type PayloadOf, type ResultOf } from '../../src/index.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

const timestamp = z.codec(z.iso.datetime(), z.date(), {
  decode: (value) => new Date(value),
  encode: (value) => value.toISOString()
})

class TypedQueue extends QueueService {
  readonly task = this.job({
    payload: zodCodec(z.object({ timestamp })),
    result: z.object({ key: z.string() }),
    failure: z.object({ code: z.literal('missing'), retryable: z.boolean() }),
    idempotencyKey: (payload) => payload.timestamp.toISOString(),
    retryable: (failure) => failure.retryable
  })

  readonly noFailure = this.job({ payload: z.string(), result: z.string() })
}

export const input: InputOf<TypedQueue['task']> = { timestamp: '2026-09-10T12:00:00.000Z' }
export const payload: PayloadOf<TypedQueue['task']> = { timestamp: new Date() }
export const result: ResultOf<TypedQueue['task']> = { key: 'file' }
export const failure: FailureOf<TypedQueue['task']> = { code: 'missing', retryable: false }

// @ts-expect-error Input remains the codec's wire representation.
export const wrongInput: InputOf<TypedQueue['task']> = { timestamp: new Date() }
// @ts-expect-error The worker receives a decoded Date.
export const wrongPayload: PayloadOf<TypedQueue['task']> = { timestamp: 'not decoded' }
// @ts-expect-error Results preserve their inferred contract.
export const wrongResult: ResultOf<TypedQueue['task']> = { key: 123 }
// @ts-expect-error Failure discriminants stay narrow.
export const wrongFailure: FailureOf<TypedQueue['task']> = { code: 'other', retryable: false }
// @ts-expect-error A job without a failure schema has no declared domain-failure type.
export const undeclaredFailure: FailureOf<TypedQueue['noFailure']> = 'error'

export async function verifyMethods(queue: TypedQueue): Promise<void> {
  await queue.task.parsePayload(input)
  await queue.task.encodePayload(payload)
  await queue.task.encodeResult(result)
  await queue.task.encodeFailure(failure)
  // @ts-expect-error Encoding decoded values does not accept a wire-format timestamp.
  await queue.task.encodePayload(input)
  // @ts-expect-error The engine-backed producer API is not a simulated stub in M1.
  await queue.task.enqueue(input)
}
