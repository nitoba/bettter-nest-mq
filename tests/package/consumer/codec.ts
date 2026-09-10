import assert from 'node:assert/strict'
import { z } from 'zod'
import {
  decodeSchema,
  encodeSchema,
  QueueService,
  type InputOf,
  type PayloadOf
} from 'better-nest-mq'
import { zodCodec } from 'better-nest-mq/zod'

const timestamp = z.codec(z.iso.datetime(), z.date(), {
  decode: (value) => new Date(value),
  encode: (value) => value.toISOString()
})
const contract = zodCodec(z.object({ timestamp }))
class Events extends QueueService {
  readonly ingest = this.job({ payload: contract, result: z.object({ accepted: z.boolean() }) })
}
const input: InputOf<Events['ingest']> = { timestamp: '2026-09-10T12:00:00.000Z' }
const value: PayloadOf<Events['ingest']> = { timestamp: new Date(input.timestamp) }
assert.deepEqual(await decodeSchema(contract, await encodeSchema(contract, value)), value)
assert.deepEqual(await new Events().ingest.parsePayload(input), value)
console.log('External consumer: optional Zod subpath, nested codecs and inferred types passed')
