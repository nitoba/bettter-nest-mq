import { z } from 'zod'
import { QueueService, QueueControls, MqModule } from '../../src/index.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

const timestamp = zodCodec(
  z.codec(z.iso.datetime(), z.date(), {
    decode: (input) => new Date(input),
    encode: (date) => date.toISOString()
  })
)
class Typed extends QueueService {
  readonly task = this.job({
    payload: timestamp,
    result: z.string(),
    dispatchKey: (payload) => payload.toISOString()
  })
  // @ts-expect-error A dispatch key must be a string or undefined, not a number.
  readonly invalid = this.job({ payload: z.string(), result: z.string(), dispatchKey: () => 12 })
}
QueueControls({
  globalConcurrency: 3,
  perKeyConcurrency: 1,
  rateLimit: { max: 10, durationMs: 1_000 }
})
// @ts-expect-error A limit is numeric, not a string.
QueueControls({ globalConcurrency: '3' })
// @ts-expect-error Policy mode is explicit and narrowly typed.
MqModule.forRoot({ controls: { mode: 'overwrite-anything' } })
export async function publish(queue: Typed): Promise<void> {
  await queue.task.enqueue('2026-09-10T12:00:00.000Z')
  await queue.task.enqueueDecoded(new Date())
}
