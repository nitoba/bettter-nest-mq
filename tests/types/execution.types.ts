import { z } from 'zod'
import { Job, Process, Queue, QueueService, type JobEnqueueOptions } from '../../src/index.ts'
import { zodCodec } from '../../src/integrations/zod.ts'

@Queue({ name: 'typed-execution' })
class TypedQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({
    payload: zodCodec(
      z.codec(z.iso.datetime(), z.date(), {
        decode: (text) => new Date(text),
        encode: (date) => date.toISOString()
      })
    ),
    result: z.object({ count: z.number() })
  })
  readonly notAJob = 'value'
}

Process(TypedQueue, 'task')
// @ts-expect-error References must point at existing job properties.
Process(TypedQueue, 'missing')
// @ts-expect-error Non-job properties cannot be processed.
Process(TypedQueue, 'notAJob')

export const scheduled: JobEnqueueOptions = { delayMs: 100 }
// @ts-expect-error Relative and absolute schedules are mutually exclusive.
export const ambiguous: JobEnqueueOptions = { delayMs: 100, at: 1_000 }

export async function verify(queue: TypedQueue): Promise<void> {
  const id: string = await queue.task.enqueue('2026-09-10T12:00:00.000Z')
  await queue.task.enqueueDecoded(new Date())
  // @ts-expect-error The input type is the wire-side timestamp.
  await queue.task.enqueue(new Date())
  // @ts-expect-error enqueueDecoded requires a decoded Date.
  await queue.task.enqueueDecoded('2026-09-10T12:00:00.000Z')
  const result = await queue.task.awaitResult(id)
  const count: number = result.count
  // @ts-expect-error Result fields remain inferred, not any.
  const invalid: string = result.count
  void count
  void invalid
}
