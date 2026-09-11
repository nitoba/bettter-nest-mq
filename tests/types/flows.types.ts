import { z } from 'zod'
import {
  Job,
  QueueService,
  flowJob,
  flowChildren,
  type FlowResultsReader
} from '../../src/index.ts'
class Typed extends QueueService {
  @Job({ name: 'task', version: 1 })
  readonly task = this.job({
    payload: z.object({ count: z.number() }),
    result: z.number(),
    failure: z.object({ reason: z.string() })
  })
  readonly notAJob = true
}
export const task = flowJob(Typed, 'task')
export const valid = flowChildren(task, [{ key: 'one', payload: { count: 1 } }])
// @ts-expect-error Only job properties can be referenced.
export const badProperty = flowJob(Typed, 'notAJob')
// @ts-expect-error Child payloads retain their input schema type.
export const badPayload = flowChildren(task, [{ key: 'one', payload: { count: 'wrong' } }])
export async function check(reader: FlowResultsReader): Promise<void> {
  const page = await reader.page(task, { limit: 10 })
  for (const item of page.items) {
    if (item.outcome === 'completed') {
      const result: number = item.result
      // @ts-expect-error Completed results retain their result schema type.
      const invalid: string = item.result
      void result
      void invalid
    } else if (item.outcome === 'failed' && item.failure !== undefined) {
      const reason: string = item.failure.reason
      void reason
    }
  }
}
