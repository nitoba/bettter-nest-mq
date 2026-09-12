import { z } from 'zod'
import { QueueService, type JobWaitOptions } from '../../src/index.ts'

class Queue extends QueueService {
  readonly task = this.job({ payload: z.string(), result: z.number() })
}
export const eventWait: JobWaitOptions = {
  strategy: 'events',
  pollFallbackMs: 1000,
  timeoutMs: 5000
}
export const pollWait: JobWaitOptions = {
  pollIntervalMs: 100,
  signal: new AbortController().signal
}
// @ts-expect-error Polling intervals and event fallback intervals have different meanings.
export const mixed: JobWaitOptions = { strategy: 'events', pollIntervalMs: 100 }
// @ts-expect-error A fallback is not accepted silently without selecting event waits.
export const implicit: JobWaitOptions = { pollFallbackMs: 1000 }
// @ts-expect-error The native event-store token must not be configured by application callers.
export const engineToken: JobWaitOptions = { strategy: 'events', eventStore: {} }
export async function typed(queue: Queue): Promise<number> {
  return queue.task.execute('input', { wait: eventWait })
}
