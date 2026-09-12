import { expect, test } from 'bun:test'
import { z } from 'zod'
import { Job, Queue, QueueService, Retry, getQueueDefinition } from '../../src/index.ts'
import { compileJob } from '../../src/engine/job-compiler.ts'

@Queue({ name: 'custom-retry', connection: 'primary' })
class RetryQueue extends QueueService {
  @Job({ name: 'task', version: 1 })
  @Retry({ attempts: 3, backoff: { type: 'custom', policy: 'remote-service', version: 1 } })
  readonly task = this.job({ payload: z.string(), result: z.string(), failure: z.string() })
}

test('producer compilation of a named policy persists no executable callback or fabricated backoff', () => {
  const registered = getQueueDefinition(new RetryQueue()).jobs[0]
  expect(registered).toBeDefined()
  if (registered === undefined) throw new Error('Missing fixture job')
  const compiled = compileJob(registered)
  expect(compiled.defaults.attempts).toBe(3)
  expect(compiled.defaults.backoff).toBeUndefined()
  expect(JSON.stringify(compiled.defaults)).not.toContain('decide')
})
